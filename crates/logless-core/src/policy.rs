//! Runtime policy: the operator-tunable part of the configuration.
//!
//! The TOML file supplies defaults; `data_dir/policy.json` (see
//! [`crate::policy_store`]) overrides it field by field. This module is the
//! pure part: the document, its validation, leaf-level diff, which leaves apply
//! hot, and the live handle the maintenance pass and the per-record pushdown
//! check read instead of the startup `Config` copy.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::budget::Watermarks;
use crate::config::{Config, LevelBucket, LevelBuckets};

/// Highest OTel severity number.
pub const MAX_SEVERITY: u64 = 24;
/// Sanity ceilings. Without them a typo becomes "never expire" or a queue that
/// allocates gigabytes at startup (crossbeam's bounded channel is preallocated).
pub const MAX_RETENTION_SECS: u64 = 100 * 365 * 24 * 3600;
pub const MAX_QUEUE_CAPACITY: u64 = 1 << 20;
pub const MAX_CRITICAL_TIMEOUT_MS: u64 = 60_000;
/// Below this a queue overflows on ordinary bursts; see [`lossy_settings`].
pub const MIN_SAFE_QUEUE_CAPACITY: u64 = 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policies {
    pub retention: Vec<RetentionRule>,
    pub storage: StoragePolicy,
    pub ingest: IngestPolicy,
    pub pushdown: PushdownPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionRule {
    pub name: String,
    pub min_severity: u64,
    pub retention_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoragePolicy {
    pub disk_budget_bytes: u64,
    pub high_water: f64,
    pub critical_water: f64,
    pub reclaim_target: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestPolicy {
    pub critical_enqueue_timeout_ms: u64,
    pub queue_capacity: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushdownPolicy {
    pub enabled: bool,
    pub trigger_severity: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Problem {
    pub path: String,
    pub message: String,
}

impl Problem {
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self { path: path.into(), message: message.into() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Apply {
    Hot,
    Restart,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiffEntry {
    pub path: String,
    pub from: Value,
    pub to: Value,
    pub apply: Apply,
}

/// How a changed leaf takes effect. Read off the code, not guessed:
///
/// * retention rules, disk budget and watermarks: `maintenance_pass` re-reads
///   them from the live handle every tick (and `merge_all` takes the buckets
///   per pass, so a changed bucket map only affects data merged afterwards);
/// * pushdown: read per record from the handle's atomics;
/// * critical enqueue timeout: a shared atomic inside the queue, read on every
///   WARN+ overflow;
/// * queue capacity: a bounded channel is sized once at construction (and the
///   HEC admission check captured the number at startup), so it needs a restart.
pub fn apply_mode(path: &str) -> Apply {
    match path {
        "ingest.queue_capacity" => Apply::Restart,
        _ => Apply::Hot,
    }
}

impl Policies {
    /// The file's view of the policy document.
    pub fn from_config(c: &Config) -> Self {
        Self {
            // The default index's buckets: the document has one `retention`
            // list, and other indexes are set in the config file only.
            retention: c
                .default_buckets()
                .iter()
                .map(|b| RetentionRule {
                    name: b.name.clone(),
                    min_severity: u64::from(b.min_severity),
                    retention_secs: b.retention.as_secs(),
                })
                .collect(),
            storage: StoragePolicy {
                disk_budget_bytes: c.storage.disk_budget_bytes,
                high_water: c.storage.high_water,
                critical_water: c.storage.critical_water,
                reclaim_target: c.storage.reclaim_target,
            },
            ingest: IngestPolicy {
                critical_enqueue_timeout_ms: c.ingest.critical_enqueue_timeout.as_millis() as u64,
                queue_capacity: c.ingest.queue_capacity as u64,
            },
            pushdown: PushdownPolicy {
                enabled: c.pushdown.enabled,
                trigger_severity: u64::from(c.pushdown.trigger_severity),
            },
        }
    }

    /// Writes this document's values into a copy of `base`. Used once at
    /// startup so restart-mode settings (queue capacity) take their effective
    /// value in the code that sizes things from `Config`.
    pub fn apply_to_config(&self, base: &Config) -> Result<Config, crate::config::ConfigError> {
        let mut c = base.clone();
        c.set_default_buckets(self.buckets()?);
        c.storage.disk_budget_bytes = self.storage.disk_budget_bytes;
        c.storage.high_water = self.storage.high_water;
        c.storage.critical_water = self.storage.critical_water;
        c.storage.reclaim_target = self.storage.reclaim_target;
        c.ingest.critical_enqueue_timeout =
            Duration::from_millis(self.ingest.critical_enqueue_timeout_ms);
        c.ingest.queue_capacity = self.ingest.queue_capacity as usize;
        c.pushdown.enabled = self.pushdown.enabled;
        c.pushdown.trigger_severity = self.pushdown.trigger_severity as u8;
        Ok(c)
    }

    pub fn buckets(&self) -> Result<LevelBuckets, crate::config::ConfigError> {
        LevelBuckets::new(
            self.retention
                .iter()
                .map(|r| LevelBucket {
                    name: r.name.clone(),
                    min_severity: r.min_severity.min(255) as u8,
                    retention: Duration::from_secs(r.retention_secs),
                })
                .collect(),
        )
    }

    pub fn watermarks(&self) -> Watermarks {
        Watermarks {
            high: self.storage.high_water,
            critical: self.storage.critical_water,
            reclaim_target: self.storage.reclaim_target,
        }
    }

    /// Every invariant, each reported at the path that breaks it. Empty means
    /// the document can be applied as is.
    pub fn validate(&self) -> Vec<Problem> {
        let mut out = Vec::new();
        self.validate_retention(&mut out);
        self.validate_storage(&mut out);

        let i = &self.ingest;
        if i.queue_capacity == 0 || i.queue_capacity > MAX_QUEUE_CAPACITY {
            out.push(Problem::new(
                "ingest.queue_capacity",
                format!("must be between 1 and {MAX_QUEUE_CAPACITY} records"),
            ));
        }
        if i.critical_enqueue_timeout_ms > MAX_CRITICAL_TIMEOUT_MS {
            out.push(Problem::new(
                "ingest.critical_enqueue_timeout_ms",
                format!(
                    "at most {MAX_CRITICAL_TIMEOUT_MS} ms: the wait is bounded so a stalled disk cannot stall the application"
                ),
            ));
        }
        if self.pushdown.trigger_severity > MAX_SEVERITY {
            out.push(Problem::new(
                "pushdown.trigger_severity",
                format!("must be 0 to {MAX_SEVERITY}"),
            ));
        }
        out
    }

    fn validate_retention(&self, out: &mut Vec<Problem>) {
        if self.retention.is_empty() {
            out.push(Problem::new("retention", "define at least one bucket"));
            return;
        }
        let before = out.len();
        for (i, r) in self.retention.iter().enumerate() {
            let at = |field: &str| format!("retention[{i}].{field}");
            if let Some(why) = crate::config::bucket_name_problem(&r.name) {
                out.push(Problem::new(at("name"), format!("{why}; name {:?} cannot be used", r.name)));
            }
            if r.min_severity > MAX_SEVERITY {
                out.push(Problem::new(at("min_severity"), format!("must be 0 to {MAX_SEVERITY}")));
            }
            if r.retention_secs == 0 || r.retention_secs > MAX_RETENTION_SECS {
                out.push(Problem::new(
                    at("retention_secs"),
                    format!(
                        "must be between 1 and {MAX_RETENTION_SECS} seconds; to stop keeping a level, remove the bucket instead"
                    ),
                ));
            }
            if i == 0 && r.min_severity != 0 {
                out.push(Problem::new(
                    at("min_severity"),
                    "the first bucket must start at severity 0 so every record has a bucket",
                ));
            }
            if i > 0 && r.min_severity <= self.retention[i - 1].min_severity {
                out.push(Problem::new(
                    at("min_severity"),
                    format!(
                        "must be greater than retention[{}].min_severity; list buckets in ascending severity order",
                        i - 1
                    ),
                ));
            }
            if let Some(j) = self.retention[..i].iter().position(|p| p.name.eq_ignore_ascii_case(&r.name)) {
                out.push(Problem::new(
                    at("name"),
                    format!("duplicates retention[{j}].name (names are compared ignoring case: they are directories, and INFO and info are one directory on macOS and Windows)"),
                ));
            }
        }
        // The path-aware checks above are the useful messages; this keeps the
        // document and `LevelBuckets` from ever disagreeing about validity.
        if out.len() == before {
            if let Err(e) = self.buckets() {
                out.push(Problem::new("retention", e.to_string()));
            }
        }
    }

    fn validate_storage(&self, out: &mut Vec<Problem>) {
        let s = &self.storage;
        if s.disk_budget_bytes == 0 {
            out.push(Problem::new("storage.disk_budget_bytes", "must be greater than 0"));
        }
        let frac = |v: f64| v.is_finite();
        if !(frac(s.reclaim_target) && s.reclaim_target > 0.0) {
            out.push(Problem::new("storage.reclaim_target", "must be greater than 0"));
        } else if s.reclaim_target >= s.high_water {
            out.push(Problem::new(
                "storage.reclaim_target",
                "must be below high_water, or reclaim would re-trigger on every pass",
            ));
        }
        if !(frac(s.high_water) && s.high_water < s.critical_water) {
            out.push(Problem::new("storage.high_water", "must be below critical_water"));
        }
        if !(frac(s.critical_water) && s.critical_water <= 1.0) {
            out.push(Problem::new("storage.critical_water", "must be at most 1"));
        }
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("policy document serialises")
    }

    /// Flattened primitives keyed by path, e.g. `retention[1].retention_secs`.
    pub fn leaves(&self) -> Vec<(String, Value)> {
        let mut out = Vec::new();
        flatten("", &self.to_value(), &mut out);
        out
    }

    pub fn leaf_map(&self) -> BTreeMap<String, Value> {
        self.leaves().into_iter().collect()
    }

    pub fn apply_modes(&self) -> BTreeMap<String, Apply> {
        self.leaves().into_iter().map(|(p, _)| { let a = apply_mode(&p); (p, a) }).collect()
    }
}

fn flatten(prefix: &str, v: &Value, out: &mut Vec<(String, Value)>) {
    match v {
        Value::Object(m) => {
            for (k, child) in m {
                let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                flatten(&path, child, out);
            }
        }
        Value::Array(a) => {
            for (i, child) in a.iter().enumerate() {
                flatten(&format!("{prefix}[{i}]"), child, out);
            }
        }
        other => out.push((prefix.to_string(), other.clone())),
    }
}

/// Leaf-level difference. A bucket added or removed shows up as its leaves
/// going from or to `null`.
pub fn diff(old: &Policies, new: &Policies) -> Vec<DiffEntry> {
    let (a, b) = (old.leaf_map(), new.leaf_map());
    let mut paths: Vec<&String> = a.keys().chain(b.keys()).collect();
    paths.sort_by(|x, y| natural_cmp(x, y));
    paths.dedup();
    paths
        .into_iter()
        .filter_map(|p| {
            let from = a.get(p).cloned().unwrap_or(Value::Null);
            let to = b.get(p).cloned().unwrap_or(Value::Null);
            (from != to).then(|| DiffEntry {
                path: p.clone(),
                from,
                to,
                apply: apply_mode(p),
            })
        })
        .collect()
}

/// `retention[2]` before `retention[10]`.
fn natural_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    fn key(s: &str) -> Vec<(String, u64)> {
        s.split(['.', '[', ']'])
            .filter(|p| !p.is_empty())
            .map(|p| match p.parse::<u64>() {
                Ok(n) => (String::new(), n),
                Err(_) => (p.to_string(), 0),
            })
            .collect()
    }
    key(a).cmp(&key(b))
}

/// Which override keys differ between the file defaults and `doc`.
///
/// Retention is field by field (`retention[1].retention_secs`) as long as the
/// bucket structure (count, names, severity floors) is the file's: then a
/// later edit of an untouched bucket in the file still reaches the process.
/// A structural change is one unit (`"retention"`): buckets are positional and
/// interdependent, so a partial overlay could produce a map neither side ever
/// validated.
pub fn overrides_between(file: &Policies, doc: &Policies) -> Vec<String> {
    let mut keys = Vec::new();
    if same_bucket_structure(file, doc) {
        for (i, (f, d)) in file.retention.iter().zip(&doc.retention).enumerate() {
            if f.retention_secs != d.retention_secs {
                keys.push(format!("retention[{i}].retention_secs"));
            }
        }
    } else {
        keys.push("retention".to_string());
    }
    let (a, b) = (file.leaf_map(), doc.leaf_map());
    for (path, value) in &b {
        if !path.starts_with("retention[") && a.get(path) != Some(value) {
            keys.push(path.clone());
        }
    }
    keys
}

fn same_bucket_structure(a: &Policies, b: &Policies) -> bool {
    a.retention.len() == b.retention.len()
        && a.retention
            .iter()
            .zip(&b.retention)
            .all(|(x, y)| x.name == y.name && x.min_severity == y.min_severity)
}

/// `retention[3].retention_secs` -> (3, "retention_secs").
fn parse_retention_key(key: &str) -> Option<(usize, &str)> {
    let rest = key.strip_prefix("retention[")?;
    let (idx, field) = rest.split_once("].")?;
    Some((idx.parse().ok()?, field))
}

/// Defaults from the file with the persisted overrides laid on top, so a field
/// the operator never touched still follows the file after an edit and restart.
pub fn overlay(file: &Policies, stored: &Policies, overrides: &[String]) -> Result<Policies, String> {
    let mut v = file.to_value();
    let s = stored.to_value();
    for key in overrides {
        if let Some((i, field)) = parse_retention_key(key) {
            let name = stored.retention.get(i).map(|r| r.name.as_str());
            let file_has = file.retention.get(i).map(|r| r.name.as_str()) == name && name.is_some();
            if !file_has {
                // The config file no longer has that bucket: the override has
                // nothing to attach to, so the file's retention stands.
                tracing::warn!(key, "ignoring a runtime retention override for a bucket the config file no longer defines");
                continue;
            }
            let value = s["retention"][i]
                .get(field)
                .ok_or_else(|| format!("override {key:?} is not part of the policy document"))?;
            v["retention"][i][field] = value.clone();
        } else if key == "retention" {
            v["retention"] = s["retention"].clone();
        } else if let Some((group, field)) = key.split_once('.') {
            let value = s
                .get(group)
                .and_then(|g| g.get(field))
                .ok_or_else(|| format!("override {key:?} is not part of the policy document"))?;
            v[group][field] = value.clone();
        } else {
            return Err(format!("unknown override key {key:?}"));
        }
    }
    serde_json::from_value(v).map_err(|e| e.to_string())
}

/// `leaf -> 'file' | 'runtime'`.
pub fn sources(effective: &Policies, overrides: &[String]) -> BTreeMap<String, &'static str> {
    let whole_retention = overrides.iter().any(|o| o == "retention");
    effective
        .leaves()
        .into_iter()
        .map(|(path, _)| {
            let runtime = if path.starts_with("retention[") {
                whole_retention || overrides.iter().any(|o| *o == path)
            } else {
                overrides.iter().any(|o| *o == path)
            };
            (path, if runtime { "runtime" } else { "file" })
        })
        .collect()
}

/// Restart-mode leaves whose stored value is not what the process started with.
pub fn pending_restart(effective: &Policies, running: &Policies) -> Vec<String> {
    let (e, r) = (effective.leaf_map(), running.leaf_map());
    e.iter()
        .filter(|(p, v)| apply_mode(p) == Apply::Restart && r.get(*p) != Some(*v))
        .map(|(p, _)| p.clone())
        .collect()
}

/// What the running process uses for each pending restart-mode leaf, so a
/// reader of `effective` alone is not misled about what is enforced now.
pub fn running_values(effective: &Policies, running: &Policies) -> BTreeMap<String, Value> {
    let r = running.leaf_map();
    pending_restart(effective, running)
        .into_iter()
        .map(|p| {
            let v = r.get(&p).cloned().unwrap_or(Value::Null);
            (p, v)
        })
        .collect()
}

/// Settings that are not deletions but still lose data that would otherwise
/// be kept, so they need the same explicit confirmation. Empty when `proposed`
/// introduces none (settings already lossy in `current` are not re-flagged).
pub fn lossy_settings(current: &Policies, proposed: &Policies) -> Vec<Problem> {
    let mut out = Vec::new();
    let tiny_queue = |p: &Policies| p.ingest.queue_capacity < MIN_SAFE_QUEUE_CAPACITY;
    if tiny_queue(proposed) && proposed.ingest.queue_capacity != current.ingest.queue_capacity {
        out.push(Problem::new(
            "ingest.queue_capacity",
            format!(
                "below {MIN_SAFE_QUEUE_CAPACITY} records the queue overflows on ordinary bursts and DEBUG/INFO records are dropped"
            ),
        ));
    }
    if proposed.ingest.critical_enqueue_timeout_ms == 0 && current.ingest.critical_enqueue_timeout_ms != 0 {
        out.push(Problem::new(
            "ingest.critical_enqueue_timeout_ms",
            "0 ms drops WARN and above immediately when the queue is full instead of waiting for room",
        ));
    }
    if current.pushdown.enabled && !proposed.pushdown.enabled {
        out.push(Problem::new(
            "pushdown.enabled",
            "disabling pushdown stops forwarding errors as they arrive; they are only kept in the store",
        ));
    }
    out
}

/// Values the maintenance pass needs, swapped as one unit so a pass never sees
/// buckets from one version and a budget from another.
#[derive(Debug, Clone)]
pub struct Runtime {
    /// The buckets of the own tenant's default index.
    pub buckets: LevelBuckets,
    pub disk_budget_bytes: u64,
    pub watermarks: Watermarks,
}

impl Runtime {
    /// The tenancy a pass enforces: the config file's (`base`) with the
    /// default index's buckets taken from the live policy.
    pub fn tenancy(&self, base: &crate::config::Tenancy) -> crate::config::Tenancy {
        let mut tenancy = base.clone();
        tenancy.set_default_buckets(self.buckets.clone());
        tenancy
    }
}

/// What the running process actually enforces. Reads are a read-lock and an
/// `Arc` clone (maintenance, once per tick) or a relaxed atomic load (per
/// record).
pub struct PolicyHandle {
    runtime: RwLock<Arc<Runtime>>,
    pushdown_enabled: AtomicBool,
    pushdown_trigger: AtomicU8,
    critical_timeout_ms: Arc<AtomicU64>,
    running: Policies,
}

impl PolicyHandle {
    /// `startup` is what the process was configured with; restart-mode changes
    /// are reported as pending until a new process starts with them.
    pub fn new(startup: Policies) -> Result<Arc<Self>, crate::config::ConfigError> {
        let runtime = Runtime {
            buckets: startup.buckets()?,
            disk_budget_bytes: startup.storage.disk_budget_bytes,
            watermarks: startup.watermarks(),
        };
        Ok(Arc::new(Self {
            runtime: RwLock::new(Arc::new(runtime)),
            pushdown_enabled: AtomicBool::new(startup.pushdown.enabled),
            pushdown_trigger: AtomicU8::new(startup.pushdown.trigger_severity as u8),
            critical_timeout_ms: Arc::new(AtomicU64::new(startup.ingest.critical_enqueue_timeout_ms)),
            running: startup,
        }))
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        match self.runtime.read() {
            Ok(g) => Arc::clone(&g),
            Err(p) => Arc::clone(&p.into_inner()),
        }
    }

    pub fn pushdown_enabled(&self) -> bool {
        self.pushdown_enabled.load(Ordering::Relaxed)
    }

    pub fn pushdown_trigger(&self) -> u8 {
        self.pushdown_trigger.load(Ordering::Relaxed)
    }

    /// Hand to [`crate::queue::IngestQueue::with_timeout_cell`].
    pub fn critical_timeout_cell(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.critical_timeout_ms)
    }

    pub fn running(&self) -> &Policies {
        &self.running
    }

    /// Publishes a validated document. Restart-mode leaves are ignored here by
    /// construction: nothing reads them from the handle.
    pub fn apply(&self, p: &Policies) -> Result<(), crate::config::ConfigError> {
        let runtime = Arc::new(Runtime {
            buckets: p.buckets()?,
            disk_budget_bytes: p.storage.disk_budget_bytes,
            watermarks: p.watermarks(),
        });
        match self.runtime.write() {
            Ok(mut g) => *g = runtime,
            Err(poisoned) => *poisoned.into_inner() = runtime,
        }
        self.pushdown_enabled.store(p.pushdown.enabled, Ordering::Relaxed);
        self.pushdown_trigger.store(p.pushdown.trigger_severity as u8, Ordering::Relaxed);
        self.critical_timeout_ms
            .store(p.ingest.critical_enqueue_timeout_ms, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_policies() -> Policies {
    Policies {
        retention: vec![
            RetentionRule { name: "debug".into(), min_severity: 0, retention_secs: 86_400 },
            RetentionRule { name: "info".into(), min_severity: 9, retention_secs: 7 * 86_400 },
            RetentionRule { name: "warn_plus".into(), min_severity: 13, retention_secs: 30 * 86_400 },
        ],
        storage: StoragePolicy {
            disk_budget_bytes: 1 << 30,
            high_water: 0.8,
            critical_water: 0.95,
            reclaim_target: 0.7,
        },
        ingest: IngestPolicy { critical_enqueue_timeout_ms: 250, queue_capacity: 65_536 },
        pushdown: PushdownPolicy { enabled: true, trigger_severity: 17 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(p: &[Problem]) -> Vec<&str> {
        p.iter().map(|p| p.path.as_str()).collect()
    }

    #[test]
    fn a_sane_document_has_no_problems() {
        assert_eq!(test_policies().validate(), vec![]);
    }

    #[test]
    fn validation_matrix_reports_the_path_that_breaks() {
        type Edit = fn(&mut Policies);
        let cases: Vec<(&str, Edit, &str)> = vec![
            ("empty retention", |p| p.retention.clear(), "retention"),
            ("first bucket not zero", |p| p.retention[0].min_severity = 1, "retention[0].min_severity"),
            ("severity above 24", |p| p.retention[2].min_severity = 25, "retention[2].min_severity"),
            ("not ascending", |p| p.retention[2].min_severity = 9, "retention[2].min_severity"),
            ("zero retention", |p| p.retention[1].retention_secs = 0, "retention[1].retention_secs"),
            ("absurd retention", |p| p.retention[1].retention_secs = u64::MAX, "retention[1].retention_secs"),
            ("slash in name", |p| p.retention[1].name = "a/b".into(), "retention[1].name"),
            ("dot in name", |p| p.retention[1].name = "..".into(), "retention[1].name"),
            ("empty name", |p| p.retention[1].name = String::new(), "retention[1].name"),
            ("space in name", |p| p.retention[1].name = "a b".into(), "retention[1].name"),
            ("duplicate name", |p| p.retention[2].name = "info".into(), "retention[2].name"),
            ("zero budget", |p| p.storage.disk_budget_bytes = 0, "storage.disk_budget_bytes"),
            ("reclaim not below high", |p| p.storage.reclaim_target = 0.8, "storage.reclaim_target"),
            ("reclaim zero", |p| p.storage.reclaim_target = 0.0, "storage.reclaim_target"),
            ("high not below critical", |p| p.storage.high_water = 0.95, "storage.high_water"),
            ("critical above one", |p| p.storage.critical_water = 1.01, "storage.critical_water"),
            ("zero queue", |p| p.ingest.queue_capacity = 0, "ingest.queue_capacity"),
            ("huge queue", |p| p.ingest.queue_capacity = u64::MAX, "ingest.queue_capacity"),
            ("long timeout", |p| p.ingest.critical_enqueue_timeout_ms = 60_001, "ingest.critical_enqueue_timeout_ms"),
            ("trigger above 24", |p| p.pushdown.trigger_severity = 25, "pushdown.trigger_severity"),
        ];
        for (name, edit, want) in cases {
            let mut p = test_policies();
            edit(&mut p);
            let problems = p.validate();
            assert!(paths(&problems).contains(&want), "{name}: wanted {want}, got {problems:?}");
        }
    }

    #[test]
    fn boundary_values_are_accepted() {
        let mut p = test_policies();
        p.storage.critical_water = 1.0;
        p.ingest.critical_enqueue_timeout_ms = 0;
        p.ingest.queue_capacity = 1;
        p.pushdown.trigger_severity = 0;
        p.retention[2].min_severity = 24;
        assert_eq!(p.validate(), vec![]);
    }

    #[test]
    fn apply_modes_follow_the_code() {
        let modes = test_policies().apply_modes();
        assert_eq!(modes["ingest.queue_capacity"], Apply::Restart);
        for hot in [
            "retention[1].retention_secs",
            "retention[0].name",
            "storage.disk_budget_bytes",
            "storage.high_water",
            "storage.critical_water",
            "storage.reclaim_target",
            "ingest.critical_enqueue_timeout_ms",
            "pushdown.enabled",
            "pushdown.trigger_severity",
        ] {
            assert_eq!(modes[hot], Apply::Hot, "{hot}");
        }
        assert_eq!(modes.len(), 3 * 3 + 4 + 2 + 2);
    }

    #[test]
    fn diff_is_leaf_level_with_apply_modes_and_natural_order() {
        let old = test_policies();
        let mut new = old.clone();
        new.retention[1].retention_secs = 3600;
        new.ingest.queue_capacity = 1024;
        new.retention.push(RetentionRule { name: "fatal".into(), min_severity: 21, retention_secs: 60 });
        let d = diff(&old, &new);
        let got: Vec<(&str, Apply)> = d.iter().map(|e| (e.path.as_str(), e.apply)).collect();
        assert_eq!(
            got,
            vec![
                ("ingest.queue_capacity", Apply::Restart),
                ("retention[1].retention_secs", Apply::Hot),
                ("retention[3].min_severity", Apply::Hot),
                ("retention[3].name", Apply::Hot),
                ("retention[3].retention_secs", Apply::Hot),
            ]
        );
        assert_eq!(d[1].from, Value::from(7 * 86_400));
        assert_eq!(d[1].to, Value::from(3600));
        let added = d.iter().find(|e| e.path == "retention[3].name").unwrap();
        assert_eq!(added.from, Value::Null);
        assert!(diff(&old, &old).is_empty());
    }

    #[test]
    fn overlay_follows_the_file_for_untouched_fields() {
        let file = test_policies();
        let mut doc = file.clone();
        doc.storage.disk_budget_bytes = 5;
        doc.pushdown.enabled = false;
        let overrides = overrides_between(&file, &doc);
        assert_eq!(overrides, vec!["pushdown.enabled", "storage.disk_budget_bytes"]);

        // The operator later edits the file's trigger; only untouched fields move.
        let mut file2 = file.clone();
        file2.pushdown.trigger_severity = 21;
        file2.storage.disk_budget_bytes = 999;
        let eff = overlay(&file2, &doc, &overrides).unwrap();
        assert_eq!(eff.pushdown.trigger_severity, 21, "untouched: follows the file");
        assert_eq!(eff.storage.disk_budget_bytes, 5, "overridden: runtime wins");
        assert!(!eff.pushdown.enabled);

        let src = sources(&eff, &overrides);
        assert_eq!(src["pushdown.trigger_severity"], "file");
        assert_eq!(src["storage.disk_budget_bytes"], "runtime");
    }

    #[test]
    fn retention_is_overridden_field_by_field_while_the_structure_is_the_files() {
        let file = test_policies();
        let mut doc = file.clone();
        doc.retention[1].retention_secs = 60;
        let overrides = overrides_between(&file, &doc);
        assert_eq!(overrides, vec!["retention[1].retention_secs"]);
        let src = sources(&doc, &overrides);
        assert_eq!(src["retention[1].retention_secs"], "runtime");
        assert_eq!(src["retention[0].retention_secs"], "file");
        assert_eq!(src["retention[1].name"], "file");
        assert_eq!(src["storage.high_water"], "file");
    }

    #[test]
    fn a_structural_retention_change_is_one_unit() {
        let file = test_policies();
        let mut doc = file.clone();
        doc.retention[1].name = "notice".into();
        let overrides = overrides_between(&file, &doc);
        assert_eq!(overrides, vec!["retention"]);
        let src = sources(&doc, &overrides);
        assert_eq!(src["retention[0].name"], "runtime");
        assert_eq!(src["storage.high_water"], "file");
        assert_eq!(overlay(&file, &doc, &overrides).unwrap(), doc);
    }

    #[test]
    fn a_field_override_for_a_bucket_the_file_dropped_is_ignored() {
        let file = test_policies();
        let mut doc = file.clone();
        doc.retention[2].retention_secs = 60;
        let overrides = overrides_between(&file, &doc);
        let mut shrunk = file.clone();
        shrunk.retention.truncate(2);
        assert_eq!(overlay(&shrunk, &doc, &overrides).unwrap(), shrunk);
    }

    #[test]
    fn bucket_names_are_safe_directory_names() {
        for bad in ["", "a b", "a/b", "a.b", "a=b", "caf\u{e9}", "x\u{202e}", "CON", "com3", "NUL", "a:b"] {
            assert!(crate::config::bucket_name_problem(bad).is_some(), "{bad:?}");
        }
        for good in ["debug", "warn_plus", "_default", "warn+", "L-1", "COM0", "CONSOLE"] {
            assert!(crate::config::bucket_name_problem(good).is_none(), "{good:?}");
        }
        let mut p = test_policies();
        p.retention[2].name = "INFO".into();
        assert!(p.validate().iter().any(|x| x.message.contains("ignoring case")));
    }

    #[test]
    fn pending_restart_clears_when_the_process_started_with_the_value() {
        let running = test_policies();
        let mut stored = running.clone();
        stored.ingest.queue_capacity = 2048;
        stored.pushdown.enabled = false; // hot: never pending
        assert_eq!(pending_restart(&stored, &running), vec!["ingest.queue_capacity"]);
        assert!(pending_restart(&stored, &stored).is_empty());
    }

    #[test]
    fn handle_publishes_hot_values_atomically() {
        let h = PolicyHandle::new(test_policies()).unwrap();
        assert_eq!(h.runtime().buckets.by_name("info").unwrap().retention.as_secs(), 7 * 86_400);
        let mut p = test_policies();
        p.retention[1].retention_secs = 60;
        p.storage.high_water = 0.5;
        p.storage.reclaim_target = 0.4;
        p.pushdown.enabled = false;
        p.pushdown.trigger_severity = 21;
        p.ingest.critical_enqueue_timeout_ms = 5;
        p.ingest.queue_capacity = 7;
        h.apply(&p).unwrap();
        let rt = h.runtime();
        assert_eq!(rt.buckets.by_name("info").unwrap().retention.as_secs(), 60);
        assert_eq!(rt.watermarks.high, 0.5);
        assert!(!h.pushdown_enabled());
        assert_eq!(h.pushdown_trigger(), 21);
        assert_eq!(h.critical_timeout_cell().load(Ordering::Relaxed), 5);
        assert_eq!(h.running().ingest.queue_capacity, 65_536, "running config does not move");
    }

    #[test]
    fn config_roundtrip_is_lossless() {
        let c: Config = toml::from_str("[storage]\ndata_dir = \"/tmp/x\"\n").unwrap();
        let p = Policies::from_config(&c);
        assert_eq!(p.validate(), vec![]);
        let c2 = p.apply_to_config(&c).unwrap();
        assert_eq!(c2, c);
    }
}
