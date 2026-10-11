//! Configuration — one declarative file, versioned schema, from v1.
//!
//! Retrofitting a config schema is painful, so `schema_version` exists from the
//! first commit even though there is only one version.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::model::Severity;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub storage: StorageConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
    /// This instance's id as a writer: the `node=` leaf of every file it
    /// writes and the `origin` of the rows it receives itself.
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub pushdown: PushdownConfig,
    /// Which attribute keys get their own typed Parquet column
    /// (`docs/typed-columns.md`).
    #[serde(default)]
    pub typed_columns: crate::typed::TypedColumnsConfig,
    /// OTLP/HTTP receiver. Off by default — binding a port is not something to
    /// start doing because someone upgraded the agent.
    #[serde(default)]
    pub otlp: crate::otlp::ReceiverConfig,
    /// Splunk HEC receiver. Off by default, same reasoning.
    #[serde(default)]
    pub hec: crate::hec::HecConfig,
    /// Read-only S3-compatible endpoint over the Parquet store, so DuckDB,
    /// Polars and Grafana consume the logs directly. Off by default.
    #[serde(default)]
    pub serve_s3: crate::s3_serve::S3ServeConfig,
    /// Runtime policy API (`docs/admin-api.md`). Off by default; it needs a
    /// bearer token before it will bind at all.
    #[serde(default)]
    pub admin: crate::admin_api::AdminConfig,
    /// Sentry ingest proxy: your SDK keeps its DSN, only the host changes.
    #[serde(default)]
    pub sentry: crate::sentry::SentryConfig,
    /// The listener other log-less instances send to (`GET /.well-known/logless`, `POST /v1/ingest`).
    /// Off by default. To send TO another instance, add a `[[destinations]]` of `type = "logless"`.
    #[serde(default)]
    pub peer: crate::peer::PeerConfig,
    /// The edge side of Parquet part upload: a timer for the upload job, and holding unconfirmed files back
    /// from retention and reclaim. Off until `interval` is set (`docs/peer-sync.md`).
    #[serde(default)]
    pub sync_parts: crate::parts_edge::EdgePartsConfig,
    /// An optional decision model that suggests which templates are the same failure (`docs/decision-model.md`). Off unless `url` is set.
    #[serde(default)]
    pub decision: crate::decision::DecisionConfig,
    /// Tenants and their indexes (`[[tenants]]`, `[[tenants.indexes]]`): the
    /// first two directories of the store and where retention is set. Left
    /// out, the install is one tenant `default` with one index `default`. The
    /// first tenant listed is the one this instance writes its own records
    /// under.
    #[serde(default = "default_tenants")]
    pub tenants: Vec<TenantConfig>,
    /// File tailing sources (`[[tail]]`). Combined with any `--tail` flags.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tail: Vec<crate::tail::TailSource>,
    /// Where curated events go. Empty means "forward nowhere" — useful for
    /// measuring what *would* be sent before pointing at a real vendor.
    ///
    /// Skipped when empty so a generated config stays appendable: TOML puts a
    /// bare `destinations = []` in the root table, which then collides with an
    /// appended `[[destinations]]` section.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub destinations: Vec<crate::forward::Destination>,
}

/// Smart pushdown: forward errors with the lines that preceded them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushdownConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Severity at or above which an error triggers a context window.
    #[serde(default = "default_trigger")]
    pub trigger_severity: u8,
    /// Memory budget for buffered context. This is the ceiling on what
    /// pushdown can cost the host, and it is enforced, not advisory.
    #[serde(default = "default_ring_bytes")]
    pub ring_max_bytes: usize,
    #[serde(default = "default_context_lines")]
    pub max_context_lines: usize,
    #[serde(default = "default_context_age", with = "humantime_serde")]
    pub context_age: Duration,
    /// Full context windows per (service, error template) per minute. Bounds
    /// egress by distinct error shapes rather than error count.
    #[serde(default = "default_windows_per_minute")]
    pub windows_per_minute: u32,
    /// Ceiling on context windows per minute across all shapes. The per-shape
    /// budget bounds one storm; this bounds an incident starting.
    #[serde(default = "default_max_windows_per_minute")]
    pub max_windows_per_minute: u32,
    #[serde(default = "default_dedupe_window", with = "humantime_serde")]
    pub dedupe_window: Duration,
}

fn default_true() -> bool {
    true
}
fn default_trigger() -> u8 {
    Severity::ERROR.0
}
fn default_ring_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_context_lines() -> usize {
    200
}
fn default_context_age() -> Duration {
    Duration::from_secs(30)
}
fn default_windows_per_minute() -> u32 {
    3
}
fn default_max_windows_per_minute() -> u32 {
    30
}
fn default_dedupe_window() -> Duration {
    Duration::from_secs(60)
}

impl Default for PushdownConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            trigger_severity: default_trigger(),
            ring_max_bytes: default_ring_bytes(),
            max_context_lines: default_context_lines(),
            context_age: default_context_age(),
            windows_per_minute: default_windows_per_minute(),
            max_windows_per_minute: default_max_windows_per_minute(),
            dedupe_window: default_dedupe_window(),
        }
    }
}

fn default_schema_version() -> u32 {
    SCHEMA_VERSION
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Root directory. Contains `wal/` and `store/`.
    pub data_dir: PathBuf,
    /// Total disk budget across WAL + committed store. Watermarks are derived
    /// from this: shed-and-expire at 80%, stop WAL'ing debug at 95%.
    #[serde(default = "default_disk_budget")]
    pub disk_budget_bytes: u64,
    /// Fraction of the budget at which files start expiring ahead of retention.
    #[serde(default = "default_high_water")]
    pub high_water: f64,
    /// Fraction at which DEBUG also stops entering the WAL.
    #[serde(default = "default_critical_water")]
    pub critical_water: f64,
    /// Fraction early reclaim stops at once triggered.
    #[serde(default = "default_reclaim_target")]
    pub reclaim_target: f64,
    #[serde(default = "default_segment_bytes")]
    pub wal_segment_bytes: u64,
    /// Group-commit interval. The crash loss window is bounded by this.
    #[serde(default = "default_fsync_interval", with = "humantime_serde")]
    pub wal_fsync_interval: Duration,
    #[serde(default = "default_fsync_bytes")]
    pub wal_fsync_bytes: u64,
    /// A record whose event time is older than this when it is written goes
    /// to the late spool (`wal-late/`) instead of the main WAL, so a replay
    /// spread over many event hours makes one file per partition per late
    /// segment rather than one per main segment (`docs/architecture.md` §2).
    #[serde(default = "default_late_threshold", with = "humantime_serde")]
    pub late_threshold: Duration,
    /// The late spool rolls its segment after this long or `late_segment_bytes`,
    /// whichever comes first. Late rows reach Parquet when their segment rolls.
    #[serde(default = "default_late_roll", with = "humantime_serde")]
    pub late_roll: Duration,
    #[serde(default = "default_late_segment_bytes")]
    pub late_segment_bytes: u64,
    /// Compaction leaves alone a partition whose newest file is younger than
    /// this: under event-time partitioning any partition can still gain a file.
    #[serde(default = "default_compact_quiet", with = "humantime_serde")]
    pub compact_quiet: Duration,
}

fn default_disk_budget() -> u64 {
    32 * 1024 * 1024 * 1024
}
fn default_high_water() -> f64 {
    crate::budget::Watermarks::default().high
}
fn default_critical_water() -> f64 {
    crate::budget::Watermarks::default().critical
}
fn default_reclaim_target() -> f64 {
    crate::budget::Watermarks::default().reclaim_target
}
fn default_segment_bytes() -> u64 {
    128 * 1024 * 1024
}
fn default_fsync_interval() -> Duration {
    Duration::from_millis(100)
}
fn default_fsync_bytes() -> u64 {
    4 * 1024 * 1024
}
pub fn default_late_threshold() -> Duration {
    Duration::from_secs(2 * 3600)
}
pub fn default_late_roll() -> Duration {
    Duration::from_secs(30 * 60)
}
pub fn default_late_segment_bytes() -> u64 {
    64 * 1024 * 1024
}
pub fn default_compact_quiet() -> Duration {
    Duration::from_secs(10 * 60)
}

/// `[node]`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// 1 to 64 of `A-Za-z0-9._-` with at least one letter. Left out, an id is
    /// generated once and kept in `<data_dir>/node-id` ([`Config::node_id`]).
    /// For an edge that uploads parts it must equal its `[[peer.nodes]] id`
    /// on central.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// What happens to a record that names an index its tenant does not have.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnknownIndex {
    /// The record is not stored (HEC answers code 7, "Incorrect index").
    Refuse,
    /// The record goes to the tenant's default index.
    #[default]
    Default,
}

/// `[[tenants]]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantConfig {
    pub id: String,
    #[serde(default)]
    pub unknown_index: UnknownIndex,
    #[serde(default = "default_indexes")]
    pub indexes: Vec<IndexConfig>,
}

/// `[[tenants.indexes]]`: a dataset inside a tenant, and the unit of retention.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexConfig {
    pub name: String,
    /// The index's level buckets and how long each is kept
    /// (`[[tenants.indexes.retention]]`).
    #[serde(default)]
    pub retention: LevelBuckets,
}

fn default_indexes() -> Vec<IndexConfig> {
    vec![IndexConfig { name: crate::partition::DEFAULT_INDEX.into(), retention: LevelBuckets::default() }]
}

pub fn default_tenants() -> Vec<TenantConfig> {
    vec![TenantConfig { id: crate::partition::DEFAULT_TENANT.into(), unknown_index: UnknownIndex::default(), indexes: default_indexes() }]
}

/// The record attribute that names the index a record is for: set from the
/// OTLP resource attribute of the same name, from a HEC event's `index`, and
/// from `[[tail]] index`. It routes the record and is not stored: the index is
/// a directory, not a column.
pub const ATTR_INDEX: &str = "logless.index";

/// Where one record goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route<'a> {
    /// It named no index (the default one) or one the tenant has.
    Known(&'a str),
    /// It named an unknown index and the tenant sends those to its default.
    Defaulted(&'a str),
    /// It named an unknown index and the tenant refuses those.
    Refused,
}

/// The tenants and indexes this instance stores, and its own place among
/// them: what the write path, retention and reclaim resolve a partition's
/// policy from.
#[derive(Debug, Clone, PartialEq)]
pub struct Tenancy {
    tenants: Vec<TenantConfig>,
    node: String,
}

impl Default for Tenancy {
    fn default() -> Self {
        Self::single(LevelBuckets::default())
    }
}

impl Tenancy {
    pub fn new(tenants: Vec<TenantConfig>, node: impl Into<String>) -> Result<Self, ConfigError> {
        let node = node.into();
        if !crate::partition::valid_ident(&node) {
            return Err(ConfigError::BadIdent { what: "node id", value: node });
        }
        if tenants.is_empty() {
            return Err(ConfigError::NoTenants);
        }
        let mut seen = Vec::new();
        for t in &tenants {
            if !crate::partition::valid_ident(&t.id) {
                return Err(ConfigError::BadIdent { what: "tenant id", value: t.id.clone() });
            }
            // Ignoring case: ids are directories, and `Acme` and `acme` are
            // one directory on a case-insensitive filesystem.
            if seen.contains(&t.id.to_lowercase()) {
                return Err(ConfigError::DuplicateIdent { what: "tenant id", value: t.id.clone() });
            }
            seen.push(t.id.to_lowercase());
            if t.indexes.is_empty() {
                return Err(ConfigError::NoIndexes(t.id.clone()));
            }
            let mut names = Vec::new();
            for i in &t.indexes {
                if !crate::partition::valid_ident(&i.name) {
                    return Err(ConfigError::BadIdent { what: "index name", value: i.name.clone() });
                }
                if names.contains(&i.name.to_lowercase()) {
                    return Err(ConfigError::DuplicateIdent { what: "index name", value: i.name.clone() });
                }
                names.push(i.name.to_lowercase());
                LevelBuckets::new(i.retention.buckets.clone())?;
            }
        }
        Ok(Self { tenants, node })
    }

    /// One tenant `default` with one index `default` holding `buckets`,
    /// written by `node-a`: a single-tenant install, and what tests use.
    pub fn single(buckets: LevelBuckets) -> Self {
        let mut tenants = default_tenants();
        tenants[0].indexes[0].retention = buckets;
        Self { tenants, node: "node-a".into() }
    }

    fn own(&self) -> &TenantConfig {
        &self.tenants[0]
    }

    /// The tenant this instance writes under: the first one configured.
    pub fn tenant(&self) -> &str {
        &self.own().id
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    /// The own tenant's default index: the one named `default`, or else the
    /// first listed.
    pub fn default_index(&self) -> &str {
        let t = self.own();
        let at = t.indexes.iter().position(|i| i.name == crate::partition::DEFAULT_INDEX).unwrap_or(0);
        &t.indexes[at].name
    }

    /// The buckets of `(tenant, index)`; `None` when either is not configured.
    pub fn buckets(&self, tenant: &str, index: &str) -> Option<&LevelBuckets> {
        self.tenants.iter().find(|t| t.id == tenant)?.indexes.iter().find(|i| i.name == index).map(|i| &i.retention)
    }

    /// The buckets of the own tenant's default index.
    pub fn default_buckets(&self) -> &LevelBuckets {
        self.buckets(self.tenant(), self.default_index()).expect("the default index exists")
    }

    /// Replaces the default index's buckets: the runtime policy document
    /// edits these and only these (`crate::policy`).
    pub fn set_default_buckets(&mut self, buckets: LevelBuckets) {
        let name = self.default_index().to_string();
        if let Some(i) = self.tenants[0].indexes.iter_mut().find(|i| i.name == name) {
            i.retention = buckets;
        }
    }

    /// Every `(tenant, index, buckets)` configured.
    pub fn indexes(&self) -> impl Iterator<Item = (&str, &str, &LevelBuckets)> {
        self.tenants.iter().flat_map(|t| t.indexes.iter().map(move |i| (t.id.as_str(), i.name.as_str(), &i.retention)))
    }

    /// Routes a record of the own tenant that asked for `requested`.
    pub fn route(&self, requested: Option<&str>) -> Route<'_> {
        let Some(requested) = requested else { return Route::Known(self.default_index()) };
        match self.own().indexes.iter().find(|i| i.name == requested) {
            Some(i) => Route::Known(&i.name),
            None if self.own().unknown_index == UnknownIndex::Default => Route::Defaulted(self.default_index()),
            None => Route::Refused,
        }
    }

    /// Routes `record` by its [`ATTR_INDEX`] attribute (or `key`, for a
    /// receiver whose protocol names it otherwise) and rewrites the attribute
    /// to the index chosen: absent for the default index, so the common case
    /// costs no bytes in the WAL. Returns `Refused` with the record untouched.
    pub fn route_record(&self, record: &mut crate::model::LogRecord, key: &str) -> Route<'_> {
        let requested = record.attributes.iter().find(|a| a.key == key || a.key == ATTR_INDEX).map(|a| match &a.value {
            crate::model::AttrValue::Str(s) => s.clone(),
            crate::model::AttrValue::I64(n) => n.to_string(),
            crate::model::AttrValue::F64(n) => n.to_string(),
            crate::model::AttrValue::Bool(b) => b.to_string(),
        });
        let route = self.route(requested.as_deref());
        let chosen = match route {
            Route::Known(i) | Route::Defaulted(i) => i,
            Route::Refused => return route,
        };
        record.attributes.retain(|a| a.key != key && a.key != ATTR_INDEX);
        if chosen != self.default_index() {
            record.attributes.push(crate::model::Attr { key: ATTR_INDEX.into(), value: crate::model::AttrValue::Str(chosen.to_string()) });
        }
        route
    }
}

/// Counts of index routing decisions, for `GET /admin/v1/info`.
#[derive(Debug, Default)]
pub struct RouteStats {
    /// Records that named an unknown index and went to the default one.
    pub defaulted: std::sync::atomic::AtomicU64,
    /// Records that named an unknown index and were refused.
    pub refused: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestConfig {
    /// Bounded queue between receivers and the WAL writer. Overflow sheds by
    /// severity; it never blocks the producing application.
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    /// Longest a WARN+ record may wait for queue space before being counted as
    /// a critical drop. Bounded so a stalled disk cannot stall ingest.
    #[serde(default = "default_critical_wait", with = "humantime_serde")]
    pub critical_enqueue_timeout: Duration,
}

fn default_queue_capacity() -> usize {
    65_536
}
fn default_critical_wait() -> Duration {
    Duration::from_millis(250)
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            queue_capacity: default_queue_capacity(),
            critical_enqueue_timeout: default_critical_wait(),
        }
    }
}

/// A severity bucket: the second partition key, and the unit of retention.
///
/// `min_severity` is inclusive; a bucket runs up to the next bucket's
/// `min_severity`. Because a bucket is a directory, retention for it is a
/// directory delete — no per-row bookkeeping, no deletion vectors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelBucket {
    /// Directory name. Must be filesystem-safe and stable — it is on disk.
    pub name: String,
    pub min_severity: u8,
    #[serde(with = "humantime_serde")]
    pub retention: Duration,
}

/// Ordered, non-overlapping, gapless cover of severities 0..=255.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LevelBuckets {
    buckets: Vec<LevelBucket>,
}

impl Default for LevelBuckets {
    /// Coarse default: three buckets. Split further only when a policy needs
    /// different retention for levels that share a bucket — every extra bucket
    /// multiplies the file count per hour.
    fn default() -> Self {
        Self {
            buckets: vec![
                LevelBucket {
                    name: "debug".into(),
                    min_severity: 0,
                    retention: Duration::from_secs(24 * 3600),
                },
                LevelBucket {
                    name: "info".into(),
                    min_severity: Severity::INFO.0,
                    retention: Duration::from_secs(7 * 24 * 3600),
                },
                LevelBucket {
                    name: "warn_plus".into(),
                    min_severity: Severity::WARN.0,
                    retention: Duration::from_secs(30 * 24 * 3600),
                },
            ],
        }
    }
}

/// Why `name` cannot be a bucket, `None` when it can. Names become directory
/// names (`level=<name>`) on any filesystem, so the alphabet is deliberately
/// small: ASCII letters, digits, `_`, `-`, `+`. That excludes path separators,
/// whitespace, control and bidi-format characters, and `. = : * ? " < > |`, and
/// Windows device names are refused because they cannot be created there.
pub fn bucket_name_problem(name: &str) -> Option<&'static str> {
    if name.is_empty() || name.len() > 64 {
        return Some("must be 1 to 64 characters");
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '+')) {
        return Some("may contain only ASCII letters, digits, _ - and + (it is a directory name on disk)");
    }
    let upper = name.to_ascii_uppercase();
    let device = matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((upper.starts_with("COM") || upper.starts_with("LPT"))
            && upper.len() == 4
            && upper.as_bytes()[3].is_ascii_digit()
            && upper.as_bytes()[3] != b'0');
    device.then_some("is a reserved device name on Windows filesystems")
}

impl LevelBuckets {
    pub fn new(mut buckets: Vec<LevelBucket>) -> Result<Self, ConfigError> {
        if buckets.is_empty() {
            return Err(ConfigError::NoBuckets);
        }
        buckets.sort_by_key(|b| b.min_severity);
        if buckets[0].min_severity != 0 {
            return Err(ConfigError::UncoveredSeverity(0));
        }
        for pair in buckets.windows(2) {
            if pair[0].min_severity == pair[1].min_severity {
                return Err(ConfigError::DuplicateMinSeverity(pair[0].min_severity));
            }
        }
        for b in &buckets {
            if bucket_name_problem(&b.name).is_some() {
                return Err(ConfigError::BadBucketName(b.name.clone()));
            }
            if b.retention.is_zero() {
                return Err(ConfigError::ZeroRetention(b.name.clone()));
            }
        }
        // Case-insensitive: the name is a directory, and `INFO` and `info` are
        // one directory on a case-insensitive filesystem (macOS, Windows).
        let mut names: Vec<String> = buckets.iter().map(|b| b.name.to_lowercase()).collect();
        names.sort_unstable();
        if names.windows(2).any(|w| w[0] == w[1]) {
            return Err(ConfigError::DuplicateBucketName);
        }
        Ok(Self { buckets })
    }

    pub fn iter(&self) -> impl Iterator<Item = &LevelBucket> {
        self.buckets.iter()
    }

    /// Bucket owning this severity. Total by construction — bucket 0 covers 0.
    pub fn bucket_for(&self, severity: Severity) -> &LevelBucket {
        let idx = self
            .buckets
            .partition_point(|b| b.min_severity <= severity.0)
            .saturating_sub(1);
        &self.buckets[idx]
    }

    pub fn by_name(&self, name: &str) -> Option<&LevelBucket> {
        self.buckets.iter().find(|b| b.name == name)
    }

    /// Names of the buckets that can hold a record of `severity` or above:
    /// the `level=` directories a "this level and up" read has to open.
    pub fn names_from(&self, severity: Severity) -> Vec<String> {
        let first = self.buckets.partition_point(|b| b.min_severity <= severity.0).saturating_sub(1);
        self.buckets[first..].iter().map(|b| b.name.clone()).collect()
    }

    /// Longest retention of any bucket — the horizon the store must plan for.
    pub fn max_retention(&self) -> Duration {
        self.buckets
            .iter()
            .map(|b| b.retention)
            .max()
            .unwrap_or_default()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("retention must define at least one level bucket")]
    NoBuckets,
    #[error("severity {0} is not covered by any level bucket; the lowest bucket must start at 0")]
    UncoveredSeverity(u8),
    #[error("two level buckets share min_severity {0}")]
    DuplicateMinSeverity(u8),
    #[error("two level buckets share a name")]
    DuplicateBucketName,
    #[error("level bucket name {0:?} is not a safe directory name (1 to 64 ASCII letters, digits, _ - +; no Windows device names)")]
    BadBucketName(String),
    #[error("level bucket {0:?} has zero retention; remove the bucket instead")]
    ZeroRetention(String),
    #[error("configure at least one [[tenants]] entry, or none at all for the single tenant `default`")]
    NoTenants,
    #[error("tenant {0:?} has no indexes; give it at least one [[tenants.indexes]]")]
    NoIndexes(String),
    #[error("{what} {value:?} is not usable: 1 to 64 of A-Za-z0-9._-, not starting with a dot, with at least one letter (it is a directory name, and a number-only name is read back as a number)")]
    BadIdent { what: &'static str, value: String },
    #[error("{what} {value:?} is used twice (names are compared ignoring case: they are directories)")]
    DuplicateIdent { what: &'static str, value: String },
    #[error("cannot keep the generated node id in {path}: {message}")]
    NodeId { path: String, message: String },
    #[error("unsupported config schema_version {found}, expected {expected}")]
    UnsupportedSchema { found: u32, expected: u32 },
    #[error("invalid policy value at {path}: {message}")]
    Policy { path: String, message: String },
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(ConfigError::UnsupportedSchema {
                found: self.schema_version,
                expected: SCHEMA_VERSION,
            });
        }
        self.sync_parts.check().map_err(|message| ConfigError::Policy { path: "sync_parts.interval".into(), message })?;
        // Re-run tenant, index and bucket invariants: a config deserialised
        // from TOML bypasses `Tenancy::new` and `LevelBuckets::new`.
        Tenancy::new(self.tenants.clone(), self.node.id.clone().unwrap_or_else(|| "node-a".into()))?;
        // The runtime policy API edits these same values, so the file must
        // satisfy the same rules or the first API read would be an invalid
        // document nobody can PUT back.
        if let Some(p) = crate::policy::Policies::from_config(self).validate().into_iter().next() {
            return Err(ConfigError::Policy { path: p.path, message: p.message });
        }
        Ok(())
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.storage.data_dir.join("catalog.sqlite")
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.storage.data_dir.join("wal")
    }

    pub fn store_dir(&self) -> PathBuf {
        self.storage.data_dir.join("store")
    }

    /// The late spool: a second WAL for records whose event time was already
    /// old when they arrived (`storage.late_threshold`).
    pub fn wal_late_dir(&self) -> PathBuf {
        crate::wal::late_dir_beside(&self.wal_dir())
    }

    /// This instance's node id: `[node] id`, or one generated on first use
    /// and kept in `<data_dir>/node-id` so it never changes afterwards
    /// (`node-` and twelve hex digits of the instance id).
    pub fn node_id(&self) -> Result<String, ConfigError> {
        if let Some(id) = &self.node.id {
            return Ok(id.clone());
        }
        let path = self.storage.data_dir.join("node-id");
        if let Ok(existing) = std::fs::read_to_string(&path) {
            let existing = existing.trim();
            if crate::partition::valid_ident(existing) {
                return Ok(existing.to_string());
            }
        }
        let instance = crate::peer::instance_id(&self.storage.data_dir);
        let id = format!("node-{}", &instance[instance.len().saturating_sub(12)..]);
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &id)
            .and_then(|()| std::fs::rename(&tmp, &path))
            .map_err(|e| ConfigError::NodeId { path: path.display().to_string(), message: e.to_string() })?;
        Ok(id)
    }

    /// The tenants and indexes of this config with this instance's node id.
    pub fn tenancy(&self) -> Result<Tenancy, ConfigError> {
        Tenancy::new(self.tenants.clone(), self.node_id()?)
    }

    /// The buckets of the own tenant's default index, which is what the
    /// runtime policy document calls `retention`.
    pub fn default_buckets(&self) -> &LevelBuckets {
        let t = &self.tenants[0];
        let at = t.indexes.iter().position(|i| i.name == crate::partition::DEFAULT_INDEX).unwrap_or(0);
        &t.indexes[at].retention
    }

    pub fn set_default_buckets(&mut self, buckets: LevelBuckets) {
        let t = &mut self.tenants[0];
        let at = t.indexes.iter().position(|i| i.name == crate::partition::DEFAULT_INDEX).unwrap_or(0);
        t.indexes[at].retention = buckets;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_config_can_be_extended_by_appending() {
        // `logless init > c.toml` then appending a destination must parse. An
        // empty `destinations = []` in the root table breaks that.
        let generated = toml::to_string_pretty(&Config {
            schema_version: SCHEMA_VERSION,
            storage: StorageConfig {
                data_dir: "/tmp/x".into(),
                disk_budget_bytes: 1,
                high_water: 0.8,
                critical_water: 0.95,
                reclaim_target: 0.7,
                wal_segment_bytes: 1,
                wal_fsync_interval: Duration::from_millis(1),
                wal_fsync_bytes: 1,
                late_threshold: default_late_threshold(),
                late_roll: default_late_roll(),
                late_segment_bytes: default_late_segment_bytes(),
                compact_quiet: default_compact_quiet(),
            },
            ingest: IngestConfig::default(),
            node: NodeConfig::default(),
            pushdown: PushdownConfig::default(),
            typed_columns: crate::typed::TypedColumnsConfig::default(),
            otlp: crate::otlp::ReceiverConfig::default(),
            hec: crate::hec::HecConfig::default(),
            serve_s3: crate::s3_serve::S3ServeConfig::default(),
            admin: crate::admin_api::AdminConfig::default(),
            sentry: crate::sentry::SentryConfig::default(),
            peer: crate::peer::PeerConfig::default(),
            sync_parts: Default::default(),
            decision: crate::decision::DecisionConfig::default(),
            tenants: default_tenants(),
            tail: Vec::new(),
            destinations: Vec::new(),
        })
        .unwrap();
        assert!(
            !generated.contains("destinations = []"),
            "empty destinations must not be serialised:\n{generated}"
        );

        let extended = format!(
            "{generated}\n[[destinations]]\ntype = \"sentry\"\ndsn = \"https://k@h/1\"\n"
        );
        let parsed: Config = toml::from_str(&extended).expect("appended config must parse");
        assert_eq!(parsed.destinations.len(), 1);
        // The generated file spells out the single default tenant and index.
        assert!(generated.contains("[[tenants]]\nid = \"default\""), "{generated}");
        assert!(generated.contains("[[tenants.indexes]]\nname = \"default\""), "{generated}");
        assert!(generated.contains("[[tenants.indexes.retention]]\nname = \"debug\""), "{generated}");
        assert_eq!(parsed.tenants, default_tenants());
    }

    #[test]
    fn a_config_without_tenants_is_one_default_tenant_and_index() {
        let c: Config = toml::from_str(MINIMAL).unwrap();
        c.validate().unwrap();
        assert_eq!(c.tenants, default_tenants());
        assert_eq!(c.default_buckets(), &LevelBuckets::default());
        assert_eq!(c.storage.late_threshold, Duration::from_secs(7200));
        assert_eq!(c.storage.compact_quiet, Duration::from_secs(600));
        // The old global table is gone, not silently ignored.
        assert!(toml::from_str::<Config>(&format!("{MINIMAL}\n[[retention]]\nname = \"debug\"\nmin_severity = 0\nretention = \"1d\"\n")).is_err());
    }

    const TWO_INDEXES: &str = r#"
[node]
id = "edge-1"

[[tenants]]
id = "acme"
unknown_index = "refuse"

[[tenants.indexes]]
name = "default"

[[tenants.indexes]]
name = "payments"

[[tenants.indexes.retention]]
name = "all"
min_severity = 0
retention = "400d"
"#;

    #[test]
    fn tenants_indexes_and_node_parse_and_route() {
        let c: Config = toml::from_str(&format!("{MINIMAL}{TWO_INDEXES}")).unwrap();
        c.validate().unwrap();
        let t = c.tenancy().unwrap();
        assert_eq!((t.tenant(), t.node(), t.default_index()), ("acme", "edge-1", "default"));
        assert_eq!(t.buckets("acme", "payments").unwrap().iter().map(|b| b.name.as_str()).collect::<Vec<_>>(), ["all"]);
        assert_eq!(t.buckets("acme", "default"), Some(&LevelBuckets::default()));
        assert_eq!(t.buckets("acme", "absent"), None);
        assert_eq!(t.buckets("other", "default"), None);
        assert_eq!(t.route(None), Route::Known("default"));
        assert_eq!(t.route(Some("payments")), Route::Known("payments"));
        assert_eq!(t.route(Some("nope")), Route::Refused);

        let lenient: Config = toml::from_str(&format!("{MINIMAL}{}", TWO_INDEXES.replace("refuse", "default"))).unwrap();
        assert_eq!(lenient.tenancy().unwrap().route(Some("nope")), Route::Defaulted("default"));

        // Routing a record leaves the attribute only for a non-default index.
        let attr = |v: &str| crate::model::Attr { key: "index".into(), value: crate::model::AttrValue::Str(v.into()) };
        let mut r = crate::model::LogRecord::new(1, Severity::INFO, "x");
        r.attributes = vec![attr("payments")];
        assert_eq!(t.route_record(&mut r, "index"), Route::Known("payments"));
        assert_eq!(r.attributes, vec![crate::model::Attr { key: ATTR_INDEX.into(), value: crate::model::AttrValue::Str("payments".into()) }]);
        r.attributes = vec![attr("default")];
        assert_eq!(t.route_record(&mut r, "index"), Route::Known("default"));
        assert!(r.attributes.is_empty());
        r.attributes = vec![attr("nope")];
        assert_eq!(t.route_record(&mut r, "index"), Route::Refused);
        assert_eq!(r.attributes.len(), 1, "a refused record is left as it came");
    }

    #[test]
    fn bad_tenant_index_and_node_names_are_refused() {
        let with = |from: &str, to: &str| toml::from_str::<Config>(&format!("{MINIMAL}{}", TWO_INDEXES.replace(from, to))).unwrap().validate();
        assert!(matches!(with("id = \"acme\"", "id = \"1234\""), Err(ConfigError::BadIdent { what: "tenant id", .. })));
        assert!(matches!(with("name = \"payments\"", "name = \"a/b\""), Err(ConfigError::BadIdent { what: "index name", .. })));
        assert!(matches!(with("name = \"payments\"", "name = \"DEFAULT\""), Err(ConfigError::DuplicateIdent { .. })));
        assert!(matches!(with("id = \"edge-1\"", "id = \"42\""), Err(ConfigError::BadIdent { what: "node id", .. })));
        assert!(toml::from_str::<Config>(&format!("{MINIMAL}{}", TWO_INDEXES.replace("refuse", "sometimes"))).is_err());
    }

    #[test]
    fn the_node_id_is_generated_once_and_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let c: Config = toml::from_str(&format!("[storage]\ndata_dir = {:?}\n", tmp.path())).unwrap();
        let first = c.node_id().unwrap();
        assert!(first.starts_with("node-") && crate::partition::valid_ident(&first), "{first}");
        assert_eq!(first, c.node_id().unwrap());
        assert_eq!(first, std::fs::read_to_string(tmp.path().join("node-id")).unwrap());
        // A configured id wins and writes nothing.
        let named: Config = toml::from_str(&format!("[storage]\ndata_dir = {:?}\n[node]\nid = \"edge-7\"\n", tmp.path().join("b"))).unwrap();
        assert_eq!("edge-7", named.node_id().unwrap());
        assert!(!tmp.path().join("b").exists());
    }

    const MINIMAL: &str = "[storage]\ndata_dir = \"/tmp/x\"\n";

    #[test]
    fn serve_s3_is_off_by_default_and_parses_when_given() {
        let c: Config = toml::from_str(MINIMAL).unwrap();
        assert!(!c.serve_s3.enabled);

        let c: Config = toml::from_str(&format!(
            "{MINIMAL}\n[serve_s3]\nenabled = true\naddr = \"127.0.0.1:9100\"\nbucket = \"b\"\naccess_key = \"ak\"\nsecret_key = \"sk\"\n"
        ))
        .unwrap();
        assert!(c.serve_s3.enabled);
        assert_eq!(c.serve_s3.addr, "127.0.0.1:9100");
        assert_eq!(c.serve_s3.bucket, "b");
        assert_eq!(c.serve_s3.access_key.as_deref(), Some("ak"));

        let back: Config = toml::from_str(&toml::to_string_pretty(&c).unwrap()).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn serve_s3_starts_and_stops_from_a_parsed_config() {
        use std::io::{Read, Write};
        let tmp = tempfile::tempdir().unwrap();
        let mut c: Config = toml::from_str(&format!(
            "[storage]\ndata_dir = {:?}\n\n[serve_s3]\nenabled = true\naddr = \"127.0.0.1:0\"\n",
            tmp.path()
        ))
        .unwrap();
        c.serve_s3.access_key = None;
        let server = crate::s3_serve::S3Server::start(&c.serve_s3, c.store_dir()).unwrap();
        let addr = server.local_addr();
        let mut conn = std::net::TcpStream::connect(addr).unwrap();
        conn.write_all(b"GET /logs?list-type=2 HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").unwrap();
        let mut out = String::new();
        conn.read_to_string(&mut out).unwrap();
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        server.shutdown();
        assert!(std::net::TcpStream::connect(addr).is_err());
    }

    #[test]
    fn serve_s3_rejects_unknown_keys() {
        let err = toml::from_str::<Config>(&format!("{MINIMAL}\n[serve_s3]\nbogus = 1\n"));
        assert!(err.is_err());
    }

    #[test]
    fn default_buckets_cover_every_severity() {
        let b = LevelBuckets::default();
        assert_eq!(b.bucket_for(Severity(0)).name, "debug");
        assert_eq!(b.bucket_for(Severity::TRACE).name, "debug");
        assert_eq!(b.bucket_for(Severity(8)).name, "debug");
        assert_eq!(b.bucket_for(Severity::INFO).name, "info");
        assert_eq!(b.bucket_for(Severity(12)).name, "info");
        assert_eq!(b.bucket_for(Severity::WARN).name, "warn_plus");
        assert_eq!(b.bucket_for(Severity::FATAL).name, "warn_plus");
        assert_eq!(b.bucket_for(Severity(255)).name, "warn_plus");
    }

    #[test]
    fn finer_buckets_split_error_from_warn() {
        // The point of config-driven buckets: 90d on error, 14d on warn, which
        // a fixed three-bucket scheme cannot express.
        let b = LevelBuckets::new(vec![
            LevelBucket {
                name: "error".into(),
                min_severity: 17,
                retention: Duration::from_secs(90 * 24 * 3600),
            },
            LevelBucket {
                name: "trace".into(),
                min_severity: 0,
                retention: Duration::from_secs(6 * 3600),
            },
            LevelBucket {
                name: "warn".into(),
                min_severity: 13,
                retention: Duration::from_secs(14 * 24 * 3600),
            },
        ])
        .unwrap();

        assert_eq!(b.bucket_for(Severity::DEBUG).name, "trace");
        assert_eq!(b.bucket_for(Severity::INFO).name, "trace");
        assert_eq!(b.bucket_for(Severity::WARN).name, "warn");
        assert_eq!(b.bucket_for(Severity::ERROR).name, "error");
        assert_eq!(b.max_retention(), Duration::from_secs(90 * 24 * 3600));
    }

    #[test]
    fn rejects_gaps_and_bad_names() {
        let missing_zero = LevelBuckets::new(vec![LevelBucket {
            name: "info".into(),
            min_severity: 9,
            retention: Duration::from_secs(60),
        }]);
        assert!(matches!(
            missing_zero,
            Err(ConfigError::UncoveredSeverity(0))
        ));

        let bad_name = LevelBuckets::new(vec![LevelBucket {
            name: "a/b".into(),
            min_severity: 0,
            retention: Duration::from_secs(60),
        }]);
        assert!(matches!(bad_name, Err(ConfigError::BadBucketName(_))));
    }
}
