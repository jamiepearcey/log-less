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
    #[serde(default)]
    pub retention: LevelBuckets,
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
    /// An optional decision model that suggests which templates are the same failure (`docs/decision-model.md`). Off unless `url` is set.
    #[serde(default)]
    pub decision: crate::decision::DecisionConfig,
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
        // Re-run bucket invariants: a config deserialised from TOML bypasses
        // LevelBuckets::new.
        LevelBuckets::new(self.retention.buckets.clone())?;
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
            },
            ingest: IngestConfig::default(),
            retention: LevelBuckets::default(),
            pushdown: PushdownConfig::default(),
            typed_columns: crate::typed::TypedColumnsConfig::default(),
            otlp: crate::otlp::ReceiverConfig::default(),
            hec: crate::hec::HecConfig::default(),
            serve_s3: crate::s3_serve::S3ServeConfig::default(),
            admin: crate::admin_api::AdminConfig::default(),
            sentry: crate::sentry::SentryConfig::default(),
            peer: crate::peer::PeerConfig::default(),
            decision: crate::decision::DecisionConfig::default(),
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
