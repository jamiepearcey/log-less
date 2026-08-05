//! log-less core: data model, config, WAL, partitioning, retention.
//!
//! Build order follows risk, not features (`docs/tasks/current.md`). What lives
//! here is the part that must be right before anything else is worth writing:
//! the agent must never block the application and must account for every record
//! it accepts.

pub mod budget;
pub mod catalog;
pub mod compact;
pub mod config;
pub mod model;
pub mod partition;
pub mod drain;
pub mod forward;
pub mod hec;
pub mod httpd;
pub mod mask;
pub mod merge;
pub mod otlp;
pub mod queue;
pub mod scan;
pub mod schema;
pub mod counters;
pub mod spool;
pub mod sentry;
pub mod store;
pub mod tail;
pub mod retention;
pub mod ring;
pub mod wal;

pub use catalog::Catalog;
pub use config::{Config, LevelBucket, LevelBuckets, StorageConfig};
pub use model::{Attr, AttrValue, LevelClass, LogRecord, Severity};
pub use partition::PartitionKey;
pub use queue::{Admission, IngestQueue, StatsSnapshot};
pub use wal::{WalError, WalWriter};

/// Wall-clock seconds since the Unix epoch.
///
/// Every other module takes time as a parameter so it stays testable; this is
/// the single place the real clock is read.
pub fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn now_unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
