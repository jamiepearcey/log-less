//! Retention enforcement.
//!
//! Retention is a **directory delete**, per level bucket. No per-row
//! bookkeeping, no deletion vectors — those are for compliance deletes, not
//! time expiry (`docs/architecture.md` §2).
//!
//! A partition expires when the hour it covers has fully elapsed *and* the
//! bucket's retention has passed since then:
//!
//! ```text
//! expired  <=>  partition_end + retention <= now
//! ```
//!
//! Safety rules, because this deletes user data:
//!   * only directories matching the exact `hour=…/level=…` shape are touched;
//!   * a bucket name absent from the current config is never deleted — a config
//!     edit must not silently destroy data under the old bucket name;
//!   * `dry_run` produces an identical report without deleting anything.

use std::fs;
use std::path::{Path, PathBuf};

use crate::config::LevelBuckets;
use crate::partition::{parse_partition_dirs, PartitionKey};

#[derive(Debug, Default, PartialEq)]
pub struct RetentionReport {
    pub expired: Vec<ExpiredPartition>,
    pub bytes_reclaimed: u64,
    /// Partitions whose bucket name is not in the current config. Left alone
    /// deliberately; surfaced so an operator can decide.
    pub orphan_buckets: Vec<PartitionKey>,
    /// Paths we failed to remove. Retention is best-effort per partition — one
    /// stuck directory must not stop the rest expiring.
    pub errors: Vec<(PathBuf, String)>,
}

#[derive(Debug, PartialEq)]
pub struct ExpiredPartition {
    pub key: PartitionKey,
    pub path: PathBuf,
    pub bytes: u64,
}

/// Delete every partition past its bucket's retention.
///
/// `now_unix_secs` is injected rather than read from the clock so this is
/// testable and so a replay/backfill run can reason about a past instant.
pub fn enforce(
    store_dir: &Path,
    buckets: &LevelBuckets,
    now_unix_secs: u64,
    dry_run: bool,
) -> RetentionReport {
    let mut report = RetentionReport::default();

    let hour_dirs = match fs::read_dir(store_dir) {
        Ok(d) => d,
        // No store yet is not an error — nothing has been committed.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return report,
        Err(e) => {
            report.errors.push((store_dir.to_path_buf(), e.to_string()));
            return report;
        }
    };

    for hour_entry in hour_dirs.flatten() {
        if !hour_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let hour_name = hour_entry.file_name();
        let Some(hour_name) = hour_name.to_str() else {
            continue;
        };
        // Establish this really is a partition directory before anything below
        // is allowed to delete inside it. Without this the tidy step at the end
        // will happily remove any empty directory a user left in the store.
        if hour_name
            .strip_prefix("hour=")
            .and_then(crate::partition::parse_hour)
            .is_none()
        {
            continue;
        }

        let level_dirs = match fs::read_dir(hour_entry.path()) {
            Ok(d) => d,
            Err(e) => {
                report.errors.push((hour_entry.path(), e.to_string()));
                continue;
            }
        };

        for level_entry in level_dirs.flatten() {
            if !level_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let level_name = level_entry.file_name();
            let Some(level_name) = level_name.to_str() else {
                continue;
            };
            let Some(key) = parse_partition_dirs(hour_name, level_name) else {
                continue;
            };

            let Some(bucket) = buckets.by_name(&key.bucket) else {
                report.orphan_buckets.push(key);
                continue;
            };

            let expires_at = key.end_unix_secs().saturating_add(bucket.retention.as_secs());
            if expires_at > now_unix_secs {
                continue;
            }

            let path = level_entry.path();
            let bytes = dir_size(&path);
            if !dry_run {
                if let Err(e) = fs::remove_dir_all(&path) {
                    report.errors.push((path, e.to_string()));
                    continue;
                }
            }
            report.bytes_reclaimed += bytes;
            report.expired.push(ExpiredPartition { key, path, bytes });
        }

        // Tidy: an hour directory with no level directories left is noise.
        if !dry_run {
            let empty = fs::read_dir(hour_entry.path())
                .map(|mut d| d.next().is_none())
                .unwrap_or(false);
            if empty {
                let _ = fs::remove_dir(hour_entry.path());
            }
        }
    }

    report.expired.sort_by(|a, b| a.key.cmp(&b.key));
    report
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        match entry.file_type() {
            Ok(t) if t.is_dir() => total += dir_size(&entry.path()),
            Ok(t) if t.is_file() => total += entry.metadata().map(|m| m.len()).unwrap_or(0),
            _ => {}
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LevelBucket;
    use crate::partition::SECS_PER_HOUR;
    use std::time::Duration;

    fn write_partition(store: &Path, epoch_hour: u64, bucket: &str, bytes: usize) -> PathBuf {
        let key = PartitionKey {
            epoch_hour,
            bucket: bucket.to_string(),
        };
        let dir = key.dir(store);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("000001.parquet"), vec![0u8; bytes]).unwrap();
        dir
    }

    #[test]
    fn expires_per_bucket_not_globally() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let buckets = LevelBuckets::default(); // debug 1d, info 7d, warn_plus 30d

        // All three partitions cover hour 100; "now" is 2 days after it ended.
        let debug = write_partition(store, 100, "debug", 10);
        let info = write_partition(store, 100, "info", 20);
        let warn = write_partition(store, 100, "warn_plus", 30);
        let now = 101 * SECS_PER_HOUR + 2 * 24 * 3600;

        let report = enforce(store, &buckets, now, false);

        assert_eq!(report.expired.len(), 1, "only debug is past 1d");
        assert_eq!(report.expired[0].key.bucket, "debug");
        assert_eq!(report.bytes_reclaimed, 10);
        assert!(!debug.exists());
        assert!(info.exists(), "info retained for 7d");
        assert!(warn.exists(), "warn_plus retained for 30d");
    }

    #[test]
    fn partition_survives_until_its_hour_has_fully_elapsed() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let buckets = LevelBuckets::new(vec![LevelBucket {
            name: "debug".into(),
            min_severity: 0,
            retention: Duration::from_secs(SECS_PER_HOUR),
        }])
        .unwrap();

        write_partition(store, 100, "debug", 1);

        // Hour 100 ends at 101h; +1h retention => expires at 102h exactly.
        let just_before = enforce(store, &buckets, 102 * SECS_PER_HOUR - 1, true);
        assert!(just_before.expired.is_empty());

        let exactly = enforce(store, &buckets, 102 * SECS_PER_HOUR, true);
        assert_eq!(exactly.expired.len(), 1);
    }

    #[test]
    fn dry_run_reports_without_deleting() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let dir = write_partition(store, 0, "debug", 42);

        let report = enforce(store, &LevelBuckets::default(), u64::MAX / 2, true);
        assert_eq!(report.expired.len(), 1);
        assert_eq!(report.bytes_reclaimed, 42);
        assert!(dir.exists(), "dry run must not delete");
    }

    #[test]
    fn unknown_bucket_is_never_deleted() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        // Written under an old config that had a "trace" bucket.
        let dir = write_partition(store, 0, "trace", 5);

        let report = enforce(store, &LevelBuckets::default(), u64::MAX / 2, false);
        assert!(dir.exists(), "config edits must not destroy data");
        assert_eq!(report.expired.len(), 0);
        assert_eq!(report.orphan_buckets.len(), 1);
        assert_eq!(report.orphan_buckets[0].bucket, "trace");
    }

    #[test]
    fn ignores_foreign_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        fs::create_dir_all(store.join("hour=nonsense/level=debug")).unwrap();
        fs::create_dir_all(store.join("scratch")).unwrap();
        fs::create_dir_all(store.join("hour=1970-01-01T00/notalevel")).unwrap();
        fs::write(store.join("catalog.sqlite"), b"x").unwrap();

        let report = enforce(store, &LevelBuckets::default(), u64::MAX / 2, false);
        assert!(report.expired.is_empty());
        // An empty directory a user left in the store is not ours to remove.
        assert!(store.join("scratch").exists());
        assert!(store.join("catalog.sqlite").exists());
        assert!(store.join("hour=nonsense/level=debug").exists());
        assert!(store.join("hour=1970-01-01T00/notalevel").exists());
    }

    #[test]
    fn missing_store_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let report = enforce(&tmp.path().join("absent"), &LevelBuckets::default(), 0, false);
        assert_eq!(report, RetentionReport::default());
    }
}
