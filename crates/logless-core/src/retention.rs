//! Retention enforcement.
//!
//! Retention is a **directory delete**, per `(tenant, index, level bucket)`.
//! No per-row bookkeeping, no deletion vectors — those are for compliance
//! deletes, not time expiry (`docs/architecture.md` §2).
//!
//! A partition expires when the hour it covers has fully elapsed *and* its
//! bucket's retention, under its index, has passed since then:
//!
//! ```text
//! expired  <=>  partition_end + retention <= now
//! ```
//!
//! The hour is the **event** hour, so retention is on event time: a row
//! replayed today with a timestamp older than the horizon lands in a
//! partition that is already past it and goes with the next pass.
//!
//! What is deleted is `tenant=…/index=…/hour=…/level=…`, which holds one
//! `node=` leaf per writer, so one delete takes every writer's files.
//!
//! Safety rules, because this deletes user data:
//!   * only directories matching the exact `tenant=/index=/hour=/level=` shape
//!     are touched;
//!   * a tenant, index or bucket absent from the current config is never
//!     deleted — a config edit must not silently destroy data under the old
//!     name;
//!   * `dry_run` produces an identical report without deleting anything.

use std::fs;
use std::path::{Path, PathBuf};

use crate::config::Tenancy;
use crate::parts_edge::PinSet;
use crate::partition::{parse_hour, valid_ident, PartitionKey};

#[derive(Debug, Default, PartialEq)]
pub struct RetentionReport {
    pub expired: Vec<ExpiredPartition>,
    pub bytes_reclaimed: u64,
    /// Partitions whose tenant, index or bucket is not in the current config.
    /// Left alone deliberately; surfaced so an operator can decide.
    pub orphan_buckets: Vec<PartitionKey>,
    /// Paths we failed to remove. Retention is best-effort per partition — one
    /// stuck directory must not stop the rest expiring.
    pub errors: Vec<(PathBuf, String)>,
    /// Files past retention that were kept because they are pinned (`parts_edge`): central has not
    /// confirmed them yet.
    pub pinned_kept: usize,
}

/// One expired `tenant=/index=/hour=/level=` directory. `key.node` is empty:
/// the directory holds every writer's leaf.
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
    tenancy: &Tenancy,
    now_unix_secs: u64,
    dry_run: bool,
) -> RetentionReport {
    enforce_pinned(store_dir, tenancy, now_unix_secs, dry_run, &PinSet::default())
}

/// The sub-directories of `dir` named `<prefix><value>`, with the value.
fn children(dir: &Path, prefix: &str, errors: &mut Vec<(PathBuf, String)>) -> Vec<(PathBuf, String)> {
    let entries = match fs::read_dir(dir) {
        Ok(d) => d,
        Err(e) => {
            errors.push((dir.to_path_buf(), e.to_string()));
            return Vec::new();
        }
    };
    let mut out: Vec<(PathBuf, String)> = entries
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter_map(|e| {
            let value = e.file_name().to_str()?.strip_prefix(prefix)?.to_string();
            Some((e.path(), value))
        })
        .collect();
    out.sort();
    out
}

/// [`enforce`], except that a pinned file is never deleted: its partition keeps just those files and is
/// expired once they are unpinned (uploaded, or past the pin limit).
pub fn enforce_pinned(
    store_dir: &Path,
    tenancy: &Tenancy,
    now_unix_secs: u64,
    dry_run: bool,
    pins: &PinSet,
) -> RetentionReport {
    let mut report = RetentionReport::default();
    // No store yet is not an error — nothing has been committed.
    if !store_dir.exists() {
        return report;
    }

    // Each level establishes that the directory really is one of ours before
    // anything below is allowed to delete inside it. Without this the tidy
    // step at the end would remove any empty directory a user left in the store.
    for (tenant_dir, tenant) in children(store_dir, "tenant=", &mut report.errors) {
        if !valid_ident(&tenant) {
            continue;
        }
        for (index_dir, index) in children(&tenant_dir, "index=", &mut report.errors) {
            if !valid_ident(&index) {
                continue;
            }
            let buckets = tenancy.buckets(&tenant, &index);
            for (hour_dir, hour) in children(&index_dir, "hour=", &mut report.errors) {
                let Some(epoch_hour) = parse_hour(&hour) else { continue };
                for (path, level) in children(&hour_dir, "level=", &mut report.errors) {
                    if level.is_empty() {
                        continue;
                    }
                    let key = PartitionKey::new(&tenant, &index, epoch_hour, &level, "");
                    let Some(bucket) = buckets.and_then(|b| b.by_name(&level)) else {
                        report.orphan_buckets.push(key);
                        continue;
                    };

                    let expires_at = key.end_unix_secs().saturating_add(bucket.retention.as_secs());
                    if expires_at > now_unix_secs {
                        continue;
                    }

                    let mut bytes = dir_size(&path);
                    if pins.pinned.iter().any(|p| p.starts_with(&path)) {
                        // Delete what is not pinned, one file at a time, and leave the directory.
                        let (freed, kept) = remove_unpinned(&path, pins, dry_run, &mut report.errors);
                        report.pinned_kept += kept;
                        bytes = freed;
                        if bytes == 0 {
                            continue;
                        }
                    } else if !dry_run {
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
                    let empty = fs::read_dir(&hour_dir).map(|mut d| d.next().is_none()).unwrap_or(false);
                    if empty {
                        let _ = fs::remove_dir(&hour_dir);
                    }
                }
            }
        }
    }

    report.expired.sort_by(|a, b| a.key.cmp(&b.key));
    report
}

/// Removes the files under a partition directory that are not pinned, in every writer's leaf. Returns the bytes
/// freed and the pinned files kept. A leaf left empty is removed.
fn remove_unpinned(dir: &Path, pins: &PinSet, dry_run: bool, errors: &mut Vec<(PathBuf, String)>) -> (u64, usize) {
    let (mut freed, mut kept) = (0, 0);
    let Ok(entries) = fs::read_dir(dir) else { return (0, 0) };
    for entry in entries.flatten() {
        let path = entry.path();
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            let (f, k) = remove_unpinned(&path, pins, dry_run, errors);
            freed += f;
            kept += k;
            if k == 0 && !dry_run {
                let _ = fs::remove_dir(&path);
            }
            continue;
        }
        if pins.contains(&path) {
            kept += 1;
            continue;
        }
        let bytes = entry.metadata().map(|m| m.len()).unwrap_or(0);
        let removed = dry_run || fs::remove_file(&path).map_err(|e| errors.push((path.clone(), e.to_string()))).is_ok();
        if removed {
            freed += bytes;
        }
    }
    (freed, kept)
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
    use crate::config::{IndexConfig, LevelBucket, LevelBuckets, TenantConfig, UnknownIndex};
    use crate::partition::SECS_PER_HOUR;
    use std::time::Duration;

    /// A partition of the default tenant and index with one file in `node-a`'s leaf.
    fn write_partition(store: &Path, epoch_hour: u64, bucket: &str, bytes: usize) -> PathBuf {
        write_in(store, &PartitionKey::at(epoch_hour, bucket), bytes)
    }

    /// Writes one file in `key`'s leaf and returns the level directory retention deletes.
    fn write_in(store: &Path, key: &PartitionKey, bytes: usize) -> PathBuf {
        fs::create_dir_all(key.dir(store)).unwrap();
        fs::write(key.dir(store).join("000001.parquet"), vec![0u8; bytes]).unwrap();
        key.level_dir(store)
    }

    fn one_bucket(retention_secs: u64) -> LevelBuckets {
        LevelBuckets::new(vec![LevelBucket { name: "debug".into(), min_severity: 0, retention: Duration::from_secs(retention_secs) }]).unwrap()
    }

    #[test]
    fn expires_per_bucket_not_globally() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let tenancy = Tenancy::default(); // debug 1d, info 7d, warn_plus 30d

        // All three partitions cover hour 100; "now" is 2 days after it ended.
        let debug = write_partition(store, 100, "debug", 10);
        let info = write_partition(store, 100, "info", 20);
        let warn = write_partition(store, 100, "warn_plus", 30);
        let now = 101 * SECS_PER_HOUR + 2 * 24 * 3600;

        let report = enforce(store, &tenancy, now, false);

        assert_eq!(report.expired.len(), 1, "only debug is past 1d");
        assert_eq!(report.expired[0].key.bucket, "debug");
        assert_eq!(report.bytes_reclaimed, 10);
        assert!(!debug.exists());
        assert!(info.exists(), "info retained for 7d");
        assert!(warn.exists(), "warn_plus retained for 30d");
    }

    #[test]
    fn each_index_expires_by_its_own_buckets_and_one_delete_takes_every_writers_files() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let tenancy = Tenancy::new(
            vec![
                TenantConfig {
                    id: "acme".into(),
                    unknown_index: UnknownIndex::Default,
                    indexes: vec![
                        IndexConfig { name: "default".into(), retention: one_bucket(24 * 3600) },
                        IndexConfig { name: "payments".into(), retention: one_bucket(400 * 24 * 3600) },
                    ],
                },
                TenantConfig { id: "globex".into(), unknown_index: UnknownIndex::Default, indexes: vec![IndexConfig { name: "default".into(), retention: one_bucket(3 * 24 * 3600) }] },
            ],
            "central-1",
        )
        .unwrap();
        // Hour 100 everywhere; "now" is 2 days after it ended.
        let short = write_in(store, &PartitionKey::new("acme", "default", 100, "debug", "edge-1"), 10);
        write_in(store, &PartitionKey::new("acme", "default", 100, "debug", "edge-2"), 7);
        let long = write_in(store, &PartitionKey::new("acme", "payments", 100, "debug", "edge-1"), 20);
        let other = write_in(store, &PartitionKey::new("globex", "default", 100, "debug", "edge-9"), 30);
        let now = 101 * SECS_PER_HOUR + 2 * 24 * 3600;

        let report = enforce(store, &tenancy, now, false);
        assert_eq!(report.expired.len(), 1);
        let gone = &report.expired[0];
        assert_eq!((gone.key.tenant.as_str(), gone.key.index.as_str(), gone.bytes), ("acme", "default", 17), "both writers' leaves");
        assert!(!short.exists() && long.exists() && other.exists());
        assert!(store.join("tenant=acme/index=default").exists(), "the index directory stays");
        assert!(!store.join("tenant=acme/index=default/hour=1970-01-05T04").exists(), "the emptied hour is tidied");

        // A day later the other tenant's three days are up too; payments keeps its 400.
        let report = enforce(store, &tenancy, now + 24 * 3600, false);
        assert_eq!(report.expired.iter().map(|e| e.key.tenant.as_str()).collect::<Vec<_>>(), ["globex"]);
        assert!(long.exists());
    }

    #[test]
    fn a_replayed_old_row_is_deleted_on_the_next_pass() {
        // Retention is on event time: the partition a replay lands in is the
        // hour the events happened, which can already be past the horizon.
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let now = 1_000 * SECS_PER_HOUR;
        let replayed = write_partition(store, 900, "debug", 5); // written now, 100 h old by event time
        let fresh = write_partition(store, 999, "debug", 5);
        let report = enforce(store, &Tenancy::default(), now, false);
        assert_eq!(report.expired.iter().map(|e| e.key.epoch_hour).collect::<Vec<_>>(), [900]);
        assert!(!replayed.exists() && fresh.exists());
    }

    #[test]
    fn partition_survives_until_its_hour_has_fully_elapsed() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let tenancy = Tenancy::single(one_bucket(SECS_PER_HOUR));

        write_partition(store, 100, "debug", 1);

        // Hour 100 ends at 101h; +1h retention => expires at 102h exactly.
        let just_before = enforce(store, &tenancy, 102 * SECS_PER_HOUR - 1, true);
        assert!(just_before.expired.is_empty());

        let exactly = enforce(store, &tenancy, 102 * SECS_PER_HOUR, true);
        assert_eq!(exactly.expired.len(), 1);
    }

    #[test]
    fn dry_run_reports_without_deleting() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let dir = write_partition(store, 0, "debug", 42);

        let report = enforce(store, &Tenancy::default(), u64::MAX / 2, true);
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

        let report = enforce(store, &Tenancy::default(), u64::MAX / 2, false);
        assert!(dir.exists(), "config edits must not destroy data");
        assert_eq!(report.expired.len(), 0);
        assert_eq!(report.orphan_buckets.len(), 1);
        assert_eq!(report.orphan_buckets[0].bucket, "trace");
    }

    #[test]
    fn an_unconfigured_tenant_or_index_is_never_deleted() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let gone_index = write_in(store, &PartitionKey::new("default", "retired", 0, "debug", "node-a"), 5);
        let gone_tenant = write_in(store, &PartitionKey::new("former", "default", 0, "debug", "node-a"), 5);
        let report = enforce(store, &Tenancy::default(), u64::MAX / 2, false);
        assert!(gone_index.exists() && gone_tenant.exists());
        assert!(report.expired.is_empty());
        assert_eq!(report.orphan_buckets.iter().map(|k| (k.tenant.as_str(), k.index.as_str())).collect::<Vec<_>>(), [("default", "retired"), ("former", "default")]);
    }

    #[test]
    fn ignores_foreign_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let index = store.join("tenant=default/index=default");
        fs::create_dir_all(index.join("hour=nonsense/level=debug")).unwrap();
        fs::create_dir_all(store.join("scratch")).unwrap();
        fs::create_dir_all(index.join("hour=1970-01-01T00/notalevel")).unwrap();
        fs::create_dir_all(index.join("_meta")).unwrap();
        // The layout before tenants: not this build's to expire.
        fs::create_dir_all(store.join("hour=1970-01-01T00/level=debug")).unwrap();
        fs::create_dir_all(store.join("tenant=1234/index=default/hour=1970-01-01T00/level=debug")).unwrap();
        fs::write(store.join("catalog.sqlite"), b"x").unwrap();

        let report = enforce(store, &Tenancy::default(), u64::MAX / 2, false);
        assert!(report.expired.is_empty());
        // An empty directory a user left in the store is not ours to remove.
        assert!(store.join("scratch").exists());
        assert!(store.join("catalog.sqlite").exists());
        assert!(index.join("hour=nonsense/level=debug").exists());
        assert!(index.join("hour=1970-01-01T00/notalevel").exists());
        assert!(index.join("_meta").exists());
        assert!(store.join("hour=1970-01-01T00/level=debug").exists());
        assert!(store.join("tenant=1234/index=default/hour=1970-01-01T00/level=debug").exists());
    }

    #[test]
    fn missing_store_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let report = enforce(&tmp.path().join("absent"), &Tenancy::default(), 0, false);
        assert_eq!(report, RetentionReport::default());
    }

    #[test]
    fn a_pinned_file_outlives_its_partitions_expiry_and_the_rest_of_the_partition_goes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path();
        let leaf = |hour| PartitionKey::at(hour, "debug").dir(store);
        let both = write_partition(store, 100, "debug", 10);
        fs::write(leaf(100).join("000002.parquet"), vec![0u8; 7]).unwrap();
        let only = write_partition(store, 101, "debug", 5);
        let now = 105 * SECS_PER_HOUR + 2 * 24 * 3600;
        let mut pins = PinSet::default();
        pins.pinned.insert(leaf(100).join("000002.parquet"));
        pins.pinned.insert(leaf(101).join("000001.parquet"));

        // A dry run predicts the same.
        let dry = enforce_pinned(store, &Tenancy::default(), now, true, &pins);
        assert_eq!((10, 2), (dry.bytes_reclaimed, dry.pinned_kept));
        assert!(leaf(100).join("000001.parquet").exists());

        let report = enforce_pinned(store, &Tenancy::default(), now, false, &pins);
        assert_eq!(10, report.bytes_reclaimed, "only the unpinned file was freed");
        assert_eq!(1, report.expired.len(), "the partition holding only a pinned file is not expired");
        assert_eq!(2, report.pinned_kept);
        assert!(!leaf(100).join("000001.parquet").exists() && leaf(100).join("000002.parquet").exists());
        assert!(leaf(101).join("000001.parquet").exists());

        // Once unpinned (uploaded, or past the limit) the next pass takes them.
        let report = enforce_pinned(store, &Tenancy::default(), now, false, &PinSet::default());
        assert_eq!(12, report.bytes_reclaimed);
        assert!(!both.exists() && !only.exists());
    }
}
