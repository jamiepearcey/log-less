//! Disk budget and pressure relief.
//!
//! The rule from `docs/architecture.md` §1: **deleting committed data is the
//! release valve, never "stop ingest"**. Under pressure we give up fidelity in
//! a defined order rather than blocking the application or filling the disk.
//!
//! * below 80% — normal;
//! * at 80% (`High`) — delete files *ahead of* their retention, cheapest bucket
//!   first, oldest data first, stopping the moment we are back under the
//!   low-water mark;
//! * at 95% (`Critical`) — additionally stop writing DEBUG to the WAL, so the
//!   inflow shrinks rather than only the backlog.
//!
//! Reclaim order is deliberately "cheapest data first": a bucket the operator
//! configured to keep for 1 day is, by their own policy, worth less than one
//! they keep for 30.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::catalog::{Catalog, CatalogError};
use crate::config::LevelBuckets;
use crate::partition::PartitionKey;
use crate::wal;

/// Fraction of budget at which we start expiring data early.
pub const HIGH_WATER: f64 = 0.80;
/// Fraction at which we also stop admitting DEBUG to the WAL.
pub const CRITICAL_WATER: f64 = 0.95;
/// Reclaim target once triggered — below the high-water mark, with hysteresis
/// so we are not re-triggering on every check.
pub const RECLAIM_TARGET: f64 = 0.70;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pressure {
    Normal,
    High,
    Critical,
}

impl Pressure {
    /// At `Critical`, DEBUG stops entering the WAL at all.
    pub fn admits_debug(self) -> bool {
        self != Pressure::Critical
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskUsage {
    pub wal_bytes: u64,
    pub store_bytes: u64,
    pub budget_bytes: u64,
}

impl DiskUsage {
    pub fn measure(wal_dir: &Path, catalog: &Catalog, budget_bytes: u64) -> Self {
        let store_bytes = catalog.totals().map(|(_, b)| b).unwrap_or(0);
        Self {
            wal_bytes: wal::dir_bytes(wal_dir),
            store_bytes,
            budget_bytes,
        }
    }

    pub fn used(self) -> u64 {
        self.wal_bytes + self.store_bytes
    }

    pub fn fraction(self) -> f64 {
        if self.budget_bytes == 0 {
            return 0.0;
        }
        self.used() as f64 / self.budget_bytes as f64
    }

    pub fn pressure(self) -> Pressure {
        let f = self.fraction();
        if f >= CRITICAL_WATER {
            Pressure::Critical
        } else if f >= HIGH_WATER {
            Pressure::High
        } else {
            Pressure::Normal
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct ReclaimReport {
    /// Files deleted before their retention expired, oldest-and-cheapest first.
    pub files_dropped: Vec<PathBuf>,
    /// Partitions that lost at least one file, for reporting.
    pub partitions_touched: Vec<PartitionKey>,
    pub bytes_reclaimed: u64,
    pub rows_lost: u64,
    /// True if we ran out of things to delete before reaching the target.
    pub exhausted: bool,
}

/// Free space by dropping individual files early, cheapest data first.
///
/// **File granularity, not partition granularity.** Reclaiming whole partitions
/// sounds tidier and is what retention does, but the two have different jobs:
/// retention expires data whose time is up, while this is an emergency valve
/// that should take the minimum. With hour-sized partitions a burst can land
/// entirely in one hour, so partition-granular reclaim has only a handful of
/// units to choose from and freeing anything at all can mean freeing every
/// error you had. Files are per-WAL-segment, so there are many more of them and
/// we stop as soon as we are under target.
///
/// Returns without doing anything when already under the reclaim target.
pub fn reclaim(
    store_dir: &Path,
    catalog: &mut Catalog,
    buckets: &LevelBuckets,
    usage: DiskUsage,
    dry_run: bool,
) -> Result<ReclaimReport, CatalogError> {
    let mut report = ReclaimReport::default();
    let target = (usage.budget_bytes as f64 * RECLAIM_TARGET) as u64;
    let mut used = usage.used();
    if used <= target {
        return Ok(report);
    }

    // Cheapest bucket first, then oldest data within it. A bucket missing from
    // the config sorts last: we do not know its policy, so we do not spend it.
    let mut files = catalog.list_files()?;
    files.sort_by_key(|f| {
        let retention = buckets
            .by_name(&f.key.bucket)
            .map(|b| b.retention.as_secs())
            .unwrap_or(u64::MAX);
        (retention, f.stats.min_observed_nanos, f.path.clone())
    });

    let mut touched: BTreeMap<PartitionKey, ()> = BTreeMap::new();
    for f in files {
        // `break`, not `return`: the catalog cleanup below must run on the
        // success path too, or a reclaim that worked leaves the catalog
        // pointing at files it just deleted.
        if used <= target {
            break;
        }
        // Containment guard: this loop deletes files chosen from catalog rows,
        // and a catalog row is just text. Never unlink anything outside the
        // store, whatever the catalog claims.
        if !f.path.starts_with(store_dir) {
            tracing::warn!(path = %f.path.display(), "catalog row outside the store; not deleting");
            continue;
        }
        if !dry_run && std::fs::remove_file(&f.path).is_err() {
            continue;
        }
        used = used.saturating_sub(f.stats.bytes);
        report.bytes_reclaimed += f.stats.bytes;
        report.rows_lost += f.stats.rows;
        report.files_dropped.push(f.path);
        touched.insert(f.key, ());
    }
    report.partitions_touched = touched.into_keys().collect();

    if !dry_run && !report.files_dropped.is_empty() {
        catalog.forget_missing_files()?;
    }
    // Everything deletable is gone and we are still over target: the budget is
    // too small for the inflow, which is an operator problem, not a bug.
    report.exhausted = used > target;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::FileEntry;
    use crate::model::{LogRecord, Severity};
    use crate::store::{self, FileStats};

    fn seg_uuid(segment: u64) -> uuid::Uuid {
        uuid::Uuid::from_u128(0xB000_0000_0000_0000_0000_0000_0000_0000u128 + segment as u128)
    }

    fn seed_file(
        store: &Path,
        cat: &mut Catalog,
        hour: u64,
        bucket: &str,
        segment: u64,
        n: usize,
    ) -> FileStats {
        let key = PartitionKey {
            epoch_hour: hour,
            bucket: bucket.into(),
        };
        let path = key
            .dir(store)
            .join(store::file_name_for_segment(segment, seg_uuid(segment)));
        let base = hour * 3600 * 1_000_000_000 + segment;
        let records: Vec<_> = (0..n)
            .map(|i| {
                let mut r = LogRecord::new(base + i as u64, Severity::INFO, "some log line here");
                r.observed_unix_nano = base + i as u64;
                r
            })
            .collect();
        let stats = store::write_partition_file(&path, &records).unwrap();
        cat.commit_merge(
            segment,
            seg_uuid(segment),
            &[FileEntry {
                path,
                key,
                stats: stats.clone(),
            }],
            0,
        )
        .unwrap();
        stats
    }

    fn seed(store: &Path, cat: &mut Catalog, hour: u64, bucket: &str, n: usize) -> FileStats {
        let key = PartitionKey {
            epoch_hour: hour,
            bucket: bucket.into(),
        };
        let path = key
            .dir(store)
            .join(store::file_name_for_segment(hour, seg_uuid(hour)));
        let base = hour * 3600 * 1_000_000_000;
        let records: Vec<_> = (0..n)
            .map(|i| {
                let mut r = LogRecord::new(base + i as u64, Severity::INFO, "some log line here");
                r.observed_unix_nano = base + i as u64;
                r
            })
            .collect();
        let stats = store::write_partition_file(&path, &records).unwrap();
        cat.commit_merge(
            hour,
            seg_uuid(hour),
            &[FileEntry {
                path,
                key,
                stats: stats.clone(),
            }],
            0,
        )
        .unwrap();
        stats
    }

    #[test]
    fn pressure_thresholds() {
        let u = |used: u64| DiskUsage {
            wal_bytes: used,
            store_bytes: 0,
            budget_bytes: 100,
        };
        assert_eq!(u(0).pressure(), Pressure::Normal);
        assert_eq!(u(79).pressure(), Pressure::Normal);
        assert_eq!(u(80).pressure(), Pressure::High);
        assert_eq!(u(94).pressure(), Pressure::High);
        assert_eq!(u(95).pressure(), Pressure::Critical);
        assert!(u(80).pressure().admits_debug());
        assert!(!u(95).pressure().admits_debug(), "debug stops at critical");
    }

    #[test]
    fn zero_budget_never_reports_pressure() {
        // A misconfigured budget must not put the agent into permanent panic.
        let u = DiskUsage {
            wal_bytes: 10_000,
            store_bytes: 0,
            budget_bytes: 0,
        };
        assert_eq!(u.pressure(), Pressure::Normal);
    }

    #[test]
    fn does_nothing_below_target() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let mut cat = Catalog::open_in_memory().unwrap();
        let s = seed(&store, &mut cat, 10, "debug", 100);

        let usage = DiskUsage {
            wal_bytes: 0,
            store_bytes: s.bytes,
            budget_bytes: s.bytes * 100,
        };
        let report = reclaim(&store, &mut cat, &LevelBuckets::default(), usage, false).unwrap();
        assert_eq!(report, ReclaimReport::default());
        assert_eq!(cat.list_files().unwrap().len(), 1);
    }

    #[test]
    fn takes_only_what_it_needs_and_keeps_the_expensive_data() {
        // The failure this guards against: a burst lands in one hour, reclaim
        // works at partition granularity, and freeing anything frees every
        // error you had.
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let mut cat = Catalog::open_in_memory().unwrap();

        // One hour, three buckets, many files each — as a real burst produces.
        for segment in 0..8 {
            seed_file(&store, &mut cat, 100, "debug", segment, 200);
            seed_file(&store, &mut cat, 100, "info", 100 + segment, 200);
            seed_file(&store, &mut cat, 100, "warn_plus", 200 + segment, 200);
        }

        let total = cat.totals().unwrap().1;
        // Slightly over budget: only a little needs to go.
        let usage = DiskUsage {
            wal_bytes: 0,
            store_bytes: total,
            budget_bytes: (total as f64 / 0.75) as u64,
        };

        let report = reclaim(&store, &mut cat, &LevelBuckets::default(), usage, false).unwrap();

        assert!(!report.files_dropped.is_empty());
        assert!(
            report.bytes_reclaimed < total / 2,
            "took {} of {total} bytes — reclaim should take the minimum",
            report.bytes_reclaimed
        );
        assert!(
            report.partitions_touched.iter().all(|k| k.bucket == "debug"),
            "only the cheapest bucket should be spent: {:?}",
            report.partitions_touched
        );

        let survivors = cat.list_files().unwrap();
        assert_eq!(
            survivors.iter().filter(|f| f.key.bucket == "warn_plus").count(),
            8,
            "every error file must survive while debug remains"
        );
        assert!(survivors.iter().all(|f| f.path.exists()));
    }

    #[test]
    fn drops_shortest_retention_bucket_first_then_oldest() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let mut cat = Catalog::open_in_memory().unwrap();

        // Same size each, so ordering is the only thing under test.
        seed(&store, &mut cat, 20, "warn_plus", 200);
        seed(&store, &mut cat, 10, "info", 200);
        let old_debug = seed(&store, &mut cat, 5, "debug", 200);
        seed(&store, &mut cat, 30, "debug", 200);

        let total = cat.totals().unwrap().1;
        // Budget such that the target forces roughly two partitions out.
        let usage = DiskUsage {
            wal_bytes: 0,
            store_bytes: total,
            budget_bytes: (total as f64 / 0.85) as u64,
        };

        let report = reclaim(&store, &mut cat, &LevelBuckets::default(), usage, false).unwrap();

        assert!(!report.files_dropped.is_empty());
        let first = &report.partitions_touched[0];
        assert_eq!(first.bucket, "debug", "cheapest bucket goes first");
        assert_eq!(first.epoch_hour, 5, "oldest hour within the bucket");
        assert!(report.bytes_reclaimed >= old_debug.bytes);

        // warn_plus is the most expensive to lose and must be last standing.
        let survivors = cat.list_files().unwrap();
        assert!(
            survivors.iter().any(|f| f.key.bucket == "warn_plus"),
            "warn_plus must not be sacrificed while cheaper data remains"
        );
        // Catalog no longer references deleted files.
        assert!(survivors.iter().all(|f| f.path.exists()));
    }

    #[test]
    fn unknown_buckets_are_spent_last() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let mut cat = Catalog::open_in_memory().unwrap();
        seed(&store, &mut cat, 1, "mystery", 200); // not in config
        seed(&store, &mut cat, 2, "debug", 200);

        let total = cat.totals().unwrap().1;
        let usage = DiskUsage {
            wal_bytes: 0,
            store_bytes: total,
            budget_bytes: (total as f64 / 0.85) as u64,
        };
        let report = reclaim(&store, &mut cat, &LevelBuckets::default(), usage, false).unwrap();

        assert_eq!(report.partitions_touched[0].bucket, "debug");
        assert!(
            cat.list_files().unwrap().iter().any(|f| f.key.bucket == "mystery"),
            "an unknown policy is not ours to spend"
        );
    }

    #[test]
    fn reports_exhaustion_when_deleting_everything_is_not_enough() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let mut cat = Catalog::open_in_memory().unwrap();
        let s = seed(&store, &mut cat, 1, "debug", 100);

        // WAL alone already exceeds the target; no amount of store deletion helps.
        let usage = DiskUsage {
            wal_bytes: 1_000_000,
            store_bytes: s.bytes,
            budget_bytes: 1000,
        };
        let report = reclaim(&store, &mut cat, &LevelBuckets::default(), usage, false).unwrap();
        assert!(report.exhausted, "must surface that relief failed");
        assert_eq!(cat.list_files().unwrap().len(), 0);
    }

    #[test]
    fn never_deletes_outside_the_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let outside = tmp.path().join("precious.parquet");
        let mut cat = Catalog::open_in_memory().unwrap();
        let real = seed_file(&store, &mut cat, 1, "debug", 1, 200);

        // A catalog row is just text; a corrupt or tampered one must not turn
        // into an unlink of an arbitrary path.
        let real_path = PartitionKey {
            epoch_hour: 1,
            bucket: "debug".into(),
        }
        .dir(&store)
        .join(store::file_name_for_segment(1, seg_uuid(1)));
        std::fs::copy(&real_path, &outside).unwrap();
        cat.commit_merge(
            2,
            seg_uuid(2),
            &[FileEntry {
                path: outside.clone(),
                key: PartitionKey { epoch_hour: 1, bucket: "debug".into() },
                stats: real.clone(),
            }],
            0,
        )
        .unwrap();

        let total = cat.totals().unwrap().1;
        let usage = DiskUsage {
            wal_bytes: 0,
            store_bytes: total,
            budget_bytes: (total as f64 / 0.99) as u64,
        };
        let report = reclaim(&store, &mut cat, &LevelBuckets::default(), usage, false).unwrap();

        assert!(outside.exists(), "must not delete outside the store");
        assert!(report.files_dropped.iter().all(|p| p.starts_with(&store)));
    }

    #[test]
    fn dry_run_reclaims_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let mut cat = Catalog::open_in_memory().unwrap();
        seed(&store, &mut cat, 1, "debug", 200);

        let total = cat.totals().unwrap().1;
        let usage = DiskUsage {
            wal_bytes: 0,
            store_bytes: total,
            budget_bytes: (total as f64 / 0.99) as u64,
        };
        let report = reclaim(&store, &mut cat, &LevelBuckets::default(), usage, true).unwrap();
        assert!(!report.files_dropped.is_empty());
        assert_eq!(cat.list_files().unwrap().len(), 1, "dry run must not delete");
    }
}
