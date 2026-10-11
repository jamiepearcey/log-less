//! Disk budget and pressure relief.
//!
//! The rule from `docs/architecture.md` §1: **deleting committed data is the
//! release valve, never "stop ingest"**. Under pressure we give up fidelity in
//! a defined order rather than blocking the application or filling the disk.
//!
//! With the default [`Watermarks`]:
//!
//! * below 80% — normal;
//! * at 80% (`High`) — delete files *ahead of* their retention, cheapest bucket
//!   first, oldest data first, stopping the moment we are back under the
//!   reclaim target (70%);
//! * at 95% (`Critical`) — additionally stop writing DEBUG to the WAL, so the
//!   inflow shrinks rather than only the backlog.
//!
//! The three fractions are runtime policy (`docs/admin-api.md`), so every
//! function that applies them takes a `&Watermarks` instead of reading a
//! constant.
//!
//! Reclaim order is deliberately "cheapest data first": a bucket the operator
//! configured to keep for 1 day is, by their own policy, worth less than one
//! they keep for 30.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::catalog::{Catalog, CatalogError, FileEntry};
use crate::config::Tenancy;
use crate::partition::PartitionKey;
use crate::parts_edge::{gave_up_of, GaveUp, PinSet};
use crate::retention::{self, RetentionReport};
use crate::wal;

/// The three fractions of the disk budget that drive pressure relief.
/// Invariant (enforced by policy validation): `0 < reclaim_target < high <
/// critical <= 1`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Watermarks {
    /// Fraction of budget at which we start expiring data early.
    pub high: f64,
    /// Fraction at which we also stop admitting DEBUG to the WAL.
    pub critical: f64,
    /// Reclaim target once triggered — below the high-water mark, with
    /// hysteresis so we are not re-triggering on every check.
    pub reclaim_target: f64,
}

impl Default for Watermarks {
    fn default() -> Self {
        Self { high: 0.80, critical: 0.95, reclaim_target: 0.70 }
    }
}

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
            // The late spool is WAL too, and counts against the same budget.
            wal_bytes: wal::dir_bytes(wal_dir) + wal::dir_bytes(&wal::late_dir_beside(wal_dir)),
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

    pub fn pressure(self, wm: &Watermarks) -> Pressure {
        let f = self.fraction();
        if f >= wm.critical {
            Pressure::Critical
        } else if f >= wm.high {
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
    /// Files left alone because they are pinned (`parts_edge`): central has not confirmed them.
    pub pinned_kept: usize,
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
    tenancy: &Tenancy,
    usage: DiskUsage,
    wm: &Watermarks,
    dry_run: bool,
) -> Result<ReclaimReport, CatalogError> {
    reclaim_pinned(store_dir, catalog, tenancy, usage, wm, dry_run, &PinSet::default())
}

/// [`reclaim`], never choosing a pinned file. When only pinned files are left the report is `exhausted`: the pin
/// limit is capped at a quarter of the disk budget, so this cannot be most of the disk.
pub fn reclaim_pinned(
    store_dir: &Path,
    catalog: &mut Catalog,
    tenancy: &Tenancy,
    usage: DiskUsage,
    wm: &Watermarks,
    dry_run: bool,
    pins: &PinSet,
) -> Result<ReclaimReport, CatalogError> {
    let (pinned, files): (Vec<_>, Vec<_>) = catalog.list_files()?.into_iter().partition(|f| pins.contains(&f.path));
    let mut report = reclaim_files(files, store_dir, tenancy, usage, wm, |path| {
        dry_run || std::fs::remove_file(path).is_ok()
    });
    report.pinned_kept = pinned.len();
    if !dry_run && !report.files_dropped.is_empty() {
        catalog.forget_files(&report.files_dropped)?;
    }
    Ok(report)
}

/// The selection loop of [`reclaim`], over an explicit file list with the
/// deletion injected. Policy preview calls it with a delete that always
/// succeeds, so what it predicts is the real ordering and the real stop point,
/// not a parallel reimplementation.
pub fn reclaim_files(
    mut files: Vec<FileEntry>,
    store_dir: &Path,
    tenancy: &Tenancy,
    usage: DiskUsage,
    wm: &Watermarks,
    mut delete: impl FnMut(&Path) -> bool,
) -> ReclaimReport {
    let mut report = ReclaimReport::default();
    let target = (usage.budget_bytes as f64 * wm.reclaim_target) as u64;
    let mut used = usage.used();
    if used <= target {
        return report;
    }

    // Cheapest bucket first, across every tenant and index, then oldest data
    // (by event time) within it. A tenant, index or bucket missing from the
    // config sorts last: we do not know its policy, so we do not spend it.
    files.sort_by_key(|f| {
        let retention = tenancy
            .buckets(&f.key.tenant, &f.key.index)
            .and_then(|b| b.by_name(&f.key.bucket))
            .map(|b| b.retention.as_secs())
            .unwrap_or(u64::MAX);
        (retention, f.stats.min_ts_nanos, f.path.clone())
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
        if !delete(&f.path) {
            continue;
        }
        used = used.saturating_sub(f.stats.bytes);
        report.bytes_reclaimed += f.stats.bytes;
        report.rows_lost += f.stats.rows;
        report.files_dropped.push(f.path);
        touched.insert(f.key, ());
    }
    report.partitions_touched = touched.into_keys().collect();

    // Everything deletable is gone and we are still over target: the budget is
    // too small for the inflow, which is an operator problem, not a bug.
    report.exhausted = used > target;
    report
}

/// What one maintenance pass did to disk: retention first, then pressure
/// relief against the budget that remains.
pub struct PassOutcome {
    pub expiry: RetentionReport,
    pub usage: DiskUsage,
    pub pressure: Pressure,
    /// `None` when pressure was normal and nothing was attempted.
    pub reclaim: Option<Result<ReclaimReport, CatalogError>>,
    /// Closed files central had not confirmed that retention or reclaim deleted anyway, because they were past
    /// the pin limit (`parts_edge`). Zero when nothing is pinned.
    pub gave_up: GaveUp,
}

/// The expiry-and-relief half of a maintenance pass, with every policy value
/// passed in. The agent calls it with values re-read from the live policy each
/// tick, which is what makes retention and budget changes hot.
pub fn expire_and_relieve(
    store_dir: &Path,
    wal_dir: &Path,
    catalog: &mut Catalog,
    tenancy: &Tenancy,
    budget_bytes: u64,
    wm: &Watermarks,
    now_unix_secs: u64,
    pins: &PinSet,
) -> PassOutcome {
    // The unconfirmed files as the catalog knows them now, to count the ones this pass deletes.
    let unconfirmed: Vec<FileEntry> = if pins.unconfirmed.is_empty() {
        Vec::new()
    } else {
        catalog.list_files().unwrap_or_default().into_iter().filter(|f| pins.unconfirmed.contains(&f.path)).collect()
    };
    let expiry = retention::enforce_pinned(store_dir, tenancy, now_unix_secs, false, pins);
    if !expiry.expired.is_empty() {
        // Only the partitions that expired are re-checked, not the table.
        let keys: Vec<PartitionKey> = expiry.expired.iter().map(|e| e.key.clone()).collect();
        if let Err(e) = catalog.forget_missing_in(&keys) {
            tracing::error!(error = %e, "catalog cleanup after retention failed");
        }
    }
    let usage = DiskUsage::measure(wal_dir, catalog, budget_bytes);
    let pressure = usage.pressure(wm);
    let reclaim = (pressure != Pressure::Normal)
        .then(|| reclaim_pinned(store_dir, catalog, tenancy, usage, wm, false, pins));
    let gave_up = gave_up_of(&unconfirmed);
    PassOutcome { expiry, usage, pressure, reclaim, gave_up }
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
        let key = PartitionKey::at(hour, bucket);
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
            &[],
            0,
        )
        .unwrap();
        stats
    }

    fn seed(store: &Path, cat: &mut Catalog, hour: u64, bucket: &str, n: usize) -> FileStats {
        let key = PartitionKey::at(hour, bucket);
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
            &[],
            0,
        )
        .unwrap();
        stats
    }

    #[test]
    fn a_replay_of_old_rows_lands_by_event_hour_and_retention_takes_exactly_what_is_past_its_horizon() {
        // The whole path: the split WAL, the merge of both logs, then a maintenance pass's expiry.
        use crate::model::LogRecord;
        use std::time::Duration;
        const HOUR: u64 = 3600;
        let tmp = tempfile::tempdir().unwrap();
        let (wal_dir, store) = (tmp.path().join("wal"), tmp.path().join("store"));
        let late_dir = wal::late_dir_beside(&wal_dir);
        let now_hour = 1_000_000u64;
        let now = now_hour * HOUR + 60;
        let rows = |hours_ago: u64, n: u64, sev: Severity, tag: &str| -> Vec<LogRecord> {
            (0..n)
                .map(|i| {
                    let mut r = LogRecord::new(((now_hour - hours_ago) * HOUR + i) * 1_000_000_000, sev, format!("{tag} {i}"));
                    r.observed_unix_nano = now * 1_000_000_000;
                    r
                })
                .collect()
        };
        // Default buckets: debug 1 day, info 7 days, warn_plus 30 days. Everything arrives now.
        let mut batch = rows(0, 100, Severity::INFO, "live");
        batch.extend(rows(10 * 24, 60, Severity::INFO, "info 10d old: past 7d"));
        batch.extend(rows(3 * 24, 40, Severity::INFO, "info 3d old: kept"));
        batch.extend(rows(3 * 24, 30, Severity::DEBUG, "debug 3d old: past 1d"));
        batch.extend(rows(20 * 24, 20, Severity::WARN, "warn 20d old: kept"));
        let config = wal::SplitConfig {
            segment_bytes: 1 << 30,
            fsync_interval: Duration::from_secs(60),
            fsync_bytes: 1 << 30,
            late_threshold: Duration::from_secs(2 * HOUR),
            late_roll: Duration::from_secs(1800),
            late_segment_bytes: 1 << 30,
        };
        let mut w = wal::SplitWal::open(&wal_dir, &late_dir, &config).unwrap();
        w.append(&batch, now * 1_000_000_000).unwrap();
        assert_eq!((w.records_written(), w.late_records), (250, 150), "every replayed row took the late spool");
        drop(w);

        let mut cat = Catalog::open_in_memory().unwrap();
        let tenancy = Tenancy::default();
        let mut drain = crate::drain::Drain::default();
        for dir in [&wal_dir, &late_dir] {
            crate::merge::merge_all(dir, &store, &mut cat, &tenancy, &mut drain, &Default::default(), None, now).unwrap();
        }
        let placed: Vec<(u64, String, u64)> = cat.list_files().unwrap().iter().map(|f| (now_hour - f.key.epoch_hour, f.key.bucket.clone(), f.stats.rows)).collect();
        assert_eq!(
            placed,
            vec![(480, "warn_plus".into(), 20), (240, "info".into(), 60), (72, "debug".into(), 30), (72, "info".into(), 40), (0, "info".into(), 100)],
            "(hours before now, level, rows): each row is in the hour it happened"
        );

        let pass = |cat: &mut Catalog| expire_and_relieve(&store, &wal_dir, cat, &tenancy, u64::MAX, &Watermarks::default(), now, &PinSet::default());
        let out = pass(&mut cat);
        let expired: Vec<(u64, &str)> = out.expiry.expired.iter().map(|e| (now_hour - e.key.epoch_hour, e.key.bucket.as_str())).collect();
        assert_eq!(expired, vec![(240, "info"), (72, "debug")], "the two partitions past their bucket's retention, and no other");
        assert!(out.reclaim.is_none(), "no disk pressure: nothing else was touched");
        assert_eq!(cat.totals().unwrap().0, 160, "250 rows less the 60 and the 30");
        assert_eq!(cat.file_count().unwrap(), 3);
        let left = crate::scan::scan(&store, &crate::scan::Query { limit: 1000, ..Default::default() }).unwrap();
        assert_eq!(left.records.len(), 160);
        assert!(left.records.iter().all(|r| !r.body.contains("past")), "what is left is what its policy keeps");
        // The next pass has nothing more to expire.
        assert!(pass(&mut cat).expiry.expired.is_empty());
    }

    #[test]
    fn pressure_thresholds() {
        const W: Watermarks = Watermarks { high: 0.80, critical: 0.95, reclaim_target: 0.70 };
        let u = |used: u64| DiskUsage {
            wal_bytes: used,
            store_bytes: 0,
            budget_bytes: 100,
        };
        assert_eq!(u(0).pressure(&W), Pressure::Normal);
        assert_eq!(u(79).pressure(&W), Pressure::Normal);
        assert_eq!(u(80).pressure(&W), Pressure::High);
        assert_eq!(u(94).pressure(&W), Pressure::High);
        assert_eq!(u(95).pressure(&W), Pressure::Critical);
        assert!(u(80).pressure(&W).admits_debug());
        assert!(!u(95).pressure(&W).admits_debug(), "debug stops at critical");
    }

    #[test]
    fn zero_budget_never_reports_pressure() {
        // A misconfigured budget must not put the agent into permanent panic.
        let u = DiskUsage {
            wal_bytes: 10_000,
            store_bytes: 0,
            budget_bytes: 0,
        };
        assert_eq!(u.pressure(&Watermarks::default()), Pressure::Normal);
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
        let report = reclaim(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), false).unwrap();
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

        let report = reclaim(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), false).unwrap();

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

        let report = reclaim(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), false).unwrap();

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
        let report = reclaim(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), false).unwrap();

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
        let report = reclaim(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), false).unwrap();
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
        let real_path = PartitionKey::at(1, "debug")
        .dir(&store)
        .join(store::file_name_for_segment(1, seg_uuid(1)));
        std::fs::copy(&real_path, &outside).unwrap();
        cat.commit_merge(
            2,
            seg_uuid(2),
            &[FileEntry {
                path: outside.clone(),
                key: PartitionKey::at(1, "debug"),
                stats: real.clone(),
            }],
            &[],
            0,
        )
        .unwrap();

        let total = cat.totals().unwrap().1;
        let usage = DiskUsage {
            wal_bytes: 0,
            store_bytes: total,
            budget_bytes: (total as f64 / 0.99) as u64,
        };
        let report = reclaim(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), false).unwrap();

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
        let report = reclaim(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), true).unwrap();
        assert!(!report.files_dropped.is_empty());
        assert_eq!(cat.list_files().unwrap().len(), 1, "dry run must not delete");
    }

    #[test]
    fn reclaim_never_picks_a_pinned_file_even_when_it_is_the_cheapest_and_oldest() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let mut cat = Catalog::open_in_memory().unwrap();
        let old = seed(&store, &mut cat, 100, "debug", 50);
        seed(&store, &mut cat, 101, "debug", 50);
        let oldest_path = cat.list_files().unwrap().into_iter().min_by_key(|f| f.stats.min_ts_nanos).unwrap().path;
        let usage = DiskUsage { wal_bytes: 0, store_bytes: 2 * old.bytes, budget_bytes: 2 * old.bytes };
        let mut pins = PinSet::default();
        pins.pinned.insert(oldest_path.clone());

        let report = reclaim_pinned(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), false, &pins).unwrap();
        assert!(oldest_path.exists(), "the pinned file is the oldest and cheapest, and stays");
        assert_eq!((1, 1), (report.files_dropped.len(), report.pinned_kept));
        assert_eq!(old.rows, report.rows_lost);
        // Only the pinned file is left and it is over target: said so, not forced.
        let usage = DiskUsage { wal_bytes: 0, store_bytes: old.bytes, budget_bytes: old.bytes };
        let report = reclaim_pinned(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), false, &pins).unwrap();
        assert!(report.files_dropped.is_empty() && report.exhausted && oldest_path.exists());
        // Unpinned, it goes.
        let report = reclaim_pinned(&store, &mut cat, &Tenancy::default(), usage, &Watermarks::default(), false, &PinSet::default()).unwrap();
        assert_eq!(1, report.files_dropped.len());
    }
}
