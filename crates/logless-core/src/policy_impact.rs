//! What applying a policy would do to the data on disk, computed with the same
//! functions the maintenance pass runs: the retention dry-run over the real
//! store directory, then the real reclaim ordering over the real catalog.
//!
//! The prediction mirrors a pass: expire by retention first, re-measure, and
//! only if pressure remains relieve it. A preview that skipped that sequence
//! would promise numbers the next pass does not deliver.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::budget::{self, DiskUsage, Pressure, ReclaimReport};
use crate::catalog::{Catalog, CatalogError};
use crate::policy::{Policies, Problem};
use crate::retention::{self, ExpiredPartition};
use crate::wal;

pub struct Locations<'a> {
    pub store_dir: &'a Path,
    pub wal_dir: &'a Path,
}

/// Everything a pass under one policy would delete.
pub struct Plan {
    expired: Vec<ExpiredPartition>,
    expired_files: BTreeMap<String, u64>,
    reclaim: ReclaimReport,
    used_before: u64,
    budget: u64,
    /// Partition directories and individually reclaimed files.
    doomed: HashSet<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BucketImpact {
    pub name: String,
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RetentionImpact {
    pub would_expire_files: u64,
    pub would_expire_bytes: u64,
    pub per_bucket: Vec<BucketImpact>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DiskImpact {
    pub used_bytes: u64,
    pub budget_bytes: u64,
    pub fraction_after: f64,
    pub would_reclaim: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Impact {
    pub retention: RetentionImpact,
    pub disk: DiskImpact,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Preview {
    pub impact: Impact,
    /// True when the proposed policy deletes something the current one would
    /// not delete on its next pass, or introduces a setting that loses data.
    pub destructive: bool,
    /// Non-deleting settings that lose data (tiny queue, pushdown off, ...).
    pub lossy: Vec<Problem>,
    /// Binds a confirmation to what was reviewed: a hash of exactly the
    /// partitions and files this change newly deletes, and the lossy settings.
    /// Data that appears after the preview changes it, so a stale "yes"
    /// cannot authorise a bigger deletion.
    pub impact_token: String,
}

fn count_files(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else { return 0 };
    entries
        .flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => count_files(&e.path()),
            Ok(t) if t.is_file() => 1,
            _ => 0,
        })
        .sum()
}

pub fn plan(
    loc: &Locations,
    catalog: &Catalog,
    policies: &Policies,
    now_unix_secs: u64,
) -> Result<Plan, PlanError> {
    let buckets = policies.buckets().map_err(|e| PlanError::Policy(e.to_string()))?;
    let wm = policies.watermarks();
    let budget = policies.storage.disk_budget_bytes;

    let report = retention::enforce(loc.store_dir, &buckets, now_unix_secs, true);
    let files = catalog.list_files()?;
    let remaining: Vec<_> = files
        .into_iter()
        .filter(|f| !report.expired.iter().any(|e| f.path.starts_with(&e.path)))
        .collect();

    let mut expired_files: BTreeMap<String, u64> = BTreeMap::new();
    for e in &report.expired {
        *expired_files.entry(e.key.bucket.clone()).or_default() += count_files(&e.path);
    }

    let wal_bytes = wal::dir_bytes(loc.wal_dir);
    let store_bytes_all = catalog.totals()?.1;
    let used_before = wal_bytes + store_bytes_all;
    let usage_after = DiskUsage {
        wal_bytes,
        store_bytes: remaining.iter().map(|f| f.stats.bytes).sum(),
        budget_bytes: budget,
    };
    let reclaim = if usage_after.pressure(&wm) == Pressure::Normal {
        ReclaimReport::default()
    } else {
        budget::reclaim_files(remaining, loc.store_dir, &buckets, usage_after, &wm, |_| true)
    };

    let mut doomed: HashSet<PathBuf> = report.expired.iter().map(|e| e.path.clone()).collect();
    doomed.extend(reclaim.files_dropped.iter().cloned());

    Ok(Plan {
        expired: report.expired,
        expired_files,
        reclaim,
        used_before,
        budget,
        doomed,
    })
}

impl Plan {
    fn impact(&self, policies: &Policies) -> Impact {
        let retention_bytes: u64 = self.expired.iter().map(|e| e.bytes).sum();
        let per_bucket: Vec<BucketImpact> = policies
            .retention
            .iter()
            .map(|r| BucketImpact {
                name: r.name.clone(),
                files: self.expired_files.get(&r.name).copied().unwrap_or(0),
                bytes: self
                    .expired
                    .iter()
                    .filter(|e| e.key.bucket == r.name)
                    .map(|e| e.bytes)
                    .sum(),
            })
            .collect();
        let would_reclaim = retention_bytes + self.reclaim.bytes_reclaimed;
        let after = self.used_before.saturating_sub(would_reclaim);
        Impact {
            retention: RetentionImpact {
                would_expire_files: self.expired_files.values().sum(),
                would_expire_bytes: retention_bytes,
                per_bucket,
            },
            disk: DiskImpact {
                used_bytes: self.used_before,
                budget_bytes: self.budget,
                fraction_after: if self.budget == 0 {
                    0.0
                } else {
                    after as f64 / self.budget as f64
                },
                would_reclaim,
            },
        }
    }

    /// Paths this plan deletes that the baseline would not, sorted.
    fn beyond(&self, baseline: &Plan) -> Vec<&PathBuf> {
        let mut v: Vec<&PathBuf> = self
            .doomed
            .iter()
            .filter(|p| !p.ancestors().any(|a| baseline.doomed.contains(a)))
            .collect();
        v.sort();
        v
    }
}

pub fn preview(
    loc: &Locations,
    catalog: &Catalog,
    current: &Policies,
    proposed: &Policies,
    now_unix_secs: u64,
) -> Result<Preview, PlanError> {
    let baseline = plan(loc, catalog, current, now_unix_secs)?;
    let next = plan(loc, catalog, proposed, now_unix_secs)?;
    let beyond = next.beyond(&baseline);
    let lossy = crate::policy::lossy_settings(current, proposed);
    let mut hasher = Sha256::new();
    for p in &beyond {
        hasher.update(p.as_os_str().as_encoded_bytes());
        hasher.update([0]);
    }
    for l in &lossy {
        hasher.update(l.path.as_bytes());
        hasher.update([1]);
    }
    let impact_token = hasher.finalize().iter().take(16).map(|b| format!("{b:02x}")).collect();
    Ok(Preview {
        impact: next.impact(proposed),
        destructive: !beyond.is_empty() || !lossy.is_empty(),
        lossy,
        impact_token,
    })
}

/// A bucket that exists today, is missing from the proposal, and still has
/// partitions on disk. Retention never deletes a bucket it does not know, so
/// the data would be stranded: not expired, not reclaimed first, invisible to
/// policy. Refuse instead.
pub fn orphan_problems(
    loc: &Locations,
    current: &Policies,
    proposed: &Policies,
    now_unix_secs: u64,
) -> Result<Vec<Problem>, PlanError> {
    let Ok(new_buckets) = proposed.buckets() else { return Ok(Vec::new()) };
    let report = retention::enforce(loc.store_dir, &new_buckets, now_unix_secs, true);
    let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
    for k in &report.orphan_buckets {
        if current.retention.iter().any(|r| r.name == k.bucket) {
            *counts.entry(k.bucket.as_str()).or_default() += 1;
        }
    }
    Ok(counts
        .into_iter()
        .map(|(name, partitions)| {
            let index = current.retention.iter().position(|r| r.name == name).unwrap_or(0);
            Problem::new(
                "retention",
                format!(
                    "bucket {name:?} (currently retention[{index}]) still has {partitions} partition(s) on disk and cannot be renamed or removed. Keep a bucket named {name:?} until its partitions have expired, or shorten its retention_secs so they expire sooner."
                ),
            )
        })
        .collect())
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error("policy cannot be converted to buckets: {0}")]
    Policy(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::{PartitionKey, SECS_PER_HOUR};
    use crate::policy::test_policies;

    fn partition(store: &Path, hour: u64, bucket: &str, bytes: usize) -> PathBuf {
        let dir = PartitionKey { epoch_hour: hour, bucket: bucket.into() }.dir(store);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("000001.parquet"), vec![0u8; bytes]).unwrap();
        dir
    }

    struct Fixture {
        _tmp: tempfile::TempDir,
        store: PathBuf,
        wal: PathBuf,
        catalog: Catalog,
    }

    fn fixture() -> Fixture {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let wal = tmp.path().join("wal");
        fs::create_dir_all(&store).unwrap();
        Fixture { store, wal, catalog: Catalog::open_in_memory().unwrap(), _tmp: tmp }
    }

    #[test]
    fn preview_counts_what_retention_would_really_delete() {
        let f = fixture();
        // Hour 100, three days later: debug (1d) and info (7d, not yet) differ.
        let now = 101 * SECS_PER_HOUR + 3 * 86_400;
        let debug = partition(&f.store, 100, "debug", 10);
        let info = partition(&f.store, 100, "info", 20);
        let loc = Locations { store_dir: &f.store, wal_dir: &f.wal };

        let current = test_policies();
        let mut proposed = current.clone();
        proposed.retention[1].retention_secs = 86_400; // info 7d -> 1d
        let p = preview(&loc, &f.catalog, &current, &proposed, now).unwrap();

        assert_eq!(p.impact.retention.would_expire_files, 2);
        assert_eq!(p.impact.retention.would_expire_bytes, 30);
        let per: Vec<_> = p.impact.retention.per_bucket.iter().map(|b| (b.name.as_str(), b.files, b.bytes)).collect();
        assert_eq!(per, vec![("debug", 1, 10), ("info", 1, 20), ("warn_plus", 0, 0)]);
        assert!(p.destructive, "info would newly be deleted");
        assert!(debug.exists() && info.exists(), "preview must not delete");
    }

    #[test]
    fn data_the_current_policy_would_delete_anyway_is_not_destructive() {
        let f = fixture();
        let now = 101 * SECS_PER_HOUR + 3 * 86_400;
        partition(&f.store, 100, "debug", 10); // already past its 1d retention
        let loc = Locations { store_dir: &f.store, wal_dir: &f.wal };
        let current = test_policies();
        let mut proposed = current.clone();
        proposed.retention[2].retention_secs += 1; // unrelated tweak
        let p = preview(&loc, &f.catalog, &current, &proposed, now).unwrap();
        assert_eq!(p.impact.retention.would_expire_files, 1, "still reported: it is what the next pass deletes");
        assert!(!p.destructive);
    }

    #[test]
    fn a_longer_retention_is_never_destructive() {
        let f = fixture();
        let now = 101 * SECS_PER_HOUR + 3 * 86_400;
        partition(&f.store, 100, "debug", 10);
        let loc = Locations { store_dir: &f.store, wal_dir: &f.wal };
        let current = test_policies();
        let mut proposed = current.clone();
        proposed.retention[0].retention_secs = 30 * 86_400;
        let p = preview(&loc, &f.catalog, &current, &proposed, now).unwrap();
        assert_eq!(p.impact.retention.would_expire_files, 0);
        assert!(!p.destructive);
    }

    #[test]
    fn a_smaller_budget_predicts_early_reclaim_in_the_real_order() {
        use crate::catalog::FileEntry;
        use crate::model::{LogRecord, Severity};
        use crate::store;
        let mut f = fixture();
        let mut seed = |hour: u64, bucket: &str, segment: u64| {
            let key = PartitionKey { epoch_hour: hour, bucket: bucket.into() };
            let path = key.dir(&f.store).join(store::file_name_for_segment(segment, uuid::Uuid::from_u128(segment as u128)));
            let base = hour * 3600 * 1_000_000_000 + segment;
            let recs: Vec<_> = (0..200)
                .map(|i| {
                    let mut r = LogRecord::new(base + i, Severity::INFO, "some log line here");
                    r.observed_unix_nano = base + i;
                    r
                })
                .collect();
            let stats = store::write_partition_file(&path, &recs).unwrap();
            f.catalog
                .commit_merge(segment, uuid::Uuid::from_u128(segment as u128), &[FileEntry { path, key, stats: stats.clone() }], 0)
                .unwrap();
            stats.bytes
        };
        let d = seed(100, "debug", 1);
        let i = seed(100, "info", 2);
        let w = seed(100, "warn_plus", 3);
        let total = d + i + w;

        let loc = Locations { store_dir: &f.store, wal_dir: &f.wal };
        let now = 100 * SECS_PER_HOUR + 10; // nothing expires by retention
        let mut current = test_policies();
        current.storage.disk_budget_bytes = total * 10;
        let mut proposed = current.clone();
        proposed.storage.disk_budget_bytes = total + total / 20; // ~95%: critical
        let p = preview(&loc, &f.catalog, &current, &proposed, now).unwrap();

        assert_eq!(p.impact.retention.would_expire_files, 0);
        assert!(p.impact.disk.would_reclaim >= d, "debug is the cheapest bucket and goes first");
        assert!(p.impact.disk.would_reclaim < total, "stops at the reclaim target, not at empty");
        assert!(p.impact.disk.fraction_after <= 0.7 + 1e-9, "{}", p.impact.disk.fraction_after);
        assert!(p.destructive);
        assert!(f.catalog.list_files().unwrap().iter().all(|e| e.path.exists()));
    }

    #[test]
    fn orphan_protection_names_the_bucket_and_what_to_do() {
        let f = fixture();
        partition(&f.store, 100, "info", 5);
        let loc = Locations { store_dir: &f.store, wal_dir: &f.wal };
        let now = 100 * SECS_PER_HOUR;
        let current = test_policies();

        let mut renamed = current.clone();
        renamed.retention[1].name = "notice".into();
        let problems = orphan_problems(&loc, &current, &renamed, now).unwrap();
        assert_eq!(problems.len(), 1);
        assert!(problems[0].message.contains("\"info\""), "{problems:?}");
        assert!(problems[0].message.contains("1 partition"));

        let mut removed = current.clone();
        removed.retention.remove(1);
        assert_eq!(orphan_problems(&loc, &current, &removed, now).unwrap().len(), 1);

        // A bucket with nothing on disk can go.
        let mut dropped_unused = current.clone();
        dropped_unused.retention.remove(2);
        assert!(orphan_problems(&loc, &current, &dropped_unused, now).unwrap().is_empty());
    }

    #[test]
    fn an_orphan_that_already_existed_does_not_block_unrelated_changes() {
        let f = fixture();
        partition(&f.store, 100, "legacy", 5); // not in the current policy either
        let loc = Locations { store_dir: &f.store, wal_dir: &f.wal };
        let current = test_policies();
        let mut proposed = current.clone();
        proposed.retention[1].retention_secs = 60;
        assert!(orphan_problems(&loc, &current, &proposed, 100 * SECS_PER_HOUR).unwrap().is_empty());
    }
}
