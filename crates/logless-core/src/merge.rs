//! Merge: WAL segments → partitioned Parquet + catalog commit.
//!
//! Ordering is chosen so every crash point is recoverable:
//!
//! 1. write Parquet to `.tmp`, fsync, atomic rename  — crash here leaves a
//!    complete file the catalog doesn't know about; the segment is still
//!    unmerged, so the next run rewrites the *same deterministic path*
//!    (idempotent), rather than appending duplicates;
//! 2. commit the catalog transaction (files + segment marker)  — crash here
//!    leaves a merged segment still on disk; the next run sees the marker,
//!    skips the work and deletes it;
//! 3. delete the WAL segment.
//!
//! The active segment — the one the writer currently holds — is never merged,
//! **and neither is any segment newer than it**. The writer rolls to a new
//! segment before it republishes its id, so a merger that trusted only the
//! last-published id could delete a segment the writer had just started
//! appending to. Deleting a file out from under an open append handle loses
//! every record written after the unlink, silently. Skipping `>= active` closes
//! that window; the newer segments are simply merged on the next pass.

use std::collections::BTreeMap;
use std::path::Path;

use crate::catalog::{Catalog, CatalogError, FileEntry};
use crate::config::LevelBuckets;
use crate::drain::Drain;
use crate::model::LogRecord;
use crate::partition::PartitionKey;
use crate::schema;
use crate::store::{self, StoreError};
use crate::wal;

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error(transparent)]
    Wal(#[from] wal::WalError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}

#[derive(Debug, Default, PartialEq)]
pub struct MergeReport {
    pub segments_merged: usize,
    /// Segments already recorded as merged — deleted without redoing the work.
    pub segments_skipped: usize,
    pub records: u64,
    pub files_written: usize,
    pub bytes_written: u64,
    pub wal_bytes_freed: u64,
    /// Segments whose damaged tail was truncated during the merge.
    pub truncated: usize,
    pub temp_files_cleaned: usize,
    /// Records assigned a template id.
    pub templated: u64,
    /// Records the miner could not template (template cap reached).
    pub untemplated: u64,
    /// Templates seen for the first time in this merge — the novelty signal.
    pub new_templates: u64,
}

/// Merge every eligible WAL segment.
///
/// `active_segment` is the id the writer currently owns. That segment and every
/// higher id are left alone. Pass `None` only when no writer is running (the
/// standalone `logless merge` command).
pub fn merge_all(
    wal_dir: &Path,
    store_dir: &Path,
    catalog: &mut Catalog,
    buckets: &LevelBuckets,
    drain: &mut Drain,
    active_segment: Option<u64>,
    now_unix_secs: u64,
) -> Result<MergeReport, MergeError> {
    let mut report = MergeReport {
        temp_files_cleaned: store::clean_temp_files(store_dir),
        ..Default::default()
    };

    for (segment_id, path) in wal::list_segments(wal_dir)? {
        if active_segment.is_some_and(|active| segment_id >= active) {
            continue;
        }

        let segment_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let segment_uuid = wal::segment_uuid(&path)?;

        if catalog.is_segment_merged(segment_uuid)? {
            // Crashed between commit and delete on a previous run.
            if std::fs::remove_file(&path).is_ok() {
                report.wal_bytes_freed += segment_bytes;
            }
            report.segments_skipped += 1;
            continue;
        }

        // Group by partition in memory. A segment is bounded by
        // `wal_segment_bytes`, so this is bounded too — the reason merge works
        // per segment rather than over the whole WAL.
        let mut partitions: BTreeMap<PartitionKey, Vec<LogRecord>> = BTreeMap::new();
        let outcome = wal::replay_segment(&path, true, |batch| {
            for record in batch {
                let bucket = buckets.bucket_for(record.severity);
                let key = PartitionKey::from_nanos(record.observed_unix_nano, &bucket.name);
                partitions.entry(key).or_default().push(record);
            }
        })?;
        if outcome.truncated_to.is_some() {
            report.truncated += 1;
        }
        report.records += outcome.records;

        let mut entries = Vec::with_capacity(partitions.len());
        for (key, mut records) in partitions {
            // Templating normally happens at ingest, so pushdown can use the
            // id while the error is still in flight. This is the fallback for
            // records that arrived without one — a WAL written before the id
            // was assigned, or a receiver that skipped it.
            for record in &mut records {
                if record.template_id.is_some() {
                    report.templated += 1;
                    continue;
                }
                match drain.add_line(&record.body, now_unix_secs) {
                    Some(m) => {
                        record.template_id = Some(m.template_id);
                        report.templated += 1;
                        if m.is_new {
                            report.new_templates += 1;
                        }
                    }
                    None => report.untemplated += 1,
                }
            }
            schema::sort_records(&mut records);
            let file_path = key
                .dir(store_dir)
                .join(store::file_name_for_segment(segment_id, segment_uuid));
            let stats = store::write_partition_file(&file_path, &records)?;
            report.bytes_written += stats.bytes;
            entries.push(FileEntry {
                path: file_path,
                key,
                stats,
            });
        }
        report.files_written += entries.len();

        catalog.commit_merge(segment_id, segment_uuid, &entries, now_unix_secs)?;
        // Persist the dictionary alongside the data it describes. Ids must
        // outlive the process or every downstream baseline resets.
        let templates: Vec<_> = drain.templates().cloned().collect();
        catalog.save_templates(&templates)?;

        if std::fs::remove_file(&path).is_ok() {
            report.wal_bytes_freed += segment_bytes;
        }
        report.segments_merged += 1;
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;
    use crate::partition::SECS_PER_HOUR;
    use std::time::Duration;

    fn record(ts_secs: u64, severity: Severity, body: &str) -> LogRecord {
        let ns = ts_secs * 1_000_000_000;
        let mut r = LogRecord::new(ns, severity, body);
        r.observed_unix_nano = ns;
        r.service = Some("api".into());
        r
    }

    fn write_wal(dir: &Path, batches: Vec<Vec<LogRecord>>) {
        let mut w =
            wal::WalWriter::open(dir, 1 << 30, Duration::from_secs(60), 1 << 30).unwrap();
        for b in batches {
            w.append(&b).unwrap();
        }
    }

    #[test]
    fn splits_one_segment_across_level_and_hour_partitions() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_dir, store_dir) = (tmp.path().join("wal"), tmp.path().join("store"));
        write_wal(
            &wal_dir,
            vec![vec![
                record(10, Severity::DEBUG, "d1"),
                record(20, Severity::INFO, "i1"),
                record(30, Severity::ERROR, "e1"),
                // Next hour, same level as the first.
                record(SECS_PER_HOUR + 10, Severity::DEBUG, "d2"),
            ]],
        );

        let mut cat = Catalog::open_in_memory().unwrap();
        let report = merge_all(
            &wal_dir,
            &store_dir,
            &mut cat,
            &LevelBuckets::default(),
            &mut Drain::default(),
            None,
            0,
        )
        .unwrap();

        assert_eq!(report.segments_merged, 1);
        assert_eq!(report.records, 4);
        assert_eq!(report.files_written, 4, "3 buckets in hour 0 + 1 in hour 1");

        let files = cat.list_files().unwrap();
        let mut seen: Vec<_> = files
            .iter()
            .map(|f| (f.key.epoch_hour, f.key.bucket.as_str(), f.stats.rows))
            .collect();
        seen.sort();
        assert_eq!(
            seen,
            vec![
                (0, "debug", 1),
                (0, "info", 1),
                (0, "warn_plus", 1),
                (1, "debug", 1),
            ]
        );
        // WAL is reclaimed once its contents are committed.
        assert!(wal::list_segments(&wal_dir).unwrap().is_empty());
        assert!(report.wal_bytes_freed > 0);
    }

    #[test]
    fn re_merging_after_a_crash_before_commit_does_not_duplicate() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_dir, store_dir) = (tmp.path().join("wal"), tmp.path().join("store"));
        write_wal(&wal_dir, vec![(0..50).map(|i| record(i, Severity::INFO, "x")).collect()]);

        // First attempt: files land, then the process dies before commit. Model
        // that by merging with a throwaway catalog and restoring the segment.
        let segment = wal::list_segments(&wal_dir).unwrap()[0].1.clone();
        let saved = std::fs::read(&segment).unwrap();
        let mut doomed = Catalog::open_in_memory().unwrap();
        merge_all(&wal_dir, &store_dir, &mut doomed, &LevelBuckets::default(), &mut Drain::default(), None, 0).unwrap();
        std::fs::write(&segment, &saved).unwrap();

        // Restart with a catalog that never saw the commit.
        let mut cat = Catalog::open_in_memory().unwrap();
        let report =
            merge_all(&wal_dir, &store_dir, &mut cat, &LevelBuckets::default(), &mut Drain::default(), None, 0).unwrap();

        assert_eq!(report.segments_merged, 1);
        assert_eq!(cat.totals().unwrap().0, 50, "rows must not double");
        assert_eq!(store::list_parquet_files(&store_dir).len(), 1);
    }

    #[test]
    fn a_segment_already_marked_merged_is_deleted_not_reprocessed() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_dir, store_dir) = (tmp.path().join("wal"), tmp.path().join("store"));
        write_wal(&wal_dir, vec![vec![record(1, Severity::INFO, "x")]]);

        let mut cat = Catalog::open_in_memory().unwrap();
        // Pretend the previous run committed but died before deleting.
        let uuid = wal::segment_uuid(&wal::list_segments(&wal_dir).unwrap()[0].1).unwrap();
        cat.commit_merge(1, uuid, &[], 0).unwrap();

        let report =
            merge_all(&wal_dir, &store_dir, &mut cat, &LevelBuckets::default(), &mut Drain::default(), None, 0).unwrap();

        assert_eq!(report.segments_skipped, 1);
        assert_eq!(report.segments_merged, 0);
        assert_eq!(report.records, 0);
        assert!(wal::list_segments(&wal_dir).unwrap().is_empty());
    }

    #[test]
    fn the_active_segment_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_dir, store_dir) = (tmp.path().join("wal"), tmp.path().join("store"));
        write_wal(&wal_dir, vec![vec![record(1, Severity::INFO, "old")]]);
        write_wal(&wal_dir, vec![vec![record(2, Severity::INFO, "live")]]);

        let segments = wal::list_segments(&wal_dir).unwrap();
        assert_eq!(segments.len(), 2);
        let active = segments[1].0;

        let mut cat = Catalog::open_in_memory().unwrap();
        let report = merge_all(
            &wal_dir,
            &store_dir,
            &mut cat,
            &LevelBuckets::default(),
            &mut Drain::default(),
            Some(active),
            0,
        )
        .unwrap();

        assert_eq!(report.segments_merged, 1);
        let left = wal::list_segments(&wal_dir).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].0, active);
    }

    #[test]
    fn segments_newer_than_the_published_active_id_are_left_alone() {
        // The writer rolls to a new segment *before* republishing its id, so a
        // merger acting on a stale id must not touch anything above it. Getting
        // this wrong unlinks a file the writer is still appending to and loses
        // every record written afterwards, with no error anywhere.
        let tmp = tempfile::tempdir().unwrap();
        let (wal_dir, store_dir) = (tmp.path().join("wal"), tmp.path().join("store"));
        for i in 0..3 {
            write_wal(&wal_dir, vec![vec![record(i, Severity::INFO, "x")]]);
        }
        let segments = wal::list_segments(&wal_dir).unwrap();
        assert_eq!(segments.len(), 3);

        // Stale published id: the writer has since rolled to 2 and then 3.
        let mut cat = Catalog::open_in_memory().unwrap();
        let report = merge_all(
            &wal_dir,
            &store_dir,
            &mut cat,
            &LevelBuckets::default(),
            &mut Drain::default(),
            Some(segments[0].0),
            0,
        )
        .unwrap();

        assert_eq!(report.segments_merged, 0, "nothing at or above active");
        assert_eq!(wal::list_segments(&wal_dir).unwrap().len(), 3);
    }

    #[test]
    fn a_torn_segment_merges_what_survived() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_dir, store_dir) = (tmp.path().join("wal"), tmp.path().join("store"));
        write_wal(
            &wal_dir,
            vec![
                (0..10).map(|i| record(i, Severity::INFO, "keep")).collect(),
                (0..10).map(|i| record(i, Severity::INFO, "torn")).collect(),
            ],
        );

        let path = wal::list_segments(&wal_dir).unwrap()[0].1.clone();
        let len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 12)
            .unwrap();

        let mut cat = Catalog::open_in_memory().unwrap();
        let report =
            merge_all(&wal_dir, &store_dir, &mut cat, &LevelBuckets::default(), &mut Drain::default(), None, 0).unwrap();

        assert_eq!(report.truncated, 1);
        assert_eq!(report.records, 10);
        assert_eq!(cat.totals().unwrap().0, 10);
    }

    #[test]
    fn assigns_stable_template_ids_across_merges() {
        let tmp = tempfile::tempdir().unwrap();
        let (wal_dir, store_dir) = (tmp.path().join("wal"), tmp.path().join("store"));
        write_wal(
            &wal_dir,
            vec![(0..100)
                .map(|i| record(i, Severity::INFO, &format!("handled request id={i} in {i}ms")))
                .collect()],
        );

        let mut cat = Catalog::open_in_memory().unwrap();
        let mut drain = Drain::default();
        let first = merge_all(
            &wal_dir, &store_dir, &mut cat, &LevelBuckets::default(), &mut drain, None, 0,
        )
        .unwrap();

        assert_eq!(first.templated, 100);
        assert_eq!(first.untemplated, 0);
        assert_eq!(first.new_templates, 1, "one shape is one template");
        assert_eq!(cat.load_templates().unwrap().len(), 1);

        // A second batch of the same shape, through a Drain restored from the
        // catalog exactly as a restarted agent would.
        write_wal(
            &wal_dir,
            vec![(200..260)
                .map(|i| record(i, Severity::INFO, &format!("handled request id={i} in {i}ms")))
                .collect()],
        );
        let mut restored = Drain::restore(Default::default(), cat.load_templates().unwrap());
        let second = merge_all(
            &wal_dir, &store_dir, &mut cat, &LevelBuckets::default(), &mut restored, None, 0,
        )
        .unwrap();

        assert_eq!(second.templated, 60);
        assert_eq!(
            second.new_templates, 0,
            "a restart must not re-announce known templates as novel"
        );
        let templates = cat.load_templates().unwrap();
        assert_eq!(templates.len(), 1);
        assert_eq!(templates[0].count, 160, "counts accumulate across restarts");
    }

    #[test]
    fn a_reused_segment_id_after_a_restart_is_not_mistaken_for_merged() {
        // Segment file names restart at 1 once the WAL drains. Keying the
        // catalog on that integer made the *new* segment 1 look already-merged,
        // so it was deleted unread — silent data loss on every restart of a
        // drained agent. The catalog keys on the segment UUID for this reason.
        let tmp = tempfile::tempdir().unwrap();
        let (wal_dir, store_dir) = (tmp.path().join("wal"), tmp.path().join("store"));
        let mut cat = Catalog::open_in_memory().unwrap();
        let mut drain = Drain::default();

        write_wal(&wal_dir, vec![vec![record(1, Severity::INFO, "first run")]]);
        assert_eq!(wal::list_segments(&wal_dir).unwrap()[0].0, 1);
        merge_all(&wal_dir, &store_dir, &mut cat, &LevelBuckets::default(), &mut drain, None, 0)
            .unwrap();
        assert!(wal::list_segments(&wal_dir).unwrap().is_empty(), "wal drained");

        // Restart: the writer starts again at id 1 because nothing is left.
        write_wal(&wal_dir, vec![vec![record(2, Severity::INFO, "second run")]]);
        assert_eq!(wal::list_segments(&wal_dir).unwrap()[0].0, 1, "id reused");

        let second = merge_all(
            &wal_dir, &store_dir, &mut cat, &LevelBuckets::default(), &mut drain, None, 0,
        )
        .unwrap();

        assert_eq!(second.segments_merged, 1, "must merge, not skip");
        assert_eq!(second.segments_skipped, 0);
        assert_eq!(second.records, 1);
        assert_eq!(cat.totals().unwrap().0, 2, "both runs' records are committed");
        assert_eq!(
            store::list_parquet_files(&store_dir).len(),
            2,
            "the two segments must not collide on one output path"
        );
    }

    #[test]
    fn merging_an_empty_wal_is_a_no_op() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cat = Catalog::open_in_memory().unwrap();
        let report = merge_all(
            &tmp.path().join("wal"),
            &tmp.path().join("store"),
            &mut cat,
            &LevelBuckets::default(),
            &mut Drain::default(),
            None,
            0,
        )
        .unwrap();
        assert_eq!(report, MergeReport::default());
    }
}
