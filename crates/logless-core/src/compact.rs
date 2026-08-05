//! Compacting small per-segment files into larger ones.
//!
//! The merger writes one file per (WAL segment × partition), so a busy agent
//! with a short segment produces many small files in the same hour. Measured:
//! 200k records became 33 files at 2.5× compression, where the design assumes
//! 10–20×. Small files hurt three ways at once — a zstd block and a dictionary
//! per file, row-group statistics too coarse to prune on, and one open per file
//! for every reader.
//!
//! Compaction is deliberately a *maintenance* operation, not part of the write
//! path: it rewrites files that are already durable and already correct, so it
//! can be interrupted at any point without losing anything.
//!
//! The order is what makes that safe: write the replacement, fsync it, record
//! it, then unlink the inputs. A crash between any two steps leaves either the
//! originals or both — never a gap. Both means duplicate rows for one window,
//! which the catalog rebuild resolves, and which is why the inputs are removed
//! last rather than first.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::catalog::Catalog;
use crate::model::LogRecord;
use crate::partition::{self, PartitionKey};
use crate::schema;
use crate::store::{self, StoreError};

#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Catalog(#[from] crate::catalog::CatalogError),
}

#[derive(Debug, Clone, Copy)]
pub struct CompactConfig {
    /// Compact a partition only when it has at least this many files. Below it,
    /// the rewrite costs more than the fragmentation.
    pub min_files: usize,
    /// Stop adding inputs once the output would exceed this. Keeps one
    /// compaction bounded in memory and in time.
    pub target_bytes: u64,
    /// Files at or above this are already big enough and are left alone, so
    /// compaction does not rewrite the same data every pass.
    pub keep_above_bytes: u64,
    /// Partitions compacted per pass, so one maintenance tick stays short.
    pub max_partitions: usize,
}

impl Default for CompactConfig {
    fn default() -> Self {
        Self {
            min_files: 4,
            target_bytes: 128 * 1024 * 1024,
            keep_above_bytes: 32 * 1024 * 1024,
            max_partitions: 4,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompactReport {
    pub partitions_compacted: u64,
    pub files_replaced: u64,
    pub files_written: u64,
    pub rows: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

impl CompactReport {
    /// Space saved, as a fraction. Negative would mean compaction made things
    /// worse, which is worth surfacing rather than averaging away.
    pub fn saved_fraction(&self) -> f64 {
        if self.bytes_before == 0 {
            return 0.0;
        }
        1.0 - (self.bytes_after as f64 / self.bytes_before as f64)
    }
}

/// Compacts the store, skipping the partition currently being written to.
///
/// `active_hour` is the epoch hour the merger may still be appending files for;
/// compacting it would race the writer for no benefit, since it is about to
/// gain more files anyway.
pub fn compact(
    store_dir: &Path,
    catalog: &mut Catalog,
    config: &CompactConfig,
    active_hour: Option<u64>,
) -> Result<CompactReport, CompactError> {
    let mut report = CompactReport::default();
    let mut by_partition: HashMap<PartitionKey, Vec<PathBuf>> = HashMap::new();
    for path in store::list_parquet_files(store_dir) {
        if let Some(key) = partition_of(&path) {
            by_partition.entry(key).or_default().push(path);
        }
    }

    let mut partitions: Vec<_> = by_partition.into_iter().collect();
    // Oldest first: a partition that will not grow again is the one worth
    // rewriting, and doing them in a stable order makes a pass reproducible.
    partitions.sort_by(|a, b| a.0.epoch_hour.cmp(&b.0.epoch_hour).then(a.0.bucket.cmp(&b.0.bucket)));

    for (key, mut files) in partitions {
        if report.partitions_compacted as usize >= config.max_partitions {
            break;
        }
        if active_hour == Some(key.epoch_hour) {
            continue;
        }
        files.sort();
        let candidates: Vec<PathBuf> = files
            .into_iter()
            .filter(|p| {
                std::fs::metadata(p).map(|m| m.len() < config.keep_above_bytes).unwrap_or(false)
            })
            .collect();
        if candidates.len() < config.min_files {
            continue;
        }

        let mut records: Vec<LogRecord> = Vec::new();
        let mut consumed: Vec<PathBuf> = Vec::new();
        let mut bytes_before = 0u64;
        for path in candidates {
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            if bytes_before + size > config.target_bytes && !consumed.is_empty() {
                break;
            }
            records.extend(read_all(&path)?);
            bytes_before += size;
            consumed.push(path);
        }
        if consumed.len() < config.min_files {
            continue;
        }

        // Same sort the merger applies, so a compacted file has the same
        // clustering — and so the same dictionary and zstd behaviour — as one
        // written in a single pass.
        schema::sort_records(&mut records);
        let rows = records.len() as u64;

        // A compacted file is identified by a fresh UUID and a segment id of 0.
        // Zero is reserved: no WAL segment has it, so a compacted file can
        // never be confused with a merged one, and `is_segment_merged` cannot
        // be tricked into skipping a real segment by a compaction artefact.
        let out_name = store::file_name_for_segment(0, uuid::Uuid::now_v7());
        let dir = key.dir(store_dir);
        std::fs::create_dir_all(&dir)
            .map_err(|source| StoreError::Io { path: dir.clone(), source })?;
        let out_path = dir.join(out_name);
        let stats = store::write_partition_file(&out_path, &records)?;

        // Recorded before the inputs are removed. A crash here leaves both, and
        // duplicate rows are recoverable; a gap is not.
        catalog.record_compaction(&out_path, &stats, &consumed)?;

        for path in &consumed {
            let _ = std::fs::remove_file(path);
        }

        report.partitions_compacted += 1;
        report.files_replaced += consumed.len() as u64;
        report.files_written += 1;
        report.rows += rows;
        report.bytes_before += bytes_before;
        report.bytes_after += stats.bytes;
    }

    Ok(report)
}

fn partition_of(path: &Path) -> Option<PartitionKey> {
    let mut hour = None;
    let mut bucket = None;
    for component in path.components() {
        let text = component.as_os_str().to_string_lossy();
        if let Some(value) = text.strip_prefix("hour=") {
            hour = partition::parse_hour(value);
        } else if let Some(value) = text.strip_prefix("level=") {
            bucket = Some(value.to_string());
        }
    }
    Some(PartitionKey { epoch_hour: hour?, bucket: bucket? })
}

fn read_all(path: &Path) -> Result<Vec<LogRecord>, StoreError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(path)
        .map_err(|source| StoreError::Io { path: path.to_path_buf(), source })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .and_then(|b| b.build())
        .map_err(|source| StoreError::Parquet { path: path.to_path_buf(), source })?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|source| StoreError::Parquet {
            path: path.to_path_buf(),
            source: parquet::errors::ParquetError::ArrowError(source.to_string()),
        })?;
        out.extend(schema::from_record_batch(&batch));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;

    const HOUR_NS: u64 = 3_600 * 1_000_000_000;

    fn write_file(store: &Path, hour: u64, bucket: &str, segment: u64, count: usize) -> PathBuf {
        let key = PartitionKey { epoch_hour: hour, bucket: bucket.to_string() };
        let dir = key.dir(store);
        std::fs::create_dir_all(&dir).unwrap();
        let records: Vec<LogRecord> = (0..count)
            .map(|i| {
                let mut r = LogRecord::new(
                    hour * HOUR_NS + i as u64,
                    Severity::ERROR,
                    format!("payment gateway timeout for order {i}"),
                );
                r.observed_unix_nano = hour * HOUR_NS + i as u64;
                r.service = Some("api".into());
                r
            })
            .collect();
        let path = dir.join(store::file_name_for_segment(segment, uuid::Uuid::now_v7()));
        store::write_partition_file(&path, &records).unwrap();
        path
    }

    #[test]
    fn many_small_files_become_one_and_the_rows_all_survive() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::open_in_memory().unwrap();
        for segment in 1..=8 {
            write_file(tmp.path(), 10, "warn_plus", segment, 100);
        }
        assert_eq!(store::list_parquet_files(tmp.path()).len(), 8);

        let report = compact(tmp.path(), &mut catalog, &CompactConfig::default(), None).unwrap();
        assert_eq!(report.files_replaced, 8);
        assert_eq!(report.files_written, 1);
        assert_eq!(report.rows, 800);
        assert_eq!(store::list_parquet_files(tmp.path()).len(), 1);

        // The rows are all still there and still queryable.
        let result = crate::scan::scan(
            tmp.path(),
            &crate::scan::Query { limit: 10_000, ..Default::default() },
        )
        .unwrap();
        assert_eq!(result.records.len(), 800);
    }

    #[test]
    fn compaction_actually_shrinks_the_store() {
        // The claim being made is a compression one, so it is measured rather
        // than assumed.
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::open_in_memory().unwrap();
        for segment in 1..=16 {
            write_file(tmp.path(), 10, "warn_plus", segment, 200);
        }
        let report = compact(tmp.path(), &mut catalog, &CompactConfig::default(), None).unwrap();
        assert!(
            report.bytes_after < report.bytes_before,
            "compaction grew the store: {} -> {}",
            report.bytes_before,
            report.bytes_after
        );
        println!(
            "compaction: {} -> {} bytes ({:.0}% saved) over {} rows",
            report.bytes_before,
            report.bytes_after,
            report.saved_fraction() * 100.0,
            report.rows
        );
    }

    #[test]
    fn a_partition_below_the_threshold_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::open_in_memory().unwrap();
        for segment in 1..=3 {
            write_file(tmp.path(), 10, "warn_plus", segment, 10);
        }
        let report = compact(tmp.path(), &mut catalog, &CompactConfig::default(), None).unwrap();
        assert_eq!(report.partitions_compacted, 0, "rewriting 3 files is not worth it");
        assert_eq!(store::list_parquet_files(tmp.path()).len(), 3);
    }

    #[test]
    fn the_partition_being_written_to_is_skipped() {
        // Compacting the hour the merger is still appending to races a writer
        // for no benefit — it is about to gain more files anyway.
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::open_in_memory().unwrap();
        for segment in 1..=8 {
            write_file(tmp.path(), 10, "warn_plus", segment, 10);
        }
        let report = compact(tmp.path(), &mut catalog, &CompactConfig::default(), Some(10)).unwrap();
        assert_eq!(report.partitions_compacted, 0);
    }

    #[test]
    fn partitions_are_compacted_independently() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::open_in_memory().unwrap();
        for segment in 1..=5 {
            write_file(tmp.path(), 10, "warn_plus", segment, 10);
            write_file(tmp.path(), 10, "debug", segment, 10);
            write_file(tmp.path(), 11, "warn_plus", segment, 10);
        }
        let report = compact(tmp.path(), &mut catalog, &CompactConfig::default(), None).unwrap();
        assert_eq!(report.partitions_compacted, 3);
        assert_eq!(store::list_parquet_files(tmp.path()).len(), 3, "one file per partition");
        // Retention still works on the result: the level directories survive.
        let dirs: Vec<String> = store::list_parquet_files(tmp.path())
            .iter()
            .map(|p| p.parent().unwrap().file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(dirs.contains(&"level=debug".to_string()));
        assert!(dirs.contains(&"level=warn_plus".to_string()));
    }

    #[test]
    fn already_large_files_are_not_rewritten_every_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::open_in_memory().unwrap();
        for segment in 1..=8 {
            write_file(tmp.path(), 10, "warn_plus", segment, 50);
        }
        let config = CompactConfig { keep_above_bytes: 1, ..Default::default() };
        let report = compact(tmp.path(), &mut catalog, &config, None).unwrap();
        assert_eq!(report.partitions_compacted, 0, "everything is already 'big enough'");
    }

    #[test]
    fn the_catalog_reflects_the_compacted_file() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::open_in_memory().unwrap();
        for segment in 1..=6 {
            write_file(tmp.path(), 10, "warn_plus", segment, 20);
        }
        // Catalog knows about the originals first, as the merger would have
        // recorded them.
        catalog.rebuild(tmp.path()).unwrap();
        assert_eq!(catalog.list_files().unwrap().len(), 6);

        compact(tmp.path(), &mut catalog, &CompactConfig::default(), None).unwrap();
        let files = catalog.list_files().unwrap();
        assert_eq!(files.len(), 1, "stale rows must not survive compaction: {files:?}");
        assert_eq!(files[0].stats.rows, 120);
        assert!(files[0].path.exists(), "the catalog must point at a file that is there");
    }
}
