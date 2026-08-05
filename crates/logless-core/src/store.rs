//! Committed state: partitioned Parquet files.
//!
//! Files are written to a `.tmp` name, fsynced, then atomically renamed into
//! place, so a crash never leaves a half-written file that a reader could pick
//! up. File names are **deterministic** — derived from the WAL segment that
//! produced them — which makes re-merging after a crash idempotent: the same
//! input rewrites the same path rather than duplicating rows.

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::statistics::Statistics;

use crate::model::LogRecord;
use crate::schema;

/// Rows per row group. Small enough that statistics prune usefully on a
/// time-bounded query, large enough that dictionary encoding still pays.
const ROW_GROUP_ROWS: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("store io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parquet error at {path}: {source}")]
    Parquet {
        path: PathBuf,
        #[source]
        source: parquet::errors::ParquetError,
    },
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// What the catalog records about a file. Every field is recoverable from the
/// Parquet footer, which is what makes the catalog rebuildable derived state.
#[derive(Debug, Clone, PartialEq)]
pub struct FileStats {
    pub rows: u64,
    pub bytes: u64,
    pub min_observed_nanos: i64,
    pub max_observed_nanos: i64,
}

/// Deterministic file name for the output of one WAL segment in one partition.
///
/// Keyed on the segment's UUID, not its numeric id: ids restart at 1 whenever
/// the WAL drains, so two unrelated segments would otherwise write to the same
/// path. The id is kept as a readable prefix for humans browsing the store.
pub fn file_name_for_segment(segment_id: u64, segment_uuid: uuid::Uuid) -> String {
    let short = &segment_uuid.simple().to_string()[..12];
    format!("s{segment_id:012}-{short}.parquet")
}

/// Write `records` as one Parquet file at `path`, replacing anything already
/// there. Caller supplies records already sorted; sorting here would hide the
/// cost from the merge loop that owns it.
pub fn write_partition_file(path: &Path, records: &[LogRecord]) -> Result<FileStats, StoreError> {
    let io = |p: &Path| {
        let p = p.to_path_buf();
        move |source| StoreError::Io { path: p.clone(), source }
    };

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io(parent))?;
    }
    let tmp = path.with_extension("parquet.tmp");

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("zstd level 3 is valid"),
        ))
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "logless.schema_version".to_string(),
            schema::SCHEMA_VERSION.to_string(),
        )]))
        .build();

    let batch = schema::to_record_batch(records)?;
    {
        let file = File::create(&tmp).map_err(io(&tmp))?;
        let mut writer = ArrowWriter::try_new(file, schema::schema(), Some(props)).map_err(|e| {
            StoreError::Parquet { path: tmp.clone(), source: e }
        })?;
        if batch.num_rows() > 0 {
            writer.write(&batch).map_err(|e| StoreError::Parquet {
                path: tmp.clone(),
                source: e,
            })?;
        }
        let file = writer.into_inner().map_err(|e| StoreError::Parquet {
            path: tmp.clone(),
            source: e,
        })?;
        // Durable before the rename: a renamed-but-unsynced file is exactly the
        // half-written file the rename was meant to prevent.
        file.sync_all().map_err(io(&tmp))?;
    }

    fs::rename(&tmp, path).map_err(io(path))?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    read_file_stats(path)
}

/// Recover a file's catalog row from its Parquet footer. This is the mechanism
/// behind "the catalog is derived state, never source of truth".
pub fn read_file_stats(path: &Path) -> Result<FileStats, StoreError> {
    let file = File::open(path).map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let bytes = file
        .metadata()
        .map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .len();

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|source| StoreError::Parquet {
            path: path.to_path_buf(),
            source,
        })?;
    let meta = builder.metadata();
    let rows = meta.file_metadata().num_rows().max(0) as u64;

    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for rg in meta.row_groups() {
        for col in rg.columns() {
            if col.column_path().string() != "observed" {
                continue;
            }
            if let Some(Statistics::Int64(s)) = col.statistics() {
                if let Some(v) = s.min_opt() {
                    min = min.min(*v);
                }
                if let Some(v) = s.max_opt() {
                    max = max.max(*v);
                }
            }
        }
    }
    if rows == 0 || min > max {
        min = 0;
        max = 0;
    }

    Ok(FileStats {
        rows,
        bytes,
        min_observed_nanos: min,
        max_observed_nanos: max,
    })
}

/// Every `*.parquet` under `store_dir`, recursively. Skips `.tmp` leftovers.
pub fn list_parquet_files(store_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect(store_dir, &mut out);
    out.sort();
    out
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        match entry.file_type() {
            Ok(t) if t.is_dir() => collect(&path, out),
            Ok(t) if t.is_file() => {
                if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    out.push(path);
                }
            }
            _ => {}
        }
    }
}

/// Remove `.parquet.tmp` files left by a crash mid-write.
pub fn clean_temp_files(store_dir: &Path) -> usize {
    fn walk(dir: &Path, removed: &mut usize) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(t) if t.is_dir() => walk(&path, removed),
                Ok(t) if t.is_file() => {
                    if path.extension().and_then(|e| e.to_str()) == Some("tmp")
                        && fs::remove_file(&path).is_ok()
                    {
                        *removed += 1;
                    }
                }
                _ => {}
            }
        }
    }
    let mut removed = 0;
    walk(store_dir, &mut removed);
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;

    fn records(n: usize, base_ts: u64) -> Vec<LogRecord> {
        (0..n)
            .map(|i| {
                let ts = base_ts + i as u64;
                let mut r = LogRecord::new(ts, Severity::INFO, format!("request {i} completed"));
                r.observed_unix_nano = ts;
                r.service = Some("api".into());
                r
            })
            .collect()
    }

    #[test]
    fn writes_and_reports_stats_from_the_footer() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp
            .path()
            .join("hour=1970-01-01T00/level=info")
            .join(file_name_for_segment(1, uuid::Uuid::now_v7()));

        let written = write_partition_file(&path, &records(1000, 5_000)).unwrap();
        assert_eq!(written.rows, 1000);
        assert_eq!(written.min_observed_nanos, 5_000);
        assert_eq!(written.max_observed_nanos, 5_999);
        assert!(written.bytes > 0);

        // The point of the footer round trip: the catalog can be rebuilt from it.
        assert_eq!(read_file_stats(&path).unwrap(), written);
    }

    #[test]
    fn rewriting_the_same_path_is_idempotent_not_additive() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("p/s000000000001.parquet");

        let first = write_partition_file(&path, &records(100, 0)).unwrap();
        let second = write_partition_file(&path, &records(100, 0)).unwrap();

        // A crash between writing the file and committing the catalog row means
        // this exact rewrite happens on restart. It must not double the rows.
        assert_eq!(first.rows, 100);
        assert_eq!(second.rows, 100);
        assert_eq!(list_parquet_files(tmp.path()).len(), 1);
    }

    #[test]
    fn leaves_no_temp_file_behind() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("p/s000000000001.parquet");
        write_partition_file(&path, &records(10, 0)).unwrap();

        let strays: Vec<_> = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(strays.is_empty(), "stray temp files: {strays:?}");
    }

    #[test]
    fn cleans_temp_files_left_by_a_crash() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("hour=1970-01-01T00/level=info");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("s000000000009.parquet.tmp"), b"partial").unwrap();
        write_partition_file(&dir.join("s000000000001.parquet"), &records(5, 0)).unwrap();

        assert_eq!(clean_temp_files(tmp.path()), 1);
        assert_eq!(list_parquet_files(tmp.path()).len(), 1);
    }

    #[test]
    fn zstd_and_dictionary_actually_compress_repetitive_logs() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("p/s000000000001.parquet");
        let recs = records(20_000, 0);

        let raw: usize = recs.iter().map(|r| r.body.len() + 64).sum();
        let stats = write_partition_file(&path, &recs).unwrap();

        // Not a benchmark, a regression guard: if compression or dictionary
        // encoding gets switched off, this fails loudly.
        assert!(
            stats.bytes * 4 < raw as u64,
            "expected >4x on repetitive logs, got {} vs {raw}",
            stats.bytes
        );
    }

    #[test]
    fn empty_partition_file_is_readable() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("p/s000000000001.parquet");
        let stats = write_partition_file(&path, &[]).unwrap();
        assert_eq!(stats.rows, 0);
        assert_eq!(read_file_stats(&path).unwrap().rows, 0);
    }
}
