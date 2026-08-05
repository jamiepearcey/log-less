//! Reading committed Parquet back — the replay side of "Parquet is the API".
//!
//! Not a query engine, and the absence is the design (`docs/architecture.md`
//! §7): users point their own DuckDB, Polars or Grafana at the directory. What
//! the agent itself needs is narrower and worth owning — pull back a bounded
//! slice of history by time, service and level, so `logless replay` can push it
//! upstream on demand. That is a scan with three predicates, not SQL.
//!
//! Pruning happens at three levels, cheapest first:
//!
//! 1. **Partition directories.** `hour=…/level=…` is in the path, so a query
//!    for the last hour of errors never opens a debug file from yesterday.
//! 2. **Row-group statistics.** Each group's min/max `observed` is in the
//!    footer, so a file spanning an hour is skipped whole when the window is a
//!    minute.
//! 3. **Rows.** Only what survives the first two is decoded.

use std::path::{Path, PathBuf};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::statistics::Statistics;

use crate::model::{LevelClass, LogRecord, Severity};
use crate::partition;
use crate::store::{self, StoreError};

/// What to pull back. Every field is optional; the defaults scan everything,
/// which is occasionally what you want and always what you get if you forget.
#[derive(Debug, Clone, Default)]
pub struct Query {
    /// Inclusive lower bound on `observed`, unix nanoseconds.
    pub from_unix_nano: Option<u64>,
    /// Exclusive upper bound.
    pub until_unix_nano: Option<u64>,
    pub service: Option<String>,
    pub min_severity: Option<Severity>,
    /// Substring match on the body. Deliberately not a regex: this is a replay
    /// filter, not a search engine, and a pathological pattern here would stall
    /// the agent rather than a user's own tool.
    pub contains: Option<String>,
    /// Stop after this many rows. A replay that pulls a day of DEBUG into
    /// memory is a worse outage than the one being investigated.
    pub limit: usize,
}

impl Query {
    pub fn new() -> Self {
        Self { limit: 10_000, ..Default::default() }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ScanStats {
    pub files_considered: u64,
    pub files_pruned_by_partition: u64,
    pub row_groups_pruned: u64,
    pub rows_scanned: u64,
    pub rows_matched: u64,
}

pub struct ScanResult {
    pub records: Vec<LogRecord>,
    pub stats: ScanStats,
    /// True when `limit` cut the result short, so a caller can say so rather
    /// than implying it replayed everything.
    pub truncated: bool,
}

/// Scans the committed store.
pub fn scan(store_dir: &Path, query: &Query) -> Result<ScanResult, StoreError> {
    let mut stats = ScanStats::default();
    let mut records = Vec::new();
    let mut truncated = false;

    let mut files = store::list_parquet_files(store_dir);
    // Newest first: a replay is nearly always about what just happened, and it
    // means `limit` cuts the oldest rows rather than the most relevant ones.
    files.sort();
    files.reverse();

    for path in files {
        stats.files_considered += 1;
        if !partition_matches(&path, query) {
            stats.files_pruned_by_partition += 1;
            continue;
        }
        let (found, group_skips, rows) = scan_file(&path, query, query.limit - records.len())?;
        stats.row_groups_pruned += group_skips;
        stats.rows_scanned += rows;
        stats.rows_matched += found.len() as u64;
        records.extend(found);
        if records.len() >= query.limit {
            truncated = true;
            break;
        }
    }

    records.sort_by_key(|r| r.observed_unix_nano);
    Ok(ScanResult { records, stats, truncated })
}

/// Uses the `hour=…/level=…` path to skip whole files without opening them.
fn partition_matches(path: &Path, query: &Query) -> bool {
    let mut hour = None;
    let mut level = None;
    for component in path.components() {
        let text = component.as_os_str().to_string_lossy();
        if let Some(value) = text.strip_prefix("hour=") {
            hour = partition::parse_hour(value);
        } else if let Some(value) = text.strip_prefix("level=") {
            level = Some(value.to_string());
        }
    }

    if let (Some(epoch_hour), Some(from)) = (hour, query.from_unix_nano) {
        // The last nanosecond of this partition's hour.
        let hour_end = (epoch_hour + 1) * 3_600 * 1_000_000_000;
        if hour_end <= from {
            return false;
        }
    }
    if let (Some(epoch_hour), Some(until)) = (hour, query.until_unix_nano) {
        let hour_start = epoch_hour * 3_600 * 1_000_000_000;
        if hour_start >= until {
            return false;
        }
    }
    // Level buckets are configurable, so only the default names can be pruned
    // by name. An unrecognised bucket is scanned rather than skipped: skipping
    // it would silently omit data from a store the user configured themselves.
    if let (Some(level), Some(min)) = (&level, query.min_severity) {
        let bucket_max = match level.as_str() {
            "debug" => Some(LevelClass::Debug),
            "info" => Some(LevelClass::Info),
            "warn_plus" => Some(LevelClass::WarnPlus),
            _ => None,
        };
        if let Some(bucket) = bucket_max {
            if bucket < min.class() {
                return false;
            }
        }
    }
    true
}

/// Scans one file, pruning row groups by their `observed` statistics.
fn scan_file(
    path: &Path,
    query: &Query,
    remaining: usize,
) -> Result<(Vec<LogRecord>, u64, u64), StoreError> {
    let file = std::fs::File::open(path)
        .map_err(|source| StoreError::Io { path: path.to_path_buf(), source })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|source| StoreError::Parquet { path: path.to_path_buf(), source })?;

    let mut keep_groups = Vec::new();
    let mut pruned = 0u64;
    for (index, group) in builder.metadata().row_groups().iter().enumerate() {
        if row_group_matches(group, query) {
            keep_groups.push(index);
        } else {
            pruned += 1;
        }
    }
    if keep_groups.is_empty() {
        return Ok((Vec::new(), pruned, 0));
    }

    let reader = builder
        .with_row_groups(keep_groups)
        .build()
        .map_err(|source| StoreError::Parquet { path: path.to_path_buf(), source })?;

    let mut out = Vec::new();
    let mut scanned = 0u64;
    for batch in reader {
        let batch = batch.map_err(|source| StoreError::Parquet {
            path: path.to_path_buf(),
            source: parquet::errors::ParquetError::ArrowError(source.to_string()),
        })?;
        scanned += batch.num_rows() as u64;
        for record in crate::schema::from_record_batch(&batch) {
            if matches(&record, query) {
                out.push(record);
                if out.len() >= remaining {
                    return Ok((out, pruned, scanned));
                }
            }
        }
    }
    Ok((out, pruned, scanned))
}

fn row_group_matches(group: &parquet::file::metadata::RowGroupMetaData, query: &Query) -> bool {
    let (Some(from), Some(until)) = (query.from_unix_nano, query.until_unix_nano) else {
        // With an open-ended window there is nothing to prune on; a one-sided
        // bound is handled by the same comparison with a saturating default.
        let from = query.from_unix_nano.unwrap_or(0);
        let until = query.until_unix_nano.unwrap_or(u64::MAX);
        return group_overlaps(group, from, until);
    };
    group_overlaps(group, from, until)
}

fn group_overlaps(
    group: &parquet::file::metadata::RowGroupMetaData,
    from: u64,
    until: u64,
) -> bool {
    for column in group.columns() {
        if column.column_path().string() != "observed" {
            continue;
        }
        if let Some(Statistics::Int64(s)) = column.statistics() {
            if let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) {
                let (min, max) = (*min as u64, *max as u64);
                return max >= from && min < until;
            }
        }
    }
    // No statistics is not "no rows": scan it.
    true
}

fn matches(record: &LogRecord, query: &Query) -> bool {
    if query.from_unix_nano.is_some_and(|t| record.observed_unix_nano < t) {
        return false;
    }
    if query.until_unix_nano.is_some_and(|t| record.observed_unix_nano >= t) {
        return false;
    }
    if let Some(service) = &query.service {
        if record.service.as_deref() != Some(service.as_str()) {
            return false;
        }
    }
    if query.min_severity.is_some_and(|s| record.severity < s) {
        return false;
    }
    if let Some(needle) = &query.contains {
        if !record.body.contains(needle.as_str()) {
            return false;
        }
    }
    true
}

/// Convenience for the common replay: "everything for this service in the last
/// N seconds, at or above this level".
pub fn recent(
    store_dir: &Path,
    now_unix_nano: u64,
    window_secs: u64,
    service: Option<String>,
    min_severity: Option<Severity>,
    limit: usize,
) -> Result<ScanResult, StoreError> {
    scan(
        store_dir,
        &Query {
            from_unix_nano: Some(now_unix_nano.saturating_sub(window_secs * 1_000_000_000)),
            until_unix_nano: None,
            service,
            min_severity,
            contains: None,
            limit,
        },
    )
}

/// Files that would be opened for a query, for `--explain`-style reporting.
pub fn plan(store_dir: &Path, query: &Query) -> Result<Vec<PathBuf>, StoreError> {
    Ok(store::list_parquet_files(store_dir)
        .into_iter()
        .filter(|p| partition_matches(p, query))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LevelBuckets;
    use crate::model::Severity;

    const HOUR_NS: u64 = 3_600 * 1_000_000_000;

    fn record(observed: u64, severity: Severity, service: &str, body: &str) -> LogRecord {
        let mut r = LogRecord::new(observed, severity, body);
        r.observed_unix_nano = observed;
        r.service = Some(service.to_string());
        r
    }

    /// Writes records into the partition layout the merger produces.
    fn write_store(dir: &Path, records: Vec<LogRecord>) {
        let buckets = LevelBuckets::default();
        let mut by_partition: std::collections::HashMap<_, Vec<LogRecord>> =
            std::collections::HashMap::new();
        for record in records {
            let key = crate::partition::PartitionKey {
                epoch_hour: record.observed_unix_nano / HOUR_NS,
                bucket: buckets.bucket_for(record.severity).name.clone(),
            };
            by_partition.entry(key).or_default().push(record);
        }
        for (key, mut records) in by_partition {
            crate::schema::sort_records(&mut records);
            let partition_dir = key.dir(dir);
            std::fs::create_dir_all(&partition_dir).unwrap();
            let path = partition_dir
                .join(store::file_name_for_segment(1, uuid::Uuid::now_v7()));
            store::write_partition_file(&path, &records).unwrap();
        }
    }

    #[test]
    fn finds_records_in_a_time_window() {
        let tmp = tempfile::tempdir().unwrap();
        write_store(
            tmp.path(),
            vec![
                record(2 * HOUR_NS, Severity::ERROR, "api", "old failure"),
                record(10 * HOUR_NS, Severity::ERROR, "api", "recent failure"),
            ],
        );
        let result = scan(
            tmp.path(),
            &Query {
                from_unix_nano: Some(9 * HOUR_NS),
                limit: 100,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].body, "recent failure");
    }

    #[test]
    fn partitions_outside_the_window_are_never_opened() {
        // The cheapest prune, and the one that makes a day of DEBUG free to
        // ignore when the question is about the last ten minutes.
        let tmp = tempfile::tempdir().unwrap();
        write_store(
            tmp.path(),
            (0..6)
                .map(|h| record(h * HOUR_NS, Severity::ERROR, "api", "x"))
                .collect(),
        );
        let query = Query { from_unix_nano: Some(5 * HOUR_NS), limit: 100, ..Default::default() };
        let result = scan(tmp.path(), &query).unwrap();
        assert!(result.stats.files_pruned_by_partition >= 4, "{:?}", result.stats);
        assert_eq!(plan(tmp.path(), &query).unwrap().len(), 1);
    }

    #[test]
    fn level_partitions_below_the_minimum_are_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        write_store(
            tmp.path(),
            vec![
                record(HOUR_NS, Severity::DEBUG, "api", "noise"),
                record(HOUR_NS, Severity::INFO, "api", "chatter"),
                record(HOUR_NS, Severity::ERROR, "api", "the failure"),
            ],
        );
        let result = scan(
            tmp.path(),
            &Query { min_severity: Some(Severity::WARN), limit: 100, ..Default::default() },
        )
        .unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].body, "the failure");
        assert!(result.stats.files_pruned_by_partition >= 2);
    }

    #[test]
    fn filters_by_service_and_body() {
        let tmp = tempfile::tempdir().unwrap();
        write_store(
            tmp.path(),
            vec![
                record(HOUR_NS, Severity::ERROR, "api", "payment declined"),
                record(HOUR_NS, Severity::ERROR, "worker", "payment declined"),
                record(HOUR_NS, Severity::ERROR, "api", "disk full"),
            ],
        );
        let result = scan(
            tmp.path(),
            &Query {
                service: Some("api".into()),
                contains: Some("payment".into()),
                limit: 100,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].service.as_deref(), Some("api"));
    }

    #[test]
    fn a_limit_truncates_and_says_so() {
        // A replay that pulls a day of DEBUG into memory is a worse outage
        // than the one being investigated.
        let tmp = tempfile::tempdir().unwrap();
        write_store(
            tmp.path(),
            (0..50).map(|i| record(HOUR_NS + i, Severity::ERROR, "api", "x")).collect(),
        );
        let result = scan(tmp.path(), &Query { limit: 10, ..Default::default() }).unwrap();
        assert_eq!(result.records.len(), 10);
        assert!(result.truncated, "a caller must be able to say the replay was cut short");
    }

    #[test]
    fn results_come_back_in_time_order() {
        let tmp = tempfile::tempdir().unwrap();
        write_store(
            tmp.path(),
            vec![
                record(HOUR_NS + 300, Severity::ERROR, "api", "third"),
                record(HOUR_NS + 100, Severity::ERROR, "api", "first"),
                record(HOUR_NS + 200, Severity::ERROR, "api", "second"),
            ],
        );
        let result = scan(tmp.path(), &Query { limit: 100, ..Default::default() }).unwrap();
        let bodies: Vec<&str> = result.records.iter().map(|r| r.body.as_str()).collect();
        assert_eq!(bodies, ["first", "second", "third"]);
    }

    #[test]
    fn an_empty_store_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let result = scan(tmp.path(), &Query::new()).unwrap();
        assert!(result.records.is_empty());
        assert!(!result.truncated);
    }

    #[test]
    fn recent_is_a_window_ending_now() {
        let tmp = tempfile::tempdir().unwrap();
        let now = 10 * HOUR_NS;
        write_store(
            tmp.path(),
            vec![
                record(now - 30 * 1_000_000_000, Severity::ERROR, "api", "inside"),
                record(now - 3 * HOUR_NS, Severity::ERROR, "api", "outside"),
            ],
        );
        let result = recent(tmp.path(), now, 60, None, Some(Severity::ERROR), 100).unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].body, "inside");
    }

    #[test]
    fn an_unknown_level_bucket_is_scanned_rather_than_skipped() {
        // Buckets are configurable; skipping one we do not recognise would
        // silently omit data from a store the user configured themselves.
        let tmp = tempfile::tempdir().unwrap();
        let key = crate::partition::PartitionKey {
            epoch_hour: 1,
            bucket: "audit".to_string(),
        };
        let partition_dir = key.dir(tmp.path());
        std::fs::create_dir_all(&partition_dir).unwrap();
        store::write_partition_file(
            &partition_dir.join(store::file_name_for_segment(1, uuid::Uuid::now_v7())),
            &[record(HOUR_NS, Severity::ERROR, "api", "in a custom bucket")],
        )
        .unwrap();
        let result = scan(
            tmp.path(),
            &Query { min_severity: Some(Severity::ERROR), limit: 10, ..Default::default() },
        )
        .unwrap();
        assert_eq!(result.records.len(), 1);
    }
}
