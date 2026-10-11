//! One-off move of a store from the layout before tenants and indexes
//! (`hour=<observed hour>/level=<bucket>/<file>`) into the current tree
//! (`tenant=/index=/hour=<event hour>/level=/node=/<file>`, `crate::partition`).
//!
//! The old tree is cut on `observed`, the new one on `timestamp`, so a file
//! cannot simply be moved: its rows are regrouped by event hour and rewritten
//! (which also gives them 8k row groups, the `timestamp` sort and the `origin`
//! column). Everything goes to the own tenant's default index under this
//! instance's node id; the old layout had no other.
//!
//! One old file at a time, so memory is one file's rows. Each output's name is
//! derived from the old file's path and the event hour, so a run interrupted
//! between writing the outputs and removing the input writes the same files
//! again when repeated instead of doubling the rows. The catalog's file table
//! is rebuilt from the new tree at the end.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::catalog::{Catalog, CatalogError};
use crate::config::Tenancy;
use crate::model::LogRecord;
use crate::partition::{self, PartitionKey};
use crate::schema;
use crate::store::{self, StoreError};

#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrateReport {
    pub files_read: u64,
    pub files_written: u64,
    pub rows: u64,
    /// Rows whose event hour is not the hour of the directory they were in:
    /// the ones the old layout could not prune by directory.
    pub rows_moved_hour: u64,
    /// Files in the catalog after the rebuild.
    pub catalog_files: u64,
}

/// Old-layout files: `<store>/hour=<h>/level=<l>/<file>.parquet`, with the hour.
fn old_files(store_dir: &Path) -> Vec<(PathBuf, u64)> {
    store::list_parquet_files(store_dir)
        .into_iter()
        .filter_map(|path| {
            let rel = path.strip_prefix(store_dir).ok()?;
            let mut parts = rel.components().map(|c| c.as_os_str().to_str());
            let hour = partition::parse_hour(parts.next()??.strip_prefix("hour=")?)?;
            parts.next()??.strip_prefix("level=")?;
            (parts.count() == 1).then_some((path, hour))
        })
        .collect()
}

/// Rewrites every old-layout file under `store_dir` into the new tree and
/// rebuilds the catalog. Safe to repeat; a store already migrated has nothing
/// to do.
pub fn migrate(store_dir: &Path, catalog: &mut Catalog, tenancy: &Tenancy) -> Result<MigrateReport, MigrateError> {
    let mut report = MigrateReport::default();
    let layout = catalog.typed_layout()?;
    let index = tenancy.default_index();
    let buckets = tenancy.default_buckets();
    for (old, old_hour) in old_files(store_dir) {
        let mut by_key: BTreeMap<PartitionKey, Vec<LogRecord>> = BTreeMap::new();
        for record in crate::compact::read_all(&old)? {
            let hour = record.timestamp_unix_nano / partition::NANOS_PER_HOUR;
            report.rows += 1;
            report.rows_moved_hour += u64::from(hour != old_hour);
            let key = PartitionKey::new(tenancy.tenant(), index, hour, &buckets.bucket_for(record.severity).name, tenancy.node());
            by_key.entry(key).or_default().push(record);
        }
        let rel = old.strip_prefix(store_dir).unwrap_or(&old).to_string_lossy().into_owned();
        for (key, mut records) in by_key {
            schema::sort_records(&mut records);
            let digest = Sha256::digest(format!("{rel}\u{1f}{}", key.epoch_hour).as_bytes());
            let id = uuid::Uuid::from_slice(&digest[..16]).expect("16 bytes");
            // Segment id 0, like a compacted file: no WAL segment has it.
            store::write_partition_file_with(&key.dir(store_dir).join(store::file_name_for_segment(0, id)), &records, &layout)?;
            report.files_written += 1;
        }
        std::fs::remove_file(&old).map_err(|source| StoreError::Io { path: old.clone(), source })?;
        report.files_read += 1;
        // The directories empty out as their files go; a non-empty one stays.
        for dir in old.ancestors().skip(1).take(2) {
            let _ = std::fs::remove_dir(dir);
        }
    }
    report.catalog_files = catalog.rebuild(store_dir)? as u64;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;

    const HOUR_NS: u64 = partition::NANOS_PER_HOUR;

    fn row(event_hour: u64, observed_hour: u64, i: u64, sev: Severity) -> LogRecord {
        let mut r = LogRecord::new(event_hour * HOUR_NS + i, sev, format!("line {i}"));
        r.observed_unix_nano = observed_hour * HOUR_NS + i;
        r.service = Some("api".into());
        r
    }

    #[test]
    fn an_old_store_is_rewritten_by_event_hour_and_a_second_run_changes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        // Observed hour 500: 30 live rows, and 20 replayed rows whose events were in hour 100.
        let mut rows: Vec<_> = (0..30).map(|i| row(500, 500, i, Severity::INFO)).collect();
        rows.extend((0..20).map(|i| row(100, 500, 100 + i, Severity::INFO)));
        let old = store.join("hour=1970-01-21T20/level=info/s000000000001-abc.parquet");
        store::write_partition_file(&old, &rows).unwrap();
        store::write_partition_file(&store.join("hour=1970-01-21T20/level=warn_plus/s000000000001-abc.parquet"), &[row(500, 500, 1, Severity::ERROR)]).unwrap();
        // The dictionary is not a store file and stays where it is.
        std::fs::create_dir_all(store.join("_meta")).unwrap();
        std::fs::write(store.join("_meta/templates.pq"), b"dict").unwrap();
        assert!(partition::is_old_layout(&store));

        let mut catalog = Catalog::open_in_memory().unwrap();
        let tenancy = Tenancy::default();
        let report = migrate(&store, &mut catalog, &tenancy).unwrap();
        assert_eq!(report, MigrateReport { files_read: 2, files_written: 3, rows: 51, rows_moved_hour: 20, catalog_files: 3 });
        assert!(!partition::is_old_layout(&store) && !store.join("hour=1970-01-21T20").exists());
        assert!(store.join("_meta/templates.pq").exists());

        let files = catalog.list_files().unwrap();
        assert_eq!(files.iter().map(|f| (f.key.epoch_hour, f.key.bucket.as_str(), f.stats.rows)).collect::<Vec<_>>(), [(100, "info", 20), (500, "info", 30), (500, "warn_plus", 1)]);
        assert!(files.iter().all(|f| f.path.starts_with(store.join("tenant=default/index=default")) && f.key.node == "node-a"));
        let all = crate::scan::scan(&store, &crate::scan::Query { limit: 1000, ..Default::default() }).unwrap();
        assert_eq!(all.records.len(), 51);

        assert_eq!(migrate(&store, &mut catalog, &tenancy).unwrap(), MigrateReport { catalog_files: 3, ..Default::default() });
    }

    #[test]
    fn a_run_cut_short_before_the_input_was_removed_does_not_double_the_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let old = store.join("hour=1970-01-21T20/level=info/a.parquet");
        store::write_partition_file(&old, &(0..10).map(|i| row(500, 500, i, Severity::INFO)).collect::<Vec<_>>()).unwrap();
        let saved = std::fs::read(&old).unwrap();
        let tenancy = Tenancy::default();
        migrate(&store, &mut Catalog::open_in_memory().unwrap(), &tenancy).unwrap();
        // The crash: outputs are there, and so is the input again.
        std::fs::create_dir_all(old.parent().unwrap()).unwrap();
        std::fs::write(&old, saved).unwrap();
        let mut catalog = Catalog::open_in_memory().unwrap();
        let report = migrate(&store, &mut catalog, &tenancy).unwrap();
        assert_eq!((report.files_written, report.catalog_files, catalog.totals().unwrap().0), (1, 1, 10));
    }
}
