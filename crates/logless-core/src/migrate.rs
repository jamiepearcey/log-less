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

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::catalog::{Catalog, CatalogError, TemplateSighting};
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
    /// Files rewritten in place, or into the new tree, to replace counter template ids with content hashes.
    pub files_rekeyed: u64,
    /// Rows whose counter template id was replaced by a hash.
    pub rows_rekeyed: u64,
    /// Rows whose counter template id the catalog does not know: they lose their template id.
    pub rows_unmapped: u64,
}

/// Replaces the counter ids in `records` by content hashes, counting what it did.
fn rekey(records: &mut [LogRecord], counters: &HashMap<u64, u64>, report: &mut MigrateReport) {
    for r in records {
        if let Some(counter) = r.template_id {
            r.template_id = counters.get(&counter).copied();
            if r.template_id.is_some() {
                report.rows_rekeyed += 1;
            } else {
                report.rows_unmapped += 1;
            }
        }
    }
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
    let counters = catalog.counter_to_hash()?;
    for (old, old_hour) in old_files(store_dir) {
        let mut by_key: BTreeMap<PartitionKey, Vec<LogRecord>> = BTreeMap::new();
        let mut old_rows = crate::compact::read_all(&old)?;
        // The old layout is from before content-hash ids: every id in it is a counter.
        rekey(&mut old_rows, &counters, &mut report);
        for record in old_rows {
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
            report.files_rekeyed += 1;
        }
        std::fs::remove_file(&old).map_err(|source| StoreError::Io { path: old.clone(), source })?;
        report.files_read += 1;
        // The directories empty out as their files go; a non-empty one stays.
        for dir in old.ancestors().skip(1).take(2) {
            let _ = std::fs::remove_dir(dir);
        }
    }
    // Files of the current layout written before ids were hashes: rewritten where they are (the write is
    // a temp file and a rename, so a reader sees the old file or the new one). Every file is read for its
    // sightings whether or not it needed rewriting, so the table ends up complete.
    let mut sightings: BTreeMap<(String, String, u64, Option<String>), (u64, i64, i64)> = BTreeMap::new();
    for path in store::list_parquet_files(store_dir) {
        let mut records = crate::compact::read_all(&path)?;
        if store::read_schema_version(&path)?.as_deref() != Some(schema::SCHEMA_VERSION) {
            rekey(&mut records, &counters, &mut report);
            store::write_partition_file_with(&path, &records, &layout)?;
            report.files_rekeyed += 1;
        }
        let Some(key) = partition::key_of_path(&path) else { continue };
        for r in &records {
            if let Some(id) = r.template_id {
                let t = r.observed_unix_nano as i64;
                let s = sightings.entry((key.tenant.clone(), key.index.clone(), id, r.service.clone())).or_insert((0, t, t));
                s.0 += 1;
                s.1 = s.1.min(t);
                s.2 = s.2.max(t);
            }
        }
    }
    let sightings: Vec<TemplateSighting> = sightings
        .into_iter()
        .map(|((tenant, index, template_id, service), (rows, lo, hi))| TemplateSighting { tenant, index, template_id, service, rows, first_observed_nanos: lo, last_observed_nanos: hi })
        .collect();
    catalog.replace_sightings(&sightings)?;
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
        assert_eq!(report, MigrateReport { files_read: 2, files_written: 3, rows: 51, rows_moved_hour: 20, catalog_files: 3, files_rekeyed: 3, ..Default::default() });
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

    /// Rewrites `path` with its footer claiming `version`: how a file from before the version existed looks.
    fn stamp_version(path: &Path, version: &str) {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
        let mut kv: Vec<_> = builder.metadata().file_metadata().key_value_metadata().cloned().unwrap_or_default();
        kv.iter_mut().filter(|e| e.key == "logless.schema_version").for_each(|e| e.value = Some(version.into()));
        let schema = builder.schema().clone();
        let batches: Vec<_> = builder.build().unwrap().map(Result::unwrap).collect();
        let props = parquet::file::properties::WriterProperties::builder().set_key_value_metadata(Some(kv)).build();
        let mut w = parquet::arrow::ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, Some(props)).unwrap();
        for b in &batches {
            w.write(b).unwrap();
        }
        w.close().unwrap();
    }

    fn with_counter(mut r: LogRecord, counter: u64) -> LogRecord {
        r.template_id = Some(counter);
        r
    }

    fn catalog_with_templates(texts: &[(u64, &str)]) -> Catalog {
        let mut catalog = Catalog::open_in_memory().unwrap();
        let templates: Vec<_> = texts
            .iter()
            .map(|(id, text)| crate::drain::Template {
                id: *id,
                tokens: text.split(' ').map(str::to_string).collect(),
                count: 1,
                first_seen_unix_secs: 0,
                last_seen_unix_secs: 0,
                lineage_id: crate::drain::fingerprint_of(text),
            })
            .collect();
        catalog.save_templates(&templates).unwrap();
        catalog
    }

    fn template_ids(store: &Path) -> Vec<Option<u64>> {
        let mut ids: Vec<_> = crate::scan::scan(store, &crate::scan::Query { limit: 1000, ..Default::default() }).unwrap().records.iter().map(|r| r.template_id).collect();
        ids.sort();
        ids
    }

    #[test]
    fn counter_template_ids_in_the_old_layout_and_in_version_3_files_become_content_hashes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let (user, pay) = (crate::drain::fingerprint_of("user <NUM> logged in"), crate::drain::fingerprint_of("payment <NUM> failed"));
        // Counters 1 and 2 are templates this catalog knows; 9 is one it lost.
        let mut catalog = catalog_with_templates(&[(1, "user <NUM> logged in"), (2, "payment <NUM> failed")]);

        let old: Vec<_> = vec![with_counter(row(500, 500, 1, Severity::INFO), 1), with_counter(row(500, 500, 2, Severity::INFO), 2), with_counter(row(500, 500, 3, Severity::INFO), 9), row(500, 500, 4, Severity::INFO)];
        store::write_partition_file(&store.join("hour=1970-01-21T20/level=info/s000000000001-abc.parquet"), &old).unwrap();
        // A file already in the current layout, written before ids were hashes.
        let v3 = PartitionKey::new("default", "default", 501, "info", "node-a").dir(&store).join("s000000000002-def.parquet");
        store::write_partition_file(&v3, &[with_counter(row(501, 501, 5, Severity::INFO), 2), with_counter(row(501, 501, 6, Severity::INFO), 2)]).unwrap();
        stamp_version(&v3, "3");
        // And one that is current and must not be touched: ids that look like counters are hashes there.
        let v4 = PartitionKey::new("default", "default", 502, "info", "node-a").dir(&store).join("s000000000003-ghi.parquet");
        store::write_partition_file(&v4, &[with_counter(row(502, 502, 7, Severity::INFO), 1)]).unwrap();
        let v4_before = std::fs::read(&v4).unwrap();

        let tenancy = Tenancy::default();
        let report = migrate(&store, &mut catalog, &tenancy).unwrap();
        assert_eq!((report.rows_rekeyed, report.rows_unmapped), (2 + 2, 1));
        assert_eq!(report.files_rekeyed, 1 + 1, "the old-layout file's one output and the version 3 file");
        assert_eq!(std::fs::read(&v4).unwrap(), v4_before, "a version 4 file is not rewritten");
        let mut want = vec![None, None, Some(1), Some(user), Some(pay), Some(pay), Some(pay)];
        want.sort();
        assert_eq!(template_ids(&store), want);
        assert_eq!(store::read_schema_version(&v3).unwrap().as_deref(), Some("4"));

        // The sightings are refilled under the hash, per tenant and index, with the rows counted.
        let mut seen: Vec<_> = catalog.load_template_services().unwrap().into_iter().map(|(_, id)| id).collect();
        seen.sort();
        let mut want = vec![1, user, pay];
        want.sort();
        assert_eq!(seen, want, "{seen:?}");

        // Repeating it changes nothing.
        let again = migrate(&store, &mut catalog, &tenancy).unwrap();
        assert_eq!((again.files_rekeyed, again.rows_rekeyed, again.rows_unmapped), (0, 0, 0));
        let mut want = vec![None, None, Some(1), Some(user), Some(pay), Some(pay), Some(pay)];
        want.sort();
        assert_eq!(template_ids(&store), want);
    }

    #[test]
    fn a_catalog_from_before_hash_ids_loses_its_counter_keyed_sightings_and_keeps_its_templates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.sqlite");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE templates (id INTEGER PRIMARY KEY, text TEXT NOT NULL, tokens TEXT NOT NULL,
                    count INTEGER NOT NULL, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, lineage_id INTEGER);
                 INSERT INTO templates VALUES (4, 'a <*> c', 'a\t<*>\tc', 9, 1, 2, 77);
                 CREATE TABLE template_services (template_id INTEGER NOT NULL, has_service INTEGER NOT NULL, service TEXT NOT NULL,
                    first_observed INTEGER NOT NULL, last_observed INTEGER NOT NULL, PRIMARY KEY (template_id, has_service, service));
                 INSERT INTO template_services VALUES (4, 1, 'api', 1, 2);",
            )
            .unwrap();
        }
        for _ in 0..2 {
            let cat = Catalog::open(&path).unwrap();
            assert_eq!(cat.load_templates().unwrap().len(), 1, "the miner's state is kept");
            assert!(cat.load_template_services().unwrap().is_empty(), "counter-keyed sightings are dropped, to be refilled by migrate-layout");
            assert_eq!(cat.counter_to_hash().unwrap()[&4], crate::drain::fingerprint_of("a <*> c"));
            assert!(cat.knows_shape(crate::drain::fingerprint_of("a <*> c")).unwrap(), "its current text is a registered shape");
        }
    }
}
