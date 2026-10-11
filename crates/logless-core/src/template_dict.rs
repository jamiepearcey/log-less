//! The template dictionary and shape history, exported into the store as Parquet.
//!
//! Rows in the store carry only `template_id`; the template *text* lives in the
//! catalog's SQLite, which nothing reading the store over S3 can reach. This
//! writes, per `(tenant, index)` and per writer,
//!
//! * `<store>/tenant=<t>/index=<i>/_meta/templates/writer=<node>.pq`, the dictionary, and
//! * `<store>/tenant=<t>/index=<i>/_meta/shape_history/writer=<node>.pq`, the shape history,
//!
//! so a reader can join ids to text, title an issue by its shape rather than by one example of
//! it, and fold a widened shape back into one identity.
//!
//! The extension is `.pq`, not `.parquet`, on purpose: readers glob the logs as
//! `**/*.parquet` with `union_by_name`, and a dictionary matching that glob
//! would be swept into every query as rows of a different schema. Each writer
//! owns its own file, so no two writers ever write the same key; a reader unions
//! `_meta/templates/*.pq`, which is meaningful because ids are content hashes
//! (below) and so agree across writers.
//!
//! **Why two files.** The dictionary is one row per `(template_id, service)`, the grain a
//! reader joins logs on. The shape history is one row per `(lineage_id, template_id)`,
//! a different grain, and a reader that unions `_meta/templates/*.pq` by name would get
//! its rows mixed into the dictionary's if they shared a directory.
//!
//! `template_id` **is the content hash** of the masked template text
//! ([`crate::drain::fingerprint_of`]): equal shapes get equal ids on every node, tenant and writer,
//! with no dictionary service. Widening a template changes the hash from that moment on that node;
//! rows already written keep the hash they were written with. `lineage_id` is fixed when the template
//! is minted and does not change, and the shape history says which hashes a lineage has held.
//!
//! Dictionary contract (version 2, `logless.templates_version` in the footer):
//!
//! | column        | type                      | null | meaning |
//! |---------------|---------------------------|------|---------|
//! | `template_id` | UInt64                    | no   | equals the logs' `template_id`: the FNV-1a 64 of `text` |
//! | `service`     | Utf8                      | yes  | equals the logs' `service`; null for rows without one |
//! | `text`        | Utf8                      | no   | the template text the id hashes: masked tokens (`<NUM>`, `<IP>`, ...) and `<*>` for mined wildcards |
//! | `count`       | UInt64                    | no   | rows merged with this (template, service) |
//! | `first_seen`  | Timestamp(µs, UTC)        | no   | earliest `observed` of a row with this (template, service) |
//! | `last_seen`   | Timestamp(µs, UTC)        | no   | latest `observed` of a row with this (template, service) |
//! | `lineage_id`  | UInt64                    | no   | the id the template was minted with; unchanged when it widens. What the Sentry forwarder fingerprints with |
//!
//! Shape history contract (version 1, `logless.shape_history_version`):
//!
//! | column        | type                      | null | meaning |
//! |---------------|---------------------------|------|---------|
//! | `lineage_id`  | UInt64                    | no   | as above |
//! | `template_id` | UInt64                    | no   | a hash this lineage has held; rows carry it |
//! | `text`        | Utf8                      | no   | the text that hash is of |
//! | `count`       | UInt64                    | no   | rows merged carrying it |
//! | `first_ts`    | Timestamp(µs, UTC)        | no   | earliest `observed` of a row carrying it |
//! | `last_ts`     | Timestamp(µs, UTC)        | no   | latest `observed` of a row carrying it |
//!
//! One row per `(template_id, service)` pair ever merged into the store, so `(service, template_id)`
//! (the console's issue identity) joins to exactly one row. `first_seen`/`last_seen` describe rows as
//! merged, not as retained: retention does not move them. A hash this writer never learned the text of
//! (an edge minted it and this writer kept the record unmined) has no row until the text is known.
//!
//! **Hash collisions.** A 64-bit hash of about 10^6 fleet-wide templates collides with probability near
//! 10^-8. If two different texts do hash alike, they are one shape to every query (`GROUP BY template_id`
//! merges them), the dictionary keeps the first text it registered, and the catalog counts the event
//! (`template_hash_collisions`, logged at merge). Nothing is lost; the second shape is mislabelled.
//!
//! Rewritten whole after every merge that committed a segment: written to a
//! hidden temp file, fsynced and renamed over the old one, so a reader sees the
//! previous dictionary or the next, never a torn one.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::metadata::KeyValue;

use crate::catalog::{Catalog, CatalogError};

pub const META_DIR: &str = "_meta";
/// Directory of the dictionary files under `tenant=<t>/index=<i>/_meta/`.
pub const DICT_DIR: &str = "templates";
/// Directory of the shape history files under `tenant=<t>/index=<i>/_meta/`.
pub const HISTORY_DIR: &str = "shape_history";
pub const TEMPLATES_VERSION: &str = "2";
pub const SHAPE_HISTORY_VERSION: &str = "1";

#[derive(Debug, thiserror::Error)]
pub enum DictError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error("template dictionary I/O on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("template dictionary parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("template dictionary arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// One row of an exported dictionary file. `tenant` and `index` choose the file and are not columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictRow {
    pub tenant: String,
    pub index: String,
    pub template_id: u64,
    pub service: Option<String>,
    pub text: String,
    pub count: u64,
    pub first_seen_micros: i64,
    pub last_seen_micros: i64,
    pub lineage_id: u64,
}

/// One row of an exported shape history file. `tenant` and `index` choose the file and are not columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRow {
    pub tenant: String,
    pub index: String,
    pub lineage_id: u64,
    pub template_id: u64,
    pub text: String,
    pub count: u64,
    pub first_ts_micros: i64,
    pub last_ts_micros: i64,
}

/// `<store>/tenant=<t>/index=<i>/_meta`, where this `(tenant, index)`'s dictionary and history live.
pub fn meta_dir(store_dir: &Path, tenant: &str, index: &str) -> PathBuf {
    store_dir.join(format!("tenant={tenant}")).join(format!("index={index}")).join(META_DIR)
}

/// Where `node`'s dictionary for `(tenant, index)` lives, relative to the store root (and so the S3 key).
pub fn dict_key(tenant: &str, index: &str, node: &str) -> String {
    format!("tenant={tenant}/index={index}/{META_DIR}/{DICT_DIR}/writer={node}.pq")
}

pub fn dict_path(store_dir: &Path, tenant: &str, index: &str, node: &str) -> PathBuf {
    meta_dir(store_dir, tenant, index).join(DICT_DIR).join(format!("writer={node}.pq"))
}

pub fn history_path(store_dir: &Path, tenant: &str, index: &str, node: &str) -> PathBuf {
    meta_dir(store_dir, tenant, index).join(HISTORY_DIR).join(format!("writer={node}.pq"))
}

fn micros_ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("template_id", DataType::UInt64, false),
        Field::new("service", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("first_seen", micros_ts(), false),
        Field::new("last_seen", micros_ts(), false),
        Field::new("lineage_id", DataType::UInt64, false),
    ]))
}

pub fn history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("lineage_id", DataType::UInt64, false),
        Field::new("template_id", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("first_ts", micros_ts(), false),
        Field::new("last_ts", micros_ts(), false),
    ]))
}

/// The dictionary as it should be written now, from the catalog.
pub fn rows_from_catalog(catalog: &Catalog) -> Result<Vec<DictRow>, DictError> {
    Ok(catalog
        .template_dictionary()?
        .into_iter()
        .map(|e| DictRow {
            tenant: e.tenant,
            index: e.index,
            lineage_id: e.lineage_id,
            template_id: e.template_id,
            service: e.service,
            text: e.text,
            count: e.rows,
            // Floor division so a pre-epoch value cannot round the wrong way.
            first_seen_micros: e.first_observed_nanos.div_euclid(1_000),
            last_seen_micros: e.last_observed_nanos.div_euclid(1_000),
        })
        .collect())
}

/// The shape history as it should be written now, from the catalog.
pub fn history_from_catalog(catalog: &Catalog) -> Result<Vec<HistoryRow>, DictError> {
    Ok(catalog
        .shape_history()?
        .into_iter()
        .map(|e| HistoryRow {
            tenant: e.tenant,
            index: e.index,
            lineage_id: e.lineage_id,
            template_id: e.template_id,
            text: e.text,
            count: e.rows,
            first_ts_micros: e.first_observed_nanos.div_euclid(1_000),
            last_ts_micros: e.last_observed_nanos.div_euclid(1_000),
        })
        .collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportReport {
    /// Dictionary rows written, over every `(tenant, index)`.
    pub rows: usize,
    /// Shape history rows written.
    pub history_rows: usize,
    /// Dictionary files written.
    pub files: usize,
    pub bytes: u64,
}

/// Rewrites every `(tenant, index)`'s dictionary and shape history for writer `node` from the catalog, atomically
/// per file.
pub fn export(store_dir: &Path, node: &str, catalog: &Catalog) -> Result<ExportReport, DictError> {
    write_all(store_dir, node, &rows_from_catalog(catalog)?, &history_from_catalog(catalog)?)
}

/// Writes `rows` and `history`, one dictionary file and one history file for each `(tenant, index)` that has
/// rows. Returns what was written.
pub fn write_all(store_dir: &Path, node: &str, rows: &[DictRow], history: &[HistoryRow]) -> Result<ExportReport, DictError> {
    let mut by_scope: BTreeMap<(&str, &str), (Vec<&DictRow>, Vec<&HistoryRow>)> = BTreeMap::new();
    for r in rows {
        by_scope.entry((&r.tenant, &r.index)).or_default().0.push(r);
    }
    for h in history {
        by_scope.entry((&h.tenant, &h.index)).or_default().1.push(h);
    }
    let mut report = ExportReport { rows: 0, history_rows: 0, files: 0, bytes: 0 };
    for ((tenant, index), (rows, history)) in by_scope {
        report.bytes += write_file(
            &dict_path(store_dir, tenant, index, node),
            &to_record_batch(&rows)?,
            schema(),
            "logless.templates_version",
            TEMPLATES_VERSION,
        )?;
        report.bytes += write_file(
            &history_path(store_dir, tenant, index, node),
            &history_batch(&history)?,
            history_schema(),
            "logless.shape_history_version",
            SHAPE_HISTORY_VERSION,
        )?;
        report.rows += rows.len();
        report.history_rows += history.len();
        report.files += 1;
    }
    Ok(report)
}

/// Whether some `(tenant, index)` the catalog has a dictionary for lacks its file: a wiped `_meta`.
pub fn missing(store_dir: &Path, node: &str, catalog: &Catalog) -> Result<bool, DictError> {
    Ok(catalog.dictionary_scopes()?.iter().any(|(t, i)| !dict_path(store_dir, t, i, node).exists()))
}

/// Temp file, fsync, rename, fsync the directory: a crash leaves the old file or the new one.
fn write_file(path: &Path, batch: &RecordBatch, schema: SchemaRef, version_key: &str, version: &str) -> Result<u64, DictError> {
    let io = |p: &Path| {
        let p = p.to_path_buf();
        move |source| DictError::Io { path: p.clone(), source }
    };
    let dir = path.parent().expect("a dictionary path has a directory");
    fs::create_dir_all(dir).map_err(io(dir))?;
    // Hidden, so neither the S3 listing nor a key lookup can ever return it.
    let tmp = dir.join(format!(".{}.tmp", path.file_name().and_then(|n| n.to_str()).unwrap_or("writer.pq")));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("zstd level 3 is valid"),
        ))
        .set_key_value_metadata(Some(vec![KeyValue::new(version_key.to_string(), version.to_string())]))
        .build();
    {
        let file = File::create(&tmp).map_err(io(&tmp))?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        if batch.num_rows() > 0 {
            writer.write(batch)?;
        }
        let file = writer.into_inner()?;
        file.sync_all().map_err(io(&tmp))?;
    }
    fs::rename(&tmp, path).map_err(io(path))?;
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(fs::metadata(path).map_err(io(path))?.len())
}

fn to_record_batch(rows: &[&DictRow]) -> Result<RecordBatch, DictError> {
    let n = rows.len();
    let mut template_id = UInt64Builder::with_capacity(n);
    let mut service = StringBuilder::new();
    let mut text = StringBuilder::new();
    let mut count = UInt64Builder::with_capacity(n);
    let mut first = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
    let mut last = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
    let mut lineage_id = UInt64Builder::with_capacity(n);
    for r in rows {
        template_id.append_value(r.template_id);
        service.append_option(r.service.as_deref());
        text.append_value(&r.text);
        count.append_value(r.count);
        first.append_value(r.first_seen_micros);
        last.append_value(r.last_seen_micros);
        lineage_id.append_value(r.lineage_id);
    }
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(template_id.finish()),
            Arc::new(service.finish()),
            Arc::new(text.finish()),
            Arc::new(count.finish()),
            Arc::new(first.finish()),
            Arc::new(last.finish()),
            Arc::new(lineage_id.finish()),
        ],
    )?)
}

fn history_batch(rows: &[&HistoryRow]) -> Result<RecordBatch, DictError> {
    let n = rows.len();
    let mut lineage_id = UInt64Builder::with_capacity(n);
    let mut template_id = UInt64Builder::with_capacity(n);
    let mut text = StringBuilder::new();
    let mut count = UInt64Builder::with_capacity(n);
    let mut first = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
    let mut last = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
    for r in rows {
        lineage_id.append_value(r.lineage_id);
        template_id.append_value(r.template_id);
        text.append_value(&r.text);
        count.append_value(r.count);
        first.append_value(r.first_ts_micros);
        last.append_value(r.last_ts_micros);
    }
    Ok(RecordBatch::try_new(
        history_schema(),
        vec![
            Arc::new(lineage_id.finish()),
            Arc::new(template_id.finish()),
            Arc::new(text.finish()),
            Arc::new(count.finish()),
            Arc::new(first.finish()),
            Arc::new(last.finish()),
        ],
    )?)
}

fn open(path: &Path) -> Result<impl Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>, DictError> {
    let file = File::open(path).map_err(|source| DictError::Io { path: path.to_path_buf(), source })?;
    Ok(ParquetRecordBatchReaderBuilder::try_new(file)?.build()?)
}

/// Read a dictionary file back. `tenant` and `index` are not columns; the caller says whose file it is.
/// Used by `logless templates` and by tests; readers of the store are expected to use their own engine.
pub fn read(path: &Path, tenant: &str, index: &str) -> Result<Vec<DictRow>, DictError> {
    let mut out = Vec::new();
    for batch in open(path)? {
        let batch = batch?;
        let col = |name: &str| batch.column_by_name(name).expect("column in v2 schema");
        let u64s = |name: &str| col(name).as_any().downcast_ref::<UInt64Array>().expect("u64").clone();
        let template_id = u64s("template_id");
        let count = u64s("count");
        let lineage_id = u64s("lineage_id");
        let service = col("service").as_any().downcast_ref::<StringArray>().expect("utf8").clone();
        let text = col("text").as_any().downcast_ref::<StringArray>().expect("utf8").clone();
        let first = col("first_seen").as_any().downcast_ref::<TimestampMicrosecondArray>().expect("timestamp µs").clone();
        let last = col("last_seen").as_any().downcast_ref::<TimestampMicrosecondArray>().expect("timestamp µs").clone();
        for i in 0..batch.num_rows() {
            out.push(DictRow {
                tenant: tenant.to_string(),
                index: index.to_string(),
                template_id: template_id.value(i),
                service: (!service.is_null(i)).then(|| service.value(i).to_string()),
                text: text.value(i).to_string(),
                count: count.value(i),
                first_seen_micros: first.value(i),
                last_seen_micros: last.value(i),
                lineage_id: lineage_id.value(i),
            });
        }
    }
    Ok(out)
}

/// Read a shape history file back (see [`read`]).
pub fn read_history(path: &Path, tenant: &str, index: &str) -> Result<Vec<HistoryRow>, DictError> {
    let mut out = Vec::new();
    for batch in open(path)? {
        let batch = batch?;
        let col = |name: &str| batch.column_by_name(name).expect("column in the history schema");
        let u64s = |name: &str| col(name).as_any().downcast_ref::<UInt64Array>().expect("u64").clone();
        let (lineage_id, template_id, count) = (u64s("lineage_id"), u64s("template_id"), u64s("count"));
        let text = col("text").as_any().downcast_ref::<StringArray>().expect("utf8").clone();
        let first = col("first_ts").as_any().downcast_ref::<TimestampMicrosecondArray>().expect("timestamp µs").clone();
        let last = col("last_ts").as_any().downcast_ref::<TimestampMicrosecondArray>().expect("timestamp µs").clone();
        for i in 0..batch.num_rows() {
            out.push(HistoryRow {
                tenant: tenant.to_string(),
                index: index.to_string(),
                lineage_id: lineage_id.value(i),
                template_id: template_id.value(i),
                text: text.value(i).to_string(),
                count: count.value(i),
                first_ts_micros: first.value(i),
                last_ts_micros: last.value(i),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ShapeRow, TemplateSighting};
    use crate::drain::{fingerprint_of, Drain};

    fn sighting(id: u64, service: Option<&str>, rows: u64, first: i64, last: i64) -> TemplateSighting {
        TemplateSighting {
            tenant: "default".into(),
            index: "default".into(),
            template_id: id,
            service: service.map(str::to_string),
            rows,
            first_observed_nanos: first,
            last_observed_nanos: last,
        }
    }

    /// A catalog that knows two shapes; returns their content-hash ids.
    fn catalog_with_two_shapes() -> (Catalog, u64, u64) {
        let mut cat = Catalog::open_in_memory().unwrap();
        let mut drain = Drain::default();
        let user = drain.add_line("user 42 logged in from 10.0.0.1", 100).unwrap().shape_id;
        let pay = drain.add_line("payment 99 failed for order 7", 100).unwrap().shape_id;
        let shapes: Vec<_> = [user, pay]
            .iter()
            .map(|id| {
                let s = drain.shape(*id).unwrap();
                ShapeRow { template_id: *id, lineage_id: s.lineage_id, text: s.text.clone() }
            })
            .collect();
        cat.register_shapes(&shapes).unwrap();
        cat.save_templates(&drain.templates().cloned().collect::<Vec<_>>()).unwrap();
        (cat, user, pay)
    }

    #[test]
    fn one_row_per_template_and_service_with_merged_bounds_and_summed_counts() {
        let (mut cat, user, _) = catalog_with_two_shapes();
        cat.commit_merge(
            1,
            uuid::Uuid::from_u128(1),
            &[],
            &[sighting(user, Some("api"), 3, 5_000_000, 9_000_000), sighting(user, None, 1, 7_000, 8_000)],
            0,
        )
        .unwrap();
        // A later segment widens the api bounds on both sides and adds worker.
        cat.commit_merge(
            2,
            uuid::Uuid::from_u128(2),
            &[],
            &[sighting(user, Some("api"), 4, 1_000_000, 20_000_000), sighting(user, Some("worker"), 2, 3_000, 4_000)],
            0,
        )
        .unwrap();

        let rows = rows_from_catalog(&cat).unwrap();
        let text = "user <NUM> logged in from <IP>";
        assert_eq!(user, fingerprint_of(text), "the id is the hash of the text");
        let expect = |service: Option<&str>, count, first, last| DictRow {
            tenant: "default".into(),
            index: "default".into(),
            template_id: user,
            service: service.map(str::to_string),
            text: text.into(),
            count,
            first_seen_micros: first,
            last_seen_micros: last,
            lineage_id: fingerprint_of(text),
        };
        assert_eq!(
            rows,
            vec![expect(None, 1, 7, 8), expect(Some("api"), 7, 1_000, 20_000), expect(Some("worker"), 2, 3, 4)],
            "the payment template has no merged rows, so no dictionary row"
        );
    }

    #[test]
    fn null_and_empty_service_stay_distinct() {
        let (mut cat, id, _) = catalog_with_two_shapes();
        cat.commit_merge(1, uuid::Uuid::from_u128(1), &[], &[sighting(id, None, 1, 0, 1_000)], 0).unwrap();
        cat.commit_merge(2, uuid::Uuid::from_u128(2), &[], &[sighting(id, Some(""), 1, 0, 2_000)], 0).unwrap();
        let services: Vec<_> = rows_from_catalog(&cat).unwrap().into_iter().map(|r| r.service).collect();
        assert_eq!(services, vec![None, Some(String::new())]);
    }

    #[test]
    fn a_hash_whose_text_this_catalog_never_learned_has_no_dictionary_row_but_is_still_counted() {
        let (mut cat, user, _) = catalog_with_two_shapes();
        let foreign = fingerprint_of("something only an edge mined <*>");
        cat.commit_merge(1, uuid::Uuid::from_u128(1), &[], &[sighting(user, Some("api"), 1, 0, 1), sighting(foreign, Some("api"), 9, 0, 1)], 0).unwrap();
        assert_eq!(rows_from_catalog(&cat).unwrap().len(), 1);
        assert!(cat.load_template_services().unwrap().contains(&(Some("api".into()), foreign)), "the planner still knows the pair");
        assert!(!cat.knows_shape(foreign).unwrap());
    }

    #[test]
    fn two_scopes_get_their_own_files_and_a_wiped_meta_dir_is_noticed() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let (mut cat, user, pay) = catalog_with_two_shapes();
        let mut other = sighting(pay, Some("billing"), 5, 3_000, 4_000);
        other.tenant = "acme".into();
        other.index = "payments".into();
        cat.commit_merge(1, uuid::Uuid::from_u128(1), &[], &[sighting(user, Some("api"), 2, 1_000, 2_000), other], 0).unwrap();

        assert!(missing(&store, "edge-1", &cat).unwrap());
        let report = export(&store, "edge-1", &cat).unwrap();
        assert_eq!((report.rows, report.history_rows, report.files), (2, 2, 2));
        assert!(!missing(&store, "edge-1", &cat).unwrap());
        assert_eq!(report.bytes, [("default", "default"), ("acme", "payments")].iter().map(|(t, i)| {
            fs::metadata(dict_path(&store, t, i, "edge-1")).unwrap().len() + fs::metadata(history_path(&store, t, i, "edge-1")).unwrap().len()
        }).sum::<u64>());

        let path = dict_path(&store, "acme", "payments", "edge-1");
        assert_eq!(path, store.join("tenant=acme/index=payments/_meta/templates/writer=edge-1.pq"));
        assert!(path.ends_with(dict_key("acme", "payments", "edge-1")));
        let rows = read(&path, "acme", "payments").unwrap();
        assert_eq!(rows.iter().map(|r| (r.template_id, r.count)).collect::<Vec<_>>(), [(pay, 5)]);
        assert!(!path.parent().unwrap().join(".writer=edge-1.pq.tmp").exists(), "temp file renamed away");

        // Never swept into the logs glob or the store's own file walk.
        assert!(crate::store::list_parquet_files(&store).is_empty());

        // The footer carries the contract version and the declared types.
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        let kv = builder.metadata().file_metadata().key_value_metadata().unwrap();
        assert!(kv.iter().any(|k| k.key == "logless.templates_version" && k.value.as_deref() == Some("2")));
        assert_eq!(builder.schema().fields(), schema().fields());

        fs::remove_dir_all(store.join("tenant=acme")).unwrap();
        assert!(missing(&store, "edge-1", &cat).unwrap());
    }

    #[test]
    fn an_empty_catalog_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let cat = Catalog::open_in_memory().unwrap();
        let report = export(tmp.path(), "edge-1", &cat).unwrap();
        assert_eq!((report.rows, report.files), (0, 0));
        assert!(!missing(tmp.path(), "edge-1", &cat).unwrap());
    }

    #[test]
    fn two_catalogs_that_mined_the_same_shape_in_different_orders_export_the_same_id() {
        // The ambiguity this replaces: both agents once called their first template 1, for different shapes.
        let export_of = |lines: &[&str]| {
            let mut cat = Catalog::open_in_memory().unwrap();
            let mut drain = Drain::default();
            let mut sightings = Vec::new();
            let mut shapes = Vec::new();
            for line in lines {
                let id = drain.add_line(line, 0).unwrap().shape_id;
                let s = drain.shape(id).unwrap();
                shapes.push(ShapeRow { template_id: id, lineage_id: s.lineage_id, text: s.text.clone() });
                sightings.push(sighting(id, Some("api"), 1, 0, 1_000));
            }
            cat.register_shapes(&shapes).unwrap();
            cat.commit_merge(1, uuid::Uuid::from_u128(1), &[], &sightings, 0).unwrap();
            rows_from_catalog(&cat).unwrap().into_iter().map(|r| (r.text, r.template_id)).collect::<std::collections::BTreeMap<_, _>>()
        };
        let a = export_of(&["user 42 logged in from 10.0.0.1", "payment 99 failed for order 7"]);
        let b = export_of(&["cache miss for key 9", "payment 12 failed for order 1", "user 1 logged in from 10.1.1.1"]);
        for text in ["user <NUM> logged in from <IP>", "payment <NUM> failed for order <NUM>"] {
            assert_eq!(a[text], b[text], "{text}");
        }
        assert_eq!(b.len(), 3);
    }

    #[test]
    fn a_widened_shape_keeps_its_lineage_and_the_history_links_the_hashes() {
        let mut cat = Catalog::open_in_memory().unwrap();
        let mut drain = Drain::new(crate::drain::DrainConfig { similarity_threshold: 0.5, ..Default::default() });
        let before = drain.add_line("job run alpha finished ok in cache", 1).unwrap();
        let lineage = drain.template(before.template_id).unwrap().lineage_id;
        let after = drain.add_line("job run beta finished ok in cache", 2).unwrap();
        assert_eq!(before.template_id, after.template_id);
        assert_ne!(before.shape_id, after.shape_id, "widening changes the hash");
        assert_eq!(drain.template(after.template_id).unwrap().lineage_id, lineage, "and not the lineage");
        let shapes: Vec<_> = [before.shape_id, after.shape_id]
            .iter()
            .map(|id| {
                let s = drain.shape(*id).unwrap();
                ShapeRow { template_id: *id, lineage_id: s.lineage_id, text: s.text.clone() }
            })
            .collect();
        cat.register_shapes(&shapes).unwrap();
        cat.commit_merge(
            1,
            uuid::Uuid::from_u128(1),
            &[],
            &[sighting(before.shape_id, Some("api"), 10, 1_000, 2_000), sighting(after.shape_id, Some("api"), 5, 3_000, 4_000)],
            0,
        )
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        export(tmp.path(), "edge-1", &cat).unwrap();
        let history = read_history(&history_path(tmp.path(), "default", "default", "edge-1"), "default", "default").unwrap();
        assert_eq!(
            history.iter().map(|h| (h.lineage_id, h.template_id, h.text.as_str(), h.count, h.first_ts_micros, h.last_ts_micros)).collect::<Vec<_>>(),
            vec![
                (lineage, before.shape_id, "job run alpha finished ok in cache", 10, 1, 2),
                (lineage, after.shape_id, "job run <*> finished ok in cache", 5, 3, 4),
            ]
        );
        let dict = read(&dict_path(tmp.path(), "default", "default", "edge-1"), "default", "default").unwrap();
        assert!(dict.iter().all(|r| r.lineage_id == lineage), "both hashes fold to one lineage in the dictionary too");
    }

    #[test]
    fn a_hash_collision_keeps_the_first_text_and_is_counted() {
        let mut cat = Catalog::open_in_memory().unwrap();
        let shape = |text: &str| ShapeRow { template_id: 77, lineage_id: 77, text: text.into() };
        assert_eq!(cat.register_shapes(&[shape("first text")]).unwrap(), 0);
        assert_eq!(cat.register_shapes(&[shape("first text")]).unwrap(), 0, "the same text again is not a collision");
        assert_eq!(cat.register_shapes(&[shape("a different text")]).unwrap(), 1);
        assert_eq!(cat.template_hash_collisions().unwrap(), 1);
        cat.commit_merge(1, uuid::Uuid::from_u128(1), &[], &[sighting(77, Some("api"), 4, 0, 1)], 0).unwrap();
        let rows = rows_from_catalog(&cat).unwrap();
        assert_eq!((rows.len(), rows[0].text.as_str(), rows[0].count), (1, "first text", 4), "both texts are one shape to a query");
    }
}
