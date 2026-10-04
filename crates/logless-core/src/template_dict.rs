//! The template dictionary, exported next to the store as Parquet.
//!
//! Rows in the store carry only `template_id`; the template *text* lives in the
//! catalog's SQLite, which nothing reading the store over S3 can reach. This
//! writes `<store>/_meta/templates.pq` so a reader can join ids to text and
//! title an issue by its shape rather than by one example of it.
//!
//! The extension is `.pq`, not `.parquet`, on purpose: readers glob the logs as
//! `**/*.parquet` with `union_by_name`, and a dictionary matching that glob
//! would be swept into every query as rows of a different schema.
//!
//! Contract (version 1, `logless.templates_version` in the footer):
//!
//! | column        | type                      | null | meaning |
//! |---------------|---------------------------|------|---------|
//! | `template_id` | UInt64                    | no   | equals the logs' `template_id`; a counter local to `agent` |
//! | `service`     | Utf8                      | yes  | equals the logs' `service`; null for rows without one |
//! | `text`        | Utf8                      | no   | current template text: masked tokens (`<NUM>`, `<IP>`, ...) and `<*>` for mined wildcards |
//! | `first_seen`  | Timestamp(µs, UTC)        | no   | earliest `observed` of a row with this (template, service) |
//! | `last_seen`   | Timestamp(µs, UTC)        | no   | latest `observed` of a row with this (template, service) |
//! | `agent`       | Utf8                      | no   | the id space `template_id` was minted in (a UUID per catalog) |
//! | `global_id`   | UInt64                    | no   | FNV-1a 64 of `text`: equal across agents and services for equal text |
//!
//! One row per `(template_id, service)` pair ever merged into the store, so
//! `(service, template_id)` — the console's issue identity — joins to exactly
//! one row. `(agent, template_id)` is unambiguous across agents; `global_id` is
//! the key for merging one shape across services or agents. It is the same
//! value the Sentry forwarder fingerprints with, so it changes when a template
//! widens, exactly as a Sentry issue does. `first_seen`/`last_seen` describe
//! rows as merged, not as retained: retention does not move them.
//!
//! Rewritten whole after every merge that committed a segment: written to a
//! hidden temp file, fsynced and renamed over the old one, so a reader sees the
//! previous dictionary or the next, never a torn one.

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

/// Where the dictionary lives, relative to the store root (and so the S3 key).
pub const DICT_KEY: &str = "_meta/templates.pq";
pub const META_DIR: &str = "_meta";
pub const DICT_FILE: &str = "templates.pq";
/// Hidden, so neither the S3 listing nor a key lookup can ever return it.
const TMP_FILE: &str = ".templates.pq.tmp";
pub const TEMPLATES_VERSION: &str = "1";

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

/// One row of the exported file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictRow {
    pub template_id: u64,
    pub service: Option<String>,
    pub text: String,
    pub first_seen_micros: i64,
    pub last_seen_micros: i64,
    pub agent: String,
    pub global_id: u64,
}

pub fn dict_path(store_dir: &Path) -> PathBuf {
    store_dir.join(META_DIR).join(DICT_FILE)
}

pub fn schema() -> SchemaRef {
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("template_id", DataType::UInt64, false),
        Field::new("service", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, false),
        Field::new("first_seen", ts.clone(), false),
        Field::new("last_seen", ts, false),
        Field::new("agent", DataType::Utf8, false),
        Field::new("global_id", DataType::UInt64, false),
    ]))
}

/// The dictionary as it should be written now, from the catalog.
pub fn rows_from_catalog(catalog: &Catalog) -> Result<Vec<DictRow>, DictError> {
    let agent = catalog.template_id_space()?;
    Ok(catalog
        .template_dictionary()?
        .into_iter()
        .map(|e| DictRow {
            global_id: crate::drain::fingerprint_of(&e.text),
            template_id: e.template_id,
            service: e.service,
            text: e.text,
            // Floor division so a pre-epoch value cannot round the wrong way.
            first_seen_micros: e.first_observed_nanos.div_euclid(1_000),
            last_seen_micros: e.last_observed_nanos.div_euclid(1_000),
            agent: agent.clone(),
        })
        .collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportReport {
    pub rows: usize,
    pub bytes: u64,
}

/// Rewrite `<store>/_meta/templates.pq` from the catalog, atomically.
pub fn export(store_dir: &Path, catalog: &Catalog) -> Result<ExportReport, DictError> {
    let rows = rows_from_catalog(catalog)?;
    let bytes = write(store_dir, &rows)?;
    Ok(ExportReport { rows: rows.len(), bytes })
}

/// Write `rows` as the dictionary. Temp file, fsync, rename, fsync the
/// directory: a crash leaves the old file or the new one.
pub fn write(store_dir: &Path, rows: &[DictRow]) -> Result<u64, DictError> {
    let io = |p: &Path| {
        let p = p.to_path_buf();
        move |source| DictError::Io { path: p.clone(), source }
    };
    let dir = store_dir.join(META_DIR);
    fs::create_dir_all(&dir).map_err(io(&dir))?;
    let tmp = dir.join(TMP_FILE);
    let path = dir.join(DICT_FILE);

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("zstd level 3 is valid"),
        ))
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "logless.templates_version".to_string(),
            TEMPLATES_VERSION.to_string(),
        )]))
        .build();
    let batch = to_record_batch(rows)?;
    {
        let file = File::create(&tmp).map_err(io(&tmp))?;
        let mut writer = ArrowWriter::try_new(file, schema(), Some(props))?;
        if batch.num_rows() > 0 {
            writer.write(&batch)?;
        }
        let file = writer.into_inner()?;
        file.sync_all().map_err(io(&tmp))?;
    }
    fs::rename(&tmp, &path).map_err(io(&path))?;
    if let Ok(d) = File::open(&dir) {
        let _ = d.sync_all();
    }
    Ok(fs::metadata(&path).map_err(io(&path))?.len())
}

fn to_record_batch(rows: &[DictRow]) -> Result<RecordBatch, DictError> {
    let n = rows.len();
    let mut template_id = UInt64Builder::with_capacity(n);
    let mut service = StringBuilder::new();
    let mut text = StringBuilder::new();
    let mut first = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
    let mut last = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
    let mut agent = StringBuilder::new();
    let mut global_id = UInt64Builder::with_capacity(n);
    for r in rows {
        template_id.append_value(r.template_id);
        service.append_option(r.service.as_deref());
        text.append_value(&r.text);
        first.append_value(r.first_seen_micros);
        last.append_value(r.last_seen_micros);
        agent.append_value(&r.agent);
        global_id.append_value(r.global_id);
    }
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(template_id.finish()),
            Arc::new(service.finish()),
            Arc::new(text.finish()),
            Arc::new(first.finish()),
            Arc::new(last.finish()),
            Arc::new(agent.finish()),
            Arc::new(global_id.finish()),
        ],
    )?)
}

/// Read a dictionary file back. Used by `logless templates --dictionary` and
/// by tests; readers of the store are expected to use their own engine.
pub fn read(path: &Path) -> Result<Vec<DictRow>, DictError> {
    let file = File::open(path).map_err(|source| DictError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let col = |name: &str| batch.column_by_name(name).expect("column in v1 schema");
        let template_id = col("template_id").as_any().downcast_ref::<UInt64Array>().expect("u64");
        let service = col("service").as_any().downcast_ref::<StringArray>().expect("utf8");
        let text = col("text").as_any().downcast_ref::<StringArray>().expect("utf8");
        let first = col("first_seen")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp µs");
        let last = col("last_seen")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp µs");
        let agent = col("agent").as_any().downcast_ref::<StringArray>().expect("utf8");
        let global_id = col("global_id").as_any().downcast_ref::<UInt64Array>().expect("u64");
        for i in 0..batch.num_rows() {
            out.push(DictRow {
                template_id: template_id.value(i),
                service: (!service.is_null(i)).then(|| service.value(i).to_string()),
                text: text.value(i).to_string(),
                first_seen_micros: first.value(i),
                last_seen_micros: last.value(i),
                agent: agent.value(i).to_string(),
                global_id: global_id.value(i),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TemplateSighting;

    fn sighting(id: u64, service: Option<&str>, first: i64, last: i64) -> TemplateSighting {
        TemplateSighting {
            template_id: id,
            service: service.map(str::to_string),
            first_observed_nanos: first,
            last_observed_nanos: last,
        }
    }

    fn catalog_with_two_templates() -> Catalog {
        let mut cat = Catalog::open_in_memory().unwrap();
        let mut drain = crate::drain::Drain::default();
        drain.add_line("user 42 logged in from 10.0.0.1", 100).unwrap();
        drain.add_line("payment 99 failed for order 7", 100).unwrap();
        let templates: Vec<_> = drain.templates().cloned().collect();
        cat.save_templates(&templates).unwrap();
        cat
    }

    #[test]
    fn one_row_per_template_and_service_with_merged_bounds() {
        let mut cat = catalog_with_two_templates();
        let user = cat.load_templates().unwrap()[0].id;
        cat.commit_merge(
            1,
            uuid::Uuid::from_u128(1),
            &[],
            &[sighting(user, Some("api"), 5_000_000, 9_000_000), sighting(user, None, 7_000, 8_000)],
            0,
        )
        .unwrap();
        // A later segment widens the api bounds on both sides and adds worker.
        cat.commit_merge(
            2,
            uuid::Uuid::from_u128(2),
            &[],
            &[
                sighting(user, Some("api"), 1_000_000, 20_000_000),
                sighting(user, Some("worker"), 3_000, 4_000),
            ],
            0,
        )
        .unwrap();

        let rows = rows_from_catalog(&cat).unwrap();
        let agent = cat.template_id_space().unwrap();
        let text = "user <NUM> logged in from <IP>";
        let expect = |service: Option<&str>, first, last| DictRow {
            template_id: user,
            service: service.map(str::to_string),
            text: text.into(),
            first_seen_micros: first,
            last_seen_micros: last,
            agent: agent.clone(),
            global_id: crate::drain::fingerprint_of(text),
        };
        assert_eq!(
            rows,
            vec![expect(None, 7, 8), expect(Some("api"), 1_000, 20_000), expect(Some("worker"), 3, 4)],
            "the payment template has no merged rows, so no dictionary row"
        );
    }

    #[test]
    fn null_and_empty_service_stay_distinct() {
        let mut cat = catalog_with_two_templates();
        let id = cat.load_templates().unwrap()[0].id;
        cat.commit_merge(1, uuid::Uuid::from_u128(1), &[], &[sighting(id, None, 0, 1_000)], 0).unwrap();
        cat.commit_merge(2, uuid::Uuid::from_u128(2), &[], &[sighting(id, Some(""), 0, 2_000)], 0).unwrap();
        let services: Vec<_> = rows_from_catalog(&cat).unwrap().into_iter().map(|r| r.service).collect();
        assert_eq!(services, vec![None, Some(String::new())]);
    }

    #[test]
    fn writes_atomically_and_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let mut cat = catalog_with_two_templates();
        let ids: Vec<u64> = cat.load_templates().unwrap().iter().map(|t| t.id).collect();
        cat.commit_merge(
            1,
            uuid::Uuid::from_u128(1),
            &[],
            &[sighting(ids[0], Some("api"), 1_000, 2_000), sighting(ids[1], Some("billing"), 3_000, 4_000)],
            0,
        )
        .unwrap();

        let report = export(&store, &cat).unwrap();
        assert_eq!(report.rows, 2);
        let path = dict_path(&store);
        assert_eq!(path, store.join(DICT_KEY));
        assert_eq!(report.bytes, fs::metadata(&path).unwrap().len());
        assert!(!store.join(META_DIR).join(TMP_FILE).exists(), "temp file renamed away");
        assert_eq!(read(&path).unwrap(), rows_from_catalog(&cat).unwrap());

        // Never swept into the logs glob or the store's own file walk.
        assert!(crate::store::list_parquet_files(&store).is_empty());

        // The footer carries the contract version and the declared types.
        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let kv = builder.metadata().file_metadata().key_value_metadata().unwrap();
        assert!(kv
            .iter()
            .any(|k| k.key == "logless.templates_version" && k.value.as_deref() == Some("1")));
        assert_eq!(builder.schema().fields(), schema().fields());
    }

    #[test]
    fn an_empty_dictionary_is_still_a_readable_file() {
        let tmp = tempfile::tempdir().unwrap();
        let cat = Catalog::open_in_memory().unwrap();
        let report = export(tmp.path(), &cat).unwrap();
        assert_eq!(report.rows, 0);
        assert!(read(&dict_path(tmp.path())).unwrap().is_empty());
    }

    #[test]
    fn two_catalogs_mint_the_same_id_in_different_spaces() {
        // The ambiguity the agent column exists for: both agents call their
        // first template 1, for different shapes; global_id tells shapes apart
        // and agrees where the shape is the same.
        let a = catalog_with_two_templates();
        let b = Catalog::open_in_memory().unwrap();
        assert_ne!(a.template_id_space().unwrap(), b.template_id_space().unwrap());

        let mut a = a;
        let a_first = a.load_templates().unwrap()[0].clone();
        a.commit_merge(1, uuid::Uuid::from_u128(1), &[], &[sighting(a_first.id, Some("api"), 0, 1_000)], 0)
            .unwrap();

        let mut b = b;
        let mut drain = crate::drain::Drain::default();
        drain.add_line("cache miss for key 9", 0).unwrap();
        drain.add_line("user 1 logged in from 10.1.1.1", 0).unwrap();
        b.save_templates(&drain.templates().cloned().collect::<Vec<_>>()).unwrap();
        let b_templates = b.load_templates().unwrap();
        for t in &b_templates {
            b.commit_merge(t.id, uuid::Uuid::from_u128(10 + t.id as u128), &[], &[sighting(t.id, Some("api"), 0, 1_000)], 0)
                .unwrap();
        }

        let ra = rows_from_catalog(&a).unwrap();
        let rb = rows_from_catalog(&b).unwrap();
        let b_same_id = rb.iter().find(|r| r.template_id == ra[0].template_id).unwrap();
        assert_ne!(b_same_id.text, ra[0].text, "same id, different shape");
        assert_ne!(b_same_id.global_id, ra[0].global_id);
        assert_ne!(b_same_id.agent, ra[0].agent);
        let b_same_shape = rb.iter().find(|r| r.text == ra[0].text).unwrap();
        assert_ne!(b_same_shape.template_id, ra[0].template_id, "same shape, different id");
        assert_eq!(b_same_shape.global_id, ra[0].global_id);
    }

    #[test]
    fn the_id_space_survives_reopening_the_catalog() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("catalog.sqlite");
        let first = Catalog::open(&path).unwrap().template_id_space().unwrap();
        let again = Catalog::open(&path).unwrap().template_id_space().unwrap();
        assert_eq!(first, again);
    }
}
