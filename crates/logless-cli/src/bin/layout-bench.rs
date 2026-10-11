//! Writes the sort-layout benchmark corpus through the store's own write path,
//! so the layout can be measured as arrow-rs writes it and not as DuckDB does.
//!
//! The corpus is the Parquet file `scripts/sort-layout-bench.py --export-corpus`
//! writes: one row per record in arrival order (`observed`, `event_id`), with
//! the WAL segment each row falls in (`seg`). Two modes:
//!
//! * `v3` (the store as it is): each segment's rows are appended through
//!   [`wal::SplitWal`] with the clock set to the rows' `observed` time, the
//!   main log is rolled at every segment boundary, and after every roll the
//!   closed segments of both logs are merged ([`merge::merge_all`]), as the
//!   maintenance pass does. At the end everything left is merged and the store
//!   is compacted until nothing more compacts.
//! * `main` (the layout before ADR 0002, re-created, not the old binary): one
//!   file per (segment, observed hour, level) under `hour=/level=`, sorted
//!   `(service, trace_id, observed, event_id)`, 64k-row row groups, then the
//!   same compaction rule (a partition with at least `min_files` files becomes
//!   one). It uses the same arrow-rs writer and encodings; the files carry the
//!   current schema (the `origin` column, NULL here), which the old layout did
//!   not have.
//!
//! Prints one JSON object of exact counts. Wall time is in it and is secondary.
//!
//! Usage: layout-bench --corpus corpus.parquet --out DIR --mode v3|main
//!          [--late-threshold-secs 7200] [--late-roll-secs 1800] [--late-off]
//!          [--min-files 4]
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arrow::array::{Array, BinaryArray, Int64Array, StringArray, UInt64Array, UInt8Array};
use logless_core::catalog::Catalog;
use logless_core::compact::{self, CompactConfig};
use logless_core::config::Tenancy;
use logless_core::drain::Drain;
use logless_core::model::{Attr, AttrValue, LogRecord, Severity};
use logless_core::typed::{TypedColumnsConfig, TypedLayout};
use logless_core::{merge, store, wal};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const HOUR_NS: u64 = 3_600_000_000_000;
/// Row groups of the layout before ADR 0002.
const OLD_ROW_GROUP_ROWS: usize = 64 * 1024;

struct Args {
    corpus: PathBuf,
    out: PathBuf,
    mode: String,
    late_threshold: Duration,
    late_roll: Duration,
    min_files: usize,
}

fn args() -> Args {
    let mut a = Args {
        corpus: PathBuf::new(),
        out: PathBuf::new(),
        mode: "v3".into(),
        late_threshold: Duration::from_secs(7200),
        late_roll: Duration::from_secs(1800),
        min_files: CompactConfig::default().min_files,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--corpus" => a.corpus = value().into(),
            "--out" => a.out = value().into(),
            "--mode" => a.mode = value(),
            "--late-threshold-secs" => a.late_threshold = Duration::from_secs(value().parse().unwrap()),
            "--late-roll-secs" => a.late_roll = Duration::from_secs(value().parse().unwrap()),
            "--min-files" => a.min_files = value().parse().unwrap(),
            // No record is ever older than this, so nothing goes to the late spool.
            "--late-off" => a.late_threshold = Duration::MAX,
            other => panic!("unknown argument {other}"),
        }
    }
    assert!(!a.corpus.as_os_str().is_empty() && !a.out.as_os_str().is_empty(), "--corpus and --out are required");
    a
}

/// Calls `each(segment, rows)` for every WAL segment of the corpus, in order.
fn for_each_segment(corpus: &Path, mut each: impl FnMut(u64, Vec<LogRecord>)) -> u64 {
    let file = std::fs::File::open(corpus).expect("corpus");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().with_batch_size(8192).build().unwrap();
    let (mut current, mut rows, mut total) = (0u64, Vec::new(), 0u64);
    for batch in reader {
        let batch = batch.unwrap();
        let col = |name: &str| batch.column_by_name(name).unwrap_or_else(|| panic!("corpus has no column {name}")).clone();
        macro_rules! typed {
            ($name:expr, $ty:ty) => {
                col($name).as_any().downcast_ref::<$ty>().unwrap_or_else(|| panic!("column {} is not {}", $name, stringify!($ty))).clone()
            };
        }
        let (event_id, trace_id, span_id) = (typed!("event_id", BinaryArray), typed!("trace_id", BinaryArray), typed!("span_id", BinaryArray));
        let (ts, obs, seg) = (typed!("ts", Int64Array), typed!("obs", Int64Array), typed!("seg", Int64Array));
        let (severity, template_id) = (typed!("severity", UInt8Array), typed!("template_id", UInt64Array));
        let (body, service, host, region) = (typed!("body", StringArray), typed!("service", StringArray), typed!("host", StringArray), typed!("region", StringArray));
        for i in 0..batch.num_rows() {
            let s = seg.value(i) as u64;
            if s != current && !rows.is_empty() {
                each(current, std::mem::take(&mut rows));
            }
            current = s;
            let mut r = LogRecord::new(ts.value(i) as u64, Severity(severity.value(i)), body.value(i));
            r.observed_unix_nano = obs.value(i) as u64;
            r.event_id = uuid::Uuid::from_slice(event_id.value(i)).unwrap();
            r.service = Some(service.value(i).to_string());
            r.trace_id = (!trace_id.is_null(i)).then(|| trace_id.value(i).try_into().unwrap());
            r.span_id = (!span_id.is_null(i)).then(|| span_id.value(i).try_into().unwrap());
            r.template_id = Some(template_id.value(i));
            r.attributes = vec![
                Attr { key: "host".into(), value: AttrValue::Str(host.value(i).to_string()) },
                Attr { key: "region".into(), value: AttrValue::Str(region.value(i).to_string()) },
            ];
            rows.push(r);
            total += 1;
        }
    }
    if !rows.is_empty() {
        each(current, rows);
    }
    total
}

fn store_shape(store_dir: &Path) -> (usize, u64) {
    let files = store::list_parquet_files(store_dir);
    let bytes = files.iter().map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)).sum();
    (files.len(), bytes)
}

/// Files per partition directory: how many hold 1, 2, 3 and 4 or more.
fn files_per_partition(store_dir: &Path) -> [usize; 4] {
    let mut per: BTreeMap<PathBuf, usize> = BTreeMap::new();
    for f in store::list_parquet_files(store_dir) {
        *per.entry(f.parent().unwrap().to_path_buf()).or_default() += 1;
    }
    let mut out = [0; 4];
    for n in per.values() {
        out[(*n).min(4) - 1] += 1;
    }
    out
}

fn run_v3(a: &Args) -> serde_json::Value {
    let (wal_dir, store_dir) = (a.out.join("wal"), a.out.join("store"));
    let late_dir = wal::late_dir_beside(&wal_dir);
    let mut catalog = Catalog::open(&a.out.join("catalog.sqlite")).unwrap();
    let tenancy = Tenancy::default();
    // Attributes stay in the JSON column, as in the DuckDB-written corpus.
    let typed = TypedColumnsConfig { max_per_service: 0, ..Default::default() };
    let mut drain = Drain::default();
    let split = wal::SplitConfig {
        segment_bytes: u64::MAX,
        fsync_interval: Duration::from_secs(3600),
        fsync_bytes: u64::MAX,
        late_threshold: a.late_threshold,
        late_roll: a.late_roll,
        late_segment_bytes: logless_core::config::default_late_segment_bytes(),
    };
    let mut w = wal::SplitWal::open(&wal_dir, &late_dir, &split).unwrap();
    let (mut files_main, mut files_late, mut segments_main, mut segments_late) = (0usize, 0usize, 0usize, 0usize);
    let mut merge_pass = |w: Option<&wal::SplitWal>, catalog: &mut Catalog, drain: &mut Drain, now_secs: u64| {
        let (active_main, active_late) = match w {
            Some(w) => (Some(w.active_segments().0), Some(w.active_segments().1)),
            None => (None, None),
        };
        let m = merge::merge_all(&wal_dir, &store_dir, catalog, &tenancy, drain, &typed, active_main, now_secs).unwrap();
        let l = merge::merge_all(&late_dir, &store_dir, catalog, &tenancy, drain, &typed, active_late, now_secs).unwrap();
        files_main += m.files_written;
        files_late += l.files_written;
        // A closed segment that held no rows is not a segment of the corpus.
        segments_main += usize::from(m.records > 0) * m.segments_merged;
        segments_late += usize::from(l.records > 0) * l.segments_merged;
    };
    let rows = for_each_segment(&a.corpus, |_, rows| {
        // Arrival order, in the batches the writer would see; the clock is the rows' own arrival time.
        let mut now = 0;
        for batch in rows.chunks(1000) {
            now = batch.last().unwrap().observed_unix_nano;
            w.append(batch, now).unwrap();
            w.maybe_sync(now).unwrap();
        }
        w.roll_main().unwrap();
        merge_pass(Some(&w), &mut catalog, &mut drain, now / 1_000_000_000);
    });
    let late_records = w.late_records;
    drop(w);
    merge_pass(None, &mut catalog, &mut drain, 0);
    let before = store_shape(&store_dir);
    let partitions_before = files_per_partition(&store_dir);

    // Compaction to a fixed point, with no partition considered busy: the state a store reaches once its
    // partitions have been quiet for `compact_quiet`.
    let config = CompactConfig { typed, min_files: a.min_files, max_partitions: usize::MAX, ..Default::default() };
    let (mut passes, mut replaced) = (0, 0);
    loop {
        let r = compact::compact(&store_dir, &mut catalog, &config, None).unwrap();
        if r.partitions_compacted == 0 {
            break;
        }
        passes += 1;
        replaced += r.files_replaced;
    }
    let after = store_shape(&store_dir);
    serde_json::json!({
        "rows": rows, "late_records": late_records,
        "segments_main": segments_main, "segments_late": segments_late,
        "precompaction_files": files_main + files_late, "precompaction_files_main_wal": files_main, "precompaction_files_late_spool": files_late,
        "files_before_compaction": before.0, "bytes_before_compaction": before.1, "partitions_with_1_2_3_4plus_files_before": partitions_before,
        "compaction_passes": passes, "files_replaced": replaced,
        "files": after.0, "bytes": after.1, "partitions_with_1_2_3_4plus_files": files_per_partition(&store_dir),
        "catalog_files": catalog.file_count().unwrap(), "catalog_rows": catalog.totals().unwrap().0,
    })
}

/// The sort before ADR 0002: observed time as the third key.
fn sort_old(records: &mut [LogRecord]) {
    records.sort_by(|a, b| {
        a.service
            .cmp(&b.service)
            .then_with(|| a.trace_id.cmp(&b.trace_id))
            .then_with(|| a.observed_unix_nano.cmp(&b.observed_unix_nano))
            .then_with(|| a.event_id.cmp(&b.event_id))
    });
}

fn run_main_layout(a: &Args) -> serde_json::Value {
    let store_dir = a.out.join("store");
    let buckets = logless_core::LevelBuckets::default();
    let layout = TypedLayout::default();
    let mut precompaction = 0usize;
    let rows = for_each_segment(&a.corpus, |seg, rows| {
        let mut by: BTreeMap<(u64, String), Vec<LogRecord>> = BTreeMap::new();
        for r in rows {
            by.entry((r.observed_unix_nano / HOUR_NS, buckets.bucket_for(r.severity).name.clone())).or_default().push(r);
        }
        for ((hour, bucket), mut records) in by {
            sort_old(&mut records);
            let dir = store_dir.join(format!("hour={}", logless_core::partition::format_hour(hour))).join(format!("level={bucket}"));
            store::write_partition_file_sized(&dir.join(format!("s{seg:012}.parquet")), &records, &layout, OLD_ROW_GROUP_ROWS).unwrap();
            precompaction += 1;
        }
    });
    let before = store_shape(&store_dir);
    let partitions_before = files_per_partition(&store_dir);
    // The compaction rule, applied once every file is there: a partition with at least `min_files` files
    // becomes one file (none here is near the size limits).
    let mut per: BTreeMap<PathBuf, Vec<PathBuf>> = BTreeMap::new();
    for f in store::list_parquet_files(&store_dir) {
        per.entry(f.parent().unwrap().to_path_buf()).or_default().push(f);
    }
    let mut replaced = 0;
    for (dir, files) in per {
        if files.len() < a.min_files {
            continue;
        }
        let mut records = Vec::new();
        for f in &files {
            let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(f).unwrap()).unwrap().build().unwrap();
            for batch in reader {
                records.extend(logless_core::schema::from_record_batch(&batch.unwrap()));
            }
        }
        sort_old(&mut records);
        store::write_partition_file_sized(&dir.join("s000000000000-compacted.parquet"), &records, &layout, OLD_ROW_GROUP_ROWS).unwrap();
        for f in &files {
            std::fs::remove_file(f).unwrap();
        }
        replaced += files.len();
    }
    let after = store_shape(&store_dir);
    serde_json::json!({
        "rows": rows, "precompaction_files": precompaction,
        "files_before_compaction": before.0, "bytes_before_compaction": before.1, "partitions_with_1_2_3_4plus_files_before": partitions_before,
        "files_replaced": replaced, "files": after.0, "bytes": after.1, "partitions_with_1_2_3_4plus_files": files_per_partition(&store_dir),
    })
}

fn main() {
    let a = args();
    assert!(!a.out.exists(), "{} exists; give a new directory", a.out.display());
    std::fs::create_dir_all(&a.out).unwrap();
    let started = Instant::now();
    let mut result = match a.mode.as_str() {
        "v3" => run_v3(&a),
        "main" => run_main_layout(&a),
        other => panic!("--mode is v3 or main, not {other}"),
    };
    result["mode"] = a.mode.clone().into();
    result["late_threshold_secs"] = if a.late_threshold == Duration::MAX { serde_json::Value::Null } else { a.late_threshold.as_secs().into() };
    result["late_roll_secs"] = a.late_roll.as_secs().into();
    result["min_files"] = a.min_files.into();
    result["row_group_rows"] = (if a.mode == "v3" { store::ROW_GROUP_ROWS } else { OLD_ROW_GROUP_ROWS }).into();
    result["wall_secs"] = started.elapsed().as_secs_f64().into();
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}
