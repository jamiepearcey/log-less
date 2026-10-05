//! What do typed attribute columns cost to write, and what do they save a
//! reader evaluating a predicate?
//!
//! Generates a synthetic request-log workload with a realistic attribute mix
//! (a handful of keys on every record, a long tail of rare ones), writes it
//! once with every attribute in the JSON and once with the promoted layout,
//! and evaluates predicates against both files the way a pushdown reader
//! would: the JSON file has to decode and parse every row's `attributes`; the
//! typed file reads one column and may skip row groups on min/max statistics.
//!
//! Synthetic data, so the absolute numbers say what the mechanism does, not
//! what a given customer's logs will do. Release builds only.
//!
//! Usage: typed-columns [records=500000] [reps=3]
use std::fs::File;
use std::path::Path;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, StringArray};
use logless_core::model::{Attr, AttrValue, LogRecord, Severity};
use logless_core::typed::{self, TypedColumnsConfig, TypedLayout};
use logless_core::{schema, store};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::statistics::Statistics;

const ROUTES: [&str; 12] = [
    "/api/orders", "/api/orders/:id", "/api/cart", "/api/checkout", "/api/login", "/api/logout",
    "/api/search", "/api/products/:id", "/api/users/:id", "/api/health", "/api/payments", "/api/refunds",
];
const SERVICES: [&str; 4] = ["checkout", "catalog", "auth", "payments"];
const BASE_NS: u64 = 1_727_000_000_000_000_000;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn workload(n: usize) -> Vec<LogRecord> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut records: Vec<LogRecord> = (0..n)
        .map(|i| {
            // One hour of traffic, 1 record per (3600 s / n).
            let observed = BASE_NS + (i as u64) * (3_600_000_000_000 / n as u64);
            let service = SERVICES[(rng.next() % 4) as usize];
            let route = ROUTES[(rng.next() % ROUTES.len() as u64) as usize];
            // Exponential latency, mean 80 ms: ~0.2% above 500 ms.
            let latency = (-(rng.unit().max(1e-12)).ln() * 80.0) as i64;
            let status = if rng.unit() < 0.01 { 500 } else if rng.unit() < 0.05 { 404 } else { 200 };
            let mut r = LogRecord::new(
                observed,
                if status == 500 { Severity::ERROR } else { Severity::INFO },
                format!("GET {route} {status} in {latency}ms"),
            );
            r.observed_unix_nano = observed;
            r.service = Some(service.into());
            r.trace_id = Some((rng.next() as u128 | ((rng.next() as u128) << 64)).to_le_bytes());
            let started = observed - latency as u64 * 1_000_000;
            let mut attrs = vec![
                ("latency_ms", AttrValue::I64(latency)),
                ("http.status_code", AttrValue::I64(status)),
                ("http.route", AttrValue::Str(route.into())),
                ("cache_hit", AttrValue::Bool(rng.unit() < 0.3)),
                ("db.duration_s", AttrValue::F64(latency as f64 / 1000.0 * rng.unit())),
                // Millisecond-precision, so the canonical form applies.
                ("request_start", AttrValue::Str(typed::format_timestamp((started / 1_000_000 * 1_000_000) as i64))),
                ("user_id", AttrValue::Str(format!("u{}", rng.next() % 200_000))),
            ];
            for k in 0..10 {
                if rng.unit() < 0.02 {
                    attrs.push((RARE[k], AttrValue::Str(format!("v{}", rng.next() % 50))));
                }
            }
            r.attributes = attrs.into_iter().map(|(k, v)| Attr { key: k.into(), value: v }).collect();
            r
        })
        .collect();
    schema::sort_records(&mut records);
    records
}

/// The losing shape: every service has its own 16 keys with random integer
/// values, so a mixed-service file gets 64 typed columns, each 75% NULL, and
/// nothing compresses. Promotion is a pure cost here unless someone filters.
fn sparse_workload(n: usize) -> Vec<LogRecord> {
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let mut records: Vec<LogRecord> = (0..n)
        .map(|i| {
            let observed = BASE_NS + (i as u64) * (3_600_000_000_000 / n as u64);
            let s = (rng.next() % 4) as usize;
            let mut r = LogRecord::new(observed, Severity::INFO, "event");
            r.observed_unix_nano = observed;
            r.service = Some(SERVICES[s].into());
            r.attributes = (0..16)
                .map(|k| Attr { key: format!("{}_k{k}", SERVICES[s]), value: AttrValue::I64((rng.next() >> 20) as i64) })
                .collect();
            r
        })
        .collect();
    schema::sort_records(&mut records);
    records
}

const RARE: [&str; 10] = [
    "feature_flag", "ab_bucket", "retry", "region", "tenant", "client_version", "locale",
    "referrer", "experiment", "debug_id",
];

fn median(mut d: Vec<Duration>) -> Duration {
    d.sort();
    d[d.len() / 2]
}

fn leaf(builder: &ParquetRecordBatchReaderBuilder<File>, name: &str) -> Option<usize> {
    builder.metadata().file_metadata().schema_descr().columns().iter().position(|c| c.name() == name)
}

/// Rows where `latency_ms > 500`, by parsing every row's JSON.
fn json_predicate(path: &Path) -> (u64, u64, u64) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let idx = leaf(&builder, "attributes").unwrap();
    let bytes: u64 =
        builder.metadata().row_groups().iter().map(|g| g.column(idx).compressed_size() as u64).sum();
    let mask = ProjectionMask::leaves(builder.metadata().file_metadata().schema_descr(), [idx]);
    let reader = builder.with_projection(mask).build().unwrap();
    let (mut matched, mut rows) = (0u64, 0u64);
    for batch in reader {
        let batch = batch.unwrap();
        let json = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..json.len() {
            rows += 1;
            if json.is_null(row) {
                continue;
            }
            let attrs: Vec<Attr> = serde_json::from_str(json.value(row)).unwrap_or_default();
            if attrs.iter().any(|a| a.key == "latency_ms" && matches!(a.value, AttrValue::I64(v) if v > 500)) {
                matched += 1;
            }
        }
    }
    (matched, rows, bytes)
}

/// The same predicate on the typed column: skip row groups whose max says no
/// row can match, read one column for the rest.
fn typed_predicate(path: &Path, column: &str, threshold: i64) -> (u64, u64, u64, usize, usize) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let idx = leaf(&builder, column).unwrap();
    let groups = builder.metadata().row_groups();
    let keep: Vec<usize> = (0..groups.len())
        .filter(|&g| match groups[g].column(idx).statistics() {
            Some(Statistics::Int64(s)) => s.max_opt().is_none_or(|m| *m > threshold),
            _ => true,
        })
        .collect();
    let bytes: u64 = keep.iter().map(|&g| groups[g].column(idx).compressed_size() as u64).sum();
    let total_groups = groups.len();
    let mask = ProjectionMask::leaves(builder.metadata().file_metadata().schema_descr(), [idx]);
    let reader = builder.with_projection(mask).with_row_groups(keep.clone()).build().unwrap();
    let (mut matched, mut rows) = (0u64, 0u64);
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        rows += col.len() as u64;
        matched += col.iter().flatten().filter(|v| *v > threshold).count() as u64;
    }
    (matched, rows, bytes, keep.len(), total_groups)
}

/// Row groups a min/max check can skip for `column > threshold` (or `>=` on
/// a timestamp), to say whether the sort order lets statistics prune at all.
fn prunable(path: &Path, column: &str, threshold: i64) -> (usize, usize) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let Some(idx) = leaf(&builder, column) else { return (0, 0) };
    let groups = builder.metadata().row_groups();
    let skip = groups
        .iter()
        .filter(|g| matches!(g.column(idx).statistics(), Some(Statistics::Int64(s)) if s.max_opt().is_some_and(|m| *m < threshold)))
        .count();
    (skip, groups.len())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let n: usize = args.first().and_then(|a| a.parse().ok()).unwrap_or(500_000);
    let reps: usize = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(3);
    let records = workload(n);
    println!("{n} records, 4 services, 7 common keys + 10 rare keys (2% each), {reps} reps, median\n");

    let t = Instant::now();
    let promoted = typed::plan(&records, &TypedLayout::default(), &TypedColumnsConfig::default(), 0);
    let plan_time = t.elapsed();
    let layout = TypedLayout::new(promoted);
    // Steady state: every frequent key is already registered, which is what
    // most merges see.
    let t = Instant::now();
    let again = typed::plan(&records, &layout, &TypedColumnsConfig::default(), 0);
    let steady = t.elapsed();
    assert!(again.is_empty());
    println!(
        "plan: {:.1} ms cold (empty registry), {:.1} ms steady (keys registered); {} keys promoted:",
        plan_time.as_secs_f64() * 1e3,
        steady.as_secs_f64() * 1e3,
        layout.len()
    );
    for c in layout.columns() {
        println!("  {:<24} {:<9} (charged to {})", c.column, c.ty.as_str(), c.service);
    }

    let tmp = std::env::temp_dir().join(format!("logless-typed-columns-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    let json_path = tmp.join("json.parquet");
    let typed_path = tmp.join("typed.parquet");

    let write = |path: &Path, layout: &TypedLayout| {
        let mut times = Vec::new();
        let mut stats = None;
        for _ in 0..reps {
            let t = Instant::now();
            stats = Some(store::write_partition_file_with(path, &records, layout).unwrap());
            times.push(t.elapsed());
        }
        (median(times), stats.unwrap())
    };
    let (json_write, json_stats) = write(&json_path, &TypedLayout::default());
    let (typed_write, typed_stats) = write(&typed_path, &layout);

    println!("\nwrite (to_record_batch + parquet + fsync + footer read):");
    let line = |name: &str, d: Duration, bytes: u64| {
        println!(
            "  {name:<14} {:>8.1} ms  {:>8.0} krec/s  {:>11} bytes  {:>5.1} B/record",
            d.as_secs_f64() * 1e3,
            n as f64 / d.as_secs_f64() / 1e3,
            bytes,
            bytes as f64 / n as f64
        );
    };
    line("json only", json_write, json_stats.bytes);
    line("typed", typed_write, typed_stats.bytes);
    println!(
        "  typed/json: time {:.2}x, bytes {:.2}x",
        typed_write.as_secs_f64() / json_write.as_secs_f64(),
        typed_stats.bytes as f64 / json_stats.bytes as f64
    );

    // CPU only, no I/O: the part of the write the layout changes, without
    // fsync variance on a shared disk.
    let encode = |layout: &TypedLayout| {
        median(
            (0..reps)
                .map(|_| {
                    let t = Instant::now();
                    std::hint::black_box(schema::to_record_batch(&records, layout).unwrap());
                    t.elapsed()
                })
                .collect(),
        )
    };
    let (json_encode, typed_encode) = (encode(&TypedLayout::default()), encode(&layout));
    println!(
        "\nencode only (to_record_batch, no parquet, no I/O):\n  json only {:>8.1} ms\n  typed     {:>8.1} ms\n  typed/json: {:.2}x",
        json_encode.as_secs_f64() * 1e3,
        typed_encode.as_secs_f64() * 1e3,
        typed_encode.as_secs_f64() / json_encode.as_secs_f64()
    );

    println!("\ncolumn bytes (attribute-related only):");
    for (label, path) in [("json only", &json_path), ("typed", &typed_path)] {
        let sizes = store::column_sizes(path).unwrap();
        let attr: Vec<_> =
            sizes.iter().filter(|(c, _)| c == "attributes" || c.starts_with(typed::COLUMN_PREFIX)).collect();
        let total: u64 = attr.iter().map(|(_, b)| b).sum();
        println!("  {label}: {total} bytes");
        for (c, b) in attr {
            println!("    {c:<24} {b:>10}");
        }
    }

    // Read: latency_ms > 500.
    let mut jt = Vec::new();
    let mut tt = Vec::new();
    let mut json_result = (0, 0, 0);
    let mut typed_result = (0, 0, 0, 0, 0);
    for _ in 0..reps {
        let t = Instant::now();
        json_result = json_predicate(&json_path);
        jt.push(t.elapsed());
        let t = Instant::now();
        typed_result = typed_predicate(&typed_path, "attr_latency_ms", 500);
        tt.push(t.elapsed());
    }
    let (jm, jrows, jbytes) = json_result;
    let (tm, trows, tbytes, kept, groups) = typed_result;
    assert_eq!(jm, tm, "both paths must find the same rows");
    println!("\nread: latency_ms > 500 ({jm} of {n} rows match, {:.2}%)", jm as f64 * 100.0 / n as f64);
    println!(
        "  json:  {:>8.1} ms, {jrows} rows decoded, {jbytes} compressed bytes read",
        median(jt.clone()).as_secs_f64() * 1e3
    );
    println!(
        "  typed: {:>8.1} ms, {trows} rows decoded, {tbytes} compressed bytes read, {kept}/{groups} row groups read",
        median(tt.clone()).as_secs_f64() * 1e3
    );
    println!(
        "  typed/json: time {:.3}x, bytes {:.3}x",
        median(tt).as_secs_f64() / median(jt).as_secs_f64(),
        tbytes as f64 / jbytes as f64
    );

    // Can min/max statistics skip row groups? Depends on the sort order
    // (service, trace_id, observed): trace ids are random, so values that are
    // not clustered by service are spread across every row group.
    println!("\nrow groups skippable on min/max alone (typed file):");
    let last_tenth = (BASE_NS + 3_240_000_000_000) as i64;
    for (label, column, threshold) in [
        ("latency_ms > 500", "attr_latency_ms", 501),
        ("http.status_code >= 500", "attr_http_status_code", 500),
        ("request_start >= last 6 min", "attr_request_start", last_tenth),
    ] {
        // Timestamps are INT64 in Parquet, so their statistics are Int64 too.
        let (skip, total) = prunable(&typed_path, column, threshold);
        println!("  {label:<30} {skip}/{total}");
    }

    // The losing shape.
    let sparse = sparse_workload(n);
    let sparse_layout =
        TypedLayout::new(typed::plan(&sparse, &TypedLayout::default(), &TypedColumnsConfig::default(), 0));
    let enc = |layout: &TypedLayout| {
        median(
            (0..reps)
                .map(|_| {
                    let t = Instant::now();
                    std::hint::black_box(schema::to_record_batch(&sparse, layout).unwrap());
                    t.elapsed()
                })
                .collect(),
        )
    };
    let (sj, st) = (enc(&TypedLayout::default()), enc(&sparse_layout));
    let sj_bytes = store::write_partition_file_with(&tmp.join("sj.parquet"), &sparse, &TypedLayout::default())
        .unwrap()
        .bytes;
    let st_bytes = store::write_partition_file_with(&tmp.join("st.parquet"), &sparse, &sparse_layout).unwrap().bytes;
    println!(
        "\nlosing shape: 4 services x 16 own keys, random ints ({} columns promoted, each ~75% NULL)",
        sparse_layout.len()
    );
    println!(
        "  encode: json {:.1} ms, typed {:.1} ms ({:.2}x)",
        sj.as_secs_f64() * 1e3,
        st.as_secs_f64() * 1e3,
        st.as_secs_f64() / sj.as_secs_f64()
    );
    println!(
        "  bytes:  json {sj_bytes}, typed {st_bytes} ({:.2}x)",
        st_bytes as f64 / sj_bytes as f64
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
