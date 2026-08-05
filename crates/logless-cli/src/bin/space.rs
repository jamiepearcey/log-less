//! What does the storage format actually cost, against the alternatives?
//!
//! The honest baseline is not "raw text". Nobody keeps uncompressed logs — a
//! customer's status quo is a rotated file run through gzip, or a vendor
//! charging by ingested byte. So this measures against `gzip -6` and `zstd -3`
//! on the same bytes, and reports the per-column breakdown that says *where*
//! the space actually goes.
//!
//! Usage: space <file-of-log-lines> [more files ...]
use std::io::Write;
use std::path::Path;

use logless_core::drain::{Drain, DrainConfig};
use logless_core::model::{LogRecord, Severity};
use logless_core::{schema, store};

fn main() {
    let files: Vec<String> = std::env::args().skip(1).collect();
    if files.is_empty() {
        eprintln!("usage: space <file-of-log-lines> ...");
        std::process::exit(2);
    }
    let mut lines: Vec<String> = Vec::new();
    for path in &files {
        if let Ok(text) = std::fs::read_to_string(path) {
            lines.extend(text.lines().filter(|l| !l.trim().is_empty()).map(str::to_string));
        }
    }
    let raw: u64 = lines.iter().map(|l| l.len() as u64 + 1).sum();
    println!("{} lines, {} bytes of raw text\n", lines.len(), raw);

    let tmp = std::env::temp_dir().join("logless-space");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let raw_path = tmp.join("raw.log");
    let mut file = std::fs::File::create(&raw_path).unwrap();
    for line in &lines {
        writeln!(file, "{line}").unwrap();
    }
    drop(file);

    // Build records the way the agent does: severity parsed, template mined,
    // then sorted by (service, trace, time) — the sort is load-bearing, so it
    // is measured with and without.
    let mut drain = Drain::new(DrainConfig::default());
    let mut records: Vec<LogRecord> = lines
        .iter()
        .enumerate()
        .map(|(i, line)| {
            let observed = 1_700_000_000_000_000_000 + i as u64 * 1_000_000;
            let severity = first_level(line).unwrap_or(Severity::INFO);
            let mut record = LogRecord::new(observed, severity, line.clone());
            record.observed_unix_nano = observed;
            record.service = Some(service_of(i));
            record.trace_id = Some(trace_of(i));
            record.template_id = drain.add_line(line, observed / 1_000_000_000).map(|m| m.template_id);
            record
        })
        .collect();

    let unsorted_path = tmp.join("unsorted.parquet");
    let unsorted = store::write_partition_file(&unsorted_path, &records).unwrap();

    schema::sort_records(&mut records);
    let sorted_path = tmp.join("sorted.parquet");
    let sorted = store::write_partition_file(&sorted_path, &records).unwrap();

    // Variants, to find where the cost actually is rather than guessing.
    let variants = [
        ("delta-encoded timestamps", write_variant(&tmp, "delta", &records, true, false, 3)),
        ("+ no event_id", write_variant(&tmp, "noid", &records, true, true, 3)),
        ("+ zstd level 9", write_variant(&tmp, "z9", &records, true, true, 9)),
    ];

    let gzip = external("gzip", &["-6", "-c"], &raw_path, &tmp.join("raw.gz"));
    let zstd = external("zstd", &["-3", "-q", "-c"], &raw_path, &tmp.join("raw.zst"));

    let row = |name: &str, bytes: u64| {
        println!(
            "{name:<38} {bytes:>10}  {:>6.2}x  {:>8.1} B/record",
            raw as f64 / bytes as f64,
            bytes as f64 / records.len() as f64
        );
    };
    println!("{:<38} {:>10}  {:>7}  {:>10}", "", "bytes", "vs raw", "per record");
    row("raw text", raw);
    if let Some(bytes) = gzip {
        row("gzip -6 of the raw file", bytes);
    }
    if let Some(bytes) = zstd {
        row("zstd -3 of the raw file", bytes);
    }
    row("logless parquet, unsorted", unsorted.bytes);
    row("logless parquet, sorted (what we write)", sorted.bytes);
    for (name, bytes) in variants {
        row(name, bytes);
    }

    println!("\ncolumn breakdown of the sorted file:");
    for (name, bytes) in store::column_sizes(&sorted_path).unwrap() {
        println!(
            "  {name:<16} {bytes:>10}  {:>5.1}%  {:>7.2} B/record",
            100.0 * bytes as f64 / sorted.bytes as f64,
            bytes as f64 / records.len() as f64
        );
    }
    println!("\ntemplates mined: {}", drain.template_count());
}

/// Writes the same records with different encodings, to locate the cost.
///
/// `delta` switches the two timestamp columns to DELTA_BINARY_PACKED, which is
/// what monotonic integers want and what PLAIN + zstd handles badly. `drop_id`
/// omits the UUID entirely — not a proposal, a measurement of what it costs.
fn write_variant(
    dir: &Path,
    name: &str,
    records: &[LogRecord],
    delta: bool,
    drop_id: bool,
    level: i32,
) -> u64 {
    use arrow::array::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, Encoding, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let full = schema::to_record_batch(records).unwrap();
    let batch = if drop_id {
        let indices: Vec<usize> = (0..full.num_columns())
            .filter(|i| full.schema().field(*i).name() != "event_id")
            .collect();
        RecordBatch::try_new(
            std::sync::Arc::new(full.schema().project(&indices).unwrap()),
            indices.iter().map(|i| full.column(*i).clone()).collect(),
        )
        .unwrap()
    } else {
        full
    };

    let mut props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(level).unwrap()))
        .set_dictionary_enabled(true);
    if delta {
        for column in ["timestamp", "observed"] {
            let path = parquet::schema::types::ColumnPath::from(column);
            props = props
                .set_column_encoding(path.clone(), Encoding::DELTA_BINARY_PACKED)
                .set_column_dictionary_enabled(path, false);
        }
    }
    let path = dir.join(format!("{name}.parquet"));
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props.build())).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    std::fs::metadata(&path).unwrap().len()
}

/// Compressed size, or `None` if the tool is not installed.
fn external(program: &str, args: &[&str], input: &Path, output: &Path) -> Option<u64> {
    let file = std::fs::File::create(output).ok()?;
    let status = std::process::Command::new(program)
        .args(args)
        .arg(input)
        .stdout(file)
        .status()
        .ok()?;
    status.success().then(|| std::fs::metadata(output).ok())?.map(|m| m.len())
}

fn first_level(line: &str) -> Option<Severity> {
    line.split_whitespace()
        .take(4)
        .find_map(|t| Severity::from_text(t.trim_matches(|c: char| !c.is_alphanumeric())))
}

/// A plausible spread of services and traces, since the corpora have neither
/// and the sort order depends on both.
fn service_of(i: usize) -> String {
    ["api", "worker", "auth", "billing"][i % 4].to_string()
}

fn trace_of(i: usize) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&((i / 12) as u64).to_be_bytes());
    id
}
