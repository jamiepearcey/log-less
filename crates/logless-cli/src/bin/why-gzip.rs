//! Why does gzip of the raw file beat a columnar store on the same logs?
//!
//! Three candidate explanations, and this separates them:
//!
//! 1. We store data the raw file does not contain (a UUID per record, binary
//!    timestamps) *on top of* the full line text.
//! 2. Parquet compresses each column chunk — and each page inside it —
//!    independently, so the compressor's window resets where a whole-file gzip
//!    keeps going.
//! 3. Our sort order may be scattering related lines rather than clustering
//!    them, which is the opposite of what it is for.
//!
//! Usage: why-gzip <file-of-log-lines> ...
use std::io::Write;
use std::path::Path;

use logless_core::model::{LogRecord, Severity};
use logless_core::{schema, store};

fn main() {
    let files: Vec<String> = std::env::args().skip(1).collect();
    let mut lines: Vec<String> = Vec::new();
    for path in &files {
        if let Ok(text) = std::fs::read_to_string(path) {
            lines.extend(text.lines().filter(|l| !l.trim().is_empty()).map(str::to_string));
        }
    }
    let raw: u64 = lines.iter().map(|l| l.len() as u64 + 1).sum();
    let n = lines.len() as f64;
    let tmp = std::env::temp_dir().join("logless-why");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();

    println!("{} lines, {raw} bytes raw ({:.1} B/line)\n", lines.len(), raw as f64 / n);

    // --- 1. The text alone, compressed as one stream. The floor for any
    // format that stores the message at all.
    let joined = lines.join("\n");
    let text_zstd = zstd_bytes(joined.as_bytes(), 3);
    println!("The message text alone, one zstd stream:");
    println!("  {text_zstd:>9} bytes  {:>6.2} B/line   <- the floor\n", text_zstd as f64 / n);

    // --- 2. The same text as a Parquet column, nothing else. Isolates the
    // cost of columnar framing from the cost of our extra columns.
    for (name, sorted, dictionary, page_kb) in [
        ("body column, file order, dict on", false, true, 1024),
        ("body column, file order, dict off", false, false, 1024),
        ("body column, sorted, dict on", true, true, 1024),
        ("body column, file order, 8 MB pages", false, true, 8192),
    ] {
        let mut records = body_only(&lines);
        if sorted {
            schema::sort_records(&mut records);
        }
        let path = tmp.join(format!("{}.parquet", name.replace(' ', "-").replace(',', "")));
        write_with(&path, &records, dictionary, page_kb * 1024);
        let body = store::column_sizes(&path)
            .unwrap()
            .into_iter()
            .find(|(c, _)| c == "body")
            .map(|(_, b)| b)
            .unwrap_or(0);
        println!(
            "  {body:>9} bytes  {:>6.2} B/line   {name}  ({:+.0}% vs the floor)",
            body as f64 / n,
            100.0 * (body as f64 / text_zstd as f64 - 1.0)
        );
    }

    // --- 3. What the other columns cost, in the file we actually write.
    let mut records = full_records(&lines);
    schema::sort_records(&mut records);
    let real = tmp.join("real.parquet");
    let stats = store::write_partition_file(&real, &records).unwrap();
    println!("\nThe file we actually write ({} bytes, {:.1} B/line):", stats.bytes, stats.bytes as f64 / n);
    for (column, bytes) in store::column_sizes(&real).unwrap() {
        if bytes as f64 / n < 0.005 {
            continue;
        }
        println!("  {bytes:>9} bytes  {:>6.2} B/line   {column}", bytes as f64 / n);
    }

    // The UUID is 48 bits of millisecond timestamp followed by 74 random bits.
    // In arrival order the timestamp prefix is near-monotonic and compresses;
    // our sort by (service, trace, observed) interleaves services, so that
    // prefix stops being monotonic. Worth knowing which effect dominates.
    let mut unsorted = full_records(&lines);
    let unsorted_path = tmp.join("arrival-order.parquet");
    store::write_partition_file(&unsorted_path, &unsorted).unwrap();
    let id_in_arrival_order = store::column_sizes(&unsorted_path)
        .unwrap()
        .into_iter()
        .find(|(c, _)| c == "event_id")
        .map(|(_, b)| b)
        .unwrap_or(0);
    unsorted.sort_by_key(|r| r.event_id);
    let id_path = tmp.join("id-order.parquet");
    store::write_partition_file(&id_path, &unsorted).unwrap();
    let id_in_id_order = store::column_sizes(&id_path)
        .unwrap()
        .into_iter()
        .find(|(c, _)| c == "event_id")
        .map(|(_, b)| b)
        .unwrap_or(0);
    println!(
        "\nevent_id column: {:.2} B/line in our sort order, {:.2} in arrival order, {:.2} sorted by id",
        140366.0 / n,
        id_in_arrival_order as f64 / n,
        id_in_id_order as f64 / n
    );
    println!("  (a UUIDv7 is 48 bits of ms timestamp + 74 random bits; 74 bits is 9.25 B of pure entropy)");

    let gzip = {
        let path = tmp.join("raw.log");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(joined.as_bytes()).unwrap();
        drop(file);
        let out = tmp.join("raw.gz");
        let handle = std::fs::File::create(&out).unwrap();
        std::process::Command::new("gzip")
            .args(["-6", "-c"])
            .arg(&path)
            .stdout(handle)
            .status()
            .ok()
            .and_then(|s| s.success().then(|| std::fs::metadata(&out).ok()).flatten())
            .map(|m| m.len())
            .unwrap_or(0)
    };
    println!("\ngzip -6 of the same text: {gzip} bytes ({:.1} B/line)", gzip as f64 / n);
}

fn body_only(lines: &[String]) -> Vec<LogRecord> {
    lines
        .iter()
        .enumerate()
        .map(|(i, line)| {
            let t = 1_700_000_000_000_000_000 + i as u64 * 1_000_000;
            let mut r = LogRecord::new(t, Severity::INFO, line.clone());
            r.observed_unix_nano = t;
            r
        })
        .collect()
}

/// The same records the agent would write, with a realistic spread of services
/// — contiguous runs, not round-robin. A real node's logs arrive in bursts per
/// service; interleaving them by `i % 4` is a property of a test harness, not
/// of production, and it destroys exactly the locality the sort is meant to
/// create.
fn full_records(lines: &[String]) -> Vec<LogRecord> {
    let services = ["api", "worker", "auth", "billing"];
    lines
        .iter()
        .enumerate()
        .map(|(i, line)| {
            let t = 1_700_000_000_000_000_000 + i as u64 * 1_000_000;
            let mut r = LogRecord::new(t, Severity::INFO, line.clone());
            r.observed_unix_nano = t;
            r.service = Some(services[(i / 500) % services.len()].to_string());
            let mut trace = [0u8; 16];
            trace[..8].copy_from_slice(&((i / 12) as u64).to_be_bytes());
            r.trace_id = Some(trace);
            r
        })
        .collect()
}

fn write_with(path: &Path, records: &[LogRecord], dictionary: bool, page_bytes: usize) {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_dictionary_enabled(dictionary)
        .set_data_page_size_limit(page_bytes)
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let batch = schema::to_record_batch(records).unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn zstd_bytes(input: &[u8], level: i32) -> u64 {
    use std::io::Read;
    use std::process::{Command, Stdio};
    let mut child = Command::new("zstd")
        .args(["-q", "-c", &format!("-{level}")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("zstd");
    child.stdin.take().unwrap().write_all(input).unwrap();
    let mut out = Vec::new();
    child.stdout.take().unwrap().read_to_end(&mut out).unwrap();
    let _ = child.wait();
    out.len() as u64
}
