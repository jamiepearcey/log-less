//! `logless` — the agent binary.
//!
//! Week 1–2 scope: prove the ingest invariant end to end and turn the WAL into
//! committed, queryable state. A stdin receiver feeds the bounded queue, a
//! writer thread group-commits to the WAL, and a maintenance thread merges
//! segments into partitioned Parquet, enforces retention and relieves disk
//! pressure. Real receivers (OTLP, file tail, HEC) and forwarding come next.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::{Parser, Subcommand};
use logless_core::budget::{self, DiskUsage, Pressure};
use logless_core::catalog::Catalog;
use logless_core::drain::{Drain, DrainConfig};
use logless_core::config::{Config, IngestConfig, LevelBuckets, PushdownConfig, StorageConfig};
use logless_core::model::{LevelClass, LogRecord, Severity};
use logless_core::forward::{Dispatch, ForwardStats, Forwarder, HttpTransport, Outbound};
use logless_core::ring::{Capture, ContextLine, ContextWindow, KeyTier, Ring, RingConfig, Suppressed};
use logless_core::hec::{self, Admitted};
use logless_core::sentry;
use logless_core::otlp::{self, Accepted};
use logless_core::spool::{Spool, SpoolConfig};
use logless_core::tail::Tailer;
use logless_core::{merge, now_unix_nanos, now_unix_secs, partition, queue, retention, store, wal};

/// How often the maintenance thread merges, expires and checks disk pressure.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);
/// How often rolled-up aggregate events are flushed upstream. A long-running
/// agent must not hold a storm's counter until shutdown.
const AGGREGATE_FLUSH_INTERVAL: Duration = Duration::from_secs(30);
/// How long the WAL writer waits for more records before sealing a frame.
/// Bounded by, and much smaller than, the fsync interval that already defines
/// the crash window, so it adds no exposure that was not already there.
const WAL_BATCH_LINGER: Duration = Duration::from_millis(5);
/// How often tailed files are checked for new data.
const TAIL_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Decoded OTLP batches buffered between the receiver threads and the record
/// path. Deep enough to absorb a burst, shallow enough that a stalled pipeline
/// turns into a 503 — and therefore an exporter retry — within a second or so.
const OTLP_PENDING_BATCHES: usize = 256;
/// Batches drained per loop pass, so a busy exporter cannot starve a tailer.
const OTLP_BATCHES_PER_PASS: usize = 64;
/// Same for HEC. Smaller because each batch also carries an ack obligation, and
/// the sooner a batch is bound the sooner its client can stop holding it.
const HEC_PENDING_BATCHES: usize = 256;
const HEC_BATCHES_PER_PASS: usize = 32;
/// Sentry proxy batches buffered between the receiver threads and the record
/// path. Each batch is one envelope's worth of items, so this is deeper in
/// events than it looks.
const SENTRY_PENDING_BATCHES: usize = 256;
const SENTRY_BATCHES_PER_PASS: usize = 32;

/// Forwarder results, published back to the main thread at shutdown.
#[derive(Default)]
struct ForwardSummary {
    per_destination: Vec<(String, ForwardStats)>,
    dispatch: logless_core::forward::DispatchStats,
}

#[derive(Parser)]
#[command(
    name = "logless",
    version,
    about = "Local log buffer that keeps what your vendor drops"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print a default configuration file.
    Init {
        #[arg(long, default_value = "/var/lib/logless")]
        data_dir: PathBuf,
    },
    /// Ingest newline-delimited records from stdin, merging in the background.
    Run {
        #[arg(long)]
        config: PathBuf,
        /// Stop after this many records (0 = until stdin closes).
        #[arg(long, default_value_t = 0)]
        max_records: u64,
        /// Skip the background merge/retention/pressure thread.
        #[arg(long)]
        no_maintenance: bool,
        /// Tail these files instead of reading stdin. Repeatable.
        #[arg(long = "tail")]
        tail: Vec<PathBuf>,
        /// When tailing a file for the first time, start at the end rather than
        /// replaying its history.
        #[arg(long)]
        from_end: bool,
        /// Accept OTLP/HTTP logs, overriding `otlp.enabled` in the config.
        #[arg(long)]
        otlp: bool,
        /// Listen address for the OTLP receiver, overriding `otlp.addr`.
        #[arg(long)]
        otlp_addr: Option<String>,
        /// Accept OTLP over gRPC, overriding `otlp.grpc.enabled`. This is what
        /// an OTel Collector's default `otlp` exporter speaks.
        #[arg(long)]
        otlp_grpc: bool,
        /// Listen address for the OTLP gRPC receiver, overriding
        /// `otlp.grpc.addr`.
        #[arg(long)]
        otlp_grpc_addr: Option<String>,
        /// Accept Splunk HEC, overriding `hec.enabled` in the config.
        #[arg(long)]
        hec: bool,
        /// Listen address for the HEC receiver, overriding `hec.addr`.
        #[arg(long)]
        hec_addr: Option<String>,
        /// Act as a Sentry ingest proxy, overriding `sentry.enabled`.
        #[arg(long)]
        sentry: bool,
        /// Listen address for the Sentry proxy, overriding `sentry.addr`.
        #[arg(long)]
        sentry_addr: Option<String>,
        /// Write pushdown context windows here as JSONL. This is what the
        /// Sentry/Splunk forwarders will consume; until they exist, it is how
        /// the feature is demonstrated and tested.
        #[arg(long)]
        pushdown_out: Option<PathBuf>,
    },
    /// Replay the WAL, reporting (and optionally repairing) torn tails.
    Recover {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        repair: bool,
    },
    /// Merge WAL segments into partitioned Parquet and commit the catalog.
    Merge {
        #[arg(long)]
        config: PathBuf,
    },
    /// Check the whole-path accounting invariant.
    ///
    /// `received == committed + still-in-WAL + deliberately dropped`. The
    /// in-process invariant stops at the WAL, and both merge data-loss bugs
    /// lived past that point.
    Verify {
        #[arg(long)]
        config: PathBuf,
        /// Records allowed to be missing, for auditing after a known SIGKILL.
        ///
        /// Zero by default, and that is the point: after a clean shutdown the
        /// counters are written *after* the final WAL sync, so there is no
        /// window and no slack to give. A tolerance derived from the fsync
        /// byte budget worked out at 65,000 records, and made this check report
        /// OK while 30,000 records were missing.
        #[arg(long, default_value_t = 0)]
        tolerance: u64,
    },
    /// Combine small per-segment files into larger ones.
    Compact {
        #[arg(long)]
        config: PathBuf,
    },
    /// Expire partitions past their level bucket's retention.
    Retention {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        dry_run: bool,
    },
    /// Inspect or rebuild the catalog.
    Catalog {
        #[arg(long)]
        config: PathBuf,
        /// Discard the catalog and reconstruct it from Parquet footers.
        #[arg(long)]
        rebuild: bool,
    },
    /// Disk usage, pressure and store contents.
    Status {
        #[arg(long)]
        config: PathBuf,
    },
    /// Show the configured level buckets and their retention.
    Buckets {
        #[arg(long)]
        config: PathBuf,
    },
    /// Show the mined template dictionary, most frequent first.
    Templates {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Query committed history, and optionally push it upstream.
    ///
    /// This is the "undo button": verbosity you chose not to forward at the
    /// time can be sent to the vendor now, after the incident has told you
    /// which window matters.
    Replay {
        #[arg(long)]
        config: PathBuf,
        /// How far back to look, e.g. `15m`, `2h`.
        #[arg(long, default_value = "1h")]
        since: String,
        #[arg(long)]
        service: Option<String>,
        /// Minimum OTel severity number (17 = ERROR).
        #[arg(long)]
        min_severity: Option<u8>,
        /// Only records whose body contains this substring.
        #[arg(long)]
        contains: Option<String>,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        /// Send the matched records to the configured destinations. Without
        /// this, `replay` only prints what it would send.
        #[arg(long)]
        forward: bool,
        /// Show which files the query would open, and stop.
        #[arg(long)]
        explain: bool,
    },
    /// Measure template-mining throughput on synthetic lines.
    Bench {
        #[arg(long, default_value_t = 1_000_000)]
        lines: u64,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LOGLESS_LOG")
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Init { data_dir } => {
            let config = Config {
                schema_version: logless_core::config::SCHEMA_VERSION,
                storage: StorageConfig {
                    data_dir,
                    disk_budget_bytes: 32 * 1024 * 1024 * 1024,
                    wal_segment_bytes: 128 * 1024 * 1024,
                    wal_fsync_interval: Duration::from_millis(100),
                    wal_fsync_bytes: 4 * 1024 * 1024,
                },
                ingest: IngestConfig::default(),
                retention: LevelBuckets::default(),
                pushdown: PushdownConfig::default(),
                otlp: otlp::ReceiverConfig::default(),
                hec: hec::HecConfig::default(),
                sentry: sentry::SentryConfig::default(),
                destinations: Vec::new(),
            };
            print!("{}", toml::to_string_pretty(&config)?);
        }

        Command::Run {
            config,
            max_records,
            no_maintenance,
            tail,
            from_end,
            otlp,
            otlp_addr,
            otlp_grpc,
            otlp_grpc_addr,
            hec,
            hec_addr,
            sentry: sentry_proxy,
            sentry_addr,
            pushdown_out,
        } => {
            let mut config = load(&config)?;
            // Flags override the file so a demo needs no config edit.
            if otlp {
                config.otlp.enabled = true;
            }
            if let Some(addr) = otlp_addr {
                config.otlp.enabled = true;
                config.otlp.addr = addr;
            }
            if otlp_grpc {
                config.otlp.grpc.enabled = true;
            }
            if let Some(addr) = otlp_grpc_addr {
                config.otlp.grpc.enabled = true;
                config.otlp.grpc.addr = addr;
            }
            if hec {
                config.hec.enabled = true;
            }
            if let Some(addr) = hec_addr {
                config.hec.enabled = true;
                config.hec.addr = addr;
            }
            if sentry_proxy {
                config.sentry.enabled = true;
            }
            if let Some(addr) = sentry_addr {
                config.sentry.enabled = true;
                config.sentry.addr = addr;
            }
            run(
                &config,
                max_records,
                !no_maintenance,
                pushdown_out,
                tail,
                from_end,
            )?
        }

        Command::Replay {
            config,
            since,
            service,
            min_severity,
            contains,
            limit,
            forward,
            explain,
        } => {
            let config = load(&config)?;
            let window = humantime::parse_duration(&since)?;
            let now = now_unix_nanos();
            let query = logless_core::scan::Query {
                from_unix_nano: Some(
                    now.saturating_sub(window.as_secs().saturating_mul(1_000_000_000)),
                ),
                until_unix_nano: None,
                service,
                min_severity: min_severity.map(Severity),
                contains,
                limit,
            };
            replay(&config, &query, forward, explain)?;
        }

        Command::Recover { config, repair } => {
            let config = load(&config)?;
            let report = wal::recover(&config.wal_dir(), repair, |_| {})?;
            println!(
                "segments={} batches={} records={} truncated={}",
                report.segments_read, report.batches, report.records, report.truncated.len()
            );
            for (path, len) in &report.truncated {
                println!("  torn tail: {} -> {len} bytes", path.display());
            }
        }

        Command::Merge { config } => {
            let config = load(&config)?;
            let mut catalog = Catalog::open(&catalog_path(&config))?;
            let mut drain =
                Drain::restore(DrainConfig::default(), catalog.load_templates()?);
            let report = merge::merge_all(
                &config.wal_dir(),
                &config.store_dir(),
                &mut catalog,
                &config.retention,
                &mut drain,
                None,
                now_unix_secs(),
            )?;
            println!(
                "merged={} skipped={} records={} files={} bytes_written={} wal_freed={} truncated={}",
                report.segments_merged,
                report.segments_skipped,
                report.records,
                report.files_written,
                report.bytes_written,
                report.wal_bytes_freed,
                report.truncated
            );
            println!(
                "templated={} untemplated={} new_templates={} dictionary={}",
                report.templated,
                report.untemplated,
                report.new_templates,
                drain.template_count()
            );
        }

        Command::Verify { config, tolerance } => {
            let config = load(&config)?;
            let counters = logless_core::counters::Counters::load(&config.storage.data_dir);
            let catalog = Catalog::open(&catalog_path(&config))?;
            let (committed, _bytes) = catalog.totals()?;

            // Replay the WAL to count what is accepted but not yet merged.
            let mut in_wal = 0u64;
            let report = wal::recover(&config.wal_dir(), false, |batch| {
                in_wal += batch.len() as u64;
            })?;

            let audit = logless_core::counters::Audit {
                received: counters.received,
                committed,
                in_wal,
                dropped: counters.dropped,
            };
            println!(
                "received={} committed={} in_wal={} dropped={} unaccounted={}",
                audit.received, audit.committed, audit.in_wal, audit.dropped, audit.unaccounted()
            );
            println!("wal segments replayed: {}", report.segments_read);
            if audit.unaccounted() < 0 {
                // More on disk than the counters know about: the previous run
                // did not exit cleanly, so the counter file predates records
                // that were nonetheless committed. Stale bookkeeping, not loss.
                println!(
                    "counters are stale by {} records — the previous run did not exit cleanly",
                    -audit.unaccounted()
                );
            } else if audit.balances(tolerance) {
                println!("OK: every accepted record is committed, still in the WAL, or was deliberately dropped");
            } else {
                println!(
                    "ACCOUNTING VIOLATION: {} records unaccounted for (tolerance {tolerance})",
                    audit.unaccounted()
                );
                std::process::exit(2);
            }
        }

        Command::Compact { config } => {
            let config = load(&config)?;
            let mut catalog = Catalog::open(&catalog_path(&config))?;
            let report = logless_core::compact::compact(
                &config.store_dir(),
                &mut catalog,
                &logless_core::compact::CompactConfig::default(),
                None,
            )?;
            println!(
                "compacted {} partitions: {} files -> {} ({} rows), {} -> {} bytes ({:.0}% saved)",
                report.partitions_compacted,
                report.files_replaced,
                report.files_written,
                report.rows,
                report.bytes_before,
                report.bytes_after,
                report.saved_fraction() * 100.0
            );
        }

        Command::Retention { config, dry_run } => {
            let config = load(&config)?;
            let report = retention::enforce(
                &config.store_dir(),
                &config.retention,
                now_unix_secs(),
                dry_run,
            );
            println!(
                "{} expired={} reclaimed={}B orphan_buckets={} errors={}",
                if dry_run { "would expire" } else { "expired" },
                report.expired.len(),
                report.bytes_reclaimed,
                report.orphan_buckets.len(),
                report.errors.len()
            );
            for p in &report.expired {
                println!("  {} ({}B)", p.path.display(), p.bytes);
            }
            for k in &report.orphan_buckets {
                println!(
                    "  orphan bucket {:?} at hour={} — not in config, left in place",
                    k.bucket,
                    partition::format_hour(k.epoch_hour)
                );
            }
            if !dry_run {
                let mut catalog = Catalog::open(&catalog_path(&config))?;
                let forgotten = catalog.forget_missing_files()?;
                println!("catalog rows removed: {forgotten}");
            }
        }

        Command::Catalog { config, rebuild } => {
            let config = load(&config)?;
            let mut catalog = Catalog::open(&catalog_path(&config))?;
            if rebuild {
                let n = catalog.rebuild(&config.store_dir())?;
                println!("rebuilt from Parquet footers: {n} files");
            }
            let files = catalog.list_files()?;
            let (rows, bytes) = catalog.totals()?;
            println!("files={} rows={} bytes={}", files.len(), rows, bytes);
            for f in files.iter().take(50) {
                println!(
                    "  hour={} level={:<10} rows={:<8} bytes={:<10} {}",
                    partition::format_hour(f.key.epoch_hour),
                    f.key.bucket,
                    f.stats.rows,
                    f.stats.bytes,
                    f.path.display()
                );
            }
            if files.len() > 50 {
                println!("  … {} more", files.len() - 50);
            }
        }

        Command::Status { config } => {
            let config = load(&config)?;
            let catalog = Catalog::open(&catalog_path(&config))?;
            let usage = DiskUsage::measure(
                &config.wal_dir(),
                &catalog,
                config.storage.disk_budget_bytes,
            );
            let (rows, _) = catalog.totals()?;
            println!(
                "wal={} store={} used={} budget={} ({:.1}%) pressure={:?}",
                human(usage.wal_bytes),
                human(usage.store_bytes),
                human(usage.used()),
                human(usage.budget_bytes),
                usage.fraction() * 100.0,
                usage.pressure()
            );
            println!(
                "rows={} parquet_files={} wal_segments={}",
                rows,
                store::list_parquet_files(&config.store_dir()).len(),
                wal::list_segments(&config.wal_dir())?.len()
            );
        }

        Command::Templates { config, limit } => {
            let config = load(&config)?;
            let catalog = Catalog::open(&catalog_path(&config))?;
            let templates = catalog.top_templates(limit)?;
            let total: u64 = catalog.load_templates()?.iter().map(|t| t.count).sum();
            println!("templates={} lines_covered={}", catalog.load_templates()?.len(), total);
            for t in templates {
                println!("  id={:<5} count={:<9} {}", t.id, t.count, t.text());
            }
        }

        Command::Bench { lines } => {
            // Target from docs/tasks/current.md: >=500k lines/s/core. Below
            // that, templating becomes the bottleneck and the fallback
            // (sorted+zstd, no templates) is the right configuration.
            let services = ["api", "worker", "auth"];
            let corpus: Vec<String> = (0..lines)
                .map(|i| {
                    let s = services[(i % 3) as usize];
                    match i % 5 {
                        0 => format!("[{s}] handled request id={i} latency_ms={}", i % 900),
                        1 => format!("[{s}] cache miss for key user:{i}"),
                        2 => format!("[{s}] connection to 10.0.0.{} established", i % 255),
                        3 => format!("[{s}] retry {} of 5 after timeout", i % 5),
                        _ => format!("[{s}] wrote {} bytes to /var/data/file{i}.bin", i * 7),
                    }
                })
                .collect();

            let mut drain = Drain::default();
            let start = std::time::Instant::now();
            for line in &corpus {
                drain.add_line(line, 0);
            }
            let elapsed = start.elapsed();
            let rate = lines as f64 / elapsed.as_secs_f64();
            println!(
                "lines={} elapsed={:.2}s rate={:.0} lines/s/core templates={} untemplated={}",
                lines,
                elapsed.as_secs_f64(),
                rate,
                drain.template_count(),
                drain.untemplated
            );
            println!(
                "target 500000 lines/s/core: {}",
                if rate >= 500_000.0 { "MET" } else { "MISSED" }
            );
        }

        Command::Buckets { config } => {
            let config = load(&config)?;
            for b in config.retention.iter() {
                println!(
                    "{:<12} severity>={:<3} retention={:?}",
                    b.name, b.min_severity, b.retention
                );
            }
        }
    }
    Ok(())
}

fn load(path: &Path) -> Result<Config, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(path)?;
    let config: Config = toml::from_str(&text)?;
    config.validate()?;
    Ok(config)
}

fn catalog_path(config: &Config) -> PathBuf {
    config.storage.data_dir.join("catalog.sqlite")
}

fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}{}", UNITS[0])
    } else {
        format!("{v:.1}{}", UNITS[unit])
    }
}

/// stdin → bounded queue → writer thread → WAL, with background maintenance.
fn run(
    config: &Config,
    max_records: u64,
    maintenance: bool,
    pushdown_out: Option<PathBuf>,
    tail_paths: Vec<PathBuf>,
    tail_from_end: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Templating happens at ingest, not at merge: pushdown needs a stable
    // template id to hash flows and to rate-limit by error shape, and by merge
    // time the error has long since been forwarded. The maintenance thread
    // shares the miner so the dictionary is persisted and restored.
    let drain = {
        let catalog = Catalog::open(&catalog_path(config))?;
        let restored = catalog.load_templates().unwrap_or_default();
        Arc::new(Mutex::new(Drain::restore(DrainConfig::default(), restored)))
    };
    let mut ring = Ring::new(RingConfig::from_pushdown(&config.pushdown));
    let mut raw_bytes_seen = 0u64;

    // Forwarding runs on its own thread behind a bounded queue. On the ingest
    // thread, a vendor that is slow, rate-limiting or down would become
    // backpressure on the application — which is the one thing this agent
    // promises never to do. When the queue is full we drop the outbound event
    // and count it, exactly as the ingest queue sheds.
    let (outbound_tx, outbound_rx) = crossbeam_channel::bounded::<Outbound>(4096);
    let dispatch_stats = Arc::new(Mutex::new(ForwardSummary::default()));
    let forwarder_thread = if config.destinations.is_empty() {
        None
    } else {
        let destinations = config.destinations.clone();
        let summary = Arc::clone(&dispatch_stats);
        // Curated events are spooled before they are dispatched, so a restart
        // resumes where delivery stopped instead of re-sending everything or
        // dropping what was in flight. The cursor lives in its own file, not
        // the catalog: it changes far more often than catalog rows, and a
        // vendor outage should not become SQLite write traffic.
        let spool_dir = config.storage.data_dir.join("forward-spool");
        let mut spool = match Spool::open(&spool_dir, SpoolConfig::default()) {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::error!(error = %e, "forward spool unavailable; delivery is memory-only");
                None
            }
        };
        Some(
            std::thread::Builder::new()
                .name("logless-forward".into())
                .spawn(move || {
                    let forwarders = destinations
                        .into_iter()
                        .map(|d| Forwarder::new(d, Box::new(HttpTransport::default())))
                        .collect();
                    let mut dispatch = Dispatch::new(forwarders, AGGREGATE_FLUSH_INTERVAL);
                    // Anything left from a previous run goes first, in order.
                    if let Some(spool) = spool.as_mut() {
                        replay_spool(spool, &mut dispatch);
                    }
                    loop {
                        match outbound_rx.recv_timeout(Duration::from_millis(200)) {
                            Ok(item) => {
                                if let Some(spool) = spool.as_mut() {
                                    if let Ok(bytes) = postcard::to_stdvec(&item) {
                                        let _ = spool.push(&bytes);
                                    }
                                }
                                dispatch.handle(&item);
                                // Committed after dispatch: a crash in between
                                // re-sends, and re-sending is recoverable in a
                                // way that silently dropping is not.
                                if let Some(spool) = spool.as_mut() {
                                    if let Ok(batch) = spool.peek(256) {
                                        if let Some((cursor, _)) = batch.last() {
                                            let _ = spool.commit(*cursor);
                                        }
                                    }
                                }
                            }
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                        }
                        dispatch.flush_if_due(false);
                    }
                    // Shutdown: a collapsed storm must not vanish with the process.
                    dispatch.flush_if_due(true);
                    if let Ok(mut s) = summary.lock() {
                        s.per_destination = dispatch
                            .forwarders()
                            .iter()
                            .map(|f| (f.destination().name().to_string(), f.stats))
                            .collect();
                        s.dispatch = dispatch.stats;
                    }
                })?,
        )
    };
    let mut outbound_dropped = 0u64;
    let mut pushdown_sink = match (&pushdown_out, config.pushdown.enabled) {
        (Some(path), true) => Some(std::io::BufWriter::new(std::fs::File::create(path)?)),
        _ => None,
    };

    let (queue, consumer) = queue::channel(
        config.ingest.queue_capacity,
        config.ingest.critical_enqueue_timeout,
    );
    // Shared with the HEC receiver threads, which check the depth before
    // admitting a batch they will have to acknowledge.
    let queue = Arc::new(queue);
    let stats = queue.stats();

    // Published by the WAL writer after each fdatasync. A HEC ack is judged
    // against this number and nothing else.
    let synced_records = Arc::new(AtomicU64::new(0));
    let acks = hec::AckTable::new(Arc::clone(&synced_records));
    let stop = Arc::new(AtomicBool::new(false));

    // Graceful shutdown. The deployment target is a systemd unit, and systemd
    // stops services with SIGTERM: without this the agent dies mid-flight and
    // loses the unsynced WAL tail, the un-flushed aggregate counters and an
    // up-to-date tail checkpoint — on every deploy. Ctrl-C is the same path.
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        if let Err(e) = signal_hook::flag::register(signal, Arc::clone(&stop)) {
            tracing::warn!(error = %e, signal, "could not install signal handler");
        }
    }

    // Published by the writer so maintenance never merges the live segment.
    let active_segment = Arc::new(AtomicU64::new(0));
    // Published by maintenance so ingest can stop admitting DEBUG under
    // critical disk pressure. An integer rather than a lock: it is read on
    // every record.
    let pressure = Arc::new(AtomicU8::new(0));

    let writer = {
        let stop = Arc::clone(&stop);
        let active = Arc::clone(&active_segment);
        let synced = Arc::clone(&synced_records);
        let wal_dir = config.wal_dir();
        let (segment_bytes, fsync_interval, fsync_bytes) = (
            config.storage.wal_segment_bytes,
            config.storage.wal_fsync_interval,
            config.storage.wal_fsync_bytes,
        );
        std::thread::Builder::new()
            .name("logless-wal".into())
            .spawn(move || -> Result<u64, wal::WalError> {
                let mut w =
                    wal::WalWriter::open(&wal_dir, segment_bytes, fsync_interval, fsync_bytes)?;
                active.store(w.current_segment_id(), Ordering::Relaxed);
                loop {
                    let batch = consumer.next_batch_lingering(
                        4096,
                        Duration::from_millis(20),
                        WAL_BATCH_LINGER,
                    );
                    if !batch.is_empty() {
                        w.append(&batch)?;
                        active.store(w.current_segment_id(), Ordering::Relaxed);
                    }
                    // Publish only after a sync actually happened: this is the
                    // number a HEC ack is judged against, so it must mean
                    // "on disk", not "appended".
                    if w.maybe_sync()? {
                        synced.store(w.records_written, Ordering::Release);
                    }
                    if stop.load(Ordering::Relaxed) && batch.is_empty() {
                        let tail = consumer.drain();
                        if !tail.is_empty() {
                            w.append(&tail)?;
                        }
                        w.sync()?;
                        synced.store(w.records_written, Ordering::Release);
                        return Ok(w.records_written);
                    }
                }
            })?
    };

    // One variable with one meaning, not two flags: the maintainer must both
    // outlive the writer (so its final pass can merge the last segment) and
    // know that the writer is gone. As two Relaxed atomics it could observe
    // "stop" without yet observing "writer finished", and skip the last
    // segment. Release/Acquire on a single phase removes the question.
    // 0 = running, 1 = writer finished, do a final pass and exit.
    const PHASE_RUNNING: u8 = 0;
    const PHASE_WRITER_DONE: u8 = 1;
    let phase = Arc::new(AtomicU8::new(PHASE_RUNNING));

    let maintainer = if maintenance {
        let phase = Arc::clone(&phase);
        let active = Arc::clone(&active_segment);
        let pressure = Arc::clone(&pressure);
        let drain = Arc::clone(&drain);
        let config = config.clone();
        Some(
            std::thread::Builder::new()
                .name("logless-maint".into())
                .spawn(move || {
                    let mut catalog = match Catalog::open(&catalog_path(&config)) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!(error = %e, "catalog unavailable; maintenance disabled");
                            return;
                        }
                    };

                    loop {
                        let writer_done = phase.load(Ordering::Acquire) == PHASE_WRITER_DONE;
                        maintenance_pass(
                            &config,
                            &mut catalog,
                            &drain,
                            &active,
                            &pressure,
                            writer_done,
                        );
                        if writer_done {
                            return;
                        }
                        // Sleep in slices so shutdown does not wait a full interval.
                        let mut slept = Duration::ZERO;
                        while slept < MAINTENANCE_INTERVAL
                            && phase.load(Ordering::Acquire) == PHASE_RUNNING
                        {
                            std::thread::sleep(Duration::from_millis(100));
                            slept += Duration::from_millis(100);
                        }
                    }
                })?,
        )
    } else {
        None
    };

    // OTLP records cross into the main loop through this channel. The receiver
    // answers 503 when it is full, which is the correct answer to an exporter
    // that is outrunning us: the batch stays on the sender and is retried,
    // rather than being shed here. `queue.submit` still sheds by severity
    // downstream, because a file tailer has no such channel.
    let otlp_any = config.otlp.enabled || config.otlp.grpc.enabled;
    let (otlp_tx, otlp_rx) = if otlp_any {
        let (tx, rx) = crossbeam_channel::bounded::<Vec<LogRecord>>(OTLP_PENDING_BATCHES);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let (hec_tx, hec_rx) = if config.hec.enabled {
        let (tx, rx) = crossbeam_channel::bounded::<hec::Batch>(HEC_PENDING_BATCHES);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let mut hec_receiver = match (&hec_tx, config.hec.enabled) {
        (Some(tx), true) => {
            let tx = tx.clone();
            let queue_capacity = config.ingest.queue_capacity;
            let queue_for_room = Arc::clone(&queue);
            Some(hec::Receiver::start(&config.hec, Arc::clone(&acks), move |batch| {
                // Refuse rather than shed. An acked batch that later loses its
                // DEBUG records to severity shedding would be acked as durable
                // while part of it was dropped, so the batch is only admitted
                // when the ingest queue has room for all of it.
                let room = queue_capacity.saturating_sub(queue_for_room.len());
                if room < batch.records.len() {
                    return Admitted::Busy;
                }
                match tx.try_send(batch) {
                    Ok(()) => Admitted::Accepted,
                    Err(_) => Admitted::Busy,
                }
            })?)
        }
        _ => None,
    };
    drop(hec_tx);
    let mut hec_summary: Option<hec::StatsSnapshot> = None;

    // The Sentry proxy has its own channel: unlike the log receivers it carries
    // whole envelopes, and the records it produces are a *copy* for the local
    // store — the bytes forwarded upstream are the SDK's own, untouched.
    let (sentry_tx, sentry_rx) = if config.sentry.enabled {
        let (tx, rx) = crossbeam_channel::bounded::<Vec<LogRecord>>(SENTRY_PENDING_BATCHES);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let mut sentry_receiver = match (&sentry_tx, config.sentry.enabled) {
        (Some(tx), true) => {
            let tx = tx.clone();
            // No upstream configured means a local-only recorder: it stores
            // everything and forwards nothing, which is how you measure what
            // Sentry would have cost before changing what it receives.
            let upstream = config
                .sentry
                .upstream_dsn
                .as_ref()
                .map(|_| sentry::Upstream::new(Box::new(HttpTransport::default())));
            let sentry_spool = config.storage.data_dir.join("sentry-spool");
            Some(sentry::Receiver::start(
                &config.sentry,
                upstream,
                Some(sentry_spool.as_path()),
                move |records| {
                    match tx.try_send(records) {
                        Ok(()) => sentry::Admitted::Accepted,
                        Err(_) => sentry::Admitted::Busy,
                    }
                },
            )?)
        }
        _ => None,
    };
    drop(sentry_tx);
    let mut sentry_summary: Option<sentry::StatsSnapshot> = None;
    let mut sentry_upstream_summary: Option<sentry::UpstreamSnapshot> = None;

    let mut otlp_receiver = match (&otlp_tx, config.otlp.enabled) {
        (Some(tx), true) => {
            let tx = tx.clone();
            Some(otlp::Receiver::start(&config.otlp, move |records| {
                match tx.try_send(records) {
                    Ok(()) => Accepted::All,
                    Err(_) => Accepted::Rejected,
                }
            })?)
        }
        _ => None,
    };
    // Both OTLP transports feed the same channel: the record path must not care
    // which wire a batch arrived on, or the same error would be templated and
    // deduped differently depending on how the fleet was configured.
    let mut otlp_grpc_receiver = match (&otlp_tx, config.otlp.grpc.enabled) {
        (Some(tx), true) => {
            let tx = tx.clone();
            Some(otlp::GrpcReceiver::start(&config.otlp.grpc, move |records| {
                match tx.try_send(records) {
                    Ok(()) => Accepted::All,
                    Err(_) => Accepted::Rejected,
                }
            })?)
        }
        _ => None,
    };
    drop(otlp_tx);
    let mut otlp_grpc_summary: Option<otlp::http::StatsSnapshot> = None;
    let mut otlp_summary: Option<otlp::http::StatsSnapshot> = None;

    let mut count = 0u64;
    let mut refused_debug = 0u64;
    let mut windows = 0u64;
    let mut errors_forwarded = 0u64;

    // One record path, three possible sources (stdin, tailed files, OTLP). The
    // sources differ only in how a record is produced; everything after — the
    // template id, the pushdown ring, the queue — must be identical, or an
    // error arriving over OTLP would be forwarded differently from the same
    // error read out of a file. `process` returns false when the cap is hit.
    let process = |mut record: LogRecord,
                       raw_len: u64,
                       raw_bytes_seen: &mut u64,
                       count: &mut u64,
                       refused_debug: &mut u64,
                       windows: &mut u64,
                       errors_forwarded: &mut u64,
                       outbound_dropped: &mut u64,
                       ring: &mut Ring,
                       pushdown_sink: &mut Option<std::io::BufWriter<std::fs::File>>|
     -> bool {
        *raw_bytes_seen += raw_len;

        // Assign the template id here so it reaches both the WAL and pushdown.
        // The template *text* is carried alongside for aggregate events, whose
        // job is to show the shape rather than one arbitrary example of it.
        let mut template_text = String::new();
        if let Ok(mut d) = drain.lock() {
            if let Some(m) = d.add_line(&record.body, now_unix_secs()) {
                record.template_id = Some(m.template_id);
                if let Some(t) = d.template(m.template_id) {
                    template_text = t.text();
                }
            }
        }

        if config.pushdown.enabled {
            ring.observe(&record);
            if record.severity.0 >= config.pushdown.trigger_severity {
                // Every error is forwarded. Only the context is economised on.
                *errors_forwarded += 1;
                let (json, outbound) = match ring.capture(&record) {
                    Capture::Window(w) => {
                        *windows += 1;
                        let json = window_json(&w);
                        (
                            json,
                            Outbound {
                                window: *w,
                                had_context: true,
                                reason: None,
                                template_text: template_text.clone(),
                            },
                        )
                    }
                    Capture::ContextSuppressed { reason, flow_hash } => (
                        error_only_json(&record, reason, flow_hash),
                        Outbound {
                            window: bare_window(&record, flow_hash),
                            had_context: false,
                            reason: Some(reason),
                            template_text: template_text.clone(),
                        },
                    ),
                };
                // Never block on a slow vendor.
                if forwarder_thread.is_some() && outbound_tx.try_send(outbound).is_err() {
                    *outbound_dropped += 1;
                }
                if let Some(sink) = pushdown_sink.as_mut() {
                    let _ = writeln!(sink, "{json}");
                }
            }
        }

        // Critical pressure: stop admitting DEBUG at the door, so inflow drops
        // rather than only the backlog being deleted.
        if pressure.load(Ordering::Relaxed) == 2 && record.level_class() == LevelClass::Debug {
            *refused_debug += 1;
        } else {
            queue.submit(record);
        }
        *count += 1;
        !(max_records > 0 && *count >= max_records)
    };

    if tail_paths.is_empty() && otlp_rx.is_none() && hec_rx.is_none() && sentry_rx.is_none() {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let line = line?;
            if line.is_empty() {
                continue;
            }
            if !process(
                parse_line(&line),
                line.len() as u64 + 1,
                &mut raw_bytes_seen,
                &mut count,
                &mut refused_debug,
                &mut windows,
                &mut errors_forwarded,
                &mut outbound_dropped,
                &mut ring,
                &mut pushdown_sink,
            ) {
                break;
            }
        }
    } else {
        // Poll mode: runs until interrupted, or until the record cap is hit.
        // Both live receivers are drained here rather than on their own
        // threads, so the template miner and the pushdown ring stay
        // single-threaded and a file error and an OTLP error of the same shape
        // share one dedupe window.
        let checkpoint = config.storage.data_dir.join("tail.ckpt");
        let mut tailer = (!tail_paths.is_empty())
            .then(|| Tailer::new(tail_paths.clone(), tail_from_end).with_checkpoint(checkpoint));
        if otlp_receiver.is_some() {
            tracing::info!(addr = %config.otlp.addr, "otlp/http receiver listening");
        }
        if otlp_grpc_receiver.is_some() {
            tracing::info!(addr = %config.otlp.grpc.addr, "otlp/grpc receiver listening");
        }
        if sentry_receiver.is_some() {
            tracing::info!(
                addr = %config.sentry.addr,
                mode = ?config.sentry.upstream_auth,
                upstream = config.sentry.upstream_dsn.is_some(),
                "sentry proxy listening"
            );
        }
        if !tail_paths.is_empty() {
            tracing::info!(files = tail_paths.len(), "tailing");
        }
        // Network receivers must be able to wake the loop. Sleeping a fixed
        // interval when idle is right for a file — nothing is waiting on the
        // other end — but an exporter posting into a bounded channel fills it
        // during the sleep and gets a 503 while the agent has nothing to do.
        // (Observed: a third-party HEC client died on exactly one spurious 503
        // after 311 events.) `Select` wakes on either channel immediately and
        // still falls back to the poll interval for the tailer.
        let mut selector = crossbeam_channel::Select::new();
        let mut wired = false;
        if let Some(rx) = &otlp_rx {
            selector.recv(rx);
            wired = true;
        }
        if let Some(rx) = &hec_rx {
            selector.recv(rx);
            wired = true;
        }
        if let Some(rx) = &sentry_rx {
            selector.recv(rx);
            wired = true;
        }

        let mut done = false;
        while !done {
            let mut idle = true;

            let mut batch: Vec<String> = Vec::new();
            if let Some(t) = tailer.as_mut() {
                t.poll(|_, line| batch.push(line.to_string()));
            }
            for line in &batch {
                idle = false;
                if !process(
                    parse_line(line),
                    line.len() as u64 + 1,
                    &mut raw_bytes_seen,
                    &mut count,
                    &mut refused_debug,
                    &mut windows,
                    &mut errors_forwarded,
                    &mut outbound_dropped,
                    &mut ring,
                    &mut pushdown_sink,
                ) {
                    done = true;
                    break;
                }
            }

            if let Some(rx) = &otlp_rx {
                // Bounded per pass so a busy exporter cannot starve the tailer.
                for _ in 0..OTLP_BATCHES_PER_PASS {
                    let Ok(records) = rx.try_recv() else { break };
                    idle = false;
                    for record in records {
                        // Charge the decoded body against the "raw bytes seen"
                        // baseline: the saving is measured against what the
                        // producer emitted, not against protobuf framing.
                        let raw_len = record.body.len() as u64 + 1;
                        if !process(
                            record,
                            raw_len,
                            &mut raw_bytes_seen,
                            &mut count,
                            &mut refused_debug,
                            &mut windows,
                            &mut errors_forwarded,
                            &mut outbound_dropped,
                            &mut ring,
                            &mut pushdown_sink,
                        ) {
                            done = true;
                            break;
                        }
                    }
                    if done {
                        break;
                    }
                }
            }

            if let Some(rx) = &hec_rx {
                for _ in 0..HEC_BATCHES_PER_PASS {
                    let Ok(batch) = rx.try_recv() else { break };
                    idle = false;
                    for record in batch.records {
                        let raw_len = record.body.len() as u64 + 1;
                        if !process(
                            record,
                            raw_len,
                            &mut raw_bytes_seen,
                            &mut count,
                            &mut refused_debug,
                            &mut windows,
                            &mut errors_forwarded,
                            &mut outbound_dropped,
                            &mut ring,
                            &mut pushdown_sink,
                        ) {
                            done = true;
                            break;
                        }
                    }
                    // Bind the batch to the WAL record count that must be
                    // synced before its ack may flip. `enqueued` and not
                    // `received`: shed records never reach the WAL, so counting
                    // them would ack a batch one fdatasync too early.
                    acks.bind(batch.seq, stats.snapshot().enqueued);
                    if done {
                        break;
                    }
                }
            }

            if let Some(rx) = &sentry_rx {
                for _ in 0..SENTRY_BATCHES_PER_PASS {
                    let Ok(records) = rx.try_recv() else { break };
                    idle = false;
                    for record in records {
                        let raw_len = record.body.len() as u64 + 1;
                        if !process(
                            record,
                            raw_len,
                            &mut raw_bytes_seen,
                            &mut count,
                            &mut refused_debug,
                            &mut windows,
                            &mut errors_forwarded,
                            &mut outbound_dropped,
                            &mut ring,
                            &mut pushdown_sink,
                        ) {
                            done = true;
                            break;
                        }
                    }
                    if done {
                        break;
                    }
                }
            }

            // Check for shutdown every pass, not only when idle: a busy source
            // would otherwise keep the agent alive indefinitely through a
            // SIGTERM, and systemd would escalate to SIGKILL.
            if stop.load(Ordering::Relaxed) {
                tracing::info!("shutdown signal received; draining");
                done = true;
            } else if idle {
                if wired {
                    // Returns as soon as a receiver has something, or after the
                    // poll interval so a tailed file is still checked on time.
                    let _ = selector.ready_timeout(TAIL_POLL_INTERVAL);
                } else {
                    std::thread::sleep(TAIL_POLL_INTERVAL);
                }
            }
        }
        if let Some(t) = &tailer {
            tracing::info!(
                lines = t.stats.lines_read,
                rotations = t.stats.rotations,
                truncations = t.stats.truncations,
                "tail finished"
            );
        }
    }

    // Stop accepting before the WAL writer is told to finish, so nothing is
    // admitted after the queue is closed. In-flight requests complete first.
    if let Some(r) = sentry_receiver.take() {
        let upstream = r.upstream_stats();
        let s = r.shutdown();
        tracing::info!(
            requests = s.requests,
            envelopes = s.envelopes,
            items_received = s.items_received,
            items_forwarded = s.items_forwarded,
            held_locally = s.items_held_locally,
            "sentry proxy stopped"
        );
        sentry_summary = Some(s);
        sentry_upstream_summary = Some(upstream);
    }
    if let Some(r) = hec_receiver.take() {
        let s = r.shutdown();
        tracing::info!(
            requests = s.requests,
            records = s.records,
            busy_503 = s.busy,
            unauthorized = s.unauthorized,
            bad_request = s.bad_request,
            ack_queries = s.ack_queries,
            "hec receiver stopped"
        );
        hec_summary = Some(s);
    }
    if let Some(r) = otlp_grpc_receiver.take() {
        let s = r.shutdown();
        tracing::info!(
            requests = s.requests,
            records = s.records,
            unavailable = s.rejected,
            bad_request = s.bad_request,
            too_large = s.too_large,
            "otlp/grpc receiver stopped"
        );
        otlp_grpc_summary = Some(s);
    }
    if let Some(r) = otlp_receiver.take() {
        let s = r.shutdown();
        tracing::info!(
            requests = s.requests,
            records = s.records,
            rejected_503 = s.rejected,
            bad_request = s.bad_request,
            too_large = s.too_large,
            dropped_upstream = s.dropped_upstream,
            "otlp receiver stopped"
        );
        otlp_summary = Some(s);
    }

    stop.store(true, Ordering::Relaxed);
    drop(queue);
    let written = writer.join().map_err(|_| "wal writer panicked")??;
    // Only now is the last segment safe to merge. Release pairs with the
    // maintainer's Acquire so it cannot see the phase change without also
    // seeing everything the writer did.
    phase.store(PHASE_WRITER_DONE, Ordering::Release);
    if let Some(m) = maintainer {
        let _ = m.join();
    }

    // Closing the channel tells the forwarder to drain, flush rollups and exit.
    drop(outbound_tx);
    if let Some(t) = forwarder_thread {
        let _ = t.join();
    }

    if let Some(mut sink) = pushdown_sink {
        sink.flush()?;
    }

    let s = stats.snapshot();
    // Persisted before anything is printed: the audit is only meaningful if
    // this survives every exit path that reached here.
    let lifetime = logless_core::counters::Counters::load(&config.storage.data_dir).add_session(
        s.received,
        s.enqueued,
        s.dropped() + refused_debug,
    );
    if let Err(e) = lifetime.save(&config.storage.data_dir) {
        tracing::warn!(error = %e, "could not persist ingest counters");
    }

    let mut out = std::io::stdout().lock();
    writeln!(
        out,
        "received={} enqueued={} written={} dropped_debug={} dropped_info={} dropped_critical={} refused_debug_pressure={}",
        s.received, s.enqueued, written, s.dropped_debug, s.dropped_info, s.dropped_critical, refused_debug
    )?;
    if let (Some(s), Some(u)) = (sentry_summary, sentry_upstream_summary) {
        writeln!(
            out,
            "sentry: envelopes={} items_received={} items_forwarded={} held_locally={} sampled_out={} bytes_received={}",
            s.envelopes,
            s.items_received,
            s.items_forwarded,
            s.items_held_locally,
            s.transactions_sampled_out,
            s.bytes_received
        )?;
        writeln!(
            out,
            "sentry upstream: envelopes_sent={} items_sent={} bytes_sent={} rate_limited_items={} failures={} retries={} | {:.2}% of received bytes forwarded",
            u.envelopes_sent,
            u.items_sent,
            u.bytes_sent,
            u.items_rate_limited,
            u.failures,
            u.retries,
            if s.bytes_received == 0 { 0.0 } else { 100.0 * u.bytes_sent as f64 / s.bytes_received as f64 }
        )?;
        if !s.accounts_for_everything() {
            writeln!(out, "ACCOUNTING VIOLATION (sentry): {s:?}")?;
            std::process::exit(2);
        }
    }
    if let Some(h) = hec_summary {
        writeln!(
            out,
            "hec: requests={} records={} busy_503={} unauthorized={} bad_request={} too_large={} ack_queries={}",
            h.requests, h.records, h.busy, h.unauthorized, h.bad_request, h.too_large, h.ack_queries
        )?;
        if !h.accounts_for_everything() {
            writeln!(out, "ACCOUNTING VIOLATION (hec): {h:?}")?;
            std::process::exit(2);
        }
    }
    if let Some(g) = otlp_grpc_summary {
        writeln!(
            out,
            "otlp/grpc: requests={} records={} unavailable={} bad_request={} too_large={} dropped_upstream={}",
            g.requests, g.records, g.rejected, g.bad_request, g.too_large, g.dropped_upstream
        )?;
        if !g.accounts_for_everything() {
            writeln!(out, "ACCOUNTING VIOLATION (otlp/grpc): {g:?}")?;
            std::process::exit(2);
        }
    }
    if let Some(o) = otlp_summary {
        writeln!(
            out,
            "otlp: requests={} records={} rejected_503={} bad_request={} too_large={} dropped_upstream={}",
            o.requests, o.records, o.rejected, o.bad_request, o.too_large, o.dropped_upstream
        )?;
        if !o.accounts_for_everything() {
            writeln!(out, "ACCOUNTING VIOLATION (otlp): {o:?}")?;
            std::process::exit(2);
        }
    }
    if config.pushdown.enabled {
        writeln!(
            out,
            "pushdown: errors_forwarded={} with_context={} deduped={} rate_limited={} context_lines_held={} ring_bytes={}",
            errors_forwarded,
            windows,
            ring.stats.windows_deduped,
            ring.stats.windows_rate_limited,
            ring.stats.lines_buffered - ring.stats.lines_evicted,
            ring.bytes_held()
        )?;
    }
    if let Ok(summary) = dispatch_stats.lock() {
        for (name, st) in &summary.per_destination {
            let avoided = raw_bytes_seen.saturating_sub(st.bytes_sent);
            writeln!(
                out,
                "forward[{}]: events={} aggregates={} bytes_sent={} failures={} retries={} | ingested={} avoided={} ({:.2}% of raw forwarded)",
                name,
                st.events_sent,
                st.aggregates_sent,
                st.bytes_sent,
                st.failures,
                st.retries,
                raw_bytes_seen,
                avoided,
                if raw_bytes_seen == 0 { 0.0 } else { 100.0 * st.bytes_sent as f64 / raw_bytes_seen as f64 }
            )?;
        }
        if outbound_dropped > 0 || summary.dispatch.dropped_queue_full > 0 {
            writeln!(
                out,
                "forward: DROPPED {} outbound events because the vendor could not keep up",
                outbound_dropped + summary.dispatch.dropped_queue_full
            )?;
        }
    }
    if !s.accounts_for_everything() {
        // The invariant is the product. Failing it is an exit code, not a log line.
        writeln!(out, "ACCOUNTING VIOLATION: {s:?}")?;
        std::process::exit(2);
    }
    Ok(())
}

/// Re-dispatches whatever a previous run spooled but never delivered.
fn replay_spool(spool: &mut Spool, dispatch: &mut Dispatch) {
    let Ok(pending) = spool.peek(4096) else { return };
    if pending.is_empty() {
        return;
    }
    tracing::info!(count = pending.len(), "replaying undelivered events from the spool");
    let mut last = None;
    for (cursor, bytes) in pending {
        match postcard::from_bytes::<Outbound>(&bytes) {
            Ok(item) => dispatch.handle(&item),
            // A record this build cannot read must not wedge the queue behind
            // it forever; skipping is the lesser loss.
            Err(e) => tracing::warn!(error = %e, "skipping an undecodable spool record"),
        }
        last = Some(cursor);
    }
    if let Some(cursor) = last {
        let _ = spool.commit(cursor);
    }
}

fn replay(
    config: &Config,
    query: &logless_core::scan::Query,
    forward: bool,
    explain: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let store_dir = config.store_dir();
    let mut out = std::io::stdout().lock();

    if explain {
        let files = logless_core::scan::plan(&store_dir, query)?;
        writeln!(out, "would open {} of {} files:", files.len(),
            logless_core::store::list_parquet_files(&store_dir).len())?;
        for file in files {
            writeln!(out, "  {}", file.display())?;
        }
        return Ok(());
    }

    let result = logless_core::scan::scan(&store_dir, query)?;
    writeln!(
        out,
        "matched {} records (scanned {} rows in {} files; {} files and {} row groups pruned){}",
        result.stats.rows_matched,
        result.stats.rows_scanned,
        result.stats.files_considered - result.stats.files_pruned_by_partition,
        result.stats.files_pruned_by_partition,
        result.stats.row_groups_pruned,
        if result.truncated { " — TRUNCATED at the limit" } else { "" }
    )?;

    if !forward {
        for record in result.records.iter().take(20) {
            writeln!(
                out,
                "  {} {:<5} {} {}",
                record.observed_unix_nano,
                record.severity.0,
                record.service.as_deref().unwrap_or("-"),
                record.body
            )?;
        }
        if result.records.len() > 20 {
            writeln!(out, "  … {} more (use --forward to send them)", result.records.len() - 20)?;
        }
        return Ok(());
    }

    if config.destinations.is_empty() {
        writeln!(out, "no destinations configured; nothing to forward to")?;
        return Ok(());
    }

    // Replayed records go through the same shaping as live ones, so an event
    // sent now is indistinguishable upstream from one sent at the time — that
    // is the whole promise of the undo button.
    let forwarders = config
        .destinations
        .iter()
        .cloned()
        .map(|d| Forwarder::new(d, Box::new(HttpTransport::default())))
        .collect();
    let mut dispatch = Dispatch::new(forwarders, AGGREGATE_FLUSH_INTERVAL);
    for record in &result.records {
        dispatch.handle(&Outbound {
            window: bare_window(record, None),
            had_context: false,
            reason: None,
            template_text: record.body.clone(),
        });
    }
    dispatch.flush_if_due(true);
    for forwarder in dispatch.forwarders() {
        let stats = forwarder.stats;
        writeln!(
            out,
            "replayed to {}: events={} aggregates={} bytes={} failures={}",
            forwarder.destination().name(),
            stats.events_sent,
            stats.aggregates_sent,
            stats.bytes_sent,
            stats.failures
        )?;
    }
    Ok(())
}

fn maintenance_pass(
    config: &Config,
    catalog: &mut Catalog,
    drain: &Arc<Mutex<Drain>>,
    active_segment: &AtomicU64,
    pressure: &AtomicU8,
    writer_done: bool,
) {
    // 0 means the writer has not published a segment id yet. Merging with
    // `None` here would treat every segment as inactive — including the one the
    // writer is about to append to — so skip merging entirely until it reports.
    // Once the writer has finished, nothing is active and everything can merge.
    let active = match (writer_done, active_segment.load(Ordering::Relaxed)) {
        (true, _) => None,
        (false, 0) => return,
        (false, id) => Some(id),
    };

    let merged = {
        let mut drain = match drain.lock() {
            Ok(d) => d,
            Err(poisoned) => poisoned.into_inner(),
        };
        merge::merge_all(
            &config.wal_dir(),
            &config.store_dir(),
            catalog,
            &config.retention,
            &mut drain,
            active,
            now_unix_secs(),
        )
    };
    // Compaction runs after the merge, so the files it just wrote are
    // candidates, and never for the hour still being written to.
    let active_hour = (!writer_done).then(|| now_unix_nanos() / (3_600 * 1_000_000_000));
    match logless_core::compact::compact(
        &config.store_dir(),
        catalog,
        &logless_core::compact::CompactConfig::default(),
        active_hour,
    ) {
        Ok(r) if r.partitions_compacted > 0 => tracing::info!(
            partitions = r.partitions_compacted,
            files_replaced = r.files_replaced,
            rows = r.rows,
            saved_pct = format!("{:.0}", r.saved_fraction() * 100.0),
            "compacted small files"
        ),
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "compaction failed"),
    }

    match merged {
        Ok(r) if r.segments_merged > 0 || r.segments_skipped > 0 => {
            tracing::info!(
                merged = r.segments_merged,
                records = r.records,
                files = r.files_written,
                wal_freed = r.wal_bytes_freed,
                templated = r.templated,
                new_templates = r.new_templates,
                "merged wal segments"
            );
        }
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "merge failed"),
    }

    let expiry = retention::enforce(
        &config.store_dir(),
        &config.retention,
        now_unix_secs(),
        false,
    );
    if !expiry.expired.is_empty() {
        tracing::info!(
            partitions = expiry.expired.len(),
            bytes = expiry.bytes_reclaimed,
            "retention expired partitions"
        );
        if let Err(e) = catalog.forget_missing_files() {
            tracing::error!(error = %e, "catalog cleanup after retention failed");
        }
    }

    let usage = DiskUsage::measure(
        &config.wal_dir(),
        catalog,
        config.storage.disk_budget_bytes,
    );
    let level = match usage.pressure() {
        Pressure::Normal => 0,
        Pressure::High => 1,
        Pressure::Critical => 2,
    };
    pressure.store(level, Ordering::Relaxed);

    if level > 0 {
        match budget::reclaim(&config.store_dir(), catalog, &config.retention, usage, false) {
            Ok(r) if !r.files_dropped.is_empty() => tracing::warn!(
                files = r.files_dropped.len(),
                partitions = r.partitions_touched.len(),
                bytes = r.bytes_reclaimed,
                rows = r.rows_lost,
                exhausted = r.exhausted,
                "disk pressure: dropped partitions ahead of retention"
            ),
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "reclaim failed"),
        }
    }
}

/// Serialise a context window as one JSON line.
///
/// Hand-rolled rather than serde-derived because this shape is the contract
/// with the forwarders and is easier to read and adjust in one place while it
/// is still settling.
fn window_json(w: &ContextWindow) -> String {
    let line_json = |l: &logless_core::ring::ContextLine| {
        format!(
            r#"{{"ts":{},"severity":{},"template_id":{},"body":{}}}"#,
            l.timestamp_unix_nano,
            l.severity,
            l.template_id.map(|t| t.to_string()).unwrap_or("null".into()),
            json_string(&l.body)
        )
    };
    let context: Vec<String> = w.context.iter().map(line_json).collect();
    format!(
        r#"{{"error":{},"context":[{}],"context_lines":{},"key_tier":"{}","flow_hash":"{:016x}","collapsed":{},"service":{}}}"#,
        line_json(&w.error),
        context.join(","),
        w.context.len(),
        w.key_tier.as_str(),
        w.flow_hash,
        w.suppressed,
        w.service.as_deref().map(json_string).unwrap_or("null".into())
    )
}

/// A window carrying only the error, for destinations that still want the event
/// when its context was suppressed.
fn bare_window(record: &LogRecord, flow_hash: Option<u64>) -> ContextWindow {
    ContextWindow {
        error: ContextLine {
            timestamp_unix_nano: record.observed_unix_nano,
            severity: record.severity.0,
            template_id: record.template_id,
            body: record.body.clone(),
        },
        context: Vec::new(),
        key_tier: KeyTier::Service,
        flow_hash: flow_hash.unwrap_or(0),
        error_template_id: record.template_id,
        service: record.service.clone(),
        suppressed: 0,
    }
}

/// An error whose context was withheld. The error still goes upstream; the
/// `context_suppressed` reason and `flow_hash` tell the consumer which
/// already-forwarded window it belongs to.
fn error_only_json(
    record: &LogRecord,
    reason: Suppressed,
    flow_hash: Option<u64>,
) -> String {
    let reason = match reason {
        Suppressed::DuplicateFlow => "duplicate_flow",
        Suppressed::RateLimited => "rate_limited",
        Suppressed::NoContext => "no_context",
    };
    format!(
        r#"{{"error":{{"ts":{},"severity":{},"template_id":{},"body":{}}},"context":[],"context_lines":0,"context_suppressed":"{}","flow_hash":{},"service":{}}}"#,
        record.observed_unix_nano,
        record.severity.0,
        record.template_id.map(|t| t.to_string()).unwrap_or("null".into()),
        json_string(&record.body),
        reason,
        flow_hash
            .map(|h| format!("\"{h:016x}\""))
            .unwrap_or("null".into()),
        record.service.as_deref().map(json_string).unwrap_or("null".into())
    )
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `<severity>\t<body>` if a leading severity is present, else the whole line
/// at INFO. Deliberately dumb — the real parsers arrive with the receivers.
fn parse_line(line: &str) -> LogRecord {
    let (severity, body) = match line.split_once('\t') {
        Some((head, rest)) => match head.trim().to_ascii_uppercase().as_str() {
            "TRACE" => (Severity::TRACE, rest),
            "DEBUG" => (Severity::DEBUG, rest),
            "INFO" => (Severity::INFO, rest),
            "WARN" | "WARNING" => (Severity::WARN, rest),
            "ERROR" => (Severity::ERROR, rest),
            "FATAL" | "CRITICAL" => (Severity::FATAL, rest),
            _ => (Severity::INFO, line),
        },
        None => (Severity::INFO, line),
    };
    let now = now_unix_nanos();
    let mut record = LogRecord::new(now, severity, body);
    record.observed_unix_nano = now;

    // Pick up logfmt-style correlation fields from the body. Real receivers
    // (OTLP, HEC) carry these as structured fields; for a stdin/file source
    // this is the only place they exist.
    for token in body.split_whitespace() {
        let Some((key, value)) = token.split_once('=') else {
            continue;
        };
        match key {
            "service" | "svc" => record.service = Some(value.trim_matches('"').to_string()),
            "trace_id" | "trace" => record.trace_id = Some(trace_bytes(value)),
            "request_id" | "req_id" | "session_id" => {
                record.attributes.push(logless_core::model::Attr {
                    key: "request_id".into(),
                    value: logless_core::model::AttrValue::Str(value.to_string()),
                })
            }
            _ => {}
        }
    }
    record
}

/// A 16-byte trace id from a token: parsed if it is 32 hex characters,
/// otherwise hashed. Hashing keeps correlation working for the many services
/// that emit short or non-hex ids, at the cost of not round-tripping the
/// original value.
fn trace_bytes(value: &str) -> [u8; 16] {
    if value.len() == 32 {
        if let Ok(parsed) = u128::from_str_radix(value, 16) {
            return parsed.to_be_bytes();
        }
    }
    use std::hash::{Hash, Hasher};
    let mut a = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut a);
    let hi = a.finish();
    let mut b = std::collections::hash_map::DefaultHasher::new();
    (value, hi).hash(&mut b);
    let lo = b.finish();
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&hi.to_be_bytes());
    out[8..].copy_from_slice(&lo.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_leading_severity() {
        assert_eq!(parse_line("ERROR\tboom").severity, Severity::ERROR);
        assert_eq!(parse_line("ERROR\tboom").body, "boom");
        assert_eq!(parse_line("warning\tcareful").severity, Severity::WARN);
    }

    #[test]
    fn unknown_prefixes_keep_the_whole_line() {
        let r = parse_line("weird\tstuff");
        assert_eq!(r.severity, Severity::INFO);
        assert_eq!(r.body, "weird\tstuff");
        assert_eq!(parse_line("no tabs here").body, "no tabs here");
    }

    #[test]
    fn picks_up_correlation_fields_from_the_body() {
        let r = parse_line("INFO\tservice=api trace_id=abc123 msg started");
        assert_eq!(r.service.as_deref(), Some("api"));
        assert!(r.trace_id.is_some());

        // The same trace token always yields the same id, or correlation breaks.
        let again = parse_line("ERROR\tservice=api trace_id=abc123 msg failed");
        assert_eq!(r.trace_id, again.trace_id);
        // Different traces must not collide.
        let other = parse_line("ERROR\tservice=api trace_id=abc124 msg failed");
        assert_ne!(r.trace_id, other.trace_id);
    }

    #[test]
    fn full_hex_trace_ids_round_trip_rather_than_hashing() {
        let hex = "0123456789abcdef0123456789abcdef";
        let r = parse_line(&format!("INFO\ttrace_id={hex} msg x"));
        assert_eq!(r.trace_id.unwrap(), u128::from_str_radix(hex, 16).unwrap().to_be_bytes());
    }

    #[test]
    fn human_sizes_read_sensibly() {
        assert_eq!(human(512), "512B");
        assert_eq!(human(2048), "2.0KiB");
        assert_eq!(human(5 * 1024 * 1024), "5.0MiB");
    }
}
