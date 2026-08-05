//! OTLP/HTTP logs receiver.
//!
//! `POST /v1/logs` with `Content-Type: application/x-protobuf` — the wire
//! contract that the OpenTelemetry Collector's `otlphttp` exporter and Vector's
//! `opentelemetry` sink speak by default. Pointing an existing fleet at
//! log-less becomes an endpoint change rather than a migration.
//!
//! The server itself is [`crate::httpd`]; this file is the protocol.
//!
//! # Backpressure
//!
//! This is the receiver with a real backpressure channel. Tailing a file cannot
//! ask the application to slow down, so [`crate::queue`] sheds by severity;
//! here, a full queue answers `503` with `Retry-After` and the exporter's own
//! retry queue holds the data. Shedding when the protocol offers a way to say
//! "not now" would throw away logs the sender was willing to keep.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::logs::{decode_request, Decoded};
use crate::httpd::{self, Handler, Incoming, Limits, Method, Reject, Reply};
use crate::model::LogRecord;

pub use crate::httpd::{DEFAULT_MAX_BODY_BYTES, DEFAULT_MAX_DECOMPRESSED_BYTES};

/// Seconds sent in `Retry-After` when the pipeline is full.
const RETRY_AFTER_SECS: u32 = 1;

const CONTENT_TYPE: &str = "application/x-protobuf";

/// gRPC status codes used in the `google.rpc.Status` bodies. OTLP/HTTP requires
/// that shape for non-2xx, and collectors surface the message in their own
/// logs, which is where an operator looks first.
mod rpc {
    pub const INVALID_ARGUMENT: u32 = 3;
    pub const RESOURCE_EXHAUSTED: u32 = 8;
    pub const UNAVAILABLE: u32 = 14;
    pub const UNIMPLEMENTED: u32 = 12;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiverConfig {
    /// Opt-in. A log agent that starts listening on a port after an upgrade is
    /// a surprise nobody wants to find in a security review.
    #[serde(default)]
    pub enabled: bool,
    /// Loopback by default: the common deployment is a collector on the same
    /// node. Binding 0.0.0.0 is a deliberate act, not a default.
    #[serde(default = "default_otlp_addr")]
    pub addr: String,
    /// Threads serving requests. Requests are short and CPU-light (decode plus
    /// a queue push), so this is about concurrent exporters, not throughput.
    #[serde(default = "default_otlp_workers")]
    pub workers: usize,
    #[serde(default = "default_max_body")]
    pub max_body_bytes: usize,
    #[serde(default = "default_max_decompressed")]
    pub max_decompressed_bytes: usize,
    /// The gRPC transport, on its own port. Declared last so TOML serialises it
    /// as a nested `[otlp.grpc]` table after this table's own scalars.
    #[serde(default)]
    pub grpc: super::grpc::GrpcConfig,
}

fn default_otlp_addr() -> String {
    "127.0.0.1:4318".to_string()
}
fn default_otlp_workers() -> usize {
    2
}
fn default_max_body() -> usize {
    DEFAULT_MAX_BODY_BYTES
}
fn default_max_decompressed() -> usize {
    DEFAULT_MAX_DECOMPRESSED_BYTES
}

impl Default for ReceiverConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: default_otlp_addr(),
            workers: default_otlp_workers(),
            max_body_bytes: default_max_body(),
            max_decompressed_bytes: default_max_decompressed(),
            grpc: super::grpc::GrpcConfig::default(),
        }
    }
}

/// What the pipeline did with a decoded batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accepted {
    /// Every record was queued.
    All,
    /// The pipeline is full. Answered with 503 so the exporter retries, which
    /// keeps the data on the sender rather than dropping it here.
    Rejected,
}

/// Counters exposed for `logless status`. Every request lands in exactly one of
/// `accepted + rejected + bad_request + too_large`, so a mismatch against the
/// exporter's own send count is attributable.
#[derive(Debug, Default)]
pub struct ReceiverStats {
    pub requests: AtomicU64,
    pub records: AtomicU64,
    pub accepted: AtomicU64,
    pub rejected: AtomicU64,
    pub bad_request: AtomicU64,
    pub too_large: AtomicU64,
    pub dropped_upstream: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub requests: u64,
    pub records: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub bad_request: u64,
    pub too_large: u64,
    pub dropped_upstream: u64,
}

impl ReceiverStats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            records: self.records.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            bad_request: self.bad_request.load(Ordering::Relaxed),
            too_large: self.too_large.load(Ordering::Relaxed),
            dropped_upstream: self.dropped_upstream.load(Ordering::Relaxed),
        }
    }
}

impl StatsSnapshot {
    /// Every request took exactly one exit.
    pub fn accounts_for_everything(&self) -> bool {
        self.requests == self.accepted + self.rejected + self.bad_request + self.too_large
    }
}

pub type ReceiverError = httpd::HttpError;

struct OtlpHandler {
    stats: Arc<ReceiverStats>,
    sink: Box<dyn Fn(Vec<LogRecord>) -> Accepted + Send + Sync>,
}

impl Handler for OtlpHandler {
    fn handle(&self, request: Incoming) -> Reply {
        if request.method != Method::Post {
            return status_reply(405, rpc::UNIMPLEMENTED, "only POST is supported");
        }
        // Path check before the counters: a probe against `/` is not a log
        // request, and counting it would corrupt the accounting invariant.
        if request.path != "/v1/logs" {
            return status_reply(404, rpc::UNIMPLEMENTED, "only /v1/logs is implemented");
        }

        self.stats.requests.fetch_add(1, Ordering::Relaxed);

        // JSON is a valid OTLP encoding we do not implement yet. Say so
        // precisely — a generic 400 sends someone hunting for a bad payload.
        if request.content_type().contains("json") {
            self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
            return status_reply(
                415,
                rpc::UNIMPLEMENTED,
                "OTLP/JSON is not implemented; send application/x-protobuf",
            );
        }

        let decoded: Decoded = match decode_request(&request.body, crate::now_unix_nanos()) {
            Ok(d) => d,
            Err(e) => {
                self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                return status_reply(
                    400,
                    rpc::INVALID_ARGUMENT,
                    &format!("malformed OTLP: {e}"),
                );
            }
        };

        self.stats.dropped_upstream.fetch_add(decoded.dropped_upstream, Ordering::Relaxed);
        let count = decoded.records.len() as u64;

        match (self.sink)(decoded.records) {
            Accepted::All => {
                self.stats.accepted.fetch_add(1, Ordering::Relaxed);
                self.stats.records.fetch_add(count, Ordering::Relaxed);
                // Empty ExportLogsServiceResponse: success, no partial_success.
                Reply::new(200, CONTENT_TYPE, Vec::new())
            }
            Accepted::Rejected => {
                self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                status_reply(503, rpc::UNAVAILABLE, "pipeline full, retry")
                    .with_header("Retry-After", &RETRY_AFTER_SECS.to_string())
            }
        }
    }

    fn reject(&self, reason: Reject) -> Reply {
        // A rejected body still consumed a request slot, so it is counted here
        // rather than in `handle`, which never runs for these.
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        match reason {
            Reject::TooLarge => {
                self.stats.too_large.fetch_add(1, Ordering::Relaxed);
                status_reply(413, rpc::RESOURCE_EXHAUSTED, "body exceeds the configured limit")
            }
            Reject::Unreadable(e) => {
                self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                status_reply(400, rpc::INVALID_ARGUMENT, &format!("cannot read body: {e}"))
            }
        }
    }
}

/// A running receiver.
pub struct Receiver {
    server: httpd::Server,
    stats: Arc<ReceiverStats>,
}

impl Receiver {
    /// Binds and starts serving. `sink` is called once per decoded batch, from
    /// a worker thread, and must not block for long — it holds a request open.
    pub fn start<F>(config: &ReceiverConfig, sink: F) -> Result<Self, ReceiverError>
    where
        F: Fn(Vec<LogRecord>) -> Accepted + Send + Sync + 'static,
    {
        let stats = Arc::new(ReceiverStats::default());
        let handler = Arc::new(OtlpHandler { stats: Arc::clone(&stats), sink: Box::new(sink) });
        let server = httpd::Server::start(
            &config.addr,
            config.workers,
            "otlp",
            Limits {
                max_body_bytes: config.max_body_bytes,
                max_decompressed_bytes: config.max_decompressed_bytes,
            },
            handler,
        )?;
        Ok(Self { server, stats })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    /// Stops accepting and joins the workers, returning the final counters.
    ///
    /// The counters are read after the join, so a request being served as
    /// shutdown starts still lands in the totals.
    pub fn shutdown(self) -> StatsSnapshot {
        self.server.shutdown();
        self.stats.snapshot()
    }
}

/// `google.rpc.Status` — field 1 `code` (varint), field 2 `message` (string).
fn status_proto(code: u32, message: &str) -> Vec<u8> {
    fn varint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }
    let mut out = Vec::with_capacity(message.len() + 8);
    out.push(0x08); // field 1, varint
    varint(&mut out, u64::from(code));
    out.push(0x12); // field 2, length-delimited
    varint(&mut out, message.len() as u64);
    out.extend_from_slice(message.as_bytes());
    out
}

fn status_reply(http_status: u16, rpc_code: u32, message: &str) -> Reply {
    Reply::new(http_status, CONTENT_TYPE, status_proto(rpc_code, message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    fn encode_one(body: &str, severity: u64) -> Vec<u8> {
        // Hand-rolled minimal request: resource_logs → scope_logs → log_records.
        fn varint(out: &mut Vec<u8>, mut v: u64) {
            loop {
                let b = (v & 0x7f) as u8;
                v >>= 7;
                if v == 0 {
                    out.push(b);
                    return;
                }
                out.push(b | 0x80);
            }
        }
        fn wrap(field: u32, payload: &[u8]) -> Vec<u8> {
            let mut out = Vec::new();
            varint(&mut out, (u64::from(field) << 3) | 2);
            varint(&mut out, payload.len() as u64);
            out.extend_from_slice(payload);
            out
        }
        let body_msg = wrap(1, body.as_bytes());
        let mut rec = wrap(5, &body_msg);
        varint(&mut rec, 2 << 3); // severity_number, varint
        varint(&mut rec, severity);
        let scope = wrap(2, &rec);
        let resource = wrap(2, &scope);
        wrap(1, &resource)
    }

    struct Harness {
        receiver: Option<Receiver>,
        seen: Arc<Mutex<Vec<LogRecord>>>,
    }

    impl Harness {
        fn start(accept: bool) -> Self {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let sink_seen = Arc::clone(&seen);
            let config =
                ReceiverConfig { addr: "127.0.0.1:0".to_string(), workers: 2, ..Default::default() };
            let receiver = Receiver::start(&config, move |records| {
                if !accept {
                    return Accepted::Rejected;
                }
                sink_seen.lock().unwrap().extend(records);
                Accepted::All
            })
            .unwrap();
            Self { receiver: Some(receiver), seen }
        }

        fn url(&self) -> String {
            format!("http://{}/v1/logs", self.receiver.as_ref().unwrap().local_addr())
        }

        fn stats(&self) -> StatsSnapshot {
            self.receiver.as_ref().unwrap().stats()
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            if let Some(r) = self.receiver.take() {
                r.shutdown();
            }
        }
    }

    fn post(url: &str, body: Vec<u8>, headers: &[(&str, &str)]) -> u16 {
        let mut req = ureq::post(url).header("Content-Type", "application/x-protobuf");
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        match req.send(&body[..]) {
            Ok(r) => r.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("request failed: {e}"),
        }
    }

    #[test]
    fn accepts_a_protobuf_batch() {
        let h = Harness::start(true);
        assert_eq!(post(&h.url(), encode_one("disk full", 17), &[]), 200);
        let seen = h.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].body, "disk full");
        assert_eq!(seen[0].severity, crate::Severity::ERROR);
        let s = h.stats();
        assert_eq!((s.requests, s.accepted, s.records), (1, 1, 1));
        assert!(s.accounts_for_everything());
    }

    #[test]
    fn accepts_gzipped_bodies() {
        // The Collector's otlphttp exporter compresses by default; rejecting
        // gzip would make the receiver useless against a stock config.
        let h = Harness::start(true);
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(&encode_one("compressed", 9)).unwrap();
        let gz = enc.finish().unwrap();
        assert_eq!(post(&h.url(), gz, &[("Content-Encoding", "gzip")]), 200);
        assert_eq!(h.seen.lock().unwrap()[0].body, "compressed");
    }

    #[test]
    fn full_pipeline_answers_503_rather_than_dropping() {
        // The exporter keeps the batch and retries — the whole reason this
        // receiver does not shed by severity the way the file tailer must.
        let h = Harness::start(false);
        assert_eq!(post(&h.url(), encode_one("x", 9), &[]), 503);
        let s = h.stats();
        assert_eq!((s.requests, s.rejected, s.records), (1, 1, 0));
        assert!(s.accounts_for_everything());
    }

    #[test]
    fn malformed_protobuf_is_400_not_a_panic() {
        let h = Harness::start(true);
        assert_eq!(post(&h.url(), vec![0xff; 12], &[]), 400);
        assert!(h.seen.lock().unwrap().is_empty());
        assert_eq!(h.stats().bad_request, 1);
    }

    #[test]
    fn json_is_refused_with_a_specific_status() {
        let h = Harness::start(true);
        let code = match ureq::post(h.url())
            .header("Content-Type", "application/json")
            .send(&b"{}"[..])
        {
            Ok(r) => r.status().as_u16(),
            Err(ureq::Error::StatusCode(c)) => c,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(code, 415, "415 tells the operator the encoding is wrong, not the payload");
    }

    #[test]
    fn oversized_body_is_413_and_not_buffered() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink_seen = Arc::clone(&seen);
        let config = ReceiverConfig {
            addr: "127.0.0.1:0".to_string(),
            workers: 1,
            max_body_bytes: 1024,
            ..Default::default()
        };
        let receiver = Receiver::start(&config, move |r| {
            sink_seen.lock().unwrap().extend(r);
            Accepted::All
        })
        .unwrap();
        let url = format!("http://{}/v1/logs", receiver.local_addr());
        assert_eq!(post(&url, vec![0u8; 4096], &[]), 413);
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(receiver.stats().too_large, 1);
        receiver.shutdown();
    }

    #[test]
    fn gzip_bomb_is_capped() {
        let config = ReceiverConfig {
            addr: "127.0.0.1:0".to_string(),
            workers: 1,
            max_decompressed_bytes: 4096,
            ..Default::default()
        };
        let receiver = Receiver::start(&config, |_| Accepted::All).unwrap();
        let url = format!("http://{}/v1/logs", receiver.local_addr());
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        enc.write_all(&vec![0u8; 1024 * 1024]).unwrap();
        let gz = enc.finish().unwrap();
        assert!(gz.len() < 4096, "test needs the compressed form to pass the body limit");
        assert_eq!(post(&url, gz, &[("Content-Encoding", "gzip")]), 413);
        receiver.shutdown();
    }

    #[test]
    fn wrong_path_and_method_do_not_count_as_log_requests() {
        let h = Harness::start(true);
        let base = format!("http://{}", h.receiver.as_ref().unwrap().local_addr());
        let code = match ureq::post(format!("{base}/v1/metrics")).send(&b""[..]) {
            Ok(r) => r.status().as_u16(),
            Err(ureq::Error::StatusCode(c)) => c,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(code, 404);
        let code = match ureq::get(format!("{base}/v1/logs")).call() {
            Ok(r) => r.status().as_u16(),
            Err(ureq::Error::StatusCode(c)) => c,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(code, 405);
        assert_eq!(h.stats().requests, 0, "probes must not pollute ingest counters");
    }

    #[test]
    fn concurrent_exporters_are_all_served() {
        let h = Harness::start(true);
        let url = h.url();
        let sent = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let url = url.clone();
                let sent = Arc::clone(&sent);
                std::thread::spawn(move || {
                    for n in 0..25 {
                        assert_eq!(post(&url, encode_one(&format!("{i}-{n}"), 9), &[]), 200);
                        sent.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(sent.load(Ordering::Relaxed), 100);
        assert_eq!(h.seen.lock().unwrap().len(), 100);
        assert_eq!(h.stats().records, 100);
    }

    #[test]
    fn empty_batch_is_accepted() {
        // Collectors probe with empty exports; a 400 would look like an outage.
        let h = Harness::start(true);
        assert_eq!(post(&h.url(), Vec::new(), &[]), 200);
        assert!(h.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn shutdown_joins_every_worker() {
        // More workers than `unblock` can wake: it pushes one token and calls
        // notify_one, so a pool that trusts it hangs on join forever.
        let config =
            ReceiverConfig { addr: "127.0.0.1:0".to_string(), workers: 4, ..Default::default() };
        let receiver = Receiver::start(&config, |_| Accepted::All).unwrap();
        let addr = receiver.local_addr();
        assert_eq!(post(&format!("http://{addr}/v1/logs"), encode_one("x", 9), &[]), 200);

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let stats = receiver.shutdown();
            let _ = done_tx.send(stats);
        });
        let stats = done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("shutdown must join every worker, not hang on the ones unblock missed");
        assert_eq!(stats.accepted, 1, "counters are read after the join");
    }

    #[test]
    fn status_proto_encodes_code_and_message() {
        assert_eq!(status_proto(3, "bad"), vec![0x08, 3, 0x12, 3, b'b', b'a', b'd']);
        // Two-byte varint for a code past 127.
        assert_eq!(&status_proto(300, "")[..3], &[0x08, 0xac, 0x02]);
    }
}
