//! OTLP/HTTP logs receiver.
//!
//! `POST /v1/logs` with `Content-Type: application/x-protobuf` — the wire
//! contract that the OpenTelemetry Collector's `otlphttp` exporter and Vector's
//! `opentelemetry` sink speak by default. Pointing an existing fleet at
//! log-less becomes an endpoint change rather than a migration.
//!
//! The server itself is [`crate::bounded_http`]; this file is the protocol.
//!
//! # Who may send
//!
//! There is no credential in OTLP/HTTP as log-less serves it, so on a loopback
//! address (the default) the `Host` must be a loopback name, and on any address
//! the `Content-Type` must be one of the two OTLP types. A web page can send
//! neither type without a CORS preflight, which this listener never answers, so
//! a page in a local browser cannot write logs here. Both rules are checked from
//! the head, before the body is read.
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
use crate::bounded_http::{self, BoundedServer};
use crate::httpd::{self, Handler, Incoming, Limits, Method, Reject, Reply};
use crate::model::LogRecord;

pub use crate::httpd::{DEFAULT_MAX_BODY_BYTES, DEFAULT_MAX_DECOMPRESSED_BYTES};

/// Seconds sent in `Retry-After` when the pipeline is full.
const RETRY_AFTER_SECS: u32 = 1;

const CONTENT_TYPE: &str = "application/x-protobuf";
const JSON_CONTENT_TYPE: &str = "application/json";

/// gRPC status codes used in the `google.rpc.Status` bodies. OTLP/HTTP requires
/// that shape for non-2xx, and collectors surface the message in their own
/// logs, which is where an operator looks first.
mod rpc {
    pub const INVALID_ARGUMENT: u32 = 3;
    pub const PERMISSION_DENIED: u32 = 7;
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
    /// Sizes the listener: each connection has its own thread, and `workers` x 32
    /// may be open at once (at least 64). Requests are short and CPU-light (decode
    /// plus a queue push), so this is about concurrent exporters, not throughput.
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
    /// Bound to a loopback address, so the `Host` has to be a loopback name.
    loopback: bool,
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

        // Both encodings of the same message. Chosen by content type, because
        // that is what the spec says and what every emitter sets.
        let json = request.content_type() == JSON_CONTENT_TYPE;
        let decoded: Decoded = if json {
            match super::json::decode_request(&request.body, crate::now_unix_nanos()) {
                Ok(d) => d,
                Err(e) => {
                    self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                    return json_status_reply(400, &format!("malformed OTLP/JSON: {e}"));
                }
            }
        } else {
            match decode_request(&request.body, crate::now_unix_nanos()) {
                Ok(d) => d,
                Err(e) => {
                    self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                    return status_reply(400, rpc::INVALID_ARGUMENT, &format!("malformed OTLP: {e}"));
                }
            }
        };

        self.stats.dropped_upstream.fetch_add(decoded.dropped_upstream, Ordering::Relaxed);
        let count = decoded.records.len() as u64;

        match (self.sink)(decoded.records) {
            Accepted::All => {
                self.stats.accepted.fetch_add(1, Ordering::Relaxed);
                self.stats.records.fetch_add(count, Ordering::Relaxed);
                // Empty ExportLogsServiceResponse. A client that sent JSON gets
                // JSON back: it is parsing the response with a JSON decoder.
                if json {
                    Reply::json(200, "{}")
                } else {
                    Reply::new(200, CONTENT_TYPE, Vec::new())
                }
            }
            Accepted::Rejected => {
                self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                let reply = if json {
                    json_status_reply(503, "pipeline full, retry")
                } else {
                    status_reply(503, rpc::UNAVAILABLE, "pipeline full, retry")
                };
                reply.with_header("Retry-After", &RETRY_AFTER_SECS.to_string())
            }
        }
    }

    /// Refusals that need only the head, answered before the body is read: a
    /// foreign `Host` on loopback, a type that is not OTLP, an encoding that is
    /// not gzip. Counted as bad requests. Other paths and methods fall through
    /// to `handle`, which answers them without counting.
    fn preflight(&self, head: &Incoming) -> Option<Reply> {
        if head.method != Method::Post || head.path != "/v1/logs" {
            return None;
        }
        let kind = head.content_type();
        let refused = if self.loopback && !bounded_http::loopback_host(head) {
            status_reply(403, rpc::PERMISSION_DENIED, "the Host header is not a loopback name")
        } else if head.header_count("content-type") != 1 || (kind != CONTENT_TYPE && kind != JSON_CONTENT_TYPE) {
            status_reply(415, rpc::INVALID_ARGUMENT, "send application/x-protobuf or application/json")
        } else if !bounded_http::encoding_accepted(head) {
            status_reply(415, rpc::INVALID_ARGUMENT, "Content-Encoding must be gzip or absent")
        } else {
            return None;
        };
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
        Some(refused)
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
    server: BoundedServer,
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
        let handler = Arc::new(OtlpHandler {
            stats: Arc::clone(&stats),
            loopback: bounded_http::loopback_addr(&config.addr),
            sink: Box::new(sink),
        });
        let server = BoundedServer::start(
            &config.addr,
            "otlp",
            Limits {
                max_body_bytes: config.max_body_bytes,
                max_decompressed_bytes: config.max_decompressed_bytes,
            },
            bounded_http::ingest_limits(config.workers),
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

    /// Stops accepting and waits (bounded) for in-flight requests, returning the
    /// final counters.
    ///
    /// The counters are read after the wait, so a request being served as
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

/// The JSON form of `google.rpc.Status`, for clients that sent JSON.
fn json_status_reply(http_status: u16, message: &str) -> Reply {
    Reply::json(
        http_status,
        format!(r#"{{"code":{},"message":"{}"}}"#, http_status, message.replace('"', "'")),
    )
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

    fn encode_one(body: &str, severity: u64) -> Vec<u8> {
        // Hand-rolled minimal request: resource_logs → scope_logs → log_records.
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
    fn json_requests_are_decoded_and_answered_in_json() {
        let h = Harness::start(true);
        let body = br#"{"resourceLogs":[{"resource":{"attributes":[
            {"key":"service.name","value":{"stringValue":"checkout"}}]},
          "scopeLogs":[{"logRecords":[
            {"severityNumber":17,"body":{"stringValue":"declined"}}]}]}]}"#;
        let mut response = ureq::post(h.url())
            .header("Content-Type", "application/json")
            .send(&body[..])
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(response.body_mut().read_to_string().unwrap(), "{}");
        let seen = h.seen.lock().unwrap();
        assert_eq!(seen[0].body, "declined");
        assert_eq!(seen[0].severity, crate::Severity::ERROR);
        assert_eq!(seen[0].service.as_deref(), Some("checkout"));
    }

    #[test]
    fn malformed_json_is_answered_in_json_not_protobuf() {
        // A client that sent JSON is parsing the reply with a JSON decoder;
        // handing it a protobuf Status makes the real error unreadable.
        let h = Harness::start(true);
        let agent: ureq::Agent =
            ureq::Agent::config_builder().http_status_as_error(false).build().into();
        let mut response = agent
            .post(h.url())
            .header("Content-Type", "application/json")
            .send(&b"{not json"[..])
            .unwrap();
        assert_eq!(response.status().as_u16(), 400);
        let body = response.body_mut().read_to_string().unwrap();
        assert!(body.starts_with('{') && body.contains("message"), "{body}");
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

    // Abuse cases, sent as raw bytes (see `crate::bounded_http::testing`).
    use crate::bounded_http::testing::{self as raw, WAIT};

    const PROTOBUF: (&str, &str) = ("Content-Type", "application/x-protobuf");

    fn logs_request(headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
        raw::request("POST", "/v1/logs", &[&[PROTOBUF][..], headers].concat(), body)
    }

    fn started(config: ReceiverConfig) -> (Receiver, Arc<Mutex<Vec<LogRecord>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink_seen = Arc::clone(&seen);
        let config = ReceiverConfig { addr: "127.0.0.1:0".to_string(), ..config };
        let receiver = Receiver::start(&config, move |records| {
            sink_seen.lock().unwrap().extend(records);
            Accepted::All
        })
        .unwrap();
        (receiver, seen)
    }

    #[test]
    fn a_stalled_upload_does_not_stop_the_receiver_answering() {
        // With a pool of two workers, two exporters that stop half way through a body held both forever.
        let (rx, seen) = started(ReceiverConfig { workers: 2, ..Default::default() });
        let head = "POST /v1/logs HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/x-protobuf\r\nContent-Length: 100000\r\n\r\n";
        let (status, text) = raw::answer_beside_stalled_uploads(rx.local_addr(), head, &logs_request(&[], &encode_one("served", 9)));
        assert_eq!(200, status, "{text}");
        assert_eq!(1, seen.lock().unwrap().len());
        rx.shutdown();
    }

    #[test]
    fn a_2_mib_header_is_431_and_the_receiver_keeps_serving() {
        let (rx, seen) = started(ReceiverConfig::default());
        let a = rx.local_addr();
        assert_eq!(431, raw::huge_header_status(a, "/v1/logs"));
        let many: Vec<(&str, &str)> = (0..200).map(|_| ("X-A", "b")).collect();
        assert_eq!(431, raw::exchange(a, &logs_request(&many, b""), WAIT).0, "200 headers");
        assert_eq!(200, raw::exchange(a, &logs_request(&[], &encode_one("after", 9)), WAIT).0);
        assert_eq!(1, seen.lock().unwrap().len());
        rx.shutdown();
    }

    #[test]
    fn connections_past_the_cap_are_closed_and_the_slots_come_back() {
        // One worker: a cap of 64 connections.
        let (rx, _) = started(ReceiverConfig { workers: 1, ..Default::default() });
        let a = rx.local_addr();
        assert_eq!(5, raw::closed_past_the_cap(a, 64, 5));
        assert_eq!(200, raw::exchange(a, &logs_request(&[], &encode_one("after", 9)), WAIT).0);
        rx.shutdown();
    }

    #[test]
    fn an_oversized_or_damaged_body_is_refused_before_the_decoder() {
        let (rx, seen) = started(ReceiverConfig { max_body_bytes: 64 * 1024, max_decompressed_bytes: 64 * 1024, ..Default::default() });
        let a = rx.local_addr();
        assert_eq!(413, raw::exchange(a, &logs_request(&[], &vec![0u8; 100_000]), WAIT).0, "sent");
        // Declared 1 TiB and never sent: answered from the head.
        let lie = "POST /v1/logs HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/x-protobuf\r\nContent-Length: 1099511627776\r\n\r\n";
        let started = std::time::Instant::now();
        assert_eq!(413, raw::exchange(a, lie.as_bytes(), WAIT).0, "declared");
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        // A bomb: 16 MiB of zeros in under the 64 KiB body limit.
        let bomb = raw::gzip(&vec![0u8; 16 * 1024 * 1024]);
        assert!(bomb.len() < 64 * 1024);
        let (status, text) = raw::exchange(a, &logs_request(&[("Content-Encoding", "gzip")], &bomb), WAIT);
        assert_eq!(413, status, "{text}");
        // Truncated gzip and bytes that only claim to be gzip.
        let packed = raw::gzip(&encode_one("x", 9));
        for junk in [&packed[..packed.len() - 4], &b"plain"[..], &[0x1f, 0x8b, 0x08][..]] {
            let (status, text) = raw::exchange(a, &logs_request(&[("Content-Encoding", "gzip")], junk), WAIT);
            assert_eq!(400, status, "{junk:?}: {text}");
        }
        assert!(seen.lock().unwrap().is_empty());
        let s = rx.shutdown();
        assert!(s.accounts_for_everything(), "{s:?}");
    }

    #[test]
    fn malformed_protobuf_and_json_are_400_and_never_a_crash() {
        let (rx, seen) = started(ReceiverConfig::default());
        let a = rx.local_addr();
        let good = encode_one("whole", 9);
        // A record body that is an array holding an array, 10 000 deep (AnyValue.array_value, ArrayValue.values).
        let mut value = wrap(1, b"");
        for _ in 0..10_000 {
            value = wrap(5, &wrap(1, &value));
        }
        let deep = wrap(1, &wrap(2, &wrap(2, &wrap(5, &value))));
        let protobuf: Vec<(&str, Vec<u8>)> = vec![
            ("truncated", good[..good.len() - 3].to_vec()),
            ("length past the end", vec![0x0a, 0xff, 0xff, 0x03, 0x01]),
            ("varint that never ends", [&[0x08u8][..], &[0xff; 12]].concat()),
            ("wire type 7", vec![0x0f, 0x00]),
            ("deeply nested values", deep),
        ];
        for (name, body) in protobuf {
            let (status, text) = raw::exchange(a, &logs_request(&[], &body), WAIT);
            assert_eq!(400, status, "{name}: {text}");
        }
        let nested = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
        let huge_number = br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":1e999999}]}]}]}"#;
        for body in [&br#"{"resourceLogs":"#[..], b"\"x\"", nested.as_bytes(), huge_number, b"\xff\xfe"] {
            let req = raw::request("POST", "/v1/logs", &[("Content-Type", "application/json")], body);
            let (status, text) = raw::exchange(a, &req, WAIT);
            assert_eq!(400, status, "{}: {text}", String::from_utf8_lossy(body));
            assert!(text.contains("\"message\""), "a JSON client gets a JSON error: {text}");
        }
        assert_eq!(200, raw::exchange(a, &logs_request(&[], &good), WAIT).0, "still serving");
        assert_eq!(1, seen.lock().unwrap().len());
        rx.shutdown();
    }

    #[test]
    fn only_protobuf_and_json_content_types_and_gzip_are_accepted() {
        // A body with no Content-Type, or text/plain, or a form type, is what a web page can send to
        // 127.0.0.1 without a CORS preflight. Those were decoded as protobuf and stored.
        let (rx, seen) = started(ReceiverConfig::default());
        let a = rx.local_addr();
        let body = encode_one("x", 9);
        for kind in ["text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x", "application/octet-stream", "application/protobuf"] {
            let req = raw::request("POST", "/v1/logs", &[("Content-Type", kind)], &body);
            let (status, text) = raw::exchange(a, &req, WAIT);
            assert_eq!(415, status, "{kind}: {text}");
        }
        assert_eq!(415, raw::exchange(a, &raw::request("POST", "/v1/logs", &[], &body), WAIT).0, "no content type");
        let two = logs_request(&[("Content-Type", "application/json")], &body);
        assert_eq!(415, raw::exchange(a, &two, WAIT).0, "two content types");
        for encoding in ["br", "deflate", "zstd", "gzip, gzip", "identity, gzip"] {
            let (status, text) = raw::exchange(a, &logs_request(&[("Content-Encoding", encoding)], &body), WAIT);
            assert_eq!(415, status, "{encoding}: {text}");
        }
        let twice = logs_request(&[("Content-Encoding", "gzip"), ("Content-Encoding", "gzip")], &raw::gzip(&raw::gzip(&body)));
        assert_eq!(415, raw::exchange(a, &twice, WAIT).0, "stacked encodings");
        assert!(seen.lock().unwrap().is_empty());
        // The spellings real exporters use still work.
        assert_eq!(200, raw::exchange(a, &logs_request(&[("Content-Encoding", "identity")], &body), WAIT).0);
        let charset = raw::request("POST", "/v1/logs", &[("Content-Type", "application/x-protobuf; charset=binary")], &body);
        assert_eq!(200, raw::exchange(a, &charset, WAIT).0);
        let s = rx.shutdown();
        assert!(s.accounts_for_everything(), "{s:?}");
    }

    #[test]
    fn a_loopback_receiver_refuses_a_foreign_host_header() {
        // DNS rebinding: a page whose own name now resolves to 127.0.0.1 is same-origin with it, and its
        // requests carry that name in Host.
        let (rx, seen) = started(ReceiverConfig::default());
        let a = rx.local_addr();
        let body = encode_one("x", 9);
        for host in ["evil.example", "evil.example:4318", "127.0.0.1.nip.io", ""] {
            let req = format!("POST /v1/logs HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\n\r\n", body.len());
            let (status, text) = raw::exchange(a, &[req.as_bytes(), &body].concat(), WAIT);
            assert_eq!(403, status, "Host {host:?}: {text}");
        }
        let none = format!("POST /v1/logs HTTP/1.1\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\n\r\n", body.len());
        assert_eq!(403, raw::exchange(a, &[none.as_bytes(), &body].concat(), WAIT).0, "no Host");
        let two = logs_request(&[("Host", "evil.example")], &body);
        assert_eq!(403, raw::exchange(a, &two, WAIT).0, "a second Host");
        assert!(seen.lock().unwrap().is_empty());
        for host in ["localhost:4318", "[::1]:4318", "127.0.0.1"] {
            let req = format!("POST /v1/logs HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\n\r\n", body.len());
            assert_eq!(200, raw::exchange(a, &[req.as_bytes(), &body].concat(), WAIT).0, "Host {host}");
        }
        rx.shutdown();
    }

    #[test]
    fn a_full_pipeline_says_when_to_retry_and_a_chunked_export_is_read() {
        let h = Harness::start(false);
        let addr = h.receiver.as_ref().unwrap().local_addr();
        let (status, text) = raw::exchange(addr, &logs_request(&[], &encode_one("x", 9)), WAIT);
        assert_eq!(503, status, "{text}");
        assert!(text.contains("Retry-After: 1\r\n"), "{text}");
        drop(h);
        // Exporters that stream a gzip body (the OpenTelemetry JS exporter) send it chunked.
        let (rx, seen) = started(ReceiverConfig::default());
        let req = raw::chunked("POST", "/v1/logs", &[PROTOBUF, ("Content-Encoding", "gzip")], &raw::gzip(&encode_one("streamed", 9)), 7);
        let (status, text) = raw::exchange(rx.local_addr(), &req, WAIT);
        assert_eq!(200, status, "{text}");
        assert_eq!("streamed", seen.lock().unwrap()[0].body);
        rx.shutdown();
    }

    #[test]
    fn status_proto_encodes_code_and_message() {
        assert_eq!(status_proto(3, "bad"), vec![0x08, 3, 0x12, 3, b'b', b'a', b'd']);
        // Two-byte varint for a code past 127.
        assert_eq!(&status_proto(300, "")[..3], &[0x08, 0xac, 0x02]);
    }
}
