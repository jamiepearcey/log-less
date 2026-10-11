//! The log-less peer endpoint: what one instance exposes so another can send to it.
//!
//! A log-less instance is already a collector (OTLP, HEC, Sentry, files) and a forwarder. Pointing one
//! at another is therefore one URL: the peer is addressed as itself, and what it can do is advertised
//! rather than configured. Two paths live on the listener:
//!
//! | Path | Behaviour |
//! |---|---|
//! | `GET /.well-known/logless` | the capability document: version, instance id, formats, limits, clock |
//! | `POST /v1/ingest` | newline-delimited JSON, one [`LogRecord`] per line, optionally gzipped |
//! | `POST /v1/parts/presign`, `/v1/parts/commit` | Parquet part upload to object storage, when `[peer.parts]` is set (`crate::parts`) |
//!
//! Plain OTLP also works against an instance's OTLP receiver and stays the fallback for a sender that
//! is not log-less. This native path exists because OTLP cannot carry what a log-less hop needs:
//!
//! * the **event id**, so a retried batch is recognised and dropped (delivery is at-least-once);
//! * the **observed time**, which partitions the store, so a replayed backlog lands in the hour it
//!   happened and not the hour it arrived;
//! * the **template id**, already mined by the sender.
//!
//! # Loops and provenance
//!
//! Every record carries two attributes. `logless.origin` is the instance that first sent it (set by the
//! first sender, kept by every later hop) and `logless.hops` counts the hops since. A receiver refuses a
//! record whose origin is itself (a cycle came back) or whose hops reached [`MAX_HOPS`], so a mis-wired
//! ring stops after one lap instead of multiplying data. The attributes stay in the store, which also
//! answers "which node produced this line" centrally.
//!
//! # Idempotency
//!
//! A bounded window of recently accepted event ids ([`DedupeWindow`]) drops a record seen again. The
//! window is memory-only and sized by `dedupe_window`; a retry older than it (a peer down for longer than
//! that many records) can duplicate. That is the same at-least-once contract as every other forwarder,
//! stated rather than hidden.

use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::httpd::{self, Handler, Incoming, Limits, Method, Reject, Reply};
use crate::model::{Attr, AttrValue, LogRecord};

pub const CAPABILITY_PATH: &str = "/.well-known/logless";
pub const INGEST_PATH: &str = "/v1/ingest";
/// The wire format name a sender and receiver both list in `formats`.
pub const WIRE_FORMAT: &str = "logless-ndjson/1";
pub const MAX_HOPS: u32 = 8;
pub const ATTR_ORIGIN: &str = "logless.origin";
pub const ATTR_HOPS: &str = "logless.hops";
const INSTANCE_ID_FILE: &str = "instance-id";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// Opt-in, like every listener here.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_addr")]
    pub addr: String,
    #[serde(default = "default_workers")]
    pub workers: usize,
    /// Accepted bearer tokens. **Empty means no authentication**, allowed on loopback only: a non-loopback
    /// bind without tokens is refused, so a misconfiguration cannot open an ingest endpoint to the network.
    #[serde(default)]
    pub tokens: Vec<String>,
    #[serde(default = "default_max_body")]
    pub max_body_bytes: usize,
    #[serde(default = "default_max_decompressed")]
    pub max_decompressed_bytes: usize,
    /// How many recently accepted event ids are remembered for duplicate detection.
    #[serde(default = "default_dedupe_window")]
    pub dedupe_window: usize,
    /// Advertised in the capability document: whether this instance also serves its store over S3.
    #[serde(default)]
    pub advertise_s3: bool,
    /// `[peer.parts]`: object storage edges upload finished Parquet parts to (`crate::parts`).
    #[serde(default)]
    pub parts: crate::parts::PartsConfig,
}

fn default_addr() -> String {
    "127.0.0.1:9300".into()
}
fn default_workers() -> usize {
    2
}
fn default_max_body() -> usize {
    httpd::DEFAULT_MAX_BODY_BYTES
}
fn default_max_decompressed() -> usize {
    httpd::DEFAULT_MAX_DECOMPRESSED_BYTES
}
fn default_dedupe_window() -> usize {
    500_000
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: default_addr(),
            workers: default_workers(),
            tokens: Vec::new(),
            max_body_bytes: default_max_body(),
            max_decompressed_bytes: default_max_decompressed(),
            dedupe_window: default_dedupe_window(),
            advertise_s3: false,
            parts: crate::parts::PartsConfig::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("the peer listener on {addr} has no tokens; refusing to accept unauthenticated records on a non-loopback address")]
    OpenOnPublicAddress { addr: String },
    #[error(transparent)]
    Http(#[from] httpd::HttpError),
}

/// What a log-less instance says about itself at [`CAPABILITY_PATH`]. A sender reads it to learn whether
/// the URL is a log-less peer at all, which formats it speaks, and what else it offers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    pub service: String,
    pub version: String,
    pub instance_id: String,
    pub formats: Vec<String>,
    pub ingest: Option<String>,
    /// `"bearer"` or `"none"`.
    pub auth: String,
    pub max_body_bytes: usize,
    pub max_hops: u32,
    /// Serves its store read-only over S3 (the console reads it through that endpoint).
    pub s3: bool,
    /// Answers a filtered scan. Not built yet; present so a client can ask without a version check.
    pub query: bool,
    /// Presigns uploads of finished Parquet parts (`crate::parts`): true only when `[peer.parts]` is
    /// enabled and complete.
    pub parts: bool,
    /// The peer's clock, unix milliseconds, so a sender can report skew.
    pub clock_ms: u64,
}

/// This instance's stable id, created on first use and kept in the data dir.
pub fn instance_id(data_dir: &Path) -> String {
    let path = data_dir.join(INSTANCE_ID_FILE);
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let existing = existing.trim();
        if !existing.is_empty() {
            return existing.to_string();
        }
    }
    let id = Uuid::now_v7().simple().to_string();
    let _ = std::fs::create_dir_all(data_dir);
    let tmp = path.with_extension("tmp");
    if std::fs::write(&tmp, &id).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
    id
}

/// Recently accepted event ids, oldest forgotten first.
#[derive(Debug)]
pub struct DedupeWindow {
    capacity: usize,
    inner: Mutex<(HashSet<Uuid>, VecDeque<Uuid>)>,
}

impl DedupeWindow {
    pub fn new(capacity: usize) -> Self {
        Self { capacity: capacity.max(1), inner: Mutex::new((HashSet::new(), VecDeque::new())) }
    }

    pub fn contains(&self, id: &Uuid) -> bool {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).0.contains(id)
    }

    pub fn insert_all<'a>(&self, ids: impl IntoIterator<Item = &'a Uuid>) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let (set, order) = &mut *guard;
        for id in ids {
            if set.insert(*id) {
                order.push_back(*id);
                while order.len() > self.capacity {
                    if let Some(old) = order.pop_front() {
                        set.remove(&old);
                    }
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).1.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What the pipeline did with a batch, as the other receivers: a full pipeline is a 503 and a retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admitted {
    Accepted,
    Busy,
}

#[derive(Debug, Default)]
pub struct PeerStats {
    pub requests: AtomicU64,
    pub records: AtomicU64,
    pub accepted: AtomicU64,
    pub duplicates: AtomicU64,
    pub loops: AtomicU64,
    pub busy: AtomicU64,
    pub unauthorized: AtomicU64,
    pub bad_request: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PeerStatsSnapshot {
    pub requests: u64,
    pub records: u64,
    pub accepted: u64,
    pub duplicates: u64,
    pub loops: u64,
    pub busy: u64,
    pub unauthorized: u64,
    pub bad_request: u64,
}

impl PeerStats {
    pub fn snapshot(&self) -> PeerStatsSnapshot {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        PeerStatsSnapshot {
            requests: load(&self.requests),
            records: load(&self.records),
            accepted: load(&self.accepted),
            duplicates: load(&self.duplicates),
            loops: load(&self.loops),
            busy: load(&self.busy),
            unauthorized: load(&self.unauthorized),
            bad_request: load(&self.bad_request),
        }
    }
}

struct PeerHandler {
    config: PeerConfig,
    instance: String,
    dedupe: DedupeWindow,
    stats: Arc<PeerStats>,
    sink: Box<dyn Fn(Vec<LogRecord>) -> Admitted + Send + Sync>,
    parts: Option<crate::parts::PartsCentral>,
}

impl PeerHandler {
    fn authorized(&self, request: &Incoming) -> Result<(), Reply> {
        if self.config.tokens.is_empty() {
            return Ok(());
        }
        let presented = request
            .header("authorization")
            .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
            .map(str::trim);
        let Some(token) = presented else {
            self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
            return Err(error_reply(401, "unauthorized", "a bearer token is required").with_header("WWW-Authenticate", "Bearer"));
        };
        // Compare against every token, with no early exit, so response time says nothing about which matched.
        let mut matched = false;
        for accepted in &self.config.tokens {
            matched |= constant_time_eq(accepted.as_bytes(), token.as_bytes());
        }
        if matched {
            Ok(())
        } else {
            self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
            Err(error_reply(403, "forbidden", "the token is not accepted").with_header("WWW-Authenticate", "Bearer"))
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            service: "log-less".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            instance_id: self.instance.clone(),
            formats: vec![WIRE_FORMAT.into()],
            ingest: Some(INGEST_PATH.into()),
            auth: if self.config.tokens.is_empty() { "none".into() } else { "bearer".into() },
            max_body_bytes: self.config.max_body_bytes,
            max_hops: MAX_HOPS,
            s3: self.config.advertise_s3,
            query: false,
            parts: self.parts.is_some(),
            clock_ms: crate::now_unix_nanos() / 1_000_000,
        }
    }

    fn ingest(&self, request: &Incoming) -> Reply {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        if let Err(denied) = self.authorized(request) {
            return denied;
        }
        let kind = request.content_type();
        if !kind.is_empty() && kind != "application/x-ndjson" && kind != "application/json" {
            self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
            return error_reply(415, "unsupported_media_type", &format!("send {WIRE_FORMAT} as application/x-ndjson"));
        }

        let mut records: Vec<LogRecord> = Vec::new();
        for (index, line) in request.body.split(|b| *b == b'\n').enumerate() {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            match serde_json::from_slice::<LogRecord>(line) {
                Ok(record) => records.push(record),
                Err(e) => {
                    self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                    return error_reply(400, "bad_record", &format!("line {} is not a log-less record: {e}", index + 1));
                }
            }
        }
        self.stats.records.fetch_add(records.len() as u64, Ordering::Relaxed);

        let mut keep = Vec::with_capacity(records.len());
        let (mut duplicates, mut loops) = (0u64, 0u64);
        let mut seen_in_batch: HashSet<Uuid> = HashSet::new();
        for record in records {
            if is_loop(&record, &self.instance) {
                loops += 1;
                continue;
            }
            if self.dedupe.contains(&record.event_id) || !seen_in_batch.insert(record.event_id) {
                duplicates += 1;
                continue;
            }
            keep.push(record);
        }
        self.stats.duplicates.fetch_add(duplicates, Ordering::Relaxed);
        self.stats.loops.fetch_add(loops, Ordering::Relaxed);

        let ids: Vec<Uuid> = keep.iter().map(|r| r.event_id).collect();
        let accepted = keep.len() as u64;
        if !keep.is_empty() {
            match (self.sink)(keep) {
                Admitted::Accepted => {}
                Admitted::Busy => {
                    self.stats.busy.fetch_add(1, Ordering::Relaxed);
                    // Not remembered: the sender retries this batch and it must not be taken for a duplicate.
                    return error_reply(503, "busy", "the ingest pipeline is full; retry").with_header("Retry-After", "1");
                }
            }
            // Remembered only after the pipeline took them.
            self.dedupe.insert_all(ids.iter());
        }
        self.stats.accepted.fetch_add(accepted, Ordering::Relaxed);
        if loops > 0 && accepted == 0 && duplicates == 0 {
            return Reply::json(508, format!(r#"{{"error":"loop_detected","accepted":0,"duplicates":0,"loops":{loops}}}"#));
        }
        Reply::json(200, format!(r#"{{"accepted":{accepted},"duplicates":{duplicates},"loops":{loops}}}"#))
    }
}

impl Handler for PeerHandler {
    fn handle(&self, request: Incoming) -> Reply {
        match (request.method, request.path) {
            (Method::Get, CAPABILITY_PATH) => {
                if let Err(denied) = self.authorized(&request) {
                    return denied;
                }
                match serde_json::to_string(&self.capabilities()) {
                    Ok(body) => Reply::json(200, body),
                    Err(e) => error_reply(500, "internal", &e.to_string()),
                }
            }
            (Method::Post, INGEST_PATH) => self.ingest(&request),
            (Method::Post, path @ (crate::parts::PRESIGN_PATH | crate::parts::COMMIT_PATH)) => {
                self.stats.requests.fetch_add(1, Ordering::Relaxed);
                if let Err(denied) = self.authorized(&request) {
                    return denied;
                }
                match &self.parts {
                    Some(parts) if path == crate::parts::PRESIGN_PATH => parts.presign(&request.body),
                    Some(parts) => parts.commit(&request.body),
                    None => error_reply(404, "not_found", "this peer does not accept parts"),
                }
            }
            (Method::Get, INGEST_PATH) => error_reply(405, "method_not_allowed", "POST records here"),
            (Method::Post, CAPABILITY_PATH) => error_reply(405, "method_not_allowed", "GET only"),
            _ => error_reply(404, "not_found", "no such path"),
        }
    }

    fn reject(&self, reason: Reject) -> Reply {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        match reason {
            Reject::TooLarge => error_reply(413, "too_large", "the request body exceeds the configured limit"),
            Reject::Unreadable(_) => {
                self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                error_reply(400, "unreadable", "the body is not readable")
            }
        }
    }
}

/// A record that came back to the instance that first sent it, or has travelled too far.
pub fn is_loop(record: &LogRecord, own_instance: &str) -> bool {
    let mut hops = 0u32;
    for attr in &record.attributes {
        if attr.key == ATTR_ORIGIN {
            if let AttrValue::Str(origin) = &attr.value {
                if origin == own_instance {
                    return true;
                }
            }
        } else if attr.key == ATTR_HOPS {
            hops = match &attr.value {
                AttrValue::I64(n) => (*n).max(0) as u32,
                AttrValue::Str(s) => s.parse().unwrap_or(0),
                _ => 0,
            };
        }
    }
    hops >= MAX_HOPS
}

/// Stamps a record for sending: the first sender's id (kept by later hops) and one more hop.
pub fn stamp_for_send(record: &mut LogRecord, own_instance: &str) {
    let mut hops = 0i64;
    let mut has_origin = false;
    record.attributes.retain(|attr| {
        if attr.key == ATTR_HOPS {
            hops = match &attr.value {
                AttrValue::I64(n) => *n,
                AttrValue::Str(s) => s.parse().unwrap_or(0),
                _ => 0,
            };
            false
        } else {
            if attr.key == ATTR_ORIGIN {
                has_origin = true;
            }
            true
        }
    });
    if !has_origin {
        record.attributes.push(Attr { key: ATTR_ORIGIN.into(), value: AttrValue::Str(own_instance.into()) });
    }
    record.attributes.push(Attr { key: ATTR_HOPS.into(), value: AttrValue::I64(hops + 1) });
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = (a.len() ^ b.len()) as u8;
    for i in 0..a.len().max(b.len()) {
        diff |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

fn error_reply(status: u16, code: &str, message: &str) -> Reply {
    Reply::json(status, serde_json::json!({ "error": code, "message": message }).to_string())
}

fn is_loopback(addr: &str) -> bool {
    use std::net::ToSocketAddrs;
    addr.to_socket_addrs().map(|mut a| a.all(|s| s.ip().is_loopback())).unwrap_or(false)
}

pub struct Receiver {
    server: httpd::Server,
    stats: Arc<PeerStats>,
    instance: String,
}

impl Receiver {
    /// Binds and starts serving. `sink` takes a batch into the ingest pipeline.
    pub fn start<F>(config: &PeerConfig, instance: String, sink: F) -> Result<Self, PeerError>
    where
        F: Fn(Vec<LogRecord>) -> Admitted + Send + Sync + 'static,
    {
        Self::start_with_catalog(config, instance, None, sink)
    }

    /// As [`Receiver::start`], and serves part upload when `[peer.parts]` is enabled and complete;
    /// committed parts are recorded in the catalog at `catalog`. An incomplete `[peer.parts]` is logged
    /// (never its secret) and not advertised.
    pub fn start_with_catalog<F>(config: &PeerConfig, instance: String, catalog: Option<PathBuf>, sink: F) -> Result<Self, PeerError>
    where
        F: Fn(Vec<LogRecord>) -> Admitted + Send + Sync + 'static,
    {
        let parts = match (config.parts.enabled, catalog) {
            (false, _) => None,
            (true, None) => {
                tracing::warn!("[peer.parts] is enabled but this process has no catalog; part upload is off");
                None
            }
            (true, Some(path)) => match crate::parts::PartsCentral::new(config.parts.clone(), path) {
                Ok(p) => Some(p),
                Err(reason) => {
                    tracing::warn!(%reason, "part upload is off");
                    None
                }
            },
        };
        if config.tokens.is_empty() && !is_loopback(&config.addr) {
            return Err(PeerError::OpenOnPublicAddress { addr: config.addr.clone() });
        }
        let stats = Arc::new(PeerStats::default());
        let handler = Arc::new(PeerHandler {
            config: config.clone(),
            instance: instance.clone(),
            dedupe: DedupeWindow::new(config.dedupe_window),
            stats: Arc::clone(&stats),
            sink: Box::new(sink),
            parts,
        });
        let server = httpd::Server::start(
            &config.addr,
            config.workers,
            "peer",
            Limits { max_body_bytes: config.max_body_bytes, max_decompressed_bytes: config.max_decompressed_bytes },
            handler,
        )?;
        Ok(Self { server, stats, instance })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub fn instance_id(&self) -> &str {
        &self.instance
    }

    pub fn stats(&self) -> PeerStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn shutdown(self) -> PeerStatsSnapshot {
        self.server.shutdown();
        self.stats.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;
    use std::sync::Mutex as StdMutex;

    fn record(body: &str) -> LogRecord {
        let mut r = LogRecord::new(1_700_000_000_000_000_000, Severity::ERROR, body);
        r.observed_unix_nano = 1_700_000_005_000_000_000;
        r.service = Some("api".into());
        r.template_id = Some(42);
        r
    }

    fn ndjson(records: &[LogRecord]) -> Vec<u8> {
        records.iter().map(|r| serde_json::to_string(r).unwrap()).collect::<Vec<_>>().join("\n").into_bytes()
    }

    fn post(addr: SocketAddr, path: &str, token: Option<&str>, body: &[u8]) -> (u16, serde_json::Value) {
        let mut req = ureq::post(format!("http://{addr}{path}")).header("Content-Type", "application/x-ndjson");
        if let Some(t) = token {
            req = req.header("Authorization", &format!("Bearer {t}"));
        }
        send(req.config().http_status_as_error(false).build().send(body))
    }

    fn get(addr: SocketAddr, path: &str, token: Option<&str>) -> (u16, serde_json::Value) {
        let mut req = ureq::get(format!("http://{addr}{path}"));
        if let Some(t) = token {
            req = req.header("Authorization", &format!("Bearer {t}"));
        }
        send(req.config().http_status_as_error(false).build().call())
    }

    fn send(result: Result<ureq::http::Response<ureq::Body>, ureq::Error>) -> (u16, serde_json::Value) {
        let mut response = result.expect("transport");
        let status = response.status().as_u16();
        let value = crate::sync::read_json(response.body_mut()).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    fn start(config: PeerConfig, busy: bool) -> (Receiver, Arc<StdMutex<Vec<LogRecord>>>) {
        let got = Arc::new(StdMutex::new(Vec::new()));
        let sink = Arc::clone(&got);
        let receiver = Receiver::start(&PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), ..config }, "self0001".into(), move |records| {
            if busy {
                return Admitted::Busy;
            }
            sink.lock().unwrap().extend(records);
            Admitted::Accepted
        })
        .unwrap();
        (receiver, got)
    }

    #[test]
    fn records_round_trip_with_their_event_id_observed_time_and_template() {
        let (rx, got) = start(PeerConfig::default(), false);
        let sent = record("boom");
        let (status, body) = post(rx.local_addr(), INGEST_PATH, None, &ndjson(&[sent.clone()]));
        assert_eq!(200, status, "{body}");
        assert_eq!(1, body["accepted"]);
        let stored = got.lock().unwrap();
        assert_eq!(sent, stored[0], "nothing about the record changes in transit");
        rx.shutdown();
    }

    #[test]
    fn a_retried_batch_is_recognised_and_not_stored_twice() {
        let (rx, got) = start(PeerConfig::default(), false);
        let batch = ndjson(&[record("a"), record("b")]);
        assert_eq!(2, post(rx.local_addr(), INGEST_PATH, None, &batch).1["accepted"]);
        let (status, again) = post(rx.local_addr(), INGEST_PATH, None, &batch);
        assert_eq!(200, status);
        assert_eq!((0, 2), (again["accepted"].as_u64().unwrap(), again["duplicates"].as_u64().unwrap()));
        assert_eq!(2, got.lock().unwrap().len());
        assert_eq!(2, rx.stats().duplicates);
        rx.shutdown();
    }

    #[test]
    fn a_busy_pipeline_is_a_503_and_the_retry_is_not_taken_for_a_duplicate() {
        let (rx, _) = start(PeerConfig::default(), true);
        let batch = ndjson(&[record("a")]);
        assert_eq!(503, post(rx.local_addr(), INGEST_PATH, None, &batch).0);
        assert_eq!(503, post(rx.local_addr(), INGEST_PATH, None, &batch).0, "still 503, not 200 duplicate");
        assert_eq!(0, rx.stats().duplicates);
        rx.shutdown();
    }

    #[test]
    fn a_record_that_came_back_to_its_origin_is_refused_and_a_long_chain_is_cut() {
        let (rx, got) = start(PeerConfig::default(), false);
        let mut home = record("back");
        home.attributes.push(Attr { key: ATTR_ORIGIN.into(), value: AttrValue::Str("self0001".into()) });
        let mut far = record("far");
        far.attributes.push(Attr { key: ATTR_HOPS.into(), value: AttrValue::I64(MAX_HOPS as i64) });
        let ok = record("fine");
        let (status, body) = post(rx.local_addr(), INGEST_PATH, None, &ndjson(&[home, far, ok.clone()]));
        assert_eq!(200, status);
        assert_eq!((1, 2), (body["accepted"].as_u64().unwrap(), body["loops"].as_u64().unwrap()));
        assert_eq!(vec![ok], *got.lock().unwrap());
        rx.shutdown();
    }

    #[test]
    fn a_batch_made_only_of_loops_says_so_with_508() {
        let (rx, _) = start(PeerConfig::default(), false);
        let mut home = record("back");
        home.attributes.push(Attr { key: ATTR_ORIGIN.into(), value: AttrValue::Str("self0001".into()) });
        assert_eq!(508, post(rx.local_addr(), INGEST_PATH, None, &ndjson(&[home])).0);
        rx.shutdown();
    }

    #[test]
    fn stamping_keeps_the_first_origin_and_counts_hops() {
        let mut r = record("x");
        stamp_for_send(&mut r, "edge");
        stamp_for_send(&mut r, "regional");
        let origin: Vec<_> = r.attributes.iter().filter(|a| a.key == ATTR_ORIGIN).collect();
        assert_eq!(1, origin.len());
        assert_eq!(AttrValue::Str("edge".into()), origin[0].value);
        let hops: Vec<_> = r.attributes.iter().filter(|a| a.key == ATTR_HOPS).collect();
        assert_eq!(vec![AttrValue::I64(2)], hops.iter().map(|a| a.value.clone()).collect::<Vec<_>>());
        assert!(!is_loop(&r, "central"));
        assert!(is_loop(&r, "edge"));
    }

    #[test]
    fn tokens_are_required_when_configured_and_the_capability_document_needs_one_too() {
        let (rx, _) = start(PeerConfig { tokens: vec!["s3cret".into(), "other".into()], ..PeerConfig::default() }, false);
        let a = rx.local_addr();
        assert_eq!(401, get(a, CAPABILITY_PATH, None).0);
        assert_eq!(403, get(a, CAPABILITY_PATH, Some("nope")).0);
        assert_eq!(401, post(a, INGEST_PATH, None, b"").0);
        let (status, caps) = get(a, CAPABILITY_PATH, Some("other"));
        assert_eq!(200, status);
        assert_eq!("log-less", caps["service"]);
        assert_eq!("bearer", caps["auth"]);
        assert_eq!(serde_json::json!([WIRE_FORMAT]), caps["formats"]);
        assert_eq!("self0001", caps["instance_id"]);
        assert_eq!(2, rx.stats().unauthorized - 1, "one 401 is counted by the capability probe too");
        rx.shutdown();
    }

    #[test]
    fn a_malformed_line_rejects_the_batch_and_names_the_line() {
        let (rx, got) = start(PeerConfig::default(), false);
        let mut body = ndjson(&[record("a")]);
        body.extend_from_slice(b"\nnot json\n");
        let (status, v) = post(rx.local_addr(), INGEST_PATH, None, &body);
        assert_eq!(400, status);
        assert!(v["message"].as_str().unwrap().contains("line 2"), "{v}");
        assert!(got.lock().unwrap().is_empty(), "a bad batch stores nothing");
        rx.shutdown();
    }

    #[test]
    fn an_open_listener_on_a_public_address_is_refused() {
        let err = Receiver::start(&PeerConfig { enabled: true, addr: "0.0.0.0:0".into(), ..PeerConfig::default() }, "x".into(), |_| Admitted::Accepted).err().unwrap();
        assert!(matches!(err, PeerError::OpenOnPublicAddress { .. }));
    }

    #[test]
    fn the_dedupe_window_forgets_the_oldest_first() {
        let w = DedupeWindow::new(2);
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::now_v7()).collect();
        w.insert_all(ids.iter());
        assert!(!w.contains(&ids[0]));
        assert!(w.contains(&ids[1]) && w.contains(&ids[2]));
        assert_eq!(2, w.len());
    }

    #[test]
    fn the_instance_id_is_created_once_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let first = instance_id(dir.path());
        assert_eq!(32, first.len());
        assert_eq!(first, instance_id(dir.path()));
    }
}
