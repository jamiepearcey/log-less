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

use crate::bounded_http::{BoundedServer, HeadLimits};
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
const CONNECTIONS_PER_WORKER: usize = 32;
const MIN_CONNECTIONS: usize = 64;
/// Wall-clock budget for a sender to deliver one request, head and body: a 4 MiB batch over a slow link fits.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone, PartialEq, Serialize, Deserialize)]
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

// Hand-written so a logged config never carries a token.
impl std::fmt::Debug for PeerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConfig")
            .field("enabled", &self.enabled)
            .field("addr", &self.addr)
            .field("workers", &self.workers)
            .field("tokens", &format_args!("<{} redacted>", self.tokens.len()))
            .field("max_body_bytes", &self.max_body_bytes)
            .field("max_decompressed_bytes", &self.max_decompressed_bytes)
            .field("dedupe_window", &self.dedupe_window)
            .field("advertise_s3", &self.advertise_s3)
            .field("parts", &self.parts)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("the peer listener on {addr} has no tokens; refusing to accept unauthenticated records on a non-loopback address")]
    OpenOnPublicAddress { addr: String },
    #[error("a peer token is {len} characters; binding the non-loopback address {addr} needs at least {min}", min = crate::admin_api::MIN_REMOTE_TOKEN_LEN)]
    WeakToken { addr: String, len: usize },
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
        // Two Authorization headers are ambiguous (a proxy may pick the other one), so neither counts.
        let presented = request
            .header("authorization")
            .filter(|_| request.header_count("authorization") == 1)
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

    fn refuse_head(&self, head: &Incoming, ingest: bool) -> Option<Reply> {
        // Without tokens the listener is loopback only, and the only thing left between it and a web
        // page is the name the page used: DNS rebinding makes a page's own origin point here.
        if self.config.tokens.is_empty() && (head.header_count("host") != 1 || !crate::admin_api::host_allowed(head.header("host"), &[])) {
            self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
            return Some(error_reply(403, "forbidden_host", "the Host header is not a loopback name"));
        }
        if let Err(denied) = self.authorized(head) {
            return Some(denied);
        }
        if !ingest {
            return None;
        }
        // A request a browser may send cross-origin without asking first has no Content-Type, or a form
        // one; the wire format is neither.
        let kind = head.content_type();
        let encoding = head.header("content-encoding").map(|v| v.trim().to_ascii_lowercase());
        let bad = if kind != "application/x-ndjson" && kind != "application/json" {
            Some(format!("send {WIRE_FORMAT} as application/x-ndjson"))
        } else if !matches!(encoding.as_deref(), None | Some("" | "identity" | "gzip")) {
            Some("Content-Encoding must be gzip or absent".to_string())
        } else {
            None
        };
        bad.map(|why| {
            self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
            error_reply(415, "unsupported_media_type", &why)
        })
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

    /// Everything that needs only the head is decided here, so a refused client is answered before its
    /// body is buffered or inflated: who may talk (Host, token), and in what encoding.
    fn preflight(&self, head: &Incoming) -> Option<Reply> {
        let (counted, ingest) = match (head.method, head.path) {
            (Method::Get, CAPABILITY_PATH) => (false, false),
            (Method::Post, INGEST_PATH) => (true, true),
            (Method::Post, crate::parts::PRESIGN_PATH | crate::parts::COMMIT_PATH) => (true, false),
            _ => return None,
        };
        let refused = self.refuse_head(head, ingest);
        if refused.is_some() && counted {
            self.stats.requests.fetch_add(1, Ordering::Relaxed);
        }
        refused
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
    let mut diff = u8::from(a.len() != b.len());
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
    server: BoundedServer,
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
        // A token that can be guessed online is no better than none once the listener is reachable.
        if !is_loopback(&config.addr) {
            if let Some(weak) = config.tokens.iter().find(|t| t.len() < crate::admin_api::MIN_REMOTE_TOKEN_LEN) {
                return Err(PeerError::WeakToken { addr: config.addr.clone(), len: weak.len() });
            }
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
        // One thread per connection, so `workers` sets how many senders are served at once; the head
        // and body must arrive within `REQUEST_TIMEOUT`, so a client that stalls frees its slot.
        let head = HeadLimits {
            max_connections: (config.workers * CONNECTIONS_PER_WORKER).max(MIN_CONNECTIONS),
            request_timeout: REQUEST_TIMEOUT,
            gzip: true,
            ..HeadLimits::default()
        };
        let server = BoundedServer::start(
            &config.addr,
            "peer",
            Limits { max_body_bytes: config.max_body_bytes, max_decompressed_bytes: config.max_decompressed_bytes },
            head,
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

    // --- hostile input -----------------------------------------------------------------------------
    //
    // These speak raw bytes on a socket: the point is what the receiver does with input a real sender
    // would never produce, so an HTTP client library that tidies the request would hide it.

    use std::io::{Read, Write};
    use std::time::{Duration, Instant};

    /// Sends `request` as is and returns the status line's code (0 when nothing came back before
    /// `wait`) and the whole response text.
    fn exchange(addr: SocketAddr, request: &[u8], wait: Duration) -> (u16, String) {
        let mut conn = std::net::TcpStream::connect(addr).unwrap();
        conn.set_read_timeout(Some(wait)).unwrap();
        // The server may answer and hang up before a big request is fully written.
        let _ = conn.write_all(request);
        let mut out = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match conn.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        let text = String::from_utf8_lossy(&out).into_owned();
        let status = text.split(' ').nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        (status, text)
    }

    fn request(method: &str, path: &str, headers: &[(&str, String)], body: &[u8]) -> Vec<u8> {
        let mut head = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: {}\r\n", body.len());
        for (k, v) in headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        head.push_str("\r\n");
        let mut out = head.into_bytes();
        out.extend_from_slice(body);
        out
    }

    fn ingest_request(headers: &[(&str, String)], body: &[u8]) -> Vec<u8> {
        let mut all = vec![("Content-Type", "application/x-ndjson".to_string())];
        all.extend(headers.iter().cloned());
        request("POST", INGEST_PATH, &all, body)
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        enc.write_all(bytes).unwrap();
        enc.finish().unwrap()
    }

    const WAIT: Duration = Duration::from_secs(5);
    const GZ: (&str, &str) = ("Content-Encoding", "gzip");

    fn gz_header() -> Vec<(&'static str, String)> {
        vec![(GZ.0, GZ.1.to_string())]
    }

    #[test]
    fn a_gzipped_batch_is_accepted_like_a_plain_one() {
        let (rx, got) = start(PeerConfig::default(), false);
        let (status, text) = exchange(rx.local_addr(), &ingest_request(&gz_header(), &gzip(&ndjson(&[record("zipped")]))), WAIT);
        assert_eq!(200, status, "{text}");
        assert_eq!(1, got.lock().unwrap().len());
        rx.shutdown();
    }

    #[test]
    fn an_oversized_body_is_413_whether_declared_or_sent_and_stores_nothing() {
        let (rx, got) = start(PeerConfig { max_body_bytes: 1024, ..PeerConfig::default() }, false);
        let a = rx.local_addr();
        let (status, text) = exchange(a, &ingest_request(&[], &vec![b'x'; 100_000]), WAIT);
        assert_eq!(413, status, "{text}");
        // A declared length far past the limit is refused before any of the body is awaited.
        let lie = b"POST /v1/ingest HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/x-ndjson\r\nContent-Length: 1099511627776\r\nConnection: close\r\n\r\n";
        let started = Instant::now();
        assert_eq!(413, exchange(a, lie, WAIT).0);
        assert!(started.elapsed() < Duration::from_secs(2), "answered from the head, not after a timeout");
        assert!(got.lock().unwrap().is_empty());
        rx.shutdown();
    }

    #[test]
    fn a_gzip_bomb_is_capped_at_the_decompressed_limit() {
        let (rx, got) = start(PeerConfig { max_decompressed_bytes: 64 * 1024, ..PeerConfig::default() }, false);
        // 16 MiB of newlines: blank lines are skipped, so only the size cap can stop it.
        let bomb = gzip(&vec![b'\n'; 16 * 1024 * 1024]);
        assert!(bomb.len() < 64 * 1024, "the compressed form fits under the body limit ({} bytes)", bomb.len());
        let (status, text) = exchange(rx.local_addr(), &ingest_request(&gz_header(), &bomb), WAIT);
        assert_eq!(413, status, "{text}");
        assert!(got.lock().unwrap().is_empty());
        rx.shutdown();
    }

    #[test]
    fn a_truncated_gzip_stream_is_refused_even_when_its_complete_lines_decode() {
        let (rx, got) = start(PeerConfig::default(), false);
        let full = gzip(&ndjson(&(0..200).map(|i| record(&format!("line {i}"))).collect::<Vec<_>>()));
        for cut in [full.len() / 2, full.len() - 8, full.len() - 1, 5] {
            let (status, text) = exchange(rx.local_addr(), &ingest_request(&gz_header(), &full[..cut]), WAIT);
            assert_eq!(400, status, "cut at {cut}: {text}");
        }
        assert!(got.lock().unwrap().is_empty(), "a damaged batch stores nothing");
        rx.shutdown();
    }

    #[test]
    fn bytes_that_are_not_gzip_but_claim_to_be_are_400() {
        let (rx, got) = start(PeerConfig::default(), false);
        for junk in [&b"plain text"[..], &[0x1f, 0x8b][..], &[0x1f, 0x8b, 0x08, 0xff, 0xff][..], &[0u8; 64][..]] {
            let (status, text) = exchange(rx.local_addr(), &ingest_request(&gz_header(), junk), WAIT);
            assert_eq!(400, status, "{junk:?}: {text}");
        }
        assert!(got.lock().unwrap().is_empty());
        rx.shutdown();
    }

    #[test]
    fn malformed_and_partial_ndjson_stores_nothing_and_is_never_a_crash() {
        let (rx, got) = start(PeerConfig::default(), false);
        let good = String::from_utf8(ndjson(&[record("ok")])).unwrap();
        let cut_mid_record = &good[..good.len() / 2];
        let deep = format!("{}{}", "[".repeat(200_000), "]".repeat(200_000));
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("partial last line", format!("{good}\n{cut_mid_record}").into_bytes()),
            ("only half a record", cut_mid_record.as_bytes().to_vec()),
            ("nul bytes", b"{\"a\":\0}\n".to_vec()),
            ("invalid utf-8", [&good.as_bytes()[..10], &[0xff, 0xfe, 0xc0], &good.as_bytes()[10..]].concat()),
            ("a json array, not an object", b"[1,2,3]".to_vec()),
            ("a bare string", b"\"hello\"".to_vec()),
            ("two records on one line", format!("{good}{good}").into_bytes()),
            ("deeply nested json", deep.into_bytes()),
            ("a lone brace", b"{".to_vec()),
        ];
        for (name, body) in cases {
            let (status, text) = exchange(rx.local_addr(), &ingest_request(&[], &body), WAIT);
            assert_eq!(400, status, "{name}: {text}");
            assert!(text.contains("bad_record"), "{name}: {text}");
        }
        assert!(got.lock().unwrap().is_empty(), "no partial batch was kept");
        assert!(rx.stats().bad_request >= 9);
        // Still serving afterwards.
        assert_eq!(200, post(rx.local_addr(), INGEST_PATH, None, &ndjson(&[record("after")])).0);
        rx.shutdown();
    }

    #[test]
    fn crlf_line_endings_and_blank_lines_are_fine() {
        let (rx, got) = start(PeerConfig::default(), false);
        let body = String::from_utf8(ndjson(&[record("a"), record("b")])).unwrap().replace('\n', "\r\n\r\n");
        assert_eq!(200, exchange(rx.local_addr(), &ingest_request(&[], body.as_bytes()), WAIT).0);
        assert_eq!(2, got.lock().unwrap().len());
        rx.shutdown();
    }

    #[test]
    fn only_the_wire_content_types_and_encodings_are_accepted() {
        let (rx, got) = start(PeerConfig::default(), false);
        let body = ndjson(&[record("x")]);
        let a = rx.local_addr();
        for kind in ["text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x", "application/octet-stream", "text/html"] {
            let req = request("POST", INGEST_PATH, &[("Content-Type", kind.to_string())], &body);
            assert_eq!(415, exchange(a, &req, WAIT).0, "{kind}");
        }
        // No Content-Type is what a browser sends for a Blob with no type; it is a CORS-safelisted
        // request, so it must not reach the parser.
        assert_eq!(415, exchange(a, &request("POST", INGEST_PATH, &[], &body), WAIT).0, "no content type");
        for encoding in ["br", "deflate", "compress", "gzip, gzip", "gzip, br", "zstd", "identity, gzip"] {
            let (status, text) = exchange(a, &ingest_request(&[("Content-Encoding", encoding.to_string())], &body), WAIT);
            assert_eq!(415, status, "{encoding}: {text}");
        }
        assert_eq!(200, exchange(a, &ingest_request(&[("Content-Encoding", "identity".to_string())], &body), WAIT).0);
        assert_eq!(1, got.lock().unwrap().len(), "only the identity-encoded request was kept");
        rx.shutdown();
    }

    fn secured() -> (Receiver, Arc<StdMutex<Vec<LogRecord>>>) {
        start(PeerConfig { tokens: vec!["s3cret-token-value".into(), "second-token".into()], ..PeerConfig::default() }, false)
    }

    #[test]
    fn a_missing_wrong_short_long_or_oddly_presented_token_never_ingests() {
        let (rx, got) = secured();
        let a = rx.local_addr();
        let body = ndjson(&[record("x")]);
        let padded = format!("Bearer s3cret-token-value{}", "\0".repeat(256));
        let attempts: Vec<(&str, Option<String>, u16)> = vec![
            ("no header", None, 401),
            ("empty bearer", Some("Bearer ".into()), 401),
            ("wrong scheme", Some("Basic czNjcmV0LXRva2VuLXZhbHVl".into()), 401),
            ("token without a scheme", Some("s3cret-token-value".into()), 401),
            ("wrong token", Some("Bearer nope-nope-nope-nope".into()), 403),
            ("a prefix of the token", Some("Bearer s3cret-token-valu".into()), 403),
            ("one character", Some("Bearer s".into()), 403),
            ("the token plus a suffix", Some("Bearer s3cret-token-value!".into()), 403),
            ("the token plus 256 NULs", Some(padded), 403),
            ("two Authorization headers, the good one first", Some("Bearer s3cret-token-value\r\nAuthorization: Bearer second-token".into()), 401),
            ("the token in another case", Some("Bearer S3CRET-TOKEN-VALUE".into()), 403),
            ("a 64 KiB token", Some(format!("Bearer {}", "a".repeat(8 * 1024))), 403),
        ];
        for (name, auth, want) in attempts {
            let headers: Vec<(&str, String)> = auth.into_iter().map(|v| ("Authorization", v)).collect();
            let (status, text) = exchange(a, &ingest_request(&headers, &body), WAIT);
            assert_eq!(want, status, "{name}: {text}");
        }
        // A token in the URL is not a token.
        let q = request("POST", "/v1/ingest?token=s3cret-token-value&access_token=s3cret-token-value", &[("Content-Type", "application/x-ndjson".into())], &body);
        assert_eq!(401, exchange(a, &q, WAIT).0);
        assert!(got.lock().unwrap().is_empty());
        // Both configured tokens work, and the lower-case scheme.
        for token in ["Bearer s3cret-token-value", "bearer second-token"] {
            let req = ingest_request(&[("Authorization", token.to_string())], &body);
            assert_eq!(200, exchange(a, &req, WAIT).0, "{token}");
        }
        rx.shutdown();
    }

    #[test]
    fn the_token_comparison_looks_at_every_byte_and_cannot_be_fooled_by_length() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(constant_time_eq(b"", b""));
        for other in [&b"abd"[..], b"xbc", b"abcd", b"ab", b"", b"ABC"] {
            assert!(!constant_time_eq(b"abc", other), "{other:?}");
            assert!(!constant_time_eq(other, b"abc"), "{other:?}");
        }
        // A length difference that is a multiple of 256 vanishes if it is narrowed to a byte, and the
        // NUL padding then compares equal to the missing bytes.
        for extra in [256usize, 512, 65_536] {
            let padded = [&b"abc"[..], &vec![0u8; extra]].concat();
            assert!(!constant_time_eq(b"abc", &padded), "+{extra} NULs");
            assert!(!constant_time_eq(&padded, b"abc"), "+{extra} NULs");
        }
    }

    #[test]
    fn unauthenticated_requests_are_answered_from_the_head_before_any_body_is_buffered() {
        let (rx, got) = secured();
        let a = rx.local_addr();
        // Declares 3 MiB and sends none of it: a server that reads the body first waits for it.
        let head = b"POST /v1/ingest HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/x-ndjson\r\nContent-Length: 3145728\r\nConnection: close\r\n\r\n";
        let started = Instant::now();
        assert_eq!(401, exchange(a, head, WAIT).0);
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        // A bomb with no token learns nothing about the size limits: 401, not 413, and it is never inflated.
        let bomb = gzip(&vec![b'\n'; 64 * 1024 * 1024]);
        let (status, text) = exchange(a, &ingest_request(&gz_header(), &bomb), WAIT);
        assert_eq!(401, status, "{text}");
        let oversized = ingest_request(&[], &vec![b'x'; 5 * 1024 * 1024]);
        assert_eq!(401, exchange(a, &oversized, WAIT).0);
        assert!(got.lock().unwrap().is_empty());
        rx.shutdown();
    }

    #[test]
    fn a_client_that_never_finishes_its_body_does_not_use_up_the_receiver() {
        let (rx, got) = start(PeerConfig { workers: 2, ..PeerConfig::default() }, false);
        let a = rx.local_addr();
        // More stalled senders than there are workers: each has sent a head and ten bytes of 100000.
        let stalled: Vec<std::net::TcpStream> = (0..6)
            .map(|_| {
                let mut c = std::net::TcpStream::connect(a).unwrap();
                c.write_all(b"POST /v1/ingest HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/x-ndjson\r\nContent-Length: 100000\r\n\r\n0123456789").unwrap();
                c
            })
            .collect();
        std::thread::sleep(Duration::from_millis(200));
        let started = Instant::now();
        let (status, text) = exchange(a, &ingest_request(&[], &ndjson(&[record("still served")])), Duration::from_secs(3));
        assert_eq!(200, status, "{text}");
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(1, got.lock().unwrap().len());
        drop(stalled);
        rx.shutdown();
    }

    #[test]
    fn huge_or_numerous_header_lines_are_refused_with_431_and_the_receiver_keeps_serving() {
        let (rx, got) = start(PeerConfig::default(), false);
        let a = rx.local_addr();
        let big = "a".repeat(2 * 1024 * 1024);
        let one_line = request("GET", CAPABILITY_PATH, &[("X-Big", big.clone())], b"");
        assert_eq!(431, exchange(a, &one_line, WAIT).0, "one 2 MiB header");
        let many: Vec<(&str, String)> = (0..200).map(|_| ("X-A", "b".to_string())).collect();
        assert_eq!(431, exchange(a, &request("GET", CAPABILITY_PATH, &many, b""), WAIT).0, "200 headers");
        let long_target = format!("/{}", "p".repeat(200_000));
        assert_eq!(431, exchange(a, &request("GET", &long_target, &[], b""), WAIT).0, "200 kB request line");
        // A head that never ends is cut off at the budget instead of being buffered.
        let endless = [b"GET / HTTP/1.1\r\nX: ".to_vec(), vec![b'z'; 4 * 1024 * 1024]].concat();
        assert_eq!(431, exchange(a, &endless, WAIT).0, "head with no end");
        assert_eq!(200, get(a, CAPABILITY_PATH, None).0);
        assert!(got.lock().unwrap().is_empty());
        rx.shutdown();
    }

    #[test]
    fn framing_tricks_are_refused_rather_than_guessed_at() {
        let (rx, got) = start(PeerConfig::default(), false);
        let a = rx.local_addr();
        let line = String::from_utf8(ndjson(&[record("smuggled")])).unwrap();
        let chunked = format!("POST /v1/ingest HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{line}\r\n0\r\n\r\n", line.len());
        assert_eq!(501, exchange(a, chunked.as_bytes(), WAIT).0, "chunked");
        let both = format!("POST /v1/ingest HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-ndjson\r\nContent-Length: {}\r\nTransfer-Encoding: chunked\r\n\r\n{line}", line.len());
        assert_eq!(501, exchange(a, both.as_bytes(), WAIT).0, "length and chunked together");
        let two = format!("POST /v1/ingest HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-ndjson\r\nContent-Length: {}\r\nContent-Length: 2\r\n\r\n{line}", line.len());
        assert_eq!(400, exchange(a, two.as_bytes(), WAIT).0, "two Content-Length");
        for bad in ["-1", "+5", "1e3", "0x10", " ", "99999999999999999999999"] {
            let req = format!("POST /v1/ingest HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-ndjson\r\nContent-Length: {bad}\r\n\r\n{line}");
            let (status, text) = exchange(a, req.as_bytes(), WAIT);
            assert!(matches!(status, 400 | 413), "Content-Length {bad:?}: {text}");
        }
        // A second request pipelined behind the first is not read as a request.
        let pipelined = [ingest_request(&[], line.as_bytes()), ingest_request(&[], line.as_bytes())].concat();
        assert_eq!(200, exchange(a, &pipelined, WAIT).0);
        assert_eq!(1, got.lock().unwrap().len(), "the first request only");
        rx.shutdown();
    }

    #[test]
    fn only_the_documented_method_and_exact_path_reach_a_handler() {
        let (rx, got) = secured();
        let a = rx.local_addr();
        let auth = [("Authorization", "Bearer s3cret-token-value".to_string()), ("Content-Type", "application/x-ndjson".to_string())];
        let body = ndjson(&[record("x")]);
        for path in ["/v1/ingest/", "//v1/ingest", "/v1//ingest", "/V1/INGEST", "/v1/./ingest", "/v1/x/../ingest", "/v1/%69ngest", "/v1/ingest%2f", "/v1/ingest%00", "/v1/ingest;x", "http://127.0.0.1/v1/ingest", "/ingest", "*"] {
            let (status, text) = exchange(a, &request("POST", path, &auth, &body), WAIT);
            assert!(matches!(status, 400 | 404), "{path}: {status} {text}");
        }
        for (method, path, want) in [
            ("GET", INGEST_PATH, 405), ("PUT", INGEST_PATH, 404), ("DELETE", INGEST_PATH, 404), ("PATCH", INGEST_PATH, 404),
            ("HEAD", INGEST_PATH, 404), ("OPTIONS", INGEST_PATH, 404), ("TRACE", INGEST_PATH, 404), ("CONNECT", INGEST_PATH, 404),
            ("POST", CAPABILITY_PATH, 405), ("DELETE", CAPABILITY_PATH, 404),
        ] {
            let (status, text) = exchange(a, &request(method, path, &auth, &body), WAIT);
            assert_eq!(want, status, "{method} {path}: {text}");
            assert!(!text.to_ascii_lowercase().contains("access-control-"), "{method} {path} must not grant cross-origin access");
        }
        // A query string does not change what a path is.
        assert_eq!(200, exchange(a, &request("POST", "/v1/ingest?x=1&y=2", &auth, &body), WAIT).0);
        assert_eq!(1, got.lock().unwrap().len());
        rx.shutdown();
    }

    #[test]
    fn a_tokenless_loopback_listener_refuses_a_foreign_host_header() {
        // DNS rebinding: a web page re-points its own name at 127.0.0.1 and then talks to the listener
        // as same-origin. Its requests carry its own name in Host.
        let (rx, got) = start(PeerConfig::default(), false);
        let a = rx.local_addr();
        let body = ndjson(&[record("x")]);
        for host in ["evil.example", "evil.example:9300", "127.0.0.1.evil.example", "localhost:80@evil.example", ""] {
            let req = format!("POST /v1/ingest HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-ndjson\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
            let (status, text) = exchange(a, &[req.as_bytes(), &body].concat(), WAIT);
            assert_eq!(403, status, "Host {host:?}: {text}");
        }
        let no_host = [&b"GET /.well-known/logless HTTP/1.1\r\nConnection: close\r\n\r\n"[..]].concat();
        assert_eq!(403, exchange(a, &no_host, WAIT).0, "no Host at all");
        assert!(got.lock().unwrap().is_empty());
        for host in ["127.0.0.1", "localhost:9300", "[::1]:9300"] {
            let req = format!("GET /.well-known/logless HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
            assert_eq!(200, exchange(a, req.as_bytes(), WAIT).0, "Host {host}");
        }
        rx.shutdown();
    }

    #[test]
    fn the_capability_document_does_not_leak_secrets() {
        let (rx, _) = secured();
        let (status, text) = exchange(rx.local_addr(), &request("GET", CAPABILITY_PATH, &[("Authorization", "Bearer second-token".into())], b""), WAIT);
        assert_eq!(200, status);
        assert!(!text.contains("s3cret-token-value") && !text.contains("second-token"), "{text}");
        rx.shutdown();
    }

    #[test]
    fn a_logged_config_never_carries_a_token() {
        let config = PeerConfig { tokens: vec!["SENTINEL-TOKEN-ONE".into(), "SENTINEL-TOKEN-TWO".into()], ..PeerConfig::default() };
        for shown in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!shown.contains("SENTINEL"), "{shown}");
            assert!(shown.contains("tokens"), "the field is still named, with a count: {shown}");
        }
    }


    #[test]
    fn a_short_token_is_refused_on_a_non_loopback_bind_only() {
        let weak = |addr: &str, tokens: Vec<String>| {
            Receiver::start(&PeerConfig { enabled: true, addr: addr.into(), tokens, ..PeerConfig::default() }, "x".into(), |_| Admitted::Accepted).err()
        };
        let long = "t".repeat(crate::admin_api::MIN_REMOTE_TOKEN_LEN);
        assert!(matches!(weak("0.0.0.0:0", vec!["short".into()]), Some(PeerError::WeakToken { len: 5, .. })));
        assert!(matches!(weak("0.0.0.0:0", vec![long.clone(), "short".into()]), Some(PeerError::WeakToken { len: 5, .. })), "every token counts");
        assert!(matches!(weak("0.0.0.0:0", vec![String::new()]), Some(PeerError::WeakToken { len: 0, .. })), "an empty token is not a token");
        assert!(weak("0.0.0.0:0", vec![long]).is_none());
        assert!(weak("127.0.0.1:0", vec!["short".into()]).is_none(), "loopback is for local testing");
    }

}
