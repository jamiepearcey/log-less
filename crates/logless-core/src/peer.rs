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
//! Provenance is read strictly: a hops value that is not a whole number from 0 up, an origin that is not
//! a non-empty string, or either attribute twice, is refused as a loop (counted with the loops), so a
//! forged `logless.hops = "0"` or `-100` cannot buy a record more hops.
//!
//! # Idempotency
//!
//! A bounded window of recently accepted event ids ([`DedupeWindow`]) drops a record seen again. The agent
//! keeps it in [`DEDUPE_FILE`] in the data dir, so it survives a restart: the ids of each admitted batch
//! are appended and synced in one write after the pipeline took the batch and before the 200 reply, and
//! the file is compacted to the window once it holds twice that many. A crash between the pipeline taking
//! a batch and its ids being saved means the sender, which never heard 200, resends it and the receiver
//! takes it again; a crash after the ids are saved but before the reply means the resend is reported as
//! duplicates. A retry older than the window (`dedupe_window` records) can still duplicate: at-least-once,
//! stated rather than hidden.
//!
//! "The pipeline took it" is only as durable as the sink. In the agent the sink is a bounded in-memory
//! channel ahead of the WAL, so the 200 (and the saved ids) can precede the WAL write: a crash in that
//! window loses the batch, and with the ids saved a resend would be dropped as a duplicate too. Closing it
//! means replying only after the WAL synced the batch, as HEC acks do; `docs/peer-sync.md` tracks it.

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
    #[error("the peer dedupe log could not be opened: {0}")]
    Dedupe(std::io::Error),
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

/// Recently accepted event ids, oldest forgotten first. With a log ([`DedupeWindow::open`]) the window
/// survives a restart: each admitted batch's ids are appended to the file as raw 16-byte ids and synced
/// in one write, and the file is rewritten to the window alone once it holds twice the window.
#[derive(Debug)]
pub struct DedupeWindow {
    capacity: usize,
    inner: Mutex<WindowState>,
}

#[derive(Debug, Default)]
struct WindowState {
    set: HashSet<Uuid>,
    order: VecDeque<Uuid>,
    log: Option<IdLog>,
}

#[derive(Debug)]
struct IdLog {
    path: PathBuf,
    file: std::fs::File,
    /// Ids in the file, current window or forgotten.
    ids: usize,
}

/// The receiver's id log in the data dir.
pub const DEDUPE_FILE: &str = "peer-dedupe.ids";

impl DedupeWindow {
    /// Memory only: forgotten by a restart.
    pub fn new(capacity: usize) -> Self {
        Self { capacity: capacity.max(1), inner: Mutex::new(WindowState::default()) }
    }

    /// Loads the last `capacity` ids from `path` (a torn tail from a crash mid-append is cut off) and appends
    /// every id admitted from now on.
    pub fn open(capacity: usize, path: &Path) -> std::io::Result<Self> {
        use std::io::{Read, Seek};
        let window = Self::new(capacity);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new().read(true).append(true).create(true).open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let whole = bytes.len() / 16 * 16;
        if whole != bytes.len() {
            file.set_len(whole as u64)?;
            file.seek(std::io::SeekFrom::End(0))?;
        }
        let ids = whole / 16;
        {
            let mut state = window.state();
            for chunk in bytes[..whole].chunks_exact(16).skip(ids.saturating_sub(window.capacity)) {
                window.remember(&mut state, Uuid::from_bytes(chunk.try_into().expect("16 bytes")));
            }
            state.log = Some(IdLog { path: path.to_path_buf(), file, ids });
        }
        Ok(window)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, WindowState> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn remember(&self, state: &mut WindowState, id: Uuid) -> bool {
        if !state.set.insert(id) {
            return false;
        }
        state.order.push_back(id);
        while state.order.len() > self.capacity {
            if let Some(old) = state.order.pop_front() {
                state.set.remove(&old);
            }
        }
        true
    }

    pub fn contains(&self, id: &Uuid) -> bool {
        self.state().set.contains(id)
    }

    /// Remembers `ids`, and with a log makes them durable before returning (one append and one sync for
    /// the batch). An error means the ids are remembered by this process but may be forgotten by a restart.
    pub fn insert_all<'a>(&self, ids: impl IntoIterator<Item = &'a Uuid>) -> std::io::Result<()> {
        use std::io::Write;
        let mut guard = self.state();
        let mut fresh: Vec<u8> = Vec::new();
        for id in ids {
            if self.remember(&mut guard, *id) && guard.log.is_some() {
                fresh.extend_from_slice(id.as_bytes());
            }
        }
        let state = &mut *guard;
        let Some(log) = state.log.as_mut() else { return Ok(()) };
        if fresh.is_empty() {
            return Ok(());
        }
        log.file.write_all(&fresh)?;
        log.file.sync_data()?;
        log.ids += fresh.len() / 16;
        if log.ids > self.capacity.saturating_mul(2) {
            compact(log, &state.order)?;
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.state().order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Rewrites the log to the current window (temporary file, sync, rename, sync the directory), so it never
/// holds more than twice the window. A crash leaves the old file or the new one.
fn compact(log: &mut IdLog, window: &VecDeque<Uuid>) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = log.path.with_extension("tmp");
    {
        let mut out = std::fs::File::create(&tmp)?;
        let mut bytes = Vec::with_capacity(window.len() * 16);
        for id in window {
            bytes.extend_from_slice(id.as_bytes());
        }
        out.write_all(&bytes)?;
        out.sync_all()?;
    }
    std::fs::rename(&tmp, &log.path)?;
    if let Some(dir) = log.path.parent().and_then(|d| std::fs::File::open(d).ok()) {
        let _ = dir.sync_all();
    }
    log.file = std::fs::OpenOptions::new().append(true).open(&log.path)?;
    log.ids = window.len();
    Ok(())
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
    /// Accepted batches whose ids could not be saved to the dedupe log.
    pub dedupe_unsaved: AtomicU64,
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
    pub dedupe_unsaved: u64,
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
            dedupe_unsaved: load(&self.dedupe_unsaved),
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
            // Remembered only after the pipeline took them, and durably before the reply: a sender that hears
            // 200 has moved on, so a restart must still recognise a late copy (see "Idempotency" above).
            if let Err(e) = self.dedupe.insert_all(ids.iter()) {
                self.stats.dedupe_unsaved.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(error = %e, "could not save accepted event ids; a restart may store a resent copy");
            }
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

/// A record's provenance, read strictly. `None` means it was forged or mangled: a hops count that is not a
/// whole number from 0 up (as an integer or a decimal string), an origin that is not a non-empty string,
/// or either attribute present twice. Such a record has no trustworthy hop count, so it is treated as a
/// loop rather than given a fresh budget of hops.
fn provenance(record: &LogRecord) -> Option<(Option<&str>, u32)> {
    let (mut origin, mut hops) = (None, None);
    for attr in &record.attributes {
        if attr.key == ATTR_ORIGIN {
            let AttrValue::Str(value) = &attr.value else { return None };
            if value.is_empty() || origin.replace(value.as_str()).is_some() {
                return None;
            }
        } else if attr.key == ATTR_HOPS {
            let n = match &attr.value {
                AttrValue::I64(n) => u32::try_from(*n).ok()?,
                AttrValue::Str(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) => s.parse().ok()?,
                _ => return None,
            };
            if hops.replace(n).is_some() {
                return None;
            }
        }
    }
    Some((origin, hops.unwrap_or(0)))
}

/// A record that came back to the instance that first sent it, has travelled too far, or carries
/// provenance that does not parse (see [`provenance`]).
pub fn is_loop(record: &LogRecord, own_instance: &str) -> bool {
    match provenance(record) {
        Some((origin, hops)) => origin == Some(own_instance) || hops >= MAX_HOPS,
        None => true,
    }
}

/// Stamps a record for sending: the first sender's id (kept by later hops) and one more hop, as exactly one
/// attribute each. Senders skip what [`is_loop`] refuses, so this normally sees valid provenance; given
/// a forged one anyway it writes [`MAX_HOPS`], which the next receiver refuses, rather than a count that
/// would buy the record more hops.
pub fn stamp_for_send(record: &mut LogRecord, own_instance: &str) {
    let (origin, hops) = match provenance(record) {
        Some((origin, hops)) => (origin.unwrap_or(own_instance).to_string(), hops.saturating_add(1).min(MAX_HOPS)),
        None => (own_instance.to_string(), MAX_HOPS),
    };
    record.attributes.retain(|attr| attr.key != ATTR_HOPS && attr.key != ATTR_ORIGIN);
    record.attributes.push(Attr { key: ATTR_ORIGIN.into(), value: AttrValue::Str(origin) });
    record.attributes.push(Attr { key: ATTR_HOPS.into(), value: AttrValue::I64(i64::from(hops)) });
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
        Self::start_in(config, instance, catalog, None, sink)
    }

    /// As [`Receiver::start_with_catalog`], with the dedupe window kept in [`DEDUPE_FILE`] under `data_dir`
    /// so it survives a restart. This is how the agent runs it.
    pub fn start_in<F>(config: &PeerConfig, instance: String, catalog: Option<PathBuf>, data_dir: Option<&Path>, sink: F) -> Result<Self, PeerError>
    where
        F: Fn(Vec<LogRecord>) -> Admitted + Send + Sync + 'static,
    {
        let dedupe = match data_dir {
            Some(dir) => DedupeWindow::open(config.dedupe_window, &dir.join(DEDUPE_FILE)).map_err(PeerError::Dedupe)?,
            None => DedupeWindow::new(config.dedupe_window),
        };
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
            dedupe,
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
    use std::io::Write as _;
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
        w.insert_all(ids.iter()).unwrap();
        assert!(!w.contains(&ids[0]));
        assert!(w.contains(&ids[1]) && w.contains(&ids[2]));
        assert_eq!(2, w.len());
    }

    fn with_attr(body: &str, key: &str, value: AttrValue) -> LogRecord {
        let mut r = record(body);
        r.attributes.push(Attr { key: key.into(), value });
        r
    }

    #[test]
    fn forged_or_mangled_provenance_is_refused_as_a_loop() {
        let (rx, got) = start(PeerConfig::default(), false);
        let forged = vec![
            with_attr("garbage hops", ATTR_HOPS, AttrValue::Str("lots".into())),
            with_attr("negative hops", ATTR_HOPS, AttrValue::I64(-5)),
            with_attr("negative string", ATTR_HOPS, AttrValue::Str("-1".into())),
            with_attr("signed string", ATTR_HOPS, AttrValue::Str("+1".into())),
            with_attr("float hops", ATTR_HOPS, AttrValue::F64(1.0)),
            with_attr("bool hops", ATTR_HOPS, AttrValue::Bool(false)),
            with_attr("huge hops", ATTR_HOPS, AttrValue::I64(i64::MAX)),
            with_attr("number origin", ATTR_ORIGIN, AttrValue::I64(7)),
            with_attr("empty origin", ATTR_ORIGIN, AttrValue::Str(String::new())),
        ];
        // A second hops attribute must not reset a chain that has run out: the old code took the last one.
        let mut reset = with_attr("reset", ATTR_HOPS, AttrValue::I64(MAX_HOPS as i64));
        reset.attributes.push(Attr { key: ATTR_HOPS.into(), value: AttrValue::I64(0) });
        let mut two_origins = with_attr("two origins", ATTR_ORIGIN, AttrValue::Str("self0001".into()));
        two_origins.attributes.push(Attr { key: ATTR_ORIGIN.into(), value: AttrValue::Str("elsewhere".into()) });
        let fine = with_attr("fine", ATTR_HOPS, AttrValue::Str("3".into()));
        let mut batch = forged.clone();
        batch.extend([reset, two_origins, fine.clone()]);
        let (status, body) = post(rx.local_addr(), INGEST_PATH, None, &ndjson(&batch));
        assert_eq!(200, status, "{body}");
        assert_eq!((1, batch.len() as u64 - 1), (body["accepted"].as_u64().unwrap(), body["loops"].as_u64().unwrap()), "{body}");
        assert_eq!(vec![fine], *got.lock().unwrap());
        assert_eq!(batch.len() as u64 - 1, rx.stats().loops);
        rx.shutdown();
    }

    #[test]
    fn stamping_never_gives_a_forged_record_a_fresh_budget_of_hops() {
        let hops_of = |r: &LogRecord| -> Vec<AttrValue> { r.attributes.iter().filter(|a| a.key == ATTR_HOPS).map(|a| a.value.clone()).collect() };
        for value in [AttrValue::I64(-5), AttrValue::Str("junk".into()), AttrValue::Str("-3".into()), AttrValue::Bool(true)] {
            let mut r = with_attr("forged", ATTR_HOPS, value.clone());
            stamp_for_send(&mut r, "edge");
            assert_eq!(vec![AttrValue::I64(MAX_HOPS as i64)], hops_of(&r), "{value:?}");
            assert!(is_loop(&r, "central"), "the next receiver refuses it: {value:?}");
        }
        let mut deep = with_attr("deep", ATTR_HOPS, AttrValue::I64(MAX_HOPS as i64 + 100));
        stamp_for_send(&mut deep, "edge");
        assert_eq!(vec![AttrValue::I64(MAX_HOPS as i64)], hops_of(&deep), "clamped, not incremented past the limit");
        let mut text = with_attr("text", ATTR_HOPS, AttrValue::Str("2".into()));
        stamp_for_send(&mut text, "edge");
        assert_eq!(vec![AttrValue::I64(3)], hops_of(&text), "a valid decimal string still counts");
        let mut bad_origin = with_attr("bad origin", ATTR_ORIGIN, AttrValue::I64(1));
        stamp_for_send(&mut bad_origin, "edge");
        let origins: Vec<_> = bad_origin.attributes.iter().filter(|a| a.key == ATTR_ORIGIN).map(|a| a.value.clone()).collect();
        assert_eq!(vec![AttrValue::Str("edge".into())], origins, "one origin, a string");
        assert!(is_loop(&bad_origin, "central"));
    }

    #[test]
    fn a_batch_replayed_after_the_receiver_restarts_is_reported_as_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let config = PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), dedupe_window: 4, ..PeerConfig::default() };
        let got = Arc::new(StdMutex::new(Vec::new()));
        let start_durable = || {
            let sink = Arc::clone(&got);
            Receiver::start_in(&config, "self0001".into(), None, Some(dir.path()), move |records| {
                sink.lock().unwrap().extend(records);
                Admitted::Accepted
            })
            .unwrap()
        };
        let old = vec![record("a"), record("b")];
        let first = start_durable();
        assert_eq!(2, post(first.local_addr(), INGEST_PATH, None, &ndjson(&old)).1["accepted"]);
        first.shutdown();

        let second = start_durable();
        let (status, again) = post(second.local_addr(), INGEST_PATH, None, &ndjson(&old));
        assert_eq!(200, status);
        assert_eq!((0, 2), (again["accepted"].as_u64().unwrap(), again["duplicates"].as_u64().unwrap()), "{again}");
        assert_eq!(2, got.lock().unwrap().len(), "stored once across the restart");

        // The window stays bounded: twelve more ids, one request each, push "a" and "b" out and the file
        // is compacted rather than growing with every id ever accepted.
        let newer: Vec<LogRecord> = (0..12).map(|i| record(&format!("n{i}"))).collect();
        for r in &newer {
            assert_eq!(1, post(second.local_addr(), INGEST_PATH, None, &ndjson(std::slice::from_ref(r))).1["accepted"]);
        }
        let log_len = std::fs::metadata(dir.path().join(DEDUPE_FILE)).unwrap().len();
        assert!(log_len <= 2 * 4 * 16, "the log holds at most twice the window: {log_len} bytes");
        assert_eq!(0, log_len % 16);
        second.shutdown();

        // A crash mid-append leaves a torn id at the end: it is cut off, not misread.
        std::fs::OpenOptions::new().append(true).open(dir.path().join(DEDUPE_FILE)).unwrap().write_all(&[0xAB; 7]).unwrap();
        let third = start_durable();
        let last = ndjson(&newer[8..]);
        let (_, recent) = post(third.local_addr(), INGEST_PATH, None, &last);
        assert_eq!((0, 4), (recent["accepted"].as_u64().unwrap(), recent["duplicates"].as_u64().unwrap()), "the last window is remembered: {recent}");
        let (_, forgotten) = post(third.local_addr(), INGEST_PATH, None, &ndjson(&old[..1]));
        assert_eq!(1, forgotten["accepted"], "older than the window: accepted again, as documented");
        third.shutdown();
    }

    #[test]
    fn the_instance_id_is_created_once_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let first = instance_id(dir.path());
        assert_eq!(32, first.len());
        assert_eq!(first, instance_id(dir.path()));
    }
}
