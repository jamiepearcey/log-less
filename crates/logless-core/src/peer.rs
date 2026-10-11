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
//! # Acknowledgement
//!
//! A 200 means **the WAL has these records on disk** (`fdatasync`ed, not merely written): the sender
//! commits its spool cursor on a 200 and forgets the batch, so anything weaker turns a crash into loss.
//! In the agent the sink only puts the batch on a bounded channel ahead of the WAL writer, so it answers
//! [`Admitted::Pending`] with a [`Ticket`] from [`WalAcks`], and the request thread waits:
//!
//! 1. the sink [`WalAcks::issue`]s a ticket and queues the batch (a full queue is [`Admitted::Busy`]: 503);
//! 2. the record path [`WalAcks::bind`]s the ticket to the WAL record count that covers the batch, as the
//!    HEC ack table does (`crate::hec::AckTable`), once the batch is submitted;
//! 3. the WAL writer [`WalAcks::publish`]es its synced record count after each `fdatasync`;
//! 4. the request thread wakes, saves the ids to the dedupe log, and answers 200.
//!
//! The wait is the next fsync, so at most `wal_fsync_interval` (100 ms by default) plus a writer pass. It
//! is bounded by `ack_wait_ms`: past it the answer is 503 with `Retry-After`, and the sender resends. The
//! batch is still in the pipeline, so its ticket is kept: once it does sync, the next request saves its
//! ids ([`WalAcks::settle`]) and the resend is reported as duplicates rather than stored twice (unless the
//! resend arrives before that sync, see below). Waits, timeouts and late settlements are in [`PeerStats`].
//!
//! # Idempotency and crash ordering
//!
//! A bounded window of recently accepted event ids ([`DedupeWindow`]) drops a record seen again. The agent
//! keeps it in [`DEDUPE_FILE`] in the data dir, so it survives a restart: the ids of each admitted batch
//! are appended and synced in one write, and the file is compacted to the window once it holds twice that
//! many. The order is **WAL synced, then ids saved, then 200**, and a crash between any two steps loses
//! nothing:
//!
//! * before the WAL sync: the sender never heard 200 and resends, and the ids were never saved, so the
//!   resend is taken as new. No loss. If an unsynced WAL write survived (a process crash keeps the page
//!   cache, a power loss does not), the restart replays it too: one duplicate copy.
//! * after the WAL sync, before the ids are saved: the WAL replays the batch on restart *and* the sender
//!   resends it, which is taken as new: one duplicate copy in the store (the store does not dedupe by
//!   event id). The same holds for a resend after a 503 timeout that beats the batch's late sync.
//! * after the ids are saved, before the 200: the resend is reported as duplicates and committed.
//!
//! A retry older than the window (`dedupe_window` records) can still duplicate: at-least-once, stated
//! rather than hidden. A sink that returns [`Admitted::Accepted`] says the batch is already durable (tests,
//! and any sink that stores synchronously).

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

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
    /// How long an ingest request waits for the WAL to sync its batch before answering 503 (see
    /// "Acknowledgement"). Well above `wal_fsync_interval`, well below the sender's 15 s request timeout.
    #[serde(default = "default_ack_wait_ms")]
    pub ack_wait_ms: u64,
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
fn default_ack_wait_ms() -> u64 {
    5_000
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
            ack_wait_ms: default_ack_wait_ms(),
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
            .field("ack_wait_ms", &self.ack_wait_ms)
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

/// What the pipeline did with a batch. A full pipeline is a 503 and a retry, as for the other receivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admitted {
    /// Taken and already durable: the sink stored it before returning.
    Accepted,
    /// Taken into a pipeline ahead of the WAL: durable once the ticket's [`WalAcks`] says so.
    Pending(Ticket),
    Busy,
}

/// One batch on its way to the WAL, from [`WalAcks::issue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ticket(u64);

/// Tickets whose request gave up waiting (503) but whose batch may still reach the WAL. Each holds a batch's
/// ids, so the list is capped; one evicted early only costs a duplicate if its sender resends.
const MAX_ABANDONED: usize = 1024;

#[derive(Debug, Default)]
struct TicketState {
    /// The WAL record count that covers the batch, once the record path has submitted it.
    watermark: Option<u64>,
    /// Set when the request answered 503: the ids to save if the batch syncs after all.
    abandoned: Option<Vec<Uuid>>,
}

#[derive(Debug, Default)]
struct AcksInner {
    tickets: HashMap<u64, TicketState>,
    abandoned: VecDeque<u64>,
    closed: bool,
}

/// How a peer request learns that the WAL has its batch (see "Acknowledgement" in the module docs). Shared
/// by the peer sink (issue), the record path (bind), the WAL writer (publish) and the request threads (wait).
#[derive(Debug)]
pub struct WalAcks {
    /// Records the WAL has `fdatasync`ed. The same counter HEC acks are judged against.
    synced: Arc<AtomicU64>,
    bound: Duration,
    next: AtomicU64,
    inner: Mutex<AcksInner>,
    changed: Condvar,
}

/// How a wait for the WAL ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalWait {
    Synced(Duration),
    /// The bound passed first; the ticket is still held until [`WalAcks::abandon`].
    TimedOut(Duration),
    /// The process is shutting down and will not drain the pipeline for this batch.
    Closed,
}

impl WalAcks {
    /// `synced` is the WAL writer's synced record count; `bound` caps each wait.
    pub fn new(synced: Arc<AtomicU64>, bound: Duration) -> Arc<Self> {
        Arc::new(Self { synced, bound, next: AtomicU64::new(1), inner: Mutex::new(AcksInner::default()), changed: Condvar::new() })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AcksInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// A ticket for a batch the sink is about to queue.
    pub fn issue(&self) -> Ticket {
        let seq = self.next.fetch_add(1, Ordering::Relaxed);
        self.lock().tickets.insert(seq, TicketState::default());
        Ticket(seq)
    }

    /// The batch never made it into the queue.
    pub fn forget(&self, ticket: Ticket) {
        self.lock().tickets.remove(&ticket.0);
    }

    /// Called by the record path once the batch is submitted: it is durable when `synced` reaches
    /// `records_submitted_total`. Submission order is the order the WAL writer appends in.
    pub fn bind(&self, ticket: Ticket, records_submitted_total: u64) {
        let mut inner = self.lock();
        if let Some(t) = inner.tickets.get_mut(&ticket.0) {
            t.watermark = Some(records_submitted_total);
        }
        drop(inner);
        self.changed.notify_all();
    }

    /// Called by the WAL writer after each `fdatasync`, with the record count now on disk.
    pub fn publish(&self, synced_records: u64) {
        self.synced.store(synced_records, Ordering::Release);
        // Taken so a waiter between its check and its wait cannot miss the change.
        drop(self.lock());
        self.changed.notify_all();
    }

    /// Wakes every waiter with [`WalWait::Closed`]: the pipeline is being torn down.
    pub fn close(&self) {
        self.lock().closed = true;
        self.changed.notify_all();
    }

    pub fn bound(&self) -> Duration {
        self.bound
    }

    fn durable(&self, t: &TicketState) -> bool {
        t.watermark.is_some_and(|w| self.synced.load(Ordering::Acquire) >= w)
    }

    /// Blocks until the WAL has synced the ticket's batch, the bound passes, or [`WalAcks::close`].
    pub fn wait(&self, ticket: Ticket) -> WalWait {
        let started = Instant::now();
        let deadline = started + self.bound;
        let mut inner = self.lock();
        loop {
            if inner.closed {
                inner.tickets.remove(&ticket.0);
                return WalWait::Closed;
            }
            if inner.tickets.get(&ticket.0).is_some_and(|t| self.durable(t)) {
                inner.tickets.remove(&ticket.0);
                return WalWait::Synced(started.elapsed());
            }
            let now = Instant::now();
            if now >= deadline {
                return WalWait::TimedOut(started.elapsed());
            }
            inner = self.changed.wait_timeout(inner, deadline - now).unwrap_or_else(|p| p.into_inner()).0;
        }
    }

    /// The request answered 503 for a ticket that may still sync: keep its ids for [`WalAcks::settle`].
    pub fn abandon(&self, ticket: Ticket, ids: Vec<Uuid>) {
        let mut inner = self.lock();
        let Some(t) = inner.tickets.get_mut(&ticket.0) else { return };
        t.abandoned = Some(ids);
        inner.abandoned.push_back(ticket.0);
        while inner.abandoned.len() > MAX_ABANDONED {
            if let Some(old) = inner.abandoned.pop_front() {
                inner.tickets.remove(&old);
            }
        }
    }

    /// The ids of abandoned batches that have since synced. They are durable now, so saving them makes a
    /// resend count as duplicates instead of a second copy.
    pub fn settle(&self) -> Vec<Uuid> {
        let mut inner = self.lock();
        if inner.abandoned.is_empty() {
            return Vec::new();
        }
        let AcksInner { tickets, abandoned, .. } = &mut *inner;
        let mut ids = Vec::new();
        abandoned.retain(|seq| {
            if !tickets.get(seq).is_some_and(|t| self.durable(t)) {
                return true;
            }
            if let Some(t) = tickets.remove(seq) {
                ids.extend(t.abandoned.unwrap_or_default());
            }
            false
        });
        ids
    }

    /// Tickets issued and not yet answered or settled.
    pub fn outstanding(&self) -> usize {
        self.lock().tickets.len()
    }
}

#[derive(Debug, Default)]
pub struct PeerStats {
    pub requests: AtomicU64,
    pub records: AtomicU64,
    pub accepted: AtomicU64,
    pub duplicates: AtomicU64,
    pub loops: AtomicU64,
    /// 503s because the pipeline queue was full.
    pub busy: AtomicU64,
    pub unauthorized: AtomicU64,
    pub bad_request: AtomicU64,
    /// Accepted batches whose ids could not be saved to the dedupe log.
    pub dedupe_unsaved: AtomicU64,
    /// Batches that waited for the WAL to sync before their 200, and the time they spent waiting.
    pub wal_waits: AtomicU64,
    pub wal_wait_us_total: AtomicU64,
    pub wal_wait_us_max: AtomicU64,
    /// 503s because the WAL did not sync the batch within `ack_wait_ms`: a slow disk.
    pub wal_timeouts: AtomicU64,
    /// 503s because the process was shutting down with the batch not yet on disk.
    pub wal_closed: AtomicU64,
    /// Ids of timed-out batches saved after their late sync (their resends count as duplicates).
    pub settled_late: AtomicU64,
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
    pub wal_waits: u64,
    pub wal_wait_us_total: u64,
    pub wal_wait_us_max: u64,
    pub wal_timeouts: u64,
    pub wal_closed: u64,
    pub settled_late: u64,
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
            wal_waits: load(&self.wal_waits),
            wal_wait_us_total: load(&self.wal_wait_us_total),
            wal_wait_us_max: load(&self.wal_wait_us_max),
            wal_timeouts: load(&self.wal_timeouts),
            wal_closed: load(&self.wal_closed),
            settled_late: load(&self.settled_late),
        }
    }

    fn waited(&self, waited: Duration) {
        let us = waited.as_micros().min(u128::from(u64::MAX)) as u64;
        self.wal_waits.fetch_add(1, Ordering::Relaxed);
        self.wal_wait_us_total.fetch_add(us, Ordering::Relaxed);
        self.wal_wait_us_max.fetch_max(us, Ordering::Relaxed);
    }
}

struct PeerHandler {
    config: PeerConfig,
    instance: String,
    dedupe: DedupeWindow,
    stats: Arc<PeerStats>,
    sink: Box<dyn Fn(Vec<LogRecord>) -> Admitted + Send + Sync>,
    acks: Option<Arc<WalAcks>>,
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

        // Batches that timed out and synced since: saved first, so this request (often their resend) sees them.
        if let Some(acks) = &self.acks {
            let late = acks.settle();
            if !late.is_empty() {
                self.stats.settled_late.fetch_add(late.len() as u64, Ordering::Relaxed);
                self.save_ids(&late);
            }
        }

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
                Admitted::Pending(ticket) => {
                    let Some(acks) = &self.acks else {
                        // A sink that queues without a way to learn the WAL synced cannot be acknowledged.
                        tracing::error!("the peer sink queued a batch but the receiver has no WAL acks");
                        return error_reply(500, "internal", "the receiver cannot confirm durability");
                    };
                    match acks.wait(ticket) {
                        WalWait::Synced(waited) => self.stats.waited(waited),
                        WalWait::TimedOut(waited) => {
                            self.stats.waited(waited);
                            self.stats.wal_timeouts.fetch_add(1, Ordering::Relaxed);
                            // Still in the pipeline: if it syncs, its ids are saved then, and the resend is a duplicate.
                            acks.abandon(ticket, ids);
                            return error_reply(503, "wal_slow", "the write-ahead log has not synced the batch yet; retry")
                                .with_header("Retry-After", &retry_after(acks.bound()));
                        }
                        WalWait::Closed => {
                            self.stats.wal_closed.fetch_add(1, Ordering::Relaxed);
                            return error_reply(503, "shutting_down", "the receiver is stopping; retry").with_header("Retry-After", "1");
                        }
                    }
                }
                Admitted::Busy => {
                    self.stats.busy.fetch_add(1, Ordering::Relaxed);
                    // Not remembered: the sender retries this batch and it must not be taken for a duplicate.
                    return error_reply(503, "busy", "the ingest pipeline is full; retry").with_header("Retry-After", "1");
                }
            }
            // Remembered only once the WAL has them, and durably before the reply: a sender that hears 200 has
            // moved on, so a restart must still recognise a late copy (see "Idempotency" above).
            self.save_ids(&ids);
        }
        self.stats.accepted.fetch_add(accepted, Ordering::Relaxed);
        if loops > 0 && accepted == 0 && duplicates == 0 {
            return Reply::json(508, format!(r#"{{"error":"loop_detected","accepted":0,"duplicates":0,"loops":{loops}}}"#));
        }
        Reply::json(200, format!(r#"{{"accepted":{accepted},"duplicates":{duplicates},"loops":{loops}}}"#))
    }

    fn save_ids(&self, ids: &[Uuid]) {
        if let Err(e) = self.dedupe.insert_all(ids.iter()) {
            self.stats.dedupe_unsaved.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %e, "could not save accepted event ids; a restart may store a resent copy");
        }
    }
}

/// Whole seconds, at least one: about as long as a batch that just timed out may still take to sync.
fn retry_after(bound: Duration) -> String {
    bound.as_secs().clamp(1, 5).to_string()
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
        Self::start_in(config, instance, catalog, None, None, sink)
    }

    /// As [`Receiver::start_with_catalog`], with the dedupe window kept in [`DEDUPE_FILE`] under `data_dir`
    /// so it survives a restart, and `acks` to wait on when the sink answers [`Admitted::Pending`]. This is
    /// how the agent runs it.
    pub fn start_in<F>(
        config: &PeerConfig,
        instance: String,
        catalog: Option<PathBuf>,
        data_dir: Option<&Path>,
        acks: Option<Arc<WalAcks>>,
        sink: F,
    ) -> Result<Self, PeerError>
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
            dedupe,
            stats: Arc::clone(&stats),
            sink: Box::new(sink),
            acks,
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

    /// The live counters, for a status endpoint that outlives this handle's borrow.
    pub fn stats_handle(&self) -> Arc<PeerStats> {
        Arc::clone(&self.stats)
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
            Receiver::start_in(&config, "self0001".into(), None, Some(dir.path()), None, move |records| {
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

    // --- acknowledgement after the WAL ------------------------------------------------------------
    //
    // The sink here is the agent's shape: it queues the batch and answers Pending. The test plays the record
    // path (bind) and the WAL writer (publish), so it decides exactly when a batch counts as synced.

    type Queued = Arc<StdMutex<Vec<(Ticket, Vec<LogRecord>)>>>;

    fn start_wal(dir: &Path, bound: Duration) -> (Receiver, Arc<WalAcks>, Queued) {
        let acks = WalAcks::new(Arc::new(AtomicU64::new(0)), bound);
        let queued: Queued = Arc::new(StdMutex::new(Vec::new()));
        let (q, a) = (Arc::clone(&queued), Arc::clone(&acks));
        let config = PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), ..PeerConfig::default() };
        let receiver = Receiver::start_in(&config, "self0001".into(), None, Some(dir), Some(Arc::clone(&acks)), move |records| {
            let ticket = a.issue();
            q.lock().unwrap().push((ticket, records));
            Admitted::Pending(ticket)
        })
        .unwrap();
        (receiver, acks, queued)
    }

    fn wait_for_batches(queued: &Queued, n: usize) {
        let started = Instant::now();
        while queued.lock().unwrap().len() < n {
            assert!(started.elapsed() < Duration::from_secs(5), "the sink never got batch {n}");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn dedupe_bytes(dir: &Path) -> u64 {
        std::fs::metadata(dir.join(DEDUPE_FILE)).map(|m| m.len()).unwrap_or(0)
    }

    #[test]
    fn the_200_comes_only_after_the_wal_synced_and_then_the_ids_are_saved() {
        let dir = tempfile::tempdir().unwrap();
        let (rx, acks, queued) = start_wal(dir.path(), Duration::from_secs(5));
        let addr = rx.local_addr();
        let body = ndjson(&[record("a"), record("b")]);
        let started = Instant::now();
        let request = std::thread::spawn(move || (post(addr, INGEST_PATH, None, &body), started.elapsed()));
        wait_for_batches(&queued, 1);
        // Submitted (bound to its watermark) but not synced: written is not durable.
        let ticket = queued.lock().unwrap()[0].0;
        acks.bind(ticket, 2);
        acks.publish(1);
        std::thread::sleep(Duration::from_millis(150));
        assert!(!request.is_finished(), "no reply while the WAL has not synced the whole batch");
        assert_eq!(0, dedupe_bytes(dir.path()), "ids are not saved before the WAL has the records");
        acks.publish(2);
        let ((status, body), elapsed) = request.join().unwrap();
        assert_eq!(200, status, "{body}");
        assert_eq!(2, body["accepted"]);
        assert!(elapsed >= Duration::from_millis(150), "answered after the sync, {elapsed:?}");
        assert_eq!(32, dedupe_bytes(dir.path()), "both ids saved once the WAL had them");
        let s = rx.stats();
        assert_eq!((1, 0), (s.wal_waits, s.wal_timeouts));
        assert!(s.wal_wait_us_max >= 150_000, "{s:?}");
        assert_eq!(0, acks.outstanding());
        rx.shutdown();
    }

    #[test]
    fn a_pipeline_that_dies_before_the_wal_syncs_never_answers_200_and_the_resend_is_taken() {
        let dir = tempfile::tempdir().unwrap();
        let batch = vec![record("a"), record("b")];
        {
            let (rx, acks, queued) = start_wal(dir.path(), Duration::from_secs(5));
            let (addr, body) = (rx.local_addr(), ndjson(&batch));
            let request = std::thread::spawn(move || post(addr, INGEST_PATH, None, &body));
            wait_for_batches(&queued, 1);
            acks.bind(queued.lock().unwrap()[0].0, 2);
            // The process goes down with the batch in memory: the WAL never syncs it.
            acks.close();
            let (status, body) = request.join().unwrap();
            assert_eq!(503, status, "{body}");
            assert_eq!("shutting_down", body["error"]);
            assert_eq!(0, dedupe_bytes(dir.path()), "nothing remembered for a batch that never reached disk");
            assert_eq!(1, rx.stats().wal_closed);
            rx.shutdown();
        }
        // After the restart the sender's resend is new, not a duplicate of the batch the crash lost.
        let (rx, acks, queued) = start_wal(dir.path(), Duration::from_secs(5));
        let (addr, body) = (rx.local_addr(), ndjson(&batch));
        let request = std::thread::spawn(move || post(addr, INGEST_PATH, None, &body));
        wait_for_batches(&queued, 1);
        acks.bind(queued.lock().unwrap()[0].0, 2);
        acks.publish(2);
        let (status, body) = request.join().unwrap();
        assert_eq!((200, 2, 0), (status, body["accepted"].as_u64().unwrap(), body["duplicates"].as_u64().unwrap()), "{body}");
        rx.shutdown();
    }

    #[test]
    fn the_wait_is_bounded_and_a_late_sync_makes_the_resend_a_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let bound = Duration::from_millis(300);
        let (rx, acks, queued) = start_wal(dir.path(), bound);
        let batch = ingest_request(&[], &ndjson(&[record("a"), record("b")]));
        let (addr, req) = (rx.local_addr(), batch.clone());
        let started = Instant::now();
        let request = std::thread::spawn(move || exchange(addr, &req, WAIT));
        wait_for_batches(&queued, 1);
        acks.bind(queued.lock().unwrap()[0].0, 2);
        let (status, text) = request.join().unwrap();
        let elapsed = started.elapsed();
        assert_eq!(503, status, "{text}");
        assert!(text.contains("wal_slow") && text.to_ascii_lowercase().contains("retry-after: 1"), "{text}");
        assert!(elapsed >= bound && elapsed < bound + Duration::from_secs(1), "bounded by ack_wait_ms: {elapsed:?}");
        assert_eq!(0, dedupe_bytes(dir.path()));
        let s = rx.stats();
        assert_eq!((1, 1, 0), (s.wal_timeouts, s.wal_waits, s.accepted), "{s:?}");
        assert!(s.wal_wait_us_max >= 300_000, "{s:?}");

        // The batch was still in the pipeline and syncs late; the resend finds its ids saved.
        acks.publish(2);
        let (status, text) = exchange(rx.local_addr(), &batch, WAIT);
        assert_eq!(200, status, "{text}");
        assert!(text.contains(r#""accepted":0,"duplicates":2"#), "{text}");
        assert_eq!(1, queued.lock().unwrap().len(), "the pipeline got the batch once");
        assert_eq!((2, 32), (rx.stats().settled_late, dedupe_bytes(dir.path())));
        assert_eq!(0, acks.outstanding());
        rx.shutdown();
    }

    #[test]
    fn abandoned_tickets_are_capped_and_settle_only_once_synced() {
        let acks = WalAcks::new(Arc::new(AtomicU64::new(0)), Duration::from_millis(1));
        let tickets: Vec<Ticket> = (0..MAX_ABANDONED + 10).map(|_| acks.issue()).collect();
        for (i, t) in tickets.iter().enumerate() {
            acks.bind(*t, i as u64 + 1);
            assert!(matches!(acks.wait(*t), WalWait::TimedOut(_)));
            acks.abandon(*t, vec![Uuid::now_v7()]);
        }
        assert_eq!(MAX_ABANDONED, acks.outstanding(), "the oldest abandoned tickets are dropped");
        assert!(acks.settle().is_empty(), "nothing synced yet");
        acks.publish(20);
        assert_eq!(10, acks.settle().len(), "tickets 11..=20 synced; 1..=10 were evicted");
        assert!(acks.settle().is_empty(), "settled once");
        let forgotten = acks.issue();
        acks.forget(forgotten);
        assert_eq!(MAX_ABANDONED - 10, acks.outstanding());
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
