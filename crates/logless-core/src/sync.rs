//! Sending to another log-less instance, and the settings that control it.
//!
//! Records the agent has accepted are tapped off the write path, filtered by severity, stamped with
//! provenance (`peer::stamp_for_send`), batched into newline-delimited JSON and appended to a durable
//! spool. A sender thread drains the spool to the peer's `POST /v1/ingest` and commits the spool cursor
//! only after the peer answered 2xx, so a peer outage, a restart or a crash resends and never drops.
//! Resending is safe because the peer drops event ids it has already accepted (`peer::DedupeWindow`).
//!
//! The write path is never slowed by the peer: the tap is a bounded channel that sheds (and counts)
//! when full, the spool is capped, and nothing here holds a lock the WAL writer takes.
//!
//! # Settings
//!
//! The destination can come from the config file (`[[destinations]] type = "logless"`) or from the
//! admin API, which keeps its own copy in `sync.json` with the token in `sync-token` (mode 0600) in the
//! data dir. The admin copy wins once it exists, and changes apply to the running sender with no restart.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};

use crate::model::LogRecord;
use crate::peer::{self, Capabilities, CAPABILITY_PATH, INGEST_PATH, WIRE_FORMAT};
use crate::spool::{Spool, SpoolConfig};

/// Raw bytes of ndjson in one spool frame, and so in one request. Well under any receiver's body limit.
const FRAME_BYTES: usize = 1024 * 1024;
const FRAMES_PER_PASS: usize = 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BACKOFF_SECS: u64 = 60;
const AUTH_RETRY_SECS: u64 = 60;
/// Batches the tap holds between the WAL writer and the sender.
pub const TAP_CAPACITY: usize = 256;
pub const SETTINGS_FILE: &str = "sync.json";
pub const TOKEN_FILE: &str = "sync-token";
pub const SPOOL_DIR: &str = "sync-spool";

/// Where records go and what is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncTarget {
    pub url: String,
    pub token: Option<String>,
    /// Only records at or above this OTel severity are sent. `0` sends everything.
    pub min_severity: u8,
}

/// The non-secret part, kept in `sync.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncSettings {
    pub enabled: bool,
    pub url: String,
    #[serde(default)]
    pub min_severity: u8,
    pub updated_at_ms: u64,
}

/// What a settings change does to the stored token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenChange {
    Keep,
    Set(String),
    Clear,
}

/// `sync.json` and `sync-token` in the data dir.
#[derive(Debug, Clone)]
pub struct SyncStore {
    dir: PathBuf,
}

impl SyncStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn load(&self) -> Option<(SyncSettings, Option<String>)> {
        let bytes = std::fs::read(self.dir.join(SETTINGS_FILE)).ok()?;
        let settings: SyncSettings = match serde_json::from_slice(&bytes) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "sync.json is unreadable; using the config file's destination");
                return None;
            }
        };
        let token = std::fs::read_to_string(self.dir.join(TOKEN_FILE)).ok().map(|t| t.trim().to_string()).filter(|t| !t.is_empty());
        Some((settings, token))
    }

    pub fn save(&self, settings: &SyncSettings, token: TokenChange) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        match token {
            TokenChange::Keep => {}
            TokenChange::Set(t) => write_private(&self.dir.join(TOKEN_FILE), t.as_bytes())?,
            TokenChange::Clear => match std::fs::remove_file(self.dir.join(TOKEN_FILE)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            },
        }
        let json = serde_json::to_vec_pretty(settings).map_err(std::io::Error::other)?;
        write_private(&self.dir.join(SETTINGS_FILE), &json)
    }

    pub fn has_token(&self) -> bool {
        self.load().is_some_and(|(_, t)| t.is_some())
    }
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(tmp, path)
}

/// What the sender reports. Everything a Sync screen shows comes from here.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct SyncStatus {
    /// `off`, `ok` or `error`.
    pub state: String,
    /// `scheme://host[:port]` of the peer: enough to recognise it, never the path or token.
    pub peer: Option<String>,
    pub min_severity: u8,
    pub has_token: bool,
    pub last_ok_ms: Option<u64>,
    pub last_error: Option<String>,
    pub last_error_ms: Option<u64>,
    pub sent_records: u64,
    pub sent_bytes: u64,
    /// Records the peer already had.
    pub duplicates: u64,
    /// Records the peer refused as a loop.
    pub loops: u64,
    /// Batches the peer refused for good (malformed), dropped after being recorded here.
    pub rejected_batches: u64,
    /// Records shed because the tap or the spool was full.
    pub dropped_records: u64,
    pub backlog_bytes: u64,
    pub queued_batches: usize,
    pub capabilities: Option<Capabilities>,
    pub probed_ms: Option<u64>,
}

/// Shared between the sender thread, the WAL tap and the admin API.
pub struct SyncControl {
    instance: String,
    store: SyncStore,
    target: RwLock<Option<SyncTarget>>,
    status: Mutex<SyncStatus>,
    active: AtomicBool,
    tap_dropped: AtomicU64,
    /// Bumped by every settings change, so the sender retries at once instead of waiting out a backoff
    /// that the old settings earned.
    epoch: AtomicU64,
}

impl SyncControl {
    pub fn new(instance: String, store: SyncStore, initial: Option<SyncTarget>) -> Arc<Self> {
        let control = Arc::new(Self {
            instance,
            store,
            target: RwLock::new(None),
            status: Mutex::new(SyncStatus::default()),
            active: AtomicBool::new(false),
            tap_dropped: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
        });
        control.set_target(initial);
        control
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }

    pub fn store(&self) -> &SyncStore {
        &self.store
    }

    pub fn target(&self) -> Option<SyncTarget> {
        self.target.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// Applies new settings to the running sender.
    pub fn set_target(&self, target: Option<SyncTarget>) {
        self.active.store(target.is_some(), Ordering::Release);
        {
            let mut status = self.status.lock().unwrap_or_else(|p| p.into_inner());
            status.peer = target.as_ref().and_then(|t| display_target(&t.url));
            status.min_severity = target.as_ref().map_or(0, |t| t.min_severity);
            status.has_token = target.as_ref().is_some_and(|t| t.token.is_some());
            if target.is_none() {
                status.last_error = None;
            }
        }
        *self.target.write().unwrap_or_else(|p| p.into_inner()) = target;
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    pub fn status(&self) -> SyncStatus {
        let mut s = self.status.lock().unwrap_or_else(|p| p.into_inner()).clone();
        s.dropped_records += self.tap_dropped.load(Ordering::Relaxed);
        s.state = if !self.is_active() {
            "off".into()
        } else if s.last_error.is_some() && s.last_error_ms >= s.last_ok_ms {
            "error".into()
        } else {
            "ok".into()
        };
        s
    }

    fn update(&self, f: impl FnOnce(&mut SyncStatus)) {
        f(&mut self.status.lock().unwrap_or_else(|p| p.into_inner()));
    }

    pub fn record_probe(&self, capabilities: Capabilities) {
        self.update(|s| {
            s.capabilities = Some(capabilities);
            s.probed_ms = Some(now_ms());
        });
    }
}

/// `scheme://host[:port]`, or None when `url` does not parse.
pub fn display_target(url: &str) -> Option<String> {
    let (scheme, rest) = url.trim().split_once("://")?;
    let host = rest.split(['/', '?', '#']).next()?;
    let host = host.rsplit_once('@').map_or(host, |(_, h)| h);
    (!host.is_empty()).then(|| format!("{scheme}://{host}"))
}

/// Why `url` cannot be a peer, or the cleaned URL.
pub fn check_url(raw: &str) -> Result<String, String> {
    let url = raw.trim();
    if url.is_empty() || url.len() > 2_000 {
        return Err("A peer URL is 1 to 2000 characters.".into());
    }
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err("A peer URL starts with http:// or https://.".into());
    };
    if !matches!(scheme, "http" | "https") {
        return Err("A peer URL starts with http:// or https://.".into());
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() {
        return Err("A peer URL needs a host.".into());
    }
    if authority.contains('@') {
        return Err("A peer URL cannot carry a user name or password; use the token field.".into());
    }
    Ok(url.trim_end_matches('/').to_string())
}

/// The write path's handle: offers each batch the WAL accepted.
#[derive(Clone)]
pub struct SyncTap {
    tx: Sender<Arc<Vec<LogRecord>>>,
    control: Arc<SyncControl>,
}

impl SyncTap {
    /// Cheap when sync is off (one atomic load). When on, copies the batch and hands it over; a full
    /// channel sheds the batch and counts its records instead of waiting.
    pub fn offer(&self, batch: &[LogRecord]) {
        if batch.is_empty() || !self.control.is_active() {
            return;
        }
        if self.tx.try_send(Arc::new(batch.to_vec())).is_err() {
            self.control.tap_dropped.fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
    }
}

pub fn channel(control: Arc<SyncControl>) -> (SyncTap, Receiver<Arc<Vec<LogRecord>>>) {
    let (tx, rx) = crossbeam_channel::bounded(TAP_CAPACITY);
    (SyncTap { tx, control }, rx)
}

fn now_ms() -> u64 {
    crate::now_unix_nanos() / 1_000_000
}

/// The JSON body of a response, or None when it is not (this build of ureq has no JSON helper).
pub(crate) fn read_json<T: serde::de::DeserializeOwned>(body: &mut ureq::Body) -> Option<T> {
    serde_json::from_slice(&body.read_to_vec().ok()?).ok()
}

enum Outcome {
    Delivered { accepted: u64, duplicates: u64, loops: u64, bytes: u64 },
    /// The peer will never take this frame; drop it, say so.
    Rejected(String),
    /// Try again later; the frame stays.
    Retry(String),
    /// The token is wrong or missing: nothing will be accepted until it is fixed.
    Unauthorized(String),
}

fn ingest_url(target: &SyncTarget) -> String {
    format!("{}{INGEST_PATH}", target.url.trim_end_matches('/'))
}

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder().timeout_global(Some(REQUEST_TIMEOUT)).http_status_as_error(false).build().into()
}

fn send_frame(instance: &str, target: &SyncTarget, ndjson: &[u8]) -> Outcome {
    let mut gz = GzEncoder::new(Vec::new(), Compression::fast());
    if gz.write_all(ndjson).is_err() {
        return Outcome::Rejected("could not compress a batch".into());
    }
    let body = match gz.finish() {
        Ok(b) => b,
        Err(e) => return Outcome::Rejected(format!("could not compress a batch: {e}")),
    };
    let mut request = agent()
        .post(&ingest_url(target))
        .header("Content-Type", "application/x-ndjson")
        .header("Content-Encoding", "gzip")
        .header("X-Logless-Origin", instance)
        .header("X-Logless-Format", WIRE_FORMAT);
    if let Some(token) = &target.token {
        request = request.header("Authorization", &format!("Bearer {token}"));
    }
    let mut response = match request.send(&body) {
        Ok(r) => r,
        Err(e) => return Outcome::Retry(format!("could not reach the peer: {e}")),
    };
    let status = response.status().as_u16();
    let reply: serde_json::Value = read_json(response.body_mut()).unwrap_or(serde_json::Value::Null);
    let message = reply["message"].as_str().or_else(|| reply["error"].as_str()).unwrap_or("").to_string();
    match status {
        200..=299 => Outcome::Delivered {
            accepted: reply["accepted"].as_u64().unwrap_or(0),
            duplicates: reply["duplicates"].as_u64().unwrap_or(0),
            loops: reply["loops"].as_u64().unwrap_or(0),
            bytes: body.len() as u64,
        },
        401 | 403 => Outcome::Unauthorized(format!("The peer refused the token (HTTP {status}). {message}").trim().to_string()),
        508 => Outcome::Delivered { accepted: 0, duplicates: 0, loops: reply["loops"].as_u64().unwrap_or(0), bytes: body.len() as u64 },
        400 | 413 | 415 | 422 => Outcome::Rejected(format!("The peer rejected a batch (HTTP {status}): {message}").trim().to_string()),
        404 | 405 => Outcome::Retry(format!("{} is not a log-less peer endpoint (HTTP {status}).", ingest_url(target))),
        408 | 429 | 500..=599 => Outcome::Retry(format!("The peer answered HTTP {status}. {message}").trim().to_string()),
        other => Outcome::Retry(format!("The peer answered HTTP {other}. {message}").trim().to_string()),
    }
}

/// What a probe learned.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProbeOutcome {
    pub capabilities: Capabilities,
    pub round_trip_ms: u64,
    /// Peer clock minus ours, milliseconds, corrected for half the round trip.
    pub clock_skew_ms: i64,
    pub accepts_native: bool,
}

/// Asks `url` whether it is a log-less peer, and what it offers. The one URL answers for everything.
pub fn probe(url: &str, token: Option<&str>) -> Result<ProbeOutcome, String> {
    let url = check_url(url)?;
    let agent: ureq::Agent = ureq::Agent::config_builder().timeout_global(Some(PROBE_TIMEOUT)).http_status_as_error(false).build().into();
    let mut request = agent.get(&format!("{url}{CAPABILITY_PATH}"));
    if let Some(token) = token {
        request = request.header("Authorization", &format!("Bearer {token}"));
    }
    let started = Instant::now();
    let before = now_ms();
    let mut response = request.call().map_err(|e| format!("Could not reach {url}: {e}"))?;
    let round_trip = started.elapsed();
    let status = response.status().as_u16();
    match status {
        200 => {}
        401 => return Err("The peer wants a token. Enter one it accepts.".into()),
        403 => return Err("The peer refused this token.".into()),
        404 => return Err(format!("{url} answered, but it is not a log-less peer (no capability document).")),
        other => return Err(format!("{url} answered HTTP {other}.")),
    }
    let capabilities: Capabilities = read_json(response.body_mut())
        .ok_or_else(|| format!("{url} answered, but not with a log-less capability document."))?;
    if capabilities.service != "log-less" {
        return Err(format!("{url} is not a log-less peer (it says it is {:?}).", capabilities.service));
    }
    let midpoint = before + round_trip.as_millis() as u64 / 2;
    Ok(ProbeOutcome {
        accepts_native: capabilities.formats.iter().any(|f| f == WIRE_FORMAT) && capabilities.ingest.is_some(),
        clock_skew_ms: capabilities.clock_ms as i64 - midpoint as i64,
        round_trip_ms: round_trip.as_millis() as u64,
        capabilities,
    })
}

/// Encodes one batch into spool frames: filtered, stamped, newline-delimited JSON of at most
/// [`FRAME_BYTES`] each. Returns the frames and how many records they hold.
pub fn encode_frames(instance: &str, min_severity: u8, batch: &[LogRecord]) -> (Vec<(Vec<u8>, usize)>, usize) {
    let mut frames: Vec<(Vec<u8>, usize)> = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    let mut in_current = 0usize;
    let mut total = 0usize;
    for record in batch {
        if record.severity.0 < min_severity || peer::is_loop(record, instance) {
            continue;
        }
        let mut stamped = record.clone();
        peer::stamp_for_send(&mut stamped, instance);
        let Ok(mut line) = serde_json::to_vec(&stamped) else { continue };
        line.push(b'\n');
        if !current.is_empty() && current.len() + line.len() > FRAME_BYTES {
            frames.push((std::mem::take(&mut current), std::mem::take(&mut in_current)));
        }
        current.extend_from_slice(&line);
        in_current += 1;
        total += 1;
    }
    if !current.is_empty() {
        frames.push((current, in_current));
    }
    (frames, total)
}

/// Runs the sender until `stop` is set and the tap is empty. Owns the spool.
pub fn run(control: Arc<SyncControl>, rx: Receiver<Arc<Vec<LogRecord>>>, spool_dir: &Path, stop: &AtomicBool) {
    let mut spool = match Spool::open(spool_dir, SpoolConfig { max_total_bytes: 256 * 1024 * 1024, ..SpoolConfig::default() }) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "sync spool unavailable; peer sync is disabled");
            control.update(|s| s.last_error = Some(format!("The sync spool could not be opened: {e}")));
            return;
        }
    };
    let mut failures = 0u32;
    let mut next_attempt = Instant::now();
    let mut disconnected = false;
    let mut seen_epoch = control.epoch.load(Ordering::Acquire);
    loop {
        let stopping = stop.load(Ordering::Acquire);
        let epoch = control.epoch.load(Ordering::Acquire);
        if epoch != seen_epoch {
            seen_epoch = epoch;
            failures = 0;
            next_attempt = Instant::now();
        }
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(batch) => enqueue(&control, &mut spool, &batch),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => disconnected = true,
        }
        while let Ok(batch) = rx.try_recv() {
            enqueue(&control, &mut spool, &batch);
        }
        let _ = spool.sync();

        if Instant::now() >= next_attempt {
            match deliver(&control, &mut spool) {
                Pass::Idle | Pass::Progress => failures = 0,
                Pass::Backoff(retry_after) => {
                    failures += 1;
                    let secs = retry_after.unwrap_or_else(|| (1u64 << failures.min(6)).min(MAX_BACKOFF_SECS));
                    next_attempt = Instant::now() + Duration::from_secs(secs);
                }
            }
        }
        control.update(|s| {
            s.backlog_bytes = spool.pending_bytes().unwrap_or(0);
            s.queued_batches = rx.len();
        });
        if (stopping || disconnected) && rx.is_empty() {
            // One last attempt so a clean shutdown flushes what a healthy peer can take now.
            let _ = deliver(&control, &mut spool);
            let _ = spool.sync();
            return;
        }
    }
}

fn enqueue(control: &SyncControl, spool: &mut Spool, batch: &[LogRecord]) {
    let Some(target) = control.target() else { return };
    let (frames, _) = encode_frames(control.instance(), target.min_severity, batch);
    for (payload, records) in frames {
        match spool.push(&payload) {
            Ok(true) => {}
            Ok(false) => control.update(|s| s.dropped_records += records as u64),
            Err(e) => {
                tracing::warn!(error = %e, "sync spool write failed");
                control.update(|s| s.dropped_records += records as u64);
            }
        }
    }
}

enum Pass {
    Idle,
    Progress,
    Backoff(Option<u64>),
}

fn deliver(control: &SyncControl, spool: &mut Spool) -> Pass {
    let Some(target) = control.target() else { return Pass::Idle };
    let frames = match spool.peek(FRAMES_PER_PASS) {
        Ok(f) => f,
        Err(e) => {
            control.update(|s| {
                s.last_error = Some(format!("The sync spool could not be read: {e}"));
                s.last_error_ms = Some(now_ms());
            });
            return Pass::Backoff(None);
        }
    };
    if frames.is_empty() {
        return Pass::Idle;
    }
    for (cursor, payload) in frames {
        match send_frame(control.instance(), &target, &payload) {
            Outcome::Delivered { accepted, duplicates, loops, bytes } => {
                let _ = spool.commit(cursor);
                control.update(|s| {
                    s.sent_records += accepted;
                    s.duplicates += duplicates;
                    s.loops += loops;
                    s.sent_bytes += bytes;
                    s.last_ok_ms = Some(now_ms());
                });
            }
            Outcome::Rejected(message) => {
                let _ = spool.commit(cursor);
                control.update(|s| {
                    s.rejected_batches += 1;
                    s.last_error = Some(message);
                    s.last_error_ms = Some(now_ms());
                });
            }
            Outcome::Retry(message) => {
                control.update(|s| {
                    s.last_error = Some(message);
                    s.last_error_ms = Some(now_ms());
                });
                return Pass::Backoff(None);
            }
            Outcome::Unauthorized(message) => {
                control.update(|s| {
                    s.last_error = Some(message);
                    s.last_error_ms = Some(now_ms());
                });
                return Pass::Backoff(Some(AUTH_RETRY_SECS));
            }
        }
    }
    Pass::Progress
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;
    use crate::peer::{Admitted, PeerConfig};
    use std::sync::Mutex as StdMutex;

    fn record(body: &str, severity: Severity) -> LogRecord {
        let mut r = LogRecord::new(1_700_000_000_000_000_000, severity, body);
        r.observed_unix_nano = 1_700_000_001_000_000_000;
        r.service = Some("api".into());
        r
    }

    struct Peer {
        receiver: peer::Receiver,
        got: Arc<StdMutex<Vec<LogRecord>>>,
        open: Arc<AtomicBool>,
    }

    fn peer(tokens: Vec<String>) -> Peer {
        let got = Arc::new(StdMutex::new(Vec::new()));
        let open = Arc::new(AtomicBool::new(true));
        let (sink, gate) = (Arc::clone(&got), Arc::clone(&open));
        let receiver = peer::Receiver::start(
            &PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), tokens, ..PeerConfig::default() },
            "central".into(),
            move |records| {
                if !gate.load(Ordering::SeqCst) {
                    return Admitted::Busy;
                }
                sink.lock().unwrap().extend(records);
                Admitted::Accepted
            },
        )
        .unwrap();
        Peer { receiver, got, open }
    }

    fn target_of(p: &Peer, token: Option<&str>, min: u8) -> SyncTarget {
        SyncTarget { url: format!("http://{}", p.receiver.local_addr()), token: token.map(str::to_string), min_severity: min }
    }

    struct Running {
        control: Arc<SyncControl>,
        tap: SyncTap,
        stop: Arc<AtomicBool>,
        handle: std::thread::JoinHandle<()>,
    }

    fn start_sender(dir: &Path, target: Option<SyncTarget>) -> Running {
        let control = SyncControl::new("edge".into(), SyncStore::new(dir), target);
        let (tap, rx) = channel(Arc::clone(&control));
        let stop = Arc::new(AtomicBool::new(false));
        let (c, s, d) = (Arc::clone(&control), Arc::clone(&stop), dir.join(SPOOL_DIR));
        let handle = std::thread::spawn(move || run(c, rx, &d, &s));
        Running { control, tap, stop, handle }
    }

    impl Running {
        fn finish(self) -> Arc<SyncControl> {
            self.stop.store(true, Ordering::Release);
            drop(self.tap);
            self.handle.join().unwrap();
            self.control
        }
    }

    fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
        let started = Instant::now();
        while !done() {
            assert!(started.elapsed() < Duration::from_secs(15), "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    #[test]
    fn records_reach_the_peer_stamped_with_their_origin_and_original_times() {
        let dir = tempfile::tempdir().unwrap();
        let p = peer(vec![]);
        let run = start_sender(dir.path(), Some(target_of(&p, None, 0)));
        let sent = record("boom", Severity::ERROR);
        run.tap.offer(std::slice::from_ref(&sent));
        wait_for("delivery", || !p.got.lock().unwrap().is_empty());
        let got = p.got.lock().unwrap()[0].clone();
        assert_eq!((sent.event_id, sent.observed_unix_nano, &sent.body), (got.event_id, got.observed_unix_nano, &got.body));
        assert!(got.attributes.iter().any(|a| a.key == peer::ATTR_ORIGIN && a.value == crate::model::AttrValue::Str("edge".into())));
        let status = run.control.status();
        assert_eq!("ok", status.state);
        wait_for("counted", || run.control.status().sent_records == 1);
        wait_for("an empty backlog", || run.control.status().backlog_bytes == 0);
        run.finish();
        p.receiver.shutdown();
    }

    #[test]
    fn only_records_at_or_above_the_minimum_severity_are_sent() {
        let dir = tempfile::tempdir().unwrap();
        let p = peer(vec![]);
        let run = start_sender(dir.path(), Some(target_of(&p, None, Severity::WARN.0)));
        run.tap.offer(&[record("noise", Severity::DEBUG), record("info", Severity::INFO), record("warn", Severity::WARN), record("err", Severity::ERROR)]);
        wait_for("delivery", || p.got.lock().unwrap().len() == 2);
        let bodies: Vec<String> = p.got.lock().unwrap().iter().map(|r| r.body.clone()).collect();
        assert_eq!(vec!["warn", "err"], bodies);
        run.finish();
        p.receiver.shutdown();
    }

    #[test]
    fn a_peer_that_is_busy_is_retried_and_nothing_is_lost_or_doubled() {
        let dir = tempfile::tempdir().unwrap();
        let p = peer(vec![]);
        p.open.store(false, Ordering::SeqCst);
        let run = start_sender(dir.path(), Some(target_of(&p, None, 0)));
        let batch: Vec<LogRecord> = (0..50).map(|i| record(&format!("line {i}"), Severity::INFO)).collect();
        run.tap.offer(&batch);
        wait_for("an error to be reported", || run.control.status().state == "error");
        assert!(p.got.lock().unwrap().is_empty());
        p.open.store(true, Ordering::SeqCst);
        wait_for("recovery", || p.got.lock().unwrap().len() == 50);
        wait_for("ok", || run.control.status().state == "ok");
        let mut ids: Vec<_> = p.got.lock().unwrap().iter().map(|r| r.event_id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(50, ids.len(), "every record exactly once");
        run.finish();
        p.receiver.shutdown();
    }

    #[test]
    fn what_was_spooled_survives_a_restart_of_the_sender() {
        let dir = tempfile::tempdir().unwrap();
        let p = peer(vec![]);
        p.open.store(false, Ordering::SeqCst);
        let first = start_sender(dir.path(), Some(target_of(&p, None, 0)));
        first.tap.offer(&[record("kept", Severity::ERROR)]);
        wait_for("it to be spooled and refused", || first.control.status().state == "error");
        first.finish();
        p.open.store(true, Ordering::SeqCst);
        let second = start_sender(dir.path(), Some(target_of(&p, None, 0)));
        wait_for("replay after restart", || p.got.lock().unwrap().len() == 1);
        assert_eq!("kept", p.got.lock().unwrap()[0].body);
        second.finish();
        p.receiver.shutdown();
    }

    #[test]
    fn a_wrong_token_is_reported_as_such_and_the_data_waits() {
        let dir = tempfile::tempdir().unwrap();
        let p = peer(vec!["right".into()]);
        let run = start_sender(dir.path(), Some(target_of(&p, Some("wrong"), 0)));
        run.tap.offer(&[record("waits", Severity::ERROR)]);
        wait_for("the refusal", || run.control.status().last_error.is_some());
        let status = run.control.status();
        assert!(status.last_error.unwrap().contains("refused the token"));
        assert_eq!("error", status.state);
        assert!(p.got.lock().unwrap().is_empty());
        // Fixing the token applies to the running sender; the waiting record is delivered.
        run.control.set_target(Some(target_of(&p, Some("right"), 0)));
        wait_for("delivery after the fix", || !p.got.lock().unwrap().is_empty());
        run.finish();
        p.receiver.shutdown();
    }

    #[test]
    fn a_ring_stops_after_one_lap() {
        // edge -> central, and central sends back to edge: edge's own records are refused on return.
        let mut r = record("loop", Severity::ERROR);
        peer::stamp_for_send(&mut r, "edge");
        assert!(peer::is_loop(&r, "edge"));
        let (frames, total) = encode_frames("edge", 0, std::slice::from_ref(&r));
        assert_eq!((0, 0), (frames.len(), total), "the sender never re-sends its own record");
    }

    #[test]
    fn with_no_target_nothing_is_tapped_and_the_status_says_off() {
        let dir = tempfile::tempdir().unwrap();
        let run = start_sender(dir.path(), None);
        run.tap.offer(&[record("x", Severity::ERROR)]);
        assert_eq!("off", run.control.status().state);
        let control = run.finish();
        assert_eq!(0, control.status().sent_records);
    }

    #[test]
    fn the_probe_reads_the_capability_document_and_explains_each_failure() {
        let p = peer(vec!["tok".into()]);
        let url = format!("http://{}", p.receiver.local_addr());
        let ok = probe(&url, Some("tok")).unwrap();
        assert_eq!("log-less", ok.capabilities.service);
        assert!(ok.accepts_native);
        assert_eq!("central", ok.capabilities.instance_id);
        assert!(ok.clock_skew_ms.abs() < 5_000, "{}", ok.clock_skew_ms);
        assert!(probe(&url, None).unwrap_err().contains("wants a token"));
        assert!(probe(&url, Some("bad")).unwrap_err().contains("refused"));
        assert!(probe("http://127.0.0.1:1", None).unwrap_err().contains("Could not reach"));
        assert!(probe("ftp://x", None).unwrap_err().contains("http"));
        p.receiver.shutdown();
    }

    #[test]
    fn settings_round_trip_with_the_token_kept_apart_and_private() {
        let dir = tempfile::tempdir().unwrap();
        let store = SyncStore::new(dir.path());
        assert!(store.load().is_none());
        let settings = SyncSettings { enabled: true, url: "https://central.example.com:9300".into(), min_severity: 13, updated_at_ms: 1 };
        store.save(&settings, TokenChange::Set("s3cret".into())).unwrap();
        assert_eq!(Some((settings.clone(), Some("s3cret".to_string()))), store.load());
        assert!(!std::fs::read_to_string(dir.path().join(SETTINGS_FILE)).unwrap().contains("s3cret"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(0o600, std::fs::metadata(dir.path().join(TOKEN_FILE)).unwrap().permissions().mode() & 0o777);
        }
        store.save(&settings, TokenChange::Keep).unwrap();
        assert!(store.has_token());
        store.save(&settings, TokenChange::Clear).unwrap();
        assert!(!store.has_token());
    }

    #[test]
    fn urls_are_checked_and_shown_without_their_path() {
        assert_eq!("https://a.example.com:9300", check_url(" https://a.example.com:9300/ ").unwrap());
        assert!(check_url("https://u:p@a.example.com").unwrap_err().contains("user name"));
        assert!(check_url("a.example.com").is_err());
        assert!(check_url("https://").is_err());
        assert_eq!(Some("https://a.example.com:9300".to_string()), display_target("https://a.example.com:9300/v1/x?k=1"));
        assert_eq!(Some("http://h".to_string()), display_target("http://user@h/p"));
    }
}
