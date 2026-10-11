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
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};

use crate::model::LogRecord;
use crate::peer::{self, Capabilities, CAPABILITY_PATH, INGEST_PATH, WIRE_FORMAT};
use crate::spool::{Spool, SpoolConfig};
use crate::sync_policy::{Anomaly, Planner, PlannerConfig, ATTR_SUPPLEMENT};

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
    /// Only records at or above this OTel severity are sent or counted. `0` takes everything.
    pub min_severity: u8,
    /// 1 to 100: the percentage of records below errors that are sent in full (`sync_policy`). Errors and
    /// fatals always are. What is left out is counted exactly in per-minute rollups.
    pub fidelity: u8,
    /// Send extra detail by itself when something unusual happens (`sync_policy`).
    pub anomaly_boost: bool,
}

impl SyncTarget {
    pub fn planner_config(&self) -> PlannerConfig {
        PlannerConfig { fidelity: self.fidelity.clamp(1, 100), min_severity: self.min_severity, anomaly_boost: self.anomaly_boost }
    }
}

/// The non-secret part, kept in `sync.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncSettings {
    pub enabled: bool,
    pub url: String,
    #[serde(default)]
    pub min_severity: u8,
    #[serde(default = "full_fidelity")]
    pub fidelity: u8,
    #[serde(default = "yes")]
    pub anomaly_boost: bool,
    pub updated_at_ms: u64,
}

fn full_fidelity() -> u8 {
    100
}
fn yes() -> bool {
    true
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

    /// The data dir these files live in.
    pub fn dir(&self) -> &Path {
        &self.dir
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
    pub fidelity: u8,
    pub anomaly_boost: bool,
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
    /// Records counted into rollups instead of being sent, and the rollups sent for them.
    pub sampled_out: u64,
    pub rollups_sent: u64,
    /// Left-out records sent afterwards as an anomaly's lead-up.
    pub lookback_sent: u64,
    /// Services at full detail right now because of an anomaly.
    pub boosted_services: Vec<String>,
    /// The most recent anomalies, newest first.
    pub anomalies: Vec<Anomaly>,
    pub backfill: Option<BackfillStatus>,
    /// A manual upload of a range's Parquet parts (`parts`).
    pub parts: Option<crate::parts::PartsJobStatus>,
}

/// A manual fetch of a range: everything in it, sent at full detail.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BackfillStatus {
    /// `running`, `done`, `cancelled` or `error`.
    pub state: String,
    pub from_ms: u64,
    pub to_ms: u64,
    pub scanned: u64,
    pub queued: u64,
    pub started_ms: u64,
    pub finished_ms: Option<u64>,
    pub error: Option<String>,
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
    /// Where the committed Parquet store is, for a manual fetch of a range.
    store_dir: OnceLock<PathBuf>,
    /// The sender's intake, so a fetch can feed it (set by `channel`).
    intake: OnceLock<Sender<Item>>,
    backfill_cancel: AtomicBool,
    parts_cancel: Arc<AtomicBool>,
}

/// What the tap hands the sender.
pub enum Item {
    /// A batch the WAL accepted: subject to the dial and the anomaly logic.
    Live(Arc<Vec<LogRecord>>),
    /// Records read back for a manual fetch: sent whole, flagged where the dial had left them out.
    Backfill(Vec<LogRecord>),
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
            store_dir: OnceLock::new(),
            intake: OnceLock::new(),
            backfill_cancel: AtomicBool::new(false),
            parts_cancel: Arc::new(AtomicBool::new(false)),
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
            status.fidelity = target.as_ref().map_or(100, |t| t.fidelity);
            status.anomaly_boost = target.as_ref().is_some_and(|t| t.anomaly_boost);
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
    tx: Sender<Item>,
    control: Arc<SyncControl>,
}

impl SyncTap {
    /// Cheap when sync is off (one atomic load). When on, copies the batch and hands it over; a full
    /// channel sheds the batch and counts its records instead of waiting.
    pub fn offer(&self, batch: &[LogRecord]) {
        if batch.is_empty() || !self.control.is_active() {
            return;
        }
        if self.tx.try_send(Item::Live(Arc::new(batch.to_vec()))).is_err() {
            self.control.tap_dropped.fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
    }
}

pub fn channel(control: Arc<SyncControl>) -> (SyncTap, Receiver<Item>) {
    let (tx, rx) = crossbeam_channel::bounded(TAP_CAPACITY);
    let _ = control.intake.set(tx.clone());
    (SyncTap { tx, control }, rx)
}

impl SyncControl {
    /// Tells the control where the committed store is, which a manual fetch reads.
    pub fn set_store_dir(&self, dir: PathBuf) {
        let _ = self.store_dir.set(dir);
    }

    /// Starts sending every record in `[from_ms, to_ms)` at full detail, on a background thread. The peer
    /// already holding some of them is fine: it recognises their event ids. Reads committed Parquet, so a
    /// range that ends in the last few minutes may still lack what has not been merged yet.
    pub fn start_backfill(self: &Arc<Self>, from_ms: u64, to_ms: u64) -> Result<(), String> {
        if to_ms <= from_ms {
            return Err("The end of the range must be after its start.".into());
        }
        if to_ms - from_ms > 31 * 24 * 3_600_000 {
            return Err("A fetch covers at most 31 days at a time.".into());
        }
        let Some(target) = self.target() else {
            return Err("Set a peer and turn sync on before fetching a range.".into());
        };
        let (Some(dir), Some(intake)) = (self.store_dir.get().cloned(), self.intake.get().cloned()) else {
            return Err("This process cannot read its store for a fetch.".into());
        };
        {
            let mut status = self.status.lock().unwrap_or_else(|p| p.into_inner());
            if status.backfill.as_ref().is_some_and(|b| b.state == "running") {
                return Err("A fetch is already running. Cancel it or wait for it to finish.".into());
            }
            status.backfill = Some(BackfillStatus {
                state: "running".into(),
                from_ms,
                to_ms,
                scanned: 0,
                queued: 0,
                started_ms: now_ms(),
                finished_ms: None,
                error: None,
            });
        }
        self.backfill_cancel.store(false, Ordering::Release);
        let control = Arc::clone(self);
        std::thread::Builder::new()
            .name("logless-backfill".into())
            .spawn(move || {
                let query = crate::scan::Query {
                    from_unix_nano: Some(from_ms * 1_000_000),
                    until_unix_nano: Some(to_ms * 1_000_000),
                    min_severity: (target.min_severity > 0).then_some(crate::model::Severity(target.min_severity)),
                    limit: usize::MAX,
                    ..Default::default()
                };
                let mut queued = 0u64;
                let result = crate::scan::scan_each(&dir, &query, 2_000, |chunk| {
                    if control.backfill_cancel.load(Ordering::Acquire) {
                        return false;
                    }
                    let n = chunk.len() as u64;
                    // Blocking send: the sender's pace is the pace of the fetch.
                    if intake.send(Item::Backfill(chunk)).is_err() {
                        return false;
                    }
                    queued += n;
                    control.update(|s| {
                        if let Some(b) = s.backfill.as_mut() {
                            b.queued = queued;
                        }
                    });
                    true
                });
                control.update(|s| {
                    if let Some(b) = s.backfill.as_mut() {
                        b.finished_ms = Some(now_ms());
                        match &result {
                            Ok(stats) => {
                                b.scanned = stats.rows_scanned;
                                b.state = if control.backfill_cancel.load(Ordering::Acquire) { "cancelled".into() } else { "done".into() };
                            }
                            Err(e) => {
                                b.state = "error".into();
                                b.error = Some(format!("The store could not be read: {e}"));
                            }
                        }
                    }
                });
            })
            .map_err(|e| format!("could not start the fetch: {e}"))?;
        Ok(())
    }

    pub fn cancel_backfill(&self) {
        self.backfill_cancel.store(true, Ordering::Release);
    }

    /// Starts uploading the committed Parquet files that overlap `[from_ms, to_ms)` to the peer's object
    /// storage (`parts`), on a background thread. Needs a peer whose capability document says
    /// `parts: true`; files an earlier run committed are skipped.
    pub fn start_parts(self: &Arc<Self>, from_ms: u64, to_ms: u64) -> Result<(), String> {
        self.start_parts_with(from_ms, to_ms, crate::parts::Tuning::default())
    }

    pub(crate) fn start_parts_with(self: &Arc<Self>, from_ms: u64, to_ms: u64, tuning: crate::parts::Tuning) -> Result<(), String> {
        if to_ms <= from_ms {
            return Err("The end of the range must be after its start.".into());
        }
        if to_ms - from_ms > 31 * 24 * 3_600_000 {
            return Err("An upload covers at most 31 days at a time.".into());
        }
        let Some(target) = self.target() else {
            return Err("Set a peer and turn sync on before uploading parts.".into());
        };
        let Some(dir) = self.store_dir.get().cloned() else {
            return Err("This process cannot read its store for an upload.".into());
        };
        {
            let mut status = self.status.lock().unwrap_or_else(|p| p.into_inner());
            if status.parts.as_ref().is_some_and(|p| p.state == "running") {
                return Err("An upload is already running. Cancel it or wait for it to finish.".into());
            }
            status.parts = Some(crate::parts::PartsJobStatus::starting(from_ms, to_ms));
        }
        self.parts_cancel.store(false, Ordering::Release);
        let control = Arc::clone(self);
        let cancel = Arc::clone(&self.parts_cancel);
        std::thread::Builder::new()
            .name("logless-parts".into())
            .spawn(move || {
                let state_dir = control.store.dir().to_path_buf();
                crate::parts::run_job(&control, &target, &dir, &state_dir, from_ms, to_ms, tuning, &cancel);
            })
            .map_err(|e| format!("could not start the upload: {e}"))?;
        Ok(())
    }

    pub fn cancel_parts(&self) {
        self.parts_cancel.store(true, Ordering::Release);
    }

    pub(crate) fn update_parts(&self, f: impl FnOnce(&mut crate::parts::PartsJobStatus)) {
        self.update(|s| {
            if let Some(p) = s.parts.as_mut() {
                f(p);
            }
        });
    }
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

/// The sender's pace and spool size. Tests shrink them; production uses the default.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Tuning {
    /// The first backoff; it doubles per consecutive failure up to [`MAX_BACKOFF_SECS`] units.
    pub backoff_unit: Duration,
    /// How long a refused token waits before the next try.
    pub auth_retry: Duration,
    pub spool_max_bytes: u64,
}

impl Default for Tuning {
    fn default() -> Self {
        Self { backoff_unit: Duration::from_secs(1), auth_retry: Duration::from_secs(AUTH_RETRY_SECS), spool_max_bytes: 256 * 1024 * 1024 }
    }
}

/// Runs the sender until `stop` is set and the tap is empty. Owns the spool and the planner.
/// `known_templates` are the template ids the catalog holds, so a restart is not a wave of novel shapes.
pub fn run(control: Arc<SyncControl>, rx: Receiver<Item>, spool_dir: &Path, stop: &AtomicBool, known_templates: Vec<u64>, known_pairs: Vec<(Option<String>, u64)>) {
    run_tuned(control, rx, spool_dir, stop, known_templates, known_pairs, Tuning::default())
}

pub(crate) fn run_tuned(
    control: Arc<SyncControl>,
    rx: Receiver<Item>,
    spool_dir: &Path,
    stop: &AtomicBool,
    known_templates: Vec<u64>,
    known_pairs: Vec<(Option<String>, u64)>,
    tuning: Tuning,
) {
    // Segments small enough that a drained spool gives its space back (whole consumed segments are unlinked).
    let segment_bytes = (tuning.spool_max_bytes / 8).min(SpoolConfig::default().segment_bytes);
    let mut spool = match Spool::open(spool_dir, SpoolConfig { max_total_bytes: tuning.spool_max_bytes, segment_bytes, ..SpoolConfig::default() }) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "sync spool unavailable; peer sync is disabled");
            control.update(|s| s.last_error = Some(format!("The sync spool could not be opened: {e}")));
            return;
        }
    };
    let first = control.target().map(|t| t.planner_config()).unwrap_or_default();
    let mut planner = Planner::new(control.instance().to_string(), first, now_ms(), known_templates).with_known_pairs(now_ms(), known_pairs);
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
            if let Some(target) = control.target() {
                planner.set_config(target.planner_config());
            }
        }
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(item) => enqueue(&control, &mut spool, &mut planner, item),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => disconnected = true,
        }
        while let Ok(item) = rx.try_recv() {
            enqueue(&control, &mut spool, &mut planner, item);
        }
        let closing = (stopping || disconnected) && rx.is_empty();
        if control.is_active() {
            let extra = planner.tick(now_ms(), closing);
            push_frames(&control, &mut spool, &extra);
        }
        if let Err(e) = spool.sync() {
            control.update(|s| {
                s.last_error = Some(format!("The sync spool could not be flushed to disk: {e}"));
                s.last_error_ms = Some(now_ms());
            });
        }

        if Instant::now() >= next_attempt {
            match deliver(&control, &mut spool) {
                Pass::Idle | Pass::Progress => failures = 0,
                Pass::Backoff(wait) => {
                    failures += 1;
                    let wait = match wait {
                        Wait::Auth => tuning.auth_retry,
                        Wait::Doubling => tuning.backoff_unit * (1u32 << failures.min(6)).min(MAX_BACKOFF_SECS as u32),
                    };
                    next_attempt = Instant::now() + wait;
                }
            }
        }
        control.update(|s| {
            s.backlog_bytes = spool.pending_bytes().unwrap_or(0);
            s.queued_batches = rx.len();
            s.sampled_out = planner.stats.sampled_out;
            s.rollups_sent = planner.stats.rollups_emitted;
            s.lookback_sent = planner.stats.lookback_sent;
            s.boosted_services = planner.boosted(now_ms());
            s.anomalies = planner.anomalies();
        });
        if closing {
            // One last attempt so a clean shutdown flushes what a healthy peer can take now.
            let _ = deliver(&control, &mut spool);
            let _ = spool.sync();
            return;
        }
    }
}

fn enqueue(control: &SyncControl, spool: &mut Spool, planner: &mut Planner, item: Item) {
    let Some(target) = control.target() else { return };
    match item {
        Item::Live(batch) => {
            let send = planner.process(now_ms(), &batch);
            push_frames(control, spool, &send);
        }
        Item::Backfill(records) => {
            let mut out = Vec::with_capacity(records.len());
            for mut record in records {
                if record.severity.0 < target.min_severity {
                    continue;
                }
                // What the dial left out was counted in a rollup; flag it so a total does not count it twice.
                if !planner.sent_live(&record) {
                    record.attributes.push(crate::model::Attr {
                        key: ATTR_SUPPLEMENT.into(),
                        value: crate::model::AttrValue::Str("backfill".into()),
                    });
                }
                out.push(record);
            }
            push_frames(control, spool, &out);
        }
    }
}

fn push_frames(control: &SyncControl, spool: &mut Spool, records: &[LogRecord]) {
    if records.is_empty() {
        return;
    }
    // The planner has already applied the floor; frames only stamp and split.
    let (frames, _) = encode_frames(control.instance(), 0, records);
    for (payload, count) in frames {
        match spool.push(&payload) {
            Ok(true) => {}
            Ok(false) => control.update(|s| s.dropped_records += count as u64),
            Err(e) => {
                tracing::warn!(error = %e, "sync spool write failed");
                control.update(|s| s.dropped_records += count as u64);
            }
        }
    }
}

enum Pass {
    Idle,
    Progress,
    Backoff(Wait),
}

enum Wait {
    /// One unit, doubling per consecutive failure.
    Doubling,
    /// The token was refused.
    Auth,
}

/// Moves the spool cursor past a frame the peer has answered for good. A cursor that cannot be written is
/// a durability failure, not a delivery one: the frame stays pending, is sent again (the peer drops what
/// it already holds) and the status says why until the cursor can be written.
fn commit(control: &SyncControl, spool: &mut Spool, cursor: crate::spool::Cursor) -> Result<(), Pass> {
    spool.commit(cursor).map_err(|e| {
        tracing::warn!(error = %e, "sync spool cursor write failed");
        control.update(|s| {
            s.last_error = Some(format!(
                "The peer has the batch, but the sync spool could not record that ({e}). It will be sent again, and the peer will drop the copy, until the spool's cursor can be written."
            ));
            s.last_error_ms = Some(now_ms());
        });
        Pass::Backoff(Wait::Doubling)
    })
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
            return Pass::Backoff(Wait::Doubling);
        }
    };
    if frames.is_empty() {
        return Pass::Idle;
    }
    for (cursor, payload) in frames {
        match send_frame(control.instance(), &target, &payload) {
            Outcome::Delivered { accepted, duplicates, loops, bytes } => {
                // What the peer said is counted whether or not the cursor moves: it did take these. A resend
                // after a failed commit comes back as duplicates.
                control.update(|s| {
                    s.sent_records += accepted;
                    s.duplicates += duplicates;
                    s.loops += loops;
                    s.sent_bytes += bytes;
                    s.last_ok_ms = Some(now_ms());
                });
                if let Err(pass) = commit(control, spool, cursor) {
                    return pass;
                }
            }
            Outcome::Rejected(message) => {
                control.update(|s| {
                    s.rejected_batches += 1;
                    s.last_error = Some(message);
                    s.last_error_ms = Some(now_ms());
                });
                if let Err(pass) = commit(control, spool, cursor) {
                    return pass;
                }
            }
            Outcome::Retry(message) => {
                control.update(|s| {
                    s.last_error = Some(message);
                    s.last_error_ms = Some(now_ms());
                });
                return Pass::Backoff(Wait::Doubling);
            }
            Outcome::Unauthorized(message) => {
                control.update(|s| {
                    s.last_error = Some(message);
                    s.last_error_ms = Some(now_ms());
                });
                return Pass::Backoff(Wait::Auth);
            }
        }
    }
    Pass::Progress
}

#[cfg(test)]
#[path = "sync_soak.rs"]
mod soak;

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
        SyncTarget { url: format!("http://{}", p.receiver.local_addr()), token: token.map(str::to_string), min_severity: min, fidelity: 100, anomaly_boost: true }
    }

    struct Running {
        control: Arc<SyncControl>,
        tap: SyncTap,
        stop: Arc<AtomicBool>,
        handle: std::thread::JoinHandle<()>,
    }

    fn start_sender(dir: &Path, target: Option<SyncTarget>) -> Running {
        start_sender_tuned(dir, target, Tuning::default())
    }

    fn quick() -> Tuning {
        Tuning { backoff_unit: Duration::from_millis(20), auth_retry: Duration::from_millis(50), ..Tuning::default() }
    }

    fn start_sender_tuned(dir: &Path, target: Option<SyncTarget>, tuning: Tuning) -> Running {
        let control = SyncControl::new("edge".into(), SyncStore::new(dir), target);
        let (tap, rx) = channel(Arc::clone(&control));
        let stop = Arc::new(AtomicBool::new(false));
        let (c, s, d) = (Arc::clone(&control), Arc::clone(&stop), dir.join(SPOOL_DIR));
        let handle = std::thread::spawn(move || run_tuned(c, rx, &d, &s, vec![1, 2, 3], Vec::new(), tuning));
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
    fn a_cursor_that_cannot_be_written_stops_the_spool_advancing_and_says_so() {
        let dir = tempfile::tempdir().unwrap();
        let p = peer(vec![]);
        // A directory where the cursor's temporary file goes: every cursor write fails, nothing else does.
        let blocker = dir.path().join(SPOOL_DIR).join("cursor.tmp");
        std::fs::create_dir_all(&blocker).unwrap();
        let run = start_sender_tuned(dir.path(), Some(target_of(&p, None, 0)), quick());
        run.tap.offer(&[record("once", Severity::ERROR)]);
        wait_for("the failure to be reported", || run.control.status().last_error.as_deref().is_some_and(|e| e.contains("could not record")));
        // The frame stays pending and is sent again; the peer drops the copies.
        wait_for("a resend the peer recognised", || run.control.status().duplicates >= 2);
        let status = run.control.status();
        assert_eq!("error", status.state, "{status:?}");
        assert!(status.backlog_bytes > 0, "the cursor did not move: {status:?}");
        assert_eq!(1, p.got.lock().unwrap().len(), "stored once however often it is resent");
        // Once the cursor can be written the sender recovers by itself.
        std::fs::remove_dir(&blocker).unwrap();
        wait_for("an empty backlog", || run.control.status().backlog_bytes == 0);
        wait_for("ok", || run.control.status().state == "ok");
        run.finish();
        // And a restart has nothing left to resend.
        let again = start_sender_tuned(dir.path(), Some(target_of(&p, None, 0)), quick());
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!((0, 0), (again.control.status().sent_records, again.control.status().duplicates));
        again.finish();
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
        let settings = SyncSettings { enabled: true, url: "https://central.example.com:9300".into(), min_severity: 13, fidelity: 40, anomaly_boost: false, updated_at_ms: 1 };
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

    fn dial(p: &Peer, fidelity: u8) -> SyncTarget {
        SyncTarget { fidelity, ..target_of(p, None, 0) }
    }

    fn rec_t(body: &str, severity: Severity, template: u64) -> LogRecord {
        let mut r = record(body, severity);
        r.template_id = Some(template);
        r.observed_unix_nano = crate::now_unix_nanos();
        r.timestamp_unix_nano = r.observed_unix_nano;
        r
    }

    fn has(r: &LogRecord, key: &str) -> bool {
        r.attributes.iter().any(|a| a.key == key)
    }

    fn rollup_count(r: &LogRecord) -> u64 {
        r.attributes.iter().find(|a| a.key == "logless.rollup.count").map_or(0, |a| if let crate::model::AttrValue::I64(n) = a.value { n as u64 } else { 0 })
    }

    #[test]
    fn at_one_percent_errors_arrive_in_full_and_the_rest_is_counted_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let p = peer(vec![]);
        let run = start_sender(dir.path(), Some(dial(&p, 1)));
        let mut batch: Vec<LogRecord> = (0..2_000).map(|i| rec_t(&format!("info {i}"), Severity::INFO, 1 + i % 3)).collect();
        batch.extend((0..20).map(|i| rec_t(&format!("err {i}"), Severity::ERROR, 2)));
        run.tap.offer(&batch);
        wait_for("the errors", || p.got.lock().unwrap().iter().filter(|r| r.severity >= Severity::ERROR && !has(r, "logless.rollup")).count() == 20);
        let control = run.finish();
        p.receiver.shutdown();
        let got = p.got.lock().unwrap().clone();
        let infos = got.iter().filter(|r| r.severity < Severity::ERROR && !has(r, "logless.rollup") && !has(r, "logless.anomaly") && !has(r, ATTR_SUPPLEMENT)).count() as u64;
        let counted: u64 = got.iter().filter(|r| has(r, "logless.rollup")).map(rollup_count).sum();
        assert_eq!(2_000, infos + counted, "sent in full plus counted is everything the agent took: {infos} + {counted}");
        assert!(infos < 200, "about 1% of 2000 was sent in full, got {infos}");
        let status = control.status();
        assert_eq!(1, status.fidelity);
        assert!(status.sampled_out > 1_800 && status.rollups_sent >= 1, "{status:?}");
    }

    #[test]
    fn an_anomaly_sends_its_lead_up_and_a_marker_the_peer_can_alert_on() {
        let dir = tempfile::tempdir().unwrap();
        let p = peer(vec![]);
        let run = start_sender(dir.path(), Some(dial(&p, 1)));
        run.tap.offer(&(0..300).map(|i| rec_t(&format!("steady {i}"), Severity::INFO, 1)).collect::<Vec<_>>());
        wait_for("the batch to be taken", || run.control.status().sampled_out >= 280);
        // A log shape the catalog never held.
        run.tap.offer(&[rec_t("something new broke", Severity::INFO, 424_242)]);
        wait_for("the marker", || p.got.lock().unwrap().iter().any(|r| has(r, "logless.anomaly")));
        wait_for("the status to show it", || !run.control.status().anomalies.is_empty());
        let status = run.control.status();
        assert_eq!("novel", status.anomalies[0].kind);
        assert_eq!(vec!["api".to_string()], status.boosted_services);
        let got = p.got.lock().unwrap().clone();
        let lead_up = got.iter().filter(|r| r.attributes.iter().any(|a| a.key == ATTR_SUPPLEMENT && a.value == crate::model::AttrValue::Str("lookback:novel".into()))).count();
        assert!(lead_up > 250, "the minute before it went with it: {lead_up}");
        assert!(got.iter().any(|r| r.body == "something new broke"), "and the record itself");
        // While boosted, the service is sent whole even at 1%.
        run.tap.offer(&(0..100).map(|i| rec_t(&format!("during {i}"), Severity::INFO, 1)).collect::<Vec<_>>());
        wait_for("everything during the boost", || p.got.lock().unwrap().iter().filter(|r| r.body.starts_with("during")).count() == 100);
        run.finish();
        p.receiver.shutdown();
    }

    fn write_store(dir: &Path, records: Vec<LogRecord>) {
        let buckets = crate::config::LevelBuckets::default();
        let mut by_partition: std::collections::HashMap<_, Vec<LogRecord>> = std::collections::HashMap::new();
        for record in records {
            let key = crate::partition::PartitionKey { epoch_hour: record.observed_unix_nano / 3_600_000_000_000, bucket: buckets.bucket_for(record.severity).name.clone() };
            by_partition.entry(key).or_default().push(record);
        }
        for (key, mut records) in by_partition {
            crate::schema::sort_records(&mut records);
            let partition_dir = key.dir(dir);
            std::fs::create_dir_all(&partition_dir).unwrap();
            crate::store::write_partition_file(&partition_dir.join(crate::store::file_name_for_segment(1, Uuid::now_v7())), &records).unwrap();
        }
    }
    use uuid::Uuid;

    #[test]
    fn a_manual_fetch_sends_a_range_in_full_and_flags_what_the_dial_had_left_out() {
        let dir = tempfile::tempdir().unwrap();
        let store = tempfile::tempdir().unwrap();
        let hour = 3_600_000_000_000u64;
        let base = 1_000 * hour;
        let mut all = Vec::new();
        for i in 0..400u64 {
            let mut r = record(&format!("line {i}"), if i % 50 == 0 { Severity::ERROR } else { Severity::INFO });
            r.observed_unix_nano = base + i * 10_000_000_000;
            r.timestamp_unix_nano = r.observed_unix_nano;
            all.push(r);
        }
        write_store(store.path(), all.clone());
        let p = peer(vec![]);
        let run = start_sender(dir.path(), Some(dial(&p, 20)));
        run.control.set_store_dir(store.path().to_path_buf());
        // The second hour only: records 360..400 are in [base + 3600 s, ...), plus a few before.
        let (from_ms, to_ms) = ((base + hour) / 1_000_000, (base + 2 * hour) / 1_000_000);
        let in_range: Vec<&LogRecord> = all.iter().filter(|r| r.observed_unix_nano >= base + hour).collect();
        assert!(!in_range.is_empty());
        run.control.start_backfill(from_ms, to_ms).unwrap();
        wait_for("the fetch to finish", || run.control.status().backfill.as_ref().is_some_and(|b| b.state == "done"));
        wait_for("delivery", || p.got.lock().unwrap().len() == in_range.len());
        let got = p.got.lock().unwrap().clone();
        let planner_probe = crate::sync_policy::select;
        for r in &got {
            let sent_live = r.severity >= Severity::ERROR || planner_probe(&r.event_id, 20);
            assert_eq!(!sent_live, has(r, ATTR_SUPPLEMENT), "only what the dial left out is flagged: {}", r.body);
        }
        let status = run.control.status();
        assert_eq!(in_range.len() as u64, status.backfill.unwrap().queued);
        // A second fetch of the same range is recognised by the peer: nothing is stored twice.
        run.control.start_backfill(from_ms, to_ms).unwrap();
        wait_for("the second fetch", || run.control.status().duplicates >= in_range.len() as u64);
        assert_eq!(in_range.len(), p.got.lock().unwrap().len());
        run.finish();
        p.receiver.shutdown();
    }

    #[test]
    fn a_fetch_is_refused_with_a_reason_when_it_cannot_run() {
        let dir = tempfile::tempdir().unwrap();
        let none = start_sender(dir.path(), None);
        assert!(none.control.start_backfill(0, 1_000).unwrap_err().contains("Set a peer"));
        none.finish();
        let p = peer(vec![]);
        let dir2 = tempfile::tempdir().unwrap();
        let run = start_sender(dir2.path(), Some(target_of(&p, None, 0)));
        assert!(run.control.start_backfill(5, 5).unwrap_err().contains("after its start"));
        assert!(run.control.start_backfill(0, 40 * 24 * 3_600_000).unwrap_err().contains("31 days"));
        assert!(run.control.start_backfill(0, 1_000).unwrap_err().contains("cannot read its store"));
        run.finish();
        p.receiver.shutdown();
    }
}
