//! The edge's side of the part lifecycle (`docs/adr/0001-parts-tiering-and-lifecycle.md`, `docs/peer-sync.md`).
//!
//! `parts` uploads committed Parquet files to central on request. This module is what makes that a lifecycle
//! an operator can leave alone:
//!
//! * **Pin.** A closed file central has not committed is the only copy of its rows, so retention and
//!   disk-pressure reclaim leave it alone, up to a limit in bytes and in age (`pin_bytes`, `pin_age`, and never
//!   more than a quarter of the disk budget). Past the limit the edge reclaims as it always did, and what it
//!   gives up is counted exactly (files, rows, bytes) and persisted.
//! * **Retire.** A file this edge uploaded and later lost (retention, reclaim, compaction) has its line in
//!   `parts-uploaded.jsonl` and no file. That is the whole queue: a pass tells central (`POST /v1/parts/retire`),
//!   then forgets the line. A restart finds the same lines, so it forgets nothing; a crash between the answer
//!   and the forgetting sends one more retire, which central answers 404 and the edge treats as done.
//! * **Timer.** With `interval` set, a thread runs the upload job for every unconfirmed file at that period.
//!   It starts the same job a manual upload does, which refuses to start while one runs, so jobs never overlap.
//!
//! Compaction replaces small files with a merged one. The merged file is a new, unconfirmed file with a new
//! name; the small ones it replaced, if uploaded, are retired. Central then holds the rows twice until the old
//! part expires, and readers of parts dedupe on event id, as `docs/peer-sync.md` says.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::parts::{self, Tuning};
use crate::sync::SyncControl;

/// A job range end far enough ahead to cover every file (year 2255, and still a valid nanosecond count).
pub const ALL_TIME_END_MS: u64 = 9_000_000_000_000;
/// The file in the data dir that adds up what the edge gave up.
pub const GAVE_UP_FILE: &str = "parts-gaveup.jsonl";
const MIN_INTERVAL: Duration = Duration::from_secs(10);
/// Pins never take more than this share of the disk budget, so pinning cannot be what fills the disk.
const MAX_PIN_SHARE: u64 = 4;
const TICK: Duration = Duration::from_millis(200);
const MAX_RETIRE_BACKOFF_MS: u64 = 300_000;

/// `[sync_parts]`. Everything is off until `interval` is set: an upgrade must not start moving gigabytes, nor
/// start holding files back from reclaim, on a machine nobody asked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgePartsConfig {
    /// How often the upload job runs by itself; `0s` (the default) is off. At least 10 s otherwise.
    /// Pinning is on exactly when this is set, because only then is something going to upload what is pinned.
    #[serde(default, with = "humantime_serde")]
    pub interval: Duration,
    /// The most bytes of unconfirmed files that retention and reclaim hold back. Newest files are pinned first.
    #[serde(default = "default_pin_bytes")]
    pub pin_bytes: u64,
    /// An unconfirmed file older than this (by when it was written) is no longer held back.
    #[serde(default = "default_pin_age", with = "humantime_serde")]
    pub pin_age: Duration,
}

fn default_pin_bytes() -> u64 {
    1024 * 1024 * 1024
}
fn default_pin_age() -> Duration {
    Duration::from_secs(7 * 24 * 3_600)
}

impl Default for EdgePartsConfig {
    fn default() -> Self {
        Self { interval: Duration::ZERO, pin_bytes: default_pin_bytes(), pin_age: default_pin_age() }
    }
}

impl EdgePartsConfig {
    pub fn timer_on(&self) -> bool {
        !self.interval.is_zero()
    }

    pub fn check(&self) -> Result<(), String> {
        if self.timer_on() && self.interval < MIN_INTERVAL {
            return Err("must be 0s (off) or at least 10s".into());
        }
        Ok(())
    }
}

/// What the edge reports in `GET /admin/v1/sync` as `edge_parts`.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct EdgePartsStatus {
    /// The timer's period in seconds; 0 is off.
    pub interval_secs: u64,
    pub last_run_ms: Option<u64>,
    /// `done`, `error`, `cancelled` or `running`, for the last run the timer started.
    pub last_run_state: Option<String>,
    pub last_run_uploaded: u64,
    pub last_run_error: Option<String>,
    pub next_run_ms: Option<u64>,
    pub runs: u64,
    /// Ticks that found another upload running and left it alone.
    pub skipped_busy: u64,
    /// Closed files central has not confirmed, and their bytes.
    pub backlog_parts: u64,
    pub backlog_bytes: u64,
    pub pin_enabled: bool,
    /// The byte limit in force: `pin_bytes`, or a quarter of the disk budget if that is less.
    pub pin_limit_bytes: u64,
    pub pin_age_secs: u64,
    pub pinned_parts: u64,
    pub pinned_bytes: u64,
    /// Unconfirmed files past the limit or too old: reclaim may take these.
    pub unpinned_parts: u64,
    pub unpinned_bytes: u64,
    /// Unconfirmed files retention or reclaim deleted anyway, since the edge started plus what earlier runs
    /// recorded in `parts-gaveup.jsonl`.
    pub gave_up_parts: u64,
    pub gave_up_rows: u64,
    pub gave_up_bytes: u64,
    /// Uploaded files removed locally whose retire central has not yet heard.
    pub retire_pending: u64,
    pub retired: u64,
    pub retire_error: Option<String>,
}

/// Unconfirmed files lost to retention or reclaim.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GaveUp {
    pub parts: u64,
    pub rows: u64,
    pub bytes: u64,
}

/// The files retention and reclaim must not delete this pass.
#[derive(Debug, Clone, Default)]
pub struct PinSet {
    pub pinned: HashSet<PathBuf>,
    /// Every closed file central has not confirmed, pinned or not: deleting one loses data.
    pub unconfirmed: HashSet<PathBuf>,
}

impl PinSet {
    pub fn contains(&self, path: &Path) -> bool {
        self.pinned.contains(path)
    }

    pub fn is_empty(&self) -> bool {
        self.pinned.is_empty()
    }
}

/// A closed file central has not confirmed.
#[derive(Debug, Clone)]
pub struct Unconfirmed {
    pub path: PathBuf,
    pub bytes: u64,
    /// Milliseconds since it was written.
    pub age_ms: u64,
}

fn now_ms() -> u64 {
    crate::now_unix_nanos() / 1_000_000
}

/// Closed Parquet files with no line in the upload state (or a line for a different size). Newest first.
pub fn unconfirmed(store_dir: &Path, state_dir: &Path, now_ms: u64) -> Vec<Unconfirmed> {
    let done = parts::load_state(state_dir);
    let mut out: Vec<Unconfirmed> = crate::store::list_parquet_files(store_dir)
        .into_iter()
        .filter_map(|path| {
            let meta = std::fs::metadata(&path).ok()?;
            let relative = path.strip_prefix(store_dir).unwrap_or(&path).to_string_lossy().into_owned();
            if done.get(&relative) == Some(&meta.len()) {
                return None;
            }
            let written_ms = meta.modified().ok()?.duration_since(std::time::UNIX_EPOCH).ok()?.as_millis() as u64;
            Some(Unconfirmed { path, bytes: meta.len(), age_ms: now_ms.saturating_sub(written_ms) })
        })
        .collect();
    out.sort_by(|a, b| a.age_ms.cmp(&b.age_ms).then_with(|| a.path.cmp(&b.path)));
    out
}

/// Which of `files` (newest first) are pinned: while they are young enough and fit in the limit. The first that
/// does not fit ends the run, so the pinned set is always the newest N files, never a scattering.
pub fn plan(files: &[Unconfirmed], config: &EdgePartsConfig, disk_budget_bytes: u64) -> (PinSet, EdgePartsStatus) {
    let limit = if disk_budget_bytes == 0 { config.pin_bytes } else { config.pin_bytes.min(disk_budget_bytes / MAX_PIN_SHARE) };
    let max_age_ms = config.pin_age.as_millis() as u64;
    let mut set = PinSet::default();
    let mut status = EdgePartsStatus { pin_enabled: true, pin_limit_bytes: limit, pin_age_secs: config.pin_age.as_secs(), ..Default::default() };
    let mut full = false;
    for f in files {
        set.unconfirmed.insert(f.path.clone());
        if !full && f.age_ms <= max_age_ms && status.pinned_bytes + f.bytes <= limit {
            set.pinned.insert(f.path.clone());
            status.pinned_parts += 1;
            status.pinned_bytes += f.bytes;
        } else {
            full = true;
            status.unpinned_parts += 1;
            status.unpinned_bytes += f.bytes;
        }
    }
    (set, status)
}

/// The pins for one maintenance pass, and the pin half of the status. Nothing is pinned unless the timer is on and
/// a peer is set: with nobody to upload to, holding files back would only fill the disk.
pub fn pins_for_pass(control: &SyncControl, config: &EdgePartsConfig, store_dir: &Path, disk_budget_bytes: u64) -> PinSet {
    if !config.timer_on() || !control.is_active() {
        control.update_edge_parts(|s| {
            s.pin_enabled = false;
            s.pinned_parts = 0;
            s.pinned_bytes = 0;
            s.unpinned_parts = 0;
            s.unpinned_bytes = 0;
        });
        return PinSet::default();
    }
    let files = unconfirmed(store_dir, control.store().dir(), now_ms());
    let (set, planned) = plan(&files, config, disk_budget_bytes);
    control.update_edge_parts(|s| {
        s.pin_enabled = true;
        s.pin_limit_bytes = planned.pin_limit_bytes;
        s.pin_age_secs = planned.pin_age_secs;
        s.pinned_parts = planned.pinned_parts;
        s.pinned_bytes = planned.pinned_bytes;
        s.unpinned_parts = planned.unpinned_parts;
        s.unpinned_bytes = planned.unpinned_bytes;
    });
    set
}

/// Counts files the edge deleted before central confirmed them: a warning with the exact numbers, a line in
/// `parts-gaveup.jsonl`, and the totals in status.
pub fn note_gave_up(control: &SyncControl, gave_up: GaveUp) {
    if gave_up.parts == 0 {
        return;
    }
    tracing::warn!(parts = gave_up.parts, rows = gave_up.rows, bytes = gave_up.bytes, "deleted parquet files central had not confirmed (past the pin limit or age)");
    let path = control.store().dir().join(GAVE_UP_FILE);
    let written = serde_json::to_vec(&gave_up).map_err(std::io::Error::other).and_then(|mut line| {
        line.push(b'\n');
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
        file.write_all(&line)?;
        file.sync_data()
    });
    if let Err(e) = written {
        tracing::warn!(error = %e, "could not record deleted unconfirmed parts; the counts in status restart from zero");
    }
    control.update_edge_parts(|s| {
        s.gave_up_parts += gave_up.parts;
        s.gave_up_rows += gave_up.rows;
        s.gave_up_bytes += gave_up.bytes;
    });
}

/// What earlier runs gave up, from `parts-gaveup.jsonl`.
pub fn load_gave_up(state_dir: &Path) -> GaveUp {
    let text = std::fs::read_to_string(state_dir.join(GAVE_UP_FILE)).unwrap_or_default();
    text.lines().filter_map(|l| serde_json::from_str::<GaveUp>(l).ok()).fold(GaveUp::default(), |a, g| GaveUp {
        parts: a.parts + g.parts,
        rows: a.rows + g.rows,
        bytes: a.bytes + g.bytes,
    })
}

/// An uploaded file that is gone from the store.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingRetire {
    pub path: String,
    pub name: String,
    /// False when another uploaded file of the same content is still here: the part stays warm.
    pub tell_central: bool,
}

/// The upload state lines whose file no longer exists. Empty if the store directory itself is gone, which says
/// nothing about the files.
pub fn pending_retires(store_dir: &Path, state_dir: &Path) -> Vec<PendingRetire> {
    if !store_dir.is_dir() {
        return Vec::new();
    }
    let lines = parts::load_state_lines(state_dir);
    let live: HashSet<&str> = lines.iter().filter(|l| store_dir.join(&l.path).exists()).map(|l| l.name.as_str()).collect();
    lines
        .iter()
        .filter(|l| !store_dir.join(&l.path).exists())
        .map(|l| PendingRetire { path: l.path.clone(), name: l.name.clone(), tell_central: !live.contains(l.name.as_str()) })
        .collect()
}

/// Backs off between failed retire passes: 1 s doubling to 5 min.
#[derive(Debug, Default)]
pub struct RetireBackoff {
    fails: u32,
    next_ms: u64,
}

/// Tells central about every pending retire, in order, and forgets those it accepted. The first failure ends
/// the pass and sets the backoff; the rest wait. Returns how many were retired.
pub fn retire_pass(control: &SyncControl, store_dir: &Path, backoff: &mut RetireBackoff, now_ms: u64) -> u64 {
    let state_dir = control.store().dir().to_path_buf();
    let pending = pending_retires(store_dir, &state_dir);
    control.update_edge_parts(|s| s.retire_pending = pending.len() as u64);
    let Some(target) = control.target() else { return 0 };
    if pending.is_empty() || now_ms < backoff.next_ms {
        return 0;
    }
    let (mut forget, mut retired) = (Vec::new(), 0u64);
    let mut failure = None;
    for p in &pending {
        if p.tell_central {
            if let Err(m) = parts::retire_remote(&target, &p.name) {
                failure = Some(m);
                break;
            }
            retired += 1;
        }
        forget.push(p.path.clone());
    }
    if let Err(e) = parts::drop_state(&state_dir, &forget) {
        tracing::warn!(error = %e, "could not forget retired parts; they are retired again next pass");
    }
    match &failure {
        Some(_) => {
            backoff.fails = (backoff.fails + 1).min(20);
            backoff.next_ms = now_ms + (1_000u64 << backoff.fails.min(9)).min(MAX_RETIRE_BACKOFF_MS);
        }
        None => *backoff = RetireBackoff::default(),
    }
    let left = (pending.len() - forget.len()) as u64;
    control.update_edge_parts(|s| {
        s.retired += retired;
        s.retire_pending = left;
        s.retire_error = failure;
    });
    retired
}

/// Runs the timer and the retire pass. One thread, so the timer's own runs cannot overlap; a manual upload
/// already running makes the tick wait (and `SyncControl::launch_parts` refuses a second job whoever asks).
pub struct Driver {
    control: Arc<SyncControl>,
    config: EdgePartsConfig,
    store_dir: PathBuf,
    tuning: Tuning,
    started: bool,
    in_flight: bool,
    next_run_ms: u64,
    next_backlog_ms: u64,
    next_retire_ms: u64,
    retire: RetireBackoff,
}

const BACKLOG_EVERY_MS: u64 = 5_000;
const RETIRE_EVERY_MS: u64 = 10_000;

impl Driver {
    pub fn new(control: Arc<SyncControl>, config: EdgePartsConfig, store_dir: PathBuf, tuning: Tuning) -> Self {
        let gave_up = load_gave_up(control.store().dir());
        control.update_edge_parts(|s| {
            s.interval_secs = config.interval.as_secs();
            s.gave_up_parts = gave_up.parts;
            s.gave_up_rows = gave_up.rows;
            s.gave_up_bytes = gave_up.bytes;
        });
        Self { control, config, store_dir, tuning, started: false, in_flight: false, next_run_ms: 0, next_backlog_ms: 0, next_retire_ms: 0, retire: RetireBackoff::default() }
    }

    /// One look at the clock: collects a finished run, starts a due one, refreshes the backlog, retires.
    pub fn step(&mut self, now_ms: u64) {
        if self.in_flight {
            if let Some(job) = self.control.status().parts.filter(|p| p.state != "running") {
                self.in_flight = false;
                self.control.update_edge_parts(|s| {
                    s.last_run_state = Some(job.state.clone());
                    s.last_run_uploaded = job.uploaded + job.already_present;
                    s.last_run_error = job.error.clone();
                });
            }
        }
        if !self.control.is_active() {
            self.control.update_edge_parts(|s| s.next_run_ms = None);
        } else if self.config.timer_on() && !self.in_flight {
            if !self.started {
                self.started = true;
                self.next_run_ms = now_ms;
            }
            if now_ms >= self.next_run_ms {
                self.next_run_ms = now_ms + self.config.interval.as_millis() as u64;
                let next = self.next_run_ms;
                match self.control.start_parts_all(self.tuning) {
                    Ok(()) => {
                        self.in_flight = true;
                        self.control.update_edge_parts(|s| {
                            s.runs += 1;
                            s.last_run_ms = Some(now_ms);
                            s.last_run_state = Some("running".into());
                            s.last_run_error = None;
                            s.next_run_ms = Some(next);
                        });
                    }
                    Err(m) => self.control.update_edge_parts(|s| {
                        s.skipped_busy += 1;
                        s.last_run_error = Some(m);
                        s.next_run_ms = Some(next);
                    }),
                }
            }
        }
        if self.config.timer_on() && now_ms >= self.next_backlog_ms {
            self.next_backlog_ms = now_ms + BACKLOG_EVERY_MS;
            let files = unconfirmed(&self.store_dir, self.control.store().dir(), now_ms);
            self.control.update_edge_parts(|s| {
                s.backlog_parts = files.len() as u64;
                s.backlog_bytes = files.iter().map(|f| f.bytes).sum();
            });
        }
        if now_ms >= self.next_retire_ms {
            self.next_retire_ms = now_ms + RETIRE_EVERY_MS;
            retire_pass(&self.control, &self.store_dir, &mut self.retire, now_ms);
        }
    }

    /// Makes the next `step` retire (and refresh the backlog) at once; for tests and for a fresh start.
    pub fn due_now(&mut self) {
        self.next_backlog_ms = 0;
        self.next_retire_ms = 0;
        self.retire = RetireBackoff::default();
    }
}

/// Starts the thread that drives [`Driver`] until `stop` is set.
pub fn spawn(control: Arc<SyncControl>, config: EdgePartsConfig, store_dir: PathBuf, stop: Arc<AtomicBool>) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name("logless-parts-timer".into()).spawn(move || {
        let mut driver = Driver::new(control, config, store_dir, Tuning::default());
        while !stop.load(Ordering::Acquire) {
            driver.step(now_ms());
            std::thread::sleep(TICK);
        }
    })
}

/// Deleted-file accounting for `budget::expire_and_relieve`: of the unconfirmed files the catalog knew before the
/// pass, those that are gone now.
pub fn gave_up_of(before: &[crate::catalog::FileEntry]) -> GaveUp {
    before.iter().filter(|f| !f.path.exists()).fold(GaveUp::default(), |g, f| GaveUp { parts: g.parts + 1, rows: g.rows + f.stats.rows, bytes: g.bytes + f.stats.bytes })
}

/// The state lines by relative path, for tests and diagnostics.
pub fn uploaded_names(state_dir: &Path) -> HashMap<String, String> {
    parts::load_state_lines(state_dir).into_iter().map(|l| (l.path, l.name)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_store_directory_retires_nothing_and_a_removed_file_is_pending() {
        let data = tempfile::tempdir().unwrap();
        let store = tempfile::tempdir().unwrap();
        let line = |path: &str, name: &str| serde_json::json!({"path": path, "size": 1, "name": name}).to_string();
        std::fs::write(store.path().join("a.parquet"), b"x").unwrap();
        std::fs::write(data.path().join(parts::STATE_FILE), [line("a.parquet", "n1"), line("b.parquet", "n2"), line("c.parquet", "n1")].join("\n") + "\n").unwrap();
        // b is gone: pending. c is gone but its content is still held as a: forgotten without telling central.
        let p = pending_retires(store.path(), data.path());
        assert_eq!(
            vec![("b.parquet", "n2", true), ("c.parquet", "n1", false)],
            p.iter().map(|p| (p.path.as_str(), p.name.as_str(), p.tell_central)).collect::<Vec<_>>()
        );
        // The store directory itself missing says nothing about the files.
        assert!(pending_retires(&store.path().join("absent"), data.path()).is_empty());
        // Dropping lines keeps the others, in order.
        parts::drop_state(data.path(), &["b.parquet".into()]).unwrap();
        assert_eq!(vec!["a.parquet", "c.parquet"], parts::load_state_lines(data.path()).iter().map(|l| l.path.as_str()).collect::<Vec<_>>());
    }
}
