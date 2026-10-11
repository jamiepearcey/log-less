//! Replication between centrals: a replica copies the parts it lacks from a source by catalog diff.
//!
//! Parts are immutable and named by their SHA-256, so two centrals cannot disagree about one: a copy is
//! right or it is not that part. That leaves no conflict to resolve, only a difference to close, and the
//! replica closes it. The traffic is outbound from the replica only; the source never connects anywhere
//! (`docs/adr/0003-parts-push-to-central-local-backend.md`, "Replica").
//!
//! | Path (source's peer listener, a `replication_tokens` bearer token) | Answer |
//! |---|---|
//! | `GET /v1/parts/replica/catalog?after=<node>/<name>&limit=N` | the catalog in `(node, name)` order after `after`: `{parts: [...], next}` |
//! | `GET /v1/parts/replica/blob/<node>/<name>` | the part's bytes, `Range: bytes=N-` honoured |
//!
//! A run pages through the source's catalog, and for each part it does not hold: if the file is already
//! on its disk (it crashed between the rename and the catalog row) it records it; otherwise it fetches it
//! to `<dir>/<node>/.fetch-<sha256>.tmp`, hashing as the bytes arrive, and only a body whose digest is the
//! name is synced, renamed into place and recorded under its original node. A fetch that is cut off keeps
//! its temporary file; the next run hashes the bytes already there and asks for the rest with a `Range`.
//! A part the replica has recorded is never fetched again.
//!
//! The replica never deletes. A part it holds that the source no longer lists (retired, swept, or never
//! there) is counted as `only_here` and named in the report, and left alone; deleting follows the
//! replica's own retention, not the source's. A source that keeps its parts in a bucket answers 409
//! `not_local` and a replica reports that and does nothing else.

use std::collections::{HashMap, HashSet};
use std::io::Seek;
use std::sync::Arc;

use super::*;

pub const REPLICA_CATALOG_PATH: &str = "/v1/parts/replica/catalog";
/// Source: `GET <REPLICA_BLOB_PREFIX><node>/<name>`.
pub const REPLICA_BLOB_PREFIX: &str = "/v1/parts/replica/blob/";
const FETCH_PREFIX: &str = ".fetch-";
/// A fetch must average at least this rate (after a 30 s allowance) or it is given up and resumed next run.
const FETCH_MIN_BYTES_PER_SEC: u64 = 16 * 1024;
/// Entries kept in a report's lists; the counters beside them are exact.
const REPORT_LIST_MAX: usize = 20;

// ---- source side ---------------------------------------------------------------------------------------

/// One catalog entry as the source lists it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Listed {
    #[serde(flatten)]
    row: crate::catalog::PeerPartRow,
    /// The part's SHA-256, which is its name without the extension, repeated so a replica need not parse it.
    sha256: String,
}

#[derive(Serialize, Deserialize)]
struct Page {
    parts: Vec<Listed>,
    /// `<node>/<name>` of the last entry when there may be more; `null` at the end.
    next: Option<String>,
}

fn split_key(key: &str) -> Option<(&str, &str)> {
    key.split_once('/').filter(|(node, name)| valid_node(node) && valid_name(name))
}

/// `bytes=N-` or `bytes=N-M`: the first and the last byte wanted. Anything else is ignored (the whole part is sent),
/// as the S3 endpoint does.
fn byte_range(value: &str, len: u64) -> Option<Result<(u64, u64), ()>> {
    let (first, last) = value.trim().strip_prefix("bytes=")?.split_once('-')?;
    let first: u64 = first.parse().ok()?;
    let last = if last.is_empty() { None } else { Some(last.parse::<u64>().ok()?) };
    if last.is_some_and(|l| l < first) {
        return None;
    }
    Some(if first >= len { Err(()) } else { Ok((first, last.map_or(len - 1, |l| l.min(len - 1)))) })
}

impl PartsCentral {
    /// Whether this central lets another copy from it: it has at least one replication token.
    pub fn replica_source_enabled(&self) -> bool {
        !self.config.replication_tokens.is_empty()
    }

    /// A request for the replica paths was refused for its token (counted in `parts_store.refused_replica`).
    pub fn count_replica_refused(&self) {
        if let Store::Local(local) = &self.store {
            local.stats.refused_replica.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn replica_local(&self) -> Result<&LocalStore, Reply> {
        match &self.store {
            Store::Local(local) => Ok(local),
            Store::S3 { .. } => Err(error_reply(409, "not_local", "this central keeps its parts in a bucket, which a replica cannot copy through it; replicate the bucket with the storage's own tools")),
        }
    }

    /// `GET /v1/parts/replica/catalog?after=&limit=`.
    pub fn replica_catalog(&self, query: &str) -> Reply {
        let local = match self.replica_local() {
            Ok(l) => l,
            Err(reply) => return reply,
        };
        let (mut after, mut limit) = (None, 500usize);
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            match pair.split_once('=') {
                Some(("after", v)) if split_key(v).is_some() => after = split_key(v),
                Some(("limit", v)) if v.parse::<usize>().is_ok_and(|n| (1..=REPLICA_PAGE_MAX).contains(&n)) => limit = v.parse().unwrap_or(limit),
                _ => return error_reply(400, "bad_request", &format!("after is <node>/<name>, limit is 1 to {REPLICA_PAGE_MAX}")),
            }
        }
        let rows = crate::catalog::Catalog::open(&self.catalog_path).and_then(|c| c.peer_parts_after(after, limit));
        match rows {
            Ok(rows) => {
                local.stats.replica_pages.fetch_add(1, Ordering::Relaxed);
                let next = (rows.len() == limit).then(|| rows.last().map(|r| format!("{}/{}", r.node, r.name))).flatten();
                let parts = rows.into_iter().map(|row| Listed { sha256: row.name[..64].to_string(), row }).collect();
                Reply::json(200, serde_json::to_string(&Page { parts, next }).unwrap_or_default())
            }
            Err(e) => error_reply(503, "catalog_busy", &format!("the catalog could not be read: {e}")).with_header("Retry-After", "1"),
        }
    }

    /// `GET /v1/parts/replica/blob/<node>/<name>`: a catalogued part, whole or from a byte on. A part nobody
    /// committed is not served, and neither is a catalogued part whose file is gone or not the catalogued size.
    pub fn replica_blob(&self, target: &str, range: Option<&str>) -> Reply {
        let local = match self.replica_local() {
            Ok(l) => l,
            Err(reply) => return reply,
        };
        let Some((node, name)) = split_key(target) else {
            return error_reply(404, "not_found", "no such path");
        };
        let row = match crate::catalog::Catalog::open(&self.catalog_path).and_then(|c| c.peer_part(node, name)) {
            Ok(Some(row)) => row,
            Ok(None) => return error_reply(404, "not_catalogued", "this central has no committed part of that name"),
            Err(e) => return error_reply(503, "catalog_busy", &format!("the catalog could not be read: {e}")).with_header("Retry-After", "1"),
        };
        let opened = std::fs::File::open(local.path(node, name)).and_then(|f| f.metadata().map(|m| (f, m)));
        let (mut file, meta) = match opened {
            Ok((f, m)) if m.is_file() && m.len() == row.size => (f, m),
            Ok(_) => return error_reply(404, "part_missing", "the catalog lists this part but the file is not its catalogued size"),
            Err(_) => return error_reply(404, "part_missing", "the catalog lists this part but its file cannot be read"),
        };
        let len = meta.len();
        let (status, first, count) = match range.and_then(|r| byte_range(r, len)) {
            Some(Ok((a, b))) => (206, a, b - a + 1),
            Some(Err(())) => return error_reply(416, "range_not_satisfiable", "the range starts at or past the end of the part").with_header("Content-Range", &format!("bytes */{len}")),
            None => (200, 0, len),
        };
        if first > 0 && file.seek(std::io::SeekFrom::Start(first)).is_err() {
            return error_reply(500, "storage_error", "the part could not be read");
        }
        local.stats.replica_served.fetch_add(1, Ordering::Relaxed);
        let mut reply = Reply::new(status, "application/octet-stream", Vec::new())
            .with_header("Accept-Ranges", "bytes")
            .with_header("ETag", &format!("\"{}\"", &name[..64]));
        if status == 206 {
            reply = reply.with_header("Content-Range", &format!("bytes {first}-{}/{len}", first + count - 1));
        }
        reply.stream = Some(crate::httpd::Streamed { reader: Some(Box::new(file.take(count))), len: count });
        reply
    }
}

// ---- replica side --------------------------------------------------------------------------------------

/// What one run found and did. The counters are exact; the lists hold the first few entries.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct RunReport {
    pub started_ms: u64,
    pub finished_ms: Option<u64>,
    /// The listing was read to its end. Only then is `only_here` a statement about the whole catalog.
    pub complete: bool,
    pub error: Option<String>,
    /// Entries the source listed.
    pub listed: u64,
    /// Already in this catalog: nothing done.
    pub present: u64,
    /// The file was already on this disk and was recorded without fetching it.
    pub adopted: u64,
    /// Fetched, verified and recorded this run.
    pub fetched: u64,
    pub fetched_bytes: u64,
    /// Of `fetched`, how many continued a partial copy an earlier run left, and how many bytes that run did not fetch again.
    pub resumed: u64,
    pub resumed_bytes: u64,
    /// The source's bytes did not hash to the name, or were not the listed size: refused, not recorded.
    pub damaged: u64,
    /// Listed but not servable (its file is missing at the source): not recorded.
    pub unservable: u64,
    /// Entries the source listed that no node directory could hold (a bad name, size or tier): skipped.
    pub invalid: u64,
    /// Present here in a different tier from the source's. Not copied: tiers are each central's own after the first copy.
    pub tier_differs: u64,
    /// Held here and not listed by the source (retired or swept there, or never there). Left alone.
    pub only_here: u64,
    pub problems: Vec<String>,
    pub only_here_sample: Vec<String>,
}

impl RunReport {
    fn problem(&mut self, what: String) {
        if self.problems.len() < REPORT_LIST_MAX {
            self.problems.push(what);
        }
    }
}

/// `[peer.parts.replicate]` state, in `GET /admin/v1/sync` as `parts_replication`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReplicationStatus {
    pub source: String,
    /// `idle` or `running`.
    pub state: &'static str,
    pub interval_secs: u64,
    pub bytes_per_sec: u64,
    pub runs: u64,
    /// The run under way, or the last one.
    pub run: Option<RunReport>,
}

pub(super) struct Replication {
    config: ReplicateConfig,
    running: AtomicBool,
    stop: AtomicBool,
    status: Mutex<ReplicationStatus>,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl Replication {
    pub(super) fn new(config: ReplicateConfig) -> Self {
        let status = ReplicationStatus {
            source: config.url.trim_end_matches('/').to_string(),
            state: "idle",
            interval_secs: config.interval_secs,
            bytes_per_sec: config.bytes_per_sec,
            runs: 0,
            run: None,
        };
        Self { config, running: AtomicBool::new(false), stop: AtomicBool::new(false), status: Mutex::new(status), threads: Mutex::new(Vec::new()) }
    }

    fn status(&self) -> std::sync::MutexGuard<'_, ReplicationStatus> {
        self.status.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn base(&self) -> &str {
        self.config.url.trim_end_matches('/')
    }
}

/// Why a run was not started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicateRefused {
    /// No `[peer.parts.replicate]`.
    NotAReplica,
    /// A run is under way.
    Running,
}

/// Why one part was not copied. `Damaged` and `Unservable` skip the part; the others end the run.
enum Failure {
    Damaged(String),
    Unservable(String),
    /// The source or the link failed; a partial copy is kept for the next run.
    Interrupted(String),
    /// This central cannot go on (its token is refused, the source is not a source, the disk is full).
    Fatal(String),
}

struct Fetched {
    bytes: u64,
    resumed_from: u64,
}

/// Spends a byte budget evenly: after `n` bytes arrive it waits until `n / rate` seconds have been paid for.
struct Pace {
    rate: u64,
    free_at: Instant,
}

impl Pace {
    fn spend(&mut self, n: usize, stop: &AtomicBool) {
        if self.rate == 0 {
            return;
        }
        let now = Instant::now();
        self.free_at = self.free_at.max(now) + Duration::from_secs_f64(n as f64 / self.rate as f64);
        while Instant::now() < self.free_at && !stop.load(Ordering::Acquire) {
            std::thread::sleep((self.free_at - Instant::now()).min(Duration::from_millis(50)));
        }
    }
}

struct Release<'a>(&'a AtomicBool);

impl Drop for Release<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl PartsCentral {
    pub fn replication_status(&self) -> Option<ReplicationStatus> {
        self.replication.as_ref().map(|r| r.status().clone())
    }

    fn claim(&self) -> Result<&Replication, ReplicateRefused> {
        let rep = self.replication.as_ref().ok_or(ReplicateRefused::NotAReplica)?;
        rep.running.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).map_err(|_| ReplicateRefused::Running)?;
        Ok(rep)
    }

    /// One run on the calling thread.
    pub fn replicate_once(&self) -> Result<RunReport, ReplicateRefused> {
        let rep = self.claim()?;
        Ok(self.run_claimed(rep))
    }

    /// One run on a thread of its own (`POST /admin/v1/parts/replicate`); progress is in [`Self::replication_status`].
    pub fn replicate_in_background(self: &Arc<Self>) -> Result<(), ReplicateRefused> {
        let rep = self.claim()?;
        let me = Arc::clone(self);
        let handle = std::thread::Builder::new().name("parts-replicate".into()).spawn(move || {
            if let Some(rep) = me.replication.as_ref() {
                me.run_claimed(rep);
            }
        });
        match handle {
            Ok(h) => rep.threads.lock().unwrap_or_else(|p| p.into_inner()).push(h),
            Err(_) => rep.running.store(false, Ordering::Release),
        }
        Ok(())
    }

    /// Starts the timer when `interval_secs` is above 0: a run now, then one every interval. It holds only a weak
    /// reference, so dropping the central ends it; [`Self::stop_replication`] ends it sooner.
    pub fn start_replication_timer(self: &Arc<Self>) {
        let Some(rep) = self.replication.as_ref().filter(|r| r.config.interval_secs > 0) else { return };
        let interval = Duration::from_secs(rep.config.interval_secs);
        let weak = Arc::downgrade(self);
        let handle = std::thread::Builder::new().name("parts-replicate-timer".into()).spawn(move || loop {
            let Some(me) = weak.upgrade() else { return };
            let Some(rep) = me.replication.as_ref() else { return };
            if rep.stop.load(Ordering::Acquire) {
                return;
            }
            if let Ok(rep) = me.claim() {
                me.run_claimed(rep);
            }
            let until = Instant::now() + interval;
            drop(me);
            while Instant::now() < until {
                std::thread::sleep(Duration::from_millis(100));
                match weak.upgrade() {
                    Some(me) if me.replication.as_ref().is_some_and(|r| !r.stop.load(Ordering::Acquire)) => {}
                    _ => return,
                }
            }
        });
        if let Ok(h) = handle {
            rep.threads.lock().unwrap_or_else(|p| p.into_inner()).push(h);
        }
    }

    /// Asks a run under way to stop at its next chunk (it keeps its partial copy), ends the timer and waits for both.
    pub fn stop_replication(&self) {
        let Some(rep) = &self.replication else { return };
        rep.stop.store(true, Ordering::Release);
        let handles: Vec<_> = rep.threads.lock().unwrap_or_else(|p| p.into_inner()).drain(..).collect();
        for h in handles {
            let _ = h.join();
        }
    }

    fn run_claimed(&self, rep: &Replication) -> RunReport {
        let _release = Release(&rep.running);
        {
            let mut status = rep.status();
            status.state = "running";
            status.runs += 1;
            status.run = Some(RunReport { started_ms: now_ms(), ..RunReport::default() });
        }
        let mut report = RunReport { started_ms: now_ms(), ..RunReport::default() };
        match self.run(rep, &mut report) {
            Ok(()) => report.complete = true,
            Err(m) => report.error = Some(m),
        }
        report.finished_ms = Some(now_ms());
        let mut status = rep.status();
        status.state = "idle";
        status.run = Some(report.clone());
        report
    }

    fn publish(&self, rep: &Replication, report: &RunReport) {
        rep.status().run = Some(report.clone());
    }

    /// One pass over the source's catalog. `Err` ends the run with that reason; the report holds what was done before.
    fn run(&self, rep: &Replication, report: &mut RunReport) -> Result<(), String> {
        let Store::Local(local) = &self.store else { return Err("replication needs the local backend".into()) };
        let control: ureq::Agent = ureq::Agent::config_builder().timeout_global(Some(CONTROL_TIMEOUT)).http_status_as_error(false).build().into();
        // No overall limit on a part, as for an upload: a large one takes as long as the link needs (a body
        // deadline is set per request from its length). Stalls are still bounded.
        let data: ureq::Agent = ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(10)))
            .timeout_recv_response(Some(Duration::from_secs(60)))
            .http_status_as_error(false)
            .build()
            .into();
        let mut catalog = crate::catalog::Catalog::open(&self.catalog_path).map_err(|e| format!("the catalog could not be opened: {e}"))?;
        let mut pace = Pace { rate: rep.config.bytes_per_sec, free_at: Instant::now() };
        let mut after: Option<String> = None;
        let mut previous: Option<(String, String)> = None;
        loop {
            if rep.stop.load(Ordering::Acquire) {
                return Err("stopped".into());
            }
            let page = self.fetch_page(rep, &control, after.as_deref())?;
            // Keys must ascend, or the comparison with this catalog below means nothing.
            let keys: Vec<(&str, &str)> = page.parts.iter().map(|l| (l.row.node.as_str(), l.row.name.as_str())).collect();
            let ascending = keys.windows(2).all(|w| w[0] < w[1]) && keys.first().zip(previous.as_ref()).is_none_or(|(k, p)| *k > (p.0.as_str(), p.1.as_str()));
            if !ascending || (page.parts.is_empty() && page.next.is_some()) {
                return Err("the source's listing is out of order or ends in an empty page".into());
            }
            let last = keys.last().map(|(n, m)| (n.to_string(), m.to_string()));
            let upto = if page.next.is_some() { last.as_ref().map(|(n, m)| (n.as_str(), m.as_str())) } else { None };
            let window: HashMap<(String, String), String> = catalog
                .peer_part_keys(previous.as_ref().map(|(n, m)| (n.as_str(), m.as_str())), upto)
                .map_err(|e| format!("the catalog could not be read: {e}"))?
                .into_iter()
                .map(|(node, name, tier)| ((node, name), tier))
                .collect();
            let listed_keys: HashSet<(&str, &str)> = keys.iter().copied().collect();
            let mut absent: Vec<String> = window.keys().filter(|k| !listed_keys.contains(&(k.0.as_str(), k.1.as_str()))).map(|k| format!("{}/{}", k.0, k.1)).collect();
            absent.sort();
            report.only_here += absent.len() as u64;
            report.only_here_sample.extend(absent.into_iter().take(REPORT_LIST_MAX.saturating_sub(report.only_here_sample.len())));

            for listed in &page.parts {
                if rep.stop.load(Ordering::Acquire) {
                    return Err("stopped".into());
                }
                let part = &listed.row;
                report.listed += 1;
                if let Some(why) = self.invalid_entry(listed) {
                    report.invalid += 1;
                    report.problem(format!("{}/{}: not copied, {why}", part.node, part.name));
                    continue;
                }
                if let Some(tier) = window.get(&(part.node.clone(), part.name.clone())) {
                    report.present += 1;
                    report.tier_differs += u64::from(tier != &part.tier);
                    continue;
                }
                // On this disk already (a crash between the rename and the row, or an upload nobody committed here):
                // a file under its final name is a whole verified part, so it is recorded, not fetched.
                let outcome = match local.present(&part.node, &part.name, part.size) {
                    Ok(true) => Ok((Fetched { bytes: 0, resumed_from: 0 }, true)),
                    Ok(false) => self.fetch_part(rep, local, &data, part, &mut pace).map(|f| (f, false)),
                    Err(m) => Err(Failure::Fatal(m)),
                };
                match outcome {
                    Ok((fetched, adopted)) => {
                        catalog.record_replicated_part(part, now_ms(), rep.base()).map_err(|e| format!("{}/{} was copied but could not be recorded: {e}", part.node, part.name))?;
                        if adopted {
                            report.adopted += 1;
                        } else {
                            report.fetched += 1;
                            report.fetched_bytes += fetched.bytes;
                            if fetched.resumed_from > 0 {
                                report.resumed += 1;
                                report.resumed_bytes += fetched.resumed_from;
                            }
                        }
                    }
                    Err(Failure::Damaged(m)) => {
                        report.damaged += 1;
                        report.problem(format!("{}/{}: refused, {m}", part.node, part.name));
                    }
                    Err(Failure::Unservable(m)) => {
                        report.unservable += 1;
                        report.problem(format!("{}/{}: listed but not served, {m}", part.node, part.name));
                    }
                    Err(Failure::Interrupted(m) | Failure::Fatal(m)) => {
                        self.publish(rep, report);
                        return Err(format!("{}/{}: {m}", part.node, part.name));
                    }
                }
                self.publish(rep, report);
            }
            previous = last;
            match page.next {
                Some(next) => after = Some(next),
                None => return Ok(()),
            }
        }
    }

    /// Why a listed entry cannot become a file in a node directory: the replica believes nothing it is told.
    fn invalid_entry(&self, listed: &Listed) -> Option<&'static str> {
        let part = &listed.row;
        if !valid_node(&part.node) || !valid_name(&part.name) {
            Some("the node or name is not valid")
        } else if listed.sha256 != part.name[..64] {
            Some("the sha256 is not the name")
        } else if part.size == 0 || part.size > self.config.max_part_bytes {
            Some("the size is not 1 to max_part_bytes")
        } else if !matches!(part.tier.as_str(), "warm" | "cold") || part.min_unix_nano > part.max_unix_nano {
            Some("the tier or the time range is not valid")
        } else {
            None
        }
    }

    fn fetch_page(&self, rep: &Replication, agent: &ureq::Agent, after: Option<&str>) -> Result<Page, String> {
        let mut url = format!("{}{REPLICA_CATALOG_PATH}?limit={}", rep.base(), rep.config.page_size);
        if let Some(after) = after {
            url.push_str(&format!("&after={after}"));
        }
        let mut response = agent.get(&url).header("Authorization", &format!("Bearer {}", rep.config.token)).call().map_err(|e| format!("the source could not be reached: {e}"))?;
        let status = response.status().as_u16();
        match status {
            200 => crate::sync::read_json::<Page>(response.body_mut()).ok_or_else(|| "the source's catalog page is unreadable".to_string()),
            401 | 403 => Err(format!("the source refused the replication token (HTTP {status})")),
            404 => Err("the source does not offer replication (is it a log-less peer with [peer.parts] replication_tokens?)".into()),
            _ => {
                let body: serde_json::Value = crate::sync::read_json(response.body_mut()).unwrap_or(serde_json::Value::Null);
                Err(format!("the source answered HTTP {status}: {}", body["message"].as_str().unwrap_or("")).trim().to_string())
            }
        }
    }

    /// Copies one part into place. The disk budget is reserved for the whole part while it is fetched.
    fn fetch_part(&self, rep: &Replication, local: &LocalStore, agent: &ureq::Agent, part: &crate::catalog::PeerPartRow, pace: &mut Pace) -> Result<Fetched, Failure> {
        let Some(_arriving) = local.begin(&part.node, &part.name) else {
            return Err(Failure::Interrupted("an upload of this part is under way here; the next run will see it".into()));
        };
        if !local.room(part.size, true) {
            return Err(Failure::Fatal("this central's parts directory is at its disk budget ([peer.parts] disk_budget_bytes)".into()));
        }
        let result = self.fetch_into_place(rep, local, agent, part, pace);
        local.settle(part.size, result.is_ok());
        if result.is_err() {
            // A copy that got nowhere (the source would not serve it) leaves no empty file to be mistaken for progress.
            let tmp = local.dir.join(&part.node).join(format!("{FETCH_PREFIX}{}{TMP_SUFFIX}", &part.name[..64]));
            if std::fs::metadata(&tmp).is_ok_and(|m| m.len() == 0) {
                let _ = std::fs::remove_file(&tmp);
            }
        }
        result
    }

    fn fetch_into_place(&self, rep: &Replication, local: &LocalStore, agent: &ureq::Agent, part: &crate::catalog::PeerPartRow, pace: &mut Pace) -> Result<Fetched, Failure> {
        let disk = |what: &str, e: std::io::Error| if e.kind() == std::io::ErrorKind::StorageFull { Failure::Fatal("the disk is full".into()) } else { Failure::Fatal(format!("{what}: {e}")) };
        let (node, name, size) = (part.node.as_str(), part.name.as_str(), part.size);
        let node_dir = local.dir.join(node);
        std::fs::create_dir_all(&node_dir).map_err(|e| disk("the node directory could not be created", e))?;
        let tmp = node_dir.join(format!("{FETCH_PREFIX}{}{TMP_SUFFIX}", &name[..64]));

        // What an earlier run left: hash it, then ask for the rest.
        let mut hasher = Sha256::new();
        let mut have = 0u64;
        if let Ok(meta) = std::fs::symlink_metadata(&tmp) {
            if meta.is_file() && meta.len() <= size {
                let mut old = std::fs::File::open(&tmp).map_err(|e| disk("a partial copy could not be read", e))?;
                have = copy_hashing_into(&mut old, meta.len(), &mut hasher).map_err(|e| disk("a partial copy could not be read", e))?;
            } else {
                let _ = std::fs::remove_file(&tmp);
            }
        }
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&tmp).map_err(|e| disk("the partial copy could not be opened", e))?;
        let mut resumed_from = have;
        let url = format!("{}{REPLICA_BLOB_PREFIX}{node}/{name}", rep.base());
        let rate = pace.rate;

        let mut remaining_attempts = 2;
        while have < size && remaining_attempts > 0 {
            remaining_attempts -= 1;
            let mut request = agent.get(&url).header("Authorization", &format!("Bearer {}", rep.config.token));
            if have > 0 {
                request = request.header("Range", &format!("bytes={have}-"));
            }
            let floor = if rate > 0 { (rate / 2).max(1) } else { FETCH_MIN_BYTES_PER_SEC };
            let body_limit = Duration::from_secs(30) + Duration::from_secs((size - have) / floor);
            let mut response = request.config().timeout_recv_body(Some(body_limit)).build().call().map_err(|e| Failure::Interrupted(format!("the source could not be reached: {e}")))?;
            let status = response.status().as_u16();
            let from_zero = |file: &std::fs::File, hasher: &mut Sha256| -> Result<(), Failure> {
                file.set_len(0).map_err(|e| disk("the partial copy could not be restarted", e))?;
                *hasher = Sha256::new();
                Ok(())
            };
            let expected = match status {
                200 => {
                    // The whole part, whatever was asked: whatever was kept is thrown away.
                    if have > 0 {
                        from_zero(&file, &mut hasher)?;
                        (have, resumed_from) = (0, 0);
                    }
                    size
                }
                206 => {
                    let range = response.headers().get("content-range").and_then(|v| v.to_str().ok()).unwrap_or("");
                    let starts_here = range.strip_prefix("bytes ").and_then(|r| r.split_once('/')).is_some_and(|(span, total)| span.split('-').next() == Some(&have.to_string()) && total == size.to_string());
                    if !starts_here {
                        from_zero(&file, &mut hasher)?;
                        (have, resumed_from) = (0, 0);
                        continue;
                    }
                    size - have
                }
                416 => {
                    from_zero(&file, &mut hasher)?;
                    (have, resumed_from) = (0, 0);
                    continue;
                }
                404 => {
                    let body: serde_json::Value = crate::sync::read_json(response.body_mut()).unwrap_or(serde_json::Value::Null);
                    return Err(Failure::Unservable(body["message"].as_str().unwrap_or("HTTP 404").to_string()));
                }
                401 | 403 => return Err(Failure::Fatal(format!("the source refused the replication token (HTTP {status})"))),
                other => return Err(Failure::Interrupted(format!("the source answered HTTP {other}"))),
            };
            let declared = response.headers().get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
            if declared != Some(expected) {
                return Err(Failure::Damaged(format!("the source serves {} bytes where its catalog says {size}", declared.map_or("an unknown number of".to_string(), |d| (have + d).to_string()))));
            }
            let mut body = response.into_body().into_reader().take(expected);
            let mut buf = vec![0u8; if rate > 0 { (rate as usize / 16).clamp(1024, 256 * 1024) } else { 256 * 1024 }];
            let mut got = 0u64;
            while got < expected {
                if rep.stop.load(Ordering::Acquire) {
                    return Err(Failure::Interrupted("stopped; the partial copy is kept".into()));
                }
                let n = match body.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(Failure::Interrupted(format!("the transfer failed at {} of {size} bytes: {e}; the partial copy is kept", have + got))),
                };
                hasher.update(&buf[..n]);
                file.write_all(&buf[..n]).map_err(|e| disk("the partial copy could not be written", e))?;
                got += n as u64;
                pace.spend(n, &rep.stop);
            }
            have += got;
            if got < expected {
                return Err(Failure::Interrupted(format!("the source ended at {have} of {size} bytes; the partial copy is kept")));
            }
        }
        if have != size {
            return Err(Failure::Interrupted("the source would not serve the part from its start".into()));
        }
        let digest = hex(&hasher.finalize());
        if digest != name[..64] {
            drop(file);
            let _ = std::fs::remove_file(&tmp);
            return Err(Failure::Damaged(format!("the {size} bytes it served hash to {digest}, not the name")));
        }
        file.sync_all().map_err(|e| disk("the copy could not be synced", e))?;
        let _installing = local.install.lock().unwrap_or_else(|p| p.into_inner());
        if matches!(local.present(node, name, size), Ok(true)) {
            // An upload of the same part won the race: the same bytes, already in place.
            let _ = std::fs::remove_file(&tmp);
        } else {
            std::fs::rename(&tmp, local.path(node, name))
                .and_then(|()| sync_dir(&node_dir))
                .and_then(|()| sync_dir(&local.dir))
                .map_err(|e| disk("the copy could not be installed", e))?;
        }
        Ok(Fetched { bytes: size - resumed_from, resumed_from })
    }
}

/// Reads `len` bytes of `from` into `hasher`.
fn copy_hashing_into(from: &mut dyn Read, len: u64, hasher: &mut Sha256) -> std::io::Result<u64> {
    let mut buf = vec![0u8; 256 * 1024];
    let mut left = len;
    while left > 0 {
        let want = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        match from.read(&mut buf[..want])? {
            0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            n => {
                hasher.update(&buf[..n]);
                left -= n as u64;
            }
        }
    }
    Ok(len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, PeerPartRow};
    use crate::peer::{self, Admitted, PeerConfig, PeerNode};
    use std::io::BufRead as _;
    use std::net::{SocketAddr, TcpListener, TcpStream};

    const REPL_TOKEN: &str = "replication-token-of-the-source";

    struct Central {
        receiver: peer::Receiver,
        root: tempfile::TempDir,
    }

    impl Central {
        fn addr(&self) -> SocketAddr {
            self.receiver.local_addr()
        }
        fn parts(&self) -> Arc<PartsCentral> {
            self.receiver.parts_handle().unwrap()
        }
        fn dir(&self) -> PathBuf {
            self.root.path().join("parts")
        }
        fn rows(&self) -> Vec<PeerPartRow> {
            Catalog::open(&self.root.path().join("catalog.sqlite")).unwrap().peer_parts_after(None, 10_000).unwrap()
        }
        /// `<node>/<file>` of every file under the parts directory, temporary files included, with its SHA-256.
        fn files(&self) -> Vec<(String, String)> {
            let mut out = Vec::new();
            for node in std::fs::read_dir(self.dir()).into_iter().flatten().flatten() {
                for f in std::fs::read_dir(node.path()).into_iter().flatten().flatten() {
                    let bytes = std::fs::read(f.path()).unwrap();
                    out.push((format!("{}/{}", node.file_name().to_string_lossy(), f.file_name().to_string_lossy()), hex(&Sha256::digest(&bytes))));
                }
            }
            out.sort();
            out
        }
    }

    fn replicate_to(url: &str, token: &str, page_size: usize) -> ReplicateConfig {
        ReplicateConfig { url: url.into(), token: token.into(), interval_secs: 0, bytes_per_sec: 0, page_size }
    }

    fn start_in(root: tempfile::TempDir, replication_tokens: Vec<String>, replicate: Option<ReplicateConfig>) -> Central {
        let config = PeerConfig {
            enabled: true,
            addr: "127.0.0.1:0".into(),
            nodes: vec![PeerNode { id: "edge-a".into(), token: "tok-a-0123456789".into() }, PeerNode { id: "edge-b".into(), token: "tok-b-0123456789".into() }],
            parts: PartsConfig { enabled: true, replication_tokens, replicate, ..PartsConfig::default() },
            ..PeerConfig::default()
        };
        let receiver = peer::Receiver::start_with_catalog(&config, "central".into(), Some(root.path().join("catalog.sqlite")), |_| Admitted::Accepted).unwrap();
        Central { receiver, root }
    }

    fn source() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    /// Writes a part of `len` bytes under `<root>/parts/<node>/` and commits it to `<root>/catalog.sqlite`, as an upload would
    /// have, before the central that owns them starts. Returns its name and bytes.
    fn seed(root: &Path, node: &str, tag: u8, len: usize) -> (String, Vec<u8>) {
        let body: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(tag)).collect();
        let sha256 = hex(&Sha256::digest(&body));
        let name = name_for(&sha256);
        std::fs::create_dir_all(root.join("parts").join(node)).unwrap();
        std::fs::write(root.join("parts").join(node).join(&name), &body).unwrap();
        let part = CommittedPart { name: name.clone(), size: len as u64, sha256, rows: len as u64 / 10, min_unix_nano: u64::from(tag) * 1_000, max_unix_nano: u64::from(tag) * 1_000 + 500, origin: "edge-origin".into(), node: None };
        Catalog::open(&root.join("catalog.sqlite")).unwrap().record_peer_part(node, &part, 1_700_000_000_000).unwrap();
        (name, body)
    }

    fn get(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> (u16, Vec<(String, String)>, Vec<u8>) {
        use std::io::{Read as _, Write as _};
        let mut head = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n");
        for (k, v) in headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        head.push_str("\r\n");
        let mut conn = TcpStream::connect(addr).unwrap();
        conn.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        conn.write_all(head.as_bytes()).unwrap();
        let mut out = Vec::new();
        let _ = conn.read_to_end(&mut out);
        let split = out.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(out.len());
        let text = String::from_utf8_lossy(&out[..split]).into_owned();
        let mut lines = text.lines();
        let status = lines.next().and_then(|l| l.split(' ').nth(1)).and_then(|s| s.parse().ok()).unwrap_or(0);
        let headers = lines.filter_map(|l| l.split_once(": ")).map(|(k, v)| (k.to_ascii_lowercase(), v.to_string())).collect();
        (status, headers, out.get(split + 4..).unwrap_or_default().to_vec())
    }

    fn bearer(token: &str) -> [(&str, String); 1] {
        [("Authorization", format!("Bearer {token}"))]
    }

    fn authed(addr: SocketAddr, path: &str, token: &str, extra: &[(&str, &str)]) -> (u16, Vec<(String, String)>, Vec<u8>) {
        let [(k, v)] = bearer(token);
        let mut headers = vec![(k, v.as_str())];
        headers.extend_from_slice(extra);
        get(addr, path, &headers)
    }

    fn run(replica: &Central) -> RunReport {
        replica.parts().replicate_once().unwrap()
    }

    /// Both the catalogs and every file under the two parts directories are the same.
    fn same_parts(a: &Central, b: &Central) {
        let strip = |rows: Vec<PeerPartRow>| rows.into_iter().map(|r| (r.node, r.name, r.size, r.rows, r.min_unix_nano, r.max_unix_nano, r.origin, r.tier)).collect::<Vec<_>>();
        assert_eq!(strip(a.rows()), strip(b.rows()));
        assert_eq!(a.files(), b.files());
    }

    #[test]
    fn a_replica_copies_every_part_by_catalog_diff_and_a_second_run_fetches_nothing() {
        let root = source();
        let mut total = 0u64;
        for (node, tag, len) in [("edge-a", 1, 3_000), ("edge-a", 2, 5_000), ("edge-a", 3, 70_000), ("edge-b", 4, 1_000), ("edge-b", 5, 9_000), ("edge-b", 6, 300_000), ("zeta", 7, 2_000)] {
            seed(root.path(), node, tag, len);
            total += len as u64;
        }
        let source = start_in(root, vec![REPL_TOKEN.into()], None);
        // Pages of three: 7 parts take three requests, the last one short.
        let replica = start_in(source_dir(), vec![], Some(replicate_to(&format!("http://{}", source.addr()), REPL_TOKEN, 3)));
        let report = run(&replica);
        assert_eq!(None, report.error);
        assert!(report.complete);
        assert_eq!((7, 0, 0, 7, total), (report.listed, report.present, report.adopted, report.fetched, report.fetched_bytes), "{report:?}");
        assert_eq!((0, 0, 0, 0, 0, 0), (report.damaged, report.unservable, report.invalid, report.only_here, report.resumed, report.tier_differs));
        same_parts(&source, &replica);
        // The original node is kept, so a restore from the replica finds the parts where the edge's inventory says.
        assert_eq!(vec!["edge-a", "edge-a", "edge-a", "edge-b", "edge-b", "edge-b", "zeta"], replica.rows().iter().map(|r| r.node.as_str()).collect::<Vec<_>>());
        let audit = Catalog::open(&replica.root.path().join("catalog.sqlite")).unwrap().peer_parts_audit(100).unwrap();
        assert_eq!(7, audit.iter().filter(|a| a.action == "replicate" && a.outcome == "ok" && a.detail.starts_with("copied from http://127.0.0.1:")).count());
        let before = source.parts().store_status().unwrap();
        assert_eq!((3, 7), (before.replica_pages, before.replica_served), "3 pages, 7 parts served");

        let again = run(&replica);
        assert!(again.complete);
        assert_eq!((7, 7, 0, 0, 0), (again.listed, again.present, again.adopted, again.fetched, again.fetched_bytes), "{again:?}");
        let after = source.parts().store_status().unwrap();
        assert_eq!((6, 7), (after.replica_pages, after.replica_served), "a second run lists again and fetches nothing");
        assert_eq!(2, replica.parts().replication_status().unwrap().runs);
        same_parts(&source, &replica);
        // The replica's own disk accounting saw the copies.
        assert_eq!(total, replica.parts().store_status().unwrap().used_bytes);
    }

    fn source_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn a_replica_that_already_holds_some_parts_fetches_only_the_rest_and_never_deletes() {
        let root = source();
        let mut names = Vec::new();
        for tag in 1..=6u8 {
            names.push(seed(root.path(), "edge-a", tag, 4_000 + usize::from(tag) * 100));
        }
        let source = start_in(root, vec![REPL_TOKEN.into()], None);

        let mine = source_dir();
        // Two the replica has recorded, one whose file is on its disk but not recorded (a crash between the
        // rename and the row), and one the source does not have at all.
        let mut sorted: Vec<_> = names.iter().map(|(n, _)| n.clone()).collect();
        sorted.sort();
        for tag in [1u8, 2] {
            seed(mine.path(), "edge-a", tag, 4_000 + usize::from(tag) * 100);
        }
        let (unrecorded, body) = &names[2];
        std::fs::create_dir_all(mine.path().join("parts/edge-a")).unwrap();
        std::fs::write(mine.path().join("parts/edge-a").join(unrecorded), body).unwrap();
        let (only_here, _) = seed(mine.path(), "edge-b", 99, 1_234);
        let replica = start_in(mine, vec![], Some(replicate_to(&format!("http://{}", source.addr()), REPL_TOKEN, 500)));

        let report = run(&replica);
        assert_eq!(None, report.error);
        assert_eq!((6, 2, 1, 3, 1), (report.listed, report.present, report.adopted, report.fetched, report.only_here), "{report:?}");
        assert_eq!(vec![format!("edge-b/{only_here}")], report.only_here_sample);
        assert_eq!(3, source.parts().store_status().unwrap().replica_served, "only the three it lacked were fetched");
        assert_eq!(7, replica.rows().len());
        assert!(replica.dir().join("edge-b").join(&only_here).is_file(), "a part the source does not list is left alone");
        assert_eq!(7, replica.files().len());
    }

    /// A proxy in front of a source that serves the n-th blob request only up to `cut` bytes and then hangs up, and
    /// records every request it relays: path, `Range` header, body bytes sent on.
    struct Cutter {
        addr: SocketAddr,
        log: Arc<Mutex<Vec<(String, Option<String>, usize)>>>,
        stop: Arc<AtomicBool>,
    }

    impl Drop for Cutter {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
        }
    }

    fn cutter(upstream: SocketAddr, cut_blob: usize, cut: usize) -> Cutter {
        use std::io::{Read as _, Write as _};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (log2, stop2) = (Arc::clone(&log), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut blobs = 0;
            while !stop2.load(Ordering::Acquire) {
                let Ok((mut client, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                };
                client.set_nonblocking(false).unwrap();
                let mut reader = std::io::BufReader::new(client.try_clone().unwrap());
                let mut head = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        break;
                    }
                    head.push_str(&line);
                    if line == "\r\n" {
                        break;
                    }
                }
                let path = head.split(' ').nth(1).unwrap_or("").to_string();
                let range = head.lines().find_map(|l| l.to_ascii_lowercase().strip_prefix("range: ").map(str::to_string));
                let mut up = TcpStream::connect(upstream).unwrap();
                up.write_all(head.as_bytes()).unwrap();
                let mut reply = Vec::new();
                let _ = up.read_to_end(&mut reply);
                let split = reply.windows(4).position(|w| w == b"\r\n\r\n").map_or(reply.len(), |p| p + 4);
                let blob = path.contains("/blob/");
                blobs += usize::from(blob);
                let end = if blob && blobs == cut_blob { (split + cut).min(reply.len()) } else { reply.len() };
                log2.lock().unwrap().push((path, range, end - split));
                let _ = client.write_all(&reply[..end]);
            }
        });
        Cutter { addr, log, stop }
    }

    #[test]
    fn an_interrupted_run_keeps_what_it_finished_and_resumes_the_cut_part_from_the_byte_it_reached() {
        let root = source();
        let mut sizes = std::collections::BTreeMap::new();
        for (tag, len) in [(1u8, 120_000usize), (2, 150_000), (3, 90_000)] {
            let (name, _) = seed(root.path(), "edge-a", tag, len);
            sizes.insert(name, len as u64);
        }
        let total: u64 = sizes.values().sum();
        let source = start_in(root, vec![REPL_TOKEN.into()], None);
        // The second part asked for is cut off after 40,000 bytes of its body.
        let proxy = cutter(source.addr(), 2, 40_000);
        let replica = start_in(source_dir(), vec![], Some(replicate_to(&format!("http://{}", proxy.addr), REPL_TOKEN, 500)));

        let first = run(&replica);
        assert!(!first.complete);
        assert!(first.error.as_deref().is_some_and(|e| e.contains("transfer failed") || e.contains("ended at")), "{first:?}");
        assert_eq!((2, 1), (first.listed, first.fetched), "the run stopped at the cut part: {first:?}");
        assert_eq!(1, replica.rows().len(), "only the finished part is catalogued");
        let partial: Vec<_> = replica.files().into_iter().filter(|(f, _)| f.contains("/.fetch-")).collect();
        assert_eq!(1, partial.len(), "the cut part's bytes are kept: {:?}", replica.files());
        assert_eq!(40_000, std::fs::metadata(replica.dir().join(&partial[0].0)).unwrap().len());

        let log = proxy.log.lock().unwrap().clone();
        let (a, b) = (log[1].0.rsplit('/').next().unwrap().to_string(), log[2].0.rsplit('/').next().unwrap().to_string());
        let c = sizes.keys().find(|n| **n != a && **n != b).unwrap().clone();

        let second = run(&replica);
        assert_eq!(None, second.error);
        assert!(second.complete);
        assert_eq!((3, 1, 2, 1, 40_000), (second.listed, second.present, second.fetched, second.resumed, second.resumed_bytes), "{second:?}");
        assert_eq!(total - sizes[&a] - 40_000, second.fetched_bytes, "the cut part's first 40,000 bytes were not fetched again");
        same_parts(&source, &replica);
        assert!(replica.files().iter().all(|(f, _)| !f.contains("/.fetch-")), "no partial copy is left once it is installed");

        // What went over the wire: the finished part once, the cut part twice (the second time with a Range from the
        // byte reached, for the rest only), the third once.
        let log = proxy.log.lock().unwrap().clone();
        let blobs: Vec<_> = log.iter().filter(|(p, _, _)| p.contains("/blob/")).map(|(p, r, n)| (p.rsplit('/').next().unwrap().to_string(), r.clone(), *n as u64)).collect();
        assert_eq!(
            vec![(a.clone(), None, sizes[&a]), (b.clone(), None, 40_000), (b.clone(), Some("bytes=40000-".to_string()), sizes[&b] - 40_000), (c.clone(), None, sizes[&c])],
            blobs,
            "{log:?}"
        );
    }

    fn flip_a_byte(path: &Path) {
        let mut bytes = std::fs::read(path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn a_damaged_source_part_is_refused_reported_and_never_catalogued_and_is_fetched_once_repaired() {
        let root = source();
        let mut parts = Vec::new();
        for tag in 1..=3u8 {
            parts.push(seed(root.path(), "edge-a", tag, 20_000 + usize::from(tag)));
        }
        let (victim, good) = (parts[1].0.clone(), parts[1].1.clone());
        let victim_path = root.path().join("parts/edge-a").join(&victim);
        flip_a_byte(&victim_path);
        let source = start_in(root, vec![REPL_TOKEN.into()], None);
        let replica = start_in(source_dir(), vec![], Some(replicate_to(&format!("http://{}", source.addr()), REPL_TOKEN, 500)));

        let report = run(&replica);
        assert_eq!(None, report.error);
        assert!(report.complete, "a damaged part skips that part, not the run");
        assert_eq!((3, 2, 1, 0), (report.listed, report.fetched, report.damaged, report.unservable), "{report:?}");
        assert!(report.problems.len() == 1 && report.problems[0].contains(&victim) && report.problems[0].contains("not the name"), "{:?}", report.problems);
        assert!(!replica.rows().iter().any(|r| r.name == victim), "not catalogued");
        assert!(!replica.dir().join("edge-a").join(&victim).exists(), "not installed");
        assert!(replica.files().iter().all(|(f, _)| !f.contains("/.fetch-")), "and no partial copy of it is kept: {:?}", replica.files());
        assert_eq!(2, replica.rows().len());

        // Repaired at the source, the next run copies it.
        std::fs::write(&victim_path, good).unwrap();
        let again = run(&replica);
        assert_eq!((3, 2, 1, 0), (again.listed, again.present, again.fetched, again.damaged), "{again:?}");
        same_parts(&source, &replica);
    }

    #[test]
    fn a_source_that_lists_a_part_it_cannot_serve_is_reported_and_the_rest_are_copied() {
        let root = source();
        let mut names = Vec::new();
        for tag in 1..=4u8 {
            names.push(seed(root.path(), "edge-a", tag, 8_000 + usize::from(tag)).0);
        }
        // One file gone, one cut short of its catalogued size.
        std::fs::remove_file(root.path().join("parts/edge-a").join(&names[0])).unwrap();
        let short = root.path().join("parts/edge-a").join(&names[1]);
        let bytes = std::fs::read(&short).unwrap();
        std::fs::write(&short, &bytes[..bytes.len() - 10]).unwrap();
        let source = start_in(root, vec![REPL_TOKEN.into()], None);
        let replica = start_in(source_dir(), vec![], Some(replicate_to(&format!("http://{}", source.addr()), REPL_TOKEN, 500)));

        let report = run(&replica);
        assert_eq!(None, report.error);
        assert!(report.complete);
        assert_eq!((4, 2, 2, 0), (report.listed, report.fetched, report.unservable, report.damaged), "{report:?}");
        assert_eq!(2, report.problems.len());
        for n in &names[..2] {
            assert!(report.problems.iter().any(|p| p.contains(n.as_str()) && p.contains("listed but not served")), "{:?}", report.problems);
            assert!(!replica.rows().iter().any(|r| &r.name == n), "{n} is not catalogued");
        }
        assert_eq!(2, replica.rows().len());
        assert_eq!(2, replica.files().len());
    }

    #[test]
    fn an_entry_that_could_not_be_a_file_in_a_node_directory_is_never_believed() {
        let root = source();
        let (name, _) = seed(root.path(), "edge-a", 1, 100);
        let central = start_in(root, vec![REPL_TOKEN.into()], None);
        let ok = Listed {
            row: PeerPartRow { node: "edge-a".into(), name: name.clone(), size: 100, rows: 1, min_unix_nano: 1, max_unix_nano: 2, origin: "o".into(), committed_ms: 1, tier: "warm".into(), tier_ms: 1 },
            sha256: name[..64].to_string(),
        };
        let parts = central.parts();
        assert_eq!(None, parts.invalid_entry(&ok));
        let with = |f: &dyn Fn(&mut Listed)| {
            let mut l = ok.clone();
            f(&mut l);
            parts.invalid_entry(&l).is_some()
        };
        assert!(with(&|l| l.row.node = "../elsewhere".into()));
        assert!(with(&|l| l.row.node = ".hidden".into()));
        assert!(with(&|l| l.row.name = "../../etc/passwd".into()));
        assert!(with(&|l| l.sha256 = "0".repeat(64)));
        assert!(with(&|l| l.row.size = 0));
        assert!(with(&|l| l.row.size = parts.config.max_part_bytes + 1));
        assert!(with(&|l| l.row.tier = "hot".into()));
        assert!(with(&|l| l.row.min_unix_nano = 9));
    }

    #[test]
    fn the_replica_paths_open_only_with_a_replication_token_and_nothing_else_opens_with_it() {
        let root = source();
        let (name, body) = seed(root.path(), "edge-a", 1, 5_000);
        seed(root.path(), "edge-b", 2, 6_000);
        let source = start_in(root, vec![REPL_TOKEN.into()], None);
        let a = source.addr();
        let (catalog, blob) = (REPLICA_CATALOG_PATH.to_string(), format!("{REPLICA_BLOB_PREFIX}edge-a/{name}"));

        // No token, a wrong token, a node's token: refused on both paths, before anything is read.
        assert_eq!(401, get(a, &catalog, &[]).0);
        assert_eq!(403, authed(a, &catalog, "not-the-token", &[]).0);
        assert_eq!(403, authed(a, &catalog, "tok-a-0123456789", &[]).0);
        assert_eq!(403, authed(a, &blob, "tok-a-0123456789", &[]).0);
        assert_eq!(4, source.parts().store_status().unwrap().refused_replica);
        // The replication token opens nothing else: not the capability document, not a presign.
        assert_eq!(403, authed(a, "/.well-known/logless", REPL_TOKEN, &[]).0);
        let agent: ureq::Agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
        let presign = agent.post(&format!("http://{a}{PRESIGN_PATH}")).header("Authorization", &format!("Bearer {REPL_TOKEN}")).header("Content-Type", "application/json").send(&b"{}"[..]).unwrap();
        assert_eq!(403, presign.status().as_u16());

        // The replicating side says why, and copies nothing.
        for token in ["not-the-token", "tok-a-0123456789"] {
            let replica = start_in(source_dir(), vec![], Some(replicate_to(&format!("http://{a}"), token, 500)));
            let report = run(&replica);
            assert!(!report.complete && report.fetched == 0 && report.listed == 0, "{report:?}");
            assert!(report.error.as_deref().is_some_and(|e| e.contains("refused the replication token (HTTP 403)")), "{report:?}");
            assert!(replica.rows().is_empty() && replica.files().is_empty());
        }
        assert_eq!(6, source.parts().store_status().unwrap().refused_replica);

        // The right token: the catalog in key order, with the digest repeated, and the bytes.
        let (status, _, listing) = authed(a, &format!("{catalog}?limit=1"), REPL_TOKEN, &[]);
        assert_eq!(200, status);
        let v: serde_json::Value = serde_json::from_slice(&listing).unwrap();
        assert_eq!((1, format!("edge-a/{name}"), name[..64].to_string(), "warm"), (v["parts"].as_array().unwrap().len(), v["next"].as_str().unwrap().to_string(), v["parts"][0]["sha256"].as_str().unwrap().to_string(), v["parts"][0]["tier"].as_str().unwrap()));
        let (_, _, rest) = authed(a, &format!("{catalog}?after=edge-a/{name}&limit=5"), REPL_TOKEN, &[]);
        let v: serde_json::Value = serde_json::from_slice(&rest).unwrap();
        assert_eq!((1, true), (v["parts"].as_array().unwrap().len(), v["next"].is_null()));
        assert_eq!(400, authed(a, &format!("{catalog}?limit=0"), REPL_TOKEN, &[]).0);
        assert_eq!(400, authed(a, &format!("{catalog}?after=../x"), REPL_TOKEN, &[]).0);

        let (status, headers, whole) = authed(a, &blob, REPL_TOKEN, &[]);
        assert_eq!((200, body.clone()), (status, whole));
        assert!(headers.contains(&("accept-ranges".into(), "bytes".into())) && headers.contains(&("etag".into(), format!("\"{}\"", &name[..64]))));
        let (status, headers, tail) = authed(a, &blob, REPL_TOKEN, &[("Range", "bytes=4990-")]);
        assert_eq!((206, body[4990..].to_vec()), (status, tail));
        assert!(headers.contains(&("content-range".into(), "bytes 4990-4999/5000".into())), "{headers:?}");
        assert_eq!((206, body[10..15].to_vec()), { let (s, _, b) = authed(a, &blob, REPL_TOKEN, &[("Range", "bytes=10-14")]); (s, b) });
        let (status, headers, _) = authed(a, &blob, REPL_TOKEN, &[("Range", "bytes=5000-")]);
        assert_eq!(416, status);
        assert!(headers.contains(&("content-range".into(), "bytes */5000".into())), "{headers:?}");
        assert_eq!((200, 5_000), { let (s, _, b) = authed(a, &blob, REPL_TOKEN, &[("Range", "bytes=zero-")]); (s, b.len()) }, "a range it cannot read is ignored");
        // Only what was committed, and only as <node>/<name>.
        std::fs::write(source.dir().join("edge-a").join(format!("{}.parquet", "9".repeat(64))), b"never committed").unwrap();
        assert_eq!(404, authed(a, &format!("{REPLICA_BLOB_PREFIX}edge-a/{}.parquet", "9".repeat(64)), REPL_TOKEN, &[]).0);
        assert_eq!(404, authed(a, &format!("{REPLICA_BLOB_PREFIX}edge-a/../edge-b/{name}"), REPL_TOKEN, &[]).0);
        assert_eq!(404, authed(a, &format!("{REPLICA_BLOB_PREFIX}{name}"), REPL_TOKEN, &[]).0);
        assert_eq!(404, authed(a, &format!("{REPLICA_BLOB_PREFIX}edge-b/{name}"), REPL_TOKEN, &[]).0, "a name under the wrong node");
    }

    #[test]
    fn a_central_with_no_replication_tokens_offers_nothing_and_a_replica_says_so() {
        let root = source();
        seed(root.path(), "edge-a", 1, 100);
        let source = start_in(root, vec![], None);
        // Not even a node's token opens it.
        assert_eq!(404, authed(source.addr(), REPLICA_CATALOG_PATH, "tok-a-0123456789", &[]).0);
        let replica = start_in(source_dir(), vec![], Some(replicate_to(&format!("http://{}", source.addr()), "anything", 500)));
        let report = run(&replica);
        assert!(report.error.as_deref().is_some_and(|e| e.contains("does not offer replication")), "{report:?}");
        assert!(replica.rows().is_empty());
    }

    #[test]
    fn a_source_that_keeps_its_parts_in_a_bucket_is_reported_and_nothing_is_copied() {
        let config = PeerConfig {
            enabled: true,
            addr: "127.0.0.1:0".into(),
            nodes: vec![PeerNode { id: "edge-a".into(), token: "tok-a-0123456789".into() }],
            parts: PartsConfig {
                enabled: true,
                endpoint: "http://127.0.0.1:1".into(),
                bucket: "parts".into(),
                access_key_id: "k".into(),
                secret_access_key: "s".into(),
                replication_tokens: vec![REPL_TOKEN.into()],
                ..PartsConfig::default()
            },
            ..PeerConfig::default()
        };
        let root = source();
        let source = peer::Receiver::start_with_catalog(&config, "central".into(), Some(root.path().join("catalog.sqlite")), |_| Admitted::Accepted).unwrap();
        let (status, _, body) = authed(source.local_addr(), REPLICA_CATALOG_PATH, REPL_TOKEN, &[]);
        assert_eq!(409, status);
        assert!(String::from_utf8_lossy(&body).contains("not_local"));
        let replica = start_in(source_dir(), vec![], Some(replicate_to(&format!("http://{}", source.local_addr()), REPL_TOKEN, 500)));
        let report = run(&replica);
        assert!(report.error.as_deref().is_some_and(|e| e.contains("keeps its parts in a bucket")), "{report:?}");
        assert_eq!((0, 0), (report.listed, replica.rows().len()));
    }

    #[test]
    fn replication_is_configured_only_where_it_can_work() {
        let ok = PartsConfig { enabled: true, replicate: Some(replicate_to("http://central.example.com:9300", "tok", 500)), replication_tokens: vec!["a".into(), "b".into()], ..PartsConfig::default() };
        assert_eq!(Ok(()), ok.check());
        let with = |f: &dyn Fn(&mut PartsConfig)| {
            let mut c = ok.clone();
            f(&mut c);
            c.check().unwrap_err()
        };
        assert!(with(&|c| c.replicate.as_mut().unwrap().url = "central.example.com".into()).contains("url must be http(s)"));
        assert!(with(&|c| c.replicate.as_mut().unwrap().token.clear()).contains("replication token"));
        assert!(with(&|c| c.replicate.as_mut().unwrap().page_size = 0).contains("page_size"));
        assert!(with(&|c| c.replicate.as_mut().unwrap().page_size = 1_001).contains("page_size"));
        assert!(with(&|c| c.replication_tokens = vec!["a".into(), "a".into()]).contains("distinct"));
        assert!(with(&|c| c.replication_tokens = vec![String::new()]).contains("non-empty"));
        // A bucket-backed replica would need the storage's own tools.
        assert!(with(&|c| {
            c.endpoint = "http://127.0.0.1:9000".into();
            c.bucket = "b".into();
            c.access_key_id = "k".into();
            c.secret_access_key = "s".into();
        })
        .contains("own disk"));
        // The secrets never show.
        let shown = format!("{ok:?}");
        assert!(!shown.contains("tok\"") && shown.contains("<redacted>"), "{shown}");
        assert!(!toml::to_string(&ok).unwrap().contains("tok"));
        // And a replication token is its own secret.
        let root = source();
        let config = PeerConfig {
            enabled: true,
            addr: "127.0.0.1:0".into(),
            nodes: vec![PeerNode { id: "edge-a".into(), token: "tok-a-0123456789".into() }],
            parts: PartsConfig { enabled: true, replication_tokens: vec!["tok-a-0123456789".into()], ..PartsConfig::default() },
            ..PeerConfig::default()
        };
        let refused = peer::Receiver::start_with_catalog(&config, "central".into(), Some(root.path().join("catalog.sqlite")), |_| Admitted::Accepted).err().unwrap();
        assert!(refused.to_string().contains("replication_tokens must differ"), "{refused}");
        // A replica with a configuration that cannot work stops the listener rather than running without it.
        let bad = PeerConfig { parts: PartsConfig { replicate: Some(replicate_to("nonsense", "t", 5)), ..config.parts.clone() }, nodes: vec![], ..config };
        let refused = peer::Receiver::start_with_catalog(&bad, "central".into(), Some(root.path().join("catalog.sqlite")), |_| Admitted::Accepted).err().unwrap();
        assert!(refused.to_string().contains("url must be http(s)"), "{refused}");
    }

    #[test]
    fn the_timer_runs_by_itself_one_run_at_a_time_and_shutdown_stops_it() {
        let root = source();
        for tag in 1..=3u8 {
            seed(root.path(), "edge-a", tag, 3_000);
        }
        let source = start_in(root, vec![REPL_TOKEN.into()], None);
        let mut config = replicate_to(&format!("http://{}", source.addr()), REPL_TOKEN, 2);
        config.interval_secs = 1;
        let replica = start_in(source_dir(), vec![], Some(config));
        let parts = replica.parts();
        let deadline = Instant::now() + Duration::from_secs(20);
        while parts.replication_status().unwrap().runs < 2 || parts.replication_status().unwrap().state != "idle" {
            assert!(Instant::now() < deadline, "{:?}", parts.replication_status());
            std::thread::sleep(Duration::from_millis(50));
        }
        let status = parts.replication_status().unwrap();
        let last = status.run.unwrap();
        // The first run copied three parts; a later one found them all.
        assert_eq!((3, 3, 0), (last.listed, last.present, last.fetched), "{last:?}");
        assert_eq!(3, replica.rows().len());
        // A manual run is refused while one is under way, and not a replica says so.
        let claimed = parts.claim().unwrap();
        assert_eq!(Err(ReplicateRefused::Running), parts.replicate_once());
        claimed.running.store(false, Ordering::Release);
        let t = Instant::now();
        let Central { receiver, .. } = replica;
        receiver.shutdown();
        assert!(t.elapsed() < Duration::from_secs(5), "shutdown joined the timer in {:?}", t.elapsed());
        let plain = start_in(source_dir(), vec![], None);
        assert_eq!(Err(ReplicateRefused::NotAReplica), plain.parts().replicate_once());
    }
}
