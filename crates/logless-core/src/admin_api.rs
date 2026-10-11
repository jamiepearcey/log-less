//! Runtime policy API: `/admin/v1/*`, JSON over a bearer-token-protected
//! listener inside the ingest process. Contract and the file-versus-runtime
//! model are in `docs/admin-api.md`.
//!
//! Browsers never talk to this listener (the console backend proxies it
//! server side), so there are no CORS headers at all: a browser page cannot
//! read a response from it, and the Host check closes DNS rebinding.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::budget::{DiskUsage, Pressure};
use crate::catalog::Catalog;
use crate::config::Config;
use crate::bounded_http::{BoundedServer, HeadLimits};
use crate::httpd::{Handler, HttpError, Incoming, Limits, Method, Reject, Reply};
use crate::policy::{self, PolicyHandle, Policies, Problem};
use crate::policy_impact::{self, Locations, Preview};
use crate::policy_store::{self, AuditEntry, PolicyFile, StoreError, POLICY_SCHEMA_VERSION};
use crate::queue::IngestStats;
use crate::now_unix_secs;

/// Shortest token accepted on a non-loopback bind. 24 characters of a
/// reasonable alphabet is out of reach of online guessing.
pub const MIN_REMOTE_TOKEN_LEN: usize = 24;
const TOKEN_ENV: &str = "LOGLESS_ADMIN_TOKEN";
const MAX_BODY_BYTES: usize = 1024 * 1024;
const DEFAULT_HISTORY_LIMIT: usize = 50;
const MAX_HISTORY_LIMIT: usize = 1000;
const DEFAULT_PARTS_LIMIT: usize = 100;
const MAX_PARTS_LIMIT: usize = 1000;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AdminConfig {
    /// Opt-in, like every listener here.
    pub enabled: bool,
    pub addr: String,
    /// Bearer token. `LOGLESS_ADMIN_TOKEN` wins when both are set, so a secret
    /// can stay out of the file. Mandatory for every request including GET.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Host header values (port ignored) accepted besides loopback names.
    pub allowed_hosts: Vec<String>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: "127.0.0.1:9101".to_string(),
            token: None,
            allowed_hosts: Vec::new(),
        }
    }
}

// Hand-written so a logged config never carries the token.
impl std::fmt::Debug for AdminConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminConfig")
            .field("enabled", &self.enabled)
            .field("addr", &self.addr)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("allowed_hosts", &self.allowed_hosts)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("admin api has no token; set {TOKEN_ENV} or [admin].token (every request needs one, including GET)")]
    MissingToken,
    #[error(
        "admin api token is {len} characters; binding the non-loopback address {addr} needs at least {MIN_REMOTE_TOKEN_LEN}"
    )]
    WeakToken { addr: String, len: usize },
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{0}")]
    Policy(String),
}

/// Counters the info endpoint reports, owned by the ingest path.
#[derive(Clone)]
pub struct IngestProbe {
    pub stats: Arc<IngestStats>,
    /// Records synced to the WAL.
    pub written: Arc<AtomicU64>,
    /// DEBUG refused at the door under critical disk pressure.
    pub refused_debug_pressure: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListenerInfo {
    pub kind: String,
    pub addr: String,
}

/// Policy as the process starts: file defaults, the persisted overrides laid
/// on them, and the metadata of the last runtime write.
pub struct Startup {
    pub file_defaults: Policies,
    pub effective: Policies,
    pub meta: Option<PolicyFile>,
}

pub fn startup(config: &Config) -> Result<Startup, AdminError> {
    let file_defaults = Policies::from_config(config);
    let (effective, meta) = policy_store::load_effective(&config.storage.data_dir, &file_defaults)?;
    Ok(Startup { file_defaults, effective, meta })
}

struct State {
    version: u64,
    updated_at_ms: u64,
    updated_by: String,
    overrides: Vec<String>,
    effective: Policies,
}

pub struct AdminService {
    data_dir: PathBuf,
    store_dir: PathBuf,
    wal_dir: PathBuf,
    catalog_path: PathBuf,
    file_defaults: Policies,
    handle: Arc<PolicyHandle>,
    ingest: IngestProbe,
    listeners: Mutex<Vec<ListenerInfo>>,
    started: Instant,
    started_at_ms: u64,
    /// Wall clock for "what expires now"; injected so tests can place
    /// partitions in the past without writing real 1970 timestamps.
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// Held across a whole write (check, persist, audit, publish) so two
    /// writers cannot interleave and `If-Match` is checked against the state
    /// the write is applied on.
    state: Mutex<State>,
    /// Held for the whole of any store scan (preview, PUT, restore). A scan is
    /// O(partitions); letting several run at once starves every other endpoint. The
    /// state mutex is never held across a scan, so reads stay instant.
    /// Previews that find it taken get 429; writers queue behind it.
    scan_gate: Mutex<()>,
    /// The decision model, when `[decision]` has a url (or a test supplied one).
    decision: Option<(crate::decision::DecisionConfig, Arc<dyn crate::decision::DecisionProvider>)>,
    /// Peer sync, set once the sender exists (`set_sync`). Absent in a process that has none.
    sync: OnceLock<Arc<crate::sync::SyncControl>>,
    /// `[peer.parts]`, for the part catalog endpoints and the retention sweep.
    parts: crate::parts::PartsConfig,
    /// The peer receiver's counters, when this process accepts records from other instances.
    peer: OnceLock<Arc<crate::peer::PeerStats>>,
    /// The part store the peer receiver serves, when it serves one: the sweep and the status use it, so
    /// they see the same disk budget the uploads do.
    parts_store: OnceLock<Arc<crate::parts::PartsCentral>>,
}

type Resp = (u16, Value);

fn err(status: u16, code: &str, message: impl Into<String>) -> Resp {
    (status, json!({"error": code, "message": message.into()}))
}

fn problems_resp(problems: Vec<Problem>) -> Resp {
    (422, json!({"error": "validation_failed", "problems": problems}))
}

fn now_ms() -> u64 {
    crate::now_unix_nanos() / 1_000_000
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SyncBody {
    enabled: bool,
    /// Absent keeps the saved peer's URL (the API never returns it, so a client cannot send it back); an
    /// empty string clears it.
    #[serde(default)]
    url: Option<String>,
    /// Write-only. Absent keeps the stored one.
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    clear_token: bool,
    #[serde(default)]
    min_severity: Option<u8>,
    /// 1 to 100. Absent keeps the current dial (100 when none was set).
    #[serde(default)]
    fidelity: Option<u8>,
    /// Absent keeps the current setting (on when none was set).
    #[serde(default)]
    anomaly_boost: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackfillBody {
    from_ms: u64,
    to_ms: u64,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SyncTestBody {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    token: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviewBody {
    policies: Policies,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteBody {
    policies: Policies,
    #[serde(default)]
    confirm_destructive: bool,
    /// `impact_token` of the preview the operator reviewed.
    #[serde(default)]
    impact_token: Option<String>,
    actor: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreBody {
    version: u64,
    #[serde(default)]
    confirm_destructive: bool,
    #[serde(default)]
    impact_token: Option<String>,
    actor: String,
}

fn parse<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, Resp> {
    serde_json::from_slice(body).map_err(|e| {
        if e.is_data() {
            // Right JSON, wrong shape: say where, as a problem like any other.
            problems_resp(vec![Problem::new("body", e.to_string())])
        } else {
            err(400, "bad_request", format!("body is not valid JSON: {e}"))
        }
    })
}

/// Characters that pass `is_control` but can reorder or hide text in the
/// audit log and the UI: bidi controls, line and paragraph separators,
/// zero-width and byte-order marks.
fn is_spoofing(c: char) -> bool {
    matches!(c,
        '\u{061C}' | '\u{200B}'..='\u{200F}' | '\u{2028}'..='\u{202E}' | '\u{2060}'..='\u{2064}'
        | '\u{2066}'..='\u{206F}' | '\u{FEFF}')
}

fn check_actor(actor: &str) -> Option<Problem> {
    let ok = !actor.trim().is_empty()
        && actor.len() <= 128
        && !actor.chars().any(|c| c.is_control() || is_spoofing(c));
    (!ok).then(|| Problem::new("actor", "must be 1 to 128 characters with no control, line-separator or bidirectional-formatting characters; it is written to the audit log"))
}

fn busy() -> Resp {
    err(429, "busy", "another preview or policy write is scanning the store; retry in a moment")
}

fn parse_if_match(raw: Option<&str>) -> Result<u64, Resp> {
    let Some(raw) = raw else {
        return Err(err(400, "if_match_required", "send the policy version you read as If-Match"));
    };
    raw.trim()
        .trim_matches('"')
        .parse::<u64>()
        .map_err(|_| err(400, "bad_if_match", "If-Match must be the integer policy version"))
}

impl AdminService {
    pub fn new(
        config: &Config,
        start: Startup,
        handle: Arc<PolicyHandle>,
        ingest: IngestProbe,
        listeners: Vec<ListenerInfo>,
    ) -> Arc<Self> {
        Self::with_clock(config, start, handle, ingest, listeners, Arc::new(now_unix_secs))
    }

    pub fn with_clock(
        config: &Config,
        start: Startup,
        handle: Arc<PolicyHandle>,
        ingest: IngestProbe,
        listeners: Vec<ListenerInfo>,
        clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Arc<Self> {
        let (version, updated_at_ms, updated_by, overrides) = match &start.meta {
            Some(f) => (f.version, f.updated_at_ms, f.updated_by.clone(), f.overrides.clone()),
            None => (0, 0, "file".to_string(), Vec::new()),
        };
        Arc::new(Self {
            data_dir: config.storage.data_dir.clone(),
            store_dir: config.store_dir(),
            wal_dir: config.wal_dir(),
            catalog_path: config.catalog_path(),
            file_defaults: start.file_defaults,
            handle,
            ingest,
            listeners: Mutex::new(listeners),
            started: Instant::now(),
            started_at_ms: now_ms(),
            clock,
            decision: config.decision.enabled().then(|| crate::decision::HttpProvider::new(&config.decision)).flatten().map(|p| (config.decision.clone(), Arc::new(p) as Arc<dyn crate::decision::DecisionProvider>)),
            state: Mutex::new(State {
                version,
                updated_at_ms,
                updated_by,
                overrides,
                effective: start.effective,
            }),
            scan_gate: Mutex::new(()),
            sync: OnceLock::new(),
            parts: config.peer.parts.clone(),
            peer: OnceLock::new(),
            parts_store: OnceLock::new(),
        })
    }

    /// `GET /admin/v1/templates/merge-suggestions`: pairs of templates the decision model is confident are one failure. A
    /// suggestion only: nothing is merged. Asks the model about at most `max_pairs` pairs, so it takes a few seconds.
    pub fn merge_suggestions(&self) -> Resp {
        let Some((cfg, provider)) = &self.decision else {
            return err(404, "decision_off", "no decision model is configured; set [decision] url in the config");
        };
        let catalog = match self.catalog() {
            Ok(c) => c,
            Err(r) => return r,
        };
        let templates = match catalog.top_templates(300) {
            Ok(t) => t,
            Err(e) => return err(500, "catalog_unavailable", e.to_string()),
        };
        let report = crate::decision::suggest_merges(provider.as_ref(), &templates, cfg, std::time::Duration::from_secs(25));
        (200, json!({ "source": "decision-model", "min_confidence": cfg.min_confidence, "templates_considered": templates.len(), "report": report }))
    }

    /// Hands the service the peer sync controls, which `/admin/v1/sync` reads and edits.
    pub fn set_sync(&self, control: Arc<crate::sync::SyncControl>) {
        let _ = self.sync.set(control);
    }

    /// Hands the service the peer receiver's counters, which `/admin/v1/sync` reports as `receiver`.
    pub fn set_peer_stats(&self, stats: Arc<crate::peer::PeerStats>) {
        let _ = self.peer.set(stats);
    }

    /// Hands the service the peer receiver's part store (`Receiver::parts_handle`).
    pub fn set_parts(&self, store: Arc<crate::parts::PartsCentral>) {
        let _ = self.parts_store.set(store);
    }

    fn sync_control(&self) -> Result<&Arc<crate::sync::SyncControl>, Resp> {
        self.sync.get().ok_or_else(|| err(404, "sync_unavailable", "peer sync is not running in this process"))
    }

    /// `GET /admin/v1/sync`: where records go, and how it is going. The token is never returned.
    pub fn sync_get(&self) -> Resp {
        let control = match self.sync_control() {
            Ok(c) => c,
            Err(r) => return r,
        };
        let source = if control.store().load().is_some() {
            "admin"
        } else if control.target().is_some() {
            "config"
        } else {
            "none"
        };
        // What this instance receives: the WAL waits and 503s show a receiver whose disk is behind.
        let receiver = self.peer.get().map(|p| p.snapshot());
        // Where uploaded parts are kept when that is this instance's own disk: bytes used, the budget, refusals.
        let parts_store = self.parts_store.get().and_then(|p| p.store_status());
        // When this central is a replica: the source, and what the last (or current) run found and did.
        let parts_replication = self.parts_store.get().and_then(|p| p.replication_status());
        (200, json!({ "source": source, "instance_id": control.instance(), "status": control.status(), "receiver": receiver, "parts_store": parts_store, "parts_replication": parts_replication }))
    }

    /// `PUT /admin/v1/sync`: set the peer. Applies to the running sender at once and is kept in the data
    /// dir, where it outranks the config file's destination.
    pub fn sync_put(&self, body: &[u8]) -> Resp {
        let control = match self.sync_control() {
            Ok(c) => c,
            Err(r) => return r,
        };
        let req: SyncBody = match serde_json::from_slice(body) {
            Ok(b) => b,
            Err(e) => return err(400, "bad_request", format!("the body is not a sync request: {e}")),
        };
        let invalid = |m: String| (422, json!({"error": "validation_failed", "message": m}));
        let saved_url = control
            .store()
            .load()
            .map(|(settings, _)| settings.url)
            .filter(|u| !u.is_empty())
            .or_else(|| control.target().map(|t| t.url))
            .unwrap_or_default();
        let url = req.url.as_deref().map_or(saved_url.as_str(), str::trim);
        let clean_url = if url.is_empty() {
            if req.enabled {
                return invalid("Enter the peer's URL to turn sync on.".into());
            }
            String::new()
        } else {
            match crate::sync::check_url(url) {
                Ok(u) => u,
                Err(m) => return invalid(m),
            }
        };
        let min_severity = req.min_severity.unwrap_or(0);
        if min_severity > 24 {
            return invalid("min_severity is an OpenTelemetry severity number, 0 to 24.".into());
        }
        let saved = control.store().load().map(|(settings, _)| settings);
        let current_target = control.target();
        let fidelity = req
            .fidelity
            .or_else(|| saved.as_ref().map(|s| s.fidelity))
            .or_else(|| current_target.as_ref().map(|t| t.fidelity))
            .unwrap_or(100);
        if !(1..=100).contains(&fidelity) {
            return invalid("fidelity is a percentage, 1 to 100.".into());
        }
        let anomaly_boost = req
            .anomaly_boost
            .or_else(|| saved.as_ref().map(|s| s.anomaly_boost))
            .or_else(|| current_target.as_ref().map(|t| t.anomaly_boost))
            .unwrap_or(true);
        let stored = control.store().load().and_then(|(_, t)| t);
        let current = control.target().and_then(|t| t.token);
        let (change, token) = if req.clear_token {
            (crate::sync::TokenChange::Clear, None)
        } else if let Some(t) = req.token.as_deref().filter(|t| !t.is_empty()) {
            if t.chars().count() > 4096 || t.chars().any(char::is_control) {
                return invalid("A token is at most 4096 characters, without control characters.".into());
            }
            (crate::sync::TokenChange::Set(t.to_string()), Some(t.to_string()))
        } else {
            // Kept: the token saved by the admin API, else the one the config file supplied (which is then
            // adopted into the admin copy so the two cannot disagree later).
            let keep = stored.or(current);
            let change = keep.clone().map_or(crate::sync::TokenChange::Keep, crate::sync::TokenChange::Set);
            (change, keep)
        };
        let settings = crate::sync::SyncSettings {
            enabled: req.enabled && !clean_url.is_empty(),
            url: clean_url.clone(),
            min_severity,
            fidelity,
            anomaly_boost,
            updated_at_ms: now_ms(),
        };
        if let Err(e) = control.store().save(&settings, change) {
            return err(500, "persist_failed", format!("could not save the sync settings: {e}"));
        }
        control.set_target(
            settings
                .enabled
                .then(|| crate::sync::SyncTarget { url: clean_url, token, min_severity, fidelity, anomaly_boost }),
        );
        self.sync_get()
    }

    /// `POST /admin/v1/sync/backfill`: send every record in a time range at full detail, now, in the
    /// background. Returns the sync document, whose `backfill` field shows progress.
    pub fn sync_backfill(&self, body: &[u8]) -> Resp {
        let control = match self.sync_control() {
            Ok(c) => c,
            Err(r) => return r,
        };
        let req: BackfillBody = match serde_json::from_slice(body) {
            Ok(b) => b,
            Err(e) => return err(400, "bad_request", format!("the body is not a range ({{\"from_ms\", \"to_ms\"}}): {e}")),
        };
        match control.start_backfill(req.from_ms, req.to_ms) {
            Ok(()) => self.sync_get(),
            Err(m) => (409, json!({"error": "backfill_refused", "message": m})),
        }
    }

    /// `POST /admin/v1/sync/backfill/cancel`.
    pub fn sync_backfill_cancel(&self) -> Resp {
        match self.sync_control() {
            Ok(c) => {
                c.cancel_backfill();
                self.sync_get()
            }
            Err(r) => r,
        }
    }

    /// `POST /admin/v1/sync/parts`: upload the committed Parquet files overlapping a time range to the
    /// peer's object storage, in the background. Returns the sync document, whose `parts` field shows
    /// progress.
    pub fn sync_parts(&self, body: &[u8]) -> Resp {
        let control = match self.sync_control() {
            Ok(c) => c,
            Err(r) => return r,
        };
        let req: BackfillBody = match serde_json::from_slice(body) {
            Ok(b) => b,
            Err(e) => return err(400, "bad_request", format!("the body is not a range ({{\"from_ms\", \"to_ms\"}}): {e}")),
        };
        match control.start_parts(req.from_ms, req.to_ms) {
            Ok(()) => self.sync_get(),
            Err(m) => (409, json!({"error": "parts_refused", "message": m})),
        }
    }

    /// `POST /admin/v1/sync/parts/cancel`.
    pub fn sync_parts_cancel(&self) -> Resp {
        match self.sync_control() {
            Ok(c) => {
                c.cancel_parts();
                self.sync_get()
            }
            Err(r) => r,
        }
    }

    /// `GET /admin/v1/parts?node=&tier=&from_ms=&to_ms=&limit=`: the parts edges uploaded, oldest data first.
    /// `tier` is `warm` or `cold`; a `hot` file is only on its node and is not in this catalog.
    pub fn parts_list(&self, query: &str) -> Resp {
        let mut filter = crate::catalog::PartsFilter { limit: DEFAULT_PARTS_LIMIT, ..Default::default() };
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let bad = |what: &str| err(400, "bad_query", format!("{key}: {what}"));
            match key {
                "node" if crate::parts::valid_node(value) => filter.node = Some(value.to_string()),
                "node" => return bad("not a node id"),
                "tier" if matches!(value, "warm" | "cold") => filter.tier = Some(value.to_string()),
                "tier" if value == "hot" => return bad("hot parts are local files on their node and are not in the central catalog"),
                "tier" => return bad("must be warm or cold"),
                "from_ms" | "to_ms" => match value.parse::<u64>().ok().and_then(|ms| ms.checked_mul(1_000_000)) {
                    Some(nano) if key == "from_ms" => filter.from_nano = Some(nano),
                    Some(nano) => filter.to_nano = Some(nano),
                    None => return bad("must be milliseconds since the epoch"),
                },
                "limit" => match value.parse::<usize>() {
                    Ok(n) if (1..=MAX_PARTS_LIMIT).contains(&n) => filter.limit = n,
                    _ => return bad(&format!("must be 1 to {MAX_PARTS_LIMIT}")),
                },
                _ => return bad("unknown parameter"),
            }
        }
        match self.catalog().and_then(|c| c.list_peer_parts(&filter).map_err(|e| err(500, "catalog_unavailable", e.to_string()))) {
            Ok(parts) => (200, json!({ "parts": parts, "count": parts.len() })),
            Err(r) => r,
        }
    }

    /// `GET /admin/v1/parts/audit?limit=`: tier moves and expiries, newest first.
    pub fn parts_audit(&self, query: &str) -> Resp {
        let limit = match query_limit(query) {
            Ok(n) => n,
            Err(r) => return r,
        };
        match self.catalog().and_then(|c| c.peer_parts_audit(limit).map_err(|e| err(500, "catalog_unavailable", e.to_string()))) {
            Ok(rows) => (200, json!({ "audit": rows })),
            Err(r) => r,
        }
    }

    /// `POST /admin/v1/parts/sweep {"dry_run": bool}`: expire catalogued parts older than the retention in
    /// `[peer.parts]`. Without `dry_run: false` it only reports what would go.
    pub fn parts_sweep(&self, body: &[u8]) -> Resp {
        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields)]
        struct Body {
            #[serde(default = "yes")]
            dry_run: bool,
        }
        fn yes() -> bool {
            true
        }
        let req: Body = if body.iter().all(u8::is_ascii_whitespace) {
            Body::default()
        } else {
            match serde_json::from_slice(body) {
                Ok(b) => b,
                Err(e) => return err(400, "bad_request", format!("the body is not {{\"dry_run\": bool}}: {e}")),
            }
        };
        let central = match self.parts_store.get() {
            Some(shared) => Arc::clone(shared),
            None => match crate::parts::PartsCentral::new(self.parts.clone(), self.catalog_path.clone()) {
                Ok(c) => Arc::new(c),
                Err(m) => return err(409, "parts_disabled", m),
            },
        };
        match central.sweep((self.clock)().saturating_mul(1_000), req.dry_run) {
            Ok(report) => (200, json!(report)),
            Err(m) => err(500, "sweep_failed", m),
        }
    }

    /// `GET /admin/v1/parts/replicate`: the replication status. `POST` starts a run in the background and
    /// answers 202 with it; progress is the same document, and `parts_replication` in `GET /admin/v1/sync`.
    pub fn parts_replicate(&self, start: bool) -> Resp {
        let Some(central) = self.parts_store.get() else {
            return err(409, "parts_disabled", "[peer.parts] is not enabled on this instance");
        };
        if start {
            match central.replicate_in_background() {
                Ok(()) => {}
                Err(crate::parts::ReplicateRefused::NotAReplica) => return err(409, "not_a_replica", "there is no [peer.parts.replicate] in this configuration"),
                Err(crate::parts::ReplicateRefused::Running) => return err(409, "already_running", "a replication run is under way; see GET /admin/v1/parts/replicate"),
            }
        }
        match central.replication_status() {
            Some(status) => (if start { 202 } else { 200 }, json!({ "replication": status })),
            None => err(409, "not_a_replica", "there is no [peer.parts.replicate] in this configuration"),
        }
    }

    /// `POST /admin/v1/sync/test`: asks a peer (the one given, or the current one) what it is.
    pub fn sync_test(&self, body: &[u8]) -> Resp {
        let control = match self.sync_control() {
            Ok(c) => c,
            Err(r) => return r,
        };
        let req: SyncTestBody = if body.iter().all(u8::is_ascii_whitespace) {
            SyncTestBody::default()
        } else {
            match serde_json::from_slice(body) {
                Ok(b) => b,
                Err(e) => return err(400, "bad_request", format!("the body is not a sync test request: {e}")),
            }
        };
        let current = control.target();
        let url = match (req.url.as_deref().map(str::trim).filter(|u| !u.is_empty()), &current) {
            (Some(u), _) => u.to_string(),
            (None, Some(c)) => c.url.clone(),
            (None, None) => return (422, json!({"error": "validation_failed", "message": "Enter a URL to test."})),
        };
        // The saved token goes only to the URL it was saved for: testing a new address must not send it there.
        let same_as_current = current.as_ref().is_some_and(|c| crate::sync::check_url(&url).ok().as_deref() == Some(c.url.as_str()));
        let token = match req.token.as_deref().filter(|t| !t.is_empty()) {
            Some(t) => Some(t.to_string()),
            None if same_as_current => current.as_ref().and_then(|c| c.token.clone()),
            None => None,
        };
        match crate::sync::probe(&url, token.as_deref()) {
            Ok(outcome) => {
                if same_as_current {
                    control.record_probe(outcome.capabilities.clone());
                }
                (200, json!({ "ok": true, "result": outcome }))
            }
            Err(message) => (200, json!({ "ok": false, "error": message })),
        }
    }

    pub fn add_listener(&self, kind: &str, addr: String) {
        if let Ok(mut l) = self.listeners.lock() {
            l.push(ListenerInfo { kind: kind.to_string(), addr });
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn locations(&self) -> Locations<'_> {
        Locations { store_dir: &self.store_dir, wal_dir: &self.wal_dir }
    }

    fn catalog(&self) -> Result<Catalog, Resp> {
        Catalog::open(&self.catalog_path)
            .map_err(|e| err(500, "catalog_unavailable", e.to_string()))
    }

    // --- reads ----------------------------------------------------------

    /// The registered typed attribute columns (`docs/typed-columns.md`), so a
    /// console can offer typed predicates without reading Parquet footers.
    pub fn typed_columns(&self) -> Resp {
        let catalog = match self.catalog() {
            Ok(c) => c,
            Err(r) => return r,
        };
        match catalog.typed_layout() {
            Ok(layout) => (
                200,
                json!({
                    "columns": layout
                        .columns()
                        .iter()
                        .map(|c| json!({
                            "column": c.column,
                            "key": c.key,
                            "type": c.ty.as_str(),
                            "service": c.service,
                            "promoted_at": c.promoted_at,
                        }))
                        .collect::<Vec<_>>(),
                }),
            ),
            Err(e) => err(500, "catalog_unavailable", e.to_string()),
        }
    }

    pub fn info(&self) -> Resp {
        let catalog = match self.catalog() {
            Ok(c) => c,
            Err(r) => return r,
        };
        let rt = self.handle.runtime();
        let usage = DiskUsage::measure(&self.wal_dir, &catalog, rt.disk_budget_bytes);
        let (rows, bytes) = catalog.totals().unwrap_or((0, 0));
        let files = catalog.file_count().unwrap_or(0);
        let s = self.ingest.stats.snapshot();
        let pressure = match usage.pressure(&rt.watermarks) {
            Pressure::Normal => "normal",
            Pressure::High => "high",
            Pressure::Critical => "critical",
        };
        let listeners = self.listeners.lock().map(|l| l.clone()).unwrap_or_default();
        (
            200,
            json!({
                "version": env!("CARGO_PKG_VERSION"),
                "started_at_ms": self.started_at_ms,
                "uptime_secs": self.started.elapsed().as_secs(),
                "listeners": listeners,
                "disk": {
                    "used_bytes": usage.used(),
                    "budget_bytes": usage.budget_bytes,
                    "fraction": usage.fraction(),
                    "pressure": pressure,
                },
                "ingest": {
                    "received": s.received,
                    "written": self.ingest.written.load(Ordering::Relaxed),
                    "dropped_by_class": {
                        "debug": s.dropped_debug,
                        "info": s.dropped_info,
                        "critical": s.dropped_critical,
                        "closed": s.closed,
                        "refused_debug_pressure": self.ingest.refused_debug_pressure.load(Ordering::Relaxed),
                    },
                },
                "store": {"files": files, "rows": rows, "bytes": bytes},
            }),
        )
    }

    fn policies_json(&self, st: &State) -> Value {
        json!({
            "version": st.version,
            "updated_at_ms": st.updated_at_ms,
            "updated_by": st.updated_by,
            "effective": st.effective,
            "sources": policy::sources(&st.effective, &st.overrides),
            "apply": st.effective.apply_modes(),
            "pending_restart": policy::pending_restart(&st.effective, self.handle.running()),
            "running": policy::running_values(&st.effective, self.handle.running()),
        })
    }

    pub fn get_policies(&self) -> Resp {
        let st = self.lock();
        (200, self.policies_json(&st))
    }

    pub fn history(&self, limit: usize) -> Resp {
        match policy_store::read_audit(&self.data_dir) {
            Ok(entries) => {
                let rows: Vec<Value> = entries
                    .iter()
                    .rev()
                    .take(limit)
                    .map(|e| {
                        json!({
                            "version": e.version,
                            "ts_ms": e.ts_ms,
                            "actor": e.actor,
                            "diff": e.diff,
                            "impact_summary": e.impact_summary,
                        })
                    })
                    .collect();
                (200, Value::Array(rows))
            }
            Err(e) => err(500, "audit_unreadable", e.to_string()),
        }
    }

    // --- evaluation -----------------------------------------------------

    fn try_scan(&self) -> Option<std::sync::MutexGuard<'_, ()>> {
        match self.scan_gate.try_lock() {
            Ok(g) => Some(g),
            Err(std::sync::TryLockError::Poisoned(p)) => Some(p.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        }
    }

    /// Validation, orphan protection and impact for `proposed` against
    /// `current`. `Err` is a finished response (422, or 500 when the catalog
    /// cannot be read). Call with the scan gate held and no state lock.
    fn evaluate(&self, current: &Policies, proposed: &Policies) -> Result<Preview, Resp> {
        let mut problems = proposed.validate();
        if problems.is_empty() {
            let now = (self.clock)();
            problems.extend(
                policy_impact::orphan_problems(&self.locations(), current, proposed, now)
                    .map_err(|e| err(500, "preview_failed", e.to_string()))?,
            );
        }
        if !problems.is_empty() {
            return Err(problems_resp(problems));
        }
        let catalog = self.catalog()?;
        policy_impact::preview(&self.locations(), &catalog, current, proposed, (self.clock)())
            .map_err(|e| err(500, "preview_failed", e.to_string()))
    }

    /// Writers wait their turn (a lost race is then a 409, not a refusal);
    /// only previews, which are optional, are turned away.
    fn wait_scan(&self) -> std::sync::MutexGuard<'_, ()> {
        self.scan_gate.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn snapshot(&self) -> (u64, Policies) {
        let st = self.lock();
        (st.version, st.effective.clone())
    }

    pub fn preview(&self, body: &[u8]) -> Resp {
        let body: PreviewBody = match parse(body) {
            Ok(b) => b,
            Err(r) => return r,
        };
        let Some(_scan) = self.try_scan() else { return busy() };
        let (_, current) = self.snapshot();
        let diff = policy::diff(&current, &body.policies);
        match self.evaluate(&current, &body.policies) {
            Ok(p) => (
                200,
                json!({
                    "valid": true,
                    "problems": [],
                    "diff": diff,
                    "impact": p.impact,
                    "destructive": p.destructive,
                    "lossy_settings": p.lossy,
                    "impact_token": p.impact_token,
                }),
            ),
            Err((422, v)) => (
                200,
                json!({
                    "valid": false,
                    "problems": v["problems"],
                    "diff": diff,
                    "impact": Value::Null,
                    "destructive": false,
                    "lossy_settings": [],
                    "impact_token": Value::Null,
                }),
            ),
            Err(other) => other,
        }
    }

    // --- writes ---------------------------------------------------------

    pub fn put(&self, if_match: Option<&str>, body: &[u8]) -> Resp {
        let expected = match parse_if_match(if_match) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let body: WriteBody = match parse(body) {
            Ok(b) => b,
            Err(r) => return r,
        };
        let _scan = self.wait_scan();
        self.apply(expected, body.policies, body.confirm_destructive, body.impact_token.as_deref(), &body.actor)
    }

    /// Restore needs `If-Match` like PUT: it replaces the whole document, so a
    /// lost update must be detectable.
    pub fn restore(&self, if_match: Option<&str>, body: &[u8]) -> Resp {
        let expected = match parse_if_match(if_match) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let body: RestoreBody = match parse(body) {
            Ok(b) => b,
            Err(r) => return r,
        };
        let _scan = self.wait_scan();
        let entries = match policy_store::read_audit(&self.data_dir) {
            Ok(e) => e,
            Err(e) => return err(500, "audit_unreadable", e.to_string()),
        };
        let Some(entry) = entries.into_iter().rev().find(|e| e.version == body.version) else {
            return err(
                404,
                "unknown_version",
                format!("no recorded policy version {}; GET /admin/v1/policies/history lists them", body.version),
            );
        };
        self.apply(expected, entry.policies, body.confirm_destructive, body.impact_token.as_deref(), &body.actor)
    }

    /// The single write path: PUT and restore both end here, so restore cannot
    /// skip a rule PUT enforces. The caller holds the scan gate, which makes it
    /// the only possible writer; the state lock is taken only to read the
    /// version and to publish, never across the scan.
    fn apply(&self, expected: u64, proposed: Policies, confirm: bool, token: Option<&str>, actor: &str) -> Resp {
        if let Some(p) = check_actor(actor) {
            return problems_resp(vec![p]);
        }
        let (version_now, current) = self.snapshot();
        if version_now != expected {
            return stale(version_now);
        }
        let preview = match self.evaluate(&current, &proposed) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let diff = policy::diff(&current, &proposed);
        if diff.is_empty() {
            let st = self.lock();
            return (200, self.policies_json(&st));
        }
        if preview.destructive {
            let (code, message) = if !confirm {
                (
                    "destructive_unconfirmed",
                    "applying this policy deletes data the current policy would keep, or loses data; resend with confirm_destructive: true and the impact_token of the preview you reviewed",
                )
            } else if token != Some(preview.impact_token.as_str()) {
                (
                    "impact_changed",
                    "what this change would delete differs from the preview you confirmed (or no impact_token was sent); review the impact below and confirm again with its impact_token",
                )
            } else {
                ("", "")
            };
            if !code.is_empty() {
                return (
                    428,
                    json!({
                        "error": code,
                        "message": message,
                        "valid": true,
                        "problems": [],
                        "diff": diff,
                        "impact": preview.impact,
                        "destructive": true,
                        "lossy_settings": preview.lossy,
                        "impact_token": preview.impact_token,
                    }),
                );
            }
        }

        let Some(version) = version_now.checked_add(1) else {
            return err(500, "version_exhausted", "the policy version counter is at its maximum; nothing was applied");
        };
        let ts = now_ms();
        let overrides = policy::overrides_between(&self.file_defaults, &proposed);
        let file = PolicyFile {
            schema_version: POLICY_SCHEMA_VERSION,
            version,
            updated_at_ms: ts,
            updated_by: actor.to_string(),
            overrides: overrides.clone(),
            document: proposed.clone(),
        };
        if let Err(e) = policy_store::write_policy_file(&self.data_dir, &file) {
            return err(500, "persist_failed", format!("nothing was applied: {e}"));
        }
        let entry = AuditEntry {
            ts_ms: ts,
            actor: actor.to_string(),
            version,
            diff,
            impact_summary: json!({
                "would_expire_files": preview.impact.retention.would_expire_files,
                "would_expire_bytes": preview.impact.retention.would_expire_bytes,
                "would_reclaim": preview.impact.disk.would_reclaim,
                "fraction_after": preview.impact.disk.fraction_after,
                "destructive": preview.destructive,
                "confirmed_destructive": preview.destructive && confirm,
                "lossy_settings": preview.lossy.iter().map(|l| l.path.clone()).collect::<Vec<_>>(),
            }),
            policies: proposed.clone(),
        };
        // policy.json already landed, so the change is real; a failed audit
        // append is repaired from it at the next start, and said loudly now.
        if let Err(e) = policy_store::append_audit(&self.data_dir, &entry) {
            tracing::error!(error = %e, version, "policy applied but its audit line could not be written");
        }
        if let Err(e) = self.handle.apply(&proposed) {
            // Unreachable for a validated document; if it ever happens the
            // persisted version and the live one disagree and that must be loud.
            tracing::error!(error = %e, version, "validated policy could not be published to the running process");
        }
        tracing::info!(version, actor, changes = entry.diff.len(), "runtime policy changed");

        let mut st = self.lock();
        st.version = version;
        st.updated_at_ms = ts;
        st.updated_by = actor.to_string();
        st.overrides = overrides;
        st.effective = proposed;
        (200, self.policies_json(&st))
    }
}

fn stale(current_version: u64) -> Resp {
    (
        409,
        json!({
            "error": "version_conflict",
            "message": "the policy changed since you read it; re-read and retry",
            "current_version": current_version,
        }),
    )
}

// --- HTTP ---------------------------------------------------------------

fn digest(s: &str) -> [u8; 32] {
    Sha256::digest(s.as_bytes()).into()
}

/// Compares digests, not the tokens: equal-length inputs make the comparison
/// time independent of where the first mismatch is, and of the token length.
fn token_matches(expected: &[u8; 32], presented: &str) -> bool {
    let got = digest(presented);
    let mut diff = 0u8;
    for (a, b) in expected.iter().zip(got.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Strict `host[:port]` / `[v6][:port]` check. Anything that is not exactly
/// that (userinfo, trailing junk, odd characters) is refused rather than
/// prefix-matched, so `localhost:80@evil.com` is not loopback.
pub(crate) fn host_allowed(host_header: Option<&str>, allowed: &[String]) -> bool {
    let Some(value) = host_header else { return false };
    let value = value.trim().to_ascii_lowercase();
    let port_ok = |p: &str| p.is_empty() || (p.len() <= 5 && p.bytes().all(|b| b.is_ascii_digit()));
    let host = if let Some(rest) = value.strip_prefix('[') {
        let Some((inner, tail)) = rest.split_once(']') else { return false };
        let tail_ok = tail.is_empty() || tail.strip_prefix(':').is_some_and(port_ok);
        if !tail_ok || !inner.bytes().all(|b| b.is_ascii_hexdigit() || b == b':' || b == b'.') {
            return false;
        }
        inner
    } else {
        let (h, p) = match value.split_once(':') {
            Some((h, p)) => (h, p),
            None => (value.as_str(), ""),
        };
        if !port_ok(p) || !h.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-') {
            return false;
        }
        h
    };
    host == "localhost"
        || host == "::1"
        || host.parse::<std::net::Ipv4Addr>().is_ok_and(|ip| ip.is_loopback())
        || allowed.iter().any(|h| h == host)
}

struct Api {
    service: Arc<AdminService>,
    token: [u8; 32],
    allowed_hosts: Vec<String>,
}

fn reply((status, body): Resp) -> Reply {
    let out = Reply::json(status, body.to_string()).with_header("Cache-Control", "no-store");
    if status == 429 { out.with_header("Retry-After", "1") } else { out }
}

fn query_limit(query: &str) -> Result<usize, Resp> {
    for pair in query.split('&') {
        if let Some(v) = pair.strip_prefix("limit=") {
            return match v.parse::<usize>() {
                Ok(n) if (1..=MAX_HISTORY_LIMIT).contains(&n) => Ok(n),
                _ => Err(err(400, "bad_limit", format!("limit must be 1 to {MAX_HISTORY_LIMIT}"))),
            };
        }
    }
    Ok(DEFAULT_HISTORY_LIMIT)
}

impl Handler for Api {
    fn handle(&self, req: Incoming) -> Reply {
        // Host before token: a rebinding page must not even learn whether a
        // token is wanted.
        if req.header_count("host") != 1 || !host_allowed(req.header("host"), &self.allowed_hosts) {
            return reply(err(403, "forbidden_host", "Host header is not loopback and not in [admin].allowed_hosts"));
        }
        // Two Authorization headers are ambiguous (a proxy may pick the other one), so neither counts.
        let presented = req
            .header("authorization")
            .filter(|_| req.header_count("authorization") == 1)
            .and_then(|v| v.trim().strip_prefix("Bearer ").or_else(|| v.trim().strip_prefix("bearer ")))
            .map(str::trim);
        if !presented.is_some_and(|t| token_matches(&self.token, t)) {
            return reply(err(401, "unauthorized", "a valid bearer token is required"))
                .with_header("WWW-Authenticate", "Bearer");
        }

        let s = &self.service;
        let etag = |r: Resp| {
            let tag = r.1.get("version").and_then(Value::as_u64).filter(|_| r.0 == 200);
            let out = reply(r);
            match tag {
                Some(v) => out.with_header("ETag", &format!("\"{v}\"")),
                None => out,
            }
        };
        let if_match = req.header("if-match");
        match (req.method, req.path) {
            (Method::Get, "/admin/v1/info") => reply(s.info()),
            (Method::Get, "/admin/v1/typed-columns") => reply(s.typed_columns()),
            (Method::Get, "/admin/v1/policies") => etag(s.get_policies()),
            (Method::Put, "/admin/v1/policies") => etag(s.put(if_match, &req.body)),
            (Method::Post, "/admin/v1/policies/preview") => reply(s.preview(&req.body)),
            (Method::Post, "/admin/v1/policies/restore") => etag(s.restore(if_match, &req.body)),
            (Method::Get, "/admin/v1/templates/merge-suggestions") => reply(s.merge_suggestions()),
            (Method::Get, "/admin/v1/parts") => reply(s.parts_list(req.query)),
            (Method::Get, "/admin/v1/parts/audit") => reply(s.parts_audit(req.query)),
            (Method::Post, "/admin/v1/parts/sweep") => reply(s.parts_sweep(&req.body)),
            (Method::Get, "/admin/v1/parts/replicate") => reply(s.parts_replicate(false)),
            (Method::Post, "/admin/v1/parts/replicate") => reply(s.parts_replicate(true)),
            (Method::Get, "/admin/v1/sync") => reply(s.sync_get()),
            (Method::Put, "/admin/v1/sync") => reply(s.sync_put(&req.body)),
            (Method::Post, "/admin/v1/sync/test") => reply(s.sync_test(&req.body)),
            (Method::Post, "/admin/v1/sync/backfill") => reply(s.sync_backfill(&req.body)),
            (Method::Post, "/admin/v1/sync/backfill/cancel") => reply(s.sync_backfill_cancel()),
            (Method::Post, "/admin/v1/sync/parts") => reply(s.sync_parts(&req.body)),
            (Method::Post, "/admin/v1/sync/parts/cancel") => reply(s.sync_parts_cancel()),
            (Method::Get, "/admin/v1/policies/history") => match query_limit(req.query) {
                Ok(n) => reply(s.history(n)),
                Err(r) => reply(r),
            },
            (
                _,
                "/admin/v1/info"
                | "/admin/v1/typed-columns"
                | "/admin/v1/policies"
                | "/admin/v1/policies/preview"
                | "/admin/v1/policies/restore"
                | "/admin/v1/policies/history"
                | "/admin/v1/sync"
                | "/admin/v1/parts"
                | "/admin/v1/parts/audit"
                | "/admin/v1/parts/sweep"
                | "/admin/v1/parts/replicate"
                | "/admin/v1/templates/merge-suggestions"
                | "/admin/v1/sync/test"
                | "/admin/v1/sync/backfill"
                | "/admin/v1/sync/backfill/cancel"
                | "/admin/v1/sync/parts"
                | "/admin/v1/sync/parts/cancel",
            ) => reply(err(405, "method_not_allowed", "wrong method for this path")),
            _ => reply(err(404, "not_found", "no such admin endpoint")),
        }
    }

    fn reject(&self, reason: Reject) -> Reply {
        match reason {
            Reject::TooLarge => reply(err(413, "too_large", "request body too large")),
            Reject::Unreadable(m) => reply(err(400, "bad_request", m)),
        }
    }
}

/// A running admin listener; stop it with [`AdminServer::shutdown`].
pub struct AdminServer {
    server: BoundedServer,
}

impl AdminServer {
    pub fn start(cfg: &AdminConfig, service: Arc<AdminService>) -> Result<Self, AdminError> {
        let token = std::env::var(TOKEN_ENV)
            .ok()
            .filter(|t| !t.is_empty())
            .or_else(|| cfg.token.clone().filter(|t| !t.is_empty()))
            .ok_or(AdminError::MissingToken)?;

        let addrs: Vec<SocketAddr> = cfg
            .addr
            .to_socket_addrs()
            .map_err(|source| HttpError::Resolve { addr: cfg.addr.clone(), source })?
            .collect();
        if addrs.is_empty() {
            return Err(HttpError::NoAddress { addr: cfg.addr.clone() }.into());
        }
        if !addrs.iter().all(|a| a.ip().is_loopback()) && token.len() < MIN_REMOTE_TOKEN_LEN {
            return Err(AdminError::WeakToken { addr: cfg.addr.clone(), len: token.len() });
        }

        let api = Arc::new(Api {
            service: Arc::clone(&service),
            token: digest(&token),
            allowed_hosts: cfg.allowed_hosts.iter().map(|h| h.to_ascii_lowercase()).collect(),
        });
        let limits = Limits { max_body_bytes: MAX_BODY_BYTES, max_decompressed_bytes: MAX_BODY_BYTES };
        let server = BoundedServer::start(&cfg.addr, "admin-api", limits, HeadLimits::default(), api)?;
        service.add_listener("admin", server.local_addr().to_string());
        Ok(Self { server })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub fn shutdown(self) {
        self.server.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget;
    use crate::partition::{PartitionKey, SECS_PER_HOUR};
    use std::io::{Read, Write};
    use std::path::Path;
    use std::time::Duration;
    use crate::policy::RetentionRule;

    const TOKEN: &str = "test-token-0123456789-abcdefghij";

    struct Rig {
        tmp: tempfile::TempDir,
        config: Config,
        service: Arc<AdminService>,
        handle: Arc<PolicyHandle>,
        server: Option<AdminServer>,
        base: String,
        agent: ureq::Agent,
    }

    fn base_config(dir: &Path) -> Config {
        toml::from_str(&format!("[storage]\ndata_dir = {dir:?}\n")).unwrap()
    }

    fn rig() -> Rig {
        rig_at(Arc::new(now_unix_secs))
    }

    fn rig_at(clock: Arc<dyn Fn() -> u64 + Send + Sync>) -> Rig {
        rig_with(clock, |_| {})
    }

    fn rig_with(clock: Arc<dyn Fn() -> u64 + Send + Sync>, edit: impl FnOnce(&mut Config)) -> Rig {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = base_config(tmp.path());
        edit(&mut config);
        std::fs::create_dir_all(config.store_dir()).unwrap();
        let start = startup(&config).unwrap();
        let handle = PolicyHandle::new(start.effective.clone()).unwrap();
        let probe = IngestProbe {
            stats: Arc::new(IngestStats::default()),
            written: Arc::new(AtomicU64::new(0)),
            refused_debug_pressure: Arc::new(AtomicU64::new(0)),
        };
        let service =
            AdminService::with_clock(&config, start, Arc::clone(&handle), probe, vec![], clock);
        let cfg = AdminConfig {
            enabled: true,
            addr: "127.0.0.1:0".into(),
            token: Some(TOKEN.into()),
            allowed_hosts: vec![],
        };
        let server = AdminServer::start(&cfg, Arc::clone(&service)).unwrap();
        let base = format!("http://{}", server.local_addr());
        let agent: ureq::Agent =
            ureq::Agent::config_builder().http_status_as_error(false).build().into();
        Rig { tmp, config, service, handle, server: Some(server), base, agent }
    }

    impl Rig {
        fn call(&self, method: &str, path: &str, headers: &[(&str, &str)], body: Option<Value>) -> (u16, Value) {
            let url = format!("{}{path}", self.base);
            let mut headers: Vec<(&str, &str)> = headers.to_vec();
            let auth = format!("Bearer {TOKEN}");
            if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("authorization")) {
                headers.push(("Authorization", &auth));
            }
            let body = body.map(|b| b.to_string()).unwrap_or_default();
            let mut resp = match method {
                "GET" => {
                    let mut r = self.agent.get(&url);
                    for (k, v) in &headers {
                        r = r.header(*k, *v);
                    }
                    r.call()
                }
                "POST" => {
                    let mut r = self.agent.post(&url);
                    for (k, v) in &headers {
                        r = r.header(*k, *v);
                    }
                    r.send(&body)
                }
                "PUT" => {
                    let mut r = self.agent.put(&url);
                    for (k, v) in &headers {
                        r = r.header(*k, *v);
                    }
                    r.send(&body)
                }
                other => panic!("method {other}"),
            }
            .unwrap();
            let status = resp.status().as_u16();
            let text = resp.body_mut().read_to_string().unwrap();
            (status, serde_json::from_str(&text).unwrap_or(Value::Null))
        }

        fn policies(&self) -> Value {
            self.call("GET", "/admin/v1/policies", &[], None).1
        }

        /// A confirming caller sends the `impact_token` of the preview it reviewed.
        fn put(&self, version: u64, doc: &Policies, confirm: bool) -> (u16, Value) {
            let token = if confirm {
                self.call("POST", "/admin/v1/policies/preview", &[], Some(json!({"policies": doc}))).1["impact_token"].clone()
            } else {
                Value::Null
            };
            self.call(
                "PUT",
                "/admin/v1/policies",
                &[("If-Match", &version.to_string())],
                Some(json!({"policies": doc, "confirm_destructive": confirm, "impact_token": token, "actor": "alice"})),
            )
        }

        /// Restore at `if_match`; a confirming caller reads the token from the 428 first.
        fn restore(&self, if_match: u64, version: u64, confirm: bool) -> (u16, Value) {
            let path = "/admin/v1/policies/restore";
            let h = [("If-Match", if_match.to_string())];
            let hdr = [(h[0].0, h[0].1.as_str())];
            let first = self.call("POST", path, &hdr, Some(json!({"version": version, "actor": "bob"})));
            if !confirm || first.0 != 428 {
                return first;
            }
            self.call("POST", path, &hdr, Some(json!({"version": version, "actor": "bob", "confirm_destructive": true, "impact_token": first.1["impact_token"]})))
        }

        fn effective(&self) -> Policies {
            serde_json::from_value(self.policies()["effective"].clone()).unwrap()
        }

        fn partition(&self, hour: u64, bucket: &str, bytes: usize) -> PathBuf {
            let dir = PartitionKey { epoch_hour: hour, bucket: bucket.into() }.dir(&self.config.store_dir());
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("000001.parquet"), vec![0u8; bytes]).unwrap();
            dir
        }
    }

    impl Drop for Rig {
        fn drop(&mut self) {
            if let Some(s) = self.server.take() {
                s.shutdown();
            }
            let _ = &self.tmp;
        }
    }

    fn raw(addr: SocketAddr, request: &str) -> String {
        let mut conn = std::net::TcpStream::connect(addr).unwrap();
        // A server may answer and hang up before a huge request is fully sent.
        let _ = conn.write_all(request.as_bytes());
        let mut out = Vec::new();
        let _ = conn.read_to_end(&mut out);
        String::from_utf8_lossy(&out).into_owned()
    }

    // --- auth -----------------------------------------------------------

    #[test]
    fn every_request_needs_the_token_including_get() {
        let r = rig();
        for (path, method) in [("/admin/v1/info", "GET"), ("/admin/v1/policies", "GET"), ("/admin/v1/policies/history", "GET")] {
            let (status, _) = r.call(method, path, &[("Authorization", "Bearer ")], None);
            assert_eq!(status, 401, "{path} with an empty token");
        }
        // No Authorization header at all.
        let resp = raw(r.server.as_ref().unwrap().local_addr(), "GET /admin/v1/info HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        assert!(resp.starts_with("HTTP/1.1 401"), "{resp}");
        assert!(resp.to_ascii_lowercase().contains("www-authenticate: bearer"));
    }

    #[test]
    fn a_wrong_or_near_miss_token_is_refused() {
        let r = rig();
        for bad in [
            "Bearer nope".to_string(),
            format!("Bearer {}x", TOKEN),
            format!("Bearer {}", &TOKEN[..TOKEN.len() - 1]),
            format!("Basic {TOKEN}"),
            TOKEN.to_string(),
        ] {
            let (status, body) = r.call("GET", "/admin/v1/policies", &[("Authorization", &bad)], None);
            assert_eq!(status, 401, "{bad}");
            assert!(body.get("effective").is_none(), "no data on a failed auth");
        }
        assert_eq!(r.call("GET", "/admin/v1/info", &[], None).0, 200);
    }

    #[test]
    fn a_foreign_host_header_is_refused_before_auth() {
        let r = rig();
        let addr = r.server.as_ref().unwrap().local_addr();
        let resp = raw(addr, &format!("GET /admin/v1/info HTTP/1.1\r\nHost: evil.example\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"));
        assert!(resp.starts_with("HTTP/1.1 403"), "{resp}");
        let resp = raw(addr, &format!("GET /admin/v1/info HTTP/1.1\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"));
        assert!(resp.starts_with("HTTP/1.1 400") || resp.starts_with("HTTP/1.1 403"), "no Host: {resp}");
        // Rebinding page without a token still gets the host verdict.
        let resp = raw(addr, "GET /admin/v1/info HTTP/1.1\r\nHost: evil.example\r\nConnection: close\r\n\r\n");
        assert!(resp.starts_with("HTTP/1.1 403"), "{resp}");
        let ok = raw(addr, &format!("GET /admin/v1/info HTTP/1.1\r\nHost: localhost:9101\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"));
        assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
    }

    #[test]
    fn allowed_hosts_extends_the_loopback_default() {
        assert!(host_allowed(Some("Console.Internal:9101"), &["console.internal".into()]));
        assert!(!host_allowed(Some("console.internal"), &[]));
        assert!(host_allowed(Some("[::1]:9101"), &[]));
        assert!(host_allowed(Some("127.0.0.1"), &[]));
        assert!(!host_allowed(None, &["x".into()]));
    }

    #[test]
    fn no_cors_headers_are_ever_sent() {
        let r = rig();
        let addr = r.server.as_ref().unwrap().local_addr();
        let resp = raw(addr, &format!("OPTIONS /admin/v1/policies HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: http://evil.example\r\nAccess-Control-Request-Method: PUT\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"));
        assert!(!resp.to_ascii_lowercase().contains("access-control"), "{resp}");
        let get = raw(addr, &format!("GET /admin/v1/info HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: http://evil.example\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"));
        assert!(!get.to_ascii_lowercase().contains("access-control"), "{get}");
    }

    #[test]
    fn the_listener_refuses_to_start_without_a_token() {
        let tmp = tempfile::tempdir().unwrap();
        let config = base_config(tmp.path());
        let start = startup(&config).unwrap();
        let handle = PolicyHandle::new(start.effective.clone()).unwrap();
        let probe = IngestProbe {
            stats: Arc::new(IngestStats::default()),
            written: Arc::new(AtomicU64::new(0)),
            refused_debug_pressure: Arc::new(AtomicU64::new(0)),
        };
        let svc = AdminService::new(&config, start, handle, probe, vec![]);
        for token in [None, Some(String::new())] {
            let cfg = AdminConfig { enabled: true, addr: "127.0.0.1:0".into(), token, allowed_hosts: vec![] };
            let e = AdminServer::start(&cfg, Arc::clone(&svc)).err().expect("must refuse");
            assert!(matches!(e, AdminError::MissingToken), "{e}");
        }
    }

    #[test]
    fn a_short_token_is_refused_on_a_non_loopback_bind_only() {
        let tmp = tempfile::tempdir().unwrap();
        let config = base_config(tmp.path());
        let start = startup(&config).unwrap();
        let handle = PolicyHandle::new(start.effective.clone()).unwrap();
        let probe = IngestProbe {
            stats: Arc::new(IngestStats::default()),
            written: Arc::new(AtomicU64::new(0)),
            refused_debug_pressure: Arc::new(AtomicU64::new(0)),
        };
        let svc = AdminService::new(&config, start, handle, probe, vec![]);

        let short = AdminConfig { enabled: true, addr: "0.0.0.0:0".into(), token: Some("x".repeat(23)), allowed_hosts: vec![] };
        let e = AdminServer::start(&short, Arc::clone(&svc)).err().expect("23 chars on 0.0.0.0");
        assert!(matches!(e, AdminError::WeakToken { len: 23, .. }), "{e}");

        let ok = AdminConfig { token: Some("x".repeat(24)), ..short.clone() };
        AdminServer::start(&ok, Arc::clone(&svc)).expect("24 chars is enough").shutdown();

        let loopback = AdminConfig { addr: "127.0.0.1:0".into(), token: Some("short".into()), ..short };
        AdminServer::start(&loopback, svc).expect("loopback may use a short token").shutdown();
    }

    #[test]
    fn config_debug_never_prints_the_token() {
        let c = AdminConfig { token: Some("s3cret-value".into()), ..Default::default() };
        assert!(!format!("{c:?}").contains("s3cret-value"));
    }

    #[test]
    fn the_listener_starts_and_stops() {
        let mut r = rig();
        let addr = r.server.as_ref().unwrap().local_addr();
        assert!(std::net::TcpStream::connect(addr).is_ok());
        r.server.take().unwrap().shutdown();
        // `shutdown` joins the accept thread, which owns the listener, so the port is closed by the
        // time it returns: no grace period.
        assert!(std::net::TcpStream::connect(addr).is_err(), "port must be released after shutdown");
    }

    // --- reads ----------------------------------------------------------

    #[test]
    fn typed_columns_lists_the_registry() {
        let r = rig();
        let (status, v) = r.call("GET", "/admin/v1/typed-columns", &[], None);
        assert_eq!(status, 200);
        assert_eq!(v, json!({"columns": []}));

        let mut cat = Catalog::open(&r.config.catalog_path()).unwrap();
        cat.register_typed_columns(&[crate::typed::TypedColumn {
            column: "attr_latency_ms".into(),
            key: "latency_ms".into(),
            ty: crate::typed::ColumnType::Int64,
            service: "api".into(),
            promoted_at: 7,
        }])
        .unwrap();
        let (_, v) = r.call("GET", "/admin/v1/typed-columns", &[], None);
        assert_eq!(
            v["columns"],
            json!([{"column": "attr_latency_ms", "key": "latency_ms", "type": "int64", "service": "api", "promoted_at": 7}])
        );
        assert_eq!(r.call("POST", "/admin/v1/typed-columns", &[], Some(json!({}))).0, 405);
    }

    #[test]
    fn info_reports_the_contract_shape() {
        let r = rig();
        let (status, v) = r.call("GET", "/admin/v1/info", &[], None);
        assert_eq!(status, 200);
        for key in ["version", "started_at_ms", "uptime_secs", "listeners", "disk", "ingest", "store"] {
            assert!(v.get(key).is_some(), "missing {key}: {v}");
        }
        assert_eq!(v["listeners"][0]["kind"], "admin");
        assert_eq!(v["disk"]["pressure"], "normal");
        assert_eq!(v["disk"]["budget_bytes"], 32u64 * 1024 * 1024 * 1024);
        for k in ["debug", "info", "critical"] {
            assert_eq!(v["ingest"]["dropped_by_class"][k], 0);
        }
        assert_eq!(v["store"], json!({"files": 0, "rows": 0, "bytes": 0}));
    }

    #[test]
    fn initial_policies_come_from_the_file_with_everything_sourced_from_it() {
        let r = rig();
        let v = r.policies();
        assert_eq!(v["version"], 0);
        assert_eq!(v["updated_by"], "file");
        assert_eq!(v["pending_restart"], json!([]));
        assert_eq!(v["effective"]["retention"][1]["name"], "info");
        assert_eq!(v["effective"]["storage"]["high_water"], 0.8);
        assert_eq!(v["apply"]["ingest.queue_capacity"], "restart");
        assert_eq!(v["apply"]["retention[2].retention_secs"], "hot");
        assert!(v["sources"].as_object().unwrap().values().all(|s| s == "file"));
    }

    // --- preview --------------------------------------------------------

    #[test]
    fn preview_reports_problems_diff_and_impact_without_changing_anything() {
        let r = rig();
        let mut doc = r.effective();
        doc.storage.high_water = 0.99; // not below critical_water
        let (status, v) = r.call("POST", "/admin/v1/policies/preview", &[], Some(json!({"policies": doc})));
        assert_eq!(status, 200);
        assert_eq!(v["valid"], false);
        assert_eq!(v["problems"][0]["path"], "storage.high_water");
        assert_eq!(v["impact"], Value::Null);

        let mut doc = r.effective();
        doc.retention[2].retention_secs = 60;
        let (_, v) = r.call("POST", "/admin/v1/policies/preview", &[], Some(json!({"policies": doc})));
        assert_eq!(v["valid"], true);
        assert_eq!(v["diff"][0]["path"], "retention[2].retention_secs");
        assert_eq!(v["diff"][0]["apply"], "hot");
        assert_eq!(v["impact"]["retention"]["per_bucket"].as_array().unwrap().len(), 3);
        assert_eq!(v["destructive"], false);
        assert_eq!(r.policies()["version"], 0, "preview must not write");
        assert!(!r.config.storage.data_dir.join("policy.json").exists());
    }

    // --- writes ---------------------------------------------------------

    #[test]
    fn put_requires_if_match_and_rejects_a_stale_one_with_the_current_version() {
        let r = rig();
        let mut doc = r.effective();
        doc.pushdown.enabled = false;

        let (status, v) = r.call("PUT", "/admin/v1/policies", &[], Some(json!({"policies": doc, "actor": "a"})));
        assert_eq!((status, v["error"].as_str()), (400, Some("if_match_required")));

        assert_eq!(r.put(0, &doc, true).0, 200);
        let mut again = doc.clone();
        again.pushdown.trigger_severity = 21;
        let (status, v) = r.put(0, &again, false); // stale: the server is at 1
        assert_eq!(status, 409);
        assert_eq!(v["current_version"], 1);
        assert_eq!(r.effective().pushdown.trigger_severity, 17, "stale write changed nothing");
        assert_eq!(r.put(1, &again, false).0, 200);
        assert_eq!(r.policies()["version"], 2);
    }

    #[test]
    fn two_concurrent_writers_cannot_both_win() {
        let r = rig();
        let mut a = r.effective();
        a.storage.disk_budget_bytes = 111_000_000;
        let mut b = r.effective();
        b.storage.disk_budget_bytes = 222_000_000;
        let (r1, r2) = std::thread::scope(|s| {
            let h1 = s.spawn(|| r.put(0, &a, false).0);
            let h2 = s.spawn(|| r.put(0, &b, false).0);
            (h1.join().unwrap(), h2.join().unwrap())
        });
        let mut got = [r1, r2];
        got.sort();
        assert_eq!(got, [200, 409]);
        assert_eq!(r.policies()["version"], 1);
    }

    #[test]
    fn invalid_documents_are_422_with_paths_and_change_nothing() {
        let r = rig();
        let mut doc = r.effective();
        doc.retention[1].min_severity = 99;
        doc.storage.disk_budget_bytes = 0;
        let (status, v) = r.put(0, &doc, true);
        assert_eq!(status, 422);
        let paths: Vec<_> = v["problems"].as_array().unwrap().iter().map(|p| p["path"].as_str().unwrap()).collect();
        assert!(paths.contains(&"retention[1].min_severity") && paths.contains(&"storage.disk_budget_bytes"), "{paths:?}");
        assert_eq!(r.policies()["version"], 0);

        // Unknown fields and wrong types are problems too, not 500s or silent drops.
        let (status, v) = r.call("PUT", "/admin/v1/policies", &[("If-Match", "0")], Some(json!({"policies": r.effective(), "actor": "a", "confirm_destructve": true})));
        assert_eq!(status, 422, "{v}");
        let (status, _) = r.call("PUT", "/admin/v1/policies", &[("If-Match", "0")], Some(json!({"policies": {"retention": "x"}, "actor": "a"})));
        assert_eq!(status, 422);
        let resp = r.agent.put(&format!("{}/admin/v1/policies", r.base)).header("Authorization", &format!("Bearer {TOKEN}")).header("If-Match", "0").send("{not json").unwrap();
        assert_eq!(resp.status().as_u16(), 400);
    }

    #[test]
    fn actor_is_required_and_sanitised() {
        let r = rig();
        let doc = r.effective();
        for actor in ["", "   ", "bad\nname"] {
            let (status, v) = r.call("PUT", "/admin/v1/policies", &[("If-Match", "0")], Some(json!({"policies": doc, "actor": actor})));
            assert_eq!(status, 422, "{actor:?}");
            assert_eq!(v["problems"][0]["path"], "actor");
        }
    }

    #[test]
    fn a_noop_put_does_not_burn_a_version_or_write_an_audit_line() {
        let r = rig();
        let doc = r.effective();
        let (status, v) = r.put(0, &doc, false);
        assert_eq!((status, v["version"].clone()), (200, json!(0)));
        assert!(!r.config.storage.data_dir.join("policy-audit.jsonl").exists());
    }

    #[test]
    fn destructive_changes_need_confirmation_and_say_what_they_would_delete() {
        let r = rig();
        let now = now_unix_secs();
        // An info partition old enough that 7d keeps it and 1h would not.
        let hour = (now - 3 * 86_400) / SECS_PER_HOUR;
        let dir = r.partition(hour, "info", 50);
        let mut doc = r.effective();
        doc.retention[1].retention_secs = 3600;

        let (status, v) = r.put(0, &doc, false);
        assert_eq!(status, 428, "{v}");
        assert_eq!(v["error"], "destructive_unconfirmed");
        assert_eq!(v["destructive"], true);
        assert_eq!(v["impact"]["retention"]["would_expire_files"], 1);
        assert_eq!(v["impact"]["retention"]["would_expire_bytes"], 50);
        assert_eq!(r.policies()["version"], 0, "unconfirmed means unapplied");
        assert!(dir.exists());

        let (status, v) = r.put(0, &doc, true);
        assert_eq!(status, 200, "{v}");
        assert_eq!(v["version"], 1);
        assert_eq!(v["sources"]["retention[1].retention_secs"], "runtime");
        assert_eq!(v["sources"]["storage.high_water"], "file");
        assert!(dir.exists(), "the API changes the policy; deletion is the next maintenance pass");
    }

    #[test]
    fn renaming_or_removing_a_bucket_with_partitions_is_refused() {
        let r = rig();
        r.partition(100, "info", 5);
        let mut renamed = r.effective();
        renamed.retention[1].name = "notice".into();
        let (status, v) = r.put(0, &renamed, true);
        assert_eq!(status, 422, "{v}");
        let msg = v["problems"][0]["message"].as_str().unwrap();
        assert!(msg.contains("\"info\"") && msg.contains("partition"), "{msg}");
        assert_eq!(r.policies()["version"], 0, "confirm_destructive does not override orphan protection");

        let (_, v) = r.call("POST", "/admin/v1/policies/preview", &[], Some(json!({"policies": renamed})));
        assert_eq!(v["valid"], false);

        // An empty bucket can be renamed.
        let mut ok = r.effective();
        ok.retention[2].name = "warn_up".into();
        assert_eq!(r.put(0, &ok, false).0, 200);
    }

    #[test]
    fn restart_mode_changes_are_stored_and_pending_until_the_process_restarts_with_them() {
        let r = rig();
        let mut doc = r.effective();
        doc.ingest.queue_capacity = 1024;
        let (status, v) = r.put(0, &doc, false);
        assert_eq!(status, 200);
        assert_eq!(v["pending_restart"], json!(["ingest.queue_capacity"]));
        assert_eq!(v["apply"]["ingest.queue_capacity"], "restart");
        assert_eq!(r.handle.running().ingest.queue_capacity, 65_536, "the running process did not change");

        // A new process over the same data_dir starts with it, and nothing is pending.
        let start = startup(&r.config).unwrap();
        assert_eq!(start.effective.ingest.queue_capacity, 1024);
        let handle = PolicyHandle::new(start.effective.clone()).unwrap();
        let probe = IngestProbe {
            stats: Arc::new(IngestStats::default()),
            written: Arc::new(AtomicU64::new(0)),
            refused_debug_pressure: Arc::new(AtomicU64::new(0)),
        };
        let svc = AdminService::new(&r.config, start, handle, probe, vec![]);
        let (_, v) = svc.get_policies();
        assert_eq!(v["pending_restart"], json!([]));
        assert_eq!(v["version"], 1, "the version counter continues across restarts");
    }

    #[test]
    fn precedence_runtime_beats_file_and_untouched_fields_follow_the_file() {
        let r = rig();
        let mut doc = r.effective();
        doc.pushdown.enabled = false;
        assert_eq!(r.put(0, &doc, true).0, 200);

        // "Restart" with an edited file: new data_dir state is the same dir.
        let mut cfg = r.config.clone();
        cfg.storage.disk_budget_bytes = 123_456_789;
        cfg.pushdown.enabled = true;
        let start = startup(&cfg).unwrap();
        assert_eq!(start.effective.storage.disk_budget_bytes, 123_456_789, "file edit reaches untouched fields");
        assert!(!start.effective.pushdown.enabled, "runtime override survives the file saying true");
    }

    // --- audit, history, restore ---------------------------------------

    #[test]
    fn every_applied_change_is_audited_and_listed_newest_first() {
        let r = rig();
        let mut d1 = r.effective();
        d1.pushdown.trigger_severity = 21;
        assert_eq!(r.put(0, &d1, false).0, 200);
        let mut d2 = d1.clone();
        d2.storage.disk_budget_bytes = 5_000_000_000;
        assert_eq!(r.put(1, &d2, false).0, 200);

        let lines = std::fs::read_to_string(r.config.storage.data_dir.join("policy-audit.jsonl")).unwrap();
        assert_eq!(lines.lines().count(), 2);
        let first: Value = serde_json::from_str(lines.lines().next().unwrap()).unwrap();
        for k in ["ts_ms", "actor", "version", "diff", "impact_summary"] {
            assert!(first.get(k).is_some(), "audit line lacks {k}: {first}");
        }
        assert_eq!(first["actor"], "alice");
        assert_eq!(first["diff"][0]["path"], "pushdown.trigger_severity");

        let (status, h) = r.call("GET", "/admin/v1/policies/history?limit=1", &[], None);
        assert_eq!(status, 200);
        assert_eq!(h.as_array().unwrap().len(), 1);
        assert_eq!(h[0]["version"], 2);
        let (_, h) = r.call("GET", "/admin/v1/policies/history", &[], None);
        assert_eq!(h.as_array().unwrap().iter().map(|e| e["version"].as_u64().unwrap()).collect::<Vec<_>>(), vec![2, 1]);
        assert!(h[0].get("policies").is_none(), "history is the contract shape; snapshots stay in the file");
        assert_eq!(r.call("GET", "/admin/v1/policies/history?limit=0", &[], None).0, 400);
    }

    #[test]
    fn restore_applies_an_old_snapshot_as_a_new_version() {
        let r = rig();
        let original = r.effective();
        let mut d1 = original.clone();
        d1.pushdown.trigger_severity = 21;
        assert_eq!(r.put(0, &d1, false).0, 200);
        let mut d2 = d1.clone();
        d2.pushdown.enabled = false;
        assert_eq!(r.put(1, &d2, true).0, 200);

        let (status, v) = r.restore(2, 1, false);
        assert_eq!(status, 200, "{v}");
        assert_eq!(v["version"], 3, "restore is a new version, history is never rewritten");
        assert_eq!(v["updated_by"], "bob");
        assert_eq!(r.effective(), d1);
        assert_eq!(r.handle.pushdown_enabled(), true, "restore is applied hot too");

        let (_, h) = r.call("GET", "/admin/v1/policies/history", &[], None);
        assert_eq!(h[0]["version"], 3);
        assert_eq!(h[0]["actor"], "bob");

        let (status, v) = r.restore(3, 42, false);
        assert_eq!((status, v["error"].as_str()), (404, Some("unknown_version")));
    }

    #[test]
    fn restore_goes_through_the_same_destructive_and_orphan_rules() {
        let r = rig();
        let now = now_unix_secs();
        let hour = (now - 3 * 86_400) / SECS_PER_HOUR;
        let dir = r.partition(hour, "info", 50);

        let mut short = r.effective();
        short.retention[1].retention_secs = 3600;
        assert_eq!(r.put(0, &short, true).0, 200);
        let mut long = short.clone();
        long.retention[1].retention_secs = 30 * 86_400;
        assert_eq!(r.put(1, &long, false).0, 200);

        // Restoring v1 (1h info retention) would newly delete the partition.
        let (status, v) = r.restore(2, 1, false);
        assert_eq!(status, 428, "{v}");
        assert_eq!(r.policies()["version"], 2);
        let (status, _) = r.restore(2, 1, true);
        assert_eq!(status, 200);
        assert!(dir.exists());

        // A snapshot whose bucket map would orphan data is refused outright.
        let mut renamed = r.effective();
        renamed.retention[1].name = "notice".into();
        let entry = AuditEntry {
            ts_ms: 1,
            actor: "x".into(),
            version: 99,
            diff: vec![],
            impact_summary: json!({}),
            policies: renamed,
        };
        policy_store::append_audit(&r.config.storage.data_dir, &entry).unwrap();
        let (status, v) = r.restore(3, 99, true);
        assert_eq!(status, 422, "{v}");
    }

    // --- hot application ------------------------------------------------

    #[test]
    fn a_retention_change_through_the_api_governs_the_very_next_maintenance_pass() {
        let now = 101 * SECS_PER_HOUR + 3 * 86_400;
        let r = rig_at(Arc::new(move || now));
        let store = r.config.store_dir();
        let wal_dir = r.config.wal_dir();

        let debug_old = r.partition(100, "debug", 10);
        let info_old = r.partition(100, "info", 20);
        let warn_old = r.partition(100, "warn_plus", 30);
        let info_young = r.partition(160, "info", 40); // ended 12h before `now`
        let mut catalog = Catalog::open(&r.config.catalog_path()).unwrap();

        // The pass exactly as maintenance_pass runs it: policy re-read each time.
        let pass = |catalog: &mut Catalog| {
            let rt = r.handle.runtime();
            budget::expire_and_relieve(&store, &wal_dir, catalog, &rt.buckets, rt.disk_budget_bytes, &rt.watermarks, now)
        };

        // Dry check under the file defaults: only debug is due (1d), info keeps 7d.
        let before = crate::retention::enforce(&store, &r.handle.runtime().buckets, now, true);
        assert_eq!(
            before.expired.iter().map(|e| e.key.bucket.as_str()).collect::<Vec<_>>(),
            vec!["debug"]
        );

        // info 7d -> 1d, through the API. Destructive, so confirmed.
        let mut doc = r.effective();
        doc.retention[1].retention_secs = 86_400;
        assert_eq!(r.put(0, &doc, false).0, 428);
        assert_eq!(r.put(0, &doc, true).0, 200);
        assert!(debug_old.exists() && info_old.exists(), "nothing is deleted until the pass runs");

        let out = pass(&mut catalog);
        let mut gone: Vec<_> = out.expiry.expired.iter().map(|e| (e.key.epoch_hour, e.key.bucket.clone())).collect();
        gone.sort();
        assert_eq!(gone, vec![(100, "debug".to_string()), (100, "info".to_string())], "exactly the partitions the new policy expires");
        assert!(!debug_old.exists() && !info_old.exists());
        assert!(warn_old.exists(), "warn_plus keeps 30d");
        assert!(info_young.exists(), "a young info partition is within the new 1d");
        assert_eq!(out.expiry.bytes_reclaimed, 30);

        // And back out: lengthening info to 30d spares the young one's future.
        let mut longer = r.effective();
        longer.retention[1].retention_secs = 30 * 86_400;
        assert_eq!(r.put(1, &longer, false).0, 200);
        let out = pass(&mut catalog);
        assert!(out.expiry.expired.is_empty());
        assert!(info_young.exists());
    }

    #[test]
    fn a_budget_and_watermark_change_through_the_api_moves_the_pressure_the_pass_reports() {
        let r = rig();
        let store = r.config.store_dir();
        let wal_dir = r.config.wal_dir();
        r.partition(1_000_000, "warn_plus", 1000); // far future: never expires
        // The catalog needs to know the bytes for DiskUsage.
        let mut catalog = Catalog::open(&r.config.catalog_path()).unwrap();
        let path = PartitionKey { epoch_hour: 1_000_000, bucket: "warn_plus".into() }.dir(&store).join("000001.parquet");
        catalog
            .commit_merge(1, uuid::Uuid::from_u128(1), &[crate::catalog::FileEntry {
                path,
                key: PartitionKey { epoch_hour: 1_000_000, bucket: "warn_plus".into() },
                stats: crate::store::FileStats { rows: 1, bytes: 1000, min_observed_nanos: 0, max_observed_nanos: 0 },
            }], &[], 0)
            .unwrap();
        let pass = |catalog: &mut Catalog| {
            let rt = r.handle.runtime();
            budget::expire_and_relieve(&store, &wal_dir, catalog, &rt.buckets, rt.disk_budget_bytes, &rt.watermarks, 0)
        };
        assert_eq!(pass(&mut catalog).pressure, Pressure::Normal);

        let mut doc = r.effective();
        doc.storage.disk_budget_bytes = 1100; // 1000/1100 = 90.9%: above high 80%
        let (status, v) = r.put(0, &doc, false);
        assert_eq!(status, 428, "early reclaim would delete the file: {v}");
        // Lower the target so nothing is deleted: high 0.95, crit 0.99.
        doc.storage.high_water = 0.95;
        doc.storage.critical_water = 0.99;
        doc.storage.reclaim_target = 0.9;
        assert_eq!(r.put(0, &doc, false).0, 200);
        assert_eq!(pass(&mut catalog).pressure, Pressure::Normal, "90.9% is under the new 95% high water");

        let mut doc2 = r.effective();
        doc2.storage.high_water = 0.5;
        doc2.storage.reclaim_target = 0.4;
        doc2.storage.critical_water = 0.9;
        let (status, _) = r.put(1, &doc2, true);
        assert_eq!(status, 200);
        let out = pass(&mut catalog);
        assert_eq!(out.pressure, Pressure::Critical, "90.9% is over the new 90% critical water");
    }

    #[test]
    fn queue_timeout_and_pushdown_apply_without_a_restart() {
        let r = rig();
        let mut doc = r.effective();
        doc.pushdown.enabled = false;
        doc.pushdown.trigger_severity = 21;
        doc.ingest.critical_enqueue_timeout_ms = 7;
        assert_eq!(r.put(0, &doc, true).0, 200);
        assert!(!r.handle.pushdown_enabled());
        assert_eq!(r.handle.pushdown_trigger(), 21);
        assert_eq!(r.handle.critical_timeout_cell().load(Ordering::Relaxed), 7);
        assert_eq!(r.policies()["pending_restart"], json!([]));
    }

    #[test]
    fn unknown_paths_and_methods_are_404_and_405() {
        let r = rig();
        assert_eq!(r.call("GET", "/admin/v1/nope", &[], None).0, 404);
        assert_eq!(r.call("POST", "/admin/v1/info", &[], Some(json!({}))).0, 405);
        assert_eq!(r.call("GET", "/", &[], None).0, 404);
        let _ = &r.service;
    }

    // --- review regressions --------------------------------------------

    #[test]
    fn a_confirmation_is_bound_to_the_impact_that_was_reviewed() {
        let r = rig();
        let now = now_unix_secs();
        let hour = (now - 3 * 86_400) / SECS_PER_HOUR;
        r.partition(hour, "info", 50);
        let mut doc = r.effective();
        doc.retention[1].retention_secs = 3600;
        let (_, reviewed) = r.call("POST", "/admin/v1/policies/preview", &[], Some(json!({"policies": doc})));
        assert_eq!(reviewed["destructive"], true);
        let token = reviewed["impact_token"].clone();
        assert!(token.is_string());

        // More old data lands after the operator looked.
        r.partition(hour - 1, "info", 70);
        let body = |t: &Value| json!({"policies": doc, "confirm_destructive": true, "impact_token": t, "actor": "alice"});
        let put = |t: &Value| r.call("PUT", "/admin/v1/policies", &[("If-Match", "0")], Some(body(t)));
        let (status, v) = put(&token);
        assert_eq!(status, 428, "{v}");
        assert_eq!(v["error"], "impact_changed");
        assert_eq!(v["impact"]["retention"]["would_expire_files"], 2, "the 428 shows what it would now delete");
        assert_eq!(r.policies()["version"], 0);

        let (status, _) = put(&Value::Null);
        assert_eq!(status, 428, "a bare confirm_destructive is not enough");

        let (status, v) = put(&v["impact_token"].clone());
        assert_eq!(status, 200, "{v}");
    }

    #[test]
    fn data_losing_settings_need_confirmation_too() {
        let r = rig();
        for (label, edit) in [
            ("tiny queue", (|d: &mut Policies| d.ingest.queue_capacity = 1) as fn(&mut Policies)),
            ("zero critical timeout", |d| d.ingest.critical_enqueue_timeout_ms = 0),
            ("pushdown off", |d| d.pushdown.enabled = false),
        ] {
            let mut doc = r.effective();
            edit(&mut doc);
            let (status, v) = r.put(0, &doc, false);
            assert_eq!(status, 428, "{label}: {v}");
            assert_eq!(v["lossy_settings"].as_array().unwrap().len(), 1, "{label}");
        }
        assert_eq!(r.policies()["version"], 0);
        let mut ok = r.effective();
        ok.ingest.queue_capacity = 2048;
        assert_eq!(r.put(0, &ok, false).0, 200, "a smaller but sane queue is not lossy");
    }

    #[test]
    fn a_scan_in_progress_refuses_a_second_one_and_never_blocks_reads() {
        let r = rig();
        let guard = r.service.try_scan().expect("gate free");
        let t = Instant::now();
        let (status, v) = r.call("POST", "/admin/v1/policies/preview", &[], Some(json!({"policies": r.effective()})));
        assert_eq!((status, v["error"].as_str()), (429, Some("busy")));
        assert_eq!(r.call("GET", "/admin/v1/policies", &[], None).0, 200);
        assert_eq!(r.call("GET", "/admin/v1/info", &[], None).0, 200);
        assert!(t.elapsed() < Duration::from_secs(5));
        drop(guard);
        assert_eq!(r.call("POST", "/admin/v1/policies/preview", &[], Some(json!({"policies": r.effective()}))).0, 200);
    }

    #[test]
    fn bucket_names_that_collide_or_cannot_be_directories_are_refused() {
        let r = rig();
        for name in ["INFO", "Info", "CON", "nul", "COM1", "Warn_Plus\u{202e}", "a b", "caf\u{e9}", "x:y", "a*"] {
            let mut doc = r.effective();
            doc.retention.push(RetentionRule { name: name.into(), min_severity: 17, retention_secs: 60 });
            let (_, v) = r.call("POST", "/admin/v1/policies/preview", &[], Some(json!({"policies": doc})));
            assert_eq!(v["valid"], false, "{name:?} must be refused: {v}");
        }
        let mut doc = r.effective();
        doc.retention.push(RetentionRule { name: "_page+1".into(), min_severity: 17, retention_secs: 60 });
        let (_, v) = r.call("POST", "/admin/v1/policies/preview", &[], Some(json!({"policies": doc})));
        assert_eq!(v["valid"], true, "{v}");
    }

    #[test]
    fn restore_needs_if_match_like_put() {
        let r = rig();
        let mut d1 = r.effective();
        d1.pushdown.trigger_severity = 21;
        assert_eq!(r.put(0, &d1, false).0, 200);
        let (status, v) = r.call("POST", "/admin/v1/policies/restore", &[], Some(json!({"version": 1, "actor": "bob"})));
        assert_eq!((status, v["error"].as_str()), (400, Some("if_match_required")));
        let (status, v) = r.restore(0, 1, false);
        assert_eq!((status, v["current_version"].as_u64()), (409, Some(1)), "stale If-Match");
    }

    #[test]
    fn host_header_must_be_exactly_one_well_formed_host() {
        for bad in ["localhost:80@evil.com", "[::1]evil.com", "127.0.0.1.evil.com", "localhost:80x", "localhost evil", "[::1", ""] {
            assert!(!host_allowed(Some(bad), &[]), "{bad:?}");
        }
        for good in ["localhost", "LOCALHOST:9101", "127.0.0.1:9101", "[::1]:9101", "[::1]"] {
            assert!(host_allowed(Some(good), &[]), "{good:?}");
        }
        let r = rig();
        let addr = r.server.as_ref().unwrap().local_addr();
        let two = |a: &str, b: &str| raw(addr, &format!("GET /admin/v1/info HTTP/1.1\r\nHost: {a}\r\nHost: {b}\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"));
        assert!(two("127.0.0.1", "evil.com").starts_with("HTTP/1.1 403"));
        assert!(two("evil.com", "127.0.0.1").starts_with("HTTP/1.1 403"));
        assert!(two("127.0.0.1", "127.0.0.1").starts_with("HTTP/1.1 403"), "ambiguous even when both are loopback");
    }

    #[test]
    fn an_actor_cannot_reorder_or_split_the_audit_display() {
        let r = rig();
        let mut doc = r.effective();
        doc.pushdown.trigger_severity = 21;
        for actor in ["ali\u{202e}ce", "ali\u{2028}ce", "ali\u{200b}ce", "ali\u{feff}ce"] {
            let (status, v) = r.call("PUT", "/admin/v1/policies", &[("If-Match", "0")], Some(json!({"policies": doc, "actor": actor})));
            assert_eq!(status, 422, "{actor:?}: {v}");
        }
        assert_eq!(r.policies()["version"], 0);
    }

    #[test]
    fn oversized_or_numerous_headers_are_refused_before_any_auth() {
        let r = rig();
        let addr = r.server.as_ref().unwrap().local_addr();
        let big = "a".repeat(1024 * 1024);
        let resp = raw(addr, &format!("GET /admin/v1/info HTTP/1.1\r\nHost: 127.0.0.1\r\nX-A: {big}\r\nConnection: close\r\n\r\n"));
        assert!(resp.starts_with("HTTP/1.1 431"), "{}", &resp[..resp.len().min(80)]);
        let many: String = (0..200).map(|i| format!("X-{i}: v\r\n")).collect();
        let resp = raw(addr, &format!("GET /admin/v1/info HTTP/1.1\r\nHost: 127.0.0.1\r\n{many}Connection: close\r\n\r\n"));
        assert!(resp.starts_with("HTTP/1.1 431"), "{}", &resp[..resp.len().min(80)]);
        // Ordinary requests are unaffected.
        assert_eq!(r.call("GET", "/admin/v1/info", &[], None).0, 200);
    }

    #[test]
    fn a_change_to_one_bucket_overrides_only_that_field_and_the_rest_follows_the_file() {
        let r = rig();
        let mut doc = r.effective();
        doc.retention[1].retention_secs += 60;
        let (_, v) = r.put(0, &doc, false);
        assert_eq!(v["sources"]["retention[1].retention_secs"], "runtime");
        for k in ["retention[0].retention_secs", "retention[1].min_severity", "retention[1].name", "retention[2].retention_secs"] {
            assert_eq!(v["sources"][k], "file", "{k}");
        }
        // The file later edits the untouched bucket: it reaches the process.
        let mut cfg = r.config.clone();
        let mut b: Vec<_> = cfg.retention.iter().cloned().collect();
        b[2].retention = Duration::from_secs(99 * 86_400);
        cfg.retention = crate::config::LevelBuckets::new(b).unwrap();
        let start = startup(&cfg).unwrap();
        assert_eq!(start.effective.retention[2].retention_secs, 99 * 86_400);
        assert_eq!(start.effective.retention[1].retention_secs, doc.retention[1].retention_secs);
    }

    #[test]
    fn a_pending_restart_value_reports_what_the_process_still_runs() {
        let r = rig();
        let mut doc = r.effective();
        doc.ingest.queue_capacity = 2048;
        let (_, v) = r.put(0, &doc, false);
        assert_eq!(v["effective"]["ingest"]["queue_capacity"], 2048);
        assert_eq!(v["running"]["ingest.queue_capacity"], 65_536);
    }

    // ---- peer sync ----------------------------------------------------------------------

    fn rig_with_sync() -> Rig {
        let r = rig();
        let control = crate::sync::SyncControl::new("edge".into(), crate::sync::SyncStore::new(r.tmp.path()), None);
        r.service.set_sync(control);
        r
    }

    fn start_peer(tokens: Vec<String>) -> crate::peer::Receiver {
        crate::peer::Receiver::start(
            &crate::peer::PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), tokens, ..Default::default() },
            "central".into(),
            |_| crate::peer::Admitted::Accepted,
        )
        .unwrap()
    }

    #[test]
    fn sync_reports_the_peer_receivers_wal_waits_and_503s_when_it_has_one() {
        let r = rig_with_sync();
        assert!(r.call("GET", "/admin/v1/sync", &[], None).1["receiver"].is_null(), "no receiver in this process");
        let stats = Arc::new(crate::peer::PeerStats::default());
        stats.wal_timeouts.store(3, Ordering::Relaxed);
        stats.wal_wait_us_max.store(250_000, Ordering::Relaxed);
        r.service.set_peer_stats(Arc::clone(&stats));
        let (_, v) = r.call("GET", "/admin/v1/sync", &[], None);
        assert_eq!((3, 250_000), (v["receiver"]["wal_timeouts"].as_u64().unwrap(), v["receiver"]["wal_wait_us_max"].as_u64().unwrap()), "{v}");
    }

    #[test]
    fn sync_is_off_until_a_peer_is_set_and_without_the_controls_it_is_404() {
        assert_eq!(404, rig().call("GET", "/admin/v1/sync", &[], None).0);
        let r = rig_with_sync();
        let (status, v) = r.call("GET", "/admin/v1/sync", &[], None);
        assert_eq!(200, status);
        assert_eq!(("none", "off"), (v["source"].as_str().unwrap(), v["status"]["state"].as_str().unwrap()));
        assert_eq!("edge", v["instance_id"]);
    }

    #[test]
    fn setting_a_peer_applies_at_once_is_kept_and_never_returns_the_token() {
        let r = rig_with_sync();
        let (status, v) = r.call(
            "PUT",
            "/admin/v1/sync",
            &[],
            Some(json!({"enabled": true, "url": "https://central.example.com:9300/ignored/path", "token": "SENTINEL-TOKEN", "min_severity": 13})),
        );
        assert_eq!(200, status, "{v}");
        assert!(!v.to_string().contains("SENTINEL-TOKEN") && !v.to_string().contains("ignored"), "{v}");
        assert_eq!("admin", v["source"]);
        assert_eq!("https://central.example.com:9300", v["status"]["peer"]);
        assert_eq!((true, 13), (v["status"]["has_token"].as_bool().unwrap(), v["status"]["min_severity"].as_u64().unwrap()));
        let saved = r.service.sync.get().unwrap().store().load().unwrap();
        assert_eq!(("https://central.example.com:9300/ignored/path".to_string(), Some("SENTINEL-TOKEN".to_string())), (saved.0.url, saved.1));
        // Saving again without a token keeps the stored one.
        let (_, v) = r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": true, "url": "https://central.example.com:9300"})));
        assert!(v["status"]["has_token"].as_bool().unwrap());
        // Turning it off keeps the settings but stops sending.
        let (_, v) = r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": false, "url": "https://central.example.com:9300"})));
        assert_eq!("off", v["status"]["state"]);
        // No URL in the body keeps the saved one: the API never returns it, so a client cannot resend it.
        let (status, v) = r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": true, "min_severity": 17})));
        assert_eq!(200, status, "{v}");
        assert_eq!(("https://central.example.com:9300", 17), (v["status"]["peer"].as_str().unwrap(), v["status"]["min_severity"].as_u64().unwrap()));
        assert_eq!("https://central.example.com:9300", r.service.sync.get().unwrap().store().load().unwrap().0.url);
        // An empty URL clears it.
        let (_, v) = r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": false, "url": ""})));
        assert!(v["status"]["peer"].is_null());
    }

    #[test]
    fn a_bad_peer_url_or_severity_is_a_422_that_says_why() {
        let r = rig_with_sync();
        for body in [
            json!({"enabled": true, "url": ""}),
            json!({"enabled": true, "url": "ftp://x"}),
            json!({"enabled": true, "url": "https://u:p@x"}),
            json!({"enabled": true, "url": "https://x", "min_severity": 99}),
        ] {
            let (status, v) = r.call("PUT", "/admin/v1/sync", &[], Some(body.clone()));
            assert_eq!(422, status, "{body}: {v}");
            assert!(v["message"].is_string(), "{v}");
        }
        assert_eq!(400, r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": true, "bogus": 1}))).0);
    }

    #[test]
    fn the_test_endpoint_reads_a_real_peer_and_explains_a_wrong_token() {
        let r = rig_with_sync();
        let peer = start_peer(vec!["good".into()]);
        let url = format!("http://{}", peer.local_addr());
        let (status, v) = r.call("POST", "/admin/v1/sync/test", &[], Some(json!({"url": url, "token": "good"})));
        assert_eq!(200, status);
        assert_eq!(true, v["ok"], "{v}");
        assert_eq!("central", v["result"]["capabilities"]["instance_id"]);
        assert_eq!(true, v["result"]["accepts_native"]);
        let (_, v) = r.call("POST", "/admin/v1/sync/test", &[], Some(json!({"url": url, "token": "bad"})));
        assert_eq!(false, v["ok"]);
        assert!(v["error"].as_str().unwrap().contains("refused"), "{v}");
        let (_, v) = r.call("POST", "/admin/v1/sync/test", &[], Some(json!({"url": "http://127.0.0.1:1"})));
        assert_eq!(false, v["ok"]);
        assert_eq!(422, r.call("POST", "/admin/v1/sync/test", &[], None).0, "no URL and no current peer");
        peer.shutdown();
    }

    #[test]
    fn a_saved_token_is_never_sent_to_a_different_address() {
        let r = rig_with_sync();
        let good = start_peer(vec!["tok".into()]);
        let good_url = format!("http://{}", good.local_addr());
        r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": true, "url": good_url, "token": "tok"})));
        // The current URL, no token given: the saved one is used and the peer accepts it.
        assert_eq!(true, r.call("POST", "/admin/v1/sync/test", &[], Some(json!({}))).1["ok"]);
        // A different address: the saved token must not go with it, so the peer (which wants one) says so.
        let other = start_peer(vec!["tok".into()]);
        let (_, v) = r.call("POST", "/admin/v1/sync/test", &[], Some(json!({"url": format!("http://{}", other.local_addr())})));
        assert_eq!(false, v["ok"]);
        assert!(v["error"].as_str().unwrap().contains("wants a token"), "{v}");
        good.shutdown();
        other.shutdown();
    }

    #[test]
    fn the_detail_dial_and_anomaly_boost_are_validated_kept_and_reported() {
        let r = rig_with_sync();
        let (status, v) = r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": true, "url": "https://central.example.com:9300", "fidelity": 5, "anomaly_boost": false})));
        assert_eq!(200, status, "{v}");
        assert_eq!((5, false), (v["status"]["fidelity"].as_u64().unwrap(), v["status"]["anomaly_boost"].as_bool().unwrap()));
        // Left out of a later save, both are kept.
        let (_, v) = r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": true, "min_severity": 9})));
        assert_eq!((5, false), (v["status"]["fidelity"].as_u64().unwrap(), v["status"]["anomaly_boost"].as_bool().unwrap()));
        let saved = r.service.sync.get().unwrap().store().load().unwrap().0;
        assert_eq!((5, false), (saved.fidelity, saved.anomaly_boost));
        for bad in [0, 101, 255] {
            let (status, v) = r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": true, "fidelity": bad})));
            assert_eq!(422, status, "{bad}: {v}");
            assert!(v["message"].as_str().unwrap().contains("1 to 100"));
        }
    }

    #[test]
    fn a_fetch_needs_a_peer_a_valid_range_and_can_be_cancelled() {
        let r = rig_with_sync();
        let (status, v) = r.call("POST", "/admin/v1/sync/backfill", &[], Some(json!({"from_ms": 0, "to_ms": 10})));
        assert_eq!(409, status);
        assert!(v["message"].as_str().unwrap().contains("Set a peer"), "{v}");
        r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": true, "url": "https://central.example.com:9300"})));
        let (status, v) = r.call("POST", "/admin/v1/sync/backfill", &[], Some(json!({"from_ms": 10, "to_ms": 10})));
        assert_eq!(409, status, "{v}");
        assert_eq!(400, r.call("POST", "/admin/v1/sync/backfill", &[], Some(json!({"from_ms": 1}))).0);
        assert_eq!(200, r.call("POST", "/admin/v1/sync/backfill/cancel", &[], None).0);
        assert_eq!(405, r.call("GET", "/admin/v1/sync/backfill", &[], None).0);
    }

    #[test]
    fn a_replication_run_is_started_and_read_through_the_admin_api() {
        let r = rig();
        let (status, v) = r.call("POST", "/admin/v1/parts/replicate", &[], None);
        assert_eq!((409, "parts_disabled"), (status, v["error"].as_str().unwrap()), "{v}");
        let parts = |replicate| {
            let config = crate::parts::PartsConfig { enabled: true, replicate, ..crate::parts::PartsConfig::default() };
            Arc::new(crate::parts::PartsCentral::new(config, r.config.catalog_path()).unwrap())
        };
        // Not a replica: there is nothing to start.
        r.service.set_parts(parts(None));
        let (status, v) = r.call("POST", "/admin/v1/parts/replicate", &[], None);
        assert_eq!((409, "not_a_replica"), (status, v["error"].as_str().unwrap()), "{v}");
        assert_eq!(405, r.call("PUT", "/admin/v1/parts/replicate", &[], None).0);
    }

    #[test]
    fn a_replica_starts_a_run_on_request_and_reports_where_it_got_to() {
        let r = rig();
        // Nothing listens on port 1, so the run ends at once with the reason.
        let replicate = crate::parts::ReplicateConfig { url: "http://127.0.0.1:1".into(), token: "t".into(), interval_secs: 0, bytes_per_sec: 0, page_size: 10 };
        let config = crate::parts::PartsConfig { enabled: true, replicate: Some(replicate), ..crate::parts::PartsConfig::default() };
        let central = Arc::new(crate::parts::PartsCentral::new(config, r.config.catalog_path()).unwrap());
        r.service.set_parts(Arc::clone(&central));
        let (status, v) = r.call("GET", "/admin/v1/parts/replicate", &[], None);
        assert_eq!((200, "idle", 0), (status, v["replication"]["state"].as_str().unwrap(), v["replication"]["runs"].as_u64().unwrap()), "{v}");
        assert_eq!("http://127.0.0.1:1", v["replication"]["source"]);
        assert!(!v.to_string().contains("\"t\""), "the token is never shown");
        let (status, v) = r.call("POST", "/admin/v1/parts/replicate", &[], None);
        assert_eq!(202, status, "{v}");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let v = loop {
            let (_, v) = r.call("GET", "/admin/v1/parts/replicate", &[], None);
            if v["replication"]["state"] == "idle" && v["replication"]["runs"] == 1 {
                break v;
            }
            assert!(std::time::Instant::now() < deadline, "{v}");
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        assert_eq!(false, v["replication"]["run"]["complete"]);
        assert!(v["replication"]["run"]["error"].as_str().unwrap().contains("could not be reached"), "{v}");
        central.stop_replication();
    }

    #[test]
    fn the_part_catalog_lists_by_node_tier_and_time_and_the_sweep_needs_parts_enabled() {
        let r = rig();
        {
            let mut cat = Catalog::open(&r.config.catalog_path()).unwrap();
            let part = |tag: char, lo_ms: u64, hi_ms: u64| crate::parts::CommittedPart {
                name: format!("{}.parquet", tag.to_string().repeat(64)),
                size: 10,
                sha256: tag.to_string().repeat(64),
                rows: 3,
                min_unix_nano: lo_ms * 1_000_000,
                max_unix_nano: hi_ms * 1_000_000,
                origin: "inst".into(),
                node: None,
            };
            cat.record_peer_part("edge-a", &part('a', 100, 200), 1).unwrap();
            cat.record_peer_part("edge-a", &part('b', 300, 400), 1).unwrap();
            cat.record_peer_part("edge-b", &part('c', 150, 350), 1).unwrap();
            assert!(cat.mark_peer_part_cold("edge-a", &part('a', 0, 0).name, 9).unwrap());
        }
        let names = |q: &str| -> Vec<String> {
            let (status, v) = r.call("GET", &format!("/admin/v1/parts{q}"), &[], None);
            assert_eq!(200, status, "{q}: {v}");
            assert_eq!(v["count"], v["parts"].as_array().unwrap().len());
            v["parts"].as_array().unwrap().iter().map(|p| format!("{}/{}/{}", p["node"].as_str().unwrap(), p["tier"].as_str().unwrap(), &p["name"].as_str().unwrap()[..1])).collect()
        };
        assert_eq!(vec!["edge-a/cold/a", "edge-b/warm/c", "edge-a/warm/b"], names(""));
        assert_eq!(vec!["edge-a/cold/a", "edge-a/warm/b"], names("?node=edge-a"));
        assert_eq!(2, names("?tier=warm").len());
        assert_eq!(vec!["edge-a/cold/a"], names("?tier=cold"));
        assert_eq!(vec!["edge-b/warm/c", "edge-a/warm/b"], names("?from_ms=250&to_ms=320"));
        assert_eq!(1, names("?limit=1").len());
        assert_eq!(vec!["edge-b/warm/c"], names("?node=edge-b&tier=warm&from_ms=0&to_ms=1000"));
        for bad in ["?tier=hot", "?tier=lukewarm", "?node=../x", "?node=", "?from_ms=x", "?limit=0", "?limit=1001", "?color=red"] {
            let (status, v) = r.call("GET", &format!("/admin/v1/parts{bad}"), &[], None);
            assert_eq!((400, "bad_query"), (status, v["error"].as_str().unwrap()), "{bad}: {v}");
        }
        let (status, v) = r.call("GET", "/admin/v1/parts/audit", &[], None);
        assert_eq!((200, 1), (status, v["audit"].as_array().unwrap().len()), "{v}");
        assert_eq!("tier", v["audit"][0]["action"]);
        let (status, v) = r.call("POST", "/admin/v1/parts/sweep", &[], Some(json!({"dry_run": false})));
        assert_eq!((409, "parts_disabled"), (status, v["error"].as_str().unwrap()), "{v}");
        assert_eq!(405, r.call("POST", "/admin/v1/parts", &[], None).0);
    }

    #[test]
    fn a_part_upload_needs_a_peer_a_valid_range_one_at_a_time_and_can_be_cancelled() {
        let r = rig_with_sync();
        let (status, v) = r.call("POST", "/admin/v1/sync/parts", &[], Some(json!({"from_ms": 0, "to_ms": 10})));
        assert_eq!(409, status);
        assert!(v["message"].as_str().unwrap().contains("Set a peer"), "{v}");
        r.service.sync.get().unwrap().set_store_dir(r.config.store_dir());
        // A peer that takes the connection and never answers keeps the first job busy probing it.
        let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        r.call("PUT", "/admin/v1/sync", &[], Some(json!({"enabled": true, "url": format!("http://{}", silent.local_addr().unwrap())})));
        assert_eq!(409, r.call("POST", "/admin/v1/sync/parts", &[], Some(json!({"from_ms": 10, "to_ms": 10}))).0);
        let (status, v) = r.call("POST", "/admin/v1/sync/parts", &[], Some(json!({"from_ms": 0, "to_ms": 40u64 * 24 * 3_600_000})));
        assert_eq!(409, status);
        assert!(v["message"].as_str().unwrap().contains("31 days"), "{v}");
        assert_eq!(400, r.call("POST", "/admin/v1/sync/parts", &[], Some(json!({"from_ms": 1}))).0);
        let (status, v) = r.call("POST", "/admin/v1/sync/parts", &[], Some(json!({"from_ms": 0, "to_ms": 3_600_000})));
        assert_eq!(200, status, "{v}");
        assert_eq!("running", v["status"]["parts"]["state"], "{v}");
        let (status, v) = r.call("POST", "/admin/v1/sync/parts", &[], Some(json!({"from_ms": 0, "to_ms": 3_600_000})));
        assert_eq!(409, status);
        assert!(v["message"].as_str().unwrap().contains("already running"), "{v}");
        assert_eq!(200, r.call("POST", "/admin/v1/sync/parts/cancel", &[], None).0);
        assert_eq!(405, r.call("GET", "/admin/v1/sync/parts", &[], None).0);
    }

    /// A decision model that answers "same" when both templates mention redis, over real HTTP.
    fn fake_decision_model() -> (String, std::thread::JoinHandle<()>, Arc<AtomicU64>) {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/decide", listener.local_addr().unwrap());
        let calls = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&calls);
        let handle = std::thread::spawn(move || {
            listener.set_nonblocking(false).unwrap();
            for stream in listener.incoming().take(40) {
                let Ok(stream) = stream else { return };
                let mut out = stream.try_clone().unwrap();
                let mut input = BufReader::new(stream);
                let (mut length, mut auth) = (0usize, String::new());
                loop {
                    let mut line = String::new();
                    if input.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(v) = lower.strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                    if let Some(v) = lower.strip_prefix("authorization:") {
                        auth = v.trim().to_string();
                    }
                }
                let mut body = vec![0u8; length];
                input.read_exact(&mut body).unwrap();
                counted.fetch_add(1, Ordering::SeqCst);
                let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let state = v["state"].as_str().unwrap().to_string();
                let p = if auth == "bearer model-token" && state.matches("redis").count() >= 2 { 0.92 } else { 0.08 };
                let answer = json!({"answers": {"q": {"type": "choice", "choice": "same", "probabilities": {"same": p, "different": 1.0 - p}}}}).to_string();
                let _ = write!(out, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}", answer.len());
            }
        });
        (url, handle, calls)
    }

    #[test]
    fn merge_suggestions_are_404_without_a_model_and_come_from_the_configured_one_with_it() {
        assert_eq!(404, rig().call("GET", "/admin/v1/templates/merge-suggestions", &[], None).0);

        let (url, _server, calls) = fake_decision_model();
        let r = rig_with(Arc::new(now_unix_secs), |c| {
            c.decision = crate::decision::DecisionConfig { url: Some(url), token: Some("model-token".into()), ..Default::default() };
        });
        let mut catalog = Catalog::open(&r.config.catalog_path()).unwrap();
        let t = |id: u64, text: &str, count: u64| crate::drain::Template { id, tokens: text.split(' ').map(str::to_string).collect(), count, first_seen_unix_secs: 0, last_seen_unix_secs: 0, lineage_id: id };
        catalog
            .save_templates(&[
                t(1, "redis timeout after <*> ms on GET session", 900),
                t(2, "redis exceptions timeout reading from redis session", 40),
                t(3, "postgres timeout after <*> ms on SELECT orders", 70),
            ])
            .unwrap();
        let (status, v) = r.call("GET", "/admin/v1/templates/merge-suggestions", &[], None);
        assert_eq!(200, status, "{v}");
        assert_eq!("decision-model", v["source"]);
        let found = v["report"]["suggestions"].as_array().unwrap();
        assert_eq!(1, found.len(), "{v}");
        assert_eq!((1, 2), (found[0]["a"]["id"].as_u64().unwrap(), found[0]["b"]["id"].as_u64().unwrap()));
        assert_eq!(900, found[0]["a"]["count"]);
        assert!(found[0]["p_same"].as_f64().unwrap() >= 0.9);
        assert!(calls.load(Ordering::SeqCst) >= 1, "the model was asked over HTTP, with the bearer token");
        // It needs the admin token like every other route.
        assert_eq!(401, r.agent.get(&format!("{}/admin/v1/templates/merge-suggestions", r.base)).call().unwrap().status().as_u16());
    }

    // --- hostile input ---------------------------------------------------

    fn status_of(resp: &str) -> u16 {
        resp.split(' ').nth(1).and_then(|s| s.parse().ok()).unwrap_or(0)
    }

    fn admin_raw(r: &Rig, request_line: &str, headers: &str, body: &[u8]) -> String {
        let addr = r.server.as_ref().unwrap().local_addr();
        let req = format!("{request_line}\r\nHost: 127.0.0.1\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n", body.len());
        let mut conn = std::net::TcpStream::connect(addr).unwrap();
        conn.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let _ = conn.write_all(&[req.as_bytes(), body].concat());
        let mut out = Vec::new();
        let _ = conn.read_to_end(&mut out);
        String::from_utf8_lossy(&out).into_owned()
    }

    fn authed() -> String {
        format!("Authorization: Bearer {TOKEN}\r\n")
    }

    #[test]
    fn an_oversized_body_is_413_declared_or_sent_and_changes_nothing() {
        let r = rig();
        let addr = r.server.as_ref().unwrap().local_addr();
        let big = vec![b' '; MAX_BODY_BYTES + 1];
        let resp = admin_raw(&r, "PUT /admin/v1/policies HTTP/1.1", &format!("{}If-Match: \"0\"\r\n", authed()), &big);
        assert_eq!(413, status_of(&resp), "{}", &resp[..resp.len().min(120)]);
        // Declared and never sent: answered from the head, not after the request timeout.
        let started = std::time::Instant::now();
        let resp = raw(addr, &format!("PUT /admin/v1/policies HTTP/1.1\r\nHost: 127.0.0.1\r\n{}Content-Length: 1099511627776\r\nConnection: close\r\n\r\n", authed()));
        assert_eq!(413, status_of(&resp), "{resp}");
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        // Exactly at the limit is read (and then judged on its content).
        let at = vec![b' '; MAX_BODY_BYTES];
        let resp = admin_raw(&r, "PUT /admin/v1/policies HTTP/1.1", &format!("{}If-Match: \"0\"\r\n", authed()), &at);
        assert_ne!(413, status_of(&resp));
        assert_eq!(r.policies()["version"], 0, "nothing was applied");
    }

    #[test]
    fn a_compressed_body_is_never_inflated_by_the_admin_listener() {
        let r = rig();
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        enc.write_all(&vec![b' '; 64 * 1024 * 1024]).unwrap();
        let bomb = enc.finish().unwrap();
        let resp = admin_raw(&r, "POST /admin/v1/policies/preview HTTP/1.1", &format!("{}Content-Encoding: gzip\r\n", authed()), &bomb);
        assert!((400..500).contains(&status_of(&resp)), "{}", &resp[..resp.len().min(120)]);
        assert_eq!(r.call("GET", "/admin/v1/info", &[], None).0, 200);
    }

    #[test]
    fn every_method_but_the_documented_one_is_refused_and_the_override_headers_do_nothing() {
        let r = rig();
        let paths = ["/admin/v1/info", "/admin/v1/policies", "/admin/v1/policies/preview", "/admin/v1/policies/restore", "/admin/v1/policies/history", "/admin/v1/sync", "/admin/v1/sync/backfill"];
        for path in paths {
            for method in ["DELETE", "PATCH", "TRACE", "CONNECT", "OPTIONS", "HEAD", "PURGE", "get", "Get"] {
                let with = admin_raw(&r, &format!("{method} {path} HTTP/1.1"), &authed(), b"");
                assert!(matches!(status_of(&with), 404 | 405), "{method} {path}: {}", &with[..with.len().min(80)]);
                let without = admin_raw(&r, &format!("{method} {path} HTTP/1.1"), "", b"");
                assert_eq!(401, status_of(&without), "{method} {path} without a token");
            }
        }
        // A POST cannot be turned into a PUT (which applies a change) by a header or a query parameter.
        for headers in ["X-HTTP-Method-Override: PUT\r\n", "X-HTTP-Method: PUT\r\n", "X-Method-Override: PUT\r\n"] {
            let resp = admin_raw(&r, "POST /admin/v1/policies HTTP/1.1", &format!("{}{headers}If-Match: \"0\"\r\n", authed()), b"{}");
            assert_eq!(405, status_of(&resp), "{headers}");
        }
        let resp = admin_raw(&r, "POST /admin/v1/policies?_method=PUT HTTP/1.1", &format!("{}If-Match: \"0\"\r\n", authed()), b"{}");
        assert_eq!(405, status_of(&resp));
        // A body on a GET is ignored, not parsed.
        let resp = admin_raw(&r, "GET /admin/v1/info HTTP/1.1", &authed(), b"{\"x\":1}");
        assert_eq!(200, status_of(&resp));
        assert_eq!(r.policies()["version"], 0);
    }

    #[test]
    fn only_the_exact_path_is_routed() {
        let r = rig();
        for path in [
            "/admin/v1/info/", "//admin/v1/info", "/admin//v1/info", "/admin/v1/./info", "/admin/v1/x/../info", "/ADMIN/V1/INFO",
            "/admin/v1/%69nfo", "/admin/v1/info%2f", "/admin/v1/info%00", "/admin/v1/info;x=1", "/admin/v1/info.json", "/admin/v1",
            "/admin/v1/", "/v1/info", "http://127.0.0.1/admin/v1/info", "*", "/admin/v1/info%20",
        ] {
            let resp = admin_raw(&r, &format!("GET {path} HTTP/1.1"), &authed(), b"");
            assert!(matches!(status_of(&resp), 400 | 404), "{path}: {}", &resp[..resp.len().min(80)]);
        }
        // Path matching ignores the query, and a query cannot smuggle a path.
        assert_eq!(200, status_of(&admin_raw(&r, "GET /admin/v1/info?x=/../policies HTTP/1.1", &authed(), b"")));
        assert_eq!(200, status_of(&admin_raw(&r, "GET /admin/v1/info?limit=999999999999999999999 HTTP/1.1", &authed(), b"")));
        for q in ["limit=0", "limit=-1", "limit=1001", "limit=abc", "limit=99999999999999999999999", "limit="] {
            assert_eq!(400, status_of(&admin_raw(&r, &format!("GET /admin/v1/policies/history?{q} HTTP/1.1"), &authed(), b"")), "{q}");
        }
        let huge = format!("GET /admin/v1/policies/history?{} HTTP/1.1", "a=b&".repeat(10_000));
        assert_eq!(431, status_of(&admin_raw(&r, &huge, &authed(), b"")), "a 40 kB query string is over the head budget");
    }

    #[test]
    fn a_token_has_to_arrive_in_exactly_one_authorization_header() {
        let r = rig();
        let good = authed();
        let bad = "Authorization: Bearer wrong-token-wrong-token-wrong\r\n";
        let nul_padded = format!("Authorization: Bearer {TOKEN}{}\r\n", "\0".repeat(256));
        for (name, headers, want) in [
            ("good then bad", format!("{good}{bad}"), 401),
            ("bad then good", format!("{bad}{good}"), 401),
            ("good twice", format!("{good}{good}"), 401),
            ("the token plus 256 NULs", nul_padded, 401),
            ("a 32 kB token", format!("Authorization: Bearer {}\r\n", "a".repeat(8000)), 401),
            ("token in a cookie", format!("Cookie: token={TOKEN}\r\n"), 401),
            ("token in the query only", String::new(), 401),
            ("good once", good.clone(), 200),
        ] {
            let target = if name == "token in the query only" { format!("/admin/v1/info?token={TOKEN}&access_token={TOKEN}") } else { "/admin/v1/info".to_string() };
            let resp = admin_raw(&r, &format!("GET {target} HTTP/1.1"), &headers, b"");
            assert_eq!(want, status_of(&resp), "{name}: {}", &resp[..resp.len().min(80)]);
        }
    }

    #[test]
    fn host_names_that_merely_resolve_to_loopback_are_not_loopback_names() {
        // Conservative on purpose: only the literal loopback spellings pass, so a name an attacker
        // controls cannot be a way in however its DNS is set up.
        for host in ["localhost.", "127.1", "2130706433", "0x7f.0.0.1", "[::ffff:127.0.0.1]", "127.0.0.1.nip.io", "localhost.localdomain", "0.0.0.0", "[::]", "127.0.0.2.evil.example", "xn--localhost"] {
            assert!(!host_allowed(Some(host), &[]), "{host}");
        }
        assert!(host_allowed(Some("127.0.0.2"), &[]), "any 127/8 address is loopback");
        let r = rig();
        let addr = r.server.as_ref().unwrap().local_addr();
        // Absolute-form: the request-target's host is not what is checked, the Host header is, and the
        // path is the whole target, which matches no route.
        let resp = raw(addr, &format!("GET http://localhost/admin/v1/info HTTP/1.1\r\nHost: evil.example\r\n{}Connection: close\r\n\r\n", authed()));
        assert_eq!(403, status_of(&resp));
        let resp = raw(addr, &format!("GET http://evil.example/admin/v1/info HTTP/1.1\r\nHost: 127.0.0.1\r\n{}Connection: close\r\n\r\n", authed()));
        assert_eq!(404, status_of(&resp));
        // HTTP/1.0 with no Host header is refused too, not treated as the default host.
        let resp = raw(addr, &format!("GET /admin/v1/info HTTP/1.0\r\n{}\r\n", authed()));
        assert_eq!(403, status_of(&resp));
    }

    #[test]
    fn replies_carry_no_cross_origin_grants_and_are_not_cacheable() {
        let r = rig();
        let resp = admin_raw(&r, "GET /admin/v1/policies HTTP/1.1", &format!("{}Origin: http://evil.example\r\n", authed()), b"");
        let lower = resp.to_ascii_lowercase();
        assert!(!lower.contains("access-control-"), "{resp}");
        assert!(lower.contains("cache-control: no-store"), "{resp}");
        assert!(lower.contains("connection: close"));
    }

}
