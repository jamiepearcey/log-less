//! Sentry ingest proxy.
//!
//! Your application keeps its Sentry SDK and its DSN; only the DSN's host
//! changes. Everything the SDK sends arrives here, is stored locally at full
//! fidelity, and a selected subset is forwarded upstream. That closes the gap
//! the brief describes as "we sit in front of it" — for Splunk that was already
//! true (HEC in, HEC out), and for Sentry it was not.
//!
//! # What a Sentry proxy is actually for
//!
//! It is not the log-volume problem. Sentry SDKs already send only errors, and
//! errors are not what makes a Sentry bill. Transactions are, and so are issue
//! groups with runaway cardinality. So the selectivity here is aimed at those:
//! sample transactions deterministically, keep every error, hold attachments
//! locally, and keep the full envelope on disk so any decision is reversible.
//!
//! # Upstream identity
//!
//! Two modes, because the right answer depends on how the org is organised:
//!
//! * [`UpstreamAuth::Relay`] — keep each client's key and project, change only
//!   the host. Events land in the project they always did, so quotas, alerts
//!   and ownership rules are untouched. This is the transparent option.
//! * [`UpstreamAuth::Resign`] — use one configured DSN for everything. Simpler,
//!   and it collapses per-project attribution, so the original key is preserved
//!   as a tag rather than lost.
//!
//! Per-project overrides beat both, which is what makes one proxy usable in
//! front of several upstream projects.

pub mod dsn;
pub mod envelope;
pub mod upstream;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::httpd::{self, Handler, Incoming, Limits, Method, Reject, Reply};
use crate::model::{Attr, AttrValue, LogRecord, Severity};
use crate::spool::{Spool, SpoolConfig};

pub use dsn::{Dsn, UpstreamAuth};
pub use envelope::{Envelope, EnvelopeError, Item, ItemType};
pub use upstream::{Delivery, RateLimits, Upstream, UpstreamSnapshot};

/// Attachment payloads are not copied into the store: they are arbitrary
/// binaries (minidumps, screenshots, view hierarchies) that would land in a
/// string column and dwarf every other row. Their metadata is kept so an
/// operator can see one existed.
const STORE_ATTACHMENT_BYTES: bool = false;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SentryConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Point your SDK's DSN host here: `http://<any key>@127.0.0.1:9000/<project>`.
    #[serde(default = "default_sentry_addr")]
    pub addr: String,
    #[serde(default = "default_sentry_workers")]
    pub workers: usize,
    /// How to authenticate upstream. See [`UpstreamAuth`].
    #[serde(default = "default_upstream_auth")]
    pub upstream_auth: UpstreamAuth,
    /// In `relay` mode only the host is used; in `resign` mode the whole DSN is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_dsn: Option<String>,
    /// Per-project routing, which beats `upstream_auth` for the projects listed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projects: Vec<ProjectRoute>,
    /// Client keys accepted. Empty accepts any key — fine on loopback, refused
    /// on a public bind, exactly as the HEC receiver does.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_keys: Vec<String>,
    /// Forward errors and messages upstream. Off makes the proxy a pure local
    /// recorder — useful for measuring what Sentry would have cost.
    #[serde(default = "default_true")]
    pub forward_events: bool,
    /// Fraction of transactions forwarded, 0.0–1.0. Sampling is deterministic
    /// on trace id, so every span of one trace shares its fate and a sampled
    /// trace is never half-present upstream.
    #[serde(default = "default_transaction_sample_rate")]
    pub transaction_sample_rate: f64,
    /// Forward attachments. Off by default: they are the expensive part of a
    /// Sentry bill after transactions, and they are exactly what a local store
    /// is good at holding until asked for.
    #[serde(default)]
    pub forward_attachments: bool,
    /// Forward item types this build does not recognise. On by default — a
    /// proxy that drops unknown items breaks every SDK feature newer than
    /// itself.
    #[serde(default = "default_true")]
    pub forward_unknown: bool,
    #[serde(default = "default_sentry_max_body")]
    pub max_body_bytes: usize,
    #[serde(default = "default_sentry_max_decompressed")]
    pub max_decompressed_bytes: usize,
    /// Spool undelivered envelopes to disk under `<data_dir>/sentry-spool`.
    ///
    /// On by default, because the alternative is losing envelopes an SDK was
    /// told `200` for — it has already discarded its copy, so nothing else can
    /// replace them. Turning it off makes the proxy strictly in-memory and is
    /// only reasonable when upstream delivery is best-effort.
    #[serde(default = "default_true")]
    pub spool: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectRoute {
    /// Project id as it appears in the client's DSN path.
    pub project: String,
    /// Where events for that project go, key included.
    pub dsn: String,
}

fn default_sentry_addr() -> String {
    "127.0.0.1:9000".to_string()
}
fn default_sentry_workers() -> usize {
    2
}
fn default_upstream_auth() -> UpstreamAuth {
    UpstreamAuth::Relay
}
fn default_true() -> bool {
    true
}
fn default_transaction_sample_rate() -> f64 {
    1.0
}
fn default_sentry_max_body() -> usize {
    // Sentry's own envelope limit is 100 MB compressed for attachments; 20 MB
    // is generous for everything else and bounds one request's allocation.
    20 * 1024 * 1024
}
fn default_sentry_max_decompressed() -> usize {
    100 * 1024 * 1024
}

impl Default for SentryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: default_sentry_addr(),
            workers: default_sentry_workers(),
            upstream_auth: default_upstream_auth(),
            upstream_dsn: None,
            projects: Vec::new(),
            allowed_keys: Vec::new(),
            forward_events: true,
            transaction_sample_rate: default_transaction_sample_rate(),
            forward_attachments: false,
            forward_unknown: true,
            max_body_bytes: default_sentry_max_body(),
            max_decompressed_bytes: default_sentry_max_decompressed(),
            spool: true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SentryError {
    #[error(transparent)]
    Http(#[from] httpd::HttpError),
    #[error("cannot open the sentry spool: {0}")]
    Spool(#[from] crate::spool::SpoolError),
    #[error("sentry.upstream_dsn is malformed: {dsn}")]
    BadDsn { dsn: String },
    #[error("sentry.projects[{index}].dsn is malformed: {dsn}")]
    BadProjectDsn { index: usize, dsn: String },
    #[error("sentry.upstream_auth = \"resign\" needs sentry.upstream_dsn")]
    ResignWithoutDsn,
    #[error(
        "sentry.allowed_keys is empty, which accepts any key; refusing to bind the non-loopback address {addr}"
    )]
    OpenOnPublicAddress { addr: String },
}

#[derive(Debug, Default)]
pub struct SentryStats {
    pub requests: AtomicU64,
    pub envelopes: AtomicU64,
    pub items_received: AtomicU64,
    pub items_forwarded: AtomicU64,
    pub items_held_locally: AtomicU64,
    pub transactions_sampled_out: AtomicU64,
    pub accepted: AtomicU64,
    pub unauthorized: AtomicU64,
    pub bad_request: AtomicU64,
    pub too_large: AtomicU64,
    pub busy: AtomicU64,
    pub bytes_received: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub requests: u64,
    pub envelopes: u64,
    pub items_received: u64,
    pub items_forwarded: u64,
    pub items_held_locally: u64,
    pub transactions_sampled_out: u64,
    pub accepted: u64,
    pub unauthorized: u64,
    pub bad_request: u64,
    pub too_large: u64,
    pub busy: u64,
    pub bytes_received: u64,
}

impl SentryStats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            envelopes: self.envelopes.load(Ordering::Relaxed),
            items_received: self.items_received.load(Ordering::Relaxed),
            items_forwarded: self.items_forwarded.load(Ordering::Relaxed),
            items_held_locally: self.items_held_locally.load(Ordering::Relaxed),
            transactions_sampled_out: self.transactions_sampled_out.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            unauthorized: self.unauthorized.load(Ordering::Relaxed),
            bad_request: self.bad_request.load(Ordering::Relaxed),
            too_large: self.too_large.load(Ordering::Relaxed),
            busy: self.busy.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

impl StatsSnapshot {
    /// Every request took exactly one exit.
    pub fn accounts_for_everything(&self) -> bool {
        self.requests
            == self.accepted + self.unauthorized + self.bad_request + self.too_large + self.busy
    }
}

/// What the local pipeline did with a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admitted {
    Accepted,
    /// Answered `429` with `Retry-After`, so the SDK's own queue holds it.
    Busy,
}

/// One envelope on its way upstream.
pub struct Outbound {
    pub target: dsn::UpstreamTarget,
    pub envelope: Envelope,
}

impl Outbound {
    /// Spool encoding: the target on one line, then the envelope's own bytes.
    /// The envelope is stored as it will be sent, so a replay after a restart
    /// forwards exactly what the SDK produced rather than a re-derivation.
    fn encode(&self) -> Vec<u8> {
        let mut out = serde_json::json!({
            "url": self.target.url,
            "key": self.target.key,
            "project": self.target.project,
        })
        .to_string()
        .into_bytes();
        out.push(b'\n');
        out.extend_from_slice(&self.envelope.to_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let split = bytes.iter().position(|b| *b == b'\n')?;
        let header: serde_json::Value = serde_json::from_slice(&bytes[..split]).ok()?;
        let envelope = Envelope::parse(&bytes[split + 1..]).ok()?;
        Some(Self {
            target: dsn::UpstreamTarget {
                url: header.get("url")?.as_str()?.to_string(),
                key: header.get("key")?.as_str()?.to_string(),
                project: header.get("project")?.as_str()?.to_string(),
            },
            envelope,
        })
    }
}

struct Routes {
    mode: UpstreamAuth,
    configured: Option<Dsn>,
    overrides: Vec<(String, Dsn)>,
}

impl Routes {
    fn resolve(&self, key: &str, project: &str) -> Option<dsn::UpstreamTarget> {
        let over = self
            .overrides
            .iter()
            .find(|(id, _)| id == project)
            .map(|(_, d)| d);
        dsn::resolve(self.mode, key, project, self.configured.as_ref(), self.configured.as_ref(), over)
    }
}

struct SentryHandler {
    config: SentryConfig,
    routes: Routes,
    stats: Arc<SentryStats>,
    limits: Arc<RateLimits>,
    sink: Box<dyn Fn(Vec<LogRecord>) -> Admitted + Send + Sync>,
    /// Durable queue. Written and fsynced *before* the SDK is answered `200`,
    /// because after that answer the SDK discards its only copy.
    spool: Option<Arc<std::sync::Mutex<Spool>>>,
    /// Nudges the forwarder; the spool is the source of truth, so a missed
    /// wake-up costs latency and never data.
    wake: Option<crossbeam_channel::Sender<()>>,
}

impl SentryHandler {
    /// `/api/<project>/envelope/` or `/api/<project>/store/`.
    fn route(path: &str) -> Option<(&str, Endpoint)> {
        let rest = path.strip_prefix("/api/")?;
        let (project, tail) = rest.split_once('/')?;
        if project.is_empty() {
            return None;
        }
        let endpoint = match tail.trim_end_matches('/') {
            "envelope" => Endpoint::Envelope,
            "store" => Endpoint::Store,
            _ => return None,
        };
        Some((project, endpoint))
    }

    fn client_key(&self, request: &Incoming) -> Option<String> {
        request
            .header("x-sentry-auth")
            .and_then(dsn::parse_auth_header)
            .or_else(|| {
                request.header("authorization").and_then(dsn::parse_auth_header)
            })
            .map(|auth| auth.key)
            .or_else(|| {
                envelope_query_key(request.query)
            })
    }

    fn authorize(&self, key: Option<&str>) -> Result<String, Reply> {
        let Some(key) = key else {
            self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
            return Err(error_reply(401, "missing sentry key"));
        };
        if !self.config.allowed_keys.is_empty()
            && !self.config.allowed_keys.iter().any(|k| k == key)
        {
            self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
            return Err(error_reply(401, "unknown sentry key"));
        }
        Ok(key.to_string())
    }

    fn ingest(&self, request: &Incoming, project: &str, endpoint: Endpoint) -> Reply {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        let key = match self.authorize(self.client_key(request).as_deref()) {
            Ok(k) => k,
            Err(reply) => return reply,
        };

        // Reflect an active upstream limit straight back to the SDK. Its own
        // backoff then behaves exactly as it would without a proxy in the way.
        let now = crate::now_unix_secs();
        if let Some(header) = self.limits.client_header(now) {
            self.stats.busy.fetch_add(1, Ordering::Relaxed);
            return error_reply(429, "rate limited upstream")
                .with_header("X-Sentry-Rate-Limits", &header)
                .with_header("Retry-After", "60");
        }

        let parsed = match endpoint {
            Endpoint::Envelope => Envelope::parse(&request.body),
            // The legacy endpoint posts a bare event; wrapping it means one
            // code path from here on, and it goes upstream as a modern
            // envelope regardless of how old the SDK is.
            Endpoint::Store => Ok(wrap_store_payload(&request.body)),
        };
        let mut env = match parsed {
            Ok(e) => e,
            Err(e) => {
                self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                return error_reply(400, &format!("malformed envelope: {e}"));
            }
        };

        self.stats.envelopes.fetch_add(1, Ordering::Relaxed);
        self.stats.items_received.fetch_add(env.items.len() as u64, Ordering::Relaxed);
        self.stats.bytes_received.fetch_add(request.body.len() as u64, Ordering::Relaxed);

        // Everything is stored, whatever the forwarding policy decides. The
        // local copy is the point: a forwarding decision stays reversible.
        let records = self.to_records(&env, project, &key);
        if self.sink(records) == Admitted::Busy {
            self.stats.busy.fetch_add(1, Ordering::Relaxed);
            return error_reply(429, "pipeline full").with_header("Retry-After", "1");
        }

        let event_id = env.event_id().unwrap_or_else(new_event_id);
        let before = env.items.len();
        let sampled_out = self.apply_policy(&mut env, &event_id);
        self.stats.transactions_sampled_out.fetch_add(sampled_out, Ordering::Relaxed);
        let forwarded = env.items.len() as u64;
        self.stats.items_forwarded.fetch_add(forwarded, Ordering::Relaxed);
        self.stats
            .items_held_locally
            .fetch_add(before as u64 - forwarded, Ordering::Relaxed);

        if !env.is_empty() {
            match self.routes.resolve(&key, project) {
                Some(target) => {
                    let outbound = Outbound { target, envelope: env };
                    if let Some(spool) = &self.spool {
                        // Durable before the 200. Costs one fsync per envelope,
                        // which a Sentry SDK — already asynchronous — will not
                        // notice, and which is the whole difference between an
                        // acknowledgement and a guess.
                        let mut spool = spool.lock().unwrap_or_else(|p| p.into_inner());
                        match spool.push(&outbound.encode()) {
                            Ok(true) => {
                                if let Err(e) = spool.sync() {
                                    tracing::warn!(error = %e, "sentry spool sync failed");
                                }
                            }
                            Ok(false) => tracing::warn!("sentry spool full; envelope not queued"),
                            Err(e) => tracing::warn!(error = %e, "sentry spool write failed"),
                        }
                    }
                    if let Some(wake) = &self.wake {
                        let _ = wake.try_send(());
                    }
                }
                None => tracing::warn!(project, "no upstream route; envelope held locally only"),
            }
        }

        self.stats.accepted.fetch_add(1, Ordering::Relaxed);
        Reply::json(200, format!(r#"{{"id":"{event_id}"}}"#))
    }

    fn sink(&self, records: Vec<LogRecord>) -> Admitted {
        if records.is_empty() {
            return Admitted::Accepted;
        }
        (self.sink)(records)
    }

    /// Applies the forwarding policy in place, returning how many transactions
    /// were sampled out (as opposed to dropped by another rule).
    fn apply_policy(&self, env: &mut Envelope, event_id: &str) -> u64 {
        let mut sampled_out = 0u64;
        let rate = self.config.transaction_sample_rate.clamp(0.0, 1.0);
        let config = &self.config;
        // Deterministic on the envelope's own id, so a trace's items share one
        // verdict and an upstream trace is never half-present.
        let keep_transaction = sample_keep(event_id, rate);
        env.retain_items(|item| match item.item_type {
            ItemType::Event => config.forward_events,
            ItemType::Transaction | ItemType::Profile => {
                if keep_transaction {
                    true
                } else {
                    sampled_out += 1;
                    false
                }
            }
            ItemType::Attachment | ItemType::ReplayRecording => config.forward_attachments,
            // Sessions and client reports are tiny and Sentry needs them for
            // release health and loss accounting; dropping them silently
            // corrupts numbers an operator will later trust.
            ItemType::Session | ItemType::Sessions | ItemType::ClientReport => true,
            ItemType::Other => config.forward_unknown,
            _ => true,
        });
        sampled_out
    }

    fn to_records(&self, env: &Envelope, project: &str, key: &str) -> Vec<LogRecord> {
        let now = crate::now_unix_nanos();
        env.items
            .iter()
            .map(|item| item_to_record(item, project, key, env, now))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    Envelope,
    Store,
}

impl Handler for SentryHandler {
    fn handle(&self, request: Incoming) -> Reply {
        match (request.method, SentryHandler::route(request.path)) {
            (Method::Post, Some((project, endpoint))) => {
                let project = project.to_string();
                self.ingest(&request, &project, endpoint)
            }
            (Method::Post, None) => error_reply(404, "not a Sentry ingest endpoint"),
            _ => error_reply(405, "only POST is supported"),
        }
    }

    fn reject(&self, reason: Reject) -> Reply {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        match reason {
            Reject::TooLarge => {
                self.stats.too_large.fetch_add(1, Ordering::Relaxed);
                error_reply(413, "envelope exceeds max_body_bytes")
            }
            Reject::Unreadable(e) => {
                self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                error_reply(400, &format!("cannot read body: {e}"))
            }
        }
    }
}

pub struct Receiver {
    server: httpd::Server,
    stats: Arc<SentryStats>,
    forwarder: Option<std::thread::JoinHandle<()>>,
    wake: Option<crossbeam_channel::Sender<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    upstream_stats: Arc<upstream::UpstreamStats>,
}

impl Receiver {
    /// Binds and starts serving. `upstream` is `None` for a pure local
    /// recorder, which is how you measure what Sentry would have cost before
    /// changing anything about what it receives.
    pub fn start<F>(
        config: &SentryConfig,
        upstream: Option<Upstream>,
        spool_dir: Option<&std::path::Path>,
        sink: F,
    ) -> Result<Self, SentryError>
    where
        F: Fn(Vec<LogRecord>) -> Admitted + Send + Sync + 'static,
    {
        if config.allowed_keys.is_empty() && !is_loopback(&config.addr) {
            return Err(SentryError::OpenOnPublicAddress { addr: config.addr.clone() });
        }
        let configured = match &config.upstream_dsn {
            Some(raw) => {
                Some(Dsn::parse(raw).ok_or_else(|| SentryError::BadDsn { dsn: raw.clone() })?)
            }
            None => None,
        };
        if config.upstream_auth == UpstreamAuth::Resign && configured.is_none() {
            return Err(SentryError::ResignWithoutDsn);
        }
        let mut overrides = Vec::with_capacity(config.projects.len());
        for (index, route) in config.projects.iter().enumerate() {
            let parsed = Dsn::parse(&route.dsn)
                .ok_or_else(|| SentryError::BadProjectDsn { index, dsn: route.dsn.clone() })?;
            overrides.push((route.project.clone(), parsed));
        }

        let stats = Arc::new(SentryStats::default());
        let limits = upstream
            .as_ref()
            .map(|u| Arc::clone(&u.limits))
            .unwrap_or_else(|| Arc::new(RateLimits::default()));
        let upstream_stats = upstream
            .as_ref()
            .map(|u| Arc::clone(&u.stats))
            .unwrap_or_default();

        let spool = match (config.spool, spool_dir, upstream.is_some()) {
            (true, Some(dir), true) => Some(Arc::new(std::sync::Mutex::new(
                Spool::open(dir, SpoolConfig::default())?,
            ))),
            _ => None,
        };
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let (wake, forwarder) = match upstream {
            Some(upstream) => {
                let (tx, rx) = crossbeam_channel::bounded::<()>(1024);
                let spool = spool.clone();
                let stop = Arc::clone(&stop);
                let handle = std::thread::Builder::new()
                    .name("sentry-proxy".into())
                    .spawn(move || {
                        forward_loop(&upstream, spool.as_deref(), &rx, &stop);
                    })
                    .expect("spawn sentry forwarder");
                (Some(tx), Some(handle))
            }
            None => (None, None),
        };

        let handler = Arc::new(SentryHandler {
            config: config.clone(),
            routes: Routes {
                mode: config.upstream_auth,
                configured,
                overrides,
            },
            stats: Arc::clone(&stats),
            limits,
            sink: Box::new(sink),
            spool: spool.clone(),
            wake: wake.clone(),
        });

        let server = httpd::Server::start(
            &config.addr,
            config.workers,
            "sentry",
            Limits {
                max_body_bytes: config.max_body_bytes,
                max_decompressed_bytes: config.max_decompressed_bytes,
            },
            handler,
        )?;

        Ok(Self { server, stats, forwarder, wake, stop, upstream_stats })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn upstream_stats(&self) -> UpstreamSnapshot {
        self.upstream_stats.snapshot()
    }

    /// Stops accepting, then drains whatever is still queued for upstream — an
    /// envelope already acknowledged to an SDK must not disappear because the
    /// agent restarted.
    pub fn shutdown(mut self) -> StatsSnapshot {
        self.server.shutdown();
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        drop(self.wake.take());
        if let Some(handle) = self.forwarder.take() {
            let _ = handle.join();
        }
        self.stats.snapshot()
    }
}

/// Drains the spool until told to stop, then makes one last attempt.
///
/// Delivery is committed only after upstream accepts, so a crash mid-send
/// re-delivers rather than dropping. Sentry deduplicates on `event_id`, which
/// is what makes at-least-once the right choice here.
fn forward_loop(
    upstream: &Upstream,
    spool: Option<&std::sync::Mutex<Spool>>,
    wake: &crossbeam_channel::Receiver<()>,
    stop: &std::sync::atomic::AtomicBool,
) {
    loop {
        let stopping = stop.load(std::sync::atomic::Ordering::Acquire);
        let sent_any = drain_once(upstream, spool);
        if stopping && !sent_any {
            return;
        }
        if !sent_any {
            // Woken by a push, or a timeout so a retry after a failed send
            // still happens without one.
            let _ = wake.recv_timeout(std::time::Duration::from_millis(200));
            if wake.is_empty() && stop.load(std::sync::atomic::Ordering::Acquire) {
                // One final pass, in case a push landed during shutdown.
                if !drain_once(upstream, spool) {
                    return;
                }
            }
        }
    }
}

fn drain_once(upstream: &Upstream, spool: Option<&std::sync::Mutex<Spool>>) -> bool {
    let Some(spool) = spool else { return false };
    let batch = {
        let guard = spool.lock().unwrap_or_else(|p| p.into_inner());
        match guard.peek(64) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "sentry spool read failed");
                return false;
            }
        }
    };
    if batch.is_empty() {
        return false;
    }
    let now = crate::now_unix_secs();
    let mut delivered = None;
    for (cursor, bytes) in batch {
        let Some(item) = Outbound::decode(&bytes) else {
            // Unparseable spool record: skip it rather than wedge the queue
            // behind one bad entry forever.
            tracing::warn!("skipping an undecodable sentry spool record");
            delivered = Some(cursor);
            continue;
        };
        match upstream.send(&item.target, &item.envelope, now) {
            Delivery::Failed { message } => {
                tracing::warn!(message, "sentry upstream failed; leaving envelope spooled");
                break;
            }
            Delivery::Sent { status } if status >= 500 => {
                tracing::warn!(status, "sentry upstream error; leaving envelope spooled");
                break;
            }
            Delivery::Sent { status } if status >= 400 && status != 429 => {
                // A permanent refusal. Retrying sends the same bytes forever,
                // so it is consumed and counted rather than looped on.
                tracing::warn!(status, "sentry upstream refused an envelope");
                delivered = Some(cursor);
            }
            _ => delivered = Some(cursor),
        }
    }
    if let Some(cursor) = delivered {
        let mut guard = spool.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = guard.commit(cursor) {
            tracing::warn!(error = %e, "sentry spool commit failed");
        }
        return true;
    }
    false
}

/// Sentry event ids are 32 lowercase hex characters with no dashes.
fn new_event_id() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

/// Deterministic sampling: same id, same verdict, on every node and every
/// restart. A random draw would split one trace across the boundary and leave
/// Sentry showing half a transaction tree.
fn sample_keep(id: &str, rate: f64) -> bool {
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    // FNV-1a: stable across processes, unlike DefaultHasher.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    ((hash % 10_000) as f64) < rate * 10_000.0
}

fn envelope_query_key(query: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == "sentry_key").then(|| v.to_string())
    })
}

/// Wraps a legacy `/store/` body as a one-item envelope.
fn wrap_store_payload(body: &[u8]) -> Envelope {
    let event_id = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("event_id").and_then(|i| i.as_str()).map(str::to_string))
        .unwrap_or_else(new_event_id);
    let mut header = serde_json::Map::new();
    header.insert("event_id".into(), serde_json::Value::String(event_id));
    Envelope {
        header,
        items: vec![Item {
            header_raw: format!(
                "{{\"type\":\"event\",\"length\":{},\"content_type\":\"application/json\"}}",
                body.len()
            )
            .into_bytes(),
            item_type: ItemType::Event,
            type_name: "event".into(),
            payload: body.to_vec(),
            had_length: true,
        }],
    }
}

/// Maps one envelope item to a stored record.
///
/// The full item payload is kept in `sentry.payload` for everything except
/// attachment bytes, so the local copy can reproduce what the SDK sent. The
/// body is a human-readable summary because that is what templating, dedupe and
/// grep all operate on.
fn item_to_record(
    item: &Item,
    project: &str,
    key: &str,
    env: &Envelope,
    now: u64,
) -> LogRecord {
    let json: Option<serde_json::Value> = if item.item_type == ItemType::Attachment {
        None
    } else {
        serde_json::from_slice(&item.payload).ok()
    };

    let mut record = LogRecord::new(now, Severity::INFO, String::new());
    let mut attrs = vec![
        Attr { key: "sentry.project".into(), value: AttrValue::Str(project.to_string()) },
        Attr { key: "sentry.key".into(), value: AttrValue::Str(key.to_string()) },
        Attr { key: "sentry.item_type".into(), value: AttrValue::Str(item.type_name.clone()) },
    ];
    if let Some(id) = env.event_id() {
        attrs.push(Attr { key: "sentry.event_id".into(), value: AttrValue::Str(id) });
    }

    match (&json, item.item_type) {
        (Some(v), ItemType::Event) => {
            record.severity = v
                .get("level")
                .and_then(serde_json::Value::as_str)
                .and_then(Severity::from_text)
                .unwrap_or(Severity::ERROR);
            record.severity_text =
                v.get("level").and_then(serde_json::Value::as_str).map(str::to_string);
            record.body = event_summary(v);
            record.timestamp_unix_nano = event_timestamp(v).unwrap_or(now);
            for (name, attr) in [
                ("release", "sentry.release"),
                ("environment", "sentry.environment"),
                ("platform", "sentry.platform"),
                ("server_name", "sentry.server_name"),
                ("transaction", "sentry.transaction"),
            ] {
                if let Some(value) = v.get(name).and_then(serde_json::Value::as_str) {
                    attrs.push(Attr {
                        key: attr.into(),
                        value: AttrValue::Str(value.to_string()),
                    });
                }
            }
            record.service = v
                .get("server_name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .or_else(|| Some(format!("sentry:{project}")));
            (record.trace_id, record.span_id) = trace_ids(v);
        }
        (Some(v), ItemType::Transaction) => {
            record.body = format!(
                "transaction {}",
                v.get("transaction").and_then(serde_json::Value::as_str).unwrap_or("?")
            );
            record.timestamp_unix_nano = event_timestamp(v).unwrap_or(now);
            record.service = Some(format!("sentry:{project}"));
            (record.trace_id, record.span_id) = trace_ids(v);
        }
        (_, ItemType::Attachment) => {
            record.body = format!("attachment {} bytes", item.payload.len());
            record.service = Some(format!("sentry:{project}"));
            attrs.push(Attr {
                key: "sentry.attachment_bytes".into(),
                value: AttrValue::I64(item.payload.len() as i64),
            });
        }
        _ => {
            record.body = format!("{} item", item.type_name);
            record.service = Some(format!("sentry:{project}"));
        }
    }

    if item.item_type != ItemType::Attachment || STORE_ATTACHMENT_BYTES {
        attrs.push(Attr {
            key: "sentry.payload".into(),
            value: AttrValue::Str(String::from_utf8_lossy(&item.payload).into_owned()),
        });
    }
    record.attributes = attrs;
    record
}

/// A one-line description of an event: the exception if there is one, else the
/// message. This is what gets templated and deduped, so it must be the stable
/// part — type and value, not the stack.
fn event_summary(v: &serde_json::Value) -> String {
    if let Some(values) = v.pointer("/exception/values").and_then(serde_json::Value::as_array) {
        if let Some(last) = values.last() {
            let kind = last.get("type").and_then(serde_json::Value::as_str).unwrap_or("Error");
            let value = last.get("value").and_then(serde_json::Value::as_str).unwrap_or("");
            return if value.is_empty() {
                kind.to_string()
            } else {
                format!("{kind}: {value}")
            };
        }
    }
    for pointer in ["/logentry/formatted", "/logentry/message", "/message"] {
        if let Some(text) = v.pointer(pointer).and_then(serde_json::Value::as_str) {
            if !text.is_empty() {
                return text.to_string();
            }
        }
    }
    "sentry event".to_string()
}

fn event_timestamp(v: &serde_json::Value) -> Option<u64> {
    match v.get("timestamp") {
        Some(serde_json::Value::Number(n)) => n.as_f64().map(|s| (s * 1.0e9) as u64),
        // RFC 3339 is also legal here; parsing it fully is not worth a date
        // library, so those fall back to receive time.
        _ => None,
    }
}

fn trace_ids(v: &serde_json::Value) -> (Option<[u8; 16]>, Option<[u8; 8]>) {
    let trace = v.pointer("/contexts/trace");
    let trace_id = trace
        .and_then(|t| t.get("trace_id"))
        .and_then(serde_json::Value::as_str)
        .and_then(hex_array::<16>);
    let span_id = trace
        .and_then(|t| t.get("span_id"))
        .and_then(serde_json::Value::as_str)
        .and_then(hex_array::<8>);
    (trace_id, span_id)
}

fn hex_array<const N: usize>(text: &str) -> Option<[u8; N]> {
    let clean: String = text.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if clean.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&clean[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn is_loopback(addr: &str) -> bool {
    use std::net::ToSocketAddrs;
    addr.to_socket_addrs()
        .map(|mut a| a.all(|s| s.ip().is_loopback()))
        .unwrap_or(false)
}

fn error_reply(status: u16, detail: &str) -> Reply {
    Reply::json(status, format!(r#"{{"detail":"{}"}}"#, detail.replace('"', "'")))
}

#[cfg(test)]
mod tests;
