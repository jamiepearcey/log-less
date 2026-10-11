//! Splunk HTTP Event Collector receiver.
//!
//! For a Splunk shop this is the cheapest possible migration: the forwarders,
//! agents and application libraries already point at a HEC URL, so adopting
//! log-less is a hostname change. Ranked #3 by adoption unlock in
//! `docs/architecture.md` §6, and the one where the *protocol detail* matters
//! most — clients with `useACK` will resend everything if the ack contract is
//! wrong, so [`ack`] keys acknowledgement to an actual `fdatasync`.
//!
//! Endpoints:
//!
//! | Path | Behaviour |
//! |---|---|
//! | `POST /services/collector` , `/event` | concatenated JSON events |
//! | `POST /services/collector/raw` | one event per line, metadata from the query |
//! | `POST /services/collector/ack` | durability poll |
//! | `GET /services/collector/health` , `/health/1.0` | liveness |
//!
//! The listener is [`crate::bounded_http`]. Everything that needs only the head
//! (token, `Host`, encoding, a repeated header) is judged before the body is
//! read, so a client without a token is answered without buffering or inflating
//! a byte of what it sent.
//!
//! An event's `index` routes it to that index of the store
//! (`config::Tenancy::route_record`); the sink decides, and for an index the
//! tenant does not have and refuses it answers [`Admitted::IncorrectIndex`],
//! which is Splunk's code 7 for the whole request.
//!
//! Not implemented: `/services/collector/raw` with `HTTP 1.0` chunk semantics.

pub mod ack;
pub mod event;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::auth::{Listener, NotAdmitted, Op, Principal, Refusal, Verifier};
use crate::bounded_http::{self, BoundedServer};
use crate::httpd::{self, Handler, Incoming, Limits, Method, Reject, Reply};
use crate::model::LogRecord;

pub use ack::AckTable;
pub use event::{Metadata, EventError};

/// Splunk HEC response codes. Clients branch on these, so they are the
/// documented numbers rather than anything of our own invention.
mod code {
    pub const SUCCESS: u32 = 0;
    pub const TOKEN_REQUIRED: u32 = 2;
    pub const INVALID_AUTHORIZATION: u32 = 3;
    pub const INVALID_TOKEN: u32 = 4;
    pub const NO_DATA: u32 = 5;
    pub const INVALID_DATA_FORMAT: u32 = 6;
    pub const INCORRECT_INDEX: u32 = 7;
    pub const SERVER_BUSY: u32 = 9;
    pub const DATA_CHANNEL_MISSING: u32 = 10;
    pub const INVALID_DATA_CHANNEL: u32 = 11;
    pub const EVENT_FIELD_REQUIRED: u32 = 12;
    pub const EVENT_FIELD_BLANK: u32 = 13;
    pub const ACK_DISABLED: u32 = 14;
    pub const HEALTHY: u32 = 17;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HecConfig {
    /// Opt-in, like every listener here.
    #[serde(default)]
    pub enabled: bool,
    /// Splunk's HEC port, so an existing client only changes hostname.
    #[serde(default = "default_hec_addr")]
    pub addr: String,
    /// Sizes the listener: `workers` x 32 connections at once (at least 64),
    /// each on its own thread.
    #[serde(default = "default_hec_workers")]
    pub workers: usize,
    /// Accepted tokens. **Empty means every token is accepted** — usable for a
    /// loopback-only trial, and refused outright on a non-loopback bind so a
    /// misconfiguration cannot expose an open ingest endpoint to the network.
    #[serde(default)]
    pub tokens: Vec<String>,
    /// Ask every request for a token even though `tokens` is empty: the setting for an instance whose
    /// senders are all `[[principals]]` (a HEC token is a principal's key, `docs/auth.md`). A
    /// non-loopback bind always asks.
    #[serde(default)]
    pub require_auth: bool,
    /// Serve indexer acknowledgement. Off by default because a client that
    /// enables `useACK` against a server that does not support it fails loudly,
    /// which is better than the reverse.
    #[serde(default)]
    pub ack_enabled: bool,
    #[serde(default = "default_hec_max_body")]
    pub max_body_bytes: usize,
    #[serde(default = "default_hec_max_decompressed")]
    pub max_decompressed_bytes: usize,
}

fn default_hec_addr() -> String {
    "127.0.0.1:8088".to_string()
}
fn default_hec_workers() -> usize {
    2
}
fn default_hec_max_body() -> usize {
    // Splunk's own default HEC limit is 800 KB per request; clients batch to
    // fit it, so a larger ceiling here costs nothing and refuses nothing.
    httpd::DEFAULT_MAX_BODY_BYTES
}
fn default_hec_max_decompressed() -> usize {
    httpd::DEFAULT_MAX_DECOMPRESSED_BYTES
}

impl Default for HecConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: default_hec_addr(),
            workers: default_hec_workers(),
            tokens: Vec::new(),
            require_auth: false,
            ack_enabled: false,
            max_body_bytes: default_hec_max_body(),
            max_decompressed_bytes: default_hec_max_decompressed(),
        }
    }
}

/// What the pipeline did with a batch. Mirrors the OTLP receiver: a full
/// pipeline is a `503` and a client retry, never a silent drop, because HEC —
/// like OTLP and unlike a log file — can be told to wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admitted {
    Accepted,
    Busy,
    /// An event named an index that does not exist and the tenant refuses
    /// those. Nothing of the request is stored: an acked batch must be whole.
    IncorrectIndex,
}

#[derive(Debug, Default)]
pub struct HecStats {
    pub requests: AtomicU64,
    pub records: AtomicU64,
    pub accepted: AtomicU64,
    pub busy: AtomicU64,
    pub unauthorized: AtomicU64,
    pub bad_request: AtomicU64,
    pub too_large: AtomicU64,
    pub ack_queries: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub requests: u64,
    pub records: u64,
    pub accepted: u64,
    pub busy: u64,
    pub unauthorized: u64,
    pub bad_request: u64,
    pub too_large: u64,
    pub ack_queries: u64,
}

impl HecStats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            records: self.records.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            busy: self.busy.load(Ordering::Relaxed),
            unauthorized: self.unauthorized.load(Ordering::Relaxed),
            bad_request: self.bad_request.load(Ordering::Relaxed),
            too_large: self.too_large.load(Ordering::Relaxed),
            ack_queries: self.ack_queries.load(Ordering::Relaxed),
        }
    }
}

impl StatsSnapshot {
    /// Every event request took exactly one exit. Ack polls and health checks
    /// are not counted as requests, so they do not appear here.
    pub fn accounts_for_everything(&self) -> bool {
        self.requests
            == self.accepted + self.busy + self.unauthorized + self.bad_request + self.too_large
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HecError {
    #[error(transparent)]
    Http(#[from] httpd::HttpError),
    #[error(
        "the hec listener on {addr} has no token it could accept (hec.tokens, [[principals]] or [auth]); refusing to bind a non-loopback address that would accept any"
    )]
    OpenOnPublicAddress { addr: String },
}

/// A batch handed to the pipeline, tagged with the sequence the ack table uses
/// to decide when it is durable.
pub struct Batch {
    pub seq: u64,
    pub records: Vec<LogRecord>,
}

struct HecHandler {
    config: HecConfig,
    stats: Arc<HecStats>,
    acks: Arc<AckTable>,
    sink: Box<dyn Fn(Batch) -> Admitted + Send + Sync>,
    auth: Arc<Verifier>,
    /// Accepts any token, and none: `tokens` is empty and the bind is loopback.
    open: bool,
}

impl HecHandler {
    /// Who the request is from: the principal its token is the key of (`crate::auth`). A token is a
    /// `hec.tokens` entry, a `[[principals]]` key or a broker token.
    fn authorized(&self, request: &Incoming) -> Result<Arc<Principal>, Reply> {
        let header = request.header("authorization").unwrap_or("").trim();
        let splunk = || header.strip_prefix("Splunk ").or_else(|| header.strip_prefix("splunk ")).map(str::trim);
        let from_query: Vec<String> = if header.is_empty() {
            // Splunk also accepts the token as a query parameter for clients
            // that cannot set headers.
            event::query_pairs(request.query).into_iter().filter(|(k, _)| k == "token").map(|(_, v)| v).collect()
        } else {
            Vec::new()
        };
        // An empty token list accepts anything, including a request that
        // presents nothing at all: otherwise "no tokens configured" would
        // reject every client instead of accepting every client, which is the
        // opposite of what it reads as. Only reachable on loopback: see
        // `Receiver::start`. A token that is a principal's key still names
        // that principal.
        if self.open {
            let one = request.header_count("authorization") <= 1;
            return self.check_token(splunk().filter(|_| one).or(from_query.first().map(String::as_str).filter(|_| from_query.len() == 1)));
        }
        // A token given twice is ambiguous (a proxy may act on the other one),
        // so neither counts.
        if request.header_count("authorization") > 1 {
            self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
            return Err(reply(401, code::INVALID_AUTHORIZATION, "Invalid authorization"));
        }
        if header.is_empty() {
            return match from_query.as_slice() {
                [token] => self.check_token(Some(token)),
                [] => {
                    self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
                    self.auth.refuse(Listener::Hec, Refusal::Missing);
                    Err(reply(401, code::TOKEN_REQUIRED, "Token is required"))
                }
                _ => {
                    self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
                    Err(reply(401, code::INVALID_AUTHORIZATION, "Invalid authorization"))
                }
            };
        }
        let Some(token) = splunk() else {
            self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
            return Err(reply(401, code::INVALID_AUTHORIZATION, "Invalid authorization"));
        };
        self.check_token(Some(token))
    }

    /// The comparison is the verifier's: by digest, the same time for any token.
    fn check_token(&self, token: Option<&str>) -> Result<Arc<Principal>, Reply> {
        self.auth.authorize(Listener::Hec, Op::Ingest, !self.open, token).map_err(|_| {
            self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
            reply(403, code::INVALID_TOKEN, "Invalid token")
        })
    }

    fn channel(&self, request: &Incoming) -> Option<String> {
        request
            .header("x-splunk-request-channel")
            .map(str::to_string)
            .or_else(|| {
                event::query_pairs(request.query)
                    .into_iter()
                    .find(|(k, _)| k == "channel")
                    .map(|(_, v)| v)
            })
            .filter(|c| !c.is_empty())
    }

    fn ingest(&self, request: &Incoming, raw: bool) -> Reply {
        let principal = match self.authorized(request) {
            Ok(principal) => principal,
            Err(denied) => {
                self.stats.requests.fetch_add(1, Ordering::Relaxed);
                return denied;
            }
        };
        self.stats.requests.fetch_add(1, Ordering::Relaxed);

        // No channel means no ack, not an error. Splunk enables acknowledgement
        // per *token*, so it can demand a channel from a client it knows has
        // acks on; our switch is server-wide, so demanding one would lock out
        // every client that does not use acks — which is most of them. A client
        // that does use acks always sends a channel, and gets an ackId back.
        let channel = self.channel(request);

        let defaults = Metadata::from_query(request.query);
        let now = crate::now_unix_nanos();
        let parsed = if raw {
            event::parse_raw_body(&request.body, &defaults, now)
        } else {
            event::parse_event_body(&request.body, &defaults, now)
        };
        let mut records = match parsed {
            Ok(r) => r,
            Err(e) => {
                self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                let (status, code) = match e {
                    EventError::NoData => (400, code::NO_DATA),
                    EventError::InvalidFormat => (400, code::INVALID_DATA_FORMAT),
                    EventError::EventRequired => (400, code::EVENT_FIELD_REQUIRED),
                    EventError::EventBlank => (400, code::EVENT_FIELD_BLANK),
                };
                return reply(status, code, &title_case(&e.to_string()));
            }
        };

        // The token's principal decides the tenant, and an event's `index` routes it within that tenant.
        // An index the tenant does not have (or the principal may not use) and refuses turns the whole
        // request away with Splunk's code 7, before anything of it is admitted: an acked batch must be
        // stored whole.
        match self.auth.admit(&principal, &mut records, "index", true) {
            Ok(_) => {}
            Err(NotAdmitted::Index) => {
                self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                return reply(400, code::INCORRECT_INDEX, "Incorrect index");
            }
            Err(NotAdmitted::WrongTenant) => {
                self.stats.unauthorized.fetch_add(1, Ordering::Relaxed);
                self.auth.refuse(Listener::Hec, Refusal::WrongTenant);
                return reply(403, code::INVALID_AUTHORIZATION, "Forbidden");
            }
        }

        // A channel whose acks have stalled is refused before the data is
        // admitted: the alternative is either an unbounded ack map or acking
        // something that is not on disk. The client retries, as it would for
        // any other busy response.
        if self.config.ack_enabled {
            if let Some(channel) = &channel {
                if !self.acks.has_capacity(channel) {
                    self.stats.busy.fetch_add(1, Ordering::Relaxed);
                    return reply(503, code::SERVER_BUSY, "Server is busy")
                        .with_header("Retry-After", "1");
                }
            }
        }

        let count = records.len() as u64;
        // The sequence is taken before the handoff so the ack id can be issued
        // and returned in this response, while the watermark that decides
        // durability is bound later by the record path.
        let seq = self.acks.next_batch_seq();
        match (self.sink)(Batch { seq, records }) {
            Admitted::Accepted => {
                self.stats.accepted.fetch_add(1, Ordering::Relaxed);
                self.stats.records.fetch_add(count, Ordering::Relaxed);
                match (self.config.ack_enabled, channel) {
                    (true, Some(channel)) => {
                        let ack_id = self.acks.issue(&channel, seq);
                        Reply::json(
                            200,
                            format!(r#"{{"text":"Success","code":0,"ackId":{ack_id}}}"#),
                        )
                    }
                    _ => reply(200, code::SUCCESS, "Success"),
                }
            }
            Admitted::Busy => {
                self.stats.busy.fetch_add(1, Ordering::Relaxed);
                reply(503, code::SERVER_BUSY, "Server is busy")
                    .with_header("Retry-After", "1")
            }
            Admitted::IncorrectIndex => {
                self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                reply(400, code::INCORRECT_INDEX, "Incorrect index")
            }
        }
    }

    fn ack_query(&self, request: &Incoming) -> Reply {
        if let Err(denied) = self.authorized(request) {
            return denied;
        }
        if !self.config.ack_enabled {
            return reply(400, code::ACK_DISABLED, "ACK is disabled");
        }
        let Some(channel) = self.channel(request) else {
            return reply(400, code::DATA_CHANNEL_MISSING, "Data channel is missing");
        };
        self.stats.ack_queries.fetch_add(1, Ordering::Relaxed);

        let ids: Vec<u64> = match serde_json::from_slice::<serde_json::Value>(&request.body) {
            Ok(serde_json::Value::Object(o)) => match o.get("acks") {
                Some(serde_json::Value::Array(items)) => {
                    items.iter().filter_map(serde_json::Value::as_u64).collect()
                }
                _ => return reply(400, code::INVALID_DATA_FORMAT, "Invalid data format"),
            },
            _ => return reply(400, code::INVALID_DATA_FORMAT, "Invalid data format"),
        };

        let resolved = self.acks.query(&channel, &ids);
        let body = resolved
            .iter()
            .map(|(id, durable)| format!(r#""{id}":{durable}"#))
            .collect::<Vec<_>>()
            .join(",");
        Reply::json(200, format!(r#"{{"acks":{{{body}}}}}"#))
    }
}

impl Handler for HecHandler {
    fn handle(&self, request: Incoming) -> Reply {
        match (request.method, request.path) {
            (Method::Get, "/services/collector/health")
            | (Method::Get, "/services/collector/health/1.0") => {
                reply(200, code::HEALTHY, "HEC is healthy")
            }
            (Method::Post, "/services/collector/ack") => self.ack_query(&request),
            (Method::Post, "/services/collector/raw") => self.ingest(&request, true),
            (Method::Post, "/services/collector")
            | (Method::Post, "/services/collector/event")
            | (Method::Post, "/services/collector/event/1.0") => self.ingest(&request, false),
            (Method::Post, _) => reply(404, code::INVALID_DATA_FORMAT, "Not found"),
            _ => reply(405, code::INVALID_DATA_FORMAT, "Only POST is supported"),
        }
    }

    /// Refusals that need only the head, answered before the body is read. A
    /// refused event request is counted as one; a refused ack poll is not, as
    /// ack polls never are.
    fn preflight(&self, head: &Incoming) -> Option<Reply> {
        let ingest = match (head.method, head.path) {
            (Method::Post, "/services/collector/ack") => false,
            (
                Method::Post,
                "/services/collector" | "/services/collector/event" | "/services/collector/event/1.0" | "/services/collector/raw",
            ) => true,
            _ => return None,
        };
        // Without tokens the listener is loopback only (see `Receiver::start`),
        // and what is left between it and a web page in a local browser is the
        // page's own marks: an `Origin` or `Sec-Fetch-Site` on a cross-site
        // POST, or its own name in `Host` after DNS rebinding. No HEC client
        // sends either of the first two.
        let browser = head.header("origin").is_some() || head.header("sec-fetch-site").is_some();
        let (refused, counter) = if self.open && (browser || !bounded_http::loopback_host(head)) {
            (reply(403, code::INVALID_AUTHORIZATION, "Forbidden"), Some(&self.stats.unauthorized))
        } else if let Err(denied) = self.authorized(head) {
            (denied, None)
        } else if head.header_count("x-splunk-request-channel") > 1 {
            (reply(400, code::INVALID_DATA_CHANNEL, "Invalid data channel"), Some(&self.stats.bad_request))
        } else if !bounded_http::encoding_accepted(head) {
            (reply(415, code::INVALID_DATA_FORMAT, "Content-Encoding must be gzip or absent"), Some(&self.stats.bad_request))
        } else {
            return None;
        };
        if ingest {
            self.stats.requests.fetch_add(1, Ordering::Relaxed);
            if let Some(counter) = counter {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
        Some(refused)
    }

    fn reject(&self, reason: Reject) -> Reply {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        match reason {
            Reject::TooLarge => {
                self.stats.too_large.fetch_add(1, Ordering::Relaxed);
                reply(413, code::INVALID_DATA_FORMAT, "Request body exceeds the configured limit")
            }
            Reject::Unreadable(_) => {
                self.stats.bad_request.fetch_add(1, Ordering::Relaxed);
                reply(400, code::INVALID_DATA_FORMAT, "Invalid data format")
            }
        }
    }
}

pub struct Receiver {
    server: BoundedServer,
    stats: Arc<HecStats>,
}

impl Receiver {
    /// Binds and starts serving. `acks` is shared with the record path, which
    /// must call [`AckTable::bind`] for each batch it submits, and with the WAL
    /// writer, which publishes the synced record count.
    pub fn start<F>(config: &HecConfig, acks: Arc<AckTable>, sink: F) -> Result<Self, HecError>
    where
        F: Fn(Batch) -> Admitted + Send + Sync + 'static,
    {
        let auth = crate::auth::Builder::standalone().hec(config).build();
        Self::start_with_auth(config, acks, auth, sink)
    }

    /// As [`Receiver::start`], with the instance's one verifier: every token this listener accepts, and
    /// the tenants and indexes its events are routed to.
    pub fn start_with_auth<F>(config: &HecConfig, acks: Arc<AckTable>, auth: Arc<Verifier>, sink: F) -> Result<Self, HecError>
    where
        F: Fn(Batch) -> Admitted + Send + Sync + 'static,
    {
        // An empty token list means "accept anything". That is a reasonable
        // trial setting on loopback and an open ingest endpoint anywhere else,
        // so off loopback (or with `require_auth`) a token is always asked
        // for, and there has to be one that could be accepted.
        let open = config.tokens.is_empty() && !config.require_auth && is_loopback(&config.addr);
        if !open && !auth.can_authenticate(Listener::Hec, Op::Ingest) {
            return Err(HecError::OpenOnPublicAddress { addr: config.addr.clone() });
        }

        let stats = Arc::new(HecStats::default());
        let handler = Arc::new(HecHandler {
            config: config.clone(),
            stats: Arc::clone(&stats),
            acks,
            sink: Box::new(sink),
            auth,
            open,
        });
        let server = BoundedServer::start(
            &config.addr,
            "hec",
            Limits {
                max_body_bytes: config.max_body_bytes,
                max_decompressed_bytes: config.max_decompressed_bytes,
            },
            bounded_http::ingest_limits(config.workers),
            handler,
        )?;
        Ok(Self { server, stats })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn shutdown(self) -> StatsSnapshot {
        self.server.shutdown();
        self.stats.snapshot()
    }
}

fn is_loopback(addr: &str) -> bool {
    use std::net::ToSocketAddrs;
    addr.to_socket_addrs()
        .map(|mut a| a.all(|s| s.ip().is_loopback()))
        .unwrap_or(false)
}

fn reply(status: u16, code: u32, text: &str) -> Reply {
    Reply::json(status, format!(r#"{{"text":"{text}","code":{code}}}"#))
}

fn title_case(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Harness {
        receiver: Option<Receiver>,
        seen: Arc<Mutex<Vec<LogRecord>>>,
        acks: Arc<AckTable>,
        synced: Arc<AtomicU64>,
        /// Batches the sink accepted, so a test can bind watermarks the way the
        /// record path would.
        batches: Arc<Mutex<Vec<u64>>>,
    }

    impl Harness {
        fn start(config: HecConfig, admit: bool) -> Self {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let batches = Arc::new(Mutex::new(Vec::new()));
            let synced = Arc::new(AtomicU64::new(0));
            let acks = AckTable::new(Arc::clone(&synced));
            let sink_seen = Arc::clone(&seen);
            let sink_batches = Arc::clone(&batches);
            let receiver = Receiver::start(&config, Arc::clone(&acks), move |batch| {
                if !admit {
                    return Admitted::Busy;
                }
                sink_batches.lock().unwrap().push(batch.seq);
                sink_seen.lock().unwrap().extend(batch.records);
                Admitted::Accepted
            })
            .unwrap();
            Self { receiver: Some(receiver), seen, acks, synced, batches }
        }

        fn plain() -> Self {
            Self::start(
                HecConfig {
                    enabled: true,
                    addr: "127.0.0.1:0".into(),
                    tokens: vec!["secret".into()],
                    ..Default::default()
                },
                true,
            )
        }

        fn with_ack() -> Self {
            Self::start(
                HecConfig {
                    enabled: true,
                    addr: "127.0.0.1:0".into(),
                    tokens: vec!["secret".into()],
                    ack_enabled: true,
                    ..Default::default()
                },
                true,
            )
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{}", self.receiver.as_ref().unwrap().local_addr(), path)
        }

        fn stats(&self) -> StatsSnapshot {
            self.receiver.as_ref().unwrap().stats()
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            if let Some(r) = self.receiver.take() {
                r.shutdown();
            }
        }
    }

    /// Reads the body on every status. HEC clients branch on the `code` in an
    /// error payload, so a test that cannot see it is not testing the contract.
    fn post(url: &str, body: &str, headers: &[(&str, &str)]) -> (u16, String) {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut req = agent.post(url);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let mut response = req.send(body.as_bytes()).expect("request failed");
        (response.status().as_u16(), response.body_mut().read_to_string().unwrap())
    }

    const AUTH: (&str, &str) = ("Authorization", "Splunk secret");

    #[test]
    fn accepts_a_standard_hec_batch() {
        let h = Harness::plain();
        let (status, body) = post(
            &h.url("/services/collector/event"),
            r#"{"event":"one","sourcetype":"checkout"}{"event":"two"}"#,
            &[AUTH],
        );
        assert_eq!(status, 200);
        assert_eq!(body, r#"{"text":"Success","code":0}"#);
        let seen = h.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].service.as_deref(), Some("checkout"));
        assert!(h.stats().accounts_for_everything());
    }

    #[test]
    fn a_sink_that_refuses_the_index_answers_code_7_and_stores_nothing() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink_seen = Arc::clone(&seen);
        let config = HecConfig { enabled: true, addr: "127.0.0.1:0".into(), tokens: vec!["secret".into()], ..HecConfig::default() };
        let receiver = Receiver::start(&config, AckTable::new(Arc::new(AtomicU64::new(0))), move |batch: Batch| {
            let asked = |r: &LogRecord| r.attributes.iter().any(|a| a.key == "index" && a.value == crate::model::AttrValue::Str("nope".into()));
            if batch.records.iter().any(asked) {
                return Admitted::IncorrectIndex;
            }
            sink_seen.lock().unwrap().extend(batch.records);
            Admitted::Accepted
        })
        .unwrap();
        let url = format!("http://{}/services/collector/event", receiver.local_addr());
        // One bad event refuses the whole request, as Splunk does.
        let (status, body) = post(&url, r#"{"event":"kept?","index":"main"}{"event":"x","index":"nope"}"#, &[AUTH]);
        assert_eq!((status, body.as_str()), (400, r#"{"text":"Incorrect index","code":7}"#));
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(post(&url, r#"{"event":"fine","index":"main"}"#, &[AUTH]).0, 200);
        assert_eq!(seen.lock().unwrap().len(), 1);
        let stats = receiver.shutdown();
        assert_eq!((stats.bad_request, stats.accepted), (1, 1));
        assert!(stats.accounts_for_everything());
    }

    #[test]
    fn the_bare_collector_path_is_the_event_path() {
        // Clients post to /services/collector far more often than to /event.
        let h = Harness::plain();
        assert_eq!(post(&h.url("/services/collector"), r#"{"event":"x"}"#, &[AUTH]).0, 200);
        assert_eq!(h.seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn raw_endpoint_takes_metadata_from_the_query() {
        let h = Harness::plain();
        let url = h.url("/services/collector/raw?sourcetype=nginx&host=web-1");
        assert_eq!(post(&url, "ERROR upstream timed out\nGET / 200\n", &[AUTH]).0, 200);
        let seen = h.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].service.as_deref(), Some("nginx"));
        assert_eq!(seen[0].severity, crate::Severity::ERROR);
    }

    #[test]
    fn missing_and_wrong_tokens_are_distinguished() {
        let h = Harness::plain();
        let url = h.url("/services/collector");
        assert_eq!(post(&url, r#"{"event":"x"}"#, &[]).0, 401, "no header at all");
        assert_eq!(
            post(&url, r#"{"event":"x"}"#, &[("Authorization", "Bearer secret")]).0,
            401,
            "not a Splunk scheme"
        );
        assert_eq!(
            post(&url, r#"{"event":"x"}"#, &[("Authorization", "Splunk wrong")]).0,
            403,
            "a wrong token is forbidden, not unauthenticated"
        );
        assert!(h.seen.lock().unwrap().is_empty());
        assert_eq!(h.stats().unauthorized, 3);
        assert!(h.stats().accounts_for_everything());
    }

    #[test]
    fn the_token_may_come_from_the_query_string() {
        let h = Harness::plain();
        assert_eq!(
            post(&h.url("/services/collector?token=secret"), r#"{"event":"x"}"#, &[]).0,
            200
        );
    }

    #[test]
    fn payload_errors_carry_their_splunk_code() {
        let h = Harness::plain();
        let url = h.url("/services/collector");
        for (body, code) in [
            (r#"{"time":1}"#, 12),
            (r#"{"event":""}"#, 13),
            ("{not json}", 6),
            ("   ", 5),
        ] {
            let (status, text) = post(&url, body, &[AUTH]);
            assert_eq!(status, 400, "body {body}");
            assert!(text.contains(&format!(r#""code":{code}"#)), "body {body} gave {text}");
        }
    }

    #[test]
    fn a_full_pipeline_is_server_busy_not_a_silent_drop() {
        let h = Harness::start(
            HecConfig {
                enabled: true,
                addr: "127.0.0.1:0".into(),
                tokens: vec!["secret".into()],
                ..Default::default()
            },
            false,
        );
        let (status, body) = post(&h.url("/services/collector"), r#"{"event":"x"}"#, &[AUTH]);
        assert_eq!(status, 503);
        assert!(body.contains(r#""code":9"#));
        assert_eq!(h.stats().busy, 1);
        assert!(h.stats().accounts_for_everything());
    }

    #[test]
    fn ack_ids_are_issued_and_only_flip_once_the_wal_has_synced() {
        // The contract that makes ack worth implementing: `true` must mean the
        // client can forget its copy.
        let h = Harness::with_ack();
        let url = h.url("/services/collector?channel=11111111-2222-3333-4444-555555555555");
        let (status, body) = post(&url, r#"{"event":"x"}"#, &[AUTH]);
        assert_eq!(status, 200);
        assert!(body.contains(r#""ackId":0"#), "got {body}");

        let ack_url = h.url("/services/collector/ack?channel=11111111-2222-3333-4444-555555555555");
        let (_, before) = post(&ack_url, r#"{"acks":[0]}"#, &[AUTH]);
        assert_eq!(before, r#"{"acks":{"0":false}}"#, "nothing is on disk yet");

        // What the record path and the WAL writer would do.
        let seq = h.batches.lock().unwrap()[0];
        h.acks.bind(seq, 1);
        h.synced.store(1, Ordering::Release);

        let (_, after) = post(&ack_url, r#"{"acks":[0]}"#, &[AUTH]);
        assert_eq!(after, r#"{"acks":{"0":true}}"#);
    }

    #[test]
    fn a_client_without_a_channel_still_works_when_ack_is_available() {
        // Enabling acks server-wide must not lock out clients that do not use
        // them — found by a third-party HEC handler failing 400 on every event.
        let h = Harness::with_ack();
        let (status, body) = post(&h.url("/services/collector"), r#"{"event":"x"}"#, &[AUTH]);
        assert_eq!(status, 200);
        assert_eq!(body, r#"{"text":"Success","code":0}"#, "no channel, so no ackId");
        assert_eq!(h.seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn the_ack_poll_still_requires_a_channel() {
        // An ack query without one is meaningless: ids are per channel.
        let h = Harness::with_ack();
        let (status, body) = post(&h.url("/services/collector/ack"), r#"{"acks":[0]}"#, &[AUTH]);
        assert_eq!(status, 400);
        assert!(body.contains(r#""code":10"#), "got {body}");
    }

    #[test]
    fn polling_ack_when_it_is_disabled_says_so() {
        // A client with useACK on and a server with it off must fail loudly.
        let h = Harness::plain();
        let (status, body) =
            post(&h.url("/services/collector/ack?channel=c"), r#"{"acks":[0]}"#, &[AUTH]);
        assert_eq!(status, 400);
        assert!(body.contains(r#""code":14"#), "got {body}");
    }

    #[test]
    fn the_channel_header_is_accepted_as_well_as_the_query() {
        let h = Harness::with_ack();
        let (status, body) = post(
            &h.url("/services/collector"),
            r#"{"event":"x"}"#,
            &[AUTH, ("X-Splunk-Request-Channel", "abc")],
        );
        assert_eq!(status, 200);
        assert!(body.contains(r#""ackId":0"#), "got {body}");
    }

    #[test]
    fn health_needs_no_token_and_reports_code_17() {
        let h = Harness::plain();
        let mut response = ureq::get(h.url("/services/collector/health")).call().unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert!(response.body_mut().read_to_string().unwrap().contains(r#""code":17"#));
        assert_eq!(h.stats().requests, 0, "a health probe is not an ingest request");
    }

    #[test]
    fn an_empty_token_list_refuses_to_bind_a_public_address() {
        // Accept-anything on loopback is a reasonable trial; on 0.0.0.0 it is
        // an open ingest endpoint, and a warning in a log nobody reads is not
        // an adequate answer to that.
        let config = HecConfig {
            enabled: true,
            addr: "0.0.0.0:0".into(),
            tokens: Vec::new(),
            ..Default::default()
        };
        let acks = AckTable::new(Arc::new(AtomicU64::new(0)));
        match Receiver::start(&config, acks, |_| Admitted::Accepted) {
            Err(HecError::OpenOnPublicAddress { .. }) => {}
            Err(other) => panic!("wrong error: {other}"),
            Ok(_) => panic!("an accept-anything endpoint must not bind a public address"),
        }
    }

    #[test]
    fn an_empty_token_list_is_allowed_on_loopback() {
        let h = Harness::start(
            HecConfig { enabled: true, addr: "127.0.0.1:0".into(), ..Default::default() },
            true,
        );
        assert_eq!(post(&h.url("/services/collector"), r#"{"event":"x"}"#, &[]).0, 200);
    }

    #[test]
    fn unknown_paths_do_not_count_as_ingest() {
        let h = Harness::plain();
        assert_eq!(post(&h.url("/services/collector/nope"), "{}", &[AUTH]).0, 404);
        assert_eq!(h.stats().requests, 0);
    }

    // Abuse cases, sent as raw bytes (see `crate::bounded_http::testing`).
    use crate::bounded_http::testing::{self as raw, WAIT};

    const RAW_AUTH: (&str, &str) = ("Authorization", "Splunk secret");

    fn event(headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
        raw::request("POST", "/services/collector/event", headers, body)
    }

    #[test]
    fn a_stalled_upload_does_not_stop_the_receiver_answering() {
        // With a pool of two workers, two forwarders that stop half way through a body held both forever.
        let h = Harness::plain();
        let head = "POST /services/collector HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Splunk secret\r\nContent-Length: 100000\r\n\r\n";
        let a = h.receiver.as_ref().unwrap().local_addr();
        let (status, text) = raw::answer_beside_stalled_uploads(a, head, &event(&[RAW_AUTH], br#"{"event":"served"}"#));
        assert_eq!(200, status, "{text}");
        assert_eq!(1, h.seen.lock().unwrap().len());
    }

    #[test]
    fn a_2_mib_header_is_431_and_the_receiver_keeps_serving() {
        let h = Harness::plain();
        let a = h.receiver.as_ref().unwrap().local_addr();
        assert_eq!(431, raw::huge_header_status(a, "/services/collector"));
        let many: Vec<(&str, &str)> = (0..200).map(|_| ("X-A", "b")).collect();
        assert_eq!(431, raw::exchange(a, &event(&many, b""), WAIT).0, "200 headers");
        assert_eq!(200, raw::exchange(a, &event(&[RAW_AUTH], br#"{"event":"after"}"#), WAIT).0);
        assert_eq!(1, h.seen.lock().unwrap().len());
    }

    #[test]
    fn connections_past_the_cap_are_closed_and_the_slots_come_back() {
        let h = Harness::start(
            HecConfig { addr: "127.0.0.1:0".into(), workers: 1, tokens: vec!["secret".into()], ..Default::default() },
            true,
        );
        let a = h.receiver.as_ref().unwrap().local_addr();
        assert_eq!(5, raw::closed_past_the_cap(a, 64, 5), "one worker: a cap of 64");
        assert_eq!(200, raw::exchange(a, &event(&[RAW_AUTH], br#"{"event":"after"}"#), WAIT).0);
    }

    #[test]
    fn oversized_bombed_or_damaged_bodies_are_refused_with_their_splunk_codes() {
        let h = Harness::start(
            HecConfig {
                addr: "127.0.0.1:0".into(),
                tokens: vec!["secret".into()],
                max_body_bytes: 64 * 1024,
                max_decompressed_bytes: 64 * 1024,
                ..Default::default()
            },
            true,
        );
        let a = h.receiver.as_ref().unwrap().local_addr();
        let (status, text) = raw::exchange(a, &event(&[RAW_AUTH], &vec![b' '; 100_000]), WAIT);
        assert_eq!(413, status, "{text}");
        assert!(text.contains(r#""code":6"#), "{text}");
        let lie = "POST /services/collector HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Splunk secret\r\nContent-Length: 1099511627776\r\n\r\n";
        let started = std::time::Instant::now();
        assert_eq!(413, raw::exchange(a, lie.as_bytes(), WAIT).0, "declared 1 TiB");
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        let bomb = raw::gzip(&vec![b' '; 16 * 1024 * 1024]);
        assert!(bomb.len() < 64 * 1024);
        assert_eq!(413, raw::exchange(a, &event(&[RAW_AUTH, ("Content-Encoding", "gzip")], &bomb), WAIT).0, "bomb");
        let packed = raw::gzip(br#"{"event":"x"}"#);
        for junk in [&packed[..packed.len() - 4], &b"plain"[..]] {
            let (status, text) = raw::exchange(a, &event(&[RAW_AUTH, ("Content-Encoding", "gzip")], junk), WAIT);
            assert_eq!(400, status, "{junk:?}: {text}");
            assert!(text.contains(r#""code":6"#), "{text}");
        }
        let deep = format!("{{\"event\":{}{}}}", "[".repeat(20_000), "]".repeat(20_000));
        for (name, body) in [
            ("truncated", &br#"{"event":"x"}{"event":"#[..]),
            ("nul", b"{\"event\":\0}"),
            ("invalid utf-8", b"{\"event\":\"\xff\xfe\"}"),
            ("an array", b"[{\"event\":\"x\"}]"),
            ("deep nesting", deep.as_bytes()),
        ] {
            let (status, text) = raw::exchange(a, &event(&[RAW_AUTH], body), WAIT);
            assert_eq!(400, status, "{name}: {text}");
            assert!(text.contains(r#""code":6"#), "{name}: {text}");
        }
        assert!(h.seen.lock().unwrap().is_empty(), "no partial batch was kept");
        assert!(h.stats().accounts_for_everything(), "{:?}", h.stats());
    }

    #[test]
    fn a_token_is_judged_from_the_head_and_must_arrive_once() {
        let h = Harness::plain();
        let a = h.receiver.as_ref().unwrap().local_addr();
        let body = br#"{"event":"x"}"#;
        for (name, headers, path, want, code) in [
            ("none", vec![], "/services/collector", 401, 2),
            ("empty", vec![("Authorization", "Splunk ")], "/services/collector", 401, 3),
            ("wrong", vec![("Authorization", "Splunk wrong")], "/services/collector", 403, 4),
            ("a prefix", vec![("Authorization", "Splunk secre")], "/services/collector", 403, 4),
            ("plus a suffix", vec![("Authorization", "Splunk secret!")], "/services/collector", 403, 4),
            ("other case", vec![("Authorization", "Splunk SECRET")], "/services/collector", 403, 4),
            ("bearer", vec![("Authorization", "Bearer secret")], "/services/collector", 401, 3),
            ("two headers, the good one first", vec![RAW_AUTH, ("Authorization", "Splunk wrong")], "/services/collector", 401, 3),
            ("two good headers", vec![RAW_AUTH, RAW_AUTH], "/services/collector", 401, 3),
            ("wrong header beats a good query token", vec![("Authorization", "Splunk wrong")], "/services/collector?token=secret", 403, 4),
            ("wrong query token", vec![], "/services/collector?token=wrong", 403, 4),
            ("two query tokens", vec![], "/services/collector?token=secret&token=wrong", 401, 3),
        ] {
            let (status, text) = raw::exchange(a, &raw::request("POST", path, &headers, body), WAIT);
            assert_eq!(want, status, "{name}: {text}");
            assert!(text.contains(&format!(r#""code":{code}"#)), "{name}: {text}");
        }
        // Without a token a body is never awaited: 3 MiB declared, none sent.
        let head = "POST /services/collector HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 3145728\r\n\r\n";
        let started = std::time::Instant::now();
        assert_eq!(401, raw::exchange(a, head.as_bytes(), WAIT).0);
        assert!(started.elapsed() < std::time::Duration::from_secs(2), "{:?}", started.elapsed());
        // Nor inflated, nor sized: a bomb with a wrong token learns nothing about the limits.
        let bomb = raw::gzip(&vec![b' '; 64 * 1024 * 1024]);
        assert_eq!(403, raw::exchange(a, &event(&[("Authorization", "Splunk wrong"), ("Content-Encoding", "gzip")], &bomb), WAIT).0);
        assert!(h.seen.lock().unwrap().is_empty());
        assert!(h.stats().accounts_for_everything(), "{:?}", h.stats());
    }

    #[test]
    fn only_gzip_is_decoded_and_stacked_headers_are_refused() {
        let h = Harness::with_ack();
        let a = h.receiver.as_ref().unwrap().local_addr();
        // A brotli body on the raw endpoint was stored as text.
        for encoding in ["br", "deflate", "zstd", "gzip, gzip"] {
            let req = raw::request("POST", "/services/collector/raw", &[RAW_AUTH, ("Content-Encoding", encoding)], b"line one\n");
            let (status, text) = raw::exchange(a, &req, WAIT);
            assert_eq!(415, status, "{encoding}: {text}");
        }
        let twice = event(&[RAW_AUTH, ("Content-Encoding", "gzip"), ("Content-Encoding", "gzip")], &raw::gzip(&raw::gzip(br#"{"event":"x"}"#)));
        assert_eq!(415, raw::exchange(a, &twice, WAIT).0, "stacked encodings");
        // Two channels: which one owns the ack id is ambiguous.
        let two = event(&[RAW_AUTH, ("X-Splunk-Request-Channel", "a"), ("X-Splunk-Request-Channel", "b")], br#"{"event":"x"}"#);
        let (status, text) = raw::exchange(a, &two, WAIT);
        assert_eq!(400, status, "{text}");
        assert!(text.contains(r#""code":11"#), "{text}");
        assert!(h.seen.lock().unwrap().is_empty());
        assert!(h.stats().accounts_for_everything(), "{:?}", h.stats());
    }

    #[test]
    fn a_tokenless_receiver_refuses_web_pages_and_foreign_host_names() {
        // No token means loopback only, and a page in the user's browser can still reach loopback: by a
        // cross-site POST (it carries an Origin) or by DNS rebinding (it carries the page's own Host).
        let h = Harness::start(HecConfig { addr: "127.0.0.1:0".into(), ..Default::default() }, true);
        let a = h.receiver.as_ref().unwrap().local_addr();
        let body = br#"{"event":"x"}"#;
        for (name, headers) in [
            ("a cross-site page", vec![("Origin", "https://evil.example"), ("Content-Type", "text/plain")]),
            ("an opaque origin", vec![("Origin", "null")]),
            ("a page on localhost", vec![("Origin", "http://localhost:3000")]),
            ("a fetch the browser labels cross-site", vec![("Sec-Fetch-Site", "cross-site")]),
            ("a second Host", vec![("Host", "evil.example")]),
        ] {
            let (status, text) = raw::exchange(a, &event(&headers, body), WAIT);
            assert_eq!(403, status, "{name}: {text}");
        }
        for host in ["evil.example", "evil.example:8088", "127.0.0.1.nip.io"] {
            let req = format!("POST /services/collector HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n\r\n", body.len());
            assert_eq!(403, raw::exchange(a, &[req.as_bytes(), body].concat(), WAIT).0, "Host {host}");
        }
        assert!(h.seen.lock().unwrap().is_empty());
        assert_eq!(200, raw::exchange(a, &event(&[], body), WAIT).0);
    }

    #[test]
    fn a_chunked_upload_gets_its_ack_id() {
        let h = Harness::with_ack();
        let a = h.receiver.as_ref().unwrap().local_addr();
        let req = raw::chunked("POST", "/services/collector?channel=c1", &[RAW_AUTH], br#"{"event":"one"}{"event":"two"}"#, 5);
        let (status, text) = raw::exchange(a, &req, WAIT);
        assert_eq!(200, status, "{text}");
        assert!(text.ends_with(r#"{"text":"Success","code":0,"ackId":0}"#), "{text}");
        assert_eq!(2, h.seen.lock().unwrap().len());
        let poll = raw::request("POST", "/services/collector/ack?channel=c1", &[RAW_AUTH], br#"{"acks":[0]}"#);
        assert!(raw::exchange(a, &poll, WAIT).1.ends_with(r#"{"acks":{"0":false}}"#));
    }

    #[test]
    fn gzip_bodies_are_accepted() {
        use std::io::Write;
        let h = Harness::plain();
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(br#"{"event":"compressed"}"#).unwrap();
        let gz = enc.finish().unwrap();
        let response = ureq::post(h.url("/services/collector"))
            .header("Authorization", "Splunk secret")
            .header("Content-Encoding", "gzip")
            .send(&gz[..])
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(h.seen.lock().unwrap()[0].body, "compressed");
    }
}
