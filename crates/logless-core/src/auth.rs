//! Who is talking to this instance: one [`Principal`] and one [`Verifier`] for every listener
//! (`docs/auth.md`, ADR 0004).
//!
//! A credential is a long-lived key an operator creates once and pastes into the config, as with
//! Sentry, Datadog, Splunk, Grafana and Elastic. Every key belongs to a principal: an id, a kind, a
//! tenant and optionally an index allow-list. The tenant a record is stored under **always** comes
//! from the principal and never from the request.
//!
//! Two sources of principals:
//!
//! * **Static keys**, from the config: `[[principals]]`, and every credential this instance already
//!   had (`[peer] tokens`, `[[peer.nodes]]`, `[peer.parts] replication_tokens`, the admin token, HEC
//!   tokens, Sentry keys), which are *read as* principals of the instance's own tenant. A static key is
//!   looked up by its SHA-256 digest, so it can be configured hashed (`key_sha256`) and the comparison
//!   takes the same time whatever was presented.
//! * **Broker tokens**, when `[auth] issuer` is set: EdDSA JWTs minted by session-broker, verified
//!   offline against its published keys in the order its verifier guide gives. The key set is cached on
//!   disk and served without a time limit while the broker is unreachable; nothing here ever calls the
//!   broker per request. Left unset, no network call is made at all.
//!
//! What a principal may do is decided by its [`Kind`] at each route ([`Op`]). Scopes and roles are
//! carried and shown, not interpreted: the agent has no RBAC.
//!
//! No secret reaches a log line or a status field: a principal is named by its id.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{RouteStats, Tenancy, UnknownIndex, ATTR_INDEX};
use crate::model::{Attr, AttrValue, LogRecord};

/// The record attribute that carries the tenant from the listener to the merge, for a record of a
/// tenant other than the instance's own. Set only by [`Verifier::admit`], from the principal; a value
/// the sender put there is checked against the principal and never believed. It is a directory in the
/// store, not a column, so the merge takes it off again.
pub const ATTR_TENANT: &str = "logless.tenant";
/// The key set of the broker, kept in the data dir so a restart while the broker is down still verifies.
pub const JWKS_CACHE_FILE: &str = "jwks-cache.json";
/// Shortest `[[principals]] key`: 24 characters of a random alphabet cannot be guessed online.
pub const MIN_KEY_LEN: usize = crate::admin_api::MIN_REMOTE_TOKEN_LEN;
/// A reader's tenant when it may read every tenant: the store's one older key pair, or a
/// `[[principals]]` reader with `tenant = "*"`. Not a name a tenant can have.
pub const EVERY_TENANT: &str = "*";
const REFRESH_INTERVAL: Duration = Duration::from_secs(3600);
/// An unknown `kid` triggers at most one refresh in this long.
const UNKNOWN_KID_REFRESH_SECS: u64 = 60;
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_JWKS_BYTES: u64 = 256 * 1024;
/// Longest bearer value looked at as a token; a longer one is refused unread.
const MAX_TOKEN_LEN: usize = 8 * 1024;
/// Principals with their own row of counters; past it the rest share one row.
const MAX_COUNTED_PRINCIPALS: usize = 1024;
const CLOCK_DRIFT_WARN_SECS: u64 = 30;

/// What a principal is, which decides what it may do ([`Kind::may`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// Posts records.
    Ingest,
    /// An edge node: posts records and uploads parts under its own node id.
    Node,
    /// Another central copying this one's parts.
    Replica,
    /// Reads the store over S3.
    Reader,
    /// The operator: the admin API.
    Admin,
    /// A person, signed in through the broker at the console. Never a static key.
    User,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Ingest => "ingest",
            Kind::Node => "node",
            Kind::Replica => "replica",
            Kind::Reader => "reader",
            Kind::Admin => "admin",
            Kind::User => "user",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "ingest" => Kind::Ingest,
            "node" => Kind::Node,
            "replica" => Kind::Replica,
            "reader" => Kind::Reader,
            "admin" => Kind::Admin,
            "user" => Kind::User,
            _ => return None,
        })
    }

    /// The whole permission table.
    pub fn may(self, op: Op) -> bool {
        matches!(
            (self, op),
            (Kind::Ingest | Kind::Node, Op::Ingest)
                | (Kind::Node, Op::Parts)
                | (Kind::Replica, Op::Replica)
                | (Kind::Admin, Op::Admin)
                | (Kind::Reader, Op::Read)
                | (Kind::User, Op::S3Credentials)
        )
    }
}

/// What a route does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Post records (peer `/v1/ingest`, HEC, OTLP, Sentry) and read the capability document.
    Ingest,
    /// Presign, commit and retire parts under the principal's own node id.
    Parts,
    /// Read the replica catalog and blobs.
    Replica,
    /// The admin API.
    Admin,
    /// Read the store over S3 with a static reader key.
    Read,
    /// Trade a user token for temporary S3 credentials.
    S3Credentials,
}

/// The listener a credential was presented at. A credential read from a listener's own older setting
/// (a HEC token, a Sentry key) is good at that listener only: a Sentry key is public by design and must
/// not open the peer endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Listener {
    Peer,
    Hec,
    Otlp,
    Sentry,
    Admin,
    S3,
}

impl Listener {
    pub fn as_str(self) -> &'static str {
        match self {
            Listener::Peer => "peer",
            Listener::Hec => "hec",
            Listener::Otlp => "otlp",
            Listener::Sentry => "sentry",
            Listener::Admin => "admin",
            Listener::S3 => "s3",
        }
    }
}

/// The indexes of its tenant a principal may touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Indexes {
    All,
    Only(Vec<String>),
}

impl Indexes {
    pub fn allows(&self, index: &str) -> bool {
        match self {
            Indexes::All => true,
            Indexes::Only(list) => list.iter().any(|i| i == index),
        }
    }

    fn from_list(list: &[String]) -> Self {
        if list.iter().any(|i| i == "*") {
            Indexes::All
        } else {
            Indexes::Only(list.to_vec())
        }
    }

    fn to_json(&self) -> serde_json::Value {
        match self {
            Indexes::All => serde_json::json!(["*"]),
            Indexes::Only(list) => serde_json::json!(list),
        }
    }
}

/// Where a principal came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// A key in the config file.
    Static,
    /// A token signed by the broker.
    Token,
    /// No credential, on a listener that asks for none (loopback).
    Anonymous,
}

impl Source {
    fn as_str(self) -> &'static str {
        match self {
            Source::Static => "static",
            Source::Token => "token",
            Source::Anonymous => "anonymous",
        }
    }
}

/// Who a request is from. Carries no secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// What status and log lines call it. For a node it is the node id.
    pub id: String,
    pub kind: Kind,
    pub tenant: String,
    pub indexes: Indexes,
    /// From a machine token. Shown, not interpreted.
    pub scopes: Vec<String>,
    /// From a user token. Shown, not interpreted.
    pub roles: Vec<String>,
    pub source: Source,
    /// A token's `exp`; `None` for a static key, which does not expire.
    pub expires_unix_secs: Option<u64>,
    /// Set for a credential read from one listener's own setting: good there only.
    only_at: Option<Listener>,
}

impl Principal {
    fn counter_key(&self) -> String {
        format!("{}/{}", self.tenant, self.id)
    }

    /// The index a record of this principal goes to when it names none, or names one it may not use and
    /// the tenant sends those to the default: the tenant's default index if the principal may write it,
    /// else the first index on its list that the tenant has.
    fn default_index<'a>(&'a self, tenancy: &'a Tenancy) -> Option<&'a str> {
        let default = tenancy.default_index_of(&self.tenant)?;
        if self.indexes.allows(default) {
            return Some(default);
        }
        match &self.indexes {
            Indexes::All => Some(default),
            Indexes::Only(list) => list.iter().find_map(|i| tenancy.index_of(&self.tenant, i)),
        }
    }
}

/// `[[principals]]`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalConfig {
    /// The name in status and logs. For `kind = "node"` it is the node id, and for `kind = "reader"`
    /// the S3 access key id.
    pub id: String,
    pub kind: Kind,
    /// Left out: the instance's own tenant, which is `default` unless `[[tenants]]` says otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// Left out: every index of the tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<String>>,
    /// The key, as the sender presents it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Or its SHA-256 in hex (`printf %s "$KEY" | shasum -a 256`), so the config file holds no secret.
    /// Not for `kind = "reader"`: S3 request signing needs the key itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_sha256: Option<String>,
}

// Hand-written so a logged config never carries a key.
impl std::fmt::Debug for PrincipalConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrincipalConfig")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("tenant", &self.tenant)
            .field("indexes", &self.indexes)
            .field("key", &self.key.as_ref().map(|_| "<redacted>"))
            .field("key_sha256", &self.key_sha256.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// `[auth]`: the broker whose tokens are accepted. Unset (`issuer` left out) means static keys only
/// and no network call, ever.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// The broker's `token_issuer`, compared exactly with a token's `iss`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(default = "default_audience")]
    pub audience: String,
    /// Where the key set is fetched. Left out: `<issuer>/auth/jwks.json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_url: Option<String>,
    /// A key set on disk, for an install that cannot reach the broker. When set it is the only source:
    /// the URL is never fetched. Re-read hourly and on an unknown key id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_file: Option<PathBuf>,
    /// Allowance on `exp` and `iat`, either way.
    #[serde(default = "default_clock_skew")]
    pub clock_skew_secs: u64,
}

fn default_audience() -> String {
    "logless".into()
}
fn default_clock_skew() -> u64 {
    300
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { issuer: None, audience: default_audience(), jwks_url: None, jwks_file: None, clock_skew_secs: default_clock_skew() }
    }
}

impl AuthConfig {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    fn issuer(&self) -> Option<&str> {
        self.issuer.as_deref().map(str::trim).filter(|i| !i.is_empty())
    }
}

/// Why a credential was not accepted. The names are the broker verifier guide's where it has one. Each
/// is a counter in status and a word in the log; the HTTP answer says none of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The listener wants a credential and none came.
    Missing,
    /// Not a configured key, and not a token (or `[auth]` is unset).
    UnknownKey,
    Malformed,
    Alg,
    UnknownKid,
    Signature,
    Claims,
    Issuer,
    Audience,
    Expired,
    NotYetValid,
    /// A token for a tenant this instance does not have.
    UnknownTenant,
    /// A good credential whose kind may not use this route.
    Kind,
    /// A node acting under another node's id.
    NodeMismatch,
    /// A record that names a tenant other than its principal's.
    WrongTenant,
}

impl Refusal {
    const ALL: [Refusal; 15] = [
        Refusal::Missing,
        Refusal::UnknownKey,
        Refusal::Malformed,
        Refusal::Alg,
        Refusal::UnknownKid,
        Refusal::Signature,
        Refusal::Claims,
        Refusal::Issuer,
        Refusal::Audience,
        Refusal::Expired,
        Refusal::NotYetValid,
        Refusal::UnknownTenant,
        Refusal::Kind,
        Refusal::NodeMismatch,
        Refusal::WrongTenant,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Refusal::Missing => "missing",
            Refusal::UnknownKey => "unknown_key",
            Refusal::Malformed => "malformed",
            Refusal::Alg => "alg",
            Refusal::UnknownKid => "unknown_kid",
            Refusal::Signature => "signature",
            Refusal::Claims => "claims",
            Refusal::Issuer => "issuer",
            Refusal::Audience => "audience",
            Refusal::Expired => "expired",
            Refusal::NotYetValid => "not_yet_valid",
            Refusal::UnknownTenant => "unknown_tenant",
            Refusal::Kind => "kind",
            Refusal::NodeMismatch => "node_mismatch",
            Refusal::WrongTenant => "wrong_tenant",
        }
    }

    fn slot(self) -> usize {
        Self::ALL.iter().position(|r| *r == self).unwrap_or(0)
    }

    /// The presented value was a token (it had the shape and `[auth]` is set) and failed as one. A
    /// listener that asks for no credential still refuses these: a token names a tenant, and storing its
    /// records under another one because it expired would be a silent misroute.
    fn is_token_failure(self) -> bool {
        !matches!(self, Refusal::Missing | Refusal::UnknownKey)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("[[principals]] is wrong: {0}")]
    Principal(String),
    #[error("[auth] is wrong: {0}")]
    Auth(String),
}

fn digest(s: &str) -> [u8; 32] {
    Sha256::digest(s.as_bytes()).into()
}

fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn unhex32(s: &str) -> Option<[u8; 32]> {
    let s = s.trim();
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in s.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(out)
}

/// Checks `[auth]` and `[[principals]]` against the tenants configured. Part of `Config::validate`.
pub fn check_config(auth: &AuthConfig, principals: &[PrincipalConfig], tenancy: &Tenancy) -> Result<(), String> {
    Builder::new(Arc::new(tenancy.clone())).principals(principals).map(|_| ()).map_err(|e| e.to_string())?;
    if auth.issuer().is_none() && (auth.jwks_url.is_some() || auth.jwks_file.is_some()) {
        return Err("[auth] jwks_url or jwks_file is set without issuer; tokens are checked against the issuer".into());
    }
    if auth.audience.trim().is_empty() {
        return Err("[auth] audience must not be empty".into());
    }
    Ok(())
}

#[derive(Default)]
struct PrincipalCounters {
    kind: &'static str,
    source: &'static str,
    requests: u64,
    records: u64,
    index_defaulted: u64,
    index_refused: u64,
    refused: u64,
    last_seen_unix_secs: u64,
}

/// What [`Verifier::admit`] did with a batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Admitted {
    /// Records that named an index the principal's tenant does not have (or the principal may not use)
    /// and went to its default one.
    pub defaulted: u64,
    /// Records taken out of the batch because their index is refused.
    pub refused: u64,
}

/// Why [`Verifier::admit`] turned a whole batch away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotAdmitted {
    /// A record names a tenant other than the principal's.
    WrongTenant,
    /// A record's index is refused and the caller asked for all or nothing (HEC).
    Index,
}

/// Collects the principals of one instance. [`Verifier::from_config`] is the whole config; a listener
/// started on its own (a test, a tool) adds just its own setting.
pub struct Builder {
    tenancy: Arc<Tenancy>,
    statics: HashMap<[u8; 32], Vec<Arc<Principal>>>,
    readers: Vec<(Arc<Principal>, String)>,
    routes: Arc<RouteStats>,
    jwt: Option<Jwt>,
    route_indexes: bool,
}

impl Builder {
    pub fn new(tenancy: Arc<Tenancy>) -> Self {
        Self { tenancy, statics: HashMap::new(), readers: Vec::new(), routes: Arc::new(RouteStats::default()), jwt: None, route_indexes: true }
    }

    /// For a listener started on its own, outside the agent (a test, a tool): the single tenant
    /// `default`, and records pass with the index they named, because nothing here knows which exist.
    pub fn standalone() -> Self {
        Self { route_indexes: false, ..Self::new(Arc::new(Tenancy::default())) }
    }

    /// The counters `GET /admin/v1/info` reports index routing from.
    pub fn routes(mut self, routes: Arc<RouteStats>) -> Self {
        self.routes = routes;
        self
    }

    fn own(&self, id: String, kind: Kind, at: Listener) -> Principal {
        Principal {
            id,
            kind,
            tenant: self.tenancy.tenant().to_string(),
            indexes: Indexes::All,
            scopes: Vec::new(),
            roles: Vec::new(),
            source: Source::Static,
            expires_unix_secs: None,
            only_at: Some(at),
        }
    }

    fn add(&mut self, key_digest: [u8; 32], principal: Principal) {
        self.statics.entry(key_digest).or_default().push(Arc::new(principal));
    }

    /// One listener's older credentials, each good at that listener only. An empty key is not a key.
    fn legacy<'a>(mut self, at: Listener, kind: Kind, label: &str, keys: impl IntoIterator<Item = &'a String>) -> Self {
        for (n, key) in keys.into_iter().enumerate() {
            if !key.is_empty() {
                let p = self.own(format!("{label}-{}", n + 1), kind, at);
                self.add(digest(key), p);
            }
        }
        self
    }

    /// `[peer] tokens` (ingest), `[[peer.nodes]]` (node) and `[peer.parts] replication_tokens` (replica).
    pub fn peer(mut self, config: &crate::peer::PeerConfig) -> Self {
        self = self.legacy(Listener::Peer, Kind::Ingest, "peer-token", &config.tokens);
        for node in &config.nodes {
            if !node.token.is_empty() {
                let p = self.own(node.id.clone(), Kind::Node, Listener::Peer);
                self.add(digest(&node.token), p);
            }
        }
        self.legacy(Listener::Peer, Kind::Replica, "replica-token", &config.parts.replication_tokens)
    }

    /// `[hec] tokens`.
    pub fn hec(self, config: &crate::hec::HecConfig) -> Self {
        self.legacy(Listener::Hec, Kind::Ingest, "hec-token", &config.tokens)
    }

    /// `[sentry] allowed_keys`: the public key of a DSN.
    pub fn sentry(self, config: &crate::sentry::SentryConfig) -> Self {
        self.legacy(Listener::Sentry, Kind::Ingest, "sentry-key", &config.allowed_keys)
    }

    /// The admin token (`LOGLESS_ADMIN_TOKEN` or `[admin] token`).
    pub fn admin(self, token: Option<&String>) -> Self {
        self.legacy(Listener::Admin, Kind::Admin, "admin-token", token)
    }

    /// `[serve_s3] access_key` and `secret_key`: a reader of the whole store.
    pub fn s3(mut self, access_key: &str, secret_key: &str) -> Self {
        let mut p = self.own(access_key.to_string(), Kind::Reader, Listener::S3);
        // The one pair an install had before tenants: it read everything, and still does.
        p.tenant = EVERY_TENANT.into();
        self.readers.push((Arc::new(p), secret_key.to_string()));
        self
    }

    /// `[[principals]]`.
    pub fn principals(mut self, list: &[PrincipalConfig]) -> Result<Self, AuthError> {
        let bad = |m: String| Err(AuthError::Principal(m));
        let mut seen: Vec<String> = Vec::new();
        for c in list {
            if !crate::partition::valid_ident(&c.id) {
                return bad(format!("id {:?} must be 1 to 64 of A-Za-z0-9._- with at least one letter", c.id));
            }
            if seen.contains(&c.id.to_ascii_lowercase()) {
                return bad(format!("id {:?} is used twice", c.id));
            }
            seen.push(c.id.to_ascii_lowercase());
            if c.kind == Kind::User {
                return bad(format!("{:?}: kind \"user\" is a person signed in through the broker, not a key in this file", c.id));
            }
            let tenant = c.tenant.clone().unwrap_or_else(|| self.tenancy.tenant().to_string());
            // A reader of every tenant is the operator's: `tenant = "*"`, for a reader and nothing else.
            let every_tenant = c.kind == Kind::Reader && tenant == EVERY_TENANT;
            // Tenants are not created on first use: a key for a tenant this instance does not have is a
            // typo to fix, not a directory to make.
            if !every_tenant && !self.tenancy.has_tenant(&tenant) {
                return bad(format!("{:?} is for tenant {tenant:?}, which has no [[tenants]] entry", c.id));
            }
            let indexes = match &c.indexes {
                None => Indexes::All,
                Some(_) if every_tenant => return bad(format!("{:?}: a reader of every tenant cannot list indexes", c.id)),
                Some(list) => {
                    if let Some(missing) = list.iter().find(|i| *i != "*" && self.tenancy.index_of(&tenant, i).is_none()) {
                        return bad(format!("{:?} lists index {missing:?}, which tenant {tenant:?} does not have", c.id));
                    }
                    Indexes::from_list(list)
                }
            };
            let key_digest = match (&c.key, &c.key_sha256) {
                (Some(key), None) if key.len() >= MIN_KEY_LEN => digest(key),
                (Some(_), None) => return bad(format!("{:?}: key is shorter than {MIN_KEY_LEN} characters (try `openssl rand -hex 24`)", c.id)),
                (None, Some(hex)) => match unhex32(hex) {
                    Some(d) => d,
                    None => return bad(format!("{:?}: key_sha256 must be 64 hex digits", c.id)),
                },
                _ => return bad(format!("{:?} needs exactly one of key and key_sha256", c.id)),
            };
            if self.statics.contains_key(&key_digest) || self.readers.iter().any(|(_, secret)| digest(secret) == key_digest) {
                return bad(format!("{:?} has the same key as another credential; every principal needs its own", c.id));
            }
            let principal = Principal {
                id: c.id.clone(),
                kind: c.kind,
                tenant,
                indexes,
                scopes: Vec::new(),
                roles: Vec::new(),
                source: Source::Static,
                expires_unix_secs: None,
                only_at: None,
            };
            if c.kind == Kind::Reader {
                let Some(key) = &c.key else {
                    return bad(format!("{:?}: a reader needs key, not key_sha256 (S3 request signing uses the key itself)", c.id));
                };
                self.readers.push((Arc::new(principal), key.clone()));
            } else {
                self.add(key_digest, principal);
            }
        }
        Ok(self)
    }

    /// `[auth]`. `data_dir` is where the key set is cached; `None` keeps it in memory only.
    pub fn broker(mut self, config: &AuthConfig, data_dir: Option<&Path>) -> Result<Self, AuthError> {
        let Some(issuer) = config.issuer() else { return Ok(self) };
        let source = match (&config.jwks_file, &config.jwks_url) {
            (Some(path), _) => KeySource::File(path.clone()),
            (None, Some(url)) => KeySource::Url(url.clone()),
            (None, None) => KeySource::Url(format!("{}/auth/jwks.json", issuer.trim_end_matches('/'))),
        };
        if let KeySource::Url(url) = &source {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(AuthError::Auth(format!("jwks_url {url:?} must start with http:// or https://")));
            }
        }
        let jwt = Jwt::new(issuer, &config.audience, config.clock_skew_secs, Some(source), data_dir.map(|d| d.join(JWKS_CACHE_FILE)));
        jwt.load_cache();
        if let Some(KeySource::File(_)) = &jwt.source {
            // The file is on this disk: read it now, so the first request finds its keys.
            jwt.refresh(crate::now_unix_secs());
        }
        self.jwt = Some(jwt);
        Ok(self)
    }

    pub fn build(self) -> Arc<Verifier> {
        let anonymous = Arc::new(Principal {
            id: "anonymous".into(),
            kind: Kind::Ingest,
            tenant: self.tenancy.tenant().to_string(),
            indexes: Indexes::All,
            scopes: Vec::new(),
            roles: Vec::new(),
            source: Source::Anonymous,
            expires_unix_secs: None,
            only_at: None,
        });
        Arc::new(Verifier {
            tenancy: self.tenancy,
            statics: self.statics,
            readers: self.readers,
            anonymous,
            jwt: self.jwt,
            routes: self.routes,
            route_indexes: self.route_indexes,
            refused: Default::default(),
            refusal_logged: Default::default(),
            by_principal: Mutex::new(BTreeMap::new()),
            stop: Arc::new(AtomicBool::new(false)),
        })
    }
}

/// Decides who a credential is. One per process, shared by every listener.
pub struct Verifier {
    tenancy: Arc<Tenancy>,
    /// Digest of a key to its principals: more than one only when two listeners' older settings hold
    /// the same string.
    statics: HashMap<[u8; 32], Vec<Arc<Principal>>>,
    /// S3 readers with the secret their requests are signed with (`crate::s3_serve`).
    readers: Vec<(Arc<Principal>, String)>,
    anonymous: Arc<Principal>,
    jwt: Option<Jwt>,
    routes: Arc<RouteStats>,
    /// False only for [`Builder::standalone`]: `admit` then binds the tenant and leaves the index alone.
    route_indexes: bool,
    refused: [AtomicU64; Refusal::ALL.len()],
    refusal_logged: [AtomicU64; Refusal::ALL.len()],
    by_principal: Mutex<BTreeMap<String, PrincipalCounters>>,
    stop: Arc<AtomicBool>,
}

impl Verifier {
    /// Every credential of `config`: the older per-listener settings read as principals,
    /// `[[principals]]`, and the broker when `[auth]` names one.
    pub fn from_config(config: &crate::Config, tenancy: Arc<Tenancy>, routes: Arc<RouteStats>) -> Result<Arc<Self>, AuthError> {
        let admin_token = crate::admin_api::configured_token(&config.admin);
        let mut builder = Builder::new(tenancy)
            .routes(routes)
            .peer(&config.peer)
            .hec(&config.hec)
            .sentry(&config.sentry)
            .admin(admin_token.as_ref());
        if let Some((access, secret)) = crate::s3_serve::configured_credentials(&config.serve_s3) {
            builder = builder.s3(&access, &secret);
        }
        Ok(builder.principals(&config.principals)?.broker(&config.auth, Some(&config.storage.data_dir))?.build())
    }

    pub fn tenancy(&self) -> &Tenancy {
        &self.tenancy
    }

    /// The principal of a request that carries no credential, on a listener that asks for none: the
    /// instance's own tenant, every index.
    pub fn anonymous(&self) -> Arc<Principal> {
        Arc::clone(&self.anonymous)
    }

    /// Whether any credential could be accepted at `listener` for `op`: a listener that must have one
    /// (a non-loopback bind) refuses to start when this is false.
    pub fn can_authenticate(&self, listener: Listener, op: Op) -> bool {
        if op == Op::Read {
            return !self.readers.is_empty();
        }
        self.jwt.is_some() || self.statics.values().flatten().any(|p| p.kind.may(op) && p.only_at.is_none_or(|at| at == listener))
    }

    /// The S3 readers: access key id (the principal id), principal, signing secret.
    pub fn readers(&self) -> impl Iterator<Item = (&Arc<Principal>, &str)> {
        self.readers.iter().map(|(p, secret)| (p, secret.as_str()))
    }

    /// Who `presented` is at `listener`, at the current time. A static key first (one hash and one map
    /// lookup), then a broker token if it has the shape and `[auth]` is set.
    pub fn verify(&self, listener: Listener, presented: &str) -> Result<Arc<Principal>, Refusal> {
        self.verify_at(listener, presented, crate::now_unix_secs())
    }

    pub fn verify_at(&self, listener: Listener, presented: &str, now_unix_secs: u64) -> Result<Arc<Principal>, Refusal> {
        let presented_digest = digest(presented);
        // The map finds the digest; the comparison that decides is over all 32 bytes, whatever was presented.
        if let Some((known, found)) = self.statics.get_key_value(&presented_digest) {
            if let Some(p) = found.iter().find(|p| p.only_at.is_none_or(|at| at == listener)).filter(|_| ct_eq(known, &presented_digest)) {
                return Ok(Arc::clone(p));
            }
        }
        let Some(jwt) = self.jwt.as_ref().filter(|_| looks_like_jwt(presented)) else {
            return Err(Refusal::UnknownKey);
        };
        let claims = jwt.verify_at(presented, now_unix_secs)?;
        if !self.tenancy.has_tenant(&claims.tenant) {
            return Err(Refusal::UnknownTenant);
        }
        Ok(Arc::new(Principal {
            // A machine token's `name` is what the operator called the principal, and for a node it is
            // the node id; a user token has none and is its subject.
            id: claims.name.unwrap_or(claims.sub),
            kind: claims.kind,
            tenant: claims.tenant,
            indexes: claims.indexes,
            scopes: claims.scopes,
            roles: claims.roles,
            source: Source::Token,
            expires_unix_secs: Some(claims.exp),
            only_at: None,
        }))
    }

    /// The whole decision for a request at `listener` that wants to do `op`. `closed` is whether the
    /// listener asks for a credential at all: an open one (loopback, nothing configured for it) takes a
    /// request without one, or with one this instance does not know, as [`Verifier::anonymous`], exactly
    /// as it did before principals existed. A refusal is counted and logged by reason here; the caller
    /// answers 401 for [`Refusal::Missing`] and 403 otherwise and says nothing more.
    pub fn authorize(&self, listener: Listener, op: Op, closed: bool, presented: Option<&str>) -> Result<Arc<Principal>, Refusal> {
        let who = match presented {
            None if closed => Err(Refusal::Missing),
            None => Ok(self.anonymous()),
            Some(value) if value.len() > MAX_TOKEN_LEN => Err(Refusal::UnknownKey),
            Some(value) => match self.verify(listener, value) {
                Err(reason) if !closed && !reason.is_token_failure() => Ok(self.anonymous()),
                other => other,
            },
        };
        let who = who.and_then(|p| {
            if p.kind.may(op) {
                Ok(p)
            } else {
                self.count(&p, |c| c.refused += 1);
                Err(Refusal::Kind)
            }
        });
        if let Err(reason) = who {
            self.refuse(listener, reason);
        }
        who
    }

    /// Counts a refusal and says so in the log, at most once a minute per reason: a flood of bad
    /// requests must not become a flood of log lines.
    pub fn refuse(&self, listener: Listener, reason: Refusal) {
        let slot = reason.slot();
        let total = self.refused[slot].fetch_add(1, Ordering::Relaxed) + 1;
        let now = crate::now_unix_secs();
        let last = self.refusal_logged[slot].load(Ordering::Relaxed);
        if now.saturating_sub(last) >= 60 && self.refusal_logged[slot].compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            tracing::warn!(listener = listener.as_str(), reason = reason.as_str(), total, "a credential was refused");
        }
    }

    /// As [`Verifier::refuse`], for a refusal that is about a known principal (a node id that is not its
    /// own, a record for another tenant).
    pub fn refuse_principal(&self, listener: Listener, principal: &Principal, reason: Refusal) {
        self.count(principal, |c| c.refused += 1);
        self.refuse(listener, reason);
    }

    fn count(&self, principal: &Principal, update: impl FnOnce(&mut PrincipalCounters)) {
        let mut map = self.by_principal.lock().unwrap_or_else(|p| p.into_inner());
        let key = if map.len() >= MAX_COUNTED_PRINCIPALS && !map.contains_key(&principal.counter_key()) { "(others)".to_string() } else { principal.counter_key() };
        let entry = map.entry(key).or_default();
        entry.kind = principal.kind.as_str();
        entry.source = principal.source.as_str();
        entry.last_seen_unix_secs = crate::now_unix_secs();
        update(entry);
    }

    /// Binds a batch to its principal before it enters the pipeline.
    ///
    /// * **Tenant.** Every record is stored under the principal's tenant. A record that carries
    ///   [`ATTR_TENANT`] for another tenant turns the whole batch away ([`NotAdmitted::WrongTenant`]).
    /// * **Index.** The record's `key` attribute (the protocol's name for it) or [`ATTR_INDEX`] names the
    ///   index. One the tenant has and the principal may use is kept. Any other follows the tenant's
    ///   `unknown_index`: to the principal's default index, or refused. A refused record is taken out
    ///   of the batch, or with `whole` the batch is turned away ([`NotAdmitted::Index`], HEC's code 7).
    ///
    /// The attributes are rewritten to what was decided, so the rest of the pipeline reads them and
    /// nothing a sender wrote: [`ATTR_INDEX`] absent for the tenant's default index, [`ATTR_TENANT`]
    /// absent for the instance's own tenant.
    pub fn admit(&self, principal: &Principal, records: &mut Vec<LogRecord>, key: &str, whole: bool) -> Result<Admitted, NotAdmitted> {
        let tenancy = &*self.tenancy;
        let tenant = principal.tenant.as_str();
        let default = principal.default_index(tenancy);
        let tenant_default = tenancy.default_index_of(tenant);
        let unknown = tenancy.unknown_index(tenant).unwrap_or(UnknownIndex::Refuse);
        let mut chosen: Vec<Option<&str>> = Vec::with_capacity(records.len());
        let mut out = Admitted::default();
        for record in records.iter() {
            let mut requested: Option<String> = None;
            for attr in &record.attributes {
                if attr.key == ATTR_TENANT {
                    if !matches!(&attr.value, AttrValue::Str(t) if t == tenant) {
                        self.count(principal, |c| c.refused += 1);
                        return Err(NotAdmitted::WrongTenant);
                    }
                } else if requested.is_none() && (attr.key == key || attr.key == ATTR_INDEX) {
                    requested = Some(match &attr.value {
                        AttrValue::Str(s) => s.clone(),
                        AttrValue::I64(n) => n.to_string(),
                        AttrValue::F64(n) => n.to_string(),
                        AttrValue::Bool(b) => b.to_string(),
                    });
                }
            }
            let index = match requested.as_deref() {
                _ if !self.route_indexes => Some(""),
                None => default,
                Some(r) => match tenancy.index_of(tenant, r).filter(|i| principal.indexes.allows(i)) {
                    Some(i) => Some(i),
                    None if unknown == UnknownIndex::Default && default.is_some() => {
                        out.defaulted += 1;
                        default
                    }
                    None => None,
                },
            };
            if index.is_none() {
                out.refused += 1;
            }
            chosen.push(index);
        }
        self.routes.defaulted.fetch_add(out.defaulted, Ordering::Relaxed);
        self.routes.refused.fetch_add(out.refused, Ordering::Relaxed);
        if whole && out.refused > 0 {
            self.count(principal, |c| c.index_refused += out.refused);
            return Err(NotAdmitted::Index);
        }
        let own_tenant = tenant == tenancy.tenant();
        let mut at = 0;
        records.retain_mut(|record| {
            let index = chosen[at];
            at += 1;
            let Some(index) = index else { return false };
            if !self.route_indexes {
                record.attributes.retain(|a| a.key != ATTR_TENANT);
                return true;
            }
            record.attributes.retain(|a| a.key != key && a.key != ATTR_INDEX && a.key != ATTR_TENANT);
            if Some(index) != tenant_default {
                record.attributes.push(Attr { key: ATTR_INDEX.into(), value: AttrValue::Str(index.to_string()) });
            }
            if !own_tenant {
                record.attributes.push(Attr { key: ATTR_TENANT.into(), value: AttrValue::Str(tenant.to_string()) });
            }
            true
        });
        let stored = records.len() as u64;
        self.count(principal, |c| {
            c.requests += 1;
            c.records += stored;
            c.index_defaulted += out.defaulted;
            c.index_refused += out.refused;
        });
        Ok(out)
    }

    /// For `GET /admin/v1/info`: who is configured, who has been seen, what was refused and why, and
    /// how old the broker's key set is. Ids and counts; never a key.
    pub fn status(&self) -> serde_json::Value {
        let now = crate::now_unix_secs();
        let mut refused = serde_json::Map::new();
        for reason in Refusal::ALL {
            refused.insert(reason.as_str().into(), self.refused[reason.slot()].load(Ordering::Relaxed).into());
        }
        let seen: Vec<serde_json::Value> = self
            .by_principal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(key, c)| {
                serde_json::json!({
                    "principal": key,
                    "kind": c.kind,
                    "source": c.source,
                    "requests": c.requests,
                    "records": c.records,
                    "index_defaulted": c.index_defaulted,
                    "index_refused": c.index_refused,
                    "refused": c.refused,
                    "last_seen_unix_secs": c.last_seen_unix_secs,
                })
            })
            .collect();
        let mut configured: Vec<serde_json::Value> = self
            .statics
            .values()
            .flatten()
            .chain(self.readers.iter().map(|(p, _)| p))
            .map(|p| {
                serde_json::json!({
                    "id": p.id,
                    "kind": p.kind.as_str(),
                    "tenant": p.tenant,
                    "indexes": p.indexes.to_json(),
                    "listener": p.only_at.map(Listener::as_str),
                })
            })
            .collect();
        configured.sort_by_key(|v| (v["tenant"].as_str().unwrap_or("").to_string(), v["id"].as_str().unwrap_or("").to_string()));
        serde_json::json!({
            "principals": configured,
            "seen": seen,
            "refused": refused,
            "broker": self.jwt.as_ref().map(|j| j.status(now)),
        })
    }

    /// Starts the hourly key refresh. Does nothing, and returns `None`, when `[auth]` is unset: no
    /// thread, no network call.
    pub fn start_refresh(self: &Arc<Self>) -> Option<std::thread::JoinHandle<()>> {
        self.jwt.as_ref().filter(|j| j.source.is_some())?;
        let me = Arc::clone(self);
        std::thread::Builder::new()
            .name("logless-auth-keys".into())
            .spawn(move || {
                let Some(jwt) = &me.jwt else { return };
                // Up to five minutes of jitter, fixed per process, so a fleet does not ask in the same second.
                let jitter = Duration::from_secs(u64::from(std::process::id()) % 300);
                loop {
                    jwt.refresh(crate::now_unix_secs());
                    let mut slept = Duration::ZERO;
                    while slept < REFRESH_INTERVAL + jitter {
                        if me.stop.load(Ordering::Relaxed) {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(250));
                        slept += Duration::from_millis(250);
                    }
                }
            })
            .ok()
    }

    /// Ends the refresh thread at its next tick.
    pub fn stop_refresh(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Three dot-separated parts, the first a base64url JSON object: what a compact JWS looks like. Decides
/// only whether a value is *tried* as a token.
fn looks_like_jwt(value: &str) -> bool {
    value.len() <= MAX_TOKEN_LEN && value.starts_with("eyJ") && value.bytes().filter(|b| *b == b'.').count() == 2
}

/// Base64url without padding, strictly: no `=`, no characters of the other alphabet, no stray bits.
fn b64url(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0u32);
    for &b in bytes {
        let v = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    (acc == 0).then_some(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Claims {
    sub: String,
    name: Option<String>,
    tenant: String,
    kind: Kind,
    indexes: Indexes,
    scopes: Vec<String>,
    roles: Vec<String>,
    exp: u64,
}

#[derive(Debug, Clone)]
enum KeySource {
    Url(String),
    File(PathBuf),
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct KeySet {
    /// `kid` to the base64url public key, as the broker published it.
    keys: BTreeMap<String, String>,
    /// When this set was last confirmed by its source (a 200 or a 304).
    fetched_unix_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    /// The issuer the set belongs to: a cache left by another `[auth] issuer` is not used.
    #[serde(default)]
    issuer: String,
}

#[derive(Debug, Default)]
struct RefreshState {
    last_attempt_unix_secs: u64,
    last_error: Option<String>,
    last_error_unix_secs: u64,
    refreshes: u64,
    failures: u64,
}

/// The broker side: the checks of `session-broker/docs/verifier/README.md`, in its order, and the key
/// cache it describes.
struct Jwt {
    issuer: String,
    audience: String,
    skew: u64,
    keys: RwLock<KeySet>,
    source: Option<KeySource>,
    cache_path: Option<PathBuf>,
    state: Mutex<RefreshState>,
}

impl Jwt {
    fn new(issuer: &str, audience: &str, skew: u64, source: Option<KeySource>, cache_path: Option<PathBuf>) -> Self {
        Self {
            issuer: issuer.to_string(),
            audience: audience.to_string(),
            skew,
            keys: RwLock::new(KeySet { issuer: issuer.to_string(), ..KeySet::default() }),
            source,
            cache_path,
            state: Mutex::new(RefreshState::default()),
        }
    }

    fn key(&self, kid: &str) -> Option<Vec<u8>> {
        self.keys.read().unwrap_or_else(|p| p.into_inner()).keys.get(kid).and_then(|x| b64url(x))
    }

    fn verify_at(&self, token: &str, now: u64) -> Result<Claims, Refusal> {
        // 1. Shape and header. The algorithm is judged before any key is looked at.
        let mut parts = token.split('.');
        let (Some(head), Some(payload), Some(signature), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
            return Err(Refusal::Malformed);
        };
        let header: serde_json::Value = b64url(head).and_then(|b| serde_json::from_slice(&b).ok()).ok_or(Refusal::Malformed)?;
        let header = header.as_object().ok_or(Refusal::Malformed)?;
        if header.get("alg").and_then(|a| a.as_str()) != Some("EdDSA") {
            return Err(Refusal::Alg);
        }
        if !matches!(header.get("typ"), None | Some(serde_json::Value::String(_))) || header.get("typ").is_some_and(|t| t.as_str() != Some("JWT")) || header.contains_key("crit") {
            return Err(Refusal::Malformed);
        }
        let kid = header.get("kid").and_then(|k| k.as_str()).filter(|k| !k.is_empty()).ok_or(Refusal::Malformed)?;

        // 2. The key, with one refresh if it is new to us.
        let key = match self.key(kid) {
            Some(key) => key,
            None => {
                if self.refresh_for_unknown_kid(now) {
                    self.key(kid).ok_or(Refusal::UnknownKid)?
                } else {
                    return Err(Refusal::UnknownKid);
                }
            }
        };

        // 3. The signature, over the bytes as received.
        let signature = b64url(signature).ok_or(Refusal::Signature)?;
        let signed = &token.as_bytes()[..head.len() + 1 + payload.len()];
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, &key).verify(signed, &signature).map_err(|_| Refusal::Signature)?;

        // 4. The claims. Nothing of the payload was read before this line.
        let claims: serde_json::Value = b64url(payload).and_then(|b| serde_json::from_slice(&b).ok()).ok_or(Refusal::Claims)?;
        let claims = claims.as_object().ok_or(Refusal::Claims)?;
        let text = |name: &str| claims.get(name).and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let seconds = |name: &str| claims.get(name).and_then(|v| v.as_u64().or_else(|| v.as_f64().filter(|f| f.is_finite() && *f >= 0.0).map(|f| f as u64)));
        let list = |name: &str| -> Result<Vec<String>, Refusal> {
            match claims.get(name) {
                None => Ok(Vec::new()),
                Some(serde_json::Value::Array(items)) => items.iter().map(|i| i.as_str().map(str::to_string).ok_or(Refusal::Claims)).collect(),
                Some(_) => Err(Refusal::Claims),
            }
        };
        if !claims.contains_key("aud") {
            return Err(Refusal::Claims);
        }
        let (Some(iss), Some(sub), Some(exp), Some(iat), Some(_jti), Some(tenant), Some(kind)) =
            (text("iss"), text("sub"), seconds("exp"), seconds("iat"), text("jti"), text("tenant"), text("kind"))
        else {
            return Err(Refusal::Claims);
        };
        if iss != self.issuer {
            return Err(Refusal::Issuer);
        }
        // One string, ours: an array, or another product's name, is refused.
        if claims.get("aud").and_then(|a| a.as_str()) != Some(self.audience.as_str()) {
            return Err(Refusal::Audience);
        }
        if exp <= now.saturating_sub(self.skew) {
            return Err(Refusal::Expired);
        }
        if iat > now.saturating_add(self.skew) {
            return Err(Refusal::NotYetValid);
        }
        let kind = Kind::parse(kind).ok_or(Refusal::Claims)?;
        // The tenant becomes a directory name, so it has to be able to be one.
        if !crate::partition::valid_ident(tenant) {
            return Err(Refusal::Claims);
        }
        Ok(Claims {
            sub: sub.to_string(),
            name: text("name").map(str::to_string),
            tenant: tenant.to_string(),
            kind,
            // No `indexes` claim is no index, not every index.
            indexes: Indexes::from_list(&list("indexes")?),
            scopes: list("scopes")?,
            roles: list("roles")?,
            exp,
        })
    }

    /// True when a refresh was made and succeeded. At most one attempt per [`UNKNOWN_KID_REFRESH_SECS`],
    /// whoever asks: the timestamp is taken under the lock before the fetch.
    fn refresh_for_unknown_kid(&self, now: u64) -> bool {
        if self.source.is_none() {
            return false;
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.last_attempt_unix_secs != 0 && now.saturating_sub(state.last_attempt_unix_secs) < UNKNOWN_KID_REFRESH_SECS {
                return false;
            }
            state.last_attempt_unix_secs = now.max(1);
        }
        self.refresh(now)
    }

    /// Reads the cache left in the data dir by an earlier run.
    fn load_cache(&self) {
        let Some(path) = &self.cache_path else { return };
        let Ok(bytes) = std::fs::read(path) else { return };
        match serde_json::from_slice::<KeySet>(&bytes) {
            Ok(set) if set.issuer == self.issuer && !set.keys.is_empty() => {
                tracing::info!(keys = set.keys.len(), fetched_unix_secs = set.fetched_unix_secs, "broker key set read from the cache");
                *self.keys.write().unwrap_or_else(|p| p.into_inner()) = set;
            }
            Ok(_) => tracing::info!("the cached broker key set is for another issuer; not used"),
            Err(e) => tracing::warn!(error = %e, "the cached broker key set does not parse; not used"),
        }
    }

    /// Asks the source for the key set. On any failure the set in memory and on disk is left exactly as
    /// it was: a key is never dropped because the broker could not be reached.
    fn refresh(&self, now: u64) -> bool {
        let Some(source) = &self.source else { return false };
        let etag = self.keys.read().unwrap_or_else(|p| p.into_inner()).etag.clone();
        let fetched = match source {
            KeySource::File(path) => std::fs::read(path).map(|bytes| Fetched::Body { bytes, etag: None }).map_err(|e| format!("cannot read {}: {e}", path.display())),
            KeySource::Url(url) => fetch(url, etag.as_deref()),
        };
        let outcome = fetched.and_then(|f| match f {
            Fetched::NotModified => Ok(None),
            Fetched::Body { bytes, etag } => parse_jwks(&bytes).map(|keys| Some((keys, etag))),
        });
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.last_attempt_unix_secs = now.max(1);
        match outcome {
            Ok(new) => {
                let mut set = self.keys.write().unwrap_or_else(|p| p.into_inner());
                if let Some((keys, etag)) = new {
                    if set.keys != keys {
                        tracing::info!(keys = keys.len(), "broker key set updated");
                    }
                    set.keys = keys;
                    set.etag = etag;
                }
                set.fetched_unix_secs = now;
                state.refreshes += 1;
                state.last_error = None;
                if let Some(path) = &self.cache_path {
                    let tmp = path.with_extension("json.tmp");
                    let written = serde_json::to_vec(&*set).map_err(std::io::Error::other).and_then(|bytes| std::fs::write(&tmp, bytes)).and_then(|()| std::fs::rename(&tmp, path));
                    if let Err(e) = written {
                        tracing::warn!(error = %e, "the broker key set could not be saved; a restart while the broker is down would start without keys");
                    }
                }
                true
            }
            Err(error) => {
                state.failures += 1;
                let age = now.saturating_sub(self.keys.read().unwrap_or_else(|p| p.into_inner()).fetched_unix_secs);
                tracing::warn!(%error, cached_key_set_age_secs = age, "the broker key set could not be refreshed; the cached keys stay in use");
                state.last_error = Some(error);
                state.last_error_unix_secs = now;
                false
            }
        }
    }

    fn status(&self, now: u64) -> serde_json::Value {
        let set = self.keys.read().unwrap_or_else(|p| p.into_inner());
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        serde_json::json!({
            "issuer": self.issuer,
            "audience": self.audience,
            "clock_skew_secs": self.skew,
            "key_source": match &self.source { Some(KeySource::Url(u)) => u.clone(), Some(KeySource::File(p)) => p.display().to_string(), None => String::new() },
            "key_ids": set.keys.keys().collect::<Vec<_>>(),
            "keys_fetched_unix_secs": set.fetched_unix_secs,
            // How stale the cache is. It is served without a time limit; this is the number to alarm on.
            "key_set_age_secs": (set.fetched_unix_secs != 0).then(|| now.saturating_sub(set.fetched_unix_secs)),
            "refreshes": state.refreshes,
            "refresh_failures": state.failures,
            "last_refresh_error": state.last_error,
            "last_refresh_error_unix_secs": state.last_error.as_ref().map(|_| state.last_error_unix_secs),
        })
    }
}

enum Fetched {
    NotModified,
    Body { bytes: Vec<u8>, etag: Option<String> },
}

fn fetch(url: &str, etag: Option<&str>) -> Result<Fetched, String> {
    let agent: ureq::Agent = ureq::Agent::config_builder().timeout_global(Some(FETCH_TIMEOUT)).http_status_as_error(false).build().into();
    let mut request = agent.get(url);
    if let Some(etag) = etag {
        request = request.header("If-None-Match", etag);
    }
    let before = crate::now_unix_secs();
    let mut response = request.call().map_err(|e| format!("could not reach {url}: {e}"))?;
    // The broker's clock against ours: the allowance on `exp` and `iat` is the whole budget, and drift eats it.
    if let Some(theirs) = response.headers().get("date").and_then(|d| d.to_str().ok()).and_then(crate::s3_serve::parse_http_date) {
        let drift = theirs.abs_diff(before);
        if drift > CLOCK_DRIFT_WARN_SECS {
            tracing::warn!(drift_secs = drift, "this clock and the broker's differ; tokens are judged with a fixed allowance");
        }
    }
    match response.status().as_u16() {
        304 => Ok(Fetched::NotModified),
        200 => {
            let etag = response.headers().get("etag").and_then(|e| e.to_str().ok()).map(str::to_string);
            let bytes = response.body_mut().with_config().limit(MAX_JWKS_BYTES).read_to_vec().map_err(|e| format!("could not read the key set from {url}: {e}"))?;
            Ok(Fetched::Body { bytes, etag })
        }
        other => Err(format!("{url} answered HTTP {other}")),
    }
}

/// The Ed25519 keys of a JWKS document. A set with none is an error, so a refresh can never replace
/// working keys with nothing.
fn parse_jwks(bytes: &[u8]) -> Result<BTreeMap<String, String>, String> {
    let doc: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| format!("the key set is not JSON: {e}"))?;
    let keys = doc.get("keys").and_then(|k| k.as_array()).ok_or("the key set has no `keys` array")?;
    let mut out = BTreeMap::new();
    for key in keys {
        let field = |name: &str| key.get(name).and_then(|v| v.as_str());
        if field("kty") != Some("OKP") || field("crv") != Some("Ed25519") {
            continue;
        }
        let (Some(kid), Some(x)) = (field("kid"), field("x")) else { continue };
        if b64url(x).is_some_and(|raw| raw.len() == 32) {
            out.insert(kid.to_string(), x.to_string());
        }
    }
    if out.is_empty() {
        return Err("the key set holds no Ed25519 key".into());
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod testing {
    //! A broker in a test: a key pair, a key set, and tokens signed with it.
    use super::*;
    use ring::signature::KeyPair;

    pub const ISSUER: &str = "https://broker.example.test";

    pub fn b64(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let n = chunk.iter().fold(0u32, |acc, b| (acc << 8) | u32::from(*b)) << (8 * (3 - chunk.len()));
            for i in 0..=chunk.len() {
                out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
            }
        }
        out
    }

    pub struct Signer {
        pair: ring::signature::Ed25519KeyPair,
        pub kid: String,
    }

    impl Signer {
        pub fn new(seed: u8, kid: &str) -> Self {
            Self { pair: ring::signature::Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap(), kid: kid.into() }
        }

        pub fn public(&self) -> Vec<u8> {
            self.pair.public_key().as_ref().to_vec()
        }

        pub fn jwks(&self) -> String {
            serde_json::json!({"keys": [{"kty": "OKP", "crv": "Ed25519", "kid": self.kid, "x": b64(&self.public()), "use": "sig", "alg": "EdDSA"}]}).to_string()
        }

        pub fn sign_parts(&self, header: &serde_json::Value, claims: &serde_json::Value) -> String {
            let signed = format!("{}.{}", b64(header.to_string().as_bytes()), b64(claims.to_string().as_bytes()));
            let signature = self.pair.sign(signed.as_bytes());
            format!("{signed}.{}", b64(signature.as_ref()))
        }

        pub fn sign(&self, claims: &serde_json::Value) -> String {
            self.sign_parts(&serde_json::json!({"alg": "EdDSA", "typ": "JWT", "kid": self.kid}), claims)
        }
    }

    /// Claims of a machine token good from `now - 10` for a day.
    pub fn claims(kind: &str, tenant: &str, name: &str, now: u64) -> serde_json::Value {
        serde_json::json!({
            "iss": ISSUER, "sub": format!("sp_{name}"), "aud": "logless", "exp": now + 86_400, "iat": now - 10,
            "jti": "AAAAAAAAAAAAAAAAAAAAAA", "tenant": tenant, "kind": kind, "name": name, "scopes": [kind], "indexes": ["*"],
        })
    }

    /// Two tenants: the instance's own, `default`, and `acme` with indexes `default` and `payments`.
    pub fn two_tenants(unknown_index: UnknownIndex) -> Arc<Tenancy> {
        let index = |name: &str| crate::config::IndexConfig { name: name.into(), retention: Default::default() };
        let mut tenants = crate::config::default_tenants();
        tenants.push(crate::config::TenantConfig { id: "acme".into(), unknown_index, indexes: vec![index("default"), index("payments")] });
        Arc::new(Tenancy::new(tenants, "central").unwrap())
    }

    pub fn principal(id: &str, kind: Kind, tenant: Option<&str>, key: &str) -> PrincipalConfig {
        PrincipalConfig { id: id.into(), kind, tenant: tenant.map(str::to_string), indexes: None, key: Some(key.into()), key_sha256: None }
    }

    /// A verifier that accepts `signer`'s tokens, with the key set held in memory.
    pub fn with_broker(builder: Builder, signer: &Signer) -> Builder {
        let jwt = Jwt::new(ISSUER, "logless", 300, None, None);
        jwt.keys.write().unwrap().keys = parse_jwks(signer.jwks().as_bytes()).unwrap();
        Builder { jwt: Some(jwt), ..builder }
    }
}

#[cfg(test)]
mod route_tests;

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::model::Severity;

    /// `session-broker/docs/verifier/vectors.json` at commit 2e59276, copied unchanged: the broker's
    /// own tokens and the verdict each must get. Regenerated there, never edited here.
    const VECTORS: &str = include_str!("../testdata/session-broker-vectors.json");

    fn vector_verifier(doc: &serde_json::Value) -> Jwt {
        let jwt = Jwt::new(doc["issuer"].as_str().unwrap(), doc["audience"].as_str().unwrap(), doc["clock_allowance_secs"].as_u64().unwrap(), None, None);
        jwt.keys.write().unwrap().keys = parse_jwks(doc["jwks"].to_string().as_bytes()).unwrap();
        jwt
    }

    #[test]
    fn every_session_broker_vector_gets_its_verdict_and_its_reason() {
        let doc: serde_json::Value = serde_json::from_str(VECTORS).unwrap();
        let jwt = vector_verifier(&doc);
        let cases = doc["cases"].as_array().unwrap();
        assert_eq!(11, cases.len(), "V1 (5 cases), V2 (3) and V3 (3)");
        for case in cases {
            let id = case["id"].as_str().unwrap();
            let got = jwt.verify_at(case["token"].as_str().unwrap(), case["verify_at"].as_u64().unwrap());
            match case["expect"]["result"].as_str().unwrap() {
                "accept" => {
                    let claims = got.unwrap_or_else(|e| panic!("{id} must be accepted, was refused: {}", e.as_str()));
                    let want = &case["payload"];
                    assert_eq!(claims.tenant, want["tenant"].as_str().unwrap(), "{id}");
                    assert_eq!(claims.kind.as_str(), want["kind"].as_str().unwrap(), "{id}");
                    assert_eq!(claims.sub, want["sub"].as_str().unwrap(), "{id}");
                    assert_eq!(claims.name.as_deref(), want.get("name").and_then(|n| n.as_str()), "{id}");
                    assert_eq!(claims.exp, want["exp"].as_u64().unwrap(), "{id}");
                }
                _ => assert_eq!(got.map_err(Refusal::as_str), Err(case["expect"]["reason"].as_str().unwrap()), "{id}"),
            }
        }
    }

    #[test]
    fn the_vector_claims_become_a_principal() {
        let doc: serde_json::Value = serde_json::from_str(VECTORS).unwrap();
        let verifier = Builder { jwt: Some(vector_verifier(&doc)), ..Builder::new(two_tenants(UnknownIndex::Default)) }.build();
        let case = |id: &str| doc["cases"].as_array().unwrap().iter().find(|c| c["id"] == id).unwrap().clone();
        let machine = case("V1.accept");
        let p = verifier.verify_at(Listener::Peer, machine["token"].as_str().unwrap(), machine["verify_at"].as_u64().unwrap()).unwrap();
        assert_eq!((p.id.as_str(), p.kind, p.tenant.as_str(), p.source), ("edge-1", Kind::Ingest, "acme", Source::Token));
        assert_eq!(p.indexes, Indexes::Only(vec!["default".into()]));
        assert_eq!((p.scopes.clone(), p.expires_unix_secs), (vec!["ingest".to_string()], Some(1_790_086_400)));
        let user = case("V3.accept");
        let p = verifier.verify_at(Listener::Peer, user["token"].as_str().unwrap(), user["verify_at"].as_u64().unwrap()).unwrap();
        assert_eq!((p.kind, p.tenant.as_str()), (Kind::User, "acme"));
        assert_eq!(p.id, user["payload"]["sub"].as_str().unwrap(), "a user token has no name: the subject is the id");
        assert!(!p.roles.is_empty());
    }

    const NOW: u64 = 1_800_000_000;

    fn broker() -> (Signer, Jwt) {
        let signer = Signer::new(7, "kid-1");
        let jwt = Jwt::new(ISSUER, "logless", 300, None, None);
        jwt.keys.write().unwrap().keys = parse_jwks(signer.jwks().as_bytes()).unwrap();
        (signer, jwt)
    }

    fn verdict(jwt: &Jwt, token: &str, now: u64) -> Result<(), &'static str> {
        jwt.verify_at(token, now).map(|_| ()).map_err(Refusal::as_str)
    }

    #[test]
    fn audience_expiry_and_issue_time_are_checked_with_the_allowance() {
        let (signer, jwt) = broker();
        let good = claims("ingest", "acme", "edge-1", NOW);
        assert_eq!(Ok(()), verdict(&jwt, &signer.sign(&good), NOW));

        let mut other = good.clone();
        other["aud"] = "other-product".into();
        assert_eq!(Err("audience"), verdict(&jwt, &signer.sign(&other), NOW));
        other["aud"] = serde_json::json!(["logless", "other-product"]);
        assert_eq!(Err("audience"), verdict(&jwt, &signer.sign(&other), NOW), "one string, ours, and nothing else");
        other["iss"] = "https://another.broker.test".into();
        assert_eq!(Err("issuer"), verdict(&jwt, &signer.sign(&other), NOW), "the issuer is judged before the audience");

        // exp: 300 s of allowance. Good 299 s past it, refused at 300.
        let exp = good["exp"].as_u64().unwrap();
        assert_eq!(Ok(()), verdict(&jwt, &signer.sign(&good), exp + 299));
        assert_eq!(Err("expired"), verdict(&jwt, &signer.sign(&good), exp + 300));
        assert_eq!(Err("expired"), verdict(&jwt, &signer.sign(&good), exp + 86_400));

        // iat: the same allowance the other way.
        let mut future = good.clone();
        future["iat"] = (NOW + 300).into();
        assert_eq!(Ok(()), verdict(&jwt, &signer.sign(&future), NOW));
        future["iat"] = (NOW + 301).into();
        assert_eq!(Err("not_yet_valid"), verdict(&jwt, &signer.sign(&future), NOW));
    }

    #[test]
    fn the_token_never_chooses_the_algorithm() {
        let (signer, jwt) = broker();
        let good = claims("ingest", "acme", "edge-1", NOW);
        let payload = b64(good.to_string().as_bytes());

        // alg none, with and without a signature part.
        let none = b64(serde_json::json!({"alg": "none", "typ": "JWT", "kid": signer.kid}).to_string().as_bytes());
        assert_eq!(Err("alg"), verdict(&jwt, &format!("{none}.{payload}."), NOW));
        assert_eq!(Err("alg"), verdict(&jwt, &format!("{none}.{payload}.AAAA"), NOW));
        assert_eq!(Err("malformed"), verdict(&jwt, &format!("{none}.{payload}"), NOW), "two parts is not a token");

        // HS256 with the public key as the secret: the classic confusion. A correct MAC is still refused.
        use hmac::Mac;
        let head = b64(serde_json::json!({"alg": "HS256", "typ": "JWT", "kid": signer.kid}).to_string().as_bytes());
        let mut mac = hmac::Hmac::<Sha256>::new_from_slice(&signer.public()).unwrap();
        mac.update(format!("{head}.{payload}").as_bytes());
        let forged = format!("{head}.{payload}.{}", b64(&mac.finalize().into_bytes()));
        assert_eq!(Err("alg"), verdict(&jwt, &forged, NOW));
        // And with the base64url text of the key as the secret, which is what a JWKS reader holds.
        let mut mac = hmac::Hmac::<Sha256>::new_from_slice(b64(&signer.public()).as_bytes()).unwrap();
        mac.update(format!("{head}.{payload}").as_bytes());
        assert_eq!(Err("alg"), verdict(&jwt, &format!("{head}.{payload}.{}", b64(&mac.finalize().into_bytes())), NOW));

        for (header, want) in [
            (serde_json::json!({"typ": "JWT", "kid": signer.kid}), "alg"),
            (serde_json::json!({"alg": "eddsa", "typ": "JWT", "kid": signer.kid}), "alg"),
            (serde_json::json!({"alg": ["EdDSA"], "typ": "JWT", "kid": signer.kid}), "alg"),
            (serde_json::json!({"alg": "EdDSA", "typ": "JWT"}), "malformed"),
            (serde_json::json!({"alg": "EdDSA", "typ": "at+jwt", "kid": signer.kid}), "malformed"),
            (serde_json::json!({"alg": "EdDSA", "typ": "JWT", "kid": signer.kid, "crit": ["x"]}), "malformed"),
        ] {
            assert_eq!(Err(want), verdict(&jwt, &signer.sign_parts(&header, &good), NOW), "{header}");
        }
        // typ may be left out.
        assert_eq!(Ok(()), verdict(&jwt, &signer.sign_parts(&serde_json::json!({"alg": "EdDSA", "kid": signer.kid}), &good), NOW));
    }

    #[test]
    fn a_changed_or_padded_token_fails_its_signature_and_missing_claims_are_named() {
        let (signer, jwt) = broker();
        let good = claims("ingest", "acme", "edge-1", NOW);
        let token = signer.sign(&good);
        let parts: Vec<&str> = token.split('.').collect();

        let mut other = good.clone();
        other["tenant"] = "globex".into();
        let swapped = format!("{}.{}.{}", parts[0], b64(other.to_string().as_bytes()), parts[2]);
        assert_eq!(Err("signature"), verdict(&jwt, &swapped, NOW), "another tenant under the old signature");
        assert_eq!(Err("signature"), verdict(&jwt, &format!("{}.{}.{}", parts[0], parts[1], &parts[2][..parts[2].len() - 4]), NOW));
        assert_eq!(Err("signature"), verdict(&jwt, &format!("{}=", token), NOW), "padding is not base64url");
        let stranger = Signer::new(9, "kid-1");
        assert_eq!(Err("signature"), verdict(&jwt, &stranger.sign(&good), NOW), "the right kid from the wrong key");

        for claim in ["iss", "sub", "aud", "exp", "iat", "jti", "tenant", "kind"] {
            let mut missing = good.clone();
            missing.as_object_mut().unwrap().remove(claim);
            assert_eq!(Err("claims"), verdict(&jwt, &signer.sign(&missing), NOW), "without {claim}");
        }
        for (claim, value) in [("kind", serde_json::json!("root")), ("tenant", serde_json::json!("../etc")), ("exp", serde_json::json!("soon")), ("indexes", serde_json::json!("*"))] {
            let mut bad = good.clone();
            bad[claim] = value;
            assert_eq!(Err("claims"), verdict(&jwt, &signer.sign(&bad), NOW), "{claim}");
        }
        for junk in ["", "..", "a.b.c", "eyJ.eyJ.sig", "not a token"] {
            assert!(verdict(&jwt, junk, NOW).is_err(), "{junk:?}");
        }
    }

    /// An HTTP server that answers every request with `body` (and counts them), or closes without
    /// answering once `down` is set: a broker that can be taken away.
    struct FakeBroker {
        url: String,
        hits: Arc<AtomicU64>,
        body: Arc<Mutex<String>>,
        down: Arc<AtomicBool>,
    }

    impl FakeBroker {
        fn start(jwks: String) -> Self {
            use std::io::{Read, Write};
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}/auth/jwks.json", listener.local_addr().unwrap());
            let (hits, body, down) = (Arc::new(AtomicU64::new(0)), Arc::new(Mutex::new(jwks)), Arc::new(AtomicBool::new(false)));
            let (h, b, d) = (Arc::clone(&hits), Arc::clone(&body), Arc::clone(&down));
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { return };
                    h.fetch_add(1, Ordering::SeqCst);
                    if d.load(Ordering::SeqCst) {
                        continue;
                    }
                    let mut buf = [0u8; 2048];
                    let _ = stream.read(&mut buf);
                    let body = b.lock().unwrap().clone();
                    let _ = write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nETag: \"v{}\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len(), body.len());
                }
            });
            Self { url, hits, body, down }
        }
    }

    #[test]
    fn keys_are_fetched_cached_on_disk_and_kept_while_the_issuer_is_unreachable() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = tmp.path().join(JWKS_CACHE_FILE);
        let first = Signer::new(1, "kid-1");
        let broker = FakeBroker::start(first.jwks());
        let jwt = Jwt::new(ISSUER, "logless", 300, Some(KeySource::Url(broker.url.clone())), Some(cache.clone()));
        let token = first.sign(&claims("ingest", "acme", "edge-1", NOW));

        // Nothing fetched yet: the unknown kid triggers the one refresh, and the token then verifies.
        assert_eq!(Ok(()), verdict(&jwt, &token, NOW));
        assert_eq!(1, broker.hits.load(Ordering::SeqCst));
        assert!(cache.exists(), "the set is saved for the next start");
        assert!(!std::fs::read_to_string(&cache).unwrap().contains("\"d\""), "public keys only");

        // The broker rotates. A token under the new key is unknown; a refresh is allowed once per 60 s.
        let second = Signer::new(2, "kid-2");
        *broker.body.lock().unwrap() = serde_json::json!({"keys": [serde_json::from_str::<serde_json::Value>(&first.jwks()).unwrap()["keys"][0], serde_json::from_str::<serde_json::Value>(&second.jwks()).unwrap()["keys"][0]]}).to_string();
        let rotated = second.sign(&claims("ingest", "acme", "edge-1", NOW));
        assert_eq!(Err("unknown_kid"), verdict(&jwt, &rotated, NOW + 30), "inside the 60 s: no second fetch");
        assert_eq!(1, broker.hits.load(Ordering::SeqCst));
        assert_eq!(Ok(()), verdict(&jwt, &rotated, NOW + 61), "past it: fetched, and the new key is known");
        assert_eq!(2, broker.hits.load(Ordering::SeqCst));
        assert_eq!(Ok(()), verdict(&jwt, &token, NOW + 61), "the old key is still published and still good");

        // The broker goes away. Known keys keep verifying, with no time limit; an unknown kid is refused
        // after one attempt, and the failed refresh drops nothing.
        broker.down.store(true, Ordering::SeqCst);
        let stranger = Signer::new(3, "kid-3").sign(&claims("ingest", "acme", "edge-1", NOW));
        assert_eq!(Err("unknown_kid"), verdict(&jwt, &stranger, NOW + 200));
        assert_eq!(3, broker.hits.load(Ordering::SeqCst), "one attempt");
        assert_eq!(Err("unknown_kid"), verdict(&jwt, &stranger, NOW + 210));
        assert_eq!(3, broker.hits.load(Ordering::SeqCst), "and no more for a minute");
        assert!(!jwt.refresh(NOW + 4000), "an hourly refresh fails too");
        let far = NOW + 365 * 86_400;
        let mut long_lived = claims("ingest", "acme", "edge-1", NOW);
        long_lived["exp"] = (far + 3600).into();
        assert_eq!(Ok(()), verdict(&jwt, &first.sign(&long_lived), far), "a year without the broker: the cached key still verifies");
        assert_eq!(Ok(()), verdict(&jwt, &second.sign(&long_lived), far));
        let status = jwt.status(far);
        assert_eq!(status["key_ids"], serde_json::json!(["kid-1", "kid-2"]));
        assert_eq!(status["key_set_age_secs"], serde_json::json!(far - (NOW + 61)), "the age is in status");
        assert!(status["last_refresh_error"].is_string() && status["refresh_failures"].as_u64().unwrap() >= 2);

        // A restart with the broker still down starts from the file.
        let restarted = Jwt::new(ISSUER, "logless", 300, Some(KeySource::Url(broker.url.clone())), Some(cache.clone()));
        restarted.load_cache();
        assert_eq!(Ok(()), verdict(&restarted, &second.sign(&long_lived), far));
        // A cache written for another issuer is not used.
        let elsewhere = Jwt::new("https://other.test", "logless", 300, None, Some(cache));
        elsewhere.load_cache();
        assert!(elsewhere.keys.read().unwrap().keys.is_empty());
    }

    #[test]
    fn a_key_set_that_is_empty_or_not_a_key_set_never_replaces_the_cached_one() {
        let signer = Signer::new(1, "kid-1");
        let broker = FakeBroker::start(signer.jwks());
        let jwt = Jwt::new(ISSUER, "logless", 300, Some(KeySource::Url(broker.url.clone())), None);
        assert!(jwt.refresh(NOW));
        for bad in [r#"{"keys":[]}"#.to_string(), "<html>bad gateway</html>".to_string(), r#"{"keys":[{"kty":"RSA","kid":"r","n":"x","e":"AQAB"}]}"#.to_string()] {
            *broker.body.lock().unwrap() = bad.clone();
            assert!(!jwt.refresh(NOW + 10), "{bad}");
            assert_eq!(Ok(()), verdict(&jwt, &signer.sign(&claims("ingest", "acme", "edge-1", NOW)), NOW + 10), "{bad}");
        }
    }

    #[test]
    fn a_key_set_file_is_the_only_source_when_given_and_without_auth_nothing_is_fetched() {
        let tmp = tempfile::tempdir().unwrap();
        let signer = Signer::new(4, "kid-file");
        let file = tmp.path().join("jwks.json");
        std::fs::write(&file, signer.jwks()).unwrap();
        // The URL would fail the test if it were asked: it is a closed port.
        let config = AuthConfig { issuer: Some(ISSUER.into()), jwks_url: Some("http://127.0.0.1:1/auth/jwks.json".into()), jwks_file: Some(file), ..AuthConfig::default() };
        let verifier = Builder::new(two_tenants(UnknownIndex::Default)).broker(&config, Some(tmp.path())).unwrap().build();
        let token = signer.sign(&claims("ingest", "acme", "edge-1", crate::now_unix_secs()));
        assert_eq!("edge-1", verifier.verify(Listener::Peer, &token).unwrap().id);
        assert_eq!(verifier.status()["broker"]["refresh_failures"], 0);

        // [auth] unset: a token is just an unknown key, there is no refresh thread and no broker in status.
        let plain = Builder::new(two_tenants(UnknownIndex::Default)).broker(&AuthConfig::default(), Some(tmp.path())).unwrap().build();
        assert_eq!(Err(Refusal::UnknownKey), plain.verify(Listener::Peer, &token).map(|_| ()));
        assert!(plain.start_refresh().is_none());
        assert!(plain.status()["broker"].is_null());
    }

    const KEY_A: &str = "key-a-0123456789-abcdefghijkl";
    const KEY_B: &str = "key-b-0123456789-abcdefghijkl";

    #[test]
    fn a_static_key_is_tried_first_and_may_be_configured_hashed() {
        let hashed = PrincipalConfig { key: None, key_sha256: Some(digest(KEY_B).iter().map(|b| format!("{b:02x}")).collect()), ..principal("acme-hashed", Kind::Ingest, Some("acme"), "") };
        let signer = Signer::new(5, "kid-1");
        let builder = Builder::new(two_tenants(UnknownIndex::Default)).principals(&[principal("acme-edge", Kind::Node, Some("acme"), KEY_A), hashed]).unwrap();
        let verifier = with_broker(builder, &signer).build();

        let p = verifier.verify(Listener::Hec, KEY_A).unwrap();
        assert_eq!((p.id.as_str(), p.kind, p.tenant.as_str(), p.source, p.expires_unix_secs), ("acme-edge", Kind::Node, "acme", Source::Static, None));
        assert_eq!("acme-hashed", verifier.verify(Listener::Peer, KEY_B).unwrap().id);
        for near in [&KEY_A[..KEY_A.len() - 1], &format!("{KEY_A}x"), &KEY_A.to_uppercase(), "", " "] {
            assert_eq!(Err(Refusal::UnknownKey), verifier.verify(Listener::Peer, near).map(|_| ()), "{near:?}");
        }
        // A token for a tenant this instance has, and one for a tenant it does not.
        let now = crate::now_unix_secs();
        assert_eq!("acme", verifier.verify(Listener::Peer, &signer.sign(&claims("ingest", "acme", "edge-9", now))).unwrap().tenant);
        assert_eq!(Err(Refusal::UnknownTenant), verifier.verify(Listener::Peer, &signer.sign(&claims("ingest", "globex", "edge-9", now))).map(|_| ()));
        // Nothing of a key is in status or in a logged config.
        let shown = format!("{}{:?}", verifier.status(), principal("x", Kind::Ingest, None, KEY_A));
        assert!(!shown.contains(KEY_A) && !shown.contains(KEY_B), "{shown}");
        assert!(shown.contains("acme-edge"));
    }

    #[test]
    fn principals_that_cannot_work_are_refused_at_load() {
        let tenancy = two_tenants(UnknownIndex::Default);
        let load = |list: &[PrincipalConfig]| Builder::new(Arc::clone(&tenancy)).principals(list).err().map(|e| e.to_string());
        let ok = principal("edge-1", Kind::Node, Some("acme"), KEY_A);
        assert_eq!(None, load(std::slice::from_ref(&ok)));
        assert_eq!(None, load(&[principal("local", Kind::Ingest, None, KEY_A)]), "no tenant is the instance's own");
        let cases: Vec<(&str, PrincipalConfig)> = vec![
            ("no [[tenants]] entry", principal("edge-1", Kind::Node, Some("globex"), KEY_A)),
            ("shorter than", principal("edge-1", Kind::Node, Some("acme"), "short")),
            ("exactly one of", PrincipalConfig { key: None, ..ok.clone() }),
            ("exactly one of", PrincipalConfig { key_sha256: Some("00".repeat(32)), ..ok.clone() }),
            ("64 hex digits", PrincipalConfig { key: None, key_sha256: Some("abc".into()), ..ok.clone() }),
            ("does not have", PrincipalConfig { indexes: Some(vec!["nope".into()]), ..ok.clone() }),
            ("not a key in this file", principal("alice", Kind::User, Some("acme"), KEY_A)),
            ("must be 1 to 64", principal("../x", Kind::Node, Some("acme"), KEY_A)),
            ("a reader needs key", PrincipalConfig { key: None, key_sha256: Some("00".repeat(32)), ..principal("ro", Kind::Reader, Some("acme"), "") }),
        ];
        for (want, bad) in cases {
            let got = load(std::slice::from_ref(&bad)).unwrap_or_default();
            assert!(got.contains(want), "{bad:?}: {got}");
            assert!(!got.contains(KEY_A), "{got}");
        }
        assert!(load(&[ok.clone(), principal("edge-2", Kind::Node, Some("acme"), KEY_A)]).unwrap().contains("same key"));
        assert!(load(&[ok.clone(), principal("EDGE-1", Kind::Ingest, Some("acme"), KEY_B)]).unwrap().contains("used twice"));
    }

    #[test]
    fn an_older_credential_is_a_principal_of_the_own_tenant_at_its_own_listener_only() {
        let peer = crate::peer::PeerConfig {
            tokens: vec!["shared-ingest".into()],
            nodes: vec![crate::peer::PeerNode { id: "edge-1".into(), token: "node-secret".into() }],
            parts: crate::parts::PartsConfig { replication_tokens: vec!["replica-secret".into()], ..Default::default() },
            ..Default::default()
        };
        let hec = crate::hec::HecConfig { tokens: vec!["hec-secret".into()], ..Default::default() };
        let sentry = crate::sentry::SentryConfig { allowed_keys: vec!["public-dsn-key".into()], ..Default::default() };
        let admin = "admin-secret".to_string();
        let v = Builder::new(two_tenants(UnknownIndex::Default)).peer(&peer).hec(&hec).sentry(&sentry).admin(Some(&admin)).s3("AKID", "s3-secret").build();
        let at = |listener, key: &str| v.verify(listener, key).map(|p| (p.id.clone(), p.kind, p.tenant.clone())).map_err(Refusal::as_str);
        let own = |id: &str, kind| Ok((id.to_string(), kind, "default".to_string()));
        assert_eq!(own("peer-token-1", Kind::Ingest), at(Listener::Peer, "shared-ingest"));
        assert_eq!(own("edge-1", Kind::Node), at(Listener::Peer, "node-secret"));
        assert_eq!(own("replica-token-1", Kind::Replica), at(Listener::Peer, "replica-secret"));
        assert_eq!(own("hec-token-1", Kind::Ingest), at(Listener::Hec, "hec-secret"));
        assert_eq!(own("sentry-key-1", Kind::Ingest), at(Listener::Sentry, "public-dsn-key"));
        assert_eq!(own("admin-token-1", Kind::Admin), at(Listener::Admin, "admin-secret"));
        // A Sentry key is public: it opens the Sentry proxy and nothing else. The same for the others.
        for (key, home) in [("public-dsn-key", Listener::Sentry), ("hec-secret", Listener::Hec), ("shared-ingest", Listener::Peer), ("admin-secret", Listener::Admin)] {
            for listener in [Listener::Peer, Listener::Hec, Listener::Otlp, Listener::Sentry, Listener::Admin, Listener::S3] {
                assert_eq!(listener == home, at(listener, key).is_ok(), "{key} at {}", listener.as_str());
            }
        }
        // The S3 pair is a reader, found by its access key id, never by presenting the secret as a bearer.
        assert_eq!(Err("unknown_key"), at(Listener::S3, "s3-secret"));
        let readers: Vec<_> = v.readers().map(|(p, _)| (p.id.clone(), p.kind)).collect();
        assert_eq!(readers, [("AKID".to_string(), Kind::Reader)]);
        assert!(v.can_authenticate(Listener::Peer, Op::Ingest) && v.can_authenticate(Listener::Peer, Op::Replica) && v.can_authenticate(Listener::S3, Op::Read));
        assert!(!v.can_authenticate(Listener::Otlp, Op::Ingest), "nothing configured is good at the OTLP listener");
    }

    #[test]
    fn what_each_kind_may_do() {
        let table = [
            (Kind::Ingest, [true, false, false, false, false, false]),
            (Kind::Node, [true, true, false, false, false, false]),
            (Kind::Replica, [false, false, true, false, false, false]),
            (Kind::Admin, [false, false, false, true, false, false]),
            (Kind::Reader, [false, false, false, false, true, false]),
            (Kind::User, [false, false, false, false, false, true]),
        ];
        for (kind, row) in table {
            for (op, allowed) in [Op::Ingest, Op::Parts, Op::Replica, Op::Admin, Op::Read, Op::S3Credentials].into_iter().zip(row) {
                assert_eq!(allowed, kind.may(op), "{} {op:?}", kind.as_str());
            }
        }
    }

    #[test]
    fn authorize_decides_open_and_closed_listeners_and_counts_refusals_by_reason() {
        let signer = Signer::new(6, "kid-1");
        let builder = Builder::new(two_tenants(UnknownIndex::Default)).principals(&[principal("acme-edge", Kind::Node, Some("acme"), KEY_A), principal("ops", Kind::Admin, None, KEY_B)]).unwrap();
        let v = with_broker(builder, &signer).build();
        let now = crate::now_unix_secs();
        let expired = {
            let mut c = claims("ingest", "acme", "edge-9", now);
            c["exp"] = (now - 4000).into();
            c["iat"] = (now - 8000).into();
            signer.sign(&c)
        };
        let who = |closed, presented: Option<&str>| v.authorize(Listener::Peer, Op::Ingest, closed, presented).map(|p| p.id.clone()).map_err(Refusal::as_str);

        // Closed: a credential is required and must be good for the route.
        assert_eq!(Err("missing"), who(true, None));
        assert_eq!(Err("unknown_key"), who(true, Some("nope")));
        assert_eq!(Ok("acme-edge".to_string()), who(true, Some(KEY_A)));
        assert_eq!(Err("kind"), who(true, Some(KEY_B)), "an admin key does not post records");
        assert_eq!(Err("expired"), who(true, Some(&expired)));
        assert_eq!(Err("unknown_key"), who(true, Some(&"a".repeat(MAX_TOKEN_LEN + 1))));
        // Open: nothing, or a value this instance does not know, is the own tenant as before...
        assert_eq!(Ok("anonymous".to_string()), who(false, None));
        assert_eq!(Ok("anonymous".to_string()), who(false, Some("whatever-the-sender-had")));
        // ...a known key is still its principal, and a token that fails as a token is still refused.
        assert_eq!(Ok("acme-edge".to_string()), who(false, Some(KEY_A)));
        assert_eq!(Err("expired"), who(false, Some(&expired)));
        assert_eq!(Err("kind"), who(false, Some(KEY_B)));

        let status = v.status();
        assert_eq!((status["refused"]["missing"].as_u64(), status["refused"]["unknown_key"].as_u64(), status["refused"]["kind"].as_u64(), status["refused"]["expired"].as_u64()), (Some(1), Some(2), Some(2), Some(2)));
        assert_eq!(status["refused"]["signature"], 0);
        let ops = status["seen"].as_array().unwrap().iter().find(|s| s["principal"] == "default/ops").unwrap();
        assert_eq!(ops["refused"], 2);
    }

    fn rec(attrs: &[(&str, &str)]) -> LogRecord {
        let mut r = LogRecord::new(1, Severity::INFO, "x");
        r.attributes = attrs.iter().map(|(k, v)| Attr { key: (*k).into(), value: AttrValue::Str((*v).into()) }).collect();
        r
    }

    fn attrs(r: &LogRecord) -> Vec<(String, String)> {
        r.attributes.iter().map(|a| (a.key.clone(), match &a.value { AttrValue::Str(s) => s.clone(), other => format!("{other:?}") })).collect()
    }

    fn pair(k: &str, v: &str) -> (String, String) {
        (k.to_string(), v.to_string())
    }

    #[test]
    fn admit_takes_the_tenant_from_the_principal_and_never_from_the_record() {
        let v = Builder::new(two_tenants(UnknownIndex::Default)).principals(&[principal("acme-edge", Kind::Node, Some("acme"), KEY_A), principal("local", Kind::Ingest, None, KEY_B)]).unwrap().build();
        let acme = v.verify(Listener::Peer, KEY_A).unwrap();
        let local = v.verify(Listener::Peer, KEY_B).unwrap();

        // Another tenant's principal: every record is stamped with its tenant; a non-default index is kept.
        let mut batch = vec![rec(&[("k", "v")]), rec(&[(ATTR_INDEX, "payments")]), rec(&[(ATTR_TENANT, "acme"), (ATTR_INDEX, "default")])];
        assert_eq!(Ok(Admitted::default()), v.admit(&acme, &mut batch, ATTR_INDEX, false));
        assert_eq!(attrs(&batch[0]), [pair("k", "v"), pair(ATTR_TENANT, "acme")]);
        assert_eq!(attrs(&batch[1]), [pair(ATTR_INDEX, "payments"), pair(ATTR_TENANT, "acme")]);
        assert_eq!(attrs(&batch[2]), [pair(ATTR_TENANT, "acme")]);

        // The own tenant: nothing is added, so a single-tenant install writes what it always wrote.
        let mut batch = vec![rec(&[("k", "v")])];
        v.admit(&local, &mut batch, ATTR_INDEX, false).unwrap();
        assert_eq!(attrs(&batch[0]), [pair("k", "v")]);

        // A record that says it is another tenant's turns the batch away, whoever sends it; nothing is changed.
        for (principal, claimed) in [(&acme, "default"), (&local, "acme"), (&v.anonymous(), "acme")] {
            let mut batch = vec![rec(&[]), rec(&[(ATTR_TENANT, claimed)])];
            assert_eq!(Err(NotAdmitted::WrongTenant), v.admit(principal, &mut batch, ATTR_INDEX, false), "{} claiming {claimed}", principal.id);
            assert_eq!(2, batch.len());
        }
        let mut forged = vec![LogRecord::new(1, Severity::INFO, "x")];
        forged[0].attributes.push(Attr { key: ATTR_TENANT.into(), value: AttrValue::I64(7) });
        assert_eq!(Err(NotAdmitted::WrongTenant), v.admit(&acme, &mut forged, ATTR_INDEX, false));
    }

    #[test]
    fn admit_applies_the_index_allow_list_and_the_tenants_unknown_index_rule() {
        let only = |indexes: &[&str]| PrincipalConfig { indexes: Some(indexes.iter().map(|i| i.to_string()).collect()), ..principal("pay", Kind::Ingest, Some("acme"), KEY_A) };
        let asked = |tenancy: Arc<Tenancy>, p: PrincipalConfig, index: Option<&str>, whole: bool| {
            let v = Builder::new(tenancy).principals(&[p]).unwrap().build();
            let who = v.verify(Listener::Hec, KEY_A).unwrap();
            let mut batch = vec![rec(&index.map(|i| vec![("index", i)]).unwrap_or_default())];
            let out = v.admit(&who, &mut batch, "index", whole);
            let landed = batch.first().map(|r| r.attributes.iter().find(|a| a.key == ATTR_INDEX).map(|a| match &a.value { AttrValue::Str(s) => s.clone(), _ => String::new() }).unwrap_or_else(|| "default".into()));
            (out, landed.filter(|_| out.is_ok()), v.status()["seen"][0].clone())
        };
        let lenient = || two_tenants(UnknownIndex::Default);
        let strict = || two_tenants(UnknownIndex::Refuse);
        let stored = |index: &str, defaulted: u64| (Ok(Admitted { defaulted, refused: 0 }), Some(index.to_string()));
        let two = |r: (Result<Admitted, NotAdmitted>, Option<String>, serde_json::Value)| (r.0, r.1);

        // Every index: a known one is kept, an unknown one follows the tenant's rule.
        let all = principal("pay", Kind::Ingest, Some("acme"), KEY_A);
        assert_eq!(stored("default", 0), two(asked(lenient(), all.clone(), None, false)));
        assert_eq!(stored("payments", 0), two(asked(lenient(), all.clone(), Some("payments"), false)));
        assert_eq!(stored("default", 1), two(asked(lenient(), all.clone(), Some("nope"), false)));
        assert_eq!((Ok(Admitted { defaulted: 0, refused: 1 }), None), two(asked(strict(), all.clone(), Some("nope"), false)), "refused: taken out of the batch");
        assert_eq!((Err(NotAdmitted::Index), None), two(asked(strict(), all, Some("nope"), true)), "refused, all or nothing: HEC's code 7");

        // An allow-list: an index the tenant has but the principal may not use is treated as unknown.
        assert_eq!(stored("default", 0), two(asked(lenient(), only(&["default"]), Some("default"), false)));
        assert_eq!(stored("default", 1), two(asked(lenient(), only(&["default"]), Some("payments"), false)));
        let (out, landed, seen) = asked(strict(), only(&["default"]), Some("payments"), false);
        assert_eq!((out, landed), (Ok(Admitted { defaulted: 0, refused: 1 }), None));
        assert_eq!((seen["principal"].as_str(), seen["index_refused"].as_u64(), seen["records"].as_u64()), (Some("acme/pay"), Some(1), Some(0)), "counted per principal");
        // A principal that may not write the tenant's default index has its own first index as its default.
        assert_eq!(stored("payments", 0), two(asked(lenient(), only(&["payments"]), None, false)));
        assert_eq!(stored("payments", 1), two(asked(lenient(), only(&["payments"]), Some("default"), false)));
        let (_, _, seen) = asked(lenient(), only(&["payments"]), Some("default"), false);
        assert_eq!(seen["index_defaulted"], 1);
        // `*` in the list is every index.
        assert_eq!(stored("payments", 0), two(asked(strict(), only(&["*"]), Some("payments"), false)));
    }

    #[test]
    fn check_config_ties_principals_and_auth_to_the_tenants() {
        let tenancy = two_tenants(UnknownIndex::Default);
        assert!(check_config(&AuthConfig::default(), &[], &tenancy).is_ok());
        assert!(check_config(&AuthConfig::default(), &[principal("x", Kind::Ingest, Some("nowhere"), KEY_A)], &tenancy).is_err());
        assert!(check_config(&AuthConfig { jwks_file: Some("/x".into()), ..AuthConfig::default() }, &[], &tenancy).unwrap_err().contains("without issuer"));
        let toml_form: crate::Config = toml::from_str(&format!(
            "[storage]\ndata_dir = \"/tmp/x\"\n[auth]\nissuer = \"https://broker.example.test\"\n[[tenants]]\nid = \"default\"\n[[tenants]]\nid = \"acme\"\n[[principals]]\nid = \"acme-edge\"\nkind = \"node\"\ntenant = \"acme\"\nindexes = [\"default\"]\nkey = \"{KEY_A}\"\n"
        ))
        .unwrap();
        toml_form.validate().unwrap();
        assert_eq!((toml_form.auth.audience.as_str(), toml_form.auth.clock_skew_secs), ("logless", 300));
        assert!(!format!("{toml_form:?}").contains(KEY_A), "a logged config never carries a key");
        let unknown: Result<crate::Config, _> = toml::from_str("[storage]\ndata_dir = \"/tmp/x\"\n[[principals]]\nid = \"a\"\nkind = \"ingest\"\nkey = \"k\"\nsecret = 1\n");
        assert!(unknown.is_err(), "unknown keys are refused, as everywhere in this file");
    }
}
