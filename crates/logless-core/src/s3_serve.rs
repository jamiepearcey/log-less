//! Read-only S3-compatible server over the hive store.
//!
//! The store is
//! `tenant=<t>/index=<i>/hour=YYYY-MM-DDTHH/level=<bucket>/node=<writer>/*.parquet`
//! (`crate::partition`; `hour` is the event hour); this exposes it path-style
//! as `/{bucket}/{key}`, the key being that path, so DuckDB (httpfs), Polars
//! and Grafana read it with plain HTTP Range requests instead of log-less
//! forwarding to a vendor. The five directories are hive columns to a reader
//! that asks for them, and one index of one tenant is one prefix. Only `GET`/`HEAD` of objects and ListObjectsV2 exist — there is no
//! write path to harden.
//!
//! It runs on [`crate::bounded_http`], like the admin API and the peer receiver:
//! the request head is capped and has to arrive within a deadline, connections
//! are capped, and a client that stops reading is cut off by the write timeout.
//! The reply streams from the file (a Parquet footer read followed by a few
//! column-chunk ranges must not be buffered), see [`crate::httpd::Streamed`].

use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::bounded_http::{BoundedServer, HeadLimits};
use crate::httpd::{self, Handler, HttpError, Incoming, Limits, Method, Reject, Streamed};
use crate::s3_sigv4::{self, AuthError, Credentials};

const MAX_KEYS_CAP: usize = 1000;
const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
/// Reported for every bucket; clients that sign with another region still verify.
const REGION: &str = "us-east-1";
/// Connections served at once, per configured worker, and never fewer than
/// `MIN_CONNECTIONS`: one thread each. Past that the listener hangs up, so
/// connections that open and say nothing cannot grow the process.
const CONNECTIONS_PER_WORKER: usize = 16;
const MIN_CONNECTIONS: usize = 64;
/// A client has this long to send its whole request. Downloads are not timed
/// as a whole, only each write (see `bounded_http`), so a slow reader is cut
/// off when it stops reading, not when it is merely slow.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// A request here is a `GET`/`HEAD`; no body is ever read, so none is let in.
const MAX_BODY_BYTES: usize = 64 * 1024;

#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct S3ServeConfig {
    /// Opt-in, like every listener here.
    pub enabled: bool,
    pub addr: String,
    /// The only bucket name served; any other is `NoSuchBucket`.
    pub bucket: String,
    pub workers: usize,
    /// Falls back to `LOGLESS_S3_ACCESS_KEY`. **Unset on both sides serves
    /// anonymously** — fine on loopback, refused on any other bind so a
    /// misconfiguration cannot publish the log store to the network.
    pub access_key: Option<String>,
    /// Falls back to `LOGLESS_S3_SECRET_KEY`.
    pub secret_key: Option<String>,
    /// Host header values (port ignored) this server answers to. Empty means:
    /// anonymous mode accepts only loopback names, closing DNS rebinding from a
    /// browser page; with credentials Host is already part of the signature.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    /// A second, read-only bucket: the parts edges uploaded to this instance's own disk
    /// (`[peer.parts]` with the local backend), as `<node>/<sha256>.parquet`. A bucket of its own so a
    /// `**/*.parquet` read of the store never counts an uploaded part's rows as well. Served only when
    /// that backend is on.
    pub parts_bucket: String,
}

impl Default for S3ServeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: "127.0.0.1:9000".to_string(),
            bucket: "logs".to_string(),
            workers: 8,
            access_key: None,
            secret_key: None,
            allowed_hosts: Vec::new(),
            parts_bucket: "parts".to_string(),
        }
    }
}

// Hand-written so a logged config never carries the secret.
impl std::fmt::Debug for S3ServeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ServeConfig")
            .field("enabled", &self.enabled)
            .field("addr", &self.addr)
            .field("bucket", &self.bucket)
            .field("workers", &self.workers)
            .field("access_key", &self.access_key)
            .field("secret_key", &self.secret_key.as_ref().map(|_| "<redacted>"))
            .field("allowed_hosts", &self.allowed_hosts)
            .field("parts_bucket", &self.parts_bucket)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum S3ServeError {
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error(
        "s3 serve has no credentials, which serves the store to anyone; refusing to bind the non-loopback address {addr}"
    )]
    OpenOnPublicAddress { addr: String },
    #[error(
        "s3 serve needs both an access key and a secret key (config or LOGLESS_S3_ACCESS_KEY / LOGLESS_S3_SECRET_KEY) but only the {present} is set"
    )]
    PartialCredentials { present: &'static str },
    #[error("s3 serve parts_bucket is {0:?}, the same name as the store's bucket; give the uploaded parts a bucket of their own")]
    SameBucket(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct S3Stats {
    pub requests: u64,
    pub range_requests: u64,
    pub bytes_served: u64,
}

struct Inner {
    store_dir: PathBuf,
    bucket: String,
    /// The parts bucket and its directory, when uploaded parts are on this disk.
    parts: Option<(String, PathBuf)>,
    creds: Option<Credentials>,
    allowed_hosts: Vec<String>,
    requests: AtomicU64,
    range_requests: AtomicU64,
    bytes_served: AtomicU64,
}

/// A running listener. Dropping it does not stop it; call [`S3Server::shutdown`].
pub struct S3Server {
    server: BoundedServer,
    inner: Arc<Inner>,
}

fn non_empty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.is_empty())
}

/// The store's one key pair, from the config or the environment, when both halves are there.
pub fn configured_credentials(cfg: &S3ServeConfig) -> Option<(String, String)> {
    resolve_credentials(cfg).ok().flatten().map(|c| (c.access_key, c.secret_key))
}

fn resolve_credentials(cfg: &S3ServeConfig) -> Result<Option<Credentials>, S3ServeError> {
    let access = non_empty(cfg.access_key.clone())
        .or_else(|| non_empty(std::env::var("LOGLESS_S3_ACCESS_KEY").ok()));
    let secret = non_empty(cfg.secret_key.clone())
        .or_else(|| non_empty(std::env::var("LOGLESS_S3_SECRET_KEY").ok()));
    match (access, secret) {
        (Some(access_key), Some(secret_key)) => Ok(Some(Credentials { access_key, secret_key })),
        (None, None) => Ok(None),
        (Some(_), None) => Err(S3ServeError::PartialCredentials { present: "access key" }),
        (None, Some(_)) => Err(S3ServeError::PartialCredentials { present: "secret key" }),
    }
}

impl S3Server {
    pub fn start(cfg: &S3ServeConfig, store_dir: PathBuf) -> Result<S3Server, S3ServeError> {
        Self::start_with_parts(cfg, store_dir, None)
    }

    /// As [`S3Server::start`], and serves `parts_dir` (the local parts directory, `crate::parts`) as the
    /// bucket `cfg.parts_bucket`, under the same credentials and the same rules.
    pub fn start_with_parts(cfg: &S3ServeConfig, store_dir: PathBuf, parts_dir: Option<PathBuf>) -> Result<S3Server, S3ServeError> {
        let creds = resolve_credentials(cfg)?;
        if parts_dir.is_some() && cfg.parts_bucket == cfg.bucket {
            return Err(S3ServeError::SameBucket(cfg.bucket.clone()));
        }

        let addrs: Vec<SocketAddr> = cfg
            .addr
            .to_socket_addrs()
            .map_err(|source| HttpError::Resolve { addr: cfg.addr.clone(), source })?
            .collect();
        let resolved = *addrs
            .first()
            .ok_or_else(|| HttpError::NoAddress { addr: cfg.addr.clone() })?;

        if creds.is_none() {
            if !addrs.iter().all(|a| a.ip().is_loopback()) {
                return Err(S3ServeError::OpenOnPublicAddress { addr: cfg.addr.clone() });
            }
            tracing::warn!(addr = %cfg.addr, "s3 serve has no credentials; serving the store anonymously on loopback");
        }

        let inner = Arc::new(Inner {
            store_dir,
            bucket: cfg.bucket.clone(),
            parts: parts_dir.map(|dir| (cfg.parts_bucket.clone(), dir)),
            allowed_hosts: cfg.allowed_hosts.iter().map(|h| h.to_ascii_lowercase()).collect(),
            creds,
            requests: AtomicU64::new(0),
            range_requests: AtomicU64::new(0),
            bytes_served: AtomicU64::new(0),
        });
        let head = HeadLimits {
            max_connections: (cfg.workers * CONNECTIONS_PER_WORKER).max(MIN_CONNECTIONS),
            request_timeout: REQUEST_TIMEOUT,
            ..HeadLimits::default()
        };
        let limits = Limits { max_body_bytes: MAX_BODY_BYTES, max_decompressed_bytes: 0 };
        let server = BoundedServer::start(
            &resolved.to_string(),
            "s3-serve",
            limits,
            head,
            Arc::new(S3Handler(Arc::clone(&inner))),
        )?;
        Ok(S3Server { server, inner })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub fn stats(&self) -> S3Stats {
        S3Stats {
            requests: self.inner.requests.load(Ordering::Relaxed),
            range_requests: self.inner.range_requests.load(Ordering::Relaxed),
            bytes_served: self.inner.bytes_served.load(Ordering::Relaxed),
        }
    }

    /// Stops accepting, then waits (bounded) for in-flight downloads.
    pub fn shutdown(self) {
        self.server.shutdown();
    }
}

struct S3Handler(Arc<Inner>);

impl Handler for S3Handler {
    fn handle(&self, request: Incoming) -> httpd::Reply {
        let inner = &self.0;
        let id = inner.requests.fetch_add(1, Ordering::Relaxed) + 1;
        let mut meta = Meta::default();
        let reply = inner.route(id, request.method_name, request.path, request.query, request.headers(), &mut meta);
        tracing::debug!(method = request.method_name, key = %meta.key, range = %meta.range, status = reply.status, "s3 request");
        reply.into_http(id, inner, request.method == Method::Head)
    }

    /// The Host and signature checks need only the head, so a client without
    /// credentials is answered before anything else about its request matters.
    fn preflight(&self, request: &Incoming) -> Option<httpd::Reply> {
        let inner = &self.0;
        // Counted only when it refuses; an admitted request is counted by `handle`.
        let id = inner.requests.load(Ordering::Relaxed) + 1;
        let denied = inner.gate(id, request.method_name, request.path, request.query, request.headers())?;
        inner.requests.fetch_add(1, Ordering::Relaxed);
        Some(denied.into_http(id, inner, request.method == Method::Head))
    }

    fn reject(&self, reason: Reject) -> httpd::Reply {
        let inner = &self.0;
        let id = inner.requests.fetch_add(1, Ordering::Relaxed) + 1;
        let reply = match reason {
            Reject::TooLarge => Reply::error(id, 413, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed size"),
            Reject::Unreadable(_) => Reply::error(id, 400, "BadRequest", "The request could not be read"),
        };
        reply.into_http(id, inner, false)
    }
}

#[derive(Default)]
struct Meta {
    key: String,
    range: String,
}

enum Body {
    Bytes(Vec<u8>),
    /// A file range, already seeked. `Take` bounds it to exactly `len` bytes.
    Stream { reader: Box<dyn Read + Send>, len: u64 },
    /// `HEAD`: the length is advertised, nothing is sent.
    Head(u64),
}

struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Body,
}

impl Reply {
    fn xml(status: u16, body: String) -> Self {
        Reply {
            status,
            headers: vec![("Content-Type", "application/xml".to_string())],
            body: Body::Bytes(body.into_bytes()),
        }
    }

    fn error(id: u64, status: u16, code: &str, message: &str) -> Self {
        Self::xml(
            status,
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message><RequestId>{id:016x}</RequestId></Error>",
                xml_escape(code),
                xml_escape(message)
            ),
        )
    }

    fn with(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    /// `head`: a `HEAD` reply advertises the length of what a `GET` would send and sends none of it.
    fn into_http(self, id: u64, inner: &Arc<Inner>, head: bool) -> httpd::Reply {
        let reply = self
            .with("x-amz-request-id", format!("{id:016x}"))
            .with("x-amz-id-2", format!("log-less/{id:016x}"));
        let content_type = reply
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        let headers = reply
            .headers
            .into_iter()
            .filter(|(k, _)| !k.eq_ignore_ascii_case("content-type"))
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let (body, stream) = match reply.body {
            Body::Bytes(v) if head => {
                let len = v.len() as u64;
                (Vec::new(), Some(Streamed { reader: None, len }))
            }
            Body::Bytes(v) => (v, None),
            // Counted at the file read, so an aborted download is not
            // credited with bytes that were never read.
            Body::Stream { reader, len } => {
                let counted = Counting { inner: reader, owner: Arc::clone(inner) };
                (Vec::new(), Some(Streamed { reader: Some(Box::new(counted)), len }))
            }
            Body::Head(len) => (Vec::new(), Some(Streamed { reader: None, len })),
        };
        httpd::Reply { status: reply.status, content_type, body, headers, stream }
    }
}

struct Counting {
    inner: Box<dyn Read + Send>,
    owner: Arc<Inner>,
}

impl Read for Counting {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.owner.bytes_served.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

impl Inner {
    fn host_allowed(&self, headers: &[(String, String)]) -> bool {
        if self.allowed_hosts.is_empty() && self.creds.is_some() {
            return true;
        }
        let Some(value) = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("host"))
            .map(|(_, v)| v.trim().to_ascii_lowercase())
        else {
            return false;
        };
        let host = match value.strip_prefix('[') {
            Some(rest) => rest.split(']').next().unwrap_or(""),
            None => value.split(':').next().unwrap_or(""),
        };
        if self.allowed_hosts.is_empty() {
            return host == "localhost"
                || host == "::1"
                || host.parse::<std::net::Ipv4Addr>().is_ok_and(|ip| ip.is_loopback());
        }
        self.allowed_hosts.iter().any(|h| h == host)
    }

    /// Host and credential checks, which come before anything else is looked at.
    fn gate(&self, id: u64, method: &str, raw_path: &str, raw_query: &str, headers: &[(String, String)]) -> Option<Reply> {
        if !self.host_allowed(headers) {
            return Some(Reply::error(id, 403, "AccessDenied", "Invalid Host header"));
        }

        let creds = self.creds.as_ref()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let Err(e) = s3_sigv4::verify(creds, method, raw_path, raw_query, headers, now) else {
            return None;
        };
        Some(match e {
            AuthError::Missing | AuthError::Malformed => Reply::error(id, 403, "AccessDenied", "Access Denied"),
            AuthError::Expired => Reply::error(id, 403, "AccessDenied", "Request has expired"),
            // An unknown key id answers like a bad secret; see `verify`.
            AuthError::UnknownKey | AuthError::BadSignature => Reply::error(
                id,
                403,
                "SignatureDoesNotMatch",
                "The request signature we calculated does not match the signature you provided",
            ),
        })
    }

    fn route(
        &self,
        id: u64,
        method: &str,
        raw_path: &str,
        raw_query: &str,
        headers: &[(String, String)],
        meta: &mut Meta,
    ) -> Reply {
        if let Some(denied) = self.gate(id, method, raw_path, raw_query, headers) {
            return denied;
        }

        let head = match method {
            "GET" => false,
            "HEAD" => true,
            _ => {
                return Reply::error(
                    id,
                    405,
                    "MethodNotAllowed",
                    "The specified method is not allowed against this resource",
                )
                .with("Allow", "GET, HEAD")
            }
        };

        let Some(rest) = raw_path.strip_prefix('/') else {
            return Reply::error(id, 400, "InvalidURI", "Couldn't parse the specified URI");
        };
        if rest.is_empty() {
            return if head { Reply::xml(200, String::new()) } else { self.list_buckets() };
        }
        let has_param = |name: &str| parse_query(raw_query).iter().any(|(k, _)| k == name);

        let (bucket_raw, key_raw) = rest.split_once('/').unwrap_or((rest, ""));
        let (bucket, root) = match percent_decode(bucket_raw, false) {
            Some(b) if b == self.bucket => (self.bucket.as_str(), self.store_dir.as_path()),
            Some(b) if self.parts.as_ref().is_some_and(|(name, _)| *name == b) => {
                let (name, dir) = self.parts.as_ref().expect("checked");
                (name.as_str(), dir.as_path())
            }
            _ => return Reply::error(id, 404, "NoSuchBucket", "The specified bucket does not exist"),
        };
        if key_raw.is_empty() {
            let reply = if head {
                Reply::xml(200, String::new())
            } else if has_param("location") {
                Reply::xml(
                    200,
                    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"{XMLNS}\">{REGION}</LocationConstraint>"),
                )
            } else {
                self.list_objects(id, bucket, root, raw_query, meta)
            };
            return reply.with("x-amz-bucket-region", REGION);
        }
        self.object(id, head, root, key_raw, headers, meta)
    }

    fn no_such_key(id: u64) -> Reply {
        Reply::error(id, 404, "NoSuchKey", "The specified key does not exist.")
    }

    /// Maps a decoded key to a file under `root` (the store, or the parts
    /// directory), or `None` for anything that is not a servable regular file
    /// reachable without a symlink.
    fn resolve(root: &Path, key: &str) -> Option<PathBuf> {
        let segments = safe_segments(key)?;
        if !servable(key) {
            return None;
        }
        let mut path = root.to_path_buf();
        for (i, segment) in segments.iter().enumerate() {
            path.push(segment);
            // `symlink_metadata`, never `metadata`: a link planted in the store
            // must not lead a request out of it.
            let md = fs::symlink_metadata(&path).ok()?;
            let ok = if i + 1 == segments.len() { md.is_file() } else { md.is_dir() };
            if !ok {
                return None;
            }
        }
        Some(path)
    }

    fn object(
        &self,
        id: u64,
        head: bool,
        root: &Path,
        key_raw: &str,
        headers: &[(String, String)],
        meta: &mut Meta,
    ) -> Reply {
        let Some(key) = percent_decode(key_raw, false) else {
            return Self::no_such_key(id);
        };
        meta.key = key.clone();
        let Some(path) = Self::resolve(root, &key) else {
            return Self::no_such_key(id);
        };
        // Open before any header is sent: compaction or retention may have
        // unlinked the file since the listing. Once open, the handle keeps
        // serving even if the file is unlinked mid-download.
        let mut file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Self::no_such_key(id),
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(), "s3 open failed");
                return Reply::error(id, 500, "InternalError", "We encountered an internal error.");
            }
        };
        let md = match file.metadata() {
            Ok(md) if md.is_file() => md,
            _ => return Self::no_such_key(id),
        };
        let len = md.len();
        let mtime = md.modified().unwrap_or(UNIX_EPOCH);
        let etag = etag(len, mtime);

        let mut hs: Vec<(&'static str, String)> = vec![
            ("Content-Type", "application/octet-stream".to_string()),
            ("ETag", etag.clone()),
            ("Last-Modified", http_date(unix_secs(mtime))),
            ("Accept-Ranges", "bytes".to_string()),
        ];

        let mtime_secs = unix_secs(mtime);
        let precondition_failed = || {
            Reply::error(
                id,
                412,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
            )
        };
        let not_modified = |hs: &[(&'static str, String)]| Reply {
            status: 304,
            headers: hs.iter().filter(|(k, _)| *k != "Content-Type").cloned().collect(),
            body: Body::Bytes(Vec::new()),
        };

        // RFC 7232 order. A client pinning an ETag across a compaction rewrite
        // must get 412 rather than silently different bytes.
        if let Some(v) = header(headers, "if-match") {
            if !v.split(',').map(str::trim).any(|t| t == "*" || t == etag) {
                return precondition_failed();
            }
        } else if let Some(since) = header(headers, "if-unmodified-since").and_then(parse_http_date) {
            if mtime_secs > since {
                return precondition_failed();
            }
        }
        if let Some(v) = header(headers, "if-none-match") {
            if v.split(',')
                .map(str::trim)
                .any(|t| t == "*" || t.trim_start_matches("W/") == etag)
            {
                return not_modified(&hs);
            }
        } else if let Some(since) = header(headers, "if-modified-since").and_then(parse_http_date) {
            if mtime_secs <= since {
                return not_modified(&hs);
            }
        }

        // A stale If-Range means the client's cached prefix belongs to another
        // version, so the whole current object is the correct answer.
        let range_current = header(headers, "if-range").is_none_or(|v| {
            let v = v.trim();
            if v.starts_with('"') || v.starts_with("W/") {
                v == etag
            } else {
                parse_http_date(v) == Some(mtime_secs)
            }
        });
        let range_header = header(headers, "range").filter(|_| range_current);
        let (status, start, n) = match range_header.map(|h| parse_range(h, len)) {
            Some(RangeOutcome::Satisfiable(a, b)) => {
                self.range_requests.fetch_add(1, Ordering::Relaxed);
                meta.range = format!("{a}-{b}");
                hs.push(("Content-Range", format!("bytes {a}-{b}/{len}")));
                (206, a, b - a + 1)
            }
            Some(RangeOutcome::Unsatisfiable) => {
                self.range_requests.fetch_add(1, Ordering::Relaxed);
                meta.range = "unsatisfiable".to_string();
                let mut r = Reply::error(
                    id,
                    416,
                    "InvalidRange",
                    "The requested range is not satisfiable",
                )
                .with("Content-Range", format!("bytes */{len}"))
                .with("Accept-Ranges", "bytes");
                r.headers.push(("ETag", etag));
                return r;
            }
            // A malformed or multi-range header is ignored, as S3 does.
            Some(RangeOutcome::Ignore) | None => (200, 0, len),
        };

        let body = if head {
            Body::Head(n)
        } else {
            if start > 0 {
                if let Err(e) = file.seek(SeekFrom::Start(start)) {
                    tracing::warn!(error = %e, "s3 seek failed");
                    return Reply::error(id, 500, "InternalError", "We encountered an internal error.");
                }
            }
            Body::Stream { reader: Box::new(file.take(n)), len: n }
        };
        Reply { status, headers: hs, body }
    }

    fn list_buckets(&self) -> Reply {
        let created = fs::metadata(&self.store_dir)
            .and_then(|m| m.modified())
            .unwrap_or(UNIX_EPOCH);
        let mut buckets = format!("<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>", xml_escape(&self.bucket), iso_date(created));
        if let Some((name, dir)) = &self.parts {
            let created = fs::metadata(dir).and_then(|m| m.modified()).unwrap_or(UNIX_EPOCH);
            buckets.push_str(&format!("<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>", xml_escape(name), iso_date(created)));
        }
        Reply::xml(
            200,
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult xmlns=\"{XMLNS}\"><Owner><ID>log-less</ID><DisplayName>log-less</DisplayName></Owner><Buckets>{buckets}</Buckets></ListAllMyBucketsResult>"
            ),
        )
    }

    fn list_objects(&self, id: u64, bucket: &str, root: &Path, raw_query: &str, meta: &mut Meta) -> Reply {
        let query = parse_query(raw_query);
        let get = |name: &str| query.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());
        let invalid = |msg: &str| Reply::error(id, 400, "InvalidArgument", msg);

        // Absent list-type is ListObjects v1, which older SDKs and s3cmd still send.
        let v2 = match get("list-type") {
            None => false,
            Some("2") => true,
            Some(_) => return invalid("Invalid list-type specified in Request"),
        };
        let url_encode = match get("encoding-type") {
            None => false,
            Some("url") => true,
            Some(_) => return invalid("Invalid Encoding Method specified in Request"),
        };
        let max_keys = match get("max-keys") {
            None => MAX_KEYS_CAP,
            Some(v) => match v.parse::<i64>() {
                Ok(n) if n >= 0 => (n as usize).min(MAX_KEYS_CAP),
                _ => return invalid("Provided max-keys not an integer or within integer range"),
            },
        };
        let prefix = get("prefix").unwrap_or("");
        let delimiter = get("delimiter").filter(|d| !d.is_empty());
        let token = match get("continuation-token").filter(|_| v2) {
            None => None,
            Some(t) => match decode_token(t) {
                Some(k) => Some(k),
                None => return invalid("The continuation token provided is incorrect"),
            },
        };
        // A token resumes exactly where the previous page stopped, so it wins
        // over `start-after`, which only positions the first page.
        let marker = if v2 {
            token.clone().or_else(|| get("start-after").map(str::to_string))
        } else {
            get("marker").map(str::to_string)
        };
        meta.key = format!("list prefix={prefix}");

        let params = ListParams { prefix, delimiter, max_keys, marker };
        let mut walker = Walker { p: &params, out: Vec::new(), truncated: false, last_prefix: None };
        if max_keys > 0 {
            if let Some((dir, base)) = list_base(root, prefix) {
                walker.walk(&dir, &base);
            }
        }

        let enc = |s: &str| if url_encode { url_encode_key(s) } else { s.to_string() };
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"{XMLNS}\"><Name>{}</Name><Prefix>{}</Prefix>",
            xml_escape(bucket),
            xml_escape(&enc(prefix))
        );
        if let Some(d) = delimiter {
            xml.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(&enc(d))));
        }
        if url_encode {
            xml.push_str("<EncodingType>url</EncodingType>");
        }
        if v2 {
            xml.push_str(&format!(
                "<KeyCount>{}</KeyCount><MaxKeys>{max_keys}</MaxKeys><IsTruncated>{}</IsTruncated>",
                walker.out.len(),
                walker.truncated
            ));
            if let Some(t) = get("continuation-token") {
                xml.push_str(&format!("<ContinuationToken>{}</ContinuationToken>", xml_escape(t)));
            }
            if let Some(s) = get("start-after") {
                xml.push_str(&format!("<StartAfter>{}</StartAfter>", xml_escape(&enc(s))));
            }
            if walker.truncated {
                if let Some(last) = walker.out.last() {
                    xml.push_str(&format!(
                        "<NextContinuationToken>{}</NextContinuationToken>",
                        encode_token(last.key())
                    ));
                }
            }
        } else {
            xml.push_str(&format!(
                "<Marker>{}</Marker><MaxKeys>{max_keys}</MaxKeys><IsTruncated>{}</IsTruncated>",
                xml_escape(&enc(get("marker").unwrap_or(""))),
                walker.truncated
            ));
            // Like S3, v1 names the next marker only when a delimiter is in
            // play; otherwise clients resume from the last key they saw.
            if walker.truncated && delimiter.is_some() {
                if let Some(last) = walker.out.last() {
                    xml.push_str(&format!("<NextMarker>{}</NextMarker>", xml_escape(&enc(last.key()))));
                }
            }
        }
        for unit in &walker.out {
            if let Unit::Key { key, size, mtime, etag } = unit {
                xml.push_str(&format!(
                    "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                    xml_escape(&enc(key)),
                    iso_date(*mtime),
                    xml_escape(etag)
                ));
            }
        }
        for unit in &walker.out {
            if let Unit::Prefix(p) = unit {
                xml.push_str(&format!(
                    "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                    xml_escape(&enc(p))
                ));
            }
        }
        xml.push_str("</ListBucketResult>");
        Reply::xml(200, xml)
    }
}

struct ListParams<'a> {
    prefix: &'a str,
    delimiter: Option<&'a str>,
    max_keys: usize,
    marker: Option<String>,
}

enum Unit {
    Key { key: String, size: u64, mtime: SystemTime, etag: String },
    Prefix(String),
}

impl Unit {
    fn key(&self) -> &str {
        match self {
            Unit::Key { key, .. } => key,
            Unit::Prefix(p) => p,
        }
    }
}

struct Walker<'a> {
    p: &'a ListParams<'a>,
    out: Vec<Unit>,
    truncated: bool,
    last_prefix: Option<String>,
}

impl Walker<'_> {
    /// Depth-first in bytewise key order. Directories sort as `name/` so a
    /// sibling like `a.parquet` correctly precedes everything under `a/`.
    /// Returns false once the page is full.
    fn walk(&mut self, dir: &Path, key_prefix: &str) -> bool {
        let Ok(read) = fs::read_dir(dir) else { return true };
        // `file_type` comes with the directory read and, like `symlink_metadata`,
        // does not follow links. Files are only stat'ed once they are known to
        // belong in the page, so a huge directory costs a read and a sort, not
        // one stat per entry.
        let mut entries: Vec<(String, String, bool)> = Vec::new();
        for entry in read.flatten() {
            let Ok(name) = entry.file_name().into_string() else { continue };
            if name.starts_with('.') {
                continue;
            }
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                entries.push((format!("{name}/"), name, true));
            } else if ft.is_file() && servable(&format!("{key_prefix}{name}")) {
                if !format!("{key_prefix}{name}").starts_with(self.p.prefix) {
                    continue;
                }
                entries.push((name.clone(), name, false));
            }
        }
        entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

        for (sort_key, name, is_dir) in entries {
            let key = format!("{key_prefix}{sort_key}");
            if is_dir {
                if !(key.starts_with(self.p.prefix) || self.p.prefix.starts_with(&key)) {
                    continue;
                }
                // Everything below sorts before the marker: skip the subtree.
                if let Some(m) = &self.p.marker {
                    if m.as_str() > key.as_str() && !m.starts_with(&key) {
                        continue;
                    }
                }
                if key.starts_with(self.p.prefix) {
                    if let Some(cp) = self.common_prefix(&key) {
                        if !self.push(Unit::Prefix(cp)) {
                            return false;
                        }
                        continue;
                    }
                }
                if !self.walk(&dir.join(&name), &key) {
                    return false;
                }
            } else {
                if self.p.marker.as_deref().is_some_and(|m| key.as_str() <= m) {
                    continue;
                }
                let unit = match self.common_prefix(&key) {
                    Some(cp) => Unit::Prefix(cp),
                    None => {
                        let Ok(md) = fs::symlink_metadata(dir.join(&name)) else { continue };
                        if !md.is_file() {
                            continue;
                        }
                        let mtime = md.modified().unwrap_or(UNIX_EPOCH);
                        Unit::Key { etag: etag(md.len(), mtime), size: md.len(), mtime, key }
                    }
                };
                if !self.push(unit) {
                    return false;
                }
            }
        }
        true
    }

    /// The roll-up for `key` if a delimiter occurs after the prefix. Only valid
    /// once `key.starts_with(prefix)`.
    fn common_prefix(&self, key: &str) -> Option<String> {
        let d = self.p.delimiter?;
        let rest = &key[self.p.prefix.len()..];
        let i = rest.find(d)?;
        Some(key[..self.p.prefix.len() + i + d.len()].to_string())
    }

    fn push(&mut self, unit: Unit) -> bool {
        if let Some(m) = &self.p.marker {
            if unit.key() <= m.as_str() {
                return true;
            }
        }
        if let Unit::Prefix(p) = &unit {
            if self.last_prefix.as_deref() == Some(p.as_str()) {
                return true;
            }
        }
        if self.out.len() >= self.p.max_keys {
            self.truncated = true;
            return false;
        }
        if let Unit::Prefix(p) = &unit {
            self.last_prefix = Some(p.clone());
        }
        self.out.push(unit);
        true
    }
}

/// The directory to start walking and its key prefix: the part of `prefix` up
/// to its last `/`. `None` when that part is unsafe or not a real directory,
/// which lists as empty rather than erroring.
fn list_base(store_dir: &Path, prefix: &str) -> Option<(PathBuf, String)> {
    let base = prefix.rfind('/').map(|i| &prefix[..=i]).unwrap_or("");
    let mut path = store_dir.to_path_buf();
    if !base.is_empty() {
        let segments = safe_segments(&base[..base.len() - 1])?;
        for segment in segments {
            path.push(segment);
            if !fs::symlink_metadata(&path).ok()?.is_dir() {
                return None;
            }
        }
    }
    Some((path, base.to_string()))
}

/// The keys this server will list or return: the store's `.parquet` files and
/// the template dictionary (`_meta/templates.pq`, see [`crate::template_dict`]),
/// whose `.pq` extension keeps it out of every `**/*.parquet` glob. Anything
/// else under the store — temp files, stray uploads — stays invisible.
fn servable(key: &str) -> bool {
    key.ends_with(".parquet") || key == crate::template_dict::DICT_KEY
}

fn safe_segments(key: &str) -> Option<Vec<&str>> {
    if key.is_empty() {
        return None;
    }
    let segments: Vec<&str> = key.split('/').collect();
    let bad = |s: &str| s.is_empty() || s.starts_with('.') || s.contains(['\\', '\0']);
    if segments.iter().any(|s| bad(s)) {
        return None;
    }
    Some(segments)
}

enum RangeOutcome {
    Satisfiable(u64, u64),
    Unsatisfiable,
    Ignore,
}

fn parse_range(value: &str, len: u64) -> RangeOutcome {
    let Some(spec) = value.trim().strip_prefix("bytes=") else {
        return RangeOutcome::Ignore;
    };
    if spec.contains(',') {
        return RangeOutcome::Ignore;
    }
    let Some((first, last)) = spec.split_once('-') else {
        return RangeOutcome::Ignore;
    };
    let num = |s: &str| {
        if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
            s.parse::<u64>().ok()
        } else {
            None
        }
    };
    match (first.trim(), last.trim()) {
        ("", n) => {
            let Some(n) = num(n) else { return RangeOutcome::Ignore };
            if n == 0 || len == 0 {
                RangeOutcome::Unsatisfiable
            } else {
                RangeOutcome::Satisfiable(len.saturating_sub(n), len - 1)
            }
        }
        (a, "") => {
            let Some(a) = num(a) else { return RangeOutcome::Ignore };
            if a >= len {
                RangeOutcome::Unsatisfiable
            } else {
                RangeOutcome::Satisfiable(a, len - 1)
            }
        }
        (a, b) => {
            let (Some(a), Some(b)) = (num(a), num(b)) else { return RangeOutcome::Ignore };
            if b < a {
                RangeOutcome::Ignore
            } else if a >= len {
                RangeOutcome::Unsatisfiable
            } else {
                RangeOutcome::Satisfiable(a, b.min(len - 1))
            }
        }
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Files are immutable once renamed into place, so length plus mtime identifies
/// the content without hashing it.
fn etag(len: u64, mtime: SystemTime) -> String {
    let nanos = mtime.duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("\"{len:x}-{nanos:x}\"")
}

fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

// Same Hinnant algorithm as `partition.rs`, which keeps it private and is not
// ours to edit; a date crate for one formatter would be worse than the copy.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parses the IMF-fixdate form [`http_date`] emits. The obsolete RFC 850 and
/// asctime forms are not accepted; an unparseable date makes the header ignored.
pub(crate) fn parse_http_date(v: &str) -> Option<u64> {
    const MONTHS: [&str; 12] =
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let parts: Vec<&str> = v.split_whitespace().collect();
    let [_, day, month, year, time, "GMT"] = parts[..] else { return None };
    let month = MONTHS.iter().position(|m| *m == month)? as u32 + 1;
    let day: u32 = day.parse().ok().filter(|d| (1..=31).contains(d))?;
    let year: i64 = year.parse().ok()?;
    let mut hms = time.split(':').map(|p| p.parse::<u64>().ok());
    let (h, m, s) = (hms.next()??, hms.next()??, hms.next()??);
    if hms.next().is_some() || h > 23 || m > 59 || s > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    u64::try_from(days).ok().map(|d| d * 86_400 + h * 3600 + m * 60 + s)
}

/// IMF-fixdate, e.g. `Sun, 06 Nov 1994 08:49:37 GMT`.
fn http_date(secs: u64) -> String {
    const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] =
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let days = (secs / 86_400) as i64;
    let (y, m, d) = civil_from_days(days);
    let rem = secs % 86_400;
    format!(
        "{}, {d:02} {} {y:04} {:02}:{:02}:{:02} GMT",
        DAYS[(days + 4).rem_euclid(7) as usize],
        MONTHS[(m - 1) as usize],
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn iso_date(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs();
    let (y, m, day) = civil_from_days((secs / 86_400) as i64);
    let rem = secs % 86_400;
    format!(
        "{y:04}-{m:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        d.subsec_millis()
    )
}

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

/// Decodes `%XX` exactly once. `None` on a malformed escape or non-UTF-8 result.
fn percent_decode(s: &str, plus_is_space: bool) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = s.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
                continue;
            }
            b'+' if plus_is_space => out.push(b' '),
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8(out).ok()
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            Some((percent_decode(k, true)?, percent_decode(v, true)?))
        })
        .collect()
}

fn url_encode_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

// Hex keeps the token opaque to clients and safe in a query string unescaped.
fn encode_token(key: &str) -> String {
    key.bytes().map(|b| format!("{b:02x}")).collect()
}

fn decode_token(token: &str) -> Option<String> {
    if token.is_empty() || token.len() % 2 != 0 || !token.is_ascii() {
        return None;
    }
    let bytes: Option<Vec<u8>> = (0..token.len() / 2)
        .map(|i| u8::from_str_radix(&token[i * 2..i * 2 + 2], 16).ok())
        .collect();
    String::from_utf8(bytes?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{LogRecord, Severity};
    use std::io::Write;
    use std::net::TcpStream;

    struct Resp {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl Resp {
        fn header(&self, name: &str) -> Option<&str> {
            header(&self.headers, name)
        }
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.body).into_owned()
        }
    }

    fn http(addr: SocketAddr, method: &str, target: &str, extra: &[(&str, &str)]) -> Resp {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut req = format!("{method} {target} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
        for (k, v) in extra {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str("\r\n");
        s.write_all(req.as_bytes()).unwrap();
        let mut raw = Vec::new();
        s.read_to_end(&mut raw).unwrap();
        let split = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("header end");
        let head = String::from_utf8_lossy(&raw[..split]).into_owned();
        let mut lines = head.split("\r\n");
        let status = lines.next().unwrap().split(' ').nth(1).unwrap().parse().unwrap();
        let headers = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            .collect();
        Resp { status, headers, body: raw[split + 4..].to_vec() }
    }

    fn tags(xml: &str, tag: &str) -> Vec<String> {
        let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
        let mut out = Vec::new();
        let mut rest = xml;
        while let Some(i) = rest.find(&open) {
            let after = &rest[i + open.len()..];
            let j = after.find(&close).unwrap();
            out.push(after[..j].to_string());
            rest = &after[j..];
        }
        out
    }

    fn patterned(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    const K_ERR_B: &str = "hour=2026-01-01T00/level=error/b.parquet";
    const K_INFO_A: &str = "hour=2026-01-01T00/level=info/a.parquet";
    const K_INFO_E: &str = "hour=2026-01-01T00/level=info/e.parquet";
    const K_H1_C: &str = "hour=2026-01-01T01/level=info/c.parquet";

    struct Fixture {
        tmp: tempfile::TempDir,
        store: PathBuf,
        server: S3Server,
        addr: SocketAddr,
        a_bytes: Vec<u8>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_cfg(S3ServeConfig { addr: "127.0.0.1:0".into(), ..Default::default() })
        }

        fn with_cfg(cfg: S3ServeConfig) -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let store = tmp.path().join("store");
            let records: Vec<LogRecord> = (0..100u64)
                .map(|i| LogRecord::new(i, Severity::INFO, format!("line {i}")))
                .collect();
            crate::store::write_partition_file(&store.join(K_INFO_A), &records).unwrap();
            let a_bytes = fs::read(store.join(K_INFO_A)).unwrap();
            for key in [K_ERR_B, K_INFO_E, K_H1_C] {
                let p = store.join(key);
                fs::create_dir_all(p.parent().unwrap()).unwrap();
                fs::write(p, patterned(1000)).unwrap();
            }
            let level = store.join("hour=2026-01-01T00/level=info");
            fs::write(level.join("d.parquet.tmp"), b"partial").unwrap();
            fs::write(level.join(".hidden.parquet"), b"hidden").unwrap();
            fs::write(level.join("notes.txt"), b"text").unwrap();
            fs::write(tmp.path().join("secret.parquet"), b"outside the store").unwrap();

            let server = S3Server::start(&cfg, store.clone()).unwrap();
            let addr = server.local_addr();
            Fixture { tmp, store, server, addr, a_bytes }
        }

        fn get(&self, target: &str) -> Resp {
            http(self.addr, "GET", target, &[])
        }
        fn get_h(&self, target: &str, h: &[(&str, &str)]) -> Resp {
            http(self.addr, "GET", target, h)
        }
        fn list(&self, query: &str) -> Resp {
            self.get(&format!("/logs?list-type=2&{query}"))
        }
    }

    fn obj(key: &str) -> String {
        format!("/logs/{key}")
    }

    #[test]
    fn the_tenant_index_hour_level_node_tree_lists_by_prefix_and_serves_its_files() {
        use crate::partition::PartitionKey;
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let rows = |n: u64| (0..n).map(|i| LogRecord::new(i, Severity::INFO, format!("line {i}"))).collect::<Vec<_>>();
        let keys = [
            PartitionKey::new("acme", "default", 491_400, "info", "edge-1"),
            PartitionKey::new("acme", "default", 491_401, "warn_plus", "edge-1"),
            PartitionKey::new("acme", "payments", 491_400, "info", "edge-1"),
            PartitionKey::new("globex", "default", 491_400, "info", "edge-9"),
        ];
        for (i, key) in keys.iter().enumerate() {
            crate::store::write_partition_file(&key.dir(&store).join("f.parquet"), &rows(10 + i as u64)).unwrap();
        }
        fs::create_dir_all(store.join("_meta")).unwrap();
        fs::write(store.join(crate::template_dict::DICT_KEY), b"dict").unwrap();
        let server = S3Server::start(&S3ServeConfig { addr: "127.0.0.1:0".into(), ..Default::default() }, store.clone()).unwrap();
        let addr = server.local_addr();
        let list = |query: &str| String::from_utf8(http(addr, "GET", &format!("/logs?list-type=2&{query}"), &[]).body).unwrap();

        // The whole store, as a `**/*.parquet` glob lists it: every key starts with its tenant.
        let all = tags(&list(""), "Key");
        assert_eq!(
            all,
            vec![
                "_meta/templates.pq",
                "tenant=acme/index=default/hour=2026-01-22T00/level=info/node=edge-1/f.parquet",
                "tenant=acme/index=default/hour=2026-01-22T01/level=warn_plus/node=edge-1/f.parquet",
                "tenant=acme/index=payments/hour=2026-01-22T00/level=info/node=edge-1/f.parquet",
                "tenant=globex/index=default/hour=2026-01-22T00/level=info/node=edge-9/f.parquet",
            ]
        );
        // One index of one tenant is one prefix, and its hours are the next directory.
        let hours = list("prefix=tenant%3Dacme/index%3Ddefault/&delimiter=/");
        assert_eq!(tags(&hours, "Prefix")[1..], ["tenant=acme/index=default/hour=2026-01-22T00/", "tenant=acme/index=default/hour=2026-01-22T01/"]);
        assert!(tags(&hours, "Key").is_empty());
        // A day of one index, the way a reader narrows by event time.
        assert_eq!(tags(&list("prefix=tenant%3Dacme/index%3Ddefault/hour%3D2026-01-22T"), "Key").len(), 2);
        assert_eq!(tags(&list("prefix=tenant%3Dglobex/"), "Key").len(), 1);

        let got = http(addr, "GET", &format!("/logs/{}", all[3]), &[]);
        assert_eq!((got.status, got.body), (200, fs::read(store.join(&all[3])).unwrap()));
        assert_eq!(http(addr, "GET", "/logs/tenant=acme/index=default", &[]).status, 404, "a directory is not an object");
        server.shutdown();
    }

    #[test]
    fn uploaded_parts_are_a_second_read_only_bucket_apart_from_the_store() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, parts) = (tmp.path().join("store"), tmp.path().join("parts"));
        let name = format!("{}.parquet", "ab".repeat(32));
        fs::create_dir_all(store.join("hour=2026-01-01T00/level=info")).unwrap();
        fs::write(store.join(K_INFO_A), patterned(500)).unwrap();
        fs::create_dir_all(parts.join("edge-a")).unwrap();
        fs::write(parts.join("edge-a").join(&name), patterned(700)).unwrap();
        // An upload in flight is a dot file: never listed, never served.
        fs::write(parts.join("edge-a/.upload-0198.tmp"), b"half").unwrap();
        let cfg = S3ServeConfig { addr: "127.0.0.1:0".into(), ..Default::default() };
        let server = S3Server::start_with_parts(&cfg, store.clone(), Some(parts.clone())).unwrap();
        let a = server.local_addr();

        let key = format!("edge-a/{name}");
        let got = http(a, "GET", &format!("/parts/{key}"), &[]);
        assert_eq!((200, patterned(700)), (got.status, got.body));
        assert_eq!(http(a, "GET", &format!("/parts/{key}"), &[("Range", "bytes=10-19")]).body, patterned(700)[10..20].to_vec());
        let listed = String::from_utf8(http(a, "GET", "/parts?list-type=2", &[]).body).unwrap();
        assert_eq!((vec![key.clone()], vec!["parts".to_string()]), (tags(&listed, "Key"), tags(&listed, "Name")));
        assert_eq!(vec![key.clone()], tags(&String::from_utf8(http(a, "GET", "/parts?list-type=2&prefix=edge-a/", &[]).body).unwrap(), "Key"));
        assert_eq!(404, http(a, "GET", "/parts/edge-a/.upload-0198.tmp", &[]).status);
        // Each bucket holds only its own: a store glob never sees a part, and the reverse.
        let store_keys = tags(&String::from_utf8(http(a, "GET", "/logs?list-type=2", &[]).body).unwrap(), "Key");
        assert_eq!(vec![K_INFO_A.to_string()], store_keys);
        assert_eq!(404, http(a, "GET", &format!("/logs/{key}"), &[]).status);
        assert_eq!(404, http(a, "GET", &format!("/parts/{K_INFO_A}"), &[]).status);
        assert_eq!(404, http(a, "GET", "/parts/../store/hour=2026-01-01T00/level=info/a.parquet", &[]).status);
        let buckets = tags(&String::from_utf8(http(a, "GET", "/", &[]).body).unwrap(), "Name");
        assert_eq!(vec!["logs".to_string(), "parts".to_string()], buckets);
        // Read-only, as the store is.
        assert_eq!(405, http(a, "PUT", &format!("/parts/{key}"), &[]).status);
        assert_eq!(405, http(a, "DELETE", &format!("/parts/{key}"), &[]).status);
        assert_eq!(patterned(700), fs::read(parts.join(&key)).unwrap());
        server.shutdown();

        // Without a local parts directory the bucket does not exist, and it cannot share the store's name.
        let plain = S3Server::start(&cfg, store.clone()).unwrap();
        assert_eq!(404, http(plain.local_addr(), "GET", &format!("/parts/{key}"), &[]).status);
        plain.shutdown();
        let same = S3ServeConfig { parts_bucket: "logs".into(), ..cfg };
        assert!(matches!(S3Server::start_with_parts(&same, store, Some(parts)), Err(S3ServeError::SameBucket(_))));
    }

    #[test]
    fn head_reports_length_and_validators_without_a_body() {
        let f = Fixture::new();
        let r = http(f.addr, "HEAD", &obj(K_INFO_A), &[]);
        assert_eq!(r.status, 200);
        assert_eq!(r.header("content-length"), Some(f.a_bytes.len().to_string().as_str()));
        assert_eq!(r.header("accept-ranges"), Some("bytes"));
        assert!(r.header("etag").unwrap().starts_with('"'));
        assert!(r.header("last-modified").unwrap().ends_with(" GMT"));
        assert!(r.body.is_empty());
    }

    #[test]
    fn full_get_returns_the_parquet_file_exactly() {
        let f = Fixture::new();
        let r = f.get(&obj(K_INFO_A));
        assert_eq!(r.status, 200);
        assert_eq!(r.body, f.a_bytes);
        assert!(r.body.starts_with(b"PAR1"));
    }

    #[test]
    fn every_range_form() {
        let f = Fixture::new();
        let full = fs::read(f.store.join(K_ERR_B)).unwrap();
        let t = obj(K_ERR_B);

        let r = f.get_h(&t, &[("Range", "bytes=10-19")]);
        assert_eq!(r.status, 206);
        assert_eq!(r.header("content-range"), Some("bytes 10-19/1000"));
        assert_eq!(r.header("content-length"), Some("10"));
        assert_eq!(r.body, &full[10..20]);

        let r = f.get_h(&t, &[("Range", "bytes=990-")]);
        assert_eq!(r.status, 206);
        assert_eq!(r.header("content-range"), Some("bytes 990-999/1000"));
        assert_eq!(r.body, &full[990..]);

        let r = f.get_h(&t, &[("Range", "bytes=-5")]);
        assert_eq!(r.status, 206);
        assert_eq!(r.header("content-range"), Some("bytes 995-999/1000"));
        assert_eq!(r.body, &full[995..]);

        // End past EOF clamps; a suffix longer than the file is the whole file.
        let r = f.get_h(&t, &[("Range", "bytes=990-5000")]);
        assert_eq!(r.header("content-range"), Some("bytes 990-999/1000"));
        let r = f.get_h(&t, &[("Range", "bytes=-5000")]);
        assert_eq!(r.status, 206);
        assert_eq!(r.header("content-range"), Some("bytes 0-999/1000"));
        assert_eq!(r.body, full);

        let r = http(f.addr, "HEAD", &t, &[("Range", "bytes=0-9")]);
        assert_eq!(r.status, 206);
        assert_eq!(r.header("content-length"), Some("10"));
        assert!(r.body.is_empty());

        let s = f.server.stats();
        assert_eq!(s.range_requests, 6);
    }

    #[test]
    fn unsatisfiable_range_is_416_with_total_length() {
        let f = Fixture::new();
        for range in ["bytes=1000-", "bytes=2000-3000", "bytes=-0"] {
            let r = f.get_h(&obj(K_ERR_B), &[("Range", range)]);
            assert_eq!(r.status, 416, "{range}");
            assert_eq!(r.header("content-range"), Some("bytes */1000"));
            assert!(r.text().contains("<Code>InvalidRange</Code>"));
        }
    }

    #[test]
    fn malformed_and_multi_range_serve_the_full_object() {
        let f = Fixture::new();
        for range in ["bytes=5-2", "bytes=abc", "items=0-5", "bytes=0-1,5-9", "bytes=", "bytes=-"] {
            let r = f.get_h(&obj(K_ERR_B), &[("Range", range)]);
            assert_eq!(r.status, 200, "{range}");
            assert_eq!(r.body.len(), 1000);
            assert!(r.header("content-range").is_none());
        }
    }

    #[test]
    fn if_none_match_gives_304() {
        let f = Fixture::new();
        let etag = f.get(&obj(K_ERR_B)).header("etag").unwrap().to_string();
        let r = f.get_h(&obj(K_ERR_B), &[("If-None-Match", &etag)]);
        assert_eq!(r.status, 304);
        assert!(r.body.is_empty());
        let r = f.get_h(&obj(K_ERR_B), &[("If-None-Match", "\"other\"")]);
        assert_eq!(r.status, 200);
    }

    #[test]
    fn path_traversal_attempts_are_all_not_found() {
        let f = Fixture::new();
        assert_eq!(f.get(&obj(K_INFO_A)).status, 200);
        let attempts = [
            "/logs/../secret.parquet",
            "/logs/hour=2026-01-01T00/../../secret.parquet",
            "/logs/%2e%2e/secret.parquet",
            "/logs/%2E%2E%2Fsecret.parquet",
            "/logs/hour=2026-01-01T00%2f..%2f..%2fsecret.parquet",
            "/logs/..%5csecret.parquet",
            "/logs/hour=2026-01-01T00%5clevel=info%5ca.parquet",
            "/logs/hour=2026-01-01T00//level=info/a.parquet",
            "/logs//hour=2026-01-01T00/level=info/a.parquet",
            "/logs/hour=2026-01-01T00/./level=info/a.parquet",
            "/logs/hour=2026-01-01T00/level=info/a.parquet%00",
            "/logs/hour=2026-01-01T00/level=info/%2ehidden.parquet",
            "/logs/%252e%252e/secret.parquet",
        ];
        for t in attempts {
            let r = f.get(t);
            assert_eq!(r.status, 404, "{t}");
            assert!(r.text().contains("<Code>NoSuchKey</Code>"), "{t}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_out_of_the_store_are_not_followed() {
        let f = Fixture::new();
        let level = f.store.join("hour=2026-01-01T00/level=info");
        std::os::unix::fs::symlink(f.tmp.path().join("secret.parquet"), level.join("link.parquet"))
            .unwrap();
        std::os::unix::fs::symlink(f.tmp.path(), f.store.join("escape")).unwrap();
        assert_eq!(f.get(&obj("hour=2026-01-01T00/level=info/link.parquet")).status, 404);
        assert_eq!(f.get(&obj("escape/secret.parquet")).status, 404);
        let keys = tags(&f.list("").text(), "Key");
        assert!(keys.iter().all(|k| !k.contains("link") && !k.contains("escape")));
    }

    #[test]
    fn tmp_hidden_and_non_parquet_files_are_not_served_or_listed() {
        let f = Fixture::new();
        for name in ["d.parquet.tmp", ".hidden.parquet", "notes.txt"] {
            let r = f.get(&obj(&format!("hour=2026-01-01T00/level=info/{name}")));
            assert_eq!(r.status, 404, "{name}");
        }
        let keys = tags(&f.list("").text(), "Key");
        assert_eq!(keys, vec![K_ERR_B, K_INFO_A, K_INFO_E, K_H1_C]);
        // A directory is not an object either.
        assert_eq!(f.get(&obj("hour=2026-01-01T00")).status, 404);
    }

    #[test]
    fn the_template_dictionary_is_served_and_listed_but_no_other_pq() {
        let f = Fixture::new();
        let rows = vec![crate::template_dict::DictRow {
            template_id: 1,
            service: Some("api".into()),
            text: "user <*> logged in".into(),
            first_seen_micros: 1,
            last_seen_micros: 2,
            agent: "space".into(),
            global_id: crate::drain::fingerprint_of("user <*> logged in"),
            lineage_id: crate::drain::fingerprint_of("user <*> logged in"),
        }];
        crate::template_dict::write(&f.store, &rows).unwrap();
        let meta = f.store.join("_meta");
        fs::write(meta.join(".templates.pq.tmp"), b"partial").unwrap();
        fs::write(meta.join("other.pq"), b"not the dictionary").unwrap();
        fs::write(f.store.join("hour=2026-01-01T00/level=info/x.pq"), b"stray").unwrap();

        let r = f.get(&obj("_meta/templates.pq"));
        assert_eq!(r.status, 200);
        assert_eq!(r.body, fs::read(meta.join("templates.pq")).unwrap());
        assert!(r.body.starts_with(b"PAR1"));
        for key in ["_meta/.templates.pq.tmp", "_meta/other.pq", "hour=2026-01-01T00/level=info/x.pq"] {
            assert_eq!(f.get(&obj(key)).status, 404, "{key}");
        }

        let keys = tags(&f.list("").text(), "Key");
        assert_eq!(keys, vec!["_meta/templates.pq", K_ERR_B, K_INFO_A, K_INFO_E, K_H1_C]);
        let keys = tags(&f.list("prefix=_meta%2F").text(), "Key");
        assert_eq!(keys, vec!["_meta/templates.pq"]);
        // A `**/*.parquet` glob filters the listing by suffix; the dictionary
        // never matches it.
        assert!(!keys[0].ends_with(".parquet"));
    }

    #[test]
    fn list_with_prefix() {
        let f = Fixture::new();
        let xml = f.list("prefix=hour%3D2026-01-01T00%2Flevel%3Dinfo%2F").text();
        assert_eq!(tags(&xml, "Key"), vec![K_INFO_A, K_INFO_E]);
        assert_eq!(tags(&xml, "KeyCount"), vec!["2"]);
        assert_eq!(tags(&xml, "IsTruncated"), vec!["false"]);
        assert_eq!(tags(&xml, "Name"), vec!["logs"]);
        assert_eq!(tags(&xml, "StorageClass"), vec!["STANDARD", "STANDARD"]);
        assert_eq!(tags(&xml, "Size")[1], "1000");

        // A prefix that ends mid-name filters within the directory.
        let xml = f.list("prefix=hour%3D2026-01-01T00%2Flevel%3Dinfo%2Fe").text();
        assert_eq!(tags(&xml, "Key"), vec![K_INFO_E]);
        // Unknown directory prefix lists empty rather than failing.
        let xml = f.list("prefix=nope%2F").text();
        assert!(tags(&xml, "Key").is_empty());
        // Unsafe prefixes list empty too.
        let xml = f.list("prefix=..%2F").text();
        assert!(tags(&xml, "Key").is_empty());
    }

    #[test]
    fn list_delimiter_rolls_up_common_prefixes() {
        let f = Fixture::new();
        let xml = f.list("delimiter=%2F").text();
        assert!(tags(&xml, "Key").is_empty());
        assert_eq!(
            tags(&xml, "CommonPrefixes"),
            vec![
                "<Prefix>hour=2026-01-01T00/</Prefix>",
                "<Prefix>hour=2026-01-01T01/</Prefix>"
            ]
        );
        assert_eq!(tags(&xml, "KeyCount"), vec!["2"]);

        let xml = f.list("delimiter=%2F&prefix=hour%3D2026-01-01T00%2F").text();
        assert!(tags(&xml, "Key").is_empty());
        assert_eq!(tags(&xml, "Prefix").len(), 1 + 2); // top-level plus two CommonPrefixes
        assert!(xml.contains("<Prefix>hour=2026-01-01T00/level=error/</Prefix>"));
        assert!(xml.contains("<Prefix>hour=2026-01-01T00/level=info/</Prefix>"));

        // Files at the leaf level of the prefix still come back as Contents.
        let xml = f.list("delimiter=%2F&prefix=hour%3D2026-01-01T00%2Flevel%3Dinfo%2F").text();
        assert_eq!(tags(&xml, "Key"), vec![K_INFO_A, K_INFO_E]);
        assert!(tags(&xml, "CommonPrefixes").is_empty());
    }

    #[test]
    fn pagination_walks_every_key_once_in_order() {
        let f = Fixture::new();
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        let mut pages = 0;
        loop {
            let q = match &token {
                Some(t) => format!("max-keys=1&continuation-token={t}"),
                None => "max-keys=1".to_string(),
            };
            let xml = f.list(&q).text();
            keys.extend(tags(&xml, "Key"));
            pages += 1;
            assert!(pages <= 10, "pagination did not terminate");
            if tags(&xml, "IsTruncated") == ["true"] {
                token = Some(tags(&xml, "NextContinuationToken").remove(0));
            } else {
                assert!(tags(&xml, "NextContinuationToken").is_empty());
                break;
            }
        }
        assert_eq!(keys, vec![K_ERR_B, K_INFO_A, K_INFO_E, K_H1_C]);
        assert_eq!(pages, 4);
    }

    #[test]
    fn pagination_over_common_prefixes_does_not_repeat_them() {
        let f = Fixture::new();
        let mut seen = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..10 {
            let q = match &token {
                Some(t) => format!("delimiter=%2F&max-keys=1&continuation-token={t}"),
                None => "delimiter=%2F&max-keys=1".to_string(),
            };
            let xml = f.list(&q).text();
            seen.extend(tags(&xml, "CommonPrefixes"));
            match tags(&xml, "NextContinuationToken").pop() {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        assert_eq!(
            seen,
            vec![
                "<Prefix>hour=2026-01-01T00/</Prefix>",
                "<Prefix>hour=2026-01-01T01/</Prefix>"
            ]
        );
    }

    #[test]
    fn start_after_and_max_keys_edges() {
        let f = Fixture::new();
        let xml = f.list(&format!("start-after={K_INFO_A}")).text();
        assert_eq!(tags(&xml, "Key"), vec![K_INFO_E, K_H1_C]);
        assert_eq!(tags(&xml, "StartAfter"), vec![K_INFO_A]);

        let xml = f.list("max-keys=0").text();
        assert!(tags(&xml, "Key").is_empty());
        assert_eq!(tags(&xml, "IsTruncated"), vec!["false"]);

        let xml = f.list("max-keys=99999").text();
        assert_eq!(tags(&xml, "MaxKeys"), vec!["1000"]);

        assert_eq!(f.list("max-keys=abc").status, 400);
        assert_eq!(f.list("continuation-token=zz").status, 400);
        assert_eq!(f.get("/logs?list-type=1&prefix=x").status, 400);
    }

    #[test]
    fn encoding_type_url_escapes_keys() {
        let f = Fixture::new();
        let xml = f.list("encoding-type=url").text();
        assert_eq!(tags(&xml, "Key")[0], "hour%3D2026-01-01T00/level%3Derror/b.parquet");
        assert_eq!(tags(&xml, "EncodingType"), vec!["url"]);
        assert_eq!(f.list("encoding-type=base64").status, 400);
    }

    #[test]
    fn xml_escape_covers_markup_characters() {
        assert_eq!(xml_escape("<a&b>\"'"), "&lt;a&amp;b&gt;&quot;&apos;");
    }

    #[test]
    fn vanished_file_is_404_not_a_broken_stream() {
        let f = Fixture::new();
        let listed = tags(&f.list("").text(), "Key");
        assert!(listed.contains(&K_INFO_E.to_string()));
        fs::remove_file(f.store.join(K_INFO_E)).unwrap();
        let r = f.get(&obj(K_INFO_E));
        assert_eq!(r.status, 404);
        assert!(r.text().contains("<Code>NoSuchKey</Code>"));
    }

    #[test]
    fn bucket_service_and_method_errors() {
        let f = Fixture::new();
        let r = f.get("/other/anything.parquet");
        assert_eq!(r.status, 404);
        assert!(r.text().contains("<Code>NoSuchBucket</Code>"));
        assert_eq!(http(f.addr, "HEAD", "/logs", &[]).status, 200);
        assert_eq!(http(f.addr, "HEAD", "/other", &[]).status, 404);

        let r = f.get("/");
        assert_eq!(r.status, 200);
        assert_eq!(tags(&r.text(), "Name"), vec!["logs"]);

        for m in ["PUT", "POST", "DELETE"] {
            let r = http(f.addr, m, &obj(K_INFO_A), &[("Content-Length", "0")]);
            assert_eq!(r.status, 405, "{m}");
            assert!(r.text().contains("<Code>MethodNotAllowed</Code>"));
            assert!(r.text().contains("<RequestId>"));
        }
    }

    #[test]
    fn stats_count_requests_ranges_and_bytes() {
        let f = Fixture::new();
        f.get(&obj(K_ERR_B));
        f.get_h(&obj(K_ERR_B), &[("Range", "bytes=0-99")]);
        let s = f.server.stats();
        assert_eq!(s.requests, 2);
        assert_eq!(s.range_requests, 1);
        assert_eq!(s.bytes_served, 1100);
    }

    #[test]
    fn credentials_gate_every_request() {
        let f = Fixture::with_cfg(S3ServeConfig {
            addr: "127.0.0.1:0".into(),
            access_key: Some("AK".into()),
            secret_key: Some("SK".into()),
            ..Default::default()
        });
        for t in [obj(K_INFO_A), "/logs?list-type=2".to_string(), "/".to_string()] {
            let r = f.get(&t);
            assert_eq!(r.status, 403, "{t}");
            assert!(r.text().contains("<Code>"));
        }
    }

    #[test]
    fn non_loopback_without_credentials_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = S3ServeConfig { addr: "0.0.0.0:0".into(), ..Default::default() };
        match S3Server::start(&cfg, tmp.path().to_path_buf()) {
            Err(S3ServeError::OpenOnPublicAddress { .. }) => {}
            other => panic!("expected refusal, got {:?}", other.map(|_| ())),
        }
    }

    fn status_with_host(addr: SocketAddr, host: Option<&str>) -> u16 {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut req = String::from("GET /logs?list-type=2 HTTP/1.1\r\nConnection: close\r\n");
        if let Some(h) = host {
            req.push_str(&format!("Host: {h}\r\n"));
        }
        req.push_str("\r\n");
        s.write_all(req.as_bytes()).unwrap();
        let mut raw = String::new();
        s.read_to_string(&mut raw).unwrap();
        raw.split(' ').nth(1).unwrap().parse().unwrap()
    }

    #[test]
    fn anonymous_mode_rejects_non_loopback_host_header() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = S3ServeConfig { addr: "127.0.0.1:0".into(), ..Default::default() };
        let server = S3Server::start(&cfg, tmp.path().to_path_buf()).unwrap();
        let addr = server.local_addr();
        for ok in ["127.0.0.1:9000", "localhost", "LOCALHOST:9000", "[::1]:9000", "127.0.0.2"] {
            assert_eq!(status_with_host(addr, Some(ok)), 200, "{ok}");
        }
        for bad in ["attacker.example", "attacker.example:9000", "127.0.0.1.evil.test", "evil.test:127", ""] {
            assert_eq!(status_with_host(addr, Some(bad)), 403, "{bad}");
        }
        server.shutdown();
    }

    #[test]
    fn allowed_hosts_is_enforced_even_with_credentials() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = S3ServeConfig {
            addr: "127.0.0.1:0".into(),
            access_key: Some("AK".into()),
            secret_key: Some("SK".into()),
            allowed_hosts: vec!["Logs.Internal".into()],
            ..Default::default()
        };
        let server = S3Server::start(&cfg, tmp.path().to_path_buf()).unwrap();
        let addr = server.local_addr();
        // Allowed host passes the Host check and then fails auth (unsigned).
        assert_eq!(status_with_host(addr, Some("logs.internal:9000")), 403);
        let body = |host| {
            let mut s = TcpStream::connect(addr).unwrap();
            s.write_all(format!("GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes()).unwrap();
            let mut raw = String::new();
            s.read_to_string(&mut raw).unwrap();
            raw
        };
        assert!(body("logs.internal").contains("AccessDenied"));
        assert!(body("other.test").contains("Invalid Host header"));
        server.shutdown();
    }

    #[test]
    fn one_credential_alone_is_a_startup_error() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = S3ServeConfig {
            addr: "127.0.0.1:0".into(),
            access_key: Some("AK".into()),
            ..Default::default()
        };
        // The env fallback could in principle supply the secret; only assert
        // when it does not.
        if std::env::var_os("LOGLESS_S3_SECRET_KEY").is_none() {
            assert!(matches!(
                S3Server::start(&cfg, tmp.path().to_path_buf()),
                Err(S3ServeError::PartialCredentials { .. })
            ));
        }
    }

    #[test]
    fn config_defaults_and_unknown_fields() {
        let cfg: S3ServeConfig = toml::from_str("").unwrap();
        assert_eq!(cfg, S3ServeConfig::default());
        assert!(!cfg.enabled);
        assert_eq!((cfg.addr.as_str(), cfg.bucket.as_str(), cfg.workers), ("127.0.0.1:9000", "logs", 8));
        assert!(toml::from_str::<S3ServeConfig>("bogus = 1").is_err());
        assert!(!format!("{:?}", S3ServeConfig { secret_key: Some("hunter2".into()), ..cfg }).contains("hunter2"));
    }

    #[test]
    fn http_date_matches_rfc_example() {
        assert_eq!(http_date(784_111_777), "Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
    }

    #[test]
    fn shutdown_joins_every_worker() {
        let f = Fixture::new();
        assert_eq!(f.get(&obj(K_ERR_B)).status, 200);
        let started = std::time::Instant::now();
        f.server.shutdown();
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    fn big_object(f: &Fixture, len: usize) -> String {
        let key = "hour=2026-01-01T00/level=info/big.parquet";
        fs::write(f.store.join(key), patterned(len)).unwrap();
        obj(key)
    }

    #[test]
    fn large_objects_advertise_content_length_not_chunked() {
        let f = Fixture::new();
        let target = big_object(&f, 200_000);
        for (method, extra) in [
            ("HEAD", vec![]),
            ("GET", vec![]),
            ("GET", vec![("Range", "bytes=0-99999")]),
        ] {
            let r = http(f.addr, method, &target, &extra);
            let want = if extra.is_empty() { "200000" } else { "100000" };
            assert_eq!(r.header("content-length"), Some(want), "{method} {extra:?}");
            assert_eq!(r.header("transfer-encoding"), None, "{method} {extra:?}");
        }
    }

    #[test]
    fn a_stalled_download_does_not_starve_other_requests() {
        let f = Fixture::with_cfg(S3ServeConfig {
            addr: "127.0.0.1:0".into(),
            workers: 2,
            ..Default::default()
        });
        let key = "hour=2026-01-01T00/level=info/huge.parquet";
        File::create(f.store.join(key)).unwrap().set_len(64 << 20).unwrap();
        // More than a socket buffer can absorb, and never read.
        let stalled: Vec<TcpStream> = (0..3)
            .map(|_| {
                let mut s = TcpStream::connect(f.addr).unwrap();
                write!(s, "GET /logs/{key} HTTP/1.1\r\nHost: {}\r\n\r\n", f.addr).unwrap();
                s
            })
            .collect();
        std::thread::sleep(Duration::from_millis(300));
        let started = std::time::Instant::now();
        let r = f.list("max-keys=1");
        assert_eq!(r.status, 200);
        assert!(started.elapsed() < Duration::from_secs(3));
        drop(stalled);
    }

    fn amz_now() -> String {
        let iso = iso_date(SystemTime::now());
        let digits: String = iso[..19].chars().filter(char::is_ascii_digit).collect();
        format!("{}T{}Z", &digits[..8], &digits[8..])
    }

    #[test]
    fn unknown_access_key_is_indistinguishable_from_a_bad_secret() {
        let f = Fixture::with_cfg(S3ServeConfig {
            addr: "127.0.0.1:0".into(),
            access_key: Some("AK".into()),
            secret_key: Some("SK".into()),
            ..Default::default()
        });
        let date = amz_now();
        let code = |access_key: &str| {
            let auth = format!(
                "AWS4-HMAC-SHA256 Credential={access_key}/{}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={}",
                &date[..8],
                "0".repeat(64)
            );
            let r = f.get_h(
                "/logs?list-type=2",
                &[("Authorization", &auth), ("x-amz-date", &date), ("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
            );
            assert_eq!(r.status, 403);
            tags(&r.text(), "Code").remove(0)
        };
        assert_eq!(code("AK"), "SignatureDoesNotMatch");
        assert_eq!(code("BADKEY"), "SignatureDoesNotMatch");
    }

    #[test]
    fn conditional_headers_follow_rfc_7232() {
        let f = Fixture::new();
        let t = obj(K_ERR_B);
        let head = http(f.addr, "HEAD", &t, &[]);
        let etag = head.header("etag").unwrap().to_string();
        let lm = head.header("last-modified").unwrap().to_string();

        assert_eq!(f.get_h(&t, &[("If-Match", "\"nope\"")]).status, 412);
        assert!(f.get_h(&t, &[("If-Match", "\"nope\"")]).text().contains("PreconditionFailed"));
        assert_eq!(f.get_h(&t, &[("If-Match", &etag)]).status, 200);
        assert_eq!(f.get_h(&t, &[("If-Match", "*")]).status, 200);
        assert_eq!(f.get_h(&t, &[("If-Match", &format!("W/{etag}"))]).status, 412);

        let past = "Thu, 01 Jan 1970 00:00:00 GMT";
        let future = "Sat, 03 Oct 2099 00:00:00 GMT";
        assert_eq!(f.get_h(&t, &[("If-Unmodified-Since", past)]).status, 412);
        assert_eq!(f.get_h(&t, &[("If-Unmodified-Since", future)]).status, 200);
        assert_eq!(f.get_h(&t, &[("If-Unmodified-Since", "garbage")]).status, 200);

        let r = f.get_h(&t, &[("If-Modified-Since", future)]);
        assert_eq!(r.status, 304);
        assert!(r.body.is_empty());
        assert_eq!(r.header("content-type"), None);
        assert_eq!(f.get_h(&t, &[("If-Modified-Since", past)]).status, 200);
        assert_eq!(f.get_h(&t, &[("If-Modified-Since", &lm)]).status, 304);
        // If-None-Match takes precedence over If-Modified-Since.
        assert_eq!(
            f.get_h(&t, &[("If-None-Match", "\"other\""), ("If-Modified-Since", future)]).status,
            200
        );
    }

    #[test]
    fn if_range_only_honours_range_for_the_current_version() {
        let f = Fixture::new();
        let t = obj(K_ERR_B);
        let head = http(f.addr, "HEAD", &t, &[]);
        let etag = head.header("etag").unwrap().to_string();
        let lm = head.header("last-modified").unwrap().to_string();

        let stale = f.get_h(&t, &[("Range", "bytes=0-1"), ("If-Range", "\"stale\"")]);
        assert_eq!((stale.status, stale.body.len()), (200, 1000));
        let stale_date = f.get_h(&t, &[("Range", "bytes=0-1"), ("If-Range", "Thu, 01 Jan 1970 00:00:00 GMT")]);
        assert_eq!((stale_date.status, stale_date.body.len()), (200, 1000));
        for fresh in [etag.as_str(), lm.as_str()] {
            let r = f.get_h(&t, &[("Range", "bytes=0-1"), ("If-Range", fresh)]);
            assert_eq!((r.status, r.body.len()), (206, 2), "{fresh}");
        }
    }

    #[test]
    fn list_objects_v1_pages_with_marker_and_next_marker() {
        let f = Fixture::new();
        let r = f.get("/logs?max-keys=2");
        assert_eq!(r.status, 200);
        let xml = r.text();
        assert_eq!(tags(&xml, "Key"), vec![K_ERR_B.to_string(), K_INFO_A.to_string()]);
        assert_eq!(tags(&xml, "IsTruncated"), vec!["true"]);
        assert_eq!(tags(&xml, "Marker"), vec![""]);
        assert!(tags(&xml, "KeyCount").is_empty());

        let xml = f.get(&format!("/logs?max-keys=5&marker={}", url_encode_key(K_INFO_A))).text();
        assert_eq!(tags(&xml, "Key"), vec![K_INFO_E.to_string(), K_H1_C.to_string()]);
        assert_eq!(tags(&xml, "IsTruncated"), vec!["false"]);

        // NextMarker is only sent with a delimiter; it may be a common prefix.
        let xml = f.get("/logs?delimiter=%2F&max-keys=1").text();
        assert_eq!(tags(&xml, "NextMarker"), vec!["hour=2026-01-01T00/"]);
        let xml = f.get("/logs?delimiter=%2F&max-keys=1&marker=hour%3D2026-01-01T00%2F").text();
        assert_eq!(tags(&xml, "Prefix"), vec!["", "hour=2026-01-01T01/"]);
        assert_eq!(tags(&xml, "CommonPrefixes").len(), 1);

        let r = f.get("/logs?list-type=3");
        assert_eq!(r.status, 400);
    }

    #[test]
    fn bucket_location_is_answered() {
        let f = Fixture::new();
        let r = f.get("/logs?location");
        assert_eq!(r.status, 200);
        assert!(r.text().contains(">us-east-1</LocationConstraint>"), "{}", r.text());
    }

    #[test]
    fn responses_carry_request_ids_and_bucket_region() {
        let f = Fixture::new();
        let r = http(f.addr, "HEAD", "/logs", &[]);
        assert_eq!(r.header("x-amz-bucket-region"), Some("us-east-1"));
        assert!(r.header("x-amz-request-id").is_some());
        assert!(r.header("x-amz-id-2").is_some());
        let r = f.get("/logs/missing.parquet");
        assert_eq!(r.status, 404);
        assert!(r.header("x-amz-request-id").is_some());
        let id = r.header("x-amz-request-id").unwrap().to_string();
        assert!(r.text().contains(&format!("<RequestId>{id}</RequestId>")));
    }

    #[test]
    fn a_huge_directory_lists_a_page_correctly() {
        let f = Fixture::new();
        let dir = f.store.join("hour=2026-01-02T00/level=info");
        fs::create_dir_all(&dir).unwrap();
        for i in 0..3000 {
            fs::write(dir.join(format!("f{i:05}.parquet")), b"x").unwrap();
        }
        let xml = f.list("prefix=hour%3D2026-01-02T00%2Flevel%3Dinfo%2F&max-keys=2&start-after=hour%3D2026-01-02T00%2Flevel%3Dinfo%2Ff01000.parquet").text();
        assert_eq!(
            tags(&xml, "Key"),
            vec![
                "hour=2026-01-02T00/level=info/f01001.parquet",
                "hour=2026-01-02T00/level=info/f01002.parquet"
            ]
        );
        assert_eq!(tags(&xml, "IsTruncated"), vec!["true"]);
    }

    // --- hostile input ---------------------------------------------------

    fn raw_request(addr: SocketAddr, bytes: &[u8]) -> (u16, String) {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let _ = s.write_all(bytes);
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        let text = String::from_utf8_lossy(&out).into_owned();
        (text.split(' ').nth(1).and_then(|c| c.parse().ok()).unwrap_or(0), text)
    }

    #[test]
    fn traversal_in_every_encoding_and_position_finds_nothing() {
        let f = Fixture::new();
        let secret = "outside the store";
        let level = "hour=2026-01-01T00/level=info";
        let paths: Vec<String> = vec![
            // absolute and rooted
            "/logs//etc/passwd".into(), "/logs/%2Fetc%2Fpasswd".into(), "/logs/%2Fsecret.parquet".into(),
            format!("/logs/{}", f.tmp.path().join("secret.parquet").display()),
            // dot segments, raw and encoded once, twice and three times
            "/logs/./../secret.parquet".into(), "/logs/%2e/%2e%2e/secret.parquet".into(), "/logs/%2E%2E/secret.parquet".into(),
            "/logs/%252e%252e%252fsecret.parquet".into(), "/logs/%25252e%25252e/secret.parquet".into(),
            "/logs/..%2fsecret.parquet".into(), "/logs/..%2Fsecret.parquet".into(), format!("/logs/{level}/..%2f..%2f..%2fsecret.parquet"),
            // backslash, raw and encoded, and a drive letter
            "/logs/..\\secret.parquet".into(), "/logs/%5c..%5csecret.parquet".into(), "/logs/C:%5csecret.parquet".into(),
            "/logs/..%255csecret.parquet".into(),
            // overlong and non-UTF-8 spellings of '.' and '/'
            "/logs/%c0%ae%c0%ae/secret.parquet".into(), "/logs/%c0%af%c0%afsecret.parquet".into(), "/logs/%e0%80%ae%e0%80%ae/secret.parquet".into(),
            "/logs/%ff%fe/secret.parquet".into(), "/logs/%u002e%u002e/secret.parquet".into(), "/logs/..%00/secret.parquet".into(),
            // NUL in the middle and at the end, raw and encoded
            format!("/logs/{level}/a.parquet%00.txt"), format!("/logs/{level}%00/a.parquet"), "/logs/%00".into(), "/logs/%00../secret.parquet".into(),
            // trailing dot, space, slash, dot-segment in the last position
            format!("/logs/{level}/a.parquet."), format!("/logs/{level}/a.parquet%20"), format!("/logs/{level}/a.parquet/"),
            format!("/logs/{level}/a.parquet/."), format!("/logs/{level}/a.parquet/.."),
            // bucket position
            "/%2e%2e/secret.parquet".into(), "/../logs/secret.parquet".into(), "/logs%2f..%2fsecret.parquet".into(),
            "/logs%00/x.parquet".into(), "/LOGS/hour=2026-01-01T00/level=info/a.parquet".into(),
            // malformed escapes
            "/logs/%".into(), "/logs/%2".into(), "/logs/%zz/a.parquet".into(), "/logs/%2e%2".into(),
        ];
        for t in &paths {
            let (status, text) = raw_request(f.addr, format!("GET {t} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", f.addr).as_bytes());
            assert!(matches!(status, 400 | 404), "{t}: {status}");
            assert!(!text.contains(secret), "{t} returned the file outside the store");
            let (status, text) = raw_request(f.addr, format!("HEAD {t} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", f.addr).as_bytes());
            assert!(matches!(status, 400 | 404), "HEAD {t}: {status} {text}");
        }
        // The same names as listing prefixes list nothing outside the store.
        for prefix in ["../", "%2e%2e/", "..%2f", "%2e%2e%2f", "./", "a/../", "\\", "/", "//", "%00", "hour=2026-01-01T00/../..", "%252e%252e/"] {
            let r = f.list(&format!("prefix={prefix}"));
            assert_eq!(200, r.status, "{prefix}");
            assert!(!r.text().contains("secret"), "{prefix}: {}", r.text());
            assert!(!r.text().contains("<Key>"), "{prefix} lists a key: {}", r.text());
        }
        // Control: the real object is still there.
        assert_eq!(200, f.get(&obj(K_INFO_A)).status);
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_directory_or_file_in_the_store_is_neither_listed_nor_read() {
        let f = Fixture::new();
        let outside = f.tmp.path().join("outside");
        fs::create_dir_all(outside.join("level=info")).unwrap();
        fs::write(outside.join("level=info/x.parquet"), b"outside the store").unwrap();
        let hour = f.store.join("hour=2026-01-01T00");
        std::os::unix::fs::symlink(&outside, f.store.join("linked")).unwrap();
        std::os::unix::fs::symlink(outside.join("level=info"), hour.join("level=linked")).unwrap();
        std::os::unix::fs::symlink(f.tmp.path().join("secret.parquet"), hour.join("level=info/s.parquet")).unwrap();
        // A link to the store itself must not make a loop the walker follows forever.
        std::os::unix::fs::symlink(&f.store, hour.join("loop")).unwrap();
        for key in ["linked/level=info/x.parquet", "hour=2026-01-01T00/level=linked/x.parquet", "hour=2026-01-01T00/level=info/s.parquet", "hour=2026-01-01T00/loop/hour=2026-01-01T00/level=info/a.parquet"] {
            assert_eq!(404, f.get(&obj(key)).status, "{key}");
        }
        for query in ["", "prefix=linked/", "prefix=hour%3D2026-01-01T00%2F", "prefix=hour%3D2026-01-01T00%2Flevel%3Dlinked%2F", "delimiter=%2F"] {
            let xml = f.list(query).text();
            let listed = [tags(&xml, "Key"), tags(&xml, "Prefix").into_iter().skip(1).collect(), tags(&xml, "CommonPrefixes")].concat().join(" ");
            for bad in ["linked", "loop", "s.parquet", "x.parquet"] {
                assert!(!listed.contains(bad), "{query:?} lists {bad}: {xml}");
            }
        }
    }

    #[test]
    fn range_requests_cannot_ask_for_more_than_the_object_or_for_many_pieces() {
        let f = Fixture::new();
        let t = obj(K_ERR_B); // 1000 bytes
        let served = |range: &str| {
            let r = f.get_h(&t, &[("Range", range)]);
            assert!(r.body.len() <= 1000, "{range}: {} bytes", r.body.len());
            r
        };
        for (range, status, len) in [
            ("bytes=0-99999999999999999999999999", 200, 1000), // does not fit a u64: ignored, whole object
            ("bytes=0-18446744073709551615", 206, 1000),
            ("bytes=18446744073709551615-", 416, 0),
            ("bytes=18446744073709551615-18446744073709551615", 416, 0),
            ("bytes=-18446744073709551615", 206, 1000),
            ("bytes=-99999999999999999999999999", 200, 1000),
            ("bytes=999-18446744073709551615", 206, 1),
            ("bytes=1000-", 416, 0),
            ("bytes=-0", 416, 0),
            ("bytes=5-4", 200, 1000),
            ("bytes=-", 200, 1000),
            ("bytes=", 200, 1000),
            ("bytes=a-b", 200, 1000),
            ("bytes=0x10-0x20", 200, 1000),
            ("bytes=+1-2", 200, 1000),
            ("bytes=1 -2", 206, 2),
            ("items=0-9", 200, 1000),
            ("bytes=0-9;x=1", 200, 1000),
        ] {
            let r = served(range);
            assert_eq!(status, r.status, "{range}");
            if status != 416 {
                assert_eq!(len, r.body.len(), "{range}");
            }
        }
        // Many ranges: S3 ignores a multi-range header, so the cost is one plain GET, never a multipart
        // assembly or one read per range.
        for count in [2usize, 100, 1000] {
            let many = format!("bytes={}", (0..count).map(|i| format!("{i}-{i}")).collect::<Vec<_>>().join(","));
            let r = served(&many);
            assert_eq!((200, 1000), (r.status, r.body.len()), "{count} ranges");
            assert_ne!(Some("multipart"), r.header("content-type").map(|c| &c[..9.min(c.len())]));
        }
        let too_many = format!("bytes={}", (0..5000).map(|i| format!("{i}-{i}")).collect::<Vec<_>>().join(","));
        assert_eq!(431, f.get_h(&t, &[("Range", &too_many)]).status, "a 30 kB Range header is over the head budget");
        // Repeated Range headers: one wins, and it is still bounded by the object.
        let (status, text) = raw_request(f.addr, format!("GET {t} HTTP/1.1\r\nHost: {}\r\nRange: bytes=0-4\r\nRange: bytes=0-99999\r\nConnection: close\r\n\r\n", f.addr).as_bytes());
        assert!(matches!(status, 200 | 206 | 416), "{text:.80}");
    }

    #[test]
    fn a_huge_directory_lists_a_bounded_page_whatever_the_client_asks() {
        let f = Fixture::new();
        let dir = f.store.join("hour=2026-02-01T00/level=info");
        fs::create_dir_all(&dir).unwrap();
        for i in 0..5000 {
            fs::write(dir.join(format!("f{i:05}.parquet")), b"x").unwrap();
        }
        let prefix = "prefix=hour%3D2026-02-01T00%2Flevel%3Dinfo%2F";
        for max_keys in ["", "&max-keys=1000000", "&max-keys=2147483647", "&max-keys=9223372036854775807"] {
            let xml = f.list(&format!("{prefix}{max_keys}")).text();
            assert_eq!(1000, tags(&xml, "Key").len(), "{max_keys:?}");
            assert_eq!(vec!["true"], tags(&xml, "IsTruncated"));
            assert!(xml.len() < 400_000, "{} bytes", xml.len());
        }
        for bad in ["-1", "abc", "99999999999999999999", "1.5", ""] {
            assert_eq!(400, f.list(&format!("{prefix}&max-keys={bad}")).status, "max-keys={bad}");
        }
        assert_eq!(0, tags(&f.list(&format!("{prefix}&max-keys=0")).text(), "Key").len());
        // Thousands of distinct sub-prefixes roll up into at most one page too.
        for i in 0..3000 {
            fs::create_dir_all(f.store.join(format!("hour=2026-03-{:02}T{:02}", i % 28 + 1, i % 24)).join(format!("level=l{i}"))).unwrap();
        }
        let xml = f.list("delimiter=%2F&max-keys=100000").text();
        assert!(tags(&xml, "CommonPrefixes").len() <= 1000, "{}", tags(&xml, "CommonPrefixes").len());
        // A continuation token that is not one this server issued is refused, not followed.
        for token in ["zz", "0", "../../etc/passwd", &"ab".repeat(2_000)] {
            let r = f.list(&format!("continuation-token={token}"));
            assert_eq!(400, r.status, "token {:.20}", token);
        }
    }

    #[test]
    fn very_long_query_strings_and_many_parameters_are_answered_not_amplified() {
        let f = Fixture::new();
        // Up to the head budget (16 KiB for the whole request line and headers) a long query is just answered.
        let long = "a".repeat(10_000);
        let r = f.list(&format!("prefix={long}"));
        assert_eq!(200, r.status);
        assert!(tags(&r.text(), "Key").is_empty());
        assert!(r.body.len() < 2 * long.len() + 4096, "the prefix is echoed once");
        let many: String = (0..1000).map(|i| format!("p{i}=v&")).collect();
        assert_eq!(200, f.list(&many).status);
        let escapes = "%41".repeat(3_000);
        assert_eq!(200, f.list(&format!("prefix={escapes}")).status);
        assert_eq!(200, f.list(&format!("prefix={}", "%".repeat(10_000))).status);
        assert_eq!(200, f.list(&format!("prefix={}", "%e0%a4%a".repeat(1_000))).status);
        // Past the budget: refused before the request is parsed any further.
        for oversized in [format!("prefix={}", "a".repeat(100_000)), (0..20_000).map(|i| format!("p{i}=v&")).collect::<String>(), "%41".repeat(100_000)] {
            assert_eq!(431, f.list(&oversized).status, "{} bytes of query", oversized.len());
        }
        // And the server is still fine.
        assert_eq!(200, f.get(&obj(K_INFO_A)).status);
    }

    #[test]
    fn nothing_but_get_and_head_is_served_and_the_store_never_changes() {
        for creds in [false, true] {
            let cfg = S3ServeConfig {
                addr: "127.0.0.1:0".into(),
                access_key: creds.then(|| "AK".to_string()),
                secret_key: creds.then(|| "SK".to_string()),
                ..Default::default()
            };
            let f = Fixture::with_cfg(cfg);
            let snapshot = |dir: &Path| -> Vec<(String, u64)> {
                fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, u64)>) {
                    for e in fs::read_dir(dir).unwrap().flatten() {
                        let p = e.path();
                        if p.is_dir() { walk(&p, base, out) } else { out.push((p.strip_prefix(base).unwrap().display().to_string(), e.metadata().unwrap().len())) }
                    }
                }
                let mut out = Vec::new();
                walk(dir, dir, &mut out);
                out.sort();
                out
            };
            let before = snapshot(f.tmp.path());
            let new_key = "hour=2026-04-01T00/level=info/new.parquet";
            for method in ["PUT", "POST", "DELETE", "PATCH", "OPTIONS", "TRACE", "COPY", "MOVE", "PROPFIND", "put"] {
                for target in [obj(new_key), obj(K_INFO_A), "/logs".to_string(), "/logs?delete".to_string(), "/logs?uploads".to_string(), "/".to_string()] {
                    let body = b"<Delete><Object><Key>x</Key></Object></Delete>";
                    let req = format!("{method} {target} HTTP/1.1\r\nHost: {}\r\nx-amz-copy-source: /logs/{K_INFO_A}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", f.addr, body.len());
                    let (status, text) = raw_request(f.addr, &[req.as_bytes(), &body[..]].concat());
                    // With credentials the request is judged unauthenticated first; either way it is refused.
                    let want: &[u16] = if creds { &[403] } else { &[405, 501] };
                    assert!(want.contains(&status), "creds={creds} {method} {target}: {status} {text:.100}");
                    if !creds {
                        assert!(text.to_ascii_lowercase().contains("allow: get, head") || status == 501, "{method} {target}: {text:.200}");
                    }
                }
            }
            assert_eq!(before, snapshot(f.tmp.path()), "creds={creds}: a refused write changed the store");
        }
    }

    // SigV4 as a client does it, so the server's checks are tried with requests that are valid in every
    // respect but the one under test.
    fn sigv4(method: &str, path: &str, query: &str, host: &str, amz_date: &str, signed: &str, secret: &str) -> Vec<(String, String)> {
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256};
        let hmac = |key: &[u8], data: &[u8]| {
            let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
            m.update(data);
            m.finalize().into_bytes().to_vec()
        };
        let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        let date = &amz_date[..8];
        let hashed = "UNSIGNED-PAYLOAD";
        let value = |name: &str| match name {
            "host" => host.to_string(),
            "x-amz-date" => amz_date.to_string(),
            _ => hashed.to_string(),
        };
        let canon_headers: String = signed.split(';').map(|n| format!("{n}:{}\n", value(n))).collect();
        let canonical = format!("{method}\n{path}\n{query}\n{canon_headers}\n{signed}\n{hashed}");
        let to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{date}/us-east-1/s3/aws4_request\n{}", hex(&Sha256::digest(canonical.as_bytes())));
        let k = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
        let k = hmac(&hmac(&hmac(&k, b"us-east-1"), b"s3"), b"aws4_request");
        let signature = hex(&hmac(&k, to_sign.as_bytes()));
        vec![
            ("x-amz-date".into(), amz_date.to_string()),
            ("x-amz-content-sha256".into(), hashed.into()),
            ("Authorization".into(), format!("AWS4-HMAC-SHA256 Credential=AK/{date}/us-east-1/s3/aws4_request, SignedHeaders={signed}, Signature={signature}")),
        ]
    }

    fn amz_at(offset_secs: i64) -> String {
        let t = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 + offset_secs) as u64;
        let digits: String = iso_date(UNIX_EPOCH + Duration::from_secs(t))[..19].chars().filter(char::is_ascii_digit).collect();
        format!("{}T{}Z", &digits[..8], &digits[8..])
    }

    fn secured_fixture() -> Fixture {
        Fixture::with_cfg(S3ServeConfig { addr: "127.0.0.1:0".into(), access_key: Some("AK".into()), secret_key: Some("SK".into()), ..Default::default() })
    }

    const SIGNED_ALL: &str = "host;x-amz-content-sha256;x-amz-date";

    fn code(r: &Resp) -> String {
        tags(&r.text(), "Code").into_iter().next().unwrap_or_default()
    }

    #[test]
    fn a_signature_is_only_good_for_the_request_it_was_made_for() {
        let f = secured_fixture();
        let host = f.addr.to_string();
        let path = obj(K_INFO_A);
        let call = |method: &str, path: &str, query: &str, h: &[(String, String)]| {
            let h: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            let target = if query.is_empty() { path.to_string() } else { format!("{path}?{query}") };
            http(f.addr, method, &target, &h)
        };
        let good = sigv4("GET", &path, "", &host, &amz_at(0), SIGNED_ALL, "SK");
        assert_eq!(200, call("GET", &path, "", &good).status, "control");

        // Wrong secret, a flipped bit anywhere in the signature, a short or empty one.
        let wrong = sigv4("GET", &path, "", &host, &amz_at(0), SIGNED_ALL, "not-the-secret");
        assert_eq!(("SignatureDoesNotMatch", 403), (code(&call("GET", &path, "", &wrong)).as_str(), call("GET", &path, "", &wrong).status));
        let auth = good.iter().find(|(k, _)| k == "Authorization").unwrap().1.clone();
        let sig_at = auth.rfind("Signature=").unwrap() + "Signature=".len();
        for (name, tampered) in [
            ("last digit changed", { let mut a = auth.clone(); let c = a.pop().unwrap(); a.push(if c == '0' { '1' } else { '0' }); a }),
            ("first digit changed", { let mut a = auth.clone(); let c = a.remove(sig_at); a.insert(sig_at, if c == '0' { '1' } else { '0' }); a }),
            ("one byte short", auth[..auth.len() - 2].to_string()),
            ("empty", auth[..sig_at].to_string()),
            ("not hex", format!("{}{}", &auth[..sig_at], "z".repeat(64))),
            ("sha512 label", auth.replace("AWS4-HMAC-SHA256", "AWS4-HMAC-SHA512")),
            ("sigv2 style", "AWS AK:c2lnbmF0dXJl".to_string()),
            ("bearer instead", "Bearer AK".to_string()),
        ] {
            let mut h = good.clone();
            h.iter_mut().find(|(k, _)| k == "Authorization").unwrap().1 = tampered;
            let r = call("GET", &path, "", &h);
            assert_eq!(403, r.status, "{name}");
            assert!(r.body.len() < 2000 && !r.text().contains("SK"), "{name}");
        }

        // Past the head budget (16 KiB) the request is refused before anything reads it.
        let mut h = good.clone();
        h.iter_mut().find(|(k, _)| k == "Authorization").unwrap().1 = format!("{}{}", &auth[..sig_at], "a".repeat(1 << 20));
        assert_eq!(431, call("GET", &path, "", &h).status, "a megabyte of signature");

        // The signature binds method, path and query: reusing it for anything else fails.
        assert_eq!(403, call("HEAD", &path, "", &good).status, "other method");
        assert_eq!(403, call("GET", &obj(K_INFO_E), "", &good).status, "other key");
        let listing = sigv4("GET", "/logs", "list-type=2", &host, &amz_at(0), SIGNED_ALL, "SK");
        assert_eq!(200, call("GET", "/logs", "list-type=2", &listing).status, "control");
        assert_eq!(403, call("GET", "/logs", "list-type=2&prefix=", &listing).status, "added a parameter");
        assert_eq!(403, call("GET", "/logs", "list-type=2&max-keys=1", &listing).status, "added a parameter");
        assert_eq!(403, call("GET", "/logs", "list-type=1", &listing).status, "changed a value");
        assert_eq!(403, call("GET", "/logs/", "list-type=2", &listing).status, "trailing slash");

        // The signature must cover the host and the date, or it could be replayed under another name.
        for signed in ["x-amz-content-sha256;x-amz-date", "host;x-amz-content-sha256", "host"] {
            let h = sigv4("GET", &path, "", &host, &amz_at(0), signed, "SK");
            assert_eq!(403, call("GET", &path, "", &h).status, "SignedHeaders={signed}");
        }
        // Another Host than the one signed.
        let mut other_host = good.clone();
        other_host.push(("Host".into(), "evil.example".into()));
        assert_eq!(403, call("GET", &path, "", &other_host).status);
    }

    #[test]
    fn a_stale_or_future_signature_is_refused_and_a_replay_inside_the_window_is_accepted() {
        let f = secured_fixture();
        let host = f.addr.to_string();
        let path = obj(K_INFO_A);
        let go = |offset: i64| {
            let h = sigv4("GET", &path, "", &host, &amz_at(offset), SIGNED_ALL, "SK");
            let h: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            http(f.addr, "GET", &path, &h)
        };
        for offset in [-3600, -86_400, -31_536_000, 3600, 86_400] {
            let r = go(offset);
            assert_eq!((403, "AccessDenied"), (r.status, code(&r).as_str()), "{offset}s");
            assert!(r.text().contains("expired"), "{offset}s: {}", r.text());
        }
        assert_eq!(200, go(-600).status, "ten minutes old is inside the 15 minute window");
        assert_eq!(200, go(600).status, "and so is ten minutes of clock skew ahead");
        // Replay: the same request, byte for byte, again. SigV4 has no nonce; the window is the defence.
        let h = sigv4("GET", &path, "", &host, &amz_at(0), SIGNED_ALL, "SK");
        let h: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert_eq!(200, http(f.addr, "GET", &path, &h).status);
        assert_eq!(200, http(f.addr, "GET", &path, &h).status, "a replay inside the window is served: read-only, same bytes");
        // Not a date at all.
        for bad in ["", "20260101", "2026-01-01T00:00:00Z", "99999999T999999Z", "00000000T000000Z", "20261301T000000Z", "20260101T000000", &"9".repeat(10_000)] {
            let h = sigv4("GET", &path, "", &host, &amz_at(0), SIGNED_ALL, "SK");
            let mut h: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            h.iter_mut().find(|(k, _)| *k == "x-amz-date").unwrap().1 = bad;
            assert_eq!(403, http(f.addr, "GET", &path, &h).status, "date {:.20}", bad);
        }
    }

    #[test]
    fn a_presigned_query_string_is_not_credentials_for_reading() {
        let f = secured_fixture();
        let creds = Credentials { access_key: "AK".into(), secret_key: "SK".into() };
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let url = s3_sigv4::presign(&creds, "GET", &f.addr.to_string(), &obj(K_INFO_A), "us-east-1", now, 300, &[]);
        let r = f.get(&url);
        assert_eq!(403, r.status, "query-string auth is for part upload on the central, not for this server");
        // Credentials in the query beside a header-less request, in the other common spellings.
        for q in ["AWSAccessKeyId=AK&Signature=x&Expires=99999999999", "access_key=AK&secret_key=SK", "X-Amz-Credential=AK"] {
            assert_eq!(403, f.get(&format!("{}?{q}", obj(K_INFO_A))).status, "{q}");
        }
    }

    #[test]
    fn authentication_is_judged_before_anything_else_about_the_request() {
        let f = secured_fixture();
        let host = f.addr.to_string();
        // A refused write, a missing key and a traversal all look the same to a caller with no signature.
        for (method, target) in [("PUT", obj(K_INFO_A)), ("GET", obj("nope.parquet")), ("GET", "/logs/../secret.parquet".to_string()), ("DELETE", "/logs".to_string()), ("GET", "/nobucket/x".to_string())] {
            let r = http(f.addr, method, &target, &[]);
            assert_eq!((403, "AccessDenied"), (r.status, code(&r).as_str()), "{method} {target}");
        }
        // With a good signature the refusals come back to what they are.
        let h = sigv4("PUT", &obj(K_INFO_A), "", &host, &amz_at(0), SIGNED_ALL, "SK");
        let h: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert_eq!(405, http(f.addr, "PUT", &obj(K_INFO_A), &h).status);
    }

    #[test]
    fn a_huge_header_is_refused_without_buffering_it() {
        let f = Fixture::new();
        let big = "a".repeat(64 * 1024 * 1024);
        let (status, _) = raw_request(f.addr, format!("GET /logs?list-type=2 HTTP/1.1\r\nHost: {}\r\nX-Big: {big}\r\nConnection: close\r\n\r\n", f.addr).as_bytes());
        assert_eq!(431, status, "answered {status} after buffering 64 MiB");
        let many: String = (0..500).map(|i| format!("X-{i}: v\r\n")).collect();
        let (status, _) = raw_request(f.addr, format!("GET /logs?list-type=2 HTTP/1.1\r\nHost: {}\r\n{many}Connection: close\r\n\r\n", f.addr).as_bytes());
        assert_eq!(431, status, "500 headers");
        let endless = [b"GET /logs HTTP/1.1\r\nX: ".to_vec(), vec![b'z'; 8 << 20]].concat();
        assert_eq!(431, raw_request(f.addr, &endless).0, "a head with no end");
        assert_eq!(200, f.list("").status, "still serving");
    }


    #[test]
    fn a_logged_config_never_carries_the_secret_key() {
        let cfg = S3ServeConfig { access_key: Some("AKID".into()), secret_key: Some("SENTINEL-SECRET".into()), ..Default::default() };
        for shown in [format!("{cfg:?}"), format!("{cfg:#?}")] {
            assert!(!shown.contains("SENTINEL"), "{shown}");
        }
    }

}
