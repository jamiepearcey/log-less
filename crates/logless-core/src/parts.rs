//! Parquet part upload: the bulk, cold path from an edge to central.
//!
//! The record path (`sync`) sends a summary or a copy of what happens, record by record. This path
//! moves whole committed Parquet files instead. The edge pushes: it asks central where to put a part
//! and gets a URL back, and that is the whole protocol whichever backend central stores parts in
//! (`docs/adr/0003-parts-push-to-central-local-backend.md`):
//!
//! * `backend = "local"` (the default): the URL is central's own peer listener and a short-lived
//!   upload token; central streams the body to its parts directory, hashing as it goes.
//! * `backend = "s3"`: a presigned PUT URL for an S3-compatible bucket central holds the keys of, so
//!   the edge needs no cloud keys and central never carries the bytes.
//!
//! | Path (central, bearer token as `/v1/ingest`) | Behaviour |
//! |---|---|
//! | `POST /v1/parts/presign` | `{name, size, sha256}`: `already_present`, or a PUT `url` and the `headers` it needs, valid `url_ttl_secs` |
//! | `PUT /v1/parts/blob/<node>/<name>` | local backend only, authorised by the upload token alone: stores the body if it hashes to the name |
//! | `POST /v1/parts/commit` | `{name, size, sha256, rows, min_unix_nano, max_unix_nano, origin}`: checks the part is stored, records it |
//! | `POST /v1/parts/retire` | `{name}`: the node no longer holds its local copy, so the part is `cold` |
//!
//! A part is named by its content: `<sha256 hex>.parquet`, stored at `<prefix>/<node>/<name>` in the
//! bucket or `<dir>/<node>/<name>` on disk, where `<node>` is the node the bearer token belongs to
//! (`[[peer.nodes]]`). The key is built here from the token's node and a validated name; nothing in a
//! request body can choose another prefix, and a body that names another node is refused with 403
//! (see [`valid_node`]).
//!
//! # Tiers and lifecycle (`docs/adr/0001-parts-tiering-and-lifecycle.md`)
//!
//! Central owns the catalog and the lifecycle; a node owns only writes under its own prefix.
//! `hot` is a file only the node has, `warm` is an uploaded part the node may still hold, `cold` is a
//! part whose only copy is the object. A sweep (`PartsCentral::sweep`) deletes catalogued parts whose
//! newest data is older than the tier's retention and writes an audit row for each.
//! The same file uploaded twice is the same object, so every step is idempotent: a presign for an
//! object already stored intact answers `already_present`, and a repeated commit changes nothing.
//!
//! The content is checked where it lands, never taken on the edge's word. On S3 the presigned PUT
//! signs `x-amz-checksum-sha256` (the name's digest, base64), so the edge must send it and the storage
//! refuses a body that does not hash to it; "stored intact" is a HEAD with `x-amz-checksum-mode:
//! ENABLED` answering that size and that same checksum; an object of the right size without it, or
//! with another, is not present: presign hands out a URL to upload it again and commit refuses it.
//! On the local backend central hashes the body while it streams to a temporary file and renames it
//! into place only if the digest is the name ([`PartsCentral::upload`]); nothing else writes there,
//! so "stored intact" is a regular file of that name and size.
//!
//! # Replication between centrals
//!
//! A replica central pulls the parts it lacks from a source central by catalog diff: see [`replicate`].
//!
//! # The edge job
//!
//! Started by hand for a time range (`SyncControl::start_parts`), like the range fetch. It finds the
//! committed files overlapping the range, hashes each, asks for a URL, streams the file to it with
//! the headers central named (it knows nothing of the backend), and commits. A line per committed file goes to [`STATE_FILE`] in the data dir, so a rerun or a restart
//! skips what is done without hashing it again. A failed PUT is retried with backoff, and an expired
//! or refused URL is presigned again. The job runs only against a peer whose capability document
//! says `parts: true`.
//!
//! `parts_edge` runs that job on a timer, holds unconfirmed files back from retention and reclaim, and
//! retires the ones the edge later loses: every line of [`STATE_FILE`] whose file is gone.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod replicate;
pub use replicate::{ReplicateRefused, ReplicationStatus, RunReport, REPLICA_BLOB_PREFIX, REPLICA_CATALOG_PATH};

use crate::httpd::Reply;
use crate::s3_sigv4::{self, Credentials};
use crate::sync::{SyncControl, SyncTarget};

pub const PRESIGN_PATH: &str = "/v1/parts/presign";
pub const COMMIT_PATH: &str = "/v1/parts/commit";
pub const RETIRE_PATH: &str = "/v1/parts/retire";
/// Local backend: a part is uploaded with `PUT <BLOB_PREFIX><node>/<name>`.
pub const BLOB_PREFIX: &str = "/v1/parts/blob/";
/// Carries the upload token of a local-backend PUT. Central names it in the presign reply's `headers`.
pub const UPLOAD_TOKEN_HEADER: &str = "x-logless-upload-token";
/// Edge side: one JSON line per committed file, in the data dir.
pub const STATE_FILE: &str = "parts-uploaded.jsonl";
/// S3 refuses a presigned URL valid for longer than seven days.
const MAX_TTL_SECS: u64 = 7 * 24 * 3_600;
/// A URL this close to expiry is presigned again before it is used.
const EXPIRY_MARGIN_MS: u64 = 5_000;
/// Most catalog entries a replica may ask for, and a source will give, in one page.
pub const REPLICA_PAGE_MAX: usize = 1_000;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(20);
const STORAGE_TIMEOUT: Duration = Duration::from_secs(10);

/// Where central keeps uploaded parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    /// Central's own disk, under `dir`.
    Local,
    /// An S3-compatible bucket central holds the credentials of.
    S3,
}

/// `[peer.parts]` on the central instance.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// `"local"` or `"s3"`. Unset: `s3` when `endpoint` is set, `local` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<Backend>,
    /// Local backend: the parts directory. Unset: `parts` beside the catalog, in the data dir.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<PathBuf>,
    /// Local backend: the most bytes the parts directory may hold. An upload that would pass it is refused
    /// with 507. 0: no budget.
    #[serde(default = "default_disk_budget")]
    pub disk_budget_bytes: u64,
    /// `http(s)://host[:port]` of the S3 API; objects are addressed path-style.
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub bucket: String,
    /// Key prefix inside the bucket, without leading or trailing slashes. May be empty.
    #[serde(default)]
    pub prefix: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub access_key_id: String,
    /// Never serialized, logged or echoed.
    #[serde(default, skip_serializing)]
    pub secret_access_key: String,
    #[serde(default = "default_max_part_bytes")]
    pub max_part_bytes: u64,
    /// How long an upload URL works: the presigned URL's expiry on S3, the upload token's on the local backend.
    #[serde(default = "default_url_ttl")]
    pub url_ttl_secs: u64,
    /// Local backend: the slowest average rate at which a part may arrive. An upload slower than this
    /// (after a 30 s allowance) is cut off with 408 and nothing is kept, so it caps how long one upload
    /// holds a connection and a reservation of the disk budget. A part of `max_part_bytes` needs
    /// `max_part_bytes / min_upload_bytes_per_sec` seconds at the very least.
    #[serde(default = "default_min_upload_rate")]
    pub min_upload_bytes_per_sec: u64,
    /// A warm part whose newest data is older than this many seconds is expired by a sweep. 0 keeps it.
    #[serde(default)]
    pub warm_retention_secs: u64,
    /// The same for a cold part.
    #[serde(default)]
    pub cold_retention_secs: u64,
    /// Source side of replication (local backend): bearer tokens another central may use to list this one's
    /// part catalog and read its parts (`GET /v1/parts/replica/...`). Their own credential: a node token or an
    /// ingest token does not open these paths, and these open nothing else. Never serialized or logged.
    #[serde(default, skip_serializing)]
    pub replication_tokens: Vec<String>,
    /// Replica side: copy another central's parts into this one (`[peer.parts.replicate]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicate: Option<ReplicateConfig>,
}

/// `[peer.parts.replicate]`: this central is a replica of the central at `url`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicateConfig {
    /// `http(s)://host[:port]` of the source's peer listener.
    pub url: String,
    /// One of the source's `replication_tokens`. Never serialized or logged.
    #[serde(default, skip_serializing)]
    pub token: String,
    /// A run starts this often, seconds. 0: only when asked (`POST /admin/v1/parts/replicate`).
    #[serde(default = "default_replicate_interval")]
    pub interval_secs: u64,
    /// Slowest the copy may go, bytes per second over all parts, so a replica does not fill a shared link. 0: no limit.
    #[serde(default)]
    pub bytes_per_sec: u64,
    /// Catalog entries asked for per request, 1 to 1000.
    #[serde(default = "default_replicate_page")]
    pub page_size: usize,
}

impl std::fmt::Debug for ReplicateConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicateConfig")
            .field("url", &self.url)
            .field("token", &"<redacted>")
            .field("interval_secs", &self.interval_secs)
            .field("bytes_per_sec", &self.bytes_per_sec)
            .field("page_size", &self.page_size)
            .finish()
    }
}

fn default_replicate_interval() -> u64 {
    300
}
fn default_replicate_page() -> usize {
    500
}

fn default_region() -> String {
    "us-east-1".into()
}
fn default_max_part_bytes() -> u64 {
    512 * 1024 * 1024
}
fn default_url_ttl() -> u64 {
    900
}
fn default_min_upload_rate() -> u64 {
    16 * 1024
}
fn default_disk_budget() -> u64 {
    64 * 1024 * 1024 * 1024
}

impl Default for PartsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: None,
            dir: None,
            disk_budget_bytes: default_disk_budget(),
            endpoint: String::new(),
            bucket: String::new(),
            prefix: String::new(),
            region: default_region(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            max_part_bytes: default_max_part_bytes(),
            url_ttl_secs: default_url_ttl(),
            min_upload_bytes_per_sec: default_min_upload_rate(),
            warm_retention_secs: 0,
            cold_retention_secs: 0,
            replication_tokens: Vec::new(),
            replicate: None,
        }
    }
}

impl std::fmt::Debug for PartsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartsConfig")
            .field("enabled", &self.enabled)
            .field("backend", &self.backend())
            .field("dir", &self.dir)
            .field("disk_budget_bytes", &self.disk_budget_bytes)
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("max_part_bytes", &self.max_part_bytes)
            .field("url_ttl_secs", &self.url_ttl_secs)
            .field("min_upload_bytes_per_sec", &self.min_upload_bytes_per_sec)
            .field("warm_retention_secs", &self.warm_retention_secs)
            .field("cold_retention_secs", &self.cold_retention_secs)
            .field("replication_tokens", &format_args!("<{} redacted>", self.replication_tokens.len()))
            .field("replicate", &self.replicate)
            .finish()
    }
}

impl PartsConfig {
    /// Why this configuration cannot serve parts. Never mentions the secret's value.
    pub fn check(&self) -> Result<(), String> {
        if !self.enabled {
            return Err("[peer.parts] is not enabled".into());
        }
        match self.backend() {
            Backend::S3 => {
                split_endpoint(&self.endpoint)?;
                if self.bucket.is_empty() || self.bucket.contains('/') {
                    return Err("[peer.parts] bucket must be a bucket name".into());
                }
                if self.region.is_empty() || self.access_key_id.is_empty() || self.secret_access_key.is_empty() {
                    return Err("[peer.parts] needs region, access_key_id and secret_access_key".into());
                }
            }
            Backend::Local => {
                // Not named, and nothing to say it is meant to be local: a table that lost its endpoint
                // (an unset variable, a template slip) must not move the parts onto this disk unannounced.
                if self.backend.is_none() && [&self.bucket, &self.prefix, &self.access_key_id, &self.secret_access_key].iter().any(|v| !v.is_empty()) {
                    return Err("[peer.parts] looks like an S3 table without an endpoint (bucket, prefix or keys are set); set endpoint, or backend = \"local\" to keep parts on this disk".into());
                }
                if self.min_upload_bytes_per_sec == 0 {
                    return Err("[peer.parts] min_upload_bytes_per_sec must be above 0".into());
                }
                if self.dir.as_ref().is_some_and(|d| d.as_os_str().is_empty()) {
                    return Err("[peer.parts] dir must be a directory path (leave it out for <data_dir>/parts)".into());
                }
                if self.disk_budget_bytes != 0 && self.disk_budget_bytes < self.max_part_bytes {
                    return Err(format!(
                        "[peer.parts] disk_budget_bytes ({}) is under max_part_bytes ({}), so no part of the largest size could ever be stored; raise it, or set 0 for no budget",
                        self.disk_budget_bytes, self.max_part_bytes
                    ));
                }
            }
        }
        if self.max_part_bytes == 0 {
            return Err("[peer.parts] max_part_bytes must be above 0".into());
        }
        if !(1..=MAX_TTL_SECS).contains(&self.url_ttl_secs) {
            return Err(format!("[peer.parts] url_ttl_secs must be 1 to {MAX_TTL_SECS}"));
        }
        if self.replication_tokens.iter().any(String::is_empty) || self.replication_tokens.iter().enumerate().any(|(i, t)| self.replication_tokens[..i].contains(t)) {
            return Err("[peer.parts] replication_tokens must be non-empty and distinct".into());
        }
        if let Some(r) = &self.replicate {
            if self.backend() != Backend::Local {
                return Err("[peer.parts.replicate] needs this central's parts on its own disk (backend = \"local\"); a bucket is replicated with the storage's own tools".into());
            }
            if split_endpoint(&r.url).is_err() {
                return Err("[peer.parts.replicate] url must be http(s)://host[:port]".into());
            }
            if r.token.is_empty() {
                return Err("[peer.parts.replicate] needs the source's replication token".into());
            }
            if !(1..=REPLICA_PAGE_MAX).contains(&r.page_size) {
                return Err(format!("[peer.parts.replicate] page_size must be 1 to {REPLICA_PAGE_MAX}"));
            }
        }
        Ok(())
    }

    /// The directory the local backend keeps parts in, for a process whose data dir is `data_dir`.
    /// `None` when parts are off or kept in a bucket.
    pub fn local_dir(&self, data_dir: &Path) -> Option<PathBuf> {
        (self.enabled && self.backend() == Backend::Local).then(|| self.dir.clone().unwrap_or_else(|| data_dir.join("parts")))
    }

    /// The backend in force: the one named, else S3 when an endpoint is configured, else local.
    pub fn backend(&self) -> Backend {
        self.backend.unwrap_or(if self.endpoint.trim().is_empty() { Backend::Local } else { Backend::S3 })
    }
}

/// `(scheme, authority)` of an endpoint that is only a scheme and a host.
pub(crate) fn split_endpoint(endpoint: &str) -> Result<(&str, &str), String> {
    let bad = || "[peer.parts] endpoint must be http(s)://host[:port]".to_string();
    let (scheme, rest) = endpoint.trim().split_once("://").ok_or_else(bad)?;
    if !matches!(scheme, "http" | "https") {
        return Err(bad());
    }
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    if authority.is_empty() || authority.contains(['/', '?', '#', '@']) {
        return Err(bad());
    }
    Ok((scheme, authority))
}

/// A part name is exactly `<64 lowercase hex>.parquet`.
pub fn valid_name(name: &str) -> bool {
    name.len() == 72
        && name.ends_with(".parquet")
        && name[..64].bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A node id is 1 to 64 of `A-Za-z0-9._-`, not starting with a dot: one path segment, never a traversal,
/// the same in a key and in a URL, and a directory that is never taken for a temporary file or hidden from
/// the budget count and from S3 serve, which skip dot-named entries.
pub fn valid_node(node: &str) -> bool {
    (1..=64).contains(&node.len())
        && !node.starts_with('.')
        && node.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

pub fn name_for(sha256_hex: &str) -> String {
    format!("{sha256_hex}.parquet")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PresignRequest {
    pub name: String,
    pub size: u64,
    pub sha256: String,
    /// Optional. The node is the token's; a node that is named must be that one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetireRequest {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PresignReply {
    pub already_present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Headers the PUT must carry, when any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<BTreeMap<String, String>>,
    pub expires_at_ms: u64,
}

/// What an edge commits, and what the central catalog keeps (`peer_parts`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommittedPart {
    pub name: String,
    pub size: u64,
    pub sha256: String,
    pub rows: u64,
    pub min_unix_nano: u64,
    pub max_unix_nano: u64,
    pub origin: String,
    /// Optional, as in [`PresignRequest`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

const CHECKSUM_SHA256: &str = "x-amz-checksum-sha256";
const CHECKSUM_MODE: &str = "x-amz-checksum-mode";

/// The `x-amz-checksum-sha256` value for a digest given in hex: base64 of its 32 bytes.
fn checksum_b64(sha256_hex: &str) -> Option<String> {
    unhex32(sha256_hex).map(|bytes| base64(&bytes))
}

/// The 32 bytes written as 64 hex digits.
fn unhex32(hex: &str) -> Option<Vec<u8>> {
    if hex.len() != 64 {
        return None;
    }
    (0..32).map(|i| u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()).collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (chunk[0] as u32) << 16 | (*chunk.get(1).unwrap_or(&0) as u32) << 8 | *chunk.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            out.push(if i <= chunk.len() { ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char } else { '=' });
        }
    }
    out
}

fn error_reply(status: u16, code: &str, message: &str) -> Reply {
    Reply::json(status, serde_json::json!({ "error": code, "message": message }).to_string())
}

fn now_ms() -> u64 {
    crate::now_unix_nanos() / 1_000_000
}

type HmacSha256 = Hmac<Sha256>;

/// The local backend re-measures its directory before it refuses for the budget, at most this often:
/// files removed behind its counter (by hand, or by another process) are then noticed.
const REMEASURE_AFTER: Duration = Duration::from_secs(10);
const TMP_PREFIX: &str = ".upload-";
const TMP_SUFFIX: &str = ".tmp";

/// Where the bytes live. Presign, the presence check behind presign and commit, and the sweep's delete
/// go through it; everything else (the catalog, tiers, the audit) is the same for both. A restore reads
/// the same two places through `crate::restore::PartSource`.
enum Store {
    S3 { agent: ureq::Agent },
    Local(LocalStore),
}

/// Parts on central's own disk: `<dir>/<node>/<name>`. Only [`PartsCentral::upload`] writes there, and
/// only by renaming a file it hashed, so a file under its final name is a whole, verified part.
struct LocalStore {
    dir: PathBuf,
    /// Signs upload tokens. Made at start and kept in memory only: a restart voids the tokens in flight,
    /// and an edge whose token is refused asks for another.
    key: [u8; 32],
    budget: u64,
    usage: Mutex<Usage>,
    /// Held across "is it there, else rename it in", so two uploads of one part count once.
    install: Mutex<()>,
    /// The parts whose bytes are arriving, `(node, name)`. One upload per part at a time, so a token
    /// replayed on many connections cannot reserve the budget many times over.
    arriving: Mutex<std::collections::HashSet<(String, String)>>,
    stats: LocalStats,
}

struct Usage {
    /// Bytes of installed parts.
    used: u64,
    /// Bytes promised to uploads in flight.
    reserved: u64,
    measured: Instant,
}

#[derive(Default)]
struct LocalStats {
    uploads: AtomicU64,
    upload_bytes: AtomicU64,
    already_present: AtomicU64,
    refused_budget: AtomicU64,
    refused_token: AtomicU64,
    refused_size: AtomicU64,
    refused_content: AtomicU64,
    incomplete: AtomicU64,
    replica_pages: AtomicU64,
    replica_served: AtomicU64,
    refused_replica: AtomicU64,
}

/// The local backend's state, in `GET /admin/v1/sync` as `parts_store`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PartsStoreStatus {
    pub backend: Backend,
    pub dir: String,
    /// Bytes of installed parts, and bytes promised to uploads in flight.
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    /// 0: no budget.
    pub budget_bytes: u64,
    pub uploads: u64,
    pub upload_bytes: u64,
    /// Uploads of a part that was already stored: verified against the name and dropped.
    pub already_present: u64,
    /// 507s: a presign or an upload that would pass the budget, or a disk that is full.
    pub refused_budget: u64,
    /// 403s: a missing, forged, expired or replayed upload token.
    pub refused_token: u64,
    /// A `Content-Length` over `max_part_bytes` or other than the size the token was issued for.
    pub refused_size: u64,
    /// 422s: a body that does not hash to its name. Nothing is kept.
    pub refused_content: u64,
    /// Uploads that stalled or were cut off. Nothing is kept.
    pub incomplete: u64,
    /// As the source of a replica: catalog pages answered, parts served (whole or a range), requests refused for their token.
    pub replica_pages: u64,
    pub replica_served: u64,
    pub refused_replica: u64,
}

/// Bytes of the installed parts under `dir`: regular files one directory down whose names do not start
/// with a dot (temporary files do).
fn dir_bytes(dir: &Path) -> u64 {
    let visible = |e: &std::fs::DirEntry| !e.file_name().to_string_lossy().starts_with('.');
    let mut total = 0;
    for node in std::fs::read_dir(dir).into_iter().flatten().flatten().filter(visible) {
        for part in std::fs::read_dir(node.path()).into_iter().flatten().flatten().filter(visible) {
            total += std::fs::symlink_metadata(part.path()).ok().filter(|m| m.is_file()).map_or(0, |m| m.len());
        }
    }
    total
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// An upload of one part under way, see [`LocalStore::begin`].
struct Arriving<'a> {
    set: &'a Mutex<std::collections::HashSet<(String, String)>>,
    key: (String, String),
}

impl Drop for Arriving<'_> {
    fn drop(&mut self) {
        self.set.lock().unwrap_or_else(|p| p.into_inner()).remove(&self.key);
    }
}

/// Removes its file when dropped, unless the file was renamed away first.
struct TempFile(PathBuf);

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Why a body did not make it to the end.
enum Broken {
    /// The client's side: it stalled or hung up.
    Body(std::io::Error),
    /// Ours: the disk.
    Disk(std::io::Error),
}

/// Reads exactly `len` bytes of `body` in fixed-size pieces, hashing them and writing them to `out` when
/// there is one. Returns the SHA-256 in hex.
fn copy_hashing(body: &mut dyn Read, len: u64, mut out: Option<&mut std::fs::File>) -> Result<String, Broken> {
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    let mut left = len;
    while left > 0 {
        let want = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        let n = match body.read(&mut buf[..want]) {
            Ok(0) => return Err(Broken::Body(std::io::ErrorKind::UnexpectedEof.into())),
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(Broken::Body(e)),
        };
        hasher.update(&buf[..n]);
        if let Some(file) = out.as_mut() {
            file.write_all(&buf[..n]).map_err(Broken::Disk)?;
        }
        left -= n as u64;
    }
    Ok(hex(&hasher.finalize()))
}

impl LocalStore {
    fn open(config: &PartsConfig, catalog_path: &Path) -> Result<Self, String> {
        let dir = config.local_dir(catalog_path.parent().unwrap_or(Path::new("."))).ok_or("[peer.parts] is not on the local backend")?;
        std::fs::create_dir_all(&dir).map_err(|e| format!("[peer.parts] dir {} cannot be created: {e}", dir.display()))?;
        let mut key = [0u8; 32];
        getrandom::fill(&mut key).map_err(|e| format!("no random bytes for the upload token key: {e}"))?;
        let usage = Mutex::new(Usage { used: dir_bytes(&dir), reserved: 0, measured: Instant::now() });
        Ok(Self { dir, key, budget: config.disk_budget_bytes, usage, install: Mutex::new(()), arriving: Mutex::new(Default::default()), stats: LocalStats::default() })
    }

    fn usage(&self) -> std::sync::MutexGuard<'_, Usage> {
        self.usage.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn path(&self, node: &str, name: &str) -> PathBuf {
        self.dir.join(node).join(name)
    }

    /// A regular file of that name and size (see the type's note on why that is enough).
    fn present(&self, node: &str, name: &str, size: u64) -> Result<bool, String> {
        match std::fs::symlink_metadata(self.path(node, name)) {
            Ok(m) => Ok(m.is_file() && m.len() == size),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(format!("the parts directory is unreadable: {e}")),
        }
    }

    /// Marks `(node, name)` as arriving until the guard is dropped; `None` when an upload of it is already under way.
    fn begin(&self, node: &str, name: &str) -> Option<Arriving<'_>> {
        let key = (node.to_string(), name.to_string());
        let fresh = self.arriving.lock().unwrap_or_else(|p| p.into_inner()).insert(key.clone());
        // Lazily: a guard built and dropped here would remove the key of the upload that holds it.
        fresh.then(|| Arriving { set: &self.arriving, key })
    }

    /// Whether `size` more bytes fit the budget; with `reserve`, they are then promised to the caller,
    /// who gives them back with [`LocalStore::settle`].
    fn room(&self, size: u64, reserve: bool) -> bool {
        let mut usage = self.usage();
        let over = |u: &Usage| self.budget != 0 && u.used.saturating_add(u.reserved).saturating_add(size) > self.budget;
        if over(&usage) && usage.measured.elapsed() >= REMEASURE_AFTER {
            usage.used = dir_bytes(&self.dir);
            usage.measured = Instant::now();
        }
        if over(&usage) {
            self.stats.refused_budget.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        if reserve {
            usage.reserved += size;
        }
        true
    }

    /// Ends a reservation: the bytes are now an installed part, or were never kept.
    fn settle(&self, size: u64, installed: bool) {
        let mut usage = self.usage();
        usage.reserved = usage.reserved.saturating_sub(size);
        if installed {
            usage.used += size;
        }
    }

    fn delete(&self, node: &str, name: &str) -> Result<(), String> {
        let path = self.path(node, name);
        let size = std::fs::symlink_metadata(&path).map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                let mut usage = self.usage();
                usage.used = usage.used.saturating_sub(size);
                drop(usage);
                sync_dir(&self.dir.join(node)).map_err(|e| format!("the parts directory could not be synced after a delete: {e}"))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!("the part could not be deleted: {e}")),
        }
    }

    /// `v1.<expires, unix seconds>.<size>.<hex HMAC-SHA256>`: good for one PUT target, `(node, name)`, of
    /// exactly `size` bytes, until `expires`. The name is the SHA-256, so the content is bound too.
    fn token(&self, node: &str, name: &str, size: u64, expires_secs: u64) -> String {
        format!("v1.{expires_secs}.{size}.{}", hex(&self.mac(node, name, size, expires_secs).finalize().into_bytes()))
    }

    fn mac(&self, node: &str, name: &str, size: u64, expires_secs: u64) -> HmacSha256 {
        // HMAC accepts any key length, so new_from_slice cannot fail.
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac key length is unrestricted");
        let sha256 = name.strip_suffix(".parquet").unwrap_or(name);
        mac.update(format!("logless-part-upload/1\n{node}\n{name}\n{size}\n{sha256}\n{expires_secs}").as_bytes());
        mac
    }

    /// The size a token was issued for, when it is this store's token for `(node, name)` and has not
    /// expired. The error is the code to answer with.
    fn check_token(&self, token: &str, node: &str, name: &str, now_secs: u64) -> Result<u64, &'static str> {
        let mut fields = token.split('.');
        let (Some("v1"), Some(expires), Some(size), Some(mac), None) = (fields.next(), fields.next(), fields.next(), fields.next(), fields.next()) else {
            return Err("bad_token");
        };
        let digits = |s: &str| !s.is_empty() && s.len() <= 20 && s.bytes().all(|b| b.is_ascii_digit());
        let (true, true, Some(mac)) = (digits(expires), digits(size), unhex32(mac)) else { return Err("bad_token") };
        let (Ok(expires), Ok(size)) = (expires.parse::<u64>(), size.parse::<u64>()) else { return Err("bad_token") };
        // Constant time, and before the expiry is looked at: a forged token learns nothing from the answer.
        if self.mac(node, name, size, expires).verify_slice(&mac).is_err() {
            return Err("bad_token");
        }
        if now_secs >= expires {
            return Err("token_expired");
        }
        Ok(size)
    }

    /// Removes what uploads cut off by a crash left behind. Only at start, before anything is served:
    /// later a temporary file belongs to an upload in flight.
    fn clear_temporaries(&self) -> u64 {
        let mut removed = 0;
        for node in std::fs::read_dir(&self.dir).into_iter().flatten().flatten() {
            for entry in std::fs::read_dir(node.path()).into_iter().flatten().flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with(TMP_PREFIX) && name.ends_with(TMP_SUFFIX) && std::fs::remove_file(entry.path()).is_ok() {
                    removed += 1;
                }
            }
        }
        removed
    }

    fn status(&self) -> PartsStoreStatus {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let (used_bytes, reserved_bytes) = {
            let usage = self.usage();
            (usage.used, usage.reserved)
        };
        PartsStoreStatus {
            backend: Backend::Local,
            dir: self.dir.display().to_string(),
            used_bytes,
            reserved_bytes,
            budget_bytes: self.budget,
            uploads: load(&self.stats.uploads),
            upload_bytes: load(&self.stats.upload_bytes),
            already_present: load(&self.stats.already_present),
            refused_budget: load(&self.stats.refused_budget),
            refused_token: load(&self.stats.refused_token),
            refused_size: load(&self.stats.refused_size),
            refused_content: load(&self.stats.refused_content),
            incomplete: load(&self.stats.incomplete),
            replica_pages: load(&self.stats.replica_pages),
            replica_served: load(&self.stats.replica_served),
            refused_replica: load(&self.stats.refused_replica),
        }
    }
}

/// Central's half: says where a part goes, checks it arrived and records it. Built only from a
/// configuration that passed [`PartsConfig::check`].
pub struct PartsCentral {
    config: PartsConfig,
    catalog_path: PathBuf,
    store: Store,
    /// Present when `[peer.parts.replicate]` is set.
    replication: Option<replicate::Replication>,
}

impl PartsCentral {
    pub fn new(config: PartsConfig, catalog_path: PathBuf) -> Result<Self, String> {
        config.check()?;
        let store = match config.backend() {
            Backend::S3 => Store::S3 { agent: ureq::Agent::config_builder().timeout_global(Some(STORAGE_TIMEOUT)).http_status_as_error(false).build().into() },
            Backend::Local => Store::Local(LocalStore::open(&config, &catalog_path)?),
        };
        let replication = config.replicate.clone().map(replicate::Replication::new);
        Ok(Self { config, catalog_path, store, replication })
    }

    /// For the process that serves uploads, once, before it listens: clears the temporary files a crash
    /// left in the local parts directory. Returns how many.
    pub fn recover(&self) -> u64 {
        match &self.store {
            Store::Local(local) => local.clear_temporaries(),
            Store::S3 { .. } => 0,
        }
    }

    /// The local backend's directory, budget and counters. `None` on S3, where the bucket is the store.
    pub fn store_status(&self) -> Option<PartsStoreStatus> {
        match &self.store {
            Store::Local(local) => Some(local.status()),
            Store::S3 { .. } => None,
        }
    }

    /// The local backend's directory, for a reader of it (S3 serve, a restore).
    pub fn local_dir(&self) -> Option<&Path> {
        match &self.store {
            Store::Local(local) => Some(&local.dir),
            Store::S3 { .. } => None,
        }
    }

    fn raw_key_path(&self, node: &str, name: &str) -> String {
        let mut path = format!("/{}", s3_sigv4::encode_segment(&self.config.bucket));
        for segment in self.config.prefix.split('/').filter(|s| !s.is_empty()) {
            path.push('/');
            path.push_str(&s3_sigv4::encode_segment(segment));
        }
        path.push('/');
        path.push_str(&s3_sigv4::encode_segment(node));
        path.push('/');
        path.push_str(name);
        path
    }

    /// A presigned URL for `method` on the part's key, signing `headers` (which the request must then
    /// carry), and when it stops working.
    fn presigned(&self, method: &str, node: &str, name: &str, headers: &[(&str, &str)]) -> (String, u64) {
        let (scheme, host) = split_endpoint(&self.config.endpoint).expect("checked at construction");
        let creds = Credentials { access_key: self.config.access_key_id.clone(), secret_key: self.config.secret_access_key.clone() };
        let now = crate::now_unix_nanos() / 1_000_000_000;
        let raw_path = self.raw_key_path(node, name);
        let path_query = s3_sigv4::presign(&creds, method, host, &raw_path, &self.config.region, now, self.config.url_ttl_secs, headers);
        (format!("{scheme}://{host}{path_query}"), (now + self.config.url_ttl_secs) * 1_000)
    }

    /// Whether the part is stored whole: on S3 an object with exactly this size and SHA-256, as the
    /// storage itself reports it (a wrong size, no checksum or another checksum is not intact); on the
    /// local backend a file of that name and size, which only a verified upload can have put there.
    fn stored_intact(&self, node: &str, name: &str, size: u64, sha256: &str) -> Result<bool, String> {
        let agent = match &self.store {
            Store::S3 { agent } => agent,
            Store::Local(local) => return local.present(node, name, size),
        };
        let (url, _) = self.presigned("HEAD", node, name, &[(CHECKSUM_MODE, "ENABLED")]);
        let response = agent.head(&url).header(CHECKSUM_MODE, "ENABLED").call().map_err(|e| format!("object storage is unreachable: {e}"))?;
        let header = |n: &str| response.headers().get(n).and_then(|v| v.to_str().ok()).map(str::trim);
        match response.status().as_u16() {
            200 => Ok(header("content-length").and_then(|v| v.parse::<u64>().ok()) == Some(size)
                && checksum_b64(sha256).is_some_and(|c| header(CHECKSUM_SHA256) == Some(c.as_str()))),
            404 => Ok(false),
            other => Err(format!("object storage answered HTTP {other} to a HEAD")),
        }
    }

    /// The request's node, if it names one, must be the token's: a node never reaches another's prefix.
    fn check_node(&self, node: &str, claimed: Option<&str>) -> Result<(), Reply> {
        if !valid_node(node) {
            return Err(error_reply(403, "bad_node", "the node id of this token cannot name a prefix"));
        }
        match claimed {
            Some(c) if c != node => Err(error_reply(403, "foreign_prefix", "a node writes only under its own prefix")),
            _ => Ok(()),
        }
    }

    fn check_part(&self, name: &str, size: u64, sha256: &str) -> Result<(), Reply> {
        if !valid_name(name) {
            return Err(error_reply(422, "bad_name", "a part name is <64 lowercase hex sha256>.parquet"));
        }
        if &name[..64] != sha256 {
            return Err(error_reply(422, "bad_hash", "the name must be the sha256 of the part"));
        }
        if size == 0 || size > self.config.max_part_bytes {
            return Err(error_reply(422, "bad_size", &format!("a part is 1 to {} bytes", self.config.max_part_bytes)));
        }
        Ok(())
    }

    pub fn presign(&self, node: &str, body: &[u8]) -> Reply {
        let req: PresignRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return error_reply(400, "bad_request", &format!("the body is not a presign request: {e}")),
        };
        if let Err(reply) = self.check_node(node, req.node.as_deref()) {
            return reply;
        }
        if let Err(reply) = self.check_part(&req.name, req.size, &req.sha256) {
            return reply;
        }
        let reply = match self.stored_intact(node, &req.name, req.size, &req.sha256) {
            Ok(true) => PresignReply { already_present: true, url: None, headers: None, expires_at_ms: 0 },
            // Absent, or something else under a content-addressed name: either way, upload it. The
            // checksum is signed, so the edge must send it and storage refuses a body that differs.
            Ok(false) => match &self.store {
                Store::S3 { .. } => {
                    let checksum = checksum_b64(&req.sha256).expect("checked by check_part");
                    let (url, expires_at_ms) = self.presigned("PUT", node, &req.name, &[(CHECKSUM_SHA256, &checksum)]);
                    let headers = BTreeMap::from([(CHECKSUM_SHA256.to_string(), checksum)]);
                    PresignReply { already_present: false, url: Some(url), headers: Some(headers), expires_at_ms }
                }
                // A path on this listener (the edge resolves it against the peer URL it already uses, so
                // central need not know its own public name) and a token for this one part.
                Store::Local(local) => {
                    if !local.room(req.size, false) {
                        return insufficient_storage();
                    }
                    let expires = crate::now_unix_secs() + self.config.url_ttl_secs;
                    // `Expect` so a refusal from the head (an expired token, a full disk) is heard before the body is sent.
                    let headers = BTreeMap::from([
                        (UPLOAD_TOKEN_HEADER.to_string(), local.token(node, &req.name, req.size, expires)),
                        ("expect".to_string(), "100-continue".to_string()),
                    ]);
                    PresignReply { already_present: false, url: Some(format!("{BLOB_PREFIX}{node}/{}", req.name)), headers: Some(headers), expires_at_ms: expires * 1_000 }
                }
            },
            Err(m) => return error_reply(502, "storage_unavailable", &m),
        };
        Reply::json(200, serde_json::to_string(&reply).unwrap_or_default())
    }

    pub fn commit(&self, node: &str, body: &[u8]) -> Reply {
        let part: CommittedPart = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return error_reply(400, "bad_request", &format!("the body is not a commit request: {e}")),
        };
        if let Err(reply) = self.check_node(node, part.node.as_deref()) {
            return reply;
        }
        if let Err(reply) = self.check_part(&part.name, part.size, &part.sha256) {
            return reply;
        }
        if part.origin.is_empty() || part.origin.len() > 128 || part.min_unix_nano > part.max_unix_nano {
            return error_reply(422, "bad_part", "origin is 1 to 128 characters and min_unix_nano is at most max_unix_nano");
        }
        match self.stored_intact(node, &part.name, part.size, &part.sha256) {
            Ok(true) => {}
            Ok(false) => return error_reply(409, "not_uploaded", "no part of that name, size and SHA-256 is stored; upload it first"),
            Err(m) => return error_reply(502, "storage_unavailable", &m),
        }
        let recorded = crate::catalog::Catalog::open(&self.catalog_path).and_then(|c| c.record_peer_part(node, &part, now_ms()));
        match recorded {
            Ok(recorded) => Reply::json(200, serde_json::json!({ "recorded": recorded }).to_string()),
            Err(e) => error_reply(503, "catalog_busy", &format!("the part could not be recorded: {e}")).with_header("Retry-After", "1"),
        }
    }
}

fn insufficient_storage() -> Reply {
    error_reply(507, "insufficient_storage", "the parts directory is at its disk budget ([peer.parts] disk_budget_bytes)").with_header("Retry-After", "60")
}

impl PartsCentral {
    /// `PUT /v1/parts/blob/<node>/<name>` on the local backend: `target` is what follows the prefix,
    /// `token` the one upload token header, `len` the declared `Content-Length`, `body` the request body
    /// still on the wire. Everything that can be refused from the head is refused before a byte of the
    /// body is read. Then the body goes to a temporary file beside its destination in fixed-size pieces
    /// (never held whole in memory), hashed on the way; only a body whose SHA-256 is the name is synced,
    /// renamed into place and its directory synced, and only then is the answer 200. A part already
    /// stored is read and checked against the name but written nowhere.
    pub fn upload(&self, target: &str, token: Option<&str>, len: u64, body: &mut dyn Read) -> Reply {
        let Store::Local(local) = &self.store else {
            return error_reply(404, "not_found", "this peer does not store parts itself");
        };
        let count = |n: &AtomicU64| {
            n.fetch_add(1, Ordering::Relaxed);
        };
        let Some((node, name)) = target.split_once('/') else {
            return error_reply(404, "not_found", "no such path");
        };
        if !valid_node(node) {
            return error_reply(403, "bad_node", "not a node id");
        }
        if !valid_name(name) {
            return error_reply(422, "bad_name", "a part name is <64 lowercase hex sha256>.parquet");
        }
        let size = match token.ok_or("bad_token").and_then(|t| local.check_token(t, node, name, crate::now_unix_secs())) {
            Ok(size) => size,
            Err(code) => {
                count(&local.stats.refused_token);
                return error_reply(403, code, "the upload token is missing, expired or not for this part; presign again");
            }
        };
        if len > self.config.max_part_bytes {
            count(&local.stats.refused_size);
            return error_reply(413, "too_large", &format!("a part is 1 to {} bytes", self.config.max_part_bytes));
        }
        if len != size {
            count(&local.stats.refused_size);
            return error_reply(400, "size_mismatch", &format!("Content-Length must be the {size} bytes the URL was issued for"));
        }
        let broken = |why: Broken| match why {
            Broken::Body(e) => {
                count(&local.stats.incomplete);
                match e.kind() {
                    std::io::ErrorKind::TimedOut => error_reply(408, "upload_stalled", "the body did not arrive in time; nothing was stored"),
                    _ => error_reply(400, "incomplete_body", "the body ended before Content-Length; nothing was stored"),
                }
            }
            Broken::Disk(e) if e.kind() == std::io::ErrorKind::StorageFull => {
                count(&local.stats.refused_budget);
                insufficient_storage()
            }
            Broken::Disk(e) => {
                tracing::warn!(error = %e, "a part upload could not be written");
                error_reply(500, "storage_error", "the part could not be written")
            }
        };
        let mismatch = || {
            count(&local.stats.refused_content);
            error_reply(422, "bad_content", "the body's SHA-256 is not the part's name; nothing was stored")
        };
        let stored = |fresh: bool| Reply::json(200, serde_json::json!({ "stored": fresh, "already_present": !fresh }).to_string());

        match local.present(node, name, size) {
            Ok(false) => {}
            // Parts are immutable: the same bytes again change nothing, and other bytes are refused.
            Ok(true) => {
                return match copy_hashing(body, len, None) {
                    Ok(sha256) if sha256 == name[..64] => {
                        count(&local.stats.already_present);
                        stored(false)
                    }
                    Ok(_) => mismatch(),
                    Err(why) => broken(why),
                }
            }
            Err(m) => return error_reply(500, "storage_error", &m),
        }
        // Not while the same part is already arriving: it could be installed once, and each copy would hold
        // `size` bytes of the budget for as long as its sender cares to dribble.
        let Some(_arriving) = local.begin(node, name) else {
            return error_reply(409, "upload_in_progress", "this part is already being uploaded; wait for it, then retry").with_header("Retry-After", "1");
        };
        if !local.room(size, true) {
            return insufficient_storage();
        }
        let node_dir = local.dir.join(node);
        let tmp = TempFile(node_dir.join(format!("{TMP_PREFIX}{}{TMP_SUFFIX}", uuid::Uuid::now_v7().simple())));
        let written = std::fs::create_dir_all(&node_dir)
            .and_then(|()| std::fs::OpenOptions::new().write(true).create_new(true).open(&tmp.0))
            .map_err(Broken::Disk)
            .and_then(|mut file| {
                let sha256 = copy_hashing(body, len, Some(&mut file))?;
                if sha256 == name[..64] {
                    file.sync_all().map_err(Broken::Disk)?;
                }
                Ok(sha256)
            });
        let outcome = match written {
            Ok(sha256) if sha256 == name[..64] => {
                let _installing = local.install.lock().unwrap_or_else(|p| p.into_inner());
                match local.present(node, name, size) {
                    // Another upload of the same part won the race; this copy is dropped with its temporary file.
                    Ok(true) => Ok(false),
                    _ => std::fs::rename(&tmp.0, local.path(node, name)).and_then(|()| sync_dir(&node_dir)).and_then(|()| sync_dir(&local.dir)).map(|()| true).map_err(Broken::Disk),
                }
            }
            Ok(_) => {
                local.settle(size, false);
                return mismatch();
            }
            Err(why) => Err(why),
        };
        local.settle(size, matches!(outcome, Ok(true)));
        match outcome {
            Ok(true) => {
                count(&local.stats.uploads);
                local.stats.upload_bytes.fetch_add(size, Ordering::Relaxed);
                stored(true)
            }
            Ok(false) => {
                count(&local.stats.already_present);
                stored(false)
            }
            Err(why) => broken(why),
        }
    }

    /// `POST /v1/parts/retire`: the node says it dropped its local copy of one of its own parts.
    pub fn retire(&self, node: &str, body: &[u8]) -> Reply {
        let req: RetireRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return error_reply(400, "bad_request", &format!("the body is not a retire request: {e}")),
        };
        if let Err(reply) = self.check_node(node, req.node.as_deref()) {
            return reply;
        }
        if !valid_name(&req.name) {
            return error_reply(422, "bad_name", "a part name is <64 lowercase hex sha256>.parquet");
        }
        match crate::catalog::Catalog::open(&self.catalog_path).and_then(|mut c| c.mark_peer_part_cold(node, &req.name, now_ms())) {
            Ok(true) => Reply::json(200, serde_json::json!({ "retired": true, "tier": "cold" }).to_string()),
            // Not this node's, not committed, or cold already: the same answer, so a node learns nothing about another's parts.
            Ok(false) => error_reply(404, "no_such_part", "this node has no warm part of that name"),
            Err(e) => error_reply(503, "catalog_busy", &format!("the part could not be updated: {e}")).with_header("Retry-After", "1"),
        }
    }

    /// Deletes the part at `<prefix>/<node>/<name>` (or `<dir>/<node>/<name>`). A missing one counts as deleted.
    fn delete_object(&self, node: &str, name: &str) -> Result<(), String> {
        if !valid_node(node) || !valid_name(name) {
            return Err("the catalogued node or name is not valid; refusing to build a key from it".into());
        }
        let agent = match &self.store {
            Store::S3 { agent } => agent,
            Store::Local(local) => return local.delete(node, name),
        };
        let (url, _) = self.presigned("DELETE", node, name, &[]);
        let response = agent.delete(&url).call().map_err(|e| format!("object storage is unreachable: {e}"))?;
        match response.status().as_u16() {
            200 | 204 | 404 => Ok(()),
            other => Err(format!("object storage answered HTTP {other} to a DELETE")),
        }
    }

    /// Expires catalogued parts older than the tier's retention: those whose newest data is before
    /// `now - retention`. It looks at nothing but the catalog, so an object nobody committed is never
    /// touched, and writes an audit row for every part it acts on. At most [`SWEEP_BATCH`] parts per
    /// tier per call; `truncated` says there were more. `dry_run` lists what would go and changes nothing.
    pub fn sweep(&self, now_ms: u64, dry_run: bool) -> Result<SweepReport, String> {
        let mut catalog = crate::catalog::Catalog::open(&self.catalog_path).map_err(|e| e.to_string())?;
        let mut report = SweepReport { dry_run, ..SweepReport::default() };
        for (tier, secs) in [("warm", self.config.warm_retention_secs), ("cold", self.config.cold_retention_secs)] {
            if secs == 0 {
                continue;
            }
            let cutoff_nano = now_ms.saturating_sub(secs.saturating_mul(1_000)).saturating_mul(1_000_000);
            let filter = crate::catalog::PartsFilter {
                tier: Some(tier.into()),
                newest_before_nano: Some(cutoff_nano),
                limit: SWEEP_BATCH + 1,
                ..Default::default()
            };
            let mut due = catalog.list_peer_parts(&filter).map_err(|e| e.to_string())?;
            if due.len() > SWEEP_BATCH {
                due.truncate(SWEEP_BATCH);
                report.truncated = true;
            }
            for part in due {
                if dry_run {
                    report.would_delete += 1;
                } else {
                    let detail = format!("{tier} retention {secs}s: newest data {} ns is before {cutoff_nano} ns", part.max_unix_nano);
                    match self.delete_object(&part.node, &part.name) {
                        Ok(()) => {
                            catalog.finish_peer_part_expiry(&part, now_ms, &detail, true).map_err(|e| e.to_string())?;
                            report.deleted += 1;
                        }
                        Err(m) => {
                            catalog.finish_peer_part_expiry(&part, now_ms, &format!("{detail}; {m}"), false).map_err(|e| e.to_string())?;
                            report.failed += 1;
                        }
                    }
                }
                report.parts.push(part);
            }
        }
        Ok(report)
    }
}

/// Parts a sweep acts on per tier per call.
pub const SWEEP_BATCH: usize = 1_000;

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct SweepReport {
    pub dry_run: bool,
    pub deleted: u64,
    pub failed: u64,
    pub would_delete: u64,
    pub truncated: bool,
    pub parts: Vec<crate::catalog::PeerPartRow>,
}

/// The edge job's progress, in `SyncStatus.parts`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PartsJobStatus {
    /// `running`, `done`, `cancelled` or `error`.
    pub state: String,
    pub from_ms: u64,
    pub to_ms: u64,
    pub files_total: u64,
    pub uploaded: u64,
    /// Committed by an earlier run (the state file says so).
    pub skipped: u64,
    /// Central already had the object; committed without uploading.
    pub already_present: u64,
    /// Refused by central for good (too large, for one).
    pub rejected: u64,
    pub bytes: u64,
    pub started_ms: u64,
    pub finished_ms: Option<u64>,
    pub error: Option<String>,
}

impl PartsJobStatus {
    pub fn starting(from_ms: u64, to_ms: u64) -> Self {
        Self {
            state: "running".into(),
            from_ms,
            to_ms,
            files_total: 0,
            uploaded: 0,
            skipped: 0,
            already_present: 0,
            rejected: 0,
            bytes: 0,
            started_ms: now_ms(),
            finished_ms: None,
            error: None,
        }
    }
}

/// How hard the job tries before it gives up on a file.
#[derive(Debug, Clone, Copy)]
pub struct Tuning {
    pub attempts: u32,
    pub backoff: Duration,
}

impl Default for Tuning {
    fn default() -> Self {
        Self { attempts: 6, backoff: Duration::from_secs(1) }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateLine {
    pub(crate) path: String,
    pub(crate) size: u64,
    pub(crate) name: String,
}

/// Held while the state file is appended to or rewritten, so a retire pass dropping lines cannot lose a line
/// the upload job appends meanwhile.
static STATE_LOCK: Mutex<()> = Mutex::new(());

/// Every file a run already committed, torn lines skipped.
pub(crate) fn load_state_lines(state_dir: &Path) -> Vec<StateLine> {
    let Ok(text) = std::fs::read_to_string(state_dir.join(STATE_FILE)) else { return Vec::new() };
    // A torn last line (a crash mid-append) is skipped: that file is uploaded again, which is idempotent.
    text.lines().filter_map(|l| serde_json::from_str::<StateLine>(l).ok()).collect()
}

/// `(path relative to the store, size)` of every file a run already committed.
pub(crate) fn load_state(state_dir: &Path) -> HashMap<String, u64> {
    load_state_lines(state_dir).into_iter().map(|l| (l.path, l.size)).collect()
}

fn append_state(state_dir: &Path, line: &StateLine) -> std::io::Result<()> {
    let _guard = STATE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(state_dir.join(STATE_FILE))?;
    let mut bytes = serde_json::to_vec(line).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    file.write_all(&bytes)?;
    file.sync_data()
}

/// Forgets the lines for `paths` (files whose removal central has been told of): the rest of the file is
/// written to a temporary file, synced and renamed over it.
pub(crate) fn drop_state(state_dir: &Path, paths: &[String]) -> std::io::Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let _guard = STATE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let keep: Vec<StateLine> = load_state_lines(state_dir).into_iter().filter(|l| !paths.contains(&l.path)).collect();
    let tmp = state_dir.join(format!("{STATE_FILE}.tmp"));
    {
        let mut file = std::fs::File::create(&tmp)?;
        for line in &keep {
            let mut bytes = serde_json::to_vec(line).map_err(std::io::Error::other)?;
            bytes.push(b'\n');
            file.write_all(&bytes)?;
        }
        file.sync_all()?;
    }
    std::fs::rename(tmp, state_dir.join(STATE_FILE))
}

/// Tells central this node no longer holds `name` (`POST /v1/parts/retire`). `Ok` means central has recorded it
/// or has nothing to record (no warm part of that name: cold already, or never committed); `Err` is worth
/// retrying later (unreachable, busy, a token it refuses).
pub(crate) fn retire_remote(target: &SyncTarget, name: &str) -> Result<(), String> {
    let cancel = AtomicBool::new(false);
    let agent = || -> ureq::Agent { ureq::Agent::config_builder().timeout_global(Some(CONTROL_TIMEOUT)).http_status_as_error(false).build().into() };
    let uploader = Uploader { target, control: agent(), storage: agent(), tuning: Tuning::default(), cancel: &cancel };
    match uploader.post(RETIRE_PATH, &RetireRequest { name: name.into(), node: None }) {
        Ok((200, _)) => Ok(()),
        Ok((404, v)) if v["error"] == "no_such_part" => Ok(()),
        // A name central will never take; keeping it would block every retire behind it.
        Ok((status @ (400 | 422), v)) => {
            tracing::warn!(name, status, message = v["message"].as_str().unwrap_or(""), "central refused a retire for good; dropping it");
            Ok(())
        }
        Ok((status, v)) => Err(format!("The peer answered HTTP {status} to a retire. {}", v["message"].as_str().unwrap_or("")).trim().to_string()),
        Err(m) => Err(m),
    }
}

fn sha256_file(path: &Path) -> std::io::Result<(String, u64)> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    let mut size = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((hasher.finalize().iter().map(|b| format!("{b:02x}")).collect(), size))
}

/// Why one file did not go up.
enum Failure {
    /// Central refused this file for good; move on to the next.
    Rejected(String),
    /// Nothing more will work (wrong token, peer without parts, cancelled, out of attempts).
    Fatal(String),
}

struct Uploader<'a> {
    target: &'a SyncTarget,
    control: ureq::Agent,
    storage: ureq::Agent,
    tuning: Tuning,
    cancel: &'a AtomicBool,
}

enum Sent {
    Uploaded,
    AlreadyPresent,
}

impl Uploader<'_> {
    fn post(&self, path: &str, body: &impl Serialize) -> Result<(u16, serde_json::Value), String> {
        let mut request = self.control.post(&format!("{}{path}", self.target.url.trim_end_matches('/'))).header("Content-Type", "application/json");
        if let Some(token) = &self.target.token {
            request = request.header("Authorization", &format!("Bearer {token}"));
        }
        let body = serde_json::to_vec(body).map_err(|e| e.to_string())?;
        let mut response = request.send(&body[..]).map_err(|e| format!("could not reach the peer: {e}"))?;
        let status = response.status().as_u16();
        Ok((status, crate::sync::read_json(response.body_mut()).unwrap_or(serde_json::Value::Null)))
    }

    /// Sleeps the backoff for `attempt`, in slices so a cancel is noticed. False when cancelled.
    fn wait(&self, attempt: u32) -> bool {
        let total = self.tuning.backoff.saturating_mul(1 << attempt.min(5)).min(Duration::from_secs(30));
        let until = std::time::Instant::now() + total;
        while std::time::Instant::now() < until {
            if self.cancel.load(Ordering::Acquire) {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10).min(total));
        }
        !self.cancel.load(Ordering::Acquire)
    }

    /// A failure answer from central, sorted into retry (Ok) or give up (Err).
    fn classify(status: u16, reply: &serde_json::Value) -> Result<String, Failure> {
        let message = reply["message"].as_str().unwrap_or("").to_string();
        match status {
            401 | 403 => Err(Failure::Fatal(format!("The peer refused the token (HTTP {status}).").trim().to_string())),
            404 | 405 => Err(Failure::Fatal("The peer does not accept parts.".into())),
            400 | 413 | 422 => Err(Failure::Rejected(format!("The peer refused a part (HTTP {status}): {message}"))),
            other => Ok(format!("The peer answered HTTP {other}. {message}").trim().to_string()),
        }
    }

    fn send(&self, path: &Path, part: &CommittedPart) -> Result<Sent, Failure> {
        let mut last = String::new();
        let mut presigned: Option<PresignReply> = None;
        let mut put_done = false;
        let mut stalled = 0u32;
        let mut attempt = 0u32;
        while attempt < self.tuning.attempts {
            if self.cancel.load(Ordering::Acquire) {
                return Err(Failure::Fatal("cancelled".into()));
            }
            // 1. A URL, unless the one in hand is still good.
            let fresh = presigned.as_ref().is_some_and(|p| p.already_present || p.expires_at_ms > now_ms() + EXPIRY_MARGIN_MS);
            if !put_done && !fresh {
                let request = PresignRequest { name: part.name.clone(), size: part.size, sha256: part.sha256.clone(), node: None };
                match self.post(PRESIGN_PATH, &request) {
                    Ok((200, v)) => match serde_json::from_value::<PresignReply>(v) {
                        Ok(p) => presigned = Some(p),
                        Err(e) => last = format!("The peer's presign answer is unreadable: {e}"),
                    },
                    Ok((status, v)) => last = Self::classify(status, &v)?,
                    Err(m) => last = m,
                }
                if presigned.is_none() {
                    attempt += 1;
                    if !self.wait(attempt) {
                        return Err(Failure::Fatal("cancelled".into()));
                    }
                    continue;
                }
            }
            let Some(p) = presigned.clone() else { continue };
            // 2. The upload, streamed from the file.
            if !put_done && !p.already_present {
                let Some(url) = p.url.as_deref() else {
                    return Err(Failure::Rejected("The peer presigned no URL.".into()));
                };
                // A path is on the peer itself (it stores parts on its own disk); a full URL is used as given.
                let url = if url.starts_with('/') { format!("{}{url}", self.target.url.trim_end_matches('/')) } else { url.to_string() };
                let file = std::fs::File::open(path).map_err(|e| Failure::Rejected(format!("{} could not be read: {e}", path.display())))?;
                let mut request = self.storage.put(&url);
                for (k, v) in p.headers.iter().flatten() {
                    request = request.header(k, v);
                }
                match request.send(file) {
                    Ok(r) if r.status().is_success() => put_done = true,
                    Ok(r) if r.status().as_u16() == 403 => {
                        // Expired or refused: a new URL, at once.
                        last = "The upload URL was refused (HTTP 403); presigning again.".into();
                        presigned = None;
                        attempt += 1;
                        continue;
                    }
                    Ok(r) => {
                        let status = r.status().as_u16();
                        // Cut off for arriving too slowly ([peer.parts] min_upload_bytes_per_sec): once may be a
                        // blip, twice is this link and this part, and the next files may be smaller.
                        stalled += u32::from(status == 408);
                        if stalled >= 2 {
                            return Err(Failure::Rejected(format!("The part store cut the upload of {} off as too slow (HTTP 408, twice); the link is under its minimum upload rate.", part.name)));
                        }
                        last = format!("The part store answered HTTP {status} to the upload.");
                    }
                    Err(e) => last = format!("The upload failed: {e}"),
                }
                if !put_done {
                    attempt += 1;
                    if !self.wait(attempt) {
                        return Err(Failure::Fatal("cancelled".into()));
                    }
                    continue;
                }
            }
            // 3. The commit.
            match self.post(COMMIT_PATH, part) {
                Ok((200, _)) => return Ok(if p.already_present && !put_done { Sent::AlreadyPresent } else { Sent::Uploaded }),
                Ok((409, v)) => {
                    // Central cannot see the object: upload again.
                    last = format!("The peer could not find the uploaded part. {}", v["message"].as_str().unwrap_or("")).trim().to_string();
                    presigned = None;
                    put_done = false;
                }
                Ok((status, v)) => last = Self::classify(status, &v)?,
                Err(m) => last = m,
            }
            attempt += 1;
            if !self.wait(attempt) {
                return Err(Failure::Fatal("cancelled".into()));
            }
        }
        Err(Failure::Fatal(format!("Gave up on {} after {} attempts: {last}", part.name, self.tuning.attempts)))
    }
}

/// Runs one upload job to the end, reporting through `control`. Called on the job's own thread.
pub(crate) fn run_job(control: &SyncControl, target: &SyncTarget, store_dir: &Path, state_dir: &Path, from_ms: u64, to_ms: u64, tuning: Tuning, cancel: &AtomicBool) {
    let finish = |state: &str, error: Option<String>| {
        control.update_parts(|p| {
            p.state = state.into();
            p.finished_ms = Some(now_ms());
            if error.is_some() {
                p.error = error;
            }
        })
    };
    match crate::sync::probe(&target.url, target.token.as_deref()) {
        Ok(probe) if probe.capabilities.parts => {}
        Ok(_) => return finish("error", Some("The peer does not accept parts (its capability document says parts: false).".into())),
        Err(m) => return finish("error", Some(m)),
    }
    let (from_ns, to_ns) = ((from_ms * 1_000_000) as i64, (to_ms * 1_000_000) as i64);
    let files: Vec<(PathBuf, crate::store::FileStats)> = crate::store::list_parquet_files(store_dir)
        .into_iter()
        .filter_map(|path| crate::store::read_file_stats(&path).ok().map(|s| (path, s)))
        .filter(|(_, s)| s.rows > 0 && s.min_observed_nanos < to_ns && s.max_observed_nanos >= from_ns)
        .collect();
    control.update_parts(|p| p.files_total = files.len() as u64);
    let done = load_state(state_dir);
    let uploader = Uploader {
        target,
        control: ureq::Agent::config_builder().timeout_global(Some(CONTROL_TIMEOUT)).http_status_as_error(false).build().into(),
        // No overall limit: a large part takes as long as the link needs. Stalls are still bounded.
        storage: ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(10)))
            .timeout_recv_response(Some(Duration::from_secs(120)))
            .http_status_as_error(false)
            .build()
            .into(),
        tuning,
        cancel,
    };
    for (path, stats) in files {
        if cancel.load(Ordering::Acquire) {
            return finish("cancelled", None);
        }
        let relative = path.strip_prefix(store_dir).unwrap_or(&path).to_string_lossy().into_owned();
        let size_now = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        if done.get(&relative) == Some(&size_now) {
            control.update_parts(|p| p.skipped += 1);
            continue;
        }
        let (sha256, size) = match sha256_file(&path) {
            Ok(h) => h,
            // Removed by retention or compaction since it was listed: nothing to upload.
            Err(_) => {
                control.update_parts(|p| p.skipped += 1);
                continue;
            }
        };
        let part = CommittedPart {
            name: name_for(&sha256),
            size,
            sha256,
            rows: stats.rows,
            min_unix_nano: stats.min_observed_nanos.max(0) as u64,
            max_unix_nano: stats.max_observed_nanos.max(0) as u64,
            origin: control.instance().to_string(),
            node: None,
        };
        match uploader.send(&path, &part) {
            Ok(sent) => {
                if let Err(e) = append_state(state_dir, &StateLine { path: relative, size, name: part.name.clone() }) {
                    tracing::warn!(error = %e, "could not record an uploaded part; a rerun commits it again");
                }
                control.update_parts(|p| match sent {
                    Sent::Uploaded => {
                        p.uploaded += 1;
                        p.bytes += size;
                    }
                    Sent::AlreadyPresent => p.already_present += 1,
                });
            }
            Err(Failure::Rejected(m)) => control.update_parts(|p| {
                p.rejected += 1;
                p.error = Some(m);
            }),
            Err(Failure::Fatal(m)) if m == "cancelled" => return finish("cancelled", None),
            Err(Failure::Fatal(m)) => return finish("error", Some(m)),
        }
    }
    finish(if cancel.load(Ordering::Acquire) { "cancelled" } else { "done" }, None);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{LogRecord, Severity};
    use crate::peer::{self, Admitted, PeerConfig, PeerNode};
    use crate::sync::SyncStore;
    use std::collections::HashMap as Map;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    const HEX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn names_are_a_lowercase_sha256_and_parquet() {
        assert!(valid_name(&format!("{HEX}.parquet")));
        assert!(!valid_name(&format!("{}.parquet", HEX.to_uppercase())));
        assert!(!valid_name(&format!("{HEX}.parq")));
        assert!(!valid_name(&format!("{}.parquet", &HEX[1..])));
        assert!(!valid_name(&format!("../{}.parquet", &HEX[3..])));
        assert!(!valid_name(&format!("{}g.parquet", &HEX[1..])));
        assert!(!valid_name(""));
    }

    #[test]
    fn the_config_is_checked_and_its_secret_never_shows() {
        let good = PartsConfig {
            enabled: true,
            endpoint: "https://s3.example.com".into(),
            bucket: "logs".into(),
            access_key_id: "AK".into(),
            secret_access_key: "SENTINEL-SECRET".into(),
            ..PartsConfig::default()
        };
        assert_eq!(Ok(()), good.check());
        assert!(!format!("{good:?}").contains("SENTINEL"));
        assert!(!serde_json::to_string(&good).unwrap().contains("SENTINEL"));
        for bad in [
            PartsConfig { enabled: false, ..good.clone() },
            PartsConfig { endpoint: "s3.example.com".into(), ..good.clone() },
            PartsConfig { endpoint: "https://s3.example.com/path".into(), ..good.clone() },
            PartsConfig { bucket: "".into(), ..good.clone() },
            PartsConfig { secret_access_key: "".into(), ..good.clone() },
            PartsConfig { url_ttl_secs: 0, ..good.clone() },
            PartsConfig { url_ttl_secs: MAX_TTL_SECS + 1, ..good.clone() },
        ] {
            let e = bad.check().unwrap_err();
            assert!(!e.contains("SENTINEL"), "{e}");
        }
        let parsed: PeerConfig = toml::from_str("[parts]\nenabled = true\nendpoint = \"http://h:9000\"\nbucket = \"b\"\naccess_key_id = \"a\"\nsecret_access_key = \"s\"\n").unwrap();
        assert_eq!((900, 512 * 1024 * 1024), (parsed.parts.url_ttl_secs, parsed.parts.max_part_bytes));
    }

    #[test]
    fn presigned_urls_point_at_the_prefixed_key_and_expire_after_the_ttl() {
        let config = PartsConfig {
            enabled: true,
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "logs".into(),
            prefix: "/edge parts/".into(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "s".into(),
            url_ttl_secs: 60,
            ..PartsConfig::default()
        };
        let central = PartsCentral::new(config, PathBuf::from("/nonexistent/catalog.sqlite")).unwrap();
        let name = format!("{HEX}.parquet");
        let (url, expires) = central.presigned("PUT", "edge-a", &name, &[]);
        assert!(url.starts_with(&format!("http://127.0.0.1:9000/logs/edge%20parts/edge-a/{name}?X-Amz-Algorithm=AWS4-HMAC-SHA256&")), "{url}");
        assert!(url.contains("X-Amz-Expires=60&") && url.contains("&X-Amz-Signature="), "{url}");
        assert!(expires.abs_diff(now_ms() + 60_000) < 2_000);
        let (url, _) = central.presigned("PUT", "edge-a", &name, &[(CHECKSUM_SHA256, "x")]);
        assert!(url.contains("&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256&"), "{url}");
    }

    #[test]
    fn the_checksum_header_is_base64_of_the_digest() {
        // The SHA-256 of no bytes, and its base64.
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(Some("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=".to_string()), checksum_b64(empty));
        assert_eq!(("", "Zg==", "Zm8=", "Zm9v"), (base64(b"").as_str(), base64(b"f").as_str(), base64(b"fo").as_str(), base64(b"foo").as_str()));
        assert_eq!(None, checksum_b64(&HEX[1..]));
        assert_eq!(None, checksum_b64(&format!("{}zz", &HEX[2..])));
    }

    // ---- a fake S3 server ----------------------------------------------------------------

    /// A stored object: its bytes and the SHA-256 checksum it was uploaded with, if any.
    #[derive(Clone)]
    struct Object {
        body: Vec<u8>,
        checksum: Option<String>,
    }

    fn b64_sha256(body: &[u8]) -> String {
        base64(&Sha256::digest(body))
    }

    #[derive(Default)]
    struct Fake {
        objects: Mutex<Map<String, Object>>,
        puts: AtomicU64,
        heads: AtomicU64,
        /// The next this many PUTs answer 500.
        fail_puts: AtomicU64,
        /// The next this many PUTs answer 403, as an expired URL does.
        expire_puts: AtomicU64,
        /// The next this many PUTs answer 408, as a store that cut a slow upload off does.
        stall_puts: AtomicU64,
        /// PUTs after this many successes answer 500 (u64::MAX: never).
        fail_after: AtomicU64,
        deletes: AtomicU64,
        /// The next this many DELETEs answer 500.
        fail_deletes: AtomicU64,
    }

    fn fake_s3() -> (String, Arc<Fake>, Arc<AtomicBool>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let fake = Arc::new(Fake { fail_after: AtomicU64::new(u64::MAX), ..Fake::default() });
        let stop = Arc::new(AtomicBool::new(false));
        let (state, halt) = (Arc::clone(&fake), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !halt.load(Ordering::Acquire) {
                let stream = match listener.accept() {
                    Ok((s, _)) => s,
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                };
                stream.set_nonblocking(false).unwrap();
                let mut out = stream.try_clone().unwrap();
                let mut input = BufReader::new(stream);
                let mut first = String::new();
                if input.read_line(&mut first).unwrap_or(0) == 0 {
                    continue;
                }
                let mut parts = first.split_whitespace();
                let (method, target) = (parts.next().unwrap_or("").to_string(), parts.next().unwrap_or("").to_string());
                let mut length = 0usize;
                let (mut checksum, mut checksum_mode) = (None, false);
                loop {
                    let mut line = String::new();
                    if input.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    let Some((name, value)) = line.split_once(':') else { continue };
                    match name.trim().to_ascii_lowercase().as_str() {
                        "content-length" => length = value.trim().parse().unwrap_or(0),
                        "x-amz-checksum-sha256" => checksum = Some(value.trim().to_string()),
                        "x-amz-checksum-mode" => checksum_mode = value.trim().eq_ignore_ascii_case("enabled"),
                        _ => {}
                    }
                }
                let key = target.split('?').next().unwrap_or("").to_string();
                // Signature-agnostic: only that the request is presigned at all.
                let signed = target.contains("X-Amz-Signature=");
                let reply = |out: &mut std::net::TcpStream, status: &str, headers: &str| {
                    let _ = write!(out, "HTTP/1.1 {status}\r\n{headers}connection: close\r\n\r\n");
                };
                match method.as_str() {
                    "HEAD" => {
                        state.heads.fetch_add(1, Ordering::SeqCst);
                        match state.objects.lock().unwrap().get(&key) {
                            Some(o) if signed => {
                                // As S3: the stored checksum only when asked for, and only if there is one.
                                let sum = o.checksum.as_ref().filter(|_| checksum_mode).map(|c| format!("x-amz-checksum-sha256: {c}\r\n")).unwrap_or_default();
                                reply(&mut out, "200 OK", &format!("content-length: {}\r\n{sum}", o.body.len()))
                            }
                            _ => reply(&mut out, "404 Not Found", "content-length: 0\r\n"),
                        }
                    }
                    "PUT" => {
                        let mut body = vec![0u8; length];
                        let _ = input.read_exact(&mut body);
                        state.puts.fetch_add(1, Ordering::SeqCst);
                        let take = |n: &AtomicU64| n.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1)).is_ok();
                        // A checksum header sent but not signed into the URL fails the signature.
                        let checksum_signed = target.contains("X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256&");
                        if take(&state.stall_puts) {
                            reply(&mut out, "408 Request Timeout", "content-length: 0\r\n");
                        } else if take(&state.expire_puts) || !signed || (checksum.is_some() != checksum_signed) {
                            reply(&mut out, "403 Forbidden", "content-length: 0\r\n");
                        } else if checksum.as_ref().is_some_and(|c| *c != b64_sha256(&body)) {
                            // As S3: a body that does not match the checksum it came with is not stored.
                            reply(&mut out, "400 Bad Request", "content-length: 0\r\n");
                        } else if take(&state.fail_puts) || !take(&state.fail_after) {
                            reply(&mut out, "500 Internal Server Error", "content-length: 0\r\n");
                        } else {
                            state.objects.lock().unwrap().insert(key, Object { body, checksum });
                            reply(&mut out, "200 OK", "content-length: 0\r\n");
                        }
                    }
                    "DELETE" => {
                        state.deletes.fetch_add(1, Ordering::SeqCst);
                        let failing = state.fail_deletes.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1)).is_ok();
                        if !signed {
                            reply(&mut out, "403 Forbidden", "content-length: 0\r\n");
                        } else if failing {
                            reply(&mut out, "500 Internal Server Error", "content-length: 0\r\n");
                        } else if state.objects.lock().unwrap().remove(&key).is_some() {
                            reply(&mut out, "204 No Content", "");
                        } else {
                            reply(&mut out, "404 Not Found", "content-length: 0\r\n");
                        }
                    }
                    _ => reply(&mut out, "405 Method Not Allowed", "content-length: 0\r\n"),
                }
            }
        });
        (url, fake, stop)
    }

    struct Central {
        receiver: peer::Receiver,
        catalog: PathBuf,
        _dir: tempfile::TempDir,
    }

    const NODE: &str = "edge-a";
    const PREFIX: &str = "/logs/edges/edge-a/";

    fn central(s3: &str, max_part_bytes: u64) -> Central {
        central_with(s3, max_part_bytes, PartsConfig::default(), vec![PeerNode { id: NODE.into(), token: "tok".into() }], vec![])
    }

    fn central_with(s3: &str, max_part_bytes: u64, retention: PartsConfig, nodes: Vec<PeerNode>, tokens: Vec<String>) -> Central {
        let dir = tempfile::tempdir().unwrap();
        let catalog = dir.path().join("catalog.sqlite");
        let config = PeerConfig {
            enabled: true,
            addr: "127.0.0.1:0".into(),
            tokens,
            nodes,
            parts: PartsConfig {
                enabled: true,
                endpoint: s3.into(),
                bucket: "logs".into(),
                prefix: "edges".into(),
                access_key_id: "AK".into(),
                secret_access_key: "SK".into(),
                max_part_bytes,
                ..retention
            },
            ..PeerConfig::default()
        };
        let receiver = peer::Receiver::start_with_catalog(&config, "central".into(), Some(catalog.clone()), |_| Admitted::Accepted).unwrap();
        Central { receiver, catalog, _dir: dir }
    }

    fn write_files(store: &Path, hours: &[u64]) {
        for (i, hour) in hours.iter().enumerate() {
            let base = hour * 3_600_000_000_000;
            let records: Vec<LogRecord> = (0..20u64)
                .map(|n| {
                    let mut r = LogRecord::new(base + n * 1_000_000_000, Severity::INFO, &format!("file {i} line {n}"));
                    r.observed_unix_nano = base + n * 1_000_000_000;
                    r.service = Some("api".into());
                    r
                })
                .collect();
            let key = crate::partition::PartitionKey::at(*hour, "info");
            let dir = key.dir(store);
            std::fs::create_dir_all(&dir).unwrap();
            crate::store::write_partition_file(&dir.join(crate::store::file_name_for_segment(i as u64 + 1, uuid::Uuid::now_v7())), &records).unwrap();
        }
    }

    struct Edge {
        control: Arc<SyncControl>,
        data: tempfile::TempDir,
        store: tempfile::TempDir,
    }

    fn edge(url: &str, hours: &[u64]) -> Edge {
        let data = tempfile::tempdir().unwrap();
        let store = tempfile::tempdir().unwrap();
        write_files(store.path(), hours);
        let target = SyncTarget { url: url.into(), token: Some("tok".into()), min_severity: 0, fidelity: 100, anomaly_boost: false };
        let control = SyncControl::new("edge".into(), SyncStore::new(data.path()), Some(target));
        control.set_store_dir(store.path().to_path_buf());
        Edge { control, data, store }
    }

    const FAST: Tuning = Tuning { attempts: 4, backoff: Duration::from_millis(5) };
    const HOUR_MS: u64 = 3_600_000;

    fn run(edge: &Edge, from_ms: u64, to_ms: u64) -> PartsJobStatus {
        edge.control.start_parts_with(from_ms, to_ms, FAST).unwrap();
        let started = Instant::now();
        loop {
            let status = edge.control.status().parts.unwrap();
            if status.state != "running" {
                return status;
            }
            assert!(started.elapsed() < Duration::from_secs(20), "the job did not finish: {status:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn parts_upload_once_and_a_rerun_or_a_lost_state_file_puts_nothing_again() {
        let (s3, fake, stop) = fake_s3();
        let c = central(&s3, 512 * 1024 * 1024);
        let e = edge(&format!("http://{}", c.receiver.local_addr()), &[1_000, 1_001, 1_005]);
        // Hours 1000 and 1001 only.
        let status = run(&e, 1_000 * HOUR_MS, 1_002 * HOUR_MS);
        assert_eq!(("done", 2, 2, 0), (status.state.as_str(), status.files_total, status.uploaded, status.skipped), "{status:?}");
        assert!(status.bytes > 0 && status.error.is_none(), "{status:?}");
        assert_eq!(2, fake.puts.load(Ordering::SeqCst));
        let keys: Vec<String> = fake.objects.lock().unwrap().keys().cloned().collect();
        assert!(keys.iter().all(|k| k.starts_with(PREFIX) && valid_name(&k[PREFIX.len()..])), "{keys:?}");
        // The stored bytes are the file: the name is their hash.
        for (k, o) in fake.objects.lock().unwrap().iter() {
            let hash: String = Sha256::digest(&o.body).iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(&k[PREFIX.len()..64 + PREFIX.len()], hash);
            assert_eq!(Some(b64_sha256(&o.body)), o.checksum, "uploaded with its checksum");
        }
        let catalog = crate::catalog::Catalog::open(&c.catalog).unwrap();
        assert_eq!(2, catalog.peer_part_count().unwrap());

        // Rerun: the state file skips both without asking central.
        let heads = fake.heads.load(Ordering::SeqCst);
        let again = run(&e, 1_000 * HOUR_MS, 1_002 * HOUR_MS);
        assert_eq!((2, 0), (again.skipped, again.uploaded), "{again:?}");
        assert_eq!((2, heads), (fake.puts.load(Ordering::SeqCst), fake.heads.load(Ordering::SeqCst)));

        // State file lost: central says already present, the commit is a no-op, nothing is PUT.
        std::fs::remove_file(e.data.path().join(STATE_FILE)).unwrap();
        let lost = run(&e, 1_000 * HOUR_MS, 1_002 * HOUR_MS);
        assert_eq!((2, 0), (lost.already_present, lost.uploaded), "{lost:?}");
        assert_eq!(2, fake.puts.load(Ordering::SeqCst));
        assert_eq!(2, catalog.peer_part_count().unwrap());
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn a_failed_put_is_retried_an_expired_url_presigned_again_and_a_stopped_job_resumes() {
        let (s3, fake, stop) = fake_s3();
        let c = central(&s3, 512 * 1024 * 1024);
        let e = edge(&format!("http://{}", c.receiver.local_addr()), &[2_000, 2_001, 2_002]);
        let range = (2_000 * HOUR_MS, 2_003 * HOUR_MS);

        // One transient 500 and one expired URL on the first file: both recovered within the attempts.
        fake.fail_puts.store(1, Ordering::SeqCst);
        fake.expire_puts.store(1, Ordering::SeqCst);
        // And after the first file, storage fails for good: the job stops with an error.
        fake.fail_after.store(1, Ordering::SeqCst);
        let heads_before = fake.heads.load(Ordering::SeqCst);
        let first = run(&e, range.0, range.1);
        assert_eq!(("error", 1), (first.state.as_str(), first.uploaded), "{first:?}");
        assert!(first.error.as_deref().unwrap().contains("Gave up"), "{first:?}");
        // The 403 cost a second presign (a HEAD each) before the commit's HEAD.
        assert!(fake.heads.load(Ordering::SeqCst) - heads_before >= 3);

        // Storage is back: the rerun uploads only the two that did not make it.
        fake.fail_after.store(u64::MAX, Ordering::SeqCst);
        let puts = fake.puts.load(Ordering::SeqCst);
        let second = run(&e, range.0, range.1);
        assert_eq!(("done", 1, 2), (second.state.as_str(), second.skipped, second.uploaded), "{second:?}");
        assert_eq!(puts + 2, fake.puts.load(Ordering::SeqCst));
        assert_eq!(3, crate::catalog::Catalog::open(&c.catalog).unwrap().peer_part_count().unwrap());
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn an_upload_central_cuts_off_as_too_slow_twice_in_a_row_moves_the_job_on_to_the_next_file() {
        let (s3, fake, stop) = fake_s3();
        let c = central(&s3, 512 * 1024 * 1024);
        let e = edge(&format!("http://{}", c.receiver.local_addr()), &[3_000, 3_001]);
        // The first file is cut off with 408 on both its tries; the second is never.
        fake.stall_puts.store(2, Ordering::SeqCst);
        let status = run(&e, 3_000 * HOUR_MS, 3_002 * HOUR_MS);
        assert_eq!(("done", 1, 1), (status.state.as_str(), status.rejected, status.uploaded), "{status:?}");
        assert!(status.error.as_deref().unwrap().contains("too slow"), "{status:?}");
        // One stall alone is a blip: it is tried again.
        fake.stall_puts.store(1, Ordering::SeqCst);
        std::fs::remove_file(e.data.path().join(STATE_FILE)).unwrap();
        let again = run(&e, 3_000 * HOUR_MS, 3_002 * HOUR_MS);
        assert_eq!(("done", 0), (again.state.as_str(), again.rejected), "{again:?}");
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    fn post(addr: std::net::SocketAddr, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
        post_as(addr, "tok", path, body)
    }

    fn post_as(addr: std::net::SocketAddr, token: &str, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
        let mut r = ureq::post(format!("http://{addr}{path}"))
            .header("Authorization", &format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .config()
            .http_status_as_error(false)
            .build()
            .send(body.to_string())
            .unwrap();
        let status = r.status().as_u16();
        (status, crate::sync::read_json(r.body_mut()).unwrap_or(serde_json::Value::Null))
    }

    #[test]
    fn central_refuses_a_bad_name_a_wrong_hash_an_oversize_part_and_a_commit_before_upload() {
        let (s3, _fake, stop) = fake_s3();
        let c = central(&s3, 1_000);
        let a = c.receiver.local_addr();
        let name = format!("{HEX}.parquet");
        assert_eq!(422, post(a, PRESIGN_PATH, serde_json::json!({"name": "x.parquet", "size": 10, "sha256": HEX})).0);
        assert_eq!(422, post(a, PRESIGN_PATH, serde_json::json!({"name": format!("../{}", &name[3..]), "size": 10, "sha256": HEX})).0);
        let other = format!("{}0", &HEX[..63]);
        assert_eq!(422, post(a, PRESIGN_PATH, serde_json::json!({"name": name, "size": 10, "sha256": other})).0);
        let (status, v) = post(a, PRESIGN_PATH, serde_json::json!({"name": name, "size": 1_001, "sha256": HEX}));
        assert_eq!(422, status);
        assert!(v["message"].as_str().unwrap().contains("1000"), "{v}");
        assert_eq!(400, post(a, PRESIGN_PATH, serde_json::json!({"name": name})).0);
        let (status, v) = post(a, PRESIGN_PATH, serde_json::json!({"name": name, "size": 10, "sha256": HEX}));
        assert_eq!(200, status, "{v}");
        assert_eq!(false, v["already_present"]);
        assert!(v["url"].as_str().unwrap().contains(PREFIX), "{v}");
        let commit = serde_json::json!({"name": name, "size": 10, "sha256": HEX, "rows": 1, "min_unix_nano": 1, "max_unix_nano": 2, "origin": "edge"});
        assert_eq!(409, post(a, COMMIT_PATH, commit).0, "nothing was uploaded");
        // Without the token, nothing.
        let r = ureq::post(format!("http://{a}{PRESIGN_PATH}")).config().http_status_as_error(false).build().send("{}").unwrap();
        assert_eq!(401, r.status().as_u16());
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn a_parts_request_a_page_could_send_without_a_preflight_is_refused_unread() {
        // On loopback, a web page could POST here with no Content-Type (or a form or text type) without
        // asking first, and so have this process sign URLs or record parts it never checked.
        let (s3, _fake, stop) = fake_s3();
        let dir = tempfile::tempdir().unwrap();
        let config = PeerConfig {
            enabled: true,
            addr: "127.0.0.1:0".into(),
            nodes: vec![PeerNode { id: NODE.into(), token: "tok".into() }],
            parts: PartsConfig {
                enabled: true,
                endpoint: s3.clone(),
                bucket: "logs".into(),
                access_key_id: "AK".into(),
                secret_access_key: "SK".into(),
                ..PartsConfig::default()
            },
            ..PeerConfig::default()
        };
        let rx = peer::Receiver::start_with_catalog(&config, "central".into(), Some(dir.path().join("catalog.sqlite")), |_| Admitted::Accepted).unwrap();
        let a = rx.local_addr();
        let presign = serde_json::json!({"name": format!("{HEX}.parquet"), "size": 10, "sha256": HEX}).to_string();
        let commit = serde_json::json!({"name": format!("{HEX}.parquet"), "size": 10, "sha256": HEX, "rows": 1, "min_unix_nano": 1, "max_unix_nano": 2, "origin": "edge"}).to_string();
        use crate::bounded_http::testing::{exchange, request, WAIT};
        for (path, body) in [(PRESIGN_PATH, &presign), (COMMIT_PATH, &commit)] {
            for kind in [None, Some("text/plain"), Some("application/x-www-form-urlencoded"), Some("multipart/form-data; boundary=x")] {
                let mut headers: Vec<(&str, &str)> = kind.map(|k| ("Content-Type", k)).into_iter().collect();
                headers.push(("Authorization", "Bearer tok"));
                let (status, text) = exchange(a, &request("POST", path, &headers, body.as_bytes()), WAIT);
                assert_eq!(415, status, "{path} {kind:?}: {text}");
            }
        }
        // The edge sends application/json, which a page cannot send without a preflight this listener never answers.
        let (status, text) = exchange(a, &request("POST", PRESIGN_PATH, &[("Content-Type", "application/json"), ("Authorization", "Bearer tok")], presign.as_bytes()), WAIT);
        assert_eq!(200, status, "{text}");
        rx.shutdown();
        stop.store(true, Ordering::Release);
    }

    /// The one Parquet file of an edge store: its bytes and `(name, sha256 hex)`.
    fn only_file(e: &Edge) -> (Vec<u8>, String, String) {
        let files = crate::store::list_parquet_files(e.store.path());
        assert_eq!(1, files.len());
        let (sha, _) = sha256_file(&files[0]).unwrap();
        (std::fs::read(&files[0]).unwrap(), name_for(&sha), sha)
    }

    #[test]
    fn a_same_size_object_with_other_content_is_never_present_or_committed_and_is_replaced() {
        let (s3, fake, stop) = fake_s3();
        let c = central(&s3, 512 * 1024 * 1024);
        let a = c.receiver.local_addr();
        let e = edge(&format!("http://{a}"), &[5_000]);
        let (body, name, sha) = only_file(&e);
        let key = format!("{PREFIX}{name}");
        let mut wrong = body.clone();
        wrong[body.len() / 2] ^= 0x01;
        let commit = serde_json::json!({"name": name, "size": body.len(), "sha256": sha, "rows": 20, "min_unix_nano": 1, "max_unix_nano": 2, "origin": "edge"});
        let presign = serde_json::json!({"name": name, "size": body.len(), "sha256": sha});
        // Same size, other bytes: uploaded with no checksum, and uploaded with its own (true) checksum.
        for checksum in [None, Some(b64_sha256(&wrong))] {
            fake.objects.lock().unwrap().insert(key.clone(), Object { body: wrong.clone(), checksum });
            let (status, v) = post(a, PRESIGN_PATH, presign.clone());
            assert_eq!((200, false), (status, v["already_present"].as_bool().unwrap()), "{v}");
            assert_eq!(b64_sha256(&body), v["headers"][CHECKSUM_SHA256].as_str().unwrap(), "{v}");
            assert_eq!(409, post(a, COMMIT_PATH, commit.clone()).0);
        }
        assert_eq!(0, crate::catalog::Catalog::open(&c.catalog).unwrap().peer_part_count().unwrap());

        // The job uploads the real file over it, and that commits.
        let status = run(&e, 5_000 * HOUR_MS, 5_001 * HOUR_MS);
        assert_eq!(("done", 1, 0), (status.state.as_str(), status.uploaded, status.already_present), "{status:?}");
        assert_eq!(body, fake.objects.lock().unwrap()[&key].body);
        assert_eq!(1, crate::catalog::Catalog::open(&c.catalog).unwrap().peer_part_count().unwrap());
        // Now stored intact: present, and a repeated commit is accepted and records nothing new.
        assert_eq!(true, post(a, PRESIGN_PATH, presign).1["already_present"]);
        let (status, v) = post(a, COMMIT_PATH, commit);
        assert_eq!((200, false), (status, v["recorded"].as_bool().unwrap()), "{v}");
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn a_put_whose_body_does_not_match_the_signed_checksum_is_refused_by_storage() {
        let (s3, fake, stop) = fake_s3();
        let c = central(&s3, 512 * 1024 * 1024);
        let a = c.receiver.local_addr();
        let e = edge(&format!("http://{a}"), &[6_000]);
        let (body, name, sha) = only_file(&e);
        let (_, v) = post(a, PRESIGN_PATH, serde_json::json!({"name": name, "size": body.len(), "sha256": sha}));
        let url = v["url"].as_str().unwrap();
        let checksum = v["headers"][CHECKSUM_SHA256].as_str().unwrap();
        let put = |payload: &[u8], header: Option<&str>| {
            let mut r = ureq::put(url).config().http_status_as_error(false).build();
            if let Some(h) = header {
                r = r.header(CHECKSUM_SHA256, h);
            }
            r.send(payload).unwrap().status().as_u16()
        };
        let mut wrong = body.clone();
        wrong[0] ^= 0xff;
        assert_eq!(400, put(&wrong, Some(checksum)), "same size, other bytes");
        // Dropping the header breaks the signature (the fake checks no more of it than that).
        assert_eq!(403, put(&wrong, None));
        assert!(fake.objects.lock().unwrap().is_empty());
        let commit = serde_json::json!({"name": name, "size": body.len(), "sha256": sha, "rows": 20, "min_unix_nano": 1, "max_unix_nano": 2, "origin": "edge"});
        assert_eq!(409, post(a, COMMIT_PATH, commit.clone()).0);
        // The right bytes with the header go in and commit.
        assert_eq!(200, put(&body, Some(checksum)));
        assert_eq!(200, post(a, COMMIT_PATH, commit).0);
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn an_oversize_file_is_rejected_and_the_job_moves_on() {
        let (s3, fake, stop) = fake_s3();
        let c = central(&s3, 10);
        let e = edge(&format!("http://{}", c.receiver.local_addr()), &[3_000]);
        let status = run(&e, 3_000 * HOUR_MS, 3_001 * HOUR_MS);
        assert_eq!(("done", 1, 0), (status.state.as_str(), status.rejected, status.uploaded), "{status:?}");
        assert!(status.error.unwrap().contains("refused a part"));
        assert_eq!(0, fake.puts.load(Ordering::SeqCst));
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn the_capability_flips_with_the_config_and_the_job_needs_it() {
        let (s3, _fake, stop) = fake_s3();
        let with = central(&s3, 1_000);
        let url = format!("http://{}", with.receiver.local_addr());
        assert!(crate::sync::probe(&url, Some("tok")).unwrap().capabilities.parts);

        let without = peer::Receiver::start(
            &PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), tokens: vec!["tok".into()], ..PeerConfig::default() },
            "plain".into(),
            |_| Admitted::Accepted,
        )
        .unwrap();
        let plain = format!("http://{}", without.local_addr());
        assert!(!crate::sync::probe(&plain, Some("tok")).unwrap().capabilities.parts);
        // Enabled but incomplete (no secret): not advertised either.
        let mut half = PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), ..PeerConfig::default() };
        half.parts = PartsConfig { enabled: true, endpoint: s3.clone(), bucket: "b".into(), access_key_id: "a".into(), ..PartsConfig::default() };
        let incomplete = peer::Receiver::start_with_catalog(&half, "half".into(), Some(PathBuf::from("/tmp/x.sqlite")), |_| Admitted::Accepted).unwrap();
        assert!(!crate::sync::probe(&format!("http://{}", incomplete.local_addr()), None).unwrap().capabilities.parts);
        assert_eq!(404, post(incomplete.local_addr(), PRESIGN_PATH, serde_json::json!({})).0);

        let e = edge(&plain, &[4_000]);
        let status = run(&e, 4_000 * HOUR_MS, 4_001 * HOUR_MS);
        assert_eq!("error", status.state);
        assert!(status.error.unwrap().contains("does not accept parts"));
        with.receiver.shutdown();
        without.shutdown();
        incomplete.shutdown();
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn a_second_job_while_one_runs_is_refused_and_ranges_are_checked() {
        // A peer that accepts the connection and never answers keeps the first job in its probe.
        let silent = TcpListener::bind("127.0.0.1:0").unwrap();
        let e = edge(&format!("http://{}", silent.local_addr().unwrap()), &[]);
        assert!(e.control.start_parts(10, 10).unwrap_err().contains("after its start"));
        assert!(e.control.start_parts(0, 40 * 24 * HOUR_MS).unwrap_err().contains("31 days"));
        e.control.start_parts(0, HOUR_MS).unwrap();
        assert!(e.control.start_parts(0, HOUR_MS).unwrap_err().contains("already running"));
        e.control.cancel_parts();
    }

    // ---- node-owned prefixes, tiers, retention ----------------------------------------------

    fn two_nodes() -> Vec<PeerNode> {
        vec![PeerNode { id: "edge-a".into(), token: "tok-a".into() }, PeerNode { id: "edge-b".into(), token: "tok-b".into() }]
    }

    fn commit_body(name: &str, size: u64, sha: &str, extra: serde_json::Value) -> serde_json::Value {
        let mut v = serde_json::json!({"name": name, "size": size, "sha256": sha, "rows": 1, "min_unix_nano": 1, "max_unix_nano": 2, "origin": "inst"});
        for (k, x) in extra.as_object().into_iter().flatten() {
            v[k] = x.clone();
        }
        v
    }

    #[test]
    fn a_node_can_only_presign_commit_and_retire_under_its_own_prefix() {
        let (s3, fake, stop) = fake_s3();
        let c = central_with(&s3, 1_000, PartsConfig::default(), two_nodes(), vec!["shared-ingest-token".into()]);
        let a = c.receiver.local_addr();
        let name = format!("{HEX}.parquet");
        let presign = serde_json::json!({"name": name, "size": 10, "sha256": HEX});

        // The key comes from the token: node A's URL is under edge-a, node B's under edge-b.
        let (status, va) = post_as(a, "tok-a", PRESIGN_PATH, presign.clone());
        assert_eq!(200, status, "{va}");
        assert!(va["url"].as_str().unwrap().contains(&format!("/logs/edges/edge-a/{name}?")), "{va}");
        let (_, vb) = post_as(a, "tok-b", PRESIGN_PATH, presign.clone());
        assert!(vb["url"].as_str().unwrap().contains(&format!("/logs/edges/edge-b/{name}?")), "{vb}");

        // A request that names another node is refused, on every path, with nothing signed.
        let foreign = serde_json::json!({"name": name, "size": 10, "sha256": HEX, "node": "edge-b"});
        for path in [PRESIGN_PATH, COMMIT_PATH, RETIRE_PATH] {
            let body = if path == PRESIGN_PATH { foreign.clone() } else { commit_body(&name, 10, HEX, serde_json::json!({"node": "edge-b"})) };
            let (status, v) = post_as(a, "tok-a", path, body);
            assert_eq!((403, "foreign_prefix"), (status, v["error"].as_str().unwrap()), "{path}: {v}");
            assert!(v.get("url").is_none(), "{v}");
        }
        // Naming itself is fine.
        assert_eq!(200, post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": name, "size": 10, "sha256": HEX, "node": "edge-a"})).0);

        // Traversal and prefix tricks in the name are not names, on every path.
        let hash = &name[..64];
        for bad in [
            format!("../edge-b/{name}"),
            format!("edge-b/{name}"),
            format!("/{name}"),
            format!("..%2Fedge-b%2F{name}"),
            format!("{hash}.parquet/../../x"),
            format!("{hash}.parquet\0"),
            format!("{hash}.parquet?x=1"),
            "..".to_string(),
        ] {
            for path in [PRESIGN_PATH, COMMIT_PATH, RETIRE_PATH] {
                let body = if path == RETIRE_PATH { serde_json::json!({"name": bad}) } else { commit_body(&bad, 10, HEX, serde_json::json!({})) };
                let (status, v) = post_as(a, "tok-a", path, body);
                assert_eq!((422, "bad_name"), (status, v["error"].as_str().unwrap_or("")), "{path} {bad:?}: {v}");
            }
        }

        // A shared ingest token owns no prefix.
        let (status, v) = post_as(a, "shared-ingest-token", PRESIGN_PATH, presign.clone());
        assert_eq!((403, "no_node"), (status, v["error"].as_str().unwrap()), "{v}");
        assert_eq!(403, post_as(a, "wrong", PRESIGN_PATH, presign.clone()).0);

        // A's object at its key is not B's: B is told to upload, and B cannot commit A's upload.
        let body = vec![7u8; 10];
        let sha: String = Sha256::digest(&body).iter().map(|b| format!("{b:02x}")).collect();
        let pname = name_for(&sha);
        fake.objects.lock().unwrap().insert(format!("{PREFIX}{pname}"), Object { body: body.clone(), checksum: Some(b64_sha256(&body)) });
        let presign = serde_json::json!({"name": pname, "size": 10, "sha256": sha});
        assert_eq!(true, post_as(a, "tok-a", PRESIGN_PATH, presign.clone()).1["already_present"]);
        assert_eq!(false, post_as(a, "tok-b", PRESIGN_PATH, presign).1["already_present"]);
        assert_eq!(409, post_as(a, "tok-b", COMMIT_PATH, commit_body(&pname, 10, &sha, serde_json::json!({}))).0);
        assert_eq!(200, post_as(a, "tok-a", COMMIT_PATH, commit_body(&pname, 10, &sha, serde_json::json!({}))).0);

        // Only A's row exists, and B cannot retire it (404, as if it did not exist).
        let catalog = crate::catalog::Catalog::open(&c.catalog).unwrap();
        let rows = catalog.list_peer_parts(&crate::catalog::PartsFilter { limit: 10, ..Default::default() }).unwrap();
        assert_eq!(vec![("edge-a", "warm")], rows.iter().map(|r| (r.node.as_str(), r.tier.as_str())).collect::<Vec<_>>());
        assert_eq!(404, post_as(a, "tok-b", RETIRE_PATH, serde_json::json!({"name": pname})).0);
        // Nothing was ever written or signed under B's prefix.
        assert!(fake.objects.lock().unwrap().keys().all(|k| k.starts_with(PREFIX)));
        assert_eq!(0, fake.puts.load(Ordering::SeqCst));
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn node_configuration_is_validated_before_anything_listens() {
        let start = |nodes: Vec<PeerNode>, tokens: Vec<String>| {
            peer::Receiver::start(&PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), nodes, tokens, ..PeerConfig::default() }, "c".into(), |_| Admitted::Accepted)
        };
        let node = |id: &str, token: &str| PeerNode { id: id.into(), token: token.into() };
        for bad in [
            vec![node("..", "t")],
            vec![node(".", "t")],
            vec![node("a/b", "t")],
            vec![node("", "t")],
            vec![node(&"x".repeat(65), "t")],
            vec![node("a", "")],
            vec![node("a", "t"), node("a", "u")],
            vec![node("a", "t"), node("b", "t")],
            // A dot-led id would hide the node from the budget count and S3 serve, and look like a temporary file.
            vec![node(".dot", "t")],
            vec![node("...", "t")],
            vec![node(".upload-x.tmp", "t")],
            // Ids that differ only in case are one directory on a case-insensitive filesystem.
            vec![node("edge-a", "t"), node("EDGE-A", "u")],
        ] {
            assert!(matches!(start(bad.clone(), vec![]), Err(peer::PeerError::BadNode(_))), "{bad:?}");
        }
        assert!(matches!(start(vec![node("a", "t")], vec!["t".into()]), Err(peer::PeerError::BadNode(_))), "a node token that is also a shared token");
        start(vec![node("a.b_c-1", "t")], vec![]).unwrap().shutdown();
        assert!(!format!("{:?}", node("a", "SENTINEL-NODE-TOKEN")).contains("SENTINEL"));
        let parsed: PeerConfig = toml::from_str("[[nodes]]\nid = \"edge-a\"\ntoken = \"t\"\n").unwrap();
        assert_eq!("edge-a", parsed.nodes[0].id);
    }

    fn seed(c: &Central, fake: &Fake, node: &str, tag: u8, max_ms: u64) -> String {
        let body = vec![tag; 16];
        let sha: String = Sha256::digest(&body).iter().map(|b| format!("{b:02x}")).collect();
        let name = name_for(&sha);
        fake.objects.lock().unwrap().insert(format!("/logs/edges/{node}/{name}"), Object { body: body.clone(), checksum: Some(b64_sha256(&body)) });
        let part = CommittedPart { name: name.clone(), size: 16, sha256: sha, rows: 1, min_unix_nano: (max_ms - 1) * 1_000_000, max_unix_nano: max_ms * 1_000_000, origin: "inst".into(), node: None };
        assert!(crate::catalog::Catalog::open(&c.catalog).unwrap().record_peer_part(node, &part, 5).unwrap());
        name
    }

    #[test]
    fn parts_are_listed_by_node_time_and_tier_and_a_node_moves_its_own_part_to_cold() {
        let (s3, fake, stop) = fake_s3();
        let c = central_with(&s3, 1_000, PartsConfig::default(), two_nodes(), vec![]);
        let a = c.receiver.local_addr();
        let (a1, a2, b1) = (seed(&c, &fake, "edge-a", 1, 1_000), seed(&c, &fake, "edge-a", 2, 5_000), seed(&c, &fake, "edge-b", 3, 3_000));
        let list = |f: crate::catalog::PartsFilter| -> Vec<String> {
            let rows = crate::catalog::Catalog::open(&c.catalog).unwrap().list_peer_parts(&f).unwrap();
            rows.into_iter().map(|r| format!("{}/{}/{}", r.node, r.tier, r.name)).collect()
        };
        let all = crate::catalog::PartsFilter { limit: 100, ..Default::default() };
        // Oldest data first.
        assert_eq!(vec![format!("edge-a/warm/{a1}"), format!("edge-b/warm/{b1}"), format!("edge-a/warm/{a2}")], list(all.clone()));
        assert_eq!(2, list(crate::catalog::PartsFilter { node: Some("edge-a".into()), ..all.clone() }).len());
        // Time: the part's span must overlap the range.
        assert_eq!(vec![format!("edge-b/warm/{b1}")], list(crate::catalog::PartsFilter { from_nano: Some(2_000 * 1_000_000), to_nano: Some(4_000 * 1_000_000), ..all.clone() }));
        assert_eq!(1, list(crate::catalog::PartsFilter { limit: 1, ..all.clone() }).len());

        // The node retires its own part; a repeat, or a part it does not have, is a 404.
        let (status, v) = post_as(a, "tok-a", RETIRE_PATH, serde_json::json!({"name": a1}));
        assert_eq!((200, "cold"), (status, v["tier"].as_str().unwrap()), "{v}");
        assert_eq!(404, post_as(a, "tok-a", RETIRE_PATH, serde_json::json!({"name": a1})).0);
        assert_eq!(404, post_as(a, "tok-a", RETIRE_PATH, serde_json::json!({"name": b1})).0);
        assert_eq!(vec![format!("edge-a/cold/{a1}")], list(crate::catalog::PartsFilter { tier: Some("cold".into()), ..all.clone() }));
        assert_eq!(2, list(crate::catalog::PartsFilter { tier: Some("warm".into()), ..all }).len());
        let audit = crate::catalog::Catalog::open(&c.catalog).unwrap().peer_parts_audit(10).unwrap();
        assert_eq!(1, audit.len());
        assert_eq!(("tier", "edge-a", a1.as_str(), "cold", "ok"), (audit[0].action.as_str(), audit[0].node.as_str(), audit[0].name.as_str(), audit[0].tier.as_str(), audit[0].outcome.as_str()));
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn the_sweep_deletes_only_catalogued_parts_older_than_their_tiers_retention_and_audits_each() {
        let (s3, fake, stop) = fake_s3();
        let retention = PartsConfig { warm_retention_secs: 1_000, cold_retention_secs: 100, ..PartsConfig::default() };
        let c = central_with(&s3, 1_000, retention.clone(), two_nodes(), vec![]);
        let now_ms: u64 = 10_000_000;
        let age = |secs: u64| now_ms - secs * 1_000;
        let old_warm = seed(&c, &fake, "edge-a", 1, age(1_500));
        let young_warm = seed(&c, &fake, "edge-a", 2, age(500));
        let old_cold = seed(&c, &fake, "edge-b", 3, age(150));
        let young_cold = seed(&c, &fake, "edge-b", 4, age(50));
        // Cold retention is shorter: a part is expired by its own tier's rule only.
        let warm_150 = seed(&c, &fake, "edge-a", 5, age(150));
        {
            let mut cat = crate::catalog::Catalog::open(&c.catalog).unwrap();
            for (node, name) in [("edge-b", &old_cold), ("edge-b", &young_cold)] {
                assert!(cat.mark_peer_part_cold(node, name, 6).unwrap());
            }
        }
        // An object nobody committed, old by any measure, under a node's prefix and outside it.
        fake.objects.lock().unwrap().insert(format!("{PREFIX}{}", name_for(&"ab".repeat(32))), Object { body: vec![9], checksum: None });
        fake.objects.lock().unwrap().insert("/logs/other/keep.txt".into(), Object { body: vec![9], checksum: None });
        let central = PartsCentral::new(c_config(&c, &s3, retention), c.catalog.clone()).unwrap();
        let rows = || crate::catalog::Catalog::open(&c.catalog).unwrap().list_peer_parts(&crate::catalog::PartsFilter { limit: 100, ..Default::default() }).unwrap().len();
        let stored = || fake.objects.lock().unwrap().len();
        assert_eq!((5, 7), (rows(), stored()));

        // Dry run: reports, changes nothing, audits nothing.
        let dry = central.sweep(now_ms, true).unwrap();
        assert_eq!((2, 0, 0), (dry.would_delete, dry.deleted, dry.failed), "{dry:?}");
        assert_eq!((5, 7, 0, 0), (rows(), stored(), fake.deletes.load(Ordering::SeqCst), crate::catalog::Catalog::open(&c.catalog).unwrap().peer_parts_audit(10).unwrap().iter().filter(|r| r.action == "expire").count()));

        // One delete fails: the row stays (and says why); the other goes with its row.
        fake.fail_deletes.store(1, Ordering::SeqCst);
        let first = central.sweep(now_ms, false).unwrap();
        assert_eq!((1, 1), (first.deleted, first.failed), "{first:?}");
        assert_eq!((4, 6), (rows(), stored()));
        let audit = crate::catalog::Catalog::open(&c.catalog).unwrap().peer_parts_audit(10).unwrap();
        let mut outcomes: Vec<_> = audit.iter().filter(|a| a.action == "expire").map(|a| a.outcome.as_str()).collect();
        outcomes.sort();
        assert_eq!(vec!["delete_failed", "deleted"], outcomes);
        assert!(audit.iter().any(|a| a.outcome == "delete_failed" && a.detail.contains("HTTP 500")), "{audit:?}");

        // The next sweep retries the failed one. Afterwards exactly the young parts and the uncatalogued objects remain.
        let second = central.sweep(now_ms, false).unwrap();
        assert_eq!((1, 0), (second.deleted, second.failed), "{second:?}");
        let mut left: Vec<String> = crate::catalog::Catalog::open(&c.catalog)
            .unwrap()
            .list_peer_parts(&crate::catalog::PartsFilter { limit: 100, ..Default::default() })
            .unwrap()
            .into_iter()
            .map(|r| r.name)
            .collect();
        left.sort();
        let mut want = vec![young_warm.clone(), young_cold.clone(), warm_150.clone()];
        want.sort();
        assert_eq!(want, left);
        assert_eq!(5, stored(), "3 catalogued parts and 2 uncatalogued objects");
        assert!(fake.objects.lock().unwrap().contains_key("/logs/other/keep.txt"));
        assert!(!fake.objects.lock().unwrap().keys().any(|k| k.ends_with(&old_warm) || k.ends_with(&old_cold)));
        // Nothing left to do, and a sweep with no retention configured never deletes.
        assert_eq!(0, central.sweep(now_ms, false).unwrap().deleted);
        let off = PartsCentral::new(c_config(&c, &s3, PartsConfig::default()), c.catalog.clone()).unwrap();
        assert_eq!(0, off.sweep(now_ms * 100, false).unwrap().deleted);
        assert_eq!(5, stored());
        // A tampered catalog row cannot make a key: refused, audited as failed.
        {
            let cat = crate::catalog::Catalog::open(&c.catalog).unwrap();
            drop(cat);
            let conn = rusqlite::Connection::open(&c.catalog).unwrap();
            conn.execute("UPDATE peer_parts SET node = '../x', max_observed = 1 WHERE name = ?1", [&young_warm]).unwrap();
        }
        let before = fake.deletes.load(Ordering::SeqCst);
        let tampered = central.sweep(now_ms, false).unwrap();
        assert_eq!((0, 1, before), (tampered.deleted, tampered.failed, fake.deletes.load(Ordering::SeqCst)), "{tampered:?}");
        c.receiver.shutdown();
        stop.store(true, Ordering::Release);
    }

    fn c_config(_c: &Central, s3: &str, retention: PartsConfig) -> PartsConfig {
        PartsConfig {
            enabled: true,
            endpoint: s3.into(),
            bucket: "logs".into(),
            prefix: "edges".into(),
            access_key_id: "AK".into(),
            secret_access_key: "SK".into(),
            ..retention
        }
    }

    // ---- the local backend: parts on central's own disk -------------------------------------

    struct Local {
        receiver: peer::Receiver,
        catalog: PathBuf,
        /// `<root>/parts`, the default directory beside the catalog.
        parts: PathBuf,
        root: tempfile::TempDir,
    }

    fn local_config(max_part_bytes: u64, disk_budget_bytes: u64) -> PeerConfig {
        PeerConfig {
            enabled: true,
            addr: "127.0.0.1:0".into(),
            nodes: two_nodes(),
            // No endpoint, no bucket, no keys: the default backend is this instance's disk.
            parts: PartsConfig { enabled: true, max_part_bytes, disk_budget_bytes, ..PartsConfig::default() },
            ..PeerConfig::default()
        }
    }

    fn local_start(root: &Path, max_part_bytes: u64, disk_budget_bytes: u64) -> peer::Receiver {
        peer::Receiver::start_with_catalog(&local_config(max_part_bytes, disk_budget_bytes), "central".into(), Some(root.join("catalog.sqlite")), |_| Admitted::Accepted).unwrap()
    }

    fn local(max_part_bytes: u64, disk_budget_bytes: u64) -> Local {
        let root = tempfile::tempdir().unwrap();
        let receiver = local_start(root.path(), max_part_bytes, disk_budget_bytes);
        Local { receiver, catalog: root.path().join("catalog.sqlite"), parts: root.path().join("parts"), root }
    }

    /// `len` bytes of `tag`, and the part name and SHA-256 they have.
    fn blob(tag: u8, len: usize) -> (Vec<u8>, String, String) {
        let body = vec![tag; len];
        let sha = hex(&Sha256::digest(&body));
        (body, name_for(&sha), sha)
    }

    /// Presigns as node A and returns the upload path and token.
    fn presign_local(a: std::net::SocketAddr, name: &str, size: usize, sha: &str) -> (String, String) {
        let (status, v) = post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": name, "size": size, "sha256": sha}));
        assert_eq!((200, false), (status, v["already_present"].as_bool().unwrap_or(true)), "{v}");
        (v["url"].as_str().unwrap().to_string(), v["headers"][UPLOAD_TOKEN_HEADER].as_str().unwrap().to_string())
    }

    /// A raw PUT: `declared` is the Content-Length sent, `sent` the bytes that follow the head; the write
    /// side is then closed. Returns the status and the error code of the JSON answer.
    fn put_raw(a: std::net::SocketAddr, path: &str, token: Option<&str>, declared: usize, sent: &[u8]) -> (u16, String) {
        use std::io::Read as _;
        let mut head = format!("PUT {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {declared}\r\n");
        if let Some(t) = token {
            head.push_str(&format!("{UPLOAD_TOKEN_HEADER}: {t}\r\n"));
        }
        head.push_str("\r\n");
        let mut conn = std::net::TcpStream::connect(a).unwrap();
        conn.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        // The server may answer and hang up before the whole request is written.
        let _ = conn.write_all(head.as_bytes());
        let _ = conn.write_all(sent);
        let _ = conn.shutdown(std::net::Shutdown::Write);
        let mut out = Vec::new();
        let _ = conn.read_to_end(&mut out);
        let text = String::from_utf8_lossy(&out).into_owned();
        let status = text.split(' ').nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let json: serde_json::Value = text.split_once("\r\n\r\n").and_then(|(_, b)| serde_json::from_str(b).ok()).unwrap_or_default();
        (status, json["error"].as_str().map(str::to_string).unwrap_or_else(|| json["stored"].to_string()))
    }

    fn put(a: std::net::SocketAddr, path: &str, token: &str, body: &[u8]) -> (u16, String) {
        put_raw(a, path, Some(token), body.len(), body)
    }

    /// Every file under the parts directory, as `<node>/<file name>`, temporary files included.
    fn files_in(parts: &Path) -> Vec<String> {
        let mut out = Vec::new();
        for node in std::fs::read_dir(parts).into_iter().flatten().flatten() {
            for f in std::fs::read_dir(node.path()).into_iter().flatten().flatten() {
                out.push(format!("{}/{}", node.file_name().to_string_lossy(), f.file_name().to_string_lossy()));
            }
        }
        out.sort();
        out
    }

    fn status_of(c: &Local) -> PartsStoreStatus {
        c.receiver.parts_handle().unwrap().store_status().unwrap()
    }

    fn local_store(c: &Local) -> Arc<PartsCentral> {
        c.receiver.parts_handle().unwrap()
    }

    /// An upload token signed by `c` itself, with any expiry: what presign would hand out.
    fn token_from(c: &Local, node: &str, name: &str, size: u64, expires_secs: u64) -> String {
        match &local_store(c).store {
            Store::Local(l) => l.token(node, name, size, expires_secs),
            Store::S3 { .. } => unreachable!("a local central"),
        }
    }

    #[test]
    fn the_backend_is_local_unless_an_endpoint_or_the_setting_says_s3() {
        let parse = |toml: &str| toml::from_str::<PeerConfig>(toml).map(|c| c.parts);
        let bare = parse("[parts]\nenabled = true\n").unwrap();
        assert_eq!((Backend::Local, Ok(())), (bare.backend(), bare.check()));
        assert_eq!(Some(PathBuf::from("/data/parts")), bare.local_dir(Path::new("/data")));
        assert_eq!(64 * 1024 * 1024 * 1024, bare.disk_budget_bytes);
        let placed = parse("[parts]\nenabled = true\ndir = \"/mnt/parts\"\ndisk_budget_bytes = 0\n").unwrap();
        assert_eq!((Some(PathBuf::from("/mnt/parts")), Ok(())), (placed.local_dir(Path::new("/data")), placed.check()));
        // An S3 table written before the setting existed is still S3, and its directory is nobody's.
        let s3 = parse("[parts]\nenabled = true\nendpoint = \"http://h:9000\"\nbucket = \"b\"\naccess_key_id = \"a\"\nsecret_access_key = \"s\"\n").unwrap();
        assert_eq!((Backend::S3, Ok(()), None), (s3.backend(), s3.check(), s3.local_dir(Path::new("/data"))));
        // Named outright, each backend is held to its own rules.
        let forced = parse("[parts]\nenabled = true\nbackend = \"s3\"\n").unwrap();
        assert!(forced.check().unwrap_err().contains("endpoint"), "{:?}", forced.check());
        let kept = parse("[parts]\nenabled = true\nbackend = \"local\"\nendpoint = \"http://h:9000\"\n").unwrap();
        assert_eq!((Backend::Local, Ok(())), (kept.backend(), kept.check()));
        assert!(parse("[parts]\nenabled = true\nbackend = \"ftp\"\n").is_err());
        // An S3 table that lost its endpoint is not quietly turned into the local backend.
        for slip in ["bucket = \"b\"", "prefix = \"edges\"", "access_key_id = \"a\"", "secret_access_key = \"s\"", "endpoint = \"  \"\nbucket = \"b\""] {
            let cfg = parse(&format!("[parts]\nenabled = true\n{slip}\n")).unwrap();
            let why = cfg.check().unwrap_err();
            assert!(why.contains("without an endpoint") && why.contains("backend = \"local\""), "{slip}: {why}");
        }
        let slipped = PeerConfig { parts: PartsConfig { enabled: true, bucket: "b".into(), access_key_id: "a".into(), secret_access_key: "s".into(), ..PartsConfig::default() }, ..local_config(1_000, 0) };
        assert!(matches!(peer::Receiver::start_with_catalog(&slipped, "c".into(), Some(PathBuf::from("/tmp/x.sqlite")), |_| Admitted::Accepted), Err(peer::PeerError::BadParts(m)) if m.contains("endpoint")));
        for (bad, says) in [
            (PartsConfig { enabled: true, max_part_bytes: 1_000, disk_budget_bytes: 999, ..PartsConfig::default() }, "disk_budget_bytes"),
            (PartsConfig { enabled: true, dir: Some(PathBuf::new()), ..PartsConfig::default() }, "dir"),
            (PartsConfig { enabled: true, url_ttl_secs: 0, ..PartsConfig::default() }, "url_ttl_secs"),
            (PartsConfig { enabled: true, max_part_bytes: 0, disk_budget_bytes: 0, ..PartsConfig::default() }, "max_part_bytes"),
        ] {
            assert!(bad.check().unwrap_err().contains(says), "{:?}", bad.check());
        }
        // A local table that cannot work stops the listener; it is not quietly left off.
        let mut wrong = local_config(1_000, 999);
        assert!(matches!(peer::Receiver::start_with_catalog(&wrong, "c".into(), Some(PathBuf::from("/tmp/x.sqlite")), |_| Admitted::Accepted), Err(peer::PeerError::BadParts(_))));
        let file = tempfile::NamedTempFile::new().unwrap();
        wrong.parts = PartsConfig { enabled: true, dir: Some(file.path().to_path_buf()), ..PartsConfig::default() };
        let refused = peer::Receiver::start_with_catalog(&wrong, "c".into(), Some(PathBuf::from("/tmp/x.sqlite")), |_| Admitted::Accepted);
        assert!(matches!(&refused, Err(peer::PeerError::BadParts(m)) if m.contains("cannot be created")), "{:?}", refused.err());
    }

    #[test]
    fn the_same_edge_job_uploads_to_centrals_own_disk_and_a_rerun_or_a_lost_state_file_puts_nothing_again() {
        let c = local(512 * 1024 * 1024, 0);
        let url = format!("http://{}", c.receiver.local_addr());
        assert!(crate::sync::probe(&url, Some("tok-a")).unwrap().capabilities.parts);
        let mut e = edge(&url, &[1_000, 1_001, 1_005]);
        // The edge is configured exactly as for S3: a peer URL and its node token.
        let target = SyncTarget { url: url.clone(), token: Some("tok-a".into()), min_severity: 0, fidelity: 100, anomaly_boost: false };
        e.control = SyncControl::new("edge".into(), SyncStore::new(e.data.path()), Some(target));
        e.control.set_store_dir(e.store.path().to_path_buf());

        let status = run(&e, 1_000 * HOUR_MS, 1_002 * HOUR_MS);
        assert_eq!(("done", 2, 2, 0), (status.state.as_str(), status.files_total, status.uploaded, status.skipped), "{status:?}");
        assert!(status.error.is_none(), "{status:?}");
        // On disk under the token's node, each file named by its own bytes, and nothing temporary left.
        let stored = files_in(&c.parts);
        assert_eq!(2, stored.len(), "{stored:?}");
        let mut bytes = 0;
        for key in &stored {
            let (node, name) = key.split_once('/').unwrap();
            assert!(node == "edge-a" && valid_name(name), "{key}");
            let (sha, size) = sha256_file(&c.parts.join(key)).unwrap();
            assert_eq!(name_for(&sha), name);
            bytes += size;
        }
        assert_eq!(status.bytes, bytes);
        let catalog = crate::catalog::Catalog::open(&c.catalog).unwrap();
        assert_eq!(2, catalog.peer_part_count().unwrap());
        let s = status_of(&c);
        assert_eq!((Backend::Local, 2, bytes, bytes, 0), (s.backend, s.uploads, s.upload_bytes, s.used_bytes, s.reserved_bytes), "{s:?}");

        // Rerun: the state file skips both. State file lost: central has them, nothing is sent again.
        let again = run(&e, 1_000 * HOUR_MS, 1_002 * HOUR_MS);
        assert_eq!((2, 0), (again.skipped, again.uploaded), "{again:?}");
        std::fs::remove_file(e.data.path().join(STATE_FILE)).unwrap();
        let lost = run(&e, 1_000 * HOUR_MS, 1_002 * HOUR_MS);
        assert_eq!((2, 0), (lost.already_present, lost.uploaded), "{lost:?}");
        let s = status_of(&c);
        assert_eq!((2, 0, bytes), (s.uploads, s.already_present, s.used_bytes), "{s:?}");
        assert_eq!((2, stored), (catalog.peer_part_count().unwrap(), files_in(&c.parts)));
        c.receiver.shutdown();
    }

    #[test]
    fn a_local_upload_with_the_wrong_bytes_or_the_wrong_length_stores_nothing() {
        let c = local(1_000, 0);
        let a = c.receiver.local_addr();
        let (body, name, sha) = blob(1, 400);
        let (path, token) = presign_local(a, &name, 400, &sha);
        assert_eq!(format!("{BLOB_PREFIX}edge-a/{name}"), path);
        let commit = commit_body(&name, 400, &sha, serde_json::json!({}));
        let nothing_kept = |why: &str| {
            assert_eq!(Vec::<String>::new(), files_in(&c.parts), "{why}");
            assert_eq!(409, post_as(a, "tok-a", COMMIT_PATH, commit.clone()).0, "{why}");
            assert_eq!(0, crate::catalog::Catalog::open(&c.catalog).unwrap().peer_part_count().unwrap(), "{why}");
        };

        // The right size, other bytes: read to the end, refused, and its temporary file is gone.
        let mut wrong = body.clone();
        wrong[200] ^= 0x01;
        assert_eq!((422, "bad_content".to_string()), put(a, &path, &token, &wrong));
        nothing_kept("wrong content");

        // A length other than the one the token was issued for is refused from the head, longer or shorter.
        assert_eq!((400, "size_mismatch".to_string()), put_raw(a, &path, Some(&token), 401, &[body.clone(), vec![1]].concat()));
        assert_eq!((400, "size_mismatch".to_string()), put_raw(a, &path, Some(&token), 399, &body[..399]));
        assert_eq!((400, "size_mismatch".to_string()), put_raw(a, &path, Some(&token), 0, b""));
        // Over the cap is refused as such, whatever the token says, and before any of it is read.
        assert_eq!((413, "too_large".to_string()), put_raw(a, &path, Some(&token), 1_001, b""));
        assert_eq!((413, "too_large".to_string()), put_raw(a, &path, Some(&token), 1 << 40, b"only the head arrives"));
        // A part over the cap gets no URL in the first place.
        let (big, big_name, big_sha) = blob(2, 1_001);
        assert_eq!(422, post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": big_name, "size": big.len(), "sha256": big_sha})).0);
        nothing_kept("wrong lengths");

        // A body that stops short of its Content-Length: the client hung up half way.
        assert_eq!((400, "incomplete_body".to_string()), put_raw(a, &path, Some(&token), 400, &body[..150]));
        nothing_kept("short body");

        // No token, a mangled one, two of them.
        assert_eq!((403, "bad_token".to_string()), put_raw(a, &path, None, 400, &body));
        assert_eq!((403, "bad_token".to_string()), put(a, &path, "v1.9999999999.400.zz", &body));
        assert_eq!((403, "bad_token".to_string()), put(a, &path, &format!("{token}\r\n{UPLOAD_TOKEN_HEADER}: {token}"), &body));
        nothing_kept("bad tokens");
        let s = status_of(&c);
        assert_eq!((0, 1, 5, 1, 3, 0, 0), (s.uploads, s.refused_content, s.refused_size, s.incomplete, s.refused_token, s.used_bytes, s.reserved_bytes), "{s:?}");

        // More bytes than Content-Length: only the declared ones are the part, and they are right.
        assert_eq!((200, "true".to_string()), put_raw(a, &path, Some(&token), 400, &[body.clone(), vec![9; 300]].concat()));
        assert_eq!(body, std::fs::read(c.parts.join("edge-a").join(&name)).unwrap());
        assert_eq!(vec![format!("edge-a/{name}")], files_in(&c.parts));
        let (status, v) = post_as(a, "tok-a", COMMIT_PATH, commit);
        assert_eq!((200, true), (status, v["recorded"].as_bool().unwrap()), "{v}");
        c.receiver.shutdown();
    }

    #[test]
    fn an_upload_token_is_good_for_one_node_one_name_one_size_and_a_while() {
        let c = local(1_000, 0);
        let a = c.receiver.local_addr();
        let (body, name, sha) = blob(3, 300);
        let (other_body, other_name, other_sha) = blob(4, 300);
        let (path, token) = presign_local(a, &name, 300, &sha);
        let now = crate::now_unix_secs();
        assert!(token.starts_with(&format!("v1.{}.300.", now + 900)) || token.starts_with(&format!("v1.{}.300.", now + 899)), "{token}");

        // Expired: signed by this central, a second too late.
        let expired = token_from(&c, "edge-a", &name, 300, now - 1);
        assert_eq!((403, "token_expired".to_string()), put(a, &path, &expired, &body));
        // Replayed for another part of the same node, even with that part's own bytes.
        assert_eq!((403, "bad_token".to_string()), put(a, &format!("{BLOB_PREFIX}edge-a/{other_name}"), &token, &other_body));
        // Replayed under another node's directory.
        assert_eq!((403, "bad_token".to_string()), put(a, &format!("{BLOB_PREFIX}edge-b/{name}"), &token, &body));
        // Its size or its expiry edited.
        let mut fields: Vec<&str> = token.split('.').collect();
        fields[2] = "301";
        assert_eq!((403, "bad_token".to_string()), put_raw(a, &path, Some(&fields.join(".")), 301, &[body.clone(), vec![3]].concat()));
        let later = (now + 90_000).to_string();
        fields[2] = "300";
        fields[1] = &later;
        assert_eq!((403, "bad_token".to_string()), put(a, &path, &fields.join("."), &body));
        // Signed by another central (or this one before a restart): another key.
        let elsewhere = local(1_000, 0);
        assert_eq!((403, "bad_token".to_string()), put(a, &path, &token_from(&elsewhere, "edge-a", &name, 300, now + 900), &body));
        elsewhere.receiver.shutdown();
        assert_eq!(Vec::<String>::new(), files_in(&c.parts));
        assert_eq!(6, status_of(&c).refused_token);

        // Node B asks for the same part and gets its own URL and token; each lands under its own node.
        let (status, vb) = post_as(a, "tok-b", PRESIGN_PATH, serde_json::json!({"name": name, "size": 300, "sha256": sha}));
        assert_eq!((200, format!("{BLOB_PREFIX}edge-b/{name}")), (status, vb["url"].as_str().unwrap().to_string()), "{vb}");
        assert_eq!((200, "true".to_string()), put(a, &path, &token, &body));
        assert_eq!(vec![format!("edge-a/{name}")], files_in(&c.parts));
        // A's part is not B's: B is not told it is present and cannot commit it.
        assert_eq!(false, post_as(a, "tok-b", PRESIGN_PATH, serde_json::json!({"name": name, "size": 300, "sha256": sha})).1["already_present"]);
        assert_eq!(409, post_as(a, "tok-b", COMMIT_PATH, commit_body(&name, 300, &sha, serde_json::json!({}))).0);
        assert_eq!(200, post_as(a, "tok-a", COMMIT_PATH, commit_body(&name, 300, &sha, serde_json::json!({}))).0);
        let _ = other_sha;
        c.receiver.shutdown();
    }

    #[test]
    fn a_foreign_node_or_a_traversal_never_reaches_another_directory_on_centrals_disk() {
        let c = local(1_000, 0);
        let a = c.receiver.local_addr();
        let (body, name, sha) = blob(5, 64);
        let (_, token) = presign_local(a, &name, 64, &sha);
        // Asking for a URL under another node, or with a name that is a path, gets none.
        let (status, v) = post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": name, "size": 64, "sha256": sha, "node": "edge-b"}));
        assert_eq!((403, "foreign_prefix"), (status, v["error"].as_str().unwrap()), "{v}");
        for bad in [format!("../edge-b/{name}"), format!("edge-b/{name}"), format!("..%2Fedge-b%2F{name}"), format!("/{name}")] {
            let (status, v) = post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": bad, "size": 64, "sha256": sha}));
            assert_eq!((422, "bad_name"), (status, v["error"].as_str().unwrap()), "{bad}: {v}");
        }
        assert_eq!(403, post_as(a, "shared", PRESIGN_PATH, serde_json::json!({"name": name, "size": 64, "sha256": sha})).0);
        // A real token on every path that is not exactly its own.
        let outside = c.root.path().join("escaped.parquet");
        for target in [
            format!("../edge-b/{name}"),
            format!("edge-a/../edge-b/{name}"),
            format!("edge-a/..%2Fedge-b%2F{name}"),
            format!("edge-a%2F..%2Fedge-b/{name}"),
            format!("edge-a/../../escaped.parquet"),
            format!("./{name}"),
            format!("edge-a/{name}/x"),
            format!("edge-a//{name}"),
            format!("edge-a/{}", name.to_uppercase()),
            format!("edge-b/{name}"),
            format!("{name}"),
            "edge-a".to_string(),
            String::new(),
        ] {
            let (status, code) = put(a, &format!("{BLOB_PREFIX}{target}"), &token, &body);
            assert!(matches!(status, 403 | 404 | 422), "{target:?}: {status} {code}");
        }
        assert_eq!(Vec::<String>::new(), files_in(&c.parts));
        assert!(!outside.exists());
        // Only the directory itself and the catalog exist under the data dir.
        let mut top: Vec<String> = std::fs::read_dir(c.root.path()).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect();
        top.retain(|n| !n.starts_with("catalog.sqlite"));
        assert_eq!(vec!["parts".to_string()], top);
        // POST and GET are not ways in either.
        assert_eq!(404, post_as(a, "tok-a", &format!("{BLOB_PREFIX}edge-a/{name}"), serde_json::json!({})).0);
        c.receiver.shutdown();
    }

    #[test]
    fn a_duplicate_local_upload_is_checked_and_changes_nothing() {
        let c = local(1_000, 0);
        let a = c.receiver.local_addr();
        let (body, name, sha) = blob(6, 500);
        let (path, token) = presign_local(a, &name, 500, &sha);
        assert_eq!((200, "true".to_string()), put(a, &path, &token, &body));
        let stored = c.parts.join("edge-a").join(&name);
        let first = std::fs::metadata(&stored).unwrap().modified().unwrap();
        // The same bytes again (the token still stands): success, nothing rewritten, counted once.
        assert_eq!((200, "false".to_string()), put(a, &path, &token, &body));
        // Other bytes of the same size under that name: refused, and the stored part is untouched.
        let mut wrong = body.clone();
        wrong[0] ^= 0xff;
        assert_eq!((422, "bad_content".to_string()), put(a, &path, &token, &wrong));
        assert_eq!((body, first), (std::fs::read(&stored).unwrap(), std::fs::metadata(&stored).unwrap().modified().unwrap()));
        assert_eq!(vec![format!("edge-a/{name}")], files_in(&c.parts));
        let s = status_of(&c);
        assert_eq!((1, 1, 1, 500, 0), (s.uploads, s.already_present, s.refused_content, s.used_bytes, s.reserved_bytes), "{s:?}");
        // And presign now says so, so an edge would not send it at all.
        assert_eq!(true, post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": name, "size": 500, "sha256": sha})).1["already_present"]);
        c.receiver.shutdown();
    }

    #[test]
    fn the_disk_budget_refuses_with_507_counts_it_and_frees_with_a_delete() {
        let c = local(100, 250);
        let a = c.receiver.local_addr();
        let parts: Vec<(Vec<u8>, String, String)> = (0..3).map(|i| blob(10 + i, 100)).collect();
        for (body, name, sha) in &parts[..2] {
            let (path, token) = presign_local(a, name, 100, sha);
            assert_eq!((200, "true".to_string()), put(a, &path, &token, body));
        }
        let (body, name, sha) = &parts[2];
        // 200 of 250 bytes used: a third 100-byte part does not fit. No URL is handed out,
        let (status, v) = post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": name, "size": 100, "sha256": sha}));
        assert_eq!((507, "insufficient_storage"), (status, v["error"].as_str().unwrap()), "{v}");
        // and a token from before the disk filled is refused at the upload, before the body is read.
        let path = format!("{BLOB_PREFIX}edge-a/{name}");
        let token = token_from(&c, "edge-a", name, 100, crate::now_unix_secs() + 60);
        assert_eq!((507, "insufficient_storage".to_string()), put(a, &path, &token, body));
        let s = status_of(&c);
        assert_eq!((2, 200, 250, 0, 2), (s.uploads, s.used_bytes, s.budget_bytes, s.reserved_bytes, s.refused_budget), "{s:?}");
        assert_eq!(2, files_in(&c.parts).len());
        // A part already stored is still "present" on a full disk: presence is asked before room.
        assert_eq!(true, post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": parts[0].1, "size": 100, "sha256": parts[0].2})).1["already_present"]);

        // The sweep's delete gives the bytes back, and the part then fits.
        local_store(&c).delete_object("edge-a", &parts[0].1).unwrap();
        local_store(&c).delete_object("edge-a", &parts[0].1).unwrap();
        assert_eq!(100, status_of(&c).used_bytes);
        assert_eq!((200, "true".to_string()), put(a, &path, &token, body));
        assert_eq!((3, 200), (status_of(&c).uploads, status_of(&c).used_bytes));
        // A failed upload gives its reservation back too.
        local_store(&c).delete_object("edge-a", name).unwrap();
        let mut wrong = body.clone();
        wrong[1] ^= 1;
        assert_eq!((422, "bad_content".to_string()), put(a, &path, &token, &wrong));
        assert_eq!((100, 0), (status_of(&c).used_bytes, status_of(&c).reserved_bytes));
        c.receiver.shutdown();
    }

    #[test]
    fn a_second_upload_of_a_part_already_arriving_reserves_nothing_and_is_told_to_retry() {
        let c = local(100, 300);
        let a = c.receiver.local_addr();
        let (body, name, sha) = blob(30, 100);
        let (path, token) = presign_local(a, &name, 100, &sha);
        // One upload holds the connection open after its head and ten bytes.
        let mut held = std::net::TcpStream::connect(a).unwrap();
        held.write_all(format!("PUT {path} HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n{UPLOAD_TOKEN_HEADER}: {token}\r\n\r\n").as_bytes()).unwrap();
        held.write_all(&body[..10]).unwrap();
        let started = Instant::now();
        while status_of(&c).reserved_bytes != 100 {
            assert!(started.elapsed() < Duration::from_secs(5), "{:?}", status_of(&c));
            std::thread::sleep(Duration::from_millis(10));
        }
        // The same part again, on the same token, with the same bytes: it is refused and holds no room, so
        // many of them cannot make another node's uploads 507.
        for _ in 0..5 {
            let (status, why) = put(a, &path, &token, &body);
            assert_eq!((409, "upload_in_progress".to_string()), (status, why));
        }
        assert_eq!(100, status_of(&c).reserved_bytes);
        let (other, other_name, other_sha) = blob(31, 100);
        let (other_path, other_token) = presign_local(a, &other_name, 100, &other_sha);
        assert_eq!((200, "true".to_string()), put(a, &other_path, &other_token, &other));
        // The first upload is unharmed, and once it ends the part is free to be sent again.
        held.write_all(&body[10..]).unwrap();
        let mut out = Vec::new();
        held.shutdown(std::net::Shutdown::Write).unwrap();
        std::io::Read::read_to_end(&mut held, &mut out).unwrap();
        assert!(String::from_utf8_lossy(&out).starts_with("HTTP/1.1 200"), "{}", String::from_utf8_lossy(&out));
        assert_eq!((200, 0), (status_of(&c).used_bytes, status_of(&c).reserved_bytes));
        assert_eq!((200, "false".to_string()), put(a, &path, &token, &body));
        c.receiver.shutdown();
    }

    #[test]
    fn the_local_sweep_deletes_the_file_with_its_row_and_leaves_what_nobody_committed() {
        let c = local(1_000, 0);
        let a = c.receiver.local_addr();
        let (old, old_name, old_sha) = blob(20, 200);
        let (orphan, orphan_name, orphan_sha) = blob(21, 200);
        for (body, name, sha) in [(&old, &old_name, &old_sha), (&orphan, &orphan_name, &orphan_sha)] {
            let (path, token) = presign_local(a, name, 200, sha);
            assert_eq!((200, "true".to_string()), put(a, &path, &token, body));
        }
        // Only one of the two is committed; its newest data is at 2 ns.
        assert_eq!(200, post_as(a, "tok-a", COMMIT_PATH, commit_body(&old_name, 200, &old_sha, serde_json::json!({}))).0);
        let central = PartsCentral::new(PartsConfig { warm_retention_secs: 10, ..local_config(1_000, 0).parts }, c.catalog.clone()).unwrap();
        let dry = central.sweep(1_000_000, true).unwrap();
        assert_eq!((1, 2), (dry.would_delete, files_in(&c.parts).len()), "{dry:?}");
        let swept = central.sweep(1_000_000, false).unwrap();
        assert_eq!((1, 0), (swept.deleted, swept.failed), "{swept:?}");
        assert_eq!(vec![format!("edge-a/{orphan_name}")], files_in(&c.parts));
        assert_eq!(0, crate::catalog::Catalog::open(&c.catalog).unwrap().peer_part_count().unwrap());
        // Gone means gone: the edge is told to upload it again, and a commit is refused until it has.
        assert_eq!(false, post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": old_name, "size": 200, "sha256": old_sha})).1["already_present"]);
        assert_eq!(409, post_as(a, "tok-a", COMMIT_PATH, commit_body(&old_name, 200, &old_sha, serde_json::json!({}))).0);
        c.receiver.shutdown();
    }

    #[test]
    fn a_crash_between_the_rename_and_the_commit_leaves_a_part_that_is_absent_or_committable() {
        let c = local(1_000, 0);
        let a = c.receiver.local_addr();
        let (body, name, sha) = blob(30, 700);
        let (half, half_name, half_sha) = blob(31, 700);
        // Renamed into place, and then central dies before the edge's commit arrives.
        let (path, token) = presign_local(a, &name, 700, &sha);
        assert_eq!((200, "true".to_string()), put(a, &path, &token, &body));
        // Another upload was cut off mid-body by the same crash: its bytes are in a temporary file only.
        std::fs::write(c.parts.join("edge-a").join(format!("{TMP_PREFIX}0198c0de{TMP_SUFFIX}")), &half[..300]).unwrap();
        let Local { receiver, catalog, parts, root } = c;
        receiver.shutdown();
        assert_eq!(0, crate::catalog::Catalog::open(&catalog).unwrap().peer_part_count().unwrap());

        let restarted = local_start(root.path(), 1_000, 0);
        let a = restarted.local_addr();
        // The temporary file is gone; the finished part is there, whole, counted, and nothing else is.
        assert_eq!(vec![format!("edge-a/{name}")], files_in(&parts));
        assert_eq!(700, restarted.parts_handle().unwrap().store_status().unwrap().used_bytes);
        // Present and committable: the edge's retry is told so and its commit records the part.
        assert_eq!(true, post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": name, "size": 700, "sha256": sha})).1["already_present"]);
        let (status, v) = post_as(a, "tok-a", COMMIT_PATH, commit_body(&name, 700, &sha, serde_json::json!({})));
        assert_eq!((200, true), (status, v["recorded"].as_bool().unwrap()), "{v}");
        assert_eq!(body, std::fs::read(parts.join("edge-a").join(&name)).unwrap());
        // Absent: the cut-off part is not present, cannot be committed, and uploads afresh.
        assert_eq!(409, post_as(a, "tok-a", COMMIT_PATH, commit_body(&half_name, 700, &half_sha, serde_json::json!({}))).0);
        // The token from before the restart died with that process; a new one works.
        assert_eq!((403, "bad_token".to_string()), put(a, &path, &token, &body));
        let (path, token) = presign_local(a, &half_name, 700, &half_sha);
        assert_eq!((200, "true".to_string()), put(a, &path, &token, &half));
        assert_eq!(200, post_as(a, "tok-a", COMMIT_PATH, commit_body(&half_name, 700, &half_sha, serde_json::json!({}))).0);
        assert_eq!(2, crate::catalog::Catalog::open(&catalog).unwrap().peer_part_count().unwrap());
        restarted.shutdown();
    }

    #[test]
    fn a_part_larger_than_the_request_cap_is_streamed_to_disk_in_pieces() {
        // 9 MiB, over the listener's 4 MiB body cap, which a buffered request could never pass.
        let c = local(64 * 1024 * 1024, 0);
        let a = c.receiver.local_addr();
        let body: Vec<u8> = (0..9 * 1024 * 1024u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8).collect();
        let sha = hex(&Sha256::digest(&body));
        let name = name_for(&sha);
        let (_, v) = post_as(a, "tok-a", PRESIGN_PATH, serde_json::json!({"name": name, "size": body.len(), "sha256": sha}));
        let path = v["url"].as_str().unwrap();
        assert_eq!("100-continue", v["headers"]["expect"], "{v}");
        // As the edge sends it: ureq, with every header central named.
        let mut request = ureq::put(format!("http://{a}{path}"));
        for (k, h) in v["headers"].as_object().unwrap() {
            request = request.header(k, h.as_str().unwrap());
        }
        let response = request.config().http_status_as_error(false).build().send(&body[..]).unwrap();
        assert_eq!(200, response.status().as_u16());
        assert_eq!((sha, body.len() as u64), sha256_file(&c.parts.join("edge-a").join(&name)).unwrap());
        // And a refusal from the head is heard by a client that waits for 100 Continue, without sending the body.
        let started = Instant::now();
        let refused = ureq::put(format!("http://{a}{path}")).header(UPLOAD_TOKEN_HEADER, "v1.1.1.00").header("expect", "100-continue").config().http_status_as_error(false).build().send(&body[..]).unwrap();
        assert_eq!(403, refused.status().as_u16());
        assert!(started.elapsed() < Duration::from_secs(5), "{:?}", started.elapsed());
        c.receiver.shutdown();
    }

    #[test]
    fn an_old_flat_catalog_is_set_aside_not_misread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.sqlite");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE peer_parts (name TEXT PRIMARY KEY, size INTEGER NOT NULL, rows INTEGER NOT NULL, min_observed INTEGER NOT NULL, max_observed INTEGER NOT NULL, origin TEXT NOT NULL, committed_ms INTEGER NOT NULL);
                 INSERT INTO peer_parts VALUES ('x.parquet', 1, 1, 1, 2, 'o', 3);",
            )
            .unwrap();
        }
        let cat = crate::catalog::Catalog::open(&path).unwrap();
        assert_eq!(0, cat.peer_part_count().unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(1, conn.query_row("SELECT COUNT(*) FROM peer_parts_flat_legacy", [], |r| r.get::<_, i64>(0)).unwrap());
    }

    // ---- the edge lifecycle: pin, upload on a timer, retire ------------------------------------

    use crate::parts_edge::{self, Driver, EdgePartsConfig};

    /// An edge whose files are also in a catalog (retention and reclaim read it), written `age_mins * i` minutes ago.
    struct Life {
        control: Arc<SyncControl>,
        data: tempfile::TempDir,
        store: tempfile::TempDir,
        catalog: crate::catalog::Catalog,
        paths: Vec<PathBuf>,
    }

    fn life_edge(url: &str, hours: &[u64]) -> Life {
        let data = tempfile::tempdir().unwrap();
        let store = tempfile::tempdir().unwrap();
        write_files(store.path(), hours);
        let mut catalog = crate::catalog::Catalog::open_in_memory().unwrap();
        let mut paths = crate::store::list_parquet_files(store.path());
        // Oldest data first, as `hours` is; the file names carry random ids, so order by the hour directory.
        paths.sort_by_key(|p| p.parent().and_then(|d| d.parent()).map(|d| d.to_path_buf()));
        let now = std::time::SystemTime::now();
        for (i, path) in paths.iter().enumerate() {
            let stats = crate::store::read_file_stats(path).unwrap();
            let key = crate::partition::PartitionKey::at(hours[i], "info");
            catalog.commit_merge(i as u64 + 1, uuid::Uuid::from_u128(i as u128 + 1), &[crate::catalog::FileEntry { path: path.clone(), key, stats }], &[], 0).unwrap();
            // Written (paths.len() - i) minutes ago: a strict order for "newest first".
            let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            file.set_modified(now - Duration::from_secs(60 * (paths.len() - i) as u64)).unwrap();
        }
        let target = SyncTarget { url: url.into(), token: Some("tok-a".into()), min_severity: 0, fidelity: 100, anomaly_boost: false };
        let control = SyncControl::new("edge".into(), SyncStore::new(data.path()), Some(target));
        control.set_store_dir(store.path().to_path_buf());
        Life { control, data, store, catalog, paths }
    }

    fn target_for(url: &str) -> SyncTarget {
        SyncTarget { url: url.into(), token: Some("tok-a".into()), min_severity: 0, fidelity: 100, anomaly_boost: false }
    }

    /// A URL nothing listens on: central is down.
    fn dead_url() -> String {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", l.local_addr().unwrap())
    }

    fn timer(interval_secs: u64, pin_bytes: u64) -> EdgePartsConfig {
        EdgePartsConfig { interval: Duration::from_secs(interval_secs), pin_bytes, pin_age: Duration::from_secs(7 * 24 * 3_600) }
    }

    fn wait_job(control: &SyncControl) {
        let started = Instant::now();
        while control.status().parts.is_none_or(|p| p.state == "running") {
            assert!(started.elapsed() < Duration::from_secs(20), "the job did not finish");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn tiers(c: &Local) -> BTreeMap<String, String> {
        let cat = crate::catalog::Catalog::open(&c.catalog).unwrap();
        cat.list_peer_parts(&crate::catalog::PartsFilter { limit: 1000, ..Default::default() }).unwrap().into_iter().map(|p| (p.name, p.tier)).collect()
    }

    fn pass(e: &mut Life, budget: u64, now_secs: u64, config: &EdgePartsConfig) -> crate::budget::PassOutcome {
        let pins = parts_edge::pins_for_pass(&e.control, config, e.store.path(), budget);
        let wal = e.data.path().join("wal");
        let out = crate::budget::expire_and_relieve(e.store.path(), &wal, &mut e.catalog, &crate::config::Tenancy::default(), budget, &crate::budget::Watermarks::default(), now_secs, &pins);
        parts_edge::note_gave_up(&e.control, out.gave_up);
        out
    }

    #[test]
    fn central_down_pins_to_the_limit_reclaim_past_it_is_counted_and_central_back_gets_the_rest() {
        let c = local(512 * 1024 * 1024, 0);
        let real = format!("http://{}", c.receiver.local_addr());
        let hours: Vec<u64> = (1000..1010).collect();
        let mut e = life_edge(&dead_url(), &hours);
        let sizes: Vec<u64> = e.paths.iter().map(|p| std::fs::metadata(p).unwrap().len()).collect();
        let rows: Vec<u64> = e.paths.iter().map(|p| crate::store::read_file_stats(p).unwrap().rows).collect();
        // Pin the two newest files; the disk budget is four times that, the most a pin may take.
        let pin_bytes = sizes[8] + sizes[9];
        let config = timer(10, pin_bytes);
        let budget = 4 * pin_bytes;
        let mut driver = Driver::new(Arc::clone(&e.control), config.clone(), e.store.path().to_path_buf(), FAST);
        let t0 = crate::now_unix_nanos() / 1_000_000;

        // Central is down: the timer's run fails, nothing is confirmed, the backlog is all ten.
        driver.step(t0);
        wait_job(&e.control);
        driver.due_now();
        driver.step(t0 + 1);
        let s = e.control.status();
        assert_eq!(Some("error"), s.parts.as_ref().map(|p| p.state.as_str()), "{s:?}");
        assert_eq!((1, 10, sizes.iter().sum::<u64>()), (s.edge_parts.runs, s.edge_parts.backlog_parts, s.edge_parts.backlog_bytes), "{:?}", s.edge_parts);
        assert_eq!((Some("error".to_string()), Some(t0 + 10_000)), (s.edge_parts.last_run_state, s.edge_parts.next_run_ms));

        // Disk pressure: reclaim wants the oldest. The two newest are pinned and survive; the rest go, oldest first, and are counted.
        let out = pass(&mut e, budget, 1_011 * 3_600, &config);
        let gone: Vec<usize> = (0..10).filter(|i| !e.paths[*i].exists()).collect();
        assert!(!gone.is_empty() && gone.iter().all(|i| *i < 8), "{gone:?}");
        assert_eq!(gone, (0..gone.len()).collect::<Vec<_>>(), "oldest first");
        assert!(e.paths[8].exists() && e.paths[9].exists(), "the pinned files survive");
        let (lost_bytes, lost_rows) = (gone.iter().map(|i| sizes[*i]).sum::<u64>(), gone.iter().map(|i| rows[*i]).sum::<u64>());
        assert_eq!((gone.len() as u64, lost_rows, lost_bytes), (out.gave_up.parts, out.gave_up.rows, out.gave_up.bytes), "{:?}", out.gave_up);
        let s = e.control.status().edge_parts;
        assert_eq!((true, pin_bytes, 2, sizes[8] + sizes[9], 8), (s.pin_enabled, s.pin_limit_bytes, s.pinned_parts, s.pinned_bytes, s.unpinned_parts), "{s:?}");
        assert_eq!((gone.len() as u64, lost_rows, lost_bytes), (s.gave_up_parts, s.gave_up_rows, s.gave_up_bytes), "{s:?}");
        // The count outlives a restart.
        assert_eq!(out.gave_up, parts_edge::load_gave_up(e.data.path()));
        assert_eq!(0, out.reclaim.unwrap().unwrap().files_dropped.iter().filter(|p| **p == e.paths[8] || **p == e.paths[9]).count());

        // Central is back: the next timer run uploads and commits every file still here, pinned or not.
        let left = 10 - gone.len();
        e.control.set_target(Some(target_for(&real)));
        driver.step(t0 + 10_000);
        wait_job(&e.control);
        driver.due_now();
        driver.step(t0 + 10_001);
        let s = e.control.status();
        assert_eq!(("done", left as u64), (s.parts.as_ref().unwrap().state.as_str(), s.parts.as_ref().unwrap().uploaded), "{s:?}");
        assert_eq!((2, 0, left as u64), (s.edge_parts.runs, s.edge_parts.backlog_parts, s.edge_parts.last_run_uploaded), "{:?}", s.edge_parts);
        assert_eq!(left, tiers(&c).len());
        assert!(tiers(&c).values().all(|t| t == "warm"));
        assert_eq!(left, files_in(&c.parts).len());
        assert_eq!(left, parts_edge::uploaded_names(e.data.path()).len());

        // Nothing is unconfirmed now, so retention deletes the lot; each deleted file is retired and its part turns cold.
        let out = pass(&mut e, budget, 10_000_000, &config);
        let remaining: u64 = (0..10).filter(|i| !gone.contains(i)).map(|i| sizes[i]).sum();
        assert_eq!((remaining, 0), (out.expiry.bytes_reclaimed, out.gave_up.parts), "{:?}", out.expiry);
        assert!(e.paths.iter().all(|p| !p.exists()));
        driver.due_now();
        driver.step(t0 + 10_002);
        let s = e.control.status().edge_parts;
        assert_eq!((left as u64, 0, None), (s.retired, s.retire_pending, s.retire_error.clone()), "{s:?}");
        assert!(tiers(&c).values().all(|t| t == "cold"), "{:?}", tiers(&c));
        assert!(parts_edge::uploaded_names(e.data.path()).is_empty(), "retired lines are forgotten");
        // The cold parts are still on central's disk: that is the point.
        assert_eq!(left, files_in(&c.parts).len());
        c.receiver.shutdown();
    }

    #[test]
    fn nothing_is_pinned_while_the_timer_is_off_or_no_peer_is_set() {
        let mut e = life_edge(&dead_url(), &[1000, 1001, 1002]);
        let size: u64 = e.paths.iter().map(|p| std::fs::metadata(p).unwrap().len()).sum();
        let off = EdgePartsConfig::default();
        assert!(!off.timer_on());
        let pins = parts_edge::pins_for_pass(&e.control, &off, e.store.path(), 4 * size);
        assert!(pins.is_empty() && pins.unconfirmed.is_empty());
        // Retention then deletes unconfirmed files exactly as it did before pinning existed.
        let out = pass(&mut e, 4 * size, 10_000_000, &off);
        assert_eq!((3, 0), (out.expiry.expired.len(), out.gave_up.parts as usize));
        let e = life_edge(&dead_url(), &[1000]);
        e.control.set_target(None);
        let pins = parts_edge::pins_for_pass(&e.control, &timer(60, 1 << 30), e.store.path(), 0);
        assert!(pins.is_empty(), "no peer: nobody to upload, so nothing is held back");
    }

    #[test]
    fn an_old_unconfirmed_file_is_not_pinned_and_the_limit_never_passes_a_quarter_of_the_budget() {
        let e = life_edge(&dead_url(), &[1000, 1001, 1002]);
        // Files are 3, 2 and 1 minutes old; an age limit of 150 s keeps the two newest.
        let by_age = EdgePartsConfig { pin_age: Duration::from_secs(150), ..timer(60, 1 << 30) };
        let pins = parts_edge::pins_for_pass(&e.control, &by_age, e.store.path(), 0);
        let mut pinned: Vec<_> = pins.pinned.iter().cloned().collect();
        pinned.sort();
        let mut want = vec![e.paths[1].clone(), e.paths[2].clone()];
        want.sort();
        assert_eq!(want, pinned);
        assert_eq!(3, pins.unconfirmed.len());
        // A quarter of the budget holds exactly the two newest files, whatever pin_bytes says. The files
        // differ by a byte or two, so the budget is built from their real sizes.
        let newest_two: u64 = e.paths[1..].iter().map(|p| std::fs::metadata(p).unwrap().len()).sum();
        let pins = parts_edge::pins_for_pass(&e.control, &timer(60, 1 << 30), e.store.path(), 4 * newest_two);
        assert_eq!(2, pins.pinned.len(), "{:?}", e.control.status().edge_parts);
        assert_eq!(newest_two, e.control.status().edge_parts.pin_limit_bytes);
    }

    #[test]
    fn a_file_removed_after_upload_is_retired_so_central_marks_it_cold_and_the_rest_stay_warm() {
        let c = local(512 * 1024 * 1024, 0);
        let e = life_edge(&format!("http://{}", c.receiver.local_addr()), &[1000, 1001, 1002]);
        let mut driver = Driver::new(Arc::clone(&e.control), timer(10, 1 << 30), e.store.path().to_path_buf(), FAST);
        let t0 = crate::now_unix_nanos() / 1_000_000;
        driver.step(t0);
        wait_job(&e.control);
        assert_eq!(3, tiers(&c).len());
        let names = parts_edge::uploaded_names(e.data.path());
        let relative = |p: &PathBuf| p.strip_prefix(e.store.path()).unwrap().to_string_lossy().into_owned();
        let gone_name = names[&relative(&e.paths[0])].clone();

        std::fs::remove_file(&e.paths[0]).unwrap();
        assert_eq!(1, parts_edge::pending_retires(e.store.path(), e.data.path()).len());
        driver.due_now();
        driver.step(t0 + 1);
        let t = tiers(&c);
        assert_eq!(Some(&"cold".to_string()), t.get(&gone_name), "{t:?}");
        assert_eq!(2, t.values().filter(|v| *v == "warm").count(), "{t:?}");
        assert!(parts_edge::pending_retires(e.store.path(), e.data.path()).is_empty());
        // Central keeps the cold part; a second pass has nothing to say.
        assert_eq!(3, files_in(&c.parts).len());
        let s = e.control.status().edge_parts;
        assert_eq!((1, 0), (s.retired, s.retire_pending), "{s:?}");
        let cat = crate::catalog::Catalog::open(&c.catalog).unwrap();
        assert_eq!(1, cat.peer_parts_audit(10).unwrap().len(), "central audited the move to cold");
        c.receiver.shutdown();
    }

    #[test]
    fn a_retire_central_never_heard_is_still_pending_after_a_restart_and_is_sent_then() {
        let c = local(512 * 1024 * 1024, 0);
        let real = format!("http://{}", c.receiver.local_addr());
        let e = life_edge(&real, &[1000, 1001]);
        let mut driver = Driver::new(Arc::clone(&e.control), timer(10, 1 << 30), e.store.path().to_path_buf(), FAST);
        let t0 = crate::now_unix_nanos() / 1_000_000;
        driver.step(t0);
        wait_job(&e.control);
        let names = parts_edge::uploaded_names(e.data.path());
        let name = names[&e.paths[1].strip_prefix(e.store.path()).unwrap().to_string_lossy().into_owned()].clone();

        // The file goes while central is unreachable: the retire fails and is kept, with an error to show.
        std::fs::remove_file(&e.paths[1]).unwrap();
        e.control.set_target(Some(target_for(&dead_url())));
        driver.due_now();
        driver.step(t0 + 1);
        let s = e.control.status().edge_parts;
        assert_eq!((0, 1), (s.retired, s.retire_pending), "{s:?}");
        assert!(s.retire_error.is_some());
        assert_eq!(Some(&"warm".to_string()), tiers(&c).get(&name));
        // Backoff: an immediate second look does not try again.
        driver.due_now();
        driver.step(t0 + 2);

        // Restart: a new process has only the data dir. The pending retire is rebuilt from it and goes out.
        let control = SyncControl::new("edge".into(), SyncStore::new(e.data.path()), Some(target_for(&real)));
        control.set_store_dir(e.store.path().to_path_buf());
        assert_eq!(1, parts_edge::pending_retires(e.store.path(), e.data.path()).len());
        let mut driver = Driver::new(Arc::clone(&control), timer(10, 1 << 30), e.store.path().to_path_buf(), FAST);
        driver.step(t0 + 3);
        let s = control.status().edge_parts;
        assert_eq!((1, 0, None), (s.retired, s.retire_pending, s.retire_error.clone()), "{s:?}");
        assert_eq!(Some(&"cold".to_string()), tiers(&c).get(&name));
        assert!(parts_edge::pending_retires(e.store.path(), e.data.path()).is_empty());
        wait_job(&control);
        c.receiver.shutdown();
    }

    #[test]
    fn compaction_of_uploaded_files_retires_the_old_parts_and_uploads_the_merged_one() {
        let c = local(512 * 1024 * 1024, 0);
        let mut e = life_edge(&format!("http://{}", c.receiver.local_addr()), &[1000, 1000, 1000, 1000]);
        let mut driver = Driver::new(Arc::clone(&e.control), timer(10, 1 << 30), e.store.path().to_path_buf(), FAST);
        let t0 = crate::now_unix_nanos() / 1_000_000;
        driver.step(t0);
        wait_job(&e.control);
        let before = tiers(&c);
        assert_eq!(4, before.len());

        let config = crate::compact::CompactConfig { min_files: 2, ..Default::default() };
        let report = crate::compact::compact(e.store.path(), &mut e.catalog, &config, None).unwrap();
        assert_eq!((1, 4, 80), (report.partitions_compacted, report.files_replaced, report.rows), "{report:?}");
        let merged = crate::store::list_parquet_files(e.store.path());
        assert_eq!(1, merged.len());
        // The merged file is a new, unconfirmed file: pinned until it is uploaded.
        let pins = parts_edge::pins_for_pass(&e.control, &timer(10, 1 << 30), e.store.path(), 0);
        assert_eq!((1, 1), (pins.pinned.len(), pins.unconfirmed.len()));
        assert_eq!(4, parts_edge::pending_retires(e.store.path(), e.data.path()).len());

        driver.due_now();
        driver.step(t0 + 10_000);
        wait_job(&e.control);
        driver.due_now();
        driver.step(t0 + 10_001);
        let after = tiers(&c);
        assert_eq!(5, after.len(), "{after:?}");
        let warm: Vec<_> = after.iter().filter(|(_, t)| *t == "warm").collect();
        let cold: Vec<_> = after.iter().filter(|(_, t)| *t == "cold").map(|(n, _)| n.clone()).collect();
        assert_eq!(1, warm.len(), "{after:?}");
        assert!(!before.contains_key(warm[0].0), "the merged part is new");
        assert_eq!(before.keys().cloned().collect::<Vec<_>>(), cold, "every replaced part is retired, none stranded warm");
        let catalog = crate::catalog::Catalog::open(&c.catalog).unwrap();
        let rows: u64 = catalog.list_peer_parts(&crate::catalog::PartsFilter { tier: Some("warm".into()), limit: 10, ..Default::default() }).unwrap().iter().map(|p| p.rows).sum();
        assert_eq!(80, rows);
        assert_eq!(1, parts_edge::uploaded_names(e.data.path()).len());
        c.receiver.shutdown();
    }

    #[test]
    fn the_timer_never_overlaps_a_running_upload_and_runs_again_only_when_due() {
        // A peer that accepts connections and never answers: the manual job sits in its probe.
        let hang = TcpListener::bind("127.0.0.1:0").unwrap();
        let e = life_edge(&format!("http://{}", hang.local_addr().unwrap()), &[1000]);
        e.control.start_parts_with(0, HOUR_MS, FAST).unwrap();
        let mut driver = Driver::new(Arc::clone(&e.control), timer(10, 1 << 30), e.store.path().to_path_buf(), FAST);
        let t0 = crate::now_unix_nanos() / 1_000_000;
        driver.step(t0);
        let s = e.control.status().edge_parts;
        assert_eq!((0, 1, Some(t0 + 10_000)), (s.runs, s.skipped_busy, s.next_run_ms), "{s:?}");
        assert!(e.control.start_parts_all(FAST).is_err(), "a second job is refused whoever asks");
        e.control.cancel_parts();
        // Not due again until the interval has passed.
        driver.step(t0 + 9_999);
        assert_eq!(1, e.control.status().edge_parts.skipped_busy);
        driver.step(t0 + 10_000);
        assert_eq!(2, e.control.status().edge_parts.skipped_busy);
        // The interval must be 0 (off) or at least 10 s.
        assert!(timer(5, 1).check().is_err() && timer(10, 1).check().is_ok() && EdgePartsConfig::default().check().is_ok());
        let parsed: crate::config::Config = toml::from_str("[storage]\ndata_dir = \"/tmp/x\"\n[sync_parts]\ninterval = \"2m\"\npin_bytes = 5\npin_age = \"1d\"\n").unwrap();
        assert_eq!((120, 5, 86_400), (parsed.sync_parts.interval.as_secs(), parsed.sync_parts.pin_bytes, parsed.sync_parts.pin_age.as_secs()));
        assert!(toml::from_str::<crate::config::Config>("[storage]\ndata_dir = \"/tmp/x\"\n[sync_parts]\ninterval = \"1s\"\n").unwrap().validate().is_err());
    }
}
