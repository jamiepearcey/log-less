//! Parquet part upload: the bulk, cold path from an edge to central object storage.
//!
//! The record path (`sync`) sends a summary or a copy of what happens, record by record. This path
//! moves whole committed Parquet files instead, and never through central's own disk: central holds
//! the S3-compatible credentials and hands an edge a short-lived presigned PUT URL per file, so the
//! edge needs no cloud keys and central never carries the bytes.
//!
//! | Path (central, bearer token as `/v1/ingest`) | Behaviour |
//! |---|---|
//! | `POST /v1/parts/presign` | `{name, size, sha256}`: `already_present`, or a PUT URL valid `url_ttl_secs` |
//! | `POST /v1/parts/commit` | `{name, size, sha256, rows, min_unix_nano, max_unix_nano, origin}`: checks the object, records it |
//! | `POST /v1/parts/retire` | `{name}`: the node no longer holds its local copy, so the part is `cold` |
//!
//! A part is named by its content: `<sha256 hex>.parquet`, stored at `<prefix>/<node>/<name>` in the
//! bucket, where `<node>` is the node the bearer token belongs to (`[[peer.nodes]]`). The key is built
//! here from the token's node and a validated name; nothing in a request body can choose another
//! prefix, and a body that names another node is refused with 403 (see [`valid_node`]).
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
//! Storage checks the content, not central and not the edge's word for it: the presigned PUT signs
//! `x-amz-checksum-sha256` (the name's digest, base64), so the edge must send it and the storage
//! refuses a body that does not hash to it. "Stored intact" is a HEAD with `x-amz-checksum-mode:
//! ENABLED` answering that size and that same checksum; an object of the right size without it, or
//! with another, is not present: presign hands out a URL to upload it again and commit refuses it.
//!
//! # The edge job
//!
//! Started by hand for a time range (`SyncControl::start_parts`), like the range fetch. It finds the
//! committed files overlapping the range, hashes each, asks for a URL, streams the file to it, and
//! commits. A line per committed file goes to [`STATE_FILE`] in the data dir, so a rerun or a restart
//! skips what is done without hashing it again. A failed PUT is retried with backoff, and an expired
//! or refused URL is presigned again. The job runs only against a peer whose capability document
//! says `parts: true`.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::httpd::Reply;
use crate::s3_sigv4::{self, Credentials};
use crate::sync::{SyncControl, SyncTarget};

pub const PRESIGN_PATH: &str = "/v1/parts/presign";
pub const COMMIT_PATH: &str = "/v1/parts/commit";
pub const RETIRE_PATH: &str = "/v1/parts/retire";
/// Edge side: one JSON line per committed file, in the data dir.
pub const STATE_FILE: &str = "parts-uploaded.jsonl";
/// S3 refuses a presigned URL valid for longer than seven days.
const MAX_TTL_SECS: u64 = 7 * 24 * 3_600;
/// A URL this close to expiry is presigned again before it is used.
const EXPIRY_MARGIN_MS: u64 = 5_000;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(20);
const STORAGE_TIMEOUT: Duration = Duration::from_secs(10);

/// `[peer.parts]` on the central instance. S3-compatible storage only.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartsConfig {
    #[serde(default)]
    pub enabled: bool,
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
    #[serde(default = "default_url_ttl")]
    pub url_ttl_secs: u64,
    /// A warm part whose newest data is older than this many seconds is expired by a sweep. 0 keeps it.
    #[serde(default)]
    pub warm_retention_secs: u64,
    /// The same for a cold part.
    #[serde(default)]
    pub cold_retention_secs: u64,
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

impl Default for PartsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            bucket: String::new(),
            prefix: String::new(),
            region: default_region(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            max_part_bytes: default_max_part_bytes(),
            url_ttl_secs: default_url_ttl(),
            warm_retention_secs: 0,
            cold_retention_secs: 0,
        }
    }
}

impl std::fmt::Debug for PartsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartsConfig")
            .field("enabled", &self.enabled)
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("max_part_bytes", &self.max_part_bytes)
            .field("url_ttl_secs", &self.url_ttl_secs)
            .field("warm_retention_secs", &self.warm_retention_secs)
            .field("cold_retention_secs", &self.cold_retention_secs)
            .finish()
    }
}

impl PartsConfig {
    /// Why this configuration cannot serve parts. Never mentions the secret's value.
    pub fn check(&self) -> Result<(), String> {
        if !self.enabled {
            return Err("[peer.parts] is not enabled".into());
        }
        split_endpoint(&self.endpoint)?;
        if self.bucket.is_empty() || self.bucket.contains('/') {
            return Err("[peer.parts] bucket must be a bucket name".into());
        }
        if self.region.is_empty() || self.access_key_id.is_empty() || self.secret_access_key.is_empty() {
            return Err("[peer.parts] needs region, access_key_id and secret_access_key".into());
        }
        if self.max_part_bytes == 0 {
            return Err("[peer.parts] max_part_bytes must be above 0".into());
        }
        if !(1..=MAX_TTL_SECS).contains(&self.url_ttl_secs) {
            return Err(format!("[peer.parts] url_ttl_secs must be 1 to {MAX_TTL_SECS}"));
        }
        Ok(())
    }
}

/// `(scheme, authority)` of an endpoint that is only a scheme and a host.
fn split_endpoint(endpoint: &str) -> Result<(&str, &str), String> {
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

/// A node id is 1 to 64 of `A-Za-z0-9._-`, and not `.` or `..`: one path segment, never a traversal, and
/// the same in a key and in a URL.
pub fn valid_node(node: &str) -> bool {
    (1..=64).contains(&node.len())
        && node != "."
        && node != ".."
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
    if sha256_hex.len() != 64 {
        return None;
    }
    let bytes: Vec<u8> = (0..32).map(|i| u8::from_str_radix(sha256_hex.get(2 * i..2 * i + 2)?, 16).ok()).collect::<Option<_>>()?;
    Some(base64(&bytes))
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

/// Central's half: presigns and records. Built only from a configuration that passed [`PartsConfig::check`].
pub struct PartsCentral {
    config: PartsConfig,
    catalog_path: PathBuf,
    agent: ureq::Agent,
}

impl PartsCentral {
    pub fn new(config: PartsConfig, catalog_path: PathBuf) -> Result<Self, String> {
        config.check()?;
        let agent = ureq::Agent::config_builder().timeout_global(Some(STORAGE_TIMEOUT)).http_status_as_error(false).build().into();
        Ok(Self { config, catalog_path, agent })
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

    /// Whether the object is stored with exactly this size and SHA-256, as the storage itself reports
    /// it. A wrong size, no checksum or another checksum is not intact.
    fn stored_intact(&self, node: &str, name: &str, size: u64, sha256: &str) -> Result<bool, String> {
        let (url, _) = self.presigned("HEAD", node, name, &[(CHECKSUM_MODE, "ENABLED")]);
        let response = self.agent.head(&url).header(CHECKSUM_MODE, "ENABLED").call().map_err(|e| format!("object storage is unreachable: {e}"))?;
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
            Ok(false) => {
                let checksum = checksum_b64(&req.sha256).expect("checked by check_part");
                let (url, expires_at_ms) = self.presigned("PUT", node, &req.name, &[(CHECKSUM_SHA256, &checksum)]);
                let headers = BTreeMap::from([(CHECKSUM_SHA256.to_string(), checksum)]);
                PresignReply { already_present: false, url: Some(url), headers: Some(headers), expires_at_ms }
            }
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
            Ok(false) => return error_reply(409, "not_uploaded", "no object of that name, size and SHA-256 checksum is in storage; upload it first"),
            Err(m) => return error_reply(502, "storage_unavailable", &m),
        }
        let recorded = crate::catalog::Catalog::open(&self.catalog_path).and_then(|c| c.record_peer_part(node, &part, now_ms()));
        match recorded {
            Ok(recorded) => Reply::json(200, serde_json::json!({ "recorded": recorded }).to_string()),
            Err(e) => error_reply(503, "catalog_busy", &format!("the part could not be recorded: {e}")).with_header("Retry-After", "1"),
        }
    }
}

impl PartsCentral {
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

    /// Deletes the object at `<prefix>/<node>/<name>`. A missing object counts as deleted.
    fn delete_object(&self, node: &str, name: &str) -> Result<(), String> {
        if !valid_node(node) || !valid_name(name) {
            return Err("the catalogued node or name is not valid; refusing to build a key from it".into());
        }
        let (url, _) = self.presigned("DELETE", node, name, &[]);
        let response = self.agent.delete(&url).call().map_err(|e| format!("object storage is unreachable: {e}"))?;
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

#[derive(Debug, Serialize, Deserialize)]
struct StateLine {
    path: String,
    size: u64,
    name: String,
}

/// `(path relative to the store, size)` of every file a run already committed.
fn load_state(state_dir: &Path) -> HashMap<String, u64> {
    let Ok(text) = std::fs::read_to_string(state_dir.join(STATE_FILE)) else { return HashMap::new() };
    // A torn last line (a crash mid-append) is skipped: that file is uploaded again, which is idempotent.
    text.lines().filter_map(|l| serde_json::from_str::<StateLine>(l).ok()).map(|l| (l.path, l.size)).collect()
}

fn append_state(state_dir: &Path, line: &StateLine) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(state_dir.join(STATE_FILE))?;
    let mut bytes = serde_json::to_vec(line).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    file.write_all(&bytes)?;
    file.sync_data()
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
                let file = std::fs::File::open(path).map_err(|e| Failure::Rejected(format!("{} could not be read: {e}", path.display())))?;
                let mut request = self.storage.put(url);
                for (k, v) in p.headers.iter().flatten() {
                    request = request.header(k, v);
                }
                match request.send(file) {
                    Ok(r) if r.status().is_success() => put_done = true,
                    Ok(r) if r.status().as_u16() == 403 => {
                        // Expired or refused: a new URL, at once.
                        last = "Object storage refused the upload URL (HTTP 403); presigning again.".into();
                        presigned = None;
                        attempt += 1;
                        continue;
                    }
                    Ok(r) => last = format!("Object storage answered HTTP {} to the upload.", r.status().as_u16()),
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
                        if take(&state.expire_puts) || !signed || (checksum.is_some() != checksum_signed) {
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
            let key = crate::partition::PartitionKey { epoch_hour: *hour, bucket: "info".into() };
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
}
