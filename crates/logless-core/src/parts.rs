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
//!
//! A part is named by its content: `<sha256 hex>.parquet`, stored at `<prefix>/<name>` in the bucket.
//! The same file uploaded twice is the same object, so every step is idempotent: a presign for an
//! object of that name and size answers `already_present`, and a repeated commit changes nothing.
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

pub fn name_for(sha256_hex: &str) -> String {
    format!("{sha256_hex}.parquet")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PresignRequest {
    pub name: String,
    pub size: u64,
    pub sha256: String,
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

    fn raw_key_path(&self, name: &str) -> String {
        let mut path = format!("/{}", s3_sigv4::encode_segment(&self.config.bucket));
        for segment in self.config.prefix.split('/').filter(|s| !s.is_empty()) {
            path.push('/');
            path.push_str(&s3_sigv4::encode_segment(segment));
        }
        path.push('/');
        path.push_str(name);
        path
    }

    /// A presigned URL for `method` on the part's key, and when it stops working.
    fn presigned(&self, method: &str, name: &str) -> (String, u64) {
        let (scheme, host) = split_endpoint(&self.config.endpoint).expect("checked at construction");
        let creds = Credentials { access_key: self.config.access_key_id.clone(), secret_key: self.config.secret_access_key.clone() };
        let now = crate::now_unix_nanos() / 1_000_000_000;
        let path_query = s3_sigv4::presign(&creds, method, host, &self.raw_key_path(name), &self.config.region, now, self.config.url_ttl_secs);
        (format!("{scheme}://{host}{path_query}"), (now + self.config.url_ttl_secs) * 1_000)
    }

    /// The stored object's size, or None when there is no such object.
    fn head(&self, name: &str) -> Result<Option<u64>, String> {
        let (url, _) = self.presigned("HEAD", name);
        let response = self.agent.head(&url).call().map_err(|e| format!("object storage is unreachable: {e}"))?;
        match response.status().as_u16() {
            200 => Ok(response.headers().get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok())),
            404 => Ok(None),
            other => Err(format!("object storage answered HTTP {other} to a HEAD")),
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

    pub fn presign(&self, body: &[u8]) -> Reply {
        let req: PresignRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return error_reply(400, "bad_request", &format!("the body is not a presign request: {e}")),
        };
        if let Err(reply) = self.check_part(&req.name, req.size, &req.sha256) {
            return reply;
        }
        let reply = match self.head(&req.name) {
            Ok(Some(size)) if size == req.size => PresignReply { already_present: true, url: None, headers: None, expires_at_ms: 0 },
            // Absent, or a damaged object under a content-addressed name: either way, upload it.
            Ok(_) => {
                let (url, expires_at_ms) = self.presigned("PUT", &req.name);
                PresignReply { already_present: false, url: Some(url), headers: None, expires_at_ms }
            }
            Err(m) => return error_reply(502, "storage_unavailable", &m),
        };
        Reply::json(200, serde_json::to_string(&reply).unwrap_or_default())
    }

    pub fn commit(&self, body: &[u8]) -> Reply {
        let part: CommittedPart = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return error_reply(400, "bad_request", &format!("the body is not a commit request: {e}")),
        };
        if let Err(reply) = self.check_part(&part.name, part.size, &part.sha256) {
            return reply;
        }
        if part.origin.is_empty() || part.origin.len() > 128 || part.min_unix_nano > part.max_unix_nano {
            return error_reply(422, "bad_part", "origin is 1 to 128 characters and min_unix_nano is at most max_unix_nano");
        }
        match self.head(&part.name) {
            Ok(Some(size)) if size == part.size => {}
            Ok(_) => return error_reply(409, "not_uploaded", "no object of that name and size is in storage; upload it first"),
            Err(m) => return error_reply(502, "storage_unavailable", &m),
        }
        let recorded = crate::catalog::Catalog::open(&self.catalog_path).and_then(|c| c.record_peer_part(&part, now_ms()));
        match recorded {
            Ok(recorded) => Reply::json(200, serde_json::json!({ "recorded": recorded }).to_string()),
            Err(e) => error_reply(503, "catalog_busy", &format!("the part could not be recorded: {e}")).with_header("Retry-After", "1"),
        }
    }
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
                let request = PresignRequest { name: part.name.clone(), size: part.size, sha256: part.sha256.clone() };
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
    use crate::peer::{self, Admitted, PeerConfig};
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
        let (url, expires) = central.presigned("PUT", &name);
        assert!(url.starts_with(&format!("http://127.0.0.1:9000/logs/edge%20parts/{name}?X-Amz-Algorithm=AWS4-HMAC-SHA256&")), "{url}");
        assert!(url.contains("X-Amz-Expires=60&") && url.contains("&X-Amz-Signature="), "{url}");
        assert!(expires.abs_diff(now_ms() + 60_000) < 2_000);
    }

    // ---- a fake S3 server ----------------------------------------------------------------

    #[derive(Default)]
    struct Fake {
        objects: Mutex<Map<String, Vec<u8>>>,
        puts: AtomicU64,
        heads: AtomicU64,
        /// The next this many PUTs answer 500.
        fail_puts: AtomicU64,
        /// The next this many PUTs answer 403, as an expired URL does.
        expire_puts: AtomicU64,
        /// PUTs after this many successes answer 500 (u64::MAX: never).
        fail_after: AtomicU64,
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
                loop {
                    let mut line = String::new();
                    if input.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap_or(0);
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
                            Some(o) if signed => reply(&mut out, "200 OK", &format!("content-length: {}\r\n", o.len())),
                            _ => reply(&mut out, "404 Not Found", "content-length: 0\r\n"),
                        }
                    }
                    "PUT" => {
                        let mut body = vec![0u8; length];
                        let _ = input.read_exact(&mut body);
                        state.puts.fetch_add(1, Ordering::SeqCst);
                        let take = |n: &AtomicU64| n.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1)).is_ok();
                        if take(&state.expire_puts) || !signed {
                            reply(&mut out, "403 Forbidden", "content-length: 0\r\n");
                        } else if take(&state.fail_puts) || !take(&state.fail_after) {
                            reply(&mut out, "500 Internal Server Error", "content-length: 0\r\n");
                        } else {
                            state.objects.lock().unwrap().insert(key, body);
                            reply(&mut out, "200 OK", "content-length: 0\r\n");
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

    fn central(s3: &str, max_part_bytes: u64) -> Central {
        let dir = tempfile::tempdir().unwrap();
        let catalog = dir.path().join("catalog.sqlite");
        let config = PeerConfig {
            enabled: true,
            addr: "127.0.0.1:0".into(),
            tokens: vec!["tok".into()],
            parts: PartsConfig {
                enabled: true,
                endpoint: s3.into(),
                bucket: "logs".into(),
                prefix: "edges".into(),
                access_key_id: "AK".into(),
                secret_access_key: "SK".into(),
                max_part_bytes,
                ..PartsConfig::default()
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
        _store: tempfile::TempDir,
    }

    fn edge(url: &str, hours: &[u64]) -> Edge {
        let data = tempfile::tempdir().unwrap();
        let store = tempfile::tempdir().unwrap();
        write_files(store.path(), hours);
        let target = SyncTarget { url: url.into(), token: Some("tok".into()), min_severity: 0, fidelity: 100, anomaly_boost: false };
        let control = SyncControl::new("edge".into(), SyncStore::new(data.path()), Some(target));
        control.set_store_dir(store.path().to_path_buf());
        Edge { control, data, _store: store }
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
        assert!(keys.iter().all(|k| k.starts_with("/logs/edges/") && valid_name(&k["/logs/edges/".len()..])), "{keys:?}");
        // The stored bytes are the file: the name is their hash.
        for (k, body) in fake.objects.lock().unwrap().iter() {
            let hash: String = Sha256::digest(body).iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(&k["/logs/edges/".len()..64 + "/logs/edges/".len()], hash);
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
        let mut r = ureq::post(format!("http://{addr}{path}"))
            .header("Authorization", "Bearer tok")
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
        assert!(v["url"].as_str().unwrap().contains("/logs/edges/"), "{v}");
        let commit = serde_json::json!({"name": name, "size": 10, "sha256": HEX, "rows": 1, "min_unix_nano": 1, "max_unix_nano": 2, "origin": "edge"});
        assert_eq!(409, post(a, COMMIT_PATH, commit).0, "nothing was uploaded");
        // Without the token, nothing.
        let r = ureq::post(format!("http://{a}{PRESIGN_PATH}")).config().http_status_as_error(false).build().send("{}").unwrap();
        assert_eq!(401, r.status().as_u16());
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
}
