//! Verified restore: rebuild an edge store from its uploaded parts alone.
//!
//! Central keeps an edge's parts on its own disk (`<parts dir>/<node>/`, read here with [`DirSource`]) or
//! in an S3-compatible bucket ([`S3Source`]), whichever `[peer.parts]` names (`crate::parts`). Either
//! holds Parquet parts and nothing else: files named `<sha256 hex>.parquet`. This module lists them, fetches each one, checks it, and only then installs it under
//! `<to>/store/hour=…/level=…/<name>.parquet`. A part that fails a check is never installed, and is reported
//! by name with the reason. Then the catalog and the template dictionary are rebuilt from what was installed.
//!
//! Checks per part, in this order:
//! 1. SHA-256 of the bytes equals the name. This is the integrity check and needs no inventory.
//! 2. The Parquet footer is readable and every row group decodes, and the decoded row count equals the footer's.
//! 3. When an inventory is given (central's `peer_parts` table), size and row count equal the inventory's.
//!
//! Missing parts can only be found against an inventory: a bucket cannot say what it used to hold.
//!
//! What a part does not carry is template *text*: rows hold `template_id` only, and the text lives in the
//! catalog, which is not in the bucket. The restore keeps every id (so rows still join), and gives each id an
//! exemplar text, the masked body of its first restored row, with no mined wildcards. Counts, services and
//! first/last seen are exact. See `docs/restore.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};

use crate::catalog::{Catalog, TemplateSighting};
use crate::config::LevelBuckets;
use crate::drain::Template;
use crate::model::Severity;
use crate::parts::{self, valid_name};
use crate::partition::PartitionKey;
use crate::s3_sigv4::{self, Credentials};

pub const TMP_DIR: &str = ".restore-tmp";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    NotFound,
    Other(String),
}

/// Where parts come from.
pub trait PartSource {
    /// Part names present (`<64 hex>.parquet` only), sorted and de-duplicated, and how many other keys were ignored.
    fn list(&self) -> Result<(Vec<String>, u64), String>;
    /// Copies the object to `dest`.
    fn fetch(&self, name: &str, dest: &Path) -> Result<(), FetchError>;
}

/// A directory of `<name>.parquet` files: one node's directory under central's local parts directory, or a
/// bucket mirrored to disk.
pub struct DirSource(pub PathBuf);

impl PartSource for DirSource {
    fn list(&self) -> Result<(Vec<String>, u64), String> {
        let mut names = BTreeSet::new();
        let mut ignored = 0;
        for entry in fs::read_dir(&self.0).map_err(|e| format!("{}: {e}", self.0.display()))?.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if entry.file_type().map(|t| t.is_file()).unwrap_or(false) && valid_name(&name) {
                names.insert(name);
            } else {
                ignored += 1;
            }
        }
        Ok((names.into_iter().collect(), ignored))
    }

    fn fetch(&self, name: &str, dest: &Path) -> Result<(), FetchError> {
        match fs::copy(self.0.join(name), dest) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(FetchError::NotFound),
            Err(e) => Err(FetchError::Other(e.to_string())),
        }
    }
}

/// S3-compatible storage, path-style, read with presigned GETs.
pub struct S3Source {
    scheme: String,
    host: String,
    bucket: String,
    prefix: String,
    region: String,
    creds: Credentials,
    agent: ureq::Agent,
}

impl S3Source {
    pub fn new(endpoint: &str, bucket: &str, prefix: &str, region: &str, access_key: &str, secret_key: &str) -> Result<Self, String> {
        let (scheme, host) = parts::split_endpoint(endpoint)?;
        if bucket.is_empty() {
            return Err("the bucket name is empty".into());
        }
        let agent = ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(900))).http_status_as_error(false).build().into();
        Ok(Self {
            scheme: scheme.into(),
            host: host.into(),
            bucket: bucket.into(),
            prefix: prefix.trim_matches('/').into(),
            region: region.into(),
            creds: Credentials { access_key: access_key.into(), secret_key: secret_key.into() },
            agent,
        })
    }

    fn url(&self, path: &str, query: &[(&str, &str)]) -> String {
        let now = crate::now_unix_secs();
        let pq = s3_sigv4::presign_with_query(&self.creds, "GET", &self.host, path, &self.region, now, 900, &[], query);
        format!("{}://{}{pq}", self.scheme, self.host)
    }

    fn bucket_path(&self) -> String {
        format!("/{}", s3_sigv4::encode_segment(&self.bucket))
    }

    fn key_prefix(&self) -> String {
        if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        }
    }
}

/// Text of every `<tag>…</tag>` in an S3 XML answer, in order. Keys here are hex names, so no unescaping is needed.
fn xml_values<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(&after[..end]);
        rest = &after[end + close.len()..];
    }
    out
}

impl PartSource for S3Source {
    fn list(&self) -> Result<(Vec<String>, u64), String> {
        let prefix = self.key_prefix();
        let mut names = BTreeSet::new();
        let mut ignored = 0;
        let mut token: Option<String> = None;
        loop {
            let mut query: Vec<(&str, &str)> = vec![("list-type", "2"), ("prefix", &prefix)];
            if let Some(t) = &token {
                query.push(("continuation-token", t));
            }
            let url = self.url(&self.bucket_path(), &query);
            let mut response = self.agent.get(&url).call().map_err(|e| format!("object storage is unreachable: {e}"))?;
            let status = response.status().as_u16();
            let body = response.body_mut().read_to_string().map_err(|e| format!("listing unreadable: {e}"))?;
            if status != 200 {
                return Err(format!("object storage answered HTTP {status} to a listing: {}", body.chars().take(200).collect::<String>()));
            }
            for key in xml_values(&body, "Key") {
                match key.strip_prefix(prefix.as_str()) {
                    Some(name) if valid_name(name) => {
                        names.insert(name.to_string());
                    }
                    _ => ignored += 1,
                }
            }
            if xml_values(&body, "IsTruncated").first() == Some(&"true") {
                token = xml_values(&body, "NextContinuationToken").first().map(|t| t.to_string());
                if token.is_none() {
                    return Err("the listing is truncated but gave no continuation token".into());
                }
            } else {
                break;
            }
        }
        Ok((names.into_iter().collect(), ignored))
    }

    fn fetch(&self, name: &str, dest: &Path) -> Result<(), FetchError> {
        let mut path = self.bucket_path();
        for segment in self.prefix.split('/').filter(|s| !s.is_empty()) {
            path.push('/');
            path.push_str(&s3_sigv4::encode_segment(segment));
        }
        path.push('/');
        path.push_str(name);
        let mut response = self.agent.get(&self.url(&path, &[])).call().map_err(|e| FetchError::Other(format!("object storage is unreachable: {e}")))?;
        match response.status().as_u16() {
            200 => {}
            404 => return Err(FetchError::NotFound),
            other => return Err(FetchError::Other(format!("object storage answered HTTP {other} to a GET"))),
        }
        let mut file = File::create(dest).map_err(|e| FetchError::Other(e.to_string()))?;
        std::io::copy(&mut response.body_mut().as_reader(), &mut file).map_err(|e| FetchError::Other(format!("download interrupted: {e}")))?;
        file.sync_all().map_err(|e| FetchError::Other(e.to_string()))
    }
}

/// What the inventory (central's `peer_parts`) says one part is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expected {
    pub size: u64,
    pub rows: u64,
}

/// Reads `peer_parts` from a central catalog without writing to it: every node's parts, or with `node`
/// only that node's (the catalog is keyed `(node, name)`, and a restore rebuilds one node).
pub fn read_inventory(catalog: &Path, node: Option<&str>) -> Result<BTreeMap<String, Expected>, String> {
    let conn = rusqlite::Connection::open_with_flags(catalog, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("{}: {e}", catalog.display()))?;
    let mut stmt = conn.prepare("SELECT name, size, rows FROM peer_parts WHERE ?1 IS NULL OR node = ?1").map_err(|e| format!("{}: {e}", catalog.display()))?;
    let rows = stmt
        .query_map([node], |r| Ok((r.get::<_, String>(0)?, Expected { size: r.get::<_, i64>(1)? as u64, rows: r.get::<_, i64>(2)? as u64 })))
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<BTreeMap<_, _>, _>>().map_err(|e| e.to_string())
}

/// Why one part was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Problem {
    HashMismatch { actual: String },
    SizeMismatch { expected: u64, actual: u64 },
    RowsMismatch { expected: u64, footer: u64 },
    Unreadable(String),
    DecodedRowsMismatch { footer: u64, decoded: u64 },
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Problem::HashMismatch { actual } => write!(f, "sha256 of the bytes is {actual}, not the name"),
            Problem::SizeMismatch { expected, actual } => write!(f, "{actual} bytes, inventory says {expected}"),
            Problem::RowsMismatch { expected, footer } => write!(f, "footer has {footer} rows, inventory says {expected}"),
            Problem::Unreadable(why) => write!(f, "unreadable Parquet: {why}"),
            Problem::DecodedRowsMismatch { footer, decoded } => write!(f, "footer has {footer} rows but {decoded} decode"),
        }
    }
}

/// What the rows of restored parts say about templates.
#[derive(Debug, Default, Clone)]
struct Derived {
    templates: BTreeMap<u64, TemplateAcc>,
    /// `(template_id, service)` to `(first, last)` observed nanoseconds.
    sightings: BTreeMap<(u64, Option<String>), (i64, i64)>,
    /// Rows per `template_id` (`None`: no template).
    rows_by_template: BTreeMap<Option<u64>, u64>,
}

#[derive(Debug, Clone)]
struct TemplateAcc {
    count: u64,
    first_secs: u64,
    last_secs: u64,
    exemplar: Vec<String>,
}

impl Derived {
    fn merge(&mut self, other: Derived) {
        for (id, t) in other.templates {
            match self.templates.get_mut(&id) {
                Some(mine) => {
                    mine.count += t.count;
                    mine.first_secs = mine.first_secs.min(t.first_secs);
                    mine.last_secs = mine.last_secs.max(t.last_secs);
                }
                None => {
                    self.templates.insert(id, t);
                }
            }
        }
        for (k, (lo, hi)) in other.sightings {
            let e = self.sightings.entry(k).or_insert((lo, hi));
            e.0 = e.0.min(lo);
            e.1 = e.1.max(hi);
        }
        for (k, n) in other.rows_by_template {
            *self.rows_by_template.entry(k).or_default() += n;
        }
    }
}

/// A part that passed every check.
#[derive(Debug)]
pub struct Verified {
    pub rows: u64,
    pub bytes: u64,
    pub key: PartitionKey,
    /// Rows of this part fall in more than one `(hour, level)` partition; the part is placed by its first row.
    pub multi_partition: bool,
    derived: Derived,
}

pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().iter().map(|b| format!("{b:02x}")).collect())
}

/// Checks one fetched file against its name and the inventory. Reads `path` only.
pub fn verify_part(path: &Path, name: &str, expected: Option<Expected>, buckets: &LevelBuckets) -> Result<Verified, Problem> {
    let actual = sha256_file(path).map_err(|e| Problem::Unreadable(e.to_string()))?;
    if actual != name.trim_end_matches(".parquet") {
        return Err(Problem::HashMismatch { actual });
    }
    let bytes = fs::metadata(path).map_err(|e| Problem::Unreadable(e.to_string()))?.len();
    if let Some(e) = expected {
        if e.size != bytes {
            return Err(Problem::SizeMismatch { expected: e.size, actual: bytes });
        }
    }
    let file = File::open(path).map_err(|e| Problem::Unreadable(e.to_string()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| Problem::Unreadable(e.to_string()))?;
    let footer = builder.metadata().file_metadata().num_rows().max(0) as u64;
    if let Some(e) = expected {
        if e.rows != footer {
            return Err(Problem::RowsMismatch { expected: e.rows, footer });
        }
    }
    let reader = builder.build().map_err(|e| Problem::Unreadable(e.to_string()))?;
    let mut derived = Derived::default();
    let mut decoded = 0u64;
    let mut key: Option<PartitionKey> = None;
    let mut multi = false;
    let (mut tokens, mut raw) = (Vec::new(), Vec::new());
    for batch in reader {
        let batch = batch.map_err(|e| Problem::Unreadable(e.to_string()))?;
        for r in crate::schema::from_record_batch(&batch) {
            decoded += 1;
            let k = PartitionKey::from_nanos(r.observed_unix_nano, buckets.bucket_for(r.severity).name.clone());
            match &key {
                None => key = Some(k),
                Some(first) => multi |= *first != k,
            }
            let tid = r.template_id;
            *derived.rows_by_template.entry(tid).or_default() += 1;
            if let Some(id) = tid {
                let secs = r.observed_unix_nano / 1_000_000_000;
                let t = derived.templates.entry(id).or_insert_with(|| {
                    crate::mask::mask_line(&r.body, &mut tokens, &mut raw);
                    TemplateAcc { count: 0, first_secs: secs, last_secs: secs, exemplar: tokens.clone() }
                });
                t.count += 1;
                t.first_secs = t.first_secs.min(secs);
                t.last_secs = t.last_secs.max(secs);
                let nanos = r.observed_unix_nano as i64;
                let s = derived.sightings.entry((id, r.service.clone())).or_insert((nanos, nanos));
                s.0 = s.0.min(nanos);
                s.1 = s.1.max(nanos);
            }
        }
    }
    if decoded != footer {
        return Err(Problem::DecodedRowsMismatch { footer, decoded });
    }
    let key = key.unwrap_or(PartitionKey { epoch_hour: 0, bucket: buckets.bucket_for(Severity(0)).name.clone() });
    Ok(Verified { rows: footer, bytes, key, multi_partition: multi, derived })
}

#[derive(Debug, Default, Clone)]
pub struct Report {
    pub listed: u64,
    pub ignored_keys: u64,
    pub inventory: Option<u64>,
    pub restored: u64,
    pub rows: u64,
    pub bytes: u64,
    pub hash_mismatch: Vec<String>,
    pub size_mismatch: Vec<String>,
    pub rows_mismatch: Vec<String>,
    pub unreadable: Vec<String>,
    pub fetch_failed: Vec<String>,
    pub missing: Vec<String>,
    /// In the bucket, intact, but not in the inventory. Restored anyway; reported because it is unexpected.
    pub not_in_inventory: Vec<String>,
    pub multi_partition: u64,
    pub templates: u64,
    pub template_services: u64,
    pub rows_by_template: BTreeMap<Option<u64>, u64>,
    pub catalog_files: u64,
    pub catalog_rows: u64,
    pub catalog_bytes: u64,
    pub dictionary_rows: u64,
    /// Second pass over the installed files (`reverify`): files whose bytes no longer hash to their name.
    pub reverify_bad: Vec<String>,
    /// `(part, reason)` for every refused part.
    pub details: Vec<(String, String)>,
    pub elapsed: Duration,
}

impl Report {
    /// Parts in the bucket or the inventory that are not safely restored.
    pub fn damaged(&self) -> u64 {
        (self.hash_mismatch.len() + self.size_mismatch.len() + self.rows_mismatch.len() + self.unreadable.len() + self.fetch_failed.len() + self.missing.len() + self.reverify_bad.len()) as u64
    }

    pub fn ok(&self) -> bool {
        self.damaged() == 0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("{0}")]
    Refused(String),
    #[error("cannot list the source: {0}")]
    List(String),
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
    #[error("catalog: {0}")]
    Catalog(#[from] crate::catalog::CatalogError),
    #[error("template dictionary: {0}")]
    Dict(#[from] crate::template_dict::DictError),
}

pub struct Options<'a> {
    pub to: &'a Path,
    pub inventory: Option<BTreeMap<String, Expected>>,
    pub buckets: &'a LevelBuckets,
    /// Install into a non-empty target (existing store files stay; same-named parts are replaced).
    pub force: bool,
    /// After installing, hash every installed file again and compare the catalog with the report.
    pub reverify: bool,
}

/// Restores `source` into `<to>/store` and `<to>/catalog.sqlite`.
pub fn restore(source: &dyn PartSource, opts: &Options) -> Result<Report, RestoreError> {
    let started = Instant::now();
    let store_dir = opts.to.join("store");
    let catalog_path = opts.to.join("catalog.sqlite");
    if !opts.force && (catalog_path.exists() || (store_dir.exists() && !crate::store::list_parquet_files(&store_dir).is_empty())) {
        return Err(RestoreError::Refused(format!("{} already holds a store or catalog; restore into an empty directory (or pass --force)", opts.to.display())));
    }
    let (names, ignored) = source.list().map_err(RestoreError::List)?;
    fs::create_dir_all(&store_dir)?;
    let tmp_dir = opts.to.join(TMP_DIR);
    fs::create_dir_all(&tmp_dir)?;

    let mut report = Report { listed: names.len() as u64, ignored_keys: ignored, inventory: opts.inventory.as_ref().map(|i| i.len() as u64), ..Default::default() };
    let mut derived = Derived::default();
    let mut installed: Vec<(PathBuf, String)> = Vec::new();
    if let Some(inv) = &opts.inventory {
        let present: BTreeSet<&String> = names.iter().collect();
        report.missing = inv.keys().filter(|n| !present.contains(n)).cloned().collect();
        for name in &report.missing {
            report.details.push((name.clone(), "in the inventory, not in the bucket".into()));
        }
    }
    for name in &names {
        let expected = opts.inventory.as_ref().and_then(|i| i.get(name)).copied();
        if opts.inventory.is_some() && expected.is_none() {
            report.not_in_inventory.push(name.clone());
        }
        let tmp = tmp_dir.join(name);
        match source.fetch(name, &tmp) {
            Ok(()) => {}
            Err(FetchError::NotFound) => {
                report.details.push((name.clone(), "listed, then not found".into()));
                report.missing.push(name.clone());
                continue;
            }
            Err(FetchError::Other(why)) => {
                report.details.push((name.clone(), why));
                report.fetch_failed.push(name.clone());
                continue;
            }
        }
        match verify_part(&tmp, name, expected, opts.buckets) {
            Ok(v) => {
                let dir = v.key.dir(&store_dir);
                fs::create_dir_all(&dir)?;
                let dest = dir.join(name);
                fs::rename(&tmp, &dest)?;
                report.restored += 1;
                report.rows += v.rows;
                report.bytes += v.bytes;
                report.multi_partition += v.multi_partition as u64;
                derived.merge(v.derived);
                installed.push((dest, name.clone()));
            }
            Err(problem) => {
                report.details.push((name.clone(), problem.to_string()));
                match problem {
                    Problem::HashMismatch { .. } => report.hash_mismatch.push(name.clone()),
                    Problem::SizeMismatch { .. } => report.size_mismatch.push(name.clone()),
                    Problem::RowsMismatch { .. } => report.rows_mismatch.push(name.clone()),
                    Problem::Unreadable(_) | Problem::DecodedRowsMismatch { .. } => report.unreadable.push(name.clone()),
                }
                let _ = fs::remove_file(&tmp);
            }
        }
    }
    let _ = fs::remove_dir(&tmp_dir);
    report.rows_by_template = derived.rows_by_template.clone();

    let mut catalog = Catalog::open(&catalog_path)?;
    catalog.rebuild(&store_dir)?;
    let templates: Vec<Template> = derived
        .templates
        .iter()
        .map(|(id, t)| {
            let mut template = Template { id: *id, tokens: t.exemplar.clone(), count: t.count, first_seen_unix_secs: t.first_secs, last_seen_unix_secs: t.last_secs, lineage_id: *id };
            // Birth text is not in the bucket; the exemplar is the best identity available.
            template.lineage_id = template.fingerprint();
            template
        })
        .collect();
    let sightings: Vec<TemplateSighting> = derived
        .sightings
        .iter()
        .map(|((id, service), (lo, hi))| TemplateSighting { template_id: *id, service: service.clone(), first_observed_nanos: *lo, last_observed_nanos: *hi })
        .collect();
    catalog.restore_templates(&templates, &sightings)?;
    report.templates = templates.len() as u64;
    report.template_services = sightings.len() as u64;
    report.dictionary_rows = crate::template_dict::export(&store_dir, &catalog)?.rows as u64;
    let (rows, bytes) = catalog.totals()?;
    report.catalog_rows = rows;
    report.catalog_bytes = bytes;
    report.catalog_files = catalog.file_count()?;

    if opts.reverify {
        for (path, name) in &installed {
            if sha256_file(path).map(|h| format!("{h}.parquet") != *name).unwrap_or(true) {
                report.reverify_bad.push(name.clone());
            }
        }
        if report.catalog_files != report.restored || report.catalog_rows != report.rows || report.catalog_bytes != report.bytes {
            report.details.push((
                "catalog".into(),
                format!("catalog has {} files, {} rows, {} bytes; installed {}, {}, {}", report.catalog_files, report.catalog_rows, report.catalog_bytes, report.restored, report.rows, report.bytes),
            ));
            report.reverify_bad.push("catalog".into());
        }
    }
    report.elapsed = started.elapsed();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::LogRecord;

    fn record(hour: u64, i: u64, sev: Severity, template: Option<u64>, service: &str) -> LogRecord {
        let nanos = hour * 3600 * 1_000_000_000 + i;
        let mut r = LogRecord::new(nanos, sev, format!("request {i} took {}ms", i % 7));
        r.observed_unix_nano = nanos;
        r.template_id = template;
        r.service = Some(service.into());
        r
    }

    /// Writes a Parquet file and moves it to `<dir>/<sha256>.parquet`; returns (name, size, rows).
    fn make_part(dir: &Path, records: &[LogRecord]) -> (String, u64, u64) {
        let tmp = dir.join("scratch.parquet");
        let stats = crate::store::write_partition_file(&tmp, records).unwrap();
        let name = format!("{}.parquet", sha256_file(&tmp).unwrap());
        fs::rename(&tmp, dir.join(&name)).unwrap();
        (name, stats.bytes, stats.rows)
    }

    struct Fixture {
        _tmp: tempfile::TempDir,
        bucket: PathBuf,
        to: PathBuf,
        parts: Vec<(String, u64, u64)>,
    }

    fn fixture() -> Fixture {
        let tmp = tempfile::tempdir().unwrap();
        let bucket = tmp.path().join("bucket");
        fs::create_dir_all(&bucket).unwrap();
        let a: Vec<_> = (0..40).map(|i| record(500, i, Severity::INFO, Some(1 + i % 2), "api")).collect();
        let b: Vec<_> = (0..25).map(|i| record(500, 100 + i, Severity::ERROR, Some(3), "db")).collect();
        let c: Vec<_> = (0..10).map(|i| record(501, i, Severity::INFO, None, "api")).collect();
        let parts = vec![make_part(&bucket, &a), make_part(&bucket, &b), make_part(&bucket, &c)];
        let to = tmp.path().join("restored");
        Fixture { _tmp: tmp, bucket, to, parts }
    }

    fn inventory(f: &Fixture) -> BTreeMap<String, Expected> {
        f.parts.iter().map(|(n, s, r)| (n.clone(), Expected { size: *s, rows: *r })).collect()
    }

    fn run(f: &Fixture, inventory: Option<BTreeMap<String, Expected>>) -> Report {
        let buckets = LevelBuckets::default();
        restore(&DirSource(f.bucket.clone()), &Options { to: &f.to, inventory, buckets: &buckets, force: false, reverify: true }).unwrap()
    }

    #[test]
    fn clean_restore_counts_everything_and_rebuilds_catalog_and_dictionary() {
        let f = fixture();
        let r = run(&f, Some(inventory(&f)));
        assert!(r.ok(), "{r:?}");
        assert_eq!((r.listed, r.restored, r.rows), (3, 3, 75));
        assert_eq!(r.bytes, f.parts.iter().map(|p| p.1).sum::<u64>());
        assert_eq!((r.catalog_files, r.catalog_rows), (3, 75));
        assert_eq!(r.rows_by_template, BTreeMap::from([(Some(1), 20), (Some(2), 20), (Some(3), 25), (None, 10)]));
        assert_eq!(r.templates, 3);
        // Templates 1 and 2 appear only in "api"; 3 only in "db".
        assert_eq!(r.template_services, 3);
        assert_eq!(r.dictionary_rows, 3);
        assert!(crate::template_dict::dict_path(&f.to.join("store")).exists());
        // Placed by hour and level bucket: info and warn_plus are distinct directories.
        let files = crate::store::list_parquet_files(&f.to.join("store"));
        assert_eq!(files.len(), 3);
        assert!(files.iter().any(|p| p.to_string_lossy().contains("level=warn_plus")));
        let cat = Catalog::open(&f.to.join("catalog.sqlite")).unwrap();
        let t = cat.load_templates().unwrap();
        assert_eq!(t.iter().map(|t| (t.id, t.count)).collect::<Vec<_>>(), vec![(1, 20), (2, 20), (3, 25)]);
        assert!(!f.to.join(TMP_DIR).exists());
    }

    #[test]
    fn a_flipped_byte_is_reported_by_name_and_not_installed() {
        let f = fixture();
        let victim = f.bucket.join(&f.parts[1].0);
        let mut bytes = fs::read(&victim).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        fs::write(&victim, bytes).unwrap();
        let r = run(&f, Some(inventory(&f)));
        assert!(!r.ok());
        assert_eq!(r.hash_mismatch, vec![f.parts[1].0.clone()]);
        assert_eq!((r.restored, r.rows, r.damaged()), (2, 50, 1));
        assert_eq!(crate::store::list_parquet_files(&f.to.join("store")).len(), 2);
        assert_eq!(r.rows_by_template.get(&Some(3)), None);
    }

    #[test]
    fn a_part_in_the_inventory_but_not_in_the_bucket_is_missing() {
        let f = fixture();
        fs::remove_file(f.bucket.join(&f.parts[0].0)).unwrap();
        let r = run(&f, Some(inventory(&f)));
        assert_eq!(r.missing, vec![f.parts[0].0.clone()]);
        assert_eq!((r.listed, r.restored, r.damaged()), (2, 2, 1));
        // Without an inventory the same loss is invisible: that is stated, not hidden.
        let g = fixture();
        fs::remove_file(g.bucket.join(&g.parts[0].0)).unwrap();
        assert!(run(&g, None).ok());
    }

    #[test]
    fn a_file_whose_name_is_its_own_hash_but_is_not_parquet_is_unreadable() {
        let f = fixture();
        let p = f.bucket.join("j");
        fs::write(&p, b"not a parquet file at all").unwrap();
        let name = format!("{}.parquet", sha256_file(&p).unwrap());
        fs::rename(&p, f.bucket.join(&name)).unwrap();
        let r = run(&f, None);
        assert_eq!(r.unreadable, vec![name]);
        assert_eq!(r.restored, 3);
    }

    #[test]
    fn inventory_size_and_row_disagreements_are_caught() {
        let f = fixture();
        let mut inv = inventory(&f);
        inv.get_mut(&f.parts[0].0).unwrap().rows += 1;
        inv.get_mut(&f.parts[1].0).unwrap().size += 1;
        let r = run(&f, Some(inv));
        assert_eq!(r.rows_mismatch, vec![f.parts[0].0.clone()]);
        assert_eq!(r.size_mismatch, vec![f.parts[1].0.clone()]);
        assert_eq!(r.restored, 1);
    }

    #[test]
    fn other_keys_are_ignored_and_unlisted_intact_parts_are_restored_but_reported() {
        let f = fixture();
        fs::write(f.bucket.join("README.txt"), "x").unwrap();
        let mut inv = inventory(&f);
        inv.remove(&f.parts[2].0);
        let r = run(&f, Some(inv));
        assert_eq!(r.ignored_keys, 1);
        assert_eq!(r.not_in_inventory, vec![f.parts[2].0.clone()]);
        assert_eq!(r.restored, 3);
        assert!(r.ok());
    }

    #[test]
    fn centrals_parts_directory_and_catalog_restore_one_node_and_only_that_node() {
        // Central's local backend: `<parts>/<node>/<name>`, and one catalog for every node.
        let f = fixture();
        let parts_dir = f._tmp.path().join("parts");
        fs::create_dir_all(parts_dir.join("edge-b")).unwrap();
        fs::rename(&f.bucket, parts_dir.join("edge-a")).unwrap();
        let other: Vec<_> = (0..5).map(|i| record(600, i, Severity::INFO, Some(9), "b")).collect();
        let (b_name, b_size, b_rows) = make_part(&parts_dir.join("edge-b"), &other);
        // An upload cut off by a crash is not a part.
        fs::write(parts_dir.join("edge-a/.upload-0198.tmp"), b"half").unwrap();
        let catalog = f._tmp.path().join("central.sqlite");
        {
            let c = Catalog::open(&catalog).unwrap();
            let commit = |node: &str, name: &str, size: u64, rows: u64| {
                let part = parts::CommittedPart { name: name.into(), size, sha256: name[..64].into(), rows, min_unix_nano: 1, max_unix_nano: 2, origin: "o".into(), node: None };
                assert!(c.record_peer_part(node, &part, 1).unwrap());
            };
            for (name, size, rows) in &f.parts {
                commit("edge-a", name, *size, *rows);
            }
            commit("edge-b", &b_name, b_size, b_rows);
        }
        assert_eq!(4, read_inventory(&catalog, None).unwrap().len());
        let inventory = read_inventory(&catalog, Some("edge-a")).unwrap();
        assert_eq!(inventory, self::inventory(&f));
        let buckets = LevelBuckets::default();
        let run = |to: &Path, inventory| restore(&DirSource(parts_dir.join("edge-a")), &Options { to, inventory: Some(inventory), buckets: &buckets, force: false, reverify: true }).unwrap();
        let r = run(&f.to, inventory.clone());
        assert!(r.ok(), "{r:?}");
        assert_eq!((3, 3, 75, 1, 0), (r.listed, r.restored, r.rows, r.ignored_keys, r.missing.len()));
        // The whole catalog as the inventory would call the other node's part missing.
        let whole = run(&f._tmp.path().join("restored-whole"), read_inventory(&catalog, None).unwrap());
        assert_eq!(vec![b_name], whole.missing);
        // A part lost from central's disk is named.
        fs::remove_file(parts_dir.join("edge-a").join(&f.parts[2].0)).unwrap();
        let lost = run(&f._tmp.path().join("restored-lost"), inventory);
        assert_eq!((vec![f.parts[2].0.clone()], 2), (lost.missing, lost.restored));
    }

    #[test]
    fn refuses_a_target_that_already_holds_a_store() {
        let f = fixture();
        run(&f, None);
        let buckets = LevelBuckets::default();
        let err = restore(&DirSource(f.bucket.clone()), &Options { to: &f.to, inventory: None, buckets: &buckets, force: false, reverify: false });
        assert!(matches!(err, Err(RestoreError::Refused(_))));
    }

    #[test]
    fn listing_xml_is_read_with_pagination_markers() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>edges/a.parquet</Key></Contents><Contents><Key>edges/b</Key></Contents><NextContinuationToken>tok</NextContinuationToken></ListBucketResult>";
        assert_eq!(xml_values(xml, "Key"), vec!["edges/a.parquet", "edges/b"]);
        assert_eq!(xml_values(xml, "NextContinuationToken"), vec!["tok"]);
        assert_eq!(xml_values(xml, "IsTruncated"), vec!["true"]);
    }

    #[test]
    fn presign_with_query_signs_the_extra_parameters() {
        let creds = Credentials { access_key: "AK".into(), secret_key: "SK".into() };
        let pq = s3_sigv4::presign_with_query(&creds, "GET", "h:9000", "/b", "us-east-1", 1_369_353_600, 900, &[], &[("list-type", "2"), ("prefix", "edges/")]);
        assert!(pq.contains("list-type=2") && pq.contains("prefix=edges%2F") && pq.contains("X-Amz-Signature="), "{pq}");
    }
}
