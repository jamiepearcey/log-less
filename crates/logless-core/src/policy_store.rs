//! Persistence for runtime policy: `policy.json` (current state) and
//! `policy-audit.jsonl` (append-only history).
//!
//! `policy.json` is replaced whole by temp file, fsync, rename, then a
//! directory fsync, so a crash leaves the previous version or the new one and
//! never a torn file. The audit log is appended after it; a crash between the
//! two loses only the audit line, and [`reconcile_audit`] writes it back at the
//! next start from the document that did land.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::policy::{self, DiffEntry, Policies};

pub const POLICY_SCHEMA_VERSION: u32 = 1;
pub const POLICY_FILE: &str = "policy.json";
pub const AUDIT_FILE: &str = "policy-audit.jsonl";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },
    #[error("{path} is not valid policy JSON ({message}); move it aside to fall back to the config file")]
    Corrupt { path: PathBuf, message: String },
    #[error("{path} has schema_version {found}, this build understands {POLICY_SCHEMA_VERSION}")]
    Schema { path: PathBuf, found: u32 },
    #[error("runtime policy in {path} does not validate against the current config file: {problem}")]
    Invalid { path: PathBuf, problem: String },
}

/// `policy.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyFile {
    pub schema_version: u32,
    /// Monotonic; 0 is "no runtime policy yet" and is never written.
    pub version: u64,
    pub updated_at_ms: u64,
    pub updated_by: String,
    /// Keys of the leaves (or `"retention"`) that differ from the config file.
    /// Only these override it: everything else keeps following the file.
    pub overrides: Vec<String>,
    /// The full effective document at this version, so the file is readable on
    /// its own and restore does not need the config file of the day.
    pub document: Policies,
}

/// One line of `policy-audit.jsonl`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub ts_ms: u64,
    pub actor: String,
    pub version: u64,
    pub diff: Vec<DiffEntry>,
    pub impact_summary: Value,
    /// Snapshot applied by this version: what `restore` re-applies.
    pub policies: Policies,
}

pub fn policy_path(data_dir: &Path) -> PathBuf {
    data_dir.join(POLICY_FILE)
}

pub fn audit_path(data_dir: &Path) -> PathBuf {
    data_dir.join(AUDIT_FILE)
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> StoreError + '_ {
    move |source| StoreError::Io { path: path.to_path_buf(), source }
}

pub fn read_policy_file(data_dir: &Path) -> Result<Option<PolicyFile>, StoreError> {
    let path = policy_path(data_dir);
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io(&path)(e)),
    };
    let file: PolicyFile = serde_json::from_str(&text)
        .map_err(|e| StoreError::Corrupt { path: path.clone(), message: e.to_string() })?;
    if file.schema_version != POLICY_SCHEMA_VERSION {
        return Err(StoreError::Schema { path, found: file.schema_version });
    }
    if file.version == u64::MAX {
        return Err(StoreError::Corrupt {
            path,
            message: "version is at its maximum, so no further version can be written".into(),
        });
    }
    Ok(Some(file))
}

pub fn write_policy_file(data_dir: &Path, file: &PolicyFile) -> Result<(), StoreError> {
    let path = policy_path(data_dir);
    let tmp = data_dir.join(format!("{POLICY_FILE}.tmp"));
    let body = serde_json::to_vec_pretty(file).expect("policy file serialises");
    {
        let mut f = File::create(&tmp).map_err(io(&tmp))?;
        f.write_all(&body).map_err(io(&tmp))?;
        f.sync_all().map_err(io(&tmp))?;
    }
    fs::rename(&tmp, &path).map_err(io(&path))?;
    if let Ok(dir) = File::open(data_dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

pub fn append_audit(data_dir: &Path, entry: &AuditEntry) -> Result<(), StoreError> {
    let path = audit_path(data_dir);
    let mut line = serde_json::to_vec(entry).expect("audit entry serialises");
    line.push(b'\n');
    let mut f = OpenOptions::new().create(true).append(true).open(&path).map_err(io(&path))?;
    // One write call, so concurrent appenders cannot interleave inside a line.
    f.write_all(&line).map_err(io(&path))?;
    f.sync_all().map_err(io(&path))?;
    if f.metadata().map(|m| m.len()).unwrap_or(0) > AUDIT_COMPACT_BYTES {
        compact_audit(data_dir)?;
    }
    Ok(())
}

/// Past this size the audit log is rewritten to its newest [`AUDIT_KEEP`]
/// entries, so history and restore stay O(constant) however long the process
/// has been tuned. Older versions stop being restorable (`unknown_version`).
const AUDIT_COMPACT_BYTES: u64 = 8 * 1024 * 1024;
const AUDIT_KEEP: usize = 500;

fn compact_audit(data_dir: &Path) -> Result<(), StoreError> {
    let path = audit_path(data_dir);
    let entries = read_audit(data_dir)?;
    let from = entries.len().saturating_sub(AUDIT_KEEP);
    let tmp = data_dir.join(format!("{AUDIT_FILE}.tmp"));
    {
        let mut f = File::create(&tmp).map_err(io(&tmp))?;
        for e in &entries[from..] {
            let mut line = serde_json::to_vec(e).expect("audit entry serialises");
            line.push(b'\n');
            f.write_all(&line).map_err(io(&tmp))?;
        }
        f.sync_all().map_err(io(&tmp))?;
    }
    fs::rename(&tmp, &path).map_err(io(&path))?;
    Ok(())
}

/// Entries oldest first. A line that does not parse (a torn final write) is
/// skipped rather than failing the whole history.
pub fn read_audit(data_dir: &Path) -> Result<Vec<AuditEntry>, StoreError> {
    let path = audit_path(data_dir);
    let f = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io(&path)(e)),
    };
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line.map_err(io(&path))?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<AuditEntry>(&line) {
            Ok(e) => out.push(e),
            Err(e) => tracing::warn!(error = %e, "skipping unreadable policy audit line"),
        }
    }
    Ok(out)
}

/// Re-creates the audit line for a `policy.json` version that landed without
/// one (crash between the two writes). The diff is unknown by then, which the
/// summary says.
pub fn reconcile_audit(data_dir: &Path, file: &PolicyFile) -> Result<bool, StoreError> {
    if read_audit(data_dir)?.iter().any(|e| e.version == file.version) {
        return Ok(false);
    }
    append_audit(
        data_dir,
        &AuditEntry {
            ts_ms: file.updated_at_ms,
            actor: file.updated_by.clone(),
            version: file.version,
            diff: Vec::new(),
            impact_summary: serde_json::json!({"recovered": true, "note": "audit line was lost to a crash; diff unknown"}),
            policies: file.document.clone(),
        },
    )?;
    Ok(true)
}

/// Effective policy at startup: file defaults overlaid by whatever
/// `policy.json` overrides. Returns the file's metadata too, so the caller's
/// version counter continues where the last process stopped.
pub fn load_effective(
    data_dir: &Path,
    file_defaults: &Policies,
) -> Result<(Policies, Option<PolicyFile>), StoreError> {
    let Some(file) = read_policy_file(data_dir)? else {
        return Ok((file_defaults.clone(), None));
    };
    let invalid = |problem: String| StoreError::Invalid { path: policy_path(data_dir), problem };
    let effective =
        policy::overlay(file_defaults, &file.document, &file.overrides).map_err(invalid)?;
    if let Some(p) = effective.validate().into_iter().next() {
        return Err(invalid(format!("{}: {}", p.path, p.message)));
    }
    reconcile_audit(data_dir, &file)?;
    Ok((effective, Some(file)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::test_policies;

    fn file(version: u64, doc: Policies, overrides: Vec<String>) -> PolicyFile {
        PolicyFile {
            schema_version: POLICY_SCHEMA_VERSION,
            version,
            updated_at_ms: 1000 + version,
            updated_by: "tester".into(),
            overrides,
            document: doc,
        }
    }

    #[test]
    fn absent_file_means_file_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let (eff, meta) = load_effective(tmp.path(), &test_policies()).unwrap();
        assert_eq!(eff, test_policies());
        assert!(meta.is_none());
    }

    #[test]
    fn write_is_atomic_and_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let mut doc = test_policies();
        doc.storage.disk_budget_bytes = 42;
        let f = file(3, doc, vec!["storage.disk_budget_bytes".into()]);
        write_policy_file(tmp.path(), &f).unwrap();
        assert_eq!(read_policy_file(tmp.path()).unwrap(), Some(f));
        assert!(!tmp.path().join("policy.json.tmp").exists(), "temp file is renamed away");
    }

    #[test]
    fn a_crash_mid_write_leaves_the_previous_version() {
        // The failure the rename protects against: the process dies after the
        // temp file is partly written and before the rename.
        let tmp = tempfile::tempdir().unwrap();
        let old = file(1, test_policies(), vec![]);
        write_policy_file(tmp.path(), &old).unwrap();
        fs::write(tmp.path().join("policy.json.tmp"), b"{\"schema_version\":1,\"vers").unwrap();

        assert_eq!(read_policy_file(tmp.path()).unwrap(), Some(old.clone()));
        // The next write simply replaces the stale temp file.
        let newer = file(2, test_policies(), vec![]);
        write_policy_file(tmp.path(), &newer).unwrap();
        assert_eq!(read_policy_file(tmp.path()).unwrap(), Some(newer));
    }

    #[test]
    fn a_corrupt_policy_file_refuses_to_load_instead_of_being_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(policy_path(tmp.path()), b"{not json").unwrap();
        let err = load_effective(tmp.path(), &test_policies()).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt { .. }), "{err}");
        assert!(err.to_string().contains("move it aside"));
    }

    #[test]
    fn unknown_schema_version_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let mut f = file(1, test_policies(), vec![]);
        f.schema_version = 9;
        fs::write(policy_path(tmp.path()), serde_json::to_vec(&f).unwrap()).unwrap();
        assert!(matches!(
            read_policy_file(tmp.path()).unwrap_err(),
            StoreError::Schema { found: 9, .. }
        ));
    }

    #[test]
    fn runtime_overrides_the_file_field_by_field() {
        let tmp = tempfile::tempdir().unwrap();
        let defaults = test_policies();
        let mut doc = defaults.clone();
        doc.pushdown.enabled = false;
        write_policy_file(
            tmp.path(),
            &file(1, doc, vec!["pushdown.enabled".into()]),
        )
        .unwrap();

        // The file changes between runs: untouched fields follow it.
        let mut changed = defaults.clone();
        changed.storage.disk_budget_bytes = 777;
        changed.pushdown.enabled = true;
        let (eff, meta) = load_effective(tmp.path(), &changed).unwrap();
        assert_eq!(eff.storage.disk_budget_bytes, 777);
        assert!(!eff.pushdown.enabled, "runtime override beats the file");
        assert_eq!(meta.unwrap().version, 1);
    }

    #[test]
    fn a_file_edit_that_conflicts_with_an_override_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        let defaults = test_policies();
        let mut doc = defaults.clone();
        doc.storage.high_water = 0.9;
        write_policy_file(tmp.path(), &file(1, doc, vec!["storage.high_water".into()])).unwrap();
        let mut changed = defaults;
        changed.storage.critical_water = 0.85; // now below the override
        let err = load_effective(tmp.path(), &changed).unwrap_err();
        assert!(matches!(err, StoreError::Invalid { .. }), "{err}");
        assert!(err.to_string().contains("storage.high_water"));
    }

    #[test]
    fn audit_appends_in_order_and_skips_a_torn_line() {
        let tmp = tempfile::tempdir().unwrap();
        for v in 1..=3 {
            append_audit(
                tmp.path(),
                &AuditEntry {
                    ts_ms: v,
                    actor: format!("a{v}"),
                    version: v,
                    diff: vec![],
                    impact_summary: serde_json::json!({}),
                    policies: test_policies(),
                },
            )
            .unwrap();
        }
        let mut f = OpenOptions::new().append(true).open(audit_path(tmp.path())).unwrap();
        f.write_all(b"{\"ts_ms\":9,\"act").unwrap();
        let got = read_audit(tmp.path()).unwrap();
        assert_eq!(got.iter().map(|e| e.version).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(got[2].actor, "a3");
    }

    #[test]
    fn reconcile_restores_an_audit_line_lost_to_a_crash() {
        let tmp = tempfile::tempdir().unwrap();
        let f = file(4, test_policies(), vec![]);
        write_policy_file(tmp.path(), &f).unwrap();
        assert!(reconcile_audit(tmp.path(), &f).unwrap());
        assert!(!reconcile_audit(tmp.path(), &f).unwrap(), "idempotent");
        let a = read_audit(tmp.path()).unwrap();
        assert_eq!((a.len(), a[0].version, a[0].impact_summary["recovered"].clone()), (1, 4, Value::Bool(true)));
    }

    #[test]
    fn a_tampered_version_at_the_maximum_is_refused_at_load() {
        let tmp = tempfile::tempdir().unwrap();
        let mut f = file(1, test_policies(), vec![]);
        f.version = u64::MAX;
        write_policy_file(tmp.path(), &f).unwrap();
        assert!(matches!(read_policy_file(tmp.path()), Err(StoreError::Corrupt { .. })));
    }

    #[test]
    fn the_audit_log_compacts_to_the_newest_entries() {
        let tmp = tempfile::tempdir().unwrap();
        for v in 1..=(AUDIT_KEEP as u64 + 100) {
            append_audit(tmp.path(), &AuditEntry {
                ts_ms: v, actor: "a".into(), version: v, diff: vec![], impact_summary: serde_json::json!({}), policies: test_policies(),
            }).unwrap();
        }
        compact_audit(tmp.path()).unwrap();
        let kept = read_audit(tmp.path()).unwrap();
        assert_eq!(kept.len(), AUDIT_KEEP);
        assert_eq!(kept.last().unwrap().version, AUDIT_KEEP as u64 + 100);
        assert_eq!(kept[0].version, 101);
    }
}
