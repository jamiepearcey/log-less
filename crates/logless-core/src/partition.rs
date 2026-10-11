//! Partition keys and on-disk layout.
//!
//! Layout is
//! `store/tenant=<t>/index=<i>/hour=YYYY-MM-DDTHH/level=<bucket>/node=<writer>/<file>.parquet`
//! (`docs/design/multi-tenancy-and-indexing.md` section 2, ADR 0002).
//!
//! Tenant comes first so read scoping, retention and offboarding are subtree
//! operations; index second because retention is per `(tenant, index, level)`.
//! `hour` is the **event** hour (the row's `timestamp`), so replayed and
//! clock-skewed rows prune by directory. Level comes before node for exactly
//! one reason: retention stays a directory delete that takes every writer's
//! files for the partition. Node is the writer that owns the file.

use std::path::{Path, PathBuf};

pub const SECS_PER_HOUR: u64 = 3600;
pub const NANOS_PER_HOUR: u64 = SECS_PER_HOUR * 1_000_000_000;
/// The tenant and the index of an install that configures neither.
pub const DEFAULT_TENANT: &str = "default";
pub const DEFAULT_INDEX: &str = "default";

/// Whether `s` can be a tenant, index or node id: 1 to 64 of `A-Za-z0-9._-`,
/// not starting with a dot (the rule for a part's node, `parts::valid_node`),
/// **and at least one ASCII letter**. Without the letter DuckDB's hive
/// partitioning reads `tenant=1234` as BIGINT and a reader's string predicate
/// then matches nothing.
pub fn valid_ident(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && !s.starts_with('.')
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        && s.bytes().any(|b| b.is_ascii_alphabetic())
}

/// `(tenant, index, event hour, level bucket, node)` — the full partition key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionKey {
    pub tenant: String,
    pub index: String,
    /// Hours since the Unix epoch, derived from the row's `timestamp`.
    pub epoch_hour: u64,
    /// Level bucket name, from the index's `config::LevelBuckets`.
    pub bucket: String,
    /// The writer that owns the file.
    pub node: String,
}

impl PartitionKey {
    pub fn new(tenant: &str, index: &str, epoch_hour: u64, bucket: &str, node: &str) -> Self {
        Self { tenant: tenant.into(), index: index.into(), epoch_hour, bucket: bucket.into(), node: node.into() }
    }

    /// A key in the default tenant and index, written by `node-a`. For tests.
    #[cfg(test)]
    pub fn at(epoch_hour: u64, bucket: &str) -> Self {
        Self::new(DEFAULT_TENANT, DEFAULT_INDEX, epoch_hour, bucket, "node-a")
    }

    /// `tenant=…/index=…/hour=…/level=…`: every writer's files for the
    /// partition, and the directory retention deletes.
    pub fn level_dir(&self, store_dir: &Path) -> PathBuf {
        store_dir
            .join(format!("tenant={}", self.tenant))
            .join(format!("index={}", self.index))
            .join(format!("hour={}", format_hour(self.epoch_hour)))
            .join(format!("level={}", self.bucket))
    }

    /// The leaf directory this writer's files live in.
    pub fn dir(&self, store_dir: &Path) -> PathBuf {
        self.level_dir(store_dir).join(format!("node={}", self.node))
    }

    /// First second of the hour after this partition — the moment its data
    /// stops accruing and its retention clock starts.
    pub fn end_unix_secs(&self) -> u64 {
        (self.epoch_hour + 1) * SECS_PER_HOUR
    }
}

/// The key of a file from its location: the five `key=value` directories that
/// end the path's parent, in order. The tree is the authority and the catalog
/// is derived from it, so every reader of a path goes through here.
pub fn key_of_path(path: &Path) -> Option<PartitionKey> {
    let mut dirs: Vec<&str> = path.parent()?.components().rev().take(5).filter_map(|c| c.as_os_str().to_str()).collect();
    dirs.reverse();
    let [tenant, index, hour, level, node] = dirs.as_slice() else { return None };
    let tenant = tenant.strip_prefix("tenant=").filter(|v| valid_ident(v))?;
    let index = index.strip_prefix("index=").filter(|v| valid_ident(v))?;
    let node = node.strip_prefix("node=").filter(|v| valid_ident(v))?;
    let epoch_hour = parse_hour(hour.strip_prefix("hour=")?)?;
    let bucket = level.strip_prefix("level=").filter(|v| !v.is_empty())?;
    Some(PartitionKey::new(tenant, index, epoch_hour, bucket, node))
}

/// True when `store_dir` holds the layout before tenants and indexes
/// (`hour=…/level=…` at the top). Such a store is refused at startup: its
/// partitions are cut on `observed` and nothing here would read them right.
pub fn is_old_layout(store_dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(store_dir) else { return false };
    entries.flatten().any(|e| {
        e.file_type().map(|t| t.is_dir()).unwrap_or(false)
            && e.file_name().to_str().and_then(|n| n.strip_prefix("hour=")).and_then(parse_hour).is_some()
    })
}

/// `2026-08-04T14` — sortable, hive-compatible, readable by a human browsing
/// the directory. Deliberately not RFC3339: no separators a shell will fight.
pub fn format_hour(epoch_hour: u64) -> String {
    let (y, m, d) = civil_from_days((epoch_hour / 24) as i64);
    let h = epoch_hour % 24;
    format!("{y:04}-{m:02}-{d:02}T{h:02}")
}

pub fn parse_hour(s: &str) -> Option<u64> {
    // YYYY-MM-DDTHH
    let (date, hour) = s.split_once('T')?;
    let mut parts = date.split('-');
    let y: i64 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let h: u64 = hour.parse().ok()?;
    if h > 23 {
        return None;
    }
    let days = days_from_civil(y, m, d);
    if days < 0 {
        return None;
    }
    Some(days as u64 * 24 + h)
}

// Howard Hinnant's civil-date algorithms. Chosen over a date crate because this
// is the only date arithmetic in the agent and it must never disagree with the
// directory names already on disk.

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
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hour_formatting_round_trips() {
        for h in [0u64, 1, 24, 1_000, 486_000, 500_000, 1_000_000] {
            let s = format_hour(h);
            assert_eq!(parse_hour(&s), Some(h), "round trip failed for {s}");
        }
    }

    #[test]
    fn known_dates() {
        assert_eq!(format_hour(0), "1970-01-01T00");
        assert_eq!(format_hour(23), "1970-01-01T23");
        assert_eq!(format_hour(24), "1970-01-02T00");
        // 2000-02-29, a leap day in a century year — the case naive algorithms
        // get wrong.
        assert_eq!(parse_hour("2000-02-29T12").map(format_hour).as_deref(), Some("2000-02-29T12"));
    }

    #[test]
    fn partition_paths_are_tenant_index_hour_level_node() {
        let key = PartitionKey::new("acme", "payments", 0, "debug", "edge-1");
        let p = key.dir(Path::new("/data/store"));
        assert_eq!(p, Path::new("/data/store/tenant=acme/index=payments/hour=1970-01-01T00/level=debug/node=edge-1"));
        assert_eq!(key.level_dir(Path::new("/s")), Path::new("/s/tenant=acme/index=payments/hour=1970-01-01T00/level=debug"));
    }

    #[test]
    fn a_file_path_parses_back_to_its_key() {
        let key = PartitionKey::new("acme", "default", 486_123, "warn_plus", "edge-1");
        let file = key.dir(Path::new("/data/store")).join("a.parquet");
        assert_eq!(key_of_path(&file), Some(key));
        for bad in [
            "/s/hour=1970-01-01T00/level=a/x.parquet",
            "/s/tenant=a/index=b/hour=1970-01-01T00/level=c/x.parquet",
            "/s/tenant=a/index=b/hour=1970-13-01T00/level=c/node=n/x.parquet",
            "/s/tenant=a/index=b/hour=1970-01-01T00/level=/node=n/x.parquet",
            "/s/tenant=123/index=b/hour=1970-01-01T00/level=c/node=n/x.parquet",
            "/s/index=b/tenant=a/hour=1970-01-01T00/level=c/node=n/x.parquet",
        ] {
            assert_eq!(key_of_path(Path::new(bad)), None, "{bad}");
        }
    }

    #[test]
    fn identifiers_need_a_letter_and_a_small_alphabet() {
        for ok in ["default", "acme", "edge-1", "a.b_c-9", "x", "9a"] {
            assert!(valid_ident(ok), "{ok}");
        }
        let long = "a".repeat(65);
        for bad in ["", "1234", "2026", ".hidden", "a/b", "a b", "a=b", "é", long.as_str(), "..", "-_-"] {
            assert!(!valid_ident(bad), "{bad}");
        }
    }

    #[test]
    fn an_old_layout_store_is_recognised_and_a_new_or_empty_one_is_not() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!is_old_layout(&tmp.path().join("absent")));
        std::fs::create_dir_all(tmp.path().join("_meta")).unwrap();
        std::fs::create_dir_all(tmp.path().join("tenant=default/index=default/hour=2026-01-01T00")).unwrap();
        assert!(!is_old_layout(tmp.path()));
        std::fs::create_dir_all(tmp.path().join("hour=2026-01-01T00/level=info")).unwrap();
        assert!(is_old_layout(tmp.path()));
    }

    #[test]
    fn end_of_partition_is_next_hour() {
        let key = PartitionKey::at(0, "debug");
        assert_eq!(key.end_unix_secs(), 3600);
    }
}
