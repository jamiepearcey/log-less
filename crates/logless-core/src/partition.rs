//! Partition keys and on-disk layout.
//!
//! Layout is `store/hour=YYYY-MM-DDTHH/level=<bucket>/<file>.parquet`.
//!
//! Hour comes **first** so time pruning works and so external engines (DuckDB,
//! Polars, Grafana) get a usable hive-partitioned directory. Level comes second
//! for exactly one reason: retention becomes a directory delete.

use std::path::{Path, PathBuf};

pub const SECS_PER_HOUR: u64 = 3600;

/// `(hour, level bucket)` — the full partition key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionKey {
    /// Hours since the Unix epoch, derived from `observed_unix_nano`.
    pub epoch_hour: u64,
    /// Level bucket name, from `config::LevelBuckets`.
    pub bucket: String,
}

impl PartitionKey {
    pub fn from_nanos(observed_unix_nano: u64, bucket: impl Into<String>) -> Self {
        Self {
            epoch_hour: observed_unix_nano / 1_000_000_000 / SECS_PER_HOUR,
            bucket: bucket.into(),
        }
    }

    pub fn dir(&self, store_dir: &Path) -> PathBuf {
        store_dir
            .join(format!("hour={}", format_hour(self.epoch_hour)))
            .join(format!("level={}", self.bucket))
    }

    /// First second of the hour after this partition — the moment its data
    /// stops accruing and its retention clock starts.
    pub fn end_unix_secs(&self) -> u64 {
        (self.epoch_hour + 1) * SECS_PER_HOUR
    }
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

/// Parse `hour=YYYY-MM-DDTHH` / `level=<bucket>` directory names.
pub fn parse_partition_dirs(hour_dir: &str, level_dir: &str) -> Option<PartitionKey> {
    let hour = parse_hour(hour_dir.strip_prefix("hour=")?)?;
    let bucket = level_dir.strip_prefix("level=")?;
    if bucket.is_empty() {
        return None;
    }
    Some(PartitionKey {
        epoch_hour: hour,
        bucket: bucket.to_string(),
    })
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
    fn partition_paths_are_hour_first() {
        let key = PartitionKey::from_nanos(0, "debug");
        let p = key.dir(Path::new("/data/store"));
        assert_eq!(p, Path::new("/data/store/hour=1970-01-01T00/level=debug"));
    }

    #[test]
    fn partition_dirs_parse_back() {
        let key = PartitionKey {
            epoch_hour: 486_123,
            bucket: "warn_plus".into(),
        };
        let hour_dir = format!("hour={}", format_hour(key.epoch_hour));
        assert_eq!(
            parse_partition_dirs(&hour_dir, "level=warn_plus"),
            Some(key)
        );
        assert_eq!(parse_partition_dirs("nothour=x", "level=a"), None);
        assert_eq!(parse_partition_dirs("hour=1970-01-01T00", "level="), None);
        assert_eq!(parse_partition_dirs("hour=1970-13-01T00", "level=a"), None);
    }

    #[test]
    fn end_of_partition_is_next_hour() {
        let key = PartitionKey::from_nanos(0, "debug");
        assert_eq!(key.end_unix_secs(), 3600);
    }
}
