//! Lifetime ingest counters, persisted across restarts.
//!
//! The in-process accounting invariant (`received == enqueued + dropped`) stops
//! at the WAL, and both of the merge data-loss bugs lived past that point: the
//! queue was perfectly balanced while records were being deleted from the WAL
//! unread. Extending the invariant across the whole path needs one number the
//! process cannot hold in memory — how much has ever been accepted — because a
//! restart resets everything else.
//!
//! Written like the spool cursor: whole value, atomic rename, never appended.
//! An approximate counter is worse than none, because it would make the
//! invariant fail for reasons that are not bugs and train everyone to ignore it.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Counters {
    /// Records ever accepted from any receiver.
    pub received: u64,
    /// Records ever handed to the WAL writer.
    pub enqueued: u64,
    /// Records deliberately discarded: severity shedding, or refused at the
    /// door under critical disk pressure. Deliberate loss is still loss and is
    /// counted separately from the kind that would be a bug.
    pub dropped: u64,
}

impl Counters {
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("ingest-counters.json")
    }

    pub fn load(data_dir: &Path) -> Self {
        fs::read_to_string(Self::path(data_dir))
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default()
    }

    /// Persists by atomic rename, so a crash mid-write leaves the previous
    /// value rather than a truncated one.
    pub fn save(&self, data_dir: &Path) -> std::io::Result<()> {
        let path = Self::path(data_dir);
        let tmp = path.with_extension("tmp");
        {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(serde_json::to_string(self).unwrap_or_default().as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        if let Ok(dir) = fs::File::open(data_dir) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    pub fn add_session(&self, received: u64, enqueued: u64, dropped: u64) -> Self {
        Self {
            received: self.received + received,
            enqueued: self.enqueued + enqueued,
            dropped: self.dropped + dropped,
        }
    }
}

/// The whole-path accounting check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Audit {
    pub received: u64,
    pub committed: u64,
    pub in_wal: u64,
    pub dropped: u64,
}

impl Audit {
    /// `received == committed + in_wal + dropped`, allowing for records lost to
    /// an ungraceful kill inside the fsync window.
    ///
    /// `tolerance` is that window: the records the WAL had accepted but not yet
    /// synced when the process died. Zero after a clean shutdown.
    pub fn balances(&self, tolerance: u64) -> bool {
        let accounted = self.committed + self.in_wal + self.dropped;
        accounted <= self.received && self.received - accounted <= tolerance
    }

    pub fn unaccounted(&self) -> i64 {
        self.received as i64 - (self.committed + self.in_wal + self.dropped) as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_survive_a_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let counters = Counters { received: 10, enqueued: 9, dropped: 1 };
        counters.save(tmp.path()).unwrap();
        assert_eq!(Counters::load(tmp.path()), counters);
    }

    #[test]
    fn a_missing_or_corrupt_file_reads_as_zero_rather_than_failing() {
        // A counter file lost to a disk wipe must not stop the agent starting.
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(Counters::load(tmp.path()), Counters::default());
        fs::write(Counters::path(tmp.path()), b"not json").unwrap();
        assert_eq!(Counters::load(tmp.path()), Counters::default());
    }

    #[test]
    fn sessions_accumulate() {
        let counters = Counters::default().add_session(100, 90, 10).add_session(50, 50, 0);
        assert_eq!(counters, Counters { received: 150, enqueued: 140, dropped: 10 });
    }

    #[test]
    fn the_audit_balances_when_everything_is_accounted_for() {
        let audit = Audit { received: 100, committed: 80, in_wal: 15, dropped: 5 };
        assert!(audit.balances(0));
        assert_eq!(audit.unaccounted(), 0);
    }

    #[test]
    fn a_gap_larger_than_the_fsync_window_does_not_balance() {
        // This is the shape of the merge bug that deleted a live WAL segment:
        // the queue accounting was perfect and 20,480 records were simply gone.
        let audit = Audit { received: 200_000, committed: 179_520, in_wal: 0, dropped: 0 };
        assert!(!audit.balances(4096));
        assert_eq!(audit.unaccounted(), 20_480);
    }

    #[test]
    fn records_can_never_be_invented() {
        let audit = Audit { received: 100, committed: 120, in_wal: 0, dropped: 0 };
        assert!(!audit.balances(u64::MAX), "more committed than received is always wrong");
    }

    #[test]
    fn an_ungraceful_kill_inside_the_fsync_window_is_tolerated() {
        let audit = Audit { received: 100_000, committed: 99_000, in_wal: 0, dropped: 0 };
        assert!(!audit.balances(500), "1000 lost is more than a 500-record window");
        assert!(audit.balances(1000));
    }
}
