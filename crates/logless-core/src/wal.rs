//! Write-ahead log.
//!
//! Append-only segments of CRC-checked frames. No LSM, no KV store: the access
//! pattern is sequential append and sequential replay, so an index would be
//! pure overhead (`docs/architecture.md` §1).
//!
//! ```text
//! segment := header frame*
//! header  := b"LGLSWAL\x02" uuid:16               (24 bytes)
//! frame   := len:u32le crc32:u32le payload[len]   (payload = postcard Vec<LogRecord>)
//! ```
//!
//! The header carries a **per-segment UUID**. Segment file names restart at 1
//! whenever the WAL drains completely, so the numeric id is not a stable
//! identity: after a restart, a brand-new segment 1 would collide with the
//! record of a long-since-merged segment 1 and be discarded unread. The UUID is
//! what the catalog keys on, and what makes the output file name unique.
//!
//! Durability is group commit: `fdatasync` every `wal_fsync_interval` or
//! `wal_fsync_bytes`, whichever comes first. The crash loss window is therefore
//! bounded by those settings and is documented rather than pretended away.
//!
//! Recovery scans forward and **truncates at the first damaged frame**. A torn
//! tail is the expected outcome of `kill -9` mid-write, not corruption.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::model::LogRecord;

pub const MAGIC: &[u8; 8] = b"LGLSWAL\x02";
const FRAME_HEADER_BYTES: u64 = 8;
const HEADER_BYTES: u64 = 24; // magic + uuid

/// Refuse to allocate for an implausible frame length; a corrupt length field
/// must not turn into a multi-GB allocation during recovery.
const MAX_FRAME_BYTES: u32 = 256 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("encoding a wal batch failed: {0}")]
    Encode(postcard::Error),
    #[error("{path} is not a log-less wal segment")]
    BadMagic { path: PathBuf },
}

fn io(path: &Path) -> impl Fn(std::io::Error) -> WalError + '_ {
    move |source| WalError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Appends batches to the current segment, rolling and syncing on policy.
///
/// Single-threaded by design: one writer thread owns it, receivers hand work
/// over through the bounded queue.
pub struct WalWriter {
    dir: PathBuf,
    segment_bytes: u64,
    fsync_interval: Duration,
    fsync_bytes: u64,

    current: BufWriter<File>,
    current_path: PathBuf,
    current_id: u64,
    current_uuid: Uuid,
    current_len: u64,
    /// Roll a segment this long after its first record, as well as on size.
    /// `None` rolls on size alone (the main WAL).
    roll_after: Option<Duration>,
    /// Unix nanoseconds of the first append into the current segment; 0 while
    /// it is empty.
    current_started: u64,

    unsynced_bytes: u64,
    last_sync: Instant,
    /// Records covered by the last sync, and the value `maybe_sync` last reported.
    synced_records: u64,
    reported_records: u64,

    pub records_written: u64,
    pub bytes_written: u64,
    pub syncs: u64,
}

impl WalWriter {
    /// Open `dir`, creating it if needed, and start a **new** segment.
    ///
    /// Always starting a new segment rather than appending to the last one
    /// keeps recovery simple: a torn tail is truncated by the reader and never
    /// written into again.
    pub fn open(
        dir: impl AsRef<Path>,
        segment_bytes: u64,
        fsync_interval: Duration,
        fsync_bytes: u64,
    ) -> Result<Self, WalError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(io(&dir))?;
        let next_id = list_segments(&dir)?.last().map(|(id, _)| id + 1).unwrap_or(1);
        let (file, path, uuid) = create_segment(&dir, next_id)?;
        Ok(Self {
            dir,
            segment_bytes,
            fsync_interval,
            fsync_bytes,
            current: BufWriter::new(file),
            current_path: path,
            current_id: next_id,
            current_uuid: uuid,
            current_len: HEADER_BYTES,
            roll_after: None,
            current_started: 0,
            unsynced_bytes: 0,
            last_sync: Instant::now(),
            synced_records: 0,
            reported_records: 0,
            records_written: 0,
            bytes_written: 0,
            syncs: 0,
        })
    }

    /// Also roll a segment `after` its first record was appended (see
    /// [`WalWriter::append_at`]). The late spool is rolled this way: its
    /// records wait for the roll to reach Parquet, so the wait is bounded.
    pub fn roll_after(mut self, after: Duration) -> Self {
        self.roll_after = Some(after);
        self
    }

    /// [`WalWriter::append`] with the time given, for a writer that rolls on
    /// age. The clock is passed in so a replay can be driven at its own pace.
    pub fn append_at(&mut self, batch: &[LogRecord], now_unix_nanos: u64) -> Result<(), WalError> {
        if batch.is_empty() {
            return Ok(());
        }
        self.roll_if_due(now_unix_nanos)?;
        if self.current_started == 0 {
            self.current_started = now_unix_nanos.max(1);
        }
        self.append(batch)
    }

    /// Closes the current segment if it holds records and is older than the
    /// roll period. Called on every writer pass, so a segment that stopped
    /// receiving records still closes on time.
    pub fn roll_if_due(&mut self, now_unix_nanos: u64) -> Result<bool, WalError> {
        let Some(after) = self.roll_after else { return Ok(false) };
        let due = self.current_started != 0
            && now_unix_nanos.saturating_sub(self.current_started) >= after.as_nanos().min(u128::from(u64::MAX)) as u64;
        if due {
            self.roll()?;
        }
        Ok(due)
    }

    pub fn current_segment(&self) -> &Path {
        &self.current_path
    }

    /// Id of the segment currently being written. The merge path must skip it.
    pub fn current_segment_id(&self) -> u64 {
        self.current_id
    }

    /// Stable identity of the segment currently being written.
    pub fn current_segment_uuid(&self) -> Uuid {
        self.current_uuid
    }

    /// Append a batch. Returns without syncing; durability is the caller's
    /// cadence via [`WalWriter::maybe_sync`].
    pub fn append(&mut self, batch: &[LogRecord]) -> Result<(), WalError> {
        if batch.is_empty() {
            return Ok(());
        }
        let payload = postcard::to_stdvec(batch).map_err(WalError::Encode)?;
        let len = u32::try_from(payload.len()).map_err(|_| {
            WalError::Encode(postcard::Error::SerializeBufferFull)
        })?;
        let crc = crc32fast::hash(&payload);

        let w = &mut self.current;
        let p = &self.current_path;
        w.write_all(&len.to_le_bytes()).map_err(io(p))?;
        w.write_all(&crc.to_le_bytes()).map_err(io(p))?;
        w.write_all(&payload).map_err(io(p))?;

        let frame_bytes = FRAME_HEADER_BYTES + payload.len() as u64;
        self.current_len += frame_bytes;
        self.unsynced_bytes += frame_bytes;
        self.bytes_written += frame_bytes;
        self.records_written += batch.len() as u64;

        if self.current_len >= self.segment_bytes {
            self.roll()?;
        }
        Ok(())
    }

    /// Sync if the byte or time threshold has been crossed. Returns true when
    /// more records are on disk than at the previous call, whichever sync put
    /// them there: a segment roll inside [`WalWriter::append`] syncs as well, and
    /// a caller publishing [`WalWriter::synced_records`] must hear about it.
    pub fn maybe_sync(&mut self) -> Result<bool, WalError> {
        if self.unsynced_bytes > 0
            && (self.unsynced_bytes >= self.fsync_bytes
                || self.last_sync.elapsed() >= self.fsync_interval)
        {
            self.sync()?;
        }
        let advanced = self.synced_records > self.reported_records;
        self.reported_records = self.synced_records;
        Ok(advanced)
    }

    /// Records known to be on disk: those written before the last sync.
    pub fn synced_records(&self) -> u64 {
        self.synced_records
    }

    /// Flush userspace buffers and `fdatasync`.
    pub fn sync(&mut self) -> Result<(), WalError> {
        let p = self.current_path.clone();
        self.current.flush().map_err(io(&p))?;
        // sync_data, not sync_all: we do not need directory metadata on every
        // commit, only on segment creation.
        self.current.get_ref().sync_data().map_err(io(&p))?;
        self.unsynced_bytes = 0;
        self.last_sync = Instant::now();
        self.synced_records = self.records_written;
        self.syncs += 1;
        Ok(())
    }

    /// Close the current segment and start the next one.
    pub fn roll(&mut self) -> Result<(), WalError> {
        self.sync()?;
        self.current_id += 1;
        let (file, path, uuid) = create_segment(&self.dir, self.current_id)?;
        self.current = BufWriter::new(file);
        self.current_path = path;
        self.current_uuid = uuid;
        self.current_len = HEADER_BYTES;
        self.current_started = 0;
        Ok(())
    }
}

/// How the two logs of a [`SplitWal`] roll and sync.
#[derive(Debug, Clone, Copy)]
pub struct SplitConfig {
    pub segment_bytes: u64,
    pub fsync_interval: Duration,
    pub fsync_bytes: u64,
    /// A record whose event time is older than this is late.
    pub late_threshold: Duration,
    pub late_roll: Duration,
    pub late_segment_bytes: u64,
}

/// The main WAL and the late spool behind one writer.
///
/// The store is partitioned on event time, so a merged segment writes one file
/// per event hour it touches. A replay or a source with a slow clock spreads
/// one main segment over many hours, each a tiny file. Records whose event
/// time is already older than `late_threshold` therefore go to a second log,
/// `wal-late/`, which rolls on age (or size) instead of with the main one: the
/// same replay then makes one file per partition per late segment. Both logs
/// are synced together and merged by the same code, so a late record is as
/// durable as any other; what it gives up is reaching Parquet before its
/// segment rolls.
pub struct SplitWal {
    main: WalWriter,
    late: WalWriter,
    late_threshold_nanos: u64,
    /// Records that went to the late spool.
    pub late_records: u64,
}

impl SplitWal {
    pub fn open(wal_dir: &Path, late_dir: &Path, config: &SplitConfig) -> Result<Self, WalError> {
        Ok(Self {
            main: WalWriter::open(wal_dir, config.segment_bytes, config.fsync_interval, config.fsync_bytes)?,
            late: WalWriter::open(late_dir, config.late_segment_bytes, config.fsync_interval, config.fsync_bytes)?.roll_after(config.late_roll),
            late_threshold_nanos: config.late_threshold.as_nanos().min(u128::from(u64::MAX)) as u64,
            late_records: 0,
        })
    }

    /// Appends `batch`, each record to the log its event time puts it in.
    pub fn append(&mut self, batch: &[LogRecord], now_unix_nanos: u64) -> Result<(), WalError> {
        let cutoff = now_unix_nanos.saturating_sub(self.late_threshold_nanos);
        if !batch.iter().any(|r| r.timestamp_unix_nano < cutoff) {
            // The live path: nothing is copied.
            return self.main.append(batch);
        }
        let (late, on_time): (Vec<LogRecord>, Vec<LogRecord>) = batch.iter().cloned().partition(|r| r.timestamp_unix_nano < cutoff);
        self.late_records += late.len() as u64;
        self.main.append(&on_time)?;
        self.late.append_at(&late, now_unix_nanos)
    }

    /// Syncs on the logs' policy and rolls the late spool when it is due.
    /// Returns the number of records on disk when that number went up. It
    /// counts records in the order they were appended, so it is only ever
    /// published with **both** logs synced: a count that covered a record
    /// still buffered in the other log would acknowledge it early.
    pub fn maybe_sync(&mut self, now_unix_nanos: u64) -> Result<Option<u64>, WalError> {
        self.late.roll_if_due(now_unix_nanos)?;
        let advanced = self.main.maybe_sync()? | self.late.maybe_sync()?;
        if !advanced {
            return Ok(None);
        }
        Ok(Some(self.sync()?))
    }

    /// Syncs both logs and returns the number of records on disk.
    pub fn sync(&mut self) -> Result<u64, WalError> {
        if self.main.unsynced_bytes > 0 {
            self.main.sync()?;
        }
        if self.late.unsynced_bytes > 0 {
            self.late.sync()?;
        }
        Ok(self.main.records_written + self.late.records_written)
    }

    /// Ids of the segments being written, main then late. A merge must skip
    /// each in its own directory.
    pub fn active_segments(&self) -> (u64, u64) {
        (self.main.current_segment_id(), self.late.current_segment_id())
    }

    pub fn records_written(&self) -> u64 {
        self.main.records_written + self.late.records_written
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Best effort: a clean shutdown should not lose the tail. Errors here
        // are unactionable, but silence would be worse than a log line.
        if let Err(e) = self.sync() {
            tracing::error!(error = %e, "wal sync failed during shutdown");
        }
    }
}

fn create_segment(dir: &Path, id: u64) -> Result<(File, PathBuf, Uuid), WalError> {
    let path = dir.join(format!("{id:012}.wal"));
    let uuid = Uuid::now_v7();
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(io(&path))?;
    file.write_all(MAGIC).map_err(io(&path))?;
    file.write_all(uuid.as_bytes()).map_err(io(&path))?;
    // Sync the header and the directory entry now, so a crash cannot leave a
    // segment that exists but has no magic.
    file.sync_all().map_err(io(&path))?;
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok((file, path, uuid))
}

/// Segment ids and paths, ascending.
pub fn list_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(io(dir)(e)),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wal") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(id) = stem.parse::<u64>() else { continue };
        out.push((id, path));
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

#[derive(Debug, Default, PartialEq)]
pub struct RecoveryReport {
    pub segments_read: usize,
    pub batches: u64,
    pub records: u64,
    /// Segments whose damaged tail was truncated, with the surviving length.
    pub truncated: Vec<(PathBuf, u64)>,
}

/// Replay every segment in `dir`, truncating torn tails.
///
/// `visit` is called per recovered batch so recovery can stream into the
/// merge/compaction path without materialising the whole WAL in memory.
pub fn recover(
    dir: &Path,
    truncate_damaged: bool,
    mut visit: impl FnMut(Vec<LogRecord>),
) -> Result<RecoveryReport, WalError> {
    let mut report = RecoveryReport::default();

    for (_, path) in list_segments(dir)? {
        let outcome = replay_segment(&path, truncate_damaged, &mut visit)?;
        report.segments_read += 1;
        report.batches += outcome.batches;
        report.records += outcome.records;
        if let Some(len) = outcome.truncated_to {
            report.truncated.push((path, len));
        }
    }

    Ok(report)
}

#[derive(Debug, Default, PartialEq)]
pub struct SegmentOutcome {
    pub batches: u64,
    pub records: u64,
    /// `Some(len)` if the segment had a damaged tail; `len` is what survived.
    pub truncated_to: Option<u64>,
    /// Stable identity of this segment, from its header.
    pub uuid: Uuid,
}

/// Replay one segment. The merge path uses this directly so it can commit a
/// segment's output and delete it before moving to the next — bounding how much
/// unmerged WAL can accumulate.
pub fn replay_segment(
    path: &Path,
    truncate_damaged: bool,
    mut visit: impl FnMut(Vec<LogRecord>),
) -> Result<SegmentOutcome, WalError> {
    let mut outcome = SegmentOutcome::default();

    let mut file = File::open(path).map_err(io(path))?;
    let mut magic = [0u8; 8];
    if file.read_exact(&mut magic).is_err() || &magic != MAGIC {
        return Err(WalError::BadMagic {
            path: path.to_path_buf(),
        });
    }
    let mut uuid_bytes = [0u8; 16];
    if file.read_exact(&mut uuid_bytes).is_err() {
        return Err(WalError::BadMagic {
            path: path.to_path_buf(),
        });
    }
    outcome.uuid = Uuid::from_bytes(uuid_bytes);

    let mut good_len = HEADER_BYTES;
    loop {
        let mut header = [0u8; 8];
        match read_full(&mut file, &mut header) {
            Ok(true) => {}
            // Clean end of segment.
            Ok(false) => break,
            Err(e) => return Err(io(path)(e)),
        }
        let len = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if len == 0 || len > MAX_FRAME_BYTES {
            break; // torn or corrupt length field
        }

        let mut payload = vec![0u8; len as usize];
        match read_full(&mut file, &mut payload) {
            Ok(true) => {}
            Ok(false) => break, // torn payload
            Err(e) => return Err(io(path)(e)),
        }
        if crc32fast::hash(&payload) != crc {
            break; // torn write that happened to have a plausible length
        }
        let Ok(batch) = postcard::from_bytes::<Vec<LogRecord>>(&payload) else {
            break;
        };

        good_len += FRAME_HEADER_BYTES + len as u64;
        outcome.batches += 1;
        outcome.records += batch.len() as u64;
        visit(batch);
    }

    let actual_len = file.metadata().map_err(io(path))?.len();
    if actual_len > good_len {
        if truncate_damaged {
            let f = OpenOptions::new().write(true).open(path).map_err(io(path))?;
            f.set_len(good_len).map_err(io(path))?;
            f.sync_all().map_err(io(path))?;
        }
        outcome.truncated_to = Some(good_len);
    }

    Ok(outcome)
}

/// `true` if the buffer was filled, `false` on clean EOF, error otherwise.
fn read_full(file: &mut File, buf: &mut [u8]) -> std::io::Result<bool> {
    let mut read = 0;
    while read < buf.len() {
        match file.read(&mut buf[read..]) {
            Ok(0) => return Ok(false),
            Ok(n) => read += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// True for a segment whose header was never fully written: a crash (or a
/// second process that died) between creating the file and the header write.
/// The header is synced before the first append, so such a file holds no
/// records, and what is there is a prefix of the magic.
pub fn is_incomplete_header(path: &Path) -> Result<bool, WalError> {
    let mut file = File::open(path).map_err(io(path))?;
    let mut buf = [0u8; HEADER_BYTES as usize];
    let mut read = 0;
    while read < buf.len() {
        match file.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(n) => read += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(io(path)(e)),
        }
    }
    let magic_seen = read.min(MAGIC.len());
    Ok(read < HEADER_BYTES as usize && buf[..magic_seen] == MAGIC[..magic_seen])
}

/// Read a segment's UUID from its header without replaying it.
pub fn segment_uuid(path: &Path) -> Result<Uuid, WalError> {
    let mut file = File::open(path).map_err(io(path))?;
    let mut header = [0u8; HEADER_BYTES as usize];
    if file.read_exact(&mut header).is_err() || &header[0..8] != MAGIC {
        return Err(WalError::BadMagic {
            path: path.to_path_buf(),
        });
    }
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&header[8..24]);
    Ok(Uuid::from_bytes(uuid_bytes))
}

/// The late spool's directory name, beside the main WAL in the data dir.
pub const LATE_DIR: &str = "wal-late";

/// The late spool that goes with the main WAL at `wal_dir`: its sibling.
pub fn late_dir_beside(wal_dir: &Path) -> PathBuf {
    wal_dir.with_file_name(LATE_DIR)
}

/// Bytes on disk under `dir` — used by the disk-budget watermarks.
pub fn dir_bytes(dir: &Path) -> u64 {
    list_segments(dir)
        .unwrap_or_default()
        .iter()
        .filter_map(|(_, p)| fs::metadata(p).ok().map(|m| m.len()))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;

    fn batch(n: usize, tag: &str) -> Vec<LogRecord> {
        (0..n)
            .map(|i| LogRecord::new(i as u64, Severity::INFO, format!("{tag}-{i}")))
            .collect()
    }

    fn collect(dir: &Path, truncate: bool) -> (RecoveryReport, Vec<LogRecord>) {
        let mut all = Vec::new();
        let report = recover(dir, truncate, |b| all.extend(b)).unwrap();
        (report, all)
    }

    #[test]
    fn a_sync_done_by_a_segment_roll_is_reported() {
        // The writer loop publishes the synced count (what HEC acks, peer 200s and
        // `ingest.written` read) only when `maybe_sync` says records became durable.
        // A roll inside `append` syncs too; when that sync went unreported and no
        // more records came, acks and peer requests waited for a sync that had
        // already happened (seen in the e2e stack run: `written` stuck at 151 of 6000).
        let tmp = tempfile::tempdir().unwrap();
        let mut w = WalWriter::open(tmp.path(), 512, Duration::from_secs(60), 1 << 30).unwrap();
        w.append(&batch(50, "rolls")).unwrap();
        assert_eq!(w.current_segment_id(), 2, "the batch is larger than a segment, so append rolled");
        assert!(w.maybe_sync().unwrap(), "the roll's sync must be reported");
        assert_eq!(w.synced_records(), 50);
        assert!(!w.maybe_sync().unwrap(), "and reported once");

        // Below every threshold: appended, not synced, not reported.
        w.append(&batch(1, "small")).unwrap();
        assert!(!w.maybe_sync().unwrap());
        assert_eq!((w.synced_records(), w.records_written), (50, 51));
        w.sync().unwrap();
        assert!(w.maybe_sync().unwrap(), "an explicit sync is reported too");
        assert_eq!(w.synced_records(), 51);
    }

    fn split(dir: &Path, late_threshold: Duration, late_roll: Duration) -> SplitWal {
        let config = SplitConfig {
            segment_bytes: 1 << 30,
            fsync_interval: Duration::from_secs(3600),
            fsync_bytes: 1 << 30,
            late_threshold,
            late_roll,
            late_segment_bytes: 1 << 30,
        };
        SplitWal::open(&dir.join("wal"), &late_dir_beside(&dir.join("wal")), &config).unwrap()
    }

    fn at(event_secs: u64, tag: &str) -> LogRecord {
        LogRecord::new(event_secs * 1_000_000_000, Severity::INFO, tag)
    }

    #[test]
    fn records_older_than_the_threshold_go_to_the_late_spool_and_the_rest_to_the_main_wal() {
        let dir = tempfile::tempdir().unwrap();
        let hour = 3600u64;
        let now = 100 * hour;
        let mut w = split(dir.path(), Duration::from_secs(2 * hour), Duration::from_secs(1800));
        w.append(&[at(now - 10, "live"), at(now - 3 * hour, "replayed"), at(now - 2 * hour + 1, "just inside"), at(now + 60, "ahead")], now * 1_000_000_000).unwrap();
        assert_eq!((w.records_written(), w.late_records), (4, 1));
        assert_eq!(w.sync().unwrap(), 4, "both logs are on disk before the count is given");
        drop(w);
        let bodies = |d: &Path| collect(d, false).1.into_iter().map(|r| r.body).collect::<Vec<_>>();
        assert_eq!(bodies(&dir.path().join("wal")), ["live", "just inside", "ahead"]);
        assert_eq!(bodies(&dir.path().join("wal-late")), ["replayed"]);
    }

    #[test]
    fn the_late_spool_rolls_on_age_whether_or_not_more_late_records_arrive() {
        let dir = tempfile::tempdir().unwrap();
        let late = dir.path().join("wal-late");
        let ns = 1_000_000_000u64;
        let now = 1_000_000 * ns;
        let mut w = split(dir.path(), Duration::from_secs(60), Duration::from_secs(1800));
        let old = |tag: &str| at(1, tag);
        w.append(&[old("a")], now).unwrap();
        w.append(&[old("b")], now + 1799 * ns).unwrap();
        assert_eq!(w.active_segments(), (1, 1), "inside the roll period: one late segment");
        // A late record past the period closes the segment and starts the next.
        w.append(&[old("c")], now + 1800 * ns).unwrap();
        assert_eq!(w.active_segments(), (1, 2));
        // With nothing arriving, the writer's own pass closes it on time; an empty segment never rolls.
        assert_eq!(w.maybe_sync(now + 1900 * ns).unwrap(), Some(3), "the roll synced: three records on disk");
        assert_eq!(w.active_segments(), (1, 2));
        w.maybe_sync(now + 3600 * ns).unwrap();
        assert_eq!(w.active_segments(), (1, 3));
        w.maybe_sync(now + 9000 * ns).unwrap();
        assert_eq!(w.active_segments(), (1, 3), "segment 3 holds nothing");
        drop(w);
        assert_eq!(list_segments(&late).unwrap().len(), 3);
        let first = replay_segment(&list_segments(&late).unwrap()[0].1, false, |_| {}).unwrap();
        assert_eq!(first.records, 2);
    }

    #[test]
    fn a_published_count_never_covers_a_record_still_buffered_in_the_other_log() {
        let dir = tempfile::tempdir().unwrap();
        let ns = 1_000_000_000u64;
        let now = 1_000_000 * ns;
        // The main log syncs at once (interval 0); the late one would wait an hour by itself.
        let config = SplitConfig {
            segment_bytes: 1 << 30,
            fsync_interval: Duration::ZERO,
            fsync_bytes: 1 << 30,
            late_threshold: Duration::from_secs(60),
            late_roll: Duration::from_secs(1800),
            late_segment_bytes: 1 << 30,
        };
        let mut w = SplitWal::open(&dir.path().join("wal"), &dir.path().join("wal-late"), &config).unwrap();
        w.late.fsync_interval = Duration::from_secs(3600);
        w.append(&[at(1, "late, first in order"), at(1_000_000, "live")], now).unwrap();
        assert_eq!(w.maybe_sync(now).unwrap(), Some(2));
        // Read back without the writer flushing anything more: both are on disk.
        assert_eq!(collect(&dir.path().join("wal-late"), false).1.len(), 1);
        assert_eq!(collect(&dir.path().join("wal"), false).1.len(), 1);
        std::mem::forget(w);
    }

    #[test]
    fn round_trips_batches() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut w = WalWriter::open(tmp.path(), 1 << 30, Duration::from_secs(60), 1 << 30)
                .unwrap();
            w.append(&batch(3, "a")).unwrap();
            w.append(&batch(2, "b")).unwrap();
            w.sync().unwrap();
        }
        let (report, records) = collect(tmp.path(), false);
        assert_eq!(report.batches, 2);
        assert_eq!(report.records, 5);
        assert!(report.truncated.is_empty());
        assert_eq!(records[0].body, "a-0");
        assert_eq!(records[4].body, "b-1");
    }

    #[test]
    fn rolls_segments_at_the_size_limit() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut w =
                WalWriter::open(tmp.path(), 512, Duration::from_secs(60), 1 << 30).unwrap();
            for i in 0..20 {
                w.append(&batch(4, &format!("s{i}"))).unwrap();
            }
        }
        let segments = list_segments(tmp.path()).unwrap();
        assert!(segments.len() > 1, "expected a roll, got {segments:?}");
        let (report, records) = collect(tmp.path(), false);
        assert_eq!(report.records, 80);
        assert_eq!(records.len(), 80);
    }

    #[test]
    fn truncates_a_torn_tail_and_keeps_earlier_records() {
        let tmp = tempfile::tempdir().unwrap();
        let seg = {
            let mut w = WalWriter::open(tmp.path(), 1 << 30, Duration::from_secs(60), 1 << 30)
                .unwrap();
            w.append(&batch(3, "keep")).unwrap();
            w.append(&batch(3, "torn")).unwrap();
            w.sync().unwrap();
            w.current_segment().to_path_buf()
        };

        // Simulate kill -9 mid-write: lop off part of the final frame.
        let len = fs::metadata(&seg).unwrap().len();
        let f = OpenOptions::new().write(true).open(&seg).unwrap();
        f.set_len(len - 10).unwrap();
        drop(f);

        let (report, records) = collect(tmp.path(), true);
        assert_eq!(report.batches, 1, "only the intact batch survives");
        assert_eq!(report.records, 3);
        assert_eq!(report.truncated.len(), 1);
        assert!(records.iter().all(|r| r.body.starts_with("keep")));

        // Truncation is idempotent: a second recovery finds nothing to fix.
        let (again, _) = collect(tmp.path(), true);
        assert!(again.truncated.is_empty());
        assert_eq!(again.records, 3);
    }

    #[test]
    fn rejects_a_frame_whose_crc_does_not_match() {
        let tmp = tempfile::tempdir().unwrap();
        let seg = {
            let mut w = WalWriter::open(tmp.path(), 1 << 30, Duration::from_secs(60), 1 << 30)
                .unwrap();
            w.append(&batch(2, "good")).unwrap();
            w.append(&batch(2, "bitrot")).unwrap();
            w.sync().unwrap();
            w.current_segment().to_path_buf()
        };

        // Flip a byte inside the last frame's payload.
        let mut bytes = fs::read(&seg).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&seg, &bytes).unwrap();

        let (report, records) = collect(tmp.path(), false);
        assert_eq!(report.batches, 1);
        assert!(records.iter().all(|r| r.body.starts_with("good")));
    }

    #[test]
    fn a_torn_segment_header_is_reported_not_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("000000000001.wal"), b"NOTAWAL!").unwrap();
        let err = recover(tmp.path(), false, |_| {}).unwrap_err();
        assert!(matches!(err, WalError::BadMagic { .. }));
    }

    #[test]
    fn reopening_starts_a_fresh_segment() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut w = WalWriter::open(tmp.path(), 1 << 30, Duration::from_secs(60), 1 << 30)
                .unwrap();
            w.append(&batch(1, "first")).unwrap();
        }
        {
            let mut w = WalWriter::open(tmp.path(), 1 << 30, Duration::from_secs(60), 1 << 30)
                .unwrap();
            w.append(&batch(1, "second")).unwrap();
        }
        assert_eq!(list_segments(tmp.path()).unwrap().len(), 2);
        let (report, _) = collect(tmp.path(), false);
        assert_eq!(report.records, 2);
    }

    #[test]
    fn empty_batches_write_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w =
            WalWriter::open(tmp.path(), 1 << 30, Duration::from_secs(60), 1 << 30).unwrap();
        w.append(&[]).unwrap();
        w.sync().unwrap();
        let (report, _) = collect(tmp.path(), false);
        assert_eq!(report.batches, 0);
    }
}
