//! Durable outbound queue.
//!
//! Everything the agent has accepted responsibility for but not yet delivered
//! lives here: envelopes a Sentry SDK was told `200` for, events waiting on a
//! vendor that is rate-limiting, aggregates not yet flushed. Holding those in
//! memory means a restart loses data the sender has already discarded — the
//! same lie the HEC ack work exists to avoid, one layer further out.
//!
//! Deliberately not the WAL. The WAL is the record path's hot loop and is tuned
//! for it (group commit, batched frames, torn-tail truncation); a spool is
//! low-rate, read from the front, and needs a *cursor* the WAL has no concept
//! of. Sharing one would couple the ingest path's fsync cadence to a vendor's
//! availability.
//!
//! The cursor is a separate file written by atomic rename, never appended to,
//! and never the catalog — `docs/architecture.md` §5 records why: it changes
//! far more often than catalog rows and would drag SQLite into the delivery
//! path.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"LGLSSPL\x01";
const HEADER_BYTES: u64 = 8;
/// `[u32 length][u32 crc]` before each payload.
const FRAME_OVERHEAD: u64 = 8;

#[derive(Debug, thiserror::Error)]
pub enum SpoolError {
    #[error("io on {path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },
    #[error("{path} is not a spool segment")]
    BadMagic { path: PathBuf },
    #[error("record of {len} bytes exceeds the {limit} byte limit")]
    TooLarge { len: usize, limit: usize },
}

fn io(path: &Path, source: std::io::Error) -> SpoolError {
    SpoolError::Io { path: path.to_path_buf(), source }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Cursor {
    pub segment: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct SpoolConfig {
    /// Roll to a new segment past this size, so consumed data can be reclaimed
    /// by unlinking whole files rather than rewriting one.
    pub segment_bytes: u64,
    /// Ceiling on the whole spool. A vendor that is down for a week must not
    /// fill the disk the logs themselves are supposed to be using.
    pub max_total_bytes: u64,
    pub max_record_bytes: usize,
}

impl Default for SpoolConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 16 * 1024 * 1024,
            max_total_bytes: 512 * 1024 * 1024,
            max_record_bytes: 32 * 1024 * 1024,
        }
    }
}

/// Append-only, read-from-the-front, with a durable cursor.
pub struct Spool {
    dir: PathBuf,
    config: SpoolConfig,
    write_segment: u64,
    writer: File,
    write_offset: u64,
    cursor: Cursor,
    /// Records dropped because the spool was full. Surfaced rather than
    /// silently absorbed: a full spool means the vendor has been unreachable
    /// long enough to matter.
    pub dropped_full: u64,
}

impl Spool {
    pub fn open(dir: &Path, config: SpoolConfig) -> Result<Self, SpoolError> {
        fs::create_dir_all(dir).map_err(|e| io(dir, e))?;
        let segments = list_segments(dir)?;
        let cursor = read_cursor(dir)?;
        let write_segment = segments.last().copied().unwrap_or(1);
        let (writer, write_offset) = open_segment_for_append(dir, write_segment)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            config,
            write_segment,
            writer,
            write_offset,
            cursor: if cursor.segment == 0 {
                Cursor { segment: segments.first().copied().unwrap_or(write_segment), offset: HEADER_BYTES }
            } else {
                cursor
            },
            dropped_full: 0,
        })
    }

    /// Appends a record. `fsync` is the caller's decision via [`Spool::sync`],
    /// because a spool that fsyncs per record cannot keep up with a burst and
    /// the data is already durable enough once the OS has it for most uses.
    pub fn push(&mut self, payload: &[u8]) -> Result<bool, SpoolError> {
        if payload.len() > self.config.max_record_bytes {
            return Err(SpoolError::TooLarge {
                len: payload.len(),
                limit: self.config.max_record_bytes,
            });
        }
        if self.total_bytes()? + payload.len() as u64 > self.config.max_total_bytes {
            self.dropped_full += 1;
            return Ok(false);
        }
        if self.write_offset + FRAME_OVERHEAD + payload.len() as u64 > self.config.segment_bytes
            && self.write_offset > HEADER_BYTES
        {
            self.roll()?;
        }
        let mut frame = Vec::with_capacity(payload.len() + FRAME_OVERHEAD as usize);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        frame.extend_from_slice(payload);
        self.writer.write_all(&frame).map_err(|e| io(&self.dir, e))?;
        self.write_offset += frame.len() as u64;
        Ok(true)
    }

    pub fn sync(&mut self) -> Result<(), SpoolError> {
        self.writer.sync_data().map_err(|e| io(&self.dir, e))
    }

    /// Reads up to `max` undelivered records without consuming them.
    ///
    /// Returns the cursor each record *ends* at, so a caller that delivers
    /// three of five can commit exactly what it delivered.
    pub fn peek(&self, max: usize) -> Result<Vec<(Cursor, Vec<u8>)>, SpoolError> {
        let mut out = Vec::new();
        let start = self.cursor;
        let mut at_segment = start.segment;
        let mut at_offset = start.offset;
        let segments = list_segments(&self.dir)?;
        for segment in segments.into_iter().filter(|s| *s >= start.segment) {
            if segment > at_segment {
                at_segment = segment;
                at_offset = HEADER_BYTES;
            }
            let path = segment_path(&self.dir, segment);
            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(io(&path, e)),
            };
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(at_offset)).map_err(|e| io(&path, e))?;
            let mut offset = at_offset;
            loop {
                if out.len() >= max {
                    return Ok(out);
                }
                match read_frame(&mut reader) {
                    Ok(Some(payload)) => {
                        offset += FRAME_OVERHEAD + payload.len() as u64;
                        out.push((Cursor { segment, offset }, payload));
                    }
                    // A torn tail is the normal shape of a crash mid-append.
                    // Everything before it is intact and deliverable.
                    Ok(None) => break,
                    Err(e) => return Err(io(&path, e)),
                }
            }
            at_segment = segment;
            at_offset = offset;
        }
        Ok(out)
    }

    /// Records that everything up to `cursor` has been delivered, and reclaims
    /// whole segments behind it.
    ///
    /// Written by atomic rename so a crash mid-commit leaves either the old
    /// cursor or the new one — never a half-written offset that would resume
    /// inside a frame.
    pub fn commit(&mut self, cursor: Cursor) -> Result<(), SpoolError> {
        self.cursor = cursor;
        write_cursor(&self.dir, cursor)?;
        for segment in list_segments(&self.dir)? {
            if segment < cursor.segment && segment != self.write_segment {
                let path = segment_path(&self.dir, segment);
                fs::remove_file(&path).map_err(|e| io(&path, e))?;
            }
        }
        Ok(())
    }

    pub fn cursor(&self) -> Cursor {
        self.cursor
    }

    /// Bytes still on disk, delivered or not.
    pub fn total_bytes(&self) -> Result<u64, SpoolError> {
        let mut total = 0;
        for segment in list_segments(&self.dir)? {
            let path = segment_path(&self.dir, segment);
            if let Ok(meta) = fs::metadata(&path) {
                total += meta.len();
            }
        }
        Ok(total)
    }

    pub fn is_empty(&self) -> Result<bool, SpoolError> {
        Ok(self.peek(1)?.is_empty())
    }

    fn roll(&mut self) -> Result<(), SpoolError> {
        self.writer.sync_data().map_err(|e| io(&self.dir, e))?;
        self.write_segment += 1;
        let (writer, offset) = open_segment_for_append(&self.dir, self.write_segment)?;
        self.writer = writer;
        self.write_offset = offset;
        Ok(())
    }
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("spool-{id:012}.log"))
}

fn cursor_path(dir: &Path) -> PathBuf {
    dir.join("cursor")
}

fn list_segments(dir: &Path) -> Result<Vec<u64>, SpoolError> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(io(dir, e)),
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name.strip_prefix("spool-").and_then(|n| n.strip_suffix(".log")) {
            if let Ok(id) = id.parse::<u64>() {
                out.push(id);
            }
        }
    }
    out.sort_unstable();
    Ok(out)
}

fn open_segment_for_append(dir: &Path, id: u64) -> Result<(File, u64), SpoolError> {
    let path = segment_path(dir, id);
    let existed = path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&path)
        .map_err(|e| io(&path, e))?;
    if !existed {
        file.write_all(MAGIC).map_err(|e| io(&path, e))?;
        file.sync_data().map_err(|e| io(&path, e))?;
        return Ok((file, HEADER_BYTES));
    }
    let mut magic = [0u8; 8];
    let mut check = File::open(&path).map_err(|e| io(&path, e))?;
    check.read_exact(&mut magic).map_err(|e| io(&path, e))?;
    if &magic != MAGIC {
        return Err(SpoolError::BadMagic { path });
    }
    let len = file.metadata().map_err(|e| io(&path, e))?.len();
    Ok((file, len))
}

/// Reads one frame. `Ok(None)` means end of file or a torn tail — both mean
/// "nothing more to deliver from this segment", and neither is an error.
fn read_frame(reader: &mut impl Read) -> std::io::Result<Option<Vec<u8>>> {
    let mut head = [0u8; 8];
    match reader.read_exact(&mut head) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes([head[0], head[1], head[2], head[3]]) as usize;
    let crc = u32::from_le_bytes([head[4], head[5], head[6], head[7]]);
    let mut payload = vec![0u8; len];
    match reader.read_exact(&mut payload) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    if crc32fast::hash(&payload) != crc {
        // A frame whose length survived but whose bytes did not. Treating it
        // as end-of-segment is the safe reading: delivering corrupt data
        // upstream is worse than delivering less.
        return Ok(None);
    }
    Ok(Some(payload))
}

fn read_cursor(dir: &Path) -> Result<Cursor, SpoolError> {
    let path = cursor_path(dir);
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Cursor::default()),
        Err(e) => return Err(io(&path, e)),
    };
    let mut parts = text.trim().split(':');
    let segment = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let offset = parts.next().and_then(|s| s.parse().ok()).unwrap_or(HEADER_BYTES);
    Ok(Cursor { segment, offset })
}

fn write_cursor(dir: &Path, cursor: Cursor) -> Result<(), SpoolError> {
    let path = cursor_path(dir);
    let tmp = dir.join("cursor.tmp");
    {
        let mut file = File::create(&tmp).map_err(|e| io(&tmp, e))?;
        write!(file, "{}:{}", cursor.segment, cursor.offset).map_err(|e| io(&tmp, e))?;
        file.sync_all().map_err(|e| io(&tmp, e))?;
    }
    fs::rename(&tmp, &path).map_err(|e| io(&path, e))?;
    // Fsync the directory, or the rename itself can be lost by a crash and the
    // cursor silently reverts to an older position — re-delivering everything
    // between the two.
    if let Ok(dir_handle) = File::open(dir) {
        let _ = dir_handle.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spool(dir: &Path) -> Spool {
        Spool::open(dir, SpoolConfig::default()).unwrap()
    }

    #[test]
    fn round_trips_records_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = spool(tmp.path());
        for i in 0..5 {
            assert!(s.push(format!("record-{i}").as_bytes()).unwrap());
        }
        s.sync().unwrap();
        let read: Vec<String> = s
            .peek(10)
            .unwrap()
            .into_iter()
            .map(|(_, p)| String::from_utf8(p).unwrap())
            .collect();
        assert_eq!(read, ["record-0", "record-1", "record-2", "record-3", "record-4"]);
    }

    #[test]
    fn a_restart_resumes_at_the_cursor_and_re_sends_nothing() {
        // The whole point: an envelope already delivered must not go again, and
        // one not yet delivered must not be lost.
        let tmp = tempfile::tempdir().unwrap();
        let mut s = spool(tmp.path());
        for i in 0..5 {
            s.push(format!("r{i}").as_bytes()).unwrap();
        }
        s.sync().unwrap();
        let batch = s.peek(3).unwrap();
        s.commit(batch.last().unwrap().0).unwrap();
        drop(s);

        let s = spool(tmp.path());
        let left: Vec<String> = s
            .peek(10)
            .unwrap()
            .into_iter()
            .map(|(_, p)| String::from_utf8(p).unwrap())
            .collect();
        assert_eq!(left, ["r3", "r4"], "delivered records must not reappear");
    }

    #[test]
    fn nothing_is_lost_when_the_process_dies_before_committing() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = spool(tmp.path());
        s.push(b"unacknowledged").unwrap();
        s.sync().unwrap();
        drop(s); // as if killed

        let s = spool(tmp.path());
        assert_eq!(s.peek(10).unwrap().len(), 1, "an uncommitted record is redelivered");
    }

    #[test]
    fn a_torn_tail_stops_the_read_without_losing_earlier_records() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = spool(tmp.path());
        s.push(b"intact").unwrap();
        s.sync().unwrap();
        drop(s);
        // Simulate a crash mid-append: a length header with no payload.
        let path = segment_path(tmp.path(), 1);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&99u32.to_le_bytes()).unwrap();
        file.write_all(&0u32.to_le_bytes()).unwrap();
        file.write_all(b"trunc").unwrap();
        drop(file);

        let s = spool(tmp.path());
        let read = s.peek(10).unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].1, b"intact");
    }

    #[test]
    fn a_corrupt_frame_is_not_delivered() {
        // Delivering corrupted bytes upstream is worse than delivering less.
        let tmp = tempfile::tempdir().unwrap();
        let mut s = spool(tmp.path());
        s.push(b"good").unwrap();
        s.push(b"will be corrupted").unwrap();
        s.sync().unwrap();
        drop(s);

        let path = segment_path(tmp.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, bytes).unwrap();

        let s = spool(tmp.path());
        let read = s.peek(10).unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].1, b"good");
    }

    #[test]
    fn segments_roll_and_consumed_ones_are_reclaimed() {
        let tmp = tempfile::tempdir().unwrap();
        let config = SpoolConfig { segment_bytes: 256, ..Default::default() };
        let mut s = Spool::open(tmp.path(), config).unwrap();
        for i in 0..50 {
            s.push(format!("{i:0>40}").as_bytes()).unwrap();
        }
        s.sync().unwrap();
        assert!(list_segments(tmp.path()).unwrap().len() > 1, "should have rolled");

        let all = s.peek(1000).unwrap();
        assert_eq!(all.len(), 50, "records must survive a roll");
        s.commit(all.last().unwrap().0).unwrap();
        assert_eq!(
            list_segments(tmp.path()).unwrap().len(),
            1,
            "fully delivered segments are unlinked, not rewritten"
        );
    }

    #[test]
    fn a_full_spool_refuses_rather_than_filling_the_disk() {
        // A vendor down for a week must not consume the space the logs need.
        let tmp = tempfile::tempdir().unwrap();
        let config = SpoolConfig { max_total_bytes: 1024, ..Default::default() };
        let mut s = Spool::open(tmp.path(), config).unwrap();
        let mut accepted = 0;
        for _ in 0..200 {
            if s.push(&[b'x'; 100]).unwrap() {
                accepted += 1;
            }
        }
        assert!(accepted > 0 && accepted < 200, "accepted {accepted}");
        assert!(s.dropped_full > 0);
        assert!(s.total_bytes().unwrap() <= 1024 + 128);
    }

    #[test]
    fn oversized_records_are_an_error_not_a_silent_truncation() {
        let tmp = tempfile::tempdir().unwrap();
        let config = SpoolConfig { max_record_bytes: 16, ..Default::default() };
        let mut s = Spool::open(tmp.path(), config).unwrap();
        assert!(matches!(s.push(&[0u8; 64]), Err(SpoolError::TooLarge { .. })));
    }

    #[test]
    fn a_partial_batch_commit_redelivers_only_the_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = spool(tmp.path());
        for i in 0..4 {
            s.push(format!("r{i}").as_bytes()).unwrap();
        }
        s.sync().unwrap();
        let batch = s.peek(4).unwrap();
        // Delivered two of four, then the vendor started failing.
        s.commit(batch[1].0).unwrap();
        let left: Vec<String> = s
            .peek(10)
            .unwrap()
            .into_iter()
            .map(|(_, p)| String::from_utf8(p).unwrap())
            .collect();
        assert_eq!(left, ["r2", "r3"]);
    }

    #[test]
    fn the_cursor_survives_as_a_whole_value_or_not_at_all() {
        let tmp = tempfile::tempdir().unwrap();
        write_cursor(tmp.path(), Cursor { segment: 7, offset: 4242 }).unwrap();
        assert_eq!(read_cursor(tmp.path()).unwrap(), Cursor { segment: 7, offset: 4242 });
        // No cursor file yet is a valid starting state, not an error.
        let fresh = tempfile::tempdir().unwrap();
        assert_eq!(read_cursor(fresh.path()).unwrap(), Cursor::default());
    }

    #[test]
    fn a_foreign_file_in_the_directory_is_rejected_not_parsed() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(segment_path(tmp.path(), 1), b"not a spool at all").unwrap();
        assert!(matches!(
            Spool::open(tmp.path(), SpoolConfig::default()),
            Err(SpoolError::BadMagic { .. })
        ));
    }

    #[test]
    fn empty_reports_empty_and_stops_reporting_it_once_pushed() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = spool(tmp.path());
        assert!(s.is_empty().unwrap());
        s.push(b"x").unwrap();
        s.sync().unwrap();
        assert!(!s.is_empty().unwrap());
    }
}
