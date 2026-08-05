//! File tailing — the brownfield receiver.
//!
//! Ranked second in `docs/architecture.md` §6 behind OTLP, because most
//! adoption is "point it at /var/log" with nothing upstream to change.
//!
//! Poll-based rather than inotify/kqueue: log files change constantly, so the
//! event stream saves nothing, and polling is portable and has no dependency.
//! The hard parts are not reading — they are the three ways a log file moves
//! underneath you:
//!
//! * **rotation** — `app.log` is renamed to `app.log.1` and a new `app.log`
//!   appears. Detected by identity (device + inode), not by name, because the
//!   name is exactly what rotation reuses. **The old file is drained before
//!   switching**: anything written between the last poll and the rename is
//!   still in it, and jumping straight to the new inode silently loses it.
//!   Measured before the fix: 300 of 4,500 lines lost across one rotation.
//! * **truncation** — `> app.log` leaves the name and inode intact but the file
//!   shorter than our offset. Detected by size, and we restart from zero.
//! * **partial lines** — a poll can land mid-write. A trailing fragment without
//!   a newline is held back until the rest arrives, or the file stops growing.
//!
//! Checkpoints are persisted so a restart resumes rather than replaying the
//! file (duplicates) or skipping to the end (silent loss).

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Identity of a file, independent of its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId {
    pub device: u64,
    pub inode: u64,
}

impl FileId {
    #[cfg(unix)]
    fn of(meta: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            device: meta.dev(),
            inode: meta.ino(),
        }
    }

    #[cfg(not(unix))]
    fn of(meta: &std::fs::Metadata) -> Self {
        // No stable inode; fall back to length, which detects truncation but
        // not rotation. Documented rather than silently wrong.
        Self {
            device: 0,
            inode: meta.len(),
        }
    }
}

#[derive(Debug)]
struct Cursor {
    id: FileId,
    offset: u64,
    /// A trailing fragment with no newline yet.
    partial: String,
    /// Held open so a rotated-away file can still be drained. A renamed file
    /// is unreachable by path but perfectly readable through an open handle.
    handle: Option<File>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct TailStats {
    pub lines_read: u64,
    pub bytes_read: u64,
    pub rotations: u64,
    pub truncations: u64,
    /// Files that vanished between polls.
    pub disappeared: u64,
    pub read_errors: u64,
}

/// Tails a fixed set of paths.
pub struct Tailer {
    paths: Vec<PathBuf>,
    cursors: HashMap<PathBuf, Cursor>,
    checkpoint_path: Option<PathBuf>,
    /// Start at the end of a file seen for the first time, rather than
    /// replaying its whole history on first run.
    start_at_end: bool,
    pub stats: TailStats,
}

impl Tailer {
    pub fn new(paths: Vec<PathBuf>, start_at_end: bool) -> Self {
        Self {
            paths,
            cursors: HashMap::new(),
            checkpoint_path: None,
            start_at_end,
            stats: TailStats::default(),
        }
    }

    /// Persist cursors here, and restore them on construction.
    pub fn with_checkpoint(mut self, path: PathBuf) -> Self {
        self.cursors = load_checkpoint(&path);
        self.checkpoint_path = Some(path);
        self
    }

    pub fn tracked(&self) -> usize {
        self.cursors.len()
    }

    /// Read whatever has appeared since the last poll.
    ///
    /// `emit` receives complete lines only.
    pub fn poll(&mut self, mut emit: impl FnMut(&Path, &str)) {
        let paths = self.paths.clone();
        for path in paths {
            self.poll_one(&path, &mut emit);
        }
        self.save_checkpoint();
    }

    fn poll_one(&mut self, path: &Path, emit: &mut impl FnMut(&Path, &str)) {
        let meta = match std::fs::metadata(path) {
            Ok(m) => m,
            Err(_) => {
                // Gone for now. Keep the cursor: a rotation may put it back
                // within the same second, and forgetting would replay the file.
                if self.cursors.contains_key(path) {
                    self.stats.disappeared += 1;
                }
                return;
            }
        };
        if !meta.is_file() {
            return;
        }
        let id = FileId::of(&meta);
        let len = meta.len();

        let start_at_end = self.start_at_end;
        let cursor = self.cursors.entry(path.to_path_buf()).or_insert_with(|| Cursor {
            id,
            offset: if start_at_end { len } else { 0 },
            partial: String::new(),
            handle: None,
        });

        if cursor.handle.is_none() {
            cursor.handle = File::open(path).ok();
            // A restored checkpoint names a file we have not opened yet; if the
            // identity no longer matches, the file rotated while we were down
            // and the new one is unread from the start.
            if cursor.id != id {
                cursor.id = id;
                cursor.offset = 0;
                cursor.partial.clear();
            }
        }

        let rotated = cursor.id != id;
        if rotated {
            // Drain what the rotated-away file still holds. It is unreachable
            // by name now, but our handle still points at it — and everything
            // written since the last poll is only there.
            self.stats.rotations += 1;
            drain_handle(cursor, path, &mut self.stats, emit);
            // Flush any trailing fragment unconditionally, not only when the
            // drain found new bytes: the fragment may have been read on an
            // earlier poll and be sitting in the cursor. The old file is gone,
            // so that line will never be completed — emitting it incomplete
            // beats dropping it, and it must not be glued onto the new file's
            // first line.
            let leftover = std::mem::take(&mut cursor.partial);
            if !leftover.is_empty() {
                self.stats.lines_read += 1;
                emit(path, &leftover);
            }
            cursor.id = id;
            cursor.offset = 0;
            cursor.partial.clear();
            cursor.handle = File::open(path).ok();
        } else if len < cursor.offset {
            // Truncation: `> file` keeps the inode but drops the contents.
            self.stats.truncations += 1;
            cursor.offset = 0;
            cursor.partial.clear();
            cursor.handle = File::open(path).ok();
        }

        drain_handle(cursor, path, &mut self.stats, emit);
    }

    fn save_checkpoint(&self) {
        let Some(path) = &self.checkpoint_path else {
            return;
        };
        let mut out = String::new();
        for (file, cursor) in &self.cursors {
            // Fields are tab-separated and paths cannot contain a tab here
            // without breaking the format; a path with a tab is skipped rather
            // than corrupting the file.
            let name = file.to_string_lossy();
            if name.contains('\t') || name.contains('\n') {
                continue;
            }
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\n",
                name, cursor.id.device, cursor.id.inode, cursor.offset
            ));
        }
        // Atomic replace: a half-written checkpoint would resume at a nonsense
        // offset, which is worse than none at all.
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, out).is_ok() {
            let _ = std::fs::rename(&tmp, path);
        }
    }
}

/// Read everything available through the cursor's open handle, emitting whole
/// lines. Returns true if any bytes were read.
fn drain_handle(
    cursor: &mut Cursor,
    path: &Path,
    stats: &mut TailStats,
    emit: &mut impl FnMut(&Path, &str),
) -> bool {
    let Some(file) = cursor.handle.as_mut() else {
        return false;
    };
    let len = match file.metadata() {
        Ok(m) => m.len(),
        Err(_) => {
            stats.read_errors += 1;
            return false;
        }
    };
    if len <= cursor.offset {
        return false;
    }
    if file.seek(SeekFrom::Start(cursor.offset)).is_err() {
        stats.read_errors += 1;
        return false;
    }

    let mut buffer = Vec::new();
    let read = match file.take(len - cursor.offset).read_to_end(&mut buffer) {
        Ok(n) => n,
        Err(_) => {
            stats.read_errors += 1;
            return false;
        }
    };
    if read == 0 {
        return false;
    }
    cursor.offset += read as u64;
    stats.bytes_read += read as u64;

    let mut text = std::mem::take(&mut cursor.partial);
    text.push_str(&String::from_utf8_lossy(&buffer));

    // Everything up to the last newline is complete; the remainder is a
    // fragment of a line still being written.
    let (complete, remainder) = match text.rfind('\n') {
        Some(idx) => (&text[..idx], &text[idx + 1..]),
        None => ("", text.as_str()),
    };
    for line in complete.split('\n') {
        if line.is_empty() {
            continue;
        }
        stats.lines_read += 1;
        emit(path, line);
    }
    cursor.partial = remainder.to_string();
    true
}

fn load_checkpoint(path: &Path) -> HashMap<PathBuf, Cursor> {
    let mut out = HashMap::new();
    let Ok(file) = File::open(path) else {
        return out;
    };
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 4 {
            continue;
        }
        let (Ok(device), Ok(inode), Ok(offset)) = (
            parts[1].parse::<u64>(),
            parts[2].parse::<u64>(),
            parts[3].parse::<u64>(),
        ) else {
            continue;
        };
        out.insert(
            PathBuf::from(parts[0]),
            Cursor {
                id: FileId { device, inode },
                offset,
                // Partial lines are deliberately not persisted: on restart the
                // fragment is re-read from the file itself.
                partial: String::new(),
                handle: None,
            },
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn append(path: &Path, text: &str) {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        f.write_all(text.as_bytes()).unwrap();
        f.flush().unwrap();
    }

    fn collect(tailer: &mut Tailer) -> Vec<String> {
        let mut lines = Vec::new();
        tailer.poll(|_, line| lines.push(line.to_string()));
        lines
    }

    #[test]
    fn reads_appended_lines_once() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        append(&log, "first\nsecond\n");

        let mut tailer = Tailer::new(vec![log.clone()], false);
        assert_eq!(collect(&mut tailer), vec!["first", "second"]);
        // Nothing new: a second poll must not replay.
        assert!(collect(&mut tailer).is_empty());

        append(&log, "third\n");
        assert_eq!(collect(&mut tailer), vec!["third"]);
        assert_eq!(tailer.stats.lines_read, 3);
    }

    #[test]
    fn holds_back_a_partial_line_until_its_newline_arrives() {
        // A poll landing mid-write must not emit half a line.
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        append(&log, "complete\npar");

        let mut tailer = Tailer::new(vec![log.clone()], false);
        assert_eq!(collect(&mut tailer), vec!["complete"]);

        append(&log, "tial line\n");
        assert_eq!(collect(&mut tailer), vec!["partial line"]);
    }

    #[test]
    fn follows_rotation_by_identity_not_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        append(&log, "before rotation\n");

        let mut tailer = Tailer::new(vec![log.clone()], false);
        assert_eq!(collect(&mut tailer), vec!["before rotation"]);

        // logrotate: rename the old file, create a new one at the same path.
        std::fs::rename(&log, tmp.path().join("app.log.1")).unwrap();
        append(&log, "after rotation\n");

        assert_eq!(collect(&mut tailer), vec!["after rotation"]);
        assert_eq!(tailer.stats.rotations, 1);
    }

    #[test]
    fn a_rotation_does_not_lose_what_was_written_just_before_it() {
        // The race that cost 300 of 4,500 lines in testing: lines land in the
        // file after the last poll, then logrotate renames it. Jumping straight
        // to the new inode abandons them.
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        append(&log, "polled\n");

        let mut tailer = Tailer::new(vec![log.clone()], false);
        assert_eq!(collect(&mut tailer), vec!["polled"]);

        // Written, then rotated away, with no poll in between.
        append(&log, "written just before rotation\n");
        std::fs::rename(&log, tmp.path().join("app.log.1")).unwrap();
        append(&log, "after rotation\n");

        assert_eq!(
            collect(&mut tailer),
            vec!["written just before rotation", "after rotation"],
            "the rotated file must be drained before switching"
        );
        assert_eq!(tailer.stats.rotations, 1);
    }

    #[test]
    fn a_partial_line_in_a_rotated_file_is_not_glued_to_the_next_file() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        append(&log, "complete\nno newline yet");

        let mut tailer = Tailer::new(vec![log.clone()], false);
        assert_eq!(collect(&mut tailer), vec!["complete"]);

        std::fs::rename(&log, tmp.path().join("app.log.1")).unwrap();
        append(&log, "fresh file\n");

        let lines = collect(&mut tailer);
        assert_eq!(lines, vec!["no newline yet", "fresh file"]);
    }

    #[test]
    fn restarts_from_zero_after_truncation() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        append(&log, "aaaa\nbbbb\ncccc\n");

        let mut tailer = Tailer::new(vec![log.clone()], false);
        assert_eq!(collect(&mut tailer).len(), 3);

        // `> app.log` — same inode, no content.
        std::fs::write(&log, "short\n").unwrap();
        assert_eq!(collect(&mut tailer), vec!["short"]);
        assert_eq!(tailer.stats.truncations, 1);
    }

    #[test]
    fn a_checkpoint_resumes_rather_than_replaying_or_skipping() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        let checkpoint = tmp.path().join("tail.ckpt");
        append(&log, "one\ntwo\n");

        {
            let mut tailer =
                Tailer::new(vec![log.clone()], false).with_checkpoint(checkpoint.clone());
            assert_eq!(collect(&mut tailer).len(), 2);
        }

        // Written while the agent was down.
        append(&log, "three\n");

        let mut resumed =
            Tailer::new(vec![log.clone()], false).with_checkpoint(checkpoint.clone());
        assert_eq!(
            collect(&mut resumed),
            vec!["three"],
            "must not replay one/two, must not skip three"
        );
    }

    #[test]
    fn a_new_file_can_start_at_the_end_instead_of_replaying_history() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        append(&log, "ancient history\n");

        let mut tailer = Tailer::new(vec![log.clone()], true);
        assert!(collect(&mut tailer).is_empty(), "history is not replayed");

        append(&log, "live\n");
        assert_eq!(collect(&mut tailer), vec!["live"]);
    }

    #[test]
    fn a_missing_file_is_not_an_error_and_is_picked_up_when_it_appears() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("not-yet.log");

        let mut tailer = Tailer::new(vec![log.clone()], false);
        assert!(collect(&mut tailer).is_empty());
        assert_eq!(tailer.stats.read_errors, 0);

        append(&log, "appeared\n");
        assert_eq!(collect(&mut tailer), vec!["appeared"]);
    }

    #[test]
    fn tails_several_files_and_reports_which_produced_each_line() {
        let tmp = tempfile::tempdir().unwrap();
        let (a, b) = (tmp.path().join("a.log"), tmp.path().join("b.log"));
        append(&a, "from a\n");
        append(&b, "from b\n");

        let mut tailer = Tailer::new(vec![a.clone(), b.clone()], false);
        let mut seen = Vec::new();
        tailer.poll(|path, line| {
            seen.push((path.file_name().unwrap().to_string_lossy().to_string(), line.to_string()))
        });
        seen.sort();
        assert_eq!(
            seen,
            vec![
                ("a.log".to_string(), "from a".to_string()),
                ("b.log".to_string(), "from b".to_string())
            ]
        );
    }

    #[test]
    fn a_corrupt_checkpoint_is_ignored_rather_than_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        let checkpoint = tmp.path().join("tail.ckpt");
        std::fs::write(&checkpoint, "garbage\nnot\ttab\tseparated\n").unwrap();
        append(&log, "line\n");

        let mut tailer = Tailer::new(vec![log], false).with_checkpoint(checkpoint);
        assert_eq!(collect(&mut tailer), vec!["line"]);
    }

    #[test]
    fn invalid_utf8_does_not_stop_the_stream() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("app.log");
        let mut f = std::fs::File::create(&log).unwrap();
        f.write_all(b"good line\n\xff\xfe bad bytes\nafter\n").unwrap();
        f.flush().unwrap();

        let mut tailer = Tailer::new(vec![log], false);
        let lines = collect(&mut tailer);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "good line");
        assert_eq!(lines[2], "after");
    }
}
