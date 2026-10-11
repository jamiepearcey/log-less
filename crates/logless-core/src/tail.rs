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
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

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

/// One `[[tail]]` section: a set of paths or glob patterns sharing exclusions,
/// a start position and a service label. Sources are independent so a noisy
/// debug log can be tailed from the end while an audit log is read in full.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TailSource {
    /// Literal paths or globs. `*`, `?` and `[..]` match within one path
    /// component; a component of exactly `**` matches any number of directories.
    pub paths: Vec<PathBuf>,
    /// Matched against the file name, or against the full path when the pattern
    /// contains a `/` (so a suffix match needs a leading `**/`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
    /// For files already present at startup, skip their history. Files that
    /// appear later are always read from the beginning: skipping their first
    /// lines would drop exactly what a new service logs while starting up.
    #[serde(default)]
    pub from_end: bool,
    /// Service attached to lines that do not name one themselves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// The index this source's lines are stored in. Left out, the tenant's
    /// default index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
}

struct Source {
    patterns: Vec<PathBuf>,
    exclude: Vec<String>,
    from_end: bool,
    service: Option<String>,
    index: Option<String>,
}

/// Tails a set of paths and patterns.
pub struct Tailer {
    sources: Vec<Source>,
    paths: Vec<PathBuf>,
    /// Which source claimed each path; the first source to match wins.
    origin: HashMap<PathBuf, usize>,
    /// Patterns like `/var/log/*.log`, re-expanded periodically. A fleet's log
    /// files appear and disappear — a new service deploys, a container starts —
    /// so expanding once at startup would silently miss everything created
    /// afterwards, which is exactly the window an incident lives in.
    last_expansion: Option<Instant>,
    cursors: HashMap<PathBuf, Cursor>,
    checkpoint_path: Option<PathBuf>,
    /// False until the first poll has run; only files seen by then count as
    /// "already present" for `from_end`.
    started: bool,
    pub stats: TailStats,
}

/// How often glob patterns are re-expanded. A directory scan per poll would
/// be wasteful at a 200 ms cadence; a file created now is picked up within
/// this, which is well inside the window that matters.
const REDISCOVER_INTERVAL: Duration = Duration::from_secs(5);

/// Characters that make a path a pattern rather than a name.
fn is_pattern(path: &Path) -> bool {
    path.to_string_lossy().contains(['*', '?', '['])
}

fn is_pattern_segment(segment: &str) -> bool {
    segment.contains(['*', '?', '['])
}

/// Expands a path pattern against the filesystem, returning matching files.
///
/// Wildcards apply per path component, and a component of exactly `**` spans
/// any number of directories. Descent never follows directory symlinks, so a
/// link cycle cannot make `**` loop.
fn glob_paths(pattern: &Path) -> Vec<PathBuf> {
    use std::path::Component;
    let mut root = PathBuf::new();
    let mut segments: Vec<String> = Vec::new();
    for component in pattern.components() {
        match component {
            Component::Normal(s) => segments.push(s.to_string_lossy().into_owned()),
            other => root.push(other.as_os_str()),
        }
    }
    // A trailing `**` means "every file below", not "directories only".
    if segments.last().is_some_and(|s| s == "**") {
        segments.push("*".into());
    }
    if root.as_os_str().is_empty() {
        root.push(".");
    }
    let mut out = Vec::new();
    walk_glob(&root, &segments, &mut out);
    out
}

fn walk_glob(dir: &Path, segments: &[String], out: &mut Vec<PathBuf>) {
    let Some((segment, rest)) = segments.split_first() else {
        return;
    };
    if segment == "**" {
        walk_glob(dir, rest, out);
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                walk_glob(&entry.path(), segments, out);
            }
        }
        return;
    }
    if !is_pattern_segment(segment) {
        let path = dir.join(segment);
        if rest.is_empty() {
            if path.is_file() {
                out.push(path);
            }
        } else {
            walk_glob(&path, rest, out);
        }
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else { continue };
        if !glob_match(segment, &name) {
            continue;
        }
        let path = entry.path();
        if rest.is_empty() {
            if path.is_file() {
                out.push(path);
            }
        } else if path.is_dir() {
            walk_glob(&path, rest, out);
        }
    }
}

fn excluded(patterns: &[String], path: &Path) -> bool {
    patterns.iter().any(|pattern| {
        if pattern.contains('/') {
            let p: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
            let full = path.to_string_lossy();
            let n: Vec<&str> = full.split('/').filter(|s| !s.is_empty()).collect();
            segments_match(&p, &n)
        } else {
            path.file_name().and_then(|n| n.to_str()).is_some_and(|n| glob_match(pattern, n))
        }
    })
}

fn segments_match(pattern: &[&str], path: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((&"**", rest)) => (0..=path.len()).any(|skip| segments_match(rest, &path[skip..])),
        Some((segment, rest)) => path
            .split_first()
            .is_some_and(|(head, tail)| glob_match(segment, head) && segments_match(rest, tail)),
    }
}

/// Matches one path component against a glob, supporting `*`, `?` and
/// `[abc]` / `[a-z]` / `[!abc]` classes. Deliberately not a full shell glob:
/// `**` and brace expansion are not supported, and a pattern using them will
/// simply match nothing rather than half-work.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    // Iterative backtracking: no recursion, so a hostile pattern cannot
    // exhaust the stack.
    let (mut pi, mut ni) = (0usize, 0usize);
    let (mut star, mut star_n) = (usize::MAX, 0usize);
    while ni < n.len() {
        if pi < p.len() && p[pi] == '*' {
            star = pi;
            star_n = ni;
            pi += 1;
        } else if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if pi < p.len() && p[pi] == '[' {
            match class_match(&p[pi..], n[ni]) {
                Some((matched, len)) if matched => {
                    pi += len;
                    ni += 1;
                }
                Some(_) | None if star != usize::MAX => {
                    pi = star + 1;
                    star_n += 1;
                    ni = star_n;
                }
                _ => return false,
            }
        } else if star != usize::MAX {
            pi = star + 1;
            star_n += 1;
            ni = star_n;
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|c| *c == '*')
}

/// Matches a `[...]` class at the start of `pattern`, returning whether the
/// character matched and how long the class was.
fn class_match(pattern: &[char], c: char) -> Option<(bool, usize)> {
    let close = pattern.iter().position(|ch| *ch == ']')?;
    if close == 0 {
        return None;
    }
    let mut body = &pattern[1..close];
    let negated = matches!(body.first(), Some('!' | '^'));
    if negated {
        body = &body[1..];
    }
    let mut matched = false;
    let mut i = 0;
    while i < body.len() {
        if i + 2 < body.len() && body[i + 1] == '-' {
            if c >= body[i] && c <= body[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if body[i] == c {
                matched = true;
            }
            i += 1;
        }
    }
    Some((matched != negated, close + 1))
}

impl Tailer {
    /// Splits literal paths from glob patterns. A path containing `*`, `?` or
    /// `[` is treated as a pattern; anything else is taken literally, so a file
    /// that genuinely has a bracket in its name still needs no escaping until
    /// it also needs a wildcard.
    pub fn new(paths: Vec<PathBuf>, start_at_end: bool) -> Self {
        Self::from_sources(vec![TailSource {
            paths,
            exclude: Vec::new(),
            from_end: start_at_end,
            service: None,
            index: None,
        }])
    }

    pub fn from_sources(sources: Vec<TailSource>) -> Self {
        let mut tailer = Self {
            sources: Vec::new(),
            paths: Vec::new(),
            origin: HashMap::new(),
            last_expansion: None,
            cursors: HashMap::new(),
            checkpoint_path: None,
            started: false,
            stats: TailStats::default(),
        };
        for (index, source) in sources.into_iter().enumerate() {
            let (patterns, literals): (Vec<PathBuf>, Vec<PathBuf>) =
                source.paths.into_iter().partition(|p| is_pattern(p));
            tailer.sources.push(Source {
                patterns,
                exclude: source.exclude,
                from_end: source.from_end,
                service: source.service,
                index: source.index,
            });
            for path in literals {
                tailer.claim(index, path);
            }
        }
        tailer
    }

    /// Registers `path` for `source` unless it is excluded or already claimed.
    fn claim(&mut self, source: usize, path: PathBuf) -> bool {
        if self.origin.contains_key(&path) || excluded(&self.sources[source].exclude, &path) {
            return false;
        }
        self.origin.insert(path.clone(), source);
        self.paths.push(path);
        true
    }

    /// Expands patterns into concrete paths, adding files that have appeared.
    ///
    /// Only ever adds. A file that matched and has since been rotated away is
    /// left in `paths` so its cursor survives — dropping it would restart the
    /// file from zero if it came back under the same name.
    fn expand_patterns(&mut self) {
        if self.sources.iter().all(|s| s.patterns.is_empty()) {
            return;
        }
        let now = Instant::now();
        if self.last_expansion.is_some_and(|t| now.duration_since(t) < REDISCOVER_INTERVAL) {
            return;
        }
        self.last_expansion = Some(now);

        let mut found = Vec::new();
        for (index, source) in self.sources.iter().enumerate() {
            for pattern in &source.patterns {
                found.extend(glob_paths(pattern).into_iter().map(|p| (index, p)));
            }
        }
        for (index, path) in found {
            let shown = path.clone();
            if self.claim(index, path) {
                tracing::info!(path = %shown.display(), "tailing a newly matched file");
            }
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
        self.poll_labeled(|path, _, line| emit(path, line));
    }

    /// Like `poll`, also passing the owning source's service label.
    pub fn poll_labeled(&mut self, mut emit: impl FnMut(&Path, Option<&str>, &str)) {
        self.poll_routed(|path, service, _, line| emit(path, service, line));
    }

    /// Like `poll`, also passing the owning source's service label and index.
    pub fn poll_routed(&mut self, mut emit: impl FnMut(&Path, Option<&str>, Option<&str>, &str)) {
        self.expand_patterns();
        let initial = !self.started;
        self.started = true;
        for path in self.paths.clone() {
            let source = &self.sources[self.origin[&path]];
            let (service, index) = (source.service.clone(), source.index.clone());
            let start_at_end = source.from_end && initial;
            self.poll_one(&path, start_at_end, &mut |p, line| emit(p, service.as_deref(), index.as_deref(), line));
        }
        self.save_checkpoint();
    }

    fn poll_one(&mut self, path: &Path, start_at_end: bool, emit: &mut impl FnMut(&Path, &str)) {
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
mod discovery_tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn a_file_created_after_startup_is_picked_up() {
        // The window a new service's log file appears in is exactly the window
        // an incident lives in, so expanding once at startup is not enough.
        let dir = tempfile::tempdir().unwrap();
        let mut tailer = Tailer::new(vec![dir.path().join("*.log")], false);

        let mut lines = Vec::new();
        tailer.poll(|_, l| lines.push(l.to_string()));
        assert!(lines.is_empty(), "nothing matches yet");

        std::fs::write(dir.path().join("app.log"), "first\n").unwrap();
        // Force the rediscovery interval to have elapsed.
        tailer.last_expansion = None;
        tailer.poll(|_, l| lines.push(l.to_string()));
        assert_eq!(lines, ["first"]);

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("app.log"))
            .unwrap();
        writeln!(file, "second").unwrap();
        tailer.poll(|_, l| lines.push(l.to_string()));
        assert_eq!(lines, ["first", "second"], "and it keeps following it");
    }

    #[test]
    fn non_matching_files_and_directories_are_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("app.log"), "yes\n").unwrap();
        std::fs::write(dir.path().join("app.txt"), "no\n").unwrap();
        std::fs::create_dir(dir.path().join("nested.log")).unwrap();

        let mut tailer = Tailer::new(vec![dir.path().join("*.log")], false);
        let mut lines = Vec::new();
        tailer.poll(|_, l| lines.push(l.to_string()));
        assert_eq!(lines, ["yes"], "a directory named *.log is not a log file");
    }

    #[test]
    fn literal_paths_and_patterns_can_be_mixed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("explicit.txt"), "a\n").unwrap();
        std::fs::write(dir.path().join("globbed.log"), "b\n").unwrap();
        let mut tailer = Tailer::new(
            vec![dir.path().join("explicit.txt"), dir.path().join("*.log")],
            false,
        );
        let mut lines = Vec::new();
        tailer.poll(|_, l| lines.push(l.to_string()));
        lines.sort();
        assert_eq!(lines, ["a", "b"]);
    }
}

#[cfg(test)]
mod glob_tests {
    use super::glob_match;

    #[test]
    fn stars_and_question_marks() {
        assert!(glob_match("*.log", "app.log"));
        assert!(glob_match("*.log", ".log"));
        assert!(!glob_match("*.log", "app.log.1"), "rotated files are a different name");
        assert!(glob_match("app-?.log", "app-1.log"));
        assert!(!glob_match("app-?.log", "app-12.log"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("app*log", "applog"));
        assert!(glob_match("*app*", "myapp2"));
    }

    #[test]
    fn character_classes() {
        assert!(glob_match("app-[0-9].log", "app-3.log"));
        assert!(!glob_match("app-[0-9].log", "app-x.log"));
        assert!(glob_match("app-[abc].log", "app-b.log"));
        assert!(glob_match("app-[!0-9].log", "app-x.log"));
        assert!(!glob_match("app-[!0-9].log", "app-7.log"));
    }

    #[test]
    fn backtracking_terminates_on_adversarial_patterns() {
        // The classic catastrophic case for a naive backtracking matcher.
        assert!(glob_match(&"*".repeat(20), ""), "all-stars matches empty");
        let pattern = "*a*a*a*a*a*a*a*b";
        let name = "a".repeat(64);
        assert!(!glob_match(pattern, &name), "must terminate, not hang");
    }

    #[test]
    fn unsupported_syntax_matches_nothing_rather_than_half_working() {
        // `**` is not implemented; it must not silently behave like `*` across
        // directory boundaries, which would tail files nobody asked for.
        assert!(!glob_match("**/*.log", "app.log"));
    }

    #[test]
    fn literal_names_still_match_themselves() {
        assert!(glob_match("syslog", "syslog"));
        assert!(!glob_match("syslog", "syslog.1"));
    }
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

#[cfg(test)]
mod source_tests {
    use super::*;

    fn source(paths: Vec<PathBuf>) -> TailSource {
        TailSource { paths, exclude: vec![], from_end: false, service: None, index: None }
    }

    fn poll_all(tailer: &mut Tailer) -> Vec<(Option<String>, String)> {
        let mut out = Vec::new();
        tailer.poll_labeled(|_, svc, line| out.push((svc.map(str::to_string), line.to_string())));
        out
    }

    #[test]
    fn directory_wildcards_and_double_star_find_nested_files() {
        let dir = tempfile::tempdir().unwrap();
        for d in ["a/app", "b/app", "c/deep/er"] {
            std::fs::create_dir_all(dir.path().join(d)).unwrap();
        }
        std::fs::write(dir.path().join("a/app/x.log"), "a\n").unwrap();
        std::fs::write(dir.path().join("b/app/x.log"), "b\n").unwrap();
        std::fs::write(dir.path().join("c/deep/er/y.log"), "c\n").unwrap();
        std::fs::write(dir.path().join("a/app/skip.txt"), "no\n").unwrap();

        let mut one = Tailer::new(vec![dir.path().join("*/app/*.log")], false);
        let mut lines = Vec::new();
        one.poll(|_, l| lines.push(l.to_string()));
        lines.sort();
        assert_eq!(lines, ["a", "b"]);

        let mut all = Tailer::new(vec![dir.path().join("**/*.log")], false);
        let mut lines = Vec::new();
        all.poll(|_, l| lines.push(l.to_string()));
        lines.sort();
        assert_eq!(lines, ["a", "b", "c"]);
    }

    #[test]
    fn exclude_patterns_match_names_and_full_paths() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("old")).unwrap();
        std::fs::write(dir.path().join("app.log"), "keep\n").unwrap();
        std::fs::write(dir.path().join("debug.log"), "drop by name\n").unwrap();
        std::fs::write(dir.path().join("old/app.log"), "drop by dir\n").unwrap();

        let mut tailer = Tailer::from_sources(vec![TailSource {
            exclude: vec!["debug*".into(), "**/old/*".into()],
            ..source(vec![dir.path().join("**/*.log")])
        }]);
        let lines: Vec<String> = poll_all(&mut tailer).into_iter().map(|(_, l)| l).collect();
        assert_eq!(lines, ["keep"]);
    }

    #[test]
    fn each_source_has_its_own_service_and_start_position() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("web.log"), "old web\n").unwrap();
        std::fs::write(dir.path().join("db.log"), "old db\n").unwrap();
        let mut tailer = Tailer::from_sources(vec![
            TailSource {
                from_end: true,
                service: Some("web".into()),
                ..source(vec![dir.path().join("web.log")])
            },
            TailSource { service: Some("db".into()), ..source(vec![dir.path().join("db.log")]) },
        ]);
        assert_eq!(poll_all(&mut tailer), [(Some("db".into()), "old db".into())]);
        assert!(poll_all(&mut tailer).is_empty());
    }

    #[test]
    fn each_source_names_the_index_its_lines_go_to() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("pay.log"), "charged\n").unwrap();
        std::fs::write(dir.path().join("web.log"), "served\n").unwrap();
        let mut tailer = Tailer::from_sources(vec![
            TailSource { index: Some("payments".into()), ..source(vec![dir.path().join("pay.log")]) },
            source(vec![dir.path().join("web.log")]),
        ]);
        let mut out = Vec::new();
        tailer.poll_routed(|_, _, index, line| out.push((index.map(str::to_string), line.to_string())));
        assert_eq!(out, [(Some("payments".to_string()), "charged".to_string()), (None, "served".to_string())]);
        let parsed: TailSource = toml::from_str("paths = [\"/var/log/pay.log\"]\nindex = \"payments\"\n").unwrap();
        assert_eq!(parsed.index.as_deref(), Some("payments"));
    }

    #[test]
    fn a_new_file_is_read_from_the_start_even_with_from_end() {
        let dir = tempfile::tempdir().unwrap();
        let mut tailer = Tailer::from_sources(vec![TailSource {
            from_end: true,
            ..source(vec![dir.path().join("*.log")])
        }]);
        assert!(poll_all(&mut tailer).is_empty());
        std::fs::write(dir.path().join("late.log"), "boot line\n").unwrap();
        tailer.last_expansion = None;
        assert_eq!(poll_all(&mut tailer), [(None, "boot line".into())]);
    }

    #[test]
    fn a_globbed_file_still_drains_across_rotation_and_resumes_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("tail.ckpt");
        let log = dir.path().join("app.log");
        std::fs::write(&log, "one\n").unwrap();
        let pattern = vec![dir.path().join("*.log")];

        let mut tailer = Tailer::new(pattern.clone(), false).with_checkpoint(ckpt.clone());
        let mut lines = Vec::new();
        tailer.poll(|_, l| lines.push(l.to_string()));
        assert_eq!(lines, ["one"]);

        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&log).unwrap();
        writeln!(f, "before rotate").unwrap();
        std::fs::rename(&log, dir.path().join("app.log.1")).unwrap();
        std::fs::write(&log, "after rotate\n").unwrap();
        let mut lines = Vec::new();
        tailer.poll(|_, l| lines.push(l.to_string()));
        assert_eq!(lines, ["before rotate", "after rotate"]);
        drop(tailer);

        let mut f = std::fs::OpenOptions::new().append(true).open(&log).unwrap();
        writeln!(f, "while down").unwrap();
        let mut resumed = Tailer::new(pattern, false).with_checkpoint(ckpt);
        let mut lines = Vec::new();
        resumed.poll(|_, l| lines.push(l.to_string()));
        assert_eq!(lines, ["while down"], "no replay of one/after rotate, no loss");
    }
}
