//! Local catalog — SQLite.
//!
//! **The catalog is derived state, never the source of truth.** Every row here
//! is recoverable from a Parquet footer plus the directory tree; delete the
//! file, corrupt it, or lose it to a crash and [`Catalog::rebuild`] reconstructs
//! it. That invariant is what keeps the engine choice reversible and what makes
//! a corrupt catalog a restart rather than an outage (`docs/architecture.md` §1).
//!
//! Consequently `synchronous = NORMAL` is correct here: paying for full fsync
//! durability on state we can regenerate would be paying twice.
//!
//! One thing rebuild cannot recover is `merged_segments`. That is safe because
//! Parquet file names are deterministic per WAL segment: re-merging an already
//! merged segment rewrites the identical path instead of duplicating rows.

use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};

use crate::partition::PartitionKey;
use crate::store::{self, FileStats};

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("catalog error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("catalog rebuild could not read {path}: {source}")]
    Store {
        path: PathBuf,
        #[source]
        source: crate::store::StoreError,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileEntry {
    pub path: PathBuf,
    pub key: PartitionKey,
    pub stats: FileStats,
}

pub struct Catalog {
    conn: Connection,
}

/// Reads `hour=…/level=…` back out of a path. The catalog is derived state, so
/// the path is the authority here, not the other way round.
fn partition_from_path(path: &Path) -> (u64, String) {
    let mut hour = 0;
    let mut bucket = String::new();
    for component in path.components() {
        let text = component.as_os_str().to_string_lossy();
        if let Some(value) = text.strip_prefix("hour=") {
            hour = super::partition::parse_hour(value).unwrap_or(0);
        } else if let Some(value) = text.strip_prefix("level=") {
            bucket = value.to_string();
        }
    }
    (hour, bucket)
}

impl Catalog {
    pub fn open(path: &Path) -> Result<Self, CatalogError> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self, CatalogError> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self, CatalogError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS files (
                path           TEXT PRIMARY KEY,
                epoch_hour     INTEGER NOT NULL,
                bucket         TEXT    NOT NULL,
                rows           INTEGER NOT NULL,
                bytes          INTEGER NOT NULL,
                min_observed   INTEGER NOT NULL,
                max_observed   INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS files_by_partition ON files (epoch_hour, bucket);
            CREATE INDEX IF NOT EXISTS files_by_time ON files (min_observed, max_observed);

            -- Keyed by the segment's UUID, never its numeric id: ids restart
            -- at 1 whenever the WAL drains, and keying on them made a fresh
            -- segment 1 look already-merged after a restart, so it was deleted
            -- unread. The id is retained for reporting only.
            CREATE TABLE IF NOT EXISTS merged_segments (
                segment_uuid TEXT PRIMARY KEY,
                segment_id   INTEGER NOT NULL,
                merged_at    INTEGER NOT NULL
            );

            -- Templates are the one piece of state that is NOT derivable from
            -- Parquet: ids must survive restarts or every downstream baseline
            -- and Sentry fingerprint shifts. Losing this table is recoverable
            -- (ids simply restart) but is a real degradation, not a no-op.
            CREATE TABLE IF NOT EXISTS templates (
                id         INTEGER PRIMARY KEY,
                text       TEXT    NOT NULL,
                tokens     TEXT    NOT NULL,
                count      INTEGER NOT NULL,
                first_seen INTEGER NOT NULL,
                last_seen  INTEGER NOT NULL
            );
            "#,
        )?;
        Ok(Self { conn })
    }

    /// Record one segment's merge output. Atomic: either the files and the
    /// segment marker both land, or neither does.
    pub fn commit_merge(
        &mut self,
        segment_id: u64,
        segment_uuid: uuid::Uuid,
        files: &[FileEntry],
        now_unix_secs: u64,
    ) -> Result<(), CatalogError> {
        let tx = self.conn.transaction()?;
        for f in files {
            tx.execute(
                "INSERT INTO files (path, epoch_hour, bucket, rows, bytes, min_observed, max_observed)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(path) DO UPDATE SET
                    rows = excluded.rows, bytes = excluded.bytes,
                    min_observed = excluded.min_observed, max_observed = excluded.max_observed",
                params![
                    f.path.to_string_lossy(),
                    f.key.epoch_hour as i64,
                    f.key.bucket,
                    f.stats.rows as i64,
                    f.stats.bytes as i64,
                    f.stats.min_observed_nanos,
                    f.stats.max_observed_nanos,
                ],
            )?;
        }
        tx.execute(
            "INSERT OR REPLACE INTO merged_segments (segment_uuid, segment_id, merged_at)
             VALUES (?1, ?2, ?3)",
            params![
                segment_uuid.to_string(),
                segment_id as i64,
                now_unix_secs as i64
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Records a compacted file and forgets the files it replaced.
    ///
    /// One transaction, because the intermediate state — the replacement
    /// recorded while its inputs are still listed — would double-count every
    /// row in that partition for anything reading the catalog's totals.
    pub fn record_compaction(
        &mut self,
        path: &Path,
        stats: &FileStats,
        replaced: &[PathBuf],
    ) -> Result<(), CatalogError> {
        let (epoch_hour, bucket) = partition_from_path(path);
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO files (path, epoch_hour, bucket, rows, bytes, min_observed, max_observed)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(path) DO UPDATE SET
                rows = excluded.rows, bytes = excluded.bytes,
                min_observed = excluded.min_observed, max_observed = excluded.max_observed",
            params![
                path.to_string_lossy(),
                epoch_hour as i64,
                bucket,
                stats.rows as i64,
                stats.bytes as i64,
                stats.min_observed_nanos,
                stats.max_observed_nanos,
            ],
        )?;
        for old in replaced {
            tx.execute("DELETE FROM files WHERE path = ?1", params![old.to_string_lossy()])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn is_segment_merged(&self, segment_uuid: uuid::Uuid) -> Result<bool, CatalogError> {
        let found: Option<String> = self
            .conn
            .query_row(
                "SELECT segment_uuid FROM merged_segments WHERE segment_uuid = ?1",
                params![segment_uuid.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    pub fn list_files(&self) -> Result<Vec<FileEntry>, CatalogError> {
        let mut stmt = self.conn.prepare(
            "SELECT path, epoch_hour, bucket, rows, bytes, min_observed, max_observed
             FROM files ORDER BY epoch_hour, bucket, path",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(FileEntry {
                path: PathBuf::from(row.get::<_, String>(0)?),
                key: PartitionKey {
                    epoch_hour: row.get::<_, i64>(1)? as u64,
                    bucket: row.get(2)?,
                },
                stats: FileStats {
                    rows: row.get::<_, i64>(3)? as u64,
                    bytes: row.get::<_, i64>(4)? as u64,
                    min_observed_nanos: row.get(5)?,
                    max_observed_nanos: row.get(6)?,
                },
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Files overlapping `[from, to]` on `observed`, optionally restricted to
    /// buckets. This is the pruning step the replay scanner rides on: it opens
    /// only the files that can contain matching rows.
    pub fn files_overlapping(
        &self,
        from_nanos: i64,
        to_nanos: i64,
        buckets: Option<&[String]>,
    ) -> Result<Vec<FileEntry>, CatalogError> {
        let all = self.list_files()?;
        Ok(all
            .into_iter()
            .filter(|f| f.stats.min_observed_nanos <= to_nanos && f.stats.max_observed_nanos >= from_nanos)
            .filter(|f| match buckets {
                Some(list) => list.contains(&f.key.bucket),
                None => true,
            })
            .collect())
    }

    pub fn totals(&self) -> Result<(u64, u64), CatalogError> {
        let (rows, bytes): (i64, i64) = self.conn.query_row(
            "SELECT COALESCE(SUM(rows), 0), COALESCE(SUM(bytes), 0) FROM files",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        Ok((rows as u64, bytes as u64))
    }

    /// Drop catalog rows for files that no longer exist — the counterpart to a
    /// retention sweep, which deletes directories without consulting SQLite.
    pub fn forget_missing_files(&mut self) -> Result<usize, CatalogError> {
        let entries = self.list_files()?;
        let gone: Vec<_> = entries
            .into_iter()
            .filter(|e| !e.path.exists())
            .map(|e| e.path)
            .collect();
        let tx = self.conn.transaction()?;
        for path in &gone {
            tx.execute(
                "DELETE FROM files WHERE path = ?1",
                params![path.to_string_lossy()],
            )?;
        }
        tx.commit()?;
        Ok(gone.len())
    }

    /// Persist the template dictionary, preserving ids.
    pub fn save_templates(&mut self, templates: &[crate::drain::Template]) -> Result<(), CatalogError> {
        let tx = self.conn.transaction()?;
        for t in templates {
            tx.execute(
                "INSERT INTO templates (id, text, tokens, count, first_seen, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(id) DO UPDATE SET
                    text = excluded.text, tokens = excluded.tokens,
                    count = excluded.count, last_seen = excluded.last_seen",
                params![
                    t.id as i64,
                    t.text(),
                    // Tab-joined: tokens never contain whitespace, having been
                    // split on it.
                    t.tokens.join("\t"),
                    t.count as i64,
                    t.first_seen_unix_secs as i64,
                    t.last_seen_unix_secs as i64,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn load_templates(&self) -> Result<Vec<crate::drain::Template>, CatalogError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, tokens, count, first_seen, last_seen FROM templates ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            let tokens: String = row.get(1)?;
            Ok(crate::drain::Template {
                id: row.get::<_, i64>(0)? as u64,
                tokens: tokens.split('\t').map(str::to_string).collect(),
                count: row.get::<_, i64>(2)? as u64,
                first_seen_unix_secs: row.get::<_, i64>(3)? as u64,
                last_seen_unix_secs: row.get::<_, i64>(4)? as u64,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Templates ordered by how often they have been seen.
    pub fn top_templates(&self, limit: usize) -> Result<Vec<crate::drain::Template>, CatalogError> {
        let mut all = self.load_templates()?;
        all.sort_by(|a, b| b.count.cmp(&a.count));
        all.truncate(limit);
        Ok(all)
    }

    /// Reconstruct `files` by scanning the store and reading Parquet footers.
    ///
    /// Safe to run at any time; it is the recovery path for a lost or corrupt
    /// catalog, and the correctness check for one we still trust.
    pub fn rebuild(&mut self, store_dir: &Path) -> Result<usize, CatalogError> {
        let mut entries = Vec::new();
        for path in store::list_parquet_files(store_dir) {
            let Some(key) = key_from_path(store_dir, &path) else {
                continue;
            };
            let stats = store::read_file_stats(&path).map_err(|source| CatalogError::Store {
                path: path.clone(),
                source,
            })?;
            entries.push(FileEntry { path, key, stats });
        }

        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM files", [])?;
        for e in &entries {
            tx.execute(
                "INSERT INTO files (path, epoch_hour, bucket, rows, bytes, min_observed, max_observed)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    e.path.to_string_lossy(),
                    e.key.epoch_hour as i64,
                    e.key.bucket,
                    e.stats.rows as i64,
                    e.stats.bytes as i64,
                    e.stats.min_observed_nanos,
                    e.stats.max_observed_nanos,
                ],
            )?;
        }
        tx.commit()?;
        Ok(entries.len())
    }
}

/// Recover a partition key from a file's location under the store root.
fn key_from_path(store_dir: &Path, path: &Path) -> Option<PartitionKey> {
    let rel = path.strip_prefix(store_dir).ok()?;
    let mut parts = rel.components();
    let hour = parts.next()?.as_os_str().to_str()?;
    let level = parts.next()?.as_os_str().to_str()?;
    crate::partition::parse_partition_dirs(hour, level)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{LogRecord, Severity};

    /// Deterministic per-segment UUID so tests can re-derive the same paths.
    fn seg_uuid(segment: u64) -> uuid::Uuid {
        uuid::Uuid::from_u128(0xA000_0000_0000_0000_0000_0000_0000_0000u128 + segment as u128)
    }

    fn write_file(store: &Path, hour: u64, bucket: &str, segment: u64, n: usize) -> FileEntry {
        let key = PartitionKey {
            epoch_hour: hour,
            bucket: bucket.into(),
        };
        let path = key
            .dir(store)
            .join(store::file_name_for_segment(segment, seg_uuid(segment)));
        let base = hour * 3600 * 1_000_000_000;
        let records: Vec<_> = (0..n)
            .map(|i| {
                let mut r = LogRecord::new(base + i as u64, Severity::INFO, "hello world");
                r.observed_unix_nano = base + i as u64;
                r
            })
            .collect();
        let stats = store::write_partition_file(&path, &records).unwrap();
        FileEntry { path, key, stats }
    }

    #[test]
    fn commit_is_atomic_and_marks_the_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cat = Catalog::open(&tmp.path().join("catalog.sqlite")).unwrap();
        let entry = write_file(tmp.path(), 100, "info", 1, 10);

        assert!(!cat.is_segment_merged(seg_uuid(1)).unwrap());
        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&entry), 0).unwrap();
        assert!(cat.is_segment_merged(seg_uuid(1)).unwrap());
        assert_eq!(cat.list_files().unwrap(), vec![entry]);
        assert_eq!(cat.totals().unwrap().0, 10);
    }

    #[test]
    fn recommitting_a_segment_does_not_duplicate_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cat = Catalog::open(&tmp.path().join("catalog.sqlite")).unwrap();
        let entry = write_file(tmp.path(), 100, "info", 1, 10);

        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&entry), 0).unwrap();
        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&entry), 0).unwrap();

        assert_eq!(cat.list_files().unwrap().len(), 1);
        assert_eq!(cat.totals().unwrap(), (10, entry.stats.bytes));
    }

    #[test]
    fn rebuild_reconstructs_everything_from_parquet() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let a = write_file(&store, 100, "info", 1, 10);
        let b = write_file(&store, 100, "debug", 1, 25);
        let c = write_file(&store, 101, "info", 2, 5);

        let mut cat = Catalog::open(&tmp.path().join("catalog.sqlite")).unwrap();
        cat.commit_merge(1, seg_uuid(1), &[a.clone(), b.clone()], 0).unwrap();
        cat.commit_merge(2, seg_uuid(2), std::slice::from_ref(&c), 0).unwrap();
        let before = cat.list_files().unwrap();

        // Lose the catalog entirely.
        drop(cat);
        std::fs::remove_file(tmp.path().join("catalog.sqlite")).unwrap();
        let mut rebuilt = Catalog::open(&tmp.path().join("catalog.sqlite")).unwrap();
        assert_eq!(rebuilt.list_files().unwrap().len(), 0);

        assert_eq!(rebuilt.rebuild(&store).unwrap(), 3);
        assert_eq!(rebuilt.list_files().unwrap(), before);
    }

    #[test]
    fn rebuild_is_idempotent_and_drops_stale_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let a = write_file(&store, 100, "info", 1, 10);
        let mut cat = Catalog::open_in_memory().unwrap();
        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&a), 0).unwrap();

        assert_eq!(cat.rebuild(&store).unwrap(), 1);
        assert_eq!(cat.rebuild(&store).unwrap(), 1);

        // Retention removes the directory; rebuild must not resurrect the row.
        std::fs::remove_dir_all(a.key.dir(&store)).unwrap();
        assert_eq!(cat.rebuild(&store).unwrap(), 0);
        assert_eq!(cat.list_files().unwrap().len(), 0);
    }

    #[test]
    fn forgets_files_deleted_by_retention() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let a = write_file(&store, 100, "debug", 1, 10);
        let b = write_file(&store, 100, "info", 1, 10);
        let mut cat = Catalog::open_in_memory().unwrap();
        cat.commit_merge(1, seg_uuid(1), &[a.clone(), b.clone()], 0).unwrap();

        std::fs::remove_dir_all(a.key.dir(&store)).unwrap();
        assert_eq!(cat.forget_missing_files().unwrap(), 1);
        assert_eq!(cat.list_files().unwrap(), vec![b]);
    }

    #[test]
    fn templates_round_trip_with_their_ids() {
        let mut cat = Catalog::open_in_memory().unwrap();
        let mut drain = crate::drain::Drain::default();
        drain.add_line("user 42 logged in from 10.0.0.1", 100).unwrap();
        drain.add_line("payment 99 failed for order 7", 100).unwrap();
        drain.add_line("user 77 logged in from 10.0.0.9", 200).unwrap();

        let before: Vec<_> = {
            let mut t: Vec<_> = drain.templates().cloned().collect();
            t.sort_by_key(|t| t.id);
            t
        };
        cat.save_templates(&before).unwrap();

        let after = cat.load_templates().unwrap();
        assert_eq!(after, before, "ids, tokens and counts must survive");

        // Saving again updates rather than duplicating.
        cat.save_templates(&before).unwrap();
        assert_eq!(cat.load_templates().unwrap().len(), before.len());

        // And a restored Drain keeps assigning the same ids.
        let mut restored = crate::drain::Drain::restore(Default::default(), after);
        let m = restored.add_line("user 5 logged in from 10.0.0.5", 300).unwrap();
        assert_eq!(m.template_id, before[0].id);
        assert!(!m.is_new);
    }

    #[test]
    fn prunes_files_by_time_and_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let early = write_file(&store, 100, "info", 1, 10);
        let late = write_file(&store, 200, "info", 2, 10);
        let debug = write_file(&store, 200, "debug", 2, 10);

        let mut cat = Catalog::open_in_memory().unwrap();
        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&early), 0).unwrap();
        cat.commit_merge(2, seg_uuid(2), &[late.clone(), debug.clone()], 0).unwrap();

        let window = cat
            .files_overlapping(late.stats.min_observed_nanos, late.stats.max_observed_nanos, None)
            .unwrap();
        assert_eq!(window.len(), 2, "hour 200 has two buckets");

        let only_info = cat
            .files_overlapping(
                late.stats.min_observed_nanos,
                late.stats.max_observed_nanos,
                Some(&["info".to_string()]),
            )
            .unwrap();
        assert_eq!(only_info, vec![late]);
    }
}
