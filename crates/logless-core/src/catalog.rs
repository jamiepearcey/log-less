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

/// Rows of one template in one service within a merged segment, by `observed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateSighting {
    pub template_id: u64,
    pub service: Option<String>,
    pub first_observed_nanos: i64,
    pub last_observed_nanos: i64,
}

/// One `(template_id, service)` pair of the exported dictionary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryEntry {
    pub template_id: u64,
    pub service: Option<String>,
    pub text: String,
    pub first_observed_nanos: i64,
    pub last_observed_nanos: i64,
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

            -- Which services each template has been seen in, and when (by the
            -- rows' `observed` time, nanoseconds). Written in the same
            -- transaction as the merge that produced the rows, so every
            -- (template_id, service) pair in Parquet has a row here. Feeds the
            -- exported dictionary (`template_dict`). `has_service` keeps a NULL
            -- service distinct from an empty one inside the primary key.
            CREATE TABLE IF NOT EXISTS template_services (
                template_id    INTEGER NOT NULL,
                has_service    INTEGER NOT NULL,
                service        TEXT    NOT NULL,
                first_observed INTEGER NOT NULL,
                last_observed  INTEGER NOT NULL,
                PRIMARY KEY (template_id, has_service, service)
            );

            -- Small key/value facts about this catalog. `template_id_space`
            -- names the namespace template ids are minted in: ids are a counter
            -- local to this catalog, so they mean nothing without it.
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- Promoted attribute keys (`crate::typed`). A column's type never
            -- changes once registered: every file that has the column agrees
            -- on it, which is what lets readers union schemas by name.
            -- Recoverable from file footers (`logless.typed_columns`).
            CREATE TABLE IF NOT EXISTS typed_columns (
                column_name TEXT PRIMARY KEY,
                attr_key    TEXT    NOT NULL UNIQUE,
                column_type TEXT    NOT NULL,
                service     TEXT    NOT NULL,
                promoted_at INTEGER NOT NULL
            );

            -- Parquet parts edges uploaded to object storage (`crate::parts`),
            -- central side. Named by content hash, so a row is never updated.
            -- Not recoverable from the local store; the bucket listing is its
            -- source of truth.
            CREATE TABLE IF NOT EXISTS peer_parts (
                name          TEXT PRIMARY KEY,
                size          INTEGER NOT NULL,
                rows          INTEGER NOT NULL,
                min_observed  INTEGER NOT NULL,
                max_observed  INTEGER NOT NULL,
                origin        TEXT    NOT NULL,
                committed_ms  INTEGER NOT NULL
            );
            "#,
        )?;
        // Minted once, with the templates table it qualifies. A lost catalog
        // restarts template ids at 1 and gets a new space with them, which is
        // exactly right: the old ids and the new ones are different namespaces.
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('template_id_space', ?1)",
            params![uuid::Uuid::now_v7().to_string()],
        )?;
        Ok(Self { conn })
    }

    /// The namespace this catalog's template ids are minted in (a UUID). Two
    /// agents can both hold template 7 for different shapes; `(agent, id)` is
    /// unambiguous.
    pub fn template_id_space(&self) -> Result<String, CatalogError> {
        Ok(self.conn.query_row(
            "SELECT value FROM meta WHERE key = 'template_id_space'",
            [],
            |r| r.get(0),
        )?)
    }

    /// Record one segment's merge output. Atomic: either the files and the
    /// segment marker both land, or neither does.
    pub fn commit_merge(
        &mut self,
        segment_id: u64,
        segment_uuid: uuid::Uuid,
        files: &[FileEntry],
        sightings: &[TemplateSighting],
        now_unix_secs: u64,
    ) -> Result<(), CatalogError> {
        let tx = self.conn.transaction()?;
        for s in sightings {
            tx.execute(
                "INSERT INTO template_services
                    (template_id, has_service, service, first_observed, last_observed)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(template_id, has_service, service) DO UPDATE SET
                    first_observed = MIN(first_observed, excluded.first_observed),
                    last_observed  = MAX(last_observed, excluded.last_observed)",
                params![
                    s.template_id as i64,
                    s.service.is_some(),
                    s.service.as_deref().unwrap_or(""),
                    s.first_observed_nanos,
                    s.last_observed_nanos,
                ],
            )?;
        }
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

    /// Records a part an edge uploaded. Returns false when the name was already recorded: a repeated
    /// commit is a no-op. Waits briefly for the writer, which shares this file.
    pub fn record_peer_part(&self, part: &crate::parts::CommittedPart, committed_ms: u64) -> Result<bool, CatalogError> {
        self.conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let n = self.conn.execute(
            "INSERT OR IGNORE INTO peer_parts (name, size, rows, min_observed, max_observed, origin, committed_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![part.name, part.size as i64, part.rows as i64, part.min_unix_nano as i64, part.max_unix_nano as i64, part.origin, committed_ms as i64],
        )?;
        Ok(n > 0)
    }

    pub fn peer_part_count(&self) -> Result<u64, CatalogError> {
        let n: i64 = self.conn.query_row("SELECT COUNT(*) FROM peer_parts", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    pub fn file_count(&self) -> Result<u64, CatalogError> {
        let n: i64 = self.conn.query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))?;
        Ok(n as u64)
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

    /// Writes the templates and `(template, service)` sightings a restore derived from the parts
    /// themselves, in one transaction (`crate::restore`). Merges first-seen/last-seen like a merge does.
    pub fn restore_templates(
        &mut self,
        templates: &[crate::drain::Template],
        sightings: &[TemplateSighting],
    ) -> Result<(), CatalogError> {
        self.save_templates(templates)?;
        let tx = self.conn.transaction()?;
        for s in sightings {
            tx.execute(
                "INSERT INTO template_services
                    (template_id, has_service, service, first_observed, last_observed)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(template_id, has_service, service) DO UPDATE SET
                    first_observed = MIN(first_observed, excluded.first_observed),
                    last_observed  = MAX(last_observed, excluded.last_observed)",
                params![s.template_id as i64, s.service.is_some(), s.service.as_deref().unwrap_or(""), s.first_observed_nanos, s.last_observed_nanos],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Every (service, template id) pair ever merged into the store, for the sync planner's "first time in this service".
    pub fn load_template_services(&self) -> Result<Vec<(Option<String>, u64)>, CatalogError> {
        let mut stmt = self.conn.prepare("SELECT template_id, has_service, service FROM template_services")?;
        let rows = stmt.query_map([], |row| {
            let has: bool = row.get(1)?;
            Ok((has.then(|| row.get::<_, String>(2)).transpose()?, row.get::<_, i64>(0)? as u64))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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

    /// One entry per `(template_id, service)` pair ever merged into the store,
    /// with the template's current text. Templates that no merged row carries
    /// yet are left out: nothing in Parquet can refer to them.
    pub fn template_dictionary(&self) -> Result<Vec<DictionaryEntry>, CatalogError> {
        let mut stmt = self.conn.prepare(
            "SELECT s.template_id, s.has_service, s.service, t.tokens,
                    s.first_observed, s.last_observed
             FROM template_services s JOIN templates t ON t.id = s.template_id
             ORDER BY s.template_id, s.has_service, s.service",
        )?;
        let rows = stmt.query_map([], |row| {
            let has_service: bool = row.get(1)?;
            let service: String = row.get(2)?;
            let tokens: String = row.get(3)?;
            Ok(DictionaryEntry {
                template_id: row.get::<_, i64>(0)? as u64,
                service: has_service.then_some(service),
                // Same rendering as `Template::text`: tokens are tab-joined in
                // storage and space-joined for display.
                text: tokens.split('\t').collect::<Vec<_>>().join(" "),
                first_observed_nanos: row.get(4)?,
                last_observed_nanos: row.get(5)?,
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

    /// The registered typed columns, as the layout a writer applies.
    pub fn typed_layout(&self) -> Result<crate::typed::TypedLayout, CatalogError> {
        let mut stmt = self.conn.prepare(
            "SELECT column_name, attr_key, column_type, service, promoted_at FROM typed_columns",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        let mut columns = Vec::new();
        for row in rows {
            let (column, key, ty, service, promoted_at) = row?;
            // An unknown type name can only come from a newer build; skipping
            // it keeps that key in the JSON, which is always correct.
            if let Some(ty) = crate::typed::ColumnType::parse(&ty) {
                columns.push(crate::typed::TypedColumn {
                    column,
                    key,
                    ty,
                    service,
                    promoted_at: promoted_at.max(0) as u64,
                });
            }
        }
        Ok(crate::typed::TypedLayout::new(columns))
    }

    /// Register promotions. Existing columns and keys are left exactly as
    /// they are — a type is never changed — so this is safe to repeat.
    pub fn register_typed_columns(
        &mut self,
        columns: &[crate::typed::TypedColumn],
    ) -> Result<(), CatalogError> {
        let tx = self.conn.transaction()?;
        for c in columns {
            tx.execute(
                "INSERT OR IGNORE INTO typed_columns (column_name, attr_key, column_type, service, promoted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![c.column, c.key, c.ty.as_str(), c.service, c.promoted_at as i64],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Reconstruct `files` by scanning the store and reading Parquet footers.
    ///
    /// Safe to run at any time; it is the recovery path for a lost or corrupt
    /// catalog, and the correctness check for one we still trust.
    ///
    /// Typed columns declared in footers are registered too (never removed:
    /// a registered column with no file yet is harmless, a forgotten one could
    /// be re-promoted with a different type).
    pub fn rebuild(&mut self, store_dir: &Path) -> Result<usize, CatalogError> {
        let mut entries = Vec::new();
        let mut typed = Vec::new();
        for path in store::list_parquet_files(store_dir) {
            let Some(key) = key_from_path(store_dir, &path) else {
                continue;
            };
            let stats = store::read_file_stats(&path).map_err(|source| CatalogError::Store {
                path: path.clone(),
                source,
            })?;
            typed.extend(store::read_typed_columns(&path).map_err(|source| {
                CatalogError::Store { path: path.clone(), source }
            })?);
            entries.push(FileEntry { path, key, stats });
        }
        self.register_typed_columns(&typed)?;

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
        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&entry), &[], 0).unwrap();
        assert!(cat.is_segment_merged(seg_uuid(1)).unwrap());
        assert_eq!(cat.list_files().unwrap(), vec![entry]);
        assert_eq!(cat.totals().unwrap().0, 10);
    }

    #[test]
    fn recommitting_a_segment_does_not_duplicate_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cat = Catalog::open(&tmp.path().join("catalog.sqlite")).unwrap();
        let entry = write_file(tmp.path(), 100, "info", 1, 10);

        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&entry), &[], 0).unwrap();
        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&entry), &[], 0).unwrap();

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
        cat.commit_merge(1, seg_uuid(1), &[a.clone(), b.clone()], &[], 0).unwrap();
        cat.commit_merge(2, seg_uuid(2), std::slice::from_ref(&c), &[], 0).unwrap();
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
    fn typed_registry_never_changes_a_type_and_rebuilds_from_footers() {
        use crate::model::{Attr, AttrValue};
        use crate::typed::{ColumnType, TypedColumn, TypedLayout};
        let col = |key: &str, ty| TypedColumn {
            column: crate::typed::column_name(key).unwrap(),
            key: key.into(),
            ty,
            service: "api".into(),
            promoted_at: 1,
        };
        let tmp = tempfile::tempdir().unwrap();
        let mut cat = Catalog::open(&tmp.path().join("catalog.sqlite")).unwrap();
        cat.register_typed_columns(&[col("latency_ms", ColumnType::Int64)]).unwrap();
        // A second registration with another type is ignored, not applied.
        cat.register_typed_columns(&[col("latency_ms", ColumnType::String)]).unwrap();
        assert_eq!(cat.typed_layout().unwrap().columns(), &[col("latency_ms", ColumnType::Int64)]);

        // A file declaring a column; a fresh catalog learns it from the footer.
        let key = PartitionKey { epoch_hour: 5, bucket: "info".into() };
        let path = key.dir(tmp.path()).join(store::file_name_for_segment(1, seg_uuid(1)));
        let mut r = LogRecord::new(5 * 3600 * 1_000_000_000, Severity::INFO, "x");
        r.observed_unix_nano = r.timestamp_unix_nano;
        r.attributes = vec![Attr { key: "ok".into(), value: AttrValue::Bool(true) }];
        let layout = TypedLayout::new(vec![col("ok", ColumnType::Bool)]);
        store::write_partition_file_with(&path, &[r], &layout).unwrap();

        let mut fresh = Catalog::open_in_memory().unwrap();
        fresh.rebuild(tmp.path()).unwrap();
        assert_eq!(fresh.typed_layout().unwrap().columns(), &[col("ok", ColumnType::Bool)]);
    }

    #[test]
    fn rebuild_is_idempotent_and_drops_stale_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let a = write_file(&store, 100, "info", 1, 10);
        let mut cat = Catalog::open_in_memory().unwrap();
        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&a), &[], 0).unwrap();

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
        cat.commit_merge(1, seg_uuid(1), &[a.clone(), b.clone()], &[], 0).unwrap();

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
        cat.commit_merge(1, seg_uuid(1), std::slice::from_ref(&early), &[], 0).unwrap();
        cat.commit_merge(2, seg_uuid(2), &[late.clone(), debug.clone()], &[], 0).unwrap();

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
