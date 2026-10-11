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
    /// `Template::lineage_id`: survives widening, unlike a hash of `text`.
    pub lineage_id: u64,
    pub first_observed_nanos: i64,
    pub last_observed_nanos: i64,
}

pub struct Catalog {
    conn: Connection,
}

/// One catalogued part (`peer_parts`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PeerPartRow {
    pub node: String,
    pub name: String,
    pub size: u64,
    pub rows: u64,
    pub min_unix_nano: u64,
    pub max_unix_nano: u64,
    pub origin: String,
    pub committed_ms: u64,
    /// `warm` or `cold`.
    pub tier: String,
    pub tier_ms: u64,
}

/// What [`Catalog::list_peer_parts`] selects. `None` matches everything.
#[derive(Debug, Clone, Default)]
pub struct PartsFilter {
    pub node: Option<String>,
    pub tier: Option<String>,
    pub from_nano: Option<u64>,
    pub to_nano: Option<u64>,
    /// Only parts whose newest data is older than this (the retention test).
    pub newest_before_nano: Option<u64>,
    pub limit: usize,
}

/// One change central made to a part after its commit (`peer_parts_audit`).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct PartsAuditRow {
    pub id: u64,
    pub at_ms: u64,
    pub action: String,
    pub node: String,
    pub name: String,
    pub tier: String,
    pub size: u64,
    pub outcome: String,
    pub detail: String,
}

const FILE_COLUMNS: &str = "path, tenant, idx, node, event_hour, bucket, rows, bytes, min_ts, max_ts, min_observed, max_observed";

fn read_file(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileEntry> {
    Ok(FileEntry {
        path: PathBuf::from(row.get::<_, String>(0)?),
        key: PartitionKey {
            tenant: row.get(1)?,
            index: row.get(2)?,
            node: row.get(3)?,
            epoch_hour: row.get::<_, i64>(4)? as u64,
            bucket: row.get(5)?,
        },
        stats: FileStats {
            rows: row.get::<_, i64>(6)? as u64,
            bytes: row.get::<_, i64>(7)? as u64,
            min_ts_nanos: row.get(8)?,
            max_ts_nanos: row.get(9)?,
            min_observed_nanos: row.get(10)?,
            max_observed_nanos: row.get(11)?,
        },
    })
}

/// The time-range query of [`Catalog::files_overlapping`]. `INDEXED BY` so it
/// is an error, not a silent table scan, if the index is ever dropped.
fn overlap_sql(level: &str) -> String {
    format!(
        "SELECT {FILE_COLUMNS} FROM files INDEXED BY files_by_ts
         WHERE tenant = ? AND idx = ? AND max_ts >= ? AND min_ts <= ?{level}
         ORDER BY min_ts, path"
    )
}

/// Inserts a file row, or replaces the numbers of the row already at its path.
fn upsert_file(conn: &Connection, f: &FileEntry) -> rusqlite::Result<usize> {
    conn.execute(
        "INSERT INTO files (path, tenant, idx, node, event_hour, bucket, rows, bytes, min_ts, max_ts, min_observed, max_observed)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(path) DO UPDATE SET
            rows = excluded.rows, bytes = excluded.bytes, min_ts = excluded.min_ts, max_ts = excluded.max_ts,
            min_observed = excluded.min_observed, max_observed = excluded.max_observed",
        params![
            f.path.to_string_lossy(),
            f.key.tenant,
            f.key.index,
            f.key.node,
            f.key.epoch_hour as i64,
            f.key.bucket,
            f.stats.rows as i64,
            f.stats.bytes as i64,
            f.stats.min_ts_nanos,
            f.stats.max_ts_nanos,
            f.stats.min_observed_nanos,
            f.stats.max_observed_nanos,
        ],
    )
}

fn read_peer_part(r: &rusqlite::Row<'_>) -> rusqlite::Result<PeerPartRow> {
    Ok(PeerPartRow {
        node: r.get(0)?,
        name: r.get(1)?,
        size: r.get::<_, i64>(2)? as u64,
        rows: r.get::<_, i64>(3)? as u64,
        min_unix_nano: r.get::<_, i64>(4)? as u64,
        max_unix_nano: r.get::<_, i64>(5)? as u64,
        origin: r.get(6)?,
        committed_ms: r.get::<_, i64>(7)? as u64,
        tier: r.get(8)?,
        tier_ms: r.get::<_, i64>(9)? as u64,
    })
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
        // `peer_parts` once keyed a part by name alone, under a flat prefix. Parts now belong to a node and
        // live under its prefix, so those rows name objects nobody can reach by their new keys: keep them
        // aside under another name and start the catalog afresh.
        let flat: bool = conn
            .query_row("SELECT COUNT(*) FROM pragma_table_info('peer_parts')", [], |r| r.get::<_, i64>(0))
            .map(|columns| columns > 0)
            .unwrap_or(false)
            && conn.query_row("SELECT COUNT(*) FROM pragma_table_info('peer_parts') WHERE name = 'node'", [], |r| r.get::<_, i64>(0))? == 0;
        if flat {
            conn.execute_batch("ALTER TABLE peer_parts RENAME TO peer_parts_flat_legacy")?;
        }
        // `files` from before tenants and indexes describes a tree this build
        // refuses to open (`partition::is_old_layout`). Its rows are derived
        // state, so the table is dropped and rebuilt from the new tree; the
        // tables that are not derivable (templates, peer_parts) are untouched.
        let old_files: bool = conn.query_row("SELECT COUNT(*) FROM pragma_table_info('files')", [], |r| r.get::<_, i64>(0))? > 0
            && conn.query_row("SELECT COUNT(*) FROM pragma_table_info('files') WHERE name = 'tenant'", [], |r| r.get::<_, i64>(0))? == 0;
        if old_files {
            conn.execute_batch("DROP TABLE files")?;
        }
        conn.execute_batch(
            r#"
            -- One row per Parquet file of this instance's own store
            -- (`tenant=/index=/hour=/level=/node=/<file>`). `event_hour` and
            -- `min_ts`/`max_ts` are event time (the row's `timestamp`), which
            -- is what a query's time range is on; `observed` is kept beside it.
            -- `idx` because "index" is a keyword.
            CREATE TABLE IF NOT EXISTS files (
                path           TEXT PRIMARY KEY,
                tenant         TEXT    NOT NULL,
                idx            TEXT    NOT NULL,
                node           TEXT    NOT NULL,
                event_hour     INTEGER NOT NULL,
                bucket         TEXT    NOT NULL,
                rows           INTEGER NOT NULL,
                bytes          INTEGER NOT NULL,
                min_ts         INTEGER NOT NULL,
                max_ts         INTEGER NOT NULL,
                min_observed   INTEGER NOT NULL,
                max_observed   INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS files_by_partition ON files (tenant, idx, event_hour, bucket);
            CREATE INDEX IF NOT EXISTS files_by_ts ON files (tenant, idx, max_ts, min_ts);

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
                last_seen  INTEGER NOT NULL,
                lineage_id INTEGER
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
            -- central side. A part belongs to the node whose token committed it
            -- and lives at `<prefix>/<node>/<name>`. Named by content hash, so
            -- only `tier` ever changes: 'warm' (uploaded; the node may still
            -- hold its local copy) or 'cold' (the node dropped its copy; the
            -- object is the only one). 'hot' is a node's local file that was
            -- never uploaded, so it has no row. Not recoverable from the local
            -- store; the bucket listing is its source of truth.
            CREATE TABLE IF NOT EXISTS peer_parts (
                node          TEXT NOT NULL,
                name          TEXT NOT NULL,
                size          INTEGER NOT NULL,
                rows          INTEGER NOT NULL,
                min_observed  INTEGER NOT NULL,
                max_observed  INTEGER NOT NULL,
                origin        TEXT    NOT NULL,
                committed_ms  INTEGER NOT NULL,
                tier          TEXT    NOT NULL DEFAULT 'warm' CHECK (tier IN ('warm', 'cold')),
                tier_ms       INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (node, name)
            );
            CREATE INDEX IF NOT EXISTS peer_parts_by_time ON peer_parts (max_observed, min_observed);

            -- Every change central makes to a part after it was committed:
            -- a tier move or an expiry (with the object delete's outcome).
            -- Append-only; a row outlives the part it describes.
            CREATE TABLE IF NOT EXISTS peer_parts_audit (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                at_ms   INTEGER NOT NULL,
                action  TEXT    NOT NULL,
                node    TEXT    NOT NULL,
                name    TEXT    NOT NULL,
                tier    TEXT    NOT NULL,
                size    INTEGER NOT NULL,
                outcome TEXT    NOT NULL,
                detail  TEXT    NOT NULL
            );
            "#,
        )?;
        Self::migrate_lineage(&conn)?;
        // Minted once, with the templates table it qualifies. A lost catalog
        // restarts template ids at 1 and gets a new space with them, which is
        // exactly right: the old ids and the new ones are different namespaces.
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('template_id_space', ?1)",
            params![uuid::Uuid::now_v7().to_string()],
        )?;
        Ok(Self { conn })
    }

    /// Additive migration: catalogs created before `lineage_id` get the column,
    /// and every template without one is given the fingerprint of its current
    /// text. That is the identity the Sentry forwarder already used for it, so
    /// the migration moves no existing issue; only widenings from here on are
    /// followed. The text it was born with is not recoverable. Idempotent.
    fn migrate_lineage(conn: &Connection) -> Result<(), CatalogError> {
        let has: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('templates') WHERE name = 'lineage_id'")?
            .exists([])?;
        if !has {
            conn.execute("ALTER TABLE templates ADD COLUMN lineage_id INTEGER", [])?;
        }
        let mut stmt = conn.prepare("SELECT id, text FROM templates WHERE lineage_id IS NULL ORDER BY id")?;
        let missing: Vec<(i64, String)> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?;
        if missing.is_empty() {
            return Ok(());
        }
        let mut used: std::collections::HashSet<i64> = conn
            .prepare("SELECT lineage_id FROM templates WHERE lineage_id IS NOT NULL")?
            .query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        for (id, text) in missing {
            let mut lineage = crate::drain::fingerprint_of(&text) as i64;
            if !used.insert(lineage) {
                lineage = crate::drain::fingerprint_of(&format!("{text}\u{1f}{id}")) as i64;
                used.insert(lineage);
            }
            conn.execute("UPDATE templates SET lineage_id = ?1 WHERE id = ?2", params![lineage, id])?;
        }
        Ok(())
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
            upsert_file(&tx, f)?;
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
    pub fn record_compaction(&mut self, output: &FileEntry, replaced: &[PathBuf]) -> Result<(), CatalogError> {
        let tx = self.conn.transaction()?;
        upsert_file(&tx, output)?;
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

    /// Every file row. For the operations that are about the whole store
    /// (reclaim's global ordering, `logless catalog`); a query for a time
    /// range goes through [`Catalog::files_overlapping`] instead.
    pub fn list_files(&self) -> Result<Vec<FileEntry>, CatalogError> {
        let mut stmt = self.conn.prepare(&format!("SELECT {FILE_COLUMNS} FROM files ORDER BY tenant, idx, event_hour, bucket, path"))?;
        let rows = stmt.query_map([], read_file)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Files of `(tenant, index)` whose event-time span overlaps `[from, to]`,
    /// optionally only in `buckets` (the level buckets that can hold the
    /// severities asked for, `LevelBuckets::names_from`). One indexed query
    /// (`files_by_ts`), oldest data first: the planning step for a time-range
    /// read, which never loads the table.
    pub fn files_overlapping(
        &self,
        tenant: &str,
        index: &str,
        from_nanos: i64,
        to_nanos: i64,
        buckets: Option<&[String]>,
    ) -> Result<Vec<FileEntry>, CatalogError> {
        // Bucket names are bound, never spliced: one placeholder each.
        let level = match buckets {
            Some(list) => format!(" AND bucket IN ({})", vec!["?"; list.len()].join(", ")),
            None => String::new(),
        };
        let mut stmt = self.conn.prepare(&overlap_sql(&level))?;
        let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(tenant.to_string()), Box::new(index.to_string()), Box::new(from_nanos), Box::new(to_nanos)];
        for b in buckets.unwrap_or_default() {
            args.push(Box::new(b.clone()));
        }
        let rows = stmt.query_map(rusqlite::params_from_iter(args.iter()), read_file)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// The files of one `(tenant, index, event hour, level)` partition, every
    /// writer's, by the `files_by_partition` index.
    pub fn files_in_partition(&self, key: &PartitionKey) -> Result<Vec<FileEntry>, CatalogError> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {FILE_COLUMNS} FROM files WHERE tenant = ?1 AND idx = ?2 AND event_hour = ?3 AND bucket = ?4 ORDER BY path"
        ))?;
        let rows = stmt.query_map(params![key.tenant, key.index, key.epoch_hour as i64, key.bucket], read_file)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Records a part `node` uploaded. Returns false when that node already committed the name: a repeated
    /// commit is a no-op. Waits briefly for the writer, which shares this file.
    pub fn record_peer_part(&self, node: &str, part: &crate::parts::CommittedPart, committed_ms: u64) -> Result<bool, CatalogError> {
        self.conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let n = self.conn.execute(
            "INSERT OR IGNORE INTO peer_parts (node, name, size, rows, min_observed, max_observed, origin, committed_ms, tier, tier_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'warm', ?8)",
            params![node, part.name, part.size as i64, part.rows as i64, part.min_unix_nano as i64, part.max_unix_nano as i64, part.origin, committed_ms as i64],
        )?;
        Ok(n > 0)
    }

    /// Moves one of `node`'s warm parts to cold (the node no longer holds its local copy) and audits it.
    /// Returns false when the node has no such part or it is cold already.
    pub fn mark_peer_part_cold(&mut self, node: &str, name: &str, at_ms: u64) -> Result<bool, CatalogError> {
        self.conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let tx = self.conn.transaction()?;
        let size: Option<i64> = tx
            .query_row("SELECT size FROM peer_parts WHERE node = ?1 AND name = ?2 AND tier = 'warm'", params![node, name], |r| r.get(0))
            .optional()?;
        let Some(size) = size else { return Ok(false) };
        tx.execute("UPDATE peer_parts SET tier = 'cold', tier_ms = ?3 WHERE node = ?1 AND name = ?2", params![node, name, at_ms as i64])?;
        tx.execute(
            "INSERT INTO peer_parts_audit (at_ms, action, node, name, tier, size, outcome, detail) VALUES (?1, 'tier', ?2, ?3, 'cold', ?4, 'ok', 'warm to cold: the node dropped its local copy')",
            params![at_ms as i64, node, name, size],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Catalogued parts, oldest data first. A part matches a time range when its observed-time span overlaps it.
    pub fn list_peer_parts(&self, filter: &PartsFilter) -> Result<Vec<PeerPartRow>, CatalogError> {
        let mut stmt = self.conn.prepare(
            "SELECT node, name, size, rows, min_observed, max_observed, origin, committed_ms, tier, tier_ms FROM peer_parts
             WHERE (?1 IS NULL OR node = ?1) AND (?2 IS NULL OR tier = ?2)
               AND (?3 IS NULL OR max_observed >= ?3) AND (?4 IS NULL OR min_observed <= ?4)
               AND (?6 IS NULL OR max_observed < ?6)
             ORDER BY max_observed, node, name LIMIT ?5",
        )?;
        let rows = stmt.query_map(
            params![filter.node, filter.tier, filter.from_nano.map(|v| v as i64), filter.to_nano.map(|v| v as i64), filter.limit as i64, filter.newest_before_nano.map(|v| v as i64)],
            read_peer_part,
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Up to `limit` parts in `(node, name)` order, strictly after `after`: one page of the listing a replica pages through.
    pub fn peer_parts_after(&self, after: Option<(&str, &str)>, limit: usize) -> Result<Vec<PeerPartRow>, CatalogError> {
        let mut stmt = self.conn.prepare(
            "SELECT node, name, size, rows, min_observed, max_observed, origin, committed_ms, tier, tier_ms FROM peer_parts
             WHERE (?1 IS NULL OR (node, name) > (?1, ?2)) ORDER BY node, name LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![after.map(|a| a.0), after.map(|a| a.1), limit as i64], read_peer_part)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// One catalogued part.
    pub fn peer_part(&self, node: &str, name: &str) -> Result<Option<PeerPartRow>, CatalogError> {
        let mut stmt = self.conn.prepare(
            "SELECT node, name, size, rows, min_observed, max_observed, origin, committed_ms, tier, tier_ms FROM peer_parts WHERE node = ?1 AND name = ?2",
        )?;
        Ok(stmt.query_row(params![node, name], read_peer_part).optional()?)
    }

    /// `(node, name, tier)` of the parts strictly after `after` and, when there is an `upto`, up to and including it.
    pub fn peer_part_keys(&self, after: Option<(&str, &str)>, upto: Option<(&str, &str)>) -> Result<Vec<(String, String, String)>, CatalogError> {
        let mut stmt = self.conn.prepare(
            "SELECT node, name, tier FROM peer_parts
             WHERE (?1 IS NULL OR (node, name) > (?1, ?2)) AND (?3 IS NULL OR (node, name) <= (?3, ?4)) ORDER BY node, name",
        )?;
        let rows = stmt.query_map(params![after.map(|a| a.0), after.map(|a| a.1), upto.map(|a| a.0), upto.map(|a| a.1)], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Records a part copied from another central, under the node that first uploaded it, with the tier the
    /// source had. `committed_ms` is when this catalog took it. Returns false when the part is already here
    /// (nothing changes); otherwise the audit row says where it came from.
    pub fn record_replicated_part(&mut self, part: &PeerPartRow, at_ms: u64, source: &str) -> Result<bool, CatalogError> {
        self.conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let tx = self.conn.transaction()?;
        let n = tx.execute(
            "INSERT OR IGNORE INTO peer_parts (node, name, size, rows, min_observed, max_observed, origin, committed_ms, tier, tier_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![part.node, part.name, part.size as i64, part.rows as i64, part.min_unix_nano as i64, part.max_unix_nano as i64, part.origin, at_ms as i64, part.tier, part.tier_ms as i64],
        )?;
        if n > 0 {
            tx.execute(
                "INSERT INTO peer_parts_audit (at_ms, action, node, name, tier, size, outcome, detail) VALUES (?1, 'replicate', ?2, ?3, ?4, ?5, 'ok', ?6)",
                params![at_ms as i64, part.node, part.name, part.tier, part.size as i64, format!("copied from {source}")],
            )?;
        }
        tx.commit()?;
        Ok(n > 0)
    }

    /// Records the outcome of expiring `part`: when the object is gone (`deleted`) the catalog row goes
    /// with it, in the same transaction as the audit row; when the delete failed the row stays and the
    /// audit row says why, so the next sweep tries again.
    pub fn finish_peer_part_expiry(&mut self, part: &PeerPartRow, at_ms: u64, detail: &str, deleted: bool) -> Result<(), CatalogError> {
        self.conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let tx = self.conn.transaction()?;
        if deleted {
            tx.execute("DELETE FROM peer_parts WHERE node = ?1 AND name = ?2", params![part.node, part.name])?;
        }
        tx.execute(
            "INSERT INTO peer_parts_audit (at_ms, action, node, name, tier, size, outcome, detail) VALUES (?1, 'expire', ?2, ?3, ?4, ?5, ?6, ?7)",
            params![at_ms as i64, part.node, part.name, part.tier, part.size as i64, if deleted { "deleted" } else { "delete_failed" }, detail],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// The most recent audit rows, newest first.
    pub fn peer_parts_audit(&self, limit: usize) -> Result<Vec<PartsAuditRow>, CatalogError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, at_ms, action, node, name, tier, size, outcome, detail FROM peer_parts_audit ORDER BY id DESC LIMIT ?1")?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok(PartsAuditRow {
                id: r.get::<_, i64>(0)? as u64,
                at_ms: r.get::<_, i64>(1)? as u64,
                action: r.get(2)?,
                node: r.get(3)?,
                name: r.get(4)?,
                tier: r.get(5)?,
                size: r.get::<_, i64>(6)? as u64,
                outcome: r.get(7)?,
                detail: r.get(8)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
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

    /// Drops the rows of files that were just deleted by name.
    pub fn forget_files(&mut self, paths: &[PathBuf]) -> Result<(), CatalogError> {
        let tx = self.conn.transaction()?;
        for path in paths {
            tx.execute("DELETE FROM files WHERE path = ?1", params![path.to_string_lossy()])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// [`Catalog::forget_missing_files`] for the partitions retention just
    /// expired: only their rows are read and their files checked, so a pass
    /// costs the partitions it touched and not the table.
    pub fn forget_missing_in(&mut self, partitions: &[PartitionKey]) -> Result<usize, CatalogError> {
        let mut gone = Vec::new();
        for key in partitions {
            gone.extend(self.files_in_partition(key)?.into_iter().filter(|e| !e.path.exists()).map(|e| e.path));
        }
        let tx = self.conn.transaction()?;
        for path in &gone {
            tx.execute("DELETE FROM files WHERE path = ?1", params![path.to_string_lossy()])?;
        }
        tx.commit()?;
        Ok(gone.len())
    }

    /// Persist the template dictionary, preserving ids.
    pub fn save_templates(&mut self, templates: &[crate::drain::Template]) -> Result<(), CatalogError> {
        let tx = self.conn.transaction()?;
        for t in templates {
            tx.execute(
                // `lineage_id` is written once and never updated: widening
                // changes `text`, not the identity.
                "INSERT INTO templates (id, text, tokens, count, first_seen, last_seen, lineage_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
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
                    t.lineage_id as i64,
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
            "SELECT id, tokens, count, first_seen, last_seen, lineage_id FROM templates ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            let tokens: String = row.get(1)?;
            Ok(crate::drain::Template {
                id: row.get::<_, i64>(0)? as u64,
                tokens: tokens.split('\t').map(str::to_string).collect(),
                count: row.get::<_, i64>(2)? as u64,
                first_seen_unix_secs: row.get::<_, i64>(3)? as u64,
                last_seen_unix_secs: row.get::<_, i64>(4)? as u64,
                lineage_id: row.get::<_, i64>(5)? as u64,
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
                    s.first_observed, s.last_observed, t.lineage_id
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
                lineage_id: row.get::<_, i64>(6)? as u64,
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
            // A Parquet file that is not at a partition leaf is not a file of
            // this store (somebody's scratch copy); it gets no row.
            let Some(key) = crate::partition::key_of_path(&path) else {
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
            upsert_file(&tx, e)?;
        }
        tx.commit()?;
        Ok(entries.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LevelBuckets;
    use crate::model::{LogRecord, Severity};

    /// Deterministic per-segment UUID so tests can re-derive the same paths.
    fn seg_uuid(segment: u64) -> uuid::Uuid {
        uuid::Uuid::from_u128(0xA000_0000_0000_0000_0000_0000_0000_0000u128 + segment as u128)
    }

    fn write_file(store: &Path, hour: u64, bucket: &str, segment: u64, n: usize) -> FileEntry {
        let key = PartitionKey::at(hour, bucket);
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
        let key = PartitionKey::at(5, "info");
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
        std::fs::remove_dir_all(a.key.level_dir(&store)).unwrap();
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

        std::fs::remove_dir_all(a.key.level_dir(&store)).unwrap();
        // Scoped to a partition that lost nothing: nothing is forgotten.
        assert_eq!(cat.forget_missing_in(std::slice::from_ref(&b.key)).unwrap(), 0);
        assert_eq!(cat.forget_missing_in(std::slice::from_ref(&a.key)).unwrap(), 1);
        assert_eq!(cat.forget_missing_files().unwrap(), 0);
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

        let (lo, hi) = (late.stats.min_ts_nanos, late.stats.max_ts_nanos);
        let window = cat.files_overlapping("default", "default", lo, hi, None).unwrap();
        assert_eq!(window.len(), 2, "hour 200 has two buckets");

        let only_info = cat.files_overlapping("default", "default", lo, hi, Some(&["info".to_string()])).unwrap();
        assert_eq!(only_info, vec![late.clone()]);
        // Warn and above, by the index's buckets: no such file in the window.
        let warn_up = LevelBuckets::default().names_from(Severity::WARN);
        assert_eq!(warn_up, ["warn_plus"]);
        assert!(cat.files_overlapping("default", "default", lo, hi, Some(&warn_up)).unwrap().is_empty());
        // Another tenant or index sees none of it.
        assert!(cat.files_overlapping("other", "default", lo, hi, None).unwrap().is_empty());
        assert!(cat.files_overlapping("default", "payments", lo, hi, None).unwrap().is_empty());
        assert_eq!(cat.files_in_partition(&late.key).unwrap(), vec![late]);
    }

    #[test]
    fn a_time_range_is_answered_from_the_event_time_index_not_a_table_scan() {
        let cat = Catalog::open_in_memory().unwrap();
        let plan: Vec<String> = cat
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {}", overlap_sql(" AND bucket IN (?)")))
            .unwrap()
            .query_map(params!["a", "b", 5, 9, "x"], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(plan.iter().any(|l| l.contains("USING INDEX files_by_ts")), "{plan:?}");
        assert!(plan.iter().any(|l| l.contains("tenant=? AND idx=? AND max_ts>?")), "the range is an index seek: {plan:?}");
    }

    #[test]
    fn event_time_and_observed_time_are_both_recorded_and_the_range_is_on_event_time() {
        // A replay: events of hour 100 observed in hour 300.
        let tmp = tempfile::tempdir().unwrap();
        let key = PartitionKey::at(100, "info");
        let path = key.dir(tmp.path()).join(store::file_name_for_segment(1, seg_uuid(1)));
        let hour = 3600 * 1_000_000_000u64;
        let records: Vec<_> = (0..5u64)
            .map(|i| {
                let mut r = LogRecord::new(100 * hour + i, Severity::INFO, "replayed");
                r.observed_unix_nano = 300 * hour + i;
                r
            })
            .collect();
        let stats = store::write_partition_file(&path, &records).unwrap();
        let mut cat = Catalog::open_in_memory().unwrap();
        cat.commit_merge(1, seg_uuid(1), &[FileEntry { path, key, stats }], &[], 0).unwrap();
        let at = |h: u64| cat.files_overlapping("default", "default", (h * hour) as i64, ((h + 1) * hour - 1) as i64, None).unwrap().len();
        assert_eq!((at(100), at(300)), (1, 0));
        let f = &cat.list_files().unwrap()[0];
        assert_eq!((f.stats.min_ts_nanos, f.stats.min_observed_nanos), ((100 * hour) as i64, (300 * hour) as i64));
    }

    #[test]
    fn a_files_table_from_before_tenants_is_dropped_and_the_parts_inventory_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE files (path TEXT PRIMARY KEY, epoch_hour INTEGER NOT NULL, bucket TEXT NOT NULL, rows INTEGER NOT NULL,
                    bytes INTEGER NOT NULL, min_observed INTEGER NOT NULL, max_observed INTEGER NOT NULL);
                 INSERT INTO files VALUES ('/s/hour=2026-01-01T00/level=info/a.parquet', 1, 'info', 1, 1, 1, 1);",
            )
            .unwrap();
        }
        let cat = Catalog::open(&path).unwrap();
        let part = crate::parts::CommittedPart { name: format!("{}.parquet", "a".repeat(64)), size: 1, sha256: "a".repeat(64), rows: 1, min_unix_nano: 1, max_unix_nano: 2, origin: "o".into(), node: None };
        assert!(cat.record_peer_part("edge-a", &part, 1).unwrap());
        assert_eq!(cat.file_count().unwrap(), 0);
        drop(cat);
        let again = Catalog::open(&path).unwrap();
        assert_eq!((again.file_count().unwrap(), again.peer_part_count().unwrap()), (0, 1));
    }

    #[test]
    fn lineage_is_stored_once_and_survives_widening_and_reload() {
        let mut cat = Catalog::open_in_memory().unwrap();
        let mut d = crate::drain::Drain::new(crate::drain::DrainConfig { similarity_threshold: 0.5, ..Default::default() });
        let id = d.add_line("job run alpha finished ok in cache", 1).unwrap().template_id;
        let born = d.template(id).unwrap().lineage_id;
        cat.save_templates(&d.templates().cloned().collect::<Vec<_>>()).unwrap();
        d.add_line("job run beta finished ok in cache", 2).unwrap();
        cat.save_templates(&d.templates().cloned().collect::<Vec<_>>()).unwrap();
        let t = &cat.load_templates().unwrap()[0];
        assert_eq!(t.text(), "job run <*> finished ok in cache");
        assert_eq!(t.lineage_id, born);
        assert_ne!(t.fingerprint(), born);
    }

    #[test]
    fn a_catalog_from_before_lineage_is_migrated_additively() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE templates (id INTEGER PRIMARY KEY, text TEXT NOT NULL, tokens TEXT NOT NULL,
                    count INTEGER NOT NULL, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL);
                 INSERT INTO templates VALUES (4, 'a <*> c', 'a\t<*>\tc', 9, 1, 2);",
            )
            .unwrap();
        }
        for _ in 0..2 {
            let cat = Catalog::open(&path).unwrap();
            let t = &cat.load_templates().unwrap()[0];
            assert_eq!((t.id, t.count), (4, 9), "existing rows untouched");
            assert_eq!(t.lineage_id, crate::drain::fingerprint_of("a <*> c"), "backfilled with today's issue identity");
        }
    }
}
