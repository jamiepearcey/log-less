<!-- Status: PROPOSED (2026-10-11). Consultant design recommendation, not yet accepted by the owner and not built.
     Numbers in section 8 are estimates. Owner decisions are listed in section 10. Tracked in Plane (LOG project). -->

# Multi-tenancy and indexing for log-less: design recommendation

Read-only review of `main` at `/Users/jamiepearcey/projects/infrastructure/log-less-wt-integ` and the console at `/Users/jamiepearcey/projects/logless-studio`. Everything marked "verified" was read in code; everything marked "estimate" was not measured. Note: `docs/evidence/s3-read-bytes.md` does not exist in the log-less repo; it is at `/Users/jamiepearcey/projects/logless-studio/docs/evidence/s3-read-bytes.md` and that is what I read.

## 0. Summary of the recommendation

One layout, one tree, one catalog table, one read path, everywhere (edge, central, replica):

```
<root>/tenant=<t>/index=<i>/hour=<event hour>/level=<bucket>/node=<writer>/<file>.parquet
<root>/tenant=<t>/index=<i>/_meta/manifest/day=<YYYY-MM-DD>/writer=<writer>.pq
<root>/tenant=<t>/index=<i>/_meta/templates/writer=<writer>.pq
```

- **Tenant and index are real dimensions** (path, retention, quota, read scope, catalog key). **Node is the ownership/writer dimension** (path leaf, catalog key, no coordination between writers). **Stream, service, template, trace are attributes** (columns, Parquet stats, bloom filters, the spine).
- **Event-hour partitioning, 8k row groups, sort `(service, trace_id, timestamp, event_id)`**: adopt ADR 0002 unchanged, in the same change as the new path, since both touch the same five files.
- **Parts keep SHA-256 names** but live in the partition directory, not in a flat `parts/<node>/` tree. Central's own record-ingest store joins the same tree as `node=<central id>`; the separate `parts` bucket and `peer_parts` table disappear.
- **The catalog stays SQLite per writer** (private, rebuildable), and is **exported as per-writer Parquet manifests** under `_meta/` so the console plans from one small file instead of listing prefixes. Own format, not Iceberg/Delta.
- **Template ids become content hashes** (the existing `fingerprint()`/`global_id`), so ids agree across nodes, tenants and writers without a dictionary service.
- **Writer scale-out is "N ordinary instances, each owning its `node=` leaves and its manifests"**, routed by tenant. Nothing is shared and nothing is coordinated; the price is that without an external bucket the console unions N endpoints.
- **Tenant isolation enforceable now**: tenant comes from the token (never the body), S3 read credentials are scoped to a `tenant=<t>/` prefix, retention and quota are per `(tenant, index)`, offboarding is a prefix delete. RBAC/SSO stay in the console.
- **First slice: the layout change at the edge** (path dimensions + event hour + 8k row groups). Everything else keys off the path.

Narrow pushdown is **not required** by this design. Manifests give the console pruning; pushdown remains the separate cost question `s3-read-bytes.md` already answered for aggregate shapes.

---

## 1. Logical model

| Concept | Kind | Unit of | Where it lives |
|---|---|---|---|
| **Tenant** | dimension | isolation, access control, quota, offboarding/erasure, encryption boundary (storage-level) | path (first), catalog key, token binding, S3 reader credential |
| **Index** (dataset) | dimension | retention policy, typed-column schema, read scope within a tenant, sub-quota | path (second), catalog key, per-node allow-list |
| **Node** | dimension (ownership) | write authorisation, accounting, restore, compaction scope | path (leaf), catalog key, token identity |
| **Event hour, level** | dimension | pruning (hour), retention granularity (level) | path |
| **Service, template, trace, span, origin node, stream/source** | attribute | nothing; they are predicates | Parquet columns + row-group stats + bloom filters; service also in a catalog side table; template in the spine |

Rules that make this hold:

1. **Tenant is never a claim.** It comes from the node token (`[[peer.nodes]] tenant`), exactly as node id does today (ADR 0001). A record or a part request that names another tenant is 403, the same `foreign_prefix` rule, one level up.
2. **Index comes from the record, checked against the node's allow-list.** Source: HEC `index` field (today kept as an attribute and ignored, `docs/tasks/current.md:241`), OTLP resource attribute `logless.index`, `[[tail]] index`, Sentry project mapping. Unknown or disallowed index: refuse (HEC code 7) or route to the tenant's default index, by config (`unknown_index = "refuse" | "default"`). Default index name `default`.
3. **One node, one tenant.** An edge that genuinely serves two tenants runs two agents. Many indexes per node is normal.
4. **Stream (source file, container, pod) is an attribute.** Splunk `source`/`sourcetype` map to attributes, not partitions; the typed-column promoter already turns frequent ones into `attr_*` columns.

**Rejected: index as the isolation unit (Splunk's model).** Splunk makes the index both the retention unit and the permission boundary, which forces one index per team per retention class and explodes file counts. Here the tenant is the hard boundary and the index is the soft one; a tenant can have three indexes with three retentions without three credentials.

**Rejected: node as a top-level partition** (`node=/tenant=/...`). Reads almost never filter by node; listing a day would cost one listing per node. Node at the leaf gives the same hive column for free and one listing per day prefix.

**Rejected: no node in the path at all** (node only in the catalog). Loses coordination-free multi-writer (two writers in one leaf directory need the catalog to agree who owns what), loses the ADR 0001 security property that an upload token can only write under its own prefix, and loses per-node restore from a directory.

---

## 2. Physical layout

### 2.1 Paths

```
# edge-1 (tenant acme), its own store
/var/lib/logless/store/tenant=acme/index=default/hour=2026-10-11T14/level=warn_plus/node=edge-1/s000000000042-018f3c2a9b1e.parquet
/var/lib/logless/store/tenant=acme/index=default/_meta/templates/writer=edge-1.pq
/var/lib/logless/store/tenant=acme/index=default/_meta/manifest/day=2026-10-11/writer=edge-1.pq

# central-1, local backend: uploaded parts and its own record-ingest files in ONE tree
/var/lib/logless/store/tenant=acme/index=default/hour=2026-10-11T14/level=warn_plus/node=edge-1/3f9c...e2a1.parquet     (uploaded part, sha name)
/var/lib/logless/store/tenant=acme/index=default/hour=2026-10-11T14/level=warn_plus/node=central-1/s000000000917-....parquet  (records via /v1/ingest)
/var/lib/logless/store/tenant=acme/index=default/_meta/manifest/day=2026-10-11/writer=central-1.pq
/var/lib/logless/store/tenant=acme/index=default/_meta/manifest/day=2026-10-11/writer=edge-1.pq   (copied up with the parts, see 3.4)

# S3 backend: same keys under <prefix>/
s3://logless-parts/edges/tenant=acme/index=default/hour=2026-10-11T14/level=warn_plus/node=edge-1/3f9c...e2a1.parquet
```

Identifier alphabet for `tenant`, `index`, `node`: `A-Za-z0-9._-`, 1 to 64 chars, not starting with `.`, unique ignoring case (the existing `valid_node` rule, `crates/logless-core/src/parts.rs`), **and must contain at least one ASCII letter**. Reason: DuckDB `hive_partitioning=true` autocasts `tenant=1234` to BIGINT and the console's string predicates then fail silently. The console should also pass `hive_types={'tenant':VARCHAR,'index':VARCHAR,'node':VARCHAR,'hour':VARCHAR,'level':VARCHAR}` defensively.

### 2.2 Partition keys and order: `tenant, index, hour(event), level, node`

- `tenant` first: read scoping is a prefix check, retention/quota/offboarding are subtree operations, S3 lifecycle rules per tenant.
- `index` second: retention is per `(tenant, index, level)`, so `hour=/level=` sits under it and retention stays "delete a directory".
- `hour` on **event time** (`timestamp`), per ADR 0002. Keep the key name `hour` (the console expects it), document that it means event hour. `observed` stays a column.
- `level` before `node`: retention deletes `hour=X/level=Y/` recursively and takes every node's files for that partition in one `remove_dir_all`.
- `node` last: ownership, no coordination, hive column for free, one listing per day covers all nodes.

### 2.3 File naming

- **Edge hot files** keep the deterministic segment name `s<segment>-<uuid12>.parquet` (`store.rs:60-63`). It is what makes a re-merge after a crash idempotent; content-addressed names at merge time would turn a crash between file write and catalog commit into duplicate rows under two names. Compaction output keeps `s000000000000-<uuid>`.
- **Uploaded parts** keep `<sha256>.parquet`, assigned at upload (`parts.rs:1524 name_for(&sha256)`), stored in the **same partition directory** the file came from. The relative directory is carried in `presign`/`commit` as `{tenant?, index, event_hour, level}`; central validates each segment and builds the key itself (tenant from the token, node from the token). Same bytes from two nodes remain two objects (`.../node=edge-1/<sha>` and `.../node=edge-2/<sha>`), so `already_present` leaks nothing across nodes, as ADR 0001 requires.
- A part is immutable; nothing in this design ever rewrites a part. Compaction at the edge produces a new file (new sha) and retires the old ones, as today.

### 2.4 Row groups, sort, encodings, bloom filters

- `ROW_GROUP_ROWS = 8 * 1024` (`store.rs:25`, today 64k). ADR 0002: +2.7% bytes, Q3/Q4 3.4-3.6x fewer bytes.
- Sort `(service, trace_id, timestamp, event_id)` (`schema.rs:120-130` today uses `observed` as key 3). ADR 0002 says switching to `timestamp` is free inside an event-hour file; do it.
- Keep zstd(3), dictionary on, DELTA_BINARY_PACKED on both timestamps.
- **Bloom filters on `trace_id` and `event_id`**, per row group, FPP 0.01. arrow-rs supports `set_bloom_filter_enabled`. Measure before relying on it: the sort-layout bench could not test blooms because DuckDB 1.5.2 returned wrong rows on BLOB equality (`sort-layout-bench.md`, "Fidelity limits"); the console works around it with `hex(trace_id)`, which defeats blooms. This is a console/DuckDB defect to chase, not a layout question.
- Statistics stay `EnabledStatistics::Chunk`; add **page indexes** (`set_column_index_truncate_length`, offset/column index) so `timestamp` within an 8k group prunes at page level for DuckDB. Cheap; not measured.

### 2.5 New columns (schema version 3)

| column | type | why |
|---|---|---|
| `template_id` | UInt64 | **semantics change**: FNV-1a 64 of the masked template text at write time (today's `Template::fingerprint()`), global across nodes. See section 6 |
| `origin` | Utf8, dictionary | the edge that produced the row, promoted from the `logless.origin` attribute (`peer.rs:93`). Distinct from the hive `node` column, which is the writer/owner of the *file*; they differ for records relayed through `/v1/ingest` and for any future cross-node recompaction |
| `index` | not a column | hive key only |

Everything else unchanged. `schema_version` footer key bumps to `"3"` (`schema.rs:28`).

### 2.6 What goes where

| Fact | Path | Catalog | Parquet stats/blooms | Manifest |
|---|---|---|---|---|
| tenant, index, event hour, level, node | yes | yes | (hive) | yes |
| file name / sha256, size, rows | | yes | | yes |
| min/max `timestamp`, min/max `observed` | | yes | row-group min/max | yes |
| distinct services in the file (few) | | side table | row-group min/max on sorted `service` | list column |
| template ids | | **no** (see 3.2) | 8k groups partially cluster via service sort | no |
| trace_id / event_id | | no | bloom filters | no |
| tier (hot/warm/cold), committed_ms, origin, audit | | yes | | tier only |
| typed-column registry | | yes (per tenant, index) | field metadata in each file | no |

### 2.7 Write fan-out under event-time partitioning (the Plane 74 item)

Verified: `merge.rs:126-133` writes one file per `(segment, partition)`; ADR 0002 measured 7-10x more pre-compaction files with a replay. Mechanism to add, in `merge.rs`:

- Rows whose event hour is within `late_threshold` (default 2 h) of now are written directly, as today.
- Rows older than that go to a **late spool**: a second WAL directory `wal-late/` with its own segment, rolled by *time* (default 30 min) or size (64 MB), not by the main WAL's roll. Its merge is the same code path. Effect: a replay spread over 24 h × 3 levels makes 72 files every 30 min instead of 72 per 5-minute main segment (12x fewer at the defaults), and the normal live path is untouched. Durability is unchanged (same fsync policy; the late WAL is just another WAL).
- `POST /admin/v1/sync/backfill` and `logless restore` know their ranges and should write partition-whole files directly, bypassing both.
- Compaction must stop skipping "the active hour" by wall clock (`compact.rs:120`, fed `now/3600` at `main.rs:2250`): under event time any partition can gain a late file. Replace with "skip a partition whose newest file is younger than `compact_quiet` (default 10 min)"; the write-record-unlink order already makes a race safe.

---

## 3. The catalog as the index

### 3.1 Schema (SQLite, per writer, replaces `files` + `peer_parts`)

```sql
CREATE TABLE parts (
  tenant        TEXT    NOT NULL,
  idx           TEXT    NOT NULL,            -- "index" is a keyword
  event_hour    INTEGER NOT NULL,
  level         TEXT    NOT NULL,
  node          TEXT    NOT NULL,            -- owner of the file (hive node=)
  name          TEXT    NOT NULL,            -- file name: s<seg>-<uuid> (hot) or <sha256>.parquet
  sha256        TEXT,                        -- NULL until the file is closed and hashed
  size          INTEGER NOT NULL,
  rows          INTEGER NOT NULL,
  min_ts        INTEGER NOT NULL,            -- event time, ns
  max_ts        INTEGER NOT NULL,
  min_observed  INTEGER NOT NULL,
  max_observed  INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  tier          TEXT    NOT NULL CHECK (tier IN ('hot','warm','cold')),
  tier_ms       INTEGER NOT NULL DEFAULT 0,
  committed_ms  INTEGER NOT NULL,
  origin        TEXT    NOT NULL,            -- producing instance id (information only, ADR 0001)
  PRIMARY KEY (tenant, idx, node, name)
);
CREATE INDEX parts_by_partition ON parts (tenant, idx, event_hour, level);
CREATE INDEX parts_by_ts        ON parts (tenant, idx, max_ts, min_ts);
CREATE INDEX parts_by_sha       ON parts (sha256);            -- replica diff, restore inventory

CREATE TABLE part_services (                 -- "which files hold service X": usually 1-20 rows per file
  tenant TEXT, idx TEXT, node TEXT, name TEXT, service TEXT, rows INTEGER,
  PRIMARY KEY (tenant, idx, node, name, service)
);

CREATE TABLE tenant_usage (                  -- quota accounting, maintained in the same tx as parts
  tenant TEXT, idx TEXT, day INTEGER, bytes INTEGER, rows INTEGER, parts INTEGER,
  PRIMARY KEY (tenant, idx, day)
);

-- unchanged in shape, gain (tenant, idx): merged_segments, templates, template_services,
-- typed_columns (PK becomes (tenant, idx, column_name)), peer_parts_audit -> parts_audit, meta
```

Tier semantics stay ADR 0001's "where the copies are", read relative to the catalog's owner: on an edge its own files are `hot`; on central an uploaded part is `warm`/`cold` and central's own record-ingest files are `hot` (only central has them). `logless restore` and the replica only ever touch `warm`/`cold`.

### 3.2 What it must answer, and how

| Question | Answer |
|---|---|
| files for (tenant, index) in `[a, b)` at level ≥ L | `parts_by_ts` / `parts_by_partition`, one indexed query. **Today** `files_overlapping` loads every row and filters in Rust (`catalog.rs:466-481`), and `files` has no event-time range at all (`catalog.rs:169-177`, only `observed`) |
| ... for service S | join `part_services` |
| ... for template T | **not from the catalog.** The spine (per-minute rollups by `(tenant, index, service, template_id)` that `/v1/ingest` already carries, `sync.rs:55`) says which services and hours T occurred in; then the time query above. This is the thesis in `platform-thesis.md` section 3, and it avoids a 10M-file × 500-template posting list |
| ... for trace X | time hint → files; then bloom filters inside the files. No catalog trace index. Unbounded trace lookup stays a scan of the tenant's retained range (ADR 0002 says no layout fixes it); the console should require a time bound or show the cost prompt |
| per-tenant bytes/rows for quota | `tenant_usage` |
| replica diff | `parts` in `(tenant, idx, node, name)` order, paged (today's `peer_parts_after`, `catalog.rs:530`) |
| restore inventory for a node | `WHERE node = ? AND tier IN ('warm','cold')` |

### 3.3 Scale to ~10M rows

SQLite handles 10M rows (estimate: ~2.5-4 GB with the indexes above; each row ~200 B). What breaks is the code, not the engine:

- Every full-table load must go: `list_files` (`catalog.rs:440`), `files_overlapping`, `forget_missing_files` (`catalog.rs:637`, which `stat`s every path), `DiskUsage::measure` summing the catalog (`budget.rs:76`), `rebuild` reading every footer (`catalog.rs:830`). Replace with scoped queries and a partition-level `rebuild --prefix tenant=.../hour=...`.
- Writes are already batched per segment (`commit_merge`) and per part commit; at the sizes in section 8 (tens of thousands of inserts a day) the catalog's single-writer lock with the 5 s `busy_timeout` is nowhere near a limit.
- `synchronous = NORMAL`, WAL mode: keep. The tree is the truth; the manifests are the public view; a lost catalog is a rebuild.

### 3.4 Export as manifests, own format

**Decision: Parquet manifests, one file per `(tenant, index, UTC day of event hour, writer)`, rewritten whole and atomically when anything in that day changed (debounced, e.g. ≤ once per 30 s), `.pq` extension so `**/*.parquet` globs skip them (the `templates.pq` trick, `template_dict.rs:8-11`).**

```
tenant=acme/index=default/_meta/manifest/day=2026-10-11/writer=edge-1.pq
columns: path (relative key), node, event_hour, level, name, sha256, rows, size,
         min_ts, max_ts, min_observed, max_observed, tier, services LIST<VARCHAR>, schema_version, committed_ms
```

- Written by the writer that owns the files, from its own catalog, next to the data. On the local backend it is a file in the tree; on S3 a PUT. No two writers ever write the same manifest key, so **no coordination**.
- Edges upload their manifests with their parts (same presign/commit path, `kind = "manifest"`, name validated as `writer=<node>.pq` under the node's own tenant/index). Central's manifest for uploaded parts is *its* catalog's view; the edge's manifest is the edge's. The console reads whichever endpoint it is connected to, so each endpoint's manifests describe that endpoint's files. (Central does not re-export the edge's manifests; its own `writer=central-1.pq` lists every file it holds, including uploaded parts, because they are in its catalog.) Simplest rule: **a manifest describes the files reachable at the endpoint that serves it.** So edges only need to upload manifests if central should answer "what does edge-1 still hold hot" — defer that; central's own manifest is enough for central's tree.
- Size: ~150 B per file row; a day for a 100-node fleet (section 8: ~10k files/day) is ~1.5 MB per day-manifest. The console reads 1-31 of them for a query.
- Templates export moves to `_meta/templates/writer=<node>.pq`, per (tenant, index). The console unions `_meta/templates/*.pq` (ids are content hashes after section 6, so the union is meaningful).

**Rejected: Apache Iceberg.** Needs a catalog (REST/Hive/Glue) or a single `metadata.json` pointer swapped atomically; multi-writer needs a lock or a catalog service; Avro manifests; snapshot/sequence-number machinery we would never use. Fails "no external system" in practice and "one mechanism" in spirit.

**Rejected: Delta Lake log (`_delta_log/NNNNN.json`).** Closest in spirit (DuckDB reads it natively, append-only commits + checkpoints), but commit ordering is a single monotonically numbered sequence per table, which is exactly the coordination point multiple writers must avoid; S3 conditional PUT would make it work but then the local backend needs an equivalent. Per-writer Parquet manifests give the same planning benefit with none of that.

**Rejected: a catalog query API for the console** (`GET /admin/v1/plan?...`). Couples the console to the admin API and its token; manifests work with the S3 credentials the console already holds and with any DuckDB. Keep the admin endpoint as an operator tool only.

**Rejected: shipping the SQLite file.** It is a writer's private, rebuildable cache; two sources of truth (restore.md already rejected this for backups).

### 3.5 Replication

Catalog-diff replication (`parts/replicate.rs`) stays as is, keyed `(tenant, idx, node, name)`, with the key in the listing. The replica writes under the same relative path. Manifests are not replicated; the replica exports its own.

---

## 4. Read path

**Pick: the real tree is Hive, plus manifests for planning. No virtual view, no catalog API, no pushdown required.**

Today (verified): the console creates `read_parquet('s3://logs/**/*.parquet', hive_partitioning=true, union_by_name=true)` (`/Users/jamiepearcey/projects/logless-studio/crates/sqls-source/src/logless.rs:239`), caps the glob at 20,000 files (ADR-0008), and for scoped statements lists `hour=<day>T*` prefixes itself and builds a temp view over an explicit file list (`logless_scan.rs:1-48`, `MAX_SCAN_FILES = 50_000`). Uploaded parts are a separate `parts` bucket of flat `<node>/<sha>` files the console does not read at all (no reference to `parts_bucket` anywhere in the console).

With this design:

1. **Cross-node, cross-writer union within an index is one prefix**: `s3://logs/tenant=acme/index=default/**/*.parquet`. Nodes are a leaf hive column.
2. **Cross-index / cross-tenant** is a list of prefixes in the view, or one glob at `tenant=acme/**` when the credential allows it.
3. **Planning**: the console replaces its prefix listings with `SELECT path FROM read_parquet('s3://logs/tenant=acme/index=default/_meta/manifest/day=2026-10-1[0-2]/*.pq') WHERE max_ts >= ? AND min_ts < ? AND level IN (...) [AND list_contains(services, ?)]`, then the explicit-list temp view it already builds. One to a few small reads instead of N listings; and the listing-cost defect in S3 serve (each page re-sorts the directory, `docs/threat-model.md:234`) stops mattering because leaf directories hold one node's files for one partition.
4. **Freshness**: a manifest can lag the tree by the debounce (30 s). The console's "open prefix" logic (`logless_scan.rs:44-50`, `OPEN_GRACE_HOURS = 2`) assumes observed-time partitioning and must change: under event time, late data can land in any partition, so freshness comes from the manifest's `committed_ms`, not from the hour being "closed". For the live tail (last few minutes, not yet merged from the WAL) the console already falls back to the unscoped glob or to the agent's admin API; nothing new.
5. **Templates**: union `_meta/templates/*.pq` per index; join on `template_id` (content hash) and `service`.

The S3 server needs: `servable` to allow `_meta/**/*.pq` (`s3_serve.rs:918-920` allows only `.parquet` and the one `DICT_KEY`), and per-reader prefix scoping (section 7). The single-bucket design makes `parts_bucket` and the `start_with_parts` split (`s3_serve.rs:160-195`) go away.

**Narrow pushdown**: not needed for correctness or for pruning. `s3-read-bytes.md` makes a cost case for filtered counts/group-bys (23k-110k× bytes-moved/answer); that remains a separate decision and this design does not depend on it. If built, the right unit is "one (tenant, index, time range) scan on the endpoint's own tree", and the prefix scoping of section 7 applies to it too.

---

## 5. Writer scale-out

**Model: a central ingestor is an ordinary instance.** Each has its own WAL, writer thread, catalog, node id (`node=central-N`), dedupe window, manifests, templates export. Shared: the policy file and, optionally, one S3 bucket.

**Routing: by tenant, sticky per node.** `[[peer.nodes]]` lives on every central (same config file); an edge's `[[destinations]] url` names its central (or a load balancer with stickiness on the bearer token). Assign tenants to writers; split a tenant across writers by consistent hash of node id only when one tenant outgrows one writer. Why tenant: quotas, retention sweeps, `tenant_usage` and typed-column registries are then writer-local with no reconciliation.

- Rejected: **route by index.** An edge emits several indexes; splitting its stream multiplies connections and spools and breaks per-node accounting and pinning.
- Rejected: **consistent hash as the primary key.** Scatters each tenant over every writer, so every per-tenant number becomes a cross-writer sum.

**Compaction**: each writer compacts only `node=<self>` leaves it wrote. Uploaded parts are already compacted at the edge and are immutable; central never rewrites them. A later optional "recompaction" job (merge many cold small parts from many nodes in one partition into one part under `node=central-N`, retire the originals) is possible *because rows carry `origin`* (section 2.5); it is not in the build order.

**Where the bytes go**: with the local backend, writer N's tree is on writer N's disk, and the console must union N endpoints (N DuckDB secrets, N globs, N manifest reads). That works and is honest; it is what "no external system" costs. With `backend = "s3"`, all writers write one bucket under disjoint `node=` leaves and disjoint manifest keys, and the console has one endpoint. **Recommend the bucket for any deployment with more than one central writer**, while keeping it optional.

**What breaks, and the fix for each:**

| Break | Fix |
|---|---|
| Typed columns: writer A promotes `latency_ms` as Int64, writer B as String; `union_by_name` then fails on type conflict (registry is per catalog, `catalog.rs:232-238`, "type never changes") | Encode the type in the column name: `attr_latency_ms_i64`, `attr_latency_ms_str`. Never a conflict; the console coalesces. Also scope the registry per (tenant, index) |
| Template ids per writer | Section 6: content hashes |
| Edge fails over to another writer: dedupe window (`peer-dedupe.ids`) is per writer, so the resend of the in-flight batch is stored twice | Accept; bounded to one batch; readers dedupe on `event_id` (already the stated rule for parts, `docs/peer-sync.md`) |
| Quota accounting is per writer | Routing by tenant keeps it exact; a split tenant gets a per-writer share of its quota |
| Catalog-diff replication between centrals | Unchanged; the replica unions N sources or runs per source |
| Sweep/retention timer | Each writer sweeps its own catalog; policy is the shared config |

---

## 6. Templates across tenants and nodes

**Verified defect**: `/v1/ingest` deserialises the full `LogRecord` including `template_id` (`peer.rs:784`), and the merger keeps a present id without mining (`merge.rs:147-150`). Central's store therefore mixes per-edge counter id spaces with no agent column; `docs/tasks/current.md:239` records this as a known gap ("rows carry no agent column"). The rollups that make up the spine are keyed by those same per-edge ids (`sync.rs:55`), so central cannot aggregate one shape across edges. `template_dict.rs` already exports `global_id` (FNV-1a of text) and `lineage_id` precisely because the counter is local.

**Decision: the stored `template_id` becomes the 64-bit content hash of the masked template text at write time** (today's `Template::fingerprint()`, `drain.rs:122`). The Drain tree keeps its internal counters; the boundary (`LogRecord.template_id`, wire record, rollups, dictionary key, Sentry fingerprint, which already uses this hash per `current.md:135`) uses the hash.

Consequences:
- Equal shapes agree across nodes, tenants and writers with no dictionary service, no coordination. `GROUP BY template_id` across 100 edges is correct.
- **Widening** (a `<*>` appears in a template) changes the hash from that moment on that node. Measured rare with clean masking (11 widenings over 9 releases, 0 with stable values, `template-churn.md`). Rows written before carry the old hash. The dictionary export carries `lineage_id` (fixed at mint, `drain.rs:283-296`) and a `shape_history (lineage_id, template_id, first_ts, last_ts)` table so the console can fold a widened shape into one issue. Lineage across *nodes* (two nodes minting the same shape at different widening states) is reconciled by text equality after widening, and remains the open research item `platform-thesis.md` section 2 names; this design neither solves nor worsens it.
- Cross-tenant: hashes of identical text are identical in two tenants. Not a leak: the dictionary (`text`) is per tenant/index and read-scoped; a hash alone reveals nothing. Rate baselines, novelty and quotas are per (tenant, index) anyway.
- Collisions: 64-bit FNV over ~10^6 fleet templates: ~10^-8 probability. Acceptable; note it.
- `template_services`, `templates` keyed by hash per (tenant, idx). The "dictionary not rebuildable from Parquet" gap (`current.md:238`) shrinks: a restore can rebuild `id -> exemplar` deterministically; text still comes from `_meta/templates`, which should be uploaded with the parts (restore.md already recommends this).
- Central stops re-mining records that arrive with an id; records without one are mined by central under central's miner, still producing a global hash.

**Rejected: a global dictionary service** (coordination, external system, the mint path is the hot path). **Rejected: `(agent, template_id)` pairs everywhere** (two columns in every row and every join; the console must carry a per-agent dictionary per query; cross-agent equality is still a text join).

---

## 7. Tenant isolation and security

### 7.1 Tokens and scopes

| Credential | Scope | Enforceable now |
|---|---|---|
| **Node token** (`[[peer.nodes]]`) | ingest + parts write under `tenant=<its tenant>/index=<allowed>/.../node=<its id>/`; retire own parts | yes: tenant from token, index allow-list check at ingest and at presign |
| **Shared ingest token** (`[peer] tokens`) | should be dropped, or made tenant-bound: `[[peer.tokens]] {token, tenant}`. A token that owns no node writes under `node=anonymous-<tenant>`... prefer removal | yes |
| **Replication token** | read all tenants' catalog and blobs (needed for a full replica). Optionally `tenants = [...]` | yes |
| **S3 reader credential** (`[[serve_s3.readers]]`) | read `tenant=<t>/index=<i>/**` and its `_meta/` | yes: prefix check on key and on listing prefix, SigV4 unchanged |
| **Admin token** | operator; everything | unchanged; per-tenant admin needs RBAC (console) |

```toml
# central
[[tenants]]
id = "acme"
unknown_index = "default"              # or "refuse"
[tenants.quota]
parts_bytes = "2TiB"                   # sum of warm+cold parts, 507 at presign past it
ingest_bytes_per_day = "50GiB"         # /v1/ingest answers 429 + Retry-After past it; the edge keeps the data
max_uploads_in_flight = 8
[[tenants.indexes]]
name = "default"
retention = { debug = "1d", info = "7d", warn_plus = "30d" }
[[tenants.indexes]]
name = "payments"
retention = { debug = "3d", info = "30d", warn_plus = "400d" }

[[peer.nodes]]
id = "edge-1"
tenant = "acme"
token = "..."
indexes = ["default", "payments"]      # optional; default: all of the tenant's

[[serve_s3.readers]]
access_key = "acme-ro"
secret_key = "..."
tenant = "acme"
indexes = ["default"]                  # optional; default: all

# edge: the same table, exactly one tenant, and it is also what names the edge's own path
[[tenants]]
id = "acme"
[[tenants.indexes]]
name = "default"
retention = { debug = "1d", info = "7d", warn_plus = "30d" }
[node]
id = "edge-1"                          # the hive node= of its own files; must equal the [[peer.nodes]] id on central
```

The global `[retention]` (`config.rs:24`) is replaced by per-index retention; level buckets stay the retention unit inside an index.

### 7.2 Noisy neighbour

Enforcement points, all writer-local: `/v1/ingest` 429 by `tenant_usage` day bytes; presign 507 by tenant parts bytes and in-flight uploads (closes the "a node can use the whole disk budget" and "uploads not capped per node" gaps in `threat-model.md`); listener connection cap stays global (per-source caps are a separate hardening item). The 429 is the thesis working as designed: the tenant's data stays at its edge, counted, not lost.

### 7.3 Retention and erasure

- Retention: `retention.rs` walks `tenant=/index=/hour=/level=` with the index's buckets; still a directory delete. Event-time semantics: a replayed row older than the horizon is deleted on the next pass (ADR 0002 notes this; document it).
- Parts sweep: per (tenant, index), `warm_retention`/`cold_retention` from the index, not from `[peer.parts]`.
- Tenant offboarding: delete `tenant=<t>/`, `DELETE FROM parts WHERE tenant=?`, audit row, revoke tokens. One mechanism.
- Subject erasure (GDPR row delete): rewrite the affected parts (new sha, old retired, audited), not deletion vectors; the content-addressed immutability survives.

### 7.4 Encryption boundaries

Storage-level only: S3 backend with SSE-KMS keyed per `tenant=` prefix; local backend relies on disk encryption. Per-tenant encryption inside the agent would need a key per tenant on every edge and in the S3 serve path; not proposed. Plaintext HTTP without a proxy remains the threat model's open item 2.

### 7.5 What needs RBAC

Users, roles, SSO, audit of *who queried what*, per-tenant self-service of retention and tokens: console. The agent only needs the coarse credentials above and a stable prefix to scope them to; that is why tenant is first in the path.

---

## 8. Sizing (all estimates unless marked)

Assumptions: 100 agents, 1 TB/day raw across the fleet (10 GB/day/agent), ~150 B/line → ~6.7 B rows/day fleet, ~2.8 M rows/agent/hour. Parquet compression 8x (fixture measured 14x on repeated lines, 4.5x on LogHub; verified in `platform-thesis.md` and `current.md`).

| Quantity | Estimate |
|---|---|
| Edge store written | 1.25 GB/day/agent; 125 GB/day fleet |
| Pre-compaction files per agent/day | ~80 WAL segments × 3 levels × ~1.5 hours touched ≈ 360 live, plus late spool ≈ 100-200 with a replay. 36k-56k/day fleet |
| Post-compaction files per agent/day | ~72 partitions (24 h × 3 levels) → 72-100 (some partitions never reach `min_files`). ~10k/day fleet, 300k/month, 3.6M/year |
| **Catalog at 10M rows** | ~3 years of compacted parts, or ~200-280 days if edges upload pre-compaction files (today they do: the timer uploads any closed file, `docs/peer-sync.md` "The timer"; compaction then uploads the merged file and retires the inputs, so central briefly holds both). **Recommend an upload policy "closed and either compacted or older than 1 h"** to keep central's file count near the post-compaction number |
| Manifest size | ~1.5 MB per day per (tenant, index) for the whole fleet at 150 B/row |
| Central record ingest (fidelity dial 1-5%, errors full) | 70-350 M rows/day → 10-50 GB/day Parquet |
| Rollup rows | 100 agents × ~500 active shapes × 1440 min ≈ 72 M rows/day (≈7 GB/day) — comparable to the sampled rows; worth a per-hour rollup tier later |
| Central disk, local backend, 30-day parts | 3.75 TB parts + ~1.5 TB own store + manifests |
| Central ingest throughput | measured 316k events/s HEC on one machine (`platform-thesis.md` section 12); with peer acks waiting on fsync, sustained 50-100k rec/s per writer is a safe planning number → 4-8 B rec/day. One writer covers the record path at these fidelities with margin |
| Parts path at central | bytes stream to disk outside the writer thread (verified, `parts.rs` streaming PUT); 125 GB/day is ~1.5 MB/s average; the connection cap (`workers × 32 ≥ 64`) is the limit, shared with ingest |

**Where the single writer per instance stops being enough**: (a) record ingest above roughly 100k rec/s sustained or fidelity near 100% for the whole fleet (6.7 B rows/day is past one fsync-bound writer); (b) the peer ack wait (`ack_wait_ms` 5 s, one batch per fsync interval per sender) saturating 100 senders × 1 MiB batches ≈ 1 GB/s theoretical but limited by the writer's throughput; (c) the catalog's full-table loads long before SQLite itself (section 3.3); (d) the console's 20k/50k file caps, which manifests remove. Compaction at central is only for its own files; CPU for 50 GB/day of merges is well within one core.

Not measured anywhere: any of the above at scale. The first measurement to run is the real arrow-rs writer on the sort-layout corpus under the new layout (ADR 0002 "Not decided here").

---

## 9. Build order

Each slice ships alone and is testable alone.

1. **Layout v3 at the edge (first slice).** Path `tenant=/index=/hour=<event>/level=/node=/`, `[[tenants]]`/`[node]` config, index routing from HEC/OTLP/tail, 8k row groups, sort key 3 = `timestamp`, `origin` column, late spool, compaction quiet-rule, retention per index over the deeper tree, catalog `parts` table with `min_ts/max_ts`, `rebuild`, `restore` placing by event hour, S3 serve serving the new tree. **Measure**: pre- and post-compaction file counts and bytes per query shape on the ADR 0002 corpus with the real writer; retention correctness with a replay; `scripts/s3-count-proxy.py` bytes for Q2/Q3/Q4 against today. Gate: Q2 ≤ 1 file, Q3/Q4 within 20% of the bench's L8 numbers, pre-compaction files ≤ 2x today's with the late spool on.
2. **Template id = content hash.** Boundary change in `drain`/`merge`/`sync`/`template_dict`/forwarders; `_meta/templates/writer=<node>.pq` per (tenant, index) with `shape_history`; central stops trusting/mining foreign ids. **Measure**: re-run `template-churn.sh`; count rows per shape across two edges ingesting the same corpus (must be equal); Sentry fingerprint unchanged.
3. **Parts in the tree.** Presign/commit carry the partition path; central builds `tenant=/index=/hour=/level=/node=/<sha>`; node token bound to tenant with index allow-list; one bucket in S3 serve; `peer_parts` and `parts_bucket` removed; replica listing keyed by the new tuple; restore from the new tree; upload policy "compacted or older than 1 h". **Measure**: the e2e stack (`docs/e2e-stack.md`) end to end; the restore drill; console reads uploaded parts through the same `logs` view with zero console changes (hive columns appear automatically).
4. **Manifests + console planning.** Writer exports `_meta/manifest/day=/writer=.pq`; S3 serve allows `_meta/**/*.pq`; console scoped view reads manifests instead of listing, and drops the observed-time "closed prefix" assumption. **Measure**: requests and bytes per statement at 100k synthetic files vs the listing approach; staleness after a late write.
5. **Tenant isolation and quotas.** `[[serve_s3.readers]]` prefix scoping; `tenant_usage`; 429/507 by tenant; per-index sweep; offboarding command. **Measure**: threat-model table rows for cross-tenant read, cross-tenant presign, quota refusal; soak with one noisy tenant.
6. **Second central writer.** `node=central-2`, type-suffixed typed columns, console union of N endpoints (and single endpoint with the S3 backend). **Measure**: duplicate rate on edge failover; manifest union correctness; no `union_by_name` type conflicts on a corpus engineered to disagree.
7. **Catalog and trace hardening.** Scoped catalog queries, partition-level rebuild, 10M-row synthetic catalog benchmark; bloom filters on `trace_id` with the DuckDB BLOB-equality defect resolved or worked around. **Measure**: catalog p99 for the planning query at 10M rows; Q1b bytes with blooms.

---

## 10. Risks, and decisions only the owner can make

**Risks**

1. Event-time retention deletes replayed-old data at once; and late data can land in any partition, so every cache that assumed "closed hour" (console `OPEN_GRACE_HOURS`, compaction's active-hour skip) is wrong until changed.
2. Write fan-out: the late spool is a design, not a measurement.
3. Content-hash template ids make widening a visible id change in the console until `shape_history` is used; cross-node lineage is still unsolved research.
4. Multi-writer without a bucket means N endpoints in the console; that is a UX and ops cost, not a correctness one.
5. DuckDB hive type autocasting on numeric-looking ids; DuckDB BLOB-equality defect blocks bloom filters.
6. SQLite full-table code paths will hurt long before the engine does; easy to forget because they work at 10k rows.
7. Changing `template_id` semantics and the path at the same time means two migrations with no users to notice; cheap now, expensive in six months.

**Owner decisions**

1. Is a *tenant* a customer, or a team within one customer? (Affects whether cross-tenant union in the console is ever legitimate and whether tenants share typed-column namespaces.)
2. Accept the `template_id` semantics change (content hash, widening = new id)?
3. Accept event-time retention semantics (ADR 0002 consequence)?
4. Central's own record-ingest files join the parts tree as `node=<central>` (recommended) or stay a separate tree?
5. Edges upload only compacted/aged files (recommended) or every closed file as today?
6. Is the shared `[peer] tokens` ingest token removed, or bound to a tenant?
7. For multi-writer: recommend the optional S3 bucket; is "N endpoints in the console" acceptable for the local backend?
8. Narrow pushdown stays undecided; nothing here needs it.

---

## Current code that contradicts this design (file:line)

- `crates/logless-core/src/merge.rs:130` partitions on `observed`; ADR 0002 wants `timestamp`.
- `crates/logless-core/src/merge.rs:147-150` keeps a foreign `template_id` unmined; `crates/logless-core/src/peer.rs:784` deserialises it; mixed id spaces land in one store with no agent column (`docs/tasks/current.md:239`).
- `crates/logless-core/src/store.rs:25` `ROW_GROUP_ROWS = 64 * 1024`.
- `crates/logless-core/src/store.rs:195` and `scan.rs:247` read/prune stats on `observed` only; `catalog.rs:169-177` `files` has no event-time range.
- `crates/logless-core/src/catalog.rs:440-481` `list_files`/`files_overlapping` load the whole table; `catalog.rs:637` `forget_missing_files` stats every path; `catalog.rs:830` `rebuild` is whole-store.
- `crates/logless-core/src/catalog.rs:248-261` `peer_parts` keyed `(node, name)`, no tenant/index/hour/level; `min_observed/max_observed` are fed observed time (`parts.rs:1528-1529`).
- `crates/logless-core/src/parts.rs:628` key is `<dir>/<node>/<name>` (flat).
- `crates/logless-core/src/s3_serve.rs:45-68,160-195` one credential, whole-bucket read, separate `parts_bucket`; `s3_serve.rs:918-920` `servable` admits only `.parquet` and the single `_meta/templates.pq`.
- `crates/logless-core/src/compact.rs:120` skips the wall-clock active hour (`crates/logless-cli/src/main.rs:2250`).
- `crates/logless-core/src/retention.rs:73-131` walks exactly `hour=/level=`; `config.rs:24` retention is global, not per (tenant, index).
- `crates/logless-core/src/restore.rs:339` places a restored part by `observed` hour.
- `crates/logless-core/src/catalog.rs:232-238` typed-column registry is per store with "type never changes" (cross-writer/cross-tenant conflict).
- `crates/logless-core/src/peer.rs:675-700` `authenticated` yields a node id only; no tenant.
- `crates/logless-core/src/template_dict.rs:55` one `_meta/templates.pq` per store.
- `crates/logless-core/src/sync.rs:55` rollups keyed by per-edge template ids.
- Console: `crates/sqls-source/src/logless.rs:239` one whole-store glob (20k cap, ADR-0008); `logless_scan.rs:40-50` `MAX_SCAN_FILES = 50_000`, `OPEN_GRACE_HOURS` assumes observed-time partitioning; console never reads the `parts` bucket.
- `docs/tasks/current.md:241` HEC `index` is ignored.

Not verified: whether `sync.rs`/`peer.rs` infer the ingest hour from any path (ADR 0002 flags this; I did not find such code but did not read all 3,400 lines); the behaviour of arrow-rs bloom filters and page indexes with DuckDB on `FixedSizeBinary(16)`; any number in section 8.

### Critical Files for Implementation
- /Users/jamiepearcey/projects/infrastructure/log-less-wt-integ/crates/logless-core/src/partition.rs (the key type: gains tenant, index, node; event hour)
- /Users/jamiepearcey/projects/infrastructure/log-less-wt-integ/crates/logless-core/src/merge.rs (partition on `timestamp`, late spool, origin column, content-hash template id at the boundary)
- /Users/jamiepearcey/projects/infrastructure/log-less-wt-integ/crates/logless-core/src/catalog.rs (unified `parts` table, scoped queries, manifest export source)
- /Users/jamiepearcey/projects/infrastructure/log-less-wt-integ/crates/logless-core/src/parts.rs (presign/commit carry the partition path; node token bound to tenant; key built from the tuple)
- /Users/jamiepearcey/projects/infrastructure/log-less-wt-integ/crates/logless-core/src/s3_serve.rs (one tree, `_meta/**/*.pq` servable, per-reader prefix scoping)
- /Users/jamiepearcey/projects/logless-studio/crates/sqls-source/src/logless_scan.rs (console planning from manifests instead of prefix listings)

---

## Owner decisions (2026-10-11)

1. **Tenant.** A customer in a hosted deployment; an isolated scope (team, environment) in a self-hosted one. A hard boundary in both. A single-tenant self-hosted install uses one tenant named `default`, so it needs no tenant configuration. Cross-tenant reads are for an operator principal only.
2. **Template id = content hash: accepted.** A widened template gets a new id; the console folds by lineage.
3. **Event-time retention: accepted.** A replayed row older than the horizon is deleted on the next pass.
4. **Central's own record files join the same tree** as `node=<central>`: accepted.
5. **Edges upload only compacted or hour-old files:** accepted.
6. **Auth.** The shared ingest token goes. It is replaced by named service principals bound to a tenant; people sign in through session-broker at the console and never reach the agent directly. Design in its own item (Plane LOG-179); not decided in detail yet.
7. **Several centrals.** The console selects a server and that server returns what it holds. When the servers share a bucket and all write the same partition layout, the same query can run directly against the bucket across `node=` leaves, outside any one server's namespace. A union across servers without a bucket is best effort in the console.
8. **Pushdown:** not answered; stays undecided. Nothing here needs it.
