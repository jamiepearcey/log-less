# log-less — architecture

Companion to `.context/project-brief.md`. Decisions here are opinionated; rejected alternatives are recorded inline so they don't get re-litigated.

## 0. Shape

```
apps ──OTLP/HEC/syslog/files──▶ ┌──────────────── log-less node agent (single Rust binary) ─────────────────┐
                                │                                                                            │
                                │  receivers ─▶ normalise (OTel model) ─▶ template (Drain) ─▶ bounded MPSC   │
                                │                                             │                    │         │
                                │                                    ring buffer (pushdown)        ▼         │
                                │                                             │                   WAL        │
                                │                                             │                    │         │
                                │                                             │              merge/compact   │
                                │                                             │                    ▼         │
                                │                                             │    Parquet + SQLite catalog   │
                                │                                             ▼                    │         │
                                │                                        forwarder ◀───────────────┘         │
                                └──────────────────────────────────┬─────────────────────────────────────────┘
                                                                   ▼
                                              Sentry / Splunk HEC / Datadog / OTLP (curated subset)
                                                                   +
                                              optional: S3 tier ─▶ aggregation tier (DuckLake) ─▶ SQL/Grafana
```

Everything above the S3 line is one process, one config file, no external dependencies.

## 1. Storage: WAL + committed state

### WAL

- **Segment format**: append-only, length-prefixed frames — `u32 len | u32 crc32c | payload`, payload = a batch of records in a flat binary encoding (`bitcode`/`postcard`, or Arrow IPC once batches are large). Roll at 64–128 MB or 5 minutes.
- **Recovery**: sequential scan of the last segment, truncate at first bad CRC.
- **Rejected**: sled/RocksDB. We never need random access, only sequential replay — an LSM is pure overhead.
- **fsync**: group commit — `fdatasync` every 50–100 ms or 1–4 MB, whichever first. Never per record. Ack ingest on enqueue, not on fsync (loss window = one flush interval; configurable to sync-per-batch for HEC-with-acks).
- **Never block the app**: bounded MPSC (~64k records) between receivers and the WAL writer. On overflow, **shed by severity** — drop DEBUG first, then INFO, always keep WARN+ — and emit a drop counter. Blocking the producer is worse than losing debug lines. This is a load-shedding system by design; say so in the docs rather than pretending otherwise.
- **Disk full**: budget + watermarks. At 80% of budget delete data ahead of retention, cheapest bucket first and oldest first; at 95% additionally stop admitting DEBUG. Deleting committed data is the release valve — **never** "stop ingest".
- **Pressure relief works at FILE granularity, retention at PARTITION granularity.** They look like the same operation and are not. Retention expires data whose time is up, so whole directories are right. Pressure relief is an emergency valve that must take the *minimum*, and hour-sized partitions give it too few units to choose from: a burst landing in a single hour leaves only three partitions, so freeing anything frees an entire severity. Measured — with a 3 MB budget against 25 MB of logs, partition-granular reclaim deleted **every record including all errors**; file-granular reclaim kept all 74k warn+ rows and spent debug and info instead.

### Committed state

- **Partitioned Parquet + a local SQLite catalog.** Catalog tables: files (path, partition keys, min/max ts, row count, template-dict version), templates (the miner's private state: counter, text, first_seen, counters, lineage), template_shapes (content-hash id to text and lineage), template_services (per `(tenant, index)`, per service: rows and first/last observed, keyed by the content-hash id, written in the merge transaction), meta. The dictionary and the shape history are exported after each merge as `store/tenant=<t>/index=<i>/_meta/templates/writer=<node>.pq` and `.../_meta/shape_history/writer=<node>.pq` so readers of the store can resolve `template_id` to text and fold a widened shape (`docs/s3-serve.md`). Atomic commit = write Parquet to temp → fsync → insert catalog row in one transaction.
- **The catalog is DERIVED STATE, not source of truth.** It must be fully rebuildable by scanning Parquet footers + the directory tree. Delete it, corrupt it, `kill -9` mid-write — the agent rebuilds on boot. Hard invariant, tested in the week 1–2 chaos harness. It also makes the engine choice reversible. Accepted loss on rebuild: template `first_seen` resets to the earliest retained data, so novelty detection needs a warm-up window.
- **Stays out of the catalog**: the WAL (bespoke append-only, §1) and per-destination delivery cursors (higher write rate — own small file, atomic rename).

### Query engine: none embedded (decided 2026-08-04)

Two jobs were being conflated. Separating them removes the engine entirely:

| Job | Solution | Cost |
|---|---|---|
| Catalog (few thousand metadata rows, ACID small txns) | SQLite | ~1 MB |
| **Replay scan** — the only v1 query: `ts BETWEEN a AND b AND service = X AND level <= Y` → stream to forwarder | `parquet` + `arrow-rs` crates, row-group statistics pruning + predicate filter. Scan/filter/project only — no joins, no GROUP BY | ~300 LOC |
| Ad-hoc SQL | **Ship the format, not the engine.** Users point their own DuckDB / Grafana / Polars / ClickHouse at the Parquet directory | 0 |
| Optional built-in SQL endpoint | `--features duckdb`, off by default | opt-in |

Context windows come from the in-memory ring (§4); bytes-avoided counters and anomaly state are maintained incrementally in memory. Neither queries Parquet.

**Rejected: embedding DuckDB in the agent.** ~30 MB of statically linked C++ to serve `WHERE ts > x`. It also *created* the problem it then needed mitigating: DuckDB's single-writer file lock means that while the agent holds the catalog read-write, no other process can attach it at all (read-only attach requires no writer). Not embedding it means a user's own DuckDB works against the Parquet dir *while the agent runs*, with a better engine than we'd ship.

**Rejected: reimplementing DuckDB's relevant parts.** It is not one algorithm — vectorized execution, query optimizer, MVCC storage, hundreds of KLOC. The answer is to not need it.

**The DuckLake progression is unaffected**: `ducklake` lives at the aggregation tier (separate process, its own Postgres/DuckDB catalog) and ingests the same Parquet files unchanged. DuckDB's `sqlite_scanner` can `ATTACH` the node catalog directly when needed. Nothing about DuckDB-later requires DuckDB-now.

**Measured (arm64 macOS, `lto=thin`, `strip=true`, `panic=abort`):**

| Build | Stripped release binary | Measured |
|---|---|---|
| model, config, WAL, partition, retention, CLI | 1.5 MB | 2026-08-04 |
| + arrow, parquet, rusqlite | 7.3 MB | 2026-08-04 |
| **current — the above plus every receiver** | **11.05 MB** | **2026-08-07** |
| + duckdb | not built; DuckDB static-links ~30 MB → **~3.7× the current binary** | — |
| + datafusion | not needed; the replay scanner uses `parquet`/`arrow-rs` directly | — |

Settled. Embedding a general-purpose query engine would still cost more than twice the agent that runs it — to serve a filtered scan we already do in ~300 lines. Users bring their own engine to the Parquet directory instead, verified working below.

**A Rust-native engine is a different question, and the answer differs.** Measured 2026-08-07 by linking RAE (`../../rust-analytical-engine`) into `logless-cli` under this profile, with a probe reachable from `main` so LTO could not discard it:

| Node binary | Size | Delta |
|---|---:|---:|
| `logless` today | 11.05 MB | — |
| + push-down kernel (`rae-storage`/`-execution`/`-aggregation`/`-common`) | 11.09 MB | **+0.04 MB** |
| + the whole engine (also `rae-sql`, `rae-planner`, `rae-runtime`, `rae-joins`) | 11.79 MB | **+0.74 MB** |

Both verified live rather than merely linked: the kernel read 1,000,000 rows from real parts, and the full build answered `SELECT count(*)` from inside the `logless` binary. The marginal cost is below RAE's 1.5 MB standalone size because this agent already links `serde`, `thiserror`, `crossbeam` and `hashbrown`, and thin LTO shares them. RAE's engine crates pull in no `arrow`, no `parquet` and no `tokio` at all.

This does **not** reopen the DuckDB decision — 30 MB of C++ for a filtered scan is still wrong. It records that "no embedded query engine" was a judgement about *cost*, and a 40 KB one does not trigger it. See `docs/platform-thesis.md` §13–§14 for what that unlocks and what it costs in engine features.
- **DuckLake is rejected at the node tier** and adopted only at the *optional* aggregation tier. Reasons: every merge becomes a catalog transaction, so at 500 nodes × frequent small commits Postgres is a serialisation point (commit conflicts, snapshot/metadata churn, small-file explosion, or catalog bloat via inlining); and 500 agents would need Postgres credentials and connectivity, reintroducing a central dependency into a product whose pitch is "local and cheap".
- **Cluster tier design**: agents ship *completed Parquet files* to the tier; one (or few) writers commit them to DuckLake. DuckLake v1.0 (April 2026) is production-ready and earns its keep there — snapshots, time travel, multi-writer, Iceberg-compatible deletion vectors.

### Delivery semantics

At-least-once with idempotency keys (UUIDv7 per event, assigned at ingest). Exactly-once to Splunk/Sentry is fiction — we don't control their dedupe. Per-destination cursor (segment id + offset) lives in its own small file (atomic rename, not the catalog — higher write rate); replay from cursor on restart.

## 2. Partitioning and retention

Decided in ADR 0002 and `docs/design/multi-tenancy-and-indexing.md` (slice 1), measured in `docs/evidence/layout-v3.md`.

```
<data_dir>/store/tenant=<t>/index=<i>/hour=YYYY-MM-DDTHH/level=<bucket>/node=<writer>/<file>.parquet
```

- **Partition key: `(tenant, index, hour, level, node)`**, in that order (`partition.rs`).
  - `tenant` is the isolation boundary and `index` the dataset inside it: retention is set per `(tenant, index)`, a read of one index is one prefix, and offboarding a tenant is a subtree delete. An install that configures neither is one tenant `default` with one index `default`.
  - `hour` is the **event hour**: the hour of the row's `timestamp`, not of `observed`. Every real query is bounded on event time, so replayed and clock-skewed rows prune by directory (one file for an issue-list hour, where observed-hour partitioning has to open every later hour). `observed` stays a column.
  - `level_class ∈ {debug, info, warn_plus}` by default: coarse buckets, not seven levels, to keep file counts sane. It exists for exactly one reason: **retention becomes `rm -rf` of a directory** plus a catalog delete. It sits above `node` so one delete takes every writer's files for the partition.
  - `node` is the writer that owns the file: `[node] id`, or an id generated on first run and kept in `<data_dir>/node-id`. A writer only ever writes and compacts its own leaf, so several writers need no coordination.
- **Identifiers** (tenant, index, node): 1 to 64 of `A-Za-z0-9._-`, not starting with a dot, unique ignoring case, and **at least one ASCII letter**. Without the letter DuckDB's hive partitioning reads `tenant=1234` as a BIGINT and a reader's string predicate matches nothing.
- **Index routing at ingest.** A record names its index in a HEC event's `index`, the OTLP resource attribute `logless.index`, or `[[tail]] index`; Sentry items and everything else go to the tenant's default index. The name is checked against the tenant's `[[tenants.indexes]]`. An unknown one is sent to the default index or refused, by `unknown_index = "default" | "refuse"` (default `default`); HEC refuses the whole request with Splunk's code 7, other receivers do not store the record. Both are counted in `GET /admin/v1/info` (`ingest.unknown_index`). The index is a directory, never a column: the routing attribute is taken off the record when it is merged.
- **Sort order inside files**: `(service, trace_id, timestamp, event_id)`. This clusters templates (good for zstd + dictionary encoding) and puts a trace's rows together within a service. Time-first was measured and rejected (2-3x worse on template and service filters, ADR 0002).
- **Row groups are 8,192 rows** (`store::ROW_GROUP_ROWS`), so min/max statistics on the sorted `service` and what clusters behind it prune inside a file.
- **`origin` column** (dictionary-encoded string): the instance that first received the row. For a row another instance relayed it is that row's `logless.origin` attribute, otherwise the node that wrote the file. Not the same as the path's `node`, which is the file's writer. Schema version 3.
- **Timestamps are UTC-qualified in the Parquet schema** (`Timestamp(ns, "UTC")`), not naive. Written without a timezone, DuckDB and Polars read them as *local* time and every "last hour" filter silently returns the wrong rows — observed in testing, where an errors-in-the-last-hour query returned 0 instead of 17,088. The schema is the long-lived contract; this had to be right before anyone builds on it.
- **Retention = file-level drop only**, per `(tenant, index, level)`, on **event time**: a partition expires when its hour has ended and its bucket's retention has passed. A row replayed today with a timestamp older than the horizon lands in a partition that is already past it and is deleted by the next pass. A tenant, index or bucket that is on disk but not in the config is never deleted, only reported. Deletion vectors are for compliance deletes, not time expiry — never pay per-row bookkeeping for something a directory delete solves.
- Partition-by-level does **not** fight pushdown: pushdown is served from memory (§4), and cross-partition reads on one local disk are cheap.

### Write fan-out and the late spool

The merger writes one file per (WAL segment, partition). Under event-time partitioning a segment that carries replayed or clock-skewed rows touches many event hours, so it writes many small files. Two things bound that:

- **The late spool.** A record whose event time is already older than `storage.late_threshold` (2 h) when it is written goes to a second WAL, `wal-late/`, instead of the main one (`wal::SplitWal`). That log rolls on age (`late_roll`, 30 min) or size (`late_segment_bytes`, 64 MiB), not with the main WAL, so a replay makes one file per partition per late segment instead of one per main segment. Both logs are fsynced together (the acknowledged record count is only published with both on disk) and merged by the same code, so a late record is exactly as durable as any other; what it gives up is reaching Parquet before its segment rolls, up to `late_roll` later.
- **Compaction** merges a writer's small files in a partition once it holds at least four. It used to skip "the hour being written", by the wall clock; under event time any partition can gain a file, so it now skips a partition whose newest file is younger than `storage.compact_quiet` (10 min). The order (write the replacement, record it, unlink the inputs) already makes a late arrival safe: it is simply not an input.

`logless restore` installs whole part files and takes neither path. A backfill received from another instance is ordinary ingest: its old rows take the late spool.

### Existing stores

A store written before this layout (`hour=…/level=…` at the top, cut on `observed`) is refused at startup with a message naming `logless migrate-layout --config <file>`. That command, run with the agent stopped, rewrites each old file by event hour into `tenant=<own>/index=<default>/…/node=<own>/` and rebuilds the catalog's file table; it can be repeated after an interruption without doubling rows. The same command re-keys template ids: a file whose footer `logless.schema_version` is below 4 (and every old-layout file) carries the writer's counter, each counter is mapped through the catalog's `templates` table to the hash of that template's current text, the file is rewritten in place (a counter the catalog does not know loses its id, and is counted), and the catalog's `template_services` is refilled from all files. Files at version 4 are not touched, so it is safe to run again. A store that was never migrated keeps counters in its old rows (small numbers, never equal to a real hash in practice) next to hashes in new ones; nothing refuses to start on that.

## 3. Templating and compression

- **Use Drain, ported to Rust.** Fixed-depth parse tree (token count → prefix tokens → similarity leaf). ~500 lines of core logic; `drain-rs` is a starting point but expect to own it. Best-in-class accuracy/throughput on the LogHub benchmark.
- **Rejected: MinHash + LSH centroid clustering.** It's an approximate-similarity engine solving a problem Drain solves exactly and more cheaply. Worse, centroid drift and bucket churn produce **unstable template ids**, which poisons dedupe, novelty detection and rate baselines downstream. Stable ids are the whole point.
- **Rejected: LLM-derived per-cluster regexes in the pipeline.** Masking heuristics before the Drain tree (timestamps, UUIDs, hex, IPs, numbers, quoted strings, `key=value`, paths) get ~95% of the value, deterministically and offline. Keep an *optional offline* LLM pass only for human-readable template naming — never in the hot path.
- **Steal from CLP** (YScope, production at Uber, ~2× gzip, searchable while compressed): its **variable taxonomy** — dictionary variables vs typed non-dictionary numerics. Do not reimplement its archive format; there is no Rust binding and it's a project in itself.
- **Honest compression math**: a sorted message column with Parquet zstd(~3) + dictionary encoding already gets ~10–20× on raw text. Template + typed param columns adds roughly **1.5–2.5× on top** — not the headline number.
  **So the justification for templating is structure, not ratio:**
  1. stable `template_id` gives dedupe, novelty and rate detection for free (§5);
  2. typed param columns give predicate pushdown (`latency_ms > 500`) that grep-over-text can't do.
- **Reconstruction**: dict lookup + interpolation, microseconds per row. Store the template dict per file (or a dict-version referenced in the catalog) so files are self-describing. Keep raw text only for lines that fail templating — target <5%.
- **The id rows carry is a content hash.** Drain keeps counters inside its tree (they index templates in memory and in the catalog's `templates` table), but everything that leaves the miner uses the 64-bit hash of the masked template text (`Template::fingerprint`, `drain::fingerprint_of`): `LogRecord.template_id` as written to Parquet and sent on `/v1/ingest`, rollup keys, the dictionary, the catalog's `template_services`, known-pairs and anomaly-signal keys, the merge-suggestions endpoint. Two nodes that mined the same shape agree on its id with no coordination, so `GROUP BY template_id` is correct across edges and a central adds one shape up across them. A record that arrives at a central with an id keeps it (the central does not mine it); one without is mined by the central's own miner and gets the hash. (Hash function: the repository's `fingerprint_of`, FNV-style with a multiplier of `0x1000_0000_01b3`; it is not the textbook FNV-1a prime and is left as it is, since changing it would move every Sentry fingerprint.)
  **Widening** (a `<*>` appears in a template) changes the hash from that moment on that node; rows already written keep the hash they were written with. The template's `lineage_id` is fixed when it is minted and does not change, the Sentry forwarder fingerprints with it, and the exported `shape_history` (`lineage_id, template_id, text, count, first_ts, last_ts`) lists every hash a lineage held so a reader folds them into one issue. **Collisions:** two different texts with one hash are one shape to every query; the dictionary keeps the first text registered, and the catalog counts the event (`template_hash_collisions`, logged at merge); about 10^-8 for 10^6 fleet-wide templates. Evidence: `docs/evidence/template-hash-ids.md`.

**Measured (2026-08-04, release build, arm64):**

| Metric | Result | Target |
|---|---|---|
| Throughput | **775,617 lines/s/core** | ≥500k — met |
| Templates from 2M synthetic lines | 15 | bounded — met |
| Templates from 200k mixed production-shaped lines | 15, covering 100% | <5% untemplated — met (0%) |
| Store compression **with** templating | 2.4× | unchanged from 2.5× without |

Note the debug build manages only ~39k lines/s — the target holds only when optimised, so never benchmark this in a debug profile.

**Measured, and it settles the question**: storing template id + parameter columns *instead of* the body is 4.7% smaller with the dictionary amortised, and 4.4% larger without — not the 4–8× assumed. zstd plus dictionary encoding over the sorted body column is already removing the redundancy templating would remove. See `docs/tasks/current.md`.

The compression row is the important one and it confirms the reasoning above rather than contradicting it: `template_id` is assigned but `body` is still stored in full, so nothing shrinks yet. Ratio gains need the follow-up step (store template id + parameter columns *instead of* the body text). Templating earns its place today through **structure** — stable ids for novelty detection, rate baselines and Sentry fingerprints.

**Typed predicate pushdown arrived without template parameters** (2026-10-05). The pushdown argument above does not need template parameters: structured sources (OTLP, HEC fields, Sentry tags) already deliver typed attributes. Keys that a service sets on most records with one type are promoted from the `attributes` JSON to `attr_<key>` columns with a type fixed for the life of the store (`docs/typed-columns.md`). Measured on synthetic data: a predicate reads 0.035x the bytes, encoding is 0.75x the cost because less goes through JSON, and min/max row-group pruning is nil under the `(service, trace_id, observed)` sort. Parameter columns extracted from unstructured bodies are still not built.

- **Hard rule**: the architecture must not *depend* on templating. If throughput or stability misses target, fall back to sorted + zstd and lose only the anomaly features.

## 4. Smart pushdown

- **In-memory ring buffer, not store queries.** Committed state is minutes stale and the WAL is unindexed. Sharded ring: hash(correlation key) → shard, each shard a bounded deque of `(ts, level, template_id, compact record ref)`. Global cap 64–256 MB, plus per-key caps (~2k lines / 30 s) so one chatty thread can't evict everyone.
- **Correlation key fallback chain**: `trace_id` → `(service, request_id/session_id)` from parsed params → `(host, pid, thread)` → `(host, service)` + time window. Configurable per source. Log which tier fired so users can fix their instrumentation.
- **Error storms**: token bucket per `(service, error_template_id)` — first ~3 context windows per template per minute go upstream in full; after that forward the error as an aggregate count only. Egress is bounded by *distinct error templates*, not error count.
- **Window dedupe**: fingerprint each window as a hash of its ordered `template_id` sequence (a "flow hash"). Same flow hash within T → suppress and increment the count on the already-sent window. Collapses retry-loop storms to one context + a counter.

### Defining dedupe (decided 2026-08-04)

"Dedupe" is three separable things, and conflating them is the mistake:

1. **Context dedupe** — do not re-send an identical *context block*. Lossless:
   every error still goes upstream in full, with its own parameters. Fully
   automatic, nothing to configure. Built.
2. **Event aggregation** — collapse N error events into one carrying `count=N`.
   This *does* lose information, and the right shape depends on the destination.
3. **Novelty / rate anomaly** — the reason to deliberately *break* dedupe.

**"Identical" means identical shape, never identical text.** The flow hash is
over the ordered sequence of `template_id`s, not bodies — raw text never repeats
(different user, latency, trace), so hashing it would mean dedupe never fires.
This is why templating had to land before pushdown.

**Aggregation is shaped per destination**, because the vendors differ:

| Destination | Shape | Why |
|---|---|---|
| Sentry | Every error sent; `fingerprint = [template_id]`; context on first-of-flow only | Sentry groups and counts natively. Do not aggregate — make its grouping *deterministic*. Aggregating fights the UI. |
| Splunk | One rolled-up event per (template, window): count, first/last seen, top params, replay hint | Bills per GB, no native grouping. Aggregation is the point. |

So the shaping belongs in the **forwarder**, not the ring. The ring decides what
repeats; the adapter decides how to say it.

**Automatic by default; rules only as an override.** If users need rules to get
sane behaviour, the defaults are wrong. Escape hatches only — `never_collapse`,
`always_full_context`. Two guardrails are non-negotiable:

* **Collapsing makes things cheaper, never invisible.** Every collapse leaves a
  counter behind; a storm that silently vanishes is worse than the bill.
* **Novelty always breaks collapse.** A new template or a rate spike gets full
  context regardless of remaining budget.

**Open, and not decidable from first principles**: the dedupe window (60s) and
windows-per-minute (3) are guesses. Config with defaults; measure against a real
corpus. Note that the synthetic demo collapsed 215 of 221 errors only because
every failure was identical by construction — real traffic will collapse less.

## 5. Dedupe, novelty and rate anomalies

- **Novel template**: the Drain tree *is* the detector — a `template_id` never assigned before is novelty by definition. Persist first-seen in the DuckDB catalog (rebuildable from Parquet if lost — accepting that a rebuild resets "first seen" to earliest retained data); "novel" = first occurrence, or first in N days. No extra sketches needed.
- **Rate spike**: per-template EWMA of per-minute counts + EWMA of absolute deviation (MAD-style); alert when `count > mean + k·dev` (k≈4) with a minimum-count floor to kill low-volume noise. Bound tracked templates with SpaceSaving heavy-hitters (~10k entries); count-min sketch for the tail.
- **Cardinality explosion**: HyperLogLog per template over param values — catches new-user-id floods and IP scans. Distinct, valuable signal.
- **Wire shape** (one synthetic event):
  ```json
  {"template_id","template_text","example_raw","count","first_seen","last_seen",
   "top_params":{"name":{"top5":[],"hll_cardinality":0}},"anomaly_kind","replay_hint"}
  ```
  `replay_hint` is a URL/query into the local store.
  - **Splunk**: HEC event with these as indexed fields.
  - **Sentry**: set `fingerprint = [template_id]` so Sentry's grouping matches our template — this makes Sentry grouping deterministic, which is a genuine selling point in its own right.

## 6. Ingest surfaces (ranked by adoption unlock ÷ cost)

| # | Surface | Notes |
|---|---|---|
| 1 | **OTLP logs (HTTP ✅, gRPC ✅)** | Makes us a drop-in **backend** for every existing OTel Collector / Vector deployment — the cheapest integration is *being the endpoint*, not being the agent. Built **without** `tonic` + `prost` + `opentelemetry-proto`: see below. gRPC on `:4317` is what an unconfigured Collector and most SDK defaults reach for; HTTP on `:4318` is what a deliberately configured one uses. |
| 2 | **File tailing** | Universal brownfield unlock. Rotation, truncation, checkpoints; `notify` + poll fallback. Most adoption is "point it at /var/log". |
| 3 | **Splunk HEC ✅** | Big unlock in Splunk shops: adoption is a hostname change. Ack semantics implemented and keyed to a real `fdatasync`, not to receipt — see below. `/event`, `/raw`, `/ack`, `/health`. |
| 4 | **Syslog 3164/5424** (TCP/UDP/octet-counted) | Cheap, table stakes for infra gear. |
| 5 | **Elasticsearch `_bulk`** | Last. Surface creeps (index templates, per-action error semantics). |

**gRPC without tonic.** gRPC is HTTP/2 plus a five-byte message prefix plus a status in the trailers. The five-byte prefix and the trailers are trivial; HTTP/2 is where the work is, and the honest accounting is that this cost ~350 lines of frame codec plus ~400 of connection state machine, against tokio + hyper + tower + h2 + prost + a code generator. What it does *not* include is everything a general HTTP/2 server needs and a gRPC-only endpoint does not: server push, priority scheduling, extended CONNECT.

One dependency was taken deliberately: `fluke-hpack`, for HPACK. The reasoning that justifies hand-writing a protobuf reader — a wire format of four types and a length prefix, derivable from the spec — stops at HPACK's Huffman code: 257 canonical entries from RFC 7541 that cannot be derived, only transcribed, where a single wrong entry corrupts header values silently instead of failing. It has no transitive dependencies.

Three details that are easy to get wrong and are load-bearing:

- **The connection-level receive window starts at 64 KiB regardless of `SETTINGS`** — `SETTINGS_INITIAL_WINDOW_SIZE` applies to streams only. A server that never sends a connection `WINDOW_UPDATE` therefore accepts exactly 64 KiB of request body and then hangs, which looks like a network fault rather than a bug.
- **A gRPC error is an HTTP 200** with `grpc-status` in the trailers. Returning an HTTP error status makes the client report something unrelated to what went wrong.
- **`UNAVAILABLE`, not `RESOURCE_EXHAUSTED`, for a full pipeline.** The latter is closer in spirit, but several exporters treat it as permanent — which turns backpressure into data loss.

## Sentry: proxy, not just a destination

Splunk was symmetric from week 6 — HEC in, HEC out — so a Splunk shop changes a hostname and log-less is genuinely *in front of* the vendor. Sentry was not: we forwarded synthesised events to it, but an application's Sentry SDK talked straight past us. The brief's claim that "we sit in front of it" was therefore only half true.

The proxy closes that. Point the SDK's DSN host at the agent and every envelope arrives here, is stored in full, and a chosen subset goes on.

**What it is for is different from logs.** Sentry SDKs already send only errors, and errors are not what makes a Sentry bill — transactions are, and so are attachments and runaway issue cardinality. So the selectivity is aimed there: keep every error, sample transactions, hold attachments locally. The local copy is what makes any of those reversible.

**Two upstream identities, because orgs differ:**

| Mode | Upstream key / project | Use when |
|---|---|---|
| `relay` | the client's own | You want events in the projects they already belong to, with existing quotas, alerts and ownership untouched. Only the host changes. |
| `resign` | one configured DSN | You want a single funnel and accept losing per-project routing. The client's key is preserved as an attribute rather than dropped. |

Per-project overrides beat both, so one proxy can sit in front of several upstream projects.

**Byte preservation is a correctness property, not an optimisation.** Item headers and payloads are forwarded exactly as received. Re-serialising the JSON would reorder keys, renormalise numbers and rewrite escapes — and the failure is silent, because Sentry accepts the result and simply shows something subtly different from what the application reported. Only the envelope header is touched, and only to drop `dsn`, which after proxying names *us*.

**Rate limits are honoured in both directions.** Sentry answers `429` with `X-Sentry-Rate-Limits` naming the categories it is refusing. Those are recorded, applied before sending (a limited category is dropped from the envelope, the rest still goes), and reflected back to clients verbatim — so the SDK's own backoff behaves exactly as it would with no proxy in the way. A proxy that swallowed the signal would leave every SDK behind it sending into a wall.

**What an ack means.** A HEC client with `useACK` discards its copy when we say `true`. Saying it on receipt costs nothing and is what a naive implementation does — and it converts the client's durability guarantee into a lie, because a crash between the ack and the `fdatasync` loses data nobody still has. So an ack flips only once the WAL has synced the records it covers: the receiver issues an id at handoff, the record path binds it to the WAL record count it needs, the writer publishes its synced count after each `fdatasync`, and the poll compares the two.

Two consequences fall out of that and are worth stating because they look like limitations until you see what the alternative costs:

- **Acks retire strictly from the bottom.** The "everything below N is durable" floor is one number, so a durable batch above an undurable one cannot be retired early without either creating a hole that answers `false` forever or a floor that answers `true` for data still in flight.
- **A stalled channel is refused, not buffered.** One batch that never reaches disk blocks retirement for its channel; at the cap the receiver answers `503 Server is busy`. The client retries — which is exactly the truth of the situation, and better than an ack map that grows without bound.

**Known issue — an oversized batch 503s forever.** The receiver enqueues a whole batch or none, so a batch larger than `ingest.queue_capacity` (65,536) can *never* succeed. `503 Server is busy` + `Retry-After` tells the client to try later, and later never works: measured 2026-08-07, 100k-event batches retried 60 times each and never landed, burning 127 seconds; the same million events at 5k per batch loaded in 3.2 s at 315,957 events/s with zero retries. Splunk's own default HEC limit is 800 KB per request so real clients batch well below this, which is why it has not bitten — but a 503 that can never clear is a lie about what is wrong. It should be a `413` naming the limit, or the receiver should split the batch. Not yet fixed.

**Why not `prost` + `opentelemetry-proto` + `tonic`?** Same judgement as embedded DuckDB. That stack brings a code generator, a build-time `protoc`, and — via tonic — tokio and an HTTP/2 implementation, into a binary whose entire pitch is that it is small enough to run on every node the customer already owns. We decode exactly one message tree, and the wire format it needs is four wire types and a length prefix: ~190 lines in `otlp/proto.rs`, plus ~250 mapping OTLP logs onto `LogRecord`. The receiver was first served by `tiny_http` (then already a dev-dependency for the fake vendor in tests), so OTLP/HTTP cost **four new crates and ~0.2 MB**, against roughly 40 for the generated stack. Every HTTP/1.1 listener (OTLP/HTTP, HEC, the Sentry proxy, the peer receiver, S3 serve and the admin API) now runs on `bounded_http.rs`, the agent's own HTTP/1.1 server with a head budget, a request deadline and a connection cap, and `tiny_http` is no longer a dependency: it buffered headers without a limit and let slow clients hold its workers (`docs/threat-model.md`).

The bet is that the OTLP logs message tree is frozen — protobuf's compatibility guarantee means field numbers never change, and unknown fields are skipped rather than fatal (tested). If OTLP logs ever break that, this decision is wrong and generated code would have been right.

**Backpressure differs by receiver, deliberately.** A file tailer cannot ask the application to slow down, so the ingest queue sheds by severity. OTLP *can*: a full pipeline answers `503` with `Retry-After`, and the exporter's own retry queue holds the batch. Shedding when the protocol offers a way to say "not now" throws away logs the sender was willing to keep.

**Do we embed Vector or the OTel Collector?** No.
- OTel Collector is Go — can't embed in a Rust binary, and running it alongside kills the single-binary story.
- Vector is Rust/MPL-2.0 but **Datadog-owned**, and we would sit between apps and Datadog — strategic risk, plus a huge dependency graph.
- Write the receivers; steal Vector's protocol conformance tests. These protocols are small and the value is in the storage/pushdown layer.

## 7. Decisions that keep stage 2 additive

- **Adopt the OTel data model internally** — resource / scope / record, `trace_id` and `span_id` as first-class columns, ns timestamps, typed attribute map. Every receiver normalises into it. This single decision makes traces and metrics later "another table" instead of a rewrite.
- **Event ids**: UUIDv7 — time-ordered, mergeable, doubles as the upstream idempotency key. Assigned at ingest, never downstream.
- **Query API**: **Parquet is the API.** Users point their own DuckDB / Grafana / Polars / MCP client at the directory. We expose a replay endpoint (time + service + level predicate) and nothing more. Do not invent a query DSL, do not embed an engine by default. Flight SQL / an embedded SQL endpoint only if demand appears — both are additive.
- **Storage stays Parquet + a versioned catalog schema**, so an aggregation tier or the `ducklake` extension can adopt the same files unchanged.
- **Config**: one declarative TOML/YAML file with a versioned schema, from v1. Retrofitting config schema is painful.

## 8. Six-week prototype — de-risking the three hard things

| Weeks | Risk | De-risk |
|---|---|---|
| 1–2 | **Zero-loss / zero-block ingest under crash + disk pressure** — this invariant *is* the product | Build WAL + shedding path first. Chaos harness: `kill -9` loops mid-fsync, `fallocate` the disk full, throttle io. Assert record accounting: `received == stored + counted_drops`. |
| 2–3 | **Templating throughput/stability at line rate** (Drain tree lock contention, template-id churn) | Bench Rust Drain on LogHub + one ugly real corpus. Targets: ≥500k lines/s/core, bounded template count, <5% untemplated. Miss → fall back to sorted+zstd; architecture must not depend on it. |
| 4–5 | **Pushdown correctness/memory under error storms** | Synthetic storm generator (10k errors/s, correlated and uncorrelated). Assert bounded RSS and "first-N-per-template windows always delivered". |
| 6 | End-to-end | file tail + OTLP in → **Splunk HEC and Sentry out** → replay via the Parquet scanner. Produce the <1h demo from the brief: bytes-avoided counter + one exception carrying its DEBUG context, shown in **both** vendor UIs. |

### v1 scope (locked 2026-08-04)

- **Two destinations, both in v1**: Sentry (envelope API; `template_id` → `fingerprint`; context window as breadcrumbs/attachment on the issue) and Splunk (HEC out; aggregate events as indexed fields). Datadog intake and OTLP-out come later.
- **Deployment: VM / bare metal**, systemd unit, file tail + journald. No k8s packaging, no S3 tiering in the prototype — both arrive together in v2, since node-local durability only becomes illusory once nodes are cattle.
- **Licence: BSL / fair-source**, rolling conversion to Apache-2.0. Practical consequence for this repo: keep all third-party deps permissive (MIT/Apache-2.0). **Vector is MPL-2.0 and Datadog-owned — already rejected in §6, and BSL makes the strategic argument sharper, not softer.**

## Rejected summary

| Proposed | Verdict | Replacement |
|---|---|---|
| DuckLake at the node | Over-engineered, central dependency, catalog contention at scale | Parquet + SQLite catalog; DuckLake at the optional aggregation tier, ingesting the same files |
| Embedded DuckDB in the agent | ~30 MB C++ to serve a filtered scan; its file lock blocks the user's own DuckDB while the agent runs | Parquet-as-API + `parquet`/`arrow-rs` replay scanner; optional `--features duckdb` |
| Reimplementing DuckDB's parts | Not one algorithm — engine + optimizer + MVCC storage, hundreds of KLOC | Don't need it |
| Embedded DataFusion | Same over-serving, large dep graph, not obviously smaller | No embedded engine; `parquet`/`arrow-rs` scan |
| Partition-granular pressure relief | Too few units under burst; freed everything including all errors (measured) | File-granular reclaim, cheapest bucket and oldest first |
| Naive (timezone-less) Parquet timestamps | External engines read them as local time; time filters silently wrong | `Timestamp(ns, "UTC")` |
| Two shutdown flags (`writer_done` + `maint_stop`) | Relaxed atomics let the maintainer see "stop" without "writer finished", so the final segment never merged | One Release/Acquire phase variable |
| Partition-by-level (leading) | Loses time pruning | `(hour, level_class)`, hour first |
| MinHash + LSH clustering | Unstable template ids | Drain (Rust port) with masking heuristics |
| LLM regex derivation in-pipeline | Unnecessary, non-deterministic, online dependency | Heuristic masking; optional offline LLM for template *naming* only |
| Pushdown from committed store | Store is minutes stale | Bounded in-memory sharded ring buffer |
| Deletion vectors for retention | Per-row bookkeeping for a directory-delete problem | File-level drop |
| Embed Vector / OTel Collector | Language mismatch, Datadog ownership, dep bloat | Own receivers |
| `prost` + `opentelemetry-proto` + `tonic` for OTLP | Code generator, build-time `protoc`, tokio and HTTP/2 for one message tree | ~440 lines: a four-wire-type protobuf reader + OTLP→`LogRecord` mapping, served over `bounded_http.rs` (first `tiny_http`, since replaced) |
| Severity shedding on the OTLP path | Discards data the exporter was willing to retry | `503` + `Retry-After`; shedding stays for receivers with no backpressure channel |
| OTel `severity_number = 0` treated as unspecified | Files every record from a producer that omits severity into `debug` — shed first, deleted first, kept a day | Fall back to `severity_text`, then to INFO; keeping too much is the cheaper error |
| `tiny_http::Server::unblock()` as the shutdown signal | Pushes one token and calls `notify_one`, so it wakes exactly one worker; multi-worker shutdown hung on join (observed) | Stop flag + `recv_timeout`, `unblock` as a nudge; `tiny_http` is gone now, and `bounded_http.rs` sets a stop flag, wakes its accept loop with one connection, joins it and waits up to 10 s for open connections |
| Acking a HEC batch on receipt | The client discards its only copy; a crash before the `fdatasync` loses it | Ack keyed to the WAL's synced record count |
| Inferring HEC severity from fields and structured-event keys only | A client sending real log lines as JSON *string* events has nowhere to put a level, so every record defaulted to INFO. Measured on 1M lines from 11 LogHub corpora: **all of them** landed in `level=info`, including every line containing `[error]`. Level partitioning collapsed to one bucket, errors inherited the `info` retention policy, and pushdown never fired (`errors_forwarded=0`) | String events fall back to the level in the line's own prefix. Structured events deliberately do not — their keys are already checked, and scanning rendered JSON would match "error" inside a message |
| Scanning only the first three tokens for a level | Real formats put the level *after* the metadata prefix: Apache at token 7, HDFS and Zookeeper at 4, BGL at 9. Three tokens found none of them | Twelve tokens, **stopping at the first `key=value`** — logfmt pairs mark where a prefix ends and content begins, and no real prefix contains one. A token count alone cannot separate BGL's token-9 `FATAL` from prose at token 12, which would forward `note=the user typed ERROR into the form` upstream and page someone |
| Dropping an ack once the client has seen it `true` (what Splunk does) | Clients re-poll windows of ids, so the second poll would answer `false` and trigger a resend | Polls never mutate; retirement happens from the bottom on the next `issue` |
| Sweeping unreferenced ack watermarks per request | O(outstanding) on every batch — a map sized by the client's backlog, walked per request | Watermark is dropped with the entry that referenced it |
| `tonic` + `prost` for OTLP gRPC | tokio, hyper, tower, h2 and a code generator, to serve one unary method | ~750 lines of HTTP/2 + gRPC framing, and `fluke-hpack` (no transitive deps) for the one part that must be transcribed rather than derived |
| Hand-writing HPACK too | Its Huffman table is 257 RFC-7541 codes that can only be transcribed; one wrong entry corrupts header values silently | `fluke-hpack` |
| Trusting `accept()` to return a blocking socket | BSD/macOS inherit the listener's non-blocking flag, Linux does not — the same code behaved differently per platform (observed) | Explicit `set_nonblocking(false)` on every accepted socket |
| Treating a read timeout as end-of-connection | Mid-frame it abandons a half-read frame, so every later frame is misparsed | Timeouts are only actionable between frames; mid-frame they mean "wait longer" |
| Re-serialising forwarded Sentry payloads | Reorders keys and renormalises numbers; Sentry accepts it and shows something subtly unlike what the app reported | Item headers and payloads forwarded byte-for-byte |
| Splitting envelopes on `\n` | Attachments are arbitrary bytes and contain newlines; the parse would shred one into fake "items" | Length-prefix aware framing, with the newline form as the fallback |
| Dropping unrecognised envelope item types | Breaks every SDK feature newer than this build | Unknown types pass through untouched, under their real name |
| Swallowing upstream `429`s | The SDKs behind the proxy never see the signal and keep sending into a wall | Limits recorded, applied before send, and reflected back verbatim |
| Random transaction sampling | Splits one trace across the boundary; Sentry shows half a transaction tree | Deterministic FNV-1a on the envelope id |
| Storing template id + parameters instead of body text | Measured on 22k real lines: 4.7% smaller at best, 4.4% larger per-file. zstd over the sorted body column already removes the same redundancy | Keep the body; templating earns its place through structure, not size |
| A global cap on context windows being unnecessary | The per-shape budget does nothing about many shapes at once, and the busiest real minute had 80 distinct new templates | `max_windows_per_minute`, default 30 |
| A similarity threshold tuned on synthetic lines | 0.4 over-merged every real corpus (0.59× ground truth, 4/11 within 2×); distinct error shapes shared one fingerprint | 0.8, measured against 11 LogHub corpora (0.99×, 10/11) |
| Scoring template similarity over *all* tokens | The score then depends on how much of the line is masked, so improving the masker splits templates that used to merge (observed: one template became a hundred) | Score over comparable (non-variable) positions only |
| An accounting tolerance derived from the fsync byte budget | Worked out at 65,000 records and reported OK while 30,000 were missing | Zero tolerance by default; slack is opt-in for auditing after a known kill |
| Keeping the outbound queue in memory | A restart loses what a sender was told was accepted and has already discarded | Durable spool with a cursor, fsynced before the acknowledgement |
| A fixed idle sleep in the record loop | Fine for a file, wrong for a socket: exporters fill the handoff channel during the sleep and get a 503 while the agent is idle (observed — one spurious 503 killed a third-party client) | `crossbeam::Select` over the receiver channels, with the poll interval as the timeout |
