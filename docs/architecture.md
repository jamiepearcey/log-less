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

- **Partitioned Parquet + a local SQLite catalog.** Catalog tables: files (path, partition keys, min/max ts, row count, template-dict version), templates (id, text, first_seen, counters). Atomic commit = write Parquet to temp → fsync → insert catalog row in one transaction.
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

**Measured (2026-08-04, arm64 macOS, `lto=thin`, `strip=true`, `panic=abort`):**

| Build | Stripped release binary |
|---|---|
| model, config, WAL, partition, retention, CLI | 1.5 MB |
| **+ arrow, parquet, rusqlite (current)** | **7.3 MB** |
| + duckdb | not built; DuckDB static-links ~30 MB → **~5× the current binary** |
| + datafusion | not needed; the replay scanner uses `parquet`/`arrow-rs` directly |

Settled. Even after the storage stack landed, embedding a query engine would still be the majority of the binary — to serve a filtered scan we already do in ~300 lines. Users bring their own engine to the Parquet directory instead, verified working below.
- **DuckLake is rejected at the node tier** and adopted only at the *optional* aggregation tier. Reasons: every merge becomes a catalog transaction, so at 500 nodes × frequent small commits Postgres is a serialisation point (commit conflicts, snapshot/metadata churn, small-file explosion, or catalog bloat via inlining); and 500 agents would need Postgres credentials and connectivity, reintroducing a central dependency into a product whose pitch is "local and cheap".
- **Cluster tier design**: agents ship *completed Parquet files* to the tier; one (or few) writers commit them to DuckLake. DuckLake v1.0 (April 2026) is production-ready and earns its keep there — snapshots, time travel, multi-writer, Iceberg-compatible deletion vectors.

### Delivery semantics

At-least-once with idempotency keys (UUIDv7 per event, assigned at ingest). Exactly-once to Splunk/Sentry is fiction — we don't control their dedupe. Per-destination cursor (segment id + offset) lives in its own small file (atomic rename, not the catalog — higher write rate); replay from cursor on restart.

## 2. Partitioning and retention

- **Partition key: `(hour, level_class)`** — hour **first**. `level_class ∈ {debug, info, warn_plus}` — coarse buckets, not seven levels, to keep file counts sane.
- Hour-first gives time pruning (every real query is time-bounded). Level second exists for exactly one reason: **retention becomes `rm -rf` of a directory** plus a catalog delete.
- **Sort order inside files**: `(service, trace_id, timestamp)` where trace_id exists, else `(service, timestamp)`. This clusters templates (good for zstd + dictionary encoding) and makes "everything for trace X" a range scan. Write row-group stats; the replay scanner (and any external engine) prunes on them.
- **Timestamps are UTC-qualified in the Parquet schema** (`Timestamp(ns, "UTC")`), not naive. Written without a timezone, DuckDB and Polars read them as *local* time and every "last hour" filter silently returns the wrong rows — observed in testing, where an errors-in-the-last-hour query returned 0 instead of 17,088. The schema is the long-lived contract; this had to be right before anyone builds on it.
- **Retention = file-level drop only.** Deletion vectors are for compliance deletes, not time expiry — never pay per-row bookkeeping for something a directory delete solves. A file straddling a boundary just lives a few extra hours.
- Partition-by-level does **not** fight pushdown: pushdown is served from memory (§4), and cross-partition reads on one local disk are cheap.

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
**Measured (2026-08-04, release build, arm64):**

| Metric | Result | Target |
|---|---|---|
| Throughput | **775,617 lines/s/core** | ≥500k — met |
| Templates from 2M synthetic lines | 15 | bounded — met |
| Templates from 200k mixed production-shaped lines | 15, covering 100% | <5% untemplated — met (0%) |
| Store compression **with** templating | 2.4× | unchanged from 2.5× without |

Note the debug build manages only ~39k lines/s — the target holds only when optimised, so never benchmark this in a debug profile.

The compression row is the important one and it confirms the reasoning above rather than contradicting it: `template_id` is assigned but `body` is still stored in full, so nothing shrinks yet. Ratio gains need the follow-up step (store template id + parameter columns *instead of* the body text). Templating earns its place today through **structure** — stable ids for novelty detection, rate baselines and Sentry fingerprints.

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

**Why not `prost` + `opentelemetry-proto` + `tonic`?** Same judgement as embedded DuckDB. That stack brings a code generator, a build-time `protoc`, and — via tonic — tokio and an HTTP/2 implementation, into a binary whose entire pitch is that it is small enough to run on every node the customer already owns. We decode exactly one message tree, and the wire format it needs is four wire types and a length prefix: ~190 lines in `otlp/proto.rs`, plus ~250 mapping OTLP logs onto `LogRecord`. The receiver itself is `tiny_http` (already a dev-dependency for the fake vendor in tests), so OTLP/HTTP cost **four new crates and ~0.2 MB**, against roughly 40 for the generated stack.

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
| `prost` + `opentelemetry-proto` + `tonic` for OTLP | Code generator, build-time `protoc`, tokio and HTTP/2 for one message tree | ~440 lines: a four-wire-type protobuf reader + OTLP→`LogRecord` mapping, served over `tiny_http` |
| Severity shedding on the OTLP path | Discards data the exporter was willing to retry | `503` + `Retry-After`; shedding stays for receivers with no backpressure channel |
| OTel `severity_number = 0` treated as unspecified | Files every record from a producer that omits severity into `debug` — shed first, deleted first, kept a day | Fall back to `severity_text`, then to INFO; keeping too much is the cheaper error |
| `tiny_http::Server::unblock()` as the shutdown signal | Pushes one token and calls `notify_one`, so it wakes exactly one worker; multi-worker shutdown hung on join (observed) | Stop flag + `recv_timeout`; `unblock` demoted to a nudge |
| Acking a HEC batch on receipt | The client discards its only copy; a crash before the `fdatasync` loses it | Ack keyed to the WAL's synced record count |
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
| A similarity threshold tuned on synthetic lines | 0.4 over-merged every real corpus (0.59× ground truth, 4/11 within 2×); distinct error shapes shared one fingerprint | 0.8, measured against 11 LogHub corpora (0.99×, 10/11) |
| Scoring template similarity over *all* tokens | The score then depends on how much of the line is masked, so improving the masker splits templates that used to merge (observed: one template became a hundred) | Score over comparable (non-variable) positions only |
| An accounting tolerance derived from the fsync byte budget | Worked out at 65,000 records and reported OK while 30,000 were missing | Zero tolerance by default; slack is opt-in for auditing after a known kill |
| Keeping the outbound queue in memory | A restart loses what a sender was told was accepted and has already discarded | Durable spool with a cursor, fsynced before the acknowledgement |
| A fixed idle sleep in the record loop | Fine for a file, wrong for a socket: exporters fill the handoff channel during the sleep and get a 503 while the agent is idle (observed — one spurious 503 killed a third-party client) | `crossbeam::Select` over the receiver channels, with the poll interval as the timeout |
