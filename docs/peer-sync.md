# Peer sync: one log-less sending to another

A log-less instance is a collector (OTLP, HEC, Sentry, files) and a forwarder. Pointing one at another is therefore
one URL. The peer is addressed as itself; what it can do is advertised, not configured.

```toml
# On the central instance
[peer]
enabled = true
addr = "0.0.0.0:9300"
tokens = ["<long random token>"]     # required on any non-loopback address, each at least 24 characters there

# On an edge instance
[[destinations]]
type = "logless"
url = "https://central.example.com:9300"
token = "<the same token>"
min_severity = 0                      # OTel severity floor; 13 takes warnings and worse, nothing below is sent or counted
fidelity = 100                        # 1 to 100: see "How much is sent"
anomaly_boost = true                  # send extra detail by itself when something unusual happens
```

The admin API (`GET`/`PUT /admin/v1/sync`, `POST /admin/v1/sync/test`) edits the same setting at run time. Its copy
is kept in `sync.json` (and the token in `sync-token`, mode 0600) in the data dir, outranks the config file, and applies
to the running sender with no restart. The console's Admin > Sync screen drives it.

## What the receiver refuses

The listener is `bounded_http.rs`, shared with the admin API. The token and (without tokens) the `Host` header are
checked from the request head, before any body is read or decompressed: an unauthenticated client gets a 401 and costs
no memory. Then: head over 16 KiB or 64 headers is a 431; the whole request must arrive within 30 s; connections past
`workers` x 32 (at least 64) are closed; chunked bodies are a 501; `Content-Type` must be `application/x-ndjson` or
`application/json` (a request with none is a 415, which is also what keeps a web page from posting here without asking)
and `Content-Encoding` must be `gzip` or absent; a body over `max_body_bytes` is a 413, and so is one that inflates past
`max_decompressed_bytes`; one bad line rejects the whole batch with a 400 and stores nothing. Without tokens the
listener binds loopback only and refuses a `Host` that is not a loopback name (DNS rebinding). Two `Authorization`
headers count as none. The full list, with the test that covers each, is in `docs/threat-model.md`.

## The handshake

`GET /.well-known/logless` returns what the peer is and offers: version, instance id, wire formats, auth, size limit,
`max_hops`, whether it serves its store over S3, and its clock (so a sender reports skew). It needs the token when the
peer has tokens, so an unauthenticated probe learns nothing about the build. `parts` is `true` when the peer presigns
Parquet part uploads (see "Uploading Parquet parts"). `query` is in the document and is `false`: scan pushdown is not
built (see below).

## What flows

`POST /v1/ingest`, `application/x-ndjson` (optionally `Content-Encoding: gzip`), one `LogRecord` per line
(`logless-ndjson/1`). OTLP cannot carry what a log-less hop needs, so this path keeps:

* the **event id**, so a retried batch is dropped by the receiver (`dedupe_window` recent ids, kept on disk; see
  "Duplicates and restarts");
* the **observed time**, which partitions the store, so a backlog lands in the hour it happened;
* the **template id** the sender already mined.

A plain OTLP collector remains a valid destination through the OTLP receiver; this is the log-less-to-log-less path.

## Delivery

Accepted records are tapped off the WAL writer, filtered by `min_severity`, stamped, and appended to a durable spool
(`sync-spool/`, capped at 256 MiB). A sender thread drains the spool and commits its cursor only after the peer answered
2xx, so an outage, a restart or a crash resends and never drops. The tap is a bounded channel that sheds and counts when
full; the peer never slows the write path. Status (`sent_records`, `duplicates`, `loops`, `busy_answers`,
`backlog_bytes`, `dropped_records`, `last_error`) is on `GET /admin/v1/sync`.

| Peer answer | Sender does |
|---|---|
| 200 | commit; count accepted, duplicate and loop records |
| 401 / 403 | keep the data, report "refused the token", retry every 60 s (a settings change retries at once) |
| 400 / 413 / 415 / 422 | drop that batch, record why |
| 503 / 429 | keep the data, back off 1 s doubling to 60 s, count it in `busy_answers` (the peer's queue or WAL is behind) |
| 5xx / network | keep the data, back off 1 s doubling to 60 s |
| 508 | batch was all loops: counted, dropped |

## What a 200 means

A 200 from the peer means **the peer's WAL has the records on disk**: written and `fdatasync`ed. The sender commits
its cursor on a 200 and forgets the batch, so nothing weaker is safe. The receiver puts the batch on a bounded queue
ahead of the WAL writer and waits, as HEC indexer acks do, until the writer reports a sync that covers it; only then
does it save the batch's ids (below) and answer.

| Receiver state | Answer |
|---|---|
| WAL synced the batch | 200 `{"accepted", "duplicates", "loops"}` |
| queue ahead of the WAL full (or the ingest queue has no room for the whole batch) | 503 `busy`, `Retry-After: 1`; nothing kept, so the resend is new |
| WAL has not synced within `peer.ack_wait_ms` (default 5000) | 503 `wal_slow`, `Retry-After`; the batch is still on its way (see below) |
| shutting down with the batch not yet synced | 503 `shutting_down`, `Retry-After: 1` |

The wait is the next fsync: at most `storage.wal_fsync_interval` (100 ms by default) plus one writer pass (20 ms),
since the writer syncs on that interval. Measured on one machine with the defaults, sequential 50-record batches:
p50 6 to 8 ms and p99 17 to 27 ms before, p50 112 to 121 ms and p99 164 to 172 ms now, over three runs each
(`docs/evidence/peer-ack-wal.md`). One sender sends its
batches one at a time, so its throughput is about one batch (up to 1 MiB) per fsync interval; more senders share a
sync. `ack_wait_ms` should be well above the fsync interval (a warning is logged at start when it is under twice
that) and below the sender's 15 s request timeout.

A batch that timed out stays in the pipeline. If it syncs later, the next request to arrive saves its ids first, so
its resend (the sender retries after the backoff) is reported as duplicates rather than stored again. A resend that
arrives before that late sync is taken again: one duplicate copy.

The receiver's counters are `receiver` in `GET /admin/v1/sync` on the peer: `wal_waits`, `wal_wait_us_total` and
`wal_wait_us_max` (time spent waiting for the fsync), `wal_timeouts` (503 `wal_slow`), `busy` (503 `busy`),
`wal_closed`, `settled_late`. A climbing `wal_timeouts` or a `wal_wait_us_max` near `ack_wait_ms` is a peer whose disk
is behind; on the sender it shows as `busy_answers`.

The cursor commit is checked. If the spool cannot record that a batch was answered (a full or read-only disk), the
cursor does not move, status goes to `error` with "The peer has the batch, but the sync spool could not record
that", and the sender backs off and resends; the peer drops the copies as duplicates. It recovers by itself once the
cursor can be written. A failed spool flush is reported the same way.

`docs/evidence/sync-soak.md` has a soak through a fault-injecting proxy with every record accounted for (minutes of
injected faults, not the 72-hour soak).

## Duplicates and restarts

The receiver remembers the event ids it accepted, the last `dedupe_window` of them (500,000 by default), and drops a
record whose id it already holds, reporting it in `duplicates`. The window is kept in `peer-dedupe.ids` in the data
dir (16 bytes per id), so it survives a restart:

* Each accepted request's ids are appended and synced in one write, after the WAL synced the batch and before the
  200 reply. Ingest pays one sync per request, not per record.
* At start the last `dedupe_window` ids are loaded; a torn id at the end (a crash mid-append) is cut off.
* Once the file holds twice the window it is rewritten to the window alone (temporary file, sync, rename), so it stays
  under `2 x dedupe_window x 16` bytes (16 MB at the default).
* A batch refused with 503 is not remembered, so its retry is not taken for a duplicate (a `wal_slow` batch is
  remembered only once it has synced, as above).

The order is **WAL synced, then ids saved, then 200**. Crash ordering:

| Crash | Effect |
|---|---|
| before the batch reached the WAL, or reached it but was not synced | the sender never heard 200 and resends; the ids were never saved, so the resend is taken. If the unsynced WAL write survived (a process crash keeps the page cache; a power loss does not), the restart also replays it: one duplicate copy. No loss either way |
| after the WAL sync, before the ids were saved | the WAL replays the batch and the sender resends it, which is taken: one duplicate copy in the store |
| after the ids were saved, before the 200 | the resend is reported as duplicates and committed by the sender |
| retry older than the window | taken again: at-least-once |

The store does not dedupe by event id, so a duplicate copy stays as a second row with the same `event_id`; the
receiver's window absorbs every resend whose ids it saved. `docs/evidence/peer-ack-wal.md` has a real `kill -9` of the
central agent mid-wait: the sender's cursor did not move, every record arrived, and the batch the killed process had
written to its WAL was stored twice.

If the ids cannot be saved, the batch is still acknowledged (the WAL has it), `dedupe_unsaved` in the peer stats
counts it and a warning is logged: this process still recognises a resend, a restart may not.

## Loops and provenance

Every record carries `logless.origin` (the first sender's instance id, kept by every later hop) and `logless.hops`.
A receiver refuses a record whose origin is itself or whose hops reached 8, so a ring stops after one lap. The
attributes stay in the store: "which node produced this line" is answerable centrally.

Provenance is strict. A record is refused as a loop (and counted in `loops`) when `logless.hops` is not a whole number
from 0 up (an integer, or a string of digits only: `"lots"`, `"-1"`, `"+1"`, `-5`, `1.0` and `true` are all refused),
when `logless.origin` is not a non-empty string, or when either attribute appears twice (a second `hops = 0` cannot
reset a chain that ran out). A sender writes exactly one of each; given forged provenance it writes `hops = 8`, which
the next receiver refuses, and it never writes more than 8.

## How much is sent: the detail dial

A peer can run from a summary to a full copy. `fidelity` (1 to 100) is the percentage of records below errors
that are sent in full.

* **Errors and fatals are always sent in full**, whatever the dial says.
* **Below that, `fidelity` percent are sent.** Which ones is decided by the event id (`sync_policy::select`), not a
  random draw, so a retry, a replay and a manual fetch all agree about what was already sent. Raising the dial only
  adds records.
* **What is left out is counted, not lost.** Each minute the peer receives one *rollup* record per (service, template,
  level class) with the exact count left out (`logless.rollup`, `logless.rollup.count`, `.window_ms`, `.class`,
  `.first_ns`, `.last_ns`). At 1 the peer holds every distinct thing that happened with exact totals.
* **Reading a peer's store:** records carrying `logless.supplement` (a lead-up record, or a record fetched by hand) are
  extra detail for counts a rollup already gave. A total is the rollup counts plus the records without it.

## Anomalies push extra through

With `anomaly_boost` the sender watches for two things and acts by itself:

| Signal | Rule |
|---|---|
| **Novel log shape** | a template id the catalog never held (read at start, so a restart is not a wave of novelty); with an empty catalog the first minute is spent learning |
| **Rate spike** | a service's warnings and worse in a 10 s bucket at least 20, at least 3 times its own running mean and more than 4 standard deviations above it, after a minute of baseline |

On either it (1) **sends the lead-up**: the last minute of that service's records that had been left out, from a bounded
in-memory ring (20,000 records), flagged `logless.supplement = lookback:<kind>`; (2) **boosts that service to full
detail for 5 minutes**; and (3) sends a **marker record** (`logless.anomaly`, `.kind`, `.lookback`) the peer can alert
on without knowing the sender's heuristics. The same service alarming again within 30 s while boosted is logged once.

More signals (`sync_signals.rs`), each against the instance's own baseline, in memory and bounded:

| `kind` | Noticed | Boosts |
|---|---|---|
| `template_rate` | one template's rate far above its own baseline | only if the flood includes warnings or worse |
| `silence` | a regular template, or a whole service, stopped (once per episode; a silent service is one finding) | no |
| `new_pair` | a known template in a service for the first time (pairs read from the catalog) | yes |
| `error_share` | errors a much larger share of a service's records, even as traffic falls | yes |
| `param_drift` | a number inside a template (latency, size, count) shifted well off its bucket-by-bucket baseline | yes |
| `flow` | two templates in one trace in an order never seen (per service) | yes |

A stall of over an hour resets the baselines rather than calling every service silent.

The planner's state (ring, baselines, unflushed rollups, boosts) is in memory: a restart forgets the current minute's
rollups and relearns baselines for about a minute.

## Fetching a range by hand

`POST /admin/v1/sync/backfill {"from_ms", "to_ms"}` reads that range back from the committed Parquet and sends every record
in it at full detail, in the background (`scan::scan_each`: oldest partition first, no row limit, one file in memory at a
time). At most 31 days per fetch and one at a time; `POST /admin/v1/sync/backfill/cancel` stops it. The peer drops what it
already holds, so overlapping a range is safe and only costs bandwidth. Records the dial had left out carry
`logless.supplement = backfill`. It reads committed files, so the last few minutes (not yet merged from the WAL) may be
missing. The agent pushes: a peer behind another network can be sent a range, but cannot ask for one.

## Uploading Parquet parts

The cold path: an edge copies whole committed Parquet files to the central instance's object storage, without cloud
keys of its own and without the bytes passing through central. S3-compatible storage only.

```toml
# On the central instance
[peer.parts]
enabled = true
endpoint = "https://s3.eu-west-2.amazonaws.com"   # http(s)://host[:port]; objects are addressed path-style
bucket = "logless-parts"
prefix = "edges"                                  # key prefix, may be empty
region = "eu-west-2"
access_key_id = "..."
secret_access_key = "..."                         # never logged, serialized or returned
max_part_bytes = 536870912                        # 512 MiB, the default
url_ttl_secs = 900                                # how long an upload URL works, the default
warm_retention_secs = 0                           # expire uploaded parts whose newest data is older than this (0: keep)
cold_retention_secs = 0                           # the same for parts the node has dropped locally

[[peer.nodes]]                                    # one per edge: its token IS its identity and its prefix
id = "edge-1"                                     # A-Za-z0-9._- , 1 to 64 characters, not "." or ".."
token = "..."                                     # the edge's [[destinations]] token; distinct per node
```

**Node-owned prefixes.** A part lives at `<prefix>/<node>/<name>`, where `<node>` is the id of the `[[peer.nodes]]` entry
whose token made the request. Central builds the key; a request cannot choose it. A body that names another node is 403
`foreign_prefix`, a name that is not `<64 hex>.parquet` (traversal, slashes, query strings) is 422 `bad_name`, and a
shared `[peer] tokens` token owns no prefix (403 `no_node`). The catalog is keyed `(node, name)`. Decision and rejected
alternatives: `docs/adr/0001-parts-tiering-and-lifecycle.md`.

The capability document says `parts: true` only when this is enabled and complete; an incomplete table is logged (not
the secret) and the endpoints answer 404. Both endpoints take the same bearer token, body limits and `Content-Type` rule
as `/v1/ingest`: the body is JSON sent as `application/json` (the edge does), and a request with no `Content-Type` or a
form or text one is a 415 before it is read, so a web page cannot post here without a preflight.

| Step | Request | Central does |
|---|---|---|
| 1 | `POST /v1/parts/presign {name, size, sha256}` | checks the name is `<64 hex sha256>.parquet` and matches `sha256`, and that `size` is 1 to `max_part_bytes` (422 otherwise); HEADs `<prefix>/<node>/<name>` with `x-amz-checksum-mode: ENABLED`; answers `already_present: true` only if the object has that size and storage reports the SHA-256 checksum of the name, else a presigned PUT `url` valid `url_ttl_secs`, its `expires_at_ms`, and `headers: {x-amz-checksum-sha256}` (signed into the URL) |
| 2 | `PUT <url>` with `headers` (edge to object storage) | nothing: the file goes straight to the bucket, and the storage refuses a body whose SHA-256 is not the one signed |
| 3 | `POST /v1/parts/commit {name, size, sha256, rows, min_unix_nano, max_unix_nano, origin}` | HEADs the object and needs that size and checksum (409 otherwise), records it in the catalog's `peer_parts` table, answers `{recorded}`; a repeated commit records nothing |

| 4 | `POST /v1/parts/retire {name}` | the node says it no longer holds its local copy: the part goes from `warm` to `cold` (404 if this node has no warm part of that name); audited |

Parts are named by their content, so every step is idempotent: the same file is the same object. The content is
checked by the storage, through S3's SHA-256 checksum: an object of the right size without the name's checksum (written
some other way, or with other bytes) is not present, so it is uploaded again and a commit before that is refused. The
checksum is what the storage recorded at upload; bit rot under the storage is the storage's to catch (MinIO refuses to
serve such an object), not central's. Run against a real MinIO: `docs/evidence/parts-minio.md`.

On the edge, `POST /admin/v1/sync/parts {"from_ms", "to_ms"}` starts an upload of the committed files that overlap the
range, in the background (at most 31 days, one at a time, 409 otherwise; `POST /admin/v1/sync/parts/cancel` stops it).
It runs only when the peer's capability document says `parts: true`. For each file it computes the sha256, asks for a
URL, streams the file to it and commits. Progress is `parts` in `GET /admin/v1/sync`: `state`, `files_total`,
`uploaded`, `skipped` (committed by an earlier run), `already_present` (central had it; committed without uploading),
`rejected` (refused by central, such as too large), `bytes`, `error`.

* **Resumable.** Each committed file is a line in `parts-uploaded.jsonl` in the data dir, so a rerun or a restart skips
  it without hashing it again. Losing that file costs a HEAD per part, not an upload.
* **Retries.** A failed PUT or a central 5xx is retried with backoff (six attempts); a URL that expired or was refused
  (403) is presigned again at once. Out of attempts, the job stops with an error and the next run carries on.
* **Edge-triggered.** The edge pushes; central cannot ask for a range. It reads committed files only, so the last few
  minutes (not yet merged from the WAL) are not included, and a file compacted after upload is a new part with the same
  rows: a reader of the bucket dedupes on event id, as with records.

### Tiers, listing and retention

| Tier | Meaning | In the central catalog |
|---|---|---|
| `hot` | a Parquet file only the node has | no: it is the node's local file |
| `warm` | uploaded and committed; the node may still hold its copy | yes |
| `cold` | the node dropped its copy; the object is the only one | yes |

On central's admin API (bearer token as the rest of `/admin/v1`):

* `GET /admin/v1/parts?node=&tier=warm|cold&from_ms=&to_ms=&limit=` lists catalogued parts, oldest data first
  (`limit` 1 to 1000, default 100; a part matches a time range when its observed-time span overlaps it).
  `tier=hot` is a 400 that says why. Each part carries `node, name, size, rows, min_unix_nano, max_unix_nano,
  origin, committed_ms, tier, tier_ms`.
* `POST /admin/v1/parts/sweep {"dry_run": false}` expires catalogued parts whose newest data is older than their
  tier's `*_retention_secs`: it deletes the object, removes the row and writes an audit row in one step per part. It
  reads only the catalog, so it never deletes an object nobody committed. The default is a dry run; at most 1000
  parts per tier per call (`truncated` says there were more). A failed delete keeps the row and audits
  `delete_failed`. Needs `[peer.parts]` enabled (409 `parts_disabled` otherwise).
* `GET /admin/v1/parts/audit?limit=` lists tier moves and expiries, newest first (`peer_parts_audit` in the catalog).

Not built: the edge calling `retire` on its own when its retention removes a file, and a timer for the sweep.

### Damage to a stored part: what is and is not checked

Central trusts the storage for the integrity of what it holds. When a part is uploaded the storage verifies the content against
the SHA-256 in the part's name, and central later accepts a part as present when a HEAD returns the same size and the same
recorded checksum. Central never downloads a part to hash it again, so **a byte that changes inside the storage afterwards (bitrot,
a failed disk, tampering with the data files) while the size stays the same is not detected by log-less.** Measured on MinIO
(`docs/evidence/parts-minio.md`): HEAD keeps returning the checksum recorded at upload, while MinIO's own bitrot check refuses to
serve the damaged object, so a read of it fails.

What an operator does about it:

* Run the storage's own scrub. On MinIO that is its background healing and scanner (`mc admin heal` monitors healing and `mc admin scanner`
  shows the scanner; check both in your MinIO release, since the admin commands change between versions); on AWS S3 and other managed stores durability and checksum scrubbing are the provider's. Run it on a
  schedule the retention policy can live with.
* A read that fails with a checksum or bitrot error means the part is damaged. Delete the object (under `<prefix>/<node>/`) and its row in `peer_parts`, then
  upload the part again from the edge (`POST /admin/v1/sync/parts` over the range, after removing its entry from
  `parts-uploaded.jsonl` in the edge's data dir if the edge still has the file). Parts whose edge copy has been retired cannot be
  rebuilt this way: keep storage-level replication or versioning for them.
* This was a decision, not an oversight: a periodic hash-everything job would make central carry every part's bytes, which the
  design avoids. If that trade changes, the job belongs on the edge or beside the storage, not in the request path.

## Not built yet

* **Query pushdown** (a filtered scan or partial aggregate answered by the instance). Measure first: the S3 path's bytes
  per query, with `scripts/s3-count-proxy.py` in the console repo, decide whether it pays, and keep the edge under the
  2 to 3 MB budget in `docs/platform-thesis.md` section 15, or ship it as a separate `logless-query` binary.
