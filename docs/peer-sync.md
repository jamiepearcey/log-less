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

`tokens` is the simple form and stays: each entry is read as an ingest principal of the central's own tenant. A
central with more than one tenant gives each sender a `[[principals]]` key (or a session-broker token) bound to its
tenant, presented the same way (`token = "..."` on the edge, `Authorization: Bearer` on the wire); the tenant its
records are stored under is the credential's, never the request's. Kinds, tenants, tokens and the
`POST /v1/auth/s3-credentials` route this listener also serves are in [auth.md](auth.md).

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

`GET /.well-known/logless` returns what the peer is and offers: version, instance id (its node id: `[node] id`, or the one
it generated and keeps in `<data_dir>/node-id`), wire formats, auth, size limit,
`max_hops`, whether it serves its store over S3, and its clock (so a sender reports skew). It needs the token when the
peer has tokens, so an unauthenticated probe learns nothing about the build. `parts` is `true` when the peer takes
Parquet part uploads (see "Uploading Parquet parts"); where a part goes is in each presign answer, not here. `query` is in the document and is `false`: scan pushdown is not
built (see below).

## What flows

`POST /v1/ingest`, `application/x-ndjson` (optionally `Content-Encoding: gzip`), one `LogRecord` per line
(`logless-ndjson/1`). OTLP cannot carry what a log-less hop needs, so this path keeps:

* the **event id**, so a retried batch is dropped by the receiver (`dedupe_window` recent ids, kept on disk; see
  "Duplicates and restarts");
* the **event time and the observed time**, both as the sender had them. The store is partitioned on event time
  (`timestamp`), so a backlog lands in the hour it happened whenever it is sent; rows more than `storage.late_threshold`
  (2 h) old on arrival go through the receiver's late spool (`docs/architecture.md` section 2);
* the **index**: a record the sender filed under an index other than its default carries `logless.index`, and the
  receiver files it under its own index of that name (or, if it has none, its default index or nowhere, by its
  `unknown_index`; counted in `GET /admin/v1/info`);
* the **template id** the sender already mined: the content hash of the template text, the same on every node, so the
  receiver keeps it without mining and rollups for one shape from different senders add up. A record sent without one is
  mined by the receiver's own miner. (A receiver does not learn the text of a hash it never mined; its dictionary has no
  row for it until dictionaries travel with the parts.)

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

Every record carries `logless.origin` (the first sender's node id, kept by every later hop) and `logless.hops`.
A receiver refuses a record whose origin is itself or whose hops reached 8, so a ring stops after one lap. The
attributes stay in the store, and the origin is also the `origin` column of every row (for a row an instance received
itself, its own node id), so "which node produced this line" is one column centrally, whoever wrote the file.

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

The cold path: an edge copies whole committed Parquet files to the central instance. The edge pushes; it asks central
where to put each part and sends it to the URL it gets back. Central keeps parts in one of two places, and the edge
neither knows nor is configured for which (`docs/adr/0003-parts-push-to-central-local-backend.md`):

* **its own disk** (`backend = "local"`, the default): nothing else to run. The bytes pass through central's peer
  listener, streamed to disk;
* **an S3-compatible bucket** (`backend = "s3"`): central holds the credentials and presigns, so the edge needs no
  cloud keys and the bytes go straight to the bucket.

```toml
# On the central instance: parts on its own disk. This is the whole table.
[peer.parts]
enabled = true
# dir = "/var/lib/logless/parts"                  # default <data_dir>/parts
disk_budget_bytes = 68719476736                   # 64 GiB, the default; uploads past it get 507. 0: no budget
max_part_bytes = 536870912                        # 512 MiB, the default
url_ttl_secs = 900                                # how long an upload token (or presigned URL) works, the default
min_upload_bytes_per_sec = 16384                  # local: slowest average upload rate, 16 KiB/s by default (see "Slow links")
warm_retention_secs = 0                           # expire uploaded parts whose newest data is older than this (0: keep)
cold_retention_secs = 0                           # the same for parts the node has dropped locally

# Or: parts in a bucket. Naming an endpoint is what selects it (backend = "s3" says so outright).
[peer.parts]
enabled = true
endpoint = "https://s3.eu-west-2.amazonaws.com"   # http(s)://host[:port]; objects are addressed path-style
bucket = "logless-parts"
prefix = "edges"                                  # key prefix, may be empty
region = "eu-west-2"
access_key_id = "..."
secret_access_key = "..."                         # never logged, serialized or returned

[[peer.nodes]]                                    # one per edge: its token IS its identity and its prefix
id = "edge-1"                                     # A-Za-z0-9._- , 1 to 64 characters, not starting with "."; unique ignoring case
token = "..."                                     # the edge's [[destinations]] token; distinct per node
```

**Node-owned prefixes.** A part lives at `<prefix>/<node>/<name>` in the bucket, or `<dir>/<node>/<name>` on central's
disk, where `<node>` is the id of the `[[peer.nodes]]` entry whose token made the request (or of the `kind = "node"` principal, `docs/auth.md`). Central builds the key; a request cannot choose it. A body that names another node is 403
`foreign_prefix`, a name that is not `<64 hex>.parquet` (traversal, slashes, query strings) is 422 `bad_name`, and a
shared `[peer] tokens` token owns no prefix (403 `no_node`). The catalog is keyed `(node, name)`. Decision and rejected
alternatives: `docs/adr/0001-parts-tiering-and-lifecycle.md`.

Node ids must be unique ignoring case (`edge-a` and `EDGE-A` are refused at start): on a case-insensitive filesystem
they would be one directory. An id may not start with a dot, because dot-named entries are skipped when the disk budget
is counted and by S3 serve. At start central logs which backend is in force. A table that names none and has no
`endpoint` is local, but if it also sets `bucket`, `prefix` or keys it looks like an S3 table that lost its endpoint, and
the listener refuses to start rather than put the parts on this disk unannounced; set `endpoint`, or `backend = "local"`
to keep them here.

The capability document says `parts: true` only when this is enabled and complete. An incomplete S3 table is logged
(not the secret) and the endpoints answer 404; a local table that cannot work (the directory cannot be created, a
budget under `max_part_bytes`) stops the peer listener at start with the reason. The JSON endpoints take the same bearer token, body limits and `Content-Type` rule
as `/v1/ingest`: the body is JSON sent as `application/json` (the edge does), and a request with no `Content-Type` or a
form or text one is a 415 before it is read, so a web page cannot post here without a preflight.

| Step | Request | Central does |
|---|---|---|
| 1 | `POST /v1/parts/presign {name, size, sha256}` | checks the name is `<64 hex sha256>.parquet` and matches `sha256`, and that `size` is 1 to `max_part_bytes` (422 otherwise); answers `already_present: true` if the part is stored intact, else a PUT `url` valid `url_ttl_secs`, its `expires_at_ms`, and the `headers` the PUT must carry |
| 2 | `PUT <url>` with `headers` | stores the part only if its SHA-256 is the name (see the two backends below) |
| 3 | `POST /v1/parts/commit {name, size, sha256, rows, min_unix_nano, max_unix_nano, origin}` | needs the part stored intact (409 `not_uploaded` otherwise), records it in the catalog's `peer_parts` table, answers `{recorded}`; a repeated commit records nothing |

| 4 | `POST /v1/parts/retire {name}` | the node says it no longer holds its local copy: the part goes from `warm` to `cold` (404 if this node has no warm part of that name); audited |

Parts are named by their content, so every step is idempotent: the same file is the same part. What steps 1 and 2 mean
on each backend:

| | `local` | `s3` |
|---|---|---|
| `url` | the path `/v1/parts/blob/<node>/<name>` on central's peer listener; the edge resolves it against the peer URL it already uses, so central need not know its public name | an absolute presigned URL for `<prefix>/<node>/<name>` |
| `headers` | `x-logless-upload-token` (a token for this node, name and size, valid `url_ttl_secs`) and `expect: 100-continue` | `x-amz-checksum-sha256` (signed into the URL) |
| who checks the bytes | central: it hashes the body while streaming it to a temporary file, and renames it into place only if the SHA-256 is the name | the storage: it refuses a body whose SHA-256 is not the one signed |
| stored intact means | a regular file of that name and size; only a verified upload can have put it there | a HEAD with `x-amz-checksum-mode: ENABLED` answers that size and the checksum of the name |
| the PUT answers | 200 `{stored, already_present}`; 403 `bad_token` / `token_expired`; 409 `upload_in_progress` (the same part is already arriving; `Retry-After: 1`); 400 `size_mismatch` / `incomplete_body`; 408 `upload_stalled`; 413 `too_large`; 422 `bad_content` / `bad_name`; 507 `insufficient_storage` | the storage's own |

On S3 an object of the right size without the name's checksum (written some other way, or with other bytes) is not
present, so it is uploaded again and a commit before that is refused. The checksum is what the storage recorded at
upload; bit rot under the storage is the storage's to catch (MinIO refuses to serve such an object), not central's. Run
against a real MinIO: `docs/evidence/parts-minio.md`.

On the local backend the PUT needs no bearer token: the upload token names the node, the part and its size, and only
central could have signed it (HMAC-SHA256 with a key made at start and kept in memory; a restart voids outstanding
tokens and the edge presigns again). Everything that can be refused from the request head is refused before the body is
read. The body is never held in memory: it goes to `<dir>/<node>/.upload-<id>.tmp` in 256 KiB pieces, and a body that
is short, stalls or hashes to something else leaves nothing behind. A good one is `fsync`ed, renamed to its name, its
directory `fsync`ed, and then answered 200. A crash at any point leaves a part absent or whole, never partial; a crash
after the rename and before the commit leaves it present, and the edge's retry commits it. The token format, the
refusals in order and the timeouts are in ADR 0003; the run with a 128 MiB part and central's memory during it is in
`docs/evidence/parts-local-backend.md`.

**Slow links (local).** A part must arrive at an average of at least `min_upload_bytes_per_sec` (16 KiB/s by default,
about 0.13 Mbit/s) after a 30 s allowance, and no single read may stall for 30 s; otherwise 408 `upload_stalled` and
nothing is kept (there is no resume, the next attempt starts at byte 0). A 512 MiB part therefore needs an uplink above
about 16 KiB/s to finish within the 9 hours that rule allows; lower the setting for slower links, at the cost of a
connection and a budget reservation held for longer. An edge that is cut off with 408 twice in a row on one part marks
that file rejected ("too slow") and goes on to the next; the first 408 alone is retried. The S3 backend has no such floor.

**Disk budget (local).** Installed parts plus uploads in flight may not pass `disk_budget_bytes`. A presign or an
upload that would is answered 507 `insufficient_storage` with `Retry-After: 60`; the edge retries with backoff and then
stops the job with that error. `GET /admin/v1/sync` on central reports `parts_store`: `backend`, `dir`, `used_bytes`,
`reserved_bytes`, `budget_bytes`, `uploads`, `upload_bytes`, `already_present`, and the refusals `refused_budget`,
`refused_token`, `refused_size`, `refused_content`, `incomplete`. It is `null` on the S3 backend.

**Reading the parts (local).** With `[serve_s3]` enabled on central, the parts directory is a second read-only bucket
(`parts_bucket`, default `parts`): `s3://parts/<node>/<name>`, under the same credentials and rules as the store's
bucket. On S3 the parts are read from their bucket directly.

On the edge, `POST /admin/v1/sync/parts {"from_ms", "to_ms"}` starts an upload of the committed files that overlap the
range, in the background (at most 31 days, one at a time, 409 otherwise; `POST /admin/v1/sync/parts/cancel` stops it).
It runs only when the peer's capability document says `parts: true`. For each file it computes the sha256, asks for a
URL, streams the file to it and commits. Progress is `parts` in `GET /admin/v1/sync`: `state`, `files_total`,
`uploaded`, `skipped` (committed by an earlier run), `already_present` (central had it; committed without uploading),
`rejected` (refused by central, such as too large), `bytes`, `error`.

* **Resumable.** Each committed file is a line in `parts-uploaded.jsonl` in the data dir, so a rerun or a restart skips
  it without hashing it again. Losing that file costs a HEAD per part, not an upload.
* **Retries.** A failed PUT or a central 5xx is retried with backoff (six attempts); a URL or upload token that expired
  or was refused (403) is presigned again at once. Out of attempts, the job stops with an error and the next run carries on.
* **Edge-triggered.** The edge pushes; central cannot ask for a range (the timer is the edge asking itself). It reads committed files only, so the last few
  minutes (not yet merged from the WAL) are not included, and a file compacted after upload is a new part with the same
  rows: a reader of the parts dedupes on event id, as with records.

### Tiers, listing and retention

| Tier | Meaning | In the central catalog |
|---|---|---|
| `hot` | a Parquet file only the node has | no: it is the node's local file |
| `warm` | uploaded and committed; the node may still hold its copy | yes |
| `cold` | the node dropped its copy; central's is the only one (on the local backend: one file on one disk) | yes |

On central's admin API (bearer token as the rest of `/admin/v1`):

* `GET /admin/v1/parts?node=&tier=warm|cold&from_ms=&to_ms=&limit=` lists catalogued parts, oldest data first
  (`limit` 1 to 1000, default 100; a part matches a time range when its observed-time span overlaps it: the part
  catalog still records observed time, unlike the store's own file table, which records both).
  `tier=hot` is a 400 that says why. Each part carries `node, name, size, rows, min_unix_nano, max_unix_nano,
  origin, committed_ms, tier, tier_ms`.
* `POST /admin/v1/parts/sweep {"dry_run": false}` expires catalogued parts whose newest data is older than their
  tier's `*_retention_secs`: it deletes the object (or the file), removes the row and writes an audit row in one step
  per part. It reads only the catalog, so it never deletes a part nobody committed. The default is a dry run; at most 1000
  parts per tier per call (`truncated` says there were more). A failed delete keeps the row and audits
  `delete_failed`. Needs `[peer.parts]` enabled (409 `parts_disabled` otherwise).
* `GET /admin/v1/parts/audit?limit=` lists tier moves and expiries, newest first (`peer_parts_audit` in the catalog).

Not built: a timer for the central sweep. The edge side of this lifecycle (timer, pinning, retire) is next.

### The edge: keeping parts until central has them, telling it when they are gone, and a timer

```toml
# On the edge. All of it is off until interval is set.
[sync_parts]
interval = "5m"            # run the upload job every 5 minutes; "0s" (the default) is off; otherwise at least 10s
pin_bytes = 1073741824     # hold back from deletion at most this many bytes of files central has not confirmed (1 GiB, the default)
pin_age = "7days"          # and none older than this (the default)
```

**The timer.** With `interval` set, a thread starts the same upload job `POST /admin/v1/sync/parts` starts, over every
file central has not confirmed, whatever its age (a manual upload is limited to 31 days; the timer's is not). It
needs a peer set, and a peer that says `parts: true`, like the manual job. There is one upload job at a time on an edge,
whoever starts it: a tick that finds a manual job running does nothing and counts it in `skipped_busy`, and a manual
request during the timer's run gets the usual 409. The first run is at start, the next `interval` after each start.
*Default off*, because turning it on moves bytes off the machine, needs a node token the operator has to have set up,
and (below) holds files back from the deletes that protect the disk. None of that should begin because someone upgraded.

**Pinning.** A closed Parquet file with no line in `parts-uploaded.jsonl` is the only copy of its rows. Retention and
disk-pressure reclaim skip such files: retention deletes the other files of an expired partition and leaves the
pinned ones (the partition goes when they are uploaded or unpinned); reclaim leaves them out of its candidates. The
pinned set is the newest unconfirmed files that fit in `pin_bytes` and are younger than `pin_age` (by when the file
was written), recomputed every maintenance pass (5 s), and never more than a quarter of `storage.disk_budget_bytes`,
so pinning cannot be what fills the disk. Past those limits the edge behaves as it always did and **counts what it
deleted**: files, rows and bytes, in `edge_parts.gave_up_*`, in a warning log line with the same numbers, and in
`parts-gaveup.jsonl` in the data dir, which is summed at start so the totals survive a restart. Pinning is on exactly
when the timer is on and a peer is set: without a timer nothing is going to upload what it holds. `logless retention`
run by hand is an explicit deletion and ignores pins. The policy preview (`/admin/v1/policies/preview`) does not
know about pins and may predict more deletion than the next pass will do.

**Retire.** A line in `parts-uploaded.jsonl` whose file is gone from the store is a pending retire, whatever removed the
file (retention, reclaim, compaction, an operator). Every 10 s the edge sends `POST /v1/parts/retire {name}` for each,
in order, and forgets the line once central answered 200, or 404 `no_such_part` (cold already, or never warm), or
400/422 (never going to be accepted, logged). The queue is the state file itself, so a restart forgets nothing; a
crash between the answer and forgetting sends one more retire, which central answers 404. Any other answer, or no
answer, stops the pass, records `retire_error`, and backs off 1 s doubling to 5 min; the lines stay. If two uploaded
files had the same content, the part stays warm until both are gone. Central turns the part `cold` and audits it, and
keeps the object: it is now the only copy.

**Compaction.** Compaction replaces several small files with one merged file. The merged file is new content with a new
name, so it is an unconfirmed file: pinned, and uploaded by the next run. The files it replaced, if they had been
uploaded, are retired. Central briefly holds the same rows twice (the old parts, cold, and the merged one, warm) until
the old parts expire; readers of the parts dedupe on event id, as they must for a file compacted after upload anyway.
Nothing is left warm on central for a file that no longer exists on the edge.

**Status.** `edge_parts` in `GET /admin/v1/sync` on the edge (there is no separate metrics endpoint in log-less): the
timer (`interval_secs`, `last_run_ms`, `last_run_state`, `last_run_uploaded`, `last_run_error`, `next_run_ms`, `runs`,
`skipped_busy`), the backlog (`backlog_parts`, `backlog_bytes`: unconfirmed files, refreshed every 5 s), the pins
(`pin_enabled`, `pin_limit_bytes`, `pin_age_secs`, `pinned_parts`, `pinned_bytes`, `unpinned_parts`, `unpinned_bytes`:
unconfirmed files reclaim may take), what was given up (`gave_up_parts`, `gave_up_rows`, `gave_up_bytes`) and the
retires (`retire_pending`, `retired`, `retire_error`).

### Damage to a stored part: what is and is not checked

This section is about the S3 backend; the local backend's version is the paragraph at its end.

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

On the local backend central is the storage. It verifies a part once, as it arrives, and afterwards accepts it as
present by name and size: **a byte that changes on central's disk later is not detected by central**, and there is no
scrub. `logless restore` hashes every part it reads and refuses a damaged one by name. To check by hand, in a node's
directory: `for f in *.parquet; do [ "$(shasum -a 256 < "$f" | cut -c1-64).parquet" = "$f" ] || echo "DAMAGED $f"; done`.
A damaged part is repaired as above: remove the file and its `peer_parts` row, and upload it again from the edge if the
edge still has it. A cold part exists only on that disk; ADR 0003 says what each configuration survives.

## Replicating parts between centrals

A second central can hold a copy of the first one's parts, so that losing the first one's disk is not losing every cold
part (`docs/adr/0003-parts-push-to-central-local-backend.md`, "Replica"). The replica **pulls** from the source by catalog
diff; the edge still pushes to one central only, and no central connects to an edge. Parts are immutable and named by
SHA-256, so there is no conflict to resolve, only a difference to close.

```toml
# On the source central (local backend): who may read its part catalog and its parts. Its own credential.
[peer.parts]
enabled = true
replication_tokens = ["<long random token, different from every node and ingest token>"]

# On the replica central (local backend too). It is an ordinary central: it may take uploads from its own edges as well.
[peer]
enabled = true
addr = "127.0.0.1:9300"
tokens = ["<its own ingest token>"]               # as any central: required on a non-loopback address
[peer.parts]
enabled = true
[peer.parts.replicate]
url = "https://central-a.example.com:9300"        # the source's peer listener
token = "<one of the source's replication_tokens>"
interval_secs = 300                               # a run starts this often; 0: only when asked
bytes_per_sec = 0                                 # 0: unlimited; otherwise the copy is paced to this rate
page_size = 500                                   # catalog entries per request, 1 to 1000
```

| Step | Request (replica to source, bearer replication token) | Source does |
|---|---|---|
| 1 | `GET /v1/parts/replica/catalog?after=<node>/<name>&limit=N` | answers `{parts: [{node, name, size, sha256, rows, min_unix_nano, max_unix_nano, origin, committed_ms, tier, tier_ms}], next}` in `(node, name)` order after `after`; `next` is the last entry's key when the page was full, `null` at the end |
| 2 | `GET /v1/parts/replica/blob/<node>/<name>` with `Range: bytes=<n>-` when resuming | streams the file (200, or 206 with `Content-Range`); only a *committed* part is served, and only if its file is the catalogued size (404 `not_catalogued` / `part_missing` otherwise); 416 past the end |

Both paths need a `replication_tokens` token and nothing else does: no token is 401, any other token (an ingest token, a
node token, the replica's own) is 403, and a central with no `replication_tokens` answers 404 as if the paths did not
exist. A replication token opens nothing else on the peer listener (presign, ingest and the capability document are 403 to
it). Refusals are counted in `parts_store.refused_replica` on the source, with `replica_pages` and `replica_served`
(responses started).

**A run.** It pages the source's catalog; for each entry it checks the replica's own catalog:

* **present**: already catalogued here. Nothing is done and nothing is fetched. (`tier_differs` counts the ones whose tier
  is not the source's: tiers are each central's own after the first copy.)
* **adopted**: the file is already on this disk under its name and size (a crash between the rename and the catalog row, or
  an upload nobody committed here). It is recorded without fetching, on the rule that a file under its final name is a
  whole verified part (ADR 0003).
* **fetched**: not here. The bytes go to `<dir>/<node>/.fetch-<sha256>.tmp` in pieces, hashed as they arrive, reserved
  against `disk_budget_bytes`. Only a body whose SHA-256 is the name is `fsync`ed, renamed to `<dir>/<node>/<name>`
  (directories synced) and recorded under its **original node** with the source's size, rows, time range, origin and tier,
  and an audit row (`action = replicate`, "copied from <source>"). A restore from the replica therefore finds the parts
  where an edge's inventory says they are.
* **resumed**: a fetch that was cut off (a network failure, a restart, a `kill -9`) keeps its `.fetch-*.tmp`. The next run
  hashes what is there and asks for the rest with `Range`; `resumed_bytes` is what it did not fetch again. A source that
  answers 200 to a range is read again from byte 0.
* **damaged**: the bytes do not hash to the name, or the source serves a size other than the listed one. Refused: the
  temporary file is removed, nothing is catalogued, the run goes on, and the part is named in `problems`.
* **unservable**: listed by the source but not served (its file is gone or not the catalogued size). Not recorded, named in
  `problems`, retried by the next run.
* **invalid**: the entry could not be a file in a node directory (a node or name that is not valid, a SHA-256 that is not
  the name, a size over `max_part_bytes`, a tier other than `warm`/`cold`). Skipped; the replica does not trust a listing
  with where to write.
* **only_here**: held here, not listed by the source. See below.

A source or link that fails (unreachable, a 5xx, a refused token, a full replica disk, a listing out of order) ends the run
with that reason in `error`, so a down source is not hammered once per part; `complete` is true only when the listing was
read to its end. Runs never overlap (a manual one while one is under way is 409 `already_running`). A restart of the
replica keeps its catalog and its partial copies; the run counter and the last report are in memory.

**It never deletes.** A part retired or swept on the source (or never there) stays on the replica and is reported as
`only_here` (with the first twenty names) on a complete run. Deletion propagation is not built and not decided; the
replica expires what it holds by its own `warm_retention_secs` / `cold_retention_secs` and sweep (`POST
/admin/v1/parts/sweep`). Likewise a retirement (`warm` to `cold`) after the first copy is not copied.

**Admin API on the replica.** `POST /admin/v1/parts/replicate` starts a run in the background (202; 409 `not_a_replica` /
`already_running` / `parts_disabled`); `GET /admin/v1/parts/replicate` and `parts_replication` in `GET /admin/v1/sync` show
`source`, `state` (`idle` or `running`), `runs`, and `run`, the current or last run: `started_ms`, `finished_ms`,
`complete`, `error`, `listed`, `present`, `adopted`, `fetched`, `fetched_bytes`, `resumed`, `resumed_bytes`, `damaged`,
`unservable`, `invalid`, `tier_differs`, `only_here`, `problems`, `only_here_sample`. The token is never shown.

**A source on the S3 backend** answers both paths 409 `not_local`, and a replica reports "keeps its parts in a bucket" and
copies nothing. A bucket is copied with the storage's own tools. A replica on the S3 backend is a configuration error and
stops the peer listener at start.

**Restoring from the replica.** The replica's directory and catalog are a central's, so `logless restore` reads them as it
reads the first one's: `--from <replica data_dir>/parts/<node> --inventory <replica catalog.sqlite> --node <node>`
(`docs/restore.md`). `docs/evidence/parts-replication.md` has a drill that destroys the first central and the edge and
restores from the replica.

## Not built yet

* **Query pushdown** (a filtered scan or partial aggregate answered by the instance). Measure first: the S3 path's bytes
  per query, with `scripts/s3-count-proxy.py` in the console repo, decide whether it pays, and keep the edge under the
  2 to 3 MB budget in `docs/platform-thesis.md` section 15, or ship it as a separate `logless-query` binary.
