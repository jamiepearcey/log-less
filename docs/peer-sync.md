# Peer sync: one log-less sending to another

A log-less instance is a collector (OTLP, HEC, Sentry, files) and a forwarder. Pointing one at another is therefore
one URL. The peer is addressed as itself; what it can do is advertised, not configured.

```toml
# On the central instance
[peer]
enabled = true
addr = "0.0.0.0:9300"
tokens = ["<long random token>"]     # required on any non-loopback address

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
full; the peer never slows the write path. Status (`sent_records`, `duplicates`, `loops`, `backlog_bytes`,
`dropped_records`, `last_error`) is on `GET /admin/v1/sync`.

| Peer answer | Sender does |
|---|---|
| 200 | commit; count accepted, duplicate and loop records |
| 401 / 403 | keep the data, report "refused the token", retry every 60 s (a settings change retries at once) |
| 400 / 413 / 415 / 422 | drop that batch, record why |
| 503 / 429 / 5xx / network | keep the data, back off 1 s doubling to 60 s |
| 508 | batch was all loops: counted, dropped |

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

* Each accepted request's ids are appended and synced in one write, after the pipeline took the batch and before the
  200 reply. Ingest pays one sync per request, not per record.
* At start the last `dedupe_window` ids are loaded; a torn id at the end (a crash mid-append) is cut off.
* Once the file holds twice the window it is rewritten to the window alone (temporary file, sync, rename), so it stays
  under `2 x dedupe_window x 16` bytes (16 MB at the default).
* A batch the pipeline refused (503) is not remembered, so its retry is not taken for a duplicate.

Crash ordering:

| Crash | Effect |
|---|---|
| after the pipeline took the batch, before its ids were saved | the sender never heard 200 and resends; the receiver takes the batch again (a duplicate in the store if the first copy reached the WAL) |
| after the ids were saved, before the 200 | the resend is reported as duplicates and committed by the sender |
| retry older than the window | taken again: at-least-once |

**Open gap.** In the agent "the pipeline took it" means the batch entered the bounded in-memory channel ahead of the
WAL, not that the WAL has it. The 200 (and the saved ids) can therefore precede the WAL write, and a crash in that
window loses the batch: the sender has committed, and a resend after the ids were saved would be dropped as a
duplicate. Closing it means replying only after the WAL synced the batch, as HEC acks do. Not built yet.

If the ids cannot be saved, the batch is still accepted (it is in the pipeline), `dedupe_unsaved` in the peer stats
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
```

The capability document says `parts: true` only when this is enabled and complete; an incomplete table is logged (not
the secret) and the endpoints answer 404. Both endpoints take the same bearer token and body limits as `/v1/ingest`.

| Step | Request | Central does |
|---|---|---|
| 1 | `POST /v1/parts/presign {name, size, sha256}` | checks the name is `<64 hex sha256>.parquet` and matches `sha256`, and that `size` is 1 to `max_part_bytes` (422 otherwise); HEADs `<prefix>/<name>` with `x-amz-checksum-mode: ENABLED`; answers `already_present: true` only if the object has that size and storage reports the SHA-256 checksum of the name, else a presigned PUT `url` valid `url_ttl_secs`, its `expires_at_ms`, and `headers: {x-amz-checksum-sha256}` (signed into the URL) |
| 2 | `PUT <url>` with `headers` (edge to object storage) | nothing: the file goes straight to the bucket, and the storage refuses a body whose SHA-256 is not the one signed |
| 3 | `POST /v1/parts/commit {name, size, sha256, rows, min_unix_nano, max_unix_nano, origin}` | HEADs the object and needs that size and checksum (409 otherwise), records it in the catalog's `peer_parts` table, answers `{recorded}`; a repeated commit records nothing |

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

## Not built yet

* **Query pushdown** (a filtered scan or partial aggregate answered by the instance). Measure first: the S3 path's bytes
  per query, with `scripts/s3-count-proxy.py` in the console repo, decide whether it pays, and keep the edge under the
  2 to 3 MB budget in `docs/platform-thesis.md` section 15, or ship it as a separate `logless-query` binary.
