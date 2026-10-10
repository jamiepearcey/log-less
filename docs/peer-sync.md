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
peer has tokens, so an unauthenticated probe learns nothing about the build. `query` and `parts` are in the document and
are `false`: scan pushdown and Parquet-part upload are not built (see below).

## What flows

`POST /v1/ingest`, `application/x-ndjson` (optionally `Content-Encoding: gzip`), one `LogRecord` per line
(`logless-ndjson/1`). OTLP cannot carry what a log-less hop needs, so this path keeps:

* the **event id**, so a retried batch is dropped by the receiver (`dedupe_window` recent ids, memory only);
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

## Loops and provenance

Every record carries `logless.origin` (the first sender's instance id, kept by every later hop) and `logless.hops`.
A receiver refuses a record whose origin is itself or whose hops reached 8, so a ring stops after one lap. The
attributes stay in the store: "which node produced this line" is answerable centrally.

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

The planner's state (ring, baselines, unflushed rollups, boosts) is in memory: a restart forgets the current minute's
rollups and relearns baselines for about a minute.

## Fetching a range by hand

`POST /admin/v1/sync/backfill {"from_ms", "to_ms"}` reads that range back from the committed Parquet and sends every record
in it at full detail, in the background (`scan::scan_each`: oldest partition first, no row limit, one file in memory at a
time). At most 31 days per fetch and one at a time; `POST /admin/v1/sync/backfill/cancel` stops it. The peer drops what it
already holds, so overlapping a range is safe and only costs bandwidth. Records the dial had left out carry
`logless.supplement = backfill`. It reads committed files, so the last few minutes (not yet merged from the WAL) may be
missing. The agent pushes: a peer behind another network can be sent a range, but cannot ask for one.

## Not built yet

* **Parquet part upload** (the bulk, cold path with short-lived upload URLs so the edge needs no cloud keys).
* **Query pushdown** (a filtered scan or partial aggregate answered by the instance). Measure first: the S3 path's bytes
  per query, with `scripts/s3-count-proxy.py` in the console repo, decide whether it pays, and keep the edge under the
  2 to 3 MB budget in `docs/platform-thesis.md` section 15, or ship it as a separate `logless-query` binary.
* **More detectors:** per-template rate and silence, a known template appearing in a service for the first time, error
  share rather than error count, numeric parameter drift, call-sequence novelty (`flow_hash`).
