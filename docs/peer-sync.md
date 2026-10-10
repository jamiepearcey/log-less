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
min_severity = 0                      # OTel severity; 13 sends warnings and worse
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

## Not built yet

* **Parquet part upload** (the bulk, cold path with short-lived upload URLs so the edge needs no cloud keys).
* **Query pushdown** (a filtered scan or partial aggregate answered by the instance). Measure first: the S3 path's bytes
  per query, with `scripts/s3-count-proxy.py` in the console repo, decide whether it pays, and keep the edge under the
  2 to 3 MB budget in `docs/platform-thesis.md` section 15, or ship it as a separate `logless-query` binary.
* **Sampled or aggregated spine** (counts and exemplars instead of every record). `min_severity` is the only filter.
