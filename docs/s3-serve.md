# Serving the store over S3

log-less can expose its hive-partitioned Parquet store
(`store/tenant=<t>/index=<i>/hour=YYYY-MM-DDTHH/level=<bucket>/node=<writer>/*.parquet`, where `hour` is the event hour) as a **read-only**
S3-compatible endpoint. DuckDB (httpfs), Polars and Grafana then read the logs
in place with HTTP Range requests, instead of log-less forwarding them to a
vendor. Only reads are implemented: there is no PUT, DELETE or multipart.

## Store-only mode

Leave `destinations` empty and there is no vendor leg at all. The forwarder
thread and spool only start when destinations are non-empty, the outbound send
is guarded, and the Sentry upstream is `None` without a DSN. Audit run: the real
binary with an `init` config (no destinations line), 20000 lines piped in,
reported `received=20000 written=20000`, logged a merge, and left 3 Parquet
files under `store/tenant=default/index=default/hour=.../level={warn_plus,info,debug}/node=<id>`. With
`hive_partitioning = true` the five directories are columns (`tenant`, `index`, `hour`, `level`, `node`); pass
`hive_types = {'tenant': VARCHAR, 'index': VARCHAR, 'node': VARCHAR, 'hour': VARCHAR, 'level': VARCHAR}` if a reader
should not guess their types. `WHERE hour = ...` now selects by event time (`timestamp`), not by when a row arrived.

## Running it

```
# beside ingest
logless run --config /etc/logless.toml --serve-s3 --serve-s3-addr 127.0.0.1:9000

# standalone: no ingest, no WAL; works on a store written by another instance
logless serve --config /etc/logless.toml
```

`serve` runs until SIGINT or SIGTERM, then stops accepting and lets in-flight
downloads finish. Passing `--serve-s3-addr` implies enabled, as `--hec-addr`
does. In `run` the default endpoint is also `127.0.0.1:9000`, the same port the
Sentry proxy example uses; set `--serve-s3-addr` if both run.

## Config

```
[serve_s3]
enabled = false          # off unless asked for
addr = "127.0.0.1:9000"
bucket = "logs"          # the store's bucket
parts_bucket = "parts"   # a second read-only bucket: the parts edges uploaded to this instance's disk
                         # (`[peer.parts]` on the local backend, docs/peer-sync.md), as <node>/<sha256>.parquet.
                         # Served only when that backend is on; any other bucket name is NoSuchBucket
workers = 8              # connections served at once = workers x 16, at least 64
access_key = "..."       # or LOGLESS_S3_ACCESS_KEY
secret_key = "..."       # or LOGLESS_S3_SECRET_KEY
```

## Auth

Requests are verified with AWS Signature V4 (header-signed; presigned URLs are not accepted for reads). Who may sign,
and what each may read, is in [auth.md](auth.md):

* **The `[serve_s3]` key pair** above reads the whole store, as it always has. Both keys must be set (config or
  environment); setting only one is an error.
* **A `[[principals]]` reader** (`kind = "reader"`; its id is the access key id, its `key` the secret) reads its own
  tenant, `tenant=<t>/`, or only the indexes it lists, `tenant=<t>/index=<i>/`, including each index's `_meta/`.
  `tenant = "*"` is the whole store.
* **Temporary credentials** (`x-amz-security-token`), issued by `POST /v1/auth/s3-credentials` on the peer listener
  for a broker token, read what that token's tenant and indexes allow and stop when they expire.

Outside its scope a reader gets `403 AccessDenied` for an object, whether or not it exists, and a listing shows only
what it may read: `s3://logs/**/*.parquet` works for every reader and returns its own files. The `parts` bucket is
kept by node, not by tenant, so only a whole-store reader sees it. An expired session token is `400 ExpiredToken`,
a changed one `400 InvalidToken`.

With no reader of any kind the endpoint serves anonymously, which is accepted on loopback only: log-less refuses to
start on a non-loopback address unless somebody could sign (a reader, or `[auth]` for temporary credentials), so a
misconfiguration cannot publish the store to the network. There is no TLS; put a proxy in front for anything beyond
loopback.

## Limits

The listener is `bounded_http.rs`, the same small HTTP/1.1 server as the admin API: the request line and headers
together are capped at 16 KiB and 64 headers (a longer one is a 431, which also bounds the query string and a `Range`
header), a client has 15 s to send its request, and connections past `workers` x 16 (at least 64) are closed unread. Every
reply is `Connection: close`, so each request is its own connection (DuckDB opens many; that is cheap on loopback and a
LAN). A download is streamed from the file; a client that stops reading is cut off after 10 s of no progress on a write.
A listing is at most 1000 entries a page whatever `max-keys` says. The threat model is `docs/threat-model.md`.

## DuckDB

```
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='127.0.0.1:9000';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='YOUR_ACCESS_KEY';
SET s3_secret_access_key='YOUR_SECRET_KEY';

SELECT *
FROM read_parquet('s3://logs/**/*.parquet', hive_partitioning=true, union_by_name=true)
WHERE hour = '2026-10-03T14'
LIMIT 100;
```

Filter on `hour` (and `level`) so DuckDB prunes partitions rather than listing
and opening every file. `union_by_name=true` is needed because files differ in
which typed `attr_*` columns they hold (`docs/typed-columns.md`); without it
DuckDB takes the first file's schema. No performance figures are claimed here.

## Template dictionary and shape history (`_meta/`)

Log rows carry only `template_id`: the 64-bit content hash of the masked template text, so equal shapes have equal ids
on every node. The text lives in the catalog's SQLite. After every merge that commits a segment, log-less rewrites, for
each `(tenant, index)` that has rows, two files for its own node id (temp file, fsync, rename):

```
tenant=<t>/index=<i>/_meta/templates/writer=<node>.pq       the dictionary
tenant=<t>/index=<i>/_meta/shape_history/writer=<node>.pq   the shape history
```

The endpoint serves them as those keys. Besides `*.parquet` it serves any `.pq` key with a `_meta` path segment and
nothing else. The `.pq` extension is deliberate: a `**/*.parquet` glob never sweeps them into a logs query. Each writer
owns its own file, so no two writers write one key; a reader unions `_meta/templates/*.pq` (ids agree across writers
because they are hashes). `logless templates --export` rewrites them on demand.

Dictionary, one row per `(template_id, service)` (footer `logless.templates_version = 2`):

| column        | Parquet / DuckDB type                    | null | meaning |
|---------------|------------------------------------------|------|---------|
| `template_id` | UINT64 / `UBIGINT`                       | no   | equals the logs' `template_id`; the hash of `text` |
| `service`     | UTF8 / `VARCHAR`                         | yes  | equals the logs' `service`; null for rows without one |
| `text`        | UTF8 / `VARCHAR`                         | no   | the text `template_id` hashes (masked tokens such as `<NUM>`, `<IP>`; `<*>` for mined wildcards) |
| `count`       | UINT64 / `UBIGINT`                       | no   | rows merged with this (template, service) |
| `first_seen`  | TIMESTAMP(MICROS, UTC) / `TIMESTAMPTZ`   | no   | earliest `observed` of a merged row with this (template, service) |
| `last_seen`   | TIMESTAMP(MICROS, UTC) / `TIMESTAMPTZ`   | no   | latest `observed` of a merged row with this (template, service) |
| `lineage_id`  | UINT64 / `UBIGINT`                       | no   | the id the template was minted with; unchanged when it widens; what the Sentry forwarder fingerprints with |

Shape history, one row per `(lineage_id, template_id)` (footer `logless.shape_history_version = 1`). It is a separate
file because its grain differs from the dictionary's, and rows of two grains in `_meta/templates/` would be mixed by
`union_by_name`:

| column        | type                | meaning |
|---------------|---------------------|---------|
| `lineage_id`  | UINT64              | as above |
| `template_id` | UINT64              | a hash this lineage has held; rows carry it |
| `text`        | UTF8                | the text that hash is of |
| `count`       | UINT64              | rows merged carrying it |
| `first_ts`, `last_ts` | TIMESTAMP(MICROS, UTC) | earliest and latest `observed` of a row carrying it |

- `tenant` and `index` are the directories (hive columns), not columns of the files.
- One row per `(template_id, service)` pair ever merged whose text this writer knows, so a join on `template_id` plus
  `service IS NOT DISTINCT FROM` matches exactly one row. A hash this writer never mined (an edge minted it and this
  node kept the record unmined) has no row; the edge's own file has it.
- Widening changes the hash from that moment; rows already written keep theirs. Group by `lineage_id` (join
  `shape_history` on `template_id`) to see a widened template as one issue.
- Two different texts with one hash are one shape to a query; the first text registered is kept and the collision is
  counted in the catalog (`template_hash_collisions`).
- `first_seen`/`last_seen` describe rows as merged; retention does not move them.
- Parquet written before ids were hashes carries counters; `logless migrate-layout` re-keys it.

```
CREATE VIEW log_templates AS
  SELECT * FROM read_parquet('s3://logs/tenant=*/index=*/_meta/templates/*.pq', hive_partitioning=true, union_by_name=true);

SELECT t.text, count(*)
FROM read_parquet('s3://logs/**/*.parquet', hive_partitioning=true, union_by_name=true) l
JOIN log_templates t
  ON l.template_id = t.template_id AND l.service IS NOT DISTINCT FROM t.service
  AND l.tenant = t.tenant AND l.index = t.index
WHERE l.hour = '2026-10-03T14'
GROUP BY t.text;
```

A union of several writers' dictionaries has one row per writer for the same `(template_id, service)`: sum `count`,
take the min of `first_seen` and the max of `last_seen`.

Checked against a local store of 22,000 merged lines (6 templates, no service) with the earlier single-file
dictionary, before content-hash ids (the join above was checked on a current store: `docs/evidence/template-hash-ids.md`):
the file was 2,829 bytes, DuckDB read the types above, and the join matched all
22,000 rows.
