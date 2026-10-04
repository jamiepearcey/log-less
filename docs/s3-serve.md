# Serving the store over S3

log-less can expose its hive-partitioned Parquet store
(`store/hour=YYYY-MM-DDTHH/level=<bucket>/*.parquet`) as a **read-only**
S3-compatible endpoint. DuckDB (httpfs), Polars and Grafana then read the logs
in place with HTTP Range requests, instead of log-less forwarding them to a
vendor. Only reads are implemented: there is no PUT, DELETE or multipart.

## Store-only mode

Leave `destinations` empty and there is no vendor leg at all. The forwarder
thread and spool only start when destinations are non-empty, the outbound send
is guarded, and the Sentry upstream is `None` without a DSN. Audit run: the real
binary with an `init` config (no destinations line), 20000 lines piped in,
reported `received=20000 written=20000`, logged a merge, and left 3 Parquet
files under `store/hour=.../level={warn_plus,info,debug}`.

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
bucket = "logs"          # the only bucket name served
workers = 8
access_key = "..."       # or LOGLESS_S3_ACCESS_KEY
secret_key = "..."       # or LOGLESS_S3_SECRET_KEY
```

## Auth

Requests are verified with AWS Signature V4 against the configured key pair.
Both keys must be set (config or environment); setting only one is an error.
With neither set the endpoint serves anonymously, which is accepted on loopback
only: log-less refuses to start on a non-loopback address without credentials,
so a misconfiguration cannot publish the store to the network. There is no TLS;
put a proxy in front for anything beyond loopback.

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
FROM read_parquet('s3://logs/**/*.parquet', hive_partitioning=true)
WHERE hour = '2026-10-03T14'
LIMIT 100;
```

Filter on `hour` (and `level`) so DuckDB prunes partitions rather than listing
and opening every file. No performance figures are claimed here.

## Template dictionary (`_meta/templates.pq`)

Log rows carry only `template_id`; the template text lives in the catalog's
SQLite. After every merge that commits a segment, log-less rewrites
`store/_meta/templates.pq` from the catalog (temp file, fsync, rename), and the
endpoint serves it as the key `_meta/templates.pq`. It is the only key served
besides `*.parquet`. The `.pq` extension is deliberate: a `**/*.parquet` glob
never sweeps it into a logs query. `logless templates --export` rewrites it on
demand.

| column        | Parquet / DuckDB type                    | null | meaning |
|---------------|------------------------------------------|------|---------|
| `template_id` | UINT64 / `UBIGINT`                       | no   | equals the logs' `template_id`; a counter local to `agent` |
| `service`     | UTF8 / `VARCHAR`                         | yes  | equals the logs' `service`; null for rows without one |
| `text`        | UTF8 / `VARCHAR`                         | no   | current template text (masked tokens such as `<NUM>`, `<IP>`; `<*>` for mined wildcards) |
| `first_seen`  | TIMESTAMP(MICROS, UTC) / `TIMESTAMPTZ`   | no   | earliest `observed` of a merged row with this (template, service) |
| `last_seen`   | TIMESTAMP(MICROS, UTC) / `TIMESTAMPTZ`   | no   | latest `observed` of a merged row with this (template, service) |
| `agent`       | UTF8 / `VARCHAR`                         | no   | the id space `template_id` was minted in: a UUID per catalog |
| `global_id`   | UINT64 / `UBIGINT`                       | no   | FNV-1a 64 of `text`; equal across agents and services for equal text |

- One row per `(template_id, service)` pair ever merged, so a join on
  `template_id` plus `service IS NOT DISTINCT FROM` matches exactly one row.
  Templates no merged row carries yet are not listed.
- `template_id` is a per-catalog counter: two agents can both call different
  shapes `7`. `(agent, template_id)` is unambiguous; `global_id` is the key for
  merging one shape across services or agents. It is the value the Sentry
  forwarder fingerprints with, so it changes when a template widens.
- `first_seen`/`last_seen` describe rows as merged; retention does not move them.
- A lost catalog restarts ids under a new `agent`, so rows written before the
  loss keep ids the new dictionary no longer describes.
- The footer carries `logless.templates_version = 1`.

```
CREATE VIEW log_templates AS
  SELECT * FROM read_parquet('s3://logs/_meta/templates.pq');

SELECT t.text, count(*)
FROM read_parquet('s3://logs/**/*.parquet', hive_partitioning=true) l
JOIN log_templates t
  ON l.template_id = t.template_id AND l.service IS NOT DISTINCT FROM t.service
WHERE l.hour = '2026-10-03T14'
GROUP BY t.text;
```

Checked against a local store of 22,000 merged lines (6 templates, no service):
the file was 2,829 bytes, DuckDB read the types above, and the join matched all
22,000 rows.
