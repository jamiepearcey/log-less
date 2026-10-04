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
FROM read_parquet('s3://logs/**/*.parquet', hive_partitioning=true, union_by_name=true)
WHERE hour = '2026-10-03T14'
LIMIT 100;
```

Filter on `hour` (and `level`) so DuckDB prunes partitions rather than listing
and opening every file. `union_by_name=true` is needed because files differ in
which typed `attr_*` columns they hold (`docs/typed-columns.md`); without it
DuckDB takes the first file's schema. No performance figures are claimed here.
