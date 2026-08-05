# log-less

Keep every log locally and cheaply. Forward only what's worth paying for.

log-less is a single Rust binary that runs on your own nodes, in front of your
existing observability vendor. It holds full-verbosity logs on local disk with
per-level retention, and forwards a curated subset upstream — so filtering stops
being a one-way door.

**Status: early. Weeks 1–2 of the prototype.** Working and tested: WAL with
crash recovery, severity load shedding, config-driven level buckets, merge to
partitioned Parquet, SQLite catalog with rebuild-from-Parquet, retention, and
disk-pressure relief, template mining (masking + Drain, ~775k lines/s/core),
smart pushdown, forwarding to Sentry and Splunk, a file-tail receiver that
survives rotation, an OTLP/HTTP receiver verified against the official
OpenTelemetry SDK over both HTTP and gRPC, and a Splunk HEC receiver whose
acknowledgements are keyed to a real `fdatasync`, and a Sentry ingest proxy
verified against the real `sentry-sdk`.

## Why

Vendors price on ingest volume, so teams raise log levels to save money and then
can't debug the incident. Existing pipelines (Vector, Cribl, Edge Delta) let you
drop data before it's billed — but dropped is dropped. Cheap second backends keep
the data and split your workflow across two UIs.

log-less keeps the data *and* keeps your vendor as the single pane of glass:

- **Every error arrives with the debug logs that caused it** — and you never paid
  to ship debug.
- **Turn yesterday's verbosity up, today** — replay historical TRACE/DEBUG into
  your vendor mid-incident.
- **Get paged about log patterns your vendor never saw** — anomaly detection on
  the 95% you didn't forward.

See [`.context/project-brief.md`](.context/project-brief.md) for positioning and
[`docs/architecture.md`](docs/architecture.md) for design decisions and their
rejected alternatives.

## Try what exists

```sh
cargo build --release

# Generate a config
./target/release/logless init --data-dir /tmp/logless-data > /tmp/logless.toml

# See the level buckets and their retention
./target/release/logless buckets --config /tmp/logless.toml

# Ingest tab-separated "<LEVEL>\t<message>" lines from stdin
printf 'ERROR\tboom\nDEBUG\tcontext\n' | ./target/release/logless run --config /tmp/logless.toml

# Or tail real log files (survives logrotate; resumes from a checkpoint).
# Runs until SIGTERM/SIGINT, then drains cleanly.
./target/release/logless run --config /tmp/logless.toml --tail /var/log/app.log

# Or accept OTLP/HTTP logs, so an existing OTel Collector or Vector fleet only
# has to change its endpoint. Off unless asked for; loopback unless told
# otherwise. Both sources can run at once.
./target/release/logless run --config /tmp/logless.toml --otlp

#   exporters:
#     otlphttp:
#       endpoint: http://127.0.0.1:4318

# ...or gRPC on :4317, which is what an unconfigured Collector and most SDK
# defaults use. Plaintext h2c; no TLS.
./target/release/logless run --config /tmp/logless.toml --otlp-grpc

#   exporters:
#     otlp:
#       endpoint: 127.0.0.1:4317
#       tls: { insecure: true }
# Protobuf, gzip or plain. OTLP/JSON and gRPC are not implemented yet; JSON is
# answered with 415 naming what is supported, and gRPC is simply not listening.
# When the pipeline is saturated the receiver answers 503 + Retry-After so the
# exporter retries, rather than shedding data the sender was willing to keep.

# Or be a Splunk HEC endpoint, so existing forwarders only change hostname.
# Set hec.tokens and hec.ack_enabled in the config first.
./target/release/logless run --config /tmp/logless.toml --hec

#   curl -H 'Authorization: Splunk <token>' \
#        -d '{"event":"hello","sourcetype":"checkout"}' \
#        http://127.0.0.1:8088/services/collector
# With acks on, a batch sent with a channel gets an ackId, and that ackId
# reports true only once the records are fsynced to the WAL — so a client that
# discards its copy on ack is not discarding data we might still lose.

# Or be a Sentry proxy: your app keeps its SDK and its DSN key, and only the
# DSN's *host* changes. Everything is stored locally; a chosen subset goes on.
./target/release/logless run --config /tmp/logless.toml --sentry

#   sentry_sdk.init(dsn="http://<your existing key>@127.0.0.1:9000/<project>")
#
#   [sentry]
#   upstream_auth = "relay"    # keep each app's key and project, change the host
#   # upstream_auth = "resign" # or funnel everything through one configured DSN
#   upstream_dsn = "https://key@o1.ingest.sentry.io/1234"
#   transaction_sample_rate = 0.25   # transactions are the bill, not errors
#   forward_attachments = false      # held locally until asked for
#
# Envelopes are forwarded byte-for-byte, unknown item types included, and
# upstream rate limits are passed back to the SDK so its backoff still works.

# Replay the WAL; --repair truncates a torn tail left by a crash
./target/release/logless recover --config /tmp/logless.toml --repair

# Merge WAL segments into partitioned Parquet (also runs in the background)
./target/release/logless merge --config /tmp/logless.toml

# Disk usage, pressure, store contents
./target/release/logless status --config /tmp/logless.toml

# Inspect the catalog, or throw it away and rebuild from Parquet footers
./target/release/logless catalog --config /tmp/logless.toml --rebuild

# Expire partitions past their bucket's retention; --dry-run to preview
./target/release/logless retention --config /tmp/logless.toml --dry-run

# Show the mined template dictionary
./target/release/logless templates --config /tmp/logless.toml

# Measure template-mining throughput
./target/release/logless bench --lines 2000000
```

To forward, append your vendor to the generated config:

```toml
[[destinations]]
type = "sentry"
dsn = "https://<key>@<host>/<project>"

[[destinations]]
type = "splunk_hec"
url = "https://splunk.internal:8088"
token = "<hec-token>"
aggregate = true          # roll repeated errors into counted events
```

Measured on 16,000 lines containing 80 errors: Sentry received all 80 errors
with their debug breadcrumbs (**3.4% of raw bytes**); Splunk received 2 events
plus 2 aggregate events covering the other 78 (**0.17% of raw bytes**).

## Design commitments

- **Never block the application.** Not the log producer, and not a slow vendor
  either: forwarding runs on its own thread behind a bounded queue, so an
  unreachable Sentry or a rate-limiting Splunk cannot become backpressure. When the queue is full, log-less sheds data —
  debug first, then info, warn-and-above only after a bounded wait. It is a
  load-shedding system by design.
- **Account for everything.** `received == enqueued + dropped_*` is asserted in
  tests and checked at runtime. Nothing vanishes silently.
- **Retention is a directory delete.** Partitions are `hour=…/level=…`; expiry
  removes a directory. No per-row bookkeeping.
- **Retention granularity drives partition granularity.** If your policy retains
  `error` for 90d and `warn` for 14d, they become separate buckets.
- **Parquet is the API.** No embedded query engine — point your own DuckDB,
  Polars or Grafana at the store:

  ```sh
  duckdb -c "SELECT level, count(*) FROM read_parquet('/tmp/logless-data/store/**/*.parquet',
                                                      hive_partitioning=true) GROUP BY level;"
  ```

  Timestamps are UTC-qualified so time filters mean what they say. The binary is
  7.3 MB; embedding a query engine would roughly quintuple it.
- **Collapsing makes things cheaper, never invisible.** Repeated errors stop
  re-sending their context, but every error still goes upstream, and collapsed
  storms surface as aggregate events carrying a count.
- **Template ids are stable and exact.** The same log shape always gets the same
  id, across restarts. Novelty detection, rate baselines and Sentry `fingerprint`
  grouping all depend on that, which is why mining uses Drain rather than
  similarity clustering.
- **The catalog is derived state.** Delete it and the agent rebuilds it from
  Parquet footers. A corrupt catalog is a restart, not an outage.

## Licence

[Business Source License 1.1](LICENSE). Free to run in production, including
commercially. You may not offer log-less itself as a hosted observability
service. Converts to Apache-2.0 on 2030-08-04.
