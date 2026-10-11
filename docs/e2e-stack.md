# End-to-end stack run

`scripts/e2e-stack.sh` starts the whole stack on one machine, sends it known traffic and checks what came out. Parts go
to central's own disk by default (`PARTS_BACKEND=local`, no MinIO) or to a MinIO the script starts (`PARTS_BACKEND=s3`).
It runs in about 15 to 20 seconds, prints a PASS/FAIL table, and exits 0 only if every check passed. Every run starts with nothing: random
free ports on 127.0.0.1, a new directory from `mktemp -d`, and random peer token, HEC token, MinIO credentials (s3) and admin
tokens (`openssl rand`). A trap stops every process the script started and removes the directory, on success, on
failure and on Ctrl-C. It kills only the PIDs it started.

```zsh
cargo build -p logless-cli
scripts/e2e-stack.sh                     # LOGLESS_BIN defaults to $CARGO_TARGET_DIR/debug/logless
PARTS_BACKEND=s3 scripts/e2e-stack.sh    # parts in MinIO instead of on central's disk
KEEP=1 scripts/e2e-stack.sh              # keep the run directory (configs, agent logs, stores) for a look afterwards
```

| Variable | Default | |
|---|---|---|
| `LOGLESS_BIN` | `$CARGO_TARGET_DIR/debug/logless`, else `target/debug/logless` | the agent under test |
| `PARTS_BACKEND` | `local` | `local`: parts on central's disk. `s3`: parts in a MinIO started here |
| `BIG_PART_MB` | 128 | local only: one part of this size is uploaded to measure throughput and central's peak RSS; 0 skips it |
| `MINIO_BIN`, `MC_BIN` | `/opt/homebrew/bin/minio`, `mc` | needed for `s3` only |
| `DUCKDB_BIN` | `duckdb` | also needs `curl`, `python3`, `openssl`, `sqlite3`, `shasum` |
| `SENTRY_DEMO` | `~/projects/logless-studio/scripts/sentry-demo` | real `@sentry/node` app; skipped (SKIP row, not a failure) if `node_modules/@sentry/node` or `node` is missing |
| `N_OTLP`, `N_HEC`, `SENTRY_REQUESTS` | 3000, 3000, 40 | traffic sent |
| `SYNC_TIMEOUT` | 90 | seconds to wait for the edge to finish syncing |
| `KEEP` | 0 | 1 keeps the run directory |

## What runs

* **central**: `logless run` with `[peer]` (one `[[peer.nodes]]` entry, `edge-1`, whose token is the edge's destination
  token), `[peer.parts]` and the admin API. With `local`, `[peer.parts]` is `enabled = true` and the two retention
  settings, nothing else, and central also runs S3 serve (anonymous, loopback) so the parts can be read back through it.
  With `s3`, `[peer.parts]` points at MinIO under prefix `edges`.
* **MinIO** with a bucket `logless-parts`, with `s3` only.
* **edge**: `logless run` with the OTLP/HTTP, HEC (token) and Sentry proxy listeners, the admin API, 128 KiB WAL
  segments (so segments roll and merge into Parquet while it runs), and one `[[destinations]] type = "logless"` to
  central with `fidelity = 100`, `min_severity = 0` and `anomaly_boost = false`. The Sentry proxy has no upstream
  (`forward_events = false`, `transaction_sample_rate = 0.0`, `spool = false`): it is a local recorder here.

Both agents run with `--tail` on an empty file, which keeps them running until SIGTERM.

## What it sends

* `N_OTLP` records over OTLP/HTTP JSON (`POST /v1/logs`, batches of 500, service `e2e-otlp`, severities cycling
  DEBUG/INFO/WARN/ERROR, ~250-byte bodies).
* `N_HEC` events over `POST /services/collector/event` (batches of 500 concatenated JSON events, `sourcetype`
  `e2e-hec`).
* `SENTRY_REQUESTS` requests through the Sentry demo app (`node app.mjs` with `SENTRY_DSN` pointed at the edge's proxy).
  It raises transactions and some errors at random, so the number of Sentry items varies from run to run; the checks
  compare central with the edge, not with a fixed number.

## What it checks

| Check | Passes when |
|---|---|
| otlp / hec batches answered 200 | every batch was answered 200 |
| sentry demo ran | `node app.mjs` exited 0 (SKIP when deps are missing) |
| edge synced everything it wrote | edge `ingest.received == ingest.written == sync sent_records` and `backlog_bytes == 0`, twice in a row 0.5 s apart |
| parts job done | `POST /admin/v1/sync/parts` over the run's range ends `done` with `uploaded == files_total >= 1`, `rejected == 0`, no error |
| stored parts match their sha256 | every part under the node's directory (or `edges/<node>/` in the bucket, read back with `mc cat`) hashes to its own name, and their number is `uploaded` |
| each part is an edge file | every object's sha256 is the sha256 of a Parquet file in the edge's store |
| central catalog lists the parts | central's `peer_parts` table holds exactly the object names |
| foreign prefix and traversal refused | with the edge's node token: a presign naming another node is 403, a `../` name is 422, a foreign-node commit is 403, and every stored part is under the node's own directory or prefix |
| central's parts store status matches its disk (local) | `parts_store` in central's `GET /admin/v1/sync` says `backend=local`, `uploads == uploaded`, `used_bytes` == the bytes of the files on disk, nothing reserved and no refusals |
| upload without a valid token is refused; nothing written (local) | a PUT to the part's own path with no token, with a forged token, and to a `../` path are each 403, and the directory holds the same files as before |
| parts are readable (only) through central's S3 endpoint (local) | the bucket `parts` lists exactly the node's parts, a GET of one hashes to its name, and a PUT there is 405 |
| catalog listing by node and tier | central `GET /admin/v1/parts?node=&tier=warm` returns exactly the uploaded parts; another node has none |
| node retires its own part to cold | `POST /v1/parts/retire` is 200 once, 404 the second time, and `tier=cold` lists one part |
| retention sweep deletes expired catalogued parts and audits them | the stack runs with 2 s warm and cold retention; after 3 s a dry run reports every part and deletes none, a real sweep deletes all (bucket empty, `peer_parts` empty) and writes one `expire`/`deleted` audit row per part |
| a 128 MiB part is stored whole (local) | a presign for `BIG_PART_MB` of random bytes, then a PUT with the headers central named, is 200, and the stored file hashes to its name. Time and MiB/s are shown. The part is not committed |
| central streams the part (local) | central's RSS, sampled every 20 ms during that PUT, grew by less than a sixteenth of the part |
| peer ack counters sane | central `receiver.accepted == edge sent_records - duplicates == central ingest.written`; `records == accepted + duplicates + loops`; `1 <= wal_waits <= requests`; `wal_wait_us_max` under `ack_wait_ms`; zero loops, unauthorized, bad requests, unsaved dedupe ids, WAL timeouts and shutdown 503s; edge state `ok`, no `last_error`, and nothing dropped, rejected, sampled out or rolled up |
| exits on SIGTERM within 10 s | edge, then central, then MinIO (s3) each exit within 10 s of SIGTERM (time shown) |
| no ERROR lines | neither agent log has a line at level ERROR |
| logged a drained shutdown | each agent logged `shutdown signal received; draining` |
| verify (accounting invariant) | `logless verify` passes on both stores (after `logless merge` of what was left in the WAL) |
| central otlp / hec records == sent | rows and distinct event ids for that service on central equal what was sent |
| central sentry records == edge | central holds the same number of Sentry records as the edge stored (and the proxy's `items_received` is shown) |
| sentry proxy counted nothing forwarded | with no upstream, the proxy reports `items_forwarded=0` and `held_locally == items_received` |
| central total == edge total, ids unique | same row count on both, every event id distinct on both |
| rollup reconciliation (fidelity 100) | no rollup records on central, and rollup counts + records that are not supplements == what the edge wrote |

Counts are taken with `duckdb` over each store's Parquet files after both agents have stopped.

## What it does not cover

OTLP gRPC, S3 serve beyond the one parts read above, the disk budget (unit-tested, not exercised here), TLS, a real Sentry upstream, `fidelity` below 100 (rollups with real counts), anomaly boosts,
restarts and faults mid-sync (those are `sync_soak` and `scripts/chaos.sh`), and anything on a non-loopback address.
Ports are found by binding port 0 and closing it, so another process could take one between that and the agent
binding it; the run then fails at start and should simply be rerun.

Output of real runs: `docs/evidence/e2e-stack.md` (MinIO), `docs/evidence/parts-local-backend.md` (both backends).
