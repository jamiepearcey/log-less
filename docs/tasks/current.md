# Current work queue — 6-week prototype

Scope locked 2026-08-04: Sentry + Splunk destinations, VM/bare-metal host agent, BSL licence.
Design: `docs/architecture.md`. Strategy: `.context/project-brief.md`.

## Week 0 — skeleton ✅
- [x] Cargo workspace: `logless-core` (model, config, wal, partition, queue, retention) + `logless-cli`.
- [x] Internal record type = OTel log data model (severity number, trace_id/span_id, ns ts, typed attrs). UUIDv7 event id at ingest.
- [x] Versioned TOML config schema (`schema_version = 1`, `deny_unknown_fields`).
- [x] LICENCE (BSL 1.1, Change Date 2030-08-04 → Apache-2.0).
- [x] **Measured stripped release binary: 1.5 MB skeleton, 7.3 MB with arrow+parquet+rusqlite.** Recorded in `docs/architecture.md`. Settles the embedded-engine question: DuckDB would still be ~5× the current binary.
- [x] **Dep-licence check** (`scripts/licences.sh`, run in CI): all 222 dependencies are permissively licensed. Parses SPDX expressions including Cargo's legacy `MIT/Apache-2.0` slash form, which a naive check rejects for no reason.

## Weeks 1–2 — the invariant: zero-loss, zero-block
- [x] WAL segments: `u32 len | u32 crc32 | payload`, magic header, roll at size; group `fdatasync` on interval-or-bytes.
- [x] Crash recovery: forward scan, truncate at first damaged frame, idempotent. Verified against a real `kill -9` — 241,992 records survived, 8-byte torn tail truncated.
- [x] Bounded queue + severity load-shedding (DEBUG → INFO → WARN+ only after a bounded wait), drop counters per class.
- [x] Accounting invariant `received == enqueued + dropped_*`, asserted in tests and enforced at runtime (exit code 2 on violation).
- [x] Config-driven level buckets — retention granularity dictates partition granularity, so `error` can outlive `warn`.
- [x] Partition layout `hour=YYYY-MM-DDTHH/level=<bucket>`, hour-first, hive-compatible.
- [x] Retention as directory delete, per bucket, with dry-run + orphan-bucket protection.
- [x] Disk budget watermarks: 80% reclaim cheapest-first, 95% stop admitting DEBUG. Verified with a 3 MB budget against 25 MB of logs — all warn+ rows survived.
- [x] Merge to Parquet, sort `(service, trace_id, ts)`, zstd + dictionary, row-group stats.
- [x] SQLite catalog: files (path, partition keys, min/max ts, rows) + merged segments. Atomic commit in one txn.
- [x] **Catalog rebuild from Parquet footers + directory scan.** Verified: catalog deleted, rebuilt to identical 21 files / 120,000 rows.
- [x] Crash-safe merge ordering (write→rename→commit→delete) with deterministic per-segment file names for idempotent re-merge.
- [x] UTC-qualified Parquet timestamps so external engines read time correctly.
- [x] **Batch linger on the WAL writer** (5 ms, bounded by and far below the fsync interval that already defines the crash window).
- [x] **Compaction** of small per-segment files, in the maintenance pass and as `logless compact`. Measured: 16 files → 1, **72% smaller**. Write-then-record-then-unlink, so a crash leaves duplicates (recoverable) rather than a gap (not).
- [x] **Durable spool with delivery cursors** (`spool.rs`): append-only segments, CRC per record, cursor written by atomic rename in its own file — not the catalog, which changes far less often and would drag SQLite into the delivery path.
- [x] **Chaos harness** (`scripts/chaos.sh`, in CI): repeated SIGKILL mid-write, then recover + merge + verify each round. Measured over 6 rounds: 561,535 records committed, monotonic across rounds, **zero duplicate event ids**. Disk-full and io-throttle injection are still open.
- [x] **Whole-path accounting**: lifetime counters persisted by atomic rename (`counters.rs`) plus `logless verify` — `received == committed + still_in_wal + dropped`. Catches a deleted committed file; the two merge bugs would both have failed it.

### Bugs found by end-to-end testing (fixed)
- **Data loss**: maintenance merged and deleted the segment the writer was actively appending to (`active_segment` sentinel `0` read as "nothing active"). 20,480 of 200,000 records lost to an unlinked file. Fixed by skipping `id >= active` and refusing to merge before the writer publishes.
- **Deferred merge**: two `Relaxed` shutdown flags let the maintainer observe "stop" without "writer finished", so the last segment never merged in-process. Fixed with a single Release/Acquire phase.
- **Retention**: empty-directory tidy would remove any empty directory under the store, not just partitions.
- **Reclaim**: success path `return`ed before catalog cleanup, leaving rows pointing at deleted files.

## Weeks 2–3 — templating
- [x] Masking heuristics (timestamp, UUID, hex, IP, number, quoted, `key=value`, path), token-classified rather than regex-scanned.
- [x] Rust Drain port; fixed-depth parse tree; stable `template_id`; wildcard merging; bounded fan-out and template cap.
- [x] Template dictionary persisted in the catalog and restored on startup, so ids survive restarts.
- [x] Templating wired into the merge path; `template_id` lands in Parquet.
- [x] **Benched: 775,617 lines/s/core release (target ≥500k, met); 15 templates from 2M lines; 0% untemplated.** Debug build is ~39k/s — never bench unoptimised.
- [x] Fallback path intact: nothing depends on templating: a `None` match writes the record untemplated.
- [x] **Benchmarked against the 11 real LogHub corpora** with their published ground-truth template counts (`scripts/loghub.sh`). 568k lines/s/core on real logs. **This changed a default**: see below.
- [x] **Store template id + parameter columns instead of body text — measured, and rejected.** See "The compression step that was not there" below. The assumption was 4–8×; it is 4.7%.
- [x] **Bracketed component prefixes are masked** (`<COMP>`): 5 shapes across 3 components now yield 5 templates, not 15. A bracketed *level* is excluded — merging `[ERROR]` with `[INFO]` would put an error's fingerprint on an info line.

### Bugs found by end-to-end testing (fixed, weeks 2–3)
- **Data loss on restart**: WAL segment ids restart at 1 once the WAL drains, so the catalog reported a fresh segment 1 as already merged and deleted it unread. Every restart of a drained agent silently lost its first segment. Fixed by giving each segment a UUID in its header and keying both the catalog and the Parquet filename on that.
- **Drain depth semantics**: `depth` counted token levels directly, but Drain3 counts the root and count nodes too, so `depth=4` must mean 2 token levels. At 4, lines differing in any of their first four tokens could never merge.

## Weeks 4–5 — pushdown
- [x] Sharded in-memory ring with enforced byte budget and per-key line cap.
- [x] Correlation chain: trace_id → (service, request_id) → (host, pid, thread) → (host, service); the tier that fired is reported upstream.
- [x] Token bucket per (service, error_template_id); flow-hash window dedupe over template sequences.
- [x] Templating moved to ingest so pushdown has a stable id while the error is in flight; merge keeps it as a fallback.
- [x] `Capture` API: suppressing context never suppresses the error, and a reopened window reports how many it collapsed.
- [x] JSONL pushdown output (`--pushdown-out`) — the forwarders' input until they exist.
- [x] Demo: 100k lines / 221 errors → every error forwarded, 6 context windows, **132× less data upstream**.
- [x] Bounds tested: 100k lines through a 1 MB ring stays under budget; one chatty key cannot evict a quiet neighbour.
- [x] **Storm harness** (`scripts/storm.sh`, in CI): 10k errors/s sustained, **peak RSS 75 MiB** against a 512 MiB budget, sampled from `ps` rather than from our own accounting — which is exactly what would be wrong if the ring leaked.
- [x] **Dedupe window measured against a contiguous real corpus** (`dedupe-stats`, Thunderbird, 871s): repeat gaps p50=1s, p90=14s, p99=22s. A 30s window collapses 99.3% of repeats and 60s collapses 99.5%, so the 60s default stands — now measured rather than guessed.
- [x] **Global context-window ceiling added** (`max_windows_per_minute`, default 30). The measurement found the real gap: the busiest minute produced **80 distinct new templates**, and the existing budget is per-shape, so 80 shapes could each spend their own — a burst arriving at the vendor exactly when everything else is going wrong.

### Bugs found by end-to-end testing (fixed, weeks 4–5)
- **Templates over-merged**: masked positions counted as *matches* in the similarity score, so a shared `service=… trace_id=…` prefix alone could clear the threshold. Four distinct debug messages collapsed into one template id, which would have broken novelty detection and flow hashing. Variable positions no longer count toward similarity.
- **Suppressed context suppressed the error**: 194 of 200 errors produced no output at all. Dedupe must withhold the context block, never the error event.

## Week 6 — end to end
- [x] **Sentry out**: envelope API, `fingerprint = ["logless:<template_id>"]`, context as breadcrumbs, tags for service/key_tier/flow_hash.
- [x] **Splunk HEC out**: one billable event carrying its context; aggregate events (`sourcetype=logless:aggregate`) with count, first/last seen, template shape and a concrete example.
- [x] Per-destination shaping wired: Sentry gets every error (it groups natively), Splunk gets rollups (it bills per GB).
- [x] Transport behind a trait; retry on 5xx/429/network, never on other 4xx; payload shaping tested without a network.
- [x] Destinations configurable via `[[destinations]]`; generated config stays appendable.
- [x] Bytes-avoided accounting per destination.
- [x] **Verified against stand-in vendor endpoints**: 16k lines / 80 errors → Sentry 80 events (3.44% of raw), Splunk 2 events + 2 aggregates (0.17% of raw).
- [x] **File tail receiver**: rotation (drains the rotated file first), truncation, partial lines, persisted checkpoints, multi-file. Verified live: 4,500 lines across a mid-run rotation, zero loss.
- [x] **Graceful shutdown on SIGTERM/SIGINT** — the deployment target is a systemd unit, and systemd stops with SIGTERM.
- [x] **Forwarding moved to its own thread** behind a bounded queue; a slow or dead vendor can no longer apply backpressure to ingest. Overflow drops and counts, as the ingest queue does.
- [x] **Periodic aggregate flush** (30s), not only at shutdown.
- [x] **OTLP/HTTP receiver** (`POST /v1/logs`, protobuf, gzip) — the highest-value surface. Hand-written protobuf reader instead of `prost`/`tonic`: 4 new crates, ~0.2 MB, versus tokio + HTTP/2 + a code generator. Off by default; `otlp.enabled` or `--otlp`.
- [x] **Verified against the official OpenTelemetry Python SDK** (an independent encoder, not our own): 20,000 records gzipped in 40 requests → 20,000 received, 20,000 written, zero dropped; `service.name`, resource attributes, typed attribute values, `trace_id`/`span_id` all present in the committed Parquet. Uncompressed path re-verified at 5,000/5,000.
- [x] **OTLP gRPC** (`:4317`): hand-written HTTP/2 (frames, HPACK via `fluke-hpack`, connection and stream flow control, multiplexed streams, PING keepalive, GOAWAY) plus gRPC message framing, trailers and status codes. No tonic, no tokio.
- [x] **Verified against the official OpenTelemetry Python gRPC exporter** (grpcio's C-core client): 20,000 records in 40 RPCs uncompressed, then 8,000 in 16 RPCs with gRPC-level gzip — all received, all written, zero errors. `trace_id` present on all 199 errors in the committed Parquet.
- [x] **OTLP/JSON** implemented, chosen by content type, answered in JSON. Handles the two things a naive reader gets wrong: 64-bit fields as decimal strings (a JSON number is a double and loses ~104 days of nanosecond resolution), and hex ids rather than base64.
- [x] **Splunk HEC receiver**: `/services/collector`, `/event`, `/raw`, `/ack`, `/health`; token auth (header or query), gzip, concatenated-JSON and line-oriented bodies, `sourcetype` → service, typed `fields`, `503 Server is busy` instead of shedding.
- [x] **Indexer acknowledgement keyed to a real `fdatasync`**, not to receipt. Verified end to end against a third-party HEC client: `false` immediately after the post, `true` 0.11s later, once the WAL had synced.
- [x] **HTTP plumbing shared** between OTLP and HEC (`httpd.rs`): body limits, gzip bomb cap, worker pool, deterministic shutdown — so the two receivers cannot drift on the parts that were expensive to get right.
- [x] **Sentry ingest proxy**: `/api/<project>/envelope/` and the legacy `/store/`, gzip, byte-exact forwarding, `relay` vs `resign` upstream identity, per-project routing, deterministic transaction sampling, attachments held locally, unknown item types passed through, upstream rate limits honoured and reflected back to SDKs.
- [x] **Verified against the real `sentry-sdk`** (2.66.1) with a stand-in upstream: 20 errors + 1 message + 12 transactions → 23 items forwarded, 10 transactions sampled out at `transaction_sample_rate = 0.25`, all 33 stored locally with full payloads. Relay sent `/api/1234/envelope/` with `sentry_key=appkey123`; resign sent `/api/77/envelope/` with `sentry_key=ourteamkey`. Stacktraces, `release`, `environment`, `server_name` and trace context all intact upstream.
- [x] **Sentry upstream queue is spooled to disk**, fsynced before the SDK is answered `200`. Tested across a restart with an unreachable upstream: envelopes are delivered later, and a delivered envelope is not re-sent.
- [x] **Sentry replay upstream** (`logless replay --to-sentry`). Verified: 6 transactions held back live at `transaction_sample_rate = 0`, then replayed to `/api/4242/envelope/` with the client's own key under relay mode, carrying the SDK's original payload bytes.
- [x] **Delivery cursor persisted** for the log forwarder too: undelivered events are replayed on start.
- [x] **Tail glob patterns** with periodic rediscovery (`*`, `?`, `[a-z]`, `[!…]`). Re-expanded every 5s, because the window in which a new service's log file appears is exactly the window an incident lives in. `**` is deliberately unsupported and matches nothing rather than half-working.
- [x] **Replay scanner** (`scan.rs`) and `logless replay`: prunes by partition directory, then row-group statistics, then rows. `--explain` shows which files a query would open. Sends matched history to the configured destinations with `--forward` — the undo button.
- [x] Verified a user's own `duckdb` CLI queries the Parquet dir with hive partitioning — group-by level, time filters, partition pruning all work.
- [x] **Demo packaged** as `scripts/demo.sh`: ingest, storage ratio, mined templates, what would have been forwarded, replay, and a DuckDB query over the same files.

### Bugs found by end-to-end testing (fixed, gRPC)
- **A socket accepted from a non-blocking listener inherits non-blocking on BSD/macOS but not on Linux.** Every read returned `EWOULDBLOCK`, so the connection died with `INTERNAL_ERROR` after the first request — and would have behaved differently on the deployment platform than on the development one. Now set explicitly on each accepted socket.
- **A read timeout mid-frame abandoned the frame.** The idle timeout that lets a connection notice shutdown also fires part-way through a frame; treating it as fatal left the next read starting mid-frame with every later frame misparsed. Timeouts are now only actionable between frames.
- **The test client sent DATA frames larger than the negotiated `max_frame_size`** — my harness, not the server, but it masked the real behaviour of the oversized-message path until fixed.

### Bugs found by end-to-end testing (fixed, HEC)
- **A spurious `503` while the agent was idle.** The record loop slept a fixed 200 ms when it had nothing to do — correct for a file, wrong for a socket. An exporter fills the bounded handoff channel during that sleep and is told the pipeline is full. A third-party HEC client (which does not retry) died on exactly one such 503 after 311 of 2,000 events. Replaced with `crossbeam::Select` over the receiver channels; re-measured at 2,001/2,001 with zero 503s.
- **`ack_enabled` locked out every client that does not use acks.** Splunk enables acknowledgement per *token*, so it can demand a channel; our switch is server-wide, so demanding one rejected every ordinary client with `400 Data channel is missing`. No channel now simply means no ackId.
- **An empty token list rejected everything instead of accepting everything.** The "no tokens configured means accept anything" path was never consulted when the request presented no token at all.
- **The ack floor could jump an undurable id.** Retiring durable entries out of order advanced the "everything below N is durable" floor past a batch still in flight, so it would have reported `true` for data not on disk — the exact failure the mechanism exists to prevent. Retirement is now strictly from the bottom, and a stalled channel is refused rather than buffered.
- **Ack bookkeeping was O(outstanding) per request**: every batch swept the watermark map for unreferenced entries. Visible as a test suite that took 20s instead of 0.5s; in production it would have been a per-request walk of a map sized by the client's backlog.
- **An explicit `INFO` was indistinguishable from no level at all** — `severity_text` was only recorded when the level differed from the default, so "the client said INFO" and "the client said nothing" both stored `NULL`.

### Bugs found by end-to-end testing (fixed, OTLP)
- **Multi-worker shutdown hung forever.** `tiny_http::Server::unblock()` pushes a single token and calls `notify_one`, so it wakes exactly one worker blocked in `recv()`; the rest blocked on the condvar and `join` never returned. Found by the test suite hanging for seven minutes, not by reading the docs. Replaced with a stop flag plus `recv_timeout`, with `unblock` demoted to a nudge.
- **A failed accept retired a worker.** The original loop was `while let Ok(request) = server.recv()`, but `recv` also returns `Err` for a per-connection error — one client dropping a connection mid-handshake would shrink the pool by one, and the receiver would silently stop serving after a few flaky connections.
- **Receiver counters were read before the join**, so a request being served as shutdown began was missing from the totals. `shutdown()` now returns the snapshot it takes after joining.

### Bugs found by end-to-end testing (fixed, week 6 receivers)
- **Rotation lost data**: jumping straight to the new inode abandoned whatever was written to the old file between the last poll and the rename. Measured: **300 of 4,500 lines lost across one rotation**. The rotated file is now drained through its still-open handle before switching; re-measured at 4,500 of 4,500.
- **A trailing partial line was dropped on rotation** unless the rotated file happened to have new bytes — the fragment could already be sitting in the cursor from an earlier poll.
- **No graceful shutdown**: `run --tail` had no termination path except a record cap, so SIGTERM killed the agent mid-flight, losing the unsynced WAL tail, un-flushed aggregate counters and an up-to-date checkpoint — on every deploy.

### Bugs found by end-to-end testing (fixed, week 6 forwarders)
- **Generated config was not appendable**: `logless init` emitted `destinations = []` in the root table, which collides with an appended `[[destinations]]` section — so the documented "init then add your vendor" flow failed to parse.
- **Aggregate events showed an example instead of a shape**: the rollup's `template` field received the raw body. An aggregate exists to show the shape, so it now carries the masked template text alongside one concrete example.
- **Long ids classified as `<NUM>`**: a 32-character all-digit trace id matched the numeric check before the hex one. Tokens of 16+ characters are identifiers, not quantities.

### The compression step that was not there

`compression-experiment` writes 22,000 real LogHub lines both ways, with identical writer settings:

| | bytes | vs raw |
|---|---|---|
| raw text | 3,178,575 | — |
| Parquet, body stored | 706,940 | 4.50× |
| Parquet, template id + params, no body | 673,794 | 4.72× |
| template dictionary (shared, written once) | 64,444 | — |

**4.7% smaller with the dictionary amortised across files; 4.4% *larger* if it is not.** The design assumed this step was worth 4–8×.

The reason is that the work is already being done: zstd plus dictionary encoding over the sorted `body` column removes exactly the redundancy templating would remove. The template text is the repetitive part and compresses to nearly nothing; the parameters are the high-entropy part and are incompressible either way. Templating was never the missing compression step — it earns its place through *structure* (stable ids for fingerprints, dedupe and novelty), which is what §3 of the architecture already claimed and what the earlier "2.4×, unchanged" measurement was actually telling us.

So this is closed as **rejected on measurement**: a few percent is not worth making `body` derivable, breaking the Parquet contract, and forcing every external reader to join a dictionary to see their own log text. Compaction (72%) and the sort order were where the real wins were.

## Still open

- **Chaos harness covers SIGKILL only.** Disk-full and io-throttle injection are not automated.
- **Two LogHub datasets still over-split** (Proxifier 3.25×, Apache 2.00×): both are dominated by lines whose only variable part is a path or a duration, which the masker splits more finely than the ground truth does.

## Deferred (do not pull in)
Novelty/rate anomaly layer (EWMA + SpaceSaving + HLL) · k8s DaemonSet · S3 tiering · DuckLake aggregation tier · Flight SQL · syslog · ES `_bulk` · Datadog/OTLP out.

### Measured against real corpora, and what it changed

Benchmarking against LogHub did what benchmarking is for: it contradicted a default.

| similarity_threshold | geometric-mean ratio to ground truth | datasets within 2× |
|---|---|---|
| 0.4 (the old default) | 0.59× | 4 / 11 |
| 0.6 | 0.79× | 8 / 11 |
| 0.7 | 0.89× | 10 / 11 |
| **0.8 (new default)** | **0.99×** | **10 / 11** |
| 0.9 | 1.22× | 10 / 11 |

At 0.4 we were **over-merging badly on every real dataset** — collapsing distinct shapes into one template, which for this product means one Sentry fingerprint covering unrelated failures and a dedupe window suppressing errors that are not duplicates. Synthetic lines never showed it, because we wrote them to be cleanly separable. The default is now 0.8, where over-splitting (which only costs dictionary entries) is the residual error rather than over-merging.

Two datasets still over-split (Proxifier 3.25×, Apache 2.00×); both are dominated by lines whose only variable part is a path or a duration, which our masker splits more finely than LogHub's ground truth does.

## Known gaps worth stating plainly
- **HEC index routing is ignored.** We have level buckets, not indexes; `index` is kept as an attribute. An event addressed to an index that does not exist is accepted rather than refused with code 7.
- **No real Splunk to test against.** Ingest was driven by a third-party HEC client, but the ack contract was polled by hand — no OSS client implements `useACK`. The shapes match Splunk's documented ones; they have not been checked against Splunk itself.
- **An acked batch could still lose records to severity shedding** if another source fills the ingest queue between the capacity check and the submit. The window is small and the check makes it unlikely, but "unlikely" is not "cannot".
- **Sentry rate limits are tracked per proxy, not per project.** A limit returned for one upstream project suppresses that category for all of them.
- **No `sentry-trace` / `baggage` continuity checks.** Envelopes are forwarded as-is, so distributed tracing works, but nothing verifies that a sampled-out transaction does not leave a dangling reference upstream.
- **gRPC is plaintext h2c only.** No TLS: termination belongs to whatever already does it on the node, and the default bind is loopback. A client configured for TLS will fail to connect rather than fall back.
- **gRPC streaming and `grpc-timeout` are ignored.** OTLP logs export is unary, so nothing sends a stream; a deadline is accepted and not enforced.
- **The 503 backpressure path is unit-tested, not load-tested.** No run yet has actually filled the pending queue with a real exporter and confirmed the retry lands the data.
- **Resource attributes are copied onto every record.** Correct for a flat schema and cheap after dictionary encoding, but unmeasured against a wide resource (50+ attributes).
- **`dropped_upstream` counts only what OTLP can express** (`dropped_attributes_count`). An SDK that drops records because its own export queue overflowed says nothing on the wire — observed during testing, where the Python SDK's default 2048-record queue silently discarded 17,184 of 20,000 records before the first byte was sent.
