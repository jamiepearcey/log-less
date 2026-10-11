# Peer ingest acknowledges only after the WAL fsync

**What this is:** the evidence for the peer receiver's ack contract (a 200 means the central's WAL has `fdatasync`ed
the batch; `docs/peer-sync.md`, "What a 200 means"), run on 2026-10-11 from branch `hard/ack` on one machine (Apple M4
Max, rustc 1.91.0, debug build). **What it is not:** a power-loss test. `kill -9` ends the process but keeps the page
cache, so it shows the ack ordering (no 200 before the sync, cursor not moved, nothing lost), not what an unsynced
WAL tail does when the machine loses power.

## 1. Real `kill -9` with two agents

Two `logless run` processes: an edge tailing a file with a `type = "logless"` destination, and a central with
`[peer]` enabled. The central runs with `wal_fsync_interval = "10s"` (and `peer.ack_wait_ms = 12000`) so that a batch
sits appended to the WAL but unsynced for seconds, and is killed with `kill -9` while the edge's request waits for
that fsync. It is then restarted on the same data dir with the default 100 ms interval. Both are stopped with SIGTERM
at the end, and the central's store is counted with DuckDB over its Parquet files.

Script: `kill9.sh <path to logless>`, in the appendix (needs `curl`, `python3`, `duckdb`). Output, verbatim:

```text
== 02:39:31.846 central #1 up with wal_fsync_interval=10s (a batch sits appended but unsynced for up to 10 s)
== 02:39:33.020 wrote 200 lines to the edge's tailed file
== 02:39:36.058 central #1 WAL bytes on disk (appended, not yet fsynced): 32926
edge: state=ok sent_records=0 duplicates=0 busy_answers=0 backlog_bytes=0 last_error=None
== 02:39:36.115 kill -9 central #1 (pid 41220) while the edge's request waits for the fsync
central #1 exit status 137
edge: state=error sent_records=0 duplicates=0 busy_answers=0 backlog_bytes=85900 last_error='could not reach the peer: io: Peer disconnected'
dedupe ids saved by central #1: 0
== 02:39:37.204 central #2 up on the same data dir, wal_fsync_interval=100ms
== 02:39:39.453 edge drained
edge: state=ok sent_records=200 duplicates=0 busy_answers=0 backlog_bytes=0 last_error='could not reach the peer: io: Peer disconnected'
central #2 receiver: {"accepted": 200, "bad_request": 0, "busy": 0, "dedupe_unsaved": 0, "duplicates": 0, "loops": 0, "records": 200, "requests": 1, "settled_late": 0, "unauthorized": 0, "wal_closed": 0, "wal_timeouts": 0, "wal_wait_us_max": 10987, "wal_wait_us_total": 10987, "wal_waits": 1}
== 02:39:40.482 both stopped (SIGTERM); central's final maintenance pass merged its WAL
rows=400 parquet_files=2 wal_segments=0
matched 400 records (scanned 400 rows in 2 files; 0 files and 0 row groups pruned)
per file (each WAL segment is merged into its own file):
file|rows|ids
s000000000001-01a1289d7b94|200|200
s000000000002-01a1289d9088|200|200
whole store:
rows|distinct_event_ids|distinct_lines|missing_lines
400|200|200|0
```

Reading it:

* Before the kill the edge had `sent_records=0`: the central had the batch in its WAL (32,926 bytes) but had not
  synced it, so it had not answered. The edge's request ended with "Peer disconnected", the cursor stayed (85,900
  bytes pending), and the central had saved no ids.
* After the restart the edge resent the batch, the central took all 200 as new (its window was empty), waited 11 ms
  for its first fsync, and answered 200.
* Nothing is missing. The store holds 400 rows for 200 event ids: segment 1 is the killed process's WAL write
  (`kill -9` kept it in the page cache, and the restart merged it) and segment 2 is the resend. That is the documented
  duplicate for a crash after the WAL write and before the ids were saved; the store does not dedupe by event id.
  Under the old code the 200 went out as soon as the batch was on the in-memory channel, so the same kill in the
  window before the WAL write lost the batch (and the edge had already committed). That window is milliseconds wide
  and was not reproduced here; it was found by reading the code.

## 2. Added latency with the default fsync interval

The central alone (`wal_fsync_interval = "100ms"`, the default; `--no-maintenance`), a Python client posting 500
sequential batches of 50 records to `/v1/ingest` after 30 warm-up batches, client-side round trip. The baseline is
`main` at 60c9d14, built from the same tree before the change. Three runs each, interleaved:

| Run | Build | p50 ms | p90 ms | p99 ms | max ms | mean ms |
|---|---|---|---|---|---|---|
| 1 | main 60c9d14 | 6.76 | 13.10 | 26.39 | 40.07 | 8.21 |
| 1 | hard/ack | 119.99 | 135.18 | 163.68 | 258.50 | 122.14 |
| 2 | main 60c9d14 | 7.73 | 11.71 | 16.53 | 22.70 | 7.90 |
| 2 | hard/ack | 120.82 | 141.20 | 172.15 | 192.48 | 123.32 |
| 3 | main 60c9d14 | 5.63 | 9.62 | 27.48 | 49.83 | 7.03 |
| 3 | hard/ack | 112.37 | 129.69 | 165.64 | 273.71 | 116.27 |

The receiver's own counters agree (run 2, `receiver` in `GET /admin/v1/sync`, warm-up included): 530 waits, mean
111.6 ms (`wal_wait_us_total` 59,170,993 / 530), max 187.0 ms, no timeouts, no 503s.

The added wait is about 110 ms at p50 and 140 to 150 ms at p99: a sequential sender's batch arrives just after the
previous batch's fsync, so it waits nearly a full interval for the next one, plus up to 20 ms because the writer checks
its interval between 20 ms queue polls. One sender therefore delivers about one batch (frames are up to 1 MiB) per
fsync interval; several senders share a sync (section 3: four senders, mean wait 29 ms). Syncing as soon as a peer
batch is waiting would cut this to the cost of one `fdatasync`, at the price of more syncs; not done here.

Raw client output, run 1:

```text
{"requests": 500, "records_per_request": 50, "statuses": {"200": 500}, "p50_ms": 6.76, "p90_ms": 13.1, "p99_ms": 26.39, "max_ms": 40.07, "mean_ms": 8.21}
{"requests": 500, "records_per_request": 50, "statuses": {"200": 500}, "p50_ms": 119.99, "p90_ms": 135.18, "p99_ms": 163.68, "max_ms": 258.5, "mean_ms": 122.14}
receiver {"accepted": 26500, "bad_request": 0, "busy": 0, "dedupe_unsaved": 0, "duplicates": 0, "loops": 0, "records": 26500, "requests": 530, "settled_late": 0, "unauthorized": 0, "wal_closed": 0, "wal_timeouts": 0, "wal_wait_us_max": 251405, "wal_wait_us_total": 58293880, "wal_waits": 530}
```

## 3. Tests

```sh
CARGO_TARGET_DIR=$HOME/tmp/logless-hard-ack-target \
  cargo test -p logless-core --lib -- peer:: sync:: wal:: admin_api::tests::sync
# test result: ok. 64 passed; 0 failed; 1 ignored (soak_for_minutes)
```

The new ones, `--nocapture --test-threads 1` (the soak test's `===` title line was cut by the filter I piped the
output through; nothing else was changed):

```text
running 6 tests
test peer::tests::a_pipeline_that_dies_before_the_wal_syncs_never_answers_200_and_the_resend_is_taken ... ok
test peer::tests::abandoned_tickets_are_capped_and_settle_only_once_synced ... ok
test peer::tests::the_200_comes_only_after_the_wal_synced_and_then_the_ids_are_saved ... ok
test peer::tests::the_wait_is_bounded_and_a_late_sync_makes_the_resend_a_duplicate ... ok
test sync::soak::a_full_queue_ahead_of_a_slow_wal_is_a_503_and_every_record_lands_exactly_once ... 
offered 4000 records in 160 batches
receiver: requests 188 = 200 after a WAL sync 160 + queue-full 503 28 + wal-timeout 503 0
receiver: records parsed 4700 = accepted 4000 + duplicates 0 + in refused batches 700
receiver: wal waits 160, mean 28.0 ms, max 63.0 ms
senders: sent 4000, duplicates 0, busy answers (503/429) 28, shed 0
stored rows 4000, distinct ids 4000, offered ids 4000, missing 0
ok
test sync::soak::a_receiver_whose_wal_never_syncs_never_moves_the_senders_cursor ... --- receiver whose WAL never synced, then died ---
receiver: requests 2, wal waits 2, wal timeouts (503) 2, accepted 0; rows appended unsynced (lost) 50
sender before the crash: sent 0, busy answers 2, backlog 30264 bytes
after the restart: receiver accepted 50, duplicates 0, stored rows 50 (unique 50), offered 50
sender: sent 50, duplicates 0, busy answers 2, backlog 0
ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 649 filtered out; finished in 9.85s
```

The queue-full test again on its own, unfiltered (a second run, so its numbers differ from the one above):

```text
running 1 test

=== full queue ahead of a slow WAL: 4 edges -> 1 central (4.4 s) ===
offered 4000 records in 160 batches
receiver: requests 179 = 200 after a WAL sync 160 + queue-full 503 19 + wal-timeout 503 0
receiver: records parsed 4475 = accepted 4000 + duplicates 0 + in refused batches 475
receiver: wal waits 160, mean 28.9 ms, max 60.5 ms
senders: sent 4000, duplicates 0, busy answers (503/429) 19, shed 0
stored rows 4000, distinct ids 4000, offered ids 4000, missing 0
test sync::soak::a_full_queue_ahead_of_a_slow_wal_is_a_503_and_every_record_lands_exactly_once ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 654 filtered out; finished in 5.76s
```

| Test | Shows |
|---|---|
| `peer::tests::the_200_comes_only_after_the_wal_synced_and_then_the_ids_are_saved` | bound but unsynced (and synced one record short) is no reply and no saved id; the sync brings the 200 and both ids |
| `peer::tests::a_pipeline_that_dies_before_the_wal_syncs_never_answers_200_and_the_resend_is_taken` | the pipeline torn down mid-wait answers 503, saves nothing, and a restarted receiver takes the resend as new |
| `peer::tests::the_wait_is_bounded_and_a_late_sync_makes_the_resend_a_duplicate` | 503 `wal_slow` with `Retry-After` within the bound (300 ms); after the late sync the resend is 2 duplicates and the pipeline saw the batch once |
| `peer::tests::abandoned_tickets_are_capped_and_settle_only_once_synced` | abandoned tickets are capped at 1024 and settle once, only after their sync |
| `sync::soak::a_receiver_whose_wal_never_syncs_never_moves_the_senders_cursor` | a real sender against a receiver whose disk never syncs: two 503s, cursor unmoved, no ids saved; after the restart all 50 land once |
| `sync::soak::a_full_queue_ahead_of_a_slow_wal_is_a_503_and_every_record_lands_exactly_once` | four real senders, a one-batch queue and a slow disk: 503 `busy` (each counted by a sender as `busy_answers`), and 4000 of 4000 records stored exactly once |
| `admin_api::tests::sync_reports_the_peer_receivers_wal_waits_and_503s_when_it_has_one` | the receiver's counters appear as `receiver` in `GET /admin/v1/sync` |

## Appendix: the kill -9 script

```bash
#!/bin/bash
# Two real agents: edge tails a file and syncs to central; central is killed -9 while a batch waits for its WAL fsync.
set -u
BIN=$1; S=$(cd "$(dirname "$0")" && pwd)
rm -rf $S/central $S/edge $S/input.log; mkdir -p $S/central $S/edge; : > $S/input.log
CP=19400; CA=19401; EA=19402; TOK=kill-test-admin-token
central_cfg() { cat > $S/central.toml <<C
schema_version = 1
[storage]
data_dir = "$S/central"
wal_fsync_interval = "$1"
[peer]
enabled = true
addr = "127.0.0.1:$CP"
ack_wait_ms = 12000
[admin]
enabled = true
addr = "127.0.0.1:$CA"
token = "$TOK"
C
}
cat > $S/edge.toml <<C
schema_version = 1
[storage]
data_dir = "$S/edge"
[admin]
enabled = true
addr = "127.0.0.1:$EA"
token = "$TOK"
[[destinations]]
type = "logless"
url = "http://127.0.0.1:$CP"
C
adm() { curl -s -H "Authorization: Bearer $TOK" http://127.0.0.1:$1/admin/v1/sync; }
edge_status() { adm $EA | python3 -I -c 'import json,sys; s=json.load(sys.stdin)["status"]; print("edge: state=%s sent_records=%s duplicates=%s busy_answers=%s backlog_bytes=%s last_error=%r" % (s["state"], s["sent_records"], s["duplicates"], s["busy_answers"], s["backlog_bytes"], s["last_error"]))'; }
up() { for i in $(seq 1 100); do curl -sf -o /dev/null http://127.0.0.1:$CP/.well-known/logless && return; sleep 0.1; done; echo "central did not come up"; }
ts() { python3 -I -c "import datetime; print(datetime.datetime.now().strftime(\"%H:%M:%S.%f\")[:-3])"; }

echo "== $(ts) central #1 up with wal_fsync_interval=10s (a batch sits appended but unsynced for up to 10 s)"
central_cfg 10s
"$BIN" run --config $S/central.toml > $S/central1.out 2> $S/central1.err &
C1=$!; up
"$BIN" run --config $S/edge.toml --tail $S/input.log > $S/edge.out 2> $S/edge.err &
E=$!
sleep 1
for i in $(seq 1 200); do echo "killtest line $i payment service timed out after 3000 ms" >> $S/input.log; done
echo "== $(ts) wrote 200 lines to the edge's tailed file"
sleep 3
echo "== $(ts) central #1 WAL bytes on disk (appended, not yet fsynced): $(find $S/central -path '*wal*' -type f -exec cat {} + | wc -c | tr -d ' ')"
edge_status
echo "== $(ts) kill -9 central #1 (pid $C1) while the edge's request waits for the fsync"
kill -9 $C1; wait $C1 2>/dev/null; echo "central #1 exit status $?"
sleep 1
edge_status
echo "dedupe ids saved by central #1: $(( $(stat -f %z $S/central/peer-dedupe.ids 2>/dev/null || echo 0) / 16 ))"

echo "== $(ts) central #2 up on the same data dir, wal_fsync_interval=100ms"
central_cfg 100ms
"$BIN" run --config $S/central.toml > $S/central2.out 2> $S/central2.err &
C2=$!; up
for i in $(seq 1 120); do
  out=$(adm $EA | python3 -I -c 'import json,sys; s=json.load(sys.stdin)["status"]; print(s["sent_records"]+s["duplicates"], s["backlog_bytes"])')
  set -- $out; [ "$1" -ge 200 ] && [ "$2" -eq 0 ] && break; sleep 1
done
echo "== $(ts) edge drained"
edge_status
adm $CA | python3 -I -c 'import json,sys; r=json.load(sys.stdin)["receiver"]; print("central #2 receiver:", json.dumps(r, sort_keys=True))'
kill -TERM $E; wait $E 2>/dev/null
kill -TERM $C2; wait $C2 2>/dev/null
echo "== $(ts) both stopped (SIGTERM); central's final maintenance pass merged its WAL"
"$BIN" status --config $S/central.toml 2>&1 | tail -1
"$BIN" replay --config $S/central.toml --since 2h --contains killtest --limit 100000 2>/dev/null | head -1
echo "per file (each WAL segment is merged into its own file):"
duckdb -list -c "SELECT regexp_extract(filename, 's[0-9]+-[0-9a-f]+', 0) AS file, count(*) AS rows, count(DISTINCT event_id) AS ids FROM read_parquet('$S/central/store/**/*.parquet', filename=true) GROUP BY 1 ORDER BY 1"
echo "whole store:"
duckdb -list -c "SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_event_ids, count(DISTINCT body) AS distinct_lines, 200 - count(DISTINCT body) AS missing_lines FROM read_parquet('$S/central/store/**/*.parquet') WHERE body LIKE 'killtest line %'"
```

## Appendix: the latency client

```python
# Posts N sequential peer batches to a running central and prints p50/p99 round trip.
import json, sys, time, uuid, urllib.request, statistics
url, n, per = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
times, statuses = [], {}
for i in range(n):
    now = time.time_ns()
    lines = []
    for j in range(per):
        lines.append(json.dumps({"event_id": str(uuid.uuid4()), "timestamp_unix_nano": now, "observed_unix_nano": now,
            "severity": 9, "severity_text": None, "body": f"bench request {i} record {j} " + "x" * 120, "service": "bench",
            "trace_id": None, "span_id": None, "attributes": [], "template_id": None}))
    body = "\n".join(lines).encode()
    req = urllib.request.Request(url + "/v1/ingest", data=body, headers={"Content-Type": "application/x-ndjson"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            r.read(); st = r.status
    except urllib.error.HTTPError as e:
        st = e.code
    times.append((time.perf_counter() - t0) * 1000)
    statuses[st] = statuses.get(st, 0) + 1
times.sort()
q = lambda p: times[min(len(times) - 1, int(p * len(times)))]
print(json.dumps({"requests": n, "records_per_request": per, "statuses": statuses,
    "p50_ms": round(q(0.50), 2), "p90_ms": round(q(0.90), 2), "p99_ms": round(q(0.99), 2),
    "max_ms": round(times[-1], 2), "mean_ms": round(statistics.mean(times), 2)}))
```
