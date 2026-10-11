# End-to-end stack run: evidence

Run on 2026-10-11 (BST) on branch `hard/stab`, base `e85dad1` plus this branch's fixes, with `scripts/e2e-stack.sh`
(see `docs/e2e-stack.md`). The agent was built with `cargo build -p logless-cli` and the binary copied to a scratch
directory so that another build in the shared target directory could not swap it mid-run. Tools:
`minio RELEASE.2025-10-15T17-29-55Z`, `mc`, `duckdb`, Node with `@sentry/node` 8.x from the console repo's
`scripts/sentry-demo`. `$SCRATCH` stands for the session scratch directory the script was pointed at with `TMPDIR`.
Each block below is the script's complete output.

## Three runs in a row

```text
$ for i in 1 2 3; do TMPDIR=$SCRATCH LOGLESS_BIN=$SCRATCH/logless scripts/e2e-stack.sh; echo "run $i exit=$?"; done
```

Run 1 (exit 0):

```text
[03:26:45] run dir $SCRATCH/logless-e2e.vgjdhq, logless logless 0.1.0
[03:26:46] minio on 65437, bucket logless-parts
[03:26:46] central: peer 65447, admin 65448
[03:26:46] edge: otlp 65451, hec 65452, sentry 65453, admin 65454
[03:26:46] sentry: 40 requests through @sentry/node

CHECK                                        RESULT DETAIL
-----                                        ------ ------
otlp batches answered 200                    PASS  3000 records over OTLP/HTTP JSON
hec batches answered 200                     PASS  3000 events over /services/collector/event
sentry demo ran                              PASS  done: 40 requests sent through the real @sentry/node SDK
edge synced everything it wrote              PASS  received=6042 written=6042 sent_records=6042 backlog_bytes=0
parts job done                               PASS  state=done files_total=17 uploaded=17 rejected=0 error=None (edge files=17)
minio parts match their sha256               PASS  objects=17 uploaded=17 mismatched=0
each part is an edge file                    PASS  objects with no edge file of that sha256: 0
central catalog lists the parts              PASS  peer_parts rows=17
peer ack counters sane                       PASS  edge sent=6042 dup=0 busy=0; receiver requests=78 accepted=6042 dup=0 busy=0 wal_waits=44 wal_wait_max_ms=155
edge exits on SIGTERM within 10 s            PASS  664 ms
central exits on SIGTERM within 10 s         PASS  831 ms
minio exits on SIGTERM within 10 s           PASS  114 ms
no ERROR lines in edge log                   PASS  0 ERROR lines
no ERROR lines in central log                PASS  0 ERROR lines
edge logged a drained shutdown               PASS  5 'stopped' lines
central logged a drained shutdown            PASS  2 'stopped' lines
edge verify (accounting invariant)           PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central verify (accounting invariant)        PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central otlp records == sent                 PASS  rows=3000 unique_ids=3000 sent=3000
central hec records == sent                  PASS  rows=3000 unique_ids=3000 sent=3000
central sentry records == edge               PASS  central=42 edge=42 (proxy items_received=42)
sentry proxy counted nothing forwarded       PASS  items_forwarded=0 held_locally=42 items_received=42
central total == edge total, ids unique      PASS  central rows=6042 ids=6042; edge rows=6042 ids=6042
rollup reconciliation (fidelity 100)         PASS  rollup records=0 rollup counts=0 + plain records=6042 = 6042; edge wrote 6042

elapsed 11 s; 24 passed, 0 failed, 0 skipped
E2E PASS
```

Run 2 (exit 0):

```text
[03:26:57] run dir $SCRATCH/logless-e2e.NjUB5Q, logless logless 0.1.0
[03:26:57] minio on 49267, bucket logless-parts
[03:26:58] central: peer 49276, admin 49277
[03:26:58] edge: otlp 49280, hec 49281, sentry 49282, admin 49283
[03:26:58] sentry: 40 requests through @sentry/node

CHECK                                        RESULT DETAIL
-----                                        ------ ------
otlp batches answered 200                    PASS  3000 records over OTLP/HTTP JSON
hec batches answered 200                     PASS  3000 events over /services/collector/event
sentry demo ran                              PASS  done: 40 requests sent through the real @sentry/node SDK
edge synced everything it wrote              PASS  received=6048 written=6048 sent_records=6048 backlog_bytes=0
parts job done                               PASS  state=done files_total=16 uploaded=16 rejected=0 error=None (edge files=16)
minio parts match their sha256               PASS  objects=16 uploaded=16 mismatched=0
each part is an edge file                    PASS  objects with no edge file of that sha256: 0
central catalog lists the parts              PASS  peer_parts rows=16
peer ack counters sane                       PASS  edge sent=6048 dup=0 busy=0; receiver requests=73 accepted=6048 dup=0 busy=0 wal_waits=41 wal_wait_max_ms=188
edge exits on SIGTERM within 10 s            PASS  960 ms
central exits on SIGTERM within 10 s         PASS  819 ms
minio exits on SIGTERM within 10 s           PASS  107 ms
no ERROR lines in edge log                   PASS  0 ERROR lines
no ERROR lines in central log                PASS  0 ERROR lines
edge logged a drained shutdown               PASS  5 'stopped' lines
central logged a drained shutdown            PASS  2 'stopped' lines
edge verify (accounting invariant)           PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central verify (accounting invariant)        PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central otlp records == sent                 PASS  rows=3000 unique_ids=3000 sent=3000
central hec records == sent                  PASS  rows=3000 unique_ids=3000 sent=3000
central sentry records == edge               PASS  central=48 edge=48 (proxy items_received=48)
sentry proxy counted nothing forwarded       PASS  items_forwarded=0 held_locally=48 items_received=48
central total == edge total, ids unique      PASS  central rows=6048 ids=6048; edge rows=6048 ids=6048
rollup reconciliation (fidelity 100)         PASS  rollup records=0 rollup counts=0 + plain records=6048 = 6048; edge wrote 6048

elapsed 11 s; 24 passed, 0 failed, 0 skipped
E2E PASS
```

Run 3 (exit 0):

```text
[03:27:08] run dir $SCRATCH/logless-e2e.emGVmR, logless logless 0.1.0
[03:27:09] minio on 49471, bucket logless-parts
[03:27:09] central: peer 49481, admin 49482
[03:27:09] edge: otlp 49485, hec 49486, sentry 49487, admin 49488
[03:27:10] sentry: 40 requests through @sentry/node

CHECK                                        RESULT DETAIL
-----                                        ------ ------
otlp batches answered 200                    PASS  3000 records over OTLP/HTTP JSON
hec batches answered 200                     PASS  3000 events over /services/collector/event
sentry demo ran                              PASS  done: 40 requests sent through the real @sentry/node SDK
edge synced everything it wrote              PASS  received=6044 written=6044 sent_records=6044 backlog_bytes=0
parts job done                               PASS  state=done files_total=12 uploaded=12 rejected=0 error=None (edge files=12)
minio parts match their sha256               PASS  objects=12 uploaded=12 mismatched=0
each part is an edge file                    PASS  objects with no edge file of that sha256: 0
central catalog lists the parts              PASS  peer_parts rows=12
peer ack counters sane                       PASS  edge sent=6044 dup=0 busy=0; receiver requests=69 accepted=6044 dup=0 busy=0 wal_waits=45 wal_wait_max_ms=165
edge exits on SIGTERM within 10 s            PASS  733 ms
central exits on SIGTERM within 10 s         PASS  758 ms
minio exits on SIGTERM within 10 s           PASS  104 ms
no ERROR lines in edge log                   PASS  0 ERROR lines
no ERROR lines in central log                PASS  0 ERROR lines
edge logged a drained shutdown               PASS  5 'stopped' lines
central logged a drained shutdown            PASS  2 'stopped' lines
edge verify (accounting invariant)           PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central verify (accounting invariant)        PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central otlp records == sent                 PASS  rows=3000 unique_ids=3000 sent=3000
central hec records == sent                  PASS  rows=3000 unique_ids=3000 sent=3000
central sentry records == edge               PASS  central=44 edge=44 (proxy items_received=44)
sentry proxy counted nothing forwarded       PASS  items_forwarded=0 held_locally=44 items_received=44
central total == edge total, ids unique      PASS  central rows=6044 ids=6044; edge rows=6044 ids=6044
rollup reconciliation (fidelity 100)         PASS  rollup records=0 rollup counts=0 + plain records=6044 = 6044; edge wrote 6044

elapsed 11 s; 24 passed, 0 failed, 0 skipped
E2E PASS
```

After the three runs, no `logless-e2e.*` directory was left in `$SCRATCH` and `pgrep -fl "minio server"` found nothing.

`requests` in the receiver line is more than `wal_waits` because it also counts the presign and the commit for each part:
78 = 44 + 17 x 2, 73 = 41 + 16 x 2, 69 = 45 + 12 x 2, and 36 = 8 + 14 x 2 in the run below.

## Without the Sentry demo

The skip path, with `SENTRY_DEMO` pointed at a directory that does not exist:

```text
$ TMPDIR=$SCRATCH LOGLESS_BIN=$SCRATCH/logless SENTRY_DEMO=/nonexistent scripts/e2e-stack.sh
[03:26:27] run dir $SCRATCH/logless-e2e.yBHzjl, logless logless 0.1.0
[03:26:28] minio on 65295, bucket logless-parts
[03:26:28] central: peer 65304, admin 65305
[03:26:28] edge: otlp 65308, hec 65309, sentry 65310, admin 65311

CHECK                                        RESULT DETAIL
-----                                        ------ ------
otlp batches answered 200                    PASS  3000 records over OTLP/HTTP JSON
hec batches answered 200                     PASS  3000 events over /services/collector/event
sentry demo ran                              SKIP  skipped: /nonexistent/node_modules/@sentry/node missing (npm install there) or no node
edge synced everything it wrote              PASS  received=6000 written=6000 sent_records=6000 backlog_bytes=0
parts job done                               PASS  state=done files_total=14 uploaded=14 rejected=0 error=None (edge files=14)
minio parts match their sha256               PASS  objects=14 uploaded=14 mismatched=0
each part is an edge file                    PASS  objects with no edge file of that sha256: 0
central catalog lists the parts              PASS  peer_parts rows=14
peer ack counters sane                       PASS  edge sent=6000 dup=0 busy=0; receiver requests=36 accepted=6000 dup=0 busy=0 wal_waits=8 wal_wait_max_ms=134
edge exits on SIGTERM within 10 s            PASS  917 ms
central exits on SIGTERM within 10 s         PASS  805 ms
minio exits on SIGTERM within 10 s           PASS  110 ms
no ERROR lines in edge log                   PASS  0 ERROR lines
no ERROR lines in central log                PASS  0 ERROR lines
edge logged a drained shutdown               PASS  5 'stopped' lines
central logged a drained shutdown            PASS  2 'stopped' lines
edge verify (accounting invariant)           PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central verify (accounting invariant)        PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central otlp records == sent                 PASS  rows=3000 unique_ids=3000 sent=3000
central hec records == sent                  PASS  rows=3000 unique_ids=3000 sent=3000
central total == edge total, ids unique      PASS  central rows=6000 ids=6000; edge rows=6000 ids=6000
rollup reconciliation (fidelity 100)         PASS  rollup records=0 rollup counts=0 + plain records=6000 = 6000; edge wrote 6000

elapsed 15 s; 21 passed, 0 failed, 1 skipped
E2E PASS
```

## Defects the script found

Both are fixed on `hard/stab`, each with a regression test that fails without the fix.

### 1. A WAL sync done by a segment roll was never published (product, fixed)

The first run without the Sentry demo failed (same script and binary, before the fix):

```text
edge synced everything it wrote              FAIL  received=6000 written=151 sent_records=6000 backlog_bytes=0
```

`ingest.written` is the WAL writer's published synced count, the same number HEC indexer acks and peer `200`s wait on.
It stayed at 151 for the whole 90 s wait. `WalWriter::append` rolls to a new segment when the current one is full,
and `roll` syncs. That sync left `unsynced_bytes` at zero, so the next `maybe_sync` returned `false` and the writer loop
published nothing. Every batch here was larger than the 128 KiB segment, so every sync after the first came from a
roll. With the Sentry demo running, its small envelopes arrived later and caused a timed sync, which hid the problem.

Effect in production, whenever the last write before a quiet spell rolls a segment: a HEC client with `useACK` is told
`false` for records already on disk until more traffic arrives; a peer batch that rolls central's segment waits out
`ack_wait_ms` and gets `503 wal_slow`, and its resend is counted as duplicates; `/admin/v1/info` reports a stale `written`.

Fix: `WalWriter` records how many records the last sync covered (`synced_records()`), and `maybe_sync` returns true
when that number has moved since the previous call, whichever sync moved it. The writer loop publishes
`synced_records()`. Test: `wal::tests::a_sync_done_by_a_segment_roll_is_reported`. With the old `maybe_sync` it fails
at `the roll's sync must be reported`.

### 2. The Sentry proxy counted items as forwarded with no upstream route (product, fixed)

The first runs reported `sentry proxy stopped ... items_received=43 items_forwarded=1 held_locally=42`, next to
`sentry upstream: envelopes_sent=0 items_sent=0`. The proxy counted the items its policy kept as forwarded before it
looked up a route. With no route they stay local, so the counter was wrong. `docs/evidence/listeners-compat.md` shows the
same thing: `items_forwarded=67` with `envelopes_sent=0`. The fix resolves the route first, and counts kept items as
forwarded only when there is one. Test: `sentry::tests::a_local_only_proxy_stores_without_forwarding` now also asserts
`(items_forwarded, items_held_locally) == (0, 1)`. Before the fix it failed with `left: (1, 0)`. The e2e check
`sentry proxy counted nothing forwarded` covers it end to end.
