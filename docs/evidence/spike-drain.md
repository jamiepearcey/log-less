# Spike fixture: wait for drain before stopping (Plane LOG-132)

## Problem

`scripts/spike-console.sh` posted its events through HEC and then sent the agent
SIGTERM the moment the last POST returned. A HEC 200 means "queued", not "in the
WAL" (`ack_enabled = false`), so the agent was stopped with a backlog, and the
fixture reported the result as loss.

## Cause (reproduced)

Two parts, only the first is fixed here.

1. **Fixture** (fixed): no drain wait between the last 200 and `kill`.
2. **Agent** (not fixed, follow-up): on SIGTERM the `run` loop sets `done`
   without first draining the HEC (and Sentry/peer) receiver channels, so
   batches already answered 200 and still in the channel are discarded. The
   agent's own accounting cannot see it: `received` counts only records that
   reached the ingest loop, so `logless verify` printed `OK` while the HEC
   server's own line said `hec: records=1000000` and only 480000 were stored.
   Once, in the loaded control, the stop also produced a genuine
   `ACCOUNTING VIOLATION: 375 records unaccounted for` (received=160000,
   enqueued=74859, written=74484); that was a single observation and was not
   re-run to isolate it.

## Fix

- `scripts/spike-load.py` posts the events, then polls `GET /admin/v1/info`
  until `received >= acked` and `received == written + dropped` (queue depth 0)
  on two consecutive polls, with a timeout (`SPIKE_DRAIN_TIMEOUT`, default
  120 s). Only then does the script stop the agent (SIGTERM, 30 s to exit).
- `spike-console.sh` enables the admin API, uses ports 18088/18101
  (`SPIKE_HEC_PORT`, `SPIKE_ADMIN_PORT`), refuses to continue if the agent did
  not come up, and ends with exact counts in `$WORK/result.json`: sent, acked,
  stored (catalog rows after merge), in WAL, dropped by policy, and
  `lost = acked - stored - in_wal - dropped`.
- `SPIKE_SYNTHETIC=1` generates events locally (no LogHub download),
  `SPIKE_SKIP_STUDIO=1` skips the console tests, `SPIKE_NO_DRAIN=1` restores the
  old stop-on-last-200 behaviour as a negative control.
- Regression check: `scripts/spike-drain-check.sh [events] [busy-loops]`, also
  in CI (100k events). It fails unless drained, queue depth 0, sent == acked
  and lost == 0.

## Measured (debug build of this branch, 14-core Apple machine, 1,000,000 synthetic HEC events, batches of 5000, 0 retries)

| Run | Mode | sent | acked | stored | dropped by policy | lost | drain wait |
| --- | --- | --- | --- | --- | --- | --- | --- |
| idle | control, no drain | 1,000,000 | 1,000,000 | 480,000 | 0 | 520,000 | none |
| idle | drain wait | 1,000,000 | 1,000,000 | 1,000,000 | 0 | 0 | 15.77 s |
| 14 busy loops | control, no drain | 1,000,000 | 1,000,000 | 320,000 | 0 | 680,000 | none |
| 14 busy loops | drain wait | 1,000,000 | 1,000,000 | 1,000,000 | 0 | 0 | 25.38 s |

Other runs of the same shapes the same day: idle drain wait took 22.21 s and
46.70 s, loaded drain wait 30.38 s, 43.27 s and 45.07 s; every one stored
1,000,000 of 1,000,000. The control varied (stored 174,621 to 480,000 idle,
247,792 to 320,000 loaded, with up to 144,909 shed by policy in one run) and is
not asserted. The "1,000,000 posted became 1,000,000 rows" criterion holds for
the drain-wait runs. The older 2026-08-07 finding was not re-derived from LogHub
data: these runs use synthetic events, and the LogHub variant (network fetch)
was not run.

Not reproduced: once, in a loaded run, the load generator exited 1 after
50,000 events (10 requests) and the cause was not captured. Six repeat runs
under the same load did not recur. The generator now reports transport errors
with counts, so the next occurrence is diagnosable.

## What this unblocks (LOG-47, LOG-48, LOG-15)

The fixture can now be trusted for loss numbers under load, which those items
need. They are not done:

- **LOG-47** (chaos with ENOSPC and slow fsync): `scripts/chaos.sh` still has no
  fault injection and runs `verify` only after SIGKILL rounds; needs ENOSPC and
  slow-fsync injection (`scripts/diskfull.sh` covers ENOSPC separately), 20
  rounds, a duplicate-event-id check (needs `duckdb` on PATH), and the
  stale-counter blind spot above closed.
- **LOG-48** (72 h soak): needs a long-running variant of `storm.sh` with RSS
  and fd sampling over 72 h, a segment-leak check, and an end reconcile that
  uses drain-then-stop; nothing here runs longer than minutes.
- **LOG-15** (gate G2): needs both of the above with `verify` clean, and the
  agent-side SIGTERM drain fix, otherwise a clean stop under load loses acked
  HEC data and `verify` cannot see it.
