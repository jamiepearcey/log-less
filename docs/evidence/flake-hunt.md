# Flake hunt: logless-core lib suite

Run on 2026-10-11 (BST) on branch `hard/stab` from `e85dad1`, with `CARGO_TARGET_DIR=~/tmp/logless-admin-target`.
Other sessions were compiling and testing on the same machine at the time (a `cargo test` of another workspace ran
through the first runs), so "normal" means normal for a busy laptop.

## The runs

Each line is `cargo test -p logless-core --lib` (`single` adds `-- --test-threads=1`; `load` ran while four
`yes > /dev/null` processes ran, killed afterwards), with the run's `test result` line:

```text
normal1 rc=0 secs=29 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 27.95s
normal2 rc=0 secs=53 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 52.06s
normal3 rc=0 secs=41 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 39.66s
normal4 rc=0 secs=24 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 23.10s
normal5 rc=0 secs=44 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 43.01s
normal6 rc=0 secs=26 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 25.40s
normal7 rc=0 secs=33 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 32.64s
normal8 rc=0 secs=32 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 31.25s
single1 rc=0 secs=90 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 90.20s
single2 rc=0 secs=91 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 84.16s
single3 rc=0 secs=94 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 93.77s
load1 rc=101 secs=27 test result: FAILED. 671 passed; 1 failed; 1 ignored; 0 measured; 0 filtered out; finished in 19.35s
load2 rc=0 secs=21 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 20.17s
load3 rc=0 secs=20 test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 19.71s
```

From `single2` on, the build included the `admin_api` test change below (cargo rebuilt the test binary for `single2`).

One failure in 14 runs:

```text
---- sync::soak::a_receiver_whose_wal_never_syncs_never_moves_the_senders_cursor stdout ----

thread 'sync::soak::a_receiver_whose_wal_never_syncs_never_moves_the_senders_cursor' (75384185) panicked at crates/logless-core/src/sync_soak.rs:852:5:
the sender counted the 503s: SyncStatus { state: "error", peer: Some("http://127.0.0.1:62665"), min_severity: 0, fidelity: 100, anomaly_boost: false, has_token: false, last_ok_ms: None, last_error: Some("The peer is busy (HTTP 503). the write-ahead log has not synced the batch yet; retry"), last_error_ms: Some(1791684645133), sent_records: 0, sent_bytes: 0, duplicates: 0, loops: 0, rejected_batches: 0, busy_answers: 1, dropped_records: 0, backlog_bytes: 30264, queued_batches: 0, capabilities: None, probed_ms: None, sampled_out: 0, rollups_sent: 0, lookback_sent: 0, boosted_services: [], anomalies: [], backfill: None, parts: None }
```

## The flaky test and its cause

`sync::soak::a_receiver_whose_wal_never_syncs_never_moves_the_senders_cursor` waited until the **receiver** had counted
two WAL timeouts and then read the **sender's** status and asserted `busy_answers >= 2`. The receiver counts a timeout
before it writes the 503, so the sender may not have read and counted the second 503 yet. On a loaded machine it had
not (`busy_answers: 1`). The cause is in the test's ordering, not in a product race: the sender counts every 503 it
reads, as later runs show.

Fix (test only): the wait now ends when both sides have counted (`wal_timeouts >= 2 && busy_answers >= 2`, still
bounded at 30 s). The assertions after it are unchanged, so the check that the sender counted the 503s is still
there; the test no longer reads the sender's counter before it can be up to date.

The same test run alone 50 times while four `yes` processes ran, before and after the fix:

```text
soak-before filter=sync::soak::a_receiver_whose_wal_never_syncs_never_moves_the_senders_cursor load=load: 47 passed, 3 failed of 50
soak filter=sync::soak::a_receiver_whose_wal_never_syncs_never_moves_the_senders_cursor load=load: 50 passed, 0 failed of 50
```

All three failures before the fix were the same assertion with `busy_answers: 1`.

## Tightened, not loosened

`admin_api::tests::the_listener_starts_and_stops` polled for up to 3 s for the port to close, with a comment saying
`tiny_http` closed the socket from its own accept thread. The admin API runs on `bounded_http.rs` now, whose `shutdown`
joins the accept thread that owns the listener, so the port is closed when `shutdown` returns. The test now asserts
that at once, with no polling. 50 runs alone under the same `yes` load: `50 passed, 0 failed of 50`.

## After all fixes on this branch

The whole suite, once under the `yes` load and once without (this was before the WAL regression test was added: 672 tests):

```text
final-load: test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 20.59s
final:      test result: ok. 672 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 18.98s
```

And after the WAL fix and its test (`docs/evidence/e2e-stack.md`, defect 1):

```text
test result: ok. 673 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 20.47s
```

`cargo test -p logless-cli`: 5 passed in `src/main.rs`, and the six helper binaries have no tests (run twice: once
before the WAL change, once after it).

## Still timing-bound, not seen failing

Several tests assert that an answer came within 2 to 5 s (`bounded_http`, `hec`, `peer`, `sentry`, `otlp::http`,
`s3_serve`), which separates "answered from the head" from "answered after a 30 s timeout". `peer.rs` asserts that an
`ack_wait_ms` timeout lands within 1 s of its bound, and `sync_soak`'s `drained()` sleeps 250 ms to let one more
sender pass refresh `backlog_bytes`. None of them failed in these 14 suite runs, three of them under load. They are
the first candidates if a flake shows up on slower CI hardware.
