# Edge pinning, retire and the upload timer: what was run

Branch `wf/parts-ack` (on `wf/parts-local`), Plane LOG-169. Debug build, a Mac that other sessions were also
using, so wall times are context only. Binary copied from the build to a private path before each run. Numbers
below are copied from the output of the runs; nothing is estimated.

## Rust tests (targeted, no full suite)

`cargo test -p logless-core --lib <filter>`, one filter at a time:

| Filter | Passed | Failed |
|---|---|---|
| `parts_edge` | 1 | 0 |
| `parts::` | 37 | 0 |
| `retention::` | 7 | 0 |
| `budget::` | 10 | 0 |
| `config::` | 7 | 0 |
| `sync::` | 18 (1 ignored, `sync_soak`, as before) | 0 |
| `admin_api::` | 57 | 0 |
| `policy_impact::` | 6 | 0 |
| `cargo test -p logless-cli` | 5 | 0 |

143 passed in `logless-core`, 0 failed, plus 5 in `logless-cli`. Ten of them are new:

* `parts::tests::central_down_pins_to_the_limit_reclaim_past_it_is_counted_and_central_back_gets_the_rest`: ten files
  uploaded to no one (central down); the timer's run fails; `backlog_parts` 10; disk pressure deletes the oldest
  unpinned files and keeps the two newest pinned; `gave_up` parts, rows and bytes equal the sums over the files that
  disappeared (checked against the file sizes and row counts taken beforehand), equal the status, and equal what
  `parts-gaveup.jsonl` adds up to; central comes back, the next timer run uploads and commits every file left
  (catalog rows = files left = objects on central's disk, all warm); retention then deletes them all and the retire
  pass turns every part cold, with the state file empty and the cold parts still on central's disk.
* `parts::tests::a_file_removed_after_upload_is_retired_so_central_marks_it_cold_and_the_rest_stay_warm`
  (one cold, two warm, one audit row).
* `parts::tests::a_retire_central_never_heard_is_still_pending_after_a_restart_and_is_sent_then` (the retire fails while
  central is unreachable and backs off; a new `SyncControl` on the same data dir finds it and sends it).
* `parts::tests::compaction_of_uploaded_files_retires_the_old_parts_and_uploads_the_merged_one` (4 uploaded files
  compacted to 1 with 80 rows; the merged file is pinned; afterwards central has 1 warm part with 80 rows and the 4
  old parts cold).
* `parts::tests::the_timer_never_overlaps_a_running_upload_and_runs_again_only_when_due`, `...nothing_is_pinned_while_the_timer_is_off_or_no_peer_is_set`,
  `...an_old_unconfirmed_file_is_not_pinned_and_the_limit_never_passes_a_quarter_of_the_budget`.
* `retention::tests::a_pinned_file_outlives_its_partitions_expiry_and_the_rest_of_the_partition_goes`,
  `budget::tests::reclaim_never_picks_a_pinned_file_even_when_it_is_the_cheapest_and_oldest`,
  `parts_edge::tests::a_missing_store_directory_retires_nothing_and_a_removed_file_is_pending`.

## scripts/e2e-stack.sh, default local mode

Seven checks added (the second edge `edge-3`, the central freeze and restart, retire on removal; see
`docs/e2e-stack.md`). Two runs in a row, `PARTS_BACKEND=local`:

| Run | Passed | Failed | Skipped | Exit | Wall |
|---|---|---|---|---|---|
| 1 | 40 | 0 | 0 | 0 | 52 s |
| 2 | 40 | 0 | 0 | 0 | 51 s |

The new rows of run 1 (run 2 had the same counts):

```
central stopped in the middle of the edge's uploads  PASS  central had committed 2 of the edge's 15 files when it was frozen; exited 1001 ms after SIGTERM; restarted
edge pinned its backlog while central was down       PASS  backlog_parts=13 pinned_parts=13 gave_up_parts=0
no part lost, none duplicated after central restarted PASS  edge files=15; central catalog rows=15 distinct=15; objects stored=15 (temporary: 0); edge state lines=15; names equal the edge files' sha256: yes
after the restart only the parts not yet stored arrived, once each PASS  uploads since the restart=12 (already_present=0); files=15, stored when central exited=3 (at the freeze: 2, committed: 2)
files removed by disk pressure are retired: central marks each cold PASS  policy PUT=200; edge files left=0; central cold=15 warm=0 of 15; edge retired=15 pending=0 state lines left=0; unconfirmed files given up=0; central still holds 15 objects
```

What the freeze did: central had committed 2 parts when it was stopped; a third had been stored (the PUT finished
while central shut down) but not committed, so after the restart 12 parts' bytes arrived (15 - 3), the third was found
present and only committed, and the catalog ended with 15 rows for 15 edge files. Run 2: 15 files, 2 committed at
the freeze, 12 uploads after the restart.

One run with `PARTS_BACKEND=s3` (MinIO): 35 passed, 0 failed, 0 skipped, exit 0, 50 s, the same four lifecycle
checks passing (the "bytes arrived once" row is local-only).

A first attempt of the "bytes arrived once" row compared against the parts stored at the freeze and failed by one
(uploads 12, expected 13): a request in flight at the freeze finished while central shut down. The row now compares
against the parts stored when central exited. It is a test written after its first failure, not before.

## What was not run or not verified

* `scripts/restore-drill.sh` was not run (not touched; the config `logless init` writes now has a `[sync_parts]`
  table).
* The over-the-limit loss (reclaim past the pin limit, counted) is covered by the unit test above, not by the e2e
  script: the script's budget is set after everything is uploaded, so nothing is unconfirmed then.
* No release build; no soak; no test with a pin limit below the size of a single file in the e2e.
* The policy preview does not account for pins (documented in `docs/peer-sync.md`).
* The compaction case was run against a real central but with `compact::compact` called directly, not through a
  maintenance pass; the maintenance pass order (merge, compact, pins, expire and reclaim) is code review only.
* Compaction near an hour boundary: the e2e files all sit in the current hour, which compaction skips. A run that
  crossed the hour could compact edge-3's files mid-test and change its file count.
