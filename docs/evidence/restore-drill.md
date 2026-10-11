# Restore drill: evidence

Run on 2026-10-11 (BST) on branch `wf/restore`, base `6c5088a` plus this branch, with `scripts/restore-drill.sh`
(`docs/restore.md`). Binary: `cargo build -p logless-cli`, copied to a scratch directory before the runs. Tools:
`minio RELEASE.2025-10-15T17-29-55Z`, `mc`, `duckdb`, `sqlite3`. `$SCRATCH` is the session scratch directory the script
was pointed at with `TMPDIR`. Everything is on loopback on one machine: MinIO, central and the edge are separate
processes, but no restore here crosses a real network.

## What the drill does

1. MinIO, central (peer receiver, `[peer.parts]` to MinIO, admin) and an edge (OTLP, 128 KiB WAL segments, sync to
   central) start on random free ports with random credentials.
2. `N_RECORDS` OTLP records go to the edge: six message shapes over two services, severities cycling DEBUG to ERROR.
   The edge syncs to central, is stopped cleanly, `logless merge` flushes its WAL to Parquet, `logless verify` passes,
   it restarts and the parts job uploads every file to MinIO.
3. The BEFORE snapshot is taken from the edge's own store with DuckDB and SQLite: rows, rows per `template_id`, rows per
   `(template_id, service)`, the `templates` table, and the keys of `_meta/templates.pq`.
4. `N_TAIL` = 500 more records are sent and acknowledged (HTTP 200), then the edge is killed with SIGKILL before any
   upload. Central's `peer_parts` is copied (`.backup`) as the inventory. The edge's data directory is deleted.
5. Case 1: `logless restore --from s3://… --inventory … --verify` into an empty directory, timed. The same DuckDB and
   SQLite queries run on the restored store and are compared with BEFORE.
6. Case 2: one byte in the middle of one stored part (the one with most rows) is flipped in MinIO. Restore again.
7. Case 3: the original is put back; a different part is deleted from MinIO. Restore again.
8. Case 4: restoring over a directory that already holds a store is refused.

## Run A: 200,000 records (the largest run)

`N_RECORDS=200000 SYNC_TIMEOUT=300 scripts/restore-drill.sh` (exit 0). Complete output of the three restores and the
check table:

```text
[09:37:13] run dir $SCRATCH/logless-restore.hM3rJ9, logless logless 0.1.0
[09:37:13] minio on 63006, bucket logless-parts
[09:37:14] central: peer 63122 admin 63123; edge: otlp 63633 admin 63649
[09:39:07] edge killed with SIGKILL after 500 acknowledged records that were never uploaded; its WAL replays 0 records

case 1 output:
  listed=3 ignored_keys=0 inventory=3
  restored_parts=3 rows=200000 bytes=3166098
  hash_mismatch=0 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=0 not_in_inventory=0
  catalog_files=3 catalog_rows=200000 catalog_bytes=3166098
  templates=6 template_services=12 dictionary_rows=12 multi_partition_parts=0
  reverify_bad=0
  elapsed_ms=970
  RESTORE OK: 3 parts, 200000 rows verified
case 2 output:
  listed=3 ignored_keys=0 inventory=3
  restored_parts=2 rows=100000 bytes=1547384
  hash_mismatch=1 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=0 not_in_inventory=0
  catalog_files=2 catalog_rows=100000 catalog_bytes=1547384
  ...
    60c8045c85fca655….parquet: sha256 of the bytes is <other digest>, not the name
  RESTORE FAILED: 1 damaged or missing parts (listed above); the damaged ones were not installed
case 3 output:
  listed=2 ignored_keys=0 inventory=3
  restored_parts=2 rows=150000 bytes=2291948
  hash_mismatch=0 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=1 not_in_inventory=0
  catalog_files=2 catalog_rows=150000 catalog_bytes=2291948
  ...
    ae807f40e3c30ca0….parquet: in the inventory, not in the bucket
  RESTORE FAILED: 1 damaged or missing parts (listed above); the damaged ones were not installed

CHECK                                                      RESULT DETAIL
ingest: all batches answered 200                           PASS  200000 records over OTLP/HTTP JSON
edge synced what it accepted                               PASS  received=200000 written=200000 sent_records=200000 backlog_bytes=0
edge stops cleanly before the flush                        PASS  3208 ms
edge verify after merge (accounting invariant)             PASS  received=200000 committed=200000 in_wal=0 dropped=0 unaccounted=0
parts upload done                                          PASS  state=done files_total=3 uploaded=3 rejected=0
before: every accepted record is in a part                 PASS  edge parquet rows=200000 accepted=200000 files=3 bytes=3166098
before: templates were mined                               PASS  template ids=6 (template,service) pairs=12
bucket holds exactly the edge's files (by sha256)          PASS  objects=3 edge files=3
tail: batches after the last upload answered 200           PASS  500 records acknowledged, none uploaded
inventory (central peer_parts) matches the bucket          PASS  peer_parts rows=3, sum(rows)=200000
edge data directory destroyed                              PASS  removed $SCRATCH/logless-restore.hM3rJ9/edge/data
case 1 restore exits 0                                     PASS  wall 996 ms; RESTORE OK: 3 parts, 200000 rows verified
case 1: parts, rows, bytes equal the edge's                PASS  restored parts=3 rows=200000 bytes=3166098; edge parts=3 rows=200000 bytes=3166098
case 1: zero mismatches, missing, unreadable               PASS  hash_mismatch=0 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=0 not_in_inventory=0 reverify_bad=0
case 1: per-template row counts identical                  PASS  6 groups (6 template ids + none): 1,33334 2,33334 3,33333 4,33333 5,33333 6,33333
case 1: per-(template,service) row counts identical        PASS  12 groups
case 1: catalog templates (id,count) identical             PASS  6 templates
case 1: dictionary (template_id,service) keys identical    PASS  12 keys (dictionary text differs by design: exemplar, not mined)
case 1: rebuilt catalog files table matches                PASS  files, rows = 3 200000
RPO: records acknowledged after the last upload are not in the restore PASS  restored rows=200000 = the 200000 uploaded; 500 acknowledged after the last upload are absent (the killed edge's WAL replayed 0 of them)
case 2 (flipped byte) exits 1                              PASS  wall 580 ms
case 2: the damaged part is named, nothing else is         PASS  hash_mismatch=1 victim=60c8045c85fca655…
case 2: the other parts restored, the damaged one was not installed PASS  restored parts=2 rows=100000 (edge 3 parts, 200000 rows; victim had 100000 rows)
case 3 (part deleted) exits 1                              PASS  wall 763 ms
case 3: the missing part is named, nothing else is         PASS  missing=1 gone=ae807f40e3c30ca0…
case 3: every other part restored                          PASS  restored parts=2 rows=150000 (missing part had 50000 rows)
case 4: restoring over an existing store is refused        PASS  Error: Refused("…")

elapsed 117 s; 27 passed, 0 failed
```

(`...` marks the repeated `templates=… reverify_bad=… elapsed_ms=…` lines of cases 2 and 3, elided here.)

## Run B: 6,000 records, three runs in a row

All three exited 0 with 27 passed, 0 failed. Case 1 restore of 3 parts, 6,000 rows, 113,175 bytes (run 1 output
`restored_parts=3 rows=6000 bytes=113175`):

| run | case 1 wall | case 2 wall (exit 1) | case 3 wall (exit 1) |
|---|---|---|---|
| 1 | 112 ms | 82 ms | 86 ms |
| 2 | 135 ms | 111 ms | 124 ms |
| 3 | 142 ms | 117 ms | 148 ms |

In run 1 the killed edge's WAL replayed 171 of the 500 acknowledged tail records; in run A it replayed 0. Either way none
of the 500 is in the bucket, so none is restored.

## Measured RTO, and what it is not

Wall time for the whole `logless restore` process (list, fetch, hash, decode every row, install, rebuild catalog and
dictionary, re-verify), over loopback to a local MinIO: 996 ms for 3 parts, 200,000 rows, 3,166,098 bytes (3.0 MiB).
That is not a prediction for production: the data is small (the six message shapes compress to 16 bytes per row),
the link is loopback, and the disk is an APFS SSD. The restore reads every row once, so its cost grows with rows and
with link bandwidth. Nothing here measures a multi-GB store or a real network.

## What the drill found

* Detection works as designed: a flipped byte in a stored part is reported by name as a SHA-256 mismatch and the part is
  not installed; a deleted part is reported by name against the inventory; both exit 1 and the remaining parts restore.
* Without `--inventory` a deleted part is invisible. This is covered by a unit test and stated in `docs/restore.md`.
* Acknowledged is not uploaded: 500 records answered HTTP 200 and were absent from the restore. That is the RPO decision
  made visible; it is not a defect of the restore.
* Template text is not in the bucket, so restored template text is an exemplar (see `docs/restore.md`). Ids, counts and
  the `(template_id, service)` keys were identical to before in every run.
* Flake, environmental: one of five runs failed because the machine had 16,685 sockets in TIME_WAIT (measured with
  `netstat` just after) and loopback connects failed with `Can't assign requested address (os error 49)`, from curl and
  from the restore's own S3 listing. Other sessions were using the loopback at the time. The script now retries OTLP
  posts (`--retry 6 --retry-all-errors`); the restore itself does not retry, and an `fetch_failed` or list error is
  reported honestly if it happens.

## Rust tests

`cargo test -p logless-core -- restore:: s3_sigv4 parts:: catalog::` : 33 passed, 0 failed. The verifier tests in
`crates/logless-core/src/restore.rs` cover: a clean restore (counts, rebuilt catalog, dictionary, level directories),
a flipped byte (named, not installed), a part missing from the inventory (named; invisible without one), a file whose
name is its own hash but is not Parquet (unreadable), inventory size and row disagreements, foreign keys ignored and an
unlisted intact part restored but reported, refusal of a non-empty target, S3 listing XML parsing, and presigning with
extra query parameters.
