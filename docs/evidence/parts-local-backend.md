# Parts on central's own disk: evidence

Run on 2026-10-11 (BST) on branch `wf/parts-local` at `2f53a55` (the code) with the scripts of `602cd88`, base
`8dd1b79`. Decision and protocol: `docs/adr/0003-parts-push-to-central-local-backend.md`. Machine: Apple M4 Max, macOS
26.1, everything on loopback and one APFS volume. Other sessions were compiling on the same machine during every run,
so the timings are from a loaded machine.

Binary: `cargo build -p logless-cli`, a **debug build**, copied to a scratch directory before the runs and passed as
`LOGLESS_BIN`. No release build was made, so every throughput number below is a debug-build number and says nothing
about a release build. `$SCRATCH` is the scratch directory the scripts were pointed at with `TMPDIR`. MinIO (s3 runs
only): `RELEASE.2025-10-15T17-29-55Z`.

## Counts

Each script was run twice in each mode, one after the other, with the same binary. Every run exited 0.

| Script | `PARTS_BACKEND` | Run 1 | Run 2 |
|---|---|---|---|
| `scripts/e2e-stack.sh` | `local` (default, no MinIO) | 32 passed, 0 failed, 0 skipped (21 s) | 32 passed, 0 failed, 0 skipped (25 s) |
| `scripts/e2e-stack.sh` | `s3` (MinIO) | 28 passed, 0 failed, 0 skipped (16 s) | 28 passed, 0 failed, 0 skipped (16 s) |
| `scripts/restore-drill.sh` | `local` (default, no MinIO) | 27 passed, 0 failed (6 s) | 27 passed, 0 failed (7 s) |
| `scripts/restore-drill.sh` | `s3` (MinIO) | 27 passed, 0 failed (8 s) | 27 passed, 0 failed (7 s) |

The local e2e run has 5 checks the s3 run does not (parts store status, token refusals, the read-only `parts` bucket,
and the two for the large part) and lacks 1 (MinIO exits on SIGTERM): 28 - 1 + 5 = 32. The drill has the same 27
checks in both modes.

Parts uploaded by the edge's own job in those runs: 21 and 20 (local e2e), 18 and 18 (s3 e2e), 3 in every drill run.
The number varies with how the edge's WAL segments happened to merge; each run checks it against the files on disk
(or objects), the catalog rows and the sweep's deletions, which all agreed.

Rust tests, `cargo test -p logless-core -- parts:: bounded_http:: restore:: s3_serve:: peer:: admin_api:: config::
httpd::`: 197 passed, 0 failed. `parts::` alone is 28 tests: the 18 that were there (the S3 backend against the
fake S3 server, unchanged) and 10 new ones for the local backend. The other new tests are one each in `bounded_http`,
`s3_serve` and `restore`. The whole workspace suite was not run.

## The large part: throughput, and central's memory

In the local e2e run the script asks central for an upload URL for 128 MiB of random bytes, exactly as the edge does
(`POST /v1/parts/presign`), and sends them with `curl -T` and the two headers central named. Meanwhile it samples
central's RSS (`ps -o rss=`) every 20 ms.

| | Run 1 | Run 2 |
|---|---|---|
| Part | 134,217,728 bytes | 134,217,728 bytes |
| PUT answered | 200 `{"already_present":false,"stored":true}` | 200, the same |
| Wall time of the PUT (curl `time_total`) | 4.37 s | 5.21 s |
| Throughput | 29 MiB/s | 25 MiB/s |
| Stored file's SHA-256 equals its name | yes | yes |
| Central RSS before the upload | 38 MiB | 39 MiB |
| Central peak RSS during it | 38 MiB (152 samples) | 39 MiB (147 samples) |
| RSS growth | 0 MiB (under the 1 MiB the report rounds to) | 0 MiB |

So central did not buffer the part: its memory did not move while 128 MiB went through it. The check in the script
fails if RSS grows by a sixteenth of the part (8 MiB here).

What the throughput number is and is not: the time covers reading the file, loopback, central's SHA-256, the writes,
`fsync` of the file and of two directories, and the rename, in an unoptimised build on a busy machine. Where the time
goes was not profiled. It is not a statement about a network or about a release build.

## Full output: e2e, local backend, run 1

```text
[10:52:14] run dir $SCRATCH/logless-e2e.R6EL1s, logless logless 0.1.0, parts backend local
[10:52:15] central: peer 60739, admin 60740
[10:52:15] edge: otlp 60743, hec 60744, sentry 60745, admin 60747
[10:52:15] sentry: 40 requests through @sentry/node

CHECK                                        RESULT DETAIL
-----                                        ------ ------
otlp batches answered 200                    PASS  3000 records over OTLP/HTTP JSON
hec batches answered 200                     PASS  3000 events over /services/collector/event
sentry demo ran                              PASS  done: 40 requests sent through the real @sentry/node SDK
edge synced everything it wrote              PASS  received=6043 written=6043 sent_records=6043 backlog_bytes=0
parts job done                               PASS  state=done files_total=21 uploaded=21 rejected=0 error=None (edge files=21)
stored parts match their sha256              PASS  objects=21 uploaded=21 mismatched=0
each part is an edge file                    PASS  objects with no edge file of that sha256: 0
central catalog lists the parts              PASS  peer_parts rows=21
foreign prefix and traversal refused; nothing outside own prefix PASS  foreign presign=403 traversal presign=422 foreign commit=403 objects outside edge-1/=0
catalog listing by node and tier             PASS  warm parts of edge-1=21 (uploaded=21) parts of edge-2=0
central's parts store status matches its disk PASS  backend=local uploads=21 used_bytes=189990 (files on disk: 189990 bytes) budget_bytes=68719476736 refusals=0
upload without a valid token is refused; nothing written PASS  no token=403 forged token=403 traversal path=403 files under parts/=21
parts are readable (only) through central's S3 endpoint PASS  bucket parts lists 21 keys under edge-1/ (disk has 21); GET sha256 matches name=yes; PUT=405
node retires its own part to cold            PASS  retire=200 repeat=404 cold parts=1
retention sweep deletes expired catalogued parts and audits them PASS  dry-run would_delete=21 objects after dry run=21 sweep deleted/failed='21 0' objects after=0 catalog rows=0 expire audit rows=21
a 128 MiB part is stored whole               PASS  PUT=200 in 4.370746s = 29 MiB/s; stored sha256 matches=yes ({"already_present":false,"stored":true})
central streams the part (RSS growth under 1/16 of it) PASS  central RSS before=38 MiB peak=38 MiB growth=0 MiB over 152 samples; part=128 MiB
peer ack counters sane                       PASS  edge sent=6043 dup=0 busy=0; receiver requests=122 accepted=6043 dup=0 busy=0 wal_waits=49 wal_wait_max_ms=176
edge exits on SIGTERM within 10 s            PASS  779 ms
central exits on SIGTERM within 10 s         PASS  732 ms
no ERROR lines in edge log                   PASS  0 ERROR lines
no ERROR lines in central log                PASS  0 ERROR lines
edge logged a drained shutdown               PASS  5 'stopped' lines
central logged a drained shutdown            PASS  3 'stopped' lines
edge verify (accounting invariant)           PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central verify (accounting invariant)        PASS  OK: every accepted record is committed, still in the WAL, or was deliberately dropped
central otlp records == sent                 PASS  rows=3000 unique_ids=3000 sent=3000
central hec records == sent                  PASS  rows=3000 unique_ids=3000 sent=3000
central sentry records == edge               PASS  central=43 edge=43 (proxy items_received=43)
sentry proxy counted nothing forwarded       PASS  items_forwarded=0 held_locally=43 items_received=43
central total == edge total, ids unique      PASS  central rows=6043 ids=6043; edge rows=6043 ids=6043
rollup reconciliation (fidelity 100)         PASS  rollup records=0 rollup counts=0 + plain records=6043 = 6043; edge wrote 6043

elapsed 21 s; 32 passed, 0 failed, 0 skipped
E2E PASS
```

## Full output: restore drill, local backend, run 1

The restore reads `<central data dir>/parts/edge1` directly (`logless restore --from <dir> --inventory <copy of
central's catalog> --node edge1 --verify`), while central is still running. The flipped byte and the deleted part are
made on central's disk.

```text
[10:52:36] run dir $SCRATCH/logless-restore.CvVZNC, logless logless 0.1.0, parts backend local
[10:52:37] central: peer 61055 admin 61056; edge: otlp 61060 admin 61061
[10:52:41] edge killed with SIGKILL after 500 acknowledged records that were never uploaded; its WAL replays 0 records

case 1 output:
  listed=3 ignored_keys=0 inventory=3
  restored_parts=3 rows=6000 bytes=112909
  hash_mismatch=0 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=0 not_in_inventory=0
  catalog_files=3 catalog_rows=6000 catalog_bytes=112909
  templates=6 template_services=12 dictionary_rows=12 multi_partition_parts=0
  reverify_bad=0
  elapsed_ms=60
  RESTORE OK: 3 parts, 6000 rows verified
case 2 output:
  listed=3 ignored_keys=0 inventory=3
  restored_parts=2 rows=3000 bytes=58170
  hash_mismatch=1 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=0 not_in_inventory=0
  catalog_files=2 catalog_rows=3000 catalog_bytes=58170
  templates=6 template_services=12 dictionary_rows=12 multi_partition_parts=0
  reverify_bad=0
  elapsed_ms=60
    57ac329e55c999c2f0ca7f60517adf63aff800befdb07590f50c4b84bbbecfdc.parquet: sha256 of the bytes is b7990c539500aeb323a3d211e7615fabe6c74c1ab9697957eda2165f370afb47, not the name
  RESTORE FAILED: 1 damaged or missing parts (listed above); the damaged ones were not installed
case 3 output:
  listed=2 ignored_keys=0 inventory=3
  restored_parts=2 rows=4500 bytes=81428
  hash_mismatch=0 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=1 not_in_inventory=0
  catalog_files=2 catalog_rows=4500 catalog_bytes=81428
  templates=6 template_services=12 dictionary_rows=12 multi_partition_parts=0
  reverify_bad=0
  elapsed_ms=48
    015c325cb2b2c3c30ebddec8004e3434d5cd2e18b9e6e115ee6f46f96c76fd3e.parquet: in the inventory, not in the bucket
  RESTORE FAILED: 1 damaged or missing parts (listed above); the damaged ones were not installed

CHECK                                                      RESULT DETAIL
-----                                                      ------ ------
ingest: all batches answered 200                           PASS  6000 records over OTLP/HTTP JSON
edge synced what it accepted                               PASS  received=6000 written=6000 sent_records=6000 backlog_bytes=0
edge stops cleanly before the flush                        PASS  1158 ms
edge verify after merge (accounting invariant)             PASS  received=6000 committed=6000 in_wal=0 dropped=0 unaccounted=0
parts upload done                                          PASS  state=done files_total=3 uploaded=3 rejected=0
before: every accepted record is in a part                 PASS  edge parquet rows=6000 accepted=6000 files=3 bytes=112909
before: templates were mined                               PASS  template ids=6 (template,service) pairs=12
central holds exactly the edge's files (by sha256)         PASS  objects=3 edge files=3
tail: batches after the last upload answered 200           PASS  500 records acknowledged, none uploaded
inventory (central peer_parts) matches the stored parts    PASS  peer_parts rows=3, sum(rows)=6000
edge data directory destroyed                              PASS  removed $SCRATCH/logless-restore.CvVZNC/edge/data
case 1 restore exits 0                                     PASS  wall 96 ms; RESTORE OK: 3 parts, 6000 rows verified
case 1: parts, rows, bytes equal the edge's                PASS  restored parts=3 rows=6000 bytes=112909; edge parts=3 rows=6000 bytes=112909
case 1: zero mismatches, missing, unreadable               PASS  hash_mismatch=0 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=0 not_in_inventory=0 reverify_bad=0
case 1: per-template row counts identical                  PASS  6 groups (6 template ids + none): 1,1000 2,1000 3,1000 4,1000 5,1000 6,1000 
case 1: per-(template,service) row counts identical        PASS  12 groups
case 1: catalog templates (id,count) identical             PASS  6 templates
case 1: dictionary (template_id,service) keys identical    PASS  12 keys (dictionary text differs by design: exemplar, not mined)
case 1: rebuilt catalog files table matches                PASS  files, rows = 3 6000
RPO: records acknowledged after the last upload are not in the restore PASS  restored rows=6000 = the 6000 uploaded; 500 acknowledged after the last upload are absent (the killed edge's WAL replayed 0 of them)
case 2 (flipped byte) exits 1                              PASS  wall 108 ms; RESTORE FAILED: 1 damaged or missing parts (listed above); the damaged ones were not installed
case 2: the damaged part is named, nothing else is         PASS  hash_mismatch=1 victim=57ac329e55c999c2…
case 2: the other parts restored, the damaged one was not installed PASS  restored parts=2 rows=3000 (edge 3 parts, 6000 rows; victim had 3000 rows)
case 3 (part deleted) exits 1                              PASS  wall 75 ms; RESTORE FAILED: 1 damaged or missing parts (listed above); the damaged ones were not installed
case 3: the missing part is named, nothing else is         PASS  missing=1 gone=015c325cb2b2c3c3…
case 3: every other part restored                          PASS  restored parts=2 rows=4500 (missing part had 1500 rows)
case 4: restoring over an existing store is refused        PASS  Error: Refused("$SCRATCH/logless-restore.

elapsed 6 s; 27 passed, 0 failed
DRILL PASS
```

The `in the inventory, not in the bucket` wording in case 3 is the restore's fixed message; here "the bucket" is the
node's directory.

## A real `kill -9` of central mid-upload, and after the rename

Not one of the two scripts: an ad hoc script run once against the same binary (central only, a 64 MiB part sent with
`curl --limit-rate 8M` so the kill lands mid-body, then `kill -KILL`, restart, and the same requests an edge's retry
makes). Its output:

```text
1. presign: HTTP 200, headers ['expect', 'x-logless-upload-token'], url /v1/parts/blob/edge-1/e7…, part of 67108864 bytes
2. mid-upload, parts dir holds: edge-1/.upload-<id>.tmp (25165824 bytes so far)
3. central killed with SIGKILL; the client's PUT last status seen was 100 (100: told to continue, never answered). parts dir holds: edge-1/.upload-<id>.tmp 
4. central restarted. parts dir holds: []; log: removed temporary files of part uploads cut off by a restart leftovers=1
5. presign again: already_present=False; commit before any upload: HTTP 409
6. PUT with the token from before the kill: HTTP 403; with the new token: HTTP 200
7. killed again after the rename, before a commit, and restarted: parts dir [edge-1/e786e5c9….parquet ], sha256 of the file matches its name: yes
8. presign: already_present=True; commit: HTTP 200 {"recorded":true}; peer_parts rows: 1
```

Steps 2 to 6 are a crash mid-body: the bytes were in a temporary file only, the restart removed it, the part was
absent (not present, not committable), the old token was refused and a new upload worked. Steps 7 and 8 are a crash
after the rename and before any commit: the part was whole on disk, `already_present`, and the commit recorded it.
The Rust test of the same name (`a_crash_between_the_rename_and_the_commit_...`) stops the listener and plants a
temporary file instead of killing a process; this run is the real kill. A power loss (page cache gone) was not
simulated.

## Not run, not shown

* A release build, and any throughput number for one.
* A part over 128 MiB; the 512 MiB default cap itself; several uploads at once; an upload over a real network or a
  slow link (the stall and minimum-rate limits are tested in `bounded_http` with a 300 ms timeout, not with the peer
  listener's 30 s).
* The disk budget in the scripts. It is unit-tested (`the_disk_budget_refuses_with_507_counts_it_and_frees_with_a_delete`)
  with a 250-byte budget; a real full disk (`ENOSPC`) was not provoked.
* The edge's own job uploading a large file: the 128 MiB part is sent with `curl`, because the edge only uploads
  Parquet files it merged, and the e2e traffic makes small ones (about 9 KiB each here). The edge's code path is the
  one that uploaded the 18 to 21 small parts per run, and a 9 MiB part in `a_part_larger_than_the_request_cap_...`.
* A restore over the network from a local-backend central (not built; ADR 0003).
* Power loss, and bit rot on central's disk after an upload (nothing scrubs; `logless restore` is what finds it, as
  case 2 of the drill shows).
* The whole test suite, clippy and rustfmt.
