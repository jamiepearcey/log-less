# Layout v3 at the edge: what was measured (Plane LOG-172)

Slice 1 of `docs/design/multi-tenancy-and-indexing.md`: the store is
`tenant=/index=/hour=<event hour>/level=/node=/`, 8k-row row groups, sort `(service, trace_id, timestamp, event_id)`,
an `origin` column, and a late spool for rows that arrive old. Measured 2026-10-11 on one shared laptop (arm64 macOS)
with a **debug** build; counts are exact, wall times are secondary and were taken while other jobs ran.

## Verdict against the gate

| target | result |
|---|---|
| (a) an hour-window issue list reads at most 1 file | **met**: 1.0 file at both scales (9.3 and 71.2 on the old layout) |
| (b) 24 h template filter within 20% of the bench's best layout (L8) | **met**: 1.00x L8's bytes at both scales |
| (c) service + warn-and-above within 20% of L8 | **met**: 0.99x and 1.01x |
| (d) time-hinted trace lookup | 6 files and 16.7 MB (12 h) / 5.0 MB (72 h), against 29 files / 64.3 MB and 315.5 files / 33.5 MB |
| pre-compaction files at most 2x the old layout's, late spool on | **NOT met at the defaults**: 3.68x (12 h scale) and 7.18x (72 h scale). Late spool off: 6.98x and 9.89x. See "Write fan-out" |
| retention correct under a replay of old rows | met (tests named below) |
| `e2e-stack.sh`, `restore-drill.sh`, `parts-replicate-drill.sh` | pass: 46/46, 29/29, 33/33; e2e with `PARTS_BACKEND=s3` (MinIO) 41/41 |

## How it was measured

The corpus is the one `scripts/sort-layout-bench.py` builds for ADR 0002 (3,360,000 rows, 12 services, a service whose clock
is 1 to 6 h behind, and 360,000 rows replayed in three ingest hours after the live window), at its two scales (live window
12 h and 72 h). `--export-corpus` writes it in arrival order with the WAL segment of each row (168 segments of 20,000 rows).

`layout-bench` (`crates/logless-cli/src/bin/layout-bench.rs`) replays that file through the store's own code:

* **v3**: rows are appended through `wal::SplitWal` with the clock set to each batch's `observed` time, the main log is
  rolled at every segment boundary, and after every roll the closed segments of both logs are merged by `merge::merge_all`,
  as the maintenance pass does. Then everything left is merged and `compact::compact` runs until nothing more compacts.
* **main layout**: the layout before this change, **re-created, not the old binary**: one file per (segment, observed
  hour, level) under `hour=/level=`, sorted `(service, trace_id, observed, event_id)`, 64k-row row groups, written by
  the same arrow-rs writer with the same encodings, then the same compaction rule (a partition with at least 4 files
  becomes one). Its files carry the current schema (an all-NULL `origin` column the old layout did not have). Its
  pre-compaction count, 546 and 726, equals the count ADR 0002 derived for L0.

`scripts/sort-layout-bench.py --external NAME=DIR:event|observed` then measures those trees with the bench's own queries
and model: files opened after partition pruning (the safe rule for each layout), row groups left by min/max statistics,
`model` bytes (footers plus the needed column chunks of those row groups, from Parquet metadata), and the bytes DuckDB
1.5.2 pulled over a counting HTTP range server. Every query's row count was checked against the source table on every tree
(no mismatch), and min/max pruning never dropped a row group that held a match. `L0` and `L8` are the bench's own
DuckDB-written layouts, re-run in the same invocation.

Reproduce (about 25 minutes per scale in a debug build):

```
scripts/sort-layout-bench.py --hours 12 --out D/duck --layouts none --export-corpus D/corpus.parquet
layout-bench --corpus D/corpus.parquet --out D/main --mode main
layout-bench --corpus D/corpus.parquet --out D/v3 --mode v3 [--late-off | --late-threshold-secs N | --late-roll-secs N | --min-files N]
scripts/sort-layout-bench.py --hours 12 --out D/duck --layouts L0,L8 --external main=D/main/store:observed --external v3=D/v3/store:event
```

## Results

Columns of the query tables: the old layout and v3 as the store's arrow-rs writer makes them, v3 compacted with
`min_files = 2`, and the bench's DuckDB-written L0 (old layout) and L8 (the layout ADR 0002 chose). Means per query.

### scale 12 h

| run | late threshold | late roll | pre-compaction files (main WAL + late spool) | vs main layout | late rows | files / bytes after compaction | partitions with 1 / 2 / 3 / 4+ files | wall s |
|---|---|---|---:|---:|---:|---:|---|---:|
| main | - | - | 546 | 1.00x | 0 | 45 / 138,914,383 | 45 / 0 / 0 / 0 | 122 |
| v3-lateoff | off | - | 3,810 (3,810 + 0) | 6.98x | 0 | 54 / 146,714,756 | 54 / 0 / 0 / 0 | 180 |
| v3 | 120 min | 30 min | 2,010 (1,407 + 603) | 3.68x | 420,488 | 57 / 146,726,582 | 51 / 3 / 0 / 0 | 171 |
| v3-t15m | 15 min | 30 min | 1,167 (489 + 678) | 2.14x | 473,185 | 57 / 146,726,582 | 51 / 3 / 0 / 0 | 163 |
| v3-r2h | 120 min | 120 min | 1,601 (1,407 + 194) | 2.93x | 420,488 | 60 / 146,742,201 | 48 / 6 / 0 / 0 | 169 |
| v3-t15m-r2h | 15 min | 120 min | 702 (489 + 213) | 1.29x | 473,185 | 72 / 146,794,729 | 42 / 6 / 6 / 0 | 147 |
| v3-min2 | 120 min | 30 min | 2,010 (1,407 + 603) | 3.68x | 420,488 | 54 / 146,714,756 | 54 / 0 / 0 / 0 | 168 |

| shape | measure | main layout (arrow-rs) | v3 (arrow-rs) | v3, min_files 2 | bench L0 (DuckDB) | bench L8 (DuckDB) | v3 vs L8 bytes |
|---|---|---:|---:|---:|---:|---:|---:|
| (a) issue list, one hour, warn+ | files opened | 9.3 | 1.0 | 1.0 | 9.3 | 1.0 | |
| | row groups read / in files | 8.2 / 10.2 | 5.0 / 5.0 | 5.0 / 5.0 | 8.2 / 10.2 | 5.0 / 5.0 | |
| | model MB | 1.52 | 0.21 | 0.21 | 1.87 | 0.28 | 0.75 |
| | DuckDB http MB | 1.65 | 0.22 | 0.22 | 2.00 | 0.29 | |
| | rows returned | 37102.8 | 37102.8 | 37102.8 | 37102.8 | 37102.8 | |
| (a) same, incident hours | files opened | 10.3 | 1.0 | 1.0 | 10.3 | 1.0 | |
| | row groups read / in files | 9.0 / 11.3 | 7.3 / 7.3 | 7.3 / 7.3 | 9.0 / 11.3 | 7.3 / 7.3 | |
| | model MB | 1.81 | 0.28 | 0.28 | 2.22 | 0.37 | 0.75 |
| | DuckDB http MB | 1.95 | 0.29 | 0.29 | 2.37 | 0.38 | |
| | rows returned | 54906.7 | 54906.7 | 54906.7 | 54906.7 | 54906.7 | |
| (b) template filter, 24h-eq. | files opened | 33.0 | 12.0 | 12.0 | 33.0 | 12.0 | |
| | row groups read / in files | 35.0 / 58.0 | 30.3 / 141.0 | 30.3 / 141.0 | 35.3 / 58.0 | 30.7 / 141.0 | |
| | model MB | 26.96 | 4.18 | 4.18 | 28.17 | 4.19 | 1.00 |
| | DuckDB http MB | 10.96 | 1.10 | 1.10 | 9.98 | 0.99 | |
| | rows returned | 2694.7 | 2694.7 | 2694.7 | 2694.7 | 2694.7 | |
| (c) service + warn+, 24h-eq. | files opened | 11.0 | 4.0 | 4.0 | 11.0 | 4.0 | |
| | row groups read / in files | 11.0 / 12.0 | 5.7 / 24.0 | 5.7 / 24.0 | 11.0 / 12.0 | 5.7 / 24.0 | |
| | model MB | 7.21 | 0.86 | 0.86 | 7.35 | 0.86 | 0.99 |
| | DuckDB http MB | 5.54 | 0.89 | 0.89 | 5.84 | 0.90 | |
| | rows returned | 22232.7 | 22232.7 | 22232.7 | 22232.7 | 22232.7 | |
| (d) trace, +-30 min hint | files opened | 29.0 | 6.0 | 6.0 | 29.0 | 6.0 | |
| | row groups read / in files | 34.2 / 50.7 | 46.3 / 73.2 | 46.3 / 73.2 | 34.3 / 51.0 | 46.3 / 73.7 | |
| | model MB | 64.30 | 16.73 | 16.73 | 65.66 | 16.80 | 1.00 |
| | DuckDB http MB | 25.90 | 11.21 | 11.21 | 26.99 | 11.72 | |
| | rows returned | 22.0 | 22.0 | 22.0 | 22.0 | 22.0 | |
| trace, no time bound | files opened | 45.0 | 57.0 | 54.0 | 45.0 | 54.0 | |
| | row groups read / in files | 73.6 / 74.0 | 292.5 / 446.0 | 290.3 / 443.0 | 74.6 / 75.0 | 290.0 / 445.0 | |
| | model MB | 138.81 | 98.69 | 98.68 | 143.01 | 98.88 | 1.00 |
| | DuckDB http MB | 51.07 | 55.64 | 55.63 | 50.71 | 52.97 | |
| | rows returned | 5020.8 | 5020.8 | 5020.8 | 5020.8 | 5020.8 | |
| template, all time | files opened | 45.0 | 57.0 | 54.0 | 45.0 | 54.0 | |
| | row groups read / in files | 45.5 / 74.0 | 75.0 / 446.0 | 75.0 / 443.0 | 46.0 / 75.0 | 76.0 / 445.0 | |
| | model MB | 34.23 | 10.61 | 10.60 | 35.78 | 10.54 | 1.01 |
| | DuckDB http MB | 10.61 | 3.50 | 3.48 | 10.40 | 3.18 | |
| | rows returned | 6289.0 | 6289.0 | 6289.0 | 6289.0 | 6289.0 | |

| tree | files | row groups | bytes |
|---|---:|---:|---:|
| main | 45 | 74 | 138,914,383 |
| v3 | 57 | 446 | 146,726,582 |
| v3min2 | 54 | 443 | 146,714,756 |
| L0 | 45 | 75 | 143,058,311 |
| L8 | 54 | 445 | 146,372,874 |

### scale 72 h

| run | late threshold | late roll | pre-compaction files (main WAL + late spool) | vs main layout | late rows | files / bytes after compaction | partitions with 1 / 2 / 3 / 4+ files | wall s |
|---|---|---|---:|---:|---:|---:|---|---:|
| main | - | - | 726 | 1.00x | 0 | 429 / 148,253,795 | 90 / 66 / 69 / 0 | 77 |
| v3-lateoff | off | - | 7,178 (7,178 + 0) | 9.89x | 0 | 239 / 155,293,214 | 230 / 3 / 1 / 0 | 226 |
| v3 | 120 min | 30 min | 5,210 (1,535 + 3,675) | 7.18x | 449,216 | 237 / 155,284,737 | 231 / 3 / 0 / 0 | 216 |
| v3-t15m | 15 min | 30 min | 4,764 (668 + 4,096) | 6.56x | 473,635 | 237 / 155,284,737 | 231 / 3 / 0 / 0 | 205 |
| v3-r2h | 120 min | 120 min | 2,889 (1,535 + 1,354) | 3.98x | 449,216 | 240 / 155,297,845 | 228 / 6 / 0 / 0 | 192 |
| v3-t15m-r2h | 15 min | 120 min | 2,127 (668 + 1,459) | 2.93x | 473,635 | 252 / 155,347,653 | 222 / 6 / 6 / 0 | 157 |
| v3-min2 | 120 min | 30 min | 5,210 (1,535 + 3,675) | 7.18x | 449,216 | 234 / 155,273,743 | 234 / 0 / 0 / 0 | 237 |

| shape | measure | main layout (arrow-rs) | v3 (arrow-rs) | v3, min_files 2 | bench L0 (DuckDB) | bench L8 (DuckDB) | v3 vs L8 bytes |
|---|---|---:|---:|---:|---:|---:|---:|
| (a) issue list, one hour, warn+ | files opened | 71.2 | 1.0 | 1.0 | 37.8 | 1.0 | |
| | row groups read / in files | 14.5 / 71.2 | 1.0 / 1.0 | 1.0 / 1.0 | 9.3 / 37.8 | 1.0 / 1.0 | |
| | model MB | 0.79 | 0.04 | 0.04 | 0.74 | 0.04 | 0.84 |
| | DuckDB http MB | 1.76 | 0.05 | 0.05 | 1.30 | 0.06 | |
| | rows returned | 5444.8 | 5444.8 | 5444.8 | 5444.8 | 5444.8 | |
| (a) same, incident hours | files opened | 85.3 | 1.0 | 1.0 | 45.7 | 1.0 | |
| | row groups read / in files | 15.7 / 85.3 | 4.0 / 4.0 | 4.0 / 4.0 | 10.0 / 45.7 | 4.0 / 4.0 | |
| | model MB | 0.92 | 0.14 | 0.14 | 0.86 | 0.18 | 0.75 |
| | DuckDB http MB | 2.08 | 0.15 | 0.15 | 1.54 | 0.20 | |
| | rows returned | 30187.3 | 30187.3 | 30187.3 | 30187.3 | 30187.3 | |
| (b) template filter, 24h-eq. | files opened | 291.0 | 72.0 | 72.0 | 153.0 | 72.0 | |
| | row groups read / in files | 188.3 / 294.0 | 90.7 / 169.0 | 90.7 / 169.0 | 100.3 / 156.0 | 91.3 / 169.0 | |
| | model MB | 26.59 | 10.89 | 10.89 | 25.94 | 10.94 | 1.00 |
| | DuckDB http MB | 10.84 | 3.97 | 3.97 | 8.70 | 3.77 | |
| | rows returned | 2721.0 | 2721.0 | 2721.0 | 2721.0 | 2721.0 | |
| (c) service + warn+, 24h-eq. | files opened | 89.0 | 24.0 | 24.0 | 47.0 | 24.0 | |
| | row groups read / in files | 62.7 / 89.0 | 25.0 / 33.0 | 25.0 / 33.0 | 33.0 / 47.0 | 25.0 / 33.0 | |
| | model MB | 5.07 | 2.75 | 2.75 | 4.79 | 2.73 | 1.01 |
| | DuckDB http MB | 6.21 | 3.07 | 3.07 | 5.23 | 3.07 | |
| | rows returned | 22129.7 | 22129.7 | 22129.7 | 22129.7 | 22129.7 | |
| (d) trace, +-30 min hint | files opened | 315.5 | 6.0 | 6.0 | 167.5 | 6.0 | |
| | row groups read / in files | 50.5 / 318.5 | 15.7 / 16.2 | 15.7 / 16.2 | 35.7 / 170.5 | 15.7 / 16.2 | |
| | model MB | 33.48 | 5.01 | 5.01 | 33.65 | 5.01 | 1.00 |
| | DuckDB http MB | 15.09 | 2.28 | 2.28 | 13.22 | 2.40 | |
| | rows returned | 4.7 | 4.7 | 4.7 | 4.7 | 4.7 | |
| trace, no time bound | files opened | 429.0 | 237.0 | 234.0 | 225.0 | 234.0 | |
| | row groups read / in files | 427.1 / 432.0 | 503.8 / 526.0 | 503.7 / 523.0 | 228.0 / 228.0 | 504.4 / 523.0 | |
| | model MB | 148.11 | 151.40 | 151.39 | 149.70 | 151.47 | 1.00 |
| | DuckDB http MB |  |  |  | 52.69 |  | |
| | rows returned | 5012.2 | 5012.2 | 5012.2 | 5012.2 | 5012.2 | |
| template, all time | files opened | 429.0 | 237.0 | 234.0 | 225.0 | 234.0 | |
| | row groups read / in files | 426.0 / 432.0 | 232.0 / 526.0 | 232.0 / 523.0 | 225.5 / 228.0 | 232.5 / 523.0 | |
| | model MB | 57.64 | 28.67 | 28.66 | 56.76 | 28.47 | 1.01 |
| | DuckDB http MB |  | 12.25 | 12.24 | 14.75 | 11.57 | |
| | rows returned | 6289.0 | 6289.0 | 6289.0 | 6289.0 | 6289.0 | |

| tree | files | row groups | bytes |
|---|---:|---:|---:|
| main | 429 | 432 | 148,253,795 |
| v3 | 237 | 526 | 155,284,737 |
| v3min2 | 234 | 523 | 155,273,743 |
| L0 | 225 | 228 | 149,703,941 |
| L8 | 234 | 523 | 155,204,914 |

## Reading it

1. **Queries: the arrow-rs writer reproduces the bench's L8.** Bytes for (b), (c), (d) and the unbounded shapes are within
   1% of L8; (a) is 16 to 25% *below* L8 because arrow-rs's DELTA-encoded timestamps and its statistics are smaller than
   DuckDB's. Against the old layout written by the same writer: (a) 1 file instead of 9.3 / 71.2, 7x and 20x fewer bytes;
   (b) 6.4x and 2.4x fewer bytes; (c) 8.4x and 1.8x; (d) 3.8x and 6.7x.
2. **Store size**: +5.6% (12 h) and +4.7% (72 h) bytes over the old layout from the same writer; 57 files against 45 at
   12 h, and 237 against 429 at 72 h (the old layout leaves 135 partitions at 2 or 3 files, below `min_files`).
3. **A few partitions stay at 2 or 3 files** after compaction (3 of 54 and 3 of 234 at the defaults): they received
   fewer than `min_files = 4` files. The six issue-list hours measured are not among them, so (a) reads exactly one file,
   but an hour that is among them reads two or three. `min_files = 2` leaves every partition at one file (54 and 234) at
   the cost of rewriting on two files; the default was not changed.

## Write fan-out: the gate that is not met

Files created before compaction, one per (segment, partition):

| scale | old layout | v3, late spool off | v3, defaults (2 h, 30 min) | threshold 15 min | roll 2 h | 15 min and 2 h |
|---|---:|---:|---:|---:|---:|---:|
| 12 h | 546 | 3,810 (6.98x) | 2,010 (3.68x) | 1,167 (2.14x) | 1,601 (2.93x) | 702 (1.29x) |
| 72 h | 726 | 7,178 (9.89x) | 5,210 (7.18x) | 4,764 (6.56x) | 2,889 (3.98x) | 2,127 (2.93x) |

The late spool does what it was designed for and it is not enough at its defaults:

* At 12 h the **main WAL** alone writes 1,407 files (2.6x). The skewed service is 1 to 6 h behind; the fifth of its
  rows that are under 2 h old are not late, so every main segment still writes to two extra event hours times three
  levels. A 15-minute threshold sends all of that service to the spool and the main WAL drops to 489, below the old
  layout's 546.
* At 72 h a main segment spans about 26 minutes, the same as `late_roll`, so a late segment batches no more than a main
  one does and the spool itself writes 3,675 files. Only a longer roll helps (1,354 at 2 h).
* No setting tried reaches 2x at the 72 h scale. The best, a 15-minute threshold with a 2-hour roll, is 2.93x, and it
  costs up to 2 hours before a late row is in Parquet, and more partitions left at 2 or 3 files (12 of 234).

The defaults were left as specified (2 h, 30 min). Which of threshold, roll and the 2x target should move is the owner's
decision; `storage.late_threshold` and `storage.late_roll` are config, so no code changes with it. The late-off column
equals the 3,810 and 7,178 ADR 0002 computed for L8, which checks the harness against the bench.

## Retention under a replay

* `budget::tests::a_replay_of_old_rows_lands_by_event_hour_and_retention_takes_exactly_what_is_past_its_horizon`: 250 rows
  arrive at once, 150 of them replayed (10-day-old and 3-day-old info, 3-day-old debug, 20-day-old warn). All 150 take the
  late spool; after the merge each row is in its event hour; one maintenance pass expires exactly the two partitions past
  their bucket's retention (60 + 30 rows) and the catalog and a scan both hold the other 160; a second pass expires nothing.
* `retention::tests::a_replayed_old_row_is_deleted_on_the_next_pass`, `each_index_expires_by_its_own_buckets_and_one_delete_takes_every_writers_files`,
  `an_unconfigured_tenant_or_index_is_never_deleted`, and the pinned-file test on the deeper tree.

## Scripts

| script | result |
|---|---|
| `scripts/e2e-stack.sh` (local backend) | 46 passed, 0 failed, 0 skipped, including six new checks: every file at `tenant/index/hour/level/node`; an `audit` index holding exactly the 30 records sent to it on the edge and on central; HEC code 7 and 6 refused records counted; `origin` = the edge's node id on all 6,076 rows of both stores; the late spool drained |
| `PARTS_BACKEND=s3 scripts/e2e-stack.sh` (MinIO) | 41 passed, 0 failed (run once) |
| `scripts/restore-drill.sh` | 29 passed, 0 failed, including: every part back in the directory it was written in, catalog rows carrying tenant, index, node and the event hour |
| `scripts/parts-replicate-drill.sh` | 33 passed, 0 failed |
| `scripts/s3-duckdb.sh` (DuckDB and curl against S3 serve) | all checks passed, including one index read as one prefix with `tenant`, `index`, `node` as hive columns |

## Not measured

* Anything in a release build, and any throughput: ingest rate, merge and compaction time, and the cost of the second
  WAL's fsync were not measured. The harness wall times above are a debug build on a busy machine.
* The old layout as the old binary writes it. "main layout" is a re-creation with the current writer.
* The live agent's compaction cadence. The harness compacts once at the end with no partition considered busy; the
  10-minute quiet rule is covered by a unit test only, not by a long run.
* Bloom filters and page indexes (not written; design section 2.4 defers them), so unbounded trace lookup is unchanged
  and still reads the whole range.
* Partitions larger than this corpus's (largest 253k rows), a real customer store, more than one tenant or index under
  load, and the catalog's range query at scale (it is checked to use its index, not timed).
* S3 serve byte counts for these shapes (`s3-count-proxy.py`, which the design names, is in the console repo); the HTTP
  column above is the bench's own range server.
