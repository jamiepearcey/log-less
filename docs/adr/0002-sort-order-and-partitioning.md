# ADR 0002: Partition on event time, use 8k-row row groups, keep the sort order

Status: accepted and implemented (Plane LOG-172, "layout v3 at the edge"). The store is cut on the event hour with 8k-row row
groups and sort key 3 is `timestamp`; the write fan-out this ADR left open is bounded by the late spool. What was built and
how it measures with the store's own arrow-rs writer: `docs/evidence/layout-v3.md` and `docs/architecture.md` section 2.
Date: 2026-10-11. Evidence: `docs/evidence/sort-layout-bench.md`, script `scripts/sort-layout-bench.py`.

## Context

`docs/architecture.md` section 2 and `docs/platform-thesis.md` section 15 left one question open: files are partitioned
`hour=<observed>/level=<bucket>` and sorted `(service, trace_id, observed)`, so an event-time (`timestamp`) query on replayed or
clock-skewed data cannot be pruned. Three ways out were named and none chosen: sort time-first, partition on event time, or
shrink row groups. Plane 73 asks for a decision with numbers; Plane 16 requires that "trace and replay both prune".

## Measured (3.36M-row synthetic corpus, 10 layouts, two partition sizes)

Detail and caveats are in the evidence file. At the 12 h scale (partitions of about 52k rows, largest 253k):

| shape | today (L0) | time-first 8k (L7) | event-hour, 64k (L2) | **event-hour, 8k (L8)** |
|---|---:|---:|---:|---:|
| Q2 issue list, one hour: files / MB | 9.3 / 1.87 | 9.3 / 0.74 | 1 / 0.27 | **1 / 0.28** |
| Q1b trace, time hint: MB | 65.7 | 20.3 | 24.2 | **16.8** |
| Q3 template, 24h-eq. window: MB | 28.2 | 16.6 | 12.7 | **4.19** |
| Q3u template, all time: MB | 35.8 | 35.8 | 33.6 | **10.5** |
| Q4 service + warn+, 24h-eq.: MB | 7.35 | 4.02 | 3.14 | **0.86** |
| Q1a trace, no time bound: MB (ideal 27-44) | 143 | 136 | 142 | 98.9 |
| store bytes / files after compaction | 143.1 MB / 45 | 135.6 MB / 45 | 142.6 MB / 54 | 146.4 MB / 54 |
| files created before compaction (168 segments) | 546 | 546 | 3,810 | 3,810 |

At the 72 h scale (partitions of about 9k rows) the same ordering holds for Q2 and Q1b (L8: 0.04 and 5.0 MB against
0.74 and 33.7 for L0); there 2k-row groups beat 8k on Q3/Q4/Q3u, so row-group size must be small relative to partition size.
A reader that assumes `observed ~ timestamp` to prune returns only 85% of the rows on average (as little as 18% for one
query) at 12 h, so observed-hour partitioning has no safe event-time prune but `hour >= hour(a)`.

## Decision

1. **Partition on event time**: `hour=<hour of timestamp>/level=<bucket>` (option 2). It is the only layout that makes replayed
   and skewed data prune by directory, it adds 2.3% bytes (3.7% at 72 h) and a few more files after compaction (54 against 45; 234 against 225).
   The two-level variant (observed hour then event hour, L4) is rejected: 360 files against 45 at 12 h and 2,160 against 225 at
   72 h, median 134 rows per file at 72 h.
2. **Shrink row groups from 64k to 8k rows** (option 3) in the same change. It is +2.7% to +3.1% bytes and cuts Q3/Q4 by
   3.4x and 3.6x on today's layout. It is independent of the partition change and can ship first (one constant,
   `ROW_GROUP_ROWS` in `store.rs`). 4k and 2k need re-measuring on the arrow-rs writer; at the 72 h scale 2k roughly halves
   Q3/Q4 again for +1.7% bytes.
3. **Keep the sort order** `(service, trace_id, observed, event_id)` (option 1 rejected). Time-first wins Q2 and Q1b modestly at
   8k, but loses Q3 by 2x, Q4 by 2x and Q3u by 3x against sorting by service, and once the partition is the event hour a time
   sort inside the file has almost nothing left to prune. No candidate sort showed a clear win over the current one inside
   an event-hour partition; that specific combination (event-hour partition with other sorts) was **not run** and is the
   obvious follow-up if someone wants to tune further (a template-id key after service is the first thing I would try, since
   template ids are per service).
   Changing the third key from `observed` to `timestamp` is free inside an event-hour file and removes the dependency on
   `observed` for ordering; recommended with the migration, not measured separately.

## Consequences and migration notes for Plane 74 and 16

(Written before the implementation. Each item below was done as it says, except where `docs/evidence/layout-v3.md` notes
otherwise: the fan-out fix is the late spool, the compaction rule is "skip a partition written to in the last
`compact_quiet`", and an existing store is rewritten offline by `logless migrate-layout` rather than read in both layouts.)

Retention stays "delete a directory", now on event time. A replayed row older than the retention horizon is deleted by the next
pass; that is probably right but is a behaviour change to state in the retention docs. Pressure relief is file-granular and
unaffected.

What has to change in code (found by reading, not by building):

- `partition.rs` `PartitionKey::from_nanos` is fed `observed`; the merger and compactor (`merge.rs`, `compact.rs`) must key on
  `timestamp`. The schema comment on `observed` ("partitioning and retention use this one") and `architecture.md` section 2 change.
- `scan.rs` prunes the `hour=` directory and the row-group statistics on `observed`. It must prune on `timestamp`, or replay
  windows silently stop matching their directory. `docs/s3-serve.md` examples (`WHERE hour = ...`) now mean event hour.
- **Write fan-out is the real cost.** Writing one file per (segment, partition) gives 7.0x (12 h) to 9.9x (72 h) more
  pre-compaction files with the replay, and 4.9x to 6.5x even with only the skewed service. Each flush of a segment that
  carries skewed or replayed rows creates one tiny file per touched event hour. Compaction (`min_files = 4`) repairs this
  afterwards, but the merge path should route rows for non-current event hours to a buffered/staged path rather than one
  fsynced file each. This needs design in Plane 74; it was not built or measured.
- `compact.rs` skips the "active hour" (`active_hour == key.epoch_hour`) on the assumption that only the ingest hour is being written. Under event-time keys any partition can
  receive late data at any moment; the write-then-record-then-unlink order tolerates it but the skip rule needs review.
- Existing stores: rows whose event hour differs from the directory hour are 14.1% of the benchmark corpus (10.7% replay, 4.0%
  skewed service, plus hour-boundary crossings), spread over every partition. An online job can rewrite each partition into a
  "stays" file and move only the minority of rows, which is cheap, and until it has run old and new partitions are both
  readable if `scan.rs` also keeps the observed bound for legacy directories.
- Peer sync (`sync.rs`, `peer.rs`) and the template dictionary export reference files by catalog row; I did not verify that
  nothing in them infers the ingest hour from the path. Check before starting 74.

Plane 16 ("trace and replay both prune"): replay prunes (1 file for an hour window). Trace lookup prunes **with a time hint**
(16.8 MB against 65.7 MB) but **not without one, in any layout measured** (Q1a: min/max cannot prune `trace_id` because a row
group spans several services, and the `(service, trace_id)` sort only helps when one service dominates a partition). If
"trace range scans prune" must hold for unbounded lookups, that needs a bloom filter or a catalog-level trace index, neither of
which this benchmark could measure (see evidence, "Fidelity limits"). The honest status of that half of the criterion is open.

## Not decided here

- The arrow-rs writer was not used; confirm the 8k and partition results with it, plus bloom filters on `trace_id`.
- Whether time-first costs file size on real log text (this corpus said no, the earlier in-repo measurement said yes).
- Behaviour at much larger partitions (millions of rows an hour): the corpus tops out at 253k.
