# Sort order and partitioning benchmark (Plane 73)

Reproduce: `python3 scripts/sort-layout-bench.py --hours 72 --json r72.json` and `--hours 12 --json r12.json`
(needs only the `duckdb` Python package with `httpfs`; about 5 minutes per scale; writes about 150 MB per layout
under `~/tmp/logless-sort-bench` and deletes it). Raw results: `sort-layout-bench-72h.json`, `sort-layout-bench-12h.json`.
Decision: `docs/adr/0001-sort-order-and-partitioning.md`.

## What was measured

One synthetic corpus of **3,360,000 rows**, identical for every layout: 12 services, 360 template ids (Zipf within a
service), 323,267 distinct trace ids (39.7% of rows carry one; ordinary traces are 3-40 rows inside a 30 s window,
20 long-running traces carry 4.8k-5k rows each across the whole window, six incident bursts put up to 15,280 rows on one
trace and one error template inside ten minutes), 539,390 warn+ rows. 2,849,409 rows are live (observed = event time + up to
2 s, one service, `edge-gw`, 4% of rows, has its clock 1-6 h behind), 150,591 are incident rows, and **360,000 (10.7%) are a
replay: event times spread over the whole window, all observed in three ingest hours after it**. That replay and the skewed
service are what make `observed` (the partition key today) and `timestamp` (what people query) differ.

The corpus is generated twice with the same row count over a different live window, because partition size decides
whether sort order and row-group size can matter at all:

| scale | live window | median rows per (hour, level) | largest |
|---|---:|---:|---:|
| `--hours 72` | 72 h | 9,217 | 82,805 |
| `--hours 12` | 12 h | 52,527 | 253,249 |

At the 72 h scale most partitions fit in one 64k row group, so sort order inside a file cannot prune anything; the 12 h scale
(about 280k rows an hour) is the one where row groups per partition exceed one. Which one a real store looks like depends on
its volume; both are reported. Nothing here is a measurement of a customer store.

Layouts (all zstd level 3 plus dictionary, written one file per partition after a full sort, 1 thread):

| id | partition | sort within file | row group |
|---|---|---|---|
| L0 | observed hour / level (today) | service, trace_id, observed, event_id | 64k |
| L1 | observed hour / level | **timestamp**, service, trace_id | 64k |
| L2 | **event-time hour** / level | as L0 | 64k |
| L3, L3b | as L0 | as L0 | **8k, 2k** |
| L4 | observed hour / **event hour** / level | as L0 | 64k |
| L5 | as L0 | service, timestamp, trace_id | 64k |
| L6 | as L0 | service, timestamp, trace_id | 8k |
| L7 | as L0 | timestamp, service, trace_id | 8k |
| L8 | event-time hour / level | as L0 | 8k |

Queries (event time is `timestamp`; the partition-pruning rule is the only *safe* one for each layout, see below):
Q1a trace lookup with no time bound (12 traces: 3 incident, 3 long-running, 6 ordinary), Q1b trace lookup with a +-30 min
event-time hint (6), Q2 warn+ issue list for one event-time hour (6 quiet hours; Q2i 3 incident hours), Q3 template filter
over a 24 h-equivalent window (3 templates) and Q3u over all time (2), Q4 service + warn+ over a 24 h-equivalent window (3).

**Safe pruning rule.** Under observed-hour partitioning a query on event time `[a, b)` can only exclude partitions with
`hour < hour(a)`, because replayed rows land in later partitions. A reader that instead assumes `observed ~ timestamp` and
opens only `hour in [a, b]` returns wrong answers: recall column below (mean 0.853 / min 0.184 at 12 h; 0.891 / 0.75 at
72 h). All numbers in the tables use the safe rule, and every row count was checked against the source table (no mismatch).

**Measures.** Counts first: files opened (a footer read each), row groups read after min/max pruning, and bytes. Bytes come
three ways: `model` is the footers plus the column chunks the query needs in the row groups that survive min/max pruning,
from Parquet metadata (what a statistics-only reader like `scan.rs` or a granule pruner reads); `ideal` is the same for only
the row groups that truly hold a matching row (the floor for any index; min/max pruning was verified never to drop one of
them); `http` is what DuckDB httpfs actually pulled from a counting Range server on loopback (bytes served, same idea as the
S3-serve byte counter).

## Fidelity limits, stated plainly

- Files are written by **DuckDB 1.5.2's Parquet writer**, not the store's arrow-rs writer. Absolute sizes and row-group
  boundaries differ (DuckDB rounds row groups to 2,048-row vectors); layout-versus-layout ratios are the point.
  Timestamps are `TIMESTAMP_NS` without the store's DELTA encoding.
- The corpus is synthetic. In particular **time-first sort did not cost any file size here (-4.4% at 64k)**, whereas the
  existing in-repo measurement (`docs/tasks/current.md`, round-robin services destroying locality) found interleaving
  costs real bytes on real log text. This corpus' bodies carry random ids, so locality helps less than on real text. That
  question is not settled by this run, and time-first is rejected here on query cost, not on size.
- **Bloom filters were not measured.** DuckDB 1.5.2 returned wrong rows for BLOB equality predicates on its own files
  (a trace lookup returned 944 of 5,200 rows; bloom filters at several false-positive ratios and a no-dictionary
  write behaved the same; the same data written as UUID, which has no min/max statistics in DuckDB, returned all 5,200).
  The runs therefore use `hex(trace_id) = ...`, which is correct but gives DuckDB no pruning on trace ids; the model column,
  which applies min/max statistics itself, is the primary measure for trace queries and `ideal` is the bound for what a bloom
  filter or trace index could reach. A bloom filter written by arrow-rs needs its own measurement.
- `http` is skipped (blank) where a query would need more than about 2,500 loopback requests (macOS ephemeral-port budget),
  and `http` for trace queries is not comparable across row-group sizes for the reason above.
- Wall times (write column) were taken on a shared machine and are secondary; the counts are exact.
- "Pre-compaction files" assumes 168 WAL segments of 20,000 rows in arrival order; real segment sizes differ. The ratio is
  what carries over.

## Results

### Live window 12 h (3,360,000 rows; partition (hour,level) median 52,527 rows, largest 253,249)

Store shape per layout. `pre-compaction files` = exact count of (WAL segment, partition) pairs, 168 segments of 20,000 rows in arrival order.

| layout | files | row groups | bytes | write s (1 thread) | pre-compaction files | recall if reader assumes observed=timestamp (mean / min) |
|---|---:|---:|---:|---:|---:|---|
| L0 baseline (observed-hour, sort service/trace/observed, 64k) | 45 | 75 | 143.1 MB | 8.2 | 546 | 0.853 / 0.184 |
| L1 time-first sort, 64k | 45 | 75 | 136.8 MB | 6.0 | 546 | 0.853 / 0.184 |
| L2 event-time partition, baseline sort, 64k | 54 | 86 | 142.6 MB | 6.6 | 3,810 | n/a (partitioned on event time) |
| L3 baseline, 8k row groups | 45 | 431 | 147.0 MB | 5.8 | 546 | 0.853 / 0.184 |
| L3b baseline, 2k row groups | 45 | 1,668 | 147.9 MB | 20.6 | 546 | 0.853 / 0.184 |
| L4 observed-hour/event-hour two-level, 64k | 360 | 387 | 143.8 MB | 12.2 | 4,078 | n/a (partitioned on event time) |
| L5 service-then-time sort, 64k | 45 | 75 | 139.4 MB | 8.9 | 546 | 0.853 / 0.184 |
| L6 service-then-time sort, 8k | 45 | 431 | 143.6 MB | 10.1 | 546 | 0.853 / 0.184 |
| L7 time-first sort, 8k | 45 | 431 | 135.6 MB | 10.8 | 546 | 0.853 / 0.184 |
| L8 event-time partition, baseline sort, 8k | 54 | 445 | 146.4 MB | 7.6 | 3,810 | n/a (partitioned on event time) |

**Q1a: trace lookup, no time bound (12 traces).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 45.0 | 74.6 / 75.0 | 22.7 | 143.01 | 44.23 | 50.71 |
| L1 | 45.0 | 75.0 / 75.0 | 21.8 | 136.83 | 40.18 | 46.32 |
| L2 | 54.0 | 84.0 / 86.0 | 21.9 | 141.59 | 41.91 | 47.74 |
| L3 | 45.0 | 296.8 / 431.0 | 80.8 | 106.97 | 29.20 | 52.58 |
| L3b | 45.0 | 482.4 / 1668.0 | 126.7 | 50.35 | 13.27 | 72.18 |
| L4 | 360.0 | 385.6 / 387.0 | 69.3 | 143.72 | 41.15 |  |
| L5 | 45.0 | 75.0 / 75.0 | 22.8 | 139.40 | 43.15 | 50.18 |
| L6 | 45.0 | 430.9 / 431.0 | 114.6 | 143.62 | 38.66 | 51.87 |
| L7 | 45.0 | 430.9 / 431.0 | 110.8 | 135.57 | 35.32 | 73.78 |
| L8 | 54.0 | 290.0 / 445.0 | 76.7 | 98.88 | 27.01 | 52.97 |

**Q1b: trace lookup, +-30 min event-time hint (6).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 29.0 | 34.3 / 51.0 | 7.7 | 65.66 | 15.79 | 26.99 |
| L1 | 29.0 | 30.3 / 51.0 | 5.5 | 58.02 | 10.79 | 19.74 |
| L2 | 6.0 | 11.5 / 11.8 | 5.5 | 24.21 | 11.90 | 14.40 |
| L3 | 29.0 | 102.5 / 298.8 | 13.8 | 37.67 | 5.20 | 18.03 |
| L3b | 29.0 | 169.0 / 1157.8 | 14.2 | 18.39 | 1.77 | 37.78 |
| L4 | 48.5 | 52.8 / 53.0 | 7.3 | 24.56 | 10.56 | 15.24 |
| L5 | 29.0 | 34.0 / 51.0 | 7.7 | 63.99 | 15.44 | 25.82 |
| L6 | 29.0 | 114.0 / 298.8 | 13.8 | 40.34 | 4.85 | 14.10 |
| L7 | 29.0 | 60.8 / 298.8 | 6.0 | 20.27 | 2.06 | 9.78 |
| L8 | 6.0 | 46.3 / 73.7 | 12.7 | 16.80 | 4.81 | 11.72 |

**Q2: warn+ issue list, one event-time hour (6 quiet hours).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 9.3 | 8.2 / 10.2 | 8.2 | 1.87 | 1.86 | 2.00 |
| L1 | 9.3 | 8.0 / 10.2 | 8.0 | 1.68 | 1.68 | 1.82 |
| L2 | 1.0 | 1.0 / 1.0 | 1.0 | 0.27 | 0.27 | 0.28 |
| L3 | 9.3 | 18.5 / 44.5 | 18.5 | 1.10 | 1.09 | 1.20 |
| L3b | 9.3 | 55.0 / 168.0 | 54.8 | 1.01 | 0.96 | 1.30 |
| L4 | 8.0 | 8.0 / 8.0 | 8.0 | 0.28 | 0.28 | 0.45 |
| L5 | 9.3 | 8.2 / 10.2 | 8.2 | 1.70 | 1.70 | 1.84 |
| L6 | 9.3 | 17.5 / 44.5 | 17.2 | 1.01 | 0.98 | 1.10 |
| L7 | 9.3 | 12.7 / 44.5 | 11.8 | 0.74 | 0.70 | 0.84 |
| L8 | 1.0 | 5.0 / 5.0 | 5.0 | 0.28 | 0.28 | 0.29 |

**Q2i: same, incident hours (3).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 10.3 | 9.0 / 11.3 | 9.0 | 2.22 | 2.21 | 2.37 |
| L1 | 10.3 | 9.0 / 11.3 | 9.0 | 2.00 | 1.99 | 2.15 |
| L2 | 1.0 | 1.3 / 1.3 | 1.3 | 0.36 | 0.36 | 0.38 |
| L3 | 10.3 | 22.0 / 52.3 | 22.0 | 1.30 | 1.29 | 1.40 |
| L3b | 10.3 | 68.0 / 198.0 | 68.0 | 1.20 | 1.16 | 1.51 |
| L4 | 9.0 | 9.0 / 9.0 | 9.0 | 0.38 | 0.38 | 0.57 |
| L5 | 10.3 | 9.0 / 11.3 | 9.0 | 2.01 | 2.00 | 2.16 |
| L6 | 10.3 | 20.7 / 52.3 | 20.3 | 1.17 | 1.14 | 1.27 |
| L7 | 10.3 | 16.7 / 52.3 | 15.7 | 0.96 | 0.91 | 1.06 |
| L8 | 1.0 | 7.3 / 7.3 | 7.3 | 0.37 | 0.37 | 0.38 |

**Q3: template filter, 24h-equivalent window (3).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 33.0 | 35.3 / 58.0 | 7.0 | 28.17 | 3.75 | 9.98 |
| L1 | 33.0 | 38.7 / 58.0 | 7.0 | 30.51 | 3.63 | 10.45 |
| L2 | 12.0 | 14.7 / 21.0 | 4.0 | 12.73 | 3.10 | 4.49 |
| L3 | 33.0 | 59.3 / 353.0 | 8.7 | 8.28 | 1.12 | 1.96 |
| L3b | 33.0 | 107.3 / 1367.0 | 16.7 | 5.12 | 0.68 | 2.57 |
| L4 | 111.0 | 57.7 / 119.0 | 16.3 | 10.34 | 2.91 | 5.59 |
| L5 | 33.0 | 35.3 / 58.0 | 7.0 | 26.66 | 3.59 | 9.14 |
| L6 | 33.0 | 57.3 / 353.0 | 8.3 | 7.72 | 1.05 | 1.90 |
| L7 | 33.0 | 129.7 / 353.0 | 28.0 | 16.64 | 3.34 | 4.69 |
| L8 | 12.0 | 30.7 / 141.0 | 5.3 | 4.19 | 0.72 | 0.99 |

**Q3u: template filter, all time (2).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 45.0 | 46.0 / 75.0 | 15.0 | 35.78 | 9.26 | 10.40 |
| L1 | 45.0 | 62.5 / 75.0 | 16.5 | 44.18 | 9.13 | 10.65 |
| L2 | 54.0 | 39.0 / 86.0 | 12.0 | 33.58 | 9.08 | 10.19 |
| L3 | 45.0 | 85.5 / 431.0 | 17.5 | 11.83 | 2.41 | 3.31 |
| L3b | 45.0 | 213.0 / 1668.0 | 36.0 | 8.88 | 1.55 | 4.16 |
| L4 | 360.0 | 162.5 / 387.0 | 48.5 | 25.87 | 8.93 | 14.67 |
| L5 | 45.0 | 46.0 / 75.0 | 15.0 | 33.80 | 8.83 | 9.97 |
| L6 | 45.0 | 85.5 / 431.0 | 17.5 | 11.23 | 2.33 | 3.23 |
| L7 | 45.0 | 288.0 / 431.0 | 76.0 | 35.77 | 9.13 | 10.32 |
| L8 | 54.0 | 76.0 / 445.0 | 15.0 | 10.54 | 2.12 | 3.18 |

**Q4: service + warn+ scan, 24h-equivalent window (3).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 11.0 | 11.0 / 12.0 | 8.0 | 7.35 | 4.78 | 5.84 |
| L1 | 11.0 | 11.0 / 12.0 | 8.0 | 7.16 | 4.65 | 5.57 |
| L2 | 4.0 | 4.0 / 4.0 | 4.0 | 3.14 | 3.14 | 3.20 |
| L3 | 11.0 | 14.3 / 56.0 | 12.0 | 2.03 | 1.74 | 1.95 |
| L3b | 11.0 | 26.0 / 212.0 | 23.0 | 1.18 | 1.00 | 1.31 |
| L4 | 37.0 | 25.0 / 37.0 | 23.0 | 3.12 | 2.18 | 2.86 |
| L5 | 11.0 | 11.0 / 12.0 | 8.0 | 7.06 | 4.60 | 5.55 |
| L6 | 11.0 | 13.3 / 56.0 | 10.0 | 1.85 | 1.43 | 1.69 |
| L7 | 11.0 | 30.0 / 56.0 | 20.0 | 4.02 | 2.69 | 3.07 |
| L8 | 4.0 | 5.7 / 24.0 | 5.7 | 0.86 | 0.86 | 0.90 |

### Live window 72 h (3,360,000 rows; partition (hour,level) median 9,217 rows, largest 82,805)

Store shape per layout. `pre-compaction files` = exact count of (WAL segment, partition) pairs, 168 segments of 20,000 rows in arrival order.

| layout | files | row groups | bytes | write s (1 thread) | pre-compaction files | recall if reader assumes observed=timestamp (mean / min) |
|---|---:|---:|---:|---:|---:|---|
| L0 baseline (observed-hour, sort service/trace/observed, 64k) | 225 | 228 | 149.7 MB | 6.7 | 726 | 0.891 / 0.750 |
| L1 time-first sort, 64k | 225 | 228 | 148.5 MB | 6.6 | 726 | 0.891 / 0.750 |
| L2 event-time partition, baseline sort, 64k | 234 | 234 | 149.9 MB | 6.8 | 7,178 | n/a (partitioned on event time) |
| L3 baseline, 8k row groups | 225 | 515 | 154.4 MB | 8.0 | 726 | 0.891 / 0.750 |
| L3b baseline, 2k row groups | 225 | 1,764 | 157.1 MB | 18.4 | 726 | 0.891 / 0.750 |
| L4 observed-hour/event-hour two-level, 64k | 2,160 | 2,160 | 155.1 MB | 22.6 | 8,521 | n/a (partitioned on event time) |
| L5 service-then-time sort, 64k | 225 | 228 | 148.8 MB | 7.3 | 726 | 0.891 / 0.750 |
| L6 service-then-time sort, 8k | 225 | 515 | 154.3 MB | 10.7 | 726 | 0.891 / 0.750 |
| L7 time-first sort, 8k | 225 | 515 | 148.8 MB | 9.0 | 726 | 0.891 / 0.750 |
| L8 event-time partition, baseline sort, 8k | 234 | 523 | 155.2 MB | 11.3 | 7,178 | n/a (partitioned on event time) |

**Q1a: trace lookup, no time bound (12 traces).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 225.0 | 228.0 / 228.0 | 58.3 | 149.70 | 39.11 | 52.69 |
| L1 | 225.0 | 228.0 / 228.0 | 58.3 | 148.50 | 38.99 | 52.93 |
| L2 | 234.0 | 233.1 / 234.0 | 56.1 | 149.90 | 38.78 | 51.10 |
| L3 | 225.0 | 493.5 / 515.0 | 124.3 | 149.66 | 38.10 |  |
| L3b | 225.0 | 1294.8 / 1764.0 | 302.8 | 119.95 | 28.57 |  |
| L4 | 2160.0 | 2026.7 / 2160.0 | 169.0 | 154.76 | 36.52 |  |
| L5 | 225.0 | 228.0 / 228.0 | 58.3 | 148.75 | 38.86 | 52.88 |
| L6 | 225.0 | 514.9 / 515.0 | 129.3 | 154.25 | 39.43 |  |
| L7 | 225.0 | 515.0 / 515.0 | 128.2 | 148.80 | 37.80 |  |
| L8 | 234.0 | 504.4 / 523.0 | 124.5 | 151.47 | 38.67 |  |

**Q1b: trace lookup, +-30 min event-time hint (6).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 167.5 | 35.7 / 170.5 | 2.5 | 33.65 | 2.79 | 13.22 |
| L1 | 167.5 | 33.7 / 170.5 | 2.5 | 32.33 | 3.16 | 12.70 |
| L2 | 6.0 | 6.0 / 6.0 | 2.0 | 5.00 | 2.10 | 2.81 |
| L3 | 167.5 | 74.2 / 393.7 | 3.8 | 26.37 | 1.35 | 11.63 |
| L3b | 167.5 | 143.0 / 1356.2 | 4.3 | 15.55 | 0.47 | 24.71 |
| L4 | 60.0 | 57.3 / 60.0 | 2.5 | 5.15 | 1.81 | 3.78 |
| L5 | 167.5 | 35.7 / 170.5 | 2.5 | 33.80 | 2.77 | 13.18 |
| L6 | 167.5 | 74.5 / 393.7 | 3.8 | 26.64 | 1.36 | 10.75 |
| L7 | 167.5 | 39.2 / 393.7 | 2.5 | 13.72 | 0.78 | 6.58 |
| L8 | 6.0 | 15.7 / 16.2 | 3.7 | 5.01 | 1.24 | 2.40 |

**Q2: warn+ issue list, one event-time hour (6 quiet hours).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 37.8 | 9.3 / 37.8 | 9.3 | 0.74 | 0.70 | 1.30 |
| L1 | 37.8 | 9.3 / 37.8 | 9.3 | 0.76 | 0.72 | 1.32 |
| L2 | 1.0 | 1.0 / 1.0 | 1.0 | 0.04 | 0.04 | 0.06 |
| L3 | 37.8 | 12.7 / 49.3 | 12.7 | 0.70 | 0.65 | 1.25 |
| L3b | 37.8 | 34.3 / 161.2 | 34.0 | 0.71 | 0.57 | 1.30 |
| L4 | 9.3 | 9.3 / 9.3 | 9.3 | 0.06 | 0.06 | 0.14 |
| L5 | 37.8 | 9.3 / 37.8 | 9.3 | 0.73 | 0.69 | 1.29 |
| L6 | 37.8 | 12.3 / 49.3 | 12.3 | 0.68 | 0.63 | 1.23 |
| L7 | 37.8 | 9.8 / 49.3 | 9.3 | 0.55 | 0.48 | 1.10 |
| L8 | 1.0 | 1.0 / 1.0 | 1.0 | 0.04 | 0.04 | 0.06 |

**Q2i: same, incident hours (3).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 45.7 | 10.0 / 45.7 | 10.0 | 0.86 | 0.81 | 1.54 |
| L1 | 45.7 | 10.0 / 45.7 | 10.0 | 0.88 | 0.83 | 1.56 |
| L2 | 1.0 | 1.0 / 1.0 | 1.0 | 0.16 | 0.16 | 0.17 |
| L3 | 45.7 | 16.0 / 60.7 | 16.0 | 0.90 | 0.83 | 1.56 |
| L3b | 45.7 | 46.0 / 202.0 | 46.0 | 0.89 | 0.71 | 1.58 |
| L4 | 10.0 | 10.0 / 10.0 | 10.0 | 0.17 | 0.17 | 0.26 |
| L5 | 45.7 | 10.0 / 45.7 | 10.0 | 0.84 | 0.79 | 1.52 |
| L6 | 45.7 | 16.0 / 60.7 | 16.0 | 0.87 | 0.80 | 1.53 |
| L7 | 45.7 | 13.7 / 60.7 | 13.0 | 0.77 | 0.67 | 1.42 |
| L8 | 1.0 | 4.0 / 4.0 | 4.0 | 0.18 | 0.18 | 0.20 |

**Q3: template filter, 24h-equivalent window (3).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 153.0 | 100.3 / 156.0 | 27.0 | 25.94 | 3.92 | 8.70 |
| L1 | 153.0 | 100.0 / 156.0 | 27.0 | 25.84 | 4.03 | 8.92 |
| L2 | 72.0 | 72.0 / 72.0 | 24.0 | 19.05 | 3.23 | 4.90 |
| L3 | 153.0 | 121.7 / 360.0 | 28.7 | 14.21 | 2.87 | 5.72 |
| L3b | 153.0 | 184.3 / 1238.0 | 32.3 | 7.71 | 1.22 | 4.35 |
| L4 | 720.0 | 318.7 / 720.0 | 79.7 | 15.03 | 3.17 |  |
| L5 | 153.0 | 100.3 / 156.0 | 27.0 | 25.40 | 3.90 | 8.67 |
| L6 | 153.0 | 121.3 / 360.0 | 28.3 | 14.02 | 2.82 | 5.69 |
| L7 | 153.0 | 172.7 / 360.0 | 38.3 | 19.87 | 3.85 | 6.98 |
| L8 | 72.0 | 91.3 / 169.0 | 24.7 | 10.94 | 2.51 | 3.77 |

**Q3u: template filter, all time (2).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 225.0 | 225.5 / 228.0 | 75.0 | 56.76 | 9.75 | 14.75 |
| L1 | 225.0 | 228.0 / 228.0 | 75.0 | 57.28 | 10.05 | 15.39 |
| L2 | 234.0 | 216.0 / 234.0 | 72.0 | 57.25 | 9.70 | 14.75 |
| L3 | 225.0 | 235.0 / 515.0 | 75.5 | 27.79 | 7.80 | 11.54 |
| L3b | 225.0 | 352.5 / 1764.0 | 79.5 | 14.27 | 3.16 | 8.53 |
| L4 | 2160.0 | 924.5 / 2160.0 | 211.0 | 38.76 | 9.35 |  |
| L5 | 225.0 | 225.5 / 228.0 | 75.0 | 55.36 | 9.69 | 14.68 |
| L6 | 225.0 | 235.0 / 515.0 | 75.5 | 27.37 | 7.77 | 11.52 |
| L7 | 225.0 | 405.5 / 515.0 | 102.0 | 45.94 | 10.08 | 14.67 |
| L8 | 234.0 | 232.5 / 523.0 | 72.0 | 28.47 | 7.85 | 11.57 |

**Q4: service + warn+ scan, 24h-equivalent window (3).** Means per query. `files` opened (footer read), `rg` read by min/max pruning of the rows groups in those files, `ideal` = row groups that really hold a matching row (the floor for any index), MB are model bytes (footers + needed column chunks), `http` = DuckDB over the counting range server (blank where skipped for port budget).

| layout | files | rg read / in files | ideal rg | model MB | ideal MB | http MB |
|---|---:|---:|---:|---:|---:|---:|
| L0 | 47.0 | 33.0 / 47.0 | 28.7 | 4.79 | 4.36 | 5.23 |
| L1 | 47.0 | 33.0 / 47.0 | 28.7 | 4.87 | 4.42 | 5.30 |
| L2 | 24.0 | 24.0 / 24.0 | 24.0 | 3.62 | 3.62 | 3.98 |
| L3 | 47.0 | 35.7 / 62.0 | 31.0 | 3.78 | 3.30 | 4.16 |
| L3b | 47.0 | 41.7 / 206.0 | 38.0 | 1.80 | 1.59 | 2.41 |
| L4 | 240.0 | 150.7 / 240.0 | 139.3 | 3.99 | 2.76 | 4.63 |
| L5 | 47.0 | 33.0 / 47.0 | 28.7 | 4.74 | 4.31 | 5.18 |
| L6 | 47.0 | 35.7 / 62.0 | 29.7 | 3.74 | 3.08 | 4.01 |
| L7 | 47.0 | 45.0 / 62.0 | 37.7 | 4.95 | 4.17 | 5.04 |
| L8 | 24.0 | 25.0 / 33.0 | 25.0 | 2.73 | 2.73 | 3.07 |

## Reading the tables

1. **Query bytes are set by partition key and row-group size, not by sort order.** At 12 h, event-hour partitioning with 8k
   row groups (L8) reads the least, or ties, on every shape except unbounded trace lookup (Q1a, where only 2k row groups
   do better): Q2 0.28 MB against 1.87 MB for today's layout (L0), Q1b 16.8 against 65.7, Q3 4.19 against 28.17, Q4 0.86
   against 7.35, Q3u 10.54 against 35.78. At 72 h, where a partition is smaller than one 64k row group and barely larger than an
   8k one, L8 still wins Q2 (0.04 against 0.74 MB) and Q1b (5.0 against 33.7), but 2k row groups beat 8k on Q3, Q3u and Q4
   (L3b: 7.71, 14.27, 1.80 MB; L3: 14.21, 27.79, 3.78; L8: 10.94, 28.47, 2.73): the row group has to be small *relative to the
   partition*.
2. **Row-group size is a clean win.** 8k rows costs +2.7% bytes at 12 h (+3.1% at 72 h) and cuts Q3 from 28.17 to 8.28 MB and
   Q4 from 7.35 to 2.03 MB (L3 versus L0). 2k rows (L3b) is +3.4% / +4.9% bytes, 2.5-3x the write time here and 4x the
   row groups; it roughly halves Q3/Q4/Q3u bytes again at 72 h and gains little at 12 h, and it multiplies http requests.
   8k is the conservative step; 4k and 2k should be re-measured on the arrow-rs writer when the constant is changed.
3. **Time-first sort (L1, L7) does not earn its cost.** At 64k it moves little (Q2 1.68 vs 1.87 MB, Q1b 58.0 vs 65.7, Q3 30.5 vs
   28.2, Q3u 44.2 vs 35.8) because a partition has one to four row groups. At 8k, against the same row groups sorted by
   service (L3/L6), time-first wins Q2 (0.74 vs 1.01-1.10 MB) and Q1b (20.3 vs 37.7-40.3 MB, traces are 30 s long) but loses
   Q3 by 2x (16.6 vs 7.7-8.3), Q4 by 2x (4.0 vs 1.9-2.0) and Q3u by 3x (35.8 vs 11.2-11.8), because template ids and services
   no longer cluster. The previous measurement that motivated the question (47.1% vs 12.3% of rows for an hour window) came
   from one huge replay partition; with a safe partition rule and partitions of this size the *files* opened, not the
   in-file row-group coverage, dominate Q2.
4. **Event time as the partition key removes the replay problem at the source.** Q2 opens 1 file (L2, L8) instead of
   9.3 (12 h) or 37.8 (72 h) files that all must be opened under the safe rule (only 8.0 / 9.3 of them hold data, which a
   catalog min/max-timestamp check would find without I/O). Two-level observed/event partitioning (L4) prunes similarly
   (Q2 8 files, 0.28 MB) but multiplies files: 360 vs 45 at 12 h and **2,160 vs 225 at 72 h** after compaction, median 134 rows
   per file at 72 h. Not viable.
5. **The price of event-time partitioning is write fan-out.** Files produced before compaction (one per segment and
   partition): 3,810 vs 546 at 12 h (7.0x) and 7,178 vs 726 at 72 h (9.9x). Without the replay rows at all, the skewed service
   alone gives 3,155 vs 489 (6.5x) and 3,289 vs 666 (4.9x). Files after compaction are about the same (54 vs 45; 234 vs 225), and
   bytes are +2.3% (12 h) / +3.7% (72 h) for L8 against L0 including the 8k row groups.
6. **Unbounded trace lookup (Q1a) is not fixed by any layout.** Every layout with 64k or 8k row groups reads 99-154 MB model
   bytes (2k: 50 MB at 12 h and 120 MB at 72 h) for traces of 3 to 15,000 rows, against an ideal of 27-44 MB: min/max on `trace_id` cannot prune a row group that spans several services,
   and at these partition sizes every row group does. The `(service, trace_id)` sort buys roughly a quarter at 8k row groups
   (L3 107 MB vs L6/L7 144/136 MB at 12 h) and the benefit disappears at 72 h partition sizes. The "trace range scan" the
   existing sort order was chosen for only exists when a service's share of a partition is much larger than a row group. With
   an event-time hint (Q1b) the picture is different and good: 16.8 MB for L8 against 65.7 MB for L0.
7. **Chosen layout meets the replay half of the Plane 16 criterion and half of the trace half.** Replayed data prunes (Q2 above;
   recall is 1.0 by construction), trace lookups with a time hint prune (item 6), unbounded trace lookups do not prune on any
   layout measured.

