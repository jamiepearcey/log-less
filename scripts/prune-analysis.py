#!/usr/bin/env python3
"""How much of the store an event-time query has to read.

Row-group statistics can only prune what the sort order has clustered. The store
sorts `(service, trace_id, observed)`, so `timestamp` is scattered across row
groups and its min/max bounds overlap heavily — which makes them nearly useless
for pruning an event-time window.

This measures that directly: per-row-group time coverage, and the fraction of
rows a window of a given width actually has to read, swept across the range so
the answer is not an artefact of where the window lands.

Compare a store against a time-sorted copy of the same data to separate the two
causes — sort order and row-group size:

    scripts/prune-analysis.py /path/to/data/store /path/to/time-sorted.parquet

Written up in docs/platform-thesis.md §15 and docs/architecture.md §2.
"""
import statistics
import sys

import duckdb

if len(sys.argv) < 2:
    sys.exit("usage: prune-analysis.py <parquet-dir-or-file> [more...]")

WINDOWS = [("1 hour", 3600), ("1 day", 86400)]
# Sweep positions across the full range. A single window placement would measure
# where it happened to land rather than the layout.
TRIALS = 24

con = duckdb.connect()

for target in sys.argv[1:]:
    glob = target if target.endswith(".parquet") else f"{target}/**/*.parquet"
    rows = con.execute(f"""
        SELECT row_group_num_rows,
               epoch(stats_min_value::TIMESTAMP),
               epoch(stats_max_value::TIMESTAMP)
        FROM parquet_metadata('{glob}')
        WHERE path_in_schema = 'timestamp'
    """).fetchall()

    if not rows:
        print(f"{target}: no timestamp statistics — pruning is impossible")
        continue

    total = sum(r[0] for r in rows)
    low, high = min(r[1] for r in rows), max(r[2] for r in rows)
    span = high - low
    if span <= 0:
        print(f"{target}: all rows share one instant; nothing to prune")
        continue

    coverage = sorted((r[2] - r[1]) / span for r in rows)
    print(f"\n{target}")
    print(f"  row groups {len(rows)}, {total // len(rows)} rows each, "
          f"{span / 86400:.1f} days spanned")
    print(f"  per-group coverage of the full range: median {statistics.median(coverage):.3f}"
          f"  mean {statistics.mean(coverage):.3f}  max {coverage[-1]:.3f}")

    for label, width in WINDOWS:
        read_rows = read_groups = 0
        for i in range(TRIALS):
            start = low + span * i / TRIALS
            end = start + width
            hits = [r for r in rows if not (r[2] < start or r[1] > end)]
            read_groups += len(hits)
            read_rows += sum(r[0] for r in hits)
        print(f"  {label:<7} window reads {100 * read_groups / (TRIALS * len(rows)):5.1f}% "
              f"of row groups, {100 * read_rows / (TRIALS * total):5.1f}% of rows")
