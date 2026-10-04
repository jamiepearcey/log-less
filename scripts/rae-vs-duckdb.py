#!/usr/bin/env python3
"""RAE vs DuckDB on the log-analytics query shapes from platform-thesis.md §3.

The question this answers is §11's: is RAE competitive on *this* workload —
time-bounded scan and low-cardinality GROUP BY over immutable parts — as opposed
to the general-purpose benchmarks where B-403 measured it losing 2-10x.

Three configurations, because two would be misleading:
  RAE(parts)        RAE over its own format. What owning the engine buys.
  DuckDB(parquet)   DuckDB over the same Parquet. The thesis's storage format,
                    and what "point your own DuckDB at it" actually means today.
  DuckDB(native)    DuckDB over its own table. DuckDB at its best, which is what
                    a "just use DuckDB for the central tier" alternative gets.

Both engines report their fastest of N runs: RAE via `--repeat`, DuckDB via the
minimum of the same count. Fastest-of-N is the right statistic for comparing
engines on a busy laptop — it is the one least polluted by other processes.

RAE's number excludes process startup; DuckDB's excludes connection setup.
Results are compared for equality before any timing is reported, because a fast
wrong answer is not a data point.
"""
import re
import subprocess
import sys
import time

import duckdb

import os

RAE = os.environ.get("RAE_BIN", "rae")
REPEAT = int(os.environ.get("REPEAT", "5"))
WORKERS = int(os.environ.get("WORKERS", "14"))

if len(sys.argv) != 5:
    sys.exit(
        "usage: rae-vs-duckdb.py <work-dir> <scale-label> <rae-part-dir> <parquet>\n"
        "\n"
        "  Build the inputs first:\n"
        "    scripts/spike-console.sh                       # the log-less store\n"
        "    # project it to RAE-compatible columns, UNCOMPRESSED (RAE's loader\n"
        "    # reads neither zstd nor snappy), then:\n"
        "    raebench-clickbench-load logs_raw.parquet --out ./rae-parts\n"
        "\n"
        "  RAE_BIN, REPEAT and WORKERS override the defaults."
    )

sp, scale, part_dir, parquet = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

# (name, sql, rae_params, duckdb_substitutions)
#
# RAE has no timestamp literal, so the window is bound as a typed parameter.
# DuckDB gets the identical instants as literals. Same predicate, two dialects.
LO, HI = "2026-08-06T00:00:00Z", "2026-08-08T00:00:00Z"
QUERIES = [
    (
        "spine (GROUP BY service, template_id)",
        "SELECT service, template_id, count(*) AS events FROM logs GROUP BY service, template_id",
        {},
    ),
    (
        "scan+filter (severity >= 17)",
        "SELECT count(*) AS n FROM logs WHERE severity >= 17",
        {},
    ),
    (
        "top-k templates",
        "SELECT template_id, count(*) AS n FROM logs GROUP BY template_id ORDER BY n DESC LIMIT 10",
        {},
    ),
    (
        "level filter + group",
        "SELECT service, count(*) AS n FROM logs WHERE level = 'warn_plus' GROUP BY service",
        {},
    ),
    (
        "time-window spine",
        "SELECT service, template_id, count(*) AS events FROM logs "
        "WHERE timestamp >= :lo AND timestamp < :hi GROUP BY service, template_id",
        {"lo": LO, "hi": HI},
    ),
    (
        "time-window count",
        "SELECT count(*) AS n FROM logs WHERE timestamp >= :lo AND timestamp < :hi",
        {"lo": LO, "hi": HI},
    ),
]


def for_duckdb(sql):
    return (sql.replace(":lo", f"TIMESTAMP '{LO[:-1].replace('T', ' ')}'")
               .replace(":hi", f"TIMESTAMP '{HI[:-1].replace('T', ' ')}'"))


def run_rae(sql, params):
    cmd = [RAE, "-t", f"logs={part_dir}", "-w", str(WORKERS),
           "--repeat", str(REPEAT), "--limit", "0"]
    for key, value in params.items():
        cmd += ["-p", f"{key}={value}"]
    cmd.append(sql)
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        return None, None, out.stderr.strip()[:200]
    text = out.stdout + out.stderr
    micros = re.search(r"([\d.]+)\s*us\b", text)
    if not micros:
        seconds = re.search(r"in ([\d.]+)s", text)
        if not seconds:
            return None, None, "no timing in output"
        micros_value = float(seconds.group(1)) * 1e6
    else:
        micros_value = float(micros.group(1))
    rows = re.search(r"(\d+) rows? in", text)
    return micros_value / 1000.0, int(rows.group(1)) if rows else None, None


def run_duckdb(con, sql):
    best = None
    rows = None
    for _ in range(REPEAT):
        started = time.perf_counter()
        result = con.execute(sql).fetchall()
        elapsed = (time.perf_counter() - started) * 1000.0
        best = elapsed if best is None else min(best, elapsed)
        rows = len(result)
    return best, rows


parquet_con = duckdb.connect()
parquet_con.execute(f"SET threads={WORKERS}")
parquet_con.execute(f"CREATE VIEW logs AS SELECT * FROM read_parquet('{parquet}')")

native_con = duckdb.connect(f"{sp}/duck_{scale}.db")
native_con.execute(f"SET threads={WORKERS}")
native_con.execute(
    f"CREATE TABLE IF NOT EXISTS logs AS SELECT * FROM read_parquet('{parquet}')")

print(f"\n=== {scale}: RAE parts vs DuckDB, fastest of {REPEAT}, {WORKERS} workers ===\n")
print(f"{'query':<40}{'RAE ms':>10}{'DuckDB/pq':>12}{'DuckDB/nat':>12}"
      f"{'vs pq':>9}{'vs nat':>9}  rows")
print("-" * 105)

for name, sql, params in QUERIES:
    rae_ms, rae_rows, err = run_rae(sql, params)
    duck_sql = for_duckdb(sql)
    pq_ms, pq_rows = run_duckdb(parquet_con, duck_sql)
    nat_ms, nat_rows = run_duckdb(native_con, duck_sql)

    if err:
        print(f"{name:<40}{'ERROR':>10}  {err}")
        continue

    agree = "ok" if rae_rows == pq_rows == nat_rows else \
        f"MISMATCH rae={rae_rows} pq={pq_rows} nat={nat_rows}"
    print(f"{name:<40}{rae_ms:>10.2f}{pq_ms:>12.2f}{nat_ms:>12.2f}"
          f"{pq_ms / rae_ms:>8.2f}x{nat_ms / rae_ms:>8.2f}x  {agree}")

print("\n'vs' columns are DuckDB time / RAE time — above 1.00x means RAE is faster.\n")
