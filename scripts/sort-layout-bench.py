#!/usr/bin/env python3
"""Sort order / partitioning benchmark for the log-less store (Plane 73).

Writes the same synthetic log corpus into several candidate Hive-partitioned
Parquet layouts, then measures, per query shape, how much each layout makes a
reader open and read. Counts come first:

  * files opened      files that survive partition pruning (a footer read each)
  * row groups read   row groups that survive min/max statistics pruning
  * bytes (model)     footers + the column chunks the query needs, in the
                      surviving row groups, from Parquet metadata
  * bytes (HTTP)      what DuckDB httpfs actually pulled from a counting range
                      server (the same measure as the S3-serve byte counter);

Corpus: several services, 360 template ids with Zipf frequencies, trace ids
(short traces, long-running traces, and six incident bursts that put 10k+ rows
on one trace and one template inside ten minutes), a clock-skewed service, and a
replay of event-time history ingested in one burst. The latter is what makes
`observed` (the partition key) and `timestamp` (what users query) diverge.

Everything is derived from hash(), so a run is reproducible for a given
--rows/--replay-frac/--seg-rows regardless of thread count.

Usage:
    scripts/sort-layout-bench.py --out ~/tmp/logless-sort-bench [--rows 3000000]
        [--replay-frac 0.12] [--layouts L0,L1,...] [--json result.json]
    scripts/sort-layout-bench.py --quick        # 150k rows, smoke test

Fidelity caveats are in docs/evidence/sort-layout-bench.md: files are written
with DuckDB's Parquet writer (zstd 3, dictionary) rather than the store's
arrow-rs writer, so absolute sizes differ; layout-versus-layout ratios are the
point. DuckDB writes bloom filters by default; every layout sets the false-positive
ratio to 1.0 so none is useful. Bloom filters are NOT measured: DuckDB 1.5.2 returns
wrong rows for BLOB equality on its own files, so trace predicates use hex(trace_id).
"""
import argparse
import http.server
import json
import math
import os
import shutil
import socketserver
import threading
import time
from collections import defaultdict
from urllib.parse import quote, unquote

import duckdb

T0_NS = 1790812800 * 10**9  # 2026-10-01T00:00:00Z
HOUR_NS = 3600 * 10**9
LIVE_HOURS = 72
SERVICES = ["api-gw", "auth", "billing", "search", "orders", "inventory", "notify",
            "payments", "ml-serve", "ingest", "cron", "edge-gw"]
SVC_W = [.20, .13, .11, .10, .09, .08, .07, .06, .05, .04, .03, .04]
EDGE = 11  # the clock-skewed service
PATTERNS = [  # (level, text); list order = Zipf rank within a service
    ("info", "request {a} completed in {b}ms status=200"),
    ("info", "cache hit key={a} ttl={b}"),
    ("debug", "span {a} attributes flushed count={b}"),
    ("info", "user u{c} session {a} refreshed"),
    ("info", "queue depth {b} on shard {c}"),
    ("debug", "gc pause {b}ms generation={c}"),
    ("info", "request {a} routed to upstream-{c}"),
    ("warn", "slow query {a} took {b}ms threshold=500"),
    ("info", "heartbeat from node-{c} seq={b}"),
    ("debug", "config reload checksum={a}"),
    ("info", "job {a} scheduled for tenant t{c}"),
    ("error", "upstream timeout after {b}ms target=svc-{c} req={a}"),
    ("info", "token issued sub=u{c} exp={b}"),
    ("warn", "retry {b} for request {a}"),
    ("debug", "connection pool size={b} idle={c}"),
    ("info", "batch {a} committed rows={b}"),
    ("error", "failed to write batch {a}: disk quota exceeded ({b} bytes)"),
    ("info", "feature flag f{c} evaluated={b}"),
    ("warn", "queue lag {b}s on partition {c}"),
    ("debug", "tls handshake {a} took {b}us"),
    ("info", "metrics scraped series={b}"),
    ("error", "unhandled exception in handler {a}: code {b}"),
    ("info", "snapshot {a} written size={b}"),
    ("warn", "certificate for host-{c} expires in {b}d"),
    ("debug", "lease renewed owner={a}"),
    ("info", "webhook {a} delivered attempt={b}"),
    ("error", "checksum mismatch on block {a} expected={b}"),
    ("info", "audit event {a} recorded actor=u{c}"),
    ("warn", "rate limit reached tenant t{c} rps={b}"),
    ("debug", "dns lookup {a} resolved in {b}ms"),
]
INCIDENT_FRACS = [0.11, 0.26, 0.42, 0.57, 0.72, 0.88]
INCIDENT_HOURS = []
INCIDENT_RANK = 11  # an error template


# ------------------------------------------------------------------ layouts
class Layout:
    def __init__(self, name, desc, parts, order, rg=65536, bloom=False):
        self.name, self.desc, self.parts, self.order = name, desc, parts, order
        self.rg, self.bloom = rg, bloom


BASE_ORDER = "service, trace_id NULLS FIRST, observed, event_id"
TIME_FIRST = "timestamp, service, trace_id NULLS FIRST, event_id"
SVC_TIME = "service, timestamp, trace_id NULLS FIRST, event_id"
LAYOUTS = [
    Layout("L0", "baseline: observed-hour/level, sort (service,trace_id,observed), 64k row groups",
           ["hour", "level"], BASE_ORDER),
    Layout("L1", "time-first sort (timestamp,service,trace_id), observed-hour/level, 64k",
           ["hour", "level"], TIME_FIRST),
    Layout("L2", "event-time partition: timestamp-hour/level, baseline sort, 64k",
           ["hour", "level"], BASE_ORDER),
    Layout("L3", "baseline layout with 8k-row row groups",
           ["hour", "level"], BASE_ORDER, rg=8192),
    Layout("L3b", "baseline layout with 2k-row row groups",
           ["hour", "level"], BASE_ORDER, rg=2048),
    Layout("L4", "two-level: observed-hour/event-hour/level, baseline sort, 64k",
           ["hour", "evhour", "level"], BASE_ORDER),
    Layout("L5", "service-then-time sort (service,timestamp,trace_id), observed-hour/level, 64k",
           ["hour", "level"], SVC_TIME),
    Layout("L6", "service-then-time sort, 8k-row row groups, observed-hour/level",
           ["hour", "level"], SVC_TIME, rg=8192),
    Layout("L7", "time-first sort, 8k-row row groups, observed-hour/level",
           ["hour", "level"], TIME_FIRST, rg=8192),
    Layout("L8", "event-time partition, baseline sort, 8k-row row groups",
           ["hour", "level"], BASE_ORDER, rg=8192),
]
EVENT_HOUR_LAYOUTS = {"L2", "L8"}  # `hour` is cut on the event timestamp, not `observed`


# ------------------------------------------------------------------ corpus
def configure(hours):
    """Live window length. Fewer hours at the same row count = heavier
    partitions, which is the lever that decides whether sort order can matter."""
    global LIVE_HOURS, INCIDENT_HOURS
    LIVE_HOURS = hours
    INCIDENT_HOURS = [min(hours - 1, int(hours * f)) for f in INCIDENT_FRACS]


def build_corpus(con, n_live, replay_frac, seg_rows):
    n_replay = int(n_live * replay_frac)
    n = n_live + n_replay
    con.execute("CREATE MACRO u(i, k) AS (hash(i * 131 + k) % 1000000007)::DOUBLE / 1000000007.0")
    w = [1 + 0.5 * math.sin(2 * math.pi * (h - 6) / 24) for h in range(LIVE_HOURS)]
    tot, acc, rows = sum(w), 0.0, []
    for h, x in enumerate(w):
        rows.append((h, acc / tot, (acc + x) / tot))
        acc += x
    rows[-1] = (rows[-1][0], rows[-1][1], 2.0)
    con.execute("CREATE TABLE hw(h INT, c0 DOUBLE, c1 DOUBLE)")
    con.executemany("INSERT INTO hw VALUES (?,?,?)", rows)
    acc, srows = 0.0, []
    for i, x in enumerate(SVC_W):
        srows.append((i, SERVICES[i], acc, acc + x))
        acc += x
    srows[-1] = (srows[-1][0], srows[-1][1], srows[-1][2], 2.0)
    con.execute("CREATE TABLE svc(s INT, name VARCHAR, c0 DOUBLE, c1 DOUBLE)")
    con.executemany("INSERT INTO svc VALUES (?,?,?,?)", srows)
    zw = [1 / (k + 1) ** 1.1 for k in range(len(PATTERNS))]
    ztot, acc, trows = sum(zw), 0.0, []
    for k, (lvl, text) in enumerate(PATTERNS):
        trows.append((k, lvl, text, acc / ztot, (acc + zw[k]) / ztot))
        acc += zw[k]
    trows[-1] = trows[-1][:4] + (2.0,)
    con.execute("CREATE TABLE pat(k INT, lvl VARCHAR, text VARCHAR, c0 DOUBLE, c1 DOUBLE)")
    con.executemany("INSERT INTO pat VALUES (?,?,?,?,?)", trows)
    con.execute("CREATE TABLE incident(k INT, hr INT)")
    con.executemany("INSERT INTO incident VALUES (?,?)", list(enumerate(INCIDENT_HOURS)))

    # one row per record: kind, event time, observed time, service, pattern rank
    con.execute(f"""
    CREATE TABLE raw AS
    WITH base AS (
      SELECT i,
        CASE WHEN i >= {n_live} THEN 'replay' WHEN u(i,3) < 0.05 THEN 'incident' ELSE 'live' END AS kind,
        u(i,1) u1, u(i,2) u2, u(i,4) u4, u(i,5) u5, u(i,6) u6, u(i,17) u17, u(i,18) u18,
        (hash(i*131+19) % {len(INCIDENT_HOURS)})::INT AS ik
      FROM range({n}) t(i)
    ), withhour AS (
      SELECT b.*, hw.h AS h FROM base b JOIN hw ON b.u1 >= hw.c0 AND b.u1 < hw.c1
    ), withsvc AS (
      SELECT w.*, CASE WHEN kind = 'incident' THEN (2 * ik + 1) % 12 ELSE s.s END AS sidx
      FROM withhour w JOIN svc s ON w.u17 >= s.c0 AND w.u17 < s.c1
    ), withpat AS (
      SELECT w.*, CASE WHEN kind = 'incident' THEN {INCIDENT_RANK} ELSE p.k END AS pk
      FROM withsvc w JOIN pat p ON w.u18 >= p.c0 AND w.u18 < p.c1
    ), t AS (
      SELECT w.*,
        CASE WHEN kind = 'incident'
             THEN inc.hr * {HOUR_NS} + 20 * 60 * 1000000000::BIGINT + (u2 * 600 * 1000000000)::BIGINT
             ELSE h * {HOUR_NS} + (u2 * {HOUR_NS})::BIGINT END AS ev0
      FROM withpat w LEFT JOIN incident inc ON inc.k = w.ik
    )
    SELECT i, kind, sidx, pk, ik,
      {T0_NS}::BIGINT + CASE WHEN kind = 'live' AND sidx = {EDGE}
                             THEN ev0 - ({HOUR_NS} + (u5 * 5 * {HOUR_NS})::BIGINT) ELSE ev0 END AS ev_ns,
      {T0_NS}::BIGINT + CASE WHEN kind = 'replay' THEN {LIVE_HOURS} * {HOUR_NS} + (u4 * 3 * {HOUR_NS})::BIGINT
                             ELSE ev0 + (u6 * 2000000000)::BIGINT END AS obs_ns
    FROM t
    """)
    pat_lvl = "CASE pk " + " ".join(f"WHEN {k} THEN '{l}'" for k, (l, _) in enumerate(PATTERNS)) + " END"
    pat_txt = "CASE pk " + " ".join(f"WHEN {k} THEN '{t}'" for k, (_, t) in enumerate(PATTERNS)) + " END"
    con.execute(f"""
    CREATE TABLE logs0 AS
    WITH r AS (
      SELECT raw.*, s.name AS service, {pat_lvl} AS lvl, {pat_txt} AS txt,
             (sidx * 30 + pk)::UBIGINT AS tid,
             CASE WHEN kind = 'incident' AND u(i,10) < 0.6 THEN 2
                  WHEN u(i,20) < 0.03 THEN 1
                  WHEN u(i,20) < 0.38 THEN 3 END AS tkind,
             hash(i*131+11) % 20 AS longk,
             (ev_ns // 30000000000) * 64 + hash(i*131+12) % 40 AS tslot
      FROM raw JOIN svc s ON s.s = raw.sidx
    )
    SELECT
      unhex(lpad(to_hex((obs_ns // 1000000)::UBIGINT), 12, '0')
            || substr(lpad(to_hex(hash(i*131+14)), 16, '0'), 1, 16)
            || substr(lpad(to_hex(hash(i*131+15)), 16, '0'), 1, 4)) AS event_id,
      make_timestamp_ns(ev_ns) AS timestamp,
      make_timestamp_ns(obs_ns) AS observed,
      (CASE lvl WHEN 'debug' THEN 5 WHEN 'info' THEN 9 WHEN 'warn' THEN 13 ELSE 17 END)::UTINYINT AS severity,
      service || ': ' ||
        replace(replace(replace(txt, '{{a}}', to_hex(hash(i*131+7) % 1048576)),
                '{{b}}', (hash(i*131+8) % 2000)::VARCHAR), '{{c}}', (hash(i*131+9) % 100000)::VARCHAR) AS body,
      service,
      CASE tkind
        WHEN 2 THEN unhex(lpad(to_hex(hash(2, ik)), 16, '0') || lpad(to_hex(hash(ik, 2, 7)), 16, '0'))
        WHEN 1 THEN unhex(lpad(to_hex(hash(1, longk)), 16, '0') || lpad(to_hex(hash(longk, 1, 7)), 16, '0'))
        WHEN 3 THEN unhex(lpad(to_hex(hash(3, tslot)), 16, '0') || lpad(to_hex(hash(tslot, 3, 7)), 16, '0'))
        END AS trace_id,
      CASE WHEN tkind IS NOT NULL THEN unhex(lpad(to_hex(hash(i*131+13)), 16, '0')) END AS span_id,
      tid AS template_id,
      '{{"host":"h-' || (hash(i*131+16) % 40)::VARCHAR || '","region":"'
        || (['eu','us','ap'])[1 + (hash(i*131+21) % 3)::INT] || '"}}' AS attributes,
      CASE lvl WHEN 'debug' THEN 'debug' WHEN 'info' THEN 'info' ELSE 'warn_plus' END AS level,
      kind, ev_ns, obs_ns
    FROM r
    """)
    con.execute(f"""
    CREATE TABLE logs AS
    SELECT *, strftime(observed, '%Y-%m-%dT%H') AS obs_hour, strftime(timestamp, '%Y-%m-%dT%H') AS ev_hour,
           (row_number() OVER (ORDER BY observed, event_id) - 1) // {seg_rows} AS seg
    FROM logs0
    """)
    con.execute("DROP TABLE logs0")
    con.execute("DROP TABLE raw")


DATA_COLS = ["event_id", "timestamp", "observed", "severity", "body", "service",
             "trace_id", "span_id", "template_id", "attributes"]


# ------------------------------------------------------------------ writing
def hour_col(layout):
    return "ev_hour" if layout.name in EVENT_HOUR_LAYOUTS else "obs_hour"


def part_expr(layout):
    out = []
    for p in layout.parts:
        if p == "hour":
            out.append(f"{hour_col(layout)} AS hour")
        elif p == "evhour":
            out.append("ev_hour AS evhour")
        else:
            out.append("level")
    return out


def write_layout(con, layout, root):
    out = os.path.join(root, layout.name)
    if os.path.exists(out):
        shutil.rmtree(out)
    cols = ", ".join(DATA_COLS)
    con.execute("SET threads=1")  # one file per partition, sort order respected
    t = time.time()
    con.execute(f"""
      COPY (SELECT {cols}, {', '.join(part_expr(layout))} FROM logs
            ORDER BY {', '.join(layout.parts)}, {layout.order})
      TO '{out}' (FORMAT parquet, PARTITION_BY ({', '.join(layout.parts)}), COMPRESSION zstd,
                  COMPRESSION_LEVEL 3, ROW_GROUP_SIZE {layout.rg},
                  BLOOM_FILTER_FALSE_POSITIVE_RATIO 1.0,
                  FILENAME_PATTERN 'f{{i}}', OVERWRITE_OR_IGNORE)
    """)
    wall = time.time() - t
    con.execute("SET threads=4")
    return out, wall


def segment_fanout(con, layout):
    """Exact count of files a live pipeline creates before compaction: one per
    (WAL segment, partition) pair, with segments formed in arrival order."""
    keys = [hour_col(layout)] + (["ev_hour"] if "evhour" in layout.parts else []) + ["level"]
    k = ", ".join(keys)
    (n,) = con.execute(f"SELECT count(*) FROM (SELECT DISTINCT seg, {k} FROM logs)").fetchone()
    (nseg,) = con.execute("SELECT count(DISTINCT seg) FROM logs").fetchone()
    return n, nseg


# ------------------------------------------------------------------ metadata
def load_metadata(con, out):
    glob = f"{out}/**/*.parquet"
    files = {}
    for r in con.execute(f"SELECT file_name, num_rows, num_row_groups, file_size_bytes, footer_size "
                         f"FROM parquet_file_metadata('{glob}')").fetchall():
        files[r[0]] = dict(rows=r[1], rgs=r[2], size=r[3], footer=r[4])
    rgs = {}
    q = f"""
      SELECT file_name, row_group_id, any_value(row_group_num_rows),
        min(epoch_ns(TRY_CAST(stats_min_value AS TIMESTAMP_NS))) FILTER (WHERE path_in_schema='timestamp'),
        max(epoch_ns(TRY_CAST(stats_max_value AS TIMESTAMP_NS))) FILTER (WHERE path_in_schema='timestamp'),
        min(stats_min_value) FILTER (WHERE path_in_schema='service'),
        max(stats_max_value) FILTER (WHERE path_in_schema='service'),
        min(hex(TRY_CAST(stats_min_value AS BLOB))) FILTER (WHERE path_in_schema='trace_id'),
        max(hex(TRY_CAST(stats_max_value AS BLOB))) FILTER (WHERE path_in_schema='trace_id'),
        min(TRY_CAST(stats_min_value AS BIGINT)) FILTER (WHERE path_in_schema='template_id'),
        max(TRY_CAST(stats_max_value AS BIGINT)) FILTER (WHERE path_in_schema='template_id')
      FROM parquet_metadata('{glob}') GROUP BY file_name, row_group_id
    """
    for r in con.execute(q).fetchall():
        rgs[(r[0], r[1])] = dict(file=r[0], rows=r[2], tmin=r[3], tmax=r[4], smin=r[5], smax=r[6],
                                 trmin=r[7], trmax=r[8], tpmin=r[9], tpmax=r[10], cols={})
    for f, g, c, sz, bf in con.execute(
            f"SELECT file_name, row_group_id, path_in_schema, total_compressed_size, bloom_filter_length "
            f"FROM parquet_metadata('{glob}')").fetchall():
        rgs[(f, g)]["cols"][c] = sz + (bf or 0)
    return files, rgs


def oracle_rgs(con, opened, q, files, by_file):
    """(file, row_group_id) pairs that contain at least one matching row."""
    if not opened:
        return set()
    lst = ", ".join(f"'{f}'" for f in opened)
    rows = con.execute(f"SELECT DISTINCT filename, file_row_number // 1 FROM read_parquet([{lst}], "
                       f"filename=true, file_row_number=true) WHERE {q.sql_where()}").fetchall()
    starts = {}
    for f in opened:
        acc, st = 0, []
        for rg in by_file[f]:
            st.append((acc, rg["id"]))
            acc += rg["rows"]
        starts[f] = st
    import bisect
    out = set()
    for f, n in rows:
        st = starts[f]
        k = bisect.bisect_right([a for a, _ in st], n) - 1
        out.add((f, st[k][1]))
    return out


def hive(path):
    d = {}
    for seg in path.split(os.sep):
        if "=" in seg:
            k, v = seg.split("=", 1)
            d[k] = v
    return d


# ------------------------------------------------------------------ queries
def iso(ns_abs):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ns_abs // 10**9))


def hour_str(ns_abs):
    return time.strftime("%Y-%m-%dT%H", time.gmtime(ns_abs // 10**9))


class Q:
    """A query: predicates + projection. Event-time predicates are on `timestamp`."""

    def __init__(self, kind, label, proj, tlo=None, thi=None, service=None, level=None,
                 trace=None, template=None):
        self.kind, self.label, self.proj = kind, label, proj
        self.tlo, self.thi = tlo, thi  # absolute ns, [tlo, thi)
        self.service, self.level, self.trace, self.template = service, level, trace, template

    def pred_cols(self):
        c = set()
        if self.tlo is not None: c.add("timestamp")
        if self.service: c.add("service")
        if self.trace: c.add("trace_id")
        if self.template is not None: c.add("template_id")
        return c

    def sql_where(self, with_level=False):
        w = []
        if self.tlo is not None:
            w.append(f"timestamp >= TIMESTAMP_NS '{iso(self.tlo)}' AND timestamp < TIMESTAMP_NS '{iso(self.thi)}'")
        if self.service: w.append(f"service = '{self.service}'")
        if self.trace: w.append(f"hex(trace_id) = '{self.trace}'")
        if self.template is not None: w.append(f"template_id = {self.template}")
        if with_level and self.level: w.append(f"level = '{self.level}'")
        return " AND ".join(w) or "true"

    def partition_ok(self, layout, hv):
        """Safe partition pruning for an event-time window. Under observed-hour
        partitioning observed >= timestamp, but replayed data lands in later
        partitions, so the only safe bound is hour >= hour(tlo)."""
        if self.level and hv.get("level") != self.level:
            return False
        if self.tlo is None:
            return True
        lo, hi = hour_str(self.tlo), hour_str(self.thi - 1)
        if layout.name in EVENT_HOUR_LAYOUTS:
            return lo <= hv["hour"] <= hi
        if "evhour" in layout.parts:
            return lo <= hv["evhour"] <= hi
        return hv["hour"] >= lo

    def naive_partition_ok(self, hv):
        """What a reader gets if it assumes observed ~= timestamp."""
        if self.level and hv.get("level") != self.level:
            return False
        if self.tlo is None:
            return True
        return hour_str(self.tlo) <= hv["hour"] <= hour_str(self.thi - 1)

    def rg_ok(self, rg):
        """Min/max pruning. A column chunk with no statistics (all null) cannot
        match an equality predicate; a missing time range cannot match a window."""
        if self.tlo is not None and (rg["tmin"] is None or rg["tmax"] < self.tlo or rg["tmin"] >= self.thi):
            return False
        if self.service and (rg["smin"] is None or not (rg["smin"] <= self.service <= rg["smax"])):
            return False
        if self.trace and (rg["trmin"] is None or not (rg["trmin"] <= self.trace.upper() <= rg["trmax"])):
            return False
        if self.template is not None and (rg["tpmin"] is None or not (rg["tpmin"] <= self.template <= rg["tpmax"])):
            return False
        return True


HTTP_MAX_REQUESTS = 2500  # est. GETs; beyond this the loopback port budget runs out
ALL = DATA_COLS


def pick_queries(con):
    qs = []
    inc = [r[0] for r in con.execute("""
        SELECT hex(trace_id) FROM logs WHERE kind='incident' AND trace_id IS NOT NULL
        GROUP BY trace_id HAVING count(*) > 1000 ORDER BY count(*) DESC, 1 LIMIT 3""").fetchall()]
    long = [r[0] for r in con.execute("""
        SELECT hex(trace_id) FROM logs WHERE kind != 'incident' AND trace_id IS NOT NULL
        GROUP BY trace_id HAVING count(*) > 1000 AND count(*) < 100000 ORDER BY hash(trace_id) LIMIT 3""").fetchall()]
    norm = con.execute("""
        SELECT hex(trace_id), min(ev_ns), max(ev_ns) FROM logs
        WHERE kind = 'live' AND trace_id IS NOT NULL
        GROUP BY trace_id HAVING count(*) BETWEEN 3 AND 40 ORDER BY hash(trace_id) LIMIT 6""").fetchall()
    for tr in inc: qs.append(Q("Q1a", f"trace {tr[:8]} (incident)", ALL, trace=tr))
    for tr in long: qs.append(Q("Q1a", f"trace {tr[:8]} (long-running)", ALL, trace=tr))
    for tr, lo, hi in norm: qs.append(Q("Q1a", f"trace {tr[:8]} (ordinary)", ALL, trace=tr))
    for tr, lo, hi in norm:
        qs.append(Q("Q1b", f"trace {tr[:8]} +-30m hint", ALL, trace=tr,
                    tlo=(lo // 10**9 - 1800) * 10**9, thi=(hi // 10**9 + 1801) * 10**9))
    issue_cols = ["timestamp", "service", "template_id", "severity"]
    for f in [0.04, 0.2, 0.46, 0.65, 0.83, 0.97]:
        h = min(LIVE_HOURS - 1, int(LIVE_HOURS * f))
        lo = T0_NS + h * HOUR_NS
        qs.append(Q("Q2", f"issues hour+{h}", issue_cols, tlo=lo, thi=lo + HOUR_NS, level="warn_plus"))
    for h in [INCIDENT_HOURS[0], INCIDENT_HOURS[2], INCIDENT_HOURS[4]]:
        lo = T0_NS + h * HOUR_NS
        qs.append(Q("Q2i", f"issues hour+{h} (incident)", issue_cols, tlo=lo, thi=lo + HOUR_NS, level="warn_plus"))
    common = con.execute("""SELECT template_id FROM logs WHERE level='warn_plus' AND kind='live'
                            GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 1 OFFSET 2""").fetchone()[0]
    mid = con.execute("""SELECT template_id FROM logs WHERE kind='live' GROUP BY 1
                         ORDER BY count(*) DESC, 1 LIMIT 1 OFFSET 60""").fetchone()[0]
    rare = con.execute("""SELECT template_id FROM logs WHERE kind='live' GROUP BY 1
                          ORDER BY count(*) ASC, 1 LIMIT 1""").fetchone()[0]
    win = max(1, LIVE_HOURS // 3)  # 24h at the default 72h window
    for name, tid in [("common warn+", common), ("mid", mid), ("rare", rare)]:
        lo = T0_NS + win * HOUR_NS
        qs.append(Q("Q3", f"template {tid} ({name}) {win}h", ["timestamp", "service", "body"],
                    tlo=lo, thi=lo + win * HOUR_NS, template=tid))
    for name, tid in [("common warn+", common), ("rare", rare)]:
        qs.append(Q("Q3u", f"template {tid} ({name}) all time", ["timestamp", "service", "body"], template=tid))
    for svc in ["api-gw", "inventory", "edge-gw"]:
        lo = T0_NS + int(LIVE_HOURS * 0.4) * HOUR_NS
        qs.append(Q("Q4", f"{svc} warn_plus {win}h", ["timestamp", "severity", "template_id", "body"],
                    tlo=lo, thi=lo + win * HOUR_NS, service=svc, level="warn_plus"))
    return qs


# ------------------------------------------------------------------ http server
class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        with self.lock:
            self.gets = self.heads = self.bytes = 0
            self.paths = set()

    def snap(self):
        with self.lock:
            return dict(gets=self.gets, heads=self.heads, bytes=self.bytes, files=len(self.paths))


def serve(root, stats):
    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _file(self):
            p = os.path.join(root, unquote(self.path.split("?")[0]).lstrip("/"))
            return p if os.path.isfile(p) else None

        def do_HEAD(self):
            p = self._file()
            if not p:
                self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
            with stats.lock:
                stats.heads += 1
                stats.paths.add(p)
            self.send_response(200)
            self.send_header("Content-Length", str(os.path.getsize(p)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            p = self._file()
            if not p:
                self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
            size = os.path.getsize(p)
            lo, hi = 0, size - 1
            rng = self.headers.get("Range")
            if rng and rng.startswith("bytes="):
                a, b = rng[6:].split("-", 1)
                if a == "":
                    lo = max(0, size - int(b))
                else:
                    lo = int(a)
                    if b: hi = min(int(b), size - 1)
            with open(p, "rb") as f:
                f.seek(lo)
                data = f.read(hi - lo + 1)
            with stats.lock:
                stats.gets += 1
                stats.bytes += len(data)
                stats.paths.add(p)
            self.send_response(206 if rng else 200)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            if rng:
                self.send_header("Content-Range", f"bytes {lo}-{hi}/{size}")
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

    class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
        daemon_threads = True
        request_queue_size = 128

    srv = S(("127.0.0.1", 0), H)  # ephemeral port, loopback only
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


# ------------------------------------------------------------------ run
def run_query_http(root, layout_name, files, q, stats):
    """Run the query through DuckDB httpfs against a fresh loopback server.
    A server per query (own ephemeral port) keeps each query's connections in
    their own TCP tuple space: a single port exhausts the macOS ephemeral range
    in TIME_WAIT after a few thousand requests."""
    if not files:
        return 0
    srv = serve(root, stats)
    try:
        return _run_query_http(srv.server_address[1], layout_name, files, q, stats)
    finally:
        srv.shutdown()
        srv.server_close()


def _run_query_http(port, layout_name, files, q, stats):
    con = duckdb.connect()
    con.execute("LOAD httpfs")
    con.execute("SET enable_external_file_cache=false")
    con.execute("SET threads=4")
    urls = [f"http://127.0.0.1:{port}/{layout_name}/{quote(f, safe='/=')}" for f in files]
    cols = sorted(set(q.proj) | q.pred_cols())
    # bit_xor(hash(col)) forces every value to be decoded. count(col) is answered
    # from Parquet statistics when the filter keeps the whole row group, which
    # reads almost nothing and would understate the bytes.
    agg = ", ".join(["count(*)"] + [f"bit_xor(hash({c}))" for c in cols])
    lst = ", ".join(f"'{u}'" for u in urls)
    for attempt in range(4):  # the loopback server occasionally refuses under 200+ parallel HEADs
        try:
            stats.reset()
            row = con.execute(f"SELECT {agg} FROM read_parquet([{lst}]) WHERE {q.sql_where()}").fetchone()
            break
        except duckdb.Error:
            if attempt == 3:
                raise
            time.sleep(0.5)
    con.close()
    return row[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.expanduser("~/tmp/logless-sort-bench"))
    ap.add_argument("--hours", type=int, default=72, help="live window length in hours")
    ap.add_argument("--rows", type=int, default=3_000_000, help="live + incident rows")
    ap.add_argument("--replay-frac", type=float, default=0.12)
    ap.add_argument("--seg-rows", type=int, default=20_000, help="rows per WAL segment")
    ap.add_argument("--layouts", default="")
    ap.add_argument("--json", default="")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--keep", action="store_true", help="keep the written layouts")
    args = ap.parse_args()
    if args.quick:
        args.rows, args.seg_rows = 150_000, 2_000
    configure(args.hours)
    os.makedirs(args.out, exist_ok=True)
    free = shutil.disk_usage(args.out).free / 2**30
    print(f"free disk {free:.1f} GiB (other sessions are writing)")
    if free < 15:
        raise SystemExit("under 15 GiB free; stopping")
    layouts = [l for l in LAYOUTS if not args.layouts or l.name in args.layouts.split(",")]

    con = duckdb.connect()
    con.execute("SET threads=4")
    t = time.time()
    build_corpus(con, args.rows, args.replay_frac, args.seg_rows)
    (total,) = con.execute("SELECT count(*) FROM logs").fetchone()
    print(f"corpus: {total} rows built in {time.time() - t:.1f}s")
    corpus = dict(rows=total)
    corpus["by_kind"] = dict(con.execute("SELECT kind, count(*) FROM logs GROUP BY 1").fetchall())
    corpus["by_level"] = dict(con.execute("SELECT level, count(*) FROM logs GROUP BY 1").fetchall())
    corpus["with_trace"] = con.execute("SELECT count(trace_id) FROM logs").fetchone()[0]
    corpus["templates"] = con.execute("SELECT count(DISTINCT template_id) FROM logs").fetchone()[0]
    corpus["traces"] = con.execute("SELECT count(DISTINCT trace_id) FROM logs").fetchone()[0]
    corpus["max_rows_one_trace"] = con.execute(
        "SELECT max(c) FROM (SELECT count(*) c FROM logs WHERE trace_id IS NOT NULL GROUP BY trace_id)").fetchone()[0]
    corpus["obs_hours"] = con.execute("SELECT count(DISTINCT obs_hour) FROM logs").fetchone()[0]
    corpus["ev_hours"] = con.execute("SELECT count(DISTINCT ev_hour) FROM logs").fetchone()[0]
    corpus["max_rows_one_obs_hour_level"] = con.execute(
        "SELECT max(c) FROM (SELECT count(*) c FROM logs GROUP BY obs_hour, level)").fetchone()[0]
    corpus["median_rows_obs_hour_level"] = con.execute(
        "SELECT median(c) FROM (SELECT count(*) c FROM logs GROUP BY obs_hour, level)").fetchone()[0]
    corpus["segments"] = con.execute("SELECT count(DISTINCT seg) FROM logs").fetchone()[0]
    print(json.dumps(corpus, indent=1, default=str))
    queries = pick_queries(con)
    truth = [con.execute(f"SELECT count(*) FROM logs WHERE {q.sql_where(True)}").fetchone()[0] for q in queries]
    print(f"{len(queries)} queries; truth rows min {min(truth)} max {max(truth)}")

    stats = Stats()
    results = {"corpus": corpus, "args": vars(args), "layouts": {}}
    try:
        for L in layouts:
            print(f"\n=== {L.name}: {L.desc}")
            out, wall = write_layout(con, L, args.out)
            files, rgs = load_metadata(con, out)
            rel = lambda f: os.path.relpath(f, out)
            nfiles = len(files)
            size = sum(f["size"] for f in files.values())
            fan, nseg = segment_fanout(con, L)
            rows_per_file = sorted(f["rows"] for f in files.values())
            lay = dict(desc=L.desc, write_wall_s=round(wall, 2), files=nfiles, bytes=size,
                       row_groups=len(rgs), rows=sum(rows_per_file),
                       partitions=len({os.path.dirname(rel(f)) for f in files}),
                       precompaction_files=fan, segments=nseg,
                       median_rows_per_file=rows_per_file[nfiles // 2],
                       footer_bytes_total=sum(f["footer"] for f in files.values()))
            print(f"  files {nfiles}  row groups {len(rgs)}  bytes {size:,}  write {wall:.1f}s  "
                  f"pre-compaction files {fan} from {nseg} segments")
            by_file = defaultdict(list)
            for (f, g), rg in sorted(rgs.items(), key=lambda kv: (kv[0][0], kv[0][1])):
                rg["id"] = g
                by_file[f].append(rg)
            hv_of = {f: hive(rel(f)) for f in files}
            per_kind = defaultdict(list)
            for i, q in enumerate(queries):
                cols = q.pred_cols() | set(q.proj)
                opened = [f for f in files if q.partition_ok(L, hv_of[f])]
                surv = [rg for f in opened for rg in by_file[f] if q.rg_ok(rg)]
                total_rgs = sum(len(by_file[f]) for f in opened)
                bytes_model = sum(files[f]["footer"] + 8 for f in opened) + \
                    sum(sum(rg["cols"].get(c, 0) for c in cols) for rg in surv)
                t = time.time()
                if 3 * len(opened) + 4 * len(surv) <= HTTP_MAX_REQUESTS:
                    try:
                        got = run_query_http(args.out, L.name, [rel(f) for f in opened], q, stats)
                        s = stats.snap()
                    except duckdb.Error as e:
                        print(f"  (http failed for {q.label}: {str(e)[:60]})")
                        got, s = None, dict(bytes=None, gets=None, files=None)
                    if got is not None and got != truth[i]:
                        print(f"  !! rows mismatch {q.kind} {q.label}: got {got} truth {truth[i]}")
                else:  # loopback port budget: ~3 connections per file
                    got, s = None, dict(bytes=None, gets=None, files=None)
                qwall = time.time() - t
                # Oracle: row groups that really hold a matching row. Any index
                # (bloom filter, page index, inverted index) can do no better, and
                # min/max pruning must never drop one of them.
                ideal = oracle_rgs(con, opened, q, files, by_file)
                surv_ids = {(rg["file"], rg["id"]) for rg in surv}
                if not ideal <= surv_ids:
                    print(f"  !! min/max pruning dropped {len(ideal - surv_ids)} row groups holding matches: {q.label}")
                bytes_ideal = sum(files[f]["footer"] + 8 for f in {f for f, _ in ideal}) + \
                    sum(sum(rgs[(f, g)]["cols"].get(c, 0) for c in cols) for f, g in ideal)
                per_kind[q.kind].append(dict(
                    kind=q.kind, label=q.label, files_opened=len(opened),
                    files_with_data=len({rg["file"] for rg in surv}), rgs_in_opened=total_rgs,
                    rgs_read=len(surv), bytes_model=bytes_model, bytes_http=s["bytes"],
                    http_gets=s["gets"], http_files=s["files"], rows=got, truth=truth[i], rgs_ideal=len(ideal), bytes_ideal=bytes_ideal,
                    wall_s=round(qwall, 3)))
            lay["queries"] = {}
            for kind, recs in sorted(per_kind.items()):
                n = len(recs)
                def m(k):
                    v = [r[k] for r in recs if r[k] is not None]
                    return sum(v) / len(v) if v else float("nan")
                lay["queries"][kind] = dict(
                    n=n, files_opened=m("files_opened"), files_with_data=m("files_with_data"),
                    rgs_in_opened=m("rgs_in_opened"), rgs_read=m("rgs_read"),
                    bytes_model=m("bytes_model"), bytes_http=m("bytes_http"),
                    rgs_ideal=m("rgs_ideal"), bytes_ideal=m("bytes_ideal"), http_gets=m("http_gets"),
                    rows=m("rows"), wall_s=m("wall_s"), detail=recs)
                print(f"  {kind:4s} n={n:2d} files {m('files_opened'):7.1f} (data {m('files_with_data'):6.1f}) "
                      f"rg {m('rgs_read'):7.1f}/{m('rgs_in_opened'):7.1f} (ideal {m('rgs_ideal'):6.1f}) model {m('bytes_model')/1e6:8.2f}MB "
                      f"ideal {m('bytes_ideal')/1e6:7.2f}MB http {m('bytes_http')/1e6:8.2f}MB gets {m('http_gets'):6.1f} rows {m('rows'):9.1f}")
            # recall of a reader that assumes observed ~= timestamp (not the safe rule)
            if "evhour" not in L.parts and L.name not in EVENT_HOUR_LAYOUTS:
                rec = []
                for i, q in enumerate(queries):
                    if q.tlo is None or truth[i] == 0:
                        continue
                    nf = [f for f in files if q.naive_partition_ok(hv_of[f])]
                    if not nf:
                        rec.append(0.0)
                        continue
                    lst = ", ".join(f"'{f}'" for f in nf)
                    (c,) = con.execute(f"SELECT count(*) FROM read_parquet([{lst}]) WHERE {q.sql_where()}").fetchone()
                    rec.append(c / truth[i])
                lay["naive_recall_mean"] = sum(rec) / len(rec)
                lay["naive_recall_min"] = min(rec)
                print(f"  naive observed~=timestamp pruning recall: mean {lay['naive_recall_mean']:.3f} "
                      f"min {lay['naive_recall_min']:.3f}")
            results["layouts"][L.name] = lay
            if not args.keep:
                shutil.rmtree(out)
    finally:
        pass
    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=1, default=str)
    if not args.keep:
        shutil.rmtree(args.out, ignore_errors=True)


if __name__ == "__main__":
    main()
