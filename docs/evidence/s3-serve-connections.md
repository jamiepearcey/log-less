# S3 serve on the bounded listener: does `Connection: close` hurt the console's read path?

Run on 2026-10-11 (BST) from branch `hard/s3conn` at `e85dad1` (identical to main; S3 serve has been on
`bounded_http` since `e4a95f7`). One machine, everything on 127.0.0.1.

**Answer: no material regression, so the code is unchanged.** Through the console (its own query builders and
client, DuckDB httpfs underneath), every scenario of the earlier byte report ran as fast or faster than the
recorded numbers, moved the same bytes in the same number of requests, and had no failed request, timeout or
truncated body. The worst scenario was 1.14x the recorded best (the single-statement dashboard panel, 109 ms against 96 ms, inside run-to-run noise);
the 1.5x bar that would have triggered keep-alive work was not reached anywhere. The failure modes the console can
hit (a 2 GiB range, 50/128/200 concurrent ranges, 300 clients hanging up mid-download, a reader that stops) all
leave the listener serving others. Two properties worth knowing are in "Findings" at the end: a stalled reader holds
its connection for about 25 s on macOS loopback, not 10 s, and a reader that is slow but never pauses 10 s holds its
connection for as long as it likes, so 128 such readers lock everyone out.

## What is compared with what

- **Recorded**: `~/projects/logless-studio/docs/evidence/s3-read-bytes.md`, taken earlier the same night on the
  old `tiny_http` S3 serve (keep-alive capable) with the same store (5,097,397 rows, 192 MiB, 76 files), the same
  driver and the same running console. **The comparison is against those recorded numbers; the old binary was not
  rebuilt or rerun.**
- **New**: the `bounded_http` S3 serve built from this branch, which answers every request with
  `Connection: close`, 15 s to receive a request, a 10 s timeout on each write, at most 128 connections (8 workers x 16).

An important detail of the recorded setup: its counting proxy (`~/tmp/s3-bytes/s3-count-proxy-log.py`) opens a
**new upstream connection for every request** and strips `Connection` headers, while keeping its own client side
(DuckDB) alive. So the recorded numbers already paid one TCP connect to the agent per request, and DuckDB never saw
the agent's connection behaviour. Rerunning only that proxy would hide exactly the effect in question. So the new
binary was measured through two proxies:

1. **original proxy** (`orig-proxy.py`: the recorded proxy plus a count of client connections). Same conditions as
   the recorded numbers: like for like.
2. **transparent proxy** (`scripts/s3-conn-proxy.py`, new): one upstream connection per client connection,
   every byte forwarded, `Connection: close` included. DuckDB sees the server's behaviour as is, and the
   connection count at the proxy is DuckDB's. It also counts replies that said `Connection: close`, bodies that
   ended before their `Content-Length`, and every non-2xx status.

Each isolated scenario ran on a fresh agent + proxy + console connection (cold: DuckDB's file cache is empty for a
new connection), twice (a, b), like the recorded runs.

## Commands

```
df -h /                                                   # 15.27 GiB free at the start; see caveat 1
cd ~/projects/infrastructure/log-less-wt-hard-s3conn
CARGO_TARGET_DIR=$HOME/tmp/logless-admin-target cargo build -p logless-cli
#   Finished `dev` profile [unoptimized + debuginfo] target(s) in 14.85s
strings ~/tmp/logless-admin-target/debug/logless | grep -c "Connection: close"     # 2: the bounded listener
S=<scratch dir>; cp -c ~/tmp/logless-admin-target/debug/logless $S/logless        # pinned copy (shared target dir)
```

`$S/run-pass.sh` is `~/tmp/s3-bytes/run-pass.sh` with three changes: the proxy script is a parameter
(`PROXYPY`), the binary is `$S/logless`, and the driver is a copy of `~/tmp/s3-bytes/driver.mjs` whose three
result lines also print the proxy's `conns`, `close_replies`, `truncated`, `upstream_errors`, `statuses` and
`max_req_per_conn`. Nothing in the console repo changed; the console is the dev server already running on
`127.0.0.1:8420`, which was not restarted; each pass adds its own connection and deletes it on exit.

```
for mode in conn orig; do for rep in a b; do
  for s in search_1h_all search_errors_day search_body_service search_trace stats_by_service issues dashboard_panel; do
    PROXYPY=$S/$mode-proxy.py ONLY=connect,$s ./run-pass.sh $mode-iso$rep-$s; done; done
  PROXYPY=$S/$mode-proxy.py ONLY=connect,issues_list_stmt ./run-pass.sh $mode-isoa-issues_list_stmt
  PROXYPY=$S/$mode-proxy.py ./run-pass.sh $mode-session
done
for i in 2 3; do for mode in conn orig; do PROXYPY=$S/$mode-proxy.py ./run-pass.sh $mode-session$i; done; done
python3 $S/compare.py        # tables below, verbatim
```

(`$S/conn-proxy.py` is `scripts/s3-conn-proxy.py`.)

## Cold, per scenario (sum of the statements the screen runs)

`compare.py` output, verbatim. "worst new/recorded" is the slower of the two new proxies' best-of-two divided by
the recorded best-of-two.

| scenario | S3 MB recorded / new | requests recorded / new | TCP conns new, transparent proxy | wall ms recorded a / b | wall ms new, original proxy a / b | wall ms new, transparent proxy a / b | worst new/recorded (best-of-2 vs best-of-2) | 4xx/5xx, truncated bodies (new; every 4xx was a console probe 404, see text) |
|---|---|---|---|---|---|---|---|---|
| dashboard panel (chart, warn+ by service, 24h) | 2.10 / 2.10 | 49 / 49 | 49 / 49 | 145 / 96 | 120 / 109 | 76 / 61 | 1.14x | 0, 0 |
| issues list (list + overview) | 6.05 / 6.05 | 99 / 99 | 99 / 99 | 244 / 221 | 161 / 155 | 158 / 119 | 0.70x | 0, 0 |
| 1h all levels | 8.37 / 8.37 | 10 / 10 | 10 / 10 | 349 / 319 | 100 / 105 | 148 / 185 | 0.46x | 0, 0 |
| api body search 24h | 200.20 / 200.22 | 337 / 339 | 339 / 337 | 1165 / 1259 | 481 / 439 | 510 / 959 | 0.44x | 1, 0 |
| errors 24h | 153.25 / 153.25 | 175 / 175 | 175 / 175 | 1008 / 434 | 308 / 282 | 292 / 374 | 0.67x | 0, 0 |
| trace-id lookup 24h | 16.41 / 16.41 | 81 / 81 | 81 / 83 | 268 / 247 | 156 / 143 | 97 / 181 | 0.58x | 1, 0 |
| count by service 24h | 18.31 / 18.31 | 145 / 145 | 145 / 145 | 171 / 162 | 127 / 85 | 73 / 141 | 0.52x | 0, 0 |
| issues list statement alone (24h) | 6.04 / 6.04 | 98 / 98 | 98 / 98 | 104 / 104 | 111 / 111 | 79 / 79 | 1.07x | 0, 0 |

Reading it:

- Bytes and request counts match the recorded run (the extra 2 requests in body search and trace lookup are the two
  404s below). Through the transparent proxy, TCP connections = requests in every scenario, and every reply said
  `Connection: close` (`close_replies` = requests and `max_req_per_conn` = 1 in every statement line of the raw
  results): DuckDB opened one connection per request, as the concern predicted. One exception in 112 statement
  lines: trace events, run b, shows 6 connections for 5 counted requests, most likely a request still in flight
  when the driver read the counters (not verified).
- That costs nothing measurable on loopback: the transparent proxy's wall times are in the same range as the
  original proxy's, and both are lower than the recorded ones. The recorded run was slower overall (its 1 h events
  statement alone took 225/280 ms against 100 to 185 ms here); the console's DuckDB is shared with other sessions,
  so wall time moves with load more than with the listener. Bytes and requests are the stable measures; wall time is
  secondary.
- **The two 4xx**: `{"200":2,"206":168,"404":1}` in the body-search count and `{"206":4,"404":1}` in the trace
  events. The matrix runs did not log their targets, so the same two scenarios were rerun four times each with the
  proxy printing every non-2xx (`status >= 400` to stderr). Every 4xx in those reruns was one of two console
  probes, both for keys this store does not have: `HEAD /logs/_meta/templates.pq` (once per connect: this store
  has no template dictionary) and `GET /logs/.sqls-status-probe` (the console's periodic status poll; it lands
  inside a statement's measuring window when the timing coincides). Neither is a listener failure. Truncated
  bodies: 0 everywhere. Upstream errors at the proxy: 0 everywhere.

## Session pass (one console, every scenario in order)

| pass | proxy | S3 MB | requests | TCP conns | wall ms (sum) | errors | truncated |
|---|---|---|---|---|---|---|---|
| cold | recorded (old listener) | 243.23 | 406 | n/a (not counted) | 2228 | n/a | n/a |
| cold | new, original proxy | 220.06 | 396 | 215 | 1476 | 0 | n/a |
| cold | new, transparent proxy | 255.21 | 459 | 459 | 1579 | 0 | 0 |
| warm | recorded (old listener) | 71.29 | 125 | n/a (not counted) | 1980 | n/a | n/a |
| warm | new, original proxy | 89.31 | 127 | 87 | 1282 | 0 | n/a |
| warm | new, transparent proxy | 114.46 | 327 | 327 | 1347 | 0 | 0 |

Two more session passes per proxy, to see the spread (sums over all statements):

```
results-conn-session.jsonl  cold MB 255.21 req 459 conns 459 wall 1579 trunc 0
results-conn-session.jsonl  warm MB 114.46 req 327 conns 327 wall 1347 trunc 0
results-conn-session2.jsonl cold MB 214.03 req 318 conns 318 wall 1725 trunc 0
results-conn-session2.jsonl warm MB 62.25 req 238 conns 238 wall 1363 trunc 0
results-conn-session3.jsonl cold MB 224.52 req 324 conns 324 wall 1412 trunc 0
results-conn-session3.jsonl warm MB 43.06 req 59 conns 59 wall 1386 trunc 0
results-orig-session.jsonl  cold MB 220.06 req 396 conns 215 wall 1476 trunc 0
results-orig-session.jsonl  warm MB 89.31 req 127 conns 87 wall 1282 trunc 0
results-orig-session2.jsonl cold MB 227.79 req 332 conns 168 wall 2062 trunc 0
results-orig-session2.jsonl warm MB 61.56 req 185 conns 121 wall 1816 trunc 0
results-orig-session3.jsonl cold MB 217.84 req 323 conns 166 wall 1118 trunc 0
results-orig-session3.jsonl warm MB 106.27 req 306 conns 193 wall 1169 trunc 0
```

Session bytes vary by about 2x on the warm pass with either proxy, because the console's DuckDB file cache is shared
and evicts; the variation is not tied to the proxy. The useful number is the original proxy's connection count:
**even with a keep-alive server side, DuckDB opened 0.42 to 0.66 connections per request** (215/396, 168/332,
166/323, 87/127, 121/185, 193/306), because it reads with many threads at once and each thread holds its own
connection. So keep-alive would save at most about half of the TCP handshakes the console makes, not all of them.

## Failure modes, against the binary directly

`scripts/s3-serve-failure-modes.py` starts its own agent (anonymous, loopback, so no signing in the way) on a
scratch store: the same config with `data_dir` pointed at a directory holding
`hour=2026-10-01T00/level=info/big.parquet`, a 2 GiB sparse file (`mkfile -n 2g`, 4.5 MB on disk; the server does
not parse objects, so zeros serve like Parquet), and one real 4.7 MB file from the byte-report store (`cp -c`).

```
perl -e 'alarm 500; exec @ARGV' -- python3 scripts/s3-serve-failure-modes.py $S/logless $S/bigstore/config.toml
{"probe": "agent up", "port": 49206, "pid": 20960, "tcp_sockets": 1}
{"probe": "big range", "range": "0-", "status": 206, "bytes": 2147483648, "content_length": 2147483648, "complete": true, "seconds": 0.58, "mib_s": 3537}
{"probe": "big range", "range": "1073741824-1610612735", "status": 206, "bytes": 536870912, "content_length": 536870912, "complete": true, "seconds": 0.1, "mib_s": 5386}
{"probe": "concurrent ranges", "clients": 50, "ok_206_complete": 50, "refused_or_empty": 0, "other": [], "wall_s": 0.03, "p50_ms": 6, "max_ms": 11, "connection_cap": 128}
{"probe": "concurrent ranges", "clients": 128, "ok_206_complete": 128, "refused_or_empty": 0, "other": [], "wall_s": 0.04, "p50_ms": 6, "max_ms": 13, "connection_cap": 128}
{"probe": "concurrent ranges", "clients": 200, "ok_206_complete": 200, "refused_or_empty": 0, "other": [], "wall_s": 0.09, "p50_ms": 10, "max_ms": 43, "connection_cap": 128}
{"probe": "mid-download disconnects", "clients": 300, "answered_200": 300, "refused": 0, "seconds": 0.09}
{"probe": "after disconnects", "status": 200, "complete": true, "ms": 3, "tcp_sockets_before": 1, "tcp_sockets_after": 1}
{"probe": "stalled reader", "others_served_while_stalled": 157, "others_worst_ms": 4, "stalled_bytes_before_eof": 1136956, "cut_off": true}
{"probe": "trickle reader (16 KiB / 2 s)", "still_connected_after_s": 26.0, "dropped": false, "bytes_read": 212992}
{"probe": "cap filled by 128 stalled readers", "new_client_result_while_full": [-1, 0, 0, 0.0, "ConnectionResetError"], "served_again_after_s": 25.7}
{"probe": "end", "status": 200, "complete": true, "tcp_sockets": 1}
```

What each line shows:

- **Big range**: a whole 2 GiB object and a 512 MiB range from its middle arrive complete (`bytes` =
  `Content-Length`). There is no whole-download deadline, only the per-write timeout, so size alone never cuts a
  download.
- **Concurrent ranges**: 50 concurrent 1 to 4 MiB ranges, all 206 and complete, p50 6 ms. 128 and 200 also all
  succeeded, but these requests finish in milliseconds, so the server never actually held 128 at once; the cap
  itself is exercised by the last probe.
- **Mid-download disconnects**: 300 clients (more than the cap) each read 1 MiB of the 2 GiB object and hang up. All
  300 were answered, none refused, so no slot leaked; afterwards the agent holds only its listening socket
  (`tcp_sockets` 1, from `lsof -iTCP`) and the next request is served in 3 ms. A hang-up surfaces as a failed write,
  which ends the copy at once.
- **Stalled reader**: one client starts the 2 GiB download and stops reading. Over the next 32 s, 157 other range
  requests were served, the slowest in 4 ms. The stalled one was then cut off (EOF after 1.1 MB).
- **When** the stalled reader is cut off, measured on its own with `scripts/s3-serve-stall-time.py` (three trials,
  polling `netstat` for the server side of that connection):

  ```
  trial 0: server side left ESTABLISHED after 25.2 s (state FIN_WAIT_1)
  trial 1: server side left ESTABLISHED after 25.2 s (state FIN_WAIT_1)
  trial 2: server side left ESTABLISHED after 25.2 s (state FIN_WAIT_1)
  ```

  Not 10 s: the stalled receiver's window still opens by a few bytes for about 15 s (its `Recv-Q` crept from
  327,964 to 343,052 bytes over 20 s while the application read nothing, seen with `netstat -an`), each of those
  tiny writes is progress, and the 10 s timer restarts on each. A 20 s stall left the connection open (the client
  then read the whole 2 GiB); a 75 s stall found it closed.
- **Trickle reader**: a client reading 16 KiB every 2 s is still connected after 26 s and would stay as long as it
  keeps reading. This is the documented design ("a slow reader is cut off when it stops reading, not when it is
  merely slow", `s3_serve.rs`), confirmed.
- **Cap filled by 128 stalled readers**: while 128 connections each hold a stalled 2 GiB download, a new client is
  hung up on at once (connection reset), the listener's past-the-cap behaviour. Service returned 25.7 s after the
  stall began, when the write timeouts released the slots.

## Findings (no code changed for these; they are outside the regression question)

1. **A stalled reader holds a connection for about 25 s, not 10 s**, on macOS loopback (other TCP stacks will differ
   in the window-probe phase). The 10 s write timeout bounds time without progress, and a receiver whose kernel
   still trickles window space counts as progress.
2. **128 slow-but-reading clients lock the S3 listener out indefinitely**, and 128 stalled ones for about 25 s.
   This needs a credential (anonymous serving is loopback only), so it is an authenticated-client or
   local-process denial of service, bounded in memory and threads but not in time. A whole-response deadline or a
   minimum transfer rate would close it; either would also cut off a legitimate client reading a big object over a
   slow link, so it is a trade-off to decide, not a fix to slip in here. Worth a line in `docs/threat-model.md`.
3. **Keep-alive is not worth adding for the console's sake now.** One connection per request costs nothing
   measurable on loopback, and even with a keep-alive server DuckDB opens a connection for roughly every second
   request. Revisit when the console reads S3 serve over a real network or through TLS (see caveat 3).

## Caveats

1. **Disk rule.** The task allowed a build only with at least 15 GiB free. The check before the build showed 15.27
   GiB, but other sessions were writing and the volume was at 13 GiB when the (incremental, 15 s) build started and
   12 GiB after it. No other cargo command was run. Free space later swung between 6.3 and 42 GiB from other
   sessions while only measurements ran.
2. Comparison is with recorded numbers taken on the old listener earlier the same night, not with a rerun of the old
   binary. Wall times through the shared console vary run to run by up to 2x (the recorded run's own a/b pairs
   differ by up to 2.3x, errors 24h: 1008 / 434 ms), so only a large ratio would have meant anything. None appeared.
3. Loopback hides the cost being asked about. Over a network every new connection costs one round trip before the
   request (and a TLS handshake if S3 serve sits behind TLS). At 145 requests for `count by service`, with DuckDB
   reading in parallel, the added latency is a few round trips per wave of parallel reads, not 145; that is
   reasoning, not a measurement.
4. The "4xx" column's two entries were attributed from four logged reruns of the same scenarios, not from the
   matrix runs themselves, whose targets were not logged.
5. The 2 GiB object is a sparse file of zeros served from the page cache, so 3.5 to 5.4 GiB/s is the listener's
   ceiling on this machine, not disk throughput.
6. The console binary is the deleted-but-running dev server (pid 3958) described in the byte report's caveat 1.
