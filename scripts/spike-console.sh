#!/usr/bin/env bash
# Builds the fixture behind docs/platform-thesis.md §12.
#
# 1M events from the 11 LogHub corpora, loaded through the HEC receiver with
# per-event timestamps spread over 7 days, merged to Parquet. Then points
# sql-studio's console tests at the resulting store.
#
# The corpora are real lines with published ground truth, which is the whole
# point: a synthetic generator would decide the template count in advance and
# the spine measurement would be circular.
#
# The load is followed by a drain wait (scripts/spike-load.py): the agent is
# stopped only once the admin API reports queue depth 0, or after
# SPIKE_DRAIN_TIMEOUT seconds, and the run ends with exact counts (sent, acked,
# stored, dropped by policy) in $WORK/result.json. Stopping on the last 200 was
# the old behaviour and under load it killed the agent with a backlog (Plane
# LOG-132); SPIKE_NO_DRAIN=1 restores it as a negative control.
#
# Usage: scripts/spike-console.sh [events]
#   SPIKE_SYNTHETIC=1   generate events locally instead of fetching LogHub
#   SPIKE_SKIP_STUDIO=1 stop after the reconciliation (no console tests)
#   SPIKE_HEC_PORT / SPIKE_ADMIN_PORT   default 18088 / 18101
set -uo pipefail
cd "$(dirname "$0")/.."

EVENTS="${1:-1000000}"
CORPUS="${LOGHUB_DIR:-target/loghub}"
WORK="${SPIKE_DIR:-$(mktemp -d)}"
STUDIO="${SQL_STUDIO_DIR:-../../sql-studio}"
BIN="${LOGLESS_BIN:-./target/release/logless}"
BATCH=5000
TOKEN=spike-token
HEC_PORT="${SPIKE_HEC_PORT:-18088}"
ADMIN_PORT="${SPIKE_ADMIN_PORT:-18101}"
DRAIN_TIMEOUT="${SPIKE_DRAIN_TIMEOUT:-120}"
export LOGLESS_ADMIN_TOKEN="spike-admin-token"
SYNTHETIC="${SPIKE_SYNTHETIC:-0}"

[[ -x "$BIN" ]] || { echo "build first: cargo build --release" >&2; exit 1; }

say() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

if [[ "$SYNTHETIC" != 1 ]]; then
say "1. Fetch the LogHub corpora"
mkdir -p "$CORPUS"
BASE=https://raw.githubusercontent.com/logpai/loghub/master
for d in HDFS Hadoop Spark Zookeeper OpenStack Linux Apache Thunderbird BGL HealthApp Proxifier; do
  [[ -s "$CORPUS/$d.log" ]] || \
    curl -sSf -m 30 -o "$CORPUS/$d.log" "$BASE/$d/${d}_2k.log" || echo "warning: no $d" >&2
done
echo "$(cat "$CORPUS"/*.log | wc -l | tr -d ' ') lines, \
$(cat "$CORPUS"/*.log | sort -u | wc -l | tr -d ' ') unique"

say "2. Generate $EVENTS HEC events"
# Datasets become services; timestamps spread over 7 days so a timechart has an
# axis. Severity is left to log-less's own inference, which is the realistic path.
python3 - "$CORPUS" "$WORK/hec.ndjson" "$EVENTS" <<'PY'
import json, os, random, sys, time
corpus, out_path, total = sys.argv[1], sys.argv[2], int(sys.argv[3])
datasets = []
for name in sorted(os.listdir(corpus)):
    if name.endswith(".log"):
        with open(os.path.join(corpus, name), errors="replace") as fh:
            lines = [l.rstrip("\n") for l in fh if l.strip()]
        if lines:
            datasets.append((name[:-4], lines))
now, span = int(time.time()), 7 * 24 * 3600
random.seed(7)
written = 0
with open(out_path, "w") as out:
    while written < total:
        for service, lines in datasets:
            for line in lines:
                if written >= total:
                    break
                t = now - span + int(span * written / total) + random.randint(-90, 90)
                out.write(json.dumps({
                    "time": t, "sourcetype": service,
                    "host": "node-%02d" % (written % 8),
                    "event": "%s trace_id=%032x" % (line, written // 12),
                }, separators=(",", ":")) + "\n")
                written += 1
print("%d events" % written)
PY
fi

say "3. Configure and start the agent with HEC"
"$BIN" init --data-dir "$WORK/data" > "$WORK/config.toml"
python3 - "$WORK/config.toml" "$TOKEN" <<'PY'
import re, sys
path, token = sys.argv[1], sys.argv[2]
text = open(path).read()
i = text.index("[hec]"); j = text.index("\n\n", i)
block = text[i:j]
for key, value in {"enabled": "true", "workers": "4", "tokens": '["%s"]' % token,
                   "max_body_bytes": "67108864",
                   "max_decompressed_bytes": "268435456"}.items():
    block = re.sub(r"(?m)^%s = .*$" % key, "%s = %s" % (key, value), block)
open(path, "w").write(text[:i] + block + text[j:])
PY
python3 - "$WORK/config.toml" "$HEC_PORT" "$ADMIN_PORT" <<'PY'
import re, sys
path, hec, admin = sys.argv[1:4]
text = open(path).read()
text = re.sub(r'(?m)^addr = "127.0.0.1:8088"$', 'addr = "127.0.0.1:%s"' % hec, text)
i = text.index("[admin]"); j = text.index("\n\n", i)
block = text[i:j]
block = re.sub(r"(?m)^enabled = .*$", "enabled = true", block)
block = re.sub(r'(?m)^addr = .*$', 'addr = "127.0.0.1:%s"' % admin, block)
open(path, "w").write(text[:i] + block + text[j:])
PY
"$BIN" run --config "$WORK/config.toml" --hec > "$WORK/agent.log" 2>&1 &
AGENT=$!
trap 'kill $AGENT 2>/dev/null' EXIT

for _ in $(seq 1 100); do
  [[ "$(curl -s -o /dev/null -w '%{http_code}' \
        http://127.0.0.1:$HEC_PORT/services/collector/health)" == "200" ]] && break
  sleep 0.1
done
if ! kill -0 $AGENT 2>/dev/null || \
   [[ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$HEC_PORT/services/collector/health)" != 200 ]]; then
  echo "FAIL: agent did not come up on HEC port $HEC_PORT (port in use by another process?)" >&2
  tail -5 "$WORK/agent.log" >&2
  exit 1
fi

say "4. Load, then wait for the agent to drain"
# Batches must stay under ingest.queue_capacity (65536): the receiver enqueues a
# whole batch or none, so an oversized batch 503s forever rather than eventually
# succeeding. A 200 means queued, not in the WAL, so the load is followed by a
# poll of the admin API until received == written + dropped (queue depth 0).
LOAD_ARGS=(--ndjson "$WORK/hec.ndjson")
[[ "$SYNTHETIC" == 1 ]] && LOAD_ARGS=()
[[ "${SPIKE_NO_DRAIN:-0}" == 1 ]] && LOAD_ARGS+=(--no-drain)
python3 "$(dirname "$0")/spike-load.py" \
  "http://127.0.0.1:$HEC_PORT/services/collector" "$TOKEN" \
  "http://127.0.0.1:$ADMIN_PORT" "$LOGLESS_ADMIN_TOKEN" \
  "$EVENTS" "$BATCH" "$DRAIN_TIMEOUT" ${LOAD_ARGS[@]+"${LOAD_ARGS[@]}"} > "$WORK/load.json" 2> "$WORK/load.err"
LOAD_RC=$?
cat "$WORK/load.json" "$WORK/load.err"

say "5. Stop, merge, reconcile"
kill -TERM $AGENT 2>/dev/null
for _ in $(seq 1 300); do kill -0 $AGENT 2>/dev/null || break; sleep 0.1; done
if kill -0 $AGENT 2>/dev/null; then
  echo "agent did not exit within 30s of SIGTERM" >&2
  kill -KILL $AGENT 2>/dev/null
fi
wait $AGENT 2>/dev/null
trap - EXIT
"$BIN" merge --config "$WORK/config.toml"
"$BIN" status --config "$WORK/config.toml"
"$BIN" verify --config "$WORK/config.toml" | tee "$WORK/verify.out"
tail -3 "$WORK/agent.log"
if (( LOAD_RC != 0 )); then
  echo "FAIL: the load generator exited $LOAD_RC (see above); no reconciliation possible" >&2
  exit 1
fi
python3 - "$WORK" <<'PY'
import json, re, subprocess, sys
work = sys.argv[1]
load = json.load(open(work + "/load.json"))
verify = open(work + "/verify.out").read()
v = dict(re.findall(r"(\w+)=(\d+)", verify.splitlines()[0]))
stored, in_wal, dropped = int(v["committed"]), int(v["in_wal"]), int(v["dropped"])
res = dict(load, stored=stored, in_wal_unmerged=in_wal, dropped_by_policy=dropped,
           lost=load["acked"] - stored - in_wal - dropped)
json.dump(res, open(work + "/result.json", "w"), indent=2)
print("sent=%d acked=%d stored=%d in_wal=%d dropped_by_policy=%d lost=%d (drained=%s in %.2fs)" % (
    res["sent"], res["acked"], stored, in_wal, dropped, res["lost"], res["drained"], res["drain_secs"]))
PY

[[ "${SPIKE_SKIP_STUDIO:-0}" == 1 ]] && exit 0
say "6. The console, against that store"
RAW=$(wc -c < "$WORK/hec.ndjson" 2>/dev/null | tr -d ' ' || echo 0)
echo "store: $WORK/data/store"
if [[ -d "$STUDIO" ]]; then
  ( cd "$STUDIO" && LOGLESS_STORE="$WORK/data/store" LOGLESS_RAW_BYTES="$RAW" \
      cargo test --release -p sqls-server --test logless_console_spike \
        -- --ignored --nocapture --test-threads=1 )
else
  echo "sql-studio not at $STUDIO — run the console tests yourself with:"
  echo "  LOGLESS_STORE=$WORK/data/store LOGLESS_RAW_BYTES=$RAW \\"
  echo "    cargo test --release -p sqls-server --test logless_console_spike -- --ignored --nocapture"
fi
