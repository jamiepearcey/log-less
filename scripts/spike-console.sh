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
# Usage: scripts/spike-console.sh [events]
set -uo pipefail
cd "$(dirname "$0")/.."

EVENTS="${1:-1000000}"
CORPUS="${LOGHUB_DIR:-target/loghub}"
WORK="${SPIKE_DIR:-$(mktemp -d)}"
STUDIO="${SQL_STUDIO_DIR:-../../sql-studio}"
BIN="${LOGLESS_BIN:-./target/release/logless}"
BATCH=5000
TOKEN=spike-token

[[ -x "$BIN" ]] || { echo "build first: cargo build --release" >&2; exit 1; }

say() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

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
"$BIN" run --config "$WORK/config.toml" --hec > "$WORK/agent.log" 2>&1 &
AGENT=$!
trap 'kill $AGENT 2>/dev/null' EXIT

for _ in $(seq 1 60); do
  [[ "$(curl -s -o /dev/null -w '%{http_code}' \
        http://127.0.0.1:8088/services/collector/health)" == "200" ]] && break
done

say "4. Load"
# Batches must stay under ingest.queue_capacity (65536): the receiver enqueues a
# whole batch or none, so an oversized batch 503s forever rather than eventually
# succeeding. Measured at 5k: ~316k events/s, zero retries.
python3 - "$WORK/hec.ndjson" "$TOKEN" "$BATCH" <<'PY'
import sys, time, urllib.error, urllib.request
path, token, size = sys.argv[1], sys.argv[2], int(sys.argv[3])
url = "http://127.0.0.1:8088/services/collector"
sent = retries = 0
started = time.time()
def post(payload):
    global retries
    delay = 0.05
    for _ in range(200):
        req = urllib.request.Request(url, data=payload, method="POST",
                                     headers={"Authorization": "Splunk " + token})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return
        except urllib.error.HTTPError as e:
            if e.code != 503:
                sys.exit("HTTP %d: %s" % (e.code, e.read()[:200]))
            retries += 1
            time.sleep(min(delay, 1.0)); delay *= 1.5
    sys.exit("gave up on 503")
buf = []
for line in open(path):
    buf.append(line)
    if len(buf) >= size:
        post("".join(buf).encode()); sent += len(buf); buf = []
if buf:
    post("".join(buf).encode()); sent += len(buf)
took = time.time() - started
print("%d events, %d retries, %.1fs, %.0f ev/s" % (sent, retries, took, sent / took))
PY

say "5. Drain and merge"
kill $AGENT 2>/dev/null; wait $AGENT 2>/dev/null
trap - EXIT
"$BIN" merge --config "$WORK/config.toml"
"$BIN" status --config "$WORK/config.toml"
tail -3 "$WORK/agent.log"

say "6. The console, against that store"
RAW=$(wc -c < "$WORK/hec.ndjson" | tr -d ' ')
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
