#!/usr/bin/env bash
# Regression check for Plane LOG-132: the spike fixture must wait for the agent
# to drain before stopping it, and must report loss as exact counts.
#
# Runs scripts/spike-console.sh on synthetic events (no network, no console
# tests) and asserts: drained within the timeout, queue depth 0, and
# acked == stored + in_wal + dropped_by_policy, i.e. lost == 0.
#
# Usage: scripts/spike-drain-check.sh [events] [busy-loops]
#   busy-loops  CPU burners started here (own PIDs, stopped on exit) to run
#               the same check under load; default 0
#   SPIKE_CHECK_CONTROL=1  also run the old stop-on-last-200 behaviour and
#               print its loss (informational: its loss is timing dependent)
set -uo pipefail
cd "$(dirname "$0")/.."

EVENTS="${1:-300000}"
LOOPS="${2:-0}"
BIN="${LOGLESS_BIN:-./target/release/logless}"
OUT="$(mktemp -d)"
BURNERS=()
cleanup() {
  for p in ${BURNERS[@]+"${BURNERS[@]}"}; do kill "$p" 2>/dev/null; done
  rm -rf "$OUT"
}
trap cleanup EXIT
[[ -x "$BIN" ]] || { echo "build first: cargo build --release (or set LOGLESS_BIN)" >&2; exit 1; }

for ((n = 0; n < LOOPS; n++)); do
  # Self-limiting (300 s) as well as killed on exit.
  perl -e 'alarm 300; 1 while 1' &
  BURNERS+=("$!")
done
echo "load: ${#BURNERS[@]} busy loops"

run() { # name, extra env...
  local name="$1"; shift
  mkdir -p "$OUT/$name"
  env SPIKE_DIR="$OUT/$name" SPIKE_SYNTHETIC=1 SPIKE_SKIP_STUDIO=1 LOGLESS_BIN="$BIN" "$@" \
    ./scripts/spike-console.sh "$EVENTS" > "$OUT/$name.log" 2>&1
  grep -E '^sent=' "$OUT/$name.log" || { echo "$name: no result"; tail -20 "$OUT/$name.log"; return 1; }
}

if [[ "${SPIKE_CHECK_CONTROL:-0}" == 1 ]]; then
  echo "-- control (SPIKE_NO_DRAIN=1, informational)"
  run control SPIKE_NO_DRAIN=1 || true
fi

echo "-- drain wait"
run drain || exit 1
python3 - "$OUT/drain/result.json" <<'PY' || exit 1
import json, sys
r = json.load(open(sys.argv[1]))
bad = []
if not r["drained"]: bad.append("agent did not drain within the timeout")
if r["queue_depth"] != 0: bad.append("queue depth %r at stop" % r["queue_depth"])
if r["sent"] != r["acked"]: bad.append("sent %d != acked %d" % (r["sent"], r["acked"]))
if r["lost"] != 0: bad.append("lost %d acked records" % r["lost"])
if bad:
    print("FAIL: " + "; ".join(bad), file=sys.stderr); sys.exit(1)
PY
echo "spike drain check passed"
