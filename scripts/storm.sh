#!/usr/bin/env bash
# Storm harness: sustain a target error *rate* and assert bounded memory.
#
# The existing benches measure throughput by volume, which says nothing about
# what happens when errors arrive faster than they can be forwarded. The thing
# that breaks first under that is the pushdown ring, and the failure mode is
# unbounded RSS — so this asserts memory from the OS, not from our own
# accounting, because our accounting is exactly what would be wrong.
#
# Usage: scripts/storm.sh [errors-per-second] [seconds] [rss-limit-mb]
set -uo pipefail

RATE="${1:-10000}"
SECONDS_TO_RUN="${2:-20}"
RSS_LIMIT_MB="${3:-512}"
BIN="${LOGLESS_BIN:-./target/release/logless}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

[[ -x "$BIN" ]] || { echo "build first: cargo build --release" >&2; exit 1; }

"$BIN" init --data-dir "$WORK/data" > "$WORK/config.toml"

# Every line an error, each with a distinct trace id: the worst case for the
# ring, because nothing dedupes and every error wants its own context window.
generator() {
  awk -v rate="$RATE" -v secs="$SECONDS_TO_RUN" 'BEGIN {
    total = rate * secs
    for (i = 0; i < total; i++) {
      printf "DEBUG\tcache lookup trace_id=%032x key=order:%d\n", i, i
      printf "ERROR\tpayment gateway timeout trace_id=%032x order=%d\n", i, i
    }
  }'
}

generator | "$BIN" run --config "$WORK/config.toml" > "$WORK/run.out" 2>&1 &
pid=$!

peak_rss_kb=0
samples=0
while kill -0 "$pid" 2>/dev/null; do
  # ps reports RSS in KiB on both macOS and Linux.
  rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')
  if [[ -n "${rss:-}" ]] && (( rss > peak_rss_kb )); then
    peak_rss_kb=$rss
  fi
  samples=$((samples + 1))
  sleep 0.2
done
wait "$pid" 2>/dev/null

peak_mb=$((peak_rss_kb / 1024))
echo "peak RSS: ${peak_mb} MiB over $samples samples (limit ${RSS_LIMIT_MB} MiB)"
grep -E "^received=|^pushdown:" "$WORK/run.out" || true

if (( peak_mb > RSS_LIMIT_MB )); then
  echo "FAIL: peak RSS ${peak_mb} MiB exceeded ${RSS_LIMIT_MB} MiB" >&2
  exit 1
fi
if ! grep -q "^received=" "$WORK/run.out"; then
  echo "FAIL: the agent did not report its accounting; it probably died" >&2
  exit 1
fi
if grep -q "ACCOUNTING VIOLATION" "$WORK/run.out"; then
  echo "FAIL: accounting violation under storm" >&2
  exit 1
fi
echo "storm harness passed"
