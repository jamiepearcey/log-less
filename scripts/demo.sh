#!/usr/bin/env bash
# The under-an-hour demo from the brief, as one script.
#
# Shows the whole claim end to end: full-fidelity logs held locally, a curated
# fraction forwarded, and the forwarding decision reversible afterwards.
#
# Usage: scripts/demo.sh [lines]
set -uo pipefail

LINES="${1:-100000}"
BIN="${LOGLESS_BIN:-./target/release/logless}"
WORK="${DEMO_DIR:-$(mktemp -d)}"
mkdir -p "$WORK"
[[ -x "$BIN" ]] || { echo "build first: cargo build --release" >&2; exit 1; }

say() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

say "1. Configure"
"$BIN" init --data-dir "$WORK/data" > "$WORK/config.toml"
"$BIN" buckets --config "$WORK/config.toml"

say "2. Ingest $LINES lines of mixed-severity application logs"
awk -v n="$LINES" 'BEGIN {
  srand(7)
  for (i = 0; i < n; i++) {
    trace = sprintf("%032x", int(i / 12))
    r = i % 500
    if (r == 0)
      printf "ERROR\t[api] payment gateway timeout after 30000ms order=%d trace_id=%s\n", i, trace
    else if (r % 37 == 0)
      printf "WARN\t[api] retrying charge order=%d attempt=2 trace_id=%s\n", i, trace
    else if (r % 3 == 0)
      printf "INFO\t[api] order accepted order=%d total=%d.99 trace_id=%s\n", i, i % 400, trace
    else
      printf "DEBUG\t[api] cache lookup key=order:%d hit=true latency=0.4ms trace_id=%s\n", i, trace
  }
}' > "$WORK/app.log"

"$BIN" run --config "$WORK/config.toml" \
  --pushdown-out "$WORK/pushdown.jsonl" < "$WORK/app.log"

say "3. What is on disk"
raw=$(wc -c < "$WORK/app.log" | tr -d ' ')
"$BIN" status --config "$WORK/config.toml"
stored=$(du -sk "$WORK/data/store" | cut -f1)
echo "raw input:  $((raw / 1024)) KiB"
echo "on disk:    ${stored} KiB"
echo "ratio:      $(awk -v r="$raw" -v s="$((stored * 1024))" 'BEGIN {printf "%.1fx", r/s}')"

say "4. The mined templates — the shapes, not the lines"
"$BIN" templates --config "$WORK/config.toml" --limit 8

say "5. What would have been forwarded"
errors=$(grep -c '^ERROR' "$WORK/app.log")
windows=$(wc -l < "$WORK/pushdown.jsonl" | tr -d ' ')
echo "errors in the input:      $errors"
echo "events that would be sent: $windows"
echo "each carries the DEBUG lines that preceded it — which were never forwarded"
echo
echo "one of them:"
head -c 600 "$WORK/pushdown.jsonl"; echo

say "6. The undo button: replay history you chose not to forward"
"$BIN" replay --config "$WORK/config.toml" --since 24h --min-severity 17 --limit 5
echo
echo "and the DEBUG context around it, still on disk:"
"$BIN" replay --config "$WORK/config.toml" --since 24h --contains "cache lookup" --limit 3

say "7. Your own tools, no engine of ours"
if command -v duckdb >/dev/null 2>&1; then
  duckdb -c "
    SELECT level, count(*) AS rows
    FROM read_parquet('$WORK/data/store/**/*.parquet', hive_partitioning = 1)
    GROUP BY level ORDER BY rows DESC;"
else
  echo "(install duckdb to query $WORK/data/store/**/*.parquet directly)"
fi

echo
echo "data dir: $WORK"
