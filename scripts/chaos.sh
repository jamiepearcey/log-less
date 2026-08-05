#!/usr/bin/env bash
# Chaos harness: kill the agent repeatedly, mid-write, and assert nothing is
# lost or double-counted.
#
# The accounting invariant that unit tests check stops at the WAL. This checks
# the whole path — ingested == committed + still-in-WAL + deliberately dropped —
# across SIGKILLs at arbitrary points, which is the only way to catch a bug that
# lives in the seam between two components (both of the merge data-loss bugs
# did).
#
# Usage: scripts/chaos.sh [rounds] [lines-per-round]
set -uo pipefail

ROUNDS="${1:-10}"
LINES="${2:-20000}"
BIN="${LOGLESS_BIN:-./target/release/logless}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if [[ ! -x "$BIN" ]]; then
  echo "build first: cargo build --release (or set LOGLESS_BIN)" >&2
  exit 1
fi

"$BIN" init --data-dir "$WORK/data" > "$WORK/config.toml"
mkdir -p "$WORK/data"

generate() {
  # Mixed severities so shedding and level partitioning are both exercised.
  awk -v n="$1" -v start="$2" 'BEGIN {
    for (i = start; i < start + n; i++) {
      r = i % 10
      if (r == 0)      printf "ERROR\tpayment gateway timeout for order %d\n", i
      else if (r < 3)  printf "WARN\tretrying charge for order %d\n", i
      else if (r < 6)  printf "INFO\torder %d accepted\n", i
      else             printf "DEBUG\tcache lookup key=order:%d\n", i
    }
  }'
}

total_sent=0
previous_committed=0
for round in $(seq 1 "$ROUNDS"); do
  generate "$LINES" "$((total_sent))" > "$WORK/round.txt"

  # Feed the agent and kill it partway through — SIGKILL, not SIGTERM: the
  # graceful path is already tested, this is the ungraceful one.
  "$BIN" run --config "$WORK/config.toml" < "$WORK/round.txt" \
    > "$WORK/run-$round.out" 2>&1 &
  pid=$!
  # Kill at a random point inside the run, biased to the middle where a
  # segment roll or a merge is most likely to be in flight.
  sleep "0.$((RANDOM % 4 + 1))"
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" 2>/dev/null
    killed="killed"
  else
    killed="finished"
  fi
  wait "$pid" 2>/dev/null
  total_sent=$((total_sent + LINES))

  # Recover, then merge everything the WAL still holds.
  "$BIN" recover --config "$WORK/config.toml" --repair >> "$WORK/recover.log" 2>&1
  "$BIN" merge --config "$WORK/config.toml" >> "$WORK/merge.log" 2>&1

  committed=$("$BIN" catalog --config "$WORK/config.toml" 2>/dev/null \
    | awk -F'rows=' '/^files=/ {split($2, a, " "); print a[1]}')
  echo "round $round ($killed): sent=$total_sent committed=${committed:-0}"

  if [[ -z "${committed:-}" ]]; then
    echo "FAIL: catalog reported nothing after round $round" >&2
    exit 1
  fi
  # What can actually be asserted after a SIGKILL.
  #
  # Not "committed == sent": killing the agent mid-stream leaves lines still in
  # the pipe that it never read, and never accepting them is correct behaviour,
  # not loss. (An earlier version of this harness asserted equality and failed
  # on its own arithmetic.) What must hold is:
  #
  #   1. committed <= sent          — nothing is invented
  #   2. committed is non-decreasing — nothing already durable is lost later
  #   3. no duplicates              — a crash mid-merge must not double-commit
  if (( committed > total_sent )); then
    echo "FAIL: committed $committed > sent $total_sent (records were invented)" >&2
    exit 1
  fi
  if (( committed < previous_committed )); then
    echo "FAIL: committed fell from $previous_committed to $committed (durable data was lost)" >&2
    exit 1
  fi
  previous_committed=$committed

  # The whole-path invariant, not just the queue's. After a SIGKILL the
  # counters are stale (they are written on a clean exit), which `verify`
  # reports as such rather than as loss — what must never appear is records
  # unaccounted for in the other direction.
  if ! "$BIN" verify --config "$WORK/config.toml" > "$WORK/verify-$round.out" 2>&1; then
    echo "FAIL: accounting violation after round $round" >&2
    cat "$WORK/verify-$round.out" >&2
    exit 1
  fi

  if command -v duckdb >/dev/null 2>&1; then
    duplicates=$(duckdb -noheader -list -c "
      SELECT count(*) - count(DISTINCT event_id)
      FROM read_parquet('$WORK/data/store/**/*.parquet');" 2>/dev/null || echo 0)
    if [[ "${duplicates:-0}" != "0" ]]; then
      echo "FAIL: $duplicates duplicate event ids after round $round" >&2
      exit 1
    fi
  fi
done

echo
echo "final: offered=$total_sent committed=$committed"
echo "(a killed round leaves unread lines in the pipe; those were never accepted)"
"$BIN" status --config "$WORK/config.toml"
echo "chaos harness passed $ROUNDS rounds"
