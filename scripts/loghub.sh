#!/usr/bin/env bash
# Benchmarks template mining against the real LogHub corpora.
#
# Downloads the 2,000-line samples and their published ground-truth template
# counts, then reports ours beside theirs. Synthetic lines flatter both accuracy
# and throughput; this is the number that means something.
#
# Usage: scripts/loghub.sh [comma-separated similarity thresholds]
set -uo pipefail
cd "$(dirname "$0")/.."

CORPUS="${LOGHUB_DIR:-target/loghub}"
BIN="${LOGHUB_BIN:-./target/release/loghub}"
DATASETS=(HDFS Hadoop Spark Zookeeper OpenStack Linux Apache Thunderbird BGL HealthApp Proxifier)
BASE=https://raw.githubusercontent.com/logpai/loghub/master

mkdir -p "$CORPUS"
for dataset in "${DATASETS[@]}"; do
  [[ -s "$CORPUS/$dataset.log" ]] || \
    curl -sSf -m 30 -o "$CORPUS/$dataset.log" "$BASE/$dataset/${dataset}_2k.log" || \
    echo "warning: could not fetch $dataset" >&2
  [[ -s "$CORPUS/${dataset}_templates.csv" ]] || \
    curl -sSf -m 30 -o "$CORPUS/${dataset}_templates.csv" \
      "$BASE/$dataset/${dataset}_2k.log_templates.csv" 2>/dev/null || true
done

[[ -x "$BIN" ]] || { echo "build first: cargo build --release" >&2; exit 1; }
SWEEP="${1:-}" exec "$BIN" "$CORPUS"
