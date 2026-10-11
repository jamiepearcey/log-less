#!/usr/bin/env bash
# Template churn across 10 real Temporal server releases (v1.20.0..v1.29.0).
# Lines are RENDERED from real log statements; see docs/evidence/template-churn.md.
# usage: scripts/template-churn.sh [temporal-repo] [workdir]
set -euo pipefail
cd "$(dirname "$0")/.."
REPO="${1:-$HOME/projects/__externals/temporal}"
WORK="${2:-${TMPDIR:-/tmp}/template-churn}"
TAGS=(v1.20.0 v1.21.0 v1.22.0 v1.23.0 v1.24.0 v1.25.0 v1.26.0 v1.27.0 v1.28.0 v1.29.0)
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/tmp/logless-admin-target}"
mkdir -p "$WORK"
python3 -I scripts/churn-extract.py "$REPO" "$WORK" "${TAGS[@]}"
cargo build -q -p logless-cli --bin churn
FILES=(); for t in "${TAGS[@]}"; do FILES+=("$WORK/$t.log"); done
"$CARGO_TARGET_DIR/debug/churn" "$WORK/churn.json" "${FILES[@]}"
python3 -I scripts/churn-analyze.py "$WORK" "${TAGS[@]}"
