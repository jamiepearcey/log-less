#!/usr/bin/env bash
# Restore drill: ingest on an edge, upload its Parquet parts to central, destroy the edge's data directory, and
# rebuild it with `logless restore` from the uploaded parts alone. Parts are on central's own disk (the default,
# PARTS_BACKEND=local, no MinIO) or in a MinIO this script starts (PARTS_BACKEND=s3). Compares row counts,
# per-template counts and the template dictionary keys before and after, and runs two damage cases (a flipped
# byte in one stored part, one part deleted) that the restore must detect, by name, with exit status 1.
#
# Usage: scripts/restore-drill.sh        See docs/restore.md and docs/evidence/restore-drill.md.
# Env:   LOGLESS_BIN (default $CARGO_TARGET_DIR/debug/logless)  PARTS_BACKEND (local | s3)
#        MINIO_BIN (/opt/homebrew/bin/minio)  MC_BIN (mc)  (s3 only)  DUCKDB_BIN (duckdb)  N_RECORDS (6000)  N_TAIL (500)  SYNC_TIMEOUT (90)  KEEP (0)
# Everything runs on random free ports in a throwaway directory with random credentials and is removed on exit.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
LOGLESS_BIN="${LOGLESS_BIN:-${CARGO_TARGET_DIR:-$HERE/../target}/debug/logless}"
MINIO_BIN="${MINIO_BIN:-/opt/homebrew/bin/minio}"
MC_BIN="${MC_BIN:-mc}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
N_RECORDS="${N_RECORDS:-6000}"
N_TAIL="${N_TAIL:-500}"
SYNC_TIMEOUT="${SYNC_TIMEOUT:-90}"
KEEP="${KEEP:-0}"
PARTS_BACKEND="${PARTS_BACKEND:-local}"
BUCKET="logless-parts"
PREFIX="edges"
NODE="edge1"   # parts live under <prefix>/<node>/ in the bucket, parts/<node>/ on central's disk (ADR 0001, 0003)

case "$PARTS_BACKEND" in
  local) TOOLS="" ;;
  s3) TOOLS="$MINIO_BIN $MC_BIN" ;;
  *) echo "PARTS_BACKEND must be local or s3" >&2; exit 2 ;;
esac
for tool in "$LOGLESS_BIN" $TOOLS "$DUCKDB_BIN" curl python3 openssl sqlite3 shasum; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing: $tool" >&2; exit 2; }
done

R="$(mktemp -d "${TMPDIR:-/tmp}/logless-restore.XXXXXX")"
chmod 700 "$R"
PIDS=""

cleanup() {
  local pid
  for pid in $PIDS; do kill -TERM "$pid" 2>/dev/null; done
  for _ in $(seq 1 50); do
    local alive=""
    for pid in $PIDS; do kill -0 "$pid" 2>/dev/null && alive=1; done
    [ -z "$alive" ] && break
    sleep 0.1
  done
  for pid in $PIDS; do kill -KILL "$pid" 2>/dev/null; done
  if [ "$KEEP" = 1 ]; then echo "kept: $R"; else rm -rf "$R"; fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
rand() { openssl rand -hex "$1"; }
free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
jq_py() { python3 -c "import sys, json; v = json.load(sys.stdin); print($1)"; }

RESULTS="$R/results.tsv"
: > "$RESULTS"
check() { local status=FAIL; [ "$2" = 1 ] && status=PASS; printf '%s\t%s\t%s\n' "$1" "$status" "$3" >> "$RESULTS"; }
eq() { [ "$1" = "$2" ] && echo 1 || echo 0; }

wait_http() {
  local deadline=$(( $(date +%s) + $2 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    curl -s -o /dev/null --max-time 1 "$1" && return 0
    sleep 0.2
  done
  return 1
}

admin() { # port, token, method, path, [body]
  if [ $# -ge 5 ]; then
    curl -s --max-time 10 -X "$3" -H "Authorization: Bearer $2" -H 'Content-Type: application/json' -d "$5" "http://127.0.0.1:$1$4"
  else
    curl -s --max-time 10 -X "$3" -H "Authorization: Bearer $2" "http://127.0.0.1:$1$4"
  fi
}

render_config() { # data_dir, overrides, extra  (same helper as e2e-stack.sh)
  "$LOGLESS_BIN" init --data-dir "$1" | OVERRIDES="$2" EXTRA="$3" python3 -c '
import os, sys
want = {}
for line in os.environ["OVERRIDES"].strip().splitlines():
    section, key, value = line.split("|", 2)
    want.setdefault(section, {})[key] = value
out, section, seen = [], "", set()
def close(section):
    for key, value in want.get(section, {}).items():
        if (section, key) not in seen:
            out.append(f"{key} = {value}")
for line in sys.stdin.read().splitlines():
    stripped = line.strip()
    if stripped.startswith("["):
        close(section)
        section = stripped.strip("[]")
    elif "=" in stripped and section in want:
        key = stripped.split("=", 1)[0].strip()
        if key in want[section]:
            line = f"{key} = {want[section][key]}"
            seen.add((section, key))
    out.append(line)
close(section)
print("\n".join(out))
print(os.environ["EXTRA"])
'
}

gone() { ! kill -0 "$1" 2>/dev/null || [ "$(ps -o stat= -p "$1" 2>/dev/null | cut -c1)" = Z ]; }
stop_within() { # pid, seconds
  local pid=$1 limit_ms=$(( $2 * 1000 )) start
  start=$(now_ms)
  kill -TERM "$pid" 2>/dev/null
  while ! gone "$pid"; do
    STOP_MS=$(( $(now_ms) - start ))
    [ "$STOP_MS" -ge "$limit_ms" ] && return 1
    sleep 0.05
  done
  STOP_MS=$(( $(now_ms) - start ))
  wait "$pid" 2>/dev/null
  return 0
}

T0=$(now_ms)
log "run dir $R, logless $("$LOGLESS_BIN" --version 2>/dev/null || echo "$LOGLESS_BIN"), parts backend $PARTS_BACKEND"

# --- where parts go ----------------------------------------------------------------------------------------------
# s3: a MinIO and its bucket. local: central's own parts directory, nothing to start. The helpers below (list, read,
# overwrite and delete a stored part, and where a restore reads from) are the only place the two differ.
NODE_DIR="$R/central/data/parts/$NODE"
if [ "$PARTS_BACKEND" = s3 ]; then
  MINIO_PORT=$(free_port); MINIO_CONSOLE=$(free_port)
  MINIO_USER="drill$(rand 6)"; MINIO_SECRET="$(rand 20)"
  MINIO_ROOT_USER="$MINIO_USER" MINIO_ROOT_PASSWORD="$MINIO_SECRET" \
    "$MINIO_BIN" server --quiet --address "127.0.0.1:$MINIO_PORT" --console-address "127.0.0.1:$MINIO_CONSOLE" \
    "$R/minio-data" > "$R/minio.log" 2>&1 &
  MINIO_PID=$!; PIDS="$PIDS $MINIO_PID"
  wait_http "http://127.0.0.1:$MINIO_PORT/minio/health/live" 30 || { echo "MinIO did not start" >&2; cat "$R/minio.log" >&2; exit 1; }
  MCA=("$MC_BIN" --config-dir "$R/mc")
  "${MCA[@]}" alias set drill "http://127.0.0.1:$MINIO_PORT" "$MINIO_USER" "$MINIO_SECRET" > /dev/null
  "${MCA[@]}" mb "drill/$BUCKET" > /dev/null
  log "minio on $MINIO_PORT, bucket $BUCKET"
  PARTS_CONFIG="
peer.parts|endpoint|\"http://127.0.0.1:$MINIO_PORT\"
peer.parts|bucket|\"$BUCKET\"
peer.parts|prefix|\"$PREFIX\"
peer.parts|access_key_id|\"$MINIO_USER\"
peer.parts|secret_access_key|\"$MINIO_SECRET\""
  export LOGLESS_S3_ACCESS_KEY_ID="$MINIO_USER" LOGLESS_S3_SECRET_ACCESS_KEY="$MINIO_SECRET"
  RESTORE_FROM=(--from "s3://$BUCKET/$PREFIX/$NODE" --endpoint "http://127.0.0.1:$MINIO_PORT")
  node_parts() { "${MCA[@]}" ls --json "drill/$BUCKET/$PREFIX/$NODE/" | python3 -c 'import sys, json; [print(json.loads(l)["key"]) for l in sys.stdin if l.strip()]' | sort; }
  cat_part() { "${MCA[@]}" cat "drill/$BUCKET/$PREFIX/$NODE/$1"; }
  put_part() { "${MCA[@]}" cp --quiet "$1" "drill/$BUCKET/$PREFIX/$NODE/$2" > /dev/null; }   # file, name
  rm_part() { "${MCA[@]}" rm --quiet "drill/$BUCKET/$PREFIX/$NODE/$1" > /dev/null; }
else
  PARTS_CONFIG=""
  RESTORE_FROM=(--from "$NODE_DIR")
  node_parts() { [ -d "$NODE_DIR" ] && (cd "$NODE_DIR" && find . -type f | sed 's|^\./||' | sort); }
  cat_part() { cat "$NODE_DIR/$1"; }
  put_part() { cp "$1" "$NODE_DIR/$2"; }
  rm_part() { rm "$NODE_DIR/$1"; }
fi

# --- central (inventory, and where the parts go) -----------------------------------------------------------------
PEER_PORT=$(free_port); CENTRAL_ADMIN=$(free_port)
PEER_TOKEN="$(rand 24)"; CENTRAL_TOKEN="$(rand 24)"
mkdir -p "$R/central"; : > "$R/central/empty.log"
render_config "$R/central/data" "
peer|enabled|true
peer|addr|\"127.0.0.1:$PEER_PORT\"
peer|nodes|[{ id = \"$NODE\", token = \"$PEER_TOKEN\" }]
peer.parts|enabled|true$PARTS_CONFIG
admin|enabled|true
admin|addr|\"127.0.0.1:$CENTRAL_ADMIN\"
admin|token|\"$CENTRAL_TOKEN\"
" "" > "$R/central/config.toml"
"$LOGLESS_BIN" run --config "$R/central/config.toml" --tail "$R/central/empty.log" > "$R/central/agent.log" 2>&1 &
CENTRAL_PID=$!; PIDS="$PIDS $CENTRAL_PID"
wait_http "http://127.0.0.1:$CENTRAL_ADMIN/" 30 || { echo "central did not start" >&2; cat "$R/central/agent.log" >&2; exit 1; }

# --- edge --------------------------------------------------------------------------------------------------------
OTLP_PORT=$(free_port); EDGE_ADMIN=$(free_port); EDGE_TOKEN="$(rand 24)"
mkdir -p "$R/edge"; : > "$R/edge/empty.log"
render_config "$R/edge/data" "
node|id|\"$NODE\"
storage|wal_segment_bytes|131072
otlp|enabled|true
otlp|addr|\"127.0.0.1:$OTLP_PORT\"
admin|enabled|true
admin|addr|\"127.0.0.1:$EDGE_ADMIN\"
admin|token|\"$EDGE_TOKEN\"
" "
[[destinations]]
type = \"logless\"
url = \"http://127.0.0.1:$PEER_PORT\"
token = \"$PEER_TOKEN\"
min_severity = 0
fidelity = 100
anomaly_boost = false
" > "$R/edge/config.toml"
start_edge() {
  "$LOGLESS_BIN" run --config "$R/edge/config.toml" --tail "$R/edge/empty.log" >> "$R/edge/agent.log" 2>&1 &
  EDGE_PID=$!; PIDS="$PIDS $EDGE_PID"
  wait_http "http://127.0.0.1:$EDGE_ADMIN/" 30 || { echo "edge did not start" >&2; tail -20 "$R/edge/agent.log" >&2; exit 1; }
  wait_http "http://127.0.0.1:$OTLP_PORT/" 30
}
start_edge
log "central: peer $PEER_PORT admin $CENTRAL_ADMIN; edge: otlp $OTLP_PORT admin $EDGE_ADMIN"

# --- send --------------------------------------------------------------------------------------------------------
# Six message shapes over two services, so the miner mints several template ids and the per-template counts mean something.
send_batches() { # start, count, tag
  mkdir -p "$R/payloads/$3"
  START="$1" COUNT="$2" OUT="$R/payloads/$3" python3 -c '
import json, os, time
out, start0, count = os.environ["OUT"], int(os.environ["START"]), int(os.environ["COUNT"])
shapes = [
  lambda i: f"GET /api/v1/orders/{i % 5000} 200 took {i % 997}ms user=u{i % 311}",
  lambda i: f"connection to db{i % 9} timed out after {1000 + i % 4000} ms retry {i % 5}",
  lambda i: f"cache miss for key session:{i * 2654435761 % 4294967296:08x} ttl={i % 600}s",
  lambda i: f"payment {100000 + i} declined code={i % 40} merchant=m{i % 70}",
  lambda i: f"worker {i % 16} heartbeat ok queue_depth={i % 1000}",
  lambda i: f"job {i} finished in {i % 8000} ms status=ok",
]
levels = [(5, "DEBUG"), (9, "INFO"), (13, "WARN"), (17, "ERROR")]
services = ["drill-api", "drill-worker"]
now = time.time_ns()
for b, start in enumerate(range(start0, start0 + count, 500)):
    records = []
    for i in range(start, min(start + 500, start0 + count)):
        num, text = levels[i % 4]
        records.append({"timeUnixNano": str(now + i * 1000), "severityNumber": num, "severityText": text,
                        "body": {"stringValue": shapes[i % len(shapes)](i) + " " + "pad" * 40},
                        "attributes": [{"key": "seq", "value": {"intValue": str(i)}}]})
    doc = {"resourceLogs": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": services[b % 2]}}]},
                             "scopeLogs": [{"logRecords": records}]}]}
    open(f"{out}/otlp-{b:03d}.json", "w").write(json.dumps(doc))
'
  local fail=0 f code
  for f in "$R"/payloads/"$3"/otlp-*.json; do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 --retry 6 --retry-all-errors --retry-delay 1 -H 'Content-Type: application/json' --data-binary "@$f" "http://127.0.0.1:$OTLP_PORT/v1/logs")
    [ "$code" = 200 ] || { fail=1; log "OTLP $f -> $code"; }
  done
  return $fail
}
send_batches 0 "$N_RECORDS" main
check "ingest: all batches answered 200" "$(eq $? 0)" "$N_RECORDS records over OTLP/HTTP JSON"

wait_synced() {
  local received=0 written=0 sent=-1 backlog=-1 stable=0 info status
  local deadline=$(( $(date +%s) + SYNC_TIMEOUT ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    info=$(admin "$EDGE_ADMIN" "$EDGE_TOKEN" GET /admin/v1/info)
    received=$(echo "$info" | jq_py 'v["ingest"]["received"]' 2>/dev/null || echo 0)
    written=$(echo "$info" | jq_py 'v["ingest"]["written"]' 2>/dev/null || echo 0)
    status=$(admin "$EDGE_ADMIN" "$EDGE_TOKEN" GET /admin/v1/sync)
    sent=$(echo "$status" | jq_py 'v["status"]["sent_records"]' 2>/dev/null || echo -1)
    backlog=$(echo "$status" | jq_py 'v["status"]["backlog_bytes"]' 2>/dev/null || echo -1)
    if [ "$written" -gt 0 ] && [ "$received" = "$written" ] && [ "$sent" = "$written" ] && [ "$backlog" = 0 ]; then
      stable=$(( stable + 1 )); [ "$stable" -ge 2 ] && break
    else stable=0; fi
    sleep 0.5
  done
  SYNC_DETAIL="received=$received written=$written sent_records=$sent backlog_bytes=$backlog"
  [ "$stable" -ge 2 ]
}
wait_synced; check "edge synced what it accepted" "$(eq $? 0)" "$SYNC_DETAIL"

# --- flush the WAL tail into Parquet, then upload every part ------------------------------------------------------
# A clean stop plus an offline merge leaves nothing only in the WAL, so every accepted record is in a Parquet part
# and the upload covers all of them. (The unclean case is the tail phase below.)
stop_within "$EDGE_PID" 15; check "edge stops cleanly before the flush" "$(eq $? 0)" "${STOP_MS} ms"
"$LOGLESS_BIN" merge --config "$R/edge/config.toml" > "$R/edge/merge.log" 2>&1
"$LOGLESS_BIN" verify --config "$R/edge/config.toml" > "$R/edge/verify.log" 2>&1
check "edge verify after merge (accounting invariant)" "$(eq $? 0)" "$(head -1 "$R/edge/verify.log" | cut -c1-160)"
start_edge

upload_parts() {
  local from=$(( T0 - 3600000 )) to=$(( $(now_ms) + 3600000 )) st
  admin "$EDGE_ADMIN" "$EDGE_TOKEN" POST /admin/v1/sync/parts "{\"from_ms\":$from,\"to_ms\":$to}" > /dev/null
  PSTATE=running
  for _ in $(seq 1 240); do
    st=$(admin "$EDGE_ADMIN" "$EDGE_TOKEN" GET /admin/v1/sync | jq_py 'json.dumps(v["status"]["parts"])')
    PSTATE=$(echo "$st" | jq_py 'v["state"] if v else "none"')
    [ "$PSTATE" != running ] && break
    sleep 0.5
  done
  PTOTAL=$(echo "$st" | jq_py 'v["files_total"]'); PUP=$(echo "$st" | jq_py 'v["uploaded"]'); PREJ=$(echo "$st" | jq_py 'v["rejected"]')
  [ "$PSTATE" = done ] && [ "$PUP" = "$PTOTAL" ] && [ "$PREJ" = 0 ]
}
upload_parts; check "parts upload done" "$(eq $? 0)" "state=$PSTATE files_total=$PTOTAL uploaded=$PUP rejected=$PREJ"

# --- BEFORE snapshot (the edge's own store) ----------------------------------------------------------------------
EDGE_STORE="$R/edge/data/store"
sql_csv() { "$DUCKDB_BIN" -noheader -csv -c "$1"; }
by_template() { sql_csv "select coalesce(cast(template_id as varchar), 'none') t, count(*) from read_parquet('$1/**/*.parquet', union_by_name = true) group by 1 order by 1"; }
by_pair() { sql_csv "select coalesce(cast(template_id as varchar), 'none') t, coalesce(service, '~') s, count(*) from read_parquet('$1/**/*.parquet', union_by_name = true) group by 1, 2 order by 1, 2"; }
total_rows() { sql_csv "select count(*) from read_parquet('$1/**/*.parquet', union_by_name = true)"; }
dict_keys() { sql_csv "select template_id, coalesce(service, '~') from read_parquet('$1/tenant=*/index=*/_meta/templates/*.pq') order by 1, 2"; }
# The catalog's templates table is the miner's private counter state (a restore numbers it afresh); what rows carry
# and what the catalog keys its sightings on is the content hash, so that is what must survive.
cat_templates() { sqlite3 "$1" "select template_id, sum(rows) from template_services group by 1 order by 1"; }

BEFORE_ROWS=$(total_rows "$EDGE_STORE")
by_template "$EDGE_STORE" > "$R/before.tpl"; by_pair "$EDGE_STORE" > "$R/before.pair"; dict_keys "$EDGE_STORE" > "$R/before.dict"
cat_templates "$R/edge/data/catalog.sqlite" > "$R/before.cat"
N_TPL=$(grep -vc '^none,' "$R/before.tpl"); N_PAIR=$(wc -l < "$R/before.dict" | tr -d ' ')
EDGE_FILES=$(find "$EDGE_STORE" -name '*.parquet' | wc -l | tr -d ' ')
EDGE_BYTES=$(find "$EDGE_STORE" -name '*.parquet' -exec stat -f %z {} + | awk '{s+=$1} END {print s}')
check "before: every accepted record is in a part" "$(eq "$BEFORE_ROWS" "$N_RECORDS")" "edge parquet rows=$BEFORE_ROWS accepted=$N_RECORDS files=$EDGE_FILES bytes=$EDGE_BYTES"
check "before: templates were mined" "$( [ "$N_TPL" -ge 3 ] && echo 1 || echo 0)" "template ids=$N_TPL (template,service) pairs=$N_PAIR"
SMALL_IDS=$(grep -v '^none,' "$R/before.tpl" | awk -F, '$1 < 1000000' | wc -l | tr -d ' ')
check "before: template ids are content hashes, not the miner's counter" "$(eq "$SMALL_IDS" 0)" "ids below 1000000: $SMALL_IDS of $N_TPL"

# --- the stored parts match the edge's files ---------------------------------------------------------------------
OBJECTS=$(node_parts)
N_OBJ=$(echo "$OBJECTS" | grep -c . || true)
EDGE_HASHES=$(find "$EDGE_STORE" -name '*.parquet' -exec shasum -a 256 {} + | cut -d' ' -f1 | sed 's/$/.parquet/' | sort)
check "central holds exactly the edge's files (by sha256)" "$(eq "$OBJECTS" "$EDGE_HASHES")" "objects=$N_OBJ edge files=$EDGE_FILES"

# --- tail phase: records accepted after the last upload, then SIGKILL ------------------------------------------
send_batches 1000000 "$N_TAIL" tail
check "tail: batches after the last upload answered 200" "$(eq $? 0)" "$N_TAIL records acknowledged, none uploaded"
kill -KILL "$EDGE_PID" 2>/dev/null; wait "$EDGE_PID" 2>/dev/null
# What an intact disk would still hold: the WAL replayed offline (this is the "WAL tail on the edge" half of the RPO).
WAL_TAIL=$("$LOGLESS_BIN" recover --config "$R/edge/config.toml" 2>&1 | sed -nE 's/.*records=([0-9]+).*/\1/p' | awk '{s += $1} END {print s + 0}')   # the WAL and the late spool
log "edge killed with SIGKILL after $N_TAIL acknowledged records that were never uploaded; its WAL replays $WAL_TAIL records"

# --- keep the inventory (central's peer_parts), destroy the edge -------------------------------------------------
sqlite3 "$R/central/data/catalog.sqlite" ".backup '$R/inventory.sqlite'"
INV_N=$(sqlite3 "$R/inventory.sqlite" "select count(*) from peer_parts"); INV_ROWS=$(sqlite3 "$R/inventory.sqlite" "select sum(rows) from peer_parts")
check "inventory (central peer_parts) matches the stored parts" "$( [ "$INV_N" = "$N_OBJ" ] && [ "$INV_ROWS" = "$BEFORE_ROWS" ] && echo 1 || echo 0)" "peer_parts rows=$INV_N, sum(rows)=$INV_ROWS"
# Where each file was, as "<tenant=/index=/hour=/level=/node= directory> <sha256>": a restore must put it back there.
places() { (cd "$1" && find . -name '*.parquet' | sed 's|^\./||' | while read -r f; do echo "$(dirname "$f") $2$(shasum -a 256 "$f" | cut -d' ' -f1)"; done | sort); }
EDGE_PLACES=$(places "$R/edge/data/store" "")
rm -rf "$R/edge/data"
check "edge data directory destroyed" "$(eq "$([ -e "$R/edge/data" ] && echo present || echo gone)" gone)" "removed $R/edge/data"

# --- restore -----------------------------------------------------------------------------------------------------
run_restore() { # dir name
  local t0 t1
  t0=$(now_ms)
  "$LOGLESS_BIN" restore "${RESTORE_FROM[@]}" --to "$1" \
    --inventory "$R/inventory.sqlite" --node "$NODE" --verify --report-json "$1.json" > "$1.out" 2>&1
  RC=$?; t1=$(now_ms); RESTORE_MS=$(( t1 - t0 ))
  cp "$1.out" "$R/$2.out" 2>/dev/null
}
rfield() { sed -nE "s/.*(^| )$2=([^ ]*).*/\2/p" "$R/$1.out" | head -1; }

run_restore "$R/restored1" case1
check "case 1 restore exits 0" "$(eq $RC 0)" "wall ${RESTORE_MS} ms; $(tail -1 "$R/case1.out")"
C1_PARTS=$(rfield case1 restored_parts); C1_ROWS=$(rfield case1 rows); C1_BYTES=$(rfield case1 bytes)
check "case 1: parts, rows, bytes equal the edge's" "$( [ "$C1_PARTS" = "$EDGE_FILES" ] && [ "$C1_ROWS" = "$BEFORE_ROWS" ] && [ "$C1_BYTES" = "$EDGE_BYTES" ] && echo 1 || echo 0)" \
  "restored parts=$C1_PARTS rows=$C1_ROWS bytes=$C1_BYTES; edge parts=$EDGE_FILES rows=$BEFORE_ROWS bytes=$EDGE_BYTES"
check "case 1: zero mismatches, missing, unreadable" "$( [ "$(rfield case1 hash_mismatch)$(rfield case1 size_mismatch)$(rfield case1 rows_mismatch)$(rfield case1 unreadable)$(rfield case1 fetch_failed)$(rfield case1 missing)$(rfield case1 reverify_bad)" = 0000000 ] && echo 1 || echo 0)" \
  "$(grep -E '^hash_mismatch' "$R/case1.out") reverify_bad=$(rfield case1 reverify_bad)"
by_template "$R/restored1/store" > "$R/after.tpl"; by_pair "$R/restored1/store" > "$R/after.pair"; dict_keys "$R/restored1/store" > "$R/after.dict"
cat_templates "$R/restored1/catalog.sqlite" > "$R/after.cat"
check "case 1: per-template row counts identical" "$(eq "$(cat "$R/before.tpl")" "$(cat "$R/after.tpl")")" "$(wc -l < "$R/after.tpl" | tr -d ' ') groups ($N_TPL template ids + none): $(tr '\n' ' ' < "$R/after.tpl")"
check "case 1: per-(template,service) row counts identical" "$(eq "$(cat "$R/before.pair")" "$(cat "$R/after.pair")")" "$(wc -l < "$R/after.pair" | tr -d ' ') groups"
check "case 1: catalog sightings (template_id,rows) identical" "$(eq "$(cat "$R/before.cat")" "$(cat "$R/after.cat")")" "$(wc -l < "$R/after.cat" | tr -d ' ') template ids"
check "case 1: dictionary (template_id,service) keys identical" "$(eq "$(cat "$R/before.dict")" "$(cat "$R/after.dict")")" "$(wc -l < "$R/after.dict" | tr -d ' ') keys (dictionary text differs by design: exemplar, not mined)"
RESTORED_PLACES=$(places "$R/restored1/store" "")
check "case 1: every part is back in the directory it was written in" "$( [ "$RESTORED_PLACES" = "$EDGE_PLACES" ] && [ "$(echo "$EDGE_PLACES" | grep -c "^tenant=default/index=default/hour=[^/]*/level=[^/]*/node=$NODE ")" = "$EDGE_FILES" ] && echo 1 || echo 0)" \
  "$(echo "$RESTORED_PLACES" | cut -d' ' -f1 | sort -u | wc -l | tr -d ' ') directories, $(echo "$RESTORED_PLACES" | grep -c .) files under tenant=default/index=default/hour=<event hour>/level=/node=$NODE"
CAT_KEYS=$(sqlite3 "$R/restored1/catalog.sqlite" "select count(*) from files where tenant = 'default' and idx = 'default' and node = '$NODE' and min_ts / 3600000000000 = event_hour and max_ts / 3600000000000 = event_hour")
check "case 1: catalog rows carry tenant, index, node and the event hour of their rows" "$(eq "$CAT_KEYS" "$EDGE_FILES")" "rows whose event-time range lies in their event_hour: $CAT_KEYS of $EDGE_FILES"
CAT_FILES=$(sqlite3 "$R/restored1/catalog.sqlite" "select count(*), sum(rows) from files" | tr '|' ' ')
check "case 1: rebuilt catalog files table matches" "$(eq "$CAT_FILES" "$EDGE_FILES $BEFORE_ROWS")" "files, rows = $CAT_FILES"
check "RPO: records acknowledged after the last upload are not in the restore" "$( [ "$C1_ROWS" = "$BEFORE_ROWS" ] && [ "$(total_rows "$R/restored1/store")" = "$N_RECORDS" ] && echo 1 || echo 0)" \
  "restored rows=$C1_ROWS = the $N_RECORDS uploaded; $N_TAIL acknowledged after the last upload are absent (the killed edge's WAL replayed $WAL_TAIL of them)"

# --- case 2: one stored part has a flipped byte -------------------------------------------------------------------
VICTIM=$(sqlite3 "$R/inventory.sqlite" "select name from peer_parts order by rows desc limit 1")
VICTIM_ROWS=$(sqlite3 "$R/inventory.sqlite" "select rows from peer_parts where name = '$VICTIM'")
cat_part "$VICTIM" > "$R/victim.orig"
python3 -c '
import sys
b = bytearray(open(sys.argv[1], "rb").read()); b[len(b) // 2] ^= 0xFF
open(sys.argv[2], "wb").write(b)' "$R/victim.orig" "$R/victim.flipped"
put_part "$R/victim.flipped" "$VICTIM"
run_restore "$R/restored2" case2
check "case 2 (flipped byte) exits 1" "$(eq $RC 1)" "wall ${RESTORE_MS} ms; $(tail -1 "$R/case2.out" | cut -c1-120)"
check "case 2: the damaged part is named, nothing else is" "$( [ "$(rfield case2 hash_mismatch)" = 1 ] && grep -q "  $VICTIM: sha256" "$R/case2.out" && [ "$(rfield case2 missing)" = 0 ] && echo 1 || echo 0)" \
  "hash_mismatch=$(rfield case2 hash_mismatch) victim=${VICTIM:0:16}…"
check "case 2: the other parts restored, the damaged one was not installed" "$( [ "$(rfield case2 restored_parts)" = $(( EDGE_FILES - 1 )) ] && [ "$(rfield case2 rows)" = $(( BEFORE_ROWS - VICTIM_ROWS )) ] && [ ! -e "$(find "$R/restored2/store" -name "$VICTIM" | head -1)" ] && echo 1 || echo 0)" \
  "restored parts=$(rfield case2 restored_parts) rows=$(rfield case2 rows) (edge $EDGE_FILES parts, $BEFORE_ROWS rows; victim had $VICTIM_ROWS rows)"
put_part "$R/victim.orig" "$VICTIM"

# --- case 3: one stored part deleted -------------------------------------------------------------------------------
GONE=$(sqlite3 "$R/inventory.sqlite" "select name from peer_parts where name != '$VICTIM' order by rows asc limit 1")
GONE_ROWS=$(sqlite3 "$R/inventory.sqlite" "select rows from peer_parts where name = '$GONE'")
rm_part "$GONE"
run_restore "$R/restored3" case3
check "case 3 (part deleted) exits 1" "$(eq $RC 1)" "wall ${RESTORE_MS} ms; $(tail -1 "$R/case3.out" | cut -c1-120)"
check "case 3: the missing part is named, nothing else is" "$( [ "$(rfield case3 missing)" = 1 ] && grep -q "  $GONE: in the inventory" "$R/case3.out" && [ "$(rfield case3 hash_mismatch)" = 0 ] && echo 1 || echo 0)" \
  "missing=$(rfield case3 missing) gone=${GONE:0:16}…"
check "case 3: every other part restored" "$( [ "$(rfield case3 restored_parts)" = $(( EDGE_FILES - 1 )) ] && [ "$(rfield case3 rows)" = $(( BEFORE_ROWS - GONE_ROWS )) ] && echo 1 || echo 0)" \
  "restored parts=$(rfield case3 restored_parts) rows=$(rfield case3 rows) (missing part had $GONE_ROWS rows)"
put_part "$R/victim.orig" "$VICTIM"

# --- case 4: a restore into a directory that already holds a store is refused --------------------------------------
"$LOGLESS_BIN" restore "${RESTORE_FROM[@]}" --to "$R/restored1" > "$R/case4.out" 2>&1
check "case 4: restoring over an existing store is refused" "$(eq $? 1)" "$(head -1 "$R/case4.out" | cut -c1-140)"

# --- report ------------------------------------------------------------------------------------------------------
echo
echo "case 1 output:"; sed 's/^/  /' "$R/case1.out"
echo "case 2 output:"; sed 's/^/  /' "$R/case2.out"
echo "case 3 output:"; sed 's/^/  /' "$R/case3.out"
echo
printf '%-58s %-5s %s\n' CHECK RESULT DETAIL
printf '%-58s %-5s %s\n' "-----" "------" "------"
while IFS=$'\t' read -r name status detail; do printf '%-58s %-5s %s\n' "$name" "$status" "$detail"; done < "$RESULTS"
fails=$(grep -c $'\tFAIL\t' "$RESULTS" || true)
echo
echo "elapsed $(( ($(now_ms) - T0) / 1000 )) s; $(grep -c $'\tPASS\t' "$RESULTS") passed, $fails failed"
if [ "$fails" = 0 ]; then echo "DRILL PASS"; exit 0; else echo "DRILL FAIL"; exit 1; fi
