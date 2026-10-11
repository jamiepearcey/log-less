#!/usr/bin/env bash
# Parts replication drill (no MinIO): an edge ingests and uploads its Parquet parts to central A; central B is a
# replica of A and copies the parts it lacks by catalog diff; one run is killed midway and the next resumes without
# fetching a finished part again; B's catalog and bytes are compared with A's exactly; then A's data directory and the
# edge's are destroyed and `logless restore` rebuilds the edge's store from B alone.
#
# Usage: scripts/parts-replicate-drill.sh        See docs/peer-sync.md ("Replicating parts between centrals"),
#        docs/restore.md and docs/evidence/parts-replication.md.
# Env:   LOGLESS_BIN (default $CARGO_TARGET_DIR/debug/logless)  DUCKDB_BIN (duckdb)  N_RECORDS (6000)  WAL_SEGMENT (131072)
#        KILL_WINDOW_SECS (10: B's rate limit is set so the whole copy would take about this long)  KILL_AFTER_PARTS (3)
#        SYNC_TIMEOUT (90)  KEEP (0)
# Everything runs on random free ports in a throwaway directory with random credentials and is removed on exit.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
LOGLESS_BIN="${LOGLESS_BIN:-${CARGO_TARGET_DIR:-$HERE/../target}/debug/logless}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
N_RECORDS="${N_RECORDS:-6000}"
WAL_SEGMENT="${WAL_SEGMENT:-131072}"
KILL_WINDOW_SECS="${KILL_WINDOW_SECS:-10}"
KILL_AFTER_PARTS="${KILL_AFTER_PARTS:-3}"
SYNC_TIMEOUT="${SYNC_TIMEOUT:-90}"
KEEP="${KEEP:-0}"
NODE="edge1"   # parts live under parts/<node>/ on a central's disk (ADR 0001, 0003) and keep that node on the replica

for tool in "$LOGLESS_BIN" "$DUCKDB_BIN" curl python3 openssl sqlite3 shasum; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing: $tool" >&2; exit 2; }
done

R="$(mktemp -d "${TMPDIR:-/tmp}/logless-replicate.XXXXXX")"
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

render_config() { # data_dir, overrides, extra  (same helper as restore-drill.sh)
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
log "run dir $R, logless $("$LOGLESS_BIN" --version 2>/dev/null || echo "$LOGLESS_BIN"), no MinIO"

A_DATA="$R/a/data"; B_DATA="$R/b/data"
A_PARTS="$A_DATA/parts"; B_PARTS="$B_DATA/parts"

# --- central A (the source: takes the edge's parts, and lets a replica copy them) --------------------------------
A_PEER=$(free_port); A_ADMIN=$(free_port)
EDGE_PEER_TOKEN="$(rand 24)"; A_TOKEN="$(rand 24)"; REPL_TOKEN="$(rand 24)"
mkdir -p "$R/a"; : > "$R/a/empty.log"
render_config "$A_DATA" "
peer|enabled|true
peer|addr|\"127.0.0.1:$A_PEER\"
peer|nodes|[{ id = \"$NODE\", token = \"$EDGE_PEER_TOKEN\" }]
peer.parts|enabled|true
peer.parts|replication_tokens|[\"$REPL_TOKEN\"]
admin|enabled|true
admin|addr|\"127.0.0.1:$A_ADMIN\"
admin|token|\"$A_TOKEN\"
" "" > "$R/a/config.toml"
"$LOGLESS_BIN" run --config "$R/a/config.toml" --tail "$R/a/empty.log" > "$R/a/agent.log" 2>&1 &
A_PID=$!; PIDS="$PIDS $A_PID"
wait_http "http://127.0.0.1:$A_ADMIN/" 30 || { echo "central A did not start" >&2; cat "$R/a/agent.log" >&2; exit 1; }

# --- edge --------------------------------------------------------------------------------------------------------
OTLP_PORT=$(free_port); EDGE_ADMIN=$(free_port); EDGE_TOKEN="$(rand 24)"
mkdir -p "$R/edge"; : > "$R/edge/empty.log"
BUCKETS=$(python3 -c '
for i, s in enumerate([0] + list(range(3, 24, 2))):
    print(f"[[tenants.indexes.retention]]\nname = \"sev{s:02d}\"\nmin_severity = {s}\nretention = \"1day\"\n")')
render_config "$R/edge/data" "
node|id|\"$NODE\"
storage|wal_segment_bytes|$WAL_SEGMENT
otlp|enabled|true
otlp|addr|\"127.0.0.1:$OTLP_PORT\"
admin|enabled|true
admin|addr|\"127.0.0.1:$EDGE_ADMIN\"
admin|token|\"$EDGE_TOKEN\"
" "
[[destinations]]
type = \"logless\"
url = \"http://127.0.0.1:$A_PEER\"
token = \"$EDGE_PEER_TOKEN\"
min_severity = 0
fidelity = 100
anomaly_boost = false
" | python3 -c '
import sys
skip = False
for line in sys.stdin.read().splitlines():
    if line.strip() == "[[tenants.indexes.retention]]":   # the default buckets of the default index; replaced below
        skip = True
        continue
    if skip and line.strip().startswith("["):
        skip = False
    if not skip:
        print(line)' > "$R/edge/config.toml"
echo "$BUCKETS" >> "$R/edge/config.toml"
start_edge() {
  "$LOGLESS_BIN" run --config "$R/edge/config.toml" --tail "$R/edge/empty.log" >> "$R/edge/agent.log" 2>&1 &
  EDGE_PID=$!; PIDS="$PIDS $EDGE_PID"
  wait_http "http://127.0.0.1:$EDGE_ADMIN/" 30 || { echo "edge did not start" >&2; tail -20 "$R/edge/agent.log" >&2; exit 1; }
  wait_http "http://127.0.0.1:$OTLP_PORT/" 30
}
start_edge
log "central A: peer $A_PEER admin $A_ADMIN; edge: otlp $OTLP_PORT admin $EDGE_ADMIN"

# --- send --------------------------------------------------------------------------------------------------------
# Six message shapes over two services, so the miner mints several template ids and the per-template counts mean something.
# Twelve severities, and twelve level buckets in the edge config, so the store holds a dozen parts rather than three.
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
levels = [(s, f"SEV{s:02d}") for s in range(1, 24, 2)]   # 12 severities, one level bucket each (see the edge config)
services = ["drill-api", "drill-worker"]
now = time.time_ns()
for b, start in enumerate(range(start0, start0 + count, 500)):
    records = []
    for i in range(start, min(start + 500, start0 + count)):
        num, text = levels[i % len(levels)]
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

# A clean stop plus an offline merge leaves nothing only in the WAL, so every accepted record is in a Parquet part.
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
upload_parts; check "edge uploaded its parts to A" "$(eq $? 0)" "state=$PSTATE files_total=$PTOTAL uploaded=$PUP rejected=$PREJ"

# --- BEFORE snapshot (the edge's own store), then A's facts ------------------------------------------------------
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
EDGE_FILES=$(find "$EDGE_STORE" -name '*.parquet' | wc -l | tr -d ' ')
EDGE_BYTES=$(find "$EDGE_STORE" -name '*.parquet' -exec stat -f %z {} + | awk '{s+=$1} END {print s}')
check "before: every accepted record is in a part" "$(eq "$BEFORE_ROWS" "$N_RECORDS")" "edge parquet rows=$BEFORE_ROWS accepted=$N_RECORDS files=$EDGE_FILES bytes=$EDGE_BYTES"

cat_dump() { sqlite3 "$1" "select node, name, size, rows, min_observed, max_observed, origin, tier from peer_parts order by node, name"; }
file_hashes() { (cd "$1" && find . -type f -name '*.parquet' | sort | xargs shasum -a 256); }   # path, so the node is part of the line
count_rows() { local n; n=$(sqlite3 -cmd ".timeout 3000" "$1" "select count(*) from peer_parts" 2>/dev/null); echo "${n:-0}"; }
post_replicate() { curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST -H "Authorization: Bearer $B_TOKEN" -H 'Content-Type: application/json' -d '' "http://127.0.0.1:$B_ADMIN/admin/v1/parts/replicate"; }
N=$(count_rows "$A_DATA/catalog.sqlite")
A_BYTES=$(sqlite3 "$A_DATA/catalog.sqlite" "select sum(size) from peer_parts")
cat_dump "$A_DATA/catalog.sqlite" > "$R/a.cat"; file_hashes "$A_PARTS" > "$R/a.files"
check "A holds exactly the edge's files (by sha256)" "$( [ "$N" = "$EDGE_FILES" ] && [ "$A_BYTES" = "$EDGE_BYTES" ] && [ "$(cut -d' ' -f1 "$R/a.files" | sort | tr '\n' ' ')" = "$(find "$EDGE_STORE" -name '*.parquet' -exec shasum -a 256 {} + | cut -d' ' -f1 | sort | tr '\n' ' ')" ] && echo 1 || echo 0)" \
  "A catalog rows=$N bytes=$A_BYTES; edge files=$EDGE_FILES bytes=$EDGE_BYTES"
check "enough parts to interrupt a copy midway" "$( [ "$N" -ge $(( KILL_AFTER_PARTS + 3 )) ] && echo 1 || echo 0)" "$N parts, kill after $KILL_AFTER_PARTS recorded"

# --- credentials on A's replica paths ------------------------------------------------------------------------------
code() { curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@"; }
CAT_URL="http://127.0.0.1:$A_PEER/v1/parts/replica/catalog"
FIRST=$(sqlite3 "$A_DATA/catalog.sqlite" "select node || '/' || name from peer_parts order by node, name limit 1")
BLOB_URL="http://127.0.0.1:$A_PEER/v1/parts/replica/blob/$FIRST"
c_none=$(code "$CAT_URL"); c_wrong=$(code -H "Authorization: Bearer $(rand 24)" "$CAT_URL")
c_node_cat=$(code -H "Authorization: Bearer $EDGE_PEER_TOKEN" "$CAT_URL"); c_node_blob=$(code -H "Authorization: Bearer $EDGE_PEER_TOKEN" "$BLOB_URL")
c_repl_presign=$(code -X POST -H "Authorization: Bearer $REPL_TOKEN" -H 'Content-Type: application/json' -d '{}' "http://127.0.0.1:$A_PEER/v1/parts/presign")
c_repl_cap=$(code -H "Authorization: Bearer $REPL_TOKEN" "http://127.0.0.1:$A_PEER/.well-known/logless")
c_repl_ok=$(code -H "Authorization: Bearer $REPL_TOKEN" "$CAT_URL?limit=1")
A_REFUSED=$(admin "$A_ADMIN" "$A_TOKEN" GET /admin/v1/sync | jq_py 'v["parts_store"]["refused_replica"]')
check "replica paths: only a replication token opens them" "$( [ "$c_none/$c_wrong/$c_node_cat/$c_node_blob" = "401/403/403/403" ] && [ "$c_repl_presign/$c_repl_cap/$c_repl_ok" = "403/403/200" ] && [ "$A_REFUSED" = 4 ] && echo 1 || echo 0)" \
  "no token=$c_none wrong=$c_wrong node token on catalog=$c_node_cat on blob=$c_node_blob; replication token: presign=$c_repl_presign capability=$c_repl_cap catalog=$c_repl_ok; A refused_replica=$A_REFUSED"

# --- central B (the replica) ---------------------------------------------------------------------------------------
# Rate-limited so the copy takes about KILL_WINDOW_SECS: there is time to kill it midway, and the byte counts are small.
RATE=$(( A_BYTES / KILL_WINDOW_SECS )); [ "$RATE" -lt 2048 ] && RATE=2048
B_PEER=$(free_port); B_ADMIN=$(free_port); B_TOKEN="$(rand 24)"; B_PEER_TOKEN="$(rand 24)"
mkdir -p "$R/b"; : > "$R/b/empty.log"
render_config "$B_DATA" "
peer|enabled|true
peer|addr|\"127.0.0.1:$B_PEER\"
peer|tokens|[\"$B_PEER_TOKEN\"]
peer.parts|enabled|true
admin|enabled|true
admin|addr|\"127.0.0.1:$B_ADMIN\"
admin|token|\"$B_TOKEN\"
" "
[peer.parts.replicate]
url = \"http://127.0.0.1:$A_PEER\"
token = \"$REPL_TOKEN\"
interval_secs = 0
bytes_per_sec = $RATE
page_size = 7
" > "$R/b/config.toml"
start_b() {
  "$LOGLESS_BIN" run --config "$R/b/config.toml" --tail "$R/b/empty.log" >> "$R/b/agent.log" 2>&1 &
  B_PID=$!; PIDS="$PIDS $B_PID"
  wait_http "http://127.0.0.1:$B_ADMIN/" 30 || { echo "central B did not start" >&2; tail -20 "$R/b/agent.log" >&2; exit 1; }
}
start_b
log "central B (replica of A, $RATE B/s): peer $B_PEER admin $B_ADMIN"
check "B starts empty" "$(eq "$(count_rows "$B_DATA/catalog.sqlite")" 0)" "peer_parts rows=0, parts dir: $(find "$B_PARTS" -type f 2>/dev/null | wc -l | tr -d ' ') files"

rep_field() { admin "$B_ADMIN" "$B_TOKEN" GET /admin/v1/parts/replicate | jq_py "v['replication']['run']['$1']"; }
wait_run_done() { # runs wanted
  local deadline=$(( $(date +%s) + 120 )) st
  while [ "$(date +%s)" -lt "$deadline" ]; do
    st=$(admin "$B_ADMIN" "$B_TOKEN" GET /admin/v1/parts/replicate | jq_py '"%s %s" % (v["replication"]["state"], v["replication"]["runs"])' 2>/dev/null)
    [ "$st" = "idle $1" ] && return 0
    sleep 0.2
  done
  return 1
}
served() { admin "$A_ADMIN" "$A_TOKEN" GET /admin/v1/sync | jq_py 'v["parts_store"]["replica_served"]'; }

part_size_of() { # a .fetch-<sha256>.tmp path: the size A's catalog gives that part
  local sha; sha=$(basename "$1" .tmp); sha=${sha#.fetch-}
  sqlite3 "$R/a.snapshot.sqlite" "select size from peer_parts where name = '$sha.parquet'" 2>/dev/null || echo 0
}
sqlite3 "$A_DATA/catalog.sqlite" ".backup '$R/a.snapshot.sqlite'"

# --- run 1, killed midway ------------------------------------------------------------------------------------------
start_code=$(post_replicate)
killed=0
deadline=$(( $(date +%s) + 60 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  partial=$(find "$B_PARTS" -name '.fetch-*.tmp' -size +0 2>/dev/null | head -1)
  # Only a copy that is at most half the part's size, and only after a few parts are done.
  if [ -n "$partial" ] && [ $(( 2 * $(stat -f %z "$partial" 2>/dev/null || echo 0) )) -le "$(part_size_of "$partial")" ] && [ "$(count_rows "$B_DATA/catalog.sqlite")" -ge "$KILL_AFTER_PARTS" ]; then
    kill -KILL "$B_PID" 2>/dev/null; wait "$B_PID" 2>/dev/null; killed=1; break
  fi
  sleep 0.01
done
FILES_AT_KILL=$(find "$B_PARTS" -name '*.parquet' | wc -l | tr -d ' ')
ROWS_AT_KILL=$(count_rows "$B_DATA/catalog.sqlite")
PARTIALS=$(find "$B_PARTS" -name '.fetch-*.tmp' -size +0 | wc -l | tr -d ' ')
ZERO_TMP=$(find "$B_PARTS" -name '.fetch-*.tmp' -size 0 | wc -l | tr -d ' ')
PARTIAL_BYTES=$(find "$B_PARTS" -name '.fetch-*.tmp' -exec stat -f %z {} + | awk '{s+=$1} END {print s+0}')
PARTIAL_WHOLE=0   # a copy that was already whole when the process died (its rename had not happened yet) needs no request
for f in $(find "$B_PARTS" -name '.fetch-*.tmp' -size +0); do [ "$(stat -f %z "$f")" = "$(part_size_of "$f")" ] && PARTIAL_WHOLE=$(( PARTIAL_WHOLE + 1 )); done
SERVED1=$(served)
check "run 1 started on request and was killed midway (SIGKILL)" "$( [ "$start_code" = 202 ] && [ "$killed" = 1 ] && [ "$FILES_AT_KILL" -lt "$N" ] && [ "$PARTIALS" -ge 1 ] && echo 1 || echo 0)" \
  "POST=$start_code; at the kill B had $FILES_AT_KILL of $N parts installed ($ROWS_AT_KILL catalogued) and $PARTIALS partial copy of $PARTIAL_BYTES bytes (+$ZERO_TMP empty); A had started $SERVED1 part responses"

# --- run 2: resumes -------------------------------------------------------------------------------------------------
start_b
post2=$(post_replicate)
wait_run_done 1; done2=$?
F() { rep_field "$1"; }
check "run 2 completed without error" "$( [ "$post2" = 202 ] && [ "$done2" = 0 ] && [ "$(F complete)" = True ] && [ "$(F error)" = None ] && echo 1 || echo 0)" "POST=$post2 complete=$(F complete) error=$(F error)"
EXP_ADOPTED=$(( FILES_AT_KILL - ROWS_AT_KILL )); EXP_FETCHED=$(( N - FILES_AT_KILL ))
check "run 2 did not fetch a finished part again" "$( [ "$(F listed)" = "$N" ] && [ "$(F present)" = "$ROWS_AT_KILL" ] && [ "$(F adopted)" = "$EXP_ADOPTED" ] && [ "$(F fetched)" = "$EXP_FETCHED" ] && echo 1 || echo 0)" \
  "listed=$(F listed) present=$(F present) adopted=$(F adopted) fetched=$(F fetched); expected listed=$N present=$ROWS_AT_KILL (catalogued at the kill) adopted=$EXP_ADOPTED (file installed, row not yet) fetched=$EXP_FETCHED"
check "run 2 resumed the cut part from the byte it had" "$( [ "$(F resumed)" = "$PARTIALS" ] && [ "$(F resumed_bytes)" = "$PARTIAL_BYTES" ] && echo 1 || echo 0)" \
  "resumed=$(F resumed) resumed_bytes=$(F resumed_bytes); the kill left $PARTIALS partial copy of $PARTIAL_BYTES bytes"
SERVED2=$(served)
SLACK=$(( ZERO_TMP > 0 ? 1 : 0 ))
EXP_SERVED=$(( N + PARTIALS - PARTIAL_WHOLE ))
check "A served each part once, the cut one twice" "$( [ "$SERVED2" -ge "$EXP_SERVED" ] && [ "$SERVED2" -le $(( EXP_SERVED + SLACK )) ] && echo 1 || echo 0)" \
  "A started $SERVED2 part responses over both runs; expected $N parts + $PARTIALS resumed - $PARTIAL_WHOLE already whole (+ up to $SLACK for an empty temporary file at the kill)"
check "run 2: nothing damaged, unservable, invalid or only here" "$( [ "$(F damaged)$(F unservable)$(F invalid)$(F only_here)$(F tier_differs)" = 00000 ] && echo 1 || echo 0)" \
  "damaged=$(F damaged) unservable=$(F unservable) invalid=$(F invalid) only_here=$(F only_here) tier_differs=$(F tier_differs)"

# --- compare B with A: catalog and bytes, exactly ----------------------------------------------------------------------
cat_dump "$B_DATA/catalog.sqlite" > "$R/b.cat"; file_hashes "$B_PARTS" > "$R/b.files"
B_N=$(count_rows "$B_DATA/catalog.sqlite"); B_BYTES=$(sqlite3 "$B_DATA/catalog.sqlite" "select sum(size) from peer_parts")
check "B's catalog equals A's (node, name, size, rows, time range, origin, tier)" "$( [ "$B_N" = "$N" ] && cmp -s "$R/a.cat" "$R/b.cat" && echo 1 || echo 0)" \
  "A rows=$N, B rows=$B_N, $(wc -l < "$R/b.cat" | tr -d ' ') lines compared"
check "B's files equal A's byte for byte (sha256 of every part)" "$(eq "$(cat "$R/a.files")" "$(cat "$R/b.files")")" \
  "$(wc -l < "$R/b.files" | tr -d ' ') files, $B_BYTES bytes on B, $A_BYTES on A"
STRAY=$(find "$B_PARTS" -type f ! -name '*.parquet' | wc -l | tr -d ' ')
B_USED=$(admin "$B_ADMIN" "$B_TOKEN" GET /admin/v1/sync | jq_py 'v["parts_store"]["used_bytes"]')
check "B left no partial copy, and its disk accounting matches" "$( [ "$STRAY" = 0 ] && [ "$B_USED" = "$A_BYTES" ] && echo 1 || echo 0)" "non-part files on B=$STRAY, B used_bytes=$B_USED"
AUDIT=$(sqlite3 "$B_DATA/catalog.sqlite" "select count(*) from peer_parts_audit where action = 'replicate' and outcome = 'ok'")
check "every copied part is in B's audit trail with its source" "$(eq "$AUDIT" "$N")" "audit rows 'replicate'=$AUDIT"

# --- run 3: nothing left to do ---------------------------------------------------------------------------------------
post_replicate > /dev/null
wait_run_done 2; done3=$?
SERVED3=$(served)
check "run 3 finds nothing to fetch" "$( [ "$done3" = 0 ] && [ "$(F listed)" = "$N" ] && [ "$(F present)" = "$N" ] && [ "$(F fetched)" = 0 ] && [ "$SERVED3" = "$SERVED2" ] && echo 1 || echo 0)" \
  "listed=$(F listed) present=$(F present) fetched=$(F fetched); A part responses $SERVED2 -> $SERVED3"

# --- destroy the first central and the edge; restore from B alone ---------------------------------------------------
stop_within "$A_PID" 15; check "central A stops cleanly" "$(eq $? 0)" "${STOP_MS} ms"
stop_within "$EDGE_PID" 15; check "edge stops cleanly" "$(eq $? 0)" "${STOP_MS} ms"
rm -rf "$A_DATA" "$R/edge/data"
check "A's and the edge's data directories destroyed" "$(eq "$([ -e "$A_DATA" ] || [ -e "$R/edge/data" ] && echo present || echo gone)" gone)" "removed $A_DATA and $R/edge/data"

# With the source gone a run reports it and changes nothing.
post_replicate > /dev/null
wait_run_done 3; done4=$?
check "with A gone, a run says so and changes nothing" "$( [ "$done4" = 0 ] && [ "$(F complete)" = False ] && [[ "$(F error)" == *"could not be reached"* ]] && [ "$(count_rows "$B_DATA/catalog.sqlite")" = "$N" ] && echo 1 || echo 0)" \
  "complete=$(F complete) error=$(F error | cut -c1-90); B rows=$(count_rows "$B_DATA/catalog.sqlite")"

sqlite3 "$B_DATA/catalog.sqlite" ".backup '$R/inventory.sqlite'"
t0=$(now_ms)
"$LOGLESS_BIN" restore --from "$B_PARTS/$NODE" --to "$R/restored" --inventory "$R/inventory.sqlite" --node "$NODE" --verify --report-json "$R/restored.json" > "$R/restore.out" 2>&1
RC=$?; RESTORE_MS=$(( $(now_ms) - t0 ))
rfield() { sed -nE "s/.*(^| )$1=([^ ]*).*/\2/p" "$R/restore.out" | head -1; }
check "restore from B exits 0" "$(eq $RC 0)" "wall ${RESTORE_MS} ms; $(tail -1 "$R/restore.out")"
R_PARTS=$(rfield restored_parts); R_ROWS=$(rfield rows); R_BYTES=$(rfield bytes)
check "restore: parts, rows, bytes equal the destroyed edge's" "$( [ "$R_PARTS" = "$EDGE_FILES" ] && [ "$R_ROWS" = "$BEFORE_ROWS" ] && [ "$R_BYTES" = "$EDGE_BYTES" ] && echo 1 || echo 0)" \
  "restored parts=$R_PARTS rows=$R_ROWS bytes=$R_BYTES; edge parts=$EDGE_FILES rows=$BEFORE_ROWS bytes=$EDGE_BYTES"
check "restore: zero mismatches, missing, unreadable" "$( [ "$(rfield hash_mismatch)$(rfield size_mismatch)$(rfield rows_mismatch)$(rfield unreadable)$(rfield fetch_failed)$(rfield missing)$(rfield reverify_bad)" = 0000000 ] && echo 1 || echo 0)" \
  "$(grep -E '^hash_mismatch' "$R/restore.out") reverify_bad=$(rfield reverify_bad)"
by_template "$R/restored/store" > "$R/after.tpl"; by_pair "$R/restored/store" > "$R/after.pair"; dict_keys "$R/restored/store" > "$R/after.dict"
cat_templates "$R/restored/catalog.sqlite" > "$R/after.cat"
check "restore: per-template row counts identical" "$(eq "$(cat "$R/before.tpl")" "$(cat "$R/after.tpl")")" "$(wc -l < "$R/after.tpl" | tr -d ' ') groups"
check "restore: per-(template,service) row counts identical" "$(eq "$(cat "$R/before.pair")" "$(cat "$R/after.pair")")" "$(wc -l < "$R/after.pair" | tr -d ' ') groups"
check "restore: catalog sightings (template_id,rows) and dictionary keys identical" "$( [ "$(cat "$R/before.cat")" = "$(cat "$R/after.cat")" ] && [ "$(cat "$R/before.dict")" = "$(cat "$R/after.dict")" ] && echo 1 || echo 0)" \
  "$(wc -l < "$R/after.cat" | tr -d ' ') templates, $(wc -l < "$R/after.dict" | tr -d ' ') keys"

stop_within "$B_PID" 15; check "central B stops cleanly" "$(eq $? 0)" "${STOP_MS} ms"
# tracing colours the level when it likes; strip escapes before looking for the ERROR level.
ERRS=$(cat "$R"/a/agent.log "$R"/b/agent.log | sed -E $'s/\x1b\\[[0-9;]*m//g' | grep -c ' ERROR ' || true)
check "no ERROR lines in either central's log" "$(eq "$ERRS" 0)" "$ERRS ERROR lines"

# --- report ------------------------------------------------------------------------------------------------------
echo
echo "restore output:"; sed 's/^/  /' "$R/restore.out"
echo
printf '%-70s %-5s %s\n' CHECK RESULT DETAIL
printf '%-70s %-5s %s\n' "-----" "------" "------"
while IFS=$'\t' read -r name status detail; do printf '%-70s %-5s %s\n' "$name" "$status" "$detail"; done < "$RESULTS"
fails=$(grep -c $'\tFAIL\t' "$RESULTS" || true)
echo
echo "elapsed $(( ($(now_ms) - T0) / 1000 )) s; $(grep -c $'\tPASS\t' "$RESULTS") passed, $fails failed"
if [ "$fails" = 0 ]; then echo "DRILL PASS"; exit 0; else echo "DRILL FAIL"; exit 1; fi
