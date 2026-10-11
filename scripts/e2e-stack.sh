#!/usr/bin/env bash
# End-to-end run of the whole stack on one machine: MinIO, a central log-less (peer receiver + Parquet parts to
# MinIO) and an edge log-less (OTLP/HTTP, Splunk HEC and the Sentry proxy, syncing to central). Sends known
# records, waits for sync, uploads parts, then checks counts, part checksums, peer counters, logs and shutdown.
# Prints a PASS/FAIL table and exits 1 if any check failed. Everything runs on random free ports in a throwaway
# directory, with random credentials, and is removed on exit (KEEP=1 keeps the directory).
#
# Usage: scripts/e2e-stack.sh            See docs/e2e-stack.md.
# Env:   LOGLESS_BIN   (default $CARGO_TARGET_DIR/debug/logless, else ./target/debug/logless)
#        MINIO_BIN     (/opt/homebrew/bin/minio)   MC_BIN (mc)   DUCKDB_BIN (duckdb)
#        SENTRY_DEMO   (~/projects/logless-studio/scripts/sentry-demo)
#        N_OTLP (3000) N_HEC (3000) SENTRY_REQUESTS (40) SYNC_TIMEOUT (90) KEEP (0)
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
LOGLESS_BIN="${LOGLESS_BIN:-${CARGO_TARGET_DIR:-$HERE/../target}/debug/logless}"
MINIO_BIN="${MINIO_BIN:-/opt/homebrew/bin/minio}"
MC_BIN="${MC_BIN:-mc}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
SENTRY_DEMO="${SENTRY_DEMO:-$HOME/projects/logless-studio/scripts/sentry-demo}"
N_OTLP="${N_OTLP:-3000}"
N_HEC="${N_HEC:-3000}"
SENTRY_REQUESTS="${SENTRY_REQUESTS:-40}"
SYNC_TIMEOUT="${SYNC_TIMEOUT:-90}"
KEEP="${KEEP:-0}"
BUCKET="logless-parts"
PREFIX="edges"
NODE="edge-1"   # the edge's node id: its parts live under $PREFIX/$NODE/

for tool in "$LOGLESS_BIN" "$MINIO_BIN" "$MC_BIN" "$DUCKDB_BIN" curl python3 openssl sqlite3 shasum; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing: $tool" >&2; exit 2; }
done

R="$(mktemp -d "${TMPDIR:-/tmp}/logless-e2e.XXXXXX")"
chmod 700 "$R"
PIDS=""          # every process this script started, killed on exit

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
# jq_py EXPR < json: prints EXPR evaluated with `v` bound to the parsed document (null prints None).
jq_py() { python3 -c "import sys, json; v = json.load(sys.stdin); print($1)"; }

RESULTS="$R/results.tsv"
: > "$RESULTS"
check() { # name, ok (0/1), detail
  local status=FAIL
  [ "$2" = 1 ] && status=PASS
  printf '%s\t%s\t%s\n' "$1" "$status" "$3" >> "$RESULTS"
}
skip() { printf '%s\t%s\t%s\n' "$1" "SKIP" "$2" >> "$RESULTS"; }
eq() { [ "$1" = "$2" ] && echo 1 || echo 0; }

wait_http() { # url, seconds
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

# Rewrites `logless init` output: OVERRIDES is lines of `section|key|toml value`; keys missing from a section are
# appended to it. EXTRA is appended verbatim at the end.
render_config() { # data_dir, overrides, extra
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

# Sends SIGTERM and waits up to the limit. Sets STOP_MS; returns 1 if the process was still running. Runs in this
# shell (not a subshell), so the exited child is reaped here and not mistaken for a live one.
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
log "run dir $R, logless $("$LOGLESS_BIN" --version 2>/dev/null || echo "$LOGLESS_BIN")"

# --- MinIO -------------------------------------------------------------------------------------------------------
MINIO_PORT=$(free_port); MINIO_CONSOLE=$(free_port)
MINIO_USER="e2e$(rand 6)"; MINIO_SECRET="$(rand 20)"
MINIO_ROOT_USER="$MINIO_USER" MINIO_ROOT_PASSWORD="$MINIO_SECRET" \
  "$MINIO_BIN" server --quiet --address "127.0.0.1:$MINIO_PORT" --console-address "127.0.0.1:$MINIO_CONSOLE" \
  "$R/minio-data" > "$R/minio.log" 2>&1 &
MINIO_PID=$!; PIDS="$PIDS $MINIO_PID"
wait_http "http://127.0.0.1:$MINIO_PORT/minio/health/live" 30 || { echo "MinIO did not start" >&2; cat "$R/minio.log" >&2; exit 1; }
export MC_CONFIG_DIR="$R/mc"
"$MC_BIN" --config-dir "$R/mc" alias set e2e "http://127.0.0.1:$MINIO_PORT" "$MINIO_USER" "$MINIO_SECRET" > /dev/null
"$MC_BIN" --config-dir "$R/mc" mb "e2e/$BUCKET" > /dev/null
log "minio on $MINIO_PORT, bucket $BUCKET"

# --- central -----------------------------------------------------------------------------------------------------
PEER_PORT=$(free_port); CENTRAL_ADMIN=$(free_port)
PEER_TOKEN="$(rand 24)"; CENTRAL_TOKEN="$(rand 24)"   # PEER_TOKEN is the edge node's token
mkdir -p "$R/central"; : > "$R/central/empty.log"
render_config "$R/central/data" "
peer|enabled|true
peer|addr|\"127.0.0.1:$PEER_PORT\"
peer|nodes|[{ id = \"$NODE\", token = \"$PEER_TOKEN\" }]
peer.parts|enabled|true
peer.parts|endpoint|\"http://127.0.0.1:$MINIO_PORT\"
peer.parts|bucket|\"$BUCKET\"
peer.parts|prefix|\"$PREFIX\"
peer.parts|access_key_id|\"$MINIO_USER\"
peer.parts|secret_access_key|\"$MINIO_SECRET\"
peer.parts|warm_retention_secs|2
peer.parts|cold_retention_secs|2
admin|enabled|true
admin|addr|\"127.0.0.1:$CENTRAL_ADMIN\"
admin|token|\"$CENTRAL_TOKEN\"
" "" > "$R/central/config.toml"
"$LOGLESS_BIN" run --config "$R/central/config.toml" --tail "$R/central/empty.log" > "$R/central/agent.log" 2>&1 &
CENTRAL_PID=$!; PIDS="$PIDS $CENTRAL_PID"
wait_http "http://127.0.0.1:$CENTRAL_ADMIN/" 30 || { echo "central did not start" >&2; cat "$R/central/agent.log" >&2; exit 1; }
log "central: peer $PEER_PORT, admin $CENTRAL_ADMIN"

# --- edge --------------------------------------------------------------------------------------------------------
OTLP_PORT=$(free_port); HEC_PORT=$(free_port); SENTRY_PORT=$(free_port); EDGE_ADMIN=$(free_port)
HEC_TOKEN="$(rand 16)"; EDGE_TOKEN="$(rand 24)"
mkdir -p "$R/edge"; : > "$R/edge/empty.log"
render_config "$R/edge/data" "
storage|wal_segment_bytes|131072
otlp|enabled|true
otlp|addr|\"127.0.0.1:$OTLP_PORT\"
hec|enabled|true
hec|addr|\"127.0.0.1:$HEC_PORT\"
hec|tokens|[\"$HEC_TOKEN\"]
sentry|enabled|true
sentry|addr|\"127.0.0.1:$SENTRY_PORT\"
sentry|forward_events|false
sentry|transaction_sample_rate|0.0
sentry|forward_unknown|false
sentry|spool|false
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
"$LOGLESS_BIN" run --config "$R/edge/config.toml" --tail "$R/edge/empty.log" > "$R/edge/agent.log" 2>&1 &
EDGE_PID=$!; PIDS="$PIDS $EDGE_PID"
wait_http "http://127.0.0.1:$EDGE_ADMIN/" 30 || { echo "edge did not start" >&2; cat "$R/edge/agent.log" >&2; exit 1; }
wait_http "http://127.0.0.1:$HEC_PORT/services/collector/health" 30
log "edge: otlp $OTLP_PORT, hec $HEC_PORT, sentry $SENTRY_PORT, admin $EDGE_ADMIN"

# --- send --------------------------------------------------------------------------------------------------------
# OTLP/HTTP JSON in batches of 500, severities cycling DEBUG/INFO/WARN/ERROR. Bodies are long enough that the edge's
# 128 KiB WAL segments roll and merge into Parquet while it runs, so the parts job has files to upload.
mkdir -p "$R/payloads"
N_OTLP="$N_OTLP" N_HEC="$N_HEC" OUT="$R/payloads" python3 -c '
import json, os, time
out, n_otlp, n_hec = os.environ["OUT"], int(os.environ["N_OTLP"]), int(os.environ["N_HEC"])
levels = [(5, "DEBUG"), (9, "INFO"), (13, "WARN"), (17, "ERROR")]
now = time.time_ns()
for b, start in enumerate(range(0, n_otlp, 500)):
    records = []
    for i in range(start, min(start + 500, n_otlp)):
        num, text = levels[i % 4]
        records.append({"timeUnixNano": str(now + i * 1000), "severityNumber": num, "severityText": text,
                        "body": {"stringValue": f"e2e otlp record seq={i:06d} order={i * 7919 % 100003} took={i % 997}ms path=/api/v1/orders/{i % 50} " + "x" * 160},
                        "attributes": [{"key": "seq", "value": {"intValue": str(i)}}]})
    doc = {"resourceLogs": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "e2e-otlp"}}]},
                             "scopeLogs": [{"logRecords": records}]}]}
    open(f"{out}/otlp-{b:03d}.json", "w").write(json.dumps(doc))
for b, start in enumerate(range(0, n_hec, 500)):
    events = []
    for i in range(start, min(start + 500, n_hec)):
        events.append(json.dumps({"time": time.time(), "sourcetype": "e2e-hec", "event": f"e2e hec event seq={i:06d} user=u{i % 300} status={200 + i % 5} " + "y" * 160,
                                  "fields": {"seq": i}}))
    open(f"{out}/hec-{b:03d}.txt", "w").write("".join(events))
'
send_fail=0
for f in "$R"/payloads/otlp-*.json; do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -H 'Content-Type: application/json' --data-binary "@$f" "http://127.0.0.1:$OTLP_PORT/v1/logs")
  [ "$code" = 200 ] || { send_fail=1; log "OTLP $f -> $code"; }
done
check "otlp batches answered 200" "$(eq $send_fail 0)" "$N_OTLP records over OTLP/HTTP JSON"
send_fail=0
for f in "$R"/payloads/hec-*.txt; do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -H "Authorization: Splunk $HEC_TOKEN" --data-binary "@$f" "http://127.0.0.1:$HEC_PORT/services/collector/event")
  [ "$code" = 200 ] || { send_fail=1; log "HEC $f -> $code"; }
done
check "hec batches answered 200" "$(eq $send_fail 0)" "$N_HEC events over /services/collector/event"

SENTRY_RAN=0
if [ -f "$SENTRY_DEMO/app.mjs" ] && [ -d "$SENTRY_DEMO/node_modules/@sentry/node" ] && command -v node >/dev/null 2>&1; then
  log "sentry: $SENTRY_REQUESTS requests through @sentry/node"
  if (cd "$SENTRY_DEMO" && SENTRY_DSN="http://e2ekey@127.0.0.1:$SENTRY_PORT/1" REQUESTS="$SENTRY_REQUESTS" \
        node app.mjs > "$R/sentry-demo.log" 2>&1); then
    SENTRY_RAN=1
    check "sentry demo ran" 1 "$(tail -1 "$R/sentry-demo.log")"
  else
    check "sentry demo ran" 0 "node app.mjs failed: $(tail -1 "$R/sentry-demo.log")"
  fi
else
  skip "sentry demo ran" "skipped: $SENTRY_DEMO/node_modules/@sentry/node missing (npm install there) or no node"
fi

# --- wait for sync -----------------------------------------------------------------------------------------------
# The edge has synced when every record it wrote has been sent and its spool is empty.
# The edge has synced when everything it received is written, every written record has been sent and its spool is
# empty, twice in a row half a second apart.
received=0; written=0; sent=-1; backlog=-1; stable=0
deadline=$(( $(date +%s) + SYNC_TIMEOUT ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  info=$(admin "$EDGE_ADMIN" "$EDGE_TOKEN" GET /admin/v1/info)
  received=$(echo "$info" | jq_py 'v["ingest"]["received"]' 2>/dev/null || echo 0)
  written=$(echo "$info" | jq_py 'v["ingest"]["written"]' 2>/dev/null || echo 0)
  status=$(admin "$EDGE_ADMIN" "$EDGE_TOKEN" GET /admin/v1/sync)
  sent=$(echo "$status" | jq_py 'v["status"]["sent_records"]' 2>/dev/null || echo -1)
  backlog=$(echo "$status" | jq_py 'v["status"]["backlog_bytes"]' 2>/dev/null || echo -1)
  if [ "$written" -gt 0 ] && [ "$received" = "$written" ] && [ "$sent" = "$written" ] && [ "$backlog" = 0 ]; then
    stable=$(( stable + 1 )); [ "$stable" -ge 2 ] && break
  else
    stable=0
  fi
  sleep 0.5
done
check "edge synced everything it wrote" "$( [ "$stable" -ge 2 ] && echo 1 || echo 0)" \
  "received=$received written=$written sent_records=$sent backlog_bytes=$backlog"

# --- parts upload ------------------------------------------------------------------------------------------------
# Wait for the edge to have merged at least one Parquet file, then upload the range around this run.
files=0
for _ in $(seq 1 60); do
  files=$(admin "$EDGE_ADMIN" "$EDGE_TOKEN" GET /admin/v1/info | jq_py 'v["store"]["files"]' 2>/dev/null || echo 0)
  [ "$files" -gt 0 ] && break
  sleep 0.5
done
FROM=$(( T0 - 3600000 )); TO=$(( $(now_ms) + 3600000 ))
admin "$EDGE_ADMIN" "$EDGE_TOKEN" POST /admin/v1/sync/parts "{\"from_ms\":$FROM,\"to_ms\":$TO}" > /dev/null
pstate=running
for _ in $(seq 1 120); do
  parts=$(admin "$EDGE_ADMIN" "$EDGE_TOKEN" GET /admin/v1/sync | jq_py 'json.dumps(v["status"]["parts"])')
  pstate=$(echo "$parts" | jq_py 'v["state"] if v else "none"')
  [ "$pstate" != running ] && break
  sleep 0.5
done
part() { echo "$parts" | jq_py "(v or {}).get('$1', -1)"; }
uploaded=$(part uploaded); ptotal=$(part files_total); perr=$(part error); prej=$(part rejected)
check "parts job done" "$( [ "$pstate" = done ] && [ "$uploaded" -ge 1 ] && [ "$uploaded" = "$ptotal" ] && [ "$prej" = 0 ] && [ "$perr" = None ] && echo 1 || echo 0)" \
  "state=$pstate files_total=$ptotal uploaded=$uploaded rejected=$prej error=$perr (edge files=$files)"

# Every object in MinIO is named by its content: sha256(object) == name, and the set matches central's catalog.
objects=$("$MC_BIN" --config-dir "$R/mc" ls --json "e2e/$BUCKET/$PREFIX/$NODE/" | python3 -c 'import sys, json; [print(json.loads(l)["key"]) for l in sys.stdin if l.strip()]' | sort)
n_obj=0; bad_obj=0
for o in $objects; do
  n_obj=$(( n_obj + 1 ))
  h=$("$MC_BIN" --config-dir "$R/mc" cat "e2e/$BUCKET/$PREFIX/$NODE/$o" | shasum -a 256 | cut -d' ' -f1)
  [ "$h.parquet" = "$o" ] || { bad_obj=$(( bad_obj + 1 )); log "MISMATCH $o sha256=$h"; }
done
check "minio parts match their sha256" "$( [ "$n_obj" -ge 1 ] && [ "$n_obj" = "$uploaded" ] && [ "$bad_obj" = 0 ] && echo 1 || echo 0)" \
  "objects=$n_obj uploaded=$uploaded mismatched=$bad_obj"
edge_hashes=$(find "$R/edge/data/store" -name '*.parquet' -exec shasum -a 256 {} + | cut -d' ' -f1 | sed 's/$/.parquet/' | sort)
missing_on_edge=$(comm -23 <(echo "$objects") <(echo "$edge_hashes") | grep -c . || true)
check "each part is an edge file" "$(eq "$missing_on_edge" 0)" "objects with no edge file of that sha256: $missing_on_edge"
catalog=$(sqlite3 "$R/central/data/catalog.sqlite" "select name from peer_parts order by name" 2>/dev/null)
check "central catalog lists the parts" "$(eq "$catalog" "$objects")" "peer_parts rows=$(echo "$catalog" | grep -c .)"

# --- tiers, node-owned prefix, listing, retention --------------------------------------------------------------
peerpost() { # path, body, [token]  -> "<http status> <body>"
  curl -s --max-time 10 -w ' %{http_code}' -X POST -H "Authorization: Bearer ${3:-$PEER_TOKEN}" -H 'Content-Type: application/json' -d "$2" "http://127.0.0.1:$PEER_PORT$1"
}
all_keys() { "$MC_BIN" --config-dir "$R/mc" ls --recursive --json "e2e/$BUCKET/" | python3 -c 'import sys, json; [print(json.loads(l)["key"]) for l in sys.stdin if l.strip()]' | sort; }
first=$(echo "$objects" | head -1)
sz=$("$MC_BIN" --config-dir "$R/mc" stat --json "e2e/$BUCKET/$PREFIX/$NODE/$first" | jq_py 'v["size"]')
pre_foreign=$(peerpost /v1/parts/presign "{\"name\":\"$first\",\"size\":$sz,\"sha256\":\"${first%.parquet}\",\"node\":\"edge-2\"}")
pre_trav=$(peerpost /v1/parts/presign "{\"name\":\"../edge-2/$first\",\"size\":$sz,\"sha256\":\"${first%.parquet}\"}")
com_foreign=$(peerpost /v1/parts/commit "{\"name\":\"$first\",\"size\":$sz,\"sha256\":\"${first%.parquet}\",\"rows\":1,\"min_unix_nano\":1,\"max_unix_nano\":2,\"origin\":\"x\",\"node\":\"edge-2\"}")
keys_outside=$(all_keys | grep -vc "^$PREFIX/$NODE/" || true)
check "foreign prefix and traversal refused; nothing outside own prefix" \
  "$( [[ "$pre_foreign" == *' 403' && "$pre_trav" == *' 422' && "$com_foreign" == *' 403' && "$keys_outside" = 0 ]] && echo 1 || echo 0)" \
  "foreign presign=${pre_foreign##* } traversal presign=${pre_trav##* } foreign commit=${com_foreign##* } objects outside $PREFIX/$NODE/=$keys_outside"
listing=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" GET "/admin/v1/parts?node=$NODE&tier=warm&limit=1000")
n_list=$(echo "$listing" | jq_py 'v["count"]')
n_other=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" GET "/admin/v1/parts?node=edge-2" | jq_py 'v["count"]')
check "catalog listing by node and tier" "$( [ "$n_list" = "$uploaded" ] && [ "$n_other" = 0 ] && echo 1 || echo 0)" \
  "warm parts of $NODE=$n_list (uploaded=$uploaded) parts of edge-2=$n_other"
ret=$(peerpost /v1/parts/retire "{\"name\":\"$first\"}")
ret_again=$(peerpost /v1/parts/retire "{\"name\":\"$first\"}")
n_cold=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" GET "/admin/v1/parts?tier=cold" | jq_py 'v["count"]')
check "node retires its own part to cold" "$( [[ "$ret" == *' 200' && "$ret_again" == *' 404' && "$n_cold" = 1 ]] && echo 1 || echo 0)" \
  "retire=${ret##* } repeat=${ret_again##* } cold parts=$n_cold"
sleep 3   # retention is 2 s in this stack; every part's newest data is now older than that
dry=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" POST /admin/v1/parts/sweep '{"dry_run":true}' | jq_py 'v["would_delete"]')
after_dry=$(all_keys | grep -c . || true)
swept=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" POST /admin/v1/parts/sweep '{"dry_run":false}' | jq_py '"%d %d" % (v["deleted"], v["failed"])')
after_sweep=$(all_keys | grep -c . || true)
cat_left=$(sqlite3 "$R/central/data/catalog.sqlite" "select count(*) from peer_parts")
audit_exp=$(sqlite3 "$R/central/data/catalog.sqlite" "select count(*) from peer_parts_audit where action='expire' and outcome='deleted'")
check "retention sweep deletes expired catalogued parts and audits them" \
  "$( [ "$dry" = "$uploaded" ] && [ "$after_dry" = "$uploaded" ] && [ "$swept" = "$uploaded 0" ] && [ "$after_sweep" = 0 ] && [ "$cat_left" = 0 ] && [ "$audit_exp" = "$uploaded" ] && echo 1 || echo 0)" \
  "dry-run would_delete=$dry objects after dry run=$after_dry sweep deleted/failed='$swept' objects after=$after_sweep catalog rows=$cat_left expire audit rows=$audit_exp"

# --- peer counters (live, before shutdown) -----------------------------------------------------------------------
edge_sync=$(admin "$EDGE_ADMIN" "$EDGE_TOKEN" GET /admin/v1/sync)
central_sync=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" GET /admin/v1/sync)
central_written=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" GET /admin/v1/info | jq_py 'v["ingest"]["written"]')
counters=$(EDGE="$edge_sync" CENTRAL="$central_sync" CW="$central_written" python3 - <<'PY'
import json, os
e = json.loads(os.environ["EDGE"])["status"]; r = json.loads(os.environ["CENTRAL"])["receiver"]; cw = int(os.environ["CW"])
problems = []
def need(cond, what):
    if not cond: problems.append(what)
need(r is not None, "central reports no receiver")
r = r or {}
need(r.get("accepted") == e["sent_records"] - e["duplicates"], "receiver.accepted != edge sent - duplicates")
need(r.get("accepted") == cw, "receiver.accepted != central ingest.written")
need(r.get("records", 0) == r.get("accepted", 0) + r.get("duplicates", 0) + r.get("loops", 0), "receiver records != accepted + duplicates + loops")
need(1 <= r.get("wal_waits", 0) <= r.get("requests", 0), "wal_waits outside 1..requests")
need(r.get("wal_wait_us_max", 0) < 5_000_000, "wal_wait_us_max reached ack_wait_ms")
for k in ("loops", "unauthorized", "bad_request", "dedupe_unsaved", "wal_timeouts", "wal_closed"):
    need(r.get(k, 0) == 0, f"receiver.{k}={r.get(k)}")
for k in ("loops", "rejected_batches", "dropped_records", "sampled_out", "rollups_sent", "lookback_sent"):
    need(e.get(k, 0) == 0, f"edge {k}={e.get(k)}")
last_error, state = e["last_error"], e["state"]
need(last_error is None, f"edge last_error={last_error}")
need(state == "ok", f"edge state={state}")
detail = (f"edge sent={e['sent_records']} dup={e['duplicates']} busy={e['busy_answers']}; receiver requests={r.get('requests')} "
          f"accepted={r.get('accepted')} dup={r.get('duplicates')} busy={r.get('busy')} wal_waits={r.get('wal_waits')} "
          f"wal_wait_max_ms={r.get('wal_wait_us_max', 0) // 1000}")
print(("1" if not problems else "0") + "\t" + detail + ("" if not problems else " PROBLEMS: " + "; ".join(problems)))
PY
)
check "peer ack counters sane" "$(echo "$counters" | cut -f1)" "$(echo "$counters" | cut -f2-)"

# --- shutdown ----------------------------------------------------------------------------------------------------
# Edge first (its sender drains into a live central), then central, then MinIO. Each must exit within 10 s of SIGTERM.
for name in edge central minio; do
  case $name in edge) pid=$EDGE_PID ;; central) pid=$CENTRAL_PID ;; minio) pid=$MINIO_PID ;; esac
  if stop_within "$pid" 10; then
    check "$name exits on SIGTERM within 10 s" 1 "${STOP_MS} ms"
  else
    check "$name exits on SIGTERM within 10 s" 0 "still running after ${STOP_MS} ms"
  fi
done
for name in edge central; do
  # tracing colours the level when it likes; strip escapes before looking for the ERROR level.
  sed -E $'s/\x1b\\[[0-9;]*m//g' "$R/$name/agent.log" > "$R/$name/agent.plain.log"
  n=$(grep -c ' ERROR ' "$R/$name/agent.plain.log" || true)
  check "no ERROR lines in $name log" "$(eq "$n" 0)" "$n ERROR lines$( [ "$n" != 0 ] && echo ": $(grep -m1 ' ERROR ' "$R/$name/agent.plain.log" | cut -c1-200)")"
done
for name in edge central; do
  grep -q 'shutdown signal received' "$R/$name/agent.log"; got_signal=$?
  check "$name logged a drained shutdown" "$(eq $got_signal 0)" "$(grep -c 'stopped' "$R/$name/agent.log") 'stopped' lines"
done

# --- counts on central -------------------------------------------------------------------------------------------
# Offline now: merge whatever is left in each WAL, then count the Parquet stores.
for name in edge central; do
  "$LOGLESS_BIN" merge --config "$R/$name/config.toml" > "$R/$name/merge.log" 2>&1
  "$LOGLESS_BIN" verify --config "$R/$name/config.toml" > "$R/$name/verify.log" 2>&1
  check "$name verify (accounting invariant)" "$(eq $? 0)" "$(tail -1 "$R/$name/verify.log" | cut -c1-160)"
done
count() { # store dir, sql where-clause -> "rows distinct_ids"
  "$DUCKDB_BIN" -noheader -csv -c "select count(*), count(distinct event_id) from read_parquet('$1/**/*.parquet', union_by_name = true) where $2" | tr ',' ' '
}
read c_otlp c_otlp_ids <<< "$(count "$R/central/data/store" "service = 'e2e-otlp'")"
read c_hec c_hec_ids <<< "$(count "$R/central/data/store" "service = 'e2e-hec'")"
read c_all c_all_ids <<< "$(count "$R/central/data/store" "true")"
read e_all e_all_ids <<< "$(count "$R/edge/data/store" "true")"
read c_rest _ <<< "$(count "$R/central/data/store" "service is distinct from 'e2e-otlp' and service is distinct from 'e2e-hec'")"
read e_rest _ <<< "$(count "$R/edge/data/store" "service is distinct from 'e2e-otlp' and service is distinct from 'e2e-hec'")"
check "central otlp records == sent" "$( [ "$c_otlp" = "$N_OTLP" ] && [ "$c_otlp_ids" = "$N_OTLP" ] && echo 1 || echo 0)" "rows=$c_otlp unique_ids=$c_otlp_ids sent=$N_OTLP"
check "central hec records == sent" "$( [ "$c_hec" = "$N_HEC" ] && [ "$c_hec_ids" = "$N_HEC" ] && echo 1 || echo 0)" "rows=$c_hec unique_ids=$c_hec_ids sent=$N_HEC"
if [ "$SENTRY_RAN" = 1 ]; then
  sentry_items=$(grep 'sentry proxy stopped' "$R/edge/agent.plain.log" | sed -E 's/.*items_received=([0-9]+).*/\1/')
  check "central sentry records == edge" "$( [ "$c_rest" = "$e_rest" ] && [ "$c_rest" -ge 1 ] && echo 1 || echo 0)" \
    "central=$c_rest edge=$e_rest (proxy items_received=$sentry_items)"
  # No upstream is configured, so the proxy forwards nothing and holds every item it received.
  sentry_fwd=$(grep 'sentry proxy stopped' "$R/edge/agent.plain.log" | sed -E 's/.*items_forwarded=([0-9]+).*/\1/')
  sentry_held=$(grep 'sentry proxy stopped' "$R/edge/agent.plain.log" | sed -E 's/.*held_locally=([0-9]+).*/\1/')
  check "sentry proxy counted nothing forwarded" "$( [ "$sentry_fwd" = 0 ] && [ "$sentry_held" = "$sentry_items" ] && echo 1 || echo 0)" \
    "items_forwarded=$sentry_fwd held_locally=$sentry_held items_received=$sentry_items"
fi
check "central total == edge total, ids unique" "$( [ "$c_all" = "$e_all" ] && [ "$c_all_ids" = "$c_all" ] && [ "$e_all_ids" = "$e_all" ] && echo 1 || echo 0)" \
  "central rows=$c_all ids=$c_all_ids; edge rows=$e_all ids=$e_all_ids"
# Fidelity 100: nothing is left out, so a total is the rollup counts (none expected) plus the records that are not
# supplements, and it must equal what the edge wrote.
recon=$("$DUCKDB_BIN" -noheader -csv -c "
  select count(*) filter (where attributes like '%\"logless.rollup\"%'),
         coalesce(sum(try_cast(json_extract_string(attributes, '\$.\"logless.rollup.count\"') as bigint)), 0),
         count(*) filter (where attributes not like '%\"logless.supplement\"%' or attributes is null)
  from read_parquet('$R/central/data/store/**/*.parquet', union_by_name = true)" | tr ',' ' ')
read n_rollups rollup_sum plain <<< "$recon"
check "rollup reconciliation (fidelity 100)" "$( [ "$n_rollups" = 0 ] && [ $(( rollup_sum + plain )) = "$e_all" ] && echo 1 || echo 0)" \
  "rollup records=$n_rollups rollup counts=$rollup_sum + plain records=$plain = $(( rollup_sum + plain )); edge wrote $e_all"

# --- report ------------------------------------------------------------------------------------------------------
echo
printf '%-44s %-5s %s\n' CHECK RESULT DETAIL
printf '%-44s %-5s %s\n' "-----" "------" "------"
while IFS=$'\t' read -r name status detail; do printf '%-44s %-5s %s\n' "$name" "$status" "$detail"; done < "$RESULTS"
fails=$(grep -c $'\tFAIL\t' "$RESULTS" || true)
echo
echo "elapsed $(( ($(now_ms) - T0) / 1000 )) s; $(grep -c $'\tPASS\t' "$RESULTS") passed, $fails failed, $(grep -c $'\tSKIP\t' "$RESULTS") skipped"
if [ "$fails" = 0 ]; then echo "E2E PASS"; exit 0; else echo "E2E FAIL"; exit 1; fi
