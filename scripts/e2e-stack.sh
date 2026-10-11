#!/usr/bin/env bash
# End-to-end run of the whole stack on one machine: a central log-less (peer receiver + Parquet parts) and an edge
# log-less (OTLP/HTTP, Splunk HEC and the Sentry proxy, syncing to central). Parts go to central's own disk (the
# default, PARTS_BACKEND=local, no MinIO) or to a MinIO this script starts (PARTS_BACKEND=s3). Sends known
# records, waits for sync, uploads parts, then checks counts, part checksums, peer counters, logs and shutdown. A
# second edge (node edge-3, upload timer on, nothing sent but parts) then has central frozen and stopped in the middle
# of its uploads and started again (no part lost, none duplicated), and its files removed by disk pressure (retired).
# Prints a PASS/FAIL table and exits 1 if any check failed. Everything runs on random free ports in a throwaway
# directory, with random credentials, and is removed on exit (KEEP=1 keeps the directory).
#
# Usage: scripts/e2e-stack.sh            See docs/e2e-stack.md.
# Env:   LOGLESS_BIN   (default $CARGO_TARGET_DIR/debug/logless, else ./target/debug/logless)
#        PARTS_BACKEND (local | s3)   BIG_PART_MB (128; local only: one part this large is uploaded to measure
#                      throughput and central's peak RSS; 0 skips it)
#        MINIO_BIN     (/opt/homebrew/bin/minio)   MC_BIN (mc)   (s3 only)   DUCKDB_BIN (duckdb)
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
PARTS_BACKEND="${PARTS_BACKEND:-local}"
BIG_PART_MB="${BIG_PART_MB:-128}"
BUCKET="logless-parts"
PREFIX="edges"
NODE="edge-1"   # the edge's node id: its parts live under $PREFIX/$NODE/ in the bucket, parts/$NODE/ on central's disk

case "$PARTS_BACKEND" in
  local) TOOLS="" ;;
  s3) TOOLS="$MINIO_BIN $MC_BIN" ;;
  *) echo "PARTS_BACKEND must be local or s3" >&2; exit 2 ;;
esac
for tool in "$LOGLESS_BIN" $TOOLS "$DUCKDB_BIN" curl python3 openssl sqlite3 shasum; do
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
log "run dir $R, logless $("$LOGLESS_BIN" --version 2>/dev/null || echo "$LOGLESS_BIN"), parts backend $PARTS_BACKEND"

# --- where parts go ----------------------------------------------------------------------------------------------
# s3: a MinIO and its bucket. local: nothing to start; central keeps parts under its data dir and serves them
# read-only as the bucket "parts" on its S3 endpoint. The helpers below are the only place the two differ.
PARTS_DIR="$R/central/data/parts"
if [ "$PARTS_BACKEND" = s3 ]; then
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
  OWN="$PREFIX/$NODE/"   # the node's key prefix
  PARTS_CONFIG="
peer.parts|endpoint|\"http://127.0.0.1:$MINIO_PORT\"
peer.parts|bucket|\"$BUCKET\"
peer.parts|prefix|\"$PREFIX\"
peer.parts|access_key_id|\"$MINIO_USER\"
peer.parts|secret_access_key|\"$MINIO_SECRET\""
  mc_keys() { "$MC_BIN" --config-dir "$R/mc" ls --json "$@" | python3 -c 'import sys, json; [print(json.loads(l)["key"]) for l in sys.stdin if l.strip()]' | sort; }
  node_parts() { mc_keys "e2e/$BUCKET/$PREFIX/$NODE/"; }
  cat_part() { "$MC_BIN" --config-dir "$R/mc" cat "e2e/$BUCKET/$PREFIX/$NODE/$1"; }
  part_size() { "$MC_BIN" --config-dir "$R/mc" stat --json "e2e/$BUCKET/$PREFIX/$NODE/$1" | jq_py 'v["size"]'; }
  all_keys() { mc_keys --recursive "e2e/$BUCKET/"; }
else
  S3_PORT=$(free_port)
  OWN="$NODE/"
  PARTS_CONFIG="
serve_s3|enabled|true
serve_s3|addr|\"127.0.0.1:$S3_PORT\""
  node_parts() { [ -d "$PARTS_DIR/$NODE" ] && (cd "$PARTS_DIR/$NODE" && find . -type f | sed 's|^\./||' | sort); }
  cat_part() { cat "$PARTS_DIR/$NODE/$1"; }
  part_size() { stat -f %z "$PARTS_DIR/$NODE/$1"; }
  all_keys() { [ -d "$PARTS_DIR" ] && (cd "$PARTS_DIR" && find . -type f | sed 's|^\./||' | sort); }   # temporary files too
fi

# --- central -----------------------------------------------------------------------------------------------------
PEER_PORT=$(free_port); CENTRAL_ADMIN=$(free_port)
PEER_TOKEN="$(rand 24)"; CENTRAL_TOKEN="$(rand 24)"   # PEER_TOKEN is the edge node's token
NODE3="edge-3"; PEER_TOKEN3="$(rand 24)"                # the second edge (upload timer), see "edge lifecycle" below
mkdir -p "$R/central"; : > "$R/central/empty.log"
render_config "$R/central/data" "
node|id|\"central-1\"
peer|enabled|true
peer|addr|\"127.0.0.1:$PEER_PORT\"
peer|nodes|[{ id = \"$NODE\", token = \"$PEER_TOKEN\" }, { id = \"$NODE3\", token = \"$PEER_TOKEN3\" }]
peer.parts|enabled|true
peer.parts|warm_retention_secs|2
peer.parts|cold_retention_secs|2$PARTS_CONFIG
admin|enabled|true
admin|addr|\"127.0.0.1:$CENTRAL_ADMIN\"
admin|token|\"$CENTRAL_TOKEN\"
" "
[[tenants.indexes]]
name = \"audit\"
" > "$R/central/config.toml"
start_central() { # appends to the log, so a restart keeps the first run's lines
  "$LOGLESS_BIN" run --config "$R/central/config.toml" --tail "$R/central/empty.log" >> "$R/central/agent.log" 2>&1 &
  CENTRAL_PID=$!; PIDS="$PIDS $CENTRAL_PID"
}
start_central
wait_http "http://127.0.0.1:$CENTRAL_ADMIN/" 30 || { echo "central did not start" >&2; cat "$R/central/agent.log" >&2; exit 1; }
log "central: peer $PEER_PORT, admin $CENTRAL_ADMIN"

# --- edge --------------------------------------------------------------------------------------------------------
OTLP_PORT=$(free_port); HEC_PORT=$(free_port); SENTRY_PORT=$(free_port); EDGE_ADMIN=$(free_port)
HEC_TOKEN="$(rand 16)"; EDGE_TOKEN="$(rand 24)"
mkdir -p "$R/edge"; : > "$R/edge/empty.log"
render_config "$R/edge/data" "
node|id|\"$NODE\"
tenants|unknown_index|\"refuse\"
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
[[tenants.indexes]]
name = \"audit\"

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

# Index routing. The edge has a second index, audit, and refuses an index it does not have. 20 HEC events name audit in
# the event's `index`, 10 OTLP records in the resource attribute logless.index; one HEC event and 5 OTLP records name an
# index that does not exist. Their service names are not e2e-otlp or e2e-hec, so the counts above are untouched.
N_AUDIT_HEC=20; N_AUDIT_OTLP=10; N_BAD_OTLP=5
python3 -c '
import json, sys, time
n_hec, n_otlp, n_bad, out = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
open(f"{out}/audit-hec.txt", "w").write("".join(json.dumps({"time": time.time(), "sourcetype": "e2e-audit-hec", "index": "audit", "event": f"audit hec seq={i}"}) for i in range(n_hec)))
def otlp(index, n, service):
    records = [{"timeUnixNano": str(time.time_ns() + i), "severityNumber": 9, "severityText": "INFO", "body": {"stringValue": f"audit otlp seq={i}"}} for i in range(n)]
    attrs = [{"key": "service.name", "value": {"stringValue": service}}, {"key": "logless.index", "value": {"stringValue": index}}]
    return json.dumps({"resourceLogs": [{"resource": {"attributes": attrs}, "scopeLogs": [{"logRecords": records}]}]})
open(f"{out}/audit-otlp.json", "w").write(otlp("audit", n_otlp, "e2e-audit-otlp"))
open(f"{out}/bad-otlp.json", "w").write(otlp("nope", n_bad, "e2e-bad-index"))
' "$N_AUDIT_HEC" "$N_AUDIT_OTLP" "$N_BAD_OTLP" "$R/payloads"
hec_post() { curl -s -w '\n%{http_code}' --max-time 15 -H "Authorization: Splunk $HEC_TOKEN" --data-binary "$1" "http://127.0.0.1:$HEC_PORT/services/collector/event"; }
audit_hec=$(hec_post "@$R/payloads/audit-hec.txt" | tail -1)
bad_hec=$(hec_post '{"event":"wrong index","sourcetype":"e2e-bad-index","index":"nope"}' | tr '\n' ' ')
audit_otlp=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -H 'Content-Type: application/json' --data-binary "@$R/payloads/audit-otlp.json" "http://127.0.0.1:$OTLP_PORT/v1/logs")
bad_otlp=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -H 'Content-Type: application/json' --data-binary "@$R/payloads/bad-otlp.json" "http://127.0.0.1:$OTLP_PORT/v1/logs")
check "hec refuses an unknown index with code 7" "$( [ "$audit_hec" = 200 ] && echo "$bad_hec" | grep -q '"code":7' && echo "$bad_hec" | grep -q ' 400 *$' && echo 1 || echo 0)" \
  "index=audit -> $audit_hec; index=nope -> $bad_hec"

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
unknown=$(admin "$EDGE_ADMIN" "$EDGE_TOKEN" GET /admin/v1/info | jq_py '"%d %d %s %s" % (v["ingest"]["unknown_index"]["refused"], v["ingest"]["unknown_index"]["defaulted"], v["node"]["id"], v["node"]["tenant"])')
check "records for an unknown index are refused and counted" "$( [ "$audit_otlp" = 200 ] && [ "$unknown" = "$(( N_BAD_OTLP + 1 )) 0 $NODE default" ] && echo 1 || echo 0)" \
  "otlp index=audit -> $audit_otlp, index=nope -> $bad_otlp; edge info: refused defaulted node tenant = $unknown (sent $N_BAD_OTLP over OTLP and 1 over HEC)"

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

# Every stored part is named by its content: sha256(part) == name, and the set matches central's catalog.
objects=$(node_parts)
n_obj=0; bad_obj=0
for o in $objects; do
  n_obj=$(( n_obj + 1 ))
  h=$(cat_part "$o" | shasum -a 256 | cut -d' ' -f1)
  [ "$h.parquet" = "$o" ] || { bad_obj=$(( bad_obj + 1 )); log "MISMATCH $o sha256=$h"; }
done
check "stored parts match their sha256" "$( [ "$n_obj" -ge 1 ] && [ "$n_obj" = "$uploaded" ] && [ "$bad_obj" = 0 ] && echo 1 || echo 0)" \
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
first=$(echo "$objects" | head -1)
sz=$(part_size "$first")
pre_foreign=$(peerpost /v1/parts/presign "{\"name\":\"$first\",\"size\":$sz,\"sha256\":\"${first%.parquet}\",\"node\":\"edge-2\"}")
pre_trav=$(peerpost /v1/parts/presign "{\"name\":\"../edge-2/$first\",\"size\":$sz,\"sha256\":\"${first%.parquet}\"}")
com_foreign=$(peerpost /v1/parts/commit "{\"name\":\"$first\",\"size\":$sz,\"sha256\":\"${first%.parquet}\",\"rows\":1,\"min_unix_nano\":1,\"max_unix_nano\":2,\"origin\":\"x\",\"node\":\"edge-2\"}")
keys_outside=$(all_keys | grep -vc "^$OWN" || true)
check "foreign prefix and traversal refused; nothing outside own prefix" \
  "$( [[ "$pre_foreign" == *' 403' && "$pre_trav" == *' 422' && "$com_foreign" == *' 403' && "$keys_outside" = 0 ]] && echo 1 || echo 0)" \
  "foreign presign=${pre_foreign##* } traversal presign=${pre_trav##* } foreign commit=${com_foreign##* } objects outside $OWN=$keys_outside"
listing=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" GET "/admin/v1/parts?node=$NODE&tier=warm&limit=1000")
n_list=$(echo "$listing" | jq_py 'v["count"]')
n_other=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" GET "/admin/v1/parts?node=edge-2" | jq_py 'v["count"]')
check "catalog listing by node and tier" "$( [ "$n_list" = "$uploaded" ] && [ "$n_other" = 0 ] && echo 1 || echo 0)" \
  "warm parts of $NODE=$n_list (uploaded=$uploaded) parts of edge-2=$n_other"
if [ "$PARTS_BACKEND" = local ]; then
  # The same edge, with no bucket anywhere: the part went to central's listener, and central's status says so.
  store=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" GET /admin/v1/sync | jq_py 'json.dumps(v["parts_store"])')
  st() { echo "$store" | jq_py "(v or {}).get('$1', -1)"; }
  disk_bytes=$(for o in $objects; do part_size "$o"; done | awk '{s+=$1} END {print s+0}')
  check "central's parts store status matches its disk" \
    "$( [ "$(st backend)" = local ] && [ "$(st uploads)" = "$uploaded" ] && [ "$(st used_bytes)" = "$disk_bytes" ] && [ "$(st reserved_bytes)" = 0 ] && [ "$(( $(st refused_budget) + $(st refused_token) + $(st refused_content) + $(st refused_size) + $(st incomplete) ))" = 0 ] && echo 1 || echo 0)" \
    "backend=$(st backend) uploads=$(st uploads) used_bytes=$(st used_bytes) (files on disk: $disk_bytes bytes) budget_bytes=$(st budget_bytes) refusals=0"
  # A PUT without a token, with a forged one, and to a traversal path: all refused, nothing new on disk.
  put_code() { curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X PUT ${2:+-H "x-logless-upload-token: $2"} --data-binary 'x' "http://127.0.0.1:$PEER_PORT$1"; }
  no_tok=$(put_code "/v1/parts/blob/$NODE/$first"); forged=$(put_code "/v1/parts/blob/$NODE/$first" "v1.9999999999.1.$(rand 32)")
  trav=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 --path-as-is -X PUT --data-binary 'x' "http://127.0.0.1:$PEER_PORT/v1/parts/blob/../$first")
  check "upload without a valid token is refused; nothing written" "$( [ "$no_tok" = 403 ] && [ "$forged" = 403 ] && [ "$trav" = 403 ] && [ "$(all_keys | grep -c .)" = "$uploaded" ] && echo 1 || echo 0)" \
    "no token=$no_tok forged token=$forged traversal path=$trav files under parts/=$(all_keys | grep -c .)"
  # Read side: the parts directory is the read-only bucket "parts" on central's S3 endpoint.
  s3_keys=$(curl -s --max-time 10 "http://127.0.0.1:$S3_PORT/parts?list-type=2&prefix=$NODE/" | python3 -c 'import sys, re; print("\n".join(sorted(k.split("/", 1)[1] for k in re.findall(r"<Key>([^<]*)</Key>", sys.stdin.read()))))')
  s3_sha=$(curl -s --max-time 30 "http://127.0.0.1:$S3_PORT/parts/$NODE/$first" | shasum -a 256 | cut -d' ' -f1)
  s3_put=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X PUT --data-binary 'x' "http://127.0.0.1:$S3_PORT/parts/$NODE/$first")
  check "parts are readable (only) through central's S3 endpoint" "$( [ "$s3_keys" = "$objects" ] && [ "$s3_sha.parquet" = "$first" ] && [ "$s3_put" = 405 ] && echo 1 || echo 0)" \
    "bucket parts lists $(echo "$s3_keys" | grep -c .) keys under $NODE/ (disk has $n_obj); GET sha256 matches name=$([ "$s3_sha.parquet" = "$first" ] && echo yes || echo no); PUT=$s3_put"
fi
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

# --- one large part: throughput, and central's memory while it streams (local backend) ----------------------------
# Asks for a URL as the edge does, sends BIG_PART_MB of random bytes with the headers central named, and samples
# central's RSS every 20 ms meanwhile. The part is never committed, so the sweep above and the counts below are
# not affected; what is checked is that it is stored whole and that central did not hold it in memory.
if [ "$PARTS_BACKEND" = local ] && [ "$BIG_PART_MB" -gt 0 ]; then
  head -c $(( BIG_PART_MB * 1024 * 1024 )) /dev/urandom > "$R/big.bin"
  big_sha=$(shasum -a 256 "$R/big.bin" | cut -d' ' -f1); big_size=$(stat -f %z "$R/big.bin")
  pre=$(peerpost /v1/parts/presign "{\"name\":\"$big_sha.parquet\",\"size\":$big_size,\"sha256\":\"$big_sha\"}")
  big_url=$(echo "${pre% *}" | jq_py 'v.get("url")')
  echo "${pre% *}" | python3 -c 'import sys, json
for k, v in (json.load(sys.stdin).get("headers") or {}).items(): print(f"header = \"{k}: {v}\"")' > "$R/big.curlrc"
  rss_kb() { ps -o rss= -p "$CENTRAL_PID" | tr -d ' '; }
  rss_before=$(rss_kb)
  ( while :; do rss_kb; sleep 0.02; done > "$R/big.rss" ) & SAMPLER=$!; PIDS="$PIDS $SAMPLER"
  big_out=$(curl -s -o "$R/big.reply" -w '%{http_code} %{time_total}' --max-time 300 -K "$R/big.curlrc" -T "$R/big.bin" "http://127.0.0.1:$PEER_PORT$big_url")
  kill "$SAMPLER" 2>/dev/null; wait "$SAMPLER" 2>/dev/null
  rss_peak=$(sort -n "$R/big.rss" | tail -1); rss_samples=$(grep -c . "$R/big.rss")
  big_code=${big_out% *}; big_secs=${big_out#* }
  big_stored=$(shasum -a 256 "$PARTS_DIR/$NODE/$big_sha.parquet" 2>/dev/null | cut -d' ' -f1)
  big_rate=$(python3 -c "print(f'{$big_size / 1048576 / $big_secs:.0f}')")
  rss_grow_kb=$(( rss_peak - rss_before ))
  check "a ${BIG_PART_MB} MiB part is stored whole" "$( [ "$big_code" = 200 ] && [ "$big_stored" = "$big_sha" ] && echo 1 || echo 0)" \
    "PUT=$big_code in ${big_secs}s = ${big_rate} MiB/s; stored sha256 matches=$([ "$big_stored" = "$big_sha" ] && echo yes || echo no) ($(cat "$R/big.reply"))"
  # Streamed, not buffered: central's RSS may not grow by more than a sixteenth of the part while it arrives.
  check "central streams the part (RSS growth under 1/16 of it)" "$( [ "$rss_grow_kb" -lt $(( BIG_PART_MB * 1024 / 16 )) ] && [ "$rss_samples" -ge 3 ] && echo 1 || echo 0)" \
    "central RSS before=$(( rss_before / 1024 )) MiB peak=$(( rss_peak / 1024 )) MiB growth=$(( rss_grow_kb / 1024 )) MiB over $rss_samples samples; part=${BIG_PART_MB} MiB"
  rm -f "$R/big.bin"
fi

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

# --- edge lifecycle: central stopped in the middle of an upload, retire on removal ---------------------------------
# A second edge, node edge-3, with the upload timer on ([sync_parts] interval = "10s") and min_severity 255 so it sends
# central no records, only parts. It is given a few thousand records; when they have merged into Parquet files the timer
# uploads them. This section freezes central (SIGSTOP) as soon as it has committed a few of them, so the edge's requests
# hang mid-job, then stops it with SIGTERM and starts it again. Then it checks that every file reached central once.
# Last, disk pressure on the edge removes every file it had uploaded, and each must turn cold on central (retire).
E3_OTLP=$(free_port); E3_ADMIN=$(free_port); E3_TOKEN="$(rand 24)"
mkdir -p "$R/edge3"; : > "$R/edge3/empty.log"
render_config "$R/edge3/data" "
node|id|\"$NODE3\"
storage|wal_segment_bytes|131072
otlp|enabled|true
otlp|addr|\"127.0.0.1:$E3_OTLP\"
admin|enabled|true
admin|addr|\"127.0.0.1:$E3_ADMIN\"
admin|token|\"$E3_TOKEN\"
sync_parts|interval|\"10s\"
" "
[[destinations]]
type = \"logless\"
url = \"http://127.0.0.1:$PEER_PORT\"
token = \"$PEER_TOKEN3\"
min_severity = 255
fidelity = 100
anomaly_boost = false
" > "$R/edge3/config.toml"
"$LOGLESS_BIN" run --config "$R/edge3/config.toml" --tail "$R/edge3/empty.log" > "$R/edge3/agent.log" 2>&1 &
E3_PID=$!; PIDS="$PIDS $E3_PID"
wait_http "http://127.0.0.1:$E3_ADMIN/" 30 || { echo "edge-3 did not start" >&2; cat "$R/edge3/agent.log" >&2; exit 1; }
for f in "$R"/payloads/otlp-00[0-5].json; do
  curl -s -o /dev/null --max-time 15 -H 'Content-Type: application/json' --data-binary "@$f" "http://127.0.0.1:$E3_OTLP/v1/logs"
done
e3() { admin "$E3_ADMIN" "$E3_TOKEN" "$@"; }
e3_parts() { e3 GET /admin/v1/sync | jq_py "v['status']['edge_parts']['$1']"; }
e3_files() { find "$R/edge3/data/store" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' '; }
cat3() { sqlite3 "$R/central/data/catalog.sqlite" "$1" 2>/dev/null; }
if [ "$PARTS_BACKEND" = s3 ]; then
  node3_objects() { mc_keys "e2e/$BUCKET/$PREFIX/$NODE3/"; }
else
  node3_objects() { [ -d "$PARTS_DIR/$NODE3" ] && (cd "$PARTS_DIR/$NODE3" && find . -type f | sed 's|^\./||' | sort); }
fi

# Freeze central the moment it has committed K parts of edge-3 (a python loop, so the poll is a millisecond, not a fork).
K_FREEZE=2
froze=$(CAT="$R/central/data/catalog.sqlite" PID="$CENTRAL_PID" K="$K_FREEZE" NODE3="$NODE3" python3 - <<'PY'
import os, signal, sqlite3, time
deadline = time.time() + 90
while time.time() < deadline:
    try:
        db = sqlite3.connect("file:" + os.environ["CAT"] + "?mode=ro", uri=True, timeout=1)
        n = db.execute("select count(*) from peer_parts where node = ?", (os.environ["NODE3"],)).fetchone()[0]
        db.close()
    except sqlite3.Error:
        n = 0
    if n >= int(os.environ["K"]):
        os.kill(int(os.environ["PID"]), signal.SIGSTOP)
        db = sqlite3.connect("file:" + os.environ["CAT"] + "?mode=ro", uri=True, timeout=1)
        frozen = db.execute("select count(*) from peer_parts where node = ?", (os.environ["NODE3"],)).fetchone()[0]
        print(frozen)
        break
    time.sleep(0.001)
else:
    print(-1)
PY
)
if [ "$froze" -ge 0 ] 2>/dev/null; then
  log "central frozen with $froze edge-3 parts committed"
  disk_frozen=$(node3_objects | grep -vc '\.upload-' || true)   # stored (PUT done), committed or not
  # The edge's requests hang. Its backlog and its pins are each published every 5 s, so wait for the two to agree.
  while_down_backlog=-1; while_down_pinned=-2; while_down_gaveup=-1
  for _ in $(seq 1 20); do
    sleep 1
    while_down_backlog=$(e3_parts backlog_parts); while_down_pinned=$(e3_parts pinned_parts); while_down_gaveup=$(e3_parts gave_up_parts)
    [ "$while_down_backlog" -ge 1 ] && [ "$while_down_pinned" = "$while_down_backlog" ] && break
  done
  ( sleep 0.3; kill -CONT "$CENTRAL_PID" 2>/dev/null ) &
  if stop_within "$CENTRAL_PID" 30; then stopped_ok=1; else stopped_ok=0; fi
  disk_stopped=$(node3_objects | grep -vc '\.upload-' || true)   # a request in flight at the freeze may finish while central shuts down
  log "central stopped (${STOP_MS} ms) with $disk_stopped parts stored; starting it again"
  start_central
  wait_http "http://127.0.0.1:$CENTRAL_ADMIN/" 30 || { echo "central did not restart" >&2; tail -20 "$R/central/agent.log" >&2; exit 1; }
else
  froze=0; stopped_ok=0; disk_frozen=0; disk_stopped=0; while_down_backlog=-1; while_down_pinned=-2; while_down_gaveup=-1
fi

# Wait until the edge has nothing unconfirmed and its file count has stopped moving.
n_files=-1; backlog=-1; stable=0
deadline=$(( $(date +%s) + 120 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  nf=$(e3_files); backlog=$(e3_parts backlog_parts)
  if [ "$nf" -ge 1 ] && [ "$backlog" = 0 ] && [ "$nf" = "$n_files" ]; then stable=$(( stable + 1 )); [ "$stable" -ge 3 ] && break; else stable=0; fi
  n_files=$nf
  sleep 2
done
check "central stopped in the middle of the edge's uploads" "$( [ "$stopped_ok" = 1 ] && [ "$froze" -ge 1 ] && [ "$froze" -lt "$n_files" ] && echo 1 || echo 0)" \
  "central had committed $froze of the edge's $n_files files when it was frozen; exited ${STOP_MS:-?} ms after SIGTERM; restarted"
check "edge pinned its backlog while central was down" "$( [ "$while_down_backlog" -ge 1 ] && [ "$while_down_pinned" = "$while_down_backlog" ] && [ "$while_down_gaveup" = 0 ] && echo 1 || echo 0)" \
  "backlog_parts=$while_down_backlog pinned_parts=$while_down_pinned gave_up_parts=$while_down_gaveup"
edge3_hashes=$(find "$R/edge3/data/store" -name '*.parquet' -exec shasum -a 256 {} + | cut -d' ' -f1 | sed 's/$/.parquet/' | sort)
cat3_names=$(cat3 "select name from peer_parts where node = '$NODE3' order by name")
disk3=$(node3_objects)
n_dist=$(echo "$cat3_names" | sort -u | grep -c . || true)
tmp3=$(echo "$disk3" | grep -c '\.upload-' || true)
state3_names=$(python3 -c 'import sys, json; print("\n".join(sorted(json.loads(l)["name"] for l in open(sys.argv[1]) if l.strip())))' "$R/edge3/data/parts-uploaded.jsonl")
check "no part lost, none duplicated after central restarted" \
  "$( [ "$stable" -ge 3 ] && [ "$cat3_names" = "$edge3_hashes" ] && [ "$disk3" = "$edge3_hashes" ] && [ "$(echo "$cat3_names" | grep -c .)" = "$n_files" ] && [ "$n_dist" = "$n_files" ] && [ "$tmp3" = 0 ] && [ "$state3_names" = "$edge3_hashes" ] && echo 1 || echo 0)" \
  "edge files=$n_files; central catalog rows=$(echo "$cat3_names" | grep -c .) distinct=$n_dist; objects stored=$(echo "$disk3" | grep -c .) (temporary: $tmp3); edge state lines=$(echo "$state3_names" | grep -c .); names equal the edge files' sha256: $([ "$cat3_names" = "$edge3_hashes" ] && [ "$disk3" = "$edge3_hashes" ] && echo yes || echo no)"
if [ "$PARTS_BACKEND" = local ]; then
  store3=$(admin "$CENTRAL_ADMIN" "$CENTRAL_TOKEN" GET /admin/v1/sync | jq_py 'json.dumps(v["parts_store"])')
  up3=$(echo "$store3" | jq_py 'v["uploads"]'); pre3=$(echo "$store3" | jq_py 'v["already_present"]')
  # Central's counters restarted with it. A part already stored when it exited (PUT done, commit perhaps not) is found
  # present by presign and only committed, so the bytes that arrive after the restart are exactly the parts not stored then.
  check "after the restart only the parts not yet stored arrived, once each" \
    "$( [ "$up3" = $(( n_files - disk_stopped )) ] && echo 1 || echo 0)" \
    "uploads since the restart=$up3 (already_present=$pre3); files=$n_files, stored when central exited=$disk_stopped (at the freeze: $disk_frozen, committed: $froze)"
fi

# Disk pressure on the edge removes the files it had uploaded; the retire pass tells central, which marks each cold.
pol=$(curl -s --max-time 10 -D "$R/e3.hdr" -H "Authorization: Bearer $E3_TOKEN" "http://127.0.0.1:$E3_ADMIN/admin/v1/policies")
E3_VERSION=$(echo "$pol" | jq_py 'v["version"]')
next=$(echo "$pol" | jq_py 'json.dumps({**v["effective"], "storage": {**v["effective"]["storage"], "disk_budget_bytes": 2048}})')
prev=$(e3 POST /admin/v1/policies/preview "{\"policies\":$next}")
tok=$(echo "$prev" | jq_py 'json.dumps(v["impact_token"])')
put=$(curl -s --max-time 20 -o "$R/e3.put" -w '%{http_code}' -X PUT -H "Authorization: Bearer $E3_TOKEN" -H "If-Match: $E3_VERSION" -H 'Content-Type: application/json' \
  -d "{\"policies\":$next,\"actor\":\"e2e\",\"confirm_destructive\":true,\"impact_token\":$tok}" "http://127.0.0.1:$E3_ADMIN/admin/v1/policies")
retired=0; cold=0; left=-1
deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  left=$(e3_files); cold=$(cat3 "select count(*) from peer_parts where node = '$NODE3' and tier = 'cold'")
  [ "$left" = 0 ] && [ "$cold" = "$n_files" ] && break
  sleep 1
done
warm=$(cat3 "select count(*) from peer_parts where node = '$NODE3' and tier = 'warm'")
retired=$(e3_parts retired); pending=$(e3_parts retire_pending); gaveup=$(e3_parts gave_up_parts)
state_left=$(python3 -c 'import sys; print(sum(1 for l in open(sys.argv[1]) if l.strip()))' "$R/edge3/data/parts-uploaded.jsonl")
check "files removed by disk pressure are retired: central marks each cold" \
  "$( [ "$put" = 200 ] && [ "$left" = 0 ] && [ "$cold" = "$n_files" ] && [ "$warm" = 0 ] && [ "$retired" = "$n_files" ] && [ "$pending" = 0 ] && [ "$state_left" = 0 ] && [ "$gaveup" = 0 ] && [ "$(node3_objects | grep -c .)" = "$n_files" ] && echo 1 || echo 0)" \
  "policy PUT=$put; edge files left=$left; central cold=$cold warm=$warm of $n_files; edge retired=$retired pending=$pending state lines left=$state_left; unconfirmed files given up=$gaveup; central still holds $(node3_objects | grep -c .) objects"

# --- shutdown ----------------------------------------------------------------------------------------------------
# Edge first (its sender drains into a live central), then central, then MinIO if there is one. Each must exit within
# 10 s of SIGTERM.
for name in edge edge3 central $([ "$PARTS_BACKEND" = s3 ] && echo minio); do
  case $name in edge) pid=$EDGE_PID ;; edge3) pid=$E3_PID ;; central) pid=$CENTRAL_PID ;; minio) pid=$MINIO_PID ;; esac
  if stop_within "$pid" 10; then
    check "$name exits on SIGTERM within 10 s" 1 "${STOP_MS} ms"
  else
    check "$name exits on SIGTERM within 10 s" 0 "still running after ${STOP_MS} ms"
  fi
done
for name in edge edge3 central; do
  # tracing colours the level when it likes; strip escapes before looking for the ERROR level.
  sed -E $'s/\x1b\\[[0-9;]*m//g' "$R/$name/agent.log" > "$R/$name/agent.plain.log"
  n=$(grep -c ' ERROR ' "$R/$name/agent.plain.log" || true)
  check "no ERROR lines in $name log" "$(eq "$n" 0)" "$n ERROR lines$( [ "$n" != 0 ] && echo ": $(grep -m1 ' ERROR ' "$R/$name/agent.plain.log" | cut -c1-200)")"
done
for name in edge edge3 central; do
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
# Template ids are content hashes the edge assigned and central keeps (it does not re-mine a record that has one), so
# the per-template row counts of the edge's store and of central's are the same table, and no id is a small counter.
# (One sending edge here; two independent miners agreeing is tests/template_hash_ids.rs.)
by_tpl() { "$DUCKDB_BIN" -noheader -csv -c "select coalesce(cast(template_id as varchar), 'none'), count(*) from read_parquet('$1/**/*.parquet', union_by_name = true) group by 1 order by 1"; }
e_tpl=$(by_tpl "$R/edge/data/store"); c_tpl=$(by_tpl "$R/central/data/store")
n_tpl=$(echo "$e_tpl" | grep -vc '^none,'); small_ids=$(echo "$c_tpl" | grep -v '^none,' | awk -F, '$1 < 1000000' | wc -l | tr -d ' ')
check "central keeps the edge's content-hash template ids" "$( [ "$e_tpl" = "$c_tpl" ] && [ "$n_tpl" -ge 3 ] && [ "$small_ids" = 0 ] && echo 1 || echo 0)" \
  "template ids: edge=$n_tpl central=$(echo "$c_tpl" | grep -vc '^none,'); per-template counts identical: $([ "$e_tpl" = "$c_tpl" ] && echo yes || echo no); central ids below 1000000: $small_ids"
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

# --- layout ------------------------------------------------------------------------------------------------------
# Every file is at tenant=/index=/hour=/level=/node=/, written by the instance's own node id; the audit index holds
# exactly the records sent to it, on the edge and (routed again by the attribute the edge sent) on central; a refused
# record is in neither; central's rows say which edge they came from.
LEAF='^tenant=default/index=(default|audit)/hour=[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}/level=(debug|info|warn_plus)/node='
shape() { # store dir, node id -> files not at a leaf of that node
  (cd "$1" && find . -name '*.parquet' | sed 's|^\./||' | grep -Evc "$LEAF$2/[^/]+\.parquet$")
}
off_edge=$(shape "$R/edge/data/store" "$NODE"); off_central=$(shape "$R/central/data/store" "central-1"); off_e3=$(shape "$R/edge3/data/store" "$NODE3")
check "every store file is at tenant/index/hour/level/node" "$( [ "$off_edge" = 0 ] && [ "$off_central" = 0 ] && [ "$off_e3" = 0 ] && echo 1 || echo 0)" \
  "files elsewhere: edge=$off_edge central=$off_central edge3=$off_e3 (of $(find "$R/edge/data/store" "$R/central/data/store" -name '*.parquet' | wc -l | tr -d ' '))"
hive() { # store dir -> "audit rows, bad-index rows, distinct tenants, distinct nodes, schema versions"
  "$DUCKDB_BIN" -noheader -csv -c "select count(*) filter (where \"index\" = 'audit'), count(*) filter (where service = 'e2e-bad-index'),
    count(distinct tenant), string_agg(distinct node, '+'), count(*) filter (where \"index\" = 'audit' and service not like 'e2e-audit-%')
    from read_parquet('$1/**/*.parquet', hive_partitioning = true, union_by_name = true)" | tr ',' ' '
}
e_hive=$(hive "$R/edge/data/store"); c_hive=$(hive "$R/central/data/store")
N_AUDIT=$(( N_AUDIT_HEC + N_AUDIT_OTLP ))
check "the audit index holds what was sent to it, on the edge and on central" "$( [ "$e_hive" = "$N_AUDIT 0 1 $NODE 0" ] && [ "$c_hive" = "$N_AUDIT 0 1 central-1 0" ] && echo 1 || echo 0)" \
  "audit rows, refused rows, tenants, nodes, other rows in audit: edge='$e_hive' central='$c_hive' (sent $N_AUDIT)"
origins=$("$DUCKDB_BIN" -noheader -csv -c "select origin, count(*) from read_parquet('$R/central/data/store/**/*.parquet', union_by_name = true) group by 1 order by 1" | tr '\n' ' ')
e_origins=$("$DUCKDB_BIN" -noheader -csv -c "select origin, count(*) from read_parquet('$R/edge/data/store/**/*.parquet', union_by_name = true) group by 1 order by 1" | tr '\n' ' ')
check "origin is the node that first received the row" "$( [ "$origins" = "$NODE,$c_all " ] && [ "$e_origins" = "$NODE,$e_all " ] && echo 1 || echo 0)" \
  "central: $origins edge: $e_origins"
late_e=$(find "$R/edge/data/wal-late" -name '*.wal' 2>/dev/null | wc -l | tr -d ' ')
check "the late spool exists and is drained" "$( [ -d "$R/edge/data/wal-late" ] && [ "$late_e" = 0 ] && echo 1 || echo 0)" "wal-late segments left on the edge after the offline merge: $late_e"

# --- report ------------------------------------------------------------------------------------------------------
echo
printf '%-44s %-5s %s\n' CHECK RESULT DETAIL
printf '%-44s %-5s %s\n' "-----" "------" "------"
while IFS=$'\t' read -r name status detail; do printf '%-44s %-5s %s\n' "$name" "$status" "$detail"; done < "$RESULTS"
fails=$(grep -c $'\tFAIL\t' "$RESULTS" || true)
echo
echo "elapsed $(( ($(now_ms) - T0) / 1000 )) s; $(grep -c $'\tPASS\t' "$RESULTS") passed, $fails failed, $(grep -c $'\tSKIP\t' "$RESULTS") skipped"
if [ "$fails" = 0 ]; then echo "E2E PASS"; exit 0; else echo "E2E FAIL"; exit 1; fi
