#!/usr/bin/env bash
# End-to-end run of log-less against a REAL session-broker (docs/auth.md, ADR 0004): the broker and its mock identity
# provider are started from their built binaries, a log-less with two tenants is pointed at the broker with `[auth]`,
# and the run checks, in order:
#   1. a machine token of tenant acme (POST /auth/token) ingests, and its rows are stored under tenant=acme/;
#   2. a token for a tenant this instance does not have is refused;
#   3. a person signs in through the identity provider, trades the user token at POST /v1/auth/s3-credentials, and
#      reads the store with DuckDB: acme's rows only, another tenant's file refused; a reading service's machine
#      token is traded the same way; the credentials stop when they expire;
#   4. the broker's signing key is rotated and a token signed by the new key verifies (the unknown-kid refresh);
#   5. the broker is stopped: a still-valid token (cached key) and a static key both keep ingesting, status shows
#      the age of the key cache, and a restart of log-less with the broker still down changes nothing: tokens still
#      verify, and S3 credentials issued before the restart still read.
# Prints a PASS/FAIL table and exits 1 if any check failed. SKIPS (exit 0) when the broker binaries are not there.
# Everything runs on random free loopback ports in a throwaway directory with random credentials, every server has
# a time limit, and no token, key or secret is printed. Takes about 90 seconds: log-less asks the broker for an
# unknown key id at most once a minute, and the run waits for that minute.
#
# Usage: scripts/auth-broker-e2e.sh
# Env:   LOGLESS_BIN  (default $CARGO_TARGET_DIR/debug/logless, else ./target/debug/logless)
#        BROKER_BIN_DIR (~/tmp/sb-target/debug: `session-broker` and `mock-idp`, built from the session-broker repo
#                      with `cargo build --workspace`)
#        DUCKDB_BIN (duckdb)   SERVER_TTL (300, seconds any server may live)   KEEP (0)
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
LOGLESS_BIN="${LOGLESS_BIN:-${CARGO_TARGET_DIR:-$HERE/../target}/debug/logless}"
BROKER_BIN_DIR="${BROKER_BIN_DIR:-$HOME/tmp/sb-target/debug}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
SERVER_TTL="${SERVER_TTL:-300}"
KEEP="${KEEP:-0}"
S3_TTL=25          # seconds a temporary S3 credential lives in this run
N_ACME=12; N_OWN=5; N_ROTATED=4; N_DOWN=3; N_DOWN_STATIC=2; N_RESTART=6
N_FILL=600         # records sent to fill and close the first WAL segment, so a merge puts files in the store

if [ ! -x "$BROKER_BIN_DIR/session-broker" ] || [ ! -x "$BROKER_BIN_DIR/mock-idp" ]; then
  echo "SKIP: no session-broker and mock-idp in $BROKER_BIN_DIR (set BROKER_BIN_DIR)"
  exit 0
fi
for tool in "$LOGLESS_BIN" "$DUCKDB_BIN" curl python3 openssl perl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing: $tool" >&2; exit 2; }
done

R="$(mktemp -d "${TMPDIR:-/tmp}/logless-auth-e2e.XXXXXX")"
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

now_s() { date +%s; }
rand() { openssl rand -hex "$1"; }
free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
jq_py() { python3 -c "import sys, json; v = json.load(sys.stdin); print($1)"; }
# Every server is started as `perl -e "$TTL" <seconds> <command>`: an alarm, then exec, so the server has the pid
# this script remembers and the kernel ends it after SERVER_TTL seconds whatever happens to the script.
TTL='alarm shift; exec @ARGV'

RESULTS="$R/results.tsv"; : > "$RESULTS"
check() { local status=FAIL; [ "$2" = 1 ] && status=PASS; printf '%s\t%s\t%s\n' "$1" "$status" "$3" >> "$RESULTS"; }
wait_http() { # url, seconds
  local deadline=$(( $(now_s) + $2 ))
  while [ "$(now_s)" -lt "$deadline" ]; do
    curl -s -o /dev/null --max-time 1 "$1" && return 0
    sleep 0.2
  done
  return 1
}
gone() { ! kill -0 "$1" 2>/dev/null || [ "$(ps -o stat= -p "$1" 2>/dev/null | cut -c1)" = Z ]; }
stop_within() { # pid, seconds
  kill -TERM "$1" 2>/dev/null
  for _ in $(seq 1 $(( $2 * 10 ))); do gone "$1" && { wait "$1" 2>/dev/null; return 0; }; sleep 0.1; done
  return 1
}

T0=$(now_s)
IDP=$(free_port); BROKER=$(free_port); BROKER_ADMIN=$(free_port)
HEC=$(free_port); PEER=$(free_port); ADMIN=$(free_port); S3=$(free_port)
BROKER_URL="http://127.0.0.1:$BROKER"     # the broker refuses a plain http base URL off loopback
BROKER_KEY="$(rand 24)"; ADMIN_TOKEN="$(rand 24)"; OWN_KEY="$(rand 24)"; S3_SECRET="$(rand 24)"
mkdir -p "$R/broker" "$R/logless"

# --- the broker and its identity provider ------------------------------------------------------------------------
MOCK_IDP_BIND="127.0.0.1:$IDP" MOCK_IDP_REDIRECT_URIS="$BROKER_URL/auth/callback" \
  perl -e "$TTL" "$SERVER_TTL" "$BROKER_BIN_DIR/mock-idp" > "$R/broker/idp.log" 2>&1 &
IDP_PID=$!; PIDS="$PIDS $IDP_PID"
wait_http "http://127.0.0.1:$IDP/.well-known/openid-configuration" 20 || { echo "mock-idp did not start" >&2; exit 2; }
start_broker() {
  BROKER_BIND_ADDR="127.0.0.1:$BROKER" BROKER_INTERNAL_BIND_ADDR="127.0.0.1:$BROKER_ADMIN" BROKER_BASE_URL="$BROKER_URL" \
  BROKER_DB_PATH="$R/broker/broker.db" BROKER_KEYFILE_PATH="$R/broker/broker.key" \
  BROKER_OIDC_ISSUER_URL="http://127.0.0.1:$IDP" BROKER_OIDC_CLIENT_ID=session-broker-dev BROKER_OIDC_CLIENT_SECRET=dev-secret \
  BROKER_ADMIN_API_KEY="$BROKER_KEY" BROKER_TOKEN_ISSUER="$BROKER_URL" BROKER_ISSUER_AUDIENCES=logless \
    perl -e "$TTL" "$SERVER_TTL" "$BROKER_BIN_DIR/session-broker" >> "$R/broker/broker.log" 2>&1 &
  BROKER_PID=$!; PIDS="$PIDS $BROKER_PID"
}
start_broker
wait_http "$BROKER_URL/auth/jwks.json" 30 || { echo "session-broker did not start: $(tail -1 "$R/broker/broker.log" | cut -c1-200)" >&2; exit 2; }
log "session-broker on :$BROKER (admin :$BROKER_ADMIN), mock-idp on :$IDP"

broker_admin() { # path, body
  curl -s --max-time 10 -X POST -H "Authorization: Bearer $BROKER_KEY" -H 'Content-Type: application/json' -d "$2" "http://127.0.0.1:$BROKER_ADMIN$1"
}
# principal NAME KIND TENANT -> "id secret" of a new service principal for audience logless
principal() {
  broker_admin /admin/principals "{\"name\":\"$1\",\"kind\":\"$2\",\"tenant\":\"$3\",\"scopes\":[\"$2\"],\"audience\":\"logless\"}" \
    | jq_py '"%s %s" % (v["principal_id"], v["secret"]["secret"])'
}
machine_token() { # "id secret" -> access token (client credentials)
  curl -s --max-time 10 -X POST -u "${1% *}:${1#* }" -d grant_type=client_credentials "$BROKER_URL/auth/token" | jq_py 'v["access_token"]'
}
kid_of() { python3 -c 'import sys, json, base64; h = sys.argv[1].split(".")[0]; print(json.loads(base64.urlsafe_b64decode(h + "=" * (-len(h) % 4)))["kid"])' "$1"; }
claim_of() { python3 -c 'import sys, json, base64; p = sys.argv[1].split(".")[1]; print(json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4))).get(sys.argv[2]))' "$1" "$2"; }

EDGE="$(principal edge-1 ingest acme)"
GHOST="$(principal edge-9 ingest globex)"        # a tenant the broker has and this log-less does not
REPORTING="$(principal reporting reader acme)"
broker_admin /admin/user-grants '{"sub":"user-1","tenant":"acme","roles":["viewer"]}' > /dev/null

# --- log-less ----------------------------------------------------------------------------------------------------
: > "$R/logless/empty.log"
"$LOGLESS_BIN" init --data-dir "$R/logless/data" | SECTIONS="
storage|wal_segment_bytes|65536
hec|enabled|true
hec|addr|\"127.0.0.1:$HEC\"
hec|require_auth|true
peer|enabled|true
peer|addr|\"127.0.0.1:$PEER\"
peer|require_auth|true
admin|enabled|true
admin|addr|\"127.0.0.1:$ADMIN\"
admin|token|\"$ADMIN_TOKEN\"
serve_s3|enabled|true
serve_s3|addr|\"127.0.0.1:$S3\"
serve_s3|access_key|\"e2e-operator\"
serve_s3|secret_key|\"$S3_SECRET\"
" EXTRA="
[auth]
issuer = \"$BROKER_URL\"
s3_credentials_ttl_secs = $S3_TTL

[[tenants]]
id = \"acme\"

[[principals]]
id = \"own-app\"
kind = \"ingest\"
key = \"$OWN_KEY\"
" python3 -c '
import os, sys
want = {}
for line in os.environ["SECTIONS"].strip().splitlines():
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
' > "$R/logless/config.toml"
chmod 600 "$R/logless/config.toml"
start_logless() {
  perl -e "$TTL" "$SERVER_TTL" "$LOGLESS_BIN" run --config "$R/logless/config.toml" --tail "$R/logless/empty.log" >> "$R/logless/agent.log" 2>&1 &
  LOGLESS_PID=$!; PIDS="$PIDS $LOGLESS_PID"
  wait_http "http://127.0.0.1:$ADMIN/" 30 && wait_http "http://127.0.0.1:$HEC/services/collector/health" 30 && wait_http "http://127.0.0.1:$S3/" 30
}
start_logless || { echo "log-less did not start: $(tail -1 "$R/logless/agent.log" | cut -c1-300)" >&2; exit 2; }
LOGLESS_T0=$(now_s)     # log-less fetched the key set now; it asks again for an unknown key id 60 s after this
log "log-less: hec :$HEC, peer :$PEER, s3 :$S3, admin :$ADMIN"

info() { curl -s --max-time 10 -H "Authorization: Bearer $ADMIN_TOKEN" "http://127.0.0.1:$ADMIN/admin/v1/info"; }
events() { python3 -c 'import sys, json; print("".join(json.dumps({"event": "%s %d %s" % (sys.argv[2], i, "x" * int(sys.argv[3])), "sourcetype": sys.argv[2]}) for i in range(int(sys.argv[1]))))' "$1" "$2" "${3:-0}"; }
hec_as() { # credential, body -> status
  curl -s -o /dev/null -w '%{http_code}' --max-time 10 -H "Authorization: Splunk $1" -d "$2" "http://127.0.0.1:$HEC/services/collector/event"
}

# 1. A machine token of tenant acme ingests.
TOKEN1="$(machine_token "$EDGE")"; KID1="$(kid_of "$TOKEN1")"
sent_acme=$(hec_as "$TOKEN1" "$(events $N_ACME auth-acme)")
sent_own=$(hec_as "$OWN_KEY" "$(events $N_OWN auth-own)")
# Enough bytes from both tenants to close the first WAL segment, so the store has files to read while this runs.
fill_acme=$(hec_as "$TOKEN1" "$(events $N_FILL auth-fill-acme 200)"); fill_own=$(hec_as "$OWN_KEY" "$(events $N_FILL auth-fill-own 200)")
seen1=$(info | jq_py '" ".join("%s:%s:%s:%d" % (s["principal"], s["kind"], s["source"], s["records"]) for s in v["auth"]["seen"])')
check "1. a machine token ingests as its principal" "$( [ "$sent_acme" = 200 ] && [ "$fill_acme" = 200 ] && [ "$sent_own" = 200 ] && [ "$fill_own" = 200 ] && [ "$seen1" = "acme/edge-1:ingest:token:$((N_ACME + N_FILL)) default/own-app:ingest:static:$((N_OWN + N_FILL))" ] && echo 1 || echo 0)" \
  "token (tenant $(claim_of "$TOKEN1" tenant), kind $(claim_of "$TOKEN1" kind), kid $KID1) -> $sent_acme; static key -> $sent_own; status seen: $seen1"

# 2. A token for a tenant this instance lacks.
GHOST_TOKEN="$(machine_token "$GHOST")"
ghost=$(hec_as "$GHOST_TOKEN" "$(events 1 auth-ghost)")
ghost_peer=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST -H "Authorization: Bearer $GHOST_TOKEN" "http://127.0.0.1:$PEER/v1/auth/s3-credentials")
unknown_tenant=$(info | jq_py 'v["auth"]["refused"]["unknown_tenant"]')
check "2. a token for a tenant the instance lacks is refused" "$( [ "$ghost" = 403 ] && [ "$ghost_peer" = 403 ] && [ "$unknown_tenant" -ge 2 ] && echo 1 || echo 0)" \
  "token of tenant $(claim_of "$GHOST_TOKEN" tenant): ingest -> $ghost, s3-credentials -> $ghost_peer; status refused.unknown_tenant = $unknown_tenant"

# 3. A person signs in, trades the user token for S3 credentials, and reads with DuckDB.
deadline=$(( $(now_s) + 30 ))
while [ "$(now_s)" -lt "$deadline" ]; do
  [ -n "$(find "$R/logless/data/store/tenant=acme" "$R/logless/data/store/tenant=default" -name '*.parquet' 2>/dev/null | head -1)" ] \
    && [ -n "$(find "$R/logless/data/store/tenant=default" -name '*.parquet' 2>/dev/null | head -1)" ] && break
  sleep 0.5
done
# The sign-in: the browser's redirects, followed by curl with a cookie jar (the mock provider has no login page).
login=$(curl -s --max-time 15 -c "$R/broker/jar" -b "$R/broker/jar" -L -o /dev/null -w '%{http_code}' "$BROKER_URL/auth/login?return_to=/")
USER_TOKEN="$(curl -s --max-time 10 -b "$R/broker/jar" -X POST -H 'Content-Type: application/json' -H "Origin: $BROKER_URL" -d '{"audience":"logless","tenant":"acme"}' "$BROKER_URL/session/token" | jq_py 'v.get("access_token", "")')"
have_session=$(grep -c broker_session "$R/broker/jar" 2>/dev/null)
exchange() { # token -> the reply saved to a file, status printed
  curl -s --max-time 10 -o "$2" -w '%{http_code}' -X POST -H "Authorization: Bearer $1" "http://127.0.0.1:$PEER/v1/auth/s3-credentials"
}
if [ -n "$USER_TOKEN" ]; then WHO="a user token (kind $(claim_of "$USER_TOKEN" kind), sub $(claim_of "$USER_TOKEN" sub), tenant $(claim_of "$USER_TOKEN" tenant))"; READ_TOKEN="$USER_TOKEN"
else WHO="a reader machine token (the sign-in could not be scripted: login -> $login, session cookie: $have_session)"; READ_TOKEN="$(machine_token "$REPORTING")"; fi
traded=$(exchange "$READ_TOKEN" "$R/logless/creds.json")
EXCHANGED_AT=$(now_s)
duck_as() { # credentials file, sql -> DuckDB's answer or its error, on one line
  "$DUCKDB_BIN" -noheader -csv -c "LOAD httpfs; CREATE SECRET (TYPE s3, KEY_ID '$(jq_py 'v["access_key_id"]' < "$1")', SECRET '$(jq_py 'v["secret_access_key"]' < "$1")', SESSION_TOKEN '$(jq_py 'v["session_token"]' < "$1")', ENDPOINT '127.0.0.1:$S3', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1'); $2" 2>&1 | grep -v '^true$' | tr '\n' ' ' | sed 's/ *$//'
}
BY_TENANT="select string_agg(tenant || ':' || (n > 0), ' ' order by tenant) from (select tenant, count(*) n from read_parquet('s3://logs/**/*.parquet', hive_partitioning = true, union_by_name = true) group by 1)"
scoped=$(duck_as "$R/logless/creds.json" "$BY_TENANT")
other_file=$(cd "$R/logless/data/store" && find tenant=default -name '*.parquet' | head -1)
other=$(duck_as "$R/logless/creds.json" "select count(*) from read_parquet('s3://logs/$other_file')")
own_file=$(cd "$R/logless/data/store" && find tenant=acme -name '*.parquet' | head -1)
own=$(duck_as "$R/logless/creds.json" "select 'rows:' || (count(*) > 0) from read_parquet('s3://logs/$own_file')")
lifetime=$(jq_py '"%s %s %s %d" % (v["tenant"], v["indexes"], v["access_key_id"][:5], v["expires_unix_secs"] - '"$EXCHANGED_AT"')' < "$R/logless/creds.json")
check "3a. a user token is traded for S3 credentials; DuckDB reads its tenant only" "$( [ "$traded" = 200 ] && [ -n "$USER_TOKEN" ] && [ "$scoped" = "acme:true" ] && [ "$own" = rows:true ] && grep -q 403 <<< "$other" && echo 1 || echo 0)" \
  "$WHO -> $traded (tenant, indexes, key id prefix, seconds to live: $lifetime); s3://logs/**: '$scoped'; a file of tenant acme: $own; a file of tenant default: $(grep -o '403[^)]*)' <<< "$other" | head -1)"
# A reading service does the same with its machine token; the operator's own pair still reads both tenants.
svc_traded=$(exchange "$(machine_token "$REPORTING")" "$R/logless/svc.json")
svc=$(duck_as "$R/logless/svc.json" "$BY_TENANT")
operator=$("$DUCKDB_BIN" -noheader -csv -c "LOAD httpfs; CREATE SECRET (TYPE s3, KEY_ID 'e2e-operator', SECRET '$S3_SECRET', ENDPOINT '127.0.0.1:$S3', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1'); $BY_TENANT" 2>&1 | grep -v '^true$' | tr '\n' ' ' | sed 's/ *$//')
ingest_traded=$(exchange "$TOKEN1" /dev/null)
check "3b. a reader machine token is traded too; an ingest token is not" "$( [ "$svc_traded" = 200 ] && [ "$svc" = "acme:true" ] && [ "$operator" = "acme:true default:true" ] && [ "$ingest_traded" = 403 ] && echo 1 || echo 0)" \
  "reader token -> $svc_traded, reads '$svc'; ingest token -> $ingest_traded; the store's own key pair reads '$operator'"

# 4. Rotation. The new key signs at once and the old one is no longer published; log-less learns it by asking once
#    for the key id it does not know, which it does at most once a minute.
wait_left=$(( LOGLESS_T0 + 62 - $(now_s) ))
[ "$wait_left" -gt 0 ] && { log "waiting ${wait_left}s: an unknown key id is asked for at most once a minute"; sleep "$wait_left"; }
rotated=$(broker_admin /admin/signing-keys/rotate '{"activate_in_secs":0}' | jq_py 'v["kid"]')
TOKEN2="$(machine_token "$EDGE")"; KID2="$(kid_of "$TOKEN2")"
READER2="$(machine_token "$REPORTING")"     # for step 5: a reader's token, got while the broker was still there
keys_before=$(info | jq_py '",".join(v["auth"]["broker"]["key_ids"])')
sent_rotated=$(hec_as "$TOKEN2" "$(events $N_ROTATED auth-rotated)")
keys_after=$(info | jq_py '",".join(v["auth"]["broker"]["key_ids"])')
check "4. a token signed by the rotated key verifies" "$( [ "$KID1" != "$KID2" ] && [ "$rotated" = "$KID2" ] && [ "$keys_before" = "$KID1" ] && [ "$sent_rotated" = 200 ] && [ "$keys_after" = "$KID2" ] && echo 1 || echo 0)" \
  "broker rotated $KID1 -> $rotated; log-less knew [$keys_before], took the new token -> $sent_rotated, now knows [$keys_after]"

# 3c. By now the temporary credentials of step 3 have run out.
left=$(( EXCHANGED_AT + S3_TTL + 1 - $(now_s) )); [ "$left" -gt 0 ] && sleep "$left"
expired=$(duck_as "$R/logless/creds.json" "$BY_TENANT")
expired_count=$(info | jq_py 'v["auth"]["refused"]["expired"]')
check "3c. an expired S3 credential is refused" "$(grep -q '400' <<< "$expired" && [ "$expired_count" -ge 1 ] && echo 1 || echo 0)" \
  "the same credentials ${S3_TTL}s on: $(grep -o 'HTTP 400[^)]*)\|400 (Bad Request)' <<< "$expired" | head -1); status refused.expired = $expired_count"

# 5. The broker goes away.
stop_within "$BROKER_PID" 10; stop_within "$IDP_PID" 5
broker_up=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$BROKER_URL/auth/jwks.json")
sleep 2
down_token=$(hec_as "$TOKEN2" "$(events $N_DOWN auth-down)")
down_static=$(hec_as "$OWN_KEY" "$(events $N_DOWN_STATIC auth-down-static)")
info > "$R/logless/info-down.json"
cache=$(jq_py '"%s %s" % (v["auth"]["broker"]["key_set_age_secs"], ",".join(v["auth"]["broker"]["key_ids"]))' < "$R/logless/info-down.json")
check "5a. with the broker stopped a cached-key token and a static key still ingest" "$( [ "$broker_up" = 000 ] && [ "$down_token" = 200 ] && [ "$down_static" = 200 ] && [ "${cache% *}" -ge 2 ] && echo 1 || echo 0)" \
  "broker answers $broker_up; token -> $down_token, static key -> $down_static; status key_set_age_secs and key ids: $cache"
# A restart while the broker is down: the key set comes from <data_dir>/jwks-cache.json, and credentials issued
# before the restart are still good because their key is in <data_dir>/s3-session.key.
before_restart=$(exchange "$READER2" "$R/logless/before-restart.json")
stop_within "$LOGLESS_PID" 15
start_logless; restarted=$?
after_restart=$(duck_as "$R/logless/before-restart.json" "$BY_TENANT")
restart_token=$(hec_as "$TOKEN2" "$(events $N_RESTART auth-restart)")
info > "$R/logless/info-restart.json"
restart_state=$(jq_py '"%s %s %s" % (",".join(v["auth"]["broker"]["key_ids"]), v["auth"]["broker"]["refresh_failures"] >= 1, v["auth"]["s3_credentials"]["key_persisted"])' < "$R/logless/info-restart.json")
key_mode=$(stat -f '%Lp' "$R/logless/data/s3-session.key" 2>/dev/null || stat -c '%a' "$R/logless/data/s3-session.key" 2>/dev/null)
check "5b. a restart with the broker down still verifies from the key cache" "$( [ "$restarted" = 0 ] && [ "$before_restart" = 200 ] && [ "$after_restart" = "acme:true" ] && [ "$restart_token" = 200 ] && [ "$restart_state" = "$KID2 True True" ] && [ "$key_mode" = 600 ] && echo 1 || echo 0)" \
  "S3 credentials issued before the restart (broker down: $before_restart) read '$after_restart' after it; token -> $restart_token; key ids, a refresh failed, s3 key persisted: $restart_state; s3-session.key mode $key_mode"

# --- what was stored, and what was written down ------------------------------------------------------------------
stop_within "$LOGLESS_PID" 15
"$LOGLESS_BIN" merge --config "$R/logless/config.toml" > "$R/logless/merge.log" 2>&1
placed=$("$DUCKDB_BIN" -noheader -csv -c "select tenant, count(*), count(distinct service) from read_parquet('$R/logless/data/store/**/*.parquet', hive_partitioning = true, union_by_name = true) group by 1 order by 1" | tr '\n' ' ')
want_acme=$(( N_ACME + N_FILL + N_ROTATED + N_DOWN + N_RESTART )); want_own=$(( N_OWN + N_FILL + N_DOWN_STATIC ))
ghost_rows=$("$DUCKDB_BIN" -noheader -csv -c "select count(*) from read_parquet('$R/logless/data/store/**/*.parquet', union_by_name = true) where service = 'auth-ghost'")
check "1b. every row is under its credential's tenant" "$( [ "$placed" = "acme,$want_acme,5 default,$want_own,3 " ] && [ "$ghost_rows" = 0 ] && [ ! -d "$R/logless/data/store/tenant=globex" ] && echo 1 || echo 0)" \
  "rows and services by tenant: $placed(want acme $want_acme, default $want_own); rows of the refused tenant: $ghost_rows"
# No token, key or secret in a log line or a status document.
secrets=("$TOKEN1" "$TOKEN2" "$GHOST_TOKEN" "$READ_TOKEN" "$OWN_KEY" "$S3_SECRET" "$ADMIN_TOKEN" "${EDGE#* }" "$(jq_py 'v["secret_access_key"]' < "$R/logless/creds.json")" "$(jq_py 'v["session_token"]' < "$R/logless/creds.json")" "$(jq_py 'v["secret_access_key"]' < "$R/logless/svc.json")" "$READER2" "$(jq_py 'v["secret_access_key"]' < "$R/logless/before-restart.json")")
leaked=0
for secret in "${secrets[@]}"; do
  n=$(cat "$R/logless/agent.log" "$R/logless/merge.log" "$R/logless/info-down.json" "$R/logless/info-restart.json" | grep -c -F -e "$secret"); leaked=$(( leaked + n ))
done
hmac_key=$(cat "$R/logless/data/s3-session.key")
leaked=$(( leaked + $(cat "$R/logless/agent.log" "$R/logless/info-restart.json" | grep -c -F -e "$hmac_key") ))
check "no token, key or secret in a log line or in status" "$( [ "$leaked" = 0 ] && echo 1 || echo 0)" \
  "${#secrets[@]} tokens, keys and secrets and the s3 session key looked for in agent.log, merge.log and two status documents: $leaked found"

# --- report ------------------------------------------------------------------------------------------------------
echo
printf '%-78s %-5s %s\n' CHECK RESULT DETAIL
while IFS=$'\t' read -r name status detail; do printf '%-78s %-5s %s\n' "$name" "$status" "$detail"; done < "$RESULTS"
fails=$(grep -c $'\tFAIL\t' "$RESULTS" || true)
echo
echo "elapsed $(( $(now_s) - T0 )) s; $(grep -c $'\tPASS\t' "$RESULTS") passed, $fails failed"
if [ "$fails" = 0 ]; then echo "AUTH E2E PASS"; exit 0; else echo "AUTH E2E FAIL"; exit 1; fi
