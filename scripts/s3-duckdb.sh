#!/usr/bin/env bash
# End-to-end proof of `logless serve` with real clients: DuckDB httpfs and curl.
#
# Builds a store with several row groups, two hours and three level buckets,
# serves it anonymously and then with credentials, and checks that DuckDB sees
# the same rows over S3 as over local disk, that a selective query reads a
# fraction of a file (measured from the server's own byte counter), and that a
# file deleted mid-download still finishes while the next request gets 404.
#
# Usage: scripts/s3-duckdb.sh [lines]
set -uo pipefail

LINES="${1:-600000}"
BIN="${LOGLESS_BIN:-./target/release/logless}"
DUCKDB="${DUCKDB:-/opt/homebrew/bin/duckdb}"
WORK="${S3_E2E_DIR:-$(mktemp -d)}"
AK=AKIALOGLESSTEST
SK=logless-test-secret
FAILS=0
SERVER_PID=""

[[ -x "$BIN" ]] || { echo "build first: cargo build --release" >&2; exit 1; }
[[ -x "$DUCKDB" ]] || { echo "duckdb not found at $DUCKDB" >&2; exit 1; }
mkdir -p "$WORK"

say() { printf '\n== %s\n' "$1"; }
check() { # name, expected, actual
  if [[ "$2" == "$3" ]]; then echo "ok    $1 ($3)"; else echo "FAIL  $1: expected $2, got $3"; FAILS=$((FAILS + 1)); fi
}
strip_ansi() { sed $'s/\033\\[[0-9;]*m//g'; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }

# start_server <data-dir> <port> [creds]; the log lands in $WORK/server.log.
start_server() {
  local dir="$1" port="$2" creds="${3:-}"
  if [[ -n "$creds" ]]; then
    LOGLESS_S3_ACCESS_KEY="$AK" LOGLESS_S3_SECRET_KEY="$SK" LOGLESS_LOG=debug \
      "$BIN" serve --config "$dir/config.toml" --serve-s3-addr "127.0.0.1:$port" > "$WORK/server.log" 2>&1 &
  else
    LOGLESS_LOG=debug "$BIN" serve --config "$dir/config.toml" --serve-s3-addr "127.0.0.1:$port" > "$WORK/server.log" 2>&1 &
  fi
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    curl -s -o /dev/null "http://127.0.0.1:$port/" && return 0
    sleep 0.1
  done
  echo "server did not come up" >&2; cat "$WORK/server.log" >&2; exit 1
}

# stop_server prints "requests range_requests bytes_served" from the exit line.
stop_server() {
  kill -TERM "$SERVER_PID" 2>/dev/null
  # Called inside $(...) or <(...), so the server is not our child and cannot be waited on.
  for _ in $(seq 1 100); do
    strip_ansi < "$WORK/server.log" | grep -q 's3 serve stopped' && break
    sleep 0.1
  done
  strip_ansi < "$WORK/server.log" | grep 's3 serve stopped' | tail -1 |
    sed -E 's/.*requests=([0-9]+) range_requests=([0-9]+) bytes_served=([0-9]+).*/\1 \2 \3/'
}
trap '[[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null' EXIT

duck() { # port, creds-key, sql
  local port="$1" key="$2" sql="$3"
  "$DUCKDB" -noheader -list -c "LOAD httpfs; SET s3_endpoint='127.0.0.1:$port'; SET s3_url_style='path'; SET s3_use_ssl=false; SET s3_region='us-east-1'; SET s3_access_key_id='$key'; SET s3_secret_access_key='${4:-$SK}'; $sql" 2>&1 | tail -1
}
local_duck() { "$DUCKDB" -noheader -list -c "$1" 2>&1 | tail -1; }

say "1. Build a store: $LINES lines, two hours, three level buckets"
rm -rf "$WORK/data"
"$BIN" init --data-dir "$WORK/data" > "$WORK/config.toml"
awk -v n="$LINES" 'BEGIN {
  srand(7)
  for (i = 0; i < n; i++) {
    t = sprintf("%032x", int(i / 12)); r = i % 500
    if (r == 0) printf "ERROR\t[api] payment timeout order=%d trace_id=%s\n", i, t
    else if (r % 37 == 0) printf "WARN\t[api] retry order=%d trace_id=%s\n", i, t
    else if (r % 3 == 0) printf "INFO\t[api] accepted order=%d total=%d.99 rnd=%d trace_id=%s\n", i, i % 400, int(rand() * 1e9), t
    else printf "DEBUG\t[api] cache key=order:%d hit=true latency=%f trace_id=%s\n", i, rand(), t
  }
}' > "$WORK/app.log"
"$BIN" run --config "$WORK/config.toml" < "$WORK/app.log" > /dev/null 2>&1
# The store is tenant=/index=/hour=<event hour>/level=/node=/. Everything here is the default tenant and index, and
# ingests into the current hour; a copy under the previous hour gives a second partition.
IDX="tenant=default/index=default"
cur=$(ls "$WORK/data/store/$IDX" | grep '^hour=' | head -1)
prev="hour=$(date -u -v-1H +%Y-%m-%dT%H 2>/dev/null || date -u -d '1 hour ago' +%Y-%m-%dT%H)"
[[ "$cur" == "$prev" ]] || cp -r "$WORK/data/store/$IDX/$cur" "$WORK/data/store/$IDX/$prev"
find "$WORK/data/store" -name '*.parquet' -exec ls -l {} \; | awk '{print $5, $9}' | sed "s#$WORK/data/store/##"
GLOB="$WORK/data/store/**/*.parquet"
LOCAL_COUNT=$(local_duck "select count(*) from read_parquet('$GLOB', hive_partitioning=true)")
echo "local rows: $LOCAL_COUNT"

TRACE=$(printf '%032x' 1000)
TRACE_SQL="trace_id = unhex('$TRACE')"
LOCAL_TRACE=$(local_duck "select count(*) from read_parquet('$GLOB', hive_partitioning=true) where $TRACE_SQL")
LOCAL_HOUR=$(local_duck "select count(*) from read_parquet('$GLOB', hive_partitioning=true) where hour='${prev#hour=}'")

run_duck_checks() { # port, key, secret
  local port="$1" key="$2" secret="${3:-$SK}"
  check "count(*) over S3 equals local" "$LOCAL_COUNT" "$(duck "$port" "$key" "select count(*) from read_parquet('s3://logs/**/*.parquet', hive_partitioning=true)" "$secret")"
  check "hour filter rows" "$LOCAL_HOUR" "$(duck "$port" "$key" "select count(*) from read_parquet('s3://logs/**/*.parquet', hive_partitioning=true) where hour='${prev#hour=}'" "$secret")"
  check "one index is one prefix; tenant, index and node are hive columns" "$LOCAL_COUNT default default 1" \
    "$(duck "$port" "$key" "select count(*) || ' ' || any_value(tenant) || ' ' || any_value(\"index\") || ' ' || count(distinct node) from read_parquet('s3://logs/$IDX/**/*.parquet', hive_partitioning=true)" "$secret")"
  check "trace_id equality rows" "$LOCAL_TRACE" "$(duck "$port" "$key" "select count(*) from read_parquet('s3://logs/**/*.parquet', hive_partitioning=true) where $TRACE_SQL" "$secret")"
}

say "2. DuckDB over anonymous serve"
PORT=$(free_port)
start_server "$WORK" "$PORT"
run_duck_checks "$PORT" "anon"
read -r REQ RNG BYTES < <(stop_server)
echo "server stats: requests=$REQ range_requests=$RNG bytes_served=$BYTES"

say "3. DuckDB over credentialed serve"
PORT=$(free_port)
start_server "$WORK" "$PORT" creds
run_duck_checks "$PORT" "$AK"
bad=$("$DUCKDB" -noheader -list -c "LOAD httpfs; SET s3_endpoint='127.0.0.1:$PORT'; SET s3_url_style='path'; SET s3_use_ssl=false; SET s3_region='us-east-1'; SET s3_access_key_id='$AK'; SET s3_secret_access_key='wrong-secret'; select count(*) from read_parquet('s3://logs/**/*.parquet', hive_partitioning=true)" 2>&1)
if grep -qiE '403|forbidden|signature' <<< "$bad"; then echo "ok    wrong secret rejected by DuckDB (403)"; else echo "FAIL  wrong secret was not rejected: $bad"; FAILS=$((FAILS + 1)); fi

say "4. curl checks (credentialed)"
LEAF="$IDX/$cur/level=debug/$(ls "$WORK/data/store/$IDX/$cur/level=debug" | head -1)"   # .../node=<this node>
KEY="$LEAF/$(ls "$WORK/data/store/$LEAF" | head -1)"
URL="http://127.0.0.1:$PORT/logs/$KEY"
SIZE=$(stat -f %z "$WORK/data/store/$KEY" 2>/dev/null || stat -c %s "$WORK/data/store/$KEY")
CURL=(curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK")
check "HEAD status" 200 "$("${CURL[@]}" -I -o /dev/null -w '%{http_code}' "$URL")"
check "HEAD content-length" "$SIZE" "$("${CURL[@]}" -I "$URL" | tr -d '\r' | awk 'tolower($1)=="content-length:" {print $2}')"
check "ranged GET status" 206 "$("${CURL[@]}" -r 0-99 -o "$WORK/r.bin" -w '%{http_code}' "$URL")"
check "ranged GET bytes" 100 "$(wc -c < "$WORK/r.bin" | tr -d ' ')"
check "ranged GET matches disk" "$(head -c 100 "$WORK/data/store/$KEY" | md5sum 2>/dev/null | cut -d' ' -f1 || head -c 100 "$WORK/data/store/$KEY" | md5)" "$(md5sum < "$WORK/r.bin" 2>/dev/null | cut -d' ' -f1 || md5 -q "$WORK/r.bin")"
check "range past EOF -> 416" 416 "$("${CURL[@]}" -r "$((SIZE + 10))-$((SIZE + 20))" -o /dev/null -w '%{http_code}' "$URL")"
check "missing key -> 404" 404 "$("${CURL[@]}" -o /dev/null -w '%{http_code}' "$URL.nope")"
check "bad signature -> 403" 403 "$(curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:not-the-secret" -o /dev/null -w '%{http_code}' "$URL")"
check "unsigned -> 403" 403 "$(curl -s -o /dev/null -w '%{http_code}' "$URL")"
check "PUT refused" 405 "$("${CURL[@]}" -X PUT -d x -o /dev/null -w '%{http_code}' "$URL")"
stop_server > /dev/null

say "5. Range-scan measurement: one file, one narrow column, selective filter"
echo "file: $KEY ($SIZE bytes)"
measure() { # label, sql
  PORT=$(free_port)
  start_server "$WORK" "$PORT"
  local out
  out=$(duck "$PORT" "anon" "$2")
  read -r REQ RNG BYTES < <(stop_server)
  printf '%-34s result=%s requests=%s ranged=%s bytes_served=%s (%s%% of %s)\n' "$1" "$out" "$REQ" "$RNG" "$BYTES" \
    "$(awk -v b="$BYTES" -v s="$SIZE" 'BEGIN {printf "%.2f", 100 * b / s}')" "$SIZE"
}
# A glob takes the object size from the listing; a bare key makes DuckDB HEAD for it first.
GLOBF="s3://logs/$LEAF/*.parquet"
measure "listed: template_id where trace_id=X" "select count(template_id) from read_parquet('$GLOBF') where $TRACE_SQL"
measure "listed: body, full scan" "select sum(length(body)) from read_parquet('$GLOBF')"
measure "listed: count(*) only (footer)" "select count(*) from read_parquet('$GLOBF')"
measure "bare key: template_id where trace_id=X" "select count(template_id) from read_parquet('s3://logs/$KEY') where $TRACE_SQL"

say "6. Race: delete a file while a download is in flight"
RACE="$WORK/race"
rm -rf "$RACE"; mkdir -p "$RACE"; cp -r "$WORK/data/store" "$RACE/store"; cp "$WORK/config.toml" "$RACE/config.toml"
sed -i.bak "s#^data_dir *= *.*#data_dir = \"$RACE\"#" "$RACE/config.toml"
grep -q "$RACE" "$RACE/config.toml" || { echo "FAIL  could not point the race config at its own dir"; FAILS=$((FAILS + 1)); }
PORT=$(free_port)
start_server "$RACE" "$PORT"
RURL="http://127.0.0.1:$PORT/logs/$KEY"
want=$(md5sum < "$WORK/data/store/$KEY" 2>/dev/null | cut -d' ' -f1 || md5 -q "$WORK/data/store/$KEY")
curl -s --limit-rate 1500k -o "$RACE/inflight.bin" "$RURL" &
CURL_PID=$!
sleep 1.5
rm -f "$RACE/store/$KEY"
wait "$CURL_PID"
got=$(md5sum < "$RACE/inflight.bin" 2>/dev/null | cut -d' ' -f1 || md5 -q "$RACE/inflight.bin")
check "in-flight download size after delete" "$SIZE" "$(wc -c < "$RACE/inflight.bin" | tr -d ' ')"
check "in-flight download content" "$want" "$got"
check "new GET after delete -> 404" 404 "$(curl -s -o /dev/null -w '%{http_code}' "$RURL")"
check "new HEAD after delete -> 404" 404 "$(curl -s -I -o /dev/null -w '%{http_code}' "$RURL")"
stop_server > /dev/null

say "result"
if [[ "$FAILS" -eq 0 ]]; then echo "all checks passed"; else echo "$FAILS check(s) failed"; exit 1; fi
