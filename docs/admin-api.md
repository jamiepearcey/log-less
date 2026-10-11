# Admin API: runtime policy

A small JSON HTTP API inside the ingest process (`logless run`) for reading and
changing the operator-tunable policy while the agent runs: retention buckets,
the disk budget and watermarks, ingest queue settings and pushdown. It exists
so a console can show "what will this change do to my data" before applying it,
and keep a record of who changed what.

Off by default. It runs only inside `logless run` (not `serve`, `merge` or the
other one-shot commands), on its own small blocking HTTP/1.1 listener.

## Config

```
[admin]
enabled = false            # opt-in
addr = "127.0.0.1:9101"
token = "..."              # or LOGLESS_ADMIN_TOKEN (the environment wins)
allowed_hosts = []         # Host values accepted besides loopback names
```

Unknown keys under `[admin]` are a config error. `LOGLESS_ADMIN_TOKEN` wins
when it is set and non-empty; an empty variable is ignored and the file's
`token` is used. `allowed_hosts` entries are compared case-insensitively and
without a port. Debug output of the config shows the token as `<redacted>`.

The listener refuses to start when:

* there is no token (config or `LOGLESS_ADMIN_TOKEN`, non-empty);
* any address `addr` resolves to is not loopback and the token is shorter than
  24 characters.

Prefer the environment variable: a token in the config file is as readable as
the file.

## Security model

* Every request needs `Authorization: Bearer <token>`, including `GET`.
  The comparison is constant time (SHA-256 of both sides, then a fixed-length
  fold). Two `Authorization` headers count as none.
* The `Host` header must be exactly one well-formed `host[:port]` that is a
  loopback name (`localhost`, `127.x.x.x`, `::1`) or listed in
  `allowed_hosts`. Userinfo, trailing junk and duplicate `Host` headers are
  refused. It is checked before the token, so a DNS-rebinding page learns
  nothing.
* No CORS headers are ever sent, and `OPTIONS` is a 405. Browsers never talk to
  this listener; the console backend proxies it server side.
* No TLS. Keep it on loopback or behind a proxy.
* Request bodies are capped at 1 MiB, and the request head (request line plus
  headers) at 16 KiB and 64 headers; over that is a 431 before any
  authentication. The listener is its own small HTTP/1.1 server
  (`bounded_http.rs`) because `tiny_http` buffers headers without a limit; it
  answers with `Connection: close`, needs `Content-Length` for bodies, gives a
  client 10 s to send a request, and caps concurrent connections at 64.
* Failures before any endpoint logic: 403 `forbidden_host` (bad or repeated
  `Host`), 401 `unauthorized` (with `WWW-Authenticate: Bearer`), 404
  `not_found`, 405 `method_not_allowed`, 413 `too_large`, 431 (oversized head).
  Responses carry `Cache-Control: no-store`.

Listener addresses, credentials, destinations and tail sources stay in the
config file. They are not part of the policy document and cannot be changed here.

## The policy document

Durations are integer seconds or milliseconds as named.

```
{
  "retention": [{"name": "debug", "min_severity": 0, "retention_secs": 86400}, ...],
  "storage":   {"disk_budget_bytes": 34359738368, "high_water": 0.8,
                "critical_water": 0.95, "reclaim_target": 0.7},
  "ingest":    {"critical_enqueue_timeout_ms": 250, "queue_capacity": 65536},
  "pushdown":  {"enabled": true, "trigger_severity": 17}
}
```

Validation (each problem carries the `path` that breaks it):

* `retention`: at least one bucket, listed in strictly ascending
  `min_severity`; the first starts at 0; `min_severity` 0 to 24; names are 1 to
  64 ASCII letters, digits, `_`, `-` or `+` (they are directory names, so
  nothing that is a separator, whitespace, control or bidirectional character,
  and no Windows device name such as `CON`), and are unique ignoring case
  (`INFO` and `info` are one directory on macOS and Windows); `retention_secs` from 1 to 100
  years. The same invariants as `LevelBuckets`, which is also run as a final
  check.
* `storage`: `disk_budget_bytes > 0` and
  `0 < reclaim_target < high_water < critical_water <= 1`. These three
  fractions used to be constants in `budget.rs`; they are now `[storage]`
  fields (`high_water`, `critical_water`, `reclaim_target`) with the old values
  as defaults.
* `ingest`: `queue_capacity` 1 to 1,048,576; `critical_enqueue_timeout_ms` 0 to
  60,000.
* `pushdown.trigger_severity` 0 to 24.

### Apply modes

Every leaf path (`retention[1].retention_secs`, `storage.disk_budget_bytes`, ...)
is `hot` or `restart`. This was read off the code:

| Leaf | Mode | Why |
| --- | --- | --- |
| `retention[*]`, `storage.*` | hot | `maintenance_pass` re-reads the live policy every tick (5 s): merge, retention, budget and watermarks all use one snapshot per pass. A changed bucket map affects data merged after the change. |
| `pushdown.*` | hot | Read per record from atomics. |
| `ingest.critical_enqueue_timeout_ms` | hot | A shared atomic read on every WARN+ queue overflow. |
| `ingest.queue_capacity` | restart | The bounded channel is sized once, and the HEC admission check captured the number at startup. |

A `restart` change is persisted and reported in `pending_restart` until the
process restarts with it. Hot changes only take effect while maintenance is
running; `--no-maintenance` stores them and never enforces them (a warning is
logged at startup).

## File versus runtime

The TOML file supplies defaults. `data_dir/policy.json` overrides it field by
field:

```
{"schema_version": 1, "version": 3, "updated_at_ms": ..., "updated_by": "alice",
 "overrides": ["storage.disk_budget_bytes", "retention"],
 "document": { ...full effective policy at this version... }}
```

At startup the effective policy is the file's values with only the keys in
`overrides` replaced from `document`. A field nobody changed through the API
keeps following the config file, so editing the file and restarting still
works. Retention is overridden per field (`"retention[1].retention_secs"`)
while the bucket structure (count, names, severity floors) is the file's, so an
edit to an untouched bucket in the file still applies; a structural change
(add, remove, rename, re-order) is overridden as one unit (`"retention"`):
buckets are positional and interdependent. `sources` in `GET /policies` reports `file` or
`runtime` per leaf.

Only `logless run` and `logless status` read `policy.json`. The one-shot
`logless buckets`, `logless retention` and `logless merge` use the config file's
buckets alone, so after a runtime change they can disagree with the running
agent; `retention --dry-run` is not a preview of the agent's next pass (use
`POST /policies/preview` for that).

If a file edit makes an override invalid (for example the file's
`critical_water` drops below an overridden `high_water`), or `policy.json` is
corrupt, the agent refuses to start with a message saying which, rather than
silently picking one. A runtime policy applies whenever `policy.json` exists,
including when `[admin]` is later disabled.

Writes are atomic: temp file, fsync, rename, directory fsync. A crash leaves
the previous version or the new one.

Every applied change appends one line to `data_dir/policy-audit.jsonl`:
`ts_ms`, `actor`, `version`, `diff`, `impact_summary`, plus a `policies`
snapshot (what restore re-applies). If a crash lands `policy.json` without its
audit line, the line is written back at the next start (marked `recovered`,
diff unknown).

## Endpoints

All under `/admin/v1`.

### `GET /info`

```
{"version", "started_at_ms", "uptime_secs",
 "listeners": [{"kind", "addr"}],
 "disk": {"used_bytes", "budget_bytes", "fraction", "pressure"},
 "ingest": {"received", "written", "dropped_by_class": {"debug", "info", "critical", "closed", "refused_debug_pressure"}},
 "store": {"files", "rows", "bytes"}}
```

`written` is records synced to the WAL. `pressure` is `normal`, `high` or
`critical` against the live watermarks.

### `GET /typed-columns`

```
{"columns": [{"column", "key", "type", "service", "promoted_at"}]}
```

The registry of attribute keys promoted to typed Parquet columns
(`docs/typed-columns.md`). `type` is one of `int64`, `float64`, `bool`,
`string`, `timestamp`; `service` is the one charged for the promotion;
`promoted_at` is unix seconds. Read only; promotion is automatic.

### `GET /policies`

```
{"version", "updated_at_ms", "updated_by", "effective", "sources", "apply", "pending_restart", "running"}
```

`effective` is what is stored. For a restart-mode field that has not taken
effect, `running` maps its path to the value the process still enforces (for
example `{"ingest.queue_capacity": 65536}` while `effective` shows 1024), so a
reader of `effective` alone is not misled.

Version 0 means no runtime policy has been written (`updated_by` is `file`).
The `ETag` header carries the version.

### `POST /policies/preview`

Body `{"policies": ...}`. Always 200 for a well-formed body:

```
{"valid", "problems": [{"path", "message"}], "diff": [{"path", "from", "to", "apply"}],
 "impact": {"retention": {"would_expire_files", "would_expire_bytes", "per_bucket": [{"name", "files", "bytes"}]},
            "disk": {"used_bytes", "budget_bytes", "fraction_after", "would_reclaim"}},
 "destructive", "lossy_settings": [{"path", "message"}], "impact_token"}
```

When `valid` is false, `impact` is `null`, `destructive` is false and
`impact_token` is `null`.

`impact_token` is a hash of exactly the partitions and files this change newly
deletes plus the lossy settings. Send it back on `PUT` and restore with
`confirm_destructive: true`; if data arrived or expired since the preview the
token no longer matches and the write is refused with 428 `impact_changed` and
a fresh preview, so a confirmation cannot authorise a bigger deletion than the
one reviewed.

Impact is computed with the real functions, not a model: the retention dry-run
over the real store directory, then the real reclaim ordering over the real
catalog, mirroring a maintenance pass (expire by retention, re-measure against
the proposed budget, relieve pressure only if the proposed watermarks say so).
`would_expire_*` is everything the next pass would expire under the proposed
policy, including partitions already due under the current one. `would_reclaim`
is retention bytes plus early-reclaim bytes, and `fraction_after` is
`(used - would_reclaim) / proposed budget`.

`destructive` is narrower: true when the proposed policy deletes something the
current policy would not delete on its next pass. Shortening a retention or
shrinking the budget can be destructive; lengthening one, or a change next to
data that is already due, is not. Settings that lose data without deleting any
count too and are listed in `lossy_settings`: a queue capacity below 1024, a
critical enqueue timeout of 0, and turning pushdown off.

A preview scans the store, so only one scan (preview, `PUT` or restore) runs at
a time. A preview that finds one running gets `429 busy` with `Retry-After`;
writers wait their turn. The state lock is never held across a scan, so `GET`
endpoints stay instant.

### `PUT /policies`

Headers `If-Match: <version>`; body
`{"policies", "confirm_destructive": false, "impact_token", "actor"}`. `actor`
is required; the other two are optional. Success returns the same shape as
`GET /policies`, with the new version in the `ETag` header.

| Status | Meaning |
| --- | --- |
| 200 | Applied (or no change: no new version, no audit line). |
| 400 | Missing or malformed `If-Match`, or the body is not JSON. |
| 409 | Stale `If-Match`; body has `current_version`. |
| 422 | `{"error": "validation_failed", "problems": [...]}`: invalid document, unknown or mistyped fields, bad `actor`, or orphan protection. |
| 428 | Destructive and `confirm_destructive` is not true (`error: "destructive_unconfirmed"`), or confirmed with a missing or stale `impact_token` (`error: "impact_changed"`). The body is the fresh preview (`diff`, `impact`, `destructive: true`, `lossy_settings`, `impact_token`). |
| 429 | Not returned: a `PUT` waits for the running scan instead (only previews get `busy`). |

Orphan protection: renaming or removing a bucket that still has partitions on
disk is a 422 problem, even with `confirm_destructive`. Retention never deletes
a bucket name it does not know, so those partitions would be stranded; the
message says to keep the name until they expire or shorten its retention.

The check, persist, audit and publish steps happen under one lock, so two
writers cannot both succeed against the same version.

### `GET /policies/history?limit=N`

Newest first, default 50, at most 1000:
`[{"version", "ts_ms", "actor", "diff", "impact_summary"}]`.

### `POST /policies/restore`

Headers `If-Match: <version>` (required, like `PUT`); body
`{"version", "confirm_destructive", "impact_token", "actor"}`. Applies the snapshot recorded
for that version as a **new** version, through exactly the same validation,
orphan check, preview and confirmation rules as `PUT` (history is never
rewritten). 404 `unknown_version` if no such version was recorded. It returns
the same 400, 409, 422 and 428 answers as `PUT`. The audit
log is compacted to its newest 500 entries once it passes 8 MiB, so only those
versions can be restored.

`actor` may not contain control, line-separator or bidirectional-formatting
characters. A `policy.json` whose version is the maximum `u64` is refused at
load.

## Safety rules at a glance

* Destructive changes need `confirm_destructive: true` plus the `impact_token`
  of the preview the operator reviewed, else 428; a stale token is 428 again
  with a fresh preview.
* Orphan protection: a bucket with partitions on disk cannot be renamed or
  removed (422, not overridable).
* Every applied change is one audit line (`actor`, diff, impact summary,
  snapshot); no-op writes add nothing.
* Restore applies an old snapshot as a new version through the same checks.
* `If-Match` is required on `PUT` and restore; a lost race is a 409.

## Examples

```sh
export LOGLESS_ADMIN_TOKEN='a-long-random-secret'
A=http://127.0.0.1:9101/admin/v1
H="Authorization: Bearer $LOGLESS_ADMIN_TOKEN"

curl -s -H "$H" $A/info
curl -s -H "$H" $A/policies | jq .version,.pending_restart

# Preview: shorten the second bucket to one day.
curl -s -H "$H" $A/policies | jq '.effective' > cur.json
jq '.retention[1].retention_secs = 86400' cur.json > next.json
curl -s -H "$H" -d "{\"policies\": $(cat next.json)}" $A/policies/preview | tee prev.json | jq '.destructive,.impact.retention'

# Apply: If-Match is the version you read; destructive needs the token.
V=$(curl -s -H "$H" $A/policies | jq .version)
curl -s -X PUT -H "$H" -H "If-Match: $V" \
  -d "{\"policies\": $(cat next.json), \"actor\": \"alice\", \"confirm_destructive\": true, \"impact_token\": $(jq .impact_token prev.json)}" \
  $A/policies

curl -s -H "$H" "$A/policies/history?limit=5"

# Restore version 1 as a new version.
V=$(curl -s -H "$H" $A/policies | jq .version)
curl -s -X POST -H "$H" -H "If-Match: $V" \
  -d '{"version": 1, "actor": "alice"}' $A/policies/restore
```

## Operational notes

* `refused_debug_pressure` counts DEBUG refused at the door under critical disk
  pressure, which `dropped_by_class` would otherwise hide.
* The admin listener is stopped before the receivers at shutdown.
* Each request opens its own catalog connection (SQLite WAL), so reads never
  block maintenance.
