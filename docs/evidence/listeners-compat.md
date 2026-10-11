# The OTLP, HEC and Sentry listeners against real clients

Run on 2026-10-11 (BST) from branch `hard/listeners`, after moving the three listeners from `tiny_http` to
`bounded_http`. One real `logless` agent built from the branch (`cargo build -p logless-cli`, binary
`~/tmp/logless-hard-listeners-target/debug/logless`), all on 127.0.0.1, working directory a scratch folder.
Every output block is the command's real output, captured by a wrapper that prints `$ command` and then what
it printed (carriage returns stripped; `cat -v` shows the binary bytes of the protobuf `Status` bodies). The
machine's host name is shown as `<hostname>`.

What this checks: that the real Sentry SDKs for Node and Python get their envelopes accepted and stored through
the new listener (the Node SDK sends every body chunked, which the old `bounded_http` refused with 501); that
OTLP/HTTP takes protobuf and JSON, plain and gzip, as `curl` sends them; that HEC takes events, raw lines, gzip,
channels and ack polls as the Splunk documentation's `curl` examples send them; and that the abuse cases the unit
tests cover also hold against the built binary (stalled uploads, a 2 MiB header).

**Not available here:** Splunk Universal/Heavy Forwarders, the OpenTelemetry Collector and the OpenTelemetry SDKs.
HEC and OTLP were exercised with `curl` (curl 8.7.1) only. Whether
those clients send anything this listener now refuses (a `Transfer-Encoding` other than a single `chunked`, a
`Content-Type` other than `application/x-protobuf` or `application/json` on OTLP, an `Origin` header to a tokenless
HEC) is not tested.

## Setup

`logless init --data-dir data` with these changes (`config.toml`):

* `[otlp] enabled = true, addr = "127.0.0.1:14318"`
* `[hec] enabled = true, addr = "127.0.0.1:18088", tokens = ["compat-hec-token"], ack_enabled = true`
* `[sentry] enabled = true, addr = "127.0.0.1:19200"` (no `upstream_dsn`: a local recorder, so every envelope is
  stored and logged as "no upstream route; envelope held locally only")

Run as `logless run --config config.toml < hold` with a FIFO held open by `sleep`, as
`logless-studio/scripts/sentry-demo/run.sh` does, and stopped with `kill -TERM <pid>` (the PID it was started with).

Clients: `@sentry/node` 8.55.2 (`logless-studio/scripts/sentry-demo/node_modules`, Node v26.0.0) and
`sentry-sdk` 2.71.0 for Python (venv `/tmp/claude-501/sentry-venv`, no `brotli` module, so it compresses with gzip).

## 1. Sentry: the real SDKs

The Node demo app (40 simulated requests: transactions with spans, and the errors it raises), a one-error probe
with the real Node SDK whose 40 KB `extra` pushes the envelope past the SDK's 32 KiB gzip threshold (so it goes
gzip and chunked), and the Python worker (20 jobs):

```
$ SENTRY_DSN=http://demokey@127.0.0.1:19200/1 REQUESTS=40 node app.mjs
0/40 requests
done: 40 requests sent through the real @sentry/node SDK
$ SENTRY_DSN=http://probekey@127.0.0.1:19200/1 EXTRA=40000 node node-probe.mjs
flushed: true
$ JOBS=20 SENTRY_DSN=http://demokey@127.0.0.1:19200/2 python worker.py
done: 20 jobs sent through the real sentry-sdk for Python
exit=0
```

`flushed: true` is the SDK's own report that its transport delivered everything. The probe script:

```js
// The real @sentry/node SDK, one error with a 40 KB extra so the body passes the SDK's 32 KiB gzip threshold.
import * as Sentry from "@sentry/node";
Sentry.init({ dsn: process.env.SENTRY_DSN, release: "probe@1", tracesSampleRate: 0 });
Sentry.captureException(new Error("compat probe"), { extra: { blob: "x".repeat(Number(process.env.EXTRA ?? 0)) } });
const ok = await Sentry.flush(3000);
console.log(`flushed: ${ok}`);
```

How the Node SDK frames a request, captured by pointing the same probe at `nc -l 127.0.0.1 19299` (the request
head only):

```
POST /api/1/envelope/?sentry_version=7&sentry_key=probekey&sentry_client=sentry.javascript.node%2F8.55.2 HTTP/1.1
Host: 127.0.0.1:19299
Connection: keep-alive
Transfer-Encoding: chunked
```

The agent's log at shutdown (the 67 "no upstream route" lines, one per envelope, are left out), after sections
1 and 2 both ran against it:

```
2026-10-11T01:41:57.457330Z  INFO logless: otlp/http receiver listening addr=127.0.0.1:14318
2026-10-11T01:41:57.460461Z  INFO logless: sentry proxy listening addr=127.0.0.1:19200 mode=Relay upstream=false
2026-10-11T01:42:07.768027Z  INFO logless: shutdown signal received; draining
2026-10-11T01:42:07.768223Z  INFO logless: sentry proxy stopped requests=67 envelopes=67 items_received=67 items_forwarded=67 held_locally=0
2026-10-11T01:42:07.768391Z  INFO logless: hec receiver stopped requests=7 records=6 busy_503=0 unauthorized=3 bad_request=0 ack_queries=1
2026-10-11T01:42:07.768514Z  INFO logless: otlp receiver stopped requests=7 records=5 rejected_503=0 bad_request=2 too_large=0 dropped_upstream=0
2026-10-11T01:42:08.240418Z  INFO logless: merged wal segments merged=1 records=78 files=2 wal_freed=236115 templated=78 new_templates=0
received=78 enqueued=78 written=78 dropped_debug=0 dropped_info=0 dropped_critical=0 refused_debug_pressure=0
sentry: envelopes=67 items_received=67 items_forwarded=67 held_locally=0 sampled_out=0 bytes_received=241592
sentry upstream: envelopes_sent=0 items_sent=0 bytes_sent=0 rate_limited_items=0 failures=0 retries=0 | 0.00% of received bytes forwarded
hec: requests=7 records=6 busy_503=0 unauthorized=3 bad_request=0 too_large=0 ack_queries=1
otlp: requests=7 records=5 rejected_503=0 bad_request=2 too_large=0 dropped_upstream=0
pushdown: errors_forwarded=10 with_context=2 deduped=0 rate_limited=0 context_lines_held=78 ring_bytes=9736
```

(Later note, `hard/stab`: `items_forwarded=67 held_locally=0` beside `envelopes_sent=0` was a counting defect. With no
upstream route the proxy counted the items its policy kept as forwarded although none were sent. It now counts them
as held locally; see `docs/evidence/e2e-stack.md`.)

`requests=67 envelopes=67`, and nothing in `unauthorized`, `bad_request` or `too_large` in the Sentry line: every
envelope the two SDKs sent was accepted. What was stored, by service (Sentry events take the SDK's `server_name`;
transactions and sessions are `sentry:<project>`; project 1 is Node, project 2 is Python). These ran right after
the shutdown above, before section 3 added records to the same store; for the per-service counts the wrapper
printed the service and then the first line of `logless replay --config config.toml --since 30m --limit 1000
--service <service>`, and the last command shows its first two lines:

```
sentry:1 matched 42 records (scanned 78 rows in 2 files; 0 files and 0 row groups pruned)
sentry:2 matched 20 records (scanned 78 rows in 2 files; 0 files and 0 row groups pruned)
billing-worker-1 matched 2 records (scanned 78 rows in 2 files; 0 files and 0 row groups pruned)
checkout matched 5 records (scanned 78 rows in 2 files; 0 files and 0 row groups pruned)
nginx matched 2 records (scanned 78 rows in 2 files; 0 files and 0 row groups pruned)
manual matched 1 records (scanned 78 rows in 2 files; 0 files and 0 row groups pruned)
$ logless replay --config config.toml --since 30m --service <hostname>
matched 3 records (scanned 78 rows in 2 files; 0 files and 0 row groups pruned)
  1791682922621047000 17    <hostname> InventoryConflict: sku B07 reserved by another order
  1791682923708814000 17    <hostname> UpstreamTimeout: psp.example did not answer within 800ms
  1791682924386184000 17    <hostname> Error: compat probe
$ logless replay --config config.toml --since 30m --contains LedgerMismatch
matched 2 records (scanned 78 rows in 2 files; 0 files and 0 row groups pruned)
  1791682926403411000 17    billing-worker-1 LedgerMismatch: acct-1: ledger off by 1485.88
```

Node: 42 + 3 = 45 envelopes (under `sentry:1` the app's 40 transactions and two sessions, the app's and the
probe's; under `<hostname>` the app's two errors and the probe's error); Python: 20 transactions + 2 errors = 22. 45 + 22 = 67. On the OTLP and HEC side:
`checkout` is the five accepted OTLP requests (protobuf plain, gzip, chunked; JSON plain, gzip), `manual` and
`nginx` are HEC (the raw request was two lines). The probe's error (`Error: compat probe`) is the gzip-and-chunked
envelope.

## 2. OTLP/HTTP and HEC with curl

`logs.pb` is an `ExportLogsServiceRequest` written by a small script (`make_otlp.py`, field numbers from
`opentelemetry/proto/logs/v1/logs.proto`); `protoc --decode_raw < logs.pb` reads it as resource → scope →
record with `service.name`, a severity, a body, an attribute and trace/span ids. `logs.json` is the same in the
OTLP/JSON encoding (hex ids, `timeUnixNano` as a string, `intValue` as a string). The HEC requests follow Splunk's
documented `curl` examples (`-d` sends `application/x-www-form-urlencoded`, which HEC ignores).

```python
# Writes an OTLP ExportLogsServiceRequest (opentelemetry/proto/collector/logs/v1) in protobuf wire format.
import struct, sys, time
def varint(v):
    out = b""
    while True:
        b = v & 0x7F; v >>= 7
        if v: out += bytes([b | 0x80])
        else: return out + bytes([b])
def ld(field, payload): return varint(field << 3 | 2) + varint(len(payload)) + payload
def fixed64(field, v): return varint(field << 3 | 1) + struct.pack("<Q", v)
def vint(field, v): return varint(field << 3 | 0) + varint(v)
def anystr(s): return ld(1, s.encode())                       # AnyValue.string_value
def kv(k, v): return ld(1, k.encode()) + ld(2, anystr(v))     # KeyValue
now = time.time_ns()
record = (fixed64(1, now) + fixed64(11, now) + vint(2, 17) + ld(3, b"ERROR")
          + ld(5, anystr("payment declined (curl protobuf)")) + ld(6, kv("http.route", "/api/pay"))
          + ld(9, bytes.fromhex("5b8efff798038103d269b633813fc60c")) + ld(10, bytes.fromhex("eee19b7ec3c1b174")))
scope = ld(1, ld(1, b"compat-probe")) + ld(2, record)          # ScopeLogs{scope, log_records}
resource = ld(1, ld(1, kv("service.name", "checkout")))       # Resource{attributes}
sys.stdout.buffer.write(ld(1, resource + ld(2, scope)))       # ExportLogsServiceRequest.resource_logs
```

```
### OTLP/HTTP
$ curl -sS -i http://127.0.0.1:14318/v1/logs -H 'Content-Type: application/x-protobuf' --data-binary @logs.pb
HTTP/1.1 200 OK
Content-Type: application/x-protobuf
Content-Length: 0
Connection: close


$ curl -sS -i http://127.0.0.1:14318/v1/logs -H 'Content-Type: application/x-protobuf' -H 'Content-Encoding: gzip' --data-binary @logs.pb.gz
HTTP/1.1 200 OK
Content-Type: application/x-protobuf
Content-Length: 0
Connection: close


$ curl -sS -i http://127.0.0.1:14318/v1/logs -H 'Content-Type: application/json' --data-binary @logs.json
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 2
Connection: close

{}
$ curl -sS -i http://127.0.0.1:14318/v1/logs -H 'Content-Type: application/json' -H 'Content-Encoding: gzip' --data-binary @logs.json.gz
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 2
Connection: close

{}
$ curl -sS -i http://127.0.0.1:14318/v1/logs -H 'Content-Type: application/x-protobuf' -H 'Transfer-Encoding: chunked' --data-binary @logs.pb
HTTP/1.1 200 OK
Content-Type: application/x-protobuf
Content-Length: 0
Connection: close


$ curl -sS -i http://127.0.0.1:14318/v1/logs -H 'Content-Type: text/plain' --data-binary @logs.pb | head -c 300
HTTP/1.1 415 Unsupported Media Type
Content-Type: application/x-protobuf
Content-Length: 51
Connection: close

^H^C^R/send application/x-protobuf or application/json
$ curl -sS -i http://127.0.0.1:14318/v1/logs -H 'Host: evil.example' -H 'Content-Type: application/x-protobuf' --data-binary @logs.pb | head -c 300
HTTP/1.1 403 Forbidden
Content-Type: application/x-protobuf
Content-Length: 42
Connection: close

^H^G^R&the Host header is not a loopback name
### Splunk HEC
$ curl -sS -i http://127.0.0.1:18088/services/collector/health
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 35
Connection: close

{"text":"HEC is healthy","code":17}
$ curl -sS -i http://127.0.0.1:18088/services/collector -H 'Authorization: Splunk compat-hec-token' -d '{"event": "hello world", "sourcetype": "manual"}'
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 27
Connection: close

{"text":"Success","code":0}
$ curl -sS -i http://127.0.0.1:18088/services/collector/event -H 'Authorization: Splunk compat-hec-token' -H 'X-Splunk-Request-Channel: 0aeeac95-ac74-4aa9-b30d-6c4c0ac581ba' -d '{"event": "one", "host": "web-1"}{"event": {"msg": "two", "level": "error"}, "time": 1760140000.123}'
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 37
Connection: close

{"text":"Success","code":0,"ackId":0}
$ curl -sS -i 'http://127.0.0.1:18088/services/collector/ack?channel=0aeeac95-ac74-4aa9-b30d-6c4c0ac581ba' -H 'Authorization: Splunk compat-hec-token' -d '{"acks": [0]}'
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 20
Connection: close

{"acks":{"0":false}}
$ printf 'ERROR upstream timed out\nGET / 200\n' | curl -sS -i 'http://127.0.0.1:18088/services/collector/raw?channel=0aeeac95-ac74-4aa9-b30d-6c4c0ac581ba&sourcetype=nginx&host=web-2' -H 'Authorization: Splunk compat-hec-token' --data-binary @-
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 37
Connection: close

{"text":"Success","code":0,"ackId":1}
$ printf '{"event":"zipped"}' | gzip | curl -sS -i http://127.0.0.1:18088/services/collector -H 'Authorization: Splunk compat-hec-token' -H 'Content-Encoding: gzip' --data-binary @-
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 27
Connection: close

{"text":"Success","code":0}
$ curl -sS -i http://127.0.0.1:18088/services/collector -H 'Authorization: Splunk wrong' -d '{"event": "x"}'
HTTP/1.1 403 Forbidden
Content-Type: application/json
Content-Length: 33
Connection: close

{"text":"Invalid token","code":4}
$ curl -sS -i http://127.0.0.1:18088/services/collector -d '{"event": "x"}'
HTTP/1.1 401 Unauthorized
Content-Type: application/json
Content-Length: 37
Connection: close

{"text":"Token is required","code":2}
$ curl -sS -i http://127.0.0.1:18088/services/collector -H 'Authorization: Splunk compat-hec-token' -H 'Authorization: Splunk compat-hec-token' -d '{"event": "x"}'
HTTP/1.1 401 Unauthorized
Content-Type: application/json
Content-Length: 41
Connection: close

{"text":"Invalid authorization","code":3}
```

## 3. Abuse against the built binary

Four uploads held open on each listener (a head and 10 of 100 000 promised bytes, never finished), then one normal
request to each. On `tiny_http` with two workers, two of these stopped a listener answering (that was shown by
the unit tests failing on the old code, not by running the old binary):

```
$ python3 stall.py <the three curl commands below>
holding 12 stalled uploads
$ curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:14318/v1/logs -H 'Content-Type: application/x-protobuf' --data-binary @logs.pb
200  (0.01s)
$ curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:18088/services/collector -H 'Authorization: Splunk compat-hec-token' -d '{"event":"beside stalled"}'
200  (0.01s)
$ printf '{}\n{"type":"event"}\n{"message":"beside stalled"}\n' | curl -sS -o /dev/null -w '%{http_code}' 'http://127.0.0.1:19200/api/1/envelope/?sentry_key=k' --data-binary @-
200  (0.01s)
```

A 2 MiB header line, sent over a raw socket because `curl` refuses to build one (`curl: (100) HTTP request too
large`), then a normal request:

```
$ python3 bighead.py; curl ... http://127.0.0.1:14318/v1/logs ...
14318 HTTP/1.1 431 Request Header Fields Too Large | Content-Type: application/json | Content-Length: 29 | Connection: close |  | {"error":"headers_too_large"}
18088 HTTP/1.1 431 Request Header Fields Too Large | Content-Type: application/json | Content-Length: 29 | Connection: close |  | {"error":"headers_too_large"}
19200 HTTP/1.1 431 Request Header Fields Too Large | Content-Type: application/json | Content-Length: 29 | Connection: close |  | {"error":"headers_too_large"}
after: 200
```

The 431 body is `bounded_http`'s own, not an OTLP `Status` or a HEC `{"text","code"}`: a refusal of the head
happens before any protocol handler runs (the same holds for 408, 400 and 501 from the listener itself).
