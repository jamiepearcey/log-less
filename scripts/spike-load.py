#!/usr/bin/env python3
"""Load a running agent through HEC, then wait for it to drain.

Used by scripts/spike-console.sh and scripts/spike-drain-check.sh.

The old fixture stopped the agent the moment the last POST returned. A 200 from
HEC means "queued", not "synced to the WAL", so under load (or any slow disk or
busy CPU) the agent was told to stop with a backlog, and the loss it reported
was the fixture's own doing. This waits for a drain condition read from the
admin API, with a timeout, and prints exact counts as one JSON object:

  sent      events handed to HEC
  acked     events in a 200 response
  retries   503 responses retried (the whole batch is re-sent; never counted twice)
  drained   true when the drain condition held before the timeout
  received/written/dropped/queue_depth
            from GET /admin/v1/info at the end of the wait

Drain condition: received >= acked, and received == written + dropped (every
accepted record is in the WAL or was dropped by policy, so queue depth is 0),
seen on two consecutive polls so a transient zero does not count.

Usage: spike-load.py HEC_URL HEC_TOKEN ADMIN_URL ADMIN_TOKEN EVENTS BATCH
                     DRAIN_TIMEOUT_SECS [--ndjson FILE] [--no-drain]
"""
import json
import sys
import time
import urllib.error
import urllib.request


def synthetic(total):
    # Mixed severities and services; distinct trace ids so nothing dedupes.
    sev = ["DEBUG", "INFO", "INFO", "WARN", "ERROR"]
    now = int(time.time())
    for i in range(total):
        yield json.dumps({
            "time": now - (total - i) // 100,
            "sourcetype": "svc-%d" % (i % 7),
            "host": "node-%02d" % (i % 8),
            "event": "%s order %d handled trace_id=%032x" % (sev[i % 5], i, i // 3),
        }, separators=(",", ":")) + "\n"


def admin_info(url, token):
    req = urllib.request.Request(url + "/admin/v1/info",
                                 headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)["ingest"]


def main():
    import argparse
    ap = argparse.ArgumentParser()
    for name in ("hec", "token", "admin", "admin_token"):
        ap.add_argument(name)
    ap.add_argument("events", type=int)
    ap.add_argument("batch", type=int)
    ap.add_argument("drain_timeout", type=float)
    ap.add_argument("--ndjson")
    ap.add_argument("--no-drain", action="store_true")
    a = ap.parse_args()
    hec, token, admin, admin_token = a.hec, a.token, a.admin, a.admin_token
    events, batch, drain_timeout, ndjson = a.events, a.batch, a.drain_timeout, a.ndjson
    flags = ["--no-drain"] if a.no_drain else []
    lines = open(ndjson) if ndjson else synthetic(events)

    sent = acked = retries = 0
    started = time.time()

    def post(payload, n):
        nonlocal retries, acked
        delay = 0.05
        for _ in range(200):
            req = urllib.request.Request(
                hec, data=payload, method="POST",
                headers={"Authorization": "Splunk " + token})
            try:
                with urllib.request.urlopen(req, timeout=120):
                    acked += n
                    return
            except urllib.error.HTTPError as e:
                if e.code != 503:
                    sys.exit("HTTP %d: %s" % (e.code, e.read()[:200]))
                retries += 1
                time.sleep(min(delay, 1.0))
                delay *= 1.5
            except OSError as e:  # URLError is an OSError
                # Not retried: the batch may or may not have been accepted, and
                # re-sending it could store it twice. Report and stop.
                sys.exit("transport error after sent=%d acked=%d (batch of %d in doubt): %r"
                         % (sent, acked, n, e))
        sys.exit("gave up on 503")

    buf = []
    for line in lines:
        buf.append(line)
        if len(buf) >= batch:
            sent += len(buf)
            post("".join(buf).encode(), len(buf))
            buf = []
    if buf:
        sent += len(buf)
        post("".join(buf).encode(), len(buf))
    load_secs = time.time() - started

    drained, info, polls = False, {}, 0
    drain_started = time.time()
    if "--no-drain" not in flags:
        stable = 0
        while time.time() - drain_started < drain_timeout:
            polls += 1
            try:
                info = admin_info(admin, admin_token)
            except Exception as e:  # agent not answering is itself a result
                info = {"error": str(e)}
                time.sleep(0.2)
                continue
            dropped = sum(info["dropped_by_class"].values())
            if info["received"] >= acked and info["received"] == info["written"] + dropped:
                stable += 1
                if stable >= 2:
                    drained = True
                    break
            else:
                stable = 0
            time.sleep(0.1)
    else:
        try:
            info = admin_info(admin, admin_token)
        except Exception as e:
            info = {"error": str(e)}
    drain_secs = time.time() - drain_started

    dropped_by_class = info.get("dropped_by_class", {})
    dropped = sum(dropped_by_class.values()) if dropped_by_class else None
    received, written = info.get("received"), info.get("written")
    print(json.dumps({
        "sent": sent, "acked": acked, "retries": retries,
        "load_secs": round(load_secs, 2),
        "drain_waited": "--no-drain" not in flags,
        "drained": drained, "drain_secs": round(drain_secs, 2), "polls": polls,
        "received": received, "written": written,
        "dropped": dropped, "dropped_by_class": dropped_by_class,
        "queue_depth": (received - written - dropped)
        if None not in (received, written, dropped) else None,
    }))


if __name__ == "__main__":
    main()
