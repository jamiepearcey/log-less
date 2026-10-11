#!/usr/bin/env python3
"""Connection-transparent counting proxy in front of logless serve's S3 port.

Unlike ~/tmp/s3-bytes/s3-count-proxy-log.py (which opens a fresh upstream connection per request and keeps the
client side alive regardless of what the server says), this one maps each client TCP connection to exactly one
upstream TCP connection and forwards every byte, including `Connection: close`. So the client (DuckDB httpfs in the
console) sees the server's connection behaviour as is, and the connection count here is the client's.

Same control endpoints as the original: GET /__stats, /__log, /__reset (answered locally, never counted).
/__stats adds: conns (client connections that carried at least one S3 request), close_replies (responses that said
Connection: close), truncated (responses whose body ended before Content-Length), upstream_errors, max_req_per_conn.
"""
import json
import socket
import sys
import threading

LISTEN = int(sys.argv[1])
UPSTREAM = int(sys.argv[2])
lock = threading.Lock()
ZERO = {"lists": 0, "gets": 0, "heads": 0, "other": 0, "bytes": 0, "conns": 0, "close_replies": 0, "truncated": 0,
        "upstream_errors": 0, "statuses": {}, "max_req_per_conn": 0}
stats = json.loads(json.dumps(ZERO))
log = []


def read_head(sock, buf):
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            return None, buf
        buf += chunk
    i = buf.index(b"\r\n\r\n") + 4
    return buf[:i], buf[i:]


def parse(head):
    lines = head[:-4].decode("latin-1").split("\r\n")
    hdrs = {}
    for l in lines[1:]:
        k, _, v = l.partition(":")
        hdrs[k.strip().lower()] = v.strip()
    return lines[0], hdrs


def local(client, path):
    if path == "/__stats":
        body = json.dumps(stats).encode()
    elif path == "/__log":
        body = json.dumps(log).encode()
    else:
        with lock:
            log.clear()
            stats.clear()
            stats.update(json.loads(json.dumps(ZERO)))
        body = b""
    client.sendall(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: %d\r\n\r\n" % len(body) + body)


def handle(client):
    up = None
    ubuf = b""
    cbuf = b""
    served = 0
    try:
        while True:
            head, cbuf = read_head(client, cbuf)
            if head is None:
                return
            line, hdrs = parse(head)
            method, target, _ = line.split(" ", 2)
            n = int(hdrs.get("content-length") or 0)
            while len(cbuf) < n:
                chunk = client.recv(65536)
                if not chunk:
                    return
                cbuf += chunk
            body, cbuf = cbuf[:n], cbuf[n:]
            path = target.split("?")[0]
            if path in ("/__stats", "/__log", "/__reset"):
                local(client, path)
                continue
            if up is None:
                up = socket.create_connection(("127.0.0.1", UPSTREAM))
                with lock:
                    stats["conns"] += 1
            up.sendall(head + body)
            rhead, ubuf = read_head(up, ubuf)
            if rhead is None:
                with lock:
                    stats["upstream_errors"] += 1
                return
            rline, rh = parse(rhead)
            status = int(rline.split(" ")[1])
            length = 0 if method == "HEAD" or status in (204, 304) else int(rh.get("content-length") or 0)
            client.sendall(rhead)
            sent = 0
            if ubuf:
                take = ubuf[:length]
                ubuf = ubuf[length:]
                client.sendall(take)
                sent += len(take)
            while sent < length:
                chunk = up.recv(min(1 << 20, length - sent))
                if not chunk:
                    break
                client.sendall(chunk)
                sent += len(chunk)
            served += 1
            closing = rh.get("connection", "").lower() == "close"
            is_list = method == "GET" and ("list-type" in target or "prefix=" in target)
            with lock:
                k = "lists" if is_list else {"GET": "gets", "HEAD": "heads"}.get(method, "other")
                stats[k] += 1
                stats["bytes"] += sent
                stats["close_replies"] += closing
                stats["truncated"] += sent < length
                stats["statuses"][str(status)] = stats["statuses"].get(str(status), 0) + 1
                stats["max_req_per_conn"] = max(stats["max_req_per_conn"], served)
                log.append({"m": method, "p": path.split("/store/")[-1][:60], "q": "list" if is_list else "",
                            "range": hdrs.get("range"), "status": status, "n": sent})
            if status >= 400:
                print(json.dumps({"status": status, "m": method, "target": target, "range": hdrs.get("range"), "served_on_conn": served}), file=sys.stderr, flush=True)
            if sent < length:
                return
            if closing:
                return
    except (ConnectionError, OSError, ValueError, IndexError):
        with lock:
            stats["upstream_errors"] += 1
    finally:
        for s in (client, up):
            if s is not None:
                try:
                    s.close()
                except OSError:
                    pass


srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", LISTEN))
srv.listen(256)
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
