#!/usr/bin/env python3
"""Failure modes the console can hit against `logless serve --serve-s3-addr`, measured on the live binary.

Usage: failure-modes.py <logless binary> <config.toml>
Starts its own agent (anonymous, loopback) on a free port, runs every probe, prints JSON lines, kills the agent.
The store must hold hour=2026-10-01T00/level=info/big.parquet (2 GiB) and s000000000001-synth.parquet.
"""
import concurrent.futures as cf
import json
import os
import socket
import subprocess
import sys
import threading
import time

BIN, CFG = sys.argv[1], sys.argv[2]
BIG = "/logs/hour=2026-10-01T00/level=info/big.parquet"
SMALL = "/logs/hour=2026-10-01T00/level=info/s000000000001-synth.parquet"
BIG_LEN = 2 * 1024 ** 3


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


PORT = free_port()
env = {k: v for k, v in os.environ.items() if not k.startswith("LOGLESS_S3_")}
agent = subprocess.Popen([BIN, "serve", "--config", CFG, "--serve-s3-addr", f"127.0.0.1:{PORT}"], env=env,
                         stdout=subprocess.DEVNULL, stderr=open(os.path.join(os.path.dirname(CFG), "agent.log"), "w"))


def out(**kw):
    print(json.dumps(kw), flush=True)


def get(path, rng=None, timeout=60, read_limit=None):
    """One request on its own connection; returns (status, body bytes read, declared length, seconds)."""
    t = time.monotonic()
    c = socket.create_connection(("127.0.0.1", PORT), timeout=timeout)
    req = f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{PORT}\r\n" + (f"Range: bytes={rng}\r\n" if rng else "") + "\r\n"
    c.sendall(req.encode())
    buf = b""
    while b"\r\n\r\n" not in buf:
        d = c.recv(65536)
        if not d:
            c.close(); return 0, 0, 0, time.monotonic() - t
        buf += d
    head, _, rest = buf.partition(b"\r\n\r\n")
    lines = head.decode().split("\r\n")
    status = int(lines[0].split()[1])
    length = next((int(l.split(":", 1)[1]) for l in lines if l.lower().startswith("content-length:")), 0)
    got = len(rest)
    want = length if read_limit is None else min(length, read_limit)
    while got < want:
        d = c.recv(1 << 20)
        if not d:
            break
        got += len(d)
    c.close()
    return status, got, length, time.monotonic() - t


def threads():
    """Open TCP sockets held by the agent (listener included): a leaked connection would show here."""
    r = subprocess.run(["lsof", "-a", "-nP", "-p", str(agent.pid), "-iTCP"], capture_output=True, text=True)
    return max(0, len(r.stdout.strip().splitlines()) - 1)


def safe_get(*a, **kw):
    try:
        return get(*a, **kw)
    except OSError as e:
        return (-1, 0, 0, 0.0, type(e).__name__)


try:
    for _ in range(100):
        try:
            if get("/logs?list-type=2&max-keys=1", timeout=2)[0] == 200:
                break
        except OSError:
            pass
        time.sleep(0.1)
    out(probe="agent up", port=PORT, pid=agent.pid, tcp_sockets=threads())

    # 1. Big range reads: the whole 2 GiB object, and 512 MiB from the middle.
    for rng in ("0-", f"{BIG_LEN // 2}-{BIG_LEN // 2 + 512 * 1024 ** 2 - 1}"):
        st, got, length, secs = get(BIG, rng, timeout=120)
        out(probe="big range", range=rng, status=st, bytes=got, content_length=length, complete=got == length,
            seconds=round(secs, 2), mib_s=round(got / 1024 ** 2 / secs))

    # 2. 50 concurrent range requests (4 MiB each at spread offsets in the big object, plus 1 MiB of the real file).
    def one(i):
        if i % 2:
            off = (i * 37 * 1024 ** 2) % (BIG_LEN - 8 * 1024 ** 2)
            return get(BIG, f"{off}-{off + 4 * 1024 ** 2 - 1}")
        return get(SMALL, f"{i * 1024}-{i * 1024 + 1024 ** 2 - 1}")
    for n in (50, 128, 200):
        t = time.monotonic()
        with cf.ThreadPoolExecutor(n) as ex:
            res = list(ex.map(one, range(n)))
        ok = [r for r in res if r[0] == 206 and r[1] == r[2]]
        lat = sorted(r[3] for r in res)
        out(probe="concurrent ranges", clients=n, ok_206_complete=len(ok), refused_or_empty=sum(1 for r in res if r[0] == 0),
            other=[r[:3] for r in res if r not in ok and r[0] != 0][:5], wall_s=round(time.monotonic() - t, 2),
            p50_ms=round(lat[len(lat) // 2] * 1000), max_ms=round(lat[-1] * 1000), connection_cap=128)

    # 3. Clients that disconnect mid-download: 300 of them (more than the 128-connection cap), each reads 1 MiB of
    #    the 2 GiB object and hangs up. If a slot leaked per hang-up, later ones would be refused.
    before = threads()
    t = time.monotonic()
    aborted = [get(BIG, None, read_limit=1024 ** 2) for _ in range(300)]
    out(probe="mid-download disconnects", clients=300, answered_200=sum(1 for a in aborted if a[0] == 200),
        refused=sum(1 for a in aborted if a[0] == 0), seconds=round(time.monotonic() - t, 2))
    time.sleep(1.0)
    st, got, length, secs = get(SMALL)
    out(probe="after disconnects", status=st, complete=got == length, ms=round(secs * 1000), tcp_sockets_before=before, tcp_sockets_after=threads())

    # 4. A reader that stops reading a 2 GiB download: the 10 s write timeout should drop it, while others are served.
    #    On macOS loopback the stalled receiver's window still opens by a few bytes for about 15 s, and each such
    #    write is progress, so the drop comes at about 25 s (scripts/s3-serve-stall-time.py measures it); wait 32 s.
    stalled = socket.create_connection(("127.0.0.1", PORT))
    stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
    stalled.sendall(f"GET {BIG} HTTP/1.1\r\nHost: 127.0.0.1:{PORT}\r\n\r\n".encode())
    t0 = time.monotonic()
    served, worst = 0, 0.0
    while time.monotonic() - t0 < 32:
        st, got, length, secs = get(SMALL, "0-1048575", timeout=5)[:4]
        served += st == 206 and got == length
        worst = max(worst, secs)
        time.sleep(0.2)
    # Now drain what the stalled socket holds; the server should have closed it (EOF well short of 2 GiB).
    stalled.settimeout(5)
    held = 0
    try:
        while True:
            d = stalled.recv(1 << 20)
            if not d:
                break
            held += len(d)
    except socket.timeout:
        held = -held  # still open: no EOF after 5 s
    stalled.close()
    out(probe="stalled reader", others_served_while_stalled=served, others_worst_ms=round(worst * 1000),
        stalled_bytes_before_eof=held, cut_off=held > 0 and held < BIG_LEN)

    # 4b. A trickle reader: reads 16 KiB every 2 s, never pauses 10 s, so the write timeout never fires.
    trickle = socket.create_connection(("127.0.0.1", PORT))
    trickle.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
    trickle.sendall(f"GET {BIG} HTTP/1.1\r\nHost: 127.0.0.1:{PORT}\r\n\r\n".encode())
    t0 = time.monotonic(); alive = True; total = 0
    while time.monotonic() - t0 < 25:
        time.sleep(2)
        try:
            d = trickle.recv(16 * 1024)
        except OSError:
            alive = False; break
        if not d:
            alive = False; break
        total += len(d)
    trickle.close()
    out(probe="trickle reader (16 KiB / 2 s)", still_connected_after_s=round(time.monotonic() - t0, 1) if alive else None,
        dropped=not alive, bytes_read=total)

    # 4c. The cap filled by stalled readers: 128 connections that each start a 2 GiB download and stop reading. While
    #     they are held (before the write timeout fires), what does a new client get?
    hogs = []
    for _ in range(128):
        h = socket.create_connection(("127.0.0.1", PORT))
        h.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
        h.sendall(f"GET {BIG} HTTP/1.1\r\nHost: 127.0.0.1:{PORT}\r\n\r\n".encode())
        hogs.append(h)
    time.sleep(1.0)
    during = safe_get(SMALL, "0-1023", timeout=5)
    t0 = time.monotonic()
    recovered = None
    while time.monotonic() - t0 < 45:
        r = safe_get(SMALL, "0-1023", timeout=5)
        if r[0] == 206:
            recovered = round(time.monotonic() - t0 + 1.0, 1)
            break
        time.sleep(0.5)
    for h in hogs:
        h.close()
    out(probe="cap filled by 128 stalled readers", new_client_result_while_full=list(during),
        served_again_after_s=recovered)
    time.sleep(1.0)
    st, got, length, secs = get(SMALL)
    out(probe="end", status=st, complete=got == length, tcp_sockets=threads())
finally:
    agent.kill()
    agent.wait()
