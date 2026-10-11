#!/usr/bin/env python3
"""How long S3 serve keeps a connection whose reader stopped reading a big download.

Usage: s3-serve-stall-time.py <logless binary> <config.toml>  (store as for s3-serve-failure-modes.py)
Three trials: start a 2 GiB GET, never read, poll netstat until the server side leaves ESTABLISHED.
"""
import socket, subprocess, sys, time, os
BIN, CFG = sys.argv[1], sys.argv[2]
s=socket.socket(); s.bind(("127.0.0.1",0)); PORT=s.getsockname()[1]; s.close()
env={k:v for k,v in os.environ.items() if not k.startswith("LOGLESS_S3_")}
a=subprocess.Popen([BIN,"serve","--config",CFG,"--serve-s3-addr",f"127.0.0.1:{PORT}"],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
try:
    time.sleep(1.5)
    for trial in range(3):
        c=socket.socket(); c.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64*1024); c.connect(("127.0.0.1",PORT))
        lp=c.getsockname()[1]
        c.sendall(f"GET /logs/hour=2026-10-01T00/level=info/big.parquet HTTP/1.1\r\nHost: 127.0.0.1:{PORT}\r\n\r\n".encode())
        t0=time.monotonic(); last=None; when=None
        while time.monotonic()-t0<90:
            ns=subprocess.run(f"netstat -an -p tcp | grep '127.0.0.1.{PORT} *127.0.0.1.{lp} '",shell=True,capture_output=True,text=True).stdout.split()
            sq=ns[2] if len(ns)>2 else None
            if sq!=last: last=sq
            if "ESTABLISHED" not in ns:
                when=time.monotonic()-t0; break
            time.sleep(0.5)
        print(f"trial {trial}: server side left ESTABLISHED after {when and round(when,1)} s (state {ns[-1] if ns else 'gone'})", flush=True)
        c.close()
finally:
    a.kill(); a.wait()
