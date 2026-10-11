#!/usr/bin/env python3
"""Extract log statements from N release tags of a Go service and render lines.

Statements are REAL (parsed from the tagged source); the lines are RENDERED:
message literal + key=value pairs from the statement's tag.* calls, with
synthetic values from a seeded RNG. Event volume per statement is fixed by
(stmt message, tag set) hash so unchanged statements get identical volume in
every release: volume never drives churn, only source edits do.

usage: churn-extract.py <repo> <outdir> <tag> [<tag> ...]
"""
import hashlib, io, json, os, random, re, subprocess, sys, tarfile
from pathlib import Path

repo, outdir, tags = sys.argv[1], Path(sys.argv[2]), sys.argv[3:]
# CHURN_VALUES=clean (default): every variable value is a shape the masker handles (number, uuid, ns-N,
# one-token error code). noisy: plain words and multi-word quoted errors, which Drain over-splits
# independent of any release change.
NOISY = os.environ.get('CHURN_VALUES', 'clean') == 'noisy'
outdir.mkdir(parents=True, exist_ok=True)
CALL = re.compile(r'([A-Za-z_][\w.()]*?)\.(Info|Warn|Error)\(\s*"((?:[^"\\\n]|\\.)*)"\s*([,)])')
RECV_OK = re.compile(r'(?i)(logger|log)\w*(\(\))?$')
TAGDEF = re.compile(r'func (\w+)\([^)]*\)\s*ZapTag\s*\{\s*return (New\w+)\("([^"]+)"', re.S)
ERRS = ["context deadline exceeded", "connection refused", "shard ownership lost",
        "workflow execution already completed", "namespace not found", "rpc error: code = Unavailable desc = closing transport",
        "operation timed out after retries", "persistence unavailable", "task queue partition not owned",
        "EOF", "history size exceeds limit", "no rows in result set"]

def read_tree(tag):
    data = subprocess.run(["git", "-C", repo, "archive", tag, "--", "service", "common", "tools", "temporal", "cmd"],
                          capture_output=True, check=True).stdout
    files = {}
    with tarfile.open(fileobj=io.BytesIO(data)) as tf:
        for m in tf:
            if m.isfile() and m.name.endswith(".go") and not m.name.endswith("_test.go"):
                files[m.name] = tf.extractfile(m).read().decode("utf8", "replace")
    return files

def call_args(src, start):
    """Text of the call argument list beginning just after the message literal."""
    depth, i, instr = 1, start, False
    while i < len(src) and depth:
        c = src[i]
        if instr:
            if c == "\\": i += 1
            elif c == '"': instr = False
        elif c == '"': instr = True
        elif c == "(": depth += 1
        elif c == ")": depth -= 1
        i += 1
    return src[start:i - 1]

def val(kind, rng):
    if kind == "NewStringTag" or kind == "NewAnyTag":
        if not NOISY: return str(rng.randint(10**7, 10**8)) if rng.random() < .5 else "%08x-%04x-%04x-%04x-%012x" % (rng.getrandbits(32), rng.getrandbits(16), rng.getrandbits(16), rng.getrandbits(16), rng.getrandbits(48))
        return rng.choice(["history", "matching", "frontend", "worker", "default", "payments", "ns-" + str(rng.randint(1, 40))])
    if kind in ("NewInt64", "NewInt32", "NewInt", "NewInt64Tag", "NewInt32Tag", "NewIntTag"): return str(rng.randint(0, 99999))
    if kind == "NewDurationTag": return f"{rng.randint(1,900)}ms"
    return "v" + str(rng.randint(0, 9999))

stats = {}
for tag in tags:
    files = read_tree(tag)
    tagkeys = {}
    for p, s in files.items():
        if p.endswith("log/tag/tags.go") or "/log/tag/" in p:
            for fn, kind, key in TAGDEF.findall(s):
                tagkeys[fn] = (key, kind)
    stmts, skipped = {}, 0
    for p, s in sorted(files.items()):
        for m in CALL.finditer(s):
            recv, lvl, msg, term = m.groups()
            if not RECV_OK.search(recv): skipped += 1; continue
            args = call_args(s, m.end()) if term == "," else ""
            tags_used = re.findall(r'\btag\.(\w+)\(', args)
            if re.search(r'\bzap\.\w+\("([^"]+)"', args):
                tags_used += ["zap:" + k for k in re.findall(r'\bzap\.\w+\("([^"]+)"', args)]
            key = (lvl, msg, tuple(tags_used))
            stmts.setdefault(key, p)
    lines, nstm = [], 0
    for (lvl, msg, tg), path in sorted(stmts.items()):
        h = int(hashlib.sha1(repr((lvl, msg, tg)).encode()).hexdigest(), 16)
        n = 3 + h % 18                       # 3..20 events, fixed per statement
        rng = random.Random(h)
        nstm += 1
        sidx = nstm - 1
        for i in range(n):
            kv = []
            for t in tg:
                if t.startswith("zap:"): k, kind = t[4:], "NewStringTag"
                else: k, kind = tagkeys.get(t, (re.sub(r'(?<!^)(?=[A-Z])', '-', t).lower(), "NewStringTag"))
                if k == "error": v = ('"' + rng.choice(ERRS) + '"') if NOISY else "E%d" % rng.randint(1000, 9999)
                else: v = val(kind, rng)
                kv.append(f"{k}={v}")
            ts = f"2026-03-{1 + i % 28:02d}T{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}.{rng.randint(0,999):03d}Z"
            lines.append((sidx, f"{ts} {lvl.upper()} {msg} {' '.join(kv)}".rstrip()))
    rng2 = random.Random(7); rng2.shuffle(lines)   # interleave; same seed each release
    (outdir / f"{tag}.log").write_text("\n".join(l for _, l in lines) + "\n")
    (outdir / f"{tag}.stmt").write_text("\n".join(str(i) for i, _ in lines) + "\n")
    (outdir / f"{tag}.stmts.json").write_text(json.dumps(
        [{"level": l, "msg": m, "tags": list(t), "file": p} for (l, m, t), p in sorted(stmts.items())], indent=0))
    stats[tag] = {"statements": nstm, "lines": len(lines), "go_files": len(files), "tagdefs": len(tagkeys), "non_logger_receiver_skipped": skipped}
    print(tag, stats[tag], file=sys.stderr)
(outdir / "extract-stats.json").write_text(json.dumps(stats, indent=1))
