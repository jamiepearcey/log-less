# Parquet parts against a real MinIO

Run on 2026-10-11 (BST) from branch `hard/parts`, against `minio RELEASE.2025-10-15T17-29-55Z` and
`mc RELEASE.2025-08-13T08-35-41Z`. Two real `logless` agents built from the branch
(`cargo build -p logless-cli`), one MinIO, all on 127.0.0.1, every directory under `R=~/tmp/hard-parts-run`.
Credentials and tokens were random (`openssl rand`), kept in a 0600 file outside the repo, and are shown
redacted below as `<minio-user>`, `<minio-secret>`, `<peer-token>`, `<admin-token>`. Every output block is
the command's real output, captured by a wrapper that prints `$ command` and then what it printed.

What this checks: that a part goes up once, named by its content and stored with that content's SHA-256
checksum; that a rerun puts nothing; that an object of the right size but other content under a part's
name is not taken as present but uploaded again; that MinIO itself refuses a body that does not match the
signed checksum; and that the central catalog survives a restart. One planned check did not come out as
planned: a byte flipped in MinIO's data file on disk is **not** seen by central (section 5).

## Setup

```zsh
minio server --address 127.0.0.1:19000 --console-address 127.0.0.1:19001 $R/minio-data   # MINIO_ROOT_USER/PASSWORD random
mc --config-dir $R/mc alias set hp http://127.0.0.1:19000 <minio-user> <minio-secret>
mc --config-dir $R/mc mb hp/logless-parts                     # Bucket created successfully `hp/logless-parts`.
mc --config-dir $R/mc admin trace hp --call s3 > $R/trace.log  # every S3 call MinIO serves, one line each
```

Configs: `logless init` output with these changes.

* **central** (`central.toml`): `[peer] enabled = true, addr = "127.0.0.1:19300", tokens = ["<peer-token>"]`;
  `[peer.parts] enabled = true, endpoint = "http://127.0.0.1:19000", bucket = "logless-parts", prefix = "edges",
  region = "us-east-1", access_key_id = "<minio-user>", secret_access_key = "<minio-secret>"`; `[admin]` on 19102.
  Run as `logless run --config central.toml --tail central-empty.log` (an empty file keeps it running).
* **edge** (`edge.toml`): `[admin]` on 19101, `wal_segment_bytes = 1048576` (so segments roll and merge into Parquet
  while it runs), and `[[destinations]] type = "logless", url = "http://127.0.0.1:19300", token = "<peer-token>"`.
  Run as `logless run --config edge.toml --tail edge.log`, then 60,000 synthetic lines (`INFO service=api request
  id=00000042 user=… took …ms path=…`, 4.66 MB) appended to `edge.log`.

The capability document central serves (`GET /.well-known/logless` with the peer token) said `"parts":true`. The edge
merged 7 Parquet files (`merged wal segments merged=7 records=59002 files=7`; the last 998 records stayed in the active
WAL segment, which a parts job does not read). The record path also delivered all 60,000 records to central
(`sent_records: 60000`), which has nothing to do with parts.

Helper scripts used below (in the run directory, not in the repo):

`job.sh` starts `POST /admin/v1/sync/parts` on the edge for a range from 2 hours before the run to 1 hour after, and
waits for it to finish:

```zsh
#!/bin/zsh
# Starts a parts job on the edge for the saved range, waits for it to finish, prints the parts status.
R=~/tmp/hard-parts-run; source $R/secrets.env; read FROM TO < $R/range
curl -s -o /dev/null -w "POST /admin/v1/sync/parts -> HTTP %{http_code}\n" -X POST -H "Authorization: Bearer $LOGLESS_ADMIN_TOKEN" -H 'Content-Type: application/json' -d "{\"from_ms\":$FROM,\"to_ms\":$TO}" http://127.0.0.1:19101/admin/v1/sync/parts
for i in $(seq 1 120); do
  s=$(curl -s -H "Authorization: Bearer $LOGLESS_ADMIN_TOKEN" http://127.0.0.1:19101/admin/v1/sync)
  st=$(echo "$s" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"]["parts"]["state"])')
  [ "$st" != running ] && break; sleep 0.5
done
echo "$s" | python3 -c 'import sys,json;p=json.load(sys.stdin)["status"]["parts"];print(json.dumps({k:p[k] for k in ["state","files_total","uploaded","skipped","already_present","rejected","bytes","error"]}))'
```


`calls.sh N` prints the `PutObject` and `HeadObject` calls in MinIO's trace after line N (the query string, which
holds the signature, cut off) and counts them:

```zsh
#!/bin/zsh
# Prints the S3 PutObject/HeadObject calls MinIO traced after line $1 of the trace, and their counts.
R=~/tmp/hard-parts-run
tail -n +$(($1+1)) $R/trace.log | grep -a "s3\.\(PutObject\|HeadObject\)" | tr -d '\000' | sed -E "s/\?X-Amz[^ ]*//; s/ +127\.0\.0\.1 .*//; s#127.0.0.1:19000/logless-parts/edges/##"
echo "PutObject: $(tail -n +$(($1+1)) $R/trace.log | grep -a -c 's3.PutObject')  HeadObject: $(tail -n +$(($1+1)) $R/trace.log | grep -a -c 's3.HeadObject')"
```


## 1. First run: seven parts go up

```text
$ curl -s -X POST -H "Authorization: Bearer $LOGLESS_ADMIN_TOKEN" -H 'Content-Type: application/json' -d "{\"from_ms\":$FROM,\"to_ms\":$TO}" http://127.0.0.1:19101/admin/v1/sync/parts
{"instance_id":"01a12880e3597180a1a837566e55e10e","source":"config","status":{ ... "parts":{"already_present":0,"bytes":0,"error":null,"files_total":0,"finished_ms":null,"from_ms":1791673721680,"rejected":0,"skipped":0,"started_ms":1791680921699,"state":"running","to_ms":1791684521680,"uploaded":0}, ... "sent_records":60000,"state":"ok"}}
$ sleep 5; curl -s -H "Authorization: Bearer $LOGLESS_ADMIN_TOKEN" http://127.0.0.1:19101/admin/v1/sync | python3 -m json.tool | sed -n '/"parts"/,/}/p'
        "parts": {
            "already_present": 0,
            "bytes": 1456894,
            "error": null,
            "files_total": 7,
            "finished_ms": 1791680921857,
            "from_ms": 1791673721680,
            "rejected": 0,
            "skipped": 0,
            "started_ms": 1791680921699,
            "state": "done",
            "to_ms": 1791684521680,
            "uploaded": 7
        },
```

(The first reply is cut with `...` here; the fields shown are as returned. This run was by hand, before `job.sh` existed.)

MinIO's trace for that run: 7 PUTs, each after a HEAD that found nothing (404) and followed by the commit's HEAD.
The first line (the 404 HEAD for `39d1e4b3…`) is missing from this listing: the trace file had been truncated while
`mc admin trace` held it open, so that line came after a run of NUL padding and was cut off by `cut -c1-160`. Counted
over the whole file at that point: `PutObject 7`, `HeadObject 14`. The last seven HEADs are from the `mc cat` checks below.

```text
$ grep -a "s3\.\(PutObject\|HeadObject\)" $R/trace.log | sed -E "s/\?X-Amz[^ ]*//; s/ +127\.0\.0\.1 .*//" | cut -c1-160

2026-10-11T02:08:41.722 [200 OK] s3.PutObject 127.0.0.1:19000/logless-parts/edges/39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet
2026-10-11T02:08:41.726 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet
2026-10-11T02:08:41.741 [404 Not Found] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parqu
2026-10-11T02:08:41.742 [200 OK] s3.PutObject 127.0.0.1:19000/logless-parts/edges/9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet
2026-10-11T02:08:41.745 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet
2026-10-11T02:08:41.763 [404 Not Found] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parqu
2026-10-11T02:08:41.765 [200 OK] s3.PutObject 127.0.0.1:19000/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
2026-10-11T02:08:41.768 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
2026-10-11T02:08:41.782 [404 Not Found] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parqu
2026-10-11T02:08:41.783 [200 OK] s3.PutObject 127.0.0.1:19000/logless-parts/edges/260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parquet
2026-10-11T02:08:41.790 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parquet
2026-10-11T02:08:41.804 [404 Not Found] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parqu
2026-10-11T02:08:41.805 [200 OK] s3.PutObject 127.0.0.1:19000/logless-parts/edges/9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parquet
2026-10-11T02:08:41.809 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parquet
2026-10-11T02:08:41.824 [404 Not Found] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parqu
2026-10-11T02:08:41.826 [200 OK] s3.PutObject 127.0.0.1:19000/logless-parts/edges/8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parquet
2026-10-11T02:08:41.829 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parquet
2026-10-11T02:08:41.844 [404 Not Found] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parqu
2026-10-11T02:08:41.846 [200 OK] s3.PutObject 127.0.0.1:19000/logless-parts/edges/560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet
2026-10-11T02:08:41.848 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet
2026-10-11T02:09:09.010 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parquet
2026-10-11T02:09:09.062 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet
2026-10-11T02:09:09.110 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet
2026-10-11T02:09:09.163 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parquet
2026-10-11T02:09:09.235 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parquet
2026-10-11T02:09:09.285 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet
2026-10-11T02:09:09.349 [200 OK] s3.HeadObject 127.0.0.1:19000/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
```


The listing, the SHA-256 of each stored object compared to its name, the edge's own files, and central's catalog:

```text
$ mc --config-dir $R/mc ls hp/logless-parts/edges/
[2026-10-11 02:08:41 BST] 203KiB STANDARD 260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parquet
[2026-10-11 02:08:41 BST] 201KiB STANDARD 39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet
[2026-10-11 02:08:41 BST] 204KiB STANDARD 560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet
[2026-10-11 02:08:41 BST] 200KiB STANDARD 8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parquet
[2026-10-11 02:08:41 BST] 202KiB STANDARD 9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parquet
[2026-10-11 02:08:41 BST] 204KiB STANDARD 9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet
[2026-10-11 02:08:41 BST] 209KiB STANDARD c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet

$ for o in $(mc --config-dir $R/mc ls --json hp/logless-parts/edges/ | python3 -c "import sys,json;[print(json.loads(l)[\"key\"]) for l in sys.stdin]"); do h=$(mc --config-dir $R/mc cat hp/logless-parts/edges/$o | shasum -a 256 | cut -d" " -f1); [ "$h.parquet" = "$o" ] && m=match || m=MISMATCH; echo "$o sha256=$h $m"; done
260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parquet sha256=260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805 match
39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet sha256=39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb match
560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet sha256=560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6 match
8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parquet sha256=8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007 match
9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parquet sha256=9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f match
9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet sha256=9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568 match
c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet sha256=c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d match

$ for f in $(find $R/edge/data/store -name "*.parquet" | sort); do echo "$(shasum -a 256 $f | cut -c1-64) $(stat -f %z $f) ${f#$R/}"; done
39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb 206223 edge/data/store/hour=2026-10-11T01/level=info/s000000000001-01a12880e35a.parquet
9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568 208629 edge/data/store/hour=2026-10-11T01/level=info/s000000000002-01a12880ec4a.parquet
c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d 214161 edge/data/store/hour=2026-10-11T01/level=info/s000000000003-01a12880ed10.parquet
260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805 207722 edge/data/store/hour=2026-10-11T01/level=info/s000000000004-01a12880eddc.parquet
9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f 207005 edge/data/store/hour=2026-10-11T01/level=info/s000000000005-01a12880eea2.parquet
8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007 204408 edge/data/store/hour=2026-10-11T01/level=info/s000000000006-01a12880ef84.parquet
560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6 208746 edge/data/store/hour=2026-10-11T01/level=info/s000000000007-01a12880f019.parquet

$ sqlite3 -header -column $R/central/data/catalog.sqlite "select * from peer_parts"
name                                                                      size    rows  min_observed         max_observed         origin                            committed_ms
------------------------------------------------------------------------  ------  ----  -------------------  -------------------  --------------------------------  -------------
39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet  206223  8322  1791680899948335000  1791680900162875000  01a12880e3597180a1a837566e55e10e  1791680921727
9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet  208629  8442  1791680900162898000  1791680900364122000  01a12880e3597180a1a837566e55e10e  1791680921747
c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet  214161  8651  1791680900364145000  1791680900566997000  01a12880e3597180a1a837566e55e10e  1791680921769
260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parquet  207722  8409  1791680900567019000  1791680900761613000  01a12880e3597180a1a837566e55e10e  1791680921791
9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parquet  207005  8381  1791680900761637000  1791680900953600000  01a12880e3597180a1a837566e55e10e  1791680921811
8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parquet  204408  8294  1791680900953622000  1791680901140137000  01a12880e3597180a1a837566e55e10e  1791680921830
560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet  208746  8503  1791680901140158000  1791680901332588000  01a12880e3597180a1a837566e55e10e  1791680921851
```


The checksum MinIO stored with an object is the base64 of the digest in its name (the second line is
`base64(bytes.fromhex(name))` worked out separately):

```text
$ mc --config-dir $R/mc stat hp/logless-parts/edges/39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet
Name      : 39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet
Date      : 2026-10-11 02:08:41 BST
Size      : 201 KiB
ETag      : c2b5f987d4984d518d90ed953613aa1b
Type      : file
Checksum  : SHA256:OdHks+Tut3+gua9ZgCCxXhi3UPJyBNqunCjA/RWMKMs=
Metadata  :
  Content-Type: binary/octet-stream
```

```text
OdHks+Tut3+gua9ZgCCxXhi3UPJyBNqunCjA/RWMKMs=
```

## 2. Second run: nothing goes up

With the edge's state file in place every file is skipped and storage is not asked anything:

```text
$ $R/job.sh
POST /admin/v1/sync/parts -> HTTP 200
{"state": "done", "files_total": 7, "uploaded": 0, "skipped": 7, "already_present": 0, "rejected": 0, "bytes": 0, "error": null}

$ $R/calls.sh $(cat $R/mark)
PutObject: 0  HeadObject: 0

$ cat $R/edge/data/parts-uploaded.jsonl | head -3; wc -l < $R/edge/data/parts-uploaded.jsonl
{"path":"hour=2026-10-11T01/level=info/s000000000001-01a12880e35a.parquet","size":206223,"name":"39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet"}
{"path":"hour=2026-10-11T01/level=info/s000000000002-01a12880ec4a.parquet","size":208629,"name":"9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet"}
{"path":"hour=2026-10-11T01/level=info/s000000000003-01a12880ed10.parquet","size":214161,"name":"c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet"}
       7
```


With the state file gone (moved aside), central HEADs each part with `x-amz-checksum-mode: ENABLED`, finds it intact,
answers `already_present`, and the commit HEADs it again. Zero PUTs; the catalog still has 7 rows:

```text
$ mv $R/edge/data/parts-uploaded.jsonl $R/parts-uploaded.jsonl.run1


$ $R/job.sh
POST /admin/v1/sync/parts -> HTTP 200
{"state": "done", "files_total": 7, "uploaded": 0, "skipped": 0, "already_present": 7, "rejected": 0, "bytes": 0, "error": null}

$ $R/calls.sh $(cat $R/mark)
2026-10-11T02:09:41.098 [200 OK] s3.HeadObject 39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet
2026-10-11T02:09:41.100 [200 OK] s3.HeadObject 39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet
2026-10-11T02:09:41.133 [200 OK] s3.HeadObject 9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet
2026-10-11T02:09:41.137 [200 OK] s3.HeadObject 9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet
2026-10-11T02:09:41.151 [200 OK] s3.HeadObject c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
2026-10-11T02:09:41.152 [200 OK] s3.HeadObject c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
2026-10-11T02:09:41.167 [200 OK] s3.HeadObject 260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parquet
2026-10-11T02:09:41.168 [200 OK] s3.HeadObject 260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parquet
2026-10-11T02:09:41.182 [200 OK] s3.HeadObject 9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parquet
2026-10-11T02:09:41.183 [200 OK] s3.HeadObject 9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parquet
2026-10-11T02:09:41.198 [200 OK] s3.HeadObject 8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parquet
2026-10-11T02:09:41.199 [200 OK] s3.HeadObject 8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parquet
2026-10-11T02:09:41.215 [200 OK] s3.HeadObject 560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet
2026-10-11T02:09:41.222 [200 OK] s3.HeadObject 560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet
PutObject: 0  HeadObject: 14

$ sqlite3 $R/central/data/catalog.sqlite "select count(*) from peer_parts"
7
```


## 3. An object of the right size with other content is replaced

The defect this branch fixes: central took any object of the claimed size as the part. Here the object for
`c1910a92…` (214,161 bytes) is overwritten through `mc` with the same file with one byte flipped: same size, another
SHA-256, and `mc cp` stores no checksum for it. Then a run with the state file removed:

```text
$ python3 -c 'import sys;b=bytearray(open(sys.argv[1],"rb").read());b[100000]^=0xff;open(sys.argv[2],"wb").write(b)' '/Users/jamiepearcey/tmp/hard-parts-run/edge/data/store/hour=2026-10-11T01/level=info/s000000000003-01a12880ed10.parquet' $R/wrong.parquet; stat -f '%z bytes' $R/wrong.parquet; shasum -a 256 $R/wrong.parquet | cut -c1-64
214161 bytes
135a7c99b1499699800518896b8ed0e1b339aae1e993c62915a5b51950509a40

$ mc --config-dir $R/mc cp --quiet $R/wrong.parquet hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet >/dev/null && mc --config-dir $R/mc stat hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet | grep -E 'Size|Checksum'; mc --config-dir $R/mc cat hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet | shasum -a 256 | cut -c1-64
Size      : 209 KiB
135a7c99b1499699800518896b8ed0e1b339aae1e993c62915a5b51950509a40

$ $R/job.sh
POST /admin/v1/sync/parts -> HTTP 200
{"state": "done", "files_total": 7, "uploaded": 1, "skipped": 0, "already_present": 6, "rejected": 0, "bytes": 214161, "error": null}

$ $R/calls.sh $(cat $R/mark) | grep -v 'HeadObject 39d1\|HeadObject 9f46\|HeadObject 260d\|HeadObject 9c66\|HeadObject 8899\|HeadObject 560a'
2026-10-11T02:10:51.399 [200 OK] s3.HeadObject c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
2026-10-11T02:10:51.401 [200 OK] s3.PutObject c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
2026-10-11T02:10:51.414 [200 OK] s3.HeadObject c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
PutObject: 1  HeadObject: 14

$ mc --config-dir $R/mc stat hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet | grep -E 'Size|Checksum'; mc --config-dir $R/mc cat hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet | shasum -a 256 | cut -c1-64
Size      : 209 KiB
Checksum  : SHA256:wZEKkuc5m/qeVgcVIsuoVypfBCnSC2cb9+8Xp3PMiW0=
c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d

$ sqlite3 $R/central/data/catalog.sqlite "select count(*) from peer_parts"
7
```


Central did not take it as present: the HEAD showed no matching checksum, so presign handed out a URL, the edge PUT
the real file, and the commit's HEAD found size and checksum right. The object is the real file again and its stored
checksum is the name's. The other six were `already_present`.

The same with the wrong bytes uploaded *with their own, correct* checksum (`mc cp --checksum SHA256`), so MinIO holds
a valid SHA-256 checksum, just not the one the name promises:

```text
$ mc --config-dir $R/mc cp --quiet --checksum SHA256 $R/wrong.parquet hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet >/dev/null && mc --config-dir $R/mc stat hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet | grep -E 'Size|Checksum'; python3 -c "import base64;print(base64.b64encode(bytes.fromhex('135a7c99b1499699800518896b8ed0e1b339aae1e993c62915a5b51950509a40')).decode(), '= base64 of the wrong bytes sha256')"
Size      : 209 KiB
Checksum  : SHA256:E1p8mbFJlpmABRiJa47Q4bM5quHpk8YpFaW1GVBQmkA=
E1p8mbFJlpmABRiJa47Q4bM5quHpk8YpFaW1GVBQmkA= = base64 of the wrong bytes sha256

$ $R/job.sh
POST /admin/v1/sync/parts -> HTTP 200
{"state": "done", "files_total": 7, "uploaded": 1, "skipped": 0, "already_present": 6, "rejected": 0, "bytes": 214161, "error": null}

$ $R/calls.sh $(cat $R/mark) | grep -v 'HeadObject 39d1\|HeadObject 9f46\|HeadObject 260d\|HeadObject 9c66\|HeadObject 8899\|HeadObject 560a'
2026-10-11T02:11:01.170 [200 OK] s3.HeadObject c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
2026-10-11T02:11:01.172 [200 OK] s3.PutObject c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
2026-10-11T02:11:01.183 [200 OK] s3.HeadObject c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet
PutObject: 1  HeadObject: 14

$ mc --config-dir $R/mc stat hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet | grep -E 'Size|Checksum'; mc --config-dir $R/mc cat hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet | shasum -a 256 | cut -c1-64
Size      : 209 KiB
Checksum  : SHA256:wZEKkuc5m/qeVgcVIsuoVypfBCnSC2cb9+8Xp3PMiW0=
c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d
```


## 4. MinIO refuses a body that does not match the signed checksum

A fresh part (4 KiB of random bytes X) presigned by central over the peer API, then PUT straight to MinIO with curl.
The script:

```zsh
#!/bin/zsh
# Presigns a fresh part (random 4 KiB X) from central, then PUTs to MinIO: Y with the header, X without it,
# Y with a header changed to match Y, and X with the header. Then tries commit.
R=~/tmp/hard-parts-run; source $R/secrets.env
head -c 4096 /dev/urandom > $R/x.bin; head -c 4096 /dev/urandom > $R/y.bin
SHA=$(shasum -a 256 $R/x.bin | cut -c1-64); N=$SHA.parquet
REPLY=$(curl -s -H "Authorization: Bearer $PEER_TOKEN" -H 'Content-Type: application/json' -d "{\"name\":\"$N\",\"size\":4096,\"sha256\":\"$SHA\"}" http://127.0.0.1:19300/v1/parts/presign)
echo "presign reply (url shortened): $(echo $REPLY | python3 -c 'import sys,json;r=json.load(sys.stdin);u=r["url"];r["url"]=u.split("?")[0]+"?...&X-Amz-SignedHeaders="+u.split("X-Amz-SignedHeaders=")[1].split("&")[0]+"&...";print(json.dumps(r))')"
URL=$(echo $REPLY | python3 -c 'import sys,json;print(json.load(sys.stdin)["url"])')
H=$(echo $REPLY | python3 -c 'import sys,json;print(json.load(sys.stdin)["headers"]["x-amz-checksum-sha256"])')
HY=$(python3 -c "import hashlib,base64;print(base64.b64encode(hashlib.sha256(open('$R/y.bin','rb').read()).digest()).decode())")
put() { curl -s -o $R/put.out -w "%{http_code}" -X PUT -T "$1" "${@:2}" "$URL"; echo " $(grep -o '<Code>[^<]*' $R/put.out | sed 's/<Code>//')"; }
echo "PUT Y (same size, other bytes) with the signed header:  $(put $R/y.bin -H "x-amz-checksum-sha256: $H")"
echo "PUT X without the header:                               $(put $R/x.bin)"
echo "PUT Y with the header changed to Y's checksum:          $(put $R/y.bin -H "x-amz-checksum-sha256: $HY")"
C="{\"name\":\"$N\",\"size\":4096,\"sha256\":\"$SHA\",\"rows\":1,\"min_unix_nano\":1,\"max_unix_nano\":2,\"origin\":\"manual\"}"
echo "commit before a good PUT: $(curl -s -w ' HTTP %{http_code}' -H "Authorization: Bearer $PEER_TOKEN" -H 'Content-Type: application/json' -d "$C" http://127.0.0.1:19300/v1/parts/commit)"
echo "PUT X with the signed header:                           $(put $R/x.bin -H "x-amz-checksum-sha256: $H")"
echo "commit after:             $(curl -s -w ' HTTP %{http_code}' -H "Authorization: Bearer $PEER_TOKEN" -H 'Content-Type: application/json' -d "$C" http://127.0.0.1:19300/v1/parts/commit)"
echo $N > $R/manual-name
```


```text
$ $R/putcheck.sh
presign reply (url shortened): {"already_present": false, "url": "http://127.0.0.1:19000/logless-parts/edges/4b7e53211d022345859609f0709bfb51cba23105b338b1c933a93bce6872f196.parquet?...&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256&...", "headers": {"x-amz-checksum-sha256": "S35TIR0CI0WFlgnwcJv7UcuiMQWzOLHJM6k7zmhy8ZY="}, "expires_at_ms": 1791681982000}
PUT Y (same size, other bytes) with the signed header:  400 XAmzContentChecksumMismatch
PUT X without the header:                               400 AccessDenied
PUT Y with the header changed to Y's checksum:          403 SignatureDoesNotMatch
commit before a good PUT: {"error":"not_uploaded","message":"no object of that name, size and SHA-256 checksum is in storage; upload it first"} HTTP 409
PUT X with the signed header:                           200
commit after:             {"recorded":true} HTTP 200
```


MinIO checks the body against the header (`XAmzContentChecksumMismatch`), and the header cannot be dropped (the URL
signs it; MinIO answers 400 `AccessDenied`) or swapped for another value (`SignatureDoesNotMatch`). Central refused
the commit until the right bytes were stored. This manual part stays in the bucket and the catalog as an eighth row
with `origin = manual`.

## 5. A byte flipped in MinIO's data file on disk: not detected

The object for `c1910a92…` is stored by MinIO (single drive) as `xl.meta` plus a `part.1` of 214,193 bytes (the
214,161 bytes of the part and a 32-byte bitrot hash). One byte flipped in place, size unchanged:

```text
$ source /Users/jamiepearcey/tmp/hard-parts-run/partpath.sh; stat -f '%z bytes' $P; shasum -a 256 $P
214193 bytes
efe6a8c08ba2e9ce909190a1843022e165ec902790716a92778032809647fe26  /Users/jamiepearcey/tmp/hard-parts-run/minio-data/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet/0ca55c20-cacc-4f17-988a-0a95199bdf40/part.1

$ source /Users/jamiepearcey/tmp/hard-parts-run/partpath.sh; python3 -c 'import sys;p=sys.argv[1];b=bytearray(open(p,"rb").read());print("byte at 100000:",hex(b[100000]));b[100000]^=0xff;open(p,"wb").write(b);print("now:",hex(b[100000]))' $P; stat -f '%z bytes' $P; shasum -a 256 $P
byte at 100000: 0xc4
now: 0x3b
214193 bytes
4bcea5d440065645fc27e3f2555a2a2cb4279c25a59268d1fc267690156f42bc  /Users/jamiepearcey/tmp/hard-parts-run/minio-data/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet/0ca55c20-cacc-4f17-988a-0a95199bdf40/part.1

$ mc --config-dir $R/mc stat hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet | grep -E 'Size|Checksum'
Size      : 209 KiB
Checksum  : SHA256:wZEKkuc5m/qeVgcVIsuoVypfBCnSC2cb9+8Xp3PMiW0=

$ mc --config-dir $R/mc cat hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet | shasum -a 256; echo exit=$?
mc: <ERROR> Unable to read from `hp/logless-parts/edges/c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet`. Resource requested is unreadable, please reduce your request rate.
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  -
exit=0
```


MinIO's HEAD still answers the size and the checksum recorded at upload time (they are metadata, not recomputed),
while a read of the object fails (MinIO's bitrot check refuses to serve it; `mc cat` gave no bytes, hence the SHA-256 of
nothing). A run with the state file removed therefore found all seven `already_present` and uploaded nothing:

```text
$ $R/job.sh
POST /admin/v1/sync/parts -> HTTP 200
{"state": "done", "files_total": 7, "uploaded": 0, "skipped": 0, "already_present": 7, "rejected": 0, "bytes": 0, "error": null}

$ $R/calls.sh $(cat $R/mark) | tail -3
2026-10-11T02:10:34.469 [200 OK] s3.HeadObject 560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet
2026-10-11T02:10:34.470 [200 OK] s3.HeadObject 560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet
PutObject: 0  HeadObject: 14
```


So central's check sees what the storage reports about an object, not the bytes at rest. Bit rot under MinIO is caught
by MinIO when the object is read: a reader gets an error, never the wrong bytes. Central catching it would mean reading
every part it calls present (a GET and hash per part on each presign or commit), which this branch does not do. The
object was repaired in section 3, which ran after this: overwriting it there replaced the damaged `part.1`.

## 6. Central restarted: the catalog is intact

`SIGTERM` stopped central's ingest, peer and admin servers, then the process did not exit: a sampled stack showed the
main thread at `main.rs:1728` joining the `logless-sync` thread, which sits in `sync::run` on `recv_timeout`. The same
happened to the edge at the end of the run. That is a shutdown defect in the sync thread, not in parts; here central was
killed with `SIGKILL`, a harder restart for the SQLite catalog than a clean stop.

```text
$ sqlite3 $R/central/data/catalog.sqlite "select count(*), sum(size), sum(rows) from peer_parts"
8|1460990|59003

$ kill -TERM $(cat $R/central.pid); for i in $(seq 1 60); do kill -0 $(cat $R/central.pid) 2>/dev/null || break; sleep 0.5; done; kill -0 $(cat $R/central.pid) 2>/dev/null && echo still running || echo central stopped
still running

(SIGTERM drained ingest, peer and admin, then hung joining the logless-sync thread; see caveat. Sampled stack: main.rs:1728 JoinHandle::join on logless-sync, parked in sync::run recv_timeout.)

$ kill -KILL $(cat $R/central.pid); sleep 1; kill -0 $(cat $R/central.pid) 2>/dev/null && echo still running || echo central killed

$ curl -s -H "Authorization: Bearer $PEER_TOKEN" http://127.0.0.1:19300/.well-known/logless | python3 -c "import sys,json;d=json.load(sys.stdin);print({k:d[k] for k in [\"instance_id\",\"parts\"]})"
{'instance_id': '01a12878ec747a118a72856cbbbd8f67', 'parts': True}

$ sqlite3 $R/central/data/catalog.sqlite "select count(*), sum(size), sum(rows) from peer_parts"
8|1460990|59003

$ $R/job.sh
POST /admin/v1/sync/parts -> HTTP 200
{"state": "done", "files_total": 7, "uploaded": 0, "skipped": 0, "already_present": 7, "rejected": 0, "bytes": 0, "error": null}

$ $R/calls.sh $(cat $R/mark) | tail -1
PutObject: 0  HeadObject: 14

$ sqlite3 $R/central/data/catalog.sqlite "select count(*), sum(size), sum(rows) from peer_parts"
8|1460990|59003

$ curl -s -w ' HTTP %{http_code}' -H "Authorization: Bearer $PEER_TOKEN" -H 'Content-Type: application/json' -d '{"name":"4b7e53211d022345859609f0709bfb51cba23105b338b1c933a93bce6872f196.parquet","size":4096,"sha256":"4b7e53211d022345859609f0709bfb51cba23105b338b1c933a93bce6872f196","rows":1,"min_unix_nano":1,"max_unix_nano":2,"origin":"manual"}' http://127.0.0.1:19300/v1/parts/commit
{"recorded":false} HTTP 200
```


Same instance id, the same 8 rows (7 parts and the manual one; 59,003 rows is the edge's 59,002 plus the manual part's
1), a run with the state file removed put nothing, and a repeated commit recorded nothing new.

## End state

```text
$ mc --config-dir $R/mc ls hp/logless-parts/edges/
[2026-10-11 02:08:41 BST] 203KiB STANDARD 260da652d5f419d7422dd7a20f87cfe610fbb0833308d7e96aea7bc6517d8805.parquet
[2026-10-11 02:08:41 BST] 201KiB STANDARD 39d1e4b3e4eeb77fa0b9af598020b15e18b750f27204daae9c28c0fd158c28cb.parquet
[2026-10-11 02:11:26 BST] 4.0KiB STANDARD 4b7e53211d022345859609f0709bfb51cba23105b338b1c933a93bce6872f196.parquet
[2026-10-11 02:08:41 BST] 204KiB STANDARD 560af771e46018e461057e5b0863faaf14d9d5ee1dab8caff4e76f4608048af6.parquet
[2026-10-11 02:08:41 BST] 200KiB STANDARD 8899d2dd62a1a366f8cf9335a47aa3786930b7b3625a907a524b3fbdd8c55007.parquet
[2026-10-11 02:08:41 BST] 202KiB STANDARD 9c66c7832f5a1428fea7df738b94c02bcbd01e21d62f6a315567c3342fe3cb2f.parquet
[2026-10-11 02:08:41 BST] 204KiB STANDARD 9f467d05a81951b73c08609e47d40a4aa0544ee05ca48f05afa955b6b8339568.parquet
[2026-10-11 02:11:01 BST] 209KiB STANDARD c1910a92e7399bfa9e56071522cba8572a5f0429d20b671bf7ef17a773cc896d.parquet

$ for o in $(mc --config-dir $R/mc ls --json hp/logless-parts/edges/ | python3 -c "import sys,json;[print(json.loads(l)[\"key\"]) for l in sys.stdin]"); do h=$(mc --config-dir $R/mc cat hp/logless-parts/edges/$o | shasum -a 256 | cut -c1-64); c=$(mc --config-dir $R/mc stat --json hp/logless-parts/edges/$o | python3 -c "import sys,json;print(json.load(sys.stdin).get(\"checksum\",{}).get(\"SHA256\",\"-\"))" 2>/dev/null); want=$(python3 -c "import base64;print(base64.b64encode(bytes.fromhex(\"${o%.parquet}\")).decode())"); [ "$h.parquet" = "$o" ] && m=content-match || m=CONTENT-MISMATCH; [ "$c" = "$want" ] && k=checksum-match || k="CHECKSUM($c)"; echo "${o:0:16}... $m $k"; done
260da652d5f419d7... content-match checksum-match
39d1e4b3e4eeb77f... content-match checksum-match
4b7e53211d022345... content-match checksum-match
560af771e46018e4... content-match checksum-match
8899d2dd62a1a366... content-match checksum-match
9c66c7832f5a1428... content-match checksum-match
9f467d05a81951b7... content-match checksum-match
c1910a92e7399bfa... content-match checksum-match
```


MinIO, the trace and both agents were stopped at the end (the agents with `SIGKILL` after `SIGTERM` hung as in section 6).
