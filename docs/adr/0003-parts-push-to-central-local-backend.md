# ADR 0003: The edge pushes parts to central; central keeps them on its own disk by default

Status: accepted, 2026-10-11 (Plane item LOG-167). Builds on ADR 0001 (node-owned prefixes, tiers, lifecycle), which
stands unchanged, and replaces the assumption in `docs/peer-sync.md` and `docs/restore.md` that parts always live in an
S3-compatible bucket. Measured runs: `docs/evidence/parts-local-backend.md`.

## Context

Until now the cold path needed an S3-compatible bucket: central held its credentials and presigned a PUT per part, and
the edge sent the file straight to the bucket. That made a second system (MinIO, Garage or a cloud bucket) a
precondition for uploading a single part, for every self-hoster, including the one-box install where central has a disk
and nothing else. The questions were who moves the bytes (push or pull), and where they land.

## Decision

1. **The edge pushes.** It asks central where to put a part and gets a URL back. There is no pull mode: central never
   connects to an edge.
2. **One upload protocol, two storage backends**, chosen on central by `[peer.parts] backend`:
   * `"local"`, the default when no S3 endpoint is configured: the URL is central itself (a path on its peer listener)
     and a short-lived upload token. Central writes the part under `<dir>/<node>/<name>`, by default
     `<data_dir>/parts`.
   * `"s3"`: a presigned URL for an S3-compatible bucket, exactly as before. An `[peer.parts]` table that names an
     `endpoint` and no `backend` is S3, so existing configurations are unchanged.
3. **The edge has no mode.** It sends `presign`, PUTs to whatever URL comes back with whatever headers come back,
   and sends `commit`. It does not know which backend central runs, and one edge binary works against both.
4. **Parts stay immutable and named by SHA-256**, under node-owned prefixes, with central owning the catalog and the
   lifecycle (ADR 0001). The catalog, tiers, retention sweep and audit are the same code for both backends; only "is
   it stored", "where does it go" and "delete it" differ (`parts::Store`).
5. **No S3 write API and no distributed object store in log-less.** The S3 endpoint stays read-only; the local parts
   directory is served through it as a second read-only bucket.

### The wire protocol

All on central's peer listener. `presign`, `commit` and `retire` are unchanged in shape and take the node's bearer
token (ADR 0001).

| Step | Request | Answer |
|---|---|---|
| 1 | `POST /v1/parts/presign` `{name, size, sha256}` | 200 `{already_present, url, headers, expires_at_ms}`. Local: `url` is the path `/v1/parts/blob/<node>/<name>` (the edge resolves it against the peer URL it already uses) and `headers` is `x-logless-upload-token` and `expect: 100-continue`. S3: `url` is the absolute presigned URL and `headers` is `x-amz-checksum-sha256`. 507 `insufficient_storage` (local) when the part would pass the disk budget; the other refusals are as before (422 `bad_name`/`bad_hash`/`bad_size`, 403 `foreign_prefix`/`no_node`) |
| 2 | `PUT <url>` with `headers` and `Content-Length`, body the part | local: 200 `{"stored": true, "already_present": false}`, or `{"stored": false, "already_present": true}` for a part that was already there. S3: the bucket's answer |
| 3 | `POST /v1/parts/commit` `{name, size, sha256, rows, min_unix_nano, max_unix_nano, origin}` | 200 `{recorded}`; 409 `not_uploaded` when the part is not stored |

The local PUT is authorised by the upload token alone; it needs no bearer token, as a presigned S3 URL needs none.

**Token.** `v1.<expires, unix seconds>.<size>.<hex HMAC-SHA256>`. The MAC is over
`logless-part-upload/1\n<node>\n<name>\n<size>\n<sha256>\n<expires>` with a 32-byte key central draws from the OS at
start and keeps in memory only. A token is therefore good for one node, one name (and so one content), one size, until
it expires (`url_ttl_secs`, default 900 s). It is not single-use: a replay can only send the same bytes to the same
place, which changes nothing. A restart voids outstanding tokens; the edge is answered 403 and presigns again, which it
already did for an expired S3 URL. The token travels in a header, not the query string, so it is not in a proxy's
access log.

**What the local PUT refuses, in order, all before the body is read:** a path that is not `<node>/<name>` (404), a node
that is not a node id (403 `bad_node`), a name that is not `<64 hex>.parquet` (422 `bad_name`), a missing, forged,
replayed or duplicated token (403 `bad_token`) or an expired one (403 `token_expired`), a `Content-Length` over
`max_part_bytes` (413 `too_large`), a `Content-Length` other than the token's size (400 `size_mismatch`), a part that
is already arriving on another connection (409 `upload_in_progress`, `Retry-After: 1`, no room reserved), and a part
that would pass the disk budget (507 `insufficient_storage`). A client that sent `Expect: 100-continue` (the edge
does, because central asks for it) is told to continue only after these, so a refusal costs it no upload.

**Then** the body streams through the bounded listener to `<dir>/<node>/.upload-<id>.tmp` in 256 KiB pieces, hashed on
the way and never held whole in memory. A body that ends early is 400 `incomplete_body`, one that stalls is 408
`upload_stalled`, one whose SHA-256 is not the name is 422 `bad_content`; in each case the temporary file is removed and
nothing is catalogued. Otherwise the file is `fsync`ed, renamed to `<dir>/<node>/<name>`, the node directory and the
parts directory are `fsync`ed, and only then is the answer 200. A part already stored is read and checked against the
name but written nowhere.

**Timeouts.** The head must arrive within the listener's request timeout (30 s). A streamed body is not held to that
as a whole, since 512 MiB over a slow link does not fit: each read must make progress within the same 30 s, and the
whole body within 30 s plus its length at `min_upload_bytes_per_sec` (default 16 KiB/s; `bounded_http::BodyReader`).

**Crash ordering.** Only the upload path writes under a final name, and only by rename of a file it hashed and synced.
So after a crash a part is either absent (its bytes, if any, are in a `.upload-*.tmp` file that the next start removes)
or present and whole. A crash between the rename and the edge's commit leaves a part that is present and not
catalogued: the edge's retry is told `already_present` and its commit records it. "Stored intact" on the local backend
is therefore a regular file of that name and size; central does not hash a part again on every presign and commit.

**Disk budget.** `disk_budget_bytes` (default 64 GiB, 0 for none) caps installed parts plus uploads in flight. A presign
or an upload that would pass it is refused with 507 and counted (`parts_store.refused_budget` in `GET /admin/v1/sync`,
with `used_bytes`, `reserved_bytes` and `budget_bytes`). The count is kept in memory from a walk at start; before
refusing, central re-measures the directory (at most every 10 s), so files removed by hand are noticed. A disk that is
simply full (`ENOSPC`) is the same 507.

### Configuration

```toml
[peer.parts]
enabled = true
# backend = "local"              # the default when endpoint is empty; "s3" needs endpoint, bucket and keys
# dir = "/var/lib/logless/parts" # default <data_dir>/parts
disk_budget_bytes = 68719476736  # 64 GiB, the default; 0: no budget. Not under max_part_bytes
max_part_bytes = 536870912       # 512 MiB, the default
url_ttl_secs = 900               # how long an upload URL (S3) or upload token (local) works
min_upload_bytes_per_sec = 16384 # local: slowest average upload rate; slower is 408 and nothing is kept
```

A local table that cannot work (the directory cannot be created, the budget is under `max_part_bytes`, a TTL out of
range, or an unnamed backend with a bucket, prefix or keys set but no endpoint, which is an S3 table that lost its
endpoint) stops the peer listener at start with the reason. An incomplete S3 table is still logged and left off, as
before, because that is what a table half filled in looks like.

## What durability each configuration gives

| Configuration | A committed part survives | Does not survive |
|---|---|---|
| `local` | a crash or power loss of central (file and directory are synced before the 200); loss of the edge. While the part is `warm` there are two copies on two machines | loss of central's disk once the part is `cold` (the edge dropped its copy): one disk, one copy. Nothing in log-less replicates the directory. Put it on redundant storage or back it up if cold parts matter |
| `s3`, managed bucket | whatever the provider gives, typically several zones; loss of central (the bucket is elsewhere) | a deleted bucket or key; the provider's failure modes |
| `s3`, one MinIO or Garage node you run | the same as `local`, with a second system to run | loss of that node's disk |

In every configuration: records that are not yet in an uploaded part exist only on the edge (the RPO in
`docs/restore.md`), and the catalog that says which parts exist (`peer_parts`) is on central's disk. Without it a
restore still verifies every part it finds, but cannot say that one is missing. Back the catalog up with the parts.

Bit rot on central's disk is not detected by central on the local backend, as it is not on S3 (ADR 0001 and
`docs/peer-sync.md`, "Damage to a stored part"): the name is the checksum, `logless restore` verifies it, and nothing
scrubs on a timer.

## Alternatives rejected

* **Central pulls from the edge's S3 endpoint.** The edge already serves its store read-only over S3. Rejected: it
  fails for an edge that is outbound-only (behind NAT or a firewall, which is the normal edge), and it would be a
  second mode with its own scheduling, authentication in the other direction and failure handling. One direction, the
  one that always works, is enough.
* **An S3 write API in log-less, or a multi-node S3 server.** Rejected: multipart uploads, listing consistency,
  replication and repair are a distributed storage system, and that is not the product. Garage and MinIO exist; the
  `s3` backend is how to use them.
* **External bucket only (the state before this ADR).** Rejected as the default: it forces every self-hoster to run a
  second system before the first part can be uploaded. It stays as an option because a managed bucket is the better
  place for parts when there is one.
* **A separate mode on the edge for local uploads** (a different endpoint, a different job). Rejected: the edge would
  have to be configured to match central, and a mismatch would be a new way to fail. The URL in the presign answer
  carries the difference.
* **An absolute URL for the local upload.** Rejected: central does not know its own public name behind a proxy or a
  TLS terminator, and a wrong `public_url` setting would be another thing to get wrong. A path, resolved by the edge
  against the URL it already reached central on, is always right.
* **The upload token in the query string**, as a presigned S3 URL does. Rejected: query strings are logged by proxies.
* **Persisting the token key.** Rejected: it would be one more secret on disk, and the only thing it buys is that a
  token survives a restart, which the edge's existing retry already covers.
* **Hashing the stored file again on every presign and commit.** Rejected: up to 512 MiB of reads per request, to
  detect only what the rename discipline already rules out. Damage after the fact is a scrub's job, not the request
  path's.

## Consequences

* A central with `[peer.parts] enabled = true` and nothing else now accepts parts on its own disk. Before, that table
  was incomplete and uploads were off.
* Central carries the bytes on the local backend: its network, its disk writes and one SHA-256 per part. Memory does
  not grow with the part (measured in the evidence file).
* An upload holds a connection of the peer listener for as long as it takes, bounded per upload as above and in number
  by the listener's connection cap (`workers` x 32, at least 64), not per node.
* `logless restore` reads a node's directory under central's parts directory directly (`--from <dir>/<node>`), with
  central's catalog as the inventory (`--inventory`, and `--node` when central has more than one). It runs where the
  directory is readable.
* The parts directory is the read-only bucket `[serve_s3] parts_bucket` (default `parts`) on central's S3 endpoint,
  beside the store's bucket and under the same credentials, so the console and DuckDB read it as they read the store.
  It is a bucket of its own so that a `**/*.parquet` read of the store never counts an uploaded part's rows as well.

## Not built in this slice

* A restore over the network from a local-backend central. `logless restore` reads S3 with presigned GETs, and
  log-less's own S3 endpoint accepts only header-signed requests, so `--from s3://parts/<node>` against central does
  not work yet. Use the directory, or copy it.
* A scrub that re-hashes stored parts on a schedule, and any replication of the parts directory.
* Cleaning up parts that were uploaded and never committed. The sweep reads only the catalog (ADR 0001), a bucket
  lifecycle rule is the backstop on S3, and the local backend has none: such files count against the budget and show
  as `used_bytes` above the catalog's total.
* A per-node cap on concurrent uploads or on bytes.
