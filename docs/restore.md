# Replication and verified restore

Plane item 119. This page records the decisions and describes `logless restore`. The measured drill is in
`docs/evidence/restore-drill.md`; the script is `scripts/restore-drill.sh`.

## Decisions

| Question | Decision |
|---|---|
| Durability target | Central's part store is the replica of an edge's committed data, reached by the parts path (`docs/peer-sync.md`, `crate::parts`). It is central's own disk or an S3-compatible bucket, whichever `[peer.parts]` configures (ADR 0003, which also says what each survives). An edge's data directory is rebuildable from it. "Bucket" below means either. A second central can keep a copy of a local-backend central's parts (`[peer.parts.replicate]`, `docs/peer-sync.md`, "Replicating parts between centrals"): losing the first central's disk then loses only what was uploaded since the replica's last run. |
| RPO | Everything in a part that was uploaded, plus whatever is still in the edge's own WAL on a surviving disk. Records accepted after the last upload and lost with the disk are not recoverable from the bucket. The bound is "what the edge accepted since its last upload", which is the operator's upload cadence; there is no automatic cadence yet. |
| RTO | Measured, not promised. See the evidence file for the numbers and what they do and do not cover. |
| Integrity | A part is named by the SHA-256 of its bytes. The restore recomputes it, so a flipped byte is detected without any inventory. |
| Completeness | Needs an inventory: central's `peer_parts` table (`--inventory <central catalog.sqlite>`, with `--node <id>` when central catalogues more than one node). A bucket or a directory cannot say what it used to hold, so a part deleted from the bucket is only detectable against the inventory. Without `--inventory` the restore says `inventory=none` and cannot report missing parts. |
| Failure policy | A part that fails any check is not installed, is named in the output with the reason, and the exit status is 1. The other parts are still restored, so a partial restore is usable, and the exit status says it is partial. |

### Alternatives rejected

* **Replicate the WAL to a second disk or node (synchronous).** Gives RPO near zero but puts a remote write on the
  ingest path. The agent must never block the application (`docs/architecture.md`), so this is rejected for the edge.
  The central sync (`docs/peer-sync.md`) already carries records, record by record, to another instance; that is a
  separate replica with its own store and is not what this restore reads.
* **Back up the SQLite catalog to the bucket.** The catalog is derived state: file rows come from Parquet footers
  (`Catalog::rebuild`), typed columns from footers, and `template_services` from the rows. Backing it up adds a second
  source of truth that can disagree with the parts. Rebuilding it is cheaper and is what the restore does.
* **Trust the bucket listing as the inventory.** A listing cannot show a part that is gone. Rejected for completeness
  checks, kept for integrity.
* **Verify with the S3 object checksum only.** Storage-side checksums protect the upload (`parts` signs
  `x-amz-checksum-sha256`) but say nothing about the object a year later. The restore hashes the bytes it actually
  received.
* **Ship a manifest object next to the parts.** Would let a restore find missing parts without central, at the cost of
  a mutable object in an otherwise immutable, content-addressed layout. Not done; the central inventory already exists.
  Revisit if edges are ever restored with no central.

## `logless restore`

```text
logless restore --from <central data dir>/parts/<node> --to <data dir> \
                [--inventory <central catalog.sqlite>] [--node <node>] [--config <edge config>] [--verify] [--force] [--report-json <file>]
logless restore --from s3://<bucket>/<prefix>/<node> --endpoint http://host:port --to <data dir> ...
```

The first form is the local backend: a node's directory under central's parts directory (any directory of
`<sha256>.parquet` files works, so a copy of it does too). It is read in place and never written; files that are not
parts, such as an upload in flight, are counted as `ignored_keys`. The second form is a bucket. `--node` takes only
that node's rows from the inventory: the catalog is keyed `(node, name)`, and without it every other node's parts
would be reported missing.

A replica central (`docs/peer-sync.md`) keeps the same layout, parts under their original node and the same catalog rows, so
a restore from it is the same command with its paths: `--from <replica data dir>/parts/<node> --inventory <replica
catalog.sqlite> --node <node>`. It has the parts that replica has copied, and the inventory is that replica's, so a part the
replica has not yet fetched is neither restored nor reported missing; run the replication (or compare the two catalogs) first.
Measured with the first central and the edge destroyed: `docs/evidence/parts-replication.md`.

Not built: restoring over the network from a local-backend central. Central serves its parts directory read-only over
S3 (`docs/peer-sync.md`), but `logless restore` reads S3 with presigned GETs and that endpoint accepts only
header-signed requests. Run the restore where the directory is readable, or copy the node's directory first.

Credentials for `s3://` come from `LOGLESS_S3_ACCESS_KEY_ID` and `LOGLESS_S3_SECRET_ACCESS_KEY` (never flags, so they
do not reach `ps`). Reads use presigned GETs; listing is `ListObjectsV2` under the prefix, with pagination. Keys that are
not `<64 hex>.parquet` are counted as `ignored_keys` and never fetched.

For each part, in order: fetch to `<to>/.restore-tmp/`, SHA-256 equals the name, size equals the inventory's, the Parquet
footer is readable, the row count equals the inventory's, every row group decodes and the decoded row count equals the
footer's. Only then is the part moved to `<to>/store/tenant=…/index=…/hour=…/level=…/node=…/<name>.parquet`:

* the hour is the **event hour** of the part's first row (its `timestamp`, not `observed`). A file the store writes holds
  one event hour and one level, so the first row speaks for all of them; a part that does not is counted in
  `multi_partition_parts` and still placed by its first row;
* the level is the bucket of its severity under its index's buckets (`--config`, the edge's config; otherwise the
  default buckets);
* tenant, index and node are what the part's Parquet footer says it was written under (`logless.tenant`, `logless.index`,
  `logless.node`, written since layout v3). A part without them (written by an older build) goes under the first tenant of
  `--config` (or `default`), that tenant's default index, and `node=<--node>` (or `node=restored`).

Parts are still `<node>/<sha256>.parquet` on central (their move into the tree is a later slice), so the footer is the only
thing that says which index a part belongs to. Then:

* `<to>/catalog.sqlite` is rebuilt from the installed files (`Catalog::rebuild`),
* template ids are kept: `templates` (id, count, first/last seen) and `template_services` ((id, service), first/last
  observed) are derived from the rows, and `<to>/store/_meta/templates.pq` is exported from them,
* with `--verify`, every installed file is hashed again and the rebuilt catalog's file, row and byte totals are compared
  with what was installed.

It refuses a `--to` that already holds a store or catalog unless `--force`.

Output is `key=value` lines (`listed`, `restored_parts`, `rows`, `bytes`, `hash_mismatch`, `size_mismatch`,
`rows_mismatch`, `unreadable`, `fetch_failed`, `missing`, `not_in_inventory`, `catalog_*`, `templates`, `elapsed_ms`),
then one line per refused part with its name and reason, then `RESTORE OK` or `RESTORE FAILED`. Exit status 0 only when
nothing was damaged, unreadable, unfetchable or missing.

### What a restore does not bring back

* **Records that were never uploaded.** See RPO.
* **Template text.** Rows in a part carry `template_id` only; the text lives in the catalog, which is not in the bucket.
  The restore keeps every id, count, service pairing and first/last seen exactly, but the text of each template is an
  exemplar: the masked body of the first restored row for that id, with no mined `<*>` wildcards. Ids and joins are
  right; titles are less general than before. Fix, if it matters: upload `_meta/templates.pq` with the parts.
* **`template_id_space`.** The rebuilt catalog mints a new one (a UUID per catalog). Ids are preserved, but a consumer
  that keys on `(agent, template_id)` sees a new agent.
* **Counters, the WAL, spool, sync state, policy and admin state.** None of these are in the bucket. A restored edge
  starts as a new edge on an existing store.
* **A part that is in the bucket and not in the inventory** is installed (it is intact) and reported as
  `not_in_inventory`; it does not fail the restore.
