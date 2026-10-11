# ADR 0001: Parts tiering, node-owned prefixes and who controls lifecycle

Status: accepted, 2026-10-11 (Plane item 118). Builds on the cold-path parts upload (`docs/peer-sync.md`,
"Uploading Parquet parts") and `docs/evidence/parts-minio.md`.

Since ADR 0003 (`0003-parts-push-to-central-local-backend.md`) central keeps parts on its own disk by default and in
an S3-compatible bucket when configured. Everything decided here holds for both: read "object" as the file
`<dir>/<node>/<name>` or the object `<prefix>/<node>/<name>`, and "bucket" as either store. Where the two differ it is
said below.

## Context

Edges upload finished Parquet files ("parts") to central's object storage through presigned URLs (the only backend
when this was written; see the note above). As built before
this decision, every edge shared one prefix and one bearer token, the catalog row was keyed by the part name alone,
central never changed or removed a part, and nothing said who may expire one. Three questions were open: who owns a
part's key space, who controls its lifecycle (central or the edge), and what retention tiers exist.

## Decision

1. **Central owns the catalog and the lifecycle; an edge owns only writes under its own prefix.**
   * A part's key is `<prefix>/<node>/<name>` (on the local backend the path `<dir>/<node>/<name>`). `<node>` is the id of the `[[peer.nodes]]` entry whose token
     authenticated the request. Central builds the key; no request field chooses it. A request that names another
     node is 403 `foreign_prefix`; a name that is not `<64 hex>.parquet` (so `../`, `/`, `%2F`, `?` and the like) is
     422 `bad_name`; a shared `[peer] tokens` token owns no prefix and gets 403 `no_node` on the parts paths.
     Node ids are `A-Za-z0-9._-`, 1 to 64 characters, not starting with a dot, unique ignoring case, and are validated again before any key is
     built, including from catalog rows. Node tokens must be distinct from each other and from shared tokens.
   * The catalog row is keyed `(node, name)`. The same bytes uploaded by two nodes are two objects and two rows, so
     one node can neither claim, probe nor retire another's part (`already_present` is per prefix; retire answers
     404 for a part the node does not have).
2. **Tiers** record where the bytes are, not how old they are:
   * `hot`: a Parquet file only the node has (not yet uploaded). It has no central row; the node's own store is the
     truth. The listing endpoint refuses `tier=hot` rather than answer an empty list that looks like "none".
   * `warm`: uploaded and committed. The node may still hold its copy.
   * `cold`: the node told central (`POST /v1/parts/retire`) that it dropped its copy, so the object is the only one.
     Only the owning node can say this, and only warm to cold is allowed.
3. **Retention** is central policy in `[peer.parts]`: `warm_retention_secs` and `cold_retention_secs` (0 keeps
   forever). A sweep expires a catalogued part whose newest data (`max_observed`) is older than `now - retention`
   for its tier. The sweep reads only the catalog, so it never lists the bucket and never deletes an object nobody
   committed. It deletes the object (404 counts as gone), and in one transaction removes the row and writes an audit
   row (`peer_parts_audit`: time, action, node, name, tier, size, outcome, detail). A failed delete keeps the row and
   audits `delete_failed`, so the next sweep retries. Tier moves are audited too. Audit rows are append-only and
   outlive the part. The sweep is started by `POST /admin/v1/parts/sweep` and defaults to a dry run
   (`{"dry_run": false}` deletes), at most 1000 parts per tier per call.

## Alternatives rejected

* **Edge controls lifecycle** (the edge deletes its own objects with delete URLs, or sets object tags). Rejected:
  it would need central to hand out DELETE URLs for the same key space it audits, a compromised or merely buggy
  edge could erase central's history, the audit trail would be the edge's word, and one retention policy would have
  to be copied to every edge. Central already holds the credentials and the catalog; the edge keeps no cloud keys.
* **Bucket lifecycle rules only** (S3 expiration by age). Rejected as the sole mechanism: it cannot tell warm from
  cold, would leave the catalog pointing at objects that no longer exist, and cannot audit. It is still a good
  backstop for orphans (objects with no catalog row), which this sweep deliberately never touches. The local backend
  has no such backstop: an uploaded part nobody committed stays on central's disk until someone removes it (ADR 0003,
  "Not built").
* **One shared token, node id taken from the request body.** Rejected: ownership would be a claim, not a fact. The
  token is the only identity central can check, so the node comes from it and the body is only compared.
* **Node id derived from the existing `origin` field.** Rejected: `origin` is the edge's self-reported instance id
  and is kept as information only.
* **Tiers by age** (hot under N hours, warm under N days). Rejected: age says nothing about whether a local copy
  exists, which is what decides whether deleting the object loses data.
* **Migrating old flat-prefix rows.** There are no deployments with them, and their keys do not exist under the
  new layout. The catalog renames an old `peer_parts` table to `peer_parts_flat_legacy` and starts afresh.

## Consequences

* Existing setups must list their edges as `[[peer.nodes]]` and give each edge its own token as its destination
  token. An edge's `[[destinations]] token` is that node token; it also works for record ingest.
* Expiring a warm part does not touch the node's copy, and the node's upload state file would not upload it again.
  Warm retention is therefore a statement that central no longer needs the object while a copy still exists on the
  node; set it to 0 until nodes report cold.
* Not built in this slice: the edge does not yet call `retire` by itself when its own retention removes a file
  (the endpoint and catalog exist and are tested), and nothing runs the sweep on a timer; an operator or cron calls
  the admin endpoint. Both are deliberate follow-ups, not gaps in the decision.
