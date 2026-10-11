# Parts tiering: node-owned prefixes, listing, retention sweep

Run on 2026-10-11 (BST) from branch `wf/tier`, `scripts/e2e-stack.sh` against a real MinIO
(`/opt/homebrew/bin/minio`), a central and an edge `logless` built from the branch. Decision record:
`docs/adr/0001-parts-tiering-and-lifecycle.md`.

## Unit tests (fake S3 server in `parts.rs`, no network beyond loopback)

`cargo test -p logless-core parts::` 18 passed, `peer::` 37 passed, `catalog::` 8 passed, `admin_api::` 57 passed,
`config::` 7 passed. New tests:

* `a_node_can_only_presign_commit_and_retire_under_its_own_prefix`: node A's presigned URL is under `edge-a/`, B's under
  `edge-b/`; a body naming the other node is 403 `foreign_prefix` on presign, commit and retire; eight traversal or
  prefix names (`../edge-b/<n>`, `edge-b/<n>`, `/<n>`, `..%2F...`, `<n>/../../x`, NUL, `?x=1`, `..`) are 422 on all three
  paths; a shared token gets 403 `no_node`; A's object at its key is not "present" for B and B cannot commit it (409);
  every key in the fake bucket starts with A's prefix and no PUT was made.
* `node_configuration_is_validated_before_anything_listens`: bad ids, empty, duplicate and shared tokens refuse to start.
* `parts_are_listed_by_node_time_and_tier_and_a_node_moves_its_own_part_to_cold`.
* `the_sweep_deletes_only_catalogued_parts_older_than_their_tiers_retention_and_audits_each`: two retention windows,
  young parts kept, a dry run changes nothing, an injected DELETE failure keeps the row and audits `delete_failed`, the
  next sweep retries, an uncatalogued old object under a node's prefix and one outside the prefix are never touched, a
  tampered catalog row (`node = '../x'`) is refused without any DELETE.
* `an_old_flat_catalog_is_set_aside_not_misread`, and in `admin_api`: the listing filters and their 400s.

## End to end against MinIO

```text
$ LOGLESS_BIN=<copy of the built binary> scripts/e2e-stack.sh
parts job done                               PASS  state=done files_total=15 uploaded=15 rejected=0 error=None (edge files=15)
minio parts match their sha256               PASS  objects=15 uploaded=15 mismatched=0
central catalog lists the parts              PASS  peer_parts rows=15
foreign prefix and traversal refused; nothing outside own prefix PASS  foreign presign=403 traversal presign=422 foreign commit=403 objects outside edges/edge-1/=0
catalog listing by node and tier             PASS  warm parts of edge-1=15 (uploaded=15) parts of edge-2=0
node retires its own part to cold            PASS  retire=200 repeat=404 cold parts=1
retention sweep deletes expired catalogued parts and audits them PASS  dry-run would_delete=15 objects after dry run=15 sweep deleted/failed='15 0' objects after=0 catalog rows=0 expire audit rows=15
elapsed 16 s; 28 passed, 0 failed, 0 skipped
```

The other 21 rows of the stack (counts, accounting invariant, shutdown) also passed. One run; not repeated.

## Not measured or not built

* One edge node only in the MinIO run; the two-node cases are fake-S3 tests.
* The edge does not call `retire` itself, and nothing runs the sweep on a timer (ADR, Consequences).
* Sweep throughput on a large catalog was not measured (batches are capped at 1000 parts per tier per call).
* Another session writes to the shared `~/tmp/logless-admin-target`; a binary there was once replaced by a build of a
  different checkout mid-session (its `init` output had no `nodes` key). The run above used a copy of a binary built
  from this tree, checked with `logless init | grep warm_retention`.
