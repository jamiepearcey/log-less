# Template id = content hash

Plane LOG-173 (slice 2 of `docs/design/multi-tenancy-and-indexing.md` section 9), owner decision 2 (accepted).
Branch `wf/tmpl`, based on main `4f31537`. Counts are exact; "not run" is stated where something was not.

## The defect, shown first

`/v1/ingest` deserialises the sender's `template_id` and the merger keeps a present id without mining, so a central's
store held several edges' local counters side by side, and the sync planner keyed rollups by those counters.
Test `crates/logless-core/tests/template_hash_ids.rs` (two edges that meet the same four shapes in opposite order, so
their counters disagree; both send to one central whose merge keeps the ids it is sent), written first against the code
as it was (the edge labelling a record with `Match::template_id`, the miner's counter):

```
shape 0 must have one id at central, has {Some(1), Some(4)}
test result: FAILED. 0 passed; 1 failed
```

With the counter in the helper again after the change, three of the four tests in that file fail (the two-edge test, the
rollup test, the widening test); with the content hash all four pass:

| test | counters (the old boundary) | content hash |
|---|---|---|
| two edges, one central: one id per shape, row counts equal | FAILED | ok |
| rollups from two edges for one shape add up | FAILED | ok |
| widening: id moves, lineage does not, history links them | FAILED | ok |
| central keeps an id it is sent and mines a record with none | ok | ok |

(The last passes either way: that part of the behaviour is unchanged; what changed is what the id means.)

## What the tests show, exact

Two edges, corpus of four shapes, edge A 10/20/30/40 rows per shape, edge B 5/6/7/8 (in opposite meeting order):

* central rows: 126 (10+20+30+40+5+6+7+8 = 126); distinct `template_id`: 4; rows per `template_id`: 15, 26, 37, 48
  (the per-shape sums), each shape with exactly one id.
* Rollups (fidelity 1, `anomaly_boost` off, so nearly everything is left out and counted in per-minute rollups),
  shapes 0..3 with 150, 260, 370, 480 rows ingested over the two edges. Sent in full + left out in rollups, by id
  alone at central: 2 + 148 = 150, 2 + 258 = 260, 6 + 364 = 370, 6 + 474 = 480. There is one rollup group per shape across
  the two edges, not one per shape per edge.
* Widening (`similarity_threshold` 0.5): `job run alpha ...` then `... beta ...` then `... gamma ...`. Ids stored:
  `[h(alpha text), h(<*> text), h(<*> text)]`; the first two differ, the lineage is the first hash for all three. The
  exported `shape_history` has `(lineage, h0, count 1)` and `(lineage, h1, count 2)`; the dictionary rows both carry the
  lineage.
* Sentry fingerprint: it is `Template::lineage_id` (`logless:t:<lineage>`, `main.rs` `process`). For an unwidened
  template the lineage equals the hash of its text, the value it had before this change; pinned in
  `drain::tests::the_boundary_id_is_the_hash_of_the_text_and_does_not_depend_on_arrival_order` (`user <NUM> logged in
  from <IP>` is `16994826430675747824`). For a widened one the lineage stays at the birth hash while the stored id moves
  (`widening_changes_the_id_and_not_the_lineage_...`). Forwarded events for a record that arrived at a central with an
  id use that id as the fingerprint (no text or lineage is known there).

The hash function is the repository's `fingerprint_of` (documented as FNV-1a). Its multiplier is `0x1000_0000_01b3`,
not the textbook FNV prime `0x100000001b3`; that was not changed, because it would move every existing Sentry
fingerprint. The id is whatever that function returns.

## Template churn, re-run (`scripts/template-churn.sh`, 10 Temporal releases, 9 transitions)

The script passes (exit 0 for `clean` and for `CHURN_VALUES=noisy`), with one new assertion in `churn.rs`: for every
line, `Match::shape_id` equals the fingerprint of the template's text after the merge. Baselines reproduce exactly:

| | before (`template-lineage.md`) | after this change |
|---|---|---|
| clean: new / widened / false-novelty ids / events | 517 / 11 / 109 / 71,196 | 517 / 11 / 109 / 71,196 |
| clean: stored ids that changed on widening (events on them) | 11 (97) | 11 (97) |
| clean: of those, identity changed under lineage | 0 (0) | 0 (0) |
| noisy: widened / stored ids changed (events) | 194 / 194 (538) | 194 / 194 (538) |
| noisy: identity changed under lineage | 0 (0) | 0 (0) |

The "fingerprint" column of that analysis is now exactly the id the store carries, so the 11 and the 194 are the
visible widenings in `GROUP BY template_id` until a reader folds by lineage (`shape_history`). The miner itself is
unchanged, which is why nothing else moved. Edited and re-tagged statements still mint new ids (98 of 517, clean); this
slice does not touch that.

## Scripts and test suites

| run | result |
|---|---|
| `cargo test -p logless-core --lib` | 768 passed, 0 failed, 1 ignored (main: 762 passed) |
| `cargo test -p logless-core --test template_hash_ids` | 4 passed |
| `cargo test -p logless-cli` | 6 passed (unit tests of `main.rs`), 0 failed |
| `scripts/e2e-stack.sh` (local backend) | 47 passed, 0 failed (46 before, plus the new check below) |
| `scripts/restore-drill.sh` | 30 passed, 0 failed (29 before, plus a content-hash check) |
| `scripts/parts-replicate-drill.sh` | 33 passed, 0 failed |

Tests that encoded the old id semantics, changed (none deleted to make something pass):

* `template_dict`: `two_catalogs_mint_the_same_id_in_different_spaces` replaced by
  `two_catalogs_that_mined_the_same_shape_in_different_orders_export_the_same_id`;
  `the_id_space_survives_reopening_the_catalog` removed (there is no id space any more); the others take the new
  per-scope paths and the `count`/`lineage_id` columns.
* `merge`: the dictionary test reads `tenant=default/index=default/_meta/templates/writer=<node>.pq` and checks the
  id is the hash of the text and `count`; `origin_is_this_node...` expects schema version `"4"`.
* `s3_serve`: the dictionary test serves the new keys, and `_meta/other.pq` is no longer invisible (any `.pq` under a
  `_meta` segment is served; hidden temp files and `.pq` outside `_meta` still are not).
* `decision` and `admin_api` merge-suggestions: sides are named by content hash, sent as strings.
* `migrate`: the report equality gains the re-key counters; `restore`: the dictionary assertion.
* `scripts/restore-drill.sh`, `scripts/parts-replicate-drill.sh`: the dictionary glob moves to the new path and the
  catalog comparison is on `template_services (template_id, sum(rows))` instead of the miner's private `templates`
  table (a restore numbers those counters afresh).

New check in `scripts/e2e-stack.sh`, "central keeps the edge's content-hash template ids": the per-template row counts of
the edge's store and of central's are the same table (10 template ids on both), and no id is below 1,000,000. It was
not run against the old binary, so that it fails there is argued from the old code (central's `process` re-mined every
peer record under its own counter) and not observed.

**Two sending edges in the e2e: not done.** A second sending edge needs its own node entry and token, a sync wait, and it
would change the "central total == edge total" and "origin" checks that assume one sender. Two independent miners
agreeing on ids, row counts and rollups at one central is `tests/template_hash_ids.rs`; the real binaries are covered for
one edge by the check above.

## Decisions and what they cost

* **Where the hash is applied.** `Match::shape_id` (new) is the content hash; the tree keeps counters
  (`Match::template_id`, `Template::id`, the catalog `templates.id`). The boundary callers use `shape_id`: the ingest
  closure in `main.rs`, the merge fallback, the dictionary, sightings. The cost is one hash-map lookup per line (the
  hash is recomputed only when a template widens). **Not measured:** miner throughput before and after (no release
  build was made; `logless bench` is not run here).
* **`templates` stays on the counter.** The catalog's `templates` table is the Drain tree's persistence; two templates
  that widen into the same text would collide on a hash primary key. What is keyed by hash: `template_services`
  (`tenant, idx, template_id, service`; rows, first/last observed) and the new `template_shapes` (hash to text and
  lineage). The design said "`templates` / `template_services` keyed by hash"; this is the deviation.
* **Central does not mine a record that arrives with an id.** It cannot then name the text of an edge's hash: its
  dictionary has a row only for a hash it mined itself (or restored). Rows and sightings are complete; the planner's
  known-pairs and anomaly keys are by hash and need no text. An edge's own `writer=<edge>.pq` has its text. Closing the
  gap is the later slice that uploads the dictionary with the parts. Forwarded (Sentry/Splunk) events a central raises
  for such a record have no template text and fingerprint on the id itself.
* **Dictionary files.** `tenant=<t>/index=<i>/_meta/templates/writer=<node>.pq`, one row per `(template_id, service)`
  (the console's join grain, so `service` stays a column rather than a `services` list), `count`, `first_seen`,
  `last_seen`, `lineage_id`; `agent` and `global_id` are gone (`template_id` is the global id).
* **Shape history is a sibling file**, `_meta/shape_history/writer=<node>.pq` (`lineage_id, template_id, text, count,
  first_ts, last_ts`). Reason: a different grain; in `_meta/templates/` a reader's `union_by_name` over `*.pq` would
  mix the two. The old single `_meta/templates.pq` is removed (no reader but the console, listed below).
* **S3 serve.** `servable` admits `*.parquet` and any `.pq` with a `_meta` path segment (the design's
  `_meta/**/*.pq`); the dot-prefixed temp files stay hidden.
* **Collision handling (defined, not observed).** Two texts with one 64-bit hash are one shape to every query. The
  catalog keeps the first text registered for the hash, increments meta `template_hash_collisions`, and the merge logs a
  warning; `logless templates` prints the count. A collision test registers two texts under one id
  (`a_hash_collision_keeps_the_first_text_and_is_counted`). Probability near 10^-8 for 10^6 fleet-wide templates
  (design section 6). Two Drain templates that widen into the same text share a hash legitimately (they are one shape);
  the first to hold it names the lineage.
* **Widening across a restart.** The in-memory `Drain::shape` knows every hash a template held since the miner started or
  was restored. An old hash widened away before a restart is registered in the catalog only if a merge saw it; a record
  carrying a hash nothing registered stays unregistered (its rows count, its text is unknown).
* **JSON number precision.** The merge-suggestions endpoint now sends `id` as a decimal string (a 64-bit hash does not
  survive a JSON reader's double). The Splunk forwarder and the `logless.template_id` Sentry tag still print the id as a
  bare number; the tag is a string in Sentry, the Splunk field is a JSON number above 2^53. Left alone; noted.
* **`template_id = 0` is reserved** by the rollup encoding (`sync_policy` treats 0 as "no template"); a text hashing to
  0 would lose its id in a rollup. Probability 2^-64; not handled.

## Migration of an existing store and catalog

* **Catalog (derived state; additive on open).** `template_services` from before (counter-keyed, no tenant or index) is
  dropped and recreated under the new key; `template_shapes` is created and seeded once from the existing `templates`
  rows (hash of current text, lineage); `templates`, the file table and the rest are untouched. The meta key
  `template_id_space` stays in old catalogs, unread. Test:
  `migrate::tests::a_catalog_from_before_hash_ids_loses_its_counter_keyed_sightings_and_keeps_its_templates`.
* **Parquet written with counter ids is rewritten, not left.** `logless migrate-layout --config <file>` (agent stopped)
  now also: maps each counter in a file below `logless.schema_version = 4` (every old-layout file, and any current-layout
  file written before this change; `SCHEMA_VERSION` is now `"4"`) through the catalog's `templates` to the hash of that
  template's *current* text, rewrites the file in place (temp file and rename), refills `template_services` from every
  file, and counts what it did (`files_rekeyed`, `rows_rekeyed`, `rows_unmapped`). A counter the catalog does not
  know loses its id (counted). A widened template's older rows get the current hash, not the one they would have had:
  the earlier text is gone. Version 4 files are not touched, so it can be repeated. Test:
  `migrate::tests::counter_template_ids_in_the_old_layout_and_in_version_3_files_become_content_hashes` (2 + 2 rows
  re-keyed, 1 unmapped, the version 4 file byte-identical afterwards, a second run changes nothing).
  **Not run on a real pre-change store**; only the unit test above.
* **No startup guard.** Unlike the layout change, an agent starts on a store with counter rows in it; the old rows are
  small integers that join nothing. The command above is the fix, or re-ingest.
* **`restore`** derives templates and sightings from the rows it installs: ids are kept as they are in the rows (hashes if
  the parts were written after this change), the lineage is the id, the text an exemplar (unchanged limitation), and
  the dictionary is exported under the new path with the restoring instance's node id. Restore-drill: 7 template ids,
  14 `(template, service)` keys, all identical before and after, and none below 1,000,000.

## Console follow-up (not edited; `/Users/jamiepearcey/projects/logless-studio`)

It reads the old single dictionary and joins on `agent`/`global_id`. Precisely:

1. `crates/sqls-source/src/logless.rs:43` `TEMPLATES_KEY = "_meta/templates.pq"` and `:46` `TEMPLATES_VERSION = "1"`:
   the dictionary is now many files, `tenant=*/index=*/_meta/templates/writer=*.pq`, version `"2"`.
2. `:320-322` `templates_uri`, `:346-348` `templates_probe_sql` (a `glob` of a literal key): glob the new pattern,
   optionally narrowed to the connection's tenant and index.
3. `:350-355` `templates_version_sql` and `:722-743` `install_templates_view`: `parquet_kv_metadata` over a glob returns
   one row per file; take the version from all of them (refuse if any is not 2), not from the first row.
4. `:328` `templates_view_sql` selects `agent, global_id`, which no longer exist. Select `template_id, service, text,
   count, first_seen, last_seen, lineage_id` plus the `tenant` and `index` hive columns, `read_parquet(glob,
   hive_partitioning = true, union_by_name = true)`, and fold the union across writers: group by `(tenant, index,
   template_id, service)`, `sum(count)`, `min(first_seen)`, `max(last_seen)`, text and lineage from any row.
   `:341` `empty_templates_view_sql` gets the same columns.
5. New view over `tenant=*/index=*/_meta/shape_history/writer=*.pq` (`lineage_id, template_id, text, count, first_ts,
   last_ts`), to fold a widened shape: map `template_id` to `lineage_id` and group issues on the lineage.
6. `ui/src/lib/insights/sql/key.ts:102-103` (`DICTIONARY` reads `arg_max(global_id, last_seen)`), `:113` (`d.global_id AS
   gid`), `:13`, `:49`, `:90`; `ui/src/lib/insights/sql/issues.ts:83,102-125,144,150` (merged-issue key
   `d.global_id`); `ui/src/lib/insights/sql/model.ts:52,93`; `crates/sqls-server/src/insights_store.rs:1123-1133` (`g1:`
   issue id built from a global id): the cross-service/cross-agent key is `template_id` itself now, and the key that
   survives widening is `lineage_id`. Merge on `lineage_id` where `global_id` was used.
7. `ui/src/lib/api.ts:738-742` `SuggestedTemplate.id: number` becomes `string` (merge-suggestions sends a decimal string).
8. Tests and fixtures that write the old dictionary: `crates/sqls-source/src/logless.rs:1167-1226`,
   `crates/sqls-server/tests/insights_sql_exec.rs:284-352`, `ui/src/lib/insights/issues/__tests__/model.test.ts:170-182`.
9. Docs: `README.md:214`, `docs/ADR-0010-insights.md:114`, `docs/INSIGHTS.md:1167-1168,1234`.
10. Behaviour to know: on a central, the dictionary only has text for hashes the central mined; an edge's hashes have no
    row there until dictionaries travel with the parts, so titles fall back to the exemplar for those.

## Not covered

* Miner throughput before and after; a real pre-change store through `migrate-layout`; the e2e check against the old
  binary; two sending edges in the e2e (above); `PARTS_BACKEND=s3` for the three scripts (local backend only).
* Cross-node lineage (two nodes reaching one widened text from different births keep different lineages): the open
  research item `platform-thesis.md` section 2 names, unchanged.
* The edited/re-tagged-statement churn (98 of 517 new ids), unchanged.
