# Parts replication between centrals: evidence

Run on 2026-10-11 (BST) on branch `wf/parts-repl` (LOG-168), on top of `929fb48` (the local parts backend, `wf/parts-local`,
which the branch fast-forwarded to before any change). Decision and protocol: `docs/adr/0003-parts-push-to-central-local-backend.md`
("Replica") and `docs/peer-sync.md` ("Replicating parts between centrals"). Machine: Apple M4 Max, macOS 26.1, everything on
loopback and one APFS volume; other sessions were compiling on the same machine during every run.

Binary: `cargo build -p logless-cli`, a **debug build**, copied to a scratch directory before the runs and passed as
`LOGLESS_BIN` (the shared target directory is rebuilt by other sessions: the very first run of the script failed on `unknown field
replication_tokens`, from a binary that lacked the new code, presumably another session's build; every number below is from
a binary rebuilt and copied straight afterwards and checked to contain the new code). No release build was made. `$SCRATCH` is the scratch directory the
scripts were pointed at with `TMPDIR`. The script starts no MinIO or other second system.

## What the drill does (`scripts/parts-replicate-drill.sh`)

1. Central **A** (the source: peer listener, node `edge1`, `[peer.parts]` on its own disk, `replication_tokens`) and an edge
   start on random free ports with random credentials. The edge has twelve level buckets and its records cycle twelve
   severities, so its store holds many small parts rather than three.
2. 6000 OTLP records go to the edge; it syncs, is stopped, `logless merge` flushes its WAL, `logless verify` passes, it
   restarts and uploads every Parquet file to A. The BEFORE snapshot is the edge's own store (rows, rows per template, per
   `(template, service)`, the `templates` table, the dictionary keys). A's catalog and the SHA-256 of every file it holds are
   recorded.
3. Credentials are checked on A's replica paths with `curl`.
4. Central **B** (the replica: `[peer.parts.replicate]`, `interval_secs = 0`, `page_size = 7`, `bytes_per_sec` set so the whole
   copy would take about 10 s) starts empty. `POST /admin/v1/parts/replicate` starts run 1, and the script `kill -9`s B as
   soon as a partial copy of at most half a part exists and at least three parts are catalogued.
5. B restarts (same data directory); run 2 is started on request. Then run 3, to show nothing is left to do.
6. B's catalog is compared with A's line by line (node, name, size, rows, time range, origin, tier) and every file on both
   disks is hashed and compared.
7. A is stopped and its data directory deleted, the edge is stopped and its data directory deleted. A run on B says the source
   cannot be reached and changes nothing. `logless restore --from <B>/parts/edge1 --inventory <B's catalog> --node edge1
   --verify` rebuilds the edge's store, and the BEFORE queries are run on it.

The replica's rate limit is there to make a kill land midway; the timings below are not a throughput measurement.

## Counts

| Script | Run 1 | Run 2 |
|---|---|---|
| `scripts/parts-replicate-drill.sh` | 33 passed, 0 failed (26 s) | 33 passed, 0 failed (25 s) |

Run 1: 92 parts, 416,113 bytes. At the kill B had 16 parts installed and catalogued and one partial copy of 2,600 bytes of a
part; A had started 17 part responses. Run 2 listed 92, found 16 present, adopted 0 and fetched 76, one of them resumed with
2,600 bytes not fetched again; A had started 93 part responses over both runs (92 + 1 resumed part). Run 3 found 92 present,
fetched 0, and A's count stayed at 93. B's catalog equals A's (92 lines), B's 92 files equal A's by SHA-256 (416,113 bytes on
each), B's `used_bytes` is 416,113, 92 `replicate` audit rows, no partial copy left. A restore from B alone, with A and the
edge deleted, restored 92 parts, 6000 rows, 416,113 bytes: the edge's counts exactly, per template and per (template,
service), the catalog's `templates` and the dictionary keys.

Run 2 (the same script, another edge run): 92 parts, 418,410 bytes; at the kill B had 3 parts installed and a partial copy of
2,615 bytes; run 2 fetched 89 (one resumed, 2,615 bytes not fetched again); A had started 93 part responses; restore from B
restored 92 parts, 6000 rows, 418,410 bytes.

The part count and the kill point differ from run to run (how the edge's WAL segments happened to merge, and when the kill
landed); every check reads the figures it needs from the disks at the kill, so they agree each time. Earlier runs while the
script was written (not counted above) landed the kill after the cut part was already whole (the replica paces itself after
each chunk, so a finished-but-not-yet-renamed copy is a real state); the script now accounts for that case too (a whole
partial needs no request, so A's count is one lower), and it is also what the check "A served each part once" allows for.

Rust tests, `cargo test -p logless-core -- parts:: bounded_http:: restore:: s3_serve:: peer:: admin_api:: config:: httpd::`:
213 passed, 0 failed; with `catalog::` added (the catalog gained four methods) 223 passed, 0 failed. Of these, 11 are new in
`parts::replicate::tests` and 2 in `admin_api::tests`. The local-backend evidence reported 197 for the first filter at its
`2f53a55`; its branch gained tests in `929fb48` after that, and the count at `929fb48` was not re-measured here. The whole
workspace suite was not run.

The two existing scripts, run once each on the same binary to see the new code left them alone: `scripts/e2e-stack.sh`
(local) 32 passed, 0 failed, 0 skipped (20 s); `scripts/restore-drill.sh` (local) 27 passed, 0 failed (6 s).

## The new Rust tests and what each shows

| Test | Exact counts it asserts |
|---|---|
| `a_replica_copies_every_part_by_catalog_diff_and_a_second_run_fetches_nothing` | 7 parts over 3 nodes, pages of 3: listed 7, fetched 7, fetched_bytes = the sum of the sizes; catalogs and files equal; 7 audit rows; source served 7 over 3 pages; run 2: present 7, fetched 0, source served still 7 over 6 pages |
| `a_replica_that_already_holds_some_parts_fetches_only_the_rest_and_never_deletes` | 6 on the source; replica had 2 catalogued, 1 on disk uncatalogued, 1 the source lacks: present 2, adopted 1, fetched 3, only_here 1 (named, left on disk and in the catalog); source served exactly 3 |
| `an_interrupted_run_keeps_what_it_finished_and_resumes_the_cut_part_from_the_byte_it_reached` | a proxy cuts the 2nd blob response after 40,000 bytes: run 1 fetched 1, kept a 40,000-byte `.fetch` file; run 2: present 1, fetched 2, resumed 1, resumed_bytes 40,000, fetched_bytes = total - first part - 40,000; the proxy saw the requests `[A, B, B with Range bytes=40000-, C]` and the resumed response carried exactly size(B) - 40,000 bytes |
| `a_damaged_source_part_is_refused_reported_and_never_catalogued_and_is_fetched_once_repaired` | one byte flipped at the source: listed 3, fetched 2, damaged 1, run complete; not catalogued, not installed, no partial left; repaired, the next run fetches it |
| `a_source_that_lists_a_part_it_cannot_serve_is_reported_and_the_rest_are_copied` | one file deleted and one cut short at the source: listed 4, fetched 2, unservable 2, both named |
| `the_replica_paths_open_only_with_a_replication_token_and_nothing_else_opens_with_it` | none 401, wrong 403, node token 403 on both paths, replication token 403 on presign and the capability document; two replicas with a wrong and a node token fail with "refused the replication token (HTTP 403)" and copy nothing; `refused_replica` 6; ranges (206 and `Content-Range`, 416, an unreadable range ignored), an uncommitted file, traversal and a wrong-node name all 404 |
| `a_central_with_no_replication_tokens_offers_nothing_and_a_replica_says_so`, `a_source_that_keeps_its_parts_in_a_bucket_is_reported_and_nothing_is_copied` | 404 even for a node token; an S3-backend source answers 409 `not_local` and the replica reports "keeps its parts in a bucket", 0 listed |
| `replication_is_configured_only_where_it_can_work`, `an_entry_that_could_not_be_a_file_in_a_node_directory_is_never_believed` | config rules (S3 replica, bad URL, empty/duplicate tokens, page size; a replication token equal to a node token stops the listener); eight kinds of invalid listing entry |
| `the_timer_runs_by_itself_one_run_at_a_time_and_shutdown_stops_it` | interval 1 s, pages of 2: run 1 copies 3 parts, a later run finds 3 present; a manual run during one is refused; receiver shutdown joins the timer in under 5 s |
| `admin_api::tests::a_replication_run_is_started_and_read_through_the_admin_api`, `...::a_replica_starts_a_run_on_request_and_reports_where_it_got_to` | 409 `parts_disabled` / `not_a_replica`; 202 on start; the run's error reaches the status; the token never appears |

Two of the tests were checked to be able to fail: with the hash comparison disabled the damaged-part test fails, and with
the `Range` header disabled the interruption test fails (both mutated by hand, then restored).

## Full output: replication drill, run 1

```text
[11:47:03] run dir $SCRATCH, logless logless 0.1.0, no MinIO
[11:47:04] central A: peer 56985 admin 56990; edge: otlp 56993 admin 56994
[11:47:13] central B (replica of A, 41611 B/s): peer 57344 admin 57345

restore output:
  listed=92 ignored_keys=0 inventory=92
  restored_parts=92 rows=6000 bytes=416113
  hash_mismatch=0 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=0 not_in_inventory=0
  catalog_files=92 catalog_rows=6000 catalog_bytes=416113
  templates=6 template_services=12 dictionary_rows=12 multi_partition_parts=0
  reverify_bad=0
  elapsed_ms=170
  RESTORE OK: 92 parts, 6000 rows verified

CHECK                                                                  RESULT DETAIL
-----                                                                  ------ ------
ingest: all batches answered 200                                       PASS  6000 records over OTLP/HTTP JSON
edge synced what it accepted                                           PASS  received=6000 written=6000 sent_records=6000 backlog_bytes=0
edge stops cleanly before the flush                                    PASS  2074 ms
edge verify after merge (accounting invariant)                         PASS  received=6000 committed=6000 in_wal=0 dropped=0 unaccounted=0
edge uploaded its parts to A                                           PASS  state=done files_total=92 uploaded=92 rejected=0
before: every accepted record is in a part                             PASS  edge parquet rows=6000 accepted=6000 files=92 bytes=416113
A holds exactly the edge's files (by sha256)                           PASS  A catalog rows=92 bytes=416113; edge files=92 bytes=416113
enough parts to interrupt a copy midway                                PASS  92 parts, kill after 3 recorded
replica paths: only a replication token opens them                     PASS  no token=401 wrong=403 node token on catalog=403 on blob=403; replication token: presign=403 capability=403 catalog=200; A refused_replica=4
B starts empty                                                         PASS  peer_parts rows=0, parts dir: 0 files
run 1 started on request and was killed midway (SIGKILL)               PASS  POST=202; at the kill B had 16 of 92 parts installed (16 catalogued) and 1 partial copy of 2600 bytes (+0 empty); A had started 17 part responses
run 2 completed without error                                          PASS  POST=202 complete=True error=None
run 2 did not fetch a finished part again                              PASS  listed=92 present=16 adopted=0 fetched=76; expected listed=92 present=16 (catalogued at the kill) adopted=0 (file installed, row not yet) fetched=76
run 2 resumed the cut part from the byte it had                        PASS  resumed=1 resumed_bytes=2600; the kill left 1 partial copy of 2600 bytes
A served each part once, the cut one twice                             PASS  A started 93 part responses over both runs; expected 92 parts + 1 resumed - 0 already whole (+ up to 0 for an empty temporary file at the kill)
run 2: nothing damaged, unservable, invalid or only here               PASS  damaged=0 unservable=0 invalid=0 only_here=0 tier_differs=0
B's catalog equals A's (node, name, size, rows, time range, origin, tier) PASS  A rows=92, B rows=92, 92 lines compared
B's files equal A's byte for byte (sha256 of every part)               PASS  92 files, 416113 bytes on B, 416113 on A
B left no partial copy, and its disk accounting matches                PASS  non-part files on B=0, B used_bytes=416113
every copied part is in B's audit trail with its source                PASS  audit rows 'replicate'=92
run 3 finds nothing to fetch                                           PASS  listed=92 present=92 fetched=0; A part responses 93 -> 93
central A stops cleanly                                                PASS  757 ms
edge stops cleanly                                                     PASS  590 ms
A's and the edge's data directories destroyed                          PASS  removed $SCRATCH/a/data and $SCRATCH/edge/data
with A gone, a run says so and changes nothing                         PASS  complete=False error=the source could not be reached: io: Connection refused; B rows=92
restore from B exits 0                                                 PASS  wall 206 ms; RESTORE OK: 92 parts, 6000 rows verified
restore: parts, rows, bytes equal the destroyed edge's                 PASS  restored parts=92 rows=6000 bytes=416113; edge parts=92 rows=6000 bytes=416113
restore: zero mismatches, missing, unreadable                          PASS  hash_mismatch=0 size_mismatch=0 rows_mismatch=0 unreadable=0 fetch_failed=0 missing=0 not_in_inventory=0 reverify_bad=0
restore: per-template row counts identical                             PASS  6 groups
restore: per-(template,service) row counts identical                   PASS  12 groups
restore: catalog templates (id,count) and dictionary keys identical    PASS  6 templates, 12 keys
central B stops cleanly                                                PASS  659 ms
no ERROR lines in either central's log                                 PASS  0 ERROR lines

elapsed 26 s; 33 passed, 0 failed
DRILL PASS
```

## What was not run or not verified

* **No release build, no real network.** Everything is loopback, so nothing here says how fast a replica copies over a link;
  the drill paces the replica on purpose. No throughput number is claimed.
* **`https://` sources** were not exercised (the client is the same `ureq` with rustls as the edge's, but no TLS run was made).
* **Damaged, unservable, wrong-token and S3-source cases are Rust tests, not drill steps.** The drill covers the kill, the
  resume, convergence, credentials by `curl`, and the restore from the replica.
* **A source that answers 200 to a `Range` request** (so the partial copy is discarded) is by construction in
  `fetch_into_place`, with no test of its own.
* **The timer** is covered by a Rust test only; the drill starts every run through the admin API (`interval_secs = 0`).
* **A replica taking uploads from its own edge while replicating** was not run; the install path takes the same lock as
  the upload path and one upload per part at a time (`LocalStore::begin`), and `fetch_part` backs off with an error if an
  upload of that part is under way, but that is not exercised by a test.
* **A source on the S3 backend** was tested with the S3 configuration and no bucket reachable (the paths answer 409 before
  any storage access); no real bucket was involved.
* **Retire and tier changes on the source** are reported as `tier_differs` and `only_here` and never copied; the
  `tier_differs` counter has no test.
* **Partial copies of parts a source later retires** are left on the replica's disk as `.fetch-*.tmp` dot files.
* The restore from the replica was run with the replica still running, reading its directory in place, as the existing drill
  does for the first central.
