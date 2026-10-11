# Threat model: the network listeners

Scope: the HTTP listeners a log-less process can open, what each one does with hostile input, and the test that
shows it. Written 2026-10-11 against branch `hard/recv` (admin API, peer receiver, S3 serve) and extended the same day
on `hard/listeners` (OTLP/HTTP, Splunk HEC, the Sentry proxy, and the parts endpoints' Content-Type rule). It feeds
Plane items 61 (receiver security), 75 (S3 serve hardening) and 136 (security review).

How to read the tables: **Test** is the name of a test in `crates/logless-core/src` that sends the hostile input and
asserts the safe outcome, or "not covered" when nothing does. "Checked, fine" means the test passed on the code as it was
before this branch, so the defence already existed and the test now pins it. "Fixed" means the test failed first. Every
test named here was run and passed (`cargo test -p logless-core --lib -- s3_serve:: httpd:: bounded_http:: peer::
admin_api::` on `hard/recv`; `cargo test -p logless-core --lib -- otlp:: hec:: sentry:: bounded_http:: parts::` on
`hard/listeners`, where the OTLP, HEC, Sentry and parts rows were written). On `hard/listeners` "fixed" means the test
was run against the `tiny_http` listeners first and failed at the assertion the row describes; a test stops at its first
failure, so the later assertions in a fixed test were not run against the old code, and what the row says the old code
did for those comes from reading it (marked "read"). Real-client checks for those three listeners are in
[evidence/listeners-compat.md](evidence/listeners-compat.md).

## Assets

| Asset | Where | Why it matters |
|---|---|---|
| The log store (Parquet under `store/`) | edge and central disks | Logs hold whatever applications print: tokens, emails, query text. Read access to the store is read access to all of it. |
| The WAL and catalog | data dir | Integrity of what is written next; a corrupted catalog loses template ids. |
| Peer tokens (`[peer].tokens`) | config file, console | Write access to the central's store, and read of its capabilities. |
| Admin token (`LOGLESS_ADMIN_TOKEN` or `[admin].token`) | env, config, console backend | Changes retention (deletes data), budgets, the sync target (where logs are sent). |
| S3 key pair (`[serve_s3]`, `LOGLESS_S3_*`) | config, env, console, DuckDB sessions | Read of the whole store. |
| `[peer.parts]` object-storage secret (S3 backend) | central config | Write to the object-storage bucket. Only ever used to sign short-lived URLs. |
| Uploaded parts (`<data_dir>/parts`, local backend) | central disk | The same logs as the store, per edge node. Written only by the upload path; read through S3 serve's `parts` bucket. |
| Upload-token key (local backend) | central process memory, never on disk | Signs upload tokens. With it, a part can be written under any node's directory (still only bytes that hash to their name). |
| Sync target token | `sync-token` (mode 0600) in the data dir | Write access to the next hop. |
| Availability of ingest | the process | Admin, S3 and peer listeners share a process with ingest: memory or threads lost to a listener are lost to ingest. |

## Trust boundaries

```
 apps ──OTLP/HEC/Sentry/files──▶ EDGE log-less ──peer ingest (bearer)──▶ CENTRAL log-less ──presigned PUT──▶ object storage
                                    │  ▲           ──part PUT (upload token)──▶   │  ▲          (S3 backend only; on the local
                                    │  │                                          │  │           backend parts stop at central)
                                    │  ▲                                    │  ▲
                              admin API (bearer, loopback)           S3 serve (SigV4)
                                    │  │                                    │
                                 console backend ◀── browser (never talks to either listener)
```

* **Edge.** Trusts its local applications (anything that can reach its OTLP/HEC/Sentry ports can write logs; those
  listeners are loopback by default; HEC and Sentry can require a token or key, OTLP cannot). Does not trust the
  network: the admin API, S3 serve, HEC and Sentry refuse to leave loopback without a token, keys or allowed keys.
  Does not trust the local browser either: a page that reaches a loopback listener is refused by `Host`,
  `Content-Type` or `Origin` rules (per listener, below).
* **Central.** Trusts only holders of a peer token. A token holder is trusted to send records, not to be able to crash or
  fill the process; the limits below are what makes that true. Every record from a peer is untrusted input to the parser.
* **Console.** A server-side process that holds the admin token and the S3 keys. The browser never receives either and
  never talks to a log-less listener directly, which is why no listener sends CORS headers and why a browser reaching one
  (DNS rebinding, a cross-site form post) is treated as an attack, not a client.
* **Secrets store.** Today: the config file (tokens and keys in clear), environment variables, and `sync.json` plus
  `sync-token` in the data dir. There is no separate secrets service. Anyone who can read those files has the secrets.

Out of scope here: the OTLP **gRPC** listener (`otlp/grpc.rs`, its own HTTP/2 server; not reviewed), TLS (there is
none; run a proxy), the CLI and file tailing, the console's own authentication, the Sentry proxy's upstream side
(spool, forwarding, replay).

## Common machinery

Every HTTP/1.1 listener (admin API, peer receiver, S3 server, OTLP/HTTP, HEC, Sentry) runs on `bounded_http.rs`, a small
HTTP/1.1 server; `tiny_http` is no longer a dependency. It exists because `tiny_http` (0.12) reads each header line into
an unbounded buffer, sets no read timeout, and runs one thread per connection with no cap (read in `tiny_http-0.12.0/src/client.rs`, `read_next_line`, and demonstrated
against the S3 server before this branch: a 64 MiB header was buffered and answered 200).

| Abuse case | Defence | Test |
|---|---|---|
| Header line, request line or header count past the budget (16 KiB, 64 headers) | 431 before any handler or authentication | `bounded_http::tests::the_head_is_capped_by_bytes_by_count_and_by_a_missing_terminator` |
| Head or body that never finishes (slowloris) | whole-request deadline, 408 | `bounded_http::tests::a_client_that_stalls_in_the_head_or_the_body_is_cut_off_at_the_request_timeout` (300 ms deadline; also a one-byte-at-a-time drip, which does not extend it) |
| Many connections | cap, extra connections closed unread; slots return | `bounded_http::tests::connections_past_the_cap_are_closed_unread_and_the_slots_come_back` |
| Malformed request line or header, folded headers, bad or repeated `Content-Length`, non-UTF-8 head, HTTP/2 preface | 400, handler never called | `bounded_http::tests::malformed_heads_are_400_and_never_reach_the_handler` |
| Chunked bodies, and `Content-Length` with `Transfer-Encoding` (request smuggling) | admin, peer, S3: 501, no chunk parser runs | same test; `peer::tests::framing_tricks_are_refused_rather_than_guessed_at` |
| Chunked bodies on the ingest listeners (OTLP, HEC, Sentry read them: `@sentry/node` and the OpenTelemetry JS exporter stream their bodies) | strict parser, `HeadLimits::chunked`: hex size only (extensions after `;` ignored), CRLF only, data capped at the body limit across chunks, trailers capped at the head budget and dropped, bytes after the last chunk dropped; `Content-Length` with `Transfer-Encoding` is 400; any coding but one `chunked` (`gzip, chunked`, `chunked, chunked`, two headers) is 501; a stalled chunk is 408 | `bounded_http::tests::chunked_bodies_are_read_strictly_and_only_when_asked_for` (new code; the old 501 for these listeners was never a defence, `tiny_http` read chunked bodies) |
| Pipelined second request | every reply is `Connection: close`; bytes after the declared body are dropped, never parsed | `peer::tests::framing_tricks_are_refused_rather_than_guessed_at` |
| Refusal that needs only the head (auth, Host) answered after a body is buffered | `Handler::preflight`: answered before the body is read | `bounded_http::tests::a_preflight_refusal_is_sent_without_waiting_for_the_body` |
| Gzip bomb | decoded only where enabled, capped, one byte over is 413 | `bounded_http::tests::gzip_is_decoded_only_when_asked_for_and_only_up_to_the_cap` |
| Client that stops reading a download | 10 s per write, then the connection is dropped | `s3_serve::tests::a_stalled_download_does_not_starve_other_requests` (checks the others are served; the 10 s cut-off itself is not covered) |

Fixed on `hard/recv`: the peer receiver and the S3 server were on `tiny_http` and now use this server. Fixed on
`hard/listeners`: OTLP/HTTP, HEC and Sentry, which were the last three (sections below). Those three use
`bounded_http::ingest_limits(workers)`: `workers` x 32 connections (at least 64), 30 s per request, gzip and chunked
bodies read.

## Peer receiver (`POST /v1/ingest`, `POST /v1/parts/*`, `GET /.well-known/logless`)

Defect fixes: `peer.rs` `Receiver::start` (was `httpd::Server::start`, HEAD line 525; now `BoundedServer::start`, line 598),
`peer.rs` `constant_time_eq` (HEAD line 460, now 525), `peer.rs` `PeerConfig` Debug (HEAD line 57, now 124),
`peer.rs` content-type check (HEAD line 313; now `refuse_head`, line 314).

| Abuse case | Defence | Test |
|---|---|---|
| Oversized body, declared or sent; declared 1 TiB and never sent | 413 from the head, nothing stored | `peer::tests::an_oversized_body_is_413_whether_declared_or_sent_and_stores_nothing` (checked, fine) |
| Gzip bomb (16 MiB of newlines in under 64 KiB) | decompressed cap, 413 | `peer::tests::a_gzip_bomb_is_capped_at_the_decompressed_limit` (checked, fine) |
| Gzip bomb or oversized body without a token | 401 from the head; never inflated, and learns nothing about the limits | `peer::tests::unauthenticated_requests_are_answered_from_the_head_before_any_body_is_buffered` (fixed: it was read and inflated first) |
| Truncated gzip, junk claiming to be gzip | 400, even when the complete lines before the cut decode | `peer::tests::a_truncated_gzip_stream_is_refused_even_when_its_complete_lines_decode`, `peer::tests::bytes_that_are_not_gzip_but_claim_to_be_are_400` (checked, fine) |
| Partial last line, half a record, NUL bytes, invalid UTF-8, arrays, bare strings, two records on a line, 200 000-deep nesting | 400 `bad_record`, whole batch rejected, nothing stored, no stack overflow (serde_json recursion limit) | `peer::tests::malformed_and_partial_ndjson_stores_nothing_and_is_never_a_crash` (checked, fine) |
| Wrong or missing `Content-Type` | 415. A request with no `Content-Type` was accepted; a web page can send exactly that without a CORS preflight (a `Blob` with no type), so on a tokenless loopback listener any page the user visited could write records | `peer::tests::only_the_wire_content_types_and_encodings_are_accepted` (fixed) |
| Wrong `Content-Encoding` (`br`, `deflate`, `gzip, gzip`, `zstd`) | 415 (was: ignored, the body parsed as plain text) | same test (fixed) |
| Missing token, wrong scheme, token in the query string, empty bearer | 401/403, nothing stored | `peer::tests::a_missing_wrong_short_long_or_oddly_presented_token_never_ingests`, `peer::tests::tokens_are_required_when_configured_and_the_capability_document_needs_one_too` |
| Short token, prefix of the token, token plus suffix, 8 KiB token | 403 | `peer::tests::a_missing_wrong_short_long_or_oddly_presented_token_never_ingests` |
| Token plus 256 NULs | was equal to the token: `constant_time_eq` narrowed the length difference to a `u8`, so a difference of 256 cancelled and NUL padding compared equal to the missing bytes. Needs the real token, so no bypass, but it is a broken comparison | `peer::tests::the_token_comparison_looks_at_every_byte_and_cannot_be_fooled_by_length` (fixed) |
| Token comparison exits early | no early exit: every configured token is compared over `max(len)` bytes. Timing is not measured by a test; this is from reading the code | `peer::tests::the_token_comparison_looks_at_every_byte_and_cannot_be_fooled_by_length` covers the result, not the timing. Timing: not covered |
| Two `Authorization` headers (a proxy and the origin may pick different ones) | neither counts, 401 | `peer::tests::a_missing_wrong_short_long_or_oddly_presented_token_never_ingests` (fixed) |
| Weak token on a public bind | refused at start, under 24 characters (as the admin API does) | `peer::tests::a_short_token_is_refused_on_a_non_loopback_bind_only` (fixed) |
| Slow client that never finishes its body, more of them than workers | each holds a connection thread for at most 30 s; with `tiny_http` two such clients held both workers forever and the receiver stopped answering | `peer::tests::a_client_that_never_finishes_its_body_does_not_use_up_the_receiver` (fixed) |
| A slow disk (or a sender that floods it) holds requests open while they wait for the WAL fsync | each authenticated ingest waits at most `peer.ack_wait_ms` (5 s) and then gets 503 `wal_slow`; the connection cap below bounds how many wait at once; a full queue is 503 `busy` before any wait | `peer::tests::the_wait_is_bounded_and_a_late_sync_makes_the_resend_a_duplicate`, `sync::soak::a_full_queue_ahead_of_a_slow_wal_is_a_503_and_every_record_lands_exactly_once` |
| Many parallel connections | cap `workers` x 32, at least 64 | `bounded_http::tests::connections_past_the_cap_are_closed_unread_and_the_slots_come_back` (cap tested at 4; the peer's own numbers are not covered) |
| Huge header line (2 MiB), 200 headers, 200 kB URL, a head with no end | 431, receiver keeps serving | `peer::tests::huge_or_numerous_header_lines_are_refused_with_431_and_the_receiver_keeps_serving` (fixed: a 2 MiB header was accepted and answered 200) |
| Method confusion: `PUT`/`DELETE`/`HEAD`/`OPTIONS`/`TRACE` on ingest, `POST` on the capability document | 404/405; no CORS headers on any of them | `peer::tests::only_the_documented_method_and_exact_path_reach_a_handler` (checked, fine) |
| Path normalisation: `//v1/ingest`, `/v1/ingest/`, `/V1/INGEST`, `/v1/%69ngest`, `/v1/x/../ingest`, `;`, `%00`, absolute-form | none of them route; only the exact path does | same test (checked, fine) |
| DNS rebinding or a foreign `Host` on a tokenless (loopback) listener | 403, also with no `Host` | `peer::tests::a_tokenless_loopback_listener_refuses_a_foreign_host_header` (fixed) |
| Tokenless listener on a public address | refused at start | `peer::tests::an_open_listener_on_a_public_address_is_refused` |
| Tokens printed by a logged config | `PeerConfig` Debug redacts them (`parts.secret_access_key` already was) | `peer::tests::a_logged_config_never_carries_a_token` (fixed) |
| Capability document leaking tokens | it carries no secret | `peer::tests::the_capability_document_does_not_leak_secrets` |
| CRLF line endings, blank lines | accepted (a control, not an attack) | `peer::tests::crlf_line_endings_and_blank_lines_are_fine` |
| Forged `logless.origin` / `logless.hops`, replayed event ids | belongs to hop and dedupe handling, not reviewed here | existing `a_record_that_came_back_to_its_origin...`, `a_retried_batch_is_recognised...` |
| Cross-site POST to the parts endpoints with no `Content-Type`, or a form or text type (tokenless, loopback, parts enabled) | 415 from the head, the same rule as ingest (`refuse_head`; `peer.rs` preflight now passes the parts paths through it). The edge sender already sent `application/json` | `parts::tests::a_parts_request_a_page_could_send_without_a_preflight_is_refused_unread` (fixed: a typeless presign was answered 200 with a signed URL) |

### Part upload to central's own disk (`PUT /v1/parts/blob/<node>/<name>`, local backend)

Added on `wf/parts-local` (ADR 0003). This is the one request on any listener whose body is larger than memory should
hold, and the one that writes a file named by the client. It is authorised by an upload token central signed at
presign, not by the bearer token. Every row is new code with a new test; all were run and passed
(`cargo test -p logless-core -- parts:: bounded_http:: s3_serve:: restore::`).

| Abuse case | Defence | Test |
|---|---|---|
| A part held in memory (512 MiB by default, several at once) | the body is handed to the handler still on the wire (`Handler::stream`, `bounded_http::BodyReader`) and copied to a temporary file in 256 KiB pieces; the 4 MiB body cap of the listener still applies to every other request | `parts::tests::a_part_larger_than_the_request_cap_is_streamed_to_disk_in_pieces` (9 MiB through a 4 MiB cap), `bounded_http::tests::a_streamed_body_is_read_past_the_body_cap_never_past_its_length_and_not_while_it_stalls`; a 128 MiB upload with central's RSS sampled is in `docs/evidence/parts-local-backend.md` |
| No token, a mangled token, two token headers | 403 `bad_token` from the head; body unread | `parts::tests::a_local_upload_with_the_wrong_bytes_or_the_wrong_length_stores_nothing` |
| Expired token | 403 `token_expired` | `parts::tests::an_upload_token_is_good_for_one_node_one_name_one_size_and_a_while` |
| Token replayed for another name, for another node's directory, with its size or expiry edited, or signed by another central (or this one before a restart) | 403 `bad_token`: the HMAC covers node, name, size and expiry, compared in constant time before the expiry is looked at | same test |
| Token replayed for its own part | allowed until it expires: the same bytes to the same place, stored once (`already_present`). Other bytes are refused; while one copy is still arriving, the others are 409 `upload_in_progress` and reserve nothing | `parts::tests::a_duplicate_local_upload_is_checked_and_changes_nothing` |
| The right size, other bytes | read to the end, 422 `bad_content`, temporary file removed, not present, commit refused 409, nothing catalogued | `parts::tests::a_local_upload_with_the_wrong_bytes_or_the_wrong_length_stores_nothing` |
| `Content-Length` over `max_part_bytes` (1 TiB declared), or other than the size the token was issued for | 413 `too_large` / 400 `size_mismatch` from the head; body unread | same test |
| Body shorter than `Content-Length` (client hangs up), body longer than it | 400 `incomplete_body`, nothing kept; bytes past `Content-Length` are never read | same test |
| Body that stalls, or drips to hold a connection | each read must progress within the listener's 30 s, and the whole body within 30 s plus its length at `min_upload_bytes_per_sec` (16 KiB/s by default); then 408 `upload_stalled`, nothing kept | `bounded_http::tests::a_streamed_body_is_read_past_the_body_cap_never_past_its_length_and_not_while_it_stalls`, `bounded_http::tests::the_slowest_average_rate_of_a_streamed_body_is_the_listeners_setting` (the listener's rule, with a 300 ms limit; the peer's own 30 s is not waited for by a test) |
| Traversal and foreign directories in the path: `../edge-b/<name>`, `edge-a/../edge-b/<name>`, `%2F` and `%2e` spellings, `//`, a trailing segment, an upper-case name, no node | the path is not decoded; node and name are each validated (`valid_node`, `valid_name`) before any path is built; 403/404/422 and no file anywhere | `parts::tests::a_foreign_node_or_a_traversal_never_reaches_another_directory_on_centrals_disk` (13 paths, with a valid token) |
| A node asking for a URL under another node, or a shared ingest token asking for one | 403 `foreign_prefix` / `no_node` at presign, as on S3 (ADR 0001); a node's part is not "present" for another node | same test, `parts::tests::an_upload_token_is_good_for_one_node_one_name_one_size_and_a_while` |
| Filling central's disk | `disk_budget_bytes` (64 GiB by default) over installed parts plus uploads in flight: 507 at presign and again at the PUT, before the body is read; counted in `parts_store.refused_budget` | `parts::tests::the_disk_budget_refuses_with_507_counts_it_and_frees_with_a_delete` |
| A crash mid-upload, or between the rename and the commit | a part is absent or whole, never partial: temporary files are removed at the next start; a renamed part is present and the retry commits it | `parts::tests::a_crash_between_the_rename_and_the_commit_leaves_a_part_that_is_absent_or_committable` (the crash is a stopped listener and a planted temporary file, not a killed process) |
| Cross-site PUT from a web page | needs the `x-logless-upload-token` header, which a page cannot send without a CORS preflight this listener never answers; and the token | not covered by a test of its own (the no-token rows above are the same refusal) |
| Reading parts back | not on this listener at all. S3 serve's `parts` bucket, read-only, same credentials, same traversal and symlink rules as the store; temporary files are dot files and are neither listed nor served | `s3_serve::tests::uploaded_parts_are_a_second_read_only_bucket_apart_from_the_store` |

### Edge pinning and retire (`[sync_parts]`)

Added on `wf/parts-ack`. Edge-side only; central's endpoints are unchanged, so a node still cannot retire another node's
part (`parts::tests::a_node_can_only_presign_commit_and_retire_under_its_own_prefix`). New code, new tests, all run.

| Abuse case | Defence | Test |
|---|---|---|
| Pinning fills the edge's disk (central down for a long time) | the pinned bytes are at most `pin_bytes` and a quarter of `disk_budget_bytes`, and nothing older than `pin_age`; past that, reclaim and retention delete as before and the loss is counted | `parts::tests::an_old_unconfirmed_file_is_not_pinned_and_the_limit_never_passes_a_quarter_of_the_budget`, `parts::tests::central_down_pins_to_the_limit_reclaim_past_it_is_counted_and_central_back_gets_the_rest`, `budget::tests::reclaim_never_picks_a_pinned_file_even_when_it_is_the_cheapest_and_oldest` |
| A deployment that never asked for the timer holds files back | pinning is on only when `[sync_parts] interval` is set and a peer is configured | `parts::tests::nothing_is_pinned_while_the_timer_is_off_or_no_peer_is_set` |
| A wrong or unmounted store directory makes every uploaded part look removed, and central marks them all cold | a store directory that does not exist yields no retires; one that exists and is empty does retire, and that is true: the node holds none of those files | `parts_edge::tests::a_missing_store_directory_retires_nothing_and_a_removed_file_is_pending` (the empty-but-present directory is not tested) |
| A retire lost to an outage or a crash | the pending retire is the state line with no file, so it is rebuilt from disk after a restart; a duplicate is a 404, treated as done | `parts::tests::a_retire_central_never_heard_is_still_pending_after_a_restart_and_is_sent_then` |
| A retire that central will never accept blocks the rest | 400 and 422 are dropped with a warning; other failures back off but do not skip ahead | same test; `parts::retire_remote` |
| Two upload jobs at once (timer and operator) | one job at a time per edge; the second start is refused | `parts::tests::the_timer_never_overlaps_a_running_upload_and_runs_again_only_when_due` |

Left open, and known:

* **Uploads are not capped per node.** A node with a valid token can hold one connection per upload for as long as the
  rule above allows (about 9 hours for 512 MiB at the default minimum rate), up to the listener's connection cap, which
  ingest from every other node shares. A node token is already write access to central's store. Only one upload of a
  given part is accepted at a time (409 `upload_in_progress` for the others, no reservation), so one token replayed on
  many connections reserves the budget once; a node that presigns many different names can still reserve one
  part-size per name, up to the connection cap (`parts::tests::a_second_upload_of_a_part_already_arriving_reserves_nothing_and_is_told_to_retry`).
* **A node can use the whole disk budget**, and parts it uploads but never commits are not cleaned up (the sweep reads
  only the catalog). They show as `parts_store.used_bytes` above the catalog's total.
* **No scrub.** A byte that changes on central's disk after the upload is found by `logless restore`, not by central.
* **A token is a bearer credential for one upload** until it expires (900 s by default). It is returned in the presign
  answer's body, to the authenticated node, and sent in a header.

## S3 serve (`crates/logless-core/src/s3_serve.rs`)

Defect fix: the listener was `tiny_http::Server::http` (HEAD line 173) with a request pool and detached stream threads;
it is now `BoundedServer::start` (line 183) with a streaming reply body (`httpd::Streamed`, `bounded_http::copy_exact`).

| Abuse case | Defence | Test |
|---|---|---|
| Traversal: `..`, `%2e%2e`, `%2f`, double and triple encoding, `\`, `%5c`, `C:\`, overlong UTF-8 (`%c0%ae`, `%e0%80%ae`), invalid UTF-8, NUL, trailing dot/space/slash, absolute paths, in the key, the bucket and listing prefixes, GET and HEAD | decoded exactly once, then each segment must be non-empty, not start with `.`, and contain no `\` or NUL; 404 | `s3_serve::tests::traversal_in_every_encoding_and_position_finds_nothing` (about 50 spellings; checked, fine), `path_traversal_attempts_are_all_not_found` |
| Symlink to a file or directory outside the store, link loop back to the store | every path component is checked with `symlink_metadata`; the walker uses `file_type`, which does not follow links | `s3_serve::tests::a_symlinked_directory_or_file_in_the_store_is_neither_listed_nor_read`, `symlinks_out_of_the_store_are_not_followed` (checked, fine) |
| Files that are not Parquet (temp, hidden, text) | not servable, not listed | `tmp_hidden_and_non_parquet_files_are_not_served_or_listed` |
| Range: numbers past `u64`, `bytes=-0`, start past the end, `5-4`, `0x10`, unit other than bytes | 416 or the whole object; never more than the object | `s3_serve::tests::range_requests_cannot_ask_for_more_than_the_object_or_for_many_pieces` (checked, fine) |
| Many ranges (1000 in one header) | a multi-range header is ignored: one plain GET, no multipart assembly | same test (checked, fine) |
| Huge `Range` header (30 kB) | 431 (head budget) | same test (fixed: tiny_http accepted any size) |
| Listing a huge directory | at most 1000 entries a page whatever `max-keys` is; `max-keys` junk is 400; continuation token must be one this server issued | `s3_serve::tests::a_huge_directory_lists_a_bounded_page_whatever_the_client_asks` (5000 files, 3000 prefixes), `a_huge_directory_lists_a_page_correctly` (checked, fine) |
| Huge query string, many parameters, malformed escapes | up to the head budget it is answered; past it 431; the prefix is echoed once | `s3_serve::tests::very_long_query_strings_and_many_parameters_are_answered_not_amplified` (fixed above 16 KiB) |
| Huge or numerous headers, a head with no end | 431 | `s3_serve::tests::a_huge_header_is_refused_without_buffering_it` (fixed: 64 MiB header was buffered, 200) |
| Slowloris on the head; thousands of idle connections | request deadline 15 s; connection cap | `bounded_http` tests above; S3's own cap and timeout values are not covered |
| `PUT`, `POST`, `DELETE`, `PATCH`, `OPTIONS`, `COPY`, `PROPFIND`, multi-delete (`?delete`), multipart (`?uploads`), copy-source header | 405 with `Allow: GET, HEAD` (403 first when unauthenticated); the store is byte-for-byte unchanged afterwards | `s3_serve::tests::nothing_but_get_and_head_is_served_and_the_store_never_changes` (checked, fine) |
| Unsigned request when keys are set | 403 for every path and method, before the key is looked up, so existence is not revealed | `credentials_gate_every_request`, `s3_serve::tests::authentication_is_judged_before_anything_else_about_the_request` |
| Bad signature: wrong secret, flipped first or last digit, short, empty, non-hex, wrong algorithm, SigV2, megabyte-long | 403 `SignatureDoesNotMatch` / `AccessDenied`; no secret in the body | `s3_serve::tests::a_signature_is_only_good_for_the_request_it_was_made_for` (checked, fine) |
| Signature reused for another method, key, query, or without `host`/`x-amz-date` signed, or under another Host | 403 | same test (checked, fine) |
| Unknown access key vs wrong secret (key-id oracle) | same code, same point in the check | `unknown_access_key_is_indistinguishable_from_a_bad_secret` |
| Expired or future-dated signature (1 h, 1 day, 1 year); garbage dates | 403, window 15 minutes either side | `s3_serve::tests::a_stale_or_future_signature_is_refused_and_a_replay_inside_the_window_is_accepted` (checked, fine) |
| Replay of a captured signed request inside the window | **accepted by design**: SigV4 has no nonce, the requests are read-only and return the same bytes. A captured request can be replayed for up to 15 minutes | same test pins this |
| Presigned (query-string) credentials on a read | not accepted | `s3_serve::tests::a_presigned_query_string_is_not_credentials_for_reading` |
| DNS rebinding in anonymous mode | only loopback `Host` values; with keys the Host is signed | `anonymous_mode_rejects_non_loopback_host_header`, `allowed_hosts_is_enforced_even_with_credentials` |
| Anonymous on a public bind; one key of the pair | refused at start | `non_loopback_without_credentials_is_refused`, `one_credential_alone_is_a_startup_error` |
| Secret key in a logged config | redacted | `s3_serve::tests::a_logged_config_never_carries_the_secret_key` |
| Stalled downloads starving everyone | each holds one connection thread until its 10 s write timeout; other requests still served | `a_stalled_download_does_not_starve_other_requests` |
| Cost of listing: each page re-reads and sorts the whole directory | not bounded; pagination over a directory of N files costs about N x (N / 1000) work | not covered, not measured |

## Admin API (`/admin/v1/*`)

It already ran on `bounded_http`. The review found one gap (duplicate `Authorization`, `admin_api.rs` line 1004) and
otherwise pinned what was there.

| Abuse case | Defence | Test |
|---|---|---|
| Body over 1 MiB, declared or sent, declared 1 TiB | 413 from the head; nothing applied | `admin_api::tests::an_oversized_body_is_413_declared_or_sent_and_changes_nothing` (checked, fine) |
| Gzip bomb | `Content-Encoding` is not decoded; the bytes are JSON-parsed and fail 4xx | `admin_api::tests::a_compressed_body_is_never_inflated_by_the_admin_listener` (checked, fine) |
| Method confusion: `DELETE`/`PATCH`/`TRACE`/`CONNECT`/`OPTIONS`/`HEAD`/lower-case `get` on every route; `X-HTTP-Method-Override`, `?_method=PUT` | 404/405 after authentication, 401 before; overrides ignored; a body on a GET is ignored | `admin_api::tests::every_method_but_the_documented_one_is_refused_and_the_override_headers_do_nothing` (checked, fine), `unknown_paths_and_methods_are_404_and_405` |
| Path normalisation: `//`, `/./`, `/../`, `%69`, `%2f`, `%00`, `;`, case, trailing slash, absolute-form, `*` | only the exact path routes; the query cannot smuggle a path; `limit` is validated | `admin_api::tests::only_the_exact_path_is_routed` (checked, fine) |
| Wrong, short, prefix, NUL-padded, 8 kB token; token in the query or a cookie | 401. The comparison is on SHA-256 digests, so length and position of a mismatch are not visible | `admin_api::tests::a_token_has_to_arrive_in_exactly_one_authorization_header`, `a_wrong_or_near_miss_token_is_refused`, `every_request_needs_the_token_including_get` |
| Two `Authorization` headers | neither counts, 401 (before: the first one counted) | `admin_api::tests::a_token_has_to_arrive_in_exactly_one_authorization_header` (fixed) |
| Foreign, duplicate, malformed or missing `Host`; names that resolve to loopback (`127.1`, `localhost.`, `nip.io`, `[::ffff:127.0.0.1]`); absolute-form target with another Host | 403 before the token is looked at; only literal loopback spellings pass | `admin_api::tests::host_names_that_merely_resolve_to_loopback_are_not_loopback_names`, `host_header_must_be_exactly_one_well_formed_host`, `a_foreign_host_header_is_refused_before_auth` (checked, fine) |
| Cross-origin reads or writes from a page | no CORS headers ever; `OPTIONS` is 405; replies are `no-store` | `admin_api::tests::replies_carry_no_cross_origin_grants_and_are_not_cacheable`, `no_cors_headers_are_ever_sent` |
| Huge headers, 200 headers | 431 before auth | `oversized_or_numerous_headers_are_refused_before_any_auth` |
| Huge query string | 431 over 16 KiB | `admin_api::tests::only_the_exact_path_is_routed` |
| No token configured; weak token on a public bind | refused at start | `the_listener_refuses_to_start_without_a_token`, `a_short_token_is_refused_on_a_non_loopback_bind_only` |
| Token in a logged config | redacted | `config_debug_never_prints_the_token` |
| Lost update by two writers; unconfirmed destructive change; log injection through `actor` | `If-Match` versions, impact-bound confirmation, sanitised actor | `two_concurrent_writers_cannot_both_win`, `destructive_changes_need_confirmation_and_say_what_they_would_delete`, `actor_is_required_and_sanitised` (pre-existing) |
| Admin API as a proxy to internal hosts (`sync` target URL, `sync/test`) | **not defended**: an admin-token holder can make the process send a request to any URL | a saved token is never sent to a different address (`a_saved_token_is_never_sent_to_a_different_address`); the SSRF itself is not covered |

## OTLP/HTTP (`POST /v1/logs`, `crates/logless-core/src/otlp/http.rs`)

No credential exists in OTLP/HTTP as served here, so the listener's defences are its limits, its `Host` rule on
loopback and its `Content-Type` rule. Defect fix: `Receiver::start` was `httpd::Server::start` (tiny_http) and is now
`BoundedServer::start` with `OtlpHandler::preflight`.

| Abuse case | Defence | Test |
|---|---|---|
| Uploads that stop half way, more of them than workers | each holds its own connection thread for at most 30 s; others are answered at once. With `tiny_http` and two workers, two such uploads stopped the receiver answering | `otlp::http::tests::a_stalled_upload_does_not_stop_the_receiver_answering` (fixed) |
| 2 MiB header line, 200 headers | 431, receiver keeps serving | `otlp::http::tests::a_2_mib_header_is_431_and_the_receiver_keeps_serving` (fixed: the 2 MiB header was accepted, 200) |
| Many connections | cap `workers` x 32, at least 64; past it closed unread; slots come back | `otlp::http::tests::connections_past_the_cap_are_closed_and_the_slots_come_back` (fixed: no cap) |
| Oversized body, sent or declared (1 TiB, never sent) | 413 from the head, as a protobuf `Status` | `otlp::http::tests::an_oversized_or_damaged_body_is_refused_before_the_decoder` (sent: checked, fine; declared: fixed, `tiny_http` had not answered within 2 s) |
| Gzip bomb (16 MiB in under 64 KiB), truncated gzip, junk claiming to be gzip | 413 at the decompressed cap; 400 | same test (not run on the old code); `gzip_bomb_is_capped` (pre-existing, passed on the old code) |
| Malformed protobuf: truncated, length past the end, endless varint, wire type 7, values nested 10 000 deep | 400 `INVALID_ARGUMENT`; nesting stops at the decoder's depth limit (16), no stack overflow | `otlp::http::tests::malformed_protobuf_and_json_are_400_and_never_a_crash` (checked, fine) |
| Malformed OTLP/JSON: truncated, bare string, 100 000-deep arrays, `1e999999`, invalid UTF-8 | 400 with a JSON `Status` (serde_json's recursion limit) | same test (checked, fine) |
| A web page posting to 127.0.0.1 with no `Content-Type`, `text/plain`, a form type (no CORS preflight needed) | 415 from the head: only `application/x-protobuf` and `application/json`, exactly one `Content-Type`. Both need a preflight, which is never answered | `otlp::http::tests::only_protobuf_and_json_content_types_and_gzip_are_accepted` (fixed: `text/plain` was decoded as protobuf and stored, 200) |
| `Content-Encoding` other than gzip (`br`, `deflate`, `zstd`, `gzip, gzip`, two headers) | 415 (before, read: ignored, the compressed bytes went to the decoder) | same test |
| DNS rebinding: a foreign, missing or second `Host` on a loopback bind | 403 from the head; literal loopback names only | `otlp::http::tests::a_loopback_receiver_refuses_a_foreign_host_header` (fixed) |
| Full pipeline | 503 with `Retry-After: 1`, as before; a chunked gzip export is read | `otlp::http::tests::a_full_pipeline_says_when_to_retry_and_a_chunked_export_is_read` |

## Splunk HEC (`/services/collector*`, `crates/logless-core/src/hec.rs`)

Defect fix: `Receiver::start` was `httpd::Server::start` and is now `BoundedServer::start` with `HecHandler::preflight`,
which judges the token, `Host`, `Origin`, channel and encoding before the body is read. Splunk's response codes are kept
(2 token required, 3 invalid authorization, 4 invalid token, 6 invalid data format, 9 busy, 10 channel missing,
11 invalid channel, 14 ack disabled, 17 healthy) and so are ack ids and the ack poll.

| Abuse case | Defence | Test |
|---|---|---|
| Uploads that stop half way, more of them than workers | 30 s per connection thread; others answered | `hec::tests::a_stalled_upload_does_not_stop_the_receiver_answering` (fixed) |
| 2 MiB header line, 200 headers | 431, keeps serving | `hec::tests::a_2_mib_header_is_431_and_the_receiver_keeps_serving` (fixed: the 2 MiB header reached the token check, 401) |
| Many connections | cap `workers` x 32, at least 64 | `hec::tests::connections_past_the_cap_are_closed_and_the_slots_come_back` (fixed) |
| Oversized body sent or declared, gzip bomb, truncated or fake gzip, malformed JSON (truncated batch, NUL, invalid UTF-8, an array, 20 000-deep nesting) | 413 / 400 with code 6; a batch with one bad event stores nothing | `hec::tests::oversized_bombed_or_damaged_bodies_are_refused_with_their_splunk_codes` (sent: checked, fine; declared 1 TiB: fixed, not answered within 2 s; the rest not run on the old code) |
| Missing, empty, wrong, prefix, suffix, other-case token; `Bearer` scheme; wrong header with a good query token | 401 code 2/3 or 403 code 4; the header wins over the query | `hec::tests::a_token_is_judged_from_the_head_and_must_arrive_once` (all but the last checked, fine; the last not run on the old code) |
| Two `Authorization` headers (also two good ones); two `token` query parameters | neither counts, 401 code 3 | same test (fixed: good one first then a wrong one was 200; the rest read: the first one counted) |
| No token with a body: 3 MiB declared and never sent; a 64 MiB gzip bomb with a wrong token | 401/403 from the head, nothing read or inflated, no size limit revealed (before, read: the body was read before the handler ran) | same test |
| `Content-Encoding` other than gzip, or stacked | 415 | `hec::tests::only_gzip_is_decoded_and_stacked_headers_are_refused` (fixed: an uncompressed body labelled `br` on `/raw` was stored, 200) |
| Two `X-Splunk-Request-Channel` headers (which channel owns the ack id) | 400 code 11 (before, read: the first was used) | same test |
| Tokenless loopback listener and a page in a local browser: a cross-site POST (`Origin`, `Sec-Fetch-Site`), `text/plain`, or DNS rebinding (`Host`) | 403 from the head when there are no tokens and the request carries `Origin` or `Sec-Fetch-Site`, or a non-loopback or second `Host`. HEC itself takes any `Content-Type`, so a type rule is not available | `hec::tests::a_tokenless_receiver_refuses_web_pages_and_foreign_host_names` (fixed: accepted, 200) |
| Chunked upload with a channel | read; ack id issued and pollable as before | `hec::tests::a_chunked_upload_gets_its_ack_id` |
| Tokenless on a public address | refused at start (pre-existing) | `an_empty_token_list_refuses_to_bind_a_public_address` |

## Sentry proxy (`/api/<project>/envelope/`, `/store/`, `crates/logless-core/src/sentry.rs`)

Defect fix: `Receiver::start` was `httpd::Server::start` and is now `BoundedServer::start` with `SentryHandler::preflight`
(key, `Host`, encoding judged from the head). Envelope handling, the `{"id"}` reply, 429 with `Retry-After` and the
reflected `X-Sentry-Rate-Limits` are unchanged (`upstream_rate_limits_are_reflected_back_to_the_sdk`,
`a_full_pipeline_is_429_not_a_silent_drop`).

| Abuse case | Defence | Test |
|---|---|---|
| Uploads that stop half way | 30 s per connection thread; others answered | `sentry::tests::a_stalled_upload_does_not_stop_the_proxy_answering` (fixed) |
| 2 MiB header line, 200 headers | 431, keeps serving | `sentry::tests::a_2_mib_header_is_431_and_the_proxy_keeps_serving` (fixed: reached the handler, 401) |
| Many connections | cap `workers` x 32, at least 64 | `sentry::tests::connections_past_the_cap_are_closed_and_the_slots_come_back` (fixed) |
| Oversized envelope sent or declared, gzip bomb, truncated or fake gzip | 413 / 400, nothing stored or forwarded | `sentry::tests::oversized_bombed_or_malformed_envelopes_store_and_forward_nothing` (sent: checked, fine; declared: fixed, not answered within 2 s; the rest not run on the old code) |
| Malformed envelopes: header not JSON or an array, 20 000-deep header, bad item header, `length` past the end or near `u64::MAX`, negative `length`, 1001 items, invalid UTF-8 | 400 | same test (not run on the old code; `malformed_envelopes_are_400_and_not_forwarded` covered two of them before) |
| Unknown, prefix or other-case key; no key | 401 | `sentry::tests::a_key_is_judged_from_the_head_and_must_arrive_once` (checked, fine) |
| Two `X-Sentry-Auth` headers; header and query keys that disagree; two `sentry_key` parameters; `X-Sentry-Auth` and `Authorization` that disagree | 401. The same key in two places is accepted | same test (fixed: a good header then a wrong one was 200; the rest read: the first source in a fixed order counted) |
| No good key with a body: 3 MiB declared and never sent; a bomb with a wrong key | 401 from the head, nothing read or inflated (before, read: the body was read first) | same test |
| `Content-Encoding` other than gzip (`br`, which `sentry-sdk` for Python uses when `brotli` is installed; `deflate`, `zstd`; stacked) | 415 naming gzip | `sentry::tests::only_gzip_is_decoded_and_other_encodings_are_named_not_misread` (fixed: an uncompressed envelope labelled `br` was accepted; a real brotli body would have been parsed as text and refused as malformed) |
| DNS rebinding when `allowed_keys` is empty (loopback only) | 403 for a foreign or second `Host` | `sentry::tests::an_open_key_policy_refuses_a_foreign_host_header` (fixed) |
| `@sentry/node` streams its body: chunked, gzipped past 32 KiB | read | `sentry::tests::the_node_sdk_shape_a_chunked_gzip_stream_is_read`; real SDKs in the evidence file |
| A web page posting `text/plain` with a key in the query | **accepted by design**: browser SDKs send exactly that so that no preflight is needed, and a DSN key is public. On a loopback proxy with no `allowed_keys` any local page can write envelopes; with `allowed_keys` a page that knows a key can | not a defect; pinned by `the_key_may_arrive_in_the_query_string` |

## Not defended or not done

1. **Residuals of the OTLP, HEC and Sentry listeners** (moved to `bounded_http` on `hard/listeners`):
   * OTLP has no credential. On a non-loopback bind anyone who can reach the port can write logs, and the `Host` rule is
     off there (any name is legitimate). The `Content-Type` rule still stops a browser page.
   * The tokenless HEC rule against browsers rests on browsers sending `Origin` (or `Sec-Fetch-Site`) on a cross-site
     POST, which current browsers do; a client that strips both is not a browser page and is trusted like any local
     process.
   * HEC token comparison is plain string equality, not constant time, and a short HEC token on a public bind is not
     refused (the peer and admin listeners refuse under 24 characters). Not changed here.
   * Refusals of the head (431, 408, malformed head 400, transfer-coding 501) come from `bounded_http` as `{"error":...}`
     JSON, not as an OTLP `Status` or a HEC `{"text","code"}`. A client that parses a fixed error shape gets a foreign one
     for those; the status code is right.
   * The 30 s request deadline of these listeners is not exercised by their own tests (it would take 30 s); the
     mechanism is tested in `bounded_http` at 300 ms, and the listener tests show other clients are served meanwhile.
   * Every reply is `Connection: close`. Exporters reconnect per batch; the cost was not measured. OTLP partial success
     (`partialSuccess` in the response) was never implemented and still is not: rejected records are not reported back.
   * Splunk forwarders, the OpenTelemetry Collector and the OpenTelemetry SDKs were not available; HEC and OTLP were
     checked with `curl` only (see the evidence file). A real client that sends `Transfer-Encoding` with anything but
     one `chunked`, or (OTLP) a `Content-Type` other than the two OTLP types, is now refused.
   * The OTLP gRPC listener has its own HTTP/2 server and was not reviewed.
2. **No TLS.** Bearer tokens and SigV4 requests cross the network in clear unless a proxy terminates TLS. A passive
   observer gets the token, or a signed request to replay for 15 minutes, and with a weak S3 secret can recover it offline
   (the S3 secret has no minimum length; the peer and admin tokens do, 24 characters, on non-loopback binds).
3. **No rate limiting or lockout on failed authentication.** Online guessing is bounded only by the connection cap and the
   token length. Per-address limits were not built.
4. **Memory of an authenticated sender.** A valid token may send 4 MiB compressed that inflates to 64 MiB and parses into
   records, on up to 64 connections at once. The defaults cap one request, not the sum. There is no global in-flight budget.
   Not measured.
5. **A token holder can write anything**: forge `logless.origin`/`logless.hops` to have records dropped as loops, fill the
   store up to the disk budget, or send a record claiming any service. Tokens are not scoped per sender.
6. **Slots can be held for the deadline.** An attacker who can open connections can hold all 64 slots of a listener for
   10 to 30 s each and so deny that listener (since `hard/listeners` this includes OTLP/HTTP, HEC and Sentry). No
   per-source cap.
7. **Anonymous loopback mode** (no tokens, no keys) trusts every local process and, for the peer receiver and S3, any web
   page that can name `127.0.0.1` itself. The Host and Content-Type rules close rebinding and blind cross-site writes for
   ingest and, since `hard/listeners`, the parts endpoints; the same kind of rules cover OTLP and tokenless HEC. A page
   can still write to a loopback Sentry proxy (above, by design).
8. **Secrets at rest are plain text** in the config file and environment. `sync-token` is written 0600 (`sync.rs`, line
   154) but that was read, not tested here. `SyncTarget` derives `Debug` and carries the token; nothing found prints it,
   but a `{:?}` would. It is in `sync.rs`, which was not mine to edit.
9. **Redirects.** The sender and the admin `sync/test` use `ureq`; its default (`RedirectAuthHeaders::Never` in 3.3.0, read
   in its source) does not forward `Authorization` to a redirect target. Not tested.
10. **Keep-alive is gone** on the S3 and peer listeners (`Connection: close`). That is deliberate (no per-connection state
    to abuse) and costs a handshake per request; not measured against a DuckDB scan.
11. **Logs are sensitive content.** Nothing here redacts what applications log; S3 read access is access to all of it.

## What was checked and was fine

* Path traversal on the S3 key and prefix in about 50 spellings and in the bucket position: none reached a file outside the
  store, on the code as it was before this branch.
* Symlinks (file, directory, loop) in the store: not followed, not listed.
* Range arithmetic at `u64` limits: no overflow, never more than the object.
* Listing bounds (1000 a page) with 5000 files and 3000 prefixes.
* SigV4: the signature binds method, path, query, host and date; the key-id oracle is closed; presigned reads are refused.
* Admin API: method override headers, path tricks, Host spellings, CORS, oversized and compressed bodies, near-miss tokens.
* Peer: oversized, bomb, truncated and malformed bodies are all rejected without storing anything; every one of those
  tests passed on the unmodified code, so those defences already existed.
