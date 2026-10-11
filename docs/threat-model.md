# Threat model: the network listeners

Scope: the four HTTP listeners a log-less process can open, what each one does with hostile input, and the test that
shows it. Written 2026-10-11 against branch `hard/recv`. It feeds Plane items 61 (receiver security), 75 (S3 serve
hardening) and 136 (security review).

How to read the tables: **Test** is the name of a test in `crates/logless-core/src` that sends the hostile input and
asserts the safe outcome, or "not covered" when nothing does. "Checked, fine" means the test passed on the code as it was
before this branch, so the defence already existed and the test now pins it. "Fixed" means the test failed first. Every
test named here was run and passed (`cargo test -p logless-core --lib -- s3_serve:: httpd:: bounded_http:: peer::
admin_api::`).

## Assets

| Asset | Where | Why it matters |
|---|---|---|
| The log store (Parquet under `store/`) | edge and central disks | Logs hold whatever applications print: tokens, emails, query text. Read access to the store is read access to all of it. |
| The WAL and catalog | data dir | Integrity of what is written next; a corrupted catalog loses template ids. |
| Peer tokens (`[peer].tokens`) | config file, console | Write access to the central's store, and read of its capabilities. |
| Admin token (`LOGLESS_ADMIN_TOKEN` or `[admin].token`) | env, config, console backend | Changes retention (deletes data), budgets, the sync target (where logs are sent). |
| S3 key pair (`[serve_s3]`, `LOGLESS_S3_*`) | config, env, console, DuckDB sessions | Read of the whole store. |
| `[peer.parts]` object-storage secret | central config | Write to the object-storage bucket. Only ever used to sign short-lived URLs. |
| Sync target token | `sync-token` (mode 0600) in the data dir | Write access to the next hop. |
| Availability of ingest | the process | Admin, S3 and peer listeners share a process with ingest: memory or threads lost to a listener are lost to ingest. |

## Trust boundaries

```
 apps ──OTLP/HEC/Sentry/files──▶ EDGE log-less ──peer ingest (bearer)──▶ CENTRAL log-less ──presigned PUT──▶ object storage
                                    │  ▲                                    │  ▲
                              admin API (bearer, loopback)           S3 serve (SigV4)
                                    │  │                                    │
                                 console backend ◀── browser (never talks to either listener)
```

* **Edge.** Trusts its local applications (anything that can reach its OTLP/HEC/Sentry ports can write logs; those
  listeners are loopback by default and have no authentication). Does not trust the network: the admin API and S3 serve
  refuse to leave loopback without a token or keys.
* **Central.** Trusts only holders of a peer token. A token holder is trusted to send records, not to be able to crash or
  fill the process; the limits below are what makes that true. Every record from a peer is untrusted input to the parser.
* **Console.** A server-side process that holds the admin token and the S3 keys. The browser never receives either and
  never talks to a log-less listener directly, which is why no listener sends CORS headers and why a browser reaching one
  (DNS rebinding, a cross-site form post) is treated as an attack, not a client.
* **Secrets store.** Today: the config file (tokens and keys in clear), environment variables, and `sync.json` plus
  `sync-token` in the data dir. There is no separate secrets service. Anyone who can read those files has the secrets.

Out of scope here: the OTLP, Splunk HEC and Sentry listeners (see "Not defended or not done"), TLS (there is none; run a
proxy), the CLI and file tailing, the console's own authentication.

## Common machinery

The admin API, the peer receiver and the S3 server run on `bounded_http.rs`, a small HTTP/1.1 server. It exists because
`tiny_http` (0.12, used by the other listeners) reads each header line into an unbounded buffer, sets no read timeout, and
runs one thread per connection with no cap (read in `tiny_http-0.12.0/src/client.rs`, `read_next_line`, and demonstrated
against the S3 server before this branch: a 64 MiB header was buffered and answered 200).

| Abuse case | Defence | Test |
|---|---|---|
| Header line, request line or header count past the budget (16 KiB, 64 headers) | 431 before any handler or authentication | `bounded_http::tests::the_head_is_capped_by_bytes_by_count_and_by_a_missing_terminator` |
| Head or body that never finishes (slowloris) | whole-request deadline, 408 | `bounded_http::tests::a_client_that_stalls_in_the_head_or_the_body_is_cut_off_at_the_request_timeout` (300 ms deadline; also a one-byte-at-a-time drip, which does not extend it) |
| Many connections | cap, extra connections closed unread; slots return | `bounded_http::tests::connections_past_the_cap_are_closed_unread_and_the_slots_come_back` |
| Malformed request line or header, folded headers, bad or repeated `Content-Length`, non-UTF-8 head, HTTP/2 preface | 400, handler never called | `bounded_http::tests::malformed_heads_are_400_and_never_reach_the_handler` |
| Chunked bodies, and `Content-Length` with `Transfer-Encoding` (request smuggling) | 501; no chunk parser exists to disagree with a proxy | same test; `peer::tests::framing_tricks_are_refused_rather_than_guessed_at` |
| Pipelined second request | every reply is `Connection: close`; bytes after the declared body are dropped, never parsed | `peer::tests::framing_tricks_are_refused_rather_than_guessed_at` |
| Refusal that needs only the head (auth, Host) answered after a body is buffered | `Handler::preflight`: answered before the body is read | `bounded_http::tests::a_preflight_refusal_is_sent_without_waiting_for_the_body` |
| Gzip bomb | decoded only where enabled, capped, one byte over is 413 | `bounded_http::tests::gzip_is_decoded_only_when_asked_for_and_only_up_to_the_cap` |
| Client that stops reading a download | 10 s per write, then the connection is dropped | `s3_serve::tests::a_stalled_download_does_not_starve_other_requests` (checks the others are served; the 10 s cut-off itself is not covered) |

Fixed on this branch: the peer receiver and the S3 server were on `tiny_http` and now use this server (see below).

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
| Cross-site POST to the parts endpoints with no `Content-Type` (tokenless, loopback, parts enabled) | **not defended**: the content-type rule is only applied to ingest because `parts.rs` was out of scope and its tests send bodies without a type | not covered |

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

## Not defended or not done

1. **OTLP, Splunk HEC and Sentry listeners are still on `tiny_http`** (`otlp/http.rs`, `hec.rs`, `sentry.rs`). They have the
   defect the S3 and peer listeners had: unbounded header buffering, no read timeout, a thread per connection with no cap,
   and a body-read that ties up one of a few workers. They are loopback by default and unauthenticated by design; on a
   public bind they are exposed to any peer that can reach the port. The fix is the same swap as the peer receiver
   (`BoundedServer::start` with `gzip: true`), but their suites are outside the filter this task ran, so it was not done.
   Treat as open until it is.
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
   10 to 30 s each and so deny that listener (not the OTLP listeners). No per-source cap.
7. **Anonymous loopback mode** (no tokens, no keys) trusts every local process and, for the peer receiver and S3, any web
   page that can name `127.0.0.1` itself. The Host and Content-Type rules close rebinding and blind cross-site writes for
   ingest; they do not close cross-site writes to the parts endpoints (above).
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
