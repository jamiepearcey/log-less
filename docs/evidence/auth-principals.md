# Principals, broker tokens, scoped reads: evidence

Plane item LOG-179, ADR 0004, `docs/auth.md`. Parts A (principals and the verifier) and B (the tenant from the
credential on every write listener) are on `main` at `6460950`; parts C (the read side), D (the documents) and E (the
run against a real session-broker) are on branch `wf/auth-read`, measured here at `621d774` plus this file.

Run on 2026-10-11 (BST), macOS (Darwin 25.1.0), `duckdb v1.5.2`, `curl 8.7.1`. The agent was built
with `cargo build -p logless-cli` and the binary copied to a scratch directory so that another build in the shared
target directory could not swap it mid-run; `$SCRATCH` stands for that directory. session-broker and its `mock-idp`
are the binaries built from its repository at `2e59276` (built 13:32 and 12:42 that day; the merge commit is 13:55,
so the binaries predate the merge commit's timestamp by minutes and were not rebuilt for this run).

No token, key or secret appears below. Key ids (`kid`) are public: they are in the broker's published key set.

## Unit and table tests

```text
$ cargo test -p logless-core
test result: ok. 803 passed; 0 failed; 1 ignored      (unit tests, src/lib.rs)
test result: ok. 3 passed; 0 failed; 0 ignored        (tests/s3_serve_e2e.rs)
test result: ok. 4 passed; 0 failed; 0 ignored        (tests/template_hash_ids.rs)
test result: ok. 3 passed; 0 failed; 0 ignored        (tests/templating.rs)
$ cargo test -p logless-cli
test result: ok. 7 passed; 0 failed; 0 ignored
```

813 passed in `logless-core` (800 on `main`: 790 + 3 + 4 + 3), 7 in `logless-cli` (6 on `main`). The 13 and the 1 new
tests are all of part C. `cargo test -p logless-cli` had not been run on parts A and B; it passes unchanged.

By module, the tests that are about auth:

| Module | Tests | Part | What they cover |
|---|---|---|---|
| `auth::tests` | 16 | A | the broker's own 11 test vectors with verdict and reason; algorithm confusion; signature and claims; the key cache (fetch, disk, rotation, broker down, restart); static keys, hashed keys; config errors; legacy credentials per listener; the permission table; `authorize`; `admit` (tenant, index) |
| `auth::route_tests` | 5 + 2 | B, C | 9 routes x 14 credentials = 126 let-in-or-refused checks (was 8 x 14 = 112: the exchange route is the ninth); tenant from the principal on every ingest route; the index allow-list; node ids; open and closed listeners; **the exchange** (C2); **a user with the role `admin` is not an operator** (C3) |
| `auth::s3` | 4 | C | the scope table (14 rows); a credential's round trip; expiry and tampering; the key file (made once, 0600, survives a restart, replaced if corrupt, in-memory fallback) |
| `s3_serve::tests` | 51 + 7 | C | see below |
| `logless-cli` `tests` | 6 + 1 | C | a line on stdin or in a tailed file cannot name a tenant or an index |

The seven new `s3_serve` tests, each against a real listener on a loopback port with real SigV4 requests:

| Test | Counts |
|---|---|
| `a_reader_reads_and_lists_its_own_tenant_and_indexes_and_nothing_else` | 5 readers (a tenant, one index of it, the own tenant, `tenant = "*"`, the older pair) x 11 objects x GET and HEAD = 110 reads, each 200 with the exact bytes or 403; 5 x 3 page sizes of a whole-bucket listing and 5 x 5 prefixes, each equal to the keys in scope; 5 path spellings of another tenant's key |
| `a_scoped_listing_pages_exactly_across_the_edges_of_the_scope` | a tenant that sorts between two others, listed one key a page: 4 pages, each with one key, `KeyCount` 1, the right `IsTruncated`, no fifth page; two continuation tokens made by hand; 5 delimiter listings; one v1 listing |
| `the_older_key_pair_still_reads_the_whole_store_and_only_a_whole_store_reader_reads_uploaded_parts` | 2 whole-store readers see 11 keys and both buckets; 3 scoped readers see one bucket and get 403 on the parts bucket (2 requests each); 5 wrong id and secret pairs are `SignatureDoesNotMatch`; per-reader `requests` and `refused` in status are exact (3 and 4; 4 and 1); no secret in status |
| `a_reader_must_not_share_an_id_with_the_older_pair_or_look_like_a_temporary_key` | 3 configs |
| `temporary_credentials_read_what_their_token_may_and_stop_when_they_expire` | round trip for a user token and a reader token; 3 keys outside the scope and the parts bucket refused; 4 wrong-secret and mixed-credential cases 403; 4 changed tokens (tenant, indexes, expiry, garbage) `400 InvalidToken`; status counts |
| `an_expired_temporary_credential_is_refused_and_a_restart_keeps_a_live_one` | 200, then `400 ExpiredToken` after its 2 s (GET and listing); a credential issued before a restart reads after it with `issued = 0` in the new process; another instance refuses it |
| `a_listener_that_asks_for_no_credential_still_judges_a_session_token` | open loopback serves unsigned and refuses a bad token; a public bind without anyone who could sign does not start; with a broker it starts closed |

Could not be run: nothing was skipped. Timing of the constant-time comparisons is not measured by any test (as
before). The console (`logless-studio`) was not changed and not run against this branch.

## C3: what was checked and found

* **Admin API and user tokens.** Decision: a broker user token is never an operator, whatever its roles; kind `admin`
  only. That was already what `Kind::may` did; it is now a test (`a_person_with_the_admin_role_is_still_not_an_operator`:
  3 admin routes x a user token with roles `admin` and `owner` are 401; a static admin key and a broker machine token
  of kind `admin` are 200) and a paragraph in `docs/auth.md` and ADR 0004.
* **stdin and `[[tail]]`.** Both produce records only through `parse_line` (`crates/logless-cli/src/main.rs`), which
  creates `request_id` attributes and nothing else; `[[tail]] index` is added from the config. Neither can carry a
  sender-supplied `logless.tenant`, so there was nothing to strip. Pinned by a test over 4 hostile lines. (OTLP gRPC,
  which could, was already stripped by part B.)
* **`cargo test -p logless-cli`.** Passed as it was (6), 7 with the new test.

## The whole stack: `scripts/e2e-stack.sh`

52 checks, 50 on `main`. The five of the two-tenant instance, the last two new:

```text
a wrong-tenant write, a missing key and an unknown key are refused PASS  no key=401 unknown key=403 acme key naming tenant default: hec=403 peer=403
status names the principal, counts refusals by reason, and holds no key PASS  seen='acme/acme-app:7 default/own-app:3' refused missing>=1/unknown_key/wrong_tenant='True 1 2' keys in info or log: 0
a [[principals]] key stores under its own tenant's path PASS  acme key -> 200, own-tenant key (configured hashed) -> 200; rows by tenant and service: acme,e2e-acme,7 default,e2e-own,3 ; files under tenant=acme/index=default/: 1
a reader of one tenant reads that tenant only PASS  acme-ro over s3://logs/**: 'acme:7'; its dictionary read: 1; a file of tenant default: 403 Forbidden); a glob of tenant default: No files found
the store's own key pair still reads every tenant PASS  operator over s3://logs/**: 'acme:7 default:3'; the reader's id with the operator's secret: 403 Forbidden); reader keys in a log or info: 0

elapsed 54 s; 52 passed, 0 failed, 0 skipped
E2E PASS
```

`scripts/restore-drill.sh`: `30 passed, 0 failed` (30 on `main`). `scripts/parts-replicate-drill.sh`:
`33 passed, 0 failed` (33 on `main`).

## Against a real session-broker: `scripts/auth-broker-e2e.sh`

The script starts `mock-idp`, `session-broker` (token issuer on, audience `logless`) and one log-less with tenants
`default` and `acme`, all on free loopback ports, each under a 300 s kernel alarm. `[auth] issuer` points at the
broker; temporary S3 credentials are set to live 25 s so their expiry is seen inside the run. Complete output:

```text
$ LOGLESS_BIN=$SCRATCH/logless scripts/auth-broker-e2e.sh
[17:21:59] session-broker on :59989 (admin :59990), mock-idp on :59988
[17:21:59] log-less: hec :59991, peer :59992, s3 :59994, admin :59993
[17:22:06] waiting 55s: an unknown key id is asked for at most once a minute

CHECK                                                                          RESULT DETAIL
1. a machine token ingests as its principal                                    PASS  token (tenant acme, kind ingest, kid zASX7HcoCiQ) -> 200; static key -> 200; status seen: acme/edge-1:ingest:token:612 default/own-app:ingest:static:605
2. a token for a tenant the instance lacks is refused                          PASS  token of tenant globex: ingest -> 403, s3-credentials -> 403; status refused.unknown_tenant = 2
3a. a user token is traded for S3 credentials; DuckDB reads its tenant only    PASS  a user token (kind user, sub user-1, tenant acme) -> 200 (tenant, indexes, key id prefix, seconds to live: acme ['*'] LLTMP 25); s3://logs/**: 'acme:true'; a file of tenant acme: rows:true; a file of tenant default: 403 Forbidden)
3b. a reader machine token is traded too; an ingest token is not               PASS  reader token -> 200, reads 'acme:true'; ingest token -> 403; the store's own key pair reads 'acme:true default:true'
4. a token signed by the rotated key verifies                                  PASS  broker rotated zASX7HcoCiQ -> 7CfwLIZa8S0; log-less knew [zASX7HcoCiQ], took the new token -> 200, now knows [7CfwLIZa8S0]
3c. an expired S3 credential is refused                                        PASS  the same credentials 25s on: HTTP 400 Bad Request); status refused.expired = 1
5a. with the broker stopped a cached-key token and a static key still ingest   PASS  broker answers 000; token -> 200, static key -> 200; status key_set_age_secs and key ids: 2 7CfwLIZa8S0
5b. a restart with the broker down still verifies from the key cache           PASS  S3 credentials issued before the restart (broker down: 200) read 'acme:true' after it; token -> 200; key ids, a refresh failed, s3 key persisted: 7CfwLIZa8S0 True True; s3-session.key mode 600
1b. every row is under its credential's tenant                                 PASS  rows and services by tenant: acme,625,5 default,607,3 (want acme 625, default 607); rows of the refused tenant: 0
no token, key or secret in a log line or in status                             PASS  13 tokens, keys and secrets and the s3 session key looked for in agent.log, merge.log and two status documents: 0 found

elapsed 70 s; 10 passed, 0 failed
AUTH E2E PASS
```

What each line shows:

| Step | Result |
|---|---|
| 1. Machine token, tenant `acme` | `POST /admin/principals` then `POST /auth/token` at the broker; HEC ingest with the token is 200; status names `acme/edge-1` as `ingest` from a `token` with 612 records. After the run the store holds 625 rows under `tenant=acme/` and 607 under `tenant=default/`, exactly what each credential sent (1b) |
| 2. A tenant the instance lacks | a token of tenant `globex` (which the broker has) is 403 on ingest and on the exchange; `refused.unknown_tenant` = 2; no `tenant=globex/` directory, no row |
| 3. A person reads | the sign-in **was scripted with curl** (`GET /auth/login` following the mock provider's redirects with a cookie jar, then `POST /session/token` with the `Origin` header); the user token (kind `user`, sub `user-1`, tenant `acme` from a `POST /admin/user-grants`) is traded at `POST /v1/auth/s3-credentials`; DuckDB with `KEY_ID`, `SECRET`, `SESSION_TOKEN` and a path-style endpoint globs `s3://logs/**/*.parquet` and gets acme only, reads a file of acme, and is refused a file of tenant `default` (403). A reader machine token is traded the same way; an ingest token is not (403). 25 s later the same credentials are `400` and `refused.expired` = 1 |
| 4. Rotation | `POST /admin/signing-keys/rotate` with `activate_in_secs: 0`; log-less knew only the old key id, took a token signed by the new key (200) and then knew the new one: the unknown-kid refresh |
| 5. Broker stopped | the broker's port answers nothing (`000`); a token signed by the cached key is 200 and a static key is 200; status shows `key_set_age_secs`. log-less is then restarted with the broker still down: S3 credentials issued before the restart read after it, the token is still 200, status shows the refresh failed and the key set came from the cache, and `s3-session.key` is mode 600 |
| Secrets | 13 tokens, keys and secrets, and the session key itself, were searched for in the agent log, the merge log and two status documents: 0 found |

The run waits about 55 s on purpose: log-less asks the broker for an unknown key id at most once a minute, and it had
asked at start. Without the broker binaries the script prints `SKIP` and exits 0.

## Not covered

* The console: it still uses the operator key pair. What it must change is listed in the LOG-179 report.
* TLS, a broker on another host, a real identity provider (the mock has no login page), more than one central sharing
  a session key, clock skew between log-less and the broker beyond the unit-tested 300 s allowance.
* A normal (non-emergency) rotation, where the new key is published a day before it signs: not run. The unit test
  `keys_are_fetched_cached_on_disk_and_kept_while_the_issuer_is_unreachable` covers a rotation that keeps the old key
  published; the hourly refresh thread itself is not exercised by any test.
* Load: the cost of two SigV4 verifications and one mutex per S3 request was not measured.
