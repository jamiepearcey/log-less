# Auth: who may talk to a log-less instance

Every listener asks one component, the verifier (`crates/logless-core/src/auth.rs`), who a request is from. The
answer is a **principal**: an id, a kind, a tenant and optionally a list of indexes. The tenant a record is stored
under, and the part of the store a reader may read, always come from the principal and never from the request.

There are four ways to hold a credential, from the simplest up. Each one keeps working when you add the next; nothing
here ever requires the one after it. The decisions and what was left out on purpose are in
[ADR 0004](adr/0004-auth-principals-and-broker-tokens.md); measured runs are in
[evidence/auth-principals.md](evidence/auth-principals.md).

## 1. Paste a key

This is how every listener has always worked, and it is not deprecated.

```toml
[peer]
tokens = ["<openssl rand -hex 24>"]          # senders:  Authorization: Bearer <token>

[hec]
tokens = ["<token>"]                         # Splunk HEC:  Authorization: Splunk <token>

[sentry]
allowed_keys = ["<the DSN's public key>"]

[admin]
token = "<token>"                            # or LOGLESS_ADMIN_TOKEN

[serve_s3]
access_key = "<id>"                          # or LOGLESS_S3_ACCESS_KEY
secret_key = "<secret>"                      # or LOGLESS_S3_SECRET_KEY
```

A key does not expire, is not rotated for you, and needs no network call to check. A listener bound to loopback with
no key configured takes requests without one, as before; on any other address it refuses to start without one.

Each of these settings is read as a principal of the instance's own tenant (`default` unless `[[tenants]]` says
otherwise), good at its own listener only:

| Setting | Principal id | Kind | Tenant | Good at |
|---|---|---|---|---|
| `[peer] tokens` | `peer-token-1`, `-2`, ... | `ingest` | own | peer |
| `[[peer.nodes]] {id, token}` | the node id | `node` | own | peer |
| `[peer.parts] replication_tokens` | `replica-token-1`, ... | `replica` | own | peer |
| `[hec] tokens` | `hec-token-1`, ... | `ingest` | own | HEC |
| `[sentry] allowed_keys` | `sentry-key-1`, ... | `ingest` | own | Sentry |
| `LOGLESS_ADMIN_TOKEN` / `[admin] token` | `admin-token-1` | `admin` | own | admin API |
| `[serve_s3] access_key` + `secret_key` (or `LOGLESS_S3_*`) | the access key | `reader` | `*`, the whole store | S3 |

A Sentry key is public by design, which is why it opens the Sentry proxy and nothing else; the same holds for each
row.

## 2. `[[principals]]`: a key with a name, a tenant and a kind

Use this when one instance holds more than one tenant, or when you want status to say who sent what.

```toml
[[tenants]]
id = "acme"
[[tenants.indexes]]
name = "payments"

[[principals]]
id = "acme-app"            # the name in status and logs; for kind "node" the node id, for "reader" the S3 access key id
kind = "ingest"            # ingest | node | replica | reader | admin
tenant = "acme"            # left out: the instance's own tenant
indexes = ["payments"]     # left out: every index of the tenant
key = "<openssl rand -hex 24>"          # at least 24 characters
# key_sha256 = "<64 hex digits>"        # instead of key: printf %s "$KEY" | shasum -a 256. Not for a reader.
```

The same key is presented exactly as in section 1 (`Bearer`, `Splunk`, a DSN key), at any listener whose routes its
kind may use. A key for a tenant with no `[[tenants]]` entry is a startup error: tenants are not created on first use.

What a kind may do is the whole permission model (`Kind::may`):

| Kind | Post records | Parts (own node id) | Replica read | Admin API | Read over S3 | Trade a token for S3 credentials |
|---|---|---|---|---|---|---|
| `ingest` | yes | | | | | |
| `node` | yes | yes | | | | |
| `replica` | | | yes | | | |
| `admin` | | | | yes | | |
| `reader` | | | | | yes (static key) | yes (broker token) |
| `user` (broker only) | | | | | | yes |

Scopes and roles carried by a token are shown in status and not interpreted: there is no RBAC in the agent. In
particular **a person whose broker token carries the role `admin` is not an operator of this instance**. The admin API
opens for kind `admin` and nothing else (a static admin key, or a broker machine token of kind `admin`).

### Writing

Every record a principal sends is stored under `tenant=<its tenant>/`. A record that names another tenant (the
`logless.tenant` attribute, or a HEC field of that name) turns the whole batch away with 403. An index the principal
may not write follows the tenant's `unknown_index` rule (to its default index, or refused), and is counted.

Sources that take no credential belong to the instance's own tenant: a request without a key on a loopback listener,
OTLP gRPC, stdin and `[[tail]]` files. Lines read from stdin or a tailed file cannot carry `logless.tenant` at all:
both go through one parser that makes only a `request_id` attribute, and a `[[tail]] index` is set from the config.

### Reading: a reader is scoped to its tenant and indexes

A `kind = "reader"` principal signs S3 requests (SigV4) with its id as the access key id and its key as the secret.
It needs `key`, not `key_sha256`: request signing uses the key itself.

```toml
[[principals]]
id = "acme-ro"
kind = "reader"
tenant = "acme"            # "*" is the whole store: a second operator credential
indexes = ["payments"]     # left out: every index of acme
key = "<secret>"
```

* It may read `tenant=acme/index=payments/**`, including that index's `_meta/` (its template dictionary and shape
  history; dictionaries are per tenant and index, so there is nothing shared to leak).
* An object outside the scope is `403 AccessDenied`, decided on the decoded key before the disk is touched, so a
  reader cannot tell whether a key outside its scope exists.
* A listing above the scope (`s3://logs/**/*.parquet`, the console's globs) answers with what the scope allows and
  nothing else. The filter runs inside the directory walk, so pages are full, `IsTruncated` is right and a
  continuation token resumes exactly; a token made by hand to point into another tenant resumes at the reader's own
  keys or past them.
* The uploaded-parts bucket is kept by node, not by tenant: only a whole-store reader sees or reads it.
* The `[serve_s3]` pair stays what it was: one credential for the whole store, the operator's own.
* A reader id may not equal the `[serve_s3]` access key or start with `LLTMP` (kept for temporary credentials).

```sql
CREATE SECRET (TYPE s3, KEY_ID 'acme-ro', SECRET '<secret>', ENDPOINT 'central:9000', URL_STYLE 'path', USE_SSL false);
SELECT count(*) FROM read_parquet('s3://logs/**/*.parquet', hive_partitioning = true);   -- acme's payments rows
```

## 3. Broker tokens: `[auth]`

For installs that already run [session-broker](https://github.com/jamiepearcey/session-broker): machines get short
tokens instead of long keys, and people sign in. It is one setting, and static keys keep working beside it.

```toml
[auth]
issuer = "https://app.example.com"     # the broker's token_issuer, compared exactly with a token's `iss`
# audience = "logless"                 # the default
# jwks_url = "<issuer>/auth/jwks.json" # the default
# jwks_file = "/etc/logless/jwks.json" # a key set on disk for an install that cannot reach the broker; then the only source
# clock_skew_secs = 300
# s3_credentials_ttl_secs = 3600       # section 4; 1 to 43200
```

A token is presented wherever a key is (`Bearer`, `Splunk`). It is an EdDSA JWT; log-less checks it offline, in the
order of the broker's verifier guide: algorithm, key id, signature, then `iss`, `aud`, `exp`, `iat`, and that the
`tenant` claim is a tenant this instance has. The principal is the token's `kind`, `tenant`, `indexes` and `name`
(for a node, its node id). A static key is always tried first: one hash and one map lookup.

**Nothing calls the broker per request.** The key set is fetched at start, hourly, and once when a token carries a key
id log-less does not know (at most once a minute). It is kept in `<data_dir>/jwks-cache.json`.

### When the broker is unreachable

| What | Happens |
|---|---|
| Static keys (sections 1 and 2) | Unaffected. They never involve the broker. |
| Tokens signed by a key log-less has cached | Keep verifying, with no time limit, until their own `exp`. |
| A restart while the broker is down | Starts from `jwks-cache.json`; the same tokens still verify. |
| Temporary S3 credentials already issued (section 4) | Keep working until they expire; they do not involve the broker at all. |
| A token signed by a key log-less has never seen | Refused (`unknown_kid`) until the broker answers again. |
| Getting a **new** token | Not possible: that is the broker's job. Senders that must survive a long outage keep a static key. |

`GET /admin/v1/info` reports it under `auth.broker`: `key_ids`, `key_set_age_secs` (the number to alarm on),
`refresh_failures`, `last_refresh_error`. A failed refresh never drops a key.

An emergency rotation at the broker (`activate_in_secs: 0`) stops publishing the old key. log-less learns the new one
on the first token that carries it, and from then on tokens signed by the old key are refused, as the broker intends.

## 4. Temporary S3 credentials for people and services

DuckDB and the console read the store over S3, which signs with a key pair, not a bearer token. So a token is traded
for a short-lived key pair:

```sh
curl -s -X POST -H "Authorization: Bearer $BROKER_TOKEN" https://central.example.com:9300/v1/auth/s3-credentials
# {"access_key_id":"LLTMP…","secret_access_key":"…","session_token":"v1.….…",
#  "expires_at":"2026-10-11T17:00:00Z","expires_unix_secs":1791738000,"tenant":"acme","indexes":["*"]}
```

```sql
CREATE SECRET (TYPE s3, KEY_ID 'LLTMP…', SECRET '…', SESSION_TOKEN 'v1.….…',
               ENDPOINT 'central:9000', URL_STYLE 'path', USE_SSL false);
```

* The route is on the peer listener (`[peer] enabled = true`) and always asks for a token, also on loopback.
* It takes a broker token of kind `user` (a person: `POST /session/token` at the broker) or `reader` (a service:
  `POST /auth/token`). Any other kind, and any static key, is 403.
* The credentials read what the token's `tenant` and `indexes` allow, exactly as a section 2 reader would. The request
  names nothing: a body, a query or a header asking for another tenant changes nothing.
* They expire after `s3_credentials_ttl_secs` (an hour by default) or when the token does, whichever is sooner. Ask
  again before then. An expired credential is `400 ExpiredToken`, a changed one `400 InvalidToken`, as on AWS.
* **Nothing is stored.** The session token carries the scope and the expiry and is authenticated with an HMAC under a
  key the instance generates once (`<data_dir>/s3-session.key`, mode 0600); the access key id and the secret are
  derived from it. The S3 server checks the token and the SigV4 signature (header-signed requests; presigned URLs are
  not accepted for reads, as before) with no table of sessions. A restart keeps issued credentials valid. Deleting
  that file ends every credential issued so far; that is the only way to end one early.
* If the data dir cannot hold the key the instance still starts, with the key in memory, and says so in the log and in
  status (`auth.s3_credentials.key_persisted = false`): credentials then end at the next restart.
* Centrals behind one name must share the file (copy it), or a credential issued by one is refused by the other.

## What status shows

`GET /admin/v1/info`, under `auth`. Ids and counts; never a key, a token or a secret.

* `principals`: every configured principal with its kind, tenant, indexes and, for a reader, the prefixes it reads.
* `seen`: per principal (`tenant/id`): `kind`, `source` (`static`, `token`, `s3-session`, `anonymous`), `requests`,
  `records`, `index_defaulted`, `index_refused`, `refused`, `last_seen_unix_secs`. For a reader `requests` is S3
  requests let in and `refused` is bad signatures plus reads outside its scope.
* `refused`: totals by reason: `missing`, `unknown_key`, `malformed`, `alg`, `unknown_kid`, `signature`, `claims`,
  `issuer`, `audience`, `expired`, `not_yet_valid`, `unknown_tenant`, `kind`, `node_mismatch`, `wrong_tenant`, `scope`.
  The HTTP answer names none of them (401 without a credential, 403 with one); the log says the reason at most once a
  minute.
* `broker`: the key cache, as above. `s3_credentials`: `ttl_secs`, `issued`, `key_persisted`.

## What this does not do

Introspection, revocation lists, required TLS or mTLS, per-request online checks, forced rotation, RBAC in the agent:
see the "deliberately not built" list in ADR 0004. There is no TLS in log-less itself; off loopback, put a proxy in
front, or a captured bearer key is good until you change it and a captured token until it expires.
