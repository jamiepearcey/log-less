# ADR 0004: Principals, one verifier, offline broker tokens, and scoped reads

Status: accepted, 2026-10-11 (Plane item LOG-179). Builds on the tenant and index layout
(`docs/design/multi-tenancy-and-indexing.md`, section 7) and on session-broker's ADR-0017 (the broker as a token
issuer) and its verifier guide. How to use it: `docs/auth.md`. Measured runs: `docs/evidence/auth-principals.md`.

## Context

Each listener had its own credential and its own comparison: peer tokens, node tokens, replication tokens, HEC tokens,
Sentry keys, the admin token, one S3 key pair. None of them named a tenant, so a multi-tenant central could not say
whose records a request carried, and the one S3 pair read the whole store. Separately, session-broker can now sign
tokens for machines and people.

The owner's steer, which bounds everything below: be pragmatic and match what the market ships (Sentry, Datadog,
Splunk, Grafana and Elastic all hand out long-lived keys); static keys are a permanent first-class option; nothing here
may take ingest or reads down.

## Decision

1. **One principal, one verifier.** A credential resolves to a `Principal { id, kind, tenant, indexes }` in one place
   (`auth::Verifier`), shared by every listener. Listeners no longer compare tokens themselves.
2. **Kind is the whole permission model.** `Kind::may(Op)` is one table of six kinds (`ingest`, `node`, `replica`,
   `reader`, `admin`, `user`) against six operations. Scopes and roles from a token are carried and shown, never
   interpreted.
3. **Static keys stay, permanently.** Every credential an install already had is read as a principal of the
   instance's own tenant, good at its own listener, with no config change. `[[principals]]` adds named keys bound to a
   tenant, optionally to indexes, optionally configured as a SHA-256 digest. A static key is looked up by digest
   before anything else is tried, never expires, and involves no network.
4. **The tenant comes from the principal.** On the write side every record is stored under the principal's tenant; a
   record naming another one refuses the batch. Tenants are configured, never created by a credential.
5. **Broker tokens are optional and checked offline.** `[auth] issuer` turns them on. EdDSA only, the checks in the
   broker guide's order, against a key set fetched at start, hourly and once per unknown key id (at most once a
   minute), cached in the data dir and used without a time limit while the broker is unreachable. Left unset, there is
   no thread and no network call.
6. **Reads are scoped by prefix.** A reader's scope is the list of key prefixes `tenant=<t>/` or
   `tenant=<t>/index=<i>/`. The S3 server refuses an object outside it and filters listings inside the directory
   walk, so a client that globs the whole bucket keeps working and pagination stays exact. The one older S3 pair
   remains a whole-store reader: it is the operator's own credential and existing installs must not change behaviour.
   `tenant = "*"` on a reader principal gives a second such credential.
7. **People and services read through temporary S3 credentials, and they are stateless.**
   `POST /v1/auth/s3-credentials` on the peer listener trades a broker token of kind `user` or `reader` for an access
   key id, a secret and a session token, scoped to the token's tenant and indexes and expiring no later than the
   token. The session token carries scope and expiry under an HMAC whose key the instance generates once and keeps in
   the data dir; the secret is derived from the token. The S3 server needs the request and that key to decide. There
   is no session table, so there is nothing to replicate, nothing to lose in a restart and nothing to look up.
   S3 clients (DuckDB, the AWS SDKs) already speak this: it is the shape of AWS STS.
8. **The exchange is on the peer listener,** not the admin one. The admin API is the operator's, has a loopback Host
   rule and is often not reachable from where a console's users are; the peer listener is the instance's public API
   and already authenticates bearers.
9. **A user is never an operator.** The admin API opens for kind `admin` only: the static admin token, an `admin`
   `[[principals]]` key, or a broker machine token of kind `admin`. A user token whose `roles` include `admin` is
   refused there. Roles are the console's to interpret; letting a role claim open retention and sync changes would
   make the broker's grant table an operator list for every log-less it fronts, which nobody asked for.
10. **Refusals are counted, not explained.** The HTTP answer is 401 without a credential and 403 with one (S3 answers
    in S3's codes). The reason is a counter in status and a log line at most once a minute per reason. No secret, key
    or token reaches a log line or a status field; principals are named by id.

## The pragmatic scope

What an operator has to do, by install:

* **One box, one tenant:** nothing. The existing keys work.
* **Several tenants:** `[[tenants]]` and one `[[principals]]` entry per sender and per reader.
* **A broker already in place:** one `[auth] issuer` line; mint tokens there. Keep static keys for anything that must
  ingest through a broker outage longer than a token's life.

Failure behaviour was chosen for availability over freshness at every point: a stale key cache keeps verifying; a
failed refresh drops nothing; a key file that cannot be written falls back to memory; a token for an unknown tenant is
refused rather than creating one.

## Deliberately not built

* **Token introspection.** No call to the broker to ask whether a token is still good. The broker offers none
  (ADR-0017) and a per-request dependency would take ingest down with the broker.
* **Revocation lists.** An issued token, and an issued S3 credential, is good until it expires. Revoking at the broker
  stops the next token. To cut one off early: shorten lifetimes; for S3 credentials, delete `s3-session.key` (ends
  all of them); for a static key, remove it and restart.
* **Required TLS or mTLS.** log-less serves plain HTTP; TLS is a proxy's job, as for every other listener here.
  Nothing refuses to work without it.
* **Per-request online checks** of any kind (broker, an IdP, a policy service).
* **Forced rotation or expiry of static keys.** A key lasts until the operator changes it.
* **RBAC in the agent.** No roles, no per-route grants, no per-tenant admin. `kind` decides.
* **Scope enforcement beyond kind.** A machine token's `scopes` and a user token's `roles` are not checked against the
  route. (Tenant and index, which are not scopes, are enforced.)
* **Per-tenant operators.** The admin API is all or nothing.
* **Narrowing at the exchange.** The S3 credentials are for everything the token allows; a request cannot ask for one
  index of several.
* **Presigned reads.** The S3 server takes header-signed requests only, with or without a session token.
* **Hot reload of `[[principals]]`.** A changed key takes a restart.
* **Rate limiting or lockout on failed authentication.** Unchanged from before (`docs/threat-model.md`).
* **Replication of the session key between centrals.** One file to copy when two centrals answer to one name.

## Consequences

* Existing configs load and behave as before; the legacy settings are documented as mappings, not as deprecated.
* A leaked static key is good until changed, and a leaked temporary S3 credential for up to
  `s3_credentials_ttl_secs` (an hour by default, twelve at most). That is the market's trade and the owner's choice.
* `s3-session.key` is a new secret at rest in the data dir (mode 0600). With it and nothing else, S3 credentials for
  any tenant can be made. It needs the protection the data dir already needs: the store it guards is in the same
  directory.
* The console has to sign users in through the broker, call the exchange, pass `SESSION_TOKEN` to DuckDB and refresh
  before expiry. Until it does, it keeps using the operator pair, which still works.
* No new crate entered the tree: `ring` (Ed25519) was already there through `ureq`'s rustls and became a direct
  dependency; the HMAC and SHA-256 of the session tokens use `hmac` and `sha2`, which SigV4 already used.
