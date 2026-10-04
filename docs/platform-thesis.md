# Platform thesis — log-less + RAE as a Splunk/Sentry competitor

Exploratory. Nothing here is locked. Companion to `.context/project-brief.md` (which
currently says "being anyone's system of record" is a **non-goal**) and
`docs/architecture.md` (which currently says **no embedded query engine**). This document
argues for changing both, and states what that costs.

Sibling repos referenced throughout:

- `../../rust-analytical-engine` (RAE) — columnar engine
- `../../sql-studio` — Tauri SQL IDE, `sqls-source` Arrow trait, native `cube-core-rs`
- `../../analytics/CubeCanvas` — OLAP cube engine + grid
- `../../__externals/zero` — Rocicorp Zero, IVM + sync engine

---

## 1. What the incumbents actually are

Feature lists are long; the load-bearing parts are short. Competing means answering the
load-bearing parts and consciously skipping the rest.

### Splunk

| Layer | What it really is | Must we match it? |
|---|---|---|
| Forwarders (UF/HF), HEC, syslog, file inputs | Getting bytes in | **Already done.** HEC in/out, OTLP HTTP+gRPC, file tail. |
| Index-time: sourcetype, line-breaking, timestamp extraction, SEDCMD masking, routing, cloning | Normalisation before storage | Partly done (masking, normalisation to OTel model). Routing/cloning is Cribl territory — table stakes only. |
| **tsidx** — per-bucket inverted index over tokens, + bloom filters | The reason a needle search over TBs returns | **Must answer, not copy.** See §3: template postings replace token postings. |
| Hot/warm/cold/frozen buckets, SmartStore (S3 cold) | Tiered storage | We go further — the edge *is* a tier. §4. |
| **SPL** | Schema-on-read + a pipeline language | Must answer. **Do not build SPL.** §6. |
| Accelerated data models / `tstats` / summary indexes | Pre-aggregation so dashboards aren't full scans | Must answer. Our version is cheaper — the spine *is* the summary. §3. |
| Lookups (CSV/KV/geo), CIM normalisation | Joining logs to context | Needed. Small hash side — RAE's weak join scaling doesn't bite here. |
| Scheduled + real-time alerts, throttling, alert actions | Where operational value lives | Needed, and we already have a class of alert they don't: novelty/rate on unshipped data. |
| Dashboards, Data Models, Pivot | Consumption | Needed, unglamorous. |
| Indexer clustering (RF/SF), search head clustering, deployment server | Distributed ops | **Skip the design entirely.** Stateless workers over object store instead. §4. |
| RBAC/SSO/audit, index-as-permission-boundary, multi-tenancy | Enterprise gate | Non-negotiable for the Splunk buyer. |
| Apps/TAs, Enterprise Security, ITSI, MLTK | Ecosystem and the actual $500k line items | Out of scope. This is the moat we do not attack. |
| Per-GB/day licence meter | The business model | The thing we're attacking. |

### Sentry

| Layer | What it really is | Must we match it? |
|---|---|---|
| ~30 SDKs, envelope API, DSN | Distribution | **Already free to us.** We are envelope-compatible as a proxy. §5. |
| **Grouping / fingerprint** | The product's atomic unit — an *issue*, not an event | **This is the whole thesis.** §2. |
| Issue lifecycle: assign, resolve, regress, snooze, resolved-in-release | Where stickiness lives — it's a workflow tool wearing a database | **Must build.** Not optional, not infra. |
| Breadcrumbs, tags, contexts, attachments | Context around the error | We have a strictly better version (real preceding DEBUG, not app-emitted crumbs). |
| Stack traces, source maps, dSYM/ProGuard/WASM symbolication, source context | Why frontend/mobile teams pay | **Large, real, and skippable if we start backend-first.** Deferring this picks our beachhead for us. |
| Releases, commits, suspect commits, CODEOWNERS auto-assign | Routing an issue to a human | Must build. Cheap relative to value. |
| Alerts + integrations (Slack/PagerDuty/Jira/GitHub) | Delivery | Must build. Commodity. |
| Tracing, spans, p95, auto-detected performance issues (N+1, slow queries) | The expansion revenue | v2. The OTel data model decision already makes it additive. |
| Session Replay, Profiling, Crons, Uptime | Expansion surface | Skip. |
| Dynamic sampling, spike protection, quotas, **Relay** (their edge proxy) | Cost control | Relay is the shape we already are — except theirs drops, ours keeps. |
| Discover / dashboards | Consumption | Needed. |
| Seer / autofix | AI layer | Later, and template ids make it cheaper than for them. |

**The observation that matters:** Sentry groups only errors, because only errors carry a
stack trace to group by. Splunk groups nothing by default — `cluster` and patterns are a
search command bolted on top of a token index. Neither has grouping as the *storage*
primitive.

We do. Drain gives a stable id for **every line**, not just exceptions.

---

## 2. The primitive: one template id serves both products

```
Sentry issue        = template of an exception
Splunk log pattern  = template of a log line
log-less template_id = both, computed once, at the edge, before anything is shipped
```

We already set Sentry `fingerprint = [template_id]`. The unrealised move is to make that
id the storage, indexing, sampling, alerting **and** UI primitive rather than a forwarding
detail.

That yields a product neither incumbent has: **an issue tracker over the whole log stream.**
Every distinct shape your fleet emits is an object with a lifecycle — first seen, count,
rate, owner, assignee, resolved-in-release, novelty. Errors are just the subset with a
stack trace.

Concretely it collapses four separate incumbent features into one:

| Incumbent feature | Our version |
|---|---|
| Sentry issue grouping | template |
| Splunk `cluster` / Datadog Log Patterns | template (the same one) |
| Splunk accelerated data model / summary index | per-(template, service, minute) rollup |
| Sampling policy / ingest rules | ship by template coverage, not by severity |

**Known unsolved problem, stated up front:** template stability across deploys. A code
change to a log line mints a new `template_id`, so every release floods novelty alerts.
Sentry has the same problem and only half-solves it (fingerprint rules + resolved-in-
release). We need template *lineage* — link a new template to its ancestor by edit
distance over the non-variable token positions, scoped to a release boundary — before
novelty alerting is trustworthy in a fleet that deploys daily. This is a genuine research
task, not a config knob, and it gates headline #3 from the brief.

---

## 3. The spine: index shapes centrally, keep instances at the edge

The user's instinct — *best stuff straight to server, noise at the edge, synced on demand* —
is right, and gets sharper with one substitution. "Best stuff" should not mean *high
severity*. Selecting by severity is what everyone already does, and it is what makes the
2 a.m. query fail.

Select by **shape coverage** instead:

> The central tier holds **every distinct thing that happened, with exact counts and at
> least one exemplar**. The edge and object store hold **the instances**.

Central payload per (template_id, service, minute):

- template id, text, parameter schema
- **exact count** — never sampled, or "did this happen?" starts lying
- first/last seen, one-to-few exemplars (first, last, most-divergent parameters)
- param sketches: top-k values, HLL cardinality (all already specified in `architecture.md` §5)
- every error in full (existing design commitment, unchanged)
- a **locator**: which node / object-store prefix / part holds the instances

Why this is the right cut:

1. **Nearly every triage query is answerable from it, in milliseconds.** "Did X happen?"
   "When did it start?" "How often?" "Is this new?" "Which service?" "Is the rate
   abnormal?" — all shape questions.
2. **The remaining queries are keyed, not scans.** "Show me the actual rows" arrives with
   `(template_id, service, time window)` already chosen from the spine. Hydration is a
   narrow keyed fetch, not a needle-in-TBs search. **This is what replaces tsidx** — we
   don't need a token inverted index because the user never issues an unqualified needle
   search; they navigate shapes first. That is a UX commitment as much as a storage one.
3. **It sizes like a normal database.** Measured forwarding ratios today are 3.4% of raw
   bytes (Sentry shape) and 0.17% (Splunk aggregate shape). A spine in that band turns a
   10 TB/day fleet into 20–300 GB/day centrally. That is one machine, not an indexer
   cluster — and it is the entire margin story.

**Rejected: ship a fixed percentage sample.** Loses rare shapes, which are the ones worth
paging on, and makes counts approximate.
**Rejected: ship WARN+ only.** That is the exact behaviour the product exists to stop
teams from being forced into.
**Rejected: build a token inverted index centrally.** It is the expensive half of Splunk,
and it exists to serve unqualified free-text search — which the shape-first UI removes the
need for. If users demand raw substring search anyway, it belongs at hydration time over a
narrowed set, not as a global index.

---

## 4. Tiers, and the honest problem with "sync on demand"

```
apps ──▶ log-less edge agent (unchanged surface: OTLP / HEC / Sentry envelope / files)
           │  WAL → Drain → Parquet parts, all data, full verbosity
           │
           ├─ SPINE PATH   always on, ~0.2–3% of bytes
           │     template deltas · exact counts · exemplars · all errors · part locators
           │            └──▶ central store  (RAE)              latency: ms
           │
           ├─ COLD PATH    bulk, async, cheap, no vendor tax
           │     completed Parquet parts ──▶ object store, node-owned prefix
           │     catalog rows ──▶ spine, so central knows what exists without holding it
           │                                                    latency: seconds–minutes
           │
           └─ EDGE PUSH-DOWN   optional, live tail only
                 coordinator sends a filtered scan; node answers a partial aggregate
                                                                latency: sub-second, last N min
```

**The trap, named:** the edge is the differentiator *and* the liability. Risk #4 in the
brief is already correct — in k8s, on spot, or behind an autoscaler, nodes are cattle. Data
that lives only on a node that no longer exists is gone precisely during the incident we
were bought for. That is a trust-ending failure, once.

So invert the default: **the object store is the on-demand tier; the node is a cache in
front of it.** The node flushes completed parts continuously and cheaply; edge push-down is
an optimisation for the live window, not the durability story. "Synced on demand" describes
the *query*, not the *data's only home*.

The cost of that inversion is honest and should be stated to buyers: full fidelity now
costs object-store bytes (~$0.02/GB/mo) instead of nothing. Against $0.10–$150/GB it is
still the same argument, and it converts "your data might be on a dead node" into "your
data is in your bucket."

**Rejected: Splunk-style indexer clustering (RF/SF, search head clustering).** It is
years of work and the reason Splunk needs a professional-services arm. Object store +
stateless workers gives durability, elasticity and multi-writer for free, and matches what
SmartStore and every post-2015 warehouse converged on. It also preserves single-binary
ops at the edge.

**Rejected: central holds everything (be a normal cheap backend).** That is OpenObserve /
Quickwit / SigNoz. It abandons the only structural cost advantage we have and puts us in a
storage-price race against ClickHouse Cloud.

**Latency variance is a UX problem, not just a systems one.** A query answered from the
spine in 200 ms and the same query one hour older taking 40 s reads as *broken*, not as
*tiered*. The planner must surface the tier before running: "this needs cold hydration —
~12 GB, ~30 s. [Run]". Datadog Rehydration and Splunk's accelerated-vs-raw split both make
this visible; hiding it is worse than the wait.

---

## 5. Compatibility is the distribution strategy

The single highest-leverage decision already taken, worth restating because everything
else should be judged against it:

**We do not write SDKs. We are wire-compatible with everyone else's.**

- Sentry envelope in → every Sentry SDK on ~30 platforms is our SDK, by changing a DSN host.
- OTLP HTTP + gRPC in → every OTel Collector and SDK, by changing an endpoint.
- HEC in → every Splunk forwarder, by changing a hostname.

Becoming a backend does not change this; it deepens it. Migration cost is a config line,
which defuses the brief's stage-2 "become the backend by attrition" plan into something
actually mechanical. Keep the same discipline that got us here — byte-preserving forwarding,
unknown item types passed through, upstream rate limits reflected — because it is what makes
"leave your vendor connected while you evaluate us" true rather than a slogan.

Corollary: **run in front of the incumbent for as long as the customer wants.** The
competitor mode and the proxy mode are the same binary with a different destination list.
No rip-and-replace sale, ever.

---

## 6. Where RAE fits — and where it does not

Honest read of RAE's current state (from `docs/task-board.md`, `benchmarks/results/`):

- Correctness is real: TPC-H SF10, 50/50 result sets agreeing with a DuckDB oracle.
- Scan and grouped aggregation scale well — Q6 6.69x and Q1 7.52x at 14 workers.
- **Joins are the ceiling** — Q14 2.39x, Q19 3.49x; I-360 (parallel hash-partitioned join)
  is the identified fix and is not done.
- At 1M rows RAE loses to ClickHouse and DuckDB by 2–10x (B-403), with four located causes.
- The SQL surface is missing `LIKE`, `HAVING`, `CASE`, subqueries/CTEs, `count(DISTINCT)`,
  `date_trunc`, `extract`, window functions — 23 of 43 ClickBench queries runnable.

**So do not enter this as a general-purpose SQL engine race.** We would lose it, publicly,
against two engines with a decade of head start.

> **Correction (§15).** The resolution below — Parquet at the edge, RAE parts centrally —
> contradicts §14's edge push-down, because RAE cannot read Parquet at all. §15 measures the
> cheapest way out and finds that §13's time-window numbers were taken on a sort order this
> product does not produce. Read both before relying on either.

> **Tested 2026-08-07 (§13).** On the log shape specifically, the B-403 result does not
> reproduce: against DuckDB over Parquet, RAE never loses by more than 1.6x and wins three
> of six queries. But against DuckDB *native* it loses 1.8–2.4x on every aggregate shape.
> **The argument below is right and the reason is wrong** — owning the engine is not
> justified by speed. It is justified by the sparse index on time-bounded queries (a 21.6x
> win, below) and by one format everywhere. Read the rest with that correction applied.

Enter it as a *workload-specific* engine, where the workload is unusually kind:

| Log-analytics query shape | RAE status |
|---|---|
| Time-bounded scan + predicate | strongest area (Q6) |
| Low-cardinality GROUP BY on `template_id` / `service` / `level` | strongest area (Q1) |
| top-k, count, sum over immutable parts | built |
| Large joins | weak — **and largely absent from this workload** |
| Lookups / enrichment (small dimension side) | weak area doesn't bite; build side is tiny |

The strategic reason to own the engine is not raw speed. It is **one part format and one
execution kernel from the edge agent, through object storage, into the central cluster.**
Vendor stacks carry three formats (agent buffer → ingest pipeline → index) and pay
translation at every hop. If a part is a part everywhere, then "query the edge" and "query
the cluster" are the same code path, and edge push-down becomes ordinary distributed
execution — the search-head/indexer split, with indexers you already own and never paid to
move data into.

Specific things owning the engine buys that a bought engine will not give us:

1. A sparse primary index on exactly `(service, template_id, ts)` — the shape of every
   query in §3.
2. Codecs tuned for log columns. Delta-encoded timestamps already moved log-less from
   3.89x to 5.19x; `template_id` is a dictionary column with extreme locality.
3. A **fleet-global template dictionary** shared across parts and nodes, so `template_id`
   comparisons are integer comparisons everywhere and the spine's rollups join to instance
   data without a string in sight.
4. Part-level pruning statistics that the coordinator can evaluate *before* deciding to
   hydrate — which is what makes the §4 "this costs 30 s, run it?" prompt possible.

### Two constraints this must not break

- **Parquet stays the API.** `architecture.md` §7 commits to it and it is a real
  anti-lock-in moat ("point your own DuckDB at it"). RAE already reads Parquet
  (`raebench-clickbench-load`). Resolution: Parquet remains the durable, interoperable,
  object-store format; RAE-native parts are an *accelerated* layer for the central hot
  tier, rebuildable from Parquet exactly as the SQLite catalog is today. Never a format
  the user cannot escape.
- **The edge binary stays small.** 7.3 MB is a selling point and the reason "no embedded
  query engine" was decided. Edge push-down needs only scan + filter + partial aggregate —
  a strict subset, far smaller than the ~30 MB DuckDB was rejected for, but **the number
  must be measured before this is committed to**, and it should ship behind a feature flag
  or as a separate `logless-query` binary for fleets that want it. If it costs more than
  ~2–3 MB, drop edge push-down and hydrate from object store only; §4 already makes that
  survivable.

### Concrete RAE gap list for this product

Ordered by whether the product works without it:

1. `LIKE` / substring predicates — mandatory for logs, missing.
2. `date_trunc` / time bucketing — every timechart needs it, missing.
3. `HAVING`, `CASE WHEN`, `count(DISTINCT)` — mandatory for alerting queries, missing.
4. Dynamic attribute access (schema-on-read over the OTel attribute map) — this is
   Splunk's whole schema-on-read story and RAE has no equivalent concept yet.
5. Subqueries/CTEs — needed for anything resembling a correlation search.
6. I-360 parallel join — matters for lookups/enrichment at scale, not for the core shape.
7. Window functions — deferrable.

Items 1–4 are the honest cost of entry and none of them are in the current task board.

---

## 7. The console already exists — and it is the third differentiator

`sql-studio` is not a nice-to-have front end for this. It is the consumption tier, and it
already solves the specific problem that shapes every log UI.

**Viewport-only transport.** Measured in `sqls-engine`'s `payload_budget` test: a
1,000,000-row result is 48.8 MB in native memory and 5,448 bytes as a 100-row viewport —
**8,953x**, and a viewport at row 999,900 costs the same as one at row 0. Splunk's search
UI streams results into the browser and therefore trains users into `| head` discipline
and result caps; that entire class of constraint does not exist here. A log console that
can scroll a 250-million-row result at constant cost is not a UI polish item, it is a
category difference in how the product feels.

**The engine seam is already the right shape.** `sqls-source` is *SQL in, Arrow out, one
module per engine* — DuckDB, ClickHouse, PostgreSQL, SQL Server today. RAE/log-less is
**a new Source, not a new product**. And `AdapterCapabilities.sourceRowWindows` already
exists precisely so an adapter can serve source-row windows rather than shipping the whole
result — which is exactly the contract a tiered log store needs to expose.

> **Tested 2026-08-07 (§12).** All three views work against the real store through the
> unmodified DuckDB `Source`, and the viewport claim gets *better* on log data (10,974x).
> "Assembly, not construction" holds, but only just: `cube-core` mapped five Arrow types and
> silently nulled the rest, so `Timestamp` and `UInt64` grouping keys — the two that matter
> most here — collapsed to one row without an error. Fixed, with tests. One real `cube-core`
> change, found in an afternoon rather than in a demo.

**Three views over one retained result** — `Results` (raw rows), `Pivot` (native cube),
`Chart` (fincharts), all bound to the same `queryId`, so switching never re-runs the SQL.
Map that onto logs and it is Splunk's Events / Statistics / Visualization tabs — except
Splunk re-runs the search to move between them. Here:

| sql-studio view | Log-console meaning |
|---|---|
| Results (raw, zero-copy `RecordBatch::slice`) | the instances — hydrated rows |
| Pivot (cube-core-rs aggregation) | **the spine** — template_id x service x minute rollups |
| Chart | timechart / rate-spike view |

The Pivot path is a particularly good fit because the cube engine is an *aggregation*
engine and the spine (§3) is pre-aggregated by construction. The raw/shaped split already
built in `sqls-engine` — raw slices retained batches, shaped materialises through the cube —
is the same split as spine-vs-hydrate, one layer up.

**The tier-cost prompt from §4 has a natural home here.** "This needs cold hydration —
~12 GB, ~30 s. [Run]" is an intolerable interruption in a consumer dashboard and completely
normal in an IDE, where the user already accepts that a query costs something. Choosing an
IDE as the primary surface makes the hardest UX problem in the tiering design almost free.

### The workflow half: Zero, not the columnar store

§2 requires an issue lifecycle over templates — assign, resolve, snooze, regress,
resolved-in-release, comment. That state is mutable, tiny, high-contention, multi-user and
wants to be live. It has nothing in common with the log data.

Rocicorp Zero (already vendored in `__externals/zero`) is the right engine for it, and the
tell is that Zero's own reference application, `apps/zbugs`, **is a bug tracker**. The
workflow half of a Sentry competitor is a bug tracker with an unusual data source.

So the console runs two planes, deliberately:

| Plane | Engine | Data | Properties |
|---|---|---|---|
| Analytical | RAE → Arrow → cube-core-rs, viewport-only | logs, spine rollups | huge, immutable, read-only, batch |
| Collaborative | Zero (IVM + sync) | template state, assignments, comments, saved views, alert rules | tiny, mutable, real-time, offline-tolerant |

They join on one integer: `template_id`. Do not put issue state in the columnar store
(mutable rows in immutable parts is the mistake that leads to deletion vectors — already
rejected in `architecture.md` for retention, and the reasoning holds here). Do not put logs
in Zero — IVM over a billion immutable rows is not what it is for.

Zero also buys the thing that makes an incident tool feel alive and is otherwise weeks of
websocket plumbing: two engineers triaging the same storm see each other's assignments and
resolutions update live, and a laptop that drops off Wi-Fi mid-incident keeps working.

### The one honest problem this creates

`sql-studio` is a **desktop Tauri app**, and its central architectural claim is *no
WebAssembly in the data path* — the cube engine runs natively in-process because a desktop
app has somewhere to run it. A multi-tenant SaaS console has no such process on the user's
machine.

The good news is the inversion is already anticipated: two hosts exist today (Tauri shell
and a browser dev host against `sqls-server` on loopback, with `lib/api.ts` hiding the
difference). The SaaS path is the browser path, with `NativeCubeSession` running in the
server tier instead of the shell, and the loopback HTTP API promoted to a real one. The
viewport-only property *survives that move intact* — arguably it matters more over a WAN
than over loopback. What does not survive is "the data never leaves your machine", so if
that becomes a sales claim, the desktop build has to stay a first-class shipping artifact,
not a dev convenience.

Cost to note plainly: this is now **four repos** (log-less, RAE, sql-studio, CubeCanvas)
plus a vendored sync engine. §9's velocity-tax risk gets worse, not better.

---

## 8. What a v1 competitor actually requires

Beyond storage and query, in rough dependency order. The infra half is the part we are
good at; the workflow half is where the incumbents' stickiness actually lives, and it is
routinely underestimated.

**Infra**
- Spine store + rollup ingest (RAE)
- Object-store tiering with node-owned prefixes + part catalog
- Query planner with visible tier costs
- Multi-tenancy, retention policy per dataset
- RBAC / SSO / audit log — hard gate for the Splunk buyer

**Console (§7) — mostly assembly, not construction**
- A `sqls-source` module for log-less/RAE — the largest single piece, and it is an adapter
- Log-shaped defaults over the existing three views: template list, spine pivot, timechart
- Tier-cost prompt in the query path
- Server-hosted `NativeCubeSession` for the browser host

**Workflow — the part that makes people stay**
- Template lifecycle over Zero: assign, resolve, snooze, regress, resolved-in-release
- Ownership routing (CODEOWNERS-style) and suspect-commit linkage
- Alert rules: threshold, novelty, rate-spike, cardinality-explosion (the last three are
  ours and neither incumbent has them on unshipped data)
- Integrations: Slack, PagerDuty, Jira/Linear, GitHub — commodity, mandatory
- Dashboards and a saved-query surface

**Deliberately skipped in v1:** symbolication/source maps, session replay, profiling, cron
and uptime monitors, SPL compatibility, an app ecosystem, Enterprise Security/ITSI.

---

## 9. Positioning, and the one thing to pick

The technology unifies Splunk and Sentry. **The go-to-market must not attempt both at once.**

Recommendation: **Sentry-side first.**

- SDK compatibility is already free, so time-to-first-value stays at minutes.
- Self-serve, bottom-up, cheap sale — matches a small team's ability to support customers.
- "**Sentry, but for every log line, not just the ones with a stack trace**" is one sentence,
  and it describes something that does not exist.
- The Splunk buyer needs RBAC/SSO/audit/multi-tenancy/PS before the first pound arrives.
  Move up-market once those exist and the spine has proven itself at fleet scale.

Pricing must keep the brand promise intact: per-host for the edge, plus retained-central-GB
(spine only) or a flat all-in per host. Never per raw GB. The reason we are cheap is the
selection function in §3, not a discount — that is defensible at renewal in a way
%-of-savings never is.

---

## 10. Risks this creates that the current design does not have

1. **We become the system of record.** The brief lists that as a non-goal for good reason.
   Losing data now loses the company, so the durability bar rises from `fdatasync` to
   replication + object-store durability + verified restore. Not a stretch goal.
2. **Scope roughly triples.** Storage, query, distributed execution, a workflow product, and
   an enterprise-controls surface. Against a six-week prototype that is mid-flight, this is
   the dominant risk and the reason §8 says pick one buyer.
3. **RAE's charter changes.** It currently states success is measured by architectural
   foundations being sound, explicitly *not* feature parity. Making it a product engine
   means adopting a compatibility and correctness burden the charter deliberately declined.
   That should be an explicit, dated decision in `CHARTER.md`, not a drift.
4. **The template primitive is load-bearing and has a known defect.** §2's lineage problem
   is unsolved. If shapes churn on every deploy, the spine's counts stay right but novelty
   alerting — headline #3 — degrades into noise.
5. **Four-repo velocity tax.** Every product feature crosses repo boundaries with different
   ownership rules and a research-grade task board on one side. `sql-studio` and CubeCanvas
   also have their own product identities; making them a log console risks forking their
   roadmaps rather than reusing them. Decide explicitly whether the log console is a
   *host* of CubeCanvas (as sql-studio already is, via `grid-react/external-engine`) or a
   fork of it. Host, or this compounds.
6. **The incumbents move.** Datadog owns Vector and ships Flex Logs and Observability
   Pipelines; Sentry ships Logs and Relay; Splunk ships Edge Processor and Ingest Actions.
   Everyone is walking toward the edge. Our differentiator is not the edge — it is
   **selection by shape coverage plus reversibility**, and that is what to build fast.

---

## 11. Smallest experiments that would settle this

None of these require committing to the plan.

| Question | Experiment | Kills the plan if |
|---|---|---|
| ~~Is the spine actually small?~~ **Run — see §12** | Compute spine bytes over the 11 LogHub corpora already in `scripts/loghub.sh`: distinct templates × (rollup + exemplars) ÷ raw bytes | >10% of raw on real corpora — the economics collapse |
| Is shape-first navigation sufficient? | Take 20 real past incidents; classify each query as shape-answerable vs needs-instances | Most need unqualified free-text search — then we do need a token index after all |
| Do templates survive deploys? | Replay one service's logs across 10 real releases; count template churn per deploy | Churn swamps genuine novelty and lineage looks unsolvable |
| ~~Can RAE serve the shape?~~ **Run — see §13: yes, but not because it is faster** | Load a real log corpus into RAE; run the §3 queries against DuckDB over the same Parquet | RAE loses by >2x on *scan + low-cardinality group-by* — its supposed strength |
| ~~Does edge push-down fit the binary?~~ **Effectively answered — see §14: the whole engine is 1.5 MB** | Build log-less with RAE's scan/filter/partial-agg subset; measure the stripped binary | >~10 MB — drop edge push-down, hydrate from object store only |
| ~~Is the console really an adapter away?~~ **Run — see §12: mostly yes, one real `cube-core` change** | Write a `sqls-source` module over the existing Parquet store (no RAE, no spine) and open a real log corpus in sql-studio's Results/Pivot/Chart | It needs grid-core or cube-core changes to work — then §7's "assembly not construction" claim is wrong and the console is a build, not a reuse |

The first and third are cheap, use corpora already in the repo, and between them decide
whether §3 — the entire thesis — is real. The last is cheap too and is the fastest way to
see the whole product on screen: a Parquet-backed Source needs no new storage tier at all.

---

## 12. Measured, 2026-08-07 — spine size and the console

Two of §11's rows were run. The fixture is 1,000,000 events built from the 11 LogHub
corpora (20,968 unique real lines, 11 datasets used as 11 services), loaded through the
**HEC receiver** with per-event `time` spread over 7 days, then merged to Parquet.
Reproduce with `scripts/spike-console.sh`; the console assertions live in
`../../sql-studio/crates/sqls-server/tests/logless_console_spike.rs`.

### Ingest

| Metric | Result |
|---|---|
| HEC ingest rate, one client, 5k-event batches | **315,957 events/s** |
| Accounting | `received=1000000 enqueued=1000000 written=1000000`, all drop counters 0 |
| 503s at 5k batches | 0 |
| 244 MiB NDJSON → store | **16.9 MiB** (~14x) |
| Templates mined | 650 |

One operational note that is not a bug but bites immediately: a batch larger than
`ingest.queue_capacity` (65,536) can never be enqueued, so it 503s forever. 100k-event
batches retried 60 times each and never landed; 5k batches landed with zero retries. Worth
stating in the HEC docs, because the failure looks like backpressure and is actually a
batch-size ceiling.

### Spine size — the number §3 rests on

| Denominator | Spine / raw |
|---|---|
| All 1,000,000 ingested events (256 MB) | **0.065%** |
| The 20,968 **unique** corpus lines (3.2 MB) | **5.2%** |

666 distinct `(service, template_id)` shapes — **0.067% of rows**. Spine payload (count,
first/last seen, one exemplar body per shape) is 167 KB.

**Read the second row, not the first.** The fixture cycles 20,968 unique lines to reach a
million, so the 0.065% is flattered by repetition. The honest bound is 5.2%: what the spine
costs if *every* line in the corpus is as shape-diverse as a deliberately-varied sample
drawn from 11 unrelated systems. Production fleets sit far below that — they emit the same
shapes millions of times — but 5.2% is the number to defend, and it still clears §3's
economics. **§3 survives its first contact with real data.**

Not yet tested, and the thing most likely to move this: shape growth over *time* and
across deploys. A one-shot corpus cannot show it. That is the §2 lineage problem, still
open.

### The console

`sqls-source`'s unmodified DuckDB `Source` reads the log-less store directly — no
extension, no schema translation. **"Parquet is the API" holds from the console side.**

| Measurement | Result |
|---|---|
| 1M log rows in native memory | 271,085,808 bytes |
| Results viewport, row 0 (100 rows, 10 cols) | 24,776 bytes |
| Results viewport, row 999,900 | 23,496 bytes |
| **Reduction** | **10,941x** |
| Pivot: 666 shapes → cube rows | 678 (11 services + 666 leaves + grand total), 21 KB window |
| Chart: `date_trunc('minute')` buckets → cube rows | 10,083 → 10,083 |

The viewport claim **strengthens** on log data — 10,941x against `payload_budget.rs`'s
8,953x on a synthetic 5-column frame — despite logs being wider and carrying an unbounded
`body` string. And the deep viewport is *cheaper* than the head one, so there is no
offset-walking anywhere in the path.

### Three defects found — two fixed, one recorded

**1. `cube-core` mapped five Arrow types and silently nulled the rest. Fixed.**
`cube-engine/src/arrow_adapter.rs::arrow_type_to_scalar_type` handled `Int32`, `Int64`,
`Float64`, `Utf8`/`LargeUtf8`, `Boolean`, and sent everything else to `ScalarType::Null`
through a catch-all arm. A `Timestamp` or `UInt64` grouping key did not error — it collapsed
to one null group and the grid rendered a plausible single row. Measured: 10,083 distinct
minute buckets became **1 cube row**, with no diagnostic. The two most important grouping
keys in this product — a time bucket and `template_id` — were both in the catch-all.

The shape of the bug is worth recording: `file_source.rs`, the crate's *own* Parquet
reader, has always handled this whole type set correctly. Only `arrow_adapter.rs` — the
path a **host** engine like sql-studio takes — did not. Two ingest paths, one complete.

The fix widens the map to the same types `file_source` already supported, and coerces the
physical array to match. The second half is what makes it work: the accessors downcast to
one exact Arrow array and return 0 on a miss, so widening the map alone would have kept
returning zeroes under a type that now *claimed* to be supported. Temporal columns render
as full-precision ISO-8601 — deliberately **not** `file_source`'s day-truncated form, which
would have collapsed a per-minute series into one row per day. Nine unit tests; the full
599-test cube suite still passes.

Verified end to end afterwards, with the SQL casts removed: an uncast
`(Timestamp, UInt64)` compound key yields **134,333 leaves + 10,083 parents = 144,416 cube
rows**, against 1 before.

Still unrepresented and now pinned by a test rather than silently nulled: `Binary`,
`Decimal`, `List`, `Struct`, `Map`, `Interval`, `Time32/64`. `Binary` is the one that
matters here — `trace_id` and `event_id` are `FixedSizeBinary(16)`, so "everything for
trace X" cannot yet be a group key.

**So §7's "assembly, not construction" claim survives, narrowly and with a caveat.** The
console is assembly *plus one real `cube-core` fix*, found in an afternoon rather than in
a demo.

**2. Nanoseconds become microseconds through DuckDB. Recorded, not fixed.** The log-less
schema declares `Timestamp(ns, "UTC")`; the console sees `Timestamp(µs, "UTC")`. The UTC
qualification — the thing `architecture.md` §2 went to trouble over — survives intact, and
it is the part that would silently corrupt time filters. Sub-microsecond precision does
not, so ordering *within* a burst is not recoverable in the console.

**3. log-less: HEC never read the level from the line. Fixed.**
On `/services/collector` (the JSON endpoint), `infer_severity` read `fields`, then keys
inside a structured `event` object — **never the event text**. `severity_of_line` existed
and did the right thing, but was wired only to `/raw`. A client sending real log lines as
JSON string events got `DEFAULT_SEVERITY` for all of them: all 1,000,000 rows landed in
`level=info`, including every line containing `[error]`.

The consequences went well past a wrong column — level-partitioning collapsed to one
bucket, per-level retention applied the `info` policy to errors, and **pushdown never
fired** (`errors_forwarded=0`), which is the product's headline feature.

The fix has two parts, because the obvious one alone does nothing:

* String events fall back to the line's own prefix. Structured events deliberately do not —
  their keys were already checked, and scanning rendered JSON would match the word "error"
  inside a message.
* The scan window widens from 3 tokens to 12, **stopping at the first `key=value` token**.
  Three tokens missed every real format measured: the level sits *after* the metadata
  prefix — Apache at token 7, HDFS and Zookeeper at 4, BGL at 9. But a count alone cannot
  separate BGL's token-9 `FATAL` from prose at token 12; logfmt pairs mark where a prefix
  ends and content begins, and no real prefix contains one. Without that second bound,
  `note=the user typed ERROR into the form` becomes an ERROR — forwarded upstream, and
  someone paged.

| | before | after |
|---|---|---|
| level partitions | 1 (`info`) | **3** (debug 225 / info 836,727 / warn_plus 163,048) |
| `errors_forwarded` | 0 | **59,462** (30 with context, 56,240 deduped, 3,190 rate-limited) |

Same 1M events, same corpora. The headline feature went from dead to working on real data.

---

## 13. Measured, 2026-08-07 — RAE against DuckDB on the log shape

§11 asked whether RAE can serve this workload, given B-403 measured it losing to
ClickHouse and DuckDB by 2–10x at 1M rows on general-purpose queries. Run against the same
corpus as §12: the 1M-row log store, and a 20M-row extension of it (the same shapes emitted
20x more often, which is how real fleets grow — they do not invent 20x more shapes).

Three configurations, because two would mislead:

| | what it represents |
|---|---|
| **RAE (parts)** | RAE over its own format — what owning the engine buys |
| **DuckDB (parquet)** | DuckDB over the same Parquet — the format `architecture.md` §7 already commits to, and what "point your own DuckDB at it" means today |
| **DuckDB (native)** | DuckDB over its own table — DuckDB at its best, which is what a "just use DuckDB centrally" alternative gets |

Fastest of five, 14 workers both sides. Every result set was compared for equality first;
all six agree across all three engines at both scales. RAE's timing excludes process
startup, which is right for the embedded use the thesis proposes.

### 20M rows

| query | RAE ms | DuckDB/pq | DuckDB/native | vs pq | vs native |
|---|---:|---:|---:|---:|---:|
| spine — `GROUP BY service, template_id` | 39.66 | 24.90 | 21.90 | 0.63x | 0.55x |
| scan + filter — `severity >= 17` | 2.88 | 5.66 | 2.01 | **1.96x** | 0.70x |
| top-k templates | 13.42 | 13.12 | 6.13 | 0.98x | 0.46x |
| level filter + group | 18.70 | 13.31 | 7.71 | 0.71x | 0.41x |
| time-window spine | 2.43 | 4.05 | 1.79 | **1.66x** | 0.74x |
| time-window count | 0.11 | 2.42 | 0.33 | **21.59x** | **2.99x** |

Above 1.00x means RAE is faster. The 1M run has the same shape, so these are throughput
differences rather than fixed overheads.

### What it says

**The B-403 result does not reproduce on this workload.** Nothing here is 2–10x. Against
DuckDB over Parquet — the comparison that matches the thesis's own storage decision — RAE
never loses by more than 1.6x and wins three of six.

**RAE wins exactly where the sparse primary index bites**, and the log workload is
time-bounded by nature. The extreme case is worth stating plainly: the time-window count
answers in **0.11 ms at 20M rows** because RAE counts 136,480 rows out of granule metadata
without reading them, pruning 85.6% of granules and half the parts. "How many X in window
Y" is the single most common question asked of a log store, and it is what the spine is
made of.

**RAE loses on unfiltered hash aggregation**, consistently, by 1.8–2.4x against DuckDB
native. That is the honest weakness, and it is the same ceiling B-417 identified.

**So the §6 argument needs correcting: owning the engine is not justified by speed.**
DuckDB native is faster on every aggregate shape. What owning it buys is (a) the
sparse-index and granule-metadata advantage on time-bounded queries, which is the dominant
log shape and not something a general engine will tune for, and (b) the one-format-
everywhere argument — edge, object store and cluster sharing a part format so "query the
edge" and "query the cluster" are one code path. That case still stands. "RAE is faster"
does not, and §6 should not be read as claiming it.

### Two gaps found in the process

**Both were fixed the same day, under RAE's I-385.** Recorded because the shapes are worth
keeping, and because §6's gap list should not carry items that are closed.

**RAE had no timestamp literal.** `WHERE timestamp >= TIMESTAMP '2026-08-01 00:00:00'` was
a parse error; instants bound only as typed *parameters* (`:lo`, ISO-8601). Good design for
a driver, blocking for a console where users type SQL — and every log query is
time-bounded. Re-verified after the fix: the literal parses and prunes identically to the
parameter form (85.59% of granules and 1/2 parts pruned, 136,480 rows counted from granule
metadata without being read).

**RAE's Parquet loader read only uncompressed Parquet.** zstd and snappy both failed with
`Disabled feature at compile time` — `parquet` was declared `default-features = false`,
silently dropping every decompressor. Since the log-less store writes zstd, this blocked
ingestion from the format this document calls the API; stock `hits.parquet` is compressed
too, so ClickBench could not have been loaded either. Re-verified after the fix: the zstd
store loads directly, 1M rows in 0.45 s.

Full write-up in RAE's `benchmarks/results/B-433_log_workload.md`.

---

## 14. Decision, 2026-08-07 — keep RAE, for sovereignty and footprint, not for speed

§13 removed the performance argument for owning the engine. The engine stays anyway, and
the reasons are better than the one they replace.

**Measured, this machine, `cargo build --release -p rae-cli` then `strip`:**

| | size |
|---|---|
| `rae` — storage, execution, aggregation, planner, SQL parser, CLI | **1.5 MB** |
| DuckDB's linked contribution (recorded in `architecture.md`) | ~30 MB |

### Why

- **Footprint.** 1.5 MB for a whole columnar engine, against ~30 MB of statically linked
  C++ for one that over-serves the workload.
- **No foreign toolchain.** No C++ compiler, no `protoc`, no build-time code generator, no
  bundled third-party build. One `cargo build`.
- **One language, one ecosystem.** Cross-compilation to every node architecture is the
  default rather than a porting exercise, and wasm stays open.
- **No third-party roadmap dependency.** DuckDB is MIT, so this is not a licensing risk —
  it is a *control* one. An engine at the centre of the product should not have its
  priorities set elsewhere.

**None of this is a new principle.** It is the same judgement already recorded three times
in `architecture.md`: `prost` + `tonic` + `opentelemetry-proto` rejected for a code
generator and an HTTP/2 stack; embedded DuckDB rejected at ~30 MB to serve a filtered scan;
DataFusion rejected for the same over-serving. §14 applies it once more rather than
inventing anything.

### The consequence nobody had connected

§7 set a kill threshold for edge push-down: *">~10 MB — drop edge push-down, hydrate from
object store only."* RAE's **entire** engine is 1.5 MB, and edge push-down needs only the
scan/filter/partial-aggregate subset of it. That clears the threshold by most of an order
of magnitude.

So §11's "does edge push-down fit the binary?" is effectively answered before it was run,
and the distributed-execution story in §4 — coordinator pushes a filtered scan to the node,
node answers a partial aggregate over its own parts — is affordable. That is the single
biggest thing the small binary buys, and it is worth more than the aggregation deficit
costs.

### Total node size, measured

The question §14 exists to answer is not "how big is RAE" but "how big is the node agent
if it carries RAE". Measured by actually linking them: RAE's crates added to
`logless-cli`, a probe called from `main` so LTO cannot discard the code, built under
log-less's own release profile (`lto = "thin"`, `codegen-units = 1`, `strip = true`,
`panic = "abort"`), then reverted.

| node binary | size | delta |
|---|---:|---:|
| `logless` today | 11.05 MB | — |
| **+ push-down kernel** — `rae-storage` + `rae-execution` + `rae-aggregation` + `rae-common` | **11.09 MB** | **+0.04 MB** |
| **+ the whole engine** — also `rae-sql`, `rae-planner`, `rae-runtime`, `rae-joins` | **11.79 MB** | **+0.74 MB** |

Both variants were verified live, not merely linked: the kernel build read 1,000,000 rows
out of real parts, and the full build answered `SELECT count(*) FROM logs` from inside the
`logless` binary.

**Edge push-down costs 40 KB.** §7's kill threshold was 10 MB. It is not close.

Even a full SQL engine on every node — parser, planner, morsel scheduler, joins — costs
0.74 MB, which is *less* than the 1.5 MB standalone `rae` binary, because log-less already
links `serde`, `thiserror`, `crossbeam` and `hashbrown` and thin LTO shares them. The
marginal cost of the engine is far below its standalone size.

Set against the alternative: embedding DuckDB was rejected at ~30 MB, which would have been
**~3.7x the entire current agent** to serve a filtered scan. RAE's full engine is 6.7% of it.

Note also that the 7.3 MB recorded in `architecture.md` is now stale — the agent is 11.05 MB
before RAE, having grown with the receivers and the identity work. The comparison above is
against the real current number, not the documented one.

### The bill, stated honestly

Sovereignty is not free, and this is what it costs — everything RAE must acquire that
DuckDB already has:

| | why it blocks |
|---|---|
| ~~Timestamp literals~~ | ~~every log query is time-bounded~~ — **closed by I-385** |
| ~~Compressed Parquet reads~~ | ~~blocks the format we call the API~~ — **closed by I-385** |
| `LIKE` / substring | mandatory for logs |
| `date_trunc` / time bucketing | every timechart |
| `HAVING`, `CASE WHEN`, `count(DISTINCT)` | alerting queries |
| Dynamic attribute access | schema-on-read, Splunk's whole story |
| Subqueries / CTEs | correlation searches |
| **Hash aggregation throughput** (B-402, still `todo`) | 1.8–2.4x behind DuckDB native |

The first two closed within a day of being found, which is some evidence the bill is
payable. The last is the real engineering. **Accepted as the price of the four properties
above** — but it should be a decision taken with the list
visible, not discovered one query at a time.

---

## 15. The contradiction in §6, and what pruning actually costs

§13 and §14 left an error in this document. This section is that error, measured.

### The contradiction

§6 resolves the format question by saying **Parquet stays the durable edge and object-store
format; RAE-native parts are an accelerated layer for the central hot tier.** §14 then
prices edge push-down at 40 KB and treats it as unlocked.

Both cannot be true. RAE cannot read Parquet — at all:

```
rae -t logs=…/logs.parquet   bad magic: expected [82, 65, 69, 49]   # "RAE1"
rae -t logs=…/store          table logs has no parts
```

Parquet is an *import* path — `raebench-clickbench-load` converts Parquet into `.part`
files, offline, and it lives in `benchmarks`, not in the engine. I-385 taught that loader to
decompress; it did not make RAE a Parquet reader. So a node that stores Parquet has nothing
the 40 KB kernel can read.

Four ways out:

| | cost |
|---|---|
| Node writes Parquet only | no edge push-down without on-the-fly conversion |
| Node writes both | ~2.8x disk (Parquet 1.0x + parts 1.8x) against a product selling cheap local retention |
| Node writes parts only | breaks "Parquet is the API" — no user DuckDB/Polars/Grafana against the store |
| **Node's Parquet is read as parts** | needs work, but no dual storage and no broken API |

### The fourth option is cheaper than it looks

The objection is that Parquet libraries are large. Measured, same profile, verified by
reading 1,000,000 rows so nothing is stripped:

| binary | size |
|---|---:|
| baseline, no dependencies | 0.32 MB |
| + `arrow` + `parquet`, reader path only | 4.67 MB |
| **marginal** | **+4.35 MB** |

That is ~3x RAE's entire engine, and it is why `parquet` belongs in `benchmarks` rather than
in the engine crates — adding it to a 1.5 MB standalone `rae` would nearly quadruple it.

**But log-less already pays that 4.35 MB**, because it writes the store. So the adapter
belongs where the format already is: log-less hosts a Parquet-to-parts read path, RAE's
engine crates stay dependency-free, and the node pays nothing extra in libraries.

For scale: DuckDB ~30 MB · arrow+parquet 4.35 MB · RAE's whole engine 0.74 MB marginal.

### Whether pruning survives — and the flaw it exposes in §13

The open question was whether Parquet row-group statistics can drive RAE's granule pruning
well enough to keep §13's time-window win. Measured on the real store and on §13's own
fixture, 24 windows swept across the range:

| layout | groups | rows/group | median coverage of full range | 1h window reads | 1d reads |
|---|---:|---:|---:|---:|---:|
| **the real store** — sorted `(service, trace_id, observed)` | 23 | 43,478 | **0.465** | **47.1%** | 55.1% |
| **§13's fixture** — sorted `timestamp` | 9 | 111,111 | 0.123 | **12.3%** | 23.3% |

**Sort order dominates, not the format.** Time-sorting cuts a one-hour window from 47.1% of
rows to 12.3% — 3.8x better — *despite* row groups being 2.6x larger. Parquet statistics
prune fine; they can only prune what the sort order has clustered. Granularity is the
remaining gap (111k-row groups against RAE's ~4,500-row granules) and row-group size is a
writer parameter log-less already controls.

**So §13's headline is measured on a layout this product does not produce.** The 21.6x
time-window win and the 85.59% granule pruning came from a fixture I sorted by `timestamp`
before loading. The store sorts by `(service, trace_id, observed)` — deliberately, per
`architecture.md` §2, to make "everything for trace X" a range scan and to cluster templates
for zstd. Against the store's real layout, a time-bounded query touches roughly half the
data. **The time-pruning story and the existing sort order are in direct conflict, and
nothing in §13 revealed it because the benchmark quietly assumed the conflict away.**

One caveat that cuts the other way, and it matters. Partitioning is `hour=…/level=…` keyed
on **`observed`**, not `timestamp`. Under live ingestion the two are nearly equal, so
directory pruning already answers time queries and none of the above bites. They diverge
only for replayed or clock-skewed sources — which is exactly what this fixture is (1M rows
ingested at once carrying seven days of event time), and exactly what headline #2, "turn
yesterday's verbosity up, today", is *made* of. So the pathological case is not exotic; it
is a named feature.

### What this selects

Not resolvable from first principles, and now the most important open question in the
document:

1. **Sort time-first and lose the trace range scan**, or sort service-first and lose time
   pruning. `architecture.md` §2 chose service-first for reasons that were good and did not
   have this in view.
2. **Or partition on event time as well as ingest time**, so replayed data still lands
   somewhere prunable — at the cost of the clean "retention is a directory delete" property.
3. **Or shrink row groups**, which is free and helps regardless, but only after 1 or 2.

None of the three is chosen here. What is settled is that §6's format contradiction has a
cheap resolution, and §13's benchmark needs re-running against the store's real sort order
before any of its time-window numbers can be quoted about this product.
