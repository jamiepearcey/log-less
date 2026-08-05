# log-less — project brief

## One-line

A single Rust binary that sits on your own nodes, keeps **full-verbosity logs cheaply and locally**, and forwards only a curated, high-value subset to your existing observability vendor (Sentry / Splunk / Datadog / Elastic) — with the ability to push more, retroactively, when you need it.

## Problem

Observability vendors price on ingest volume, so teams pay for the tax or delete the data:

| Vendor | List price (Aug 2026) |
|---|---|
| Sentry Logs | 5 GB included, then **$0.50/GB** |
| Datadog Logs | **$0.10/GB** ingest + **$1.06–2.50 per million events** indexed (retention-tiered); Flex Logs $0.10/GB + $0.05/M events/month stored |
| Splunk | list **~$150+/GB/day**; enterprise contracts $40k–$500k+/yr |
| Cribl Stream | ~$0.26–0.32 credits/GB, enterprise $40k–$500k+/yr |
| Edge Delta | from ~$0.10/GB |

The behavioural consequence, not the price, is the real pain: **teams raise log levels to save money and then can't debug the incident.** The current workarounds are all bad:

- Sample / drop at the pipeline (Vector, OTel Collector, Cribl, Edge Delta) — filtering is a **one-way door**; the data is gone.
- Keep verbose files on the box — no retention policy, no query, no correlation, no alerting.
- Run a second cheap backend (OpenObserve, SigNoz, Quickwit, Parseable) — keeps the data but splits the workflow into two UIs, two alerting systems, two things to operate.

## Positioning

**"Filtering with an undo button, inside the UI you already use."**

We do not replace the vendor. We sit in front of it, hold the full-fidelity data locally, and make the forwarding decision **reversible**. The vendor UI stays the single pane of glass; the local store is invisible until it's needed.

This is deliberately a wedge, not an end-to-end platform (see stage 2 below).

## The three headlines

1. **"Every error arrives with the debug logs that caused it — and you never paid to ship debug."**
   Exception-triggered context windows pushed into an *unmodified* Sentry issue / Splunk event. Tail-sampling, but for logs. Nothing off-the-shelf does this.
2. **"Turn yesterday's log verbosity up, today."**
   Mid-incident replay of historical TRACE/DEBUG into the vendor, scoped to service + time window.
3. **"Get paged about log patterns your vendor never saw."**
   Novel-template and rate-spike alerts computed on the 95% you didn't ship.

## Feature ranking (build order = value order)

| Rank | Feature | Role |
|---|---|---|
| 1 | Smart pushdown (error + preceding context) | **The week-1 install reason.** Demoable in minutes. |
| 2 | Ad-hoc verbose replay/backfill | The retention justification; episodic but high value. |
| 3 | Per-level retention (debug 1d / info 1w / error 1m) | The pricing story. Alone, it's just a config file. |
| 4 | Template dedupe + novelty/rate anomaly | Real differentiation, trust-heavy — ship after 1–3. |
| 5 | Local WAL + Parquet store | Enabling infrastructure. Nobody buys a lakehouse. |
| 6 | Template compression | Implementation detail. Justified by *structure*, not ratio. |
| 7 | Multi-destination fan-out | Commodity — Vector does it free. Table stakes only. |

## Buyer and trigger

- **Buyer**: platform-eng / SRE lead at a 50–500-engineer company with a $200k–$1M/yr observability bill. Economic buyer is VP Eng / CTO at renewal.
- **Trigger events**: (a) renewal or overage shock; (b) *"we turned debug off to save money, then couldn't debug the outage."* Lead marketing with (b) — it's the story people retell.
- **<1 hour win**: single binary on ONE noisy service, tail stdout/journald, forward ERROR upstream over the vendor's native protocol (no code change). Show two numbers and one screenshot: *"would-have-ingested 38 GB → shipped 1.1 GB"* and a real exception in the vendor UI carrying its local DEBUG context.

## Business model

- **Agent is source-available under BSL / fair-source** (converting to Apache-2.0 on a rolling delay). Free for end users to run in production; blocks Datadog/Cribl from forking it into a competing hosted product. Accepted cost: some enterprises and distros treat non-OSI licences as friction, and it slows organic adoption — which is the wedge. Revisit if adoption stalls.
- **Monetise the control plane**: fleet config/policy, cross-node backfill orchestration, S3 tiering + federated query, RBAC/SSO/audit, the anomaly layer.
- **Flat per-host pricing** (~$10–25/host/mo). Never per-GB — "no GB tax" is the brand, and per-host aligns price with the deployment unit rather than with data we're proudly not shipping.
- Frame ROI as "pays for itself above ~2 GB/day/host". Do not contract as %-of-savings (unauditable, hostile at renewal).

## Stage 2 — the Trojan lakehouse

Front-of-vendor positioning is the right start (it's how Cribl reached $B scale without rip-and-replace), but only with an explicit stage 2, or the ceiling is an acqui-feature.

Stage 2 = add SQL query / Grafana / MCP access over the local Parquet store. Teams notice most investigations resolve locally. We become the backend **by attrition**, never by migration. The v1 decisions in `docs/architecture.md` (OTel data model, Parquet-as-API, rebuildable catalog) exist to make stage 2 additive rather than a rewrite.

## Top risks

1. **DIY substitution** — the buyers who feel this pain most can wire Vector → ClickHouse in a weekend. We sell convenience to the least convenience-buying persona.
2. **Vendor squeeze** — Datadog owns Vector, ships Observability Pipelines + Flex Logs, and can shrink the problem 60% with a renewal discount in a sales call we're not in.
3. **Category incumbency** — Cribl/Edge Delta already own the "observability pipeline" budget line. We're down-market where willingness to pay is lowest.
4. **Stateful-agent trust** — a WAL + compaction engine on every prod node is a disk-pressure liability. The first time we lose logs *during* the incident we were bought for, trust is gone permanently. In k8s, nodes are cattle: node-local durability is partly illusory, so **S3 tiering must be designed in from day one**.
5. **Cost tools are nice-to-haves** — adopted in downturns, churned when attention moves. Stickiness has to come from stage-2 query + alerts people depend on.

## v1 scope decisions (locked 2026-08-04)

- **Downstream targets: Sentry *and* Splunk.** Sentry gives the fast, screenshot-able demo (envelope API; `template_id` → Sentry `fingerprint` is a selling point on its own). Splunk gives the money story ($150+/GB/day list) and the HEC surface we already need on the ingest side. Datadog/OTLP forwarding comes after.
- **Deployment: VM / bare-metal host agent.** systemd unit, file tailing + journald, node-local durability that is actually durable. Kubernetes DaemonSet is v2 and brings S3 tiering with it — do not let k8s pull tiering into the 6-week prototype.
- **Licence: BSL / fair-source** with rolling conversion to Apache-2.0.

## Non-goals for v1

- Metrics, traces, RUM, dashboards, a UI.
- Being anyone's system of record.
- Competing with Cribl on enterprise routing breadth.
