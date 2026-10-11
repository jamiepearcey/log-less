# Optional decision model

An opt-in model that answers typed questions about log clusters. Off unless `[decision]` has a `url`.

```toml
[decision]
url = "http://127.0.0.1:8099/decide"   # a server taking the request shape below
token = "..."                          # optional, sent as Authorization: Bearer
min_confidence = 0.8                   # a merge is suggested at or above this P(same failure)
max_pairs = 60                         # candidate pairs put to the model per request
timeout_secs = 10
```

## What it does

`GET /admin/v1/templates/merge-suggestions` takes the 300 most frequent templates, picks candidate pairs that share at least
10% of their words (most alike first, at most `max_pairs`), asks the model "are A and B the same underlying failure?", and
returns the pairs it is `min_confidence` sure about, with both counts. **Nothing is merged or changed**: it is a list for a person.
A provider that is down is reported once (`report.error`) and the rest of the questions are not attempted.

## What it deliberately does not do

It does not triage clusters, judge whether an anomaly deserves a page, or gate the sync boost. Measured on Laya's base English
checkpoint (421M parameters, CPU, about 200 ms per call, 2026-10-11, hand-written fixtures so optimistic):

| Job | Result |
|---|---|
| Same failure? (50 pairs) | 74% right, AUC 0.82 (token overlap: AUC 0.48). At 0.8 confidence 12 of 13 suggested merges right. |
| Triage a cluster (60 clusters, 5 classes) | 52% (chance 20%); answers "bug" for 38 of 60 whatever the option order. Actionable-vs-noise AUC 0.59, a plain level >= error rule 0.81. |
| Page / show / ignore an anomaly (37 signals) | 19% (majority class 43%), AUC 0.49. |

So the boost is gated by plain context instead (`sync_signals`: a template flood that is all info or debug is noted, not
boosted). Retest triage and gating after fine-tuning on labelled real clusters, or with the typed-decisions checkpoint
(not tried). Harness: `~/tmp/laya-eval/run`.

A live check against the real model on a small catalog suggested one true pair (0.86) and one wrong one (0.82), so treat a
suggestion as a prompt to look, not a fact.

## The request

`POST <url>`: `{"state": "...", "questions": {"q": {"type": "choice", "instructions": "...", "criteria": {"option": "meaning"}}}}`
and the answer `{"answers": {"q": {"type": "choice", "choice": "...", "probabilities": {"option": 0.93}}}}`. This is the shape
`system_one(state, questions)` takes and returns in Laya's inference code; a small server around it is
`~/tmp/laya-eval/run/serve.py`. Not checked against `laya-serve` or Jev's hosted API, whose HTTP envelopes may differ.
