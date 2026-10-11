# Template lineage: stable issue identity across widening

Plane item 66, building on `docs/evidence/template-churn.md` (LOG-65/LOG-18).

## What was built (the smallest thing the evidence supports)

The churn run found two kinds of identity loss:

1. **Widening in place** (a template gains a `<*>`): the content fingerprint changes, so a Sentry issue
   and a dictionary `global_id` split. This is deterministic and fully fixable. Built.
2. **A new template id for an edited or re-tagged statement** (35 + 63 of 517 new ids, clean run): fixing
   this needs a similarity linker. The only one measured linked 100 of 418 genuinely new ids falsely
   (precision 31/(31+100) = 24%). Not built; it would be wrong on the evidence.

Built for (1):

- `Template.lineage_id` (drain.rs): the fingerprint of the template's text at the moment it was minted,
  never recomputed. Unique within a catalog (a re-minted shape colliding with a widened template's birth text is salted with its
  id). `restore` keeps it. `global_id` and `fingerprint()` keep their meaning (current text) and remain the version key.
- Catalog: `templates.lineage_id`, written at first insert and never updated by the upsert. Additive
  migration on open: adds the column and backfills each NULL with the fingerprint of the template's *current* text, which is
  the identity the Sentry forwarder already used, so migration moves no existing issue. For a template that had
  already widened before the upgrade its birth text is gone; its lineage starts at the current text (documented limit).
- Sentry forwarder (`main.rs`): `error_template_fingerprint` is now the lineage id, not the hash of the current text.
  Identical to before for any template that never widens.
- Dictionary `_meta/templates.pq`: new last column `lineage_id` (UInt64, additive; `read` falls back to `global_id` when the
  column is absent). `docs/s3-serve.md` updated. The planners (`sync_policy`, `sync_signals`) key on
  `(service, template_id)`, which widening never changed, so they needed no change; the console's issue identity was already
  `(service, template_id)`.

## Measurement (same replay as template-churn.md, exact counts)

Command: `scripts/template-churn.sh` and `CHURN_VALUES=noisy scripts/template-churn.sh`, 10 Temporal releases, 9 transitions.
Baseline numbers reproduce exactly (517 new, 11 widened, 109 false-novelty ids, 71,196 rendered events), so the miner is unchanged.

| run | widened templates whose issue identity changed | events on them | of events | after lineage_id |
|---|---|---|---|---|
| clean | 11 | 97 | 71,196 | 0 ids, 0 events |
| noisy | 194 | 538 | 71,196 | 0 ids, 0 events |

"Events on them" counts the release's rendered events landing on a template whose identity differs between the
catalog before and after the release (an upper bound on events actually re-issued, since events before the widening point in the
release kept the old identity). Under lineage the identity never differs, by construction.

## What this does not fix (unproven or not done)

- Edited or re-tagged statements still mint new template ids: 98 of 517 new ids (clean), 1,204 events. Lineage across
  *distinct* template ids is not implemented. Needs a linker with precision >= 95% first (the owner's proposed bar); the
  naive one is at 24%.
- The noisy variant's instability (word values, multi-word errors) is untouched; widening is only part of it.
- Cross-agent: two agents agree on lineage only if they minted the same birth text. Agents that reached the same widened
  text from different births keep different lineages (the price of no central registry).
- Data is rendered log lines from real statements, not production traffic (see template-churn.md).
- `lineage_id` of templates already widened before migration starts at their current text.

## Similar-templates merge flow

`GET /admin/v1/templates/merge-suggestions` is suggest-only: nothing in the repo applies a merge (no endpoint, no CLI,
no catalog operation), so there is no merge to record a lineage link from. Followup: when an apply operation is
built, it should write the retired template's lineage into an alias table (`lineage_alias(from_lineage, to_lineage)`) and have the Sentry
fingerprint resolve through it. Not wired now; adding the table with no writer would be dead schema.
