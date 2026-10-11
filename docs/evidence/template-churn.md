# Template churn across 10 releases of one service

Plane LOG-65 (measurement) feeding LOG-18 ("Template lineage meets the churn bar", gate G2).
Reproduce: `scripts/template-churn.sh [temporal-repo] [workdir]` (clean values);
`CHURN_VALUES=noisy scripts/template-churn.sh ...` for the over-splitting variant.
Code: `scripts/churn-extract.py`, `crates/logless-cli/src/bin/churn.rs`, `scripts/churn-analyze.py`.

## Verdict

**Partial. G2 is not closed by this evidence.**

- **There is no numeric churn bar in the repo or in Plane LOG-18.** LOG-18's description is only
  "Exit criterion for gate G2. Verify with evidence before closing." The only bar written down is the
  kill criterion in `docs/platform-thesis.md` section 11: *"Churn swamps genuine novelty and lineage looks
  unsolvable."* I judge against that, and propose a numeric bar below for the owner to accept or change.
- **Against the kill criterion: not killed, not passed.** Across 9 release transitions, 109 of 528
  new-or-widened template ids (20.6%) came from source edits or re-tagged statements, not from new
  functionality. Worst release: 30.8% of its new ids (v1.27.0). They carried 1,204 of 71,196 events
  (1.7%, worst release 3.9%). So genuine novelty is not swamped: 79% of new ids are statements that really
  are new. But about one in five novelty alerts per release would be false, and the miner has no lineage
  step today.
- **A naive lineage step is promising but not shippable.** Token edit-similarity >= 0.6 against retired
  templates linked 35/35 edit-origin ids, 31 to the correct predecessor, but also falsely linked 100 of 418
  genuinely-new ids (24%). It was not tuned and is not in the product.
- **Proposed bar (mine, not from the repo):** false-novelty ids <= 5% of new ids per release, with a
  lineage link precision >= 95%. Today: 20.6% overall, 31% worst; precision 31/(31+100)=24% on the naive linker.

## Data: real statements, rendered lines (read this before trusting any number)

- **Service:** Temporal server (`~/projects/__externals/temporal`, full non-shallow clone), tags
  v1.20.0 .. v1.29.0: ten consecutive minor releases. That is 10 releases and 9 transitions (v1.20.0 seeds
  the catalog).
- **No real per-release log files exist on this machine.** The LogHub 2k samples are not on disk, are
  single-snapshot corpora, and carry no release axis. Real production logs of one service across 10 releases
  are not publicly available. So:
- **Real:** the log statements. `logger.Info/Warn/Error("literal", tag.X(...))` calls parsed with a regex
  from `git archive <tag>` (non-test `.go` under service/, common/, tools/, temporal/, cmd/), including
  message literals and tag names. The tag keys come from each release's `common/log/tag` definitions.
- **Rendered (synthetic):** the lines. Each statement is emitted 3..20 times (count fixed by a hash of the
  statement, so an unchanged statement has identical volume in every release), as
  `<timestamp> <LEVEL> <message> key=value ...`, with values from a seeded RNG. Event volumes are therefore
  NOT production traffic; every "events" figure below is a count of rendered lines.
- **Why this is acceptable:** the question is how often source edits change template identity. That is
  decided by the statement set, which is real; volumes only weight it. **Why it is not enough:** real traffic
  weights hot statements, error values vary in ways the render does not model, and statements with
  `fmt.Sprintf`/non-literal first arguments, `Debug`, and non-`logger` receivers are not captured
  (statements per release: 463 -> 880; the grep for all `logger.Info/Warn/Error(` calls finds more,
  e.g. 741 vs 681 at v1.25.0 for two directories, so coverage is partial).
- **Value rendering matters.** `clean` (the main run): numbers, uuids and one-token error codes, all shapes
  the masker handles: baseline catalog 444 templates for 463 statements (1:1). `noisy`: plain words and
  multi-word quoted errors: Drain then over-splits 5x (2,350 templates for 463 statements) regardless of
  any release change. The clean run is the miner's best case, so the churn figures are a floor for
  edit-driven churn but the noisy run shows baseline fragmentation is the larger problem when values are
  not maskable.
- **Miner:** `logless_core::drain::Drain` with `DrainConfig::default()` (depth 4, similarity 0.9), one
  instance carried across all releases (equivalent to restoring the same catalog). "Global id" is
  `Template::fingerprint()` (FNV over template text); the per-agent counter `id` is stable but widening
  changes the fingerprint.
- **Ground truth for edits** is source-level: a statement is *unchanged* if (level, message, tags) is
  identical in the previous release; *retagged* if the message is the same but the tags differ; *edited* if
  new and a removed statement in the same file and level has token similarity >= 0.5; otherwise *added*.
  A new template id is assigned the majority category of the lines that landed on it. This pairing is a
  heuristic, so the edited/added split carries error.

## Results (clean values, the main run)

Counts are exact for this rendered input. "active" = templates that received events in the release;
"events on changed id" = events landing on a template minted or widened in the release (includes
genuine new statements); "coverage retained" = 1 - that / events.

| release | statements | lines | catalog | active | new | widened | events on changed id | coverage retained |
|---|---|---|---|---|---|---|---|---|
| v1.20.0 (baseline) | 463 | 5309 | 444 | 444 | 444 (seed) | - | - | - |
| v1.21.0 | 521 | 5963 | 508 | 472 | 64 | 0 | 964 / 5963 | 0.8383 |
| v1.22.0 | 535 | 6103 | 536 | 481 | 28 | 1 | 324 / 6103 | 0.9469 |
| v1.23.0 | 566 | 6428 | 594 | 507 | 58 | 0 | 659 / 6428 | 0.8975 |
| v1.24.0 | 624 | 7095 | 672 | 564 | 78 | 3 | 894 / 7095 | 0.8740 |
| v1.25.0 | 681 | 7814 | 761 | 617 | 89 | 1 | 1079 / 7814 | 0.8619 |
| v1.26.0 | 697 | 8017 | 778 | 631 | 17 | 0 | 218 / 8017 | 0.9728 |
| v1.27.0 | 837 | 9689 | 892 | 679 | 114 | 3 | 2413 / 9689 | 0.7510 |
| v1.28.0 | 861 | 9950 | 927 | 692 | 35 | 1 | 432 / 9950 | 0.9566 |
| v1.29.0 | 880 | 10137 | 961 | 710 | 34 | 2 | 417 / 10137 | 0.9589 |

| release | stmts unchanged | retagged | edited | added | removed | new ids from edited | from added | from retagged | prev-active ids whose fingerprint is lost |
|---|---|---|---|---|---|---|---|---|---|
| v1.21.0 | 446 | 2 | 2 | 71 | 17 | 2 | 61 | 0 | 0 / 444 |
| v1.22.0 | 504 | 3 | 2 | 26 | 17 | 1 | 24 | 3 | 1 / 472 |
| v1.23.0 | 500 | 9 | 4 | 53 | 35 | 2 | 48 | 8 | 0 / 481 |
| v1.24.0 | 536 | 15 | 10 | 63 | 30 | 10 | 61 | 7 | 3 / 507 |
| v1.25.0 | 583 | 15 | 7 | 76 | 41 | 7 | 71 | 11 | 1 / 564 |
| v1.26.0 | 678 | 0 | 1 | 18 | 3 | 1 | 16 | 0 | 0 / 617 |
| v1.27.0 | 623 | 32 | 8 | 174 | 74 | 8 | 81 | 25 | 3 / 631 |
| v1.28.0 | 817 | 13 | 1 | 30 | 20 | 1 | 27 | 7 | 1 / 679 |
| v1.29.0 | 843 | 4 | 3 | 30 | 18 | 3 | 29 | 2 | 2 / 692 |

| release | false-novelty ids (edited + retagged + widened) | share of new+widened ids | events on them | share of events |
|---|---|---|---|---|
| v1.21.0 | 2 | 0.031 | 31 | 0.0052 |
| v1.22.0 | 5 | 0.172 | 49 | 0.0080 |
| v1.23.0 | 10 | 0.172 | 98 | 0.0152 |
| v1.24.0 | 20 | 0.247 | 221 | 0.0311 |
| v1.25.0 | 19 | 0.211 | 237 | 0.0303 |
| v1.26.0 | 1 | 0.059 | 10 | 0.0012 |
| v1.27.0 | 36 | 0.308 | 376 | 0.0388 |
| v1.28.0 | 9 | 0.250 | 91 | 0.0091 |
| v1.29.0 | 7 | 0.194 | 91 | 0.0090 |

| release | edited-origin new ids | linked by edit-distance (>=0.6) | linked correctly | genuinely-added new ids | of which falsely linked |
|---|---|---|---|---|---|
| v1.21.0 | 2 | 2 | 2 | 61 | 9 |
| v1.22.0 | 1 | 1 | 1 | 24 | 3 |
| v1.23.0 | 2 | 2 | 2 | 48 | 16 |
| v1.24.0 | 10 | 10 | 10 | 61 | 12 |
| v1.25.0 | 7 | 7 | 7 | 71 | 26 |
| v1.26.0 | 1 | 1 | 1 | 16 | 3 |
| v1.27.0 | 8 | 8 | 7 | 81 | 17 |
| v1.28.0 | 1 | 1 | 1 | 27 | 6 |
| v1.29.0 | 3 | 3 | 0 | 29 | 8 |


Totals, 9 transitions: 517 new ids (418 from added statements, 35 from edited, 63 from re-tagged, 1 other),
11 widened ids (fingerprint changed in place), 109 false-novelty ids, 71,196 rendered events, 7,400 on
new/widened ids, 1,204 on false-novelty ids. Mean shape coverage retained 0.8953, minimum 0.7510 (v1.27.0,
which added 174 statements). Untemplated lines: 0.

Reading the numbers:
- "Shape coverage retained" is dominated by genuine additions: Temporal grows from 463 to 880 statements, so
  a lot of mass lands on new shapes legitimately. The churn-attributable share is the false-novelty table.
- Widening is rare here (11 total) because clean values leave nothing for Drain to widen; in `noisy` it is
  194. Widening changes the fingerprint, so a Sentry issue splits at that point.
- "prev-active ids whose fingerprint is lost" (0..3 per release, 126 only in the noisy run for v1.27.0)
  counts templates that were live last release and whose fingerprint no longer exists in the catalog.

## Variant: noisy values (baseline fragmentation, not release churn)

Catalog 2,350 at the baseline (463 statements). Over 9 transitions: 3,395 new ids, 194 widened, 8,415 of
71,196 events on new/widened ids, 791 false-novelty ids (worst release 38.3% of new ids), 1,773 false-novelty
events (worst release 6.1%). Mean coverage retained 0.8826, minimum 0.6772. Edit-distance linking in this
variant: 209/224 edit-origin ids linked, 45 correctly, 1,050 of 2,339 added ids falsely linked: unusable.
Take-away: lineage cannot be judged until ids are stable inside a release; unmaskable word values and
multi-word error strings are the dominant instability, not code changes.

## Not measured

- Real production lines or real traffic weights; messages built with `Sprintf`; Debug-level statements;
  statements outside logger calls; multi-line stack traces.
- Lineage on the real ingest path (there is none in `logless-core`), threshold tuning for the linker, and a
  second service. One service, one language, one author culture (Temporal) is not a fleet.
- Daily-deploy cadence: these are minor releases months apart; patch releases (v1.x.y) were not replayed.
