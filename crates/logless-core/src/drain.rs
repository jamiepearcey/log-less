//! Drain — online log template mining.
//!
//! A fixed-depth parse tree, per `docs/architecture.md` §3. For each line:
//!
//! 1. mask variable tokens ([`crate::mask`]);
//! 2. bucket by token count;
//! 3. descend `depth` levels keyed by the leading tokens;
//! 4. at the leaf, score candidate templates by the fraction of positions whose
//!    tokens match; above the threshold, merge (differing positions become
//!    `<*>`), otherwise start a new template.
//!
//! Cost is O(depth) per line — no pairwise comparison against every known
//! template, which is what makes this viable at line rate and why it was chosen
//! over MinHash/LSH clustering.
//!
//! **Template ids are stable and exact.** The same line always lands on the same
//! id, for the lifetime of the store. Everything downstream depends on that:
//! novelty detection ("an id never seen before"), rate baselines per template,
//! and Sentry `fingerprint` grouping. Clustering approaches drift, ids move, and
//! all three quietly break.

use std::collections::HashMap;

use crate::mask;

/// Token used for a position that varies between merged lines.
pub const WILDCARD: &str = "<*>";

#[derive(Debug, Clone, PartialEq)]
pub struct DrainConfig {
    /// Parse-tree depth, counted as Drain3 counts it: the root and the
    /// token-count node are included, so `depth = 4` means **two** levels of
    /// token-keyed descent. Deeper is more selective but fragments templates
    /// whose leading tokens vary — at four token levels,
    /// `connection to primary database succeeded` and
    /// `connection to replica database succeeded` can never meet.
    pub depth: usize,
    /// Cap on distinct children per node; beyond it, lines fall into the
    /// wildcard branch rather than growing the tree without bound.
    pub max_children: usize,
    /// Fraction of matching positions required to join an existing template.
    /// Fraction of *comparable* positions that must match for a line to join an
    /// existing template.
    ///
    /// Measured against the 11 LogHub corpora and their published ground-truth
    /// template counts, comparing on the parsed `Content` field — which is what
    /// the ground truth describes. Scored by mean absolute log-ratio
    /// ("dispersion"), because a geometric mean can sit at 1.00 while half the
    /// datasets over-split and half under-merge and the errors cancel:
    ///
    /// | threshold | geo-mean | dispersion |
    /// |---|---|---|
    /// | 0.4 | 0.91x | 0.138 |
    /// | 0.8 | 0.96x | 0.124 |
    /// | **0.9** | **1.00x** | **0.098** |
    /// | 0.93–1.0 | 1.02x | 0.094 (plateau) |
    ///
    /// 0.9 rather than the flat optimum at 0.93+: on this corpus they are worth
    /// 0.004, and staying below the plateau keeps the wildcard-widening path
    /// available for variability the masker misses — hostnames and thread names
    /// that real deployments have and these corpora do not.
    ///
    /// Over-merging is the more dangerous direction, because distinct error
    /// shapes collapsing into one template means one Sentry fingerprint for
    /// unrelated failures and a dedupe window suppressing errors that are not
    /// duplicates. Over-splitting only costs dictionary entries.
    /// `scripts/loghub.sh` reproduces the sweep.
    pub similarity_threshold: f64,
    /// Hard cap on distinct templates. Past it, lines are reported untemplated
    /// rather than growing memory without limit — a corpus of pure noise must
    /// degrade, not OOM.
    pub max_templates: usize,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            depth: 4,
            max_children: 100,
            similarity_threshold: 0.9,
            max_templates: 10_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Template {
    pub id: u64,
    pub tokens: Vec<String>,
    pub count: u64,
    pub first_seen_unix_secs: u64,
    pub last_seen_unix_secs: u64,
}

impl Template {
    /// Human-readable template text.
    pub fn text(&self) -> String {
        self.tokens.join(" ")
    }

    /// Content-derived identity, stable across agents.
    ///
    /// [`Template::id`] is a per-agent counter minted in arrival order, which
    /// is right for local bookkeeping and wrong for anything a *fleet* shares.
    /// Two agents seeing the same log shape assign it different numbers, so a
    /// Sentry fingerprint built from the id splits one issue across nodes and
    /// merges unrelated errors that happen to share an index — the exact
    /// opposite of the deterministic grouping this feature promises.
    ///
    /// Derived from the masked token sequence, so every agent watching the
    /// same service agrees without coordinating. Widening a template does
    /// change it, which starts a new Sentry issue at that point; that is the
    /// price of not needing a central registry, and it is rare compared with
    /// the alternative, which is wrong on every node from the start.
    pub fn fingerprint(&self) -> u64 {
        fingerprint_of(&self.text())
    }
}

/// FNV-1a over the template text. Not [`std::hash::DefaultHasher`], which is
/// explicitly not stable across builds or processes — the one property this
/// needs.
pub fn fingerprint_of(template_text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in template_text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

#[derive(Debug, PartialEq)]
pub struct Match {
    pub template_id: u64,
    /// Original text at each variable position, in order.
    pub params: Vec<String>,
    /// First time this template has ever been seen — the novelty signal.
    pub is_new: bool,
}

#[derive(Debug, Default)]
struct Node {
    children: HashMap<String, Node>,
    /// Template ids at this leaf.
    templates: Vec<u64>,
}

pub struct Drain {
    config: DrainConfig,
    /// Keyed by token count, then by leading tokens.
    roots: HashMap<usize, Node>,
    templates: HashMap<u64, Template>,
    next_id: u64,
    /// Lines rejected because `max_templates` was reached.
    pub untemplated: u64,
    pub lines_seen: u64,
    // Scratch buffers, reused across calls to keep the hot path allocation-light.
    masked: Vec<String>,
    raw: Vec<String>,
}

impl Default for Drain {
    fn default() -> Self {
        Self::new(DrainConfig::default())
    }
}

impl Drain {
    /// Levels of token-keyed descent. See [`DrainConfig::depth`].
    fn token_levels(&self) -> usize {
        self.config.depth.saturating_sub(2).max(1)
    }

    pub fn new(config: DrainConfig) -> Self {
        Self {
            config,
            roots: HashMap::new(),
            templates: HashMap::new(),
            next_id: 1,
            untemplated: 0,
            lines_seen: 0,
            masked: Vec::new(),
            raw: Vec::new(),
        }
    }

    /// Rebuild from persisted templates, preserving their ids.
    ///
    /// Without this, a restart would reassign ids and every downstream baseline
    /// and Sentry fingerprint would shift.
    pub fn restore(config: DrainConfig, templates: Vec<Template>) -> Self {
        let mut drain = Self::new(config);
        for template in templates {
            drain.next_id = drain.next_id.max(template.id + 1);
            let key: Vec<String> = template.tokens.clone();
            drain.insert_into_tree(&key, template.id);
            drain.templates.insert(template.id, template);
        }
        drain
    }

    pub fn templates(&self) -> impl Iterator<Item = &Template> {
        self.templates.values()
    }

    pub fn template(&self, id: u64) -> Option<&Template> {
        self.templates.get(&id)
    }

    pub fn template_count(&self) -> usize {
        self.templates.len()
    }

    /// Match a line, creating or updating a template.
    ///
    /// Returns `None` only when the template cap is reached and the line does
    /// not match anything already known.
    pub fn add_line(&mut self, line: &str, now_unix_secs: u64) -> Option<Match> {
        self.lines_seen += 1;
        let (mut masked, mut raw) = (std::mem::take(&mut self.masked), std::mem::take(&mut self.raw));
        mask::mask_line(line, &mut masked, &mut raw);
        let result = self.match_masked(&masked, &raw, now_unix_secs);
        self.masked = masked;
        self.raw = raw;
        result
    }

    fn match_masked(
        &mut self,
        masked: &[String],
        raw: &[String],
        now_unix_secs: u64,
    ) -> Option<Match> {
        if masked.is_empty() {
            return None;
        }

        let candidates = self.leaf_templates(masked);
        let best = candidates
            .iter()
            .filter_map(|id| self.templates.get(id).map(|t| (*id, similarity(&t.tokens, masked))))
            .filter(|(_, score)| *score >= self.config.similarity_threshold)
            .max_by(|a, b| a.1.total_cmp(&b.1));

        if let Some((id, _)) = best {
            let template = self.templates.get_mut(&id).expect("candidate exists");
            merge_into(&mut template.tokens, masked);
            template.count += 1;
            template.last_seen_unix_secs = now_unix_secs;
            let params = params_for(&template.tokens, masked, raw);
            return Some(Match {
                template_id: id,
                params,
                is_new: false,
            });
        }

        if self.templates.len() >= self.config.max_templates {
            self.untemplated += 1;
            return None;
        }

        let id = self.next_id;
        self.next_id += 1;
        let tokens = masked.to_vec();
        let params = params_for(&tokens, masked, raw);
        self.insert_into_tree(&tokens, id);
        self.templates.insert(
            id,
            Template {
                id,
                tokens,
                count: 1,
                first_seen_unix_secs: now_unix_secs,
                last_seen_unix_secs: now_unix_secs,
            },
        );
        Some(Match {
            template_id: id,
            params,
            is_new: true,
        })
    }

    /// Descend to the leaf for these tokens, returning its template ids.
    fn leaf_templates(&self, tokens: &[String]) -> Vec<u64> {
        let Some(mut node) = self.roots.get(&tokens.len()) else {
            return Vec::new();
        };
        for token in tokens.iter().take(self.token_levels()) {
            let key = tree_key(token);
            node = match node.children.get(&key) {
                Some(n) => n,
                // Fall back to the wildcard branch: a line whose leading token
                // is novel still belongs with structurally similar lines.
                None => match node.children.get(WILDCARD) {
                    Some(n) => n,
                    None => return node.templates.clone(),
                },
            };
        }
        node.templates.clone()
    }

    fn insert_into_tree(&mut self, tokens: &[String], id: u64) {
        let max_children = self.config.max_children;
        let depth = self.token_levels();
        let mut node = self.roots.entry(tokens.len()).or_default();
        for token in tokens.iter().take(depth) {
            let mut key = tree_key(token);
            // Bound the fan-out: past the cap everything shares one branch
            // rather than the tree growing with the data.
            if !node.children.contains_key(&key) && node.children.len() >= max_children {
                key = WILDCARD.to_string();
            }
            node = node.children.entry(key).or_default();
        }
        node.templates.push(id);
    }
}

/// Tokens containing a placeholder key the wildcard branch — otherwise every
/// distinct value would spawn a branch and the tree would be the data.
fn tree_key(token: &str) -> String {
    if mask::is_placeholder(token) {
        WILDCARD.to_string()
    } else {
        token.to_string()
    }
}

/// Fraction of positions holding the same token.
///
/// **Variable positions never count as matches**, whether wildcard or typed
/// placeholder. Two lines both carrying a `<HEX>` in slot 2 agree on nothing —
/// that slot is variable by definition. Counting it inflates similarity by
/// whatever fraction of the line is data, and with a common prefix
/// (`service=… trace_id=… …`) that alone can clear the threshold and merge
/// genuinely unrelated shapes into one template. Observed in testing: four
/// distinct debug messages collapsed to a single id, which would have broken
/// novelty detection and flow hashing downstream.
///
/// Measuring agreement on the *static* part is what the score is for.
fn similarity(template: &[String], tokens: &[String]) -> f64 {
    if template.len() != tokens.len() {
        return 0.0;
    }
    if template.is_empty() {
        return 1.0;
    }
    // Score over *comparable* positions only, not over every token.
    //
    // Variable positions carry no evidence either way: two lines that both say
    // `id=<NUM>` there are neither more nor less alike for it. Dividing by the
    // total instead makes the score depend on how much of the line happens to
    // be masked, so improving the masking heuristics silently pushes shapes
    // below the threshold and splits templates that used to merge. (Observed:
    // masking a leading `[component]` turned one template into a hundred.)
    let mut comparable = 0usize;
    let mut matches = 0usize;
    for (t, l) in template.iter().zip(tokens) {
        if *t == WILDCARD || mask::is_placeholder(t) || mask::is_placeholder(l) {
            continue;
        }
        comparable += 1;
        if t == l {
            matches += 1;
        }
    }
    if comparable == 0 {
        // Same length, same tree path, and nothing but variables: one shape.
        return 1.0;
    }
    // Tried and rejected: allowing a single differing position outright,
    // regardless of length, on the reasoning that one-in-five is the same
    // evidence as one-in-twenty. Measured, it over-merges — dispersion across
    // the LogHub corpora went from 0.098 to 0.128, worse than the entire gain
    // from tuning this threshold, and it made the threshold itself irrelevant
    // (0.8, 0.9 and 0.95 all produced identical results).
    //
    // The consequence, which is deliberate: on a short line, merging depends on
    // the masker rather than on wildcard widening. A five-token template at 0.9
    // needs all five comparable positions to match. That costs extra dictionary
    // entries, which is the cheap error; the expensive one is distinct error
    // shapes sharing a fingerprint.
    matches as f64 / comparable as f64
}

/// Widen a template to cover a new line: differing positions become wildcards.
fn merge_into(template: &mut [String], tokens: &[String]) {
    for (slot, token) in template.iter_mut().zip(tokens) {
        if slot != token {
            slot.clear();
            slot.push_str(WILDCARD);
        }
    }
}

/// Parameters are the raw text at every position the template treats as
/// variable — both masked tokens and wildcards created by merging.
fn params_for(template: &[String], masked: &[String], raw: &[String]) -> Vec<String> {
    template
        .iter()
        .enumerate()
        .filter(|(i, t)| {
            *t == WILDCARD || masked.get(*i).is_some_and(|m| mask::is_placeholder(m))
        })
        .filter_map(|(i, _)| raw.get(i).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain() -> Drain {
        Drain::default()
    }

    #[test]
    fn identical_shapes_collapse_to_one_template() {
        let mut d = drain();
        let ids: Vec<_> = (0..100)
            .map(|i| {
                d.add_line(
                    &format!("[api] handled request id={i} user=u{i} latency_ms={}", i * 3),
                    0,
                )
                .unwrap()
                .template_id
            })
            .collect();

        assert_eq!(d.template_count(), 1, "one shape must be one template");
        assert!(ids.iter().all(|id| *id == ids[0]));
        assert_eq!(d.templates().next().unwrap().count, 100);
    }

    #[test]
    fn different_shapes_stay_separate() {
        let mut d = drain();
        let a = d.add_line("user 42 logged in from 10.0.0.1", 0).unwrap();
        let b = d.add_line("payment 99 failed for order 7", 0).unwrap();
        let c = d.add_line("user 77 logged in from 10.0.0.9", 0).unwrap();

        assert_ne!(a.template_id, b.template_id);
        assert_eq!(a.template_id, c.template_id);
        assert_eq!(d.template_count(), 2);
    }

    #[test]
    fn extracts_parameters_in_order() {
        let mut d = drain();
        let m = d
            .add_line("[api] handled request id=4711 user=u99 latency_ms=812", 0)
            .unwrap();
        // The component is a variable position, so it is a parameter too —
        // which is what lets an aggregate report which components hit a shape.
        assert_eq!(m.params, vec!["[api]", "4711", "u99", "812"]);
        assert!(m.is_new);
    }

    #[test]
    fn novelty_is_reported_once_and_only_once() {
        let mut d = drain();
        assert!(d.add_line("cache warmed in 12ms", 0).unwrap().is_new);
        assert!(!d.add_line("cache warmed in 87ms", 0).unwrap().is_new);
        assert!(!d.add_line("cache warmed in 3ms", 0).unwrap().is_new);
    }

    #[test]
    fn merging_widens_a_static_position_to_a_wildcard() {
        let mut d = drain();
        // Long enough that one differing word is inside the threshold: 13
        // comparable positions, one of which differs, is 0.92 >= 0.9.
        let a = d
            .add_line("connection to primary database succeeded after retry from pool worker on node alpha", 0)
            .unwrap();
        let b = d
            .add_line("connection to replica database succeeded after retry from pool worker on node alpha", 0)
            .unwrap();

        assert_eq!(a.template_id, b.template_id, "should merge, not fork");
        let template = d.template(a.template_id).unwrap();
        assert!(template.text().contains("connection to <*> database"), "{}", template.text());
        // The differing word is now recoverable as a parameter.
        assert!(b.params.contains(&"replica".to_string()), "{:?}", b.params);
    }

    #[test]
    fn a_short_line_with_a_differing_word_forks_rather_than_merging() {
        // The deliberate consequence of a high threshold: five tokens, one
        // differing, is 0.8 and below the bar. Short lines rely on the masker,
        // not on wildcard widening. Measured across the LogHub corpora, the
        // alternative — allowing one difference regardless of length —
        // over-merges and costs more than tuning the threshold ever gained.
        let mut d = drain();
        let a = d.add_line("connection to primary database succeeded", 0).unwrap();
        let b = d.add_line("connection to replica database succeeded", 0).unwrap();
        assert_ne!(a.template_id, b.template_id);
    }

    #[test]
    fn ids_are_stable_across_restore() {
        let mut d = drain();
        let first = d.add_line("user 42 logged in from 10.0.0.1", 0).unwrap();
        let second = d.add_line("payment 99 failed for order 7", 0).unwrap();
        let persisted: Vec<_> = d.templates().cloned().collect();

        // Restart: rebuild from what the catalog held.
        let mut restored = Drain::restore(DrainConfig::default(), persisted);
        let again = restored.add_line("user 77 logged in from 10.0.0.9", 0).unwrap();
        assert_eq!(again.template_id, first.template_id, "id must survive restart");
        assert!(!again.is_new, "a restored template is not novel");

        // New templates continue after the restored ids, never colliding.
        let fresh = restored.add_line("entirely different message here", 0).unwrap();
        assert!(fresh.template_id > second.template_id);
    }

    #[test]
    fn respects_the_template_cap_instead_of_growing_without_bound() {
        let config = DrainConfig {
            max_templates: 10,
            ..Default::default()
        };
        let mut d = Drain::new(config);
        // Every line a different shape, so nothing merges.
        for i in 0..100 {
            let line: String = (0..=i % 30).map(|w| format!("w{i}x{w} ")).collect();
            d.add_line(&line, 0);
        }
        assert!(d.template_count() <= 10, "cap must hold");
        assert!(d.untemplated > 0, "overflow must be counted, not hidden");
        assert_eq!(d.lines_seen, 100);
    }

    #[test]
    fn empty_lines_produce_no_template() {
        let mut d = drain();
        assert!(d.add_line("", 0).is_none());
        assert!(d.add_line("   ", 0).is_none());
        assert_eq!(d.template_count(), 0);
    }

    #[test]
    fn a_shared_variable_prefix_does_not_merge_unrelated_shapes() {
        // Every line here starts `service=… trace_id=…`. If those variable slots
        // counted toward similarity, all four would collapse into one template.
        let mut d = drain();
        let lines = [
            "service=api trace_id=0123456789abcdef0123456789abcdef received request path=/v1/orders/7",
            "service=api trace_id=0123456789abcdef0123456789abcdef resolved user user_id=u2182",
            "service=api trace_id=0123456789abcdef0123456789abcdef querying database rows=149",
            "service=api trace_id=0123456789abcdef0123456789abcdef applying discount pct=26",
        ];
        let ids: Vec<_> = lines
            .iter()
            .map(|l| d.add_line(l, 0).unwrap().template_id)
            .collect();

        let mut unique = ids.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 4, "four distinct shapes, four templates: {ids:?}");

        // And the same shapes with different data still collapse correctly.
        let repeat = d
            .add_line(
                "service=api trace_id=ffffffffffffffffffffffffffffffff querying database rows=999",
                0,
            )
            .unwrap();
        assert_eq!(repeat.template_id, ids[2]);
        assert!(!repeat.is_new);
    }

    #[test]
    fn a_realistic_mixed_corpus_yields_few_templates() {
        // The value claim: heterogeneous production logs collapse to a small,
        // stable set. If this number drifts up, compression and anomaly
        // detection both degrade.
        let mut d = drain();
        let services = ["api", "worker", "auth"];
        for i in 0..3000 {
            let s = services[i % 3];
            match i % 5 {
                0 => d.add_line(&format!("[{s}] handled request id={i} latency_ms={}", i % 900), 0),
                1 => d.add_line(&format!("[{s}] cache miss for key user:{i}"), 0),
                2 => d.add_line(&format!("[{s}] connection to 10.0.0.{} established", i % 255), 0),
                3 => d.add_line(&format!("[{s}] retry {} of 5 after timeout", i % 5), 0),
                _ => d.add_line(&format!("[{s}] wrote {} bytes to /var/data/file{i}.bin", i * 7), 0),
            };
        }
        assert!(
            d.template_count() <= 20,
            "expected a handful of templates, got {}",
            d.template_count()
        );
        assert_eq!(d.untemplated, 0);
    }
}
