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
            similarity_threshold: 0.4,
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
    let matches = template
        .iter()
        .zip(tokens)
        .filter(|(t, l)| {
            t == l && *t != WILDCARD && !mask::is_placeholder(t) && !mask::is_placeholder(l)
        })
        .count();
    matches as f64 / template.len() as f64
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
        assert_eq!(m.params, vec!["4711", "u99", "812"]);
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
        // Same length and mostly the same tokens, differing in one static word.
        let a = d.add_line("connection to primary database succeeded", 0).unwrap();
        let b = d.add_line("connection to replica database succeeded", 0).unwrap();

        assert_eq!(a.template_id, b.template_id, "should merge, not fork");
        let template = d.template(a.template_id).unwrap();
        assert_eq!(template.text(), "connection to <*> database succeeded");
        // The differing word is now recoverable as a parameter.
        assert_eq!(b.params, vec!["replica"]);
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
