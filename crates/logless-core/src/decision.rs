//! An optional decision model that answers typed questions about log clusters.
//!
//! A decision model (Laya, Jev, or anything with the same request shape) is handed a state and a question with named options,
//! and returns a probability for each option. It never writes text. Off by default: nothing here runs unless `[decision]` has
//! a `url`.
//!
//! # What it is used for
//!
//! Only what a measurement supported (`docs/decision-model.md`): **suggesting that two templates are the same failure**
//! ([`suggest_merges`]). Drain over-splits a failure whose wording varies, and a person merging them by hand is slow. Laya's
//! base English checkpoint, zero-shot, separated reworded repeats from similar-looking different failures with AUC 0.82 on 50
//! hand-written pairs, and at 0.8 confidence 12 of 13 suggested merges were right. The same model did **not** triage
//! clusters (52% on five classes, mostly answering "bug") or judge whether a flood is worth a page (chance), so it is not
//! used for those, and nothing it says changes what is sent, kept or alerted: a suggestion is a suggestion.
//!
//! # The request
//!
//! `POST <url>` with `{"state": "...", "questions": {"q": {"type": "choice", "instructions": "...", "criteria": {"option":
//! "what it means"}}}}`, answered by `{"answers": {"q": {"type": "choice", "choice": "...", "probabilities": {"option": 0.93}}}}`.
//! That is the shape `system_one(state, questions)` takes and returns in Laya's inference code; a hosted model with the same
//! shape works too. A provider that is down, slow or answers something else makes the call fail, and the caller reports it.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::drain::Template;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DecisionConfig {
    /// Where to send questions. Absent means off.
    #[serde(default)]
    pub url: Option<String>,
    /// Sent as `Authorization: Bearer`.
    #[serde(default)]
    pub token: Option<String>,
    /// A merge is suggested only at or above this probability that two templates are the same failure.
    #[serde(default = "default_min_confidence")]
    pub min_confidence: f64,
    /// At most this many candidate pairs are put to the model per request.
    #[serde(default = "default_max_pairs")]
    pub max_pairs: usize,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_min_confidence() -> f64 {
    0.8
}
fn default_max_pairs() -> usize {
    60
}
fn default_timeout_secs() -> u64 {
    10
}

impl Default for DecisionConfig {
    fn default() -> Self {
        Self { url: None, token: None, min_confidence: default_min_confidence(), max_pairs: default_max_pairs(), timeout_secs: default_timeout_secs() }
    }
}

impl DecisionConfig {
    pub fn enabled(&self) -> bool {
        self.url.as_deref().is_some_and(|u| !u.trim().is_empty())
    }

    pub fn problem(&self) -> Option<String> {
        if let Some(url) = self.url.as_deref().filter(|u| !u.trim().is_empty()) {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Some("[decision] url starts with http:// or https://".into());
            }
        }
        if !(0.5..=1.0).contains(&self.min_confidence) {
            return Some("[decision] min_confidence is 0.5 to 1.0".into());
        }
        if self.max_pairs == 0 || self.max_pairs > 500 {
            return Some("[decision] max_pairs is 1 to 500".into());
        }
        None
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum DecisionError {
    Unreachable(String),
    BadAnswer(String),
}

impl std::fmt::Display for DecisionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(m) => write!(f, "the decision model could not be reached: {m}"),
            Self::BadAnswer(m) => write!(f, "the decision model answered something unexpected: {m}"),
        }
    }
}

/// Something that can answer a choice question with a probability per option.
pub trait DecisionProvider: Send + Sync {
    fn choose(&self, state: &str, instructions: &str, options: &[(&str, &str)]) -> Result<HashMap<String, f64>, DecisionError>;
}

/// A provider reached over HTTP with the request shape in the module docs.
pub struct HttpProvider {
    url: String,
    token: Option<String>,
    agent: ureq::Agent,
}

impl HttpProvider {
    pub fn new(cfg: &DecisionConfig) -> Option<Self> {
        let url = cfg.url.clone().filter(|u| !u.trim().is_empty())?;
        let agent = ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(cfg.timeout_secs.max(1)))).http_status_as_error(false).build().into();
        Some(Self { url, token: cfg.token.clone().filter(|t| !t.is_empty()), agent })
    }
}

impl DecisionProvider for HttpProvider {
    fn choose(&self, state: &str, instructions: &str, options: &[(&str, &str)]) -> Result<HashMap<String, f64>, DecisionError> {
        let criteria: serde_json::Map<String, Value> = options.iter().map(|(k, v)| ((*k).to_string(), json!(v))).collect();
        let body = json!({"state": state, "questions": {"q": {"type": "choice", "instructions": instructions, "criteria": criteria}}});
        let mut request = self.agent.post(&self.url).header("content-type", "application/json");
        if let Some(token) = &self.token {
            request = request.header("authorization", &format!("Bearer {token}"));
        }
        let mut response = request.send(serde_json::to_vec(&body).unwrap_or_default().as_slice()).map_err(|e| DecisionError::Unreachable(e.to_string()))?;
        if !response.status().is_success() {
            return Err(DecisionError::Unreachable(format!("HTTP {}", response.status().as_u16())));
        }
        let value: Value = crate::sync::read_json(response.body_mut()).ok_or_else(|| DecisionError::BadAnswer("not JSON".into()))?;
        parse_probabilities(&value, options)
    }
}

fn parse_probabilities(value: &Value, options: &[(&str, &str)]) -> Result<HashMap<String, f64>, DecisionError> {
    let probs = value
        .pointer("/answers/q/probabilities")
        .and_then(Value::as_object)
        .ok_or_else(|| DecisionError::BadAnswer("no answers.q.probabilities".into()))?;
    let mut out = HashMap::new();
    for (key, _) in options {
        let p = probs.get(*key).and_then(Value::as_f64).filter(|p| (0.0..=1.0).contains(p)).ok_or_else(|| DecisionError::BadAnswer(format!("no probability for \"{key}\"")))?;
        out.insert((*key).to_string(), p);
    }
    Ok(out)
}

/// P(the two texts are the same underlying failure).
pub fn same_failure(provider: &dyn DecisionProvider, a: &str, b: &str) -> Result<f64, DecisionError> {
    let probs = provider.choose(
        &format!("A: {a}\nB: {b}"),
        "Are A and B the same underlying failure?",
        &[
            ("same", "the same underlying failure worded differently or with different ids, hosts or numbers"),
            ("different", "different failures, even if the wording looks similar"),
        ],
    )?;
    Ok(probs.get("same").copied().unwrap_or(0.0))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Side {
    pub id: u64,
    pub text: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MergeSuggestion {
    pub a: Side,
    pub b: Side,
    pub p_same: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MergeReport {
    /// Candidate pairs put to the model.
    pub checked: usize,
    /// Candidates left unasked because of `max_pairs` or the deadline.
    pub skipped: usize,
    pub suggestions: Vec<MergeSuggestion>,
    /// Calls that failed (the first error is in `error`).
    pub failed: usize,
    pub error: Option<String>,
}

fn shape(t: &Template) -> BTreeSet<String> {
    t.text()
        .to_lowercase()
        .split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '<' || c == '>'))
        .filter(|w| !w.is_empty() && !w.chars().all(|c| c.is_ascii_digit()))
        .map(str::to_string)
        .collect()
}

/// Candidate pairs, most alike first: pairs of templates sharing at least 10% of their words. Deliberately generous: wording
/// that changes between two reports of one failure is the case the model is for, and the model makes the decision. On the 25
/// reworded pairs in the experiment this keeps 22 (88%); the three it drops share no words at all.
pub fn candidate_pairs(templates: &[Template], max: usize) -> (Vec<(usize, usize)>, usize) {
    let shapes: Vec<BTreeSet<String>> = templates.iter().map(shape).collect();
    let mut scored: Vec<(f64, usize, usize)> = Vec::new();
    for i in 0..templates.len() {
        for j in i + 1..templates.len() {
            let inter = shapes[i].intersection(&shapes[j]).count();
            let union = shapes[i].len() + shapes[j].len() - inter;
            if union == 0 {
                continue;
            }
            let jac = inter as f64 / union as f64;
            // Identical text is the same template already.
            if jac >= 0.1 && templates[i].text() != templates[j].text() {
                scored.push((jac, i, j));
            }
        }
    }
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    let skipped = scored.len().saturating_sub(max);
    (scored.into_iter().take(max).map(|(_, i, j)| (i, j)).collect(), skipped)
}

/// Puts the likeliest pairs among `templates` to the model and returns the ones it is `cfg.min_confidence` sure are one failure.
pub fn suggest_merges(provider: &dyn DecisionProvider, templates: &[Template], cfg: &DecisionConfig, deadline: Duration) -> MergeReport {
    let (pairs, mut skipped) = candidate_pairs(templates, cfg.max_pairs);
    let started = Instant::now();
    let (mut checked, mut failed, mut error) = (0usize, 0usize, None);
    let mut suggestions = Vec::new();
    let total = pairs.len();
    for (n, (i, j)) in pairs.into_iter().enumerate() {
        if started.elapsed() > deadline {
            skipped += total - n;
            break;
        }
        match same_failure(provider, &templates[i].text(), &templates[j].text()) {
            Ok(p) => {
                checked += 1;
                if p >= cfg.min_confidence {
                    let side = |t: &Template| Side { id: t.id, text: t.text(), count: t.count };
                    suggestions.push(MergeSuggestion { a: side(&templates[i]), b: side(&templates[j]), p_same: (p * 10_000.0).round() / 10_000.0 });
                }
            }
            Err(e) => {
                failed += 1;
                error.get_or_insert_with(|| e.to_string());
                // A provider that is down fails every call; do not wait out each one.
                if failed >= 3 && checked == 0 {
                    skipped += total - n - 1;
                    break;
                }
            }
        }
    }
    suggestions.sort_by(|a, b| b.p_same.partial_cmp(&a.p_same).unwrap_or(std::cmp::Ordering::Equal));
    MergeReport { checked, skipped, suggestions, failed, error }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(id: u64, text: &str, count: u64) -> Template {
        Template { id, tokens: text.split(' ').map(str::to_string).collect(), count, first_seen_unix_secs: 0, last_seen_unix_secs: 0, lineage_id: id }
    }

    /// Says "same" when both texts name the same product and the same symptom.
    struct Keyword;
    impl DecisionProvider for Keyword {
        fn choose(&self, state: &str, _: &str, _: &[(&str, &str)]) -> Result<HashMap<String, f64>, DecisionError> {
            let both = |k: &str| state.matches(k).count() >= 2;
            let same = (both("redis") && both("timeout")) || both("refused");
            let p = if same { 0.93 } else { 0.1 };
            Ok(HashMap::from([("same".to_string(), p), ("different".to_string(), 1.0 - p)]))
        }
    }

    struct Down;
    impl DecisionProvider for Down {
        fn choose(&self, _: &str, _: &str, _: &[(&str, &str)]) -> Result<HashMap<String, f64>, DecisionError> {
            Err(DecisionError::Unreachable("connection refused".into()))
        }
    }

    fn corpus() -> Vec<Template> {
        vec![
            t(1, "redis timeout after <*> ms on GET session", 900),
            t(2, "redis exceptions timeout reading from redis session", 40),
            t(3, "postgres timeout after <*> ms on SELECT orders", 70),
            t(4, "disk full no space left on device", 5),
            t(5, "connection refused payments gateway", 30),
            t(6, "ConnectException connection refused to payments gateway", 12),
        ]
    }

    #[test]
    fn candidates_are_pairs_that_share_words_most_alike_first_and_bounded() {
        let (pairs, skipped) = candidate_pairs(&corpus(), 100);
        assert!(pairs.contains(&(4, 5)), "{pairs:?}");
        assert!(pairs.iter().all(|(i, j)| i < j));
        assert!(!pairs.iter().any(|(i, j)| *i == 3 && *j != 4 || *j == 3 && *i != 4), "'disk full' only pairs with its own words: {pairs:?}");
        assert_eq!(0, skipped);
        let (two, skipped) = candidate_pairs(&corpus(), 2);
        assert_eq!(2, two.len());
        assert_eq!(pairs.len() - 2, skipped);
        let same = vec![t(1, "a b c", 1), t(2, "a b c", 1)];
        assert!(candidate_pairs(&same, 10).0.is_empty(), "identical text is one template already");
    }

    #[test]
    fn only_confident_pairs_are_suggested_with_the_counts_a_person_needs() {
        let cfg = DecisionConfig::default();
        let report = suggest_merges(&Keyword, &corpus(), &cfg, Duration::from_secs(5));
        assert_eq!(0, report.failed);
        let ids: Vec<(u64, u64)> = report.suggestions.iter().map(|s| (s.a.id, s.b.id)).collect();
        assert!(ids.contains(&(5, 6)) && ids.contains(&(1, 2)), "{ids:?}");
        assert!(!ids.contains(&(1, 3)), "different services' timeouts are not one failure: {ids:?}");
        let s = report.suggestions.iter().find(|s| s.a.id == 1).unwrap();
        assert_eq!((900, 40), (s.a.count, s.b.count));
        assert!(s.p_same >= 0.8);
    }

    #[test]
    fn the_floor_is_honoured() {
        let cfg = DecisionConfig { min_confidence: 0.95, ..DecisionConfig::default() };
        assert!(suggest_merges(&Keyword, &corpus(), &cfg, Duration::from_secs(5)).suggestions.is_empty());
    }

    #[test]
    fn a_provider_that_is_down_is_reported_once_and_not_waited_on_for_every_pair() {
        let report = suggest_merges(&Down, &corpus(), &DecisionConfig::default(), Duration::from_secs(5));
        assert!(report.suggestions.is_empty());
        assert_eq!(3, report.failed, "gives up after three failures with no success");
        assert!(report.error.as_deref().unwrap().contains("could not be reached"));
        let (pairs, _) = candidate_pairs(&corpus(), 100);
        assert_eq!(pairs.len() - 3, report.skipped, "the rest were not asked");
    }

    #[test]
    fn the_deadline_stops_the_questions() {
        let report = suggest_merges(&Keyword, &corpus(), &DecisionConfig::default(), Duration::ZERO);
        assert_eq!(0, report.checked);
        assert!(report.skipped > 0);
    }

    #[test]
    fn config_is_off_by_default_and_validated() {
        let cfg = DecisionConfig::default();
        assert!(!cfg.enabled() && cfg.problem().is_none());
        assert!(DecisionConfig { url: Some("ftp://x".into()), ..cfg.clone() }.problem().unwrap().contains("http"));
        assert!(DecisionConfig { min_confidence: 0.2, ..cfg.clone() }.problem().unwrap().contains("min_confidence"));
        assert!(DecisionConfig { max_pairs: 0, ..cfg }.problem().unwrap().contains("max_pairs"));
        assert!(toml::from_str::<DecisionConfig>("url = \"http://127.0.0.1:8099/decide\"\nmin_confidence = 0.9").unwrap().enabled());
        assert!(toml::from_str::<DecisionConfig>("bogus = 1").is_err());
    }

    #[test]
    fn answers_are_read_strictly() {
        let opts = [("same", ""), ("different", "")];
        let ok = json!({"answers": {"q": {"probabilities": {"same": 0.9, "different": 0.1}}}});
        assert_eq!(0.9, parse_probabilities(&ok, &opts).unwrap()["same"]);
        assert!(parse_probabilities(&json!({}), &opts).is_err());
        assert!(parse_probabilities(&json!({"answers": {"q": {"probabilities": {"same": 1.5, "different": 0.1}}}}), &opts).is_err());
        assert!(parse_probabilities(&json!({"answers": {"q": {"probabilities": {"same": 0.5}}}}), &opts).is_err());
    }
}
