//! What else the sender watches for besides a new log shape and a rate spike (`sync_policy`).
//!
//! Every detector learns a baseline from the instance's own traffic, in memory, and says what it saw as a [`Finding`]. The
//! planner turns a finding into a marker record for the peer and, unless the finding is a silence, boosts the service to full
//! detail with its lead-up. Nothing here sends anything itself.
//!
//! | kind | what it notices | boosts |
//! |---|---|---|
//! | `template_rate` | one template's rate far above its own baseline | only if the flood includes warnings or worse |
//! | `silence` | a regular template, or a whole service, stopped | no: there is nothing more to send |
//! | `new_pair` | a known template appearing in a service for the first time | yes |
//! | `error_share` | errors a much bigger share of a service's records, even when volume is flat or falling | yes |
//! | `param_drift` | a number inside a template (a latency, a size, a count) shifted well away from its baseline | yes |
//! | `flow` | two templates in one trace in an order never seen before | yes |
//!
//! # Bounds
//!
//! Each map has a cap, so a high-cardinality stream cannot grow the sender without limit: at most [`MAX_KEYS`] baselines per
//! detector and [`MAX_PER_BUCKET`] keys counted per 10 s bucket; beyond that, new keys are not tracked (the service-level
//! rate detector in `sync_policy` still sees them). A baseline that has been quiet for a long time at a low rate is dropped.
//!
//! # False-positive control
//!
//! Every bucket detector needs [`WARMUP_BUCKETS`] buckets of history (drift needs more), a minimum count, a deviation of
//! several standard deviations and a minimum ratio to the baseline. A long incident moves a baseline slowly, so it does not
//! become normal at once. After a restart every baseline relearns for about a minute; known templates and known
//! (service, template) pairs are read back from the catalog so a restart is not a wave of novelty.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::model::LogRecord;

pub const BUCKET_MS: u64 = 10_000;
const WARMUP_BUCKETS: u32 = 6;
const DRIFT_WARMUP_BUCKETS: u32 = 12;
const MAX_KEYS: usize = 10_000;
const MAX_PER_BUCKET: usize = 20_000;
const MAX_TRACES: usize = 20_000;
const MAX_SET: usize = 200_000;
const LEARN_MS: u64 = 60_000;

const TEMPLATE_RATE_MIN: f64 = 30.0;
const SILENCE_MIN_MEAN: f64 = 3.0;
const SERVICE_SILENCE_MIN_MEAN: f64 = 5.0;
const SILENCE_BUCKETS: u32 = 6;
const SHARE_MIN_TOTAL: u32 = 20;
const SHARE_MIN_ERRORS: u32 = 5;
const DRIFT_MIN_VALUES: u32 = 10;
const DRIFT_SLOTS: usize = 3;
const FORGET_AFTER_QUIET: u32 = 360;
/// Severity at and above which a record is an "error" for the share detector.
const ERROR: u8 = 17;
const WARN: u8 = 13;

/// One thing a detector noticed.
#[derive(Debug, Clone, PartialEq)]
pub struct Finding {
    pub service: Option<String>,
    pub kind: &'static str,
    pub detail: String,
    /// Whether to send the lead-up and raise the service to full detail.
    pub boost: bool,
}

type Key = (Option<String>, u64);

/// A running mean and variance (exponentially weighted) of a per-bucket measurement.
#[derive(Debug, Default, Clone, Copy)]
struct Baseline {
    mean: f64,
    var: f64,
    n: u32,
}

impl Baseline {
    fn sd(&self) -> f64 {
        self.var.sqrt()
    }

    /// `slow` after a deviation, so a long incident does not become normal at once.
    fn update(&mut self, x: f64, slow: bool) {
        let alpha = if slow && self.n >= WARMUP_BUCKETS { 0.02 } else { 0.2 };
        if self.n == 0 {
            self.mean = x;
            self.var = 0.0;
        } else {
            let d = x - self.mean;
            self.mean += alpha * d;
            self.var = (1.0 - alpha) * (self.var + alpha * d * d);
        }
        self.n += 1;
    }
}

#[derive(Debug, Default)]
struct Tracked {
    base: Baseline,
    /// Consecutive empty buckets.
    quiet: u32,
    flagged: bool,
}

#[derive(Debug, Default)]
struct Bucket {
    template: HashMap<Key, u32>,
    /// The worst severity each template had in the bucket.
    template_severity: HashMap<Key, u8>,
    service_total: HashMap<Option<String>, u32>,
    service_errors: HashMap<Option<String>, u32>,
    /// (service, template, slot) -> (sum, count) of a number found in the body.
    numbers: HashMap<(Option<String>, u64, u8), (f64, u32)>,
}

pub struct Signals {
    bucket_start_ms: u64,
    bucket: Bucket,
    templates: HashMap<Key, Tracked>,
    services: HashMap<Option<String>, Tracked>,
    share: HashMap<Option<String>, Baseline>,
    drift: HashMap<(Option<String>, u64, u8), Baseline>,
    pairs: HashSet<Key>,
    pairs_learn_until_ms: u64,
    bigrams: HashSet<(Option<String>, u64, u64)>,
    bigrams_learn_until_ms: u64,
    traces: HashMap<[u8; 16], u64>,
    trace_order: VecDeque<[u8; 16]>,
}

impl Signals {
    /// `known_pairs` are the (service, template) pairs the catalog holds. With none, the first minute learns them.
    pub fn new(now_ms: u64, known_pairs: impl IntoIterator<Item = (Option<String>, u64)>) -> Self {
        let pairs: HashSet<Key> = known_pairs.into_iter().take(MAX_SET).collect();
        let pairs_learn_until_ms = if pairs.is_empty() { now_ms + LEARN_MS } else { now_ms };
        Self {
            bucket_start_ms: now_ms - now_ms % BUCKET_MS,
            bucket: Bucket::default(),
            templates: HashMap::new(),
            services: HashMap::new(),
            share: HashMap::new(),
            drift: HashMap::new(),
            pairs,
            pairs_learn_until_ms,
            bigrams: HashSet::new(),
            bigrams_learn_until_ms: now_ms + LEARN_MS,
            traces: HashMap::new(),
            trace_order: VecDeque::new(),
        }
    }

    /// Looks at one record. `first_ever` says the template had never been seen at all (the planner reports that as `novel`, so
    /// the pair and flow detectors stay quiet about it). Returns what can be said straight away.
    pub fn observe(&mut self, now_ms: u64, record: &LogRecord, first_ever: bool) -> Vec<Finding> {
        let mut found = Vec::new();
        let service = &record.service;
        if self.bucket.service_total.len() < MAX_PER_BUCKET || self.bucket.service_total.contains_key(service) {
            *self.bucket.service_total.entry(service.clone()).or_insert(0) += 1;
            if record.severity.0 >= ERROR {
                *self.bucket.service_errors.entry(service.clone()).or_insert(0) += 1;
            }
        }
        let Some(template) = record.template_id else { return found };
        let key: Key = (service.clone(), template);

        if self.bucket.template.len() < MAX_PER_BUCKET || self.bucket.template.contains_key(&key) {
            *self.bucket.template.entry(key.clone()).or_insert(0) += 1;
            let worst = self.bucket.template_severity.entry(key.clone()).or_insert(0);
            *worst = (*worst).max(record.severity.0);
            for (slot, value) in numbers(&record.body).into_iter().enumerate().take(DRIFT_SLOTS) {
                let acc = self.bucket.numbers.entry((service.clone(), template, slot as u8)).or_insert((0.0, 0));
                acc.0 += value;
                acc.1 += 1;
            }
        }

        if !self.pairs.contains(&key) {
            if self.pairs.len() < MAX_SET {
                self.pairs.insert(key.clone());
            }
            if !first_ever && now_ms >= self.pairs_learn_until_ms {
                found.push(Finding {
                    service: service.clone(),
                    kind: "new_pair",
                    detail: format!("template {template} appeared in {} for the first time", name(service)),
                    boost: true,
                });
            }
        }

        if let Some(trace) = record.trace_id {
            let previous = self.traces.insert(trace, template);
            if previous.is_none() {
                self.trace_order.push_back(trace);
                if self.trace_order.len() > MAX_TRACES {
                    if let Some(old) = self.trace_order.pop_front() {
                        self.traces.remove(&old);
                    }
                }
            }
            if let Some(prev) = previous.filter(|p| *p != template) {
                let bigram = (service.clone(), prev, template);
                if !self.bigrams.contains(&bigram) {
                    if self.bigrams.len() < MAX_SET {
                        self.bigrams.insert(bigram);
                    }
                    if !first_ever && now_ms >= self.bigrams_learn_until_ms {
                        found.push(Finding {
                            service: service.clone(),
                            kind: "flow",
                            detail: format!("a call sequence never seen before in {}: template {prev} then {template}", name(service)),
                            boost: true,
                        });
                    }
                }
            }
        }
        found
    }

    /// Closes the buckets that have finished and says what they showed.
    pub fn close_buckets(&mut self, now_ms: u64) -> Vec<Finding> {
        let mut found = Vec::new();
        // After a long stall (a suspended laptop, a stopped process) the gap says nothing about the traffic: start again
        // rather than call every service silent.
        if now_ms.saturating_sub(self.bucket_start_ms) > u64::from(FORGET_AFTER_QUIET) * BUCKET_MS {
            *self = Self::new(now_ms, std::mem::take(&mut self.pairs));
            return found;
        }
        while now_ms >= self.bucket_start_ms + BUCKET_MS {
            let bucket = std::mem::take(&mut self.bucket);
            // Services first: a silent service is one finding, and its templates stay quiet about it.
            self.close_services(&bucket, &mut found);
            self.close_templates(&bucket, &mut found);
            self.close_share(&bucket, &mut found);
            self.close_drift(&bucket, &mut found);
            self.bucket_start_ms += BUCKET_MS;
        }
        found
    }

    fn close_templates(&mut self, bucket: &Bucket, found: &mut Vec<Finding>) {
        let silent_services: HashSet<&Option<String>> =
            self.services.iter().filter(|(_, t)| t.flagged).map(|(s, _)| s).collect();
        for (key, count) in &bucket.template {
            let x = f64::from(*count);
            if !self.templates.contains_key(key) && self.templates.len() >= MAX_KEYS {
                continue;
            }
            let t = self.templates.entry(key.clone()).or_default();
            t.quiet = 0;
            t.flagged = false;
            let b = t.base;
            let spiked = b.n >= WARMUP_BUCKETS && x >= TEMPLATE_RATE_MIN && x > b.mean + 4.0 * b.sd() + 10.0 && x >= 4.0 * b.mean;
            if spiked {
                found.push(Finding {
                    service: key.0.clone(),
                    kind: "template_rate",
                    detail: format!("template {} in {}: {} in {} s, against about {:.0} for a bucket this long", key.1, name(&key.0), *count, BUCKET_MS / 1000, b.mean),
                    // A flood of info or debug lines is noted but does not send the whole service at full detail: that
                    // would spend the bandwidth the dial exists to save on what is most often a loop or a load test.
                    // A flood that includes warnings or worse does boost.
                    boost: bucket.template_severity.get(key).copied().unwrap_or(0) >= WARN,
                });
            }
            t.base.update(x, spiked);
        }
        let mut forget = Vec::new();
        for (key, t) in &mut self.templates {
            if bucket.template.contains_key(key) {
                continue;
            }
            t.quiet += 1;
            // Keep the baseline a regular template had: decaying it would hide the silence it is being measured for.
            if t.base.mean < SILENCE_MIN_MEAN {
                t.base.update(0.0, false);
            }
            if t.base.mean >= SILENCE_MIN_MEAN && t.quiet >= SILENCE_BUCKETS && !t.flagged && !silent_services.contains(&key.0) {
                t.flagged = true;
                found.push(Finding {
                    service: key.0.clone(),
                    kind: "silence",
                    detail: format!(
                        "template {} in {} has not appeared for {} s; it normally appears about {:.0} times every {} s",
                        key.1,
                        name(&key.0),
                        u64::from(t.quiet) * BUCKET_MS / 1000,
                        t.base.mean,
                        BUCKET_MS / 1000
                    ),
                    boost: false,
                });
            }
            if t.quiet >= FORGET_AFTER_QUIET && t.base.mean < SILENCE_MIN_MEAN {
                forget.push(key.clone());
            }
        }
        for key in forget {
            self.templates.remove(&key);
        }
    }

    fn close_services(&mut self, bucket: &Bucket, found: &mut Vec<Finding>) {
        for (service, count) in &bucket.service_total {
            if !self.services.contains_key(service) && self.services.len() >= MAX_KEYS {
                continue;
            }
            let t = self.services.entry(service.clone()).or_default();
            t.quiet = 0;
            t.flagged = false;
            t.base.update(f64::from(*count), false);
        }
        for (service, t) in &mut self.services {
            if bucket.service_total.contains_key(service) {
                continue;
            }
            t.quiet += 1;
            if t.base.mean < SERVICE_SILENCE_MIN_MEAN {
                t.base.update(0.0, false);
            }
            if t.base.mean >= SERVICE_SILENCE_MIN_MEAN && t.quiet >= SILENCE_BUCKETS && !t.flagged {
                t.flagged = true;
                found.push(Finding {
                    service: service.clone(),
                    kind: "silence",
                    detail: format!("{} has sent nothing for {} s; it normally sends about {:.0} records every {} s", name(service), u64::from(t.quiet) * BUCKET_MS / 1000, t.base.mean, BUCKET_MS / 1000),
                    boost: false,
                });
            }
        }
        self.services.retain(|_, t| !(t.quiet >= FORGET_AFTER_QUIET && t.base.mean < SERVICE_SILENCE_MIN_MEAN));
    }

    /// Errors as a share of a service's records. A count alone misses the failure that arrives while traffic falls.
    fn close_share(&mut self, bucket: &Bucket, found: &mut Vec<Finding>) {
        for (service, total) in &bucket.service_total {
            if *total < SHARE_MIN_TOTAL {
                continue;
            }
            if !self.share.contains_key(service) && self.share.len() >= MAX_KEYS {
                continue;
            }
            let errors = *bucket.service_errors.get(service).unwrap_or(&0);
            let x = f64::from(errors) / f64::from(*total);
            let b = self.share.entry(service.clone()).or_default();
            let spiked = b.n >= WARMUP_BUCKETS && errors >= SHARE_MIN_ERRORS && x > b.mean + 4.0 * b.sd() + 0.05 && x >= 3.0 * b.mean;
            if spiked {
                found.push(Finding {
                    service: service.clone(),
                    kind: "error_share",
                    detail: format!("{} of {} records in {} s were errors ({:.0}%), against about {:.1}% normally", errors, total, BUCKET_MS / 1000, x * 100.0, b.mean * 100.0),
                    boost: true,
                });
            }
            b.update(x, spiked);
        }
    }

    /// A number inside a template (a latency, a size, a count), compared with its own history bucket by bucket.
    fn close_drift(&mut self, bucket: &Bucket, found: &mut Vec<Finding>) {
        for (key, (sum, count)) in &bucket.numbers {
            if *count < DRIFT_MIN_VALUES {
                continue;
            }
            if !self.drift.contains_key(key) && self.drift.len() >= MAX_KEYS {
                continue;
            }
            let x = sum / f64::from(*count);
            let b = self.drift.entry(key.clone()).or_default();
            let shift = (x - b.mean).abs();
            let drifted = b.n >= DRIFT_WARMUP_BUCKETS && shift > 5.0 * b.sd() && shift >= 0.5 * b.mean.abs() && shift >= 1.0;
            if drifted {
                found.push(Finding {
                    service: key.0.clone(),
                    kind: "param_drift",
                    detail: format!(
                        "a number in template {} in {} (value {} of the line) averaged {} over {} s, against about {} normally",
                        key.1,
                        name(&key.0),
                        key.2 + 1,
                        short(x),
                        BUCKET_MS / 1000,
                        short(b.mean)
                    ),
                    boost: true,
                });
            }
            b.update(x, drifted);
        }
    }
}

fn name(service: &Option<String>) -> String {
    service.clone().unwrap_or_else(|| "(no service)".into())
}

fn short(v: f64) -> String {
    if v.abs() >= 100.0 || v.fract() == 0.0 {
        format!("{v:.0}")
    } else {
        format!("{v:.2}")
    }
}

/// The numbers in a log line, in order: `took 120ms`, `size=4096`, `(code 05)` give 120, 4096, 5. A token is a number only if
/// what is left after its unit suffix parses; paths, timestamps (`12:30:45`) and ids with letters in them are not.
pub fn numbers(body: &str) -> Vec<f64> {
    let mut out = Vec::new();
    for token in body.split_whitespace().take(64) {
        let value = token.rsplit('=').next().unwrap_or(token);
        let value = value.trim_start_matches(['(', '[', '"', '\'']).trim_end_matches([')', ']', ',', ';', '.', '"', '\'']);
        let digits = value.trim_end_matches(|c: char| c.is_ascii_alphabetic() || c == '%');
        if digits.is_empty() || !digits.starts_with(|c: char| c.is_ascii_digit() || c == '-') {
            continue;
        }
        if let Ok(v) = digits.parse::<f64>() {
            if v.is_finite() {
                out.push(v);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;

    fn rec(service: &str, severity: Severity, template: u64, body: &str, trace: Option<u8>) -> LogRecord {
        let mut r = LogRecord::new(0, severity, body);
        r.service = Some(service.into());
        r.template_id = Some(template);
        r.trace_id = trace.map(|t| [t; 16]);
        r
    }

    /// Feeds `per_bucket` records of one template for each of `buckets` buckets, then closes them.
    fn feed(s: &mut Signals, start_bucket: u64, buckets: u64, per_bucket: u32, make: impl Fn(u32) -> LogRecord) -> Vec<Finding> {
        let mut all = Vec::new();
        for b in 0..buckets {
            let now = (start_bucket + b) * BUCKET_MS + 1;
            for i in 0..per_bucket {
                all.extend(s.observe(now, &make(i), false));
            }
            all.extend(s.close_buckets((start_bucket + b + 1) * BUCKET_MS));
        }
        all
    }

    fn warm(pairs: &[(&str, u64)]) -> Signals {
        // Start well after the learning minute so novelty counts.
        Signals::new(0, pairs.iter().map(|(s, t)| (Some((*s).to_string()), *t)))
    }

    #[test]
    fn numbers_come_out_in_order_and_paths_and_clocks_are_not_numbers() {
        assert_eq!(vec![120.0, 4096.0, 5.0], numbers("GET /api/users/42 took 120ms size=4096 (code 05)"));
        assert!(numbers("12:30:45 abc123 v1.2.3 /a/b/7").is_empty());
        assert_eq!(vec![-3.5, 99.0], numbers("delta -3.5 load 99%"));
    }

    #[test]
    fn one_template_flooding_is_noticed_against_its_own_baseline() {
        let mut s = warm(&[("api", 7)]);
        let make = |_| rec("api", Severity::INFO, 7, "tick", None);
        assert!(feed(&mut s, 0, 12, 10, make).iter().all(|f| f.kind != "template_rate"), "steady traffic raises nothing");
        let found = feed(&mut s, 12, 1, 400, make);
        let f = found.iter().find(|f| f.kind == "template_rate").expect("a flood is noticed");
        assert!(f.service.as_deref() == Some("api") && f.detail.contains("template 7"), "{f:?}");
        assert!(!f.boost, "a flood of info lines is noted, not boosted: {f:?}");
        // The same flood at warning level does boost.
        let mut w = warm(&[("api", 7)]);
        let warn = |_| rec("api", Severity::WARN, 7, "tick", None);
        feed(&mut w, 0, 12, 10, warn);
        let found = feed(&mut w, 12, 1, 400, warn);
        assert!(found.iter().any(|f| f.kind == "template_rate" && f.boost), "{found:?}");
    }

    #[test]
    fn a_regular_template_going_quiet_is_reported_once_and_does_not_boost() {
        let mut s = warm(&[("api", 7), ("api", 8)]);
        let both = |i: u32| rec("api", Severity::INFO, if i % 2 == 0 { 7 } else { 8 }, "x", None);
        feed(&mut s, 0, 12, 20, both);
        // Template 8 stops; 7 carries on.
        let only7 = |_| rec("api", Severity::INFO, 7, "x", None);
        let found = feed(&mut s, 12, 12, 10, only7);
        let silent: Vec<_> = found.iter().filter(|f| f.kind == "silence").collect();
        assert_eq!(1, silent.len(), "reported once: {found:?}");
        assert!(!silent[0].boost && silent[0].detail.contains("template 8"), "{:?}", silent[0]);
        // It comes back, then goes quiet again: reported again.
        feed(&mut s, 24, 3, 10, both);
        let again = feed(&mut s, 27, 12, 10, only7);
        assert_eq!(1, again.iter().filter(|f| f.kind == "silence").count(), "{again:?}");
    }

    #[test]
    fn a_whole_service_going_quiet_is_one_finding_not_one_per_template() {
        let mut s = warm(&[("api", 7), ("api", 8), ("db", 9)]);
        let all = |i: u32| match i % 3 {
            0 => rec("api", Severity::INFO, 7, "x", None),
            1 => rec("api", Severity::INFO, 8, "x", None),
            _ => rec("db", Severity::INFO, 9, "x", None),
        };
        feed(&mut s, 0, 12, 30, all);
        let db_only = |_| rec("db", Severity::INFO, 9, "x", None);
        let found = feed(&mut s, 12, 12, 10, db_only);
        let silent: Vec<_> = found.iter().filter(|f| f.kind == "silence").collect();
        assert_eq!(1, silent.len(), "{found:?}");
        assert!(silent[0].service.as_deref() == Some("api") && silent[0].detail.contains("api has sent nothing"), "{:?}", silent[0]);
    }

    #[test]
    fn a_known_template_in_a_new_service_is_a_pair_but_a_new_template_is_not() {
        let mut s = Signals::new(0, [(Some("api".to_string()), 7)]);
        assert!(s.observe(1_000, &rec("api", Severity::INFO, 7, "x", None), false).is_empty());
        let found = s.observe(1_000, &rec("worker", Severity::INFO, 7, "x", None), false);
        assert_eq!(1, found.len());
        assert_eq!(("new_pair", Some("worker".to_string())), (found[0].kind, found[0].service.clone()));
        assert!(s.observe(2_000, &rec("worker", Severity::INFO, 7, "x", None), false).is_empty(), "only the first time");
        assert!(s.observe(2_000, &rec("worker", Severity::INFO, 99, "x", None), true).is_empty(), "a brand new template is `novel`, not a pair");
    }

    #[test]
    fn with_nothing_known_the_first_minute_only_learns_pairs() {
        let mut s = Signals::new(0, []);
        assert!(s.observe(1_000, &rec("api", Severity::INFO, 7, "x", None), false).is_empty());
        assert!(s.observe(61_000, &rec("api", Severity::INFO, 8, "x", None), false).len() == 1, "after the minute a new pair counts");
    }

    #[test]
    fn errors_becoming_a_larger_share_is_noticed_even_when_traffic_falls() {
        let mut s = warm(&[("api", 7), ("api", 8)]);
        // 1 error in 100 per bucket, steady.
        let steady = |i: u32| if i == 0 { rec("api", Severity::ERROR, 8, "boom", None) } else { rec("api", Severity::INFO, 7, "ok", None) };
        assert!(feed(&mut s, 0, 12, 100, steady).iter().all(|f| f.kind != "error_share"));
        // Traffic falls to 40 per bucket but 12 of them are errors: the count of errors is small, the share is not.
        let failing = |i: u32| if i < 12 { rec("api", Severity::ERROR, 8, "boom", None) } else { rec("api", Severity::INFO, 7, "ok", None) };
        let found = feed(&mut s, 12, 1, 40, failing);
        let f = found.iter().find(|f| f.kind == "error_share").expect("share noticed");
        assert!(f.boost && f.detail.contains("12 of 40"), "{f:?}");
    }

    #[test]
    fn a_number_inside_a_template_drifting_is_noticed_and_ids_are_not() {
        let mut s = warm(&[("api", 7)]);
        let normal = |i: u32| rec("api", Severity::INFO, 7, &format!("request {} took {}ms", 50_000 + i * 7919 % 40_000, 100 + i % 5), None);
        assert!(feed(&mut s, 0, 20, 20, normal).iter().all(|f| f.kind != "param_drift"), "random ids and a steady latency raise nothing");
        let slow = |i: u32| rec("api", Severity::INFO, 7, &format!("request {} took {}ms", 50_000 + i * 7919 % 40_000, 900 + i % 5), None);
        let found = feed(&mut s, 20, 1, 20, slow);
        let f = found.iter().find(|f| f.kind == "param_drift").expect("latency drift noticed");
        assert!(f.boost && f.detail.contains("value 2 of the line") && f.detail.contains("template 7"), "{f:?}");
        assert_eq!(1, found.iter().filter(|f| f.kind == "param_drift").count(), "the id slot did not drift: {found:?}");
    }

    #[test]
    fn a_call_sequence_never_seen_is_noticed_but_only_after_learning() {
        let mut s = Signals::new(0, [(Some("api".to_string()), 1), (Some("api".to_string()), 2), (Some("api".to_string()), 3)]);
        let mut at = 0u64;
        let flow = |s: &mut Signals, trace: u8, templates: &[u64], at: u64| -> Vec<Finding> {
            templates.iter().flat_map(|t| s.observe(at, &rec("api", Severity::INFO, *t, "x", Some(trace)), false)).collect()
        };
        // During the learning minute 1 -> 2 is learned silently.
        assert!(flow(&mut s, 1, &[1, 2], 1_000).is_empty());
        at += 120_000;
        assert!(flow(&mut s, 2, &[1, 2], at).is_empty(), "a sequence already seen");
        let found = flow(&mut s, 3, &[1, 3], at);
        assert_eq!(1, found.len(), "{found:?}");
        assert_eq!("flow", found[0].kind);
        assert!(found[0].detail.contains("template 1 then 3"));
        assert!(flow(&mut s, 4, &[1, 3], at).is_empty(), "only the first time");
    }

    #[test]
    fn the_maps_stay_bounded_under_a_flood_of_distinct_keys() {
        let mut s = warm(&[]);
        for i in 0..(MAX_PER_BUCKET as u64 + 5_000) {
            s.observe(1_000, &rec(&format!("svc{}", i % 50), Severity::INFO, i, "x", None), false);
        }
        assert!(s.bucket.template.len() <= MAX_PER_BUCKET);
        s.close_buckets(BUCKET_MS);
        assert!(s.templates.len() <= MAX_KEYS);
    }
}
