//! How much of what the agent accepted is sent to a peer, and when to send more.
//!
//! A peer can run anywhere from a summary (1) to a full copy (100). The dial is `fidelity`, a percentage
//! of the records below [`FULL_FROM`] that are sent in full:
//!
//! * **At or above [`FULL_FROM`] (errors and fatals) everything is sent**, whatever the dial says.
//! * **Below it, `fidelity` percent are sent.** Which ones is decided by the event id ([`select`]), not a
//!   random draw, so a retry, a replay and a manual fetch all agree about which records were already sent.
//! * **What was not sent is counted, not lost.** Each minute the peer receives one rollup record per
//!   (service, template, level class) with the exact count of records left out, so the peer always has
//!   exact totals and every distinct thing that happened, at a fraction of the bytes.
//!
//! Anomalies raise the detail by themselves. When the sender sees a template it has never seen, or a
//! service's warning-and-worse rate jump well above its own baseline, it
//!
//! 1. **sends the lead-up**: the last minute of that service's records that had been left out, from a
//!    bounded in-memory ring (the same idea as the agent's own error context windows), and
//! 2. **boosts the service to full detail** for [`BOOST_MS`], then lets it fall back to the dial.
//!
//! # Reading a peer's store
//!
//! Records the peer received that carry `logless.supplement` (a lead-up record, or a record fetched by
//! hand) are extra detail for counts a rollup already gave. To total what happened, add the rollup
//! counts to the records that do not carry it.
//!
//! # State
//!
//! The planner's state (the ring, the baselines, the not-yet-flushed rollups, the boosts) is in memory.
//! A restart forgets the current minute's rollups and relearns baselines for about a minute; known
//! templates are re-read from the catalog so a restart is not a wave of "novel" templates.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::model::{Attr, AttrValue, LogRecord};
use crate::sync_signals::{Finding, Signals};

/// Records at or above this OTel severity (errors and fatals) are always sent in full.
pub const FULL_FROM: u8 = 17;
const WARN: u8 = 13;
pub const ATTR_SUPPLEMENT: &str = "logless.supplement";
/// How long a service stays at full detail after an anomaly.
pub const BOOST_MS: u64 = 5 * 60_000;
/// How far back an anomaly reaches for the records that were left out.
pub const LOOKBACK_MS: u64 = 60_000;
const RING_MAX: usize = 20_000;
const RATE_BUCKET_MS: u64 = 10_000;
const RATE_WARMUP_BUCKETS: u32 = 6;
const RATE_MIN_COUNT: f64 = 20.0;
const NOVELTY_WARMUP_MS: u64 = 60_000;
const SEEN_MAX: usize = 200_000;
const ANOMALY_LOG: usize = 20;
const ANOMALY_QUIET_MS: u64 = 30_000;
const ROLLUP_WINDOW_MS: u64 = 60_000;
const ROLLUP_GRACE_MS: u64 = 5_000;

/// Whether this record is one of the `fidelity` percent that are sent in full. Stable: the same id always
/// answers the same, so retries, replays and manual fetches agree about what was already sent.
pub fn select(event_id: &Uuid, fidelity: u8) -> bool {
    if fidelity >= 100 {
        return true;
    }
    if fidelity == 0 {
        return false;
    }
    // A UUIDv7's low 62 bits are random; fold them with FNV-1a so the percentage is even.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in &event_id.as_bytes()[8..16] {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h % 100 < u64::from(fidelity)
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannerConfig {
    /// 1 to 100.
    pub fidelity: u8,
    /// Below this nothing is sent or counted.
    pub min_severity: u8,
    pub anomaly_boost: bool,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self { fidelity: 100, min_severity: 0, anomaly_boost: true }
    }
}

/// One thing the sender noticed and acted on.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Anomaly {
    pub at_ms: u64,
    pub service: Option<String>,
    /// `novel`, `rate`, `template_rate`, `silence`, `new_pair`, `error_share`, `param_drift` or `flow`; see `sync_signals`.
    pub kind: String,
    pub detail: String,
    /// Records of lead-up sent with it.
    pub lookback_records: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PlannerStats {
    /// Records counted into rollups instead of being sent.
    pub sampled_out: u64,
    pub rollups_emitted: u64,
    /// Left-out records sent afterwards as an anomaly's lead-up.
    pub lookback_sent: u64,
    pub boosts_started: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RollupKey {
    minute_start_ms: u64,
    service: Option<String>,
    template_id: u64,
    class: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct RollupAcc {
    count: u64,
    first_ns: u64,
    last_ns: u64,
    max_severity: u8,
}

#[derive(Debug, Default)]
struct Rates {
    bucket_start_ms: u64,
    counts: HashMap<Option<String>, u32>,
    // service -> (mean, variance, buckets seen)
    baselines: HashMap<Option<String>, (f64, f64, u32)>,
}

pub struct Planner {
    instance: String,
    cfg: PlannerConfig,
    boosts: HashMap<Option<String>, u64>,
    ring: VecDeque<(u64, LogRecord)>,
    rollups: BTreeMap<RollupKey, RollupAcc>,
    seen: HashSet<u64>,
    learn_until_ms: u64,
    rates: Rates,
    signals: Signals,
    anomalies: VecDeque<Anomaly>,
    last_anomaly: HashMap<(Option<String>, &'static str), u64>,
    pub stats: PlannerStats,
}

impl Planner {
    /// `known_templates` are the ids the catalog already holds: they are not novel. With none, the first
    /// minute is spent learning.
    pub fn new(instance: String, cfg: PlannerConfig, now_ms: u64, known_templates: impl IntoIterator<Item = u64>) -> Self {
        let seen: HashSet<u64> = known_templates.into_iter().take(SEEN_MAX).collect();
        let learn_until_ms = if seen.is_empty() { now_ms + NOVELTY_WARMUP_MS } else { now_ms };
        Self {
            instance,
            cfg,
            boosts: HashMap::new(),
            ring: VecDeque::new(),
            rollups: BTreeMap::new(),
            seen,
            learn_until_ms,
            rates: Rates { bucket_start_ms: now_ms - now_ms % RATE_BUCKET_MS, ..Rates::default() },
            signals: Signals::new(now_ms, []),
            anomalies: VecDeque::new(),
            last_anomaly: HashMap::new(),
            stats: PlannerStats::default(),
        }
    }

    /// The (service, template) pairs the catalog already holds, so a restart is not a wave of "new in this service".
    pub fn with_known_pairs(mut self, now_ms: u64, pairs: impl IntoIterator<Item = (Option<String>, u64)>) -> Self {
        self.signals = Signals::new(now_ms, pairs);
        self
    }

    pub fn config(&self) -> &PlannerConfig {
        &self.cfg
    }

    pub fn set_config(&mut self, cfg: PlannerConfig) {
        self.cfg = cfg;
    }

    pub fn anomalies(&self) -> Vec<Anomaly> {
        self.anomalies.iter().rev().cloned().collect()
    }

    pub fn boosted(&self, now_ms: u64) -> Vec<String> {
        let mut names: Vec<String> = self.boosts.iter().filter(|(_, until)| **until > now_ms).map(|(s, _)| s.clone().unwrap_or_else(|| "(no service)".into())).collect();
        names.sort();
        names
    }

    /// Would this record be sent live at the current dial, ignoring any boost?
    pub fn sent_live(&self, record: &LogRecord) -> bool {
        record.severity.0 >= FULL_FROM || select(&record.event_id, self.cfg.fidelity)
    }

    /// The records to send now, in order, for a batch the agent accepted.
    pub fn process(&mut self, now_ms: u64, batch: &[LogRecord]) -> Vec<LogRecord> {
        let mut out = Vec::new();
        for record in batch {
            if record.severity.0 < self.cfg.min_severity {
                continue;
            }
            if self.cfg.anomaly_boost {
                let mut first_ever = false;
                if let Some(template) = record.template_id {
                    if !self.seen.contains(&template) {
                        first_ever = true;
                        if self.seen.len() < SEEN_MAX {
                            self.seen.insert(template);
                        }
                        if now_ms >= self.learn_until_ms {
                            let detail = format!("a log shape never seen before (template {template})");
                            self.trigger(record.service.clone(), "novel", detail, true, now_ms, &mut out);
                        }
                    }
                }
                for f in self.signals.observe(now_ms, record, first_ever) {
                    self.finding(f, now_ms, &mut out);
                }
                if record.severity.0 >= WARN {
                    *self.rates.counts.entry(record.service.clone()).or_insert(0) += 1;
                }
            }
            let boosted = self.boosts.get(&record.service).is_some_and(|until| *until > now_ms);
            if self.sent_live(record) || boosted {
                out.push(record.clone());
            } else {
                self.leave_out(record, now_ms);
            }
        }
        out
    }

    /// Closes finished rate buckets (which may raise an anomaly) and returns the rollup records of finished
    /// minutes, plus any lead-up. `force` flushes everything, for shutdown.
    pub fn tick(&mut self, now_ms: u64, force: bool) -> Vec<LogRecord> {
        let mut out = Vec::new();
        if self.cfg.anomaly_boost {
            for (service, count, mean) in self.close_rate_buckets(now_ms) {
                let detail = format!("{count} warnings or worse in {} s, against about {mean:.0} for a bucket this long", RATE_BUCKET_MS / 1000);
                self.trigger(service, "rate", detail, true, now_ms, &mut out);
            }
            for f in self.signals.close_buckets(now_ms) {
                self.finding(f, now_ms, &mut out);
            }
        }
        self.prune_ring(now_ms);
        let due: Vec<RollupKey> = self
            .rollups
            .keys()
            .filter(|k| force || k.minute_start_ms + ROLLUP_WINDOW_MS + ROLLUP_GRACE_MS <= now_ms)
            .cloned()
            .collect();
        for key in due {
            if let Some(acc) = self.rollups.remove(&key) {
                out.push(self.rollup_record(&key, &acc));
                self.stats.rollups_emitted += 1;
            }
        }
        out
    }

    fn leave_out(&mut self, record: &LogRecord, now_ms: u64) {
        self.stats.sampled_out += 1;
        let minute = record.observed_unix_nano / 1_000_000 / ROLLUP_WINDOW_MS * ROLLUP_WINDOW_MS;
        let key = RollupKey {
            minute_start_ms: minute,
            service: record.service.clone(),
            template_id: record.template_id.unwrap_or(0),
            class: record.severity.class().as_str(),
        };
        let acc = self.rollups.entry(key).or_insert(RollupAcc {
            count: 0,
            first_ns: record.observed_unix_nano,
            last_ns: record.observed_unix_nano,
            max_severity: record.severity.0,
        });
        acc.count += 1;
        acc.first_ns = acc.first_ns.min(record.observed_unix_nano);
        acc.last_ns = acc.last_ns.max(record.observed_unix_nano);
        acc.max_severity = acc.max_severity.max(record.severity.0);
        if self.cfg.anomaly_boost {
            self.ring.push_back((now_ms, record.clone()));
            while self.ring.len() > RING_MAX {
                self.ring.pop_front();
            }
        }
    }

    fn prune_ring(&mut self, now_ms: u64) {
        while self.ring.front().is_some_and(|(at, _)| now_ms.saturating_sub(*at) > LOOKBACK_MS) {
            self.ring.pop_front();
        }
    }

    fn finding(&mut self, f: Finding, now_ms: u64, out: &mut Vec<LogRecord>) {
        self.trigger(f.service, f.kind, f.detail, f.boost, now_ms, out);
    }

    /// Notes an anomaly: a marker record for the peer and a line in the log. With `boost`, also sends the service's lead-up and
    /// raises it to full detail for [`BOOST_MS`]; without (a silence), there is nothing more to send.
    fn trigger(&mut self, service: Option<String>, kind: &'static str, detail: String, boost: bool, now_ms: u64, out: &mut Vec<LogRecord>) {
        let mut newly_boosted = false;
        if boost {
            let entry = self.boosts.entry(service.clone()).or_insert(0);
            newly_boosted = *entry <= now_ms;
            *entry = (*entry).max(now_ms + BOOST_MS);
        }
        let quiet = self.last_anomaly.get(&(service.clone(), kind)).is_some_and(|at| now_ms.saturating_sub(*at) < ANOMALY_QUIET_MS);
        if quiet && !newly_boosted {
            return;
        }
        if newly_boosted {
            self.stats.boosts_started += 1;
        }
        self.last_anomaly.insert((service.clone(), kind), now_ms);
        // The lead-up: this service's records that were left out during the last minute.
        self.prune_ring(now_ms);
        let mut lead = Vec::new();
        let mut keep = VecDeque::with_capacity(self.ring.len());
        for (at, mut record) in std::mem::take(&mut self.ring) {
            if boost && record.service == service {
                record.attributes.push(Attr { key: ATTR_SUPPLEMENT.into(), value: AttrValue::Str(format!("lookback:{kind}")) });
                lead.push(record);
            } else {
                keep.push_back((at, record));
            }
        }
        self.ring = keep;
        self.stats.lookback_sent += lead.len() as u64;
        // A marker record: the peer can alert on `logless.anomaly` without understanding the sender's heuristics.
        let mut marker = LogRecord::new(now_ms * 1_000_000, crate::model::Severity::WARN, format!("anomaly ({kind}): {detail}"));
        marker.severity_text = Some("anomaly".into());
        marker.service = service.clone();
        marker.attributes = vec![
            Attr { key: "logless.anomaly".into(), value: AttrValue::Bool(true) },
            Attr { key: "logless.anomaly.kind".into(), value: AttrValue::Str(kind.into()) },
            Attr { key: "logless.anomaly.lookback".into(), value: AttrValue::I64(lead.len() as i64) },
        ];
        out.push(marker);
        self.anomalies.push_back(Anomaly { at_ms: now_ms, service, kind: kind.to_string(), detail, lookback_records: lead.len() });
        while self.anomalies.len() > ANOMALY_LOG {
            self.anomalies.pop_front();
        }
        out.extend(lead);
    }

    /// Finished buckets, as (service, count, baseline mean) for the ones that spiked.
    fn close_rate_buckets(&mut self, now_ms: u64) -> Vec<(Option<String>, u32, f64)> {
        let mut spikes = Vec::new();
        while now_ms >= self.rates.bucket_start_ms + RATE_BUCKET_MS {
            let counts = std::mem::take(&mut self.rates.counts);
            let services: HashSet<Option<String>> = counts.keys().cloned().chain(self.rates.baselines.keys().cloned()).collect();
            for service in services {
                let x = f64::from(*counts.get(&service).unwrap_or(&0));
                let (mean, var, n) = self.rates.baselines.get(&service).copied().unwrap_or((0.0, 0.0, 0));
                if n >= RATE_WARMUP_BUCKETS && x >= RATE_MIN_COUNT && x > mean + 4.0 * var.sqrt() + 10.0 && x >= 3.0 * mean {
                    spikes.push((service.clone(), x as u32, mean));
                }
                // A spike moves the baseline only a little, so a long incident does not become normal at once.
                let alpha = if x > mean * 3.0 && n >= RATE_WARMUP_BUCKETS { 0.02 } else { 0.2 };
                let new_mean = if n == 0 { x } else { mean + alpha * (x - mean) };
                let new_var = if n == 0 { 0.0 } else { (1.0 - alpha) * (var + alpha * (x - mean) * (x - mean)) };
                self.rates.baselines.insert(service, (new_mean, new_var, n + 1));
            }
            self.rates.bucket_start_ms += RATE_BUCKET_MS;
        }
        spikes
    }

    fn rollup_record(&self, key: &RollupKey, acc: &RollupAcc) -> LogRecord {
        let mut digest = Sha256::new();
        digest.update(self.instance.as_bytes());
        digest.update(key.minute_start_ms.to_le_bytes());
        digest.update(key.service.as_deref().unwrap_or("").as_bytes());
        digest.update(key.template_id.to_le_bytes());
        digest.update(key.class.as_bytes());
        let hash = digest.finalize();
        let mut id = [0u8; 16];
        id.copy_from_slice(&hash[..16]);
        let ns = key.minute_start_ms * 1_000_000;
        let mut record = LogRecord::new(ns, crate::model::Severity(acc.max_severity), format!("{} similar records were left out of sync ({})", acc.count, key.class));
        record.event_id = Uuid::from_bytes(id);
        record.observed_unix_nano = ns;
        record.severity_text = Some("rollup".into());
        record.service = key.service.clone();
        record.template_id = (key.template_id != 0).then_some(key.template_id);
        let int = |key: &str, v: u64| Attr { key: key.into(), value: AttrValue::I64(i64::try_from(v).unwrap_or(i64::MAX)) };
        record.attributes = vec![
            Attr { key: "logless.rollup".into(), value: AttrValue::Bool(true) },
            int("logless.rollup.count", acc.count),
            int("logless.rollup.window_ms", ROLLUP_WINDOW_MS),
            Attr { key: "logless.rollup.class".into(), value: AttrValue::Str(key.class.into()) },
            int("logless.rollup.first_ns", acc.first_ns),
            int("logless.rollup.last_ns", acc.last_ns),
        ];
        record
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;

    fn rec(service: &str, severity: Severity, template: u64, observed_ms: u64) -> LogRecord {
        let mut r = LogRecord::new(observed_ms * 1_000_000, severity, "line");
        r.observed_unix_nano = observed_ms * 1_000_000;
        r.service = Some(service.into());
        r.template_id = Some(template);
        r
    }

    fn planner(fidelity: u8, now: u64) -> Planner {
        Planner::new("edge".into(), PlannerConfig { fidelity, ..PlannerConfig::default() }, now, [1, 2, 3, 4, 5])
    }

    fn rollup_total(out: &[LogRecord]) -> u64 {
        out.iter()
            .filter(|r| r.attributes.iter().any(|a| a.key == "logless.rollup"))
            .map(|r| r.attributes.iter().find(|a| a.key == "logless.rollup.count").map_or(0, |a| if let AttrValue::I64(n) = a.value { n as u64 } else { 0 }))
            .sum()
    }

    fn is_rollup(r: &LogRecord) -> bool {
        r.attributes.iter().any(|a| a.key == "logless.rollup")
    }

    #[test]
    fn selection_is_stable_and_even() {
        let ids: Vec<Uuid> = (0..20_000).map(|_| Uuid::now_v7()).collect();
        for f in [1u8, 10, 50, 90] {
            let n = ids.iter().filter(|id| select(id, f)).count() as f64;
            let expected = 200.0 * f64::from(f);
            assert!((n - expected).abs() < expected * 0.25 + 40.0, "{f}%: {n} of 20000");
            assert!(ids.iter().all(|id| select(id, f) == select(id, f)));
        }
        assert!(ids.iter().all(|id| select(id, 100)));
        assert!(!ids.iter().any(|id| select(id, 0)));
        // A higher dial only adds records: nothing sent at 10 is dropped at 50.
        assert!(ids.iter().filter(|id| select(id, 10)).all(|id| select(id, 50)));
    }

    #[test]
    fn at_full_fidelity_everything_is_sent_and_nothing_is_rolled_up() {
        let mut p = planner(100, 0);
        let batch: Vec<_> = (0..100).map(|i| rec("api", Severity::INFO, 1, i)).collect();
        assert_eq!(100, p.process(1_000, &batch).len());
        assert!(p.tick(10 * 60_000, true).iter().all(|r| !is_rollup(r)), "nothing was left out, so nothing is rolled up");
        assert_eq!(0, p.stats.sampled_out);
    }

    #[test]
    fn errors_always_go_and_the_rest_is_counted_exactly() {
        let mut p = planner(10, 0);
        let mut batch = Vec::new();
        for i in 0..2_000u64 {
            batch.push(rec("api", Severity::INFO, 1 + i % 3, 1_000 + i));
        }
        for i in 0..50u64 {
            batch.push(rec("api", Severity::ERROR, 4, 1_000 + i));
        }
        let sent = p.process(2_000, &batch);
        let errors = sent.iter().filter(|r| r.severity >= Severity::ERROR).count();
        assert_eq!(50, errors, "every error is sent");
        let infos_sent = sent.iter().filter(|r| r.severity < Severity::ERROR).count() as u64;
        let rollups = p.tick(10 * 60_000, true);
        assert_eq!(2_000, infos_sent + rollup_total(&rollups), "sent plus rolled-up is exactly what the agent took");
        assert!(rollups.iter().filter(|r| is_rollup(r)).count() > 0);
        assert!(infos_sent > 50 && infos_sent < 400, "about 10% of 2000, got {infos_sent}");
    }

    #[test]
    fn rollups_are_idempotent_across_a_retry_and_carry_the_window() {
        // Boost off: this is about the rollups, and a first sighting of a pair would (rightly) send everything.
        let quiet = || Planner::new("edge".into(), PlannerConfig { fidelity: 1, anomaly_boost: false, ..PlannerConfig::default() }, 0, [1, 2, 3, 4, 5]);
        let (mut a, mut b) = (quiet(), quiet());
        let batch: Vec<_> = (0..500).map(|i| rec("api", Severity::INFO, 2, 61_000 + i)).collect();
        a.process(62_000, &batch);
        b.process(62_000, &batch);
        let (ra, rb) = (a.tick(3_600_000, true), b.tick(3_600_000, true));
        let (ra, rb): (Vec<_>, Vec<_>) = (ra.into_iter().filter(is_rollup).collect(), rb.into_iter().filter(is_rollup).collect());
        assert_eq!(ra.iter().map(|r| r.event_id).collect::<Vec<_>>(), rb.iter().map(|r| r.event_id).collect::<Vec<_>>(), "same key, same id");
        let r = &ra[0];
        assert_eq!(60_000 * 1_000_000, r.observed_unix_nano, "the minute it covers");
        assert!(r.attributes.iter().any(|x| x.key == "logless.rollup.window_ms"));
    }

    #[test]
    fn minimum_severity_excludes_a_record_even_from_the_counts() {
        let mut p = Planner::new("edge".into(), PlannerConfig { fidelity: 1, min_severity: 9, anomaly_boost: true }, 0, [1]);
        let batch: Vec<_> = (0..300).map(|i| rec("api", Severity::DEBUG, 1, i)).collect();
        assert!(p.process(1_000, &batch).is_empty());
        assert!(p.tick(10 * 60_000, true).is_empty(), "debug below the floor is not counted");
    }

    #[test]
    fn a_new_template_after_the_known_ones_boosts_the_service_and_sends_the_lead_up() {
        let mut p = planner(1, 100_000);
        // Quiet lead-up for api, left out.
        let lead: Vec<_> = (0..200).map(|i| rec("api", Severity::INFO, 1, 100_000 + i)).collect();
        let sent_lead = p.process(110_000, &lead);
        assert!(sent_lead.len() < 20);
        // A shape never seen before appears.
        let novel = rec("api", Severity::INFO, 999, 120_000);
        let out = p.process(120_000, &[novel.clone()]);
        let supplements = out.iter().filter(|r| r.attributes.iter().any(|a| a.key == ATTR_SUPPLEMENT)).count();
        let marker = out.iter().find(|r| r.attributes.iter().any(|a| a.key == "logless.anomaly")).expect("a marker record for the peer to alert on");
        assert_eq!(Some("api"), marker.service.as_deref());
        assert!(marker.body.contains("never seen before"));
        assert!(supplements > 150, "the lead-up goes with it: {supplements}");
        assert!(out.iter().any(|r| r.event_id == novel.event_id), "and the novel record itself");
        let anomaly = &p.anomalies()[0];
        assert_eq!(("novel", Some("api".to_string())), (anomaly.kind.as_str(), anomaly.service.clone()));
        assert_eq!(supplements, anomaly.lookback_records);
        // Now boosted: everything from api goes, other services still follow the dial.
        let more: Vec<_> = (0..100).map(|i| rec("api", Severity::INFO, 1, 121_000 + i)).collect();
        assert_eq!(100, p.process(121_000, &more).len());
        assert_eq!(vec!["api".to_string()], p.boosted(121_000));
        let other: Vec<_> = (0..100).map(|i| rec("web", Severity::INFO, 2, 121_000 + i)).collect();
        assert!(p.process(121_000, &other).len() < 20);
        // The boost ends.
        let later = rec("api", Severity::INFO, 1, 121_000 + BOOST_MS + 10);
        let late: Vec<_> = (0..100).map(|_| later.clone()).collect();
        let _ = late;
        assert!(p.boosted(121_000 + BOOST_MS + 10).is_empty());
    }

    #[test]
    fn known_templates_are_not_novel_and_the_first_minute_without_any_is_spent_learning() {
        let mut known = planner(1, 0);
        known.process(1_000, &[rec("api", Severity::INFO, 3, 1_000)]);
        assert!(known.anomalies().is_empty(), "template 3 is in the catalog");
        let mut fresh = Planner::new("edge".into(), PlannerConfig { fidelity: 1, ..PlannerConfig::default() }, 0, std::iter::empty());
        fresh.process(1_000, &[rec("api", Severity::INFO, 77, 1_000)]);
        assert!(fresh.anomalies().is_empty(), "learning");
        fresh.process(NOVELTY_WARMUP_MS + 1, &[rec("api", Severity::INFO, 78, NOVELTY_WARMUP_MS)]);
        assert_eq!(1, fresh.anomalies().len(), "after the warm-up an unseen shape counts");
    }

    #[test]
    fn a_rate_spike_over_the_baseline_boosts_and_a_steady_rate_does_not() {
        let mut p = planner(1, 0);
        let mut now = 0u64;
        // Ten quiet buckets at about 5 warnings each.
        for _ in 0..10 {
            let batch: Vec<_> = (0..5).map(|i| rec("api", Severity::WARN, 1, now + i)).collect();
            p.process(now + 5_000, &batch);
            now += RATE_BUCKET_MS;
            p.tick(now, false);
        }
        assert!(p.anomalies().is_empty(), "a steady rate is not an anomaly");
        // Then 400 in one bucket.
        let burst: Vec<_> = (0..400).map(|i| rec("api", Severity::WARN, 1, now + i)).collect();
        p.process(now + 5_000, &burst);
        now += RATE_BUCKET_MS;
        let out = p.tick(now, false);
        let anomaly = p.anomalies();
        let rate = anomaly.iter().find(|a| a.kind == "rate").expect("the service-level rate spike is logged");
        assert!(rate.detail.contains("400"));
        assert!(!out.is_empty() || p.boosted(now) == vec!["api".to_string()]);
        assert_eq!(vec!["api".to_string()], p.boosted(now));
    }

    #[test]
    fn a_service_that_keeps_alarming_is_logged_once_per_quiet_period() {
        let mut p = planner(1, 100_000);
        for i in 0..5u64 {
            p.process(100_000 + i * 1_000, &[rec("api", Severity::INFO, 900 + i, 100_000)]);
        }
        assert_eq!(1, p.anomalies().len(), "five new shapes in a row, one anomaly while it is boosted");
    }

    #[test]
    fn the_lead_up_ring_is_bounded_and_forgets_what_is_old() {
        let mut p = planner(1, 0);
        let batch: Vec<_> = (0..RING_MAX as u64 + 500).map(|i| rec("api", Severity::INFO, 1, i)).collect();
        p.process(1_000, &batch);
        assert!(p.ring.len() <= RING_MAX);
        p.tick(1_000 + LOOKBACK_MS + 1, false);
        assert!(p.ring.is_empty(), "older than the look-back");
    }

    #[test]
    fn with_anomaly_boost_off_nothing_is_kept_for_a_lead_up() {
        let mut p = Planner::new("edge".into(), PlannerConfig { fidelity: 1, min_severity: 0, anomaly_boost: false }, 0, [1]);
        let batch: Vec<_> = (0..100).map(|i| rec("api", Severity::INFO, 999, i)).collect();
        p.process(1_000, &batch);
        assert!(p.ring.is_empty() && p.anomalies().is_empty());
    }
}
