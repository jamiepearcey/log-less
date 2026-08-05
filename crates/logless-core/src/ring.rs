//! Smart pushdown — the wedge feature.
//!
//! "Every error arrives with the debug logs that caused it, and you never paid
//! to ship debug." When an ERROR passes through, we attach the preceding lines
//! that share its correlation key and forward that bundle upstream, while the
//! debug lines themselves are never sent on their own.
//!
//! **Served from memory, not the store** (`docs/architecture.md` §4). Committed
//! state is minutes stale and the WAL is unindexed; by the time an error is
//! forwarded, its context has to already be at hand.
//!
//! Three bounds keep this safe under an error storm, which is exactly when it
//! matters and exactly when a naive implementation falls over:
//!
//! * **memory** — per-shard byte budget, oldest evicted first;
//! * **egress** — a token bucket per `(service, error template)`, so cost is
//!   O(distinct error shapes) rather than O(errors);
//! * **repetition** — identical context windows collapse by flow hash, turning
//!   a retry loop into one window and a counter.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::Duration;

use crate::model::LogRecord;

/// Which signal identified the context. Reported upstream so users can see when
/// correlation fell back to something weak and fix their instrumentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum KeyTier {
    /// `trace_id` — the good case.
    Trace,
    /// A request/session id from attributes.
    Request,
    /// host + pid + thread.
    Thread,
    /// host + service, correlated by time alone. Weak: concurrent requests on
    /// one service interleave.
    Service,
}

impl KeyTier {
    pub fn as_str(self) -> &'static str {
        match self {
            KeyTier::Trace => "trace",
            KeyTier::Request => "request",
            KeyTier::Thread => "thread",
            KeyTier::Service => "service",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CorrelationKey {
    pub tier: KeyTier,
    pub value: String,
}

/// Derive the strongest available correlation key.
///
/// The chain exists because most real logs have no `trace_id`. Falling all the
/// way through to `(host, service)` is deliberately still useful — noisy, but a
/// noisy context beats none during an incident.
pub fn correlation_key(record: &LogRecord) -> CorrelationKey {
    if let Some(trace) = record.trace_id {
        return CorrelationKey {
            tier: KeyTier::Trace,
            value: hex16(&trace),
        };
    }
    let attr = |name: &str| {
        record.attributes.iter().find_map(|a| {
            if a.key == name {
                match &a.value {
                    crate::model::AttrValue::Str(s) => Some(s.clone()),
                    crate::model::AttrValue::I64(i) => Some(i.to_string()),
                    _ => None,
                }
            } else {
                None
            }
        })
    };
    let service = record.service.clone().unwrap_or_default();

    for name in ["request_id", "req_id", "session_id", "correlation_id"] {
        if let Some(id) = attr(name) {
            return CorrelationKey {
                tier: KeyTier::Request,
                value: format!("{service}/{id}"),
            };
        }
    }

    let host = attr("host").unwrap_or_default();
    if let (Some(pid), Some(thread)) = (attr("pid"), attr("thread")) {
        return CorrelationKey {
            tier: KeyTier::Thread,
            value: format!("{host}/{pid}/{thread}"),
        };
    }

    CorrelationKey {
        tier: KeyTier::Service,
        value: format!("{host}/{service}"),
    }
}

fn hex16(bytes: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[derive(Debug, Clone, PartialEq)]
pub struct RingConfig {
    pub shards: usize,
    /// Total budget across all shards.
    pub max_bytes: usize,
    /// Cap per correlation key, so one chatty thread cannot evict everyone.
    pub max_lines_per_key: usize,
    /// Lines older than this are never used as context.
    pub context_age: Duration,
    /// Most context lines attached to one error.
    pub max_context_lines: usize,
    /// Full context windows allowed per `(service, template)` per minute.
    /// Beyond it, errors are still reported but without their context.
    pub windows_per_minute: u32,
    /// Identical windows within this period collapse into one.
    pub dedupe_window: Duration,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self {
            shards: 16,
            max_bytes: 64 * 1024 * 1024,
            max_lines_per_key: 2_000,
            context_age: Duration::from_secs(30),
            max_context_lines: 200,
            windows_per_minute: 3,
            dedupe_window: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ContextLine {
    pub timestamp_unix_nano: u64,
    pub severity: u8,
    pub template_id: Option<u64>,
    pub body: String,
}

/// An error plus the lines that led to it — the payload forwarded upstream.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ContextWindow {
    pub error: ContextLine,
    pub context: Vec<ContextLine>,
    pub key_tier: KeyTier,
    /// Hash of the ordered template sequence; identical flows collapse on it.
    pub flow_hash: u64,
    pub error_template_id: Option<u64>,
    pub service: Option<String>,
    /// Occurrences collapsed into this window since it was first emitted.
    pub suppressed: u64,
}

/// Why a context window was not attached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Suppressed {
    /// An identical flow was forwarded recently; `flow_hash` points at it.
    DuplicateFlow,
    /// The per-(service, template) budget is spent for this minute.
    RateLimited,
    /// Nothing correlated was buffered.
    NoContext,
}

/// Outcome of capturing context for an error.
///
/// Deliberately not `Option<ContextWindow>`: suppressing the *context* must
/// never suppress the *error*. The vendor still needs every error event; what
/// we are economising on is the expensive context, and the caller has to be
/// able to tell the difference.
#[derive(Debug, Clone, PartialEq)]
pub enum Capture {
    Window(Box<ContextWindow>),
    ContextSuppressed {
        reason: Suppressed,
        /// Identifies the window this error would have duplicated, so a
        /// consumer can link back to the one that was sent.
        flow_hash: Option<u64>,
    },
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RingStats {
    pub lines_buffered: u64,
    pub lines_evicted: u64,
    pub windows_emitted: u64,
    /// Suppressed because an identical flow was emitted recently.
    pub windows_deduped: u64,
    /// Suppressed because the per-template budget was exhausted.
    pub windows_rate_limited: u64,
    pub bytes_held: usize,
}

#[derive(Debug, Default)]
struct Entry {
    timestamp_unix_nano: u64,
    severity: u8,
    template_id: Option<u64>,
    body: String,
}

impl Entry {
    fn bytes(&self) -> usize {
        // Body plus a rough allowance for the struct and map overhead. Exactness
        // is not the point; a bound that actually holds is.
        self.body.len() + 96
    }
}

#[derive(Default)]
struct Shard {
    keys: HashMap<CorrelationKey, VecDeque<Entry>>,
    /// Insertion order of keys, for oldest-first eviction.
    order: VecDeque<CorrelationKey>,
    bytes: usize,
}

pub struct Ring {
    config: RingConfig,
    shards: Vec<Shard>,
    /// `(service, template)` → (window start nanos, windows emitted).
    budgets: HashMap<(String, u64), (u64, u32)>,
    /// flow hash → (last emitted nanos, suppressed count).
    recent_flows: HashMap<u64, (u64, u64)>,
    pub stats: RingStats,
}

impl RingConfig {
    /// Build from the user-facing config section.
    pub fn from_pushdown(p: &crate::config::PushdownConfig) -> Self {
        Self {
            max_bytes: p.ring_max_bytes,
            max_context_lines: p.max_context_lines,
            context_age: p.context_age,
            windows_per_minute: p.windows_per_minute,
            dedupe_window: p.dedupe_window,
            ..Default::default()
        }
    }
}

impl Default for Ring {
    fn default() -> Self {
        Self::new(RingConfig::default())
    }
}

impl Ring {
    pub fn new(config: RingConfig) -> Self {
        let shards = (0..config.shards.max(1)).map(|_| Shard::default()).collect();
        Self {
            config,
            shards,
            budgets: HashMap::new(),
            recent_flows: HashMap::new(),
            stats: RingStats::default(),
        }
    }

    fn shard_for(&self, key: &CorrelationKey) -> usize {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % self.shards.len()
    }

    fn shard_budget(&self) -> usize {
        self.config.max_bytes / self.shards.len().max(1)
    }

    /// Record a line as potential context.
    ///
    /// Errors are recorded too: an error is often the context for the *next*
    /// error in a cascade.
    pub fn observe(&mut self, record: &LogRecord) {
        let key = correlation_key(record);
        let entry = Entry {
            timestamp_unix_nano: record.observed_unix_nano,
            severity: record.severity.0,
            template_id: record.template_id,
            body: record.body.clone(),
        };
        let bytes = entry.bytes();
        let budget = self.shard_budget();
        let max_lines = self.config.max_lines_per_key;
        let idx = self.shard_for(&key);
        let shard = &mut self.shards[idx];

        let queue = match shard.keys.get_mut(&key) {
            Some(q) => q,
            None => {
                shard.order.push_back(key.clone());
                shard.keys.entry(key.clone()).or_default()
            }
        };
        queue.push_back(entry);
        shard.bytes += bytes;
        self.stats.lines_buffered += 1;

        // Per-key cap first: one loud key must not consume the shard.
        while queue.len() > max_lines {
            if let Some(old) = queue.pop_front() {
                shard.bytes = shard.bytes.saturating_sub(old.bytes());
                self.stats.lines_evicted += 1;
            }
        }

        // Then the shard budget, oldest key first.
        while shard.bytes > budget {
            let Some(oldest) = shard.order.pop_front() else {
                break;
            };
            if let Some(queue) = shard.keys.remove(&oldest) {
                for e in &queue {
                    shard.bytes = shard.bytes.saturating_sub(e.bytes());
                    self.stats.lines_evicted += 1;
                }
            }
        }

        self.stats.bytes_held = self.shards.iter().map(|s| s.bytes).sum();
    }

    /// Build the context window for an error, applying rate limiting and
    /// flow dedupe.
    ///
    /// Always returns something: either a window, or the reason its context was
    /// withheld. The error itself is always forwardable.
    pub fn capture(&mut self, error: &LogRecord) -> Capture {
        let key = correlation_key(error);
        let idx = self.shard_for(&key);
        let cutoff = error
            .observed_unix_nano
            .saturating_sub(self.config.context_age.as_nanos() as u64);

        let Some(queue) = self.shards[idx].keys.get(&key) else {
            return Capture::ContextSuppressed {
                reason: Suppressed::NoContext,
                flow_hash: None,
            };
        };
        let context: Vec<ContextLine> = {
            queue
                .iter()
                .filter(|e| e.timestamp_unix_nano >= cutoff)
                // Do not include the error line itself in its own context.
                .filter(|e| e.body != error.body || e.timestamp_unix_nano != error.observed_unix_nano)
                .rev()
                .take(self.config.max_context_lines)
                .map(|e| ContextLine {
                    timestamp_unix_nano: e.timestamp_unix_nano,
                    severity: e.severity,
                    template_id: e.template_id,
                    body: e.body.clone(),
                })
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect()
        };

        if context.is_empty() {
            return Capture::ContextSuppressed {
                reason: Suppressed::NoContext,
                flow_hash: None,
            };
        }

        let flow_hash = flow_hash(&context, error.template_id);
        let now = error.observed_unix_nano;

        // Flow dedupe: many requests failing the same way produce the same
        // window over and over. Collapse to one window plus a count.
        let mut suppressed_since = 0;
        if let Some((last, count)) = self.recent_flows.get_mut(&flow_hash) {
            if now.saturating_sub(*last) < self.config.dedupe_window.as_nanos() as u64 {
                *count += 1;
                self.stats.windows_deduped += 1;
                return Capture::ContextSuppressed {
                    reason: Suppressed::DuplicateFlow,
                    flow_hash: Some(flow_hash),
                };
            }
            // The window reopens: report how many were folded into the last one.
            suppressed_since = *count;
            *last = now;
            *count = 0;
        } else {
            self.prune_flows(now);
            self.recent_flows.insert(flow_hash, (now, 0));
        }

        // Rate limit per (service, error template): bounds egress by the number
        // of distinct error shapes, not the number of errors.
        let service = error.service.clone().unwrap_or_default();
        let template = error.template_id.unwrap_or(0);
        let minute = Duration::from_secs(60).as_nanos() as u64;
        let budget = self.budgets.entry((service.clone(), template)).or_insert((now, 0));
        if now.saturating_sub(budget.0) >= minute {
            *budget = (now, 0);
        }
        if budget.1 >= self.config.windows_per_minute {
            self.stats.windows_rate_limited += 1;
            return Capture::ContextSuppressed {
                reason: Suppressed::RateLimited,
                flow_hash: Some(flow_hash),
            };
        }
        budget.1 += 1;

        self.stats.windows_emitted += 1;
        Capture::Window(Box::new(ContextWindow {
            error: ContextLine {
                timestamp_unix_nano: error.observed_unix_nano,
                severity: error.severity.0,
                template_id: error.template_id,
                body: error.body.clone(),
            },
            context,
            key_tier: key.tier,
            flow_hash,
            error_template_id: error.template_id,
            service: error.service.clone(),
            suppressed: suppressed_since,
        }))
    }

    /// Drop flow records older than the dedupe window so the map stays bounded
    /// under a stream of ever-changing flows.
    fn prune_flows(&mut self, now: u64) {
        if self.recent_flows.len() < 10_000 {
            return;
        }
        let window = self.config.dedupe_window.as_nanos() as u64;
        self.recent_flows
            .retain(|_, (last, _)| now.saturating_sub(*last) < window);
        // Still full of live flows: this is genuine cardinality, so drop the lot
        // rather than grow without bound. Worst case we re-emit some windows.
        if self.recent_flows.len() >= 10_000 {
            self.recent_flows.clear();
        }
    }

    pub fn bytes_held(&self) -> usize {
        self.shards.iter().map(|s| s.bytes).sum()
    }

    pub fn keys_held(&self) -> usize {
        self.shards.iter().map(|s| s.keys.len()).sum()
    }
}

/// Identity of a context window: the ordered template sequence plus the error.
///
/// Bodies are deliberately not hashed — two runs of the same code path differ
/// only in their parameters and should collapse to one window.
fn flow_hash(context: &[ContextLine], error_template: Option<u64>) -> u64 {
    let mut h = DefaultHasher::new();
    for line in context {
        line.template_id.hash(&mut h);
    }
    error_template.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Attr, AttrValue, Severity};

    fn line(ts_secs: u64, sev: Severity, body: &str, template: u64) -> LogRecord {
        let mut r = LogRecord::new(ts_secs * 1_000_000_000, sev, body);
        r.observed_unix_nano = ts_secs * 1_000_000_000;
        r.service = Some("api".into());
        r.template_id = Some(template);
        r
    }

    #[track_caller]
    fn expect_window(c: Capture) -> ContextWindow {
        match c {
            Capture::Window(w) => *w,
            other => panic!("expected a window, got {other:?}"),
        }
    }

    fn with_trace(mut r: LogRecord, trace: u8) -> LogRecord {
        r.trace_id = Some([trace; 16]);
        r
    }

    #[test]
    fn an_error_arrives_with_the_debug_lines_that_preceded_it() {
        // The headline feature, in one test.
        let mut ring = Ring::default();
        for i in 0..5 {
            ring.observe(&with_trace(
                line(i, Severity::DEBUG, &format!("step {i}"), 10 + i),
                7,
            ));
        }
        let error = with_trace(line(6, Severity::ERROR, "boom", 99), 7);
        ring.observe(&error);

        let window = expect_window(ring.capture(&error));
        assert_eq!(window.error.body, "boom");
        assert_eq!(window.context.len(), 5);
        assert_eq!(window.context[0].body, "step 0");
        assert_eq!(window.context[4].body, "step 4");
        assert_eq!(window.key_tier, KeyTier::Trace);
    }

    #[test]
    fn context_does_not_leak_between_traces() {
        let mut ring = Ring::default();
        ring.observe(&with_trace(line(1, Severity::DEBUG, "other trace", 1), 1));
        ring.observe(&with_trace(line(2, Severity::DEBUG, "mine", 2), 2));
        let error = with_trace(line(3, Severity::ERROR, "boom", 3), 2);
        ring.observe(&error);

        let window = expect_window(ring.capture(&error));
        assert_eq!(window.context.len(), 1);
        assert_eq!(window.context[0].body, "mine");
    }

    #[test]
    fn correlation_falls_back_through_the_chain() {
        let plain = line(1, Severity::INFO, "x", 1);
        assert_eq!(correlation_key(&plain).tier, KeyTier::Service);

        let mut with_request = plain.clone();
        with_request.attributes = vec![Attr {
            key: "request_id".into(),
            value: AttrValue::Str("abc".into()),
        }];
        assert_eq!(correlation_key(&with_request).tier, KeyTier::Request);

        let mut with_thread = plain.clone();
        with_thread.attributes = vec![
            Attr { key: "host".into(), value: AttrValue::Str("h1".into()) },
            Attr { key: "pid".into(), value: AttrValue::I64(42) },
            Attr { key: "thread".into(), value: AttrValue::Str("w-3".into()) },
        ];
        assert_eq!(correlation_key(&with_thread).tier, KeyTier::Thread);

        assert_eq!(correlation_key(&with_trace(plain, 9)).tier, KeyTier::Trace);
    }

    #[test]
    fn stale_lines_are_not_used_as_context() {
        let mut ring = Ring::new(RingConfig {
            context_age: Duration::from_secs(10),
            ..Default::default()
        });
        ring.observe(&with_trace(line(0, Severity::DEBUG, "ancient", 1), 5));
        ring.observe(&with_trace(line(95, Severity::DEBUG, "recent", 2), 5));
        let error = with_trace(line(100, Severity::ERROR, "boom", 3), 5);
        ring.observe(&error);

        let window = expect_window(ring.capture(&error));
        assert_eq!(window.context.len(), 1);
        assert_eq!(window.context[0].body, "recent");
    }

    #[test]
    fn many_requests_failing_the_same_way_collapse_to_one_window() {
        // The storm that matters: N different requests walking one code path
        // into one failure. Same template sequence, different parameters and
        // different traces — exactly what flow hashing is for.
        let mut ring = Ring::default();
        for req in 0..50u8 {
            ring.observe(&with_trace(
                line(1, Severity::DEBUG, &format!("loading user {req}"), 1),
                req,
            ));
            let error = with_trace(line(2, Severity::ERROR, &format!("timeout for {req}"), 2), req);
            ring.observe(&error);
            ring.capture(&error);
        }
        assert_eq!(ring.stats.windows_emitted, 1, "one shape is one window");
        assert_eq!(ring.stats.windows_deduped, 49);
    }

    #[test]
    fn a_suppressed_context_still_reports_the_error() {
        // Suppressing context must never suppress the error itself — the vendor
        // needs every error event, we are only economising on context.
        let mut ring = Ring::default();
        for req in 0..3u8 {
            ring.observe(&with_trace(line(1, Severity::DEBUG, "loading", 1), req));
            let error = with_trace(line(2, Severity::ERROR, "timeout", 2), req);
            ring.observe(&error);
            let capture = ring.capture(&error);
            if req == 0 {
                assert!(matches!(capture, Capture::Window(_)));
            } else {
                match capture {
                    Capture::ContextSuppressed { reason, flow_hash } => {
                        assert_eq!(reason, Suppressed::DuplicateFlow);
                        assert!(flow_hash.is_some(), "must link to the window it duplicates");
                    }
                    other => panic!("expected suppression, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn a_reopened_window_carries_the_count_it_collapsed() {
        let mut ring = Ring::new(RingConfig {
            dedupe_window: Duration::from_secs(10),
            windows_per_minute: 100,
            ..Default::default()
        });
        // First window, then four collapsed into it.
        for req in 0..5u8 {
            ring.observe(&with_trace(line(1, Severity::DEBUG, "loading", 1), req));
            let error = with_trace(line(2, Severity::ERROR, "timeout", 2), req);
            ring.observe(&error);
            ring.capture(&error);
        }
        // Past the dedupe window: the flow reopens and reports the backlog.
        ring.observe(&with_trace(line(100, Severity::DEBUG, "loading", 1), 200));
        let error = with_trace(line(101, Severity::ERROR, "timeout", 2), 200);
        ring.observe(&error);
        let window = expect_window(ring.capture(&error));
        assert_eq!(window.suppressed, 4, "one window plus a counter");
    }

    #[test]
    fn a_growing_trace_is_not_deduped_away() {
        // Within one trace, retries genuinely accumulate context, so each
        // window is a different flow. Suppressing those would hide the
        // escalation an operator needs to see; the rate limiter bounds them.
        let mut ring = Ring::new(RingConfig {
            windows_per_minute: 100,
            ..Default::default()
        });
        for round in 0..5u64 {
            let t = round * 2;
            ring.observe(&with_trace(line(t, Severity::DEBUG, &format!("attempt {round}"), 1), 3));
            let error = with_trace(line(t + 1, Severity::ERROR, &format!("failed {round}"), 2), 3);
            ring.observe(&error);
            ring.capture(&error);
        }
        assert_eq!(ring.stats.windows_emitted, 5);
        assert_eq!(ring.stats.windows_deduped, 0);
    }

    #[test]
    fn egress_is_bounded_by_distinct_error_shapes_not_error_count() {
        let mut ring = Ring::new(RingConfig {
            windows_per_minute: 3,
            // Disable flow dedupe so the rate limiter is what is under test.
            dedupe_window: Duration::from_nanos(1),
            ..Default::default()
        });
        // All 100 errors inside one minute (10 ms apart), so the budget cannot
        // legitimately refill mid-test.
        for i in 0..100u64 {
            let ts = i * 10_000_000; // 10 ms
            let mut ctx = line(0, Severity::DEBUG, "ctx", 1000 + i);
            ctx.observed_unix_nano = ts;
            ctx.trace_id = Some([i as u8; 16]);
            ring.observe(&ctx);

            let mut error = line(0, Severity::ERROR, "same error", 7);
            error.observed_unix_nano = ts + 1_000_000;
            error.trace_id = Some([i as u8; 16]);
            ring.observe(&error);
            ring.capture(&error);
        }
        assert_eq!(
            ring.stats.windows_emitted, 3,
            "one minute, three windows per (service, template)"
        );
        assert!(ring.stats.windows_rate_limited > 90);
    }

    #[test]
    fn memory_stays_bounded_under_an_error_storm() {
        // 100k lines across 10k correlation keys through a 1 MB ring.
        let budget = 1024 * 1024;
        let mut ring = Ring::new(RingConfig {
            max_bytes: budget,
            ..Default::default()
        });
        for i in 0..100_000u64 {
            let mut r = line(i, Severity::DEBUG, "a reasonably long log line for bulk", 1);
            r.trace_id = Some([(i % 10_000) as u8; 16]);
            r.service = Some(format!("svc{}", i % 10_000));
            ring.observe(&r);
        }
        assert!(
            ring.bytes_held() <= budget,
            "held {} over budget {budget}",
            ring.bytes_held()
        );
        assert!(ring.stats.lines_evicted > 0, "eviction must actually happen");
    }

    #[test]
    fn one_chatty_key_cannot_evict_everyone_else() {
        let mut ring = Ring::new(RingConfig {
            max_lines_per_key: 10,
            ..Default::default()
        });
        ring.observe(&with_trace(line(0, Severity::DEBUG, "quiet neighbour", 1), 1));
        for i in 0..1000 {
            ring.observe(&with_trace(line(i % 5, Severity::DEBUG, "chatter", 2), 2));
        }
        let error = with_trace(line(5, Severity::ERROR, "boom", 3), 1);
        ring.observe(&error);

        let window = expect_window(ring.capture(&error));
        assert!(
            window.context.iter().any(|l| l.body == "quiet neighbour"),
            "the quiet key's context survived the chatty one"
        );
    }

    #[test]
    fn an_error_with_no_preceding_lines_yields_no_window() {
        let mut ring = Ring::default();
        let error = with_trace(line(1, Severity::ERROR, "lonely", 1), 4);
        ring.observe(&error);
        assert!(matches!(
            ring.capture(&error),
            Capture::ContextSuppressed { reason: Suppressed::NoContext, .. }
        ));
    }

    #[test]
    fn flow_hash_ignores_parameters_but_not_shape() {
        let a = vec![ContextLine {
            timestamp_unix_nano: 1,
            severity: 5,
            template_id: Some(10),
            body: "id=1".into(),
        }];
        let b = vec![ContextLine {
            timestamp_unix_nano: 2,
            severity: 5,
            template_id: Some(10),
            body: "id=99999".into(),
        }];
        let c = vec![ContextLine {
            timestamp_unix_nano: 3,
            severity: 5,
            template_id: Some(11),
            body: "id=1".into(),
        }];
        assert_eq!(flow_hash(&a, Some(1)), flow_hash(&b, Some(1)));
        assert_ne!(flow_hash(&a, Some(1)), flow_hash(&c, Some(1)));
        assert_ne!(flow_hash(&a, Some(1)), flow_hash(&a, Some(2)));
    }
}
