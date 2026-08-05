//! Sending proxied envelopes to Sentry, and honouring what Sentry says back.
//!
//! The part that makes a proxy well-behaved rather than merely functional is
//! rate limiting. Sentry answers `429` with `X-Sentry-Rate-Limits`, naming the
//! categories it is refusing and for how long. A proxy that ignores it keeps
//! hammering an endpoint that is already saying no, and — worse — leaves the
//! SDKs behind it unaware, because their own backoff logic never sees the
//! signal. So limits are recorded, applied *before* sending, and reflected back
//! to clients in the proxy's own responses.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::dsn::{auth_header, UpstreamTarget};
use super::envelope::{Envelope, ItemType};
use crate::forward::Transport;

/// `sentry_client` we present upstream. Sentry surfaces this in the UI, and an
/// operator seeing an unfamiliar event should be able to tell it came through
/// here.
pub const CLIENT: &str = "logless-proxy/0.1";

#[derive(Debug, Default)]
pub struct UpstreamStats {
    pub envelopes_sent: AtomicU64,
    pub items_sent: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub failures: AtomicU64,
    pub retries: AtomicU64,
    /// Items not sent because a limit for their category was already active.
    pub items_rate_limited: AtomicU64,
    pub rate_limit_responses: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpstreamSnapshot {
    pub envelopes_sent: u64,
    pub items_sent: u64,
    pub bytes_sent: u64,
    pub failures: u64,
    pub retries: u64,
    pub items_rate_limited: u64,
    pub rate_limit_responses: u64,
}

impl UpstreamStats {
    pub fn snapshot(&self) -> UpstreamSnapshot {
        UpstreamSnapshot {
            envelopes_sent: self.envelopes_sent.load(Ordering::Relaxed),
            items_sent: self.items_sent.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            items_rate_limited: self.items_rate_limited.load(Ordering::Relaxed),
            rate_limit_responses: self.rate_limit_responses.load(Ordering::Relaxed),
        }
    }
}

/// Active rate limits, by category. `None` as a key means "all categories".
#[derive(Debug, Default)]
pub struct RateLimits {
    /// category → unix second the limit expires.
    until: Mutex<HashMap<String, u64>>,
    /// The raw header, so it can be handed back to clients verbatim.
    raw: Mutex<Option<(String, u64)>>,
}

impl RateLimits {
    /// Records limits from an upstream response.
    ///
    /// Format: `retry_after:categories:scope:reason:namespaces`, several
    /// separated by `,`. An empty category list means every category.
    pub fn record(&self, header: &str, now_unix_secs: u64) {
        let mut until = self.lock_until();
        for limit in header.split(',') {
            let mut parts = limit.trim().split(':');
            let Some(seconds) = parts.next().and_then(|s| s.trim().parse::<f64>().ok()) else {
                continue;
            };
            let expiry = now_unix_secs + seconds.ceil() as u64;
            let categories = parts.next().unwrap_or("").trim();
            if categories.is_empty() {
                until.insert(ALL.to_string(), expiry);
            } else {
                for category in categories.split(';') {
                    let category = category.trim();
                    if !category.is_empty() {
                        let entry = until.entry(category.to_string()).or_insert(0);
                        *entry = (*entry).max(expiry);
                    }
                }
            }
        }
        drop(until);
        let longest = self.lock_until().values().copied().max().unwrap_or(0);
        *self.lock_raw() = Some((header.to_string(), longest));
    }

    /// Records a bare `Retry-After`, which is what Sentry sends when it is
    /// refusing everything rather than one category.
    pub fn record_retry_after(&self, seconds: u64, now_unix_secs: u64) {
        self.lock_until().insert(ALL.to_string(), now_unix_secs + seconds);
        *self.lock_raw() = Some((format!("{seconds}::organization:proxy"), now_unix_secs + seconds));
    }

    pub fn is_limited(&self, item_type: ItemType, now_unix_secs: u64) -> bool {
        let until = self.lock_until();
        let category = item_type.rate_limit_category();
        until.get(ALL).is_some_and(|t| *t > now_unix_secs)
            || until.get(category).is_some_and(|t| *t > now_unix_secs)
    }

    /// The header to hand back to clients, if a limit is still active. Passing
    /// Sentry's own wording through means the SDK backs off exactly as it would
    /// have without the proxy in the way.
    pub fn client_header(&self, now_unix_secs: u64) -> Option<String> {
        let raw = self.lock_raw();
        match raw.as_ref() {
            Some((header, expiry)) if *expiry > now_unix_secs => Some(header.clone()),
            _ => None,
        }
    }

    fn lock_until(&self) -> std::sync::MutexGuard<'_, HashMap<String, u64>> {
        self.until.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn lock_raw(&self) -> std::sync::MutexGuard<'_, Option<(String, u64)>> {
        self.raw.lock().unwrap_or_else(|p| p.into_inner())
    }
}

const ALL: &str = "*";

/// What happened to one envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    Sent { status: u16 },
    /// Every item was dropped by an active rate limit, so nothing was sent.
    RateLimited,
    /// Nothing left to send after policy filtering.
    Nothing,
    Failed { message: String },
}

pub struct Upstream {
    transport: Box<dyn Transport>,
    pub stats: Arc<UpstreamStats>,
    pub limits: Arc<RateLimits>,
    max_attempts: u32,
}

impl Upstream {
    pub fn new(transport: Box<dyn Transport>) -> Self {
        Self {
            transport,
            stats: Arc::new(UpstreamStats::default()),
            limits: Arc::new(RateLimits::default()),
            max_attempts: 3,
        }
    }

    pub fn with_limits(mut self, limits: Arc<RateLimits>) -> Self {
        self.limits = limits;
        self
    }

    /// Sends an envelope, dropping items whose category is currently limited.
    pub fn send(
        &self,
        target: &UpstreamTarget,
        envelope: &Envelope,
        now_unix_secs: u64,
    ) -> Delivery {
        let mut envelope = envelope.clone();
        let before = envelope.items.len();
        let limits = Arc::clone(&self.limits);
        envelope.retain_items(|item| !limits.is_limited(item.item_type, now_unix_secs));
        let dropped = before - envelope.items.len();
        if dropped > 0 {
            self.stats.items_rate_limited.fetch_add(dropped as u64, Ordering::Relaxed);
        }
        if envelope.is_empty() {
            return if before > 0 { Delivery::RateLimited } else { Delivery::Nothing };
        }

        let body = envelope.to_bytes();
        let headers = vec![
            ("Content-Type".to_string(), "application/x-sentry-envelope".to_string()),
            ("X-Sentry-Auth".to_string(), auth_header(&target.key, CLIENT)),
        ];

        let mut last_error = String::new();
        for attempt in 1..=self.max_attempts {
            match self.transport.post_full(&target.url, &headers, &body) {
                Ok(response) => {
                    self.absorb_limits(&response, now_unix_secs);
                    if response.status == 429 {
                        self.stats.rate_limit_responses.fetch_add(1, Ordering::Relaxed);
                        // Retrying a 429 immediately is the behaviour the
                        // header exists to prevent.
                        return Delivery::Sent { status: 429 };
                    }
                    if (500..600).contains(&response.status) && attempt < self.max_attempts {
                        self.stats.retries.fetch_add(1, Ordering::Relaxed);
                        last_error = format!("upstream {}", response.status);
                        continue;
                    }
                    if response.status >= 400 {
                        // 4xx other than 429 is a permanent verdict on this
                        // payload — retrying sends the same bad bytes again.
                        self.stats.failures.fetch_add(1, Ordering::Relaxed);
                        return Delivery::Sent { status: response.status };
                    }
                    self.stats.envelopes_sent.fetch_add(1, Ordering::Relaxed);
                    self.stats.items_sent.fetch_add(envelope.items.len() as u64, Ordering::Relaxed);
                    self.stats.bytes_sent.fetch_add(body.len() as u64, Ordering::Relaxed);
                    return Delivery::Sent { status: response.status };
                }
                Err(e) => {
                    last_error = e;
                    if attempt < self.max_attempts {
                        self.stats.retries.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        self.stats.failures.fetch_add(1, Ordering::Relaxed);
        Delivery::Failed { message: last_error }
    }

    fn absorb_limits(&self, response: &crate::forward::Response, now_unix_secs: u64) {
        if let Some(header) = response.header("x-sentry-rate-limits") {
            if !header.trim().is_empty() {
                self.limits.record(header, now_unix_secs);
                return;
            }
        }
        if response.status == 429 {
            let seconds = response
                .header("retry-after")
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(60);
            self.limits.record_retry_after(seconds, now_unix_secs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forward::Response;

    /// (url, headers, body) as it went out.
    type SentRequest = (String, Vec<(String, String)>, Vec<u8>);

    #[derive(Default)]
    struct FakeUpstream {
        sent: Mutex<Vec<SentRequest>>,
        replies: Mutex<Vec<Response>>,
    }

    impl FakeUpstream {
        fn with(replies: Vec<Response>) -> Arc<Self> {
            Arc::new(Self { sent: Mutex::new(Vec::new()), replies: Mutex::new(replies) })
        }
    }

    struct Handle(Arc<FakeUpstream>);

    impl Transport for Handle {
        fn post(&self, _u: &str, _h: &[(String, String)], _b: &[u8]) -> Result<u16, String> {
            unreachable!("the proxy uses post_full")
        }
        fn post_full(
            &self,
            url: &str,
            headers: &[(String, String)],
            body: &[u8],
        ) -> Result<Response, String> {
            self.0.sent.lock().unwrap().push((
                url.to_string(),
                headers.to_vec(),
                body.to_vec(),
            ));
            let mut replies = self.0.replies.lock().unwrap();
            if replies.is_empty() {
                return Ok(Response { status: 200, ..Default::default() });
            }
            Ok(replies.remove(0))
        }
    }

    fn envelope(items: &[(&str, &str)]) -> Envelope {
        let mut body = String::from("{\"event_id\":\"abc\"}\n");
        for (kind, payload) in items {
            body.push_str(&format!(
                "{{\"type\":\"{kind}\",\"length\":{}}}\n{payload}\n",
                payload.len()
            ));
        }
        Envelope::parse(body.as_bytes()).unwrap()
    }

    fn target() -> UpstreamTarget {
        UpstreamTarget {
            url: "https://sentry.io/api/1/envelope/".into(),
            key: "thekey".into(),
            project: "1".into(),
        }
    }

    #[test]
    fn sends_with_the_targets_key_in_the_auth_header() {
        let fake = FakeUpstream::with(vec![]);
        let upstream = Upstream::new(Box::new(Handle(Arc::clone(&fake))));
        assert_eq!(
            upstream.send(&target(), &envelope(&[("event", "{}")]), 0),
            Delivery::Sent { status: 200 }
        );
        let sent = fake.sent.lock().unwrap();
        assert_eq!(sent[0].0, "https://sentry.io/api/1/envelope/");
        let auth = sent[0].1.iter().find(|(k, _)| k == "X-Sentry-Auth").unwrap();
        assert!(auth.1.contains("sentry_key=thekey"), "{}", auth.1);
        assert!(auth.1.contains("sentry_version=7"));
        let content_type = sent[0].1.iter().find(|(k, _)| k == "Content-Type").unwrap();
        assert_eq!(content_type.1, "application/x-sentry-envelope");
    }

    #[test]
    fn parses_rate_limits_and_stops_sending_that_category() {
        let fake = FakeUpstream::with(vec![Response {
            status: 429,
            headers: vec![(
                "X-Sentry-Rate-Limits".into(),
                "60:transaction:organization:quota_exceeded".into(),
            )],
            body: Vec::new(),
        }]);
        let upstream = Upstream::new(Box::new(Handle(Arc::clone(&fake))));
        upstream.send(&target(), &envelope(&[("transaction", "{}")]), 1_000);

        assert!(upstream.limits.is_limited(ItemType::Transaction, 1_030));
        assert!(!upstream.limits.is_limited(ItemType::Event, 1_030), "only that category");
        assert!(!upstream.limits.is_limited(ItemType::Transaction, 1_061), "and only until it expires");

        // A later transaction is dropped before it reaches the wire.
        let before = fake.sent.lock().unwrap().len();
        assert_eq!(
            upstream.send(&target(), &envelope(&[("transaction", "{}")]), 1_030),
            Delivery::RateLimited
        );
        assert_eq!(fake.sent.lock().unwrap().len(), before, "nothing sent while limited");
        assert_eq!(upstream.stats.snapshot().items_rate_limited, 1);
    }

    #[test]
    fn a_limited_category_does_not_block_the_rest_of_the_envelope() {
        let fake = FakeUpstream::with(vec![Response {
            status: 429,
            headers: vec![("X-Sentry-Rate-Limits".into(), "60:transaction".into())],
            body: Vec::new(),
        }]);
        let upstream = Upstream::new(Box::new(Handle(Arc::clone(&fake))));
        upstream.send(&target(), &envelope(&[("transaction", "{}")]), 0);

        let mixed = envelope(&[("transaction", "{}"), ("event", "{\"m\":1}")]);
        assert_eq!(upstream.send(&target(), &mixed, 10), Delivery::Sent { status: 200 });
        let sent = fake.sent.lock().unwrap();
        let body = String::from_utf8_lossy(&sent.last().unwrap().2);
        assert!(body.contains("event"), "{body}");
        assert!(!body.contains("transaction"), "the limited item must be dropped: {body}");
    }

    #[test]
    fn an_empty_category_list_limits_everything() {
        let limits = RateLimits::default();
        limits.record("30::organization:quota", 100);
        for kind in [ItemType::Event, ItemType::Transaction, ItemType::Attachment] {
            assert!(limits.is_limited(kind, 120), "{kind:?}");
        }
        assert!(!limits.is_limited(ItemType::Event, 131));
    }

    #[test]
    fn several_limits_in_one_header_all_apply() {
        let limits = RateLimits::default();
        limits.record("60:transaction:organization,120:error:project", 0);
        assert!(limits.is_limited(ItemType::Transaction, 59));
        assert!(!limits.is_limited(ItemType::Transaction, 61));
        assert!(limits.is_limited(ItemType::Event, 119));
    }

    #[test]
    fn semicolon_separated_categories_all_apply() {
        // Sentry groups categories with `;` inside one limit.
        let limits = RateLimits::default();
        limits.record("60:error;transaction:organization", 0);
        assert!(limits.is_limited(ItemType::Event, 10));
        assert!(limits.is_limited(ItemType::Transaction, 10));
    }

    #[test]
    fn a_bare_retry_after_limits_everything() {
        let fake = FakeUpstream::with(vec![Response {
            status: 429,
            headers: vec![("Retry-After".into(), "45".into())],
            body: Vec::new(),
        }]);
        let upstream = Upstream::new(Box::new(Handle(Arc::clone(&fake))));
        upstream.send(&target(), &envelope(&[("event", "{}")]), 1_000);
        assert!(upstream.limits.is_limited(ItemType::Event, 1_040));
        assert!(!upstream.limits.is_limited(ItemType::Event, 1_046));
    }

    #[test]
    fn the_client_header_is_sentrys_own_wording() {
        // Passing it through unchanged means the SDK backs off exactly as it
        // would have without a proxy in the way.
        let limits = RateLimits::default();
        assert_eq!(limits.client_header(0), None);
        limits.record("60:transaction:organization:quota_exceeded", 0);
        assert_eq!(
            limits.client_header(10).as_deref(),
            Some("60:transaction:organization:quota_exceeded")
        );
        assert_eq!(limits.client_header(61), None, "expired limits are not advertised");
    }

    #[test]
    fn server_errors_retry_and_client_errors_do_not() {
        let fake = FakeUpstream::with(vec![
            Response { status: 503, ..Default::default() },
            Response { status: 200, ..Default::default() },
        ]);
        let upstream = Upstream::new(Box::new(Handle(Arc::clone(&fake))));
        assert_eq!(
            upstream.send(&target(), &envelope(&[("event", "{}")]), 0),
            Delivery::Sent { status: 200 }
        );
        assert_eq!(fake.sent.lock().unwrap().len(), 2);
        assert_eq!(upstream.stats.snapshot().retries, 1);

        let fake = FakeUpstream::with(vec![Response { status: 400, ..Default::default() }]);
        let upstream = Upstream::new(Box::new(Handle(Arc::clone(&fake))));
        assert_eq!(
            upstream.send(&target(), &envelope(&[("event", "{}")]), 0),
            Delivery::Sent { status: 400 }
        );
        assert_eq!(fake.sent.lock().unwrap().len(), 1, "a 400 is not retried");
    }

    #[test]
    fn a_429_is_not_retried() {
        let fake = FakeUpstream::with(vec![
            Response {
                status: 429,
                headers: vec![("X-Sentry-Rate-Limits".into(), "60:error".into())],
                body: Vec::new(),
            },
            Response { status: 200, ..Default::default() },
        ]);
        let upstream = Upstream::new(Box::new(Handle(Arc::clone(&fake))));
        assert_eq!(
            upstream.send(&target(), &envelope(&[("event", "{}")]), 0),
            Delivery::Sent { status: 429 }
        );
        assert_eq!(fake.sent.lock().unwrap().len(), 1, "retrying is what the header forbids");
    }

    #[test]
    fn network_failures_retry_then_report() {
        struct Dead;
        impl Transport for Dead {
            fn post(&self, _: &str, _: &[(String, String)], _: &[u8]) -> Result<u16, String> {
                Err("connection refused".into())
            }
            fn post_full(
                &self,
                _: &str,
                _: &[(String, String)],
                _: &[u8],
            ) -> Result<Response, String> {
                Err("connection refused".into())
            }
        }
        let upstream = Upstream::new(Box::new(Dead));
        assert_eq!(
            upstream.send(&target(), &envelope(&[("event", "{}")]), 0),
            Delivery::Failed { message: "connection refused".into() }
        );
        assert_eq!(upstream.stats.snapshot().failures, 1);
        assert_eq!(upstream.stats.snapshot().retries, 2);
    }

    #[test]
    fn an_envelope_with_no_items_is_not_sent() {
        let fake = FakeUpstream::with(vec![]);
        let upstream = Upstream::new(Box::new(Handle(Arc::clone(&fake))));
        assert_eq!(upstream.send(&target(), &envelope(&[]), 0), Delivery::Nothing);
        assert!(fake.sent.lock().unwrap().is_empty(), "Sentry records nothing for these");
    }
}
