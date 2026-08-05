//! Forwarding to the existing observability vendor.
//!
//! Per `docs/architecture.md` §5, the same dedupe decision is *shaped
//! differently per destination*, because the vendors differ:
//!
//! * **Sentry** groups and counts natively. So every error is sent, with
//!   `fingerprint = [template_id]` to make that grouping deterministic, and the
//!   context attached as breadcrumbs on the first error of each flow. We do not
//!   aggregate — that would fight the UI.
//! * **Splunk** bills per GB and has no native grouping. So repeated errors are
//!   rolled up into one aggregate event carrying a count, first/last seen and
//!   top parameters.
//!
//! Payload construction is pure and separately tested; the network sits behind
//! [`Transport`] so shaping can be verified without a vendor account.
//!
//! Delivery is **at-least-once with idempotency keys** (the record's UUIDv7).
//! Exactly-once to Sentry or Splunk is fiction — we do not control their dedupe.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::ring::{ContextWindow, Suppressed};

pub mod payload;

pub use payload::{sentry_envelope, splunk_aggregate, splunk_event};

#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    #[error("{destination} rejected the payload: HTTP {status}")]
    Rejected { destination: String, status: u16 },
    #[error("{destination} transport failure: {message}")]
    Transport {
        destination: String,
        message: String,
    },
}

/// HTTP for the forwarders. Behind a trait so payload shaping and retry
/// behaviour are testable without a network or a vendor account.
pub trait Transport: Send {
    fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<u16, String>;

    /// Full response, for callers that need more than a status code.
    ///
    /// The Sentry proxy needs the response headers (`X-Sentry-Rate-Limits`,
    /// `Retry-After`) and the body (the event id Sentry assigns). The default
    /// keeps every existing implementation working — it just cannot report
    /// what it never read.
    fn post_full(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<Response, String> {
        self.post(url, headers, body)
            .map(|status| Response { status, headers: Vec::new(), body: Vec::new() })
    }
}

/// An upstream response, headers included.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    /// Case-insensitive header lookup.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Real HTTP, blocking — the agent's forwarder owns a thread, so async would
/// buy nothing here and cost a runtime.
pub struct HttpTransport {
    agent: ureq::Agent,
}

impl Default for HttpTransport {
    fn default() -> Self {
        Self::new(Duration::from_secs(10))
    }
}

impl HttpTransport {
    pub fn new(timeout: Duration) -> Self {
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .build();
        Self {
            agent: config.into(),
        }
    }
}

impl Transport for HttpTransport {
    fn post(&self, url: &str, headers: &[(String, String)], body: &[u8]) -> Result<u16, String> {
        let mut request = self.agent.post(url);
        for (name, value) in headers {
            request = request.header(name.as_str(), value.as_str());
        }
        match request.send(body) {
            Ok(response) => Ok(response.status().as_u16()),
            Err(ureq::Error::StatusCode(code)) => Ok(code),
            Err(e) => Err(e.to_string()),
        }
    }

    fn post_full(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<Response, String> {
        // Read the body on every status: an error response is where Sentry
        // explains itself, and a proxy that discards it has nothing to relay
        // back to the SDK.
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .http_status_as_error(false)
            .build()
            .into();
        let mut request = agent.post(url);
        for (name, value) in headers {
            request = request.header(name.as_str(), value.as_str());
        }
        match request.send(body) {
            Ok(mut response) => {
                let status = response.status().as_u16();
                let headers = response
                    .headers()
                    .iter()
                    .map(|(k, v)| {
                        (k.as_str().to_string(), v.to_str().unwrap_or("").to_string())
                    })
                    .collect();
                let body = response
                    .body_mut()
                    .read_to_vec()
                    .unwrap_or_default();
                Ok(Response { status, headers, body })
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Destination {
    /// Sentry envelope endpoint, built from a DSN.
    Sentry {
        /// `https://<key>@<host>/<project>` — the DSN as Sentry issues it.
        dsn: String,
    },
    /// Splunk HTTP Event Collector.
    SplunkHec {
        /// Base URL, e.g. `https://splunk:8088`.
        url: String,
        token: String,
        /// Roll repeated errors into aggregate events rather than sending each.
        #[serde(default = "default_aggregate")]
        aggregate: bool,
    },
}

fn default_aggregate() -> bool {
    true
}

impl Destination {
    /// True when repeated errors should be rolled up rather than sent
    /// individually. Sentry never aggregates: it groups natively, and an
    /// aggregate event would fight its UI.
    pub fn aggregates(&self) -> bool {
        matches!(self, Destination::SplunkHec { aggregate: true, .. })
    }

    pub fn name(&self) -> &'static str {
        match self {
            Destination::Sentry { .. } => "sentry",
            Destination::SplunkHec { .. } => "splunk",
        }
    }

    /// Endpoint URL and headers for a POST.
    fn request(&self) -> Result<(String, Vec<(String, String)>), ForwardError> {
        match self {
            Destination::Sentry { dsn } => {
                let parsed = parse_dsn(dsn).ok_or_else(|| ForwardError::Transport {
                    destination: "sentry".into(),
                    message: format!("malformed DSN: {dsn}"),
                })?;
                Ok((
                    format!("{}/api/{}/envelope/", parsed.origin, parsed.project),
                    vec![
                        ("Content-Type".into(), "application/x-sentry-envelope".into()),
                        (
                            "X-Sentry-Auth".into(),
                            format!(
                                "Sentry sentry_version=7, sentry_client=logless/0.1, sentry_key={}",
                                parsed.key
                            ),
                        ),
                    ],
                ))
            }
            Destination::SplunkHec { url, token, .. } => Ok((
                format!("{}/services/collector/event", url.trim_end_matches('/')),
                vec![
                    ("Authorization".into(), format!("Splunk {token}")),
                    ("Content-Type".into(), "application/json".into()),
                ],
            )),
        }
    }
}

#[derive(Debug, PartialEq)]
struct Dsn {
    origin: String,
    key: String,
    project: String,
}

/// Parse `https://<key>@<host>/<project>`.
fn parse_dsn(dsn: &str) -> Option<Dsn> {
    let (scheme, rest) = dsn.split_once("://")?;
    let (key, host_and_path) = rest.split_once('@')?;
    let (host, project) = host_and_path.rsplit_once('/')?;
    if key.is_empty() || host.is_empty() || project.is_empty() {
        return None;
    }
    Some(Dsn {
        origin: format!("{scheme}://{host}"),
        key: key.to_string(),
        project: project.to_string(),
    })
}

/// Rolls repeated errors into aggregate events for destinations that have no
/// native grouping.
///
/// The guardrail from §5: **collapsing makes things cheaper, never invisible.**
/// Every suppressed error still increments a counter that is flushed upstream,
/// so a collapsed storm always leaves a trace.
#[derive(Debug, Default)]
pub struct Aggregator {
    rollups: BTreeMap<(String, u64), Rollup>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Rollup {
    pub service: String,
    pub template_id: u64,
    pub template_text: String,
    pub count: u64,
    pub first_seen_unix_nano: u64,
    pub last_seen_unix_nano: u64,
    /// One real line, so the aggregate is actionable rather than abstract.
    pub example: String,
}

impl Aggregator {
    /// Record an error whose context was suppressed.
    pub fn record(
        &mut self,
        service: &str,
        template_id: u64,
        template_text: &str,
        body: &str,
        ts_unix_nano: u64,
    ) {
        let entry = self
            .rollups
            .entry((service.to_string(), template_id))
            .or_insert_with(|| Rollup {
                service: service.to_string(),
                template_id,
                template_text: template_text.to_string(),
                count: 0,
                first_seen_unix_nano: ts_unix_nano,
                last_seen_unix_nano: ts_unix_nano,
                example: body.to_string(),
            });
        entry.count += 1;
        entry.last_seen_unix_nano = entry.last_seen_unix_nano.max(ts_unix_nano);
        entry.first_seen_unix_nano = entry.first_seen_unix_nano.min(ts_unix_nano);
    }

    pub fn is_empty(&self) -> bool {
        self.rollups.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rollups.len()
    }

    /// Take everything accumulated so far, leaving the aggregator empty.
    pub fn drain(&mut self) -> Vec<Rollup> {
        std::mem::take(&mut self.rollups).into_values().collect()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct ForwardStats {
    pub events_sent: u64,
    pub aggregates_sent: u64,
    pub bytes_sent: u64,
    pub failures: u64,
    pub retries: u64,
    /// Bytes of raw log seen that were never sent — the number the whole
    /// product is judged on.
    pub bytes_avoided: u64,
}

/// Sends shaped payloads to one destination, with bounded retries.
pub struct Forwarder {
    destination: Destination,
    transport: Box<dyn Transport>,
    max_retries: u32,
    pub stats: ForwardStats,
}

impl Forwarder {
    pub fn new(destination: Destination, transport: Box<dyn Transport>) -> Self {
        Self {
            destination,
            transport,
            max_retries: 2,
            stats: ForwardStats::default(),
        }
    }

    pub fn destination(&self) -> &Destination {
        &self.destination
    }

    /// Forward an error, with context if the ring provided one.
    pub fn send_window(&mut self, window: &ContextWindow) -> Result<(), ForwardError> {
        let body = match &self.destination {
            Destination::Sentry { .. } => sentry_envelope(window),
            Destination::SplunkHec { .. } => splunk_event(window),
        };
        self.post(body.as_bytes())?;
        self.stats.events_sent += 1;
        Ok(())
    }

    /// Forward an error whose context was withheld.
    pub fn send_error_only(
        &mut self,
        window: &ContextWindow,
        reason: Suppressed,
    ) -> Result<(), ForwardError> {
        let stripped = ContextWindow {
            context: Vec::new(),
            ..window.clone()
        };
        let body = match &self.destination {
            Destination::Sentry { .. } => sentry_envelope(&stripped),
            Destination::SplunkHec { .. } => splunk_event(&stripped),
        };
        let _ = reason;
        self.post(body.as_bytes())?;
        self.stats.events_sent += 1;
        Ok(())
    }

    pub fn send_aggregate(&mut self, rollup: &Rollup) -> Result<(), ForwardError> {
        let body = splunk_aggregate(rollup);
        self.post(body.as_bytes())?;
        self.stats.aggregates_sent += 1;
        Ok(())
    }

    fn post(&mut self, body: &[u8]) -> Result<(), ForwardError> {
        let (url, headers) = self.destination.request()?;
        let name = self.destination.name().to_string();

        let mut attempt = 0;
        loop {
            match self.transport.post(&url, &headers, body) {
                Ok(status) if (200..300).contains(&status) => {
                    self.stats.bytes_sent += body.len() as u64;
                    return Ok(());
                }
                // 4xx other than 429 will never succeed; retrying wastes the
                // budget and delays everything behind it.
                Ok(status) if (400..500).contains(&status) && status != 429 => {
                    self.stats.failures += 1;
                    return Err(ForwardError::Rejected {
                        destination: name,
                        status,
                    });
                }
                Ok(status) => {
                    if attempt >= self.max_retries {
                        self.stats.failures += 1;
                        return Err(ForwardError::Rejected {
                            destination: name,
                            status,
                        });
                    }
                }
                Err(message) => {
                    if attempt >= self.max_retries {
                        self.stats.failures += 1;
                        return Err(ForwardError::Transport {
                            destination: name,
                            message,
                        });
                    }
                }
            }
            attempt += 1;
            self.stats.retries += 1;
        }
    }
}

/// One error handed to the dispatcher.
///
/// Carries everything the per-destination shaping needs, so the decision of
/// *how* to say it stays with the destination rather than the ingest path.
#[derive(Debug, Clone, PartialEq)]
pub struct Outbound {
    pub window: ContextWindow,
    /// False when the ring withheld context; the error is still forwarded.
    pub had_context: bool,
    pub reason: Option<Suppressed>,
    /// Masked template text, for aggregate events that report a shape.
    pub template_text: String,
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct DispatchStats {
    pub handled: u64,
    pub aggregated: u64,
    pub flushes: u64,
    /// Dropped because the outbound queue was full — a slow vendor must never
    /// become backpressure on the application.
    pub dropped_queue_full: u64,
}

/// Fans one error out to every destination, applying per-destination shaping,
/// and flushes rollups on an interval.
///
/// Separated from the thread that drives it so the shaping decisions are
/// testable without concurrency.
pub struct Dispatch {
    forwarders: Vec<Forwarder>,
    aggregator: Aggregator,
    flush_interval: Duration,
    last_flush: Option<std::time::Instant>,
    pub stats: DispatchStats,
}

impl Dispatch {
    pub fn new(forwarders: Vec<Forwarder>, flush_interval: Duration) -> Self {
        Self {
            forwarders,
            aggregator: Aggregator::default(),
            flush_interval,
            last_flush: None,
            stats: DispatchStats::default(),
        }
    }

    pub fn forwarders(&self) -> &[Forwarder] {
        &self.forwarders
    }

    pub fn pending_rollups(&self) -> usize {
        self.aggregator.len()
    }

    pub fn handle(&mut self, outbound: &Outbound) {
        self.stats.handled += 1;
        for forwarder in &mut self.forwarders {
            let result = if outbound.had_context {
                forwarder.send_window(&outbound.window)
            } else if forwarder.destination().aggregates() {
                // Rolled up rather than re-sent. The count reaches the vendor at
                // the next flush, so the storm is cheaper but never invisible.
                self.aggregator.record(
                    outbound.window.service.as_deref().unwrap_or(""),
                    outbound.window.error_template_id.unwrap_or(0),
                    &outbound.template_text,
                    &outbound.window.error.body,
                    outbound.window.error.timestamp_unix_nano,
                );
                self.stats.aggregated += 1;
                Ok(())
            } else {
                forwarder.send_error_only(
                    &outbound.window,
                    outbound.reason.unwrap_or(Suppressed::DuplicateFlow),
                )
            };
            if let Err(e) = result {
                tracing::warn!(error = %e, "forward failed");
            }
        }
    }

    /// Flush rollups if the interval has elapsed. `force` flushes regardless,
    /// which is what shutdown does.
    pub fn flush_if_due(&mut self, force: bool) {
        let due = match self.last_flush {
            None => force,
            Some(last) => force || last.elapsed() >= self.flush_interval,
        };
        if !due {
            return;
        }
        self.last_flush = Some(std::time::Instant::now());
        if self.aggregator.is_empty() {
            return;
        }
        let rollups = self.aggregator.drain();
        self.stats.flushes += 1;
        for forwarder in &mut self.forwarders {
            if !forwarder.destination().aggregates() {
                continue;
            }
            for rollup in &rollups {
                if let Err(e) = forwarder.send_aggregate(rollup) {
                    tracing::warn!(error = %e, "aggregate forward failed");
                }
            }
        }
    }

    /// Combined stats across destinations.
    pub fn totals(&self) -> ForwardStats {
        let mut total = ForwardStats::default();
        for f in &self.forwarders {
            total.events_sent += f.stats.events_sent;
            total.aggregates_sent += f.stats.aggregates_sent;
            total.bytes_sent += f.stats.bytes_sent;
            total.failures += f.stats.failures;
            total.retries += f.stats.retries;
        }
        total
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// url, headers, body — one captured request.
    type Request = (String, Vec<(String, String)>, String);

    /// Records every request and replays a scripted sequence of responses.
    #[derive(Clone, Default)]
    pub struct RecordingTransport {
        pub requests: Arc<Mutex<Vec<Request>>>,
        pub responses: Arc<Mutex<Vec<Result<u16, String>>>>,
    }

    impl RecordingTransport {
        pub fn ok() -> Self {
            Self::default()
        }

        pub fn scripted(responses: Vec<Result<u16, String>>) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(responses)),
            }
        }

        pub fn bodies(&self) -> Vec<String> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|(_, _, b)| b.clone())
                .collect()
        }

        pub fn urls(&self) -> Vec<String> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|(u, _, _)| u.clone())
                .collect()
        }

        pub fn headers(&self) -> Vec<Vec<(String, String)>> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|(_, h, _)| h.clone())
                .collect()
        }
    }

    impl Transport for RecordingTransport {
        fn post(
            &self,
            url: &str,
            headers: &[(String, String)],
            body: &[u8],
        ) -> Result<u16, String> {
            self.requests.lock().unwrap().push((
                url.to_string(),
                headers.to_vec(),
                String::from_utf8_lossy(body).to_string(),
            ));
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                Ok(200)
            } else {
                responses.remove(0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::RecordingTransport;
    use super::*;
    use crate::ring::{ContextLine, KeyTier};

    fn window() -> ContextWindow {
        ContextWindow {
            error: ContextLine {
                timestamp_unix_nano: 1_700_000_000_000_000_000,
                severity: 17,
                template_id: Some(7),
                body: "payment gateway timeout after 4181ms".into(),
            },
            context: vec![ContextLine {
                timestamp_unix_nano: 1_699_999_999_000_000_000,
                severity: 5,
                template_id: Some(2),
                body: "received request path=/v1/orders/7".into(),
            }],
            key_tier: KeyTier::Trace,
            flow_hash: 0xdead_beef,
            error_template_id: Some(7),
            service: Some("api".into()),
            suppressed: 3,
        }
    }

    #[test]
    fn parses_a_sentry_dsn() {
        assert_eq!(
            parse_dsn("https://abc123@o1.ingest.sentry.io/456"),
            Some(Dsn {
                origin: "https://o1.ingest.sentry.io".into(),
                key: "abc123".into(),
                project: "456".into(),
            })
        );
        assert_eq!(parse_dsn("not a dsn"), None);
        assert_eq!(parse_dsn("https://@host/1"), None);
        assert_eq!(parse_dsn("https://key@host/"), None);
    }

    #[test]
    fn sentry_posts_to_the_envelope_endpoint_with_auth() {
        let transport = RecordingTransport::ok();
        let mut f = Forwarder::new(
            Destination::Sentry {
                dsn: "https://abc123@o1.ingest.sentry.io/456".into(),
            },
            Box::new(transport.clone()),
        );
        f.send_window(&window()).unwrap();

        assert_eq!(
            transport.urls(),
            vec!["https://o1.ingest.sentry.io/api/456/envelope/"]
        );
        let headers = &transport.headers()[0];
        assert!(headers
            .iter()
            .any(|(k, v)| k == "X-Sentry-Auth" && v.contains("sentry_key=abc123")));
        assert_eq!(f.stats.events_sent, 1);
        assert!(f.stats.bytes_sent > 0);
    }

    #[test]
    fn splunk_posts_to_the_hec_collector_with_a_token() {
        let transport = RecordingTransport::ok();
        let mut f = Forwarder::new(
            Destination::SplunkHec {
                url: "https://splunk:8088/".into(),
                token: "tok".into(),
                aggregate: true,
            },
            Box::new(transport.clone()),
        );
        f.send_window(&window()).unwrap();

        assert_eq!(
            transport.urls(),
            vec!["https://splunk:8088/services/collector/event"]
        );
        assert!(transport.headers()[0]
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Splunk tok"));
    }

    #[test]
    fn retries_server_errors_then_gives_up() {
        let transport = RecordingTransport::scripted(vec![Ok(503), Ok(503), Ok(503)]);
        let mut f = Forwarder::new(
            Destination::SplunkHec {
                url: "https://splunk:8088".into(),
                token: "t".into(),
                aggregate: false,
            },
            Box::new(transport.clone()),
        );
        let err = f.send_window(&window()).unwrap_err();
        assert!(matches!(err, ForwardError::Rejected { status: 503, .. }));
        assert_eq!(transport.bodies().len(), 3, "initial attempt plus 2 retries");
        assert_eq!(f.stats.retries, 2);
        assert_eq!(f.stats.failures, 1);
    }

    #[test]
    fn a_server_error_that_clears_succeeds_without_a_failure() {
        let transport = RecordingTransport::scripted(vec![Err("connection reset".into()), Ok(200)]);
        let mut f = Forwarder::new(
            Destination::SplunkHec {
                url: "https://splunk:8088".into(),
                token: "t".into(),
                aggregate: false,
            },
            Box::new(transport),
        );
        f.send_window(&window()).unwrap();
        assert_eq!(f.stats.events_sent, 1);
        assert_eq!(f.stats.retries, 1);
        assert_eq!(f.stats.failures, 0);
    }

    #[test]
    fn client_errors_are_not_retried() {
        // A 401 will never succeed. Retrying burns the budget and delays
        // everything queued behind it.
        let transport = RecordingTransport::scripted(vec![Ok(401), Ok(200), Ok(200)]);
        let mut f = Forwarder::new(
            Destination::SplunkHec {
                url: "https://splunk:8088".into(),
                token: "bad".into(),
                aggregate: false,
            },
            Box::new(transport.clone()),
        );
        assert!(f.send_window(&window()).is_err());
        assert_eq!(transport.bodies().len(), 1, "no retry on 401");
        assert_eq!(f.stats.retries, 0);
    }

    #[test]
    fn rate_limiting_is_retried_unlike_other_client_errors() {
        let transport = RecordingTransport::scripted(vec![Ok(429), Ok(200)]);
        let mut f = Forwarder::new(
            Destination::SplunkHec {
                url: "https://splunk:8088".into(),
                token: "t".into(),
                aggregate: false,
            },
            Box::new(transport.clone()),
        );
        f.send_window(&window()).unwrap();
        assert_eq!(transport.bodies().len(), 2);
    }

    #[test]
    fn aggregator_rolls_up_by_service_and_template() {
        let mut agg = Aggregator::default();
        agg.record("api", 7, "timeout after <NUM>ms", "timeout after 10ms", 100);
        agg.record("api", 7, "timeout after <NUM>ms", "timeout after 20ms", 300);
        agg.record("api", 9, "other failure", "other failure", 200);
        agg.record("worker", 7, "timeout after <NUM>ms", "timeout after 5ms", 150);

        assert_eq!(agg.len(), 3, "keyed by (service, template)");
        let rollups = agg.drain();
        assert!(agg.is_empty(), "drain empties");

        let api7 = rollups
            .iter()
            .find(|r| r.service == "api" && r.template_id == 7)
            .unwrap();
        assert_eq!(api7.count, 2);
        assert_eq!(api7.first_seen_unix_nano, 100);
        assert_eq!(api7.last_seen_unix_nano, 300);
        assert_eq!(api7.example, "timeout after 10ms");
    }

    fn outbound(had_context: bool) -> Outbound {
        let mut w = window();
        if !had_context {
            w.context.clear();
        }
        Outbound {
            window: w,
            had_context,
            reason: (!had_context).then_some(Suppressed::DuplicateFlow),
            template_text: "payment gateway timeout after <NUM>ms".into(),
        }
    }

    fn dispatch_with(
        destinations: Vec<Destination>,
    ) -> (Dispatch, Vec<RecordingTransport>) {
        let mut transports = Vec::new();
        let forwarders = destinations
            .into_iter()
            .map(|d| {
                let t = RecordingTransport::ok();
                transports.push(t.clone());
                Forwarder::new(d, Box::new(t))
            })
            .collect();
        (Dispatch::new(forwarders, Duration::from_secs(60)), transports)
    }

    #[test]
    fn one_error_is_shaped_differently_per_destination() {
        // The whole point of §5: the same suppressed error becomes an event in
        // Sentry (which groups natively) and a rollup in Splunk (which does not).
        let (mut dispatch, transports) = dispatch_with(vec![
            Destination::Sentry {
                dsn: "https://k@h/1".into(),
            },
            Destination::SplunkHec {
                url: "https://splunk:8088".into(),
                token: "t".into(),
                aggregate: true,
            },
        ]);

        for _ in 0..5 {
            dispatch.handle(&outbound(false));
        }

        assert_eq!(transports[0].bodies().len(), 5, "sentry gets every error");
        assert_eq!(transports[1].bodies().len(), 0, "splunk waits for the rollup");
        assert_eq!(dispatch.pending_rollups(), 1);

        dispatch.flush_if_due(true);
        let splunk = transports[1].bodies();
        assert_eq!(splunk.len(), 1, "one aggregate for five errors");
        let event: serde_json::Value = serde_json::from_str(&splunk[0]).unwrap();
        assert_eq!(event["event"]["count"], 5);
        assert_eq!(
            event["event"]["template"],
            "payment gateway timeout after <NUM>ms"
        );
    }

    #[test]
    fn an_error_with_context_goes_to_everyone_immediately() {
        let (mut dispatch, transports) = dispatch_with(vec![
            Destination::Sentry {
                dsn: "https://k@h/1".into(),
            },
            Destination::SplunkHec {
                url: "https://splunk:8088".into(),
                token: "t".into(),
                aggregate: true,
            },
        ]);
        dispatch.handle(&outbound(true));

        assert_eq!(transports[0].bodies().len(), 1);
        assert_eq!(transports[1].bodies().len(), 1, "full context is worth sending");
        assert_eq!(dispatch.pending_rollups(), 0);
        assert!(transports[1].bodies()[0].contains("received request path="));
    }

    #[test]
    fn splunk_without_aggregation_receives_every_error() {
        let (mut dispatch, transports) = dispatch_with(vec![Destination::SplunkHec {
            url: "https://splunk:8088".into(),
            token: "t".into(),
            aggregate: false,
        }]);
        for _ in 0..3 {
            dispatch.handle(&outbound(false));
        }
        assert_eq!(transports[0].bodies().len(), 3);
        assert_eq!(dispatch.pending_rollups(), 0);
    }

    #[test]
    fn flush_before_the_interval_does_nothing_but_shutdown_forces_it() {
        let (mut dispatch, transports) = dispatch_with(vec![Destination::SplunkHec {
            url: "https://splunk:8088".into(),
            token: "t".into(),
            aggregate: true,
        }]);
        dispatch.handle(&outbound(false));

        dispatch.flush_if_due(false);
        assert_eq!(transports[0].bodies().len(), 0, "interval has not elapsed");

        dispatch.flush_if_due(true);
        assert_eq!(transports[0].bodies().len(), 1, "shutdown must not lose it");
        assert_eq!(dispatch.stats.flushes, 1);

        // Nothing pending: a flush must not emit an empty aggregate.
        dispatch.flush_if_due(true);
        assert_eq!(transports[0].bodies().len(), 1);
    }

    #[test]
    fn a_dead_destination_does_not_stop_the_others() {
        let dead = RecordingTransport::scripted(vec![Ok(500), Ok(500), Ok(500)]);
        let live = RecordingTransport::ok();
        let mut dispatch = Dispatch::new(
            vec![
                Forwarder::new(
                    Destination::Sentry {
                        dsn: "https://k@h/1".into(),
                    },
                    Box::new(dead.clone()),
                ),
                Forwarder::new(
                    Destination::SplunkHec {
                        url: "https://splunk:8088".into(),
                        token: "t".into(),
                        aggregate: false,
                    },
                    Box::new(live.clone()),
                ),
            ],
            Duration::from_secs(60),
        );
        dispatch.handle(&outbound(true));

        assert_eq!(live.bodies().len(), 1, "the healthy vendor still receives it");
        assert_eq!(dispatch.totals().failures, 1);
    }

    #[test]
    fn an_error_with_suppressed_context_still_carries_its_own_body() {
        // Collapsing withholds context, never the error's own parameters.
        let transport = RecordingTransport::ok();
        let mut f = Forwarder::new(
            Destination::Sentry {
                dsn: "https://k@h/1".into(),
            },
            Box::new(transport.clone()),
        );
        f.send_error_only(&window(), Suppressed::DuplicateFlow).unwrap();

        let body = &transport.bodies()[0];
        assert!(body.contains("payment gateway timeout after 4181ms"));
        assert!(
            !body.contains("received request path="),
            "context must be withheld"
        );
    }
}
