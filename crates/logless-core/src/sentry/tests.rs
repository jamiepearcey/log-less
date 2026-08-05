use super::*;
use crate::forward::{Response, Transport};
use std::sync::Mutex;

/// (url, headers, body) as it went out.
type SentRequest = (String, Vec<(String, String)>, Vec<u8>);

/// Records what an upstream Sentry would have received.
#[derive(Default)]
struct FakeSentry {
    sent: Mutex<Vec<SentRequest>>,
    reply: Mutex<Response>,
}

impl FakeSentry {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            sent: Mutex::new(Vec::new()),
            reply: Mutex::new(Response { status: 200, ..Default::default() }),
        })
    }

    fn bodies(&self) -> Vec<String> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, b)| String::from_utf8_lossy(b).into_owned())
            .collect()
    }
}

struct FakeHandle(Arc<FakeSentry>);

impl Transport for FakeHandle {
    fn post(&self, _: &str, _: &[(String, String)], _: &[u8]) -> Result<u16, String> {
        unreachable!("proxy uses post_full")
    }
    fn post_full(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<Response, String> {
        self.0.sent.lock().unwrap().push((url.into(), headers.to_vec(), body.to_vec()));
        Ok(self.0.reply.lock().unwrap().clone())
    }
}

struct Harness {
    receiver: Option<Receiver>,
    stored: Arc<Mutex<Vec<LogRecord>>>,
    fake: Arc<FakeSentry>,
    _spool: tempfile::TempDir,
}

impl Harness {
    fn start(config: SentryConfig) -> Self {
        Self::start_with(config, true)
    }

    fn start_with(mut config: SentryConfig, accept: bool) -> Self {
        config.addr = "127.0.0.1:0".into();
        let stored = Arc::new(Mutex::new(Vec::new()));
        let sink_stored = Arc::clone(&stored);
        let fake = FakeSentry::new();
        let upstream = Upstream::new(Box::new(FakeHandle(Arc::clone(&fake))));
        let spool = tempfile::tempdir().unwrap();
        let receiver =
            Receiver::start(&config, Some(upstream), Some(spool.path()), move |records| {
                if !accept {
                    return Admitted::Busy;
                }
                sink_stored.lock().unwrap().extend(records);
                Admitted::Accepted
            })
            .unwrap();
        Self { receiver: Some(receiver), stored, fake, _spool: spool }
    }

    fn relay() -> Self {
        Self::start(SentryConfig {
            enabled: true,
            upstream_auth: UpstreamAuth::Relay,
            upstream_dsn: Some("https://unused@sentry.example/999".into()),
            ..Default::default()
        })
    }

    fn url(&self, project: &str) -> String {
        format!(
            "http://{}/api/{project}/envelope/",
            self.receiver.as_ref().unwrap().local_addr()
        )
    }

    fn stats(&self) -> StatsSnapshot {
        self.receiver.as_ref().unwrap().stats()
    }

    /// Waits for the forwarder thread to catch up.
    fn settle(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if !self.fake.sent.lock().unwrap().is_empty() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(r) = self.receiver.take() {
            r.shutdown();
        }
    }
}

const AUTH: (&str, &str) = ("X-Sentry-Auth", "Sentry sentry_version=7, sentry_key=appkey");

fn post(url: &str, body: &[u8], headers: &[(&str, &str)]) -> (u16, String, Vec<(String, String)>) {
    let agent: ureq::Agent =
        ureq::Agent::config_builder().http_status_as_error(false).build().into();
    let mut req = agent.post(url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let mut response = req.send(body).expect("request failed");
    let status = response.status().as_u16();
    let out_headers = response
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    (status, response.body_mut().read_to_string().unwrap(), out_headers)
}

fn envelope_bytes(items: &[(&str, &str)]) -> Vec<u8> {
    let mut body = String::from("{\"event_id\":\"9ecf1f2a3b4c4d5e8f90112233445566\"}\n");
    for (kind, payload) in items {
        body.push_str(&format!("{{\"type\":\"{kind}\",\"length\":{}}}\n{payload}\n", payload.len()));
    }
    body.into_bytes()
}

const AN_ERROR: &str = r#"{"level":"error","server_name":"node-7","release":"1.2.3","exception":{"values":[{"type":"ValueError","value":"bad input"}]},"contexts":{"trace":{"trace_id":"0123456789abcdef0123456789abcdef","span_id":"0123456789abcdef"}}}"#;

#[test]
fn proxies_an_error_and_stores_it_locally() {
    let h = Harness::relay();
    let (status, body, _) = post(&h.url("1234"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    assert_eq!(status, 200);
    assert!(body.contains("\"id\""), "SDKs read the id back: {body}");

    let stored = h.stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].body, "ValueError: bad input");
    assert_eq!(stored[0].severity, Severity::ERROR);
    assert_eq!(stored[0].service.as_deref(), Some("node-7"));
    assert!(stored[0].trace_id.is_some(), "trace id must survive for correlation");
    drop(stored);

    h.settle();
    assert_eq!(h.fake.bodies().len(), 1);
    assert!(h.fake.bodies()[0].contains("ValueError"));
}

#[test]
fn relay_mode_keeps_the_clients_key_and_project() {
    // The transparent option: events land in the project they always did, so
    // quotas, alerts and ownership rules are untouched.
    let h = Harness::relay();
    post(&h.url("1234"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    h.settle();
    let sent = h.fake.sent.lock().unwrap();
    assert_eq!(sent[0].0, "https://sentry.example/api/1234/envelope/");
    let auth = sent[0].1.iter().find(|(k, _)| k == "X-Sentry-Auth").unwrap();
    assert!(auth.1.contains("sentry_key=appkey"), "{}", auth.1);
}

#[test]
fn resign_mode_replaces_key_and_project() {
    let h = Harness::start(SentryConfig {
        enabled: true,
        upstream_auth: UpstreamAuth::Resign,
        upstream_dsn: Some("https://ourkey@sentry.example/42".into()),
        ..Default::default()
    });
    post(&h.url("1234"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    h.settle();
    let sent = h.fake.sent.lock().unwrap();
    assert_eq!(sent[0].0, "https://sentry.example/api/42/envelope/");
    let auth = sent[0].1.iter().find(|(k, _)| k == "X-Sentry-Auth").unwrap();
    assert!(auth.1.contains("sentry_key=ourkey"), "{}", auth.1);
    // The client's own key is still recorded locally, so attribution is not
    // lost — only moved.
    let stored = h.stored.lock().unwrap();
    assert!(stored[0]
        .attributes
        .iter()
        .any(|a| a.key == "sentry.key" && a.value == AttrValue::Str("appkey".into())));
}

#[test]
fn a_per_project_route_beats_the_mode() {
    let h = Harness::start(SentryConfig {
        enabled: true,
        upstream_auth: UpstreamAuth::Resign,
        upstream_dsn: Some("https://ourkey@sentry.example/42".into()),
        projects: vec![ProjectRoute {
            project: "1234".into(),
            dsn: "https://special@other.example/7".into(),
        }],
        ..Default::default()
    });
    post(&h.url("1234"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    post(&h.url("5678"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while h.fake.sent.lock().unwrap().len() < 2 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let sent = h.fake.sent.lock().unwrap();
    assert_eq!(sent[0].0, "https://other.example/api/7/envelope/", "routed project");
    assert_eq!(sent[1].0, "https://sentry.example/api/42/envelope/", "everything else");
}

#[test]
fn the_forwarded_payload_is_byte_identical() {
    // A re-serialised event reorders keys and renormalises numbers, so what
    // Sentry shows would quietly differ from what the application reported.
    let h = Harness::relay();
    post(&h.url("1"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    h.settle();
    assert!(h.fake.bodies()[0].contains(AN_ERROR), "payload was rewritten:\n{}", h.fake.bodies()[0]);
}

#[test]
fn transactions_can_be_sampled_while_errors_are_not() {
    // The Sentry bill is transactions, not errors — this is the whole point of
    // being selective here.
    let h = Harness::start(SentryConfig {
        enabled: true,
        upstream_auth: UpstreamAuth::Relay,
        upstream_dsn: Some("https://u@sentry.example/9".into()),
        transaction_sample_rate: 0.0,
        ..Default::default()
    });
    post(
        &h.url("1"),
        &envelope_bytes(&[("event", AN_ERROR), ("transaction", r#"{"transaction":"GET /"}"#)]),
        &[AUTH],
    );
    h.settle();
    let body = &h.fake.bodies()[0];
    assert!(body.contains("ValueError"), "errors are never sampled out");
    assert!(!body.contains("GET /"), "transaction should have been sampled out: {body}");

    let stats = h.stats();
    assert_eq!(stats.items_received, 2);
    assert_eq!(stats.items_forwarded, 1);
    assert_eq!(stats.transactions_sampled_out, 1);
    // Both are still on disk: the decision is reversible.
    assert_eq!(h.stored.lock().unwrap().len(), 2);
}

#[test]
fn sampling_is_deterministic_on_the_event_id() {
    // A random draw would split one trace across the boundary and leave Sentry
    // showing half a transaction tree.
    for rate in [0.1, 0.5, 0.9] {
        for id in ["abc", "9ecf1f2a3b4c4d5e8f90112233445566", "zz"] {
            let first = sample_keep(id, rate);
            for _ in 0..10 {
                assert_eq!(sample_keep(id, rate), first, "id={id} rate={rate}");
            }
        }
    }
    assert!(sample_keep("anything", 1.0));
    assert!(!sample_keep("anything", 0.0));
}

#[test]
fn sampling_keeps_roughly_the_requested_fraction() {
    let kept = (0..2000)
        .filter(|i| sample_keep(&format!("event-{i}"), 0.25))
        .count();
    assert!((400..=600).contains(&kept), "expected ~500 of 2000, got {kept}");
}

#[test]
fn attachments_are_held_locally_by_default() {
    let h = Harness::relay();
    let mut body = Vec::from(&b"{\"event_id\":\"a\"}\n"[..]);
    body.extend_from_slice(b"{\"type\":\"event\",\"length\":2}\n{}\n");
    body.extend_from_slice(b"{\"type\":\"attachment\",\"length\":5,\"filename\":\"core\"}\nBYTES\n");
    post(&h.url("1"), &body, &[AUTH]);
    h.settle();
    assert!(!h.fake.bodies()[0].contains("BYTES"), "attachment should not be forwarded");

    let stored = h.stored.lock().unwrap();
    let attachment = stored.iter().find(|r| r.body.starts_with("attachment")).unwrap();
    assert!(attachment
        .attributes
        .iter()
        .any(|a| a.key == "sentry.attachment_bytes" && a.value == AttrValue::I64(5)));
    // The bytes themselves are deliberately not copied into a string column.
    assert!(!attachment.attributes.iter().any(|a| a.key == "sentry.payload"));
}

#[test]
fn unknown_item_types_are_forwarded_untouched() {
    // Anything else breaks every SDK feature newer than this build.
    let h = Harness::relay();
    let body = envelope_bytes(&[("some_future_thing", r#"{"a":1}"#)]);
    post(&h.url("1"), &body, &[AUTH]);
    h.settle();
    let forwarded = &h.fake.bodies()[0];
    assert!(forwarded.contains("some_future_thing"), "{forwarded}");
    assert!(forwarded.contains(r#"{"a":1}"#));
}

#[test]
fn sessions_and_client_reports_always_go_upstream() {
    // Sentry needs these for release health and loss accounting; dropping them
    // silently corrupts numbers an operator will later trust.
    let h = Harness::start(SentryConfig {
        enabled: true,
        upstream_auth: UpstreamAuth::Relay,
        upstream_dsn: Some("https://u@sentry.example/9".into()),
        forward_events: false,
        transaction_sample_rate: 0.0,
        ..Default::default()
    });
    let body = envelope_bytes(&[
        ("event", AN_ERROR),
        ("session", r#"{"sid":"s1"}"#),
        ("client_report", r#"{"discarded_events":[]}"#),
    ]);
    post(&h.url("1"), &body, &[AUTH]);
    h.settle();
    let forwarded = &h.fake.bodies()[0];
    assert!(forwarded.contains("\"sid\":\"s1\""), "{forwarded}");
    assert!(forwarded.contains("discarded_events"));
    assert!(!forwarded.contains("ValueError"), "forward_events=false was ignored");
}

#[test]
fn the_legacy_store_endpoint_becomes_a_modern_envelope() {
    // Old SDKs post a bare event; upstream gets a proper envelope regardless.
    let h = Harness::relay();
    let url = format!(
        "http://{}/api/1234/store/",
        h.receiver.as_ref().unwrap().local_addr()
    );
    let (status, _, _) = post(&url, AN_ERROR.as_bytes(), &[AUTH]);
    assert_eq!(status, 200);
    h.settle();
    let forwarded = &h.fake.bodies()[0];
    assert!(forwarded.contains("\"type\":\"event\""), "{forwarded}");
    assert!(forwarded.contains("ValueError"));
    assert_eq!(h.stored.lock().unwrap().len(), 1);
}

#[test]
fn upstream_rate_limits_are_reflected_back_to_the_sdk() {
    // The SDK's own backoff then behaves as it would without a proxy in the
    // way — otherwise clients keep sending into a limit they cannot see.
    let h = Harness::relay();
    *h.fake.reply.lock().unwrap() = Response {
        status: 429,
        headers: vec![("X-Sentry-Rate-Limits".into(), "60:error:organization:quota".into())],
        body: Vec::new(),
    };
    post(&h.url("1"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    h.settle();

    let (status, _, headers) = post(&h.url("1"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    assert_eq!(status, 429);
    let limit = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-sentry-rate-limits"))
        .map(|(_, v)| v.as_str());
    assert_eq!(limit, Some("60:error:organization:quota"), "Sentry's own wording, passed through");
    assert_eq!(h.stats().busy, 1);
    assert!(h.stats().accounts_for_everything());
}

#[test]
fn a_missing_or_unknown_key_is_refused_when_keys_are_configured() {
    let h = Harness::start(SentryConfig {
        enabled: true,
        upstream_auth: UpstreamAuth::Relay,
        upstream_dsn: Some("https://u@sentry.example/9".into()),
        allowed_keys: vec!["goodkey".into()],
        ..Default::default()
    });
    let body = envelope_bytes(&[("event", AN_ERROR)]);
    assert_eq!(post(&h.url("1"), &body, &[]).0, 401, "no credential at all");
    assert_eq!(
        post(&h.url("1"), &body, &[("X-Sentry-Auth", "Sentry sentry_key=wrong")]).0,
        401
    );
    assert_eq!(
        post(&h.url("1"), &body, &[("X-Sentry-Auth", "Sentry sentry_key=goodkey")]).0,
        200
    );
    assert!(h.stored.lock().unwrap().len() == 1, "only the authorised one was stored");
    assert!(h.stats().accounts_for_everything());
}

#[test]
fn the_key_may_arrive_in_the_query_string() {
    // Browser SDKs send it this way.
    let h = Harness::relay();
    let url = format!("{}?sentry_key=browserkey&sentry_version=7", h.url("1"));
    assert_eq!(post(&url, &envelope_bytes(&[("event", AN_ERROR)]), &[]).0, 200);
}

#[test]
fn a_full_pipeline_is_429_not_a_silent_drop() {
    let h = Harness::start_with(
        SentryConfig {
            enabled: true,
            upstream_auth: UpstreamAuth::Relay,
            upstream_dsn: Some("https://u@sentry.example/9".into()),
            ..Default::default()
        },
        false,
    );
    let (status, _, headers) = post(&h.url("1"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    assert_eq!(status, 429);
    assert!(headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("retry-after")));
    assert!(h.stats().accounts_for_everything());
}

#[test]
fn malformed_envelopes_are_400_and_not_forwarded() {
    let h = Harness::relay();
    assert_eq!(post(&h.url("1"), b"not an envelope", &[AUTH]).0, 400);
    assert_eq!(post(&h.url("1"), b"", &[AUTH]).0, 400);
    assert!(h.fake.sent.lock().unwrap().is_empty());
    assert_eq!(h.stats().bad_request, 2);
    assert!(h.stats().accounts_for_everything());
}

#[test]
fn non_ingest_paths_are_404_and_not_counted() {
    let h = Harness::relay();
    let base = format!("http://{}", h.receiver.as_ref().unwrap().local_addr());
    assert_eq!(post(&format!("{base}/"), b"{}", &[AUTH]).0, 404);
    assert_eq!(post(&format!("{base}/api/1/security/"), b"{}", &[AUTH]).0, 404);
    assert_eq!(h.stats().requests, 0);
}

#[test]
fn gzipped_envelopes_are_accepted() {
    // Every Sentry SDK compresses by default.
    use std::io::Write;
    let h = Harness::relay();
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(&envelope_bytes(&[("event", AN_ERROR)])).unwrap();
    let gz = enc.finish().unwrap();
    let (status, _, _) = post(&h.url("1"), &gz, &[AUTH, ("Content-Encoding", "gzip")]);
    assert_eq!(status, 200);
    assert_eq!(h.stored.lock().unwrap()[0].body, "ValueError: bad input");
}

#[test]
fn a_local_only_proxy_stores_without_forwarding() {
    // How you measure what Sentry would have cost before changing anything.
    let stored = Arc::new(Mutex::new(Vec::new()));
    let sink_stored = Arc::clone(&stored);
    let config = SentryConfig { enabled: true, addr: "127.0.0.1:0".into(), ..Default::default() };
    let receiver = Receiver::start(&config, None, None, move |records| {
        sink_stored.lock().unwrap().extend(records);
        Admitted::Accepted
    })
    .unwrap();
    let url = format!("http://{}/api/1/envelope/", receiver.local_addr());
    assert_eq!(post(&url, &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]).0, 200);
    assert_eq!(stored.lock().unwrap().len(), 1);
    receiver.shutdown();
}

#[test]
fn resign_without_a_dsn_is_a_configuration_error_not_a_silent_drop() {
    let config = SentryConfig {
        enabled: true,
        addr: "127.0.0.1:0".into(),
        upstream_auth: UpstreamAuth::Resign,
        upstream_dsn: None,
        ..Default::default()
    };
    match Receiver::start(&config, None, None, |_| Admitted::Accepted) {
        Err(SentryError::ResignWithoutDsn) => {}
        other => panic!("expected a config error, got {:?}", other.map(|_| "ok")),
    }
}

#[test]
fn a_malformed_dsn_is_refused_at_startup() {
    let config = SentryConfig {
        enabled: true,
        addr: "127.0.0.1:0".into(),
        upstream_dsn: Some("not a dsn".into()),
        ..Default::default()
    };
    match Receiver::start(&config, None, None, |_| Admitted::Accepted) {
        Err(SentryError::BadDsn { .. }) => {}
        other => panic!("expected a bad DSN error, got {:?}", other.map(|_| "ok")),
    }
}

#[test]
fn an_open_key_policy_refuses_to_bind_a_public_address() {
    let config = SentryConfig {
        enabled: true,
        addr: "0.0.0.0:0".into(),
        allowed_keys: Vec::new(),
        ..Default::default()
    };
    match Receiver::start(&config, None, None, |_| Admitted::Accepted) {
        Err(SentryError::OpenOnPublicAddress { .. }) => {}
        other => panic!("expected a refusal, got {:?}", other.map(|_| "ok")),
    }
}

#[test]
fn shutdown_drains_what_was_already_acknowledged() {
    // An envelope answered with 200 must not vanish because the agent stopped.
    let h = Harness::relay();
    for _ in 0..25 {
        post(&h.url("1"), &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    }
    let mut this = h;
    let r = this.receiver.take().unwrap();
    let fake = Arc::clone(&this.fake);
    let stats = r.shutdown();
    assert_eq!(stats.accepted, 25);
    assert_eq!(fake.sent.lock().unwrap().len(), 25, "queued envelopes must be drained");
}

#[test]
fn envelopes_survive_a_restart_when_upstream_is_down() {
    // The reason the spool exists: the SDK was told 200 and has discarded its
    // only copy, so a restart with an unreachable Sentry must not lose it.
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

    let spool = tempfile::tempdir().unwrap();
    let config = SentryConfig {
        enabled: true,
        addr: "127.0.0.1:0".into(),
        upstream_auth: UpstreamAuth::Relay,
        upstream_dsn: Some("https://u@sentry.example/9".into()),
        ..Default::default()
    };

    // First run: Sentry is unreachable, so nothing is delivered.
    let receiver = Receiver::start(
        &config,
        Some(Upstream::new(Box::new(Dead))),
        Some(spool.path()),
        |_| Admitted::Accepted,
    )
    .unwrap();
    let url = format!("http://{}/api/1/envelope/", receiver.local_addr());
    for _ in 0..3 {
        assert_eq!(post(&url, &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]).0, 200);
    }
    receiver.shutdown();

    // Second run: Sentry is back. The envelopes are still there.
    let fake = FakeSentry::new();
    let receiver = Receiver::start(
        &config,
        Some(Upstream::new(Box::new(FakeHandle(Arc::clone(&fake))))),
        Some(spool.path()),
        |_| Admitted::Accepted,
    )
    .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while fake.sent.lock().unwrap().len() < 3 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    receiver.shutdown();
    assert_eq!(fake.sent.lock().unwrap().len(), 3, "spooled envelopes must be delivered later");
    assert!(fake.bodies()[0].contains("ValueError"));
}

#[test]
fn a_delivered_envelope_is_not_sent_twice_after_a_restart() {
    let spool = tempfile::tempdir().unwrap();
    let config = SentryConfig {
        enabled: true,
        addr: "127.0.0.1:0".into(),
        upstream_auth: UpstreamAuth::Relay,
        upstream_dsn: Some("https://u@sentry.example/9".into()),
        ..Default::default()
    };
    let fake = FakeSentry::new();
    let receiver = Receiver::start(
        &config,
        Some(Upstream::new(Box::new(FakeHandle(Arc::clone(&fake))))),
        Some(spool.path()),
        |_| Admitted::Accepted,
    )
    .unwrap();
    let url = format!("http://{}/api/1/envelope/", receiver.local_addr());
    post(&url, &envelope_bytes(&[("event", AN_ERROR)]), &[AUTH]);
    receiver.shutdown();
    assert_eq!(fake.sent.lock().unwrap().len(), 1);

    let again = FakeSentry::new();
    let receiver = Receiver::start(
        &config,
        Some(Upstream::new(Box::new(FakeHandle(Arc::clone(&again))))),
        Some(spool.path()),
        |_| Admitted::Accepted,
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(400));
    receiver.shutdown();
    assert!(again.sent.lock().unwrap().is_empty(), "the committed cursor must hold");
}

#[test]
fn event_summaries_prefer_the_exception_then_the_message() {
    let exception = serde_json::json!({
        "exception": {"values": [{"type": "IOError", "value": "disk full"}]},
        "message": "ignored"
    });
    assert_eq!(event_summary(&exception), "IOError: disk full");

    let message = serde_json::json!({"logentry": {"formatted": "payment failed for 42"}});
    assert_eq!(event_summary(&message), "payment failed for 42");

    let bare = serde_json::json!({"message": "plain"});
    assert_eq!(event_summary(&bare), "plain");

    // The last exception in the chain is the proximate one.
    let chained = serde_json::json!({
        "exception": {"values": [
            {"type": "Outer", "value": "wrapped"},
            {"type": "Inner", "value": "root cause"}
        ]}
    });
    assert_eq!(event_summary(&chained), "Inner: root cause");

    assert_eq!(event_summary(&serde_json::json!({})), "sentry event");
}

#[test]
fn routes_are_parsed_from_the_path() {
    assert_eq!(
        SentryHandler::route("/api/1234/envelope/").map(|(p, e)| (p.to_string(), e)),
        Some(("1234".to_string(), Endpoint::Envelope))
    );
    assert_eq!(
        SentryHandler::route("/api/1234/store").map(|(p, e)| (p.to_string(), e)),
        Some(("1234".to_string(), Endpoint::Store))
    );
    assert!(SentryHandler::route("/api//envelope/").is_none());
    assert!(SentryHandler::route("/envelope/").is_none());
    assert!(SentryHandler::route("/api/1/security/").is_none());
}
