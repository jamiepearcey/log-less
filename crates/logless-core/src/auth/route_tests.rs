//! Every kind of principal against every listener's routes: who is let in, under which tenant its
//! records are stored, and what is refused.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use super::testing::*;
use super::*;
use crate::bounded_http::testing::{exchange, request, WAIT};
use crate::model::Severity;

const K_INGEST: &str = "acme-ingest-key-0123456789abcd";
const K_NODE: &str = "acme-node-key-0123456789abcdef";
const K_PAYMENTS: &str = "acme-payments-key-0123456789ab";
const K_LOCAL: &str = "own-tenant-key-0123456789abcde";
const K_REPLICA: &str = "replica-key-0123456789abcdefgh";
const K_READER: &str = "reader-key-0123456789abcdefghi";
const K_ADMIN: &str = "admin-key-0123456789abcdefghij";

struct Rig {
    verifier: Arc<Verifier>,
    signer: Signer,
    stored: Arc<Mutex<Vec<LogRecord>>>,
    _tmp: tempfile::TempDir,
    peer: crate::peer::Receiver,
    hec: crate::hec::Receiver,
    otlp: crate::otlp::Receiver,
    sentry: crate::sentry::Receiver,
    admin: crate::admin_api::AdminServer,
}

/// Two tenants (`default`, the instance's own, and `acme`, which refuses unknown indexes), one static
/// principal of each kind, a broker, and every listener closed (`require_auth`) on loopback.
fn rig() -> Rig {
    let tmp = tempfile::tempdir().unwrap();
    let tenancy = two_tenants(UnknownIndex::Refuse);
    let signer = Signer::new(11, "kid-1");
    let principals = [
        principal("acme-ingest", Kind::Ingest, Some("acme"), K_INGEST),
        principal("edge-1", Kind::Node, Some("acme"), K_NODE),
        PrincipalConfig { indexes: Some(vec!["payments".into()]), ..principal("acme-payments", Kind::Ingest, Some("acme"), K_PAYMENTS) },
        principal("local", Kind::Ingest, None, K_LOCAL),
        principal("copy", Kind::Replica, None, K_REPLICA),
        principal("acme-ro", Kind::Reader, Some("acme"), K_READER),
        principal("ops", Kind::Admin, None, K_ADMIN),
    ];
    let verifier = with_broker(Builder::new(tenancy).principals(&principals).unwrap(), &signer).build();
    let stored = Arc::new(Mutex::new(Vec::new()));

    let peer_config = crate::peer::PeerConfig {
        enabled: true,
        addr: "127.0.0.1:0".into(),
        require_auth: true,
        parts: crate::parts::PartsConfig { enabled: true, dir: Some(tmp.path().join("parts")), ..Default::default() },
        ..Default::default()
    };
    let sink = Arc::clone(&stored);
    let peer = crate::peer::Receiver::start_with_auth(&peer_config, "central".into(), Some(tmp.path().join("catalog.sqlite")), None, None, Arc::clone(&verifier), move |records| {
        sink.lock().unwrap().extend(records);
        crate::peer::Admitted::Accepted
    })
    .unwrap();

    let hec_config = crate::hec::HecConfig { enabled: true, addr: "127.0.0.1:0".into(), require_auth: true, ..Default::default() };
    let sink = Arc::clone(&stored);
    let hec = crate::hec::Receiver::start_with_auth(&hec_config, crate::hec::AckTable::new(Arc::new(AtomicU64::new(0))), Arc::clone(&verifier), move |batch| {
        sink.lock().unwrap().extend(batch.records);
        crate::hec::Admitted::Accepted
    })
    .unwrap();

    let otlp_config = crate::otlp::ReceiverConfig { enabled: true, addr: "127.0.0.1:0".into(), require_auth: true, ..Default::default() };
    let sink = Arc::clone(&stored);
    let otlp = crate::otlp::Receiver::start_with_auth(&otlp_config, Arc::clone(&verifier), move |records| {
        sink.lock().unwrap().extend(records);
        crate::otlp::Accepted::All
    })
    .unwrap();

    let sentry_config = crate::sentry::SentryConfig { enabled: true, addr: "127.0.0.1:0".into(), require_auth: true, ..Default::default() };
    let sink = Arc::clone(&stored);
    let sentry = crate::sentry::Receiver::start_with_auth(&sentry_config, None, None, Arc::clone(&verifier), move |records| {
        sink.lock().unwrap().extend(records);
        crate::sentry::Admitted::Accepted
    })
    .unwrap();

    let service = admin_service(tmp.path());
    service.set_auth(Arc::clone(&verifier));
    let admin_config = crate::admin_api::AdminConfig { enabled: true, addr: "127.0.0.1:0".into(), token: None, allowed_hosts: vec![] };
    let admin = crate::admin_api::AdminServer::start_with_auth(&admin_config, service, Arc::clone(&verifier)).unwrap();

    Rig { verifier, signer, stored, _tmp: tmp, peer, hec, otlp, sentry, admin }
}

fn admin_service(data_dir: &std::path::Path) -> Arc<crate::admin_api::AdminService> {
    let config: crate::Config = toml::from_str(&format!("[storage]\ndata_dir = {data_dir:?}\n")).unwrap();
    std::fs::create_dir_all(config.store_dir()).unwrap();
    let start = crate::admin_api::startup(&config).unwrap();
    let handle = crate::policy::PolicyHandle::new(start.effective.clone()).unwrap();
    let probe = crate::admin_api::IngestProbe {
        stats: Arc::new(crate::queue::IngestStats::default()),
        written: Arc::new(AtomicU64::new(0)),
        refused_debug_pressure: Arc::new(AtomicU64::new(0)),
        routes: Default::default(),
    };
    crate::admin_api::AdminService::new(&config, start, handle, probe, vec![])
}

impl Rig {
    fn token(&self, kind: &str, tenant: &str, name: &str) -> String {
        self.signer.sign(&claims(kind, tenant, name, crate::now_unix_secs()))
    }

    /// A user token: no name, roles instead of scopes.
    fn user_token(&self, tenant: &str) -> String {
        let mut c = claims("user", tenant, "x", crate::now_unix_secs());
        let o = c.as_object_mut().unwrap();
        o.remove("name");
        o.remove("scopes");
        o.insert("sub".into(), "alice@example.test".into());
        o.insert("roles".into(), serde_json::json!(["viewer"]));
        o.insert("sid".into(), "sess-1".into());
        self.signer.sign(&c)
    }

    fn take(&self) -> Vec<LogRecord> {
        std::mem::take(&mut *self.stored.lock().unwrap())
    }

    fn shutdown(self) {
        self.peer.shutdown();
        self.hec.shutdown();
        self.otlp.shutdown();
        self.sentry.shutdown();
        self.admin.shutdown();
    }
}

fn status(addr: SocketAddr, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> u16 {
    exchange(addr, &request(method, path, headers, body), WAIT).0
}

fn ndjson(records: &[LogRecord]) -> Vec<u8> {
    records.iter().map(|r| serde_json::to_string(r).unwrap()).collect::<Vec<_>>().join("\n").into_bytes()
}

fn record(attrs: &[(&str, &str)]) -> LogRecord {
    let mut r = LogRecord::new(crate::now_unix_nanos(), Severity::INFO, "hello");
    r.attributes = attrs.iter().map(|(k, v)| Attr { key: (*k).into(), value: AttrValue::Str((*v).into()) }).collect();
    r
}

/// Where a stored record will land: `(tenant, index)` as the merge reads them off it.
fn placed(r: &LogRecord) -> (String, String) {
    let get = |key: &str, default: &str| r.attributes.iter().find(|a| a.key == key).map(|a| match &a.value { AttrValue::Str(s) => s.clone(), _ => String::new() }).unwrap_or_else(|| default.into());
    (get(ATTR_TENANT, "default"), get(ATTR_INDEX, "default"))
}

const SENTRY_ENVELOPE: &str = "{\"event_id\":\"9ecf1f2a3b4c4d5e8f90112233445566\"}\n{\"type\":\"event\",\"length\":35}\n{\"level\":\"error\",\"message\":\"boom!\"}\n";

#[test]
fn each_kind_of_credential_on_each_route_is_let_in_or_refused() {
    let rig = rig();
    let presign = serde_json::json!({"name": format!("{}.parquet", "ab".repeat(32)), "size": 10, "sha256": "ab".repeat(32)}).to_string();
    let hec_event = br#"{"event":"hello"}"#;
    let otlp_body = br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"x"}}]}]}]}"#;

    // (what, the credential, its kind when it is good)
    let credentials: Vec<(&str, Option<String>, Option<Kind>)> = vec![
        ("no credential", None, None),
        ("a key nobody has", Some("not-a-key-0123456789abcdefghij".into()), None),
        ("static ingest", Some(K_INGEST.into()), Some(Kind::Ingest)),
        ("static node", Some(K_NODE.into()), Some(Kind::Node)),
        ("static replica", Some(K_REPLICA.into()), Some(Kind::Replica)),
        ("static admin", Some(K_ADMIN.into()), Some(Kind::Admin)),
        // A reader's key signs S3 requests; presented as a bearer it is nobody's.
        ("static reader", Some(K_READER.into()), None),
        ("token ingest", Some(rig.token("ingest", "acme", "svc-1")), Some(Kind::Ingest)),
        ("token node", Some(rig.token("node", "acme", "edge-2")), Some(Kind::Node)),
        ("token replica", Some(rig.token("replica", "default", "copy-2")), Some(Kind::Replica)),
        ("token admin", Some(rig.token("admin", "default", "ops-2")), Some(Kind::Admin)),
        ("token reader", Some(rig.token("reader", "acme", "ro-2")), Some(Kind::Reader)),
        ("token user", Some(rig.user_token("acme")), Some(Kind::User)),
        ("token for a tenant this instance does not have", Some(rig.token("ingest", "globex", "svc-9")), None),
    ];

    // (route, what it asks for, how to call it with a credential)
    type Call<'a> = Box<dyn Fn(Option<&str>) -> u16 + 'a>;
    let bearer = |c: Option<&str>| c.map(|c| format!("Bearer {c}"));
    let peer = rig.peer.local_addr();
    let routes: Vec<(&str, Op, Call)> = vec![
        ("peer GET /.well-known/logless", Op::Ingest, Box::new(|c| {
            let auth = bearer(c);
            status(peer, "GET", crate::peer::CAPABILITY_PATH, &auth.iter().map(|a| ("Authorization", a.as_str())).collect::<Vec<_>>(), b"")
        })),
        ("peer POST /v1/ingest", Op::Ingest, Box::new(|c| {
            let auth = bearer(c);
            let mut h = vec![("Content-Type", "application/x-ndjson")];
            h.extend(auth.iter().map(|a| ("Authorization", a.as_str())));
            status(peer, "POST", crate::peer::INGEST_PATH, &h, &ndjson(&[record(&[])]))
        })),
        ("peer POST /v1/parts/presign", Op::Parts, Box::new(|c| {
            let auth = bearer(c);
            let mut h = vec![("Content-Type", "application/json")];
            h.extend(auth.iter().map(|a| ("Authorization", a.as_str())));
            status(peer, "POST", crate::parts::PRESIGN_PATH, &h, presign.as_bytes())
        })),
        ("peer GET /v1/parts/replica/catalog", Op::Replica, Box::new(|c| {
            let auth = bearer(c);
            status(peer, "GET", crate::parts::REPLICA_CATALOG_PATH, &auth.iter().map(|a| ("Authorization", a.as_str())).collect::<Vec<_>>(), b"")
        })),
        ("peer POST /v1/auth/s3-credentials", Op::S3Credentials, Box::new(|c| {
            let auth = bearer(c);
            status(peer, "POST", crate::peer::S3_CREDENTIALS_PATH, &auth.iter().map(|a| ("Authorization", a.as_str())).collect::<Vec<_>>(), b"")
        })),
        ("hec POST /services/collector", Op::Ingest, Box::new(|c| {
            let auth = c.map(|c| format!("Splunk {c}"));
            status(rig.hec.local_addr(), "POST", "/services/collector", &auth.iter().map(|a| ("Authorization", a.as_str())).collect::<Vec<_>>(), hec_event)
        })),
        ("otlp POST /v1/logs", Op::Ingest, Box::new(|c| {
            let auth = bearer(c);
            let mut h = vec![("Content-Type", "application/json")];
            h.extend(auth.iter().map(|a| ("Authorization", a.as_str())));
            status(rig.otlp.local_addr(), "POST", "/v1/logs", &h, otlp_body)
        })),
        ("sentry POST /api/1/envelope/", Op::Ingest, Box::new(|c| {
            let auth = c.map(|c| format!("Sentry sentry_version=7, sentry_key={c}"));
            status(rig.sentry.local_addr(), "POST", "/api/1/envelope/", &auth.iter().map(|a| ("X-Sentry-Auth", a.as_str())).collect::<Vec<_>>(), SENTRY_ENVELOPE.as_bytes())
        })),
        ("admin GET /admin/v1/info", Op::Admin, Box::new(|c| {
            let auth = bearer(c);
            status(rig.admin.local_addr(), "GET", "/admin/v1/info", &auth.iter().map(|a| ("Authorization", a.as_str())).collect::<Vec<_>>(), b"")
        })),
    ];

    let mut checked = 0;
    for (route, op, call) in &routes {
        for (what, credential, kind) in &credentials {
            let got = call(credential.as_deref());
            let allowed = kind.is_some_and(|k| k.may(*op));
            if allowed {
                assert_eq!(200, got, "{route} with {what}: let in");
            } else {
                // Refused, and the answer is the same whatever was wrong with the credential: 401 for the
                // admin API and the Sentry proxy (as each always answered), else 401 without one and 403 with.
                let want = if route.starts_with("admin") || route.starts_with("sentry") || credential.is_none() { 401 } else { 403 };
                assert_eq!(want, got, "{route} with {what}: refused");
            }
            checked += 1;
        }
    }
    assert_eq!(9 * 14, checked);

    // Everything that was let in to an ingest route was stored under its principal's tenant: the four
    // ingest and node credentials are all acme's.
    let stored = rig.take();
    assert_eq!(4 * 4, stored.len(), "four credentials that may post, on four ingest routes");
    assert!(stored.iter().all(|r| placed(r).0 == "acme"), "{:?}", stored.iter().map(placed).collect::<Vec<_>>());

    // The refusals are counted by reason and by principal; the status names ids and never a key.
    let auth = rig.verifier.status();
    assert!(auth["refused"]["missing"].as_u64().unwrap() >= 6 && auth["refused"]["kind"].as_u64().unwrap() >= 20 && auth["refused"]["unknown_tenant"].as_u64().unwrap() >= 7, "{}", auth["refused"]);
    let text = auth.to_string();
    for key in [K_INGEST, K_NODE, K_REPLICA, K_READER, K_ADMIN, K_PAYMENTS, K_LOCAL] {
        assert!(!text.contains(key), "a key in status");
    }
    assert!(text.contains("acme/edge-1") && text.contains("acme/svc-1") && text.contains("default/ops"), "{text}");
    drop(routes);
    rig.shutdown();
}

#[test]
fn the_tenant_is_the_principals_on_every_ingest_route_and_a_cross_tenant_write_is_refused() {
    let rig = rig();
    let peer = rig.peer.local_addr();
    let ingest = |key: &str, records: &[LogRecord]| {
        let auth = format!("Bearer {key}");
        status(peer, "POST", crate::peer::INGEST_PATH, &[("Content-Type", "application/x-ndjson"), ("Authorization", &auth)], &ndjson(records))
    };
    let pair = |t: &str, i: &str| (t.to_string(), i.to_string());

    // The own tenant's key and another tenant's key, the same records: each lands in its own tenant.
    assert_eq!(200, ingest(K_LOCAL, &[record(&[])]));
    assert_eq!(rig.take().iter().map(placed).collect::<Vec<_>>(), [pair("default", "default")]);
    assert_eq!(200, ingest(K_INGEST, &[record(&[]), record(&[(ATTR_INDEX, "payments")])]));
    assert_eq!(rig.take().iter().map(placed).collect::<Vec<_>>(), [pair("acme", "default"), pair("acme", "payments")]);
    // A broker token is the same: the claim decides.
    assert_eq!(200, ingest(&rig.token("ingest", "acme", "svc-1"), &[record(&[])]));
    assert_eq!(rig.take().iter().map(placed).collect::<Vec<_>>(), [pair("acme", "default")]);

    // A record that says which tenant it is for is not believed: for another tenant the batch is refused
    // whole and nothing is stored, in either direction.
    assert_eq!(403, ingest(K_INGEST, &[record(&[]), record(&[(ATTR_TENANT, "default")])]));
    assert_eq!(403, ingest(K_LOCAL, &[record(&[(ATTR_TENANT, "acme")])]));
    assert_eq!(403, ingest(&rig.token("ingest", "acme", "svc-1"), &[record(&[(ATTR_TENANT, "default")])]));
    assert!(rig.take().is_empty());
    // Naming its own tenant changes nothing.
    assert_eq!(200, ingest(K_INGEST, &[record(&[(ATTR_TENANT, "acme")])]));
    assert_eq!(rig.take().iter().map(placed).collect::<Vec<_>>(), [pair("acme", "default")]);
    // Nor does a tenant in the query string or a header: there is nowhere in a request to name one.
    let auth = format!("Bearer {K_INGEST}");
    let sneaky = request("POST", "/v1/ingest?tenant=default", &[("Content-Type", "application/x-ndjson"), ("Authorization", &auth), ("X-Tenant", "default"), ("X-Scope-OrgID", "default")], &ndjson(&[record(&[])]));
    assert_eq!(200, exchange(peer, &sneaky, WAIT).0);
    assert_eq!(rig.take().iter().map(placed).collect::<Vec<_>>(), [pair("acme", "default")]);

    // HEC: a field cannot name the tenant either, and the token's tenant is where the event goes.
    let hec = |key: &str, body: &str| {
        let auth = format!("Splunk {key}");
        status(rig.hec.local_addr(), "POST", "/services/collector", &[("Authorization", &auth)], body.as_bytes())
    };
    assert_eq!(200, hec(K_INGEST, r#"{"event":"x","index":"payments"}"#));
    assert_eq!(rig.take().iter().map(placed).collect::<Vec<_>>(), [pair("acme", "payments")]);
    assert_eq!(403, hec(K_INGEST, r#"{"event":"x","fields":{"logless.tenant":"default"}}"#));
    assert!(rig.take().is_empty());

    // OTLP: a resource attribute cannot name it.
    let otlp = |key: &str, tenant: Option<&str>| {
        let auth = format!("Bearer {key}");
        let attrs = tenant.map(|t| format!(r#"{{"key":"logless.tenant","value":{{"stringValue":"{t}"}}}}"#)).unwrap_or_default();
        let body = format!(r#"{{"resourceLogs":[{{"resource":{{"attributes":[{attrs}]}},"scopeLogs":[{{"logRecords":[{{"body":{{"stringValue":"x"}}}}]}}]}}]}}"#);
        status(rig.otlp.local_addr(), "POST", "/v1/logs", &[("Content-Type", "application/json"), ("Authorization", &auth)], body.as_bytes())
    };
    assert_eq!(200, otlp(K_INGEST, None));
    assert_eq!(rig.take().iter().map(placed).collect::<Vec<_>>(), [pair("acme", "default")]);
    assert_eq!(403, otlp(K_INGEST, Some("default")));
    assert!(rig.take().is_empty());

    // Sentry: the DSN's public key is a principal's key.
    let auth = format!("Sentry sentry_version=7, sentry_key={K_INGEST}");
    assert_eq!(200, status(rig.sentry.local_addr(), "POST", "/api/1/envelope/", &[("X-Sentry-Auth", &auth)], SENTRY_ENVELOPE.as_bytes()));
    assert_eq!(rig.take().iter().map(placed).collect::<Vec<_>>(), [pair("acme", "default")]);
    rig.shutdown();
}

#[test]
fn the_index_allow_list_holds_on_the_wire_and_is_counted_for_the_principal() {
    let rig = rig();
    let peer = rig.peer.local_addr();
    let auth = format!("Bearer {K_PAYMENTS}");
    let ingest = |records: &[LogRecord]| status(peer, "POST", crate::peer::INGEST_PATH, &[("Content-Type", "application/x-ndjson"), ("Authorization", &auth)], &ndjson(records));
    // acme refuses unknown indexes. This principal may write `payments` only: a record that names none goes
    // there, one for `default` (which acme has, but this principal may not use) or for an index nobody has
    // is left out, and the rest of the batch is stored.
    assert_eq!(200, ingest(&[record(&[]), record(&[(ATTR_INDEX, "payments")]), record(&[(ATTR_INDEX, "default")]), record(&[(ATTR_INDEX, "nope")])]));
    let stored: Vec<_> = rig.take().iter().map(placed).collect();
    assert_eq!(stored, [("acme".to_string(), "payments".to_string()), ("acme".to_string(), "payments".to_string())]);
    // HEC answers Splunk's code 7 for the whole request.
    let splunk = format!("Splunk {K_PAYMENTS}");
    let (code, text) = exchange(rig.hec.local_addr(), &request("POST", "/services/collector", &[("Authorization", &splunk)], br#"{"event":"a","index":"payments"}{"event":"b","index":"default"}"#), WAIT);
    assert!(code == 400 && text.contains(r#""code":7"#), "{text}");
    assert!(rig.take().is_empty());

    let status = rig.verifier.status();
    let seen = status["seen"].as_array().unwrap().iter().find(|s| s["principal"] == "acme/acme-payments").unwrap();
    assert_eq!((seen["records"].as_u64(), seen["index_refused"].as_u64()), (Some(2), Some(3)), "{seen}");
    rig.shutdown();
}

#[test]
fn a_node_uses_the_parts_routes_under_its_own_id_only() {
    let rig = rig();
    let peer = rig.peer.local_addr();
    let presign = |key: &str, node: Option<&str>| {
        let auth = format!("Bearer {key}");
        let mut body = serde_json::json!({"name": format!("{}.parquet", "cd".repeat(32)), "size": 10, "sha256": "cd".repeat(32)});
        if let Some(node) = node {
            body["node"] = node.into();
        }
        let (code, text) = exchange(peer, &request("POST", crate::parts::PRESIGN_PATH, &[("Content-Type", "application/json"), ("Authorization", &auth)], body.to_string().as_bytes()), WAIT);
        (code, text.split("\r\n\r\n").nth(1).unwrap_or("").to_string())
    };
    // Its own id, named or not: a place to upload to, under that id.
    for node in [None, Some("edge-1")] {
        let (code, body) = presign(K_NODE, node);
        assert!(code == 200 && body.contains("/v1/parts/blob/edge-1/"), "{node:?}: {code} {body}");
    }
    // Another node's id is refused, for a static key and for a token (whose `name` is its node id).
    let (code, body) = presign(K_NODE, Some("edge-2"));
    assert!(code == 403 && body.contains("foreign_prefix"), "{code} {body}");
    let token = rig.token("node", "acme", "edge-2");
    let (code, body) = presign(&token, None);
    assert!(code == 200 && body.contains("/v1/parts/blob/edge-2/"), "{code} {body}");
    let (code, body) = presign(&token, Some("edge-1"));
    assert!(code == 403 && body.contains("foreign_prefix"), "{code} {body}");
    // A name that cannot be a directory owns no prefix.
    let (code, _) = presign(&rig.token("node", "acme", "../edge-1"), None);
    assert_eq!(403, code);
    // An ingest credential is good, and not enough: it is told so.
    let (code, body) = presign(K_INGEST, None);
    assert!(code == 403 && body.contains("no_node"), "{code} {body}");
    rig.shutdown();
}

#[test]
fn a_listener_with_nothing_configured_is_open_on_loopback_as_before_and_closed_anywhere_else() {
    // Nothing configured at all: a single-tenant install. Every ingest listener takes a request with no
    // credential, and stores it under the one tenant.
    let stored = Arc::new(Mutex::new(Vec::<LogRecord>::new()));
    let verifier = Builder::new(Arc::new(Tenancy::default())).build();
    let sink = Arc::clone(&stored);
    let peer = crate::peer::Receiver::start_with_auth(&crate::peer::PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), ..Default::default() }, "x".into(), None, None, None, Arc::clone(&verifier), move |records| {
        sink.lock().unwrap().extend(records);
        crate::peer::Admitted::Accepted
    })
    .unwrap();
    let post = |headers: &[(&str, &str)]| {
        let mut h = vec![("Content-Type", "application/x-ndjson")];
        h.extend_from_slice(headers);
        status(peer.local_addr(), "POST", crate::peer::INGEST_PATH, &h, &ndjson(&[record(&[])]))
    };
    assert_eq!(200, post(&[]));
    assert_eq!(200, post(&[("Authorization", "Bearer whatever-the-sender-was-given")]), "an open listener never looked at the header");
    assert!(stored.lock().unwrap().iter().all(|r| placed(r) == ("default".to_string(), "default".to_string())));
    assert_eq!(2, stored.lock().unwrap().len());
    peer.shutdown();

    // Off loopback there has to be a credential that could be accepted, and then one is always asked for.
    let public = crate::peer::PeerConfig { enabled: true, addr: "0.0.0.0:0".into(), ..Default::default() };
    let start = |v: &Arc<Verifier>| crate::peer::Receiver::start_with_auth(&public, "x".into(), None, None, None, Arc::clone(v), |_| crate::peer::Admitted::Accepted);
    assert!(matches!(start(&verifier).err(), Some(crate::peer::PeerError::OpenOnPublicAddress { .. })));
    let with_principal = Builder::new(Arc::new(Tenancy::default())).principals(&[principal("edge", Kind::Ingest, None, K_LOCAL)]).unwrap().build();
    let open_to_the_network = start(&with_principal).unwrap();
    let addr = SocketAddr::from(([127, 0, 0, 1], open_to_the_network.local_addr().port()));
    assert_eq!(401, status(addr, "POST", crate::peer::INGEST_PATH, &[("Content-Type", "application/x-ndjson")], &ndjson(&[record(&[])])));
    let auth = format!("Bearer {K_LOCAL}");
    assert_eq!(200, status(addr, "POST", crate::peer::INGEST_PATH, &[("Content-Type", "application/x-ndjson"), ("Authorization", &auth)], &ndjson(&[record(&[])])));
    open_to_the_network.shutdown();

    // The same for HEC and Sentry, which refused a non-loopback bind without their own list.
    let hec = |v: &Arc<Verifier>| crate::hec::Receiver::start_with_auth(&crate::hec::HecConfig { enabled: true, addr: "0.0.0.0:0".into(), ..Default::default() }, crate::hec::AckTable::new(Arc::new(AtomicU64::new(0))), Arc::clone(v), |_| crate::hec::Admitted::Accepted);
    assert!(matches!(hec(&verifier).err(), Some(crate::hec::HecError::OpenOnPublicAddress { .. })));
    hec(&with_principal).unwrap().shutdown();
    let sentry = |v: &Arc<Verifier>| crate::sentry::Receiver::start_with_auth(&crate::sentry::SentryConfig { enabled: true, addr: "0.0.0.0:0".into(), ..Default::default() }, None, None, Arc::clone(v), |_| crate::sentry::Admitted::Accepted);
    assert!(matches!(sentry(&verifier).err(), Some(crate::sentry::SentryError::OpenOnPublicAddress { .. })));
    sentry(&with_principal).unwrap().shutdown();
    // The admin API is never open: without any admin credential it does not start.
    let tmp = tempfile::tempdir().unwrap();
    let admin = crate::admin_api::AdminConfig { enabled: true, addr: "127.0.0.1:0".into(), token: None, allowed_hosts: vec![] };
    assert!(matches!(crate::admin_api::AdminServer::start_with_auth(&admin, admin_service(tmp.path()), Arc::clone(&with_principal)).err(), Some(crate::admin_api::AdminError::MissingToken)));
    let with_admin = Builder::new(Arc::new(Tenancy::default())).principals(&[principal("ops", Kind::Admin, None, K_ADMIN)]).unwrap().build();
    crate::admin_api::AdminServer::start_with_auth(&admin, admin_service(tmp.path()), with_admin).unwrap().shutdown();
}

#[test]
fn a_broker_token_is_traded_for_s3_credentials_of_its_own_tenant_and_nobody_elses() {
    let rig = rig();
    let peer = rig.peer.local_addr();
    let trade = |token: &str, body: &[u8]| {
        let auth = format!("Bearer {token}");
        let (status, text) = exchange(peer, &request("POST", crate::peer::S3_CREDENTIALS_PATH, &[("Authorization", &auth), ("Content-Type", "application/json")], body), WAIT);
        let json = text.split_once("\r\n\r\n").and_then(|(_, b)| serde_json::from_str::<serde_json::Value>(b).ok()).unwrap_or_default();
        (status, json, text)
    };
    let now = crate::now_unix_secs();

    // A person of tenant acme, whose token has ten minutes left: the credentials are acme's, end with the
    // token (the instance would give an hour), and the reply is not to be cached.
    let mut claims = claims("user", "acme", "x", now);
    claims.as_object_mut().unwrap().remove("name");
    claims["sub"] = "alice@example.test".into();
    claims["exp"] = (now + 600).into();
    claims["indexes"] = serde_json::json!(["payments"]);
    let alice = rig.signer.sign(&claims);
    let (code, json, text) = trade(&alice, b"");
    assert_eq!(200, code, "{text}");
    assert_eq!((json["tenant"].as_str(), json["indexes"].clone(), json["expires_unix_secs"].as_u64()), (Some("acme"), serde_json::json!(["payments"]), Some(now + 600)));
    assert!(json["access_key_id"].as_str().unwrap().starts_with(TEMP_KEY_PREFIX) && json["expires_at"].as_str().unwrap().ends_with('Z'));
    assert!(text.to_ascii_lowercase().contains("cache-control: no-store"), "{text}");
    let (who, creds) = rig.verifier.s3_session(json["session_token"].as_str().unwrap(), now).unwrap();
    assert_eq!((who.id.as_str(), who.tenant.as_str(), who.kind), ("alice@example.test", "acme", Kind::User));
    assert_eq!(Some(creds.secret_key.as_str()), json["secret_access_key"].as_str());
    let scope = ReadScope::of(&who);
    assert!(scope.allows("tenant=acme/index=payments/x.parquet") && !scope.allows("tenant=acme/index=default/x.parquet") && !scope.allows("tenant=default/index=default/x.parquet"));

    // The request cannot ask for more: a body, a query or a header naming another tenant changes nothing.
    for (path, body) in [(crate::peer::S3_CREDENTIALS_PATH.to_string(), br#"{"tenant":"default","indexes":["*"]}"#.to_vec()), (format!("{}?tenant=default", crate::peer::S3_CREDENTIALS_PATH), Vec::new())] {
        let auth = format!("Bearer {alice}");
        let (code, text) = exchange(peer, &request("POST", &path, &[("Authorization", &auth), ("X-Tenant", "default"), ("Content-Type", "application/json")], &body), WAIT);
        assert_eq!(200, code, "{path}");
        let json: serde_json::Value = serde_json::from_str(text.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!((json["tenant"].as_str(), json["indexes"].clone()), (Some("acme"), serde_json::json!(["payments"])), "{path}");
    }

    // A reading service's machine token is traded the same way; the instance's own hour is the limit.
    let (code, json, _) = trade(&rig.token("reader", "default", "reporting"), b"");
    assert_eq!((200, Some("default"), serde_json::json!(["*"])), (code, json["tenant"].as_str(), json["indexes"].clone()));
    assert!(json["expires_unix_secs"].as_u64().unwrap() <= crate::now_unix_secs() + 3600);

    // Refused: a tenant this instance does not have, kinds that do not read, a static key, an expired
    // token, one inside the clock allowance past its expiry (nothing left to give), and no token.
    let mut late = claims.clone();
    late["exp"] = (now - 100).into();
    late["iat"] = (now - 4000).into();
    let mut expired = claims.clone();
    expired["exp"] = (now - 4000).into();
    expired["iat"] = (now - 8000).into();
    for (what, token) in [
        ("another instance's tenant", rig.token("reader", "globex", "ro-9")),
        ("an ingest token", rig.token("ingest", "acme", "svc-1")),
        ("an admin token", rig.token("admin", "default", "ops-2")),
        ("a static ingest key", K_INGEST.to_string()),
        ("a static reader key", K_READER.to_string()),
        ("an expired token", rig.signer.sign(&expired)),
        ("a token past its expiry, inside the allowance", rig.signer.sign(&late)),
    ] {
        let (code, json, _) = trade(&token, b"");
        assert_eq!(403, code, "{what}");
        assert!(json.get("secret_access_key").is_none(), "{what}");
    }
    assert_eq!(401, status(peer, "POST", crate::peer::S3_CREDENTIALS_PATH, &[], b""));
    assert_eq!(404, status(peer, "GET", crate::peer::S3_CREDENTIALS_PATH, &[], b""), "POST only");

    let auth = rig.verifier.status();
    assert_eq!(auth["s3_credentials"]["issued"].as_u64(), Some(4), "three for alice and one for the service; nothing for a refusal");
    let text = auth.to_string();
    assert!(!text.contains(json_secret(&trade(&alice, b"").1)), "a secret in status");
    rig.shutdown();
}

fn json_secret(json: &serde_json::Value) -> &str {
    json["secret_access_key"].as_str().unwrap()
}

#[test]
fn a_person_with_the_admin_role_is_still_not_an_operator() {
    // The admin API opens for kind `admin` and nothing else. A role on a user token is carried and shown,
    // not interpreted (ADR 0004): making a person an operator of this instance is not the broker's to do.
    let rig = rig();
    let mut claims = claims("user", "default", "x", crate::now_unix_secs());
    let o = claims.as_object_mut().unwrap();
    o.remove("name");
    o.remove("scopes");
    o.insert("sub".into(), "root@example.test".into());
    o.insert("roles".into(), serde_json::json!(["admin", "owner"]));
    let token = rig.signer.sign(&claims);
    let auth = format!("Bearer {token}");
    for (method, path) in [("GET", "/admin/v1/info"), ("GET", "/admin/v1/policy"), ("POST", "/admin/v1/retention/run")] {
        assert_eq!(401, status(rig.admin.local_addr(), method, path, &[("Authorization", &auth), ("Content-Type", "application/json")], b"{}"), "{method} {path}");
    }
    let p = rig.verifier.verify(Listener::Admin, &token).unwrap();
    assert_eq!((p.kind, p.roles.clone()), (Kind::User, vec!["admin".to_string(), "owner".to_string()]));
    assert_eq!(200, status(rig.admin.local_addr(), "GET", "/admin/v1/info", &[("Authorization", &format!("Bearer {K_ADMIN}"))], b""));
    assert_eq!(200, status(rig.admin.local_addr(), "GET", "/admin/v1/info", &[("Authorization", &format!("Bearer {}", rig.token("admin", "default", "ops-2")))], b""), "a broker token of kind admin is an operator's");
    rig.shutdown();
}
