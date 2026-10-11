//! Flapping-peer soak with exact accounting.
//!
//! A sender delivers to a real receiver through a fault-injecting proxy that cycles 200, 503, 429, 401,
//! a connection dropped mid-body, a batch forwarded but its reply dropped (so the peer has it and the
//! sender resends) and a 422. Around that the soak restarts the sender mid-stream, fills the spool until
//! it sheds and drains it, restarts the receiver (whose dedupe log must recognise the resend), re-offers
//! records the peer already holds, and mixes in records that are loops for the receiver. A two-node
//! ring (A to B to A) runs beside it. Every record is accounted for at the end, and the ledger is
//! printed.
//!
//! Two more runs put the agent's WAL acknowledgement under it (`peer` module docs, "Acknowledgement"): a
//! receiver whose disk never syncs must never let the sender's cursor move, and a queue ahead of a slow WAL
//! that fills must answer 503 until every record has landed exactly once.
//!
//! `cargo test -p logless-core --lib -- sync::soak --nocapture` runs one round of each phase.
//! The ignored `soak_for_minutes` repeats them for `LOGLESS_SOAK_SECS` (default 180) seconds.

use std::collections::{HashSet, VecDeque};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use uuid::Uuid;

use super::*;
use crate::model::{Attr, AttrValue, Severity};
use crate::peer::{Admitted, PeerConfig, PeerStatsSnapshot, Ticket, WalAcks};

const SPOOL_MAX: u64 = 256 * 1024;
const RECEIVER: &str = "central";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Pass,
    Busy503,
    Limit429,
    Auth401,
    DropMidBody,
    /// Forwarded to the receiver, which stores it, but the reply never reaches the sender.
    ForwardThenDrop,
    Reject422,
}

const CYCLE: [Action; 12] = [
    Action::Pass,
    Action::Busy503,
    Action::Pass,
    Action::Limit429,
    Action::Pass,
    Action::Auth401,
    Action::Pass,
    Action::DropMidBody,
    Action::Pass,
    Action::ForwardThenDrop,
    Action::Pass,
    Action::Reject422,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Base {
    Cycle,
    Down,
    Pass,
}

#[derive(Default)]
struct ProxyLedger {
    requests: u64,
    by_action: [u64; 7],
    /// Records in bodies whose receiver reply was relayed to the sender.
    relayed: u64,
    /// Records in bodies the receiver answered but whose reply was dropped.
    unheard: u64,
    /// Records in bodies the proxy refused with 422 (the sender drops them for good).
    rejected: u64,
    rejected_ids: HashSet<Uuid>,
    /// Copies the receiver saw, by kind: its own origin (loops) or not.
    forwarded_loops: u64,
    forwarded_other: u64,
    loop_ids: HashSet<Uuid>,
    forward_errors: u64,
}

struct ProxyState {
    upstream: String,
    base: Base,
    forced: VecDeque<Action>,
    step: usize,
    ledger: ProxyLedger,
}

struct Proxy {
    addr: SocketAddr,
    state: Arc<Mutex<ProxyState>>,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Proxy {
    fn start(upstream: String) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(ProxyState { upstream, base: Base::Cycle, forced: VecDeque::new(), step: 0, ledger: ProxyLedger::default() }));
        let stop = Arc::new(AtomicBool::new(false));
        let (st, sp) = (Arc::clone(&state), Arc::clone(&stop));
        let handle = std::thread::spawn(move || {
            while !sp.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        handle_connection(stream, &st);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => std::thread::sleep(Duration::from_millis(2)),
                    Err(_) => break,
                }
            }
        });
        Proxy { addr, state, stop, handle: Some(handle) }
    }

    fn set_base(&self, base: Base) {
        self.state.lock().unwrap().base = base;
    }

    fn force(&self, action: Action) {
        self.state.lock().unwrap().forced.push_back(action);
    }

    fn set_upstream(&self, upstream: String) {
        self.state.lock().unwrap().upstream = upstream;
    }

    fn read<T>(&self, f: impl FnOnce(&ProxyLedger) -> T) -> T {
        f(&self.state.lock().unwrap().ledger)
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Index into `ProxyLedger::by_action`, in declaration order.
fn action_index(a: Action) -> usize {
    a as usize
}

/// Reads one request: headers, then up to `Content-Length` body bytes (or `limit` of them).
fn read_request(stream: &mut TcpStream) -> Option<(Vec<(String, String)>, Vec<u8>, usize)> {
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let headers: Vec<(String, String)> = head
        .lines()
        .skip(1)
        .filter_map(|l| l.split_once(':').map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string())))
        .collect();
    let length = headers.iter().find(|(k, _)| k == "content-length").and_then(|(_, v)| v.parse().ok()).unwrap_or(0usize);
    Some((headers, buf.split_off(header_end), length))
}

fn read_body(stream: &mut TcpStream, mut body: Vec<u8>, length: usize) -> Option<Vec<u8>> {
    let mut chunk = [0u8; 65536];
    while body.len() < length {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Some(body)
}

fn respond(stream: &mut TcpStream, status: u16, body: &[u8]) -> bool {
    let head = format!("HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    stream.write_all(head.as_bytes()).and_then(|_| stream.write_all(body)).and_then(|_| stream.flush()).is_ok()
}

/// Each record in a gzipped ndjson body: its event id and whether its origin is the receiver.
fn parse_body(body: &[u8]) -> Vec<(Uuid, bool)> {
    let mut text = Vec::new();
    flate2::read::GzDecoder::new(body).read_to_end(&mut text).expect("the sender gzips");
    text.split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| {
            let r: LogRecord = serde_json::from_slice(l).expect("a record");
            let home = r.attributes.iter().any(|a| a.key == peer::ATTR_ORIGIN && a.value == AttrValue::Str(RECEIVER.into()));
            (r.event_id, home)
        })
        .collect()
}

fn handle_connection(mut stream: TcpStream, state: &Mutex<ProxyState>) {
    let Some((headers, start, length)) = read_request(&mut stream) else { return };
    let (action, upstream) = {
        let mut s = state.lock().unwrap();
        let action = match s.forced.pop_front() {
            Some(a) => a,
            None => match s.base {
                Base::Down => Action::Busy503,
                Base::Pass => Action::Pass,
                Base::Cycle => {
                    s.step += 1;
                    CYCLE[(s.step - 1) % CYCLE.len()]
                }
            },
        };
        s.ledger.requests += 1;
        s.ledger.by_action[action_index(action)] += 1;
        (action, s.upstream.clone())
    };
    if action == Action::DropMidBody {
        // Take part of the body, then hang up without a reply.
        let mut part = vec![0u8; (length / 2).saturating_sub(start.len()).min(65536)];
        let _ = stream.read_exact(&mut part);
        let _ = stream.shutdown(Shutdown::Both);
        return;
    }
    let Some(body) = read_body(&mut stream, start, length) else { return };
    let records = parse_body(&body);
    match action {
        Action::Busy503 => {
            respond(&mut stream, 503, br#"{"error":"busy","message":"soak: injected 503"}"#);
        }
        Action::Limit429 => {
            respond(&mut stream, 429, br#"{"error":"slow_down","message":"soak: injected 429"}"#);
        }
        Action::Auth401 => {
            respond(&mut stream, 401, br#"{"error":"unauthorized","message":"soak: injected 401"}"#);
        }
        Action::Reject422 => {
            if respond(&mut stream, 422, br#"{"error":"rejected","message":"soak: injected 422"}"#) {
                let mut s = state.lock().unwrap();
                s.ledger.rejected += records.len() as u64;
                s.ledger.rejected_ids.extend(records.iter().map(|(id, _)| *id));
            }
        }
        Action::Pass | Action::ForwardThenDrop => {
            let agent: ureq::Agent = ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(10))).http_status_as_error(false).build().into();
            let mut request = agent.post(&format!("{upstream}{INGEST_PATH}"));
            for (k, v) in &headers {
                if matches!(k.as_str(), "content-type" | "content-encoding" | "authorization" | "x-logless-origin" | "x-logless-format") {
                    request = request.header(k.as_str(), v.as_str());
                }
            }
            let (status, reply) = match request.send(&body[..]) {
                Ok(mut r) => (r.status().as_u16(), r.body_mut().read_to_vec().unwrap_or_default()),
                Err(_) => {
                    state.lock().unwrap().ledger.forward_errors += 1;
                    respond(&mut stream, 502, br#"{"error":"bad_gateway"}"#);
                    return;
                }
            };
            assert!(matches!(status, 200 | 508), "the soak receiver always takes a well-formed batch, got {status}");
            {
                let mut s = state.lock().unwrap();
                for (id, home) in &records {
                    if *home {
                        s.ledger.forwarded_loops += 1;
                        s.ledger.loop_ids.insert(*id);
                    } else {
                        s.ledger.forwarded_other += 1;
                    }
                }
            }
            let heard = action == Action::Pass && respond(&mut stream, status, &reply);
            let mut s = state.lock().unwrap();
            if heard {
                s.ledger.relayed += records.len() as u64;
            } else {
                s.ledger.unheard += records.len() as u64;
            }
            if !heard {
                drop(s);
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
        Action::DropMidBody => unreachable!(),
    }
}

fn token() -> String {
    format!("{}{}", Uuid::now_v7().simple(), Uuid::now_v7().simple())
}

fn soak_record(n: u64, home_loop: bool) -> LogRecord {
    let now = crate::now_unix_nanos();
    let mut r = LogRecord::new(now, if n % 7 == 0 { Severity::ERROR } else { Severity::INFO }, format!("soak record {n} with some padding to give the frame weight {}", "x".repeat(200)));
    r.observed_unix_nano = now;
    r.service = Some("soak".into());
    r.template_id = Some(1 + n % 3);
    if home_loop {
        // Came from the receiver once already: it must refuse it as a loop when it comes back.
        r.attributes.push(Attr { key: peer::ATTR_ORIGIN.into(), value: AttrValue::Str(RECEIVER.into()) });
        r.attributes.push(Attr { key: peer::ATTR_HOPS.into(), value: AttrValue::I64(1) });
    }
    r
}

struct Node {
    control: Arc<SyncControl>,
    tap: SyncTap,
    stop: Arc<AtomicBool>,
    handle: std::thread::JoinHandle<()>,
}

fn tuning() -> Tuning {
    Tuning { backoff_unit: Duration::from_millis(10), auth_retry: Duration::from_millis(30), spool_max_bytes: SPOOL_MAX }
}

fn start_node(instance: &str, dir: &Path, target: Option<SyncTarget>) -> Node {
    start_node_tuned(instance, dir, target, tuning())
}

fn start_node_tuned(instance: &str, dir: &Path, target: Option<SyncTarget>, tuning: Tuning) -> Node {
    let control = SyncControl::new(instance.into(), SyncStore::new(dir), target);
    let (tap, rx) = channel(Arc::clone(&control));
    let stop = Arc::new(AtomicBool::new(false));
    let (c, s, d) = (Arc::clone(&control), Arc::clone(&stop), dir.join(SPOOL_DIR));
    let handle = std::thread::spawn(move || run_tuned(c, rx, &d, &s, vec![1, 2, 3], Vec::new(), tuning));
    Node { control, tap, stop, handle }
}

impl Node {
    fn finish(self) -> SyncStatus {
        self.stop.store(true, Ordering::Release);
        drop(self.tap);
        self.handle.join().unwrap();
        self.control.status()
    }
}

fn wait_until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(started.elapsed() < limit, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir).map(|it| it.flatten().filter_map(|e| e.metadata().ok()).filter(|m| m.is_file()).map(|m| m.len()).sum()).unwrap_or(0)
}

type Store = Arc<Mutex<Vec<LogRecord>>>;

fn start_receiver(dir: &Path, token: &str, store: &Store) -> peer::Receiver {
    let sink = Arc::clone(store);
    peer::Receiver::start_in(
        &PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), tokens: vec![token.into()], dedupe_window: 50_000, ..PeerConfig::default() },
        RECEIVER.into(),
        None,
        Some(dir),
        None,
        move |records| {
            sink.lock().unwrap().extend(records);
            Admitted::Accepted
        },
    )
    .unwrap()
}

#[derive(Debug, Default)]
struct Totals {
    offered_copies: u64,
    offered_ids: HashSet<Uuid>,
    loop_injected: HashSet<Uuid>,
    reoffered: u64,
    sender: Vec<SyncStatus>,
    receiver: Vec<PeerStatsSnapshot>,
    sender_restarts: u64,
    receiver_restarts: u64,
    fills: u64,
    max_spool_bytes: u64,
    replay_duplicates_after_receiver_restart: u64,
}

fn add(a: &mut PeerStatsSnapshot, b: &PeerStatsSnapshot) {
    a.requests += b.requests;
    a.records += b.records;
    a.accepted += b.accepted;
    a.duplicates += b.duplicates;
    a.loops += b.loops;
    a.busy += b.busy;
    a.unauthorized += b.unauthorized;
    a.bad_request += b.bad_request;
    a.dedupe_unsaved += b.dedupe_unsaved;
}

struct Rig {
    sender_dir: tempfile::TempDir,
    receiver_dir: tempfile::TempDir,
    token: String,
    store: Store,
    receiver: Option<peer::Receiver>,
    proxy: Proxy,
    node: Option<Node>,
    totals: Totals,
    next: u64,
    sampler_stop: Arc<AtomicBool>,
    max_spool: Arc<AtomicU64>,
    sampler: Option<std::thread::JoinHandle<()>>,
}

impl Rig {
    fn new() -> Rig {
        let (sender_dir, receiver_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let token = token();
        let store: Store = Arc::new(Mutex::new(Vec::new()));
        let receiver = start_receiver(receiver_dir.path(), &token, &store);
        let proxy = Proxy::start(format!("http://{}", receiver.local_addr()));
        let sampler_stop = Arc::new(AtomicBool::new(false));
        let max_spool = Arc::new(AtomicU64::new(0));
        let (ss, ms, sd) = (Arc::clone(&sampler_stop), Arc::clone(&max_spool), sender_dir.path().join(SPOOL_DIR));
        let sampler = std::thread::spawn(move || {
            while !ss.load(Ordering::Acquire) {
                ms.fetch_max(dir_bytes(&sd), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        let mut rig = Rig {
            sender_dir,
            receiver_dir,
            token,
            store,
            receiver: Some(receiver),
            proxy,
            node: None,
            totals: Totals::default(),
            next: 0,
            sampler_stop,
            max_spool,
            sampler: Some(sampler),
        };
        rig.node = Some(start_node("edge", rig.sender_dir.path(), Some(rig.target())));
        rig
    }

    fn target(&self) -> SyncTarget {
        SyncTarget { url: format!("http://{}", self.proxy.addr), token: Some(self.token.clone()), min_severity: 0, fidelity: 100, anomaly_boost: false }
    }

    fn node(&self) -> &Node {
        self.node.as_ref().unwrap()
    }

    fn shed(&self) -> u64 {
        self.totals.sender.iter().map(|s| s.dropped_records).sum::<u64>() + self.node().control.status().dropped_records
    }

    fn offer_new(&mut self, n: usize) {
        let mut batch = Vec::with_capacity(n);
        for _ in 0..n {
            self.next += 1;
            let home = self.next % 10 == 0;
            let r = soak_record(self.next, home);
            self.totals.offered_ids.insert(r.event_id);
            if home {
                self.totals.loop_injected.insert(r.event_id);
            }
            batch.push(r);
        }
        self.totals.offered_copies += n as u64;
        self.node().tap.offer(&batch);
    }

    fn restart_sender(&mut self) {
        let status = self.node.take().unwrap().finish();
        self.totals.sender.push(status);
        self.totals.sender_restarts += 1;
        self.node = Some(start_node("edge", self.sender_dir.path(), Some(self.target())));
    }

    fn drained(&self) -> bool {
        let s = self.node().control.status();
        s.backlog_bytes == 0 && s.queued_batches == 0
    }

    fn wait_drained(&self, limit: Duration) {
        wait_until("the spool to drain", limit, || self.drained());
        // backlog_bytes is refreshed once per sender pass: let one more pass confirm it.
        std::thread::sleep(Duration::from_millis(250));
        wait_until("the spool to drain", limit, || self.drained());
    }

    /// One round of every phase.
    fn round(&mut self) {
        // 1. Stream through the fault cycle, restarting the sender halfway.
        self.proxy.set_base(Base::Cycle);
        for i in 0..40 {
            self.offer_new(25);
            std::thread::sleep(Duration::from_millis(25));
            if i == 20 {
                self.restart_sender();
            }
        }
        self.wait_drained(Duration::from_secs(120));

        // 2. Peer down: fill the spool until it sheds, then let it drain through the fault cycle.
        self.proxy.set_base(Base::Down);
        let shed_before = self.shed();
        let mut offers = 0;
        while self.shed() == shed_before {
            self.offer_new(50);
            offers += 1;
            assert!(offers < 4_000, "the spool never filled");
            std::thread::sleep(Duration::from_millis(3));
        }
        self.totals.fills += 1;
        self.proxy.set_base(Base::Cycle);
        self.wait_drained(Duration::from_secs(120));

        // 3. The receiver stores a batch but the sender never hears; the receiver restarts; the resend must
        //    be recognised from the dedupe log.
        self.proxy.set_base(Base::Down);
        self.proxy.force(Action::ForwardThenDrop);
        let unheard_before = self.proxy.read(|l| l.unheard);
        let before = self.totals.offered_copies;
        self.offer_new(30);
        let batch = self.totals.offered_copies - before;
        wait_until("the dropped reply", Duration::from_secs(30), || self.proxy.read(|l| l.unheard) >= unheard_before + batch);
        let old = self.receiver.take().unwrap();
        self.totals.receiver.push(old.shutdown());
        self.totals.receiver_restarts += 1;
        let receiver = start_receiver(self.receiver_dir.path(), &self.token, &self.store);
        self.proxy.set_upstream(format!("http://{}", receiver.local_addr()));
        self.receiver = Some(receiver);
        self.proxy.set_base(Base::Pass);
        self.wait_drained(Duration::from_secs(60));
        let replayed = self.receiver.as_ref().unwrap().stats();
        assert!(replayed.duplicates + replayed.loops >= batch, "the restarted receiver recognised the resend: {replayed:?}");
        self.totals.replay_duplicates_after_receiver_restart += replayed.duplicates;

        // 4. Records the peer already holds, offered again.
        let again: Vec<LogRecord> = {
            let store = self.store.lock().unwrap();
            // As a replay from the edge's own store would carry them: without the provenance a send adds.
            store
                .iter()
                .rev()
                .take(100)
                .cloned()
                .map(|mut r| {
                    r.attributes.retain(|a| a.key != peer::ATTR_ORIGIN && a.key != peer::ATTR_HOPS);
                    r
                })
                .collect()
        };
        self.totals.offered_copies += again.len() as u64;
        self.totals.reoffered += again.len() as u64;
        self.node().tap.offer(&again);
        self.wait_drained(Duration::from_secs(60));
        self.proxy.set_base(Base::Cycle);
    }

    fn finish(mut self) -> Ledger {
        self.proxy.set_base(Base::Pass);
        self.wait_drained(Duration::from_secs(60));
        self.totals.sender.push(self.node.take().unwrap().finish());
        self.totals.receiver.push(self.receiver.take().unwrap().shutdown());
        self.sampler_stop.store(true, Ordering::Release);
        self.sampler.take().unwrap().join().unwrap();
        self.totals.max_spool_bytes = self.max_spool.load(Ordering::Relaxed);
        let store = self.store.lock().unwrap().clone();
        let proxy = std::mem::take(&mut self.proxy.state.lock().unwrap().ledger);
        Ledger { totals: std::mem::take(&mut self.totals), store, proxy, spool_left: dir_bytes(&self.sender_dir.path().join(SPOOL_DIR)) }
    }
}

struct Ledger {
    totals: Totals,
    store: Vec<LogRecord>,
    proxy: ProxyLedger,
    spool_left: u64,
}

impl Ledger {
    fn check_and_print(&self, elapsed: Duration) {
        let t = &self.totals;
        let p = &self.proxy;
        let mut r = PeerStatsSnapshot::default();
        for s in &t.receiver {
            add(&mut r, s);
        }
        let sum = |f: fn(&SyncStatus) -> u64| t.sender.iter().map(f).sum::<u64>();
        let (s_accepted, s_dup, s_loops, s_shed, s_rejected) =
            (sum(|s| s.sent_records), sum(|s| s.duplicates), sum(|s| s.loops), sum(|s| s.dropped_records), sum(|s| s.rejected_batches));
        let stored: HashSet<Uuid> = self.store.iter().map(|r| r.event_id).collect();
        let accounted: HashSet<Uuid> = stored.iter().chain(p.loop_ids.iter()).chain(p.rejected_ids.iter()).copied().collect();
        let missing = t.offered_ids.difference(&accounted).count() as u64;
        let names = ["200 pass", "503", "429", "401", "drop mid-body", "forward, drop reply", "422"];

        println!("\n=== log-less sync soak: exact accounting ({:.1} s of injected faults) ===", elapsed.as_secs_f64());
        println!("sender restarts {}, receiver restarts {}, spool fills {}", t.sender_restarts, t.receiver_restarts, t.fills);
        println!("proxy requests {}:", p.requests);
        for (name, n) in names.iter().zip(p.by_action) {
            println!("  {name:<22} {n}");
        }
        println!("--- copies (every record handed to the sender, re-offers included) ---");
        println!("offered copies                          {}", t.offered_copies);
        println!("  of which re-offers of stored records  {}", t.reoffered);
        println!("sender: accepted (unique, heard)        {s_accepted}");
        println!("sender: duplicates                      {s_dup}");
        println!("sender: loops                           {s_loops}");
        println!("proxy:  rejected (422, dropped for good) {}", p.rejected);
        println!("sender: shed (tap or spool full)        {s_shed}");
        println!(
            "offered = accepted + duplicates + loops + rejected + shed:  {} = {} + {} + {} + {} + {} = {}",
            t.offered_copies,
            s_accepted,
            s_dup,
            s_loops,
            p.rejected,
            s_shed,
            s_accepted + s_dup + s_loops + p.rejected + s_shed
        );
        println!("--- receiver (both incarnations) ---");
        println!("records parsed {} = accepted {} + duplicates {} + loops {}", r.records, r.accepted, r.duplicates, r.loops);
        println!("proxy forwarded and answered: relayed {} + reply dropped {} = {}", p.relayed, p.unheard, p.relayed + p.unheard);
        println!("duplicates recognised by a restarted receiver: {}", t.replay_duplicates_after_receiver_restart);
        println!("dedupe batches not saved: {}", r.dedupe_unsaved);
        println!("--- distinct event ids ---");
        println!("offered ids {} (loops injected {})", t.offered_ids.len(), t.loop_injected.len());
        println!("stored once {} (stored rows {}), refused as loop {}, rejected {}", stored.len(), self.store.len(), p.loop_ids.len(), p.rejected_ids.len());
        println!("not stored, not refused, not rejected: {missing}  (must equal shed {s_shed})");
        println!("rejected batches seen by the sender: {s_rejected}");
        println!("--- spool ---");
        let pending = t.sender.last().map_or(0, |s| s.backlog_bytes);
        println!(
            "max spool bytes on disk {} (cap {}); at the end {} on disk (the current segment, already delivered), {} pending",
            t.max_spool_bytes, SPOOL_MAX, self.spool_left, pending
        );

        assert_eq!(t.offered_copies, s_accepted + s_dup + s_loops + p.rejected + s_shed, "every copy has exactly one fate");
        assert_eq!(p.relayed, s_accepted + s_dup + s_loops, "the sender counted exactly what the receiver told it");
        assert_eq!(r.records, p.relayed + p.unheard, "the receiver parsed exactly what was forwarded");
        assert_eq!(r.records, r.accepted + r.duplicates + r.loops);
        assert_eq!(self.store.len(), stored.len(), "nothing stored twice");
        assert_eq!(r.accepted as usize, stored.len());
        assert_eq!(r.loops, p.forwarded_loops);
        assert_eq!(p.forwarded_other, stored.len() as u64 + r.duplicates, "each forwarded non-loop copy was stored once or dropped as a duplicate");
        assert!(stored.is_disjoint(&t.loop_injected), "no loop was stored");
        assert_eq!(missing, s_shed, "nothing missing beyond what was shed and counted");
        assert!(t.fills >= 1 && s_shed > 0, "the spool was filled until it shed");
        assert!(t.replay_duplicates_after_receiver_restart > 0, "the dedupe log was exercised across a restart");
        assert!(t.max_spool_bytes <= SPOOL_MAX + 4_096, "spool stayed bounded: {} bytes", t.max_spool_bytes);
        assert_eq!(0, r.dedupe_unsaved);
        assert_eq!(0, pending, "nothing left to send");
        for (name, n) in names.iter().zip(p.by_action) {
            assert!(n > 0, "the fault '{name}' was injected");
        }
    }
}

/// A two-node ring: A sends to B, B forwards everything it takes back to A. A refuses its own records as
/// loops and the traffic stops.
fn ring(n: usize) {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // A spool of the default size: this part is about loops, not shedding.
    let roomy = Tuning { spool_max_bytes: Tuning::default().spool_max_bytes, ..tuning() };
    let node_a = start_node_tuned("A", dir_a.path(), None, roomy);
    let node_b = start_node_tuned("B", dir_b.path(), None, roomy);
    let (tap_a, tap_b) = (node_a.tap.clone(), node_b.tap.clone());
    let config = PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), ..PeerConfig::default() };
    // What each receiver takes goes into its node's WAL, and so to its sender, as in the agent.
    let recv_a = peer::Receiver::start(&config, "A".into(), move |records| {
        tap_a.offer(&records);
        Admitted::Accepted
    })
    .unwrap();
    let recv_b = peer::Receiver::start(&config, "B".into(), move |records| {
        tap_b.offer(&records);
        Admitted::Accepted
    })
    .unwrap();
    let to = |r: &peer::Receiver| SyncTarget { url: format!("http://{}", r.local_addr()), token: None, min_severity: 0, fidelity: 100, anomaly_boost: false };
    node_a.control.set_target(Some(to(&recv_b)));
    node_b.control.set_target(Some(to(&recv_a)));
    let batch: Vec<LogRecord> = (0..n as u64).map(|i| soak_record(i, false)).collect();
    for chunk in batch.chunks(50) {
        node_a.tap.offer(chunk);
    }
    let lap = Instant::now();
    while !(recv_b.stats().accepted == n as u64 && recv_a.stats().loops == n as u64) {
        assert!(lap.elapsed() < Duration::from_secs(60), "no lap: A {:?} B {:?} sender A {:?} sender B {:?}", recv_a.stats(), recv_b.stats(), node_a.control.status(), node_b.control.status());
        std::thread::sleep(Duration::from_millis(10));
    }
    let settled = (recv_a.stats().requests, recv_b.stats().requests);
    std::thread::sleep(Duration::from_millis(1_500));
    let (a, b) = (recv_a.stats(), recv_b.stats());
    let (sa, sb) = (node_a.control.status(), node_b.control.status());
    println!("--- two-node ring A -> B -> A ---");
    println!("offered at A {n}; B accepted {}; A refused as loop {}; A accepted {}", b.accepted, a.loops, a.accepted);
    println!("requests after the lap and 1.5 s later: A {} -> {}, B {} -> {}", settled.0, a.requests, settled.1, b.requests);
    println!("A backlog {} bytes, B backlog {} bytes", sa.backlog_bytes, sb.backlog_bytes);
    assert_eq!((n as u64, 0, n as u64), (b.accepted, a.accepted, a.loops), "one lap, then refused");
    assert_eq!(settled, (a.requests, b.requests), "the ring went quiet");
    assert_eq!((0, 0), (sa.backlog_bytes, sb.backlog_bytes));
    assert_eq!((0, 0), (sa.dropped_records, sb.dropped_records));
    node_a.finish();
    node_b.finish();
    recv_a.shutdown();
    recv_b.shutdown();
}

fn soak(min_duration: Duration) {
    let started = Instant::now();
    let mut rig = Rig::new();
    let mut rounds = 0;
    while rounds == 0 || started.elapsed() < min_duration {
        rig.round();
        rounds += 1;
    }
    let ledger = rig.finish();
    println!("rounds {rounds}");
    ledger.check_and_print(started.elapsed());
    ring(500);
}

#[test]
fn a_flapping_peer_loses_nothing_and_every_record_is_accounted_for() {
    soak(Duration::ZERO);
}

#[test]
#[ignore = "minutes long: LOGLESS_SOAK_SECS (default 180)"]
fn soak_for_minutes() {
    let secs = std::env::var("LOGLESS_SOAK_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(180);
    soak(Duration::from_secs(secs));
}

// --- acknowledgement after the WAL -------------------------------------------------------------------
//
// A central shaped like the agent: the sink queues each batch with a ticket on a bounded channel, a writer
// thread appends it after `append_delay` (a slow disk) and binds the ticket, and every `fsync_every` it
// publishes what it has appended as synced. `None` is a disk that never syncs.

struct WalCentral {
    receiver: Option<peer::Receiver>,
    acks: Arc<WalAcks>,
    /// Appended rows, synced or not.
    appended: Store,
    stop: Arc<AtomicBool>,
    writer: Option<std::thread::JoinHandle<()>>,
}

fn start_wal_central(dir: &Path, queue: usize, bound: Duration, append_delay: Duration, fsync_every: Option<Duration>) -> WalCentral {
    let acks = WalAcks::new(Arc::new(AtomicU64::new(0)), bound);
    let (tx, rx) = crossbeam_channel::bounded::<(Ticket, Vec<LogRecord>)>(queue);
    let appended: Store = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (acks, appended, stop) = (Arc::clone(&acks), Arc::clone(&appended), Arc::clone(&stop));
        std::thread::spawn(move || {
            let (mut total, mut last_sync) = (0u64, Instant::now());
            while !stop.load(Ordering::Acquire) {
                if let Ok((ticket, records)) = rx.recv_timeout(Duration::from_millis(2)) {
                    std::thread::sleep(append_delay);
                    total += records.len() as u64;
                    appended.lock().unwrap().extend(records);
                    acks.bind(ticket, total);
                }
                if let Some(every) = fsync_every {
                    if last_sync.elapsed() >= every {
                        acks.publish(total);
                        last_sync = Instant::now();
                    }
                }
            }
        })
    };
    let config = PeerConfig { enabled: true, addr: "127.0.0.1:0".into(), dedupe_window: 50_000, ..PeerConfig::default() };
    let tickets = Arc::clone(&acks);
    let receiver = peer::Receiver::start_in(&config, RECEIVER.into(), None, Some(dir), Some(Arc::clone(&acks)), move |records| {
        let ticket = tickets.issue();
        match tx.try_send((ticket, records)) {
            Ok(()) => Admitted::Pending(ticket),
            Err(_) => {
                tickets.forget(ticket);
                Admitted::Busy
            }
        }
    })
    .unwrap();
    WalCentral { receiver: Some(receiver), acks, appended, stop, writer: Some(writer) }
}

impl WalCentral {
    fn receiver(&self) -> &peer::Receiver {
        self.receiver.as_ref().unwrap()
    }

    fn target(&self) -> SyncTarget {
        SyncTarget { url: format!("http://{}", self.receiver().local_addr()), token: None, min_severity: 0, fidelity: 100, anomaly_boost: false }
    }

    /// Tears the pipeline down as the agent does on the way out (or as a crash does, for what was not synced).
    fn stop(mut self) -> (PeerStatsSnapshot, Vec<LogRecord>) {
        self.acks.close();
        let stats = self.receiver.take().unwrap().shutdown();
        self.stop.store(true, Ordering::Release);
        self.writer.take().unwrap().join().unwrap();
        let appended = self.appended.lock().unwrap().clone();
        (stats, appended)
    }
}

fn offer_records(node: &Node, batches: usize, per_batch: usize, ids: &mut HashSet<Uuid>, next: &mut u64) {
    for _ in 0..batches {
        let batch: Vec<LogRecord> = (0..per_batch)
            .map(|_| {
                *next += 1;
                soak_record(*next, false)
            })
            .collect();
        ids.extend(batch.iter().map(|r| r.event_id));
        node.tap.offer(&batch);
    }
}

#[test]
fn a_receiver_whose_wal_never_syncs_never_moves_the_senders_cursor() {
    let (edge_dir, central_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // The disk never syncs: every batch waits out the bound and is answered 503.
    let dying = start_wal_central(central_dir.path(), 8, Duration::from_millis(300), Duration::ZERO, None);
    let node = start_node("edge", edge_dir.path(), Some(dying.target()));
    let (mut ids, mut next) = (HashSet::new(), 0);
    offer_records(&node, 2, 25, &mut ids, &mut next);
    // The receiver counts a timeout before its 503 reaches the sender, so wait for both sides to have counted it.
    wait_until("the sender to have been refused twice", Duration::from_secs(30), || {
        dying.receiver().stats().wal_timeouts >= 2 && node.control.status().busy_answers >= 2
    });
    let before_crash = node.control.status();
    assert_eq!(0, before_crash.sent_records, "no 200 for a batch the WAL never synced: {before_crash:?}");
    assert!(before_crash.backlog_bytes > 0, "the cursor did not move: the batch is still pending");
    assert!(before_crash.busy_answers >= 2, "the sender counted the 503s: {before_crash:?}");
    let dedupe = central_dir.path().join(peer::DEDUPE_FILE);
    assert_eq!(0, std::fs::metadata(&dedupe).map(|m| m.len()).unwrap_or(0), "no id saved for an unsynced batch");

    // The central dies. What its writer had appended but never synced is gone with it.
    let (dead, lost) = dying.stop();
    println!("--- receiver whose WAL never synced, then died ---");
    println!("receiver: requests {}, wal waits {}, wal timeouts (503) {}, accepted {}; rows appended unsynced (lost) {}", dead.requests, dead.wal_waits, dead.wal_timeouts, dead.accepted, lost.len());
    println!("sender before the crash: sent {}, busy answers {}, backlog {} bytes", before_crash.sent_records, before_crash.busy_answers, before_crash.backlog_bytes);

    // Restarted on the same data dir with a healthy disk: the spool resends and every record lands once.
    let healthy = start_wal_central(central_dir.path(), 8, Duration::from_secs(5), Duration::ZERO, Some(Duration::from_millis(20)));
    node.control.set_target(Some(healthy.target()));
    wait_until("the resend to land", Duration::from_secs(30), || {
        let s = node.control.status();
        s.sent_records == ids.len() as u64 && s.backlog_bytes == 0
    });
    let edge = node.finish();
    let (after, stored) = healthy.stop();
    let unique: HashSet<Uuid> = stored.iter().map(|r| r.event_id).collect();
    println!("after the restart: receiver accepted {}, duplicates {}, stored rows {} (unique {}), offered {}", after.accepted, after.duplicates, stored.len(), unique.len(), ids.len());
    println!("sender: sent {}, duplicates {}, busy answers {}, backlog {}", edge.sent_records, edge.duplicates, edge.busy_answers, edge.backlog_bytes);
    assert_eq!(dead.accepted, 0);
    assert_eq!(ids, unique, "every offered record is stored");
    assert_eq!(stored.len(), unique.len(), "and stored once");
    assert_eq!((ids.len() as u64, 0), (edge.sent_records, edge.duplicates));
    assert_eq!(ids.len() as u64 * 16, std::fs::metadata(&dedupe).unwrap().len(), "ids saved once synced");
}

#[test]
fn a_full_queue_ahead_of_a_slow_wal_is_a_503_and_every_record_lands_exactly_once() {
    let central_dir = tempfile::tempdir().unwrap();
    // One batch of queue, a disk that takes 15 ms per append and syncs every 25 ms, and four edges at once.
    let central = start_wal_central(central_dir.path(), 1, Duration::from_secs(5), Duration::from_millis(15), Some(Duration::from_millis(25)));
    let edge_dirs: Vec<tempfile::TempDir> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    // A spool of the default size: this run is about the receiver refusing, not the sender shedding.
    let roomy = Tuning { spool_max_bytes: Tuning::default().spool_max_bytes, ..tuning() };
    let nodes: Vec<Node> = edge_dirs.iter().enumerate().map(|(i, d)| start_node_tuned(&format!("edge{i}"), d.path(), Some(central.target()), roomy)).collect();
    let (mut ids, mut next) = (HashSet::new(), 0);
    let started = Instant::now();
    for _ in 0..10 {
        for node in &nodes {
            offer_records(node, 4, 25, &mut ids, &mut next);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    wait_until("every edge to drain", Duration::from_secs(120), || {
        nodes.iter().all(|n| {
            let s = n.control.status();
            s.backlog_bytes == 0 && s.queued_batches == 0
        }) && central.receiver().stats().accepted == ids.len() as u64
    });
    let elapsed = started.elapsed();
    let edges: Vec<SyncStatus> = nodes.into_iter().map(Node::finish).collect();
    let (r, stored) = central.stop();
    let unique: HashSet<Uuid> = stored.iter().map(|rec| rec.event_id).collect();
    let sum = |f: fn(&SyncStatus) -> u64| edges.iter().map(f).sum::<u64>();
    let (sent, dup, busy, shed) = (sum(|s| s.sent_records), sum(|s| s.duplicates), sum(|s| s.busy_answers), sum(|s| s.dropped_records));

    println!("\n=== full queue ahead of a slow WAL: 4 edges -> 1 central ({:.1} s) ===", elapsed.as_secs_f64());
    println!("offered {} records in {} batches", ids.len(), ids.len() / 25);
    println!("receiver: requests {} = 200 after a WAL sync {} + queue-full 503 {} + wal-timeout 503 {}", r.requests, r.wal_waits - r.wal_timeouts, r.busy, r.wal_timeouts);
    println!("receiver: records parsed {} = accepted {} + duplicates {} + in refused batches {}", r.records, r.accepted, r.duplicates, r.records - r.accepted - r.duplicates);
    println!(
        "receiver: wal waits {}, mean {:.1} ms, max {:.1} ms",
        r.wal_waits,
        r.wal_wait_us_total as f64 / r.wal_waits.max(1) as f64 / 1000.0,
        r.wal_wait_us_max as f64 / 1000.0
    );
    println!("senders: sent {sent}, duplicates {dup}, busy answers (503/429) {busy}, shed {shed}");
    println!("stored rows {}, distinct ids {}, offered ids {}, missing {}", stored.len(), unique.len(), ids.len(), ids.difference(&unique).count());

    assert!(r.busy > 0, "the queue filled and answered 503: {r:?}");
    assert_eq!(busy, r.busy + r.wal_timeouts, "each 503 the receiver sent, a sender counted");
    assert_eq!(0, r.wal_timeouts, "the slow WAL stayed inside the bound");
    assert_eq!(r.requests, r.wal_waits + r.busy, "every request either waited for the WAL or was refused");
    assert_eq!(ids, unique, "every record landed");
    assert_eq!(stored.len(), unique.len(), "exactly once");
    assert_eq!((sent, dup, shed), (ids.len() as u64, 0, 0));
}
