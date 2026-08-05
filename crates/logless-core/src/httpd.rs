//! Shared blocking HTTP plumbing for the network receivers.
//!
//! OTLP and Splunk HEC differ in their payloads and in the shape of their
//! errors, and in nothing else: both are `POST` with an optionally gzipped body,
//! served by a small pool of threads, stopped deterministically at shutdown.
//! That part lives here so the two receivers cannot drift on the things that
//! were expensive to get right — body limits, decompression bombs, a worker
//! retiring on a transient accept error, and a shutdown that actually joins.
//!
//! No async runtime, matching the rest of the agent: a few exporters posting
//! batches does not need an executor, and adding one would drag tokio in behind
//! a receiver that is idle most of the day.

use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

/// Largest request body accepted, compressed or not. The OTel Collector's
/// default max is 4 MiB; anything larger is either a misconfiguration or an
/// attempt to make us allocate.
pub const DEFAULT_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Cap on bytes produced by gzip decompression. Without it a few kB of zeros
/// expands into gigabytes and the agent is OOM-killed by its own receiver.
pub const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

/// How long a worker blocks before re-checking the stop flag. Bounds shutdown
/// latency; four wakeups a second per idle worker costs nothing.
const RECV_POLL: std::time::Duration = std::time::Duration::from_millis(250);

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_body_bytes: usize,
    pub max_decompressed_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_decompressed_bytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Other,
}

/// A request whose body has already been read and decompressed within limits.
pub struct Incoming<'a> {
    pub method: Method,
    /// Path with the query string removed.
    pub path: &'a str,
    pub query: &'a str,
    headers: &'a [(String, String)],
    pub body: Vec<u8>,
}

impl Incoming<'_> {
    /// Case-insensitive header lookup. Returns the first match, which is what
    /// every header here is defined to have at most one of.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// `Content-Type` without parameters, lowercased.
    pub fn content_type(&self) -> String {
        self.header("content-type")
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase()
    }
}

/// Why a request never reached the handler's main path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reject {
    /// Body over `max_body_bytes`, or decompressed past `max_decompressed_bytes`.
    TooLarge,
    /// Body unreadable or not valid gzip.
    Unreadable(String),
}

pub struct Reply {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl Reply {
    pub fn new(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: content_type.to_string(),
            body,
            headers: Vec::new(),
        }
    }

    pub fn json(status: u16, body: impl Into<String>) -> Self {
        Self::new(status, "application/json", body.into().into_bytes())
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

/// One protocol. Implementations own their own counters — the two receivers
/// count different things, and a shared "requests" number that meant something
/// different per protocol would be worse than none.
pub trait Handler: Send + Sync + 'static {
    fn handle(&self, request: Incoming) -> Reply;

    /// Shapes the error for a request that never got a body. Protocols differ
    /// here — OTLP wants a protobuf `Status`, HEC wants its own JSON — and a
    /// client parsing a fixed shape must not receive a foreign one.
    fn reject(&self, reason: Reject) -> Reply;
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("cannot resolve listen address {addr}: {source}")]
    Resolve { addr: String, source: std::io::Error },
    #[error("no address resolved for {addr}")]
    NoAddress { addr: String },
    #[error("cannot bind {addr}: {message}")]
    Bind { addr: String, message: String },
}

/// A running listener. Dropping it does not stop the workers — call
/// [`Server::shutdown`], so the caller's shutdown path stays explicit and
/// ordered against the WAL flush.
pub struct Server {
    server: Arc<tiny_http::Server>,
    workers: Vec<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    local_addr: SocketAddr,
}

impl Server {
    pub fn start(
        addr: &str,
        workers: usize,
        thread_prefix: &str,
        limits: Limits,
        handler: Arc<dyn Handler>,
    ) -> Result<Self, HttpError> {
        let resolved = addr
            .to_socket_addrs()
            .map_err(|source| HttpError::Resolve { addr: addr.to_string(), source })?
            .next()
            .ok_or_else(|| HttpError::NoAddress { addr: addr.to_string() })?;

        let server = tiny_http::Server::http(resolved).map_err(|e| HttpError::Bind {
            addr: addr.to_string(),
            message: e.to_string(),
        })?;
        // Ask the OS what it actually bound, so port 0 works in tests and the
        // logged address is the real one.
        let local_addr = match server.server_addr() {
            tiny_http::ListenAddr::IP(a) => a,
            #[allow(unreachable_patterns)]
            _ => resolved,
        };

        let server = Arc::new(server);
        let stop = Arc::new(AtomicBool::new(false));
        let workers = (0..workers.max(1))
            .map(|i| {
                let server = Arc::clone(&server);
                let stop = Arc::clone(&stop);
                let handler = Arc::clone(&handler);
                std::thread::Builder::new()
                    .name(format!("{thread_prefix}-{i}"))
                    .spawn(move || serve(&server, limits, handler.as_ref(), &stop))
                    .expect("spawn http worker")
            })
            .collect();

        Ok(Self { server, workers, stop, local_addr })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting and joins every worker. In-flight requests finish first,
    /// so a client never sees a truncated response and re-sends a batch we
    /// already committed.
    ///
    /// The listening socket is closed by tiny_http's own accept thread, which
    /// it nudges but does not join, so the port can stay bound for a moment
    /// after this returns. Fine for process exit; an in-process restart on a
    /// fixed port should expect to retry the bind.
    pub fn shutdown(self) {
        self.stop.store(true, Ordering::Release);
        // `unblock` only wakes one waiter — it pushes a single token and calls
        // `notify_one` — so it cannot retire a pool on its own. It is a nudge
        // to shorten the last poll; the stop flag is what ends the loop.
        // (Relying on `unblock` alone hung every multi-worker shutdown.)
        self.server.unblock();
        for worker in self.workers {
            let _ = worker.join();
        }
    }
}

fn serve(server: &tiny_http::Server, limits: Limits, handler: &dyn Handler, stop: &AtomicBool) {
    while !stop.load(Ordering::Acquire) {
        match server.recv_timeout(RECV_POLL) {
            Ok(Some(request)) => dispatch(request, limits, handler),
            // Timed out, or woken by `unblock` — re-check the stop flag.
            Ok(None) => {}
            // A failed accept must not retire the worker: one client dropping a
            // connection mid-handshake would otherwise shrink the pool by one,
            // and the receiver would silently stop serving after a few flaky
            // connections. Back off briefly so a broken listener cannot spin.
            Err(error) => {
                tracing::debug!(%error, "http accept failed");
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
}

fn dispatch(mut request: tiny_http::Request, limits: Limits, handler: &dyn Handler) {
    let url = request.url().to_string();
    let (path, query) = match url.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (url, String::new()),
    };
    let method = match request.method() {
        tiny_http::Method::Get => Method::Get,
        tiny_http::Method::Post => Method::Post,
        _ => Method::Other,
    };
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|h| (h.field.as_str().as_str().to_string(), h.value.as_str().to_string()))
        .collect();
    let gzipped = headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("content-encoding") && v.eq_ignore_ascii_case("gzip"));

    // Reject on the declared length before reading anything.
    if request.body_length().is_some_and(|len| len > limits.max_body_bytes) {
        respond(request, handler.reject(Reject::TooLarge));
        return;
    }

    let body = match read_limited(request.as_reader(), limits.max_body_bytes) {
        Ok(b) => b,
        Err(reason) => {
            respond(request, handler.reject(reason));
            return;
        }
    };
    let body = if gzipped {
        match read_limited(
            &mut flate2::read::GzDecoder::new(&body[..]),
            limits.max_decompressed_bytes,
        ) {
            Ok(b) => b,
            Err(Reject::Unreadable(e)) => {
                respond(request, handler.reject(Reject::Unreadable(format!("bad gzip: {e}"))));
                return;
            }
            Err(reason) => {
                respond(request, handler.reject(reason));
                return;
            }
        }
    } else {
        body
    };

    let reply = handler.handle(Incoming {
        method,
        path: &path,
        query: &query,
        headers: &headers,
        body,
    });
    respond(request, reply);
}

/// Reads at most `limit` bytes, then one more to tell "exactly at the limit"
/// from "over it".
fn read_limited(reader: &mut dyn Read, limit: usize) -> Result<Vec<u8>, Reject> {
    let mut buf = Vec::new();
    let read = reader
        .take(limit as u64 + 1)
        .read_to_end(&mut buf)
        .map_err(|e| Reject::Unreadable(e.to_string()))?;
    if read > limit {
        return Err(Reject::TooLarge);
    }
    Ok(buf)
}

fn respond(request: tiny_http::Request, reply: Reply) {
    let mut response = tiny_http::Response::from_data(reply.body)
        .with_status_code(reply.status)
        .with_header(header("Content-Type", &reply.content_type));
    for (name, value) in &reply.headers {
        response = response.with_header(header(name, value));
    }
    let _ = request.respond(response);
}

fn header(name: &str, value: &str) -> tiny_http::Header {
    tiny_http::Header::from_bytes(name.as_bytes(), value.as_bytes())
        .unwrap_or_else(|_| tiny_http::Header::from_bytes(&b"X-Invalid"[..], &b"1"[..]).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Echo {
        seen: Mutex<Vec<(String, String, Vec<u8>)>>,
        rejects: Mutex<Vec<Reject>>,
    }

    impl Handler for Echo {
        fn handle(&self, request: Incoming) -> Reply {
            self.seen.lock().unwrap().push((
                request.path.to_string(),
                request.query.to_string(),
                request.body.clone(),
            ));
            if request.method != Method::Post {
                return Reply::json(405, r#"{"m":"post only"}"#);
            }
            Reply::json(200, format!(r#"{{"bytes":{}}}"#, request.body.len()))
                .with_header("X-Test", "1")
        }

        fn reject(&self, reason: Reject) -> Reply {
            self.rejects.lock().unwrap().push(reason.clone());
            match reason {
                Reject::TooLarge => Reply::json(413, r#"{"m":"too large"}"#),
                Reject::Unreadable(_) => Reply::json(400, r#"{"m":"unreadable"}"#),
            }
        }
    }

    fn start(limits: Limits) -> (Server, Arc<Echo>) {
        let handler = Arc::new(Echo::default());
        let server =
            Server::start("127.0.0.1:0", 2, "test", limits, handler.clone() as Arc<dyn Handler>)
                .unwrap();
        (server, handler)
    }

    fn post(url: &str, body: Vec<u8>, headers: &[(&str, &str)]) -> (u16, String) {
        let mut req = ureq::post(url);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        match req.send(&body[..]) {
            Ok(mut r) => (r.status().as_u16(), r.body_mut().read_to_string().unwrap()),
            Err(ureq::Error::StatusCode(code)) => (code, String::new()),
            Err(e) => panic!("request failed: {e}"),
        }
    }

    #[test]
    fn splits_path_from_query_and_reads_the_body() {
        let (server, handler) = start(Limits::default());
        let url = format!("http://{}/services/collector?channel=abc", server.local_addr());
        let (status, body) = post(&url, b"hello".to_vec(), &[]);
        assert_eq!(status, 200);
        assert_eq!(body, r#"{"bytes":5}"#);
        let seen = handler.seen.lock().unwrap();
        assert_eq!(seen[0].0, "/services/collector");
        assert_eq!(seen[0].1, "channel=abc");
        server.shutdown();
    }

    #[test]
    fn decompresses_gzip_before_the_handler_sees_it() {
        let (server, handler) = start(Limits::default());
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(b"payload").unwrap();
        let gz = enc.finish().unwrap();
        let url = format!("http://{}/x", server.local_addr());
        assert_eq!(post(&url, gz, &[("Content-Encoding", "gzip")]).0, 200);
        assert_eq!(handler.seen.lock().unwrap()[0].2, b"payload");
        server.shutdown();
    }

    #[test]
    fn oversized_body_reaches_reject_not_handle() {
        // The handler must never see a body it did not budget for.
        let (server, handler) = start(Limits { max_body_bytes: 64, ..Default::default() });
        let url = format!("http://{}/x", server.local_addr());
        assert_eq!(post(&url, vec![b'x'; 4096], &[]).0, 413);
        assert!(handler.seen.lock().unwrap().is_empty());
        assert_eq!(handler.rejects.lock().unwrap()[0], Reject::TooLarge);
        server.shutdown();
    }

    #[test]
    fn gzip_bomb_is_capped_by_the_decompressed_limit() {
        let (server, handler) =
            start(Limits { max_body_bytes: 1 << 20, max_decompressed_bytes: 4096 });
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        enc.write_all(&vec![0u8; 1024 * 1024]).unwrap();
        let gz = enc.finish().unwrap();
        assert!(gz.len() < 4096, "test needs the compressed form under the body limit");
        let url = format!("http://{}/x", server.local_addr());
        assert_eq!(post(&url, gz, &[("Content-Encoding", "gzip")]).0, 413);
        assert!(handler.seen.lock().unwrap().is_empty());
        server.shutdown();
    }

    #[test]
    fn corrupt_gzip_is_rejected_as_unreadable() {
        let (server, handler) = start(Limits::default());
        let url = format!("http://{}/x", server.local_addr());
        assert_eq!(post(&url, b"not gzip at all".to_vec(), &[("Content-Encoding", "gzip")]).0, 400);
        assert!(matches!(handler.rejects.lock().unwrap()[0], Reject::Unreadable(_)));
        server.shutdown();
    }

    #[test]
    fn headers_are_case_insensitive_and_method_is_classified() {
        let (server, _) = start(Limits::default());
        let url = format!("http://{}/x", server.local_addr());
        let code = match ureq::get(&url).call() {
            Ok(r) => r.status().as_u16(),
            Err(ureq::Error::StatusCode(c)) => c,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(code, 405);
        server.shutdown();
    }

    #[test]
    fn custom_headers_reach_the_client() {
        let (server, _) = start(Limits::default());
        let url = format!("http://{}/x", server.local_addr());
        let response = ureq::post(&url).send(&b"a"[..]).unwrap();
        assert_eq!(response.headers().get("X-Test").unwrap(), "1");
        server.shutdown();
    }

    #[test]
    fn shutdown_joins_every_worker() {
        // `unblock` wakes one worker; a pool that trusts it hangs on join.
        let handler = Arc::new(Echo::default());
        let server =
            Server::start("127.0.0.1:0", 4, "test", Limits::default(), handler as Arc<dyn Handler>)
                .unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            server.shutdown();
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("shutdown must join every worker, not only the one unblock wakes");
    }
}
