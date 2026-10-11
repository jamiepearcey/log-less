//! A small HTTP/1.1 listener with a hard cap on the request head, for the
//! admin API and the peer receiver.
//!
//! `tiny_http` buffers request headers without any limit, before a handler
//! (and so before authentication) runs: one unauthenticated 100 MB header
//! grows the process by that much, and the admin listener shares a process with
//! ingest. This listener reads the head into a fixed budget, refuses anything
//! over it with 431, bounds how long a client may take to send it, caps
//! concurrent connections, and answers every request with `Connection: close`
//! so there is no connection state to abuse. Bodies need `Content-Length`;
//! chunked requests are refused. Gzip bodies are decoded only for a listener
//! that asks for it ([`HeadLimits::gzip`]), under the decompressed-size cap.
//!
//! A handler can refuse on the head alone ([`Handler::preflight`]), so an
//! unauthenticated client is answered before this process buffers or inflates
//! a byte of its body.

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::httpd::{Handler, HttpError, Incoming, Limits, Method, Reject, Reply, Streamed};

#[derive(Debug, Clone, Copy)]
pub struct HeadLimits {
    /// Request line plus every header, in bytes.
    pub max_head_bytes: usize,
    pub max_headers: usize,
    /// Wall-clock budget to receive the whole request (head and body).
    pub request_timeout: Duration,
    pub max_connections: usize,
    /// Decode `Content-Encoding: gzip` bodies, capped by
    /// `Limits::max_decompressed_bytes`. Off: the body reaches the handler as sent.
    pub gzip: bool,
}

impl Default for HeadLimits {
    fn default() -> Self {
        Self {
            max_head_bytes: 16 * 1024,
            max_headers: 64,
            request_timeout: Duration::from_secs(10),
            max_connections: 64,
            gzip: false,
        }
    }
}

pub struct BoundedServer {
    acceptor: JoinHandle<()>,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    local_addr: SocketAddr,
}

impl BoundedServer {
    pub fn start(
        addr: &str,
        thread_name: &str,
        limits: Limits,
        head: HeadLimits,
        handler: Arc<dyn Handler>,
    ) -> Result<Self, HttpError> {
        let resolved = addr
            .to_socket_addrs()
            .map_err(|source| HttpError::Resolve { addr: addr.to_string(), source })?
            .next()
            .ok_or_else(|| HttpError::NoAddress { addr: addr.to_string() })?;
        let listener = TcpListener::bind(resolved)
            .map_err(|e| HttpError::Bind { addr: addr.to_string(), message: e.to_string() })?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| HttpError::Bind { addr: addr.to_string(), message: e.to_string() })?;
        let stop = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let acceptor = {
            let (stop, active, name) = (Arc::clone(&stop), Arc::clone(&active), thread_name.to_string());
            std::thread::Builder::new()
                .name(thread_name.to_string())
                .spawn(move || accept_loop(listener, name, limits, head, handler, stop, active))
                .expect("spawn http acceptor")
        };
        Ok(Self { acceptor, stop, active, local_addr })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting, then waits (bounded) for in-flight requests so a
    /// client never sees a truncated response.
    pub fn shutdown(self) {
        self.stop.store(true, Ordering::Release);
        // Wake the blocking accept.
        let _ = TcpStream::connect_timeout(&self.local_addr, Duration::from_millis(500));
        let _ = self.acceptor.join();
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.active.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

struct ActiveGuard(Arc<AtomicUsize>);
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn accept_loop(
    listener: TcpListener,
    thread_name: String,
    limits: Limits,
    head: HeadLimits,
    handler: Arc<dyn Handler>,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
) {
    loop {
        let stream = match listener.accept() {
            Ok((s, _)) => s,
            Err(e) => {
                tracing::debug!(error = %e, "http accept failed");
                if stop.load(Ordering::Acquire) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        if stop.load(Ordering::Acquire) {
            return;
        }
        if active.fetch_add(1, Ordering::AcqRel) >= head.max_connections {
            active.fetch_sub(1, Ordering::AcqRel);
            let _ = stream.shutdown(Shutdown::Both);
            continue;
        }
        let guard = ActiveGuard(Arc::clone(&active));
        let handler = Arc::clone(&handler);
        let spawned = std::thread::Builder::new().name(format!("{thread_name}-conn")).spawn(move || {
            let _guard = guard;
            serve(stream, limits, head, handler.as_ref());
        });
        if spawned.is_err() {
            tracing::warn!("http listener could not spawn a connection thread");
        }
    }
}

enum Bad {
    /// 400 with a reason.
    Request(&'static str),
    HeadTooLarge,
    /// The handler answered from the head alone; the body is never read.
    Early(Reply),
    Unsupported(&'static str),
    Timeout,
    Closed,
}

fn serve(mut stream: TcpStream, limits: Limits, head: HeadLimits, handler: &dyn Handler) {
    let deadline = Instant::now() + head.request_timeout;
    let reply = match read_request(&mut stream, limits, head, handler, deadline) {
        Ok(Parsed { method, method_name, path, query, headers, body }) => {
            handler.handle(Incoming::new(method, &method_name, &path, &query, &headers, body))
        }
        Err(Err(reject)) => handler.reject(reject),
        Err(Ok(Bad::Closed)) => return,
        Err(Ok(Bad::Early(reply))) => reply,
        Err(Ok(Bad::HeadTooLarge)) => plain(431, r#"{"error":"headers_too_large"}"#),
        Err(Ok(Bad::Request(why))) => plain(400, &format!(r#"{{"error":"bad_request","message":"{why}"}}"#)),
        Err(Ok(Bad::Unsupported(why))) => plain(501, &format!(r#"{{"error":"unsupported","message":"{why}"}}"#)),
        Err(Ok(Bad::Timeout)) => plain(408, r#"{"error":"request_timeout"}"#),
    };
    write_reply(&mut stream, reply);
    let _ = stream.shutdown(Shutdown::Write);
    drain(&mut stream);
}

/// Reads and discards what the client is still sending, briefly. Closing a
/// socket with unread input makes the OS reset the connection, which can
/// destroy the reply (a 431 for an oversized head) before the client reads it.
fn drain(stream: &mut TcpStream) {
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut left = 4 * 1024 * 1024usize;
    let mut sink = [0u8; 8192];
    while left > 0 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || stream.set_read_timeout(Some(remaining)).is_err() {
            break;
        }
        match stream.read(&mut sink) {
            Ok(0) | Err(_) => break,
            Ok(n) => left = left.saturating_sub(n),
        }
    }
}

fn plain(status: u16, body: &str) -> Reply {
    Reply::json(status, body.to_string())
}

struct Parsed {
    method: Method,
    method_name: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

type ReadResult = Result<Parsed, Result<Bad, Reject>>;
// `Err(Err(reject))` is a body-level rejection the handler words itself;
// `Err(Ok(bad))` is a head-level refusal this module words.
fn fail(b: Bad) -> Result<Parsed, Result<Bad, Reject>> {
    Err(Ok(b))
}

fn fill(stream: &mut TcpStream, buf: &mut Vec<u8>, deadline: Instant) -> Result<usize, Bad> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err(Bad::Timeout);
    }
    let _ = stream.set_read_timeout(Some(left));
    let mut chunk = [0u8; 4096];
    match stream.read(&mut chunk) {
        Ok(0) => Err(Bad::Closed),
        Ok(n) => {
            buf.extend_from_slice(&chunk[..n]);
            Ok(n)
        }
        Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {
            Err(Bad::Timeout)
        }
        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => Ok(0),
        Err(_) => Err(Bad::Closed),
    }
}

fn read_request(
    stream: &mut TcpStream,
    limits: Limits,
    head: HeadLimits,
    handler: &dyn Handler,
    deadline: Instant,
) -> ReadResult {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let end = loop {
        if let Some(i) = find_head_end(&buf) {
            break i;
        }
        if buf.len() > head.max_head_bytes {
            return fail(Bad::HeadTooLarge);
        }
        if let Err(b) = fill(stream, &mut buf, deadline) {
            return fail(b);
        }
    };
    if end > head.max_head_bytes {
        return fail(Bad::HeadTooLarge);
    }
    let Ok(text) = std::str::from_utf8(&buf[..end]) else {
        return fail(Bad::Request("request head is not UTF-8"));
    };
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split(' ');
    let (Some(m), Some(target), Some(version), None) = (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return fail(Bad::Request("malformed request line"));
    };
    if version != "HTTP/1.1" && version != "HTTP/1.0" {
        return fail(Bad::Request("unsupported HTTP version"));
    }
    let method = match m {
        "GET" => Method::Get,
        "POST" => Method::Post,
        "PUT" => Method::Put,
        "HEAD" => Method::Head,
        _ => Method::Other,
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));

    let mut headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        if headers.len() >= head.max_headers {
            return fail(Bad::HeadTooLarge);
        }
        if line.starts_with([' ', '\t']) {
            return fail(Bad::Request("folded headers are not accepted"));
        }
        let Some((name, value)) = line.split_once(':') else {
            return fail(Bad::Request("malformed header"));
        };
        if name.is_empty() || name.bytes().any(|b| b <= b' ' || b == 0x7f) {
            return fail(Bad::Request("malformed header name"));
        }
        headers.push((name.to_string(), value.trim().to_string()));
    }

    if headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("transfer-encoding")) {
        return fail(Bad::Unsupported("send a Content-Length body; chunked requests are not accepted"));
    }
    let mut lengths = headers.iter().filter(|(k, _)| k.eq_ignore_ascii_case("content-length"));
    let declared = match (lengths.next(), lengths.next()) {
        (None, _) => 0usize,
        (Some((_, v)), None) => match v.parse::<usize>() {
            Ok(n) if v.bytes().all(|b| b.is_ascii_digit()) => n,
            _ => return fail(Bad::Request("bad Content-Length")),
        },
        _ => return fail(Bad::Request("duplicate Content-Length")),
    };
    let (path, query) = (path.to_string(), query.to_string());
    if let Some(reply) = handler.preflight(&Incoming::new(method, m, &path, &query, &headers, Vec::new())) {
        return fail(Bad::Early(reply));
    }
    if declared > limits.max_body_bytes {
        return Err(Err(Reject::TooLarge));
    }

    let mut body = buf[end + 4..].to_vec();
    while body.len() < declared {
        if let Err(b) = fill(stream, &mut body, deadline) {
            return fail(b);
        }
    }
    body.truncate(declared);
    if head.gzip && headers.iter().any(|(k, v)| k.eq_ignore_ascii_case("content-encoding") && v.eq_ignore_ascii_case("gzip")) {
        body = inflate(&body, limits.max_decompressed_bytes)?;
    }
    Ok(Parsed { method, method_name: m.to_string(), path, query, headers, body })
}

/// Reads at most `limit` bytes out of the gzip stream, then one more to tell
/// "exactly at the limit" from "over it".
fn inflate(gz: &[u8], limit: usize) -> Result<Vec<u8>, Result<Bad, Reject>> {
    let mut out = Vec::new();
    match flate2::read::GzDecoder::new(gz).take(limit as u64 + 1).read_to_end(&mut out) {
        Ok(n) if n > limit => Err(Err(Reject::TooLarge)),
        Ok(_) => Ok(out),
        Err(e) => Err(Err(Reject::Unreadable(format!("bad gzip: {e}")))),
    }
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        206 => "Partial Content",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        412 => "Precondition Failed",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Content",
        428 => "Precondition Required",
        416 => "Range Not Satisfiable",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        508 => "Loop Detected",
        _ => "Status",
    }
}

fn write_reply(stream: &mut TcpStream, reply: Reply) {
    let length = reply.stream.as_ref().map_or(reply.body.len() as u64, |s| s.len);
    let mut out = format!("HTTP/1.1 {} {}\r\n", reply.status, reason(reply.status));
    if !reply.content_type.is_empty() {
        out.push_str(&format!("Content-Type: {}\r\n", reply.content_type));
    }
    out.push_str(&format!("Content-Length: {length}\r\nConnection: close\r\n"));
    for (k, v) in &reply.headers {
        if !k.contains(['\r', '\n']) && !v.contains(['\r', '\n']) {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
    }
    out.push_str("\r\n");
    // Each write is bounded, so a client that stops reading is dropped after this long.
    let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
    if stream.write_all(out.as_bytes()).is_err() {
        return;
    }
    match reply.stream {
        None => {
            let _ = stream.write_all(&reply.body);
        }
        Some(Streamed { reader: Some(mut reader), len }) => copy_exact(&mut reader, stream, len),
        Some(Streamed { reader: None, .. }) => {}
    }
    let _ = stream.flush();
}

/// Copies up to `len` bytes. A source that ends early or a client that goes away stops it; the
/// connection then closes with fewer bytes than `Content-Length` said, which the client sees.
fn copy_exact(reader: &mut dyn Read, stream: &mut TcpStream, len: u64) {
    let mut buf = vec![0u8; 64 * 1024];
    let mut left = len;
    while left > 0 {
        let want = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        match reader.read(&mut buf[..want]) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                if stream.write_all(&buf[..n]).is_err() {
                    return;
                }
                left -= n as u64;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::httpd::Reject;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Probe {
        handled: Mutex<Vec<Vec<u8>>>,
        deny: bool,
    }

    impl Handler for Probe {
        fn handle(&self, request: Incoming) -> Reply {
            self.handled.lock().unwrap().push(request.body);
            Reply::json(200, "{}")
        }
        fn reject(&self, reason: Reject) -> Reply {
            match reason {
                Reject::TooLarge => Reply::json(413, "{}"),
                Reject::Unreadable(_) => Reply::json(400, "{}"),
            }
        }
        fn preflight(&self, request: &Incoming) -> Option<Reply> {
            (self.deny && request.header("x-deny").is_some()).then(|| Reply::json(401, "{}"))
        }
    }

    fn start(head: HeadLimits, limits: Limits, deny: bool) -> (BoundedServer, Arc<Probe>) {
        let probe = Arc::new(Probe { deny, ..Default::default() });
        let server = BoundedServer::start("127.0.0.1:0", "test", limits, head, probe.clone() as Arc<dyn Handler>).unwrap();
        (server, probe)
    }

    /// Sends `bytes`, returns the status code (0 for none) and whether the server closed the connection.
    fn send(addr: SocketAddr, bytes: &[u8], wait: Duration) -> u16 {
        let mut conn = TcpStream::connect(addr).unwrap();
        conn.set_read_timeout(Some(wait)).unwrap();
        let _ = conn.write_all(bytes);
        let mut out = Vec::new();
        let _ = conn.read_to_end(&mut out);
        String::from_utf8_lossy(&out).split(' ').nth(1).and_then(|s| s.parse().ok()).unwrap_or(0)
    }

    #[test]
    fn a_client_that_stalls_in_the_head_or_the_body_is_cut_off_at_the_request_timeout() {
        let head = HeadLimits { request_timeout: Duration::from_millis(300), ..Default::default() };
        let (server, probe) = start(head, Limits::default(), false);
        let a = server.local_addr();
        let started = Instant::now();
        // A head that never ends: sent a byte at a time would be the same; here it just goes quiet.
        let mut c = TcpStream::connect(a).unwrap();
        c.write_all(b"POST / HTTP/1.1\r\nHost: x\r\n").unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut out = Vec::new();
        let _ = c.read_to_end(&mut out);
        assert!(String::from_utf8_lossy(&out).starts_with("HTTP/1.1 408"), "{}", String::from_utf8_lossy(&out));
        assert!(started.elapsed() < Duration::from_secs(3), "{:?}", started.elapsed());
        // A body that stops half way.
        let status = send(a, b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789", Duration::from_secs(5));
        assert_eq!(408, status);
        // A drip that keeps the connection busy does not extend the budget: the deadline covers the whole request.
        let mut c = TcpStream::connect(a).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let started = Instant::now();
        for byte in b"POST / HTTP/1.1\r\nHost: x\r\nX-Slow: yyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" {
            if c.write_all(&[*byte]).is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
            if started.elapsed() > Duration::from_secs(2) {
                break;
            }
        }
        let mut out = Vec::new();
        let _ = c.read_to_end(&mut out);
        assert!(String::from_utf8_lossy(&out).starts_with("HTTP/1.1 408"), "{}", String::from_utf8_lossy(&out));
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        assert!(probe.handled.lock().unwrap().is_empty());
        server.shutdown();
    }

    #[test]
    fn connections_past_the_cap_are_closed_unread_and_the_slots_come_back() {
        let head = HeadLimits { max_connections: 4, request_timeout: Duration::from_millis(800), ..Default::default() };
        let (server, _) = start(head, Limits::default(), false);
        let a = server.local_addr();
        let idle: Vec<TcpStream> = (0..4).map(|_| TcpStream::connect(a).unwrap()).collect();
        std::thread::sleep(Duration::from_millis(150));
        // Past the cap the server hangs up at once, without waiting for a request.
        let mut refused = 0;
        for _ in 0..10 {
            let mut c = TcpStream::connect(a).unwrap();
            c.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
            let mut b = [0u8; 1];
            if matches!(c.read(&mut b), Ok(0)) {
                refused += 1;
            }
        }
        assert_eq!(10, refused, "every connection past the cap is closed");
        // Once the idle ones time out their slots are free again.
        std::thread::sleep(Duration::from_millis(1200));
        assert_eq!(200, send(a, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n", Duration::from_secs(3)));
        drop(idle);
        server.shutdown();
    }

    #[test]
    fn the_head_is_capped_by_bytes_by_count_and_by_a_missing_terminator() {
        let head = HeadLimits { max_head_bytes: 1024, max_headers: 8, ..Default::default() };
        let (server, probe) = start(head, Limits::default(), false);
        let a = server.local_addr();
        let line = |extra: String| format!("GET / HTTP/1.1\r\nHost: x\r\n{extra}\r\n").into_bytes();
        assert_eq!(431, send(a, &line(format!("X: {}\r\n", "a".repeat(2000))), Duration::from_secs(3)));
        assert_eq!(431, send(a, &line("A: b\r\n".repeat(20)), Duration::from_secs(3)));
        assert_eq!(431, send(a, &[b"GET / HTTP/1.1\r\nX: ".to_vec(), vec![b'z'; 1 << 20]].concat(), Duration::from_secs(3)));
        assert_eq!(431, send(a, &[b"GET /".to_vec(), vec![b'p'; 1 << 20]].concat(), Duration::from_secs(3)));
        assert_eq!(200, send(a, &line("A: b\r\n".repeat(5)), Duration::from_secs(3)));
        assert_eq!(1, probe.handled.lock().unwrap().len());
        server.shutdown();
    }

    #[test]
    fn malformed_heads_are_400_and_never_reach_the_handler() {
        let (server, probe) = start(HeadLimits::default(), Limits::default(), false);
        let a = server.local_addr();
        for bad in [
            &b"GET\r\n\r\n"[..],
            b"GET /\r\n\r\n",
            b"GET / HTTP/1.1 extra\r\n\r\n",
            b"GET / HTTP/2.0\r\nHost: x\r\n\r\n",
            b"GET / HTTP/1.1\r\nno colon here\r\n\r\n",
            b"GET / HTTP/1.1\r\nBad Name: v\r\n\r\n",
            b"GET / HTTP/1.1\r\n: empty-name\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\n folded: continuation\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx",
            b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: abc\r\n\r\n",
            b"GET /\xff HTTP/1.1\r\nHost: x\r\n\r\n",
        ] {
            let status = send(a, bad, Duration::from_secs(3));
            assert_eq!(400, status, "{:?}", String::from_utf8_lossy(bad));
        }
        assert_eq!(501, send(a, b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", Duration::from_secs(3)));
        assert!(probe.handled.lock().unwrap().is_empty());
        server.shutdown();
    }

    #[test]
    fn a_preflight_refusal_is_sent_without_waiting_for_the_body() {
        let (server, probe) = start(HeadLimits::default(), Limits::default(), true);
        let a = server.local_addr();
        let started = Instant::now();
        let status = send(a, b"POST / HTTP/1.1\r\nHost: x\r\nX-Deny: 1\r\nContent-Length: 1000000\r\n\r\n", Duration::from_secs(5));
        assert_eq!(401, status);
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        assert!(probe.handled.lock().unwrap().is_empty());
        server.shutdown();
    }

    fn gz(bytes: &[u8]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        enc.write_all(bytes).unwrap();
        enc.finish().unwrap()
    }

    fn post_gz(addr: SocketAddr, body: &[u8]) -> u16 {
        let head = format!("POST / HTTP/1.1\r\nHost: x\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n", body.len());
        send(addr, &[head.as_bytes(), body].concat(), Duration::from_secs(5))
    }

    #[test]
    fn gzip_is_decoded_only_when_asked_for_and_only_up_to_the_cap() {
        let limits = Limits { max_body_bytes: 1 << 20, max_decompressed_bytes: 4096 };
        let (on, probe) = start(HeadLimits { gzip: true, ..Default::default() }, limits, false);
        let a = on.local_addr();
        assert_eq!(200, post_gz(a, &gz(b"hello")));
        assert_eq!(b"hello", probe.handled.lock().unwrap()[0].as_slice());
        assert_eq!(200, post_gz(a, &gz(&vec![b'a'; 4096])), "exactly at the cap");
        assert_eq!(413, post_gz(a, &gz(&vec![b'a'; 4097])), "one byte over");
        assert_eq!(413, post_gz(a, &gz(&vec![0u8; 64 * 1024 * 1024])), "a bomb");
        assert_eq!(400, post_gz(a, b"not gzip"));
        assert_eq!(2, probe.handled.lock().unwrap().len());
        on.shutdown();

        // Off, the bytes arrive as sent and the handler decides what they are.
        let (off, probe) = start(HeadLimits::default(), limits, false);
        let packed = gz(b"hello");
        assert_eq!(200, post_gz(off.local_addr(), &packed));
        assert_eq!(packed, probe.handled.lock().unwrap()[0]);
        off.shutdown();
    }
}
