//! A small HTTP/1.1 listener with a hard cap on the request head, for the
//! admin API.
//!
//! `tiny_http` buffers request headers without any limit, before a handler
//! (and so before authentication) runs: one unauthenticated 100 MB header
//! grows the process by that much, and the admin listener shares a process with
//! ingest. This listener reads the head into a fixed budget, refuses anything
//! over it with 431, bounds how long a client may take to send it, caps
//! concurrent connections, and answers every request with `Connection: close`
//! so there is no connection state to abuse. Bodies need `Content-Length`;
//! chunked requests are refused. No gzip: the admin API takes small JSON.

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::httpd::{Handler, HttpError, Incoming, Limits, Method, Reject, Reply};

#[derive(Debug, Clone, Copy)]
pub struct HeadLimits {
    /// Request line plus every header, in bytes.
    pub max_head_bytes: usize,
    pub max_headers: usize,
    /// Wall-clock budget to receive the whole request (head and body).
    pub request_timeout: Duration,
    pub max_connections: usize,
}

impl Default for HeadLimits {
    fn default() -> Self {
        Self {
            max_head_bytes: 16 * 1024,
            max_headers: 64,
            request_timeout: Duration::from_secs(10),
            max_connections: 64,
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
            let (stop, active) = (Arc::clone(&stop), Arc::clone(&active));
            std::thread::Builder::new()
                .name(thread_name.to_string())
                .spawn(move || accept_loop(listener, limits, head, handler, stop, active))
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
                tracing::debug!(error = %e, "admin accept failed");
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
        let spawned = std::thread::Builder::new().name("admin-conn".into()).spawn(move || {
            let _guard = guard;
            serve(stream, limits, head, handler.as_ref());
        });
        if spawned.is_err() {
            tracing::warn!("admin api could not spawn a connection thread");
        }
    }
}

enum Bad {
    /// 400 with a reason.
    Request(&'static str),
    HeadTooLarge,
    Unsupported(&'static str),
    Timeout,
    Closed,
}

fn serve(mut stream: TcpStream, limits: Limits, head: HeadLimits, handler: &dyn Handler) {
    let deadline = Instant::now() + head.request_timeout;
    let reply = match read_request(&mut stream, limits, head, deadline) {
        Ok(Parsed { method, path, query, headers, body }) => {
            handler.handle(Incoming::new(method, &path, &query, &headers, body))
        }
        Err(Err(reject)) => handler.reject(reject),
        Err(Ok(Bad::Closed)) => return,
        Err(Ok(Bad::HeadTooLarge)) => plain(431, r#"{"error":"headers_too_large"}"#),
        Err(Ok(Bad::Request(why))) => plain(400, &format!(r#"{{"error":"bad_request","message":"{why}"}}"#)),
        Err(Ok(Bad::Unsupported(why))) => plain(501, &format!(r#"{{"error":"unsupported","message":"{why}"}}"#)),
        Err(Ok(Bad::Timeout)) => plain(408, r#"{"error":"request_timeout"}"#),
    };
    write_reply(&mut stream, &reply);
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

fn read_request(stream: &mut TcpStream, limits: Limits, head: HeadLimits, deadline: Instant) -> ReadResult {
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
    Ok(Parsed { method, path: path.to_string(), query: query.to_string(), headers, body })
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Payload Too Large",
        422 => "Unprocessable Content",
        428 => "Precondition Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "Status",
    }
}

fn write_reply(stream: &mut TcpStream, reply: &Reply) {
    let mut out = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        reply.status,
        reason(reply.status),
        reply.content_type,
        reply.body.len()
    );
    for (k, v) in &reply.headers {
        if !k.contains(['\r', '\n']) && !v.contains(['\r', '\n']) {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
    }
    out.push_str("\r\n");
    let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
    let _ = stream.write_all(out.as_bytes());
    let _ = stream.write_all(&reply.body);
    let _ = stream.flush();
}
