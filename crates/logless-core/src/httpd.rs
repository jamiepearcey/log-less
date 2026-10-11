//! Shared HTTP types for the network receivers.
//!
//! Every listener runs on [`crate::bounded_http`]; this module holds what the
//! protocols share with it: the request and reply shapes, the [`Handler`] a
//! protocol implements, and the body limits. OTLP, Splunk HEC and the Sentry
//! proxy differ in their payloads and in the shape of their errors, and in
//! nothing else, so the expensive parts (body limits, decompression bombs,
//! header and connection caps, a shutdown that waits) live in one place and
//! cannot drift between them.
//!
//! No async runtime, matching the rest of the agent: a few exporters posting
//! batches does not need an executor, and adding one would drag tokio in behind
//! a receiver that is idle most of the day.

use std::io::Read;

/// Largest request body accepted, compressed or not. The OTel Collector's
/// default max is 4 MiB; anything larger is either a misconfiguration or an
/// attempt to make us allocate.
pub const DEFAULT_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Cap on bytes produced by gzip decompression. Without it a few kB of zeros
/// expands into gigabytes and the agent is OOM-killed by its own receiver.
pub const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

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
    Put,
    Head,
    Other,
}

/// A request whose body has already been read and decompressed within limits.
pub struct Incoming<'a> {
    pub method: Method,
    /// The method as sent (`DELETE`, `PROPFIND`), for a handler that has to name it.
    pub method_name: &'a str,
    /// Path with the query string removed.
    pub path: &'a str,
    pub query: &'a str,
    headers: &'a [(String, String)],
    pub body: Vec<u8>,
}

impl<'a> Incoming<'a> {
    pub(crate) fn new(
        method: Method,
        method_name: &'a str,
        path: &'a str,
        query: &'a str,
        headers: &'a [(String, String)],
        body: Vec<u8>,
    ) -> Self {
        Self { method, method_name, path, query, headers, body }
    }

    pub(crate) fn headers(&self) -> &'a [(String, String)] {
        self.headers
    }

    /// How many headers carry this name. A header that must be unique (Host)
    /// is ambiguous when this is above one.
    pub fn header_count(&self, name: &str) -> usize {
        self.headers.iter().filter(|(k, _)| k.eq_ignore_ascii_case(name)).count()
    }

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
    /// Sent instead of `body` by `crate::bounded_http`, which ignores `body`
    /// then.
    pub(crate) stream: Option<Streamed>,
}

/// A reply body read from `reader` while it is written, so a large file is
/// never held in memory. `reader` is `None` for a `HEAD`: `len` is advertised
/// in `Content-Length` and nothing is sent.
pub struct Streamed {
    pub reader: Option<Box<dyn Read + Send>>,
    pub len: u64,
}

impl Reply {
    pub fn new(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: content_type.to_string(),
            body,
            headers: Vec::new(),
            stream: None,
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

    /// Called with the request head before the body is read. `Some` answers
    /// the request there and then, so a refusal that needs no body
    /// (authentication) never buffers or inflates one. `request.body` is empty
    /// here.
    fn preflight(&self, _request: &Incoming) -> Option<Reply> {
        None
    }
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
