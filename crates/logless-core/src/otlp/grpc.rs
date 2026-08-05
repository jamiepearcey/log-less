//! OTLP/gRPC logs receiver — `opentelemetry.proto.collector.logs.v1.LogsService/Export`.
//!
//! The OTel Collector's `otlp` exporter, and the default endpoint of most
//! language SDKs, is gRPC on `:4317`. OTLP/HTTP already covers the fleets that
//! were configured deliberately; this covers the ones that were not configured
//! at all, which is most of them.
//!
//! Plaintext h2c only. TLS termination belongs to whatever already terminates
//! it on the node — and the default bind is loopback, where TLS buys nothing.
//!
//! # Shape of a call
//!
//! ```text
//! client → HEADERS  :method POST, :path /…/Export, content-type application/grpc, te trailers
//!        → DATA     [compressed flag][u32 length][protobuf]  END_STREAM
//! server → HEADERS  :status 200, content-type application/grpc
//!        → DATA     [0][u32 length][empty ExportLogsServiceResponse]
//!        → HEADERS  grpc-status: 0                            END_STREAM
//! ```
//!
//! The status lives in the *trailers*, not in `:status` — a gRPC error is an
//! HTTP 200 with `grpc-status` set. Returning an HTTP error status instead is
//! the classic way to make a gRPC client report something unrelated to what
//! actually went wrong.

use std::io::{BufWriter, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::h2::{self, error_code, flag, kind, setting, FrameHeader, H2Error};
use super::http::{Accepted, ReceiverStats, StatsSnapshot};
use super::logs::decode_request;
use crate::model::LogRecord;

/// gRPC status codes we return. `OK` travels in the trailers of a successful
/// call just like an error does.
pub mod status {
    pub const OK: u32 = 0;
    pub const INVALID_ARGUMENT: u32 = 3;
    pub const RESOURCE_EXHAUSTED: u32 = 8;
    pub const UNIMPLEMENTED: u32 = 12;
    pub const UNAVAILABLE: u32 = 14;
}

const EXPORT_PATH: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

/// Stream-level flow-control window we advertise, and the connection window we
/// top up to. Big enough that a 4 MiB batch never round-trips for credit.
const WINDOW: u32 = 8 * 1024 * 1024;

/// Concurrent streams per connection. gRPC clients pipeline aggressively; this
/// bounds how much half-assembled request body one connection can hold.
const MAX_CONCURRENT_STREAMS: u32 = 128;

/// How long a connection may sit idle before the worker re-checks the stop flag.
const READ_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    /// Opt-in, like every listener here.
    #[serde(default)]
    pub enabled: bool,
    /// The port every OTLP gRPC client already defaults to.
    #[serde(default = "default_grpc_addr")]
    pub addr: String,
    /// Connections served at once. One thread each: a gRPC client holds one
    /// connection open and multiplexes over it, so this is a count of clients,
    /// not of requests.
    #[serde(default = "default_grpc_connections")]
    pub max_connections: usize,
    #[serde(default = "default_grpc_max_message")]
    pub max_message_bytes: usize,
    #[serde(default = "default_grpc_max_decompressed")]
    pub max_decompressed_bytes: usize,
}

fn default_grpc_addr() -> String {
    "127.0.0.1:4317".to_string()
}
fn default_grpc_connections() -> usize {
    64
}
fn default_grpc_max_message() -> usize {
    super::http::DEFAULT_MAX_BODY_BYTES
}
fn default_grpc_max_decompressed() -> usize {
    super::http::DEFAULT_MAX_DECOMPRESSED_BYTES
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: default_grpc_addr(),
            max_connections: default_grpc_connections(),
            max_message_bytes: default_grpc_max_message(),
            max_decompressed_bytes: default_grpc_max_decompressed(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    #[error("cannot resolve listen address {addr}: {source}")]
    Resolve { addr: String, source: std::io::Error },
    #[error("no address resolved for {addr}")]
    NoAddress { addr: String },
    #[error("cannot bind {addr}: {source}")]
    Bind { addr: String, source: std::io::Error },
}

type Sink = Arc<dyn Fn(Vec<LogRecord>) -> Accepted + Send + Sync>;

pub struct Receiver {
    stop: Arc<AtomicBool>,
    acceptor: Option<std::thread::JoinHandle<()>>,
    stats: Arc<ReceiverStats>,
    connections: Arc<AtomicU64>,
    local_addr: SocketAddr,
}

impl Receiver {
    pub fn start<F>(config: &GrpcConfig, sink: F) -> Result<Self, GrpcError>
    where
        F: Fn(Vec<LogRecord>) -> Accepted + Send + Sync + 'static,
    {
        let resolved = config
            .addr
            .to_socket_addrs()
            .map_err(|source| GrpcError::Resolve { addr: config.addr.clone(), source })?
            .next()
            .ok_or_else(|| GrpcError::NoAddress { addr: config.addr.clone() })?;
        let listener = TcpListener::bind(resolved)
            .map_err(|source| GrpcError::Bind { addr: config.addr.clone(), source })?;
        let local_addr = listener.local_addr().unwrap_or(resolved);
        // Non-blocking accept so the acceptor can notice the stop flag without
        // needing a self-connect to unwedge it.
        listener
            .set_nonblocking(true)
            .map_err(|source| GrpcError::Bind { addr: config.addr.clone(), source })?;

        let stop = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(ReceiverStats::default());
        let connections = Arc::new(AtomicU64::new(0));
        let sink: Sink = Arc::new(sink);

        let acceptor = {
            let stop = Arc::clone(&stop);
            let stats = Arc::clone(&stats);
            let connections = Arc::clone(&connections);
            let config = config.clone();
            std::thread::Builder::new()
                .name("otlp-grpc".into())
                .spawn(move || {
                    accept_loop(listener, &config, &stop, &stats, &connections, sink);
                })
                .expect("spawn grpc acceptor")
        };

        Ok(Self { stop, acceptor: Some(acceptor), stats, connections, local_addr })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn open_connections(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }

    /// Stops accepting and joins the acceptor. Connection threads are detached
    /// and observe the same stop flag within one read timeout, so an in-flight
    /// export still gets its trailers rather than a severed socket — a client
    /// that sees a dropped connection retries a batch we already committed.
    pub fn shutdown(mut self) -> StatsSnapshot {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.acceptor.take() {
            let _ = handle.join();
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while self.connections.load(Ordering::Acquire) > 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        self.stats.snapshot()
    }
}

fn accept_loop(
    listener: TcpListener,
    config: &GrpcConfig,
    stop: &Arc<AtomicBool>,
    stats: &Arc<ReceiverStats>,
    connections: &Arc<AtomicU64>,
    sink: Sink,
) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _peer)) => {
                if connections.load(Ordering::Acquire) as usize >= config.max_connections {
                    // Refuse by closing: a gRPC client reconnects, and holding
                    // an unserved socket open looks like a hang instead.
                    drop(stream);
                    continue;
                }
                connections.fetch_add(1, Ordering::AcqRel);
                let stop = Arc::clone(stop);
                let stats = Arc::clone(stats);
                let connections = Arc::clone(connections);
                let sink = Arc::clone(&sink);
                let config = config.clone();
                let counter = Arc::clone(&connections);
                let spawned = std::thread::Builder::new()
                    .name("otlp-grpc-conn".into())
                    .spawn(move || {
                        if let Err(e) = serve_connection(stream, &config, &stop, &stats, &sink) {
                            tracing::debug!(error = %e, "grpc connection ended");
                        }
                        counter.fetch_sub(1, Ordering::AcqRel);
                    });
                if spawned.is_err() {
                    connections.fetch_sub(1, Ordering::AcqRel);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => {
                tracing::debug!(error = %e, "grpc accept failed");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// One in-flight request stream.
#[derive(Default)]
struct Stream {
    path: String,
    content_type: String,
    grpc_encoding: String,
    /// HPACK is stateful across a connection, so a header block split over
    /// CONTINUATION frames must be reassembled and decoded exactly once.
    header_fragment: Vec<u8>,
    headers_done: bool,
    body: Vec<u8>,
    too_large: bool,
}

fn serve_connection(
    stream: TcpStream,
    config: &GrpcConfig,
    stop: &AtomicBool,
    stats: &ReceiverStats,
    sink: &Sink,
) -> Result<(), H2Error> {
    stream.set_nodelay(true).ok();
    // BSD (and so macOS) hands back a socket that inherits the listener's
    // non-blocking flag; Linux does not. Left alone, every read here returns
    // EWOULDBLOCK on macOS and blocks on Linux — the same code behaving
    // differently per platform, which is worse than either behaviour.
    stream.set_nonblocking(false).ok();
    stream.set_read_timeout(Some(READ_TIMEOUT)).ok();
    let mut reader = stream.try_clone()?;
    let mut writer = BufWriter::new(stream);

    h2::read_preface(&mut reader)?;
    h2::write_settings(
        &mut writer,
        &[
            (setting::MAX_CONCURRENT_STREAMS, MAX_CONCURRENT_STREAMS),
            (setting::INITIAL_WINDOW_SIZE, WINDOW),
            (setting::MAX_FRAME_SIZE, h2::DEFAULT_MAX_FRAME_SIZE as u32),
            (setting::ENABLE_PUSH, 0),
        ],
    )?;
    // Lift the connection-level receive window immediately; it starts at 64 KiB
    // no matter what SETTINGS says, and one OTLP batch is bigger than that.
    h2::write_window_update(&mut writer, 0, WINDOW)?;
    writer.flush()?;

    let mut decoder = fluke_hpack::Decoder::new();
    let mut encoder = fluke_hpack::Encoder::new();
    let mut streams: std::collections::HashMap<u32, Stream> = std::collections::HashMap::new();
    let mut connection_credit_used: u32 = 0;

    let result = loop {
        if stop.load(Ordering::Acquire) {
            break Ok(());
        }
        // An idle connection times out here, which is how the stop flag gets
        // noticed. Past this point the frame is half-read, so a timeout must
        // mean "wait longer", not "give up" — abandoning it would leave the
        // next read starting mid-frame and every frame after it misparsed.
        let mut head = [0u8; 9];
        match read_exact_patient(&mut reader, &mut head, stop, true) {
            Ok(true) => {}
            Ok(false) => break Ok(()),
            Err(e) => break Err(e.into()),
        }
        let header = h2::parse_frame_header(&head);
        if header.length as usize > h2::DEFAULT_MAX_FRAME_SIZE {
            break Err(H2Error::protocol(
                error_code::FRAME_SIZE_ERROR,
                format!("frame of {} bytes exceeds max_frame_size", header.length),
            ));
        }
        let mut payload = vec![0u8; header.length as usize];
        match read_exact_patient(&mut reader, &mut payload, stop, false) {
            Ok(true) => {}
            Ok(false) => break Ok(()),
            Err(e) => break Err(e.into()),
        }

        let outcome = handle_frame(
            &header,
            &payload,
            config,
            stats,
            sink,
            &mut streams,
            &mut decoder,
            &mut encoder,
            &mut writer,
            &mut connection_credit_used,
        );
        match outcome {
            Ok(()) => {}
            Err(e) => break Err(e),
        }
        writer.flush()?;
    };

    // Say why we are going, so the client logs something useful instead of a
    // bare connection reset.
    match &result {
        Ok(()) => {
            let _ = h2::write_goaway(&mut writer, 0, error_code::NO_ERROR, "shutting down");
        }
        Err(e) => {
            let _ = h2::write_goaway(&mut writer, 0, e.code(), &e.to_string());
        }
    }
    let _ = writer.flush();
    result
}

#[allow(clippy::too_many_arguments)]
fn handle_frame(
    header: &FrameHeader,
    payload: &[u8],
    config: &GrpcConfig,
    stats: &ReceiverStats,
    sink: &Sink,
    streams: &mut std::collections::HashMap<u32, Stream>,
    decoder: &mut fluke_hpack::Decoder,
    encoder: &mut fluke_hpack::Encoder,
    writer: &mut impl Write,
    connection_credit_used: &mut u32,
) -> Result<(), H2Error> {
    match header.kind {
        kind::SETTINGS => {
            if header.has(flag::ACK) {
                return Ok(());
            }
            for (id, value) in h2::parse_settings(payload)? {
                if id == setting::HEADER_TABLE_SIZE {
                    encoder.set_max_table_size(value as usize);
                }
            }
            h2::write_frame(writer, kind::SETTINGS, flag::ACK, 0, &[])?;
        }
        kind::PING => {
            if !header.has(flag::ACK) {
                h2::write_frame(writer, kind::PING, flag::ACK, 0, payload)?;
            }
        }
        kind::WINDOW_UPDATE => {
            // Our responses are a few hundred bytes, so send-side credit is
            // never the constraint; the frame is still validated.
            h2::parse_window_update(payload)?;
        }
        kind::GOAWAY => {
            return Err(H2Error::protocol(error_code::NO_ERROR, "client sent GOAWAY"));
        }
        kind::RST_STREAM => {
            streams.remove(&header.stream_id);
        }
        kind::PRIORITY => {}
        kind::PUSH_PROMISE => {
            return Err(H2Error::protocol(
                error_code::PROTOCOL_ERROR,
                "clients must not push",
            ));
        }
        kind::HEADERS | kind::CONTINUATION => {
            if header.stream_id == 0 {
                return Err(H2Error::protocol(
                    error_code::PROTOCOL_ERROR,
                    "HEADERS on stream 0",
                ));
            }
            if header.kind == kind::HEADERS && streams.len() >= MAX_CONCURRENT_STREAMS as usize {
                h2::write_rst_stream(writer, header.stream_id, error_code::REFUSED_STREAM)?;
                return Ok(());
            }
            let fragment = h2::strip_padding(payload, header.flags, header.kind == kind::HEADERS)?;
            let stream = streams.entry(header.stream_id).or_default();
            stream.header_fragment.extend_from_slice(fragment);

            if header.has(flag::END_HEADERS) {
                // Decoding must happen exactly once per block and in arrival
                // order: the HPACK dynamic table is connection state, so a
                // skipped or repeated block desynchronises every later header.
                let decoded = decoder
                    .decode(&stream.header_fragment)
                    .map_err(|e| H2Error::protocol(error_code::COMPRESSION_ERROR, format!("{e:?}")))?;
                stream.header_fragment.clear();
                stream.headers_done = true;
                for (name, value) in decoded {
                    let name = String::from_utf8_lossy(&name).to_ascii_lowercase();
                    let value = String::from_utf8_lossy(&value).into_owned();
                    match name.as_str() {
                        ":path" => stream.path = value,
                        "content-type" => stream.content_type = value.to_ascii_lowercase(),
                        "grpc-encoding" => stream.grpc_encoding = value.to_ascii_lowercase(),
                        _ => {}
                    }
                }
            }
            if header.has(flag::END_STREAM) {
                finish_stream(header.stream_id, config, stats, sink, streams, encoder, writer)?;
            }
        }
        kind::DATA => {
            let body = h2::strip_padding(payload, header.flags, false)?;
            // Credit is returned for the *whole* frame including padding, or
            // the peer's accounting and ours drift apart and it stalls.
            *connection_credit_used += header.length;
            if *connection_credit_used >= WINDOW / 2 {
                h2::write_window_update(writer, 0, *connection_credit_used)?;
                *connection_credit_used = 0;
            }

            if let Some(stream) = streams.get_mut(&header.stream_id) {
                if stream.body.len() + body.len() > config.max_message_bytes {
                    // Keep reading so flow control stays consistent, but stop
                    // buffering — the answer is already decided.
                    stream.too_large = true;
                    stream.body.clear();
                } else if !stream.too_large {
                    stream.body.extend_from_slice(body);
                }
                if header.length > 0 {
                    h2::write_window_update(writer, header.stream_id, header.length)?;
                }
            }
            if header.has(flag::END_STREAM) {
                finish_stream(header.stream_id, config, stats, sink, streams, encoder, writer)?;
            }
        }
        _ => {} // Unknown frame types must be ignored, not fatal.
    }
    Ok(())
}

fn finish_stream(
    stream_id: u32,
    config: &GrpcConfig,
    stats: &ReceiverStats,
    sink: &Sink,
    streams: &mut std::collections::HashMap<u32, Stream>,
    encoder: &mut fluke_hpack::Encoder,
    writer: &mut impl Write,
) -> Result<(), H2Error> {
    let Some(stream) = streams.remove(&stream_id) else {
        return Ok(());
    };
    if !stream.headers_done {
        return Err(H2Error::protocol(error_code::PROTOCOL_ERROR, "stream ended mid-headers"));
    }

    stats.requests.fetch_add(1, Ordering::Relaxed);

    if stream.too_large {
        stats.too_large.fetch_add(1, Ordering::Relaxed);
        return trailers_only(
            writer,
            encoder,
            stream_id,
            status::RESOURCE_EXHAUSTED,
            "message exceeds max_message_bytes",
        );
    }
    if !stream.content_type.starts_with("application/grpc") {
        stats.bad_request.fetch_add(1, Ordering::Relaxed);
        return trailers_only(
            writer,
            encoder,
            stream_id,
            status::INVALID_ARGUMENT,
            "content-type must be application/grpc",
        );
    }
    if stream.path != EXPORT_PATH {
        stats.bad_request.fetch_add(1, Ordering::Relaxed);
        return trailers_only(
            writer,
            encoder,
            stream_id,
            status::UNIMPLEMENTED,
            "only LogsService/Export is implemented",
        );
    }

    let message = match unframe(&stream.body, &stream.grpc_encoding, config.max_decompressed_bytes)
    {
        Ok(m) => m,
        Err(reason) => {
            stats.bad_request.fetch_add(1, Ordering::Relaxed);
            return trailers_only(writer, encoder, stream_id, status::INVALID_ARGUMENT, &reason);
        }
    };

    let decoded = match decode_request(&message, crate::now_unix_nanos()) {
        Ok(d) => d,
        Err(e) => {
            stats.bad_request.fetch_add(1, Ordering::Relaxed);
            return trailers_only(
                writer,
                encoder,
                stream_id,
                status::INVALID_ARGUMENT,
                &format!("malformed OTLP: {e}"),
            );
        }
    };

    stats.dropped_upstream.fetch_add(decoded.dropped_upstream, Ordering::Relaxed);
    let count = decoded.records.len() as u64;

    match sink(decoded.records) {
        Accepted::All => {
            stats.accepted.fetch_add(1, Ordering::Relaxed);
            stats.records.fetch_add(count, Ordering::Relaxed);
            // Empty ExportLogsServiceResponse, gRPC-framed: uncompressed flag,
            // then a zero length.
            let response = [0u8, 0, 0, 0, 0];
            write_headers(writer, encoder, stream_id, &grpc_ok_headers(), false)?;
            h2::write_frame(writer, kind::DATA, 0, stream_id, &response)?;
            write_headers(
                writer,
                encoder,
                stream_id,
                &[("grpc-status".to_string(), status::OK.to_string())],
                true,
            )
        }
        Accepted::Rejected => {
            stats.rejected.fetch_add(1, Ordering::Relaxed);
            // UNAVAILABLE is the status an OTel exporter retries on. RESOURCE_
            // EXHAUSTED would be closer in spirit but several exporters treat
            // it as permanent, which turns backpressure into data loss.
            trailers_only(writer, encoder, stream_id, status::UNAVAILABLE, "pipeline full, retry")
        }
    }
}

fn grpc_ok_headers() -> Vec<(String, String)> {
    vec![
        (":status".to_string(), "200".to_string()),
        ("content-type".to_string(), "application/grpc".to_string()),
    ]
}

/// A gRPC error is an HTTP 200 whose HEADERS frame carries the status and ends
/// the stream — the "trailers-only" response.
fn trailers_only(
    writer: &mut impl Write,
    encoder: &mut fluke_hpack::Encoder,
    stream_id: u32,
    code: u32,
    message: &str,
) -> Result<(), H2Error> {
    let headers = vec![
        (":status".to_string(), "200".to_string()),
        ("content-type".to_string(), "application/grpc".to_string()),
        ("grpc-status".to_string(), code.to_string()),
        ("grpc-message".to_string(), percent_encode(message)),
    ];
    write_headers(writer, encoder, stream_id, &headers, true)
}

fn write_headers(
    writer: &mut impl Write,
    encoder: &mut fluke_hpack::Encoder,
    stream_id: u32,
    headers: &[(String, String)],
    end_stream: bool,
) -> Result<(), H2Error> {
    let pairs: Vec<(&[u8], &[u8])> =
        headers.iter().map(|(k, v)| (k.as_bytes(), v.as_bytes())).collect();
    let block = encoder.encode(pairs);
    let mut flags = flag::END_HEADERS;
    if end_stream {
        flags |= flag::END_STREAM;
    }
    h2::write_frame(writer, kind::HEADERS, flags, stream_id, &block)
}

/// `grpc-message` is percent-encoded ASCII; a raw newline or non-ASCII byte in
/// a header value is a protocol violation, and our messages carry decoder
/// output that can contain anything.
fn percent_encode(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    for byte in message.bytes() {
        match byte {
            b' '..=b'~' if byte != b'%' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Unwraps the gRPC length-prefixed message framing: one byte of "is this
/// message compressed", four bytes of big-endian length, then the payload.
fn unframe(body: &[u8], encoding: &str, max_decompressed: usize) -> Result<Vec<u8>, String> {
    if body.is_empty() {
        return Err("empty request body".to_string());
    }
    if body.len() < 5 {
        return Err("truncated gRPC frame header".to_string());
    }
    let compressed = body[0];
    let length = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    let payload = &body[5..];
    if payload.len() < length {
        return Err(format!("gRPC frame claims {length} bytes, got {}", payload.len()));
    }
    let payload = &payload[..length];

    match compressed {
        0 => Ok(payload.to_vec()),
        1 => {
            if encoding != "gzip" {
                return Err(format!("compressed message with unsupported encoding '{encoding}'"));
            }
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(payload)
                .take(max_decompressed as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| format!("bad gzip: {e}"))?;
            if out.len() > max_decompressed {
                return Err("decompressed message exceeds the configured limit".to_string());
            }
            Ok(out)
        }
        other => Err(format!("unknown compressed-flag {other}")),
    }
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)
}

/// Fills `buf`, retrying through read timeouts.
///
/// Returns `Ok(false)` when the connection is finished: end of stream, or —
/// only when `interruptible` — a shutdown observed while no frame is in
/// progress. Mid-frame the stop flag is deliberately ignored, so a client that
/// is halfway through sending a batch still gets its trailers instead of a
/// severed socket and a retry of data we already committed.
fn read_exact_patient(
    reader: &mut impl Read,
    buf: &mut [u8],
    stop: &AtomicBool,
    interruptible: bool,
) -> std::io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        if interruptible && filled == 0 && stop.load(Ordering::Acquire) {
            return Ok(false);
        }
        match reader.read(&mut buf[filled..]) {
            Ok(0) => return Ok(false),
            Ok(n) => filled += n,
            Err(ref e) if is_timeout(e) => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                let _ = e;
                return Ok(false);
            }
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn unframes_an_uncompressed_message() {
        let mut body = vec![0u8];
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(b"abc");
        assert_eq!(unframe(&body, "", 1 << 20).unwrap(), b"abc");
    }

    #[test]
    fn unframes_a_gzipped_message() {
        use std::io::Write as _;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(b"payload").unwrap();
        let gz = enc.finish().unwrap();
        let mut body = vec![1u8];
        body.extend_from_slice(&(gz.len() as u32).to_be_bytes());
        body.extend_from_slice(&gz);
        assert_eq!(unframe(&body, "gzip", 1 << 20).unwrap(), b"payload");
    }

    #[test]
    fn a_compressed_message_without_a_declared_encoding_is_refused() {
        // Guessing gzip here would mean decoding attacker-chosen bytes as a
        // compression stream on the strength of one flag byte.
        let mut body = vec![1u8];
        body.extend_from_slice(&1u32.to_be_bytes());
        body.push(0);
        assert!(unframe(&body, "", 1 << 20).unwrap_err().contains("unsupported encoding"));
    }

    #[test]
    fn a_gzip_bomb_is_capped() {
        use std::io::Write as _;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        enc.write_all(&vec![0u8; 1 << 20]).unwrap();
        let gz = enc.finish().unwrap();
        let mut body = vec![1u8];
        body.extend_from_slice(&(gz.len() as u32).to_be_bytes());
        body.extend_from_slice(&gz);
        assert!(unframe(&body, "gzip", 4096).unwrap_err().contains("exceeds"));
    }

    #[test]
    fn truncated_and_lying_frames_are_rejected() {
        assert!(unframe(&[], "", 1 << 20).unwrap_err().contains("empty"));
        assert!(unframe(&[0, 0, 0], "", 1 << 20).unwrap_err().contains("truncated"));
        let mut body = vec![0u8];
        body.extend_from_slice(&99u32.to_be_bytes());
        body.extend_from_slice(b"ab");
        assert!(unframe(&body, "", 1 << 20).unwrap_err().contains("claims 99"));
    }

    #[test]
    fn grpc_message_is_percent_encoded() {
        // A raw newline in a header value would frame-inject.
        assert_eq!(percent_encode("bad\nvalue"), "bad%0Avalue");
        assert_eq!(percent_encode("100%"), "100%25");
        assert_eq!(percent_encode("plain text"), "plain text");
    }

    // --- end-to-end over a real socket, speaking raw HTTP/2 -----------------

    fn encode_one_log(body: &str, severity: u64) -> Vec<u8> {
        fn varint(out: &mut Vec<u8>, mut v: u64) {
            loop {
                let b = (v & 0x7f) as u8;
                v >>= 7;
                if v == 0 {
                    out.push(b);
                    return;
                }
                out.push(b | 0x80);
            }
        }
        fn wrap(field: u32, payload: &[u8]) -> Vec<u8> {
            let mut out = Vec::new();
            varint(&mut out, (u64::from(field) << 3) | 2);
            varint(&mut out, payload.len() as u64);
            out.extend_from_slice(payload);
            out
        }
        let body_msg = wrap(1, body.as_bytes());
        let mut rec = wrap(5, &body_msg);
        varint(&mut rec, 2 << 3);
        varint(&mut rec, severity);
        let scope = wrap(2, &rec);
        let resource = wrap(2, &scope);
        wrap(1, &resource)
    }

    struct Client {
        stream: TcpStream,
        encoder: fluke_hpack::Encoder<'static>,
        decoder: fluke_hpack::Decoder<'static>,
    }

    impl Client {
        fn connect(addr: SocketAddr) -> Self {
            let stream = TcpStream::connect(addr).unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut client = Self {
                stream,
                encoder: fluke_hpack::Encoder::new(),
                decoder: fluke_hpack::Decoder::new(),
            };
            client.stream.write_all(h2::PREFACE).unwrap();
            h2::write_settings(&mut client.stream, &[]).unwrap();
            client
        }

        fn export(&mut self, stream_id: u32, path: &str, message: &[u8]) -> Vec<(String, String)> {
            let headers: Vec<(String, String)> = vec![
                (":method".into(), "POST".into()),
                (":scheme".into(), "http".into()),
                (":path".into(), path.into()),
                (":authority".into(), "localhost".into()),
                ("content-type".into(), "application/grpc".into()),
                ("te".into(), "trailers".into()),
            ];
            let pairs: Vec<(&[u8], &[u8])> =
                headers.iter().map(|(k, v)| (k.as_bytes(), v.as_bytes())).collect();
            let block = self.encoder.encode(pairs);
            h2::write_frame(&mut self.stream, kind::HEADERS, flag::END_HEADERS, stream_id, &block)
                .unwrap();

            let mut framed = vec![0u8];
            framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
            framed.extend_from_slice(message);
            // Chunked because a DATA frame may not exceed the negotiated
            // max_frame_size — a real client honours this, and a server that
            // let it slide would be lying about its own SETTINGS.
            let chunks: Vec<&[u8]> = framed.chunks(h2::DEFAULT_MAX_FRAME_SIZE).collect();
            for (i, chunk) in chunks.iter().enumerate() {
                let flags = if i == chunks.len() - 1 { flag::END_STREAM } else { 0 };
                h2::write_frame(&mut self.stream, kind::DATA, flags, stream_id, chunk).unwrap();
            }

            self.collect_response(stream_id)
        }

        /// Reads frames until the server ends the stream, returning every
        /// header it sent (initial and trailers, in order).
        fn collect_response(&mut self, stream_id: u32) -> Vec<(String, String)> {
            let mut out = Vec::new();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                let header = h2::read_frame_header(&mut self.stream).unwrap();
                let payload =
                    h2::read_payload(&mut self.stream, &header, h2::DEFAULT_MAX_FRAME_SIZE).unwrap();
                match header.kind {
                    kind::SETTINGS if !header.has(flag::ACK) => {
                        h2::write_frame(&mut self.stream, kind::SETTINGS, flag::ACK, 0, &[])
                            .unwrap();
                    }
                    kind::HEADERS if header.stream_id == stream_id => {
                        let fragment =
                            h2::strip_padding(&payload, header.flags, true).unwrap().to_vec();
                        for (name, value) in self.decoder.decode(&fragment).unwrap() {
                            out.push((
                                String::from_utf8_lossy(&name).into_owned(),
                                String::from_utf8_lossy(&value).into_owned(),
                            ));
                        }
                        if header.has(flag::END_STREAM) {
                            return out;
                        }
                    }
                    kind::GOAWAY => {
                        let code = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                        let debug = String::from_utf8_lossy(&payload[8..]).into_owned();
                        panic!("server sent GOAWAY code={code} debug={debug}");
                    }
                    _ => {}
                }
            }
            panic!("server never ended stream {stream_id}");
        }
    }

    struct Harness {
        receiver: Option<Receiver>,
        seen: Arc<Mutex<Vec<LogRecord>>>,
    }

    impl Harness {
        fn start(accept: bool) -> Self {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let sink_seen = Arc::clone(&seen);
            let config = GrpcConfig {
                enabled: true,
                addr: "127.0.0.1:0".into(),
                ..Default::default()
            };
            let receiver = Receiver::start(&config, move |records| {
                if !accept {
                    return Accepted::Rejected;
                }
                sink_seen.lock().unwrap().extend(records);
                Accepted::All
            })
            .unwrap();
            Self { receiver: Some(receiver), seen }
        }

        fn addr(&self) -> SocketAddr {
            self.receiver.as_ref().unwrap().local_addr()
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            if let Some(r) = self.receiver.take() {
                r.shutdown();
            }
        }
    }

    fn find<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    #[test]
    fn serves_a_unary_export() {
        let h = Harness::start(true);
        let mut client = Client::connect(h.addr());
        let headers = client.export(1, EXPORT_PATH, &encode_one_log("disk full", 17));
        assert_eq!(find(&headers, ":status"), Some("200"));
        assert_eq!(find(&headers, "grpc-status"), Some("0"));
        let seen = h.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].body, "disk full");
    }

    #[test]
    fn several_streams_share_one_connection() {
        // gRPC clients multiplex; a server that handles one stream per
        // connection would deadlock a real exporter.
        let h = Harness::start(true);
        let mut client = Client::connect(h.addr());
        for (n, id) in [1u32, 3, 5, 7].iter().enumerate() {
            let headers = client.export(*id, EXPORT_PATH, &encode_one_log(&format!("m{n}"), 9));
            assert_eq!(find(&headers, "grpc-status"), Some("0"), "stream {id}: {headers:?}");
        }
        assert_eq!(h.seen.lock().unwrap().len(), 4);
    }

    #[test]
    fn an_unknown_method_is_unimplemented_not_a_404() {
        // gRPC carries the error in trailers; an HTTP status here would make
        // the client report something unrelated.
        let h = Harness::start(true);
        let mut client = Client::connect(h.addr());
        let headers = client.export(1, "/some.other.Service/Method", &encode_one_log("x", 9));
        assert_eq!(find(&headers, ":status"), Some("200"));
        assert_eq!(find(&headers, "grpc-status"), Some("12"));
        assert!(h.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn malformed_protobuf_is_invalid_argument() {
        let h = Harness::start(true);
        let mut client = Client::connect(h.addr());
        let headers = client.export(1, EXPORT_PATH, &[0xff; 12]);
        assert_eq!(find(&headers, "grpc-status"), Some("3"));
        assert!(find(&headers, "grpc-message").unwrap().contains("malformed"));
    }

    #[test]
    fn a_full_pipeline_is_unavailable_so_the_exporter_retries() {
        let h = Harness::start(false);
        let mut client = Client::connect(h.addr());
        let headers = client.export(1, EXPORT_PATH, &encode_one_log("x", 9));
        assert_eq!(find(&headers, "grpc-status"), Some("14"));
        assert_eq!(h.receiver.as_ref().unwrap().stats().rejected, 1);
    }

    #[test]
    fn a_batch_larger_than_one_frame_is_reassembled() {
        // 16 KiB max frame size means any real OTLP batch spans several DATA
        // frames, and the connection window must be topped up or it stalls.
        let h = Harness::start(true);
        let mut client = Client::connect(h.addr());
        let big = "x".repeat(200_000);
        let message = encode_one_log(&big, 9);
        assert!(message.len() > 10 * h2::DEFAULT_MAX_FRAME_SIZE);

        let headers: Vec<(String, String)> = vec![
            (":method".into(), "POST".into()),
            (":scheme".into(), "http".into()),
            (":path".into(), EXPORT_PATH.into()),
            ("content-type".into(), "application/grpc".into()),
            ("te".into(), "trailers".into()),
        ];
        let pairs: Vec<(&[u8], &[u8])> =
            headers.iter().map(|(k, v)| (k.as_bytes(), v.as_bytes())).collect();
        let block = client.encoder.encode(pairs);
        h2::write_frame(&mut client.stream, kind::HEADERS, flag::END_HEADERS, 1, &block).unwrap();

        let mut framed = vec![0u8];
        framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
        framed.extend_from_slice(&message);
        let chunks: Vec<&[u8]> = framed.chunks(h2::DEFAULT_MAX_FRAME_SIZE).collect();
        for (i, chunk) in chunks.iter().enumerate() {
            let last = i == chunks.len() - 1;
            let flags = if last { flag::END_STREAM } else { 0 };
            h2::write_frame(&mut client.stream, kind::DATA, flags, 1, chunk).unwrap();
        }
        let response = client.collect_response(1);
        assert_eq!(find(&response, "grpc-status"), Some("0"), "response {response:?}");
        assert_eq!(h.seen.lock().unwrap()[0].body.len(), 200_000);
    }

    #[test]
    fn oversized_messages_are_resource_exhausted() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink_seen = Arc::clone(&seen);
        let config = GrpcConfig {
            enabled: true,
            addr: "127.0.0.1:0".into(),
            max_message_bytes: 1024,
            ..Default::default()
        };
        let receiver = Receiver::start(&config, move |r| {
            sink_seen.lock().unwrap().extend(r);
            Accepted::All
        })
        .unwrap();
        let mut client = Client::connect(receiver.local_addr());
        let headers = client.export(1, EXPORT_PATH, &encode_one_log(&"x".repeat(50_000), 9));
        assert_eq!(find(&headers, "grpc-status"), Some("8"));
        assert!(seen.lock().unwrap().is_empty());
        receiver.shutdown();
    }

    #[test]
    fn pings_are_answered_so_keepalive_does_not_kill_the_connection() {
        // gRPC clients ping idle connections and drop them on no reply.
        let h = Harness::start(true);
        let mut client = Client::connect(h.addr());
        h2::write_frame(&mut client.stream, kind::PING, 0, 0, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            assert!(std::time::Instant::now() < deadline, "no PING ACK");
            let header = h2::read_frame_header(&mut client.stream).unwrap();
            let payload =
                h2::read_payload(&mut client.stream, &header, h2::DEFAULT_MAX_FRAME_SIZE).unwrap();
            if header.kind == kind::PING && header.has(flag::ACK) {
                assert_eq!(payload, vec![1, 2, 3, 4, 5, 6, 7, 8], "opaque data must echo");
                break;
            }
        }
    }

    #[test]
    fn a_non_http2_client_is_dropped_without_hanging() {
        let h = Harness::start(true);
        let mut stream = TcpStream::connect(h.addr()).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        let mut buf = Vec::new();
        // The server closes; the read must end rather than block forever.
        let _ = stream.read_to_end(&mut buf);
    }

    #[test]
    fn shutdown_stops_accepting() {
        let h = Harness::start(true);
        let addr = h.addr();
        let mut client = Client::connect(addr);
        assert_eq!(
            find(&client.export(1, EXPORT_PATH, &encode_one_log("x", 9)), "grpc-status"),
            Some("0")
        );
        let stats = h.receiver.as_ref().unwrap().stats();
        assert_eq!(stats.accepted, 1);
        drop(h); // shuts the receiver down

        // A fresh connection either fails outright or is closed without serving.
        if let Ok(mut late) = TcpStream::connect(addr) {
            late.set_read_timeout(Some(Duration::from_secs(2))).ok();
            let _ = late.write_all(h2::PREFACE);
            let mut buf = [0u8; 64];
            let read = late.read(&mut buf).unwrap_or(0);
            assert!(read == 0 || buf[..read].windows(4).any(|w| w[3] == kind::GOAWAY));
        }
    }
}
