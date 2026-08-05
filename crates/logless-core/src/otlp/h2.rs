//! The slice of HTTP/2 that gRPC unary calls need.
//!
//! Not a general HTTP/2 implementation and not trying to be. A gRPC server that
//! only answers `ExportLogsServiceRequest` needs: the connection preface, seven
//! frame types, connection and stream flow control, and HPACK. It does not need
//! server push, priority scheduling, or `:protocol` extended CONNECT — those are
//! parsed enough to be ignored safely and no further.
//!
//! HPACK itself comes from `fluke-hpack`. Everything else here is hand-written
//! for the same reason the protobuf reader is (`otlp/proto.rs`), but HPACK is
//! where that reasoning stops: its Huffman table is 257 canonical codes from
//! RFC 7541 that cannot be derived, only transcribed, and a single wrong entry
//! would corrupt header values silently rather than failing.

use std::io::{Read, Write};

/// Client connection preface. A client that does not send this exactly is not
/// speaking HTTP/2, and the only safe response is to close.
pub const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Smallest legal `SETTINGS_MAX_FRAME_SIZE`, and what we write with. Our
/// responses are a few hundred bytes, so there is nothing to gain by
/// negotiating higher.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16_384;

/// Flow-control window a connection starts with, fixed by the RFC — a peer's
/// `SETTINGS_INITIAL_WINDOW_SIZE` applies to streams only. A server that never
/// sends `WINDOW_UPDATE` therefore stalls forever at 64 KiB of request body,
/// which is well under one OTLP batch.
pub const INITIAL_CONNECTION_WINDOW: i64 = 65_535;

pub mod kind {
    pub const DATA: u8 = 0x0;
    pub const HEADERS: u8 = 0x1;
    pub const PRIORITY: u8 = 0x2;
    pub const RST_STREAM: u8 = 0x3;
    pub const SETTINGS: u8 = 0x4;
    pub const PUSH_PROMISE: u8 = 0x5;
    pub const PING: u8 = 0x6;
    pub const GOAWAY: u8 = 0x7;
    pub const WINDOW_UPDATE: u8 = 0x8;
    pub const CONTINUATION: u8 = 0x9;
}

pub mod flag {
    pub const END_STREAM: u8 = 0x1;
    /// Same bit as `END_STREAM`; meaningful on SETTINGS and PING.
    pub const ACK: u8 = 0x1;
    pub const END_HEADERS: u8 = 0x4;
    pub const PADDED: u8 = 0x8;
    pub const PRIORITY: u8 = 0x20;
}

pub mod setting {
    pub const HEADER_TABLE_SIZE: u16 = 0x1;
    pub const ENABLE_PUSH: u16 = 0x2;
    pub const MAX_CONCURRENT_STREAMS: u16 = 0x3;
    pub const INITIAL_WINDOW_SIZE: u16 = 0x4;
    pub const MAX_FRAME_SIZE: u16 = 0x5;
    pub const MAX_HEADER_LIST_SIZE: u16 = 0x6;
}

pub mod error_code {
    pub const NO_ERROR: u32 = 0x0;
    pub const PROTOCOL_ERROR: u32 = 0x1;
    pub const INTERNAL_ERROR: u32 = 0x2;
    pub const FLOW_CONTROL_ERROR: u32 = 0x3;
    pub const FRAME_SIZE_ERROR: u32 = 0x6;
    pub const REFUSED_STREAM: u32 = 0x7;
    pub const COMPRESSION_ERROR: u32 = 0x9;
    pub const ENHANCE_YOUR_CALM: u32 = 0xb;
}

#[derive(Debug, thiserror::Error)]
pub enum H2Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("not an HTTP/2 connection preface")]
    BadPreface,
    /// Anything the peer did that the RFC says must terminate the connection.
    #[error("protocol error ({code}): {reason}")]
    Protocol { code: u32, reason: String },
}

impl H2Error {
    pub fn protocol(code: u32, reason: impl Into<String>) -> Self {
        Self::Protocol { code, reason: reason.into() }
    }

    pub fn code(&self) -> u32 {
        match self {
            H2Error::Protocol { code, .. } => *code,
            H2Error::BadPreface => error_code::PROTOCOL_ERROR,
            H2Error::Io(_) => error_code::INTERNAL_ERROR,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub length: u32,
    pub kind: u8,
    pub flags: u8,
    pub stream_id: u32,
}

impl FrameHeader {
    pub fn has(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

pub fn read_preface(reader: &mut impl Read) -> Result<(), H2Error> {
    let mut buf = [0u8; PREFACE.len()];
    reader.read_exact(&mut buf)?;
    if buf != PREFACE {
        return Err(H2Error::BadPreface);
    }
    Ok(())
}

pub fn read_frame_header(reader: &mut impl Read) -> Result<FrameHeader, H2Error> {
    let mut head = [0u8; 9];
    reader.read_exact(&mut head)?;
    Ok(parse_frame_header(&head))
}

/// Splits the nine-byte frame header. Separate from the read so a caller that
/// has to do its own timeout-tolerant read can still share the parsing.
pub fn parse_frame_header(head: &[u8; 9]) -> FrameHeader {
    FrameHeader {
        length: u32::from_be_bytes([0, head[0], head[1], head[2]]),
        kind: head[3],
        flags: head[4],
        // Top bit is the reserved bit, and the RFC says to ignore it rather
        // than treat it as part of the id.
        stream_id: u32::from_be_bytes([head[5], head[6], head[7], head[8]]) & 0x7fff_ffff,
    }
}

/// Reads a frame payload, refusing anything over `max_frame_size` before
/// allocating for it.
pub fn read_payload(
    reader: &mut impl Read,
    header: &FrameHeader,
    max_frame_size: usize,
) -> Result<Vec<u8>, H2Error> {
    if header.length as usize > max_frame_size {
        return Err(H2Error::protocol(
            error_code::FRAME_SIZE_ERROR,
            format!("frame of {} bytes exceeds max_frame_size", header.length),
        ));
    }
    let mut buf = vec![0u8; header.length as usize];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn write_frame(
    writer: &mut impl Write,
    kind: u8,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> Result<(), H2Error> {
    let len = payload.len();
    let head = [
        (len >> 16) as u8,
        (len >> 8) as u8,
        len as u8,
        kind,
        flags,
        (stream_id >> 24) as u8,
        (stream_id >> 16) as u8,
        (stream_id >> 8) as u8,
        stream_id as u8,
    ];
    writer.write_all(&head)?;
    writer.write_all(payload)?;
    Ok(())
}

pub fn write_settings(writer: &mut impl Write, settings: &[(u16, u32)]) -> Result<(), H2Error> {
    let mut payload = Vec::with_capacity(settings.len() * 6);
    for (id, value) in settings {
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(&value.to_be_bytes());
    }
    write_frame(writer, kind::SETTINGS, 0, 0, &payload)
}

pub fn write_window_update(
    writer: &mut impl Write,
    stream_id: u32,
    increment: u32,
) -> Result<(), H2Error> {
    write_frame(writer, kind::WINDOW_UPDATE, 0, stream_id, &increment.to_be_bytes())
}

pub fn write_goaway(
    writer: &mut impl Write,
    last_stream_id: u32,
    code: u32,
    debug: &str,
) -> Result<(), H2Error> {
    let mut payload = Vec::with_capacity(8 + debug.len());
    payload.extend_from_slice(&last_stream_id.to_be_bytes());
    payload.extend_from_slice(&code.to_be_bytes());
    payload.extend_from_slice(debug.as_bytes());
    write_frame(writer, kind::GOAWAY, 0, 0, &payload)
}

pub fn write_rst_stream(
    writer: &mut impl Write,
    stream_id: u32,
    code: u32,
) -> Result<(), H2Error> {
    write_frame(writer, kind::RST_STREAM, 0, stream_id, &code.to_be_bytes())
}

/// Strips padding and the priority block, leaving the fragment that matters.
///
/// Getting this wrong is not a parse error but a data error: the padding length
/// byte counts *itself* out of the payload, so an off-by-one feeds pad bytes
/// into HPACK, where they decode as garbage headers rather than failing.
pub fn strip_padding(payload: &[u8], flags: u8, has_priority_field: bool) -> Result<&[u8], H2Error> {
    let mut body = payload;
    let mut pad_len = 0usize;
    if flags & flag::PADDED != 0 {
        let (first, rest) = body
            .split_first()
            .ok_or_else(|| H2Error::protocol(error_code::PROTOCOL_ERROR, "padded frame is empty"))?;
        pad_len = *first as usize;
        body = rest;
    }
    if has_priority_field && flags & flag::PRIORITY != 0 {
        if body.len() < 5 {
            return Err(H2Error::protocol(
                error_code::PROTOCOL_ERROR,
                "priority block truncated",
            ));
        }
        body = &body[5..];
    }
    if pad_len > body.len() {
        return Err(H2Error::protocol(
            error_code::PROTOCOL_ERROR,
            "padding longer than the frame",
        ));
    }
    Ok(&body[..body.len() - pad_len])
}

/// Parses a SETTINGS payload into (id, value) pairs.
pub fn parse_settings(payload: &[u8]) -> Result<Vec<(u16, u32)>, H2Error> {
    if payload.len() % 6 != 0 {
        return Err(H2Error::protocol(
            error_code::FRAME_SIZE_ERROR,
            "SETTINGS payload is not a multiple of 6",
        ));
    }
    Ok(payload
        .chunks_exact(6)
        .map(|c| {
            (
                u16::from_be_bytes([c[0], c[1]]),
                u32::from_be_bytes([c[2], c[3], c[4], c[5]]),
            )
        })
        .collect())
}

pub fn parse_window_update(payload: &[u8]) -> Result<u32, H2Error> {
    if payload.len() != 4 {
        return Err(H2Error::protocol(
            error_code::FRAME_SIZE_ERROR,
            "WINDOW_UPDATE payload must be 4 bytes",
        ));
    }
    Ok(u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_headers_round_trip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, kind::DATA, flag::END_STREAM, 5, b"hello").unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let header = read_frame_header(&mut cursor).unwrap();
        assert_eq!(
            header,
            FrameHeader { length: 5, kind: kind::DATA, flags: flag::END_STREAM, stream_id: 5 }
        );
        assert_eq!(read_payload(&mut cursor, &header, 16384).unwrap(), b"hello");
    }

    #[test]
    fn the_reserved_bit_is_ignored_not_read_as_the_stream_id() {
        let mut buf = vec![0, 0, 0, kind::DATA, 0];
        buf.extend_from_slice(&(0x8000_0001u32).to_be_bytes()); // reserved bit set
        let header = read_frame_header(&mut std::io::Cursor::new(buf)).unwrap();
        assert_eq!(header.stream_id, 1);
    }

    #[test]
    fn oversized_frames_are_refused_before_allocation() {
        let header = FrameHeader { length: 1 << 20, kind: kind::DATA, flags: 0, stream_id: 1 };
        let err = read_payload(&mut std::io::Cursor::new(Vec::new()), &header, 16384).unwrap_err();
        assert_eq!(err.code(), error_code::FRAME_SIZE_ERROR);
    }

    #[test]
    fn padding_is_stripped_including_its_own_length_byte() {
        // 1 length byte + 3 data + 2 pad.
        let payload = [2u8, b'a', b'b', b'c', 0, 0];
        assert_eq!(strip_padding(&payload, flag::PADDED, false).unwrap(), b"abc");
    }

    #[test]
    fn the_priority_block_is_stripped_from_headers() {
        let mut payload = vec![0u8; 5]; // stream dependency + weight
        payload.extend_from_slice(b"block");
        assert_eq!(strip_padding(&payload, flag::PRIORITY, true).unwrap(), b"block");
    }

    #[test]
    fn padding_and_priority_combine() {
        let mut payload = vec![2u8]; // pad length
        payload.extend_from_slice(&[0u8; 5]); // priority
        payload.extend_from_slice(b"block");
        payload.extend_from_slice(&[0, 0]); // padding
        let flags = flag::PADDED | flag::PRIORITY;
        assert_eq!(strip_padding(&payload, flags, true).unwrap(), b"block");
    }

    #[test]
    fn padding_longer_than_the_frame_is_a_protocol_error() {
        // Would otherwise underflow into a panic or a huge slice.
        let payload = [200u8, b'a'];
        let err = strip_padding(&payload, flag::PADDED, false).unwrap_err();
        assert_eq!(err.code(), error_code::PROTOCOL_ERROR);
    }

    #[test]
    fn settings_parse_in_pairs_and_reject_ragged_payloads() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&setting::MAX_FRAME_SIZE.to_be_bytes());
        payload.extend_from_slice(&16384u32.to_be_bytes());
        assert_eq!(parse_settings(&payload).unwrap(), vec![(setting::MAX_FRAME_SIZE, 16384)]);
        assert_eq!(
            parse_settings(&payload[..5]).unwrap_err().code(),
            error_code::FRAME_SIZE_ERROR
        );
    }

    #[test]
    fn window_update_masks_the_reserved_bit() {
        assert_eq!(parse_window_update(&0x8000_1234u32.to_be_bytes()).unwrap(), 0x1234);
        assert_eq!(parse_window_update(&[0, 0, 0]).unwrap_err().code(), error_code::FRAME_SIZE_ERROR);
    }

    #[test]
    fn a_wrong_preface_is_rejected() {
        let mut cursor = std::io::Cursor::new(b"GET / HTTP/1.1\r\n\r\nnot h2".to_vec());
        assert!(matches!(read_preface(&mut cursor), Err(H2Error::BadPreface)));
    }
}
