//! Minimal protobuf wire-format reader.
//!
//! Deliberately not `prost` + `opentelemetry-proto` + `tonic`. That stack pulls
//! a code generator, a build-time `protoc` dependency and (via tonic) tokio and
//! an HTTP/2 stack into a binary whose whole pitch is that it is small enough to
//! run on every node the customer already owns. We decode exactly one message
//! tree — `ExportLogsServiceRequest` — and the wire format needed for it is
//! four wire types and a length prefix. Same judgement as rejecting embedded
//! DuckDB in `docs/architecture.md`: take the 300 lines, not the 30 MB.
//!
//! Only what OTLP logs actually use is implemented. Group wire types (3 and 4)
//! are deprecated and never emitted by any OTLP producer; they are rejected
//! rather than skipped, because silently skipping an unknown frame would
//! desynchronise the reader and turn a malformed payload into wrong data.

/// Protobuf decode failure. Always caused by input, never by us, so callers
/// map it to 400 rather than 500.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ProtoError {
    #[error("truncated: wanted {wanted} bytes at offset {at}, {left} left")]
    Truncated { at: usize, wanted: usize, left: usize },
    #[error("varint at offset {at} exceeds 64 bits")]
    VarintOverflow { at: usize },
    #[error("unsupported wire type {wire_type} for field {field} at offset {at}")]
    UnsupportedWireType { at: usize, field: u32, wire_type: u8 },
    #[error("field number 0 is not valid (offset {at})")]
    ZeroFieldNumber { at: usize },
    #[error("nested message deeper than {limit} levels")]
    TooDeep { limit: usize },
    #[error("invalid utf-8 in string field at offset {at}")]
    InvalidUtf8 { at: usize },
}

/// Guards against a hostile payload of nothing but nested length-delimited
/// headers, which would otherwise recurse until the stack dies. OTLP logs nest
/// at most six levels (request → resource_logs → scope_logs → log_records →
/// attributes → value → array element).
const MAX_DEPTH: usize = 16;

#[derive(Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    depth: usize,
}

/// One decoded field header. The payload stays in the buffer; the caller
/// decides how to interpret it, which is what keeps this allocation-free for
/// everything except strings.
#[derive(Debug)]
pub enum Field<'a> {
    Varint(u64),
    Fixed64(u64),
    Fixed32(u32),
    Bytes(&'a [u8]),
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0, depth: 0 }
    }

    fn nested(buf: &'a [u8], depth: usize) -> Self {
        Self { buf, pos: 0, depth }
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    /// Reads the next (field number, value) pair, or `None` at end of buffer.
    pub fn next_field(&mut self) -> Result<Option<(u32, Field<'a>)>, ProtoError> {
        if self.is_empty() {
            return Ok(None);
        }
        let at = self.pos;
        let key = self.varint()?;
        let field = (key >> 3) as u32;
        let wire_type = (key & 0x7) as u8;
        if field == 0 {
            return Err(ProtoError::ZeroFieldNumber { at });
        }
        let value = match wire_type {
            0 => Field::Varint(self.varint()?),
            1 => Field::Fixed64(u64::from_le_bytes(self.array::<8>()?)),
            2 => {
                let len = self.varint()? as usize;
                Field::Bytes(self.take(len)?)
            }
            5 => Field::Fixed32(u32::from_le_bytes(self.array::<4>()?)),
            other => {
                return Err(ProtoError::UnsupportedWireType { at, field, wire_type: other })
            }
        };
        Ok(Some((field, value)))
    }

    fn varint(&mut self) -> Result<u64, ProtoError> {
        let at = self.pos;
        let mut value: u64 = 0;
        for shift in (0..64).step_by(7) {
            let byte = *self
                .buf
                .get(self.pos)
                .ok_or(ProtoError::Truncated { at, wanted: 1, left: 0 })?;
            self.pos += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(ProtoError::VarintOverflow { at })
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ProtoError> {
        let left = self.buf.len() - self.pos;
        if len > left {
            return Err(ProtoError::Truncated { at: self.pos, wanted: len, left });
        }
        let out = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(out)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ProtoError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }
}

impl<'a> Field<'a> {
    /// Length-delimited payload as a sub-message reader.
    pub fn message(&self, parent: &Reader<'a>) -> Result<Reader<'a>, ProtoError> {
        let depth = parent.depth + 1;
        if depth > MAX_DEPTH {
            return Err(ProtoError::TooDeep { limit: MAX_DEPTH });
        }
        match self {
            Field::Bytes(b) => Ok(Reader::nested(b, depth)),
            _ => Ok(Reader::nested(&[], depth)),
        }
    }

    pub fn bytes(&self) -> &'a [u8] {
        match self {
            Field::Bytes(b) => b,
            _ => &[],
        }
    }

    /// UTF-8 string. Invalid UTF-8 is an error rather than a lossy conversion:
    /// a body that silently becomes replacement characters would be templated
    /// and fingerprinted on corrupted text, and the corruption would then be
    /// permanent in the store.
    pub fn string(&self) -> Result<&'a str, ProtoError> {
        match self {
            Field::Bytes(b) => {
                std::str::from_utf8(b).map_err(|_| ProtoError::InvalidUtf8 { at: 0 })
            }
            _ => Ok(""),
        }
    }

    pub fn as_u64(&self) -> u64 {
        match self {
            Field::Varint(v) => *v,
            Field::Fixed64(v) => *v,
            Field::Fixed32(v) => u64::from(*v),
            Field::Bytes(_) => 0,
        }
    }

    pub fn as_i64(&self) -> i64 {
        self.as_u64() as i64
    }

    pub fn as_f64(&self) -> f64 {
        match self {
            Field::Fixed64(v) => f64::from_bits(*v),
            Field::Fixed32(v) => f64::from(f32::from_bits(*v)),
            other => other.as_u64() as f64,
        }
    }

    pub fn as_bool(&self) -> bool {
        self.as_u64() != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encoders, test-only — we never produce OTLP, only consume it.
    pub fn varint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }

    pub fn tag(out: &mut Vec<u8>, field: u32, wire: u8) {
        varint(out, (u64::from(field) << 3) | u64::from(wire));
    }

    pub fn bytes_field(out: &mut Vec<u8>, field: u32, payload: &[u8]) {
        tag(out, field, 2);
        varint(out, payload.len() as u64);
        out.extend_from_slice(payload);
    }

    #[test]
    fn reads_each_wire_type() {
        let mut buf = Vec::new();
        tag(&mut buf, 1, 0);
        varint(&mut buf, 300);
        tag(&mut buf, 2, 1);
        buf.extend_from_slice(&7u64.to_le_bytes());
        bytes_field(&mut buf, 3, b"hello");
        tag(&mut buf, 4, 5);
        buf.extend_from_slice(&9u32.to_le_bytes());

        let mut r = Reader::new(&buf);
        let (f, v) = r.next_field().unwrap().unwrap();
        assert_eq!((f, v.as_u64()), (1, 300));
        let (f, v) = r.next_field().unwrap().unwrap();
        assert_eq!((f, v.as_u64()), (2, 7));
        let (f, v) = r.next_field().unwrap().unwrap();
        assert_eq!((f, v.string().unwrap()), (3, "hello"));
        let (f, v) = r.next_field().unwrap().unwrap();
        assert_eq!((f, v.as_u64()), (4, 9));
        assert!(r.next_field().unwrap().is_none());
    }

    #[test]
    fn unknown_fields_are_skipped_not_fatal() {
        // Forward compatibility is the whole point of protobuf: a newer
        // collector adding a field must not break ingest.
        let mut buf = Vec::new();
        bytes_field(&mut buf, 99, b"from the future");
        tag(&mut buf, 1, 0);
        varint(&mut buf, 42);
        let mut r = Reader::new(&buf);
        let mut seen = Vec::new();
        while let Some((f, v)) = r.next_field().unwrap() {
            seen.push((f, v.as_u64()));
        }
        assert_eq!(seen, vec![(99, 0), (1, 42)]);
    }

    #[test]
    fn truncated_length_prefix_is_an_error() {
        let mut buf = Vec::new();
        tag(&mut buf, 1, 2);
        varint(&mut buf, 100); // claims 100 bytes, supplies 2
        buf.extend_from_slice(b"ab");
        let err = Reader::new(&buf).next_field().unwrap_err();
        assert!(matches!(err, ProtoError::Truncated { wanted: 100, left: 2, .. }));
    }

    #[test]
    fn oversized_varint_is_rejected() {
        let buf = vec![0xffu8; 12]; // continuation bit set forever
        let err = Reader::new(&buf).next_field().unwrap_err();
        assert!(matches!(err, ProtoError::VarintOverflow { .. }));
    }

    #[test]
    fn group_wire_types_are_rejected_not_skipped() {
        // Skipping wire type 3 would desynchronise the reader and produce
        // plausible-looking garbage; refusing the payload is the safe answer.
        let mut buf = Vec::new();
        tag(&mut buf, 1, 3);
        let err = Reader::new(&buf).next_field().unwrap_err();
        assert!(matches!(err, ProtoError::UnsupportedWireType { wire_type: 3, .. }));
    }

    #[test]
    fn zero_field_number_is_rejected() {
        let buf = vec![0x00u8, 0x00];
        let err = Reader::new(&buf).next_field().unwrap_err();
        assert!(matches!(err, ProtoError::ZeroFieldNumber { .. }));
    }

    #[test]
    fn nesting_is_bounded() {
        let mut r = Reader::new(&[]);
        r.depth = MAX_DEPTH;
        let err = Field::Bytes(b"x").message(&r).unwrap_err();
        assert_eq!(err, ProtoError::TooDeep { limit: MAX_DEPTH });
    }

    #[test]
    fn invalid_utf8_is_an_error_not_lossy() {
        let f = Field::Bytes(&[0xff, 0xfe]);
        assert!(matches!(f.string(), Err(ProtoError::InvalidUtf8 { .. })));
    }

    #[test]
    fn empty_buffer_yields_no_fields() {
        assert!(Reader::new(&[]).next_field().unwrap().is_none());
    }
}
