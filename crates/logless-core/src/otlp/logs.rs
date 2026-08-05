//! `ExportLogsServiceRequest` → [`LogRecord`].
//!
//! Field numbers are from `opentelemetry/proto/logs/v1/logs.proto` and
//! `common/v1/common.proto` (OTLP 1.x). They are frozen by the protobuf
//! compatibility guarantee, so hard-coding them is safe in a way that
//! hard-coding, say, a JSON shape would not be.

use super::proto::{ProtoError, Reader};
use crate::model::{Attr, AttrValue, LogRecord, Severity};
use uuid::Uuid;

/// Severity used when the producer sets neither `severity_number` nor a
/// recognisable `severity_text`.
///
/// The spec's own default is 0 ("unspecified"), but honouring that would put
/// every record from a producer that omits severity into the `debug` bucket —
/// shed first under memory pressure, deleted first under disk pressure, and
/// retained for a day. A fleet that never sets severity would quietly lose all
/// its logs. Defaulting to INFO is wrong in the other direction (we keep more
/// than asked) and that is the cheaper error.
const DEFAULT_SEVERITY: Severity = Severity::INFO;

/// Everything decoded from one export request.
#[derive(Debug, Default)]
pub struct Decoded {
    pub records: Vec<LogRecord>,
    /// Records the producer itself says it dropped before sending. Surfaced in
    /// stats so a gap in the store can be attributed upstream rather than to us.
    pub dropped_upstream: u64,
}

/// Decodes an OTLP logs export request.
///
/// `now_unix_nano` fills in `observed_unix_nano` for producers that leave it
/// zero, so partitioning always has a real timestamp to work from.
pub fn decode_request(buf: &[u8], now_unix_nano: u64) -> Result<Decoded, ProtoError> {
    let mut out = Decoded::default();
    let mut r = Reader::new(buf);
    while let Some((field, value)) = r.next_field()? {
        if field == 1 {
            // resource_logs
            resource_logs(&mut value.message(&r)?, now_unix_nano, &mut out)?;
        }
    }
    Ok(out)
}

fn resource_logs(r: &mut Reader, now: u64, out: &mut Decoded) -> Result<(), ProtoError> {
    // Resource attributes are copied onto every record. The Parquet schema is
    // flat by design (§ `schema.rs`) — there is no resource side-table — so not
    // copying them would discard `host.name`, `k8s.pod.name` and friends
    // entirely. Dictionary encoding makes the repetition nearly free on disk.
    let mut resource_attrs: Vec<Attr> = Vec::new();
    let mut service: Option<String> = None;
    let mut pending_scopes: Vec<Reader> = Vec::new();

    while let Some((field, value)) = r.next_field()? {
        match field {
            1 => {
                let mut res = value.message(r)?;
                while let Some((f, v)) = res.next_field()? {
                    match f {
                        1 => {
                            if let Some(attr) = key_value(&mut v.message(&res)?)? {
                                if attr.key == "service.name" {
                                    if let AttrValue::Str(s) = &attr.value {
                                        service = Some(s.clone());
                                    }
                                }
                                resource_attrs.push(attr);
                            }
                        }
                        2 => out.dropped_upstream += v.as_u64(),
                        _ => {}
                    }
                }
            }
            // Scope blocks may precede the resource block on the wire (protobuf
            // does not guarantee field order), so collect them and apply the
            // resource once it is fully known.
            2 => pending_scopes.push(value.message(r)?),
            _ => {}
        }
    }

    for mut scope in pending_scopes {
        scope_logs(&mut scope, now, &resource_attrs, service.as_deref(), out)?;
    }
    Ok(())
}

fn scope_logs(
    r: &mut Reader,
    now: u64,
    resource_attrs: &[Attr],
    service: Option<&str>,
    out: &mut Decoded,
) -> Result<(), ProtoError> {
    let mut scope_name: Option<String> = None;
    let mut pending: Vec<Reader> = Vec::new();

    while let Some((field, value)) = r.next_field()? {
        match field {
            1 => {
                let mut scope = value.message(r)?;
                while let Some((f, v)) = scope.next_field()? {
                    if f == 1 {
                        let name = v.string()?;
                        if !name.is_empty() {
                            scope_name = Some(name.to_string());
                        }
                    }
                }
            }
            2 => pending.push(value.message(r)?),
            _ => {}
        }
    }

    for mut rec in pending {
        let mut record = log_record(&mut rec, now, out)?;
        record.service = service.map(str::to_string);
        if let Some(name) = &scope_name {
            record.attributes.push(Attr {
                key: "otel.scope.name".to_string(),
                value: AttrValue::Str(name.clone()),
            });
        }
        record.attributes.extend_from_slice(resource_attrs);
        out.records.push(record);
    }
    Ok(())
}

fn log_record(r: &mut Reader, now: u64, out: &mut Decoded) -> Result<LogRecord, ProtoError> {
    let mut time = 0u64;
    let mut observed = 0u64;
    let mut severity_number = 0u64;
    let mut severity_text: Option<String> = None;
    let mut body = String::new();
    let mut attributes = Vec::new();
    let mut trace_id: Option<[u8; 16]> = None;
    let mut span_id: Option<[u8; 8]> = None;
    let mut event_name: Option<String> = None;

    while let Some((field, value)) = r.next_field()? {
        match field {
            1 => time = value.as_u64(),
            2 => severity_number = value.as_u64(),
            3 => {
                let text = value.string()?;
                if !text.is_empty() {
                    severity_text = Some(text.to_string());
                }
            }
            5 => body = any_value(&mut value.message(r)?)?.render(),
            6 => {
                if let Some(attr) = key_value(&mut value.message(r)?)? {
                    attributes.push(attr);
                }
            }
            7 => out.dropped_upstream += value.as_u64(),
            9 => trace_id = fixed_id::<16>(value.bytes()),
            10 => span_id = fixed_id::<8>(value.bytes()),
            11 => observed = value.as_u64(),
            12 => {
                let name = value.string()?;
                if !name.is_empty() {
                    event_name = Some(name.to_string());
                }
            }
            _ => {}
        }
    }

    let severity = match (severity_number, severity_text.as_deref()) {
        (0, Some(text)) => Severity::from_text(text).unwrap_or(DEFAULT_SEVERITY),
        (0, None) => DEFAULT_SEVERITY,
        // Clamp rather than reject: the field is an open enum, and a record with
        // an out-of-range severity is still worth keeping.
        (n, _) => Severity(n.min(24) as u8),
    };

    if let Some(name) = event_name {
        attributes.push(Attr { key: "event.name".to_string(), value: AttrValue::Str(name) });
    }

    // A record with no timestamp still has to land in a partition. Falling back
    // to receive time keeps it queryable; falling back to 0 would file it under
    // 1970 where retention deletes it on the next sweep.
    let observed = if observed != 0 { observed } else { now };
    let time = if time != 0 { time } else { observed };

    Ok(LogRecord {
        event_id: Uuid::now_v7(),
        timestamp_unix_nano: time,
        observed_unix_nano: observed,
        severity,
        severity_text,
        body,
        service: None,
        trace_id,
        span_id,
        attributes,
        template_id: None,
    })
}

/// An all-zero id means "unset" in the OTel spec, not "id zero".
fn fixed_id<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    if bytes.len() != N || bytes.iter().all(|b| *b == 0) {
        return None;
    }
    let mut out = [0u8; N];
    out.copy_from_slice(bytes);
    Some(out)
}

fn key_value(r: &mut Reader) -> Result<Option<Attr>, ProtoError> {
    let mut key = String::new();
    let mut value = Value::Empty;
    while let Some((field, v)) = r.next_field()? {
        match field {
            1 => key = v.string()?.to_string(),
            2 => value = any_value(&mut v.message(r)?)?,
            _ => {}
        }
    }
    if key.is_empty() {
        return Ok(None);
    }
    Ok(Some(Attr { key, value: value.into_attr() }))
}

/// Intermediate form: `AnyValue` is a oneof with structured arms our flat
/// [`AttrValue`] has no room for, so they are rendered to a string here rather
/// than leaking a nested type into the storage schema.
enum Value {
    Empty,
    Str(String),
    Bool(bool),
    I64(i64),
    F64(f64),
    Structured(String),
}

impl Value {
    fn render(self) -> String {
        match self {
            Value::Empty => String::new(),
            Value::Str(s) | Value::Structured(s) => s,
            Value::Bool(b) => b.to_string(),
            Value::I64(v) => v.to_string(),
            Value::F64(v) => v.to_string(),
        }
    }

    fn into_attr(self) -> AttrValue {
        match self {
            Value::Str(s) | Value::Structured(s) => AttrValue::Str(s),
            Value::Bool(b) => AttrValue::Bool(b),
            Value::I64(v) => AttrValue::I64(v),
            Value::F64(v) => AttrValue::F64(v),
            Value::Empty => AttrValue::Str(String::new()),
        }
    }
}

fn any_value(r: &mut Reader) -> Result<Value, ProtoError> {
    let mut out = Value::Empty;
    while let Some((field, v)) = r.next_field()? {
        out = match field {
            1 => Value::Str(v.string()?.to_string()),
            2 => Value::Bool(v.as_bool()),
            3 => Value::I64(v.as_i64()),
            4 => Value::F64(v.as_f64()),
            5 => Value::Structured(array_value(&mut v.message(r)?)?),
            6 => Value::Structured(kvlist_value(&mut v.message(r)?)?),
            7 => Value::Structured(hex(v.bytes())),
            _ => out,
        };
    }
    Ok(out)
}

fn array_value(r: &mut Reader) -> Result<String, ProtoError> {
    let mut parts = Vec::new();
    while let Some((field, v)) = r.next_field()? {
        if field == 1 {
            parts.push(any_value(&mut v.message(r)?)?.render());
        }
    }
    Ok(format!("[{}]", parts.join(",")))
}

fn kvlist_value(r: &mut Reader) -> Result<String, ProtoError> {
    let mut parts = Vec::new();
    while let Some((field, v)) = r.next_field()? {
        if field == 1 {
            if let Some(attr) = key_value(&mut v.message(r)?)? {
                let rendered = match attr.value {
                    AttrValue::Str(s) => s,
                    AttrValue::I64(v) => v.to_string(),
                    AttrValue::F64(v) => v.to_string(),
                    AttrValue::Bool(b) => b.to_string(),
                };
                parts.push(format!("{}={}", attr.key, rendered));
            }
        }
    }
    Ok(format!("{{{}}}", parts.join(" ")))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal OTLP encoder. Test-only: log-less consumes OTLP, never emits it.
    #[derive(Default)]
    struct Enc(Vec<u8>);

    impl Enc {
        fn varint(&mut self, mut v: u64) {
            loop {
                let byte = (v & 0x7f) as u8;
                v >>= 7;
                if v == 0 {
                    self.0.push(byte);
                    return;
                }
                self.0.push(byte | 0x80);
            }
        }
        fn tag(&mut self, field: u32, wire: u8) {
            self.varint((u64::from(field) << 3) | u64::from(wire));
        }
        fn uint(&mut self, field: u32, v: u64) {
            self.tag(field, 0);
            self.varint(v);
        }
        fn fixed64(&mut self, field: u32, v: u64) {
            self.tag(field, 1);
            self.0.extend_from_slice(&v.to_le_bytes());
        }
        fn bytes(&mut self, field: u32, payload: &[u8]) {
            self.tag(field, 2);
            self.varint(payload.len() as u64);
            self.0.extend_from_slice(payload);
        }
        fn string(&mut self, field: u32, s: &str) {
            self.bytes(field, s.as_bytes());
        }
        fn msg(&mut self, field: u32, build: impl FnOnce(&mut Enc)) {
            let mut inner = Enc::default();
            build(&mut inner);
            self.bytes(field, &inner.0);
        }
        fn any_string(&mut self, field: u32, s: &str) {
            self.msg(field, |v| v.string(1, s));
        }
        fn attr(&mut self, field: u32, key: &str, value: &str) {
            self.msg(field, |kv| {
                kv.string(1, key);
                kv.any_string(2, value);
            });
        }
    }

    /// One record wrapped in the full resource/scope tree.
    fn request(build: impl FnOnce(&mut Enc)) -> Vec<u8> {
        let mut e = Enc::default();
        e.msg(1, |rl| {
            rl.msg(1, |res| res.attr(1, "service.name", "checkout"));
            rl.msg(2, |sl| {
                sl.msg(1, |scope| scope.string(1, "app"));
                sl.msg(2, build);
            });
        });
        e.0
    }

    #[test]
    fn decodes_a_full_record() {
        let buf = request(|r| {
            r.fixed64(1, 1_700_000_000_000_000_000);
            r.uint(2, 17);
            r.string(3, "ERROR");
            r.msg(5, |b| b.string(1, "payment declined"));
            r.attr(6, "http.method", "POST");
            r.bytes(9, &[1u8; 16]);
            r.bytes(10, &[2u8; 8]);
            r.fixed64(11, 1_700_000_000_500_000_000);
        });

        let d = decode_request(&buf, 99).unwrap();
        assert_eq!(d.records.len(), 1);
        let rec = &d.records[0];
        assert_eq!(rec.body, "payment declined");
        assert_eq!(rec.severity, Severity::ERROR);
        assert_eq!(rec.severity_text.as_deref(), Some("ERROR"));
        assert_eq!(rec.service.as_deref(), Some("checkout"));
        assert_eq!(rec.timestamp_unix_nano, 1_700_000_000_000_000_000);
        assert_eq!(rec.observed_unix_nano, 1_700_000_000_500_000_000);
        assert_eq!(rec.trace_id, Some([1u8; 16]));
        assert_eq!(rec.span_id, Some([2u8; 8]));
        // Record attribute, then scope, then resource attributes.
        let keys: Vec<&str> = rec.attributes.iter().map(|a| a.key.as_str()).collect();
        assert_eq!(keys, ["http.method", "otel.scope.name", "service.name"]);
    }

    #[test]
    fn resource_attributes_land_on_every_record() {
        // Losing these would discard host.name / k8s.pod.name, the fields an
        // operator filters by first during an incident.
        let buf = request(|s| {
            s.msg(5, |b| b.string(1, "one"));
        });
        let d = decode_request(&buf, 1).unwrap();
        assert!(d.records[0]
            .attributes
            .iter()
            .any(|a| a.key == "service.name" && a.value == AttrValue::Str("checkout".into())));
    }

    #[test]
    fn severity_text_used_when_number_is_unset() {
        for (text, expect) in [
            ("WARN", Severity::WARN),
            ("critical", Severity::FATAL),
            ("Debug", Severity::DEBUG),
        ] {
            let buf = request(|r| {
                r.string(3, text);
                r.msg(5, |b| b.string(1, "x"));
            });
            assert_eq!(decode_request(&buf, 1).unwrap().records[0].severity, expect, "{text}");
        }
    }

    #[test]
    fn unspecified_severity_defaults_to_info_not_shed_first() {
        let buf = request(|r| r.msg(5, |b| b.string(1, "no severity at all")));
        let rec = &decode_request(&buf, 1).unwrap().records[0];
        assert_eq!(rec.severity, Severity::INFO);
        assert_eq!(rec.severity_text, None);
    }

    #[test]
    fn unrecognised_severity_text_falls_back_but_is_preserved() {
        let buf = request(|r| {
            r.string(3, "NOTABLE");
            r.msg(5, |b| b.string(1, "x"));
        });
        let rec = &decode_request(&buf, 1).unwrap().records[0];
        assert_eq!(rec.severity, Severity::INFO);
        assert_eq!(rec.severity_text.as_deref(), Some("NOTABLE"));
    }

    #[test]
    fn missing_timestamps_fall_back_to_receive_time() {
        let buf = request(|r| r.msg(5, |b| b.string(1, "x")));
        let rec = &decode_request(&buf, 4242).unwrap().records[0];
        assert_eq!(rec.observed_unix_nano, 4242);
        assert_eq!(rec.timestamp_unix_nano, 4242, "must not partition under 1970");
    }

    #[test]
    fn zero_trace_id_is_unset_not_zero() {
        let buf = request(|r| {
            r.msg(5, |b| b.string(1, "x"));
            r.bytes(9, &[0u8; 16]);
            r.bytes(10, &[0u8; 8]);
        });
        let rec = &decode_request(&buf, 1).unwrap().records[0];
        assert_eq!(rec.trace_id, None);
        assert_eq!(rec.span_id, None);
    }

    #[test]
    fn wrong_length_trace_id_is_dropped_not_padded() {
        let buf = request(|r| {
            r.msg(5, |b| b.string(1, "x"));
            r.bytes(9, &[1u8; 8]);
        });
        assert_eq!(decode_request(&buf, 1).unwrap().records[0].trace_id, None);
    }

    #[test]
    fn typed_attribute_values_keep_their_type() {
        let mut e = Enc::default();
        e.msg(1, |rl| {
            rl.msg(2, |sl| {
                sl.msg(2, |r| {
                    r.msg(5, |b| b.string(1, "x"));
                    r.msg(6, |kv| {
                        kv.string(1, "retries");
                        kv.msg(2, |v| v.uint(3, 3));
                    });
                    r.msg(6, |kv| {
                        kv.string(1, "ok");
                        kv.msg(2, |v| v.uint(2, 1));
                    });
                    r.msg(6, |kv| {
                        kv.string(1, "ratio");
                        kv.msg(2, |v| {
                            v.tag(4, 1);
                            v.0.extend_from_slice(&0.5f64.to_bits().to_le_bytes());
                        });
                    });
                });
            });
        });
        let rec = &decode_request(&e.0, 1).unwrap().records[0];
        assert_eq!(rec.attributes[0].value, AttrValue::I64(3));
        assert_eq!(rec.attributes[1].value, AttrValue::Bool(true));
        assert_eq!(rec.attributes[2].value, AttrValue::F64(0.5));
    }

    #[test]
    fn structured_body_is_flattened_not_dropped() {
        // A kvlist body is what structured loggers send. Rendering beats
        // dropping: the text still templates and still greps.
        let mut e = Enc::default();
        e.msg(1, |rl| {
            rl.msg(2, |sl| {
                sl.msg(2, |r| {
                    r.msg(5, |b| {
                        b.msg(6, |kvl| {
                            kvl.msg(1, |kv| {
                                kv.string(1, "msg");
                                kv.any_string(2, "timeout");
                            });
                            kvl.msg(1, |kv| {
                                kv.string(1, "ms");
                                kv.msg(2, |v| v.uint(3, 30));
                            });
                        });
                    });
                });
            });
        });
        assert_eq!(decode_request(&e.0, 1).unwrap().records[0].body, "{msg=timeout ms=30}");
    }

    #[test]
    fn array_body_is_rendered() {
        let mut e = Enc::default();
        e.msg(1, |rl| {
            rl.msg(2, |sl| {
                sl.msg(2, |r| {
                    r.msg(5, |b| {
                        b.msg(5, |arr| {
                            arr.any_string(1, "a");
                            arr.any_string(1, "b");
                        });
                    });
                });
            });
        });
        assert_eq!(decode_request(&e.0, 1).unwrap().records[0].body, "[a,b]");
    }

    #[test]
    fn multiple_resources_and_scopes_all_decode() {
        let mut e = Enc::default();
        for service in ["a", "b"] {
            e.msg(1, |rl| {
                rl.msg(1, |res| res.attr(1, "service.name", service));
                for scope in ["one", "two"] {
                    rl.msg(2, |sl| {
                        sl.msg(1, |s| s.string(1, scope));
                        sl.msg(2, |r| r.msg(5, |b| b.string(1, "x")));
                        sl.msg(2, |r| r.msg(5, |b| b.string(1, "y")));
                    });
                }
            });
        }
        let d = decode_request(&e.0, 1).unwrap();
        assert_eq!(d.records.len(), 8);
        assert_eq!(d.records.iter().filter(|r| r.service.as_deref() == Some("a")).count(), 4);
    }

    #[test]
    fn scope_before_resource_still_gets_the_service() {
        // protobuf does not guarantee field order on the wire.
        let mut e = Enc::default();
        e.msg(1, |rl| {
            rl.msg(2, |sl| sl.msg(2, |r| r.msg(5, |b| b.string(1, "x"))));
            rl.msg(1, |res| res.attr(1, "service.name", "late"));
        });
        assert_eq!(decode_request(&e.0, 1).unwrap().records[0].service.as_deref(), Some("late"));
    }

    #[test]
    fn upstream_drops_are_counted() {
        let mut e = Enc::default();
        e.msg(1, |rl| {
            rl.msg(1, |res| res.uint(2, 5));
            rl.msg(2, |sl| {
                sl.msg(2, |r| {
                    r.msg(5, |b| b.string(1, "x"));
                    r.uint(7, 3);
                });
            });
        });
        assert_eq!(decode_request(&e.0, 1).unwrap().dropped_upstream, 8);
    }

    #[test]
    fn empty_request_is_valid_and_yields_nothing() {
        // Collectors send empty batches as health probes.
        let d = decode_request(&[], 1).unwrap();
        assert!(d.records.is_empty());
    }

    #[test]
    fn garbage_is_rejected_not_silently_empty() {
        let err = decode_request(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 1);
        assert!(err.is_err());
    }

    #[test]
    fn severity_number_above_range_is_clamped() {
        let buf = request(|r| {
            r.uint(2, 9999);
            r.msg(5, |b| b.string(1, "x"));
        });
        assert_eq!(decode_request(&buf, 1).unwrap().records[0].severity, Severity(24));
    }
}
