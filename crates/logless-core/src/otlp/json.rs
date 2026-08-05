//! OTLP/JSON — the second encoding of the same message tree.
//!
//! Protobuf covers the Collector and the language SDKs. JSON covers everything
//! that cannot link a protobuf runtime: browser SDKs, a `curl` in a runbook, a
//! Lambda that would rather not ship a code generator. It is the same
//! `ExportLogsServiceRequest`, so this maps onto the same [`LogRecord`] and
//! nothing downstream can tell which encoding a record arrived in.
//!
//! Two details the spec fixes and a naive reader gets wrong:
//!
//! * **64-bit fields are strings.** JSON numbers are doubles, so a nanosecond
//!   timestamp past 2^53 loses precision — about 104 days of resolution at
//!   today's epoch. The spec therefore encodes `timeUnixNano` as a decimal
//!   *string*, and both forms have to be accepted because emitters differ.
//! * **Ids are hex, not base64.** `traceId` is 32 hex characters here and 16
//!   raw bytes in protobuf.
//!
//! Field names are lowerCamelCase per the protobuf JSON mapping, but the
//! original proto names are also legal, so both are accepted.

use serde_json::Value;

use crate::model::{Attr, AttrValue, LogRecord, Severity};
use super::logs::Decoded;

/// Severity for a record that declares none — same reasoning as the protobuf
/// path: `0` would file a whole fleet's logs into the `debug` bucket.
const DEFAULT_SEVERITY: Severity = Severity::INFO;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum JsonError {
    #[error("body is not valid JSON")]
    NotJson,
    #[error("request is not a JSON object")]
    NotAnObject,
}

/// Decodes an OTLP/JSON logs export request.
pub fn decode_request(body: &[u8], now_unix_nano: u64) -> Result<Decoded, JsonError> {
    let root: Value = serde_json::from_slice(body).map_err(|_| JsonError::NotJson)?;
    let Value::Object(root) = root else {
        return Err(JsonError::NotAnObject);
    };
    let mut out = Decoded::default();

    for resource_logs in array(&root, "resourceLogs", "resource_logs") {
        let resource = resource_logs.get("resource");
        let mut resource_attrs = Vec::new();
        let mut service = None;
        if let Some(Value::Object(resource)) = resource {
            for attr in array(resource, "attributes", "attributes") {
                if let Some(attr) = key_value(attr) {
                    if attr.key == "service.name" {
                        if let AttrValue::Str(name) = &attr.value {
                            service = Some(name.clone());
                        }
                    }
                    resource_attrs.push(attr);
                }
            }
            out.dropped_upstream += resource
                .get("droppedAttributesCount")
                .or_else(|| resource.get("dropped_attributes_count"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
        }

        let Value::Object(resource_logs) = resource_logs else { continue };
        for scope_logs in array(resource_logs, "scopeLogs", "scope_logs") {
            let Value::Object(scope_logs) = scope_logs else { continue };
            let scope_name = scope_logs
                .get("scope")
                .and_then(|s| s.get("name"))
                .and_then(Value::as_str)
                .filter(|n| !n.is_empty())
                .map(str::to_string);

            for record in array(scope_logs, "logRecords", "log_records") {
                let Value::Object(record) = record else { continue };
                let mut log = log_record(record, now_unix_nano, &mut out);
                log.service = service.clone();
                if let Some(name) = &scope_name {
                    log.attributes.push(Attr {
                        key: "otel.scope.name".into(),
                        value: AttrValue::Str(name.clone()),
                    });
                }
                log.attributes.extend_from_slice(&resource_attrs);
                out.records.push(log);
            }
        }
    }
    Ok(out)
}

fn log_record(
    record: &serde_json::Map<String, Value>,
    now: u64,
    out: &mut Decoded,
) -> LogRecord {
    let observed = u64_field(record, "observedTimeUnixNano", "observed_time_unix_nano")
        .filter(|t| *t != 0)
        .unwrap_or(now);
    let time = u64_field(record, "timeUnixNano", "time_unix_nano")
        .filter(|t| *t != 0)
        .unwrap_or(observed);

    let severity_text = record
        .get("severityText")
        .or_else(|| record.get("severity_text"))
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .map(str::to_string);
    // The enum is legal as a number *or* as its name (`SEVERITY_NUMBER_ERROR`).
    let severity_number = match record.get("severityNumber").or_else(|| record.get("severity_number"))
    {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
        Some(Value::String(s)) => severity_from_enum_name(s).unwrap_or(0),
        _ => 0,
    };
    let severity = match (severity_number, severity_text.as_deref()) {
        (0, Some(text)) => Severity::from_text(text).unwrap_or(DEFAULT_SEVERITY),
        (0, None) => DEFAULT_SEVERITY,
        (n, _) => Severity(n.min(24) as u8),
    };

    let body = record.get("body").map(any_value_to_string).unwrap_or_default();

    let mut attributes = Vec::new();
    for attr in array(record, "attributes", "attributes") {
        if let Some(attr) = key_value(attr) {
            attributes.push(attr);
        }
    }
    out.dropped_upstream += record
        .get("droppedAttributesCount")
        .or_else(|| record.get("dropped_attributes_count"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if let Some(name) = record
        .get("eventName")
        .or_else(|| record.get("event_name"))
        .and_then(Value::as_str)
        .filter(|n| !n.is_empty())
    {
        attributes.push(Attr { key: "event.name".into(), value: AttrValue::Str(name.into()) });
    }

    LogRecord {
        event_id: uuid::Uuid::now_v7(),
        timestamp_unix_nano: time,
        observed_unix_nano: observed,
        severity,
        severity_text,
        body,
        service: None,
        trace_id: record
            .get("traceId")
            .or_else(|| record.get("trace_id"))
            .and_then(Value::as_str)
            .and_then(hex_bytes::<16>),
        span_id: record
            .get("spanId")
            .or_else(|| record.get("span_id"))
            .and_then(Value::as_str)
            .and_then(hex_bytes::<8>),
        attributes,
        template_id: None,
    }
}

fn array<'a>(
    object: &'a serde_json::Map<String, Value>,
    camel: &str,
    snake: &str,
) -> impl Iterator<Item = &'a Value> {
    object
        .get(camel)
        .or_else(|| object.get(snake))
        .and_then(Value::as_array)
        .map(|a| a.iter())
        .unwrap_or_else(|| [].iter())
}

/// Reads a 64-bit field in either of its legal forms.
///
/// A JSON number is a double: past 2^53 a nanosecond timestamp silently loses
/// precision, which is why the spec puts these in strings. Emitters do both, so
/// both are read — but only the string form is exact.
fn u64_field(object: &serde_json::Map<String, Value>, camel: &str, snake: &str) -> Option<u64> {
    match object.get(camel).or_else(|| object.get(snake))? {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)),
        _ => None,
    }
}

fn key_value(value: &Value) -> Option<Attr> {
    let key = value.get("key")?.as_str()?;
    if key.is_empty() {
        return None;
    }
    let value = value.get("value").map(any_value).unwrap_or(AttrValue::Str(String::new()));
    Some(Attr { key: key.to_string(), value })
}

/// `AnyValue` is a tagged union: exactly one of these keys is present.
fn any_value(value: &Value) -> AttrValue {
    if let Some(v) = value.get("stringValue").or_else(|| value.get("string_value")) {
        return AttrValue::Str(v.as_str().unwrap_or_default().to_string());
    }
    if let Some(v) = value.get("boolValue").or_else(|| value.get("bool_value")) {
        return AttrValue::Bool(v.as_bool().unwrap_or(false));
    }
    if let Some(v) = value.get("intValue").or_else(|| value.get("int_value")) {
        // int64 is a string here for the same precision reason as timestamps.
        let parsed = match v {
            Value::String(s) => s.parse::<i64>().ok(),
            Value::Number(n) => n.as_i64(),
            _ => None,
        };
        return AttrValue::I64(parsed.unwrap_or(0));
    }
    if let Some(v) = value.get("doubleValue").or_else(|| value.get("double_value")) {
        return AttrValue::F64(v.as_f64().unwrap_or(0.0));
    }
    AttrValue::Str(any_value_to_string(value))
}

/// Renders a body or a structured value to text, matching what the protobuf
/// path does so the same event templates identically over either encoding.
fn any_value_to_string(value: &Value) -> String {
    if let Some(v) = value.get("stringValue").or_else(|| value.get("string_value")) {
        return v.as_str().unwrap_or_default().to_string();
    }
    if let Some(v) = value.get("boolValue").or_else(|| value.get("bool_value")) {
        return v.as_bool().unwrap_or(false).to_string();
    }
    if let Some(v) = value.get("intValue").or_else(|| value.get("int_value")) {
        return match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
    }
    if let Some(v) = value.get("doubleValue").or_else(|| value.get("double_value")) {
        return v.as_f64().map(|f| f.to_string()).unwrap_or_default();
    }
    if let Some(v) = value.get("arrayValue").or_else(|| value.get("array_value")) {
        let items = v
            .get("values")
            .and_then(Value::as_array)
            .map(|a| a.iter().map(any_value_to_string).collect::<Vec<_>>())
            .unwrap_or_default();
        return format!("[{}]", items.join(","));
    }
    if let Some(v) = value.get("kvlistValue").or_else(|| value.get("kvlist_value")) {
        let items = v
            .get("values")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|kv| {
                        let key = kv.get("key")?.as_str()?;
                        let value = kv.get("value").map(any_value_to_string).unwrap_or_default();
                        Some(format!("{key}={value}"))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        return format!("{{{}}}", items.join(" "));
    }
    if let Some(v) = value.get("bytesValue").or_else(|| value.get("bytes_value")) {
        return v.as_str().unwrap_or_default().to_string();
    }
    String::new()
}

fn severity_from_enum_name(name: &str) -> Option<u64> {
    let name = name.trim().to_ascii_uppercase();
    let suffix = name.strip_prefix("SEVERITY_NUMBER_").unwrap_or(&name);
    let (word, offset) = match suffix.find(|c: char| c.is_ascii_digit()) {
        Some(i) => (&suffix[..i], suffix[i..].parse::<u64>().ok()? - 1),
        None => (suffix, 0),
    };
    let base = match word {
        "TRACE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARN" => 13,
        "ERROR" => 17,
        "FATAL" => 21,
        _ => return None,
    };
    Some(base + offset)
}

/// Hex → fixed-size bytes. All-zero means unset, as in the protobuf path.
fn hex_bytes<const N: usize>(text: &str) -> Option<[u8; N]> {
    if text.len() != N * 2 || !text.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).ok()?;
    }
    if out.iter().all(|b| *b == 0) {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000_000_000_000;

    fn one(json: &str) -> LogRecord {
        let decoded = decode_request(json.as_bytes(), NOW).unwrap();
        assert_eq!(decoded.records.len(), 1, "expected exactly one record");
        decoded.records.into_iter().next().unwrap()
    }

    #[test]
    fn decodes_the_documented_shape() {
        let record = one(
            r#"{"resourceLogs":[{"resource":{"attributes":[
                {"key":"service.name","value":{"stringValue":"checkout"}}]},
              "scopeLogs":[{"scope":{"name":"app"},"logRecords":[{
                "timeUnixNano":"1700000000000000000",
                "severityNumber":17,
                "severityText":"ERROR",
                "body":{"stringValue":"payment declined"},
                "attributes":[{"key":"http.method","value":{"stringValue":"POST"}}],
                "traceId":"0123456789abcdef0123456789abcdef",
                "spanId":"0123456789abcdef"
              }]}]}]}"#,
        );
        assert_eq!(record.body, "payment declined");
        assert_eq!(record.severity, Severity::ERROR);
        assert_eq!(record.service.as_deref(), Some("checkout"));
        assert_eq!(record.timestamp_unix_nano, NOW);
        assert_eq!(record.trace_id, Some([0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                                          0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]));
        let keys: Vec<&str> = record.attributes.iter().map(|a| a.key.as_str()).collect();
        assert_eq!(keys, ["http.method", "otel.scope.name", "service.name"]);
    }

    #[test]
    fn nanosecond_timestamps_are_exact_as_strings() {
        // The reason the spec uses strings: as a JSON number this value is a
        // double and loses the last few digits, which is hundreds of
        // milliseconds of drift on every record.
        let record = one(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
                {"timeUnixNano":"1700000000123456789","body":{"stringValue":"x"}}]}]}]}"#,
        );
        assert_eq!(record.timestamp_unix_nano, 1_700_000_000_123_456_789);
    }

    #[test]
    fn numeric_timestamps_are_accepted_because_emitters_send_them() {
        let record = one(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
                {"timeUnixNano":1700000000000000000,"body":{"stringValue":"x"}}]}]}]}"#,
        );
        assert_eq!(record.timestamp_unix_nano, NOW);
    }

    #[test]
    fn severity_accepts_a_number_a_name_or_only_text() {
        for (field, expect) in [
            (r#""severityNumber":17"#, Severity::ERROR),
            (r#""severityNumber":"SEVERITY_NUMBER_ERROR""#, Severity::ERROR),
            (r#""severityNumber":"SEVERITY_NUMBER_WARN2""#, Severity(14)),
            (r#""severityText":"warning""#, Severity::WARN),
        ] {
            let json = format!(
                r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[
                    {{{field},"body":{{"stringValue":"x"}}}}]}}]}}]}}"#
            );
            assert_eq!(one(&json).severity, expect, "{field}");
        }
    }

    #[test]
    fn an_unspecified_severity_defaults_to_info() {
        let record = one(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
                {"body":{"stringValue":"x"}}]}]}]}"#,
        );
        assert_eq!(record.severity, Severity::INFO);
        assert_eq!(record.observed_unix_nano, NOW, "and falls back to receive time");
    }

    #[test]
    fn typed_attributes_keep_their_type_including_stringly_int64() {
        let record = one(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{
                "body":{"stringValue":"x"},
                "attributes":[
                  {"key":"retries","value":{"intValue":"3"}},
                  {"key":"ok","value":{"boolValue":true}},
                  {"key":"ratio","value":{"doubleValue":0.5}}]}]}]}]}"#,
        );
        assert_eq!(record.attributes[0].value, AttrValue::I64(3));
        assert_eq!(record.attributes[1].value, AttrValue::Bool(true));
        assert_eq!(record.attributes[2].value, AttrValue::F64(0.5));
    }

    #[test]
    fn structured_bodies_render_the_same_as_over_protobuf() {
        // Same event, either encoding, one template downstream.
        let record = one(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{
                "body":{"kvlistValue":{"values":[
                    {"key":"msg","value":{"stringValue":"timeout"}},
                    {"key":"ms","value":{"intValue":"30"}}]}}}]}]}]}"#,
        );
        assert_eq!(record.body, "{msg=timeout ms=30}");
    }

    #[test]
    fn snake_case_field_names_are_accepted_too() {
        // Both spellings are legal in the protobuf JSON mapping.
        let record = one(
            r#"{"resource_logs":[{"scope_logs":[{"log_records":[
                {"time_unix_nano":"1700000000000000000","severity_number":17,
                 "body":{"string_value":"x"}}]}]}]}"#,
        );
        assert_eq!(record.severity, Severity::ERROR);
        assert_eq!(record.timestamp_unix_nano, NOW);
    }

    #[test]
    fn zero_and_malformed_ids_are_unset_rather_than_wrong() {
        let record = one(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{
                "body":{"stringValue":"x"},
                "traceId":"00000000000000000000000000000000",
                "spanId":"nothex!!"}]}]}]}"#,
        );
        assert_eq!(record.trace_id, None);
        assert_eq!(record.span_id, None);
    }

    #[test]
    fn an_empty_request_is_valid_and_garbage_is_not() {
        assert!(decode_request(b"{}", NOW).unwrap().records.is_empty());
        assert_eq!(decode_request(b"not json", NOW).unwrap_err(), JsonError::NotJson);
        assert_eq!(decode_request(b"[]", NOW).unwrap_err(), JsonError::NotAnObject);
    }

    #[test]
    fn upstream_drops_are_counted() {
        let decoded = decode_request(
            br#"{"resourceLogs":[{"resource":{"droppedAttributesCount":5},
                "scopeLogs":[{"logRecords":[
                  {"body":{"stringValue":"x"},"droppedAttributesCount":3}]}]}]}"#,
            NOW,
        )
        .unwrap();
        assert_eq!(decoded.dropped_upstream, 8);
    }
}
