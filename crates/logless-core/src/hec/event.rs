//! Splunk HEC payloads → [`LogRecord`].
//!
//! Two body formats:
//!
//! * `/services/collector/event` — one or more JSON objects **concatenated**,
//!   not wrapped in an array. That is the format Splunk documents and every
//!   client emits, and it is why this parses a stream rather than a document.
//! * `/services/collector/raw` — the body is the event, split on newlines, with
//!   metadata coming from query parameters instead.

use crate::model::{Attr, AttrValue, LogRecord, Severity};

/// Used when nothing in the event suggests a level. HEC has no severity concept
/// at all, so most events land here; INFO rather than DEBUG because a record
/// that lands in the `debug` bucket is shed first and kept for a day, and
/// silently applying that to a Splunk shop's entire feed would be a data-loss
/// bug disguised as a default.
const DEFAULT_SEVERITY: Severity = Severity::INFO;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum EventError {
    #[error("invalid data format")]
    InvalidFormat,
    #[error("event field is required")]
    EventRequired,
    #[error("event field cannot be blank")]
    EventBlank,
    #[error("no data")]
    NoData,
}

/// Metadata carried on the request rather than in the body — the query
/// parameters of `/raw`, and the defaults a `/event` object may override.
#[derive(Debug, Default, Clone)]
pub struct Metadata {
    pub host: Option<String>,
    pub source: Option<String>,
    pub sourcetype: Option<String>,
    pub index: Option<String>,
}

impl Metadata {
    /// Parses the `host`/`source`/`sourcetype`/`index` query parameters.
    pub fn from_query(query: &str) -> Self {
        let mut out = Self::default();
        for (key, value) in query_pairs(query) {
            match key.as_str() {
                "host" => out.host = Some(value),
                "source" => out.source = Some(value),
                "sourcetype" => out.sourcetype = Some(value),
                "index" => out.index = Some(value),
                _ => {}
            }
        }
        out
    }
}

/// Splits a query string into decoded key/value pairs.
pub fn query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

/// Minimal percent-decoding. Channel GUIDs and source names are the values that
/// matter here, and both are routinely sent with `%2F` or `+` in them.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    Ok(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parses the concatenated-JSON body of `/services/collector/event`.
pub fn parse_event_body(
    body: &[u8],
    defaults: &Metadata,
    now_unix_nano: u64,
) -> Result<Vec<LogRecord>, EventError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Err(EventError::NoData);
    }
    let mut out = Vec::new();
    let stream = serde_json::Deserializer::from_slice(body).into_iter::<serde_json::Value>();
    for value in stream {
        let value = value.map_err(|_| EventError::InvalidFormat)?;
        let serde_json::Value::Object(object) = value else {
            return Err(EventError::InvalidFormat);
        };
        out.push(record_from_object(&object, defaults, now_unix_nano)?);
    }
    if out.is_empty() {
        return Err(EventError::NoData);
    }
    Ok(out)
}

/// Parses the body of `/services/collector/raw`: one event per line.
pub fn parse_raw_body(
    body: &[u8],
    defaults: &Metadata,
    now_unix_nano: u64,
) -> Result<Vec<LogRecord>, EventError> {
    let text = String::from_utf8_lossy(body);
    let records: Vec<LogRecord> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut record = LogRecord::new(now_unix_nano, severity_of_line(line), line);
            apply_metadata(&mut record, defaults);
            record
        })
        .collect();
    if records.is_empty() {
        return Err(EventError::NoData);
    }
    Ok(records)
}

fn record_from_object(
    object: &serde_json::Map<String, serde_json::Value>,
    defaults: &Metadata,
    now_unix_nano: u64,
) -> Result<LogRecord, EventError> {
    let event = object.get("event").ok_or(EventError::EventRequired)?;
    let body = match event {
        serde_json::Value::String(s) if s.is_empty() => return Err(EventError::EventBlank),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => return Err(EventError::EventBlank),
        // Structured events are the common case from modern clients. Rendering
        // to compact JSON keeps them greppable and templatable rather than
        // dropping the structure on the floor.
        other => other.to_string(),
    };

    let observed = now_unix_nano;
    let timestamp = object.get("time").and_then(hec_time_to_nanos).unwrap_or(observed);

    let mut record = LogRecord::new(timestamp, DEFAULT_SEVERITY, body);
    record.observed_unix_nano = observed;

    // Request-level metadata first, then per-event overrides — the event wins,
    // which is the Splunk precedence.
    apply_metadata(&mut record, defaults);
    for key in ["host", "source", "sourcetype", "index"] {
        if let Some(serde_json::Value::String(value)) = object.get(key) {
            set_attr(&mut record, key, AttrValue::Str(value.clone()));
            if key == "sourcetype" {
                record.service = Some(value.clone());
            }
        }
    }

    if let Some(serde_json::Value::Object(fields)) = object.get("fields") {
        for (key, value) in fields {
            set_attr(&mut record, key, json_to_attr(value));
        }
    }

    record.severity = infer_severity(object, &record).unwrap_or(DEFAULT_SEVERITY);
    // Recorded whenever the event said a level, including when that level
    // happens to be the default: "the client said INFO" and "the client said
    // nothing" are different facts, and only the second is a guess.
    record.severity_text = severity_text(object, &record);
    Ok(record)
}

/// HEC severity is a convention, not a field: clients put it in `fields`, in the
/// structured event itself, in the text of the line, or nowhere. Checked in that
/// order, because an explicit indexed field is a stronger signal than a word
/// inside the text.
fn infer_severity(
    object: &serde_json::Map<String, serde_json::Value>,
    record: &LogRecord,
) -> Option<Severity> {
    for key in ["severity", "level", "log_level", "loglevel"] {
        if let Some(attr) = record.attributes.iter().find(|a| a.key.eq_ignore_ascii_case(key)) {
            if let AttrValue::Str(value) = &attr.value {
                if let Some(severity) = Severity::from_text(value) {
                    return Some(severity);
                }
            }
        }
    }
    if let Some(serde_json::Value::Object(event)) = object.get("event") {
        for key in ["severity", "level", "log_level", "loglevel"] {
            if let Some(serde_json::Value::String(value)) = event.get(key) {
                if let Some(severity) = Severity::from_text(value) {
                    return Some(severity);
                }
            }
        }
        return None;
    }
    // A **string** event is a real log line, and a log line's level lives in its
    // prefix — there is nowhere else for the client to have put it. Without this
    // an agent fronting an application that sends its existing lines through
    // `/event` files every record at INFO, which is not merely a wrong column:
    // partition-by-level collapses to one bucket, errors inherit the `info`
    // retention policy, and pushdown never fires because nothing reaches
    // `trigger_severity`. Observed against 1M lines from 11 LogHub corpora —
    // every one landed in `level=info`, including those containing `[error]`.
    //
    // Deliberately not applied to structured events: their keys were checked
    // above, and scanning their rendered JSON would match the word "error"
    // inside a message and call it a level.
    if let Some(serde_json::Value::String(_)) = object.get("event") {
        return severity_in_line(&record.body);
    }
    None
}

fn severity_text(
    object: &serde_json::Map<String, serde_json::Value>,
    record: &LogRecord,
) -> Option<String> {
    for key in ["severity", "level", "log_level", "loglevel"] {
        if let Some(attr) = record.attributes.iter().find(|a| a.key.eq_ignore_ascii_case(key)) {
            if let AttrValue::Str(value) = &attr.value {
                if Severity::from_text(value).is_some() {
                    return Some(value.clone());
                }
            }
        }
    }
    if let Some(serde_json::Value::Object(event)) = object.get("event") {
        for key in ["severity", "level", "log_level", "loglevel"] {
            if let Some(serde_json::Value::String(value)) = event.get(key) {
                if Severity::from_text(value).is_some() {
                    return Some(value.clone());
                }
            }
        }
    }
    None
}

/// How far into a line to look for a level word.
///
/// Three tokens was too few for every real format measured: the level sits
/// *after* the metadata prefix, and real prefixes are long. Apache puts it at
/// token 7 (`[Sun Dec 04 04:47:44 2005] [error] …`), HDFS and Zookeeper at 4,
/// and BGL at 9 (`- 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 … RAS KERNEL INFO
/// …`). Twelve covers all eleven LogHub corpora with headroom.
const LEVEL_SCAN_TOKENS: usize = 12;

/// The level named in a line's prefix, if it names one.
///
/// Two bounds, because one is not enough. A level belongs to the *prefix* a
/// logger writes, never to the message a human wrote, and the whole risk here is
/// reading a word out of the message: classifying `note=the user typed ERROR
/// into the form` as an error would forward it upstream and page someone.
///
/// * **Token count.** Deep enough for BGL's token 9, no deeper than needed.
/// * **Stop at the first `key=value` token.** Logfmt pairs are where a prefix
///   ends and content begins; no real prefix contains one. This is the bound
///   that actually separates BGL's token-9 `FATAL` from prose at token 12,
///   which a count alone cannot do.
///
/// Matching is exact-token after trimming punctuation, never substring, so
/// `errors=0` and `error_count` are not levels.
///
/// `None` rather than a default, so callers can distinguish "the line said
/// INFO" from "the line said nothing" — [`infer_severity`] needs that to know
/// whether to keep looking.
fn severity_in_line(line: &str) -> Option<Severity> {
    line.split_whitespace()
        .take(LEVEL_SCAN_TOKENS)
        .take_while(|token| !is_key_value(token))
        .find_map(|token| Severity::from_text(token.trim_matches(|c: char| !c.is_alphanumeric())))
}

/// A logfmt-style pair: a non-empty key, then `=`. `=` alone, or a leading `=`,
/// is punctuation rather than structure and does not end the prefix.
fn is_key_value(token: &str) -> bool {
    match token.find('=') {
        Some(0) | None => false,
        Some(_) => true,
    }
}

/// `/raw` has nowhere to put a level, so the line itself is the only signal.
/// Bracketed and bare forms both appear in real logs.
fn severity_of_line(line: &str) -> Severity {
    severity_in_line(line).unwrap_or(DEFAULT_SEVERITY)
}

fn apply_metadata(record: &mut LogRecord, meta: &Metadata) {
    for (key, value) in [
        ("host", &meta.host),
        ("source", &meta.source),
        ("sourcetype", &meta.sourcetype),
        ("index", &meta.index),
    ] {
        if let Some(value) = value {
            set_attr(record, key, AttrValue::Str(value.clone()));
        }
    }
    // `sourcetype` is the closest thing HEC has to a service name, and service
    // is a sort key on every file and a predicate on every replay query.
    if let Some(sourcetype) = &meta.sourcetype {
        record.service = Some(sourcetype.clone());
    }
}

fn set_attr(record: &mut LogRecord, key: &str, value: AttrValue) {
    if let Some(existing) = record.attributes.iter_mut().find(|a| a.key == key) {
        existing.value = value;
    } else {
        record.attributes.push(Attr { key: key.to_string(), value });
    }
}

fn json_to_attr(value: &serde_json::Value) -> AttrValue {
    match value {
        serde_json::Value::String(s) => AttrValue::Str(s.clone()),
        serde_json::Value::Bool(b) => AttrValue::Bool(*b),
        serde_json::Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => AttrValue::I64(i),
            (None, Some(f)) => AttrValue::F64(f),
            _ => AttrValue::Str(n.to_string()),
        },
        other => AttrValue::Str(other.to_string()),
    }
}

/// HEC `time` is epoch **seconds**, with an optional fractional part — and
/// clients also send milliseconds, which Splunk itself tolerates. Distinguished
/// by magnitude: a value past ~2001 in milliseconds would be year 33658 in
/// seconds, so no real timestamp is ambiguous.
fn hec_time_to_nanos(value: &serde_json::Value) -> Option<u64> {
    const MILLIS_THRESHOLD: f64 = 1.0e11;
    let seconds = match value {
        serde_json::Value::Number(n) => n.as_f64()?,
        serde_json::Value::String(s) => s.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    if seconds <= 0.0 {
        return None;
    }
    let nanos = if seconds >= MILLIS_THRESHOLD {
        seconds * 1.0e6
    } else {
        seconds * 1.0e9
    };
    Some(nanos as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000_000_000_000;

    fn one(json: &str) -> LogRecord {
        parse_event_body(json.as_bytes(), &Metadata::default(), NOW).unwrap().remove(0)
    }

    #[test]
    fn parses_concatenated_objects_not_an_array() {
        // This is the format Splunk documents and clients emit; treating the
        // body as a single JSON document would reject every real batch.
        let body = r#"{"event":"one"}{"event":"two"}
        {"event":"three"}"#;
        let records = parse_event_body(body.as_bytes(), &Metadata::default(), NOW).unwrap();
        let bodies: Vec<&str> = records.iter().map(|r| r.body.as_str()).collect();
        assert_eq!(bodies, ["one", "two", "three"]);
    }

    /// One line per LogHub corpus, verbatim, because the level's *position*
    /// is the whole point and a paraphrase would move it.
    #[test]
    fn a_string_event_gets_its_level_from_the_line() {
        let cases: [(&str, Severity); 6] = [
            // Apache — level at token 7, inside brackets.
            (
                "[Sun Dec 04 04:47:44 2005] [error] mod_jk child workerEnv in error state 6",
                Severity::ERROR,
            ),
            // HDFS — level at token 4.
            (
                "081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 terminating",
                Severity::INFO,
            ),
            // Zookeeper — level at token 4, after a bare dash.
            (
                "2015-07-29 17:41:41,648 - WARN  [main:QuorumPeer@1000] - Unexpected exception",
                Severity::WARN,
            ),
            // BGL — level at token 9, the deepest of the eleven corpora.
            (
                "- 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.363779 \
                 R02-M1-N0-C:J12-U11 RAS KERNEL FATAL instruction cache parity error",
                Severity::FATAL,
            ),
            // Linux syslog names no level at all — the default must still apply.
            (
                "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure",
                DEFAULT_SEVERITY,
            ),
            // A level word beyond the scan window is prose, not a level. This is
            // the false-positive the bound exists to prevent.
            (
                "2015-07-29 17:41:41,648 svc-checkout req=1 user=42 session=abc path=/pay \
                 status=200 note=the user typed ERROR into the form",
                DEFAULT_SEVERITY,
            ),
        ];
        for (line, expected) in cases {
            let record = one(&serde_json::json!({ "event": line }).to_string());
            assert_eq!(
                expected, record.severity,
                "wrong level for: {line}"
            );
        }
    }

    #[test]
    fn an_explicit_field_still_beats_the_line_text() {
        // The line says ERROR; the client says DEBUG in an indexed field. The
        // declared value wins — inference is the last resort, not the first.
        let record = one(
            r#"{"event":"2015-07-29 17:41:41,648 - ERROR - boom","fields":{"level":"debug"}}"#,
        );
        assert_eq!(Severity::DEBUG, record.severity);
        assert_eq!(Some("debug".to_string()), record.severity_text);
    }

    #[test]
    fn a_structured_event_is_not_scanned_as_text() {
        // Its keys were already checked. Scanning the rendered JSON would match
        // the word inside the message and call it a level.
        let record = one(r#"{"event":{"msg":"error while parsing","code":7}}"#);
        assert_eq!(DEFAULT_SEVERITY, record.severity);
    }

    #[test]
    fn inferring_from_text_is_not_the_client_declaring_a_level() {
        // `severity_text` means "the client named a level in a field". A level
        // recovered from the prefix is our reading of their text, not their
        // statement, and recording it as one would launder a guess into a fact.
        let record = one(r#"{"event":"081109 203615 148 INFO dfs.DataNode: terminating"}"#);
        assert_eq!(Severity::INFO, record.severity);
        assert_eq!(None, record.severity_text);
    }

    #[test]
    fn structured_events_are_rendered_not_dropped() {
        let record = one(r#"{"event":{"msg":"timeout","ms":30}}"#);
        assert!(record.body.contains("\"msg\":\"timeout\""), "got {}", record.body);
        assert!(record.body.contains("\"ms\":30"));
    }

    #[test]
    fn time_accepts_seconds_fractions_and_milliseconds() {
        assert_eq!(one(r#"{"event":"x","time":1700000000}"#).timestamp_unix_nano, NOW);
        assert_eq!(
            one(r#"{"event":"x","time":1700000000.5}"#).timestamp_unix_nano,
            NOW + 500_000_000
        );
        // Milliseconds, which Splunk tolerates and clients send.
        assert_eq!(one(r#"{"event":"x","time":1700000000000}"#).timestamp_unix_nano, NOW);
        // A string, which JSON-encoding clients produce for large numbers.
        assert_eq!(one(r#"{"event":"x","time":"1700000000"}"#).timestamp_unix_nano, NOW);
    }

    #[test]
    fn a_missing_time_falls_back_to_receipt_not_to_1970() {
        // Filing an event under the epoch would put it in a partition retention
        // deletes on the next sweep.
        let record = one(r#"{"event":"x"}"#);
        assert_eq!(record.timestamp_unix_nano, NOW);
        assert_eq!(record.observed_unix_nano, NOW);
    }

    #[test]
    fn severity_comes_from_fields_then_from_the_event() {
        assert_eq!(one(r#"{"event":"x","fields":{"severity":"error"}}"#).severity, Severity::ERROR);
        assert_eq!(one(r#"{"event":{"level":"WARN","m":"x"}}"#).severity, Severity::WARN);
        // An indexed field outranks a word inside the payload.
        let record = one(r#"{"event":{"level":"debug"},"fields":{"severity":"fatal"}}"#);
        assert_eq!(record.severity, Severity::FATAL);
    }

    #[test]
    fn an_explicit_info_is_recorded_as_stated_not_as_a_guess() {
        let record = one(r#"{"event":{"level":"INFO","m":"x"}}"#);
        assert_eq!(record.severity, Severity::INFO);
        assert_eq!(
            record.severity_text.as_deref(),
            Some("INFO"),
            "an explicit level must be distinguishable from an absent one"
        );
    }

    #[test]
    fn events_without_a_level_default_to_info_not_debug() {
        // HEC has no severity concept, so this is the common case. Defaulting
        // to debug would shed and expire a Splunk shop's whole feed first.
        let record = one(r#"{"event":"nothing level-ish here"}"#);
        assert_eq!(record.severity, Severity::INFO);
        assert_eq!(record.severity_text, None);
    }

    #[test]
    fn metadata_becomes_attributes_and_sourcetype_becomes_service() {
        let defaults = Metadata {
            host: Some("node-7".into()),
            sourcetype: Some("checkout".into()),
            ..Default::default()
        };
        let records =
            parse_event_body(br#"{"event":"x","index":"main"}"#, &defaults, NOW).unwrap();
        let record = &records[0];
        assert_eq!(record.service.as_deref(), Some("checkout"));
        let keys: Vec<&str> = record.attributes.iter().map(|a| a.key.as_str()).collect();
        assert!(keys.contains(&"host") && keys.contains(&"index"));
    }

    #[test]
    fn per_event_metadata_overrides_the_request_defaults() {
        let defaults = Metadata { host: Some("default".into()), ..Default::default() };
        let records =
            parse_event_body(br#"{"event":"x","host":"specific"}"#, &defaults, NOW).unwrap();
        let host = records[0].attributes.iter().find(|a| a.key == "host").unwrap();
        assert_eq!(host.value, AttrValue::Str("specific".into()));
        assert_eq!(records[0].attributes.iter().filter(|a| a.key == "host").count(), 1);
    }

    #[test]
    fn typed_fields_keep_their_type() {
        let record = one(r#"{"event":"x","fields":{"retries":3,"ok":true,"ratio":0.5}}"#);
        let get = |k: &str| record.attributes.iter().find(|a| a.key == k).unwrap().value.clone();
        assert_eq!(get("retries"), AttrValue::I64(3));
        assert_eq!(get("ok"), AttrValue::Bool(true));
        assert_eq!(get("ratio"), AttrValue::F64(0.5));
    }

    #[test]
    fn missing_or_blank_event_is_an_error_with_its_own_code() {
        let m = Metadata::default();
        assert_eq!(parse_event_body(br#"{"time":1}"#, &m, NOW), Err(EventError::EventRequired));
        assert_eq!(parse_event_body(br#"{"event":""}"#, &m, NOW), Err(EventError::EventBlank));
        assert_eq!(parse_event_body(br#"{"event":null}"#, &m, NOW), Err(EventError::EventBlank));
    }

    #[test]
    fn malformed_json_and_empty_bodies_are_distinguished() {
        let m = Metadata::default();
        assert_eq!(parse_event_body(b"   \n ", &m, NOW), Err(EventError::NoData));
        assert_eq!(parse_event_body(b"{not json}", &m, NOW), Err(EventError::InvalidFormat));
        assert_eq!(parse_event_body(b"\"a string\"", &m, NOW), Err(EventError::InvalidFormat));
    }

    #[test]
    fn raw_bodies_split_on_lines_and_sniff_a_level() {
        let records = parse_raw_body(
            b"ERROR payment declined\nplain line\n\n[WARN] retrying\n",
            &Metadata::default(),
            NOW,
        )
        .unwrap();
        assert_eq!(records.len(), 3, "blank lines are not events");
        assert_eq!(records[0].severity, Severity::ERROR);
        assert_eq!(records[1].severity, Severity::INFO);
        assert_eq!(records[2].severity, Severity::WARN);
    }

    #[test]
    fn query_parameters_are_percent_decoded() {
        let meta = Metadata::from_query("sourcetype=my%2Fapp&host=node+7&index=main");
        assert_eq!(meta.sourcetype.as_deref(), Some("my/app"));
        assert_eq!(meta.host.as_deref(), Some("node 7"));
        assert_eq!(meta.index.as_deref(), Some("main"));
    }
}
