//! Vendor payload shaping.
//!
//! Pure functions: a window in, a wire-format string out. Kept separate from
//! transport so the shape — which is what a user actually sees in Sentry or
//! Splunk — can be asserted exactly, without a network or an account.

use crate::forward::Rollup;
use crate::ring::ContextWindow;

/// Sentry severity names for OTel severity numbers.
fn sentry_level(severity: u8) -> &'static str {
    match severity {
        0..=8 => "debug",
        9..=12 => "info",
        13..=16 => "warning",
        17..=20 => "error",
        _ => "fatal",
    }
}

fn nanos_to_secs_f64(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000_000.0
}

/// A Sentry envelope carrying one error event.
///
/// Two deliberate choices:
///
/// * `fingerprint = ["logless:<template fingerprint>"]` — Sentry groups on this, so our
///   template becomes its issue. Grouping stops being heuristic and starts
///   being deterministic, which is a selling point in its own right.
/// * context lines become **breadcrumbs**, which is exactly the widget Sentry
///   already renders above a stack trace. No custom UI needed for "the debug
///   logs that caused this error" to show up where people already look.
pub fn sentry_envelope(window: &ContextWindow) -> String {
    let event_id = format!("{:032x}", window.flow_hash as u128 ^ window.error.timestamp_unix_nano as u128);
    let event = sentry_event(window, &event_id);
    // Envelope: header line, item header line, item payload.
    format!(
        "{{\"event_id\":\"{event_id}\"}}\n{{\"type\":\"event\",\"content_type\":\"application/json\",\"length\":{}}}\n{event}",
        event.len()
    )
}

fn sentry_event(window: &ContextWindow, event_id: &str) -> String {
    // Grouping key. Content-derived, never the per-agent template counter:
    // that number is assigned in arrival order, so two nodes give the same
    // shape different numbers and Sentry would split one issue across a fleet
    // while merging unrelated errors that share an index.
    let fingerprint = match window.error_template_fingerprint {
        Some(f) => format!("\"logless:t:{f:016x}\""),
        None => format!("\"logless:flow:{:016x}\"", window.flow_hash),
    };

    let breadcrumbs: Vec<String> = window
        .context
        .iter()
        .map(|line| {
            format!(
                "{{\"timestamp\":{},\"level\":\"{}\",\"type\":\"default\",\"category\":\"logless\",\"message\":{}}}",
                nanos_to_secs_f64(line.timestamp_unix_nano),
                sentry_level(line.severity),
                json_string(&line.body)
            )
        })
        .collect();

    let mut tags = vec![
        ("logless.key_tier".to_string(), window.key_tier.as_str().to_string()),
        ("logless.flow_hash".to_string(), format!("{:016x}", window.flow_hash)),
    ];
    if let Some(service) = &window.service {
        tags.push(("service".to_string(), service.clone()));
    }
    if let Some(template) = window.error_template_id {
        tags.push(("logless.template_id".to_string(), template.to_string()));
    }
    let tags: Vec<String> = tags
        .iter()
        .map(|(k, v)| format!("{}:{}", json_string(k), json_string(v)))
        .collect();

    format!(
        "{{\"event_id\":\"{event_id}\",\"timestamp\":{},\"platform\":\"other\",\"level\":\"{}\",\
\"logger\":\"logless\",\"fingerprint\":[{fingerprint}],\
\"message\":{{\"formatted\":{}}},\
\"tags\":{{{}}},\
\"breadcrumbs\":{{\"values\":[{}]}},\
\"extra\":{{\"logless.context_lines\":{},\"logless.collapsed\":{}}}}}",
        nanos_to_secs_f64(window.error.timestamp_unix_nano),
        sentry_level(window.error.severity),
        json_string(&window.error.body),
        tags.join(","),
        breadcrumbs.join(","),
        window.context.len(),
        window.suppressed,
    )
}

/// A Splunk HEC event carrying one error and its context.
///
/// Context ships as a field rather than separate events: Splunk bills per GB
/// and per event, so N context lines must not become N billable events.
pub fn splunk_event(window: &ContextWindow) -> String {
    let context: Vec<String> = window
        .context
        .iter()
        .map(|line| {
            format!(
                "{{\"ts\":{},\"severity\":{},\"template_id\":{},\"body\":{}}}",
                line.timestamp_unix_nano,
                line.severity,
                line.template_id
                    .map(|t| t.to_string())
                    .unwrap_or("null".into()),
                json_string(&line.body)
            )
        })
        .collect();

    format!(
        "{{\"time\":{},\"source\":\"logless\",\"sourcetype\":\"logless:error\",\"event\":{{\
\"body\":{},\"severity\":{},\"service\":{},\"template_id\":{},\"key_tier\":\"{}\",\
\"flow_hash\":\"{:016x}\",\"collapsed\":{},\"context_lines\":{},\"context\":[{}]}}}}",
        nanos_to_secs_f64(window.error.timestamp_unix_nano),
        json_string(&window.error.body),
        window.error.severity,
        window
            .service
            .as_deref()
            .map(json_string)
            .unwrap_or("null".into()),
        window
            .error_template_id
            .map(|t| t.to_string())
            .unwrap_or("null".into()),
        window.key_tier.as_str(),
        window.flow_hash,
        window.suppressed,
        window.context.len(),
        context.join(",")
    )
}

/// A rolled-up aggregate event: N occurrences of one template as one event.
///
/// This is what makes collapsing safe — the storm is cheaper, never invisible.
pub fn splunk_aggregate(rollup: &Rollup) -> String {
    format!(
        "{{\"time\":{},\"source\":\"logless\",\"sourcetype\":\"logless:aggregate\",\"event\":{{\
\"template_id\":{},\"template\":{},\"service\":{},\"count\":{},\
\"first_seen\":{},\"last_seen\":{},\"example\":{}}}}}",
        nanos_to_secs_f64(rollup.last_seen_unix_nano),
        rollup.template_id,
        json_string(&rollup.template_text),
        json_string(&rollup.service),
        rollup.count,
        nanos_to_secs_f64(rollup.first_seen_unix_nano),
        nanos_to_secs_f64(rollup.last_seen_unix_nano),
        json_string(&rollup.example),
    )
}

pub fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::{ContextLine, KeyTier};

    fn window(context: usize) -> ContextWindow {
        ContextWindow {
            error: ContextLine {
                timestamp_unix_nano: 1_700_000_000_000_000_000,
                severity: 17,
                template_id: Some(7),
                body: "payment \"gateway\" timeout after 4181ms".into(),
            },
            context: (0..context)
                .map(|i| ContextLine {
                    timestamp_unix_nano: 1_699_999_990_000_000_000 + i as u64,
                    severity: 5,
                    template_id: Some(2 + i as u64),
                    body: format!("step {i}"),
                })
                .collect(),
            key_tier: KeyTier::Trace,
            flow_hash: 0xdead_beef,
            error_template_id: Some(7),
            error_template_fingerprint: Some(0x1234_5678_9abc_def0),
            service: Some("api".into()),
            suppressed: 3,
        }
    }

    fn parse(json: &str) -> serde_json::Value {
        serde_json::from_str(json).expect("payload must be valid JSON")
    }

    #[test]
    fn sentry_envelope_has_three_lines_and_a_correct_length_header() {
        let envelope = sentry_envelope(&window(2));
        let lines: Vec<&str> = envelope.split('\n').collect();
        assert_eq!(lines.len(), 3, "header, item header, payload");

        let header = parse(lines[0]);
        let item = parse(lines[1]);
        assert_eq!(item["type"], "event");
        // Sentry validates this length; a mismatch silently drops the event.
        assert_eq!(item["length"].as_u64().unwrap() as usize, lines[2].len());
        assert_eq!(header["event_id"], parse(lines[2])["event_id"]);
    }

    #[test]
    fn sentry_fingerprint_is_the_template_so_grouping_is_deterministic() {
        let event = parse(sentry_envelope(&window(1)).split('\n').nth(2).unwrap());
        assert_eq!(
            event["fingerprint"][0], "logless:t:123456789abcdef0",
            "the grouping key must be content-derived, not the per-agent counter"
        );
        assert_eq!(event["level"], "error");
        assert_eq!(event["tags"]["service"], "api");
        assert_eq!(event["tags"]["logless.template_id"], "7");
        assert_eq!(event["tags"]["logless.key_tier"], "trace");
    }

    #[test]
    fn sentry_context_becomes_breadcrumbs_in_order() {
        let event = parse(sentry_envelope(&window(3)).split('\n').nth(2).unwrap());
        let crumbs = event["breadcrumbs"]["values"].as_array().unwrap();
        assert_eq!(crumbs.len(), 3);
        assert_eq!(crumbs[0]["message"], "step 0");
        assert_eq!(crumbs[2]["message"], "step 2");
        assert_eq!(crumbs[0]["level"], "debug");
        assert_eq!(event["extra"]["logless.collapsed"], 3);
    }

    #[test]
    fn an_untemplated_error_still_gets_a_stable_fingerprint() {
        // Both fields are cleared: a line that did not template has neither a
        // local id nor a content fingerprint, and the flow hash is the fallback
        // grouping key.
        let mut w = window(1);
        w.error_template_id = None;
        w.error_template_fingerprint = None;
        let event = parse(sentry_envelope(&w).split('\n').nth(2).unwrap());
        assert_eq!(event["fingerprint"][0], "logless:flow:00000000deadbeef");
    }

    #[test]
    fn the_local_template_id_never_reaches_the_grouping_key() {
        // The id is a per-agent counter assigned in arrival order. If it ever
        // leaked into the fingerprint, one shape would land in a different
        // Sentry issue on every node in the fleet.
        let mut w = window(1);
        w.error_template_id = Some(999_999);
        w.error_template_fingerprint = Some(0xabcd);
        let event = parse(sentry_envelope(&w).split('\n').nth(2).unwrap());
        let fingerprint = event["fingerprint"][0].as_str().unwrap().to_string();
        assert!(!fingerprint.contains("999999"), "{fingerprint}");
        assert_eq!(fingerprint, "logless:t:000000000000abcd");
    }

    #[test]
    fn splunk_event_keeps_context_inside_one_billable_event() {
        let event = parse(&splunk_event(&window(5)));
        assert_eq!(event["sourcetype"], "logless:error");
        assert_eq!(event["event"]["context_lines"], 5);
        assert_eq!(event["event"]["context"].as_array().unwrap().len(), 5);
        assert_eq!(event["event"]["service"], "api");
        assert_eq!(event["event"]["template_id"], 7);
        assert_eq!(event["event"]["flow_hash"], "00000000deadbeef");
    }

    #[test]
    fn splunk_aggregate_carries_the_count_and_an_example() {
        let rollup = Rollup {
            service: "api".into(),
            template_id: 7,
            template_text: "timeout after <NUM>ms".into(),
            count: 215,
            first_seen_unix_nano: 1_700_000_000_000_000_000,
            last_seen_unix_nano: 1_700_000_060_000_000_000,
            example: "timeout after 4181ms".into(),
        };
        let event = parse(&splunk_aggregate(&rollup));
        assert_eq!(event["sourcetype"], "logless:aggregate");
        assert_eq!(event["event"]["count"], 215);
        assert_eq!(event["event"]["template"], "timeout after <NUM>ms");
        assert_eq!(event["event"]["example"], "timeout after 4181ms");
        assert!(event["event"]["last_seen"].as_f64().unwrap() > event["event"]["first_seen"].as_f64().unwrap());
    }

    #[test]
    fn payloads_survive_quotes_newlines_and_control_characters() {
        // A log line is attacker-influenced text; a broken escape here means a
        // malformed payload the vendor silently drops.
        let mut w = window(1);
        w.error.body = "he said \"hi\"\nthen\ttabbed\\ and \u{7} rang".into();
        w.service = Some("svc\"quoted".into());

        let event = parse(sentry_envelope(&w).split('\n').nth(2).unwrap());
        assert_eq!(
            event["message"]["formatted"],
            "he said \"hi\"\nthen\ttabbed\\ and \u{7} rang"
        );
        assert_eq!(event["tags"]["service"], "svc\"quoted");

        let splunk = parse(&splunk_event(&w));
        assert_eq!(
            splunk["event"]["body"],
            "he said \"hi\"\nthen\ttabbed\\ and \u{7} rang"
        );
    }

    #[test]
    fn severity_numbers_map_to_vendor_levels() {
        assert_eq!(sentry_level(1), "debug");
        assert_eq!(sentry_level(9), "info");
        assert_eq!(sentry_level(13), "warning");
        assert_eq!(sentry_level(17), "error");
        assert_eq!(sentry_level(21), "fatal");
    }

    #[test]
    fn an_error_with_no_context_is_still_a_valid_payload() {
        let event = parse(sentry_envelope(&window(0)).split('\n').nth(2).unwrap());
        assert_eq!(event["breadcrumbs"]["values"].as_array().unwrap().len(), 0);
        let splunk = parse(&splunk_event(&window(0)));
        assert_eq!(splunk["event"]["context_lines"], 0);
    }
}
