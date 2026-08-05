//! Sentry envelope codec.
//!
//! An envelope is newline-framed: one JSON header line, then repeating pairs of
//! a JSON item header and a payload. The payload length comes from the item
//! header's `length` field when present, and otherwise runs to the next
//! newline — so an envelope cannot be parsed by splitting on `\n`, because an
//! attachment's bytes may contain newlines, NUL bytes, or anything else.
//!
//! Everything here preserves bytes. A proxy that re-encodes what it forwards
//! will eventually differ from what the SDK sent — a re-serialised JSON object
//! reorders keys, changes number formatting and normalises escapes — and the
//! failure mode is silent: Sentry accepts the envelope and the event is subtly
//! not what the application reported. Item headers and payloads are therefore
//! carried as raw bytes and re-emitted verbatim. Only the envelope header is
//! ever rewritten, and only its `dsn` key (see [`Envelope::to_bytes`]).

/// Item types worth naming. Anything else is carried through untouched — a
/// proxy that drops what it does not recognise breaks every SDK feature added
/// after it was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemType {
    /// An error or message. What most people mean by "a Sentry event".
    Event,
    /// A performance transaction. Usually the volume — and the bill.
    Transaction,
    Session,
    Sessions,
    Attachment,
    /// The SDK reporting its own drops. Small, and Sentry needs it for
    /// accurate client-side loss accounting.
    ClientReport,
    CheckIn,
    Profile,
    ReplayEvent,
    ReplayRecording,
    Log,
    Other,
}

impl ItemType {
    pub fn parse(raw: &str) -> Self {
        match raw {
            "event" => ItemType::Event,
            "transaction" => ItemType::Transaction,
            "session" => ItemType::Session,
            "sessions" => ItemType::Sessions,
            "attachment" => ItemType::Attachment,
            "client_report" => ItemType::ClientReport,
            "check_in" => ItemType::CheckIn,
            "profile" => ItemType::Profile,
            "replay_event" => ItemType::ReplayEvent,
            "replay_recording" => ItemType::ReplayRecording,
            "log" => ItemType::Log,
            _ => ItemType::Other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ItemType::Event => "event",
            ItemType::Transaction => "transaction",
            ItemType::Session => "session",
            ItemType::Sessions => "sessions",
            ItemType::Attachment => "attachment",
            ItemType::ClientReport => "client_report",
            ItemType::CheckIn => "check_in",
            ItemType::Profile => "profile",
            ItemType::ReplayEvent => "replay_event",
            ItemType::ReplayRecording => "replay_recording",
            ItemType::Log => "log",
            ItemType::Other => "other",
        }
    }

    /// Rate-limit category name, as Sentry uses in `X-Sentry-Rate-Limits`.
    pub fn rate_limit_category(self) -> &'static str {
        match self {
            ItemType::Event => "error",
            ItemType::Transaction => "transaction",
            ItemType::Session | ItemType::Sessions => "session",
            ItemType::Attachment => "attachment",
            ItemType::Profile => "profile",
            ItemType::ReplayEvent | ItemType::ReplayRecording => "replay",
            ItemType::Log => "log_item",
            ItemType::CheckIn => "monitor",
            ItemType::ClientReport | ItemType::Other => "default",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Item {
    /// The item header exactly as it arrived.
    pub header_raw: Vec<u8>,
    pub item_type: ItemType,
    /// Raw `type` string, so an unrecognised type survives a round trip and
    /// still appears in logs and metrics under its real name.
    pub type_name: String,
    pub payload: Vec<u8>,
    /// Whether the original header declared `length`. Preserved so a header we
    /// do not rewrite is re-emitted byte for byte.
    pub had_length: bool,
}

#[derive(Debug, Clone)]
pub struct Envelope {
    pub header: serde_json::Map<String, serde_json::Value>,
    pub items: Vec<Item>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum EnvelopeError {
    #[error("empty body")]
    Empty,
    #[error("envelope header is not a JSON object")]
    BadHeader,
    #[error("item header at offset {at} is not a JSON object")]
    BadItemHeader { at: usize },
    #[error("item at offset {at} declares {declared} bytes, only {available} present")]
    Truncated { at: usize, declared: usize, available: usize },
    #[error("envelope has more than {limit} items")]
    TooManyItems { limit: usize },
}

/// Cap on items in one envelope. Sentry's own relay bounds this; without a
/// bound, a small body of empty item headers turns into unbounded allocation.
pub const MAX_ITEMS: usize = 1000;

impl Envelope {
    pub fn parse(body: &[u8]) -> Result<Self, EnvelopeError> {
        if body.is_empty() {
            return Err(EnvelopeError::Empty);
        }
        let (header_line, mut rest) = split_line(body);
        let header: serde_json::Value =
            serde_json::from_slice(header_line).map_err(|_| EnvelopeError::BadHeader)?;
        let serde_json::Value::Object(header) = header else {
            return Err(EnvelopeError::BadHeader);
        };

        let mut items = Vec::new();
        loop {
            // A trailing newline after the last item is normal, and so is its
            // absence. Both mean "no more items".
            if rest.is_empty() || rest.iter().all(|b| *b == b'\n') {
                break;
            }
            if items.len() >= MAX_ITEMS {
                return Err(EnvelopeError::TooManyItems { limit: MAX_ITEMS });
            }
            let at = body.len() - rest.len();
            let (item_header_line, after_header) = split_line(rest);
            let item_header: serde_json::Value = serde_json::from_slice(item_header_line)
                .map_err(|_| EnvelopeError::BadItemHeader { at })?;
            let serde_json::Value::Object(item_header) = item_header else {
                return Err(EnvelopeError::BadItemHeader { at });
            };

            let type_name = item_header
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string();
            let declared = item_header.get("length").and_then(serde_json::Value::as_u64);

            let (payload, remainder) = match declared {
                Some(length) => {
                    let length = length as usize;
                    if after_header.len() < length {
                        return Err(EnvelopeError::Truncated {
                            at,
                            declared: length,
                            available: after_header.len(),
                        });
                    }
                    let payload = &after_header[..length];
                    // The newline after a length-delimited payload is optional
                    // at the end of the body.
                    let remainder = after_header[length..].strip_prefix(b"\n").unwrap_or(&after_header[length..]);
                    (payload, remainder)
                }
                None => split_line(after_header),
            };

            items.push(Item {
                header_raw: item_header_line.to_vec(),
                item_type: ItemType::parse(&type_name),
                type_name,
                payload: payload.to_vec(),
                had_length: declared.is_some(),
            });
            rest = remainder;
        }

        Ok(Self { header, items })
    }

    pub fn event_id(&self) -> Option<String> {
        self.header.get("event_id").and_then(serde_json::Value::as_str).map(str::to_string)
    }

    /// Serialises for sending upstream.
    ///
    /// The envelope header is rewritten only to drop `dsn`: an SDK puts its own
    /// DSN there, and after proxying that DSN names *us*, not the upstream
    /// project. Sentry would route on it and the event would land nowhere.
    /// Authentication travels in the `X-Sentry-Auth` header instead.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut header = self.header.clone();
        header.remove("dsn");
        let mut out = serde_json::to_vec(&serde_json::Value::Object(header)).unwrap_or_default();
        out.push(b'\n');
        for item in &self.items {
            out.extend_from_slice(&item.header_raw);
            out.push(b'\n');
            out.extend_from_slice(&item.payload);
            out.push(b'\n');
        }
        out
    }

    /// Keeps only the items the policy selected. An envelope whose items are
    /// all dropped is not worth sending: Sentry accepts it and records nothing.
    pub fn retain_items(&mut self, keep: impl FnMut(&Item) -> bool) {
        self.items.retain(keep);
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Splits at the first newline, returning (line, rest-after-newline).
fn split_line(input: &[u8]) -> (&[u8], &[u8]) {
    match input.iter().position(|b| *b == b'\n') {
        Some(i) => (&input[..i], &input[i + 1..]),
        None => (input, &input[input.len()..]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_length_delimited_event() {
        let payload = br#"{"message":"hello"}"#;
        let body = format!(
            "{{\"event_id\":\"abc\"}}\n{{\"type\":\"event\",\"length\":{}}}\n{}\n",
            payload.len(),
            String::from_utf8_lossy(payload)
        );
        let envelope = Envelope::parse(body.as_bytes()).unwrap();
        assert_eq!(envelope.event_id().as_deref(), Some("abc"));
        assert_eq!(envelope.items.len(), 1);
        assert_eq!(envelope.items[0].item_type, ItemType::Event);
        assert_eq!(envelope.items[0].payload, payload);
    }

    #[test]
    fn parses_an_item_with_no_declared_length() {
        // Legal and emitted by several SDKs: the payload runs to the newline.
        let body = b"{}\n{\"type\":\"session\"}\n{\"sid\":\"1\"}\n";
        let envelope = Envelope::parse(body).unwrap();
        assert_eq!(envelope.items.len(), 1);
        assert_eq!(envelope.items[0].payload, br#"{"sid":"1"}"#);
        assert!(!envelope.items[0].had_length);
    }

    #[test]
    fn binary_payloads_with_newlines_survive() {
        // The reason this cannot be a line-splitting parser: an attachment is
        // arbitrary bytes, and splitting on \n would shred it into "items".
        let blob: Vec<u8> = vec![0x00, b'\n', 0xff, b'\n', b'{', 0x7f];
        let mut body = Vec::new();
        body.extend_from_slice(b"{}\n");
        body.extend_from_slice(
            format!("{{\"type\":\"attachment\",\"length\":{}}}\n", blob.len()).as_bytes(),
        );
        body.extend_from_slice(&blob);
        body.push(b'\n');

        let envelope = Envelope::parse(&body).unwrap();
        assert_eq!(envelope.items.len(), 1, "binary payload must not split into items");
        assert_eq!(envelope.items[0].payload, blob);
        assert_eq!(envelope.items[0].item_type, ItemType::Attachment);
    }

    #[test]
    fn several_items_in_one_envelope() {
        let mut body = String::from("{\"event_id\":\"x\"}\n");
        body.push_str("{\"type\":\"event\",\"length\":2}\n{}\n");
        body.push_str("{\"type\":\"attachment\",\"length\":3}\nabc\n");
        body.push_str("{\"type\":\"client_report\"}\n{\"z\":1}\n");
        let envelope = Envelope::parse(body.as_bytes()).unwrap();
        let types: Vec<ItemType> = envelope.items.iter().map(|i| i.item_type).collect();
        assert_eq!(types, [ItemType::Event, ItemType::Attachment, ItemType::ClientReport]);
        assert_eq!(envelope.items[1].payload, b"abc");
    }

    #[test]
    fn an_unknown_item_type_round_trips_under_its_real_name() {
        // A proxy that drops what it does not recognise breaks every SDK
        // feature added after it was written.
        let body = b"{}\n{\"type\":\"something_new\",\"length\":2}\nhi\n";
        let envelope = Envelope::parse(body).unwrap();
        assert_eq!(envelope.items[0].item_type, ItemType::Other);
        assert_eq!(envelope.items[0].type_name, "something_new");
        let out = envelope.to_bytes();
        assert!(String::from_utf8_lossy(&out).contains("something_new"));
    }

    #[test]
    fn item_headers_and_payloads_are_re_emitted_verbatim() {
        // Re-serialising the JSON would reorder keys and renormalise numbers —
        // silently changing what the application reported.
        let header = r#"{"type":"event","length":10,"content_type":"application/json","z_first":1}"#;
        let body = format!("{{\"event_id\":\"a\"}}\n{header}\n{{\"m\":\"hi\"}}\n");
        let envelope = Envelope::parse(body.as_bytes()).unwrap();
        let out = String::from_utf8(envelope.to_bytes()).unwrap();
        assert!(out.contains(header), "item header was rewritten:\n{out}");
        assert!(out.contains(r#"{"m":"hi"}"#));
    }

    #[test]
    fn the_dsn_is_stripped_from_the_forwarded_header() {
        // After proxying, the SDK's DSN names us. Leaving it in makes Sentry
        // route on an address that is not the upstream project.
        let body = b"{\"event_id\":\"a\",\"dsn\":\"http://k@127.0.0.1:9000/1\"}\n";
        let envelope = Envelope::parse(body).unwrap();
        let out = String::from_utf8(envelope.to_bytes()).unwrap();
        assert!(!out.contains("dsn"), "{out}");
        assert!(out.contains("event_id"));
    }

    #[test]
    fn a_header_only_envelope_is_valid() {
        // SDKs send these; they are not an error.
        let envelope = Envelope::parse(b"{\"event_id\":\"a\"}\n").unwrap();
        assert!(envelope.items.is_empty());
        let envelope = Envelope::parse(b"{}").unwrap();
        assert!(envelope.items.is_empty());
    }

    #[test]
    fn a_truncated_item_is_rejected_rather_than_silently_short() {
        let body = b"{}\n{\"type\":\"event\",\"length\":100}\nshort";
        let err = Envelope::parse(body).unwrap_err();
        assert!(matches!(err, EnvelopeError::Truncated { declared: 100, available: 5, .. }));
    }

    #[test]
    fn malformed_headers_are_rejected() {
        assert_eq!(Envelope::parse(b"").unwrap_err(), EnvelopeError::Empty);
        assert_eq!(Envelope::parse(b"not json\n").unwrap_err(), EnvelopeError::BadHeader);
        assert_eq!(Envelope::parse(b"[]\n").unwrap_err(), EnvelopeError::BadHeader);
        assert!(matches!(
            Envelope::parse(b"{}\nnot json\npayload\n").unwrap_err(),
            EnvelopeError::BadItemHeader { .. }
        ));
    }

    #[test]
    fn item_count_is_bounded() {
        let mut body = String::from("{}\n");
        for _ in 0..MAX_ITEMS + 1 {
            body.push_str("{\"type\":\"event\",\"length\":0}\n\n");
        }
        assert!(matches!(
            Envelope::parse(body.as_bytes()).unwrap_err(),
            EnvelopeError::TooManyItems { .. }
        ));
    }

    #[test]
    fn a_missing_trailing_newline_is_accepted() {
        // Real SDKs vary on this.
        let body = b"{}\n{\"type\":\"event\",\"length\":2}\n{}";
        let envelope = Envelope::parse(body).unwrap();
        assert_eq!(envelope.items.len(), 1);
        assert_eq!(envelope.items[0].payload, b"{}");
    }

    #[test]
    fn dropping_items_leaves_a_parseable_envelope() {
        let body = b"{}\n{\"type\":\"event\",\"length\":2}\n{}\n{\"type\":\"transaction\",\"length\":2}\n{}\n";
        let mut envelope = Envelope::parse(body).unwrap();
        envelope.retain_items(|i| i.item_type != ItemType::Transaction);
        assert_eq!(envelope.items.len(), 1);
        let reparsed = Envelope::parse(&envelope.to_bytes()).unwrap();
        assert_eq!(reparsed.items.len(), 1);
        assert_eq!(reparsed.items[0].item_type, ItemType::Event);
    }

    #[test]
    fn rate_limit_categories_match_sentrys_names() {
        assert_eq!(ItemType::Event.rate_limit_category(), "error");
        assert_eq!(ItemType::Transaction.rate_limit_category(), "transaction");
        assert_eq!(ItemType::ReplayRecording.rate_limit_category(), "replay");
    }
}
