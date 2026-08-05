//! Masking heuristics — the step before template mining.
//!
//! Drain groups lines by their *static* tokens, so anything obviously variable
//! has to be neutralised first or every request id becomes its own template.
//! `docs/architecture.md` §3: masking heuristics replace the LLM regex
//! derivation entirely. They are deterministic, offline, and cost nothing.
//!
//! **Token-classified, not regex-scanned.** Drain3 runs a list of regexes over
//! the whole line; we classify whitespace-separated tokens with hand-written
//! character checks. Same outcome, far cheaper — and templating must not become
//! the bottleneck, or the whole feature gets switched off.
//!
//! Placeholders are typed (`<NUM>`, `<IP>`, …) rather than a bare `<*>`. The
//! type is what later makes typed parameter columns possible, which is where
//! predicate pushdown like `latency_ms > 500` comes from.

/// A masked token: either literal static text, or a typed placeholder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mask {
    /// Not variable — part of the template.
    Static,
    Num,
    Uuid,
    Hex,
    Ip,
    Time,
    Path,
    Quoted,
    /// Contains digits but fits no specific shape (`u4567`, `req-42`).
    Var,
}

impl Mask {
    pub fn placeholder(self) -> &'static str {
        match self {
            Mask::Static => "",
            Mask::Num => "<NUM>",
            Mask::Uuid => "<UUID>",
            Mask::Hex => "<HEX>",
            Mask::Ip => "<IP>",
            Mask::Time => "<TIME>",
            Mask::Path => "<PATH>",
            Mask::Quoted => "<STR>",
            Mask::Var => "<VAR>",
        }
    }

    pub fn is_variable(self) -> bool {
        self != Mask::Static
    }
}

/// Classify one whitespace-separated token.
///
/// Order matters: the most specific shapes are tested first, so a UUID is not
/// reported as generic hex and an IP is not reported as a number.
pub fn classify(token: &str) -> Mask {
    if token.is_empty() {
        return Mask::Static;
    }

    // `key=value` — keep the key (it is static and highly discriminating),
    // classify only the value. Handled by the caller via `split_kv`.
    let body = token;

    if is_quoted(body) {
        return Mask::Quoted;
    }
    if is_uuid(body) {
        return Mask::Uuid;
    }
    if is_timestamp(body) {
        return Mask::Time;
    }
    if is_ip(body) {
        return Mask::Ip;
    }
    // Long all-digit tokens are identifiers, not quantities. A 32-character
    // decimal number does not occur in practice; a 32-character id does, every
    // request. Checking hex first at that length keeps the placeholder honest.
    if body.len() >= 16 && is_hex(body) {
        return Mask::Hex;
    }
    if is_number(body) {
        return Mask::Num;
    }
    if is_hex(body) {
        return Mask::Hex;
    }
    if is_path(body) {
        return Mask::Path;
    }
    if body.chars().any(|c| c.is_ascii_digit()) {
        return Mask::Var;
    }
    Mask::Static
}

/// Split `key=value` into its parts. Returns `None` when the token is not a
/// key/value pair, or when the key itself looks variable.
pub fn split_kv(token: &str) -> Option<(&str, &str)> {
    let (key, value) = token.split_once('=')?;
    if key.is_empty() || value.is_empty() {
        return None;
    }
    // `a=b=c` is not a clean pair; and a key with digits is usually itself data.
    if key.chars().any(|c| c.is_ascii_digit() || c == '"') {
        return None;
    }
    Some((key, value))
}

/// Mask a whole line into template tokens.
///
/// `tokens` receives the masked form; `raw` receives the *parameter candidate*
/// at the same index — the value for `key=value`, the punctuation-stripped core
/// otherwise. Not the original token: a parameter is `4711`, not `id=4711,`.
///
/// The two stay index-aligned because template mining creates *additional*
/// wildcard positions when it merges two lines, and the parameter at such a
/// position can only be recovered from this side channel.
pub fn mask_line(line: &str, tokens: &mut Vec<String>, raw: &mut Vec<String>) {
    tokens.clear();
    raw.clear();
    for raw_token in line.split_whitespace() {
        // Strip trailing punctuation that would otherwise fuse into the token
        // and defeat classification (`id=42,` / `done.`).
        let (core, trailing) = split_trailing_punctuation(raw_token);

        if let Some((key, value)) = split_kv(core) {
            raw.push(value.to_string());
            let mask = classify(value);
            if mask.is_variable() {
                tokens.push(format!("{key}={}{trailing}", mask.placeholder()));
            } else {
                tokens.push(format!("{core}{trailing}"));
            }
            continue;
        }

        raw.push(core.to_string());
        let mask = classify(core);
        if mask.is_variable() {
            tokens.push(format!("{}{trailing}", mask.placeholder()));
        } else {
            tokens.push(raw_token.to_string());
        }
    }
}

fn split_trailing_punctuation(token: &str) -> (&str, &str) {
    let trimmed = token.trim_end_matches([',', ';', '.', ')', ']', '}', ':']);
    // Never strip everything away: a lone "..." is static text, not an empty token.
    if trimmed.is_empty() {
        return (token, "");
    }
    (trimmed, &token[trimmed.len()..])
}

fn is_quoted(t: &str) -> bool {
    t.len() >= 2
        && ((t.starts_with('"') && t.ends_with('"')) || (t.starts_with('\'') && t.ends_with('\'')))
}

fn is_number(t: &str) -> bool {
    let t = t.strip_prefix(['-', '+']).unwrap_or(t);
    if t.is_empty() {
        return false;
    }
    let mut seen_digit = false;
    let mut seen_dot = false;
    for c in t.chars() {
        match c {
            '0'..='9' => seen_digit = true,
            '.' if !seen_dot => seen_dot = true,
            // Trailing unit suffixes are part of the value, not the template.
            'm' | 's' | 'h' | 'd' | 'B' | 'K' | 'M' | 'G' if seen_digit => {}
            _ => return false,
        }
    }
    seen_digit
}

fn is_hex(t: &str) -> bool {
    let t = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")).unwrap_or(t);
    // Short hex is indistinguishable from an ordinary word ("deed", "face").
    t.len() >= 8 && t.chars().all(|c| c.is_ascii_hexdigit())
}

fn is_uuid(t: &str) -> bool {
    let t = t.trim_matches(['{', '}']);
    if t.len() != 36 {
        return false;
    }
    let groups = [8usize, 4, 4, 4, 12];
    let mut parts = t.split('-');
    for expected in groups {
        match parts.next() {
            Some(p) if p.len() == expected && p.chars().all(|c| c.is_ascii_hexdigit()) => {}
            _ => return false,
        }
    }
    parts.next().is_none()
}

fn is_ip(t: &str) -> bool {
    // Optional :port suffix — `10.0.0.1:8080` is one variable, not two.
    let host = t.split_once(':').map(|(h, p)| {
        if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() {
            h
        } else {
            t
        }
    });
    let host = host.unwrap_or(t);
    let mut octets = 0;
    for part in host.split('.') {
        if part.is_empty() || part.len() > 3 || !part.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
        if part.parse::<u16>().unwrap_or(999) > 255 {
            return false;
        }
        octets += 1;
    }
    octets == 4
}

fn is_timestamp(t: &str) -> bool {
    // ISO-8601-ish: starts with a date, or is a bare clock time.
    let bytes = t.as_bytes();
    let iso_date = bytes.len() >= 10
        && bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit);
    if iso_date {
        return true;
    }
    // HH:MM:SS(.frac)
    let mut parts = t.split(':');
    let ok = (0..3).all(|_| {
        parts
            .next()
            .is_some_and(|p| !p.is_empty() && p.split('.').next().is_some_and(|q| {
                q.len() <= 2 && !q.is_empty() && q.chars().all(|c| c.is_ascii_digit())
            }))
    });
    ok && parts.next().is_none()
}

fn is_path(t: &str) -> bool {
    (t.starts_with('/') || t.starts_with("./") || t.starts_with("../")) && t.len() > 1
}

/// True if a masked token contains a placeholder, i.e. this position is
/// variable. Also recognises the bare `<*>` that template merging introduces.
pub fn is_placeholder(masked_token: &str) -> bool {
    masked_token.contains('<') && masked_token.contains('>')
}

/// Original text at every variable position, in order.
pub fn variable_params(tokens: &[String], raw: &[String]) -> Vec<String> {
    tokens
        .iter()
        .zip(raw)
        .filter(|(t, _)| is_placeholder(t))
        .map(|(_, r)| r.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn masked(line: &str) -> (Vec<String>, Vec<String>) {
        let (mut t, mut r) = (Vec::new(), Vec::new());
        mask_line(line, &mut t, &mut r);
        let params = variable_params(&t, &r);
        (t, params)
    }

    #[test]
    fn classifies_the_common_shapes() {
        assert_eq!(classify("42"), Mask::Num);
        assert_eq!(classify("-3.14"), Mask::Num);
        assert_eq!(classify("250ms"), Mask::Num);
        assert_eq!(classify("550e8400-e29b-41d4-a716-446655440000"), Mask::Uuid);
        assert_eq!(classify("deadbeefcafe1234"), Mask::Hex);
        assert_eq!(classify("0xDEADBEEF"), Mask::Hex);
        assert_eq!(classify("10.0.0.1"), Mask::Ip);
        assert_eq!(classify("10.0.0.1:8080"), Mask::Ip);
        assert_eq!(classify("2026-08-04T17:20:31"), Mask::Time);
        assert_eq!(classify("17:20:31.123"), Mask::Time);
        assert_eq!(classify("/var/log/app.log"), Mask::Path);
        assert_eq!(classify("\"some text\""), Mask::Quoted);
        assert_eq!(classify("u4567"), Mask::Var);
        // A long all-digit token is an id, not a quantity.
        assert_eq!(classify("00000000000000000000000000000003"), Mask::Hex);
        assert_eq!(classify("1234567890123456"), Mask::Hex);
        // Ordinary numbers, even large ones, stay numbers.
        assert_eq!(classify("123456789"), Mask::Num);
        assert_eq!(classify("4181"), Mask::Num);
    }

    #[test]
    fn leaves_ordinary_words_alone() {
        for word in ["handled", "request", "completed", "GET", "error", "café"] {
            assert_eq!(classify(word), Mask::Static, "{word} should be static");
        }
        // Short hex-looking words are real words far more often than they are hex.
        assert_eq!(classify("deed"), Mask::Static);
        assert_eq!(classify("face"), Mask::Static);
        // A version number is data, but a bare dotted word is not an IP.
        assert_eq!(classify("app.log"), Mask::Static);
    }

    #[test]
    fn keeps_the_key_and_masks_the_value() {
        let (tokens, params) = masked("handled request id=4711 latency_ms=812 path=/v1/thing");
        assert_eq!(
            tokens,
            vec!["handled", "request", "id=<NUM>", "latency_ms=<NUM>", "path=<PATH>"]
        );
        assert_eq!(params, vec!["4711", "812", "/v1/thing"]);
    }

    #[test]
    fn two_lines_differing_only_in_data_mask_identically() {
        // The whole point: these must produce one template, not two.
        let (a, pa) = masked("[api] handled request id=12 user=u2988 latency_ms=801");
        let (b, pb) = masked("[api] handled request id=99999 user=u17 latency_ms=3");
        assert_eq!(a, b);
        assert_ne!(pa, pb);
        assert_eq!(pa.len(), 3);
    }

    #[test]
    fn trailing_punctuation_does_not_defeat_classification() {
        let (tokens, params) = masked("connected to 10.0.0.1, retries=3.");
        assert_eq!(tokens, vec!["connected", "to", "<IP>,", "retries=<NUM>."]);
        assert_eq!(params, vec!["10.0.0.1", "3"]);
    }

    #[test]
    fn punctuation_only_tokens_survive() {
        let (tokens, _) = masked("done ... ok");
        assert_eq!(tokens, vec!["done", "...", "ok"]);
    }

    #[test]
    fn empty_and_whitespace_lines_are_harmless() {
        assert_eq!(masked("").0, Vec::<String>::new());
        assert_eq!(masked("   \t ").0, Vec::<String>::new());
    }

    #[test]
    fn rejects_malformed_uuids_and_ips() {
        assert_ne!(classify("550e8400-e29b-41d4-a716-44665544000"), Mask::Uuid);
        assert_ne!(classify("999.1.1.1"), Mask::Ip);
        assert_ne!(classify("1.2.3"), Mask::Ip);
    }
}
