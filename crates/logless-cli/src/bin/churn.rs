//! Template churn across releases: one Drain carried through a sequence of
//! per-release line files, as an agent's catalog is in production.
//!
//! usage: churn <out.json> <release.log>...   (releases in order)
//!
//! Emits, per release: the catalog before and after (id, content fingerprint,
//! text, lineage id), per-id event counts, ids minted, and, for every input line, the id
//! it landed on (`<release.log>.ids`, one per line, `-` when untemplated).
use std::collections::BTreeMap;
use std::fmt::Write as _;

use logless_core::drain::{Drain, DrainConfig};

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn snapshot(d: &Drain) -> String {
    let mut ts: Vec<_> = d.templates().collect();
    ts.sort_by_key(|t| t.id);
    let mut s = String::from("[");
    for (i, t) in ts.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "[{},\"{:016x}\",\"{}\",\"{:016x}\"]", t.id, t.fingerprint(), esc(&t.text()), t.lineage_id);
    }
    s.push(']');
    s
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (out, files) = args.split_first().expect("usage: churn <out.json> <release.log>...");
    let mut drain = Drain::new(DrainConfig::default());
    let mut json = String::from("[");
    for (n, f) in files.iter().enumerate() {
        let text = std::fs::read_to_string(f).expect("read release");
        let before = snapshot(&drain);
        let mut events: BTreeMap<u64, u64> = BTreeMap::new();
        let mut new_ids = Vec::new();
        let mut ids = String::new();
        let (mut lines, mut untemplated) = (0u64, 0u64);
        for line in text.lines() {
            lines += 1;
            match drain.add_line(line, 1_700_000_000 + n as u64) {
                Some(m) => {
                    *events.entry(m.template_id).or_default() += 1;
                    if m.is_new {
                        new_ids.push(m.template_id);
                    }
                    let _ = writeln!(ids, "{}", m.template_id);
                }
                None => {
                    untemplated += 1;
                    ids.push_str("-\n");
                }
            }
        }
        std::fs::write(format!("{f}.ids"), ids).expect("write ids");
        if n > 0 {
            json.push(',');
        }
        let ev: Vec<String> = events.iter().map(|(k, v)| format!("[{k},{v}]")).collect();
        let _ = write!(
            json,
            "{{\"release\":\"{}\",\"lines\":{lines},\"untemplated\":{untemplated},\"new_ids\":{:?},\"events\":[{}],\"before\":{before},\"after\":{}}}",
            esc(f),
            new_ids,
            ev.join(","),
            snapshot(&drain)
        );
    }
    json.push(']');
    std::fs::write(out, json).expect("write out");
}
