//! Measures what the pushdown dedupe defaults should be, from real logs.
//!
//! `dedupe_window` (60s) and `windows_per_minute` (3) were guesses. They decide
//! how much context reaches the vendor during an incident, so guessing wrong is
//! expensive in both directions: too short and every recurrence ships a full
//! context window; too long and a genuinely new occurrence is suppressed.
//!
//! Wants a corpus with real, contiguous timestamps. LogHub's BGL sample is
//! subsampled across 213 days, so its inter-arrival gaps are fiction; its
//! Thunderbird sample is contiguous. The file format assumed here is LogHub's:
//! the second whitespace field is a unix timestamp.
//!
//! Usage: dedupe-stats <file.log> [more.log ...]
use std::collections::HashMap;

use logless_core::drain::{Drain, DrainConfig};

fn main() {
    let files: Vec<String> = std::env::args().skip(1).collect();
    if files.is_empty() {
        eprintln!("usage: dedupe-stats <loghub-format file> ...");
        std::process::exit(2);
    }

    for path in files {
        let Ok(text) = std::fs::read_to_string(&path) else {
            eprintln!("cannot read {path}");
            continue;
        };
        let mut drain = Drain::new(DrainConfig::default());
        // template id -> the times it was seen, in order.
        let mut seen: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut first_seen: Vec<u64> = Vec::new();
        let (mut lines, mut skipped) = (0u64, 0u64);

        for line in text.lines() {
            let mut fields = line.split_whitespace();
            let _label = fields.next();
            let Some(ts) = fields.next().and_then(|t| t.parse::<u64>().ok()) else {
                skipped += 1;
                continue;
            };
            lines += 1;
            if let Some(m) = drain.add_line(line, ts) {
                let times = seen.entry(m.template_id).or_default();
                if times.is_empty() {
                    first_seen.push(ts);
                }
                times.push(ts);
            }
        }

        let span = match (first_seen.iter().min(), seen.values().flatten().max()) {
            (Some(lo), Some(hi)) => hi.saturating_sub(*lo),
            _ => 0,
        };
        println!("\n=== {path} ===");
        println!("{lines} usable lines ({skipped} without a timestamp), span {span}s, {} templates",
            seen.len());
        if span == 0 {
            println!("no usable time span");
            continue;
        }

        // Inter-arrival gaps between repeats of the same template. This is the
        // quantity `dedupe_window` is really about: how long after seeing a
        // shape are you still seeing the same incident?
        let mut gaps: Vec<u64> = Vec::new();
        for times in seen.values() {
            for pair in times.windows(2) {
                gaps.push(pair[1].saturating_sub(pair[0]));
            }
        }
        gaps.sort_unstable();
        if gaps.is_empty() {
            println!("no repeats; nothing to dedupe");
            continue;
        }
        let pct = |p: f64| gaps[((gaps.len() - 1) as f64 * p) as usize];
        println!(
            "repeat gaps (s): p50={} p75={} p90={} p95={} p99={} max={}",
            pct(0.50), pct(0.75), pct(0.90), pct(0.95), pct(0.99), gaps[gaps.len() - 1]
        );
        for window in [10u64, 30, 60, 120, 300] {
            let covered = gaps.iter().filter(|g| **g <= window).count();
            println!(
                "  a {window:>3}s window collapses {:.1}% of repeats",
                100.0 * covered as f64 / gaps.len() as f64
            );
        }

        // Distinct shapes appearing per minute, which is what
        // `windows_per_minute` bounds. Reported as the busiest minute, because
        // that is the minute an incident happens in.
        let mut per_minute: HashMap<u64, u64> = HashMap::new();
        for ts in &first_seen {
            *per_minute.entry(ts / 60).or_default() += 1;
        }
        let mut counts: Vec<u64> = per_minute.values().copied().collect();
        counts.sort_unstable();
        println!(
            "new shapes per minute: median={} p90={} busiest={}",
            counts[counts.len() / 2],
            counts[(counts.len() as f64 * 0.9) as usize % counts.len()],
            counts[counts.len() - 1]
        );
    }
}
