//! Benchmarks template mining against the real LogHub corpora.
//!
//! The synthetic bench measures throughput on lines we wrote ourselves, which
//! flatters both the throughput and the template count: real logs have
//! multi-line stack traces, inconsistent field ordering, and shapes that differ
//! by one optional clause. LogHub publishes ground-truth template counts for
//! each 2,000-line sample, so this reports ours next to theirs.
//!
//! Usage: loghub <directory of *.log and *_templates.csv>
use std::path::Path;
use std::time::Instant;

use logless_core::drain::{Drain, DrainConfig};

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: loghub <corpus-dir>");
        std::process::exit(2);
    });
    let dir = Path::new(&dir);

    let mut datasets: Vec<String> = std::fs::read_dir(dir)
        .expect("corpus directory")
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.strip_suffix(".log").map(str::to_string)
        })
        .collect();
    datasets.sort();

    // A sweep, not a single number: the similarity threshold is the one knob
    // that decides over- versus under-merging, and its default was picked on
    // synthetic data.
    let sweep: Vec<f64> = std::env::var("SWEEP")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.split(',').filter_map(|v| v.parse().ok()).collect())
        .unwrap_or_else(|| vec![DrainConfig::default().similarity_threshold]);

    for threshold in &sweep {
        run(dir, &datasets, *threshold);
    }
}

fn run(dir: &Path, datasets: &[String], threshold: f64) {
    println!("\n--- similarity_threshold = {threshold} ---");
    println!(
        "{:<14} {:>7} {:>9} {:>9} {:>8} {:>12}",
        "dataset", "lines", "ours", "truth", "ratio", "lines/s"
    );
    let (mut total_lines, mut total_nanos) = (0u64, 0u128);
    let mut ratios = Vec::new();
    for dataset in datasets {
        let text = match std::fs::read_to_string(dir.join(format!("{dataset}.log"))) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let truth = std::fs::read_to_string(dir.join(format!("{dataset}_templates.csv")))
            .map(|t| t.lines().count().saturating_sub(1))
            .unwrap_or(0);

        let mut drain = Drain::new(DrainConfig {
            similarity_threshold: threshold,
            ..DrainConfig::default()
        });
        let start = Instant::now();
        let mut lines = 0u64;
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            drain.add_line(line, 0);
            lines += 1;
        }
        let elapsed = start.elapsed();
        total_lines += lines;
        total_nanos += elapsed.as_nanos();

        let ours = drain.template_count();
        if truth > 0 {
            ratios.push(ours as f64 / truth as f64);
        }
        println!(
            "{:<14} {:>7} {:>9} {:>9} {:>7.2}x {:>12.0}",
            dataset,
            lines,
            ours,
            truth,
            if truth == 0 { 0.0 } else { ours as f64 / truth as f64 },
            lines as f64 / elapsed.as_secs_f64()
        );
    }
    // Geometric mean, because the error is multiplicative: 0.5x and 2.0x are
    // equally wrong, and an arithmetic mean would call them 1.25x on average.
    let geo = (ratios.iter().map(|r| r.ln()).sum::<f64>() / ratios.len() as f64).exp();
    let within_2x = ratios.iter().filter(|r| **r >= 0.5 && **r <= 2.0).count();
    println!(
        "\ntotal: {total_lines} lines at {:.0} lines/s/core",
        total_lines as f64 / (total_nanos as f64 / 1e9)
    );
    println!(
        "geometric-mean ratio {geo:.2}x, {within_2x}/{} datasets within 2x of ground truth",
        ratios.len()
    );
}
