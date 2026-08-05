//! Benchmarks template mining against the real LogHub corpora.
//!
//! The synthetic bench measures throughput on lines we wrote ourselves, which
//! flatters both the throughput and the template count: real logs have
//! multi-line stack traces, inconsistent field ordering, and shapes that differ
//! by one optional clause. LogHub publishes ground-truth template counts for
//! each 2,000-line sample, so this reports ours next to theirs.
//!
//! Reads LogHub's *structured* CSV — the `Content` column — not the raw line.
//! That is what the ground-truth templates describe: LogHub splits the header
//! (timestamp, level, component) out with a per-dataset log format before
//! templating. Comparing our raw-line templates against content-only ground
//! truth measures our masking of *their header format*, which is neither
//! interesting nor a fair test in either direction.
//!
//! Usage: loghub <directory of *_structured.csv and *_templates.csv>
//!   DUMP=<dataset> to print our templates for one dataset.
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
            name.strip_suffix("_structured.csv").map(str::to_string)
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

/// LogHub's Parsing Accuracy: the fraction of lines whose predicted group is
/// *exactly* the ground-truth group — same members, no more and no fewer.
///
/// Strictly harder than comparing template counts, which is why it is the one
/// worth reporting: a parser can produce exactly the right number of templates
/// while assigning the wrong lines to each, and count agreement would score
/// that a perfect 1.00.
fn parsing_accuracy(predicted: &[u64], truth: &[String]) -> f64 {
    use std::collections::HashMap;
    let mut predicted_groups: HashMap<u64, Vec<usize>> = HashMap::new();
    let mut truth_groups: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, (p, t)) in predicted.iter().zip(truth).enumerate() {
        predicted_groups.entry(*p).or_default().push(i);
        truth_groups.entry(t.as_str()).or_default().push(i);
    }
    let mut correct = 0usize;
    for members in predicted_groups.values() {
        // Every member must share one ground-truth label, and that label's
        // group must be exactly this set.
        let label = truth[members[0]].as_str();
        if members.iter().all(|i| truth[*i] == label)
            && truth_groups.get(label).is_some_and(|g| g.len() == members.len())
        {
            correct += members.len();
        }
    }
    correct as f64 / predicted.len().max(1) as f64
}

/// Reads the `Content` column out of LogHub's structured CSV.
///
/// Hand-rolled rather than a CSV crate: the only quoting that appears here is
/// a whole field wrapped in double quotes with `""` for an embedded quote, and
/// a dependency for that would be the tail wagging the dog.
fn read_labelled(path: &Path) -> std::io::Result<Vec<(String, String)>> {
    let text = std::fs::read_to_string(path)?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    let index = |name: &str, fallback: usize| {
        header
            .split(',')
            .position(|column| column.trim() == name)
            .unwrap_or(fallback)
    };
    let content_index = index("Content", 3);
    let event_index = index("EventId", 4);

    Ok(lines
        .filter_map(|line| {
            let fields = split_csv(line);
            Some((
                fields.get(content_index)?.clone(),
                fields.get(event_index).cloned().unwrap_or_default(),
            ))
        })
        .collect())
}

fn split_csv(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => fields.push(std::mem::take(&mut field)),
            other => field.push(other),
        }
    }
    fields.push(field);
    fields
}

fn run(dir: &Path, datasets: &[String], threshold: f64) {
    println!("\n--- similarity_threshold = {threshold} ---");
    println!(
        "{:<14} {:>7} {:>9} {:>9} {:>8} {:>9} {:>12}",
        "dataset", "lines", "ours", "truth", "ratio", "accuracy", "lines/s"
    );
    let (mut total_lines, mut total_nanos) = (0u64, 0u128);
    let mut ratios = Vec::new();
    let mut accuracies: Vec<f64> = Vec::new();
    for dataset in datasets {
        let Ok(rows) = read_labelled(&dir.join(format!("{dataset}_structured.csv"))) else {
            continue;
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
        // Predicted cluster per line, alongside the ground-truth label, so the
        // partitions can be compared rather than just their sizes.
        let mut predicted: Vec<u64> = Vec::new();
        let mut truth_labels: Vec<String> = Vec::new();
        for (content, label) in &rows {
            if content.trim().is_empty() {
                continue;
            }
            match drain.add_line(content, 0) {
                Some(m) => predicted.push(m.template_id),
                // An untemplated line is its own singleton cluster, which is
                // what it is: nothing was grouped with it.
                None => predicted.push(u64::MAX - lines),
            }
            truth_labels.push(label.clone());
            lines += 1;
        }
        let accuracy = parsing_accuracy(&predicted, &truth_labels);
        let elapsed = start.elapsed();
        total_lines += lines;
        total_nanos += elapsed.as_nanos();

        if std::env::var("DUMP").is_ok_and(|d| d == *dataset) {
            let mut templates: Vec<_> = drain.templates().collect();
            templates.sort_by_key(|t| std::cmp::Reverse(t.count));
            println!("\n-- our templates for {dataset} --");
            for template in templates {
                println!("  {:>6}  {}", template.count, template.text());
            }
            println!();
        }
        let ours = drain.template_count();
        if truth > 0 {
            ratios.push(ours as f64 / truth as f64);
        }
        accuracies.push(accuracy);
        println!(
            "{:<14} {:>7} {:>9} {:>9} {:>7.2}x {:>9.3} {:>12.0}",
            dataset,
            lines,
            ours,
            truth,
            if truth == 0 { 0.0 } else { ours as f64 / truth as f64 },
            accuracy,
            lines as f64 / elapsed.as_secs_f64()
        );
    }
    // Geometric mean, because the error is multiplicative: 0.5x and 2.0x are
    // equally wrong, and an arithmetic mean would call them 1.25x on average.
    let geo = (ratios.iter().map(|r| r.ln()).sum::<f64>() / ratios.len() as f64).exp();
    // Mean absolute log-ratio: how far each dataset is from ground truth
    // regardless of direction. The geometric mean alone can sit at 1.00 while
    // half the datasets over-split and half under-merge — the errors cancel and
    // the number flatters. Lower is better; this is the one to optimise.
    let dispersion = ratios.iter().map(|r| r.ln().abs()).sum::<f64>() / ratios.len() as f64;
    let within_2x = ratios.iter().filter(|r| **r >= 0.5 && **r <= 2.0).count();
    println!(
        "\ntotal: {total_lines} lines at {:.0} lines/s/core",
        total_lines as f64 / (total_nanos as f64 / 1e9)
    );
    let mean_accuracy = accuracies.iter().sum::<f64>() / accuracies.len() as f64;
    println!(
        "geometric-mean ratio {geo:.2}x, dispersion {dispersion:.3} (lower is better), \
         {within_2x}/{} datasets within 2x",
        ratios.len()
    );
    println!(
        "mean parsing accuracy {mean_accuracy:.3} \
         (fraction of lines whose group exactly matches the ground-truth group)"
    );
}
