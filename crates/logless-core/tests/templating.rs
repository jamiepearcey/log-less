//! Template-mining behaviour that is easy to regress and expensive to notice.
//!
//! These assert *counts*, not shapes: the failure mode that matters is a change
//! to masking or similarity that quietly forks one logical shape into many, or
//! collapses distinct shapes into one. Both look fine in a unit test of the
//! masker and only show up in aggregate.

use logless_core::drain::{Drain, DrainConfig};

const SHAPES: [&str; 5] = [
    "handled request id={} user=u{} latency_ms={}",
    "cache lookup key=order:{} hit=true",
    "payment gateway timeout after {}ms for order {}",
    "retrying charge for order {} attempt {}",
    "connection pool exhausted size={} waiters={}",
];

#[test]
fn one_shape_logged_by_many_components_stays_one_template() {
    // Measured before the component mask: 15 templates for these 5 shapes.
    let mut drain = Drain::new(DrainConfig::default());
    for i in 0..200 {
        for component in ["api", "worker", "auth"] {
            for shape in SHAPES {
                drain.add_line(&format!("[{component}] {}", shape.replace("{}", &i.to_string())), 0);
            }
        }
    }
    assert_eq!(
        drain.template_count(),
        SHAPES.len(),
        "one logical shape per template, regardless of which component logged it"
    );
}

#[test]
fn distinct_shapes_do_not_collapse_into_each_other() {
    // The opposite failure: a similarity threshold loose enough to merge
    // everything would also report one template, so both directions are pinned.
    let mut drain = Drain::new(DrainConfig::default());
    for i in 0..50 {
        for shape in SHAPES {
            drain.add_line(&shape.replace("{}", &i.to_string()), 0);
        }
    }
    assert_eq!(drain.template_count(), SHAPES.len());
}

#[test]
fn a_bracketed_level_still_separates_shapes() {
    // `[ERROR] x` and `[INFO] x` are different shapes; merging them would put
    // an error's template — and so its fingerprint — on an info line.
    let mut drain = Drain::new(DrainConfig::default());
    for i in 0..20 {
        drain.add_line(&format!("[ERROR] disk full on volume {i}"), 0);
        drain.add_line(&format!("[INFO] disk full on volume {i}"), 0);
    }
    assert_eq!(drain.template_count(), 2);
}
