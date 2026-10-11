//! Template ids at the boundary are content hashes (`docs/design/multi-tenancy-and-indexing.md` section 6).
//!
//! Two edges mine the same corpus with different local counters (they met the shapes in a different
//! order), their records reach one central, and central must hold one id per shape: per-shape row counts
//! equal what the edges sent, and `GROUP BY template_id` gives one group per shape.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use logless_core::config::Tenancy;
use logless_core::drain::{fingerprint_of, Drain, DrainConfig};
use logless_core::sync_policy::{Planner, PlannerConfig};
use logless_core::typed::TypedColumnsConfig;
use logless_core::{merge, scan, template_dict, wal, AttrValue, Catalog, LogRecord, Severity};
use uuid::Uuid;

const SHAPES: [&str; 4] = [
    "handled request id={} user=u{} latency_ms={}",
    "cache lookup key=order:{} hit=true",
    "payment gateway timeout after {}ms for order {}",
    "connection pool exhausted size={} waiters={}",
];

/// What an edge does to a record at ingest: mine its body and label it. Returns the miner's own counter, which
/// is local to the edge and which nothing may store or send.
fn mint(drain: &mut Drain, record: &mut LogRecord) -> u64 {
    let m = drain.add_line(&record.body, 100).expect("templated");
    record.template_id = Some(m.shape_id);
    m.template_id
}

/// One edge's records, met in `order`, `per_shape[shape]` rows of each. Returns the records and the shape each is.
fn edge(order: &[usize], per_shape: [u64; 4], shape_of: &mut HashMap<Uuid, usize>, counters: &mut HashMap<Uuid, u64>) -> Vec<LogRecord> {
    let mut drain = Drain::default();
    let mut out = Vec::new();
    let mut i = 0u64;
    for &shape in order {
        for _ in 0..per_shape[shape] {
            i += 1;
            let ns = (1_000 + i) * 1_000_000_000;
            let mut r = LogRecord::new(ns, Severity::INFO, SHAPES[shape].replace("{}", &(i * 7).to_string()));
            r.observed_unix_nano = ns;
            r.service = Some("api".into());
            counters.insert(r.event_id, mint(&mut drain, &mut r));
            shape_of.insert(r.event_id, shape);
            out.push(r);
        }
    }
    out
}

/// A store that `batches` were merged into with `drain`, as a central or an edge would.
struct Store {
    _tmp: tempfile::TempDir,
    dir: std::path::PathBuf,
    catalog: Catalog,
}

impl Store {
    fn rows(&self) -> Vec<LogRecord> {
        scan::scan(&self.dir, &scan::Query { limit: 1_000_000, ..Default::default() }).unwrap().records
    }
}

fn merged(batches: &[Vec<LogRecord>], drain: &mut Drain) -> Store {
    let tmp = tempfile::tempdir().unwrap();
    let (wal_dir, store_dir) = (tmp.path().join("wal"), tmp.path().join("store"));
    let mut w = wal::WalWriter::open(&wal_dir, 1 << 30, Duration::from_secs(60), 1 << 30).unwrap();
    for b in batches {
        w.append(b).unwrap();
    }
    drop(w);
    let mut catalog = Catalog::open_in_memory().unwrap();
    merge::merge_all(&wal_dir, &store_dir, &mut catalog, &Tenancy::default(), drain, &TypedColumnsConfig::default(), None, 0).unwrap();
    Store { _tmp: tmp, dir: store_dir, catalog }
}

fn central_rows(batches: &[Vec<LogRecord>]) -> Vec<LogRecord> {
    merged(batches, &mut Drain::default()).rows()
}

#[test]
fn two_edges_with_different_counters_agree_on_one_id_per_shape_at_central() {
    let (mut shape_of, mut counters) = (HashMap::new(), HashMap::new());
    let a = edge(&[0, 1, 2, 3], [10, 20, 30, 40], &mut shape_of, &mut counters);
    let b = edge(&[3, 2, 1, 0], [5, 6, 7, 8], &mut shape_of, &mut counters);
    // The two edges really do number the same shape differently...
    let counter = |rows: &[LogRecord], shape: usize| counters[&rows.iter().find(|r| shape_of[&r.event_id] == shape).unwrap().event_id];
    assert_ne!(counter(&a, 0), counter(&b, 0), "the corpus is only a test if the edges' counters disagree");
    // ...and the ids they send are the same.
    let sent = |rows: &[LogRecord], shape: usize| rows.iter().find(|r| shape_of[&r.event_id] == shape).unwrap().template_id;
    assert!((0..4).all(|shape| sent(&a, shape) == sent(&b, shape)));

    let rows = central_rows(&[a, b]);
    assert_eq!(rows.len(), 10 + 20 + 30 + 40 + 5 + 6 + 7 + 8);

    let mut ids_of_shape: BTreeMap<usize, BTreeSet<Option<u64>>> = BTreeMap::new();
    let mut rows_of_id: BTreeMap<Option<u64>, u64> = BTreeMap::new();
    for r in &rows {
        ids_of_shape.entry(shape_of[&r.event_id]).or_default().insert(r.template_id);
        *rows_of_id.entry(r.template_id).or_default() += 1;
    }
    for (shape, ids) in &ids_of_shape {
        assert_eq!(ids.len(), 1, "shape {shape} must have one id at central, has {ids:?}");
    }
    assert_eq!(rows_of_id.len(), SHAPES.len(), "GROUP BY template_id must give one group per shape: {rows_of_id:?}");
    eprintln!("central rows per template_id: {rows_of_id:?}");
    let expected: BTreeSet<u64> = [15, 26, 37, 48].into_iter().collect();
    assert_eq!(rows_of_id.values().copied().collect::<BTreeSet<_>>(), expected, "per-shape row counts are the sum of both edges");
}

fn rollup_count(r: &LogRecord) -> Option<u64> {
    r.attributes.iter().find(|a| a.key == "logless.rollup.count").and_then(|a| if let AttrValue::I64(n) = a.value { Some(n as u64) } else { None })
}

#[test]
fn rollups_for_one_shape_from_two_edges_add_up_at_central() {
    let mut shape_of = HashMap::new();
    let mut counters = HashMap::new();
    let per_edge = [([0usize, 1, 2, 3], [100u64, 200, 300, 400]), ([3, 2, 1, 0], [50, 60, 70, 80])];
    let mut sent_to_central = Vec::new();
    let mut ingested = [0u64; 4];
    for (n, (order, per_shape)) in per_edge.iter().enumerate() {
        let rows = edge(order, *per_shape, &mut shape_of, &mut counters);
        for r in &rows {
            ingested[shape_of[&r.event_id]] += 1;
        }
        // 1% fidelity: nearly everything is left out and counted in per-minute rollups.
        let cfg = PlannerConfig { fidelity: 1, min_severity: 0, anomaly_boost: false };
        let mut planner = Planner::new(format!("edge-{n}"), cfg, 0, []);
        let mut out = planner.process(5_000_000, &rows);
        out.extend(planner.tick(10_000_000, true));
        sent_to_central.push(out);
    }
    assert_eq!(ingested, [150, 260, 370, 480]);

    let rows = central_rows(&sent_to_central);
    let rollups: Vec<_> = rows.iter().filter(|r| rollup_count(r).is_some()).collect();
    assert!(rollups.len() >= 2 * SHAPES.len(), "every shape of every edge produced a rollup: {}", rollups.len());

    // The rollup records carry the shape's id, so the two edges' rollups for one shape are one group.
    let mut left_out: BTreeMap<u64, u64> = BTreeMap::new();
    for r in &rollups {
        *left_out.entry(r.template_id.expect("a rollup keeps its shape")).or_default() += rollup_count(r).unwrap();
    }
    assert_eq!(left_out.len(), SHAPES.len(), "one rollup group per shape across both edges: {left_out:?}");

    // Exact: sent in full + left out = what the two edges ingested, for every shape, by id alone.
    let mut sent: BTreeMap<u64, u64> = BTreeMap::new();
    for r in rows.iter().filter(|r| rollup_count(r).is_none()) {
        *sent.entry(r.template_id.unwrap()).or_default() += 1;
    }
    let id_of_shape: Vec<u64> = (0..4)
        .map(|shape| {
            let mut d = Drain::default();
            d.add_line(&SHAPES[shape].replace("{}", "1"), 0).unwrap().shape_id
        })
        .collect();
    for shape in 0..4 {
        let id = id_of_shape[shape];
        let total = sent.get(&id).copied().unwrap_or(0) + left_out.get(&id).copied().unwrap_or(0);
        eprintln!("shape {shape}: id {id}: sent in full {} + left out {} = {total}; the edges ingested {}", sent.get(&id).copied().unwrap_or(0), left_out.get(&id).copied().unwrap_or(0), ingested[shape]);
        assert_eq!(total, ingested[shape], "shape {shape}: sent {:?} + left out {:?}", sent.get(&id), left_out.get(&id));
    }
    assert!(left_out.values().sum::<u64>() > 1_000, "most rows were rolled up, so the sum is the test: {left_out:?}");
}

#[test]
fn central_keeps_an_id_it_is_sent_and_mines_a_record_that_has_none() {
    // The edge's mined record, one with an id minted by something else, and a bare one.
    let mut shape_of = HashMap::new();
    let mut counters = HashMap::new();
    let mut from_edge = edge(&[0], [3, 0, 0, 0], &mut shape_of, &mut counters);
    let edge_id = from_edge[0].template_id.unwrap();
    let mut foreign = from_edge.pop().unwrap();
    foreign.event_id = Uuid::now_v7();
    foreign.template_id = Some(0xfeed_f00d);
    let mut bare = LogRecord::new(2_000 * 1_000_000_000, Severity::INFO, "cache lookup key=order:5 hit=true");
    bare.service = Some("api".into());
    assert!(bare.template_id.is_none());

    let store = merged(&[from_edge, vec![foreign, bare]], &mut Drain::default());
    let rows = store.rows();
    let ids: BTreeMap<Option<u64>, u64> = rows.iter().fold(BTreeMap::new(), |mut m, r| {
        *m.entry(r.template_id).or_default() += 1;
        m
    });
    let mined_here = Drain::default().add_line("cache lookup key=order:5 hit=true", 0).unwrap().shape_id;
    assert_eq!(ids, BTreeMap::from([(Some(edge_id), 2), (Some(0xfeed_f00d), 1), (Some(mined_here), 1)]), "{ids:?}");

    // Central can name what it mined and not what it was only sent: the dictionary has the one, the
    // sightings (which the sync planner reads) have both.
    let dict = template_dict::read(&template_dict::dict_path(&store.dir, "default", "default", "node-a"), "default", "default").unwrap();
    let texts: Vec<_> = dict.iter().map(|d| (d.template_id, d.count)).collect();
    assert_eq!(texts, vec![(mined_here, 1)] , "{dict:?}");
    assert!(store.catalog.load_template_services().unwrap().contains(&(Some("api".into()), 0xfeed_f00d)));
}

#[test]
fn widening_moves_the_id_keeps_the_lineage_and_the_exported_history_folds_them() {
    let mut drain = Drain::new(DrainConfig { similarity_threshold: 0.5, ..DrainConfig::default() });
    let mut rows = Vec::new();
    for (i, word) in ["alpha", "beta", "gamma"].iter().enumerate() {
        let ns = (1_000 + i as u64) * 1_000_000_000;
        let mut r = LogRecord::new(ns, Severity::INFO, format!("job run {word} finished ok in cache"));
        r.observed_unix_nano = ns;
        r.service = Some("api".into());
        mint(&mut drain, &mut r);
        rows.push(r);
    }
    let (first, widened) = (rows[0].template_id.unwrap(), rows[1].template_id.unwrap());
    assert_ne!(first, widened, "widening changes the id from that row on");
    assert_eq!(rows[2].template_id, Some(widened));
    assert_eq!(first, fingerprint_of("job run alpha finished ok in cache"));
    assert_eq!(widened, fingerprint_of("job run <*> finished ok in cache"));

    let store = merged(&[rows], &mut drain);
    let stored: Vec<_> = store.rows().iter().map(|r| r.template_id.unwrap()).collect();
    assert_eq!(stored, vec![first, widened, widened], "rows already written keep the hash they were written with");

    let node = Tenancy::default().node().to_string();
    let history = template_dict::read_history(&template_dict::history_path(&store.dir, "default", "default", &node), "default", "default").unwrap();
    assert_eq!(
        history.iter().map(|h| (h.lineage_id, h.template_id, h.count)).collect::<Vec<_>>(),
        vec![(first, first, 1), (first, widened, 2)],
        "one lineage, two hashes, linked"
    );
    let dict = template_dict::read(&template_dict::dict_path(&store.dir, "default", "default", &node), "default", "default").unwrap();
    assert_eq!(dict.iter().map(|d| (d.template_id, d.lineage_id)).collect::<Vec<_>>(), vec![(first, first), (widened, first)]);
    // What the Sentry forwarder fingerprints with is that lineage: the id the template was minted with.
    let counter = drain.templates().next().unwrap();
    assert_eq!(counter.lineage_id, first);
}
