//! Typed attribute columns: promoting stable, frequent attribute keys out of
//! the `attributes` JSON so consumers can push predicates into Parquet.
//!
//! `attributes` is JSON text, which no engine can prune on: `latency_ms > 500`
//! means reading and parsing every row's JSON. A key that a service sets on
//! most of its records, always with the same type, is promoted to its own
//! column (`attr_latency_ms BIGINT`) with row-group statistics and dictionary
//! encoding, and its values leave the JSON.
//!
//! The contract (`docs/typed-columns.md`) in one paragraph:
//!
//! * A column is named `attr_` + the key lowercased, with every byte outside
//!   `[a-z0-9_]` replaced by `_`. Keys that are empty, longer than
//!   [`MAX_KEY_BYTES`], have no ASCII alphanumeric, or would collide with an
//!   existing column are never promoted.
//! * A column has exactly one type for the life of the store: `int64`,
//!   `float64`, `bool`, `string` or `timestamp` (ns, UTC). The registry in the
//!   catalog never changes a type, which is what makes reading many files with
//!   `union_by_name` safe.
//! * A value that does not conform to its column's type stays in the JSON, so
//!   nothing is ever lost or coerced into a wrong value. The typed column is
//!   NULL on that row.
//! * The set is bounded: at most `max_per_service` promotions are charged to
//!   any one service and at most `max_columns` exist in total.
//! * A file contains only the typed columns that have at least one value in
//!   it. Files written before a promotion simply lack the column, so readers
//!   must union schemas by name.
//!
//! Every typed field carries Arrow field metadata `logless.attr_key` (the
//! original key) and `logless.attr_type`, and every file lists its typed
//! columns in the Parquet key-value metadata `logless.typed_columns`, so the
//! registry is recoverable from footers like the rest of the catalog.

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::datatypes::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::model::{AttrValue, LogRecord};

/// Prefix for every typed attribute column. No base column starts with it.
pub const COLUMN_PREFIX: &str = "attr_";
/// Keys longer than this are not promoted: a column name is an identifier
/// people type, and very long keys are usually generated, not stable.
pub const MAX_KEY_BYTES: usize = 64;
/// Arrow field metadata: the original attribute key.
pub const FIELD_META_KEY: &str = "logless.attr_key";
/// Arrow field metadata: the column type name.
pub const FIELD_META_TYPE: &str = "logless.attr_type";
/// Parquet key-value metadata listing the file's typed columns as JSON.
pub const FILE_META_KEY: &str = "logless.typed_columns";

/// Integers whose magnitude exceeds this cannot be represented exactly as an
/// `f64`; they stay in the JSON rather than land in a float column rounded.
const F64_EXACT_INT: i64 = 1 << 53;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Int64,
    Float64,
    Bool,
    String,
    Timestamp,
}

impl ColumnType {
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnType::Int64 => "int64",
            ColumnType::Float64 => "float64",
            ColumnType::Bool => "bool",
            ColumnType::String => "string",
            ColumnType::Timestamp => "timestamp",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "int64" => ColumnType::Int64,
            "float64" => ColumnType::Float64,
            "bool" => ColumnType::Bool,
            "string" => ColumnType::String,
            "timestamp" => ColumnType::Timestamp,
            _ => return None,
        })
    }

    pub fn arrow_type(self) -> DataType {
        match self {
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::Bool => DataType::Boolean,
            ColumnType::String => DataType::Utf8,
            ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        }
    }
}

/// One registered typed column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedColumn {
    pub column: String,
    pub key: String,
    #[serde(rename = "type")]
    pub ty: ColumnType,
    /// The service whose records caused the promotion; its budget is charged.
    /// Once promoted, the column holds conforming values from every service.
    pub service: String,
    pub promoted_at: u64,
}

/// The registry snapshot a writer applies: every promoted key, sorted by
/// column name so field order in a file is deterministic.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TypedLayout {
    columns: Vec<TypedColumn>,
    by_key: HashMap<String, usize>,
}

impl TypedLayout {
    pub fn new(mut columns: Vec<TypedColumn>) -> Self {
        columns.sort_by(|a, b| a.column.cmp(&b.column));
        columns.dedup_by(|a, b| a.column == b.column);
        let by_key = columns.iter().enumerate().map(|(i, c)| (c.key.clone(), i)).collect();
        Self { columns, by_key }
    }

    pub fn columns(&self) -> &[TypedColumn] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn index_of(&self, key: &str) -> Option<usize> {
        self.by_key.get(key).copied()
    }

    pub fn has_column(&self, column: &str) -> bool {
        self.columns.iter().any(|c| c.column == column)
    }

    pub fn charged_to(&self, service: &str) -> usize {
        self.columns.iter().filter(|c| c.service == service).count()
    }

    pub fn extend(&mut self, new: Vec<TypedColumn>) {
        let mut all = std::mem::take(&mut self.columns);
        all.extend(new);
        *self = TypedLayout::new(all);
    }
}

/// Promotion thresholds, `[typed_columns]` in the config.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TypedColumnsConfig {
    /// Most promotions charged to one service. 0 promotes nothing.
    pub max_per_service: usize,
    /// Most typed columns in the store, across all services.
    pub max_columns: usize,
    /// A service needs at least this many records in one written batch before
    /// any of its keys is judged; fewer is not evidence of "frequently seen".
    pub min_rows: usize,
    /// Fraction of a service's records that must carry the key.
    pub min_presence: f64,
    /// Fraction of the key's values that must conform to one type.
    pub min_type_share: f64,
    /// String keys whose mean value length exceeds this are left in the JSON:
    /// blobs gain nothing from a column and inflate every file.
    pub max_string_bytes: usize,
}

impl Default for TypedColumnsConfig {
    fn default() -> Self {
        Self {
            max_per_service: 16,
            max_columns: 128,
            min_rows: 200,
            min_presence: 0.5,
            min_type_share: 0.95,
            max_string_bytes: 256,
        }
    }
}

/// `attr_` + sanitised key, or `None` when the key is not promotable.
pub fn column_name(key: &str) -> Option<String> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES || !key.bytes().any(|b| b.is_ascii_alphanumeric()) {
        return None;
    }
    let mut out = String::with_capacity(COLUMN_PREFIX.len() + key.len());
    out.push_str(COLUMN_PREFIX);
    for b in key.bytes() {
        let c = b.to_ascii_lowercase();
        out.push(if c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' { c as char } else { '_' });
    }
    Some(out)
}

/// A value converted for a typed column.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Cell<'a> {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(&'a str),
    Timestamp(i64),
}

/// The value as it would be stored in a column of type `ty`, or `None` when
/// it does not conform and must stay in the JSON.
///
/// Conformance is chosen so a stored value replays as the value that arrived:
/// an integer goes into a float column only when exact, and a string goes into
/// a timestamp column only when it is already in canonical form.
pub fn conform(value: &AttrValue, ty: ColumnType) -> Option<Cell<'_>> {
    match (ty, value) {
        (ColumnType::Int64, AttrValue::I64(v)) => Some(Cell::Int(*v)),
        (ColumnType::Float64, AttrValue::F64(v)) => Some(Cell::Float(*v)),
        (ColumnType::Float64, AttrValue::I64(v)) if v.unsigned_abs() <= F64_EXACT_INT as u64 => {
            Some(Cell::Float(*v as f64))
        }
        (ColumnType::Bool, AttrValue::Bool(v)) => Some(Cell::Bool(*v)),
        (ColumnType::String, AttrValue::Str(s)) => Some(Cell::Str(s)),
        (ColumnType::Timestamp, AttrValue::Str(s)) => parse_canonical_timestamp(s).map(Cell::Timestamp),
        _ => None,
    }
}

/// Rows per [`plan`] call whose attributes are inspected; see the stride.
const SAMPLE_ROWS: usize = 32 * 1024;

#[derive(Default)]
struct ServiceStats<'a> {
    rows: usize,
    sampled: usize,
    keys: HashMap<&'a str, KeyStats>,
}

#[derive(Default)]
struct KeyStats {
    present: usize,
    ints: usize,
    floats: usize,
    bools: usize,
    timestamps: usize,
    strings: usize,
    string_bytes: usize,
}

impl KeyStats {
    fn observe(&mut self, value: &AttrValue) {
        self.present += 1;
        match value {
            AttrValue::I64(_) => self.ints += 1,
            AttrValue::F64(_) => self.floats += 1,
            AttrValue::Bool(_) => self.bools += 1,
            AttrValue::Str(s) => {
                self.strings += 1;
                self.string_bytes += s.len();
                if parse_canonical_timestamp(s).is_some() {
                    self.timestamps += 1;
                }
            }
        }
    }

    /// The single type at least `share` of the values conform to, if any.
    /// Floats win over ints when both appear, because SDKs serialise `2.0` as
    /// `2` and a column that refused those rows would miss real values.
    fn stable_type(&self, share: f64, max_string_bytes: usize) -> Option<ColumnType> {
        let n = self.present as f64;
        let enough = |count: usize| count as f64 >= share * n;
        if self.floats > 0 && enough(self.ints + self.floats) {
            Some(ColumnType::Float64)
        } else if enough(self.ints) {
            Some(ColumnType::Int64)
        } else if enough(self.bools) {
            Some(ColumnType::Bool)
        } else if enough(self.timestamps) {
            Some(ColumnType::Timestamp)
        } else if enough(self.strings) && self.string_bytes <= max_string_bytes * self.strings {
            Some(ColumnType::String)
        } else {
            None
        }
    }
}

/// Decide which keys in `records` to promote, given what is already promoted.
///
/// Deterministic for a given input and layout — a re-merge after a crash must
/// make the same decisions, because it rewrites the same file path.
pub fn plan(
    records: &[LogRecord],
    layout: &TypedLayout,
    config: &TypedColumnsConfig,
    now_unix_secs: u64,
) -> Vec<TypedColumn> {
    if config.max_per_service == 0 || layout.len() >= config.max_columns {
        return Vec::new();
    }

    // Key statistics come from a deterministic stride sample: a fraction and
    // a type share are as well estimated from 32k rows as from a million, and
    // judging every attribute of every row cost more than the Parquet write
    // it precedes. `min_rows` is still checked against the full count.
    let stride = (records.len() / SAMPLE_ROWS).max(1);
    let mut services: BTreeMap<&str, ServiceStats> = BTreeMap::new();
    for (row, r) in records.iter().enumerate() {
        let stats = services.entry(r.service.as_deref().unwrap_or("")).or_default();
        stats.rows += 1;
        if row % stride != 0 {
            continue;
        }
        stats.sampled += 1;
        for (i, attr) in r.attributes.iter().enumerate() {
            // Registered keys are the common case once a store has run for a
            // while, so they are skipped before anything else is paid for.
            if layout.index_of(&attr.key).is_some() {
                continue;
            }
            // First occurrence only: a duplicated key on one record is one
            // observation, and only the first can land in the column. Linear,
            // because attribute lists are short and a set costs more.
            if r.attributes[..i].iter().any(|a| a.key == attr.key) {
                continue;
            }
            stats.keys.entry(attr.key.as_str()).or_default().observe(&attr.value);
        }
    }

    let mut taken: HashSet<String> = layout.columns().iter().map(|c| c.column.clone()).collect();
    let mut out = Vec::new();
    for (service, ServiceStats { rows, sampled, keys }) in services {
        if rows < config.min_rows || sampled == 0 {
            continue;
        }
        let mut budget = config.max_per_service.saturating_sub(layout.charged_to(service));
        let mut candidates: Vec<(&str, &KeyStats, ColumnType)> = keys
            .iter()
            .filter(|(_, s)| s.present as f64 >= config.min_presence * sampled as f64)
            .filter_map(|(k, s)| {
                s.stable_type(config.min_type_share, config.max_string_bytes).map(|t| (*k, s, t))
            })
            .collect();
        // Most-present first, then by key, so the budget goes to the keys
        // people are most likely to filter on and ties break the same way
        // every time.
        candidates.sort_by(|a, b| b.1.present.cmp(&a.1.present).then_with(|| a.0.cmp(b.0)));
        for (key, _, ty) in candidates {
            if budget == 0 || layout.len() + out.len() >= config.max_columns {
                break;
            }
            let Some(column) = column_name(key) else { continue };
            if !taken.insert(column.clone()) {
                continue;
            }
            out.push(TypedColumn {
                column,
                key: key.to_string(),
                ty,
                service: service.to_string(),
                promoted_at: now_unix_secs,
            });
            budget -= 1;
        }
    }
    out
}

// --- canonical RFC 3339 timestamps ---------------------------------------

const NANOS_PER_SEC: i64 = 1_000_000_000;

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

/// Render nanoseconds since the epoch in the canonical form: UTC, `Z`, and a
/// fraction of 0, 3, 6 or 9 digits — the fewest that are exact.
pub fn format_timestamp(nanos: i64) -> String {
    let secs = nanos.div_euclid(NANOS_PER_SEC);
    let frac = nanos.rem_euclid(NANOS_PER_SEC);
    let (y, mo, d) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    let (h, mi, s) = (sod / 3600, sod % 3600 / 60, sod % 60);
    let base = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}");
    if frac == 0 {
        format!("{base}Z")
    } else if frac % 1_000_000 == 0 {
        format!("{base}.{:03}Z", frac / 1_000_000)
    } else if frac % 1_000 == 0 {
        format!("{base}.{:06}Z", frac / 1_000)
    } else {
        format!("{base}.{frac:09}Z")
    }
}

/// Parse a string that is exactly in the canonical form [`format_timestamp`]
/// produces. Anything else (offsets, spaces, other fraction lengths) is
/// `None`, which keeps it in the JSON byte-for-byte.
pub fn parse_canonical_timestamp(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if !(20..=30).contains(&b.len()) || b[b.len() - 1] != b'Z' {
        return None;
    }
    let num = |range: std::ops::Range<usize>| -> Option<i64> {
        let part = b.get(range)?;
        if part.iter().all(u8::is_ascii_digit) {
            std::str::from_utf8(part).ok()?.parse().ok()
        } else {
            None
        }
    };
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    // i64 nanoseconds cover 1677..2262; stay inside it.
    if !(1678..=2261).contains(&y) || !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
        return None;
    }
    if h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    let frac = match b.len() {
        20 => 0,
        24 | 27 | 30 if b[19] == b'.' => {
            let digits = b.len() - 21;
            num(20..b.len() - 1)? * 10i64.pow((9 - digits) as u32)
        }
        _ => return None,
    };
    // Canonical means the minimal fraction, the one `format_timestamp` would
    // choose: `.000Z` or `.100000Z` would replay as a different string, so
    // they are not accepted. Checked arithmetically — this runs on every
    // string attribute a promotion pass sees.
    let minimal_digits = if frac == 0 {
        0
    } else if frac % 1_000_000 == 0 {
        3
    } else if frac % 1_000 == 0 {
        6
    } else {
        9
    };
    if b.len() != 20 + if minimal_digits == 0 { 0 } else { minimal_digits + 1 } {
        return None;
    }
    let secs = days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + sec;
    secs.checked_mul(NANOS_PER_SEC)?.checked_add(frac)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Attr, Severity};

    fn rec(service: &str, attrs: Vec<(&str, AttrValue)>) -> LogRecord {
        let mut r = LogRecord::new(1, Severity::INFO, "m");
        r.service = Some(service.into());
        r.attributes = attrs.into_iter().map(|(k, v)| Attr { key: k.into(), value: v }).collect();
        r
    }

    fn config() -> TypedColumnsConfig {
        TypedColumnsConfig { min_rows: 10, ..Default::default() }
    }

    #[test]
    fn column_names_are_lowercase_identifiers_with_a_prefix() {
        assert_eq!(column_name("latency_ms").unwrap(), "attr_latency_ms");
        assert_eq!(column_name("http.status_code").unwrap(), "attr_http_status_code");
        assert_eq!(column_name("User-Id").unwrap(), "attr_user_id");
        assert_eq!(column_name(""), None);
        assert_eq!(column_name("..."), None);
        assert_eq!(column_name(&"k".repeat(MAX_KEY_BYTES + 1)), None);
        assert!(column_name(&"k".repeat(MAX_KEY_BYTES)).is_some());
    }

    #[test]
    fn canonical_timestamps_round_trip_and_nothing_else_parses() {
        for s in [
            "2024-03-01T12:34:56Z",
            "2024-03-01T12:34:56.123Z",
            "2024-03-01T12:34:56.123456Z",
            "2024-03-01T12:34:56.123456789Z",
            "1970-01-01T00:00:00Z",
            "1969-12-31T23:59:59.500Z",
            "2000-02-29T00:00:00Z",
        ] {
            let n = parse_canonical_timestamp(s).unwrap_or_else(|| panic!("{s}"));
            assert_eq!(format_timestamp(n), s);
        }
        assert_eq!(parse_canonical_timestamp("1970-01-01T00:00:01Z"), Some(1_000_000_000));
        for s in [
            "2024-03-01T12:34:56.000Z",
            "2024-03-01T12:34:56.1Z",
            "2024-03-01T12:34:56.120000Z",
            "2024-03-01T12:34:56+00:00",
            "2024-03-01 12:34:56Z",
            "2023-02-29T00:00:00Z",
            "2024-13-01T00:00:00Z",
            "2024-03-01T24:00:00Z",
            "not a timestamp at all",
        ] {
            assert_eq!(parse_canonical_timestamp(s), None, "{s}");
        }
    }

    #[test]
    fn conformance_never_changes_a_value() {
        assert_eq!(conform(&AttrValue::I64(5), ColumnType::Int64), Some(Cell::Int(5)));
        assert_eq!(conform(&AttrValue::F64(1.5), ColumnType::Int64), None);
        assert_eq!(conform(&AttrValue::I64(2), ColumnType::Float64), Some(Cell::Float(2.0)));
        assert_eq!(conform(&AttrValue::I64(i64::MAX), ColumnType::Float64), None);
        assert_eq!(conform(&AttrValue::Str("x".into()), ColumnType::Bool), None);
        assert_eq!(conform(&AttrValue::Str("2024-01-01T00:00:00+01:00".into()), ColumnType::Timestamp), None);
    }

    #[test]
    fn promotes_frequent_stable_keys_and_ignores_rare_or_mixed_ones() {
        let records: Vec<_> = (0..100)
            .map(|i| {
                let mut attrs = vec![
                    ("latency_ms", AttrValue::I64(i)),
                    ("route", AttrValue::Str(format!("/r/{}", i % 3))),
                    ("cache_hit", AttrValue::Bool(i % 2 == 0)),
                    ("ratio", if i % 4 == 0 { AttrValue::I64(1) } else { AttrValue::F64(0.5) }),
                    ("started", AttrValue::Str(format_timestamp(i * 1_000_000_000))),
                    // half ints, half strings: unstable
                    ("mixed", if i % 2 == 0 { AttrValue::I64(i) } else { AttrValue::Str("x".into()) }),
                ];
                if i % 10 == 0 {
                    attrs.push(("rare", AttrValue::I64(1)));
                }
                rec("api", attrs)
            })
            .collect();

        let new = plan(&records, &TypedLayout::default(), &config(), 7);
        let got: Vec<_> = new.iter().map(|c| (c.column.as_str(), c.ty)).collect();
        let mut got_sorted = got.clone();
        got_sorted.sort();
        assert_eq!(
            got_sorted,
            vec![
                ("attr_cache_hit", ColumnType::Bool),
                ("attr_latency_ms", ColumnType::Int64),
                ("attr_ratio", ColumnType::Float64),
                ("attr_route", ColumnType::String),
                ("attr_started", ColumnType::Timestamp),
            ]
        );
        assert!(new.iter().all(|c| c.service == "api" && c.promoted_at == 7));
    }

    #[test]
    fn small_batches_are_not_evidence() {
        let records: Vec<_> = (0..9).map(|i| rec("api", vec![("n", AttrValue::I64(i))])).collect();
        assert!(plan(&records, &TypedLayout::default(), &config(), 0).is_empty());
    }

    #[test]
    fn budget_is_per_service_and_global() {
        let keys: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();
        let mut records = Vec::new();
        for service in ["a", "b"] {
            for i in 0..20 {
                let attrs = keys.iter().map(|k| (k.as_str(), AttrValue::I64(i))).collect();
                records.push(rec(service, attrs));
            }
        }
        // b repeats a's keys with different names so both have candidates.
        for r in records.iter_mut().filter(|r| r.service.as_deref() == Some("b")) {
            for a in &mut r.attributes {
                a.key = format!("b_{}", a.key);
            }
        }
        let cfg = TypedColumnsConfig { max_per_service: 3, max_columns: 5, ..config() };
        let new = plan(&records, &TypedLayout::default(), &cfg, 0);
        assert_eq!(new.iter().filter(|c| c.service == "a").count(), 3);
        assert_eq!(new.iter().filter(|c| c.service == "b").count(), 2, "global cap of 5");

        // Already-charged promotions use up the service's budget.
        let layout = TypedLayout::new(new);
        assert!(plan(&records, &layout, &cfg, 0).is_empty());
    }

    #[test]
    fn colliding_names_and_already_promoted_keys_are_skipped() {
        let records: Vec<_> =
            (0..20).map(|i| rec("a", vec![("User.Id", AttrValue::I64(i)), ("user_id", AttrValue::I64(i))])).collect();
        let new = plan(&records, &TypedLayout::default(), &config(), 0);
        assert_eq!(new.len(), 1, "both keys sanitise to attr_user_id; only one may own it");

        let layout = TypedLayout::new(new);
        assert!(plan(&records, &layout, &config(), 0).is_empty());
    }

    #[test]
    fn plan_is_deterministic() {
        let records: Vec<_> = (0..50)
            .map(|i| rec(if i % 2 == 0 { "x" } else { "y" }, vec![("a", AttrValue::I64(i)), ("b", AttrValue::Bool(true))]))
            .collect();
        let cfg = TypedColumnsConfig { min_rows: 5, ..Default::default() };
        let first = plan(&records, &TypedLayout::default(), &cfg, 1);
        for _ in 0..5 {
            assert_eq!(plan(&records, &TypedLayout::default(), &cfg, 1), first);
        }
    }

    #[test]
    fn large_batches_are_judged_on_a_sample_with_the_same_thresholds() {
        // 100k rows: well past SAMPLE_ROWS, so the stride sample decides.
        let records: Vec<_> = (0..100_000i64)
            .map(|i| {
                let mut attrs = vec![];
                if i % 10 < 6 {
                    attrs.push(("often", AttrValue::I64(i)));
                }
                if i % 10 < 4 {
                    attrs.push(("sometimes", AttrValue::I64(i)));
                }
                rec("a", attrs)
            })
            .collect();
        let new = plan(&records, &TypedLayout::default(), &TypedColumnsConfig::default(), 0);
        let keys: Vec<_> = new.iter().map(|c| c.key.as_str()).collect();
        assert_eq!(keys, vec!["often"], "60% clears min_presence 0.5, 40% does not");
    }

    #[test]
    fn long_strings_are_not_promoted() {
        let records: Vec<_> = (0..20).map(|_| rec("a", vec![("blob", AttrValue::Str("x".repeat(1000)))])).collect();
        assert!(plan(&records, &TypedLayout::default(), &config(), 0).is_empty());
    }
}
