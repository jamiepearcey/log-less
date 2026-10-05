//! Arrow schema for the committed store, and `LogRecord` → `RecordBatch`.
//!
//! The schema is the long-lived contract: these files outlive the agent that
//! wrote them and are read directly by the user's own DuckDB/Polars/Grafana
//! (`docs/architecture.md` §7 — Parquet is the API). Changing a column name or
//! type is a breaking change to somebody's dashboard, so add columns, never
//! repurpose them.

use std::sync::Arc;

use std::collections::HashMap;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    Float64Array, Float64Builder, Int64Array, Int64Builder, RecordBatch, StringArray,
    StringBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder, UInt64Array,
    UInt64Builder, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::model::{Attr, AttrValue, LogRecord};
use crate::typed::{self, Cell, ColumnType, TypedColumn, TypedLayout};

/// Bumped when the column set changes. Written into Parquet key-value metadata
/// so a reader can tell which generation produced a file.
///
/// 2: typed `attr_*` columns after the base columns (`crate::typed`).
pub const SCHEMA_VERSION: &str = "2";

/// Timestamps are UTC and say so. See the note on the `timestamp` field.
const UTC: &str = "UTC";

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // UUIDv7: time-ordered, and the idempotency key upstream.
        Field::new("event_id", DataType::FixedSizeBinary(16), false),
        // UTC is explicit, not implied. Written without a timezone, engines
        // read these as *local* time and every "last hour" query silently
        // returns the wrong rows. The schema is the long-lived contract, so
        // this must be right before anyone builds a dashboard on it.
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC.into())),
            false,
        ),
        // Partitioning and retention use this one, not `timestamp`: replayed and
        // clock-skewed sources must land in the partition we actually wrote.
        Field::new(
            "observed",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC.into())),
            false,
        ),
        // Raw OTel severity number, so unusual vendor levels round-trip.
        Field::new("severity", DataType::UInt8, false),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, true),
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
        Field::new("template_id", DataType::UInt64, true),
        // JSON for every attribute that is not promoted to a typed `attr_*`
        // column (see `crate::typed`), and for any value that does not conform
        // to its column's type. Typed columns follow the base columns, vary
        // per file, and are discovered from field metadata.
        Field::new("attributes", DataType::Utf8, true),
    ]))
}

/// The Arrow field for a typed attribute column, carrying the original key
/// and the type name so a file is self-describing.
pub fn typed_field(column: &TypedColumn) -> Field {
    Field::new(&column.column, column.ty.arrow_type(), true).with_metadata(HashMap::from([
        (typed::FIELD_META_KEY.to_string(), column.key.clone()),
        (typed::FIELD_META_TYPE.to_string(), column.ty.as_str().to_string()),
    ]))
}

/// Typed columns present in a batch's schema, from field metadata.
pub fn typed_columns_of(schema: &Schema) -> Vec<TypedColumn> {
    schema
        .fields()
        .iter()
        .filter_map(|f| {
            let key = f.metadata().get(typed::FIELD_META_KEY)?;
            let ty = ColumnType::parse(f.metadata().get(typed::FIELD_META_TYPE)?)?;
            Some(TypedColumn {
                column: f.name().clone(),
                key: key.clone(),
                ty,
                service: String::new(),
                promoted_at: 0,
            })
        })
        .collect()
}

/// A typed column's values from a batch, as attribute values, by row.
type TypedReader<'a> = Box<dyn Fn(usize) -> Option<AttrValue> + 'a>;

fn typed_reader(array: &ArrayRef, ty: ColumnType) -> Option<TypedReader<'_>> {
    macro_rules! read {
        ($arr:ty, $wrap:expr) => {{
            let a = array.as_any().downcast_ref::<$arr>()?;
            Some(Box::new(move |row: usize| (!a.is_null(row)).then(|| $wrap(a.value(row)))))
        }};
    }
    match ty {
        ColumnType::Int64 => read!(Int64Array, AttrValue::I64),
        ColumnType::Float64 => read!(Float64Array, AttrValue::F64),
        ColumnType::Bool => read!(BooleanArray, AttrValue::Bool),
        ColumnType::String => read!(StringArray, |v: &str| AttrValue::Str(v.to_string())),
        ColumnType::Timestamp => {
            read!(TimestampNanosecondArray, |v: i64| AttrValue::Str(typed::format_timestamp(v)))
        }
    }
}

/// Sort order applied before writing: clusters templates for zstd/dictionary
/// encoding and makes "everything for trace X" a range scan.
pub fn sort_records(records: &mut [LogRecord]) {
    records.sort_by(|a, b| {
        a.service
            .cmp(&b.service)
            .then_with(|| a.trace_id.cmp(&b.trace_id))
            .then_with(|| a.observed_unix_nano.cmp(&b.observed_unix_nano))
            // Tie-break on the time-ordered id so the sort is total and file
            // contents are reproducible for a given input.
            .then_with(|| a.event_id.cmp(&b.event_id))
    });
}

/// `RecordBatch` → `LogRecord`, the inverse of [`to_record_batch`].
///
/// Used by replay (`crate::scan`). Rows whose required columns are missing or
/// of the wrong type are skipped rather than defaulted: a replay that invents
/// a timestamp or a severity would push fabricated data upstream, which is
/// worse than replaying less.
pub fn from_record_batch(batch: &RecordBatch) -> Vec<LogRecord> {
    let column = |name: &str| batch.column_by_name(name);
    let strings = |name: &str| column(name).and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let binaries =
        |name: &str| column(name).and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

    let (Some(timestamp), Some(observed), Some(severity), Some(body)) = (
        column("timestamp").and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>()),
        column("observed").and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>()),
        column("severity").and_then(|c| c.as_any().downcast_ref::<UInt8Array>()),
        strings("body"),
    ) else {
        return Vec::new();
    };
    let event_id = binaries("event_id");
    let trace_id = binaries("trace_id");
    let span_id = binaries("span_id");
    let service = strings("service");
    let severity_text = strings("severity_text");
    let attributes = strings("attributes");
    let template_id = column("template_id").and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
    let schema = batch.schema();
    let typed_columns: Vec<(String, TypedReader<'_>)> =
        typed_columns_of(&schema)
            .into_iter()
            .filter_map(|c| {
                let reader = typed_reader(batch.column_by_name(&c.column)?, c.ty)?;
                Some((c.key, reader))
            })
            .collect();

    (0..batch.num_rows())
        .map(|row| {
            let mut record = LogRecord::new(
                timestamp.value(row).max(0) as u64,
                crate::model::Severity(severity.value(row)),
                body.value(row),
            );
            record.observed_unix_nano = observed.value(row).max(0) as u64;
            if let Some(ids) = event_id {
                if !ids.is_null(row) {
                    if let Ok(bytes) = <[u8; 16]>::try_from(ids.value(row)) {
                        record.event_id = uuid::Uuid::from_bytes(bytes);
                    }
                }
            }
            record.trace_id = trace_id
                .filter(|a| !a.is_null(row))
                .and_then(|a| <[u8; 16]>::try_from(a.value(row)).ok());
            record.span_id = span_id
                .filter(|a| !a.is_null(row))
                .and_then(|a| <[u8; 8]>::try_from(a.value(row)).ok());
            record.service =
                service.filter(|a| !a.is_null(row)).map(|a| a.value(row).to_string());
            record.severity_text =
                severity_text.filter(|a| !a.is_null(row)).map(|a| a.value(row).to_string());
            record.template_id = template_id.filter(|a| !a.is_null(row)).map(|a| a.value(row));
            // Typed values first, then the JSON remainder. Attribute order is
            // not preserved across promotion; keys and values are.
            let mut attrs: Vec<Attr> = typed_columns
                .iter()
                .filter_map(|(key, read)| read(row).map(|value| Attr { key: key.clone(), value }))
                .collect();
            if let Some(rest) = attributes
                .filter(|a| !a.is_null(row))
                .map(|a| a.value(row))
                .and_then(|json| serde_json::from_str::<Vec<Attr>>(json).ok())
            {
                attrs.extend(rest);
            }
            record.attributes = attrs;
            record
        })
        .collect()
}

/// Values bound for one typed column: (row, value), in row order. Sparse,
/// because most keys are absent from most rows of a mixed-service file.
type Sparse<'a> = Vec<(usize, Cell<'a>)>;

fn build_typed(ty: ColumnType, n: usize, cells: &Sparse<'_>) -> ArrayRef {
    macro_rules! build {
        ($builder:expr, $pat:pat => $v:expr) => {{
            let mut b = $builder;
            let mut next = 0;
            for (row, cell) in cells {
                b.append_nulls(row - next);
                match *cell {
                    $pat => b.append_value($v),
                    _ => b.append_null(),
                }
                next = row + 1;
            }
            b.append_nulls(n - next);
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    match ty {
        ColumnType::Int64 => build!(Int64Builder::with_capacity(n), Cell::Int(v) => v),
        ColumnType::Float64 => build!(Float64Builder::with_capacity(n), Cell::Float(v) => v),
        ColumnType::Bool => build!(BooleanBuilder::with_capacity(n), Cell::Bool(v) => v),
        ColumnType::String => build!(StringBuilder::with_capacity(n, 0), Cell::Str(v) => v),
        ColumnType::Timestamp => {
            let mut b = TimestampNanosecondBuilder::with_capacity(n).with_timezone("UTC");
            let mut next = 0;
            for (row, cell) in cells {
                b.append_nulls(row - next);
                match *cell {
                    Cell::Timestamp(v) => b.append_value(v),
                    _ => b.append_null(),
                }
                next = row + 1;
            }
            b.append_nulls(n - next);
            Arc::new(b.finish())
        }
    }
}

/// `LogRecord`s → one batch: the base columns, then a typed column for every
/// key in `layout` that has at least one conforming value in `records`.
///
/// A conforming value is written to its column and left out of the JSON; a
/// non-conforming one (wrong type, a second occurrence of the key on the same
/// record) stays in the JSON, so the batch always holds every attribute.
pub fn to_record_batch(
    records: &[LogRecord],
    layout: &TypedLayout,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let n = records.len();
    let mut typed_cells: Vec<Sparse<'_>> = vec![Vec::new(); layout.len()];
    let mut used: Vec<usize> = Vec::new();
    let mut rest: Vec<&Attr> = Vec::new();

    let mut event_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut trace_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut span_id = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut severity_text = StringBuilder::new();
    let mut body = StringBuilder::new();
    let mut service = StringBuilder::new();
    let mut template_id = UInt64Builder::with_capacity(n);
    let mut attributes = StringBuilder::new();

    for (row, r) in records.iter().enumerate() {
        event_id.append_value(r.event_id.as_bytes())?;
        match &r.trace_id {
            Some(t) => trace_id.append_value(t)?,
            None => trace_id.append_null(),
        }
        match &r.span_id {
            Some(s) => span_id.append_value(s)?,
            None => span_id.append_null(),
        }
        match &r.severity_text {
            Some(s) => severity_text.append_value(s),
            None => severity_text.append_null(),
        }
        body.append_value(&r.body);
        match &r.service {
            Some(s) => service.append_value(s),
            None => service.append_null(),
        }
        match r.template_id {
            Some(t) => template_id.append_value(t),
            None => template_id.append_null(),
        }
        used.clear();
        rest.clear();
        for attr in &r.attributes {
            let cell = layout.index_of(&attr.key).filter(|i| !used.contains(i)).and_then(|i| {
                typed::conform(&attr.value, layout.columns()[i].ty).map(|cell| (i, cell))
            });
            match cell {
                Some((i, cell)) => {
                    used.push(i);
                    typed_cells[i].push((row, cell));
                }
                None => rest.push(attr),
            }
        }
        if rest.is_empty() {
            attributes.append_null();
        } else {
            attributes.append_value(serde_json::to_string(&rest).unwrap_or_default());
        }
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(event_id.finish()),
        Arc::new(
            TimestampNanosecondArray::from_iter_values(
                records.iter().map(|r| r.timestamp_unix_nano as i64),
            )
            .with_timezone(UTC),
        ),
        Arc::new(
            TimestampNanosecondArray::from_iter_values(
                records.iter().map(|r| r.observed_unix_nano as i64),
            )
            .with_timezone(UTC),
        ),
        Arc::new(UInt8Array::from_iter_values(
            records.iter().map(|r| r.severity.0),
        )),
        Arc::new(severity_text.finish()),
        Arc::new(body.finish()),
        Arc::new(service.finish()),
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(template_id.finish()),
        Arc::new(attributes.finish()),
    ];

    let mut fields: Vec<Field> = schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    for (column, cells) in layout.columns().iter().zip(&typed_cells) {
        if cells.is_empty() {
            continue;
        }
        fields.push(typed_field(column));
        columns.push(build_typed(column.ty, n, cells));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Attr, AttrValue, Severity};
    use arrow::array::Array;

    fn rec(service: Option<&str>, trace: Option<u8>, ts: u64) -> LogRecord {
        let mut r = LogRecord::new(ts, Severity::INFO, "msg");
        r.observed_unix_nano = ts;
        r.service = service.map(String::from);
        r.trace_id = trace.map(|b| [b; 16]);
        r
    }

    #[test]
    fn sorts_by_service_then_trace_then_time() {
        let mut records = vec![
            rec(Some("b"), Some(1), 100),
            rec(Some("a"), Some(2), 50),
            rec(Some("a"), Some(1), 200),
            rec(Some("a"), Some(1), 100),
        ];
        sort_records(&mut records);

        let order: Vec<_> = records
            .iter()
            .map(|r| (r.service.clone().unwrap(), r.trace_id.unwrap()[0], r.observed_unix_nano))
            .collect();
        assert_eq!(
            order,
            vec![
                ("a".into(), 1, 100),
                ("a".into(), 1, 200),
                ("a".into(), 2, 50),
                ("b".into(), 1, 100),
            ]
        );
    }

    #[test]
    fn builds_a_batch_with_nulls_where_expected() {
        let mut with_attrs = rec(Some("svc"), None, 7);
        with_attrs.attributes = vec![Attr {
            key: "latency_ms".into(),
            value: AttrValue::I64(42),
        }];
        let records = vec![rec(None, None, 5), with_attrs];

        let batch = to_record_batch(&records, &TypedLayout::default()).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().fields().len(), 11);

        let service = batch
            .column_by_name("service")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert!(service.is_null(0));
        assert_eq!(service.value(1), "svc");

        let attrs = batch
            .column_by_name("attributes")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert!(attrs.is_null(0), "no attributes must be null, not '[]'");
        assert!(attrs.value(1).contains("latency_ms"));
    }

    #[test]
    fn timestamps_carry_utc_so_engines_do_not_read_them_as_local() {
        // Without the timezone, DuckDB/Polars interpret these as local time and
        // a "last hour" filter silently returns the wrong rows.
        let s = schema();
        for name in ["timestamp", "observed"] {
            let field = s.field_with_name(name).unwrap();
            assert_eq!(
                field.data_type(),
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                "{name} must be UTC-qualified"
            );
        }

        let batch = to_record_batch(&[rec(None, None, 1_000)], &TypedLayout::default()).unwrap();
        assert_eq!(
            batch.column_by_name("observed").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    fn layout(cols: &[(&str, ColumnType)]) -> TypedLayout {
        TypedLayout::new(
            cols.iter()
                .map(|(k, t)| TypedColumn {
                    column: typed::column_name(k).unwrap(),
                    key: k.to_string(),
                    ty: *t,
                    service: "svc".into(),
                    promoted_at: 0,
                })
                .collect(),
        )
    }

    fn attrs(pairs: Vec<(&str, AttrValue)>) -> Vec<Attr> {
        pairs.into_iter().map(|(k, v)| Attr { key: k.into(), value: v }).collect()
    }

    #[test]
    fn typed_columns_carry_conforming_values_and_the_json_keeps_the_rest() {
        let layout = layout(&[
            ("latency_ms", ColumnType::Int64),
            ("ok", ColumnType::Bool),
            ("at", ColumnType::Timestamp),
            ("never_seen", ColumnType::String),
        ]);
        let mut a = rec(Some("svc"), None, 1);
        a.attributes = attrs(vec![
            ("latency_ms", AttrValue::I64(812)),
            ("ok", AttrValue::Bool(true)),
            ("at", AttrValue::Str("2024-05-01T10:00:00.250Z".into())),
            ("other", AttrValue::Str("kept".into())),
        ]);
        let mut b = rec(Some("svc"), None, 2);
        b.attributes = attrs(vec![
            // wrong type for the column: stays in the JSON, column is NULL
            ("latency_ms", AttrValue::Str("slow".into())),
            // not canonical: stays in the JSON verbatim
            ("at", AttrValue::Str("2024-05-01T10:00:00+01:00".into())),
        ]);
        let c = rec(None, None, 3);

        let batch = to_record_batch(&[a.clone(), b.clone(), c.clone()], &layout).unwrap();
        let schema = batch.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(&names[..11], &schema_names()[..]);
        assert_eq!(&names[11..], &["attr_at", "attr_latency_ms", "attr_ok"], "absent keys get no column");

        let lat = batch.column_by_name("attr_latency_ms").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(lat.value(0), 812);
        assert!(lat.is_null(1) && lat.is_null(2));
        let at = batch
            .column_by_name("attr_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(at.value(0), 1_714_557_600_250_000_000);
        assert_eq!(schema.field_with_name("attr_at").unwrap().data_type(), &ColumnType::Timestamp.arrow_type());
        assert_eq!(
            schema.field_with_name("attr_latency_ms").unwrap().metadata().get(typed::FIELD_META_KEY).unwrap(),
            "latency_ms"
        );

        let json = batch.column_by_name("attributes").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        assert!(json.value(0).contains("other") && !json.value(0).contains("latency_ms"));
        assert!(json.value(1).contains("slow") && json.value(1).contains("+01:00"));
        assert!(json.is_null(2));

        // Replay gets every attribute back, typed first then JSON.
        let back = from_record_batch(&batch);
        let sorted = |mut v: Vec<Attr>| {
            v.sort_by(|x, y| x.key.cmp(&y.key));
            v
        };
        assert_eq!(sorted(back[0].attributes.clone()), sorted(a.attributes));
        assert_eq!(sorted(back[1].attributes.clone()), sorted(b.attributes));
        assert!(back[2].attributes.is_empty());
    }

    #[test]
    fn a_repeated_key_on_one_record_keeps_its_second_value_in_the_json() {
        let layout = layout(&[("n", ColumnType::Int64)]);
        let mut r = rec(Some("svc"), None, 1);
        r.attributes = attrs(vec![("n", AttrValue::I64(1)), ("n", AttrValue::I64(2))]);
        let batch = to_record_batch(std::slice::from_ref(&r), &layout).unwrap();
        let back = from_record_batch(&batch);
        assert_eq!(back[0].attributes, r.attributes);
    }

    #[test]
    fn integers_in_a_float_column_replay_as_floats() {
        let layout = layout(&[("ratio", ColumnType::Float64)]);
        let mut r = rec(Some("svc"), None, 1);
        r.attributes = attrs(vec![("ratio", AttrValue::I64(2))]);
        let back = from_record_batch(&to_record_batch(&[r], &layout).unwrap());
        assert_eq!(back[0].attributes, attrs(vec![("ratio", AttrValue::F64(2.0))]));
    }

    fn schema_names() -> Vec<&'static str> {
        vec![
            "event_id", "timestamp", "observed", "severity", "severity_text", "body", "service",
            "trace_id", "span_id", "template_id", "attributes",
        ]
    }

    #[test]
    fn empty_input_is_a_valid_empty_batch() {
        let batch = to_record_batch(&[], &TypedLayout::default()).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
