//! Arrow schema for the committed store, and `LogRecord` → `RecordBatch`.
//!
//! The schema is the long-lived contract: these files outlive the agent that
//! wrote them and are read directly by the user's own DuckDB/Polars/Grafana
//! (`docs/architecture.md` §7 — Parquet is the API). Changing a column name or
//! type is a breaking change to somebody's dashboard, so add columns, never
//! repurpose them.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, RecordBatch, StringArray,
    StringBuilder, TimestampNanosecondArray, UInt64Array, UInt64Builder, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::model::LogRecord;

/// Bumped when the column set changes. Written into Parquet key-value metadata
/// so a reader can tell which generation produced a file.
pub const SCHEMA_VERSION: &str = "1";

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
        // TODO(templating): JSON for now. Typed attribute columns arrive with
        // template parameter extraction (§3), which is the point at which typed
        // predicate pushdown (`latency_ms > 500`) becomes possible. Documented
        // as a known gap rather than pretended to be the final shape.
        Field::new("attributes", DataType::Utf8, true),
    ]))
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
            record.attributes = attributes
                .filter(|a| !a.is_null(row))
                .map(|a| a.value(row))
                .and_then(|json| serde_json::from_str(json).ok())
                .unwrap_or_default();
            record
        })
        .collect()
}

pub fn to_record_batch(records: &[LogRecord]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let n = records.len();

    let mut event_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut trace_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut span_id = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut severity_text = StringBuilder::new();
    let mut body = StringBuilder::new();
    let mut service = StringBuilder::new();
    let mut template_id = UInt64Builder::with_capacity(n);
    let mut attributes = StringBuilder::new();

    for r in records {
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
        if r.attributes.is_empty() {
            attributes.append_null();
        } else {
            attributes.append_value(serde_json::to_string(&r.attributes).unwrap_or_default());
        }
    }

    let columns: Vec<ArrayRef> = vec![
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

    RecordBatch::try_new(schema(), columns)
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

        let batch = to_record_batch(&records).unwrap();
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

        let batch = to_record_batch(&[rec(None, None, 1_000)]).unwrap();
        assert_eq!(
            batch.column_by_name("observed").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn empty_input_is_a_valid_empty_batch() {
        let batch = to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
