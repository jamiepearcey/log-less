//! Internal log data model.
//!
//! Deliberately mirrors the OpenTelemetry logs data model (resource / scope /
//! record). Every receiver normalises into `LogRecord`; every writer and
//! forwarder consumes it. See `docs/architecture.md` §7 — this is the decision
//! that makes adding traces and metrics later "another table" rather than a
//! rewrite, so resist adding fields that only make sense for logs.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// OpenTelemetry severity numbers (1..=24).
///
/// Stored as the raw number so unusual vendor levels round-trip unchanged;
/// grouping for partitioning and shedding goes through [`LevelClass`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Severity(pub u8);

impl Severity {
    pub const TRACE: Severity = Severity(1);
    pub const DEBUG: Severity = Severity(5);
    pub const INFO: Severity = Severity(9);
    pub const WARN: Severity = Severity(13);
    pub const ERROR: Severity = Severity(17);
    pub const FATAL: Severity = Severity(21);

    /// Coarse bucket used for partitioning and for load shedding.
    pub fn class(self) -> LevelClass {
        match self.0 {
            0..=8 => LevelClass::Debug,
            9..=12 => LevelClass::Info,
            _ => LevelClass::WarnPlus,
        }
    }

    pub fn is_error_or_worse(self) -> bool {
        self.0 >= Severity::ERROR.0
    }

    /// Best-effort level name → severity, for the receivers whose protocols
    /// carry a string rather than a number (OTLP `severity_text`, HEC fields,
    /// bare log lines). Lives here so every receiver agrees: the same word must
    /// not mean WARN on one input path and INFO on another, or retention would
    /// depend on how a record arrived.
    pub fn from_text(text: &str) -> Option<Severity> {
        let t = text.trim().to_ascii_lowercase();
        Some(match t.as_str() {
            "trace" | "trc" | "verbose" => Severity::TRACE,
            "debug" | "dbg" => Severity::DEBUG,
            "info" | "inf" | "information" | "notice" => Severity::INFO,
            "warn" | "warning" | "wrn" => Severity::WARN,
            "error" | "err" | "severe" => Severity::ERROR,
            "fatal" | "critical" | "crit" | "panic" | "emergency" | "alert" => Severity::FATAL,
            _ => return None,
        })
    }
}

/// Coarse severity bucket — the *default* partition bucket set, and the order
/// in which load shedding sacrifices data.
///
/// Partitioning is not hard-wired to these three: retention granularity
/// dictates partition granularity, so the actual bucket set comes from
/// [`crate::config::LevelBuckets`]. If a policy retains `error` for 90d and
/// `warn` for 14d, they must be separate directories or retention cannot be a
/// directory delete. These three are just the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LevelClass {
    Debug,
    Info,
    WarnPlus,
}

impl LevelClass {
    pub const ALL: [LevelClass; 3] = [LevelClass::Debug, LevelClass::Info, LevelClass::WarnPlus];

    pub fn as_str(self) -> &'static str {
        match self {
            LevelClass::Debug => "debug",
            LevelClass::Info => "info",
            LevelClass::WarnPlus => "warn_plus",
        }
    }
}

/// Attribute value. Kept small on purpose — nested maps and arrays are flattened
/// by receivers rather than represented here, so the Parquet schema stays flat.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AttrValue {
    Str(String),
    I64(i64),
    F64(f64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attr {
    pub key: String,
    pub value: AttrValue,
}

/// A single normalised log record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    /// UUIDv7 — time-ordered, assigned at ingest, never downstream. Doubles as
    /// the idempotency key for at-least-once delivery upstream.
    pub event_id: Uuid,
    /// Event time as claimed by the source.
    pub timestamp_unix_nano: u64,
    /// Time we received it. Diverges from `timestamp_unix_nano` on replay and
    /// on sources with skewed clocks; partitioning uses this one.
    pub observed_unix_nano: u64,
    pub severity: Severity,
    pub severity_text: Option<String>,
    pub body: String,
    /// Denormalised from resource attributes because it is a sort key on every
    /// file and a predicate on every replay query.
    pub service: Option<String>,
    pub trace_id: Option<[u8; 16]>,
    pub span_id: Option<[u8; 8]>,
    pub attributes: Vec<Attr>,
    /// Assigned by the template miner (§3). `None` until templating runs, and
    /// permanently `None` for lines that fail to template.
    pub template_id: Option<u64>,
}

impl LogRecord {
    /// Minimal constructor — receivers fill the rest.
    pub fn new(timestamp_unix_nano: u64, severity: Severity, body: impl Into<String>) -> Self {
        Self {
            event_id: Uuid::now_v7(),
            timestamp_unix_nano,
            observed_unix_nano: timestamp_unix_nano,
            severity,
            severity_text: None,
            body: body.into(),
            service: None,
            trace_id: None,
            span_id: None,
            attributes: Vec::new(),
            template_id: None,
        }
    }

    pub fn level_class(&self) -> LevelClass {
        self.severity.class()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_maps_to_three_classes() {
        assert_eq!(Severity::TRACE.class(), LevelClass::Debug);
        assert_eq!(Severity::DEBUG.class(), LevelClass::Debug);
        assert_eq!(Severity(8).class(), LevelClass::Debug);
        assert_eq!(Severity::INFO.class(), LevelClass::Info);
        assert_eq!(Severity(12).class(), LevelClass::Info);
        assert_eq!(Severity::WARN.class(), LevelClass::WarnPlus);
        assert_eq!(Severity::ERROR.class(), LevelClass::WarnPlus);
        assert_eq!(Severity::FATAL.class(), LevelClass::WarnPlus);
        // Severity 0 is "unspecified" in OTel; treat as debug so it is shed
        // first rather than retained as warn.
        assert_eq!(Severity(0).class(), LevelClass::Debug);
    }

    #[test]
    fn event_ids_are_time_ordered() {
        let a = LogRecord::new(0, Severity::INFO, "a").event_id;
        let b = LogRecord::new(0, Severity::INFO, "b").event_id;
        assert!(a < b, "uuidv7 must sort by creation time");
    }
}
