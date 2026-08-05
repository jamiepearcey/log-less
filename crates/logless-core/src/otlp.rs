//! OTLP logs ingest.
//!
//! The adoption-unlock surface (`docs/architecture.md` §6): every OTel
//! Collector and Vector deployment already speaks it, so log-less becomes a
//! destination change rather than a migration. `model.rs` is already the OTel
//! logs data model, so this is a decode, not a translation.
//!
//! Two transports and two encodings, one data model: OTLP/HTTP (`http`) and
//! OTLP/gRPC (`grpc`) carry protobuf to [`logs::decode_request`], and
//! OTLP/JSON goes through [`json::decode_request`]. All three produce the same
//! `LogRecord`, so nothing downstream can tell which wire a record arrived on —
//! which is what lets one error dedupe against the same error from a different
//! transport.

pub mod grpc;
pub mod h2;
pub mod http;
pub mod json;
pub mod logs;
pub mod proto;

pub use grpc::{GrpcConfig, Receiver as GrpcReceiver};
pub use http::{Accepted, Receiver, ReceiverConfig, ReceiverError};
pub use logs::{decode_request, Decoded};
pub use proto::ProtoError;
