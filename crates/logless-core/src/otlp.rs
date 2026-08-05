//! OTLP logs ingest.
//!
//! The adoption-unlock surface (`docs/architecture.md` §6): every OTel
//! Collector and Vector deployment already speaks it, so log-less becomes a
//! destination change rather than a migration. `model.rs` is already the OTel
//! logs data model, so this is a decode, not a translation.
//!
//! Two transports, one decoder: OTLP/HTTP (`http`) and OTLP/gRPC (`grpc`) both
//! hand their protobuf to [`logs::decode_request`]. OTLP/JSON is a second
//! *encoding* and is not implemented; the HTTP receiver answers `415` naming
//! what is supported rather than failing as a malformed payload.

pub mod grpc;
pub mod h2;
pub mod http;
pub mod logs;
pub mod proto;

pub use grpc::{GrpcConfig, Receiver as GrpcReceiver};
pub use http::{Accepted, Receiver, ReceiverConfig, ReceiverError};
pub use logs::{decode_request, Decoded};
pub use proto::ProtoError;
