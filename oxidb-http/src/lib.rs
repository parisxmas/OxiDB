//! Minimal, dependency-free HTTP for OxiDB's own services.
//!
//! Extracted (ADR-0021) so the data-plane server (`oxidb-server`) and the
//! control plane (`oxibase`) share one small, audited HTTP implementation
//! instead of each carrying its own — and so the control plane can be a lean
//! binary that never links the database engine.
//!
//! - [`message`] — `HttpRequest` / `HttpResponse` + request parsing (moved
//!   verbatim from the server's `s3::http`, pure `std`).
//! - [`server`] — a blocking, thread-pooled listener: `serve(addr, …, handler)`.
//! - [`client`] — a blocking HTTP/1.1 client for service-to-service calls.

pub mod client;
pub mod message;
pub mod server;

pub use message::{HttpRequest, HttpResponse, parse_request_from_reader};
