//! Cooperators: a localhost inter-agent communication broker.
//!
//! The crate provides the daemon core (`coop daemon run`), the `coop` CLI
//! skeleton, and the storage primitives (append-only JSONL logs with a
//! SHA-256 hash chain) that later milestones build on.
//!
//! Module map:
//!
//! - [`envelope`] — normative v1 message envelope types.
//! - [`storage`] — hash-chained JSONL log, replay, torn-tail recovery.
//! - [`rpc`] — newline-delimited JSON-RPC 2.0 types.
//! - [`server`] — UDS listener and method dispatch.
//! - [`client`] — UDS client used by the CLI.
//! - [`status`] — per-log sequence-head scan for `daemon.status`.
//! - [`error`] — canonical broker error taxonomy and JSON-RPC mapping.
//! - [`paths`] — root/socket resolution and layout creation.
//! - [`id`] — ULID and RFC 3339 helpers.

pub mod client;
pub mod envelope;
pub mod error;
pub mod id;
pub mod paths;
pub mod rpc;
pub mod server;
pub mod status;
pub mod storage;

/// Crate version reported by `ping` and `daemon.status`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
