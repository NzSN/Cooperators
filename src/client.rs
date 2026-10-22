//! Compatibility helpers over the shared broker client transport.
//!
//! [`call`] keeps the original five-second deadline for short requests.
//! Long-polling callers use [`call_with_timeout`] (root-relative) or
//! [`call_at`] (explicit socket) with a deadline that covers the wait.

use std::path::Path;
use std::time::Duration;

use serde_json::Value;

use crate::error::ErrorCode;
use crate::rpc::RpcError;
pub use crate::transport::{BrokerClient, Endpoint, HttpsOptions};

/// Default deadline for short JSON-RPC calls.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Client-side failures. Transport failures map to `DAEMON_UNREACHABLE`.
#[derive(Debug)]
pub enum ClientError {
    /// Could not connect to or communicate with the daemon.
    Unreachable(std::io::Error),
    /// The request timed out.
    Timeout,
    /// The daemon returned malformed JSON-RPC.
    Protocol(String),
    /// The daemon returned a JSON-RPC error.
    Rpc(RpcError),
    /// HTTPS connection or TLS setup failed before a usable response.
    RemoteTransport(String),
    /// A non-success status without automatically following or retrying it.
    HttpStatus(u16),
    /// A mutation may have committed. Retry only with its durable operation ID.
    OutcomeUnknown(String),
}

impl ClientError {
    /// Canonical broker code for this failure, when one applies.
    pub fn error_code(&self) -> Option<ErrorCode> {
        match self {
            Self::Rpc(error) => ErrorCode::from_rpc_code(error.code),
            Self::Unreachable(_) | Self::Timeout => Some(ErrorCode::DaemonUnreachable),
            Self::Protocol(_) => None,
            Self::OutcomeUnknown(_) => Some(ErrorCode::OutcomeUnknown),
            Self::RemoteTransport(_) | Self::HttpStatus(_) => Some(ErrorCode::DaemonUnreachable),
        }
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(error) => write!(formatter, "daemon unreachable: {error}"),
            Self::Timeout => formatter.write_str("daemon unreachable: request timed out"),
            Self::Protocol(message) => write!(formatter, "protocol error: {message}"),
            Self::Rpc(error) => write!(formatter, "rpc error {}: {}", error.code, error.message),
            Self::RemoteTransport(detail) => write!(formatter, "HTTPS transport failed: {detail}"),
            Self::HttpStatus(status) => write!(formatter, "broker returned HTTP status {status}"),
            Self::OutcomeUnknown(detail) => write!(formatter, "broker outcome unknown: {detail}"),
        }
    }
}

impl std::error::Error for ClientError {}

/// Calls one JSON-RPC method with the default five-second deadline.
pub async fn call(root: &Path, method: &str, params: Value) -> Result<Value, ClientError> {
    call_with_timeout(root, method, params, DEFAULT_TIMEOUT).await
}

/// Calls one JSON-RPC method with an explicit deadline.
pub async fn call_with_timeout(
    root: &Path,
    method: &str,
    params: Value,
    deadline: Duration,
) -> Result<Value, ClientError> {
    crate::transport::for_root(root)
        .call(method, params, deadline)
        .await
}

/// Calls one JSON-RPC method on an explicit socket path with an explicit
/// deadline. Useful for tests and for clients that receive `COOP_SOCKET`.
pub async fn call_at(
    socket: &Path,
    method: &str,
    params: Value,
    deadline: Duration,
) -> Result<Value, ClientError> {
    BrokerClient::uds(socket)
        .call(method, params, deadline)
        .await
}
