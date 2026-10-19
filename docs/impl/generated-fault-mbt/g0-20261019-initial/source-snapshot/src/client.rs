//! Minimal UDS JSON-RPC client used by the `coop` CLI.
//!
//! [`call`] keeps the original five-second deadline for short requests.
//! Long-polling callers use [`call_with_timeout`] (root-relative) or
//! [`call_at`] (explicit socket) with a deadline that covers the wait.

use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::error::ErrorCode;
use crate::paths;
use crate::rpc::{RpcError, RpcResponse};

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
}

impl ClientError {
    /// Canonical broker code for this failure, when one applies.
    pub fn error_code(&self) -> Option<ErrorCode> {
        match self {
            Self::Rpc(error) => ErrorCode::from_rpc_code(error.code),
            Self::Unreachable(_) | Self::Timeout => Some(ErrorCode::DaemonUnreachable),
            Self::Protocol(_) => None,
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
    call_at(&paths::socket_path(root), method, params, deadline).await
}

/// Calls one JSON-RPC method on an explicit socket path with an explicit
/// deadline. Useful for tests and for clients that receive `COOP_SOCKET`.
pub async fn call_at(
    socket: &Path,
    method: &str,
    params: Value,
    deadline: Duration,
) -> Result<Value, ClientError> {
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(ClientError::Unreachable)?;

    let request = json!({
        "jsonrpc": "2.0",
        "id": "cli",
        "method": method,
        "params": params,
    });
    let mut encoded =
        serde_json::to_vec(&request).map_err(|error| ClientError::Protocol(error.to_string()))?;
    encoded.push(b'\n');
    stream
        .write_all(&encoded)
        .await
        .map_err(ClientError::Unreachable)?;
    stream.flush().await.map_err(ClientError::Unreachable)?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    match tokio::time::timeout(deadline, reader.read_line(&mut line)).await {
        Err(_) => Err(ClientError::Timeout),
        Ok(Err(error)) => Err(ClientError::Unreachable(error)),
        Ok(Ok(0)) => Err(ClientError::Protocol(
            "daemon closed the connection without a response".into(),
        )),
        Ok(Ok(_)) => {
            let response: RpcResponse =
                serde_json::from_str(line.trim_end_matches(['\r', '\n']))
                    .map_err(|error| ClientError::Protocol(error.to_string()))?;
            response.into_result().map_err(ClientError::Rpc)
        }
    }
}
