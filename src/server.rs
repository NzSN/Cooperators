//! Unix-domain-socket JSON-RPC server.
//!
//! The daemon listens on `<root>/run/daemon.sock` (mode 0600) unless
//! `COOP_SOCKET` overrides the path. Each connection carries newline-delimited
//! JSON-RPC 2.0. T1 exposes `ping`, `echo`, and `daemon.status`; later waves
//! add the authenticated broker methods to the same dispatch point.

use std::fs;
use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::id;
use crate::paths;
use crate::rpc::{self, RpcError, RpcRequest, RpcResponse};
use crate::status::{self, StatusResponse};
use crate::VERSION;

/// Shared daemon state. T1 only needs identity and uptime; later waves hold
/// indexes and notifier handles here.
#[derive(Debug)]
pub struct DaemonState {
    root: PathBuf,
    started: Instant,
    started_at: String,
}

impl DaemonState {
    /// Creates daemon state rooted at `root`.
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            started: Instant::now(),
            started_at: id::now_rfc3339(),
        }
    }

    /// Bus root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Builds the current `daemon.status` payload.
    pub fn status(&self) -> StatusResponse {
        StatusResponse {
            version: VERSION.to_string(),
            pid: std::process::id(),
            uptime_s: self.started.elapsed().as_secs(),
            root: self.root.display().to_string(),
            started_at: self.started_at.clone(),
            seq_heads: status::scan_seq_heads(&self.root),
        }
    }
}

/// Creates the bus layout, clears a stale socket, binds the listener, and
/// restricts the socket to mode 0600.
///
/// An existing socket is removed only when no daemon answers on it. A
/// non-socket path at the socket location is never replaced.
pub fn bind(root: &Path) -> io::Result<(UnixListener, PathBuf)> {
    paths::ensure_layout(root)?;
    let socket = paths::socket_path(root);
    if let Some(parent) = socket.parent() {
        if !parent.as_os_str().is_empty() {
            paths::create_private_dir(parent)?;
        }
    }

    match fs::symlink_metadata(&socket) {
        Ok(metadata) => {
            if !metadata.file_type().is_socket() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("refusing to replace non-socket path {}", socket.display()),
                ));
            }
            match StdUnixStream::connect(&socket) {
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        format!("a daemon is already listening on {}", socket.display()),
                    ));
                }
                Err(_) => {
                    fs::remove_file(&socket)?;
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let listener = UnixListener::bind(&socket)?;
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
    Ok((listener, socket))
}

/// Runs the daemon until the listener fails.
pub async fn run(root: &Path) -> io::Result<()> {
    let (listener, socket) = bind(root)?;
    let state = Arc::new(DaemonState::new(root.to_path_buf()));
    eprintln!(
        "coop daemon: listening on {} (root {})",
        socket.display(),
        root.display()
    );
    serve(listener, state).await
}

/// Accepts connections and spawns one task per client.
pub async fn serve(listener: UnixListener, state: Arc<DaemonState>) -> io::Result<()> {
    loop {
        let (stream, _address) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            handle_connection(stream, state).await;
        });
    }
}

/// Dispatches one request. Returns `None` for notifications.
pub async fn dispatch(state: &Arc<DaemonState>, request: RpcRequest) -> Option<RpcResponse> {
    let id = request.id.clone().unwrap_or(Value::Null);
    let outcome: Result<Value, RpcError> = match request.method.as_str() {
        "ping" => Ok(json!({
            "pong": true,
            "version": VERSION,
            "ts": id::now_rfc3339(),
        })),
        "echo" => Ok(request.params.clone()),
        "daemon.status" => {
            let state = Arc::clone(state);
            match tokio::task::spawn_blocking(move || state.status()).await {
                Ok(status) => serde_json::to_value(status)
                    .map_err(|error| RpcError::internal(error.to_string())),
                Err(error) => Err(RpcError::internal(format!("status task failed: {error}"))),
            }
        }
        other => Err(RpcError::method_not_found(other)),
    };

    if request.is_notification {
        return None;
    }
    Some(match outcome {
        Ok(result) => RpcResponse::success(id, result),
        Err(error) => RpcResponse::failure(id, error),
    })
}

async fn handle_connection(stream: UnixStream, state: Arc<DaemonState>) {
    handle_stream(stream, state).await;
}

/// Shared NDJSON framing loop. Generic over the stream so the exact framing
/// can be exercised with `tokio::io::duplex` without requiring AF_UNIX I/O.
async fn handle_stream<S>(stream: S, state: Arc<DaemonState>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {
                let trimmed = line.trim_end_matches(['\r', '\n']);
                if trimmed.trim().is_empty() {
                    continue;
                }
                let response = match rpc::parse_request(trimmed) {
                    Ok(request) => dispatch(&state, request).await,
                    Err(error) => Some(RpcResponse::failure(Value::Null, error)),
                };
                let Some(response) = response else {
                    continue;
                };
                let Ok(mut encoded) = serde_json::to_string(&response) else {
                    break;
                };
                encoded.push('\n');
                if write_half.write_all(encoded.as_bytes()).await.is_err() {
                    break;
                }
                if write_half.flush().await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: Value, method: &str, params: Value) -> RpcRequest {
        RpcRequest {
            id: Some(id),
            method: method.to_string(),
            params,
            is_notification: false,
        }
    }

    fn test_state() -> Arc<DaemonState> {
        let root = std::env::temp_dir().join(format!("coop-dispatch-unit-{}", std::process::id()));
        Arc::new(DaemonState::new(root))
    }

    #[tokio::test]
    async fn dispatch_handles_ping_echo_and_unknown_methods() {
        let state = test_state();

        let ping = dispatch(&state, request(json!(1), "ping", Value::Null))
            .await
            .expect("ping response");
        assert_eq!(ping.result.expect("ping result")["pong"], true);

        let echo = dispatch(
            &state,
            request(json!("e"), "echo", json!({"hello": "world"})),
        )
        .await
        .expect("echo response");
        assert_eq!(echo.result.expect("echo result"), json!({"hello": "world"}));

        let missing = dispatch(&state, request(json!(2), "missing", Value::Null))
            .await
            .expect("error response");
        assert_eq!(missing.error.expect("error object").code, -32601);
    }

    #[tokio::test]
    async fn notifications_produce_no_response() {
        let state = test_state();
        let mut notification = request(Value::Null, "ping", Value::Null);
        notification.is_notification = true;
        assert!(dispatch(&state, notification).await.is_none());
    }

    #[tokio::test]
    async fn ndjson_request_response_framing_over_an_in_process_stream() {
        let (client, server_side) = tokio::io::duplex(16 * 1024);
        let handle = tokio::spawn(handle_stream(server_side, test_state()));
        let mut reader = BufReader::new(client);
        {
            let stream = reader.get_mut();
            stream
                .write_all(
                    b"{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"echo\",\"params\":{\"x\":1}}\n",
                )
                .await
                .expect("write request");
            stream.flush().await.expect("flush");
        }
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("read response");
        let response: RpcResponse = serde_json::from_str(line.trim_end()).expect("response json");
        assert_eq!(response.id, json!(7));
        assert_eq!(response.result.expect("result"), json!({"x": 1}));

        {
            let stream = reader.get_mut();
            stream
                .write_all(b"not json\n")
                .await
                .expect("write parse error request");
            stream.flush().await.expect("flush");
        }
        line.clear();
        reader
            .read_line(&mut line)
            .await
            .expect("read error response");
        let response: RpcResponse = serde_json::from_str(line.trim_end()).expect("response json");
        assert_eq!(response.error.expect("error").code, -32700);
        assert!(response.id.is_null());

        drop(reader);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn status_reports_identity_and_an_empty_head_map() {
        let state = test_state();
        let response = dispatch(&state, request(json!(3), "daemon.status", Value::Null))
            .await
            .expect("status response");
        let result = response.result.expect("status result");
        assert_eq!(result["version"], crate::VERSION);
        assert_eq!(result["pid"], std::process::id());
        assert!(result["seq_heads"].is_object());
    }
}
