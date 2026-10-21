//! Private, launch-scoped IPC between a managed bridge and its MCP frontend.
//!
//! Only the bridge process owns or replays the journal. Provider CLIs start
//! the MCP frontend in another process, which uses this authenticated socket
//! to reach the already-owned bridge instead of opening another writer.

use std::fs;
use std::io;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

use crate::error::{BrokerError, ErrorCode};
use crate::host::{
    claim_context, confirm_authenticated, observe_claim_authenticated, resolve_authenticated,
    ClaimAttempt, ClaimResult, ConfirmActivationRequest, ResolveArrivalsRequest, SharedHostBridge,
};
use crate::rpc::{RpcError, RpcResponse};

/// Includes the JSON bytes and excludes the terminating newline.
pub const HOST_CONTROL_MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Bounds a complete request/response exchange, including partial frames.
pub const HOST_CONTROL_FRAME_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONNECTIONS: usize = 32;

// Deliberately no Debug: authentication material must never enter diagnostics.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    secret: String,
    bus_id: String,
    agent_id: String,
    method: String,
    params: Value,
}

/// Credentials and identity are supplied by the trusted managed-launch path.
/// Model-provided tool parameters cannot change this binding context.
#[derive(Clone)]
pub struct HostControlClient {
    socket: PathBuf,
    secret: String,
    bus_id: String,
    agent_id: String,
}

impl std::fmt::Debug for HostControlClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostControlClient")
            .field("socket", &self.socket)
            .field("bus_id", &self.bus_id)
            .field("agent_id", &self.agent_id)
            .finish_non_exhaustive()
    }
}

impl HostControlClient {
    pub fn new(
        socket: impl Into<PathBuf>,
        secret: impl Into<String>,
        bus_id: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Self {
        Self {
            socket: socket.into(),
            secret: secret.into(),
            bus_id: bus_id.into(),
            agent_id: agent_id.into(),
        }
    }

    pub fn bus_id(&self) -> &str {
        &self.bus_id
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// A fresh connection per request prevents cancellation or a partial
    /// response from associating a later result with the wrong claim attempt.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, BrokerError> {
        let request = Request {
            secret: self.secret.clone(),
            bus_id: self.bus_id.clone(),
            agent_id: self.agent_id.clone(),
            method: method.into(),
            params,
        };
        let bytes = serde_json::to_vec(&request)
            .map_err(|_| BrokerError::invalid("host-control request cannot be encoded"))?;
        if bytes.len() > HOST_CONTROL_MAX_FRAME_BYTES {
            return Err(frame_too_large());
        }
        let response = tokio::time::timeout(HOST_CONTROL_FRAME_TIMEOUT, async {
            let mut stream = UnixStream::connect(&self.socket)
                .await
                .map_err(transport_error)?;
            stream.write_all(&bytes).await.map_err(transport_error)?;
            stream.write_all(b"\n").await.map_err(transport_error)?;
            let bytes = read_frame(&mut stream).await?;
            let response: RpcResponse = serde_json::from_slice(&bytes)
                .map_err(|_| BrokerError::invalid("invalid host-control response"))?;
            if response.jsonrpc != "2.0"
                || response.id != json!(1)
                || response.result.is_some() == response.error.is_some()
            {
                return Err(BrokerError::invalid(
                    "invalid host-control response envelope",
                ));
            }
            match (response.result, response.error) {
                (Some(value), None) => Ok(value),
                (None, Some(error)) => Err(remote_error(error)),
                _ => unreachable!("response envelope was validated"),
            }
        })
        .await
        .map_err(|_| unavailable("host-control request timed out"))?;
        response
    }
}

/// Retains the listener task and removes only the socket inode it created.
pub struct HostControlServer {
    socket: SocketGuard,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for HostControlServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostControlServer")
            .field("socket", &self.socket.path)
            .finish_non_exhaustive()
    }
}

impl HostControlServer {
    /// The supplied bridge has already acquired its canonical ownership lock.
    /// Never reopen its journal or replace an active listener during startup.
    pub async fn start(
        path: impl AsRef<Path>,
        secret: impl Into<String>,
        bridge: SharedHostBridge,
    ) -> Result<Self, BrokerError> {
        let secret = secret.into();
        if secret.is_empty() {
            return Err(BrokerError::invalid("host-control secret is required"));
        }
        let path = path.as_ref();
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| BrokerError::invalid("host-control socket requires a private parent"))?;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)?;
        let metadata = fs::symlink_metadata(parent)?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o777 != 0o700 {
            return Err(BrokerError::invalid(
                "host-control socket parent must be a private directory with mode 0700",
            ));
        }
        let name = path
            .file_name()
            .ok_or_else(|| BrokerError::invalid("host-control socket name is required"))?;
        let path = fs::canonicalize(parent)?.join(name);
        remove_stale_socket(&path).await?;
        let listener = UnixListener::bind(&path)?;
        let socket = SocketGuard::new(path)?;
        fs::set_permissions(&socket.path, fs::Permissions::from_mode(0o600))?;

        let (shutdown, receiver) = watch::channel(false);
        let secret = Arc::new(secret);
        let task = tokio::spawn(serve(listener, secret, bridge, receiver));
        Ok(Self {
            socket,
            shutdown,
            task: Some(task),
        })
    }

    pub fn socket(&self) -> &Path {
        &self.socket.path
    }

    /// Stops accepting work, cancels incomplete frames, and reaps all handlers
    /// before releasing the socket. The bridge owner is retained by the caller.
    pub async fn shutdown(mut self) -> Result<(), BrokerError> {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|_| BrokerError::internal("host-control listener task failed"))?;
        }
        Ok(())
    }
}

impl Drop for HostControlServer {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketGuard {
    fn new(path: PathBuf) -> io::Result<Self> {
        let metadata = fs::symlink_metadata(&path)?;
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path).is_ok_and(|metadata| self.matches(&metadata)) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl SocketGuard {
    fn matches(&self, metadata: &fs::Metadata) -> bool {
        metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
    }
}

async fn remove_stale_socket(path: &Path) -> Result<(), BrokerError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "refusing to replace a non-socket host-control path",
        )
        .into());
    }
    match tokio::time::timeout(Duration::from_secs(1), UnixStream::connect(path)).await {
        Ok(Err(error))
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) => {}
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "host-control socket is active or cannot safely be probed",
            )
            .into())
        }
    }
    let current = match fs::symlink_metadata(path) {
        Ok(current) => current,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !current.file_type().is_socket()
        || current.dev() != metadata.dev()
        || current.ino() != metadata.ino()
    {
        return Err(BrokerError::application(
            ErrorCode::Conflict,
            "host-control socket changed during stale-listener probe",
        ));
    }
    fs::remove_file(path)?;
    Ok(())
}

async fn serve(
    listener: UnixListener,
    secret: Arc<String>,
    bridge: SharedHostBridge,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut handlers = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            Some(_) = handlers.join_next(), if !handlers.is_empty() => {},
            accepted = listener.accept(), if handlers.len() < MAX_CONNECTIONS => {
                let Ok((stream, _)) = accepted else { break };
                let secret = secret.clone();
                let bridge = bridge.clone();
                handlers.spawn(async move {
                    let _ = tokio::time::timeout(
                        HOST_CONTROL_FRAME_TIMEOUT,
                        handle(stream, &secret, &bridge),
                    ).await;
                });
            }
        }
    }
    handlers.abort_all();
    while handlers.join_next().await.is_some() {}
}

async fn handle(
    mut stream: UnixStream,
    secret: &str,
    bridge: &SharedHostBridge,
) -> Result<(), BrokerError> {
    let result = async {
        let bytes = read_frame(&mut stream).await?;
        let request: Request = serde_json::from_slice(&bytes)
            .map_err(|_| BrokerError::invalid("invalid host-control request"))?;
        if !secret_matches(secret, &request.secret) {
            return Err(BrokerError::application(
                ErrorCode::Unauthenticated,
                "host-control authentication failed",
            ));
        }
        // Authenticate before dispatch, even when params are malformed or the
        // method is unknown. Runtime helpers repeat this under their own lock.
        bridge
            .lock()
            .await
            .authenticate_binding(&request.bus_id, &request.agent_id)?;
        dispatch(bridge, request).await
    }
    .await;
    let response = match result {
        Ok(value) => RpcResponse::success(json!(1), value),
        Err(error) => RpcResponse::failure(json!(1), error.into_rpc_error()),
    };
    let bytes = serde_json::to_vec(&response)
        .map_err(|_| BrokerError::internal("host-control response cannot be encoded"))?;
    if bytes.len() > HOST_CONTROL_MAX_FRAME_BYTES {
        let failure = RpcResponse::failure(json!(1), frame_too_large().into_rpc_error());
        let bytes = serde_json::to_vec(&failure)
            .map_err(|_| BrokerError::internal("host-control response cannot be encoded"))?;
        stream.write_all(&bytes).await?;
    } else {
        stream.write_all(&bytes).await?;
    }
    stream.write_all(b"\n").await?;
    Ok(())
}

async fn dispatch(bridge: &SharedHostBridge, request: Request) -> Result<Value, BrokerError> {
    let Request {
        bus_id,
        agent_id,
        method,
        params,
        ..
    } = request;
    match method.as_str() {
        "resolve" => encode(
            resolve_authenticated(
                bridge,
                &bus_id,
                &agent_id,
                decode::<ResolveArrivalsRequest>(params)?,
            )
            .await?,
        ),
        "confirm" => encode(
            confirm_authenticated(
                bridge,
                &bus_id,
                &agent_id,
                decode::<ConfirmActivationRequest>(params)?,
                crate::id::unix_millis(),
            )
            .await?,
        ),
        "begin_claim" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct BeginClaim {
                queue: String,
            }
            let params: BeginClaim = decode(params)?;
            encode(claim_context(bridge, &bus_id, &agent_id, &params.queue).await?)
        }
        "finish_claim" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct FinishClaim {
                context: ClaimAttempt,
                outcome: ClaimResult,
            }
            let params: FinishClaim = decode(params)?;
            observe_claim_authenticated(bridge, &bus_id, &agent_id, params.context, params.outcome)
                .await?;
            Ok(json!({"observed": true}))
        }
        "status" => {
            let bridge = bridge.lock().await;
            bridge.authenticate_binding(&bus_id, &agent_id)?;
            encode(bridge.status())
        }
        _ => Err(BrokerError::invalid("unknown host-control method")),
    }
}

fn decode<T: DeserializeOwned>(params: Value) -> Result<T, BrokerError> {
    serde_json::from_value(params)
        .map_err(|_| BrokerError::invalid("invalid host-control method parameters"))
}

fn encode(value: impl Serialize) -> Result<Value, BrokerError> {
    serde_json::to_value(value)
        .map_err(|_| BrokerError::internal("host-control result cannot be encoded"))
}

fn secret_matches(expected: &str, actual: &str) -> bool {
    let mut difference = expected.len() ^ actual.len();
    for index in 0..expected.len().max(actual.len()) {
        difference |= usize::from(
            expected.as_bytes().get(index).copied().unwrap_or(0)
                ^ actual.as_bytes().get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

async fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>, BrokerError> {
    // A fixed chunk and bounded accumulator keep unterminated input bounded.
    let mut frame = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let count = stream.read(&mut chunk).await.map_err(transport_error)?;
        if count == 0 {
            return Err(unavailable(
                "host-control connection ended before a complete frame",
            ));
        }
        let content = &chunk[..count];
        if let Some(newline) = content.iter().position(|byte| *byte == b'\n') {
            if frame.len().saturating_add(newline) > HOST_CONTROL_MAX_FRAME_BYTES {
                return Err(frame_too_large());
            }
            frame.extend_from_slice(&content[..newline]);
            return Ok(frame);
        }
        if frame.len().saturating_add(count) > HOST_CONTROL_MAX_FRAME_BYTES {
            return Err(frame_too_large());
        }
        frame.extend_from_slice(content);
    }
}

fn frame_too_large() -> BrokerError {
    BrokerError::application(
        ErrorCode::ResourceExhausted,
        "host-control frame exceeds the size limit",
    )
}

fn unavailable(detail: &str) -> BrokerError {
    BrokerError::application(ErrorCode::DaemonUnreachable, detail)
}

fn transport_error(_: io::Error) -> BrokerError {
    // Do not include peer-controlled data or request bytes in diagnostics.
    unavailable("host-control transport unavailable")
}

fn remote_error(error: RpcError) -> BrokerError {
    match ErrorCode::from_rpc_code(error.code) {
        Some(code) => match error.data {
            Some(data) => BrokerError::application_with_data(code, error.message, data),
            None => BrokerError::application(code, error.message),
        },
        None if error.code == crate::rpc::INVALID_PARAMS => BrokerError::invalid(error.message),
        None => BrokerError::internal(error.message),
    }
}
