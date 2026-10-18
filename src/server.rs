//! Unix-domain-socket JSON-RPC server and daemon state.
//!
//! The daemon listens on `<root>/run/daemon.sock` (mode 0600) unless
//! `COOP_SOCKET` overrides the path. Each connection carries newline-delimited
//! JSON-RPC 2.0.
//!
//! T1 methods (`ping`, `echo`, `daemon.status`) stay unauthenticated so the
//! original gate and local operators can inspect a running bus. Every broker
//! method except `register_agent` requires an agent token. Tokens arrive in
//! `params.token`; after a successful call the connection remembers the token
//! so later requests on the same socket may omit it.
//!
//! # M2 state
//!
//! `DaemonState` keeps replay-derived maps in `pub(crate)` fields. Channels,
//! queues, and documents each follow the inbox pattern: one
//! `LogFile<Record>` per log behind a `tokio::sync::Mutex`, with a
//! `storage::ChainRecord` implementation and a parallel replay-derived
//! runtime index. `send_lock` gives mutations one global order when a
//! transition spans several logs (claim selection, dead-lettering, lease
//! renewal).

use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, RwLock};

use crate::channels::ChannelLog;
use crate::docs::{self, DocEvent, DocRuntime};
use crate::envelope::Body;
use crate::error::{BrokerError, ErrorCode};
use crate::id;
use crate::messaging::{self, DedupeEntry, Inbox};
use crate::paths;
use crate::presence::Presence;
use crate::queues::{self, QueueEvent, QueueRuntime};
use crate::registry::{AgentIdentity, Registry};
use crate::rpc::{self, RpcError, RpcRequest, RpcResponse};
use crate::status::{self, StatusResponse};
use crate::storage::LogFile;
use crate::VERSION;

/// Methods that require a valid agent token.
const AUTHENTICATED_METHODS: [&str; 19] = [
    "send_message",
    "wait_message",
    "ack",
    "heartbeat",
    "list_agents",
    "publish",
    "read_channel",
    "create_queue",
    "enqueue",
    "claim_task",
    "renew_lease",
    "complete_task",
    "fail_task",
    "publish_doc",
    "update_doc",
    "claim_doc",
    "release_doc",
    "read_doc",
    "list_docs",
];

/// Daemon tunables. Presence TTLs are injectable so tests can use
/// millisecond-scale windows without sleeping for 45 seconds.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub root: PathBuf,
    /// Silence after which an agent is reported `stale`.
    pub stale_after: Duration,
    /// Silence after which an agent is reported `gone`.
    pub gone_after: Duration,
    /// Background presence sweep interval.
    pub presence_sweep_interval: Duration,
    /// Background queue/document lease sweep interval.
    pub lease_sweep_interval: Duration,
    /// Maximum messages returned by one `wait_message` call.
    pub wait_batch_limit: usize,
}

impl DaemonConfig {
    /// Design defaults: stale at 45 s, gone at 10 min, sweep every 5 s.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            stale_after: Duration::from_secs(45),
            gone_after: Duration::from_secs(600),
            presence_sweep_interval: Duration::from_secs(5),
            lease_sweep_interval: Duration::from_secs(1),
            wait_batch_limit: 100,
        }
    }
}

/// Shared daemon state: identity plus the M1 indexes.
#[derive(Debug)]
pub struct DaemonState {
    pub(crate) root: PathBuf,
    started: Instant,
    started_at: String,
    pub(crate) config: DaemonConfig,
    pub(crate) registry: RwLock<Registry>,
    pub(crate) presence: Mutex<Presence>,
    pub(crate) inboxes: Mutex<HashMap<String, Inbox>>,
    pub(crate) dedupe: Mutex<HashMap<(String, String), DedupeEntry>>,
    pub(crate) send_lock: Mutex<()>,
    pub(crate) channel_logs: Mutex<HashMap<String, ChannelLog>>,
    /// Open queue logs, one per queue name (design §7).
    pub(crate) queues: Mutex<HashMap<String, LogFile<QueueEvent>>>,
    /// Replay-derived queue state parallel to `queues`.
    pub(crate) queue_states: Mutex<HashMap<String, QueueRuntime>>,
    /// Open document logs, one per document name (design §8).
    pub(crate) docs: Mutex<HashMap<String, LogFile<DocEvent>>>,
    /// Replay-derived document state parallel to `docs`.
    pub(crate) doc_states: Mutex<HashMap<String, DocRuntime>>,
}

impl DaemonState {
    /// Creates state with design-default TTLs.
    ///
    /// Panics when the registry or inbox logs cannot be replayed; daemon
    /// callers should use [`DaemonState::with_config`] to report that error.
    pub fn new(root: PathBuf) -> Self {
        Self::with_config(DaemonConfig::new(root.clone())).unwrap_or_else(|error| {
            panic!("initialize daemon state at {}: {error}", root.display())
        })
    }

    /// Creates state with explicit tunables, replaying registry, inboxes, and
    /// the `#presence` channel before returning.
    pub fn with_config(config: DaemonConfig) -> Result<Self, BrokerError> {
        paths::ensure_layout(&config.root)?;
        let registry = Registry::load(&config.root)?;
        let (mut inboxes, dedupe) = messaging::load_all_inboxes(&config.root)?;
        for record in registry.records() {
            if !inboxes.contains_key(&record.agent_id) {
                let inbox = messaging::open_inbox(&config.root, &record.agent_id)?;
                inboxes.insert(record.agent_id.clone(), inbox);
            }
        }
        let presence_log = crate::channels::open_channel(&config.root, "#presence")?;
        let mut channel_logs = HashMap::new();
        channel_logs.insert("#presence".to_string(), presence_log);

        let (queues, queue_states) = queues::load_all_queues(&config.root)?;
        let (docs, doc_states) = docs::load_all_docs(&config.root)?;

        let mut presence = Presence::default();
        let now_ms = id::unix_millis();
        for (name, record) in registry.records_with_names() {
            presence.seed(name, record, now_ms);
        }

        Ok(Self {
            root: config.root.clone(),
            started: Instant::now(),
            started_at: id::now_rfc3339(),
            config,
            registry: RwLock::new(registry),
            presence: Mutex::new(presence),
            inboxes: Mutex::new(inboxes),
            dedupe: Mutex::new(dedupe),
            send_lock: Mutex::new(()),
            channel_logs: Mutex::new(channel_logs),
            queues: Mutex::new(queues),
            queue_states: Mutex::new(queue_states),
            docs: Mutex::new(docs),
            doc_states: Mutex::new(doc_states),
        })
    }

    /// Bus root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Active daemon configuration (TTLs, batch limit).
    pub fn config(&self) -> &DaemonConfig {
        &self.config
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

/// Per-connection authentication memory. A token supplied on one request is
/// reused by later requests on the same socket that omit `params.token`.
#[derive(Debug, Default)]
pub struct ConnectionSession {
    token: Option<String>,
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
    let config = DaemonConfig::new(root.to_path_buf());
    let state = Arc::new(
        DaemonState::with_config(config)
            .map_err(|error| io::Error::other(format!("initialize daemon state: {error}")))?,
    );
    let (listener, socket) = bind(root)?;
    state.spawn_presence_sweeper();
    state.spawn_lease_sweeper();
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
pub async fn dispatch(
    state: &Arc<DaemonState>,
    session: &mut ConnectionSession,
    request: RpcRequest,
) -> Option<RpcResponse> {
    let id = request.id.clone().unwrap_or(Value::Null);
    let outcome = route(state, session, &request).await;

    if request.is_notification {
        return None;
    }
    Some(match outcome {
        Ok(result) => RpcResponse::success(id, result),
        Err(error) => RpcResponse::failure(id, error),
    })
}

async fn route(
    state: &Arc<DaemonState>,
    session: &mut ConnectionSession,
    request: &RpcRequest,
) -> Result<Value, RpcError> {
    match request.method.as_str() {
        "ping" => Ok(json!({
            "pong": true,
            "version": VERSION,
            "ts": id::now_rfc3339(),
        })),
        "echo" => Ok(request.params.clone()),
        "daemon.status" => {
            let state = Arc::clone(state);
            match tokio::task::spawn_blocking(move || state.status()).await {
                Ok(status) => to_value(status),
                Err(error) => Err(RpcError::internal(format!("status task failed: {error}"))),
            }
        }
        "register_agent" => {
            let params: RegisterParams = parse_params(&request.params)?;
            let session_token = session.token.clone();
            let old_token = params.token.as_deref().or(session_token.as_deref());
            let registration = state
                .register_agent(params.name, params.kind, params.capabilities, old_token)
                .await
                .map_err(BrokerError::into_rpc_error)?;
            session.token = Some(registration.token.clone());
            to_value(registration)
        }
        method if AUTHENTICATED_METHODS.contains(&method) => {
            let identity = authenticate_request(state, session, &request.params).await?;
            match method {
                "send_message" => {
                    let params: SendParams = parse_params(&request.params)?;
                    let body = parse_body(&params)?;
                    let ttl = match params.ttl_s {
                        Some(0) => {
                            return Err(RpcError::invalid_params("ttl_s must be greater than zero"))
                        }
                        Some(seconds) => Some(Duration::from_secs(seconds)),
                        None => None,
                    };
                    let receipt = state
                        .send_message(
                            &identity,
                            messaging::SendRequest {
                                to: params.to,
                                kind: params.kind,
                                body,
                                reply_to: params.reply_to,
                                idempotency_key: params.idempotency_key,
                                ttl,
                            },
                        )
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "wait_message" => {
                    let params: WaitParams = parse_params(&request.params)?;
                    let timeout_s = params.timeout_s.unwrap_or(30);
                    if timeout_s > 600 {
                        return Err(RpcError::invalid_params(
                            "timeout_s must be at most 600 seconds",
                        ));
                    }
                    let batch = state
                        .wait_message(
                            &identity.agent_id,
                            params.cursor.as_deref(),
                            Duration::from_secs(timeout_s),
                        )
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(batch)
                }
                "ack" => {
                    let params: AckParams = parse_params(&request.params)?;
                    let receipt = state
                        .ack(&identity.agent_id, &params.cursor)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "heartbeat" => {
                    let heartbeat = state
                        .heartbeat(&identity)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(heartbeat)
                }
                "list_agents" => {
                    let params: ListAgentsParams = parse_params(&request.params)?;
                    let agents = state
                        .list_agents(params.include_stale.unwrap_or(false))
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    Ok(json!({ "agents": agents }))
                }
                "publish" => {
                    let params: PublishParams = parse_params(&request.params)?;
                    let body = parse_body_fields(params.body.as_ref(), params.text.as_deref())?;
                    let receipt = state
                        .publish(&identity, &params.channel, body, params.idempotency_key)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "read_channel" => {
                    let params: ReadChannelParams = parse_params(&request.params)?;
                    let cursor = params
                        .cursor
                        .as_ref()
                        .map(|value| crate::channels::parse_channel_cursor(value, &params.channel))
                        .transpose()
                        .map_err(BrokerError::into_rpc_error)?;
                    let wait_s = params.wait_s.unwrap_or(0);
                    if wait_s > crate::channels::MAX_READ_WAIT_SECS {
                        return Err(RpcError::invalid_params(format!(
                            "wait_s must be at most {} seconds",
                            crate::channels::MAX_READ_WAIT_SECS
                        )));
                    }
                    let batch = state
                        .read_channel(
                            &params.channel,
                            cursor,
                            params.limit.unwrap_or(100),
                            Duration::from_secs(wait_s),
                        )
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(batch)
                }
                "create_queue" => {
                    let params: CreateQueueParams = parse_params(&request.params)?;
                    let receipt = state
                        .create_queue(&params.queue)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "enqueue" => {
                    let params: EnqueueParams = parse_params(&request.params)?;
                    let receipt = state
                        .enqueue(&params.queue, params.payload, params.max_attempts)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "claim_task" => {
                    let params: ClaimTaskParams = parse_params(&request.params)?;
                    let receipt = state
                        .claim_task(&identity, &params.queue, params.lease_ttl_s.unwrap_or(60))
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "renew_lease" => {
                    let params: RenewLeaseParams = parse_params(&request.params)?;
                    let receipt = state
                        .renew_lease(&identity, &params.lease_id)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "complete_task" => {
                    let params: CompleteTaskParams = parse_params(&request.params)?;
                    let receipt = state
                        .complete_task(
                            &identity,
                            &params.lease_id,
                            params.fence,
                            params.outcome,
                            params.receipt,
                        )
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "fail_task" => {
                    let params: FailTaskParams = parse_params(&request.params)?;
                    let receipt = state
                        .fail_task(&identity, &params.lease_id, params.fence, &params.reason)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "publish_doc" => {
                    let params: PublishDocParams = parse_params(&request.params)?;
                    let body = parse_body_fields(params.body.as_ref(), params.text.as_deref())?;
                    let mime = params.mime.unwrap_or_else(|| body.mime.clone());
                    let receipt = state
                        .publish_doc(&identity, &params.name, &mime, body, params.title)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "update_doc" => {
                    let params: UpdateDocParams = parse_params(&request.params)?;
                    let body = parse_body_fields(params.body.as_ref(), params.text.as_deref())?;
                    let lease = match (params.lease_id.as_deref(), params.fence) {
                        (Some(lease_id), Some(fence)) => Some((lease_id, fence)),
                        (None, None) => None,
                        _ => {
                            return Err(RpcError::invalid_params(
                                "lease_id and fence must be supplied together",
                            ))
                        }
                    };
                    let receipt = state
                        .update_doc(
                            &identity,
                            &params.name,
                            params.base_version,
                            body,
                            params.title,
                            lease,
                        )
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "claim_doc" => {
                    let params: ClaimDocParams = parse_params(&request.params)?;
                    let receipt = state
                        .claim_doc(&identity, &params.name, params.lease_ttl_s.unwrap_or(60))
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "release_doc" => {
                    let params: ReleaseDocParams = parse_params(&request.params)?;
                    let receipt = state
                        .release_doc(&identity, &params.lease_id, params.fence)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(receipt)
                }
                "read_doc" => {
                    let params: ReadDocParams = parse_params(&request.params)?;
                    let response = state
                        .read_doc(&params.name, params.version)
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(response)
                }
                "list_docs" => {
                    let params: ListDocsParams = parse_params(&request.params)?;
                    let response = state
                        .list_docs(params.prefix.as_deref())
                        .await
                        .map_err(BrokerError::into_rpc_error)?;
                    to_value(response)
                }
                other => Err(RpcError::method_not_found(other)),
            }
        }
        other => Err(RpcError::method_not_found(other)),
    }
}

async fn authenticate_request(
    state: &Arc<DaemonState>,
    session: &mut ConnectionSession,
    params: &Value,
) -> Result<AgentIdentity, RpcError> {
    let token = request_token(params, session)?;
    let identity = state
        .authenticate(&token)
        .await
        .map_err(BrokerError::into_rpc_error)?;
    session.token = Some(token);
    Ok(identity)
}

fn request_token(params: &Value, session: &ConnectionSession) -> Result<String, RpcError> {
    let missing = || {
        RpcError::application(
            ErrorCode::Unauthenticated,
            "missing agent token; pass params.token or set COOP_TOKEN",
        )
    };
    match params {
        Value::Null => session.token.clone().ok_or_else(missing),
        Value::Object(object) => match object.get("token") {
            None | Some(Value::Null) => session.token.clone().ok_or_else(missing),
            Some(Value::String(token)) if token.is_empty() => Err(RpcError::application(
                ErrorCode::Unauthenticated,
                "agent token must not be empty",
            )),
            Some(Value::String(token)) => Ok(token.clone()),
            Some(_) => Err(RpcError::invalid_params("token must be a string")),
        },
        _ => Err(RpcError::invalid_params("params must be a JSON object")),
    }
}

fn parse_params<T: DeserializeOwned>(params: &Value) -> Result<T, RpcError> {
    if params.is_null() {
        return Err(RpcError::invalid_params("params must be a JSON object"));
    }
    serde_json::from_value(params.clone())
        .map_err(|error| RpcError::invalid_params(error.to_string()))
}

fn parse_body(params: &SendParams) -> Result<Body, RpcError> {
    parse_body_fields(params.body.as_ref(), params.text.as_deref())
}

fn parse_body_fields(body: Option<&Value>, text: Option<&str>) -> Result<Body, RpcError> {
    if let Some(body) = body {
        return match body {
            Value::String(text) => Ok(Body::new("text/plain", text.clone())),
            Value::Object(object) => {
                let mime = object
                    .get("mime")
                    .and_then(Value::as_str)
                    .unwrap_or("text/plain");
                let text = object
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| RpcError::invalid_params("body.text must be a string"))?;
                if mime.is_empty() {
                    return Err(RpcError::invalid_params("body.mime must not be empty"));
                }
                Ok(Body::new(mime, text))
            }
            _ => Err(RpcError::invalid_params(
                "body must be a string or {mime, text} object",
            )),
        };
    }
    text.map(|text| Body::new("text/plain", text.to_string()))
        .ok_or_else(|| RpcError::invalid_params("body or text is required"))
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, RpcError> {
    serde_json::to_value(value).map_err(|error| RpcError::internal(error.to_string()))
}

#[derive(Debug, Deserialize)]
struct RegisterParams {
    name: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    capabilities: Option<Vec<String>>,
    /// Present only when rotating an existing name.
    #[serde(default)]
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SendParams {
    to: String,
    kind: String,
    #[serde(default)]
    body: Option<Value>,
    /// Convenience alias for a `text/plain` body.
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    reply_to: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    ttl_s: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct WaitParams {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    timeout_s: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct AckParams {
    cursor: String,
}

#[derive(Debug, Deserialize)]
struct ListAgentsParams {
    #[serde(default)]
    include_stale: Option<bool>,
}

async fn handle_connection(stream: UnixStream, state: Arc<DaemonState>) {
    serve_stream(stream, state).await;
}

/// Serves one already-connected stream with a fresh connection session.
///
/// `handle_connection` uses this for UDS clients; tests use it with
/// `tokio::io::duplex` to exercise the full NDJSON framing and session-token
/// path without AF_UNIX.
pub async fn serve_stream<S>(stream: S, state: Arc<DaemonState>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut session = ConnectionSession::default();
    handle_stream(stream, state, &mut session).await;
}

/// Shared NDJSON framing loop. Generic over the stream so the exact framing
/// can be exercised with `tokio::io::duplex` without requiring AF_UNIX I/O.
async fn handle_stream<S>(stream: S, state: Arc<DaemonState>, session: &mut ConnectionSession)
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
                    Ok(request) => dispatch(&state, session, request).await,
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
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn request(id: Value, method: &str, params: Value) -> RpcRequest {
        RpcRequest {
            id: Some(id),
            method: method.to_string(),
            params,
            is_notification: false,
        }
    }

    fn test_state() -> Arc<DaemonState> {
        let sequence = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "coop-dispatch-unit-{}-{sequence}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        Arc::new(DaemonState::new(root))
    }

    #[tokio::test]
    async fn dispatch_handles_ping_echo_and_unknown_methods() {
        let state = test_state();
        let mut session = ConnectionSession::default();

        let ping = dispatch(&state, &mut session, request(json!(1), "ping", Value::Null))
            .await
            .expect("ping response");
        assert_eq!(ping.result.expect("ping result")["pong"], true);

        let echo = dispatch(
            &state,
            &mut session,
            request(json!("e"), "echo", json!({"hello": "world"})),
        )
        .await
        .expect("echo response");
        assert_eq!(echo.result.expect("echo result"), json!({"hello": "world"}));

        let missing = dispatch(
            &state,
            &mut session,
            request(json!(2), "missing", Value::Null),
        )
        .await
        .expect("error response");
        assert_eq!(missing.error.expect("error object").code, -32601);
    }

    #[tokio::test]
    async fn notifications_produce_no_response() {
        let state = test_state();
        let mut session = ConnectionSession::default();
        let mut notification = request(Value::Null, "ping", Value::Null);
        notification.is_notification = true;
        assert!(dispatch(&state, &mut session, notification).await.is_none());
    }

    #[tokio::test]
    async fn ndjson_request_response_framing_over_an_in_process_stream() {
        let (client, server_side) = tokio::io::duplex(16 * 1024);
        let state = test_state();
        let handle = tokio::spawn(async move {
            let mut session = ConnectionSession::default();
            handle_stream(server_side, state, &mut session).await;
        });
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
        let mut session = ConnectionSession::default();
        let response = dispatch(
            &state,
            &mut session,
            request(json!(3), "daemon.status", Value::Null),
        )
        .await
        .expect("status response");
        let result = response.result.expect("status result");
        assert_eq!(result["version"], crate::VERSION);
        assert_eq!(result["pid"], std::process::id());
        assert!(result["seq_heads"].is_object());
    }
}

#[derive(Debug, Deserialize)]
struct PublishParams {
    channel: String,
    #[serde(default)]
    body: Option<Value>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReadChannelParams {
    channel: String,
    #[serde(default)]
    cursor: Option<Value>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    wait_s: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CreateQueueParams {
    #[serde(alias = "name")]
    queue: String,
}

#[derive(Debug, Deserialize)]
struct EnqueueParams {
    queue: String,
    payload: Value,
    #[serde(default)]
    max_attempts: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ClaimTaskParams {
    queue: String,
    #[serde(default)]
    lease_ttl_s: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RenewLeaseParams {
    lease_id: String,
}

#[derive(Debug, Deserialize)]
struct CompleteTaskParams {
    lease_id: String,
    fence: u64,
    outcome: Value,
    #[serde(default)]
    receipt: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct FailTaskParams {
    lease_id: String,
    fence: u64,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct PublishDocParams {
    name: String,
    #[serde(default)]
    mime: Option<String>,
    #[serde(default)]
    body: Option<Value>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateDocParams {
    name: String,
    base_version: u64,
    #[serde(default)]
    body: Option<Value>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    lease_id: Option<String>,
    #[serde(default)]
    fence: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ClaimDocParams {
    name: String,
    #[serde(default)]
    lease_ttl_s: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ReleaseDocParams {
    lease_id: String,
    fence: u64,
}

#[derive(Debug, Deserialize)]
struct ReadDocParams {
    name: String,
    #[serde(default)]
    version: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ListDocsParams {
    #[serde(default)]
    prefix: Option<String>,
}
