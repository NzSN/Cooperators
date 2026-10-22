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
use std::net::SocketAddr;
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, RwLock};

use crate::arrivals::{
    self, ArrivalLimits, ArrivalService, SourceCoordinate, SubscribeArrivalsRequest,
};
use crate::channels::ChannelLog;
use crate::docs::{self, DocEvent, DocRuntime};
use crate::envelope::Body;
use crate::error::{BrokerError, ErrorCode};
use crate::id;
use crate::inbox_writer::InboxStore;
use crate::messaging;
use crate::paths;
use crate::presence::Presence;
use crate::queues::{self, QueueEvent, QueueRuntime};
use crate::registry::{AgentIdentity, Registry};
use crate::rpc::{self, RpcError, RpcRequest, RpcResponse};
use crate::status::{self, StatusResponse};
use crate::storage::LogFile;
use crate::VERSION;

/// Methods that require a valid agent token.
const AUTHENTICATED_METHODS: [&str; 24] = [
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
    "subscribe_arrivals",
    "unsubscribe_arrivals",
    "list_arrival_subscriptions",
    "wait_arrivals",
    "ack_arrivals",
];

/// Default per-inbox pending-message limit (design §9/§10).
pub const DEFAULT_MAX_INBOX_MESSAGES: usize = 10_000;
/// Default per-inbox pending-byte limit (design §9/§10): 64 MiB.
pub const DEFAULT_MAX_INBOX_BYTES: u64 = 64 * 1024 * 1024;

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
    /// Maximum unacknowledged messages held by one inbox. A send that would
    /// exceed either this count or [`Self::max_inbox_bytes`] fails with
    /// `RESOURCE_EXHAUSTED`.
    pub max_inbox_messages: usize,
    /// Maximum unacknowledged bytes held by one inbox (serialized envelope
    /// bytes). A send that would exceed this fails with
    /// `RESOURCE_EXHAUSTED`.
    pub max_inbox_bytes: u64,
    /// Segment size threshold for channel, queue, and document logs.
    pub log_rotation_bytes: u64,
    /// Maximum records committed together in one inbox file sync.
    pub write_batch_messages: usize,
    /// Maximum serialized bytes collected into one inbox batch. A single
    /// larger message is still committed on its own.
    pub write_batch_bytes: u64,
    /// Maximum intentional collection delay measured from the oldest queued
    /// request. Disk and scheduling time are additional.
    pub write_batch_delay: Duration,
    /// Maximum requests waiting in one inbox's writer queue, excluding the
    /// bounded batch currently being committed.
    pub write_queue_messages: usize,
    /// Maximum serialized-size upper bound for requests waiting in one writer
    /// queue, excluding the active batch.
    pub write_queue_bytes: u64,
    /// Opt in to the irreversible arrival-capable root format upgrade.
    pub enable_arrivals: bool,
    /// Separate arrival subscription/feed bounds.
    pub arrival_limits: ArrivalLimits,
    /// Explicit approved-principal policy; activates the incompatible network
    /// root format before source-log recovery. A client connection never does.
    pub network_policy: Option<crate::network::NetworkPolicy>,
}

impl DaemonConfig {
    /// Design defaults: stale at 45 s, gone at 10 min, sweep every 5 s,
    /// inbox caps 10 000 messages / 64 MiB.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            stale_after: Duration::from_secs(45),
            gone_after: Duration::from_secs(600),
            presence_sweep_interval: Duration::from_secs(5),
            lease_sweep_interval: Duration::from_secs(1),
            wait_batch_limit: 100,
            max_inbox_messages: DEFAULT_MAX_INBOX_MESSAGES,
            max_inbox_bytes: DEFAULT_MAX_INBOX_BYTES,
            log_rotation_bytes: crate::storage::DEFAULT_ROTATION_BYTES,
            write_batch_messages: 64,
            write_batch_bytes: 256 * 1024,
            write_batch_delay: Duration::from_micros(250),
            write_queue_messages: 1024,
            write_queue_bytes: 8 * 1024 * 1024,
            enable_arrivals: false,
            arrival_limits: ArrivalLimits::default(),
            network_policy: None,
        }
    }
}

/// Shared daemon state: identity plus the M1 indexes.
#[derive(Debug)]
pub struct DaemonState {
    // Background tasks hold the state, so ownership outlives every writer even
    // if the listening future is cancelled before the process exits.
    _root_lease: Option<Arc<RootLease>>,
    pub(crate) root: PathBuf,
    started: Instant,
    started_at: String,
    pub(crate) config: DaemonConfig,
    pub(crate) registry: RwLock<Registry>,
    pub(crate) presence: Mutex<Presence>,
    pub(crate) inboxes: Arc<InboxStore>,
    pub(crate) arrivals: Option<Arc<ArrivalService>>,
    pub(crate) network: Option<Arc<crate::network::NetworkRuntime>>,
    pub(crate) send_lock: Arc<Mutex<()>>,
    pub(crate) channel_logs: Mutex<HashMap<String, ChannelLog>>,
    /// Open queue logs, one per queue name (design §7).
    pub(crate) queues: Mutex<HashMap<String, LogFile<QueueEvent>>>,
    /// Replay-derived queue state parallel to `queues`.
    pub(crate) queue_states: Mutex<HashMap<String, QueueRuntime>>,
    /// Open document logs, one per document name (design §8).
    pub(crate) docs: Mutex<HashMap<String, LogFile<DocEvent>>>,
    /// Replay-derived document state parallel to `docs`.
    pub(crate) doc_states: Mutex<HashMap<String, DocRuntime>>,
    /// True while the streamable-HTTP listener is answering requests.
    http_listener: AtomicBool,
    /// In-process HTTP request counters and size-rejection counters.
    pub(crate) metrics: crate::metrics::Metrics,
}

impl DaemonState {
    /// Production constructor: locks the root before replay and holds ownership
    /// until all state users, including background workers, have gone away.
    pub fn owned(config: DaemonConfig) -> Result<Self, BrokerError> {
        let lease = Arc::new(acquire_root_lease(&config.root)?);
        let mut state = Self::with_config(config)?;
        state.inboxes.set_owner(Arc::clone(&lease));
        if let Some(arrivals) = &state.arrivals {
            arrivals.set_owner(Arc::clone(&lease));
            arrivals.start_projection();
        }
        state._root_lease = Some(lease);
        Ok(state)
    }
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
        if let Some(policy) = &config.network_policy {
            policy.validate()?;
        }
        paths::ensure_layout(&config.root)?;
        let mut registry = Registry::load(&config.root)?;
        if registry.network_meta().is_some() && config.network_policy.is_none() {
            return Err(BrokerError::invalid(
                "an activated network root requires its approved policy on every reopen",
            ));
        }
        // Validate/activate the format guard before any log recovery can repair
        // source files containing fields that an older binary cannot hash.
        let arrival_meta =
            arrivals::migration::prepare(&config.root, &mut registry, config.enable_arrivals)?;
        let network = if let Some(policy) = &config.network_policy {
            let meta = registry.prepare_network()?;
            registry.apply_network_policy_generations(
                &policy
                    .devices
                    .iter()
                    .map(|d| (d.principal_id.clone(), d.generation))
                    .collect::<Vec<_>>(),
            )?;
            Some(Arc::new(crate::network::NetworkRuntime::new(
                policy.clone(),
                meta,
            )?))
        } else {
            None
        };
        let arrivals = arrival_meta
            .map(|meta| {
                ArrivalService::open(
                    &config.root,
                    meta,
                    config.arrival_limits.clone(),
                    registry.retired_agent_ids(),
                )
            })
            .transpose()?;
        let (mut inboxes, dedupe) = messaging::load_all_inboxes(&config.root)?;
        for record in registry.records() {
            if !inboxes.contains_key(&record.agent_id) {
                let inbox = messaging::open_inbox(&config.root, &record.agent_id)?;
                inboxes.insert(record.agent_id.clone(), inbox);
            }
        }
        let inboxes = Arc::new(InboxStore::new(&config, inboxes, dedupe)?);
        let presence_log = crate::channels::open_channel_rotating(
            &config.root,
            "#presence",
            config.log_rotation_bytes,
        )?;
        let mut channel_logs = HashMap::new();
        channel_logs.insert("#presence".to_string(), presence_log);

        let (queues, queue_states) =
            queues::load_all_queues(&config.root, config.log_rotation_bytes)?;
        let (docs, doc_states) = docs::load_all_docs(&config.root, config.log_rotation_bytes)?;

        if let Some(arrivals) = &arrivals {
            for (name, log) in &queues {
                arrivals.register_source_head(SourceCoordinate {
                    stream: "queue".into(),
                    name: name.clone(),
                    seq: log.seq_head(),
                    hash: log.last_hash().into(),
                });
            }
            for (name, log) in &docs {
                arrivals.register_source_head(SourceCoordinate {
                    stream: "document".into(),
                    name: name.clone(),
                    seq: log.seq_head(),
                    hash: log.last_hash().into(),
                });
            }
        }

        let mut presence = Presence::default();
        let now_ms = id::unix_millis();
        for (name, record) in registry.records_with_names() {
            presence.seed(name, record, now_ms);
        }

        Ok(Self {
            _root_lease: None,
            root: config.root.clone(),
            started: Instant::now(),
            started_at: id::now_rfc3339(),
            config,
            registry: RwLock::new(registry),
            presence: Mutex::new(presence),
            inboxes,
            arrivals,
            network,
            send_lock: Arc::new(Mutex::new(())),
            channel_logs: Mutex::new(channel_logs),
            queues: Mutex::new(queues),
            queue_states: Mutex::new(queue_states),
            docs: Mutex::new(docs),
            doc_states: Mutex::new(doc_states),
            http_listener: AtomicBool::new(false),
            metrics: crate::metrics::Metrics::default(),
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

    pub fn network(&self) -> Option<&Arc<crate::network::NetworkRuntime>> {
        self.network.as_ref()
    }

    /// Seconds since this daemon state was constructed.
    pub fn uptime_s(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// Whether the HTTP listener is currently serving.
    pub fn http_listener(&self) -> bool {
        self.http_listener.load(Ordering::Relaxed)
    }

    /// Publishes the HTTP listener state to `/healthz`.
    pub fn set_http_listener(&self, listening: bool) {
        self.http_listener.store(listening, Ordering::Relaxed);
    }

    /// Builds the current `daemon.status` payload.
    pub fn status(&self) -> StatusResponse {
        let mut seq_heads = status::scan_seq_heads(&self.root);
        self.inboxes.apply_committed_heads(&mut seq_heads);
        StatusResponse {
            version: VERSION.to_string(),
            pid: std::process::id(),
            uptime_s: self.uptime_s(),
            root: self.root.display().to_string(),
            started_at: self.started_at.clone(),
            seq_heads,
        }
    }
}

/// Per-connection authentication memory. A token supplied on one request is
/// reused by later requests on the same socket that omit `params.token`.
#[derive(Debug, Default)]
pub struct ConnectionSession {
    token: Option<String>,
}

impl ConnectionSession {
    pub(crate) fn authenticated(token: String) -> Self {
        Self { token: Some(token) }
    }
}

/// Exclusive ownership of a bus root, released by the kernel on process exit.
/// Keep the lock file in place: unlinking it would allow locks on two inodes.
#[derive(Debug)]
pub struct RootLease {
    _file: fs::File,
}

/// Acquires ownership before any registry/log replay or recovery. The lock is
/// rooted in the bus directory and is independent of COOP_SOCKET.
pub fn acquire_root_lease(root: &Path) -> io::Result<RootLease> {
    paths::create_private_dir(root)?;
    paths::create_private_dir(&root.join("run"))?;
    let path = root.join("run/owner.lock");
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)?;
    file.try_lock().map_err(|error| {
        let cause = io::Error::from(error);
        io::Error::new(
            cause.kind(),
            format!(
                "bus root {} already owned or unavailable: {cause}",
                root.display()
            ),
        )
    })?;
    Ok(RootLease { _file: file })
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

/// Daemon startup options.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// Streamable-HTTP MCP listen address; `None` disables the endpoint.
    pub http_addr: Option<SocketAddr>,
    /// Explicitly activate durable arrival notifications for this root.
    pub enable_arrivals: bool,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            http_addr: crate::mcp::DEFAULT_HTTP_ADDR.parse().ok(),
            enable_arrivals: false,
        }
    }
}

/// Runs the daemon with the default HTTP listener.
pub async fn run(root: &Path) -> io::Result<()> {
    run_with_options(root, ServerOptions::default()).await
}

/// Runs the daemon until the UDS listener fails.
///
/// The HTTP listener is best-effort: a port already in use degrades the
/// daemon to UDS-only instead of taking the broker down.
pub async fn run_with_options(root: &Path, options: ServerOptions) -> io::Result<()> {
    run_with_network_policy(root, options, None).await
}

pub async fn run_with_network_policy(
    root: &Path,
    options: ServerOptions,
    policy: Option<crate::network::NetworkPolicy>,
) -> io::Result<()> {
    if policy.is_some()
        && options
            .http_addr
            .is_none_or(|address| !address.ip().is_loopback())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "network mode requires a loopback HTTP backend behind a same-host verified TLS ingress",
        ));
    }
    let mut config = DaemonConfig::new(root.to_path_buf());
    config.enable_arrivals = options.enable_arrivals;
    config.network_policy = policy;
    let state = Arc::new(DaemonState::owned(config).map_err(|error| match error {
        BrokerError::Io(error) => error,
        error => io::Error::other(format!("initialize daemon state: {error}")),
    })?);
    let (listener, socket) = bind(root)?;
    state.spawn_presence_sweeper();
    state.spawn_lease_sweeper();
    eprintln!(
        "coop daemon: listening on {} (root {})",
        socket.display(),
        root.display()
    );

    if let Some(address) = options.http_addr {
        match tokio::net::TcpListener::bind(address).await {
            Ok(http_listener) => {
                state.set_http_listener(true);
                let http_state = Arc::clone(&state);
                tokio::spawn(async move {
                    let result =
                        crate::mcp::serve_http(http_listener, Arc::clone(&http_state)).await;
                    http_state.set_http_listener(false);
                    if let Err(error) = result {
                        eprintln!("coop daemon: MCP HTTP listener stopped: {error}");
                    }
                });
            }
            Err(error) => {
                if state.network().is_some() {
                    return Err(error);
                }
                state.set_http_listener(false);
                eprintln!("coop daemon: MCP HTTP listener {address} unavailable: {error}");
            }
        }
    } else {
        state.set_http_listener(false);
    }

    let result = serve(listener, Arc::clone(&state)).await;
    state
        .shutdown_arrivals()
        .await
        .map_err(|error| io::Error::other(format!("shutdown arrival writers: {error}")))?;
    result
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
    if state.network().is_some() {
        if request
            .params
            .get("token")
            .is_some_and(|v| !v.is_null() && v.as_str().is_none_or(str::is_empty))
        {
            return (!request.is_notification).then(|| {
                RpcResponse::failure(
                    request.id.clone().unwrap_or(Value::Null),
                    RpcError::invalid_params("token must be a nonempty string"),
                )
            });
        }
        let bound_token = if let Some(token) = &session.token {
            let registry = state.registry.read().await;
            let local = registry
                .authenticate(token)
                .is_some_and(|id| registry.network_grant(&id.agent_id).is_none());
            (!local).then(|| token.clone())
        } else {
            None
        };
        if let Ok(token) = bound_token
            .map(Ok)
            .unwrap_or_else(|| request_token(&request.params, session))
        {
            let local = {
                let registry = state.registry.read().await;
                registry
                    .authenticate(&token)
                    .is_some_and(|id| registry.network_grant(&id.agent_id).is_none())
            };
            if !local {
                let id = request.id.clone().unwrap_or(Value::Null);
                let result =
                    crate::network::call_remote(state, &token, &request.method, request.params)
                        .await;
                if result.is_ok() {
                    session.token = Some(token);
                }
                if request.is_notification {
                    return None;
                }
                return Some(match result {
                    Ok(value) => RpcResponse::success(id, value),
                    Err(error) => RpcResponse::failure(id, error),
                });
            }
        }
    }
    dispatch_trusted(state, session, request).await
}

pub(crate) async fn dispatch_trusted(
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
        "enroll_agent" => {
            let params: EnrollParams = parse_params(&request.params)?;
            let enrollment = state
                .enroll_agent(
                    params.launch_key,
                    params.kind,
                    params.display_name,
                    params.capabilities,
                )
                .await
                .map_err(BrokerError::into_rpc_error)?;
            session.token = Some(enrollment.token.clone());
            to_value(enrollment)
        }
        "instance_metadata" | "retire_agent" => {
            let identity = authenticate_request(state, session, &request.params).await?;
            let token = session
                .token
                .as_deref()
                .ok_or_else(|| RpcError::internal("authenticated session has no token"))?;
            let metadata = if request.method == "retire_agent" {
                state.retire_agent(token).await
            } else {
                state.instance_metadata(token).await
            }
            .map_err(BrokerError::into_rpc_error)?;
            debug_assert_eq!(identity.agent_id, metadata.agent_id);
            if request.method == "retire_agent" {
                session.token = None;
            }
            let mut reply = to_value(metadata)?;
            if state
                .registry
                .read()
                .await
                .network_grant(&identity.agent_id)
                .is_some()
            {
                reply["network_scoped"] = Value::Bool(true);
            }
            Ok(reply)
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
                "subscribe_arrivals" => {
                    let params: SubscribeArrivalsRequest = parse_params(&request.params)?;
                    to_value(
                        state
                            .subscribe_arrivals(&identity, params)
                            .await
                            .map_err(BrokerError::into_rpc_error)?,
                    )
                }
                "unsubscribe_arrivals" => {
                    let params: UnsubscribeArrivalsParams = parse_params(&request.params)?;
                    to_value(
                        state
                            .unsubscribe_arrivals(&identity, params.subscription_id)
                            .await
                            .map_err(BrokerError::into_rpc_error)?,
                    )
                }
                "list_arrival_subscriptions" => to_value(
                    state
                        .list_arrival_subscriptions(&identity)
                        .await
                        .map_err(BrokerError::into_rpc_error)?,
                ),
                "wait_arrivals" => {
                    let params: WaitArrivalsParams = parse_params(&request.params)?;
                    let timeout_s = params.timeout_s.unwrap_or(30);
                    if timeout_s > 600 {
                        return Err(RpcError::invalid_params(
                            "timeout_s must be at most 600 seconds",
                        ));
                    }
                    to_value(
                        state
                            .wait_arrivals(
                                &identity,
                                params.cursor.as_deref(),
                                Duration::from_secs(timeout_s),
                                params.limit.unwrap_or(64),
                            )
                            .await
                            .map_err(BrokerError::into_rpc_error)?,
                    )
                }
                "ack_arrivals" => {
                    let params: AckParams = parse_params(&request.params)?;
                    to_value(
                        state
                            .ack_arrivals(&identity, params.cursor)
                            .await
                            .map_err(BrokerError::into_rpc_error)?,
                    )
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
struct EnrollParams {
    launch_key: String,
    kind: String,
    display_name: String,
    #[serde(default)]
    capabilities: Option<Vec<String>>,
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
struct UnsubscribeArrivalsParams {
    subscription_id: String,
}

#[derive(Debug, Deserialize)]
struct WaitArrivalsParams {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    timeout_s: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
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
