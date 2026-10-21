//! Managed duplex host transports. Protocol baseline: Codex CLI 0.163.0 and
//! Claude Code 2.1.300. These adapters never select a latest/recent session,
//! silently grant permissions, or equate host acceptance with observation.
//!
//! Codex uses its documented app-server NDJSON protocol and generated local
//! schemas. Claude uses persistent stream-json input, explicit session resume,
//! replayed user-message acknowledgments, and stdio control requests. Its CLI
//! has no authoritative history query; uncertain acceptance remains Unknown.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};

use super::{
    AdapterFuture, Binding, HostAdapter, HostApproval, HostCapabilities, HostEvent, SessionState,
    Submission, Wake,
};
use crate::error::{BrokerError, ErrorCode};

const FRAME_BYTES: usize = 4 * 1024 * 1024;
const FRAME_QUEUE: usize = 16;
const EVENT_QUEUE: usize = 128;
const NOTIFICATION_BYTES: usize = 64 * 1024;
const APPROVAL_LIMIT: usize = 64;
const ACCEPTED_CACHE: usize = 64;
const HISTORY_PAGES: usize = 8;

/// App-server launch options. Configuration overrides primarily install the
/// bridge's MCP proxy. They are explicit operator configuration, not policies
/// inferred by the adapter. The adapter never overrides approvalPolicy itself.
#[derive(Debug, Clone)]
pub struct CodexAdapterConfig {
    pub executable: PathBuf,
    pub env: BTreeMap<String, String>,
    pub config_overrides: Vec<String>,
    pub request_timeout: Duration,
}

impl Default for CodexAdapterConfig {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("codex"),
            env: BTreeMap::new(),
            config_overrides: Vec::new(),
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// Persistent Claude host-mode launch options. `resume_existing=false` creates
/// the explicitly bound session UUID; true resumes that exact UUID. No fork,
/// most-recent lookup, preview Channels flags, or blanket tool grants are used.
#[derive(Debug, Clone)]
pub struct ClaudeAdapterConfig {
    pub executable: PathBuf,
    pub env: BTreeMap<String, String>,
    pub mcp_config: Option<PathBuf>,
    pub resume_existing: bool,
    pub request_timeout: Duration,
}

impl Default for ClaudeAdapterConfig {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("claude"),
            env: BTreeMap::new(),
            mcp_config: None,
            resume_existing: true,
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// One child with bounded NDJSON buffering. Keeping stdin owned by this object
/// keeps the Claude stream alive between turns. stdout is continuously drained
/// into a bounded channel, and stderr is drained without retaining transcripts
/// or leaking credentials into bridge journals.
struct DuplexProcess {
    child: Child,
    stdin: ChildStdin,
    frames: mpsc::Receiver<Result<Value, String>>,
    reader: JoinHandle<()>,
    stderr_reader: JoinHandle<()>,
    closed: bool,
    request_timeout: Duration,
}

impl DuplexProcess {
    fn spawn(mut command: Command, request_timeout: Duration) -> Result<Self, BrokerError> {
        if request_timeout.is_zero() {
            return Err(BrokerError::invalid(
                "host request timeout must be positive",
            ));
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        let (sender, frames) = mpsc::channel(FRAME_QUEUE);
        let reader = tokio::spawn(async move {
            let mut output = BufReader::new(stdout);
            loop {
                let frame = match read_frame(&mut output).await {
                    Ok(Some(bytes)) => serde_json::from_slice(&bytes)
                        .map_err(|_| "host emitted malformed JSON".to_string()),
                    Ok(None) => Err("host stdout closed".to_string()),
                    Err(error) => Err(format!("host stdout: {error}")),
                };
                let failed = frame.is_err();
                if sender.send(frame).await.is_err() || failed {
                    break;
                }
            }
        });
        let stderr_reader = tokio::spawn(async move {
            let mut buffer = [0u8; 8192];
            while matches!(stderr.read(&mut buffer).await, Ok(count) if count > 0) {}
        });
        Ok(Self {
            child,
            stdin,
            frames,
            reader,
            stderr_reader,
            closed: false,
            request_timeout,
        })
    }

    async fn write(&mut self, value: Value) -> Result<(), BrokerError> {
        if self.closed {
            return Err(BrokerError::internal("host process is offline"));
        }
        let mut line =
            serde_json::to_vec(&value).map_err(|error| BrokerError::internal(error.to_string()))?;
        if line.len() > FRAME_BYTES {
            return Err(BrokerError::invalid("host request exceeds frame limit"));
        }
        line.push(b'\n');
        let result = timeout(self.request_timeout, async {
            self.stdin.write_all(&line).await?;
            self.stdin.flush().await
        })
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.closed = true;
                let _ = self.child.start_kill();
                Err(error.into())
            }
            Err(_) => {
                // A timed-out partial write makes framing uncertain. Never
                // append another request to that same byte stream.
                self.closed = true;
                let _ = self.child.start_kill();
                Err(BrokerError::internal("host stdin write timed out"))
            }
        }
    }

    async fn next(&mut self, deadline: Instant) -> Result<Value, BrokerError> {
        if self.closed {
            return Err(BrokerError::internal("host process is offline"));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, self.frames.recv()).await {
            Ok(Some(Ok(frame))) => Ok(frame),
            Ok(Some(Err(detail))) => {
                self.closed = true;
                let _ = self.child.start_kill();
                Err(BrokerError::internal(detail))
            }
            Ok(None) => {
                self.closed = true;
                let _ = self.child.start_kill();
                Err(BrokerError::internal("host stdout reader stopped"))
            }
            Err(_) => Err(BrokerError::internal(
                "host response timed out; acceptance unknown",
            )),
        }
    }

    fn try_next(&mut self) -> Option<Result<Value, String>> {
        // The reader may have queued valid-looking frames before a fatal
        // protocol error closed this process. Closure fences that entire
        // stream, including buffered approvals, echoes and status changes.
        if self.closed {
            return None;
        }
        match self.frames.try_recv() {
            Ok(frame) => {
                if frame.is_err() {
                    self.closed = true;
                    let _ = self.child.start_kill();
                }
                Some(frame)
            }
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) if !self.closed => {
                self.closed = true;
                let _ = self.child.start_kill();
                Some(Err("host stdout reader stopped".to_string()))
            }
            Err(mpsc::error::TryRecvError::Disconnected) => None,
        }
    }

    async fn shutdown(&mut self) -> Result<(), BrokerError> {
        self.closed = true;
        let _ = self.child.start_kill();
        let result = self.child.wait().await;
        self.reader.abort();
        self.stderr_reader.abort();
        result.map(|_| ()).map_err(Into::into)
    }
}

impl Drop for DuplexProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        self.reader.abort();
        self.stderr_reader.abort();
    }
}

async fn read_frame<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unterminated host frame",
                ))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(available.len(), |index| index + 1);
        if line.len() + count > FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "host frame exceeds limit",
            ));
        }
        line.extend_from_slice(&available[..count]);
        reader.consume(count);
        if newline.is_some() {
            return Ok(Some(line));
        }
    }
}

struct AdapterState {
    binding: Binding,
    workspace: PathBuf,
    state: SessionState,
    events: VecDeque<HostEvent>,
    approvals: BTreeMap<String, HostApproval>,
    accepted: VecDeque<(String, Option<String>)>,
}

impl AdapterState {
    fn new(binding: &Binding, kinds: &[&str]) -> Result<Self, BrokerError> {
        if !kinds.contains(&binding.adapter_kind.as_str()) {
            return Err(BrokerError::invalid(
                "binding has the wrong host adapter kind",
            ));
        }
        if binding.exact_host_session_id.is_empty() || binding.binding_id.is_empty() {
            return Err(BrokerError::invalid(
                "host binding needs an exact session ID",
            ));
        }
        let workspace = Path::new(&binding.workspace).canonicalize()?;
        if !workspace.is_dir() {
            return Err(BrokerError::invalid("host workspace must be a directory"));
        }
        Ok(Self {
            binding: binding.clone(),
            workspace,
            state: SessionState::Unknown,
            events: VecDeque::new(),
            approvals: BTreeMap::new(),
            accepted: VecDeque::new(),
        })
    }

    fn check(&self, binding: &Binding, wake: Option<&Wake>) -> Result<(), BrokerError> {
        let original = &self.binding;
        if original.binding_id != binding.binding_id
            || original.generation != binding.generation
            || original.bus_id != binding.bus_id
            || original.agent_id != binding.agent_id
            || original.adapter_kind != binding.adapter_kind
            || original.exact_host_session_id != binding.exact_host_session_id
            || original.workspace != binding.workspace
        {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "stale or redirected host binding",
            ));
        }
        if let Some(wake) = wake {
            if wake.binding_id != original.binding_id
                || wake.generation != original.generation
                || wake.bus_id != original.bus_id
                || wake.agent_id != original.agent_id
            {
                return Err(BrokerError::application(
                    ErrorCode::Conflict,
                    "wake belongs to another binding generation",
                ));
            }
        }
        Ok(())
    }

    fn push(&mut self, mut event: HostEvent) {
        // Approvals are retained separately until explicitly answered. Ordinary
        // output is diagnostic and bounded; it is not the durable arrival feed.
        if let HostEvent::Notification { value } = &event {
            let size = serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len());
            if size > NOTIFICATION_BYTES {
                event = HostEvent::Notification {
                    value: json!({
                        "type":"host_notification_omitted", "bytes":size,
                        "method":value.get("method"), "event_type":value.get("type"),
                    }),
                };
            }
        }
        if self.events.len() == EVENT_QUEUE {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    fn set_state(&mut self, state: SessionState) {
        let state = if !self.approvals.is_empty() && state != SessionState::Offline {
            SessionState::WaitingForPermission
        } else {
            state
        };
        if self.state != state {
            self.state = state;
            self.push(HostEvent::State { state });
        }
    }

    fn offline(&mut self, detail: impl Into<String>) {
        self.set_state(SessionState::Offline);
        self.push(HostEvent::Offline {
            detail: detail.into(),
        });
    }

    fn approval(&mut self, approval: HostApproval) -> Result<(), BrokerError> {
        let key = approval.request_id.to_string();
        if let Some(previous) = self.approvals.get(&key) {
            if previous != &approval {
                return Err(BrokerError::internal(
                    "host reused an outstanding approval request ID",
                ));
            }
        } else {
            if self.approvals.len() >= APPROVAL_LIMIT {
                return Err(BrokerError::application(
                    ErrorCode::ResourceExhausted,
                    "too many unanswered host approval requests",
                ));
            }
            self.approvals.insert(key, approval);
        }
        self.set_state(SessionState::WaitingForPermission);
        Ok(())
    }

    fn take_events(&mut self) -> Vec<HostEvent> {
        let mut events: Vec<_> = self.events.drain(..).collect();
        // Re-emit unanswered requests on every drain so a disconnected operator
        // client cannot lose a permission prompt through diagnostic truncation.
        events.extend(
            self.approvals
                .values()
                .cloned()
                .map(|approval| HostEvent::Approval { approval }),
        );
        events
    }

    fn accepted(&mut self, wake: &Wake, turn_id: Option<String>) -> Submission {
        if let Some(entry) = self
            .accepted
            .iter_mut()
            .find(|entry| entry.0 == wake.wake_id)
        {
            entry.1 = turn_id.clone();
        } else {
            if self.accepted.len() == ACCEPTED_CACHE {
                self.accepted.pop_front();
            }
            self.accepted
                .push_back((wake.wake_id.clone(), turn_id.clone()));
        }
        Submission::Accepted { turn_id }
    }

    fn known(&self, wake: &Wake) -> Option<Submission> {
        self.accepted
            .iter()
            .find(|entry| entry.0 == wake.wake_id)
            .map(|entry| Submission::Accepted {
                turn_id: entry.1.clone(),
            })
    }
}

fn unknown(error: impl std::fmt::Display) -> Submission {
    Submission::Unknown {
        detail: error.to_string(),
    }
}

fn thread_state(status: &Value) -> SessionState {
    match status["type"].as_str().or_else(|| status.as_str()) {
        Some("idle") => SessionState::Idle,
        Some("active")
            if status["activeFlags"].as_array().is_some_and(|flags| {
                flags.iter().any(|flag| {
                    matches!(
                        flag.as_str(),
                        Some("waitingOnApproval" | "waitingOnUserInput")
                    )
                })
            }) =>
        {
            SessionState::WaitingForPermission
        }
        Some("active") => SessionState::Busy,
        Some("systemError") => SessionState::Offline,
        _ => SessionState::Unknown,
    }
}

fn user_text(message: &Value) -> Option<String> {
    if let Some(text) = message["content"].as_str() {
        return Some(text.to_string());
    }
    let content = message["content"].as_array()?;
    let text: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "text")
        .filter_map(|item| item["text"].as_str())
        .collect();
    if text.is_empty() {
        None
    } else {
        Some(text.join("\n"))
    }
}

/// Managed stdio Codex App Server connection, resumed to exactly one thread.
pub struct CodexAppServerAdapter {
    process: DuplexProcess,
    inner: AdapterState,
    next_request: u64,
}

fn codex_command(config: &CodexAdapterConfig, workspace: &Path) -> Command {
    let mut command = Command::new(&config.executable);
    command.arg("app-server").args(["--listen", "stdio://"]);
    for item in &config.config_overrides {
        command.arg("-c").arg(item);
    }
    command.current_dir(workspace).envs(&config.env);
    command
}

fn codex_initialize_params() -> Value {
    json!({
        "clientInfo": {"name":"cooperators_host", "title":"Cooperators host bridge", "version":env!("CARGO_PKG_VERSION")},
        "capabilities": {"experimentalApi": false}
    })
}

// Bootstrap has no bound operator control connection yet. Surface any server
// request as a failure and reap the process rather than silently granting it.
async fn bootstrap_rpc(
    process: &mut DuplexProcess,
    id: u64,
    method: &str,
    params: Value,
) -> Result<Value, BrokerError> {
    process
        .write(json!({"id":id,"method":method,"params":params}))
        .await?;
    let deadline = Instant::now() + process.request_timeout;
    loop {
        let value = process.next(deadline).await?;
        if !value.is_object() {
            return Err(BrokerError::internal(
                "app-server emitted a non-object frame",
            ));
        }
        if value.get("method").is_some() {
            if value.get("id").is_some() {
                return Err(BrokerError::internal(
                    "app-server bootstrap requires an operator response",
                ));
            }
            continue;
        }
        if value["id"] != id {
            return Err(BrokerError::internal(
                "unexpected app-server bootstrap response ID",
            ));
        }
        if let Some(error) = value.get("error") {
            return Err(BrokerError::internal(format!(
                "app-server {method} failed: {error}"
            )));
        }
        return value
            .get("result")
            .cloned()
            .ok_or_else(|| BrokerError::internal("app-server response has no result"));
    }
}

impl CodexAppServerAdapter {
    /// Create and persist a thread without starting a model turn. The caller
    /// binds the returned exact ID before opening the managed resume connection.
    /// MCP overrides are applied to this process exactly as on normal resume.
    pub async fn create_session(
        config: &CodexAdapterConfig,
        workspace: &Path,
    ) -> Result<String, BrokerError> {
        let workspace = workspace.canonicalize()?;
        if !workspace.is_dir() {
            return Err(BrokerError::invalid("host workspace must be a directory"));
        }
        let mut process =
            DuplexProcess::spawn(codex_command(config, &workspace), config.request_timeout)?;
        let created = async {
            bootstrap_rpc(&mut process, 1, "initialize", codex_initialize_params()).await?;
            process.write(json!({"method":"initialized"})).await?;
            let result =
                bootstrap_rpc(&mut process, 2, "thread/start", json!({"cwd":workspace})).await?;
            let thread = &result["thread"];
            let id = thread["id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| BrokerError::internal("thread/start omitted exact thread ID"))?;
            let cwd = thread["cwd"]
                .as_str()
                .ok_or_else(|| BrokerError::internal("thread/start omitted workspace"))?;
            if Path::new(cwd).canonicalize()? != workspace {
                return Err(BrokerError::application(
                    ErrorCode::Conflict,
                    "thread/start returned a different workspace",
                ));
            }
            // Fresh app-server threads may remain staged until the first
            // durable metadata mutation or model turn. Naming this newly
            // created thread materializes its rollout before bootstrap exits,
            // so the managed connection can resume the exact ID without
            // spending a model turn or touching an existing conversation.
            bootstrap_rpc(
                &mut process,
                3,
                "thread/name/set",
                json!({"threadId":id,"name":"Cooperators managed session"}),
            )
            .await?;
            Ok(id.to_string())
        }
        .await;
        let shutdown = process.shutdown().await;
        let id = created?;
        shutdown?;
        Ok(id)
    }

    pub async fn spawn(config: CodexAdapterConfig, binding: &Binding) -> Result<Self, BrokerError> {
        let inner = AdapterState::new(binding, &["codex_app_server", "codex"])?;
        let process = DuplexProcess::spawn(
            codex_command(&config, &inner.workspace),
            config.request_timeout,
        )?;
        let mut adapter = Self {
            process,
            inner,
            next_request: 1,
        };
        let connected = async {
            adapter.rpc("initialize", codex_initialize_params()).await?;
            adapter
                .process
                .write(json!({"method":"initialized"}))
                .await?;
            let resumed = adapter
                .rpc(
                    "thread/resume",
                    json!({
                        "threadId": binding.exact_host_session_id, "excludeTurns":true
                    }),
                )
                .await?;
            adapter.validate_thread(&resumed["thread"])?;
            adapter
                .inner
                .set_state(thread_state(&resumed["thread"]["status"]));
            Ok::<_, BrokerError>(())
        }
        .await;
        if let Err(error) = connected {
            let _ = adapter.process.shutdown().await;
            return Err(error);
        }
        Ok(adapter)
    }

    fn validate_thread(&mut self, thread: &Value) -> Result<(), BrokerError> {
        if thread["id"] != self.inner.binding.exact_host_session_id {
            self.inner.offline("app-server returned a different thread");
            self.process.closed = true;
            let _ = self.process.child.start_kill();
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "app-server resumed a different thread",
            ));
        }
        let cwd = thread["cwd"]
            .as_str()
            .ok_or_else(|| BrokerError::internal("app-server thread omitted cwd"))?;
        if Path::new(cwd).canonicalize()? != self.inner.workspace {
            self.inner
                .offline("app-server thread belongs to a different workspace");
            self.process.closed = true;
            let _ = self.process.child.start_kill();
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "app-server thread workspace differs from binding",
            ));
        }
        Ok(())
    }

    fn frame(&mut self, value: Value) -> Result<(), BrokerError> {
        if !value.is_object() {
            return Err(BrokerError::internal(
                "app-server emitted a non-object frame",
            ));
        }
        let Some(method) = value["method"].as_str() else {
            if value.get("method").is_some() {
                return Err(BrokerError::internal("app-server method is not a string"));
            }
            return Ok(());
        };
        let params = &value["params"];
        if let Some(thread) = params["threadId"]
            .as_str()
            .or_else(|| params["conversationId"].as_str())
        {
            if thread != self.inner.binding.exact_host_session_id {
                return Ok(());
            }
        }
        if value.get("id").is_some() {
            if !(value["id"].is_string() || value["id"].is_i64() || value["id"].is_u64())
                || !params.is_object()
            {
                return Err(BrokerError::internal("invalid app-server request envelope"));
            }
            // Every server request is operator-visible, including tool/user
            // input requests that are not named requestApproval.
            return self.inner.approval(HostApproval {
                request_id: value["id"].clone(),
                method: method.to_string(),
                params: params.clone(),
            });
        }
        match method {
            "thread/status/changed" => self.inner.set_state(thread_state(&params["status"])),
            "thread/closed" => {
                return Err(BrokerError::internal("bound app-server thread closed"));
            }
            "serverRequest/resolved" => {
                self.inner
                    .approvals
                    .remove(&params["requestId"].to_string());
                self.inner.set_state(SessionState::Unknown);
            }
            "turn/started" => self.inner.set_state(SessionState::Busy),
            "turn/completed" => {
                self.inner
                    .approvals
                    .retain(|_, approval| approval.params["turnId"] != params["turn"]["id"]);
                self.inner.set_state(SessionState::Idle);
                self.inner.push(HostEvent::TurnCompleted {
                    turn_id: params["turn"]["id"].as_str().map(str::to_string),
                });
            }
            _ => self.inner.push(HostEvent::Notification { value }),
        }
        Ok(())
    }

    fn pump(&mut self) {
        // A single drain is bounded even if the child is continuously noisy.
        for _ in 0..FRAME_QUEUE {
            match self.process.try_next() {
                Some(Ok(frame)) => {
                    if let Err(error) = self.frame(frame) {
                        self.process.closed = true;
                        let _ = self.process.child.start_kill();
                        self.inner.offline(error.to_string());
                        break;
                    }
                }
                Some(Err(detail)) => {
                    self.inner.offline(detail);
                    break;
                }
                None => break,
            }
        }
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Result<Value, BrokerError> {
        let request_id = self.next_request;
        self.next_request += 1;
        self.process
            .write(json!({"id":request_id,"method":method,"params":params}))
            .await?;
        let deadline = Instant::now() + self.process.request_timeout;
        loop {
            let value = self.process.next(deadline).await?;
            if value.get("method").is_none() && value["id"] == request_id {
                if let Some(error) = value.get("error") {
                    return Err(BrokerError::internal(format!(
                        "app-server {method} failed: {error}"
                    )));
                }
                return value
                    .get("result")
                    .cloned()
                    .ok_or_else(|| BrokerError::internal("app-server response has no result"));
            }
            if let Err(error) = self.frame(value) {
                self.process.closed = true;
                let _ = self.process.child.start_kill();
                self.inner.offline(error.to_string());
                return Err(error);
            }
        }
    }

    async fn query_state(&mut self) -> Result<SessionState, BrokerError> {
        self.pump();
        if self.process.closed || self.inner.state == SessionState::Offline {
            return Ok(SessionState::Offline);
        }
        let result = self
            .rpc(
                "thread/read",
                json!({
                    "threadId":self.inner.binding.exact_host_session_id, "includeTurns":false
                }),
            )
            .await?;
        self.validate_thread(&result["thread"])?;
        self.inner
            .set_state(thread_state(&result["thread"]["status"]));
        Ok(self.inner.state)
    }

    async fn history(
        &mut self,
        wake: &Wake,
        _turn_id: Option<&str>,
    ) -> Result<Submission, BrokerError> {
        let mut cursor = Value::Null;
        let prompt = wake.prompt();
        for _ in 0..HISTORY_PAGES {
            let result = self
                .rpc(
                    "thread/items/list",
                    json!({
                        "threadId":self.inner.binding.exact_host_session_id,
                        "cursor":cursor, "limit":64, "sortDirection":"desc"
                    }),
                )
                .await?;
            let entries = result["data"]
                .as_array()
                .ok_or_else(|| BrokerError::internal("app-server history has no data array"))?;
            for entry in entries {
                let entry_turn = entry["turnId"].as_str();
                let item = &entry["item"];
                let matches_wake = item["type"] == "userMessage"
                    && user_text(item).as_deref() == Some(prompt.as_str());
                // A supplied turn ID is only a correlation hint. The exact
                // user-message payload proves acceptance of this wake.
                if matches_wake && entry_turn.is_some_and(|id| !id.is_empty()) {
                    return Ok(self.inner.accepted(wake, entry_turn.map(str::to_string)));
                }
            }
            cursor = result.get("nextCursor").cloned().unwrap_or(Value::Null);
            if cursor.is_null() {
                return Ok(unknown("wake acceptance not found in exact thread history; safe repeat requires bridge admission"));
            }
        }
        Ok(unknown(
            "history scan bound reached; acceptance remains unknown",
        ))
    }
}

impl HostAdapter for CodexAppServerAdapter {
    fn capabilities(&self) -> HostCapabilities {
        HostCapabilities {
            exact_session: true,
            reports_state: true,
            reconciles_history: true,
            approvals: true,
        }
    }

    fn session_state<'a>(&'a mut self, binding: &'a Binding) -> AdapterFuture<'a, SessionState> {
        Box::pin(async move {
            self.inner.check(binding, None)?;
            match self.query_state().await {
                Ok(state) => Ok(state),
                Err(error) => {
                    if self.process.closed {
                        self.inner.offline(error.to_string());
                    } else {
                        self.inner.set_state(SessionState::Unknown);
                    }
                    Ok(self.inner.state)
                }
            }
        })
    }

    fn submit_wake<'a>(
        &'a mut self,
        binding: &'a Binding,
        wake: &'a Wake,
    ) -> AdapterFuture<'a, Submission> {
        Box::pin(async move {
            self.inner.check(binding, Some(wake))?;
            if let Some(known) = self.inner.known(wake) {
                return Ok(known);
            }
            match self.query_state().await {
                Ok(SessionState::Idle) => {}
                Ok(SessionState::Busy) => return Ok(Submission::Busy),
                Ok(SessionState::WaitingForPermission) => return Ok(Submission::PermissionBlocked),
                Ok(state) => return Ok(unknown(format!("host is {state:?}; no turn submitted"))),
                Err(error) => return Ok(unknown(error)),
            }
            // Do not steer an active turn. Recheck state above before each new
            // submission; the bridge owns admission to this managed session.
            let result = self
                .rpc(
                    "turn/start",
                    json!({
                        "threadId":binding.exact_host_session_id,
                        "input":[{"type":"text","text":wake.prompt()}]
                    }),
                )
                .await;
            match result {
                Ok(result) => {
                    let Some(id) = result["turn"]["id"].as_str() else {
                        return Ok(unknown("turn/start response omitted turn ID"));
                    };
                    self.inner.set_state(SessionState::Busy);
                    Ok(self.inner.accepted(wake, Some(id.to_string())))
                }
                Err(error) => Ok(unknown(error)),
            }
        })
    }

    fn reconcile_wake<'a>(
        &'a mut self,
        binding: &'a Binding,
        wake: &'a Wake,
        turn_id: Option<&'a str>,
    ) -> AdapterFuture<'a, Submission> {
        Box::pin(async move {
            self.inner.check(binding, Some(wake))?;
            self.pump();
            if let Some(known) = self.inner.known(wake) {
                return Ok(known);
            }
            match self.query_state().await {
                Ok(SessionState::Offline) => Ok(unknown("host offline during reconciliation")),
                Err(error) => Ok(unknown(error)),
                Ok(_) => match self.history(wake, turn_id).await {
                    Ok(found @ Submission::Accepted { .. }) => Ok(found),
                    Ok(result) => match self.inner.state {
                        SessionState::WaitingForPermission => Ok(Submission::PermissionBlocked),
                        SessionState::Busy => Ok(Submission::Busy),
                        _ => Ok(result),
                    },
                    Err(error) => Ok(unknown(error)),
                },
            }
        })
    }

    fn drain_events(&mut self) -> Vec<HostEvent> {
        self.pump();
        self.inner.take_events()
    }

    fn answer_approval<'a>(
        &'a mut self,
        request_id: &'a Value,
        response: Value,
    ) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            self.pump();
            let key = request_id.to_string();
            let approval = self.inner.approvals.get(&key).ok_or_else(|| {
                BrokerError::invalid("unknown or already answered host approval request")
            })?;
            validate_codex_response(&approval.method, &response)?;
            self.process
                .write(json!({"id":request_id,"result":response}))
                .await?;
            self.inner.approvals.remove(&key);
            self.inner.set_state(SessionState::Busy);
            Ok(())
        })
    }

    fn shutdown<'a>(&'a mut self) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            self.inner.set_state(SessionState::Offline);
            self.process.shutdown().await
        })
    }
}

fn validate_codex_response(method: &str, response: &Value) -> Result<(), BrokerError> {
    if !response.is_object() {
        return Err(BrokerError::invalid(
            "host approval response must be an object",
        ));
    }
    match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            if response.as_object().is_some_and(|fields| fields.len() != 1) {
                return Err(BrokerError::invalid(
                    "approval response only accepts a decision",
                ));
            }
            if !matches!(
                response["decision"].as_str(),
                Some("accept" | "decline" | "cancel")
            ) {
                return Err(BrokerError::invalid(
                    "only per-request accept, decline, or cancel decisions are supported",
                ));
            }
        }
        "execCommandApproval" | "applyPatchApproval" => {
            if response.as_object().is_some_and(|fields| fields.len() != 1) {
                return Err(BrokerError::invalid(
                    "approval response only accepts a decision",
                ));
            }
            if !matches!(
                response["decision"].as_str(),
                Some("approved" | "denied" | "abort")
            ) {
                return Err(BrokerError::invalid(
                    "only per-request legacy approval decisions are supported",
                ));
            }
        }
        "item/permissions/requestApproval" => {
            if response.get("scope").is_some_and(|scope| scope != "turn") {
                return Err(BrokerError::invalid(
                    "session-wide permission grants are not supported",
                ));
            }
            if !response["permissions"].is_object() {
                return Err(BrokerError::invalid(
                    "permission approval needs an explicit permissions object",
                ));
            }
        }
        _ => {} // Tool calls and user-input requests use their explicit result schema.
    }
    Ok(())
}

/// Persistent Claude host-mode subprocess. The CLI is a transport, not a
/// durable acceptance journal: after adapter restart reconciliation explicitly
/// reports Unknown and the bridge may retry the same harmless wake reference.
pub struct ClaudeHostAdapter {
    process: DuplexProcess,
    inner: AdapterState,
    pending_echo: Option<(String, String)>,
}

impl ClaudeHostAdapter {
    pub async fn spawn(
        config: ClaudeAdapterConfig,
        binding: &Binding,
    ) -> Result<Self, BrokerError> {
        let mut inner = AdapterState::new(binding, &["claude_host", "claude"])?;
        if !is_uuid(&binding.exact_host_session_id) {
            return Err(BrokerError::invalid(
                "Claude binding requires an exact session UUID",
            ));
        }
        let mut command = Command::new(&config.executable);
        command.args([
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-prompt-tool",
            "stdio",
            "--permission-mode",
            "manual",
            "--replay-user-messages",
        ]);
        command
            .arg(if config.resume_existing {
                "--resume"
            } else {
                "--session-id"
            })
            .arg(&binding.exact_host_session_id);
        if let Some(path) = config.mcp_config {
            command.arg("--mcp-config").arg(path);
        }
        command.current_dir(&inner.workspace).envs(&config.env);
        let process = DuplexProcess::spawn(command, config.request_timeout)?;
        // The CLI emits system/init on the first input, not necessarily at
        // spawn. Configured is not verified; binding verification still needs
        // the model's authenticated activation nonce through the bridge.
        inner.set_state(SessionState::Idle);
        Ok(Self {
            process,
            inner,
            pending_echo: None,
        })
    }

    fn frame(&mut self, value: Value) -> Result<(), BrokerError> {
        if !value.is_object() || !value["type"].is_string() {
            return Err(BrokerError::internal(
                "invalid Claude stream message envelope",
            ));
        }
        if matches!(
            value["type"].as_str(),
            Some("user" | "assistant" | "result" | "system")
        ) && !value["session_id"].is_string()
        {
            return Err(BrokerError::internal(
                "Claude session message omitted exact session ID",
            ));
        }
        if let Some(session_id) = value["session_id"].as_str() {
            if session_id != self.inner.binding.exact_host_session_id {
                self.inner.offline("Claude returned a different session ID");
                self.process.closed = true;
                let _ = self.process.child.start_kill();
                return Err(BrokerError::application(
                    ErrorCode::Conflict,
                    "Claude session ID differs from exact binding",
                ));
            }
        }
        match value["type"].as_str() {
            Some("control_request") => {
                let id = value
                    .get("request_id")
                    .filter(|id| id.as_str().is_some_and(|id| !id.is_empty()))
                    .ok_or_else(|| {
                        BrokerError::internal("Claude control request omitted request_id")
                    })?;
                if !value["request"].is_object() || !value["request"]["subtype"].is_string() {
                    return Err(BrokerError::internal(
                        "invalid Claude control request envelope",
                    ));
                }
                self.inner.approval(HostApproval {
                    request_id: id.clone(),
                    method: value["request"]["subtype"]
                        .as_str()
                        .unwrap_or("unknown_control_request")
                        .to_string(),
                    params: value["request"].clone(),
                })?;
            }
            Some("control_cancel_request") => {
                self.inner
                    .approvals
                    .remove(&value["request_id"].to_string());
                self.inner.set_state(SessionState::Busy);
                self.inner.push(HostEvent::Notification { value });
            }
            Some("user") => {
                if let Some((wake_id, prompt)) = &self.pending_echo {
                    if value["session_id"] == self.inner.binding.exact_host_session_id
                        && user_text(&value["message"]).as_deref() == Some(prompt.as_str())
                    {
                        if self.inner.accepted.len() == ACCEPTED_CACHE {
                            self.inner.accepted.pop_front();
                        }
                        self.inner.accepted.push_back((
                            wake_id.clone(),
                            value["uuid"].as_str().map(str::to_string),
                        ));
                        self.pending_echo = None;
                    }
                }
            }
            Some("assistant") => self.inner.set_state(SessionState::Busy),
            Some("result") => {
                // A final turn result cancels any requests that the provider
                // withdrew while ending/interruption. An old operator reply
                // must not revive a request on the next turn.
                self.inner.approvals.clear();
                self.inner.set_state(SessionState::Idle);
                self.inner.push(HostEvent::TurnCompleted {
                    turn_id: value["uuid"].as_str().map(str::to_string),
                });
            }
            _ => self.inner.push(HostEvent::Notification { value }),
        }
        Ok(())
    }

    fn pump(&mut self) {
        for _ in 0..FRAME_QUEUE {
            match self.process.try_next() {
                Some(Ok(frame)) => {
                    if let Err(error) = self.frame(frame) {
                        self.process.closed = true;
                        let _ = self.process.child.start_kill();
                        self.inner.offline(error.to_string());
                        break;
                    }
                }
                Some(Err(detail)) => {
                    self.inner.offline(detail);
                    break;
                }
                None => break,
            }
        }
    }
}

impl HostAdapter for ClaudeHostAdapter {
    fn capabilities(&self) -> HostCapabilities {
        HostCapabilities {
            exact_session: true,
            reports_state: true,
            reconciles_history: false,
            approvals: true,
        }
    }

    fn session_state<'a>(&'a mut self, binding: &'a Binding) -> AdapterFuture<'a, SessionState> {
        Box::pin(async move {
            self.inner.check(binding, None)?;
            self.pump();
            Ok(self.inner.state)
        })
    }

    fn submit_wake<'a>(
        &'a mut self,
        binding: &'a Binding,
        wake: &'a Wake,
    ) -> AdapterFuture<'a, Submission> {
        Box::pin(async move {
            self.inner.check(binding, Some(wake))?;
            self.pump();
            if let Some(known) = self.inner.known(wake) {
                return Ok(known);
            }
            match self.inner.state {
                SessionState::Idle => {}
                SessionState::Busy => return Ok(Submission::Busy),
                SessionState::WaitingForPermission => return Ok(Submission::PermissionBlocked),
                state => {
                    return Ok(unknown(format!(
                        "Claude host is {state:?}; no wake submitted"
                    )))
                }
            }
            let prompt = wake.prompt();
            self.pending_echo = Some((wake.wake_id.clone(), prompt.clone()));
            self.inner.set_state(SessionState::Busy);
            if let Err(error) = self
                .process
                .write(json!({
                    "type":"user", "session_id":binding.exact_host_session_id,
                    "message":{"role":"user", "content":[{"type":"text", "text":prompt}]},
                    "parent_tool_use_id":null
                }))
                .await
            {
                return Ok(unknown(error));
            }
            let deadline = Instant::now() + self.process.request_timeout;
            loop {
                if let Some(known) = self.inner.known(wake) {
                    return Ok(known);
                }
                if self.inner.state == SessionState::WaitingForPermission {
                    return Ok(Submission::PermissionBlocked);
                }
                match self.process.next(deadline).await {
                    Ok(frame) => {
                        if let Err(error) = self.frame(frame) {
                            self.process.closed = true;
                            let _ = self.process.child.start_kill();
                            self.inner.offline(error.to_string());
                            return Ok(unknown(error));
                        }
                    }
                    Err(error) => {
                        if self.process.closed {
                            self.inner.offline(error.to_string());
                        }
                        return Ok(unknown(error));
                    }
                }
            }
        })
    }

    fn reconcile_wake<'a>(
        &'a mut self,
        binding: &'a Binding,
        wake: &'a Wake,
        _turn_id: Option<&'a str>,
    ) -> AdapterFuture<'a, Submission> {
        Box::pin(async move {
            self.inner.check(binding, Some(wake))?;
            self.pump();
            if let Some(known) = self.inner.known(wake) {
                return Ok(known);
            }
            Ok(match self.inner.state {
                SessionState::Busy => Submission::Busy,
                SessionState::WaitingForPermission => Submission::PermissionBlocked,
                _ => unknown("Claude CLI has no history acceptance query; retain and retry the same wake reference when idle"),
            })
        })
    }

    fn drain_events(&mut self) -> Vec<HostEvent> {
        self.pump();
        self.inner.take_events()
    }

    fn answer_approval<'a>(
        &'a mut self,
        request_id: &'a Value,
        response: Value,
    ) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            self.pump();
            let key = request_id.to_string();
            let approval = self.inner.approvals.get(&key).ok_or_else(|| {
                BrokerError::invalid("unknown or already answered Claude control request")
            })?;
            if approval.method == "can_use_tool" {
                if !matches!(response["behavior"].as_str(), Some("allow" | "deny")) {
                    return Err(BrokerError::invalid(
                        "Claude tool response needs explicit allow or deny behavior",
                    ));
                }
                let fields = response.as_object().ok_or_else(|| {
                    BrokerError::invalid("Claude control response must be an object")
                })?;
                if fields.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "behavior" | "updatedInput" | "message" | "interrupt"
                    )
                }) {
                    return Err(BrokerError::invalid(
                        "standing permission updates are not supported",
                    ));
                }
                if response
                    .get("updatedInput")
                    .is_some_and(|input| !input.is_object())
                    || response
                        .get("message")
                        .is_some_and(|message| !message.is_string())
                    || response
                        .get("interrupt")
                        .is_some_and(|interrupt| !interrupt.is_boolean())
                {
                    return Err(BrokerError::invalid(
                        "invalid Claude per-request response fields",
                    ));
                }
            } else if !response.is_object() {
                return Err(BrokerError::invalid(
                    "Claude control response must be an object",
                ));
            }
            self.process
                .write(json!({
                    "type":"control_response", "response":{
                        "subtype":"success", "request_id":request_id, "response":response
                    }
                }))
                .await?;
            self.inner.approvals.remove(&key);
            self.inner.set_state(SessionState::Busy);
            Ok(())
        })
    }

    fn shutdown<'a>(&'a mut self) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            self.inner.set_state(SessionState::Offline);
            self.process.shutdown().await
        })
    }
}

fn is_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}
