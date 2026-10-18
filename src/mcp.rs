//! MCP facade over the broker JSON-RPC surface.
//!
//! Two transports share one tool catalog and one forwarding path:
//!
//! - [`serve_stdio`] runs an MCP server on stdin/stdout (`coop mcp-stdio`).
//!   Each tool call is forwarded to the running daemon over the Unix socket.
//! - [`http_service`] wraps the same handler in rmcp's streamable HTTP
//!   service. The daemon mounts it at `POST /mcp` behind bearer auth. HTTP
//!   tool calls run against the daemon state in-process rather than looping
//!   back through the socket.
//!
//! A third backend ([`Backend::Embedded`]) lets `coop mcp-stdio --embedded`
//! serve a bus root without a daemon. It exists for tests that cannot bind
//! sockets and for single-process recovery/debugging; the daemon remains the
//! only writer in normal operation.
//!
//! Tool names are the daemon method names prefixed with `coop_`
//! (`send_message` -> `coop_send_message`). The one exception is
//! `daemon.status` -> `coop_get_status`, matching design §9.

use std::convert::Infallible;
use std::fmt::Display;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use http::header;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body::Body;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, Implementation, JsonObject,
    ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::ErrorData as McpError;
use rmcp::ServiceExt;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tower_service::Service;

use crate::client::{self, ClientError};
use crate::error::ErrorCode;
use crate::rpc::{self, RpcError, RpcRequest, RpcResponse};
use crate::server::{self, ConnectionSession, DaemonState};
use crate::VERSION;

/// Default streamable-HTTP listen address (design §2).
pub const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:7420";

/// Deadline used for ordinary forwarded calls.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);
/// Extra time granted on top of a long-poll window.
const LONG_POLL_SLACK: Duration = Duration::from_secs(15);

/// One MCP tool mapped to one daemon JSON-RPC method.
#[derive(Debug)]
pub struct ToolSpec {
    /// MCP tool name (`coop_*`).
    pub tool: &'static str,
    /// Daemon JSON-RPC method name.
    pub method: &'static str,
    /// Human-readable description shown by `tools/list`.
    pub description: &'static str,
    /// Builds the tool's JSON Schema.
    pub schema: fn() -> Value,
}

/// The complete v1 tool surface, in design §9 order.
pub const TOOL_SPECS: &[ToolSpec] = &[
    ToolSpec {
        tool: "coop_register_agent",
        method: "register_agent",
        description:
            "Register an agent name and return its agent id and token. Re-registering a name with its current token rotates the token.",
        schema: schema_register_agent,
    },
    ToolSpec {
        tool: "coop_send_message",
        method: "send_message",
        description: "Send a durable direct message to another agent's inbox.",
        schema: schema_send_message,
    },
    ToolSpec {
        tool: "coop_wait_message",
        method: "wait_message",
        description:
            "Long-poll the authenticated agent's inbox. Peek semantics: pass next_cursor to coop_ack to consume.",
        schema: schema_wait_message,
    },
    ToolSpec {
        tool: "coop_ack",
        method: "ack",
        description: "Advance the authenticated agent's acknowledged inbox cursor.",
        schema: schema_ack,
    },
    ToolSpec {
        tool: "coop_heartbeat",
        method: "heartbeat",
        description: "Record a presence heartbeat for the authenticated agent.",
        schema: schema_heartbeat,
    },
    ToolSpec {
        tool: "coop_list_agents",
        method: "list_agents",
        description: "List registered agents and their presence state.",
        schema: schema_list_agents,
    },
    ToolSpec {
        tool: "coop_publish",
        method: "publish",
        description: "Publish a message to a named channel with idempotent dedupe.",
        schema: schema_publish,
    },
    ToolSpec {
        tool: "coop_read_channel",
        method: "read_channel",
        description: "Read channel records after a cursor; wait_s long-polls for new records.",
        schema: schema_read_channel,
    },
    ToolSpec {
        tool: "coop_create_queue",
        method: "create_queue",
        description: "Create a named work queue.",
        schema: schema_create_queue,
    },
    ToolSpec {
        tool: "coop_enqueue",
        method: "enqueue",
        description: "Append a task payload to a queue.",
        schema: schema_enqueue,
    },
    ToolSpec {
        tool: "coop_claim_task",
        method: "claim_task",
        description: "Claim the oldest ready task under a fenced lease.",
        schema: schema_claim_task,
    },
    ToolSpec {
        tool: "coop_renew_lease",
        method: "renew_lease",
        description: "Renew a task or document edit lease without changing its fence.",
        schema: schema_renew_lease,
    },
    ToolSpec {
        tool: "coop_complete_task",
        method: "complete_task",
        description: "Complete a claimed task; the fence must match the live lease.",
        schema: schema_complete_task,
    },
    ToolSpec {
        tool: "coop_fail_task",
        method: "fail_task",
        description: "Fail a claimed attempt; the task requeues or dead-letters.",
        schema: schema_fail_task,
    },
    ToolSpec {
        tool: "coop_publish_doc",
        method: "publish_doc",
        description: "Publish version 1 of a named document (inline or blob).",
        schema: schema_publish_doc,
    },
    ToolSpec {
        tool: "coop_update_doc",
        method: "update_doc",
        description:
            "Append a document version using compare-and-swap on base_version, optionally under an edit lease.",
        schema: schema_update_doc,
    },
    ToolSpec {
        tool: "coop_claim_doc",
        method: "claim_doc",
        description: "Claim exclusive editing rights on a document.",
        schema: schema_claim_doc,
    },
    ToolSpec {
        tool: "coop_release_doc",
        method: "release_doc",
        description: "Release a document edit lease with its fence.",
        schema: schema_release_doc,
    },
    ToolSpec {
        tool: "coop_read_doc",
        method: "read_doc",
        description: "Read a document's head or a pinned historical version.",
        schema: schema_read_doc,
    },
    ToolSpec {
        tool: "coop_list_docs",
        method: "list_docs",
        description: "List document summaries, optionally filtered by name prefix.",
        schema: schema_list_docs,
    },
    ToolSpec {
        tool: "coop_get_status",
        method: "daemon.status",
        description: "Return daemon version, pid, uptime, root, and per-log sequence heads.",
        schema: schema_get_status,
    },
];

/// Finds the tool spec for an MCP tool name.
pub fn spec_for_tool(tool: &str) -> Option<&'static ToolSpec> {
    TOOL_SPECS.iter().find(|spec| spec.tool == tool)
}

/// Finds the tool spec for a daemon JSON-RPC method.
pub fn spec_for_method(method: &str) -> Option<&'static ToolSpec> {
    TOOL_SPECS.iter().find(|spec| spec.method == method)
}

/// The `tools/list` catalog with freshly built schemas.
pub fn tool_catalog() -> Vec<Tool> {
    TOOL_SPECS
        .iter()
        .map(|spec| {
            Tool::new(
                spec.tool,
                spec.description,
                Arc::new(schema_object((spec.schema)())),
            )
        })
        .collect()
}

fn schema_object(value: Value) -> JsonObject {
    match value {
        Value::Object(object) => object,
        other => panic!("tool schema must be a JSON object, got {other}"),
    }
}

// ---------------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------------

/// Where a tool call is executed.
#[derive(Clone, Debug)]
pub enum Backend {
    /// Forward over the daemon Unix socket (production `coop mcp-stdio`).
    Uds {
        /// Socket path resolved from the bus root or `COOP_SOCKET`.
        socket: PathBuf,
    },
    /// Dispatch directly against in-process daemon state (HTTP endpoint and
    /// `mcp-stdio --embedded`).
    Embedded(Arc<DaemonState>),
}

impl Backend {
    /// Backend that forwards to a daemon socket.
    pub fn uds(socket: PathBuf) -> Self {
        Self::Uds { socket }
    }

    /// Backend that dispatches against in-process state.
    pub fn embedded(state: Arc<DaemonState>) -> Self {
        Self::Embedded(state)
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        match self {
            Self::Uds { socket } => {
                let deadline = deadline_for(method, &params);
                client::call_at(socket, method, params, deadline)
                    .await
                    .map_err(client_error_to_rpc)
            }
            Self::Embedded(state) => {
                let request = RpcRequest {
                    id: Some(Value::from(1)),
                    method: method.to_string(),
                    params,
                    is_notification: false,
                };
                let mut session = ConnectionSession::default();
                match server::dispatch(state, &mut session, request).await {
                    Some(response) => response.into_result(),
                    None => Err(RpcError::internal(
                        "daemon produced no response for a request",
                    )),
                }
            }
        }
    }
}

fn deadline_for(method: &str, params: &Value) -> Duration {
    let seconds = match method {
        "wait_message" => params
            .get("timeout_s")
            .and_then(Value::as_u64)
            .unwrap_or(30)
            .min(600),
        "read_channel" => params
            .get("wait_s")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(crate::channels::MAX_READ_WAIT_SECS),
        _ => 0,
    };
    if seconds == 0 {
        CALL_TIMEOUT
    } else {
        Duration::from_secs(seconds) + LONG_POLL_SLACK
    }
}

fn client_error_to_rpc(error: ClientError) -> RpcError {
    match error {
        ClientError::Rpc(error) => error,
        ClientError::Unreachable(error) => RpcError::application(
            ErrorCode::DaemonUnreachable,
            format!("daemon unreachable: {error}"),
        ),
        ClientError::Timeout => RpcError::application(
            ErrorCode::DaemonUnreachable,
            "daemon unreachable: request timed out",
        ),
        ClientError::Protocol(message) => RpcError::invalid_request(format!(
            "daemon returned an invalid JSON-RPC response: {message}"
        )),
    }
}

// ---------------------------------------------------------------------------
// Canonical results (shared by the conformance suite)
// ---------------------------------------------------------------------------

/// Canonical JSON-RPC outcome: `{ok: true, result}` or `{ok: false, error}`.
pub fn canonical_response(response: RpcResponse) -> Value {
    match response.into_result() {
        Ok(result) => json!({"ok": true, "result": result}),
        Err(error) => json!({"ok": false, "error": canonical_error(&error)}),
    }
}

/// Canonical JSON-RPC error object.
pub fn canonical_error(error: &RpcError) -> Value {
    json!({
        "code": error.code,
        "message": error.message,
        "data": error.data,
    })
}

/// Canonical outcome of an MCP tool result.
///
/// Both transports deliberately report tool failures as `CallToolResult`
/// errors (`isError: true`) whose structured content is the same canonical
/// error object the UDS layer produces, so a conformance transcript can be
/// replayed against either surface.
pub fn canonical_tool_result(result: &CallToolResult) -> Value {
    let structured = result.structured_content.clone().unwrap_or(Value::Null);
    if result.is_error.unwrap_or(false) {
        json!({"ok": false, "error": structured})
    } else {
        json!({"ok": true, "result": structured})
    }
}

/// Structured error for failures before a daemon call could complete.
fn tool_error(code: i64, message: impl Into<String>, symbolic: &str) -> CallToolResult {
    CallToolResult::structured_error(json!({
        "code": code,
        "message": message.into(),
        "data": {"code": symbolic},
    }))
}

// ---------------------------------------------------------------------------
// Server handler
// ---------------------------------------------------------------------------

/// How the facade obtains the agent token for a tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// Token captured from `COOP_TOKEN`/`--token`, updated by register calls.
    Captured,
    /// Token read from the `Authorization: Bearer` header of each HTTP request.
    HttpHeader,
}

/// MCP `ServerHandler` shared by stdio and streamable HTTP.
#[derive(Clone)]
pub struct CoopMcpHandler {
    backend: Backend,
    token: Arc<RwLock<Option<String>>>,
    auth: AuthMode,
}

impl CoopMcpHandler {
    /// Handler for `coop mcp-stdio`.
    pub fn stdio(backend: Backend, token: Option<String>) -> Self {
        Self {
            backend,
            token: Arc::new(RwLock::new(token)),
            auth: AuthMode::Captured,
        }
    }

    /// Handler for the HTTP endpoint; identity comes from each request header.
    pub fn http(backend: Backend) -> Self {
        Self {
            backend,
            token: Arc::new(RwLock::new(None)),
            auth: AuthMode::HttpHeader,
        }
    }

    /// Current captured token (stdio mode).
    pub async fn captured_token(&self) -> Option<String> {
        self.token.read().await.clone()
    }

    /// Executes one tool call without HTTP request context.
    ///
    /// This is the same code path used by `tools/call` on both transports and
    /// is what the conformance suite exercises as "the MCP tool layer".
    pub async fn dispatch_tool(
        &self,
        tool_name: &str,
        arguments: Option<JsonObject>,
    ) -> CallToolResponse {
        self.dispatch_with_token(tool_name, arguments, None).await
    }

    async fn dispatch_with_token(
        &self,
        tool_name: &str,
        arguments: Option<JsonObject>,
        http_token: Option<String>,
    ) -> CallToolResponse {
        let Some(spec) = spec_for_tool(tool_name) else {
            return tool_error(
                rpc::METHOD_NOT_FOUND,
                format!("tool not found: {tool_name}"),
                "METHOD_NOT_FOUND",
            )
            .into();
        };

        let token = match self.auth {
            AuthMode::Captured => self.token.read().await.clone(),
            AuthMode::HttpHeader => http_token,
        };

        let mut params = Value::Object(arguments.unwrap_or_default());
        if let Some(token) = token {
            if let Value::Object(map) = &mut params {
                map.entry("token").or_insert(Value::String(token));
            }
        }

        match self.backend.call(spec.method, params).await {
            Ok(result) => {
                if spec.method == "register_agent" {
                    if let Some(new_token) = result.get("token").and_then(Value::as_str) {
                        if self.auth == AuthMode::Captured {
                            *self.token.write().await = Some(new_token.to_string());
                        }
                    }
                }
                CallToolResult::structured(result).into()
            }
            Err(error) => CallToolResult::structured_error(canonical_error(&error)).into(),
        }
    }

    fn http_token(&self, context: &RequestContext<RoleServer>) -> Option<String> {
        let parts = context.extensions.get::<http::request::Parts>()?;
        bearer_token(&parts.headers)
    }
}

impl ServerHandler for CoopMcpHandler {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::new(ServerCapabilities::builder().enable_tools().build());
        info.server_info = Implementation::new("cooperators", VERSION);
        info.instructions = Some(
            "Cooperators inter-agent broker. Tools forward to the local cooperatorsd \
             JSON-RPC surface. Every tool except coop_register_agent requires an agent \
             token (COOP_TOKEN/--token for stdio, Authorization: Bearer for HTTP)."
                .to_string(),
        );
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(
            cached_tool_catalog().to_vec(),
        ))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        cached_tool_catalog()
            .iter()
            .find(|tool| tool.name.as_ref() == name)
            .cloned()
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let http_token = if self.auth == AuthMode::HttpHeader {
            self.http_token(&context)
        } else {
            None
        };
        Ok(self
            .dispatch_with_token(request.name.as_ref(), request.arguments, http_token)
            .await)
    }
}

// ---------------------------------------------------------------------------
// stdio transport
// ---------------------------------------------------------------------------

/// Runs the MCP server over stdin/stdout until the client closes the pipe.
pub async fn serve_stdio(handler: CoopMcpHandler) -> Result<(), Box<dyn std::error::Error>> {
    let (stdin, stdout) = rmcp::transport::stdio();
    let running = handler.serve((stdin, stdout)).await?;
    let _ = running.waiting().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Streamable HTTP transport
// ---------------------------------------------------------------------------

/// Concrete rmcp streamable HTTP service type used by the daemon and tests.
pub type CoopHttpService = StreamableHttpService<CoopMcpHandler, LocalSessionManager>;

/// Builds the streamable HTTP service backed by in-process daemon state.
///
/// The service is stateless (SEP-2567) and prefers plain JSON responses; tool
/// calls never emit notifications, so SSE is unnecessary for this surface.
pub fn http_service(state: Arc<DaemonState>) -> CoopHttpService {
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_sse_keep_alive(None);
    StreamableHttpService::new(
        move || Ok(CoopMcpHandler::http(Backend::embedded(state.clone()))),
        Arc::new(LocalSessionManager::default()),
        config,
    )
}

/// Serves `/mcp` on an already-bound listener until it fails.
pub async fn serve_http(listener: TcpListener, state: Arc<DaemonState>) -> io::Result<()> {
    let address = listener.local_addr()?;
    let service = http_service(Arc::clone(&state));
    eprintln!("coop daemon: MCP streamable HTTP on http://{address}/mcp");
    loop {
        let (stream, _peer) = listener.accept().await?;
        let io = hyper_util::rt::TokioIo::new(stream);
        let service = service.clone();
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let connection = hyper::server::conn::http1::Builder::new().serve_connection(
                io,
                hyper::service::service_fn(move |request| {
                    let service = service.clone();
                    let state = Arc::clone(&state);
                    async move { Ok::<_, Infallible>(handle_http(&service, &state, request).await) }
                }),
            );
            let _ = connection.await;
        });
    }
}

/// Full HTTP request path: path routing, bearer authentication, and the rmcp
/// streamable HTTP service. Exposed so tests can drive it in-process without
/// binding a socket.
pub async fn handle_http<B>(
    service: &CoopHttpService,
    state: &Arc<DaemonState>,
    request: Request<B>,
) -> Response<BoxBody<Bytes, Infallible>>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Display,
{
    if request.uri().path() != "/mcp" {
        return http_json(
            StatusCode::NOT_FOUND,
            json!({"error": "not found; the MCP endpoint is POST /mcp"}),
        );
    }

    let Some(token) = bearer_token(request.headers()) else {
        return unauthorized("missing Authorization: Bearer token");
    };
    if state.authenticate(&token).await.is_err() {
        return unauthorized("invalid bearer token");
    }

    let mut service = service.clone();
    match service.call(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    }
}

fn unauthorized(message: &str) -> Response<BoxBody<Bytes, Infallible>> {
    let error = RpcError::application(ErrorCode::Unauthenticated, message);
    let body = serde_json::to_value(RpcResponse::failure(Value::Null, error))
        .unwrap_or_else(|_| json!({"error": message}));
    let mut response = http_json(StatusCode::UNAUTHORIZED, body);
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        header::HeaderValue::from_static("Bearer realm=\"cooperators\""),
    );
    response
}

fn http_json(status: StatusCode, body: Value) -> Response<BoxBody<Bytes, Infallible>> {
    let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(bytes)).boxed())
        .expect("valid HTTP response")
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then(|| token.to_string())
}

// ---------------------------------------------------------------------------
// Schemas (mirror the daemon param structs in server.rs)
// ---------------------------------------------------------------------------

fn body_schema() -> Value {
    json!({
        "oneOf": [
            {"type": "string", "description": "text/plain body"},
            {
                "type": "object",
                "properties": {
                    "mime": {"type": "string", "description": "Defaults to text/plain"},
                    "text": {"type": "string"}
                },
                "required": ["text"],
                "additionalProperties": false
            }
        ],
        "description": "Message body: a plain string or a {mime, text} object."
    })
}

fn channel_cursor_schema() -> Value {
    json!({
        "oneOf": [
            {"type": "string", "description": "\"<seq>\" or \"<file>#<seq>\""},
            {"type": "integer", "minimum": 0},
            {
                "type": "object",
                "properties": {
                    "file": {"type": "string"},
                    "seq": {"type": "integer", "minimum": 0}
                },
                "required": ["seq"],
                "additionalProperties": false
            }
        ]
    })
}

fn schema_register_agent() -> Value {
    json!({
        "type": "object",
        "properties": {
            "name": {"type": "string", "minLength": 1},
            "kind": {"type": "string", "description": "Agent kind, e.g. codex or claude"},
            "capabilities": {"type": "array", "items": {"type": "string"}},
            "token": {
                "type": "string",
                "description": "Existing token when rotating; omit for a new agent."
            }
        },
        "required": ["name"],
        "additionalProperties": false
    })
}

fn schema_send_message() -> Value {
    json!({
        "type": "object",
        "properties": {
            "to": {"type": "string", "minLength": 1, "description": "Recipient agent name or id"},
            "kind": {"type": "string", "minLength": 1, "description": "Message kind"},
            "body": body_schema(),
            "text": {"type": "string", "description": "Convenience text/plain body"},
            "reply_to": {"type": "string"},
            "idempotency_key": {"type": "string"},
            "ttl_s": {"type": "integer", "minimum": 1, "description": "Message TTL in seconds"}
        },
        "required": ["to", "kind"],
        "anyOf": [{"required": ["body"]}, {"required": ["text"]}],
        "additionalProperties": false
    })
}

fn schema_wait_message() -> Value {
    json!({
        "type": "object",
        "properties": {
            "cursor": {"type": "string", "description": "Opaque cursor; defaults to the acked cursor"},
            "timeout_s": {"type": "integer", "minimum": 0, "maximum": 600}
        },
        "additionalProperties": false
    })
}

fn schema_ack() -> Value {
    json!({
        "type": "object",
        "properties": {"cursor": {"type": "string", "minLength": 1}},
        "required": ["cursor"],
        "additionalProperties": false
    })
}

fn schema_heartbeat() -> Value {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
}

fn schema_list_agents() -> Value {
    json!({
        "type": "object",
        "properties": {"include_stale": {"type": "boolean"}},
        "additionalProperties": false
    })
}

fn schema_publish() -> Value {
    json!({
        "type": "object",
        "properties": {
            "channel": {"type": "string", "minLength": 1},
            "body": body_schema(),
            "text": {"type": "string"},
            "idempotency_key": {"type": "string"}
        },
        "required": ["channel"],
        "anyOf": [{"required": ["body"]}, {"required": ["text"]}],
        "additionalProperties": false
    })
}

fn schema_read_channel() -> Value {
    json!({
        "type": "object",
        "properties": {
            "channel": {"type": "string", "minLength": 1},
            "cursor": channel_cursor_schema(),
            "limit": {"type": "integer", "minimum": 1},
            "wait_s": {
                "type": "integer",
                "minimum": 0,
                "maximum": crate::channels::MAX_READ_WAIT_SECS
            }
        },
        "required": ["channel"],
        "additionalProperties": false
    })
}

fn schema_create_queue() -> Value {
    json!({
        "type": "object",
        "properties": {
            "queue": {"type": "string", "minLength": 1},
            "name": {"type": "string", "minLength": 1, "description": "Alias for queue"}
        },
        "anyOf": [{"required": ["queue"]}, {"required": ["name"]}],
        "additionalProperties": false
    })
}

fn schema_enqueue() -> Value {
    json!({
        "type": "object",
        "properties": {
            "queue": {"type": "string", "minLength": 1},
            "payload": {"description": "Arbitrary JSON task payload"},
            "max_attempts": {"type": "integer", "minimum": 1, "maximum": 1000}
        },
        "required": ["queue", "payload"],
        "additionalProperties": false
    })
}

fn schema_claim_task() -> Value {
    json!({
        "type": "object",
        "properties": {
            "queue": {"type": "string", "minLength": 1},
            "lease_ttl_s": {
                "type": "integer",
                "minimum": 1,
                "maximum": crate::queues::MAX_LEASE_TTL_S
            }
        },
        "required": ["queue"],
        "additionalProperties": false
    })
}

fn schema_renew_lease() -> Value {
    json!({
        "type": "object",
        "properties": {"lease_id": {"type": "string", "minLength": 1}},
        "required": ["lease_id"],
        "additionalProperties": false
    })
}

fn schema_complete_task() -> Value {
    json!({
        "type": "object",
        "properties": {
            "lease_id": {"type": "string", "minLength": 1},
            "fence": {"type": "integer", "minimum": 0},
            "outcome": {"description": "Arbitrary JSON outcome"},
            "receipt": {"description": "Optional arbitrary JSON receipt"}
        },
        "required": ["lease_id", "fence", "outcome"],
        "additionalProperties": false
    })
}

fn schema_fail_task() -> Value {
    json!({
        "type": "object",
        "properties": {
            "lease_id": {"type": "string", "minLength": 1},
            "fence": {"type": "integer", "minimum": 0},
            "reason": {"type": "string", "minLength": 1}
        },
        "required": ["lease_id", "fence", "reason"],
        "additionalProperties": false
    })
}

fn schema_publish_doc() -> Value {
    json!({
        "type": "object",
        "properties": {
            "name": {"type": "string", "minLength": 1},
            "mime": {"type": "string"},
            "body": body_schema(),
            "text": {"type": "string"},
            "title": {"type": "string"}
        },
        "required": ["name"],
        "anyOf": [{"required": ["body"]}, {"required": ["text"]}],
        "additionalProperties": false
    })
}

fn schema_update_doc() -> Value {
    json!({
        "type": "object",
        "properties": {
            "name": {"type": "string", "minLength": 1},
            "base_version": {"type": "integer", "minimum": 1},
            "body": body_schema(),
            "text": {"type": "string"},
            "title": {"type": "string"},
            "lease_id": {"type": "string"},
            "fence": {"type": "integer", "minimum": 0}
        },
        "required": ["name", "base_version"],
        "anyOf": [{"required": ["body"]}, {"required": ["text"]}],
        "additionalProperties": false,
        "description": "lease_id and fence must be supplied together when editing under a claim."
    })
}

fn schema_claim_doc() -> Value {
    json!({
        "type": "object",
        "properties": {
            "name": {"type": "string", "minLength": 1},
            "lease_ttl_s": {
                "type": "integer",
                "minimum": 1,
                "maximum": crate::queues::MAX_LEASE_TTL_S
            }
        },
        "required": ["name"],
        "additionalProperties": false
    })
}

fn schema_release_doc() -> Value {
    json!({
        "type": "object",
        "properties": {
            "lease_id": {"type": "string", "minLength": 1},
            "fence": {"type": "integer", "minimum": 0}
        },
        "required": ["lease_id", "fence"],
        "additionalProperties": false
    })
}

fn schema_read_doc() -> Value {
    json!({
        "type": "object",
        "properties": {
            "name": {"type": "string", "minLength": 1},
            "version": {"type": "integer", "minimum": 1}
        },
        "required": ["name"],
        "additionalProperties": false
    })
}

fn schema_list_docs() -> Value {
    json!({
        "type": "object",
        "properties": {"prefix": {"type": "string"}},
        "additionalProperties": false
    })
}

fn schema_get_status() -> Value {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
}

// Cache the catalog used by `get_tool` so repeated HTTP requests do not rebuild
// every schema. `tool_catalog` itself stays allocation-per-call for tests.
static TOOL_INDEX: OnceLock<Vec<Tool>> = OnceLock::new();

/// Cached tool definitions for repeated lookups.
pub fn cached_tool_catalog() -> &'static [Tool] {
    TOOL_INDEX.get_or_init(tool_catalog)
}
