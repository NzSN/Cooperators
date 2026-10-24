//! M3 conformance: golden transcripts over the UDS dispatch layer and the MCP
//! tool layer, in-process streamable HTTP parity, and a real `coop mcp-stdio`
//! subprocess roundtrip over pipes.
//!
//! The sandbox denies sockets, so nothing here binds. The UDS layer is driven
//! through `server::dispatch` (the exact function the socket loop calls), and
//! the HTTP layer is driven through `mcp::handle_http` with an in-memory body.

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cooperators::mcp::{self, CoopMcpHandler, TOOL_SPECS};
use cooperators::rpc::RpcRequest;
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use http_body_util::BodyExt;
use rmcp::model::CallToolResponse;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

// ---------------------------------------------------------------------------
// Transcript data model
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Transcript {
    name: String,
    steps: Vec<Step>,
}

#[derive(Debug, Deserialize)]
struct Step {
    label: String,
    method: String,
    tool: String,
    args: Value,
    #[serde(default)]
    capture: BTreeMap<String, String>,
    expect: Value,
}

const TRANSCRIPTS: &[(&str, &str)] = &[
    (
        "m1_messaging.json",
        include_str!("conformance/m1_messaging.json"),
    ),
    (
        "m2_channels.json",
        include_str!("conformance/m2_channels.json"),
    ),
    ("m2_queues.json", include_str!("conformance/m2_queues.json")),
    ("m2_docs.json", include_str!("conformance/m2_docs.json")),
    ("errors.json", include_str!("conformance/errors.json")),
];

fn load_transcript(file: &str, text: &str) -> Transcript {
    let transcript: Transcript =
        serde_json::from_str(text).unwrap_or_else(|error| panic!("parse {file}: {error}"));
    assert!(!transcript.steps.is_empty(), "{file} has no steps");
    transcript
}

// ---------------------------------------------------------------------------
// Golden matching, variables, and cross-mode comparison
// ---------------------------------------------------------------------------

fn is_token(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_timestamp(value: &str) -> bool {
    value.len() >= 20 && value.contains('T') && value.ends_with('Z')
}

fn assert_match(expected: &Value, actual: &Value, context: &str) {
    if let Value::Object(object) = expected {
        if object.len() == 1 {
            if let Some(kind) = object.get("$any").and_then(Value::as_str) {
                let ok = match kind {
                    "any" => true,
                    "ulid" => actual.as_str().is_some_and(cooperators::id::is_ulid),
                    "token" => actual.as_str().is_some_and(is_token),
                    "ts" => actual.as_str().is_some_and(is_timestamp),
                    "hash" => actual
                        .as_str()
                        .is_some_and(cooperators::storage::is_chain_hash),
                    "number" => actual.is_number(),
                    "string" => actual.is_string(),
                    "object" => actual.is_object(),
                    "array" => actual.is_array(),
                    other => panic!("{context}: unknown $any kind {other:?}"),
                };
                assert!(
                    ok,
                    "{context}: expected $any:{kind}, got {actual}\nexpected: {expected}"
                );
                return;
            }
            if let Some(prefix) = object.get("$prefix").and_then(Value::as_str) {
                let ok = actual.as_str().is_some_and(|text| text.starts_with(prefix));
                assert!(ok, "{context}: expected prefix {prefix:?}, got {actual}");
                return;
            }
        }
    }

    match expected {
        Value::Object(expected_object) => {
            let actual_object = actual
                .as_object()
                .unwrap_or_else(|| panic!("{context}: expected object, got {actual}"));
            let mut expected_keys: Vec<&String> = expected_object.keys().collect();
            let mut actual_keys: Vec<&String> = actual_object.keys().collect();
            expected_keys.sort();
            actual_keys.sort();
            assert_eq!(
                expected_keys, actual_keys,
                "{context}: key set differs\nexpected: {expected}\nactual: {actual}"
            );
            for (key, expected_value) in expected_object {
                assert_match(
                    expected_value,
                    &actual_object[key],
                    &format!("{context}.{key}"),
                );
            }
        }
        Value::Array(expected_items) => {
            let actual_items = actual
                .as_array()
                .unwrap_or_else(|| panic!("{context}: expected array, got {actual}"));
            assert_eq!(
                expected_items.len(),
                actual_items.len(),
                "{context}: array length differs\nexpected: {expected}\nactual: {actual}"
            );
            for (index, (expected_item, actual_item)) in
                expected_items.iter().zip(actual_items).enumerate()
            {
                assert_match(expected_item, actual_item, &format!("{context}[{index}]"));
            }
        }
        _ => assert_eq!(
            expected, actual,
            "{context}: value differs\nexpected: {expected}\nactual: {actual}"
        ),
    }
}

fn substitute(value: &Value, vars: &BTreeMap<String, Value>) -> Value {
    match value {
        Value::String(text) => {
            if let Some(name) = text
                .strip_prefix("${")
                .and_then(|rest| rest.strip_suffix('}'))
            {
                if let Some(replacement) = vars.get(name) {
                    return replacement.clone();
                }
            }
            let mut rendered = String::new();
            let mut rest = text.as_str();
            while let Some(start) = rest.find("${") {
                rendered.push_str(&rest[..start]);
                let after = &rest[start + 2..];
                if let Some(end) = after.find('}') {
                    let name = &after[..end];
                    if let Some(replacement) = vars.get(name) {
                        match replacement {
                            Value::String(text) => rendered.push_str(text),
                            other => rendered.push_str(&other.to_string()),
                        }
                        rest = &after[end + 1..];
                        continue;
                    }
                }
                rendered.push_str("${");
                rest = after;
            }
            rendered.push_str(rest);
            Value::String(rendered)
        }
        Value::Array(items) => {
            Value::Array(items.iter().map(|item| substitute(item, vars)).collect())
        }
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), substitute(value, vars)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn capture(result: &Value, path: &str) -> Value {
    let mut current = result;
    for segment in path.split('.') {
        current = current.get(segment).unwrap_or_else(|| {
            panic!("capture path {path:?} missing segment {segment:?} in {result}")
        });
    }
    current.clone()
}

fn normalize(value: &Value, root: &str) -> Value {
    match value {
        Value::String(text) => {
            let normalized = if text == root {
                "$root".to_string()
            } else if cooperators::id::is_ulid(text) {
                "$ulid".to_string()
            } else if is_token(text) {
                "$token".to_string()
            } else if is_timestamp(text) {
                "$ts".to_string()
            } else if cooperators::storage::is_chain_hash(text) {
                "$hash".to_string()
            } else {
                text.clone()
            };
            Value::String(normalized)
        }
        Value::Array(items) => {
            Value::Array(items.iter().map(|item| normalize(item, root)).collect())
        }
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), normalize(value, root)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Compares every non-wildcard leaf across the two modes. Wildcards document
/// values that are expected to differ between independent runs (ids,
/// timestamps, tokens).
fn assert_same(
    expected: &Value,
    left: &Value,
    right: &Value,
    left_root: &str,
    right_root: &str,
    context: &str,
) {
    if expected
        .as_object()
        .is_some_and(|object| object.contains_key("$any") || object.contains_key("$prefix"))
    {
        return;
    }
    match expected {
        Value::Object(expected_object) => {
            for (key, expected_value) in expected_object {
                assert_same(
                    expected_value,
                    &left[key],
                    &right[key],
                    left_root,
                    right_root,
                    &format!("{context}.{key}"),
                );
            }
        }
        Value::Array(expected_items) => {
            for (index, expected_item) in expected_items.iter().enumerate() {
                assert_same(
                    expected_item,
                    &left[index],
                    &right[index],
                    left_root,
                    right_root,
                    &format!("{context}[{index}]"),
                );
            }
        }
        _ => assert_eq!(
            normalize(left, left_root),
            normalize(right, right_root),
            "{context}: UDS and MCP results are not semantically identical"
        ),
    }
}

#[derive(Debug)]
struct Observation {
    label: String,
    expect: Value,
    actual: Value,
}

// ---------------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Uds,
    Mcp,
    Http,
}

enum Driver {
    Uds {
        state: Arc<DaemonState>,
        session: ConnectionSession,
    },
    Mcp {
        handler: CoopMcpHandler,
    },
    Http {
        client: HttpDriver,
    },
}

impl Driver {
    async fn call(&mut self, method: &str, tool: &str, args: Value) -> Value {
        match self {
            Self::Uds { state, session } => {
                let request = RpcRequest {
                    id: Some(json!("conformance")),
                    method: method.to_string(),
                    params: args,
                    is_notification: false,
                };
                let response = server::dispatch(state, session, request)
                    .await
                    .expect("dispatch returns a response for requests");
                mcp::canonical_response(response)
            }
            Self::Mcp { handler } => {
                let arguments = args
                    .as_object()
                    .unwrap_or_else(|| panic!("tool arguments must be an object: {args}"))
                    .clone();
                let response = handler.dispatch_tool(tool, Some(arguments)).await;
                canonical_mcp_response(response)
            }
            Self::Http { client } => client.call_tool(tool, args).await,
        }
    }
}

fn canonical_mcp_response(response: CallToolResponse) -> Value {
    match response {
        CallToolResponse::Complete(result) => mcp::canonical_tool_result(&result),
        other => panic!("unexpected MCP response variant: {other:?}"),
    }
}

struct HttpDriver {
    service: mcp::CoopHttpService,
    state: Arc<DaemonState>,
    token: String,
    next_id: u64,
    protocol_version: String,
}

impl HttpDriver {
    async fn start(root: &Path) -> Self {
        let state = Arc::new(DaemonState::with_config(DaemonConfig::new(root)).expect("state"));
        let mut session = ConnectionSession::default();
        let registration = server::dispatch(
            &state,
            &mut session,
            RpcRequest {
                id: Some(json!(0)),
                method: "register_agent".to_string(),
                params: json!({"name": "bootstrap", "kind": "bootstrap"}),
                is_notification: false,
            },
        )
        .await
        .expect("bootstrap registration")
        .into_result()
        .expect("bootstrap result");
        let token = registration["token"].as_str().expect("token").to_string();
        Self::connect(state, token).await
    }

    async fn connect(state: Arc<DaemonState>, token: String) -> Self {
        let service = mcp::http_service(Arc::clone(&state));
        let mut driver = Self {
            service,
            state,
            token,
            next_id: 0,
            protocol_version: "2025-03-26".to_string(),
        };

        let initialize = driver
            .post(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "m3-conformance", "version": "0.1.0"}
                }
            }))
            .await;
        assert_eq!(initialize.0, 200, "initialize status");
        let message = initialize
            .1
            .iter()
            .find(|message| message["id"] == json!(1))
            .unwrap_or_else(|| panic!("initialize response missing: {:?}", initialize.1));
        assert_eq!(message["result"]["serverInfo"]["name"], "cooperators");
        assert!(message["result"]["capabilities"]["tools"].is_object());
        assert_eq!(message["result"]["protocolVersion"], "2025-03-26");
        driver.next_id = 1;

        let initialized = driver
            .post(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .await;
        assert!(
            (200..300).contains(&initialized.0),
            "initialized notification status {}",
            initialized.0
        );
        driver
    }

    async fn post(&self, body: Value) -> (u16, Vec<Value>) {
        // Golden local transcripts can explicitly switch identities. HTTP
        // carries that selected identity in the header as well as the body.
        let token = body
            .pointer("/params/arguments/token")
            .and_then(Value::as_str)
            .unwrap_or(&self.token);
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("http://localhost/mcp")
            .header(http::header::HOST, "localhost")
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::ACCEPT, "application/json, text/event-stream")
            .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
            .header("MCP-Protocol-Version", &self.protocol_version)
            .body(http_body_util::Full::new(Bytes::from(body.to_string())))
            .expect("HTTP request");
        let response = mcp::handle_http(&self.service, &self.state, request).await;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("collect HTTP body")
            .to_bytes();
        let text = String::from_utf8_lossy(&bytes);
        if text.trim().is_empty() {
            return (status, Vec::new());
        }
        if content_type.contains("application/json") {
            let value: Value = serde_json::from_str(&text)
                .unwrap_or_else(|error| panic!("JSON body: {error}: {text}"));
            return (status, vec![value]);
        }
        // SSE framing: one JSON value per `data:` line.
        let messages = text
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .unwrap_or_else(|error| panic!("SSE data: {error}: {line}"))
            })
            .collect();
        (status, messages)
    }

    async fn tools_list(&mut self) -> Vec<Value> {
        self.next_id += 1;
        let id = self.next_id;
        let (status, messages) = self
            .post(json!({"jsonrpc": "2.0", "id": id, "method": "tools/list"}))
            .await;
        assert_eq!(status, 200);
        let message = messages
            .iter()
            .find(|message| message["id"] == json!(id))
            .unwrap_or_else(|| panic!("tools/list response missing: {messages:?}"));
        assert!(
            message.get("error").is_none(),
            "tools/list error: {message}"
        );
        message["result"]["tools"]
            .as_array()
            .expect("tools array")
            .clone()
    }

    async fn call_tool(&mut self, tool: &str, arguments: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let (status, messages) = self
            .post(json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": {"name": tool, "arguments": arguments}
            }))
            .await;
        assert_eq!(status, 200, "tools/call status for {tool}");
        let message = messages
            .iter()
            .find(|message| message["id"] == json!(id))
            .unwrap_or_else(|| panic!("tools/call response missing for {tool}: {messages:?}"));
        assert!(
            message.get("error").is_none(),
            "tools/call protocol error for {tool}: {message}"
        );
        let result = &message["result"];
        let mut structured = result
            .get("structuredContent")
            .cloned()
            .unwrap_or(Value::Null);
        let is_error = result
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        // The HTTP transport authenticates the handshake with a bootstrap
        // agent that has no UDS/MCP-stdio counterpart. Hide that test-only
        // principal from list_agents so the transcript comparison stays exact.
        if tool == "coop_list_agents" && !is_error {
            if let Some(agents) = structured.get_mut("agents").and_then(Value::as_array_mut) {
                agents.retain(|agent| agent["name"] != json!("bootstrap"));
            }
        }
        if tool == "coop_register_agent" && !is_error {
            if let Some(token) = structured.get("token").and_then(Value::as_str) {
                self.token = token.to_string();
            }
        }
        if is_error {
            json!({"ok": false, "error": structured})
        } else {
            json!({"ok": true, "result": structured})
        }
    }
}

async fn run_transcript(mode: Mode, transcript: &Transcript, root: &Path) -> Vec<Observation> {
    let state = Arc::new(
        DaemonState::with_config(DaemonConfig::new(root)).expect("initialize daemon state"),
    );
    let mut driver = match mode {
        Mode::Uds => Driver::Uds {
            state,
            session: ConnectionSession::default(),
        },
        Mode::Mcp => Driver::Mcp {
            handler: CoopMcpHandler::stdio(mcp::Backend::embedded(state), None),
        },
        Mode::Http => Driver::Http {
            client: HttpDriver::start(root).await,
        },
    };
    let mut vars: BTreeMap<String, Value> = BTreeMap::new();
    vars.insert("root".to_string(), json!(root.display().to_string()));
    vars.insert("version".to_string(), json!(env!("CARGO_PKG_VERSION")));
    let mut observations = Vec::new();

    for step in &transcript.steps {
        assert_eq!(
            mcp::spec_for_method(&step.method).map(|spec| spec.tool),
            Some(step.tool.as_str()),
            "{} [{}]: method/tool mapping",
            transcript.name,
            step.label
        );
        let args = substitute(&step.args, &vars);
        let expected = substitute(&step.expect, &vars);
        let actual = driver.call(&step.method, &step.tool, args).await;
        let context = format!("{} [{}] ({mode:?})", transcript.name, step.label);
        assert_match(&expected, &actual, &context);
        if actual["ok"] == Value::Bool(true) {
            for (name, path) in &step.capture {
                vars.insert(name.clone(), capture(&actual["result"], path));
            }
        }
        observations.push(Observation {
            label: step.label.clone(),
            expect: expected,
            actual,
        });
    }
    observations
}

// ---------------------------------------------------------------------------
// Conformance suite
// ---------------------------------------------------------------------------

#[tokio::test]
async fn golden_transcripts_match_uds_and_mcp_tool_layer() {
    for (file, text) in TRANSCRIPTS {
        let transcript = load_transcript(file, text);
        let temp = common::TempDir::new("m3-conformance");
        let uds_root = temp.path().join("uds");
        let mcp_root = temp.path().join("mcp");

        let uds = run_transcript(Mode::Uds, &transcript, &uds_root).await;
        let mcp_observations = run_transcript(Mode::Mcp, &transcript, &mcp_root).await;
        assert_eq!(uds.len(), mcp_observations.len());

        for (uds_step, mcp_step) in uds.iter().zip(&mcp_observations) {
            assert_eq!(uds_step.label, mcp_step.label);
            assert_same(
                &uds_step.expect,
                &uds_step.actual,
                &mcp_step.actual,
                &uds_root.display().to_string(),
                &mcp_root.display().to_string(),
                &format!("{file} [{}]", uds_step.label),
            );
        }
    }
}

#[tokio::test]
async fn http_endpoint_parity_in_process() {
    for (file, text) in TRANSCRIPTS {
        // The errors transcript starts from an unauthenticated connection,
        // which cannot exist behind the HTTP transport's mandatory bearer
        // check. Its UDS/MCP error paths are covered by the conformance test;
        // the HTTP auth failures have dedicated tests below.
        if *file == "errors.json" {
            continue;
        }
        let transcript = load_transcript(file, text);
        let temp = common::TempDir::new("m3-http-parity");
        let uds_root = temp.path().join("uds");
        let http_root = temp.path().join("http");

        let uds = run_transcript(Mode::Uds, &transcript, &uds_root).await;
        let http = run_transcript(Mode::Http, &transcript, &http_root).await;
        for (uds_step, http_step) in uds.iter().zip(&http) {
            assert_same(
                &uds_step.expect,
                &uds_step.actual,
                &http_step.actual,
                &uds_display(&uds_root),
                &uds_display(&http_root),
                &format!("{file} [{}] (HTTP)", uds_step.label),
            );
        }
    }
}

fn uds_display(path: &Path) -> String {
    path.display().to_string()
}

#[tokio::test]
async fn http_messaging_presence_and_status_roundtrip() {
    let temp = common::TempDir::new("m3-http-messaging");
    let root = temp.path().join("root");
    let state = Arc::new(DaemonState::with_config(DaemonConfig::new(root)).expect("state"));
    let mut session = ConnectionSession::default();
    let registration = server::dispatch(
        &state,
        &mut session,
        RpcRequest {
            id: Some(json!(0)),
            method: "register_agent".to_string(),
            params: json!({"name": "peer", "kind": "codex"}),
            is_notification: false,
        },
    )
    .await
    .expect("registration")
    .into_result()
    .expect("registration result");
    let token = registration["token"].as_str().expect("token").to_string();
    let agent_id = registration["agent_id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let mut client = HttpDriver::connect(state, token).await;
    let sent = client
        .call_tool(
            "coop_send_message",
            json!({"to": "peer", "kind": "request", "text": "loop"}),
        )
        .await;
    assert_eq!(sent["ok"], true, "send: {sent}");
    assert_eq!(sent["result"]["to"], "peer");

    let waited = client
        .call_tool("coop_wait_message", json!({"timeout_s": 0}))
        .await;
    assert_eq!(waited["ok"], true, "wait: {waited}");
    assert_eq!(waited["result"]["messages"][0]["body"]["text"], "loop");
    let cursor = waited["result"]["next_cursor"]
        .as_str()
        .expect("cursor")
        .to_string();

    let acked = client
        .call_tool("coop_ack", json!({"cursor": cursor}))
        .await;
    assert_eq!(acked["result"]["acked_cursor"], "1");

    let heartbeat = client.call_tool("coop_heartbeat", json!({})).await;
    assert_eq!(heartbeat["result"]["agent_id"], agent_id);

    let status = client.call_tool("coop_get_status", json!({})).await;
    assert_eq!(status["result"]["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn http_tools_list_matches_the_stdio_catalog() {
    let temp = common::TempDir::new("m3-http-tools");
    let root = temp.path().join("root");
    let mut client = HttpDriver::start(&root).await;
    let wire = client.tools_list().await;
    assert_eq!(wire.len(), 29, "HTTP tools/list exposes the full surface");
    let catalog = mcp::tool_catalog();
    for expected in &catalog {
        let wire_tool = wire
            .iter()
            .find(|tool| tool["name"] == json!(expected.name.as_ref()))
            .unwrap_or_else(|| panic!("HTTP tools/list missing {}", expected.name));
        assert_eq!(
            wire_tool["description"],
            json!(expected.description.as_deref()),
            "description for {}",
            expected.name
        );
        assert_eq!(
            wire_tool["inputSchema"],
            serde_json::to_value(&*expected.input_schema).expect("schema"),
            "schema for {}",
            expected.name
        );
    }
}

#[tokio::test]
async fn http_requires_bearer_and_rejects_bad_tokens() {
    let temp = common::TempDir::new("m3-http-auth");
    let root = temp.path().join("root");
    let state = Arc::new(DaemonState::with_config(DaemonConfig::new(root)).expect("state"));
    let service = mcp::http_service(Arc::clone(&state));

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "auth-test", "version": "0.1.0"}
        }
    })
    .to_string();

    for authorization in [None, Some("Bearer 00000000000000000000000000000000")] {
        let mut builder = http::Request::builder()
            .method(http::Method::POST)
            .uri("http://localhost/mcp")
            .header(http::header::HOST, "localhost")
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::ACCEPT, "application/json, text/event-stream");
        if let Some(authorization) = authorization {
            builder = builder.header(http::header::AUTHORIZATION, authorization);
        }
        let request = builder
            .body(http_body_util::Full::new(Bytes::from(body.clone())))
            .expect("request");
        let response = mcp::handle_http(&service, &state, request).await;
        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        assert!(response
            .headers()
            .get(http::header::WWW_AUTHENTICATE)
            .is_some());
        let payload = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&payload).expect("json error body");
        assert_eq!(value["error"]["code"], -32001);
        assert_eq!(value["error"]["data"]["code"], "UNAUTHENTICATED");
    }
}

// ---------------------------------------------------------------------------
// Tool surface
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tools_list_exposes_every_method_with_matching_schemas() {
    let catalog = mcp::tool_catalog();
    assert_eq!(
        catalog.len(),
        29,
        "broker, instance, arrival and operation tools are present"
    );
    let names: Vec<&str> = catalog.iter().map(|tool| tool.name.as_ref()).collect();
    for spec in TOOL_SPECS {
        assert!(
            names.contains(&spec.tool),
            "missing tool {} in tools/list",
            spec.tool
        );
        assert!(spec.tool.starts_with("coop_"));
    }
    assert_eq!(names.len(), TOOL_SPECS.len(), "no duplicate tools");

    // Independent contract for the daemon param shapes in src/server.rs.
    let contract: &[(&str, &[&str], &[&str])] = &[
        ("coop_instance_metadata", &[], &[]),
        ("coop_retire_agent", &[], &[]),
        (
            "coop_register_agent",
            &["name"],
            &["name", "kind", "capabilities", "token"],
        ),
        (
            "coop_send_message",
            &["to", "kind"],
            &[
                "to",
                "kind",
                "body",
                "text",
                "reply_to",
                "idempotency_key",
                "ttl_s",
            ],
        ),
        ("coop_wait_message", &[], &["cursor", "timeout_s"]),
        ("coop_ack", &["cursor"], &["cursor"]),
        ("coop_heartbeat", &[], &[]),
        (
            "coop_list_agents",
            &[],
            &["include_stale", "limit", "cursor"],
        ),
        (
            "coop_publish",
            &["channel"],
            &["channel", "body", "text", "idempotency_key"],
        ),
        (
            "coop_read_channel",
            &["channel"],
            &["channel", "cursor", "limit", "wait_s"],
        ),
        ("coop_create_queue", &[], &["queue", "name"]),
        (
            "coop_enqueue",
            &["queue", "payload"],
            &["queue", "payload", "max_attempts"],
        ),
        ("coop_claim_task", &["queue"], &["queue", "lease_ttl_s"]),
        ("coop_renew_lease", &["lease_id"], &["lease_id"]),
        (
            "coop_complete_task",
            &["lease_id", "fence", "outcome"],
            &["lease_id", "fence", "outcome", "receipt"],
        ),
        (
            "coop_fail_task",
            &["lease_id", "fence", "reason"],
            &["lease_id", "fence", "reason"],
        ),
        (
            "coop_publish_doc",
            &["name"],
            &["name", "mime", "body", "text", "title"],
        ),
        (
            "coop_update_doc",
            &["name", "base_version"],
            &[
                "name",
                "base_version",
                "body",
                "text",
                "title",
                "lease_id",
                "fence",
            ],
        ),
        ("coop_claim_doc", &["name"], &["name", "lease_ttl_s"]),
        (
            "coop_release_doc",
            &["lease_id", "fence"],
            &["lease_id", "fence"],
        ),
        ("coop_read_doc", &["name"], &["name", "version"]),
        ("coop_list_docs", &[], &["prefix", "limit", "cursor"]),
        ("coop_operation_status", &["operation"], &["operation"]),
        ("coop_get_status", &[], &[]),
        (
            "coop_subscribe_arrivals",
            &["client_key", "resource", "event_kinds"],
            &["client_key", "resource", "event_kinds", "catch_up"],
        ),
        (
            "coop_unsubscribe_arrivals",
            &["subscription_id"],
            &["subscription_id"],
        ),
        ("coop_list_arrival_subscriptions", &[], &["limit", "cursor"]),
        ("coop_wait_arrivals", &[], &["cursor", "timeout_s", "limit"]),
        ("coop_ack_arrivals", &["cursor"], &["cursor"]),
    ];
    assert_eq!(contract.len(), catalog.len());

    for (tool, required, properties) in contract {
        let entry = catalog
            .iter()
            .find(|entry| entry.name.as_ref() == *tool)
            .unwrap_or_else(|| panic!("catalog missing {tool}"));
        let schema = &entry.input_schema;
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("object"));
        let mut actual_required: Vec<&str> = schema
            .get("required")
            .and_then(Value::as_array)
            .map(|items| items.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        actual_required.sort_unstable();
        let mut expected_required: Vec<&str> = required.to_vec();
        expected_required.sort_unstable();
        assert_eq!(actual_required, expected_required, "{tool} required fields");
        let mut actual_properties: Vec<&str> = schema
            .get("properties")
            .and_then(Value::as_object)
            .map(|object| object.keys().map(String::as_str).collect())
            .unwrap_or_default();
        actual_properties.sort_unstable();
        let mut expected_properties: Vec<&str> = properties.to_vec();
        expected_properties.sort_unstable();
        assert_eq!(actual_properties, expected_properties, "{tool} properties");
    }
}

#[tokio::test]
async fn daemon_status_is_namespaced_as_coop_get_status() {
    let spec = mcp::spec_for_method("daemon.status").expect("status mapping");
    assert_eq!(spec.tool, "coop_get_status");
    assert!(mcp::spec_for_tool("coop_get_status").is_some());
}

#[tokio::test]
async fn unreachable_daemon_is_a_tool_error_not_a_protocol_error() {
    let temp = common::TempDir::new("m3-unreachable");
    let socket = temp.path().join("no-daemon.sock");
    let handler = CoopMcpHandler::stdio(mcp::Backend::uds(socket), Some("token".into()));
    let response = handler.dispatch_tool("coop_heartbeat", None).await;
    let canonical = canonical_mcp_response(response);
    assert_eq!(canonical["ok"], false);
    assert_eq!(canonical["error"]["code"], -32009);
    assert_eq!(canonical["error"]["data"]["code"], "DAEMON_UNREACHABLE");
}

#[tokio::test]
async fn unknown_tool_is_a_tool_error_with_method_not_found() {
    let temp = common::TempDir::new("m3-unknown");
    let root = temp.path().join("root");
    let state = Arc::new(DaemonState::with_config(DaemonConfig::new(root)).expect("state"));
    let handler = CoopMcpHandler::stdio(mcp::Backend::embedded(state), None);
    let canonical = canonical_mcp_response(handler.dispatch_tool("coop_nope", None).await);
    assert_eq!(canonical["ok"], false);
    assert_eq!(canonical["error"]["code"], -32601);
    assert_eq!(canonical["error"]["data"]["code"], "METHOD_NOT_FOUND");
}

// ---------------------------------------------------------------------------
// Real subprocess roundtrip over stdio pipes
// ---------------------------------------------------------------------------

struct StdioProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl StdioProcess {
    async fn spawn(root: &Path, token: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_coop"))
            .args(["mcp-stdio", "--embedded", "--resume"])
            .env("COOPERATORS_HOME", root)
            .env("COOP_TOKEN", token)
            .env_remove("COOP_SOCKET")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn coop mcp-stdio");
        let stdin = child.stdin.take().expect("child stdin");
        let stdout = BufReader::new(child.stdout.take().expect("child stdout"));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    async fn send(&mut self, message: Value) {
        let mut line = serde_json::to_string(&message).expect("encode MCP request");
        line.push('\n');
        self.stdin
            .write_all(line.as_bytes())
            .await
            .expect("write MCP request");
        self.stdin.flush().await.expect("flush MCP request");
    }

    async fn read_message(&mut self) -> Value {
        let mut line = String::new();
        let read = tokio::time::timeout(Duration::from_secs(15), self.stdout.read_line(&mut line))
            .await
            .expect("timed out waiting for MCP response")
            .expect("read MCP response");
        assert!(read > 0, "coop mcp-stdio closed stdout");
        serde_json::from_str(line.trim_end()).expect("MCP response is JSON")
    }

    async fn read_response(&mut self, id: u64) -> Value {
        loop {
            let message = self.read_message().await;
            if message.get("id") == Some(&json!(id)) {
                return message;
            }
        }
    }

    async fn shutdown(mut self) -> std::process::ExitStatus {
        drop(self.stdin);
        tokio::time::timeout(Duration::from_secs(15), self.child.wait())
            .await
            .expect("timed out waiting for coop mcp-stdio to exit")
            .expect("wait for coop mcp-stdio")
    }
}

#[tokio::test]
async fn mcp_stdio_subprocess_roundtrip_over_pipes() {
    let temp = common::TempDir::new("m3-stdio");
    let root = temp.path().join("root");

    // Seed daemon state in-process, then hand the root to the subprocess. The
    // child replays the same durable logs in --embedded mode, so the roundtrip
    // needs no socket.
    let state = Arc::new(DaemonState::with_config(DaemonConfig::new(root.clone())).expect("state"));
    let mut session = ConnectionSession::default();
    let alice = server::dispatch(
        &state,
        &mut session,
        RpcRequest {
            id: Some(json!(1)),
            method: "register_agent".to_string(),
            params: json!({"name": "alice", "kind": "codex"}),
            is_notification: false,
        },
    )
    .await
    .expect("alice response")
    .into_result()
    .expect("alice registration");
    let alice_token = alice["token"].as_str().expect("alice token").to_string();
    let bob = server::dispatch(
        &state,
        &mut session,
        RpcRequest {
            id: Some(json!(2)),
            method: "register_agent".to_string(),
            params: json!({"name": "bob", "kind": "claude"}),
            is_notification: false,
        },
    )
    .await
    .expect("bob response")
    .into_result()
    .expect("bob registration");
    let bob_token = bob["token"].as_str().expect("bob token").to_string();
    server::dispatch(
        &state,
        &mut session,
        RpcRequest {
            id: Some(json!(3)),
            method: "send_message".to_string(),
            params: json!({"to": "alice", "kind": "request", "text": "ping from bob"}),
            is_notification: false,
        },
    )
    .await
    .expect("seed send response")
    .into_result()
    .expect("seed send");
    drop(state);

    let mut process = StdioProcess::spawn(&root, &alice_token).await;
    process
        .send(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "stdio-roundtrip", "version": "0.1.0"}
            }
        }))
        .await;
    let initialize = process.read_response(1).await;
    assert_eq!(initialize["result"]["serverInfo"]["name"], "cooperators");

    process
        .send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        .await;

    process
        .send(json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
        .await;
    let tools = process.read_response(2).await;
    let wire_tools = tools["result"]["tools"].as_array().expect("tools array");
    assert_eq!(wire_tools.len(), 29);
    let expected = mcp::tool_catalog();
    for expected_tool in &expected {
        let wire = wire_tools
            .iter()
            .find(|tool| tool["name"] == json!(expected_tool.name.as_ref()))
            .unwrap_or_else(|| panic!("wire tools/list missing {}", expected_tool.name));
        assert_eq!(
            wire["inputSchema"],
            serde_json::to_value(&*expected_tool.input_schema).expect("schema"),
            "schema for {}",
            expected_tool.name
        );
    }

    process
        .send(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "coop_wait_message", "arguments": {"timeout_s": 0}}
        }))
        .await;
    let waited = process.read_response(3).await;
    assert_eq!(waited["result"]["isError"], false);
    let structured = &waited["result"]["structuredContent"];
    assert_eq!(structured["messages"][0]["body"]["text"], "ping from bob");
    let next_cursor = structured["next_cursor"].as_str().expect("cursor");
    let message_id = structured["messages"][0]["id"]
        .as_str()
        .expect("message id")
        .to_string();

    process
        .send(json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {"name": "coop_ack", "arguments": {"cursor": next_cursor}}
        }))
        .await;
    let acked = process.read_response(4).await;
    assert_eq!(acked["result"]["structuredContent"]["acked_cursor"], "1");

    process
        .send(json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "coop_send_message",
                "arguments": {
                    "to": "bob",
                    "kind": "response",
                    "text": "pong from alice",
                    "reply_to": message_id
                }
            }
        }))
        .await;
    let sent = process.read_response(5).await;
    assert_eq!(sent["result"]["isError"], false);
    assert_eq!(sent["result"]["structuredContent"]["to"], "bob");

    process
        .send(json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {"name": "coop_get_status", "arguments": {}}
        }))
        .await;
    let status = process.read_response(6).await;
    assert_eq!(
        status["result"]["structuredContent"]["version"],
        env!("CARGO_PKG_VERSION")
    );

    let exit = process.shutdown().await;
    assert!(exit.success(), "coop mcp-stdio exited with {exit}");

    // The child's send is durable: reopen the root and read bob's inbox.
    let state = Arc::new(DaemonState::with_config(DaemonConfig::new(root)).expect("reopen state"));
    let mut session = ConnectionSession::default();
    let pong = server::dispatch(
        &state,
        &mut session,
        RpcRequest {
            id: Some(json!(7)),
            method: "wait_message".to_string(),
            params: json!({"token": bob_token, "timeout_s": 0}),
            is_notification: false,
        },
    )
    .await
    .expect("pong response")
    .into_result()
    .expect("pong wait");
    assert_eq!(pong["messages"][0]["body"]["text"], "pong from alice");
    assert_eq!(pong["messages"][0]["from"], "alice");
}
