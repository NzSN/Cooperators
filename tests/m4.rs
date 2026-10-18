//! M4 hardening: inbox backpressure, health/metrics endpoints, and the stdio
//! message-size cap. Everything here is in-process; no socket is bound.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use cooperators::mcp::{self, Backend, CoopMcpHandler, LineCappedReader};
use cooperators::rpc::{RpcError, RpcRequest};
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use http_body_util::BodyExt;
use rmcp::model::CallToolResponse;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn request(method: &str, params: Value) -> RpcRequest {
    RpcRequest {
        id: Some(json!(1)),
        method: method.to_string(),
        params,
        is_notification: false,
    }
}

async fn call(state: &Arc<DaemonState>, method: &str, params: Value) -> Result<Value, RpcError> {
    server::dispatch(
        state,
        &mut ConnectionSession::default(),
        request(method, params),
    )
    .await
    .expect("request returns a response")
    .into_result()
}

async fn register(state: &Arc<DaemonState>, name: &str) -> String {
    let result = call(
        state,
        "register_agent",
        json!({"name": name, "kind": "agent"}),
    )
    .await
    .expect("register agent");
    result["token"].as_str().expect("token").to_string()
}

fn send_params(token: &str, to: &str, text: &str) -> Value {
    json!({
        "token": token,
        "to": to,
        "kind": "text",
        "body": {"mime": "text/plain", "text": text},
    })
}

fn assert_exhausted(error: &RpcError) {
    assert_eq!(error.code, -32003, "RESOURCE_EXHAUSTED code");
    assert_eq!(
        error.data.as_ref().expect("error data")["code"],
        "RESOURCE_EXHAUSTED",
        "symbolic error code"
    );
}

#[tokio::test]
async fn inbox_message_cap_rejects_senders_until_ack_frees_capacity() {
    let temp = common::TempDir::new("m4-message-cap");
    let mut config = DaemonConfig::new(temp.path().join("root"));
    config.max_inbox_messages = 2;
    config.max_inbox_bytes = u64::MAX;
    let state = Arc::new(DaemonState::with_config(config).expect("state"));

    let sender = register(&state, "sender").await;
    let recipient = register(&state, "recipient").await;

    call(
        &state,
        "send_message",
        send_params(&sender, "recipient", "one"),
    )
    .await
    .expect("first send");
    call(
        &state,
        "send_message",
        send_params(&sender, "recipient", "two"),
    )
    .await
    .expect("second send");

    let error = call(
        &state,
        "send_message",
        send_params(&sender, "recipient", "three"),
    )
    .await
    .expect_err("third send is over the cap");
    assert_exhausted(&error);

    // Wait is a peek: it does not free backlog. Ack does.
    call(&state, "ack", json!({"token": recipient, "cursor": "2"}))
        .await
        .expect("ack through both messages");

    call(
        &state,
        "send_message",
        send_params(&sender, "recipient", "three"),
    )
    .await
    .expect("send succeeds after ack");
}

#[tokio::test]
async fn inbox_byte_cap_rejects_large_messages_without_blocking_small_ones() {
    let temp = common::TempDir::new("m4-byte-cap");
    let mut config = DaemonConfig::new(temp.path().join("root"));
    config.max_inbox_messages = 100;
    config.max_inbox_bytes = 1024;
    let state = Arc::new(DaemonState::with_config(config).expect("state"));

    let sender = register(&state, "sender").await;
    let _recipient = register(&state, "recipient").await;

    let large = "x".repeat(4096);
    let error = call(
        &state,
        "send_message",
        send_params(&sender, "recipient", &large),
    )
    .await
    .expect_err("oversized message is rejected");
    assert_exhausted(&error);

    call(
        &state,
        "send_message",
        send_params(&sender, "recipient", "small"),
    )
    .await
    .expect("small message still fits");
}

#[tokio::test]
async fn dedupe_still_answers_when_the_inbox_is_full() {
    let temp = common::TempDir::new("m4-dedupe-cap");
    let mut config = DaemonConfig::new(temp.path().join("root"));
    config.max_inbox_messages = 1;
    let state = Arc::new(DaemonState::with_config(config).expect("state"));

    let sender = register(&state, "sender").await;
    let _recipient = register(&state, "recipient").await;

    let mut params = send_params(&sender, "recipient", "one");
    params["idempotency_key"] = json!("key-1");
    call(&state, "send_message", params.clone())
        .await
        .expect("first send");

    let duplicate = call(&state, "send_message", params)
        .await
        .expect("duplicate returns the original receipt");
    assert_eq!(duplicate["duplicate"], true);

    let error = call(
        &state,
        "send_message",
        send_params(&sender, "recipient", "two"),
    )
    .await
    .expect_err("new message is rejected");
    assert_exhausted(&error);
}

#[tokio::test]
async fn healthz_is_public_and_metrics_requires_a_bearer_token() {
    let temp = common::TempDir::new("m4-health-metrics");
    let state = Arc::new(
        DaemonState::with_config(DaemonConfig::new(temp.path().join("root"))).expect("state"),
    );
    state.set_http_listener(true);

    let (status, content_type, body) = http_get(&state, "/healthz", None).await;
    assert_eq!(status, 200);
    assert!(content_type.starts_with("application/json"));
    let health: Value = serde_json::from_str(&body).expect("health JSON");
    assert_eq!(health["status"], "ok");
    assert_eq!(health["version"], env!("CARGO_PKG_VERSION"));
    assert!(health["uptime_s"].is_u64());
    assert_eq!(health["http_listener"], true);

    let (status, _, body) = http_get(&state, "/metrics", None).await;
    assert_eq!(status, 401);
    let error: Value = serde_json::from_str(&body).expect("auth error JSON");
    assert_eq!(error["error"]["data"]["code"], "UNAUTHENTICATED");

    let token = register(&state, "operator").await;
    let (status, content_type, body) = http_get(&state, "/metrics", Some(&token)).await;
    assert_eq!(status, 200);
    assert!(content_type.starts_with("text/plain"));
    assert!(body.contains("coop_agents_registered 1"));
    assert!(body.contains("coop_http_listener 1"));
    assert!(body.contains("coop_http_requests_total{path=\"/healthz\",status=\"200\"} 1"));
    assert!(body.contains("coop_http_requests_total{path=\"/metrics\",status=\"401\"} 1"));
}

#[tokio::test]
async fn metrics_report_inbox_backlog_queues_leases_and_message_heads() {
    let temp = common::TempDir::new("m4-metrics-values");
    let state = Arc::new(
        DaemonState::with_config(DaemonConfig::new(temp.path().join("root"))).expect("state"),
    );
    state.set_http_listener(true);

    let sender = register(&state, "sender").await;
    let _recipient = register(&state, "recipient").await;

    call(
        &state,
        "create_queue",
        json!({"token": sender, "queue": "work"}),
    )
    .await
    .expect("create queue");
    call(
        &state,
        "enqueue",
        json!({"token": sender, "queue": "work", "payload": {"n": 1}}),
    )
    .await
    .expect("enqueue");
    call(
        &state,
        "claim_task",
        json!({"token": sender, "queue": "work", "lease_ttl_s": 60}),
    )
    .await
    .expect("claim");

    call(
        &state,
        "send_message",
        send_params(&sender, "recipient", "hello"),
    )
    .await
    .expect("send");
    call(
        &state,
        "publish",
        json!({
            "token": sender,
            "channel": "ops",
            "body": {"mime": "text/plain", "text": "published"},
        }),
    )
    .await
    .expect("publish");

    let token = sender.clone();
    let (status, _, body) = http_get(&state, "/metrics", Some(&token)).await;
    assert_eq!(status, 200, "metrics body: {body}");
    assert!(body.contains("coop_agents_registered 2"), "{body}");
    assert!(body.contains("coop_agents{state=\"active\"} 2"), "{body}");
    assert!(
        body.contains("coop_queue_pending{queue=\"work\"} 0"),
        "{body}"
    );
    assert!(
        body.contains("coop_queue_leased{queue=\"work\"} 1"),
        "{body}"
    );
    assert!(body.contains("coop_active_leases 1"), "{body}");
    assert!(
        body.contains("coop_inbox_pending_messages{agent=\"recipient\"} 1"),
        "{body}"
    );
    assert!(
        body.contains("coop_inbox_pending_bytes{agent=\"recipient\"}"),
        "{body}"
    );
    assert!(
        body.contains("coop_messages_appended_total{log=\"agents/"),
        "{body}"
    );
    assert!(
        body.contains("coop_messages_appended_total{log=\"channels/ops.jsonl\"} 1"),
        "{body}"
    );
    // The recipient token was never used for a request after registration,
    // so presence remains active for both agents in this short test.
    assert!(body.contains("coop_agents{state=\"stale\"} 0"), "{body}");
}

#[tokio::test]
async fn stdio_message_cap_returns_resource_exhausted() {
    let temp = common::TempDir::new("m4-stdio-cap");
    let state = Arc::new(
        DaemonState::with_config(DaemonConfig::new(temp.path().join("root"))).expect("state"),
    );
    let sender = register(&state, "sender").await;
    let _recipient = register(&state, "recipient").await;

    let handler =
        CoopMcpHandler::stdio_with_limit(Backend::embedded(Arc::clone(&state)), Some(sender), 128);
    let arguments = json!({
        "to": "recipient",
        "kind": "text",
        "text": "x".repeat(1024),
    })
    .as_object()
    .cloned();
    match handler.dispatch_tool("coop_send_message", arguments).await {
        CallToolResponse::Complete(result) => {
            assert!(result.is_error.unwrap_or(false), "{result:?}");
            let structured = result.structured_content.expect("structured error");
            assert_eq!(structured["code"], -32003);
            assert_eq!(structured["data"]["code"], "RESOURCE_EXHAUSTED");
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

#[tokio::test]
async fn line_capped_reader_rejects_a_runaway_line() {
    let (mut writer, reader) = tokio::io::duplex(64);
    writer.write_all(b"123456789").await.expect("write");
    let mut reader = LineCappedReader::new(reader, 8);
    let mut buffer = [0u8; 32];
    let error = reader.read(&mut buffer).await.expect_err("cap exceeded");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn line_capped_reader_accepts_lines_at_the_limit() {
    let (mut writer, reader) = tokio::io::duplex(64);
    writer.write_all(b"12345678\nnext\n").await.expect("write");
    drop(writer);
    let mut reader = LineCappedReader::new(reader, 8);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.expect("read");
    assert_eq!(buffer, b"12345678\nnext\n");
}

async fn http_get(
    state: &Arc<DaemonState>,
    path: &str,
    token: Option<&str>,
) -> (u16, String, String) {
    let service = mcp::http_service(Arc::clone(state));
    let mut builder = http::Request::builder()
        .method(http::Method::GET)
        .uri(format!("http://localhost{path}"))
        .header(http::header::HOST, "localhost");
    if let Some(token) = token {
        builder = builder.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = builder
        .body(http_body_util::Full::new(Bytes::new()))
        .expect("HTTP request");
    let response = mcp::handle_http(&service, state, request).await;
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
        .expect("collect body")
        .to_bytes();
    (
        status,
        content_type,
        String::from_utf8_lossy(&bytes).to_string(),
    )
}
