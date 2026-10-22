mod common;

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;

use cooperators::envelope::{Body, Envelope};
use cooperators::messaging::SendRequest;
use cooperators::rpc::{RpcError, RpcRequest};
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use cooperators::storage::LogFile;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

async fn call(
    state: &Arc<DaemonState>,
    session: &mut ConnectionSession,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    let request = RpcRequest {
        id: Some(json!(1)),
        method: method.to_string(),
        params: params.clone(),
        is_notification: false,
    };
    server::dispatch(state, session, request)
        .await
        .expect("dispatch returns a response for a request with id")
        .into_result()
        .map_err(|error| {
            let mut safe_params = params.clone();
            if let Some(object) = safe_params.as_object_mut() {
                object.remove("token");
            }
            eprintln!("rpc {method} {safe_params} failed: {error:?}");
            error
        })
}

fn app_code(error: &RpcError) -> &str {
    error
        .data
        .as_ref()
        .and_then(|data| data.get("code"))
        .and_then(Value::as_str)
        .expect("application errors carry error.data.code")
}

async fn register(state: &Arc<DaemonState>, name: &str, kind: &str) -> (String, String) {
    let mut session = ConnectionSession::default();
    let value = call(
        state,
        &mut session,
        "register_agent",
        json!({"name": name, "kind": kind}),
    )
    .await
    .expect("register agent");
    (
        value["agent_id"].as_str().expect("agent_id").to_string(),
        value["token"].as_str().expect("token").to_string(),
    )
}

fn state_with(
    root: &std::path::Path,
    configure: impl FnOnce(&mut DaemonConfig),
) -> Arc<DaemonState> {
    let mut config = DaemonConfig::new(root);
    configure(&mut config);
    Arc::new(DaemonState::with_config(config).expect("daemon state"))
}

fn inbox_line_count(root: &std::path::Path, agent_id: &str) -> usize {
    let path = root.join("agents").join(agent_id).join("inbox.jsonl");
    let bytes = std::fs::read(&path).expect("read inbox");
    bytes.iter().filter(|byte| **byte == b'\n').count()
}

#[tokio::test]
async fn register_auth_roundtrip_and_wrong_token_rejection() {
    let temp = common::TempDir::new("m1-auth");
    let root = temp.path().join("root");
    let state = state_with(&root, |_| {});

    let mut session = ConnectionSession::default();
    let registration = call(
        &state,
        &mut session,
        "register_agent",
        json!({"name": "alice", "kind": "codex", "capabilities": ["review", "build"]}),
    )
    .await
    .expect("register");
    let agent_id = registration["agent_id"].as_str().expect("agent id");
    let token = registration["token"].as_str().expect("token");
    assert!(cooperators::id::is_ulid(agent_id));
    assert_eq!(token.len(), 32, "128-bit token as 32 hex chars");
    assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(registration["rotated"], false);
    assert_eq!(registration["capabilities"], json!(["review", "build"]));

    let registry_path = root.join("registry.json");
    let raw = std::fs::read_to_string(&registry_path).expect("registry contents");
    assert!(
        !raw.contains(token),
        "plaintext token must never be persisted"
    );
    assert!(raw.contains(&cooperators::registry::token_hash(token)));
    let mode = std::fs::metadata(&registry_path)
        .expect("registry metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "registry must be 0600");

    let mut bare = ConnectionSession::default();
    let missing = call(&state, &mut bare, "heartbeat", json!({}))
        .await
        .expect_err("missing token");
    assert_eq!(app_code(&missing), "UNAUTHENTICATED");
    assert_eq!(missing.code, -32001);

    let wrong = call(
        &state,
        &mut bare,
        "heartbeat",
        json!({"token": "00".repeat(16)}),
    )
    .await
    .expect_err("wrong token");
    assert_eq!(app_code(&wrong), "UNAUTHENTICATED");

    let unauthorized_send = call(
        &state,
        &mut bare,
        "send_message",
        json!({
            "token": "00".repeat(16),
            "to": "alice",
            "kind": "event",
            "text": "must not append",
        }),
    )
    .await
    .expect_err("wrong token must not send");
    assert_eq!(app_code(&unauthorized_send), "UNAUTHENTICATED");

    let heartbeat = call(&state, &mut bare, "heartbeat", json!({"token": token}))
        .await
        .expect("valid token");
    assert_eq!(heartbeat["state"], "active");
    assert_eq!(heartbeat["agent_id"], agent_id);

    // Token cached on the connection: an authenticated follow-up may omit it.
    let agents = call(&state, &mut bare, "list_agents", json!({}))
        .await
        .expect("session token reuse");
    assert_eq!(agents["agents"][0]["name"], "alice");

    // Re-registration needs the old token; a fresh connection has none.
    let mut fresh = ConnectionSession::default();
    let conflict = call(
        &state,
        &mut fresh,
        "register_agent",
        json!({"name": "alice", "kind": "codex"}),
    )
    .await
    .expect_err("name already taken");
    assert_eq!(app_code(&conflict), "CONFLICT");

    let rotated = call(
        &state,
        &mut fresh,
        "register_agent",
        json!({"name": "alice", "kind": "codex", "token": token}),
    )
    .await
    .expect("rotate token");
    assert_eq!(rotated["rotated"], true);
    assert_eq!(rotated["agent_id"], agent_id);
    let new_token = rotated["token"].as_str().expect("rotated token");
    assert_ne!(new_token, token);

    let mut old_session = ConnectionSession::default();
    let old_rejected = call(
        &state,
        &mut old_session,
        "heartbeat",
        json!({"token": token}),
    )
    .await
    .expect_err("old token is invalid after rotation");
    assert_eq!(app_code(&old_rejected), "UNAUTHENTICATED");

    let mut new_session = ConnectionSession::default();
    call(
        &state,
        &mut new_session,
        "heartbeat",
        json!({"token": new_token}),
    )
    .await
    .expect("rotated token works");
}

#[tokio::test]
async fn send_wait_ack_ordering_and_peek_semantics() {
    let temp = common::TempDir::new("m1-messaging");
    let root = temp.path().join("root");
    let state = state_with(&root, |_| {});
    let (_alice_id, alice) = register(&state, "alice", "codex").await;
    let (_bob_id, bob) = register(&state, "bob", "claude").await;

    let mut session = ConnectionSession::default();
    let first = call(
        &state,
        &mut session,
        "send_message",
        json!({
            "token": alice,
            "to": "bob",
            "kind": "request",
            "body": {"mime": "text/plain", "text": "one"},
        }),
    )
    .await
    .expect("send one");
    assert_eq!(first["seq"], 1);
    assert_eq!(first["duplicate"], false);

    let second = call(
        &state,
        &mut session,
        "send_message",
        json!({
            "token": alice,
            "to": "bob",
            "kind": "request",
            "body": {"mime": "text/plain", "text": "two"},
            "reply_to": first["message_id"],
        }),
    )
    .await
    .expect("send two");
    assert_eq!(second["seq"], 2);

    let mut bob_session = ConnectionSession::default();
    let batch = call(
        &state,
        &mut bob_session,
        "wait_message",
        json!({"token": bob, "cursor": "0", "timeout_s": 0}),
    )
    .await
    .expect("wait");
    let messages = batch["messages"].as_array().expect("messages array");
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["seq"], 1);
    assert_eq!(messages[1]["seq"], 2);
    assert_eq!(messages[0]["body"]["text"], "one");
    assert_eq!(messages[1]["reply_to"], first["message_id"]);
    assert_eq!(batch["next_cursor"], "2");
    assert_eq!(batch["timed_out"], false);

    // Wait is a peek: the same cursor returns the same batch.
    let again = call(
        &state,
        &mut bob_session,
        "wait_message",
        json!({"token": bob, "cursor": "0", "timeout_s": 0}),
    )
    .await
    .expect("peek again");
    assert_eq!(again["messages"].as_array().expect("messages").len(), 2);
    assert_eq!(again["acked_cursor"], "0");

    let acked = call(
        &state,
        &mut bob_session,
        "ack",
        json!({"token": bob, "cursor": "2"}),
    )
    .await
    .expect("ack");
    assert_eq!(acked["acked_cursor"], "2");

    // With no explicit cursor the wait starts at the acknowledged cursor.
    let empty = call(
        &state,
        &mut bob_session,
        "wait_message",
        json!({"token": bob, "timeout_s": 0}),
    )
    .await
    .expect("wait after ack");
    assert_eq!(empty["messages"], json!([]));
    assert_eq!(empty["acked_cursor"], "2");
    assert_eq!(empty["timed_out"], true);

    // An explicit cursor still addresses earlier messages after ack.
    let only_second = call(
        &state,
        &mut bob_session,
        "wait_message",
        json!({"token": bob, "cursor": "1", "timeout_s": 0}),
    )
    .await
    .expect("explicit cursor");
    assert_eq!(
        only_second["messages"].as_array().expect("messages").len(),
        1
    );
    assert_eq!(only_second["messages"][0]["seq"], 2);

    // Long-poll wakes on a later send instead of sleeping the full timeout.
    let waiter_state = Arc::clone(&state);
    let waiter_token = bob.clone();
    let waiter = tokio::spawn(async move {
        let mut session = ConnectionSession::default();
        call(
            &waiter_state,
            &mut session,
            "wait_message",
            json!({"token": waiter_token, "cursor": "2", "timeout_s": 2}),
        )
        .await
        .expect("long poll")
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    call(
        &state,
        &mut session,
        "send_message",
        json!({
            "token": alice,
            "to": "bob",
            "kind": "event",
            "text": "three",
        }),
    )
    .await
    .expect("send three");
    let delivered = waiter.await.expect("waiter task");
    assert_eq!(delivered["messages"][0]["seq"], 3);
    assert_eq!(delivered["messages"][0]["body"]["text"], "three");

    let unknown = call(
        &state,
        &mut session,
        "send_message",
        json!({"token": alice, "to": "nobody", "kind": "event", "text": "x"}),
    )
    .await
    .expect_err("unknown recipient");
    assert_eq!(app_code(&unknown), "NOT_FOUND");

    let bad_cursor = call(
        &state,
        &mut bob_session,
        "wait_message",
        json!({"token": bob, "cursor": "not-a-cursor", "timeout_s": 0}),
    )
    .await
    .expect_err("invalid cursor");
    assert_eq!(bad_cursor.code, -32602);
}

#[tokio::test]
async fn dedupe_returns_original_seq_without_second_append() {
    let temp = common::TempDir::new("m1-dedupe");
    let root = temp.path().join("root");
    let state = state_with(&root, |_| {});
    let (_alice_id, alice) = register(&state, "alice", "codex").await;
    let (bob_id, _bob) = register(&state, "bob", "claude").await;

    let mut session = ConnectionSession::default();
    let params = json!({
        "token": alice,
        "to": "bob",
        "kind": "request",
        "body": {"mime": "text/plain", "text": "once"},
        "idempotency_key": "send-once",
    });
    let first = call(&state, &mut session, "send_message", params.clone())
        .await
        .expect("first send");
    let repeat = call(&state, &mut session, "send_message", params)
        .await
        .expect("duplicate send");
    assert_eq!(repeat["seq"], first["seq"]);
    assert_eq!(repeat["message_id"], first["message_id"]);
    assert_eq!(repeat["duplicate"], true);
    assert_eq!(inbox_line_count(&root, &bob_id), 1);
}

#[tokio::test]
async fn restart_rebuilds_messages_dedupe_and_auth_without_shutdown_hooks() {
    let temp = common::TempDir::new("m1-restart");
    let root = temp.path().join("root");

    let (alice_token, bob_token, bob_id, first_seq, first_message_id) = {
        let state = state_with(&root, |_| {});
        let (_alice_id, alice) = register(&state, "alice", "codex").await;
        let (bob_id, bob) = register(&state, "bob", "claude").await;
        let mut session = ConnectionSession::default();
        let receipt = call(
            &state,
            &mut session,
            "send_message",
            json!({
                "token": alice,
                "to": "bob",
                "kind": "request",
                "body": {"mime": "text/plain", "text": "durable"},
                "idempotency_key": "restart-key",
            }),
        )
        .await
        .expect("send before restart");
        (
            alice,
            bob,
            bob_id,
            receipt["seq"].clone(),
            receipt["message_id"].clone(),
        )
        // state drops here without any shutdown hook
    };

    let state = state_with(&root, |_| {});
    let mut bob_session = ConnectionSession::default();
    let batch = call(
        &state,
        &mut bob_session,
        "wait_message",
        json!({"token": bob_token, "cursor": "0", "timeout_s": 0}),
    )
    .await
    .expect("wait after restart");
    assert_eq!(batch["messages"].as_array().expect("messages").len(), 1);
    assert_eq!(batch["messages"][0]["body"]["text"], "durable");
    assert_eq!(batch["messages"][0]["seq"], first_seq);

    let mut alice_session = ConnectionSession::default();
    call(
        &state,
        &mut alice_session,
        "heartbeat",
        json!({"token": alice_token}),
    )
    .await
    .expect("registry survived restart");

    let repeat = call(
        &state,
        &mut alice_session,
        "send_message",
        json!({
            "token": alice_token,
            "to": "bob",
            "kind": "request",
            "body": {"mime": "text/plain", "text": "durable"},
            "idempotency_key": "restart-key",
        }),
    )
    .await
    .expect("dedupe after restart");
    assert_eq!(repeat["duplicate"], true);
    assert_eq!(repeat["seq"], first_seq);
    assert_eq!(repeat["message_id"], first_message_id);
    assert_eq!(inbox_line_count(&root, &bob_id), 1);
}

#[tokio::test]
async fn presence_stale_transition_and_heartbeat_revival() {
    let temp = common::TempDir::new("m1-presence-stale");
    let root = temp.path().join("root");
    let state = state_with(&root, |config| {
        config.stale_after = Duration::from_millis(100);
        config.gone_after = Duration::from_secs(30);
        config.presence_sweep_interval = Duration::from_secs(60);
    });
    let (agent_id, token) = register(&state, "alice", "codex").await;
    let (_observer_id, observer) = register(&state, "observer", "agent").await;

    // Listing is itself an authenticated call, so observe through a second
    // agent; otherwise alice's own list would refresh her heartbeat.
    let mut session = ConnectionSession::default();
    let active = call(
        &state,
        &mut session,
        "list_agents",
        json!({"token": observer}),
    )
    .await
    .expect("initial list");
    assert_eq!(active["agents"][0]["state"], "active");

    tokio::time::sleep(Duration::from_millis(250)).await;
    let visible = call(
        &state,
        &mut session,
        "list_agents",
        json!({"token": observer}),
    )
    .await
    .expect("active-only list");
    let names: Vec<&str> = visible["agents"]
        .as_array()
        .expect("agents")
        .iter()
        .map(|agent| agent["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, vec!["observer"]);

    let all = call(
        &state,
        &mut session,
        "list_agents",
        json!({"token": observer, "include_stale": true}),
    )
    .await
    .expect("full list");
    assert_eq!(all["agents"][0]["state"], "stale");

    // Any authenticated call revives the agent and emits an active event.
    let heartbeat = call(&state, &mut session, "heartbeat", json!({"token": token}))
        .await
        .expect("heartbeat");
    assert_eq!(heartbeat["state"], "active");

    let (log, report) =
        LogFile::<Envelope>::open(root.join("channels/#presence.jsonl")).expect("presence log");
    drop(log);
    let kinds: Vec<&str> = report
        .records
        .iter()
        .map(|record| record.kind.as_str())
        .collect();
    assert!(kinds.contains(&"agent_stale"), "kinds: {kinds:?}");
    assert!(kinds.contains(&"agent_active"), "kinds: {kinds:?}");
    let stale_event = report
        .records
        .iter()
        .find(|record| record.kind == "agent_stale")
        .expect("stale event");
    let event: Value = serde_json::from_str(&stale_event.body.text).expect("event json");
    assert_eq!(event["agent_id"], agent_id);
    assert_eq!(event["state"], "stale");
    assert_eq!(stale_event.to, "#presence");
}

#[tokio::test]
async fn presence_gone_transition_with_short_ttl() {
    let temp = common::TempDir::new("m1-presence-gone");
    let root = temp.path().join("root");
    let state = state_with(&root, |config| {
        config.stale_after = Duration::from_millis(50);
        config.gone_after = Duration::from_millis(200);
        config.presence_sweep_interval = Duration::from_secs(60);
    });
    let (_agent_id, _token) = register(&state, "alice", "codex").await;
    let (_observer_id, observer) = register(&state, "observer", "agent").await;

    tokio::time::sleep(Duration::from_millis(450)).await;
    let mut session = ConnectionSession::default();
    let all = call(
        &state,
        &mut session,
        "list_agents",
        json!({"token": observer, "include_stale": true}),
    )
    .await
    .expect("gone list");
    assert_eq!(all["agents"][0]["state"], "gone");

    let (log, report) =
        LogFile::<Envelope>::open(root.join("channels/#presence.jsonl")).expect("presence log");
    drop(log);
    assert!(
        report
            .records
            .iter()
            .any(|record| record.kind == "agent_gone"),
        "gone transition must be logged"
    );
}

#[tokio::test]
async fn ttl_messages_expire_for_waiters_but_survive_in_the_log() {
    let temp = common::TempDir::new("m1-ttl");
    let root = temp.path().join("root");
    let state = state_with(&root, |_| {});
    let (_alice_id, alice) = register(&state, "alice", "codex").await;
    let (bob_id, _bob) = register(&state, "bob", "claude").await;
    let identity = state.authenticate(&alice).await.expect("alice identity");

    let durable = state
        .send_message(
            &identity,
            SendRequest {
                to: "bob".into(),
                kind: "event".into(),
                body: Body::new("text/plain", "kept"),
                reply_to: None,
                idempotency_key: None,
                ttl: Some(Duration::from_secs(30)),
            },
        )
        .await
        .expect("send durable ttl message");
    assert!(root
        .join("agents")
        .join(&bob_id)
        .join("expiry.jsonl")
        .is_file());

    let immediate = state
        .wait_message(&bob_id, Some("0"), Duration::ZERO)
        .await
        .expect("immediate wait");
    assert_eq!(immediate.messages.len(), 1);
    assert_eq!(immediate.messages[0].seq, durable.seq);

    let short = state
        .send_message(
            &identity,
            SendRequest {
                to: "bob".into(),
                kind: "event".into(),
                body: Body::new("text/plain", "short lived"),
                reply_to: None,
                idempotency_key: None,
                ttl: Some(Duration::from_millis(50)),
            },
        )
        .await
        .expect("send short ttl message");
    assert!(short.seq > durable.seq);

    tokio::time::sleep(Duration::from_millis(100)).await;
    let expired = state
        .wait_message(&bob_id, Some(&short.seq.to_string()), Duration::ZERO)
        .await
        .expect("expired wait");
    assert!(expired.messages.is_empty());

    // The durable message is still visible; only the short-lived record is
    // filtered from delivery.
    let remaining = state
        .wait_message(&bob_id, Some("0"), Duration::ZERO)
        .await
        .expect("remaining wait");
    assert_eq!(remaining.messages.len(), 1);
    assert_eq!(remaining.messages[0].seq, durable.seq);
    assert_eq!(inbox_line_count(&root, &bob_id), 2, "ttl keeps evidence");
}

async fn rpc_stream(
    reader: &mut BufReader<tokio::io::DuplexStream>,
    id: u64,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    let mut encoded = serde_json::to_vec(&request).expect("serialize request");
    encoded.push(b'\n');
    let stream = reader.get_mut();
    stream.write_all(&encoded).await.expect("write request");
    stream.flush().await.expect("flush request");
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read response");
    let response: cooperators::rpc::RpcResponse =
        serde_json::from_str(line.trim_end_matches(['\r', '\n'])).expect("response json");
    response.into_result()
}

#[tokio::test]
async fn ndjson_session_auth_and_messaging_over_in_process_streams() {
    let temp = common::TempDir::new("m1-duplex");
    let root = temp.path().join("root");
    let state = state_with(&root, |_| {});

    let (alice_client, alice_server) = tokio::io::duplex(64 * 1024);
    let alice_task = tokio::spawn(server::serve_stream(alice_server, Arc::clone(&state)));
    let mut alice = BufReader::new(alice_client);

    let registered = rpc_stream(
        &mut alice,
        1,
        "register_agent",
        json!({"name": "alice", "kind": "codex"}),
    )
    .await
    .expect("register alice over NDJSON");
    assert_eq!(registered["token"].as_str().expect("token").len(), 32);

    let (bob_client, bob_server) = tokio::io::duplex(64 * 1024);
    let bob_task = tokio::spawn(server::serve_stream(bob_server, Arc::clone(&state)));
    let mut bob = BufReader::new(bob_client);
    rpc_stream(
        &mut bob,
        3,
        "register_agent",
        json!({"name": "bob", "kind": "claude"}),
    )
    .await
    .expect("register bob over NDJSON");

    // No token on this request: the connection session supplies alice's.
    let sent = rpc_stream(
        &mut alice,
        2,
        "send_message",
        json!({"to": "bob", "kind": "request", "text": "over duplex"}),
    )
    .await
    .expect("send with session auth");
    assert_eq!(sent["seq"], 1);

    let waited = rpc_stream(
        &mut bob,
        4,
        "wait_message",
        json!({"cursor": "0", "timeout_s": 0}),
    )
    .await
    .expect("wait with session auth");
    assert_eq!(waited["messages"][0]["body"]["text"], "over duplex");
    assert_eq!(waited["messages"][0]["from"], "alice");

    let acked = rpc_stream(&mut bob, 5, "ack", json!({"cursor": "1"}))
        .await
        .expect("ack with session auth");
    assert_eq!(acked["acked_cursor"], "1");

    drop(alice);
    drop(bob);
    tokio::time::timeout(Duration::from_secs(2), alice_task)
        .await
        .expect("alice connection closes")
        .expect("alice task");
    tokio::time::timeout(Duration::from_secs(2), bob_task)
        .await
        .expect("bob connection closes")
        .expect("bob task");
}
