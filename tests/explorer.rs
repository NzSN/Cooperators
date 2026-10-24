mod common;

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use cooperators::error::ErrorCode;
use cooperators::rpc::RpcRequest;
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use serde_json::{json, Value};

const TOKEN: &str = "explorer-test-token-not-a-real-secret-0123456789";

#[tokio::test]
async fn inbox_content_is_visible_only_to_its_authenticated_recipient() {
    let temp = common::TempDir::new("inbox-content-privacy");
    let file = temp.path().join("explorer.token");
    token_file(&file, TOKEN);
    let state =
        Arc::new(DaemonState::with_config(config(&temp.path().join("root"), &file)).unwrap());
    let alice = state
        .register_agent("alice".into(), None, None, None)
        .await
        .unwrap();
    let bob = state
        .register_agent("bob".into(), None, None, None)
        .await
        .unwrap();
    let observer = state
        .register_agent("unrelated".into(), None, None, None)
        .await
        .unwrap();
    let secret = "private-inbox-content-for-bob-only";
    let sent = call(
        &state,
        &mut ConnectionSession::default(),
        "send_message",
        json!({
            "token":alice.token, "to":bob.agent_id, "kind":"text", "text":secret
        }),
    )
    .await;
    assert!(sent.get("result").is_some());
    for token in [&alice.token, &observer.token] {
        let reply = call(
            &state,
            &mut ConnectionSession::default(),
            "wait_message",
            json!({
                "token":token, "timeout_s":0, "agent_id":bob.agent_id,
                "to":bob.name, "name":bob.name, "cursor":"0"
            }),
        )
        .await;
        assert_eq!(reply["result"]["messages"], json!([]));
        assert!(!reply.to_string().contains(secret));
    }
    let own = call(
        &state,
        &mut ConnectionSession::default(),
        "wait_message",
        json!({"token":bob.token,"timeout_s":0}),
    )
    .await;
    assert_eq!(own["result"]["messages"].as_array().unwrap().len(), 1);
    assert!(own.to_string().contains(secret));
    for method in ["wait_message", "ack"] {
        let denied = call(
            &state,
            &mut ConnectionSession::default(),
            method,
            json!({"token":TOKEN,"agent_id":bob.agent_id,"cursor":"0","timeout_s":0}),
        )
        .await;
        assert_eq!(denied["error"]["code"], ErrorCode::Forbidden.rpc_code());
        assert!(!denied.to_string().contains(secret));
    }
    for method in ["list_agents", "daemon.status"] {
        let metadata = call(
            &state,
            &mut ConnectionSession::default(),
            method,
            json!({"token":TOKEN,"include_stale":true}),
        )
        .await;
        assert!(metadata.get("result").is_some());
        assert!(!metadata.to_string().contains(secret));
    }
    assert_mcp_inbox_binding(
        cooperators::mcp::Backend::embedded(Arc::clone(&state)),
        &alice.token,
        &bob.token,
        secret,
    )
    .await;
}

async fn assert_mcp_inbox_binding(
    backend: cooperators::mcp::Backend,
    alice: &str,
    bob: &str,
    secret: &str,
) {
    use cooperators::mcp::{canonical_tool_result, CoopMcpHandler};
    use rmcp::model::CallToolResponse;
    let alice_handler = CoopMcpHandler::stdio(backend.clone(), Some(alice.to_owned()));
    let unbound_handler = CoopMcpHandler::stdio(backend.clone(), None);
    let bob_handler = CoopMcpHandler::stdio(backend, Some(bob.to_owned()));
    for (handler, should_see_content) in [(&alice_handler, false), (&bob_handler, true)] {
        let CallToolResponse::Complete(result) = handler
            .dispatch_tool(
                "coop_wait_message",
                Some(serde_json::from_value(json!({"timeout_s":0})).unwrap()),
            )
            .await
        else {
            panic!("unexpected MCP task")
        };
        let result = canonical_tool_result(&result);
        assert_eq!(result["ok"], true);
        assert_eq!(result.to_string().contains(secret), should_see_content);
    }
    for tool in ["coop_wait_message", "coop_ack", "coop_instance_metadata"] {
        let CallToolResponse::Complete(result) = alice_handler
            .dispatch_tool(
                tool,
                Some(
                    serde_json::from_value(json!({"token":bob,"timeout_s":0,"cursor":"0"}))
                        .unwrap(),
                ),
            )
            .await
        else {
            panic!("unexpected MCP task")
        };
        let denied = canonical_tool_result(&result);
        assert_eq!(denied["error"]["code"], ErrorCode::Forbidden.rpc_code());
        assert!(!denied.to_string().contains(secret));
        assert_eq!(alice_handler.captured_token().await.as_deref(), Some(alice));
    }
    let CallToolResponse::Complete(retired) =
        alice_handler.dispatch_tool("coop_retire_agent", None).await
    else {
        panic!("unexpected MCP task")
    };
    assert_eq!(canonical_tool_result(&retired)["ok"], true);
    for handler in [&unbound_handler, &alice_handler] {
        let CallToolResponse::Complete(result) = handler
            .dispatch_tool(
                "coop_wait_message",
                Some(serde_json::from_value(json!({"token":bob,"timeout_s":0})).unwrap()),
            )
            .await
        else {
            panic!("unexpected MCP task")
        };
        let denied = canonical_tool_result(&result);
        assert_eq!(denied["error"]["code"], ErrorCode::Forbidden.rpc_code());
        assert!(!denied.to_string().contains(secret));
    }
}

#[tokio::test]
async fn real_uds_mcp_adapter_cannot_replace_its_associated_agent_token() {
    let temp = common::TempDir::new("uds-inbox-privacy");
    let root = temp.path().join("root");
    let state = Arc::new(DaemonState::with_config(DaemonConfig::new(&root)).unwrap());
    let alice = state
        .register_agent("alice".into(), None, None, None)
        .await
        .unwrap();
    let bob = state
        .register_agent("bob".into(), None, None, None)
        .await
        .unwrap();
    let secret = "private-real-uds-message-for-bob";
    call(
        &state,
        &mut ConnectionSession::default(),
        "send_message",
        json!({"token":alice.token,"to":bob.agent_id,"kind":"text","text":secret}),
    )
    .await;
    let (listener, socket) = match server::bind(&root) {
        Ok(bound) => bound,
        Err(error)
            if error.raw_os_error() == Some(1)
                && std::env::var_os("COOP_REQUIRE_UDS").is_none() =>
        {
            eprintln!("SKIP real UDS inbox privacy: sandbox denies AF_UNIX");
            return;
        }
        Err(error) => panic!("UDS bind failed: {error}"),
    };
    let server = tokio::spawn(server::serve(listener, state));
    assert_mcp_inbox_binding(
        cooperators::mcp::Backend::uds(socket),
        &alice.token,
        &bob.token,
        secret,
    )
    .await;
    server.abort();
    let _ = server.await;
}

fn token_file(path: &Path, token: &str) {
    fs::write(path, format!("{token}\n")).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn config(root: &Path, file: &Path) -> DaemonConfig {
    let mut config = DaemonConfig::new(root);
    config.explorer_token_file = Some(file.to_owned());
    config
}

async fn call(
    state: &Arc<DaemonState>,
    session: &mut ConnectionSession,
    method: &str,
    params: Value,
) -> Value {
    let response = server::dispatch(
        state,
        session,
        RpcRequest {
            id: Some(json!(1)),
            method: method.into(),
            params,
            is_notification: false,
        },
    )
    .await
    .unwrap();
    serde_json::to_value(response).unwrap()
}

fn files(root: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            result.extend(files(&path));
        } else {
            let bytes = fs::read(&path).unwrap();
            result.insert(path, bytes);
        }
    }
    result
}

#[test]
fn configured_credentials_fail_closed_and_never_print_secrets() {
    let temp = common::TempDir::new("explorer-files");
    let file = temp.path().join("explorer.token");
    let root = temp.path().join("root");
    assert!(DaemonState::with_config(config(&root, &file)).is_err());
    assert!(!root.exists());
    for invalid in [
        "",
        "too-short",
        "token with spaces in it and sufficient length",
    ] {
        token_file(&file, invalid);
        assert!(DaemonState::with_config(config(&root, &file)).is_err());
        assert!(!root.exists());
    }
    token_file(&file, TOKEN);
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
    let error = DaemonState::with_config(config(&root, &file))
        .unwrap_err()
        .to_string();
    assert!(!error.contains(TOKEN));
    assert!(!root.exists());
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
    let link = temp.path().join("link");
    std::os::unix::fs::symlink(&file, &link).unwrap();
    assert!(DaemonState::with_config(config(&root, &link)).is_err());
    assert!(!root.exists());
    let state = DaemonState::with_config(config(&root, &file)).unwrap();
    assert!(!format!("{state:?}").contains(TOKEN));
    assert_eq!(
        cooperators::explorer::read_token_file(&file).unwrap(),
        TOKEN
    );
}

#[tokio::test]
async fn observation_never_registers_touches_presence_or_writes_source_files() {
    let temp = common::TempDir::new("explorer-observation");
    let file = temp.path().join("explorer.token");
    token_file(&file, TOKEN);
    let root = temp.path().join("root");
    let mut cfg = config(&root, &file);
    cfg.stale_after = Duration::ZERO;
    cfg.gone_after = Duration::ZERO;
    let state = Arc::new(DaemonState::with_config(cfg).unwrap());
    let agent = state
        .register_agent("real-agent".into(), None, None, None)
        .await
        .unwrap();
    let before = files(&root);
    let mut session = ConnectionSession::default();
    let active = call(&state, &mut session, "list_agents", json!({"token":TOKEN})).await;
    assert_eq!(active["result"]["agents"], json!([]));
    let all = call(
        &state,
        &mut session,
        "list_agents",
        json!({"include_stale":true}),
    )
    .await;
    assert_eq!(all["result"]["agents"].as_array().unwrap().len(), 1);
    assert_eq!(all["result"]["agents"][0]["agent_id"], agent.agent_id);
    assert_eq!(all["result"]["agents"][0]["state"], "gone");
    assert!(call(&state, &mut session, "daemon.status", json!({}))
        .await
        .get("result")
        .is_some());
    assert_eq!(before, files(&root));
    assert!(!fs::read_to_string(root.join("registry.json"))
        .unwrap()
        .contains(TOKEN));
}

#[tokio::test]
async fn explorer_is_denied_every_agent_method_and_session_credential_bypass() {
    let temp = common::TempDir::new("explorer-denials");
    let file = temp.path().join("explorer.token");
    token_file(&file, TOKEN);
    let root = temp.path().join("root");
    let state = Arc::new(DaemonState::with_config(config(&root, &file)).unwrap());
    let before = files(&root);
    for method in [
        "register_agent",
        "enroll_agent",
        "retire_agent",
        "instance_metadata",
        "send_message",
        "wait_message",
        "ack",
        "heartbeat",
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
        "operation_status",
        "unknown_method",
        "echo",
    ] {
        let reply = call(
            &state,
            &mut ConnectionSession::default(),
            method,
            json!({"token":TOKEN}),
        )
        .await;
        assert_eq!(
            reply["error"]["code"],
            ErrorCode::Forbidden.rpc_code(),
            "{method}: {reply}"
        );
    }
    let mut session = ConnectionSession::default();
    call(&state, &mut session, "list_agents", json!({"token":TOKEN})).await;
    for token in [Value::Null, json!("invalid-other-token"), json!("")] {
        let reply = call(&state, &mut session, "enroll_agent", json!({"token":token, "launch_key":"another-key", "kind":"cli", "display_name":"bypass"})).await;
        assert!(reply.get("error").is_some());
    }
    let reply = call(
        &state,
        &mut ConnectionSession::default(),
        "enroll_agent",
        json!({"launch_key":TOKEN,"kind":"cli","display_name":"bypass"}),
    )
    .await;
    assert_eq!(reply["error"]["code"], ErrorCode::Forbidden.rpc_code());
    assert_eq!(before, files(&root));
}

#[tokio::test]
async fn restart_rotates_explorer_without_registering_or_changing_agent_tokens() {
    let temp = common::TempDir::new("explorer-rotation");
    let file = temp.path().join("explorer.token");
    token_file(&file, TOKEN);
    let root = temp.path().join("root");
    let state = Arc::new(DaemonState::with_config(config(&root, &file)).unwrap());
    let agent = state
        .register_agent("normal".into(), None, None, None)
        .await
        .unwrap();
    let next = "different-explorer-test-token-0123456789";
    token_file(&file, next);
    let old = call(
        &state,
        &mut ConnectionSession::default(),
        "list_agents",
        json!({"token":TOKEN}),
    )
    .await;
    assert!(
        old.get("result").is_some(),
        "file changes apply only at restart"
    );
    drop(state);
    let state = Arc::new(DaemonState::with_config(config(&root, &file)).unwrap());
    let old = call(
        &state,
        &mut ConnectionSession::default(),
        "list_agents",
        json!({"token":TOKEN}),
    )
    .await;
    assert_eq!(old["error"]["code"], ErrorCode::Unauthenticated.rpc_code());
    let new = call(
        &state,
        &mut ConnectionSession::default(),
        "list_agents",
        json!({"token":next,"include_stale":true}),
    )
    .await;
    assert_eq!(new["result"]["agents"].as_array().unwrap().len(), 1);
    assert!(state.authenticate(&agent.token).await.is_ok());
    drop(state);
    token_file(&file, &agent.token);
    assert!(
        DaemonState::with_config(config(&root, &file)).is_err(),
        "agent credentials cannot become explorer credentials"
    );
}

#[tokio::test]
async fn explorer_is_not_an_http_or_mcp_bearer() {
    let temp = common::TempDir::new("explorer-http-denied");
    let file = temp.path().join("explorer.token");
    token_file(&file, TOKEN);
    let root = temp.path().join("root");
    let state = Arc::new(DaemonState::with_config(config(&root, &file)).unwrap());
    let service = cooperators::mcp::http_service(Arc::clone(&state));
    for endpoint in ["/mcp", "/metrics"] {
        let request = http::Request::builder()
            .method(if endpoint == "/mcp" { "POST" } else { "GET" })
            .uri(endpoint)
            .header(http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .body(http_body_util::Full::new(bytes::Bytes::from_static(b"{}")))
            .unwrap();
        let response = cooperators::mcp::handle_http(&service, &state, request).await;
        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
async fn explorer_stays_local_on_network_roots_and_cannot_share_device_credentials() {
    let temp = common::TempDir::new("explorer-network");
    let file = temp.path().join("explorer.token");
    token_file(&file, TOKEN);
    let device = cooperators::registry::new_token().unwrap();
    let mut policy: cooperators::network::NetworkPolicy = serde_json::from_value(json!({
        "v":1, "allowed_hosts":["bus.example:443"], "devices":[{
            "principal_id":"device", "token_hash":cooperators::registry::token_hash(&device),
            "expires_at_ms":cooperators::id::unix_millis() + 60_000,
            "allow_enroll":true, "max_instances":4, "instance_ttl_s":60,
            "permissions":[{"method":"list_agents","resources":["*"]}]
        }]
    }))
    .unwrap();
    let mut cfg = config(&temp.path().join("root"), &file);
    cfg.network_policy = Some(policy.clone());
    let state = Arc::new(DaemonState::with_config(cfg).unwrap());
    let reply = call(
        &state,
        &mut ConnectionSession::default(),
        "list_agents",
        json!({"token":TOKEN}),
    )
    .await;
    assert_eq!(reply["result"]["agents"], json!([]));
    assert!(
        cooperators::network::call_remote(&state, TOKEN, "list_agents", json!({}))
            .await
            .is_err()
    );
    let denied = call(
        &state,
        &mut ConnectionSession::default(),
        "register_agent",
        json!({"token":TOKEN,"name":"human"}),
    )
    .await;
    assert_eq!(denied["error"]["code"], ErrorCode::Forbidden.rpc_code());
    policy.devices[0].token_hash = cooperators::registry::token_hash(TOKEN);
    let mut cfg = config(&temp.path().join("collision"), &file);
    cfg.network_policy = Some(policy);
    assert!(DaemonState::with_config(cfg).is_err());
}

struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn command(root: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_coop"));
    for key in [
        "COOP_TOKEN",
        "COOP_ENDPOINT",
        "COOP_SOCKET",
        "COOP_EXPLORER_TOKEN_FILE",
        "COOP_HTTP_ADDR",
    ] {
        cmd.env_remove(key);
    }
    cmd.env("COOPERATORS_HOME", root);
    cmd
}

#[test]
fn real_daemon_accepts_startup_file_and_cli_reads_without_registration() {
    let temp = common::TempDir::new("explorer-cli");
    let probe = temp.path().join("probe.sock");
    match std::os::unix::net::UnixListener::bind(&probe) {
        Ok(listener) => {
            drop(listener);
            fs::remove_file(probe).unwrap();
        }
        Err(error)
            if error.raw_os_error() == Some(1)
                && std::env::var_os("COOP_REQUIRE_UDS").is_none() =>
        {
            eprintln!(
                "SKIP explorer CLI: sandbox denies AF_UNIX; set COOP_REQUIRE_UDS=1 outside sandbox"
            );
            return;
        }
        Err(error) => panic!("real UDS probe failed: {error}"),
    }
    let file = temp.path().join("explorer.token");
    token_file(&file, TOKEN);
    for use_env in [false, true] {
        let root = temp
            .path()
            .join(if use_env { "env-root" } else { "flag-root" });
        let mut cmd = command(&root);
        cmd.args(["daemon", "run", "--http-addr", "off"]);
        if use_env {
            cmd.env("COOP_EXPLORER_TOKEN_FILE", &file);
        } else {
            cmd.arg("--explorer-token-file").arg(&file);
        }
        let mut daemon = Daemon(
            cmd.stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let socket = root.join("run/daemon.sock");
        let mut ready = false;
        for _ in 0..250 {
            if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
                ready = true;
                break;
            }
            assert!(
                daemon.0.try_wait().unwrap().is_none(),
                "daemon exited before listening"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(ready, "daemon did not bind its real UDS socket");
        let output = command(&root)
            .arg("--token-file")
            .arg(&file)
            .args(["agents", "--include-stale"])
            .env("COOP_TOKEN", "invalid-inherited-token")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap(),
            json!({"agents":[]})
        );
        let denied = command(&root)
            .arg("--token-file")
            .arg(&file)
            .arg("heartbeat")
            .output()
            .unwrap();
        assert!(!denied.status.success());
        assert!(!String::from_utf8_lossy(&denied.stderr).contains(TOKEN));
        assert!(
            !root.join("registry.json").exists(),
            "explorer did not need to create a registry"
        );
    }
}
