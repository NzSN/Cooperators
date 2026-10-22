mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cooperators::client::{BrokerClient, ClientError, Endpoint, HttpsOptions};
use cooperators::error::ErrorCode;
use cooperators::mcp;
use cooperators::network::{method_contracts, NetworkPolicy};
use cooperators::rpc::RpcRequest;
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[test]
fn contracts_cover_every_mcp_alias_and_native_route() {
    let contracts = method_contracts();
    let methods: BTreeSet<_> = contracts.iter().map(|c| c.method.as_str()).collect();
    assert_eq!(methods.len(), contracts.len());
    for spec in mcp::TOOL_SPECS {
        let contract = contracts.iter().find(|c| c.method == spec.method).unwrap();
        assert_eq!(contract.mcp_tool.as_deref(), Some(spec.tool));
        assert!(!contract.permission.is_empty());
        assert!(!contract.authoritative_storage.is_empty());
    }
    for method in ["ping", "echo", "enroll_agent"] {
        assert!(methods.contains(method));
    }
    assert_eq!(methods.len(), mcp::TOOL_SPECS.len() + 3);
}

#[test]
fn endpoint_rejects_plaintext_credentials_and_ambiguous_paths() {
    for value in [
        "http://localhost:7420",
        "https://user:secret@localhost",
        "https://localhost/mcp",
        "https://localhost?token=secret",
        "https://localhost#fragment",
    ] {
        let error = Endpoint::https(value).unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
    assert!(Endpoint::https("https://localhost:7420").is_ok());
}

#[test]
fn profile_credentials_are_private_referenced_and_not_debugged() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let temp = common::TempDir::new("network-profile");
    let secret = cooperators::registry::new_token().unwrap();
    let credential = temp.path().join("credential.json");
    std::fs::write(&credential, json!({"token":secret}).to_string()).unwrap();
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o600)).unwrap();
    let bus = cooperators::id::new_ulid().unwrap();
    let file = temp.path().join("profiles.json");
    std::fs::write(&file,json!({"v":1,"profiles":{"test":{"endpoint":"https://localhost:7420","expected_bus_id":bus,"credential_file":"credential.json"}}}).to_string()).unwrap();
    let profile = cooperators::transport::profile::load_profile(&file, "test").unwrap();
    assert_eq!(profile.token, secret);
    assert_eq!(profile.expected_bus_id, bus);
    assert!(!format!("{:?}", profile.client).contains(&secret));
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(cooperators::transport::profile::load_profile(&file, "test").is_err());
    std::fs::remove_file(&credential).unwrap();
    let target = temp.path().join("actual.json");
    std::fs::write(&target, json!({"token":secret}).to_string()).unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
    symlink(&target, &credential).unwrap();
    assert!(cooperators::transport::profile::load_profile(&file, "test").is_err());
}

async fn local(state: &Arc<DaemonState>, method: &str, params: Value) -> Value {
    server::dispatch(
        state,
        &mut ConnectionSession::default(),
        RpcRequest {
            id: Some(json!(1)),
            method: method.into(),
            params,
            is_notification: false,
        },
    )
    .await
    .unwrap()
    .into_result()
    .unwrap()
}

struct Fixture {
    temp: common::TempDir,
    proxy: Child,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
    uds_task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    state: Arc<DaemonState>,
    origin: String,
    token: String,
    agent_id: String,
    device_token: String,
    local_token: String,
    bus_id: String,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(task) = &self.uds_task {
            task.abort();
        }
        let _ = self.proxy.kill();
        let _ = self.proxy.wait();
    }
}

impl Fixture {
    async fn start(mode: &str) -> Option<Self> {
        let temp = common::TempDir::new("network-tls");
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.raw_os_error() == Some(1) => {
                assert!(
                    std::env::var_os("COOP_REQUIRE_REMOTE").is_none()
                        && std::env::var_os("COOP_REQUIRE_UDS").is_none(),
                    "required TCP fixture denied: {error}"
                );
                eprintln!("SKIP network TLS: sandbox TCP denial");
                return None;
            }
            Err(error) => panic!("native HTTP bind: {error}"),
        };
        let port = listener.local_addr().unwrap().port();
        let mut config = DaemonConfig::new(temp.path().join("root"));
        let device_token = cooperators::registry::new_token().unwrap();
        let policy: NetworkPolicy = serde_json::from_value(json!({
            "v":1, "allowed_hosts":["localhost"],
            "allowed_origins":["https://approved.example"],
            "devices":[{
                "principal_id":"test-device", "generation":1,
                "token_hash":cooperators::registry::token_hash(&device_token),
                "expires_at_ms":cooperators::id::unix_millis() + 60_000,
                "allow_enroll":true, "max_instances":8, "instance_ttl_s":60,
                "permissions":[
                    {"method":"ping","resources":["*"]},
                    {"method":"instance_metadata","resources":["agent:*"]},
                    {"method":"wait_message","resources":["inbox:*"]},
                    {"method":"read_doc","resources":["document:allowed"]},
                    {"method":"read_channel","resources":["channel:allowed"]},
                    {"method":"list_docs","resources":["document:allowed"]},
                    {"method":"list_agents","resources":["agent:*"]}
                ]
            }]
        }))
        .unwrap();
        config.network_policy = Some(policy);
        let state = Arc::new(DaemonState::owned(config).unwrap());
        let registration = local(&state, "register_agent", json!({"name":"local-only"})).await;
        let local_token = registration["token"].as_str().unwrap().to_string();
        let bus_id = state.network().unwrap().meta.bus_id.clone();
        for name in ["allowed", "private"] {
            local(
                &state,
                "publish_doc",
                json!({"token":local_token,"name":name,"text":name}),
            )
            .await;
        }
        let http_state = state.clone();
        let task = tokio::spawn(async move { mcp::serve_http(listener, http_state).await });
        let ready = temp.path().join("ready.json");
        let proxy = Command::new("python3")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/network_tls_proxy.py"),
            )
            .args(["--upstream-port", &port.to_string(), "--mode", mode])
            .arg("--ready")
            .arg(&ready)
            .arg("--events")
            .arg(temp.path().join("events.jsonl"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut fixture = Self {
            temp,
            proxy,
            task,
            uds_task: None,
            state,
            origin: String::new(),
            token: String::new(),
            agent_id: String::new(),
            device_token,
            local_token,
            bus_id,
        };
        for _ in 0..300 {
            if ready.exists() {
                let value: Value = serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
                fixture.origin = format!("https://localhost:{}", value["port"]);
                let enrollment = fixture.enroll("network-alice").await;
                fixture.token = enrollment["token"].as_str().unwrap().to_string();
                fixture.agent_id = enrollment["agent_id"].as_str().unwrap().to_string();
                assert_eq!(enrollment["bus_id"], fixture.bus_id);
                std::fs::write(fixture.temp.path().join("events.jsonl"), "").unwrap();
                return Some(fixture);
            }
            assert!(
                fixture.proxy.try_wait().unwrap().is_none(),
                "TLS ingress exited"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("TLS ingress readiness timeout");
    }

    fn client(&self) -> BrokerClient {
        BrokerClient::https(
            &self.origin,
            HttpsOptions {
                ca_pem: Some(include_bytes!("fixtures/tls/ca.pem").to_vec()),
                ..Default::default()
            },
        )
        .unwrap()
    }

    async fn start_uds(&mut self) -> PathBuf {
        let socket = self.temp.path().join("scoped-stdio.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let state = self.state.clone();
        self.uds_task = Some(tokio::spawn(
            async move { server::serve(listener, state).await },
        ));
        socket
    }

    async fn enroll(&self, label: &str) -> Value {
        self.client()
            .with_expected_bus_id(self.bus_id.clone())
            .unwrap()
            .call(
                "enroll_agent",
                json!({
                    "token":self.device_token,
                    "launch_key":cooperators::registry::new_token().unwrap(),
                    "kind":"test", "display_name":label
                }),
                Duration::from_secs(5),
            )
            .await
            .unwrap()
    }

    fn http_client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .add_root_certificate(
                reqwest::Certificate::from_pem(include_bytes!("fixtures/tls/ca.pem")).unwrap(),
            )
            .build()
            .unwrap()
    }

    async fn post(&self, path: &str, token: Option<&str>, request: Value) -> (u16, Value) {
        let mut builder = self
            .http_client()
            .post(format!("{}{path}", self.origin))
            .header("Accept", "application/json, text/event-stream")
            .json(&request);
        if let Some(token) = token {
            builder = builder.bearer_auth(token);
        }
        let response = builder.send().await.unwrap();
        let status = response.status().as_u16();
        let bytes = response.bytes().await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
            let text = std::str::from_utf8(&bytes).unwrap();
            text.lines()
                .find_map(|line| {
                    line.strip_prefix("data: ")
                        .and_then(|data| serde_json::from_str(data).ok())
                })
                .unwrap()
        });
        (status, value)
    }

    async fn mcp_tool(&self, token: &str, tool: &str, args: Value) -> (u16, Value) {
        self.post(
            "/mcp",
            Some(token),
            json!({
                "jsonrpc":"2.0", "id":7, "method":"tools/call",
                "params":{"name":tool,"arguments":args}
            }),
        )
        .await
    }

    fn events(&self) -> Vec<Value> {
        std::fs::read_to_string(self.temp.path().join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

#[tokio::test]
async fn verified_tls_forwards_native_identity_and_wait_without_body_credentials() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let client = fixture.client();
    let cli = tokio::process::Command::new(env!("CARGO_BIN_EXE_coop"))
        .args(["--endpoint", &fixture.origin, "--ca-cert"])
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls/ca.pem"))
        .args(["--token", &fixture.token, "wait", "--timeout", "0"])
        .env_remove("COOP_ENDPOINT")
        .output()
        .await
        .unwrap();
    assert!(
        cli.status.success(),
        "native remote CLI failed: {}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli_page: Value = serde_json::from_slice(&cli.stdout).unwrap();
    assert_eq!(cli_page["messages"], json!([]));
    let metadata = client
        .call(
            "instance_metadata",
            json!({"token":fixture.token}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(metadata["agent_id"], fixture.agent_id);
    let page = client
        .call(
            "wait_message",
            json!({"token":fixture.token,"timeout_s":0}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(page["messages"], json!([]));
    assert!(fixture
        .events()
        .iter()
        .all(|e| e["body_token"] == false && e["bearer_present"] == true));
    let result = client
        .call(
            "send_message",
            json!({"token":fixture.token,"to":fixture.agent_id,"text":"blocked"}),
            Duration::from_secs(5),
        )
        .await;
    assert!(
        matches!(result, Err(ClientError::Rpc(error)) if error.code == ErrorCode::Forbidden.rpc_code())
    );
    assert_eq!(
        local(
            &fixture.state,
            "wait_message",
            json!({"token":fixture.token,"timeout_s":0})
        )
        .await["messages"],
        json!([])
    );
}

#[tokio::test]
async fn tls_rejects_untrusted_certificate_and_wrong_hostname() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let untrusted = BrokerClient::https(&fixture.origin, HttpsOptions::default()).unwrap();
    assert!(matches!(
        untrusted
            .call(
                "ping",
                json!({"token":fixture.token}),
                Duration::from_secs(3)
            )
            .await,
        Err(ClientError::RemoteTransport(_))
    ));
    let mismatch = BrokerClient::https(
        &fixture.origin.replace("localhost", "127.0.0.1"),
        HttpsOptions {
            ca_pem: Some(include_bytes!("fixtures/tls/ca.pem").to_vec()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        mismatch
            .call(
                "ping",
                json!({"token":fixture.token}),
                Duration::from_secs(3)
            )
            .await,
        Err(ClientError::RemoteTransport(_))
    ));
    assert!(fixture.events().is_empty());
}

#[tokio::test]
async fn redirects_do_not_forward_credentials_or_retry() {
    let Some(fixture) = Fixture::start("redirect").await else {
        return;
    };
    assert!(matches!(
        fixture
            .client()
            .call(
                "ping",
                json!({"token":fixture.token}),
                Duration::from_secs(3)
            )
            .await,
        Err(ClientError::HttpStatus(307))
    ));
    assert_eq!(fixture.events().len(), 1);
}

#[tokio::test]
async fn lost_mutation_response_is_unknown_and_is_never_automatically_retried() {
    let Some(fixture) = Fixture::start("drop").await else {
        return;
    };
    let result = fixture
        .client()
        .call(
            "enqueue",
            json!({"token":fixture.token,"queue":"missing","payload":1}),
            Duration::from_secs(3),
        )
        .await;
    assert!(matches!(result, Err(ClientError::OutcomeUnknown(_))));
    assert_eq!(fixture.events().len(), 1);
}

#[tokio::test]
async fn deadlines_and_response_limits_cover_the_tls_exchange() {
    let Some(fixture) = Fixture::start("slow").await else {
        return;
    };
    let start = Instant::now();
    assert!(fixture
        .client()
        .call(
            "ping",
            json!({"token":fixture.token}),
            Duration::from_millis(100)
        )
        .await
        .is_err());
    assert!(start.elapsed() < Duration::from_millis(400));
    drop(fixture);
    for mode in ["malformed", "oversize"] {
        let Some(fixture) = Fixture::start(mode).await else {
            return;
        };
        assert!(fixture
            .client()
            .call(
                "ping",
                json!({"token":fixture.token}),
                Duration::from_secs(3)
            )
            .await
            .is_err());
    }
}

#[tokio::test]
async fn pinned_bus_identity_is_checked_before_rpc_and_enrollment() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let pinned = fixture
        .client()
        .with_expected_bus_id(fixture.bus_id.clone())
        .unwrap();
    let metadata = pinned
        .call(
            "instance_metadata",
            json!({"token":fixture.token}),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(metadata["agent_id"], fixture.agent_id);
    let before = fixture.events().len();
    let wrong = fixture
        .client()
        .with_expected_bus_id(cooperators::id::new_ulid().unwrap())
        .unwrap();
    for (method, params) in [
        ("instance_metadata", json!({"token":fixture.token})),
        (
            "enroll_agent",
            json!({"token":fixture.device_token,"launch_key":cooperators::registry::new_token().unwrap(),"kind":"test","display_name":"blocked"}),
        ),
    ] {
        assert!(matches!(
            wrong.call(method, params, Duration::from_secs(3)).await,
            Err(ClientError::Protocol(_))
        ));
    }
    assert_eq!(
        fixture.events().len(),
        before,
        "bus mismatch must prevent POST dispatch"
    );
}

#[tokio::test]
async fn native_and_http_mcp_require_scoped_instance_credentials() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    for token in [
        None,
        Some(fixture.local_token.as_str()),
        Some(fixture.device_token.as_str()),
    ] {
        let native = fixture
            .post(
                "/v1/rpc",
                token,
                json!({"jsonrpc":"2.0","id":1,"method":"ping","params":{}}),
            )
            .await;
        assert_eq!(native.0, 401);
        let mcp = fixture
            .post(
                "/mcp",
                token,
                json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}),
            )
            .await;
        assert_eq!(mcp.0, 401);
    }
    for token in [
        None,
        Some(fixture.local_token.as_str()),
        Some(fixture.token.as_str()),
    ] {
        let enroll = fixture.post("/v1/enroll", token, json!({"jsonrpc":"2.0","id":1,"method":"enroll_agent","params":{"launch_key":cooperators::registry::new_token().unwrap(),"kind":"test","display_name":"denied"}})).await;
        assert_eq!(enroll.0, 401);
    }
    let info = fixture
        .http_client()
        .get(format!("{}/v1/info", fixture.origin))
        .send()
        .await
        .unwrap();
    assert_eq!(info.status().as_u16(), 401);
}

#[tokio::test]
async fn native_and_http_mcp_do_not_accept_different_body_identity() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let second = fixture.enroll("second").await;
    let second_token = second["token"].as_str().unwrap();
    let native = fixture.post("/v1/rpc", Some(&fixture.token), json!({"jsonrpc":"2.0","id":1,"method":"instance_metadata","params":{"token":second_token}})).await;
    assert_eq!(native.0, 400);
    let mcp = fixture
        .mcp_tool(
            &fixture.token,
            "coop_instance_metadata",
            json!({"token":second_token}),
        )
        .await;
    assert_eq!(mcp.0, 200);
    assert_eq!(mcp.1["result"]["isError"], true);
    assert_eq!(
        mcp.1["result"]["structuredContent"]["data"]["code"],
        "FORBIDDEN"
    );
    let same = fixture
        .mcp_tool(&fixture.token, "coop_instance_metadata", json!({}))
        .await;
    assert_eq!(
        same.1["result"]["structuredContent"]["agent_id"],
        fixture.agent_id
    );
}

#[tokio::test]
async fn native_and_http_mcp_enforce_resource_permissions_and_read_only_catalog() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let allowed = fixture
        .client()
        .call(
            "read_doc",
            json!({"token":fixture.token,"name":"allowed"}),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(allowed["body"]["text"], "allowed");
    assert!(matches!(
        fixture
            .client()
            .call(
                "read_doc",
                json!({"token":fixture.token,"name":"private"}),
                Duration::from_secs(3)
            )
            .await,
        Err(ClientError::Rpc(error)) if error.code == ErrorCode::Forbidden.rpc_code()
    ));
    let mcp = fixture
        .mcp_tool(&fixture.token, "coop_read_doc", json!({"name":"private"}))
        .await;
    assert_eq!(mcp.1["result"]["isError"], true);
    assert_eq!(
        mcp.1["result"]["structuredContent"]["data"]["code"],
        "FORBIDDEN"
    );
    let list = fixture
        .client()
        .call(
            "list_docs",
            json!({"token":fixture.token}),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    let names: Vec<_> = list["docs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["allowed"]);
    let catalog = fixture
        .post(
            "/mcp",
            Some(&fixture.token),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}),
        )
        .await;
    let tools = catalog.1["result"]["tools"].as_array().unwrap();
    assert!(tools.iter().any(|t| t["name"] == "coop_read_doc"));
    assert!(!tools.iter().any(|t| matches!(
        t["name"].as_str(),
        Some("coop_send_message" | "coop_register_agent" | "coop_daemon_status")
    )));
    let blocked = fixture
        .mcp_tool(
            &fixture.token,
            "coop_send_message",
            json!({"to":fixture.agent_id,"text":"blocked"}),
        )
        .await;
    assert_eq!(blocked.1["result"]["isError"], true);
}

#[tokio::test]
async fn https_host_and_origin_policy_applies_to_native_and_mcp() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    for path in ["/v1/rpc", "/mcp"] {
        let body = if path == "/mcp" {
            json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}})
        } else {
            json!({"jsonrpc":"2.0","id":1,"method":"ping","params":{}})
        };
        for (header, value) in [
            ("Origin", "https://unapproved.example"),
            ("X-Test-Upstream-Host", "unapproved.example"),
        ] {
            let response = fixture
                .http_client()
                .post(format!("{}{path}", fixture.origin))
                .bearer_auth(&fixture.token)
                .header("Accept", "application/json, text/event-stream")
                .header(header, value)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 403);
        }
        let response = fixture
            .http_client()
            .post(format!("{}{path}", fixture.origin))
            .bearer_auth(&fixture.token)
            .header("Accept", "application/json, text/event-stream")
            .header("Origin", "https://approved.example")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
    }
}

struct RemoteStdio {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    next_id: u64,
}

impl RemoteStdio {
    async fn start(fixture: &Fixture, token: &str, resume: bool) -> Self {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_coop"));
        command
            .args([
                "--endpoint",
                &fixture.origin,
                "--expected-bus-id",
                &fixture.bus_id,
                "--ca-cert",
            ])
            .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls/ca.pem"))
            .args([
                "mcp-stdio",
                "--kind",
                "test",
                "--display-name",
                "stdio-remote",
            ])
            .env("COOP_TOKEN", token)
            .env_remove("COOP_ENDPOINT")
            .env_remove("COOP_SOCKET")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if resume {
            command.arg("--resume");
        }
        Self::start_command(command).await
    }

    async fn start_uds(socket: &std::path::Path, token: &str) -> Self {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_coop"));
        command
            .args(["mcp-stdio", "--resume"])
            .env("COOP_TOKEN", token)
            .env("COOP_SOCKET", socket)
            .env_remove("COOP_ENDPOINT")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        Self::start_command(command).await
    }

    async fn start_command(mut command: tokio::process::Command) -> Self {
        let mut child = command.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut process = Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        };
        let initialized = process.request("initialize", json!({"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"network-test","version":"1"}})).await;
        assert!(initialized.get("error").is_none());
        process
            .send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await;
        process
    }

    async fn send(&mut self, message: Value) {
        let mut bytes = serde_json::to_vec(&message).unwrap();
        bytes.push(b'\n');
        self.stdin.write_all(&bytes).await.unwrap();
        self.stdin.flush().await.unwrap();
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .await;
        loop {
            let mut line = String::new();
            let bytes =
                tokio::time::timeout(Duration::from_secs(10), self.stdout.read_line(&mut line))
                    .await
                    .expect("stdio response deadline")
                    .unwrap();
            assert_ne!(bytes, 0, "remote stdio closed before response");
            let reply: Value = serde_json::from_str(&line).unwrap();
            if reply["id"] == id {
                return reply;
            }
        }
    }

    async fn tool(&mut self, name: &str, args: Value) -> Value {
        self.request("tools/call", json!({"name":name,"arguments":args}))
            .await["result"]
            .clone()
    }

    async fn stop(mut self) {
        drop(self.stdin);
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
    }
}

#[tokio::test]
async fn actual_remote_stdio_enrolls_fresh_identity_and_enforces_resources() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let mut process = RemoteStdio::start(&fixture, &fixture.device_token, false).await;
    let metadata = process.tool("coop_instance_metadata", json!({})).await;
    assert!(!metadata["isError"].as_bool().unwrap_or(false));
    assert_ne!(metadata["structuredContent"]["agent_id"], fixture.agent_id);
    assert_eq!(
        metadata["structuredContent"]["display_name"],
        "stdio-remote"
    );
    let allowed = process
        .tool("coop_read_doc", json!({"name":"allowed"}))
        .await;
    assert_eq!(allowed["structuredContent"]["body"]["text"], "allowed");
    let denied = process
        .tool("coop_read_doc", json!({"name":"private"}))
        .await;
    assert_eq!(denied["isError"], true);
    let switch = process
        .tool("coop_instance_metadata", json!({"token":fixture.token}))
        .await;
    assert_eq!(switch["isError"], true);
    assert_eq!(switch["structuredContent"]["data"]["code"], "FORBIDDEN");
    let unchanged = process.tool("coop_instance_metadata", json!({})).await;
    assert_eq!(
        unchanged["structuredContent"]["agent_id"],
        metadata["structuredContent"]["agent_id"]
    );
    let tools = process.request("tools/list", json!({})).await;
    assert!(!tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["name"] == "coop_send_message"));
    assert!(fixture
        .events()
        .iter()
        .all(|e| e["body_token"] == false && e["bearer_present"] == true));
    process.stop().await;
}

#[tokio::test]
async fn principal_generation_and_expiry_fence_native_mcp_and_stdio() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let mut process = RemoteStdio::start(&fixture, &fixture.token, true).await;
    let before = process.tool("coop_instance_metadata", json!({})).await;
    assert_eq!(before["structuredContent"]["agent_id"], fixture.agent_id);
    let network = fixture.state.network().unwrap();
    let mut policy = network.policy();
    policy.devices[0].generation += 1;
    network
        .replace_policy(&fixture.state, policy.clone())
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .client()
            .call(
                "instance_metadata",
                json!({"token":fixture.token}),
                Duration::from_secs(3)
            )
            .await,
        Err(ClientError::HttpStatus(401))
    ));
    assert_eq!(
        fixture
            .mcp_tool(&fixture.token, "coop_instance_metadata", json!({}))
            .await
            .0,
        401
    );
    assert_eq!(
        process.tool("coop_instance_metadata", json!({})).await["isError"],
        true
    );
    process.stop().await;
    let current = fixture.enroll("current-generation").await;
    let token = current["token"].as_str().unwrap();
    let mut current_process = RemoteStdio::start(&fixture, token, true).await;
    assert_eq!(
        current_process
            .tool("coop_instance_metadata", json!({}))
            .await["structuredContent"]["agent_id"],
        current["agent_id"]
    );
    policy.devices[0].expires_at_ms = cooperators::id::unix_millis().saturating_sub(1);
    network
        .replace_policy(&fixture.state, policy)
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .client()
            .call(
                "instance_metadata",
                json!({"token":token}),
                Duration::from_secs(3)
            )
            .await,
        Err(ClientError::HttpStatus(401))
    ));
    assert_eq!(
        fixture
            .mcp_tool(token, "coop_instance_metadata", json!({}))
            .await
            .0,
        401
    );
    assert_eq!(
        current_process
            .tool("coop_instance_metadata", json!({}))
            .await["isError"],
        true
    );
    current_process.stop().await;
}

#[tokio::test]
async fn actual_remote_stdio_cannot_resume_anonymous_or_local_unscoped_identity() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    for token in ["", fixture.local_token.as_str()] {
        let mut process = RemoteStdio::start(&fixture, token, true).await;
        let result = process.tool("coop_instance_metadata", json!({})).await;
        assert_eq!(result["isError"], true);
        process.stop().await;
    }
}

#[tokio::test]
async fn permission_revocation_interrupts_native_mcp_and_stdio_long_polls() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let mut process = RemoteStdio::start(&fixture, &fixture.token, true).await;
    let client = fixture.client();
    let before = fixture.events().len();
    let started = Instant::now();
    let revoke = async {
        for _ in 0..200 {
            if fixture.events().len() >= before + 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            fixture.events().len() >= before + 3,
            "all three wait requests must reach TLS ingress"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let network = fixture.state.network().unwrap();
        let mut policy = network.policy();
        policy.devices[0]
            .permissions
            .retain(|permission| permission.method != "wait_message");
        network
            .replace_policy(&fixture.state, policy)
            .await
            .unwrap();
    };
    let (native, http_mcp, stdio, ()) = tokio::time::timeout(Duration::from_secs(4), async {
        tokio::join!(
            client.call(
                "wait_message",
                json!({"token":fixture.token,"timeout_s":20}),
                Duration::from_secs(3)
            ),
            fixture.mcp_tool(&fixture.token, "coop_wait_message", json!({"timeout_s":20})),
            process.tool("coop_wait_message", json!({"timeout_s":20})),
            revoke
        )
    })
    .await
    .expect("revocation must interrupt long polls before their wait deadline");
    assert!(
        matches!(native, Err(ClientError::Rpc(error)) if error.code == ErrorCode::Forbidden.rpc_code())
    );
    assert_eq!(http_mcp.0, 200);
    assert_eq!(http_mcp.1["result"]["isError"], true);
    assert_eq!(stdio["isError"], true);
    assert!(started.elapsed() < Duration::from_secs(3));
    process.stop().await;
}

#[tokio::test]
async fn uds_backed_stdio_cannot_replace_scoped_identity_or_register_unscoped_identity() {
    let Some(mut fixture) = Fixture::start("proxy").await else {
        return;
    };
    let socket = fixture.start_uds().await;
    let mut process = RemoteStdio::start_uds(&socket, &fixture.token).await;
    let metadata = process.tool("coop_instance_metadata", json!({})).await;
    assert_eq!(metadata["structuredContent"]["agent_id"], fixture.agent_id);
    for attempted in [
        Value::Null,
        json!(""),
        json!(17),
        json!(fixture.local_token),
    ] {
        let denied = process
            .tool(
                "coop_register_agent",
                json!({"name":"escape","token":attempted}),
            )
            .await;
        assert_eq!(denied["isError"], true);
        assert_eq!(denied["structuredContent"]["data"]["code"], "FORBIDDEN");
    }
    let denied = process
        .tool("coop_create_queue", json!({"queue":"escape"}))
        .await;
    assert_eq!(denied["isError"], true);
    assert_eq!(denied["structuredContent"]["data"]["code"], "FORBIDDEN");
    let unchanged = process
        .tool("coop_instance_metadata", json!({"token":fixture.token}))
        .await;
    assert_eq!(unchanged["structuredContent"]["agent_id"], fixture.agent_id);
    let registry: Value = serde_json::from_slice(
        &std::fs::read(fixture.temp.path().join("root/registry.json")).unwrap(),
    )
    .unwrap();
    assert!(registry["agents"].get("escape").is_none());
    process.stop().await;
}
