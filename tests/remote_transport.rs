mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cooperators::client::{BrokerClient, ClientError, Endpoint, HttpsOptions};
use cooperators::mcp;
use cooperators::network::method_contracts;
use cooperators::rpc::RpcRequest;
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use serde_json::{json, Value};

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
    state: Arc<DaemonState>,
    origin: String,
    token: String,
    agent_id: String,
    device_token: String,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
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
        config.network_policy = Some(serde_json::from_value(json!({
            "v":1, "allowed_hosts":["localhost"], "devices":[{
                "principal_id":"tls-test", "token_hash":cooperators::registry::token_hash(&device_token),
                "expires_at_ms":cooperators::id::unix_millis()+3_600_000,
                "allow_enroll":true, "max_instances":10, "instance_ttl_s":3600,
                "permissions":method_contracts().iter().map(|m|json!({"method":m.method,"resources":["*"]})).collect::<Vec<_>>()
            }]
        })).unwrap());
        let state = Arc::new(DaemonState::owned(config).unwrap());
        let registration = state.network().unwrap().enroll(&state, &device_token,
            json!({"launch_key":cooperators::registry::new_token().unwrap(),"kind":"agent","display_name":"network-alice"})).await.unwrap();
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
            state,
            origin: String::new(),
            token: registration["token"].as_str().unwrap().into(),
            agent_id: registration["agent_id"].as_str().unwrap().into(),
            device_token,
        };
        for _ in 0..300 {
            if ready.exists() {
                let value: Value = serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
                fixture.origin = format!("https://localhost:{}", value["port"]);
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
    assert!(matches!(result, Err(ClientError::HttpStatus(403))));
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
async fn tls_enrollment_and_pinned_bus_identity_use_the_approved_gateway() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let bus = fixture.state.network().unwrap().meta.bus_id.clone();
    let client = fixture.client().with_expected_bus_id(bus.clone()).unwrap();
    let enrollment=client.call("enroll_agent",json!({"token":fixture.device_token,
        "launch_key":cooperators::registry::new_token().unwrap(),"kind":"agent","display_name":"tls-enrolled"}),Duration::from_secs(5)).await.unwrap();
    assert_eq!(enrollment["bus_id"], bus);
    let identity = client
        .call(
            "instance_metadata",
            json!({"token":enrollment["token"]}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(identity["agent_id"], enrollment["agent_id"]);
    let wrong = fixture
        .client()
        .with_expected_bus_id(cooperators::id::new_ulid().unwrap())
        .unwrap();
    let before = fixture.events().len();
    assert!(matches!(
        wrong
            .call(
                "ping",
                json!({"token":fixture.token}),
                Duration::from_secs(5)
            )
            .await,
        Err(ClientError::Protocol(_))
    ));
    assert_eq!(
        fixture.events().len(),
        before,
        "bus mismatch must fail before RPC submission"
    );
}

#[tokio::test]
async fn scoped_credentials_keep_authorization_over_a_real_uds_connection() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let socket = fixture.temp.path().join("scoped.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let state = fixture.state.clone();
    let task = tokio::spawn(async move { server::serve(listener, state).await });
    let client = BrokerClient::uds(&socket);
    let identity = client
        .call(
            "instance_metadata",
            json!({"token":fixture.token}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(identity["agent_id"], fixture.agent_id);
    let error = client
        .call(
            "create_queue",
            json!({"token":fixture.token,"queue":"bypass"}),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error,ClientError::Rpc(error) if error.code == cooperators::error::ErrorCode::Forbidden.rpc_code())
    );
    assert!(!fixture
        .temp
        .path()
        .join("root/queues/bypass.jsonl")
        .exists());
    task.abort();
}

#[tokio::test]
async fn real_tls_mcp_preserves_header_identity_and_hides_unready_mutations() {
    let Some(fixture) = Fixture::start("proxy").await else {
        return;
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(
            reqwest::Certificate::from_pem(include_bytes!("fixtures/tls/ca.pem")).unwrap(),
        )
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let response = client
        .post(format!("{}/mcp", fixture.origin))
        .bearer_auth(&fixture.token)
        .header("Accept", "application/json, text/event-stream")
        .json(
            &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
            "name":"coop_instance_metadata","arguments":{}}}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let reply: Value = response.json().await.unwrap();
    assert_eq!(
        reply["result"]["structuredContent"]["agent_id"],
        fixture.agent_id
    );
    let response = client
        .post(format!("{}/mcp", fixture.origin))
        .bearer_auth(&fixture.token)
        .header("Accept", "application/json, text/event-stream")
        .json(
            &json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
            "name":"coop_instance_metadata","arguments":{"token":fixture.device_token}}}),
        )
        .send()
        .await
        .unwrap();
    let reply: Value = response.json().await.unwrap();
    assert!(reply.to_string().contains("FORBIDDEN"));
    let response = client
        .post(format!("{}/mcp", fixture.origin))
        .bearer_auth(&fixture.token)
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}))
        .send()
        .await
        .unwrap();
    let reply: Value = response.json().await.unwrap();
    assert!(reply["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .all(|tool| !matches!(
            tool["name"].as_str(),
            Some("coop_create_queue" | "coop_send_message" | "coop_register_agent")
        )));
}
