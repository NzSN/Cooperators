mod common;
#[path = "common/policy.rs"]
mod policy_approval;

use std::sync::Arc;
use std::time::Duration;

use cooperators::error::{BrokerError, ErrorCode};
use cooperators::network::{call_remote, DevicePrincipal, NetworkPolicy, Permission};
use cooperators::registry::{new_token, token_hash};
use cooperators::rpc::RpcRequest;
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use serde_json::{json, Value};

fn permission(method: &str, resource: &str) -> Permission {
    Permission {
        method: method.into(),
        resources: vec![resource.into()],
    }
}

fn policy(token: &str) -> NetworkPolicy {
    NetworkPolicy {
        v: 1,
        revision: 1,
        enable_operations: false,
        enable_resource_operations: false,
        allowed_hosts: vec!["bus.example:443".into()],
        allowed_origins: vec!["https://ui.example".into()],
        devices: vec![DevicePrincipal {
            principal_id: "device-a".into(),
            generation: 1,
            token_hash: token_hash(token),
            expires_at_ms: cooperators::id::unix_millis() + 60_000,
            allow_enroll: true,
            max_instances: 4,
            instance_ttl_s: 60,
            permissions: vec![
                permission("ping", "*"),
                permission("instance_metadata", "*"),
                permission("wait_message", "*"),
                permission("read_channel", "channel:#allowed"),
                permission("read_doc", "document:allowed-*"),
                permission("list_docs", "document:allowed-*"),
                permission("send_message", "*"),
            ],
        }],
        max_connections: 8,
        requests_per_principal: 3,
        polls_per_principal: 1,
    }
}

fn state(root: &common::TempDir, policy: NetworkPolicy) -> Arc<DaemonState> {
    let mut config = DaemonConfig::new(root.path().to_path_buf());
    config.network_policy = Some(policy);
    Arc::new(DaemonState::with_config(config).unwrap())
}

fn enrollment(key: &str) -> Value {
    json!({"launch_key":key,"kind":"test","display_name":"security-test"})
}

fn code(error: BrokerError) -> Option<ErrorCode> {
    ErrorCode::from_rpc_code(error.into_rpc_error().code)
}

async fn enroll(state: &DaemonState, device: &str, key: &str) -> Value {
    state
        .network()
        .unwrap()
        .enroll(state, device, enrollment(key))
        .await
        .unwrap()
}

async fn await_poll_admission(state: &DaemonState) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match state.network().unwrap().request_permit("device-a", true) {
                Ok(permit) => drop(permit),
                Err(error) => {
                    assert_eq!(code(error), Some(ErrorCode::ResourceExhausted));
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("read admitted as a bounded poll");
}

#[test]
fn policy_requires_exact_host_origin_and_unambiguous_patterns() {
    let mut policy = policy(&new_token().unwrap());
    policy.validate().unwrap();
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::HOST, "BUS.EXAMPLE:443".parse().unwrap());
    assert!(policy.headers_allowed(&headers));
    headers.insert(http::header::HOST, "bus.example:444".parse().unwrap());
    assert!(!policy.headers_allowed(&headers));
    policy.allowed_hosts = vec!["bus.example".into()];
    assert!(!policy.headers_allowed(&headers));
    headers.insert(http::header::HOST, "bus.example".parse().unwrap());
    assert!(policy.headers_allowed(&headers));
    for host in ["bus.example:65536", "someone@bus.example", "bus.example:"] {
        headers.insert(http::header::HOST, host.parse().unwrap());
        assert!(!policy.headers_allowed(&headers));
    }
    headers.insert(http::header::HOST, "bus.example".parse().unwrap());
    headers.insert(http::header::ORIGIN, "https://ui.example".parse().unwrap());
    assert!(policy.headers_allowed(&headers));
    headers.append(
        http::header::ORIGIN,
        "https://evil.example".parse().unwrap(),
    );
    assert!(!policy.headers_allowed(&headers));
    headers.remove(http::header::ORIGIN);
    headers.append(http::header::HOST, "evil.example".parse().unwrap());
    assert!(!policy.headers_allowed(&headers));
    for invalid in ["bus.example:", "bus.example:65536", "*@bus.example"] {
        policy.allowed_hosts = vec![invalid.into()];
        assert!(policy.validate().is_err(), "invalid authority: {invalid}");
    }
    policy.allowed_hosts = vec!["bus.example".into()];
    for invalid in [
        "**",
        "document:allowed-**",
        "document:*private",
        "document:\n",
    ] {
        policy.devices[0].permissions = vec![permission("read_doc", invalid)];
        assert!(policy.validate().is_err(), "invalid resource pattern");
    }
    policy.devices[0].permissions = vec![permission("unlisted_method", "*")];
    assert!(policy.validate().is_err());
    for authority in ["[::1]", "[::1]:443"] {
        let mut ipv6_policy = policy.clone();
        ipv6_policy.devices.clear();
        ipv6_policy.allowed_hosts = vec![authority.into()];
        ipv6_policy.validate().unwrap();
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::HOST, authority.parse().unwrap());
        assert!(ipv6_policy.headers_allowed(&headers));
    }
}

#[tokio::test]
async fn scoped_authority_denies_local_credentials_resources_and_mutations() {
    let root = common::TempDir::new("network-scope");
    let device = new_token().unwrap();
    let state = state(&root, policy(&device));
    let local = state
        .register_agent("local".into(), None, None, None)
        .await
        .unwrap();
    let key = new_token().unwrap();
    let issued = enroll(&state, &device, &key).await;
    let network = state.network().unwrap();
    assert_eq!(
        code(
            network
                .authenticate(&state, &local.token)
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::Unauthenticated)
    );
    let context = network.authenticate(&state, &key).await.unwrap();
    assert_eq!(
        network
            .authorize(&state, &context, "read_doc", &json!({"name":"allowed-one"}))
            .await
            .unwrap(),
        "document:allowed-one"
    );
    for (method, params) in [
        ("read_doc", json!({"name":"private"})),
        ("read_channel", json!({"channel":"#private"})),
        ("daemon.status", json!({})),
        ("register_agent", json!({"name":"unapproved"})),
        ("not-a-route", json!({})),
    ] {
        assert_eq!(
            call_remote(&state, &key, method, params)
                .await
                .unwrap_err()
                .code,
            ErrorCode::Forbidden.rpc_code()
        );
    }
    assert_eq!(
        call_remote(&state, &key, "ping", json!({"token":local.token}))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Forbidden.rpc_code()
    );
    // A granted resource mutation still remains unavailable until N3.
    assert_eq!(
        call_remote(
            &state,
            &key,
            "send_message",
            json!({"to":issued["agent_id"],"body":{"mime":"text/plain","text":"no effect"}})
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::Forbidden.rpc_code()
    );
    let mut session = ConnectionSession::default();
    let response = server::dispatch(
        &state,
        &mut session,
        RpcRequest {
            id: Some(json!(1)),
            method: "read_doc".into(),
            params: json!({"name":"private", "token":key}),
            is_notification: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        response.into_result().unwrap_err().code,
        ErrorCode::Forbidden.rpc_code()
    );
    assert_eq!(
        call_remote(&state, &key, "wait_message", json!({"timeout_s":0}))
            .await
            .unwrap()["messages"],
        json!([])
    );
}

#[tokio::test]
async fn enrollment_budget_is_atomic_and_retry_does_not_extend_or_duplicate() {
    let root = common::TempDir::new("network-enrollment-budget");
    let device = new_token().unwrap();
    let mut policy = policy(&device);
    policy.devices[0].max_instances = 1;
    let state = state(&root, policy);
    let first_key = new_token().unwrap();
    let second_key = new_token().unwrap();
    let network = state.network().unwrap();
    let (first, second) = tokio::join!(
        network.enroll(&state, &device, enrollment(&first_key)),
        network.enroll(&state, &device, enrollment(&second_key)),
    );
    let (accepted, key, rejected) = match (first, second) {
        (Ok(accepted), Err(error)) => (accepted, first_key, error),
        (Err(error), Ok(accepted)) => (accepted, second_key, error),
        _ => panic!("exactly one identity must fit the principal budget"),
    };
    assert_eq!(code(rejected), Some(ErrorCode::ResourceExhausted));
    let before = std::fs::read(root.path().join("registry.json")).unwrap();
    let retry = enroll(&state, &device, &key).await;
    assert_eq!(accepted["agent_id"], retry["agent_id"]);
    assert_eq!(
        accepted["credential_expires_at_ms"],
        retry["credential_expires_at_ms"]
    );
    assert_eq!(retry["resumed"], true);
    assert_eq!(
        before,
        std::fs::read(root.path().join("registry.json")).unwrap()
    );
    state.retire_agent(&key).await.unwrap();
    assert_eq!(
        code(
            network
                .enroll(&state, &device, enrollment(&new_token().unwrap()))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::ResourceExhausted)
    );
}

#[tokio::test]
async fn generation_changes_and_readdition_fence_existing_contexts() {
    let root = common::TempDir::new("network-generation");
    let device = new_token().unwrap();
    let initial = policy(&device);
    let state = state(&root, initial.clone());
    let network = state.network().unwrap();
    let key = new_token().unwrap();
    enroll(&state, &device, &key).await;
    let context = network.authenticate(&state, &key).await.unwrap();
    let mut bumped = initial.clone();
    bumped.devices[0].generation = 2;
    policy_approval::replace_network_policy(&state, bumped.clone())
        .await
        .unwrap();
    assert_eq!(
        code(network.authenticate(&state, &key).await.unwrap_err()),
        Some(ErrorCode::Unauthenticated)
    );
    for method in ["ping", "list_docs"] {
        assert_eq!(
            code(
                network
                    .authorize(&state, &context, method, &json!({}))
                    .await
                    .unwrap_err()
            ),
            Some(ErrorCode::Unauthenticated)
        );
    }
    assert_eq!(
        code(
            network
                .enroll(&state, &device, enrollment(&key))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::Unauthenticated)
    );
    assert!(matches!(
        policy_approval::replace_network_policy(&state, initial).await,
        Err(BrokerError::Invalid(_))
    ));
    let mut removed = bumped.clone();
    removed.devices.clear();
    policy_approval::replace_network_policy(&state, removed)
        .await
        .unwrap();
    assert!(matches!(
        policy_approval::replace_network_policy(&state, bumped.clone()).await,
        Err(BrokerError::Invalid(_))
    ));
    bumped.devices[0].generation = 3;
    policy_approval::replace_network_policy(&state, bumped)
        .await
        .unwrap();
    assert_eq!(
        code(network.authenticate(&state, &key).await.unwrap_err()),
        Some(ErrorCode::Unauthenticated)
    );
    enroll(&state, &device, &new_token().unwrap()).await;
}

#[tokio::test]
async fn principal_removal_and_generation_floor_survive_daemon_reopen() {
    let root = common::TempDir::new("network-generation-reopen");
    let device = new_token().unwrap();
    let mut initial = policy(&device);
    initial.devices[0].generation = 7;
    let key = new_token().unwrap();
    {
        let state = state(&root, initial.clone());
        enroll(&state, &device, &key).await;
        let mut removed = initial.clone();
        removed.devices.clear();
        policy_approval::replace_network_policy(&state, removed)
            .await
            .unwrap();
    }
    for generation in [6, 7] {
        let mut config = DaemonConfig::new(root.path().to_path_buf());
        let mut readded = initial.clone();
        readded.devices[0].generation = generation;
        config.network_policy = Some(readded);
        let before = std::fs::read(root.path().join("registry.json")).unwrap();
        assert!(matches!(
            DaemonState::with_config(config),
            Err(BrokerError::Invalid(_))
        ));
        assert_eq!(
            before,
            std::fs::read(root.path().join("registry.json")).unwrap()
        );
    }
    initial.devices[0].generation = 8;
    initial.revision = 3;
    let reopened = state(&root, initial);
    let network = reopened.network().unwrap();
    assert_eq!(
        code(network.authenticate(&reopened, &key).await.unwrap_err()),
        Some(ErrorCode::Unauthenticated)
    );
    enroll(&reopened, &device, &new_token().unwrap()).await;
}

#[tokio::test]
async fn failed_policy_persistence_fences_runtime_until_reopen() {
    let root = common::TempDir::new("network-policy-failure");
    let device = new_token().unwrap();
    let initial = policy(&device);
    let key = new_token().unwrap();
    {
        let state = state(&root, initial.clone());
        enroll(&state, &device, &key).await;
        let network = state.network().unwrap();
        let changes = network.policy_changes();
        let registry_path = root.path().join("registry.json");
        let retained_path = root.path().join("registry.before-failure.json");
        std::fs::rename(&registry_path, &retained_path).unwrap();
        std::fs::create_dir(&registry_path).unwrap();
        let mut replacement = initial.clone();
        replacement.devices[0].generation += 1;
        assert!(matches!(
            policy_approval::replace_network_policy(&state, replacement).await,
            Err(BrokerError::Io(_))
        ));
        assert!(changes.has_changed().unwrap());
        assert_eq!(
            code(network.authenticate(&state, &key).await.unwrap_err()),
            Some(ErrorCode::Unauthenticated)
        );
        assert_eq!(
            code(network.request_permit("device-a", false).err().unwrap()),
            Some(ErrorCode::Unauthenticated)
        );
        std::fs::remove_dir(&registry_path).unwrap();
        std::fs::rename(&retained_path, &registry_path).unwrap();
        assert_eq!(
            code(network.authenticate(&state, &key).await.unwrap_err()),
            Some(ErrorCode::Unauthenticated)
        );
    }
    // This failure occurred before replacement, so reopening recovers the
    // original durable authority. The failed in-memory process never resumes.
    let reopened = state(&root, initial);
    assert!(reopened
        .network()
        .unwrap()
        .authenticate(&reopened, &key)
        .await
        .is_ok());
}

#[tokio::test]
async fn launch_retry_cannot_transfer_principal_or_change_metadata() {
    let root = common::TempDir::new("network-enrollment-principal");
    let device = new_token().unwrap();
    let other_token = new_token().unwrap();
    let mut initial = policy(&device);
    let mut other = initial.devices[0].clone();
    other.principal_id = "device-b".into();
    other.token_hash = token_hash(&other_token);
    initial.devices.push(other);
    let state = state(&root, initial);
    let key = new_token().unwrap();
    enroll(&state, &device, &key).await;
    let before = std::fs::read(root.path().join("registry.json")).unwrap();
    let network = state.network().unwrap();
    for credential in [&device, &other_token] {
        assert!(matches!(
            network
                .enroll(&state, &device, enrollment(credential))
                .await,
            Err(BrokerError::Invalid(_))
        ));
    }
    assert_eq!(
        code(
            network
                .enroll(&state, &other_token, enrollment(&key))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::Unauthenticated)
    );
    let mut changed = enrollment(&key);
    changed["display_name"] = json!("different");
    assert_eq!(
        code(network.enroll(&state, &device, changed).await.unwrap_err()),
        Some(ErrorCode::Conflict)
    );
    assert_eq!(
        before,
        std::fs::read(root.path().join("registry.json")).unwrap()
    );
}

#[tokio::test]
async fn poll_capacity_preserves_request_and_global_capacity_across_policy_replacement() {
    let root = common::TempDir::new("network-capacity");
    let device = new_token().unwrap();
    let mut initial = policy(&device);
    initial.max_connections = 2;
    let mut other = initial.devices[0].clone();
    other.principal_id = "device-b".into();
    other.token_hash = token_hash(&new_token().unwrap());
    initial.devices.push(other);
    let state = state(&root, initial.clone());
    let network = state.network().unwrap();
    let poll = network.request_permit("device-a", true).unwrap();
    assert_eq!(
        code(network.request_permit("device-a", true).err().unwrap()),
        Some(ErrorCode::ResourceExhausted)
    );
    assert_eq!(
        code(network.request_permit("device-b", true).err().unwrap()),
        Some(ErrorCode::ResourceExhausted)
    );
    let request = network.request_permit("device-a", false).unwrap();
    let reserved = network.request_permit("device-b", false).unwrap();
    let mut removed = initial.clone();
    removed.devices.remove(0);
    policy_approval::replace_network_policy(&state, removed)
        .await
        .unwrap();
    assert_eq!(
        code(network.request_permit("device-a", false).err().unwrap()),
        Some(ErrorCode::Unauthenticated)
    );
    initial.devices[0].generation += 1;
    policy_approval::replace_network_policy(&state, initial.clone())
        .await
        .unwrap();
    assert_eq!(
        code(network.request_permit("device-a", true).err().unwrap()),
        Some(ErrorCode::ResourceExhausted)
    );
    let final_slot = network.request_permit("device-a", false).unwrap();
    assert_eq!(
        code(network.request_permit("device-a", false).err().unwrap()),
        Some(ErrorCode::ResourceExhausted)
    );
    drop((poll, request, reserved, final_slot));
    assert!(network.request_permit("device-a", true).is_ok());
    initial.requests_per_principal += 1;
    assert!(matches!(
        policy_approval::replace_network_policy(&state, initial).await,
        Err(BrokerError::Invalid(_))
    ));
}

#[tokio::test]
async fn revoking_permission_cancels_an_already_admitted_wait() {
    let root = common::TempDir::new("network-wait-revocation");
    let device = new_token().unwrap();
    let state = state(&root, policy(&device));
    let key = new_token().unwrap();
    enroll(&state, &device, &key).await;
    let waiter = {
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            call_remote(&state, &key, "wait_message", json!({"timeout_s":25})).await
        })
    };
    await_poll_admission(&state).await;
    let mut revoked = state.network().unwrap().policy();
    revoked.devices[0]
        .permissions
        .retain(|p| p.method != "wait_message");
    policy_approval::replace_network_policy(&state, revoked)
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("revocation cancels wait before timeout")
        .unwrap();
    assert_eq!(result.unwrap_err().code, ErrorCode::Forbidden.rpc_code());
}

#[tokio::test]
async fn credential_expiry_cancels_wait_and_retry_never_renews_it() {
    let root = common::TempDir::new("network-wait-expiry");
    let device = new_token().unwrap();
    let mut initial = policy(&device);
    initial.devices[0].instance_ttl_s = 1;
    let state = state(&root, initial);
    let key = new_token().unwrap();
    let issued = enroll(&state, &device, &key).await;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        call_remote(&state, &key, "wait_message", json!({"timeout_s":25})),
    )
    .await
    .expect("expiry cancels wait");
    assert_eq!(
        result.unwrap_err().code,
        ErrorCode::Unauthenticated.rpc_code()
    );
    let network = state.network().unwrap();
    assert_eq!(
        code(
            network
                .enroll(&state, &device, enrollment(&key))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::Unauthenticated)
    );
    let stored: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("registry.json")).unwrap()).unwrap();
    let stored_grant = &stored["agents"][issued["name"].as_str().unwrap()]["network_grant"];
    assert_eq!(
        stored_grant["expires_at_ms"],
        issued["credential_expires_at_ms"]
    );
}

#[tokio::test]
async fn retirement_generation_and_principal_expiry_cancel_admitted_waits() {
    for action in ["retire", "generation", "principal-expiry"] {
        let root = common::TempDir::new("network-wait-identity-fencing");
        let device = new_token().unwrap();
        let state = state(&root, policy(&device));
        let key = new_token().unwrap();
        enroll(&state, &device, &key).await;
        let waiter = {
            let state = Arc::clone(&state);
            let key = key.clone();
            tokio::spawn(async move {
                call_remote(&state, &key, "wait_message", json!({"timeout_s":25})).await
            })
        };
        await_poll_admission(&state).await;
        if action == "retire" {
            state.retire_agent(&key).await.unwrap();
        } else {
            let mut replacement = state.network().unwrap().policy();
            if action == "generation" {
                replacement.devices[0].generation += 1;
            } else {
                replacement.devices[0].expires_at_ms = cooperators::id::unix_millis();
            }
            policy_approval::replace_network_policy(&state, replacement)
                .await
                .unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("identity invalidation cancels wait")
            .unwrap();
        assert_eq!(
            result.unwrap_err().code,
            ErrorCode::Unauthenticated.rpc_code(),
            "{action}"
        );
    }
}
