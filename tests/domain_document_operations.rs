mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cooperators::docs::{DocEvent, INLINE_BODY_LIMIT};
use cooperators::envelope::{Body, Envelope};
use cooperators::error::ErrorCode;
use cooperators::network::{call_remote, DevicePrincipal, Permission};
use cooperators::operations::OperationRef;
use cooperators::registry::{new_token, token_hash, AgentIdentity};
use cooperators::rpc::{RpcError, RpcRequest};
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use cooperators::storage::LogFile;
use serde_json::{json, Value};

fn config(root: &Path, device: &str) -> DaemonConfig {
    let mut config = DaemonConfig::new(root);
    config.enable_arrivals = true;
    config.log_rotation_bytes = 2048;
    config.network_policy = Some(
        serde_json::from_value(json!({
            "v":1,"enable_operations":true,"enable_resource_operations":true,
            "allowed_hosts":["bus.example:443"],"devices":[]
        }))
        .unwrap(),
    );
    config
        .network_policy
        .as_mut()
        .unwrap()
        .devices
        .push(DevicePrincipal {
            principal_id: "device-a".into(),
            generation: 1,
            token_hash: token_hash(device),
            expires_at_ms: cooperators::id::unix_millis() + 3_600_000,
            allow_enroll: true,
            max_instances: 8,
            instance_ttl_s: 3600,
            permissions: [
                "publish",
                "read_channel",
                "publish_doc",
                "update_doc",
                "claim_doc",
                "renew_lease",
                "release_doc",
                "read_doc",
                "operation_status",
            ]
            .into_iter()
            .map(|method| Permission {
                method: method.into(),
                resources: vec!["*".into()],
            })
            .collect(),
        });
    config
}

async fn member_with_device(state: &Arc<DaemonState>, device: &str) -> (String, AgentIdentity) {
    let token = new_token().unwrap();
    state
        .network()
        .unwrap()
        .enroll(
            state,
            device,
            json!({
                "launch_key":token, "kind":"test", "display_name":"writer"
            }),
        )
        .await
        .unwrap();
    let identity = state.authenticate(&token).await.unwrap();
    (token, identity)
}

fn operation(state: &DaemonState) -> OperationRef {
    OperationRef {
        v: 1,
        id: cooperators::id::new_ulid().unwrap(),
        epoch: state.network().unwrap().meta.operation_epoch.clone(),
        not_after_ms: cooperators::id::unix_millis() + 60_000,
    }
}

async fn remote(
    state: &Arc<DaemonState>,
    token: &str,
    method: &str,
    mut input: Value,
    operation: &OperationRef,
) -> Result<Value, RpcError> {
    input["operation"] = serde_json::to_value(operation).unwrap();
    call_remote(state, token, method, input).await
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

fn documents(root: &Path) -> Vec<DocEvent> {
    LogFile::<DocEvent>::read_segments(root.join("docs/plan.jsonl"))
        .unwrap()
        .records
}

#[tokio::test]
async fn document_receipts_survive_new_versions_new_claims_and_restart_with_blob_and_arrival() {
    let root = common::TempDir::new("domain-document-receipts");
    let device = new_token().unwrap();
    let state = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    let (token, author) = member_with_device(&state, &device).await;
    let reader = local(
        &state,
        "register_agent",
        json!({"name":"reader","kind":"test"}),
    )
    .await;
    let subscription = local(
        &state,
        "subscribe_arrivals",
        json!({
            "token":reader["token"],"client_key":"plan-wake",
            "resource":{"type":"document","name":"plan"},
            "event_kinds":["doc.published","doc.updated"],"catch_up":false,
        }),
    )
    .await;

    let body = "b".repeat(INLINE_BODY_LIMIT + 17);
    let publish_ref = operation(&state);
    let publish_input = json!({"name":"plan","title":"Original","text":body});
    let published = remote(
        &state,
        &token,
        "publish_doc",
        publish_input.clone(),
        &publish_ref,
    )
    .await
    .unwrap();
    assert_eq!(published["storage"], "blob");
    let source = documents(root.path());
    assert!(source[0].domain_operation.is_some());
    let routes = &source[0].notification_intent.as_ref().unwrap().routes;
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].agent_id, reader["agent_id"]);
    assert_eq!(
        routes[0].subscription_ids,
        vec![subscription["subscription_id"].as_str().unwrap()]
    );
    let blob = source[0].blob.as_ref().unwrap();
    assert_eq!(
        std::fs::read_to_string(root.path().join("blobs/sha256").join(&blob.sha256)).unwrap(),
        body
    );

    let update_ref = operation(&state);
    let update_input = json!({"name":"plan","base_version":1,"text":"version two"});
    let updated = remote(
        &state,
        &token,
        "update_doc",
        update_input.clone(),
        &update_ref,
    )
    .await
    .unwrap();
    state
        .update_doc(
            &author,
            "plan",
            2,
            Body::new("text/plain", "version three"),
            Some("Changed".into()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        remote(
            &state,
            &token,
            "publish_doc",
            publish_input.clone(),
            &publish_ref
        )
        .await
        .unwrap(),
        published
    );
    assert_eq!(
        remote(
            &state,
            &token,
            "update_doc",
            update_input.clone(),
            &update_ref
        )
        .await
        .unwrap(),
        updated
    );

    let claim_ref = operation(&state);
    let claim_input = json!({"name":"plan","lease_ttl_s":60});
    let claim = remote(&state, &token, "claim_doc", claim_input.clone(), &claim_ref)
        .await
        .unwrap();
    let renew_ref = operation(&state);
    let renew_input = json!({"lease_id":claim["lease_id"]});
    let renewed = remote(
        &state,
        &token,
        "renew_lease",
        renew_input.clone(),
        &renew_ref,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(
        remote(
            &state,
            &token,
            "renew_lease",
            renew_input.clone(),
            &renew_ref
        )
        .await
        .unwrap(),
        renewed
    );
    let view = state.read_doc("plan", None).await.unwrap();
    assert_eq!(view.claim.unwrap().expires_at, renewed["expires_at"]);
    let release_ref = operation(&state);
    let release_input = json!({"lease_id":claim["lease_id"],"fence":claim["fence"]});
    let released = remote(
        &state,
        &token,
        "release_doc",
        release_input.clone(),
        &release_ref,
    )
    .await
    .unwrap();
    let next_claim = state.claim_doc(&author, "plan", 120).await.unwrap();
    assert_ne!(next_claim.lease_id, claim["lease_id"]);
    let count = documents(root.path()).len();
    assert_eq!(count, 7);
    drop(state);

    let reopened = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    for (method, input, reference, expected) in [
        ("publish_doc", publish_input, publish_ref, published),
        ("update_doc", update_input, update_ref, updated),
        ("claim_doc", claim_input, claim_ref, claim),
        ("renew_lease", renew_input, renew_ref, renewed),
        ("release_doc", release_input, release_ref, released),
    ] {
        assert_eq!(
            remote(&reopened, &token, method, input, &reference)
                .await
                .unwrap(),
            expected,
            "{method}"
        );
        let status = remote(&reopened, &token, "operation_status", json!({}), &reference)
            .await
            .unwrap();
        assert_eq!(status, json!({"state":"committed","result":expected}));
    }
    assert_eq!(documents(root.path()).len(), count);
    let view = reopened.read_doc("plan", Some(1)).await.unwrap();
    assert_eq!(view.body.text, body);
    assert_eq!(view.head_version, 3);
    assert_eq!(view.claim.unwrap().fence, next_claim.fence);
}

#[tokio::test]
async fn cas_no_effect_error_stays_original_when_later_state_would_accept_it() {
    let root = common::TempDir::new("domain-document-cas");
    let device = new_token().unwrap();
    let state = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    let (token, author) = member_with_device(&state, &device).await;
    state
        .publish_doc(
            &author,
            "plan",
            "text/plain",
            Body::new("text/plain", "one"),
            None,
        )
        .await
        .unwrap();
    let reference = operation(&state);
    let input = json!({"name":"plan","base_version":2,"text":"operation three"});
    let original = remote(&state, &token, "update_doc", input.clone(), &reference)
        .await
        .unwrap_err();
    assert_eq!(
        ErrorCode::from_rpc_code(original.code),
        Some(ErrorCode::VersionConflict)
    );
    assert_eq!(original.data.as_ref().unwrap()["head_version"], 1);
    state
        .update_doc(
            &author,
            "plan",
            1,
            Body::new("text/plain", "two"),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        remote(&state, &token, "update_doc", input.clone(), &reference)
            .await
            .unwrap_err(),
        original
    );
    assert_eq!(documents(root.path()).len(), 2);
    drop(state);
    let reopened = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    assert_eq!(
        remote(&reopened, &token, "update_doc", input.clone(), &reference)
            .await
            .unwrap_err(),
        original
    );
    let status = remote(&reopened, &token, "operation_status", json!({}), &reference)
        .await
        .unwrap();
    assert_eq!(status, json!({"state":"committed","error":original}));
    let fresh = operation(&reopened);
    assert_eq!(
        remote(&reopened, &token, "update_doc", input, &fresh)
            .await
            .unwrap()["version"],
        3
    );
    assert_eq!(documents(root.path()).len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_channel_duplicates_have_one_effect_and_recover_exact_receipt() {
    let root = common::TempDir::new("domain-channel-receipts");
    let device = new_token().unwrap();
    let state = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    let (token, author) = member_with_device(&state, &device).await;
    let reference = operation(&state);
    let input = json!({"channel":"build","text":"one publication"});
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let (state, token, input, reference) = (
            state.clone(),
            token.clone(),
            input.clone(),
            reference.clone(),
        );
        tasks.push(tokio::spawn(async move {
            remote(&state, &token, "publish", input, &reference)
                .await
                .unwrap()
        }));
    }
    let original = tasks.remove(0).await.unwrap();
    for task in tasks {
        assert_eq!(task.await.unwrap(), original);
    }
    assert_eq!(original["duplicate"], false);
    state
        .publish(
            &author,
            "build",
            Body::new("text/plain", "later publication"),
            None,
        )
        .await
        .unwrap();
    let path = root.path().join("channels/build.jsonl");
    assert_eq!(
        LogFile::<Envelope>::read_segments(&path)
            .unwrap()
            .records
            .len(),
        2
    );
    drop(state);
    let reopened = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    assert_eq!(
        remote(&reopened, &token, "publish", input, &reference)
            .await
            .unwrap(),
        original
    );
    assert_eq!(
        LogFile::<Envelope>::read_segments(path)
            .unwrap()
            .records
            .len(),
        2
    );
}

#[tokio::test]
async fn recovery_rejects_a_receipt_not_bound_to_its_document_event() {
    let root = common::TempDir::new("domain-document-proof-binding");
    let device = new_token().unwrap();
    let state = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    let (token, _) = member_with_device(&state, &device).await;
    remote(
        &state,
        &token,
        "publish_doc",
        json!({"name":"plan","text":"one"}),
        &operation(&state),
    )
    .await
    .unwrap();
    drop(state);
    let path = root.path().join("docs/plan.jsonl");
    let mut event: Value =
        serde_json::from_str(std::fs::read_to_string(&path).unwrap().trim()).unwrap();
    // A valid final record has no next hash link. Semantic source validation
    // must therefore reject this forged receipt independently of hash replay.
    event["domain_operation"]["outcome"]["value"]["seq"] = json!(999);
    std::fs::write(
        path,
        format!("{}\n", serde_json::to_string(&event).unwrap()),
    )
    .unwrap();
    let error = DaemonState::with_config(config(root.path(), &device)).unwrap_err();
    assert!(
        error.to_string().contains("document operation proof"),
        "{error}"
    );
}

#[tokio::test]
async fn incidental_claim_expiry_never_becomes_a_terminal_no_effect_receipt() {
    let root = common::TempDir::new("domain-document-expiry");
    let device = new_token().unwrap();
    let state = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    let (token, author) = member_with_device(&state, &device).await;
    state
        .publish_doc(
            &author,
            "plan",
            "text/plain",
            Body::new("text/plain", "one"),
            None,
        )
        .await
        .unwrap();
    let claim = state.claim_doc(&author, "plan", 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1010)).await;
    let reference = operation(&state);
    let input = json!({"name":"plan","base_version":1,"text":"two","lease_id":claim.lease_id,"fence":claim.fence});
    let error = remote(&state, &token, "update_doc", input.clone(), &reference)
        .await
        .unwrap_err();
    assert_eq!(
        ErrorCode::from_rpc_code(error.code),
        Some(ErrorCode::OutcomeUnknown)
    );
    assert_eq!(
        remote(&state, &token, "operation_status", json!({}), &reference)
            .await
            .unwrap(),
        json!({"state":"unknown"})
    );
    assert_eq!(
        remote(&state, &token, "update_doc", input.clone(), &reference)
            .await
            .unwrap_err(),
        error
    );
    let events = documents(root.path());
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "doc_claim_expired")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "doc_updated")
            .count(),
        0
    );
    drop(state);
    let reopened = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    // Reopen proves the primary update never committed. The retried identity
    // can now acquire a terminal lease-lost receipt without an append attempt.
    let error = remote(&reopened, &token, "update_doc", input, &reference)
        .await
        .unwrap_err();
    assert_eq!(
        ErrorCode::from_rpc_code(error.code),
        Some(ErrorCode::LeaseLost)
    );
    assert_eq!(documents(root.path()).len(), events.len());
}

#[cfg(feature = "mbt-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ambiguous_document_and_channel_appends_recover_without_a_second_effect() {
    use cooperators::faults::{arm_append, AppendFault};
    for method in ["publish_doc", "publish"] {
        for fault in [
            AppendFault::FailSync,
            AppendFault::PartialWrite { bytes: 40 },
        ] {
            let root = common::TempDir::new("domain-document-channel-io");
            let device = new_token().unwrap();
            let state = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
            let (token, _) = member_with_device(&state, &device).await;
            let (path, input) = if method == "publish_doc" {
                (
                    root.path().join("docs/plan.jsonl"),
                    json!({"name":"plan","text":"one"}),
                )
            } else {
                (
                    root.path().join("channels/build.jsonl"),
                    json!({"channel":"build","text":"one"}),
                )
            };
            let reference = operation(&state);
            let injection = arm_append(&path, fault).unwrap();
            let error = remote(&state, &token, method, input.clone(), &reference)
                .await
                .unwrap_err();
            assert_eq!(
                ErrorCode::from_rpc_code(error.code),
                Some(ErrorCode::OutcomeUnknown)
            );
            assert_eq!(
                remote(&state, &token, method, input.clone(), &reference)
                    .await
                    .unwrap_err(),
                error
            );
            injection.wait_reached(Duration::from_secs(1)).unwrap();
            drop(injection);
            drop(state);
            let reopened =
                Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
            let original = remote(&reopened, &token, method, input.clone(), &reference)
                .await
                .unwrap();
            assert_eq!(
                remote(&reopened, &token, method, input, &reference)
                    .await
                    .unwrap(),
                original
            );
            if method == "publish_doc" {
                let effects: Vec<_> = documents(root.path())
                    .into_iter()
                    .filter(|e| e.kind == "doc_published")
                    .collect();
                assert_eq!(effects.len(), 1);
                assert_eq!(
                    effects[0].domain_operation.as_ref().unwrap().outcome,
                    cooperators::operations::OperationOutcome::Success(original)
                );
            } else {
                let effects: Vec<_> = LogFile::<Envelope>::read_segments(&path)
                    .unwrap()
                    .records
                    .into_iter()
                    .filter(|e| e.kind == "publish")
                    .collect();
                assert_eq!(effects.len(), 1);
                assert_eq!(
                    effects[0].domain_operation.as_ref().unwrap().outcome,
                    cooperators::operations::OperationOutcome::Success(original)
                );
            }
        }
    }
}

#[cfg(feature = "mbt-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_while_document_sync_is_blocked_does_not_cancel_its_owner() {
    use cooperators::faults::{arm_append, AppendFault, SyncOutcome};
    let root = common::TempDir::new("domain-document-cancellation");
    let device = new_token().unwrap();
    let state = Arc::new(DaemonState::with_config(config(root.path(), &device)).unwrap());
    let (token, _) = member_with_device(&state, &device).await;
    let reference = operation(&state);
    let input = json!({"name":"plan","text":"one"});
    let injection = arm_append(
        root.path().join("docs/plan.jsonl"),
        AppendFault::PauseBeforeSync,
    )
    .unwrap();
    let caller = {
        let (state, token, reference, input) = (
            state.clone(),
            token.clone(),
            reference.clone(),
            input.clone(),
        );
        tokio::spawn(async move { remote(&state, &token, "publish_doc", input, &reference).await })
    };
    injection.wait_reached(Duration::from_secs(3)).unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    injection.release(SyncOutcome::Succeed);
    let receipt = tokio::time::timeout(
        Duration::from_secs(3),
        remote(&state, &token, "publish_doc", input, &reference),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(documents(root.path()).len(), 1);
    assert_eq!(receipt["version"], 1);
}
