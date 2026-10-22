mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cooperators::envelope::{Body, Envelope};
use cooperators::error::{BrokerError, ErrorCode};
use cooperators::messaging::SendRequest;
use cooperators::operations::{OperationRef, OperationScope};
use cooperators::registry::AgentIdentity;
use cooperators::server::{DaemonConfig, DaemonState};
use cooperators::storage::LogFile;
use serde_json::json;

fn config(root: &Path) -> DaemonConfig {
    let mut config = DaemonConfig::new(root);
    config.network_policy = Some(
        serde_json::from_value(json!({
            "v":1,"enable_operations":true,"allowed_hosts":["bus.example:443"],"devices":[]
        }))
        .unwrap(),
    );
    config.write_batch_delay = Duration::from_millis(10);
    config
}

async fn identity(state: &DaemonState, name: &str) -> AgentIdentity {
    let registration = state
        .register_agent(name.into(), Some("test".into()), None, None)
        .await
        .unwrap();
    state.authenticate(&registration.token).await.unwrap()
}

fn operation(state: &DaemonState, sender: &AgentIdentity) -> (OperationScope, OperationRef) {
    let meta = &state.network().unwrap().meta;
    (
        OperationScope {
            bus_id: meta.bus_id.clone(),
            principal_id: "device-a".into(),
            agent_id: sender.agent_id.clone(),
        },
        OperationRef {
            v: 1,
            id: cooperators::id::new_ulid().unwrap(),
            epoch: meta.operation_epoch.clone(),
            not_after_ms: cooperators::id::unix_millis() + 60_000,
        },
    )
}

fn request(to: &str) -> SendRequest {
    SendRequest {
        to: to.into(),
        kind: "request".into(),
        body: Body::new("text/plain", "one intent"),
        reply_to: None,
        idempotency_key: None,
        ttl: None,
    }
}

fn inbox_path(root: &Path, recipient: &AgentIdentity) -> std::path::PathBuf {
    root.join("agents")
        .join(&recipient.agent_id)
        .join("inbox.jsonl")
}

fn records(root: &Path, recipient: &AgentIdentity) -> Vec<Envelope> {
    let report = LogFile::<Envelope>::read_segments(inbox_path(root, recipient)).unwrap();
    assert_eq!(report.truncated_bytes, 0);
    report.records
}

fn code(error: BrokerError) -> Option<ErrorCode> {
    ErrorCode::from_rpc_code(error.into_rpc_error().code)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn largest_admitted_remote_envelope_is_readable_and_next_byte_has_no_effect() {
    use cooperators::network::{call_remote, DevicePrincipal, Permission};
    use cooperators::registry::{new_token, token_hash};

    let root = common::TempDir::new("operation-page-boundary");
    let device = new_token().unwrap();
    let mut config = config(root.path());
    config
        .network_policy
        .as_mut()
        .unwrap()
        .devices
        .push(DevicePrincipal {
            principal_id: "device-a".into(),
            generation: 1,
            token_hash: token_hash(&device),
            expires_at_ms: cooperators::id::unix_millis() + 60_000,
            allow_enroll: true,
            max_instances: 2,
            instance_ttl_s: 60,
            permissions: ["send_message", "wait_message"]
                .into_iter()
                .map(|method| Permission {
                    method: method.into(),
                    resources: vec!["*".into()],
                })
                .collect(),
        });
    let state = Arc::new(DaemonState::with_config(config).unwrap());
    let mut members = Vec::new();
    for name in ["sender", "recipient"] {
        let token = new_token().unwrap();
        state
            .network()
            .unwrap()
            .enroll(
                &state,
                &device,
                json!({
                    "launch_key":token, "kind":"test", "display_name":name,
                }),
            )
            .await
            .unwrap();
        members.push((state.authenticate(&token).await.unwrap(), token));
    }
    let (sender, sender_token) = &members[0];
    let (recipient, recipient_token) = &members[1];
    let (scope, reference) = operation(&state, sender);
    let mut seed = request(&recipient.name);
    seed.ttl = Some(Duration::from_secs(60));
    let seed_receipt = state
        .send_operation(sender, scope, reference, seed)
        .await
        .unwrap();

    // Include all actual identity/proof metadata, then maximize the fields
    // assigned at commit. A one-byte ASCII body increment is one encoded byte.
    let mut worst_case = records(root.path(), recipient).remove(0);
    worst_case.body.text.clear();
    worst_case.seq = u64::MAX;
    let proof = worst_case.operation.as_mut().unwrap();
    proof.result.seq = u64::MAX;
    proof.expires_at_ms = Some(u64::MAX);
    let overhead = serde_json::to_vec(&worst_case).unwrap().len() + 1;
    let payload_bytes = cooperators::messaging::REMOTE_INBOX_PAGE_BYTES - overhead;
    let payload = "x".repeat(payload_bytes);
    let (scope, reference) = operation(&state, sender);
    let params = json!({"to":recipient.name,"kind":"request","body":{"mime":"text/plain","text":payload},"ttl_s":60,"operation":reference});
    let receipt = call_remote(&state, sender_token, "send_message", params)
        .await
        .unwrap();
    assert_eq!(records(root.path(), recipient).len(), 2);
    let page = call_remote(
        &state,
        recipient_token,
        "wait_message",
        json!({"cursor":seed_receipt.seq.to_string(),"timeout_s":0}),
    )
    .await
    .unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 1);
    assert_eq!(page["messages"][0]["id"], receipt["message_id"]);
    assert_eq!(
        page["messages"][0]["body"]["text"].as_str().unwrap().len(),
        payload_bytes
    );
    assert!(serde_json::to_vec(&page).unwrap().len() < cooperators::transport::MAX_RPC_BYTES);

    let (_, oversized_ref) = operation(&state, sender);
    let mut oversized = request(&recipient.name);
    oversized.ttl = Some(Duration::from_secs(60));
    oversized.body.text = "x".repeat(payload_bytes + 1);
    assert_eq!(
        code(
            state
                .send_operation(sender, scope, oversized_ref.clone(), oversized.clone())
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::ResourceExhausted)
    );
    let error = call_remote(&state, sender_token, "send_message", json!({
        "to":recipient.name,"kind":"request","body":oversized.body,"ttl_s":60,"operation":oversized_ref,
    })).await.unwrap_err();
    assert_eq!(
        ErrorCode::from_rpc_code(error.code),
        Some(ErrorCode::ResourceExhausted)
    );
    assert_eq!(
        records(root.path(), recipient).len(),
        2,
        "neither oversized call appends an authoritative effect"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicates_and_restart_return_identical_source_receipt() {
    let root = common::TempDir::new("operation-duplicates");
    let state = Arc::new(DaemonState::with_config(config(root.path())).unwrap());
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let (scope, reference) = operation(&state, &sender);
    let barrier = Arc::new(tokio::sync::Barrier::new(25));
    let mut tasks = Vec::new();
    for _ in 0..24 {
        let (state, sender, recipient, scope, reference, barrier) = (
            state.clone(),
            sender.clone(),
            recipient.clone(),
            scope.clone(),
            reference.clone(),
            barrier.clone(),
        );
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            state
                .send_operation(&sender, scope, reference, request(&recipient.name))
                .await
                .unwrap()
        }));
    }
    barrier.wait().await;
    let original = tasks.remove(0).await.unwrap();
    for task in tasks {
        assert_eq!(original, task.await.unwrap());
    }
    assert!(!original.duplicate);
    let source = records(root.path(), &recipient);
    assert_eq!(source.len(), 1);
    assert_eq!(source[0].operation.as_ref().unwrap().result, original);
    assert_eq!(original.ts.as_ref(), Some(&source[0].ts));
    drop(state);
    let reopened = DaemonState::with_config(config(root.path())).unwrap();
    let replay = reopened
        .send_operation(&sender, scope, reference, request(&recipient.agent_id))
        .await
        .unwrap();
    assert_eq!(
        original, replay,
        "name and stable id aliases share an immutable receipt"
    );
    assert_eq!(records(root.path(), &recipient).len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_scope_conflicts_do_not_create_effects_in_other_recipients() {
    let root = common::TempDir::new("operation-conflicts");
    let state = DaemonState::with_config(config(root.path())).unwrap();
    let sender = identity(&state, "sender").await;
    let a = identity(&state, "a").await;
    let b = identity(&state, "b").await;
    let (scope, reference) = operation(&state, &sender);
    state
        .send_operation(&sender, scope.clone(), reference.clone(), request(&a.name))
        .await
        .unwrap();
    let mut changed = Vec::new();
    changed.push(request(&b.name));
    let mut body = request(&a.name);
    body.body.text.push('!');
    changed.push(body);
    let mut mime = request(&a.name);
    mime.body.mime = "application/json".into();
    changed.push(mime);
    let mut kind = request(&a.name);
    kind.kind = "event".into();
    changed.push(kind);
    let mut reply = request(&a.name);
    reply.reply_to = Some("other".into());
    changed.push(reply);
    let mut ttl = request(&a.name);
    ttl.ttl = Some(Duration::from_secs(1));
    changed.push(ttl);
    for request in changed {
        assert_eq!(
            code(
                state
                    .send_operation(&sender, scope.clone(), reference.clone(), request)
                    .await
                    .unwrap_err()
            ),
            Some(ErrorCode::IdempotencyConflict)
        );
    }
    let mut altered = reference.clone();
    altered.not_after_ms += 1;
    assert_eq!(
        code(
            state
                .send_operation(&sender, scope.clone(), altered, request(&a.name))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::IdempotencyConflict)
    );
    let mut altered = reference.clone();
    altered.epoch = cooperators::id::new_ulid().unwrap();
    assert_eq!(
        code(
            state
                .send_operation(&sender, scope.clone(), altered, request(&a.name))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::IdempotencyConflict)
    );
    assert_eq!(records(root.path(), &a).len(), 1);
    assert!(records(root.path(), &b).is_empty());
    let mut independent_scope = scope;
    independent_scope.principal_id = "device-b".into();
    state
        .send_operation(&sender, independent_scope, reference, request(&b.name))
        .await
        .unwrap();
    assert_eq!(
        records(root.path(), &b).len(),
        1,
        "stable principal is part of scope"
    );
}

#[tokio::test]
async fn malformed_legacy_expired_and_far_future_intents_never_write() {
    let root = common::TempDir::new("operation-pre-admission");
    let state = DaemonState::with_config(config(root.path())).unwrap();
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let (scope, reference) = operation(&state, &sender);
    let mut legacy = request(&recipient.name);
    legacy.idempotency_key = Some("old".into());
    assert!(state
        .send_operation(&sender, scope.clone(), reference.clone(), legacy)
        .await
        .is_err());
    let mut expired = reference.clone();
    expired.not_after_ms = 1;
    assert_eq!(
        code(
            state
                .send_operation(&sender, scope.clone(), expired, request(&recipient.name))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::Conflict)
    );
    let mut future = reference.clone();
    future.not_after_ms = u64::MAX;
    assert!(state
        .send_operation(&sender, scope.clone(), future, request(&recipient.name))
        .await
        .is_err());
    let mut wrong_scope = scope;
    wrong_scope.agent_id = recipient.agent_id.clone();
    assert_eq!(
        code(
            state
                .send_operation(&sender, wrong_scope, reference, request(&recipient.name))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::Forbidden)
    );
    assert!(records(root.path(), &recipient).is_empty());
}

#[tokio::test]
async fn ttl_proof_matches_synced_companion_and_survives_replay() {
    let root = common::TempDir::new("operation-ttl");
    let state = DaemonState::with_config(config(root.path())).unwrap();
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let (scope, reference) = operation(&state, &sender);
    let mut intent = request(&recipient.name);
    intent.ttl = Some(Duration::from_secs(60));
    let original = state
        .send_operation(&sender, scope.clone(), reference.clone(), intent.clone())
        .await
        .unwrap();
    let source = records(root.path(), &recipient);
    let proof = source[0].operation.as_ref().unwrap();
    let expiries = LogFile::<cooperators::messaging::ExpiryRecord>::read_segments(
        inbox_path(root.path(), &recipient).with_file_name("expiry.jsonl"),
    )
    .unwrap();
    assert_eq!(expiries.records.len(), 1);
    assert_eq!(proof.ttl_ms, Some(60_000));
    assert_eq!(proof.expires_at_ms, Some(expiries.records[0].expires_at_ms));
    drop(state);
    let state = DaemonState::with_config(config(root.path())).unwrap();
    assert_eq!(
        state
            .send_operation(&sender, scope, reference, intent)
            .await
            .unwrap(),
        original
    );
    assert_eq!(records(root.path(), &recipient).len(), 1);
}

#[tokio::test]
async fn duplicate_authoritative_proofs_fail_closed_on_recovery() {
    let root = common::TempDir::new("operation-duplicate-source");
    let state = DaemonState::with_config(config(root.path())).unwrap();
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let (scope, reference) = operation(&state, &sender);
    state
        .send_operation(&sender, scope, reference, request(&recipient.name))
        .await
        .unwrap();
    drop(state);
    let (mut log, report) = LogFile::<Envelope>::open(inbox_path(root.path(), &recipient)).unwrap();
    let mut duplicate = report.records[0].clone();
    duplicate.operation.as_mut().unwrap().result.seq = log.next_seq();
    log.append(&mut duplicate).unwrap();
    drop(log);
    assert!(DaemonState::with_config(config(root.path())).is_err());
}

#[tokio::test]
async fn disabled_policy_replays_sources_but_rejects_new_durable_sends() {
    let root = common::TempDir::new("operation-disabled");
    let state = DaemonState::with_config(config(root.path())).unwrap();
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let (scope, reference) = operation(&state, &sender);
    state
        .send_operation(
            &sender,
            scope.clone(),
            reference.clone(),
            request(&recipient.name),
        )
        .await
        .unwrap();
    drop(state);
    let mut disabled = config(root.path());
    disabled.network_policy.as_mut().unwrap().enable_operations = false;
    let reopened = DaemonState::with_config(disabled).unwrap();
    assert_eq!(
        code(
            reopened
                .send_operation(&sender, scope, reference, request(&recipient.name))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::Forbidden)
    );
    assert_eq!(records(root.path(), &recipient).len(), 1);
    reopened
        .send_message(&sender, request(&recipient.name))
        .await
        .unwrap();
    assert_eq!(
        records(root.path(), &recipient).len(),
        2,
        "local unscoped API remains available"
    );
}

#[cfg(feature = "mbt-injection")]
#[tokio::test]
async fn original_receipt_survives_submission_deadline_and_reduced_capacity() {
    let root = common::TempDir::new("operation-delayed-retry");
    let state = DaemonState::with_config(config(root.path())).unwrap();
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let (scope, reference) = operation(&state, &sender);
    let original = state
        .send_operation(
            &sender,
            scope.clone(),
            reference.clone(),
            request(&recipient.name),
        )
        .await
        .unwrap();
    drop(state);
    let mut constrained = config(root.path());
    constrained.max_inbox_messages = 0;
    let state = DaemonState::with_config(constrained).unwrap();
    let clock = cooperators::faults::set_clock(root.path(), reference.not_after_ms + 1).unwrap();
    assert_eq!(
        state
            .send_operation(
                &sender,
                scope.clone(),
                reference.clone(),
                request(&recipient.name)
            )
            .await
            .unwrap(),
        original
    );
    let mut unseen = reference;
    unseen.id = cooperators::id::new_ulid().unwrap();
    assert_eq!(
        code(
            state
                .send_operation(&sender, scope, unseen, request(&recipient.name))
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::Conflict)
    );
    assert_eq!(records(root.path(), &recipient).len(), 1);
    drop(clock);
}

#[cfg(feature = "mbt-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_admitted_send_finishes_and_uncertain_sync_is_fenced_until_replay() {
    use cooperators::faults::{arm_append, AppendFault, SyncOutcome};
    let root = common::TempDir::new("operation-cancellation");
    let state = Arc::new(DaemonState::with_config(config(root.path())).unwrap());
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let (scope, reference) = operation(&state, &sender);
    let barrier = arm_append(
        inbox_path(root.path(), &recipient),
        AppendFault::PauseBeforeSync,
    )
    .unwrap();
    let (s, a, r, sc, op) = (
        state.clone(),
        sender.clone(),
        recipient.clone(),
        scope.clone(),
        reference.clone(),
    );
    let task = tokio::spawn(async move { s.send_operation(&a, sc, op, request(&r.name)).await });
    barrier.wait_reached(Duration::from_secs(5)).unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    barrier.release(SyncOutcome::Succeed);
    let original = state
        .send_operation(
            &sender,
            scope.clone(),
            reference.clone(),
            request(&recipient.name),
        )
        .await
        .unwrap();
    assert_eq!(records(root.path(), &recipient).len(), 1);
    assert_eq!(
        records(root.path(), &recipient)[0]
            .operation
            .as_ref()
            .unwrap()
            .result,
        original
    );

    let (scope, reference) = operation(&state, &sender);
    let fail = arm_append(inbox_path(root.path(), &recipient), AppendFault::FailSync).unwrap();
    assert_eq!(
        code(
            state
                .send_operation(
                    &sender,
                    scope.clone(),
                    reference.clone(),
                    request(&recipient.name)
                )
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::OutcomeUnknown)
    );
    assert_eq!(
        code(
            state
                .send_operation(
                    &sender,
                    scope.clone(),
                    reference.clone(),
                    request(&recipient.name)
                )
                .await
                .unwrap_err()
        ),
        Some(ErrorCode::OutcomeUnknown)
    );
    assert_eq!(
        records(root.path(), &recipient).len(),
        2,
        "uncertain full source record is never resubmitted"
    );
    drop(fail);
    drop(barrier);
    drop(state);
    let reopened = DaemonState::with_config(config(root.path())).unwrap();
    let recovered = reopened
        .send_operation(&sender, scope, reference, request(&recipient.name))
        .await
        .unwrap();
    assert_eq!(records(root.path(), &recipient).len(), 2);
    assert_eq!(
        records(root.path(), &recipient)[1]
            .operation
            .as_ref()
            .unwrap()
            .result,
        recovered
    );
}
