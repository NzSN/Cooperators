mod common;

use cooperators::envelope::Body;
use cooperators::error::{BrokerError, ErrorCode};
use cooperators::messaging::SendRequest;
use cooperators::registry::{new_token, token_hash, Registry};
use cooperators::rpc::RpcRequest;
use cooperators::server::DaemonState;
use cooperators::server::{self, ConnectionSession};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn enroll(registry: &mut Registry, key: &str) -> cooperators::registry::Enrollment {
    registry
        .enroll(key.into(), "codex".into(), "codex".into(), None)
        .expect("enroll launch")
}

fn conflict_identity(error: BrokerError, expected_id: &str) {
    match error {
        BrokerError::ApplicationData(ErrorCode::Conflict, _, data) => {
            assert_eq!(data["agent_id"], expected_id);
        }
        other => panic!("expected structured stable-identity conflict, got {other:?}"),
    }
}

/// This baseline documents the existing shared-configuration behavior. It is
/// not an MBT result: callers presenting one credential are one identity.
#[tokio::test]
async fn legacy_shared_credential_observes_one_identity() {
    let temp = common::TempDir::new("shared-credential-baseline");
    let state = DaemonState::new(temp.path().to_path_buf());
    let registered = state
        .register_agent("codex".into(), Some("codex".into()), None, None)
        .await
        .expect("register shared configuration");
    let first = state
        .authenticate(&registered.token)
        .await
        .expect("first client");
    let second = state
        .authenticate(&registered.token)
        .await
        .expect("second client");
    assert_eq!(first.agent_id, second.agent_id);
    let raw: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("registry.json")).expect("actual registry"),
    )
    .expect("registry JSON");
    assert_eq!(raw["agents"].as_object().expect("agent records").len(), 1);
    eprintln!(
        "baseline: two clients using one configured token authenticate as {}; registry records=1",
        first.agent_id
    );
}

#[test]
fn same_kind_launches_are_distinct_and_retry_survives_registry_reload() {
    let temp = common::TempDir::new("enroll-distinct");
    let key_a = new_token().expect("launch key A");
    let key_b = new_token().expect("launch key B");
    let mut registry = Registry::load(temp.path()).expect("registry");
    let first = enroll(&mut registry, &key_a);
    let second = enroll(&mut registry, &key_b);
    assert_ne!(first.agent_id, second.agent_id);
    assert_ne!(first.token, second.token);
    assert_ne!(first.name, second.name);
    assert_eq!(first.display_name, second.display_name);
    assert_eq!(first.kind, second.kind);
    assert!(!first.resumed);
    let original = std::fs::read(temp.path().join("registry.json")).expect("registry bytes");
    let mut reloaded = Registry::load(temp.path()).expect("reload registry");
    let retry = enroll(&mut reloaded, &key_a);
    assert_eq!(retry.agent_id, first.agent_id);
    assert_eq!(retry.name, first.name);
    assert_eq!(retry.token, key_a);
    assert!(retry.resumed);
    assert_eq!(
        original,
        std::fs::read(temp.path().join("registry.json")).unwrap()
    );
    let raw: Value = serde_json::from_slice(&original).expect("registry JSON");
    assert_eq!(raw["agents"].as_object().unwrap().len(), 2);
    assert_eq!(
        raw["agents"][&first.name]["launch_key_hash"],
        token_hash(&key_a)
    );
    assert_eq!(raw["agents"][&first.name]["display_name"], "codex");
    let text = String::from_utf8(original).expect("UTF-8 registry");
    assert!(!text.contains(&key_a));
    assert!(!text.contains(&key_b));
}

#[test]
fn enrollment_retry_with_changed_metadata_is_rejected_without_mutation() {
    let temp = common::TempDir::new("enroll-metadata");
    let mut registry = Registry::load(temp.path()).unwrap();
    let key = new_token().unwrap();
    let first = enroll(&mut registry, &key);
    let before = std::fs::read(temp.path().join("registry.json")).unwrap();
    let error = registry
        .enroll(key, "claude".into(), "codex".into(), None)
        .unwrap_err();
    conflict_identity(error, &first.agent_id);
    assert_eq!(
        before,
        std::fs::read(temp.path().join("registry.json")).unwrap()
    );
}

#[test]
fn rotated_launch_key_never_revives_and_canonical_metadata_survives() {
    let temp = common::TempDir::new("enroll-rotate");
    let mut registry = Registry::load(temp.path()).unwrap();
    let launch_key = new_token().unwrap();
    let first = enroll(&mut registry, &launch_key);
    let rotated = registry
        .register(
            first.name.clone(),
            Some(first.kind.clone()),
            None,
            Some(&first.token),
        )
        .unwrap();
    assert_eq!(rotated.agent_id, first.agent_id);
    assert!(registry.authenticate(&launch_key).is_none());
    assert_eq!(
        registry.authenticate(&rotated.token).unwrap().agent_id,
        first.agent_id
    );
    let mut reloaded = Registry::load(temp.path()).unwrap();
    conflict_identity(
        reloaded
            .enroll(launch_key.clone(), "codex".into(), "codex".into(), None)
            .unwrap_err(),
        &first.agent_id,
    );
    let metadata = reloaded.instance_metadata(&rotated.token).unwrap();
    assert_eq!(metadata.display_name, "codex");
    assert_eq!(metadata.name, first.name);
    let raw = std::fs::read_to_string(temp.path().join("registry.json")).unwrap();
    assert!(!raw.contains(&launch_key));
    assert!(!raw.contains(&rotated.token));
    let raw: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        raw["agents"][&first.name]["launch_key_hash"],
        token_hash(&launch_key)
    );
    assert!(raw["revoked_token_hashes"]
        .as_array()
        .unwrap()
        .contains(&json!(token_hash(&launch_key))));
}

#[test]
fn retirement_reserves_identity_and_alias_and_revokes_credentials_durably() {
    let temp = common::TempDir::new("enroll-retire");
    let mut registry = Registry::load(temp.path()).unwrap();
    let key = new_token().unwrap();
    let first = enroll(&mut registry, &key);
    let metadata = registry.retire(&first.token).expect("retire");
    assert!(metadata.retired);
    let mut reloaded = Registry::load(temp.path()).unwrap();
    assert!(reloaded.authenticate(&first.token).is_none());
    assert!(reloaded.resolve(&first.agent_id).is_none());
    assert!(reloaded.resolve(&first.name).is_none());
    assert!(reloaded.is_empty());
    conflict_identity(
        reloaded
            .enroll(key, "codex".into(), "codex".into(), None)
            .unwrap_err(),
        &first.agent_id,
    );
    conflict_identity(
        reloaded
            .register(first.name.clone(), None, None, Some(&first.token))
            .unwrap_err(),
        &first.agent_id,
    );
    assert!(
        reloaded
            .register(first.agent_id.clone(), None, None, None)
            .is_err(),
        "retired IDs remain reserved in the name namespace"
    );
    let second = enroll(&mut reloaded, &new_token().unwrap());
    assert_ne!(first.agent_id, second.agent_id);
    let raw: Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("registry.json")).unwrap()).unwrap();
    assert_eq!(raw["agents"][&first.name]["retired"], true);
    assert_eq!(raw["agents"].as_object().unwrap().len(), 2);
}

#[test]
fn enrollment_cannot_reuse_current_or_revoked_legacy_credentials() {
    let temp = common::TempDir::new("enroll-reserved-token");
    let mut registry = Registry::load(temp.path()).unwrap();
    let first = registry
        .register("legacy".into(), None, None, None)
        .unwrap();
    assert!(registry
        .enroll(first.token.clone(), "codex".into(), "codex".into(), None)
        .is_err());
    let second = registry
        .register("legacy".into(), None, None, Some(&first.token))
        .unwrap();
    assert_ne!(first.token, second.token);
    let mut reloaded = Registry::load(temp.path()).unwrap();
    assert!(reloaded
        .enroll(first.token, "codex".into(), "codex".into(), None)
        .is_err());
}

#[test]
fn existing_v1_registry_deserializes_without_new_fields() {
    let temp = common::TempDir::new("old-registry");
    let key = new_token().unwrap();
    let agent_id = cooperators::id::new_ulid().unwrap();
    std::fs::write(
        temp.path().join("registry.json"),
        serde_json::to_vec(&json!({
            "v": 1, "agents": {"legacy": {
                "agent_id": agent_id, "token_hash": token_hash(&key), "kind": "codex",
                "registered_at": "2026-10-19T00:00:00.000Z"
            }}
        }))
        .unwrap(),
    )
    .unwrap();
    let registry = Registry::load(temp.path()).unwrap();
    let auth = registry.authenticate(&key).expect("legacy token");
    assert_eq!(auth.agent_id, agent_id);
    assert_eq!(
        registry.instance_metadata(&key).unwrap().display_name,
        "legacy"
    );
}

#[test]
fn invalid_launch_secrets_do_not_create_registry_records() {
    let temp = common::TempDir::new("invalid-launch-key");
    let mut registry = Registry::load(temp.path()).unwrap();
    for invalid in [
        "",
        "codex",
        "ABCDEF0123456789ABCDEF0123456789",
        "0000000000000000000000000000000g",
    ] {
        assert!(registry
            .enroll(invalid.into(), "codex".into(), "codex".into(), None)
            .is_err());
    }
    assert!(!temp.path().join("registry.json").exists());
}

#[tokio::test]
async fn concurrent_retry_uses_one_actual_id_and_independent_launch_uses_another() {
    let temp = common::TempDir::new("concurrent-enroll");
    let state = DaemonState::new(temp.path().to_path_buf());
    let key = new_token().unwrap();
    let (one, retry, independent) = tokio::join!(
        state.enroll_agent(key.clone(), "codex".into(), "codex".into(), None),
        state.enroll_agent(key, "codex".into(), "codex".into(), None),
        state.enroll_agent(new_token().unwrap(), "codex".into(), "codex".into(), None),
    );
    let (one, retry, independent) = (one.unwrap(), retry.unwrap(), independent.unwrap());
    assert_eq!(one.agent_id, retry.agent_id);
    assert_eq!(one.token, retry.token);
    assert_ne!(one.resumed, retry.resumed);
    assert_ne!(one.agent_id, independent.agent_id);
    let agents = state.list_agents(true).await.unwrap();
    assert_eq!(agents.len(), 2);
}

#[tokio::test]
async fn enrolled_instances_keep_message_ack_presence_and_lease_ownership_isolated() {
    let temp = common::TempDir::new("instance-protocol");
    let state = Arc::new(DaemonState::new(temp.path().to_path_buf()));
    let first = state
        .enroll_agent(new_token().unwrap(), "codex".into(), "codex".into(), None)
        .await
        .unwrap();
    let second = state
        .enroll_agent(new_token().unwrap(), "codex".into(), "codex".into(), None)
        .await
        .unwrap();
    let actor_a = state.authenticate(&first.token).await.unwrap();
    let actor_b = state.authenticate(&second.token).await.unwrap();
    let request = |to: &str, key: &str, text: &str| SendRequest {
        to: to.into(),
        kind: "chat".into(),
        body: Body::new("text/plain", text),
        reply_to: None,
        idempotency_key: Some(key.into()),
        ttl: None,
    };
    let a_to_b = state
        .send_message(&actor_a, request(&second.agent_id, "same-key", "from A"))
        .await
        .unwrap();
    let b_to_b = state
        .send_message(&actor_b, request(&second.agent_id, "same-key", "from B"))
        .await
        .unwrap();
    assert!(!a_to_b.duplicate && !b_to_b.duplicate);
    assert_ne!(a_to_b.message_id, b_to_b.message_id);
    let duplicate = state
        .send_message(&actor_a, request(&second.agent_id, "same-key", "retry"))
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.message_id, a_to_b.message_id);
    state
        .send_message(&actor_b, request(&first.agent_id, "reply", "to A"))
        .await
        .unwrap();
    let a_batch = state
        .wait_message(&first.agent_id, None, Duration::ZERO)
        .await
        .unwrap();
    let b_batch = state
        .wait_message(&second.agent_id, None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(a_batch.messages.len(), 1);
    assert_eq!(b_batch.messages.len(), 2);
    assert_eq!(b_batch.messages[0].from, first.name);
    assert_eq!(b_batch.messages[1].from, second.name);

    // Even a request supplying the other ID can only ack the authenticated
    // caller's inbox: the real RPC route binds the effect to its token.
    let response = server::dispatch(&state, &mut ConnectionSession::default(), RpcRequest {
        id: Some(json!(1)), method: "ack".into(), is_notification: false,
        params: json!({"token": first.token, "cursor": a_batch.next_cursor, "agent_id": second.agent_id}),
    }).await.unwrap().into_result().unwrap();
    assert_eq!(response["acked_cursor"], a_batch.next_cursor);
    let b_after_ack = state
        .wait_message(&second.agent_id, None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(b_after_ack.acked_cursor, "0");
    assert_eq!(b_after_ack.messages.len(), 2);
    let presence = state.list_agents(true).await.unwrap();
    assert_eq!(presence.len(), 2);
    assert!(presence.iter().any(|view| view.agent_id == first.agent_id));
    assert!(presence.iter().any(|view| view.agent_id == second.agent_id));

    state.create_queue("jobs").await.unwrap();
    state
        .enqueue("jobs", json!({"work": true}), None)
        .await
        .unwrap();
    let task_claim = state.claim_task(&actor_a, "jobs", 60).await.unwrap();
    let error = state
        .complete_task(
            &actor_b,
            &task_claim.lease_id,
            task_claim.fence,
            json!({}),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BrokerError::Application(ErrorCode::LeaseLost, _)
    ));
    state
        .publish_doc(
            &actor_a,
            "draft",
            "text/plain",
            Body::new("text/plain", "v1"),
            None,
        )
        .await
        .unwrap();
    let doc_claim = state.claim_doc(&actor_a, "draft", 60).await.unwrap();
    let error = state
        .update_doc(
            &actor_b,
            "draft",
            1,
            Body::new("text/plain", "foreign"),
            None,
            Some((&doc_claim.lease_id, doc_claim.fence)),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BrokerError::ApplicationData(ErrorCode::ConflictClaimed, _, _)
    ));

    let rotated = state
        .register_agent(
            first.name.clone(),
            Some(first.kind.clone()),
            None,
            Some(&first.token),
        )
        .await
        .unwrap();
    assert!(state.authenticate(&first.token).await.is_err());
    let current_a = state.authenticate(&rotated.token).await.unwrap();
    assert_eq!(current_a.agent_id, first.agent_id);
    state
        .complete_task(
            &current_a,
            &task_claim.lease_id,
            task_claim.fence,
            json!({"done": true}),
            None,
        )
        .await
        .unwrap();
    let updated = state
        .update_doc(
            &current_a,
            "draft",
            1,
            Body::new("text/plain", "v2"),
            None,
            Some((&doc_claim.lease_id, doc_claim.fence)),
        )
        .await
        .unwrap();
    assert_eq!(updated.version, 2);
    let inbox_bytes = std::fs::read_to_string(
        temp.path()
            .join("agents")
            .join(&second.agent_id)
            .join("inbox.jsonl"),
    )
    .unwrap();
    let records: Vec<Value> = inbox_bytes
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["from"], first.name);
    assert_eq!(records[1]["from"], second.name);
    let raw_doc = std::fs::read_to_string(temp.path().join("docs/draft.jsonl")).unwrap();
    let last_doc: Value = serde_json::from_str(raw_doc.lines().last().unwrap()).unwrap();
    assert_eq!(last_doc["actor_id"], first.agent_id);

    let retired = state.retire_agent(&second.token).await.unwrap();
    assert!(retired.retired);
    assert!(state.authenticate(&second.token).await.is_err());
    assert_eq!(state.list_agents(true).await.unwrap().len(), 1);
    let error = state
        .send_message(
            &current_a,
            request(&second.agent_id, "after-retire", "blocked"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BrokerError::Application(ErrorCode::NotFound, _)
    ));
}
