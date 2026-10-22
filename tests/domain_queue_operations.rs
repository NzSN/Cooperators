mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cooperators::error::ErrorCode;
use cooperators::network::{call_remote, NetworkPolicy};
use cooperators::queues::{DeadLetterRecord, QueueEvent};
use cooperators::server::{DaemonConfig, DaemonState};
use cooperators::storage::LogFile;
use serde_json::{json, Value};

fn config(root: &Path, policy: &NetworkPolicy) -> DaemonConfig {
    let mut config = DaemonConfig::new(root);
    config.network_policy = Some(policy.clone());
    config
}

async fn prepare(root: &Path) -> (Arc<DaemonState>, NetworkPolicy, String) {
    let device = cooperators::registry::new_token().unwrap();
    let permissions = [
        "create_queue",
        "enqueue",
        "claim_task",
        "renew_lease",
        "complete_task",
        "fail_task",
        "operation_status",
        "instance_metadata",
    ]
    .into_iter()
    .map(|method| json!({"method":method,"resources":["*"]}))
    .collect::<Vec<_>>();
    let policy: NetworkPolicy = serde_json::from_value(json!({
        "v":1,"enable_operations":true,"enable_resource_operations":true,
        "allowed_hosts":["localhost"],"devices":[{
            "principal_id":"queue-operations","generation":1,
            "token_hash":cooperators::registry::token_hash(&device),
            "expires_at_ms":cooperators::id::unix_millis()+3_600_000,
            "allow_enroll":true,"max_instances":2,"instance_ttl_s":3600,
            "permissions":permissions
        }]
    }))
    .unwrap();
    let state = Arc::new(DaemonState::with_config(config(root, &policy)).unwrap());
    let token = cooperators::registry::new_token().unwrap();
    state
        .network()
        .unwrap()
        .enroll(
            &state,
            &device,
            json!({
                "launch_key":token,"kind":"test","display_name":"queue worker"
            }),
        )
        .await
        .unwrap();
    (state, policy, token)
}

fn intent(state: &DaemonState, mut params: Value) -> Value {
    params["operation"] = json!({
        "v":1,"id":cooperators::id::new_ulid().unwrap(),
        "epoch":state.network().unwrap().meta.operation_epoch,
        "not_after_ms":cooperators::id::unix_millis()+600_000,
    });
    params
}

fn records(root: &Path, queue: &str) -> Vec<QueueEvent> {
    LogFile::<QueueEvent>::read_segments(root.join("queues").join(format!("{queue}.jsonl")))
        .unwrap()
        .records
}

fn dead_records(root: &Path, queue: &str) -> Vec<DeadLetterRecord> {
    LogFile::<DeadLetterRecord>::read_segments(root.join("dead").join(format!("{queue}.jsonl")))
        .unwrap()
        .records
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_queue_receipts_survive_completed_state_and_restart() {
    let root = common::TempDir::new("domain-queue-receipts");
    let (state, policy, token) = prepare(root.path()).await;
    let mut saved = Vec::new();
    for (method, params) in [
        ("create_queue", json!({"queue":"jobs"})),
        ("enqueue", json!({"queue":"jobs","payload":{"work":"one"}})),
        ("claim_task", json!({"queue":"jobs","lease_ttl_s":60})),
    ] {
        let params = intent(&state, params);
        let result = call_remote(&state, &token, method, params.clone())
            .await
            .unwrap();
        saved.push((method, params, result));
    }
    let claim = &saved[2].2;
    let renew = intent(&state, json!({"lease_id":claim["lease_id"]}));
    let renewed = call_remote(&state, &token, "renew_lease", renew.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        call_remote(&state, &token, "renew_lease", renew.clone())
            .await
            .unwrap(),
        renewed
    );
    let complete = intent(
        &state,
        json!({
            "lease_id":claim["lease_id"],"fence":claim["fence"],
            "outcome":{"answer":42},"receipt":{"worker":"done"}
        }),
    );
    let completed = call_remote(&state, &token, "complete_task", complete.clone())
        .await
        .unwrap();
    saved.push(("renew_lease", renew, renewed));
    saved.push(("complete_task", complete, completed));
    let count = records(root.path(), "jobs").len();
    assert_eq!(count, 5);
    for (method, params, expected) in &saved {
        assert_eq!(
            call_remote(&state, &token, method, params.clone())
                .await
                .unwrap(),
            *expected
        );
    }
    let mut changed = saved[1].1.clone();
    changed["payload"] = json!({"work":"different"});
    assert_eq!(
        call_remote(&state, &token, "enqueue", changed)
            .await
            .unwrap_err()
            .code,
        ErrorCode::IdempotencyConflict.rpc_code()
    );
    assert_eq!(records(root.path(), "jobs").len(), count);
    drop(state);
    let state = Arc::new(DaemonState::with_config(config(root.path(), &policy)).unwrap());
    for (method, params, expected) in saved {
        assert_eq!(
            call_remote(&state, &token, method, params).await.unwrap(),
            expected
        );
    }
    assert_eq!(records(root.path(), "jobs").len(), count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_claim_remains_not_found_after_enqueue_and_restart() {
    let root = common::TempDir::new("domain-empty-claim");
    let (state, policy, token) = prepare(root.path()).await;
    state.create_queue("jobs").await.unwrap();
    let claim = intent(&state, json!({"queue":"jobs"}));
    let initial = call_remote(&state, &token, "claim_task", claim.clone())
        .await
        .unwrap_err();
    assert_eq!(initial.code, ErrorCode::NotFound.rpc_code());
    state.enqueue("jobs", json!("later"), None).await.unwrap();
    let replay = call_remote(&state, &token, "claim_task", claim.clone())
        .await
        .unwrap_err();
    assert_eq!(replay, initial);
    drop(state);
    let state = Arc::new(DaemonState::with_config(config(root.path(), &policy)).unwrap());
    assert_eq!(
        call_remote(&state, &token, "claim_task", claim)
            .await
            .unwrap_err(),
        initial
    );
    assert_eq!(
        records(root.path(), "jobs")
            .iter()
            .filter(|e| e.kind == "task_claimed")
            .count(),
        0
    );
    let fresh = intent(&state, json!({"queue":"jobs"}));
    assert_eq!(
        call_remote(&state, &token, "claim_task", fresh)
            .await
            .unwrap()["task"]["payload"],
        "later"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_receipts_remain_original_after_reclaim_and_dead_letter() {
    let root = common::TempDir::new("domain-failed-task");
    let (state, policy, token) = prepare(root.path()).await;
    state.create_queue("jobs").await.unwrap();
    state
        .enqueue("jobs", json!("retry"), Some(2))
        .await
        .unwrap();
    let mut failures = Vec::new();
    for attempt in 1..=2 {
        let claim = call_remote(
            &state,
            &token,
            "claim_task",
            intent(&state, json!({"queue":"jobs"})),
        )
        .await
        .unwrap();
        let params = intent(
            &state,
            json!({"lease_id":claim["lease_id"],"fence":claim["fence"],"reason":format!("attempt {attempt}")}),
        );
        let result = call_remote(&state, &token, "fail_task", params.clone())
            .await
            .unwrap();
        assert_eq!(
            result["status"],
            if attempt == 1 { "requeued" } else { "dead" }
        );
        failures.push((params, result));
    }
    assert_eq!(dead_records(root.path(), "jobs").len(), 1);
    let count = records(root.path(), "jobs").len();
    for (params, result) in &failures {
        assert_eq!(
            call_remote(&state, &token, "fail_task", params.clone())
                .await
                .unwrap(),
            *result
        );
    }
    drop(state);
    let state = Arc::new(DaemonState::with_config(config(root.path(), &policy)).unwrap());
    for (params, result) in failures {
        assert_eq!(
            call_remote(&state, &token, "fail_task", params)
                .await
                .unwrap(),
            result
        );
    }
    assert_eq!(records(root.path(), "jobs").len(), count);
    assert_eq!(dead_records(root.path(), "jobs").len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hash_valid_claim_receipt_must_match_original_enqueue_state() {
    let root = common::TempDir::new("domain-queue-proof-binding");
    let (state, policy, token) = prepare(root.path()).await;
    state.create_queue("jobs").await.unwrap();
    state.enqueue("jobs", json!("source"), None).await.unwrap();
    call_remote(
        &state,
        &token,
        "claim_task",
        intent(&state, json!({"queue":"jobs"})),
    )
    .await
    .unwrap();
    drop(state);
    let mut source = records(root.path(), "jobs");
    let event = source.last_mut().unwrap();
    let forged_timestamp = "2000-01-01T00:00:00.000Z";
    event.enqueued_at = Some(forged_timestamp.into());
    let cooperators::operations::OperationOutcome::Success(result) =
        &mut event.domain_operation.as_mut().unwrap().outcome
    else {
        panic!("success proof");
    };
    result["task"]["enqueued_at"] = forged_timestamp.into();
    let mut previous = source[0].prev_hash.clone();
    let mut rewritten = String::new();
    for event in &mut source {
        event.prev_hash = previous;
        previous = cooperators::storage::hash_record(event).unwrap();
        rewritten.push_str(&serde_json::to_string(event).unwrap());
        rewritten.push('\n');
    }
    std::fs::write(root.path().join("queues/jobs.jsonl"), rewritten).unwrap();
    // Storage accepts this chain; domain validation must still reject its
    // receipt because both forged copies disagree with the enqueue event.
    assert_eq!(records(root.path(), "jobs").len(), 3);
    let error = match DaemonState::with_config(config(root.path(), &policy)) {
        Ok(_) => panic!("forged receipt accepted"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("invalid authoritative queue operation proof"));
}

#[cfg(feature = "mbt-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_projection_failure_recovers_actual_sequence_without_reservation() {
    use cooperators::faults::{arm_point, FaultPoint, PointFault};
    for point in [
        FaultPoint::BeforeDeadProjection,
        FaultPoint::AfterDeadProjectionAppend,
    ] {
        let root = common::TempDir::new("domain-dead-projection");
        let (state, policy, token) = prepare(root.path()).await;
        state.create_queue("jobs").await.unwrap();
        state
            .enqueue("jobs", json!("first"), Some(1))
            .await
            .unwrap();
        let claim = call_remote(
            &state,
            &token,
            "claim_task",
            intent(&state, json!({"queue":"jobs"})),
        )
        .await
        .unwrap();
        let params = intent(
            &state,
            json!({"lease_id":claim["lease_id"],"fence":claim["fence"],"reason":"terminal"}),
        );
        let hook = arm_point(root.path().join("dead/jobs.jsonl"), point, PointFault::Fail).unwrap();
        assert_eq!(
            call_remote(&state, &token, "fail_task", params.clone())
                .await
                .unwrap_err()
                .code,
            ErrorCode::OutcomeUnknown.rpc_code()
        );
        assert_eq!(
            call_remote(&state, &token, "fail_task", params.clone())
                .await
                .unwrap_err()
                .code,
            ErrorCode::OutcomeUnknown.rpc_code()
        );
        drop(hook);
        // A different task may occupy the projection sequence while the first
        // operation is uncertain; recovery must derive the actual sequence.
        let identity = state.authenticate(&token).await.unwrap();
        state
            .enqueue("jobs", json!("second"), Some(1))
            .await
            .unwrap();
        let second = state.claim_task(&identity, "jobs", 60).await.unwrap();
        state
            .fail_task(&identity, &second.lease_id, second.fence, "second terminal")
            .await
            .unwrap();
        let source_count = records(root.path(), "jobs").len();
        drop(state);
        let state = Arc::new(DaemonState::with_config(config(root.path(), &policy)).unwrap());
        let result = call_remote(&state, &token, "fail_task", params.clone())
            .await
            .unwrap();
        assert_eq!(result["status"], "dead");
        assert_eq!(
            result["dead_letter_seq"],
            if point == FaultPoint::BeforeDeadProjection {
                2
            } else {
                1
            }
        );
        let dead = dead_records(root.path(), "jobs");
        assert_eq!(dead.len(), 2);
        let actual = dead
            .iter()
            .find(|record| record.task_id == result["task_id"].as_str().unwrap())
            .unwrap();
        assert_eq!(result["dead_letter_seq"], actual.seq);
        assert_eq!(
            call_remote(&state, &token, "fail_task", params)
                .await
                .unwrap(),
            result
        );
        assert_eq!(records(root.path(), "jobs").len(), source_count);
    }
}
