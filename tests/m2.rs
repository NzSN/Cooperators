mod common;

use std::sync::Arc;
use std::time::Duration;

use cooperators::rpc::{RpcError, RpcRequest};
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use cooperators::storage::LogFile;
use serde_json::{json, Value};

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
        .expect("dispatch returns a response")
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

async fn register(state: &Arc<DaemonState>, name: &str) -> String {
    let mut session = ConnectionSession::default();
    let value = call(
        state,
        &mut session,
        "register_agent",
        json!({"name": name, "kind": "test"}),
    )
    .await
    .expect("register agent");
    value["token"].as_str().expect("token").to_string()
}

fn state_with(root: &std::path::Path) -> Arc<DaemonState> {
    Arc::new(DaemonState::with_config(DaemonConfig::new(root)).expect("daemon state"))
}

#[tokio::test]
async fn channel_publish_dedupe_read_and_cursor_replay() {
    let temp = common::TempDir::new("m2-channel");
    let root = temp.path().join("root");
    let state = state_with(&root);
    let alice = register(&state, "alice").await;
    let bob = register(&state, "bob").await;
    let mut session = ConnectionSession::default();

    let first = call(
        &state,
        &mut session,
        "publish",
        json!({
            "token": alice,
            "channel": "build",
            "body": {"mime": "text/markdown", "text": "hello"},
            "idempotency_key": "build-1",
        }),
    )
    .await
    .expect("publish first");
    assert_eq!(first["seq"], 1);
    assert_eq!(first["duplicate"], false);
    assert_eq!(
        first["cursor"],
        json!({"file": "channels/build.jsonl", "seq": 1})
    );

    let duplicate = call(
        &state,
        &mut session,
        "publish",
        json!({
            "token": alice,
            "channel": "build",
            "text": "hello again",
            "idempotency_key": "build-1",
        }),
    )
    .await
    .expect("duplicate publish");
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["seq"], first["seq"]);

    call(
        &state,
        &mut session,
        "publish",
        json!({"token": alice, "channel": "build", "text": "second"}),
    )
    .await
    .expect("publish second");

    let mut bob_session = ConnectionSession::default();
    let page_one = call(
        &state,
        &mut bob_session,
        "read_channel",
        json!({
            "token": bob,
            "channel": "build",
            "cursor": {"file": "channels/build.jsonl", "seq": 0},
            "limit": 1,
        }),
    )
    .await
    .expect("read page one");
    assert_eq!(page_one["messages"].as_array().expect("messages").len(), 1);
    assert_eq!(page_one["messages"][0]["body"]["text"], "hello");
    assert_eq!(page_one["next_cursor"]["seq"], 1);
    assert_eq!(page_one["timed_out"], false);

    let page_two = call(
        &state,
        &mut bob_session,
        "read_channel",
        json!({
            "token": bob,
            "channel": "build",
            "cursor": page_one["next_cursor"].clone(),
            "limit": 10,
        }),
    )
    .await
    .expect("read page two");
    assert_eq!(page_two["messages"].as_array().expect("messages").len(), 1);
    assert_eq!(page_two["messages"][0]["body"]["text"], "second");
    assert_eq!(page_two["next_cursor"]["seq"], 2);

    // Empty read is a peek and does not move the cursor.
    let empty = call(
        &state,
        &mut bob_session,
        "read_channel",
        json!({"token": bob, "channel": "build", "cursor": "2", "wait_s": 0}),
    )
    .await
    .expect("empty read");
    assert_eq!(empty["messages"], json!([]));
    assert_eq!(empty["next_cursor"]["seq"], 2);
    assert_eq!(empty["timed_out"], true);

    // Long-poll wakes on a later publish.
    let waiter_state = Arc::clone(&state);
    let waiter_token = bob.clone();
    let waiter = tokio::spawn(async move {
        let mut session = ConnectionSession::default();
        call(
            &waiter_state,
            &mut session,
            "read_channel",
            json!({"token": waiter_token, "channel": "build", "cursor": "2", "wait_s": 2}),
        )
        .await
        .expect("wait for third")
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    call(
        &state,
        &mut session,
        "publish",
        json!({"token": alice, "channel": "build", "text": "third"}),
    )
    .await
    .expect("publish third");
    let delivered = waiter.await.expect("waiter task");
    assert_eq!(delivered["messages"][0]["body"]["text"], "third");

    let missing = call(
        &state,
        &mut bob_session,
        "read_channel",
        json!({"token": bob, "channel": "never", "cursor": "0"}),
    )
    .await
    .expect_err("unknown channel");
    assert_eq!(app_code(&missing), "NOT_FOUND");
}

#[tokio::test]
async fn queue_claim_expiry_requeue_dead_letter_and_stale_fence() {
    let temp = common::TempDir::new("m2-queue");
    let root = temp.path().join("root");
    let state = state_with(&root);
    let alice = register(&state, "alice").await;
    let mut session = ConnectionSession::default();

    let created = call(
        &state,
        &mut session,
        "create_queue",
        json!({"token": alice, "queue": "jobs"}),
    )
    .await
    .expect("create queue");
    assert_eq!(created["queue"], "jobs");

    let existing = call(
        &state,
        &mut session,
        "create_queue",
        json!({"token": alice, "queue": "jobs"}),
    )
    .await
    .expect_err("queue name taken");
    assert_eq!(app_code(&existing), "EXISTS");

    let enqueued = call(
        &state,
        &mut session,
        "enqueue",
        json!({"token": alice, "queue": "jobs", "payload": {"n": 1}, "max_attempts": 2}),
    )
    .await
    .expect("enqueue");
    assert_eq!(enqueued["attempts"], 0);

    let first_claim = call(
        &state,
        &mut session,
        "claim_task",
        json!({"token": alice, "queue": "jobs", "lease_ttl_s": 1}),
    )
    .await
    .expect("first claim");
    assert_eq!(first_claim["task"]["payload"], json!({"n": 1}));
    let first_lease = first_claim["lease_id"].as_str().expect("lease").to_string();
    let first_fence = first_claim["fence"].as_u64().expect("fence");
    assert_eq!(
        first_fence,
        enqueued["seq"].as_u64().unwrap() + 1,
        "claim follows the enqueue in the queue log"
    );

    // A wrong fence on the live lease is rejected.
    let bad_fence = call(
        &state,
        &mut session,
        "complete_task",
        json!({
            "token": alice,
            "lease_id": first_lease,
            "fence": first_fence - 1,
            "outcome": {"ok": true},
        }),
    )
    .await
    .expect_err("stale fence");
    assert_eq!(app_code(&bad_fence), "LEASE_LOST");

    tokio::time::sleep(Duration::from_millis(1_050)).await;
    state.sweep_expired_queues().await.expect("sweep");

    let second_claim = call(
        &state,
        &mut session,
        "claim_task",
        json!({"token": alice, "queue": "jobs", "lease_ttl_s": 1}),
    )
    .await
    .expect("reclaim after expiry");
    assert_eq!(second_claim["task"]["attempts"], 2);
    let second_lease = second_claim["lease_id"]
        .as_str()
        .expect("lease")
        .to_string();
    let second_fence = second_claim["fence"].as_u64().expect("fence");
    assert!(second_fence > first_fence);

    // The previous attempt is stale after the task was re-issued.
    let stale = call(
        &state,
        &mut session,
        "complete_task",
        json!({
            "token": alice,
            "lease_id": first_lease,
            "fence": first_fence,
            "outcome": {"ok": true},
        }),
    )
    .await
    .expect_err("old lease");
    assert_eq!(app_code(&stale), "LEASE_LOST");

    tokio::time::sleep(Duration::from_millis(1_050)).await;
    state.sweep_expired_queues().await.expect("sweep again");

    let exhausted = call(
        &state,
        &mut session,
        "claim_task",
        json!({"token": alice, "queue": "jobs", "lease_ttl_s": 1}),
    )
    .await
    .expect_err("dead lettered");
    assert_eq!(app_code(&exhausted), "NOT_FOUND");

    let dead_path = root.join("dead/jobs.jsonl");
    let (_dead_log, report) =
        LogFile::<cooperators::queues::DeadLetterRecord>::open(&dead_path).expect("dead log");
    assert_eq!(report.records.len(), 1, "one dead letter");
    let dead = &report.records[0];
    assert_eq!(dead.task_id, enqueued["task_id"].as_str().unwrap());
    assert_eq!(dead.attempts, 2);
    assert_eq!(dead.max_attempts, 2);
    assert_eq!(dead.history.len(), 2);
    assert_eq!(dead.history[0].lease_id, first_lease);
    assert_eq!(dead.history[1].lease_id, second_lease);
    assert!(dead.history[0].ended_at.is_some());
}

#[tokio::test]
async fn doc_cas_claim_exclusion_expired_write_and_blob_roundtrip() {
    let temp = common::TempDir::new("m2-docs");
    let root = temp.path().join("root");
    let state = state_with(&root);
    let alice = register(&state, "alice").await;
    let bob = register(&state, "bob").await;
    let mut session = ConnectionSession::default();

    let published = call(
        &state,
        &mut session,
        "publish_doc",
        json!({
            "token": alice,
            "name": "plan",
            "mime": "text/markdown",
            "text": "# one",
            "title": "Plan",
        }),
    )
    .await
    .expect("publish doc");
    assert_eq!(published["version"], 1);
    assert_eq!(published["storage"], "inline");

    let duplicate = call(
        &state,
        &mut session,
        "publish_doc",
        json!({"token": alice, "name": "plan", "text": "again"}),
    )
    .await
    .expect_err("name taken");
    assert_eq!(app_code(&duplicate), "EXISTS");

    let conflict = call(
        &state,
        &mut session,
        "update_doc",
        json!({"token": alice, "name": "plan", "base_version": 2, "text": "# two"}),
    )
    .await
    .expect_err("CAS conflict");
    assert_eq!(app_code(&conflict), "VERSION_CONFLICT");
    assert_eq!(
        conflict.data.as_ref().expect("data")["head_version"],
        json!(1)
    );

    let read = call(
        &state,
        &mut session,
        "read_doc",
        json!({"token": bob, "name": "plan"}),
    )
    .await
    .expect("open read");
    assert_eq!(read["body"]["text"], "# one");
    assert_eq!(read["head_version"], 1);
    assert_eq!(read["claim"], Value::Null);

    let updated = call(
        &state,
        &mut session,
        "update_doc",
        json!({"token": alice, "name": "plan", "base_version": 1, "text": "# two"}),
    )
    .await
    .expect("CAS retry");
    assert_eq!(updated["version"], 2);

    let old_version = call(
        &state,
        &mut session,
        "read_doc",
        json!({"token": bob, "name": "plan", "version": 1}),
    )
    .await
    .expect("pinned version");
    assert_eq!(old_version["body"]["text"], "# one");
    assert_eq!(old_version["head_version"], 2);

    let claim = call(
        &state,
        &mut session,
        "claim_doc",
        json!({"token": alice, "name": "plan", "lease_ttl_s": 1}),
    )
    .await
    .expect("claim doc");
    let lease_id = claim["lease_id"].as_str().expect("lease").to_string();
    let fence = claim["fence"].as_u64().expect("fence");
    assert_eq!(claim["head_version"], 2);

    let mut bob_session = ConnectionSession::default();
    let blocked = call(
        &state,
        &mut bob_session,
        "claim_doc",
        json!({"token": bob, "name": "plan", "lease_ttl_s": 30}),
    )
    .await
    .expect_err("claimed");
    assert_eq!(app_code(&blocked), "CONFLICT_CLAIMED");
    let blocked_data = blocked.data.as_ref().expect("data");
    assert_eq!(blocked_data["holder"]["name"], "alice");
    assert!(blocked_data["expires_at"].is_string());
    assert_eq!(blocked_data["fence"], json!(fence));
    assert_eq!(blocked_data["head_version"], json!(2));

    let blocked_update = call(
        &state,
        &mut bob_session,
        "update_doc",
        json!({"token": bob, "name": "plan", "base_version": 2, "text": "sneak"}),
    )
    .await
    .expect_err("conflict claimed on update");
    assert_eq!(app_code(&blocked_update), "CONFLICT_CLAIMED");
    assert_eq!(
        blocked_update.data.as_ref().expect("data")["holder"]["name"],
        "alice"
    );

    tokio::time::sleep(Duration::from_millis(1_050)).await;
    let expired_write = call(
        &state,
        &mut session,
        "update_doc",
        json!({
            "token": alice,
            "name": "plan",
            "base_version": 2,
            "text": "late",
            "lease_id": lease_id,
            "fence": fence,
        }),
    )
    .await
    .expect_err("expired claim write");
    assert_eq!(app_code(&expired_write), "LEASE_LOST");

    let after_expiry = call(
        &state,
        &mut bob_session,
        "read_doc",
        json!({"token": bob, "name": "plan"}),
    )
    .await
    .expect("read after expiry");
    assert_eq!(after_expiry["claim"], Value::Null);
    assert!(after_expiry["fence"].as_u64().expect("fence") > fence);
    assert_eq!(after_expiry["head_version"], 2);

    // A fresh claim succeeds after expiry, and renewal keeps the fence.
    let second_claim = call(
        &state,
        &mut bob_session,
        "claim_doc",
        json!({"token": bob, "name": "plan", "lease_ttl_s": 30}),
    )
    .await
    .expect("claim after expiry");
    let renewed = call(
        &state,
        &mut bob_session,
        "renew_lease",
        json!({"token": bob, "lease_id": second_claim["lease_id"]}),
    )
    .await
    .expect("renew doc lease");
    assert_eq!(renewed["kind"], "doc");
    assert_eq!(renewed["fence"], second_claim["fence"]);

    let fenced_update = call(
        &state,
        &mut bob_session,
        "update_doc",
        json!({
            "token": bob,
            "name": "plan",
            "base_version": 2,
            "text": "# three",
            "lease_id": second_claim["lease_id"],
            "fence": second_claim["fence"],
        }),
    )
    .await
    .expect("fenced update");
    assert_eq!(fenced_update["version"], 3);

    let released = call(
        &state,
        &mut bob_session,
        "release_doc",
        json!({
            "token": bob,
            "lease_id": second_claim["lease_id"],
            "fence": second_claim["fence"],
        }),
    )
    .await
    .expect("release");
    assert_eq!(released["released"], true);
    assert!(
        released["new_fence"].as_u64().expect("new fence")
            > second_claim["fence"].as_u64().unwrap()
    );

    // Large body spills to a content-addressed blob and round-trips.
    let large = "x".repeat(70_000);
    let large_receipt = call(
        &state,
        &mut session,
        "publish_doc",
        json!({"token": alice, "name": "large", "text": large, "mime": "text/plain"}),
    )
    .await
    .expect("publish large");
    assert_eq!(large_receipt["storage"], "blob");
    let digest = large_receipt["blob"]["sha256"].as_str().expect("digest");
    assert!(root.join("blobs/sha256").join(digest).is_file());
    assert_eq!(large_receipt["blob"]["size"], json!(70_000));

    let large_read = call(
        &state,
        &mut session,
        "read_doc",
        json!({"token": bob, "name": "large"}),
    )
    .await
    .expect("read large");
    assert_eq!(large_read["storage"], "blob");
    assert_eq!(large_read["body"]["text"].as_str().unwrap().len(), 70_000);
    assert_eq!(large_read["body"]["text"], json!(large));

    let listed = call(
        &state,
        &mut session,
        "list_docs",
        json!({"token": bob, "prefix": "la"}),
    )
    .await
    .expect("list docs");
    assert_eq!(listed["docs"].as_array().expect("docs").len(), 1);
    assert_eq!(listed["docs"][0]["name"], "large");
}

#[tokio::test]
async fn restart_replays_queue_leases_doc_claims_and_channels() {
    let temp = common::TempDir::new("m2-restart");
    let root = temp.path().join("root");

    let (alice, task_lease, task_fence, doc_lease, doc_fence, channel_seq) = {
        let state = state_with(&root);
        let alice = register(&state, "alice").await;
        let mut session = ConnectionSession::default();
        call(
            &state,
            &mut session,
            "create_queue",
            json!({"token": alice, "queue": "restart"}),
        )
        .await
        .expect("create queue");
        call(
            &state,
            &mut session,
            "enqueue",
            json!({"token": alice, "queue": "restart", "payload": {"work": 1}}),
        )
        .await
        .expect("enqueue");
        let claim = call(
            &state,
            &mut session,
            "claim_task",
            json!({"token": alice, "queue": "restart", "lease_ttl_s": 60}),
        )
        .await
        .expect("claim task");

        call(
            &state,
            &mut session,
            "publish_doc",
            json!({"token": alice, "name": "shared", "text": "v1"}),
        )
        .await
        .expect("publish doc");
        let doc_claim = call(
            &state,
            &mut session,
            "claim_doc",
            json!({"token": alice, "name": "shared", "lease_ttl_s": 60}),
        )
        .await
        .expect("claim doc");

        let channel = call(
            &state,
            &mut session,
            "publish",
            json!({
                "token": alice,
                "channel": "restart",
                "text": "durable",
                "idempotency_key": "durable-key",
            }),
        )
        .await
        .expect("publish channel");
        (
            alice,
            claim["lease_id"].as_str().unwrap().to_string(),
            claim["fence"].as_u64().unwrap(),
            doc_claim["lease_id"].as_str().unwrap().to_string(),
            doc_claim["fence"].as_u64().unwrap(),
            channel["seq"].as_u64().unwrap(),
        )
    };

    let state = state_with(&root);
    let mut session = ConnectionSession::default();

    let tasks = call(
        &state,
        &mut session,
        "complete_task",
        json!({
            "token": alice,
            "lease_id": task_lease,
            "fence": task_fence,
            "outcome": {"done": true},
            "receipt": {"note": "after restart"},
        }),
    )
    .await
    .expect("complete after restart");
    assert_eq!(tasks["status"], "completed");

    let doc = call(
        &state,
        &mut session,
        "update_doc",
        json!({
            "token": alice,
            "name": "shared",
            "base_version": 1,
            "text": "v2",
            "lease_id": doc_lease,
            "fence": doc_fence,
        }),
    )
    .await
    .expect("update after restart");
    assert_eq!(doc["version"], 2);

    let read = call(
        &state,
        &mut session,
        "read_doc",
        json!({"token": alice, "name": "shared"}),
    )
    .await
    .expect("read after restart");
    assert_eq!(read["head_version"], 2);
    assert_eq!(read["body"]["text"], "v2");
    assert_eq!(read["claim"]["holder"]["name"], "alice");

    let channel = call(
        &state,
        &mut session,
        "read_channel",
        json!({"token": alice, "channel": "restart", "cursor": "0"}),
    )
    .await
    .expect("read channel after restart");
    assert_eq!(channel["messages"][0]["seq"], channel_seq);
    assert_eq!(channel["messages"][0]["body"]["text"], "durable");

    let duplicate = call(
        &state,
        &mut session,
        "publish",
        json!({
            "token": alice,
            "channel": "restart",
            "text": "durable again",
            "idempotency_key": "durable-key",
        }),
    )
    .await
    .expect("dedupe after restart");
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["seq"], channel_seq);

    // The completed task is not claimable again.
    let empty = call(
        &state,
        &mut session,
        "claim_task",
        json!({"token": alice, "queue": "restart", "lease_ttl_s": 5}),
    )
    .await
    .expect_err("no task left");
    assert_eq!(app_code(&empty), "NOT_FOUND");
}

#[tokio::test]
async fn queue_renew_and_fail_retry_then_dead_letter() {
    let temp = common::TempDir::new("m2-queue-fail");
    let root = temp.path().join("root");
    let state = state_with(&root);
    let alice = register(&state, "alice").await;
    let mut session = ConnectionSession::default();

    call(
        &state,
        &mut session,
        "create_queue",
        json!({"token": alice, "queue": "retry"}),
    )
    .await
    .expect("create");
    call(
        &state,
        &mut session,
        "enqueue",
        json!({"token": alice, "queue": "retry", "payload": ["a"], "max_attempts": 2}),
    )
    .await
    .expect("enqueue");

    let claim = call(
        &state,
        &mut session,
        "claim_task",
        json!({"token": alice, "queue": "retry", "lease_ttl_s": 5}),
    )
    .await
    .expect("claim");
    let lease_id = claim["lease_id"].as_str().expect("lease").to_string();
    let fence = claim["fence"].as_u64().expect("fence");

    let renewed = call(
        &state,
        &mut session,
        "renew_lease",
        json!({"token": alice, "lease_id": lease_id}),
    )
    .await
    .expect("renew task");
    assert_eq!(renewed["kind"], "task");
    assert_eq!(renewed["fence"], json!(fence));

    let failed = call(
        &state,
        &mut session,
        "fail_task",
        json!({"token": alice, "lease_id": lease_id, "fence": fence, "reason": "boom"}),
    )
    .await
    .expect("fail first attempt");
    assert_eq!(failed["status"], "requeued");
    assert_eq!(failed["next_attempt"], 2);

    let second = call(
        &state,
        &mut session,
        "claim_task",
        json!({"token": alice, "queue": "retry", "lease_ttl_s": 5}),
    )
    .await
    .expect("second attempt");
    assert_eq!(second["task"]["attempts"], 2);
    let second_lease = second["lease_id"].as_str().expect("lease").to_string();
    let second_fence = second["fence"].as_u64().expect("fence");

    let dead = call(
        &state,
        &mut session,
        "fail_task",
        json!({
            "token": alice,
            "lease_id": second_lease,
            "fence": second_fence,
            "reason": "still broken",
        }),
    )
    .await
    .expect("fail second attempt");
    assert_eq!(dead["status"], "dead");
    assert!(dead["dead_letter_seq"].as_u64().is_some());

    let (_log, report) =
        LogFile::<cooperators::queues::DeadLetterRecord>::open(root.join("dead/retry.jsonl"))
            .expect("dead log");
    assert_eq!(report.records.len(), 1);
    assert_eq!(report.records[0].history.len(), 2);
    assert_eq!(
        report.records[0].history[1].result.as_deref(),
        Some("failed: still broken")
    );
}

#[tokio::test]
async fn restart_after_expiry_requeues_task_and_frees_doc_claim() {
    let temp = common::TempDir::new("m2-restart-expired");
    let root = temp.path().join("root");

    let (alice, old_task_lease, old_task_fence, old_doc_fence) = {
        let state = state_with(&root);
        let alice = register(&state, "alice").await;
        let mut session = ConnectionSession::default();
        call(
            &state,
            &mut session,
            "create_queue",
            json!({"token": alice, "queue": "expiring"}),
        )
        .await
        .expect("create queue");
        call(
            &state,
            &mut session,
            "enqueue",
            json!({"token": alice, "queue": "expiring", "payload": 7, "max_attempts": 5}),
        )
        .await
        .expect("enqueue");
        let task = call(
            &state,
            &mut session,
            "claim_task",
            json!({"token": alice, "queue": "expiring", "lease_ttl_s": 1}),
        )
        .await
        .expect("claim task");
        call(
            &state,
            &mut session,
            "publish_doc",
            json!({"token": alice, "name": "expiring-doc", "text": "v1"}),
        )
        .await
        .expect("publish doc");
        let doc = call(
            &state,
            &mut session,
            "claim_doc",
            json!({"token": alice, "name": "expiring-doc", "lease_ttl_s": 1}),
        )
        .await
        .expect("claim doc");
        (
            alice,
            task["lease_id"].as_str().unwrap().to_string(),
            task["fence"].as_u64().unwrap(),
            doc["fence"].as_u64().unwrap(),
        )
    };

    tokio::time::sleep(Duration::from_millis(1_050)).await;
    let state = state_with(&root);
    let mut session = ConnectionSession::default();

    let stale_task = call(
        &state,
        &mut session,
        "complete_task",
        json!({
            "token": alice,
            "lease_id": old_task_lease,
            "fence": old_task_fence,
            "outcome": true,
        }),
    )
    .await
    .expect_err("expired across restart");
    assert_eq!(app_code(&stale_task), "LEASE_LOST");

    let reclaim = call(
        &state,
        &mut session,
        "claim_task",
        json!({"token": alice, "queue": "expiring", "lease_ttl_s": 5}),
    )
    .await
    .expect("reclaim after restart");
    assert_eq!(reclaim["task"]["attempts"], 2);
    assert!(reclaim["fence"].as_u64().unwrap() > old_task_fence);

    let freed = call(
        &state,
        &mut session,
        "read_doc",
        json!({"token": alice, "name": "expiring-doc"}),
    )
    .await
    .expect("read doc");
    assert_eq!(freed["claim"], Value::Null);
    assert!(freed["fence"].as_u64().unwrap() > old_doc_fence);
}
