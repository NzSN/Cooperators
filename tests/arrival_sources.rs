mod common;

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cooperators::docs::DocEvent;
use cooperators::queues::QueueEvent;
use cooperators::rpc::{RpcError, RpcRequest};
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use cooperators::storage::{hash_record, LogFile};
use serde_json::{json, Value};

fn state(root: &Path, enabled: bool) -> Arc<DaemonState> {
    let mut config = DaemonConfig::new(root);
    config.enable_arrivals = enabled;
    Arc::new(DaemonState::with_config(config).expect("open daemon"))
}

async fn call(state: &Arc<DaemonState>, method: &str, params: Value) -> Result<Value, RpcError> {
    server::dispatch(
        state,
        &mut ConnectionSession::default(),
        RpcRequest {
            id: Some(json!(1)),
            method: method.to_string(),
            params,
            is_notification: false,
        },
    )
    .await
    .expect("response")
    .into_result()
}

async fn agent(state: &Arc<DaemonState>, name: &str) -> Value {
    call(
        state,
        "register_agent",
        json!({"name": name, "kind": "test"}),
    )
    .await
    .expect("register agent")
}

async fn subscribe(
    state: &Arc<DaemonState>,
    token: &Value,
    key: &str,
    resource_type: &str,
    name: &str,
    catch_up: bool,
) -> Value {
    call(
        state,
        "subscribe_arrivals",
        json!({
            "token": token,
            "client_key": key,
            "resource": {"type": resource_type, "name": name},
            "event_kinds": if resource_type == "document" {
                json!(["doc.published", "doc.updated"])
            } else {
                json!(["queue.task_available"])
            },
            "catch_up": catch_up,
        }),
    )
    .await
    .expect("subscribe")
}

#[test]
fn historical_document_and_queue_hashes_omit_absent_intent() {
    // These are complete V1 typed layouts. The new optional member must not
    // appear, including as null, when a historical record is reserialized.
    let document = json!({
        "seq": 1, "prev_hash": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        "ts": "2026-10-20T00:00:00Z", "kind": "doc_published", "doc": "plan",
        "version": 1, "previous_version": null, "actor_id": "agent-1", "actor_name": "writer",
        "title": "Plan", "mime": "text/plain", "body": {"mime": "text/plain", "text": "v1"},
        "blob": null, "lease_id": null, "fence": null, "expires_at_ms": null,
        "lease_ttl_s": null, "reason": null, "truncated_bytes": null,
    });
    let queue = json!({
        "seq": 2, "prev_hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        "ts": "2026-10-20T00:00:00Z", "kind": "task_enqueued", "queue": "jobs", "task_id": "task-1",
        "payload": {"doc": "plan@1"}, "max_attempts": 5, "attempts": 0, "attempt": null,
        "lease_id": null, "agent_id": null, "agent_name": null, "expires_at_ms": null,
        "lease_ttl_s": null, "outcome": null, "receipt": null, "reason": null,
        "history": null, "truncated_bytes": null,
    });
    let typed_doc: DocEvent = serde_json::from_value(document.clone()).unwrap();
    let typed_queue: QueueEvent = serde_json::from_value(queue.clone()).unwrap();
    assert!(typed_doc.notification_intent.is_none());
    assert!(typed_queue.notification_intent.is_none());
    assert_eq!(serde_json::to_value(&typed_doc).unwrap(), document);
    assert_eq!(serde_json::to_value(&typed_queue).unwrap(), queue);
    assert_eq!(
        hash_record(&typed_doc).unwrap(),
        hash_record(&document).unwrap()
    );
    assert_eq!(
        hash_record(&typed_queue).unwrap(),
        hash_record(&queue).unwrap()
    );
}

#[tokio::test]
async fn disabled_source_writes_keep_the_historical_shape() {
    let temp = common::TempDir::new("arrivals-disabled-source");
    let daemon = state(temp.path(), false);
    let author = agent(&daemon, "author").await;
    call(
        &daemon,
        "publish_doc",
        json!({"token": author["token"], "name": "plan", "text": "one"}),
    )
    .await
    .unwrap();
    call(
        &daemon,
        "update_doc",
        json!({"token": author["token"], "name": "plan", "base_version": 1, "text": "two"}),
    )
    .await
    .unwrap();
    call(
        &daemon,
        "create_queue",
        json!({"token": author["token"], "queue": "jobs"}),
    )
    .await
    .unwrap();
    call(
        &daemon,
        "enqueue",
        json!({"token": author["token"], "queue": "jobs", "payload": {}}),
    )
    .await
    .unwrap();
    for relative in ["docs/plan.jsonl", "queues/jobs.jsonl"] {
        for line in fs::read_to_string(temp.path().join(relative))
            .unwrap()
            .lines()
        {
            let event: Value = serde_json::from_str(line).unwrap();
            assert!(event.get("notification_intent").is_none());
        }
    }
}

#[tokio::test]
async fn document_intent_is_immutable_and_catch_up_pins_content_across_replay() {
    let temp = common::TempDir::new("arrivals-document-source");
    let mut daemon = state(temp.path(), true);
    let author = agent(&daemon, "author").await;
    let reader = agent(&daemon, "reader").await;
    let subscribed = subscribe(
        &daemon,
        &reader["token"],
        "future",
        "document",
        "plan",
        true,
    )
    .await;
    assert!(subscribed["initial_notice"].is_null());
    assert!(subscribed["boundary"].is_null());
    call(
        &daemon,
        "publish_doc",
        json!({"token": author["token"], "name": "plan", "text": "one"}),
    )
    .await
    .unwrap();
    call(
        &daemon,
        "unsubscribe_arrivals",
        json!({"token": reader["token"], "subscription_id": subscribed["subscription_id"]}),
    )
    .await
    .unwrap();
    call(
        &daemon,
        "update_doc",
        json!({"token": author["token"], "name": "plan", "base_version": 1, "text": "two"}),
    )
    .await
    .unwrap();
    // A lease event follows the content change. Catch-up must still pin the
    // exact document version record, including after runtime reconstruction.
    call(
        &daemon,
        "claim_doc",
        json!({"token": author["token"], "name": "plan", "lease_ttl_s": 60}),
    )
    .await
    .unwrap();
    drop(daemon);
    daemon = state(temp.path(), false);
    let catch_up = subscribe(
        &daemon,
        &reader["token"],
        "current",
        "document",
        "plan",
        true,
    )
    .await;

    let events = LogFile::<DocEvent>::read_segments(temp.path().join("docs/plan.jsonl"))
        .unwrap()
        .records;
    let published = events
        .iter()
        .find(|event| event.kind == "doc_published")
        .unwrap();
    let updated = events
        .iter()
        .find(|event| event.kind == "doc_updated")
        .unwrap();
    let route = &published.notification_intent.as_ref().unwrap().routes;
    assert_eq!(route.len(), 1);
    assert_eq!(route[0].agent_id, reader["agent_id"]);
    assert_eq!(
        route[0].subscription_ids,
        vec![subscribed["subscription_id"].as_str().unwrap()]
    );
    assert!(updated.notification_intent.is_none());
    let snapshot = &catch_up["initial_notice"];
    assert!(snapshot.is_object());
    assert_eq!(
        snapshot["resource"],
        json!({"type": "document", "name": "plan", "version": 2})
    );
    assert_eq!(snapshot["source"]["seq"], updated.seq);
    assert_eq!(snapshot["source"]["hash"], hash_record(updated).unwrap());
    assert_eq!(snapshot["kind"], "doc.updated");
}

#[tokio::test]
async fn queue_intent_covers_enqueue_failure_and_forced_expiry_without_granting_ownership() {
    let temp = common::TempDir::new("arrivals-queue-source");
    let daemon = state(temp.path(), true);
    let author = agent(&daemon, "author").await;
    let reader = agent(&daemon, "reader").await;
    subscribe(&daemon, &reader["token"], "future", "queue", "jobs", false).await;
    call(
        &daemon,
        "create_queue",
        json!({"token": author["token"], "queue": "jobs"}),
    )
    .await
    .unwrap();
    call(
        &daemon,
        "enqueue",
        json!({"token": author["token"], "queue": "jobs", "payload": {}, "max_attempts": 5}),
    )
    .await
    .unwrap();
    let first = call(
        &daemon,
        "claim_task",
        json!({"token": author["token"], "queue": "jobs", "lease_ttl_s": 60}),
    )
    .await
    .unwrap();
    call(&daemon, "fail_task", json!({"token": author["token"], "lease_id": first["lease_id"], "fence": first["fence"], "reason": "retry"}))
        .await.unwrap();
    let second = call(
        &daemon,
        "claim_task",
        json!({"token": author["token"], "queue": "jobs", "lease_ttl_s": 60}),
    )
    .await
    .unwrap();
    assert!(second["fence"].as_u64().unwrap() > first["fence"].as_u64().unwrap());
    assert_eq!(daemon.debug_force_expire_queue("jobs").await.unwrap(), 1);
    let events = LogFile::<QueueEvent>::read_segments(temp.path().join("queues/jobs.jsonl"))
        .unwrap()
        .records;
    let ready: Vec<_> = events
        .iter()
        .filter(|event| matches!(event.kind.as_str(), "task_enqueued" | "task_requeued"))
        .collect();
    assert_eq!(ready.len(), 3);
    for event in ready {
        let routes = &event
            .notification_intent
            .as_ref()
            .expect("durable routes")
            .routes;
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].agent_id, reader["agent_id"]);
    }
    for event in events
        .iter()
        .filter(|event| !matches!(event.kind.as_str(), "task_enqueued" | "task_requeued"))
    {
        assert!(event.notification_intent.is_none());
    }
    // The notification recipient has no lease until it claims normally.
    let third = call(
        &daemon,
        "claim_task",
        json!({"token": reader["token"], "queue": "jobs", "lease_ttl_s": 60}),
    )
    .await
    .unwrap();
    assert_eq!(third["task"]["attempts"], 3);
    assert!(third["fence"].as_u64().unwrap() > second["fence"].as_u64().unwrap());
}

#[tokio::test]
async fn queue_catch_up_is_one_current_hint_and_empty_queues_have_none() {
    let temp = common::TempDir::new("arrivals-queue-snapshot");
    let mut daemon = state(temp.path(), true);
    let author = agent(&daemon, "author").await;
    let reader = agent(&daemon, "reader").await;
    call(
        &daemon,
        "create_queue",
        json!({"token": author["token"], "queue": "jobs"}),
    )
    .await
    .unwrap();
    let empty = subscribe(&daemon, &reader["token"], "empty", "queue", "jobs", true).await;
    assert!(empty["initial_notice"].is_null());
    assert_eq!(empty["boundary"]["seq"], 1);
    assert_eq!(empty["boundary"]["stream"], "queue");
    for value in [1, 2] {
        call(
            &daemon,
            "enqueue",
            json!({"token": author["token"], "queue": "jobs", "payload": value}),
        )
        .await
        .unwrap();
    }
    let claimed = call(
        &daemon,
        "claim_task",
        json!({"token": author["token"], "queue": "jobs", "lease_ttl_s": 60}),
    )
    .await
    .unwrap();
    drop(daemon);
    daemon = state(temp.path(), false);
    let catch_up = subscribe(&daemon, &reader["token"], "current", "queue", "jobs", true).await;
    let events = LogFile::<QueueEvent>::read_segments(temp.path().join("queues/jobs.jsonl"))
        .unwrap()
        .records;
    let last = events.last().unwrap();
    assert_eq!(last.seq, claimed["fence"]);
    let snapshot = &catch_up["initial_notice"];
    assert!(snapshot.is_object());
    assert_eq!(
        snapshot["resource"],
        json!({"type": "queue", "name": "jobs", "cause": "current", "ready_count": 1})
    );
    assert_eq!(snapshot["source"]["seq"], last.seq);
    assert_eq!(snapshot["source"]["hash"], hash_record(last).unwrap());
}

#[tokio::test]
async fn on_demand_and_sweeper_expiry_capture_queue_arrival_routes() {
    let temp = common::TempDir::new("arrivals-queue-expiry");
    let daemon = state(temp.path(), true);
    let author = agent(&daemon, "author").await;
    let reader = agent(&daemon, "reader").await;
    let mut claims = std::collections::HashMap::new();
    for queue in ["ondemand", "swept", "renewed", "completed", "failed"] {
        subscribe(&daemon, &reader["token"], queue, "queue", queue, false).await;
        call(
            &daemon,
            "create_queue",
            json!({"token": author["token"], "queue": queue}),
        )
        .await
        .unwrap();
        call(
            &daemon,
            "enqueue",
            json!({"token": author["token"], "queue": queue, "payload": {}}),
        )
        .await
        .unwrap();
        let claim = call(
            &daemon,
            "claim_task",
            json!({"token": author["token"], "queue": queue, "lease_ttl_s": 1}),
        )
        .await
        .unwrap();
        claims.insert(queue, claim);
    }
    tokio::time::sleep(Duration::from_millis(1100)).await;
    call(
        &daemon,
        "claim_task",
        json!({"token": author["token"], "queue": "ondemand", "lease_ttl_s": 60}),
    )
    .await
    .unwrap();
    // These APIs expire the lease before rejecting the stale operation. The
    // preceding committed requeue still owes its captured arrival notice.
    for (queue, method) in [
        ("renewed", "renew_lease"),
        ("completed", "complete_task"),
        ("failed", "fail_task"),
    ] {
        let claim = &claims[queue];
        let error = call(
            &daemon,
            method,
            json!({
                "token": author["token"],
                "lease_id": claim["lease_id"],
                "fence": claim["fence"],
                "outcome": {},
                "reason": "stale failure",
            }),
        )
        .await
        .expect_err("expired operation loses the lease");
        assert_eq!(error.data.unwrap()["code"], "LEASE_LOST");
    }
    assert_eq!(daemon.sweep_expired_queues().await.unwrap(), 1);
    for queue in ["ondemand", "swept", "renewed", "completed", "failed"] {
        let events =
            LogFile::<QueueEvent>::read_segments(temp.path().join(format!("queues/{queue}.jsonl")))
                .unwrap()
                .records;
        let requeue = events
            .iter()
            .find(|event| event.kind == "task_requeued")
            .unwrap();
        assert_eq!(
            requeue.notification_intent.as_ref().unwrap().routes[0].agent_id,
            reader["agent_id"]
        );
    }
}

#[cfg(feature = "mbt-injection")]
#[tokio::test]
async fn uncertain_subscription_commit_fences_document_and_queue_arrivals_until_reopen() {
    use cooperators::faults::{arm_append, AppendFault};

    let temp = common::TempDir::new("arrivals-source-subscription-uncertain");
    let mut daemon = state(temp.path(), true);
    let author = agent(&daemon, "author").await;
    let reader = agent(&daemon, "reader").await;
    call(
        &daemon,
        "publish_doc",
        json!({"token": author["token"], "name": "plan", "text": "one"}),
    )
    .await
    .unwrap();
    call(
        &daemon,
        "create_queue",
        json!({"token": author["token"], "queue": "jobs"}),
    )
    .await
    .unwrap();
    let doc_before = fs::read(temp.path().join("docs/plan.jsonl")).unwrap();
    let queue_before = fs::read(temp.path().join("queues/jobs.jsonl")).unwrap();
    let fault = arm_append(
        temp.path().join("notifications/subscriptions.jsonl"),
        AppendFault::FailSync,
    )
    .unwrap();
    let failed = call(
        &daemon,
        "subscribe_arrivals",
        json!({
            "token": reader["token"],
            "client_key": "uncertain",
            "resource": {"type": "document", "name": "plan"},
            "event_kinds": ["doc.published", "doc.updated"],
            "catch_up": true,
        }),
    )
    .await;
    assert!(
        failed.is_err(),
        "sync failure cannot return a subscription receipt"
    );
    assert!(!fault.observation().sync_succeeded);
    for (method, params) in [
        (
            "publish_doc",
            json!({"token": author["token"], "name": "new-plan", "text": "new"}),
        ),
        (
            "update_doc",
            json!({"token": author["token"], "name": "plan", "base_version": 1, "text": "two"}),
        ),
        (
            "enqueue",
            json!({"token": author["token"], "queue": "jobs", "payload": {}}),
        ),
    ] {
        assert!(
            call(&daemon, method, params).await.is_err(),
            "an uncertain subscription must fence source routing for {method}"
        );
    }
    assert_eq!(
        fs::read(temp.path().join("docs/plan.jsonl")).unwrap(),
        doc_before
    );
    assert_eq!(
        fs::read(temp.path().join("queues/jobs.jsonl")).unwrap(),
        queue_before
    );
    let unpublished =
        LogFile::<DocEvent>::read_segments(temp.path().join("docs/new-plan.jsonl")).unwrap();
    assert!(unpublished.records.is_empty());
    drop(fault);
    drop(daemon);
    daemon = state(temp.path(), false);
    call(
        &daemon,
        "update_doc",
        json!({"token": author["token"], "name": "plan", "base_version": 1, "text": "two"}),
    )
    .await
    .expect("validated recovered subscription restores route capture");
    call(
        &daemon,
        "enqueue",
        json!({"token": author["token"], "queue": "jobs", "payload": {}}),
    )
    .await
    .expect("source mutations resume after subscription recovery");
    let document = LogFile::<DocEvent>::read_segments(temp.path().join("docs/plan.jsonl")).unwrap();
    assert_eq!(
        document
            .records
            .last()
            .unwrap()
            .notification_intent
            .as_ref()
            .unwrap()
            .routes[0]
            .agent_id,
        reader["agent_id"]
    );
}
