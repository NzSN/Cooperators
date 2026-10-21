#![cfg(feature = "mbt-injection")]

mod common;

use std::fs;
use std::time::Duration;

use cooperators::envelope::Body;
use cooperators::faults::{self, FaultPoint, PointFault};
use cooperators::messaging::SendRequest;
use cooperators::server::{DaemonConfig, DaemonState};
use serde_json::{json, Value};

async fn identity(state: &DaemonState) -> cooperators::registry::AgentIdentity {
    let registration = state
        .register_agent("runtime-test".into(), Some("codex".into()), None, None)
        .await
        .unwrap();
    state.authenticate(&registration.token).await.unwrap()
}

fn send(to: &str, ttl: Option<Duration>) -> SendRequest {
    SendRequest {
        to: to.into(),
        kind: "test".into(),
        body: Body::new("text/plain", "message"),
        reply_to: None,
        idempotency_key: None,
        ttl,
    }
}

fn rows(path: &std::path::Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn expired_inbox_returns_acknowledgeable_progress_after_restart() {
    let temp = common::TempDir::new("expired-cursor");
    let clock = faults::set_clock(temp.path(), 1000).unwrap();
    let mut config = DaemonConfig::new(temp.path());
    config.max_inbox_messages = 2;
    config.wait_batch_limit = 1;
    let state = DaemonState::with_config(config.clone()).unwrap();
    let agent = identity(&state).await;
    for _ in 0..2 {
        state
            .send_message(&agent, send(&agent.name, Some(Duration::from_millis(10))))
            .await
            .unwrap();
    }
    clock.advance(10);
    drop(state);
    let state = DaemonState::with_config(config).unwrap();
    let batch = state
        .wait_message(&agent.agent_id, None, Duration::ZERO)
        .await
        .unwrap();
    assert!(batch.messages.is_empty());
    assert_eq!(batch.next_cursor, "2");
    state
        .ack(&agent.agent_id, &batch.next_cursor)
        .await
        .unwrap();
    assert_eq!(
        state.debug_inbox_backlog(&agent.agent_id).await.unwrap(),
        (0, 0)
    );
    assert_eq!(
        state
            .send_message(&agent, send(&agent.name, None))
            .await
            .unwrap()
            .seq,
        3
    );
}

#[tokio::test]
async fn batch_cursor_never_skips_an_unreturned_live_message() {
    let temp = common::TempDir::new("batch-cursor");
    let clock = faults::set_clock(temp.path(), 1000).unwrap();
    let mut config = DaemonConfig::new(temp.path());
    config.wait_batch_limit = 1;
    let state = DaemonState::with_config(config).unwrap();
    let agent = identity(&state).await;
    for ttl in [
        Some(Duration::from_millis(10)),
        None,
        None,
        Some(Duration::from_millis(10)),
    ] {
        state
            .send_message(&agent, send(&agent.name, ttl))
            .await
            .unwrap();
    }
    clock.advance(10);
    for seq in [2, 3] {
        let batch = state
            .wait_message(&agent.agent_id, None, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            batch.messages.iter().map(|m| m.seq).collect::<Vec<_>>(),
            vec![seq]
        );
        assert_eq!(batch.next_cursor, seq.to_string());
        state
            .ack(&agent.agent_id, &batch.next_cursor)
            .await
            .unwrap();
    }
    let batch = state
        .wait_message(&agent.agent_id, None, Duration::ZERO)
        .await
        .unwrap();
    assert!(batch.messages.is_empty());
    assert_eq!(batch.next_cursor, "4");
    state
        .ack(&agent.agent_id, &batch.next_cursor)
        .await
        .unwrap();
    assert_eq!(
        state.debug_inbox_backlog(&agent.agent_id).await.unwrap(),
        (0, 0)
    );
}

#[tokio::test]
async fn missing_dead_letter_is_reconciled_once_from_durable_terminal_event() {
    let temp = common::TempDir::new("dead-projection");
    let state = DaemonState::with_config(DaemonConfig::new(temp.path())).unwrap();
    let agent = identity(&state).await;
    state.create_queue("queue").await.unwrap();
    let task = state
        .enqueue("queue", json!({"payload":"retained"}), Some(1))
        .await
        .unwrap();
    let claim = state.claim_task(&agent, "queue", 60).await.unwrap();
    let path = temp.path().join("dead/queue.jsonl");
    let hook =
        faults::arm_point(&path, FaultPoint::BeforeDeadProjection, PointFault::Fail).unwrap();
    assert!(state
        .fail_task(&agent, &claim.lease_id, claim.fence, "failure")
        .await
        .is_err());
    assert_eq!(
        hook.observation().stage,
        faults::FaultStage::InjectedFailure
    );
    assert!(!path.exists());
    drop(state);
    let state = DaemonState::with_config(DaemonConfig::new(temp.path())).unwrap();
    state.sweep_expired_queues().await.unwrap();
    let first = fs::read(&path).unwrap();
    let records = rows(&path);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["task_id"], task.task_id);
    assert_eq!(records[0]["payload"], json!({"payload":"retained"}));
    assert_eq!(records[0]["history"][0]["fence"], claim.fence);
    state.sweep_expired_queues().await.unwrap();
    assert_eq!(
        fs::read(&path).unwrap(),
        first,
        "reconciliation must preserve the existing record bytes"
    );
    drop(state);
    let state = DaemonState::with_config(DaemonConfig::new(temp.path())).unwrap();
    state.sweep_expired_queues().await.unwrap();
    assert_eq!(fs::read(path).unwrap(), first);
}

#[tokio::test]
async fn failed_initial_document_creation_can_retry_before_and_after_restart() {
    for restart in [false, true] {
        let temp = common::TempDir::new("doc-create");
        let mut state = DaemonState::with_config(DaemonConfig::new(temp.path())).unwrap();
        let agent = identity(&state).await;
        let path = temp.path().join("docs/document.jsonl");
        let hook = faults::arm_point(&path, FaultPoint::AfterDocCreate, PointFault::Fail).unwrap();
        assert!(state
            .publish_doc(
                &agent,
                "document",
                "text/plain",
                Body::new("text/plain", "failed"),
                None
            )
            .await
            .is_err());
        assert_eq!(
            hook.observation().stage,
            faults::FaultStage::InjectedFailure
        );
        assert!(path.exists());
        assert!(rows(&path).is_empty());
        if restart {
            drop(state);
            state = DaemonState::with_config(DaemonConfig::new(temp.path())).unwrap();
        }
        let receipt = state
            .publish_doc(
                &agent,
                "document",
                "text/plain",
                Body::new("text/plain", "committed"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(receipt.version, 1);
        assert_eq!(
            state.read_doc("document", None).await.unwrap().body.text,
            "committed"
        );
        assert_eq!(
            rows(&path)
                .iter()
                .filter(|r| r["kind"] == "doc_published")
                .count(),
            1
        );
        assert!(state
            .publish_doc(
                &agent,
                "document",
                "text/plain",
                Body::new("text/plain", "duplicate"),
                None
            )
            .await
            .is_err());
    }
}

#[test]
fn status_handles_large_final_records_and_does_not_modify_tails() {
    let temp = common::TempDir::new("large-status");
    fs::create_dir_all(temp.path().join("channels")).unwrap();
    let path = temp.path().join("channels/large.jsonl");
    let contents = format!(
        "{}\n{}\n",
        json!({"seq":1}),
        json!({"seq":2,"body":"x".repeat(128*1024)})
    );
    fs::write(&path, &contents).unwrap();
    let heads = cooperators::status::scan_seq_heads(temp.path());
    assert_eq!(heads["channels/large.jsonl"].seq, 2);
    assert_eq!(fs::read(&path).unwrap(), contents.as_bytes());
    let torn = format!(
        "{contents}{{\"seq\":3,\"body\":\"{}",
        "y".repeat(128 * 1024)
    );
    fs::write(&path, &torn).unwrap();
    assert_eq!(
        cooperators::status::scan_seq_heads(temp.path())["channels/large.jsonl"].seq,
        2
    );
    assert_eq!(fs::read(path).unwrap(), torn.as_bytes());
}
