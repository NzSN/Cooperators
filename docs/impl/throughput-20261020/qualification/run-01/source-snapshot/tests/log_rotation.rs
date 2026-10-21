mod common;

use std::fs;
use std::path::Path;
use std::time::Duration;

use cooperators::channels::ChannelCursor;
use cooperators::docs::DocEvent;
use cooperators::envelope::{Body, Envelope};
use cooperators::queues::QueueEvent;
use cooperators::server::{DaemonConfig, DaemonState};
use cooperators::storage::{hash_record, segment_paths, ChainRecord, LogFile, GENESIS_HASH};
use serde::Serialize;
use serde_json::json;

fn state(root: &Path) -> DaemonState {
    let mut config = DaemonConfig::new(root);
    config.log_rotation_bytes = 1024;
    DaemonState::with_config(config).unwrap()
}
async fn agent(state: &DaemonState) -> cooperators::registry::AgentIdentity {
    let registration = state
        .register_agent("rotation-test".into(), Some("codex".into()), None, None)
        .await
        .unwrap();
    state.authenticate(&registration.token).await.unwrap()
}
fn body(n: usize) -> Body {
    Body::new("text/plain", format!("{n}:{}", "x".repeat(512)))
}
fn assert_chain<T: ChainRecord + Serialize>(records: &[T]) {
    let mut previous = GENESIS_HASH.to_string();
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.seq(), index as u64 + 1);
        assert_eq!(record.prev_hash(), previous);
        previous = hash_record(record).unwrap();
    }
}

fn seal_active<T: ChainRecord + Serialize + serde::de::DeserializeOwned>(path: &Path) {
    let report = LogFile::<T>::read_segments(path).unwrap();
    let last = report.records.last().unwrap();
    let first: serde_json::Value =
        serde_json::from_str(fs::read_to_string(path).unwrap().lines().next().unwrap()).unwrap();
    let directory = path
        .parent()
        .unwrap()
        .join(".segments")
        .join(path.file_name().unwrap());
    fs::create_dir_all(&directory).unwrap();
    let target = directory.join(format!(
        "{:020}-{:020}-{}.jsonl",
        first["seq"].as_u64().unwrap(),
        last.seq(),
        hash_record(last).unwrap().strip_prefix("sha256:").unwrap()
    ));
    fs::rename(path, target).unwrap();
}

#[tokio::test]
async fn channel_cursor_and_dedupe_survive_rotation_and_restart() {
    let temp = common::TempDir::new("channel-rotation");
    let mut daemon = state(temp.path());
    let identity = agent(&daemon).await;
    let channel = "channel.v1";
    for n in 0..8 {
        assert_eq!(
            daemon
                .publish(&identity, channel, body(n), Some(format!("key-{n}")))
                .await
                .unwrap()
                .seq,
            n as u64 + 1
        );
    }
    let cursor = ChannelCursor::new(channel, 3);
    let path = temp.path().join("channels/channel.v1.jsonl");
    assert!(segment_paths(&path).unwrap().len() > 1);
    drop(daemon);
    daemon = state(temp.path());
    let batch = daemon
        .read_channel(channel, Some(cursor), 100, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(
        batch.messages.iter().map(|m| m.seq).collect::<Vec<_>>(),
        (4..=8).collect::<Vec<_>>()
    );
    assert_eq!(batch.next_cursor.file, "channels/channel.v1.jsonl");
    assert_eq!(batch.next_cursor.seq, 8);
    let duplicate = daemon
        .publish(&identity, channel, body(0), Some("key-0".into()))
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.seq, 1);
    assert_eq!(
        daemon
            .publish(&identity, channel, body(8), Some("key-8".into()))
            .await
            .unwrap()
            .seq,
        9
    );
    let records = LogFile::<Envelope>::read_segments(&path).unwrap().records;
    assert_eq!(records.len(), 9);
    assert_chain(&records);
    assert_eq!(
        cooperators::status::scan_seq_heads(temp.path())["channels/channel.v1.jsonl"].seq,
        9
    );
}

#[tokio::test]
async fn new_system_event_channel_uses_the_configured_rotation_threshold() {
    let temp = common::TempDir::new("system-event-rotation");
    let daemon = state(temp.path());
    for n in 0..4 {
        daemon
            .append_channel_event("events.v1", "daemon:cooperators", "event", body(n))
            .await
            .unwrap();
    }
    let path = temp.path().join("channels/events.v1.jsonl");
    assert!(segment_paths(&path).unwrap().len() > 1);
    let records = LogFile::<Envelope>::read_segments(&path).unwrap().records;
    assert_eq!(records.len(), 4);
    assert_chain(&records);
}

#[tokio::test]
async fn queue_lease_fences_survive_dotted_log_rotation_and_restart() {
    let temp = common::TempDir::new("queue-rotation");
    let mut daemon = state(temp.path());
    let identity = agent(&daemon).await;
    let queue = "queue.v1";
    daemon.create_queue(queue).await.unwrap();
    daemon
        .enqueue(queue, json!({"body":"x".repeat(512)}), Some(2))
        .await
        .unwrap();
    let first = daemon.claim_task(&identity, queue, 3600).await.unwrap();
    drop(daemon);
    daemon = state(temp.path());
    daemon
        .complete_task(
            &identity,
            &first.lease_id,
            first.fence,
            json!({"ok":1}),
            None,
        )
        .await
        .unwrap();
    daemon
        .enqueue(queue, json!({"body":"y".repeat(512)}), Some(2))
        .await
        .unwrap();
    let second = daemon.claim_task(&identity, queue, 3600).await.unwrap();
    assert!(second.fence > first.fence);
    assert!(daemon
        .complete_task(
            &identity,
            &first.lease_id,
            first.fence,
            json!({"stale":true}),
            None,
        )
        .await
        .is_err());
    daemon
        .complete_task(
            &identity,
            &second.lease_id,
            second.fence,
            json!({"ok":2}),
            None,
        )
        .await
        .unwrap();
    let path = temp.path().join("queues/queue.v1.jsonl");
    assert!(segment_paths(&path).unwrap().len() > 1);
    let records = LogFile::<QueueEvent>::read_segments(&path).unwrap().records;
    assert_chain(&records);
    assert_eq!(
        records
            .iter()
            .filter(|record| record.kind == "task_completed")
            .count(),
        2
    );
    assert_eq!(
        cooperators::status::scan_seq_heads(temp.path())["queues/queue.v1.jsonl"].seq,
        records.len() as u64
    );
}

#[tokio::test]
async fn document_versions_and_fences_survive_dotted_log_rotation_and_restart() {
    let temp = common::TempDir::new("doc-rotation");
    let mut daemon = state(temp.path());
    let identity = agent(&daemon).await;
    let name = "document.v1";
    daemon
        .publish_doc(&identity, name, "text/plain", body(1), None)
        .await
        .unwrap();
    daemon
        .update_doc(&identity, name, 1, body(2), None, None)
        .await
        .unwrap();
    let claim = daemon.claim_doc(&identity, name, 3600).await.unwrap();
    daemon
        .update_doc(
            &identity,
            name,
            2,
            body(3),
            None,
            Some((&claim.lease_id, claim.fence)),
        )
        .await
        .unwrap();
    let released = daemon
        .release_doc(&identity, &claim.lease_id, claim.fence)
        .await
        .unwrap();
    drop(daemon);
    daemon = state(temp.path());
    assert_eq!(daemon.read_doc(name, Some(1)).await.unwrap().body, body(1));
    let current = daemon.read_doc(name, None).await.unwrap();
    assert_eq!(current.version, 3);
    assert_eq!(current.fence, released.new_fence);
    assert!(daemon
        .update_doc(
            &identity,
            name,
            3,
            body(4),
            None,
            Some((&claim.lease_id, claim.fence))
        )
        .await
        .is_err());
    let next = daemon
        .update_doc(&identity, name, 3, body(4), None, None)
        .await
        .unwrap();
    assert_eq!(next.version, 4);
    assert!(next.seq > released.seq);
    let path = temp.path().join("docs/document.v1.jsonl");
    assert!(segment_paths(&path).unwrap().len() > 1);
    let records = LogFile::<DocEvent>::read_segments(&path).unwrap().records;
    assert_chain(&records);
    assert_eq!(
        cooperators::status::scan_seq_heads(temp.path())["docs/document.v1.jsonl"].seq,
        next.seq
    );
}

#[test]
fn status_of_sealed_only_log_is_read_only_and_uses_logical_path() {
    let temp = common::TempDir::new("sealed-status");
    let path = temp.path().join("channels/log.v1.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open_rotating(&path, 1024).unwrap();
    for n in 0..3 {
        let mut record = Envelope::new("sender", "log.v1", "test", body(n)).unwrap();
        log.append(&mut record).unwrap();
    }
    drop(log);
    // Preserve the active file as an additional sealed segment, matching the
    // durable rename-before-new-active crash window. Use an actual writer's
    // sealed file name and anchor convention, including dotted log names.
    let report = LogFile::<Envelope>::read_segments(&path).unwrap();
    let active = report.records.last().unwrap();
    let directory = path.parent().unwrap().join(".segments/log.v1.jsonl");
    let target = directory.join(format!(
        "{:020}-{:020}-{}.jsonl",
        active.seq,
        active.seq,
        hash_record(active)
            .unwrap()
            .strip_prefix("sha256:")
            .unwrap()
    ));
    fs::rename(&path, &target).unwrap();
    let files = segment_paths(&path).unwrap();
    let before: Vec<_> = files
        .iter()
        .map(|file| (file.clone(), fs::read(file).unwrap()))
        .collect();
    let heads = cooperators::status::scan_seq_heads(temp.path());
    assert_eq!(heads["channels/log.v1.jsonl"].seq, 3);
    assert!(heads.keys().all(|key| !key.contains(".segments")));
    assert!(
        !path.exists(),
        "status must not create a missing active file"
    );
    for (file, bytes) in before {
        assert_eq!(fs::read(file).unwrap(), bytes);
    }
}

#[tokio::test]
async fn sealed_only_queue_and_document_are_discovered_on_restart() {
    let temp = common::TempDir::new("sealed-only-resources");
    let mut daemon = state(temp.path());
    let identity = agent(&daemon).await;
    daemon.create_queue("queue.v2").await.unwrap();
    daemon
        .enqueue("queue.v2", json!({"body":"x".repeat(512)}), Some(2))
        .await
        .unwrap();
    let task = daemon
        .claim_task(&identity, "queue.v2", 3600)
        .await
        .unwrap();
    daemon
        .publish_doc(&identity, "document.v2", "text/plain", body(1), None)
        .await
        .unwrap();
    let claim = daemon
        .claim_doc(&identity, "document.v2", 3600)
        .await
        .unwrap();
    drop(daemon);
    let queue = temp.path().join("queues/queue.v2.jsonl");
    let document = temp.path().join("docs/document.v2.jsonl");
    seal_active::<QueueEvent>(&queue);
    seal_active::<DocEvent>(&document);
    fs::create_dir_all(temp.path().join("queues/.segments/empty.v2.jsonl")).unwrap();
    fs::create_dir_all(temp.path().join("docs/.segments/empty.v2.jsonl")).unwrap();
    assert!(!queue.exists());
    assert!(!document.exists());
    daemon = state(temp.path());
    assert_eq!(
        daemon
            .read_doc("document.v2", None)
            .await
            .unwrap()
            .head_version,
        1
    );
    let completed = daemon
        .complete_task(
            &identity,
            &task.lease_id,
            task.fence,
            json!({"ok":true}),
            None,
        )
        .await
        .unwrap();
    assert!(completed.seq > task.fence);
    let updated = daemon
        .update_doc(
            &identity,
            "document.v2",
            1,
            body(2),
            None,
            Some((&claim.lease_id, claim.fence)),
        )
        .await
        .unwrap();
    assert_eq!(updated.version, 2);
    assert!(updated.seq > claim.fence);
    // Empty metadata directories must not create phantom resource names.
    daemon.create_queue("empty.v2").await.unwrap();
    daemon
        .publish_doc(&identity, "empty.v2", "text/plain", body(1), None)
        .await
        .unwrap();
    assert_chain(
        &LogFile::<QueueEvent>::read_segments(&queue)
            .unwrap()
            .records,
    );
    assert_chain(
        &LogFile::<DocEvent>::read_segments(&document)
            .unwrap()
            .records,
    );
}
