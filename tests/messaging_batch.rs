mod common;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cooperators::envelope::{Body, Envelope};
use cooperators::error::{BrokerError, ErrorCode};
use cooperators::messaging::{SendReceipt, SendRequest};
use cooperators::registry::AgentIdentity;
use cooperators::server::{DaemonConfig, DaemonState};
use cooperators::storage::{hash_record, LogFile, GENESIS_HASH};
use tokio::sync::Barrier;

fn config(root: &Path) -> DaemonConfig {
    let mut config = DaemonConfig::new(root);
    config.write_batch_delay = Duration::from_millis(20);
    config
}

async fn identity(state: &DaemonState, name: &str) -> AgentIdentity {
    let registration = state
        .register_agent(name.into(), Some("test".into()), None, None)
        .await
        .expect("register test agent");
    state
        .authenticate(&registration.token)
        .await
        .expect("authenticate test agent")
}

fn request(to: &str, text: &str, key: Option<&str>) -> SendRequest {
    SendRequest {
        to: to.into(),
        kind: "event".into(),
        body: Body::new("text/plain", text),
        reply_to: None,
        idempotency_key: key.map(str::to_string),
        ttl: None,
    }
}

fn inbox_path(root: &Path, recipient: &AgentIdentity) -> PathBuf {
    root.join("agents")
        .join(&recipient.agent_id)
        .join("inbox.jsonl")
}

fn records(root: &Path, recipient: &AgentIdentity) -> Vec<Envelope> {
    let replay =
        LogFile::<Envelope>::read_segments(inbox_path(root, recipient)).expect("read inbox chain");
    assert_eq!(
        replay.truncated_bytes, 0,
        "successful sends leave clean logs"
    );
    replay.records
}

fn assert_full(error: BrokerError) {
    match error {
        BrokerError::Application(ErrorCode::ResourceExhausted, _) => {}
        other => panic!("expected capacity rejection, got {other:?}"),
    }
}

async fn concurrent_sends(
    state: &Arc<DaemonState>,
    sender: &AgentIdentity,
    requests: Vec<SendRequest>,
) -> Vec<Result<SendReceipt, BrokerError>> {
    let barrier = Arc::new(Barrier::new(requests.len() + 1));
    let mut workers = Vec::new();
    for request in requests {
        let state = Arc::clone(state);
        let sender = sender.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            state.send_message(&sender, request).await
        }));
    }
    barrier.wait().await;
    let mut results = Vec::new();
    for worker in workers {
        results.push(worker.await.expect("sender task"));
    }
    results
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inbox_sends_have_unique_receipts_and_a_replayable_ordered_chain() {
    let temp = common::TempDir::new("batch-order");
    let state = Arc::new(DaemonState::with_config(config(temp.path())).unwrap());
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let requests = (0..48)
        .map(|index| {
            request(
                &recipient.name,
                &format!("message-{index}"),
                Some(&format!("key-{index}")),
            )
        })
        .collect();
    let mut receipts = concurrent_sends(&state, &sender, requests)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    receipts.sort_by_key(|receipt| receipt.seq);
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.seq)
            .collect::<Vec<_>>(),
        (1..=48).collect::<Vec<_>>()
    );
    assert!(receipts.iter().all(|receipt| !receipt.duplicate));
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| &receipt.message_id)
            .collect::<HashSet<_>>()
            .len(),
        48
    );

    let persisted = records(temp.path(), &recipient);
    assert_eq!(persisted.len(), receipts.len());
    let mut previous_hash = GENESIS_HASH.to_string();
    for (record, receipt) in persisted.iter().zip(&receipts) {
        assert_eq!(record.id, receipt.message_id);
        assert_eq!(record.seq, receipt.seq);
        assert_eq!(record.prev_hash, previous_hash);
        assert_eq!(record.idempotency_key, receipt.idempotency_key);
        previous_hash = hash_record(record).unwrap();
    }
    let delivered = state
        .wait_message(&recipient.agent_id, Some("0"), Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(delivered.messages, persisted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_key_retries_across_inboxes_commit_one_message() {
    let temp = common::TempDir::new("batch-dedupe");
    let state = Arc::new(DaemonState::with_config(config(temp.path())).unwrap());
    let sender = identity(&state, "sender").await;
    let first = identity(&state, "first").await;
    let second = identity(&state, "second").await;
    let requests = (0..24)
        .map(|index| {
            let to = if index % 2 == 0 { &first } else { &second };
            request(&to.name, "same operation", Some("shared-key"))
        })
        .collect();
    let receipts = concurrent_sends(&state, &sender, requests)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    assert_eq!(
        receipts.iter().filter(|receipt| !receipt.duplicate).count(),
        1
    );
    let original = receipts.iter().find(|receipt| !receipt.duplicate).unwrap();
    for receipt in &receipts {
        assert_eq!(receipt.message_id, original.message_id);
        assert_eq!(receipt.seq, original.seq);
        assert_eq!(receipt.to, original.to);
        assert_eq!(receipt.to_agent_id, original.to_agent_id);
    }
    let mut persisted = records(temp.path(), &first);
    persisted.extend(records(temp.path(), &second));
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].id, original.message_id);
}

#[tokio::test]
async fn retry_of_committed_key_succeeds_when_inbox_is_full() {
    let temp = common::TempDir::new("batch-full-dedupe");
    let mut config = config(temp.path());
    config.max_inbox_messages = 1;
    let state = DaemonState::with_config(config).unwrap();
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let first = state
        .send_message(&sender, request(&recipient.name, "original", Some("one")))
        .await
        .unwrap();
    assert_full(
        state
            .send_message(&sender, request(&recipient.name, "overflow", None))
            .await
            .unwrap_err(),
    );
    let duplicate = state
        .send_message(&sender, request(&recipient.name, "retry", Some("one")))
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.message_id, first.message_id);
    assert_eq!(records(temp.path(), &recipient).len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admissions_respect_inbox_message_capacity() {
    let temp = common::TempDir::new("batch-message-cap");
    let mut config = config(temp.path());
    config.max_inbox_messages = 3;
    let state = Arc::new(DaemonState::with_config(config).unwrap());
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    let outcomes = concurrent_sends(
        &state,
        &sender,
        (0..12)
            .map(|_| request(&recipient.name, "message", None))
            .collect(),
    )
    .await;
    let mut accepted = 0;
    for outcome in outcomes {
        match outcome {
            Ok(_) => accepted += 1,
            Err(error) => assert_full(error),
        }
    }
    assert_eq!(accepted, 3);
    let persisted = records(temp.path(), &recipient);
    assert_eq!(persisted.len(), 3);
    state.ack(&recipient.agent_id, "3").await.unwrap();
    let next = state
        .send_message(&sender, request(&recipient.name, "after ack", None))
        .await
        .unwrap();
    assert_eq!(next.seq, 4, "ack releases only the admitted backlog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admissions_respect_exact_serialized_inbox_byte_capacity() {
    let temp = common::TempDir::new("batch-byte-cap");
    // Routing names, IDs, timestamps, hash links and one-digit sequence numbers
    // have fixed serialized lengths. The first run obtains those routing names
    // before reopening the same logs under a precisely chosen byte limit.
    let initial = DaemonState::with_config(config(temp.path())).unwrap();
    let sender = identity(&initial, "sender").await;
    let recipient = identity(&initial, "recipient").await;
    let mut template = Envelope::new(
        &sender.name,
        &recipient.name,
        "event",
        Body::new("text/plain", "fixed payload"),
    )
    .unwrap();
    template.seq = 1;
    let line_bytes = serde_json::to_vec(&template).unwrap().len() as u64 + 1;
    drop(initial);
    let mut config = config(temp.path());
    config.max_inbox_bytes = line_bytes * 3 - 1;
    let cap = config.max_inbox_bytes;
    let state = Arc::new(DaemonState::with_config(config).unwrap());
    let outcomes = concurrent_sends(
        &state,
        &sender,
        (0..8)
            .map(|_| request(&recipient.name, "fixed payload", None))
            .collect(),
    )
    .await;
    let mut accepted = 0;
    for outcome in outcomes {
        match outcome {
            Ok(_) => accepted += 1,
            Err(error) => assert_full(error),
        }
    }
    assert_eq!(accepted, 2);
    let persisted = records(temp.path(), &recipient);
    let actual_bytes = persisted
        .iter()
        .map(|record| serde_json::to_vec(record).unwrap().len() as u64 + 1)
        .sum::<u64>();
    assert_eq!(actual_bytes, line_bytes * 2);
    assert!(actual_bytes <= cap);
    assert!(actual_bytes + line_bytes > cap);
}

#[tokio::test]
async fn oversized_write_queue_request_is_rejected_without_using_inbox_capacity() {
    let temp = common::TempDir::new("batch-queue-byte-cap");
    let mut config = config(temp.path());
    config.write_queue_bytes = 1_024;
    config.max_inbox_messages = 1;
    let state = DaemonState::with_config(config).unwrap();
    let sender = identity(&state, "sender").await;
    let recipient = identity(&state, "recipient").await;
    assert_full(
        state
            .send_message(&sender, request(&recipient.name, &"x".repeat(2_048), None))
            .await
            .unwrap_err(),
    );
    assert!(records(temp.path(), &recipient).is_empty());
    let accepted = state
        .send_message(&sender, request(&recipient.name, "fits", None))
        .await
        .unwrap();
    assert_eq!(accepted.seq, 1);
    assert_eq!(records(temp.path(), &recipient).len(), 1);
}

#[cfg(feature = "mbt-injection")]
mod faults {
    use super::*;
    use cooperators::faults::{self, AppendFault, FaultStage, SyncOutcome};
    use std::future::{poll_fn, Future};
    use std::task::Poll;

    async fn status(state: &Arc<DaemonState>) -> cooperators::status::StatusResponse {
        let state = Arc::clone(state);
        tokio::time::timeout(
            Duration::from_secs(1),
            tokio::task::spawn_blocking(move || state.status()),
        )
        .await
        .expect("status must not wait for an inbox sync")
        .expect("status worker")
    }

    fn log_key(recipient: &AgentIdentity, file: &str) -> String {
        format!("agents/{}/{file}", recipient.agent_id)
    }

    async fn reopen_owned(config: DaemonConfig) -> DaemonState {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match DaemonState::owned(config.clone()) {
                    Ok(state) => return state,
                    Err(BrokerError::Io(error))
                        if error.kind() == std::io::ErrorKind::WouldBlock =>
                    {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("reopen owned daemon: {error}"),
                }
            }
        })
        .await
        .expect("finished writer releases root ownership")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn paused_sync_hides_message_and_allows_another_inbox_to_commit() {
        paused_sync_preserves_committed_delivery_and_ack(SyncOutcome::Succeed).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failed_sync_preserves_the_prior_ack_without_publishing_provisional_backlog() {
        paused_sync_preserves_committed_delivery_and_ack(SyncOutcome::Fail).await;
    }

    async fn paused_sync_preserves_committed_delivery_and_ack(outcome: SyncOutcome) {
        let temp = common::TempDir::new("batch-independent");
        let state = Arc::new(DaemonState::with_config(config(temp.path())).unwrap());
        let sender = identity(&state, "sender").await;
        let first = identity(&state, "first").await;
        let second = identity(&state, "second").await;
        state
            .send_message(&sender, request(&first.name, "committed seed", None))
            .await
            .unwrap();
        let first_key = log_key(&first, "inbox.jsonl");
        let second_key = log_key(&second, "inbox.jsonl");
        let previous = status(&state).await.seq_heads[&first_key].clone();
        let hook = faults::arm_append(
            inbox_path(temp.path(), &first),
            AppendFault::PauseBeforeSync,
        )
        .unwrap();
        let send_state = Arc::clone(&state);
        let send_identity = sender.clone();
        let first_name = first.name.clone();
        let paused = tokio::spawn(async move {
            send_state
                .send_message(&send_identity, request(&first_name, "paused", None))
                .await
        });
        let observed = hook.wait_reached(Duration::from_secs(2)).unwrap();
        assert_eq!(observed.stage, FaultStage::BeforeSync);
        assert!(!paused.is_finished(), "receipt must wait for covering sync");
        let committed_before_sync = tokio::time::timeout(
            Duration::from_millis(250),
            state.wait_message(&first.agent_id, Some("0"), Duration::ZERO),
        )
        .await
        .expect("a later sync must not block delivery of committed messages")
        .unwrap();
        assert_eq!(committed_before_sync.messages.len(), 1);
        assert_eq!(committed_before_sync.messages[0].seq, 1);
        assert_eq!(
            committed_before_sync.messages[0].body.text,
            "committed seed"
        );
        let visible = tokio::time::timeout(
            Duration::from_millis(250),
            state.wait_message(&first.agent_id, Some("1"), Duration::ZERO),
        )
        .await
        .expect("an empty committed peek must not wait for disk sync")
        .unwrap();
        assert!(visible.messages.is_empty());
        assert_eq!(visible.next_cursor, "1");
        let acknowledged =
            tokio::time::timeout(Duration::from_millis(250), state.ack(&first.agent_id, "1"))
                .await
                .expect("acknowledging a committed message must not wait for later sync")
                .unwrap();
        assert_eq!(acknowledged.acked_cursor, "1");
        let uncommitted_ack =
            tokio::time::timeout(Duration::from_millis(250), state.ack(&first.agent_id, "2"))
                .await
                .expect("checking an ack must use the committed head");
        assert!(matches!(uncommitted_ack, Err(BrokerError::Invalid(_))));
        let backlog = tokio::time::timeout(
            Duration::from_millis(250),
            state.debug_inbox_backlog(&first.agent_id),
        )
        .await
        .expect("committed backlog counters must not wait for later sync")
        .unwrap();
        assert_eq!(
            backlog,
            (0, 0),
            "the prior ack releases its capacity before the new batch commits"
        );
        let independent = tokio::time::timeout(
            Duration::from_secs(1),
            state.send_message(&sender, request(&second.name, "independent", None)),
        )
        .await;
        let paused_status = status(&state).await;
        let provisional_bytes = std::fs::metadata(inbox_path(temp.path(), &first))
            .unwrap()
            .len();
        assert!(!paused.is_finished());
        hook.release(outcome);
        assert_eq!(independent.unwrap().unwrap().seq, 1);
        assert_eq!(paused_status.seq_heads[&first_key], previous);
        assert!(provisional_bytes > previous.bytes);
        assert_eq!(paused_status.seq_heads[&second_key].seq, 1);
        assert_eq!(
            paused_status.seq_heads[&second_key].bytes,
            std::fs::metadata(inbox_path(temp.path(), &second))
                .unwrap()
                .len()
        );
        let send_result = paused.await.unwrap();
        let committed = state
            .wait_message(&first.agent_id, None, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(committed.cursor, "1");
        assert_eq!(committed.acked_cursor, "1");
        match outcome {
            SyncOutcome::Succeed => {
                assert_eq!(send_result.unwrap().seq, 2);
                assert_eq!(committed.messages.len(), 1);
                assert_eq!(committed.messages[0].seq, 2);
                assert_eq!(committed.messages[0].body.text, "paused");
                let bytes = serde_json::to_vec(&committed.messages[0]).unwrap().len() as u64 + 1;
                assert_eq!(
                    state.debug_inbox_backlog(&first.agent_id).await.unwrap(),
                    (1, bytes),
                    "only the newly committed batch consumes backlog after ack1"
                );
                assert!(hook.observation().sync_succeeded);
            }
            SyncOutcome::Fail => {
                assert!(send_result.is_err());
                assert!(committed.messages.is_empty());
                assert_eq!(committed.next_cursor, "1");
                assert_eq!(
                    state.debug_inbox_backlog(&first.agent_id).await.unwrap(),
                    (0, 0),
                    "failed bytes cannot restore previously acknowledged capacity"
                );
                assert_eq!(status(&state).await.seq_heads[&first_key], previous);
                assert_eq!(hook.observation().stage, FaultStage::SyncFailed);
                assert!(!hook.observation().sync_succeeded);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failed_active_write_retains_its_key_but_releases_a_known_queued_key() {
        let temp = common::TempDir::new("batch-queued-key");
        let mut config = config(temp.path());
        config.write_batch_messages = 1;
        config.write_queue_messages = 1;
        let state = Arc::new(DaemonState::with_config(config).unwrap());
        let sender = identity(&state, "sender").await;
        let first = identity(&state, "first").await;
        let second = identity(&state, "second").await;
        let hook = faults::arm_append(
            inbox_path(temp.path(), &first),
            AppendFault::PauseBeforeSync,
        )
        .unwrap();
        let active_state = Arc::clone(&state);
        let active_sender = sender.clone();
        let active_name = first.name.clone();
        let active = tokio::spawn(async move {
            active_state
                .send_message(
                    &active_sender,
                    request(&active_name, "active", Some("active")),
                )
                .await
        });
        hook.wait_reached(Duration::from_secs(2)).unwrap();

        let mut queued = Box::pin(state.send_message(
            &sender,
            request(&first.name, "never written", Some("queued")),
        ));
        let first_poll = poll_fn(|cx| Poll::Ready(queued.as_mut().poll(cx))).await;
        assert!(first_poll.is_pending());
        let full = tokio::time::timeout(
            Duration::from_millis(250),
            state.send_message(&sender, request(&first.name, "probe", Some("probe"))),
        )
        .await;
        // A full one-slot queue proves the specifically polled request is
        // queued behind the paused active write, without timing assumptions.
        hook.release(SyncOutcome::Fail);
        assert_full(full.unwrap().unwrap_err());
        assert!(active.await.unwrap().is_err());
        assert!(queued.await.is_err());
        assert!(state
            .send_message(&sender, request(&second.name, "retry", Some("active")))
            .await
            .is_err());
        let moved = state
            .send_message(&sender, request(&second.name, "safe retry", Some("queued")))
            .await
            .unwrap();
        assert!(!moved.duplicate);
        assert_eq!(moved.to_agent_id, second.agent_id);
        let first_rows = records(temp.path(), &first);
        assert_eq!(first_rows.len(), 1);
        assert_eq!(first_rows[0].idempotency_key.as_deref(), Some("active"));
        let second_rows = records(temp.path(), &second);
        assert_eq!(second_rows.len(), 1);
        assert_eq!(second_rows[0].idempotency_key.as_deref(), Some("queued"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn expiry_sync_failure_hides_messages_and_retains_uncertain_keys_until_replay() {
        let temp = common::TempDir::new("batch-expiry-fail");
        let config = config(temp.path());
        let state = Arc::new(DaemonState::owned(config.clone()).unwrap());
        let sender = identity(&state, "sender").await;
        let first = identity(&state, "first").await;
        let second = identity(&state, "second").await;
        let expiry_path = inbox_path(temp.path(), &first).with_file_name("expiry.jsonl");
        let hook = faults::arm_append(&expiry_path, AppendFault::FailSync).unwrap();
        let keys = (0..12)
            .map(|index| format!("ttl-{index}"))
            .collect::<Vec<_>>();
        let requests = keys
            .iter()
            .map(|key| {
                let mut request = request(&first.name, "metadata fails", Some(key));
                request.ttl = Some(Duration::from_secs(30));
                request
            })
            .collect();
        let results = concurrent_sends(&state, &sender, requests).await;
        assert!(results.iter().all(Result::is_err));
        assert_eq!(hook.observation().stage, FaultStage::SyncFailed);
        assert!(!hook.observation().sync_succeeded);
        assert!(std::fs::metadata(&expiry_path).unwrap().len() > 0);
        assert!(records(temp.path(), &first).is_empty());
        assert!(state
            .wait_message(&first.agent_id, Some("0"), Duration::ZERO)
            .await
            .unwrap()
            .messages
            .is_empty());
        let failed_status = status(&state).await;
        assert!(!failed_status
            .seq_heads
            .contains_key(&log_key(&first, "inbox.jsonl")));
        assert!(!failed_status
            .seq_heads
            .contains_key(&log_key(&first, "expiry.jsonl")));

        let mut uncertain_keys = Vec::new();
        let mut safely_moved = 0;
        for key in &keys {
            match state
                .send_message(&sender, request(&second.name, "retry", Some(key)))
                .await
            {
                Err(_) => uncertain_keys.push(key.clone()),
                Ok(receipt) => {
                    // A request still queued when the active batch failed was
                    // never written; its reservation can be released safely.
                    assert!(!receipt.duplicate);
                    assert_eq!(receipt.to_agent_id, second.agent_id);
                    safely_moved += 1;
                }
            }
        }
        assert!(!uncertain_keys.is_empty(), "active batch retains its keys");
        state
            .send_message(
                &sender,
                request(&second.name, "unrelated", Some("unrelated")),
            )
            .await
            .unwrap();
        assert_eq!(records(temp.path(), &second).len(), safely_moved + 1);
        drop(state);

        let replayed = reopen_owned(config).await;
        for key in &uncertain_keys {
            let receipt = replayed
                .send_message(&sender, request(&second.name, "after replay", Some(key)))
                .await
                .unwrap();
            assert!(
                !receipt.duplicate,
                "failed expiry never appended an inbox row"
            );
            assert_eq!(receipt.to_agent_id, second.agent_id);
        }
        assert!(records(temp.path(), &first).is_empty());
        assert_eq!(records(temp.path(), &second).len(), keys.len() + 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn status_publishes_expiry_head_only_after_its_sync_and_inbox_head_after_both() {
        let temp = common::TempDir::new("batch-expiry-status");
        let state = Arc::new(DaemonState::with_config(config(temp.path())).unwrap());
        let sender = identity(&state, "sender").await;
        let recipient = identity(&state, "recipient").await;
        let path = inbox_path(temp.path(), &recipient);
        let expiry_path = path.with_file_name("expiry.jsonl");
        let expiry_hook = faults::arm_append(&expiry_path, AppendFault::PauseBeforeSync).unwrap();
        let inbox_hook = faults::arm_append(&path, AppendFault::PauseBeforeSync).unwrap();
        let inbox_key = log_key(&recipient, "inbox.jsonl");
        let expiry_key = log_key(&recipient, "expiry.jsonl");
        let send_state = Arc::clone(&state);
        let recipient_name = recipient.name.clone();
        let sending = tokio::spawn(async move {
            let mut request = request(&recipient_name, "ttl body", Some("ttl-status"));
            request.ttl = Some(Duration::from_secs(30));
            send_state.send_message(&sender, request).await
        });
        expiry_hook.wait_reached(Duration::from_secs(2)).unwrap();
        let expiry_pending = status(&state).await;
        assert!(!expiry_pending.seq_heads.contains_key(&expiry_key));
        assert!(!expiry_pending.seq_heads.contains_key(&inbox_key));
        assert!(std::fs::metadata(&expiry_path).unwrap().len() > 0);
        assert!(!sending.is_finished());

        expiry_hook.release(SyncOutcome::Succeed);
        inbox_hook.wait_reached(Duration::from_secs(2)).unwrap();
        let inbox_pending = status(&state).await;
        assert!(expiry_hook.observation().sync_succeeded);
        assert_eq!(inbox_pending.seq_heads[&expiry_key].seq, 1);
        assert_eq!(
            inbox_pending.seq_heads[&expiry_key].bytes,
            std::fs::metadata(&expiry_path).unwrap().len()
        );
        assert!(!inbox_pending.seq_heads.contains_key(&inbox_key));
        assert!(!sending.is_finished());

        inbox_hook.release(SyncOutcome::Succeed);
        assert_eq!(sending.await.unwrap().unwrap().seq, 1);
        let committed = status(&state).await;
        assert_eq!(committed.seq_heads[&inbox_key].seq, 1);
        assert_eq!(committed.seq_heads[&expiry_key].seq, 1);
        assert!(inbox_hook.observation().sync_succeeded);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn paused_writer_bounds_queued_requests_separately_from_its_active_batch() {
        let temp = common::TempDir::new("batch-queue-count-cap");
        let mut config = config(temp.path());
        config.write_batch_messages = 1;
        config.write_queue_messages = 2;
        let state = Arc::new(DaemonState::with_config(config).unwrap());
        let sender = identity(&state, "sender").await;
        let recipient = identity(&state, "recipient").await;
        let hook = faults::arm_append(
            inbox_path(temp.path(), &recipient),
            AppendFault::PauseBeforeSync,
        )
        .unwrap();
        let head_state = Arc::clone(&state);
        let head_sender = sender.clone();
        let head_recipient = recipient.name.clone();
        let head = tokio::spawn(async move {
            head_state
                .send_message(&head_sender, request(&head_recipient, "active", None))
                .await
        });
        hook.wait_reached(Duration::from_secs(2)).unwrap();

        let barrier = Arc::new(Barrier::new(4));
        let (done, mut results) = tokio::sync::mpsc::unbounded_channel();
        let mut queued = Vec::new();
        for _ in 0..3 {
            let state = Arc::clone(&state);
            let sender = sender.clone();
            let recipient = recipient.name.clone();
            let barrier = Arc::clone(&barrier);
            let done = done.clone();
            queued.push(tokio::spawn(async move {
                barrier.wait().await;
                let result = state
                    .send_message(&sender, request(&recipient, "queued", None))
                    .await;
                done.send(result).unwrap();
            }));
        }
        drop(done);
        barrier.wait().await;
        let rejected = tokio::time::timeout(Duration::from_secs(1), results.recv()).await;
        // Release before assertions so a failing capacity check still drains
        // the real blocking writer during runtime shutdown.
        hook.release(SyncOutcome::Succeed);
        assert_full(rejected.unwrap().unwrap().unwrap_err());
        assert_eq!(head.await.unwrap().unwrap().seq, 1);
        let mut accepted = Vec::new();
        while let Some(result) = results.recv().await {
            accepted.push(result.unwrap().seq);
        }
        for worker in queued {
            worker.await.unwrap();
        }
        accepted.sort_unstable();
        assert_eq!(accepted, vec![2, 3]);
        assert_eq!(records(temp.path(), &recipient).len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn canceling_a_sender_keeps_its_admitted_write_and_dedupe_result() {
        let temp = common::TempDir::new("batch-cancel");
        let state = Arc::new(DaemonState::with_config(config(temp.path())).unwrap());
        let sender = identity(&state, "sender").await;
        let recipient = identity(&state, "recipient").await;
        let hook = faults::arm_append(
            inbox_path(temp.path(), &recipient),
            AppendFault::PauseBeforeSync,
        )
        .unwrap();
        let send_state = Arc::clone(&state);
        let send_identity = sender.clone();
        let recipient_name = recipient.name.clone();
        let canceled = tokio::spawn(async move {
            send_state
                .send_message(
                    &send_identity,
                    request(&recipient_name, "admitted", Some("canceled-key")),
                )
                .await
        });
        hook.wait_reached(Duration::from_secs(2)).unwrap();
        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());
        hook.release(SyncOutcome::Succeed);
        let retry = tokio::time::timeout(
            Duration::from_secs(2),
            state.send_message(
                &sender,
                request(&recipient.name, "retry payload", Some("canceled-key")),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(retry.duplicate);
        let persisted = records(temp.path(), &recipient);
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].id, retry.message_id);
        assert_eq!(persisted[0].body.text, "admitted");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn uncertain_key_cannot_move_to_another_inbox_until_actual_replay() {
        let temp = common::TempDir::new("batch-uncertain-key");
        let config = config(temp.path());
        let state = Arc::new(DaemonState::owned(config.clone()).unwrap());
        let sender = identity(&state, "sender").await;
        let first = identity(&state, "first").await;
        let second = identity(&state, "second").await;
        let hook =
            faults::arm_append(inbox_path(temp.path(), &first), AppendFault::FailSync).unwrap();
        assert!(state
            .send_message(&sender, request(&first.name, "uncertain", Some("key")))
            .await
            .is_err());
        assert_eq!(hook.observation().stage, FaultStage::SyncFailed);
        assert!(state
            .send_message(&sender, request(&second.name, "retry", Some("key")))
            .await
            .is_err());
        assert!(records(temp.path(), &second).is_empty());
        state
            .send_message(&sender, request(&second.name, "unrelated", Some("other")))
            .await
            .unwrap();
        assert!(state
            .wait_message(&first.agent_id, Some("0"), Duration::ZERO)
            .await
            .unwrap()
            .messages
            .is_empty());
        drop(state);
        let replayed = reopen_owned(config).await;
        let retry = replayed
            .send_message(&sender, request(&second.name, "retry", Some("key")))
            .await
            .unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.to_agent_id, first.agent_id);
        assert_eq!(retry.to, first.name);
        assert_eq!(records(temp.path(), &first).len(), 1);
        assert_eq!(records(temp.path(), &second).len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn canceled_writer_holds_root_ownership_through_its_last_sync() {
        let temp = common::TempDir::new("batch-owner");
        let config = config(temp.path());
        let state = Arc::new(DaemonState::owned(config.clone()).unwrap());
        let sender = identity(&state, "sender").await;
        let recipient = identity(&state, "recipient").await;
        let hook = faults::arm_append(
            inbox_path(temp.path(), &recipient),
            AppendFault::PauseBeforeSync,
        )
        .unwrap();
        let sender_state = Arc::clone(&state);
        let recipient_name = recipient.name.clone();
        let canceled = tokio::spawn(async move {
            sender_state
                .send_message(&sender, request(&recipient_name, "retained", None))
                .await
        });
        hook.wait_reached(Duration::from_secs(2)).unwrap();
        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());
        drop(state);
        let error = DaemonState::owned(config.clone()).unwrap_err();
        assert!(matches!(error, BrokerError::Io(ref error)
            if error.kind() == std::io::ErrorKind::WouldBlock));
        hook.release(SyncOutcome::Succeed);
        let successor = reopen_owned(config).await;
        assert!(hook.observation().sync_succeeded);
        let delivered = successor
            .wait_message(&recipient.agent_id, Some("0"), Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(delivered.messages.len(), 1);
        assert_eq!(delivered.messages[0].body.text, "retained");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batched_ttl_records_recover_with_expired_cursor_progress() {
        let temp = common::TempDir::new("batch-ttl");
        let clock = faults::set_clock(temp.path(), 1_000).unwrap();
        let config = config(temp.path());
        let state = Arc::new(DaemonState::owned(config.clone()).unwrap());
        let sender = identity(&state, "sender").await;
        let recipient = identity(&state, "recipient").await;
        let requests = (0..12)
            .map(|_| {
                let mut request = request(&recipient.name, "expires", None);
                request.ttl = Some(Duration::from_millis(25));
                request
            })
            .collect();
        for result in concurrent_sends(&state, &sender, requests).await {
            result.unwrap();
        }
        clock.advance(25);
        drop(state);
        let replayed = reopen_owned(config).await;
        let batch = replayed
            .wait_message(&recipient.agent_id, Some("0"), Duration::ZERO)
            .await
            .unwrap();
        assert!(batch.messages.is_empty());
        assert_eq!(batch.next_cursor, "12");
        assert!(!batch.timed_out);
        assert_eq!(records(temp.path(), &recipient).len(), 12);
        let expiry = LogFile::<cooperators::messaging::ExpiryRecord>::read_segments(
            inbox_path(temp.path(), &recipient).with_file_name("expiry.jsonl"),
        )
        .unwrap();
        assert_eq!(expiry.records.len(), 12);
        assert!(expiry
            .records
            .iter()
            .all(|record| record.expires_at_ms == 1_025));
    }
}
