//! Receipt pump, trusted proxy, and scheduler integration with deterministic hosts.

mod common;

use std::collections::VecDeque;
use std::fs;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use cooperators::arrivals::{
    ArrivalKind, ArrivalNotice, ArrivalReference, ObligationCoordinate, SourceCoordinate,
    WaitArrivals,
};
use cooperators::error::{BrokerError, ErrorCode};
use cooperators::host::*;
use cooperators::storage::{hash_record, GENESIS_HASH};
use serde_json::{json, Value};
use tokio::sync::{watch, Mutex, Notify};

fn binding(temp: &common::TempDir) -> Binding {
    Binding {
        binding_id: "binding-1".into(),
        generation: 1,
        bus_id: bus(),
        agent_id: "agent-1".into(),
        adapter_kind: "fake".into(),
        exact_host_session_id: "session-1".into(),
        workspace: temp.path().to_string_lossy().into_owned(),
        capability_status: Readiness::Configured,
    }
}

fn bridge(temp: &common::TempDir, limits: BridgeLimits) -> SharedHostBridge {
    fs::create_dir_all(temp.path().join("broker")).unwrap();
    Arc::new(Mutex::new(
        HostBridge::open(
            &temp.path().join("state"),
            &temp.path().join("broker"),
            binding(temp),
            limits,
        )
        .unwrap(),
    ))
}

fn limits() -> BridgeLimits {
    BridgeLimits {
        coalesce_ms: 0,
        observation_timeout_ms: 10,
        retry_min_ms: 1,
        ..Default::default()
    }
}

fn bus() -> String {
    format!(
        "bus-{}",
        cooperators::storage::sha256_hex(
            std::thread::current()
                .name()
                .expect("named test thread")
                .as_bytes()
        )
    )
}

fn cursor(seq: u64) -> String {
    format!("arrival:{}:agent-1:{seq}", bus())
}

fn notice(previous: Option<&ArrivalNotice>, queue: bool) -> ArrivalNotice {
    let seq = previous.map_or(1, |notice| notice.seq + 1);
    ArrivalNotice {
        v: 1,
        seq,
        prev_hash: previous.map_or_else(
            || GENESIS_HASH.into(),
            |notice| hash_record(notice).unwrap(),
        ),
        delivery_id: format!("delivery-{seq}"),
        recipient_agent_id: "agent-1".into(),
        kind: if queue {
            ArrivalKind::QueueTaskAvailable
        } else {
            ArrivalKind::DocPublished
        },
        obligation: ObligationCoordinate {
            id: format!("obligation-{seq}"),
            journal_type: "test".into(),
            journal_name: "resource".into(),
            seq,
            hash: "source-hash".into(),
            item_index: 0,
        },
        source: SourceCoordinate {
            stream: "test".into(),
            name: "resource".into(),
            seq,
            hash: "source-hash".into(),
        },
        resource: if queue {
            ArrivalReference::Queue {
                name: "resource".into(),
                cause: "enqueued".into(),
                task_id: Some("task-a".into()),
                ready_count: None,
            }
        } else {
            ArrivalReference::Document {
                name: "resource".into(),
                version: seq,
            }
        },
        subscription_ids: vec!["subscription-1".into()],
    }
}

fn page(notices: Vec<ArrivalNotice>, sequence: u64) -> WaitArrivals {
    WaitArrivals {
        bus_id: bus(),
        agent_id: "agent-1".into(),
        notices,
        next_cursor: cursor(sequence),
        acked_cursor: cursor(0),
        timed_out: false,
    }
}

struct FakeBroker {
    bridge: SharedHostBridge,
    pages: StdMutex<VecDeque<WaitArrivals>>,
    calls: StdMutex<Vec<String>>,
    fail_ack_once: StdMutex<bool>,
    acked: Notify,
}

impl FakeBroker {
    fn new(bridge: SharedHostBridge, pages: Vec<WaitArrivals>, fail_ack_once: bool) -> Arc<Self> {
        Arc::new(Self {
            bridge,
            pages: StdMutex::new(pages.into()),
            calls: StdMutex::new(Vec::new()),
            fail_ack_once: StdMutex::new(fail_ack_once),
            acked: Notify::new(),
        })
    }
}

impl BrokerArrivalClient for FakeBroker {
    fn wait<'a>(&'a self, cursor: Option<&'a str>) -> AdapterFuture<'a, WaitArrivals> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push(format!("wait:{}", cursor.unwrap_or("none")));
            let page = self.pages.lock().unwrap().pop_front();
            match page {
                Some(page) => Ok(page),
                None => std::future::pending().await,
            }
        })
    }

    fn ack<'a>(&'a self, cursor: &'a str) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            let bridge = self.bridge.lock().await;
            assert_eq!(
                bridge.received_cursor(),
                Some(cursor),
                "ACK is forbidden before durable receipt"
            );
            let persisted = fs::read_to_string(bridge.directory().join("bridge.jsonl")).unwrap();
            assert!(persisted
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .any(|record| record["event"] == "received" && record["cursor"] == cursor));
            drop(bridge);
            self.calls.lock().unwrap().push(format!("ack:{cursor}"));
            let should_fail = std::mem::take(&mut *self.fail_ack_once.lock().unwrap());
            if should_fail {
                return Err(BrokerError::application(
                    ErrorCode::DaemonUnreachable,
                    "ACK response lost",
                ));
            }
            self.acked.notify_one();
            Ok(())
        })
    }
}

#[tokio::test]
async fn receipt_loop_syncs_local_journal_before_ack_and_retries_lost_ack() {
    let temp = common::TempDir::new("host-receipt");
    let bridge = bridge(&temp, limits());
    let broker = FakeBroker::new(
        bridge.clone(),
        vec![page(vec![notice(None, false)], 1)],
        true,
    );
    let (stop, shutdown) = watch::channel(false);
    let task = tokio::spawn(run_receipt_loop(bridge.clone(), broker.clone(), shutdown));
    tokio::time::timeout(Duration::from_secs(3), broker.acked.notified())
        .await
        .unwrap();
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    let calls = broker.calls.lock().unwrap();
    assert_eq!(
        &calls[..3],
        &[
            "wait:none".to_string(),
            format!("ack:{}", cursor(1)),
            format!("ack:{}", cursor(1))
        ]
    );
    assert_eq!(bridge.lock().await.status().pending_notices, 1);
}

#[tokio::test]
async fn restarted_receipt_loop_acks_replayed_cursor_before_waiting() {
    let temp = common::TempDir::new("host-receipt-resume");
    let original = bridge(&temp, limits());
    original
        .lock()
        .await
        .receive(&bus(), "agent-1", vec![notice(None, false)], cursor(1), 0)
        .unwrap();
    drop(original);
    let bridge = bridge(&temp, limits());
    let broker = FakeBroker::new(bridge.clone(), vec![], false);
    let (stop, shutdown) = watch::channel(false);
    let task = tokio::spawn(run_receipt_loop(bridge, broker.clone(), shutdown));
    tokio::time::timeout(Duration::from_secs(1), broker.acked.notified())
        .await
        .unwrap();
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(
        broker.calls.lock().unwrap()[0],
        format!("ack:{}", cursor(1))
    );
}

#[tokio::test]
async fn inconsistent_empty_page_and_full_journal_withhold_ack_and_require_attention() {
    for full in [false, true] {
        let temp = common::TempDir::new("host-receipt-reject");
        let mut bounds = limits();
        if full {
            bounds.max_pending_bytes = 1;
        }
        let bridge = bridge(&temp, bounds);
        let page = if full {
            page(vec![notice(None, false)], 1)
        } else {
            page(vec![], 99)
        };
        let broker = FakeBroker::new(bridge.clone(), vec![page], false);
        let (_stop, shutdown) = watch::channel(false);
        assert!(run_receipt_loop(bridge.clone(), broker.clone(), shutdown)
            .await
            .is_err());
        assert!(broker
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|call| !call.starts_with("ack:")));
        assert_eq!(
            bridge.lock().await.status().readiness,
            Readiness::AttentionRequired
        );
        assert_eq!(bridge.lock().await.status().received_seq, 0);
    }
}

#[derive(Default)]
struct FakeHostState {
    lifecycle: Option<SessionState>,
    submissions: Vec<Wake>,
    reconciliations: Vec<(Wake, Option<String>)>,
    results: VecDeque<Submission>,
    events: Vec<HostEvent>,
    answers: Vec<(Value, Value)>,
    shutdown: bool,
}

struct FakeHost(Arc<StdMutex<FakeHostState>>);

impl HostAdapter for FakeHost {
    fn capabilities(&self) -> HostCapabilities {
        HostCapabilities {
            exact_session: true,
            reports_state: true,
            reconciles_history: true,
            approvals: true,
        }
    }
    fn session_state<'a>(&'a mut self, binding: &'a Binding) -> AdapterFuture<'a, SessionState> {
        Box::pin(async move {
            assert_eq!(binding.exact_host_session_id, "session-1");
            Ok(self
                .0
                .lock()
                .unwrap()
                .lifecycle
                .unwrap_or(SessionState::Idle))
        })
    }
    fn submit_wake<'a>(
        &'a mut self,
        binding: &'a Binding,
        wake: &'a Wake,
    ) -> AdapterFuture<'a, Submission> {
        Box::pin(async move {
            assert_eq!(binding.generation, wake.generation);
            let mut state = self.0.lock().unwrap();
            state.submissions.push(wake.clone());
            Ok(state.results.pop_front().unwrap_or(Submission::Accepted {
                turn_id: Some("turn-1".into()),
            }))
        })
    }
    fn reconcile_wake<'a>(
        &'a mut self,
        binding: &'a Binding,
        wake: &'a Wake,
        turn_id: Option<&'a str>,
    ) -> AdapterFuture<'a, Submission> {
        Box::pin(async move {
            assert_eq!(binding.generation, wake.generation);
            self.0
                .lock()
                .unwrap()
                .reconciliations
                .push((wake.clone(), turn_id.map(str::to_owned)));
            Ok(Submission::Accepted {
                turn_id: Some("turn-1".into()),
            })
        })
    }
    fn drain_events(&mut self) -> Vec<HostEvent> {
        std::mem::take(&mut self.0.lock().unwrap().events)
    }
    fn answer_approval<'a>(
        &'a mut self,
        request_id: &'a Value,
        response: Value,
    ) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .answers
                .push((request_id.clone(), response));
            Ok(())
        })
    }
    fn shutdown<'a>(&'a mut self) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            self.0.lock().unwrap().shutdown = true;
            Ok(())
        })
    }
}

#[tokio::test]
async fn runner_coalesces_busy_receipts_and_preserves_next_batch_and_exact_session() {
    let temp = common::TempDir::new("host-runner-busy");
    let bridge = bridge(&temp, limits());
    let host = Arc::new(StdMutex::new(FakeHostState {
        lifecycle: Some(SessionState::Busy),
        ..Default::default()
    }));
    let mut runner = BridgeRunner::new(bridge.clone(), Box::new(FakeHost(host.clone())));
    let first = notice(None, false);
    bridge
        .lock()
        .await
        .receive(&bus(), "agent-1", vec![first.clone()], cursor(1), 0)
        .unwrap();
    runner.tick(0).await.unwrap();
    assert!(host.lock().unwrap().submissions.is_empty());
    host.lock().unwrap().lifecycle = Some(SessionState::Idle);
    runner.tick(1).await.unwrap();
    let wake = host.lock().unwrap().submissions[0].clone();
    let next = notice(Some(&first), false);
    bridge
        .lock()
        .await
        .receive(&bus(), "agent-1", vec![next], cursor(2), 2)
        .unwrap();
    host.lock().unwrap().lifecycle = Some(SessionState::Busy);
    runner.tick(2).await.unwrap();
    assert_eq!(host.lock().unwrap().submissions.len(), 1);
    resolve_authenticated(
        &bridge,
        &bus(),
        "agent-1",
        ResolveArrivalsRequest {
            wake_id: wake.wake_id,
            generation: 1,
            decisions: vec![ResolutionDecision {
                delivery_id: "delivery-1".into(),
                disposition: Disposition::DocumentRead,
            }],
            continuation: false,
        },
    )
    .await
    .unwrap();
    host.lock().unwrap().lifecycle = Some(SessionState::Idle);
    runner.tick(3).await.unwrap();
    let state = host.lock().unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.submissions[1].notices.len(), 1);
    assert_eq!(state.submissions[1].notices[0].seq, 2);
}

#[tokio::test]
async fn runner_reconciles_uncertain_acceptance_without_an_extra_submission() {
    let temp = common::TempDir::new("host-runner-unknown");
    let bridge = bridge(&temp, limits());
    bridge
        .lock()
        .await
        .receive(&bus(), "agent-1", vec![notice(None, false)], cursor(1), 0)
        .unwrap();
    let host = Arc::new(StdMutex::new(FakeHostState {
        results: vec![Submission::Unknown {
            detail: "lost response".into(),
        }]
        .into(),
        ..Default::default()
    }));
    let mut runner = BridgeRunner::new(bridge.clone(), Box::new(FakeHost(host.clone())));
    runner.tick(0).await.unwrap();
    runner.tick(1).await.unwrap();
    runner.tick(2).await.unwrap();
    let state = host.lock().unwrap();
    assert_eq!(state.submissions.len(), 1);
    assert_eq!(state.reconciliations.len(), 1);
    assert_eq!(state.reconciliations[0].0, state.submissions[0]);
    assert_eq!(bridge.lock().await.status().activation_attempts, 1);
}

#[tokio::test]
async fn runner_surfaces_individual_approval_requests_and_retains_pending_notices() {
    let temp = common::TempDir::new("host-runner-approval");
    let bridge = bridge(&temp, limits());
    bridge
        .lock()
        .await
        .receive(&bus(), "agent-1", vec![notice(None, false)], cursor(1), 0)
        .unwrap();
    let approval = HostApproval {
        request_id: json!(7),
        method: "permission".into(),
        params: json!({"command":"approved individually"}),
    };
    let host = Arc::new(StdMutex::new(FakeHostState {
        lifecycle: Some(SessionState::WaitingForPermission),
        events: vec![HostEvent::Approval {
            approval: approval.clone(),
        }],
        ..Default::default()
    }));
    let mut runner = BridgeRunner::new(bridge.clone(), Box::new(FakeHost(host.clone())));
    assert_eq!(
        runner.tick(0).await.unwrap(),
        vec![HostEvent::Approval { approval }]
    );
    assert!(host.lock().unwrap().submissions.is_empty());
    assert_eq!(
        bridge.lock().await.status().readiness,
        Readiness::WaitingForPermission
    );
    runner
        .answer_approval(&json!(7), json!({"decision":"deny"}))
        .await
        .unwrap();
    runner.shutdown().await.unwrap();
    let host = host.lock().unwrap();
    assert_eq!(host.answers, vec![(json!(7), json!({"decision":"deny"}))]);
    assert!(host.shutdown);
    assert_eq!(bridge.lock().await.status().pending_notices, 1);
}

#[tokio::test]
async fn trusted_proxy_claim_context_authenticates_and_cannot_be_forged() {
    let temp = common::TempDir::new("host-claim-context");
    let bridge = bridge(&temp, limits());
    bridge
        .lock()
        .await
        .receive(&bus(), "agent-1", vec![notice(None, true)], cursor(1), 0)
        .unwrap();
    let wake = match bridge.lock().await.next_action(0).unwrap().unwrap() {
        BridgeAction::Submit(wake) => wake,
        _ => unreachable!(),
    };
    assert!(claim_context(&bridge, &bus(), "other-agent", "resource")
        .await
        .is_err());
    assert!(claim_context(&bridge, &bus(), "agent-1", "other-queue")
        .await
        .unwrap()
        .is_none());
    let context = claim_context(&bridge, &bus(), "agent-1", "resource")
        .await
        .unwrap()
        .unwrap();
    let mut forged = context.clone();
    forged.queue_generation += 1;
    assert!(
        observe_claim_authenticated(&bridge, &bus(), "agent-1", forged, ClaimResult::Empty)
            .await
            .is_err()
    );
    observe_claim_authenticated(&bridge, &bus(), "agent-1", context, ClaimResult::Empty)
        .await
        .unwrap();
    assert!(confirm_authenticated(
        &bridge,
        "other-bus",
        "agent-1",
        ConfirmActivationRequest {
            binding_id: wake.binding_id.clone(),
            generation: 1,
            nonce: wake.observation_nonce.clone(),
            wake_id: Some(wake.wake_id.clone())
        },
        1
    )
    .await
    .is_err());
    let receipt = resolve_authenticated(
        &bridge,
        &bus(),
        "agent-1",
        ResolveArrivalsRequest {
            wake_id: wake.wake_id,
            generation: 1,
            decisions: vec![ResolutionDecision {
                delivery_id: "delivery-1".into(),
                disposition: Disposition::QueueDrained,
            }],
            continuation: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(receipt.pending_notices, 0);
}
