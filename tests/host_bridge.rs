//! Durable bridge and scheduler acceptance without a provider or model.

mod common;

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};

use cooperators::arrivals::{
    ArrivalKind, ArrivalNotice, ArrivalReference, ObligationCoordinate, SourceCoordinate,
};
use cooperators::host::*;
use cooperators::storage::{hash_record, GENESIS_HASH};

struct Fixture {
    temp: common::TempDir,
    binding: Binding,
    limits: BridgeLimits,
}

impl Fixture {
    fn new() -> Self {
        let temp = common::TempDir::new("host-bridge");
        fs::create_dir(temp.path().join("broker")).unwrap();
        Self {
            binding: Binding {
                binding_id: "binding-1".into(),
                generation: 1,
                bus_id: bus(),
                agent_id: "agent-1".into(),
                adapter_kind: "fake".into(),
                exact_host_session_id: "session-1".into(),
                workspace: temp.path().to_string_lossy().into_owned(),
                capability_status: Readiness::Configured,
            },
            limits: BridgeLimits {
                coalesce_ms: 0,
                observation_timeout_ms: 10,
                retry_min_ms: 2,
                retry_max_ms: 4,
                ..Default::default()
            },
            temp,
        }
    }

    fn open(&self) -> HostBridge {
        HostBridge::open(
            &self.temp.path().join("state"),
            &self.temp.path().join("broker"),
            self.binding.clone(),
            self.limits.clone(),
        )
        .unwrap()
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
            journal_type: if queue { "queue" } else { "document" }.into(),
            journal_name: "resource".into(),
            seq,
            hash: format!("hash-{seq}"),
            item_index: 0,
        },
        source: SourceCoordinate {
            stream: if queue { "queue" } else { "document" }.into(),
            name: "resource".into(),
            seq,
            hash: format!("hash-{seq}"),
        },
        resource: if queue {
            ArrivalReference::Queue {
                name: "resource".into(),
                cause: "enqueued".into(),
                task_id: Some(format!("task-{seq}")),
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

fn receive(bridge: &mut HostBridge, notices: Vec<ArrivalNotice>, now: u64) {
    let seq = notices.last().unwrap().seq;
    bridge
        .receive(&bus(), "agent-1", notices, cursor(seq), now)
        .unwrap();
}

fn submit(bridge: &mut HostBridge, now: u64) -> Wake {
    match bridge.next_action(now).unwrap().expect("scheduled action") {
        BridgeAction::Submit(wake) => wake,
        other => panic!("expected submit, got {other:?}"),
    }
}

fn request(wake: &Wake, disposition: Disposition, continuation: bool) -> ResolveArrivalsRequest {
    ResolveArrivalsRequest {
        wake_id: wake.wake_id.clone(),
        generation: wake.generation,
        decisions: wake
            .notices
            .iter()
            .map(|notice| ResolutionDecision {
                delivery_id: notice.delivery_id.clone(),
                disposition: disposition.clone(),
            })
            .collect(),
        continuation,
    }
}

fn confirm(wake: &Wake) -> ConfirmActivationRequest {
    ConfirmActivationRequest {
        binding_id: wake.binding_id.clone(),
        generation: wake.generation,
        nonce: wake.observation_nonce.clone(),
        wake_id: Some(wake.wake_id.clone()),
    }
}

#[test]
fn receipt_survives_restart_and_torn_tail_without_resetting_cursor() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    let first = notice(None, false);
    receive(&mut bridge, vec![first.clone()], 0);
    let directory = bridge.directory().to_owned();
    drop(bridge);
    OpenOptions::new()
        .append(true)
        .open(directory.join("bridge.jsonl"))
        .unwrap()
        .write_all(b"{\"v\":1,\"seq\":3")
        .unwrap();
    let mut bridge = fixture.open();
    assert_eq!(bridge.received_cursor(), Some(cursor(1).as_str()));
    assert_eq!(bridge.status().pending_notices, 1);
    assert_eq!(
        bridge
            .receive(&bus(), "agent-1", vec![first], cursor(1), 1)
            .unwrap()
            .added_notices,
        0
    );
    assert!(bridge
        .receive(&bus(), "agent-1", vec![], cursor(0), 1)
        .is_err());
    assert!(bridge
        .receive(
            &bus(),
            "agent-1",
            vec![],
            "arrival:other:agent-1:1".into(),
            1
        )
        .is_err());
    assert_eq!(submit(&mut bridge, 1).notices.len(), 1);
}

#[test]
fn canonical_lock_survives_release_and_rejects_duplicate_and_broker_paths() {
    let fixture = Fixture::new();
    let bridge = fixture.open();
    let owner = bridge.directory().join("owner.lock");
    let inode = fs::metadata(&owner).unwrap().ino();
    symlink(
        fixture.temp.path().join("state"),
        fixture.temp.path().join("state-alias"),
    )
    .unwrap();
    assert!(HostBridge::open(
        &fixture.temp.path().join("state-alias"),
        &fixture.temp.path().join("broker"),
        fixture.binding.clone(),
        fixture.limits.clone()
    )
    .is_err());
    assert!(
        HostBridge::open(
            &fixture.temp.path().join("other-state"),
            &fixture.temp.path().join("broker"),
            fixture.binding.clone(),
            fixture.limits.clone(),
        )
        .is_err(),
        "the fixed identity owner cannot depend on the selected journal base"
    );
    assert_eq!(
        fs::metadata(bridge.directory())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    drop(bridge);
    let bridge = fixture.open();
    assert_eq!(fs::metadata(owner).unwrap().ino(), inode);
    drop(bridge);
    let alternative = HostBridge::open(
        &fixture.temp.path().join("other-state"),
        &fixture.temp.path().join("broker"),
        fixture.binding.clone(),
        fixture.limits.clone(),
    )
    .unwrap();
    drop(alternative);
    let nested = fixture.temp.path().join("broker/forbidden/nested");
    assert!(HostBridge::open(
        &nested,
        &fixture.temp.path().join("broker"),
        fixture.binding.clone(),
        fixture.limits.clone()
    )
    .is_err());
    assert!(!fixture.temp.path().join("broker/forbidden").exists());
}

#[test]
fn keyed_claim_admission_and_result_retries_are_durable_and_bounded() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    receive(&mut bridge, vec![notice(None, true)], 0);
    let wake = submit(&mut bridge, 0);
    let attempt = bridge
        .begin_claim_with_key(&wake.wake_id, 1, "resource", "frontend:attempt")
        .unwrap();
    assert_eq!(
        bridge
            .begin_claim_with_key(&wake.wake_id, 1, "resource", "frontend:attempt")
            .unwrap(),
        attempt
    );
    assert_eq!(bridge.status().in_flight_claims, 1);
    let result = ClaimResult::Adopted {
        task_id: "task-a".into(),
    };
    bridge
        .finish_claim_context(&attempt, result.clone())
        .unwrap();
    let before = fs::read(bridge.directory().join("bridge.jsonl")).unwrap();
    bridge
        .finish_claim_context(&attempt, result.clone())
        .unwrap();
    assert_eq!(
        fs::read(bridge.directory().join("bridge.jsonl")).unwrap(),
        before
    );
    assert!(bridge
        .finish_claim_context(&attempt, ClaimResult::Empty)
        .is_err());
    drop(bridge);
    let mut bridge = fixture.open();
    assert_eq!(
        bridge
            .begin_claim_with_key(&wake.wake_id, 1, "resource", "frontend:attempt")
            .unwrap(),
        attempt
    );
    bridge.finish_claim_context(&attempt, result).unwrap();
    for index in 1..64 {
        let context = bridge
            .begin_claim_with_key(
                &wake.wake_id,
                1,
                "resource",
                &format!("frontend:attempt-{index}"),
            )
            .unwrap();
        bridge
            .finish_claim_context(
                &context,
                ClaimResult::Adopted {
                    task_id: format!("task-{index}"),
                },
            )
            .unwrap();
    }
    assert!(bridge
        .begin_claim_with_key(&wake.wake_id, 1, "resource", "frontend:over-budget")
        .is_err());
    assert_eq!(bridge.status().in_flight_claims, 0);
    bridge
        .resolve(request(
            &wake,
            Disposition::WorkAdopted {
                task_id: "task-a".into(),
            },
            true,
        ))
        .unwrap();
    let next = submit(&mut bridge, 1);
    assert!(bridge
        .begin_claim_with_key(&next.wake_id, 1, "resource", "frontend:new-wake")
        .is_ok());
}

#[test]
fn restart_keeps_unknown_in_flight_claims_fenced_until_exact_result_reconciles() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    receive(&mut bridge, vec![notice(None, true)], 0);
    let wake = submit(&mut bridge, 0);
    let attempt = bridge
        .begin_claim_with_key(&wake.wake_id, 1, "resource", "frontend:pending")
        .unwrap();
    let mut replacement = fixture.binding.clone();
    replacement.generation += 1;
    assert!(bridge.rebind(replacement, true).is_err());
    drop(bridge);
    let mut bridge = fixture.open();
    assert_eq!(bridge.status().in_flight_claims, 1);
    assert_eq!(bridge.status().readiness, Readiness::AttentionRequired);
    assert!(bridge.next_action(100).unwrap().is_none());
    assert!(bridge
        .resolve(request(&wake, Disposition::QueueDrained, false))
        .is_err());
    assert_eq!(
        bridge
            .begin_claim_with_key(&wake.wake_id, 1, "resource", "frontend:pending")
            .unwrap(),
        attempt
    );
    bridge
        .finish_claim_context(&attempt, ClaimResult::Unproven)
        .unwrap();
    let empty = bridge
        .begin_claim_with_key(&wake.wake_id, 1, "resource", "frontend:fresh-empty")
        .unwrap();
    bridge
        .finish_claim_context(&empty, ClaimResult::Empty)
        .unwrap();
    assert_eq!(
        bridge
            .resolve(request(&wake, Disposition::QueueDrained, false))
            .unwrap()
            .pending_notices,
        0
    );
    assert_eq!(bridge.status().readiness, Readiness::Verified);
}

#[test]
fn binding_generation_fences_resolution_and_prevents_pending_redirection() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    receive(&mut bridge, vec![notice(None, false)], 0);
    let wake = submit(&mut bridge, 0);
    let mut next = fixture.binding.clone();
    next.generation += 1;
    assert!(bridge.rebind(next.clone(), false).is_err());
    next.exact_host_session_id = "other-session".into();
    assert!(bridge.rebind(next.clone(), true).is_err());
    next.exact_host_session_id = fixture.binding.exact_host_session_id.clone();
    bridge.rebind(next, true).unwrap();
    assert!(bridge
        .resolve(request(&wake, Disposition::DocumentRead, false))
        .is_err());
    assert!(bridge.confirm_activation(confirm(&wake), 1).is_err());
    assert!(bridge.authenticate_binding(&bus(), "other-agent").is_err());
    assert_eq!(submit(&mut bridge, 1).generation, 2);
}

#[test]
fn unknown_host_acceptance_reconciles_same_wake_and_replays_after_restart() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    receive(&mut bridge, vec![notice(None, false)], 0);
    let wake = submit(&mut bridge, 0);
    bridge
        .record_submission(
            &wake.wake_id,
            Submission::Unknown {
                detail: "reply lost".into(),
            },
            0,
        )
        .unwrap();
    assert!(
        matches!(bridge.next_action(1).unwrap(), Some(BridgeAction::Reconcile { wake: ref retry, .. }) if retry == &wake)
    );
    bridge
        .record_reconciliation(
            &wake.wake_id,
            Submission::Accepted {
                turn_id: Some("turn-1".into()),
            },
        )
        .unwrap();
    drop(bridge);
    let mut bridge = fixture.open();
    assert!(
        matches!(bridge.next_action(2).unwrap(), Some(BridgeAction::Reconcile { wake: ref retry, turn_id: Some(ref turn) }) if retry == &wake && turn == "turn-1")
    );
    bridge
        .record_reconciliation(
            &wake.wake_id,
            Submission::Unknown {
                detail: "no history".into(),
            },
        )
        .unwrap();
    assert!(bridge.next_action(9).unwrap().is_none());
    assert_eq!(submit(&mut bridge, 10), wake);
}

#[test]
fn successful_transport_without_observation_exhausts_five_attempts() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    receive(&mut bridge, vec![notice(None, false)], 0);
    let mut wake_id = None;
    for attempt in 1..=5 {
        let wake = submit(&mut bridge, (attempt - 1) * 10);
        assert_eq!(wake_id.get_or_insert(wake.wake_id.clone()), &wake.wake_id);
        bridge
            .record_submission(
                &wake.wake_id,
                Submission::Accepted { turn_id: None },
                (attempt - 1) * 10,
            )
            .unwrap();
        assert_eq!(bridge.status().activation_attempts, attempt as u32);
    }
    assert!(bridge.next_action(50).unwrap().is_none());
    assert_eq!(bridge.status().readiness, Readiness::AttentionRequired);
    assert_eq!(bridge.status().pending_notices, 1);
    bridge
        .set_session_state(SessionState::WaitingForPermission, 51)
        .unwrap();
    assert_eq!(bridge.status().readiness, Readiness::AttentionRequired);
    bridge.set_session_state(SessionState::Idle, 52).unwrap();
    bridge.retry_pending(52).unwrap();
    assert!(matches!(
        bridge.next_action(52).unwrap(),
        Some(BridgeAction::Reconcile { .. })
    ));
}

#[test]
fn busy_permission_and_offline_do_not_discard_receipts_or_storm_wakes() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    let first = notice(None, false);
    receive(&mut bridge, vec![first.clone()], 0);
    for (time, state) in [
        (1, SessionState::Busy),
        (100, SessionState::WaitingForPermission),
        (200, SessionState::Offline),
    ] {
        bridge.set_session_state(state, time).unwrap();
        assert!(bridge.next_action(time).unwrap().is_none());
    }
    receive(&mut bridge, vec![notice(Some(&first), false)], 201);
    bridge.set_session_state(SessionState::Idle, 300).unwrap();
    let wake = submit(&mut bridge, 300);
    assert_eq!(wake.notices.len(), 2);
    bridge
        .record_submission(&wake.wake_id, Submission::Busy, 300)
        .unwrap();
    assert_eq!(bridge.status().activation_attempts, 0);
    assert!(bridge.next_action(301).unwrap().is_none());
    let wake = submit(&mut bridge, 302);
    bridge
        .record_submission(&wake.wake_id, Submission::PermissionBlocked, 302)
        .unwrap();
    assert_eq!(bridge.status().activation_attempts, 0);
    assert_eq!(bridge.status().readiness, Readiness::WaitingForPermission);
}

#[test]
fn known_busy_time_pauses_observation_and_nonce_expiry() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    receive(&mut bridge, vec![notice(None, false)], 0);
    let wake = submit(&mut bridge, 0);
    bridge
        .record_submission(&wake.wake_id, Submission::Accepted { turn_id: None }, 0)
        .unwrap();
    bridge.set_session_state(SessionState::Busy, 5).unwrap();
    assert!(bridge.next_action(100).unwrap().is_none());
    assert_eq!(
        bridge
            .confirm_activation(confirm(&wake), 100)
            .unwrap()
            .readiness,
        Readiness::Verified
    );
    bridge.set_session_state(SessionState::Idle, 105).unwrap();
    assert!(bridge.next_action(109).unwrap().is_none());
    assert_eq!(submit(&mut bridge, 110).wake_id, wake.wake_id);
}

#[test]
fn activation_challenges_enforce_purpose_expiry_and_reconnection() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    let probe = bridge.issue_probe(0).unwrap();
    let mut confirmation = ConfirmActivationRequest {
        binding_id: probe.binding_id.clone(),
        generation: probe.generation,
        nonce: probe.nonce.clone(),
        wake_id: Some("wrong-wake".into()),
    };
    assert!(bridge.confirm_activation(confirmation.clone(), 1).is_err());
    confirmation.wake_id = None;
    assert!(bridge.confirm_activation(confirmation.clone(), 10).is_err());
    let probe = bridge.issue_probe(11).unwrap();
    confirmation.nonce = probe.nonce;
    let receipt = bridge.confirm_activation(confirmation.clone(), 12).unwrap();
    assert_eq!(
        bridge.confirm_activation(confirmation.clone(), 50).unwrap(),
        receipt
    );
    drop(bridge);
    let mut bridge = fixture.open();
    assert_eq!(bridge.status().readiness, Readiness::Configured);
    assert!(bridge.confirm_activation(confirmation, 13).is_err());
}

#[test]
fn queue_empty_proof_cannot_cover_a_new_received_generation_or_another_wake() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    let first = notice(None, true);
    receive(&mut bridge, vec![first.clone()], 0);
    let wake = submit(&mut bridge, 0);
    let resolution = request(&wake, Disposition::QueueDrained, false);
    assert!(bridge.resolve(resolution.clone()).is_err());
    let attempt = bridge.begin_claim(&wake.wake_id, 1, "resource").unwrap();
    receive(&mut bridge, vec![notice(Some(&first), true)], 1);
    bridge
        .finish_claim_context(&attempt, ClaimResult::Empty)
        .unwrap();
    let receipt = bridge.resolve(resolution.clone()).unwrap();
    assert_eq!(receipt.pending_notices, 1);
    assert!(receipt.continuation);
    assert_eq!(bridge.resolve(resolution).unwrap(), receipt);
    let next = submit(&mut bridge, 2);
    assert_eq!(next.notices[0].seq, 2);
    assert!(bridge
        .resolve(request(&next, Disposition::QueueDrained, false))
        .is_err());
    assert!(bridge
        .finish_claim_context(&attempt, ClaimResult::Empty)
        .is_err());
}

#[test]
fn adoption_and_uncertain_claim_invalidate_earlier_empty_proofs() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    receive(&mut bridge, vec![notice(None, true)], 0);
    let wake = submit(&mut bridge, 0);
    let early = bridge.begin_claim(&wake.wake_id, 1, "resource").unwrap();
    let adoption = bridge.begin_claim(&wake.wake_id, 1, "resource").unwrap();
    bridge
        .finish_claim_context(
            &adoption,
            ClaimResult::Adopted {
                task_id: "task-a".into(),
            },
        )
        .unwrap();
    bridge
        .finish_claim_context(&early, ClaimResult::Empty)
        .unwrap();
    let resolution = request(&wake, Disposition::QueueDrained, false);
    assert!(bridge.resolve(resolution.clone()).is_err());
    bridge
        .observe_claim(ClaimObservation {
            wake_id: wake.wake_id.clone(),
            generation: 1,
            queue: "resource".into(),
            result: ClaimResult::Empty,
        })
        .unwrap();
    bridge
        .observe_claim(ClaimObservation {
            wake_id: wake.wake_id.clone(),
            generation: 1,
            queue: "resource".into(),
            result: ClaimResult::Unproven,
        })
        .unwrap();
    assert!(bridge.resolve(resolution.clone()).is_err());
    bridge
        .observe_claim(ClaimObservation {
            wake_id: wake.wake_id.clone(),
            generation: 1,
            queue: "resource".into(),
            result: ClaimResult::Empty,
        })
        .unwrap();
    assert_eq!(bridge.resolve(resolution).unwrap().pending_notices, 0);
}

#[test]
fn adopted_work_requires_continuation_and_keeps_the_hint_for_a_later_wake() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    receive(&mut bridge, vec![notice(None, true)], 0);
    let wake = submit(&mut bridge, 0);
    let adopted = Disposition::WorkAdopted {
        task_id: "task-a".into(),
    };
    assert!(bridge
        .resolve(request(&wake, adopted.clone(), true))
        .is_err());
    bridge
        .observe_claim(ClaimObservation {
            wake_id: wake.wake_id.clone(),
            generation: 1,
            queue: "resource".into(),
            result: ClaimResult::Adopted {
                task_id: "task-a".into(),
            },
        })
        .unwrap();
    assert!(bridge
        .resolve(request(&wake, adopted.clone(), false))
        .is_err());
    let resolution = request(&wake, adopted, true);
    let receipt = bridge.resolve(resolution.clone()).unwrap();
    assert_eq!(receipt.pending_notices, 1);
    assert!(receipt.resolved_delivery_ids.is_empty());
    assert_eq!(bridge.resolve(resolution).unwrap(), receipt);
    let next = submit(&mut bridge, 1);
    assert_ne!(next.wake_id, wake.wake_id);
    assert_eq!(next.notices, wake.notices);
}

#[test]
fn bounded_wakes_are_immutable_and_conflicting_resolutions_are_rejected() {
    let mut fixture = Fixture::new();
    fixture.limits.wake_notices = 1;
    let mut bridge = fixture.open();
    let first = notice(None, false);
    receive(
        &mut bridge,
        vec![first.clone(), notice(Some(&first), false)],
        0,
    );
    let wake = submit(&mut bridge, 0);
    assert_eq!(wake.notices.len(), 1);
    let mut resolution = request(&wake, Disposition::DocumentRead, false);
    let receipt = bridge.resolve(resolution.clone()).unwrap();
    assert_eq!(receipt.pending_notices, 1);
    resolution.decisions[0].disposition = Disposition::DocumentSuperseded {
        newer_version: 2,
        policy: "only latest version matters".into(),
    };
    assert!(bridge.resolve(resolution).is_err());
    let next = submit(&mut bridge, 1);
    assert_eq!(next.notices[0].seq, 2);
    assert_eq!(wake.notices[0].seq, 1);
}

#[test]
fn full_receive_and_oversized_wake_preserve_all_due_notices() {
    let mut fixture = Fixture::new();
    fixture.limits.max_pending_notices = 1;
    fixture.limits.wake_bytes = 1;
    let mut bridge = fixture.open();
    let first = notice(None, false);
    let second = notice(Some(&first), false);
    assert!(bridge
        .receive(&bus(), "agent-1", vec![first.clone(), second], cursor(2), 0)
        .is_err());
    assert_eq!(bridge.status().received_seq, 0);
    receive(&mut bridge, vec![first], 0);
    assert!(bridge.next_action(0).unwrap().is_none());
    assert_eq!(bridge.status().pending_notices, 1);
    assert_eq!(bridge.status().readiness, Readiness::AttentionRequired);
}

#[test]
fn confirmation_after_resolution_rejects_before_appending_an_invalid_journal_event() {
    let fixture = Fixture::new();
    let mut bridge = fixture.open();
    receive(&mut bridge, vec![notice(None, false)], 0);
    let wake = submit(&mut bridge, 0);
    bridge
        .resolve(request(&wake, Disposition::DocumentRead, false))
        .unwrap();
    let journal = bridge.directory().join("bridge.jsonl");
    let before = fs::read(&journal).unwrap();
    assert!(bridge.confirm_activation(confirm(&wake), 1).is_err());
    assert_eq!(fs::read(&journal).unwrap(), before);
    drop(bridge);
    assert_eq!(fixture.open().status().pending_notices, 0);
}

#[cfg(feature = "mbt-injection")]
#[test]
fn uncertain_receive_sync_withholds_a_receipt_until_actual_recovery() {
    use cooperators::faults::{arm_append, AppendFault};
    for fault in [
        AppendFault::PartialWrite { bytes: 20 },
        AppendFault::FailSync,
    ] {
        let fixture = Fixture::new();
        let mut bridge = fixture.open();
        let _injection = arm_append(bridge.directory().join("bridge.jsonl"), fault).unwrap();
        assert!(bridge
            .receive(&bus(), "agent-1", vec![notice(None, false)], cursor(1), 0)
            .is_err());
        assert_eq!(bridge.status().received_seq, 0);
        assert!(bridge.next_action(1).is_err());
        drop(bridge);
        let mut bridge = fixture.open();
        let expected = if fault == AppendFault::FailSync { 1 } else { 0 };
        assert_eq!(bridge.status().received_seq, expected);
        assert_eq!(
            bridge
                .receive(&bus(), "agent-1", vec![notice(None, false)], cursor(1), 1)
                .unwrap()
                .received_seq,
            1
        );
    }
}
