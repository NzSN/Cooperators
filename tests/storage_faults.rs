#![cfg(feature = "mbt-injection")]

mod common;

use std::fs;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use cooperators::envelope::{Body, Envelope};
use cooperators::faults::{
    arm_append, arm_point, now_millis, reach, set_clock, AppendFault, FaultPoint, FaultStage,
    PointFault, SyncOutcome,
};
use cooperators::storage::{LogError, LogFile};

fn envelope() -> Envelope {
    Envelope::new(
        "codex:test",
        "claude:test",
        "event",
        Body::new("text/plain", "payload"),
    )
    .unwrap()
}

#[test]
fn partial_write_returns_error_with_actual_prefix_on_disk() {
    let temp = common::TempDir::new("fault-partial");
    let path = temp.path().join("inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).unwrap();
    let hook = arm_append(&path, AppendFault::PartialWrite { bytes: 9 }).unwrap();
    let mut record = envelope();
    assert!(log.append(&mut record).is_err());
    let serialized = serde_json::to_vec(&record).unwrap();
    assert_eq!(fs::read(&path).unwrap(), serialized[..9]);
    let observed = hook.wait_reached(Duration::from_secs(1)).unwrap();
    assert_eq!(observed.stage, FaultStage::PartialWriteFailed);
    assert_eq!(observed.seq, Some(1));
    assert_eq!(observed.bytes_requested, serialized.len() + 1);
    assert_eq!(observed.bytes_written, 9);
    assert!(!observed.sync_attempted);
}

#[test]
fn pause_exposes_complete_real_bytes_before_receipt_then_performs_real_sync() {
    let temp = common::TempDir::new("fault-pause");
    let path = temp.path().join("inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).unwrap();
    let hook = arm_append(&path, AppendFault::PauseBeforeSync).unwrap();
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = log.append(&mut envelope());
        tx.send(result).unwrap();
        log
    });
    let observed = hook.wait_reached(Duration::from_secs(2)).unwrap();
    assert_eq!(observed.stage, FaultStage::BeforeSync);
    let bytes = fs::read(&path).unwrap();
    assert_eq!(bytes.len(), observed.bytes_written);
    assert_eq!(bytes.last(), Some(&b'\n'));
    let record: Envelope = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(record.seq, 1);
    assert!(
        rx.try_recv().is_err(),
        "receipt cannot precede release/sync"
    );
    assert!(!observed.sync_attempted);
    hook.release(SyncOutcome::Succeed);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    let mut log = worker.join().unwrap();
    let observed = hook.observation();
    assert_eq!(observed.stage, FaultStage::Synced);
    assert!(observed.sync_attempted && observed.sync_succeeded);
    // The consumed hook cannot pause the next append.
    assert_eq!(log.append(&mut envelope()).unwrap().seq, 2);
}

#[test]
fn release_can_turn_reached_sync_boundary_into_real_error_result() {
    let temp = common::TempDir::new("fault-release-fail");
    let path = temp.path().join("inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).unwrap();
    let hook = arm_append(&path, AppendFault::PauseBeforeSync).unwrap();
    let worker = thread::spawn(move || log.append(&mut envelope()));
    hook.wait_reached(Duration::from_secs(2)).unwrap();
    hook.release(SyncOutcome::Fail);
    assert!(worker.join().unwrap().is_err());
    let observed = hook.observation();
    assert_eq!(observed.stage, FaultStage::SyncFailed);
    assert!(!observed.sync_attempted);
    let bytes = fs::read(&path).unwrap();
    assert_eq!(bytes.len(), observed.bytes_written);
    assert!(serde_json::from_slice::<Envelope>(&bytes).is_ok());
}

#[test]
fn immediate_sync_failure_leaves_real_complete_record_and_is_path_scoped() {
    let temp = common::TempDir::new("fault-sync-path");
    let path = temp.path().join("inbox.jsonl");
    let other = temp.path().join("other.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).unwrap();
    let (mut other_log, _) = LogFile::<Envelope>::open(&other).unwrap();
    let hook = arm_append(&path, AppendFault::FailSync).unwrap();
    assert!(other_log.append(&mut envelope()).is_ok());
    assert_eq!(hook.observation().stage, FaultStage::Armed);
    assert!(log.append(&mut envelope()).is_err());
    let observed = hook.observation();
    assert_eq!(observed.stage, FaultStage::SyncFailed);
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        observed.bytes_written as u64
    );
    assert!(!observed.sync_attempted);
}

#[test]
fn dropping_pause_handle_releases_waiter_with_error() {
    let temp = common::TempDir::new("fault-cancel-pause");
    let path = temp.path().join("inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).unwrap();
    let hook = arm_append(&path, AppendFault::PauseBeforeSync).unwrap();
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || tx.send(log.append(&mut envelope())).unwrap());
    hook.wait_reached(Duration::from_secs(2)).unwrap();
    drop(hook);
    assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_err());
    worker.join().unwrap();
}

#[test]
fn named_points_are_one_shot_and_do_not_affect_other_boundaries() {
    let temp = common::TempDir::new("fault-point");
    let path = temp.path().join("dead.jsonl");
    let hook = arm_point(&path, FaultPoint::BeforeDeadProjection, PointFault::Fail).unwrap();
    reach(&path, FaultPoint::AfterDeadProjectionAppend).unwrap();
    assert_eq!(hook.observation().stage, FaultStage::Armed);
    assert!(reach(&path, FaultPoint::BeforeDeadProjection).is_err());
    assert_eq!(hook.observation().stage, FaultStage::InjectedFailure);
    reach(&path, FaultPoint::BeforeDeadProjection).unwrap();
}

#[test]
fn test_clock_is_scoped_advances_and_unregisters_on_drop() {
    let first = common::TempDir::new("clock-first");
    let second = common::TempDir::new("clock-second");
    assert_eq!(now_millis(first.path()), None);
    let clock = set_clock(first.path(), 10_000).unwrap();
    assert_eq!(now_millis(first.path()), Some(10_000));
    assert_eq!(now_millis(second.path()), None);
    assert!(set_clock(first.path(), 999).is_err());
    assert_eq!(clock.advance(250), 10_250);
    assert_eq!(now_millis(first.path()), Some(10_250));
    clock.set(12_000);
    assert_eq!(clock.now(), 12_000);
    assert_eq!(now_millis(first.path()), Some(12_000));
    clock.set(u64::MAX - 1);
    assert_eq!(clock.advance(10), u64::MAX);
    drop(clock);
    assert_eq!(now_millis(first.path()), None);
    assert!(set_clock(first.path(), 50).is_ok());
}

#[test]
fn sync_failure_quarantines_handle_without_mutating_until_reopen() {
    let temp = common::TempDir::new("fault-sync-quarantine");
    let path = temp.path().join("inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).unwrap();
    let _hook = arm_append(&path, AppendFault::FailSync).unwrap();
    assert!(log.append(&mut envelope()).is_err());
    assert!(log.requires_recovery());
    let before = fs::read(&path).unwrap();
    assert!(matches!(
        log.append(&mut envelope()),
        Err(LogError::RecoveryRequired)
    ));
    assert_eq!(fs::read(&path).unwrap(), before);
    drop(log);
    let (mut log, report) = LogFile::<Envelope>::open(&path).unwrap();
    assert_eq!(
        report.seq_head, 1,
        "complete uncertain tail can be recovered"
    );
    assert!(!log.requires_recovery());
    assert_eq!(log.append(&mut envelope()).unwrap().seq, 2);
    drop(log);
    assert_eq!(LogFile::<Envelope>::open(&path).unwrap().1.seq_head, 2);
}

#[test]
fn partial_failure_quarantines_then_reopen_repairs_before_new_append() {
    let temp = common::TempDir::new("fault-partial-quarantine");
    let path = temp.path().join("inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).unwrap();
    log.append(&mut envelope()).unwrap();
    let _hook = arm_append(&path, AppendFault::PartialWrite { bytes: 9 }).unwrap();
    assert!(log.append(&mut envelope()).is_err());
    let before = fs::read(&path).unwrap();
    assert!(matches!(
        log.append(&mut envelope()),
        Err(LogError::RecoveryRequired)
    ));
    assert_eq!(fs::read(&path).unwrap(), before);
    drop(log);
    let (mut log, report) = LogFile::<Envelope>::open(&path).unwrap();
    assert_eq!(report.seq_head, 1);
    assert_eq!(report.truncated_bytes, 9);
    assert_eq!(log.append(&mut envelope()).unwrap().seq, 2);
}

#[test]
fn recovered_prefix_must_sync_before_open_exposes_records() {
    let temp = common::TempDir::new("fault-recovery-sync");
    let path = temp.path().join("inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).unwrap();
    let _append_hook = arm_append(&path, AppendFault::FailSync).unwrap();
    assert!(log.append(&mut envelope()).is_err());
    drop(log);
    let before = fs::read(&path).unwrap();
    let report = LogFile::<Envelope>::read_segments(&path).unwrap();
    assert_eq!(report.seq_head, 1);
    let recovery = arm_point(&path, FaultPoint::BeforeRecoverySync, PointFault::Fail).unwrap();
    assert!(LogFile::<Envelope>::open(&path).is_err());
    assert_eq!(recovery.observation().stage, FaultStage::InjectedFailure);
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        LogFile::<Envelope>::read_segments(&path).unwrap().seq_head,
        1
    );
    assert_eq!(LogFile::<Envelope>::open(&path).unwrap().1.seq_head, 1);
}

#[test]
fn daemon_initialization_refuses_unsynced_recovered_inbox() {
    use cooperators::server::{DaemonConfig, DaemonState};
    let temp = common::TempDir::new("fault-daemon-recovery-sync");
    let path = temp.path().join("agents/a/inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).unwrap();
    let _append_hook = arm_append(&path, AppendFault::FailSync).unwrap();
    let mut record = envelope();
    record.idempotency_key = Some("recovered-dedupe".into());
    assert!(log.append(&mut record).is_err());
    drop(log);
    let before = fs::read(&path).unwrap();
    let _recovery = arm_point(&path, FaultPoint::BeforeRecoverySync, PointFault::Fail).unwrap();
    assert!(DaemonState::with_config(DaemonConfig::new(temp.path().to_path_buf())).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        LogFile::<Envelope>::read_segments(&path).unwrap().seq_head,
        1
    );
    assert!(DaemonState::with_config(DaemonConfig::new(temp.path().to_path_buf())).is_ok());
}
