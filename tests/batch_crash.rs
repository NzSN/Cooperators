#![cfg(all(feature = "mbt-injection", unix))]

mod common;

use std::fs::{self, File};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use cooperators::envelope::{Body, Envelope};
use cooperators::faults::{arm_append, AppendFault, FaultObservation, FaultStage, SyncOutcome};
use cooperators::messaging::{SendReceipt, SendRequest};
use cooperators::registry::{AgentIdentity, Registry};
use cooperators::server::{DaemonConfig, DaemonState};
use cooperators::storage::{hash_record, LogFile, GENESIS_HASH};
use serde_json::{json, Value};

const BATCH_SIZE: usize = 3;

fn write_json(path: &Path, value: Value) {
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    fs::rename(temporary, path).unwrap();
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn request(key: &str) -> SendRequest {
    SendRequest {
        to: "recipient".into(),
        kind: "batch-crash".into(),
        body: Body::new("text/plain", format!("payload-{key}")),
        reply_to: None,
        idempotency_key: Some(key.into()),
        ttl: None,
    }
}

fn observation(value: &FaultObservation) -> Value {
    json!({
        "stage": format!("{:?}", value.stage),
        "seq": value.seq,
        "bytes_requested": value.bytes_requested,
        "bytes_written": value.bytes_written,
        "bytes_written_known": value.bytes_written_known,
        "sync_attempted": value.sync_attempted,
        "sync_succeeded": value.sync_succeeded,
    })
}

fn idle_until_killed() -> ! {
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

/// Invoked by current_exe only with child-local environment. Ordinary test
/// collection returns immediately; every crash assertion lives in a parent.
#[test]
fn batch_crash_child() {
    let Some(mode) = std::env::var_os("COOP_BATCH_CRASH_MODE") else {
        return;
    };
    let mode = mode.to_str().unwrap();
    let evidence = PathBuf::from(std::env::var_os("COOP_BATCH_CRASH_ROOT").unwrap());
    let root = evidence.join("root");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut config = DaemonConfig::new(&root);
    config.write_batch_messages = BATCH_SIZE;
    config.write_batch_delay = Duration::from_millis(200);
    let state = Arc::new(DaemonState::with_config(config).unwrap());
    let (sender, recipient, prefix_receipt, prefix) = runtime.block_on(async {
        let sender = state
            .register_agent("sender".into(), None, None, None)
            .await
            .unwrap();
        let recipient = state
            .register_agent("recipient".into(), None, None, None)
            .await
            .unwrap();
        let sender = state.authenticate(&sender.token).await.unwrap();
        let recipient = state.authenticate(&recipient.token).await.unwrap();
        let receipt = state
            .send_message(&sender, request("prefix"))
            .await
            .unwrap();
        let prefix = state
            .wait_message(&recipient.agent_id, Some("0"), Duration::ZERO)
            .await
            .unwrap()
            .messages;
        (sender, recipient, receipt, prefix)
    });
    assert_eq!(prefix.len(), 1);
    write_json(
        &evidence.join("prefix.json"),
        json!({"receipt": prefix_receipt, "records": prefix}),
    );
    let path = root
        .join("agents")
        .join(&recipient.agent_id)
        .join("inbox.jsonl");
    let first_pause = arm_append(&path, AppendFault::PauseBeforeSync).unwrap();
    let (tx, rx) = mpsc::channel();
    for index in 0..BATCH_SIZE {
        let state = Arc::clone(&state);
        let sender = sender.clone();
        let tx = tx.clone();
        runtime.spawn(async move {
            let result = state
                .send_message(&sender, request(&format!("batch-{index}")))
                .await;
            tx.send(result).unwrap();
        });
    }
    drop(tx);
    let reached = first_pause.wait_reached(Duration::from_secs(10)).unwrap();
    assert_eq!(reached.seq, Some(2));
    assert_eq!(reached.stage, FaultStage::BeforeSync);

    if mode == "partial-failure" {
        let partial = arm_append(&path, AppendFault::PartialWrite { bytes: 9 }).unwrap();
        first_pause.release(SyncOutcome::Succeed);
        let mut errors = Vec::new();
        for _ in 0..BATCH_SIZE {
            errors.push(
                rx.recv_timeout(Duration::from_secs(10))
                    .unwrap()
                    .unwrap_err()
                    .to_string(),
            );
        }
        let published =
            runtime.block_on(state.wait_message(&recipient.agent_id, Some("0"), Duration::ZERO));
        assert_eq!(published.unwrap().messages, prefix);
        let disk = LogFile::<Envelope>::read_segments(&path).unwrap();
        assert_eq!(disk.records.len(), 2);
        assert_eq!(disk.truncated_bytes, 9);
        let partial_observation = partial.observation();
        assert_eq!(partial_observation.seq, Some(3));
        assert_eq!(partial_observation.stage, FaultStage::PartialWriteFailed);
        assert!(!first_pause.observation().sync_attempted);
        write_json(
            &evidence.join("boundary.json"),
            json!({
                "mode": mode, "successful_batch_receipts": [], "errors": errors,
                "published_records": prefix, "disk_records": disk.records,
                "truncated_bytes": disk.truncated_bytes,
                "hooks": [observation(&first_pause.observation()), observation(&partial_observation)],
            }),
        );
        idle_until_killed();
    }

    // Pause each successive record before releasing its predecessor. Reaching
    // seq 4 while no hook has synced proves all three records share the still
    // pending covering sync, rather than three independent successful sends.
    let mut pauses = vec![first_pause];
    for expected_seq in 3..=4 {
        let next = arm_append(&path, AppendFault::PauseBeforeSync).unwrap();
        pauses.last().unwrap().release(SyncOutcome::Succeed);
        let reached = next.wait_reached(Duration::from_secs(10)).unwrap();
        assert_eq!(reached.seq, Some(expected_seq));
        assert_eq!(reached.stage, FaultStage::BeforeSync);
        pauses.push(next);
    }
    assert!(rx.try_recv().is_err(), "no batch receipt may precede sync");
    assert!(pauses.iter().all(|hook| !hook.observation().sync_attempted));
    let disk = LogFile::<Envelope>::read_segments(&path).unwrap();
    assert_eq!(disk.records.len(), 4);
    assert_eq!(disk.truncated_bytes, 0);

    let receipts = if mode == "before-sync" {
        let published = runtime
            .block_on(state.wait_message(&recipient.agent_id, Some("0"), Duration::ZERO))
            .unwrap();
        assert_eq!(published.messages, prefix);
        Vec::new()
    } else {
        assert_eq!(mode, "after-sync");
        pauses.last().unwrap().release(SyncOutcome::Succeed);
        let mut receipts: Vec<SendReceipt> = (0..BATCH_SIZE)
            .map(|_| rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap())
            .collect();
        receipts.sort_by_key(|receipt| receipt.seq);
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.seq)
                .collect::<Vec<_>>(),
            [2, 3, 4]
        );
        assert!(receipts.iter().all(|receipt| !receipt.duplicate));
        assert!(pauses.iter().all(|hook| {
            let observed = hook.observation();
            observed.sync_attempted && observed.sync_succeeded
        }));
        let published = runtime
            .block_on(state.wait_message(&recipient.agent_id, Some("0"), Duration::ZERO))
            .unwrap();
        assert_eq!(published.messages, disk.records);
        receipts
    };
    write_json(
        &evidence.join("boundary.json"),
        json!({
            "mode": mode, "successful_batch_receipts": receipts,
            "published_records": if mode == "before-sync" { &prefix } else { &disk.records },
            "disk_records": disk.records, "truncated_bytes": disk.truncated_bytes,
            "hooks": pauses.iter().map(|hook| observation(&hook.observation())).collect::<Vec<_>>(),
        }),
    );
    idle_until_killed();
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn recovered_sender(root: &Path) -> AgentIdentity {
    let registry = Registry::load(root).unwrap();
    let (name, record) = registry
        .records_with_names()
        .find(|(name, _)| *name == "sender")
        .unwrap();
    AgentIdentity {
        name: name.into(),
        agent_id: record.agent_id.clone(),
        kind: record.kind.clone(),
        capabilities: record.capabilities.clone(),
    }
}

fn run_crash_control(mode: &str) {
    let temporary;
    let evidence = if let Some(base) = std::env::var_os("COOP_BATCH_CRASH_EVIDENCE_DIR") {
        let path = PathBuf::from(base).join(mode);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::create_dir(&path).expect("retain earlier runs; choose a new evidence directory");
        path
    } else {
        temporary = common::TempDir::new(&format!("batch-crash-{mode}"));
        temporary.path().to_path_buf()
    };
    let program = std::env::current_exe().unwrap();
    let child = Command::new(&program)
        .args([
            "--exact",
            "batch_crash_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("COOP_BATCH_CRASH_MODE", mode)
        .env("COOP_BATCH_CRASH_ROOT", &evidence)
        .stdout(Stdio::from(
            File::create(evidence.join("child.stdout.log")).unwrap(),
        ))
        .stderr(Stdio::from(
            File::create(evidence.join("child.stderr.log")).unwrap(),
        ))
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);
    let pid = child.0.id();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !evidence.join("boundary.json").exists() {
        assert!(
            Instant::now() < deadline,
            "child boundary timed out; evidence: {}",
            evidence.display()
        );
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before boundary; evidence: {}",
            evidence.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
    let boundary = read_json(&evidence.join("boundary.json"));
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    write_json(
        &evidence.join("process-result.json"),
        json!({
            "mode": mode, "pid": pid, "program": program,
            "exit_status": status.to_string(), "exit_code": status.code(),
            "signal": status.signal(), "success": status.success(),
        }),
    );
    assert!(!status.success());
    assert_eq!(status.signal(), Some(9), "control requires actual SIGKILL");

    let root = evidence.join("root");
    let sender = recovered_sender(&root);
    let registry = Registry::load(&root).unwrap();
    let recipient = registry
        .records_with_names()
        .find(|(name, _)| *name == "recipient")
        .unwrap()
        .1
        .clone();
    let path = root
        .join("agents")
        .join(&recipient.agent_id)
        .join("inbox.jsonl");
    let prefix: Vec<Envelope> =
        serde_json::from_value(read_json(&evidence.join("prefix.json"))["records"].clone())
            .unwrap();
    let before: Vec<Envelope> = serde_json::from_value(boundary["disk_records"].clone()).unwrap();
    let (log, report) = LogFile::<Envelope>::open(&path).unwrap();
    drop(log);
    assert!(
        !report.records.is_empty(),
        "earlier acknowledged prefix must survive"
    );
    assert_eq!(&report.records[..prefix.len()], prefix);
    assert_eq!(report.records, before[..report.records.len()]);
    match mode {
        "after-sync" => assert_eq!(report.records.len(), BATCH_SIZE + 1),
        "partial-failure" => {
            assert_eq!(report.records.len(), 2);
            assert_eq!(report.truncated_bytes, 9);
        }
        "before-sync" => assert!(report.records.len() <= BATCH_SIZE + 1),
        _ => panic!("unknown control"),
    }
    let mut previous_hash = GENESIS_HASH.to_string();
    for (index, record) in report.records.iter().enumerate() {
        assert_eq!(record.seq, index as u64 + 1);
        assert_eq!(record.prev_hash, previous_hash);
        previous_hash = hash_record(record).unwrap();
    }
    if mode == "after-sync" {
        for receipt in boundary["successful_batch_receipts"].as_array().unwrap() {
            let seq = receipt["seq"].as_u64().unwrap();
            let record = &report.records[seq as usize - 1];
            assert_eq!(receipt["message_id"], record.id);
            assert_eq!(receipt["idempotency_key"], json!(record.idempotency_key));
        }
    } else {
        assert!(boundary["successful_batch_receipts"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(boundary["published_records"], json!(prefix));
    }
    let before_retries = fs::read(&path).unwrap();
    let state = DaemonState::with_config(DaemonConfig::new(&root)).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut retries = Vec::new();
    for record in &report.records {
        let receipt = runtime
            .block_on(
                state.send_message(&sender, request(record.idempotency_key.as_deref().unwrap())),
            )
            .unwrap();
        assert!(receipt.duplicate);
        assert_eq!(receipt.seq, record.seq);
        assert_eq!(receipt.message_id, record.id);
        retries.push(receipt);
    }
    assert_eq!(
        fs::read(&path).unwrap(),
        before_retries,
        "recovered retries must not append duplicates"
    );
    write_json(
        &evidence.join("recovery.json"),
        json!({
            "recovered_records": report.records, "seq_head": report.seq_head,
            "last_hash": report.last_hash, "truncated_bytes": report.truncated_bytes,
            "prior_acknowledged_prefix_preserved": true,
            "duplicate_retries": retries, "dedupe_left_log_unchanged": true,
        }),
    );
}

#[test]
fn process_kill_before_batch_covering_sync_preserves_acknowledged_prefix() {
    run_crash_control("before-sync");
}

#[test]
fn process_kill_after_batch_receipts_recovers_every_receipt_and_dedupe() {
    run_crash_control("after-sync");
}

#[test]
fn process_kill_after_partial_batch_failure_repairs_tail_and_preserves_prefix() {
    run_crash_control("partial-failure");
}
