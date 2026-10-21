#![cfg(all(feature = "mbt-injection", unix))]

mod common;

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use cooperators::arrivals::{
    delivery_id, obligation_id, ArrivalKind, ArrivalNotice, ArrivalReference, ArrivalResource,
    SourceCoordinate, SubscribeArrivalsRequest,
};
use cooperators::docs::DocEvent;
use cooperators::envelope::Body;
use cooperators::faults::{
    arm_append, arm_point, AppendFault, FaultObservation, FaultPoint, FaultStage, PointFault,
};
use cooperators::registry::{AgentIdentity, Registry};
use cooperators::server::{DaemonConfig, DaemonState};
use cooperators::storage::{hash_record, sha256_hex, LogFile};
use serde_json::{json, Value};

fn write_json(path: &Path, value: Value) {
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    fs::rename(temporary, path).unwrap();
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

fn config(root: &Path) -> DaemonConfig {
    let mut config = DaemonConfig::new(root);
    config.enable_arrivals = true;
    config
}

fn recovered_identity(root: &Path, name: &str) -> AgentIdentity {
    let registry = Registry::load(root).unwrap();
    let record = registry
        .records_with_names()
        .find(|(candidate, _)| *candidate == name)
        .unwrap()
        .1;
    AgentIdentity {
        name: name.into(),
        agent_id: record.agent_id.clone(),
        kind: record.kind.clone(),
        capabilities: record.capabilities.clone(),
    }
}

async fn register(state: &DaemonState, name: &str) -> AgentIdentity {
    let registration = state
        .register_agent(name.into(), None, None, None)
        .await
        .unwrap();
    // Credentials are used only in memory, never serialized into evidence.
    state.authenticate(&registration.token).await.unwrap()
}

fn observation(value: &FaultObservation) -> Value {
    json!({
        "point": value.point.map(|point| format!("{point:?}")),
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

/// This test executable is its own subprocess helper. Every crash assertion
/// belongs to the parent; ordinary test collection returns immediately.
#[test]
fn arrival_crash_child() {
    let Some(mode) = std::env::var_os("COOP_ARRIVAL_CRASH_MODE") else {
        return;
    };
    let mode = mode.to_str().unwrap();
    let evidence = PathBuf::from(std::env::var_os("COOP_ARRIVAL_CRASH_ROOT").unwrap());
    let root = evidence.join("root");
    let runtime = runtime();
    let state = Arc::new(DaemonState::owned(config(&root)).unwrap());
    let author = runtime.block_on(register(&state, "author"));
    let recipient = runtime.block_on(register(&state, "recipient"));
    let subscription = runtime
        .block_on(state.subscribe_arrivals(
            &recipient,
            SubscribeArrivalsRequest {
                client_key: "crash-plan".into(),
                resource: ArrivalResource::Document {
                    name: "plan".into(),
                },
                event_kinds: vec![ArrivalKind::DocPublished, ArrivalKind::DocUpdated],
                catch_up: false,
            },
        ))
        .unwrap();
    let feed_path = root
        .join("agents")
        .join(&recipient.agent_id)
        .join("arrivals.jsonl");
    let source_pause = (mode == "source-before-feed")
        .then(|| arm_point(&feed_path, FaultPoint::BeforeRecordWrite, PointFault::Pause).unwrap());
    let feed_pause = (mode == "feed-before-sync")
        .then(|| arm_append(&feed_path, AppendFault::PauseBeforeSync).unwrap());
    let source_receipt = runtime
        .block_on(state.publish_doc(
            &author,
            "plan",
            "text/plain",
            Body::new("text/plain", "crash control version one"),
            None,
        ))
        .unwrap();
    let source_path = root.join("docs/plan.jsonl");
    let mut hook = None;
    let mut prior_ack = None;
    let mut ack_receipt = None;
    let mut default_page = None;
    let (pending_tx, pending_rx) = mpsc::channel();
    if let Some(pause) = source_pause.as_ref() {
        let reached = pause.wait_reached(Duration::from_secs(10)).unwrap();
        assert_eq!(reached.stage, FaultStage::Reached);
        assert_eq!(
            LogFile::<ArrivalNotice>::read_segments(&feed_path)
                .unwrap()
                .seq_head,
            0
        );
        hook = Some(observation(&reached));
    } else if let Some(pause) = feed_pause.as_ref() {
        let reached = pause.wait_reached(Duration::from_secs(10)).unwrap();
        assert_eq!(reached.stage, FaultStage::BeforeSync);
        assert!(!reached.sync_attempted);
        assert_eq!(
            LogFile::<ArrivalNotice>::read_segments(&feed_path)
                .unwrap()
                .seq_head,
            1
        );
        hook = Some(observation(&reached));
    } else {
        let page = runtime
            .block_on(state.wait_arrivals(&recipient, None, Duration::from_secs(10), 64))
            .unwrap();
        assert_eq!(page.notices.len(), 1);
        let receipt = runtime
            .block_on(state.ack_arrivals(&recipient, page.next_cursor))
            .unwrap();
        prior_ack = Some(receipt.clone());
        if mode == "ack-after-success" {
            ack_receipt = Some(receipt);
            let page = runtime
                .block_on(state.wait_arrivals(&recipient, None, Duration::ZERO, 64))
                .unwrap();
            assert!(page.notices.is_empty());
            default_page = Some(page);
        } else {
            assert_eq!(mode, "ack-before-sync");
            runtime
                .block_on(state.update_doc(
                    &author,
                    "plan",
                    1,
                    Body::new("text/plain", "crash control version two"),
                    None,
                    None,
                ))
                .unwrap();
            let page = runtime
                .block_on(state.wait_arrivals(&recipient, None, Duration::from_secs(10), 64))
                .unwrap();
            assert_eq!(page.notices.len(), 1);
            let ack_path = feed_path.with_file_name("arrivals-acks.jsonl");
            let pause = arm_append(&ack_path, AppendFault::PauseBeforeSync).unwrap();
            let state = Arc::clone(&state);
            let recipient = recipient.clone();
            let pending_tx = pending_tx.clone();
            runtime.spawn(async move {
                let result = state.ack_arrivals(&recipient, page.next_cursor).await;
                pending_tx.send(result.is_ok()).unwrap();
            });
            let reached = pause.wait_reached(Duration::from_secs(10)).unwrap();
            assert_eq!(reached.stage, FaultStage::BeforeSync);
            assert_eq!(reached.seq, Some(2));
            assert!(!reached.sync_attempted);
            assert!(pending_rx.try_recv().is_err());
            hook = Some(observation(&reached));
            // Keep this pause alive while evidence is written and the parent kills us.
            std::mem::forget(pause);
        }
    }
    if source_pause.is_some() || feed_pause.is_some() {
        let state = Arc::clone(&state);
        let recipient = recipient.clone();
        runtime.spawn(async move {
            let result = state
                .wait_arrivals(&recipient, None, Duration::ZERO, 64)
                .await;
            pending_tx.send(result.is_ok()).unwrap();
        });
        thread::sleep(Duration::from_millis(50));
        assert!(
            pending_rx.try_recv().is_err(),
            "paused feed must not return a notice"
        );
    }
    let sources = LogFile::<DocEvent>::read_segments(&source_path).unwrap();
    let feed = LogFile::<ArrivalNotice>::read_segments(&feed_path).unwrap();
    let expected_deliveries = sources
        .records
        .iter()
        .map(|source| {
            let hash = hash_record(source).unwrap();
            let obligation = obligation_id(
                &subscription.bus_id,
                "document",
                "plan",
                source.seq,
                &hash,
                0,
            );
            delivery_id(&obligation, &recipient.agent_id)
        })
        .collect::<Vec<_>>();
    for source in &sources.records {
        let intent = source.notification_intent.as_ref().unwrap();
        assert_eq!(intent.routes.len(), 1);
        assert_eq!(intent.routes[0].agent_id, recipient.agent_id);
        assert_eq!(
            intent.routes[0].subscription_ids,
            [subscription.subscription_id.clone()]
        );
    }
    write_json(
        &evidence.join("boundary.json"),
        json!({
            "mode": mode, "bus_id": subscription.bus_id,
            "recipient_agent_id": recipient.agent_id,
            "subscription_id": subscription.subscription_id,
            "source_success_receipt": source_receipt,
            "sources": sources.records, "source_head": sources.seq_head,
            "expected_delivery_ids": expected_deliveries,
            "feed_disk_records": feed.records,
            "prior_ack_success": prior_ack, "ack_success": ack_receipt,
            "default_page_after_ack": default_page,
            "pending_call_returned_before_kill": false,
            "hook": hook,
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

fn evidence_dir(mode: &str) -> (PathBuf, Option<common::TempDir>) {
    if let Some(base) = std::env::var_os("COOP_ARRIVAL_CRASH_EVIDENCE_DIR") {
        let path = PathBuf::from(base).join(mode);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::create_dir(&path).expect("preserve earlier runs; choose a new evidence directory");
        (path, None)
    } else {
        let temporary = common::TempDir::new(&format!("arrival-crash-{mode}"));
        (temporary.path().to_path_buf(), Some(temporary))
    }
}

fn crash_control(mode: &str) {
    let (evidence, _temporary) = evidence_dir(mode);
    let program = std::env::current_exe().unwrap();
    let child = Command::new(&program)
        .args([
            "--exact",
            "arrival_crash_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("COOP_ARRIVAL_CRASH_MODE", mode)
        .env("COOP_ARRIVAL_CRASH_ROOT", &evidence)
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
    let deadline = Instant::now() + Duration::from_secs(25);
    while !evidence.join("boundary.json").exists() {
        assert!(
            Instant::now() < deadline,
            "child boundary timed out: {}",
            evidence.display()
        );
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before boundary: {}",
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
            "program_sha256": sha256_hex(&fs::read(&program).unwrap()),
            "arguments": ["--exact", "arrival_crash_child", "--nocapture", "--test-threads=1"],
            "exit_status": status.to_string(), "exit_code": status.code(),
            "signal": status.signal(), "success": status.success(), "child_reaped": true,
        }),
    );
    assert_eq!(status.signal(), Some(9), "control requires actual SIGKILL");
    let root = evidence.join("root");
    let recipient = recovered_identity(&root, "recipient");
    assert_eq!(boundary["recipient_agent_id"], recipient.agent_id);
    let feed_path = root
        .join("agents")
        .join(&recipient.agent_id)
        .join("arrivals.jsonl");
    let before_feed = fs::read(&feed_path).unwrap();
    let source_path = root.join("docs/plan.jsonl");
    let before_source = fs::read(&source_path).unwrap();
    let runtime = runtime();
    let state = DaemonState::owned(config(&root)).unwrap();
    let zero_cursor = format!(
        "arrival:{}:{}:0",
        boundary["bus_id"].as_str().unwrap(),
        recipient.agent_id
    );
    let historical = runtime
        .block_on(state.wait_arrivals(&recipient, Some(&zero_cursor), Duration::from_secs(10), 64))
        .unwrap();
    let expected_count = boundary["expected_delivery_ids"].as_array().unwrap().len();
    assert_eq!(historical.notices.len(), expected_count);
    for (index, notice) in historical.notices.iter().enumerate() {
        let source: DocEvent = serde_json::from_value(boundary["sources"][index].clone()).unwrap();
        let hash = hash_record(&source).unwrap();
        assert_eq!(notice.delivery_id, boundary["expected_delivery_ids"][index]);
        assert_eq!(notice.seq, index as u64 + 1);
        assert_eq!(notice.recipient_agent_id, recipient.agent_id);
        assert_eq!(
            notice.source,
            SourceCoordinate {
                stream: "document".into(),
                name: "plan".into(),
                seq: source.seq,
                hash: hash.clone()
            }
        );
        assert_eq!(notice.obligation.hash, hash);
        assert_eq!(notice.obligation.seq, source.seq);
        assert_eq!(notice.obligation.journal_type, "document");
        assert_eq!(notice.obligation.journal_name, "plan");
        assert_eq!(notice.obligation.item_index, 0);
        assert_eq!(
            notice.resource,
            ArrivalReference::Document {
                name: "plan".into(),
                version: source.version.unwrap()
            }
        );
        assert_eq!(
            notice.subscription_ids,
            [boundary["subscription_id"].as_str().unwrap()]
        );
    }
    let default_page = runtime
        .block_on(state.wait_arrivals(&recipient, None, Duration::ZERO, 64))
        .unwrap();
    match mode {
        "ack-after-success" => {
            assert!(default_page.notices.is_empty());
            assert_eq!(default_page.acked_cursor, boundary["ack_success"]["cursor"]);
        }
        "ack-before-sync" => {
            let prior = boundary["prior_ack_success"]["cursor"].as_str().unwrap();
            let acked_seq = default_page
                .acked_cursor
                .rsplit(':')
                .next()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            let prior_seq = prior.rsplit(':').next().unwrap().parse::<u64>().unwrap();
            assert!(acked_seq >= prior_seq);
            assert!(acked_seq <= 2);
            assert!(default_page
                .notices
                .iter()
                .all(|notice| notice.seq > prior_seq));
        }
        _ => assert_eq!(default_page.notices, historical.notices),
    }
    runtime.block_on(state.shutdown_arrivals()).unwrap();
    let recovered = LogFile::<ArrivalNotice>::read_segments(&feed_path).unwrap();
    assert_eq!(recovered.records, historical.notices);
    assert_eq!(recovered.seq_head, expected_count as u64);
    assert_eq!(recovered.truncated_bytes, 0);
    assert_eq!(fs::read(&source_path).unwrap(), before_source);
    if mode != "source-before-feed" {
        assert_eq!(
            fs::read(&feed_path).unwrap(),
            before_feed,
            "recovery must dedupe a complete feed row"
        );
    }
    write_json(
        &evidence.join("recovery.json"),
        json!({
            "historical_page": historical, "default_page": default_page,
            "recovered_feed_records": recovered.records, "feed_head": recovered.seq_head,
            "feed_last_hash": recovered.last_hash, "exact_delivery_count": true,
            "source_bytes_preserved": true,
            "complete_existing_feed_deduped_without_append": mode != "source-before-feed",
            "process_kill_only_not_power_loss": true,
        }),
    );
}

#[test]
fn process_kill_after_source_commit_before_feed_write_recovers_exact_arrival() {
    crash_control("source-before-feed");
}

#[test]
fn process_kill_before_feed_sync_recovers_complete_row_without_duplicate() {
    crash_control("feed-before-sync");
}

#[test]
fn process_kill_after_ack_success_preserves_default_and_historical_cursors() {
    crash_control("ack-after-success");
}

#[test]
fn process_kill_before_ack_sync_preserves_prior_acknowledged_prefix() {
    crash_control("ack-before-sync");
}

#[test]
fn preupgrade_binary_refuses_v2_before_repairing_source_files() {
    let Some(program) = std::env::var_os("COOP_PREUPGRADE_BINARY") else {
        eprintln!("COOP_PREUPGRADE_BINARY unset: retained old executable gate not run");
        return;
    };
    let program = PathBuf::from(program);
    let (evidence, _temporary) = evidence_dir("preupgrade-refusal");
    let root = evidence.join("root");
    let runtime = runtime();
    let state = DaemonState::owned(config(&root)).unwrap();
    let author = runtime.block_on(register(&state, "author"));
    runtime
        .block_on(state.publish_doc(
            &author,
            "guard",
            "text/plain",
            Body::new("text/plain", "preserve source bytes"),
            None,
        ))
        .unwrap();
    runtime.block_on(state.shutdown_arrivals()).unwrap();
    drop(state);
    let source_paths = [
        root.join("docs/guard.jsonl"),
        root.join("queues/guard.jsonl"),
        root.join("agents")
            .join(&author.agent_id)
            .join("inbox.jsonl"),
    ];
    for path in &source_paths {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap()
            .write_all(b"{\"torn_guard_tail\":")
            .unwrap();
    }
    let before = source_paths
        .iter()
        .map(|path| fs::read(path).unwrap())
        .collect::<Vec<_>>();
    let registry_before = fs::read(root.join("registry.json")).unwrap();
    assert_eq!(read_json(&root.join("registry.json"))["v"], 2);
    let child = Command::new(&program)
        .arg("--root")
        .arg(&root)
        .args(["daemon", "run", "--http-addr", "off"])
        .env_remove("COOPERATORS_HOME")
        .env_remove("COOP_SOCKET")
        .env_remove("COOP_TOKEN")
        .stdout(Stdio::from(
            File::create(evidence.join("old.stdout.log")).unwrap(),
        ))
        .stderr(Stdio::from(
            File::create(evidence.join("old.stderr.log")).unwrap(),
        ))
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "old binary failed to reject upgraded root"
        );
        thread::sleep(Duration::from_millis(10));
    };
    let stderr = fs::read_to_string(evidence.join("old.stderr.log")).unwrap();
    assert!(!status.success());
    assert!(
        stderr.contains("unsupported registry version 2"),
        "{stderr}"
    );
    assert!(!root.join("run/daemon.sock").exists());
    assert_eq!(
        fs::read(root.join("registry.json")).unwrap(),
        registry_before
    );
    let snapshots = source_paths.iter().zip(&before).map(|(path, bytes)| {
        let after = fs::read(path).unwrap();
        assert_eq!(&after, bytes, "old binary repaired {}", path.display());
        json!({"path": path.strip_prefix(&root).unwrap(), "bytes": bytes.len(), "before_sha256": sha256_hex(bytes), "after_sha256": sha256_hex(&after), "identical": true})
    }).collect::<Vec<_>>();
    write_json(
        &evidence.join("guard-result.json"),
        json!({
            "program": program, "program_sha256": sha256_hex(&fs::read(&program).unwrap()),
            "arguments": ["--root", root.to_str().unwrap(), "daemon", "run", "--http-addr", "off"],
            "exit_status": status.to_string(), "exit_code": status.code(), "signal": status.signal(),
            "stderr": stderr, "source_files": snapshots, "registry_identical": true,
            "no_socket_created": true, "child_reaped": true,
        }),
    );
}
