//! N3a process-crash and wire-response-loss controls. A process kill is not a
//! power-loss simulation; each receipt is reconstructed from the source log.
#![cfg(all(feature = "mbt-injection", unix))]

mod common;
#[path = "common/policy.rs"]
mod policy_approval;

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use cooperators::client::{BrokerClient, ClientError, HttpsOptions};
use cooperators::envelope::Envelope;
use cooperators::faults::{
    arm_append, arm_point, AppendFault, FaultObservation, FaultPoint, FaultStage, PointFault,
    SyncOutcome,
};
use cooperators::network::{call_remote, NetworkPolicy};
use cooperators::server::{DaemonConfig, DaemonState};
use cooperators::storage::{hash_record, sha256_hex, LogFile};
use cooperators::transport::journal::{JournalOutcome, OperationJournal};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn write_json(path: &Path, value: &Value) {
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(value).unwrap()).unwrap();
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

fn policy(device: &str) -> NetworkPolicy {
    serde_json::from_value(json!({
        "v": 1, "enable_operations": true, "allowed_hosts": ["localhost"],
        "devices": [{
            "principal_id": "operation-crash", "generation": 1,
            "token_hash": cooperators::registry::token_hash(device),
            "expires_at_ms": cooperators::id::unix_millis() + 3_600_000,
            "allow_enroll": true, "max_instances": 4, "instance_ttl_s": 3600,
            "permissions": [
                {"method": "send_message", "resources": ["inbox:*"]},
                {"method": "instance_metadata", "resources": ["agent:*"]},
                {"method": "operation_status", "resources": ["agent:*"]},
                {"method": "wait_message", "resources": ["inbox:*"]}
            ]
        }]
    }))
    .unwrap()
}

fn config(root: &Path, policy: NetworkPolicy) -> DaemonConfig {
    let mut config = DaemonConfig::new(root);
    config.network_policy = Some(policy);
    config
}

async fn prepare_resources(root: &Path) -> Prepared {
    let device = cooperators::registry::new_token().unwrap();
    let mut policy = policy(&device);
    policy.enable_resource_operations = true;
    for method in [
        "publish",
        "create_queue",
        "enqueue",
        "claim_task",
        "renew_lease",
        "complete_task",
        "fail_task",
        "publish_doc",
        "update_doc",
        "claim_doc",
        "release_doc",
    ] {
        policy.devices[0]
            .permissions
            .push(cooperators::network::Permission {
                method: method.into(),
                resources: vec!["*".into()],
            });
    }
    let state = Arc::new(DaemonState::with_config(config(root, policy.clone())).unwrap());
    let recipient = state
        .register_agent("recipient".into(), None, None, None)
        .await
        .unwrap();
    let enrollment = state
        .network()
        .unwrap()
        .enroll(
            &state,
            &device,
            json!({
                "launch_key":cooperators::registry::new_token().unwrap(),
                "kind":"test", "display_name":"resource sender"
            }),
        )
        .await
        .unwrap();
    Prepared {
        state,
        policy,
        token: enrollment["token"].as_str().unwrap().into(),
        recipient_id: recipient.agent_id,
        intent: Value::Null,
    }
}

struct Prepared {
    state: Arc<DaemonState>,
    policy: NetworkPolicy,
    token: String,
    recipient_id: String,
    intent: Value,
}

async fn prepare(root: &Path) -> Prepared {
    prepare_with_operations(root, true, true).await
}

async fn prepare_with_operations(root: &Path, enable_operations: bool, own_root: bool) -> Prepared {
    let device = cooperators::registry::new_token().unwrap();
    let mut policy = policy(&device);
    policy.enable_operations = enable_operations;
    let config = config(root, policy.clone());
    let state = Arc::new(if own_root {
        DaemonState::owned(config).unwrap()
    } else {
        DaemonState::with_config(config).unwrap()
    });
    let recipient = state
        .register_agent("recipient".into(), None, None, None)
        .await
        .unwrap();
    let enrollment = state
        .network()
        .unwrap()
        .enroll(
            &state,
            &device,
            json!({
                "launch_key": cooperators::registry::new_token().unwrap(),
                "kind": "test", "display_name": "operation sender"
            }),
        )
        .await
        .unwrap();
    let intent = json!({
        "to": recipient.agent_id, "kind": "operation-crash",
        "body": {"mime": "text/plain", "text": "one durable remote intent"},
        "operation": {
            "v": 1, "id": cooperators::id::new_ulid().unwrap(),
            "epoch": state.network().unwrap().meta.operation_epoch,
            "not_after_ms": cooperators::id::unix_millis() + 600_000
        }
    });
    Prepared {
        state,
        policy,
        token: enrollment["token"].as_str().unwrap().into(),
        recipient_id: recipient.agent_id,
        intent,
    }
}

fn inbox(root: &Path, recipient: &str) -> PathBuf {
    root.join("agents").join(recipient).join("inbox.jsonl")
}

fn observation(value: &FaultObservation) -> Value {
    json!({
        "point": value.point.map(|point| format!("{point:?}")),
        "stage": format!("{:?}", value.stage), "seq": value.seq,
        "bytes_requested": value.bytes_requested, "bytes_written": value.bytes_written,
        "bytes_written_known": value.bytes_written_known,
        "sync_attempted": value.sync_attempted, "sync_succeeded": value.sync_succeeded
    })
}

fn evidence_dir(mode: &str) -> (PathBuf, Option<common::TempDir>) {
    if let Some(base) = std::env::var_os("COOP_OPERATION_CRASH_EVIDENCE_DIR") {
        let path = PathBuf::from(base).join(mode);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::create_dir(&path).expect("preserve previous runs; choose fresh evidence directory");
        (path, None)
    } else {
        let temp = common::TempDir::new(&format!("operation-{mode}"));
        (temp.path().to_path_buf(), Some(temp))
    }
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The parent owns credentials in memory and passes them in the child environment.
/// Evidence contains intent, hashes and receipts, never bearer credentials.
#[test]
fn operation_crash_child() {
    let Some(mode) = std::env::var_os("COOP_OPERATION_CRASH_MODE") else {
        return;
    };
    let mode = mode.to_str().unwrap();
    let evidence = PathBuf::from(std::env::var_os("COOP_OPERATION_CRASH_ROOT").unwrap());
    let root = evidence.join("root");
    let intent = read_json(&evidence.join("intent.json"));
    let policy: NetworkPolicy =
        serde_json::from_value(read_json(&evidence.join("policy.json"))).unwrap();
    let token = std::env::var("COOP_OPERATION_CRASH_TOKEN").unwrap();
    std::env::remove_var("COOP_OPERATION_CRASH_TOKEN");
    let path = inbox(&root, intent["to"].as_str().unwrap());
    let runtime = runtime();
    let state = Arc::new(DaemonState::owned(config(&root, policy)).unwrap());
    let hook = match mode {
        "before-sync" => arm_append(&path, AppendFault::PauseBeforeSync).unwrap(),
        "after-sync-before-reply" => arm_point(
            &path,
            FaultPoint::AfterOperationSyncBeforePublish,
            PointFault::Pause,
        )
        .unwrap(),
        "partial-write" => arm_append(&path, AppendFault::PartialWrite { bytes: 9 }).unwrap(),
        _ => panic!("unknown child mode"),
    };
    let (tx, rx) = mpsc::channel();
    let task_state = Arc::clone(&state);
    let task_token = token.clone();
    let task_intent = intent.clone();
    runtime.spawn(async move {
        tx.send(call_remote(&task_state, &task_token, "send_message", task_intent).await)
            .unwrap();
    });
    let reached = hook.wait_reached(Duration::from_secs(10)).unwrap();
    let errors = if mode == "partial-write" {
        assert_eq!(reached.stage, FaultStage::PartialWriteFailed);
        let initial = rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap_err();
        let retry = runtime
            .block_on(call_remote(&state, &token, "send_message", intent))
            .unwrap_err();
        let uncertain = cooperators::error::ErrorCode::OutcomeUnknown.rpc_code();
        assert_eq!(initial.code, uncertain);
        assert_eq!(
            retry.code, uncertain,
            "uncertain identity must remain fenced until reopen"
        );
        vec![
            json!({"code": initial.code, "message": initial.message}),
            json!({"code": retry.code, "message": retry.message}),
        ]
    } else {
        assert!(
            rx.try_recv().is_err(),
            "receipt preceded required publication boundary"
        );
        Vec::new()
    };
    let disk = LogFile::<Envelope>::read_segments(&path).unwrap();
    if mode == "partial-write" {
        assert!(disk.records.is_empty());
        assert_eq!(disk.truncated_bytes, 9);
    } else {
        assert_eq!(disk.records.len(), 1);
        assert_eq!(disk.truncated_bytes, 0);
    }
    write_json(
        &evidence.join("boundary.json"),
        &json!({
            "mode": mode, "hook": observation(&hook.observation()),
            "successful_receipts": [], "errors": errors,
            "disk_records": disk.records, "truncated_bytes": disk.truncated_bytes
        }),
    );
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

fn crash_control(mode: &str) {
    let (evidence, _temporary) = evidence_dir(mode);
    let root = evidence.join("root");
    let runtime = runtime();
    let prepared = runtime.block_on(prepare(&root));
    write_json(
        &evidence.join("policy.json"),
        &serde_json::to_value(&prepared.policy).unwrap(),
    );
    write_json(&evidence.join("intent.json"), &prepared.intent);
    let path = inbox(&root, &prepared.recipient_id);
    let Prepared {
        state,
        policy,
        token,
        recipient_id,
        intent,
    } = prepared;
    drop(state);
    let program = std::env::current_exe().unwrap();
    let child = Command::new(&program)
        .args([
            "--exact",
            "operation_crash_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("COOP_OPERATION_CRASH_MODE", mode)
        .env("COOP_OPERATION_CRASH_ROOT", &evidence)
        .env("COOP_OPERATION_CRASH_TOKEN", &token)
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
            "child boundary timed out: {}",
            evidence.display()
        );
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited: {}",
            evidence.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
    let boundary = read_json(&evidence.join("boundary.json"));
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    assert_eq!(status.signal(), Some(9), "control requires actual SIGKILL");
    write_json(
        &evidence.join("process-result.json"),
        &json!({
            "mode": mode, "pid": pid, "program": program,
            "program_sha256": sha256_hex(&fs::read(&program).unwrap()),
            "exit_status": status.to_string(), "exit_code": status.code(),
            "signal": status.signal(), "child_reaped": true,
            "process_kill_only_not_power_loss": true
        }),
    );
    let before = fs::read(&path).unwrap();
    let state = Arc::new(DaemonState::owned(config(&root, policy)).unwrap());
    let expected_operation = intent["operation"].clone();
    let receipt = runtime
        .block_on(call_remote(&state, &token, "send_message", intent.clone()))
        .unwrap();
    let retry = runtime
        .block_on(call_remote(&state, &token, "send_message", intent))
        .unwrap();
    assert_eq!(
        receipt, retry,
        "retry must return original receipt without changing duplicate flag"
    );
    let recovered = LogFile::<Envelope>::read_segments(&path).unwrap();
    let effects = recovered
        .records
        .iter()
        .filter(|record| record.operation.is_some())
        .collect::<Vec<_>>();
    assert_eq!(effects.len(), 1);
    let effect = effects[0];
    assert_eq!(
        serde_json::to_value(&effect.operation.as_ref().unwrap().operation).unwrap(),
        expected_operation
    );
    assert_eq!(recovered.truncated_bytes, 0);
    assert_eq!(receipt["message_id"], effect.id);
    assert_eq!(receipt["seq"], effect.seq);
    assert_eq!(receipt["ts"], effect.ts);
    assert_eq!(receipt["to_agent_id"], recipient_id);
    if mode != "partial-write" {
        assert_eq!(
            serde_json::to_value(&recovered.records).unwrap(),
            boundary["disk_records"]
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            before,
            "recovered complete proof must not append"
        );
    } else {
        // Repair publishes the existing local recovery notice before replaying
        // the caller's intent. It is not a second application effect.
        assert_eq!(recovered.records.len(), 2);
        assert_eq!(recovered.records[0].kind, "recovery");
        assert_eq!(
            recovered.records[0].from,
            cooperators::messaging::DAEMON_SENDER
        );
        let notice: Value = serde_json::from_str(&recovered.records[0].body.text).unwrap();
        assert_eq!(notice["truncated_bytes"], 9);
    }
    let page = runtime
        .block_on(state.wait_message(&recipient_id, Some("0"), Duration::ZERO))
        .unwrap();
    assert_eq!(page.messages, recovered.records);
    write_json(
        &evidence.join("recovery.json"),
        &json!({
            "receipt": receipt, "repeated_receipt": retry, "records": recovered.records,
            "operation_record_hash": hash_record(effect).unwrap(),
            "operation_effect_count": effects.len(),
            "exactly_one_effect": true, "complete_source_bytes_preserved": mode != "partial-write",
            "repaired_partial_tail": mode == "partial-write"
        }),
    );
}

#[test]
fn sigkill_before_inbox_sync_recovers_complete_operation_proof_once() {
    crash_control("before-sync");
}

#[test]
fn sigkill_after_inbox_sync_before_receipt_recovers_original_outcome() {
    crash_control("after-sync-before-reply");
}

#[test]
fn sigkill_after_partial_write_repairs_then_replays_original_intent_once() {
    crash_control("partial-write");
}

#[test]
fn preoperation_envelope_hash_and_absent_optional_fields_remain_identical() {
    // This frozen v1 envelope/hash predates durable_operations_v1. Reading old
    // bytes must not materialize a null operation field and change the chain.
    let value = json!({
        "v": 1, "id": "01J8ZQ6S41TSV4RRFFQ69G5FAV", "seq": 412,
        "ts": "2026-10-18T12:34:56.789Z", "from": "codex:/root/task1",
        "to": "claude:reviewer-1", "kind": "request",
        "reply_to": "01J8ZQ6S40TSV4RRFFQ69G5FAV", "idempotency_key": "sha256:abc123",
        "body": {"mime": "text/markdown", "text": "please review"},
        "prev_hash": cooperators::storage::GENESIS_HASH
    });
    let envelope: Envelope = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(&envelope).unwrap(), value);
    assert_eq!(
        hash_record(&envelope).unwrap(),
        "sha256:72d45fb74874923ae3f4ed2beb54bbede71276305ae1fd2f23e78fbbcee52d0b"
    );
}

#[test]
fn preoperation_binary_refuses_upgraded_root_before_repairing_any_source() {
    let Some(program) = std::env::var_os("COOP_PREOPERATION_BINARY") else {
        assert!(
            std::env::var_os("COOP_REQUIRE_REMOTE").is_none(),
            "COOP_PREOPERATION_BINARY required for N3a qualification"
        );
        eprintln!("COOP_PREOPERATION_BINARY unset: old executable control not run");
        return;
    };
    let program = PathBuf::from(program);
    let (evidence, _temporary) = evidence_dir("preoperation-refusal");
    let root = evidence.join("root");
    let runtime = runtime();
    let prepared = runtime.block_on(prepare(&root));
    runtime
        .block_on(call_remote(
            &prepared.state,
            &prepared.token,
            "send_message",
            prepared.intent,
        ))
        .unwrap();
    let paths = [
        inbox(&root, &prepared.recipient_id),
        root.join("docs/guard.jsonl"),
        root.join("queues/guard.jsonl"),
    ];
    drop(prepared.state);
    for path in &paths {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap()
            .write_all(b"{\"torn_guard_tail\":")
            .unwrap();
    }
    let before = paths
        .iter()
        .map(|path| fs::read(path).unwrap())
        .collect::<Vec<_>>();
    let registry_before = fs::read(root.join("registry.json")).unwrap();
    let registry: Value = serde_json::from_slice(&registry_before).unwrap();
    assert!(registry["required_features"]
        .as_array()
        .unwrap()
        .contains(&json!("durable_operations_v1")));
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
            "old binary did not refuse upgraded root"
        );
        thread::sleep(Duration::from_millis(10));
    };
    let stderr = fs::read_to_string(evidence.join("old.stderr.log")).unwrap();
    assert!(!status.success());
    assert!(
        (stderr.contains("unsupported required registry feature")
            && stderr.contains("durable_operations_v1"))
            || stderr.contains("unknown field `operation_epoch_expires_at_ms`"),
        "{stderr}"
    );
    assert_eq!(
        fs::read(root.join("registry.json")).unwrap(),
        registry_before
    );
    assert!(!root.join("run/daemon.sock").exists());
    let snapshots = paths.iter().zip(&before).map(|(path, bytes)| {
        let after = fs::read(path).unwrap();
        assert_eq!(&after, bytes, "old binary repaired {}", path.display());
        json!({"path": path.strip_prefix(&root).unwrap(), "bytes": bytes.len(), "before_sha256": sha256_hex(bytes), "after_sha256": sha256_hex(&after), "identical": true})
    }).collect::<Vec<_>>();
    write_json(
        &evidence.join("guard-result.json"),
        &json!({
            "program": program, "program_sha256": sha256_hex(&fs::read(&program).unwrap()),
            "exit_status": status.to_string(), "exit_code": status.code(), "signal": status.signal(),
            "stderr": stderr, "source_files": snapshots, "registry_identical": true,
            "no_socket_created": true, "child_reaped": true
        }),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_https_reply_after_commit_recovers_exact_original_receipt() {
    let (evidence, _temporary) = evidence_dir("https-lost-reply");
    let prepared = prepare(&evidence.join("root")).await;
    let Some(tls) = TlsServer::start(&evidence, &prepared.state, "drop-send-once").await else {
        return;
    };
    let client = tls.client(&prepared);
    let mut submitted = prepared.intent.clone();
    submitted.as_object_mut().unwrap().remove("operation");
    submitted["token"] = prepared.token.clone().into();
    let journal_path = evidence.join("client-journal");
    let journal = OperationJournal::open(&journal_path).unwrap();
    let entry = client
        .prepare_operation(&journal, "send_message", submitted, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(entry.outcome, JournalOutcome::Pending);
    assert_eq!(journal.get(&entry.operation.id).unwrap(), entry);
    assert!(!serde_json::to_string(&entry)
        .unwrap()
        .contains(&prepared.token));
    let path = inbox(prepared.state.root(), &prepared.recipient_id);
    assert!(LogFile::<Envelope>::read_segments(&path)
        .unwrap()
        .records
        .is_empty());
    let error = client
        .submit_operation(
            &journal,
            &entry.operation.id,
            &prepared.token,
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ClientError::OutcomeUnknown(_)), "{error}");
    assert_eq!(
        journal.get(&entry.operation.id).unwrap().outcome,
        JournalOutcome::Unresolved
    );
    drop(journal);
    let events = fs::read_to_string(evidence.join("events.jsonl")).unwrap();
    assert_eq!(
        events
            .lines()
            .filter(|line| {
                let event = serde_json::from_str::<Value>(line).unwrap();
                event["method"] == "send_message" && event["path"] == "/v1/rpc"
            })
            .count(),
        1,
        "client must not silently retry an interrupted mutation"
    );
    let dropped = events
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["event"] == "dropped_send_reply")
        .unwrap();
    assert_eq!(dropped["upstream_status"], 200);
    assert!(dropped["error"].is_null());
    assert!(dropped["result"].is_object());
    let before = fs::read(&path).unwrap();
    let journal = OperationJournal::open(&journal_path).unwrap();
    let resumed = journal.get(&entry.operation.id).unwrap();
    assert_eq!(resumed.operation, entry.operation);
    assert_eq!(resumed.intent, entry.intent);
    assert_eq!(resumed.outcome, JournalOutcome::Unresolved);
    let receipt = client
        .submit_operation(
            &journal,
            &entry.operation.id,
            &prepared.token,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(receipt, dropped["result"]);
    assert_eq!(
        journal.get(&entry.operation.id).unwrap().outcome,
        JournalOutcome::Completed {
            result: receipt.clone()
        }
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    let records = LogFile::<Envelope>::read_segments(&path).unwrap();
    assert_eq!(records.records.len(), 1);
    assert_eq!(receipt["message_id"], records.records[0].id);
    assert_eq!(receipt["ts"], records.records[0].ts);
    assert!(!events.contains(&prepared.token));
    write_json(
        &evidence.join("recovery.json"),
        &json!({
            "first_result": "OUTCOME_UNKNOWN", "committed_upstream_receipt": dropped["result"],
            "retry_receipt": receipt, "exactly_one_effect": true, "source_bytes_preserved": true,
            "journal_reopened": true, "operation_reference_preserved": true,
            "credential_free_journal": !serde_json::to_string(&journal.list().unwrap()).unwrap().contains(&prepared.token),
            "transport": "loopback HTTPS ingress to loopback HTTP broker", "lan_or_internet_qualified": false
        }),
    );
}

struct TlsServer {
    origin: String,
    _proxy: ChildGuard,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Drop for TlsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TlsServer {
    async fn start(evidence: &Path, state: &Arc<DaemonState>, mode: &str) -> Option<Self> {
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.raw_os_error() == Some(1) => {
                assert!(
                    std::env::var_os("COOP_REQUIRE_REMOTE").is_none()
                        && std::env::var_os("COOP_REQUIRE_UDS").is_none(),
                    "required TLS fixture denied: {error}"
                );
                eprintln!("SKIP operation TLS: sandbox TCP denial");
                return None;
            }
            Err(error) => panic!("HTTP bind: {error}"),
        };
        let port = listener.local_addr().unwrap().port();
        let state = Arc::clone(state);
        let task = tokio::spawn(async move { cooperators::mcp::serve_http(listener, state).await });
        let ready = evidence.join("ready.json");
        let child = Command::new("python3")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/network_tls_proxy.py"),
            )
            .args(["--upstream-port", &port.to_string(), "--mode", mode])
            .arg("--ready")
            .arg(&ready)
            .arg("--events")
            .arg(evidence.join("events.jsonl"))
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                File::create(evidence.join("proxy.stderr.log")).unwrap(),
            ))
            .spawn()
            .unwrap();
        let mut server = Self {
            origin: String::new(),
            _proxy: ChildGuard(child),
            task,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() {
            assert!(
                Instant::now() < deadline && server._proxy.0.try_wait().unwrap().is_none(),
                "TLS proxy did not start"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        server.origin = format!("https://localhost:{}", read_json(&ready)["port"]);
        Some(server)
    }

    fn client(&self, prepared: &Prepared) -> BrokerClient {
        BrokerClient::https(
            &self.origin,
            HttpsOptions {
                ca_pem: Some(include_bytes!("fixtures/tls/ca.pem").to_vec()),
                ..Default::default()
            },
        )
        .unwrap()
        .with_expected_bus_id(prepared.state.network().unwrap().meta.bus_id.clone())
        .unwrap()
    }

    fn command(&self, prepared: &Prepared, journal: &Path) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_coop"));
        command
            .args([
                "--endpoint",
                &self.origin,
                "--expected-bus-id",
                &prepared.state.network().unwrap().meta.bus_id,
                "--ca-cert",
            ])
            .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls/ca.pem"))
            .arg("--operation-journal")
            .arg(journal)
            .env("COOP_TOKEN", &prepared.token)
            .env_remove("COOP_ENDPOINT")
            .env_remove("COOP_SOCKET")
            .env_remove("COOPERATORS_HOME");
        command
    }

    async fn mcp_request(&self, token: &str, method: &str, params: Value) -> Value {
        let http = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .add_root_certificate(
                reqwest::Certificate::from_pem(include_bytes!("fixtures/tls/ca.pem")).unwrap(),
            )
            .build()
            .unwrap();
        let response = http
            .post(format!("{}/mcp", self.origin))
            .bearer_auth(token)
            .header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0", "id":7, "method":method, "params":params}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let bytes = response.bytes().await.unwrap();
        serde_json::from_slice(&bytes).unwrap_or_else(|_| {
            std::str::from_utf8(&bytes)
                .unwrap()
                .lines()
                .find_map(|line| {
                    line.strip_prefix("data: ")
                        .and_then(|data| serde_json::from_str(data).ok())
                })
                .unwrap()
        })
    }

    async fn mcp_tool(&self, token: &str, name: &str, arguments: Value) -> Value {
        self.mcp_request(
            token,
            "tools/call",
            json!({"name":name,"arguments":arguments}),
        )
        .await["result"]
            .clone()
    }
}

fn cli_output(command: &mut Command, token: &str) -> Value {
    let output = command.output().unwrap();
    assert!(!String::from_utf8_lossy(&output.stdout).contains(token));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(token));
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resource_families_recover_dropped_https_receipts_from_source_after_restart() {
    for method in ["publish", "enqueue", "update_doc"] {
        let (evidence, _temporary) = evidence_dir(&format!("domain-lost-{method}"));
        let root = evidence.join("root");
        let _root_lease = cooperators::server::acquire_root_lease(&root).unwrap();
        let prepared = prepare_resources(&root).await;
        let identity = prepared.state.authenticate(&prepared.token).await.unwrap();
        let (mut params, source) = match method {
            "publish" => (
                json!({"channel":"#durable","text":"channel payload"}),
                root.join("channels/#durable.jsonl"),
            ),
            "enqueue" => {
                prepared.state.create_queue("durable").await.unwrap();
                (
                    json!({"queue":"durable","payload":{"task":"once"}}),
                    root.join("queues/durable.jsonl"),
                )
            }
            _ => {
                prepared
                    .state
                    .publish_doc(
                        &identity,
                        "durable",
                        "text/plain",
                        cooperators::envelope::Body::new("text/plain", "base"),
                        None,
                    )
                    .await
                    .unwrap();
                (
                    json!({"name":"durable","base_version":1,"text":"updated once"}),
                    root.join("docs/durable.jsonl"),
                )
            }
        };
        let Some(server) =
            TlsServer::start(&evidence, &prepared.state, &format!("drop-method-{method}")).await
        else {
            return;
        };
        let client = server.client(&prepared);
        let journal_path = evidence.join("journal");
        let journal = OperationJournal::open(&journal_path).unwrap();
        params["token"] = json!(prepared.token);
        let entry = client
            .prepare_operation(&journal, method, params, Duration::from_secs(5))
            .await
            .unwrap();
        let error = client
            .submit_operation(
                &journal,
                &entry.operation.id,
                &prepared.token,
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, ClientError::OutcomeUnknown(_)),
            "{method}: {error}"
        );
        let dropped = fs::read_to_string(evidence.join("events.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .find(|event| event["event"] == "dropped_mutation_reply")
            .unwrap();
        assert!(dropped["error"].is_null(), "{dropped}");
        let before = fs::read(&source).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&before)
                .lines()
                .filter(|line| line.contains("\"domain_operation\""))
                .count(),
            1
        );
        let repeated = client
            .submit_operation(
                &journal,
                &entry.operation.id,
                &prepared.token,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(repeated, dropped["result"]);
        assert_eq!(fs::read(&source).unwrap(), before);
        server.task.abort();
        drop(server);
        drop(client);
        let token = prepared.token.clone();
        let policy = prepared.policy.clone();
        drop(prepared);
        tokio::task::yield_now().await;
        let recovered = Arc::new(DaemonState::with_config(config(&root, policy)).unwrap());
        let mut retry = entry.intent.clone();
        retry["operation"] = serde_json::to_value(&entry.operation).unwrap();
        let receipt = call_remote(&recovered, &token, method, retry)
            .await
            .unwrap();
        assert_eq!(receipt, dropped["result"]);
        assert_eq!(fs::read(&source).unwrap(), before);
        write_json(
            &evidence.join("result.json"),
            &json!({"method":method,"original":dropped["result"],"recovered":receipt,"source_sha256":sha256_hex(&before),"one_source_proof":true,"actual_topology":"loopback TLS"}),
        );
    }
}

#[test]
fn send_only_binary_refuses_resource_format_before_repair() {
    let Some(program) = std::env::var_os("COOP_PRERESOURCE_BINARY") else {
        assert!(
            std::env::var_os("COOP_REQUIRE_REMOTE").is_none(),
            "COOP_PRERESOURCE_BINARY required for N3b qualification"
        );
        eprintln!("COOP_PRERESOURCE_BINARY unset: resource format control not run");
        return;
    };
    let (evidence, _temporary) = evidence_dir("preresource-refusal");
    let root = evidence.join("root");
    let rt = runtime();
    let prepared = rt.block_on(prepare_resources(&root));
    drop(prepared);
    let paths = [
        root.join("channels/guard.jsonl"),
        root.join("queues/guard.jsonl"),
        root.join("docs/guard.jsonl"),
        root.join("operations/outcomes.jsonl"),
    ];
    for path in &paths {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap()
            .write_all(b"{\"torn_resource_tail\":")
            .unwrap();
    }
    let before = paths
        .iter()
        .map(|p| fs::read(p).unwrap())
        .collect::<Vec<_>>();
    let registry_before = fs::read(root.join("registry.json")).unwrap();
    let output = Command::new(program)
        .arg("--root")
        .arg(&root)
        .args(["daemon", "run", "--http-addr", "off"])
        .env_remove("COOPERATORS_HOME")
        .env_remove("COOP_SOCKET")
        .env_remove("COOP_TOKEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("durable_resource_operations_v1"),
        "{stderr}"
    );
    assert_eq!(
        fs::read(root.join("registry.json")).unwrap(),
        registry_before
    );
    for (path, expected) in paths.iter().zip(&before) {
        assert_eq!(fs::read(path).unwrap(), *expected);
    }
    write_json(
        &evidence.join("result.json"),
        &json!({"refused":true,"required_feature":"durable_resource_operations_v1","all_source_bytes_preserved":true}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_claim_terminal_error_is_journaled_after_lost_reply_and_later_enqueue() {
    let (evidence, _temporary) = evidence_dir("empty-claim-journal");
    let root = evidence.join("root");
    let _root_lease = cooperators::server::acquire_root_lease(&root).unwrap();
    let prepared = prepare_resources(&root).await;
    prepared.state.create_queue("empty").await.unwrap();
    let Some(server) = TlsServer::start(&evidence, &prepared.state, "drop-method-claim_task").await
    else {
        return;
    };
    let client = server.client(&prepared);
    let journal_path = evidence.join("journal");
    let journal = OperationJournal::open(&journal_path).unwrap();
    let entry = client
        .prepare_operation(
            &journal,
            "claim_task",
            json!({"token":prepared.token,"queue":"empty"}),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(matches!(
        client
            .submit_operation(
                &journal,
                &entry.operation.id,
                &prepared.token,
                Duration::from_secs(5)
            )
            .await,
        Err(ClientError::OutcomeUnknown(_))
    ));
    let dropped = fs::read_to_string(evidence.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["event"] == "dropped_mutation_reply")
        .unwrap();
    let original: cooperators::rpc::RpcError =
        serde_json::from_value(dropped["error"].clone()).unwrap();
    assert_eq!(
        original.code,
        cooperators::error::ErrorCode::NotFound.rpc_code()
    );
    assert_eq!(
        original.data.as_ref().unwrap()["durable_operation"]["state"],
        "terminal_no_effect"
    );
    prepared
        .state
        .enqueue("empty", json!({"later":"task"}), None)
        .await
        .unwrap();
    let before = fs::read(root.join("queues/empty.jsonl")).unwrap();
    let error = client
        .submit_operation(
            &journal,
            &entry.operation.id,
            &prepared.token,
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert!(matches!(error,ClientError::Rpc(ref value) if value == &original));
    assert_eq!(
        journal.get(&entry.operation.id).unwrap().outcome,
        JournalOutcome::Rejected {
            error: original.clone()
        }
    );
    drop(journal);
    assert_eq!(
        OperationJournal::open(&journal_path)
            .unwrap()
            .get(&entry.operation.id)
            .unwrap()
            .outcome,
        JournalOutcome::Rejected {
            error: original.clone()
        }
    );
    assert_eq!(fs::read(root.join("queues/empty.jsonl")).unwrap(), before);
    let mut fresh = json!({"queue":"empty","operation":entry.operation});
    fresh["operation"]["id"] = json!(cooperators::id::new_ulid().unwrap());
    let claim = call_remote(&prepared.state, &prepared.token, "claim_task", fresh)
        .await
        .unwrap();
    assert_eq!(claim["task"]["payload"]["later"], "task");
    write_json(
        &evidence.join("result.json"),
        &json!({"terminal_error":original,"journal_state":"rejected","same_operation_did_not_claim_later_task":true,"fresh_operation_claimed":claim["task"]["task_id"]}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_fresh_sends_are_distinct_and_explicit_resume_preserves_receipt() {
    let (evidence, _temporary) = evidence_dir("cli-journal");
    let prepared = prepare(&evidence.join("root")).await;
    let Some(tls) = TlsServer::start(&evidence, &prepared.state, "proxy").await else {
        return;
    };
    let journal_path = evidence.join("client-journal");
    let first = cli_output(
        tls.command(&prepared, &journal_path).args([
            "send",
            "--to",
            &prepared.recipient_id,
            "--text",
            "fresh CLI intent",
        ]),
        &prepared.token,
    );
    let journal = OperationJournal::open(&journal_path).unwrap();
    let entries = journal.list().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].outcome,
        JournalOutcome::Completed {
            result: first.clone()
        }
    );
    let before_events = fs::read(evidence.join("events.jsonl")).unwrap();
    // Listing uses only local state, even with an unreachable selected endpoint.
    let listed = cli_output(
        Command::new(env!("CARGO_BIN_EXE_coop"))
            .args(["--endpoint", "https://localhost:1", "--operation-journal"])
            .arg(&journal_path)
            .args(["operations", "list"])
            .env_remove("COOP_TOKEN")
            .env_remove("COOP_ENDPOINT"),
        &prepared.token,
    );
    assert_eq!(
        listed["operations"],
        serde_json::to_value(&entries).unwrap()
    );
    assert_eq!(
        fs::read(evidence.join("events.jsonl")).unwrap(),
        before_events
    );
    let second = cli_output(
        tls.command(&prepared, &journal_path).args([
            "send",
            "--to",
            &prepared.recipient_id,
            "--text",
            "fresh CLI intent",
        ]),
        &prepared.token,
    );
    let fresh = journal.list().unwrap();
    assert_eq!(fresh.len(), 2);
    assert_ne!(fresh[0].operation.id, fresh[1].operation.id);
    assert_ne!(first["message_id"], second["message_id"]);
    let path = inbox(prepared.state.root(), &prepared.recipient_id);
    let before = fs::read(&path).unwrap();
    let resumed = cli_output(
        tls.command(&prepared, &journal_path).args([
            "operations",
            "resume",
            &entries[0].operation.id,
        ]),
        &prepared.token,
    );
    assert_eq!(resumed, first);
    assert_eq!(fs::read(&path).unwrap(), before);
    let records = LogFile::<Envelope>::read_segments(&path).unwrap();
    assert_eq!(records.records.len(), 2);
    assert!(records
        .records
        .iter()
        .all(|record| record.operation.is_some()));
    write_json(
        &evidence.join("result.json"),
        &json!({"first":first,"second":second,"resumed":resumed,"fresh_ids_distinct":true,"operation_effect_count":2,"list_is_offline":true}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_mcp_explicit_operation_replays_and_binds_header_credentials() {
    let (evidence, _temporary) = evidence_dir("http-mcp-operation");
    let prepared = prepare(&evidence.join("root")).await;
    let Some(tls) = TlsServer::start(&evidence, &prepared.state, "proxy").await else {
        return;
    };
    let info = tls
        .client(&prepared)
        .discover_info(Some(&prepared.token), Duration::from_secs(5))
        .await
        .unwrap();
    let expected: BTreeSet<String> = [
        "instance_metadata",
        "list_agents",
        "list_arrival_subscriptions",
        "list_docs",
        "ping",
        "read_channel",
        "read_doc",
        "wait_arrivals",
        "wait_message",
        "operation_status",
        "send_message",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    assert_eq!(
        info.supported_methods.into_iter().collect::<BTreeSet<_>>(),
        expected
    );
    let tools = tls
        .mcp_request(&prepared.token, "tools/list", json!({}))
        .await;
    let tools = tools["result"]["tools"].as_array().unwrap();
    let send = tools
        .iter()
        .find(|tool| tool["name"] == "coop_send_message")
        .unwrap();
    assert!(send["inputSchema"]["required"]
        .as_array()
        .unwrap()
        .contains(&json!("operation")));
    assert!(send["inputSchema"]["properties"]
        .get("idempotency_key")
        .is_none());
    assert!(!tools.iter().any(|tool| tool["name"] == "coop_ack"));
    let missing = {
        let mut args = prepared.intent.clone();
        args.as_object_mut().unwrap().remove("operation");
        tls.mcp_tool(&prepared.token, "coop_send_message", args)
            .await
    };
    assert_eq!(missing["isError"], true);
    let first = tls
        .mcp_tool(
            &prepared.token,
            "coop_send_message",
            prepared.intent.clone(),
        )
        .await;
    assert_ne!(first["isError"], true, "{first}");
    let path = inbox(prepared.state.root(), &prepared.recipient_id);
    let before = fs::read(&path).unwrap();
    let duplicate = tls
        .mcp_tool(
            &prepared.token,
            "coop_send_message",
            prepared.intent.clone(),
        )
        .await;
    assert_eq!(duplicate["structuredContent"], first["structuredContent"]);
    assert_eq!(fs::read(&path).unwrap(), before);
    let borrowed = prepared
        .state
        .register_agent("borrowed".into(), None, None, None)
        .await
        .unwrap();
    let mut replaced = prepared.intent.clone();
    replaced["token"] = borrowed.token.into();
    let denied = tls
        .mcp_tool(&prepared.token, "coop_send_message", replaced)
        .await;
    assert_eq!(denied["isError"], true);
    assert_eq!(denied["structuredContent"]["data"]["code"], "FORBIDDEN");
    assert_eq!(fs::read(&path).unwrap(), before);
    write_json(
        &evidence.join("result.json"),
        &json!({"original":first["structuredContent"],"duplicate":duplicate["structuredContent"],"operation_effect_count":1,"replacement_denied":true,"supported_methods":expected}),
    );
}

struct OperationStdio {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    next_id: u64,
}

impl OperationStdio {
    async fn start(tls: &TlsServer, prepared: &Prepared, journal: &Path) -> Self {
        let mut command: tokio::process::Command = tls.command(prepared, journal).into();
        command
            .args(["mcp-stdio", "--resume"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut process = Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        };
        let initialized = process.request("initialize", json!({"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"operation-test","version":"1"}})).await;
        assert!(initialized.get("error").is_none());
        process
            .send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await;
        process
    }

    async fn send(&mut self, value: Value) {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        self.stdin.write_all(&bytes).await.unwrap();
        self.stdin.flush().await.unwrap();
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .await;
        loop {
            let mut line = String::new();
            let bytes =
                tokio::time::timeout(Duration::from_secs(10), self.stdout.read_line(&mut line))
                    .await
                    .expect("stdio response deadline")
                    .unwrap();
            assert_ne!(bytes, 0, "stdio closed before result");
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["id"] == id {
                return value;
            }
        }
    }

    async fn tool(&mut self, name: &str, arguments: Value) -> Value {
        self.request("tools/call", json!({"name":name,"arguments":arguments}))
            .await["result"]
            .clone()
    }

    async fn stop(mut self) {
        drop(self.stdin);
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_stdio_journals_fresh_intents_and_resumes_only_matching_saved_intent() {
    let (evidence, _temporary) = evidence_dir("stdio-journal");
    let prepared = prepare(&evidence.join("root")).await;
    let Some(tls) = TlsServer::start(&evidence, &prepared.state, "proxy").await else {
        return;
    };
    let journal_path = evidence.join("client-journal");
    let mut process = OperationStdio::start(&tls, &prepared, &journal_path).await;
    let tools = process.request("tools/list", json!({})).await;
    assert!(tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tool| tool["name"] == "coop_send_message"));
    let args = json!({"to":prepared.recipient_id,"kind":"text","text":"fresh stdio intent"});
    let first = process.tool("coop_send_message", args.clone()).await;
    assert_ne!(first["isError"], true, "{first}");
    let journal = OperationJournal::open(&journal_path).unwrap();
    let initial = journal.list().unwrap();
    assert_eq!(initial.len(), 1);
    let second = process.tool("coop_send_message", args.clone()).await;
    assert_ne!(second["isError"], true, "{second}");
    let entries = journal.list().unwrap();
    assert_eq!(entries.len(), 2);
    assert_ne!(entries[0].operation.id, entries[1].operation.id);
    assert_ne!(
        first["structuredContent"]["message_id"],
        second["structuredContent"]["message_id"]
    );
    let path = inbox(prepared.state.root(), &prepared.recipient_id);
    let before = fs::read(&path).unwrap();
    let mut resumed_args = initial[0].intent.clone();
    resumed_args["operation"] = serde_json::to_value(&initial[0].operation).unwrap();
    let resumed = process
        .tool(
            "coop_resume_operation",
            json!({"id":initial[0].operation.id}),
        )
        .await;
    assert_eq!(resumed["structuredContent"], first["structuredContent"]);
    resumed_args["text"] = "changed intent".into();
    assert_eq!(
        process.tool("coop_send_message", resumed_args).await["isError"],
        true
    );
    let borrowed = prepared
        .state
        .register_agent("borrowed".into(), None, None, None)
        .await
        .unwrap();
    let mut replacement = args;
    replacement["token"] = borrowed.token.into();
    let denied = process.tool("coop_send_message", replacement).await;
    assert_eq!(denied["isError"], true);
    assert_eq!(denied["structuredContent"]["data"]["code"], "FORBIDDEN");
    assert_eq!(journal.list().unwrap().len(), 2);
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        LogFile::<Envelope>::read_segments(&path)
            .unwrap()
            .records
            .len(),
        2
    );
    process.stop().await;
    assert!(
        prepared
            .state
            .network()
            .unwrap()
            .authenticate(&prepared.state, &prepared.token)
            .await
            .is_ok(),
        "borrowed credential must survive stdio exit"
    );
    assert!(!serde_json::to_string(&journal.list().unwrap())
        .unwrap()
        .contains(&prepared.token));
    write_json(
        &evidence.join("result.json"),
        &json!({"first":first["structuredContent"],"second":second["structuredContent"],"resumed":resumed["structuredContent"],"operation_effect_count":2,"borrowed_identity_retained":true,"changed_resume_rejected":true,"replacement_denied":true}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_status_tracks_pending_commit_and_revokes_historical_receipt_access() {
    let (evidence, _temporary) = evidence_dir("status-authority");
    let prepared = prepare(&evidence.join("root")).await;
    let Some(tls) = TlsServer::start(&evidence, &prepared.state, "proxy").await else {
        return;
    };
    let client = tls.client(&prepared);
    let status_params = json!({"token":prepared.token,"operation":prepared.intent["operation"]});
    let unseen = client
        .call(
            "operation_status",
            status_params.clone(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(unseen, json!({"state":"unseen"}));
    let path = inbox(prepared.state.root(), &prepared.recipient_id);
    let hook = arm_append(&path, AppendFault::PauseBeforeSync).unwrap();
    let mut send_params = prepared.intent.clone();
    send_params["token"] = prepared.token.clone().into();
    let send_client = tls.client(&prepared);
    let send = tokio::spawn(async move {
        send_client
            .call("send_message", send_params, Duration::from_secs(10))
            .await
    });
    let reached = hook.wait_reached(Duration::from_secs(5)).unwrap();
    assert!(!reached.sync_attempted);
    let pending = client
        .call(
            "operation_status",
            status_params.clone(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(pending, json!({"state":"pending"}));
    hook.release(SyncOutcome::Succeed);
    let receipt = send.await.unwrap().unwrap();
    let committed = client
        .call(
            "operation_status",
            status_params.clone(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(committed, json!({"state":"committed","result":receipt}));
    let before = fs::read(&path).unwrap();
    for revoked in ["send_message", "operation_status"] {
        let mut policy = prepared.policy.clone();
        policy.devices[0]
            .permissions
            .retain(|permission| permission.method != revoked);
        policy_approval::replace_network_policy(&prepared.state, policy)
            .await
            .unwrap();
        let denied = client
            .call(
                "operation_status",
                status_params.clone(),
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();
        assert_eq!(
            denied.error_code(),
            Some(cooperators::error::ErrorCode::Forbidden)
        );
    }
    policy_approval::replace_network_policy(&prepared.state, prepared.policy.clone())
        .await
        .unwrap();
    let restored = client
        .call("operation_status", status_params, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(restored, committed);
    assert_eq!(fs::read(&path).unwrap(), before);
    write_json(
        &evidence.join("result.json"),
        &json!({"unseen":unseen,"pending":pending,"committed":committed,"restored":restored,"source_permission_revocation_denied":true,"status_permission_revocation_denied":true,"source_bytes_preserved":true}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_stdio_lost_reply_resumes_using_only_id_reported_in_error() {
    let (evidence, _temporary) = evidence_dir("stdio-lost-reply-resume-id");
    let prepared = prepare(&evidence.join("root")).await;
    let Some(tls) = TlsServer::start(&evidence, &prepared.state, "drop-send-once").await else {
        return;
    };
    let journal_path = evidence.join("client-journal");
    let mut process = OperationStdio::start(&tls, &prepared, &journal_path).await;
    let catalog = process.request("tools/list", json!({})).await;
    let resume_tool = catalog["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "coop_resume_operation")
        .expect("ID-only recovery tool advertised");
    assert_eq!(resume_tool["inputSchema"]["required"], json!(["id"]));
    assert_eq!(
        resume_tool["inputSchema"]["properties"]
            .as_object()
            .unwrap()
            .len(),
        1
    );
    let unknown = process.tool("coop_send_message", json!({
        "to":prepared.recipient_id, "kind":"text", "text":"resume through agent-visible ID only"
    })).await;
    assert_eq!(unknown["isError"], true);
    assert_eq!(
        unknown["structuredContent"]["data"]["code"],
        "OUTCOME_UNKNOWN"
    );
    let detail = unknown["structuredContent"]["message"].as_str().unwrap();
    let id = detail
        .split(|character: char| !character.is_ascii_alphanumeric())
        .find(|part| cooperators::id::is_ulid(part))
        .expect("unknown response exposes saved operation ID")
        .to_owned();
    let journal = OperationJournal::open(&journal_path).unwrap();
    let unresolved = journal.get(&id).unwrap();
    assert_eq!(unresolved.outcome, JournalOutcome::Unresolved);
    let events = fs::read_to_string(evidence.join("events.jsonl")).unwrap();
    let committed = events
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["event"] == "dropped_send_reply")
        .unwrap()["result"]
        .clone();
    assert!(committed.is_object());
    let path = inbox(prepared.state.root(), &prepared.recipient_id);
    let before = fs::read(&path).unwrap();
    // The model supplies only the ID from its error. Epoch, deadline, recipient
    // and text are recovered by the local adapter from the existing journal.
    let resumed = process
        .tool("coop_resume_operation", json!({"id":id}))
        .await;
    assert_ne!(resumed["isError"], true, "{resumed}");
    assert_eq!(resumed["structuredContent"], committed);
    assert_eq!(
        journal.get(&id).unwrap().outcome,
        JournalOutcome::Completed {
            result: committed.clone()
        }
    );
    let extra_credential = process
        .tool(
            "coop_resume_operation",
            json!({"id":id,"token":"replacement"}),
        )
        .await;
    assert_eq!(extra_credential["isError"], true);
    let missing = process
        .tool(
            "coop_resume_operation",
            json!({"id":cooperators::id::new_ulid().unwrap()}),
        )
        .await;
    assert_eq!(missing["isError"], true);
    assert_eq!(journal.list().unwrap().len(), 1);
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        LogFile::<Envelope>::read_segments(&path)
            .unwrap()
            .records
            .len(),
        1
    );
    process.stop().await;
    write_json(
        &evidence.join("result.json"),
        &json!({
            "operation_id_from_error":id, "initial_code":"OUTCOME_UNKNOWN",
            "upstream_receipt":committed, "resumed_receipt":resumed["structuredContent"],
            "resume_arguments":["id"], "operation_effect_count":1, "source_bytes_preserved":true,
            "additional_credential_rejected":true, "unknown_saved_id_rejected":true
        }),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncertain_registry_activation_must_sync_recovered_guard_before_source_repair() {
    let (evidence, _temporary) = evidence_dir("registry-recovery-sync");
    let root = evidence.join("root");
    // This control tests registry durability, not owner handoff. Retain one
    // explicit owner across the failed constructors so concurrent test-process
    // launches cannot affect immediate nonblocking lease reacquisition.
    let _ownership = cooperators::server::acquire_root_lease(&root).unwrap();
    let prepared = prepare_with_operations(&root, false, false).await;
    let Prepared {
        state,
        mut policy,
        token,
        recipient_id,
        intent,
    } = prepared;
    drop(state);
    let source_paths = [
        inbox(&root, &recipient_id),
        root.join("docs/guard.jsonl"),
        root.join("queues/guard.jsonl"),
    ];
    for path in &source_paths {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap()
            .write_all(b"{\"torn_guard_tail\":")
            .unwrap();
    }
    let snapshots = source_paths
        .iter()
        .map(|path| fs::read(path).unwrap())
        .collect::<Vec<_>>();
    policy.enable_operations = true;
    policy.revision += 1;
    let registry_path = root.join("registry.json");
    // Approval now has its own durable snapshot before operation activation.
    // Permit that write, then fail the operation guard's replacement so this
    // control still proves recovery of the exact durable operation epoch.
    let approval = arm_point(
        &registry_path,
        FaultPoint::AfterRegistryReplace,
        PointFault::Pause,
    )
    .unwrap();
    let activating = {
        let root = root.clone();
        let policy = policy.clone();
        tokio::task::spawn_blocking(move || DaemonState::with_config(config(&root, policy)))
    };
    approval.wait_reached(Duration::from_secs(5)).unwrap();
    let activation = arm_point(
        &registry_path,
        FaultPoint::AfterRegistryReplace,
        PointFault::Fail,
    )
    .unwrap();
    approval.release(SyncOutcome::Succeed);
    let activation_error = activating
        .await
        .unwrap()
        .err()
        .expect("activation must fail at injected boundary");
    drop(approval);
    assert_eq!(
        activation.observation().stage,
        FaultStage::InjectedFailure,
        "unexpected activation error: {activation_error}"
    );
    let guard_bytes = fs::read(&registry_path).unwrap();
    let guard: Value = serde_json::from_slice(&guard_bytes).unwrap();
    assert!(guard["required_features"]
        .as_array()
        .unwrap()
        .contains(&json!("durable_operations_v1")));
    for (path, bytes) in source_paths.iter().zip(&snapshots) {
        assert_eq!(
            fs::read(path).unwrap(),
            *bytes,
            "activation repaired source before proven registry sync"
        );
    }
    let recovery_sync = arm_point(
        &registry_path,
        FaultPoint::BeforeRecoverySync,
        PointFault::Fail,
    )
    .unwrap();
    let source_sync = arm_point(
        &source_paths[0],
        FaultPoint::BeforeRecoverySync,
        PointFault::Fail,
    )
    .unwrap();
    let recovery_error = DaemonState::with_config(config(&root, policy.clone()))
        .err()
        .expect("registry recovery sync must fail at injected boundary");
    assert_eq!(
        recovery_sync.observation().stage,
        FaultStage::InjectedFailure,
        "unexpected recovery error: {recovery_error}"
    );
    assert_eq!(
        source_sync.observation().stage,
        FaultStage::Armed,
        "source recovery ran before registry recovery sync"
    );
    assert_eq!(fs::read(&registry_path).unwrap(), guard_bytes);
    let unchanged = source_paths.iter().zip(&snapshots).map(|(path, before)| {
        let after = fs::read(path).unwrap();
        assert_eq!(&after, before, "failed registry recovery sync repaired a source");
        json!({"path":path.strip_prefix(&root).unwrap(),"before_sha256":sha256_hex(before),"after_sha256":sha256_hex(&after)})
    }).collect::<Vec<_>>();
    let registry_observation = observation(&recovery_sync.observation());
    drop(source_sync);
    drop(recovery_sync);
    drop(activation);
    let state = Arc::new(DaemonState::with_config(config(&root, policy)).unwrap());
    assert_eq!(
        fs::read(&registry_path).unwrap(),
        guard_bytes,
        "reopen must retain the original feature and epoch guard"
    );
    let receipt = call_remote(&state, &token, "send_message", intent)
        .await
        .unwrap();
    let records = LogFile::<Envelope>::read_segments(&source_paths[0]).unwrap();
    assert_eq!(records.truncated_bytes, 0);
    assert_eq!(
        records
            .records
            .iter()
            .filter(|record| record.operation.is_some())
            .count(),
        1
    );
    assert_eq!(receipt["message_id"], records.records.last().unwrap().id);
    assert_eq!(fs::read(&registry_path).unwrap(), guard_bytes);
    write_json(
        &evidence.join("result.json"),
        &json!({
            "activation_failed_after_replace":true,"recovery_sync_failure":registry_observation,
            "source_snapshots_unchanged_before_sync":unchanged,"source_sync_not_reached":true,
            "registry_sha256":sha256_hex(&guard_bytes),"exact_guard_preserved_after_successful_reopen":true,
            "post_recovery_receipt":receipt,"post_recovery_operation_effect_count":1
        }),
    );
}

fn domain_source(root: &Path, method: &str) -> PathBuf {
    root.join(match method {
        "publish" => "channels/#crash.jsonl",
        "enqueue" => "queues/crash.jsonl",
        "update_doc" => "docs/crash.jsonl",
        _ => panic!("unsupported domain crash method"),
    })
}

fn domain_records(path: &Path, method: &str) -> (Vec<Value>, u64) {
    fn values<T: serde::Serialize>(
        report: cooperators::storage::ReplayReport<T>,
    ) -> (Vec<Value>, u64) {
        (
            report
                .records
                .iter()
                .map(|record| serde_json::to_value(record).unwrap())
                .collect(),
            report.truncated_bytes,
        )
    }
    match method {
        "publish" => values(LogFile::<Envelope>::read_segments(path).unwrap()),
        "enqueue" => {
            values(LogFile::<cooperators::queues::QueueEvent>::read_segments(path).unwrap())
        }
        "update_doc" => {
            values(LogFile::<cooperators::docs::DocEvent>::read_segments(path).unwrap())
        }
        _ => panic!("unsupported domain crash method"),
    }
}

#[test]
fn domain_crash_child() {
    let Some(method) = std::env::var_os("COOP_DOMAIN_CRASH_METHOD") else {
        return;
    };
    let method = method.to_str().unwrap();
    let mode = std::env::var("COOP_DOMAIN_CRASH_MODE").unwrap();
    let evidence = PathBuf::from(std::env::var_os("COOP_DOMAIN_CRASH_ROOT").unwrap());
    let root = evidence.join("root");
    let intent = read_json(&evidence.join("intent.json"));
    let policy: NetworkPolicy =
        serde_json::from_value(read_json(&evidence.join("policy.json"))).unwrap();
    let token = std::env::var("COOP_DOMAIN_CRASH_TOKEN").unwrap();
    std::env::remove_var("COOP_DOMAIN_CRASH_TOKEN");
    let runtime = runtime();
    let state = Arc::new(DaemonState::owned(config(&root, policy)).unwrap());
    let source = domain_source(&root, method);
    let hook = match mode.as_str() {
        "before-sync" => arm_append(&source, AppendFault::PauseBeforeSync).unwrap(),
        "partial-write" => arm_append(&source, AppendFault::PartialWrite { bytes: 9 }).unwrap(),
        "after-sync-before-reply" => arm_point(
            &source,
            FaultPoint::AfterOperationSyncBeforePublish,
            PointFault::Pause,
        )
        .unwrap(),
        _ => panic!("unknown domain crash mode"),
    };
    let (tx, rx) = mpsc::channel();
    let task_state = Arc::clone(&state);
    let task_token = token.clone();
    let task_intent = intent.clone();
    let task_method = method.to_owned();
    runtime.spawn(async move {
        tx.send(call_remote(&task_state, &task_token, &task_method, task_intent).await)
            .unwrap();
    });
    let reached = hook.wait_reached(Duration::from_secs(10)).unwrap();
    let errors = if mode == "partial-write" {
        assert_eq!(reached.stage, FaultStage::PartialWriteFailed);
        assert_eq!(reached.bytes_written, 9);
        assert!(!reached.sync_attempted);
        let first = rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap_err();
        let repeated = runtime
            .block_on(call_remote(&state, &token, method, intent.clone()))
            .unwrap_err();
        assert_eq!(
            first.code,
            cooperators::error::ErrorCode::OutcomeUnknown.rpc_code()
        );
        assert_eq!(
            repeated.code, first.code,
            "uncertain operation must remain fenced until authoritative reopen"
        );
        vec![first, repeated]
    } else {
        assert_eq!(
            reached.stage,
            if mode == "before-sync" {
                FaultStage::BeforeSync
            } else {
                FaultStage::Reached
            }
        );
        if mode == "before-sync" {
            assert!(!reached.sync_attempted);
        }
        assert!(
            rx.try_recv().is_err(),
            "caller received success before selected crash boundary"
        );
        Vec::new()
    };
    let (records, truncated) = domain_records(&source, method);
    let proofs = records
        .iter()
        .filter_map(|record| record.get("domain_operation"))
        .collect::<Vec<_>>();
    if mode == "partial-write" {
        assert_eq!(truncated, 9);
        assert!(proofs.is_empty());
    } else {
        assert_eq!(truncated, 0);
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0]["context"]["operation"], intent["operation"]);
        assert_eq!(proofs[0]["context"]["method"], method);
        assert_eq!(proofs[0]["outcome"]["kind"], "success");
        assert!(proofs[0]["outcome"]["value"].is_object());
    }
    write_json(
        &evidence.join("boundary.json"),
        &json!({
            "method":method,"mode":mode,"hook":observation(&hook.observation()),
            "successful_receipts":[],"errors":errors,"disk_records":records,"truncated_bytes":truncated,
            "original_source_outcome":proofs.first().map(|proof|proof["outcome"]["value"].clone())
        }),
    );
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

fn domain_crash_control(method: &str, mode: &str) {
    let (evidence, _temporary) = evidence_dir(&format!("domain-crash-{method}-{mode}"));
    let root = evidence.join("root");
    let runtime = runtime();
    // Fixture setup has one owner in this process and no listener. The actual
    // child obtains the production root lease; after SIGKILL it is reaped before
    // the parent obtains one lease for all remaining replay assertions.
    let prepared = runtime.block_on(prepare_resources(&root));
    let identity = runtime
        .block_on(prepared.state.authenticate(&prepared.token))
        .unwrap();
    let mut intent = match method {
        "publish" => json!({"channel":"#crash","text":"publish exactly once after crash"}),
        "enqueue" => {
            runtime
                .block_on(prepared.state.create_queue("crash"))
                .unwrap();
            json!({"queue":"crash","payload":{"task":"enqueue exactly once after crash"},"max_attempts":3})
        }
        "update_doc" => {
            runtime
                .block_on(prepared.state.publish_doc(
                    &identity,
                    "crash",
                    "text/plain",
                    cooperators::envelope::Body::new("text/plain", "initial version"),
                    None,
                ))
                .unwrap();
            json!({"name":"crash","base_version":1,"text":"update exactly once after crash"})
        }
        _ => panic!("unsupported domain crash method"),
    };
    intent["operation"] = json!({
        "v":1,"id":cooperators::id::new_ulid().unwrap(),
        "epoch":prepared.state.network().unwrap().meta.operation_epoch,
        "not_after_ms":cooperators::id::unix_millis()+600_000
    });
    write_json(&evidence.join("intent.json"), &intent);
    write_json(
        &evidence.join("policy.json"),
        &serde_json::to_value(&prepared.policy).unwrap(),
    );
    let policy = prepared.policy.clone();
    let token = prepared.token.clone();
    drop(prepared);
    let program = std::env::current_exe().unwrap();
    let child = Command::new(&program)
        .args([
            "--exact",
            "domain_crash_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("COOP_DOMAIN_CRASH_METHOD", method)
        .env("COOP_DOMAIN_CRASH_MODE", mode)
        .env("COOP_DOMAIN_CRASH_ROOT", &evidence)
        .env("COOP_DOMAIN_CRASH_TOKEN", &token)
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
            "domain child boundary timed out: {}",
            evidence.display()
        );
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "domain child exited before boundary: {}",
            evidence.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
    let boundary = read_json(&evidence.join("boundary.json"));
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    assert_eq!(
        status.signal(),
        Some(9),
        "domain crash control requires actual SIGKILL"
    );
    write_json(
        &evidence.join("process-result.json"),
        &json!({
            "method":method,"mode":mode,"pid":pid,"program":program,
            "program_sha256":sha256_hex(&fs::read(&program).unwrap()),
            "signal":status.signal(),"exit_code":status.code(),"exit_status":status.to_string(),
            "child_reaped":true,"process_kill_only_not_power_loss":true
        }),
    );
    let source = domain_source(&root, method);
    let before = fs::read(&source).unwrap();
    let _root_lease = cooperators::server::acquire_root_lease(&root).unwrap();
    let state = Arc::new(DaemonState::with_config(config(&root, policy)).unwrap());
    let receipt = runtime
        .block_on(call_remote(&state, &token, method, intent.clone()))
        .unwrap();
    let repeated = runtime
        .block_on(call_remote(&state, &token, method, intent.clone()))
        .unwrap();
    assert_eq!(
        receipt, repeated,
        "replay must preserve every original receipt field"
    );
    let (records, truncated) = domain_records(&source, method);
    assert_eq!(truncated, 0);
    let effects = records
        .iter()
        .filter(|record| record.get("domain_operation").is_some())
        .collect::<Vec<_>>();
    assert_eq!(
        effects.len(),
        1,
        "same intent must have exactly one authoritative source proof"
    );
    let effect = effects[0];
    let proof = &effect["domain_operation"];
    assert_eq!(proof["context"]["operation"], intent["operation"]);
    assert_eq!(proof["context"]["method"], method);
    assert_eq!(proof["outcome"]["kind"], "success");
    assert_eq!(proof["outcome"]["value"], receipt);
    let effect_kind = match method {
        "publish" => "publish",
        "enqueue" => "task_enqueued",
        "update_doc" => "doc_updated",
        _ => unreachable!(),
    };
    assert_eq!(
        records
            .iter()
            .filter(|record| record["kind"] == effect_kind)
            .count(),
        1
    );
    if mode != "partial-write" {
        assert_eq!(receipt, boundary["original_source_outcome"]);
        assert_eq!(
            serde_json::to_value(&records).unwrap(),
            boundary["disk_records"]
        );
        assert_eq!(
            fs::read(&source).unwrap(),
            before,
            "complete source proof must recover without appending"
        );
    } else {
        assert_eq!(
            records
                .iter()
                .filter(|record| record["kind"] == "recovery")
                .count(),
            1
        );
        assert_eq!(boundary["truncated_bytes"], 9);
    }
    if method == "update_doc" {
        let document = runtime.block_on(state.read_doc("crash", None)).unwrap();
        assert_eq!(document.version, 2);
        assert_eq!(document.versions, 2);
        assert_eq!(document.body.text, "update exactly once after crash");
    }
    write_json(
        &evidence.join("recovery.json"),
        &json!({
            "method":method,"mode":mode,"receipt":receipt,"repeated_receipt":repeated,
            "records":records,"operation_effect_count":effects.len(),"operation_record_hash":hash_record(effect).unwrap(),
            "source_sha256":sha256_hex(&fs::read(&source).unwrap()),
            "complete_source_bytes_preserved":mode!="partial-write","repaired_partial_tail":mode=="partial-write"
        }),
    );
}

#[test]
fn publish_sigkill_before_sync_partial_write_and_after_sync_recovers_once() {
    for mode in ["before-sync", "partial-write", "after-sync-before-reply"] {
        domain_crash_control("publish", mode);
    }
}

#[test]
fn enqueue_sigkill_before_sync_partial_write_and_after_sync_recovers_once() {
    for mode in ["before-sync", "partial-write", "after-sync-before-reply"] {
        domain_crash_control("enqueue", mode);
    }
}

#[test]
fn update_doc_sigkill_before_sync_partial_write_and_after_sync_recovers_once() {
    for mode in ["before-sync", "partial-write", "after-sync-before-reply"] {
        domain_crash_control("update_doc", mode);
    }
}
