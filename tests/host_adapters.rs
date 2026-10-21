//! Deterministic subprocess protocol coverage. The executable below is a local
//! fake; no installed provider CLI, credentials, or model API is invoked.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use cooperators::host::adapters::{
    ClaudeAdapterConfig, ClaudeHostAdapter, CodexAdapterConfig, CodexAppServerAdapter,
};
use cooperators::host::{
    Binding, HostAdapter, HostEvent, Readiness, SessionState, Submission, Wake,
};
use serde_json::{json, Value};

const SESSION: &str = "00000000-0000-4000-8000-000000000001";
static FAKE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn binding(workspace: &Path, kind: &str) -> Binding {
    Binding {
        binding_id: "binding-1".into(),
        generation: 3,
        bus_id: "bus-1".into(),
        agent_id: "agent-1".into(),
        adapter_kind: kind.into(),
        exact_host_session_id: SESSION.into(),
        workspace: workspace.to_string_lossy().into_owned(),
        capability_status: Readiness::Configured,
    }
}

fn wake(binding: &Binding, id: &str) -> Wake {
    Wake {
        wake_id: id.into(),
        binding_id: binding.binding_id.clone(),
        generation: binding.generation,
        bus_id: binding.bus_id.clone(),
        agent_id: binding.agent_id.clone(),
        notices: Vec::new(),
        observation_nonce: format!("nonce-{id}"),
        created_at_ms: 1,
    }
}

fn fake(root: &Path, kind: &str, mode: &str) -> (PathBuf, BTreeMap<String, String>) {
    // Restarts share journals and history, but never rewrite an executable
    // inode that an earlier process may still hold. Publish a fully synced,
    // closed script before handing its unique path to the process adapter.
    let sequence = FAKE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let executable = root.join(format!("fake-host-{}-{sequence}.py", std::process::id()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o700)
        .open(&executable)
        .expect("create unique fake host executable");
    file.write_all(FAKE_HOST.as_bytes())
        .expect("write fake host executable");
    file.sync_all().expect("sync fake host executable");
    drop(file);
    (
        executable,
        BTreeMap::from([
            ("COOP_FAKE_KIND".into(), kind.into()),
            ("COOP_FAKE_MODE".into(), mode.into()),
            ("COOP_FAKE_ROOT".into(), root.to_string_lossy().into_owned()),
            ("COOP_FAKE_SESSION".into(), SESSION.into()),
        ]),
    )
}

fn codex_config(root: &Path, mode: &str) -> CodexAdapterConfig {
    let (executable, env) = fake(root, "codex", mode);
    CodexAdapterConfig {
        executable,
        env,
        request_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

fn claude_config(root: &Path, mode: &str) -> ClaudeAdapterConfig {
    let (executable, env) = fake(root, "claude", mode);
    ClaudeAdapterConfig {
        executable,
        env,
        request_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

fn requests(root: &Path) -> Vec<Value> {
    fs::read_to_string(root.join("requests.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn approval(adapter: &mut dyn HostAdapter) -> Value {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            for event in adapter.drain_events() {
                if let HostEvent::Approval { approval } = event {
                    return approval.request_id;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("operator receives an approval request")
}

async fn eventually_idle(adapter: &mut dyn HostAdapter, binding: &Binding) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if adapter.session_state(binding).await.unwrap() == SessionState::Idle {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("host turn finishes");
}

fn assert_reaped(root: &Path) {
    let launch: Value =
        serde_json::from_slice(&fs::read(root.join("launch.json")).unwrap()).unwrap();
    let pid = launch["pid"].as_u64().unwrap();
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists(),
        "child {pid} is reaped"
    );
}

#[tokio::test]
async fn codex_bootstrap_creates_exact_thread_without_a_model_turn_and_reaps_child() {
    let dir = common::TempDir::new("host-codex-bootstrap");
    let mut config = codex_config(dir.path(), "normal");
    config
        .config_overrides
        .push("mcp_servers.cooperators.command=\"/local/coop\"".into());
    let id = CodexAppServerAdapter::create_session(&config, dir.path())
        .await
        .unwrap();
    assert_eq!(id, SESSION);
    let frames = requests(dir.path());
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0]["method"], "initialize");
    assert_eq!(frames[1]["method"], "initialized");
    assert_eq!(frames[2]["method"], "thread/start");
    assert_eq!(
        frames[2]["params"]["cwd"],
        dir.path().canonicalize().unwrap().to_str().unwrap()
    );
    let launch: Value =
        serde_json::from_slice(&fs::read(dir.path().join("launch.json")).unwrap()).unwrap();
    assert_eq!(
        launch["args"],
        json!([
            "app-server",
            "--listen",
            "stdio://",
            "-c",
            config.config_overrides[0]
        ])
    );
    assert_reaped(dir.path());
}

#[tokio::test]
async fn codex_bootstrap_and_resume_failure_reap_children() {
    for mode in ["wrong_workspace", "missing_thread", "malformed", "scalar"] {
        let dir = common::TempDir::new("host-codex-bootstrap-invalid");
        assert!(
            CodexAppServerAdapter::create_session(&codex_config(dir.path(), mode), dir.path())
                .await
                .is_err(),
            "{mode}"
        );
        assert_reaped(dir.path());
    }
    let dir = common::TempDir::new("host-codex-resume-invalid");
    assert!(CodexAppServerAdapter::spawn(
        codex_config(dir.path(), "wrong_thread"),
        &binding(dir.path(), "codex")
    )
    .await
    .is_err());
    assert_reaped(dir.path());
}

#[tokio::test]
async fn codex_initializes_exact_thread_and_fences_binding_and_generation() {
    let dir = common::TempDir::new("host-codex-exact");
    let binding = binding(dir.path(), "codex_app_server");
    let wake = wake(&binding, "wake-1");
    let mut adapter = CodexAppServerAdapter::spawn(codex_config(dir.path(), "normal"), &binding)
        .await
        .unwrap();
    assert_eq!(
        adapter.session_state(&binding).await.unwrap(),
        SessionState::Idle
    );
    assert_eq!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Accepted {
            turn_id: Some("turn-1".into())
        }
    );
    // Same logical wake does not add another turn on the live connection.
    assert!(matches!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Accepted { .. }
    ));
    let mut stale = wake.clone();
    stale.generation -= 1;
    assert!(adapter.submit_wake(&binding, &stale).await.is_err());
    let mut redirected = binding.clone();
    redirected.exact_host_session_id = "other-thread".into();
    assert!(adapter.session_state(&redirected).await.is_err());
    adapter.shutdown().await.unwrap();
    let frames = requests(dir.path());
    assert_eq!(frames[0]["method"], "initialize");
    assert_eq!(frames[1]["method"], "initialized");
    assert_eq!(frames[2]["method"], "thread/resume");
    assert_eq!(frames[2]["params"]["threadId"], SESSION);
    let starts: Vec<_> = frames
        .iter()
        .filter(|frame| frame["method"] == "turn/start")
        .collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0]["params"]["input"][0]["text"], wake.prompt());
    assert!(frames
        .iter()
        .all(|frame| frame["params"].get("approvalPolicy").is_none()));
    let launch: Value =
        serde_json::from_slice(&fs::read(dir.path().join("launch.json")).unwrap()).unwrap();
    assert_eq!(
        launch["args"],
        json!(["app-server", "--listen", "stdio://"])
    );
    let pid = launch["pid"].as_u64().unwrap();
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists(),
        "shutdown reaps child"
    );
}

#[tokio::test]
async fn codex_busy_state_never_starts_or_steers_a_turn() {
    let dir = common::TempDir::new("host-codex-busy");
    let binding = binding(dir.path(), "codex");
    let wake = wake(&binding, "wake-busy");
    let mut adapter = CodexAppServerAdapter::spawn(codex_config(dir.path(), "busy"), &binding)
        .await
        .unwrap();
    assert_eq!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Busy
    );
    assert!(!requests(dir.path())
        .iter()
        .any(|frame| matches!(frame["method"].as_str(), Some("turn/start" | "turn/steer"))));
    fs::write(dir.path().join("idle"), b"").unwrap();
    assert!(matches!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Accepted { .. }
    ));
    adapter.shutdown().await.unwrap();
}

#[tokio::test]
async fn codex_approval_is_retained_until_operator_answers_that_request() {
    let dir = common::TempDir::new("host-codex-approval");
    let binding = binding(dir.path(), "codex");
    let first = wake(&binding, "wake-approval");
    let next = wake(&binding, "wake-next");
    let mut adapter = CodexAppServerAdapter::spawn(codex_config(dir.path(), "approval"), &binding)
        .await
        .unwrap();
    assert!(matches!(
        adapter.submit_wake(&binding, &first).await.unwrap(),
        Submission::Accepted { .. }
    ));
    let id = approval(&mut adapter).await;
    assert_eq!(
        adapter.session_state(&binding).await.unwrap(),
        SessionState::WaitingForPermission
    );
    assert_eq!(
        adapter.submit_wake(&binding, &next).await.unwrap(),
        Submission::PermissionBlocked
    );
    assert!(adapter
        .drain_events()
        .iter()
        .any(|event| matches!(event, HostEvent::Approval { .. })));
    assert!(adapter
        .answer_approval(&json!("other"), json!({"decision":"accept"}))
        .await
        .is_err());
    assert!(adapter
        .answer_approval(&id, json!({"decision":"acceptForSession"}))
        .await
        .is_err());
    assert!(!requests(dir.path())
        .iter()
        .any(|frame| frame.get("result").is_some()));
    adapter
        .answer_approval(&id, json!({"decision":"decline"}))
        .await
        .unwrap();
    eventually_idle(&mut adapter, &binding).await;
    assert!(adapter
        .answer_approval(&id, json!({"decision":"accept"}))
        .await
        .is_err());
    adapter.shutdown().await.unwrap();
    let frames = requests(dir.path());
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame.get("result").is_some())
            .count(),
        1
    );
    assert!(frames
        .iter()
        .any(|frame| frame["id"] == id && frame["result"] == json!({"decision":"decline"})));
}

#[tokio::test]
async fn codex_lost_response_reconciles_exact_wake_from_history_after_restart() {
    let dir = common::TempDir::new("host-codex-lost");
    let binding = binding(dir.path(), "codex");
    let wake = wake(&binding, "wake-lost");
    let mut adapter = CodexAppServerAdapter::spawn(codex_config(dir.path(), "lost"), &binding)
        .await
        .unwrap();
    assert!(matches!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Unknown { .. }
    ));
    adapter.shutdown().await.unwrap();
    let mut resumed = CodexAppServerAdapter::spawn(codex_config(dir.path(), "normal"), &binding)
        .await
        .unwrap();
    assert_eq!(
        resumed.reconcile_wake(&binding, &wake, None).await.unwrap(),
        Submission::Accepted {
            turn_id: Some("turn-1".into())
        }
    );
    assert!(matches!(
        resumed.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Accepted { .. }
    ));
    resumed.shutdown().await.unwrap();
    assert_eq!(
        requests(dir.path())
            .iter()
            .filter(|frame| frame["method"] == "turn/start")
            .count(),
        1
    );
}

#[tokio::test]
async fn codex_reconciliation_does_not_treat_unrelated_turn_as_wake_acceptance() {
    let dir = common::TempDir::new("host-codex-history-identity");
    fs::write(dir.path().join("history.json"), serde_json::to_vec(&json!([
        {"turnId":"turn-unrelated", "item":{"type":"userMessage", "content":[{"type":"text", "text":"another request"}]}}
    ])).unwrap()).unwrap();
    let binding = binding(dir.path(), "codex");
    let wake = wake(&binding, "wake-not-accepted");
    let mut adapter = CodexAppServerAdapter::spawn(codex_config(dir.path(), "normal"), &binding)
        .await
        .unwrap();
    assert!(matches!(
        adapter
            .reconcile_wake(&binding, &wake, Some("turn-unrelated"))
            .await
            .unwrap(),
        Submission::Unknown { .. }
    ));
    adapter.shutdown().await.unwrap();
}

#[tokio::test]
async fn codex_approval_survives_timeout_and_bounded_diagnostic_overflow() {
    let dir = common::TempDir::new("host-codex-noisy-approval");
    let binding = binding(dir.path(), "codex");
    let wake = wake(&binding, "wake-noisy");
    let mut adapter =
        CodexAppServerAdapter::spawn(codex_config(dir.path(), "noisy_approval"), &binding)
            .await
            .unwrap();
    assert_eq!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::PermissionBlocked
    );
    let events = adapter.drain_events();
    assert!(events.len() <= 129);
    assert!(events.iter().any(|event| matches!(event, HostEvent::Notification { value } if value["type"] == "host_notification_omitted")));
    let id = events
        .iter()
        .find_map(|event| match event {
            HostEvent::Approval { approval } => Some(approval.request_id.clone()),
            _ => None,
        })
        .unwrap();
    assert!(adapter
        .drain_events()
        .iter()
        .any(|event| matches!(event, HostEvent::Approval { .. })));
    adapter
        .answer_approval(&id, json!({"decision":"decline"}))
        .await
        .unwrap();
    eventually_idle(&mut adapter, &binding).await;
    adapter.shutdown().await.unwrap();

    let dir = common::TempDir::new("host-codex-timeout-approval");
    let binding = self::binding(dir.path(), "codex");
    let wake = self::wake(&binding, "wake-timeout-approval");
    let mut adapter =
        CodexAppServerAdapter::spawn(codex_config(dir.path(), "approval_lost"), &binding)
            .await
            .unwrap();
    assert!(matches!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Unknown { .. }
    ));
    let id = approval(&mut adapter).await;
    adapter
        .answer_approval(&id, json!({"decision":"decline"}))
        .await
        .unwrap();
    eventually_idle(&mut adapter, &binding).await;
    assert!(matches!(
        adapter.reconcile_wake(&binding, &wake, None).await.unwrap(),
        Submission::Accepted { .. }
    ));
    adapter.shutdown().await.unwrap();
    assert_eq!(
        requests(dir.path())
            .iter()
            .filter(|frame| frame["method"] == "turn/start")
            .count(),
        1
    );
}

#[tokio::test]
async fn oversized_or_invalid_runtime_frames_stop_further_submissions() {
    for kind in ["codex", "claude"] {
        for mode in ["oversized", "runtime_scalar", "missing_session"] {
            if kind == "codex" && mode == "missing_session" {
                continue;
            }
            let dir = common::TempDir::new("host-runtime-invalid");
            let binding = binding(dir.path(), kind);
            let wake = wake(&binding, "wake-invalid");
            let mut adapter: Box<dyn HostAdapter> = if kind == "codex" {
                Box::new(
                    CodexAppServerAdapter::spawn(codex_config(dir.path(), mode), &binding)
                        .await
                        .unwrap(),
                )
            } else {
                Box::new(
                    ClaudeHostAdapter::spawn(claude_config(dir.path(), mode), &binding)
                        .await
                        .unwrap(),
                )
            };
            assert!(
                matches!(
                    adapter.submit_wake(&binding, &wake).await.unwrap(),
                    Submission::Unknown { .. }
                ),
                "{kind} {mode}"
            );
            assert_eq!(
                adapter.session_state(&binding).await.unwrap(),
                SessionState::Offline,
                "{kind} {mode}"
            );
            assert!(matches!(
                adapter.submit_wake(&binding, &wake).await.unwrap(),
                Submission::Unknown { .. }
            ));
            adapter.shutdown().await.unwrap();
            assert_reaped(dir.path());
            assert_eq!(
                requests(dir.path())
                    .iter()
                    .filter(|frame| frame["method"] == "turn/start" || frame["type"] == "user")
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn codex_wrong_thread_workspace_and_malformed_stream_fail_closed() {
    for mode in ["wrong_thread", "wrong_workspace", "malformed"] {
        let dir = common::TempDir::new("host-codex-invalid");
        let binding = binding(dir.path(), "codex");
        assert!(
            CodexAppServerAdapter::spawn(codex_config(dir.path(), mode), &binding)
                .await
                .is_err(),
            "mode {mode}"
        );
        assert!(!requests(dir.path())
            .iter()
            .any(|frame| frame["method"] == "turn/start"));
    }
}

#[tokio::test]
async fn claude_uses_persistent_host_mode_exact_session_and_explicit_approval() {
    let dir = common::TempDir::new("host-claude-protocol");
    let binding = binding(dir.path(), "claude_host");
    let first = wake(&binding, "wake-1");
    let next = wake(&binding, "wake-2");
    let mut adapter = ClaudeHostAdapter::spawn(claude_config(dir.path(), "normal"), &binding)
        .await
        .unwrap();
    assert!(!adapter.capabilities().reconciles_history);
    assert!(matches!(
        adapter.submit_wake(&binding, &first).await.unwrap(),
        Submission::Accepted { .. }
    ));
    let id = approval(&mut adapter).await;
    assert_eq!(
        adapter.submit_wake(&binding, &next).await.unwrap(),
        Submission::PermissionBlocked
    );
    assert!(adapter
        .answer_approval(&id, json!({"behavior":"allow", "permission_updates":[{}]}))
        .await
        .is_err());
    assert!(!requests(dir.path())
        .iter()
        .any(|frame| frame["type"] == "control_response"));
    adapter
        .answer_approval(&id, json!({"behavior":"allow"}))
        .await
        .unwrap();
    eventually_idle(&mut adapter, &binding).await;
    assert!(matches!(
        adapter.submit_wake(&binding, &next).await.unwrap(),
        Submission::Accepted { .. }
    ));
    let second_id = approval(&mut adapter).await;
    adapter
        .answer_approval(
            &second_id,
            json!({"behavior":"deny", "message":"operator declined"}),
        )
        .await
        .unwrap();
    eventually_idle(&mut adapter, &binding).await;
    adapter.shutdown().await.unwrap();
    let frames = requests(dir.path());
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame["type"] == "user")
            .count(),
        2
    );
    let replies: Vec<_> = frames
        .iter()
        .filter(|frame| frame["type"] == "control_response")
        .collect();
    assert_eq!(replies.len(), 2);
    assert_eq!(
        replies[0]["response"],
        json!({"subtype":"success", "request_id":id, "response":{"behavior":"allow"}})
    );
    let launch: Value =
        serde_json::from_slice(&fs::read(dir.path().join("launch.json")).unwrap()).unwrap();
    let args = launch["args"].as_array().unwrap();
    for pair in [
        ["--input-format", "stream-json"],
        ["--output-format", "stream-json"],
        ["--permission-prompt-tool", "stdio"],
        ["--permission-mode", "manual"],
        ["--resume", SESSION],
    ] {
        assert!(args
            .windows(2)
            .any(|window| window == [json!(pair[0]), json!(pair[1])]));
    }
    assert!(args.iter().all(|arg| !matches!(
        arg.as_str(),
        Some(
            "--dangerously-skip-permissions"
                | "--allowedTools"
                | "--continue"
                | "--dangerously-load-development-channels"
        )
    )));
}

#[tokio::test]
async fn claude_new_session_uses_bound_uuid_without_resume_or_fork() {
    let dir = common::TempDir::new("host-claude-new");
    let binding = binding(dir.path(), "claude");
    let mut config = claude_config(dir.path(), "normal");
    config.resume_existing = false;
    let mut adapter = ClaudeHostAdapter::spawn(config, &binding).await.unwrap();
    let wake = wake(&binding, "wake-new");
    assert!(matches!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Accepted { .. }
    ));
    adapter.shutdown().await.unwrap();
    let launch: Value =
        serde_json::from_slice(&fs::read(dir.path().join("launch.json")).unwrap()).unwrap();
    let args = launch["args"].as_array().unwrap();
    assert!(args
        .windows(2)
        .any(|pair| pair == [json!("--session-id"), json!(SESSION)]));
    assert!(!args.iter().any(|arg| matches!(
        arg.as_str(),
        Some("--resume" | "--fork-session" | "--continue")
    )));
    assert_reaped(dir.path());
}

#[tokio::test]
async fn claude_permission_before_echo_remains_reconcilable_without_resubmission() {
    let dir = common::TempDir::new("host-claude-blocked");
    let binding = binding(dir.path(), "claude");
    let wake = wake(&binding, "wake-blocked");
    let mut adapter =
        ClaudeHostAdapter::spawn(claude_config(dir.path(), "approval_before_echo"), &binding)
            .await
            .unwrap();
    assert_eq!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::PermissionBlocked
    );
    let id = approval(&mut adapter).await;
    adapter
        .answer_approval(
            &id,
            json!({"behavior":"allow", "updatedInput":{"command":"safe modified command"}}),
        )
        .await
        .unwrap();
    eventually_idle(&mut adapter, &binding).await;
    assert!(matches!(
        adapter.reconcile_wake(&binding, &wake, None).await.unwrap(),
        Submission::Accepted { .. }
    ));
    adapter.shutdown().await.unwrap();
    assert_eq!(
        requests(dir.path())
            .iter()
            .filter(|frame| frame["type"] == "user")
            .count(),
        1
    );
}

#[tokio::test]
async fn claude_never_claims_unknown_write_or_restart_as_acceptance() {
    let dir = common::TempDir::new("host-claude-unknown");
    let binding = binding(dir.path(), "claude");
    let wake = wake(&binding, "wake-unknown");
    let mut adapter = ClaudeHostAdapter::spawn(claude_config(dir.path(), "silent"), &binding)
        .await
        .unwrap();
    assert!(matches!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Unknown { .. }
    ));
    assert_eq!(
        adapter.reconcile_wake(&binding, &wake, None).await.unwrap(),
        Submission::Busy
    );
    adapter.shutdown().await.unwrap();
    let mut resumed = ClaudeHostAdapter::spawn(claude_config(dir.path(), "normal"), &binding)
        .await
        .unwrap();
    assert!(matches!(
        resumed
            .reconcile_wake(&binding, &wake, Some("unverified-id"))
            .await
            .unwrap(),
        Submission::Unknown { .. }
    ));
    resumed.shutdown().await.unwrap();
}

#[tokio::test]
async fn claude_wrong_session_and_stale_generation_never_get_a_second_wake() {
    let dir = common::TempDir::new("host-claude-wrong");
    let binding = binding(dir.path(), "claude");
    let wake = wake(&binding, "wake-wrong");
    let mut adapter =
        ClaudeHostAdapter::spawn(claude_config(dir.path(), "wrong_session"), &binding)
            .await
            .unwrap();
    let mut stale = wake.clone();
    stale.generation -= 1;
    assert!(adapter.submit_wake(&binding, &stale).await.is_err());
    assert!(matches!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Unknown { .. }
    ));
    assert_eq!(
        adapter.session_state(&binding).await.unwrap(),
        SessionState::Offline
    );
    assert!(matches!(
        adapter.submit_wake(&binding, &wake).await.unwrap(),
        Submission::Unknown { .. }
    ));
    adapter.shutdown().await.unwrap();
    assert_eq!(
        requests(dir.path())
            .iter()
            .filter(|frame| frame["type"] == "user")
            .count(),
        1
    );
}

const FAKE_HOST: &str = r##"#!/usr/bin/env python3
import json, os, pathlib, sys

root = pathlib.Path(os.environ['COOP_FAKE_ROOT'])
kind = os.environ['COOP_FAKE_KIND']
mode = os.environ['COOP_FAKE_MODE']
session = os.environ['COOP_FAKE_SESSION']
(root / 'launch.json').write_text(json.dumps({'args': sys.argv[1:], 'pid': os.getpid(), 'cwd': os.getcwd()}))
history_path = root / 'history.json'
history = json.loads(history_path.read_text()) if history_path.exists() else []
state = 'active' if mode == 'busy' else 'idle'
last = None
count = len(history)

def emit(value):
    print(json.dumps(value), flush=True)

def response(request, result):
    emit({'id': request['id'], 'result': result})

def thread():
    return {'id': 'wrong' if mode == 'wrong_thread' else session,
            'cwd': '/' if mode == 'wrong_workspace' else os.getcwd(),
            'status': {'type': 'idle' if (root / 'idle').exists() else state,
                       'activeFlags': ['waitingOnApproval'] if state == 'approval' else []}}

def claude_echo():
    emit({'type': 'user', 'session_id': session, 'uuid': 'echo-' + str(count),
          'message': last['message']})

for line in sys.stdin:
    request = json.loads(line)
    with (root / 'requests.jsonl').open('a') as output:
        output.write(json.dumps(request) + '\n')
    if kind == 'codex':
        method = request.get('method')
        if method == 'initialize':
            if mode == 'malformed':
                print('invalid-json', flush=True)
            elif mode == 'scalar':
                emit(17)
            else:
                response(request, {'userAgent': 'fake-codex/0.163.0'})
        elif method == 'initialized':
            pass
        elif method == 'thread/start':
            assert request['params']['cwd'] == os.getcwd()
            value = thread()
            if mode == 'missing_thread':
                del value['id']
            response(request, {'thread': value})
        elif method == 'thread/resume':
            assert request['params']['threadId'] == session
            response(request, {'thread': thread()})
        elif method == 'thread/read':
            assert request['params']['threadId'] == session
            if mode == 'noisy_approval':
                state = 'approval'
                emit({'id': 'approval-' + str(count), 'method': 'item/commandExecution/requestApproval',
                      'params': {'threadId': session, 'turnId': 'turn-noisy', 'command': 'gated command'}})
                for i in range(300):
                    emit({'method':'diagnostic', 'params':{'threadId':session, 'value':i}})
                emit({'method':'diagnostic', 'params':{'threadId':session, 'value':'x' * 100000}})
                mode = 'approval'
            value = thread()
            if state == 'approval':
                value['status']['type'] = 'active'
            response(request, {'thread': value})
        elif method == 'turn/start':
            assert request['params']['threadId'] == session
            if mode == 'oversized':
                print('x' * (4 * 1024 * 1024 + 1), flush=True)
                continue
            if mode == 'runtime_scalar':
                emit(17)
                continue
            count += 1
            turn_id = 'turn-' + str(count)
            history.append({'turnId': turn_id, 'item': {'type': 'userMessage', 'content': request['params']['input']}})
            history_path.write_text(json.dumps(history))
            state = 'active'
            if mode not in ['lost', 'approval_lost']:
                response(request, {'turn': {'id': turn_id, 'status': 'inProgress', 'items': []}})
            if mode in ['approval', 'approval_lost']:
                state = 'approval'
                emit({'id': 'approval-' + str(count), 'method': 'item/commandExecution/requestApproval',
                      'params': {'threadId': session, 'turnId': turn_id, 'command': 'gated command'}})
        elif method == 'thread/items/list':
            assert request['params']['threadId'] == session
            response(request, {'data': list(reversed(history)), 'nextCursor': None})
        elif method is None and 'result' in request:
            assert request['id'] == 'approval-' + str(count)
            assert request['result']['decision'] in ['accept', 'decline', 'cancel']
            state = 'idle'
            emit({'method':'turn/completed', 'params': {'threadId':session, 'turn':{'id':'turn-' + str(count), 'status':'completed'}}})
        else:
            raise AssertionError(request)
    else:
        if request['type'] == 'user':
            assert request['session_id'] == session
            count += 1
            last = request
            if mode == 'oversized':
                print('x' * (4 * 1024 * 1024 + 1), flush=True)
                continue
            if mode == 'runtime_scalar':
                emit(17)
                continue
            if mode == 'missing_session':
                emit({'type':'result', 'subtype':'success'})
                continue
            if mode == 'silent':
                continue
            emit({'type':'system', 'subtype':'init', 'session_id': 'wrong' if mode == 'wrong_session' else session})
            if mode != 'approval_before_echo':
                claude_echo()
            emit({'type':'control_request', 'request_id':'approval-' + str(count),
                  'request':{'subtype':'can_use_tool', 'tool_name':'Bash', 'input':{'command':'gated command'}}})
        elif request['type'] == 'control_response':
            assert request['response']['subtype'] == 'success'
            assert request['response']['request_id'] == 'approval-' + str(count)
            assert request['response']['response']['behavior'] in ['allow','deny']
            if mode == 'approval_before_echo':
                claude_echo()
            emit({'type':'result', 'subtype':'success', 'session_id':session, 'uuid':'result-' + str(count)})
        else:
            raise AssertionError(request)
"##;
