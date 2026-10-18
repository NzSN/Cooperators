//! Real-daemon acceptance: CLI round-trip and restart durability across
//! `kill -9`.
//!
//! The agent sandbox denies AF_UNIX with EPERM. Probe first and report a skip
//! instead of a misleading pass/fail; set `COOP_REQUIRE_UDS=1` to turn the
//! skip into a failure when running the real gate outside the sandbox.

mod common;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};

fn unix_sockets_available(temp: &Path) -> bool {
    let probe = temp.join("probe.sock");
    match std::os::unix::net::UnixListener::bind(&probe) {
        Ok(listener) => {
            drop(listener);
            let _ = std::fs::remove_file(&probe);
            true
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::PermissionDenied
                && error.raw_os_error() == Some(1) =>
        {
            if std::env::var_os("COOP_REQUIRE_UDS").is_some() {
                panic!("AF_UNIX bind denied and COOP_REQUIRE_UDS is set: {error}");
            }
            eprintln!("SKIP m1_daemon: AF_UNIX bind denied by the agent sandbox (EPERM)");
            false
        }
        Err(error) => panic!("probe AF_UNIX: {error}"),
    }
}

struct DaemonProcess {
    child: Child,
}

impl DaemonProcess {
    fn spawn(root: &Path, log_name: &str) -> Self {
        std::fs::create_dir_all(root).expect("create bus root");
        let log = File::create(root.join(log_name)).expect("daemon log file");
        let child = Command::new(env!("CARGO_BIN_EXE_coop"))
            .args(["daemon", "run"])
            .env("COOPERATORS_HOME", root)
            .env_remove("COOP_SOCKET")
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn coop daemon");
        Self { child }
    }

    fn wait_for_socket(&mut self, socket: &Path, log_path: &Path) {
        for _ in 0..400 {
            if std::os::unix::net::UnixStream::connect(socket).is_ok() {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("poll daemon") {
                let log = std::fs::read_to_string(log_path).unwrap_or_default();
                panic!("daemon exited before listening ({status}):\n{log}");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let log = std::fs::read_to_string(log_path).unwrap_or_default();
        panic!("daemon did not listen on {}:\n{log}", socket.display());
    }

    fn kill9(&mut self) {
        self.child.kill().expect("kill -9 daemon");
        self.child.wait().expect("reap daemon");
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn rpc(socket: &Path, method: &str, params: Value) -> Value {
    cooperators::client::call_at(socket, method, params, Duration::from_secs(10))
        .await
        .unwrap_or_else(|error| panic!("rpc {method} failed: {error}"))
}

fn coop(root: &Path, args: &[&str], token: Option<&str>) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_coop"));
    command
        .args(args)
        .env("COOPERATORS_HOME", root)
        .env_remove("COOP_SOCKET")
        .env_remove("COOP_TOKEN");
    if let Some(token) = token {
        command.env("COOP_TOKEN", token);
    }
    let output = command.output().expect("run coop CLI");
    assert!(
        output.status.success(),
        "coop {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "coop {args:?} did not print JSON: {error}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn inbox_line_count(root: &Path, agent_id: &str) -> usize {
    let path = root.join("agents").join(agent_id).join("inbox.jsonl");
    let bytes = std::fs::read(&path).expect("read inbox");
    bytes.iter().filter(|byte| **byte == b'\n').count()
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_9_restart_preserves_messages_dedupe_and_cli_roundtrip() {
    let temp = common::TempDir::new("m1-daemon");
    if !unix_sockets_available(temp.path()) {
        return;
    }
    let root: PathBuf = temp.path().join("root");
    let socket = root.join("run/daemon.sock");
    let log1 = root.join("daemon-1.stderr");
    let mut daemon = DaemonProcess::spawn(&root, "daemon-1.stderr");
    daemon.wait_for_socket(&socket, &log1);

    let alice = rpc(
        &socket,
        "register_agent",
        json!({"name": "alice", "kind": "codex"}),
    )
    .await;
    let bob = rpc(
        &socket,
        "register_agent",
        json!({"name": "bob", "kind": "claude"}),
    )
    .await;
    let alice_token = alice["token"].as_str().expect("alice token").to_string();
    let bob_token = bob["token"].as_str().expect("bob token").to_string();
    let bob_id = bob["agent_id"].as_str().expect("bob id").to_string();

    let sent = rpc(
        &socket,
        "send_message",
        json!({
            "token": alice_token,
            "to": "bob",
            "kind": "request",
            "body": {"mime": "text/markdown", "text": "survive kill -9"},
            "idempotency_key": "kill9-key",
        }),
    )
    .await;
    let first_seq = sent["seq"].clone();
    let first_id = sent["message_id"].clone();

    // CLI registration, send, wait, and ack against the live daemon.
    let carol = coop(&root, &["register", "carol", "--kind", "agent"], None);
    let carol_token = carol["token"].as_str().expect("carol token");
    coop(
        &root,
        &[
            "send", "--to", "bob", "--kind", "event", "--text", "from cli",
        ],
        Some(carol_token),
    );
    let waited = coop(
        &root,
        &["wait", "--cursor", "0", "--timeout", "1"],
        Some(&bob_token),
    );
    assert_eq!(
        waited["messages"].as_array().expect("messages").len(),
        2,
        "both messages visible before the crash"
    );
    let next_cursor = waited["next_cursor"].as_str().expect("next cursor");
    let acked = coop(&root, &["ack", "--cursor", next_cursor], Some(&bob_token));
    assert_eq!(acked["acked_cursor"], next_cursor);
    assert_eq!(inbox_line_count(&root, &bob_id), 2);

    // No graceful shutdown: SIGKILL the process and start a fresh daemon.
    daemon.kill9();
    let log2 = root.join("daemon-2.stderr");
    let mut restarted = DaemonProcess::spawn(&root, "daemon-2.stderr");
    restarted.wait_for_socket(&socket, &log2);

    let replay = rpc(
        &socket,
        "wait_message",
        json!({"token": bob_token, "cursor": "0", "timeout_s": 0}),
    )
    .await;
    assert_eq!(
        replay["messages"].as_array().expect("messages").len(),
        2,
        "messages intact after kill -9"
    );
    assert_eq!(replay["messages"][0]["seq"], first_seq);

    let repeat = rpc(
        &socket,
        "send_message",
        json!({
            "token": alice_token,
            "to": "bob",
            "kind": "request",
            "body": {"mime": "text/markdown", "text": "survive kill -9"},
            "idempotency_key": "kill9-key",
        }),
    )
    .await;
    assert_eq!(repeat["duplicate"], true);
    assert_eq!(repeat["seq"], first_seq);
    assert_eq!(repeat["message_id"], first_id);
    assert_eq!(
        inbox_line_count(&root, &bob_id),
        2,
        "dedupe survived crash recovery"
    );

    drop(restarted);
}
