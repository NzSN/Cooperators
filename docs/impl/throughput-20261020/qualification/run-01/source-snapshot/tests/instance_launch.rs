//! Actual process acceptance for independently launched MCP clients. The UDS
//! profile probes sandbox socket support; COOP_REQUIRE_UDS makes a skip fail.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child as DaemonChild, Command as DaemonCommand, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

fn sockets_available(temp: &Path) -> bool {
    let probe = temp.join("probe.sock");
    match std::os::unix::net::UnixListener::bind(&probe) {
        Ok(listener) => {
            drop(listener);
            fs::remove_file(probe).unwrap();
            true
        }
        Err(failure) if matches!(failure.raw_os_error(), Some(1 | 13)) => {
            assert!(
                std::env::var_os("COOP_REQUIRE_UDS").is_none(),
                "required UDS profile unavailable: {failure}"
            );
            eprintln!("SKIP instance_launch UDS profile: {failure}; set COOP_REQUIRE_UDS=1 for the host gate");
            false
        }
        Err(failure) => panic!("UDS probe: {failure}"),
    }
}

struct Daemon {
    child: DaemonChild,
}

impl Daemon {
    async fn start(root: &Path) -> Self {
        fs::create_dir_all(root).unwrap();
        let stderr = fs::File::create(root.join("daemon-test.stderr")).unwrap();
        let child = DaemonCommand::new(env!("CARGO_BIN_EXE_coop"))
            .args(["daemon", "run"])
            .env("COOPERATORS_HOME", root)
            .env("COOP_HTTP_ADDR", "off")
            .env_remove("COOP_SOCKET")
            .env_remove("COOP_TOKEN")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn daemon");
        let mut daemon = Self { child };
        for _ in 0..400 {
            if tokio::net::UnixStream::connect(root.join("run/daemon.sock"))
                .await
                .is_ok()
            {
                return daemon;
            }
            if let Some(status) = daemon.child.try_wait().unwrap() {
                panic!(
                    "daemon exited {status}: {}",
                    fs::read_to_string(root.join("daemon-test.stderr")).unwrap_or_default()
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("daemon did not become ready");
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct McpProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl McpProcess {
    async fn start(root: &Path, token: &str, embedded: bool, resume: bool) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_coop"));
        command.args(["mcp-stdio", "--kind", "codex", "--display-name", "codex"]);
        if embedded {
            command.arg("--embedded");
        }
        if resume {
            command.arg("--resume");
        }
        let mut child = command
            .env("COOPERATORS_HOME", root)
            .env("COOP_TOKEN", token)
            .env_remove("COOP_SOCKET")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn MCP client");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut process = Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        };
        let initialize = process
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-03-26", "capabilities": {},
                    "clientInfo": {"name": "same-config-codex", "version": "0.1.0"}
                }),
            )
            .await;
        assert!(
            initialize.get("error").is_none(),
            "MCP initialize failed: {initialize}"
        );
        process
            .send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .await;
        process
    }

    async fn send(&mut self, message: Value) {
        let mut line = serde_json::to_vec(&message).unwrap();
        line.push(b'\n');
        self.stdin.write_all(&line).await.unwrap();
        self.stdin.flush().await.unwrap();
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await;
        loop {
            let mut line = String::new();
            let read =
                tokio::time::timeout(Duration::from_secs(15), self.stdout.read_line(&mut line))
                    .await
                    .expect("MCP response timeout")
                    .expect("read MCP response");
            assert_ne!(read, 0, "MCP process closed stdout");
            let message: Value = serde_json::from_str(&line).expect("MCP JSON");
            if message["id"] == id {
                return message;
            }
        }
    }

    async fn tool(&mut self, name: &str, arguments: Value) -> Result<Value, Value> {
        let response = self
            .request("tools/call", json!({"name": name, "arguments": arguments}))
            .await;
        assert!(
            response.get("error").is_none(),
            "tool protocol error: {response}"
        );
        let result = &response["result"];
        let structured = result["structuredContent"].clone();
        if result["isError"].as_bool().unwrap_or(false) {
            Err(structured)
        } else {
            Ok(structured)
        }
    }

    async fn stop(mut self) {
        drop(self.stdin);
        let status = tokio::time::timeout(Duration::from_secs(15), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success(), "MCP process exited {status}");
    }
}

async fn rpc(root: &Path, method: &str, params: Value) -> Value {
    cooperators::client::call_at(
        &root.join("run/daemon.sock"),
        method,
        params,
        Duration::from_secs(10),
    )
    .await
    .unwrap_or_else(|failure| panic!("RPC {method}: {failure}"))
}

fn rows(path: impl AsRef<Path>) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &entry.path(), files);
            } else if entry.file_type().unwrap().is_file() {
                files.insert(
                    entry.path().strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

fn retain(root: &Path, metadata: &Value, profile: &str) {
    if let Some(directory) = std::env::var_os("COOP_MBT_EVIDENCE_DIR") {
        let destination = PathBuf::from(directory)
            .join("instance-launch")
            .join(format!(
                "{profile}-{}",
                cooperators::id::new_ulid().unwrap()
            ));
        fs::create_dir_all(&destination).unwrap();
        for (relative, bytes) in snapshot(root) {
            let path = destination.join("root").join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
        fs::write(
            destination.join("metadata.json"),
            serde_json::to_vec_pretty(metadata).unwrap(),
        )
        .unwrap();
        eprintln!("instance launch evidence: {}", destination.display());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_mcp_processes_sharing_configuration_keep_independent_principals() {
    let temp = common::TempDir::new("instance-launch");
    if !sockets_available(temp.path()) {
        return;
    }
    let root = temp.path().join("root");
    let daemon = Daemon::start(&root).await;
    let shared = rpc(
        &root,
        "register_agent",
        json!({"name": "shared-config", "kind": "codex"}),
    )
    .await;
    let shared_token = shared["token"].as_str().unwrap();
    let mut first = McpProcess::start(&root, shared_token, false, false).await;
    let mut second = McpProcess::start(&root, shared_token, false, false).await;
    assert_ne!(first.child.id(), second.child.id());
    let a = first
        .tool("coop_instance_metadata", json!({}))
        .await
        .unwrap();
    let b = second
        .tool("coop_instance_metadata", json!({}))
        .await
        .unwrap();
    assert_ne!(a["agent_id"], b["agent_id"]);
    assert_ne!(a["name"], b["name"]);
    assert_ne!(a["agent_id"], shared["agent_id"]);
    assert_ne!(b["agent_id"], shared["agent_id"]);
    assert_eq!(a["kind"], "codex");
    assert_eq!(b["kind"], "codex");
    assert_eq!(a["display_name"], "codex");
    assert_eq!(b["display_name"], "codex");
    let raw_registry: Value =
        serde_json::from_slice(&fs::read(root.join("registry.json")).unwrap()).unwrap();
    let record_a = &raw_registry["agents"][a["name"].as_str().unwrap()];
    let record_b = &raw_registry["agents"][b["name"].as_str().unwrap()];
    assert_ne!(record_a["token_hash"], record_b["token_hash"]);
    assert_ne!(record_a["launch_key_hash"], record_b["launch_key_hash"]);
    eprintln!(
        "independent MCP PIDs {:?}/{:?}, actual IDs {}/{}, current credential hashes {}/{}",
        first.child.id(),
        second.child.id(),
        a["agent_id"],
        b["agent_id"],
        record_a["token_hash"],
        record_b["token_hash"]
    );

    let sent_a = first.tool("coop_send_message", json!({"to": b["agent_id"], "kind": "event", "text": "A", "idempotency_key": "same-key"})).await.unwrap();
    let sent_b = second.tool("coop_send_message", json!({"to": b["agent_id"], "kind": "event", "text": "B", "idempotency_key": "same-key"})).await.unwrap();
    assert_eq!(sent_a["duplicate"], false);
    assert_eq!(sent_b["duplicate"], false);
    assert_ne!(sent_a["message_id"], sent_b["message_id"]);
    let duplicate = first.tool("coop_send_message", json!({"to": b["agent_id"], "kind": "event", "text": "retry", "idempotency_key": "same-key"})).await.unwrap();
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["message_id"], sent_a["message_id"]);
    second
        .tool(
            "coop_send_message",
            json!({"to": a["agent_id"], "kind": "reply", "text": "to A"}),
        )
        .await
        .unwrap();
    let inbox_a = first
        .tool("coop_wait_message", json!({"timeout_s": 0}))
        .await
        .unwrap();
    let inbox_b = second
        .tool("coop_wait_message", json!({"timeout_s": 0}))
        .await
        .unwrap();
    assert_eq!(inbox_a["messages"].as_array().unwrap().len(), 1);
    assert_eq!(inbox_b["messages"].as_array().unwrap().len(), 2);
    first
        .tool("coop_ack", json!({"cursor": inbox_a["next_cursor"]}))
        .await
        .unwrap();
    let still_b = second
        .tool("coop_wait_message", json!({"timeout_s": 0}))
        .await
        .unwrap();
    assert_eq!(still_b["acked_cursor"], "0");
    assert_eq!(still_b["messages"].as_array().unwrap().len(), 2);
    let presence = first
        .tool("coop_list_agents", json!({"include_stale": true}))
        .await
        .unwrap();
    assert!(presence["agents"]
        .as_array()
        .unwrap()
        .iter()
        .any(|agent| agent["agent_id"] == a["agent_id"]));
    assert!(presence["agents"]
        .as_array()
        .unwrap()
        .iter()
        .any(|agent| agent["agent_id"] == b["agent_id"]));

    first
        .tool("coop_create_queue", json!({"queue": "jobs"}))
        .await
        .unwrap();
    first
        .tool(
            "coop_enqueue",
            json!({"queue": "jobs", "payload": {"work": 1}}),
        )
        .await
        .unwrap();
    let task = first
        .tool(
            "coop_claim_task",
            json!({"queue": "jobs", "lease_ttl_s": 60}),
        )
        .await
        .unwrap();
    let denied = second
        .tool(
            "coop_complete_task",
            json!({"lease_id": task["lease_id"], "fence": task["fence"], "outcome": {}}),
        )
        .await
        .unwrap_err();
    assert_eq!(denied["data"]["code"], "LEASE_LOST");
    first
        .tool("coop_publish_doc", json!({"name": "draft", "text": "v1"}))
        .await
        .unwrap();
    let doc = first
        .tool(
            "coop_claim_doc",
            json!({"name": "draft", "lease_ttl_s": 60}),
        )
        .await
        .unwrap();
    let denied = second.tool("coop_update_doc", json!({"name": "draft", "base_version": 1, "text": "foreign", "lease_id": doc["lease_id"], "fence": doc["fence"]})).await.unwrap_err();
    assert_eq!(denied["data"]["code"], "CONFLICT_CLAIMED");

    let rotated = first
        .tool(
            "coop_register_agent",
            json!({"name": a["name"], "kind": "codex"}),
        )
        .await
        .unwrap();
    assert_eq!(rotated["agent_id"], a["agent_id"]);
    let after_rotate = first
        .tool("coop_instance_metadata", json!({}))
        .await
        .unwrap();
    assert_eq!(after_rotate["agent_id"], a["agent_id"]);
    first.tool("coop_complete_task", json!({"lease_id": task["lease_id"], "fence": task["fence"], "outcome": {"done": true}})).await.unwrap();
    first.tool("coop_update_doc", json!({"name": "draft", "base_version": 1, "text": "v2", "lease_id": doc["lease_id"], "fence": doc["fence"]})).await.unwrap();
    let actual_inbox = rows(
        root.join("agents")
            .join(b["agent_id"].as_str().unwrap())
            .join("inbox.jsonl"),
    );
    assert_eq!(actual_inbox.len(), 2);
    assert_eq!(actual_inbox[0]["from"], a["name"]);
    assert_eq!(actual_inbox[1]["from"], b["name"]);
    assert_eq!(
        rows(root.join("docs/draft.jsonl")).last().unwrap()["actor_id"],
        a["agent_id"]
    );

    let retired = second.tool("coop_retire_agent", json!({})).await.unwrap();
    assert_eq!(retired["agent_id"], b["agent_id"]);
    assert_eq!(retired["retired"], true);
    assert!(second
        .tool("coop_instance_metadata", json!({}))
        .await
        .is_err());
    let current_registry: Value =
        serde_json::from_slice(&fs::read(root.join("registry.json")).unwrap()).unwrap();
    assert_eq!(
        current_registry["agents"].as_object().unwrap().len(),
        3,
        "retirement cannot silently create a replacement launch"
    );
    retain(
        &root,
        &json!({"first": a, "second": b, "shared_agent_id": shared["agent_id"], "first_pid": first.child.id(), "second_pid": second.child.id()}),
        "two-mcp",
    );
    first.stop().await;
    second.stop().await;
    drop(daemon);

    // Resume is explicit: reuse the actual rotated credential after restart,
    // while a fresh launch above ignored the same shared configured token.
    let _restarted = Daemon::start(&root).await;
    let mut resumed =
        McpProcess::start(&root, rotated["token"].as_str().unwrap(), false, true).await;
    assert_eq!(
        resumed
            .tool("coop_instance_metadata", json!({}))
            .await
            .unwrap()["agent_id"],
        a["agent_id"]
    );
    let mut legacy_resume = McpProcess::start(&root, shared_token, false, true).await;
    assert_eq!(
        legacy_resume
            .tool("coop_instance_metadata", json!({}))
            .await
            .unwrap()["agent_id"],
        shared["agent_id"]
    );
    resumed.stop().await;
    legacy_resume.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_mcp_competitor_cannot_mutate_the_active_instance_root() {
    let temp = common::TempDir::new("embedded-instance-owner");
    let root = temp.path().join("root");
    let mut owner = McpProcess::start(&root, "inherited-shared-token", true, false).await;
    let identity = owner
        .tool("coop_instance_metadata", json!({}))
        .await
        .unwrap();
    let before = snapshot(&root);
    let competitor = Command::new(env!("CARGO_BIN_EXE_coop"))
        .args([
            "mcp-stdio",
            "--embedded",
            "--kind",
            "codex",
            "--display-name",
            "codex",
        ])
        .env("COOPERATORS_HOME", &root)
        .env("COOP_TOKEN", "inherited-shared-token")
        .env_remove("COOP_SOCKET")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output();
    let result = tokio::time::timeout(Duration::from_secs(10), competitor)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !result.status.success(),
        "competing embedded writer was accepted"
    );
    assert_eq!(
        before,
        snapshot(&root),
        "rejected embedded launch changed durable files"
    );
    assert_eq!(
        owner
            .tool("coop_instance_metadata", json!({}))
            .await
            .unwrap()["agent_id"],
        identity["agent_id"]
    );
    retain(
        &root,
        &json!({"owner": identity, "competitor_exit": result.status.code(), "competitor_stderr": String::from_utf8_lossy(&result.stderr)}),
        "embedded-root",
    );
    owner.stop().await;
}
