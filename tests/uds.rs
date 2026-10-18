mod common;

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use cooperators::envelope::{Body, Envelope};
use cooperators::server::{self, DaemonState};
use cooperators::storage::LogFile;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// The default agent sandbox denies AF_UNIX `bind`/I/O with EPERM. Report a
/// skip (with a loud message) instead of a misleading failure; run this suite
/// outside the sandbox for the real gate.
fn sandbox_denies_unix_sockets(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::PermissionDenied && error.raw_os_error() == Some(1)
}

/// Skips on a sandbox EPERM, but fails instead when `COOP_REQUIRE_UDS=1` so a
/// gate runner can insist that the real socket path was exercised.
fn skip_or_fail(name: &str, error: &std::io::Error) {
    if std::env::var_os("COOP_REQUIRE_UDS").is_some() {
        panic!("{name}: AF_UNIX bind denied and COOP_REQUIRE_UDS is set: {error}");
    }
    eprintln!("SKIP {name}: AF_UNIX bind denied by the agent sandbox (EPERM): {error}");
}

async fn rpc_raw(reader: &mut BufReader<UnixStream>, line: &str) -> Value {
    let stream = reader.get_mut();
    stream
        .write_all(line.as_bytes())
        .await
        .expect("write request");
    stream.write_all(b"\n").await.expect("write newline");
    stream.flush().await.expect("flush request");

    let mut response = String::new();
    reader
        .read_line(&mut response)
        .await
        .expect("read response line");
    serde_json::from_str(response.trim_end_matches(['\r', '\n'])).expect("response is JSON")
}

async fn rpc(reader: &mut BufReader<UnixStream>, request: Value) -> Value {
    let line = serde_json::to_string(&request).expect("serialize request");
    rpc_raw(reader, &line).await
}

#[tokio::test]
async fn jsonrpc_echo_ping_and_status_over_a_real_uds_socket() {
    let temp = common::TempDir::new("uds");
    let root = temp.path().join("root");

    // Seed an inbox log so daemon.status has a real sequence head to report.
    let log_path = root.join("agents/a/inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&log_path).expect("seed log");
    for index in 0..2 {
        let mut record = Envelope::new(
            "codex:a",
            "claude:b",
            "event",
            Body::new("text/plain", format!("seed {index}")),
        )
        .expect("envelope");
        log.append(&mut record).expect("append seed");
    }
    drop(log);

    let (listener, socket) = match server::bind(&root) {
        Ok(bound) => bound,
        Err(error) if sandbox_denies_unix_sockets(&error) => {
            skip_or_fail(
                "jsonrpc_echo_ping_and_status_over_a_real_uds_socket",
                &error,
            );
            return;
        }
        Err(error) => panic!("bind daemon socket: {error}"),
    };
    let mode = std::fs::metadata(&socket)
        .expect("socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "daemon socket must be mode 0600");

    let state = Arc::new(DaemonState::new(root.clone()));
    let handle = tokio::spawn(server::serve(listener, state));

    let stream = UnixStream::connect(&socket).await.expect("connect");
    let mut reader = BufReader::new(stream);

    let echo = rpc(
        &mut reader,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "echo",
            "params": {"hello": "world", "n": 7},
        }),
    )
    .await;
    assert_eq!(echo["jsonrpc"], "2.0");
    assert_eq!(echo["id"], 1);
    assert_eq!(echo["result"], json!({"hello": "world", "n": 7}));

    let ping = rpc(
        &mut reader,
        json!({"jsonrpc": "2.0", "id": "p", "method": "ping"}),
    )
    .await;
    assert_eq!(ping["result"]["pong"], true);
    assert_eq!(ping["result"]["version"], cooperators::VERSION);

    let status = rpc(
        &mut reader,
        json!({"jsonrpc": "2.0", "id": 2, "method": "daemon.status"}),
    )
    .await;
    assert_eq!(status["result"]["pid"], std::process::id());
    assert_eq!(status["result"]["version"], cooperators::VERSION);
    assert_eq!(status["result"]["root"], root.display().to_string());
    assert_eq!(
        status["result"]["seq_heads"]["agents/a/inbox.jsonl"]["seq"],
        2
    );

    // A notification must be executed without producing a response line.
    let stream = reader.get_mut();
    stream
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"ping\"}\n")
        .await
        .expect("write notification");
    stream.flush().await.expect("flush notification");
    let after_notification = rpc(
        &mut reader,
        json!({"jsonrpc": "2.0", "id": 9, "method": "echo", "params": "next"}),
    )
    .await;
    assert_eq!(after_notification["id"], 9);
    assert_eq!(after_notification["result"], "next");

    let missing = rpc(
        &mut reader,
        json!({"jsonrpc": "2.0", "id": 3, "method": "does.not.exist"}),
    )
    .await;
    assert_eq!(missing["error"]["code"], -32601);

    let invalid_request = rpc(&mut reader, json!({"jsonrpc": "2.0", "id": 4})).await;
    assert_eq!(invalid_request["error"]["code"], -32600);

    let parse_error = rpc_raw(&mut reader, "this is not json").await;
    assert_eq!(parse_error["error"]["code"], -32700);
    assert!(parse_error["id"].is_null());

    handle.abort();
}

#[tokio::test]
async fn second_bind_fails_while_the_first_daemon_is_listening() {
    let temp = common::TempDir::new("bind-conflict");
    let root = temp.path().join("root");
    let (listener, _socket) = match server::bind(&root) {
        Ok(bound) => bound,
        Err(error) if sandbox_denies_unix_sockets(&error) => {
            skip_or_fail(
                "second_bind_fails_while_the_first_daemon_is_listening",
                &error,
            );
            return;
        }
        Err(error) => panic!("first bind: {error}"),
    };
    let state = Arc::new(DaemonState::new(root.clone()));
    let handle = tokio::spawn(server::serve(listener, state));

    let error = server::bind(&root).expect_err("second bind must fail");
    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);

    handle.abort();
}
