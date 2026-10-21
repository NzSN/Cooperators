mod common;

use std::sync::Arc;
use std::time::Duration;

use cooperators::mcp::{self, Backend, CoopMcpHandler};
use cooperators::rpc;
use cooperators::server::{self, ConnectionSession, DaemonState};
use rmcp::model::CallToolResponse;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

#[tokio::test]
async fn lost_enrollment_reply_retries_same_key_and_keeps_one_durable_identity() {
    let temp = common::TempDir::new("enrollment-lost-reply");
    let socket = temp.path().join("fixture.sock");
    let listener = match UnixListener::bind(&socket) {
        Ok(listener) => listener,
        Err(error) if error.raw_os_error() == Some(1) => {
            if std::env::var_os("COOP_REQUIRE_UDS").is_some() {
                panic!("enrollment retry requires actual UDS, but bind was denied: {error}");
            }
            eprintln!("SKIP enrollment retry: sandbox denies actual UDS bind: {error}");
            return;
        }
        Err(error) => panic!("bind enrollment fixture: {error}"),
    };
    let state = Arc::new(DaemonState::new(temp.path().join("root")));
    let serving = Arc::clone(&state);
    let fixture = tokio::spawn(async move {
        let mut observed = Vec::new();
        for index in 0..3 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            assert!(stream.read_line(&mut line).await.unwrap() > 0);
            let request = rpc::parse_request(&line).unwrap();
            let params = request.params.clone();
            let method = request.method.clone();
            let response = server::dispatch(&serving, &mut ConnectionSession::default(), request)
                .await
                .unwrap();
            let actual = response.clone().into_result().unwrap();
            observed.push((method, params, actual));
            if index == 0 {
                // The real registry commit completed. Close the real socket
                // without writing its reply, creating a transport ambiguity.
                continue;
            }
            let mut encoded = serde_json::to_vec(&response).unwrap();
            encoded.push(b'\n');
            stream.get_mut().write_all(&encoded).await.unwrap();
        }
        observed
    });
    let handler = tokio::time::timeout(
        Duration::from_secs(3),
        CoopMcpHandler::fresh_stdio(Backend::uds(socket), "codex".into(), "codex".into(), vec![]),
    )
    .await
    .expect("fresh enrollment must complete without a lost-response stall")
    .expect("clean EOF must be retried as a transport failure");
    let metadata = match handler.dispatch_tool("coop_instance_metadata", None).await {
        CallToolResponse::Complete(result) => mcp::canonical_tool_result(&result),
        other => panic!("unexpected metadata response: {other:?}"),
    };
    assert_eq!(metadata["ok"], true);
    let observed = tokio::time::timeout(Duration::from_secs(3), fixture)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.len(), 3);
    assert_eq!(observed[0].0, "enroll_agent");
    assert_eq!(observed[1].0, "enroll_agent");
    assert_eq!(observed[2].0, "instance_metadata");
    assert_eq!(observed[0].1["launch_key"], observed[1].1["launch_key"]);
    assert_eq!(observed[0].2["agent_id"], observed[1].2["agent_id"]);
    assert_eq!(observed[1].2["resumed"], true);
    assert_eq!(metadata["result"]["agent_id"], observed[0].2["agent_id"]);
    let raw: Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("root/registry.json")).unwrap())
            .unwrap();
    assert_eq!(raw["agents"].as_object().unwrap().len(), 1);
    assert_eq!(state.list_agents(true).await.unwrap().len(), 1);
}
