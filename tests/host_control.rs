//! Real local IPC with deterministic bridge state and no provider/model calls.

mod common;

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cooperators::arrivals::{
    ArrivalKind, ArrivalNotice, ArrivalReference, ObligationCoordinate, SourceCoordinate,
};
use cooperators::error::{BrokerError, ErrorCode};
use cooperators::host::{
    Binding, BridgeAction, BridgeLimits, HostBridge, Readiness, SharedHostBridge, Wake,
};
use cooperators::host_control::{
    HostControlClient, HostControlServer, HOST_CONTROL_FRAME_TIMEOUT, HOST_CONTROL_MAX_FRAME_BYTES,
};
use cooperators::storage::GENESIS_HASH;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

const SECRET: &str = "private-test-launch-secret-must-not-be-printed";

struct Fixture {
    _directory: common::TempDir,
    bridge: SharedHostBridge,
    bus_id: String,
    socket: PathBuf,
    server: Option<HostControlServer>,
}

impl Fixture {
    async fn start(name: &str) -> Option<Self> {
        let directory = common::TempDir::new("hc");
        let broker = directory.path().join("broker");
        fs::create_dir_all(&broker).unwrap();
        let bus_id = format!("bus-{}", cooperators::id::new_ulid().unwrap());
        let binding = Binding {
            binding_id: "binding-1".into(),
            generation: 3,
            bus_id: bus_id.clone(),
            agent_id: "agent-1".into(),
            adapter_kind: "deterministic".into(),
            exact_host_session_id: "session-1".into(),
            workspace: directory.path().to_string_lossy().into_owned(),
            capability_status: Readiness::Configured,
        };
        let bridge = Arc::new(tokio::sync::Mutex::new(
            HostBridge::open(
                &directory.path().join("state"),
                &broker,
                binding,
                BridgeLimits {
                    coalesce_ms: 0,
                    ..BridgeLimits::default()
                },
            )
            .unwrap(),
        ));
        let socket = directory.path().join("ipc/c");
        let server = match HostControlServer::start(&socket, SECRET, bridge.clone()).await {
            Ok(server) => server,
            Err(BrokerError::Io(error))
                if error.kind() == std::io::ErrorKind::PermissionDenied
                    && error.raw_os_error() == Some(1) =>
            {
                if std::env::var_os("COOP_REQUIRE_UDS").is_some() {
                    panic!("{name}: AF_UNIX denied and COOP_REQUIRE_UDS is set: {error}");
                }
                eprintln!("SKIP {name}: AF_UNIX bind denied by the agent sandbox (EPERM): {error}");
                return None;
            }
            Err(error) => panic!("{name}: host-control startup failed: {error}"),
        };
        Some(Self {
            _directory: directory,
            bridge,
            bus_id,
            socket,
            server: Some(server),
        })
    }

    fn client(&self) -> HostControlClient {
        HostControlClient::new(&self.socket, SECRET, &self.bus_id, "agent-1")
    }

    async fn wake(&self, queue: bool) -> Wake {
        let mut bridge = self.bridge.lock().await;
        let now = cooperators::id::unix_millis();
        bridge
            .receive(
                &self.bus_id,
                "agent-1",
                vec![notice(queue)],
                format!("arrival:{}:agent-1:1", self.bus_id),
                now,
            )
            .unwrap();
        match bridge.next_action(now).unwrap().unwrap() {
            BridgeAction::Submit(wake) => wake,
            other => panic!("unexpected bridge action: {other:?}"),
        }
    }

    async fn shutdown(mut self) {
        self.server.take().unwrap().shutdown().await.unwrap();
        assert!(!self.socket.exists(), "shutdown removes its socket inode");
    }
}

fn notice(queue: bool) -> ArrivalNotice {
    ArrivalNotice {
        v: 1,
        seq: 1,
        prev_hash: GENESIS_HASH.into(),
        delivery_id: "delivery-1".into(),
        recipient_agent_id: "agent-1".into(),
        kind: if queue {
            ArrivalKind::QueueTaskAvailable
        } else {
            ArrivalKind::DocPublished
        },
        obligation: ObligationCoordinate {
            id: "obligation-1".into(),
            journal_type: if queue { "queue" } else { "document" }.into(),
            journal_name: "work".into(),
            seq: 1,
            hash: GENESIS_HASH.into(),
            item_index: 0,
        },
        source: SourceCoordinate {
            stream: if queue { "queue" } else { "document" }.into(),
            name: "work".into(),
            seq: 1,
            hash: GENESIS_HASH.into(),
        },
        resource: if queue {
            ArrivalReference::Queue {
                name: "work".into(),
                cause: "enqueue".into(),
                task_id: Some("task-1".into()),
                ready_count: Some(1),
            }
        } else {
            ArrivalReference::Document {
                name: "work".into(),
                version: 1,
            }
        },
        subscription_ids: vec!["subscription-1".into()],
    }
}

fn has_code(error: &BrokerError, expected: ErrorCode) -> bool {
    matches!(error, BrokerError::Application(code, _) | BrokerError::ApplicationData(code, _, _) if *code == expected)
}

fn resolution(wake: &Wake, disposition: &str) -> Value {
    json!({
        "wake_id": wake.wake_id,
        "generation": wake.generation,
        "decisions": [{"delivery_id": "delivery-1", "disposition": disposition}],
        "continuation": false,
    })
}

struct Relayed {
    method: String,
    attempt_key: Option<String>,
    result: Option<Value>,
}

/// Forward real IPC requests, durably receive the server response, then drop
/// selected replies before the frontend can observe them. No fault hooks are
/// required in the production transport or bridge.
fn losing_reply_relay(
    fixture: &Fixture,
    mut drop_methods: Vec<&'static str>,
    request_count: usize,
) -> (HostControlClient, tokio::task::JoinHandle<Vec<Relayed>>) {
    let socket = fixture.socket.with_file_name("relay");
    let listener = UnixListener::bind(&socket).unwrap();
    let backend_socket = fixture.socket.clone();
    let client = HostControlClient::new(socket, SECRET, &fixture.bus_id, "agent-1");
    let task = tokio::spawn(async move {
        let mut observations = Vec::new();
        for _ in 0..request_count {
            let (frontend, _) = listener.accept().await.unwrap();
            let mut frontend = BufReader::new(frontend);
            let mut request = String::new();
            frontend.read_line(&mut request).await.unwrap();
            let envelope: Value = serde_json::from_str(&request).unwrap();
            let method = envelope["method"].as_str().unwrap().to_owned();
            let mut backend = UnixStream::connect(&backend_socket).await.unwrap();
            backend.write_all(request.as_bytes()).await.unwrap();
            let mut backend = BufReader::new(backend);
            let mut response = String::new();
            backend.read_line(&mut response).await.unwrap();
            let parsed: Value = serde_json::from_str(&response).unwrap();
            observations.push(Relayed {
                method: method.clone(),
                attempt_key: envelope["params"]["attempt_key"]
                    .as_str()
                    .map(str::to_owned),
                result: parsed.get("result").cloned(),
            });
            if drop_methods
                .first()
                .is_some_and(|expected| *expected == method)
            {
                drop_methods.remove(0);
            } else {
                frontend
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .unwrap();
            }
        }
        observations
    });
    (client, task)
}

#[tokio::test]
async fn lost_begin_and_finish_replies_retry_the_same_durable_claim_once() {
    let Some(fixture) =
        Fixture::start("lost_begin_and_finish_replies_retry_the_same_durable_claim_once").await
    else {
        return;
    };
    let wake = fixture.wake(true).await;
    let (client, relay) = losing_reply_relay(&fixture, vec!["begin_claim", "finish_claim"], 5);
    let context = client
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap();
    assert_eq!(fixture.bridge.lock().await.status().in_flight_claims, 1);
    client
        .call(
            "finish_claim",
            json!({"context": context, "outcome": {"result": "empty"}}),
        )
        .await
        .unwrap();
    assert_eq!(fixture.bridge.lock().await.status().in_flight_claims, 0);
    client
        .call("resolve", resolution(&wake, "queue_drained"))
        .await
        .unwrap();
    let observed = relay.await.unwrap();
    assert_eq!(observed[0].method, "begin_claim");
    assert_eq!(observed[0].attempt_key, observed[1].attempt_key);
    assert_eq!(observed[0].result, observed[1].result);
    assert_eq!(observed[0].result.as_ref(), Some(&context));
    assert_eq!(observed[2].method, "finish_claim");
    assert_eq!(observed[2].result, observed[3].result);
    fixture.shutdown().await;
}

#[tokio::test]
async fn exhausted_begin_retries_surface_attention_and_retain_one_context() {
    let Some(fixture) =
        Fixture::start("exhausted_begin_retries_surface_attention_and_retain_one_context").await
    else {
        return;
    };
    fixture.wake(true).await;
    let (client, relay) = losing_reply_relay(&fixture, vec!["begin_claim"; 3], 5);
    let error = client
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap_err();
    assert!(has_code(&error, ErrorCode::DaemonUnreachable));
    assert!(has_code(
        &client
            .call("begin_claim", json!({"queue": "work"}))
            .await
            .unwrap_err(),
        ErrorCode::Conflict
    ));
    let status = client.call("status", json!({})).await.unwrap();
    assert_eq!(status["readiness"], "attention_required");
    assert_eq!(status["in_flight_claims"], 1);
    let observed = relay.await.unwrap();
    assert_eq!(observed[0].attempt_key, observed[1].attempt_key);
    assert_eq!(observed[1].attempt_key, observed[2].attempt_key);
    fixture.shutdown().await;
}

#[tokio::test]
async fn cancelled_unsubmitted_begin_reports_its_retained_context() {
    let Some(fixture) =
        Fixture::start("cancelled_unsubmitted_begin_reports_its_retained_context").await
    else {
        return;
    };
    fixture.wake(true).await;
    let socket = fixture.socket.with_file_name("relay");
    let listener = UnixListener::bind(&socket).unwrap();
    let backend_socket = fixture.socket.clone();
    let (ready, ready_receiver) = tokio::sync::oneshot::channel();
    let (release, release_receiver) = tokio::sync::oneshot::channel();
    let relay = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let mut first = BufReader::new(first);
        let mut request = String::new();
        first.read_line(&mut request).await.unwrap();
        let mut backend = UnixStream::connect(&backend_socket).await.unwrap();
        backend.write_all(request.as_bytes()).await.unwrap();
        let mut backend = BufReader::new(backend);
        let mut response = String::new();
        backend.read_line(&mut response).await.unwrap();
        ready.send(()).unwrap();
        // Hold the completed begin reply until the frontend has been cancelled.
        release_receiver.await.unwrap();
        drop(first);
        for _ in 0..2 {
            let (next, _) = listener.accept().await.unwrap();
            let mut next = BufReader::new(next);
            request.clear();
            next.read_line(&mut request).await.unwrap();
            let mut backend = UnixStream::connect(&backend_socket).await.unwrap();
            backend.write_all(request.as_bytes()).await.unwrap();
            let mut backend = BufReader::new(backend);
            response.clear();
            backend.read_line(&mut response).await.unwrap();
            next.get_mut().write_all(response.as_bytes()).await.unwrap();
        }
    });
    let client = HostControlClient::new(socket, SECRET, &fixture.bus_id, "agent-1");
    let issuing = client.clone();
    let begin =
        tokio::spawn(async move { issuing.call("begin_claim", json!({"queue": "work"})).await });
    ready_receiver.await.unwrap();
    begin.abort();
    assert!(begin.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    assert!(has_code(
        &client
            .call("begin_claim", json!({"queue": "work"}))
            .await
            .unwrap_err(),
        ErrorCode::Conflict
    ));
    let status = client.call("status", json!({})).await.unwrap();
    assert_eq!(status["in_flight_claims"], 1);
    assert_eq!(status["readiness"], "attention_required");
    relay.await.unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn restarted_frontend_preserves_unreconciled_claims_and_requests_attention() {
    let Some(fixture) =
        Fixture::start("restarted_frontend_preserves_unreconciled_claims_and_requests_attention")
            .await
    else {
        return;
    };
    let original = fixture.client();
    let wake = fixture.wake(true).await;
    let context = original
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap();
    let restarted = fixture.client();
    let status = restarted.call("status", json!({})).await.unwrap();
    assert_eq!(status["readiness"], "configured");
    assert_eq!(status["in_flight_claims"], 1);
    assert!(has_code(
        &restarted
            .call("begin_claim", json!({"queue": "work"}))
            .await
            .unwrap_err(),
        ErrorCode::Conflict
    ));
    assert_eq!(fixture.bridge.lock().await.status().in_flight_claims, 1);
    assert_eq!(
        restarted.call("status", json!({})).await.unwrap()["readiness"],
        "attention_required"
    );
    // The original authenticated proxy can still reconcile its exact result.
    original
        .call(
            "finish_claim",
            json!({"context": context, "outcome": {"result": "empty"}}),
        )
        .await
        .unwrap();
    restarted
        .call("resolve", resolution(&wake, "queue_drained"))
        .await
        .unwrap();
    assert_eq!(
        restarted.call("status", json!({})).await.unwrap()["readiness"],
        "verified"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn independent_status_monitors_preserve_inflight_frontend_and_readiness() {
    let Some(fixture) =
        Fixture::start("independent_status_monitors_preserve_inflight_frontend_and_readiness")
            .await
    else {
        return;
    };
    let original = fixture.client();
    let wake = fixture.wake(true).await;
    let first = original
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap();
    let (before, journal) = {
        let bridge = fixture.bridge.lock().await;
        let journal = bridge.directory().join("bridge.jsonl");
        (fs::read(&journal).unwrap(), journal)
    };
    for _ in 0..3 {
        let monitor = fixture.client();
        let status = monitor.call("status", json!({})).await.unwrap();
        assert_eq!(status["readiness"], "configured");
        assert_eq!(status["in_flight_claims"], 1);
        assert!(
            before == fs::read(&journal).unwrap(),
            "status must not journal mutations"
        );
    }
    // A monitor must not replace the original frontend owner: a second known
    // claim from the active frontend remains admissible while the first runs.
    let second = original
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap();
    for context in [first, second] {
        original
            .call(
                "finish_claim",
                json!({"context": context, "outcome": {"result": "empty"}}),
            )
            .await
            .unwrap();
    }
    original
        .call("resolve", resolution(&wake, "queue_drained"))
        .await
        .unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn restarted_control_server_cannot_adopt_an_unknown_inflight_frontend() {
    let Some(mut fixture) =
        Fixture::start("restarted_control_server_cannot_adopt_an_unknown_inflight_frontend").await
    else {
        return;
    };
    fixture.wake(true).await;
    let original = fixture.client();
    let context = original
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap();
    fixture.server.take().unwrap().shutdown().await.unwrap();
    fixture.server = Some(
        HostControlServer::start(&fixture.socket, SECRET, fixture.bridge.clone())
            .await
            .unwrap(),
    );
    let restarted = fixture.client();
    for _ in 0..2 {
        assert!(restarted
            .call("begin_claim", json!({"queue": "work"}))
            .await
            .is_err());
        let status = restarted.call("status", json!({})).await.unwrap();
        assert_eq!(status["readiness"], "attention_required");
        assert_eq!(status["in_flight_claims"], 1);
    }
    // A surviving original proxy can still supply exact authoritative evidence.
    original
        .call(
            "finish_claim",
            json!({"context": context, "outcome": {"result": "unproven"}}),
        )
        .await
        .unwrap();
    fixture.shutdown().await;
}

#[tokio::test]
async fn local_rpc_resolves_confirms_and_fences_generation() {
    let Some(fixture) = Fixture::start("local_rpc_resolves_confirms_and_fences_generation").await
    else {
        return;
    };
    let client = fixture.client();
    assert_eq!(
        fs::metadata(fixture.socket.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(&fixture.socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let probe = fixture
        .bridge
        .lock()
        .await
        .issue_probe(cooperators::id::unix_millis())
        .unwrap();
    let mut confirmation = json!({
        "binding_id": probe.binding_id,
        "generation": probe.generation - 1,
        "nonce": probe.nonce,
    });
    assert!(has_code(
        &client
            .call("confirm", confirmation.clone())
            .await
            .unwrap_err(),
        ErrorCode::Conflict
    ));
    confirmation["generation"] = json!(probe.generation);
    let confirmed = client.call("confirm", confirmation).await.unwrap();
    assert_eq!(confirmed["readiness"], "verified");
    let wake = fixture.wake(false).await;
    let mut request = resolution(&wake, "document_read");
    request["generation"] = json!(wake.generation - 1);
    assert!(has_code(
        &client.call("resolve", request.clone()).await.unwrap_err(),
        ErrorCode::Conflict
    ));
    request["generation"] = json!(wake.generation);
    let receipt = client.call("resolve", request.clone()).await.unwrap();
    assert_eq!(receipt["resolved_delivery_ids"], json!(["delivery-1"]));
    assert_eq!(client.call("resolve", request).await.unwrap(), receipt);
    assert_eq!(
        client.call("status", json!({})).await.unwrap()["pending_notices"],
        0
    );
    assert!(!format!("{client:?}").contains(SECRET));
    assert!(!format!("{:?}", fixture.server.as_ref().unwrap()).contains(SECRET));
    fixture.shutdown().await;
}

#[tokio::test]
async fn wrong_secret_bus_and_agent_are_rejected_before_dispatch() {
    let Some(fixture) =
        Fixture::start("wrong_secret_bus_and_agent_are_rejected_before_dispatch").await
    else {
        return;
    };
    for (secret, bus, agent) in [
        ("wrong-secret", fixture.bus_id.as_str(), "agent-1"),
        (SECRET, "other-bus", "agent-1"),
        (SECRET, fixture.bus_id.as_str(), "other-agent"),
    ] {
        let client = HostControlClient::new(&fixture.socket, secret, bus, agent);
        for method in [
            "status",
            "resolve",
            "confirm",
            "begin_claim",
            "finish_claim",
        ] {
            let error = client.call(method, json!({})).await.unwrap_err();
            assert!(has_code(&error, ErrorCode::Unauthenticated), "{error}");
            assert!(!error.to_string().contains(secret));
            assert!(!format!("{error:?}").contains(secret));
        }
    }
    assert_eq!(fixture.bridge.lock().await.status().pending_notices, 0);
    fixture.shutdown().await;
}

#[tokio::test]
async fn immutable_claim_contexts_prevent_forged_and_stale_drain_evidence() {
    let Some(fixture) =
        Fixture::start("immutable_claim_contexts_prevent_forged_and_stale_drain_evidence").await
    else {
        return;
    };
    let client = fixture.client();
    let wake = fixture.wake(true).await;
    let drain = resolution(&wake, "queue_drained");
    assert!(client.call("resolve", drain.clone()).await.is_err());
    assert!(client
        .call("begin_claim", json!({"queue": "other"}))
        .await
        .unwrap()
        .is_null());
    let old_empty = client
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap();
    for key in ["generation", "queue_generation", "adoption_epoch"] {
        let mut forged = old_empty.clone();
        forged[key] = json!(forged[key].as_u64().unwrap() + 1);
        assert!(client
            .call(
                "finish_claim",
                json!({
                    "context": forged, "outcome": {"result": "empty"}
                })
            )
            .await
            .is_err());
    }
    let adopted = client
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap();
    client
        .call(
            "finish_claim",
            json!({
                "context": adopted, "outcome": {"result": "adopted", "task_id": "task-1"}
            }),
        )
        .await
        .unwrap();
    client
        .call(
            "finish_claim",
            json!({
                "context": old_empty, "outcome": {"result": "empty"}
            }),
        )
        .await
        .unwrap();
    assert!(
        client.call("resolve", drain.clone()).await.is_err(),
        "an empty result begun before adoption cannot prove drain"
    );
    let unproven = client
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap();
    client
        .call(
            "finish_claim",
            json!({
                "context": unproven, "outcome": {"result": "unproven"}
            }),
        )
        .await
        .unwrap();
    assert!(client.call("resolve", drain.clone()).await.is_err());
    let fresh = client
        .call("begin_claim", json!({"queue": "work"}))
        .await
        .unwrap();
    client
        .call(
            "finish_claim",
            json!({
                "context": fresh, "outcome": {"result": "empty"}
            }),
        )
        .await
        .unwrap();
    client.call("resolve", drain).await.unwrap();
    assert!(
        client
            .call(
                "finish_claim",
                json!({
                    "context": fresh, "outcome": {"result": "empty"}
                })
            )
            .await
            .is_err(),
        "a retired wake cannot accept stale claim contexts"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn oversized_frames_are_bounded_and_partial_frames_time_out() {
    let Some(fixture) =
        Fixture::start("oversized_frames_are_bounded_and_partial_frames_time_out").await
    else {
        return;
    };
    let error = fixture
        .client()
        .call(
            "status",
            json!({
                "oversize": "x".repeat(HOST_CONTROL_MAX_FRAME_BYTES)
            }),
        )
        .await
        .unwrap_err();
    assert!(has_code(&error, ErrorCode::ResourceExhausted));

    let mut oversized = UnixStream::connect(&fixture.socket).await.unwrap();
    oversized
        .write_all(&vec![b'x'; HOST_CONTROL_MAX_FRAME_BYTES + 1])
        .await
        .unwrap();
    let mut reader = BufReader::new(oversized);
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response: Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["error"]["data"]["code"], "RESOURCE_EXHAUSTED");

    let mut partial = UnixStream::connect(&fixture.socket).await.unwrap();
    partial.write_all(b"{").await.unwrap();
    // A slow client does not block a separate authenticated request.
    fixture.client().call("status", json!({})).await.unwrap();
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(
            HOST_CONTROL_FRAME_TIMEOUT + Duration::from_secs(2),
            partial.read(&mut byte)
        )
        .await
        .unwrap()
        .unwrap(),
        0
    );
    let mut interrupted = UnixStream::connect(&fixture.socket).await.unwrap();
    interrupted.write_all(b"{").await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), fixture.shutdown())
        .await
        .unwrap();
    match interrupted.read(&mut byte).await {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("shutdown must close the incomplete connection: {other:?}"),
    }
}

#[tokio::test]
async fn stale_socket_is_recovered_and_active_or_non_socket_paths_are_preserved() {
    let Some(mut fixture) =
        Fixture::start("stale_socket_is_recovered_and_active_or_non_socket_paths_are_preserved")
            .await
    else {
        return;
    };
    let inode = fs::metadata(&fixture.socket).unwrap().ino();
    assert!(
        HostControlServer::start(&fixture.socket, SECRET, fixture.bridge.clone())
            .await
            .is_err()
    );
    assert_eq!(fs::metadata(&fixture.socket).unwrap().ino(), inode);
    fixture.client().call("status", json!({})).await.unwrap();
    fixture.server.take().unwrap().shutdown().await.unwrap();

    let stale = UnixListener::bind(&fixture.socket).unwrap();
    drop(stale);
    let server = HostControlServer::start(&fixture.socket, SECRET, fixture.bridge.clone())
        .await
        .unwrap();
    fixture.client().call("status", json!({})).await.unwrap();
    server.shutdown().await.unwrap();
    fs::write(&fixture.socket, b"preserve this regular file").unwrap();
    assert!(
        HostControlServer::start(&fixture.socket, SECRET, fixture.bridge.clone())
            .await
            .is_err()
    );
    assert_eq!(
        fs::read(&fixture.socket).unwrap(),
        b"preserve this regular file"
    );
    fs::remove_file(&fixture.socket).unwrap();

    let target = fixture.socket.with_file_name("target");
    fs::write(&target, b"preserve the symlink target").unwrap();
    std::os::unix::fs::symlink(&target, &fixture.socket).unwrap();
    assert!(
        HostControlServer::start(&fixture.socket, SECRET, fixture.bridge.clone())
            .await
            .is_err()
    );
    assert!(fs::symlink_metadata(&fixture.socket)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(fs::read(&target).unwrap(), b"preserve the symlink target");
    fs::remove_file(&fixture.socket).unwrap();

    let server = HostControlServer::start(&fixture.socket, SECRET, fixture.bridge.clone())
        .await
        .unwrap();
    // Cleanup must not unlink a replacement pathname belonging to another server.
    fs::remove_file(&fixture.socket).unwrap();
    let replacement = UnixListener::bind(&fixture.socket).unwrap();
    let replacement_inode = fs::metadata(&fixture.socket).unwrap().ino();
    server.shutdown().await.unwrap();
    assert_eq!(
        fs::metadata(&fixture.socket).unwrap().ino(),
        replacement_inode
    );
    drop(replacement);
    fs::remove_file(&fixture.socket).unwrap();
}

#[tokio::test]
async fn public_parent_and_empty_launch_secret_are_rejected() {
    let Some(fixture) = Fixture::start("public_parent_and_empty_launch_secret_are_rejected").await
    else {
        return;
    };
    let public_parent = fixture.socket.parent().unwrap().join("public");
    fs::create_dir(&public_parent).unwrap();
    fs::set_permissions(&public_parent, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        HostControlServer::start(public_parent.join("c"), SECRET, fixture.bridge.clone(),)
            .await
            .is_err()
    );
    assert_eq!(
        fs::metadata(&public_parent).unwrap().permissions().mode() & 0o777,
        0o755,
        "startup must not chmod an unrelated shared directory"
    );
    let empty_socket = fixture.socket.with_file_name("empty");
    assert!(
        HostControlServer::start(&empty_socket, "", fixture.bridge.clone())
            .await
            .is_err()
    );
    assert!(!empty_socket.exists());
    fixture.shutdown().await;
}

#[tokio::test]
async fn independent_process_frontend_reaches_the_existing_bridge_owner() {
    let Some(fixture) =
        Fixture::start("independent_process_frontend_reaches_the_existing_bridge_owner").await
    else {
        return;
    };
    let child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("host_control_child")
        .env("COOP_HOST_CONTROL_TEST_CHILD", "1")
        .env("COOP_HOST_CONTROL_TEST_SOCKET", &fixture.socket)
        .env("COOP_HOST_CONTROL_TEST_SECRET", SECRET)
        .env("COOP_HOST_CONTROL_TEST_BUS", &fixture.bus_id)
        .output()
        .await
        .unwrap();
    assert!(
        child.status.success(),
        "child test failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    fixture.shutdown().await;
}

#[test]
fn host_control_child() {
    if std::env::var_os("COOP_HOST_CONTROL_TEST_CHILD").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let client = HostControlClient::new(
            PathBuf::from(std::env::var_os("COOP_HOST_CONTROL_TEST_SOCKET").unwrap()),
            std::env::var("COOP_HOST_CONTROL_TEST_SECRET").unwrap(),
            std::env::var("COOP_HOST_CONTROL_TEST_BUS").unwrap(),
            "agent-1",
        );
        let status = client.call("status", json!({})).await.unwrap();
        assert_eq!(status["binding"]["binding_id"], "binding-1");
        assert_eq!(status["binding"]["generation"], 3);
    });
}
