//! M4 performance smoke test (design §11).
//!
//! Ignored by default. Run in release mode:
//!
//! ```text
//! cargo test --offline --release -- --ignored --nocapture
//! ```
//!
//! Every measurement is in-process; this test never binds a socket.

mod common;

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cooperators::envelope::{Body, Envelope};
use cooperators::rpc::RpcRequest;
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use cooperators::storage::{hash_record, LogFile, GENESIS_HASH};
use serde_json::{json, Value};

const SEND_SAMPLES: usize = 100;
const APPEND_WINDOW: Duration = Duration::from_secs(2);
const REPLAY_RECORDS: usize = 100_000;

#[tokio::test]
#[ignore = "performance smoke; run with --ignored --nocapture in release mode"]
async fn perf_smoke_against_design_targets() {
    let temp = common::TempDir::new("m4-perf");
    let root = temp.path().join("root");
    let mut config = DaemonConfig::new(&root);
    config.max_inbox_messages = 1_000_000;
    config.max_inbox_bytes = u64::MAX;
    let state = Arc::new(DaemonState::with_config(config).expect("state"));

    let sender = register(&state, "perf-sender").await;
    let recipient = register(&state, "perf-recipient").await;

    // Warm up the code paths and the file cache.
    for index in 0..10 {
        send(&state, &sender, "perf-recipient", index).await;
    }
    ack_all(&state, &recipient).await;

    let mut send_latencies = Vec::with_capacity(SEND_SAMPLES);
    let mut wait_latencies = Vec::with_capacity(SEND_SAMPLES);
    for index in 0..SEND_SAMPLES {
        let started = Instant::now();
        send(&state, &sender, "perf-recipient", 1_000 + index as u64).await;
        let sent = Instant::now();
        let wait_started = Instant::now();
        let _ = wait_once(&state, &recipient).await;
        let delivered = Instant::now();
        send_latencies.push(sent.duration_since(started));
        wait_latencies.push(delivered.duration_since(wait_started));
        ack_all(&state, &recipient).await;
    }

    let append_started = Instant::now();
    let mut appended = 0u64;
    let mut index = 10_000u64;
    while append_started.elapsed() < APPEND_WINDOW {
        send(&state, &sender, "perf-recipient", index).await;
        appended += 1;
        index += 1;
    }
    let append_elapsed = append_started.elapsed();
    let rate = appended as f64 / append_elapsed.as_secs_f64();

    let replay_elapsed = measure_replay(temp.path()).expect("replay fixture");

    let send_p50 = p50(&mut send_latencies);
    let wait_p50 = p50(&mut wait_latencies);
    let replay_1m = replay_elapsed.as_secs_f64() * (1_000_000.0 / REPLAY_RECORDS as f64);

    println!("cooperators M4 perf smoke");
    println!("date: {}", cooperators::id::now_rfc3339());
    println!(
        "profile: {}",
        if cfg!(debug_assertions) {
            "debug (targets assume release)"
        } else {
            "release"
        }
    );
    println!(
        "host: {} / {} logical CPUs",
        std::env::consts::OS,
        available_parallelism()
    );
    println!("cpu: {}", cpu_model());
    println!("root: {}", root.display());
    println!();
    println!("| Metric | Target | Measured | Result |");
    println!("|---|---|---|---|");
    println!(
        "| send ack p50 (fsynced) | < 10 ms | {:.3} ms | {} |",
        milliseconds(send_p50),
        verdict(milliseconds(send_p50) < 10.0)
    );
    println!(
        "| wait delivery after send p50 | < 25 ms | {:.3} ms | {} |",
        milliseconds(wait_p50),
        verdict(milliseconds(wait_p50) < 25.0)
    );
    println!(
        "| sustained append | >= 5 000 msg/s | {:.0} msg/s ({} msgs in {:.2} s) | {} |",
        rate,
        appended,
        append_elapsed.as_secs_f64(),
        verdict(rate >= 5_000.0)
    );
    println!(
        "| restart replay | < 10 s / 1 M records | {:.3} s / 100 k, {:.3} s extrapolated / 1 M | {} |",
        replay_elapsed.as_secs_f64(),
        replay_1m,
        verdict(replay_1m < 10.0)
    );
    println!();
    println!(
        "sample sizes: send/wait {} each; append window {:.1} s; replay {} records",
        SEND_SAMPLES,
        APPEND_WINDOW.as_secs_f64(),
        REPLAY_RECORDS
    );

    // Sanity checks only: this is a measurement, not a hard gate on the
    // machine's fsync throughput. Pass/miss is printed above and recorded in
    // docs/impl/perf-smoke.md.
    assert!(send_p50 > Duration::ZERO);
    assert!(wait_p50 > Duration::ZERO);
    assert!(rate > 0.0);
    assert!(appended > 0);
}

fn verdict(pass: bool) -> &'static str {
    if pass {
        "PASS"
    } else {
        "MISS"
    }
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn p50(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn available_parallelism() -> usize {
    std::thread::available_parallelism().map_or(0, |value| value.get())
}

fn cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split_once(':'))
                .map(|(_, value)| value.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

async fn register(state: &Arc<DaemonState>, name: &str) -> String {
    let response = server::dispatch(
        state,
        &mut ConnectionSession::default(),
        request("register_agent", json!({"name": name, "kind": "perf"})),
    )
    .await
    .expect("register response");
    response.into_result().expect("register result")["token"]
        .as_str()
        .expect("token")
        .to_string()
}

async fn send(state: &Arc<DaemonState>, token: &str, to: &str, index: u64) {
    server::dispatch(
        state,
        &mut ConnectionSession::default(),
        request(
            "send_message",
            json!({
                "token": token,
                "to": to,
                "kind": "perf",
                "body": {"mime": "text/plain", "text": format!("perf message {index}")},
            }),
        ),
    )
    .await
    .expect("send response")
    .into_result()
    .expect("send result");
}

async fn wait_once(state: &Arc<DaemonState>, token: &str) -> Value {
    server::dispatch(
        state,
        &mut ConnectionSession::default(),
        request("wait_message", json!({"token": token, "timeout_s": 5})),
    )
    .await
    .expect("wait response")
    .into_result()
    .expect("wait result")
}

async fn ack_all(state: &Arc<DaemonState>, token: &str) {
    let batch = wait_once(state, token).await;
    if let Some(cursor) = batch["next_cursor"].as_str() {
        server::dispatch(
            state,
            &mut ConnectionSession::default(),
            request("ack", json!({"token": token, "cursor": cursor})),
        )
        .await
        .expect("ack response")
        .into_result()
        .expect("ack result");
    }
}

fn request(method: &str, params: Value) -> RpcRequest {
    RpcRequest {
        id: Some(json!(1)),
        method: method.to_string(),
        params,
        is_notification: false,
    }
}

fn measure_replay(base: &std::path::Path) -> std::io::Result<Duration> {
    let path = base.join("replay/agents/replay/inbox.jsonl");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);
    let base_ms = cooperators::id::unix_millis();
    let mut previous = GENESIS_HASH.to_string();
    for index in 0..REPLAY_RECORDS {
        let mut random = [0u8; 10];
        random[2..].copy_from_slice(&(index as u64).to_be_bytes());
        let mut envelope = Envelope::new(
            "perf-sender",
            "perf-recipient",
            "perf",
            Body::new("text/plain", format!("replay record {index}")),
        )
        .map_err(std::io::Error::other)?;
        envelope.id = cooperators::id::ulid_from_parts(base_ms + index as u64, random);
        envelope.seq = index as u64 + 1;
        envelope.prev_hash = previous;
        previous =
            hash_record(&envelope).map_err(|error| std::io::Error::other(error.to_string()))?;
        serde_json::to_writer(&mut writer, &envelope)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    drop(writer);

    let started = Instant::now();
    let (_log, report) = LogFile::<Envelope>::open(&path)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let elapsed = started.elapsed();
    assert_eq!(report.records.len(), REPLAY_RECORDS);
    Ok(elapsed)
}
