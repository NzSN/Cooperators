//! Native asynchronous UDS control for scripts/benchmark_throughput.py.
//!
//! The workload and gates stay fixed; only the client implementation changes.
//! Build/run instructions and declared limitations are in the native method.md.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use cooperators::envelope::Envelope;
use cooperators::storage::{hash_record, LogFile, GENESIS_HASH};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

type Result<T> = std::result::Result<T, String>;
const PAYLOAD: &str = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
const PROFILES: [&str; 2] = ["same-inbox", "distributed-inboxes"];
const CONCURRENCIES: [usize; 4] = [1, 8, 32, 100];
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, value_parser = ["baseline", "candidate"])]
    label: String,
    /// Required for baseline; accepts original source_files or files manifests.
    #[arg(long)]
    source_manifest: Option<PathBuf>,
    /// Diagnostic subset only; full qualification defaults to both profiles.
    #[arg(long, num_args = 1.., value_parser = PROFILES, default_values = PROFILES)]
    profiles: Vec<String>,
    /// Diagnostic subset only; full qualification defaults to 1, 8, 32, 100.
    #[arg(long, num_args = 1.., default_values = ["1", "8", "32", "100"])]
    concurrencies: Vec<usize>,
}

struct RpcClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    token: Option<String>,
    sequence: u64,
    line: Vec<u8>,
}

impl RpcClient {
    async fn connect(socket: &Path, token: Option<String>) -> Result<Self> {
        let stream = tokio::time::timeout(RPC_TIMEOUT, UnixStream::connect(socket))
            .await
            .map_err(|_| "UDS connect timeout".to_string())?
            .map_err(|error| format!("UDS connect: {error}"))?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
            token,
            sequence: 0,
            line: Vec::with_capacity(65536),
        })
    }

    async fn call(&mut self, method: &str, mut params: Value) -> Result<Value> {
        self.sequence += 1;
        if let Some(token) = &self.token {
            params["token"] = json!(token);
        }
        let mut request = serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "id": self.sequence, "method": method, "params": params,
        }))
        .map_err(|error| error.to_string())?;
        request.push(b'\n');
        let sequence = self.sequence;
        let exchange = async {
            self.writer
                .write_all(&request)
                .await
                .map_err(|error| format!("UDS {method} write: {error}"))?;
            self.line.clear();
            let size = self
                .reader
                .read_until(b'\n', &mut self.line)
                .await
                .map_err(|error| format!("UDS {method} read: {error}"))?;
            if size == 0 || self.line.last() != Some(&b'\n') {
                return Err(format!("UDS {method}: closed or unterminated response"));
            }
            let response: Value = serde_json::from_slice(&self.line)
                .map_err(|error| format!("UDS {method} JSON: {error}"))?;
            if response["jsonrpc"] != "2.0"
                || response["id"] != sequence
                || response.get("error").is_some()
                || response.get("result").is_none()
            {
                return Err(format!("UDS {method}: invalid/error response {response}"));
            }
            Ok(response["result"].clone())
        };
        tokio::time::timeout(RPC_TIMEOUT, exchange)
            .await
            .map_err(|_| {
                format!("UDS {method}: 30-second timeout; commit outcome may be unknown")
            })?
    }
}

#[derive(Clone, Serialize)]
struct Agent {
    agent_id: String,
    name: String,
    display_name: String,
}

struct Sender {
    client: RpcClient,
    agent: Agent,
    receiver: Agent,
}

#[derive(Clone, Serialize)]
struct Sample {
    message_id: String,
    seq: u64,
    sender_id: String,
    sender_name: String,
    receiver_id: String,
    receiver_name: String,
    iteration: usize,
    warmup: bool,
    started_monotonic_ns: u64,
    completed_monotonic_ns: u64,
    send_ack_ms: f64,
    receipt: Value,
    receipt_verified: bool,
}

#[derive(Serialize)]
struct Delivery {
    message_id: String,
    seq: u64,
    sender_name: String,
    receiver_name: String,
    receiver_id: String,
    envelope: Value,
    delivery_verified: bool,
    ack_verified: bool,
}

struct SenderOutcome {
    sender: Sender,
    rows: Vec<Sample>,
    errors: Vec<String>,
    attempts: Vec<Value>,
}

#[derive(Default)]
struct ReceiverEvidence {
    deliveries: Vec<Delivery>,
    rpc_events: Vec<Value>,
    errors: Vec<String>,
    acked_head: u64,
    wait_calls: usize,
    ack_calls: usize,
    max_batch: usize,
}

struct ReceiverOutcome {
    client: RpcClient,
    agent: Agent,
    evidence: ReceiverEvidence,
}

struct WindowOutcome {
    senders: Vec<Sender>,
    samples: Vec<Sample>,
    attempts: Vec<Value>,
    errors: Vec<String>,
    elapsed_seconds: f64,
    resources: Value,
}

struct WorkloadEvidence {
    agents: Vec<Agent>,
    samples: Vec<Sample>,
    attempts: Vec<Value>,
    receivers: Vec<ReceiverOutcome>,
    errors: Vec<String>,
    measured_elapsed_seconds: Option<f64>,
    resources: Vec<Value>,
    warmup_fully_acked: bool,
    all_receivers_empty: bool,
}

impl WorkloadEvidence {
    fn new() -> Self {
        Self {
            agents: Vec::new(),
            samples: Vec::new(),
            attempts: Vec::new(),
            receivers: Vec::new(),
            errors: Vec::new(),
            measured_elapsed_seconds: None,
            resources: Vec::new(),
            warmup_fully_acked: false,
            all_receivers_empty: false,
        }
    }
}

/// This guard reaps only the child launched for this one profile, also on error.
struct DaemonProcess {
    child: Child,
    stopped: bool,
}

impl DaemonProcess {
    fn stop(&mut self) -> Result<String> {
        if self.stopped {
            return Ok("already stopped".into());
        }
        if self.child.try_wait().map_err(|e| e.to_string())?.is_none() {
            let pid = self.child.id().to_string();
            let signaled = Command::new("kill")
                .args(["-TERM", &pid])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false);
            if signaled {
                let deadline = Instant::now() + Duration::from_secs(5);
                while Instant::now() < deadline {
                    if let Some(status) = self.child.try_wait().map_err(|e| e.to_string())? {
                        self.stopped = true;
                        return Ok(format!("SIGTERM; {status}"));
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            self.child.kill().map_err(|e| e.to_string())?;
        }
        let status = self.child.wait().map_err(|e| e.to_string())?;
        self.stopped = true;
        Ok(format!("reaped; {status}"))
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Result<Self> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos();
        let path = PathBuf::from(format!("/tmp/ctn-{}-{stamp}", std::process::id()));
        fs::create_dir(&path).map_err(|e| e.to_string())?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).map_err(|e| e.to_string())?;
        Ok(Self(path))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn ns(origin: Instant) -> u64 {
    origin.elapsed().as_nanos() as u64
}

fn require(condition: bool, message: impl Into<String>) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn string_field(value: &Value, field: &str) -> Result<String> {
    value[field]
        .as_str()
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("missing/invalid {field}: {value}"))
}

fn sha(path: &Path) -> Result<String> {
    let mut input = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut digest = Sha256::new();
    let mut bytes = vec![0; 1024 * 1024];
    loop {
        let count = input.read(&mut bytes).map_err(|e| e.to_string())?;
        if count == 0 {
            break;
        }
        digest.update(&bytes[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn command(args: &[&str]) -> Value {
    match Command::new(args[0]).args(&args[1..]).output() {
        Ok(output) if output.status.success() => {
            json!(String::from_utf8_lossy(&output.stdout).trim())
        }
        Ok(output) => {
            json!({"error": String::from_utf8_lossy(&output.stderr).trim(), "status": output.status.code()})
        }
        Err(error) => json!({"error": error.to_string()}),
    }
}

fn regular_files(path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !path.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(path).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let metadata = entry.file_type().map_err(|e| e.to_string())?;
        if metadata.is_dir() {
            files.extend(regular_files(&entry.path())?);
        } else if metadata.is_file() {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

fn sources(repository: &Path) -> Result<BTreeMap<String, String>> {
    let mut paths = regular_files(&repository.join("src"))?;
    paths.retain(|path| path.extension().is_some_and(|extension| extension == "rs"));
    paths.extend([repository.join("Cargo.toml"), repository.join("Cargo.lock")]);
    paths
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(repository)
                .map_err(|e| e.to_string())?
                .to_string_lossy()
                .into_owned();
            Ok((relative, sha(&path)?))
        })
        .collect()
}

fn redact(mut text: String, credentials: &[String]) -> String {
    for credential in credentials {
        text = text.replace(credential, "[REDACTED]");
    }
    text
}

fn write_json(path: &Path, value: &impl Serialize, credentials: &[String]) -> Result<()> {
    let mut text = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
    text.push('\n');
    fs::write(path, redact(text, credentials)).map_err(|e| e.to_string())
}

fn write_jsonl(path: &Path, rows: &[impl Serialize], credentials: &[String]) -> Result<()> {
    let file = File::create(path).map_err(|e| e.to_string())?;
    let mut output = std::io::BufWriter::new(file);
    for row in rows {
        let text = redact(
            serde_json::to_string(row).map_err(|e| e.to_string())?,
            credentials,
        );
        writeln!(output, "{text}").map_err(|e| e.to_string())?;
    }
    output.flush().map_err(|e| e.to_string())
}

fn resource(pid: u32) -> Value {
    let base = format!("/proc/{pid}");
    let stat = fs::read_to_string(format!("{base}/stat"));
    let mut result = json!({"pid": pid});
    match stat {
        Ok(text) => {
            if let Some((_, fields)) = text.rsplit_once(") ") {
                let fields: Vec<_> = fields.split_whitespace().collect();
                for (key, index) in [
                    ("user_ticks", 11),
                    ("system_ticks", 12),
                    ("minor_faults", 7),
                    ("major_faults", 9),
                    ("threads", 17),
                    ("resident_pages", 21),
                ] {
                    result[key] = fields
                        .get(index)
                        .and_then(|n| n.parse::<u64>().ok())
                        .map_or(Value::Null, |n| json!(n));
                }
            } else {
                result["stat_error"] = json!("unrecognized proc stat");
            }
        }
        Err(error) => result["stat_error"] = json!(error.to_string()),
    }
    match fs::read_to_string(format!("{base}/io")) {
        Ok(text) => {
            let counters: BTreeMap<_, _> = text
                .lines()
                .filter_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    Some((key.to_owned(), value.trim().parse::<u64>().ok()?))
                })
                .collect();
            result["io"] = json!(counters);
        }
        Err(error) => result["io_error"] = json!(error.to_string()),
    }
    result
}

fn resource_delta(before: &Value, after: &Value, seconds: f64, ticks: f64) -> Value {
    let mut delta = json!({"wall_seconds": seconds, "clock_ticks_per_second": ticks,
        "one_logical_cpu_percent": 100, "before": before, "after": after});
    for key in ["user_ticks", "system_ticks", "minor_faults", "major_faults"] {
        if let (Some(start), Some(end)) = (before[key].as_u64(), after[key].as_u64()) {
            delta[key] = json!(end.saturating_sub(start));
        }
    }
    if let (Some(user), Some(system)) =
        (delta["user_ticks"].as_u64(), delta["system_ticks"].as_u64())
    {
        delta["total_cpu_percent"] = json!((user + system) as f64 / ticks / seconds * 100.0);
    }
    delta
}

async fn enroll(
    socket: &Path,
    label: &str,
    index: usize,
    credentials: &mut Vec<String>,
) -> Result<(RpcClient, Agent)> {
    let mut random = [0u8; 16];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut random))
        .map_err(|e| e.to_string())?;
    let launch: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    credentials.push(launch.clone());
    let mut client = RpcClient::connect(socket, None).await?;
    let response = client
        .call(
            "enroll_agent",
            json!({"launch_key": launch,
        "kind": "benchmark", "display_name": format!("{label}-{index}"), "capabilities": []}),
        )
        .await?;
    let token = string_field(&response, "token")?;
    credentials.push(token.clone());
    let agent = Agent {
        agent_id: string_field(&response, "agent_id")?,
        name: string_field(&response, "name")?,
        display_name: string_field(&response, "display_name")?,
    };
    require(
        cooperators::id::is_ulid(&agent.agent_id),
        "invalid enrolled agent ID",
    )?;
    client.token = Some(token);
    Ok((client, agent))
}

async fn sender_window(
    mut sender: Sender,
    warmup: bool,
    ready: Arc<Barrier>,
    release: Arc<Barrier>,
    clock: Arc<OnceLock<(Instant, Instant)>>,
    origin: Instant,
    failed: Arc<AtomicBool>,
) -> SenderOutcome {
    let mut rows = Vec::new();
    let mut errors = Vec::new();
    let mut attempts = Vec::new();
    ready.wait().await;
    release.wait().await;
    let Some((_, deadline)) = clock.get() else {
        return SenderOutcome {
            sender,
            rows,
            attempts,
            errors: vec!["window clock absent".into()],
        };
    };
    while Instant::now() < *deadline || (warmup && rows.len() < 3) {
        if failed.load(Ordering::Acquire) {
            break;
        }
        let started = ns(origin);
        let receipt = sender
            .client
            .call(
                "send_message",
                json!({"to": sender.receiver.agent_id,
            "kind": "benchmark", "body": {"mime": "text/plain", "text": PAYLOAD}}),
            )
            .await;
        let ended = ns(origin);
        match receipt {
            Ok(receipt) => {
                let id = receipt["message_id"].as_str().unwrap_or("").to_string();
                let seq = receipt["seq"].as_u64().unwrap_or(0);
                let verified = cooperators::id::is_ulid(&id)
                    && seq > 0
                    && receipt["duplicate"] == false
                    && receipt["to"] == sender.receiver.name
                    && receipt["to_agent_id"] == sender.receiver.agent_id;
                rows.push(Sample {
                    message_id: id,
                    seq,
                    sender_id: sender.agent.agent_id.clone(),
                    sender_name: sender.agent.name.clone(),
                    receiver_id: sender.receiver.agent_id.clone(),
                    receiver_name: sender.receiver.name.clone(),
                    iteration: rows.len(),
                    warmup,
                    started_monotonic_ns: started,
                    completed_monotonic_ns: ended,
                    send_ack_ms: (ended - started) as f64 / 1e6,
                    receipt: receipt.clone(),
                    receipt_verified: verified,
                });
                if !verified {
                    errors.push(format!(
                        "invalid send receipt for {}: {receipt}",
                        sender.agent.agent_id
                    ));
                    failed.store(true, Ordering::Release);
                    break;
                }
            }
            Err(error) => {
                attempts.push(json!({"method": "send_message", "sender_id": sender.agent.agent_id,
                    "receiver_id": sender.receiver.agent_id, "warmup": warmup,
                    "started_monotonic_ns": started, "completed_monotonic_ns": ended, "error": error}));
                errors.push(error);
                failed.store(true, Ordering::Release);
                break;
            }
        }
    }
    SenderOutcome {
        sender,
        rows,
        errors,
        attempts,
    }
}

async fn window(
    senders: Vec<Sender>,
    warmup: bool,
    origin: Instant,
    failed: Arc<AtomicBool>,
    daemon_pid: u32,
    ticks: f64,
) -> WindowOutcome {
    let ready = Arc::new(Barrier::new(senders.len() + 1));
    let release = Arc::new(Barrier::new(senders.len() + 1));
    let clock = Arc::new(OnceLock::new());
    let mut tasks = JoinSet::new();
    for sender in senders {
        tasks.spawn(sender_window(
            sender,
            warmup,
            ready.clone(),
            release.clone(),
            clock.clone(),
            origin,
            failed.clone(),
        ));
    }
    ready.wait().await;
    let driver_before = resource(std::process::id());
    let daemon_before = resource(daemon_pid);
    let started = Instant::now();
    let start_ns = started.duration_since(origin).as_nanos() as u64;
    let _ = clock.set((
        started,
        started + Duration::from_secs(if warmup { 1 } else { 10 }),
    ));
    release.wait().await;
    let mut result = WindowOutcome {
        senders: Vec::new(),
        samples: Vec::new(),
        attempts: Vec::new(),
        errors: Vec::new(),
        elapsed_seconds: 0.0,
        resources: Value::Null,
    };
    while let Some(outcome) = tasks.join_next().await {
        match outcome {
            Ok(outcome) => {
                result.senders.push(outcome.sender);
                result.samples.extend(outcome.rows);
                result.attempts.extend(outcome.attempts);
                result.errors.extend(outcome.errors);
            }
            Err(error) => {
                result.errors.push(format!(
                    "sender task failed: {error}; task-local evidence may be incomplete"
                ));
                failed.store(true, Ordering::Release);
            }
        }
    }
    let driver_after = resource(std::process::id());
    let daemon_after = resource(daemon_pid);
    let resource_seconds = started.elapsed().as_secs_f64();
    let ended_ns = result
        .samples
        .iter()
        .map(|row| row.completed_monotonic_ns)
        .max()
        .unwrap_or(start_ns);
    result.elapsed_seconds = (ended_ns - start_ns) as f64 / 1e9;
    if result.samples.is_empty() {
        result.errors.push("empty send window".into());
    }
    result.resources = json!({"warmup": warmup, "resource_window_seconds": resource_seconds,
        "sample_window_seconds": result.elapsed_seconds,
        "driver": resource_delta(&driver_before, &driver_after, resource_seconds, ticks),
        "daemon": resource_delta(&daemon_before, &daemon_after, resource_seconds, ticks)});
    result
}

async fn drain_batch(
    client: &mut RpcClient,
    agent: &Agent,
    allowed_senders: &HashSet<String>,
    evidence: &mut ReceiverEvidence,
    origin: Instant,
    delivered: &AtomicU64,
) -> Result<bool> {
    let started = ns(origin);
    let response = client.call("wait_message", json!({"timeout_s": 0})).await;
    let completed = ns(origin);
    evidence.wait_calls += 1;
    let batch = match response {
        Ok(value) => value,
        Err(error) => {
            evidence.rpc_events.push(
                json!({"method": "wait_message", "receiver_id": agent.agent_id,
            "started_monotonic_ns": started, "completed_monotonic_ns": completed, "error": error}),
            );
            return Err(error);
        }
    };
    let previous = evidence.acked_head;
    let checked = (|| -> Result<Vec<Envelope>> {
        require(
            batch["cursor"] == previous.to_string()
                && batch["acked_cursor"] == previous.to_string(),
            "wait cursor/acked_cursor mismatch",
        )?;
        let values = batch["messages"]
            .as_array()
            .ok_or("wait messages is not an array")?;
        require(values.len() <= 100, "default wait batch exceeded 100")?;
        let messages: Vec<Envelope> =
            serde_json::from_value(batch["messages"].clone()).map_err(|e| e.to_string())?;
        require(
            batch["timed_out"] == messages.is_empty(),
            "wait timed_out mismatch",
        )?;
        let next = previous + messages.len() as u64;
        require(
            batch["next_cursor"] == next.to_string(),
            "wait next_cursor mismatch",
        )?;
        for (offset, message) in messages.iter().enumerate() {
            message.validate().map_err(|e| e.to_string())?;
            require(
                message.seq == previous + offset as u64 + 1,
                "noncontiguous delivered sequence",
            )?;
            require(
                message.to == agent.name && allowed_senders.contains(&message.from),
                "delivered identity mismatch",
            )?;
            require(
                message.kind == "benchmark"
                    && message.body.mime == "text/plain"
                    && message.body.text == PAYLOAD,
                "delivered kind/payload mismatch",
            )?;
        }
        Ok(messages)
    })();
    let messages = match checked {
        Ok(messages) => messages,
        Err(error) => {
            evidence.rpc_events.push(
                json!({"method": "wait_message", "receiver_id": agent.agent_id,
            "started_monotonic_ns": started, "completed_monotonic_ns": completed,
            "response": batch, "validation_error": error}),
            );
            return Err(error);
        }
    };
    evidence.rpc_events.push(json!({"method": "wait_message", "receiver_id": agent.agent_id,
        "started_monotonic_ns": started, "completed_monotonic_ns": completed,
        "cursor": batch["cursor"], "next_cursor": batch["next_cursor"], "acked_cursor": batch["acked_cursor"],
        "timed_out": batch["timed_out"], "message_count": messages.len(), "verified": true}));
    evidence.max_batch = evidence.max_batch.max(messages.len());
    if messages.is_empty() {
        return Ok(false);
    }
    let next = previous + messages.len() as u64;
    let first = evidence.deliveries.len();
    for message in &messages {
        evidence.deliveries.push(Delivery {
            message_id: message.id.clone(),
            seq: message.seq,
            sender_name: message.from.clone(),
            receiver_name: message.to.clone(),
            receiver_id: agent.agent_id.clone(),
            envelope: serde_json::to_value(message).map_err(|e| e.to_string())?,
            delivery_verified: true,
            ack_verified: false,
        });
    }
    let started = ns(origin);
    let response = client
        .call("ack", json!({"cursor": next.to_string()}))
        .await;
    let completed = ns(origin);
    evidence.ack_calls += 1;
    match response {
        Ok(ack) => {
            let verified = ack["acked_cursor"] == next.to_string();
            evidence
                .rpc_events
                .push(json!({"method": "ack", "receiver_id": agent.agent_id,
                "started_monotonic_ns": started, "completed_monotonic_ns": completed,
                "requested_cursor": next.to_string(), "response": ack, "verified": verified}));
            require(verified, "ack cursor mismatch")?;
        }
        Err(error) => {
            evidence
                .rpc_events
                .push(json!({"method": "ack", "receiver_id": agent.agent_id,
            "started_monotonic_ns": started, "completed_monotonic_ns": completed,
            "requested_cursor": next.to_string(), "error": error}));
            return Err(error);
        }
    }
    for delivery in &mut evidence.deliveries[first..] {
        delivery.ack_verified = true;
    }
    evidence.acked_head = next;
    delivered.fetch_add(messages.len() as u64, Ordering::Release);
    Ok(true)
}

async fn receiver(
    mut client: RpcClient,
    agent: Agent,
    allowed: Arc<HashSet<String>>,
    origin: Instant,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    delivered: Arc<AtomicU64>,
) -> ReceiverOutcome {
    let mut evidence = ReceiverEvidence::default();
    while !stop.load(Ordering::Acquire) {
        let mut error = None;
        loop {
            match drain_batch(
                &mut client,
                &agent,
                &allowed,
                &mut evidence,
                origin,
                &delivered,
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => break,
                Err(failure) => {
                    error = Some(failure);
                    break;
                }
            }
        }
        if let Some(error) = error {
            evidence.errors.push(error);
            failed.store(true, Ordering::Release);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ReceiverOutcome {
        client,
        agent,
        evidence,
    }
}

async fn caught_up(
    expected: usize,
    delivered: &AtomicU64,
    tasks: &JoinSet<ReceiverOutcome>,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while delivered.load(Ordering::Acquire) < expected as u64 {
        require(
            !tasks.is_empty(),
            "all receiver tasks exited before catch-up",
        )?;
        require(
            Instant::now() < deadline,
            "receiver catch-up deadline expired",
        )?;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    require(
        delivered.load(Ordering::Acquire) == expected as u64,
        "more acknowledged deliveries than successful receipts",
    )
}

async fn workload(
    socket: &Path,
    label: &str,
    profile: &str,
    concurrency: usize,
    daemon_pid: u32,
    ticks: f64,
    credentials: &mut Vec<String>,
    evidence: &mut WorkloadEvidence,
) -> Result<()> {
    let origin = Instant::now();
    let mut enrolled = Vec::new();
    for index in 0..concurrency {
        let pair = enroll(socket, label, index, credentials).await?;
        evidence.agents.push(pair.1.clone());
        enrolled.push(pair);
    }
    let mut receivers = Vec::new();
    if profile == "same-inbox" {
        let pair = enroll(socket, label, concurrency, credentials).await?;
        evidence.agents.push(pair.1.clone());
        receivers.push(pair);
    } else {
        for (client, agent) in &enrolled {
            receivers.push((
                RpcClient::connect(socket, client.token.clone()).await?,
                agent.clone(),
            ));
        }
    }
    let destinations: Vec<_> = receivers.iter().map(|(_, agent)| agent.clone()).collect();
    let allowed: Arc<HashSet<String>> = Arc::new(
        enrolled
            .iter()
            .map(|(_, agent)| agent.name.clone())
            .collect(),
    );
    let senders = enrolled
        .into_iter()
        .enumerate()
        .map(|(index, (client, agent))| Sender {
            client,
            agent,
            receiver: destinations[if profile == "same-inbox" { 0 } else { index }].clone(),
        })
        .collect();
    let stop = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let delivered = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    for (client, agent) in receivers {
        tasks.spawn(receiver(
            client,
            agent,
            allowed.clone(),
            origin,
            stop.clone(),
            failed.clone(),
            delivered.clone(),
        ));
    }
    let warmup = window(senders, true, origin, failed.clone(), daemon_pid, ticks).await;
    evidence.samples.extend(warmup.samples);
    evidence.attempts.extend(warmup.attempts);
    evidence.errors.extend(warmup.errors);
    evidence.resources.push(warmup.resources);
    match caught_up(evidence.samples.len(), &delivered, &tasks).await {
        Ok(()) => evidence.warmup_fully_acked = true,
        Err(error) => evidence.errors.push(error),
    }
    if evidence.errors.is_empty() && !failed.load(Ordering::Acquire) {
        let measured = window(
            warmup.senders,
            false,
            origin,
            failed.clone(),
            daemon_pid,
            ticks,
        )
        .await;
        evidence.measured_elapsed_seconds = Some(measured.elapsed_seconds);
        evidence.samples.extend(measured.samples);
        evidence.attempts.extend(measured.attempts);
        evidence.errors.extend(measured.errors);
        evidence.resources.push(measured.resources);
        drop(measured.senders);
    } else {
        drop(warmup.senders);
    }
    // Catch-up is outside the measured rate denominator, including on send errors.
    if let Err(error) = caught_up(evidence.samples.len(), &delivered, &tasks).await {
        evidence.errors.push(error);
    }
    stop.store(true, Ordering::Release);
    let mut final_empty_checks_pass = true;
    while let Some(outcome) = tasks.join_next().await {
        match outcome {
            Ok(mut outcome) => {
                evidence
                    .errors
                    .extend(outcome.evidence.errors.iter().cloned());
                let started = ns(origin);
                match outcome
                    .client
                    .call("wait_message", json!({"timeout_s": 0}))
                    .await
                {
                    Ok(empty) => {
                        let head = outcome.evidence.acked_head.to_string();
                        let verified = empty["messages"].as_array().is_some_and(Vec::is_empty)
                            && empty["timed_out"] == true
                            && empty["acked_cursor"] == head
                            && empty["cursor"] == head
                            && empty["next_cursor"] == head;
                        outcome.evidence.rpc_events.push(json!({"method": "wait_message", "final_empty_check": true,
                            "receiver_id": outcome.agent.agent_id, "started_monotonic_ns": started,
                            "completed_monotonic_ns": ns(origin), "response": empty, "verified": verified}));
                        if !verified {
                            final_empty_checks_pass = false;
                            evidence.errors.push(format!(
                                "receiver {} final empty/cursor check failed",
                                outcome.agent.agent_id
                            ));
                        }
                    }
                    Err(error) => {
                        final_empty_checks_pass = false;
                        outcome.evidence.rpc_events.push(
                            json!({"method": "wait_message", "final_empty_check": true,
                        "receiver_id": outcome.agent.agent_id, "started_monotonic_ns": started,
                        "completed_monotonic_ns": ns(origin), "error": error}),
                        );
                        evidence.errors.push(error);
                    }
                }
                evidence.receivers.push(outcome);
            }
            Err(error) => {
                final_empty_checks_pass = false;
                evidence.errors.push(format!(
                    "receiver task failed: {error}; task-local evidence may be incomplete"
                ));
            }
        }
    }
    evidence.all_receivers_empty =
        final_empty_checks_pass && evidence.receivers.len() == destinations.len();
    Ok(())
}

fn disk_validation(root: &Path, evidence: &WorkloadEvidence) -> Result<Value> {
    let mut errors = Vec::new();
    let mut check = |condition: bool, message: String| {
        if !condition {
            errors.push(message);
        }
    };
    let expected: HashMap<_, _> = evidence
        .samples
        .iter()
        .map(|row| (row.message_id.as_str(), row))
        .collect();
    check(
        expected.len() == evidence.samples.len(),
        "duplicate successful receipt IDs".into(),
    );
    check(
        evidence.samples.iter().all(|row| row.receipt_verified),
        "invalid successful receipt".into(),
    );
    let deliveries: Vec<_> = evidence
        .receivers
        .iter()
        .flat_map(|outcome| &outcome.evidence.deliveries)
        .collect();
    let received: HashMap<_, _> = deliveries
        .iter()
        .map(|row| (row.message_id.as_str(), *row))
        .collect();
    check(
        received.len() == deliveries.len(),
        "duplicate delivered IDs".into(),
    );
    check(
        received.len() == expected.len(),
        "delivery/receipt cardinalities differ".into(),
    );
    for (id, row) in &expected {
        match received.get(id) {
            Some(delivery) => check(
                delivery.delivery_verified
                    && delivery.ack_verified
                    && delivery.seq == row.seq
                    && delivery.sender_name == row.sender_name
                    && delivery.receiver_name == row.receiver_name
                    && delivery.receiver_id == row.receiver_id,
                format!("delivery/receipt mismatch: {id}"),
            ),
            None => check(false, format!("receipt not delivered: {id}")),
        }
    }
    let agents_path = root.join("agents");
    let inboxes: Vec<_> = regular_files(&agents_path)?
        .into_iter()
        .filter(|path| path.file_name().is_some_and(|name| name == "inbox.jsonl"))
        .filter(|path| path.parent().and_then(Path::parent) == Some(agents_path.as_path()))
        .collect();
    let mut actual = HashSet::new();
    let mut heads = BTreeMap::new();
    let mut log_reads = Vec::new();
    for path in inboxes {
        let receiver_id = path
            .parent()
            .and_then(Path::file_name)
            .ok_or("inbox path has no agent ID")?
            .to_string_lossy()
            .into_owned();
        // Never repair evidence. Keep checking independent inboxes after a failure.
        let replay = match LogFile::<Envelope>::read_segments(&path) {
            Ok(replay) => replay,
            Err(error) => {
                check(false, format!("{}: {error}", path.display()));
                log_reads.push(json!({"receiver_id": receiver_id, "hash_chain_read_pass": false, "error": error.to_string()}));
                continue;
            }
        };
        let clean_tail = replay.truncated_bytes == 0 && !replay.trailing_newline_missing;
        check(
            clean_tail,
            format!("{receiver_id}: persisted inbox has torn/unterminated tail"),
        );
        log_reads.push(
            json!({"receiver_id": receiver_id, "hash_chain_read_pass": clean_tail,
            "records_read": replay.records.len(), "truncated_bytes": replay.truncated_bytes,
            "trailing_newline_missing": replay.trailing_newline_missing}),
        );
        let mut previous = GENESIS_HASH.to_string();
        for (index, message) in replay.records.iter().enumerate() {
            check(
                message.validate().is_ok(),
                format!("invalid persisted envelope: {}", message.id),
            );
            check(
                message.seq == index as u64 + 1 && message.prev_hash == previous,
                format!("persisted sequence/hash mismatch: {}", message.id),
            );
            match hash_record(message) {
                Ok(hash) => previous = hash,
                Err(error) => check(false, format!("record {} hash failed: {error}", message.id)),
            }
            check(
                actual.insert(message.id.clone()),
                format!("duplicate persisted message ID: {}", message.id),
            );
            match expected.get(message.id.as_str()) {
                Some(row) => check(
                    message.seq == row.seq
                        && message.from == row.sender_name
                        && message.to == row.receiver_name
                        && receiver_id == row.receiver_id
                        && message.kind == "benchmark"
                        && message.body.mime == "text/plain"
                        && message.body.text == PAYLOAD,
                    format!(
                        "persisted receipt/payload/identity mismatch: {}",
                        message.id
                    ),
                ),
                None => check(
                    false,
                    format!("persisted record has no successful receipt: {}", message.id),
                ),
            }
            match received.get(message.id.as_str()) {
                Some(delivered) => check(
                    serde_json::to_value(message).map_err(|e| e.to_string())? == delivered.envelope,
                    format!("delivered/persisted envelope mismatch: {}", message.id),
                ),
                None => check(false, format!("disk record not delivered: {}", message.id)),
            }
        }
        check(
            previous == replay.last_hash && replay.seq_head == replay.records.len() as u64,
            format!("{receiver_id}: read-only replay head mismatch"),
        );
        match evidence
            .receivers
            .iter()
            .find(|outcome| outcome.agent.agent_id == receiver_id)
        {
            Some(receiver) => check(
                receiver.evidence.acked_head == replay.seq_head,
                format!("{receiver_id}: acknowledged cursor differs from persisted head"),
            ),
            None => check(
                replay.seq_head == 0,
                format!("{receiver_id}: nonreceiver inbox has records"),
            ),
        }
        heads.insert(
            receiver_id,
            json!({"seq": replay.seq_head, "hash": replay.last_hash}),
        );
    }
    check(
        actual.len() == expected.len() && expected.keys().all(|id| actual.contains(*id)),
        "persisted IDs differ from successful receipts".into(),
    );
    let correct = errors.is_empty();
    Ok(
        json!({"correctness_passed": correct, "receipt_count": expected.len(), "durable_records": actual.len(),
        "verified_deliveries_and_acks": deliveries.iter().filter(|row| row.delivery_verified && row.ack_verified).count(),
        "unique_receipts": expected.len() == evidence.samples.len(), "contiguous_sequences": correct,
        "hash_chains_verified": log_reads.iter().all(|row| row["hash_chain_read_pass"] == true),
        "exact_envelope_disk_delivery_match": correct, "inbox_heads": heads, "all_inbox_reads": log_reads, "errors": errors,
        "reader": "LogFile<Envelope>::read_segments and storage::hash_record; no repair/open"}),
    )
}

fn percentiles(samples: &[&Sample]) -> Value {
    let mut values: Vec<_> = samples.iter().map(|row| row.send_ack_ms).collect();
    values.sort_by(f64::total_cmp);
    if values.is_empty() {
        return Value::Null;
    }
    let rank = |p: usize| values[(values.len() * p).div_ceil(100).saturating_sub(1)];
    json!({"count": values.len(), "min": values[0], "max": values[values.len() - 1],
        "mean": values.iter().sum::<f64>() / values.len() as f64, "p50": rank(50), "p95": rank(95), "p99": rank(99)})
}

async fn phase(
    args: &Args,
    profile: &str,
    concurrency: usize,
    binary_sha: &str,
    ticks: f64,
    credentials: &mut Vec<String>,
) -> Result<Value> {
    let label = format!("{profile}-c{concurrency:03}");
    println!(
        "START {}/{label}: 10s native UDS, 128 bytes, one outstanding/sender",
        args.label
    );
    let output = args.output.join(&label);
    fs::create_dir(&output).map_err(|e| e.to_string())?;
    let root = TempRoot::new()?;
    let socket = root.0.join("run/daemon.sock");
    let mut daemon_info = json!({"label": label, "root": root.0, "start_utc": cooperators::id::now_rfc3339(),
        "http": "off", "correctness_passed": false});
    let mut evidence = WorkloadEvidence::new();
    let mut process: Option<DaemonProcess> = None;
    let execution: Result<()> = async {
        let mut command = Command::new(&args.binary);
        command
            .arg("--root")
            .arg(&root.0)
            .args(["daemon", "run", "--http-addr", "off"])
            .env_remove("COOPERATORS_HOME")
            .env_remove("COOP_SOCKET")
            .env_remove("COOP_HTTP_ADDR")
            .env_remove("COOP_TOKEN")
            .stdin(Stdio::null())
            .stdout(File::create(output.join("daemon.stdout.log")).map_err(|e| e.to_string())?)
            .stderr(File::create(output.join("daemon.stderr.log")).map_err(|e| e.to_string())?);
        let child = command.spawn().map_err(|e| e.to_string())?;
        let pid = child.id();
        daemon_info["pid"] = json!(pid);
        daemon_info["command"] = json!([
            args.binary.to_string_lossy(),
            "--root",
            root.0.to_string_lossy(),
            "daemon",
            "run",
            "--http-addr",
            "off"
        ]);
        process = Some(DaemonProcess {
            child,
            stopped: false,
        });
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let process = process.as_mut().ok_or("daemon guard absent")?;
            require(
                process
                    .child
                    .try_wait()
                    .map_err(|e| e.to_string())?
                    .is_none(),
                "daemon exited before readiness",
            )?;
            if socket.exists() {
                if let Ok(mut client) = RpcClient::connect(&socket, None).await {
                    let status = client.call("daemon.status", json!({})).await?;
                    require(status["pid"] == pid, "daemon.status PID mismatch")?;
                    break;
                }
            }
            require(
                Instant::now() < deadline,
                "15-second daemon readiness deadline expired",
            )?;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let actual_sha = sha(Path::new(&format!("/proc/{pid}/exe")))?;
        daemon_info["running_executable_sha256"] = json!(actual_sha);
        require(
            actual_sha == binary_sha,
            "actual running daemon hash differs from selected binary",
        )?;
        workload(
            &socket,
            &label,
            profile,
            concurrency,
            pid,
            ticks,
            credentials,
            &mut evidence,
        )
        .await
    }
    .await;
    if let Err(error) = execution {
        evidence.errors.push(error);
    }
    // All RPC tasks have joined before child exit and raw disk validation.
    // Idle receiver descriptors close when the retained evidence is dropped.
    if let Some(process) = process.as_mut() {
        match process.stop() {
            Ok(status) => daemon_info["stop"] = json!(status),
            Err(error) => {
                evidence.errors.push(format!("daemon cleanup: {error}"));
            }
        }
    }
    // Drop the process guard now so a stop error still reaps before validation.
    drop(process);
    let disk = match disk_validation(&root.0, &evidence) {
        Ok(value) => {
            if value["correctness_passed"] != true {
                evidence.errors.push(format!(
                    "read-only disk validation failed: {}",
                    value["errors"]
                ));
            }
            value
        }
        Err(error) => {
            evidence
                .errors
                .push(format!("read-only disk validation: {error}"));
            json!({"correctness_passed": false, "error": error})
        }
    };
    evidence.samples.sort_by_key(|row| row.started_monotonic_ns);
    write_jsonl(
        &output.join("samples.jsonl"),
        &evidence.samples,
        credentials,
    )?;
    write_jsonl(
        &output.join("failed-send-attempts.jsonl"),
        &evidence.attempts,
        credentials,
    )?;
    let deliveries: Vec<_> = evidence
        .receivers
        .iter()
        .flat_map(|receiver| &receiver.evidence.deliveries)
        .collect();
    write_jsonl(&output.join("deliveries.jsonl"), &deliveries, credentials)?;
    let mut rpc_events: Vec<_> = evidence
        .receivers
        .iter()
        .flat_map(|receiver| &receiver.evidence.rpc_events)
        .collect();
    rpc_events.sort_by_key(|row| row["started_monotonic_ns"].as_u64().unwrap_or(0));
    write_jsonl(&output.join("receiver-rpc.jsonl"), &rpc_events, credentials)?;
    write_json(
        &output.join("process-resources.json"),
        &evidence.resources,
        credentials,
    )?;
    write_json(&output.join("disk-validation.json"), &disk, credentials)?;
    let measured: Vec<_> = evidence.samples.iter().filter(|row| !row.warmup).collect();
    let latency = percentiles(&measured);
    let rate = evidence
        .measured_elapsed_seconds
        .filter(|seconds| *seconds > 0.0)
        .map(|seconds| measured.len() as f64 / seconds);
    let throughput_pass = rate.is_some_and(|rate| rate >= 5000.0);
    let latency_pass = latency["p50"].as_f64().is_some_and(|p50| p50 < 10.0);
    let correct = evidence.errors.is_empty()
        && disk["correctness_passed"] == true
        && evidence.warmup_fully_acked
        && evidence.all_receivers_empty
        && !measured.is_empty();
    let receiver_summary: Vec<_> = evidence.receivers.iter().map(|receiver| json!({"agent": receiver.agent,
        "wait_calls": receiver.evidence.wait_calls, "ack_calls": receiver.evidence.ack_calls,
        "max_batch": receiver.evidence.max_batch, "acked_head": receiver.evidence.acked_head,
        "delivered_records": receiver.evidence.deliveries.len(), "errors": receiver.evidence.errors})).collect();
    let summary = json!({"label": label, "profile": profile, "transport": "native-async-uds", "http": "off",
        "active_sender_agents": concurrency, "active_receiver_connections": receiver_summary.len(),
        "enrolled_agents": evidence.agents.len(), "receiver_identity_shared_with_sender": profile == "distributed-inboxes",
        "outstanding_sends_per_sender": 1, "warmup_messages": evidence.samples.iter().filter(|row| row.warmup).count(),
        "warmup_fully_delivered_and_acked_before_measurement": evidence.warmup_fully_acked,
        "measured_messages": measured.len(), "requested_measurement_seconds": 10,
        "elapsed_seconds": evidence.measured_elapsed_seconds, "durable_acknowledgments_per_second": rate,
        "send_ack_ms": latency, "throughput_5000_per_second_target_pass": throughput_pass,
        "send_ack_p50_below_10_ms_target_pass": latency_pass, "correctness_passed": correct,
        "phase_completed_without_errors": evidence.errors.is_empty(),
        "all_receiver_inboxes_empty_with_verified_acked_heads": evidence.all_receivers_empty,
        "receiver_drain": receiver_summary, "errors": evidence.errors,
        "all_targets_and_correctness_pass": correct && throughput_pass && latency_pass,
        "partial_sample_warning": if evidence.errors.is_empty() { Value::Null } else { json!("Errors invalidate qualification; partial rates are descriptive only") }});
    daemon_info["agents"] = json!(evidence.agents);
    daemon_info["correctness_passed"] = json!(correct);
    daemon_info["errors"] = json!(evidence.errors);
    write_json(&output.join("daemon.json"), &daemon_info, credentials)?;
    write_json(&output.join("summary.json"), &summary, credentials)?;
    for source in regular_files(&root.0)? {
        let target = output
            .join("raw-root")
            .join(source.strip_prefix(&root.0).map_err(|e| e.to_string())?);
        fs::create_dir_all(target.parent().ok_or("raw target has no parent")?)
            .map_err(|e| e.to_string())?;
        fs::copy(&source, target).map_err(|e| e.to_string())?;
    }
    println!(
        "DONE {}/{label}: {} acks/s; p50 {} ms; correctness {}; errors {}",
        args.label,
        rate.map(|rate| format!("{rate:.1}"))
            .unwrap_or("unavailable".into()),
        latency["p50"],
        correct,
        evidence.errors.len()
    );
    Ok(summary)
}

fn plan(args: &Args) -> Value {
    let full = args.profiles.iter().map(String::as_str).collect::<Vec<_>>() == PROFILES
        && args.concurrencies == CONCURRENCIES;
    json!({"schema": "cooperators.native-concurrent-throughput-plan/v1", "declared_utc": cooperators::id::now_rfc3339(),
        "profiles": args.profiles, "sender_concurrencies": args.concurrencies, "full_eight_profile_qualification": full,
        "driver_change": "Tokio asynchronous persistent UnixStream clients replace Python threads and socket.makefile; production binary selected separately",
        "transport": "persistent AF_UNIX NDJSON JSON-RPC 2.0; token included on every authenticated request", "http": "off for both native baseline and candidate; frozen Python driver enabled an idle HTTP listener",
        "payload": {"mime": "text/plain", "text_bytes": 128, "text": PAYLOAD},
        "outstanding_sends_per_sender": 1, "same_inbox": "C senders plus one additional receiver identity",
        "distributed_inboxes": "C self-sending identities; separate persistent receiver connection per identity",
        "warmup_seconds": 1, "warmup_minimum_sends_per_sender": 3, "measurement_seconds": 10,
        "scheduling": "Closed loop per sender; ready barrier then release barrier; deadline set at release; fully ack warmup before measurement",
        "rate_denominator": "Wall time from barrier release through last successful receipt for requests admitted before deadline; includes in-flight completions",
        "rate_numerator": "All successful measured-window receipts, including completions after deadline; errored phases are invalid qualification",
        "receiver_drain": {"interval_ms": 20, "operation": "Repeated wait_message(timeout_s=0), verify batch, ack, repeat until empty; sleep only after empty",
            "default_batch_limit": 100, "capacity": "Unchanged 10000-message/64MiB caps; no client credits or pacing", "catchup": "30-second final catch-up outside measured interval"},
        "quantiles": "Nearest rank client call latency p50/p95/p99, excluding warmup",
        "targets_declared_before_measurement": {"durable_acknowledgments_per_second_gte": 5000, "send_ack_p50_ms_lt": 10,
            "application": "Every topology/concurrency independently; aggregate gate fails on any phase error or target miss"},
        "correctness": ["Unique validated send receipts; exact sender/recipient/payload/kind/sequence correspondence",
            "Contiguous wait and ack cursors; each delivery acked; final inboxes empty at persisted heads",
            "After child exit, read_segments/hash_record verify every disk row and chain against every receipt and delivered envelope without repair",
            "Raw successful receipts, deliveries, cursor RPC timing, and failure attempts retained even on capacity errors",
            "Actual /proc child executable hash equals selected binary; source, driver, helper and binary identity recorded before and after",
            "Credential scan across every retained regular-file byte; no plaintext tokens or launch keys retained"],
        "limitations": ["New driver control does not replace or invalidate earlier Python qualification failures",
            "One host/filesystem and one 10-second window per condition; no confidence intervals",
            "Different client/runtime and idle HTTP listener setting from frozen Python controls; compare native baseline and native candidate using the identical driver",
            "Client verification, framing and concurrent drains remain inside timed workload; evidence serialization and disk readback occur outside timing",
            "A single outstanding sender cannot supply group commit overlap; no externally paced arrival-rate claim",
            "Readback verifies retained records and acknowledged effects, not crash durability under power loss",
            "A filtered run is diagnostic and never a full eight-profile qualification"]})
}

fn credential_scan(output: &Path, credentials: &[String]) -> Result<Value> {
    let secrets: HashSet<&[u8]> = credentials
        .iter()
        .map(|credential| credential.as_bytes())
        .collect();
    let mut scanned = 0u64;
    let mut leaks = Vec::new();
    for path in regular_files(output)? {
        let mut bytes = fs::read(&path).map_err(|e| e.to_string())?;
        scanned += bytes.len() as u64;
        let mut position = 0;
        let mut found = false;
        while position < bytes.len() {
            if !(bytes[position].is_ascii_digit() || (b'a'..=b'f').contains(&bytes[position])) {
                position += 1;
                continue;
            }
            let start = position;
            while position < bytes.len()
                && (bytes[position].is_ascii_digit() || (b'a'..=b'f').contains(&bytes[position]))
            {
                position += 1;
            }
            if position - start >= 32 {
                for index in start..=position - 32 {
                    if secrets.contains(&bytes[index..index + 32]) {
                        bytes[index..index + 32].fill(b'*');
                        found = true;
                    }
                }
            }
        }
        if found {
            // Remove any accidental plaintext immediately, but fail the run audit.
            fs::write(&path, bytes).map_err(|e| e.to_string())?;
            leaks.push(
                path.strip_prefix(output)
                    .map_err(|e| e.to_string())?
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    Ok(
        json!({"credential_plaintext_absent": true, "generated_credentials_checked": secrets.len(),
        "regular_file_bytes_scanned": scanned, "unexpected_leaks_redacted_and_run_failed": leaks,
        "audit_passed": leaks.is_empty(), "check": "All contiguous lowercase hexadecimal runs scanned for every generated 32-byte credential, including substrings of longer runs"}),
    )
}

#[tokio::main(worker_threads = 4)]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut args = Args::parse();
    require(!cfg!(debug_assertions), "build this example with --release")?;
    require(
        args.label != "baseline" || args.source_manifest.is_some(),
        "baseline requires --source-manifest",
    )?;
    require(
        !args.concurrencies.is_empty()
            && args.concurrencies.iter().all(|n| CONCURRENCIES.contains(n)),
        "concurrencies must be selected from 1 8 32 100",
    )?;
    require(
        args.concurrencies.iter().collect::<HashSet<_>>().len() == args.concurrencies.len(),
        "duplicate concurrencies",
    )?;
    require(
        args.profiles.iter().collect::<HashSet<_>>().len() == args.profiles.len(),
        "duplicate profiles",
    )?;
    args.binary = args.binary.canonicalize()?;
    require(
        !args.output.exists(),
        "output already exists; provide a new directory",
    )?;
    fs::create_dir_all(&args.output)?;
    args.output = args.output.canonicalize()?;
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let driver_source = repository.join("examples/throughput_native.rs");
    let driver_binary = std::env::current_exe()?.canonicalize()?;
    let initial_source = sources(&repository)?;
    let driver_sha = sha(&driver_source)?;
    let driver_binary_sha = sha(&driver_binary)?;
    let binary_sha = sha(&args.binary)?;
    let python_sha = sha(&repository.join("scripts/benchmark_throughput.py"))?;
    let helper_sha = sha(&repository.join("scripts/benchmark_transport.py"))?;
    let method_sha = sha(&repository.join("docs/impl/throughput-20261020/native/method.md"))?;
    let mut source_identity = json!({"basis": "Observed worktree source; separate release build provenance required to attribute executable", "source_sha256": initial_source});
    let mut pinned_manifest_sha = None;
    if let Some(path) = &args.source_manifest {
        let manifest: Value = serde_json::from_slice(&fs::read(path)?)?;
        let pinned_binary = manifest["release_binary"]["sha256"]
            .as_str()
            .or_else(|| manifest["release_binary_sha256"].as_str());
        require(
            pinned_binary == Some(binary_sha.as_str()),
            "source manifest binary hash does not match selected executable",
        )?;
        let pinned_sources = manifest
            .get("source_files")
            .or_else(|| manifest.get("files"))
            .ok_or("source manifest lacks source_files/files")?;
        pinned_manifest_sha = Some(sha(path)?);
        source_identity = json!({"basis": "Retained original build/source manifest; current source is not automatically attributed to selected executable",
            "manifest": path.canonicalize()?, "manifest_sha256": pinned_manifest_sha, "source_sha256": pinned_sources,
            "base_commit": manifest.get("base_commit")});
    }
    let ticks = command(&["getconf", "CLK_TCK"])
        .as_str()
        .and_then(|text| text.parse::<f64>().ok())
        .ok_or("cannot obtain CLK_TCK")?;
    let declared_plan = plan(&args);
    let environment = json!({"label": args.label, "start_utc": cooperators::id::now_rfc3339(), "git_head": command(&["git", "rev-parse", "HEAD"]),
        "git_status_before": command(&["git", "status", "--short"]), "binary": args.binary, "binary_sha256": binary_sha,
        "executable_source_identity": source_identity, "observed_worktree_source_sha256": initial_source,
        "driver_source": driver_source, "driver_source_sha256": driver_sha, "driver_binary": driver_binary,
        "driver_binary_sha256": driver_binary_sha, "frozen_python_harness_sha256": python_sha,
        "frozen_python_transport_helper_sha256": helper_sha, "method_sha256": method_sha,
        "logical_cpus": std::thread::available_parallelism()?.get(), "tokio_worker_threads": 4,
        "rustc": command(&["rustc", "--version"]), "kernel": fs::read_to_string("/proc/sys/kernel/osrelease")?.trim(),
        "cpu": fs::read_to_string("/proc/cpuinfo")?.lines().find(|line| line.starts_with("model name")).unwrap_or("unknown"),
        "filesystem_tmp": command(&["stat", "-f", "-c", "%T", "/tmp"]),
        "filesystem_mount": command(&["findmnt", "-T", "/tmp", "-o", "SOURCE,FSTYPE,OPTIONS", "-n"]),
        "clock_ticks_per_second": ticks, "http": "off", "meminfo_before": fs::read_to_string("/proc/meminfo")?});
    write_json(&args.output.join("plan.json"), &declared_plan, &[])?;
    write_json(&args.output.join("environment.json"), &environment, &[])?;
    let mut credentials = Vec::new();
    let mut results = Vec::new();
    let mut run_errors = Vec::new();
    for profile in &args.profiles {
        for &concurrency in &args.concurrencies {
            match phase(
                &args,
                profile,
                concurrency,
                &binary_sha,
                ticks,
                &mut credentials,
            )
            .await
            {
                Ok(result) => results.push(result),
                Err(error) => {
                    let error = redact(error, &credentials);
                    eprintln!("PHASE ERROR {profile}/C={concurrency}: {error}");
                    run_errors.push(format!("{profile}/C={concurrency}: {error}"));
                    results.push(
                        json!({"profile": profile, "active_sender_agents": concurrency,
                        "phase_completed_without_errors": false, "correctness_passed": false,
                        "all_targets_and_correctness_pass": false, "error": error}),
                    );
                }
            }
        }
    }
    let final_source = sources(&repository)?;
    let identities_after = json!({"binary_sha256": sha(&args.binary)?, "driver_source_sha256": sha(&driver_source)?,
        "driver_binary_sha256": sha(&driver_binary)?, "frozen_python_harness_sha256": sha(&repository.join("scripts/benchmark_throughput.py"))?,
        "frozen_python_transport_helper_sha256": sha(&repository.join("scripts/benchmark_transport.py"))?,
        "method_sha256": sha(&repository.join("docs/impl/throughput-20261020/native/method.md"))?,
        "observed_worktree_source_sha256": final_source,
        "pinned_source_manifest_sha256": args.source_manifest.as_ref().map(|path| sha(path)).transpose()?});
    let identity_stable = identities_after["binary_sha256"] == binary_sha
        && identities_after["driver_source_sha256"] == driver_sha
        && identities_after["driver_binary_sha256"] == driver_binary_sha
        && identities_after["frozen_python_harness_sha256"] == python_sha
        && identities_after["frozen_python_transport_helper_sha256"] == helper_sha
        && identities_after["method_sha256"] == method_sha
        && final_source == initial_source
        && identities_after["pinned_source_manifest_sha256"] == json!(pinned_manifest_sha);
    if !identity_stable {
        run_errors.push("identity changed during run; results invalid for qualification".into());
    }
    let audit = credential_scan(&args.output, &credentials)?;
    if audit["audit_passed"] != true {
        run_errors
            .push("unexpected credentials required redaction; see credential-check.json".into());
    }
    write_json(
        &args.output.join("credential-check.json"),
        &audit,
        &credentials,
    )?;
    let complete = run_errors.is_empty()
        && results
            .iter()
            .all(|result| result["phase_completed_without_errors"] == true);
    let all_gates = complete
        && identity_stable
        && results
            .iter()
            .all(|result| result["all_targets_and_correctness_pass"] == true);
    let report = json!({"schema": "cooperators.native-concurrent-throughput-results/v1", "completed": complete,
        "full_eight_profile_qualification": declared_plan["full_eight_profile_qualification"],
        "end_utc": cooperators::id::now_rfc3339(), "identity": environment, "plan": declared_plan,
        "results": results, "identity_stable": identity_stable, "identities_after": identities_after,
        "errors": run_errors, "all_selected_profiles_targets_and_correctness_pass": all_gates,
        "full_qualification_pass": all_gates && declared_plan["full_eight_profile_qualification"] == true});
    write_json(&args.output.join("summary.json"), &report, &credentials)?;
    let mut hashes = BTreeMap::new();
    for path in regular_files(&args.output)? {
        hashes.insert(
            path.strip_prefix(&args.output)?
                .to_string_lossy()
                .into_owned(),
            sha(&path)?,
        );
    }
    write_json(&args.output.join("sha256.json"), &hashes, &credentials)?;
    println!(
        "SAVED {}; completed={complete}; all selected profile gates={all_gates}",
        args.output.display()
    );
    if !complete {
        return Err("one or more native throughput profiles failed; all independent profiles were attempted and evidence retained".into());
    }
    Ok(())
}
