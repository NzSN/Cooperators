//! Actual replay baseline, including a fresh `coop daemon run` process.
//!
//! Build with `cargo build --offline --locked --release --example replay_baseline`.
//! Run with `--help` for the disposable fixture and evidence paths. This does
//! not change broker code or weaken its per-message durability contract.

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use clap::Parser;
use cooperators::envelope::{Body, Envelope, ENVELOPE_VERSION};
use cooperators::registry::Registry;
use cooperators::storage::{hash_record, LogFile, GENESIS_HASH};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug, Parser)]
#[command(about = "Measure actual durable log replay and production daemon startup")]
struct Args {
    /// New, disposable directory. Refused if it already exists.
    #[arg(long)]
    fixture_root: PathBuf,
    /// Directory for JSONL measurements and child daemon stderr.
    #[arg(long)]
    output_dir: PathBuf,
    /// Release `coop` binary used for actual process startup.
    #[arg(long)]
    coop_binary: PathBuf,
    #[arg(long, default_value_t = 1_000_000)]
    records: u64,
    #[arg(long, default_value_t = 3)]
    samples: usize,
    /// Delete only this invocation's fixture after all checks succeed.
    #[arg(long)]
    remove_fixture_on_success: bool,
}

struct Evidence {
    output: File,
}

impl Evidence {
    fn record(&mut self, value: Value) -> Result<()> {
        let line = serde_json::to_string(&value)?;
        println!("{line}");
        writeln!(self.output, "{line}")?;
        self.output.flush()?;
        Ok(())
    }
}

/// Always reap a benchmark daemon, including when validation fails.
struct DaemonProcess(Child);

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.records == 0 || args.samples == 0 {
        return Err("records and samples must both be positive".into());
    }
    if cfg!(debug_assertions) {
        return Err("build this benchmark with --release".into());
    }
    if args.fixture_root.exists() {
        return Err("fixture root already exists; supply a new disposable path".into());
    }
    let coop_binary = args.coop_binary.canonicalize()?;
    let harness_binary = std::env::current_exe()?.canonicalize()?;
    fs::create_dir_all(&args.output_dir)?;
    let mut evidence = Evidence {
        output: OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(args.output_dir.join("results.jsonl"))?,
    };
    evidence.record(json!({
        "event": "environment",
        "date_utc": cooperators::id::now_rfc3339(),
        "profile": "release",
        "crate_version": env!("CARGO_PKG_VERSION"),
        "features": if cfg!(feature = "mbt-injection") { "mbt-injection" } else { "default" },
        "logical_cpus": std::thread::available_parallelism()?.get(),
        "kernel": fs::read_to_string("/proc/sys/kernel/osrelease")?.trim(),
        "cpu": cpu_model(),
        "meminfo_before": fs::read_to_string("/proc/meminfo")?,
        "fixture_root": args.fixture_root,
        "records": args.records,
        "samples_per_scope": args.samples,
        "cache_condition": "warm filesystem cache; no cache eviction requested",
        "coop_binary": coop_binary,
        "coop_binary_sha256": file_sha256(&coop_binary)?,
        "harness_binary": harness_binary,
        "harness_binary_sha256": file_sha256(&harness_binary)?,
    }))?;

    // Set up real registry entries so the process benchmark can authenticate a
    // final-message lookup against the replayed in-memory inbox. Tokens never
    // enter the evidence files or command-line arguments.
    fs::create_dir(&args.fixture_root)?;
    cooperators::paths::ensure_layout(&args.fixture_root)?;
    let mut registry = Registry::load(&args.fixture_root)?;
    let recipient = registry.register(
        "perf-recipient".into(),
        Some("replay-baseline".into()),
        None,
        None,
    )?;
    registry.register(
        "perf-sender".into(),
        Some("replay-baseline".into()),
        None,
        None,
    )?;
    drop(registry);
    let inbox = args
        .fixture_root
        .join("agents")
        .join(&recipient.agent_id)
        .join("inbox.jsonl");
    fs::create_dir_all(inbox.parent().ok_or("inbox has no parent")?)?;
    let generation_started = Instant::now();
    let expected_head = write_fixture(&inbox, args.records)?;
    let generation_seconds = generation_started.elapsed().as_secs_f64();
    let fixture_bytes = fs::metadata(&inbox)?.len();
    let fixture_sha256 = file_sha256(&inbox)?;
    evidence.record(json!({
        "event": "fixture",
        "path": inbox,
        "records": args.records,
        "bytes": fixture_bytes,
        "sha256": fixture_sha256,
        "last_record_chain_hash": expected_head,
        "generation_and_sync_seconds": generation_seconds,
        "durability_before_timing": "BufWriter flush; file sync_all; parent directory sync_all",
        "record_schema": "Envelope v1; unique deterministic ULID; seq 1..N; validated hash chain",
        "payload": "text/plain: replay record {zero_based_index}",
        "timestamp": "2026-10-20T00:00:00.000Z",
        "idempotency_keys": false,
        "expiry_sidecars": false,
    }))?;

    for sample in 1..=args.samples {
        let started = Instant::now();
        let (log, report) = LogFile::<Envelope>::open(&inbox)?;
        let elapsed = started.elapsed();
        // All these checks and all destruction happen after stopping the timer.
        assert_eq!(report.records.len() as u64, args.records);
        assert_eq!(report.seq_head, args.records);
        assert_eq!(report.last_hash, expected_head);
        assert_eq!(report.truncated_bytes, 0);
        assert_eq!(report.last_valid_offset, fixture_bytes);
        assert!(!report.trailing_newline_missing);
        assert_eq!(log.record_count(), args.records);
        assert_eq!(log.seq_head(), args.records);
        assert_eq!(log.last_hash(), expected_head);
        assert_eq!(report.records[0].seq, 1);
        assert_eq!(report.records[0].prev_hash, GENESIS_HASH);
        assert_eq!(report.records.last().unwrap().seq, args.records);
        assert_eq!(hash_record(report.records.last().unwrap())?, expected_head);
        assert!(report
            .records
            .iter()
            .enumerate()
            .all(|(index, record)| record.seq == index as u64 + 1));
        evidence.record(json!({
            "event": "sample",
            "scope": "LogFile<Envelope>::open",
            "sample": sample,
            "seconds": elapsed.as_secs_f64(),
            "records": report.records.len(),
            "seq_head": report.seq_head,
            "last_hash": report.last_hash,
            "truncated_bytes": report.truncated_bytes,
            "last_valid_offset": report.last_valid_offset,
            "process_memory": process_memory(std::process::id()),
            "target_seconds_exclusive": 10.0,
            "target_met": elapsed < Duration::from_secs(10),
            "timing_includes": "open; read; JSON parse; sequence and hash chain verification; recovery sync_all; seek",
            "timing_excludes": "fixture preparation; explicit result assertions; record destruction; daemon runtime reconstruction",
        }))?;
        drop(report);
        drop(log);
    }

    let socket = args.fixture_root.join("run/daemon.sock");
    for sample in 1..=args.samples {
        // A killed earlier process leaves a socket pathname; remove only that
        // pathname owned by this invocation before measuring a fresh start.
        match fs::remove_file(&socket) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let stderr_path = args.output_dir.join(format!("daemon-{sample}.stderr.log"));
        let stderr = File::create(&stderr_path)?;
        let started = Instant::now();
        let mut daemon = DaemonProcess(
            Command::new(&coop_binary)
                .arg("--root")
                .arg(&args.fixture_root)
                .args(["daemon", "run", "--http-addr", "off"])
                .env_remove("COOPERATORS_HOME")
                .env_remove("COOP_SOCKET")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(stderr)
                .spawn()?,
        );
        let status = loop {
            if let Some(exit) = daemon.0.try_wait()? {
                return Err(format!(
                    "daemon exited before ready ({exit}); inspect {}",
                    stderr_path.display()
                )
                .into());
            }
            if started.elapsed() > Duration::from_secs(120) {
                return Err("daemon readiness timed out after 120 seconds".into());
            }
            if let Ok(status) = cooperators::client::call_at(
                &socket,
                "daemon.status",
                json!({}),
                Duration::from_secs(1),
            )
            .await
            {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let elapsed = started.elapsed();
        let relative_inbox = format!("agents/{}/inbox.jsonl", recipient.agent_id);
        assert_eq!(status["seq_heads"][&relative_inbox]["seq"], args.records);
        assert_eq!(status["seq_heads"][&relative_inbox]["bytes"], fixture_bytes);
        // Confirm the last message is available through the production runtime,
        // beyond the seq-head metadata exposed by status. This is untimed.
        let batch = cooperators::client::call_at(
            &socket,
            "wait_message",
            json!({
                "token": recipient.token,
                "cursor": (args.records - 1).to_string(),
                "timeout_s": 1,
            }),
            Duration::from_secs(5),
        )
        .await?;
        assert_eq!(batch["messages"].as_array().unwrap().len(), 1);
        assert_eq!(batch["messages"][0]["seq"], args.records);
        assert_eq!(
            batch["messages"][0]["body"]["text"],
            format!("replay record {}", args.records - 1)
        );
        evidence.record(json!({
            "event": "sample",
            "scope": "fresh coop daemon process to successful UDS daemon.status",
            "sample": sample,
            "seconds": elapsed.as_secs_f64(),
            "pid": daemon.0.id(),
            "records": args.records,
            "seq_head": status["seq_heads"][&relative_inbox]["seq"],
            "fixture_bytes": status["seq_heads"][&relative_inbox]["bytes"],
            "last_message_runtime_lookup_verified": true,
            "process_memory": process_memory(daemon.0.id()),
            "stderr": stderr_path,
            "target_seconds_exclusive": 10.0,
            "target_met": elapsed < Duration::from_secs(10),
            "timing_includes": "process spawn; production owned state construction; root lock; registry and all log replay; inbox backlog reconstruction; presence initialization; socket bind; first successful UDS status round trip; polling jitter up to 5 ms",
            "timing_excludes": "fixture preparation; HTTP listener (explicitly disabled); final-message runtime validation; shutdown",
        }))?;
        drop(daemon);
    }

    assert_eq!(fs::metadata(&inbox)?.len(), fixture_bytes);
    assert_eq!(file_sha256(&inbox)?, fixture_sha256);
    evidence.record(json!({
        "event": "completed",
        "date_utc": cooperators::id::now_rfc3339(),
        "fixture_unchanged_after_all_samples": true,
        "fixture_removed": args.remove_fixture_on_success,
        "samples": args.samples * 2,
    }))?;
    evidence.output.sync_all()?;
    if args.remove_fixture_on_success {
        fs::remove_dir_all(&args.fixture_root)?;
    }
    Ok(())
}

fn write_fixture(path: &Path, records: u64) -> Result<String> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    let mut previous = GENESIS_HASH.to_string();
    for index in 0..records {
        let mut random = [0u8; 10];
        random[2..].copy_from_slice(&index.to_be_bytes());
        let envelope = Envelope {
            v: ENVELOPE_VERSION,
            id: cooperators::id::ulid_from_parts(1_792_454_400_000 + index, random),
            seq: index + 1,
            ts: "2026-10-20T00:00:00.000Z".to_string(),
            from: "perf-sender".to_string(),
            to: "perf-recipient".to_string(),
            kind: "perf".to_string(),
            reply_to: None,
            idempotency_key: None,
            body: Body::new("text/plain", format!("replay record {index}")),
            prev_hash: previous,
        };
        envelope.validate()?;
        previous = hash_record(&envelope)?;
        serde_json::to_writer(&mut writer, &envelope)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    File::open(path.parent().ok_or("fixture has no parent")?)?.sync_all()?;
    Ok(previous)
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 1024 * 64];
    loop {
        let length = file.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        hasher.update(&buffer[..length]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split_once(':'))
                .map(|(_, value)| value.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn process_memory(pid: u32) -> String {
    fs::read_to_string(format!("/proc/{pid}/status"))
        .map(|text| {
            text.lines()
                .filter(|line| line.starts_with("VmRSS:") || line.starts_with("VmHWM:"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}
