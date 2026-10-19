//! Trace-driven root ownership checks against real daemon subprocesses.
//! Observations come from process liveness and read-only filesystem reads.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cooperators::envelope::Envelope;
use cooperators::server::{self, DaemonState, ServerOptions};
use cooperators::storage::{hash_record, GENESIS_HASH};
use mirrorrust::{run_client_with_traces, ApalacheConfig, BindingError, FallibleStateComputer};
use num_bigint::BigInt;
use serde_json::json;

#[path = "../writers/generated/MBWritersMirror.generated.rs"]
#[allow(dead_code)]
pub(crate) mod generated;
use generated::{
    MBWritersObservation, MBWritersPort, MirrorMap, MirrorSeq, SendInput, StartInput, StopInput,
    TearTailInput,
};

const WRITERS: [&str; 2] = ["w1", "w2"];
const ROOTS: [&str; 2] = ["r1", "r2"];
const TORN_TAIL: &[u8] = b"{\"unfinished_writer_record\":";
const REFUSED: u8 = 75;
pub const REQUIRED_TRACES: [&str; 5] = [
    "NoCompeteW1.itf.json",
    "NoCompeteW2.itf.json",
    "NoHandoff.itf.json",
    "NoIndependentRoots.itf.json",
    "NoRejectedRecovery.itf.json",
];

fn failure(error: impl std::fmt::Display) -> BindingError {
    BindingError::new("writer_probe_failed", error.to_string())
}

/// Internal subprocess mode: the production daemon entry point, HTTP disabled.
pub fn child_main(root: &Path) -> ExitCode {
    let result = tokio::runtime::Runtime::new().and_then(|rt| {
        rt.block_on(server::run_with_options(
            root,
            ServerOptions { http_addr: None },
        ))
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("writer daemon failed: {error}");
            match error.kind() {
                std::io::ErrorKind::AddrInUse
                | std::io::ErrorKind::AlreadyExists
                | std::io::ErrorKind::WouldBlock => ExitCode::from(REFUSED),
                _ => ExitCode::FAILURE,
            }
        }
    }
}

struct RunningWriter {
    child: Child,
    root: String,
    socket: PathBuf,
}

impl Drop for RunningWriter {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct BusRoot {
    path: PathBuf,
    inbox: PathBuf,
    token: String,
}

pub(crate) struct WriterSut {
    rt: tokio::runtime::Runtime,
    home: PathBuf,
    roots: BTreeMap<String, BusRoot>,
    children: BTreeMap<String, RunningWriter>,
    rejected: BTreeMap<String, bool>,
    rejected_read_only: bool,
    starts: u64,
}

impl WriterSut {
    pub(crate) fn new(home: PathBuf) -> Result<Self, BindingError> {
        Ok(Self {
            rt: tokio::runtime::Runtime::new().map_err(failure)?,
            home,
            roots: BTreeMap::new(),
            children: BTreeMap::new(),
            rejected: WRITERS
                .into_iter()
                .map(|w| (w.to_string(), false))
                .collect(),
            rejected_read_only: true,
            starts: 0,
        })
    }
}

/// Includes all durable regular files, excluding the run/ socket directory.
/// Never opens LogFile: its recovery behavior would mutate the observation.
fn durable_snapshot(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>, BindingError> {
    fn walk(
        base: &Path,
        dir: &Path,
        files: &mut BTreeMap<PathBuf, Vec<u8>>,
    ) -> std::io::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path == base.join("run") {
                continue;
            }
            if entry.file_type()?.is_dir() {
                walk(base, &path, files)?;
            } else if entry.file_type()?.is_file() {
                files.insert(
                    path.strip_prefix(base).unwrap().to_path_buf(),
                    fs::read(path)?,
                );
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    walk(root, root, &mut files).map_err(failure)?;
    Ok(files)
}

struct ObservedLog {
    records: Vec<String>,
    seqs: Vec<BigInt>,
    valid: bool,
    torn: bool,
}

fn read_inbox(path: &Path) -> Result<ObservedLog, BindingError> {
    let bytes = fs::read(path).map_err(failure)?;
    let torn = bytes.ends_with(TORN_TAIL);
    let complete = if torn {
        &bytes[..bytes.len() - TORN_TAIL.len()]
    } else {
        &bytes
    };
    let mut records = Vec::new();
    let mut seqs = Vec::new();
    let mut valid = true;
    let mut previous = GENESIS_HASH.to_string();
    for line in complete
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: Envelope = serde_json::from_slice(line).map_err(failure)?;
        valid &= record.seq == records.len() as u64 + 1 && record.prev_hash == previous;
        valid &= record.validate().is_ok();
        previous = hash_record(&record).map_err(failure)?;
        seqs.push(BigInt::from(record.seq));
        records.push(if record.kind == "recovery" {
            "recovery".into()
        } else {
            record.body.text
        });
    }
    Ok(ObservedLog {
        records,
        seqs,
        valid,
        torn,
    })
}

impl MBWritersPort for WriterSut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        for name in ROOTS {
            let path = self.home.join(name);
            // Bootstrap before either writer starts; drop all bootstrap handles.
            let state =
                DaemonState::with_config(server::DaemonConfig::new(&path)).map_err(failure)?;
            let sender = self
                .rt
                .block_on(state.register_agent("sender".into(), None, None, None))
                .map_err(failure)?;
            let receiver = self
                .rt
                .block_on(state.register_agent("receiver".into(), None, None, None))
                .map_err(failure)?;
            let inbox = path
                .join("agents")
                .join(receiver.agent_id)
                .join("inbox.jsonl");
            self.roots.insert(
                name.into(),
                BusRoot {
                    path,
                    inbox,
                    token: sender.token,
                },
            );
        }
        Ok(())
    }

    fn start(&mut self, input: StartInput) -> Result<(), BindingError> {
        if !WRITERS.contains(&input.writer.as_str()) || self.children.contains_key(&input.writer) {
            return Err(failure("unknown or already running writer"));
        }
        let root = self
            .roots
            .get(&input.root)
            .ok_or_else(|| failure("unknown root"))?;
        let socket = root.path.join("run").join(match input.socket.as_str() {
            "same" => "daemon.sock".to_string(),
            "different" => format!("{}.sock", input.writer),
            _ => return Err(failure("unknown socket mode")),
        });
        let before = durable_snapshot(&root.path)?;
        self.starts += 1;
        let log = self
            .home
            .join(format!("{}-{}.stderr.log", input.writer, self.starts));
        let child = Command::new(std::env::current_exe().map_err(failure)?)
            .arg("--writer-child")
            .arg(&root.path)
            .env("COOP_SOCKET", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log).map_err(failure)?)
            .spawn()
            .map_err(failure)?;
        let mut writer = RunningWriter {
            child,
            root: input.root,
            socket,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = writer.child.try_wait().map_err(failure)? {
                let stderr = fs::read_to_string(&log).map_err(failure)?;
                if status.code() != Some(i32::from(REFUSED)) {
                    return Err(failure(format!("startup exited {status}: {stderr}")));
                }
                self.rejected.insert(input.writer, true);
                self.rejected_read_only &= before == durable_snapshot(&root.path)?;
                return Ok(());
            }
            let response = self.rt.block_on(cooperators::client::call_at(
                &writer.socket,
                "daemon.status",
                json!({}),
                Duration::from_millis(100),
            ));
            // Checking the PID avoids mistaking the first daemon's shared
            // socket response for readiness of a rejected competing child.
            if response
                .is_ok_and(|value| value["pid"].as_u64() == Some(u64::from(writer.child.id())))
            {
                self.rejected.insert(input.writer.clone(), false);
                self.children.insert(input.writer, writer);
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(failure(format!(
                    "startup timed out; stderr: {}",
                    fs::read_to_string(log).unwrap_or_default()
                )));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn stop(&mut self, input: StopInput) -> Result<(), BindingError> {
        let writer = self
            .children
            .remove(&input.writer)
            .ok_or_else(|| failure("writer is not running"))?;
        drop(writer); // SIGKILL + wait; the next owner must handle a stale socket.
        Ok(())
    }

    fn send(&mut self, input: SendInput) -> Result<(), BindingError> {
        let writer = self
            .children
            .get(&input.writer)
            .ok_or_else(|| failure("writer is not running"))?;
        let root = &self.roots[&writer.root];
        self.rt
            .block_on(cooperators::client::call_at(
                &writer.socket,
                "send_message",
                json!({
                    "token": root.token, "to": "receiver", "kind": "writers-test",
                    "text": input.writer,
                }),
                Duration::from_secs(2),
            ))
            .map_err(failure)?;
        Ok(())
    }

    fn tear_tail(&mut self, input: TearTailInput) -> Result<(), BindingError> {
        let root = self
            .roots
            .get(&input.root)
            .ok_or_else(|| failure("unknown root"))?;
        let mut file = OpenOptions::new()
            .append(true)
            .open(&root.inbox)
            .map_err(failure)?;
        file.write_all(TORN_TAIL).map_err(failure)?;
        file.sync_all().map_err(failure)
    }

    fn observe(&mut self) -> Result<MBWritersObservation, BindingError> {
        let mut writers: BTreeMap<_, _> = WRITERS
            .into_iter()
            .map(|w| (w.to_string(), "-".to_string()))
            .collect();
        for (name, writer) in &mut self.children {
            if let Some(status) = writer.child.try_wait().map_err(failure)? {
                return Err(failure(format!(
                    "running writer {name} exited unexpectedly: {status}"
                )));
            }
            writers.insert(name.clone(), writer.root.clone());
        }
        let mut records = BTreeMap::new();
        let mut seqs = BTreeMap::new();
        let mut valid = BTreeMap::new();
        let mut torn = BTreeMap::new();
        for (name, root) in &self.roots {
            let observed = read_inbox(&root.inbox)?;
            records.insert(name.clone(), MirrorSeq(observed.records));
            seqs.insert(name.clone(), MirrorSeq(observed.seqs));
            valid.insert(name.clone(), observed.valid);
            torn.insert(name.clone(), observed.torn);
        }
        Ok(MBWritersObservation {
            writers: MirrorMap(writers),
            rejected: MirrorMap(self.rejected.clone()),
            records: MirrorMap(records),
            seqs: MirrorMap(seqs),
            valid: MirrorMap(valid),
            torn: MirrorMap(torn),
            rejected_read_only: self.rejected_read_only,
        })
    }
}

pub fn replay(bin: &str, path: &Path) -> Result<(), String> {
    let fixture = tempfile::Builder::new()
        .prefix("coop-writers-mbt-")
        .tempdir()
        .map_err(|e| e.to_string())?;
    let cfg = ApalacheConfig {
        spec_path: "MBWriters.tla".into(),
        init_predicate: None,
        next_predicate: None,
        const_init: Some("CInit".into()),
        invariant: "RootWriterExclusive".into(),
        length_bound: 6,
        param_vars: Some("parameters".into()),
    };
    let sut = WriterSut::new(fixture.path().to_path_buf()).map_err(|e| format!("{e:?}"))?;
    let mut binding = generated::bind_m_b_writers(sut, &cfg).map_err(|e| format!("{e:?}"))?;
    let error = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&error);
    let compute =
        move |action: &str, params: &mirrorrust::State, prev: &mirrorrust::State| match binding
            .compute(action, params, prev)
        {
            Ok(state) => state,
            Err(e) => {
                *captured.lock().unwrap() = Some(format!("{e:?}"));
                mirrorrust::State::new()
            }
        };
    let result =
        run_client_with_traces(bin, cfg, vec![path.to_string_lossy().into_owned()], compute);
    let adapter_error = error.lock().unwrap().take();
    if result.is_err() || adapter_error.is_some() {
        let evidence = fixture.keep();
        Err(format!(
            "{}{}; evidence: {}",
            result.err().map_or_else(String::new, |e| e.to_string()),
            adapter_error.map_or_else(String::new, |e| format!("; adapter: {e}")),
            evidence.display()
        ))
    } else {
        Ok(())
    }
}
