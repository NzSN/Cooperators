//! Generated adapter ports backed by actual broker operations and files.
//! Subprocess fixtures execute one real operation and expose only barrier
//! markers; generated bindings remain the action dispatcher and trace oracle.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

use cooperators::envelope::Body;
use cooperators::faults::{self, ClockHandle, FaultPoint, PointFault, SyncOutcome};
use cooperators::messaging::SendRequest;
use cooperators::registry::AgentIdentity;
use cooperators::server::{DaemonConfig, DaemonState};
use cooperators::storage::{ChainRecord, LogFile};
use mirrorrust::BindingError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::fault_support::{config, error, run, value};

#[path = "../fault_interfaces/deadletter/generated/MBFaultDeadLetterMirror.generated.rs"]
#[allow(dead_code)]
mod dead;
#[path = "../fault_interfaces/document/generated/MBFaultDocumentMirror.generated.rs"]
#[allow(dead_code)]
mod doc;
#[path = "../fault_interfaces/inbox/generated/MBFaultInboxMirror.generated.rs"]
#[allow(dead_code)]
mod inbox;
#[path = "../fault_interfaces/status/generated/MBFaultStatusMirror.generated.rs"]
#[allow(dead_code)]
mod status;

const QUEUE: &str = "fault-queue";
const DOCUMENT: &str = "fault-document";
const STATUS_LOG: &str = "channels/fault-status.jsonl";
const UNIT: usize = 32 * 1024;
const WAIT: Duration = Duration::from_secs(10);

fn has_code(e: &cooperators::error::BrokerError, code: cooperators::error::ErrorCode) -> bool {
    matches!(e, cooperators::error::BrokerError::Application(actual, _) | cooperators::error::BrokerError::ApplicationData(actual, _, _) if *actual == code)
}

fn rows(path: &Path) -> Result<Vec<Value>, BindingError> {
    match fs::read(path) {
        Ok(bytes) => bytes
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).map_err(error))
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(error(e)),
    }
}

struct RuntimeSut {
    root: tempfile::TempDir,
    rt: tokio::runtime::Runtime,
    state: Option<DaemonState>,
    identity: Option<AgentIdentity>,
    inbox_id: String,
    clock: Option<ClockHandle>,
    child: Option<Child>,
    result: String,
    cursor: u64,
    messages: Vec<u64>,
    reported: i64,
    up: bool,
    evidence: Option<PathBuf>,
    observations: usize,
}

impl Drop for RuntimeSut {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl RuntimeSut {
    fn new(profile: &str, trace: &Path) -> Result<Self, BindingError> {
        let evidence = std::env::var_os("COOP_MBT_EVIDENCE_DIR")
            .map(PathBuf::from)
            .map(|base| base.join(profile).join(trace.file_stem().unwrap()));
        if let Some(path) = &evidence {
            fs::create_dir_all(path.parent().unwrap()).map_err(error)?;
            fs::create_dir(path).map_err(error)?;
        }
        Ok(Self {
            root: tempfile::tempdir().map_err(error)?,
            rt: tokio::runtime::Runtime::new().map_err(error)?,
            state: None,
            identity: None,
            inbox_id: String::new(),
            clock: None,
            child: None,
            result: "none".into(),
            cursor: 0,
            messages: vec![],
            reported: -1,
            up: true,
            evidence,
            observations: 0,
        })
    }

    fn record(&mut self, observation: Value) -> Result<Value, BindingError> {
        if let Some(base) = &self.evidence {
            fn copy(source: &Path, destination: &Path) -> std::io::Result<()> {
                fs::create_dir_all(destination)?;
                for item in fs::read_dir(source)? {
                    let item = item?;
                    if item.file_type()?.is_dir() {
                        copy(&item.path(), &destination.join(item.file_name()))?;
                    } else if item.file_type()?.is_file() {
                        fs::copy(item.path(), destination.join(item.file_name()))?;
                    }
                }
                Ok(())
            }
            let step = base.join(format!("step-{:02}", self.observations));
            copy(self.root.path(), &step.join("root")).map_err(error)?;
            fs::write(
                step.join("observation.json"),
                serde_json::to_vec_pretty(&json!({"observation":observation,"result":self.result}))
                    .map_err(error)?,
            )
            .map_err(error)?;
        }
        self.observations += 1;
        Ok(observation)
    }

    fn seed(&mut self) -> Result<(), BindingError> {
        let mut cfg = DaemonConfig::new(self.root.path());
        cfg.max_inbox_messages = 2;
        cfg.wait_batch_limit = 1;
        let state = DaemonState::with_config(cfg).map_err(error)?;
        let reg = self
            .rt
            .block_on(state.register_agent(
                "fault-agent".into(),
                Some("codex".into()),
                Some(vec![]),
                None,
            ))
            .map_err(error)?;
        let identity = self
            .rt
            .block_on(state.authenticate(&reg.token))
            .map_err(error)?;
        self.inbox_id = identity.agent_id.clone();
        fs::create_dir_all(self.root.path().join("fixture")).map_err(error)?;
        fs::write(
            self.root.path().join("fixture/config.json"),
            serde_json::to_vec(&json!({"token":reg.token})).map_err(error)?,
        )
        .map_err(error)?;
        self.identity = Some(identity);
        self.state = Some(state);
        Ok(())
    }

    fn marker(&self, name: &str) -> PathBuf {
        self.root.path().join("fixture").join(name)
    }

    fn start(&mut self, profile: &str) -> Result<(), BindingError> {
        self.state.take();
        for name in ["before", "after", "done", "release-before", "release-after"] {
            let _ = fs::remove_file(self.marker(name));
        }
        self.child = Some(
            Command::new(std::env::current_exe().map_err(error)?)
                .arg("--runtime-fixture-child")
                .arg(profile)
                .arg(self.root.path())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .map_err(error)?,
        );
        Ok(())
    }

    /// Wait for an actual reached barrier or operation outcome. Never assume
    /// the checkpoint was reached because the model expected it.
    fn wait_marker(&mut self, name: &str) -> Result<bool, BindingError> {
        let deadline = Instant::now() + WAIT;
        loop {
            if self.marker(name).exists() {
                return Ok(true);
            }
            if self.marker("done").exists() {
                return Ok(false);
            }
            if let Some(child) = self.child.as_mut() {
                if let Some(status) = child.try_wait().map_err(error)? {
                    return Err(error(format!("fixture exited {status} before {name}")));
                }
            }
            if Instant::now() >= deadline {
                return Err(error(format!("fixture timeout: {name}")));
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn release(&self, point: &str, success: bool) -> Result<(), BindingError> {
        fs::write(self.marker(point), if success { "ok" } else { "fail" }).map_err(error)
    }

    fn finish(&mut self) -> Result<String, BindingError> {
        self.wait_marker("done")?;
        let result = fs::read_to_string(self.marker("done")).map_err(error)?;
        if let Some(mut child) = self.child.take() {
            if !child.wait().map_err(error)?.success() {
                return Err(error("fixture operation process failed"));
            }
        }
        Ok(result)
    }

    fn crash(&mut self) -> Result<(), BindingError> {
        if let Some(mut child) = self.child.take() {
            child.kill().map_err(error)?;
            child.wait().map_err(error)?;
        }
        self.state.take();
        self.up = false;
        self.result = "stopped".into();
        Ok(())
    }

    fn recover(&mut self) -> Result<(), BindingError> {
        self.state =
            Some(DaemonState::with_config(DaemonConfig::new(self.root.path())).map_err(error)?);
        self.up = true;
        self.result = "ok".into();
        Ok(())
    }
}

impl inbox::MBFaultInboxPort for RuntimeSut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        self.clock = Some(faults::set_clock(self.root.path(), 1_000).map_err(error)?);
        self.seed()
    }
    fn expire_inbox(&mut self) -> Result<(), BindingError> {
        self.clock.as_ref().unwrap().advance(10);
        self.result = "ok".into();
        Ok(())
    }
    fn inbox_ack(&mut self, input: inbox::InboxAckInput) -> Result<(), BindingError> {
        self.rt
            .block_on(
                self.state
                    .as_ref()
                    .unwrap()
                    .ack(&self.inbox_id, &input.value.to_string()),
            )
            .map_err(error)?;
        self.result = "ok".into();
        Ok(())
    }
    fn inbox_send(&mut self) -> Result<(), BindingError> {
        let send = self.rt.block_on(self.state.as_ref().unwrap().send_message(
            self.identity.as_ref().unwrap(),
            SendRequest {
                to: "fault-agent".into(),
                kind: "fault-message".into(),
                body: Body::new("text/plain", "fixture"),
                reply_to: None,
                idempotency_key: None,
                ttl: Some(Duration::from_millis(10)),
            },
        ));
        self.result = match send {
            Ok(_) => "ok",
            Err(e) if has_code(&e, cooperators::error::ErrorCode::ResourceExhausted) => "full",
            Err(e) => return Err(error(e)),
        }
        .into();
        Ok(())
    }
    fn inbox_wait(&mut self) -> Result<(), BindingError> {
        let batch = self
            .rt
            .block_on(self.state.as_ref().unwrap().wait_message(
                &self.inbox_id,
                None,
                Duration::ZERO,
            ))
            .map_err(error)?;
        self.cursor = batch.next_cursor.parse().map_err(error)?;
        self.messages = batch.messages.iter().map(|m| m.seq).collect();
        self.result = "ok".into();
        Ok(())
    }
    fn observe(&mut self) -> Result<inbox::MBFaultInboxObservation, BindingError> {
        use inbox::NativeCodec;
        let state = self.state.as_ref().unwrap();
        let batch = self
            .rt
            .block_on(state.wait_message(&self.inbox_id, Some("0"), Duration::ZERO))
            .map_err(error)?;
        let pending = self
            .rt
            .block_on(state.debug_inbox_backlog(&self.inbox_id))
            .map_err(error)?
            .0;
        let path = self
            .root
            .path()
            .join("agents")
            .join(&self.inbox_id)
            .join("inbox.jsonl");
        let head = rows(&path)?
            .last()
            .and_then(|r| r["seq"].as_u64())
            .unwrap_or(0);
        let observed = self.record(json!({
            "head":head,"ack":batch.acked_cursor.parse::<u64>().map_err(error)?,
            "cursor":self.cursor,"messages":self.messages,"pending":pending,
        }))?;
        Ok(inbox::MBFaultInboxObservation {
            observation: inbox::MiTypeO0::decode(&value(observed)?)?,
            result: self.result.clone(),
        })
    }
}

impl doc::MBFaultDocumentPort for RuntimeSut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        self.seed()
    }
    fn begin_publish(&mut self) -> Result<(), BindingError> {
        self.start("document")?;
        self.result = if self.wait_marker("before")? {
            "pending".into()
        } else {
            self.finish()?
        };
        Ok(())
    }
    fn commit_publish(&mut self) -> Result<(), BindingError> {
        self.release("release-before", true)?;
        self.result = self.finish()?;
        Ok(())
    }
    fn abort_publish(&mut self) -> Result<(), BindingError> {
        self.release("release-before", false)?;
        self.result = self.finish()?;
        Ok(())
    }
    fn crash_doc(&mut self) -> Result<(), BindingError> {
        self.crash()
    }
    fn recover_doc(&mut self) -> Result<(), BindingError> {
        self.recover()
    }
    fn read_doc(&mut self) -> Result<(), BindingError> {
        if self.state.is_none() {
            self.state =
                Some(DaemonState::with_config(DaemonConfig::new(self.root.path())).map_err(error)?);
        }
        let read = self
            .rt
            .block_on(self.state.as_ref().unwrap().read_doc(DOCUMENT, None));
        self.result = match read {
            Ok(_) => "ok",
            Err(e) if has_code(&e, cooperators::error::ErrorCode::NotFound) => "not-found",
            Err(e) => return Err(error(e)),
        }
        .into();
        Ok(())
    }
    fn observe(&mut self) -> Result<doc::MBFaultDocumentObservation, BindingError> {
        use doc::NativeCodec;
        let path = self
            .root
            .path()
            .join("docs")
            .join(format!("{DOCUMENT}.jsonl"));
        let version = rows(&path)?
            .iter()
            .filter_map(|r| r["version"].as_u64())
            .max()
            .unwrap_or(0);
        let observed = self.record(json!({"file":path.exists(),"version":version,"up":self.up}))?;
        Ok(doc::MBFaultDocumentObservation {
            observation: doc::MiTypeO0::decode(&value(observed)?)?,
            result: self.result.clone(),
        })
    }
}

impl dead::MBFaultDeadLetterPort for RuntimeSut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        self.seed()?;
        let state = self.state.as_ref().unwrap();
        self.rt.block_on(state.create_queue(QUEUE)).map_err(error)?;
        self.rt
            .block_on(state.enqueue(QUEUE, json!({"fixture":1}), Some(1)))
            .map_err(error)?;
        let claim = self
            .rt
            .block_on(state.claim_task(self.identity.as_ref().unwrap(), QUEUE, 3600))
            .map_err(error)?;
        let path = self.marker("config.json");
        let mut cfg: Value =
            serde_json::from_slice(&fs::read(&path).map_err(error)?).map_err(error)?;
        cfg["lease"] = json!(claim.lease_id);
        cfg["fence"] = json!(claim.fence);
        fs::write(path, serde_json::to_vec(&cfg).map_err(error)?).map_err(error)?;
        Ok(())
    }
    fn mark_dead(&mut self) -> Result<(), BindingError> {
        self.start("deadletter")?;
        self.result = if self.wait_marker("before")? {
            "pending".into()
        } else {
            self.finish()?
        };
        Ok(())
    }
    fn fail_projection(&mut self) -> Result<(), BindingError> {
        if self.child.is_none() {
            // A trace may inject projection failure after a crash/recovery,
            // when the original fail_task process no longer exists. Resume
            // the real sweeper and pause its reconciliation append instead.
            self.start("projection-failure")?;
            if !self.wait_marker("before")? {
                return Err(error(format!(
                    "reconciliation did not reach the projection failure hook: {}",
                    self.finish()?
                )));
            }
        }
        self.release("release-before", false)?;
        self.result = self.finish()?;
        Ok(())
    }
    fn project_dead(&mut self) -> Result<(), BindingError> {
        let path = self.root.path().join("dead").join(format!("{QUEUE}.jsonl"));
        let before = rows(&path)?.len();
        if self.child.is_some() {
            self.release("release-before", true)?;
        } else {
            self.start("projection")?;
        }
        let paused = self.wait_marker("after")?;
        let after = rows(&path)?.len();
        self.result = if after > before {
            "written"
        } else if after > 0 {
            "already-projected"
        } else {
            "no-work"
        }
        .into();
        if !paused {
            let _ = self.finish()?;
        }
        Ok(())
    }
    fn finish_projection(&mut self) -> Result<(), BindingError> {
        if self.child.is_some() {
            self.release("release-after", true)?;
            let result = self.finish()?;
            if result != "ok" {
                return Err(error(result));
            }
        }
        self.result = "ok".into();
        Ok(())
    }
    fn crash_dead(&mut self) -> Result<(), BindingError> {
        self.crash()
    }
    fn recover_dead(&mut self) -> Result<(), BindingError> {
        self.recover()
    }
    fn observe(&mut self) -> Result<dead::MBFaultDeadLetterObservation, BindingError> {
        use dead::NativeCodec;
        let terminal = rows(
            &self
                .root
                .path()
                .join("queues")
                .join(format!("{QUEUE}.jsonl")),
        )?
        .iter()
        .any(|r| r["kind"] == "task_dead");
        // Each real projection refers to this fixture's sole task, normalized
        // to model id 1. Keep duplicates rather than hiding them in a set.
        let records: Vec<u64> =
            rows(&self.root.path().join("dead").join(format!("{QUEUE}.jsonl")))?
                .iter()
                .map(|_| 1)
                .collect();
        let observed =
            self.record(json!({"terminal":terminal,"deadRecords":records,"up":self.up}))?;
        Ok(dead::MBFaultDeadLetterObservation {
            observation: dead::MiTypeO0::decode(&value(observed)?)?,
            result: self.result.clone(),
        })
    }
}

#[derive(Serialize, Deserialize)]
struct StatusRecord {
    seq: u64,
    prev_hash: String,
    padding: String,
}
impl ChainRecord for StatusRecord {
    fn seq(&self) -> u64 {
        self.seq
    }
    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }
    fn prev_hash(&self) -> &str {
        &self.prev_hash
    }
    fn set_prev_hash(&mut self, hash: String) {
        self.prev_hash = hash;
    }
}
impl status::MBFaultStatusPort for RuntimeSut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        fs::create_dir_all(self.root.path().join("channels")).map_err(error)
    }
    fn append_status(&mut self, input: status::AppendStatusInput) -> Result<(), BindingError> {
        let size: usize = input.value.to_string().parse().map_err(error)?;
        let (mut log, _) =
            LogFile::<StatusRecord>::open(&self.root.path().join(STATUS_LOG)).map_err(error)?;
        let mut record = StatusRecord {
            seq: log.next_seq(),
            prev_hash: log.last_hash().into(),
            padding: String::new(),
        };
        let overhead = serde_json::to_vec(&record).map_err(error)?.len() + 1;
        record.padding = "x".repeat(
            size.checked_mul(UNIT)
                .and_then(|n| n.checked_sub(overhead))
                .ok_or_else(|| error("status unit too small"))?,
        );
        log.append(&mut record).map_err(error)?;
        self.result = "ok".into();
        Ok(())
    }
    fn sample_status(&mut self) -> Result<(), BindingError> {
        let path = self.root.path().join(STATUS_LOG);
        let before = fs::read(&path)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(vec![])
                } else {
                    Err(e)
                }
            })
            .map_err(error)?;
        self.reported = cooperators::status::scan_seq_heads(self.root.path())
            .get(STATUS_LOG)
            .map(|h| h.seq as i64)
            .unwrap_or(-1);
        let after = fs::read(&path)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(vec![])
                } else {
                    Err(e)
                }
            })
            .map_err(error)?;
        if before != after {
            return Err(error("status modified the observed log bytes"));
        }
        self.result = "ok".into();
        Ok(())
    }
    fn observe(&mut self) -> Result<status::MBFaultStatusObservation, BindingError> {
        use status::NativeCodec;
        let records: Vec<Value> = rows(&self.root.path().join(STATUS_LOG))?
            .iter()
            .map(|r| {
                let bytes = serde_json::to_vec(r).unwrap().len() + 1;
                json!({"id":r["seq"],"bytes":bytes/UNIT})
            })
            .collect();
        let observed = self.record(json!({"records":records,"reported":self.reported}))?;
        Ok(status::MBFaultStatusObservation {
            observation: status::MiTypeO0::decode(&value(observed)?)?,
            result: self.result.clone(),
        })
    }
}

/// Fixture markers report synchronization only. No model observation or
/// expected result is supplied to the operation process.
pub fn child_main(profile: &str, root: &Path) -> ExitCode {
    match child(profile, root) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("runtime fixture failed: {e}");
            ExitCode::FAILURE
        }
    }
}
fn child(profile: &str, root: &Path) -> Result<(), BindingError> {
    let rt = tokio::runtime::Runtime::new().map_err(error)?;
    let state = DaemonState::with_config(DaemonConfig::new(root)).map_err(error)?;
    let cfg: Value =
        serde_json::from_slice(&fs::read(root.join("fixture/config.json")).map_err(error)?)
            .map_err(error)?;
    let identity = rt
        .block_on(
            state.authenticate(
                cfg["token"]
                    .as_str()
                    .ok_or_else(|| error("missing token"))?,
            ),
        )
        .map_err(error)?;
    let before = if profile == "document" {
        Some(
            faults::arm_point(
                root.join("docs").join(format!("{DOCUMENT}.jsonl")),
                FaultPoint::AfterDocCreate,
                PointFault::Pause,
            )
            .map_err(error)?,
        )
    } else if matches!(profile, "deadletter" | "projection-failure") {
        Some(
            faults::arm_point(
                root.join("dead").join(format!("{QUEUE}.jsonl")),
                FaultPoint::BeforeDeadProjection,
                PointFault::Pause,
            )
            .map_err(error)?,
        )
    } else {
        None
    };
    let after = if profile != "document" {
        Some(
            faults::arm_point(
                root.join("dead").join(format!("{QUEUE}.jsonl")),
                FaultPoint::AfterDeadProjectionAppend,
                PointFault::Pause,
            )
            .map_err(error)?,
        )
    } else {
        None
    };
    let mode = profile.to_string();
    let op = std::thread::spawn(move || {
        let result = match mode.as_str() {
            "document" => rt
                .block_on(state.publish_doc(
                    &identity,
                    DOCUMENT,
                    "text/plain",
                    Body::new("text/plain", "fixture"),
                    None,
                ))
                .map(|_| ()),
            "deadletter" => rt
                .block_on(state.fail_task(
                    &identity,
                    cfg["lease"].as_str().unwrap(),
                    cfg["fence"].as_u64().unwrap(),
                    "injected fixture failure",
                ))
                .map(|_| ()),
            "projection" | "projection-failure" => {
                rt.block_on(state.sweep_expired_queues()).map(|_| ())
            }
            _ => return Err(error("unknown runtime fixture")),
        };
        Ok::<_, BindingError>(
            match result {
                Ok(()) => "ok",
                Err(e) if has_code(&e, cooperators::error::ErrorCode::Exists) => "exists",
                Err(_) => "io-error",
            }
            .to_string(),
        )
    });
    for (name, handle) in [("before", before), ("after", after)] {
        if let Some(handle) = handle {
            let deadline = Instant::now() + WAIT;
            while handle.observation().stage == faults::FaultStage::Armed && !op.is_finished() {
                if Instant::now() >= deadline {
                    return Err(error("operation did not reach fixture barrier"));
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            if handle.observation().stage == faults::FaultStage::Armed {
                continue;
            }
            fs::write(root.join("fixture").join(name), "reached").map_err(error)?;
            let release = root.join("fixture").join(format!("release-{name}"));
            loop {
                match fs::read_to_string(&release) {
                    Ok(text) => {
                        handle.release(if text == "ok" {
                            SyncOutcome::Succeed
                        } else {
                            SyncOutcome::Fail
                        });
                        break;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(error(e)),
                }
                if Instant::now() >= deadline {
                    return Err(error("fixture barrier not released"));
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
    let result = op
        .join()
        .map_err(|_| error("operation thread panicked"))??;
    let temporary = root.join("fixture/done.tmp");
    fs::write(&temporary, result).map_err(error)?;
    fs::rename(temporary, root.join("fixture/done")).map_err(error)?;
    Ok(())
}

pub fn replay(profile: &str, bin: &str, trace: &Path) -> Result<(), String> {
    let sut = RuntimeSut::new(profile, trace).map_err(|e| e.to_string())?;
    match profile {
        "inbox" => {
            let cfg = config("MBFaultInbox");
            let binding = inbox::bind_m_b_fault_inbox(sut, &cfg).map_err(|e| e.to_string())?;
            run(bin, trace, cfg, binding)
        }
        "deadletter" => {
            let cfg = config("MBFaultDeadLetter");
            let binding = dead::bind_m_b_fault_dead_letter(sut, &cfg).map_err(|e| e.to_string())?;
            run(bin, trace, cfg, binding)
        }
        "document" => {
            let cfg = config("MBFaultDocument");
            let binding = doc::bind_m_b_fault_document(sut, &cfg).map_err(|e| e.to_string())?;
            run(bin, trace, cfg, binding)
        }
        "status" => {
            let cfg = config("MBFaultStatus");
            let binding = status::bind_m_b_fault_status(sut, &cfg).map_err(|e| e.to_string())?;
            run(bin, trace, cfg, binding)
        }
        _ => Err(format!("unsupported runtime fault profile {profile}")),
    }
}
