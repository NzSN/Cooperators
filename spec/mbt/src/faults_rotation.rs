//! Segmented real-storage replay through the compiler-generated rotation port.
//! One model byte unit is exactly 1024 encoded JSONL bytes, including newline.
//! The production storage threshold is 2048 bytes for these isolated fixtures.
//! Actions use actual subprocesses and barriers inside LogFile::append; records
//! and segment membership are read from disk, independently of the model state.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use cooperators::faults::{arm_point, FaultPoint, FaultStage, PointFault, SyncOutcome};
use cooperators::storage::{segment_paths, ChainRecord, LogFile};
use mirrorrust::BindingError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::fault_support::{self, error};
#[path = "../fault_interfaces/rotation/generated/MBFaultRotationMirror.generated.rs"]
#[allow(dead_code)]
mod generated;
use generated::{
    BeginRotatingAppendInput, MBFaultRotationObservation, MBFaultRotationPort, NativeCodec,
    ReadCursorInput,
};

const UNIT: usize = 1024;
const THRESHOLD: u64 = 2 * UNIT as u64;
const LOG: &str = "rotation.jsonl";

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    seq: u64,
    prev_hash: String,
    body: String,
}
impl ChainRecord for Record {
    fn seq(&self) -> u64 {
        self.seq
    }
    fn set_seq(&mut self, value: u64) {
        self.seq = value;
    }
    fn prev_hash(&self) -> &str {
        &self.prev_hash
    }
    fn set_prev_hash(&mut self, value: String) {
        self.prev_hash = value;
    }
}

/// Internal subprocess entrypoint. Barriers signal actual reached boundaries.
pub fn child(root: &Path, units: u64) -> Result<(), String> {
    let path = root.join(LOG);
    let control = root.join("control");
    let (mut log, _) =
        LogFile::<Record>::open_rotating(&path, THRESHOLD).map_err(|e| e.to_string())?;
    let mut record = Record {
        seq: log.next_seq(),
        prev_hash: log.last_hash().into(),
        body: String::new(),
    };
    let overhead = serde_json::to_vec(&record)
        .map_err(|e| e.to_string())?
        .len()
        + 1;
    let bytes = usize::try_from(units)
        .map_err(|e| e.to_string())?
        .checked_mul(UNIT)
        .ok_or("record size overflow")?;
    if bytes < overhead {
        return Err("record units smaller than encoding overhead".into());
    }
    record.body = "x".repeat(bytes - overhead);
    let mut hooks = vec![
        (
            "before-rotation",
            arm_point(&path, FaultPoint::BeforeRotation, PointFault::Pause)
                .map_err(|e| e.to_string())?,
        ),
        (
            "before-write",
            arm_point(&path, FaultPoint::BeforeRecordWrite, PointFault::Pause)
                .map_err(|e| e.to_string())?,
        ),
    ];
    if root.join("pause-after-seal").exists() {
        hooks.push((
            "after-seal",
            arm_point(&path, FaultPoint::AfterSegmentSeal, PointFault::Pause)
                .map_err(|e| e.to_string())?,
        ));
    }
    if root.join("pause-after-create").exists() {
        hooks.push((
            "after-create",
            arm_point(&path, FaultPoint::AfterSegmentCreate, PointFault::Pause)
                .map_err(|e| e.to_string())?,
        ));
    }
    let worker = thread::spawn(move || log.append(&mut record));
    while !worker.is_finished() {
        for (name, hook) in &hooks {
            if hook.observation().stage == FaultStage::Reached {
                let marker = control.join(format!("reached-{name}"));
                if !marker.exists() {
                    fs::write(&marker, "reached\n").map_err(|e| e.to_string())?;
                }
                if control.join(format!("release-{name}")).exists() {
                    hook.release(SyncOutcome::Succeed);
                }
            }
        }
        thread::sleep(Duration::from_millis(2));
    }
    let receipt = worker
        .join()
        .map_err(|_| "append worker panicked")?
        .map_err(|e| e.to_string())?;
    fs::write(
        control.join("receipt.json"),
        json!({"seq":receipt.seq,"bytes":receipt.bytes_written,"hash":receipt.hash}).to_string(),
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

struct RotationSut {
    root: PathBuf,
    child: Option<Child>,
    cursor_file: usize,
    cursor_seq: u64,
    read_back: Vec<u64>,
    up: bool,
    result: String,
}

impl RotationSut {
    fn new(name: &str) -> Result<Self, BindingError> {
        let root = if let Some(base) = std::env::var_os("COOP_MBT_EVIDENCE_DIR") {
            let root = PathBuf::from(base).join("rotation").join(name);
            fs::create_dir_all(root.parent().unwrap()).map_err(error)?;
            fs::create_dir(&root).map_err(error)?;
            root
        } else {
            tempfile::Builder::new()
                .prefix("coop-rotation-mbt-")
                .tempdir()
                .map_err(error)?
                .keep()
        };
        eprintln!("rotation fixture: {}", root.display());
        Ok(Self {
            root,
            child: None,
            cursor_file: 1,
            cursor_seq: 0,
            read_back: vec![],
            up: false,
            result: "none".into(),
        })
    }

    fn begin(&mut self, units: u64) -> Result<(), BindingError> {
        if !self.up || self.child.is_some() {
            return Err(error("rotation writer unavailable or already pending"));
        }
        let control = self.root.join("control");
        if control.exists() {
            fs::remove_dir_all(&control).map_err(error)?;
        }
        fs::create_dir(&control).map_err(error)?;
        let stderr = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("child.stderr.log"))
            .map_err(error)?;
        self.child = Some(
            Command::new(std::env::current_exe().map_err(error)?)
                .arg("--fault-rotation-child")
                .arg(&self.root)
                .arg(units.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(stderr)
                .spawn()
                .map_err(error)?,
        );
        self.wait_reached("before-rotation")?;
        self.result = "pending".into();
        Ok(())
    }

    fn release(&self, name: &str) -> Result<(), BindingError> {
        fs::write(
            self.root.join("control").join(format!("release-{name}")),
            "continue\n",
        )
        .map_err(error)
    }

    fn wait_reached(&mut self, name: &str) -> Result<(), BindingError> {
        let marker = self.root.join("control").join(format!("reached-{name}"));
        let deadline = Instant::now() + Duration::from_secs(10);
        while !marker.exists() {
            if let Some(status) = self
                .child
                .as_mut()
                .ok_or_else(|| error("rotation child missing"))?
                .try_wait()
                .map_err(error)?
            {
                return Err(error(format!(
                    "rotation child exited before {name}: {status}; see {}/child.stderr.log",
                    self.root.display()
                )));
            }
            if Instant::now() >= deadline {
                return Err(error(format!("rotation child did not reach {name}")));
            }
            thread::sleep(Duration::from_millis(2));
        }
        Ok(())
    }

    fn kill(&mut self) -> Result<(), BindingError> {
        let mut child = self
            .child
            .take()
            .ok_or_else(|| error("no live subprocess to crash"))?;
        let pid = child.id();
        child.kill().map_err(error)?;
        let status = child.wait().map_err(error)?;
        let mut events = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("crashes.jsonl"))
            .map_err(error)?;
        writeln!(
            events,
            "{}",
            json!({"pid":pid,"exit":status.to_string(),"success":status.success()})
        )
        .map_err(error)?;
        if status.success() {
            return Err(error(
                "crash control exited successfully instead of being killed",
            ));
        }
        self.up = false;
        self.result = "stopped".into();
        Ok(())
    }

    fn disk(&self) -> Result<(Vec<Value>, Vec<Vec<u64>>), BindingError> {
        // Validate actual global chain without modifying the stream.
        let report = LogFile::<Record>::read_segments(self.root.join(LOG)).map_err(error)?;
        if report.was_truncated() {
            return Err(error("rotation observation found torn bytes"));
        }
        let mut records = vec![];
        let mut files = vec![];
        for path in segment_paths(self.root.join(LOG)).map_err(error)? {
            let mut file = vec![];
            for line in fs::read(&path)
                .map_err(error)?
                .split_inclusive(|b| *b == b'\n')
            {
                if line.len() % UNIT != 0 {
                    return Err(error("encoded record is not a whole model unit"));
                }
                let record: Record = serde_json::from_slice(line).map_err(error)?;
                file.push(record.seq);
                records.push(json!({"id":record.seq,"bytes":line.len()/UNIT}));
            }
            files.push(file);
        }
        if files.is_empty() {
            files.push(vec![]);
        }
        Ok((records, files))
    }
}

impl Drop for RotationSut {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl MBFaultRotationPort for RotationSut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        LogFile::<Record>::open_rotating(self.root.join(LOG), THRESHOLD).map_err(error)?;
        self.up = true;
        self.result = "none".into();
        Ok(())
    }
    fn begin_rotating_append(
        &mut self,
        input: BeginRotatingAppendInput,
    ) -> Result<(), BindingError> {
        self.begin(input.value.to_string().parse().map_err(error)?)
    }
    fn prepare_segment(&mut self) -> Result<(), BindingError> {
        self.release("before-rotation")?;
        self.wait_reached("before-write")?;
        self.result = "pending".into();
        Ok(())
    }
    fn commit_rotating_append(&mut self) -> Result<(), BindingError> {
        self.release("before-write")?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .ok_or_else(|| error("no pending rotation child"))?
                .try_wait()
                .map_err(error)?
            {
                if !status.success() {
                    return Err(error(format!("rotation append failed: {status}")));
                }
                break;
            }
            if Instant::now() >= deadline {
                return Err(error("rotation append did not finish"));
            }
            thread::sleep(Duration::from_millis(2));
        }
        self.child.take();
        let receipt: Value = serde_json::from_slice(
            &fs::read(self.root.join("control/receipt.json")).map_err(error)?,
        )
        .map_err(error)?;
        let actual = LogFile::<Record>::read_segments(self.root.join(LOG)).map_err(error)?;
        if receipt["seq"].as_u64() != Some(actual.seq_head) {
            return Err(error("actual receipt does not match recovered head"));
        }
        self.result = "ok".into();
        Ok(())
    }
    fn crash_rotation(&mut self) -> Result<(), BindingError> {
        self.kill()
    }
    fn recover_rotation(&mut self) -> Result<(), BindingError> {
        if self.up {
            return Err(error("recover requires a stopped stream"));
        }
        LogFile::<Record>::open_rotating(self.root.join(LOG), THRESHOLD).map_err(error)?;
        self.up = true;
        self.result = "ok".into();
        Ok(())
    }
    fn save_cursor(&mut self) -> Result<(), BindingError> {
        let (_, files) = self.disk()?;
        self.cursor_seq = LogFile::<Record>::read_segments(self.root.join(LOG))
            .map_err(error)?
            .seq_head;
        self.cursor_file = if self.cursor_seq == 0 {
            1
        } else {
            files
                .iter()
                .position(|file| file.contains(&self.cursor_seq))
                .ok_or_else(|| error("actual cursor record missing"))?
                + 1
        };
        self.result = "ok".into();
        Ok(())
    }
    fn read_cursor(&mut self, input: ReadCursorInput) -> Result<(), BindingError> {
        let seq: u64 = input.value.to_string().parse().map_err(error)?;
        self.read_back = LogFile::<Record>::read_segments(self.root.join(LOG))
            .map_err(error)?
            .records
            .into_iter()
            .filter(|record| record.seq > seq)
            .map(|record| record.seq)
            .collect();
        self.result = "ok".into();
        Ok(())
    }
    fn observe(&mut self) -> Result<MBFaultRotationObservation, BindingError> {
        let (records, files) = self.disk()?;
        let observed = json!({"observation":{"records":records,"files":files,"cursor":{"file":self.cursor_file,"seq":self.cursor_seq},"readBack":self.read_back,"up":self.up},"result":self.result});
        let mut history = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("observations.jsonl"))
            .map_err(error)?;
        writeln!(history, "{observed}").map_err(error)?;
        let observation =
            generated::MiTypeO0::decode(&fault_support::value(observed["observation"].clone())?)?;
        Ok(MBFaultRotationObservation {
            observation,
            result: self.result.clone(),
        })
    }
}

pub fn replay(bin: &str, trace: &Path) -> Result<(), String> {
    let name = trace.file_stem().unwrap().to_string_lossy();
    let config = fault_support::config("MBFaultRotation");
    let binding = generated::bind_m_b_fault_rotation(
        RotationSut::new(&name).map_err(|e| e.to_string())?,
        &config,
    )
    .map_err(|e| e.to_string())?;
    fault_support::run(bin, trace, config, binding)
}

/// Additional real-process crash controls at both archive/create boundaries.
pub fn check_crash_boundaries() -> Result<(), String> {
    for (point, missing) in [("after-seal", true), ("after-create", false)] {
        let mut sut = RotationSut::new(&format!("boundary-{point}")).map_err(|e| e.to_string())?;
        let check = |result: Result<(), BindingError>| result.map_err(|e| e.to_string());
        check(sut.initialize())?;
        check(sut.begin(1))?;
        check(sut.prepare_segment())?;
        check(sut.commit_rotating_append())?;
        fs::write(sut.root.join(format!("pause-{point}")), "pause\n").map_err(|e| e.to_string())?;
        check(sut.begin(3))?;
        check(sut.release("before-rotation"))?;
        check(sut.wait_reached(point))?;
        let path = sut.root.join(LOG);
        if path.exists() == missing {
            return Err(format!("unexpected active-file existence at {point}"));
        }
        if !missing && fs::metadata(&path).map_err(|e| e.to_string())?.len() != 0 {
            return Err("prepared active segment was not empty".into());
        }
        check(sut.kill())?;
        if LogFile::<Record>::read_segments(&path)
            .map_err(|e| e.to_string())?
            .seq_head
            != 1
        {
            return Err("crash lost prior receipt".into());
        }
        check(sut.recover_rotation())?;
        fs::remove_file(sut.root.join(format!("pause-{point}"))).map_err(|e| e.to_string())?;
        check(sut.begin(1))?;
        check(sut.prepare_segment())?;
        check(sut.commit_rotating_append())?;
        if LogFile::<Record>::read_segments(&path)
            .map_err(|e| e.to_string())?
            .seq_head
            != 2
        {
            return Err("post-crash append lost sequence continuity".into());
        }
        check(sut.observe().map(|_| ()))?;
        eprintln!("PASS actual process crash boundary {point}");
    }
    Ok(())
}
