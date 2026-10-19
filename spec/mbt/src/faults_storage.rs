//! Real LogFile failure/recovery through the compiler-generated storage port.
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use cooperators::faults::{self, AppendFault, FaultHandle, FaultStage, SyncOutcome};
use cooperators::storage::{AppendReceipt, ChainRecord, LogError, LogFile, GENESIS_HASH};
use mirrorrust::BindingError;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::fault_support::{self, error};
#[path = "../fault_interfaces/storage/generated/MBFaultStorageMirror.generated.rs"]
#[allow(dead_code)]
mod generated;
use generated::{
    AppendBytesInput, MBFaultStorageObservation, MBFaultStoragePort, NativeCodec,
    RecoverStorageInput,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    id: u64,
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
type AppendThread = JoinHandle<(LogFile<Record>, Result<AppendReceipt, LogError>, u64)>;
struct Pending {
    worker: AppendThread,
    hook: FaultHandle,
}
/// Test-harness controls used to demonstrate that replay rejects bad evidence
/// and missing injection. They never alter production storage hooks or traces.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DiagnosticControl {
    WrongReceipt,
    CorruptObservation,
    MissingInjection,
}

impl DiagnosticControl {
    fn from_env() -> Result<Option<Self>, BindingError> {
        match std::env::var("COOP_MBT_CONTROL").as_deref() {
            Err(std::env::VarError::NotPresent) => Ok(None),
            Ok("storage-wrong-receipt") => Ok(Some(Self::WrongReceipt)),
            Ok("storage-corrupt-observation") => Ok(Some(Self::CorruptObservation)),
            Ok("storage-missing-injection") => Ok(Some(Self::MissingInjection)),
            _ => Err(error("unknown COOP_MBT_CONTROL for storage")),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::WrongReceipt => "storage-wrong-receipt",
            Self::CorruptObservation => "storage-corrupt-observation",
            Self::MissingInjection => "storage-missing-injection",
        }
    }
}

struct StorageSut {
    root: PathBuf,
    path: PathBuf,
    log: Option<LogFile<Record>>,
    pending: Option<Pending>,
    receipts: Vec<serde_json::Value>,
    result: String,
    control: Option<DiagnosticControl>,
}

impl StorageSut {
    fn new() -> Result<Self, BindingError> {
        let root = if let Ok(parent) =
            std::env::var("COOP_MBT_EVIDENCE_DIR").or_else(|_| std::env::var("COOP_MBT_EVIDENCE"))
        {
            fs::create_dir_all(&parent).map_err(error)?;
            tempfile::Builder::new()
                .prefix("storage-")
                .tempdir_in(parent)
                .map_err(error)?
        } else {
            tempfile::Builder::new()
                .prefix("coop-storage-mbt-")
                .tempdir()
                .map_err(error)?
        }
        .keep();
        eprintln!("storage fixture: {}", root.display());
        let path = root.join("storage.jsonl");
        Ok(Self {
            root,
            path,
            log: None,
            pending: None,
            receipts: Vec::new(),
            result: "none".into(),
            control: DiagnosticControl::from_env()?,
        })
    }
    fn complete(&mut self, pending: Pending, outcome: SyncOutcome) -> Result<(), BindingError> {
        pending.hook.release(outcome);
        self.collect(pending.worker)
    }
    fn collect(&mut self, worker: AppendThread) -> Result<(), BindingError> {
        let (log, result, id) = worker.join().map_err(|_| error("append worker panicked"))?;
        self.log = Some(log);
        match result {
            Ok(receipt) => {
                self.result = "ok".into();
                self.receipts.push(json!({"id":id,"seq":receipt.seq}));
            }
            Err(failure) => {
                self.result = if failure.to_string().to_lowercase().contains("recovery") {
                    "recovery-required"
                } else {
                    "io-error"
                }
                .into();
            }
        }
        Ok(())
    }
    fn read_records(&self) -> Result<(Vec<serde_json::Value>, Vec<usize>), BindingError> {
        let bytes = fs::read(&self.path).map_err(error)?;
        let mut records = Vec::new();
        let mut ends = Vec::new();
        let mut offset = 0;
        for line in bytes.split_inclusive(|byte| *byte == b'\n') {
            offset += line.len();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<Record>(line) {
                Ok(record) => records.push(json!({"id":record.id,"seq":record.seq})),
                Err(_) => {
                    let raw = std::str::from_utf8(line).map_err(error)?;
                    let tag = raw
                        .strip_prefix("{\"id\":")
                        .ok_or_else(|| error("unidentified partial record"))?;
                    let id = tag
                        .chars()
                        .take_while(|c| c.is_ascii_digit())
                        .collect::<String>()
                        .parse::<u64>()
                        .map_err(error)?;
                    records.push(json!({"id":id,"seq":0}));
                }
            }
            ends.push(offset);
        }
        Ok((records, ends))
    }
}
impl Drop for StorageSut {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.take() {
            let _ = self.complete(pending, SyncOutcome::Fail);
        }
    }
}
impl MBFaultStoragePort for StorageSut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        self.log = Some(LogFile::open(&self.path).map_err(error)?.0);
        self.result = "none".into();
        Ok(())
    }
    fn append_bytes(&mut self, input: AppendBytesInput) -> Result<(), BindingError> {
        if self.pending.is_some() {
            return Err(error("append already pending"));
        }
        let id = input.value.to_string().parse::<u64>().map_err(error)?;
        let fault = match input.mode.as_str() {
            "whole" => AppendFault::PauseBeforeSync,
            "partial" => AppendFault::PartialWrite { bytes: 10 },
            _ => return Err(error("unknown write mode")),
        };
        // This diagnostic deliberately omits the real hook. The actual append
        // runs normally; unexpected completion below is a binding failure.
        let mut hook = if self.control == Some(DiagnosticControl::MissingInjection) {
            None
        } else {
            Some(faults::arm_append(&self.path, fault).map_err(error)?)
        };
        let mut log = self.log.take().ok_or_else(|| error("writer unavailable"))?;
        let worker = thread::spawn(move || {
            let mut record = Record {
                id,
                seq: 0,
                prev_hash: GENESIS_HASH.into(),
                body: "fault-mbt".into(),
            };
            let result = log.append(&mut record);
            (log, result, id)
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if hook
                .as_ref()
                .is_some_and(|handle| handle.observation().stage == FaultStage::BeforeSync)
            {
                self.pending = Some(Pending {
                    worker,
                    hook: hook.take().unwrap(),
                });
                self.result = "pending".into();
                return Ok(());
            }
            if worker.is_finished() {
                if let Some(hook) = hook {
                    return self.complete(Pending { worker, hook }, SyncOutcome::Succeed);
                }
                self.collect(worker)?;
                // Preserve the actual successful receipt and disk observation;
                // never manufacture the pending response expected by the model.
                let _ = self.observe()?;
                return Err(error(
                    "missing fault injection: actual append completed without required barrier",
                ));
            }
            if Instant::now() >= deadline {
                if let Some(hook) = hook {
                    hook.release(SyncOutcome::Fail);
                }
                let _ = worker.join();
                return Err(error("real append failed to reach barrier"));
            }
            thread::yield_now();
        }
    }
    fn sync_fail(&mut self) -> Result<(), BindingError> {
        let pending = self
            .pending
            .take()
            .ok_or_else(|| error("no pending sync"))?;
        self.complete(pending, SyncOutcome::Fail)
    }
    fn sync_ok(&mut self) -> Result<(), BindingError> {
        let pending = self
            .pending
            .take()
            .ok_or_else(|| error("no pending sync"))?;
        self.complete(pending, SyncOutcome::Succeed)
    }
    fn recover_storage(&mut self, input: RecoverStorageInput) -> Result<(), BindingError> {
        if self.pending.is_some() {
            return Err(error("cannot recover a running operation"));
        }
        self.log.take();
        let kept = input.value.to_string().parse::<usize>().map_err(error)?;
        let (records, ends) = self.read_records()?;
        if kept > records.len() {
            return Err(error("retained prefix exceeds actual bytes"));
        }
        for receipt in &self.receipts {
            if !records[..kept].contains(receipt) {
                return Err(error("fixture would remove acknowledged bytes"));
            }
        }
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(error)?;
        file.set_len(if kept == 0 { 0 } else { ends[kept - 1] as u64 })
            .map_err(error)?;
        self.log = Some(LogFile::open(&self.path).map_err(error)?.0);
        self.result = "ok".into();
        Ok(())
    }
    fn inspect_storage(&mut self) -> Result<(), BindingError> {
        self.result = "ok".into();
        Ok(())
    }
    fn observe(&mut self) -> Result<MBFaultStorageObservation, BindingError> {
        let records = self.read_records()?.0;
        let observed = json!({"observation":{"records":records,"receipts":self.receipts},"result":self.result});
        let mut reported = observed["observation"].clone();
        match self.control {
            Some(DiagnosticControl::WrongReceipt) => {
                if let Some(receipt) = reported["receipts"]
                    .as_array_mut()
                    .and_then(|rows| rows.first_mut())
                {
                    let sequence = receipt["seq"]
                        .as_u64()
                        .ok_or_else(|| error("non-integer actual receipt"))?;
                    receipt["seq"] = json!(sequence + 1);
                }
            }
            Some(DiagnosticControl::CorruptObservation) => {
                if let Some(record) = reported["records"]
                    .as_array_mut()
                    .and_then(|rows| rows.first_mut())
                {
                    let sequence = record["seq"]
                        .as_u64()
                        .ok_or_else(|| error("non-integer actual record sequence"))?;
                    record["seq"] = json!(sequence + 1);
                }
            }
            _ => {}
        }
        // Evidence retains factual observations separately from diagnostic
        // corruption returned to replay. The receipt ledger itself is intact.
        let mut evidence = observed;
        if let Some(control) = self.control {
            evidence["diagnosticControl"] = json!(control.label());
            evidence["reportedObservation"] = reported.clone();
        }
        let mut history = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("observations.jsonl"))
            .map_err(error)?;
        serde_json::to_writer(&mut history, &evidence).map_err(error)?;
        writeln!(history).map_err(error)?;
        let observation = generated::MiTypeO0::decode(&fault_support::value(reported)?)?;
        Ok(MBFaultStorageObservation {
            observation,
            result: self.result.clone(),
        })
    }
}
pub fn replay(bin: &str, path: &Path) -> Result<(), String> {
    let config = fault_support::config("MBFaultStorage");
    let binding =
        generated::bind_m_b_fault_storage(StorageSut::new().map_err(|e| e.to_string())?, &config)
            .map_err(|e| e.to_string())?;
    fault_support::run(bin, path, config, binding)
}
