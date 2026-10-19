//! Root ownership fault traces through compiler-generated typed dispatch.
//!
//! The existing writer fixture owns real daemon children, verifies their PID
//! over UDS, and reads raw durable records. This adapter reuses that fixture;
//! the generated fault interface supplies its inputs and compares observations.
use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use mirrorrust::BindingError;
use serde_json::json;

use crate::fault_support::{self, error};
use crate::writers::{self, generated as process};
use process::MBWritersPort;

#[path = "../fault_interfaces/writer/generated/MBFaultWriterMirror.generated.rs"]
#[allow(dead_code)]
mod generated;
use generated::{MBFaultWriterObservation, MBFaultWriterPort, NativeCodec};

struct WriterFaultSut {
    process: writers::WriterSut,
    home: PathBuf,
    rejected: BTreeSet<String>,
    result: String,
    observations: u64,
}

fn writer_name(actor: &str) -> Result<&'static str, BindingError> {
    match actor {
        "p1" => Ok("w1"),
        "p2" => Ok("w2"),
        _ => Err(error("unknown fault writer actor")),
    }
}

impl WriterFaultSut {
    fn new(home: PathBuf) -> Result<Self, BindingError> {
        Ok(Self {
            process: writers::WriterSut::new(home.clone())?,
            home,
            rejected: BTreeSet::new(),
            result: "none".into(),
            observations: 0,
        })
    }

    /// Retain raw bytes without opening a repairing LogFile handle. The run/
    /// directory contains sockets and is intentionally excluded from snapshots.
    fn snapshot(&self, label: &str) -> Result<(), BindingError> {
        fn copy_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
            fs::create_dir_all(destination)?;
            for entry in fs::read_dir(source)? {
                let entry = entry?;
                if entry.file_name() == "run" {
                    continue;
                }
                let target = destination.join(entry.file_name());
                if entry.file_type()?.is_dir() {
                    copy_tree(&entry.path(), &target)?;
                } else if entry.file_type()?.is_file() {
                    fs::copy(entry.path(), target)?;
                }
            }
            Ok(())
        }
        copy_tree(
            &self.home.join("r1"),
            &self.home.join("evidence").join(label),
        )
        .map_err(error)
    }

    fn start_attempt(&mut self, actor: &str, mode: &str) -> Result<bool, BindingError> {
        let writer = writer_name(actor)?;
        self.process.start(process::StartInput {
            root: "r1".into(),
            socket: mode.into(),
            writer: writer.into(),
        })?;
        let actual = self.process.observe()?;
        let admitted = actual.writers.0[writer] == "r1";
        if !admitted && !actual.rejected.0[writer] {
            return Err(error("startup produced neither ready PID nor refusal"));
        }
        Ok(admitted)
    }
}

impl MBFaultWriterPort for WriterFaultSut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        self.process.initialize()?;
        self.result = "none".into();
        Ok(())
    }

    fn start(&mut self, input: generated::StartInput) -> Result<(), BindingError> {
        self.snapshot(&format!("{}-before-start", self.observations))?;
        let admitted = self.start_attempt(&input.actor, &input.mode)?;
        self.snapshot(&format!("{}-after-start", self.observations))?;
        if admitted {
            self.rejected.remove(&input.actor);
        } else {
            self.rejected.insert(input.actor);
        }
        self.result = if admitted { "ok" } else { "busy" }.into();
        Ok(())
    }

    fn stop(&mut self, input: generated::StopInput) -> Result<(), BindingError> {
        self.process.stop(process::StopInput {
            writer: writer_name(&input.actor)?.into(),
        })?;
        self.result = "ok".into();
        Ok(())
    }

    fn tear_tail(&mut self) -> Result<(), BindingError> {
        self.process
            .tear_tail(process::TearTailInput { root: "r1".into() })?;
        self.result = "ok".into();
        Ok(())
    }

    fn writer_append(&mut self, input: generated::WriterAppendInput) -> Result<(), BindingError> {
        let writer = writer_name(&input.actor)?;
        let actual = self.process.observe()?;
        if actual.writers.0[writer] == "r1" {
            self.process.send(process::SendInput {
                writer: writer.into(),
            })?;
            self.result = "ok".into();
            return Ok(());
        }

        // The actor has no running daemon. Attempt its real production startup
        // and, when admitted, the actual append. Ownership rejection must come
        // from the SUT's guard; fixture bookkeeping never manufactures refusal.
        let admitted = self.start_attempt(&input.actor, "different")?;
        if admitted {
            let result = self.process.send(process::SendInput {
                writer: writer.into(),
            });
            let stopped = self.process.stop(process::StopInput {
                writer: writer.into(),
            });
            result?;
            stopped?;
            self.result = "ok".into();
        } else {
            self.result = "not-owner".into();
        }
        Ok(())
    }

    fn observe(&mut self) -> Result<MBFaultWriterObservation, BindingError> {
        let actual = self.process.observe()?;
        let owners = ["p1", "p2"]
            .into_iter()
            .filter_map(|actor| {
                let writer = writer_name(actor).expect("known writer");
                (actual.writers.0[writer] == "r1").then_some(actor)
            })
            .collect::<Vec<_>>();
        let records = actual.records.0["r1"]
            .0
            .iter()
            .zip(&actual.seqs.0["r1"].0)
            .map(|(record, seq)| {
                if record == "recovery" {
                    json!(99)
                } else {
                    json!({"#bigint": seq.to_string()})
                }
            })
            .collect::<Vec<_>>();
        let observed = json!({
            "observation": {
                "owners": {"#set": owners}, "records": records,
                "rejected": {"#set": self.rejected}, "torn": actual.torn.0["r1"]
            },
            "result": self.result,
            "rawValidity": actual.valid.0["r1"],
            "rejectedReadOnly": actual.rejected_read_only
        });
        self.snapshot(&format!("{}-observation", self.observations))?;
        self.observations += 1;
        let mut history = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.home.join("observations.jsonl"))
            .map_err(error)?;
        serde_json::to_writer(&mut history, &observed).map_err(error)?;
        writeln!(history).map_err(error)?;
        let observation =
            generated::MiTypeO0::decode(&fault_support::value(observed["observation"].clone())?)?;
        Ok(MBFaultWriterObservation {
            observation,
            result: self.result.clone(),
        })
    }
}

pub fn replay(bin: &str, trace: &Path) -> Result<(), String> {
    let parent = std::env::var_os("COOP_MBT_EVIDENCE_DIR").map(PathBuf::from);
    let fixture = if let Some(parent) = &parent {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        tempfile::Builder::new()
            .prefix("writer-")
            .tempdir_in(parent)
    } else {
        tempfile::Builder::new()
            .prefix("coop-fault-writer-")
            .tempdir()
    }
    .map_err(|e| e.to_string())?;
    // Retain before entering the client: an unexpected binding failure may
    // unwind through replay, and must not delete the evidence it just captured.
    let home = fixture.keep();
    eprintln!("writer fault fixture: {}", home.display());
    let config = fault_support::config("MBFaultWriter");
    let sut = WriterFaultSut::new(home.clone()).map_err(|e| e.to_string())?;
    let binding = generated::bind_m_b_fault_writer(sut, &config).map_err(|e| e.to_string())?;
    let result = fault_support::run(bin, trace, config, binding);
    eprintln!("retained writer fault fixture: {}", home.display());
    result
}
