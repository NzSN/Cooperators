mod common;

use std::fs::{self, OpenOptions};
use std::io::Write;

use cooperators::storage::{segment_paths, ChainRecord, LogFile, GENESIS_HASH};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    seq: u64,
    prev_hash: String,
    body: String,
}
impl ChainRecord for Record {
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
fn sized(bytes: usize) -> Record {
    let mut record = Record {
        seq: 1,
        prev_hash: GENESIS_HASH.into(),
        body: String::new(),
    };
    let overhead = serde_json::to_vec(&record).unwrap().len() + 1;
    record.body = "x".repeat(bytes - overhead);
    record
}

#[test]
fn exact_threshold_and_global_sequence_hash_survive_restart() {
    let temp = common::TempDir::new("rotate-chain");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open_rotating(&path, 1024).unwrap();
    log.append(&mut sized(512)).unwrap();
    let second = log.append(&mut sized(512)).unwrap();
    assert_eq!(segment_paths(&path).unwrap().len(), 1);
    let mut third = sized(512);
    assert_eq!(log.append(&mut third).unwrap().seq, 3);
    assert_eq!(third.prev_hash, second.hash);
    let paths = segment_paths(&path).unwrap();
    assert_eq!(paths.len(), 2);
    assert_eq!(fs::metadata(&paths[0]).unwrap().len(), 1024);
    assert_eq!(fs::metadata(&paths[1]).unwrap().len(), 512);
    drop(log);
    let (mut log, report) = LogFile::<Record>::open_rotating(&path, 1024).unwrap();
    assert_eq!(report.records.len(), 3);
    assert_eq!(report.seq_head, 3);
    assert_eq!(log.append(&mut sized(512)).unwrap().seq, 4);
}

#[test]
fn oversized_record_is_alone_and_next_append_rotates() {
    let temp = common::TempDir::new("rotate-oversize");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open_rotating(&path, 1024).unwrap();
    log.append(&mut sized(1536)).unwrap();
    assert_eq!(segment_paths(&path).unwrap().len(), 1);
    log.append(&mut sized(512)).unwrap();
    let paths = segment_paths(&path).unwrap();
    assert_eq!(paths.len(), 2);
    assert_eq!(fs::metadata(&paths[0]).unwrap().len(), 1536);
    assert_eq!(fs::metadata(&paths[1]).unwrap().len(), 512);
    assert_eq!(LogFile::<Record>::read_segments(&path).unwrap().seq_head, 2);
}

#[test]
fn sealed_final_record_corruption_is_fatal_and_read_only() {
    let temp = common::TempDir::new("rotate-sealed-corrupt");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open_rotating(&path, 512).unwrap();
    log.append(&mut sized(512)).unwrap();
    log.append(&mut sized(512)).unwrap();
    drop(log);
    let paths = segment_paths(&path).unwrap();
    let mut bad = fs::read(&paths[0]).unwrap();
    let index = bad.iter().position(|b| *b == b'x').unwrap();
    bad[index] = b'y';
    fs::write(&paths[0], &bad).unwrap();
    let before = paths
        .iter()
        .map(|path| fs::read(path).unwrap())
        .collect::<Vec<_>>();
    assert!(LogFile::<Record>::read_segments(&path).is_err());
    assert!(LogFile::<Record>::open_rotating(&path, 512).is_err());
    assert_eq!(
        paths
            .iter()
            .map(|path| fs::read(path).unwrap())
            .collect::<Vec<_>>(),
        before
    );
}

#[test]
fn active_tail_repair_keeps_seals_and_global_head() {
    let temp = common::TempDir::new("rotate-tail");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open_rotating(&path, 512).unwrap();
    log.append(&mut sized(512)).unwrap();
    log.append(&mut sized(512)).unwrap();
    drop(log);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"torn-tail")
        .unwrap();
    let before = fs::metadata(&path).unwrap().len();
    let report = LogFile::<Record>::read_segments(&path).unwrap();
    assert_eq!(report.truncated_bytes, 9);
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        before,
        "read-only inspection cannot repair"
    );
    let (mut log, report) = LogFile::<Record>::open_rotating(&path, 512).unwrap();
    assert_eq!(report.seq_head, 2);
    assert_eq!(report.truncated_bytes, 9);
    assert_eq!(log.append(&mut sized(512)).unwrap().seq, 3);
}

#[cfg(feature = "mbt-injection")]
#[test]
fn interrupted_archive_rename_recovers_missing_active_without_losing_head() {
    use cooperators::faults::{arm_point, FaultPoint, PointFault};
    let temp = common::TempDir::new("rotate-after-seal");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open_rotating(&path, 512).unwrap();
    log.append(&mut sized(512)).unwrap();
    let _hook = arm_point(&path, FaultPoint::AfterSegmentSeal, PointFault::Fail).unwrap();
    assert!(log.append(&mut sized(512)).is_err());
    assert!(log.requires_recovery());
    assert!(!path.exists());
    assert_eq!(LogFile::<Record>::read_segments(&path).unwrap().seq_head, 1);
    drop(log);
    let (mut log, report) = LogFile::<Record>::open_rotating(&path, 512).unwrap();
    assert_eq!(report.seq_head, 1);
    assert_eq!(log.append(&mut sized(512)).unwrap().seq, 2);
}

#[cfg(feature = "mbt-injection")]
#[test]
fn interrupted_empty_active_creation_recovers_without_empty_seal() {
    use cooperators::faults::{arm_point, FaultPoint, PointFault};
    let temp = common::TempDir::new("rotate-after-create");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open_rotating(&path, 512).unwrap();
    log.append(&mut sized(512)).unwrap();
    let _hook = arm_point(&path, FaultPoint::AfterSegmentCreate, PointFault::Fail).unwrap();
    assert!(log.append(&mut sized(512)).is_err());
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    drop(log);
    let (mut log, report) = LogFile::<Record>::open_rotating(&path, 512).unwrap();
    assert_eq!(report.seq_head, 1);
    log.append(&mut sized(512)).unwrap();
    assert_eq!(segment_paths(&path).unwrap().len(), 2);
}

#[test]
fn dotted_names_have_distinct_segment_directories() {
    let temp = common::TempDir::new("rotate-names");
    for name in ["queue.a.jsonl", "queue.a.dead.jsonl"] {
        let path = temp.path().join(name);
        let (mut log, _) = LogFile::<Record>::open_rotating(&path, 512).unwrap();
        log.append(&mut sized(512)).unwrap();
        log.append(&mut sized(512)).unwrap();
        assert_eq!(LogFile::<Record>::read_segments(&path).unwrap().seq_head, 2);
        assert!(
            segment_paths(&path).unwrap()[0].starts_with(temp.path().join(".segments").join(name))
        );
    }
}
