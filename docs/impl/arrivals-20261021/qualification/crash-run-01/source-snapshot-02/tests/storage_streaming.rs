mod common;

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use cooperators::storage::{
    hash_record, segment_paths, ChainRecord, LogError, LogFile, ReplayReport, ReplaySummary,
    GENESIS_HASH,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

fn record(index: u64) -> Record {
    Record {
        seq: 0,
        prev_hash: GENESIS_HASH.into(),
        body: format!("record-{index}-{}", "x".repeat(170)),
    }
}

fn bytes(path: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    segment_paths(path)
        .unwrap()
        .into_iter()
        .map(|part| {
            let contents = fs::read(&part).unwrap();
            (part, contents)
        })
        .collect()
}

fn same_summary(summary: &ReplaySummary, report: &ReplayReport<Record>) {
    assert_eq!(summary.record_count, report.records.len() as u64);
    assert_eq!(summary.seq_head, report.seq_head);
    assert_eq!(summary.last_hash, report.last_hash);
    assert_eq!(summary.last_valid_offset, report.last_valid_offset);
    assert_eq!(summary.truncated_bytes, report.truncated_bytes);
    assert_eq!(
        summary.trailing_newline_missing,
        report.trailing_newline_missing
    );
}

#[test]
fn streaming_replay_preserves_order_hashes_offsets_and_append_position() {
    for rotating in [false, true] {
        let root = common::TempDir::new("streaming-order");
        let path = root.path().join("events.jsonl");
        let (mut log, _) = if rotating {
            LogFile::<Record>::open_rotating(&path, 700).unwrap()
        } else {
            LogFile::<Record>::open(&path).unwrap()
        };
        for index in 1..=9 {
            log.append(&mut record(index)).unwrap();
        }
        drop(log);
        let expected = LogFile::<Record>::read_segments(&path).unwrap();
        let before = bytes(&path);
        let mut read = Vec::new();
        let summary = LogFile::<Record>::read_segments_streaming(&path, |row| {
            read.push(row.clone());
            Ok(())
        })
        .unwrap();
        same_summary(&summary, &expected);
        assert_eq!(read, expected.records);
        assert_eq!(bytes(&path), before, "read-only replay cannot mutate files");
        let mut recovered = Vec::new();
        let visitor = |row: &Record| {
            recovered.push(row.clone());
            Ok(())
        };
        let (mut log, summary) = if rotating {
            LogFile::<Record>::open_rotating_streaming(&path, 700, visitor).unwrap()
        } else {
            LogFile::<Record>::open_streaming(&path, visitor).unwrap()
        };
        same_summary(&summary, &expected);
        assert_eq!(recovered, expected.records);
        assert_eq!(log.seq_head(), 9);
        assert_eq!(log.last_hash(), expected.last_hash);
        let mut next = record(10);
        let receipt = log.append(&mut next).unwrap();
        assert_eq!(receipt.seq, 10);
        assert_eq!(next.prev_hash, summary.last_hash);
        assert_eq!(receipt.hash, hash_record(&next).unwrap());
        assert_eq!(
            LogFile::<Record>::read_segments(&path).unwrap().seq_head,
            10
        );
        if rotating {
            assert!(segment_paths(&path).unwrap().len() > 1);
        }
    }
}

#[test]
fn streaming_read_does_not_create_missing_logs_or_directories() {
    let root = common::TempDir::new("streaming-missing");
    let path = root.path().join("missing/events.jsonl");
    assert!(LogFile::<Record>::read_segments_streaming(&path, |_| Ok(())).is_err());
    assert!(!path.parent().unwrap().exists());
    let (log, summary) = LogFile::<Record>::open_streaming(&path, |_| Ok(())).unwrap();
    assert_eq!(summary.record_count, 0);
    assert_eq!(summary.last_hash, GENESIS_HASH);
    assert_eq!(log.next_seq(), 1);
    assert!(path.exists());
}

#[test]
fn streaming_torn_active_tail_is_read_only_until_open_and_repaired_before_append() {
    let root = common::TempDir::new("streaming-tail");
    let path = root.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open_rotating(&path, 400).unwrap();
    for index in 1..=3 {
        log.append(&mut record(index)).unwrap();
    }
    drop(log);
    let valid_len = fs::metadata(&path).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{\"torn\":")
        .unwrap();
    let before = bytes(&path);
    let report = LogFile::<Record>::read_segments(&path).unwrap();
    let summary = LogFile::<Record>::read_segments_streaming(&path, |_| Ok(())).unwrap();
    same_summary(&summary, &report);
    assert_eq!(summary.last_valid_offset, valid_len);
    assert_eq!(summary.truncated_bytes, 8);
    assert_eq!(bytes(&path), before);
    let (mut log, recovered) =
        LogFile::<Record>::open_rotating_streaming(&path, 400, |_| Ok(())).unwrap();
    assert_eq!(summary, recovered);
    assert_eq!(fs::metadata(&path).unwrap().len(), valid_len);
    assert_eq!(
        &bytes(&path)[..before.len() - 1],
        &before[..before.len() - 1]
    );
    assert_eq!(log.append(&mut record(4)).unwrap().seq, 4);
}

#[test]
fn streaming_missing_final_newline_is_preserved_then_separated_on_append() {
    let root = common::TempDir::new("streaming-newline");
    let path = root.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
    log.append(&mut record(1)).unwrap();
    drop(log);
    let mut contents = fs::read(&path).unwrap();
    assert_eq!(contents.pop(), Some(b'\n'));
    fs::write(&path, &contents).unwrap();
    let (mut log, summary) = LogFile::<Record>::open_streaming(&path, |_| Ok(())).unwrap();
    assert!(summary.trailing_newline_missing);
    assert_eq!(summary.last_valid_offset, contents.len() as u64);
    assert_eq!(fs::read(&path).unwrap(), contents);
    assert_eq!(log.append(&mut record(2)).unwrap().seq, 2);
    assert_eq!(
        LogFile::<Record>::read_segments(&path)
            .unwrap()
            .records
            .len(),
        2
    );
}

#[test]
fn streaming_invalid_middle_and_corrupt_seal_are_fatal_without_mutation() {
    for sealed in [false, true] {
        let root = common::TempDir::new("streaming-corrupt");
        let path = root.path().join("events.jsonl");
        let threshold = if sealed { 400 } else { u64::MAX };
        let (mut log, _) = LogFile::<Record>::open_rotating(&path, threshold).unwrap();
        for index in 1..=3 {
            log.append(&mut record(index)).unwrap();
        }
        drop(log);
        let first = segment_paths(&path).unwrap().remove(0);
        let mut contents = fs::read(&first).unwrap();
        let index = contents.iter().position(|byte| *byte == b'x').unwrap();
        contents[index] = b'y';
        fs::write(&first, contents).unwrap();
        let before = bytes(&path);
        assert!(LogFile::<Record>::read_segments_streaming(&path, |_| Ok(())).is_err());
        assert!(LogFile::<Record>::open_rotating_streaming(&path, threshold, |_| Ok(())).is_err());
        assert_eq!(bytes(&path), before);
    }
}

#[test]
fn streaming_visitor_error_is_propagated_without_repair_or_usable_log() {
    let root = common::TempDir::new("streaming-visitor");
    let path = root.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
    log.append(&mut record(1)).unwrap();
    drop(log);
    let before = bytes(&path);
    let error = LogFile::<Record>::open_streaming(&path, |_| {
        Err(LogError::InvalidRecord("visitor refused record".into()))
    })
    .unwrap_err();
    assert!(error.to_string().contains("visitor refused record"));
    assert_eq!(bytes(&path), before);
}

#[cfg(feature = "mbt-injection")]
#[test]
fn streaming_recovery_sync_failure_cannot_publish_a_recovered_handle() {
    use cooperators::faults::{arm_append, arm_point, AppendFault, FaultPoint, PointFault};
    let root = common::TempDir::new("streaming-recovery-sync");
    let path = root.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
    let _append = arm_append(&path, AppendFault::FailSync).unwrap();
    assert!(log.append(&mut record(1)).is_err());
    drop(log);
    let before = bytes(&path);
    let _recovery = arm_point(&path, FaultPoint::BeforeRecoverySync, PointFault::Fail).unwrap();
    let mut private_index = Vec::new();
    let result = LogFile::<Record>::open_streaming(&path, |row| {
        private_index.push(row.seq);
        Ok(())
    });
    assert!(
        result.is_err(),
        "a visited prefix is not a successful recovery"
    );
    assert_eq!(private_index, [1]);
    assert_eq!(bytes(&path), before);
    let (mut log, summary) = LogFile::<Record>::open_streaming(&path, |_| Ok(())).unwrap();
    assert_eq!(summary.seq_head, 1);
    assert_eq!(log.append(&mut record(2)).unwrap().seq, 2);
}
