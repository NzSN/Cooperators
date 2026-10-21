mod common;

use std::fs;

use cooperators::storage::{
    hash_record, segment_paths, ChainRecord, LogError, LogFile, GENESIS_HASH,
};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    seq: u64,
    prev_hash: String,
    #[serde(serialize_with = "serialize_body")]
    body: String,
}

fn serialize_body<S: Serializer>(body: &str, serializer: S) -> Result<S::Ok, S::Error> {
    if body == "!fail-serialization!" {
        return Err(serde::ser::Error::custom("requested serialization failure"));
    }
    serializer.serialize_str(body)
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

fn record(body: &str) -> Record {
    Record {
        seq: 0,
        prev_hash: String::new(),
        body: body.into(),
    }
}

#[test]
fn batch_receipts_bytes_and_chain_match_sequential_appends() {
    let temp = common::TempDir::new("batch-chain");
    let path = temp.path().join("batch.jsonl");
    let sequential_path = temp.path().join("sequential.jsonl");
    let (mut batch_log, _) = LogFile::<Record>::open(&path).unwrap();
    let (mut sequential_log, _) = LogFile::<Record>::open(&sequential_path).unwrap();
    let prefix = batch_log.append(&mut record("prefix")).unwrap();
    sequential_log.append(&mut record("prefix")).unwrap();
    let mut records = vec![record("one"), record("two"), record("three")];
    let sequential_receipts: Vec<_> = records
        .clone()
        .iter_mut()
        .map(|record| sequential_log.append(record).unwrap())
        .collect();
    let receipts = batch_log.append_batch(&mut records).unwrap();

    assert_eq!(receipts, sequential_receipts);
    assert_eq!(records[0].prev_hash, prefix.hash);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.seq, index as u64 + 2);
        assert_eq!(receipts[index].hash, hash_record(record).unwrap());
        if index > 0 {
            assert_eq!(record.prev_hash, receipts[index - 1].hash);
        }
    }
    assert_eq!(batch_log.seq_head(), 4);
    assert_eq!(batch_log.record_count(), 4);
    assert_eq!(batch_log.last_hash(), receipts[2].hash);
    assert_eq!(fs::read(&path).unwrap(), fs::read(sequential_path).unwrap());
    drop(batch_log);
    let (mut reopened, report) = LogFile::<Record>::open(&path).unwrap();
    assert_eq!(&report.records[1..], records);
    assert!(!report.was_truncated());
    assert_eq!(reopened.append(&mut record("next")).unwrap().seq, 5);
}

#[test]
fn empty_batch_keeps_file_and_durable_head_unchanged() {
    let temp = common::TempDir::new("batch-empty");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
    assert!(log.append_batch(&mut []).unwrap().is_empty());
    assert_eq!(log.seq_head(), 0);
    assert_eq!(log.last_hash(), GENESIS_HASH);
    let receipt = log.append(&mut record("prefix")).unwrap();
    let before = fs::read(&path).unwrap();
    assert!(log.append_batch(&mut []).unwrap().is_empty());
    assert_eq!(log.seq_head(), receipt.seq);
    assert_eq!(log.last_hash(), receipt.hash);
    assert_eq!(log.record_count(), 1);
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn serialization_failure_precedes_all_writes_and_handle_stays_reusable() {
    let temp = common::TempDir::new("batch-serialization");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
    let receipt = log.append(&mut record("prefix")).unwrap();
    let before = fs::read(&path).unwrap();
    let mut records = [record("serializable"), record("!fail-serialization!")];
    assert!(matches!(
        log.append_batch(&mut records),
        Err(LogError::Serialize(_))
    ));
    assert_eq!(log.seq_head(), receipt.seq);
    assert_eq!(log.last_hash(), receipt.hash);
    assert_eq!(log.record_count(), 1);
    assert!(!log.requires_recovery());
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(log.append(&mut record("retry")).unwrap().seq, 2);
}

#[test]
fn batch_repairs_missing_separator_and_accounts_for_it_once() {
    let temp = common::TempDir::new("batch-separator");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
    let first = log.append(&mut record("prefix")).unwrap();
    drop(log);
    let mut bytes = fs::read(&path).unwrap();
    assert_eq!(bytes.pop(), Some(b'\n'));
    fs::write(&path, &bytes).unwrap();
    let (mut log, report) = LogFile::<Record>::open(&path).unwrap();
    assert!(report.trailing_newline_missing);
    let mut records = [record("one"), record("two")];
    let receipts = log.append_batch(&mut records).unwrap();
    assert_eq!(records[0].prev_hash, first.hash);
    assert_eq!(
        receipts[0].bytes_written as usize,
        serde_json::to_vec(&records[0]).unwrap().len() + 2
    );
    assert_eq!(
        receipts[1].bytes_written as usize,
        serde_json::to_vec(&records[1]).unwrap().len() + 1
    );
    let expected_len = bytes.len() as u64
        + receipts
            .iter()
            .map(|receipt| receipt.bytes_written)
            .sum::<u64>();
    assert_eq!(fs::metadata(&path).unwrap().len(), expected_len);
    drop(log);
    let (_, report) = LogFile::<Record>::open(&path).unwrap();
    assert_eq!(report.seq_head, 3);
    assert!(!report.trailing_newline_missing);
    assert!(!report.was_truncated());
}

#[test]
fn rotating_batch_preserves_sealed_stream_chain_and_recovery() {
    let temp = common::TempDir::new("batch-rotating");
    let path = temp.path().join("events.jsonl");
    let (mut log, _) = LogFile::<Record>::open_rotating(&path, 1).unwrap();
    let mut records = [record("one"), record("two"), record("three")];
    let receipts = log.append_batch(&mut records).unwrap();
    assert_eq!(segment_paths(&path).unwrap().len(), 3);
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.seq)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(records[1].prev_hash, receipts[0].hash);
    assert_eq!(records[2].prev_hash, receipts[1].hash);
    drop(log);
    let (mut log, report) = LogFile::<Record>::open_rotating(&path, 1).unwrap();
    assert_eq!(report.records, records);
    assert_eq!(report.last_hash, receipts[2].hash);
    assert_eq!(log.append(&mut record("next")).unwrap().seq, 4);
}

#[cfg(feature = "mbt-injection")]
mod faults {
    use super::*;
    use cooperators::faults::{
        arm_append, arm_point, AppendFault, FaultPoint, FaultStage, PointFault, SyncOutcome,
    };
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn batch_receipts_wait_for_covering_sync() {
        let temp = common::TempDir::new("batch-sync-pause");
        let path = temp.path().join("events.jsonl");
        let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
        let hook = arm_append(&path, AppendFault::PauseBeforeSync).unwrap();
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            tx.send(log.append_batch(&mut [record("one"), record("two")]))
                .unwrap();
            log
        });
        let observed = hook.wait_reached(Duration::from_secs(2)).unwrap();
        assert_eq!(observed.stage, FaultStage::BeforeSync);
        assert_eq!(observed.seq, Some(1));
        assert!(!observed.sync_attempted);
        assert!(rx.try_recv().is_err());
        let written = fs::read(&path).unwrap();
        assert_eq!(written.len(), observed.bytes_written);
        hook.release(SyncOutcome::Succeed);
        let receipts = rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
        assert_eq!(receipts.len(), 2);
        let mut log = worker.join().unwrap();
        assert_eq!(log.seq_head(), 2);
        assert_eq!(log.record_count(), 2);
        let observed = hook.observation();
        assert_eq!(observed.stage, FaultStage::Synced);
        assert!(observed.sync_attempted && observed.sync_succeeded);
        assert_eq!(log.append(&mut record("next")).unwrap().seq, 3);
    }

    #[test]
    fn sync_failure_keeps_acknowledged_head_and_quarantines_until_reopen() {
        let temp = common::TempDir::new("batch-sync-failure");
        let path = temp.path().join("events.jsonl");
        let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
        let acknowledged = log.append(&mut record("acknowledged")).unwrap();
        let prefix = fs::read(&path).unwrap();
        let hook = arm_append(&path, AppendFault::FailSync).unwrap();
        assert!(matches!(
            log.append_batch(&mut [record("one"), record("two")]),
            Err(LogError::Io(_))
        ));
        assert_eq!(hook.observation().seq, Some(2));
        assert!(log.requires_recovery());
        assert_eq!(log.seq_head(), acknowledged.seq);
        assert_eq!(log.last_hash(), acknowledged.hash);
        assert_eq!(log.record_count(), 1);
        let before_retry = fs::read(&path).unwrap();
        assert!(before_retry.starts_with(&prefix));
        let mut retry = [record("retry")];
        let unassigned = retry.clone();
        assert!(matches!(
            log.append_batch(&mut retry),
            Err(LogError::RecoveryRequired)
        ));
        assert_eq!(retry, unassigned);
        assert!(matches!(
            log.append_batch(&mut []),
            Err(LogError::RecoveryRequired)
        ));
        assert_eq!(fs::read(&path).unwrap(), before_retry);
        drop(log);
        let (mut reopened, report) = LogFile::<Record>::open(&path).unwrap();
        assert_eq!(report.seq_head, 2);
        assert_eq!(hash_record(&report.records[0]).unwrap(), acknowledged.hash);
        assert!(!report.was_truncated());
        assert_eq!(reopened.append(&mut record("next")).unwrap().seq, 3);
    }

    #[test]
    fn partial_second_record_recovers_complete_unacknowledged_prefix() {
        let temp = common::TempDir::new("batch-partial-second");
        let path = temp.path().join("events.jsonl");
        let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
        let acknowledged = log.append(&mut record("acknowledged")).unwrap();
        let prefix = fs::read(&path).unwrap();
        let pause = arm_append(&path, AppendFault::PauseBeforeSync).unwrap();
        let worker = thread::spawn(move || {
            let result = log.append_batch(&mut [record("one"), record("two"), record("three")]);
            (log, result)
        });
        let paused = pause.wait_reached(Duration::from_secs(2)).unwrap();
        assert_eq!(paused.seq, Some(2));
        let partial = arm_append(&path, AppendFault::PartialWrite { bytes: 9 }).unwrap();
        pause.release(SyncOutcome::Succeed);
        let (mut log, result) = worker.join().unwrap();
        assert!(matches!(result, Err(LogError::Io(_))));
        assert_eq!(partial.observation().seq, Some(3));
        assert_eq!(partial.observation().stage, FaultStage::PartialWriteFailed);
        assert!(!pause.observation().sync_attempted);
        assert!(log.requires_recovery());
        assert_eq!(log.seq_head(), 1);
        assert_eq!(log.last_hash(), acknowledged.hash);
        assert_eq!(log.record_count(), 1);
        let before_retry = fs::read(&path).unwrap();
        assert!(before_retry.starts_with(&prefix));
        assert!(matches!(
            log.append(&mut record("blocked")),
            Err(LogError::RecoveryRequired)
        ));
        assert_eq!(fs::read(&path).unwrap(), before_retry);
        drop(log);
        let (mut reopened, report) = LogFile::<Record>::open(&path).unwrap();
        assert_eq!(report.seq_head, 2);
        assert_eq!(report.truncated_bytes, 9);
        assert_eq!(hash_record(&report.records[0]).unwrap(), acknowledged.hash);
        assert_eq!(report.records[1].prev_hash, acknowledged.hash);
        assert_eq!(reopened.append(&mut record("next")).unwrap().seq, 3);
        drop(reopened);
        assert_eq!(LogFile::<Record>::open(&path).unwrap().1.seq_head, 3);
    }

    #[test]
    fn recovered_batch_prefix_must_sync_before_open_returns() {
        let temp = common::TempDir::new("batch-recovery-sync");
        let path = temp.path().join("events.jsonl");
        let (mut log, _) = LogFile::<Record>::open(&path).unwrap();
        let _hook = arm_append(&path, AppendFault::FailSync).unwrap();
        assert!(log
            .append_batch(&mut [record("one"), record("two")])
            .is_err());
        drop(log);
        let before = fs::read(&path).unwrap();
        let recovery = arm_point(&path, FaultPoint::BeforeRecoverySync, PointFault::Fail).unwrap();
        assert!(LogFile::<Record>::open(&path).is_err());
        assert_eq!(recovery.observation().stage, FaultStage::InjectedFailure);
        assert_eq!(fs::read(&path).unwrap(), before);
        let (_, report) = LogFile::<Record>::open(&path).unwrap();
        assert_eq!(report.seq_head, 1);
    }
}
