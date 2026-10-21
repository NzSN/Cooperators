mod common;

use std::fs::{self, OpenOptions};
use std::io::Write;

use cooperators::envelope::{Body, Envelope};
use cooperators::storage::{hash_record, LogError, LogFile, GENESIS_HASH};

fn envelope(text: &str) -> Envelope {
    Envelope::new(
        "codex:test",
        "claude:test",
        "event",
        Body::new("text/plain", text),
    )
    .expect("create envelope")
}

fn append_n(log: &mut LogFile<Envelope>, count: usize) -> Vec<String> {
    let mut hashes = Vec::new();
    for index in 0..count {
        let mut record = envelope(&format!("message {index}"));
        hashes.push(log.append(&mut record).expect("append record").hash);
    }
    hashes
}

#[test]
fn valid_log_replays_with_an_intact_hash_chain() {
    let temp = common::TempDir::new("chain");
    let path = temp.path().join("agents/a/inbox.jsonl");

    let (mut log, report) = LogFile::<Envelope>::open(&path).expect("open empty log");
    assert!(report.records.is_empty());
    assert_eq!(report.seq_head, 0);
    assert_eq!(report.last_hash, GENESIS_HASH);
    assert_eq!(log.next_seq(), 1);

    let hashes = append_n(&mut log, 5);
    assert_eq!(log.seq_head(), 5);
    assert_eq!(log.record_count(), 5);
    drop(log);

    let (log, report) = LogFile::<Envelope>::open(&path).expect("replay log");
    assert_eq!(report.records.len(), 5);
    assert_eq!(report.seq_head, 5);
    assert_eq!(report.last_hash, hashes[4]);
    assert_eq!(report.truncated_bytes, 0);
    assert!(!report.trailing_newline_missing);
    assert_eq!(log.next_seq(), 6);

    assert_eq!(report.records[0].prev_hash, GENESIS_HASH);
    for (index, record) in report.records.iter().enumerate() {
        assert_eq!(record.seq, index as u64 + 1);
        record.validate().expect("replayed envelope validates");
        if index > 0 {
            let previous_hash = hash_record(&report.records[index - 1]).expect("hash previous");
            assert_eq!(record.prev_hash, previous_hash);
        }
    }
    assert_eq!(
        hash_record(report.records.last().expect("records")).unwrap(),
        report.last_hash
    );
}

#[test]
fn record_hash_excludes_the_prev_hash_member() {
    let mut first = envelope("payload");
    first.prev_hash = GENESIS_HASH.to_string();
    let mut second = first.clone();
    second.prev_hash = format!("sha256:{}", "ab".repeat(32));

    let first_hash = hash_record(&first).expect("hash");
    let second_hash = hash_record(&second).expect("hash");
    assert_eq!(first_hash, second_hash);
    assert!(first_hash.starts_with("sha256:"));
    assert_eq!(first_hash.len(), 71);
}

#[test]
fn torn_tail_is_truncated_and_the_log_continues() {
    let bad_hash_tail = {
        let mut record = envelope("bad-hash");
        record.seq = 4;
        record.prev_hash = format!("sha256:{}", "cd".repeat(32));
        let mut line = serde_json::to_vec(&record).expect("serialize");
        line.push(b'\n');
        line
    };

    let cases: [(&str, Vec<u8>); 3] = [
        ("invalid-json-newline", b"garbage-tail\n".to_vec()),
        ("partial-json", b"{\"v\":1,\"truncated\":".to_vec()),
        ("bad-hash", bad_hash_tail),
    ];

    for (name, garbage) in cases {
        let temp = common::TempDir::new(name);
        let path = temp.path().join("agents/a/inbox.jsonl");

        let (mut log, _) = LogFile::<Envelope>::open(&path).expect("open log");
        append_n(&mut log, 3);
        drop(log);
        let valid_len = fs::metadata(&path).expect("metadata").len();

        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open for garbage");
        file.write_all(&garbage).expect("write garbage");
        file.sync_all().expect("sync garbage");
        drop(file);

        let (mut log, report) = LogFile::<Envelope>::open(&path).expect("replay repairs torn tail");
        assert_eq!(report.records.len(), 3, "case {name}");
        assert_eq!(report.seq_head, 3, "case {name}");
        assert_eq!(
            report.truncated_bytes,
            garbage.len() as u64,
            "case {name}: garbage was not fully truncated"
        );
        assert!(report.was_truncated(), "case {name}");
        assert_eq!(log.next_seq(), 4, "case {name}");

        let repaired_len = fs::metadata(&path).expect("metadata").len();
        assert_eq!(repaired_len, valid_len, "case {name}: file not truncated");

        let mut next = envelope("after-repair");
        let receipt = log.append(&mut next).expect("append after repair");
        assert_eq!(receipt.seq, 4, "case {name}");
        drop(log);

        let (_, report) = LogFile::<Envelope>::open(&path).expect("replay repaired log");
        assert_eq!(report.records.len(), 4, "case {name}");
        assert_eq!(report.truncated_bytes, 0, "case {name}");
        for index in 1..report.records.len() {
            let previous = hash_record(&report.records[index - 1]).unwrap();
            assert_eq!(report.records[index].prev_hash, previous, "case {name}");
        }
    }
}

#[test]
fn mid_log_corruption_is_an_error_not_a_tail_truncation() {
    let temp = common::TempDir::new("mid-corrupt");
    let path = temp.path().join("agents/a/inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).expect("open");
    append_n(&mut log, 4);
    drop(log);

    let content = fs::read_to_string(&path).expect("read log");
    let mut lines: Vec<String> = content.lines().map(str::to_string).collect();
    lines[1] = lines[1].replace("message 1", "message tampered");
    fs::write(&path, format!("{}\n", lines.join("\n"))).expect("write tampered log");

    let error = LogFile::<Envelope>::open(&path).expect_err("tamper must be detected");
    assert!(matches!(error, LogError::Corrupt { line: 3, .. }));
}

#[test]
fn sequencer_is_rebuilt_and_monotonic_across_restart() {
    let temp = common::TempDir::new("sequencer");
    let path = temp.path().join("agents/a/inbox.jsonl");

    let (mut log, _) = LogFile::<Envelope>::open(&path).expect("open");
    append_n(&mut log, 2);
    drop(log);

    let (mut log, report) = LogFile::<Envelope>::open(&path).expect("reopen");
    assert_eq!(report.seq_head, 2);
    assert_eq!(log.next_seq(), 3);

    let mut assigned = Vec::new();
    for _ in 0..3 {
        let mut record = envelope("restarted");
        assigned.push(log.append(&mut record).expect("append").seq);
    }
    assert_eq!(assigned, vec![3, 4, 5]);
    drop(log);

    let (_, report) = LogFile::<Envelope>::open(&path).expect("final replay");
    assert_eq!(report.seq_head, 5);
    assert_eq!(
        report
            .records
            .iter()
            .map(|record| record.seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
}

#[test]
fn valid_final_record_without_newline_is_preserved() {
    let temp = common::TempDir::new("no-newline");
    let path = temp.path().join("agents/a/inbox.jsonl");
    let (mut log, _) = LogFile::<Envelope>::open(&path).expect("open");
    append_n(&mut log, 2);
    drop(log);

    let bytes = fs::read(&path).expect("read");
    assert_eq!(bytes.last(), Some(&b'\n'));
    fs::write(&path, &bytes[..bytes.len() - 1]).expect("strip newline");

    let (mut log, report) = LogFile::<Envelope>::open(&path).expect("replay");
    assert_eq!(report.records.len(), 2);
    assert_eq!(report.truncated_bytes, 0);
    assert!(report.trailing_newline_missing);

    let mut record = envelope("after-missing-newline");
    assert_eq!(log.append(&mut record).expect("append").seq, 3);
    drop(log);

    let (_, report) = LogFile::<Envelope>::open(&path).expect("final replay");
    assert_eq!(report.records.len(), 3);
    assert_eq!(report.seq_head, 3);
    assert!(!report.trailing_newline_missing);
}
