mod common;

use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use cooperators::client::{BrokerClient, ClientError, HttpsOptions};
use cooperators::id::{new_ulid, unix_millis};
use cooperators::transport::is_journaled_mutation;
use cooperators::transport::journal::{
    JournalOutcome, OperationJournal, OperationScope, MAX_JOURNAL_BYTES, MAX_JOURNAL_ENTRIES,
};
use serde_json::json;

fn scope() -> OperationScope {
    OperationScope {
        endpoint: "https://localhost:7420/".into(),
        bus_id: new_ulid().unwrap(),
        agent_id: new_ulid().unwrap(),
    }
}

fn prepare(journal: &OperationJournal) -> cooperators::transport::journal::JournalEntry {
    journal
        .prepare(
            scope(),
            "send_message",
            json!({"to":"recipient","body":{"type":"text","text":"durable intent"}}),
            &new_ulid().unwrap(),
            unix_millis() + 60_000,
        )
        .unwrap()
}

#[test]
fn persisted_intent_survives_reopen_and_fresh_invocations_get_new_ids() {
    let temp = common::TempDir::new("operation-journal-roundtrip");
    let path = temp.path().join("journal");
    let journal = OperationJournal::open(&path).unwrap();
    let first = prepare(&journal);
    let second = journal
        .prepare(
            first.scope.clone(),
            &first.method,
            first.intent.clone(),
            &first.operation.epoch,
            first.operation.not_after_ms,
        )
        .unwrap();
    assert_ne!(first.operation.id, second.operation.id);
    assert_eq!(first.outcome, JournalOutcome::Pending);
    drop(journal);
    let reopened = OperationJournal::open(&path).unwrap();
    assert_eq!(reopened.get(&first.operation.id).unwrap(), first);
    assert_eq!(reopened.list().unwrap().len(), 2);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for name in ["operations.json", "owner.lock"] {
        assert_eq!(
            std::fs::metadata(path.join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn stale_temporary_file_is_not_an_admitted_intent() {
    let temp = common::TempDir::new("operation-journal-scratch");
    let path = temp.path().join("journal");
    let journal = OperationJournal::open(&path).unwrap();
    let entry = prepare(&journal);
    let temporary = path.join("operations.tmp");
    std::fs::write(&temporary, b"{torn candidate").unwrap();
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(journal);
    let journal = OperationJournal::open(&path).unwrap();
    assert_eq!(journal.list().unwrap(), vec![entry]);
    prepare(&journal);
    assert_eq!(journal.list().unwrap().len(), 2);
    assert!(!temporary.exists());
}

#[test]
fn corrupt_authoritative_state_fails_closed() {
    let temp = common::TempDir::new("operation-journal-corrupt");
    let path = temp.path().join("journal");
    let journal = OperationJournal::open(&path).unwrap();
    prepare(&journal);
    let file = path.join("operations.json");
    std::fs::write(&file, b"{torn authoritative state").unwrap();
    assert!(journal.list().is_err());
    assert!(OperationJournal::open(&path).is_err());
    assert_eq!(std::fs::read(&file).unwrap(), b"{torn authoritative state");
}

#[test]
fn journal_refuses_symlinks_hardlinks_and_nonprivate_files() {
    let temp = common::TempDir::new("operation-journal-private");
    let path = temp.path().join("journal");
    let journal = OperationJournal::open(&path).unwrap();
    prepare(&journal);
    let alias = temp.path().join("alias");
    symlink(&path, &alias).unwrap();
    assert!(OperationJournal::open(&alias).is_err());
    assert!(OperationJournal::open(alias.join("nested")).is_err());
    let file = path.join("operations.json");
    let target = path.join("actual.json");
    std::fs::rename(&file, &target).unwrap();
    symlink(&target, &file).unwrap();
    assert!(journal.list().is_err());
    std::fs::remove_file(&file).unwrap();
    std::fs::hard_link(&target, &file).unwrap();
    assert!(journal.list().is_err());
    std::fs::remove_file(&target).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(journal.list().is_err());
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(journal.list().is_ok());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(journal.list().is_err());
}

#[test]
fn journal_refuses_symlink_scratch_without_touching_its_target() {
    let temp = common::TempDir::new("operation-journal-symlink-scratch");
    let path = temp.path().join("journal");
    let journal = OperationJournal::open(&path).unwrap();
    let secret = temp.path().join("secret");
    std::fs::write(&secret, "do not overwrite").unwrap();
    symlink(&secret, path.join("operations.tmp")).unwrap();
    assert!(journal
        .prepare(
            scope(),
            "send_message",
            json!({}),
            &new_ulid().unwrap(),
            unix_millis() + 60_000
        )
        .is_err());
    assert_eq!(
        std::fs::read_to_string(&secret).unwrap(),
        "do not overwrite"
    );
    assert!(journal.list().unwrap().is_empty());
}

#[test]
fn journal_rejects_credentials_and_out_of_scope_mutations() {
    let temp = common::TempDir::new("operation-journal-no-credentials");
    let journal = OperationJournal::open(temp.path().join("journal")).unwrap();
    for field in [
        "token",
        "authorization",
        "bearer",
        "access_token",
        "refresh_token",
        "operation",
    ] {
        let mut params = json!({"to":"recipient"});
        params[field] = json!("not-stored");
        assert!(journal
            .prepare(
                scope(),
                "send_message",
                params,
                &new_ulid().unwrap(),
                unix_millis() + 60_000
            )
            .is_err());
    }
    assert!(journal
        .prepare(
            scope(),
            "enroll_agent",
            json!({}),
            &new_ulid().unwrap(),
            unix_millis() + 60_000
        )
        .is_err());
    assert!(journal.list().unwrap().is_empty());
    assert!(!journal.directory().join("operations.json").exists());
}

#[test]
fn every_admitted_domain_method_roundtrips_with_immutable_method_and_fresh_identity() {
    let temp = common::TempDir::new("operation-journal-domain-methods");
    let journal = OperationJournal::open(temp.path().join("journal")).unwrap();
    let scope = scope();
    let epoch = new_ulid().unwrap();
    let methods = [
        "send_message",
        "publish",
        "create_queue",
        "enqueue",
        "claim_task",
        "renew_lease",
        "complete_task",
        "fail_task",
        "publish_doc",
        "update_doc",
        "claim_doc",
        "release_doc",
    ];
    let mut identities = std::collections::BTreeSet::new();
    for method in methods {
        assert!(is_journaled_mutation(method));
        let entry = journal
            .prepare(
                scope.clone(),
                method,
                json!({"name":"immutable-target"}),
                &epoch,
                unix_millis() + 60_000,
            )
            .unwrap();
        assert!(identities.insert(entry.operation.id.clone()));
        assert_eq!(journal.get(&entry.operation.id).unwrap(), entry);
    }
    for method in [
        "operation_status",
        "read_doc",
        "heartbeat",
        "ack",
        "ack_arrivals",
        "renew_instance_token",
        "enroll_agent",
    ] {
        assert!(!is_journaled_mutation(method));
        assert!(journal
            .prepare(
                scope.clone(),
                method,
                json!({}),
                &epoch,
                unix_millis() + 60_000
            )
            .is_err());
    }
    assert_eq!(
        OperationJournal::open(journal.directory())
            .unwrap()
            .list()
            .unwrap()
            .len(),
        methods.len()
    );
}

#[test]
fn journal_rejects_unbounded_deadlines_and_noncanonical_scopes() {
    let temp = common::TempDir::new("operation-journal-reference");
    let journal = OperationJournal::open(temp.path().join("journal")).unwrap();
    for deadline in [0, unix_millis() - 1, unix_millis() + 48 * 60 * 60 * 1_000] {
        assert!(journal
            .prepare(
                scope(),
                "send_message",
                json!({}),
                &new_ulid().unwrap(),
                deadline
            )
            .is_err());
    }
    let mut unsafe_scope = scope();
    unsafe_scope.endpoint = "https://user:credential@localhost/".into();
    assert!(journal
        .prepare(
            unsafe_scope,
            "send_message",
            json!({}),
            &new_ulid().unwrap(),
            unix_millis() + 60_000
        )
        .is_err());
    assert!(journal.list().unwrap().is_empty());
}

#[test]
fn journal_entry_limit_is_checked_before_atomic_replacement() {
    let temp = common::TempDir::new("operation-journal-limit");
    let journal = OperationJournal::open(temp.path().join("journal")).unwrap();
    let first = prepare(&journal);
    let mut entries = serde_json::Map::new();
    for _ in 0..MAX_JOURNAL_ENTRIES {
        let mut entry = first.clone();
        entry.operation.id = new_ulid().unwrap();
        entries.insert(
            entry.operation.id.clone(),
            serde_json::to_value(entry).unwrap(),
        );
    }
    let file = journal.directory().join("operations.json");
    std::fs::write(
        &file,
        serde_json::to_vec(&json!({"v":1,"entries":entries})).unwrap(),
    )
    .unwrap();
    let before = std::fs::read(&file).unwrap();
    assert!(journal
        .prepare(
            scope(),
            "send_message",
            json!({}),
            &new_ulid().unwrap(),
            unix_millis() + 60_000
        )
        .is_err());
    assert_eq!(std::fs::read(file).unwrap(), before);
    assert_eq!(journal.list().unwrap().len(), MAX_JOURNAL_ENTRIES);
}

#[test]
fn oversized_intent_and_state_are_rejected_before_unbounded_loading() {
    let temp = common::TempDir::new("operation-journal-byte-limits");
    let journal = OperationJournal::open(temp.path().join("journal")).unwrap();
    assert!(journal
        .prepare(
            scope(),
            "send_message",
            json!({"body":{"text":"x".repeat(cooperators::transport::MAX_RPC_BYTES)}}),
            &new_ulid().unwrap(),
            unix_millis() + 60_000,
        )
        .is_err());
    assert!(journal.list().unwrap().is_empty());
    prepare(&journal);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(journal.directory().join("operations.json"))
        .unwrap();
    file.set_len(MAX_JOURNAL_BYTES as u64 + 1).unwrap();
    let error = journal.list().unwrap_err();
    assert!(
        matches!(error, ClientError::Protocol(ref detail) if detail.contains("exceeds byte limit"))
    );
}

#[test]
fn simultaneous_process_updates_are_not_lost() {
    let temp = common::TempDir::new("operation-journal-processes");
    let path = temp.path().join("journal");
    OperationJournal::open(&path).unwrap();
    let mut children = Vec::new();
    for _ in 0..4 {
        children.push(child(&path, "append").spawn().unwrap());
    }
    for mut process in children {
        assert!(process.wait().unwrap().success());
    }
    assert_eq!(
        OperationJournal::open(&path).unwrap().list().unwrap().len(),
        80
    );
}

#[test]
fn abrupt_process_death_preserves_the_prepared_operation() {
    let temp = common::TempDir::new("operation-journal-process-death");
    let path = temp.path().join("journal");
    let ready = temp.path().join("ready.json");
    let mut process = child(&path, "prepare-and-stop")
        .env("COOP_TEST_JOURNAL_READY", &ready)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.exists() {
        if Instant::now() >= deadline || process.try_wait().unwrap().is_some() {
            let _ = process.kill();
            panic!("journal child did not reach the persisted boundary");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    process.kill().unwrap();
    process.wait().unwrap();
    let entry: cooperators::transport::journal::JournalEntry =
        serde_json::from_slice(&std::fs::read(ready).unwrap()).unwrap();
    assert_eq!(
        OperationJournal::open(&path).unwrap().list().unwrap(),
        vec![entry]
    );
}

fn child(path: &Path, mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "operation_journal_process_child", "--nocapture"])
        .env("COOP_TEST_JOURNAL_PATH", path)
        .env("COOP_TEST_JOURNAL_MODE", mode)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    command
}

#[test]
fn operation_journal_process_child() {
    let Some(path) = std::env::var_os("COOP_TEST_JOURNAL_PATH") else {
        return;
    };
    let journal = OperationJournal::open(path).unwrap();
    match std::env::var("COOP_TEST_JOURNAL_MODE").unwrap().as_str() {
        "append" => {
            for _ in 0..20 {
                prepare(&journal);
            }
        }
        "prepare-and-stop" => {
            let entry = prepare(&journal);
            let ready =
                std::path::PathBuf::from(std::env::var_os("COOP_TEST_JOURNAL_READY").unwrap());
            let temporary = ready.with_extension("tmp");
            std::fs::write(&temporary, serde_json::to_vec(&entry).unwrap()).unwrap();
            std::fs::rename(temporary, ready).unwrap();
            loop {
                std::thread::park();
            }
        }
        _ => panic!("unknown journal child mode"),
    }
}

#[tokio::test]
async fn invalid_local_state_or_scope_fails_before_any_network_exchange() {
    // A syntactically valid endpoint on a closed port would report a transport
    // error if queried. Local persistence and scope failures must win first.
    let client = BrokerClient::https("https://localhost:1", HttpsOptions::default()).unwrap();
    let temp = common::TempDir::new("operation-journal-no-network");
    let journal = OperationJournal::open(temp.path().join("journal")).unwrap();
    let entry = prepare(&journal);
    let error = client
        .submit_operation(
            &journal,
            &entry.operation.id,
            "current-credential",
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, ClientError::Protocol(ref detail) if detail.contains("different broker endpoint"))
    );
    std::fs::write(journal.directory().join("operations.json"), "{torn").unwrap();
    let error = client
        .call_journaled(
            &journal,
            "send_message",
            json!({"token":"current-credential","to":"recipient","body":{}}),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, ClientError::Protocol(ref detail) if detail.contains("invalid operation journal"))
    );
}

#[tokio::test]
async fn current_credential_inside_application_payload_is_rejected_without_persistence() {
    let client = BrokerClient::https("https://localhost:1", HttpsOptions::default()).unwrap();
    let temp = common::TempDir::new("operation-journal-nested-credential");
    let journal = OperationJournal::open(temp.path().join("journal")).unwrap();
    let error = client.prepare_operation(&journal, "send_message", json!({"token":"current-credential","to":"recipient","body":{"text":"do not leak current-credential"}}), Duration::from_secs(1)).await.unwrap_err();
    assert!(
        matches!(error, ClientError::Protocol(ref detail) if detail.contains("contains the current credential"))
    );
    assert_eq!(journal.list().unwrap(), Vec::new());
    assert!(!journal.directory().join("operations.json").exists());
}

#[cfg(feature = "mbt-injection")]
#[tokio::test]
async fn recovered_intent_is_not_submitted_until_file_and_directory_sync_succeed() {
    use cooperators::faults::{arm_point, FaultPoint, FaultStage, PointFault};
    let temp = common::TempDir::new("operation-journal-recovery-sync");
    let journal = OperationJournal::open(temp.path().join("journal")).unwrap();
    let entry = prepare(&journal);
    let client = BrokerClient::https("https://localhost:1", HttpsOptions::default()).unwrap();
    for path in [
        journal.directory().join("operations.json"),
        journal.directory().to_owned(),
    ] {
        let hook = arm_point(path, FaultPoint::BeforeRecoverySync, PointFault::Fail).unwrap();
        let error = client
            .submit_operation(
                &journal,
                &entry.operation.id,
                "current-credential",
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, ClientError::Unreachable(ref error) if error.to_string().contains("injected MBT filesystem failure"))
        );
        assert_eq!(hook.observation().stage, FaultStage::InjectedFailure);
        drop(hook);
        assert_eq!(journal.get(&entry.operation.id).unwrap(), entry);
    }
}

#[cfg(feature = "mbt-injection")]
#[test]
fn reopening_repairs_existing_journal_directory_ancestry_before_use() {
    use cooperators::faults::{arm_point, FaultPoint, FaultStage, PointFault};
    let temp = common::TempDir::new("operation-journal-ancestry-sync");
    let directory = temp.path().join("journal");
    let journal = OperationJournal::open(&directory).unwrap();
    let entry = prepare(&journal);
    drop(journal);
    let hook = arm_point(
        temp.path(),
        FaultPoint::BeforeRecoverySync,
        PointFault::Fail,
    )
    .unwrap();
    assert!(OperationJournal::open(&directory).is_err());
    assert_eq!(hook.observation().stage, FaultStage::InjectedFailure);
    drop(hook);
    assert_eq!(
        OperationJournal::open(&directory)
            .unwrap()
            .get(&entry.operation.id)
            .unwrap(),
        entry
    );
}
