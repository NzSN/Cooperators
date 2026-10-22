//! Network activation is an explicit, irreversible format transition. These
//! controls use disposable roots and compare persistent bytes at refusal gates.
mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use cooperators::arrivals::migration::{prepare, ArrivalMeta};
use cooperators::network::NetworkPolicy;
use cooperators::registry::{Registry, ARRIVALS_REQUIRED_FEATURE, NETWORK_REQUIRED_FEATURE};
use cooperators::server::{DaemonConfig, DaemonState};
use serde_json::{json, Value};

fn policy() -> NetworkPolicy {
    serde_json::from_value(json!({
        "v": 1, "allowed_hosts": ["localhost"], "devices": []
    }))
    .unwrap()
}

fn config(root: &Path, arrivals: bool) -> DaemonConfig {
    let mut config = DaemonConfig::new(root);
    config.enable_arrivals = arrivals;
    config.network_policy = Some(policy());
    config
}

fn registry_json(root: &Path) -> Value {
    serde_json::from_slice(&fs::read(root.join("registry.json")).unwrap()).unwrap()
}

fn arrival_meta(root: &Path) -> ArrivalMeta {
    serde_json::from_slice(&fs::read(root.join("notifications/meta.json")).unwrap()).unwrap()
}

fn orphan_meta(root: &Path, bus_id: &str) -> Vec<u8> {
    fs::create_dir_all(root.join("notifications")).unwrap();
    let bytes = serde_json::to_vec(&json!({"v": 1, "bus_id": bus_id})).unwrap();
    fs::write(root.join("notifications/meta.json"), &bytes).unwrap();
    bytes
}

fn source_tails(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let registry = Registry::load(root).unwrap();
    let agent = registry.records().next().unwrap();
    let paths = [
        root.join("docs/guard.jsonl"),
        root.join("queues/guard.jsonl"),
        root.join("agents")
            .join(&agent.agent_id)
            .join("inbox.jsonl"),
    ];
    paths
        .into_iter()
        .map(|path| {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let bytes = b"{\"intentionally_torn_tail\":".to_vec();
            fs::write(&path, &bytes).unwrap();
            (path, bytes)
        })
        .collect()
}

fn assert_bytes(snapshot: &BTreeMap<PathBuf, Vec<u8>>) {
    for (path, before) in snapshot {
        assert_eq!(
            &fs::read(path).unwrap(),
            before,
            "unexpected repair: {}",
            path.display()
        );
    }
}

#[tokio::test]
async fn v1_activation_preserves_instances_and_persistent_network_identity() {
    let root = common::TempDir::new("network-v1");
    let mut registry = Registry::load(root.path()).unwrap();
    let alice = registry.register("alice".into(), None, None, None).unwrap();
    let retired = registry
        .register("retired".into(), None, None, None)
        .unwrap();
    registry.retire(&retired.token).unwrap();
    let before = registry_json(root.path());
    let state = DaemonState::with_config(config(root.path(), false)).unwrap();
    let meta = state.network().unwrap().meta.clone();
    let after = registry_json(root.path());
    assert_eq!(after["v"], 3);
    assert_eq!(after["agents"], before["agents"]);
    assert_eq!(
        after["revoked_token_hashes"],
        before["revoked_token_hashes"]
    );
    assert_eq!(
        after["required_features"],
        json!([NETWORK_REQUIRED_FEATURE])
    );
    assert!(!root.path().join("notifications").exists());
    drop(state);
    let mut registry = Registry::load(root.path()).unwrap();
    assert!(registry.authenticate(&retired.token).is_none());
    let rotated = registry
        .register(alice.name, None, None, Some(&alice.token))
        .unwrap();
    assert_eq!(rotated.agent_id, alice.agent_id);
    assert_eq!(registry.network_meta(), Some(&meta));
    registry.retire(&rotated.token).unwrap();
    let state = DaemonState::with_config(config(root.path(), false)).unwrap();
    assert_eq!(state.network().unwrap().meta, meta);
    assert_eq!(registry_json(root.path())["v"], 3);
}

#[tokio::test]
async fn both_upgrade_orders_share_one_bus_and_preserve_arrival_feature() {
    for arrivals_first in [false, true] {
        let root = common::TempDir::new("network-upgrade-order");
        let mut registry = Registry::load(root.path()).unwrap();
        registry.register("alice".into(), None, None, None).unwrap();
        let before = if arrivals_first {
            Some(prepare(root.path(), &mut registry, true).unwrap().unwrap())
        } else {
            None
        };
        let state = DaemonState::with_config(config(root.path(), false)).unwrap();
        let network = state.network().unwrap().meta.clone();
        drop(state);
        if let Some(before) = before {
            assert_eq!(network.bus_id, before.bus_id);
        }
        let state = DaemonState::with_config(config(root.path(), true)).unwrap();
        assert_eq!(state.network().unwrap().meta, network);
        assert_eq!(arrival_meta(root.path()).bus_id, network.bus_id);
        let registry = Registry::load(root.path()).unwrap();
        assert!(registry.arrivals_enabled());
        assert!(registry
            .required_features()
            .contains(&ARRIVALS_REQUIRED_FEATURE.into()));
        assert!(registry
            .required_features()
            .contains(&NETWORK_REQUIRED_FEATURE.into()));
        assert_eq!(registry.format_version(), 3);
    }
}

#[tokio::test]
async fn orphan_arrival_identity_is_adopted_without_enabling_arrivals() {
    let root = common::TempDir::new("network-orphan-arrival");
    let mut registry = Registry::load(root.path()).unwrap();
    registry.register("alice".into(), None, None, None).unwrap();
    let bus = cooperators::id::new_ulid().unwrap();
    let bytes = orphan_meta(root.path(), &bus);
    let state = DaemonState::with_config(config(root.path(), false)).unwrap();
    let meta = state.network().unwrap().meta.clone();
    assert_eq!(meta.bus_id, bus);
    assert!(!Registry::load(root.path()).unwrap().arrivals_enabled());
    assert_eq!(
        fs::read(root.path().join("notifications/meta.json")).unwrap(),
        bytes
    );
    drop(state);
    let state = DaemonState::with_config(config(root.path(), true)).unwrap();
    assert_eq!(state.network().unwrap().meta, meta);
    assert_eq!(arrival_meta(root.path()).bus_id, bus);
}

#[tokio::test]
async fn conflicting_or_corrupt_arrival_identity_refuses_before_guard_or_log_changes() {
    for enable_arrivals in [false, true] {
        let root = common::TempDir::new("network-conflicting-meta");
        let mut registry = Registry::load(root.path()).unwrap();
        registry.register("alice".into(), None, None, None).unwrap();
        drop(DaemonState::with_config(config(root.path(), false)).unwrap());
        orphan_meta(root.path(), &cooperators::id::new_ulid().unwrap());
        let before = fs::read(root.path().join("registry.json")).unwrap();
        let tails = source_tails(root.path());
        let error = DaemonState::with_config(config(root.path(), enable_arrivals)).unwrap_err();
        assert!(
            error.to_string().contains("bus identities conflict"),
            "{error}"
        );
        assert_eq!(fs::read(root.path().join("registry.json")).unwrap(), before);
        assert_bytes(&tails);
    }
    for malformed in [
        b"invalid".as_slice(),
        b"{\"v\":1,\"bus_id\":\"bad\"}".as_slice(),
    ] {
        let root = common::TempDir::new("network-invalid-meta");
        let mut registry = Registry::load(root.path()).unwrap();
        registry.register("alice".into(), None, None, None).unwrap();
        fs::create_dir(root.path().join("notifications")).unwrap();
        fs::write(root.path().join("notifications/meta.json"), malformed).unwrap();
        let before = fs::read(root.path().join("registry.json")).unwrap();
        let tails = source_tails(root.path());
        assert!(DaemonState::with_config(config(root.path(), false)).is_err());
        assert_eq!(fs::read(root.path().join("registry.json")).unwrap(), before);
        assert_bytes(&tails);
    }
}

#[tokio::test]
async fn invalid_policy_cannot_activate_arrivals_or_network() {
    let root = common::TempDir::new("network-invalid-policy");
    let mut registry = Registry::load(root.path()).unwrap();
    registry.register("alice".into(), None, None, None).unwrap();
    let before = fs::read(root.path().join("registry.json")).unwrap();
    let mut invalid = config(root.path(), true);
    invalid.network_policy.as_mut().unwrap().v = 99;
    assert!(DaemonState::with_config(invalid).is_err());
    assert_eq!(fs::read(root.path().join("registry.json")).unwrap(), before);
    assert!(!root.path().join("notifications").exists());
}

fn refused_process(program: &Path, root: &Path, expected_error: &str) {
    let stderr_path = root.join("refusal.stderr");
    let mut child = Command::new(program)
        .arg("--root")
        .arg(root)
        .args(["daemon", "run", "--http-addr", "off"])
        .env_remove("COOPERATORS_HOME")
        .env_remove("COOP_SOCKET")
        .env_remove("COOP_TOKEN")
        .stdout(Stdio::null())
        .stderr(fs::File::create(&stderr_path).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("binary did not refuse upgraded root");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stderr = fs::read_to_string(stderr_path).unwrap();
    assert!(!status.success(), "{stderr}");
    assert!(stderr.contains(expected_error), "{stderr}");
    assert!(!root.join("run/daemon.sock").exists());
}

#[tokio::test]
async fn activated_v3_root_requires_policy_before_any_log_repair() {
    let root = common::TempDir::new("network-policy-required");
    let mut registry = Registry::load(root.path()).unwrap();
    registry.register("alice".into(), None, None, None).unwrap();
    drop(DaemonState::with_config(config(root.path(), false)).unwrap());
    let before = fs::read(root.path().join("registry.json")).unwrap();
    let tails = source_tails(root.path());
    assert!(DaemonState::with_config(DaemonConfig::new(root.path())).is_err());
    refused_process(
        Path::new(env!("CARGO_BIN_EXE_coop")),
        root.path(),
        "approved policy on every reopen",
    );
    assert_eq!(fs::read(root.path().join("registry.json")).unwrap(), before);
    assert_bytes(&tails);
}

#[test]
fn incomplete_v3_guard_or_principal_history_is_rejected_without_changes() {
    let bus = cooperators::id::new_ulid().unwrap();
    let epoch = cooperators::id::new_ulid().unwrap();
    let valid = json!({"v":3, "required_features":[NETWORK_REQUIRED_FEATURE],
        "network":{"bus_id":bus,"operation_epoch":epoch}, "network_principals":{}, "agents":{}});
    let mut cases = Vec::new();
    for field in ["required_features", "network", "network_principals"] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        cases.push(missing);
    }
    let mut invalid_history = valid.clone();
    invalid_history["network_principals"] = json!({"device":{"generation":0,"active":true}});
    cases.push(invalid_history);
    for value in cases {
        let root = common::TempDir::new("network-invalid-guard");
        let bytes = serde_json::to_vec(&value).unwrap();
        fs::write(root.path().join("registry.json"), &bytes).unwrap();
        assert!(Registry::load(root.path()).is_err());
        assert_eq!(fs::read(root.path().join("registry.json")).unwrap(), bytes);
    }
}

#[cfg(feature = "mbt-injection")]
mod injection {
    use super::*;
    use cooperators::faults::{arm_point, FaultPoint, PointFault};

    #[tokio::test]
    async fn pre_network_binary_refuses_v3_before_log_repair() {
        let Some(program) = std::env::var_os("COOP_PRENETWORK_BINARY") else {
            assert_ne!(
                std::env::var("COOP_REQUIRE_NETWORK_MIGRATION").as_deref(),
                Ok("1"),
                "required migration gate needs COOP_PRENETWORK_BINARY pinned to pre-network source"
            );
            eprintln!("COOP_PRENETWORK_BINARY unset: actual pre-network refusal gate skipped");
            return;
        };
        for arrivals in [false, true] {
            let root = common::TempDir::new("network-old-binary");
            let mut registry = Registry::load(root.path()).unwrap();
            registry.register("alice".into(), None, None, None).unwrap();
            drop(DaemonState::with_config(config(root.path(), arrivals)).unwrap());
            let before = fs::read(root.path().join("registry.json")).unwrap();
            let tails = source_tails(root.path());
            refused_process(
                Path::new(&program),
                root.path(),
                "unsupported registry version 3",
            );
            assert_eq!(fs::read(root.path().join("registry.json")).unwrap(), before);
            assert_bytes(&tails);
        }
    }

    #[tokio::test]
    async fn failed_activation_reopens_actual_disk_guard_without_repairing_logs_early() {
        for point in [
            FaultPoint::BeforeRegistryReplace,
            FaultPoint::AfterRegistryReplace,
            FaultPoint::AfterRegistrySync,
        ] {
            let root = common::TempDir::new("network-failed-activation");
            let mut registry = Registry::load(root.path()).unwrap();
            registry.register("alice".into(), None, None, None).unwrap();
            let tails = source_tails(root.path());
            let hook =
                arm_point(root.path().join("registry.json"), point, PointFault::Fail).unwrap();
            assert!(DaemonState::with_config(config(root.path(), false)).is_err());
            assert_bytes(&tails);
            assert_eq!(hook.observation().point, Some(point));
            let old_meta = Registry::load(root.path()).unwrap().network_meta().cloned();
            let state = DaemonState::with_config(config(root.path(), false)).unwrap();
            if let Some(meta) = old_meta {
                assert_eq!(state.network().unwrap().meta, meta);
            }
            assert_eq!(registry_json(root.path())["v"], 3);
        }
    }

    #[test]
    fn network_migration_child() {
        let Some(root) = std::env::var_os("COOP_NETWORK_MIGRATION_CHILD_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let point = match std::env::var("COOP_NETWORK_MIGRATION_CHILD_POINT")
            .unwrap()
            .as_str()
        {
            "before" => FaultPoint::BeforeRegistryReplace,
            "replace" => FaultPoint::AfterRegistryReplace,
            "sync" => FaultPoint::AfterRegistrySync,
            other => panic!("unknown point {other}"),
        };
        let hook = arm_point(root.join("registry.json"), point, PointFault::Pause).unwrap();
        let child_root = root.clone();
        std::thread::spawn(move || {
            let _state = DaemonState::owned(config(&child_root, false)).unwrap();
            panic!("activation pause unexpectedly released");
        });
        hook.wait_reached(Duration::from_secs(10)).unwrap();
        fs::write(root.join("paused"), b"migration boundary reached").unwrap();
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    #[tokio::test]
    async fn process_kill_at_registry_boundaries_preserves_recoverable_identity() {
        for arrivals in [false, true] {
            for point in ["before", "replace", "sync"] {
                let root = common::TempDir::new("network-killed-activation");
                let mut registry = Registry::load(root.path()).unwrap();
                registry.register("alice".into(), None, None, None).unwrap();
                let arrival = prepare(root.path(), &mut registry, arrivals).unwrap();
                let before = fs::read(root.path().join("registry.json")).unwrap();
                let tails = source_tails(root.path());
                let mut child = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "injection::network_migration_child",
                        "--nocapture",
                    ])
                    .env("COOP_NETWORK_MIGRATION_CHILD_ROOT", root.path())
                    .env("COOP_NETWORK_MIGRATION_CHILD_POINT", point)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap();
                let deadline = Instant::now() + Duration::from_secs(10);
                while !root.path().join("paused").exists() {
                    if let Some(status) = child.try_wait().unwrap() {
                        panic!("migration child exited: {status}");
                    }
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("migration pause not reached");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                child.kill().unwrap();
                let status = child.wait().unwrap();
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(status.signal(), Some(9));
                assert_bytes(&tails);
                let persisted = Registry::load(root.path()).unwrap();
                if point == "before" {
                    assert_eq!(fs::read(root.path().join("registry.json")).unwrap(), before);
                    assert!(persisted.network_meta().is_none());
                } else {
                    assert_eq!(persisted.format_version(), 3);
                }
                let old_meta = persisted.network_meta().cloned();
                let state = DaemonState::owned(config(root.path(), false)).unwrap();
                if let Some(meta) = old_meta {
                    assert_eq!(state.network().unwrap().meta, meta);
                }
                if let Some(arrival) = arrival {
                    assert_eq!(state.network().unwrap().meta.bus_id, arrival.bus_id);
                }
            }
        }
    }
}
