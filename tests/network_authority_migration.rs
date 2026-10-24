mod common;
#[path = "common/network.rs"]
mod net;
use cooperators::registry::{new_token, Registry};
use cooperators::server::{DaemonConfig, DaemonState};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

fn config(root: &Path, device: &str, arrivals: bool) -> DaemonConfig {
    let mut config = DaemonConfig::new(root);
    config.enable_arrivals = arrivals;
    config.network_policy = Some(net::policy(device));
    config
}
fn read(path: impl AsRef<Path>) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

#[tokio::test]
async fn upgrades_in_both_orders_preserve_bus_identity_and_historical_registry_fields() {
    for arrivals_first in [false, true] {
        let temp = common::TempDir::new("network-migration");
        let root = temp.path().join("root");
        let device = new_token().unwrap();
        let mut baseline = DaemonConfig::new(&root);
        baseline.enable_arrivals = arrivals_first;
        let state = DaemonState::owned(baseline).unwrap();
        let local = state
            .register_agent("local".into(), None, None, None)
            .await
            .unwrap();
        state.shutdown_arrivals().await.unwrap();
        drop(state);
        let old = read(root.join("registry.json"));
        let arrival_id =
            arrivals_first.then(|| read(root.join("notifications/meta.json"))["bus_id"].clone());
        let state = DaemonState::owned(config(&root, &device, false)).unwrap();
        assert!(state.authenticate(&local.token).await.is_ok());
        let bus = state.network().unwrap().meta.bus_id.clone();
        if let Some(arrival) = arrival_id {
            assert_eq!(bus, arrival.as_str().unwrap());
        }
        state.shutdown_arrivals().await.unwrap();
        drop(state);
        let upgraded = read(root.join("registry.json"));
        assert_eq!(upgraded["v"], 3);
        assert_eq!(old["agents"], upgraded["agents"]);
        assert!(upgraded["required_features"]
            .as_array()
            .unwrap()
            .contains(&json!("network_authority_v1")));
        let state = DaemonState::owned(config(&root, &device, true)).unwrap();
        assert_eq!(state.network().unwrap().meta.bus_id, bus);
        assert_eq!(read(root.join("notifications/meta.json"))["bus_id"], bus);
        assert!(Registry::load(&root).unwrap().arrivals_enabled());
        state.shutdown_arrivals().await.unwrap();
        drop(state);
        assert_eq!(
            read(root.join("registry.json"))["network"],
            upgraded["network"]
        );
    }
}

#[tokio::test]
async fn activated_root_refuses_missing_policy_before_repair_and_invalid_policy_before_activation()
{
    let temp = common::TempDir::new("network-reopen-guard");
    let root = temp.path().join("root");
    let device = new_token().unwrap();
    let state = DaemonState::owned(config(&root, &device, false)).unwrap();
    drop(state);
    let source = root.join("channels/torn.jsonl");
    fs::write(&source, b"{\"torn\":").unwrap();
    let before = fs::read(root.join("registry.json")).unwrap();
    assert!(DaemonState::owned(DaemonConfig::new(&root)).is_err());
    assert_eq!(fs::read(&source).unwrap(), b"{\"torn\":");
    assert_eq!(fs::read(root.join("registry.json")).unwrap(), before);
    let fresh = temp.path().join("fresh");
    let mut invalid = config(&fresh, &device, true);
    invalid.network_policy.as_mut().unwrap().v = 99;
    assert!(DaemonState::owned(invalid).is_err());
    assert!(!fresh.join("registry.json").exists());
    assert!(!fresh.join("notifications/meta.json").exists());
}

#[tokio::test]
async fn orphan_metadata_is_adopted_and_conflicts_do_not_promote_arrivals() {
    let temp = common::TempDir::new("network-orphan");
    let root = temp.path().join("root");
    let device = new_token().unwrap();
    fs::create_dir_all(root.join("notifications")).unwrap();
    let bus = cooperators::id::new_ulid().unwrap();
    fs::write(
        root.join("notifications/meta.json"),
        json!({"v":1,"bus_id":bus}).to_string(),
    )
    .unwrap();
    let state = DaemonState::owned(config(&root, &device, false)).unwrap();
    assert_eq!(state.network().unwrap().meta.bus_id, bus);
    drop(state);
    let before = fs::read(root.join("registry.json")).unwrap();
    fs::write(
        root.join("notifications/meta.json"),
        json!({"v":1,"bus_id":cooperators::id::new_ulid().unwrap()}).to_string(),
    )
    .unwrap();
    assert!(DaemonState::owned(config(&root, &device, true)).is_err());
    assert_eq!(fs::read(root.join("registry.json")).unwrap(), before);
}

#[tokio::test]
async fn enrollment_expiry_and_budget_survive_reopen_without_revival() {
    let f = net::Fixture::new().await;
    let root = f.temp.path().join("root");
    let mut policy = f.state.network().unwrap().policy();
    policy.devices[0].max_instances = 1;
    policy.revision += 1;
    f.state.shutdown_arrivals().await.unwrap();
    drop(f.state);
    let mut registry = read(root.join("registry.json"));
    let record = registry["agents"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .find(|a| a["agent_id"] == f.agent_id)
        .unwrap();
    record["network_grant"]["expires_at_ms"] = json!(0);
    fs::write(
        root.join("registry.json"),
        serde_json::to_vec(&registry).unwrap(),
    )
    .unwrap();
    let mut cfg = config(&root, &f.device, true);
    cfg.network_policy = Some(policy);
    let state = DaemonState::owned(cfg).unwrap();
    assert!(state.authenticate(&f.instance).await.is_err());
    assert!(state
        .network()
        .unwrap()
        .enroll(
            &state,
            &f.device,
            json!({"launch_key":f.instance,"kind":"agent","display_name":"reader"})
        )
        .await
        .is_err());
    assert!(state
        .network()
        .unwrap()
        .enroll(
            &state,
            &f.device,
            json!({"launch_key":new_token().unwrap(),"kind":"agent","display_name":"excess"})
        )
        .await
        .is_err());
    state.shutdown_arrivals().await.unwrap();
}

#[tokio::test]
async fn actual_pre_network_binary_refuses_v3_before_any_source_repair() {
    let Some(program) = std::env::var_os("COOP_PRENETWORK_BINARY") else {
        assert!(
            std::env::var_os("COOP_REQUIRE_REMOTE").is_none(),
            "pre-network binary required for migration gate"
        );
        eprintln!("SKIP pre-network executable refusal: COOP_PRENETWORK_BINARY unset");
        return;
    };
    let fixture: Value = serde_json::from_str(include_str!(
        "../docs/impl/networks-20261022/N0-baseline/binary.json"
    ))
    .unwrap();
    assert_eq!(
        cooperators::storage::sha256_hex(&fs::read(&program).unwrap()),
        fixture["sha256"].as_str().unwrap()
    );
    let temp = common::TempDir::new("network-old-binary");
    let root = temp.path().join("root");
    let state = DaemonState::owned(config(&root, &new_token().unwrap(), false)).unwrap();
    drop(state);
    let sources = [
        "channels/guard.jsonl",
        "queues/guard.jsonl",
        "docs/guard.jsonl",
    ];
    for path in sources {
        fs::write(root.join(path), b"{\"torn\":").unwrap();
    }
    let registry_before = fs::read(root.join("registry.json")).unwrap();
    let mut child = std::process::Command::new(program)
        .arg("--root")
        .arg(&root)
        .args(["daemon", "run", "--http-addr", "off"])
        .env_remove("COOP_SOCKET")
        .env_remove("COOP_TOKEN")
        .env_remove("COOP_ENDPOINT")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("old executable failed to refuse V3");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(!status.success());
    for path in sources {
        assert_eq!(fs::read(root.join(path)).unwrap(), b"{\"torn\":");
    }
    assert_eq!(
        fs::read(root.join("registry.json")).unwrap(),
        registry_before
    );
}

#[cfg(feature = "mbt-injection")]
#[test]
fn network_activation_crash_child() {
    let Some(root) = std::env::var_os("COOP_NETWORK_CRASH_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let phase = std::env::var("COOP_NETWORK_CRASH_PHASE").unwrap();
    let point = if phase == "before" {
        cooperators::faults::FaultPoint::BeforeRegistryRename
    } else {
        cooperators::faults::FaultPoint::AfterRegistryRename
    };
    let hook = cooperators::faults::arm_point(
        root.join("registry.json"),
        point,
        cooperators::faults::PointFault::Pause,
    )
    .unwrap();
    let child_root = root.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();
        let _state = DaemonState::owned(config(&child_root, &new_token().unwrap(), false)).unwrap();
    });
    hook.wait_reached(Duration::from_secs(10)).unwrap();
    fs::write(root.join("reached"), phase).unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(feature = "mbt-injection")]
#[tokio::test]
async fn real_process_kill_at_registry_replacement_recovers_one_stable_authority() {
    use std::os::unix::process::ExitStatusExt;
    for phase in ["before", "after"] {
        let temp = common::TempDir::new("network-activation-kill");
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        let mut registry = Registry::load(&root).unwrap();
        registry
            .register("legacy".into(), None, None, None)
            .unwrap();
        drop(registry);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "network_activation_crash_child", "--nocapture"])
            .env("COOP_NETWORK_CRASH_ROOT", &root)
            .env("COOP_NETWORK_CRASH_PHASE", phase)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !root.join("reached").exists() {
            if Instant::now() > deadline || child.try_wait().unwrap().is_some() {
                let _ = child.kill();
                let _ = child.wait();
                panic!("activation crash hook not reached");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let before = read(root.join("registry.json"));
        assert_eq!(before["v"], if phase == "before" { 1 } else { 3 });
        child.kill().unwrap();
        assert_eq!(child.wait().unwrap().signal(), Some(9));
        let device = new_token().unwrap();
        let state = DaemonState::owned(config(&root, &device, false)).unwrap();
        let meta = state.network().unwrap().meta.clone();
        drop(state);
        if phase == "after" {
            assert_eq!(before["network"], serde_json::to_value(&meta).unwrap());
        }
        let state = DaemonState::owned(config(&root, &device, false)).unwrap();
        assert_eq!(state.network().unwrap().meta, meta);
    }
}

#[tokio::test]
async fn policy_revision_and_generation_fences_survive_restart_and_reject_stale_approval() {
    let f = net::Fixture::new().await;
    let root = f.temp.path().join("root");
    let original = f.state.network().unwrap().policy();
    let mut revoked = original.clone();
    revoked.devices.clear();
    net::replace_policy(&f.state, revoked).await.unwrap();
    let current = f.state.network().unwrap().policy();
    let bytes = fs::read(root.join("registry.json")).unwrap();
    f.state.shutdown_arrivals().await.unwrap();
    drop(f.state);
    let mut cfg = config(&root, &f.device, true);
    cfg.network_policy = Some(original.clone());
    assert!(DaemonState::owned(cfg.clone()).is_err());
    assert_eq!(fs::read(root.join("registry.json")).unwrap(), bytes);
    let mut restore = original.clone();
    restore.revision = current.revision + 1;
    cfg.network_policy = Some(restore.clone());
    assert!(
        DaemonState::owned(cfg.clone()).is_err(),
        "a newer policy revision must not revive an old principal generation"
    );
    restore.devices[0].generation += 1;
    cfg.network_policy = Some(restore.clone());
    let state = DaemonState::owned(cfg).unwrap();
    assert!(cooperators::network::call_remote(
        &std::sync::Arc::new(state),
        &f.instance,
        "ping",
        json!({})
    )
    .await
    .is_err());
    let registry = read(root.join("registry.json"));
    assert!(registry["required_features"]
        .as_array()
        .unwrap()
        .contains(&json!("network_policy_revisions_v1")));
    assert_eq!(
        registry["network_authority"]["policy"]["revision"],
        restore.revision
    );
}

#[cfg(feature = "mbt-injection")]
#[tokio::test]
async fn uncertain_policy_replacement_fails_closed_until_reopen() {
    use cooperators::faults::{arm_point, FaultPoint, PointFault};
    for point in [
        FaultPoint::BeforeRegistryRename,
        FaultPoint::AfterRegistryRename,
    ] {
        let f = net::Fixture::new().await;
        let path = f.temp.path().join("root/registry.json");
        let hook = arm_point(&path, point, PointFault::Fail).unwrap();
        let mut policy = f.state.network().unwrap().policy();
        policy.devices[0].generation += 1;
        assert!(net::replace_policy(&f.state, policy).await.is_err());
        drop(hook);
        assert!(
            cooperators::network::call_remote(&f.state, &f.instance, "ping", json!({}))
                .await
                .is_err()
        );
        let disk = read(&path)["network_authority"]["policy"].clone();
        let policy: cooperators::network::NetworkPolicy = serde_json::from_value(disk).unwrap();
        let mut cfg = config(f.temp.path().join("root").as_path(), &f.device, true);
        cfg.network_policy = Some(policy.clone());
        f.state.shutdown_arrivals().await.unwrap();
        drop(f.state);
        let state = std::sync::Arc::new(DaemonState::owned(cfg).unwrap());
        let result =
            cooperators::network::call_remote(&state, &f.instance, "ping", json!({})).await;
        if point == FaultPoint::AfterRegistryRename {
            assert!(result.is_err());
        } else {
            assert!(result.is_ok());
        }
        state.shutdown_arrivals().await.unwrap();
    }
}

/// Each parent persisted a different policy fence. Both formats must retain
/// revoked principals and the immutable operation epoch when first combined.
#[tokio::test]
async fn both_parent_registry_formats_preserve_tombstones_and_operation_epoch() {
    for authority_only in [false, true] {
        let f = net::Fixture::new().await;
        let root = f.temp.path().join("root");
        let original = f.state.network().unwrap().policy();
        let meta = f.state.network().unwrap().meta.clone();
        let mut removed = original.clone();
        removed.devices.clear();
        net::replace_policy(&f.state, removed).await.unwrap();
        f.state.shutdown_arrivals().await.unwrap();
        drop(f.state);

        let path = root.join("registry.json");
        let mut registry = read(&path);
        if authority_only {
            registry
                .as_object_mut()
                .unwrap()
                .remove("network_principals");
        } else {
            registry
                .as_object_mut()
                .unwrap()
                .remove("network_authority");
            registry["required_features"]
                .as_array_mut()
                .unwrap()
                .retain(|feature| feature != "network_policy_revisions_v1");
        }
        fs::write(&path, serde_json::to_vec(&registry).unwrap()).unwrap();
        let before = fs::read(&path).unwrap();
        let mut restored = original;
        restored.revision = 3;
        let mut cfg = config(&root, &f.device, true);
        cfg.network_policy = Some(restored.clone());
        assert!(
            DaemonState::owned(cfg.clone()).is_err(),
            "parent format revived revoked generation"
        );
        assert_eq!(fs::read(&path).unwrap(), before);

        restored.devices[0].generation += 1;
        // Operation activation is a newly approved change on either format.
        restored.enable_operations = true;
        cfg.network_policy = Some(restored.clone());
        let state = std::sync::Arc::new(DaemonState::owned(cfg.clone()).unwrap());
        let upgraded_meta = state.network().unwrap().meta.clone();
        assert_eq!(upgraded_meta.bus_id, meta.bus_id);
        assert_eq!(upgraded_meta.operation_epoch, meta.operation_epoch);
        assert!(upgraded_meta.operation_epoch_expires_at_ms.is_some());
        assert!(
            cooperators::network::call_remote(&state, &f.instance, "ping", json!({}))
                .await
                .is_err()
        );
        let upgraded = read(&path);
        assert_eq!(upgraded["network_principals"]["device-a"]["generation"], 2);
        assert_eq!(upgraded["network_authority"]["policy"]["revision"], 3);
        state.shutdown_arrivals().await.unwrap();
        drop(state);
        let reopened = DaemonState::owned(cfg).unwrap();
        assert_eq!(reopened.network().unwrap().meta, upgraded_meta);
        reopened.shutdown_arrivals().await.unwrap();
    }
}
