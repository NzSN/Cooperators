//! Managed launcher integration against a local protocol fake. No provider
//! account, model request, or global client configuration is used.

mod common;

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::process::{Child, Command};

async fn daemon(root: &Path) -> Option<Child> {
    fs::create_dir_all(root).unwrap();
    let probe = root.join("probe.sock");
    match std::os::unix::net::UnixListener::bind(&probe) {
        Ok(listener) => {
            drop(listener);
            fs::remove_file(probe).unwrap();
        }
        Err(error) => {
            assert_ne!(
                std::env::var("COOP_REQUIRE_UDS").as_deref(),
                Ok("1"),
                "required UDS: {error}"
            );
            eprintln!("SKIP host CLI: UDS unavailable: {error}");
            return None;
        }
    }
    let mut child = Command::new(env!("CARGO_BIN_EXE_coop"))
        .args([
            "--root",
            root.to_str().unwrap(),
            "daemon",
            "run",
            "--http-addr",
            "off",
            "--enable-arrivals",
        ])
        .env_remove("COOP_SOCKET")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "daemon exited during startup"
            );
            if cooperators::client::call(root, "ping", json!({}))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("daemon ready");
    Some(child)
}

fn fake(path: &Path) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/host_cli_fake.py");
    symlink(fixture, path).unwrap();
}

fn output_value(bytes: &[u8], kind: &str) -> Value {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|row| row["type"] == kind)
        .unwrap_or_else(|| panic!("missing {kind}: {}", String::from_utf8_lossy(bytes)))
}

#[tokio::test]
async fn managed_cli_runs_and_resumes_exact_binding_without_model_turns() {
    let temp = common::TempDir::new("managed-cli-resume");
    let root = temp.path().join("bus");
    let Some(mut broker) = daemon(&root).await else {
        return;
    };
    let provider = temp.path().join("provider.py");
    fake(&provider);
    let workspace = temp.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let state = temp.path().join("state");
    let config = state.join("resume.json");
    let record = temp.path().join("requests.jsonl");
    let first = Command::new(env!("CARGO_BIN_EXE_coop"))
        .args([
            "--root",
            root.to_str().unwrap(),
            "host",
            "run",
            "--provider",
            "codex",
            "--workspace",
            workspace.to_str().unwrap(),
            "--state-dir",
            state.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "--executable",
            provider.to_str().unwrap(),
            "--env",
            &format!("COOP_CLI_TEST_RECORD={}", record.display()),
            "--shutdown-after",
            "0",
        ])
        .env_remove("COOP_SOCKET")
        .env("COOP_TOKEN", "unrelated-inherited-agent-token")
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stdout)
    );
    let ready = output_value(&first.stdout, "host_ready");
    output_value(&first.stdout, "host_stopped");
    let saved: Value = serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
    assert_eq!(
        fs::metadata(&config).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(ready["status"]["binding"], saved["binding"]);
    let credential = saved["token"].as_str().unwrap();
    assert!(!String::from_utf8_lossy(&first.stdout).contains(credential));
    assert!(!String::from_utf8_lossy(&first.stderr).contains(credential));
    let second = Command::new(env!("CARGO_BIN_EXE_coop"))
        .args([
            "host",
            "resume",
            "--config",
            config.to_str().unwrap(),
            "--shutdown-after",
            "0",
        ])
        .env_remove("COOP_SOCKET")
        .env("COOP_TOKEN", "unrelated-inherited-agent-token")
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stdout)
    );
    let resumed = output_value(&second.stdout, "host_ready");
    assert_eq!(resumed["status"]["binding"], saved["binding"]);
    let requests: Vec<Value> = fs::read_to_string(record)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        requests
            .iter()
            .filter(|row| row["method"] == "thread/start")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|row| row["method"] == "thread/resume")
            .count(),
        2
    );
    assert!(!requests.iter().any(|row| row["method"] == "turn/start"));
    broker.kill().await.unwrap();
    broker.wait().await.unwrap();
}

#[tokio::test]
async fn managed_cli_redacts_bootstrap_error_secrets() {
    let temp = common::TempDir::new("managed-cli-redaction");
    let root = temp.path().join("bus");
    let Some(mut broker) = daemon(&root).await else {
        return;
    };
    let provider = temp.path().join("provider.py");
    fake(&provider);
    let sensitive = "private-test-provider-value-should-not-print";
    let result = Command::new(env!("CARGO_BIN_EXE_coop"))
        .args([
            "--root",
            root.to_str().unwrap(),
            "host",
            "run",
            "--provider",
            "codex",
            "--workspace",
            temp.path().to_str().unwrap(),
            "--state-dir",
            temp.path().join("state").to_str().unwrap(),
            "--executable",
            provider.to_str().unwrap(),
            "--env",
            "COOP_CLI_TEST_FAIL=1",
            "--env",
            &format!("COOP_TEST_SECRET={sensitive}"),
        ])
        .env_remove("COOP_SOCKET")
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(!result.status.success());
    assert!(!String::from_utf8_lossy(&result.stdout).contains(sensitive));
    assert!(!String::from_utf8_lossy(&result.stderr).contains(sensitive));
    let error = output_value(&result.stdout, "host_error");
    assert!(error["error"].as_str().unwrap().contains("[redacted]"));
    broker.kill().await.unwrap();
    broker.wait().await.unwrap();
}
