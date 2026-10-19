mod common;

use cooperators::server::DaemonState;

/// This baseline documents the existing shared-configuration behavior. It is
/// not an MBT result: callers presenting one credential are one identity.
#[tokio::test]
async fn legacy_shared_credential_observes_one_identity() {
    let temp = common::TempDir::new("shared-credential-baseline");
    let state = DaemonState::new(temp.path().to_path_buf());
    let registered = state
        .register_agent("codex".into(), Some("codex".into()), None, None)
        .await
        .expect("register shared configuration");
    let first = state.authenticate(&registered.token).await.expect("first client");
    let second = state.authenticate(&registered.token).await.expect("second client");
    assert_eq!(first.agent_id, second.agent_id);
    let raw: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("registry.json")).expect("actual registry"),
    )
    .expect("registry JSON");
    assert_eq!(raw["agents"].as_object().expect("agent records").len(), 1);
    eprintln!("baseline: two clients using one configured token authenticate as {}; registry records=1", first.agent_id);
}
