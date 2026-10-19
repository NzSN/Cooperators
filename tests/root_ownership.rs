mod common;

use cooperators::server::{DaemonConfig, DaemonState};
use std::sync::Arc;

#[tokio::test]
async fn background_writer_retains_root_ownership_after_listener_state_drops() {
    let temp = common::TempDir::new("ownership-worker");
    let config = DaemonConfig::new(temp.path());
    let state = Arc::new(DaemonState::owned(config.clone()).unwrap());
    let worker = state.spawn_presence_sweeper();
    drop(state);
    let failure = DaemonState::owned(config.clone()).unwrap_err();
    assert!(
        matches!(failure, cooperators::error::BrokerError::Io(ref error)
        if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    worker.abort();
    let _ = worker.await;
    let successor = DaemonState::owned(config).unwrap();
    drop(successor);
    assert!(temp.path().join("run/owner.lock").is_file());
}
