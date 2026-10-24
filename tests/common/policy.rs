/// Approve a changed test policy at the next durable revision.
pub async fn replace_network_policy(
    state: &cooperators::server::DaemonState,
    mut policy: cooperators::network::NetworkPolicy,
) -> Result<(), cooperators::error::BrokerError> {
    let runtime = state.network().unwrap();
    policy.revision = runtime.policy().revision + 1;
    runtime.replace_policy(state, policy).await
}
