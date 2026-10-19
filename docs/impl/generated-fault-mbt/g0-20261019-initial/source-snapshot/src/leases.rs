//! Shared lease renewal and the background expiry sweeper.
//!
//! Queue tasks and document edit claims use the same lease concept. A lease
//! id is globally unique, so `renew_lease` can search queues and documents in
//! one call without asking the client which kind it holds.

use std::sync::Arc;
use std::time::Duration;

use crate::docs::DocRuntime;
use crate::error::{BrokerError, ErrorCode};
use crate::queues::{QueueRuntime, RenewLeaseReceipt};
use crate::registry::AgentIdentity;
use crate::server::DaemonState;

impl DaemonState {
    /// Renews a queue or document lease. Expired leases return `LEASE_LOST`.
    pub async fn renew_lease(
        &self,
        identity: &AgentIdentity,
        lease_id: &str,
    ) -> Result<RenewLeaseReceipt, BrokerError> {
        let _send_guard = self.send_lock.lock().await;

        let queue = {
            let states: tokio::sync::MutexGuard<
                '_,
                std::collections::HashMap<String, QueueRuntime>,
            > = self.queue_states.lock().await;
            states.iter().find_map(|(name, runtime)| {
                runtime
                    .tasks
                    .values()
                    .any(|task| {
                        task.phase == crate::queues::TaskPhase::Leased
                            && task
                                .lease
                                .as_ref()
                                .is_some_and(|lease| lease.lease_id == lease_id)
                    })
                    .then(|| name.clone())
            })
        };
        if let Some(queue) = queue {
            return self.renew_queue_lease(identity, &queue, lease_id).await;
        }

        let doc = {
            let states: tokio::sync::MutexGuard<'_, std::collections::HashMap<String, DocRuntime>> =
                self.doc_states.lock().await;
            states.iter().find_map(|(name, runtime)| {
                runtime
                    .claim
                    .as_ref()
                    .is_some_and(|claim| claim.lease_id == lease_id)
                    .then(|| name.clone())
            })
        };
        if let Some(doc) = doc {
            return self.renew_doc_lease(identity, &doc, lease_id).await;
        }

        Err(BrokerError::application(
            ErrorCode::LeaseLost,
            "unknown or expired lease",
        ))
    }

    /// Runs both queue and document expiry passes once.
    pub async fn sweep_expired_leases(&self) -> Result<u64, BrokerError> {
        let queues = self.sweep_expired_queues().await?;
        let docs = self.sweep_expired_docs().await?;
        Ok(queues + docs)
    }

    /// Starts a background sweep for expired queue leases and document
    /// claims. The interval is read from `DaemonConfig`.
    pub fn spawn_lease_sweeper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let state = Arc::clone(self);
        let interval = self
            .config
            .lease_sweep_interval
            .max(Duration::from_millis(10));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if let Err(error) = state.sweep_expired_leases().await {
                    eprintln!("coop daemon: lease sweep failed: {error}");
                }
            }
        })
    }
}
