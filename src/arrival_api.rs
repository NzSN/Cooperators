//! Async broker interface around the synchronous durable arrival journals.

use std::sync::Arc;
use std::time::Duration;

use crate::arrivals::{
    AckArrivalsReceipt, ArrivalResource, ArrivalService, SubscribeArrivalsRequest,
    SubscriptionList, SubscriptionReceipt, UnsubscribeReceipt, WaitArrivals,
};
use crate::error::{BrokerError, ErrorCode};
use crate::registry::AgentIdentity;
use crate::server::DaemonState;

impl DaemonState {
    fn arrival_service(&self) -> Result<Arc<ArrivalService>, BrokerError> {
        self.arrivals.as_ref().cloned().ok_or_else(|| {
            BrokerError::application(
                ErrorCode::Conflict,
                "arrival notifications are not enabled; start the daemon with --enable-arrivals",
            )
        })
    }

    pub async fn subscribe_arrivals(
        &self,
        identity: &AgentIdentity,
        request: SubscribeArrivalsRequest,
    ) -> Result<SubscriptionReceipt, BrokerError> {
        let service = self.arrival_service()?;
        // Move this owned guard into the blocking writer. Canceling the caller
        // must not release source admission between the captured snapshot and
        // the durable subscription record.
        let admission = Arc::clone(&self.send_lock).lock_owned().await;
        let snapshot = match &request.resource {
            ArrivalResource::Document { name } => self.document_arrival_snapshot(name).await?,
            ArrivalResource::Queue { name } => self.queue_arrival_snapshot(name).await?,
        };
        let agent_id = identity.agent_id.clone();
        blocking(move || {
            let _admission = admission;
            service.subscribe(&agent_id, request, snapshot)
        })
        .await
    }

    pub async fn unsubscribe_arrivals(
        &self,
        identity: &AgentIdentity,
        subscription_id: String,
    ) -> Result<UnsubscribeReceipt, BrokerError> {
        let service = self.arrival_service()?;
        let admission = Arc::clone(&self.send_lock).lock_owned().await;
        let agent_id = identity.agent_id.clone();
        blocking(move || {
            let _admission = admission;
            service.unsubscribe(&agent_id, &subscription_id)
        })
        .await
    }

    pub async fn list_arrival_subscriptions(
        &self,
        identity: &AgentIdentity,
    ) -> Result<SubscriptionList, BrokerError> {
        let service = self.arrival_service()?;
        let agent_id = identity.agent_id.clone();
        blocking(move || service.list(&agent_id)).await
    }

    pub(crate) async fn list_arrival_subscriptions_page(
        &self,
        identity: &AgentIdentity,
        after: Option<String>,
        limit: usize,
        bytes: usize,
        allows: impl Fn(&ArrivalResource) -> bool + Send + 'static,
    ) -> Result<(SubscriptionList, Option<String>), BrokerError> {
        let service = self.arrival_service()?;
        let agent_id = identity.agent_id.clone();
        blocking(move || service.list_page(&agent_id, after.as_deref(), limit, bytes, allows)).await
    }

    pub async fn wait_arrivals(
        &self,
        identity: &AgentIdentity,
        cursor: Option<&str>,
        timeout: Duration,
        limit: usize,
    ) -> Result<WaitArrivals, BrokerError> {
        self.arrival_service()?
            .wait(&identity.agent_id, cursor, timeout, limit)
            .await
    }

    pub(crate) async fn wait_arrivals_limited(
        &self,
        identity: &AgentIdentity,
        cursor: Option<&str>,
        timeout: Duration,
        limit: usize,
        bytes: usize,
    ) -> Result<WaitArrivals, BrokerError> {
        self.arrival_service()?
            .wait_limited(&identity.agent_id, cursor, timeout, limit, Some(bytes))
            .await
    }

    pub async fn ack_arrivals(
        &self,
        identity: &AgentIdentity,
        cursor: String,
    ) -> Result<AckArrivalsReceipt, BrokerError> {
        let service = self.arrival_service()?;
        let agent_id = identity.agent_id.clone();
        blocking(move || service.ack(&agent_id, &cursor)).await
    }

    pub async fn shutdown_arrivals(&self) -> Result<(), BrokerError> {
        if let Some(service) = self.arrivals.as_ref().cloned() {
            blocking(move || {
                service.shutdown();
                Ok(())
            })
            .await?;
        }
        Ok(())
    }
}

async fn blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, BrokerError> + Send + 'static,
) -> Result<T, BrokerError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| BrokerError::internal(format!("arrival writer failed: {error}")))?
}
