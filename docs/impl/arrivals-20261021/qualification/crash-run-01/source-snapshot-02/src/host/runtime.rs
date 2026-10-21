//! Async integration seams for the broker receipt pump, MCP proxy and operator.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{watch, Mutex};

use crate::arrivals::{AckArrivalsReceipt, WaitArrivals};
use crate::error::{BrokerError, ErrorCode};
use crate::id;

use super::{
    ActivationReceipt, AdapterFuture, BridgeAction, ClaimAttempt, ClaimResult,
    ConfirmActivationRequest, HostAdapter, HostBridge, HostEvent, ResolutionReceipt,
    ResolveArrivalsRequest, SessionState, Submission,
};

pub type SharedHostBridge = Arc<Mutex<HostBridge>>;

pub async fn resolve_authenticated(
    bridge: &SharedHostBridge,
    bus_id: &str,
    agent_id: &str,
    request: ResolveArrivalsRequest,
) -> Result<ResolutionReceipt, BrokerError> {
    let mut bridge = bridge.lock().await;
    bridge.authenticate_binding(bus_id, agent_id)?;
    bridge.resolve(request)
}

pub async fn confirm_authenticated(
    bridge: &SharedHostBridge,
    bus_id: &str,
    agent_id: &str,
    request: ConfirmActivationRequest,
    now_ms: u64,
) -> Result<ActivationReceipt, BrokerError> {
    let mut bridge = bridge.lock().await;
    bridge.authenticate_binding(bus_id, agent_id)?;
    bridge.confirm_activation(request, now_ms)
}

/// Called by the trusted MCP proxy before submitting claim_task. Normal task
/// calls outside the current wake return None and create no drain evidence.
pub async fn claim_context(
    bridge: &SharedHostBridge,
    bus_id: &str,
    agent_id: &str,
    queue: &str,
) -> Result<Option<ClaimAttempt>, BrokerError> {
    let mut bridge = bridge.lock().await;
    bridge.authenticate_binding(bus_id, agent_id)?;
    let Some(wake) = bridge.active_wake() else {
        return Ok(None);
    };
    if !wake.notices.iter().any(|notice| {
        matches!(&notice.resource,
        crate::arrivals::ArrivalReference::Queue { name, .. } if name == queue)
    }) {
        return Ok(None);
    }
    let wake_id = wake.wake_id.clone();
    let generation = wake.generation;
    bridge.begin_claim(&wake_id, generation, queue).map(Some)
}

/// Called only with the actual broker result. Empty is reserved for the
/// authoritative NOT_FOUND result, not a model assertion or transport error.
pub async fn observe_claim_authenticated(
    bridge: &SharedHostBridge,
    bus_id: &str,
    agent_id: &str,
    context: ClaimAttempt,
    outcome: ClaimResult,
) -> Result<(), BrokerError> {
    let mut bridge = bridge.lock().await;
    bridge.authenticate_binding(bus_id, agent_id)?;
    bridge.finish_claim_context(&context, outcome)
}

/// Ordinary authenticated broker-client operations; adapters do not open any
/// broker files. Implementable by deterministic transports in acceptance tests.
pub trait BrokerArrivalClient: Send + Sync {
    fn wait<'a>(&'a self, cursor: Option<&'a str>) -> AdapterFuture<'a, WaitArrivals>;
    fn ack<'a>(&'a self, cursor: &'a str) -> AdapterFuture<'a, ()>;
}

/// Credentials remain in this client, outside bridge records and notices.
#[derive(Clone)]
pub struct UdsArrivalClient {
    pub root: PathBuf,
    token: String,
    bus_id: String,
    agent_id: String,
}

impl UdsArrivalClient {
    pub fn new(root: PathBuf, token: String, bus_id: String, agent_id: String) -> Self {
        Self {
            root,
            token,
            bus_id,
            agent_id,
        }
    }
}

impl BrokerArrivalClient for UdsArrivalClient {
    fn wait<'a>(&'a self, cursor: Option<&'a str>) -> AdapterFuture<'a, WaitArrivals> {
        Box::pin(async move {
            let value = crate::client::call_with_timeout(
                &self.root,
                "wait_arrivals",
                json!({"token": self.token, "cursor": cursor, "timeout_s": 30, "limit": 64}),
                Duration::from_secs(35),
            )
            .await
            .map_err(client_error)?;
            let page: WaitArrivals = serde_json::from_value(value)
                .map_err(|error| BrokerError::internal(format!("invalid arrival page: {error}")))?;
            if page.bus_id != self.bus_id || page.agent_id != self.agent_id {
                return Err(BrokerError::invalid(
                    "broker arrival page belongs to another binding",
                ));
            }
            Ok(page)
        })
    }

    fn ack<'a>(&'a self, cursor: &'a str) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            let value = crate::client::call(
                &self.root,
                "ack_arrivals",
                json!({"token": self.token, "cursor": cursor}),
            )
            .await
            .map_err(client_error)?;
            let receipt: AckArrivalsReceipt = serde_json::from_value(value)
                .map_err(|error| BrokerError::internal(format!("invalid arrival ACK: {error}")))?;
            if receipt.bus_id != self.bus_id
                || receipt.agent_id != self.agent_id
                || receipt.cursor != cursor
            {
                return Err(BrokerError::invalid(
                    "broker ACK does not match the durable received cursor",
                ));
            }
            Ok(())
        })
    }
}

fn client_error(error: crate::client::ClientError) -> BrokerError {
    match error {
        crate::client::ClientError::Unreachable(_) | crate::client::ClientError::Timeout => {
            BrokerError::application(ErrorCode::DaemonUnreachable, error.to_string())
        }
        crate::client::ClientError::Rpc(error) => match ErrorCode::from_rpc_code(error.code) {
            Some(code) => BrokerError::application(code, error.message),
            None => {
                BrokerError::invalid(format!("broker arrival request failed: {}", error.message))
            }
        },
        crate::client::ClientError::Protocol(detail) => BrokerError::invalid(detail),
    }
}

/// Runs independently of model execution and approval prompts. A durable
/// receive always precedes its ACK; restart retries an uncertain ACK before
/// continuing from the already received cursor.
pub async fn run_receipt_loop(
    bridge: SharedHostBridge,
    broker: Arc<dyn BrokerArrivalClient>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), BrokerError> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let cursor = bridge.lock().await.received_cursor().map(str::to_owned);
        let operation = async {
            if let Some(cursor) = &cursor {
                broker.ack(cursor).await?;
            }
            let page = broker.wait(cursor.as_deref()).await?;
            let has_notices = !page.notices.is_empty();
            let receipt = bridge.lock().await.receive(
                &page.bus_id,
                &page.agent_id,
                page.notices,
                page.next_cursor,
                id::unix_millis(),
            )?;
            if has_notices {
                broker.ack(&receipt.cursor).await?;
            }
            Ok::<(), BrokerError>(())
        };
        let result = tokio::select! {
            result = operation => result,
            _ = shutdown.changed() => {
                if *shutdown.borrow() { return Ok(()); }
                continue;
            }
        };
        if let Err(error) = result {
            if matches!(
                error,
                BrokerError::Application(ErrorCode::DaemonUnreachable, _)
            ) {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    _ = shutdown.changed() => { if *shutdown.borrow() { return Ok(()); } }
                }
                continue;
            }
            // Journal/full/cursor/protocol errors require operator attention;
            // they must never be treated as a new empty inbox.
            let _ = bridge.lock().await.attention(error.to_string());
            return Err(error);
        }
    }
}

/// One non-overlapping scheduler step. The caller remains responsible for
/// multiplexing host-control IPC, operator responses, and shutdown.
pub struct BridgeRunner {
    pub bridge: SharedHostBridge,
    adapter: Box<dyn HostAdapter>,
}

impl BridgeRunner {
    pub fn new(bridge: SharedHostBridge, adapter: Box<dyn HostAdapter>) -> Self {
        Self { bridge, adapter }
    }

    pub async fn tick(&mut self, now_ms: u64) -> Result<Vec<HostEvent>, BrokerError> {
        let mut events = self.adapter.drain_events();
        for event in &events {
            let state = match event {
                HostEvent::State { state } => Some(*state),
                HostEvent::Approval { .. } => Some(SessionState::WaitingForPermission),
                HostEvent::TurnCompleted { .. } => Some(SessionState::Idle),
                HostEvent::Offline { .. } => Some(SessionState::Offline),
                HostEvent::Notification { .. } => None,
            };
            if let Some(state) = state {
                self.bridge.lock().await.set_session_state(state, now_ms)?;
            }
        }
        let binding = self.bridge.lock().await.binding().clone();
        if !self.adapter.capabilities().exact_session {
            self.bridge
                .lock()
                .await
                .attention("adapter cannot prove exact session routing")?;
            return Ok(events);
        }
        let state = match self.adapter.session_state(&binding).await {
            Ok(state) => state,
            Err(error) => {
                self.bridge
                    .lock()
                    .await
                    .set_session_state(SessionState::Offline, now_ms)?;
                events.push(HostEvent::Offline {
                    detail: error.to_string(),
                });
                return Ok(events);
            }
        };
        let action = {
            let mut bridge = self.bridge.lock().await;
            if bridge.binding() != &binding {
                // Control may have fenced this generation while the host
                // query was in flight. Query the selected binding next tick.
                return Ok(events);
            }
            bridge.set_session_state(state, now_ms)?;
            bridge.next_action(now_ms)?
        };
        match action {
            Some(BridgeAction::Submit(wake)) => {
                let submission = self
                    .adapter
                    .submit_wake(&binding, &wake)
                    .await
                    .unwrap_or_else(|error| Submission::Unknown {
                        detail: error.to_string(),
                    });
                self.bridge
                    .lock()
                    .await
                    .record_submission(&wake.wake_id, submission, now_ms)?;
            }
            Some(BridgeAction::Reconcile { wake, turn_id }) => {
                let submission = self
                    .adapter
                    .reconcile_wake(&binding, &wake, turn_id.as_deref())
                    .await
                    .unwrap_or_else(|error| Submission::Unknown {
                        detail: error.to_string(),
                    });
                self.bridge
                    .lock()
                    .await
                    .record_reconciliation(&wake.wake_id, submission)?;
            }
            None => {}
        }
        events.extend(self.adapter.drain_events());
        Ok(events)
    }

    pub async fn answer_approval(
        &mut self,
        request_id: &Value,
        response: Value,
    ) -> Result<(), BrokerError> {
        self.adapter.answer_approval(request_id, response).await
    }

    pub async fn shutdown(&mut self) -> Result<(), BrokerError> {
        self.adapter.shutdown().await
    }
}
