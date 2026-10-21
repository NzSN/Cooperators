//! Durable host activation bridge and managed-session adapters.

pub mod adapters;
mod bridge;
mod runtime;

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::arrivals::ArrivalNotice;
use crate::error::BrokerError;

pub use bridge::*;
pub use runtime::*;

pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, BrokerError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    Configured,
    Verified,
    WaitingForPermission,
    Offline,
    Unsupported,
    AttentionRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub binding_id: String,
    pub generation: u64,
    pub bus_id: String,
    pub agent_id: String,
    pub adapter_kind: String,
    pub exact_host_session_id: String,
    pub workspace: String,
    pub capability_status: Readiness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Wake {
    pub wake_id: String,
    pub binding_id: String,
    pub generation: u64,
    pub bus_id: String,
    pub agent_id: String,
    pub notices: Vec<ArrivalNotice>,
    pub observation_nonce: String,
    pub created_at_ms: u64,
}

impl Wake {
    /// Structured references remain available after the broker cursor is ACKed.
    pub fn prompt(&self) -> String {
        format!(
            "Cooperators arrival wake. Handle these references in this exact session. \
             Confirm observation with coop_confirm_activation(binding_id, generation, nonce, wake_id). \
             Read documents with coop_read_doc(name, version); claim queue work with coop_claim_task. \
             Resolve supplied delivery IDs with coop_resolve_arrivals; continue=true when queue work remains. \
             Repeated wake references do not authorize repeated task effects.\n{}",
            serde_json::to_string(self).expect("Wake serialization is infallible")
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostCapabilities {
    pub exact_session: bool,
    pub reports_state: bool,
    pub reconciles_history: bool,
    pub approvals: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Idle,
    Busy,
    WaitingForPermission,
    Unknown,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Submission {
    Accepted { turn_id: Option<String> },
    Busy,
    PermissionBlocked,
    Unknown { detail: String },
    Unsupported { detail: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostApproval {
    pub request_id: Value,
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostEvent {
    State { state: SessionState },
    Approval { approval: HostApproval },
    TurnCompleted { turn_id: Option<String> },
    Notification { value: Value },
    Offline { detail: String },
}

/// Implementations keep duplex transport and per-tool approval handling alive.
/// Successful transport writes never establish agent observation or resolution.
pub trait HostAdapter: Send {
    fn capabilities(&self) -> HostCapabilities;
    fn session_state<'a>(&'a mut self, binding: &'a Binding) -> AdapterFuture<'a, SessionState>;
    fn submit_wake<'a>(
        &'a mut self,
        binding: &'a Binding,
        wake: &'a Wake,
    ) -> AdapterFuture<'a, Submission>;
    fn reconcile_wake<'a>(
        &'a mut self,
        binding: &'a Binding,
        wake: &'a Wake,
        turn_id: Option<&'a str>,
    ) -> AdapterFuture<'a, Submission>;
    fn drain_events(&mut self) -> Vec<HostEvent>;
    /// `response` approves or denies only this outstanding request. No standing
    /// permissions are installed or inferred from a response.
    fn answer_approval<'a>(
        &'a mut self,
        request_id: &'a Value,
        response: Value,
    ) -> AdapterFuture<'a, ()>;
    fn shutdown<'a>(&'a mut self) -> AdapterFuture<'a, ()>;
}
