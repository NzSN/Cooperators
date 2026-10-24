//! Presence tracking and transition events (design §6).
//!
//! Any authenticated call refreshes the agent's last-seen timestamp. A
//! background sweep (or an explicit call from tests and `list_agents`) moves
//! agents from `active` to `stale` after `stale_after` silence and from
//! `stale` to `gone` after `gone_after` silence. Every transition is appended
//! to `channels/#presence.jsonl` as an ordinary envelope so supervisors can
//! tail one log.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::envelope::Body;
use crate::error::BrokerError;
use crate::id;
use crate::messaging::DAEMON_SENDER;
use crate::registry::{AgentIdentity, AgentRecord};
use crate::server::DaemonState;

/// Presence state exposed by `list_agents` and transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentState {
    Active,
    Stale,
    Gone,
}

impl AgentState {
    /// Stable lowercase name used in JSON and log kinds.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Stale => "stale",
            Self::Gone => "gone",
        }
    }
}

#[derive(Debug, Clone)]
struct PresenceEntry {
    pub name: String,
    pub kind: String,
    pub state: AgentState,
    pub last_seen_ms: u64,
}

/// In-memory presence table keyed by stable agent id.
#[derive(Debug, Default)]
pub(crate) struct Presence {
    entries: HashMap<String, PresenceEntry>,
}

/// Read-only presence counts used by `/metrics`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct AgentStateCounts {
    pub active: usize,
    pub stale: usize,
    pub gone: usize,
}

#[derive(Debug, Clone)]
struct Transition {
    agent_id: String,
    name: String,
    kind: String,
    from: AgentState,
    to: AgentState,
    last_seen_ms: u64,
}

/// One row of the `list_agents` result.
#[derive(Debug, Clone, Serialize)]
pub struct AgentView {
    pub agent_id: String,
    pub name: String,
    pub kind: String,
    pub capabilities: Vec<String>,
    pub state: AgentState,
    pub last_seen: String,
    pub registered_at: String,
}

/// Response returned by `heartbeat`.
#[derive(Debug, Clone, Serialize)]
pub struct HeartbeatResponse {
    pub agent_id: String,
    pub state: AgentState,
    pub last_seen: String,
}

impl Presence {
    /// Seeds an entry for a registry record at daemon start. Presence is
    /// ephemeral across restarts; agents get one full stale window to
    /// heartbeat again.
    pub(crate) fn seed(&mut self, name: &str, record: &AgentRecord, now_ms: u64) {
        self.entries.insert(
            record.agent_id.clone(),
            PresenceEntry {
                name: name.to_string(),
                kind: record.kind.clone(),
                state: AgentState::Active,
                last_seen_ms: now_ms,
            },
        );
    }

    /// Inserts or refreshes an agent, reporting a transition when a stale or
    /// gone agent becomes active again.
    fn register(&mut self, identity: &AgentIdentity, now_ms: u64) -> Option<Transition> {
        match self.entries.get_mut(&identity.agent_id) {
            Some(entry) => {
                entry.name = identity.name.clone();
                entry.kind = identity.kind.clone();
                entry.last_seen_ms = now_ms;
                if entry.state == AgentState::Active {
                    None
                } else {
                    let from = entry.state;
                    entry.state = AgentState::Active;
                    Some(Transition {
                        agent_id: identity.agent_id.clone(),
                        name: identity.name.clone(),
                        kind: identity.kind.clone(),
                        from,
                        to: AgentState::Active,
                        last_seen_ms: now_ms,
                    })
                }
            }
            None => {
                self.entries.insert(
                    identity.agent_id.clone(),
                    PresenceEntry {
                        name: identity.name.clone(),
                        kind: identity.kind.clone(),
                        state: AgentState::Active,
                        last_seen_ms: now_ms,
                    },
                );
                None
            }
        }
    }

    fn touch(&mut self, identity: &AgentIdentity, now_ms: u64) -> Option<Transition> {
        self.register(identity, now_ms)
    }

    /// Counts agents by the state they would have at `now_ms` without
    /// mutating presence or emitting transitions.
    pub(crate) fn counts(
        &self,
        now_ms: u64,
        stale_after_ms: u64,
        gone_after_ms: u64,
    ) -> AgentStateCounts {
        let mut counts = AgentStateCounts::default();
        for entry in self.entries.values() {
            let silence = now_ms.saturating_sub(entry.last_seen_ms);
            if silence >= gone_after_ms {
                counts.gone += 1;
            } else if silence >= stale_after_ms {
                counts.stale += 1;
            } else {
                counts.active += 1;
            }
        }
        counts
    }

    fn classify(
        &mut self,
        now_ms: u64,
        stale_after_ms: u64,
        gone_after_ms: u64,
    ) -> Vec<Transition> {
        let mut transitions = Vec::new();
        for (agent_id, entry) in &mut self.entries {
            let silence = now_ms.saturating_sub(entry.last_seen_ms);
            let target = if silence >= gone_after_ms {
                AgentState::Gone
            } else if silence >= stale_after_ms {
                AgentState::Stale
            } else {
                AgentState::Active
            };
            if target != entry.state {
                transitions.push(Transition {
                    agent_id: agent_id.clone(),
                    name: entry.name.clone(),
                    kind: entry.kind.clone(),
                    from: entry.state,
                    to: target,
                    last_seen_ms: entry.last_seen_ms,
                });
                entry.state = target;
            }
        }
        transitions.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        transitions
    }
}

impl DaemonState {
    /// Refreshes presence for an authenticated identity.
    pub(crate) async fn touch_presence(&self, identity: &AgentIdentity) {
        let transition = {
            let mut presence = self.presence.lock().await;
            presence.touch(identity, id::unix_millis())
        };
        if let Some(transition) = transition {
            if let Err(error) = self.emit_presence(transition).await {
                eprintln!("coop daemon: failed to emit presence transition: {error}");
            }
        }
    }

    /// Registers presence for a newly registered or rotated agent.
    pub(crate) async fn register_presence(
        &self,
        identity: &AgentIdentity,
    ) -> Result<(), BrokerError> {
        let transition = {
            let mut presence = self.presence.lock().await;
            presence.register(identity, id::unix_millis())
        };
        if let Some(transition) = transition {
            self.emit_presence(transition).await?;
        }
        Ok(())
    }

    /// Classifies all agents and emits any state transitions.
    pub async fn sweep_presence(&self) -> Result<(), BrokerError> {
        let stale_after_ms = u64::try_from(self.config.stale_after.as_millis()).unwrap_or(u64::MAX);
        let gone_after_ms = u64::try_from(self.config.gone_after.as_millis()).unwrap_or(u64::MAX);
        let transitions = {
            let mut presence = self.presence.lock().await;
            presence.classify(id::unix_millis(), stale_after_ms, gone_after_ms)
        };
        for transition in transitions {
            self.emit_presence(transition).await?;
        }
        Ok(())
    }

    /// Runs the presence sweep on the configured interval until the daemon
    /// exits. Returns the task handle so tests can abort it.
    pub fn spawn_presence_sweeper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let state = Arc::clone(self);
        let interval = self
            .config
            .presence_sweep_interval
            .max(Duration::from_millis(10));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if let Err(error) = state.sweep_presence().await {
                    eprintln!("coop daemon: presence sweep failed: {error}");
                }
            }
        })
    }

    /// Lists registered agents. When `include_stale` is false only `active`
    /// agents are returned; otherwise every state is included.
    pub async fn list_agents(&self, include_stale: bool) -> Result<Vec<AgentView>, BrokerError> {
        self.sweep_presence().await?;
        Ok(self.agent_snapshot(include_stale).await)
    }

    /// Observation without touching presence or emitting state-transition events.
    pub(crate) async fn agent_snapshot(&self, include_stale: bool) -> Vec<AgentView> {
        let registry = self.registry.read().await;
        let presence = self.presence.lock().await;
        let now = id::unix_millis();
        let stale_ms = u64::try_from(self.config.stale_after.as_millis()).unwrap_or(u64::MAX);
        let gone_ms = u64::try_from(self.config.gone_after.as_millis()).unwrap_or(u64::MAX);
        let mut views = Vec::new();
        for (name, record) in registry.records_with_names() {
            let entry = presence.entries.get(&record.agent_id);
            let state = entry.map_or(AgentState::Active, |entry| {
                let silence = now.saturating_sub(entry.last_seen_ms);
                if silence >= gone_ms {
                    AgentState::Gone
                } else if silence >= stale_ms {
                    AgentState::Stale
                } else {
                    AgentState::Active
                }
            });
            if !include_stale && state != AgentState::Active {
                continue;
            }
            let last_seen = entry.map_or_else(
                || record.registered_at.clone(),
                |entry| id::format_rfc3339_millis(entry.last_seen_ms),
            );
            views.push(AgentView {
                agent_id: record.agent_id.clone(),
                name: name.to_string(),
                kind: record.kind.clone(),
                capabilities: record.capabilities.clone(),
                state,
                last_seen,
                registered_at: record.registered_at.clone(),
            });
        }
        views
    }

    pub(crate) async fn list_agents_page(
        &self,
        include_stale: bool,
        after: Option<&str>,
        limit: usize,
        bytes: usize,
        allows: impl Fn(&str) -> bool,
    ) -> Result<(Vec<AgentView>, Option<String>), BrokerError> {
        self.sweep_presence().await?;
        let registry = self.registry.read().await;
        let presence = self.presence.lock().await;
        let mut budget = crate::network::page::PageBudget::new(limit, bytes);
        let mut views: Vec<AgentView> = Vec::new();
        let mut next = None;
        for (name, record) in registry.records_with_names() {
            if after.is_some_and(|after| name <= after) || !allows(&record.agent_id) {
                continue;
            }
            let entry = presence.entries.get(&record.agent_id);
            let state = entry.map_or(AgentState::Active, |entry| entry.state);
            if !include_stale && state != AgentState::Active {
                continue;
            }
            let last_seen = entry.map_or_else(
                || record.registered_at.clone(),
                |entry| id::format_rfc3339_millis(entry.last_seen_ms),
            );
            #[derive(Serialize)]
            struct View<'a> {
                agent_id: &'a str,
                name: &'a str,
                kind: &'a str,
                capabilities: &'a [String],
                state: AgentState,
                last_seen: &'a str,
                registered_at: &'a str,
            }
            let view = View {
                agent_id: &record.agent_id,
                name,
                kind: &record.kind,
                capabilities: &record.capabilities,
                state,
                last_seen: &last_seen,
                registered_at: &record.registered_at,
            };
            if !budget.admit(&view)? {
                next = views.last().map(|view| view.name.clone());
                break;
            }
            views.push(AgentView {
                agent_id: record.agent_id.clone(),
                name: name.into(),
                kind: record.kind.clone(),
                capabilities: record.capabilities.clone(),
                state,
                last_seen,
                registered_at: record.registered_at.clone(),
            });
        }
        Ok((views, next))
    }

    /// Returns the current presence snapshot for one authenticated agent.
    pub async fn heartbeat(
        &self,
        identity: &AgentIdentity,
    ) -> Result<HeartbeatResponse, BrokerError> {
        let presence = self.presence.lock().await;
        let entry = presence.entries.get(&identity.agent_id).ok_or_else(|| {
            BrokerError::internal(format!("no presence entry for agent {}", identity.agent_id))
        })?;
        Ok(HeartbeatResponse {
            agent_id: identity.agent_id.clone(),
            state: entry.state,
            last_seen: id::format_rfc3339_millis(entry.last_seen_ms),
        })
    }

    async fn emit_presence(&self, transition: Transition) -> Result<(), BrokerError> {
        let body = Body::new(
            "application/json",
            serde_json::to_string(&serde_json::json!({
                "event": format!("agent_{}", transition.to.as_str()),
                "agent_id": transition.agent_id,
                "name": transition.name,
                "kind": transition.kind,
                "state": transition.to,
                "previous_state": transition.from,
                "last_seen": id::format_rfc3339_millis(transition.last_seen_ms),
                "at": id::now_rfc3339(),
            }))
            .map_err(|error| BrokerError::internal(error.to_string()))?,
        );
        let kind = format!("agent_{}", transition.to.as_str());
        self.append_channel_event("#presence", DAEMON_SENDER, &kind, body)
            .await?;
        Ok(())
    }
}
