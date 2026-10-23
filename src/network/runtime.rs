use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{BrokerError, ErrorCode};
use crate::registry::{AgentIdentity, NetworkGrant, NetworkMeta};
use crate::server::DaemonState;

use super::policy::NetworkPolicy;

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub principal_id: String,
    pub principal_generation: u64,
    pub identity: AgentIdentity,
}

pub struct NetworkRuntime {
    pub meta: NetworkMeta,
    pub incarnation: String,
    pub connections: Arc<Semaphore>,
    policy: RwLock<NetworkPolicy>,
    requests: Mutex<BTreeMap<String, Arc<Semaphore>>>,
    polls: Mutex<BTreeMap<String, Arc<Semaphore>>>,
}

impl std::fmt::Debug for NetworkRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkRuntime")
            .field("bus_id", &self.meta.bus_id)
            .finish_non_exhaustive()
    }
}

impl NetworkRuntime {
    pub fn new(policy: NetworkPolicy, meta: NetworkMeta) -> Result<Self, BrokerError> {
        policy.validate()?;
        let requests = policy
            .devices
            .iter()
            .map(|d| {
                (
                    d.principal_id.clone(),
                    Arc::new(Semaphore::new(policy.requests_per_principal)),
                )
            })
            .collect();
        let polls = policy
            .devices
            .iter()
            .map(|d| {
                (
                    d.principal_id.clone(),
                    Arc::new(Semaphore::new(policy.polls_per_principal)),
                )
            })
            .collect();
        Ok(Self {
            incarnation: crate::id::new_ulid()?,
            connections: Arc::new(Semaphore::new(policy.max_connections)),
            policy: RwLock::new(policy),
            requests: Mutex::new(requests),
            polls: Mutex::new(polls),
            meta,
        })
    }

    pub fn policy(&self) -> NetworkPolicy {
        self.policy.read().unwrap().clone()
    }

    /// Admissions authorized before this replacement may complete; read results
    /// are checked again before return. Capacity changes require restart.
    pub async fn replace_policy(
        &self,
        state: &DaemonState,
        policy: NetworkPolicy,
    ) -> Result<(), BrokerError> {
        policy.validate()?;
        if !state
            .network()
            .is_some_and(|runtime| std::ptr::eq(runtime.as_ref(), self))
        {
            return Err(BrokerError::invalid(
                "policy replacement requires its owning daemon state",
            ));
        }
        let mut registry = state.registry.write().await;
        let mut current = self.policy.write().unwrap();
        if current.max_connections != policy.max_connections
            || current.requests_per_principal != policy.requests_per_principal
            || current.polls_per_principal != policy.polls_per_principal
        {
            return Err(BrokerError::invalid(
                "network capacity changes require restart",
            ));
        }
        registry.approve_network_policy(&policy)?;
        for (map, capacity) in [
            (&self.requests, policy.requests_per_principal),
            (&self.polls, policy.polls_per_principal),
        ] {
            let mut gates = map.lock().unwrap();
            // Retain gates across removal so outstanding requests still count
            // if a principal is restored with a newer generation.
            for device in &policy.devices {
                gates
                    .entry(device.principal_id.clone())
                    .or_insert_with(|| Arc::new(Semaphore::new(capacity)));
            }
        }
        *current = policy;
        Ok(())
    }

    pub async fn authenticate(
        &self,
        state: &DaemonState,
        token: &str,
    ) -> Result<AuthContext, BrokerError> {
        let registry = state.registry.read().await;
        registry.check_activation_ready()?;
        let identity = registry.authenticate(token).ok_or_else(unauthenticated)?;
        let grant = registry
            .network_grant(&identity.agent_id)
            .ok_or_else(unauthenticated)?;
        let policy = self.policy.read().unwrap();
        let device = policy
            .device(&grant.principal_id)
            .ok_or_else(unauthenticated)?;
        if device.generation != grant.principal_generation {
            return Err(unauthenticated());
        }
        Ok(AuthContext {
            principal_id: grant.principal_id.clone(),
            principal_generation: grant.principal_generation,
            identity,
        })
    }

    pub fn request_permit(
        &self,
        principal: &str,
        poll: bool,
    ) -> Result<NetworkPermit, BrokerError> {
        let request = self
            .requests
            .lock()
            .unwrap()
            .get(principal)
            .cloned()
            .ok_or_else(unauthenticated)?
            .try_acquire_owned()
            .map_err(|_| {
                BrokerError::application(
                    ErrorCode::ResourceExhausted,
                    "principal request capacity exhausted",
                )
            })?;
        let poll = if poll {
            Some(
                self.polls
                    .lock()
                    .unwrap()
                    .get(principal)
                    .cloned()
                    .ok_or_else(unauthenticated)?
                    .try_acquire_owned()
                    .map_err(|_| {
                        BrokerError::application(
                            ErrorCode::ResourceExhausted,
                            "principal long-poll capacity exhausted",
                        )
                    })?,
            )
        } else {
            None
        };
        Ok(NetworkPermit {
            _request: request,
            _poll: poll,
        })
    }

    pub async fn enroll(
        &self,
        state: &DaemonState,
        token: &str,
        params: Value,
    ) -> Result<Value, BrokerError> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Enrollment {
            launch_key: String,
            kind: String,
            display_name: String,
            #[serde(default)]
            capabilities: Option<Vec<String>>,
        }
        let request: Enrollment = serde_json::from_value(params)
            .map_err(|_| BrokerError::invalid("invalid approved enrollment request"))?;
        let principal = self
            .policy()
            .authenticate_device(token)?
            .principal_id
            .clone();
        let _permit = self.request_permit(&principal, false)?;
        let enrollment = {
            let mut registry = state.registry.write().await;
            // Policy replacement and the registry commit share this admission
            // boundary. Never hold a policy guard across an await.
            let policy = self.policy.read().unwrap();
            let device = policy.authenticate_device(token)?;
            if !device.allow_enroll || device.principal_id != principal {
                return Err(forbidden());
            }
            if policy
                .devices
                .iter()
                .any(|d| d.token_hash == crate::registry::token_hash(&request.launch_key))
            {
                return Err(BrokerError::invalid(
                    "instance launch proof must differ from every device credential",
                ));
            }
            let grant = NetworkGrant {
                principal_id: device.principal_id.clone(),
                principal_generation: device.generation,
                expires_at_ms: crate::id::unix_millis()
                    .saturating_add(device.instance_ttl_s * 1000)
                    .min(device.expires_at_ms),
            };
            let retry = registry.authenticate(&request.launch_key).is_some();
            if !retry && registry.network_issued(&device.principal_id) >= device.max_instances {
                return Err(BrokerError::application(
                    ErrorCode::ResourceExhausted,
                    "principal issued-identity budget exhausted",
                ));
            }
            registry.enroll_network(
                request.launch_key,
                request.kind,
                request.display_name,
                request.capabilities,
                grant,
            )?
        };
        state.ensure_inbox(&enrollment.agent_id).await?;
        state
            .register_presence(&AgentIdentity {
                agent_id: enrollment.agent_id.clone(),
                name: enrollment.name.clone(),
                kind: enrollment.kind.clone(),
                capabilities: enrollment.capabilities.clone(),
            })
            .await?;
        let mut reply = serde_json::to_value(&enrollment)
            .map_err(|_| BrokerError::internal("enrollment serialization failed"))?;
        let registry = state.registry.read().await;
        let grant = registry
            .network_grant(&enrollment.agent_id)
            .ok_or_else(unauthenticated)?;
        reply["bus_id"] = Value::String(self.meta.bus_id.clone());
        reply["credential_expires_at_ms"] = grant.expires_at_ms.into();
        Ok(reply)
    }

    pub async fn authorize(
        &self,
        state: &DaemonState,
        context: &AuthContext,
        method: &str,
        params: &Value,
    ) -> Result<String, BrokerError> {
        if !super::method_contracts().iter().any(|m| m.method == method)
            || matches!(
                method,
                "echo" | "daemon.status" | "register_agent" | "enroll_agent"
            )
        {
            return Err(forbidden());
        }
        if !self
            .policy()
            .device(&context.principal_id)
            .is_some_and(|d| d.generation == context.principal_generation)
        {
            return Err(unauthenticated());
        }
        let resource = match method {
            "send_message" => {
                let recipient = state
                    .registry
                    .read()
                    .await
                    .resolve(string(params, "to")?)
                    .ok_or_else(|| {
                        BrokerError::application(ErrorCode::NotFound, "recipient not found")
                    })?;
                format!("inbox:{}", recipient.agent_id)
            }
            "wait_message" | "ack" => format!("inbox:{}", context.identity.agent_id),
            "publish" | "read_channel" => format!("channel:{}", string(params, "channel")?),
            "create_queue" | "enqueue" | "claim_task" => {
                format!("queue:{}", string(params, "queue")?)
            }
            "publish_doc" | "update_doc" | "read_doc" | "claim_doc" => {
                format!("document:{}", string(params, "name")?)
            }
            "renew_lease" | "complete_task" | "fail_task" | "release_doc" => {
                lease_resource(state, string(params, "lease_id")?).await?
            }
            "subscribe_arrivals" => {
                let resource = params
                    .get("resource")
                    .ok_or_else(|| BrokerError::invalid("resource required"))?;
                let kind = string(resource, "type")?;
                if !matches!(kind, "queue" | "document") {
                    return Err(BrokerError::invalid("invalid subscription resource"));
                }
                format!("{kind}:{}", string(resource, "name")?)
            }
            "wait_arrivals"
            | "ack_arrivals"
            | "unsubscribe_arrivals"
            | "list_arrival_subscriptions" => format!("arrival:{}", context.identity.agent_id),
            "list_docs" | "list_agents" => {
                if !self.policy().has_method(&context.principal_id, method) {
                    return Err(forbidden());
                }
                return Ok("filtered-list".into());
            }
            _ => format!("agent:{}", context.identity.agent_id),
        };
        let policy = self.policy();
        if !policy
            .device(&context.principal_id)
            .is_some_and(|d| d.generation == context.principal_generation)
            || !policy.allows(&context.principal_id, method, &resource)
        {
            return Err(forbidden());
        }
        Ok(resource)
    }
}

pub struct NetworkPermit {
    _request: OwnedSemaphorePermit,
    _poll: Option<OwnedSemaphorePermit>,
}

pub(super) fn forbidden() -> BrokerError {
    BrokerError::application(ErrorCode::Forbidden, "method or resource is not authorized")
}
fn unauthenticated() -> BrokerError {
    BrokerError::application(
        ErrorCode::Unauthenticated,
        "valid scoped instance credential required",
    )
}
fn string<'a>(params: &'a Value, key: &str) -> Result<&'a str, BrokerError> {
    params
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| BrokerError::invalid(format!("{key} string required")))
}

async fn lease_resource(state: &DaemonState, lease_id: &str) -> Result<String, BrokerError> {
    for (name, queue) in state.queue_states.lock().await.iter() {
        if queue.tasks.values().any(|task| {
            task.lease
                .as_ref()
                .is_some_and(|lease| lease.lease_id == lease_id)
        }) {
            return Ok(format!("queue:{name}"));
        }
    }
    for (name, doc) in state.doc_states.lock().await.iter() {
        if doc
            .claim
            .as_ref()
            .is_some_and(|claim| claim.lease_id == lease_id)
        {
            return Ok(format!("document:{name}"));
        }
    }
    Err(BrokerError::application(
        ErrorCode::LeaseLost,
        "lease is not active",
    ))
}
