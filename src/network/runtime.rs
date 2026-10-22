use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

use crate::error::{BrokerError, ErrorCode};
use crate::registry::{AgentIdentity, NetworkGrant, NetworkMeta};
use crate::server::DaemonState;

use super::policy::NetworkPolicy;

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub principal_id: String,
    pub principal_generation: u64,
    pub credential_expires_at_ms: u64,
    pub identity: AgentIdentity,
}

pub struct NetworkRuntime {
    pub meta: NetworkMeta,
    pub incarnation: String,
    pub connections: Arc<Semaphore>,
    policy: RwLock<NetworkPolicy>,
    requests: Mutex<BTreeMap<String, Arc<Semaphore>>>,
    polls: Mutex<BTreeMap<String, Arc<Semaphore>>>,
    global_polls: Arc<Semaphore>,
    policy_changes: watch::Sender<u64>,
    policy_fenced: AtomicBool,
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
            global_polls: Arc::new(Semaphore::new(policy.max_connections.saturating_sub(1))),
            policy_changes: watch::channel(0).0,
            policy_fenced: AtomicBool::new(false),
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
        self.check_ready()?;
        policy.validate()?;
        let mut registry = state.registry.write().await;
        self.check_ready()?;
        let mut current = self.policy.write().unwrap();
        if current.max_connections != policy.max_connections
            || current.enable_operations != policy.enable_operations
            || current.enable_resource_operations != policy.enable_resource_operations
            || current.requests_per_principal != policy.requests_per_principal
            || current.polls_per_principal != policy.polls_per_principal
        {
            return Err(BrokerError::invalid(
                "network capacity or operation activation changes require restart",
            ));
        }
        let generations = policy
            .devices
            .iter()
            .map(|d| (d.principal_id.clone(), d.generation))
            .collect::<Vec<_>>();
        if let Err(error) = registry.apply_network_policy_generations(&generations) {
            if !matches!(error, BrokerError::Invalid(_)) {
                // Atomic replacement may already have happened before an I/O
                // failure. Do not serve with an unproven authority snapshot.
                self.policy_fenced.store(true, Ordering::Release);
                self.policy_changes
                    .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
            }
            return Err(error);
        }
        for (map, capacity) in [
            (&self.requests, policy.requests_per_principal),
            (&self.polls, policy.polls_per_principal),
        ] {
            let mut gates = map.lock().unwrap();
            gates.retain(|id, gate| {
                policy.devices.iter().any(|d| d.principal_id == *id) || Arc::strong_count(gate) > 1
            });
            for device in &policy.devices {
                gates
                    .entry(device.principal_id.clone())
                    .or_insert_with(|| Arc::new(Semaphore::new(capacity)));
            }
        }
        *current = policy;
        self.policy_changes
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        Ok(())
    }

    pub fn policy_changes(&self) -> watch::Receiver<u64> {
        self.policy_changes.subscribe()
    }

    pub fn context_expires_at_ms(&self, context: &AuthContext) -> u64 {
        if self.check_ready().is_err() {
            return 0;
        }
        self.policy
            .read()
            .unwrap()
            .device(&context.principal_id)
            .filter(|d| d.generation == context.principal_generation)
            .map_or(0, |d| d.expires_at_ms.min(context.credential_expires_at_ms))
    }

    fn check_ready(&self) -> Result<(), BrokerError> {
        if self.policy_fenced.load(Ordering::Acquire) {
            return Err(unauthenticated());
        }
        Ok(())
    }

    pub async fn authenticate(
        &self,
        state: &DaemonState,
        token: &str,
    ) -> Result<AuthContext, BrokerError> {
        self.check_ready()?;
        let registry = state.registry.read().await;
        self.check_ready()?;
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
            credential_expires_at_ms: grant.expires_at_ms,
            identity,
        })
    }

    pub fn request_permit(
        &self,
        principal: &str,
        poll: bool,
    ) -> Result<NetworkPermit, BrokerError> {
        self.check_ready()?;
        let policy = self.policy.read().unwrap();
        policy.device(principal).ok_or_else(unauthenticated)?;
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
        let global_poll = if poll {
            Some(self.global_polls.clone().try_acquire_owned().map_err(|_| {
                BrokerError::application(
                    ErrorCode::ResourceExhausted,
                    "global long-poll capacity exhausted",
                )
            })?)
        } else {
            None
        };
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
            _global_poll: global_poll,
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
        self.check_ready()?;
        let policy = self.policy();
        let device = policy.authenticate_device(token)?;
        if !device.allow_enroll {
            return Err(forbidden());
        }
        let launch_digest = crate::registry::token_hash(&request.launch_key);
        if policy.devices.iter().any(|d| d.token_hash == launch_digest) {
            return Err(BrokerError::invalid(
                "instance launch credential must differ from approved principal credentials",
            ));
        }
        let _permit = self.request_permit(&device.principal_id, false)?;
        let enrollment = {
            let mut registry = state.registry.write().await;
            // The registry lock is the issuance admission boundary. Recheck
            // after waiting for it, and hold the policy read lock through the
            // synchronous durable update so replacement cannot race issuance.
            self.check_ready()?;
            let current = self.policy.read().unwrap();
            let current_device = current.authenticate_device(token)?;
            if current_device.principal_id != device.principal_id
                || current_device.generation != device.generation
            {
                return Err(unauthenticated());
            }
            if !current_device.allow_enroll {
                return Err(forbidden());
            }
            if current
                .devices
                .iter()
                .any(|d| d.token_hash == launch_digest)
            {
                return Err(BrokerError::invalid(
                    "instance launch credential must differ from approved principal credentials",
                ));
            }
            let device = current_device;
            let grant = NetworkGrant {
                principal_id: device.principal_id.clone(),
                principal_generation: device.generation,
                expires_at_ms: crate::id::unix_millis()
                    .saturating_add(device.instance_ttl_s * 1000)
                    .min(device.expires_at_ms),
            };
            let retry = registry
                .authenticate(&request.launch_key)
                .is_some_and(|identity| {
                    registry
                        .network_grant(&identity.agent_id)
                        .is_some_and(|grant| {
                            grant.principal_id == device.principal_id
                                && grant.principal_generation == device.generation
                        })
                });
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
        registry
            .authenticate(&enrollment.token)
            .filter(|identity| identity.agent_id == enrollment.agent_id)
            .ok_or_else(unauthenticated)?;
        let grant = registry
            .network_grant(&enrollment.agent_id)
            .ok_or_else(unauthenticated)?;
        self.check_ready()?;
        let current = self.policy.read().unwrap();
        let current_device = current.authenticate_device(token)?;
        if current_device.principal_id != grant.principal_id
            || current_device.generation != grant.principal_generation
            || grant.expires_at_ms <= crate::id::unix_millis()
        {
            return Err(unauthenticated());
        }
        if !current_device.allow_enroll {
            return Err(forbidden());
        }
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
        self.check_ready()?;
        if self.context_expires_at_ms(context) <= crate::id::unix_millis() {
            return Err(unauthenticated());
        }
        if !super::method_contracts().iter().any(|m| m.method == method)
            || matches!(
                method,
                "echo" | "daemon.status" | "register_agent" | "enroll_agent"
            )
        {
            return Err(forbidden());
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
                let policy = self.policy.read().unwrap();
                let device = policy
                    .device(&context.principal_id)
                    .ok_or_else(unauthenticated)?;
                if device.generation != context.principal_generation
                    || context.credential_expires_at_ms <= crate::id::unix_millis()
                {
                    return Err(unauthenticated());
                }
                if !policy.has_method(&context.principal_id, method) {
                    return Err(forbidden());
                }
                return Ok("filtered-list".into());
            }
            _ => format!("agent:{}", context.identity.agent_id),
        };
        self.authorize_resource(context, method, &resource)?;
        Ok(resource)
    }

    /// Receipt replay uses its original resolved resource, after current
    /// authentication, so a completed lease or retired recipient does not
    /// prevent recovering the already committed result.
    pub fn authorize_resource(
        &self,
        context: &AuthContext,
        method: &str,
        resource: &str,
    ) -> Result<(), BrokerError> {
        self.check_ready()?;
        let policy = self.policy.read().unwrap();
        let device = policy
            .device(&context.principal_id)
            .ok_or_else(unauthenticated)?;
        if device.generation != context.principal_generation
            || context.credential_expires_at_ms <= crate::id::unix_millis()
        {
            return Err(unauthenticated());
        }
        if !policy.allows(&context.principal_id, method, resource) {
            return Err(forbidden());
        }
        Ok(())
    }
}

pub struct NetworkPermit {
    _request: OwnedSemaphorePermit,
    _poll: Option<OwnedSemaphorePermit>,
    _global_poll: Option<OwnedSemaphorePermit>,
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

#[cfg(test)]
#[path = "../../tests/common/mod.rs"]
mod test_common;

#[cfg(test)]
mod tests {
    use super::test_common as common;
    use super::*;
    use crate::registry::{new_token, token_hash};
    use crate::server::DaemonConfig;
    use serde_json::json;
    use std::future::{poll_fn, Future};
    use std::task::Poll;
    use std::time::Duration;

    fn policy(token: &str) -> NetworkPolicy {
        serde_json::from_value(json!({
            "v":1, "allowed_hosts":["bus.example"],
            "devices":[{
                "principal_id":"device-a", "token_hash":token_hash(token),
                "expires_at_ms":crate::id::unix_millis()+60_000,
                "allow_enroll":true, "max_instances":4, "instance_ttl_s":60,
                "permissions":[]
            }]
        }))
        .unwrap()
    }
    fn state(root: &common::TempDir, policy: NetworkPolicy) -> DaemonState {
        let mut config = DaemonConfig::new(root.path().to_path_buf());
        config.network_policy = Some(policy);
        DaemonState::with_config(config).unwrap()
    }
    fn enrollment(key: &str) -> Value {
        json!({"launch_key":key,"kind":"test","display_name":"security-test"})
    }
    fn code(error: BrokerError) -> Option<ErrorCode> {
        ErrorCode::from_rpc_code(error.into_rpc_error().code)
    }
    #[tokio::test]
    async fn enrollment_rechecks_authority_after_waiting_for_registry() {
        let root = common::TempDir::new("network-enrollment-revocation-race");
        let device = new_token().unwrap();
        let initial = policy(&device);
        let state = state(&root, initial.clone());
        let network = state.network().unwrap();
        let locked = state.registry.write().await;
        let mut revoked = initial;
        revoked.devices.clear();
        let replacement = network.replace_policy(&state, revoked);
        tokio::pin!(replacement);
        poll_fn(|cx| {
            assert!(replacement.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let issuing = network.enroll(&state, &device, enrollment(&new_token().unwrap()));
        tokio::pin!(issuing);
        poll_fn(|cx| {
            assert!(issuing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(locked);
        replacement.await.unwrap();
        assert_eq!(
            code(issuing.await.unwrap_err()),
            Some(ErrorCode::Unauthenticated)
        );
        assert!(state.registry.read().await.is_empty());
    }

    #[tokio::test]
    async fn enrollment_rechecks_expiry_after_waiting_for_registry() {
        let root = common::TempDir::new("network-enrollment-expiry-race");
        let device = new_token().unwrap();
        let state = state(&root, policy(&device));
        let network = state.network().unwrap();
        let mut expiring = network.policy();
        expiring.devices[0].expires_at_ms = crate::id::unix_millis() + 100;
        network.replace_policy(&state, expiring).await.unwrap();
        let locked = state.registry.write().await;
        let issuing = network.enroll(&state, &device, enrollment(&new_token().unwrap()));
        tokio::pin!(issuing);
        poll_fn(|cx| {
            assert!(issuing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        tokio::time::sleep(Duration::from_millis(110)).await;
        drop(locked);
        assert_eq!(
            code(issuing.await.unwrap_err()),
            Some(ErrorCode::Unauthenticated)
        );
        assert!(state.registry.read().await.is_empty());
    }

    #[tokio::test]
    async fn enrollment_cannot_return_a_retired_or_rotated_credential() {
        for action in ["retire", "rotate"] {
            let root = common::TempDir::new("network-enrollment-return-race");
            let device = new_token().unwrap();
            let state = state(&root, policy(&device));
            let network = state.network().unwrap();
            let key = new_token().unwrap();
            let presence = state.presence.lock().await;
            let issuing = network.enroll(&state, &device, enrollment(&key));
            tokio::pin!(issuing);
            poll_fn(|cx| {
                assert!(issuing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            {
                let mut registry = state.registry.write().await;
                let issued = registry
                    .authenticate(&key)
                    .expect("issuance committed before presence wait");
                if action == "retire" {
                    registry.retire(&key).unwrap();
                } else {
                    registry
                        .register(issued.name, Some(issued.kind), None, Some(&key))
                        .unwrap();
                }
            }
            drop(presence);
            assert_eq!(
                code(issuing.await.unwrap_err()),
                Some(ErrorCode::Unauthenticated),
                "{action}"
            );
        }
    }
}
