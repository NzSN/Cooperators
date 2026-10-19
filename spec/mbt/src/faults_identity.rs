//! Instance lifecycle replay through the compiler-generated identity port.
//! Actual RPC replies and registry bytes supply all enrolled identities. Opaque
//! names are normalized by first issuance, independently of model expectations.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cooperators::error::ErrorCode;
use cooperators::registry::{new_token, token_hash};
use cooperators::rpc::{RpcError, RpcRequest};
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use mirrorrust::BindingError;
use serde_json::{json, Value};

use crate::fault_support::{config, error, run, value};

#[path = "../fault_interfaces/identity/generated/MBFaultIdentityMirror.generated.rs"]
#[allow(dead_code)]
mod generated;
use generated::{
    AuthenticateInput, DisconnectInput, EnrollInput, MBFaultIdentityObservation,
    MBFaultIdentityPort, NativeCodec, ReconnectInput, RetireInput, RotateTokenInput,
};

const LAUNCHES: [&str; 2] = ["launch1", "launch2"];

/// A bijection on observed opaque strings, not on actor slots. Returning a
/// duplicate concrete ID or credential therefore returns the SAME model name.
#[derive(Default)]
struct Names {
    actual_to_name: BTreeMap<String, String>,
}

impl Names {
    fn name(&mut self, actual: &str, prefix: &str) -> String {
        if let Some(name) = self.actual_to_name.get(actual) {
            return name.clone();
        }
        let name = format!("{prefix}{}", self.actual_to_name.len() + 1);
        self.actual_to_name.insert(actual.to_owned(), name.clone());
        name
    }
}

struct Client {
    launch_key: String,
    token: Option<String>,
    routing_name: Option<String>,
    issued_id: Option<String>,
    reply: Option<String>,
    revoked: Vec<String>,
    session: Option<ConnectionSession>,
}

impl Client {
    fn new() -> Result<Self, BindingError> {
        Ok(Self {
            launch_key: new_token().map_err(error)?,
            token: None,
            routing_name: None,
            issued_id: None,
            reply: None,
            revoked: Vec::new(),
            session: None,
        })
    }
}

struct IdentitySut {
    root: tempfile::TempDir,
    rt: tokio::runtime::Runtime,
    state: Arc<DaemonState>,
    clients: BTreeMap<String, Client>,
    ids: Names,
    tokens: Names,
    result: String,
    evidence: Option<PathBuf>,
    observation_index: usize,
}

impl IdentitySut {
    fn new(trace: &Path) -> Result<Self, BindingError> {
        let root = tempfile::tempdir().map_err(error)?;
        let rt = tokio::runtime::Runtime::new().map_err(error)?;
        let state =
            Arc::new(DaemonState::with_config(DaemonConfig::new(root.path())).map_err(error)?);
        let clients = LAUNCHES
            .into_iter()
            .map(|launch| Ok((launch.into(), Client::new()?)))
            .collect::<Result<_, BindingError>>()?;
        let evidence = std::env::var_os("COOP_MBT_EVIDENCE_DIR")
            .map(|directory| {
                let trace_name = trace
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .unwrap_or("trace");
                let run_id = cooperators::id::new_ulid().map_err(error)?;
                let directory = PathBuf::from(directory)
                    .join("identity")
                    .join(format!("{trace_name}-{run_id}"));
                fs::create_dir_all(&directory).map_err(error)?;
                eprintln!("identity evidence: {}", directory.display());
                Ok(directory)
            })
            .transpose()?;
        Ok(Self {
            root,
            rt,
            state,
            clients,
            ids: Names::default(),
            tokens: Names::default(),
            result: "none".into(),
            evidence,
            observation_index: 0,
        })
    }

    fn client(&self, actor: &str) -> Result<&Client, BindingError> {
        self.clients
            .get(actor)
            .ok_or_else(|| error(format!("unknown launch input {actor}")))
    }

    fn call(
        &self,
        session: &mut ConnectionSession,
        method: &str,
        params: Value,
    ) -> Result<Value, RpcError> {
        let result = self
            .rt
            .block_on(server::dispatch(
                &self.state,
                session,
                RpcRequest {
                    id: Some(json!(1)),
                    method: method.into(),
                    params: params.clone(),
                    is_notification: false,
                },
            ))
            .expect("request with id must have a response")
            .into_result();
        if let Some(directory) = &self.evidence {
            let response = match &result {
                Ok(reply) => json!({"result": redact(reply)}),
                Err(failure) => json!({"error": failure}),
            };
            fs::write(
                directory.join(format!("{:03}-{method}-rpc.json", self.observation_index)),
                serde_json::to_vec_pretty(&json!({
                    "method": method,
                    "request": redact(&params),
                    "response": response,
                }))
                .map_err(|failure| RpcError::internal(failure.to_string()))?,
            )
            .map_err(|failure| {
                RpcError::internal(format!("retain identity evidence: {failure}"))
            })?;
        }
        result
    }

    fn field<'a>(reply: &'a Value, field: &str) -> Result<&'a str, BindingError> {
        reply[field]
            .as_str()
            .ok_or_else(|| error(format!("actual RPC reply lacks {field}")))
    }

    fn credential(&self, actor: &str) -> Result<String, BindingError> {
        self.client(actor)?
            .token
            .clone()
            .ok_or_else(|| error("launch has no issued credential"))
    }

    fn retire_instance(&mut self, actor: &str) -> Result<(), BindingError> {
        let token = self.credential(actor)?;
        let mut session = self
            .clients
            .get_mut(actor)
            .unwrap()
            .session
            .take()
            .unwrap_or_default();
        let reply = self
            .call(&mut session, "retire_agent", json!({"token": token}))
            .map_err(|failure| error(format!("retirement RPC failed: {failure:?}")))?;
        if self.client(actor)?.issued_id.as_deref() != reply["agent_id"].as_str()
            || reply["retired"] != true
        {
            return Err(error(
                "retirement reply does not identify the retired instance",
            ));
        }
        let client = self.clients.get_mut(actor).unwrap();
        client.revoked.push(token);
        client.session = None;
        self.result = "ok".into();
        Ok(())
    }
}

impl MBFaultIdentityPort for IdentitySut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        self.result = "none".into();
        Ok(())
    }

    fn enroll(&mut self, input: EnrollInput) -> Result<(), BindingError> {
        let key = self.client(&input.actor)?.launch_key.clone();
        let first_enrollment = self.client(&input.actor)?.issued_id.is_none();
        let was_connected = self.client(&input.actor)?.session.is_some();
        let mut session = self
            .clients
            .get_mut(&input.actor)
            .unwrap()
            .session
            .take()
            .unwrap_or_default();
        let reply = match self.call(
            &mut session,
            "enroll_agent",
            json!({"launch_key": key, "kind": "codex", "display_name": "codex"}),
        ) {
            Ok(reply) => reply,
            Err(failure)
                if !first_enrollment
                    && failure.data.as_ref().and_then(|data| data["code"].as_str())
                        == Some(ErrorCode::Conflict.as_str()) =>
            {
                let data = failure.data.as_ref().unwrap();
                let actual_id = Self::field(data, "agent_id")?.to_owned();
                self.ids.name(&actual_id, "id");
                let client = self.clients.get_mut(&input.actor).unwrap();
                client.reply = Some(actual_id);
                client.session = was_connected.then_some(session);
                self.result = "conflict".into();
                return Ok(());
            }
            Err(failure) => return Err(error(format!("enrollment RPC failed: {failure:?}"))),
        };
        let id = Self::field(&reply, "agent_id")?.to_owned();
        let token = Self::field(&reply, "token")?.to_owned();
        let name = Self::field(&reply, "name")?.to_owned();
        // Allocation order is registered at the actual reply boundary. Model
        // state/expected labels are never passed into these maps.
        self.ids.name(&id, "id");
        self.tokens.name(&token, "token");
        let client = self.clients.get_mut(&input.actor).unwrap();
        client.issued_id = Some(id.clone());
        client.reply = Some(id);
        client.token = Some(token);
        client.routing_name = Some(name);
        client.session = (first_enrollment || was_connected).then_some(session);
        self.result = "ok".into();
        Ok(())
    }

    fn disconnect(&mut self, input: DisconnectInput) -> Result<(), BindingError> {
        self.client(&input.actor)?;
        // Drop the real dispatcher connection context, including cached auth.
        self.clients.get_mut(&input.actor).unwrap().session.take();
        self.result = "ok".into();
        Ok(())
    }

    fn reconnect(&mut self, input: ReconnectInput) -> Result<(), BindingError> {
        let token = self.credential(&input.actor)?;
        let mut session = ConnectionSession::default();
        let reply = self
            .call(&mut session, "heartbeat", json!({"token": token}))
            .map_err(|failure| error(format!("reconnect RPC failed: {failure:?}")))?;
        let actual_id = Self::field(&reply, "agent_id")?.to_owned();
        self.ids.name(&actual_id, "id");
        let client = self.clients.get_mut(&input.actor).unwrap();
        client.reply = Some(actual_id);
        client.session = Some(session);
        self.result = "ok".into();
        Ok(())
    }

    fn retire(&mut self, input: RetireInput) -> Result<(), BindingError> {
        self.retire_instance(&input.actor)
    }

    fn rotate_token(&mut self, input: RotateTokenInput) -> Result<(), BindingError> {
        let client = self.client(&input.actor)?;
        let old_token = self.credential(&input.actor)?;
        let name = client
            .routing_name
            .clone()
            .ok_or_else(|| error("launch has no issued routing name"))?;
        let was_connected = client.session.is_some();
        let mut session = self
            .clients
            .get_mut(&input.actor)
            .unwrap()
            .session
            .take()
            .unwrap_or_default();
        let reply = self
            .call(
                &mut session,
                "register_agent",
                json!({"name": name, "token": old_token, "kind": "codex"}),
            )
            .map_err(|failure| error(format!("rotation RPC failed: {failure:?}")))?;
        let token = Self::field(&reply, "token")?.to_owned();
        let actual_id = Self::field(&reply, "agent_id")?.to_owned();
        if self.client(&input.actor)?.issued_id.as_deref() != Some(actual_id.as_str()) {
            return Err(error("rotation reply changed the canonical instance ID"));
        }
        let actual_name = Self::field(&reply, "name")?.to_owned();
        self.ids.name(&actual_id, "id");
        self.tokens.name(&token, "token");
        let client = self.clients.get_mut(&input.actor).unwrap();
        // Rotation is required to preserve the previously observed ID. The
        // raw registry projection below observes any backend reallocation.
        client.revoked.push(old_token);
        client.token = Some(token);
        client.routing_name = Some(actual_name);
        client.session = was_connected.then_some(session);
        self.result = "ok".into();
        Ok(())
    }

    fn authenticate(&mut self, input: AuthenticateInput) -> Result<(), BindingError> {
        let token = match input.mode.as_str() {
            "current" => self.credential(&input.actor)?,
            "revoked" => self
                .client(&input.actor)?
                .revoked
                .last()
                .cloned()
                .ok_or_else(|| error("no actual revoked credential was observed"))?,
            _ => return Err(error("unknown authentication mode")),
        };
        // A transient probe is closed after the actual auth result, preserving
        // the long-lived client's connection state as the model requires.
        let mut session = ConnectionSession::default();
        self.result = match self.call(&mut session, "heartbeat", json!({"token": token})) {
            Ok(reply) => {
                let id = Self::field(&reply, "agent_id")?;
                if self.client(&input.actor)?.issued_id.as_deref() != Some(id) {
                    return Err(error("authentication resolved a different instance ID"));
                }
                "ok"
            }
            Err(failure)
                if failure.data.as_ref().and_then(|data| data["code"].as_str())
                    == Some(ErrorCode::Unauthenticated.as_str()) =>
            {
                "unauthenticated"
            }
            Err(failure) => return Err(error(format!("authentication RPC failed: {failure:?}"))),
        }
        .into();
        Ok(())
    }

    fn observe(&mut self) -> Result<MBFaultIdentityObservation, BindingError> {
        let raw_registry = match fs::read(self.root.path().join("registry.json")) {
            Ok(bytes) => Some(bytes),
            Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => None,
            Err(failure) => return Err(error(failure)),
        };
        let registry = match &raw_registry {
            Some(bytes) => serde_json::from_slice::<Value>(bytes).map_err(error)?,
            None => json!({"agents": {}}),
        };
        let mut ids = Vec::new();
        let mut replies = Vec::new();
        let mut tokens = Vec::new();
        let mut kinds = Vec::new();
        let mut labels = Vec::new();
        let mut active = Vec::new();
        let mut connected = Vec::new();
        for launch in LAUNCHES {
            let client = self.clients.get(launch).unwrap();
            if let Some(name) = &client.routing_name {
                let record = &registry["agents"][name];
                let actual_id = Self::field(record, "agent_id")?;
                let actual_hash = Self::field(record, "token_hash")?;
                let actual_token = self
                    .tokens
                    .actual_to_name
                    .keys()
                    .find(|token| token_hash(token) == actual_hash)
                    .ok_or_else(|| {
                        error("registry current hash has no actually issued credential")
                    })?;
                ids.push(json!([launch, self.ids.name(actual_id, "id")]));
                replies.push(json!([
                    launch,
                    self.ids.name(
                        client
                            .reply
                            .as_deref()
                            .ok_or_else(|| error("missing actual enrollment reply"))?,
                        "id"
                    )
                ]));
                tokens.push(json!([launch, self.tokens.actual_to_name[actual_token]]));
                kinds.push(json!([launch, Self::field(record, "kind")?]));
                labels.push(json!([launch, Self::field(record, "display_name")?]));
                if !record["retired"].as_bool().unwrap_or(false) {
                    active.push(launch);
                    if client.session.is_some() {
                        connected.push(launch);
                    }
                }
            } else {
                ids.push(json!([launch, "-id"]));
                replies.push(json!([launch, "-id"]));
                tokens.push(json!([launch, "-token"]));
                // Before enrollment these are the common launch configuration
                // inputs; afterwards both fields come from the actual file.
                kinds.push(json!([launch, "codex"]));
                labels.push(json!([launch, "codex"]));
            }
        }
        let observed = json!({
            "ids": {"#map": ids}, "replies": {"#map": replies},
            "currentTokens": {"#map": tokens}, "kinds": {"#map": kinds},
            "labels": {"#map": labels}, "active": {"#set": active},
            "connected": {"#set": connected},
        });
        if let Some(directory) = &self.evidence {
            if let Some(bytes) = &raw_registry {
                fs::write(
                    directory.join(format!("{:03}-registry.json", self.observation_index)),
                    bytes,
                )
                .map_err(error)?;
            }
            let token_bijection: BTreeMap<_, _> = self
                .tokens
                .actual_to_name
                .iter()
                .map(|(token, name)| (token_hash(token), name.clone()))
                .collect();
            fs::write(
                directory.join(format!("{:03}-observation.json", self.observation_index)),
                serde_json::to_vec_pretty(&json!({"observation": observed, "result": self.result}))
                    .map_err(error)?,
            )
            .map_err(error)?;
            fs::write(directory.join("opaque-bijections.json"), serde_json::to_vec_pretty(&json!({
                "ids": self.ids.actual_to_name,
                "credential_hashes": token_bijection,
                "rule": "independent first issuance order; repeated actual values keep their original alias",
            })).map_err(error)?).map_err(error)?;
        }
        self.observation_index += 1;
        Ok(MBFaultIdentityObservation {
            observation: generated::MiTypeO0::decode(&value(observed)?)?,
            result: self.result.clone(),
        })
    }
}

pub fn replay(bin: &str, trace: &Path) -> Result<(), String> {
    let cfg = config("MBFaultIdentity");
    let sut = IdentitySut::new(trace).map_err(|failure| failure.to_string())?;
    let binding =
        generated::bind_m_b_fault_identity(sut, &cfg).map_err(|failure| failure.to_string())?;
    run(bin, trace, cfg, binding)
}

/// Evidence retains the actual identity and metadata replies but replaces
/// each plaintext enrollment/auth credential with its existing hash format.
fn redact(value: &Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| {
                    let value = if matches!(key.as_str(), "token" | "launch_key" | "old_token") {
                        value
                            .as_str()
                            .map(|secret| json!({"credential_hash": token_hash(secret)}))
                            .unwrap_or_else(|| redact(value))
                    } else {
                        redact(value)
                    };
                    (key.clone(), value)
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(redact).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{redact, Names};
    use serde_json::json;

    #[test]
    fn opaque_renaming_preserves_real_equalities_and_allocation_order() {
        let mut ids = Names::default();
        assert_eq!(ids.name("actual-B", "id"), "id1");
        assert_eq!(ids.name("actual-A", "id"), "id2");
        assert_eq!(ids.name("actual-B", "id"), "id1");
    }

    #[test]
    fn retained_replies_and_requests_do_not_contain_plaintext_credentials() {
        let source = json!({"token": "secret-A", "nested": {"launch_key": "secret-B"}, "agent_id": "actual-ID"});
        let retained = redact(&source);
        let text = retained.to_string();
        assert!(!text.contains("secret-A"));
        assert!(!text.contains("secret-B"));
        assert_eq!(retained["agent_id"], "actual-ID");
        assert_eq!(
            retained["token"]["credential_hash"],
            cooperators::registry::token_hash("secret-A")
        );
    }
}
