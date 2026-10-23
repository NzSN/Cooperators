#![allow(dead_code)]
use bytes::Bytes;
use cooperators::network::{method_contracts, NetworkPolicy};
use cooperators::registry::{new_token, token_hash};
use cooperators::rpc::{RpcError, RpcRequest};
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use serde_json::{json, Value};
use std::sync::Arc;

pub fn policy(token: &str) -> NetworkPolicy {
    serde_json::from_value(json!({"v":1,"allowed_hosts":["localhost"],
        "max_connections":8,"requests_per_principal":3,"polls_per_principal":1,
        "devices":[{"principal_id":"device-a","token_hash":token_hash(token),
            "expires_at_ms":4_102_444_800_000u64,"allow_enroll":true,
            "max_instances":4,"instance_ttl_s":3600,
            "permissions":method_contracts().iter().map(|m|json!({"method":m.method,"resources":["*"]})).collect::<Vec<_>>() }]})).unwrap()
}

pub struct Fixture {
    pub temp: crate::common::TempDir,
    pub state: Arc<DaemonState>,
    pub device: String,
    pub instance: String,
    pub agent_id: String,
    pub operator: String,
}
impl Fixture {
    pub async fn new() -> Self {
        let temp = crate::common::TempDir::new("network-policy");
        let device = new_token().unwrap();
        let mut config = DaemonConfig::new(temp.path().join("root"));
        config.enable_arrivals = true;
        config.network_policy = Some(policy(&device));
        let state = Arc::new(DaemonState::owned(config).unwrap());
        let instance = new_token().unwrap();
        let enrollment = state
            .network()
            .unwrap()
            .enroll(
                &state,
                &device,
                json!({"launch_key":instance,"kind":"agent","display_name":"reader"}),
            )
            .await
            .unwrap();
        let operator = call(&state, "register_agent", json!({"name":"operator"}))
            .await
            .unwrap()["token"]
            .as_str()
            .unwrap()
            .into();
        Self {
            temp,
            state,
            device,
            instance,
            agent_id: enrollment["agent_id"].as_str().unwrap().into(),
            operator,
        }
    }
    pub async fn restrict(&self, method: &str, resources: &[&str]) {
        let mut policy = self.state.network().unwrap().policy();
        policy.devices[0]
            .permissions
            .iter_mut()
            .find(|p| p.method == method)
            .unwrap()
            .resources = resources.iter().map(|s| s.to_string()).collect();
        replace_policy(&self.state, policy).await.unwrap();
    }
    pub async fn local(&self, method: &str, mut params: Value) -> Value {
        params["token"] = json!(self.operator);
        call(&self.state, method, params).await.unwrap()
    }
}

pub async fn call(
    state: &Arc<DaemonState>,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    server::dispatch(
        state,
        &mut ConnectionSession::default(),
        RpcRequest {
            id: Some(json!(1)),
            method: method.into(),
            params,
            is_notification: false,
        },
    )
    .await
    .unwrap()
    .into_result()
}
pub async fn http(
    state: &Arc<DaemonState>,
    path: &str,
    token: Option<&str>,
    host: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(if path == "/v1/info" { "GET" } else { "POST" })
        .uri(path)
        .header("Host", host)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream");
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let response = cooperators::mcp::handle_http(
        &cooperators::mcp::http_service(state.clone()),
        state,
        request
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap(),
    )
    .await;
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
pub fn rpc(method: &str, params: Value) -> Value {
    json!({"jsonrpc":"2.0","id":1,"method":method,"params":params})
}
pub fn code(reply: &Value) -> Option<&str> {
    reply.pointer("/error/data/code").and_then(Value::as_str)
}

pub async fn replace_policy(
    state: &DaemonState,
    mut policy: NetworkPolicy,
) -> Result<(), cooperators::error::BrokerError> {
    let runtime = state.network().unwrap();
    policy.revision = runtime.policy().revision + 1;
    runtime.replace_policy(state, policy).await
}
