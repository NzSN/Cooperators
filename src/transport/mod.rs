//! Client transport boundary. HTTPS requests never retry a mutation implicitly.

mod https;
pub mod journal;
pub mod profile;
mod uds;

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::ClientError;
use crate::rpc::{RpcError, RpcResponse};

pub use https::HttpsOptions;

/// Authenticated discovery advertised by `/v1/info`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub v: u16,
    pub bus_id: String,
    pub operation_epoch: String,
    pub supported_methods: Vec<String>,
    #[serde(default)]
    pub operation_max_age_ms: Option<u64>,
    #[serde(default)]
    pub operation_epoch_expires_at_ms: Option<u64>,
}

/// Shared limit for one request or response, including JSON framing.
pub const MAX_RPC_BYTES: usize = 4 * 1024 * 1024;

/// Domain mutations supported by the private client intent journal. Exposure
/// still requires the selected broker to advertise the particular method.
pub fn is_journaled_mutation(method: &str) -> bool {
    matches!(
        method,
        "send_message"
            | "publish"
            | "create_queue"
            | "enqueue"
            | "claim_task"
            | "renew_lease"
            | "complete_task"
            | "fail_task"
            | "publish_doc"
            | "update_doc"
            | "claim_doc"
            | "release_doc"
    )
}

/// A broker's location. HTTPS URLs identify an origin, never credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    Uds(PathBuf),
    Https(reqwest::Url),
}

impl Endpoint {
    pub fn https(value: &str) -> Result<Self, ClientError> {
        let url = reqwest::Url::parse(value)
            .map_err(|_| ClientError::Protocol("invalid broker endpoint URL".into()))?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err(ClientError::Protocol(
                "broker endpoint must be an HTTPS origin without credentials, path, query or fragment"
                    .into(),
            ));
        }
        Ok(Self::Https(url))
    }
}

/// Reusable pooled broker client. Credentials belong to each request's context.
#[derive(Clone)]
pub struct BrokerClient {
    endpoint: Endpoint,
    http: Option<reqwest::Client>,
    expected_bus_id: Option<String>,
}

impl std::fmt::Debug for BrokerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerClient")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl BrokerClient {
    pub fn uds(socket: impl Into<PathBuf>) -> Self {
        Self {
            endpoint: Endpoint::Uds(socket.into()),
            http: None,
            expected_bus_id: None,
        }
    }

    pub fn https(origin: &str, options: HttpsOptions) -> Result<Self, ClientError> {
        Ok(Self {
            endpoint: Endpoint::https(origin)?,
            http: Some(https::build(options)?),
            expected_bus_id: None,
        })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Read discovery without enabling implicit retries or logging credentials.
    pub async fn discover_info(
        &self,
        token: Option<&str>,
        deadline: Duration,
    ) -> Result<BrokerInfo, ClientError> {
        let Endpoint::Https(origin) = &self.endpoint else {
            return Err(ClientError::Protocol(
                "broker discovery requires an HTTPS endpoint".into(),
            ));
        };
        let value = tokio::time::timeout(
            deadline,
            https::discover(
                self.http.as_ref().expect("HTTPS client initialized"),
                origin,
                token,
                deadline,
            ),
        )
        .await
        .map_err(|_| ClientError::RemoteTransport("broker identity query timed out".into()))??;
        let info: BrokerInfo = serde_json::from_value(value)
            .map_err(|_| ClientError::Protocol("invalid broker discovery response".into()))?;
        if info.v != 1
            || !crate::id::is_ulid(&info.bus_id)
            || !crate::id::is_ulid(&info.operation_epoch)
            || self
                .expected_bus_id
                .as_ref()
                .is_some_and(|expected| expected != &info.bus_id)
        {
            return Err(ClientError::Protocol(
                "broker discovery identity does not match selected profile".into(),
            ));
        }
        Ok(info)
    }

    pub fn with_expected_bus_id(mut self, bus_id: String) -> Result<Self, ClientError> {
        if !crate::id::is_ulid(&bus_id) {
            return Err(ClientError::Protocol("invalid expected bus ID".into()));
        }
        self.expected_bus_id = Some(bus_id);
        Ok(self)
    }

    /// The deadline covers connection setup, request write and bounded response.
    pub async fn call(
        &self,
        method: &str,
        params: Value,
        deadline: Duration,
    ) -> Result<Value, ClientError> {
        match &self.endpoint {
            Endpoint::Uds(socket) => {
                tokio::time::timeout(deadline, uds::call(socket, method, params))
                    .await
                    .map_err(|_| ClientError::Timeout)?
            }
            Endpoint::Https(origin) => tokio::time::timeout(
                deadline,
                https::call(
                    self.http.as_ref().expect("HTTPS client initialized"),
                    origin,
                    method,
                    params,
                    deadline,
                    self.expected_bus_id.as_deref(),
                ),
            )
            .await
            .map_err(|_| https::uncertain(method, "HTTPS exchange deadline elapsed"))?,
        }
    }
}

pub(crate) fn encode(method: &str, params: Value) -> Result<Vec<u8>, ClientError> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "jsonrpc":"2.0", "id":"cli", "method":method, "params":params
    }))
    .map_err(|_| ClientError::Protocol("request is not JSON serializable".into()))?;
    if bytes.len() > MAX_RPC_BYTES {
        return Err(ClientError::Protocol("broker request exceeds 4 MiB".into()));
    }
    Ok(bytes)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Value, ClientError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| ClientError::Protocol("broker response is not valid JSON".into()))?;
    let has_result = value.get("result").is_some();
    let has_error = value.get("error").is_some();
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || value.get("id").and_then(Value::as_str) != Some("cli")
        || has_result == has_error
    {
        return Err(ClientError::Protocol(
            "broker response has an invalid version, correlation ID or outcome".into(),
        ));
    }
    if has_error && serde_json::from_value::<RpcError>(value["error"].clone()).is_err() {
        return Err(ClientError::Protocol(
            "invalid broker error response".into(),
        ));
    }
    let response: RpcResponse = serde_json::from_value(value)
        .map_err(|_| ClientError::Protocol("invalid broker response envelope".into()))?;
    response.into_result().map_err(ClientError::Rpc)
}

pub(crate) fn for_root(root: &Path) -> BrokerClient {
    BrokerClient::uds(crate::paths::socket_path(root))
}
