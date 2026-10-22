//! Executable inventory of network methods. Exposure is separately gated.

mod gateway;
mod policy;
mod runtime;

pub use gateway::{call_remote, handle_network_http};
pub use policy::{DevicePrincipal, NetworkPolicy, Permission};
pub use runtime::{AuthContext, NetworkRuntime};

use std::sync::OnceLock;

use serde::Deserialize;
use serde_json::{json, Value};

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body::Body;
use http_body_util::{combinators::BoxBody, BodyExt, Full, Limited};

use crate::error::ErrorCode;
use crate::rpc::{self, RpcError, RpcResponse};
use crate::server::{self, ConnectionSession, DaemonState};
use crate::transport::MAX_RPC_BYTES;

#[derive(Debug, Deserialize)]
pub struct MethodContract {
    pub method: String,
    pub mcp_tool: Option<String>,
    pub principal: String,
    pub permission: String,
    pub resource: String,
    pub retry: String,
    pub authoritative_storage: String,
    pub commit: String,
    pub negative_result: String,
    pub retention: String,
    pub initial_remote_exposure: String,
}

#[derive(Deserialize)]
struct Contracts {
    methods: Vec<MethodContract>,
}

pub fn method_contracts() -> &'static [MethodContract] {
    static CONTRACTS: OnceLock<Vec<MethodContract>> = OnceLock::new();
    CONTRACTS.get_or_init(|| {
        serde_json::from_str::<Contracts>(include_str!("../spec/network/methods.json"))
            .expect("checked-in method contract is valid")
            .methods
    })
}

pub fn is_read(method: &str) -> bool {
    method_contracts()
        .iter()
        .any(|contract| contract.method == method && contract.retry == "read")
}

pub(crate) fn json_response(
    status: StatusCode,
    value: Value,
) -> Response<BoxBody<Bytes, Infallible>> {
    let bytes = serde_json::to_vec(&value).expect("JSON value serializes");
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(bytes)).boxed())
        .expect("valid HTTP response")
}

fn failure(status: StatusCode, error: RpcError) -> Response<BoxBody<Bytes, Infallible>> {
    json_response(
        status,
        serde_json::to_value(RpcResponse::failure(Value::Null, error)).unwrap(),
    )
}

/// The first transport gate intentionally exposes only authenticated reads on
/// disposable loopback fixtures. Network policy activation replaces this gate.
pub(crate) async fn handle_native_read<B>(
    state: &Arc<DaemonState>,
    request: Request<B>,
) -> Response<BoxBody<Bytes, Infallible>>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display,
{
    if request.method() != http::Method::POST {
        return failure(
            StatusCode::METHOD_NOT_ALLOWED,
            RpcError::invalid_request("POST required"),
        );
    }
    let host = request
        .headers()
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<http::uri::Authority>().ok());
    let local_host = host.as_ref().is_some_and(|v| {
        matches!(
            v.host().trim_matches(['[', ']']),
            "localhost" | "127.0.0.1" | "::1"
        )
    });
    if !local_host || request.headers().contains_key(http::header::ORIGIN) {
        return failure(
            StatusCode::FORBIDDEN,
            RpcError::invalid_request("Host or Origin is not allowed"),
        );
    }
    let token = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    let Some(token) = token else {
        return failure(
            StatusCode::UNAUTHORIZED,
            RpcError::application(ErrorCode::Unauthenticated, "bearer credential required"),
        );
    };
    if state.authenticate(&token).await.is_err() {
        return failure(
            StatusCode::UNAUTHORIZED,
            RpcError::application(ErrorCode::Unauthenticated, "invalid bearer credential"),
        );
    }
    if request
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_none_or(|v| v.split(';').next().map(str::trim) != Some("application/json"))
    {
        return failure(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            RpcError::invalid_request("application/json required"),
        );
    }
    let body = request
        .into_body()
        .map_err(|_| std::io::Error::other("body read failed"));
    let bytes = match Limited::new(body, MAX_RPC_BYTES).collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            return failure(
                StatusCode::PAYLOAD_TOO_LARGE,
                RpcError::application(
                    ErrorCode::ResourceExhausted,
                    "request body exceeds limit or is incomplete",
                ),
            )
        }
    };
    let parsed = std::str::from_utf8(&bytes).ok().map(rpc::parse_request);
    let mut rpc = match parsed {
        Some(Ok(rpc)) if !rpc.is_notification => rpc,
        _ => {
            return failure(
                StatusCode::BAD_REQUEST,
                RpcError::invalid_request("one JSON-RPC request with an ID required"),
            )
        }
    };
    if !is_read(&rpc.method) || matches!(rpc.method.as_str(), "echo" | "daemon.status") {
        return failure(
            StatusCode::FORBIDDEN,
            RpcError::invalid_request("method has not passed its remote exposure gate"),
        );
    }
    if rpc.params.get("token").is_some() {
        return failure(
            StatusCode::BAD_REQUEST,
            RpcError::invalid_params("body credentials are not allowed"),
        );
    }
    if rpc.params.is_null() {
        rpc.params = json!({});
    }
    if !rpc.params.is_object() {
        return failure(
            StatusCode::BAD_REQUEST,
            RpcError::invalid_params("object params required"),
        );
    }
    let mut session = ConnectionSession::authenticated(token.clone());
    let response = server::dispatch(state, &mut session, rpc)
        .await
        .expect("request has ID");
    if state.authenticate(&token).await.is_err() {
        return failure(
            StatusCode::UNAUTHORIZED,
            RpcError::application(
                ErrorCode::Unauthenticated,
                "credential revoked while waiting",
            ),
        );
    }
    let encoded = serde_json::to_value(response).expect("response serializes");
    if serde_json::to_vec(&encoded).map_or(true, |v| v.len() > MAX_RPC_BYTES) {
        return failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            RpcError::application(ErrorCode::ResourceExhausted, "response exceeds limit"),
        );
    }
    json_response(StatusCode::OK, encoded)
}
