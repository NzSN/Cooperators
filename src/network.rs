//! Executable inventory of network methods. Exposure is separately gated.

mod gateway;
pub(crate) mod page;
mod policy;
mod runtime;

pub use gateway::{call_remote, handle_network_http};
pub use policy::{DevicePrincipal, NetworkPolicy, Permission};
pub use runtime::{AuthContext, NetworkRuntime};

use std::sync::OnceLock;

use serde::Deserialize;
use serde_json::Value;

use std::convert::Infallible;

use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::{combinators::BoxBody, BodyExt, Full};

use crate::rpc::{RpcError, RpcResponse};

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
