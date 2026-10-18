//! Newline-delimited JSON-RPC 2.0 request/response types.
//!
//! The daemon accepts exactly one request object per line and emits exactly
//! one response line for requests that carry an `id`. Notifications (requests
//! without `id`) are executed but produce no response. Batch arrays are not
//! part of the NDJSON transport and are rejected as invalid requests.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::ErrorCode;

/// JSON-RPC protocol version implemented here.
pub const JSONRPC_VERSION: &str = "2.0";

/// Invalid JSON was received.
pub const PARSE_ERROR: i64 = -32700;
/// The JSON was valid but not a valid request object.
pub const INVALID_REQUEST: i64 = -32600;
/// The requested method does not exist.
pub const METHOD_NOT_FOUND: i64 = -32601;
/// Method parameters were invalid.
pub const INVALID_PARAMS: i64 = -32602;
/// Unexpected server-side failure.
pub const INTERNAL_ERROR: i64 = -32603;

/// A parsed JSON-RPC request. `id == None` plus `is_notification == false`
/// cannot occur: a present `id` of `null` is preserved as `Some(Value::Null)`.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcRequest {
    pub id: Option<Value>,
    pub method: String,
    pub params: Value,
    pub is_notification: bool,
}

/// A JSON-RPC error object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    /// Creates an error with no `data` member.
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Maps a canonical broker error to its JSON-RPC encoding. The symbolic
    /// name is carried in `error.data.code` so clients need not rely on the
    /// numeric mapping alone.
    pub fn application(code: ErrorCode, detail: impl Into<String>) -> Self {
        let detail = detail.into();
        let message = if detail.is_empty() {
            code.message().to_string()
        } else {
            detail
        };
        Self {
            code: code.rpc_code(),
            message,
            data: Some(json!({ "code": code.as_str() })),
        }
    }

    pub fn parse_error(message: impl Into<String>) -> Self {
        Self::new(PARSE_ERROR, message)
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(INVALID_REQUEST, message)
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(INVALID_PARAMS, message)
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(METHOD_NOT_FOUND, format!("method not found: {method}"))
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(INTERNAL_ERROR, message)
    }
}

/// A JSON-RPC response. Exactly one of `result`/`error` is present.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl RpcResponse {
    /// Successful response.
    pub fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Error response.
    pub fn failure(id: Value, error: RpcError) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id,
            result: None,
            error: Some(error),
        }
    }

    /// Extracts the result or returns the wire error.
    pub fn into_result(self) -> Result<Value, RpcError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok(self.result.unwrap_or(Value::Null))
    }
}

/// Parses one NDJSON request line.
pub fn parse_request(line: &str) -> Result<RpcRequest, RpcError> {
    let value: Value =
        serde_json::from_str(line).map_err(|error| RpcError::parse_error(error.to_string()))?;
    let object = value
        .as_object()
        .ok_or_else(|| RpcError::invalid_request("request must be a JSON object"))?;

    if object.get("jsonrpc").and_then(Value::as_str) != Some(JSONRPC_VERSION) {
        return Err(RpcError::invalid_request("jsonrpc must be \"2.0\""));
    }

    let method = object
        .get("method")
        .and_then(Value::as_str)
        .filter(|method| !method.is_empty())
        .ok_or_else(|| RpcError::invalid_request("method must be a non-empty string"))?
        .to_string();

    let is_notification = !object.contains_key("id");
    let id = object.get("id").cloned();
    if let Some(candidate) = &id {
        if !(candidate.is_null() || candidate.is_string() || candidate.is_number()) {
            return Err(RpcError::invalid_request(
                "id must be a string, number, or null",
            ));
        }
    }

    let params = object.get("params").cloned().unwrap_or(Value::Null);
    if !params.is_null() && !params.is_object() && !params.is_array() {
        return Err(RpcError::invalid_params(
            "params must be an object, array, or null",
        ));
    }

    Ok(RpcRequest {
        id,
        method,
        params,
        is_notification,
    })
}
