use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body::Body;
use http_body_util::{combinators::BoxBody, BodyExt, Limited};
use serde_json::{json, Value};

use crate::error::ErrorCode;
use crate::rpc::{self, RpcError, RpcRequest, RpcResponse};
use crate::server::{self, ConnectionSession, DaemonState};
use crate::transport::MAX_RPC_BYTES;

use super::{failure, json_response, AuthContext};

/// Shared policy/identity path for native RPC and HTTP MCP. No body field can
/// replace the authenticated transport principal.
pub async fn call_remote(
    state: &Arc<DaemonState>,
    token: &str,
    method: &str,
    mut params: Value,
) -> Result<Value, RpcError> {
    let network = state
        .network()
        .ok_or_else(|| RpcError::method_not_found(method))?;
    if let Some(body_token) = params.as_object_mut().and_then(|p| p.remove("token")) {
        if body_token.as_str() != Some(token) {
            return Err(RpcError::application(
                ErrorCode::Forbidden,
                "body credential cannot replace transport identity",
            ));
        }
    }
    if params.is_null() {
        params = json!({});
    }
    if !params.is_object() {
        return Err(RpcError::invalid_params("object params required"));
    }
    let context = network
        .authenticate(state, token)
        .await
        .map_err(|e| e.into_rpc_error())?;
    let poll = matches!(method, "wait_message" | "wait_arrivals")
        && params
            .get("timeout_s")
            .and_then(Value::as_u64)
            .unwrap_or(30)
            > 0
        || method == "read_channel"
            && params.get("wait_s").and_then(Value::as_u64).unwrap_or(0) > 0;
    let _permit = network
        .request_permit(&context.principal_id, poll)
        .map_err(|e| e.into_rpc_error())?;
    network
        .authorize(state, &context, method, &params)
        .await
        .map_err(|e| e.into_rpc_error())?;
    if !remote_method_ready(method) {
        return Err(RpcError::application(
            ErrorCode::Forbidden,
            "method has not passed its remote mutation gate",
        ));
    }
    // Bound caller-selected read windows and page sizes before domain dispatch.
    if params
        .get("timeout_s")
        .and_then(Value::as_u64)
        .is_some_and(|s| s > 25)
        || params
            .get("wait_s")
            .and_then(Value::as_u64)
            .is_some_and(|s| s > 25)
        || params
            .get("limit")
            .and_then(Value::as_u64)
            .is_some_and(|n| n > 64)
    {
        return Err(RpcError::invalid_params("remote wait/page bounds exceeded"));
    }
    if matches!(method, "wait_message" | "wait_arrivals") && params.get("timeout_s").is_none() {
        params["timeout_s"] = json!(25);
    }
    if matches!(method, "read_channel" | "wait_arrivals") && params.get("limit").is_none() {
        params["limit"] = json!(64);
    }
    let original = params.clone();
    state.touch_presence(&context.identity).await;
    let result = if method == "wait_message" {
        serde_json::to_value(
            state
                .wait_message_limited(
                    &context.identity.agent_id,
                    params.get("cursor").and_then(Value::as_str),
                    Duration::from_secs(
                        params
                            .get("timeout_s")
                            .and_then(Value::as_u64)
                            .unwrap_or(25),
                    ),
                    Some(MAX_RPC_BYTES - 8192),
                )
                .await
                .map_err(|e| e.into_rpc_error())?,
        )
        .expect("wait result serializes")
    } else if method == "read_doc" {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .expect("authorized name");
        serde_json::to_value(
            state
                .read_doc_limited(
                    name,
                    params.get("version").and_then(Value::as_u64),
                    Some(MAX_RPC_BYTES - 8192),
                )
                .await
                .map_err(|e| e.into_rpc_error())?,
        )
        .expect("document result serializes")
    } else {
        server::dispatch_trusted(
            state,
            &mut ConnectionSession::authenticated(token.into()),
            RpcRequest {
                id: Some(json!("native")),
                method: method.into(),
                params,
                is_notification: false,
            },
        )
        .await
        .expect("request has id")
        .into_result()?
    };
    let current = network
        .authenticate(state, token)
        .await
        .map_err(|e| e.into_rpc_error())?;
    if current.principal_generation != context.principal_generation {
        return Err(RpcError::application(
            ErrorCode::Unauthenticated,
            "principal generation changed",
        ));
    }
    network
        .authorize(state, &current, method, &original)
        .await
        .map_err(|e| e.into_rpc_error())?;
    filter_reply(state, &current, method, result)
}

pub(super) fn remote_method_ready(method: &str) -> bool {
    super::is_read(method) && !matches!(method, "echo" | "daemon.status")
}

fn filter_reply(
    state: &DaemonState,
    context: &AuthContext,
    method: &str,
    mut reply: Value,
) -> Result<Value, RpcError> {
    let policy = state.network().expect("network enabled").policy();
    match method {
        "list_docs" => {
            if let Some(docs) = reply["docs"].as_array_mut() {
                docs.retain(|doc| {
                    doc["name"].as_str().is_some_and(|name| {
                        policy.allows(&context.principal_id, method, &format!("document:{name}"))
                    })
                });
            }
        }
        "list_agents" => {
            if let Some(agents) = reply["agents"].as_array_mut() {
                agents.retain(|agent| {
                    agent["agent_id"].as_str().is_some_and(|id| {
                        policy.allows(&context.principal_id, method, &format!("agent:{id}"))
                    })
                });
            }
        }
        "list_arrival_subscriptions" => {
            if let Some(subscriptions) = reply["subscriptions"].as_array_mut() {
                subscriptions.retain(|s| {
                    reference(&s["resource"]).is_some_and(|r| {
                        policy.allows(&context.principal_id, "subscribe_arrivals", &r)
                    })
                });
            }
        }
        "wait_arrivals" => {
            if let Some(notices) = reply["notices"].as_array() {
                if notices.iter().any(|n| {
                    reference(&n["resource"]).is_none_or(|r| {
                        !policy.allows(&context.principal_id, "subscribe_arrivals", &r)
                    })
                }) {
                    return Err(RpcError::application(ErrorCode::Forbidden,"arrival page contains a resource outside current policy; cursor not advanced"));
                }
            }
        }
        _ => {}
    }
    if crate::rpc::bounded_json_size(&reply, MAX_RPC_BYTES - 1024).is_none() {
        return Err(RpcError::application(
            ErrorCode::ResourceExhausted,
            "remote response exceeds limit",
        ));
    }
    Ok(reply)
}

fn reference(value: &Value) -> Option<String> {
    let kind = value.get("type")?.as_str()?;
    let name = value.get("name")?.as_str()?;
    matches!(kind, "queue" | "document").then(|| format!("{kind}:{name}"))
}

pub async fn handle_network_http<B>(
    state: &Arc<DaemonState>,
    request: Request<B>,
) -> Response<BoxBody<Bytes, Infallible>>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display,
{
    let network = state.network().expect("network policy selected");
    if !network.policy().headers_allowed(request.headers()) {
        return failure(
            StatusCode::FORBIDDEN,
            RpcError::application(ErrorCode::Forbidden, "Host or Origin is not authorized"),
        );
    }
    let token = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .filter(|t| !t.is_empty() && t.len() <= 4096)
        .map(str::to_owned);
    let Some(token) = token else {
        return failure(
            StatusCode::UNAUTHORIZED,
            RpcError::application(ErrorCode::Unauthenticated, "bearer credential required"),
        );
    };
    let path = request.uri().path().to_string();
    if path == "/v1/info" {
        if request.method() != http::Method::GET {
            return failure(
                StatusCode::METHOD_NOT_ALLOWED,
                RpcError::invalid_request("GET required"),
            );
        }
        if network.authenticate(state, &token).await.is_err()
            && network.policy().authenticate_device(&token).is_err()
        {
            return failure(
                StatusCode::UNAUTHORIZED,
                RpcError::application(ErrorCode::Unauthenticated, "approved credential required"),
            );
        }
        return json_response(
            StatusCode::OK,
            json!({"v":1,"bus_id":network.meta.bus_id,"incarnation":network.incarnation,"operation_epoch":network.meta.operation_epoch,"supported_methods":super::method_contracts().iter().filter(|m|remote_method_ready(&m.method)).map(|m|&m.method).collect::<Vec<_>>(),"limits":{"request_bytes":MAX_RPC_BYTES,"response_bytes":MAX_RPC_BYTES,"wait_s":25,"page_items":64}}),
        );
    }
    if request.method() != http::Method::POST {
        return failure(
            StatusCode::METHOD_NOT_ALLOWED,
            RpcError::invalid_request("POST required"),
        );
    }
    if path == "/v1/enroll" {
        if network.policy().authenticate_device(&token).is_err() {
            return failure(
                StatusCode::UNAUTHORIZED,
                RpcError::application(
                    ErrorCode::Unauthenticated,
                    "approved enrollment principal required",
                ),
            );
        }
    } else if network.authenticate(state, &token).await.is_err() {
        return failure(
            StatusCode::UNAUTHORIZED,
            RpcError::application(
                ErrorCode::Unauthenticated,
                "scoped instance credential required",
            ),
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
        .map_err(|_| std::io::Error::other("request body interrupted"));
    let bytes = match tokio::time::timeout(
        Duration::from_secs(10),
        Limited::new(body, MAX_RPC_BYTES).collect(),
    )
    .await
    {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(_)) => {
            return failure(
                StatusCode::PAYLOAD_TOO_LARGE,
                RpcError::application(
                    ErrorCode::ResourceExhausted,
                    "request body exceeds limit or is incomplete",
                ),
            )
        }
        Err(_) => {
            return failure(
                StatusCode::REQUEST_TIMEOUT,
                RpcError::invalid_request("request body deadline exceeded"),
            )
        }
    };
    let request = match std::str::from_utf8(&bytes).ok().map(rpc::parse_request) {
        Some(Ok(request)) if !request.is_notification => request,
        _ => {
            return failure(
                StatusCode::BAD_REQUEST,
                RpcError::invalid_request("one JSON-RPC request with ID required"),
            )
        }
    };
    let id = request.id.clone().expect("not notification");
    if request.params.get("token").is_some() {
        return json_response(
            StatusCode::BAD_REQUEST,
            serde_json::to_value(RpcResponse::failure(
                id,
                RpcError::invalid_params("body credentials are not allowed"),
            ))
            .unwrap(),
        );
    }
    let result = if path == "/v1/enroll" && request.method == "enroll_agent" {
        network
            .enroll(state, &token, request.params)
            .await
            .map_err(|e| e.into_rpc_error())
    } else if path == "/v1/rpc" {
        call_remote(state, &token, &request.method, request.params).await
    } else {
        Err(RpcError::method_not_found(&request.method))
    };
    let status = match &result {
        Err(error) if error.code == ErrorCode::Unauthenticated.rpc_code() => {
            StatusCode::UNAUTHORIZED
        }
        Err(error) if error.code == ErrorCode::Forbidden.rpc_code() => StatusCode::FORBIDDEN,
        Err(error) if error.code == ErrorCode::ResourceExhausted.rpc_code() => {
            StatusCode::TOO_MANY_REQUESTS
        }
        _ => StatusCode::OK,
    };
    let reply = match result {
        Ok(value) => RpcResponse::success(id, value),
        Err(error) => RpcResponse::failure(id, error),
    };
    json_response(status, serde_json::to_value(reply).unwrap())
}
