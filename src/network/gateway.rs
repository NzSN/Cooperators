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

const PAGE_BYTES: usize = MAX_RPC_BYTES - 8192;
const PAGE_ITEMS: usize = 64;

/// Shared admission path for native, UDS and MCP scoped credentials.
pub async fn call_remote(
    state: &Arc<DaemonState>,
    token: &str,
    method: &str,
    mut params: Value,
) -> Result<Value, RpcError> {
    let network = state
        .network()
        .ok_or_else(|| RpcError::method_not_found(method))?;
    let mut policy_changes = network.policy_changes();
    let context = network
        .authenticate(state, token)
        .await
        .map_err(|e| e.into_rpc_error())?;
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
    validate_read_params(method, &params)?;
    let poll = match method {
        "wait_message" | "wait_arrivals" => number(&params, "timeout_s", 25) > 0,
        "read_channel" => number(&params, "wait_s", 0) > 0,
        _ => false,
    };
    // Invalid or unauthorized requests never consume long-poll capacity.
    let _permit = network
        .request_permit(&context.principal_id, poll)
        .map_err(|e| e.into_rpc_error())?;
    state.touch_presence(&context.identity).await;
    let operation = dispatch_read(state, token, &context, method, &params);
    tokio::pin!(operation);
    // Registry retirement/token rotation and wall-clock expiry also cancel a
    // wait, even when no policy replacement occurred. Recheck before return.
    let mut recheck = tokio::time::interval(Duration::from_millis(100));
    recheck.tick().await;
    let result = loop {
        tokio::select! {
            result = &mut operation => break result?,
            _ = async {
                tokio::select! { _ = recheck.tick() => {}, _ = policy_changes.changed() => {} }
            }, if poll => {
                let current = network.authenticate(state, token).await.map_err(|e| e.into_rpc_error())?;
                network.authorize(state, &current, method, &params).await.map_err(|e| e.into_rpc_error())?;
            }
        }
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
        .authorize(state, &current, method, &params)
        .await
        .map_err(|e| e.into_rpc_error())?;
    filter_reply(state, &current, method, result)
}

fn number(params: &Value, field: &str, default: u64) -> u64 {
    params.get(field).and_then(Value::as_u64).unwrap_or(default)
}

fn validate_read_params(method: &str, params: &Value) -> Result<(), RpcError> {
    let fields: &[&str] = match method {
        "wait_message" => &["cursor", "timeout_s"],
        "wait_arrivals" => &["cursor", "timeout_s", "limit"],
        "read_channel" => &["channel", "cursor", "wait_s", "limit"],
        "read_doc" => &["name", "version"],
        "list_docs" => &["prefix", "cursor", "limit"],
        "list_agents" => &["include_stale", "cursor", "limit"],
        "list_arrival_subscriptions" => &["cursor", "limit"],
        _ => &[],
    };
    for (field, value) in params.as_object().expect("object checked") {
        if !fields.contains(&field.as_str()) {
            return Err(RpcError::invalid_params(format!(
                "unexpected parameter {field}"
            )));
        }
        let valid = match field.as_str() {
            "limit" => value
                .as_u64()
                .is_some_and(|v| (1..=PAGE_ITEMS as u64).contains(&v)),
            "timeout_s" | "wait_s" => value.as_u64().is_some_and(|v| v <= 25),
            "version" => value.as_u64().is_some_and(|v| v > 0),
            "include_stale" => value.is_boolean(),
            "name" | "channel" | "prefix" => value.as_str().is_some_and(|s| s.len() <= 128),
            "cursor" if matches!(method, "wait_message" | "wait_arrivals") => value
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 512),
            "cursor" if method == "read_channel" => {
                if let Some(object) = value.as_object() {
                    object.keys().all(|k| matches!(k.as_str(), "file" | "seq"))
                        && object.get("file").is_none_or(|v| {
                            v.as_str().is_some_and(|s| !s.is_empty() && s.len() <= 256)
                        })
                } else {
                    value.is_string() || value.as_u64().is_some()
                }
            }
            "cursor" => value.is_object(),
            _ => true,
        };
        if !valid {
            return Err(RpcError::invalid_params(format!(
                "invalid {field} type or remote bound"
            )));
        }
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ListCursor {
    v: u8,
    bus_id: String,
    agent_id: String,
    generation: u64,
    method: String,
    filter: Value,
    after: String,
}

fn list_after(
    state: &DaemonState,
    context: &AuthContext,
    method: &str,
    params: &Value,
    filter: &Value,
) -> Result<Option<String>, RpcError> {
    let Some(value) = params.get("cursor") else {
        return Ok(None);
    };
    let cursor: ListCursor = serde_json::from_value(value.clone())
        .map_err(|_| RpcError::invalid_params("invalid list cursor"))?;
    if cursor.v != 1
        || cursor.bus_id != state.network().unwrap().meta.bus_id
        || cursor.agent_id != context.identity.agent_id
        || cursor.generation != context.principal_generation
        || cursor.method != method
        || &cursor.filter != filter
        || cursor.after.is_empty()
        || cursor.after.len() > 256
    {
        return Err(RpcError::invalid_params(
            "list cursor scope or filter mismatch",
        ));
    }
    Ok(Some(cursor.after))
}

fn list_reply(
    state: &DaemonState,
    context: &AuthContext,
    method: &str,
    filter: Value,
    mut reply: Value,
    after: Option<String>,
) -> Value {
    reply["next_cursor"] = after
        .map(|after| {
            serde_json::to_value(ListCursor {
                v: 1,
                bus_id: state.network().unwrap().meta.bus_id.clone(),
                agent_id: context.identity.agent_id.clone(),
                generation: context.principal_generation,
                method: method.into(),
                filter,
                after,
            })
            .expect("cursor serializes")
        })
        .unwrap_or(Value::Null);
    reply
}

async fn dispatch_read(
    state: &Arc<DaemonState>,
    token: &str,
    context: &AuthContext,
    method: &str,
    params: &Value,
) -> Result<Value, RpcError> {
    let limit = number(params, "limit", PAGE_ITEMS as u64) as usize;
    let result = match method {
        "wait_message" => serde_json::to_value(
            state
                .wait_message_limited(
                    &context.identity.agent_id,
                    params.get("cursor").and_then(Value::as_str),
                    Duration::from_secs(number(params, "timeout_s", 25)),
                    Some(PAGE_BYTES),
                )
                .await
                .map_err(|e| e.into_rpc_error())?,
        ),
        "wait_arrivals" => serde_json::to_value(
            state
                .wait_arrivals_limited(
                    &context.identity,
                    params.get("cursor").and_then(Value::as_str),
                    Duration::from_secs(number(params, "timeout_s", 25)),
                    limit,
                    PAGE_BYTES,
                )
                .await
                .map_err(|e| e.into_rpc_error())?,
        ),
        "read_doc" => serde_json::to_value(
            state
                .read_doc_limited(
                    params["name"].as_str().expect("authorized name"),
                    params.get("version").and_then(Value::as_u64),
                    Some(PAGE_BYTES),
                )
                .await
                .map_err(|e| e.into_rpc_error())?,
        ),
        "read_channel" => {
            let channel = params["channel"].as_str().expect("authorized channel");
            let cursor = params
                .get("cursor")
                .map(|v| crate::channels::parse_channel_cursor(v, channel))
                .transpose()
                .map_err(|e| e.into_rpc_error())?;
            serde_json::to_value(
                state
                    .read_channel_limited(
                        channel,
                        cursor,
                        limit,
                        Duration::from_secs(number(params, "wait_s", 0)),
                        Some(PAGE_BYTES),
                    )
                    .await
                    .map_err(|e| e.into_rpc_error())?,
            )
        }
        "list_docs" | "list_agents" | "list_arrival_subscriptions" => {
            let filter = match method {
                "list_docs" => json!({"prefix":params.get("prefix").and_then(Value::as_str)}),
                "list_agents" => {
                    json!({"include_stale":params.get("include_stale").and_then(Value::as_bool).unwrap_or(false)})
                }
                _ => json!({}),
            };
            let after = list_after(state, context, method, params, &filter)?;
            let policy = state.network().unwrap().policy();
            let principal = context.principal_id.clone();
            let (reply, next) = match method {
                "list_docs" => {
                    let (reply, next) = state
                        .list_docs_page(
                            params.get("prefix").and_then(Value::as_str),
                            after.as_deref(),
                            limit,
                            PAGE_BYTES,
                            |name| policy.allows(&principal, method, &format!("document:{name}")),
                        )
                        .await
                        .map_err(|e| e.into_rpc_error())?;
                    (serde_json::to_value(reply).unwrap(), next)
                }
                "list_agents" => {
                    let (agents, next) = state
                        .list_agents_page(
                            filter["include_stale"].as_bool().unwrap(),
                            after.as_deref(),
                            limit,
                            PAGE_BYTES,
                            |id| policy.allows(&principal, method, &format!("agent:{id}")),
                        )
                        .await
                        .map_err(|e| e.into_rpc_error())?;
                    (json!({"agents":agents}), next)
                }
                _ => {
                    let (reply, next) = state
                        .list_arrival_subscriptions_page(
                            &context.identity,
                            after,
                            limit,
                            PAGE_BYTES,
                            move |resource| {
                                let resource = match resource {
                                    crate::arrivals::ArrivalResource::Queue { name } => {
                                        format!("queue:{name}")
                                    }
                                    crate::arrivals::ArrivalResource::Document { name } => {
                                        format!("document:{name}")
                                    }
                                };
                                policy.allows(&principal, "subscribe_arrivals", &resource)
                            },
                        )
                        .await
                        .map_err(|e| e.into_rpc_error())?;
                    (serde_json::to_value(reply).unwrap(), next)
                }
            };
            return Ok(list_reply(state, context, method, filter, reply, next));
        }
        _ => {
            return server::dispatch_trusted(
                state,
                &mut ConnectionSession::authenticated(token.into()),
                RpcRequest {
                    id: Some(json!("native")),
                    method: method.into(),
                    params: params.clone(),
                    is_notification: false,
                },
            )
            .await
            .expect("request has id")
            .into_result()
        }
    };
    Ok(result.expect("read response serializes"))
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
                let before = docs.len();
                docs.retain(|doc| {
                    doc["name"].as_str().is_some_and(|name| {
                        policy.allows(&context.principal_id, method, &format!("document:{name}"))
                    })
                });
                if docs.len() != before {
                    return Err(policy_changed_page());
                }
            }
        }
        "list_agents" => {
            if let Some(agents) = reply["agents"].as_array_mut() {
                let before = agents.len();
                agents.retain(|agent| {
                    agent["agent_id"].as_str().is_some_and(|id| {
                        policy.allows(&context.principal_id, method, &format!("agent:{id}"))
                    })
                });
                if agents.len() != before {
                    return Err(policy_changed_page());
                }
            }
        }
        "list_arrival_subscriptions" => {
            if let Some(subscriptions) = reply["subscriptions"].as_array_mut() {
                let before = subscriptions.len();
                subscriptions.retain(|s| {
                    reference(&s["resource"]).is_some_and(|r| {
                        policy.allows(&context.principal_id, "subscribe_arrivals", &r)
                    })
                });
                if subscriptions.len() != before {
                    return Err(policy_changed_page());
                }
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

fn policy_changed_page() -> RpcError {
    RpcError::application(
        ErrorCode::Forbidden,
        "page authorization changed; cursor not advanced",
    )
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
        .filter(|_| {
            request
                .headers()
                .get_all(http::header::AUTHORIZATION)
                .iter()
                .count()
                == 1
        })
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
    if !matches!(path.as_str(), "/v1/info" | "/v1/enroll" | "/v1/rpc")
        || request.uri().query().is_some()
    {
        return failure(
            StatusCode::NOT_FOUND,
            RpcError::invalid_request("unknown network endpoint"),
        );
    }
    if path == "/v1/info" {
        if request.method() != http::Method::GET {
            return failure(
                StatusCode::METHOD_NOT_ALLOWED,
                RpcError::invalid_request("GET required"),
            );
        }
        let principal = match network.authenticate(state, &token).await {
            Ok(context) => context.principal_id,
            Err(_) => match network.policy().authenticate_device(&token) {
                Ok(device) => device.principal_id.clone(),
                Err(_) => {
                    return failure(
                        StatusCode::UNAUTHORIZED,
                        RpcError::application(
                            ErrorCode::Unauthenticated,
                            "approved credential required",
                        ),
                    )
                }
            },
        };
        let _permit = match network.request_permit(&principal, false) {
            Ok(permit) => permit,
            Err(error) => {
                let error = error.into_rpc_error();
                return failure(error_status(&error), error);
            }
        };
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
    let principal = if path == "/v1/enroll" {
        match network.policy().authenticate_device(&token) {
            Ok(device) if device.allow_enroll => device.principal_id.clone(),
            Ok(_) => {
                return failure(
                    StatusCode::FORBIDDEN,
                    RpcError::application(ErrorCode::Forbidden, "enrollment is not authorized"),
                )
            }
            Err(error) => return failure(StatusCode::UNAUTHORIZED, error.into_rpc_error()),
        }
    } else {
        match network.authenticate(state, &token).await {
            Ok(context) => context.principal_id,
            Err(error) => return failure(StatusCode::UNAUTHORIZED, error.into_rpc_error()),
        }
    };
    let body_permit = match network.request_permit(&principal, false) {
        Ok(permit) => permit,
        Err(error) => {
            let error = error.into_rpc_error();
            return failure(error_status(&error), error);
        }
    };
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
        Ok(Err(error)) => {
            if !error.is::<http_body_util::LengthLimitError>() {
                return failure(
                    StatusCode::BAD_REQUEST,
                    RpcError::invalid_request("request body interrupted"),
                );
            }
            return failure(
                StatusCode::PAYLOAD_TOO_LARGE,
                RpcError::application(ErrorCode::ResourceExhausted, "request body exceeds limit"),
            );
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
    if crate::rpc::bounded_json_size(&id, 1024).is_none() {
        return failure(
            StatusCode::BAD_REQUEST,
            RpcError::invalid_request("request ID exceeds limit"),
        );
    }
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
    // Execution obtains its own permit after parameter and resource checks.
    drop(body_permit);
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
    let status = result.as_ref().err().map_or(StatusCode::OK, error_status);
    let reply = match result {
        Ok(value) => RpcResponse::success(id, value),
        Err(error) => RpcResponse::failure(id, error),
    };
    let reply = serde_json::to_value(reply).unwrap();
    if crate::rpc::bounded_json_size(&reply, MAX_RPC_BYTES).is_none() {
        return failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            RpcError::application(
                ErrorCode::ResourceExhausted,
                "remote response exceeds limit",
            ),
        );
    }
    json_response(status, reply)
}

fn error_status(error: &RpcError) -> StatusCode {
    match error.code {
        rpc::INVALID_PARAMS | rpc::INVALID_REQUEST | rpc::PARSE_ERROR => StatusCode::BAD_REQUEST,
        rpc::METHOD_NOT_FOUND => StatusCode::NOT_FOUND,
        rpc::INTERNAL_ERROR => StatusCode::INTERNAL_SERVER_ERROR,
        code if code == ErrorCode::Unauthenticated.rpc_code() => StatusCode::UNAUTHORIZED,
        code if code == ErrorCode::Forbidden.rpc_code() => StatusCode::FORBIDDEN,
        code if code == ErrorCode::ResourceExhausted.rpc_code() => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::OK,
    }
}
