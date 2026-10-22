//! Typed normalization and independently owned resource operations.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::envelope::Body;
use crate::error::{BrokerError, ErrorCode};
use crate::operations::{DomainAdmission, DomainTicket, OperationContext, OperationOutcome};
use crate::registry::AgentIdentity;
use crate::rpc::RpcError;
use crate::server::DaemonState;

/// Input and original result copies must fit a bounded remote response page.
pub(crate) const MAX_DOMAIN_INPUT_BYTES: usize = crate::operations::MAX_DOMAIN_INPUT_BYTES;

pub(crate) fn is_domain_mutation(method: &str) -> bool {
    matches!(
        method,
        "publish"
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

fn string<'a>(params: &'a Value, name: &str, limit: usize) -> Result<&'a str, RpcError> {
    params
        .get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= limit)
        .ok_or_else(|| RpcError::invalid_params(format!("invalid {name}")))
}

fn integer(params: &Value, name: &str, default: Option<u64>, max: u64) -> Result<u64, RpcError> {
    let value = match params.get(name) {
        None => default,
        Some(value) => value.as_u64(),
    };
    value
        .filter(|v| *v > 0 && *v <= max)
        .ok_or_else(|| RpcError::invalid_params(format!("invalid {name}")))
}

fn body(params: &Value) -> Result<Body, RpcError> {
    if params.get("body").is_some() && params.get("text").is_some() {
        return Err(RpcError::invalid_params("choose body or text"));
    }
    let value = params
        .get("body")
        .or_else(|| params.get("text"))
        .ok_or_else(|| RpcError::invalid_params("body or text required"))?;
    if let Some(text) = value.as_str() {
        return Ok(Body::new("text/plain", text));
    }
    let object = value
        .as_object()
        .ok_or_else(|| RpcError::invalid_params("invalid body"))?;
    if object
        .keys()
        .any(|k| !matches!(k.as_str(), "mime" | "text"))
    {
        return Err(RpcError::invalid_params("invalid body fields"));
    }
    let text = object
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::invalid_params("body.text string required"))?;
    let mime = match object.get("mime") {
        Some(value) => value
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 256)
            .ok_or_else(|| RpcError::invalid_params("invalid body.mime"))?,
        None => "text/plain",
    };
    Ok(Body::new(mime, text))
}

/// Normalize defaults and alternate body spellings before computing identity.
/// Unknown fields, credentials and the legacy dedupe key are never digested.
pub(crate) fn normalize(method: &str, params: &Value) -> Result<Value, RpcError> {
    let fields: &[&str] = match method {
        "publish" => &["channel", "body", "text"],
        "create_queue" => &["queue", "name"],
        "enqueue" => &["queue", "payload", "max_attempts"],
        "claim_task" => &["queue", "lease_ttl_s"],
        "renew_lease" => &["lease_id"],
        "complete_task" => &["lease_id", "fence", "outcome", "receipt"],
        "fail_task" => &["lease_id", "fence", "reason"],
        "publish_doc" => &["name", "mime", "body", "text", "title"],
        "update_doc" => &[
            "name",
            "base_version",
            "body",
            "text",
            "title",
            "lease_id",
            "fence",
        ],
        "claim_doc" => &["name", "lease_ttl_s"],
        "release_doc" => &["lease_id", "fence"],
        _ => return Err(RpcError::method_not_found(method)),
    };
    let object = params
        .as_object()
        .ok_or_else(|| RpcError::invalid_params("object params required"))?;
    if object.keys().any(|k| !fields.contains(&k.as_str())) {
        return Err(RpcError::invalid_params(
            "unsupported remote mutation parameter",
        ));
    }
    let normalized = match method {
        "publish" => {
            let channel = string(params, "channel", 128)?;
            crate::messaging::validate_channel_name(channel)
                .map_err(BrokerError::into_rpc_error)?;
            json!({"channel":channel,"body":body(params)?})
        }
        "create_queue" | "enqueue" | "claim_task" => {
            if params.get("queue").is_some() && params.get("name").is_some() {
                return Err(RpcError::invalid_params("choose queue or name"));
            }
            let queue = string(
                params,
                if method == "create_queue" && params.get("queue").is_none() {
                    "name"
                } else {
                    "queue"
                },
                128,
            )?;
            crate::queues::validate_queue_name(queue).map_err(BrokerError::into_rpc_error)?;
            match method {
                "create_queue" => json!({"queue":queue}),
                "enqueue" => {
                    json!({"queue":queue,"payload":params.get("payload").ok_or_else(||RpcError::invalid_params("payload required"))?,"max_attempts":integer(params,"max_attempts",Some(crate::queues::DEFAULT_MAX_ATTEMPTS),1000)?})
                }
                _ => {
                    json!({"queue":queue,"lease_ttl_s":integer(params,"lease_ttl_s",Some(60),crate::queues::MAX_LEASE_TTL_S)?})
                }
            }
        }
        "publish_doc" | "update_doc" | "claim_doc" => {
            let name = string(params, "name", 128)?;
            crate::docs::validate_doc_name(name).map_err(BrokerError::into_rpc_error)?;
            if method == "claim_doc" {
                json!({"name":name,"lease_ttl_s":integer(params,"lease_ttl_s",Some(60),crate::queues::MAX_LEASE_TTL_S)?})
            } else {
                let mut body = body(params)?;
                let title = match params.get("title") {
                    Some(Value::Null) | None => None,
                    Some(Value::String(value)) if value.len() <= 4096 => Some(value.as_str()),
                    _ => return Err(RpcError::invalid_params("invalid title")),
                };
                if method == "publish_doc" {
                    let mime = match params.get("mime") {
                        None | Some(Value::Null) => body.mime.clone(),
                        Some(_) => string(params, "mime", 256)?.into(),
                    };
                    body.mime = mime.clone();
                    json!({"name":name,"mime":mime,"body":body,"title":title.unwrap_or("")})
                } else {
                    let lease = params.get("lease_id").filter(|v| !v.is_null());
                    let fence = params.get("fence").filter(|v| !v.is_null());
                    if lease.is_some() != fence.is_some() {
                        return Err(RpcError::invalid_params(
                            "lease_id and fence must be supplied together",
                        ));
                    }
                    let lease = lease.map(|_| string(params, "lease_id", 128)).transpose()?;
                    let fence = fence
                        .map(|_| integer(params, "fence", None, u64::MAX))
                        .transpose()?;
                    json!({"name":name,"base_version":integer(params,"base_version",None,u64::MAX)?,"body":body,"title":title,"lease_id":lease,"fence":fence})
                }
            }
        }
        _ => {
            let lease = string(params, "lease_id", 128)?;
            match method {
                "renew_lease" => json!({"lease_id":lease}),
                "complete_task" => {
                    json!({"lease_id":lease,"fence":integer(params,"fence",None,u64::MAX)?,"outcome":params.get("outcome").ok_or_else(||RpcError::invalid_params("outcome required"))?,"receipt":params.get("receipt").cloned().unwrap_or(Value::Null)})
                }
                "fail_task" => {
                    let reason = string(params, "reason", 4096)?;
                    if reason.trim().is_empty() {
                        return Err(RpcError::invalid_params("reason must not be blank"));
                    }
                    json!({"lease_id":lease,"fence":integer(params,"fence",None,u64::MAX)?,"reason":reason})
                }
                "release_doc" => {
                    json!({"lease_id":lease,"fence":integer(params,"fence",None,u64::MAX)?})
                }
                _ => unreachable!(),
            }
        }
    };
    if crate::rpc::bounded_json_size(&normalized, MAX_DOMAIN_INPUT_BYTES).is_none() {
        return Err(RpcError::application(
            ErrorCode::ResourceExhausted,
            "remote mutation input exceeds 256 KiB",
        ));
    }
    Ok(normalized)
}

/// Admission's owner is detached from the requesting connection. No await may
/// intervene between reserving a new identity and transferring its ticket.
pub(crate) async fn execute(
    state: &Arc<DaemonState>,
    identity: &AgentIdentity,
    context: OperationContext,
) -> Result<Value, RpcError> {
    let admission = state
        .operations
        .begin_domain(context.clone(), crate::id::unix_millis())
        .map_err(BrokerError::into_rpc_error)?;
    let waiter = match admission {
        DomainAdmission::Existing(waiter) => waiter,
        DomainAdmission::New(ticket) => {
            let waiter = ticket.waiter();
            let state = Arc::clone(state);
            let identity = identity.clone();
            tokio::spawn(async move {
                let _guard = state.send_lock.lock().await;
                let result = invoke(&state, &identity, &context, &ticket).await;
                #[cfg(feature = "mbt-injection")]
                let result = if result.is_ok() && ticket.effect_attempted() {
                    let (kind, name) = context
                        .resource
                        .split_once(':')
                        .expect("validated resource");
                    let directory = match kind {
                        "channel" => "channels",
                        "queue" => "queues",
                        "document" => "docs",
                        _ => unreachable!(),
                    };
                    let path = state.root.join(directory).join(format!("{name}.jsonl"));
                    match crate::faults::reach(
                        path,
                        crate::faults::FaultPoint::AfterOperationSyncBeforePublish,
                    ) {
                        Ok(()) => result,
                        Err(error) => Err(error.into()),
                    }
                } else {
                    result
                };
                match result {
                    Ok(value) if ticket.effect_attempted() => {
                        if ticket.finish_success(value).is_err() {
                            ticket.finish_unknown();
                        }
                    }
                    Ok(value) => {
                        if state
                            .operations
                            .finish_no_effect(&ticket, OperationOutcome::Success(value))
                            .is_err()
                        {
                            ticket.finish_unknown();
                        }
                    }
                    Err(error)
                        if !ticket.effect_attempted()
                            && matches!(
                                error,
                                BrokerError::Application(..) | BrokerError::ApplicationData(..)
                            ) =>
                    {
                        if state
                            .operations
                            .finish_no_effect(
                                &ticket,
                                OperationOutcome::Error(error.into_rpc_error()),
                            )
                            .is_err()
                        {
                            ticket.finish_unknown();
                        }
                    }
                    Err(_) => ticket.finish_unknown(),
                }
            });
            waiter
        }
    };
    waiter.wait().await
}

async fn invoke(
    state: &DaemonState,
    identity: &AgentIdentity,
    context: &OperationContext,
    ticket: &DomainTicket,
) -> Result<Value, BrokerError> {
    let p = &context.input;
    let text = |key: &str| p[key].as_str().expect("normalized string");
    let number = |key: &str| p[key].as_u64().expect("normalized integer");
    let body = || serde_json::from_value::<Body>(p["body"].clone()).expect("normalized body");
    fn value<T: serde::Serialize>(value: T) -> Result<Value, BrokerError> {
        serde_json::to_value(value)
            .map_err(|_| BrokerError::internal("domain receipt serialization failed"))
    }
    match context.method.as_str() {
        "publish" => value(
            state
                .publish_operation(identity, text("channel"), body(), ticket)
                .await?,
        ),
        "create_queue" => value(state.create_queue_operation(text("queue"), ticket).await?),
        "enqueue" => value(
            state
                .enqueue_operation(
                    text("queue"),
                    p["payload"].clone(),
                    Some(number("max_attempts")),
                    ticket,
                )
                .await?,
        ),
        "claim_task" => value(
            state
                .claim_task_operation(identity, text("queue"), number("lease_ttl_s"), ticket)
                .await?,
        ),
        "renew_lease" => value(
            state
                .renew_lease_operation(identity, text("lease_id"), ticket)
                .await?,
        ),
        "complete_task" => value(
            state
                .complete_task_operation(
                    identity,
                    text("lease_id"),
                    number("fence"),
                    p["outcome"].clone(),
                    p.get("receipt").filter(|v| !v.is_null()).cloned(),
                    ticket,
                )
                .await?,
        ),
        "fail_task" => value(
            state
                .fail_task_operation(
                    identity,
                    text("lease_id"),
                    number("fence"),
                    text("reason"),
                    ticket,
                )
                .await?,
        ),
        "publish_doc" => value(
            state
                .publish_doc_operation(
                    identity,
                    text("name"),
                    text("mime"),
                    body(),
                    Some(text("title").into()),
                    ticket,
                )
                .await?,
        ),
        "update_doc" => value(
            state
                .update_doc_operation(
                    identity,
                    text("name"),
                    number("base_version"),
                    body(),
                    p["title"].as_str().map(str::to_owned),
                    p["lease_id"].as_str().map(|lease| (lease, number("fence"))),
                    ticket,
                )
                .await?,
        ),
        "claim_doc" => value(
            state
                .claim_doc_operation(identity, text("name"), number("lease_ttl_s"), ticket)
                .await?,
        ),
        "release_doc" => value(
            state
                .release_doc_operation(identity, text("lease_id"), number("fence"), ticket)
                .await?,
        ),
        _ => Err(BrokerError::invalid("unsupported domain operation")),
    }
}
