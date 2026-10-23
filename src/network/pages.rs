//! Bounded, authorized remote list pages. Cursors are query and principal scoped.
use super::AuthContext;
use crate::rpc::RpcError;
use crate::server::DaemonState;
use serde_json::{json, Value};

pub(super) async fn list_page(
    state: &DaemonState,
    context: &AuthContext,
    method: &str,
    params: &Value,
) -> Result<Value, RpcError> {
    let policy = state.network().expect("network enabled").policy();
    let prefix = params.get("prefix").and_then(Value::as_str);
    let include_stale = params
        .get("include_stale")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let limit = params.get("limit").and_then(Value::as_u64).unwrap_or(64) as usize;
    let scope = json!({"v":1,"bus_id":state.network().unwrap().meta.bus_id,
        "method":method,"principal_id":context.principal_id,"agent_id":context.identity.agent_id,"generation":context.principal_generation,
        "prefix":prefix,"include_stale":include_stale});
    let after = match params.get("cursor") {
        None | Some(Value::Null) => None,
        Some(cursor) => {
            if cursor.get("scope") != Some(&scope)
                || cursor.as_object().is_none_or(|o| o.len() != 2)
            {
                return Err(RpcError::invalid_params(
                    "list cursor does not match this query and principal",
                ));
            }
            Some(
                cursor
                    .get("after")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty() && s.len() <= 256)
                    .ok_or_else(|| RpcError::invalid_params("invalid list cursor"))?,
            )
        }
    };
    let mut reply = match method {
        "list_docs" => serde_json::to_value(
            state
                .list_docs_page(prefix, after, limit, |name| {
                    policy.allows(&context.principal_id, method, &format!("document:{name}"))
                })
                .await
                .map_err(|e| e.into_rpc_error())?,
        )
        .unwrap(),
        "list_agents" => json!({"agents":state.list_agents_page(include_stale, after, limit,
            |_, id| policy.allows(&context.principal_id, method, &format!("agent:{id}")))
            .await.map_err(|e| e.into_rpc_error())?}),
        "list_arrival_subscriptions" => serde_json::to_value(
            state
                .arrivals
                .as_ref()
                .ok_or_else(|| RpcError::invalid_params("arrival notifications are not enabled"))?
                .list_page(&context.identity.agent_id, after, limit, |resource| {
                    policy.allows(
                        &context.principal_id,
                        "subscribe_arrivals",
                        &format!("{}:{}", resource.stream_type(), resource.name()),
                    )
                })
                .map_err(|e| e.into_rpc_error())?,
        )
        .unwrap(),
        _ => unreachable!(),
    };
    let rows = if method == "list_arrival_subscriptions" {
        &reply["subscriptions"]
    } else if method == "list_docs" {
        &reply["docs"]
    } else {
        &reply["agents"]
    };
    // An empty page terminates traversal; never skip an item that did not fit.
    let next = rows
        .as_array()
        .and_then(|rows| rows.last())
        .and_then(|row| {
            row[if method == "list_arrival_subscriptions" {
                "subscription_id"
            } else {
                "name"
            }]
            .as_str()
        })
        .map(|name| json!({"scope":scope,"after":name}));
    reply["next_cursor"] = next.unwrap_or(Value::Null);
    Ok(reply)
}
