mod common;

use bytes::Bytes;
use cooperators::{
    error::ErrorCode,
    mcp,
    network::NetworkPolicy,
    rpc::RpcRequest,
    server::{self, ConnectionSession, DaemonConfig, DaemonState},
};
use http::{Request, StatusCode};
use http_body_util::Full;
use serde_json::{json, Value};
use std::sync::Arc;

async fn fixture() -> (common::TempDir, Arc<DaemonState>, String, String) {
    let temp = common::TempDir::new("network-facades");
    let credential = cooperators::registry::new_token().unwrap();
    let policy: NetworkPolicy = serde_json::from_value(json!({
        "v":1,"allowed_hosts":["localhost"],"devices":[{
            "principal_id":"device","token_hash":cooperators::registry::token_hash(&credential),
            "expires_at_ms":cooperators::id::unix_millis()+60000,
            "allow_enroll":true,"max_instances":8,"instance_ttl_s":60,
            "permissions":[{"method":"ping","resources":["*"]}]
        }]
    }))
    .unwrap();
    let mut config = DaemonConfig::new(temp.path());
    config.network_policy = Some(policy);
    let state = Arc::new(DaemonState::with_config(config).unwrap());
    let enrolled = state
        .network()
        .unwrap()
        .enroll(
            &state,
            &credential,
            json!({
                "launch_key":cooperators::registry::new_token().unwrap(),
                "kind":"test","display_name":"scoped"
            }),
        )
        .await
        .unwrap();
    let local = state
        .register_agent("local".into(), None, None, None)
        .await
        .unwrap();
    (
        temp,
        state,
        enrolled["token"].as_str().unwrap().into(),
        local.token,
    )
}

async fn call(
    state: &Arc<DaemonState>,
    session: &mut ConnectionSession,
    method: &str,
    params: Value,
) -> cooperators::rpc::RpcResponse {
    server::dispatch(
        state,
        session,
        RpcRequest {
            id: Some(json!(1)),
            method: method.into(),
            params,
            is_notification: false,
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn scoped_session_never_switches_to_local_body_credential_or_falls_back_after_retirement() {
    let (_temp, state, token, local) = fixture().await;
    let mut session = ConnectionSession::default();
    assert!(call(&state, &mut session, "ping", json!({"token":token}))
        .await
        .into_result()
        .is_ok());
    let error = call(
        &state,
        &mut session,
        "daemon.status",
        json!({"token":local}),
    )
    .await
    .into_result()
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::Forbidden.rpc_code());
    assert!(call(&state, &mut session, "ping", json!({}))
        .await
        .into_result()
        .is_ok());
    state.retire_agent(&token).await.unwrap();
    for method in ["ping", "daemon.status", "register_agent", "enroll_agent"] {
        let error = call(&state, &mut session, method, json!({}))
            .await
            .into_result()
            .unwrap_err();
        assert_eq!(
            error.code,
            ErrorCode::Unauthenticated.rpc_code(),
            "{method}"
        );
    }
}

#[tokio::test]
async fn unknown_or_retired_scoped_bearer_cannot_use_unauthenticated_local_routes() {
    let (_temp, state, token, _local) = fixture().await;
    state.retire_agent(&token).await.unwrap();
    for bearer in [&token, "invalid-bearer"] {
        for method in ["ping", "daemon.status", "register_agent", "enroll_agent"] {
            let error = call(
                &state,
                &mut ConnectionSession::default(),
                method,
                json!({"token":bearer}),
            )
            .await
            .into_result()
            .unwrap_err();
            assert_eq!(
                error.code,
                ErrorCode::Unauthenticated.rpc_code(),
                "{method}"
            );
        }
    }
}

#[tokio::test]
async fn mcp_admission_is_before_body_and_releases_capacity_after_oversized_body() {
    let (_temp, state, token, _local) = fixture().await;
    let service = mcp::http_service(state.clone());
    let response = mcp::handle_http(
        &service,
        &state,
        Request::post("/mcp")
            .header("host", "localhost")
            .header("authorization", format!("Bearer {token}"))
            .body(Full::new(Bytes::from(vec![
                b' ';
                cooperators::transport::MAX_RPC_BYTES
                    + 1
            ])))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let network = state.network().unwrap();
    let mut permits = Vec::new();
    for _ in 0..network.policy().requests_per_principal {
        permits.push(network.request_permit("device", false).unwrap());
    }
    let response = mcp::handle_http(
        &service,
        &state,
        Request::post("/mcp")
            .header("host", "localhost")
            .header("authorization", format!("Bearer {token}"))
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn mcp_rejects_ambiguous_authorization_and_oversized_response_ids() {
    let (_temp, state, token, _local) = fixture().await;
    let service = mcp::http_service(state.clone());
    let response = mcp::handle_http(
        &service,
        &state,
        Request::post("/mcp")
            .header("host", "localhost")
            .header("authorization", format!("Bearer {token}"))
            .header("authorization", format!("Bearer {token}"))
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = mcp::handle_http(
        &service,
        &state,
        Request::post("/mcp")
            .header("host", "localhost")
            .header("authorization", format!("Bearer {token}"))
            .body(Full::new(Bytes::from(
                json!({"jsonrpc":"2.0","id":"a".repeat(513),"method":"tools/list"}).to_string(),
            )))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    for value in [json!(42), json!(""), json!([])] {
        let error = call(
            &state,
            &mut ConnectionSession::default(),
            "daemon.status",
            json!({"token":value}),
        )
        .await
        .into_result()
        .unwrap_err();
        assert_eq!(error.code, cooperators::rpc::INVALID_PARAMS);
    }
}

#[tokio::test]
async fn mcp_counts_text_and_structured_copies_against_response_budget() {
    let (_temp, state, token, local) = fixture().await;
    let mut policy = state.network().unwrap().policy();
    policy.devices[0]
        .permissions
        .push(cooperators::network::Permission {
            method: "read_doc".into(),
            resources: vec!["document:large".into()],
        });
    state
        .network()
        .unwrap()
        .replace_policy(&state, policy)
        .await
        .unwrap();
    call(
        &state,
        &mut ConnectionSession::default(),
        "publish_doc",
        json!({"token":local,"name":"large","body":"\"".repeat(800_000)}),
    )
    .await
    .into_result()
    .unwrap();
    // The native read fits; MCP duplicates JSON into a text block whose escape
    // expansion exceeds the shared response cap.
    cooperators::network::call_remote(&state, &token, "read_doc", json!({"name":"large"}))
        .await
        .unwrap();
    let handler = mcp::CoopMcpHandler::stdio(mcp::Backend::embedded(state.clone()), Some(token));
    let result = handler
        .dispatch_tool(
            "coop_read_doc",
            Some(json!({"name":"large"}).as_object().unwrap().clone()),
        )
        .await;
    let rmcp::model::CallToolResponse::Complete(result) = result else {
        panic!("completed tool result required")
    };
    let encoded = serde_json::to_value(result).unwrap();
    assert_eq!(encoded["isError"], true);
    assert_eq!(
        encoded["structuredContent"]["data"]["code"],
        "RESOURCE_EXHAUSTED"
    );
}
