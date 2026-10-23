mod common;
#[path = "common/network.rs"]
mod net;
use cooperators::error::ErrorCode;
use cooperators::network::call_remote;
use cooperators::registry::{new_token, token_hash};
use http::StatusCode;
use net::*;
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn native_ingress_requires_approved_scoped_credentials_and_exact_authority() {
    let f = Fixture::new().await;
    for token in [
        None,
        Some("invalid"),
        Some(f.operator.as_str()),
        Some(f.device.as_str()),
    ] {
        let (status, _) = http(
            &f.state,
            "/v1/rpc",
            token,
            "localhost",
            rpc("ping", json!({})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    for host in [
        "evil.example",
        "localhost:443",
        "localhost.evil",
        "user@localhost",
        "localhost:99999",
        "localhost:abc",
    ] {
        let (status, _) = http(
            &f.state,
            "/v1/rpc",
            Some(&f.instance),
            host,
            rpc("ping", json!({})),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    let (status, reply) = http(
        &f.state,
        "/v1/rpc",
        Some(&f.instance),
        "localhost",
        rpc("ping", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reply["result"]["pong"], true);
    for method in [
        "register_agent",
        "enroll_agent",
        "daemon.status",
        "echo",
        "unlisted",
    ] {
        let (_, reply) = http(
            &f.state,
            "/v1/rpc",
            Some(&f.instance),
            "localhost",
            rpc(method, json!({})),
        )
        .await;
        assert_eq!(code(&reply), Some("FORBIDDEN"));
    }
}

#[tokio::test]
async fn body_credentials_and_resource_denials_apply_through_native_uds_and_mcp() {
    let f = Fixture::new().await;
    for name in ["public", "private"] {
        f.local("publish_doc", json!({"name":name,"text":"contents"}))
            .await;
    }
    f.restrict("read_doc", &["document:public"]).await;
    let (status, reply) = http(
        &f.state,
        "/v1/rpc",
        Some(&f.instance),
        "localhost",
        rpc("read_doc", json!({"name":"private","token":f.operator})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(reply.get("error").is_some());
    let denied = call(
        &f.state,
        "read_doc",
        json!({"token":f.instance,"name":"private"}),
    )
    .await
    .unwrap_err();
    assert_eq!(denied.code, ErrorCode::Forbidden.rpc_code());
    assert!(call(
        &f.state,
        "read_doc",
        json!({"token":f.instance,"name":"public"})
    )
    .await
    .is_ok());
    let handler = cooperators::mcp::CoopMcpHandler::stdio(
        cooperators::mcp::Backend::embedded(f.state.clone()),
        Some(f.instance.clone()),
    );
    let result = handler
        .dispatch_tool(
            "coop_read_doc",
            Some(serde_json::from_value(json!({"name":"private"})).unwrap()),
        )
        .await;
    let rmcp::model::CallToolResponse::Complete(result) = result else {
        panic!("unexpected task response")
    };
    let encoded = serde_json::to_value(result).unwrap();
    assert!(encoded.to_string().contains("FORBIDDEN"));
    for body_token in [None, Some(f.operator.as_str())] {
        let mut arguments = json!({"name":"private"});
        if let Some(token) = body_token {
            arguments["token"] = json!(token);
        }
        let (status, reply) = http(
            &f.state,
            "/mcp",
            Some(&f.instance),
            "localhost",
            rpc(
                "tools/call",
                json!({"name":"coop_read_doc","arguments":arguments}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(reply.to_string().contains("FORBIDDEN"));
        assert!(!reply.to_string().contains("contents"));
    }
}

#[tokio::test]
async fn enrollment_retries_keep_identity_expiry_and_persistent_budget() {
    let f = Fixture::new().await;
    assert!(f
        .state
        .network()
        .unwrap()
        .enroll(
            &f.state,
            &f.device,
            json!({"launch_key":f.device,"kind":"agent","display_name":"device-as-instance"})
        )
        .await
        .is_err());
    let key = new_token().unwrap();
    let params = json!({"launch_key":key,"kind":"agent","display_name":"second"});
    let (_, first) = http(
        &f.state,
        "/v1/enroll",
        Some(&f.device),
        "localhost",
        rpc("enroll_agent", params.clone()),
    )
    .await;
    assert!(first.get("result").is_some());
    let (_, again) = http(
        &f.state,
        "/v1/enroll",
        Some(&f.device),
        "localhost",
        rpc("enroll_agent", params.clone()),
    )
    .await;
    assert_eq!(first["result"]["agent_id"], again["result"]["agent_id"]);
    assert_eq!(
        first["result"]["credential_expires_at_ms"],
        again["result"]["credential_expires_at_ms"]
    );
    assert_eq!(again["result"]["resumed"], true);
    let mut policy = f.state.network().unwrap().policy();
    policy.devices[0].max_instances = 2;
    net::replace_policy(&f.state, policy.clone()).await.unwrap();
    let (status, _) = http(
        &f.state,
        "/v1/enroll",
        Some(&f.device),
        "localhost",
        rpc(
            "enroll_agent",
            json!({"launch_key":new_token().unwrap(),"kind":"agent","display_name":"excess"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let mut other = policy.devices[0].clone();
    let other_token = new_token().unwrap();
    other.principal_id = "device-b".into();
    other.token_hash = token_hash(&other_token);
    policy.devices.push(other);
    net::replace_policy(&f.state, policy).await.unwrap();
    let (_, reply) = http(
        &f.state,
        "/v1/enroll",
        Some(&other_token),
        "localhost",
        rpc("enroll_agent", params),
    )
    .await;
    assert_eq!(code(&reply), Some("UNAUTHENTICATED"));
    let registry_bytes = std::fs::read_to_string(f.temp.path().join("root/registry.json")).unwrap();
    for secret in [&key, &f.instance, &f.device] {
        assert!(!registry_bytes.contains(secret));
    }
}

#[tokio::test]
async fn generation_expiry_retirement_and_policy_replacement_fence_instances() {
    let f = Fixture::new().await;
    let runtime = f.state.network().unwrap();
    let mut policy = runtime.policy();
    policy.devices[0].generation += 1;
    net::replace_policy(&f.state, policy.clone()).await.unwrap();
    assert!(call_remote(&f.state, &f.instance, "ping", json!({}))
        .await
        .is_err());
    policy.devices[0].generation -= 1;
    assert!(net::replace_policy(&f.state, policy.clone()).await.is_err());
    policy.devices[0].generation += 1;
    policy.devices[0].expires_at_ms = 0;
    net::replace_policy(&f.state, policy.clone()).await.unwrap();
    policy.devices[0].expires_at_ms = cooperators::id::unix_millis() + 3600000;
    assert!(net::replace_policy(&f.state, policy).await.is_err());
    assert!(runtime
        .enroll(
            &f.state,
            &f.device,
            json!({"launch_key":new_token().unwrap(),"kind":"agent","display_name":"expired"})
        )
        .await
        .is_err());
    let f = Fixture::new().await;
    f.state.retire_agent(&f.instance).await.unwrap();
    assert!(call_remote(&f.state, &f.instance, "ping", json!({}))
        .await
        .is_err());
    assert!(f
        .state
        .network()
        .unwrap()
        .enroll(
            &f.state,
            &f.device,
            json!({"launch_key":f.instance,"kind":"agent","display_name":"reader"})
        )
        .await
        .is_err());
}

#[tokio::test]
async fn policy_removal_does_not_reset_capacity_or_restore_old_generations() {
    let f = Fixture::new().await;
    let runtime = f.state.network().unwrap();
    let permit = runtime.request_permit("device-a", true).unwrap();
    let original = runtime.policy();
    let mut changed = original.clone();
    changed.devices.clear();
    net::replace_policy(&f.state, changed).await.unwrap();
    assert!(net::replace_policy(&f.state, original.clone())
        .await
        .is_err());
    let mut restored = original.clone();
    restored.devices[0].generation += 1;
    net::replace_policy(&f.state, restored.clone())
        .await
        .unwrap();
    assert!(runtime.request_permit("device-a", true).is_err());
    drop(permit);
    assert!(runtime.request_permit("device-a", true).is_ok());
    restored.requests_per_principal += 1;
    assert!(net::replace_policy(&f.state, restored).await.is_err());
}

#[tokio::test]
async fn revocation_during_a_wait_suppresses_the_result_and_releases_capacity() {
    let f = Fixture::new().await;
    let state = f.state.clone();
    let token = f.instance.clone();
    let task = tokio::spawn(async move {
        call_remote(&state, &token, "wait_message", json!({"timeout_s":2})).await
    });
    let runtime = f.state.network().unwrap();
    let mut started = false;
    for _ in 0..100 {
        if runtime.request_permit("device-a", true).is_err() {
            started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(started);
    // Poll saturation reserves capacity for a short request.
    assert!(call_remote(&f.state, &f.instance, "ping", json!({}))
        .await
        .is_ok());
    let mut policy = runtime.policy();
    policy.devices[0].generation += 1;
    net::replace_policy(&f.state, policy).await.unwrap();
    f.local(
        "send_message",
        json!({"to":f.agent_id,"kind":"text","text":"must not escape"}),
    )
    .await;
    let error = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Unauthenticated.rpc_code());
    assert!(runtime.request_permit("device-a", true).is_ok());
}

#[tokio::test]
async fn pagination_is_authorized_query_scoped_and_rejects_bad_types() {
    let f = Fixture::new().await;
    for name in [
        "secret-a",
        "visible-a",
        "visible-b",
        "visible-c",
        "secret-z",
    ] {
        f.local("publish_doc", json!({"name":name,"text":"value"}))
            .await;
    }
    f.restrict("list_docs", &["document:visible-*"]).await;
    let mut cursor = Value::Null;
    let mut names = Vec::new();
    for _ in 0..4 {
        let mut params = json!({"limit":2});
        if !cursor.is_null() {
            params["cursor"] = cursor.clone();
        }
        let page = call_remote(&f.state, &f.instance, "list_docs", params)
            .await
            .unwrap();
        assert!(!page.to_string().contains("secret"));
        names.extend(
            page["docs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|d| d["name"].as_str().unwrap().to_string()),
        );
        cursor = page["next_cursor"].clone();
        if cursor.is_null() {
            break;
        }
        assert!(call_remote(
            &f.state,
            &f.instance,
            "list_docs",
            json!({"prefix":"visible","cursor":cursor})
        )
        .await
        .is_err());
        assert!(call_remote(
            &f.state,
            &f.instance,
            "list_agents",
            json!({"cursor":cursor})
        )
        .await
        .is_err());
    }
    assert_eq!(names, vec!["visible-a", "visible-b", "visible-c"]);
    for params in [
        json!({"limit":0}),
        json!({"limit":65}),
        json!({"limit":"2"}),
        json!({"include_stale":"yes"}),
        json!({"prefix":5}),
    ] {
        assert!(call_remote(&f.state, &f.instance, "list_docs", params)
            .await
            .is_err());
    }
    for params in [
        json!({"timeout_s":-1}),
        json!({"timeout_s":"2"}),
        json!({"timeout_s":26}),
        json!({"cursor":1}),
    ] {
        assert!(call_remote(&f.state, &f.instance, "wait_message", params)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn unready_mutations_cannot_bypass_policy_through_any_dispatch() {
    let f = Fixture::new().await;
    for (method, params) in [
        ("send_message", json!({"to":"operator","text":"forbidden"})),
        ("publish", json!({"channel":"private","text":"forbidden"})),
        ("create_queue", json!({"queue":"private"})),
        ("ack", json!({"cursor":"999"})),
        ("ack_arrivals", json!({"cursor":"999"})),
        ("publish_doc", json!({"name":"private","text":"forbidden"})),
    ] {
        assert_eq!(
            call_remote(&f.state, &f.instance, method, params.clone())
                .await
                .unwrap_err()
                .code,
            ErrorCode::Forbidden.rpc_code()
        );
        let mut params = params;
        params["token"] = json!(f.instance);
        assert_eq!(
            call(&f.state, method, params).await.unwrap_err().code,
            ErrorCode::Forbidden.rpc_code()
        );
    }
    assert!(!f.temp.path().join("root/docs/private.jsonl").exists());
    assert!(!f.temp.path().join("root/queues/private.jsonl").exists());
    assert!(!f.temp.path().join("root/channels/private.jsonl").exists());
}

#[tokio::test]
async fn bounded_channel_pages_preserve_cursors_and_refuse_a_single_oversized_record() {
    let f = Fixture::new().await;
    for _ in 0..3 {
        f.local(
            "publish",
            json!({"channel":"large","text":"x".repeat(1_500_000)}),
        )
        .await;
    }
    let page = call_remote(
        &f.state,
        &f.instance,
        "read_channel",
        json!({"channel":"large","limit":64}),
    )
    .await
    .unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 2);
    let next = call_remote(
        &f.state,
        &f.instance,
        "read_channel",
        json!({"channel":"large","cursor":page["next_cursor"]}),
    )
    .await
    .unwrap();
    assert_eq!(next["messages"].as_array().unwrap().len(), 1);
    assert_eq!(next["messages"][0]["seq"], 3);
    assert!(call_remote(
        &f.state,
        &f.instance,
        "read_channel",
        json!({"channel":"large","cursor":{"file":"channels/other.jsonl","seq":0}})
    )
    .await
    .is_err());
    f.local(
        "publish",
        json!({"channel":"oversized","text":"x".repeat(4_200_000)}),
    )
    .await;
    let error = call_remote(
        &f.state,
        &f.instance,
        "read_channel",
        json!({"channel":"oversized"}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::ResourceExhausted.rpc_code());
}

#[tokio::test]
async fn bounded_inbox_and_document_reads_do_not_skip_oversized_contents() {
    let f = Fixture::new().await;
    for _ in 0..3 {
        f.local(
            "send_message",
            json!({"to":f.agent_id,"kind":"text","text":"x".repeat(1_500_000)}),
        )
        .await;
    }
    let page = call_remote(
        &f.state,
        &f.instance,
        "wait_message",
        json!({"timeout_s":0}),
    )
    .await
    .unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 2);
    let next = call_remote(
        &f.state,
        &f.instance,
        "wait_message",
        json!({"timeout_s":0,"cursor":page["next_cursor"]}),
    )
    .await
    .unwrap();
    assert_eq!(next["messages"].as_array().unwrap().len(), 1);
    assert_eq!(next["messages"][0]["seq"], 3);
    f.local(
        "publish_doc",
        json!({"name":"oversized","text":"x".repeat(4_200_000)}),
    )
    .await;
    assert_eq!(
        call_remote(
            &f.state,
            &f.instance,
            "read_doc",
            json!({"name":"oversized"})
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::ResourceExhausted.rpc_code()
    );
}

#[tokio::test]
async fn arrival_pages_and_subscription_lists_check_current_resource_policy() {
    use cooperators::arrivals::{ArrivalKind, ArrivalResource, SubscribeArrivalsRequest};
    let f = Fixture::new().await;
    let identity = f.state.authenticate(&f.instance).await.unwrap();
    for name in ["allowed", "secret"] {
        f.local("publish_doc", json!({"name":name,"text":"seed"}))
            .await;
        f.state
            .subscribe_arrivals(
                &identity,
                SubscribeArrivalsRequest {
                    client_key: name.into(),
                    resource: ArrivalResource::Document { name: name.into() },
                    event_kinds: vec![ArrivalKind::DocUpdated],
                    catch_up: false,
                },
            )
            .await
            .unwrap();
    }
    f.restrict("subscribe_arrivals", &["document:allowed"])
        .await;
    let page = call_remote(
        &f.state,
        &f.instance,
        "list_arrival_subscriptions",
        json!({"limit":1}),
    )
    .await
    .unwrap();
    assert_eq!(page["subscriptions"].as_array().unwrap().len(), 1);
    assert_eq!(page["subscriptions"][0]["resource"]["name"], "allowed");
    let end = call_remote(
        &f.state,
        &f.instance,
        "list_arrival_subscriptions",
        json!({"cursor":page["next_cursor"]}),
    )
    .await
    .unwrap();
    assert_eq!(end["subscriptions"], json!([]));
    f.local(
        "update_doc",
        json!({"name":"secret","base_version":1,"text":"restricted update"}),
    )
    .await;
    // Wait for projection using the trusted domain API, then verify ingress suppresses it.
    let pending = f
        .state
        .wait_arrivals(&identity, None, Duration::from_secs(2), 64)
        .await
        .unwrap();
    assert!(!pending.notices.is_empty());
    assert_eq!(
        call_remote(
            &f.state,
            &f.instance,
            "wait_arrivals",
            json!({"timeout_s":0})
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::Forbidden.rpc_code()
    );
    let again = f
        .state
        .wait_arrivals(&identity, None, Duration::ZERO, 64)
        .await
        .unwrap();
    assert_eq!(again.next_cursor, pending.next_cursor);
    f.state.shutdown_arrivals().await.unwrap();
}

#[test]
fn policy_validation_and_header_controls_reject_ambiguous_authority() {
    let mut policy = net::policy(&new_token().unwrap());
    for resource in ["document:**", "doc*ument:x"] {
        policy.devices[0].permissions[0].resources = vec![resource.into()];
        assert!(policy.validate().is_err());
    }
    let policy = net::policy(&new_token().unwrap());
    let mut headers = http::HeaderMap::new();
    headers.insert("host", "localhost".parse().unwrap());
    assert!(policy.headers_allowed(&headers));
    headers.insert("origin", "https://evil.example".parse().unwrap());
    assert!(!policy.headers_allowed(&headers));
    headers.remove("origin");
    headers.append("host", "evil.example".parse().unwrap());
    assert!(!policy.headers_allowed(&headers));
}

#[tokio::test]
async fn retired_or_expired_scope_never_falls_back_to_local_provisioning() {
    let f = Fixture::new().await;
    f.state.retire_agent(&f.instance).await.unwrap();
    for token in [&f.instance, &f.device] {
        for (method, params) in [
            ("register_agent", json!({"token":token,"name":"bypass"})),
            (
                "enroll_agent",
                json!({"token":token,"launch_key":new_token().unwrap(),"kind":"agent","display_name":"bypass"}),
            ),
        ] {
            assert_eq!(
                call(&f.state, method, params).await.unwrap_err().code,
                ErrorCode::Unauthenticated.rpc_code()
            );
        }
    }
    assert!(
        !cooperators::registry::Registry::load(&f.temp.path().join("root"))
            .unwrap()
            .records_with_names()
            .any(|(name, _)| name == "bypass")
    );
}

#[tokio::test]
async fn http_request_budgets_bound_native_and_mcp_admission_before_dispatch() {
    let f = Fixture::new().await;
    let runtime = f.state.network().unwrap();
    let permits: Vec<_> = (0..3)
        .map(|_| runtime.request_permit("device-a", false).unwrap())
        .collect();
    for path in ["/v1/rpc", "/v1/enroll", "/mcp"] {
        let token = if path == "/v1/enroll" {
            &f.device
        } else {
            &f.instance
        };
        let (status, _) = http(
            &f.state,
            path,
            Some(token),
            "localhost",
            rpc("ping", json!({})),
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    }
    drop(permits);
    for path in ["/v1/rpc", "/mcp"] {
        let (status, _) = http(
            &f.state,
            path,
            Some(&f.instance),
            "localhost",
            rpc("ping", json!({"padding":"x".repeat(4_200_000)})),
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        let mut request = rpc("ping", json!({}));
        request["id"] = json!("x".repeat(1024));
        let (status, _) = http(&f.state, path, Some(&f.instance), "localhost", request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    assert!(call_remote(&f.state, &f.instance, "ping", json!({}))
        .await
        .is_ok());
}

#[tokio::test]
async fn agent_pages_filter_before_construction_and_cursors_cannot_cross_instances() {
    let f = Fixture::new().await;
    f.restrict("list_agents", &[&format!("agent:{}", f.agent_id)])
        .await;
    let page = call_remote(
        &f.state,
        &f.instance,
        "list_agents",
        json!({"limit":1,"include_stale":true}),
    )
    .await
    .unwrap();
    assert_eq!(page["agents"].as_array().unwrap().len(), 1);
    assert_eq!(page["agents"][0]["agent_id"], f.agent_id);
    assert!(!page.to_string().contains("operator"));
    let cursor = page["next_cursor"].clone();
    let end = call_remote(
        &f.state,
        &f.instance,
        "list_agents",
        json!({"cursor":cursor,"include_stale":true}),
    )
    .await
    .unwrap();
    assert!(end["agents"].as_array().unwrap().is_empty());
    let other = new_token().unwrap();
    f.state
        .network()
        .unwrap()
        .enroll(
            &f.state,
            &f.device,
            json!({"launch_key":other,"kind":"agent","display_name":"other"}),
        )
        .await
        .unwrap();
    assert!(call_remote(
        &f.state,
        &other,
        "list_agents",
        json!({"cursor":cursor,"include_stale":true})
    )
    .await
    .is_err());
}
