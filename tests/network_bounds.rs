mod common;

use std::sync::Arc;

use bytes::Bytes;
use cooperators::network::{call_remote, handle_network_http, NetworkPolicy};
use cooperators::rpc::{RpcError, RpcRequest};
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use serde_json::{json, Value};

struct Fixture {
    _temp: common::TempDir,
    state: Arc<DaemonState>,
    token: String,
    local: String,
    device: String,
}

impl Fixture {
    async fn new() -> Self {
        let temp = common::TempDir::new("network-bounds");
        let device = cooperators::registry::new_token().unwrap();
        let policy: NetworkPolicy = serde_json::from_value(json!({
            "v":1,"allowed_hosts":["broker.test"],"requests_per_principal":3,"polls_per_principal":1,
            "devices":[{"principal_id":"bounds","token_hash":cooperators::registry::token_hash(&device),
                "expires_at_ms":cooperators::id::unix_millis()+60_000,"allow_enroll":true,
                "max_instances":8,"instance_ttl_s":60,
                "permissions":[
                    {"method":"ping","resources":["*"]},
                    {"method":"read_channel","resources":["channel:allowed"]},
                    {"method":"read_doc","resources":["document:allowed*"]},
                    {"method":"wait_message","resources":["inbox:*"]},
                    {"method":"wait_arrivals","resources":["arrival:*"]},
                    {"method":"list_docs","resources":["document:allowed*"]},
                    {"method":"list_agents","resources":["agent:*"]},
                    {"method":"list_arrival_subscriptions","resources":["arrival:*"]},
                    {"method":"subscribe_arrivals","resources":["document:allowed*"]},
                    {"method":"publish","resources":["channel:allowed"]}
                ]}]
        })).unwrap();
        let mut config = DaemonConfig::new(temp.path().join("root"));
        config.network_policy = Some(policy);
        config.enable_arrivals = true;
        let state = Arc::new(DaemonState::with_config(config).unwrap());
        let local = local(&state, "register_agent", json!({"name":"local"})).await["token"]
            .as_str()
            .unwrap()
            .to_owned();
        let token = enroll(&state, &device).await;
        Self {
            _temp: temp,
            state,
            token,
            local,
            device,
        }
    }

    async fn remote(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        call_remote(&self.state, &self.token, method, params).await
    }

    async fn doc(&self, name: &str, title: &str) {
        local(
            &self.state,
            "publish_doc",
            json!({"token":self.local,"name":name,"title":title,"text":"body"}),
        )
        .await;
    }

    async fn http(&self, id: Value, method: &str, params: Value) -> (StatusCode, Value) {
        let body = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        let request = Request::post("/v1/rpc")
            .header("host", "broker.test")
            .header("authorization", format!("Bearer {}", self.token))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
            .unwrap();
        let response = handle_network_http(&self.state, request).await;
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert!(bytes.len() <= cooperators::transport::MAX_RPC_BYTES);
        (status, serde_json::from_slice(&bytes).unwrap())
    }
}

async fn enroll(state: &DaemonState, device: &str) -> String {
    state.network().unwrap().enroll(state,device,json!({
        "launch_key":cooperators::registry::new_token().unwrap(),"kind":"agent","display_name":"remote"
    })).await.unwrap()["token"].as_str().unwrap().to_owned()
}

async fn local(state: &Arc<DaemonState>, method: &str, params: Value) -> Value {
    server::dispatch(
        state,
        &mut ConnectionSession::default(),
        RpcRequest {
            id: Some(json!(1)),
            method: method.into(),
            params,
            is_notification: false,
        },
    )
    .await
    .unwrap()
    .into_result()
    .unwrap()
}

#[tokio::test]
async fn documents_are_authorized_before_paging_and_cursors_bind_scope_and_filters() {
    let f = Fixture::new().await;
    // Oversized inaccessible metadata must never enter the response builder.
    f.doc(
        "private",
        &"x".repeat(cooperators::transport::MAX_RPC_BYTES + 1),
    )
    .await;
    for index in 0..5 {
        f.doc(&format!("allowed-{index}"), "small").await;
    }
    let mut cursor = Value::Null;
    let mut names = Vec::new();
    loop {
        let mut params = json!({"limit":2});
        if !cursor.is_null() {
            params["cursor"] = cursor.clone();
        }
        let page = f.remote("list_docs", params).await.unwrap();
        let docs = page["docs"].as_array().unwrap();
        assert!(!docs.is_empty() && docs.len() <= 2);
        names.extend(
            docs.iter()
                .map(|doc| doc["name"].as_str().unwrap().to_owned()),
        );
        if cursor.is_null() {
            let first = page["next_cursor"].clone();
            assert_eq!(
                f.remote("list_agents", json!({"cursor":first}))
                    .await
                    .unwrap_err()
                    .code,
                -32602
            );
            assert_eq!(
                f.remote("list_docs", json!({"cursor":first,"prefix":"allowed"}))
                    .await
                    .unwrap_err()
                    .code,
                -32602
            );
            let other = enroll(&f.state, &f.device).await;
            assert_eq!(
                call_remote(&f.state, &other, "list_docs", json!({"cursor":first}))
                    .await
                    .unwrap_err()
                    .code,
                -32602
            );
        }
        cursor = page["next_cursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    assert_eq!(
        names,
        (0..5).map(|i| format!("allowed-{i}")).collect::<Vec<_>>()
    );
    f.state.shutdown_arrivals().await.unwrap();
}

#[tokio::test]
async fn channel_pages_count_escaped_bytes_and_never_skip_an_unreturned_record() {
    let f = Fixture::new().await;
    for _ in 0..3 {
        local(
            &f.state,
            "publish",
            json!({"token":f.local,"channel":"allowed","text":"\n".repeat(900_000)}),
        )
        .await;
    }
    let page = f
        .remote("read_channel", json!({"channel":"allowed"}))
        .await
        .unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 2);
    assert_eq!(page["next_cursor"]["seq"], 2);
    assert!(serde_json::to_vec(&page).unwrap().len() < cooperators::transport::MAX_RPC_BYTES);
    let next = f
        .remote(
            "read_channel",
            json!({"channel":"allowed","cursor":page["next_cursor"]}),
        )
        .await
        .unwrap();
    assert_eq!(next["messages"].as_array().unwrap().len(), 1);
    assert_eq!(next["messages"][0]["seq"], 3);
    assert_eq!(
        f.remote("read_channel", json!({"channel":"allowed","cursor":4}))
            .await
            .unwrap_err()
            .code,
        -32602
    );
    assert_eq!(
        f.remote("publish", json!({"channel":"allowed","text":"denied"}))
            .await
            .unwrap_err()
            .code,
        -32010
    );
    local(
        &f.state,
        "publish",
        json!({"token":f.local,"channel":"allowed","text":"\n".repeat(2_100_000)}),
    )
    .await;
    assert_eq!(
        f.remote("read_channel", json!({"channel":"allowed","cursor":3}))
            .await
            .unwrap_err()
            .code,
        -32003
    );
    f.state.shutdown_arrivals().await.unwrap();
}

#[tokio::test]
async fn document_metadata_pages_stop_at_bytes_and_resume_at_the_last_returned_name() {
    let f = Fixture::new().await;
    for index in 0..3 {
        f.doc(&format!("allowed-{index}"), &"x".repeat(1_500_000))
            .await;
    }
    let first = f.remote("list_docs", json!({})).await.unwrap();
    assert_eq!(first["docs"].as_array().unwrap().len(), 2);
    assert_eq!(first["next_cursor"]["after"], "allowed-1");
    let second = f
        .remote("list_docs", json!({"cursor":first["next_cursor"]}))
        .await
        .unwrap();
    assert_eq!(second["docs"].as_array().unwrap().len(), 1);
    assert_eq!(second["docs"][0]["name"], "allowed-2");
    assert_eq!(second["next_cursor"], Value::Null);
    f.state.shutdown_arrivals().await.unwrap();
}

#[tokio::test]
async fn agents_and_subscriptions_filter_before_counting_the_page_limit() {
    let f = Fixture::new().await;
    let allowed = local(&f.state, "register_agent", json!({"name":"z-visible"})).await;
    let mut policy = f.state.network().unwrap().policy();
    policy.devices[0]
        .permissions
        .iter_mut()
        .find(|p| p.method == "list_agents")
        .unwrap()
        .resources = vec![format!("agent:{}", allowed["agent_id"].as_str().unwrap())];
    f.state
        .network()
        .unwrap()
        .replace_policy(&f.state, policy)
        .await
        .unwrap();
    let agents = f.remote("list_agents", json!({"limit":1})).await.unwrap();
    assert_eq!(agents["agents"].as_array().unwrap().len(), 1);
    assert_eq!(agents["agents"][0]["name"], "z-visible");
    assert_eq!(agents["next_cursor"], Value::Null);
    let identity = f
        .state
        .network()
        .unwrap()
        .authenticate(&f.state, &f.token)
        .await
        .unwrap()
        .identity;
    for name in ["private", "allowed-a", "allowed-b", "allowed-c"] {
        f.doc(name, "small").await;
        f.state
            .subscribe_arrivals(
                &identity,
                serde_json::from_value(json!({
                    "client_key":name,"resource":{"type":"document","name":name},
                    "event_kinds":["doc.updated"],"catch_up":false
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let mut params = json!({"limit":1});
    let mut names = Vec::new();
    loop {
        let page = f
            .remote("list_arrival_subscriptions", params.clone())
            .await
            .unwrap();
        let rows = page["subscriptions"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        names.push(rows[0]["resource"]["name"].as_str().unwrap().to_owned());
        if page["next_cursor"].is_null() {
            break;
        }
        params["cursor"] = page["next_cursor"].clone();
    }
    names.sort();
    assert_eq!(names, vec!["allowed-a", "allowed-b", "allowed-c"]);
    f.state.shutdown_arrivals().await.unwrap();
}

#[tokio::test]
async fn malformed_optional_values_and_http_statuses_are_explicit() {
    let f = Fixture::new().await;
    for (method, params) in [
        ("wait_message", json!({"cursor":1,"timeout_s":0})),
        ("wait_message", json!({"timeout_s":"0"})),
        ("wait_message", json!({"timeout_s":null})),
        ("read_doc", json!({"name":"allowed","version":"1"})),
        (
            "read_channel",
            json!({"channel":"allowed","cursor":{"seq":0,"file":false}}),
        ),
        ("read_channel", json!({"channel":"allowed","limit":0})),
        ("read_channel", json!({"channel":"allowed","limit":65})),
        ("read_channel", json!({"channel":"allowed","wait_s":26})),
        ("list_agents", json!({"include_stale":"false"})),
        ("list_docs", json!({"prefix":1})),
        ("list_docs", json!({"cursor":{}})),
        ("list_docs", json!({"unknown":true})),
    ] {
        let (status, reply) = f.http(json!(1), method, params).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{method}: {reply}");
        assert_eq!(reply["error"]["code"], -32602);
    }
    let (status, _) = f.http(json!("x".repeat(1025)), "ping", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let permits = (0..3)
        .map(|_| {
            f.state
                .network()
                .unwrap()
                .request_permit("bounds", false)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let (status, reply) = f.http(json!(1), "ping", json!({})).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(reply["error"]["code"], -32003);
    drop(permits);
    let (status, _) = f.http(json!(1), "ping", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    f.state.shutdown_arrivals().await.unwrap();
}

#[tokio::test]
async fn inbox_cursors_and_document_blob_budgets_are_checked_before_returning_data() {
    let f = Fixture::new().await;
    assert_eq!(
        f.remote("wait_message", json!({"cursor":"1","timeout_s":0}))
            .await
            .unwrap_err()
            .code,
        -32602
    );
    let empty = f
        .remote("wait_message", json!({"cursor":"0","timeout_s":0}))
        .await
        .unwrap();
    assert_eq!(empty["next_cursor"], "0");
    for (name, text) in [
        ("allowed-small", "x".repeat(100_000)),
        ("allowed-escaped", "\n".repeat(2_100_000)),
    ] {
        local(
            &f.state,
            "publish_doc",
            json!({"token":f.local,"name":name,"text":text}),
        )
        .await;
    }
    let small = f
        .remote("read_doc", json!({"name":"allowed-small"}))
        .await
        .unwrap();
    assert_eq!(small["storage"], "blob");
    assert_eq!(small["body"]["text"].as_str().unwrap().len(), 100_000);
    assert_eq!(
        f.remote("read_doc", json!({"name":"allowed-escaped"}))
            .await
            .unwrap_err()
            .code,
        -32003
    );
    f.state.shutdown_arrivals().await.unwrap();
}

#[tokio::test]
async fn duplicate_authorization_headers_never_select_one_credential() {
    let f = Fixture::new().await;
    let valid = format!("Bearer {}", f.token);
    for (first, second) in [
        (valid.as_str(), "Bearer invalid"),
        ("Bearer invalid", valid.as_str()),
        (valid.as_str(), valid.as_str()),
    ] {
        let request = Request::post("/v1/rpc")
            .header("host", "broker.test")
            .header("authorization", first)
            .header("authorization", second)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from_static(
                br#"{"jsonrpc":"2.0","id":1,"method":"ping","params":{}}"#,
            )))
            .unwrap();
        let response = handle_network_http(&f.state, request).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply["error"]["code"], -32001);
    }
    f.state.shutdown_arrivals().await.unwrap();
}
