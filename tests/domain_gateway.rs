mod common;

use std::sync::Arc;

use cooperators::error::ErrorCode;
use cooperators::network::{call_remote, NetworkPolicy};
use cooperators::registry::{new_token, token_hash};
use cooperators::server::{DaemonConfig, DaemonState};
use serde_json::{json, Value};

async fn setup(root: &std::path::Path) -> (Arc<DaemonState>, String, NetworkPolicy) {
    let device = new_token().unwrap();
    let policy: NetworkPolicy = serde_json::from_value(json!({
        "v":1,"enable_operations":true,"enable_resource_operations":true,
        "allowed_hosts":["localhost"],"devices":[{
            "principal_id":"gateway-test","token_hash":token_hash(&device),
            "expires_at_ms":cooperators::id::unix_millis()+600_000,
            "allow_enroll":true,"max_instances":2,"instance_ttl_s":600,
            "permissions":([
                "create_queue","enqueue","claim_task","publish","publish_doc","update_doc",
                "operation_status","send_message"
            ].into_iter().map(|method|json!({"method":method,"resources":["*"]})).collect::<Vec<_>>())
        }]
    })).unwrap();
    let mut config = DaemonConfig::new(root);
    config.network_policy = Some(policy.clone());
    let state = Arc::new(DaemonState::with_config(config).unwrap());
    let token = new_token().unwrap();
    state.network().unwrap().enroll(&state,&device,json!({"launch_key":token,"kind":"test","display_name":"gateway"})).await.unwrap();
    (state, token, policy)
}

fn operation(state: &DaemonState) -> Value {
    json!({"v":1,"id":cooperators::id::new_ulid().unwrap(),"epoch":state.network().unwrap().meta.operation_epoch,"not_after_ms":cooperators::id::unix_millis()+60_000})
}

#[tokio::test]
async fn historical_domain_receipt_and_status_require_original_resource_permission() {
    let root = common::TempDir::new("domain-authority");
    let (state,token,mut policy) = setup(root.path()).await;
    state.create_queue("allowed").await.unwrap();
    let reference = operation(&state);
    let request = json!({"queue":"allowed","payload":{"value":1},"operation":reference});
    let receipt = call_remote(&state,&token,"enqueue",request.clone()).await.unwrap();
    let status = call_remote(&state,&token,"operation_status",json!({"operation":reference})).await.unwrap();
    assert_eq!(status["state"],"committed");
    policy.devices[0].permissions.iter_mut().find(|p|p.method=="enqueue").unwrap().resources = vec!["queue:different".into()];
    state.network().unwrap().replace_policy(&state,policy).await.unwrap();
    for (method,params) in [("enqueue",request),("operation_status",json!({"operation":reference}))] {
        assert_eq!(call_remote(&state,&token,method,params).await.unwrap_err().code,ErrorCode::Forbidden.rpc_code());
    }
    let rows = cooperators::storage::LogFile::<cooperators::queues::QueueEvent>::read_segments(root.path().join("queues/allowed.jsonl")).unwrap().records;
    assert_eq!(rows.iter().filter(|row|row.kind=="task_enqueued").count(),1);
    assert_eq!(receipt["task_id"],rows.iter().find(|row|row.kind=="task_enqueued").unwrap().task_id.as_ref().unwrap().as_str());
}

#[tokio::test]
async fn normalized_defaults_replay_and_malformed_inputs_never_reserve_identity() {
    let root = common::TempDir::new("domain-normalization");
    let (state,token,_) = setup(root.path()).await;
    let reference = operation(&state);
    let invalid = json!({"channel":"#test","text":"x","idempotency_key":"legacy","operation":reference});
    assert_eq!(call_remote(&state,&token,"publish",invalid).await.unwrap_err().code,-32602);
    let status = call_remote(&state,&token,"operation_status",json!({"operation":reference})).await.unwrap();
    assert_eq!(status["state"],"unseen");
    let first = call_remote(&state,&token,"publish",json!({"channel":"#test","text":"x","operation":reference})).await.unwrap();
    let second = call_remote(&state,&token,"publish",json!({"channel":"#test","body":{"mime":"text/plain","text":"x"},"operation":reference})).await.unwrap();
    assert_eq!(first,second);
    assert_eq!(call_remote(&state,&token,"create_queue",json!({"queue":"new","operation":reference})).await.unwrap_err().code,ErrorCode::IdempotencyConflict.rpc_code());
    assert!(!root.path().join("queues/new.jsonl").exists());
}

#[tokio::test]
async fn oversized_local_task_cannot_create_an_unreadable_remote_claim() {
    let root = common::TempDir::new("domain-claim-bound");
    let (state,token,_) = setup(root.path()).await;
    state.create_queue("large").await.unwrap();
    state.enqueue("large",json!({"body":"x".repeat(cooperators::transport::MAX_RPC_BYTES)}),None).await.unwrap();
    let path = root.path().join("queues/large.jsonl");
    let before = std::fs::read(&path).unwrap();
    let reference = operation(&state);
    let request = json!({"queue":"large","operation":reference});
    let first = call_remote(&state,&token,"claim_task",request.clone()).await.unwrap_err();
    assert_eq!(first.code,ErrorCode::ResourceExhausted.rpc_code());
    assert_eq!(first.data.as_ref().unwrap()["durable_operation"]["state"],"terminal_no_effect");
    let second = call_remote(&state,&token,"claim_task",request).await.unwrap_err();
    assert_eq!(first,second);
    assert_eq!(std::fs::read(path).unwrap(),before);
}
