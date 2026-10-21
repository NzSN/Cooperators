#![cfg(feature = "mbt-injection")]

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cooperators::arrivals::{
    ArrivalKind, ArrivalLimits, ArrivalMeta, ArrivalResource, ArrivalService, NotificationIntent,
    SourceCoordinate, SubscribeArrivalsRequest,
};
use cooperators::faults::{arm_append, arm_point, AppendFault, FaultPoint, PointFault};
use cooperators::storage::{ChainRecord, LogFile, GENESIS_HASH};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct SourceEvent {
    seq: u64,
    prev_hash: String,
    kind: String,
    doc: String,
    version: u64,
    notification_intent: Option<NotificationIntent>,
}
impl ChainRecord for SourceEvent {
    fn seq(&self) -> u64 {
        self.seq
    }
    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }
    fn prev_hash(&self) -> &str {
        &self.prev_hash
    }
    fn set_prev_hash(&mut self, hash: String) {
        self.prev_hash = hash;
    }
}
fn meta() -> ArrivalMeta {
    ArrivalMeta {
        v: 1,
        bus_id: cooperators::id::new_ulid().unwrap(),
    }
}
fn agent() -> String {
    cooperators::id::new_ulid().unwrap()
}
fn resource() -> ArrivalResource {
    ArrivalResource::Document {
        name: "plan".into(),
    }
}
fn routes(arrivals: &ArrivalService, kind: ArrivalKind) -> Option<NotificationIntent> {
    arrivals.capture_routes(&resource(), kind).unwrap()
}
fn request(key: &str) -> SubscribeArrivalsRequest {
    SubscribeArrivalsRequest {
        client_key: key.into(),
        resource: resource(),
        event_kinds: vec![ArrivalKind::DocPublished],
        catch_up: false,
    }
}
fn service(root: &Path, meta: ArrivalMeta) -> Arc<ArrivalService> {
    ArrivalService::open(root, meta, ArrivalLimits::default(), BTreeSet::new()).unwrap()
}
fn source(root: &Path, arrivals: &ArrivalService) -> SourceCoordinate {
    let (mut log, _) = LogFile::<SourceEvent>::open(root.join("docs/plan.jsonl")).unwrap();
    let mut event = SourceEvent {
        seq: 0,
        prev_hash: GENESIS_HASH.into(),
        kind: "doc_published".into(),
        doc: "plan".into(),
        version: 1,
        notification_intent: routes(&arrivals, ArrivalKind::DocPublished),
    };
    let receipt = log.append(&mut event).unwrap();
    SourceCoordinate {
        stream: "document".into(),
        name: "plan".into(),
        seq: receipt.seq,
        hash: receipt.hash,
    }
}

#[tokio::test]
async fn uncertain_feed_sync_replays_complete_notice_before_retry_and_isolates_recipient() {
    for fault in [
        AppendFault::FailSync,
        AppendFault::PartialWrite { bytes: 40 },
    ] {
        let root = common::TempDir::new("arrival-feed-io");
        let meta = meta();
        let bad = agent();
        let healthy = agent();
        let arrivals = service(root.path(), meta.clone());
        arrivals.subscribe(&bad, request("bad"), None).unwrap();
        arrivals
            .subscribe(&healthy, request("healthy"), None)
            .unwrap();
        let head = source(root.path(), &arrivals);
        let path = root.path().join("agents").join(&bad).join("arrivals.jsonl");
        let injection = arm_append(&path, fault).unwrap();
        arrivals.register_source_head(head.clone());
        arrivals.start_projection();
        let seen = arrivals
            .wait(&healthy, None, Duration::from_secs(3), 64)
            .await
            .unwrap();
        assert_eq!(seen.notices.len(), 1);
        injection.wait_reached(Duration::from_secs(3)).unwrap();
        arrivals.shutdown();
        assert!(arrivals.status().feed_errors.contains_key(&bad));
        drop(arrivals);
        drop(injection);
        let arrivals = service(root.path(), meta);
        arrivals.register_source_head(head);
        arrivals.start_projection();
        let recovered = arrivals
            .wait(&bad, None, Duration::from_secs(3), 64)
            .await
            .unwrap();
        assert_eq!(recovered.notices.len(), 1);
        arrivals.shutdown();
        let report = LogFile::<cooperators::arrivals::ArrivalNotice>::read_segments(&path).unwrap();
        assert_eq!(
            report.records.len(),
            1,
            "one source obligation has one durable feed record"
        );
    }
}

#[tokio::test]
async fn uncertain_ack_is_quarantined_and_recovered_before_success() {
    for fault in [
        AppendFault::FailSync,
        AppendFault::PartialWrite { bytes: 25 },
    ] {
        let root = common::TempDir::new("arrival-ack-io");
        let meta = meta();
        let agent = agent();
        let arrivals = service(root.path(), meta.clone());
        arrivals.subscribe(&agent, request("live"), None).unwrap();
        let head = source(root.path(), &arrivals);
        arrivals.register_source_head(head.clone());
        arrivals.start_projection();
        let page = arrivals
            .wait(&agent, None, Duration::from_secs(3), 64)
            .await
            .unwrap();
        let path = root
            .path()
            .join("agents")
            .join(&agent)
            .join("arrivals-acks.jsonl");
        let injection = arm_append(path, fault).unwrap();
        assert!(arrivals.ack(&agent, &page.next_cursor).is_err());
        assert!(arrivals.ack(&agent, &page.acked_cursor).is_err());
        arrivals.shutdown();
        drop(arrivals);
        drop(injection);
        let arrivals = service(root.path(), meta);
        arrivals.register_source_head(head);
        arrivals.start_projection();
        let recovered = arrivals
            .wait(&agent, None, Duration::from_millis(100), 64)
            .await
            .unwrap();
        if fault == AppendFault::FailSync {
            assert!(recovered.notices.is_empty());
        } else {
            assert_eq!(recovered.notices.len(), 1);
        }
        arrivals.ack(&agent, &page.next_cursor).unwrap();
        arrivals.shutdown();
    }
}

#[test]
fn subscription_sync_failure_fences_mutation_until_reopen_and_recovers_client_key() {
    let root = common::TempDir::new("arrival-subscription-sync");
    let meta = meta();
    let agent = agent();
    let arrivals = service(root.path(), meta.clone());
    let injection = arm_append(
        root.path().join("notifications/subscriptions.jsonl"),
        AppendFault::FailSync,
    )
    .unwrap();
    assert!(arrivals
        .subscribe(&agent, request("same-key"), None)
        .is_err());
    assert!(arrivals
        .capture_routes(&resource(), ArrivalKind::DocPublished)
        .is_err());
    assert!(arrivals
        .subscribe(&agent, request("another-key"), None)
        .is_err());
    drop(arrivals);
    drop(injection);
    let arrivals = service(root.path(), meta);
    let receipt = arrivals
        .subscribe(&agent, request("same-key"), None)
        .unwrap();
    assert_eq!(arrivals.list(&agent).unwrap().subscriptions, vec![receipt]);
    assert!(arrivals
        .capture_routes(&resource(), ArrivalKind::DocPublished)
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn recovery_sync_failure_never_publishes_uncertain_feed_indexes() {
    let root = common::TempDir::new("arrival-recovery-sync");
    let meta = meta();
    let agent = agent();
    let arrivals = service(root.path(), meta.clone());
    arrivals.subscribe(&agent, request("live"), None).unwrap();
    let head = source(root.path(), &arrivals);
    let path = root
        .path()
        .join("agents")
        .join(&agent)
        .join("arrivals.jsonl");
    let injection = arm_append(&path, AppendFault::FailSync).unwrap();
    arrivals.register_source_head(head.clone());
    arrivals.start_projection();
    injection.wait_reached(Duration::from_secs(3)).unwrap();
    arrivals.shutdown();
    drop(arrivals);
    drop(injection);
    let arrivals = service(root.path(), meta.clone());
    let recovery = arm_point(&path, FaultPoint::BeforeRecoverySync, PointFault::Fail).unwrap();
    assert!(arrivals
        .wait(&agent, None, Duration::ZERO, 64)
        .await
        .is_err());
    drop(arrivals);
    drop(recovery);
    let arrivals = service(root.path(), meta);
    arrivals.register_source_head(head);
    arrivals.start_projection();
    let page = arrivals
        .wait(&agent, None, Duration::from_secs(3), 64)
        .await
        .unwrap();
    assert_eq!(page.notices.len(), 1);
    arrivals.shutdown();
    assert_eq!(
        LogFile::<cooperators::arrivals::ArrivalNotice>::read_segments(&path)
            .unwrap()
            .records
            .len(),
        1
    );
}
