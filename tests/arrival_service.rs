mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cooperators::arrivals::{
    ArrivalKind, ArrivalLimits, ArrivalMeta, ArrivalReference, ArrivalResource, ArrivalService,
    CatchUpSnapshot, NotificationIntent, SourceCoordinate, SubscribeArrivalsRequest,
};
use cooperators::error::{BrokerError, ErrorCode};
use cooperators::storage::{ChainRecord, LogFile, GENESIS_HASH};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceEvent {
    seq: u64,
    prev_hash: String,
    kind: String,
    doc: String,
    version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
fn request(key: &str, catch_up: bool) -> SubscribeArrivalsRequest {
    SubscribeArrivalsRequest {
        client_key: key.into(),
        resource: resource(),
        event_kinds: vec![ArrivalKind::DocPublished, ArrivalKind::DocUpdated],
        catch_up,
    }
}
fn service(root: &Path, meta: ArrivalMeta, limits: ArrivalLimits) -> Arc<ArrivalService> {
    ArrivalService::open(root, meta, limits, BTreeSet::new()).unwrap()
}
fn source(root: &Path) -> LogFile<SourceEvent> {
    LogFile::open_rotating(root.join("docs/plan.jsonl"), 800)
        .unwrap()
        .0
}
fn append(
    log: &mut LogFile<SourceEvent>,
    intent: Option<NotificationIntent>,
    version: u64,
) -> SourceCoordinate {
    let mut event = SourceEvent {
        seq: 0,
        prev_hash: GENESIS_HASH.into(),
        kind: if version == 1 {
            "doc_published"
        } else {
            "doc_updated"
        }
        .into(),
        doc: "plan".into(),
        version,
        notification_intent: intent,
    };
    let receipt = log.append(&mut event).unwrap();
    SourceCoordinate {
        stream: "document".into(),
        name: "plan".into(),
        seq: receipt.seq,
        hash: receipt.hash,
    }
}
async fn page(service: &Arc<ArrivalService>, agent: &str) -> cooperators::arrivals::WaitArrivals {
    service
        .wait(agent, None, Duration::from_secs(3), 64)
        .await
        .unwrap()
}

#[tokio::test]
async fn replay_recovers_source_to_feed_gap_and_dedupes_after_ack() {
    let root = common::TempDir::new("arrival-replay-gap");
    let meta = meta();
    let agent = agent();
    let arrivals = service(root.path(), meta.clone(), ArrivalLimits::default());
    arrivals
        .subscribe(&agent, request("live", false), None)
        .unwrap();
    let mut source = source(root.path());
    let head = append(&mut source, routes(&arrivals, ArrivalKind::DocPublished), 1);
    // Crash equivalent: committed intent exists; no projector ever ran.
    drop(arrivals);
    let arrivals = service(root.path(), meta.clone(), ArrivalLimits::default());
    arrivals.register_source_head(head.clone());
    arrivals.start_projection();
    let first = page(&arrivals, &agent).await;
    assert_eq!(first.notices.len(), 1);
    arrivals.ack(&agent, &first.next_cursor).unwrap();
    arrivals.shutdown();
    drop(arrivals);
    let arrivals = service(root.path(), meta, ArrivalLimits::default());
    arrivals.register_source_head(head);
    arrivals.start_projection();
    let empty = arrivals
        .wait(&agent, None, Duration::from_millis(100), 64)
        .await
        .unwrap();
    assert!(empty.notices.is_empty());
    let historical = arrivals
        .wait(&agent, Some(&first.acked_cursor), Duration::ZERO, 64)
        .await
        .unwrap();
    assert_eq!(historical.notices, first.notices);
    arrivals.shutdown();
}

#[tokio::test]
async fn catchup_watermark_never_suppresses_earlier_live_source_obligations() {
    let root = common::TempDir::new("arrival-catchup-live");
    let arrivals = service(root.path(), meta(), ArrivalLimits::default());
    let agent = agent();
    arrivals
        .subscribe(&agent, request("first", false), None)
        .unwrap();
    let mut log = source(root.path());
    append(&mut log, routes(&arrivals, ArrivalKind::DocPublished), 1);
    let head = append(&mut log, routes(&arrivals, ArrivalKind::DocUpdated), 2);
    let snapshot = CatchUpSnapshot {
        source: head.clone(),
        resource: ArrivalReference::Document {
            name: "plan".into(),
            version: 2,
        },
        kind: ArrivalKind::DocUpdated,
    };
    arrivals
        .subscribe(&agent, request("catchup", true), Some(snapshot))
        .unwrap();
    arrivals.start_projection();
    let caught = page(&arrivals, &agent).await;
    assert_eq!(caught.notices.len(), 1);
    assert_eq!(caught.notices[0].obligation.journal_type, "subscription");
    arrivals.ack(&agent, &caught.next_cursor).unwrap();
    // Register only now: the catch-up's source boundary must not advance the
    // document watermark and erase these two captured live routes.
    arrivals.source_committed(head);
    let mut notices = Vec::new();
    let mut cursor = caught.next_cursor;
    while notices.len() < 2 {
        let next = arrivals
            .wait(&agent, Some(&cursor), Duration::from_secs(3), 64)
            .await
            .unwrap();
        assert!(!next.timed_out);
        cursor = next.next_cursor;
        notices.extend(next.notices);
    }
    assert_eq!(
        notices
            .iter()
            .map(|notice| notice.obligation.seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    arrivals.shutdown();
}

#[test]
fn client_keys_are_identity_scoped_durable_and_never_recreate_catchup() {
    let root = common::TempDir::new("arrival-client-key");
    let meta = meta();
    let agent_a = agent();
    let agent_b = agent();
    let arrivals = service(root.path(), meta.clone(), ArrivalLimits::default());
    let first = arrivals
        .subscribe(&agent_a, request("shared", false), None)
        .unwrap();
    assert_eq!(
        arrivals
            .subscribe(&agent_a, request("shared", false), None)
            .unwrap(),
        first
    );
    let mut changed = request("shared", false);
    changed.catch_up = true;
    assert!(matches!(
        arrivals.subscribe(&agent_a, changed, None),
        Err(BrokerError::Application(ErrorCode::Conflict, _))
    ));
    let other = arrivals
        .subscribe(&agent_b, request("shared", false), None)
        .unwrap();
    assert_ne!(other.subscription_id, first.subscription_id);
    arrivals
        .unsubscribe(&agent_a, &first.subscription_id)
        .unwrap();
    arrivals
        .unsubscribe(&agent_a, &first.subscription_id)
        .unwrap();
    assert_eq!(
        arrivals
            .subscribe(&agent_a, request("shared", false), None)
            .unwrap(),
        first
    );
    assert!(!arrivals.list(&agent_a).unwrap().subscriptions[0].active);
    drop(arrivals);
    let arrivals = service(root.path(), meta, ArrivalLimits::default());
    assert_eq!(
        arrivals
            .subscribe(&agent_a, request("shared", false), None)
            .unwrap(),
        first
    );
    assert!(!arrivals.list(&agent_a).unwrap().subscriptions[0].active);
}

#[tokio::test]
async fn full_feed_does_not_block_healthy_recipient_or_reorder_source() {
    let root = common::TempDir::new("arrival-full-isolation");
    let limits = ArrivalLimits {
        max_pending_messages: 1,
        scan_batch: 2,
        ..ArrivalLimits::default()
    };
    let arrivals = service(root.path(), meta(), limits);
    let stalled = agent();
    let healthy = agent();
    arrivals
        .subscribe(&stalled, request("stalled", false), None)
        .unwrap();
    arrivals
        .subscribe(&healthy, request("healthy", false), None)
        .unwrap();
    let mut log = source(root.path());
    let mut head = None;
    for version in 1..=4 {
        head = Some(append(
            &mut log,
            routes(
                &arrivals,
                if version == 1 {
                    ArrivalKind::DocPublished
                } else {
                    ArrivalKind::DocUpdated
                },
            ),
            version,
        ));
    }
    arrivals.register_source_head(head.unwrap());
    arrivals.start_projection();
    let first_stalled = page(&arrivals, &stalled).await;
    assert_eq!(first_stalled.notices[0].source.seq, 1);
    for seq in 1..=4 {
        let healthy_page = page(&arrivals, &healthy).await;
        assert_eq!(healthy_page.notices.len(), 1);
        assert_eq!(healthy_page.notices[0].source.seq, seq);
        arrivals.ack(&healthy, &healthy_page.next_cursor).unwrap();
    }
    assert_eq!(page(&arrivals, &stalled).await.notices[0].source.seq, 1);
    arrivals.ack(&stalled, &first_stalled.next_cursor).unwrap();
    assert_eq!(page(&arrivals, &stalled).await.notices[0].source.seq, 2);
    arrivals.shutdown();
}

#[tokio::test]
async fn cursors_are_independent_bound_to_bus_and_identity_and_cannot_skip_head() {
    let root = common::TempDir::new("arrival-cursors");
    let arrivals = service(root.path(), meta(), ArrivalLimits::default());
    let a = agent();
    let b = agent();
    arrivals.subscribe(&a, request("a", false), None).unwrap();
    arrivals.subscribe(&b, request("b", false), None).unwrap();
    let mut log = source(root.path());
    let head = append(&mut log, routes(&arrivals, ArrivalKind::DocPublished), 1);
    arrivals.register_source_head(head);
    arrivals.start_projection();
    let first = page(&arrivals, &a).await;
    arrivals.ack(&a, &first.next_cursor).unwrap();
    assert_eq!(page(&arrivals, &b).await.notices.len(), 1);
    assert!(arrivals.ack(&b, &first.next_cursor).is_err());
    assert!(arrivals
        .ack(&a, &format!("arrival:{}:{a}:999", arrivals.bus_id()))
        .is_err());
    assert!(arrivals.ack(&a, "seq:1").is_err());
    assert_eq!(
        arrivals.ack(&a, &first.acked_cursor).unwrap().cursor,
        first.next_cursor
    );
    arrivals.shutdown();
}

#[tokio::test]
async fn route_capture_survives_unsubscribe_and_retirement_is_terminal() {
    let root = common::TempDir::new("arrival-remove-route");
    let meta = meta();
    let agent = agent();
    let arrivals = service(root.path(), meta.clone(), ArrivalLimits::default());
    let subscription = arrivals
        .subscribe(&agent, request("before-remove", false), None)
        .unwrap();
    let routes = routes(&arrivals, ArrivalKind::DocPublished);
    arrivals
        .unsubscribe(&agent, &subscription.subscription_id)
        .unwrap();
    assert!(arrivals
        .capture_routes(&resource(), ArrivalKind::DocPublished)
        .unwrap()
        .is_none());
    let mut log = source(root.path());
    let head = append(&mut log, routes, 1);
    arrivals.register_source_head(head.clone());
    arrivals.start_projection();
    assert_eq!(page(&arrivals, &agent).await.notices.len(), 1);
    arrivals.retire_agent(&agent);
    assert!(arrivals
        .wait(&agent, None, Duration::ZERO, 1)
        .await
        .is_err());
    arrivals.shutdown();
    drop(arrivals);
    let retired = BTreeSet::from([agent.clone()]);
    let arrivals =
        ArrivalService::open(root.path(), meta, ArrivalLimits::default(), retired).unwrap();
    arrivals.register_source_head(head);
    arrivals.start_projection();
    assert!(arrivals.list(&agent).is_err());
    arrivals.shutdown();
}

#[tokio::test]
async fn source_scan_stops_at_validated_committed_head_and_handles_rotation() {
    let root = common::TempDir::new("arrival-committed-head");
    let arrivals = service(root.path(), meta(), ArrivalLimits::default());
    let agent = agent();
    arrivals
        .subscribe(&agent, request("a", false), None)
        .unwrap();
    let mut log = source(root.path());
    let first = append(&mut log, routes(&arrivals, ArrivalKind::DocPublished), 1);
    let later = append(&mut log, routes(&arrivals, ArrivalKind::DocUpdated), 2);
    arrivals.register_source_head(first);
    arrivals.start_projection();
    let seen = page(&arrivals, &agent).await;
    assert_eq!(seen.notices.len(), 1);
    arrivals.ack(&agent, &seen.next_cursor).unwrap();
    assert!(arrivals
        .wait(&agent, None, Duration::from_millis(50), 64)
        .await
        .unwrap()
        .notices
        .is_empty());
    arrivals.source_committed(later);
    let seen = page(&arrivals, &agent).await;
    assert_eq!(seen.notices[0].source.seq, 2);
    arrivals.shutdown();
}

#[test]
fn limits_reject_excess_subscriptions_and_matching_recipients_before_commit() {
    let root = common::TempDir::new("arrival-limits");
    let limits = ArrivalLimits {
        subscriptions_per_agent: 1,
        recipients_per_resource: 1,
        ..ArrivalLimits::default()
    };
    let arrivals = service(root.path(), meta(), limits);
    let a = agent();
    let b = agent();
    arrivals.subscribe(&a, request("one", false), None).unwrap();
    assert!(matches!(
        arrivals.subscribe(&a, request("two", false), None),
        Err(BrokerError::Application(ErrorCode::ResourceExhausted, _))
    ));
    assert!(matches!(
        arrivals.subscribe(&b, request("one", false), None),
        Err(BrokerError::Application(ErrorCode::ResourceExhausted, _))
    ));
    assert_eq!(arrivals.list(&a).unwrap().subscriptions.len(), 1);
    assert!(arrivals.list(&b).unwrap().subscriptions.is_empty());
}

#[tokio::test]
async fn sparse_feed_pages_recover_rotated_history_and_resume_from_old_anchor() {
    use cooperators::arrivals::{delivery_id, obligation_id, ArrivalNotice, ObligationCoordinate};
    let root = common::TempDir::new("arrival-sparse-anchors");
    let meta = meta();
    let agent = agent();
    let arrivals = service(root.path(), meta.clone(), ArrivalLimits::default());
    let subscription = arrivals
        .subscribe(&agent, request("history", false), None)
        .unwrap();
    let mut log = source(root.path());
    let path = root
        .path()
        .join("agents")
        .join(&agent)
        .join("arrivals.jsonl");
    let (mut feed, _) = LogFile::<ArrivalNotice>::open_rotating(&path, 4000).unwrap();
    let mut head = None;
    for version in 1..=150 {
        let coordinate = append(
            &mut log,
            routes(
                &arrivals,
                if version == 1 {
                    ArrivalKind::DocPublished
                } else {
                    ArrivalKind::DocUpdated
                },
            ),
            version,
        );
        let id = obligation_id(
            &meta.bus_id,
            "document",
            "plan",
            coordinate.seq,
            &coordinate.hash,
            0,
        );
        let mut notice = ArrivalNotice {
            v: 1,
            seq: 0,
            prev_hash: GENESIS_HASH.into(),
            delivery_id: delivery_id(&id, &agent),
            recipient_agent_id: agent.clone(),
            kind: if version == 1 {
                ArrivalKind::DocPublished
            } else {
                ArrivalKind::DocUpdated
            },
            obligation: ObligationCoordinate {
                id,
                journal_type: "document".into(),
                journal_name: "plan".into(),
                seq: coordinate.seq,
                hash: coordinate.hash.clone(),
                item_index: 0,
            },
            source: coordinate.clone(),
            resource: ArrivalReference::Document {
                name: "plan".into(),
                version,
            },
            subscription_ids: vec![subscription.subscription_id.clone()],
        };
        feed.append(&mut notice).unwrap();
        head = Some(coordinate);
    }
    drop(feed);
    drop(arrivals);
    let arrivals = service(root.path(), meta, ArrivalLimits::default());
    arrivals.register_source_head(head.unwrap());
    arrivals.start_projection();
    let first = page(&arrivals, &agent).await;
    assert_eq!(first.notices.len(), 64);
    let second = arrivals
        .wait(&agent, Some(&first.next_cursor), Duration::ZERO, 64)
        .await
        .unwrap();
    assert_eq!(second.notices.len(), 64);
    assert_eq!(second.notices[0].source.seq, 65);
    let third = arrivals
        .wait(&agent, Some(&second.next_cursor), Duration::ZERO, 64)
        .await
        .unwrap();
    assert_eq!(third.notices.len(), 22);
    assert_eq!(third.notices[0].source.seq, 129);
    let latest = append(&mut log, routes(&arrivals, ArrivalKind::DocUpdated), 151);
    arrivals.source_committed(latest);
    let appended = arrivals
        .wait(&agent, Some(&third.next_cursor), Duration::from_secs(3), 64)
        .await
        .unwrap();
    assert_eq!(appended.notices.len(), 1);
    assert_eq!(appended.notices[0].source.seq, 151);
    arrivals.shutdown();
}
