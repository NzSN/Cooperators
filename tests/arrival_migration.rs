mod common;

use std::fs;
use std::os::unix::fs::PermissionsExt;

use cooperators::arrivals::migration::{prepare, ArrivalMeta};
use cooperators::arrivals::types::{
    delivery_id, obligation_id, ArrivalKind, ArrivalLimits, NotificationIntent, NotificationRoute,
    SubscribeArrivalsRequest,
};
use cooperators::docs::DocEvent;
use cooperators::queues::QueueEvent;
use cooperators::registry::{new_token, token_hash, Registry, ARRIVALS_REQUIRED_FEATURE};
use cooperators::storage::{hash_record, GENESIS_HASH};
use serde_json::{json, Value};

fn registry_json(root: &std::path::Path) -> Value {
    serde_json::from_slice(&fs::read(root.join("registry.json")).unwrap()).unwrap()
}

#[test]
fn disabled_root_keeps_v1_without_notification_artifacts() {
    let root = common::TempDir::new("arrival-disabled-v1");
    let mut registry = Registry::load(root.path()).unwrap();
    registry.register("alice".into(), None, None, None).unwrap();
    let before = fs::read(root.path().join("registry.json")).unwrap();
    assert!(prepare(root.path(), &mut registry, false)
        .unwrap()
        .is_none());
    assert_eq!(registry.format_version(), 1);
    assert!(registry.required_features().is_empty());
    assert!(!registry.arrivals_enabled());
    assert_eq!(before, fs::read(root.path().join("registry.json")).unwrap());
    assert!(!root.path().join("notifications").exists());
}

#[test]
fn promotion_preserves_identities_and_all_following_saves_preserve_v2() {
    let root = common::TempDir::new("arrival-preserve-v2");
    let mut registry = Registry::load(root.path()).unwrap();
    let launch_key = new_token().unwrap();
    let first = registry
        .enroll(launch_key.clone(), "codex".into(), "reviewer".into(), None)
        .unwrap();
    let before = registry_json(root.path());
    let meta = prepare(root.path(), &mut registry, true).unwrap().unwrap();
    assert_eq!(before["agents"], registry_json(root.path())["agents"]);
    assert!(registry.arrivals_enabled());
    assert_eq!(registry.format_version(), 2);
    assert_eq!(registry.required_features(), &[ARRIVALS_REQUIRED_FEATURE]);
    assert_eq!(
        fs::metadata(root.path().join("notifications/meta.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let second = registry
        .enroll(
            new_token().unwrap(),
            "claude".into(),
            "reviewer".into(),
            None,
        )
        .unwrap();
    assert_eq!(registry_json(root.path())["v"], 2);
    let rotated = registry
        .register(
            first.name.clone(),
            Some("codex".into()),
            None,
            Some(&first.token),
        )
        .unwrap();
    assert_eq!(rotated.agent_id, first.agent_id);
    assert_eq!(registry_json(root.path())["v"], 2);
    registry.retire(&second.token).unwrap();
    let after = registry_json(root.path());
    assert_eq!(after["v"], 2);
    assert_eq!(
        after["required_features"],
        json!([ARRIVALS_REQUIRED_FEATURE])
    );
    assert_eq!(
        after["agents"][&first.name]["launch_key_hash"],
        token_hash(&launch_key)
    );
    assert!(after["revoked_token_hashes"]
        .as_array()
        .unwrap()
        .contains(&json!(token_hash(&launch_key))));
    assert!(registry.retired_agent_ids().contains(&second.agent_id));

    let mut reopened = Registry::load(root.path()).unwrap();
    assert!(reopened.authenticate(&rotated.token).is_some());
    assert!(reopened.authenticate(&second.token).is_none());
    assert_eq!(
        prepare(root.path(), &mut reopened, false).unwrap(),
        Some(meta)
    );
    assert_eq!(reopened.format_version(), 2);
}

#[test]
fn metadata_before_v2_is_reused_without_changing_bus_identity() {
    let root = common::TempDir::new("arrival-orphan-meta");
    fs::create_dir(root.path().join("notifications")).unwrap();
    let meta = ArrivalMeta {
        v: 1,
        bus_id: cooperators::id::new_ulid().unwrap(),
    };
    fs::write(
        root.path().join("notifications/meta.json"),
        serde_json::to_vec(&meta).unwrap(),
    )
    .unwrap();
    let mut registry = Registry::load(root.path()).unwrap();
    assert!(prepare(root.path(), &mut registry, false)
        .unwrap()
        .is_none());
    assert_eq!(registry.format_version(), 1);
    assert_eq!(
        prepare(root.path(), &mut registry, true).unwrap(),
        Some(meta)
    );
}

#[test]
fn v2_missing_or_incompatible_metadata_fails_without_replacing_it() {
    for invalid in [None, Some(json!({"v": 2, "bus_id": "invalid"}))] {
        let root = common::TempDir::new("arrival-v2-bad-meta");
        fs::write(
            root.path().join("registry.json"),
            serde_json::to_vec(&json!({
                "v": 2,
                "required_features": [ARRIVALS_REQUIRED_FEATURE],
                "agents": {}
            }))
            .unwrap(),
        )
        .unwrap();
        if let Some(value) = &invalid {
            fs::create_dir(root.path().join("notifications")).unwrap();
            fs::write(
                root.path().join("notifications/meta.json"),
                serde_json::to_vec(value).unwrap(),
            )
            .unwrap();
        }
        let mut registry = Registry::load(root.path()).unwrap();
        assert!(prepare(root.path(), &mut registry, true).is_err());
        assert!(prepare(root.path(), &mut registry, false).is_err());
        assert!(registry
            .register("blocked".into(), None, None, None)
            .is_err());
        match invalid {
            Some(value) => assert_eq!(
                serde_json::from_slice::<Value>(
                    &fs::read(root.path().join("notifications/meta.json")).unwrap()
                )
                .unwrap(),
                value
            ),
            None => assert!(!root.path().join("notifications").exists()),
        }
    }
}

#[test]
fn unknown_required_features_and_invalid_format_combinations_fail_early() {
    for value in [
        json!({"v": 2, "required_features": [ARRIVALS_REQUIRED_FEATURE, "future_feature"]}),
        json!({"v": 1, "required_features": ["future_feature"]}),
        json!({"v": 1, "required_features": [ARRIVALS_REQUIRED_FEATURE]}),
        json!({"v": 2}),
        json!({"v": 3}),
    ] {
        let root = common::TempDir::new("arrival-unknown-format");
        fs::write(
            root.path().join("registry.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        let source = root.path().join("source.jsonl");
        fs::write(&source, b"an intentionally unrepaired source tail").unwrap();
        assert!(Registry::load(root.path()).is_err());
        assert_eq!(
            fs::read(&source).unwrap(),
            b"an intentionally unrepaired source tail"
        );
    }
}

#[test]
fn promotion_failure_fences_retry_and_mutations_until_real_reopen() {
    let root = common::TempDir::new("arrival-fence-promotion");
    let mut registry = Registry::load(root.path()).unwrap();
    // Force final registry rename to fail after metadata has become durable.
    fs::create_dir(root.path().join("registry.json")).unwrap();
    assert!(prepare(root.path(), &mut registry, true).is_err());
    let meta_bytes = fs::read(root.path().join("notifications/meta.json")).unwrap();
    let meta: ArrivalMeta = serde_json::from_slice(&meta_bytes).unwrap();
    assert_eq!(registry.format_version(), 2);
    fs::remove_dir(root.path().join("registry.json")).unwrap();
    assert!(prepare(root.path(), &mut registry, true).is_err());
    assert!(registry
        .register("blocked".into(), None, None, None)
        .is_err());
    assert!(registry
        .enroll(new_token().unwrap(), "codex".into(), "blocked".into(), None)
        .is_err());
    assert!(!root.path().join("registry.json").exists());
    let mut reopened = Registry::load(root.path()).unwrap();
    assert_eq!(reopened.format_version(), 1);
    assert_eq!(
        prepare(root.path(), &mut reopened, true).unwrap(),
        Some(meta)
    );
    assert_eq!(
        fs::read(root.path().join("notifications/meta.json")).unwrap(),
        meta_bytes
    );
}

#[test]
fn directory_failure_also_requires_reopen_and_does_not_promote() {
    let root = common::TempDir::new("arrival-fence-directory");
    let mut registry = Registry::load(root.path()).unwrap();
    fs::write(root.path().join("notifications"), b"not a directory").unwrap();
    assert!(prepare(root.path(), &mut registry, true).is_err());
    fs::remove_file(root.path().join("notifications")).unwrap();
    assert!(prepare(root.path(), &mut registry, true).is_err());
    assert_eq!(registry.format_version(), 1);
    assert!(!root.path().join("registry.json").exists());
    let mut reopened = Registry::load(root.path()).unwrap();
    assert!(prepare(root.path(), &mut reopened, true).unwrap().is_some());
}

#[test]
fn historical_source_hashes_omit_absent_notification_intent() {
    let document = json!({
        "seq": 1, "prev_hash": GENESIS_HASH, "ts": "2026-10-20T00:00:00Z",
        "kind": "doc_published", "doc": "architecture", "version": 1,
        "previous_version": null, "actor_id": "actor", "actor_name": "alice",
        "title": "Architecture", "mime": "text/plain", "body": null,
        "blob": null, "lease_id": null, "fence": null, "expires_at_ms": null,
        "lease_ttl_s": null, "reason": null, "truncated_bytes": null
    });
    let mut decoded: DocEvent = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), document);
    assert_eq!(
        hash_record(&decoded).unwrap(),
        hash_record(&document).unwrap()
    );
    decoded.notification_intent = Some(NotificationIntent {
        v: 1,
        routes: vec![NotificationRoute {
            agent_id: "recipient".into(),
            subscription_ids: vec!["subscription".into()],
        }],
    });
    assert_ne!(
        hash_record(&decoded).unwrap(),
        hash_record(&document).unwrap()
    );
    let annotated: DocEvent =
        serde_json::from_value(serde_json::to_value(&decoded).unwrap()).unwrap();
    assert_eq!(
        hash_record(&decoded).unwrap(),
        hash_record(&annotated).unwrap()
    );

    let queue = json!({
        "seq": 1, "prev_hash": GENESIS_HASH, "ts": "2026-10-20T00:00:00Z",
        "kind": "task_enqueued", "queue": "reviews", "task_id": "task",
        "payload": null, "max_attempts": 5, "attempts": 0, "attempt": null,
        "lease_id": null, "agent_id": null, "agent_name": null,
        "expires_at_ms": null, "lease_ttl_s": null, "outcome": null,
        "receipt": null, "reason": null, "history": null, "truncated_bytes": null
    });
    let decoded: QueueEvent = serde_json::from_value(queue.clone()).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), queue);
    assert_eq!(hash_record(&decoded).unwrap(), hash_record(&queue).unwrap());
}

#[test]
fn stable_id_tuples_are_versioned_unambiguous_and_scope_every_coordinate() {
    let expected = "sha256:5590e2841dd242043e6954d002e5ae8664b4e468607392803e980b7ff6a764bc";
    assert_eq!(
        obligation_id("bus", "document", "architecture", 17, "sha256:record", 0),
        expected
    );
    assert_eq!(
        delivery_id(expected, "agent"),
        "sha256:2d1b5043b59f2eecf24c91361449b98a1150ff71eb2dfe53baa58104e56d742b"
    );
    for changed in [
        obligation_id("other", "document", "architecture", 17, "sha256:record", 0),
        obligation_id("bus", "queue", "architecture", 17, "sha256:record", 0),
        obligation_id("bus", "document", "other", 17, "sha256:record", 0),
        obligation_id("bus", "document", "architecture", 18, "sha256:record", 0),
        obligation_id("bus", "document", "architecture", 17, "sha256:other", 0),
        obligation_id("bus", "document", "architecture", 17, "sha256:record", 1),
    ] {
        assert_ne!(expected, changed);
    }
    assert_ne!(
        delivery_id(expected, "agent"),
        delivery_id(expected, "other")
    );
    assert_ne!(
        obligation_id("a|b", "c", "d", 1, "hash", 0),
        obligation_id("a", "b|c", "d", 1, "hash", 0)
    );
}

#[test]
fn wire_defaults_and_kind_names_match_the_arrival_contract() {
    let request: SubscribeArrivalsRequest = serde_json::from_value(json!({
        "client_key": "reviewer", "resource": {"type": "document", "name": "plan"},
        "event_kinds": ["doc.published", "doc.updated"]
    }))
    .unwrap();
    assert!(request.catch_up);
    assert_eq!(
        request.event_kinds,
        vec![ArrivalKind::DocPublished, ArrivalKind::DocUpdated]
    );
    assert_eq!(request.resource.name(), "plan");
    assert_eq!(request.resource.stream_type(), "document");
    assert!(ArrivalKind::DocUpdated.matches_resource(&request.resource));
    assert!(!ArrivalKind::QueueTaskAvailable.matches_resource(&request.resource));
    assert_eq!(
        ArrivalKind::QueueTaskAvailable.as_str(),
        "queue.task_available"
    );
    let limits: ArrivalLimits = serde_json::from_value(json!({"scan_batch": 3})).unwrap();
    assert_eq!(limits.scan_batch, 3);
    assert_eq!(limits.page_messages, 64);
    assert_eq!(limits.page_bytes, 256 * 1024);
    assert_eq!(limits.notice_bytes, 16 * 1024);
    assert_eq!(limits.max_pending_messages, 10_000);
    assert_eq!(limits.max_pending_bytes, 64 * 1024 * 1024);
}
