use cooperators::envelope::{Body, Envelope};
use cooperators::storage::GENESIS_HASH;
use serde_json::{json, Value};

fn sample() -> Envelope {
    Envelope {
        v: 1,
        id: "01J8ZQ6S41TSV4RRFFQ69G5FAV".to_string(),
        seq: 412,
        ts: "2026-10-18T12:34:56.789Z".to_string(),
        from: "codex:/root/task1".to_string(),
        to: "claude:reviewer-1".to_string(),
        kind: "request".to_string(),
        reply_to: Some("01J8ZQ6S40TSV4RRFFQ69G5FAV".to_string()),
        idempotency_key: Some("sha256:abc123".to_string()),
        body: Body::new("text/markdown", "please review"),
        prev_hash: GENESIS_HASH.to_string(),
    }
}

#[test]
fn envelope_serde_roundtrip_preserves_every_design_field() {
    let envelope = sample();
    let encoded = serde_json::to_string(&envelope).expect("serialize envelope");
    let value: Value = serde_json::from_str(&encoded).expect("parse envelope json");
    let object = value.as_object().expect("envelope is an object");

    let expected = [
        "v",
        "id",
        "seq",
        "ts",
        "from",
        "to",
        "kind",
        "reply_to",
        "idempotency_key",
        "body",
        "prev_hash",
    ];
    assert_eq!(object.len(), expected.len());
    for key in expected {
        assert!(object.contains_key(key), "missing envelope field {key}");
    }
    assert_eq!(object["from"], json!("codex:/root/task1"));
    assert_eq!(
        object["body"],
        json!({"mime": "text/markdown", "text": "please review"})
    );

    let decoded: Envelope = serde_json::from_str(&encoded).expect("deserialize envelope");
    assert_eq!(decoded, envelope);
    decoded.validate().expect("sample envelope is valid");
}

#[test]
fn optional_fields_accept_absence_and_serialize_as_null() {
    let mut value = serde_json::to_value(sample()).expect("envelope to value");
    let object = value.as_object_mut().expect("object");
    object.remove("reply_to");
    object.remove("idempotency_key");

    let decoded: Envelope = serde_json::from_value(value).expect("optional fields are optional");
    assert!(decoded.reply_to.is_none());
    assert!(decoded.idempotency_key.is_none());

    let encoded = serde_json::to_value(&decoded).expect("serialize");
    assert!(encoded.get("reply_to").is_some());
    assert!(encoded["reply_to"].is_null());
}

#[test]
fn new_envelope_has_ulid_timestamp_and_genesis_link() {
    let envelope = Envelope::new(
        "codex:test",
        "claude:test",
        "request",
        Body::new("text/plain", "hello"),
    )
    .expect("create envelope");
    assert_eq!(envelope.v, 1);
    assert_eq!(envelope.id.len(), 26);
    assert!(cooperators::id::is_ulid(&envelope.id));
    assert!(envelope.ts.ends_with('Z'), "ts is not UTC: {}", envelope.ts);
    assert_eq!(envelope.seq, 0);
    assert_eq!(envelope.prev_hash, GENESIS_HASH);
}

#[test]
fn validation_rejects_draft_and_malformed_records() {
    let draft = Envelope::new("a", "b", "event", Body::new("text/plain", "x")).unwrap();
    assert!(draft.validate().is_err());

    let mut envelope = sample();
    envelope.v = 2;
    assert!(envelope.validate().is_err());

    let mut envelope = sample();
    envelope.body.mime.clear();
    assert!(envelope.validate().is_err());

    let mut envelope = sample();
    envelope.prev_hash = "sha256:not-a-digest".to_string();
    assert!(envelope.validate().is_err());
}
