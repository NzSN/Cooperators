//! Shared, versioned source intent and arrival-feed wire records.

use serde::{Deserialize, Serialize};

use crate::storage::{canonical_json, sha256_hex, ChainRecord};

/// One exact resource to which an identity can subscribe.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArrivalResource {
    Document { name: String },
    Queue { name: String },
}

impl ArrivalResource {
    pub fn name(&self) -> &str {
        match self {
            Self::Document { name } | Self::Queue { name } => name,
        }
    }

    pub fn stream_type(&self) -> &'static str {
        match self {
            Self::Document { .. } => "document",
            Self::Queue { .. } => "queue",
        }
    }
}

/// Events supported by the first arrival protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ArrivalKind {
    #[serde(rename = "doc.published")]
    DocPublished,
    #[serde(rename = "doc.updated")]
    DocUpdated,
    #[serde(rename = "queue.task_available")]
    QueueTaskAvailable,
}

impl ArrivalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DocPublished => "doc.published",
            Self::DocUpdated => "doc.updated",
            Self::QueueTaskAvailable => "queue.task_available",
        }
    }

    pub fn matches_resource(self, resource: &ArrivalResource) -> bool {
        matches!(
            (self, resource),
            (
                Self::DocPublished | Self::DocUpdated,
                ArrivalResource::Document { .. }
            ) | (Self::QueueTaskAvailable, ArrivalResource::Queue { .. })
        )
    }
}

/// Immutable recipients captured with the source commit, never re-routed on replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationRoute {
    pub agent_id: String,
    pub subscription_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationIntent {
    pub v: u16,
    pub routes: Vec<NotificationRoute>,
}

/// The committed resource boundary referenced by an arrival.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCoordinate {
    pub stream: String,
    pub name: String,
    pub seq: u64,
    pub hash: String,
}

/// The source obligation that caused a delivery. Catch-up obligations belong
/// to the subscription journal, independently of their resource boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObligationCoordinate {
    pub id: String,
    pub journal_type: String,
    pub journal_name: String,
    pub seq: u64,
    pub hash: String,
    pub item_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArrivalReference {
    Document {
        name: String,
        version: u64,
    },
    Queue {
        name: String,
        cause: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ready_count: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatchUpSnapshot {
    pub source: SourceCoordinate,
    pub resource: ArrivalReference,
    pub kind: ArrivalKind,
}

/// Broker-authored independent feed record, containing references only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrivalNotice {
    pub v: u16,
    pub seq: u64,
    pub prev_hash: String,
    pub delivery_id: String,
    pub recipient_agent_id: String,
    pub kind: ArrivalKind,
    pub obligation: ObligationCoordinate,
    pub source: SourceCoordinate,
    pub resource: ArrivalReference,
    pub subscription_ids: Vec<String>,
}

impl ChainRecord for ArrivalNotice {
    fn seq(&self) -> u64 {
        self.seq
    }

    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    fn prev_hash(&self) -> &str {
        &self.prev_hash
    }

    fn set_prev_hash(&mut self, prev_hash: String) {
        self.prev_hash = prev_hash;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeArrivalsRequest {
    pub client_key: String,
    pub resource: ArrivalResource,
    pub event_kinds: Vec<ArrivalKind>,
    #[serde(default = "default_catch_up")]
    pub catch_up: bool,
}

fn default_catch_up() -> bool {
    true
}

/// Bounds apply to active subscriptions, dispatch buffers and pending pages;
/// retained journal history remains available on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ArrivalLimits {
    pub subscriptions_per_agent: usize,
    pub recipients_per_resource: usize,
    pub page_messages: usize,
    pub page_bytes: u64,
    pub notice_bytes: u64,
    pub max_pending_messages: usize,
    pub max_pending_bytes: u64,
    pub scan_batch: usize,
}

impl Default for ArrivalLimits {
    fn default() -> Self {
        Self {
            subscriptions_per_agent: 64,
            recipients_per_resource: 64,
            page_messages: 64,
            page_bytes: 256 * 1024,
            notice_bytes: 16 * 1024,
            max_pending_messages: 10_000,
            max_pending_bytes: 64 * 1024 * 1024,
            scan_batch: 64,
        }
    }
}

/// Stable SHA-256 over a versioned canonical JSON tuple. Arrays retain explicit
/// field boundaries, so names containing delimiters cannot alias another ID.
pub fn obligation_id(
    bus_id: &str,
    journal_type: &str,
    journal_name: &str,
    seq: u64,
    record_hash: &str,
    item_index: u32,
) -> String {
    tuple_hash(serde_json::json!([
        "obligation/v1",
        bus_id,
        journal_type,
        journal_name,
        seq,
        record_hash,
        item_index
    ]))
}

pub fn delivery_id(obligation_id: &str, recipient_agent_id: &str) -> String {
    tuple_hash(serde_json::json!([
        "arrival/v1",
        obligation_id,
        recipient_agent_id
    ]))
}

fn tuple_hash(tuple: serde_json::Value) -> String {
    format!("sha256:{}", sha256_hex(canonical_json(&tuple).as_bytes()))
}
