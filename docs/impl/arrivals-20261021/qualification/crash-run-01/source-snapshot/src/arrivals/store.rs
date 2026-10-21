//! Durable subscription and independent feed journals.

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::BrokerError;
use crate::storage::{hash_record, ChainRecord, LogError, LogFile, GENESIS_HASH};

use super::scan::ScanCursor;
use super::{
    delivery_id, obligation_id, ArrivalKind, ArrivalNotice, ArrivalReference, ArrivalResource,
    CatchUpSnapshot, SourceCoordinate, SubscribeArrivalsRequest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionReceipt {
    pub bus_id: String,
    pub agent_id: String,
    pub subscription_id: String,
    pub client_key: String,
    pub resource: ArrivalResource,
    pub event_kinds: Vec<ArrivalKind>,
    pub catch_up: bool,
    pub active: bool,
    pub boundary: Option<SourceCoordinate>,
    pub initial_notice: Option<CatchUpSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionList {
    pub bus_id: String,
    pub agent_id: String,
    pub subscriptions: Vec<SubscriptionReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeReceipt {
    pub bus_id: String,
    pub agent_id: String,
    pub subscription_id: String,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitArrivals {
    pub bus_id: String,
    pub agent_id: String,
    pub notices: Vec<ArrivalNotice>,
    pub next_cursor: String,
    pub acked_cursor: String,
    pub timed_out: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckArrivalsReceipt {
    pub bus_id: String,
    pub agent_id: String,
    pub cursor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SubscriptionRecord {
    pub v: u16,
    pub seq: u64,
    pub prev_hash: String,
    pub kind: String,
    pub agent_id: String,
    pub subscription_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<SubscribeArrivalsRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary: Option<SourceCoordinate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<CatchUpSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AckRecord {
    pub v: u16,
    pub seq: u64,
    pub prev_hash: String,
    pub bus_id: String,
    pub agent_id: String,
    pub through: u64,
}

macro_rules! chain_record {
    ($name:ty) => {
        impl ChainRecord for $name {
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
    };
}
chain_record!(SubscriptionRecord);
chain_record!(AckRecord);

pub(super) type StreamKey = (String, String);

pub(super) struct SubscriptionState {
    pub log: LogFile<SubscriptionRecord>,
    pub receipts: BTreeMap<String, SubscriptionReceipt>,
    pub keys: BTreeMap<(String, String), String>,
    /// Retains removed subscriptions for replay routing and key idempotency.
    pub resources: BTreeMap<String, std::collections::BTreeSet<StreamKey>>,
}

impl SubscriptionState {
    pub fn open(root: &Path, bus_id: &str) -> Result<Self, BrokerError> {
        let mut receipts = BTreeMap::<String, SubscriptionReceipt>::new();
        let mut keys = BTreeMap::<(String, String), String>::new();
        let mut resources = BTreeMap::<String, std::collections::BTreeSet<StreamKey>>::new();
        let (log, _) = LogFile::<SubscriptionRecord>::open_streaming(
            root.join("notifications/subscriptions.jsonl"),
            |event| {
                if event.v != 1 || !crate::id::is_ulid(&event.agent_id) {
                    return Err(LogError::InvalidRecord(
                        "invalid subscription identity/version".into(),
                    ));
                }
                match event.kind.as_str() {
                    "subscription_created" => {
                        let request = event.request.as_ref().ok_or_else(|| {
                            LogError::InvalidRecord("subscription create missing request".into())
                        })?;
                        let key = (event.agent_id.clone(), request.client_key.clone());
                        if keys.contains_key(&key) || receipts.contains_key(&event.subscription_id)
                        {
                            return Err(LogError::InvalidRecord(
                                "duplicate subscription key or ID".into(),
                            ));
                        }
                        super::validate_request(request)
                            .map_err(|error| LogError::InvalidRecord(error.to_string()))?;
                        if let Some(snapshot) = &event.snapshot {
                            super::validate_snapshot(request, snapshot)
                                .map_err(|error| LogError::InvalidRecord(error.to_string()))?;
                            if !request.catch_up
                                || !request.event_kinds.contains(&snapshot.kind)
                                || !snapshot_has_work(snapshot)
                            {
                                return Err(LogError::InvalidRecord(
                                    "invalid subscription catch-up".into(),
                                ));
                            }
                        }
                        resources
                            .entry(event.agent_id.clone())
                            .or_default()
                            .insert((
                                request.resource.stream_type().into(),
                                request.resource.name().into(),
                            ));
                        keys.insert(key, event.subscription_id.clone());
                        receipts.insert(
                            event.subscription_id.clone(),
                            SubscriptionReceipt {
                                bus_id: bus_id.into(),
                                agent_id: event.agent_id.clone(),
                                subscription_id: event.subscription_id.clone(),
                                client_key: request.client_key.clone(),
                                resource: request.resource.clone(),
                                event_kinds: request.event_kinds.clone(),
                                catch_up: request.catch_up,
                                active: true,
                                boundary: event.boundary.clone(),
                                initial_notice: event.snapshot.clone(),
                            },
                        );
                    }
                    "subscription_removed" => {
                        let receipt =
                            receipts.get_mut(&event.subscription_id).ok_or_else(|| {
                                LogError::InvalidRecord("removed subscription is unknown".into())
                            })?;
                        if receipt.agent_id != event.agent_id || !receipt.active {
                            return Err(LogError::InvalidRecord(
                                "invalid subscription removal".into(),
                            ));
                        }
                        receipt.active = false;
                    }
                    _ => return Err(LogError::InvalidRecord("unknown subscription event".into())),
                }
                Ok(())
            },
        )?;
        Ok(Self {
            log,
            receipts,
            keys,
            resources,
        })
    }

    pub fn head(&self) -> SourceCoordinate {
        SourceCoordinate {
            stream: "subscription".into(),
            name: "subscriptions".into(),
            seq: self.log.seq_head(),
            hash: self.log.last_hash().into(),
        }
    }
}

pub(super) fn snapshot_has_work(snapshot: &CatchUpSnapshot) -> bool {
    !matches!(
        &snapshot.resource,
        ArrivalReference::Queue {
            ready_count: Some(0),
            ..
        }
    )
}

#[derive(Debug, Clone)]
pub(super) struct Watermark {
    pub seq: u64,
    pub item_index: u32,
    pub hash: String,
}

pub(super) struct FeedState {
    pub log: LogFile<ArrivalNotice>,
    pub ack_log: LogFile<AckRecord>,
    pub acked: u64,
    pub pending_sizes: VecDeque<(u64, u64)>,
    pub pending_bytes: u64,
    pub watermarks: BTreeMap<StreamKey, Watermark>,
    pub scans: BTreeMap<StreamKey, ScanCursor>,
    pub page_anchors: BTreeMap<u64, ScanCursor>,
    page_scan: ScanCursor,
    max_notice_bytes: usize,
}

impl FeedState {
    pub fn open(
        root: &Path,
        bus_id: &str,
        agent_id: &str,
        max_notice_bytes: usize,
    ) -> Result<Self, BrokerError> {
        let directory = root.join("agents").join(agent_id);
        ensure_directory(&directory)?;
        let mut acked = 0;
        let (ack_log, _) =
            LogFile::<AckRecord>::open_streaming(directory.join("arrivals-acks.jsonl"), |event| {
                if event.v != 1
                    || event.bus_id != bus_id
                    || event.agent_id != agent_id
                    || event.through < acked
                {
                    return Err(LogError::InvalidRecord("invalid arrival ACK record".into()));
                }
                acked = event.through;
                Ok(())
            })?;
        let mut pending_sizes = VecDeque::new();
        let mut pending_bytes = 0u64;
        let mut watermarks = BTreeMap::<StreamKey, Watermark>::new();
        let (log, _) =
            LogFile::<ArrivalNotice>::open_streaming(directory.join("arrivals.jsonl"), |notice| {
                validate_notice(notice, bus_id, agent_id)?;
                let obligation = &notice.obligation;
                let key = (
                    obligation.journal_type.clone(),
                    obligation.journal_name.clone(),
                );
                if let Some(previous) = watermarks.get(&key) {
                    if (obligation.seq, obligation.item_index)
                        <= (previous.seq, previous.item_index)
                    {
                        return Err(LogError::InvalidRecord(
                            "arrival obligation out of order or duplicated".into(),
                        ));
                    }
                }
                watermarks.insert(
                    key,
                    Watermark {
                        seq: obligation.seq,
                        item_index: obligation.item_index,
                        hash: obligation.hash.clone(),
                    },
                );
                if notice.seq > acked {
                    let bytes = serde_json::to_vec(notice)
                        .map_err(LogError::Serialize)?
                        .len() as u64
                        + 1;
                    pending_sizes.push_back((notice.seq, bytes));
                    pending_bytes = pending_bytes.checked_add(bytes).ok_or_else(|| {
                        LogError::InvalidRecord("arrival backlog byte count overflow".into())
                    })?;
                }
                Ok(())
            })?;
        if acked > log.seq_head() {
            return Err(BrokerError::internal(
                "arrival ACK is ahead of restored feed",
            ));
        }
        let head = SourceCoordinate {
            stream: "arrival".into(),
            name: agent_id.into(),
            seq: log.seq_head(),
            hash: log.last_hash().into(),
        };
        let mut page_scan = ScanCursor::default();
        let mut page_anchors = BTreeMap::from([(0, page_scan.clone())]);
        while page_scan
            .next(log.path(), &head, max_notice_bytes)?
            .is_some()
        {
            if page_scan.seq % 64 == 0 {
                page_anchors.insert(page_scan.seq, page_scan.clone());
            }
        }
        Ok(Self {
            log,
            ack_log,
            acked,
            pending_sizes,
            pending_bytes,
            watermarks,
            scans: BTreeMap::new(),
            page_anchors,
            page_scan,
            max_notice_bytes,
        })
    }

    pub fn already_delivered(&self, notice: &ArrivalNotice) -> Result<bool, BrokerError> {
        let obligation = &notice.obligation;
        let key = (
            obligation.journal_type.clone(),
            obligation.journal_name.clone(),
        );
        let Some(previous) = self.watermarks.get(&key) else {
            return Ok(false);
        };
        if obligation.seq == previous.seq && obligation.hash != previous.hash {
            return Err(BrokerError::internal(
                "arrival obligation source hash changed",
            ));
        }
        Ok((obligation.seq, obligation.item_index) <= (previous.seq, previous.item_index))
    }

    pub fn append(&mut self, notice: &mut ArrivalNotice) -> Result<(), BrokerError> {
        let receipt = self.log.append(notice)?;
        // The watermark becomes visible only after its covering sync.
        self.watermarks.insert(
            (
                notice.obligation.journal_type.clone(),
                notice.obligation.journal_name.clone(),
            ),
            Watermark {
                seq: notice.obligation.seq,
                item_index: notice.obligation.item_index,
                hash: notice.obligation.hash.clone(),
            },
        );
        let bytes = serde_json::to_vec(notice)
            .map_err(|error| BrokerError::internal(error.to_string()))?
            .len() as u64
            + 1;
        self.pending_sizes.push_back((receipt.seq, bytes));
        self.pending_bytes += bytes;
        let head = SourceCoordinate {
            stream: "arrival".into(),
            name: notice.recipient_agent_id.clone(),
            seq: self.log.seq_head(),
            hash: self.log.last_hash().into(),
        };
        while self
            .page_scan
            .next(self.log.path(), &head, self.max_notice_bytes)?
            .is_some()
        {
            if self.page_scan.seq % 64 == 0 {
                self.page_anchors
                    .insert(self.page_scan.seq, self.page_scan.clone());
            }
        }
        Ok(())
    }
}

fn validate_notice(notice: &ArrivalNotice, bus_id: &str, agent_id: &str) -> Result<(), LogError> {
    let obligation = &notice.obligation;
    let id = obligation_id(
        bus_id,
        &obligation.journal_type,
        &obligation.journal_name,
        obligation.seq,
        &obligation.hash,
        obligation.item_index,
    );
    if notice.v != 1
        || notice.recipient_agent_id != agent_id
        || obligation.id != id
        || notice.delivery_id != delivery_id(&id, agent_id)
        || obligation.seq == 0
        || notice.source.seq == 0
        || obligation.item_index != 0
    {
        return Err(LogError::InvalidRecord(
            "invalid arrival notice identity".into(),
        ));
    }
    let resource = match &notice.resource {
        ArrivalReference::Document { name, version } if *version > 0 => {
            ArrivalResource::Document { name: name.clone() }
        }
        ArrivalReference::Queue { name, .. } => ArrivalResource::Queue { name: name.clone() },
        _ => return Err(LogError::InvalidRecord("invalid arrival reference".into())),
    };
    if !notice.kind.matches_resource(&resource)
        || notice.source.stream != resource.stream_type()
        || notice.source.name != resource.name()
        || notice.subscription_ids.is_empty()
        || (obligation.journal_type != "subscription"
            && (obligation.journal_type != notice.source.stream
                || obligation.journal_name != notice.source.name
                || obligation.seq != notice.source.seq
                || obligation.hash != notice.source.hash))
    {
        return Err(LogError::InvalidRecord(
            "invalid arrival obligation/reference".into(),
        ));
    }
    // Validate serialization now, before accepting replay-derived indexes.
    hash_record(notice)?;
    Ok(())
}

/// Persist new directory entries all the way to an existing ancestor.
pub(super) fn ensure_directory(path: &Path) -> Result<(), BrokerError> {
    if path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        ensure_directory(parent)?;
    }
    match fs::create_dir(path) {
        Ok(()) => {
            File::open(path)?.sync_all()?;
            if let Some(parent) = path.parent() {
                File::open(parent)?.sync_all()?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

pub(super) fn blank_subscription(
    kind: &str,
    agent_id: &str,
    subscription_id: &str,
) -> SubscriptionRecord {
    SubscriptionRecord {
        v: 1,
        seq: 0,
        prev_hash: GENESIS_HASH.into(),
        kind: kind.into(),
        agent_id: agent_id.into(),
        subscription_id: subscription_id.into(),
        request: None,
        boundary: None,
        snapshot: None,
    }
}
