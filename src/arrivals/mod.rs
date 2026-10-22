//! Durable, broker-authored arrival feeds. Source events remain authoritative;
//! finite workers reconstruct delivery obligations without a completion WAL.

pub mod migration;
mod scan;
mod store;
pub mod types;

pub use migration::ArrivalMeta;
pub use store::{
    AckArrivalsReceipt, SubscriptionList, SubscriptionReceipt, UnsubscribeReceipt, WaitArrivals,
};
pub use types::*;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::sync::Notify;

use crate::error::{BrokerError, ErrorCode};
use crate::storage::GENESIS_HASH;

use scan::MAX_SOURCE_RECORD_BYTES;
use store::{
    blank_subscription, snapshot_has_work, AckRecord, FeedState, StreamKey, SubscriptionRecord,
    SubscriptionState,
};

const MAX_PROJECTION_WORKERS: usize = 4;

struct Feed {
    state: Mutex<FeedState>,
    changed: Notify,
}

#[derive(Default)]
struct Scheduler {
    running: BTreeSet<String>,
    pending: BTreeSet<String>,
    queue: VecDeque<String>,
    errors: BTreeMap<String, String>,
}

impl Scheduler {
    fn enqueue(&mut self, agent_id: String) {
        if self.pending.insert(agent_id.clone()) {
            self.queue.push_back(agent_id);
        }
    }
}

/// Diagnostic failures remain visible until actual service reopen or a
/// successful retry; no failed/full feed discards its source obligation.
#[derive(Debug, Clone, Serialize)]
pub struct ArrivalStatus {
    pub bus_id: String,
    pub projection_started: bool,
    pub workers: usize,
    pub pending_agents: usize,
    pub feed_errors: BTreeMap<String, String>,
}

pub struct ArrivalService {
    root: PathBuf,
    bus_id: String,
    limits: ArrivalLimits,
    subscriptions: Mutex<SubscriptionState>,
    feeds: Mutex<HashMap<String, Arc<Mutex<Option<Arc<Feed>>>>>>,
    retired: Mutex<BTreeSet<String>>,
    sources: Mutex<BTreeMap<StreamKey, SourceCoordinate>>,
    owner: Mutex<Option<Arc<crate::server::RootLease>>>,
    scheduler: Mutex<Scheduler>,
    idle: Condvar,
    started: AtomicBool,
    stopped: AtomicBool,
}

impl std::fmt::Debug for ArrivalService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArrivalService")
            .field("bus_id", &self.bus_id)
            .field("limits", &self.limits)
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl ArrivalService {
    /// Replay uses one-record buffers, pending-window metadata and sparse page
    /// anchors. Retained subscriptions and anchors grow with retained history.
    /// Projection starts explicitly after the caller assigns root ownership.
    pub fn open(
        root: &Path,
        meta: ArrivalMeta,
        limits: ArrivalLimits,
        retired: BTreeSet<String>,
    ) -> Result<Arc<Self>, BrokerError> {
        if meta.v != 1
            || !crate::id::is_ulid(&meta.bus_id)
            || limits.scan_batch == 0
            || limits.page_messages == 0
            || limits.page_bytes < limits.notice_bytes
            || limits.notice_bytes < 1024
            || limits.max_pending_messages == 0
            || limits.max_pending_bytes < limits.notice_bytes
        {
            return Err(BrokerError::invalid("invalid arrival metadata or limits"));
        }
        store::ensure_directory(&root.join("notifications"))?;
        let subscriptions = SubscriptionState::open(root, &meta.bus_id)?;
        Ok(Arc::new(Self {
            root: root.into(),
            bus_id: meta.bus_id,
            limits,
            subscriptions: Mutex::new(subscriptions),
            feeds: Mutex::new(HashMap::new()),
            retired: Mutex::new(retired),
            sources: Mutex::new(BTreeMap::new()),
            owner: Mutex::new(None),
            scheduler: Mutex::new(Scheduler::default()),
            idle: Condvar::new(),
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        }))
    }

    pub fn bus_id(&self) -> &str {
        &self.bus_id
    }

    pub(crate) fn set_owner(&self, owner: Arc<crate::server::RootLease>) {
        *self.owner.lock().unwrap() = Some(owner);
    }

    pub fn start_projection(self: &Arc<Self>) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        self.started.store(true, Ordering::Release);
        self.project_pending();
    }

    /// Stop scheduling and wait until every finite writer releases its lease.
    pub fn shutdown(&self) {
        self.stopped.store(true, Ordering::Release);
        let mut scheduler = self.scheduler.lock().unwrap();
        scheduler.pending.clear();
        scheduler.queue.clear();
        while !scheduler.running.is_empty() {
            scheduler = self.idle.wait(scheduler).unwrap();
        }
        drop(scheduler);
        let slots: Vec<_> = self.feeds.lock().unwrap().values().cloned().collect();
        for slot in slots {
            if let Some(feed) = slot.lock().unwrap().as_ref() {
                feed.changed.notify_waiters();
            }
        }
    }

    pub fn status(&self) -> ArrivalStatus {
        let scheduler = self.scheduler.lock().unwrap();
        ArrivalStatus {
            bus_id: self.bus_id.clone(),
            projection_started: self.started.load(Ordering::Acquire),
            workers: scheduler.running.len(),
            pending_agents: scheduler.pending.len(),
            feed_errors: scheduler.errors.clone(),
        }
    }

    /// Call only while the broker's subscription/source admission lock is held.
    /// No file I/O or destination feed lock is taken here.
    pub fn capture_routes(
        &self,
        resource: &ArrivalResource,
        kind: ArrivalKind,
    ) -> Result<Option<NotificationIntent>, BrokerError> {
        let retired = self.retired.lock().unwrap().clone();
        let subscriptions = self.subscriptions.lock().unwrap();
        if subscriptions.log.requires_recovery() {
            return Err(BrokerError::internal(
                "arrival subscription recovery required before source mutation",
            ));
        }
        if !kind.matches_resource(resource) {
            return Ok(None);
        }
        let mut routes = BTreeMap::<String, Vec<String>>::new();
        for receipt in subscriptions.receipts.values() {
            if receipt.active
                && &receipt.resource == resource
                && receipt.event_kinds.contains(&kind)
                && !retired.contains(&receipt.agent_id)
            {
                routes
                    .entry(receipt.agent_id.clone())
                    .or_default()
                    .push(receipt.subscription_id.clone());
            }
        }
        if routes.is_empty() {
            return Ok(None);
        }
        Ok(Some(NotificationIntent {
            v: 1,
            routes: routes
                .into_iter()
                .map(|(agent_id, mut subscription_ids)| {
                    subscription_ids.sort();
                    NotificationRoute {
                        agent_id,
                        subscription_ids,
                    }
                })
                .collect(),
        }))
    }

    pub fn register_source_head(self: &Arc<Self>, head: SourceCoordinate) {
        self.record_head(head);
    }

    /// A scheduling hint only. Recovery re-discovers obligations from sources.
    pub fn source_committed(self: &Arc<Self>, head: SourceCoordinate) {
        self.record_head(head);
        self.project_pending();
    }

    fn record_head(&self, head: SourceCoordinate) {
        let key = (head.stream.clone(), head.name.clone());
        let mut sources = self.sources.lock().unwrap();
        match sources.get(&key) {
            Some(previous) if previous.seq > head.seq => {}
            Some(previous) if previous.seq == head.seq && previous.hash != head.hash => {
                self.scheduler
                    .lock()
                    .unwrap()
                    .errors
                    .insert("source".into(), "conflicting committed source head".into());
            }
            _ => {
                sources.insert(key, head);
            }
        }
    }

    pub fn retire_agent(&self, agent_id: &str) {
        self.retired.lock().unwrap().insert(agent_id.into());
        self.scheduler.lock().unwrap().pending.remove(agent_id);
        let slot = self.feeds.lock().unwrap().get(agent_id).cloned();
        if let Some(slot) = slot {
            if let Some(feed) = slot.lock().unwrap().as_ref() {
                feed.changed.notify_waiters();
            }
        }
    }

    pub fn subscribe(
        self: &Arc<Self>,
        agent_id: &str,
        mut request: SubscribeArrivalsRequest,
        snapshot: Option<CatchUpSnapshot>,
    ) -> Result<SubscriptionReceipt, BrokerError> {
        self.check_agent(agent_id)?;
        request.event_kinds.sort();
        request.event_kinds.dedup();
        validate_request(&request)?;
        if let Some(snapshot) = &snapshot {
            validate_snapshot(&request, snapshot)?;
        }
        let mut subscriptions = self.subscriptions.lock().unwrap();
        let key = (agent_id.to_owned(), request.client_key.clone());
        if let Some(id) = subscriptions.keys.get(&key) {
            let receipt = &subscriptions.receipts[id];
            if receipt.resource != request.resource
                || receipt.event_kinds != request.event_kinds
                || receipt.catch_up != request.catch_up
            {
                return Err(BrokerError::application(
                    ErrorCode::Conflict,
                    "arrival client_key reused with different arguments",
                ));
            }
            let mut original = receipt.clone();
            original.active = true;
            return Ok(original);
        }
        if subscriptions
            .receipts
            .values()
            .filter(|entry| entry.active && entry.agent_id == agent_id)
            .count()
            >= self.limits.subscriptions_per_agent
        {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "arrival subscription limit reached",
            ));
        }
        let retired = self.retired.lock().unwrap();
        let mut recipient_ids: BTreeSet<&str> = subscriptions
            .receipts
            .values()
            .filter(|entry| {
                entry.active
                    && entry.resource == request.resource
                    && !retired.contains(&entry.agent_id)
            })
            .map(|entry| entry.agent_id.as_str())
            .collect();
        recipient_ids.insert(agent_id);
        if recipient_ids.len() > self.limits.recipients_per_resource {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "arrival resource recipient limit reached",
            ));
        }
        drop(retired);
        let subscription_id = crate::id::new_ulid()?;
        let boundary = snapshot.as_ref().map(|snapshot| snapshot.source.clone());
        let initial_notice = snapshot.filter(|snapshot| {
            request.catch_up
                && request.event_kinds.contains(&snapshot.kind)
                && snapshot_has_work(snapshot)
        });
        let receipt = SubscriptionReceipt {
            bus_id: self.bus_id.clone(),
            agent_id: agent_id.into(),
            subscription_id: subscription_id.clone(),
            client_key: request.client_key.clone(),
            resource: request.resource.clone(),
            event_kinds: request.event_kinds.clone(),
            catch_up: request.catch_up,
            active: true,
            boundary: boundary.clone(),
            initial_notice: initial_notice.clone(),
        };
        // A single recipient can match all its active subscriptions; reject a
        // route configuration whose future reference cannot fit one notice.
        self.check_route_wire_bound(&subscriptions, agent_id, &request, &subscription_id)?;
        let mut event = blank_subscription("subscription_created", agent_id, &subscription_id);
        event.request = Some(request.clone());
        event.boundary = boundary;
        event.snapshot = initial_notice;
        subscriptions.log.append(&mut event)?;
        subscriptions.keys.insert(key, subscription_id.clone());
        subscriptions
            .resources
            .entry(agent_id.into())
            .or_default()
            .insert((
                request.resource.stream_type().into(),
                request.resource.name().into(),
            ));
        subscriptions
            .receipts
            .insert(subscription_id, receipt.clone());
        drop(subscriptions);
        self.project_pending();
        Ok(receipt)
    }

    pub fn unsubscribe(
        self: &Arc<Self>,
        agent_id: &str,
        subscription_id: &str,
    ) -> Result<UnsubscribeReceipt, BrokerError> {
        self.check_agent(agent_id)?;
        let mut subscriptions = self.subscriptions.lock().unwrap();
        let receipt = subscriptions
            .receipts
            .get(subscription_id)
            .filter(|receipt| receipt.agent_id == agent_id)
            .ok_or_else(|| {
                BrokerError::application(ErrorCode::NotFound, "arrival subscription not found")
            })?;
        if receipt.active {
            let mut event = blank_subscription("subscription_removed", agent_id, subscription_id);
            subscriptions.log.append(&mut event)?;
            subscriptions
                .receipts
                .get_mut(subscription_id)
                .unwrap()
                .active = false;
        }
        Ok(UnsubscribeReceipt {
            bus_id: self.bus_id.clone(),
            agent_id: agent_id.into(),
            subscription_id: subscription_id.into(),
            active: false,
        })
    }

    pub fn list(&self, agent_id: &str) -> Result<SubscriptionList, BrokerError> {
        self.check_agent(agent_id)?;
        Ok(SubscriptionList {
            bus_id: self.bus_id.clone(),
            agent_id: agent_id.into(),
            subscriptions: self
                .subscriptions
                .lock()
                .unwrap()
                .receipts
                .values()
                .filter(|receipt| receipt.agent_id == agent_id)
                .cloned()
                .collect(),
        })
    }

    pub(crate) fn list_page(
        &self,
        agent_id: &str,
        after: Option<&str>,
        limit: usize,
        bytes: usize,
        allows: impl Fn(&ArrivalResource) -> bool,
    ) -> Result<(SubscriptionList, Option<String>), BrokerError> {
        self.check_agent(agent_id)?;
        let state = self.subscriptions.lock().unwrap();
        let mut budget = crate::network::page::PageBudget::new(limit, bytes);
        let mut subscriptions: Vec<SubscriptionReceipt> = Vec::new();
        let mut next = None;
        for receipt in state.receipts.values() {
            if receipt.agent_id != agent_id
                || after.is_some_and(|after| receipt.subscription_id.as_str() <= after)
                || !allows(&receipt.resource)
            {
                continue;
            }
            if !budget.admit(receipt)? {
                next = subscriptions
                    .last()
                    .map(|receipt| receipt.subscription_id.clone());
                break;
            }
            subscriptions.push(receipt.clone());
        }
        Ok((
            SubscriptionList {
                bus_id: self.bus_id.clone(),
                agent_id: agent_id.into(),
                subscriptions,
            },
            next,
        ))
    }

    pub async fn wait(
        self: &Arc<Self>,
        agent_id: &str,
        cursor: Option<&str>,
        timeout: Duration,
        limit: usize,
    ) -> Result<WaitArrivals, BrokerError> {
        self.wait_limited(agent_id, cursor, timeout, limit, None)
            .await
    }

    pub(crate) async fn wait_limited(
        self: &Arc<Self>,
        agent_id: &str,
        cursor: Option<&str>,
        timeout: Duration,
        limit: usize,
        byte_limit: Option<usize>,
    ) -> Result<WaitArrivals, BrokerError> {
        self.check_agent(agent_id)?;
        if limit == 0 {
            return Err(BrokerError::invalid("arrival page limit must be positive"));
        }
        let explicit = cursor
            .map(|cursor| self.parse_cursor(agent_id, cursor))
            .transpose()?;
        let service = Arc::clone(self);
        let agent = agent_id.to_owned();
        let feed = tokio::task::spawn_blocking(move || service.feed(&agent))
            .await
            .map_err(|error| BrokerError::internal(error.to_string()))??;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = feed.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let service = Arc::clone(self);
            let agent = agent_id.to_owned();
            let feed_copy = Arc::clone(&feed);
            let page = tokio::task::spawn_blocking(move || {
                service.page(&agent, &feed_copy, explicit, limit, byte_limit)
            })
            .await
            .map_err(|error| BrokerError::internal(error.to_string()))??;
            if !page.notices.is_empty() {
                return Ok(page);
            }
            self.check_agent(agent_id)?;
            if tokio::time::Instant::now() >= deadline {
                return Ok(WaitArrivals {
                    timed_out: true,
                    ..page
                });
            }
            let _ = tokio::time::timeout_at(deadline, notified).await;
        }
    }

    pub fn ack(
        self: &Arc<Self>,
        agent_id: &str,
        cursor: &str,
    ) -> Result<AckArrivalsReceipt, BrokerError> {
        self.check_agent(agent_id)?;
        let through = self.parse_cursor(agent_id, cursor)?;
        let feed = self.feed(agent_id)?;
        let mut state = feed.state.lock().unwrap();
        if through > state.log.seq_head() {
            return Err(BrokerError::invalid(
                "arrival ACK exceeds committed feed head",
            ));
        }
        // Even an idempotent ACK cannot claim success after an uncertain ACK
        // append: reopen first, since its on-disk durable position is unknown.
        if state.ack_log.requires_recovery() {
            return Err(BrokerError::internal("arrival ACK recovery required"));
        }
        if through > state.acked {
            let mut event = AckRecord {
                v: 1,
                seq: 0,
                prev_hash: GENESIS_HASH.into(),
                bus_id: self.bus_id.clone(),
                agent_id: agent_id.into(),
                through,
            };
            state.ack_log.append(&mut event)?;
            state.acked = through;
            while state
                .pending_sizes
                .front()
                .is_some_and(|(seq, _)| *seq <= through)
            {
                let (_, bytes) = state.pending_sizes.pop_front().unwrap();
                state.pending_bytes -= bytes;
            }
        }
        let cursor = self.cursor(agent_id, state.acked);
        drop(state);
        self.project_pending();
        Ok(AckArrivalsReceipt {
            bus_id: self.bus_id.clone(),
            agent_id: agent_id.into(),
            cursor,
        })
    }

    fn page(
        &self,
        agent_id: &str,
        feed: &Feed,
        explicit: Option<u64>,
        limit: usize,
        byte_limit: Option<usize>,
    ) -> Result<WaitArrivals, BrokerError> {
        let state = feed.state.lock().unwrap();
        if state.log.requires_recovery() {
            return Err(BrokerError::internal("arrival feed recovery required"));
        }
        let start = explicit.unwrap_or(state.acked);
        let head = SourceCoordinate {
            stream: "arrival".into(),
            name: agent_id.into(),
            seq: state.log.seq_head(),
            hash: state.log.last_hash().into(),
        };
        let acked_cursor = self.cursor(agent_id, state.acked);
        if start > head.seq {
            return Err(BrokerError::invalid(
                "arrival cursor exceeds committed feed head",
            ));
        }
        // Keep the feed lock through read: rotation and committed-head capture
        // must describe one coherent source. Read allocation is page bounded.
        // One compact anchor per 64 retained records bounds prefix work for
        // explicit historical cursors without retaining historical payloads.
        let mut scanner = state
            .page_anchors
            .range(..=start)
            .next_back()
            .map(|(_, anchor)| anchor.clone())
            .unwrap_or_default();
        let mut notices = Vec::new();
        let mut bytes = 0u64;
        let mut next = start;
        let maximum = self
            .limits
            .page_bytes
            .min(byte_limit.unwrap_or(usize::MAX) as u64);
        while let Some(record) = scanner
            .next(
                state.log.path(),
                &head,
                self.limits
                    .notice_bytes
                    .min(byte_limit.unwrap_or(usize::MAX) as u64) as usize,
            )
            .map_err(|error| {
                if byte_limit.is_some()
                    && matches!(&error, BrokerError::Internal(message)
                if message == "arrival source record exceeds scan bound")
                {
                    BrokerError::application(
                        crate::error::ErrorCode::ResourceExhausted,
                        "next arrival exceeds remote scan budget",
                    )
                } else {
                    error
                }
            })?
        {
            if scanner.seq <= start {
                continue;
            }
            let size = crate::rpc::bounded_json_size(
                &record.value,
                maximum.saturating_sub(bytes + 1) as usize,
            );
            let Some(size) = size else {
                if notices.is_empty() && byte_limit.is_some() {
                    return Err(BrokerError::application(
                        crate::error::ErrorCode::ResourceExhausted,
                        "next arrival exceeds remote page budget",
                    ));
                }
                break;
            };
            let size = size as u64 + 1;
            let notice: ArrivalNotice = serde_json::from_value(record.value)
                .map_err(|error| BrokerError::internal(error.to_string()))?;
            bytes += size;
            next = notice.seq;
            notices.push(notice);
            if notices.len() >= limit.min(self.limits.page_messages) {
                break;
            }
        }
        Ok(WaitArrivals {
            bus_id: self.bus_id.clone(),
            agent_id: agent_id.into(),
            notices,
            next_cursor: self.cursor(agent_id, next),
            acked_cursor,
            timed_out: false,
        })
    }

    fn cursor(&self, agent_id: &str, seq: u64) -> String {
        format!("arrival:{}:{agent_id}:{seq}", self.bus_id)
    }

    fn parse_cursor(&self, agent_id: &str, cursor: &str) -> Result<u64, BrokerError> {
        let prefix = format!("arrival:{}:{agent_id}:", self.bus_id);
        cursor
            .strip_prefix(&prefix)
            .and_then(|seq| seq.parse().ok())
            .ok_or_else(|| {
                BrokerError::invalid(
                    "arrival cursor belongs to another bus/identity or is malformed",
                )
            })
    }

    fn check_agent(&self, agent_id: &str) -> Result<(), BrokerError> {
        if !crate::id::is_ulid(agent_id) {
            return Err(BrokerError::invalid("invalid arrival agent_id"));
        }
        if self.retired.lock().unwrap().contains(agent_id) {
            return Err(BrokerError::application(
                ErrorCode::Unauthenticated,
                "arrival identity retired",
            ));
        }
        if self.stopped.load(Ordering::Acquire) {
            return Err(BrokerError::internal("arrival service is stopped"));
        }
        Ok(())
    }

    fn feed(&self, agent_id: &str) -> Result<Arc<Feed>, BrokerError> {
        // Reserve a per-identity initialization slot, then release the map
        // before recovery I/O. One poisoned recipient cannot block healthy
        // identities opening their feeds.
        let slot = self
            .feeds
            .lock()
            .unwrap()
            .entry(agent_id.into())
            .or_insert_with(|| Arc::new(Mutex::new(None)))
            .clone();
        let mut entry = slot.lock().unwrap();
        if let Some(feed) = entry.as_ref() {
            return Ok(Arc::clone(feed));
        }
        let state = FeedState::open(
            &self.root,
            &self.bus_id,
            agent_id,
            self.limits.notice_bytes as usize,
        )?;
        let feed = Arc::new(Feed {
            state: Mutex::new(state),
            changed: Notify::new(),
        });
        *entry = Some(Arc::clone(&feed));
        Ok(feed)
    }

    /// Explicit retries are finite; a full/failed identity is requeued by a
    /// later source hint, acknowledgment, subscription or operator kick.
    pub fn project_pending(self: &Arc<Self>) {
        if !self.started.load(Ordering::Acquire) || self.stopped.load(Ordering::Acquire) {
            return;
        }
        let retired = self.retired.lock().unwrap().clone();
        let agents: Vec<String> = self
            .subscriptions
            .lock()
            .unwrap()
            .resources
            .keys()
            .filter(|agent| !retired.contains(*agent))
            .cloned()
            .collect();
        drop(retired);
        let mut scheduler = self.scheduler.lock().unwrap();
        for agent_id in agents {
            scheduler.enqueue(agent_id);
        }
        self.launch_workers(&mut scheduler);
    }

    fn launch_workers(self: &Arc<Self>, scheduler: &mut Scheduler) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        while scheduler.running.len() < MAX_PROJECTION_WORKERS {
            let mut next_agent = None;
            for _ in 0..scheduler.queue.len() {
                let candidate = scheduler.queue.pop_front().unwrap();
                if !scheduler.pending.contains(&candidate) {
                    continue;
                }
                if scheduler.running.contains(&candidate) {
                    scheduler.queue.push_back(candidate);
                } else {
                    next_agent = Some(candidate);
                    break;
                }
            }
            let Some(agent_id) = next_agent else {
                break;
            };
            scheduler.pending.remove(&agent_id);
            scheduler.running.insert(agent_id.clone());
            let service = Arc::clone(self);
            let owner = self.owner.lock().unwrap().clone();
            std::thread::spawn(move || {
                // Keep the root lock through every source read/recovery/feed sync.
                let _owner = owner;
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    service.project_agent(&agent_id)
                }))
                .unwrap_or_else(|_| {
                    Err(BrokerError::internal("arrival projection worker panicked"))
                });
                let mut scheduler = service.scheduler.lock().unwrap();
                scheduler.running.remove(&agent_id);
                match outcome {
                    Ok(more) => {
                        scheduler.errors.remove(&agent_id);
                        if more && !service.stopped.load(Ordering::Acquire) {
                            scheduler.enqueue(agent_id);
                        }
                    }
                    Err(error) => {
                        scheduler.errors.insert(agent_id, error.to_string());
                    }
                }
                service.launch_workers(&mut scheduler);
                service.idle.notify_all();
            });
        }
    }

    fn project_agent(&self, agent_id: &str) -> Result<bool, BrokerError> {
        if self.stopped.load(Ordering::Acquire) || self.retired.lock().unwrap().contains(agent_id) {
            return Ok(false);
        }
        let mut heads = self.sources.lock().unwrap().clone();
        let resources = {
            let subscriptions = self.subscriptions.lock().unwrap();
            heads.insert(
                ("subscription".into(), "subscriptions".into()),
                subscriptions.head(),
            );
            subscriptions
                .resources
                .get(agent_id)
                .cloned()
                .unwrap_or_default()
        };
        heads.retain(|key, _| key.0 == "subscription" || resources.contains(key));
        let feed = self.feed(agent_id)?;
        let mut state = feed.state.lock().unwrap();
        if state.log.requires_recovery() {
            return Err(BrokerError::internal("arrival feed recovery required"));
        }
        let mut work = 0;
        let mut made_progress = false;
        let mut unfinished = false;
        for (key, head) in &heads {
            if let Some(watermark) = state.watermarks.get(key) {
                if watermark.seq > head.seq
                    || (watermark.seq == head.seq && watermark.hash != head.hash)
                {
                    return Err(BrokerError::internal(
                        "arrival feed obligation is ahead of restored source",
                    ));
                }
            }
            let path = source_path(&self.root, key)?;
            loop {
                if work >= self.limits.scan_batch || self.stopped.load(Ordering::Acquire) {
                    unfinished = true;
                    break;
                }
                if self.retired.lock().unwrap().contains(agent_id) {
                    return Ok(false);
                }
                let mut scanner = state.scans.remove(key).unwrap_or_default();
                let before = scanner.clone();
                let record = scanner.next(&path, head, MAX_SOURCE_RECORD_BYTES);
                let record = match record {
                    Ok(Some(record)) => record,
                    Ok(None) => {
                        state.scans.insert(key.clone(), scanner);
                        break;
                    }
                    Err(error) => {
                        state.scans.insert(key.clone(), before);
                        return Err(error);
                    }
                };
                work += 1;
                let notice = self.notice_from_record(
                    agent_id,
                    key,
                    scanner.seq,
                    &record.hash,
                    record.value,
                )?;
                if let Some(mut notice) = notice {
                    if !state.already_delivered(&notice)? {
                        // Include assigned chain fields when checking the byte bound.
                        notice.seq = state.log.next_seq();
                        notice.prev_hash = state.log.last_hash().into();
                        let size = serde_json::to_vec(&notice)
                            .map_err(|error| BrokerError::internal(error.to_string()))?
                            .len() as u64
                            + 1;
                        if size > self.limits.notice_bytes {
                            state.scans.insert(key.clone(), before);
                            return Err(BrokerError::internal(
                                "captured arrival notice exceeds configured bound",
                            ));
                        }
                        if state.pending_sizes.len() >= self.limits.max_pending_messages
                            || state.pending_bytes + size > self.limits.max_pending_bytes
                        {
                            state.scans.insert(key.clone(), before);
                            // Other source streams cannot append to a full feed either.
                            return Ok(false);
                        }
                        if let Err(error) = state.append(&mut notice) {
                            state.scans.insert(key.clone(), before);
                            return Err(error);
                        }
                        feed.changed.notify_waiters();
                    }
                }
                state.scans.insert(key.clone(), scanner);
                made_progress = true;
            }
        }
        Ok(unfinished && made_progress)
    }

    fn notice_from_record(
        &self,
        agent_id: &str,
        key: &StreamKey,
        seq: u64,
        hash: &str,
        value: Value,
    ) -> Result<Option<ArrivalNotice>, BrokerError> {
        let (kind, resource, source, subscription_ids) = if key.0 == "subscription" {
            let event: SubscriptionRecord = serde_json::from_value(value)
                .map_err(|error| BrokerError::internal(error.to_string()))?;
            if event.kind != "subscription_created" || event.agent_id != agent_id {
                return Ok(None);
            }
            let Some(snapshot) = event.snapshot else {
                return Ok(None);
            };
            (
                snapshot.kind,
                snapshot.resource,
                snapshot.source,
                vec![event.subscription_id],
            )
        } else {
            let Some(intent) = value
                .get("notification_intent")
                .filter(|value| !value.is_null())
            else {
                return Ok(None);
            };
            let intent: NotificationIntent = serde_json::from_value(intent.clone())
                .map_err(|error| BrokerError::internal(error.to_string()))?;
            if intent.v != 1 {
                return Err(BrokerError::internal("unsupported notification intent"));
            }
            let Some(route) = intent
                .routes
                .into_iter()
                .find(|route| route.agent_id == agent_id)
            else {
                return Ok(None);
            };
            if route.subscription_ids.is_empty() {
                return Err(BrokerError::internal("empty captured notification route"));
            }
            let event_kind = value.get("kind").and_then(Value::as_str).unwrap_or("");
            let (kind, resource) = match (key.0.as_str(), event_kind) {
                ("document", "doc_published" | "doc_updated") => {
                    let version = value
                        .get("version")
                        .and_then(Value::as_u64)
                        .filter(|version| *version > 0)
                        .ok_or_else(|| {
                            BrokerError::internal("notified document missing version")
                        })?;
                    (
                        if event_kind == "doc_published" {
                            ArrivalKind::DocPublished
                        } else {
                            ArrivalKind::DocUpdated
                        },
                        ArrivalReference::Document {
                            name: key.1.clone(),
                            version,
                        },
                    )
                }
                ("queue", "task_enqueued" | "task_requeued") => (
                    ArrivalKind::QueueTaskAvailable,
                    ArrivalReference::Queue {
                        name: key.1.clone(),
                        cause: if event_kind == "task_enqueued" {
                            "enqueued"
                        } else {
                            "requeued"
                        }
                        .into(),
                        task_id: value
                            .get("task_id")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        ready_count: None,
                    },
                ),
                _ => {
                    return Err(BrokerError::internal(
                        "notification intent on unsupported source event",
                    ))
                }
            };
            (
                kind,
                resource,
                SourceCoordinate {
                    stream: key.0.clone(),
                    name: key.1.clone(),
                    seq,
                    hash: hash.into(),
                },
                route.subscription_ids,
            )
        };
        let id = obligation_id(&self.bus_id, &key.0, &key.1, seq, hash, 0);
        Ok(Some(ArrivalNotice {
            v: 1,
            seq: 0,
            prev_hash: GENESIS_HASH.into(),
            delivery_id: delivery_id(&id, agent_id),
            recipient_agent_id: agent_id.into(),
            kind,
            obligation: ObligationCoordinate {
                id,
                journal_type: key.0.clone(),
                journal_name: key.1.clone(),
                seq,
                hash: hash.into(),
                item_index: 0,
            },
            source,
            resource,
            subscription_ids,
        }))
    }

    fn check_route_wire_bound(
        &self,
        subscriptions: &SubscriptionState,
        agent_id: &str,
        request: &SubscribeArrivalsRequest,
        new_id: &str,
    ) -> Result<(), BrokerError> {
        let mut subscription_ids: Vec<String> = subscriptions
            .receipts
            .values()
            .filter(|entry| {
                entry.active && entry.agent_id == agent_id && entry.resource == request.resource
            })
            .map(|entry| entry.subscription_id.clone())
            .collect();
        subscription_ids.push(new_id.into());
        let hash = format!("sha256:{}", "f".repeat(64));
        let id = obligation_id(
            &self.bus_id,
            request.resource.stream_type(),
            request.resource.name(),
            u64::MAX,
            &hash,
            0,
        );
        let resource = match &request.resource {
            ArrivalResource::Document { name } => ArrivalReference::Document {
                name: name.clone(),
                version: u64::MAX,
            },
            ArrivalResource::Queue { name } => ArrivalReference::Queue {
                name: name.clone(),
                cause: "requeued".into(),
                task_id: Some("Z".repeat(26)),
                ready_count: Some(u64::MAX),
            },
        };
        let notice = ArrivalNotice {
            v: 1,
            seq: u64::MAX,
            prev_hash: hash.clone(),
            delivery_id: delivery_id(&id, agent_id),
            recipient_agent_id: agent_id.into(),
            kind: request.event_kinds[0],
            obligation: ObligationCoordinate {
                id,
                journal_type: request.resource.stream_type().into(),
                journal_name: request.resource.name().into(),
                seq: u64::MAX,
                hash: hash.clone(),
                item_index: 0,
            },
            source: SourceCoordinate {
                stream: request.resource.stream_type().into(),
                name: request.resource.name().into(),
                seq: u64::MAX,
                hash,
            },
            resource,
            subscription_ids,
        };
        let mut catchup_notice = notice.clone();
        catchup_notice.obligation.journal_type = "subscription".into();
        catchup_notice.obligation.journal_name = "subscriptions".into();
        let catchup_id = obligation_id(
            &self.bus_id,
            "subscription",
            "subscriptions",
            u64::MAX,
            &catchup_notice.obligation.hash,
            0,
        );
        catchup_notice.obligation.id = catchup_id.clone();
        catchup_notice.delivery_id = delivery_id(&catchup_id, agent_id);
        let catchup_size = serde_json::to_vec(&catchup_notice)
            .map_err(|error| BrokerError::internal(error.to_string()))?
            .len() as u64
            + 1;
        let size = serde_json::to_vec(&notice)
            .map_err(|error| BrokerError::internal(error.to_string()))?
            .len() as u64
            + 1;
        if size.max(catchup_size) > self.limits.notice_bytes {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "arrival route would exceed notice wire bound",
            ));
        }
        Ok(())
    }
}

fn source_path(root: &Path, key: &StreamKey) -> Result<PathBuf, BrokerError> {
    match key.0.as_str() {
        "document" => {
            crate::docs::validate_doc_name(&key.1)?;
            Ok(root.join("docs").join(format!("{}.jsonl", key.1)))
        }
        "queue" => {
            crate::queues::validate_queue_name(&key.1)?;
            Ok(root.join("queues").join(format!("{}.jsonl", key.1)))
        }
        "subscription" if key.1 == "subscriptions" => {
            Ok(root.join("notifications/subscriptions.jsonl"))
        }
        _ => Err(BrokerError::invalid("unsupported arrival source stream")),
    }
}

pub(super) fn validate_request(request: &SubscribeArrivalsRequest) -> Result<(), BrokerError> {
    if request.client_key.is_empty()
        || request.client_key.len() > 128
        || request.client_key.chars().any(char::is_control)
    {
        return Err(BrokerError::invalid(
            "arrival client_key must be 1..=128 bytes without control characters",
        ));
    }
    match &request.resource {
        ArrivalResource::Document { name } => crate::docs::validate_doc_name(name)?,
        ArrivalResource::Queue { name } => crate::queues::validate_queue_name(name)?,
    }
    if request.event_kinds.is_empty()
        || request.event_kinds.len() > 2
        || request
            .event_kinds
            .iter()
            .any(|kind| !kind.matches_resource(&request.resource))
    {
        return Err(BrokerError::invalid(
            "arrival event_kinds do not match resource",
        ));
    }
    Ok(())
}

pub(super) fn validate_snapshot(
    request: &SubscribeArrivalsRequest,
    snapshot: &CatchUpSnapshot,
) -> Result<(), BrokerError> {
    let same = match (&request.resource, &snapshot.resource) {
        (
            ArrivalResource::Document { name },
            ArrivalReference::Document {
                name: other,
                version,
            },
        ) => name == other && *version > 0,
        (ArrivalResource::Queue { name }, ArrivalReference::Queue { name: other, .. }) => {
            name == other
        }
        _ => false,
    };
    if !same
        || !snapshot.kind.matches_resource(&request.resource)
        || snapshot.source.stream != request.resource.stream_type()
        || snapshot.source.name != request.resource.name()
        || snapshot.source.seq == 0
        || snapshot.source.hash.len() != 71
        || !snapshot.source.hash.starts_with("sha256:")
    {
        return Err(BrokerError::invalid(
            "arrival catch-up snapshot does not match resource",
        ));
    }
    Ok(())
}
