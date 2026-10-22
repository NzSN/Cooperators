//! Bounded per-inbox group commit. Blocking workers live only while their
//! inbox has queued work; admitted writes outlive cancellation of their caller.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex as StdMutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};

use crate::envelope::Envelope;
use crate::error::{BrokerError, ErrorCode};
use crate::messaging::{
    envelope_line_bytes, inbox_now, DedupeEntry, DedupeIndex, Inbox, InboxFiles, InboxState,
    SendReceipt, SendRequest,
};
use crate::operations::{OperationProof, OperationRef, OperationScope, OperationStore};
use crate::registry::AgentIdentity;
use crate::server::{DaemonConfig, RootLease};
use crate::status::LogHead;
use crate::storage::{segment_paths, GENESIS_HASH};

type DedupeKey = (String, String);
type SendResult = Result<SendReceipt, BrokerError>;

fn lock<T>(mutex: &StdMutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn clone_error(error: &BrokerError) -> BrokerError {
    match error {
        BrokerError::Application(code, detail) => BrokerError::Application(*code, detail.clone()),
        BrokerError::ApplicationData(code, detail, data) => {
            BrokerError::ApplicationData(*code, detail.clone(), data.clone())
        }
        BrokerError::Invalid(detail) => BrokerError::Invalid(detail.clone()),
        BrokerError::Internal(detail) => BrokerError::Internal(detail.clone()),
        BrokerError::Io(error) => BrokerError::Io(match error.raw_os_error() {
            Some(code) => std::io::Error::from_raw_os_error(code),
            None => std::io::Error::new(error.kind(), error.to_string()),
        }),
    }
}

#[derive(Debug, Default)]
pub(crate) struct Completion {
    result: StdMutex<Option<SendResult>>,
    changed: Notify,
}

impl Completion {
    pub(crate) fn operation_status(&self) -> serde_json::Value {
        match lock(&self.result).as_ref() {
            None => serde_json::json!({"state":"pending"}),
            Some(Ok(receipt)) => serde_json::json!({"state":"committed", "result":receipt}),
            Some(Err(_)) => serde_json::json!({"state":"unknown"}),
        }
    }
    pub(crate) fn finish(&self, result: SendResult) {
        let mut slot = lock(&self.result);
        if slot.is_none() {
            *slot = Some(result);
            drop(slot);
            self.changed.notify_waiters();
        }
    }

    pub(crate) async fn wait(&self, duplicate: bool) -> SendResult {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let result = lock(&self.result);
                if let Some(result) = result.as_ref() {
                    return match result {
                        Ok(receipt) => {
                            let mut receipt = receipt.clone();
                            receipt.duplicate = duplicate;
                            Ok(receipt)
                        }
                        Err(error) => Err(clone_error(error)),
                    };
                }
            }
            notified.await;
        }
    }
}

#[derive(Debug, Clone)]
enum DedupeState {
    Committed(DedupeEntry),
    // A failed uncertain write stays here until the store is reconstructed.
    Pending(Arc<Completion>),
}

#[derive(Debug)]
struct WriteJob {
    envelope: Envelope,
    to_agent_id: String,
    ttl: Option<Duration>,
    dedupe_key: Option<DedupeKey>,
    completion: Arc<Completion>,
    enqueued: Instant,
    bound_bytes: u64,
}

#[derive(Debug, Default)]
struct WriteQueue {
    jobs: VecDeque<WriteJob>,
    bytes: u64,
    running: bool,
    failed: Option<Arc<BrokerError>>,
}

/// Independently synchronized inbox state and its last covering-sync heads.
#[derive(Debug)]
pub(crate) struct InboxHandle {
    pub(crate) state: Mutex<InboxState>,
    files: StdMutex<InboxFiles>,
    committed_head: StdMutex<LogHead>,
    committed_expiry_head: StdMutex<Option<LogHead>>,
    queue: StdMutex<WriteQueue>,
    queued: Condvar,
}

fn log_head<T: crate::storage::ChainRecord>(
    log: &crate::storage::LogFile<T>,
) -> Result<LogHead, BrokerError> {
    let mut bytes = 0u64;
    for path in segment_paths(log.path())? {
        bytes = bytes.saturating_add(fs::metadata(path)?.len());
    }
    Ok(LogHead {
        seq: log.seq_head(),
        bytes,
    })
}

impl InboxHandle {
    fn new(inbox: Inbox) -> Result<Self, BrokerError> {
        let committed_head = log_head(&inbox.files.log)?;
        let committed_expiry_head = inbox.files.expiry_log.as_ref().map(log_head).transpose()?;
        Ok(Self {
            state: Mutex::new(inbox.state),
            files: StdMutex::new(inbox.files),
            committed_head: StdMutex::new(committed_head),
            committed_expiry_head: StdMutex::new(committed_expiry_head),
            queue: StdMutex::new(WriteQueue::default()),
            queued: Condvar::new(),
        })
    }

    pub(crate) fn snapshot_head(&self) -> LogHead {
        lock(&self.committed_head).clone()
    }
}

/// Directory, global sender/key reservations, and bounded per-inbox writers.
#[derive(Debug)]
pub(crate) struct InboxStore {
    config: DaemonConfig,
    directory: StdMutex<HashMap<String, Arc<InboxHandle>>>,
    creation: Arc<Mutex<()>>,
    dedupe: StdMutex<HashMap<DedupeKey, DedupeState>>,
    operations: Arc<OperationStore>,
    // Workers hold the store through drain, retaining exclusive root ownership.
    owner: StdMutex<Option<Arc<RootLease>>>,
}

impl InboxStore {
    pub(crate) fn new(
        config: &DaemonConfig,
        loaded: HashMap<String, Inbox>,
        dedupe: DedupeIndex,
        operations: Arc<OperationStore>,
    ) -> Result<Self, BrokerError> {
        if config.write_batch_messages == 0
            || config.write_batch_bytes == 0
            || config.write_queue_messages == 0
            || config.write_queue_bytes == 0
        {
            return Err(BrokerError::invalid(
                "write batch and queue message/byte limits must be positive",
            ));
        }
        operations.recover_inboxes(&loaded)?;
        let mut directory = HashMap::with_capacity(loaded.len());
        for (agent_id, inbox) in loaded {
            directory.insert(agent_id, Arc::new(InboxHandle::new(inbox)?));
        }
        Ok(Self {
            config: config.clone(),
            operations,
            directory: StdMutex::new(directory),
            creation: Arc::new(Mutex::new(())),
            dedupe: StdMutex::new(
                dedupe
                    .into_iter()
                    .map(|(key, entry)| (key, DedupeState::Committed(entry)))
                    .collect(),
            ),
            owner: StdMutex::new(None),
        })
    }

    pub(crate) fn get(&self, agent_id: &str) -> Option<Arc<InboxHandle>> {
        lock(&self.directory).get(agent_id).cloned()
    }

    pub(crate) fn set_owner(&self, owner: Arc<RootLease>) {
        *lock(&self.owner) = Some(owner);
    }

    pub(crate) async fn ensure(
        self: &Arc<Self>,
        agent_id: &str,
    ) -> Result<Arc<InboxHandle>, BrokerError> {
        if let Some(inbox) = self.get(agent_id) {
            return Ok(inbox);
        }
        let creation = self.creation.clone().lock_owned().await;
        if let Some(inbox) = self.get(agent_id) {
            return Ok(inbox);
        }
        let store = self.clone();
        let agent_id = agent_id.to_string();
        tokio::task::spawn_blocking(move || {
            let _creation = creation;
            let inbox = Inbox::open(&store.config.root, &agent_id)?;
            let recovered: Vec<_> = inbox
                .state
                .records
                .iter()
                .filter_map(|envelope| {
                    envelope.idempotency_key.as_ref().map(|key| {
                        (
                            (envelope.from.clone(), key.clone()),
                            DedupeEntry {
                                message_id: envelope.id.clone(),
                                seq: envelope.seq,
                                to: envelope.to.clone(),
                                to_agent_id: agent_id.clone(),
                            },
                        )
                    })
                })
                .collect();
            let handle = Arc::new(InboxHandle::new(inbox)?);
            // Install replayed keys before making the inbox available to sends.
            let mut dedupe = lock(&store.dedupe);
            for (key, entry) in recovered {
                dedupe.entry(key).or_insert(DedupeState::Committed(entry));
            }
            lock(&store.directory).insert(agent_id, handle.clone());
            Ok(handle)
        })
        .await
        .map_err(|error| BrokerError::internal(format!("inbox open worker failed: {error}")))?
    }

    pub(crate) async fn send(
        self: &Arc<Self>,
        sender: &AgentIdentity,
        recipient: &AgentIdentity,
        request: SendRequest,
    ) -> SendResult {
        self.send_inner(sender, recipient, request, None).await
    }

    pub(crate) async fn send_operation(
        self: &Arc<Self>,
        sender: &AgentIdentity,
        recipient: &AgentIdentity,
        request: SendRequest,
        scope: OperationScope,
        reference: OperationRef,
    ) -> SendResult {
        self.send_inner(sender, recipient, request, Some((scope, reference)))
            .await
    }

    async fn send_inner(
        self: &Arc<Self>,
        sender: &AgentIdentity,
        recipient: &AgentIdentity,
        request: SendRequest,
        operation: Option<(OperationScope, OperationRef)>,
    ) -> SendResult {
        // Existing global keys remain usable even if the requested recipient's
        // inbox can no longer be opened or admitted for writes.
        let existing = request.idempotency_key.as_ref().and_then(|key| {
            lock(&self.dedupe)
                .get(&(sender.name.clone(), key.clone()))
                .cloned()
        });
        match existing {
            Some(DedupeState::Committed(entry)) => {
                return Ok(SendReceipt {
                    message_id: entry.message_id,
                    seq: entry.seq,
                    duplicate: true,
                    to: entry.to,
                    to_agent_id: entry.to_agent_id,
                    idempotency_key: request.idempotency_key,
                    ts: None,
                });
            }
            Some(DedupeState::Pending(completion)) => return completion.wait(true).await,
            None => {}
        }
        let digest = operation
            .as_ref()
            .map(|_| crate::operations::send_digest(&recipient.agent_id, &request));
        let mut envelope = Envelope::new(
            sender.name.clone(),
            recipient.name.clone(),
            request.kind,
            request.body,
        )
        .map_err(|error| BrokerError::internal(error.to_string()))?;
        envelope.reply_to = request.reply_to;
        envelope.idempotency_key = request.idempotency_key;
        if let Some((scope, reference)) = operation {
            envelope.operation = Some(OperationProof {
                scope,
                operation: reference,
                digest: digest.expect("operation digest"),
                method: "send_message".into(),
                resource: format!("inbox:{}", recipient.agent_id),
                ttl_ms: request
                    .ttl
                    .map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
                expires_at_ms: request.ttl.map(|_| u64::MAX),
                result: SendReceipt {
                    message_id: envelope.id.clone(),
                    seq: u64::MAX,
                    duplicate: false,
                    to: recipient.name.clone(),
                    to_agent_id: recipient.agent_id.clone(),
                    idempotency_key: None,
                    ts: Some(envelope.ts.clone()),
                },
            });
        }
        if let Some(proof) = &envelope.operation {
            if let Some(existing) = self
                .operations
                .lookup(proof, inbox_now(&self.config.root))?
            {
                return existing.wait(false).await;
            }
        }
        // Every later sequence fits this decimal width; all chain hashes have
        // the same serialized width. This bounds queued bytes before assignment.
        envelope.seq = u64::MAX;
        envelope.prev_hash = GENESIS_HASH.to_string();
        let bound_bytes = envelope_line_bytes(&envelope);
        // Remote reads budget one separator byte per message, matching this
        // JSON-line bound. Include the proof, original result and worst-case
        // sequence/expiry widths so an accepted remote send is always readable.
        if envelope.operation.is_some()
            && bound_bytes > crate::messaging::REMOTE_INBOX_PAGE_BYTES as u64
        {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "durable message and operation proof exceed the remote inbox page budget",
            ));
        }
        let inbox = self.ensure(&recipient.agent_id).await?;
        let dedupe_key = envelope
            .idempotency_key
            .as_ref()
            .map(|key| (sender.name.clone(), key.clone()));
        let completion = Arc::new(Completion::default());
        let (wait_for, duplicate, start_worker) = 'admit: {
            let mut dedupe = lock(&self.dedupe);
            match dedupe_key.as_ref().and_then(|key| dedupe.get(key)) {
                Some(DedupeState::Committed(entry)) => {
                    return Ok(SendReceipt {
                        message_id: entry.message_id.clone(),
                        seq: entry.seq,
                        duplicate: true,
                        to: entry.to.clone(),
                        to_agent_id: entry.to_agent_id.clone(),
                        idempotency_key: envelope.idempotency_key,
                        ts: None,
                    });
                }
                Some(DedupeState::Pending(pending)) => (pending.clone(), true, false),
                None => {
                    let mut queue = lock(&inbox.queue);
                    if let Some(proof) = &envelope.operation {
                        if let Some(existing) = self
                            .operations
                            .lookup(proof, inbox_now(&self.config.root))?
                        {
                            break 'admit (existing, false, false);
                        }
                    }
                    if let Some(error) = &queue.failed {
                        return Err(clone_error(error));
                    }
                    if queue.jobs.len() >= self.config.write_queue_messages
                        || bound_bytes > self.config.write_queue_bytes
                        || queue.bytes.saturating_add(bound_bytes) > self.config.write_queue_bytes
                    {
                        return Err(BrokerError::application(
                            ErrorCode::ResourceExhausted,
                            format!(
                                "inbox write queue for {:?} is full (limits {} messages, {} bytes)",
                                recipient.name,
                                self.config.write_queue_messages,
                                self.config.write_queue_bytes
                            ),
                        ));
                    }
                    if let Some(key) = &dedupe_key {
                        dedupe.insert(key.clone(), DedupeState::Pending(completion.clone()));
                    }
                    if let Some(proof) = &envelope.operation {
                        if let Some(existing) = self.operations.reserve(
                            proof,
                            &completion,
                            inbox_now(&self.config.root),
                        )? {
                            break 'admit (existing, false, false);
                        }
                    }
                    queue.bytes += bound_bytes;
                    queue.jobs.push_back(WriteJob {
                        envelope,
                        to_agent_id: recipient.agent_id.clone(),
                        ttl: request.ttl,
                        dedupe_key,
                        completion: completion.clone(),
                        enqueued: Instant::now(),
                        bound_bytes,
                    });
                    let start_worker = !queue.running;
                    queue.running = true;
                    (completion, false, start_worker)
                }
            }
        };
        // There is no await between reserving the key, enqueueing, and spawning
        // its owner. Dropping this future from here onward cannot cancel I/O.
        if start_worker {
            let store = self.clone();
            let handle = inbox.clone();
            tokio::task::spawn_blocking(move || {
                let mut worker = Worker {
                    store,
                    inbox: handle,
                    active: Vec::new(),
                };
                if catch_unwind(AssertUnwindSafe(|| worker.run())).is_err() {
                    worker.poison_and_fail(BrokerError::internal(
                        "inbox writer panicked; reopening the store is required",
                    ));
                }
            });
        }
        inbox.queued.notify_one();
        wait_for.wait(duplicate).await
    }

    pub(crate) fn apply_committed_heads(&self, heads: &mut BTreeMap<String, LogHead>) {
        let inboxes: Vec<_> = lock(&self.directory)
            .iter()
            .map(|(id, inbox)| (id.clone(), inbox.clone()))
            .collect();
        for (id, inbox) in inboxes {
            let key = format!("agents/{id}/inbox.jsonl");
            let head = inbox.snapshot_head();
            if head.seq == 0 {
                heads.remove(&key);
            } else {
                heads.insert(key, head);
            }
            let key = format!("agents/{id}/expiry.jsonl");
            match lock(&inbox.committed_expiry_head).as_ref() {
                Some(head) if head.seq != 0 => {
                    heads.insert(key, head.clone());
                }
                _ => {
                    heads.remove(&key);
                }
            }
        }
    }

    pub(crate) async fn backlog_snapshot(&self) -> Vec<(String, usize, u64)> {
        let inboxes: Vec<_> = lock(&self.directory)
            .iter()
            .map(|(id, inbox)| (id.clone(), inbox.clone()))
            .collect();
        let mut snapshot = Vec::with_capacity(inboxes.len());
        for (id, handle) in inboxes {
            let inbox = handle.state.lock().await;
            snapshot.push((id, inbox.pending_messages, inbox.pending_bytes));
        }
        snapshot
    }
}

struct Worker {
    store: Arc<InboxStore>,
    inbox: Arc<InboxHandle>,
    // Retained outside catch_unwind so even a panic completes every waiter.
    active: Vec<WriteJob>,
}

impl Worker {
    fn run(&mut self) {
        while self.collect_batch() {
            if let Err(error) = self.commit_batch() {
                self.poison_and_fail(error);
                return;
            }
            self.active.clear();
        }
    }

    fn collect_batch(&mut self) -> bool {
        let mut queue = lock(&self.inbox.queue);
        let Some(front) = queue.jobs.front() else {
            queue.running = false;
            return false;
        };
        let enqueued = front.enqueued;
        loop {
            let remaining = self
                .store
                .config
                .write_batch_delay
                .saturating_sub(enqueued.elapsed());
            if remaining.is_zero()
                || queue.jobs.len() >= self.store.config.write_batch_messages
                || queue.bytes >= self.store.config.write_batch_bytes
            {
                break;
            }
            queue = self
                .inbox
                .queued
                .wait_timeout(queue, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        let mut batch_bytes = 0u64;
        while self.active.len() < self.store.config.write_batch_messages {
            let Some(front) = queue.jobs.front() else {
                break;
            };
            // One envelope larger than the batch target commits alone; the
            // queue admission byte cap still bounds that singleton.
            if !self.active.is_empty()
                && batch_bytes.saturating_add(front.bound_bytes)
                    > self.store.config.write_batch_bytes
            {
                break;
            }
            let job = queue.jobs.pop_front().expect("front was present");
            queue.bytes -= job.bound_bytes;
            batch_bytes += job.bound_bytes;
            self.active.push(job);
        }
        true
    }

    fn reject(&self, job: &WriteJob, error: BrokerError) {
        if let Some(proof) = &job.envelope.operation {
            self.store
                .operations
                .reject_before_effect(proof, &job.completion);
        }
        if let Some(key) = &job.dedupe_key {
            let mut dedupe = lock(&self.store.dedupe);
            if matches!(dedupe.get(key), Some(DedupeState::Pending(pending)) if Arc::ptr_eq(pending, &job.completion))
            {
                dedupe.remove(key);
            }
        }
        job.completion.finish(Err(error));
    }

    fn commit_batch(&mut self) -> Result<(), BrokerError> {
        // Files have one writer, while consumers see only committed state.
        // No read/ack path needs this disk ownership lock.
        let mut files = lock(&self.inbox.files);
        let inbox = self.inbox.state.blocking_lock();
        if inbox.write_failed {
            return Err(BrokerError::internal(
                "inbox write failed; reopening the store is required",
            ));
        }
        // Queue waiting precedes inbox admission, so it does not consume TTL.
        let admitted_at_ms = inbox_now(&self.store.config.root);
        let mut accepted = Vec::new();
        let mut records = Vec::new();
        let mut accepted_bytes = 0u64;
        for (index, job) in self.active.iter().enumerate() {
            let mut envelope = job.envelope.clone();
            let Some(seq) = files.log.next_seq().checked_add(accepted.len() as u64) else {
                self.reject(
                    job,
                    BrokerError::internal("inbox sequence numbers exhausted"),
                );
                continue;
            };
            envelope.seq = seq;
            if let Some(proof) = &mut envelope.operation {
                proof.result.seq = seq;
                proof.expires_at_ms = proof.ttl_ms.map(|ttl| admitted_at_ms.saturating_add(ttl));
            }
            // Actual predecessor hashes differ but retain the same width, so
            // this gives the exact persisted size before touching either log.
            envelope.prev_hash = files.log.last_hash().to_string();
            let bytes = envelope_line_bytes(&envelope);
            let pending_messages = inbox.pending_messages.saturating_add(accepted.len());
            let pending_bytes = inbox.pending_bytes.saturating_add(accepted_bytes);
            if pending_messages >= self.store.config.max_inbox_messages
                || pending_bytes.saturating_add(bytes) > self.store.config.max_inbox_bytes
            {
                self.reject(
                    job,
                    BrokerError::application(
                        ErrorCode::ResourceExhausted,
                        format!(
                            "inbox for {:?} is full (pending {} messages, {} bytes; limits {} messages, {} bytes)",
                            envelope.to,
                            pending_messages,
                            pending_bytes,
                            self.store.config.max_inbox_messages,
                            self.store.config.max_inbox_bytes
                        ),
                    ),
                );
                continue;
            }
            accepted.push(index);
            records.push(envelope);
            accepted_bytes += bytes;
        }
        if records.is_empty() {
            return Ok(());
        }
        // Ack may advance the committed cursor and reduce backlog counters
        // while the files are syncing. Accepted records remain unpublished.
        drop(inbox);
        let expiries: Vec<_> = accepted
            .iter()
            .filter_map(|&index| {
                let job = &self.active[index];
                job.ttl.map(|ttl| {
                    let expires = admitted_at_ms
                        .saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
                    (job.envelope.id.clone(), expires)
                })
            })
            .collect();
        let expiry_receipts = files.append_expiry_batch(&expiries)?;
        if let Some(last) = expiry_receipts.last() {
            let mut snapshot = lock(&self.inbox.committed_expiry_head);
            let bytes = snapshot.as_ref().map_or(0, |head| head.bytes)
                + expiry_receipts
                    .iter()
                    .map(|receipt| receipt.bytes_written)
                    .sum::<u64>();
            *snapshot = Some(LogHead {
                seq: last.seq,
                bytes,
            });
        }
        let receipts = files.log.append_batch(&mut records)?;
        #[cfg(feature = "mbt-injection")]
        if records.iter().any(|envelope| envelope.operation.is_some()) {
            crate::faults::reach(
                files.log.path(),
                crate::faults::FaultPoint::AfterOperationSyncBeforePublish,
            )?;
        }
        let mut inbox = self.inbox.state.blocking_lock();
        // Add to current counters: acknowledgments may have changed them
        // since admission, and cannot have covered these unpublished records.
        inbox.pending_messages += records.len();
        inbox.pending_bytes = inbox.pending_bytes.saturating_add(accepted_bytes);
        inbox.records.extend(records.iter().cloned());
        inbox.expires.extend(expiries);
        {
            let mut snapshot = lock(&self.inbox.committed_head);
            snapshot.seq = files.log.seq_head();
            snapshot.bytes = snapshot.bytes.saturating_add(
                receipts
                    .iter()
                    .map(|receipt| receipt.bytes_written)
                    .sum::<u64>(),
            );
        }
        let notify = inbox.notify.clone();
        drop(inbox);
        drop(files);
        notify.notify_waiters();
        for (&index, envelope) in accepted.iter().zip(records) {
            let job = &self.active[index];
            let receipt = SendReceipt {
                message_id: envelope.id.clone(),
                seq: envelope.seq,
                duplicate: false,
                to: envelope.to.clone(),
                to_agent_id: job.to_agent_id.clone(),
                idempotency_key: envelope.idempotency_key,
                ts: envelope.operation.as_ref().map(|_| envelope.ts.clone()),
            };
            if let Some(key) = &job.dedupe_key {
                lock(&self.store.dedupe).insert(
                    key.clone(),
                    DedupeState::Committed(DedupeEntry {
                        message_id: envelope.id,
                        seq: envelope.seq,
                        to: envelope.to,
                        to_agent_id: job.to_agent_id.clone(),
                    }),
                );
            }
            job.completion.finish(Ok(receipt));
        }
        Ok(())
    }

    fn poison_and_fail(&mut self, error: BrokerError) {
        let queued: Vec<_> = {
            let mut queue = lock(&self.inbox.queue);
            queue.failed = Some(Arc::new(clone_error(&error)));
            queue.bytes = 0;
            queue.running = false;
            queue.jobs.drain(..).collect()
        };
        self.inbox.state.blocking_lock().write_failed = true;
        for job in queued {
            // These jobs never entered a batch, so their keys can be retried
            // against another inbox without risking a duplicate durable write.
            self.reject(&job, clone_error(&error));
        }
        for job in self.active.drain(..) {
            // Retain pending keys for uncertain writes. Retrying against
            // another recipient cannot create a second durable message.
            job.completion
                .finish(Err(if job.envelope.operation.is_some() {
                    BrokerError::application(
                        ErrorCode::OutcomeUnknown,
                        "operation source write is uncertain; authoritative reopen is required",
                    )
                } else {
                    clone_error(&error)
                }));
        }
    }
}
