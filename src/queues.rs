//! Work queues with leases, fencing tokens, retries, and dead letters
//! (design §7).
//!
//! Each queue is one append-only `queues/<name>.jsonl` log of
//! [`QueueEvent`]s. The claim event's own sequence number is the task's
//! `fence`: a later claim or expiry appends a higher sequence, so a stale
//! worker's `complete_task`/`fail_task` is rejected with `LEASE_LOST`.
//!
//! `attempts` counts claims that actually happened. When an attempt expires
//! or is explicitly failed and `attempts >= max_attempts`, the task moves to
//! `dead/<queue>.jsonl` with its full attempt history; otherwise it becomes
//! pending again and the next claim uses `attempts + 1`.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{BrokerError, ErrorCode};
use crate::id;
use crate::registry::AgentIdentity;
use crate::server::DaemonState;
use crate::storage::{AppendReceipt, ChainRecord, LogFile};

/// Default maximum number of claims before a task is dead-lettered.
pub const DEFAULT_MAX_ATTEMPTS: u64 = 5;
/// Longest accepted lease TTL.
pub const MAX_LEASE_TTL_S: u64 = 86_400;

/// One queue-log record. Every field is top-level so the record participates
/// directly in the hash chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEvent {
    pub seq: u64,
    pub prev_hash: String,
    pub ts: String,
    /// `queue_created`, `task_enqueued`, `task_claimed`, `lease_renewed`,
    /// `task_requeued`, `task_completed`, `task_dead`, or `recovery`.
    pub kind: String,
    pub queue: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub payload: Option<Value>,
    #[serde(default)]
    pub max_attempts: Option<u64>,
    /// Attempts made so far.
    #[serde(default)]
    pub attempts: Option<u64>,
    /// Attempt number this event introduces (next attempt for a requeue).
    #[serde(default)]
    pub attempt: Option<u64>,
    #[serde(default)]
    pub lease_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
    #[serde(default)]
    pub lease_ttl_s: Option<u64>,
    #[serde(default)]
    pub outcome: Option<Value>,
    #[serde(default)]
    pub receipt: Option<Value>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub history: Option<Vec<AttemptRecord>>,
    #[serde(default)]
    pub truncated_bytes: Option<u64>,
}

impl ChainRecord for QueueEvent {
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

/// One claim attempt, retained in the task history and in dead letters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptRecord {
    pub attempt: u64,
    pub lease_id: String,
    pub fence: u64,
    pub agent_id: String,
    pub agent_name: String,
    pub claimed_at: String,
    #[serde(default)]
    pub ended_at: Option<String>,
    #[serde(default)]
    pub result: Option<String>,
}

/// One dead-letter record with the complete attempt history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub seq: u64,
    pub prev_hash: String,
    pub ts: String,
    pub queue: String,
    pub task_id: String,
    pub payload: Value,
    pub max_attempts: u64,
    pub attempts: u64,
    pub reason: String,
    pub history: Vec<AttemptRecord>,
}

impl ChainRecord for DeadLetterRecord {
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

/// Runtime phase of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskPhase {
    Pending,
    Leased,
    Completed,
    Dead,
}

/// An active task lease.
#[derive(Debug, Clone)]
pub(crate) struct TaskLease {
    pub lease_id: String,
    pub fence: u64,
    pub agent_id: String,
    pub agent_name: String,
    pub expires_at_ms: u64,
    pub ttl_s: u64,
    pub attempt: u64,
}

/// Replay-derived task state.
#[derive(Debug, Clone)]
pub(crate) struct TaskState {
    pub task_id: String,
    pub payload: Value,
    pub max_attempts: u64,
    pub attempts: u64,
    pub enqueued_at: String,
    pub ready_seq: u64,
    pub phase: TaskPhase,
    pub lease: Option<TaskLease>,
    pub history: Vec<AttemptRecord>,
    pub outcome: Option<Value>,
    pub reason: Option<String>,
}

/// Replay-derived queue state.
#[derive(Debug, Clone)]
pub(crate) struct QueueRuntime {
    pub created_seq: u64,
    pub created_at: String,
    pub tasks: HashMap<String, TaskState>,
}

impl QueueRuntime {
    fn empty() -> Self {
        Self {
            created_seq: 0,
            created_at: String::new(),
            tasks: HashMap::new(),
        }
    }

    fn pending_task(&self) -> Option<&TaskState> {
        self.tasks
            .values()
            .filter(|task| task.phase == TaskPhase::Pending)
            .min_by_key(|task| task.ready_seq)
    }
}

/// Receipt returned by `create_queue`.
#[derive(Debug, Clone, Serialize)]
pub struct CreateQueueReceipt {
    pub queue: String,
    pub created_seq: u64,
    pub created_at: String,
}

/// Receipt returned by `enqueue`.
#[derive(Debug, Clone, Serialize)]
pub struct EnqueueReceipt {
    pub queue: String,
    pub task_id: String,
    pub seq: u64,
    pub max_attempts: u64,
    pub attempts: u64,
    pub enqueued_at: String,
}

/// Lease view included in task snapshots.
#[derive(Debug, Clone, Serialize)]
pub struct LeaseView {
    pub lease_id: String,
    pub fence: u64,
    pub agent_id: String,
    pub agent_name: String,
    pub expires_at: String,
    pub lease_ttl_s: u64,
    pub attempt: u64,
}

/// Task snapshot returned by `claim_task`.
#[derive(Debug, Clone, Serialize)]
pub struct TaskView {
    pub task_id: String,
    pub payload: Value,
    pub max_attempts: u64,
    pub attempts: u64,
    pub attempt: u64,
    pub enqueued_at: String,
    pub state: TaskPhase,
    pub lease: Option<LeaseView>,
}

/// Receipt returned by `claim_task`.
#[derive(Debug, Clone, Serialize)]
pub struct ClaimTaskReceipt {
    pub queue: String,
    pub task: TaskView,
    pub lease_id: String,
    pub fence: u64,
    pub expires_at: String,
    pub lease_ttl_s: u64,
}

/// Receipt returned by renewal of either a task or document lease.
#[derive(Debug, Clone, Serialize)]
pub struct RenewLeaseReceipt {
    pub lease_id: String,
    pub kind: String,
    pub fence: u64,
    pub expires_at: String,
    pub lease_ttl_s: u64,
}

/// Receipt returned by `complete_task`.
#[derive(Debug, Clone, Serialize)]
pub struct CompleteTaskReceipt {
    pub queue: String,
    pub task_id: String,
    pub lease_id: String,
    pub fence: u64,
    pub status: String,
    pub seq: u64,
    pub outcome: Value,
}

/// Receipt returned by `fail_task`.
#[derive(Debug, Clone, Serialize)]
pub struct FailTaskReceipt {
    pub queue: String,
    pub task_id: String,
    pub lease_id: String,
    pub fence: u64,
    /// `requeued` or `dead`.
    pub status: String,
    pub attempts: u64,
    pub max_attempts: u64,
    pub next_attempt: Option<u64>,
    pub dead_letter_seq: Option<u64>,
    pub seq: u64,
    pub reason: String,
}

/// Validates a queue name before it is used as a file stem.
pub fn validate_queue_name(name: &str) -> Result<(), BrokerError> {
    if name.is_empty() || name.len() > 128 {
        return Err(BrokerError::invalid("queue name must be 1..=128 bytes"));
    }
    if name.starts_with('.') || name.contains("..") {
        return Err(BrokerError::invalid(
            "queue name must not contain path components",
        ));
    }
    if !name.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':')
    }) {
        return Err(BrokerError::invalid(
            "queue name may contain only ASCII letters, digits, '-', '_', '.', and ':'",
        ));
    }
    Ok(())
}

fn queue_path(root: &Path, queue: &str) -> PathBuf {
    root.join("queues").join(format!("{queue}.jsonl"))
}

fn dead_path(root: &Path, queue: &str) -> PathBuf {
    root.join("dead").join(format!("{queue}.jsonl"))
}

fn base_event(kind: &str, queue: &str) -> QueueEvent {
    QueueEvent {
        seq: 0,
        prev_hash: String::new(),
        ts: id::now_rfc3339(),
        kind: kind.to_string(),
        queue: queue.to_string(),
        task_id: None,
        payload: None,
        max_attempts: None,
        attempts: None,
        attempt: None,
        lease_id: None,
        agent_id: None,
        agent_name: None,
        expires_at_ms: None,
        lease_ttl_s: None,
        outcome: None,
        receipt: None,
        reason: None,
        history: None,
        truncated_bytes: None,
    }
}

/// Opens a queue log, replays it into a [`QueueRuntime`], and emits a
/// recovery event when the tail was repaired.
fn open_queue(
    root: &Path,
    queue: &str,
) -> Result<(LogFile<QueueEvent>, QueueRuntime), BrokerError> {
    validate_queue_name(queue)?;
    let path = queue_path(root, queue);
    let (mut log, report) = LogFile::open(&path).map_err(BrokerError::from)?;
    let truncated_bytes = report.truncated_bytes;
    let last_valid_offset = report.last_valid_offset;
    let was_truncated = report.was_truncated();
    let mut runtime = QueueRuntime::empty();
    for event in &report.records {
        apply_event(&mut runtime, event)?;
    }
    if was_truncated {
        let mut recovery = base_event("recovery", queue);
        recovery.truncated_bytes = Some(truncated_bytes);
        recovery.reason = Some(format!(
            "truncated_bytes={truncated_bytes}, last_valid_offset={last_valid_offset}"
        ));
        log.append(&mut recovery)?;
    }
    Ok((log, runtime))
}

/// Loads every queue log at daemon startup.
pub(crate) fn load_all_queues(
    root: &Path,
) -> Result<
    (
        HashMap<String, LogFile<QueueEvent>>,
        HashMap<String, QueueRuntime>,
    ),
    BrokerError,
> {
    let directory = root.join("queues");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((HashMap::new(), HashMap::new()))
        }
        Err(error) => return Err(error.into()),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
                names.push(stem.to_string());
            }
        }
    }
    names.sort();
    let mut logs = HashMap::new();
    let mut states = HashMap::new();
    for name in names {
        if validate_queue_name(&name).is_err() {
            continue;
        }
        let (log, runtime) = open_queue(root, &name)?;
        logs.insert(name.clone(), log);
        states.insert(name, runtime);
    }
    Ok((logs, states))
}

fn apply_event(runtime: &mut QueueRuntime, event: &QueueEvent) -> Result<(), BrokerError> {
    match event.kind.as_str() {
        "queue_created" => {
            if runtime.created_seq == 0 {
                runtime.created_seq = event.seq;
                runtime.created_at = event.ts.clone();
            }
        }
        "task_enqueued" => {
            let task_id = event
                .task_id
                .clone()
                .ok_or_else(|| BrokerError::internal("task_enqueued without task_id"))?;
            let payload = event
                .payload
                .clone()
                .ok_or_else(|| BrokerError::internal("task_enqueued without payload"))?;
            let max_attempts = event.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);
            runtime.tasks.insert(
                task_id.clone(),
                TaskState {
                    task_id,
                    payload,
                    max_attempts,
                    attempts: 0,
                    enqueued_at: event.ts.clone(),
                    ready_seq: event.seq,
                    phase: TaskPhase::Pending,
                    lease: None,
                    history: Vec::new(),
                    outcome: None,
                    reason: None,
                },
            );
        }
        "task_claimed" => {
            let task = runtime
                .tasks
                .get_mut(event.task_id.as_deref().unwrap_or_default())
                .ok_or_else(|| BrokerError::internal("task_claimed for unknown task"))?;
            let attempt = event.attempt.unwrap_or(task.attempts + 1);
            task.attempts = attempt;
            task.phase = TaskPhase::Leased;
            task.lease = Some(TaskLease {
                lease_id: event.lease_id.clone().unwrap_or_default(),
                fence: event.seq,
                agent_id: event.agent_id.clone().unwrap_or_default(),
                agent_name: event.agent_name.clone().unwrap_or_default(),
                expires_at_ms: event.expires_at_ms.unwrap_or(0),
                ttl_s: event.lease_ttl_s.unwrap_or(0),
                attempt,
            });
            task.history.push(AttemptRecord {
                attempt,
                lease_id: event.lease_id.clone().unwrap_or_default(),
                fence: event.seq,
                agent_id: event.agent_id.clone().unwrap_or_default(),
                agent_name: event.agent_name.clone().unwrap_or_default(),
                claimed_at: event.ts.clone(),
                ended_at: None,
                result: None,
            });
        }
        "lease_renewed" => {
            if let Some(task) = runtime
                .tasks
                .get_mut(event.task_id.as_deref().unwrap_or_default())
            {
                if let Some(lease) = &mut task.lease {
                    if lease.lease_id == event.lease_id.clone().unwrap_or_default() {
                        lease.expires_at_ms = event.expires_at_ms.unwrap_or(lease.expires_at_ms);
                    }
                }
            }
        }
        "task_requeued" => {
            if let Some(task) = runtime
                .tasks
                .get_mut(event.task_id.as_deref().unwrap_or_default())
            {
                if let Some(last) = task.history.last_mut() {
                    if last.ended_at.is_none() {
                        last.ended_at = Some(event.ts.clone());
                        last.result = event.reason.clone();
                    }
                }
                task.lease = None;
                task.phase = TaskPhase::Pending;
                task.ready_seq = event.seq;
                task.reason = event.reason.clone();
            }
        }
        "task_completed" => {
            if let Some(task) = runtime
                .tasks
                .get_mut(event.task_id.as_deref().unwrap_or_default())
            {
                if let Some(last) = task.history.last_mut() {
                    if last.ended_at.is_none() {
                        last.ended_at = Some(event.ts.clone());
                        last.result = Some("completed".to_string());
                    }
                }
                task.lease = None;
                task.phase = TaskPhase::Completed;
                task.outcome = event.outcome.clone();
            }
        }
        "task_dead" => {
            if let Some(task) = runtime
                .tasks
                .get_mut(event.task_id.as_deref().unwrap_or_default())
            {
                if let Some(history) = &event.history {
                    task.history = history.clone();
                } else if let Some(last) = task.history.last_mut() {
                    if last.ended_at.is_none() {
                        last.ended_at = Some(event.ts.clone());
                        last.result = event.reason.clone();
                    }
                }
                task.lease = None;
                task.phase = TaskPhase::Dead;
                task.reason = event.reason.clone();
            }
        }
        "recovery" => {}
        other => {
            return Err(BrokerError::internal(format!(
                "unknown queue event kind {other:?}"
            )))
        }
    }
    Ok(())
}

fn is_expired(expires_at_ms: u64, now_ms: u64) -> bool {
    expires_at_ms <= now_ms
}

/// Expires every overdue lease in one queue, appending requeue/dead events
/// and dead-letter records as needed. The caller holds `send_lock`.
fn expire_queue_leases(
    root: &Path,
    queue: &str,
    log: &mut LogFile<QueueEvent>,
    runtime: &mut QueueRuntime,
    now_ms: u64,
) -> Result<u64, BrokerError> {
    let expired: Vec<String> = runtime
        .tasks
        .values()
        .filter(|task| {
            task.phase == TaskPhase::Leased
                && task
                    .lease
                    .as_ref()
                    .is_some_and(|lease| is_expired(lease.expires_at_ms, now_ms))
        })
        .map(|task| task.task_id.clone())
        .collect();
    let mut count = 0;
    for task_id in expired {
        let Some(task) = runtime.tasks.get(&task_id).cloned() else {
            continue;
        };
        let Some(lease) = task.lease.clone() else {
            continue;
        };
        let mut history = task.history.clone();
        if let Some(last) = history.last_mut() {
            last.ended_at = Some(id::now_rfc3339());
            last.result = Some("lease_expired".to_string());
        }
        let next_attempt = task.attempts.saturating_add(1);
        let dead = task.attempts >= task.max_attempts;
        let mut event = if dead {
            let mut event = base_event("task_dead", queue);
            event.reason = Some("lease_expired".to_string());
            event.history = Some(history.clone());
            event
        } else {
            let mut event = base_event("task_requeued", queue);
            event.reason = Some("lease_expired".to_string());
            event.attempt = Some(next_attempt);
            event
        };
        event.task_id = Some(task.task_id.clone());
        event.payload = Some(task.payload.clone());
        event.max_attempts = Some(task.max_attempts);
        event.attempts = Some(task.attempts);
        event.lease_id = Some(lease.lease_id.clone());
        event.agent_id = Some(lease.agent_id.clone());
        event.agent_name = Some(lease.agent_name.clone());
        event.expires_at_ms = Some(lease.expires_at_ms);
        event.lease_ttl_s = Some(lease.ttl_s);
        log.append(&mut event)?;
        apply_event(runtime, &event)?;
        if dead {
            let record = DeadLetterRecord {
                seq: 0,
                prev_hash: String::new(),
                ts: id::now_rfc3339(),
                queue: queue.to_string(),
                task_id: task.task_id.clone(),
                payload: task.payload.clone(),
                max_attempts: task.max_attempts,
                attempts: task.attempts,
                reason: "lease_expired".to_string(),
                history,
            };
            append_dead_letter(root, queue, record)?;
        }
        count += 1;
    }
    Ok(count)
}

fn append_dead_letter(
    root: &Path,
    queue: &str,
    mut record: DeadLetterRecord,
) -> Result<AppendReceipt, BrokerError> {
    let path = dead_path(root, queue);
    let (mut log, _report) = LogFile::open(&path).map_err(BrokerError::from)?;
    log.append(&mut record).map_err(BrokerError::from)
}

impl DaemonState {
    /// Ensures a queue's log and replay state are loaded. Returns false when
    /// the queue does not exist.
    async fn ensure_queue_loaded(&self, queue: &str) -> Result<bool, BrokerError> {
        validate_queue_name(queue)?;
        {
            let states = self.queue_states.lock().await;
            if states.contains_key(queue) {
                return Ok(true);
            }
        }
        if !queue_path(&self.root, queue).exists() {
            return Ok(false);
        }
        let (log, runtime) = open_queue(&self.root, queue)?;
        self.queues.lock().await.insert(queue.to_string(), log);
        self.queue_states
            .lock()
            .await
            .insert(queue.to_string(), runtime);
        Ok(true)
    }

    /// Creates a queue and records `queue_created`. Existing names are never
    /// recycled and return `EXISTS`.
    pub async fn create_queue(&self, queue: &str) -> Result<CreateQueueReceipt, BrokerError> {
        validate_queue_name(queue)?;
        let _send_guard = self.send_lock.lock().await;
        if self.queue_states.lock().await.contains_key(queue)
            || queue_path(&self.root, queue).exists()
        {
            return Err(BrokerError::application(
                ErrorCode::Exists,
                format!("queue {queue:?} already exists"),
            ));
        }
        let (mut log, mut runtime) = open_queue(&self.root, queue)?;
        let mut event = base_event("queue_created", queue);
        let receipt = log.append(&mut event)?;
        apply_event(&mut runtime, &event)?;
        self.queues.lock().await.insert(queue.to_string(), log);
        self.queue_states
            .lock()
            .await
            .insert(queue.to_string(), runtime);
        Ok(CreateQueueReceipt {
            queue: queue.to_string(),
            created_seq: receipt.seq,
            created_at: event.ts,
        })
    }

    /// Appends one task to a queue.
    pub async fn enqueue(
        &self,
        queue: &str,
        payload: Value,
        max_attempts: Option<u64>,
    ) -> Result<EnqueueReceipt, BrokerError> {
        let max_attempts = max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);
        if max_attempts == 0 || max_attempts > 1_000 {
            return Err(BrokerError::invalid("max_attempts must be 1..=1000"));
        }
        let _send_guard = self.send_lock.lock().await;
        if !self.ensure_queue_loaded(queue).await? {
            return Err(BrokerError::application(
                ErrorCode::NotFound,
                format!("unknown queue {queue:?}"),
            ));
        }
        let task_id = id::new_ulid()
            .map_err(|error| BrokerError::internal(format!("generate task id: {error}")))?;
        let mut logs = self.queues.lock().await;
        let log = logs
            .get_mut(queue)
            .ok_or_else(|| BrokerError::internal("queue log vanished"))?;
        let mut event = base_event("task_enqueued", queue);
        event.task_id = Some(task_id.clone());
        event.payload = Some(payload);
        event.max_attempts = Some(max_attempts);
        event.attempts = Some(0);
        let receipt = log.append(&mut event)?;
        let mut states = self.queue_states.lock().await;
        let runtime = states
            .get_mut(queue)
            .ok_or_else(|| BrokerError::internal("queue state vanished"))?;
        apply_event(runtime, &event)?;
        Ok(EnqueueReceipt {
            queue: queue.to_string(),
            task_id,
            seq: receipt.seq,
            max_attempts,
            attempts: 0,
            enqueued_at: event.ts,
        })
    }

    /// Claims the oldest ready task in a queue. The claim log sequence is the
    /// fence returned to the worker.
    pub async fn claim_task(
        &self,
        identity: &AgentIdentity,
        queue: &str,
        lease_ttl_s: u64,
    ) -> Result<ClaimTaskReceipt, BrokerError> {
        if lease_ttl_s == 0 || lease_ttl_s > MAX_LEASE_TTL_S {
            return Err(BrokerError::invalid(format!(
                "lease_ttl_s must be 1..={MAX_LEASE_TTL_S}"
            )));
        }
        let _send_guard = self.send_lock.lock().await;
        if !self.ensure_queue_loaded(queue).await? {
            return Err(BrokerError::application(
                ErrorCode::NotFound,
                format!("unknown queue {queue:?}"),
            ));
        }
        let now_ms = id::unix_millis();
        let mut logs = self.queues.lock().await;
        let log = logs
            .get_mut(queue)
            .ok_or_else(|| BrokerError::internal("queue log vanished"))?;
        let mut states = self.queue_states.lock().await;
        let runtime = states
            .get_mut(queue)
            .ok_or_else(|| BrokerError::internal("queue state vanished"))?;
        expire_queue_leases(&self.root, queue, log, runtime, now_ms)?;

        let task = runtime.pending_task().cloned().ok_or_else(|| {
            BrokerError::application(
                ErrorCode::NotFound,
                format!("queue {queue:?} has no claimable task"),
            )
        })?;
        let lease_id = id::new_ulid()
            .map_err(|error| BrokerError::internal(format!("generate lease id: {error}")))?;
        let attempt = task.attempts.saturating_add(1);
        let expires_at_ms = now_ms.saturating_add(lease_ttl_s.saturating_mul(1_000));
        let mut event = base_event("task_claimed", queue);
        event.task_id = Some(task.task_id.clone());
        event.payload = Some(task.payload.clone());
        event.max_attempts = Some(task.max_attempts);
        event.attempts = Some(attempt);
        event.attempt = Some(attempt);
        event.lease_id = Some(lease_id.clone());
        event.agent_id = Some(identity.agent_id.clone());
        event.agent_name = Some(identity.name.clone());
        event.expires_at_ms = Some(expires_at_ms);
        event.lease_ttl_s = Some(lease_ttl_s);
        let receipt = log.append(&mut event)?;
        apply_event(runtime, &event)?;
        let fence = receipt.seq;
        let lease = runtime
            .tasks
            .get(&task.task_id)
            .and_then(|task| task.lease.clone())
            .ok_or_else(|| BrokerError::internal("claim did not install a lease"))?;
        Ok(ClaimTaskReceipt {
            queue: queue.to_string(),
            task: TaskView {
                task_id: task.task_id,
                payload: task.payload,
                max_attempts: task.max_attempts,
                attempts: attempt,
                attempt,
                enqueued_at: task.enqueued_at,
                state: TaskPhase::Leased,
                lease: Some(lease_view(&lease)),
            },
            lease_id,
            fence,
            expires_at: id::format_rfc3339_millis(expires_at_ms),
            lease_ttl_s,
        })
    }

    /// Renews a queue lease held by `identity`.
    pub(crate) async fn renew_queue_lease(
        &self,
        identity: &AgentIdentity,
        queue: &str,
        lease_id: &str,
    ) -> Result<RenewLeaseReceipt, BrokerError> {
        let now_ms = id::unix_millis();
        let mut logs = self.queues.lock().await;
        let log = logs
            .get_mut(queue)
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "lease lost"))?;
        let mut states = self.queue_states.lock().await;
        let runtime = states
            .get_mut(queue)
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "lease lost"))?;
        expire_queue_leases(&self.root, queue, log, runtime, now_ms)?;
        let task = runtime
            .tasks
            .values()
            .find(|task| {
                task.phase == TaskPhase::Leased
                    && task
                        .lease
                        .as_ref()
                        .is_some_and(|lease| lease.lease_id == lease_id)
            })
            .cloned()
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "lease lost"))?;
        let lease = task
            .lease
            .clone()
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "lease lost"))?;
        if lease.agent_id != identity.agent_id {
            return Err(BrokerError::application(
                ErrorCode::LeaseLost,
                "lease is held by another agent",
            ));
        }
        let expires_at_ms = now_ms.saturating_add(lease.ttl_s.saturating_mul(1_000));
        let mut event = base_event("lease_renewed", queue);
        event.task_id = Some(task.task_id.clone());
        event.attempts = Some(task.attempts);
        event.attempt = Some(lease.attempt);
        event.lease_id = Some(lease.lease_id.clone());
        event.agent_id = Some(lease.agent_id.clone());
        event.agent_name = Some(lease.agent_name.clone());
        event.expires_at_ms = Some(expires_at_ms);
        event.lease_ttl_s = Some(lease.ttl_s);
        log.append(&mut event)?;
        apply_event(runtime, &event)?;
        Ok(RenewLeaseReceipt {
            lease_id: lease.lease_id,
            kind: "task".to_string(),
            fence: lease.fence,
            expires_at: id::format_rfc3339_millis(expires_at_ms),
            lease_ttl_s: lease.ttl_s,
        })
    }

    /// Completes a task only when `lease_id` and the current fence match.
    pub async fn complete_task(
        &self,
        identity: &AgentIdentity,
        lease_id: &str,
        fence: u64,
        outcome: Value,
        receipt: Option<Value>,
    ) -> Result<CompleteTaskReceipt, BrokerError> {
        let _send_guard = self.send_lock.lock().await;
        let queue = self
            .queue_for_lease(lease_id)
            .await
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "unknown lease"))?;
        let now_ms = id::unix_millis();
        let mut logs = self.queues.lock().await;
        let log = logs
            .get_mut(&queue)
            .ok_or_else(|| BrokerError::internal("queue log vanished"))?;
        let mut states = self.queue_states.lock().await;
        let runtime = states
            .get_mut(&queue)
            .ok_or_else(|| BrokerError::internal("queue state vanished"))?;
        expire_queue_leases(&self.root, &queue, log, runtime, now_ms)?;
        let task = runtime
            .tasks
            .values()
            .find(|task| {
                task.phase == TaskPhase::Leased
                    && task
                        .lease
                        .as_ref()
                        .is_some_and(|lease| lease.lease_id == lease_id)
            })
            .cloned()
            .ok_or_else(|| {
                BrokerError::application(ErrorCode::LeaseLost, "lease lost or expired")
            })?;
        let lease = task
            .lease
            .clone()
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "lease lost"))?;
        if lease.agent_id != identity.agent_id {
            return Err(BrokerError::application(
                ErrorCode::LeaseLost,
                "lease is held by another agent",
            ));
        }
        if lease.fence != fence {
            return Err(BrokerError::application(
                ErrorCode::LeaseLost,
                format!("stale fence {fence}; current fence is {}", lease.fence),
            ));
        }
        let mut event = base_event("task_completed", &queue);
        event.task_id = Some(task.task_id.clone());
        event.attempts = Some(task.attempts);
        event.attempt = Some(lease.attempt);
        event.lease_id = Some(lease.lease_id.clone());
        event.agent_id = Some(lease.agent_id.clone());
        event.agent_name = Some(lease.agent_name.clone());
        event.outcome = Some(outcome.clone());
        event.receipt = receipt;
        let append_receipt = log.append(&mut event)?;
        apply_event(runtime, &event)?;
        Ok(CompleteTaskReceipt {
            queue,
            task_id: task.task_id,
            lease_id: lease.lease_id,
            fence,
            status: "completed".to_string(),
            seq: append_receipt.seq,
            outcome,
        })
    }

    /// Reports a failed attempt. The task is requeued while attempts remain,
    /// otherwise it is dead-lettered.
    pub async fn fail_task(
        &self,
        identity: &AgentIdentity,
        lease_id: &str,
        fence: u64,
        reason: &str,
    ) -> Result<FailTaskReceipt, BrokerError> {
        if reason.trim().is_empty() {
            return Err(BrokerError::invalid("reason must not be empty"));
        }
        let _send_guard = self.send_lock.lock().await;
        let queue = self
            .queue_for_lease(lease_id)
            .await
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "unknown lease"))?;
        let now_ms = id::unix_millis();
        let mut logs = self.queues.lock().await;
        let log = logs
            .get_mut(&queue)
            .ok_or_else(|| BrokerError::internal("queue log vanished"))?;
        let mut states = self.queue_states.lock().await;
        let runtime = states
            .get_mut(&queue)
            .ok_or_else(|| BrokerError::internal("queue state vanished"))?;
        expire_queue_leases(&self.root, &queue, log, runtime, now_ms)?;
        let task = runtime
            .tasks
            .values()
            .find(|task| {
                task.phase == TaskPhase::Leased
                    && task
                        .lease
                        .as_ref()
                        .is_some_and(|lease| lease.lease_id == lease_id)
            })
            .cloned()
            .ok_or_else(|| {
                BrokerError::application(ErrorCode::LeaseLost, "lease lost or expired")
            })?;
        let lease = task
            .lease
            .clone()
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "lease lost"))?;
        if lease.agent_id != identity.agent_id {
            return Err(BrokerError::application(
                ErrorCode::LeaseLost,
                "lease is held by another agent",
            ));
        }
        if lease.fence != fence {
            return Err(BrokerError::application(
                ErrorCode::LeaseLost,
                format!("stale fence {fence}; current fence is {}", lease.fence),
            ));
        }
        let mut history = task.history.clone();
        if let Some(last) = history.last_mut() {
            last.ended_at = Some(id::now_rfc3339());
            last.result = Some(format!("failed: {reason}"));
        }
        let dead = task.attempts >= task.max_attempts;
        let next_attempt = task.attempts.saturating_add(1);
        let mut event = if dead {
            let mut event = base_event("task_dead", &queue);
            event.reason = Some(format!("failed: {reason}"));
            event.history = Some(history.clone());
            event
        } else {
            let mut event = base_event("task_requeued", &queue);
            event.reason = Some(format!("failed: {reason}"));
            event.attempt = Some(next_attempt);
            event
        };
        event.task_id = Some(task.task_id.clone());
        event.payload = Some(task.payload.clone());
        event.max_attempts = Some(task.max_attempts);
        event.attempts = Some(task.attempts);
        event.lease_id = Some(lease.lease_id.clone());
        event.agent_id = Some(lease.agent_id.clone());
        event.agent_name = Some(lease.agent_name.clone());
        event.expires_at_ms = Some(lease.expires_at_ms);
        event.lease_ttl_s = Some(lease.ttl_s);
        let append_receipt = log.append(&mut event)?;
        apply_event(runtime, &event)?;
        let dead_letter_seq = if dead {
            Some(
                append_dead_letter(
                    &self.root,
                    &queue,
                    DeadLetterRecord {
                        seq: 0,
                        prev_hash: String::new(),
                        ts: id::now_rfc3339(),
                        queue: queue.clone(),
                        task_id: task.task_id.clone(),
                        payload: task.payload.clone(),
                        max_attempts: task.max_attempts,
                        attempts: task.attempts,
                        reason: format!("failed: {reason}"),
                        history,
                    },
                )?
                .seq,
            )
        } else {
            None
        };
        Ok(FailTaskReceipt {
            queue,
            task_id: task.task_id,
            lease_id: lease.lease_id,
            fence,
            status: if dead { "dead" } else { "requeued" }.to_string(),
            attempts: task.attempts,
            max_attempts: task.max_attempts,
            next_attempt: (!dead).then_some(next_attempt),
            dead_letter_seq,
            seq: append_receipt.seq,
            reason: reason.to_string(),
        })
    }

    pub(crate) async fn queue_for_lease(&self, lease_id: &str) -> Option<String> {
        let states = self.queue_states.lock().await;
        states.iter().find_map(|(queue, runtime)| {
            runtime
                .tasks
                .values()
                .any(|task| {
                    task.phase == TaskPhase::Leased
                        && task
                            .lease
                            .as_ref()
                            .is_some_and(|lease| lease.lease_id == lease_id)
                })
                .then(|| queue.clone())
        })
    }

    /// Expires overdue leases in every queue. Used by the background sweeper
    /// and directly by tests.
    pub async fn sweep_expired_queues(&self) -> Result<u64, BrokerError> {
        let _send_guard = self.send_lock.lock().await;
        let names: Vec<String> = self.queue_states.lock().await.keys().cloned().collect();
        let now_ms = id::unix_millis();
        let mut total = 0;
        for queue in names {
            let mut logs = self.queues.lock().await;
            let Some(log) = logs.get_mut(&queue) else {
                continue;
            };
            let mut states = self.queue_states.lock().await;
            let Some(runtime) = states.get_mut(&queue) else {
                continue;
            };
            total += expire_queue_leases(&self.root, &queue, log, runtime, now_ms)?;
        }
        Ok(total)
    }
}

fn lease_view(lease: &TaskLease) -> LeaseView {
    LeaseView {
        lease_id: lease.lease_id.clone(),
        fence: lease.fence,
        agent_id: lease.agent_id.clone(),
        agent_name: lease.agent_name.clone(),
        expires_at: id::format_rfc3339_millis(lease.expires_at_ms),
        lease_ttl_s: lease.ttl_s,
        attempt: lease.attempt,
    }
}
