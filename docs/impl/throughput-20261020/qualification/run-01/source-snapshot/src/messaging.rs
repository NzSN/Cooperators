//! Direct messaging: inbox logs, dedupe, peek cursors, and durable TTLs
//! (design §5).
//!
//! Every inbox is an append-only [`LogFile<Envelope>`] at
//! `agents/<agent_id>/inbox.jsonl`. The daemon owns all appends. Replay at
//! startup rebuilds three indexes from the logs alone:
//!
//! - the message list used by `wait_message`,
//! - `(sender, idempotency_key) -> (message_id, seq)` for dedupe, and
//! - `message_id -> expires_at` from the optional companion
//!   `agents/<agent_id>/expiry.jsonl` log.
//!
//! TTL is recorded in the companion log because the normative v1 envelope has
//! no expiry field; keeping it out of the envelope preserves the frozen wire
//! schema while making expiry survive restarts.
//!
//! Cursors are opaque decimal sequence numbers (`"0"` means "from the
//! beginning"). `wait_message` never advances state; only `ack` moves the
//! per-agent acknowledged cursor, and clients may always pass an explicit
//! cursor instead. Acknowledged cursors are in-memory only: clients that need
//! exactly-once consumption across a daemon restart must persist the cursor
//! they last consumed and pass it explicitly.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Notify;
use tokio::time::{timeout, Instant};

use crate::envelope::{Body, Envelope};
use crate::error::{BrokerError, ErrorCode};
use crate::id;
use crate::registry::AgentIdentity;
use crate::server::DaemonState;
use crate::storage::{AppendReceipt, ChainRecord, LogFile};

/// Sender name used by daemon-generated records (recovery, presence).
pub const DAEMON_SENDER: &str = "daemon:cooperators";

/// Companion-log record that persists a message TTL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpiryRecord {
    pub seq: u64,
    pub prev_hash: String,
    pub message_id: String,
    pub expires_at_ms: u64,
    pub ts: String,
}

impl ChainRecord for ExpiryRecord {
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

/// Request accepted by [`DaemonState::send_message`].
#[derive(Debug, Clone)]
pub struct SendRequest {
    pub to: String,
    pub kind: String,
    pub body: Body,
    pub reply_to: Option<String>,
    pub idempotency_key: Option<String>,
    pub ttl: Option<Duration>,
}

/// Durable send acknowledgement.
#[derive(Debug, Clone, Serialize)]
pub struct SendReceipt {
    pub message_id: String,
    pub seq: u64,
    /// True when the idempotency key resolved to an existing message.
    pub duplicate: bool,
    pub to: String,
    pub to_agent_id: String,
    pub idempotency_key: Option<String>,
}

/// Batch returned by a wait (peek).
#[derive(Debug, Clone, Serialize)]
pub struct WaitBatch {
    pub messages: Vec<Envelope>,
    /// Cursor for the last message in this batch; pass to `ack` to consume.
    pub next_cursor: String,
    /// Cursor this wait started from.
    pub cursor: String,
    /// Current acknowledged cursor for the agent.
    pub acked_cursor: String,
    /// True when the wait expired without new messages.
    pub timed_out: bool,
}

/// Result of advancing the acknowledged cursor.
#[derive(Debug, Clone, Serialize)]
pub struct AckReceipt {
    pub acked_cursor: String,
}

/// Replay-derived dedupe map: `(sender, key) -> entry`.
pub(crate) type DedupeIndex = HashMap<(String, String), DedupeEntry>;

/// Replay-derived inbox state.
pub(crate) type LoadedInboxes = (HashMap<String, Inbox>, DedupeIndex);

/// Replay-derived dedupe index entry.
#[derive(Debug, Clone)]
pub(crate) struct DedupeEntry {
    pub message_id: String,
    pub seq: u64,
    pub to: String,
    pub to_agent_id: String,
}

/// One open inbox with its replay-derived indexes.
#[derive(Debug)]
pub(crate) struct Inbox {
    pub log: LogFile<Envelope>,
    pub records: Vec<Envelope>,
    pub notify: Arc<Notify>,
    pub acked_seq: u64,
    /// Unacknowledged messages (`seq > acked_seq`), including expired ones.
    /// Expiry does not reclaim backlog; ack does.
    pub pending_messages: usize,
    /// Serialized bytes of the unacknowledged messages (one line each).
    pub pending_bytes: u64,
    pub(crate) expiry_log: Option<LogFile<ExpiryRecord>>,
    expiry_path: PathBuf,
    pub expires: HashMap<String, u64>,
    pub(crate) write_failed: bool,
}

/// Serialized size of one inbox line, including its newline separator.
pub(crate) fn envelope_line_bytes(envelope: &Envelope) -> u64 {
    serde_json::to_vec(envelope).map_or(0, |bytes| bytes.len() as u64 + 1)
}

pub(crate) fn inbox_now(root: &Path) -> u64 {
    #[cfg(feature = "mbt-injection")]
    if let Some(now) = crate::faults::now_millis(root) {
        return now;
    }
    let _ = root;
    id::unix_millis()
}

impl Inbox {
    fn inbox_path(root: &Path, agent_id: &str) -> PathBuf {
        root.join("agents").join(agent_id).join("inbox.jsonl")
    }

    pub(crate) fn open(root: &Path, agent_id: &str) -> Result<Self, BrokerError> {
        let path = Self::inbox_path(root, agent_id);
        let (mut log, report) = LogFile::open(&path).map_err(BrokerError::from)?;
        let truncated_bytes = report.truncated_bytes;
        let last_valid_offset = report.last_valid_offset;
        let mut records = report.records;

        if truncated_bytes > 0 {
            let body = Body::new(
                "application/json",
                json!({
                    "event": "recovery",
                    "truncated_bytes": truncated_bytes,
                    "last_valid_offset": last_valid_offset,
                    "at": id::now_rfc3339(),
                })
                .to_string(),
            );
            let mut recovery = Envelope::new(DAEMON_SENDER, agent_id, "recovery", body)
                .map_err(|error| BrokerError::internal(error.to_string()))?;
            log.append(&mut recovery)?;
            records.push(recovery);
        }

        let expiry_path = path.with_file_name("expiry.jsonl");
        let (expiry_log, expires) = if expiry_path.exists() {
            let (expiry_log, report) =
                LogFile::<ExpiryRecord>::open(&expiry_path).map_err(BrokerError::from)?;
            let mut expires = HashMap::new();
            for record in report.records {
                expires.insert(record.message_id, record.expires_at_ms);
            }
            (Some(expiry_log), expires)
        } else {
            (None, HashMap::new())
        };

        let pending_messages = records.len();
        let pending_bytes = records.iter().map(envelope_line_bytes).sum();
        Ok(Self {
            log,
            records,
            notify: Arc::new(Notify::new()),
            acked_seq: 0,
            pending_messages,
            pending_bytes,
            expiry_log,
            expiry_path,
            expires,
            write_failed: false,
        })
    }

    fn is_expired(&self, message_id: &str, now_ms: u64) -> bool {
        self.expires
            .get(message_id)
            .is_some_and(|expires_at| *expires_at <= now_ms)
    }

    /// Commits companion TTL metadata before the corresponding inbox batch.
    pub(crate) fn append_expiry_batch(
        &mut self,
        entries: &[(String, u64)],
    ) -> Result<Vec<AppendReceipt>, BrokerError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        if self.expiry_log.is_none() {
            let (log, report) =
                LogFile::<ExpiryRecord>::open(&self.expiry_path).map_err(BrokerError::from)?;
            if report.was_truncated() {
                eprintln!(
                    "coop daemon: repaired {} bytes of torn expiry tail in {}",
                    report.truncated_bytes,
                    self.expiry_path.display()
                );
            }
            self.expiry_log = Some(log);
        }
        let mut records: Vec<ExpiryRecord> = entries
            .iter()
            .map(|(message_id, expires_at_ms)| ExpiryRecord {
                seq: 0,
                prev_hash: String::new(),
                message_id: message_id.clone(),
                expires_at_ms: *expires_at_ms,
                ts: id::now_rfc3339(),
            })
            .collect();
        let receipts = self
            .expiry_log
            .as_mut()
            .expect("expiry log was just created")
            .append_batch(&mut records)?;
        for (message_id, expires_at_ms) in entries {
            self.expires.insert(message_id.clone(), *expires_at_ms);
        }
        Ok(receipts)
    }
}

/// Opens one inbox (creating its directory and log if needed).
pub(crate) fn open_inbox(root: &Path, agent_id: &str) -> Result<Inbox, BrokerError> {
    Inbox::open(root, agent_id)
}

/// Loads every existing inbox and rebuilds the dedupe index.
pub(crate) fn load_all_inboxes(root: &Path) -> Result<LoadedInboxes, BrokerError> {
    let directory = root.join("agents");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((HashMap::new(), HashMap::new()))
        }
        Err(error) => return Err(error.into()),
    };

    let mut agent_ids = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() && entry.path().join("inbox.jsonl").is_file() {
            if let Some(name) = entry.file_name().to_str() {
                agent_ids.push(name.to_string());
            }
        }
    }
    agent_ids.sort();

    let mut inboxes = HashMap::new();
    let mut dedupe = HashMap::new();
    for agent_id in agent_ids {
        let inbox = Inbox::open(root, &agent_id)?;
        for envelope in &inbox.records {
            if let Some(key) = &envelope.idempotency_key {
                dedupe
                    .entry((envelope.from.clone(), key.clone()))
                    .or_insert_with(|| DedupeEntry {
                        message_id: envelope.id.clone(),
                        seq: envelope.seq,
                        to: envelope.to.clone(),
                        to_agent_id: agent_id.clone(),
                    });
            }
        }
        inboxes.insert(agent_id, inbox);
    }
    Ok((inboxes, dedupe))
}

/// Validates a channel name before it is used as a file stem.
pub fn validate_channel_name(name: &str) -> Result<(), BrokerError> {
    if name.is_empty() || name.len() > 128 {
        return Err(BrokerError::invalid("channel name must be 1..=128 bytes"));
    }
    if name.starts_with('.') || name.contains("..") {
        return Err(BrokerError::invalid(
            "channel name must not contain path components",
        ));
    }
    if !name.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '#' | '-' | '_' | '.' | ':')
    }) {
        return Err(BrokerError::invalid(
            "channel name may contain only ASCII letters, digits, '#', '-', '_', '.', and ':'",
        ));
    }
    Ok(())
}

/// Parses an opaque cursor into a sequence number.
pub fn parse_cursor(cursor: &str) -> Result<u64, BrokerError> {
    let raw = cursor.strip_prefix("seq:").unwrap_or(cursor);
    raw.parse::<u64>()
        .map_err(|_| BrokerError::invalid(format!("invalid cursor {cursor:?}")))
}

impl DaemonState {
    /// Read-only access to the actual committed backlog counters for MBT ports.
    #[cfg(feature = "mbt-injection")]
    #[doc(hidden)]
    pub async fn debug_inbox_backlog(&self, agent_id: &str) -> Result<(usize, u64), BrokerError> {
        let handle = self
            .inboxes
            .get(agent_id)
            .ok_or_else(|| BrokerError::internal("inbox not loaded"))?;
        let inbox = handle.state.lock().await;
        Ok((inbox.pending_messages, inbox.pending_bytes))
    }

    /// Ensures one shared inbox writer exists, creating it off the async worker.
    pub(crate) async fn ensure_inbox(&self, agent_id: &str) -> Result<(), BrokerError> {
        self.inboxes.ensure(agent_id).await.map(|_| ())
    }

    /// Appends one message to the recipient's inbox and fsyncs before
    /// returning. Unknown recipients fail with `NOT_FOUND`.
    pub async fn send_message(
        &self,
        sender: &AgentIdentity,
        request: SendRequest,
    ) -> Result<SendReceipt, BrokerError> {
        if request.kind.is_empty() {
            return Err(BrokerError::invalid("message kind must not be empty"));
        }
        if request.body.mime.is_empty() {
            return Err(BrokerError::invalid("body.mime must not be empty"));
        }
        if request.reply_to.as_deref().is_some_and(str::is_empty) {
            return Err(BrokerError::invalid("reply_to must not be empty"));
        }
        if request
            .idempotency_key
            .as_deref()
            .is_some_and(str::is_empty)
        {
            return Err(BrokerError::invalid("idempotency_key must not be empty"));
        }

        let recipient = {
            let registry = self.registry.read().await;
            registry.resolve(&request.to).ok_or_else(|| {
                BrokerError::application(
                    ErrorCode::NotFound,
                    format!("unknown recipient {:?}", request.to),
                )
            })?
        };
        self.inboxes.send(sender, &recipient, request).await
    }

    /// Long-polls the agent's inbox from `cursor` (or the acknowledged
    /// cursor when omitted) without advancing any cursor.
    pub async fn wait_message(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        timeout_duration: Duration,
    ) -> Result<WaitBatch, BrokerError> {
        let explicit_start = cursor.map(parse_cursor).transpose()?;
        let deadline = Instant::now() + timeout_duration;
        let batch_limit = self.config.wait_batch_limit.max(1);

        let mut start = 0u64;
        let mut initialized = false;
        let handle = self.inboxes.get(agent_id).ok_or_else(|| {
            BrokerError::internal(format!("no inbox loaded for agent {agent_id}"))
        })?;
        loop {
            let (messages, next_seq, acked_seq, notified) = {
                let inbox = handle.state.lock().await;
                if !initialized {
                    start = explicit_start.unwrap_or(inbox.acked_seq);
                    initialized = true;
                }

                let now_ms = inbox_now(&self.root);
                let mut messages = Vec::new();
                let mut scanned_seq = start;
                let first = inbox.records.partition_point(|record| record.seq <= start);
                for record in &inbox.records[first..] {
                    if messages.len() >= batch_limit {
                        break;
                    }
                    // Expired records are still durable backlog until ack.
                    // Expose the scanned prefix so an expired-only inbox can
                    // be acknowledged without skipping an unreturned message.
                    scanned_seq = record.seq;
                    if inbox.is_expired(&record.id, now_ms) {
                        continue;
                    }
                    messages.push(record.clone());
                }
                let next_seq = scanned_seq;
                let notified = Arc::clone(&inbox.notify).notified_owned();
                (messages, next_seq, inbox.acked_seq, notified)
            };

            if !messages.is_empty() || next_seq > start {
                return Ok(WaitBatch {
                    messages,
                    next_cursor: next_seq.to_string(),
                    cursor: start.to_string(),
                    acked_cursor: acked_seq.to_string(),
                    timed_out: false,
                });
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(WaitBatch {
                    messages: Vec::new(),
                    next_cursor: start.to_string(),
                    cursor: start.to_string(),
                    acked_cursor: acked_seq.to_string(),
                    timed_out: true,
                });
            }
            match timeout(remaining, notified).await {
                Ok(()) => continue,
                Err(_) => {
                    return Ok(WaitBatch {
                        messages: Vec::new(),
                        next_cursor: start.to_string(),
                        cursor: start.to_string(),
                        acked_cursor: acked_seq.to_string(),
                        timed_out: true,
                    })
                }
            }
        }
    }

    /// Advances the agent's acknowledged cursor through `cursor`.
    pub async fn ack(&self, agent_id: &str, cursor: &str) -> Result<AckReceipt, BrokerError> {
        let seq = parse_cursor(cursor)?;
        let handle = self.inboxes.get(agent_id).ok_or_else(|| {
            BrokerError::internal(format!("no inbox loaded for agent {agent_id}"))
        })?;
        let mut inbox = handle.state.lock().await;
        let head = inbox.log.seq_head();
        if seq > head {
            return Err(BrokerError::invalid(format!(
                "cursor {seq} is beyond the inbox head {head}"
            )));
        }
        if seq > inbox.acked_seq {
            let previous = inbox.acked_seq;
            let first = inbox
                .records
                .partition_point(|record| record.seq <= previous);
            let last = inbox.records.partition_point(|record| record.seq <= seq);
            let released_messages = last - first;
            let released_bytes: u64 = inbox.records[first..last]
                .iter()
                .map(envelope_line_bytes)
                .sum();
            inbox.acked_seq = seq;
            inbox.pending_messages = inbox.pending_messages.saturating_sub(released_messages);
            inbox.pending_bytes = inbox.pending_bytes.saturating_sub(released_bytes);
        }
        Ok(AckReceipt {
            acked_cursor: inbox.acked_seq.to_string(),
        })
    }

    /// Appends a daemon-generated event to a channel log. This is the hook
    /// later milestones reuse for channel fan-out and presence supervision.
    pub async fn append_channel_event(
        &self,
        channel: &str,
        from: &str,
        kind: &str,
        body: Body,
    ) -> Result<AppendReceipt, BrokerError> {
        validate_channel_name(channel)?;
        let mut logs = self.channel_logs.lock().await;
        if !logs.contains_key(channel) {
            let log = crate::channels::open_channel_rotating(
                &self.root,
                channel,
                self.config.log_rotation_bytes,
            )?;
            logs.insert(channel.to_string(), log);
        }
        let log = logs
            .get_mut(channel)
            .expect("channel log is present after insertion");
        let mut envelope = Envelope::new(from, channel, kind, body)
            .map_err(|error| BrokerError::internal(error.to_string()))?;
        let receipt = log.log.append(&mut envelope)?;
        log.records.push(envelope);
        Arc::clone(&log.notify).notify_waiters();
        Ok(receipt)
    }
}
