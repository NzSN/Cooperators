//! Append-only channels and replayable readers (design §3/§5/§9).
//!
//! A channel is one `LogFile<Envelope>` at `channels/<name>.jsonl`. Each
//! opened channel also keeps a replay-derived record cache, a publish dedupe
//! map keyed by `(sender, idempotency_key)`, and a [`Notify`] used by
//! `read_channel` long-polls.
//!
//! Cursors address `(file, seq)` as a JSON object:
//! `{"file":"channels/build.jsonl","seq":17}`. The string forms `"17"`,
//! `"seq:17"`, and `"channels/build.jsonl#17"` are also accepted. Rotation
//! preserves this logical stream name across physical segments. Sequence
//! numbers remain global, so saved cursors continue through rotation.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Notify;
use tokio::time::{timeout, Instant};

use crate::envelope::{Body, Envelope};
use crate::error::{BrokerError, ErrorCode};
use crate::id;
use crate::messaging::{parse_cursor, validate_channel_name, DAEMON_SENDER};
use crate::operations::{DomainTicket, OperationOutcome, OperationStore};
use crate::registry::AgentIdentity;
use crate::server::DaemonState;
use crate::storage::{segment_paths, LogFile};

/// Maximum records one `read_channel` call returns.
pub const MAX_READ_LIMIT: usize = 1000;
/// Maximum `wait_s` accepted by `read_channel`.
pub const MAX_READ_WAIT_SECS: u64 = 300;

/// An opened channel and its replay-derived indexes.
#[derive(Debug)]
pub(crate) struct ChannelLog {
    pub log: LogFile<Envelope>,
    pub records: Vec<Envelope>,
    pub notify: Arc<Notify>,
    pub dedupe: HashMap<(String, String), ChannelPublishEntry>,
}

/// Replay-derived idempotent publish result.
#[derive(Debug, Clone)]
pub(crate) struct ChannelPublishEntry {
    pub message_id: String,
    pub seq: u64,
    pub ts: String,
}

/// `(file, seq)` cursor used by channel reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelCursor {
    /// Log path relative to the bus root, for example
    /// `channels/build.jsonl`.
    pub file: String,
    /// Highest consumed sequence number in that file.
    pub seq: u64,
}

impl ChannelCursor {
    /// Builds a cursor for a channel and sequence number.
    pub fn new(channel: &str, seq: u64) -> Self {
        Self {
            file: format!("channels/{channel}.jsonl"),
            seq,
        }
    }
}

/// Durable acknowledgement returned by `publish`.
#[derive(Debug, Clone, Serialize)]
pub struct PublishReceipt {
    pub channel: String,
    pub message_id: String,
    pub seq: u64,
    pub ts: String,
    /// True when an idempotency key resolved to an existing record.
    pub duplicate: bool,
    pub idempotency_key: Option<String>,
    pub cursor: ChannelCursor,
}

/// Batch returned by `read_channel`.
#[derive(Debug, Clone, Serialize)]
pub struct ChannelReadBatch {
    pub channel: String,
    pub file: String,
    pub messages: Vec<Envelope>,
    pub cursor: ChannelCursor,
    pub next_cursor: ChannelCursor,
    pub head: ChannelCursor,
    /// True when the wait elapsed without a record newer than the cursor.
    pub timed_out: bool,
}

/// Opens (creating if needed) one channel and rebuilds its indexes.
pub(crate) fn open_channel_rotating(
    root: &std::path::Path,
    channel: &str,
    rotation_bytes: u64,
) -> Result<ChannelLog, BrokerError> {
    open_channel_operation(root, channel, rotation_bytes, None)
}

fn open_channel_operation(
    root: &Path,
    channel: &str,
    rotation_bytes: u64,
    operation: Option<&DomainTicket>,
) -> Result<ChannelLog, BrokerError> {
    validate_channel_name(channel)?;
    let path = root.join("channels").join(format!("{channel}.jsonl"));
    let (mut log, report) =
        LogFile::open_rotating(&path, rotation_bytes).map_err(BrokerError::from)?;
    let truncated = report.was_truncated();
    let truncated_bytes = report.truncated_bytes;
    let last_valid_offset = report.last_valid_offset;
    let mut records = report.records;
    if truncated {
        let body = Body::new(
            "application/json",
            json!({
                "event": "recovery",
                "channel": channel,
                "truncated_bytes": truncated_bytes,
                "last_valid_offset": last_valid_offset,
                "at": id::now_rfc3339(),
            })
            .to_string(),
        );
        let mut recovery = Envelope::new(DAEMON_SENDER, channel, "recovery", body)
            .map_err(|error| BrokerError::internal(error.to_string()))?;
        let maintenance = operation.map(DomainTicket::begin_maintenance);
        log.append(&mut recovery)?;
        records.push(recovery);
        if let Some(maintenance) = maintenance {
            maintenance.complete();
        }
    }

    let mut dedupe = HashMap::new();
    for envelope in &records {
        if let Some(key) = &envelope.idempotency_key {
            dedupe
                .entry((envelope.from.clone(), key.clone()))
                .or_insert_with(|| ChannelPublishEntry {
                    message_id: envelope.id.clone(),
                    seq: envelope.seq,
                    ts: envelope.ts.clone(),
                });
        }
    }

    Ok(ChannelLog {
        log,
        records,
        notify: Arc::new(Notify::new()),
        dedupe,
    })
}

/// Loads every logical channel, including streams sealed during a crash.
pub(crate) fn load_all_channels(
    root: &Path,
    rotation_bytes: u64,
) -> Result<HashMap<String, ChannelLog>, BrokerError> {
    let directory = root.join("channels");
    let mut names = BTreeSet::new();
    match fs::read_dir(&directory) {
        Ok(entries) => {
            for entry in entries {
                let path = entry?.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
                    if let Some(name) = path.file_stem().and_then(|name| name.to_str()) {
                        names.insert(name.to_string());
                    }
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    match fs::read_dir(directory.join(".segments")) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let logical = directory.join(entry.file_name());
                if logical.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
                    && !segment_paths(&logical)?.is_empty()
                {
                    if let Some(name) = logical.file_stem().and_then(|name| name.to_str()) {
                        names.insert(name.to_string());
                    }
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut channels = HashMap::new();
    for name in names {
        if validate_channel_name(&name).is_err() {
            continue;
        }
        channels.insert(
            name.clone(),
            open_channel_rotating(root, &name, rotation_bytes)?,
        );
    }
    Ok(channels)
}

pub(crate) fn recover_channel_operations(
    logs: &HashMap<String, ChannelLog>,
    operations: &OperationStore,
) -> Result<(), BrokerError> {
    for (name, log) in logs {
        for envelope in &log.records {
            if envelope.operation.is_some() {
                return Err(BrokerError::internal(
                    "inbox operation proof found in channel source",
                ));
            }
            let Some(proof) = &envelope.domain_operation else {
                continue;
            };
            // Historical envelopes identify the sender by name. The embedded
            // proof retains the authenticated stable scope, without requiring
            // a still-live registry identity during authoritative replay.
            proof.verify("publish", &format!("channel:{name}"))?;
            let result = serde_json::to_value(PublishReceipt {
                channel: name.clone(),
                message_id: envelope.id.clone(),
                seq: envelope.seq,
                ts: envelope.ts.clone(),
                duplicate: false,
                idempotency_key: None,
                cursor: ChannelCursor::new(name, envelope.seq),
            })
            .map_err(|error| BrokerError::internal(format!("channel receipt: {error}")))?;
            if envelope.validate().is_err()
                || envelope.kind != "publish"
                || envelope.to != *name
                || envelope.idempotency_key.is_some()
                || proof.completion.is_some()
                || proof.context.input != json!({"channel":name,"body":envelope.body})
                || !matches!(&proof.outcome, OperationOutcome::Success(value) if value == &result)
            {
                return Err(BrokerError::internal(
                    "invalid authoritative channel operation proof",
                ));
            }
            operations.recover_domain(proof, proof.outcome.clone())?;
        }
    }
    Ok(())
}

/// Parses a cursor supplied as JSON or as a compact string.
pub fn parse_channel_cursor(value: &Value, channel: &str) -> Result<ChannelCursor, BrokerError> {
    let expected = ChannelCursor::new(channel, 0).file;
    let mut cursor = match value {
        Value::Object(object) => {
            let seq = object
                .get("seq")
                .and_then(Value::as_u64)
                .ok_or_else(|| BrokerError::invalid("cursor.seq must be a non-negative integer"))?;
            let file = match object.get("file").and_then(Value::as_str) {
                Some(file) if !file.is_empty() => file.to_string(),
                _ => expected.clone(),
            };
            ChannelCursor { file, seq }
        }
        Value::String(text) => {
            let (file, seq) = match text.rsplit_once('#') {
                Some((file, seq)) => (Some(file.to_string()), seq),
                None => (None, text.as_str()),
            };
            ChannelCursor {
                file: file.unwrap_or(expected.clone()),
                seq: parse_cursor(seq)?,
            }
        }
        Value::Number(number) => ChannelCursor {
            file: expected.clone(),
            seq: number
                .as_u64()
                .ok_or_else(|| BrokerError::invalid("cursor must be a non-negative integer"))?,
        },
        _ => {
            return Err(BrokerError::invalid(
                "cursor must be {file, seq}, a sequence number, or \"<file>#<seq>\"",
            ))
        }
    };
    let bare_name = format!("{channel}.jsonl");
    if cursor.file != expected && cursor.file != bare_name {
        return Err(BrokerError::application(
            ErrorCode::NotFound,
            format!(
                "cursor file {:?} is not the current log for channel {channel:?}",
                cursor.file
            ),
        ));
    }
    cursor.file = expected;
    Ok(cursor)
}

impl DaemonState {
    /// Appends one published body to a channel and fsyncs before returning.
    ///
    /// Idempotency keys are scoped to `(sender, key)`. A duplicate returns
    /// the original message id and sequence without a second append.
    pub async fn publish(
        &self,
        sender: &AgentIdentity,
        channel: &str,
        body: Body,
        idempotency_key: Option<String>,
    ) -> Result<PublishReceipt, BrokerError> {
        self.publish_impl(sender, channel, body, idempotency_key, None)
            .await
    }

    /// Caller holds `send_lock` for the independent operation task.
    pub(crate) async fn publish_operation(
        &self,
        sender: &AgentIdentity,
        channel: &str,
        body: Body,
        operation: &DomainTicket,
    ) -> Result<PublishReceipt, BrokerError> {
        self.publish_impl(sender, channel, body, None, Some(operation))
            .await
    }

    async fn publish_impl(
        &self,
        sender: &AgentIdentity,
        channel: &str,
        body: Body,
        idempotency_key: Option<String>,
        operation: Option<&DomainTicket>,
    ) -> Result<PublishReceipt, BrokerError> {
        validate_channel_name(channel)?;
        if body.mime.is_empty() {
            return Err(BrokerError::invalid("body.mime must not be empty"));
        }
        if idempotency_key.as_deref().is_some_and(str::is_empty) {
            return Err(BrokerError::invalid("idempotency_key must not be empty"));
        }

        let _send_guard = if operation.is_none() {
            Some(self.send_lock.lock().await)
        } else {
            None
        };
        let mut logs = self.channel_logs.lock().await;
        if !logs.contains_key(channel) {
            logs.insert(
                channel.to_string(),
                open_channel_operation(
                    &self.root,
                    channel,
                    self.config().log_rotation_bytes,
                    operation,
                )?,
            );
        }
        let log = logs
            .get_mut(channel)
            .expect("channel log is present after insertion");

        if let Some(key) = idempotency_key.as_deref() {
            if let Some(existing) = log.dedupe.get(&(sender.name.clone(), key.to_string())) {
                return Ok(PublishReceipt {
                    channel: channel.to_string(),
                    message_id: existing.message_id.clone(),
                    seq: existing.seq,
                    ts: existing.ts.clone(),
                    duplicate: true,
                    idempotency_key: Some(key.to_string()),
                    cursor: ChannelCursor::new(channel, existing.seq),
                });
            }
        }

        let mut envelope = Envelope::new(sender.name.clone(), channel, "publish", body)
            .map_err(|error| BrokerError::internal(error.to_string()))?;
        envelope.idempotency_key = idempotency_key.clone();
        let result = PublishReceipt {
            channel: channel.to_string(),
            message_id: envelope.id.clone(),
            seq: log.log.seq_head() + 1,
            ts: envelope.ts.clone(),
            duplicate: false,
            idempotency_key: idempotency_key.clone(),
            cursor: ChannelCursor::new(channel, log.log.seq_head() + 1),
        };
        if let Some(operation) = operation {
            let value = serde_json::to_value(&result)
                .map_err(|error| BrokerError::internal(format!("channel receipt: {error}")))?;
            envelope.domain_operation = Some(operation.proof(OperationOutcome::Success(value))?);
            operation.mark_effect_attempt();
        }
        let receipt = log.log.append(&mut envelope)?;
        log.records.push(envelope.clone());
        if let Some(key) = envelope.idempotency_key.clone() {
            log.dedupe.insert(
                (sender.name.clone(), key),
                ChannelPublishEntry {
                    message_id: envelope.id.clone(),
                    seq: receipt.seq,
                    ts: envelope.ts.clone(),
                },
            );
        }
        let notify = Arc::clone(&log.notify);
        drop(logs);
        notify.notify_waiters();

        Ok(result)
    }

    /// Reads channel records strictly after `cursor` without advancing server
    /// state. A non-zero `wait` long-polls for the first matching record.
    pub async fn read_channel(
        &self,
        channel: &str,
        cursor: Option<ChannelCursor>,
        limit: usize,
        wait: Duration,
    ) -> Result<ChannelReadBatch, BrokerError> {
        self.read_channel_limited(channel, cursor, limit, wait, None)
            .await
    }

    pub(crate) async fn read_channel_limited(
        &self,
        channel: &str,
        cursor: Option<ChannelCursor>,
        limit: usize,
        wait: Duration,
        byte_limit: Option<usize>,
    ) -> Result<ChannelReadBatch, BrokerError> {
        validate_channel_name(channel)?;
        if limit == 0 || limit > MAX_READ_LIMIT {
            return Err(BrokerError::invalid(format!(
                "limit must be 1..={MAX_READ_LIMIT}"
            )));
        }
        let start = cursor.unwrap_or_else(|| ChannelCursor::new(channel, 0));
        let deadline = Instant::now() + wait;

        loop {
            let (messages, head, notified) = {
                let mut logs = self.channel_logs.lock().await;
                if !logs.contains_key(channel) {
                    if segment_paths(self.root.join("channels").join(format!("{channel}.jsonl")))?
                        .is_empty()
                    {
                        return Err(BrokerError::application(
                            ErrorCode::NotFound,
                            format!("unknown channel {channel:?}"),
                        ));
                    }
                    logs.insert(
                        channel.to_string(),
                        open_channel_rotating(
                            &self.root,
                            channel,
                            self.config().log_rotation_bytes,
                        )?,
                    );
                }
                let log = logs
                    .get_mut(channel)
                    .expect("channel log is present after insertion");
                if byte_limit.is_some() && start.seq > log.log.seq_head() {
                    return Err(BrokerError::invalid(
                        "channel cursor exceeds committed head",
                    ));
                }
                let mut budget =
                    byte_limit.map(|bytes| crate::network::page::PageBudget::new(limit, bytes));
                let first = log
                    .records
                    .partition_point(|record| record.seq <= start.seq);
                let mut messages = Vec::new();
                for record in log.records[first..].iter().take(limit) {
                    if let Some(budget) = &mut budget {
                        if !budget.admit(record)? {
                            break;
                        }
                    }
                    messages.push(record.clone());
                }
                let head = ChannelCursor::new(channel, log.log.seq_head());
                let notified = Arc::clone(&log.notify).notified_owned();
                (messages, head, notified)
            };

            if !messages.is_empty() {
                let next_seq = messages.last().map_or(start.seq, |record| record.seq);
                return Ok(ChannelReadBatch {
                    channel: channel.to_string(),
                    file: start.file.clone(),
                    messages,
                    cursor: start.clone(),
                    next_cursor: ChannelCursor::new(channel, next_seq),
                    head,
                    timed_out: false,
                });
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(ChannelReadBatch {
                    channel: channel.to_string(),
                    file: start.file.clone(),
                    messages: Vec::new(),
                    cursor: start.clone(),
                    next_cursor: start,
                    head,
                    timed_out: true,
                });
            }
            match timeout(remaining, notified).await {
                Ok(()) => continue,
                Err(_) => {
                    return Ok(ChannelReadBatch {
                        channel: channel.to_string(),
                        file: start.file.clone(),
                        messages: Vec::new(),
                        cursor: start.clone(),
                        next_cursor: start,
                        head,
                        timed_out: true,
                    })
                }
            }
        }
    }
}
