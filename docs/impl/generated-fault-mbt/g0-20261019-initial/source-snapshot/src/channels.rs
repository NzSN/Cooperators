//! Append-only channels and replayable readers (design §3/§5/§9).
//!
//! A channel is one `LogFile<Envelope>` at `channels/<name>.jsonl`. Each
//! opened channel also keeps a replay-derived record cache, a publish dedupe
//! map keyed by `(sender, idempotency_key)`, and a [`Notify`] used by
//! `read_channel` long-polls.
//!
//! Cursors address `(file, seq)` as a JSON object:
//! `{"file":"channels/build.jsonl","seq":17}`. The string forms `"17"`,
//! `"seq:17"`, and `"channels/build.jsonl#17"` are also accepted. Rotation is
//! not implemented yet; a cursor naming any other file is rejected rather
//! than silently repositioned.

use std::collections::HashMap;
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
use crate::registry::AgentIdentity;
use crate::server::DaemonState;
use crate::storage::LogFile;

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
pub(crate) fn open_channel(
    root: &std::path::Path,
    channel: &str,
) -> Result<ChannelLog, BrokerError> {
    validate_channel_name(channel)?;
    let path = root.join("channels").join(format!("{channel}.jsonl"));
    let (mut log, report) = LogFile::open(&path).map_err(BrokerError::from)?;
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
        log.append(&mut recovery)?;
        records.push(recovery);
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
        validate_channel_name(channel)?;
        if body.mime.is_empty() {
            return Err(BrokerError::invalid("body.mime must not be empty"));
        }
        if idempotency_key.as_deref().is_some_and(str::is_empty) {
            return Err(BrokerError::invalid("idempotency_key must not be empty"));
        }

        let _send_guard = self.send_lock.lock().await;
        let mut logs = self.channel_logs.lock().await;
        if !logs.contains_key(channel) {
            logs.insert(channel.to_string(), open_channel(&self.root, channel)?);
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

        Ok(PublishReceipt {
            channel: channel.to_string(),
            message_id: envelope.id,
            seq: receipt.seq,
            ts: envelope.ts,
            duplicate: false,
            idempotency_key: envelope.idempotency_key,
            cursor: ChannelCursor::new(channel, receipt.seq),
        })
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
                    if !self
                        .root
                        .join("channels")
                        .join(format!("{channel}.jsonl"))
                        .exists()
                    {
                        return Err(BrokerError::application(
                            ErrorCode::NotFound,
                            format!("unknown channel {channel:?}"),
                        ));
                    }
                    logs.insert(channel.to_string(), open_channel(&self.root, channel)?);
                }
                let log = logs
                    .get_mut(channel)
                    .expect("channel log is present after insertion");
                let messages = log
                    .records
                    .iter()
                    .filter(|record| record.seq > start.seq)
                    .take(limit)
                    .cloned()
                    .collect::<Vec<_>>();
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
