//! The v1 message envelope (design §3).
//!
//! Every persisted record carries a `seq` (assigned by the per-log sequencer)
//! and a `prev_hash` (the digest of the previous record in the same log).
//! `reply_to` and `idempotency_key` are optional; they serialize as `null`
//! when absent so the field set stays stable on the wire.

use serde::{Deserialize, Serialize};

use crate::id;
use crate::storage::{ChainRecord, GENESIS_HASH};

/// Envelope schema version implemented by this crate.
pub const ENVELOPE_VERSION: u16 = 1;

/// Message body. `text` is normative for v1; MIME describes how to render it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Body {
    pub mime: String,
    pub text: String,
}

impl Body {
    /// Constructs a body from a MIME type and text payload.
    pub fn new(mime: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            mime: mime.into(),
            text: text.into(),
        }
    }
}

/// A single broker message record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub v: u16,
    pub id: String,
    pub seq: u64,
    pub ts: String,
    #[serde(rename = "from")]
    pub from: String,
    pub to: String,
    pub kind: String,
    #[serde(default)]
    pub reply_to: Option<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    pub body: Body,
    pub prev_hash: String,
}

impl Envelope {
    /// Builds an unsequenced envelope with a fresh ULID and UTC timestamp.
    ///
    /// `seq` starts at 0 and `prev_hash` at the genesis hash; both are
    /// overwritten by [`crate::storage::LogFile::append`].
    pub fn new(
        from: impl Into<String>,
        to: impl Into<String>,
        kind: impl Into<String>,
        body: Body,
    ) -> Result<Self, EnvelopeError> {
        Ok(Self {
            v: ENVELOPE_VERSION,
            id: id::new_ulid()?,
            seq: 0,
            ts: id::now_rfc3339(),
            from: from.into(),
            to: to.into(),
            kind: kind.into(),
            reply_to: None,
            idempotency_key: None,
            body,
            prev_hash: GENESIS_HASH.to_string(),
        })
    }

    /// Sets the `reply_to` correlation ID.
    pub fn with_reply_to(mut self, reply_to: impl Into<String>) -> Self {
        self.reply_to = Some(reply_to.into());
        self
    }

    /// Sets the dedupe key.
    pub fn with_idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }

    /// Validates an envelope that is about to be persisted or transmitted.
    ///
    /// Draft envelopes returned by [`Envelope::new`] have `seq == 0` and are
    /// only valid after the log sequencer has assigned a sequence number.
    pub fn validate(&self) -> Result<(), EnvelopeError> {
        if self.v != ENVELOPE_VERSION {
            return Err(EnvelopeError::InvalidField("v"));
        }
        if !id::is_ulid(&self.id) {
            return Err(EnvelopeError::InvalidField("id"));
        }
        if self.seq == 0 {
            return Err(EnvelopeError::InvalidField("seq"));
        }
        if self.ts.is_empty() {
            return Err(EnvelopeError::InvalidField("ts"));
        }
        if self.from.is_empty() {
            return Err(EnvelopeError::InvalidField("from"));
        }
        if self.to.is_empty() {
            return Err(EnvelopeError::InvalidField("to"));
        }
        if self.kind.is_empty() {
            return Err(EnvelopeError::InvalidField("kind"));
        }
        if self.body.mime.is_empty() {
            return Err(EnvelopeError::InvalidField("body.mime"));
        }
        if self.reply_to.as_deref().is_some_and(str::is_empty) {
            return Err(EnvelopeError::InvalidField("reply_to"));
        }
        if self.idempotency_key.as_deref().is_some_and(str::is_empty) {
            return Err(EnvelopeError::InvalidField("idempotency_key"));
        }
        if !crate::storage::is_chain_hash(&self.prev_hash) {
            return Err(EnvelopeError::InvalidField("prev_hash"));
        }
        Ok(())
    }
}

impl ChainRecord for Envelope {
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

/// Errors produced while constructing or validating envelopes.
#[derive(Debug)]
pub enum EnvelopeError {
    /// A required field is missing or malformed.
    InvalidField(&'static str),
    /// ID generation or clock access failed.
    Id(std::io::Error),
}

impl std::fmt::Display for EnvelopeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidField(field) => write!(formatter, "invalid envelope field: {field}"),
            Self::Id(error) => write!(formatter, "failed to generate envelope id: {error}"),
        }
    }
}

impl std::error::Error for EnvelopeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Id(error) => Some(error),
            Self::InvalidField(_) => None,
        }
    }
}

impl From<std::io::Error> for EnvelopeError {
    fn from(error: std::io::Error) -> Self {
        Self::Id(error)
    }
}
