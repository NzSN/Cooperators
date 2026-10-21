//! Local durable admission, exact-session binding, and resolution evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::arrivals::{ArrivalNotice, ArrivalReference};
use crate::error::{BrokerError, ErrorCode};
use crate::id;
use crate::storage::{canonical_json, hash_record, sha256_hex, ChainRecord, LogFile, GENESIS_HASH};

use super::{Binding, Readiness, SessionState, Submission, Wake};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BridgeLimits {
    pub max_pending_notices: usize,
    pub max_pending_bytes: u64,
    pub wake_notices: usize,
    pub wake_bytes: u64,
    pub coalesce_ms: u64,
    pub observation_timeout_ms: u64,
    pub max_attempts: u32,
    pub retry_min_ms: u64,
    pub retry_max_ms: u64,
}

impl Default for BridgeLimits {
    fn default() -> Self {
        Self {
            max_pending_notices: 10_000,
            max_pending_bytes: 64 * 1024 * 1024,
            wake_notices: 64,
            wake_bytes: 256 * 1024,
            coalesce_ms: 100,
            observation_timeout_ms: 30_000,
            max_attempts: 5,
            retry_min_ms: 1_000,
            retry_max_ms: 60_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiveReceipt {
    pub cursor: String,
    pub received_seq: u64,
    pub added_notices: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum Disposition {
    DocumentRead,
    DocumentSuperseded { newer_version: u64, policy: String },
    QueueDrained,
    WorkAdopted { task_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionDecision {
    pub delivery_id: String,
    #[serde(flatten)]
    pub disposition: Disposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveArrivalsRequest {
    pub wake_id: String,
    pub generation: u64,
    pub decisions: Vec<ResolutionDecision>,
    #[serde(default = "default_continuation")]
    pub continuation: bool,
}

fn default_continuation() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionReceipt {
    pub wake_id: String,
    pub generation: u64,
    pub resolved_delivery_ids: Vec<String>,
    pub continuation: bool,
    pub pending_notices: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmActivationRequest {
    pub binding_id: String,
    pub generation: u64,
    pub nonce: String,
    #[serde(default)]
    pub wake_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationReceipt {
    pub binding_id: String,
    pub generation: u64,
    pub wake_id: Option<String>,
    pub observed_at_ms: u64,
    pub readiness: Readiness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "purpose", rename_all = "snake_case")]
pub enum NoncePurpose {
    Probe,
    Wake { wake_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationChallenge {
    pub binding_id: String,
    pub generation: u64,
    pub nonce: String,
    pub expires_at_ms: u64,
    #[serde(flatten)]
    pub purpose: NoncePurpose,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ClaimResult {
    Adopted { task_id: String },
    Empty,
    Unproven,
}

/// Created by the trusted MCP proxy from an authoritative claim response,
/// never accepted as a model-provided queue-empty assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimObservation {
    pub wake_id: String,
    pub generation: u64,
    pub queue: String,
    #[serde(flatten)]
    pub result: ClaimResult,
}

/// The trusted proxy captures this boundary before awaiting the broker. An
/// empty reply started before a newer adoption cannot become drain evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimAttempt {
    pub attempt_id: String,
    pub wake_id: String,
    pub generation: u64,
    pub queue: String,
    pub queue_generation: u64,
    pub adoption_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeStatus {
    pub binding: Binding,
    pub readiness: Readiness,
    pub lifecycle: SessionState,
    pub received_cursor: Option<String>,
    pub received_seq: u64,
    pub pending_notices: usize,
    pub pending_bytes: u64,
    pub active_wake_id: Option<String>,
    pub activation_attempts: u32,
    pub last_successful_probe_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeAction {
    Submit(Wake),
    Reconcile { wake: Wake, turn_id: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WakeState {
    wake: Wake,
    attempts: u32,
    deadline_ms: Option<u64>,
    retry_at_ms: u64,
    paused_at_ms: Option<u64>,
    observed: bool,
    turn_id: Option<String>,
    last_submission: Option<Submission>,
    /// True after crash recovery or an ambiguous external submission.
    needs_reconcile: bool,
    /// Last trusted empty response plus the received queue generation it saw.
    empty_proofs: BTreeMap<String, u64>,
    adopted_tasks: BTreeMap<String, BTreeSet<String>>,
    adoption_epoch: u64,
    claim_attempts: BTreeMap<String, ClaimAttempt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum BridgeEventRecord {
    Bound {
        binding: Binding,
    },
    Received {
        notices: Vec<ArrivalNotice>,
        cursor: String,
        at_ms: u64,
    },
    WakeCreated {
        wake: Wake,
    },
    AttemptStarted {
        wake_id: String,
        at_ms: u64,
        deadline_ms: u64,
    },
    SubmissionRecorded {
        wake_id: String,
        submission: Submission,
        retry_at_ms: u64,
    },
    Reconciled {
        wake_id: String,
        submission: Submission,
    },
    Lifecycle {
        state: SessionState,
        at_ms: u64,
    },
    ChallengeCreated {
        challenge: ActivationChallenge,
    },
    Confirmed {
        request: ConfirmActivationRequest,
        at_ms: u64,
    },
    ClaimStarted {
        attempt: ClaimAttempt,
    },
    ClaimFinished {
        attempt_id: String,
        result: ClaimResult,
    },
    Resolved {
        fingerprint: String,
        receipt: ResolutionReceipt,
    },
    Attention {
        detail: String,
    },
    RetryEnabled {
        at_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BridgeRecord {
    v: u16,
    seq: u64,
    prev_hash: String,
    #[serde(flatten)]
    event: BridgeEventRecord,
}

impl ChainRecord for BridgeRecord {
    fn seq(&self) -> u64 {
        self.seq
    }
    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }
    fn prev_hash(&self) -> &str {
        &self.prev_hash
    }
    fn set_prev_hash(&mut self, value: String) {
        self.prev_hash = value;
    }
}

#[derive(Debug)]
struct BridgeState {
    binding: Option<Binding>,
    readiness: Readiness,
    lifecycle: SessionState,
    cursor: Option<String>,
    received_seq: u64,
    received_hash: String,
    pending: BTreeMap<u64, ArrivalNotice>,
    pending_bytes: u64,
    queue_generations: BTreeMap<String, u64>,
    pending_since_ms: Option<u64>,
    active: Option<WakeState>,
    resolutions: BTreeMap<String, (String, ResolutionReceipt)>,
    challenges: BTreeMap<String, ActivationChallenge>,
    confirmed: BTreeMap<String, ActivationReceipt>,
    last_probe_ms: Option<u64>,
    error: Option<String>,
}

impl Default for BridgeState {
    fn default() -> Self {
        Self {
            binding: None,
            readiness: Readiness::Configured,
            lifecycle: SessionState::Unknown,
            cursor: None,
            received_seq: 0,
            received_hash: GENESIS_HASH.into(),
            pending: BTreeMap::new(),
            pending_bytes: 0,
            queue_generations: BTreeMap::new(),
            pending_since_ms: None,
            active: None,
            resolutions: BTreeMap::new(),
            challenges: BTreeMap::new(),
            confirmed: BTreeMap::new(),
            last_probe_ms: None,
            error: None,
        }
    }
}

/// Exactly one conforming local owner holds this journal. The broker root is
/// never mutated here; the OS lock inode survives shutdown and restart.
#[derive(Debug)]
pub struct HostBridge {
    directory: PathBuf,
    _owner: File,
    log: LogFile<BridgeRecord>,
    limits: BridgeLimits,
    state: BridgeState,
}

impl HostBridge {
    pub fn open(
        state_base: &Path,
        broker_root: &Path,
        binding: Binding,
        limits: BridgeLimits,
    ) -> Result<Self, BrokerError> {
        validate_binding(&binding)?;
        validate_limits(&limits)?;
        let broker = fs::canonicalize(broker_root)?;
        // Resolve existing ancestors before making anything. A rejected state
        // path must not create a directory under the broker's writer root.
        if canonical_destination(state_base)?.starts_with(&broker) {
            return Err(BrokerError::invalid(
                "host bridge state must be outside the broker root",
            ));
        }
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(state_base)?;
        let base = fs::canonicalize(state_base)?;
        let key = sha256_hex(
            canonical_json(&json!(["host-bridge/v1", binding.bus_id, binding.agent_id])).as_bytes(),
        );
        let directory = base.join(key);
        if directory.starts_with(&broker) {
            return Err(BrokerError::invalid(
                "host bridge state must be outside the broker root",
            ));
        }
        if fs::symlink_metadata(&directory).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            return Err(BrokerError::invalid("host bridge identity directory cannot be a symlink"));
        }
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&directory)?;
        let directory = fs::canonicalize(directory)?;
        if directory.starts_with(&broker) {
            return Err(BrokerError::invalid(
                "host bridge state resolves inside the broker root",
            ));
        }
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join("owner.lock"))?;
        owner.try_lock().map_err(|error| {
            BrokerError::application(
                ErrorCode::Conflict,
                format!("host bridge already owned or unavailable: {error}"),
            )
        })?;
        owner.sync_all()?;
        sync_ancestry(&directory)?;
        let mut state = BridgeState::default();
        let (log, _) =
            LogFile::<BridgeRecord>::open_streaming(directory.join("bridge.jsonl"), |record| {
                if record.v != 1 {
                    return Err(crate::storage::LogError::InvalidRecord(
                        "unknown host bridge version".into(),
                    ));
                }
                state
                    .apply(&record.event)
                    .map_err(|error| crate::storage::LogError::InvalidRecord(error.to_string()))
            })?;
        File::open(&directory)?.sync_all()?;
        if let Some(persisted) = &state.binding {
            if !same_binding(persisted, &binding) {
                return Err(BrokerError::application(ErrorCode::Conflict,
                    "persisted binding differs; explicitly reconcile and rebind the selected session"));
            }
        }
        if let Some(active) = &mut state.active {
            if active.attempts > 0 {
                active.needs_reconcile = true;
            }
            // Outcomes of external claims in flight at process death are
            // uncertain. Require a fresh empty result before declaring drain.
            active.claim_attempts.clear();
            active.empty_proofs.clear();
        }
        // A previous transport's probe cannot verify this connection. Wake
        // retries install their challenge again when a fresh attempt starts.
        state.challenges.clear();
        state.confirmed.clear();
        // Reconnect never inherits a previous transport's readiness proof.
        if !matches!(
            state.readiness,
            Readiness::AttentionRequired | Readiness::Unsupported
        ) {
            state.readiness = Readiness::Configured;
        }
        state.lifecycle = SessionState::Unknown;
        let mut bridge = Self {
            directory,
            _owner: owner,
            log,
            limits,
            state,
        };
        if bridge.state.binding.is_none() {
            bridge.commit(BridgeEventRecord::Bound { binding })?;
        }
        Ok(bridge)
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn binding(&self) -> &Binding {
        self.state
            .binding
            .as_ref()
            .expect("binding committed on open")
    }
    pub fn received_cursor(&self) -> Option<&str> {
        self.state.cursor.as_deref()
    }
    pub fn active_wake(&self) -> Option<&Wake> {
        self.state.active.as_ref().map(|state| &state.wake)
    }

    pub fn status(&self) -> BridgeStatus {
        BridgeStatus {
            binding: self.binding().clone(),
            readiness: self.state.readiness.clone(),
            lifecycle: self.state.lifecycle,
            received_cursor: self.state.cursor.clone(),
            received_seq: self.state.received_seq,
            pending_notices: self.state.pending.len(),
            pending_bytes: self.state.pending_bytes,
            active_wake_id: self.state.active.as_ref().map(|s| s.wake.wake_id.clone()),
            activation_attempts: self.state.active.as_ref().map_or(0, |s| s.attempts),
            last_successful_probe_ms: self.state.last_probe_ms,
            error: self.state.error.clone(),
        }
    }

    pub fn authenticate_binding(&self, bus_id: &str, agent_id: &str) -> Result<(), BrokerError> {
        if self.binding().bus_id != bus_id || self.binding().agent_id != agent_id {
            return Err(BrokerError::application(
                ErrorCode::Unauthenticated,
                "host binding identity mismatch",
            ));
        }
        Ok(())
    }

    /// A receipt becomes eligible for broker ACK only after this append sync.
    /// A full/failed journal returns an error and withholds the ACK.
    pub fn receive(
        &mut self,
        bus_id: &str,
        agent_id: &str,
        notices: Vec<ArrivalNotice>,
        cursor: String,
        now_ms: u64,
    ) -> Result<ReceiveReceipt, BrokerError> {
        self.check_ready()?;
        self.authenticate_binding(bus_id, agent_id)?;
        let cursor_seq = parse_received_cursor(bus_id, agent_id, &cursor)?;
        if notices.is_empty() {
            if cursor_seq != self.state.received_seq
                || self.state.cursor.as_deref().is_some_and(|previous| previous != cursor)
            {
                return Err(BrokerError::invalid(
                    "empty page cannot advance the bridge cursor",
                ));
            }
            return Ok(ReceiveReceipt {
                cursor,
                received_seq: self.state.received_seq,
                added_notices: 0,
            });
        }
        if notices.last().is_some_and(|notice| notice.seq != cursor_seq) {
            return Err(BrokerError::invalid("arrival cursor does not cover the received batch"));
        }
        let mut previous_seq = None;
        let mut previous_hash = None;
        let mut added = Vec::new();
        let mut bytes = 0u64;
        let mut reached_head = self.state.received_seq == 0;
        let mut new_ids = BTreeSet::new();
        for notice in &notices {
            if notice.v != 1 || notice.recipient_agent_id != agent_id {
                return Err(BrokerError::invalid(
                    "arrival notice version or recipient mismatch",
                ));
            }
            if let Some(seq) = previous_seq {
                if notice.seq != seq + 1
                    || previous_hash.as_deref() != Some(notice.prev_hash.as_str())
                {
                    return Err(BrokerError::invalid("noncontiguous arrival batch"));
                }
            }
            let hash = hash_record(notice)?;
            if notice.seq == self.state.received_seq {
                if hash != self.state.received_hash {
                    return Err(BrokerError::invalid(
                        "restored arrival feed conflicts with received head",
                    ));
                }
                reached_head = true;
            }
            if notice.seq > self.state.received_seq {
                let expected = self.state.received_seq + added.len() as u64 + 1;
                if notice.seq != expected
                    || (added.is_empty() && notice.prev_hash != self.state.received_hash)
                {
                    return Err(BrokerError::invalid(
                        "arrival batch skips or replaces the received head",
                    ));
                }
                if !new_ids.insert(notice.delivery_id.clone())
                    || self
                        .state
                        .pending
                        .values()
                        .any(|old| old.delivery_id == notice.delivery_id)
                {
                    return Err(BrokerError::invalid(
                        "duplicate arrival delivery ID at a new sequence",
                    ));
                }
                bytes = bytes.saturating_add(encoded_size(notice)?);
                added.push(notice.clone());
                reached_head = true;
            }
            previous_seq = Some(notice.seq);
            previous_hash = Some(hash);
        }
        if added.is_empty() {
            if !reached_head || self.state.cursor.as_deref() != Some(cursor.as_str()) {
                return Err(BrokerError::invalid(
                    "old arrival batch does not anchor the durable cursor",
                ));
            }
            return Ok(ReceiveReceipt {
                cursor,
                received_seq: self.state.received_seq,
                added_notices: 0,
            });
        }
        if self.state.pending.len().saturating_add(added.len()) > self.limits.max_pending_notices
            || self.state.pending_bytes.saturating_add(bytes) > self.limits.max_pending_bytes
        {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "bridge pending journal is full; broker ACK withheld",
            ));
        }
        let count = added.len();
        self.commit(BridgeEventRecord::Received {
            notices: added,
            cursor: cursor.clone(),
            at_ms: now_ms,
        })?;
        Ok(ReceiveReceipt {
            cursor,
            received_seq: self.state.received_seq,
            added_notices: count,
        })
    }

    /// Changes lifecycle durably so busy/permission time pauses observation
    /// deadlines, while an unknown host still consumes finite retry attempts.
    pub fn set_session_state(
        &mut self,
        state: SessionState,
        now_ms: u64,
    ) -> Result<(), BrokerError> {
        self.check_ready()?;
        if self.state.lifecycle != state {
            self.commit(BridgeEventRecord::Lifecycle {
                state,
                at_ms: now_ms,
            })?;
        }
        Ok(())
    }

    /// Returns at most one immutable logical wake, with a bounded next pending
    /// batch retained in the journal rather than another external submission.
    pub fn next_action(&mut self, now_ms: u64) -> Result<Option<BridgeAction>, BrokerError> {
        self.check_ready()?;
        if matches!(
            self.state.readiness,
            Readiness::AttentionRequired | Readiness::Unsupported
        ) || matches!(
            self.state.lifecycle,
            SessionState::Busy | SessionState::WaitingForPermission | SessionState::Offline
        ) {
            return Ok(None);
        }
        if self.state.active.is_none() {
            if self.state.pending.is_empty()
                || self
                    .state
                    .pending_since_ms
                    .is_some_and(|since| now_ms < since.saturating_add(self.limits.coalesce_ms))
            {
                return Ok(None);
            }
            let binding = self.binding();
            let mut wake = Wake {
                wake_id: id::new_ulid()?,
                binding_id: binding.binding_id.clone(),
                generation: binding.generation,
                bus_id: binding.bus_id.clone(),
                agent_id: binding.agent_id.clone(),
                notices: Vec::new(),
                observation_nonce: crate::registry::new_token()?,
                created_at_ms: now_ms,
            };
            for notice in self.state.pending.values() {
                if wake.notices.len() >= self.limits.wake_notices { break; }
                wake.notices.push(notice.clone());
                if encoded_size(&wake)? > self.limits.wake_bytes {
                    wake.notices.pop();
                    if wake.notices.is_empty() {
                        self.attention("one arrival exceeds configured wake size")?;
                    }
                    break;
                }
            }
            if wake.notices.is_empty() {
                return Ok(None);
            }
            self.commit(BridgeEventRecord::WakeCreated { wake })?;
        }
        let active = self.state.active.as_ref().unwrap();
        if active.needs_reconcile {
            return Ok(Some(BridgeAction::Reconcile {
                wake: active.wake.clone(),
                turn_id: active.turn_id.clone(),
            }));
        }
        if active.deadline_ms.is_some_and(|deadline| now_ms < deadline)
            || now_ms < active.retry_at_ms
        {
            return Ok(None);
        }
        if active.attempts >= self.limits.max_attempts {
            self.attention("activation retry budget exhausted; unresolved notices retained")?;
            return Ok(None);
        }
        let wake = active.wake.clone();
        self.commit(BridgeEventRecord::AttemptStarted {
            wake_id: wake.wake_id.clone(),
            at_ms: now_ms,
            deadline_ms: now_ms.saturating_add(self.limits.observation_timeout_ms),
        })?;
        Ok(Some(BridgeAction::Submit(wake)))
    }

    pub fn record_submission(
        &mut self,
        wake_id: &str,
        submission: Submission,
        now_ms: u64,
    ) -> Result<(), BrokerError> {
        self.check_ready()?;
        let active = self.require_active(wake_id, self.binding().generation)?;
        if active.attempts == 0 {
            return Err(BrokerError::invalid(
                "submission has no durable activation attempt",
            ));
        }
        let shift = active.attempts.saturating_sub(1).min(31);
        let backoff = self
            .limits
            .retry_min_ms
            .saturating_mul(1u64 << shift)
            .min(self.limits.retry_max_ms);
        self.commit(BridgeEventRecord::SubmissionRecorded {
            wake_id: wake_id.into(),
            submission,
            retry_at_ms: now_ms.saturating_add(backoff),
        })
    }

    pub fn record_reconciliation(
        &mut self,
        wake_id: &str,
        submission: Submission,
    ) -> Result<(), BrokerError> {
        self.check_ready()?;
        self.require_active(wake_id, self.binding().generation)?;
        self.commit(BridgeEventRecord::Reconciled {
            wake_id: wake_id.into(),
            submission,
        })
    }

    pub fn issue_probe(&mut self, now_ms: u64) -> Result<ActivationChallenge, BrokerError> {
        self.check_ready()?;
        let binding = self.binding();
        let challenge = ActivationChallenge {
            binding_id: binding.binding_id.clone(),
            generation: binding.generation,
            nonce: crate::registry::new_token()?,
            expires_at_ms: now_ms.saturating_add(self.limits.observation_timeout_ms),
            purpose: NoncePurpose::Probe,
        };
        self.commit(BridgeEventRecord::ChallengeCreated {
            challenge: challenge.clone(),
        })?;
        Ok(challenge)
    }

    pub fn confirm_activation(
        &mut self,
        request: ConfirmActivationRequest,
        now_ms: u64,
    ) -> Result<ActivationReceipt, BrokerError> {
        self.check_ready()?;
        if request.binding_id != self.binding().binding_id
            || request.generation != self.binding().generation
        {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "stale activation binding generation",
            ));
        }
        if let Some(receipt) = self.state.confirmed.get(&request.nonce) {
            if receipt.wake_id != request.wake_id {
                return Err(BrokerError::invalid("nonce purpose mismatch"));
            }
            return Ok(receipt.clone());
        }
        let challenge = self
            .state
            .challenges
            .get(&request.nonce)
            .ok_or_else(|| BrokerError::invalid("unknown activation nonce"))?;
        let expected_wake = match &challenge.purpose {
            NoncePurpose::Probe => None,
            NoncePurpose::Wake { wake_id } => Some(wake_id.as_str()),
        };
        if let Some(wake_id) = expected_wake {
            self.require_active(wake_id, request.generation)?;
        }
        let paused_time = self
            .state
            .active
            .as_ref()
            .filter(|active| expected_wake == Some(active.wake.wake_id.as_str()))
            .and_then(|active| active.paused_at_ms)
            .map_or(0, |start| now_ms.saturating_sub(start));
        if challenge.binding_id != request.binding_id
            || challenge.generation != request.generation
            || expected_wake != request.wake_id.as_deref()
            || now_ms >= challenge.expires_at_ms.saturating_add(paused_time)
        {
            return Err(BrokerError::invalid(
                "activation nonce expired or has the wrong purpose",
            ));
        }
        self.commit(BridgeEventRecord::Confirmed {
            request: request.clone(),
            at_ms: now_ms,
        })?;
        Ok(self.state.confirmed[&request.nonce].clone())
    }

    pub fn observe_claim(&mut self, observation: ClaimObservation) -> Result<(), BrokerError> {
        if matches!(&observation.result, ClaimResult::Adopted { task_id } if task_id.is_empty()) {
            return Err(BrokerError::invalid("claim evidence is missing a task ID"));
        }
        let attempt = self.begin_claim(
            &observation.wake_id,
            observation.generation,
            &observation.queue,
        )?;
        self.finish_claim(&attempt.attempt_id, observation.result)
    }

    pub fn begin_claim(
        &mut self,
        wake_id: &str,
        generation: u64,
        queue: &str,
    ) -> Result<ClaimAttempt, BrokerError> {
        self.check_ready()?;
        let active = self.require_active(wake_id, generation)?;
        if !active.wake.notices.iter().any(|notice| matches!(&notice.resource, ArrivalReference::Queue { name, .. } if name == queue)) {
            return Err(BrokerError::invalid("claim evidence names a queue outside the active wake"));
        }
        if active.claim_attempts.len() >= 64 {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "too many concurrent observed claims",
            ));
        }
        let attempt = ClaimAttempt {
            attempt_id: id::new_ulid()?,
            wake_id: wake_id.into(),
            generation,
            queue: queue.into(),
            queue_generation: self
                .state
                .queue_generations
                .get(queue)
                .copied()
                .unwrap_or(0),
            adoption_epoch: active.adoption_epoch,
        };
        self.commit(BridgeEventRecord::ClaimStarted {
            attempt: attempt.clone(),
        })?;
        Ok(attempt)
    }

    pub fn finish_claim(
        &mut self,
        attempt_id: &str,
        result: ClaimResult,
    ) -> Result<(), BrokerError> {
        self.check_ready()?;
        if matches!(&result, ClaimResult::Adopted { task_id } if task_id.is_empty()) {
            return Err(BrokerError::invalid("claim evidence is missing a task ID"));
        }
        if !self
            .state
            .active
            .as_ref()
            .is_some_and(|active| active.claim_attempts.contains_key(attempt_id))
        {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "claim attempt belongs to another wake or binding generation",
            ));
        }
        self.commit(BridgeEventRecord::ClaimFinished {
            attempt_id: attempt_id.into(),
            result,
        })
    }

    pub fn finish_claim_context(
        &mut self,
        context: &ClaimAttempt,
        result: ClaimResult,
    ) -> Result<(), BrokerError> {
        let persisted = self
            .require_active(&context.wake_id, context.generation)?
            .claim_attempts
            .get(&context.attempt_id)
            .ok_or_else(|| {
                BrokerError::application(ErrorCode::Conflict, "unknown claim attempt context")
            })?;
        if persisted != context {
            return Err(BrokerError::invalid("claim attempt context was changed"));
        }
        self.finish_claim(&context.attempt_id, result)
    }

    pub fn resolve(
        &mut self,
        request: ResolveArrivalsRequest,
    ) -> Result<ResolutionReceipt, BrokerError> {
        self.check_ready()?;
        if request.generation != self.binding().generation {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "stale resolution generation",
            ));
        }
        let fingerprint = format!(
            "sha256:{}",
            sha256_hex(
                canonical_json(
                    &serde_json::to_value(&request)
                        .map_err(|error| BrokerError::internal(error.to_string()))?
                )
                .as_bytes()
            )
        );
        if let Some((previous, receipt)) = self.state.resolutions.get(&request.wake_id) {
            if previous != &fingerprint {
                return Err(BrokerError::application(
                    ErrorCode::Conflict,
                    "conflicting resolution retry",
                ));
            }
            return Ok(receipt.clone());
        }
        let active = self.require_active(&request.wake_id, request.generation)?;
        if request.decisions.is_empty() || !active.claim_attempts.is_empty() {
            return Err(BrokerError::invalid(
                "resolution requires decisions and all observed claims to finish",
            ));
        }
        let mut seen = BTreeSet::new();
        let mut resolved = Vec::new();
        for decision in &request.decisions {
            if !seen.insert(&decision.delivery_id) {
                return Err(BrokerError::invalid("duplicate resolution decision"));
            }
            let notice = active
                .wake
                .notices
                .iter()
                .find(|notice| notice.delivery_id == decision.delivery_id)
                .ok_or_else(|| {
                    BrokerError::invalid("resolution names a notice outside the wake")
                })?;
            match (&notice.resource, &decision.disposition) {
                (ArrivalReference::Document { .. }, Disposition::DocumentRead) => {
                    resolved.push(decision.delivery_id.clone())
                }
                (
                    ArrivalReference::Document { version, .. },
                    Disposition::DocumentSuperseded {
                        newer_version,
                        policy,
                    },
                ) if newer_version > version && !policy.trim().is_empty() => {
                    resolved.push(decision.delivery_id.clone())
                }
                (ArrivalReference::Queue { name, .. }, Disposition::QueueDrained) => {
                    if active.empty_proofs.get(name).copied().unwrap_or(0) < notice.seq {
                        return Err(BrokerError::invalid("queue drained needs a fresh proxy-observed empty claim for this wake and generation"));
                    }
                    resolved.push(decision.delivery_id.clone());
                }
                (ArrivalReference::Queue { name, .. }, Disposition::WorkAdopted { task_id }) => {
                    if !active
                        .adopted_tasks
                        .get(name)
                        .is_some_and(|tasks| tasks.contains(task_id))
                    {
                        return Err(BrokerError::invalid(
                            "work adoption needs a proxy-observed claim for this wake",
                        ));
                    }
                    if !request.continuation {
                        return Err(BrokerError::invalid("work adoption leaves queue availability unresolved; continuation must remain true"));
                    }
                    // Retaining the hint creates the later bounded wake even
                    // when no newer availability notice has arrived.
                }
                _ => {
                    return Err(BrokerError::invalid(
                        "disposition is incompatible with its notice reference",
                    ))
                }
            }
        }
        let pending_notices = self.state.pending.len().saturating_sub(resolved.len());
        let receipt = ResolutionReceipt {
            wake_id: request.wake_id,
            generation: request.generation,
            resolved_delivery_ids: resolved,
            continuation: request.continuation || pending_notices > 0,
            pending_notices,
        };
        self.commit(BridgeEventRecord::Resolved {
            fingerprint,
            receipt: receipt.clone(),
        })?;
        Ok(receipt)
    }

    /// Explicit retry is an operator decision after the bounded budget stopped.
    pub fn retry_pending(&mut self, now_ms: u64) -> Result<(), BrokerError> {
        self.check_ready()?;
        self.commit(BridgeEventRecord::RetryEnabled { at_ms: now_ms })
    }

    /// Rebinding is explicit and must first quiesce any externally accepted
    /// turn. Pending references cannot be silently moved to another session.
    pub fn rebind(
        &mut self,
        binding: Binding,
        external_turn_quiesced: bool,
    ) -> Result<(), BrokerError> {
        self.check_ready()?;
        validate_binding(&binding)?;
        if !external_turn_quiesced
            || binding.bus_id != self.binding().bus_id
            || binding.agent_id != self.binding().agent_id
            || binding.generation != self.binding().generation.saturating_add(1)
        {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "rebind requires quiescence, same identity and the next generation",
            ));
        }
        if !self.state.pending.is_empty()
            && (binding.exact_host_session_id != self.binding().exact_host_session_id
                || binding.adapter_kind != self.binding().adapter_kind
                || binding.workspace != self.binding().workspace)
        {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "pending notices cannot be redirected to another host session",
            ));
        }
        self.commit(BridgeEventRecord::Bound { binding })
    }

    pub fn attention(&mut self, detail: impl Into<String>) -> Result<(), BrokerError> {
        self.check_ready()?;
        self.commit(BridgeEventRecord::Attention {
            detail: detail.into(),
        })
    }

    fn require_active(&self, wake_id: &str, generation: u64) -> Result<&WakeState, BrokerError> {
        let active = self
            .state
            .active
            .as_ref()
            .ok_or_else(|| BrokerError::invalid("no active arrival wake"))?;
        if generation != self.binding().generation
            || active.wake.generation != generation
            || active.wake.wake_id != wake_id
        {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "wake or binding generation mismatch",
            ));
        }
        Ok(active)
    }

    fn check_ready(&self) -> Result<(), BrokerError> {
        if self.log.requires_recovery() {
            return Err(BrokerError::internal(
                "bridge journal requires actual reopen after uncertain I/O",
            ));
        }
        Ok(())
    }

    fn commit(&mut self, event: BridgeEventRecord) -> Result<(), BrokerError> {
        let mut record = BridgeRecord {
            v: 1,
            seq: 0,
            prev_hash: GENESIS_HASH.into(),
            event,
        };
        self.log.append(&mut record)?;
        self.state.apply(&record.event)
    }
}

impl BridgeState {
    fn apply(&mut self, event: &BridgeEventRecord) -> Result<(), BrokerError> {
        match event {
            BridgeEventRecord::Bound { binding } => {
                self.binding = Some(binding.clone());
                self.active = None;
                self.challenges.clear();
                self.confirmed.clear();
                self.readiness = Readiness::Configured;
                self.lifecycle = SessionState::Unknown;
                self.error = None;
            }
            BridgeEventRecord::Received {
                notices,
                cursor,
                at_ms,
            } => {
                for notice in notices {
                    self.received_seq = notice.seq;
                    self.received_hash = hash_record(notice)?;
                    self.pending_bytes = self.pending_bytes.saturating_add(encoded_size(notice)?);
                    if let ArrivalReference::Queue { name, .. } = &notice.resource {
                        self.queue_generations.insert(name.clone(), notice.seq);
                    }
                    self.pending.insert(notice.seq, notice.clone());
                }
                self.cursor = Some(cursor.clone());
                self.pending_since_ms.get_or_insert(*at_ms);
            }
            BridgeEventRecord::WakeCreated { wake } => {
                self.active = Some(WakeState {
                    wake: wake.clone(),
                    attempts: 0,
                    deadline_ms: None,
                    retry_at_ms: 0,
                    paused_at_ms: None,
                    observed: false,
                    turn_id: None,
                    last_submission: None,
                    needs_reconcile: false,
                    empty_proofs: BTreeMap::new(),
                    adopted_tasks: BTreeMap::new(),
                    adoption_epoch: 0,
                    claim_attempts: BTreeMap::new(),
                });
            }
            BridgeEventRecord::AttemptStarted {
                wake_id,
                at_ms,
                deadline_ms,
            } => {
                let active = self.active_mut(wake_id)?;
                active.attempts = active.attempts.saturating_add(1);
                active.deadline_ms = Some(*deadline_ms);
                active.retry_at_ms = *at_ms;
                active.needs_reconcile = false;
                active.last_submission = None;
                let challenge = ActivationChallenge {
                    binding_id: active.wake.binding_id.clone(),
                    generation: active.wake.generation,
                    nonce: active.wake.observation_nonce.clone(),
                    expires_at_ms: *deadline_ms,
                    purpose: NoncePurpose::Wake {
                        wake_id: wake_id.clone(),
                    },
                };
                self.challenges.insert(challenge.nonce.clone(), challenge);
            }
            BridgeEventRecord::SubmissionRecorded {
                wake_id,
                submission,
                retry_at_ms,
            } => {
                let active = self.active_mut(wake_id)?;
                active.retry_at_ms = *retry_at_ms;
                match submission {
                    Submission::Accepted { turn_id } => active.turn_id = turn_id.clone(),
                    Submission::Unknown { .. } => active.needs_reconcile = true,
                    Submission::Busy | Submission::PermissionBlocked => {
                        // No external submission was admitted, so it does not
                        // consume an activation failure budget.
                        active.attempts = active.attempts.saturating_sub(1);
                        active.deadline_ms = None;
                    }
                    Submission::Unsupported { .. } => active.deadline_ms = None,
                }
                active.last_submission = Some(submission.clone());
                match submission {
                    Submission::PermissionBlocked => {
                        self.readiness = Readiness::WaitingForPermission
                    }
                    Submission::Unsupported { detail } => {
                        self.readiness = Readiness::Unsupported;
                        self.error = Some(detail.clone());
                    }
                    _ => {}
                }
            }
            BridgeEventRecord::Reconciled {
                wake_id,
                submission,
            } => {
                let active = self.active_mut(wake_id)?;
                active.needs_reconcile = false;
                if let Submission::Accepted { turn_id } = submission {
                    active.turn_id = turn_id.clone();
                }
                active.last_submission = Some(submission.clone());
            }
            BridgeEventRecord::Lifecycle { state, at_ms } => {
                let paused = matches!(
                    state,
                    SessionState::Busy | SessionState::WaitingForPermission
                );
                if let Some(active) = &mut self.active {
                    if paused {
                        active.paused_at_ms.get_or_insert(*at_ms);
                    } else if let Some(start) = active.paused_at_ms.take() {
                        let delta = at_ms.saturating_sub(start);
                        if let Some(deadline) = &mut active.deadline_ms {
                            *deadline = deadline.saturating_add(delta);
                        }
                        if let Some(challenge) =
                            self.challenges.get_mut(&active.wake.observation_nonce)
                        {
                            challenge.expires_at_ms = challenge.expires_at_ms.saturating_add(delta);
                        }
                    }
                }
                self.lifecycle = *state;
                if matches!(self.readiness, Readiness::AttentionRequired | Readiness::Unsupported) {
                    // Lifecycle updates cannot undo an explicit admission
                    // fence. Retry or a valid observation resolves that state.
                } else if *state == SessionState::WaitingForPermission {
                    self.readiness = Readiness::WaitingForPermission;
                } else if *state == SessionState::Offline {
                    self.readiness = Readiness::Offline;
                } else if matches!(
                    self.readiness,
                    Readiness::WaitingForPermission | Readiness::Offline
                ) {
                    self.readiness = Readiness::Configured;
                }
            }
            BridgeEventRecord::ChallengeCreated { challenge } => {
                self.challenges
                    .insert(challenge.nonce.clone(), challenge.clone());
            }
            BridgeEventRecord::Confirmed { request, at_ms } => {
                if let Some(wake_id) = &request.wake_id {
                    self.active_mut(wake_id)?.observed = true;
                }
                self.readiness = Readiness::Verified;
                self.last_probe_ms = Some(*at_ms);
                self.confirmed.insert(
                    request.nonce.clone(),
                    ActivationReceipt {
                        binding_id: request.binding_id.clone(),
                        generation: request.generation,
                        wake_id: request.wake_id.clone(),
                        observed_at_ms: *at_ms,
                        readiness: Readiness::Verified,
                    },
                );
            }
            BridgeEventRecord::ClaimStarted { attempt } => {
                self.active_mut(&attempt.wake_id)?
                    .claim_attempts
                    .insert(attempt.attempt_id.clone(), attempt.clone());
            }
            BridgeEventRecord::ClaimFinished { attempt_id, result } => {
                let active = self
                    .active
                    .as_mut()
                    .ok_or_else(|| BrokerError::internal("claim finish has no active wake"))?;
                let attempt = active
                    .claim_attempts
                    .remove(attempt_id)
                    .ok_or_else(|| BrokerError::internal("unknown persisted claim attempt"))?;
                match result {
                    ClaimResult::Adopted { task_id } => {
                        active.adoption_epoch = active.adoption_epoch.saturating_add(1);
                        active.empty_proofs.remove(&attempt.queue);
                        active
                            .adopted_tasks
                            .entry(attempt.queue)
                            .or_default()
                            .insert(task_id.clone());
                    }
                    ClaimResult::Empty if attempt.adoption_epoch == active.adoption_epoch => {
                        active
                            .empty_proofs
                            .insert(attempt.queue, attempt.queue_generation);
                    }
                    ClaimResult::Unproven => {
                        // A lost claim response may have adopted work. An
                        // older empty result cannot survive that uncertainty.
                        active.adoption_epoch = active.adoption_epoch.saturating_add(1);
                        active.empty_proofs.remove(&attempt.queue);
                    }
                    ClaimResult::Empty => {}
                }
            }
            BridgeEventRecord::Resolved {
                fingerprint,
                receipt,
            } => {
                self.active_mut(&receipt.wake_id)?;
                let resolved: BTreeSet<_> = receipt.resolved_delivery_ids.iter().collect();
                self.pending
                    .retain(|_, notice| !resolved.contains(&notice.delivery_id));
                self.pending_bytes = self.pending.values().try_fold(0u64, |total, notice| {
                    encoded_size(notice).map(|size| total.saturating_add(size))
                })?;
                self.active = None;
                self.resolutions.insert(
                    receipt.wake_id.clone(),
                    (fingerprint.clone(), receipt.clone()),
                );
                if self.pending.is_empty() {
                    self.pending_since_ms = None;
                }
                self.readiness = Readiness::Verified;
                self.error = None;
            }
            BridgeEventRecord::Attention { detail } => {
                self.readiness = Readiness::AttentionRequired;
                self.error = Some(detail.clone());
            }
            BridgeEventRecord::RetryEnabled { at_ms } => {
                if let Some(active) = &mut self.active {
                    active.attempts = 0;
                    active.deadline_ms = None;
                    active.retry_at_ms = *at_ms;
                    active.needs_reconcile = true;
                }
                self.readiness = Readiness::Configured;
                self.error = None;
            }
        }
        Ok(())
    }

    fn active_mut(&mut self, wake_id: &str) -> Result<&mut WakeState, BrokerError> {
        self.active
            .as_mut()
            .filter(|active| active.wake.wake_id == wake_id)
            .ok_or_else(|| {
                BrokerError::internal("bridge journal references an unknown active wake")
            })
    }
}

fn encoded_size<T: Serialize>(value: &T) -> Result<u64, BrokerError> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len() as u64)
        .map_err(|error| BrokerError::internal(error.to_string()))
}

fn same_binding(left: &Binding, right: &Binding) -> bool {
    left.binding_id == right.binding_id
        && left.generation == right.generation
        && left.bus_id == right.bus_id
        && left.agent_id == right.agent_id
        && left.adapter_kind == right.adapter_kind
        && left.exact_host_session_id == right.exact_host_session_id
        && left.workspace == right.workspace
}

fn validate_binding(binding: &Binding) -> Result<(), BrokerError> {
    if binding.binding_id.is_empty()
        || binding.generation == 0
        || binding.bus_id.is_empty()
        || binding.agent_id.is_empty()
        || binding.adapter_kind.is_empty()
        || binding.exact_host_session_id.is_empty()
        || binding.workspace.is_empty()
    {
        return Err(BrokerError::invalid(
            "host binding requires canonical identity, exact session and positive generation",
        ));
    }
    Ok(())
}

fn validate_limits(limits: &BridgeLimits) -> Result<(), BrokerError> {
    if limits.max_pending_notices == 0
        || limits.max_pending_bytes == 0
        || limits.wake_notices == 0
        || limits.wake_bytes == 0
        || limits.max_attempts == 0
        || limits.observation_timeout_ms == 0
        || limits.retry_min_ms == 0
        || limits.retry_max_ms < limits.retry_min_ms
    {
        return Err(BrokerError::invalid("invalid host bridge bounds"));
    }
    Ok(())
}

fn sync_ancestry(path: &Path) -> std::io::Result<()> {
    for directory in path
        .ancestors()
        .filter(|entry| !entry.as_os_str().is_empty())
    {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

fn parse_received_cursor(bus_id: &str, agent_id: &str, cursor: &str) -> Result<u64, BrokerError> {
    cursor.strip_prefix(&format!("arrival:{bus_id}:{agent_id}:"))
        .and_then(|sequence| sequence.parse().ok())
        .ok_or_else(|| BrokerError::invalid("arrival cursor belongs to another binding or is malformed"))
}

fn canonical_destination(path: &Path) -> std::io::Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let name = path.file_name().ok_or(error)?;
            Ok(canonical_destination(parent)?.join(name))
        }
        Err(error) => Err(error),
    }
}
