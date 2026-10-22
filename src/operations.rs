//! Source-bound durable remote operation identity and replay-derived receipts.
//! The index is only a projection of proofs embedded in authoritative records.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use crate::envelope::Envelope;
use crate::error::{BrokerError, ErrorCode};
use crate::inbox_writer::Completion;
use crate::messaging::{Inbox, SendReceipt, SendRequest};
use crate::rpc::RpcError;
use crate::storage::{ChainRecord, LogFile};

pub const OPERATION_QUOTA: usize = 10_000;
pub const MAX_SUBMISSION_WINDOW_MS: u64 = 24 * 60 * 60 * 1000;
pub const MAX_DOMAIN_INPUT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRef {
    pub v: u16,
    pub id: String,
    pub epoch: String,
    pub not_after_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationScope {
    pub bus_id: String,
    pub principal_id: String,
    pub agent_id: String,
}

/// Typed method adapters supply normalized, credential-free semantic input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationContext {
    pub scope: OperationScope,
    pub operation: OperationRef,
    pub method: String,
    pub resource: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum OperationOutcome {
    Success(Value),
    Error(RpcError),
}

// RpcError's fields (integer, string, JSON value) have total equality.
impl Eq for OperationOutcome {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionRequirement {
    DeadLetter { queue: String, task_id: String },
}

/// Resource mutation proof. The original inbox proof above retains its N3a
/// representation, while both proof families enter one global identity index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainOperationProof {
    pub context: OperationContext,
    pub digest: String,
    pub outcome: OperationOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<CompletionRequirement>,
}

impl DomainOperationProof {
    /// Common validation only. Each source owner must additionally bind input
    /// and original outcome to the actual event and verified prior source state.
    pub fn verify(&self, method: &str, resource: &str) -> Result<(), BrokerError> {
        self.context.validate()?;
        if self.context.method != method
            || self.context.resource != resource
            || self.digest != semantic_digest(method, &self.context.input)
            || crate::rpc::bounded_json_size(&self.outcome, crate::transport::MAX_RPC_BYTES - 8192)
                .is_none()
        {
            return Err(BrokerError::internal(
                "invalid resource operation proof binding",
            ));
        }
        if let Some(CompletionRequirement::DeadLetter { queue, task_id }) = &self.completion {
            if self.context.method != "fail_task"
                || self.context.resource != format!("queue:{queue}")
                || !crate::id::is_ulid(task_id)
                || !matches!(&self.outcome, OperationOutcome::Success(value)
                    if value.get("dead_letter_seq") == Some(&Value::Null)
                    && value.get("queue").and_then(Value::as_str) == Some(queue)
                    && value.get("task_id").and_then(Value::as_str) == Some(task_id))
            {
                return Err(BrokerError::internal(
                    "invalid operation completion requirement",
                ));
            }
        }
        Ok(())
    }

    fn verify_completed(&self, completed: &OperationOutcome) -> Result<(), BrokerError> {
        let matches = match (&self.completion, &self.outcome, completed) {
            (None, expected, actual) => expected == actual,
            (
                Some(CompletionRequirement::DeadLetter { .. }),
                OperationOutcome::Success(template),
                OperationOutcome::Success(actual),
            ) => {
                let seq = actual.get("dead_letter_seq").and_then(Value::as_u64);
                let mut expected = template.clone();
                expected["dead_letter_seq"] = seq.map(Value::from).unwrap_or(Value::Null);
                seq.is_some_and(|seq| seq > 0) && &expected == actual
            }
            _ => false,
        };
        if !matches {
            return Err(BrokerError::internal(
                "completed operation differs from authoritative source proof",
            ));
        }
        Ok(())
    }
}

impl OperationContext {
    fn validate(&self) -> Result<(), BrokerError> {
        if crate::rpc::bounded_json_size(&self.input, MAX_DOMAIN_INPUT_BYTES).is_none() {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "resource operation input exceeds 256 KiB",
            ));
        }
        if self.operation.v != 1
            || !crate::id::is_ulid(&self.operation.id)
            || !crate::id::is_ulid(&self.operation.epoch)
            || self.operation.not_after_ms == 0
            || !crate::id::is_ulid(&self.scope.bus_id)
            || !crate::id::is_ulid(&self.scope.agent_id)
            || self.scope.principal_id.is_empty()
            || self.scope.principal_id.len() > 128
            || !matches!(
                self.method.as_str(),
                "publish"
                    | "create_queue"
                    | "enqueue"
                    | "claim_task"
                    | "renew_lease"
                    | "complete_task"
                    | "fail_task"
                    | "publish_doc"
                    | "update_doc"
                    | "claim_doc"
                    | "release_doc"
            )
            || self.resource.is_empty()
            || self.resource.len() > 512
            || !self.input.is_object()
            || self.input.get("token").is_some()
            || self.input.get("operation").is_some()
        {
            return Err(BrokerError::invalid(
                "invalid normalized resource operation context",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationMetadata {
    pub method: String,
    pub resource: String,
}

#[derive(Debug, Default)]
struct DomainCompletion {
    // Outer Err is an uncertain, nonterminal failure. An inner Error is an
    // authoritative no-effect outcome and remains a committed status.
    outcome: Mutex<Option<Result<OperationOutcome, RpcError>>>,
    changed: Notify,
}

impl DomainCompletion {
    fn finish(&self, outcome: Result<OperationOutcome, RpcError>) {
        let mut slot = self.outcome.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(outcome);
            drop(slot);
            self.changed.notify_waiters();
        }
    }

    fn status(&self) -> Value {
        match self
            .outcome
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            None => json!({"state":"pending"}),
            Some(Ok(OperationOutcome::Success(value))) => {
                json!({"state":"committed","result":value})
            }
            Some(Ok(OperationOutcome::Error(error))) => json!({"state":"committed","error":error}),
            Some(Err(_)) => json!({"state":"unknown"}),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DomainWaiter(Arc<DomainCompletion>);

impl DomainWaiter {
    pub async fn wait(&self) -> Result<Value, RpcError> {
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let slot = self.0.outcome.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(outcome) = slot.as_ref() {
                    return match outcome {
                        Ok(OperationOutcome::Success(value)) => Ok(value.clone()),
                        Ok(OperationOutcome::Error(error)) | Err(error) => Err(error.clone()),
                    };
                }
            }
            notified.await;
        }
    }
}

#[derive(Debug)]
pub enum DomainAdmission {
    Existing(DomainWaiter),
    New(DomainTicket),
}

/// A root-owned detached worker holds the ticket and the domain admission lock.
/// Dropping or panicking never resets the identity to unseen.
#[derive(Debug)]
pub struct DomainTicket {
    context: OperationContext,
    completion: Arc<DomainCompletion>,
    effect_attempted: AtomicBool,
    uncertain_maintenance: AtomicUsize,
    proof: Mutex<Option<DomainOperationProof>>,
}

/// A maintenance append may precede evaluation of the requested operation.
/// Only successful completion of its entire durable/derived work releases this
/// guard's uncertainty. Dropping it after an error keeps the ticket fenced.
#[derive(Debug)]
#[must_use = "complete maintenance only after all durable and derived work succeeds"]
pub struct DomainMaintenance<'a> {
    ticket: &'a DomainTicket,
}

impl DomainMaintenance<'_> {
    pub fn complete(self) {
        self.ticket
            .uncertain_maintenance
            .fetch_sub(1, Ordering::AcqRel);
    }
}

impl DomainTicket {
    pub fn context(&self) -> &OperationContext {
        &self.context
    }
    pub fn waiter(&self) -> DomainWaiter {
        DomainWaiter(self.completion.clone())
    }
    pub fn mark_effect_attempt(&self) {
        self.effect_attempted.store(true, Ordering::Release);
    }
    pub fn effect_attempted(&self) -> bool {
        self.effect_attempted.load(Ordering::Acquire)
            || self.uncertain_maintenance.load(Ordering::Acquire) > 0
    }
    pub fn begin_maintenance(&self) -> DomainMaintenance<'_> {
        self.uncertain_maintenance.fetch_add(1, Ordering::AcqRel);
        DomainMaintenance { ticket: self }
    }

    pub fn proof(&self, outcome: OperationOutcome) -> Result<DomainOperationProof, BrokerError> {
        self.make_proof(outcome, None)
    }

    pub fn proof_with_completion(
        &self,
        outcome: OperationOutcome,
        completion: CompletionRequirement,
    ) -> Result<DomainOperationProof, BrokerError> {
        self.make_proof(outcome, Some(completion))
    }

    fn make_proof(
        &self,
        outcome: OperationOutcome,
        completion: Option<CompletionRequirement>,
    ) -> Result<DomainOperationProof, BrokerError> {
        if crate::rpc::bounded_json_size(&outcome, crate::transport::MAX_RPC_BYTES - 8192).is_none()
        {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "resource operation result exceeds remote response budget",
            ));
        }
        let proof = DomainOperationProof {
            context: self.context.clone(),
            digest: semantic_digest(&self.context.method, &self.context.input),
            outcome,
            completion,
        };
        proof.verify(&self.context.method, &self.context.resource)?;
        let mut slot = self.proof.lock().unwrap_or_else(|p| p.into_inner());
        if slot.as_ref().is_some_and(|existing| existing != &proof) {
            return Err(BrokerError::internal(
                "operation worker attempted to replace its source proof",
            ));
        }
        *slot = Some(proof.clone());
        Ok(proof)
    }

    pub fn finish_success(&self, result: Value) -> Result<(), BrokerError> {
        if !self.effect_attempted.load(Ordering::Acquire)
            || self.uncertain_maintenance.load(Ordering::Acquire) > 0
        {
            return Err(BrokerError::internal(
                "operation success has no attempted source effect",
            ));
        }
        let slot = self.proof.lock().unwrap_or_else(|p| p.into_inner());
        let proof = slot
            .as_ref()
            .ok_or_else(|| BrokerError::internal("operation success has no source proof"))?;
        let outcome = OperationOutcome::Success(result);
        proof.verify_completed(&outcome)?;
        self.completion.finish(Ok(outcome));
        Ok(())
    }

    pub fn finish_unknown(&self) {
        self.completion.finish(Err(RpcError::application(
            ErrorCode::OutcomeUnknown,
            "operation source outcome is uncertain; authoritative reopen is required",
        )));
    }
}

impl Drop for DomainTicket {
    fn drop(&mut self) {
        self.finish_unknown();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeRecord {
    seq: u64,
    prev_hash: String,
    ts: String,
    proof: DomainOperationProof,
}

impl ChainRecord for OutcomeRecord {
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

/// The result and all information needed to verify its semantic binding live
/// in the same hash-chained inbox record as the message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationProof {
    pub scope: OperationScope,
    pub operation: OperationRef,
    pub digest: String,
    pub method: String,
    pub resource: String,
    pub ttl_ms: Option<u64>,
    pub expires_at_ms: Option<u64>,
    pub result: SendReceipt,
}

impl OperationProof {
    fn same_intent(&self, other: &Self) -> bool {
        self.scope == other.scope
            && self.operation == other.operation
            && self.digest == other.digest
            && self.method == other.method
            && self.resource == other.resource
    }

    pub(crate) fn verify(
        &self,
        envelope: &Envelope,
        agent_id: &str,
        expires: Option<u64>,
    ) -> Result<(), BrokerError> {
        let invalid = || BrokerError::internal("invalid authoritative operation proof");
        if envelope.validate().is_err()
            || self.operation.v != 1
            || !crate::id::is_ulid(&self.operation.id)
            || !crate::id::is_ulid(&self.operation.epoch)
            || self.operation.not_after_ms == 0
            || !crate::id::is_ulid(&self.scope.bus_id)
            || self.scope.principal_id.is_empty()
            || !crate::id::is_ulid(&self.scope.agent_id)
            || self.method != "send_message"
            || self.resource != format!("inbox:{agent_id}")
            || self.result.message_id != envelope.id
            || self.result.seq != envelope.seq
            || self.result.ts.as_deref() != Some(&envelope.ts)
            || self.result.to != envelope.to
            || self.result.to_agent_id != agent_id
            || self.result.duplicate
            || self.result.idempotency_key.is_some()
            || envelope.idempotency_key.is_some()
            || self.expires_at_ms != expires
            || self.ttl_ms.is_some() != expires.is_some()
        {
            return Err(invalid());
        }
        let digest = send_digest(
            agent_id,
            &SendRequest {
                to: agent_id.into(),
                kind: envelope.kind.clone(),
                body: envelope.body.clone(),
                reply_to: envelope.reply_to.clone(),
                idempotency_key: None,
                ttl: self.ttl_ms.map(std::time::Duration::from_millis),
            },
        );
        if self.digest != digest {
            return Err(invalid());
        }
        Ok(())
    }
}

/// Stable JSON digest. Array order and JSON numeric representations are retained.
pub fn semantic_digest(method: &str, value: &Value) -> String {
    fn canonical(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let mut entries: Vec<_> = object.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                Value::Object(
                    entries
                        .into_iter()
                        .map(|(k, v)| (k.clone(), canonical(v)))
                        .collect(),
                )
            }
            Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
            value => value.clone(),
        }
    }
    let bytes = serde_json::to_vec(&json!({"method":method,"input":canonical(value)}))
        .expect("JSON value serializes");
    format!("sha256:{:x}", Sha256::digest(bytes))
}

pub(crate) fn send_digest(recipient_id: &str, request: &SendRequest) -> String {
    semantic_digest(
        "send_message",
        &json!({
            "to":recipient_id, "kind":request.kind, "body":request.body,
            "reply_to":request.reply_to,
            "ttl_ms":request.ttl.map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
        }),
    )
}

#[derive(Debug, Clone)]
enum Entry {
    Send {
        proof: OperationProof,
        completion: Arc<Completion>,
    },
    Domain {
        context: OperationContext,
        completion: Arc<DomainCompletion>,
    },
}

impl Entry {
    fn reference(&self) -> &OperationRef {
        match self {
            Self::Send { proof, .. } => &proof.operation,
            Self::Domain { context, .. } => &context.operation,
        }
    }

    fn metadata(&self) -> OperationMetadata {
        match self {
            Self::Send { proof, .. } => OperationMetadata {
                method: proof.method.clone(),
                resource: proof.resource.clone(),
            },
            Self::Domain { context, .. } => OperationMetadata {
                method: context.method.clone(),
                resource: context.resource.clone(),
            },
        }
    }

    fn status(&self) -> Value {
        match self {
            Self::Send { completion, .. } => completion.operation_status(),
            Self::Domain { completion, .. } => completion.status(),
        }
    }
}

#[derive(Debug)]
pub struct OperationStore {
    bus_id: String,
    epoch: Option<(String, u64)>,
    entries: Mutex<HashMap<(OperationScope, String), Entry>>,
    outcomes: Mutex<Option<LogFile<OutcomeRecord>>>,
    resource_recovery_enabled: AtomicBool,
}

impl OperationStore {
    pub fn new(bus_id: String, epoch: Option<(String, u64)>) -> Self {
        Self {
            bus_id,
            epoch,
            entries: Mutex::new(HashMap::new()),
            outcomes: Mutex::new(None),
            resource_recovery_enabled: AtomicBool::new(false),
        }
    }

    pub fn epoch(&self) -> Option<&str> {
        self.epoch.as_ref().map(|(epoch, _)| epoch.as_str())
    }

    fn key(proof: &OperationProof) -> (OperationScope, String) {
        (proof.scope.clone(), proof.operation.id.clone())
    }

    fn validate_reference(
        &self,
        scope: &OperationScope,
        reference: &OperationRef,
        now_ms: u64,
    ) -> Result<(), BrokerError> {
        let Some((epoch, expires)) = &self.epoch else {
            return Err(BrokerError::application(
                ErrorCode::Forbidden,
                "durable operations are disabled",
            ));
        };
        if reference.v != 1
            || !crate::id::is_ulid(&reference.id)
            || scope.bus_id != self.bus_id
            || scope.principal_id.is_empty()
            || !crate::id::is_ulid(&scope.agent_id)
        {
            return Err(BrokerError::invalid("invalid operation identity or scope"));
        }
        if reference.epoch != *epoch || now_ms >= *expires {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "operation epoch is invalid or expired",
            ));
        }
        Ok(())
    }

    /// Metadata permits the edge to authorize the original resource before a
    /// historical receipt is returned. This never admits an unseen operation.
    pub fn resource(
        &self,
        scope: &OperationScope,
        reference: &OperationRef,
    ) -> Result<Option<String>, BrokerError> {
        Ok(self
            .metadata(scope, reference)?
            .map(|metadata| metadata.resource))
    }

    pub fn metadata(
        &self,
        scope: &OperationScope,
        reference: &OperationRef,
    ) -> Result<Option<OperationMetadata>, BrokerError> {
        let entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let entry = entries.get(&(scope.clone(), reference.id.clone()));
        if entry.is_some_and(|entry| entry.reference() != reference) {
            return Err(conflict());
        }
        self.validate_reference(scope, reference, crate::id::unix_millis())?;
        Ok(entry.map(Entry::metadata))
    }

    /// Query only this stable scope. The caller authorizes `resource()` before
    /// returning a committed outcome; an unseen ID conveys no other scope.
    pub fn status(
        &self,
        scope: &OperationScope,
        reference: &OperationRef,
    ) -> Result<Value, BrokerError> {
        self.metadata(scope, reference)?;
        let entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        Ok(entries
            .get(&(scope.clone(), reference.id.clone()))
            .map_or_else(|| json!({"state":"unseen"}), Entry::status))
    }

    pub(crate) fn known(
        &self,
        scope: &OperationScope,
        reference: &OperationRef,
        now_ms: u64,
    ) -> Result<Option<OperationProof>, BrokerError> {
        let entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let entry = entries.get(&(scope.clone(), reference.id.clone()));
        if entry.is_some_and(|entry| entry.reference() != reference) {
            return Err(conflict());
        }
        self.validate_reference(scope, reference, now_ms)?;
        match entry {
            Some(Entry::Send { proof, .. }) => Ok(Some(proof.clone())),
            Some(Entry::Domain { .. }) => Err(conflict()),
            None => Ok(None),
        }
    }

    pub(crate) fn lookup(
        &self,
        proof: &OperationProof,
        now_ms: u64,
    ) -> Result<Option<Arc<Completion>>, BrokerError> {
        self.validate_reference(&proof.scope, &proof.operation, now_ms)?;
        let entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        match entries.get(&Self::key(proof)) {
            Some(Entry::Send {
                proof: existing,
                completion,
            }) if existing.same_intent(proof) => Ok(Some(completion.clone())),
            Some(_) => Err(conflict()),
            None => Ok(None),
        }
    }

    /// Called with the inbox admission queue locked and before effects. No
    /// await separates this reservation from enqueueing its independent owner.
    pub(crate) fn reserve(
        &self,
        proof: &OperationProof,
        completion: &Arc<Completion>,
        now_ms: u64,
    ) -> Result<Option<Arc<Completion>>, BrokerError> {
        self.validate_reference(&proof.scope, &proof.operation, now_ms)?;
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = entries.get(&Self::key(proof)) {
            return match entry {
                Entry::Send {
                    proof: existing,
                    completion,
                } if existing.same_intent(proof) => Ok(Some(completion.clone())),
                _ => Err(conflict()),
            };
        }
        if proof.operation.not_after_ms <= now_ms
            || proof.operation.not_after_ms > now_ms.saturating_add(MAX_SUBMISSION_WINDOW_MS)
        {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "unseen operation submission deadline is expired or exceeds 24 hours",
            ));
        }
        if entries.len() >= OPERATION_QUOTA {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "operation epoch quota exhausted",
            ));
        }
        entries.insert(
            Self::key(proof),
            Entry::Send {
                proof: proof.clone(),
                completion: completion.clone(),
            },
        );
        Ok(None)
    }

    /// A rejected capacity admission has touched no source records. Uncertain
    /// I/O never calls this: its reservation remains fenced until replay.
    pub(crate) fn reject_before_effect(
        &self,
        proof: &OperationProof,
        completion: &Arc<Completion>,
    ) {
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let key = Self::key(proof);
        if entries
            .get(&key)
            .is_some_and(|entry| matches!(entry,Entry::Send {completion:existing,..} if Arc::ptr_eq(existing,completion)))
        {
            entries.remove(&key);
        }
    }

    /// Atomically reserves the shared scope/ID. The caller must move a new
    /// ticket and root-owning state into its worker without an intervening await.
    /// That worker takes the domain admission lock before evaluating state.
    pub fn begin_domain(
        self: &Arc<Self>,
        context: OperationContext,
        now_ms: u64,
    ) -> Result<DomainAdmission, BrokerError> {
        context.validate()?;
        if self
            .outcomes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_none()
        {
            return Err(BrokerError::application(
                ErrorCode::Forbidden,
                "resource operation outcomes are not activated",
            ));
        }
        let key = (context.scope.clone(), context.operation.id.clone());
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = entries.get(&key) {
            if entry.reference() != &context.operation {
                return Err(conflict());
            }
            self.validate_reference(&context.scope, &context.operation, now_ms)?;
            return match entry {
                Entry::Domain {
                    context: original,
                    completion,
                } if original == &context => {
                    Ok(DomainAdmission::Existing(DomainWaiter(completion.clone())))
                }
                _ => Err(conflict()),
            };
        }
        self.validate_reference(&context.scope, &context.operation, now_ms)?;
        if context.operation.not_after_ms <= now_ms
            || context.operation.not_after_ms > now_ms.saturating_add(MAX_SUBMISSION_WINDOW_MS)
        {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "unseen operation submission deadline is expired or exceeds 24 hours",
            ));
        }
        if entries.len() >= OPERATION_QUOTA {
            return Err(BrokerError::application(
                ErrorCode::ResourceExhausted,
                "operation epoch quota exhausted",
            ));
        }
        let completion = Arc::new(DomainCompletion::default());
        entries.insert(
            key,
            Entry::Domain {
                context: context.clone(),
                completion: completion.clone(),
            },
        );
        Ok(DomainAdmission::New(DomainTicket {
            context,
            completion,
            effect_attempted: AtomicBool::new(false),
            uncertain_maintenance: AtomicUsize::new(0),
            proof: Mutex::new(None),
        }))
    }

    /// Startup only, after the required resource-operation feature is durable.
    /// Replay and covering sync finish before any worker may be admitted.
    pub fn open_outcomes(&self, root: &Path, rotation_bytes: u64) -> Result<(), BrokerError> {
        let directory = root.join("operations");
        crate::arrivals::migration::durable_create_directory(&directory)?;
        // Existing directory names may be leftovers from a crash before the
        // creating process synced their parent; make recovery durable too.
        File::open(&directory)?.sync_all()?;
        File::open(root)?.sync_all()?;
        let path = directory.join("outcomes.jsonl");
        let mut slot = self.outcomes.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(BrokerError::internal(
                "operation outcome source already open",
            ));
        }
        let (log, report) = LogFile::<OutcomeRecord>::open_rotating(path, rotation_bytes)?;
        self.resource_recovery_enabled
            .store(true, Ordering::Release);
        let recovered = (|| -> Result<(), BrokerError> {
            for record in report.records {
                if record.proof.completion.is_some() {
                    return Err(BrokerError::internal(
                        "no-effect source contains a projection requirement",
                    ));
                }
                validate_terminal_outcome(&record.proof.context, &record.proof.outcome)?;
                self.recover_domain(&record.proof, record.proof.outcome.clone())?;
            }
            Ok(())
        })();
        if let Err(error) = recovered {
            self.resource_recovery_enabled
                .store(false, Ordering::Release);
            return Err(error);
        }
        *slot = Some(log);
        Ok(())
    }

    /// Append only while holding the same domain admission lock used to decide
    /// this no-effect outcome. Never call after any source-effect attempt.
    pub fn finish_no_effect(
        &self,
        ticket: &DomainTicket,
        mut outcome: OperationOutcome,
    ) -> Result<(), BrokerError> {
        if ticket.effect_attempted() {
            ticket.finish_unknown();
            return Err(BrokerError::internal(
                "cannot record a no-effect outcome after a source attempt",
            ));
        }
        {
            let entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
            let key = (
                ticket.context.scope.clone(),
                ticket.context.operation.id.clone(),
            );
            if !entries.get(&key).is_some_and(|entry| matches!(entry,Entry::Domain {completion,..} if Arc::ptr_eq(completion,&ticket.completion))) {
                return Err(BrokerError::internal("no-effect ticket does not belong to operation store"));
            }
        }
        if let OperationOutcome::Error(error) = &mut outcome {
            let code = crate::error::ErrorCode::from_rpc_code(error.code);
            if !matches!(
                code,
                Some(
                    ErrorCode::NotFound
                        | ErrorCode::ResourceExhausted
                        | ErrorCode::Conflict
                        | ErrorCode::Exists
                        | ErrorCode::VersionConflict
                        | ErrorCode::ConflictClaimed
                        | ErrorCode::LeaseLost
                )
            ) {
                return Err(BrokerError::internal(
                    "nonterminal error cannot be persisted as no-effect outcome",
                ));
            }
            let data = error
                .data
                .get_or_insert_with(|| json!({}))
                .as_object_mut()
                .ok_or_else(|| {
                    BrokerError::internal("terminal operation error data must be an object")
                })?;
            if data.contains_key("durable_operation") {
                return Err(BrokerError::internal(
                    "terminal operation error already contains a durability marker",
                ));
            }
            data.insert("durable_operation".into(), terminal_marker(&ticket.context));
        }
        validate_terminal_outcome(&ticket.context, &outcome)?;
        let proof = ticket.proof(outcome.clone())?;
        let mut record = OutcomeRecord {
            seq: 0,
            prev_hash: String::new(),
            ts: crate::id::now_rfc3339(),
            proof,
        };
        let mut slot = self.outcomes.lock().unwrap_or_else(|p| p.into_inner());
        let log = slot
            .as_mut()
            .ok_or_else(|| BrokerError::internal("operation outcome source is not open"))?;
        ticket.mark_effect_attempt();
        if let Err(error) = log.append(&mut record) {
            ticket.finish_unknown();
            return Err(error.into());
        }
        ticket.completion.finish(Ok(outcome));
        Ok(())
    }

    /// Source owners call this only after their own semantic checks and the
    /// source's covering sync; required derived records must also be complete.
    pub fn recover_domain(
        &self,
        proof: &DomainOperationProof,
        completed_outcome: OperationOutcome,
    ) -> Result<(), BrokerError> {
        if !self.resource_recovery_enabled.load(Ordering::Acquire) {
            return Err(BrokerError::internal(
                "resource operation proof requires its durable activation guard",
            ));
        }
        proof.verify(&proof.context.method, &proof.context.resource)?;
        proof.verify_completed(&completed_outcome)?;
        if proof.context.scope.bus_id != self.bus_id
            || self
                .epoch
                .as_ref()
                .is_none_or(|(epoch, _)| *epoch != proof.context.operation.epoch)
        {
            return Err(BrokerError::internal(
                "resource operation source requires its durable registry identity and epoch",
            ));
        }
        let key = (
            proof.context.scope.clone(),
            proof.context.operation.id.clone(),
        );
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if entries.contains_key(&key) {
            return Err(BrokerError::internal(
                "duplicate or conflicting authoritative operation proof",
            ));
        }
        if entries.len() >= OPERATION_QUOTA {
            return Err(BrokerError::internal(
                "operation source exceeds epoch quota",
            ));
        }
        let completion = Arc::new(DomainCompletion::default());
        completion.finish(Ok(completed_outcome));
        entries.insert(
            key,
            Entry::Domain {
                context: proof.context.clone(),
                completion,
            },
        );
        Ok(())
    }

    pub(crate) fn recover_inboxes(
        &self,
        inboxes: &HashMap<String, Inbox>,
    ) -> Result<(), BrokerError> {
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        for (agent_id, inbox) in inboxes {
            for envelope in &inbox.state.records {
                if envelope.domain_operation.is_some() {
                    return Err(BrokerError::internal(
                        "resource operation proof found in inbox source",
                    ));
                }
                let Some(proof) = &envelope.operation else {
                    continue;
                };
                if proof.scope.bus_id != self.bus_id
                    || self
                        .epoch
                        .as_ref()
                        .is_none_or(|(epoch, _)| *epoch != proof.operation.epoch)
                {
                    return Err(BrokerError::internal(
                        "operation source requires its durable registry feature and epoch",
                    ));
                }
                proof.verify(
                    envelope,
                    agent_id,
                    inbox.state.expires.get(&envelope.id).copied(),
                )?;
                if entries.contains_key(&Self::key(proof)) {
                    return Err(BrokerError::internal(
                        "duplicate or conflicting authoritative operation proof",
                    ));
                }
                if entries.len() >= OPERATION_QUOTA {
                    return Err(BrokerError::internal(
                        "operation source exceeds epoch quota",
                    ));
                }
                let completion = Arc::new(Completion::default());
                completion.finish(Ok(proof.result.clone()));
                entries.insert(
                    Self::key(proof),
                    Entry::Send {
                        proof: proof.clone(),
                        completion,
                    },
                );
            }
        }
        Ok(())
    }
}

fn conflict() -> BrokerError {
    BrokerError::application(
        ErrorCode::IdempotencyConflict,
        "operation identity conflicts with original input",
    )
}

fn terminal_marker(context: &OperationContext) -> Value {
    json!({"v":1,"operation":context.operation,"method":context.method,"state":"terminal_no_effect",
        "bus_id":context.scope.bus_id,"agent_id":context.scope.agent_id})
}

fn validate_terminal_outcome(
    context: &OperationContext,
    outcome: &OperationOutcome,
) -> Result<(), BrokerError> {
    if let OperationOutcome::Error(error) = outcome {
        let code = crate::error::ErrorCode::from_rpc_code(error.code);
        if !matches!(
            code,
            Some(
                ErrorCode::NotFound
                    | ErrorCode::ResourceExhausted
                    | ErrorCode::Conflict
                    | ErrorCode::Exists
                    | ErrorCode::VersionConflict
                    | ErrorCode::ConflictClaimed
                    | ErrorCode::LeaseLost
            )
        ) || error
            .data
            .as_ref()
            .and_then(|data| data.get("durable_operation"))
            != Some(&terminal_marker(context))
        {
            return Err(BrokerError::internal(
                "invalid authoritative terminal operation error",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DomainFixture {
        root: std::path::PathBuf,
        store: Arc<OperationStore>,
        bus: String,
        epoch: String,
        agent: String,
        now: u64,
    }

    impl DomainFixture {
        fn new() -> Self {
            let bus = crate::id::new_ulid().unwrap();
            let epoch = crate::id::new_ulid().unwrap();
            let agent = crate::id::new_ulid().unwrap();
            let now = crate::id::unix_millis();
            let root = std::env::temp_dir().join(format!(
                "coop-domain-core-{}",
                crate::id::new_ulid().unwrap()
            ));
            let store = Arc::new(OperationStore::new(
                bus.clone(),
                Some((epoch.clone(), now + 60_000)),
            ));
            store.open_outcomes(&root, 1024 * 1024).unwrap();
            Self {
                root,
                store,
                bus,
                epoch,
                agent,
                now,
            }
        }

        fn context(&self, method: &str) -> OperationContext {
            OperationContext {
                scope: OperationScope {
                    bus_id: self.bus.clone(),
                    principal_id: "device".into(),
                    agent_id: self.agent.clone(),
                },
                operation: OperationRef {
                    v: 1,
                    id: crate::id::new_ulid().unwrap(),
                    epoch: self.epoch.clone(),
                    not_after_ms: self.now + 30_000,
                },
                method: method.into(),
                resource: "queue:q".into(),
                input: json!({"queue":"q"}),
            }
        }

        fn ticket(&self, context: OperationContext) -> DomainTicket {
            match self.store.begin_domain(context, self.now).unwrap() {
                DomainAdmission::New(ticket) => ticket,
                DomainAdmission::Existing(_) => panic!("expected new operation"),
            }
        }

        fn reopened(&self) -> Arc<OperationStore> {
            let store = Arc::new(OperationStore::new(
                self.bus.clone(),
                Some((self.epoch.clone(), self.now + 60_000)),
            ));
            store.open_outcomes(&self.root, 1024 * 1024).unwrap();
            store
        }

        fn records(&self) -> Vec<OutcomeRecord> {
            LogFile::<OutcomeRecord>::read_segments(self.root.join("operations/outcomes.jsonl"))
                .unwrap()
                .records
        }
    }

    impl Drop for DomainFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn scoped_identity_conflicts_across_send_and_every_domain_method() {
        let fixture = DomainFixture::new();
        let send = proof(&fixture.bus, &fixture.epoch, &fixture.agent, fixture.now);
        fixture
            .store
            .reserve(&send, &Arc::new(Completion::default()), fixture.now)
            .unwrap();
        let mut context = fixture.context("create_queue");
        context.scope = send.scope.clone();
        context.operation = send.operation.clone();
        assert!(matches!(
            fixture.store.begin_domain(context, fixture.now),
            Err(BrokerError::Application(ErrorCode::IdempotencyConflict, _))
        ));

        let context = fixture.context("create_queue");
        let ticket = fixture.ticket(context.clone());
        let mut send = proof(&fixture.bus, &fixture.epoch, &fixture.agent, fixture.now);
        send.scope = context.scope.clone();
        send.operation = context.operation.clone();
        assert!(matches!(
            fixture
                .store
                .reserve(&send, &Arc::new(Completion::default()), fixture.now),
            Err(BrokerError::Application(ErrorCode::IdempotencyConflict, _))
        ));
        let mut changed = context.clone();
        changed.method = "enqueue".into();
        assert!(matches!(
            fixture.store.begin_domain(changed, fixture.now),
            Err(BrokerError::Application(ErrorCode::IdempotencyConflict, _))
        ));
        let metadata = fixture
            .store
            .metadata(&context.scope, &context.operation)
            .unwrap()
            .unwrap();
        assert_eq!(metadata.method, "create_queue");
        assert_eq!(metadata.resource, "queue:q");
        drop(ticket);
    }

    #[test]
    fn quota_is_shared_between_send_and_domain_reservations() {
        let fixture = DomainFixture::new();
        for index in 0..OPERATION_QUOTA {
            if index % 2 == 0 {
                let send = proof(&fixture.bus, &fixture.epoch, &fixture.agent, fixture.now);
                fixture
                    .store
                    .reserve(&send, &Arc::new(Completion::default()), fixture.now)
                    .unwrap();
            } else {
                // Dropping a newly admitted owner conservatively fences it;
                // uncertain reservations still consume the shared epoch budget.
                drop(fixture.ticket(fixture.context("create_queue")));
            }
        }
        assert!(matches!(
            fixture
                .store
                .begin_domain(fixture.context("create_queue"), fixture.now),
            Err(BrokerError::Application(ErrorCode::ResourceExhausted, _))
        ));
        let send = proof(&fixture.bus, &fixture.epoch, &fixture.agent, fixture.now);
        assert!(matches!(
            fixture
                .store
                .reserve(&send, &Arc::new(Completion::default()), fixture.now),
            Err(BrokerError::Application(ErrorCode::ResourceExhausted, _))
        ));
        assert!(
            fixture.records().is_empty(),
            "quota checks did not invent source outcomes"
        );
    }

    #[tokio::test]
    async fn terminal_no_effect_error_replays_exactly_with_bound_marker_and_scoped_status() {
        let fixture = DomainFixture::new();
        let context = fixture.context("claim_task");
        let ticket = fixture.ticket(context.clone());
        let waiter = ticket.waiter();
        fixture
            .store
            .finish_no_effect(
                &ticket,
                OperationOutcome::Error(RpcError::application(
                    ErrorCode::NotFound,
                    "queue q has no claimable task",
                )),
            )
            .unwrap();
        let original = waiter.wait().await.unwrap_err();
        assert_eq!(
            original.data.as_ref().unwrap()["durable_operation"],
            terminal_marker(&context)
        );
        assert_eq!(fixture.records().len(), 1);
        assert_eq!(
            fixture.records()[0].proof.outcome,
            OperationOutcome::Error(original.clone())
        );
        assert_eq!(
            fixture
                .store
                .status(&context.scope, &context.operation)
                .unwrap(),
            json!({"state":"committed","error":original})
        );
        let mut other = context.scope.clone();
        other.agent_id = crate::id::new_ulid().unwrap();
        assert_eq!(
            fixture.store.status(&other, &context.operation).unwrap(),
            json!({"state":"unseen"})
        );
        assert_eq!(
            fixture.store.metadata(&other, &context.operation).unwrap(),
            None
        );
        drop(ticket);

        let reopened = fixture.reopened();
        let DomainAdmission::Existing(waiter) =
            reopened.begin_domain(context, fixture.now).unwrap()
        else {
            panic!("replay must find original outcome")
        };
        assert_eq!(waiter.wait().await.unwrap_err(), original);
        assert_eq!(fixture.records().len(), 1);
    }

    #[tokio::test]
    async fn dropped_owner_fences_until_authoritative_reopen_and_never_claims_terminal_error() {
        let fixture = DomainFixture::new();
        let context = fixture.context("create_queue");
        let ticket = fixture.ticket(context.clone());
        let waiter = ticket.waiter();
        drop(ticket);
        let error = waiter.wait().await.unwrap_err();
        assert_eq!(error.code, ErrorCode::OutcomeUnknown.rpc_code());
        assert!(error.data.unwrap().get("durable_operation").is_none());
        assert_eq!(
            fixture
                .store
                .status(&context.scope, &context.operation)
                .unwrap(),
            json!({"state":"unknown"})
        );
        let DomainAdmission::Existing(waiter) = fixture
            .store
            .begin_domain(context.clone(), fixture.now)
            .unwrap()
        else {
            panic!("uncertainty reservation disappeared")
        };
        assert_eq!(
            waiter.wait().await.unwrap_err().code,
            ErrorCode::OutcomeUnknown.rpc_code()
        );
        assert!(fixture.records().is_empty());
        assert!(matches!(
            fixture
                .reopened()
                .begin_domain(context, fixture.now)
                .unwrap(),
            DomainAdmission::New(_)
        ));
    }

    #[tokio::test]
    async fn source_completion_checks_original_result_and_projection_recipe() {
        let fixture = DomainFixture::new();
        let mut context = fixture.context("fail_task");
        context.input = json!({"lease_id":"lease","fence":1,"reason":"failed"});
        let ticket = fixture.ticket(context.clone());
        let task_id = crate::id::new_ulid().unwrap();
        let template =
            json!({"queue":"q","task_id":task_id,"status":"dead","dead_letter_seq":null});
        let proof = ticket
            .proof_with_completion(
                OperationOutcome::Success(template.clone()),
                CompletionRequirement::DeadLetter {
                    queue: "q".into(),
                    task_id,
                },
            )
            .unwrap();
        ticket.mark_effect_attempt();
        let mut incorrect = template.clone();
        incorrect["status"] = json!("changed");
        incorrect["dead_letter_seq"] = json!(1);
        assert!(ticket.finish_success(incorrect).is_err());
        assert!(fixture
            .store
            .finish_no_effect(
                &ticket,
                OperationOutcome::Error(RpcError::application(ErrorCode::NotFound, "after effect"))
            )
            .is_err());
        assert!(fixture.records().is_empty());
        // A projection error has already fenced this live ticket. Replay can
        // publish only the exact source template plus the verified dead seq.
        let mut completed = template;
        completed["dead_letter_seq"] = json!(7);
        let reopened = fixture.reopened();
        reopened
            .recover_domain(&proof, OperationOutcome::Success(completed.clone()))
            .unwrap();
        assert!(
            reopened
                .recover_domain(&proof, OperationOutcome::Success(completed.clone()))
                .is_err(),
            "duplicate source proof rejected"
        );
        let DomainAdmission::Existing(waiter) =
            reopened.begin_domain(context, fixture.now).unwrap()
        else {
            panic!("missing recovered proof")
        };
        assert_eq!(waiter.wait().await.unwrap(), completed);
        let disabled = OperationStore::new(
            fixture.bus.clone(),
            Some((fixture.epoch.clone(), fixture.now + 60_000)),
        );
        assert!(
            disabled
                .recover_domain(&proof, OperationOutcome::Success(json!({})))
                .is_err(),
            "resource guard is required before recovery"
        );
    }

    #[tokio::test]
    async fn oversized_result_is_rejected_before_effect_and_can_be_recorded_terminally() {
        let fixture = DomainFixture::new();
        let ticket = fixture.ticket(fixture.context("claim_task"));
        let error = ticket
            .proof(OperationOutcome::Success(
                json!({"payload":"x".repeat(crate::transport::MAX_RPC_BYTES)}),
            ))
            .unwrap_err();
        assert!(matches!(
            error,
            BrokerError::Application(ErrorCode::ResourceExhausted, _)
        ));
        assert!(!ticket.effect_attempted());
        fixture
            .store
            .finish_no_effect(&ticket, OperationOutcome::Error(error.into_rpc_error()))
            .unwrap();
        assert_eq!(
            ticket.waiter().wait().await.unwrap_err().code,
            ErrorCode::ResourceExhausted.rpc_code()
        );
        assert_eq!(fixture.records().len(), 1);
        let mut context = fixture.context("enqueue");
        context.input = json!({"payload":"x".repeat(MAX_DOMAIN_INPUT_BYTES)});
        assert!(matches!(
            fixture.store.begin_domain(context, fixture.now),
            Err(BrokerError::Application(ErrorCode::ResourceExhausted, _))
        ));
    }

    #[tokio::test]
    async fn completed_maintenance_allows_terminal_no_effect_but_cannot_clear_other_uncertainty() {
        let fixture = DomainFixture::new();
        let ticket = fixture.ticket(fixture.context("claim_task"));
        let maintenance = ticket.begin_maintenance();
        assert!(ticket.effect_attempted());
        maintenance.complete();
        assert!(!ticket.effect_attempted());
        fixture
            .store
            .finish_no_effect(
                &ticket,
                OperationOutcome::Error(RpcError::application(
                    ErrorCode::NotFound,
                    "empty after expiry",
                )),
            )
            .unwrap();
        let terminal = ticket.waiter().wait().await.unwrap_err();
        assert_eq!(terminal.code, ErrorCode::NotFound.rpc_code());
        assert!(terminal.data.unwrap().get("durable_operation").is_some());

        let uncertain = fixture.ticket(fixture.context("claim_task"));
        let failed = uncertain.begin_maintenance();
        let succeeded = uncertain.begin_maintenance();
        drop(failed);
        succeeded.complete();
        assert!(
            uncertain.effect_attempted(),
            "a successful guard cannot clear another guard's failed I/O"
        );
        assert!(fixture
            .store
            .finish_no_effect(
                &uncertain,
                OperationOutcome::Error(RpcError::application(
                    ErrorCode::NotFound,
                    "unproven empty"
                ))
            )
            .is_err());
        assert_eq!(
            uncertain.waiter().wait().await.unwrap_err().code,
            ErrorCode::OutcomeUnknown.rpc_code()
        );
        assert_eq!(
            fixture.records().len(),
            1,
            "uncertain maintenance did not produce a terminal source outcome"
        );

        let primary = fixture.ticket(fixture.context("create_queue"));
        primary.mark_effect_attempt();
        primary.begin_maintenance().complete();
        assert!(
            primary.effect_attempted(),
            "successful maintenance cannot clear an attempted primary effect"
        );
    }

    #[cfg(feature = "mbt-injection")]
    #[tokio::test]
    async fn uncertain_outcome_sync_and_recovery_sync_failure_do_not_expose_terminal_result() {
        use crate::faults::{arm_append, arm_point, AppendFault, FaultPoint, PointFault};
        let fixture = DomainFixture::new();
        let context = fixture.context("claim_task");
        let ticket = fixture.ticket(context.clone());
        let path = fixture.root.join("operations/outcomes.jsonl");
        let fail = arm_append(&path, AppendFault::FailSync).unwrap();
        assert!(fixture
            .store
            .finish_no_effect(
                &ticket,
                OperationOutcome::Error(RpcError::application(ErrorCode::NotFound, "empty"))
            )
            .is_err());
        assert_eq!(
            ticket.waiter().wait().await.unwrap_err().code,
            ErrorCode::OutcomeUnknown.rpc_code()
        );
        assert_eq!(
            fixture.records().len(),
            1,
            "full uncertain line exists but has not been published"
        );
        drop(fail);
        let recovery = arm_point(&path, FaultPoint::BeforeRecoverySync, PointFault::Fail).unwrap();
        let blocked = Arc::new(OperationStore::new(
            fixture.bus.clone(),
            Some((fixture.epoch.clone(), fixture.now + 60_000)),
        ));
        assert!(blocked.open_outcomes(&fixture.root, 1024 * 1024).is_err());
        assert!(blocked.begin_domain(context.clone(), fixture.now).is_err());
        assert_eq!(
            blocked.status(&context.scope, &context.operation).unwrap(),
            json!({"state":"unseen"})
        );
        drop(recovery);
        let reopened = fixture.reopened();
        let DomainAdmission::Existing(waiter) =
            reopened.begin_domain(context.clone(), fixture.now).unwrap()
        else {
            panic!("full source outcome must recover")
        };
        let error = waiter.wait().await.unwrap_err();
        assert_eq!(
            error.data.unwrap()["durable_operation"],
            terminal_marker(&context)
        );
    }

    fn proof(bus_id: &str, epoch: &str, agent_id: &str, now_ms: u64) -> OperationProof {
        OperationProof {
            scope: OperationScope {
                bus_id: bus_id.into(),
                principal_id: "device".into(),
                agent_id: agent_id.into(),
            },
            operation: OperationRef {
                v: 1,
                id: crate::id::new_ulid().unwrap(),
                epoch: epoch.into(),
                not_after_ms: now_ms + 1000,
            },
            digest: semantic_digest("send_message", &json!({"value":1})),
            method: "send_message".into(),
            resource: "inbox:recipient".into(),
            ttl_ms: None,
            expires_at_ms: None,
            result: SendReceipt {
                message_id: crate::id::new_ulid().unwrap(),
                seq: 1,
                duplicate: false,
                to: "recipient".into(),
                to_agent_id: "recipient".into(),
                idempotency_key: None,
                ts: Some(crate::id::now_rfc3339()),
            },
        }
    }

    #[test]
    fn canonical_inputs_sort_objects_but_keep_array_order_numbers_and_methods() {
        let a: Value = serde_json::from_str(r#"{"z":[1,2],"a":{"y":1,"x":2}}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"a":{"x":2,"y":1},"z":[1,2]}"#).unwrap();
        assert_eq!(semantic_digest("method", &a), semantic_digest("method", &b));
        assert_ne!(
            semantic_digest("method", &a),
            semantic_digest("different", &a)
        );
        assert_ne!(
            semantic_digest("method", &json!([1, 2])),
            semantic_digest("method", &json!([2, 1]))
        );
        assert_ne!(
            semantic_digest("method", &json!(1)),
            semantic_digest("method", &json!(1.0))
        );
    }

    #[test]
    fn finite_quota_rejects_new_admission_but_preserves_existing_completion() {
        let (bus, epoch, agent) = (
            crate::id::new_ulid().unwrap(),
            crate::id::new_ulid().unwrap(),
            crate::id::new_ulid().unwrap(),
        );
        let now = crate::id::unix_millis();
        let store = OperationStore::new(bus.clone(), Some((epoch.clone(), now + 60_000)));
        let completion = Arc::new(Completion::default());
        let first = proof(&bus, &epoch, &agent, now);
        assert!(store.reserve(&first, &completion, now).unwrap().is_none());
        for _ in 1..OPERATION_QUOTA {
            let item = proof(&bus, &epoch, &agent, now);
            store
                .reserve(&item, &Arc::new(Completion::default()), now)
                .unwrap();
        }
        let extra = proof(&bus, &epoch, &agent, now);
        assert!(matches!(
            store.reserve(&extra, &Arc::new(Completion::default()), now),
            Err(BrokerError::Application(ErrorCode::ResourceExhausted, _))
        ));
        assert!(
            Arc::ptr_eq(
                &completion,
                &store
                    .reserve(&first, &Arc::new(Completion::default()), now + 2000)
                    .unwrap()
                    .unwrap()
            ),
            "known reference ignores expired submission deadline"
        );
        let mut changed = first.clone();
        changed.method = "other".into();
        assert!(matches!(
            store.lookup(&changed, now),
            Err(BrokerError::Application(ErrorCode::IdempotencyConflict, _))
        ));
        assert!(
            store.lookup(&first, now + 60_000).is_err(),
            "expired epoch rejects old receipt access"
        );
    }

    #[test]
    fn status_distinguishes_shared_pending_unknown_and_committed_without_scope_leaks() {
        let (bus, epoch, agent) = (
            crate::id::new_ulid().unwrap(),
            crate::id::new_ulid().unwrap(),
            crate::id::new_ulid().unwrap(),
        );
        let now = crate::id::unix_millis();
        let store = OperationStore::new(bus.clone(), Some((epoch.clone(), now + 60_000)));
        let item = proof(&bus, &epoch, &agent, now);
        assert_eq!(
            store.status(&item.scope, &item.operation).unwrap(),
            json!({"state":"unseen"})
        );
        let completion = Arc::new(Completion::default());
        store.reserve(&item, &completion, now).unwrap();
        assert_eq!(
            store.status(&item.scope, &item.operation).unwrap(),
            json!({"state":"pending"})
        );
        let mut other_scope = item.scope.clone();
        other_scope.principal_id = "other".into();
        assert_eq!(
            store.status(&other_scope, &item.operation).unwrap(),
            json!({"state":"unseen"})
        );
        completion.finish(Err(BrokerError::application(
            ErrorCode::OutcomeUnknown,
            "sync uncertain",
        )));
        assert_eq!(
            store.status(&item.scope, &item.operation).unwrap(),
            json!({"state":"unknown"})
        );
        let committed = proof(&bus, &epoch, &agent, now);
        let completion = Arc::new(Completion::default());
        store.reserve(&committed, &completion, now).unwrap();
        completion.finish(Ok(committed.result.clone()));
        assert_eq!(
            store
                .status(&committed.scope, &committed.operation)
                .unwrap(),
            json!({"state":"committed","result":committed.result})
        );
    }
}
