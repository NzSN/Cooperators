//! Versioned documents with compare-and-swap updates and exclusive edit
//! leases (design §8).
//!
//! A document is one append-only `docs/<name>.jsonl` log of [`DocEvent`]s.
//! The log is the provenance record: version numbers are monotonic per
//! document and every mutation carries actor, timestamp, sequence, and
//! `prev_hash`.
//!
//! Bodies up to 64 KiB are inline; larger bodies are written once to
//! `blobs/sha256/<hex>` and referenced from the log by digest and size.
//!
//! Two write paths share one fence namespace: an unclaimed document accepts
//! optimistic updates (`base_version == head`), while a claimed document
//! accepts only the holder presenting the current `lease_id` and `fence`.
//! Expiry and release append events whose sequence becomes the new fence, so
//! a stale holder's write is rejected with `LEASE_LOST`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::arrivals::{
    ArrivalKind, ArrivalReference, ArrivalResource, CatchUpSnapshot, NotificationIntent,
    SourceCoordinate,
};
use crate::envelope::Body;
use crate::error::{BrokerError, ErrorCode};
use crate::id;
use crate::messaging::DAEMON_SENDER;
use crate::queues::{RenewLeaseReceipt, MAX_LEASE_TTL_S};
use crate::registry::AgentIdentity;
use crate::server::DaemonState;
use crate::storage::{sha256_hex, ChainRecord, LogFile};

/// Inline body limit: 64 KiB. Larger bodies spill to a content-addressed blob.
pub const INLINE_BODY_LIMIT: usize = 64 * 1024;

/// One document-log record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocEvent {
    pub seq: u64,
    pub prev_hash: String,
    pub ts: String,
    /// `doc_published`, `doc_updated`, `doc_claimed`, `doc_lease_renewed`,
    /// `doc_released`, `doc_claim_expired`, or `recovery`.
    pub kind: String,
    pub doc: String,
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub previous_version: Option<u64>,
    pub actor_id: String,
    pub actor_name: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub mime: Option<String>,
    /// Present when the body is inline (at most 64 KiB).
    #[serde(default)]
    pub body: Option<Body>,
    /// Present when the body lives in `blobs/sha256/<sha256>`.
    #[serde(default)]
    pub blob: Option<BlobRef>,
    #[serde(default)]
    pub lease_id: Option<String>,
    #[serde(default)]
    pub fence: Option<u64>,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
    #[serde(default)]
    pub lease_ttl_s: Option<u64>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub truncated_bytes: Option<u64>,
    /// Recipients captured under the source mutation order, before commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification_intent: Option<NotificationIntent>,
}

impl ChainRecord for DocEvent {
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

/// Content-addressed body reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    /// Lowercase hex SHA-256 of the body bytes.
    pub sha256: String,
    /// Body length in bytes.
    pub size: u64,
}

/// One immutable document version.
#[derive(Debug, Clone)]
pub(crate) struct DocVersion {
    pub version: u64,
    pub title: String,
    pub mime: String,
    pub body: Option<Body>,
    pub blob: Option<BlobRef>,
}

/// Active edit lease on a document.
#[derive(Debug, Clone)]
pub(crate) struct DocClaim {
    pub lease_id: String,
    pub fence: u64,
    pub agent_id: String,
    pub agent_name: String,
    pub expires_at_ms: u64,
    pub ttl_s: u64,
    pub claimed_at: String,
    pub head_version: u64,
}

/// Replay-derived document state.
#[derive(Debug, Clone)]
pub(crate) struct DocRuntime {
    pub versions: BTreeMap<u64, DocVersion>,
    pub head: u64,
    pub title: String,
    pub created_seq: u64,
    pub updated_at: String,
    /// Current claim; `None` also means released or expired.
    pub claim: Option<DocClaim>,
    /// Latest claim/expiry/release sequence. A stale holder's fence is lower.
    pub fence: u64,
    /// Pins the current immutable version to the record that published it.
    pub arrival_snapshot: Option<CatchUpSnapshot>,
}

impl DocRuntime {
    fn empty() -> Self {
        Self {
            versions: BTreeMap::new(),
            head: 0,
            title: String::new(),
            created_seq: 0,
            updated_at: String::new(),
            claim: None,
            fence: 0,
            arrival_snapshot: None,
        }
    }

    fn head_version(&self) -> Option<&DocVersion> {
        self.versions.get(&self.head)
    }
}

/// Receipt returned by `publish_doc` and `update_doc`.
#[derive(Debug, Clone, Serialize)]
pub struct DocWriteReceipt {
    pub name: String,
    pub version: u64,
    pub head_version: u64,
    pub title: String,
    pub mime: String,
    /// `inline` or `blob`.
    pub storage: String,
    pub blob: Option<BlobRef>,
    pub seq: u64,
    pub ts: String,
}

/// Claim state exposed by `read_doc` and `list_docs`.
#[derive(Debug, Clone, Serialize)]
pub struct DocClaimView {
    pub holder: DocHolder,
    pub fence: u64,
    pub head_version: u64,
    pub expires_at: String,
    pub lease_ttl_s: u64,
    pub claimed_at: String,
}

/// Identity of a claim holder.
#[derive(Debug, Clone, Serialize)]
pub struct DocHolder {
    pub agent_id: String,
    pub name: String,
}

/// Result of `read_doc`.
#[derive(Debug, Clone, Serialize)]
pub struct DocReadResponse {
    pub name: String,
    pub version: u64,
    pub head_version: u64,
    pub title: String,
    pub mime: String,
    pub body: Body,
    /// `inline` or `blob`.
    pub storage: String,
    pub blob: Option<BlobRef>,
    pub claim: Option<DocClaimView>,
    /// Latest claim/expiry/release sequence (0 before the first claim).
    pub fence: u64,
    pub versions: usize,
    pub updated_at: String,
}

/// Result of `claim_doc`.
#[derive(Debug, Clone, Serialize)]
pub struct ClaimDocReceipt {
    pub name: String,
    pub lease_id: String,
    pub fence: u64,
    pub head_version: u64,
    pub expires_at: String,
    pub lease_ttl_s: u64,
}

/// Result of `release_doc`.
#[derive(Debug, Clone, Serialize)]
pub struct ReleaseDocReceipt {
    pub name: String,
    pub lease_id: String,
    pub fence: u64,
    pub new_fence: u64,
    pub released: bool,
    pub seq: u64,
}

/// One `list_docs` row.
#[derive(Debug, Clone, Serialize)]
pub struct DocSummary {
    pub name: String,
    pub head_version: u64,
    pub title: String,
    pub mime: String,
    pub versions: usize,
    pub updated_at: String,
    pub fence: u64,
    pub claim: Option<DocClaimView>,
}

/// Result of `list_docs`.
#[derive(Debug, Clone, Serialize)]
pub struct ListDocsResponse {
    pub prefix: Option<String>,
    pub docs: Vec<DocSummary>,
}

/// Validates a document name before it is used as a file stem.
pub fn validate_doc_name(name: &str) -> Result<(), BrokerError> {
    if name.is_empty() || name.len() > 128 {
        return Err(BrokerError::invalid("doc name must be 1..=128 bytes"));
    }
    if name.starts_with('.') || name.contains("..") {
        return Err(BrokerError::invalid(
            "doc name must not contain path components",
        ));
    }
    if name.contains('@') {
        return Err(BrokerError::invalid(
            "doc name must not contain '@'; append '@version' only in references",
        ));
    }
    if !name.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':')
    }) {
        return Err(BrokerError::invalid(
            "doc name may contain only ASCII letters, digits, '-', '_', '.', and ':'",
        ));
    }
    Ok(())
}

fn doc_path(root: &Path, name: &str) -> PathBuf {
    root.join("docs").join(format!("{name}.jsonl"))
}

fn blob_path(root: &Path, digest: &str) -> PathBuf {
    root.join("blobs").join("sha256").join(digest)
}

fn base_event(kind: &str, doc: &str, identity: &AgentIdentity) -> DocEvent {
    DocEvent {
        seq: 0,
        prev_hash: String::new(),
        ts: id::now_rfc3339(),
        kind: kind.to_string(),
        doc: doc.to_string(),
        version: None,
        previous_version: None,
        actor_id: identity.agent_id.clone(),
        actor_name: identity.name.clone(),
        title: None,
        mime: None,
        body: None,
        blob: None,
        lease_id: None,
        fence: None,
        expires_at_ms: None,
        lease_ttl_s: None,
        reason: None,
        truncated_bytes: None,
        notification_intent: None,
    }
}

/// Writes body text to a content-addressed blob and returns its reference.
fn store_blob(root: &Path, text: &str) -> Result<BlobRef, BrokerError> {
    let bytes = text.as_bytes();
    let digest = sha256_hex(bytes);
    let path = blob_path(root, &digest);
    if path.exists() {
        return Ok(BlobRef {
            sha256: digest,
            size: bytes.len() as u64,
        });
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_file_name(format!(
        ".tmp-{}-{}",
        std::process::id(),
        id::new_ulid().map_err(|error| BrokerError::internal(format!("temp name: {error}")))?
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &path)?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(BlobRef {
        sha256: digest,
        size: bytes.len() as u64,
    })
}

/// Reads and verifies a blob referenced by a document event.
fn load_blob(root: &Path, blob: &BlobRef) -> Result<String, BrokerError> {
    load_blob_limited(root, blob, usize::MAX)
}

fn load_blob_limited(root: &Path, blob: &BlobRef, maximum: usize) -> Result<String, BrokerError> {
    use std::io::Read;
    let file = fs::File::open(blob_path(root, &blob.sha256))?;
    if blob.size > maximum as u64 || file.metadata()?.len() > maximum as u64 {
        return Err(BrokerError::application(
            ErrorCode::ResourceExhausted,
            "document body exceeds remote budget",
        ));
    }
    let mut bytes = Vec::new();
    file.take((maximum as u64).saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(BrokerError::application(
            ErrorCode::ResourceExhausted,
            "document body exceeds remote budget",
        ));
    }
    if bytes.len() as u64 != blob.size {
        return Err(BrokerError::internal(format!(
            "blob {} size mismatch: expected {}, found {}",
            blob.sha256,
            blob.size,
            bytes.len()
        )));
    }
    let actual = sha256_hex(&bytes);
    if actual != blob.sha256 {
        return Err(BrokerError::internal(format!(
            "blob digest mismatch: expected {}, found {actual}",
            blob.sha256
        )));
    }
    String::from_utf8(bytes)
        .map_err(|error| BrokerError::internal(format!("blob is not UTF-8: {error}")))
}

fn encode_body(
    root: &Path,
    body: &Body,
) -> Result<(Option<Body>, Option<BlobRef>, &'static str), BrokerError> {
    if body.text.len() <= INLINE_BODY_LIMIT {
        Ok((Some(body.clone()), None, "inline"))
    } else {
        let blob = store_blob(root, &body.text)?;
        Ok((None, Some(blob), "blob"))
    }
}

fn open_doc(
    root: &Path,
    name: &str,
    rotation_bytes: u64,
) -> Result<(LogFile<DocEvent>, DocRuntime), BrokerError> {
    validate_doc_name(name)?;
    let path = doc_path(root, name);
    let (mut log, report) =
        LogFile::<DocEvent>::open_rotating(&path, rotation_bytes).map_err(BrokerError::from)?;
    let truncated_bytes = report.truncated_bytes;
    let last_valid_offset = report.last_valid_offset;
    let was_truncated = report.was_truncated();
    let mut runtime = DocRuntime::empty();
    for (index, event) in report.records.iter().enumerate() {
        // Replay already verified every hash. Reuse the following link (or
        // committed head) rather than hashing retained bodies a second time.
        let source_hash = report
            .records
            .get(index + 1)
            .map(|next| next.prev_hash.as_str())
            .unwrap_or(&report.last_hash);
        apply_event(&mut runtime, event, source_hash)?;
    }
    if was_truncated {
        let mut recovery = base_event(
            "recovery",
            name,
            &AgentIdentity {
                agent_id: String::new(),
                name: DAEMON_SENDER.to_string(),
                kind: "daemon".to_string(),
                capabilities: Vec::new(),
            },
        );
        recovery.truncated_bytes = Some(truncated_bytes);
        recovery.reason = Some(format!(
            "truncated_bytes={truncated_bytes}, last_valid_offset={last_valid_offset}"
        ));
        log.append(&mut recovery)?;
    }
    Ok((log, runtime))
}

/// Loads every document log at daemon startup.
pub(crate) fn load_all_docs(
    root: &Path,
    rotation_bytes: u64,
) -> Result<
    (
        HashMap<String, LogFile<DocEvent>>,
        HashMap<String, DocRuntime>,
    ),
    BrokerError,
> {
    let directory = root.join("docs");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((HashMap::new(), HashMap::new()))
        }
        Err(error) => return Err(error.into()),
    };
    let mut names = BTreeSet::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
                names.insert(stem.to_string());
            }
        }
    }
    // A crash after sealing can leave no active file. Discover the logical
    // stream from its segment directory as well, without treating an empty
    // segment directory as a resource.
    match fs::read_dir(directory.join(".segments")) {
        Ok(segments) => {
            for entry in segments {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let logical = directory.join(entry.file_name());
                if logical.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
                    && !crate::storage::segment_paths(&logical)?.is_empty()
                {
                    if let Some(stem) = logical.file_stem().and_then(|stem| stem.to_str()) {
                        names.insert(stem.to_string());
                    }
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut logs = HashMap::new();
    let mut states = HashMap::new();
    for name in names {
        if validate_doc_name(&name).is_err() {
            continue;
        }
        let (log, runtime) = open_doc(root, &name, rotation_bytes)?;
        logs.insert(name.clone(), log);
        states.insert(name, runtime);
    }
    Ok((logs, states))
}

fn apply_event(
    runtime: &mut DocRuntime,
    event: &DocEvent,
    source_hash: &str,
) -> Result<(), BrokerError> {
    match event.kind.as_str() {
        "doc_published" | "doc_updated" => {
            let version_number = event
                .version
                .ok_or_else(|| BrokerError::internal(format!("{} without version", event.kind)))?;
            let mime = event
                .mime
                .clone()
                .unwrap_or_else(|| "text/plain".to_string());
            let version = DocVersion {
                version: version_number,
                title: event.title.clone().unwrap_or_default(),
                mime: mime.clone(),
                body: event.body.clone(),
                blob: event.blob.clone(),
            };
            runtime.versions.insert(version_number, version);
            if version_number >= runtime.head {
                runtime.head = version_number;
                runtime.title = event.title.clone().unwrap_or_default();
                runtime.updated_at = event.ts.clone();
                runtime.arrival_snapshot = Some(CatchUpSnapshot {
                    source: SourceCoordinate {
                        stream: "document".to_string(),
                        name: event.doc.clone(),
                        seq: event.seq,
                        hash: source_hash.to_string(),
                    },
                    resource: ArrivalReference::Document {
                        name: event.doc.clone(),
                        version: version_number,
                    },
                    kind: if event.kind == "doc_published" {
                        ArrivalKind::DocPublished
                    } else {
                        ArrivalKind::DocUpdated
                    },
                });
            }
            if runtime.created_seq == 0 && event.kind == "doc_published" {
                runtime.created_seq = event.seq;
            }
        }
        "doc_claimed" => {
            let head_version = event.version.unwrap_or(runtime.head);
            runtime.claim = Some(DocClaim {
                lease_id: event.lease_id.clone().unwrap_or_default(),
                fence: event.seq,
                agent_id: event.actor_id.clone(),
                agent_name: event.actor_name.clone(),
                expires_at_ms: event.expires_at_ms.unwrap_or(0),
                ttl_s: event.lease_ttl_s.unwrap_or(0),
                claimed_at: event.ts.clone(),
                head_version,
            });
            runtime.fence = event.seq;
        }
        "doc_lease_renewed" => {
            if let Some(claim) = &mut runtime.claim {
                if Some(claim.lease_id.as_str()) == event.lease_id.as_deref() {
                    claim.expires_at_ms = event.expires_at_ms.unwrap_or(claim.expires_at_ms);
                }
            }
        }
        "doc_released" | "doc_claim_expired" => {
            runtime.claim = None;
            // Expiry and release advance the fence, so a stale holder's
            // subsequent write cannot match.
            runtime.fence = event.seq;
        }
        "recovery" => {}
        other => {
            return Err(BrokerError::internal(format!(
                "unknown doc event kind {other:?}"
            )))
        }
    }
    Ok(())
}

fn claim_view(claim: &DocClaim) -> DocClaimView {
    DocClaimView {
        holder: DocHolder {
            agent_id: claim.agent_id.clone(),
            name: claim.agent_name.clone(),
        },
        fence: claim.fence,
        head_version: claim.head_version,
        expires_at: id::format_rfc3339_millis(claim.expires_at_ms),
        lease_ttl_s: claim.ttl_s,
        claimed_at: claim.claimed_at.clone(),
    }
}

fn conflict_claimed(claim: &DocClaim, head_version: u64) -> BrokerError {
    BrokerError::application_with_data(
        ErrorCode::ConflictClaimed,
        format!(
            "document is claimed by {} until {}",
            claim.agent_name,
            id::format_rfc3339_millis(claim.expires_at_ms)
        ),
        json!({
            "holder": {
                "agent_id": claim.agent_id,
                "name": claim.agent_name,
            },
            // Flattened aliases make the conflict easy to consume from
            // adapters that do not want to inspect a nested object.
            "holder_agent_id": claim.agent_id,
            "holder_name": claim.agent_name,
            "expires_at": id::format_rfc3339_millis(claim.expires_at_ms),
            "expires_at_ms": claim.expires_at_ms,
            "fence": claim.fence,
            "head_version": head_version,
        }),
    )
}

impl DaemonState {
    /// Captures the current version while the caller holds `send_lock`.
    /// Claim/release events do not change this immutable content reference.
    pub(crate) async fn document_arrival_snapshot(
        &self,
        name: &str,
    ) -> Result<Option<CatchUpSnapshot>, BrokerError> {
        if !self.ensure_doc_loaded(name).await? {
            return Ok(None);
        }
        Ok(self
            .doc_states
            .lock()
            .await
            .get(name)
            .and_then(|runtime| runtime.arrival_snapshot.clone()))
    }

    fn capture_document_arrival_routes(
        &self,
        name: &str,
        kind: ArrivalKind,
    ) -> Result<Option<NotificationIntent>, BrokerError> {
        match &self.arrivals {
            Some(arrivals) => arrivals.capture_routes(
                &ArrivalResource::Document {
                    name: name.to_string(),
                },
                kind,
            ),
            None => Ok(None),
        }
    }

    fn announce_document_commit(&self, name: &str, seq: u64, hash: &str) {
        if let Some(arrivals) = &self.arrivals {
            arrivals.source_committed(SourceCoordinate {
                stream: "document".to_string(),
                name: name.to_string(),
                seq,
                hash: hash.to_string(),
            });
        }
    }

    async fn ensure_doc_loaded(&self, name: &str) -> Result<bool, BrokerError> {
        validate_doc_name(name)?;
        if self.doc_states.lock().await.contains_key(name) {
            return Ok(true);
        }
        if crate::storage::segment_paths(doc_path(&self.root, name))?.is_empty() {
            return Ok(false);
        }
        let (log, runtime) = open_doc(&self.root, name, self.config.log_rotation_bytes)?;
        self.docs.lock().await.insert(name.to_string(), log);
        self.doc_states
            .lock()
            .await
            .insert(name.to_string(), runtime);
        Ok(true)
    }

    /// Expires one overdue document claim. The caller holds `send_lock`.
    fn expire_doc_claim(
        &self,
        name: &str,
        log: &mut LogFile<DocEvent>,
        runtime: &mut DocRuntime,
        now_ms: u64,
    ) -> Result<bool, BrokerError> {
        let Some(claim) = runtime.claim.clone() else {
            return Ok(false);
        };
        if claim.expires_at_ms > now_ms {
            return Ok(false);
        }
        let mut event = base_event(
            "doc_claim_expired",
            name,
            &AgentIdentity {
                agent_id: String::new(),
                name: DAEMON_SENDER.to_string(),
                kind: "daemon".to_string(),
                capabilities: Vec::new(),
            },
        );
        event.lease_id = Some(claim.lease_id.clone());
        event.fence = Some(claim.fence);
        event.expires_at_ms = Some(claim.expires_at_ms);
        event.lease_ttl_s = Some(claim.ttl_s);
        event.reason = Some("lease_expired".to_string());
        event.version = Some(runtime.head);
        let receipt = log.append(&mut event)?;
        self.announce_document_commit(&name, receipt.seq, &receipt.hash);
        apply_event(runtime, &event, &receipt.hash)?;
        Ok(true)
    }

    /// Publishes version 1 of a document. Committed names are never recycled;
    /// an empty log left by interrupted creation may be retried.
    pub async fn publish_doc(
        &self,
        identity: &AgentIdentity,
        name: &str,
        mime: &str,
        body: Body,
        title: Option<String>,
    ) -> Result<DocWriteReceipt, BrokerError> {
        validate_doc_name(name)?;
        if mime.is_empty() {
            return Err(BrokerError::invalid("mime must not be empty"));
        }
        let _send_guard = self.send_lock.lock().await;
        if self
            .doc_states
            .lock()
            .await
            .get(name)
            .is_some_and(|state| state.head > 0)
        {
            return Err(BrokerError::application(
                ErrorCode::Exists,
                format!("document {name:?} already exists"),
            ));
        }
        let (mut log, mut runtime) = open_doc(&self.root, name, self.config.log_rotation_bytes)?;
        if runtime.head > 0 {
            return Err(BrokerError::application(
                ErrorCode::Exists,
                format!("document {name:?} already exists"),
            ));
        }
        #[cfg(feature = "mbt-injection")]
        crate::faults::reach(
            doc_path(&self.root, name),
            crate::faults::FaultPoint::AfterDocCreate,
        )?;
        let body = Body::new(mime.to_string(), body.text);
        let (inline, blob, storage) = encode_body(&self.root, &body)?;
        let mut event = base_event("doc_published", name, identity);
        event.notification_intent =
            self.capture_document_arrival_routes(name, ArrivalKind::DocPublished)?;
        event.version = Some(1);
        event.title = Some(title.unwrap_or_default());
        event.mime = Some(mime.to_string());
        event.body = inline;
        event.blob = blob.clone();
        let receipt = log.append(&mut event)?;
        self.announce_document_commit(&name, receipt.seq, &receipt.hash);
        apply_event(&mut runtime, &event, &receipt.hash)?;
        self.docs.lock().await.insert(name.to_string(), log);
        self.doc_states
            .lock()
            .await
            .insert(name.to_string(), runtime);
        Ok(DocWriteReceipt {
            name: name.to_string(),
            version: 1,
            head_version: 1,
            title: event.title.unwrap_or_default(),
            mime: mime.to_string(),
            storage: storage.to_string(),
            blob,
            seq: receipt.seq,
            ts: event.ts,
        })
    }

    /// Compare-and-swap update. When a claim is active the caller must also
    /// present matching `lease_id` and `fence`.
    pub async fn update_doc(
        &self,
        identity: &AgentIdentity,
        name: &str,
        base_version: u64,
        body: Body,
        title: Option<String>,
        lease: Option<(&str, u64)>,
    ) -> Result<DocWriteReceipt, BrokerError> {
        validate_doc_name(name)?;
        if body.mime.is_empty() {
            return Err(BrokerError::invalid("body.mime must not be empty"));
        }
        let _send_guard = self.send_lock.lock().await;
        if !self.ensure_doc_loaded(name).await? {
            return Err(BrokerError::application(
                ErrorCode::NotFound,
                format!("unknown document {name:?}"),
            ));
        }
        let now_ms = id::unix_millis();
        let mut logs = self.docs.lock().await;
        let log = logs
            .get_mut(name)
            .ok_or_else(|| BrokerError::internal("doc log vanished"))?;
        let mut states = self.doc_states.lock().await;
        let runtime = states
            .get_mut(name)
            .ok_or_else(|| BrokerError::internal("doc state vanished"))?;
        let expired = self.expire_doc_claim(name, log, runtime, now_ms)?;

        match lease {
            Some((lease_id, fence)) => {
                let claim = runtime.claim.clone().ok_or_else(|| {
                    BrokerError::application(
                        ErrorCode::LeaseLost,
                        if expired {
                            "document claim expired; fence advanced"
                        } else {
                            "document is not claimed"
                        },
                    )
                })?;
                if claim.lease_id != lease_id {
                    return Err(BrokerError::application(
                        ErrorCode::LeaseLost,
                        "lease id does not match the current document claim",
                    ));
                }
                if claim.agent_id != identity.agent_id {
                    return Err(conflict_claimed(&claim, runtime.head));
                }
                if claim.fence != fence {
                    return Err(BrokerError::application(
                        ErrorCode::LeaseLost,
                        format!("stale fence {fence}; current fence is {}", claim.fence),
                    ));
                }
            }
            None => {
                if let Some(claim) = &runtime.claim {
                    return Err(conflict_claimed(claim, runtime.head));
                }
            }
        }
        if base_version != runtime.head {
            return Err(BrokerError::application_with_data(
                ErrorCode::VersionConflict,
                format!(
                    "base_version {base_version} does not match head {}",
                    runtime.head
                ),
                json!({ "head_version": runtime.head }),
            ));
        }

        let (inline, blob, storage) = encode_body(&self.root, &body)?;
        let previous_title = runtime.title.clone();
        let head = runtime.head;
        let mut event = base_event("doc_updated", name, identity);
        event.notification_intent =
            self.capture_document_arrival_routes(name, ArrivalKind::DocUpdated)?;
        event.version = Some(head + 1);
        event.previous_version = Some(head);
        event.title = Some(title.unwrap_or(previous_title));
        event.mime = Some(body.mime.clone());
        event.body = inline;
        event.blob = blob.clone();
        if let Some(claim) = &runtime.claim {
            event.lease_id = Some(claim.lease_id.clone());
            event.fence = Some(claim.fence);
        }
        let receipt = log.append(&mut event)?;
        self.announce_document_commit(&name, receipt.seq, &receipt.hash);
        apply_event(runtime, &event, &receipt.hash)?;
        Ok(DocWriteReceipt {
            name: name.to_string(),
            version: head + 1,
            head_version: head + 1,
            title: event.title.unwrap_or_default(),
            mime: body.mime,
            storage: storage.to_string(),
            blob,
            seq: receipt.seq,
            ts: event.ts,
        })
    }

    /// Acquires the exclusive edit lease. A live claim owned by anyone else
    /// (including the caller) returns `CONFLICT_CLAIMED` with holder/expiry.
    pub async fn claim_doc(
        &self,
        identity: &AgentIdentity,
        name: &str,
        lease_ttl_s: u64,
    ) -> Result<ClaimDocReceipt, BrokerError> {
        if lease_ttl_s == 0 || lease_ttl_s > MAX_LEASE_TTL_S {
            return Err(BrokerError::invalid(format!(
                "lease_ttl_s must be 1..={MAX_LEASE_TTL_S}"
            )));
        }
        validate_doc_name(name)?;
        let _send_guard = self.send_lock.lock().await;
        if !self.ensure_doc_loaded(name).await? {
            return Err(BrokerError::application(
                ErrorCode::NotFound,
                format!("unknown document {name:?}"),
            ));
        }
        let now_ms = id::unix_millis();
        let mut logs = self.docs.lock().await;
        let log = logs
            .get_mut(name)
            .ok_or_else(|| BrokerError::internal("doc log vanished"))?;
        let mut states = self.doc_states.lock().await;
        let runtime = states
            .get_mut(name)
            .ok_or_else(|| BrokerError::internal("doc state vanished"))?;
        self.expire_doc_claim(name, log, runtime, now_ms)?;
        if let Some(claim) = &runtime.claim {
            return Err(conflict_claimed(claim, runtime.head));
        }
        let lease_id = id::new_ulid()
            .map_err(|error| BrokerError::internal(format!("generate lease id: {error}")))?;
        let expires_at_ms = now_ms.saturating_add(lease_ttl_s.saturating_mul(1_000));
        let head_version = runtime.head;
        let mut event = base_event("doc_claimed", name, identity);
        event.version = Some(head_version);
        event.lease_id = Some(lease_id.clone());
        event.expires_at_ms = Some(expires_at_ms);
        event.lease_ttl_s = Some(lease_ttl_s);
        let receipt = log.append(&mut event)?;
        self.announce_document_commit(&name, receipt.seq, &receipt.hash);
        apply_event(runtime, &event, &receipt.hash)?;
        Ok(ClaimDocReceipt {
            name: name.to_string(),
            lease_id,
            fence: receipt.seq,
            head_version,
            expires_at: id::format_rfc3339_millis(expires_at_ms),
            lease_ttl_s,
        })
    }

    /// Renews a document lease held by `identity`.
    pub(crate) async fn renew_doc_lease(
        &self,
        identity: &AgentIdentity,
        name: &str,
        lease_id: &str,
    ) -> Result<RenewLeaseReceipt, BrokerError> {
        let now_ms = id::unix_millis();
        let mut logs = self.docs.lock().await;
        let log = logs
            .get_mut(name)
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "lease lost"))?;
        let mut states = self.doc_states.lock().await;
        let runtime = states
            .get_mut(name)
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "lease lost"))?;
        self.expire_doc_claim(name, log, runtime, now_ms)?;
        let claim = runtime
            .claim
            .clone()
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "lease lost"))?;
        if claim.lease_id != lease_id || claim.agent_id != identity.agent_id {
            return Err(BrokerError::application(ErrorCode::LeaseLost, "lease lost"));
        }
        let expires_at_ms = now_ms.saturating_add(claim.ttl_s.saturating_mul(1_000));
        let mut event = base_event("doc_lease_renewed", name, identity);
        // Renewal extends the deadline and never changes the claim's fence.
        event.version = Some(runtime.head);
        event.fence = Some(claim.fence);
        event.lease_id = Some(claim.lease_id.clone());
        event.expires_at_ms = Some(expires_at_ms);
        event.lease_ttl_s = Some(claim.ttl_s);
        let receipt = log.append(&mut event)?;
        self.announce_document_commit(&name, receipt.seq, &receipt.hash);
        apply_event(runtime, &event, &receipt.hash)?;
        let new_claim = runtime
            .claim
            .clone()
            .ok_or_else(|| BrokerError::internal("renewal did not install a claim"))?;
        Ok(RenewLeaseReceipt {
            lease_id: new_claim.lease_id,
            kind: "doc".to_string(),
            fence: new_claim.fence,
            expires_at: id::format_rfc3339_millis(expires_at_ms),
            lease_ttl_s: new_claim.ttl_s,
        })
    }

    /// Releases a claim. The release event advances the fence.
    pub async fn release_doc(
        &self,
        identity: &AgentIdentity,
        lease_id: &str,
        fence: u64,
    ) -> Result<ReleaseDocReceipt, BrokerError> {
        let _send_guard = self.send_lock.lock().await;
        let name = self
            .doc_for_lease(lease_id)
            .await
            .ok_or_else(|| BrokerError::application(ErrorCode::LeaseLost, "unknown lease"))?;
        let now_ms = id::unix_millis();
        let mut logs = self.docs.lock().await;
        let log = logs
            .get_mut(&name)
            .ok_or_else(|| BrokerError::internal("doc log vanished"))?;
        let mut states = self.doc_states.lock().await;
        let runtime = states
            .get_mut(&name)
            .ok_or_else(|| BrokerError::internal("doc state vanished"))?;
        self.expire_doc_claim(&name, log, runtime, now_ms)?;
        let claim = runtime.claim.clone().ok_or_else(|| {
            BrokerError::application(ErrorCode::LeaseLost, "lease lost or expired")
        })?;
        if claim.lease_id != lease_id {
            return Err(BrokerError::application(ErrorCode::LeaseLost, "lease lost"));
        }
        if claim.agent_id != identity.agent_id {
            return Err(BrokerError::application(
                ErrorCode::LeaseLost,
                "lease is held by another agent",
            ));
        }
        if claim.fence != fence {
            return Err(BrokerError::application(
                ErrorCode::LeaseLost,
                format!("stale fence {fence}; current fence is {}", claim.fence),
            ));
        }
        let mut event = base_event("doc_released", &name, identity);
        event.version = Some(runtime.head);
        event.lease_id = Some(claim.lease_id.clone());
        event.fence = Some(claim.fence);
        let receipt = log.append(&mut event)?;
        self.announce_document_commit(&name, receipt.seq, &receipt.hash);
        apply_event(runtime, &event, &receipt.hash)?;
        Ok(ReleaseDocReceipt {
            name,
            lease_id: claim.lease_id,
            fence,
            new_fence: receipt.seq,
            released: true,
            seq: receipt.seq,
        })
    }

    /// Reads the head or a pinned version. Claims are visible but reads never
    /// require them.
    pub async fn read_doc(
        &self,
        name: &str,
        version: Option<u64>,
    ) -> Result<DocReadResponse, BrokerError> {
        self.read_doc_limited(name, version, None).await
    }

    pub(crate) async fn read_doc_limited(
        &self,
        name: &str,
        version: Option<u64>,
        byte_limit: Option<usize>,
    ) -> Result<DocReadResponse, BrokerError> {
        validate_doc_name(name)?;
        let _send_guard = self.send_lock.lock().await;
        if !self.ensure_doc_loaded(name).await? {
            return Err(BrokerError::application(
                ErrorCode::NotFound,
                format!("unknown document {name:?}"),
            ));
        }
        let now_ms = id::unix_millis();
        let snapshot = {
            let mut logs = self.docs.lock().await;
            let log = logs
                .get_mut(name)
                .ok_or_else(|| BrokerError::internal("doc log vanished"))?;
            let mut states = self.doc_states.lock().await;
            let runtime = states
                .get_mut(name)
                .ok_or_else(|| BrokerError::internal("doc state vanished"))?;
            self.expire_doc_claim(name, log, runtime, now_ms)?;
            let version_number = version.unwrap_or(runtime.head);
            let version = runtime.versions.get(&version_number).ok_or_else(|| {
                BrokerError::application(
                    ErrorCode::NotFound,
                    format!("document {name:?} has no version {version_number}"),
                )
            })?;
            if let Some(maximum) = byte_limit {
                if crate::rpc::bounded_json_size(
                    &(&version.title, &version.mime, &version.body),
                    maximum,
                )
                .is_none()
                    || version
                        .blob
                        .as_ref()
                        .is_some_and(|blob| blob.size > maximum as u64)
                {
                    return Err(BrokerError::application(
                        ErrorCode::ResourceExhausted,
                        "document version exceeds remote budget",
                    ));
                }
            }
            let version = version.clone();
            let claim = runtime.claim.as_ref().map(claim_view);
            (
                version,
                runtime.head,
                claim,
                runtime.fence,
                runtime.versions.len(),
                runtime.updated_at.clone(),
            )
        };
        let (version, head, claim, fence, versions, updated_at) = snapshot;
        let body = if let Some(body) = version.body {
            body
        } else if let Some(blob) = &version.blob {
            Body::new(
                version.mime.clone(),
                match byte_limit {
                    Some(maximum) => load_blob_limited(&self.root, blob, maximum)?,
                    None => load_blob(&self.root, blob)?,
                },
            )
        } else {
            return Err(BrokerError::internal("document version has no body"));
        };
        Ok(DocReadResponse {
            name: name.to_string(),
            version: version.version,
            head_version: head,
            title: version.title,
            mime: version.mime,
            body,
            storage: if version.blob.is_some() {
                "blob"
            } else {
                "inline"
            }
            .to_string(),
            blob: version.blob,
            claim,
            fence,
            versions,
            updated_at,
        })
    }

    /// Lists document metadata, optionally filtered by name prefix.
    pub async fn list_docs(&self, prefix: Option<&str>) -> Result<ListDocsResponse, BrokerError> {
        self.list_docs_page(prefix, None, usize::MAX, |_| true)
            .await
    }

    pub(crate) async fn list_docs_page(
        &self,
        prefix: Option<&str>,
        after: Option<&str>,
        limit: usize,
        allowed: impl Fn(&str) -> bool,
    ) -> Result<ListDocsResponse, BrokerError> {
        let _send_guard = self.send_lock.lock().await;
        // Keep only the next authorized names; never construct a full remote list.
        let mut names = std::collections::BTreeSet::new();
        for name in self.doc_states.lock().await.keys() {
            if prefix.is_some_and(|prefix| !name.starts_with(prefix))
                || after.is_some_and(|after| name.as_str() <= after)
                || !allowed(name)
            {
                continue;
            }
            if names.len() < limit || names.last().is_some_and(|last| name < last) {
                names.insert(name.clone());
                if names.len() > limit {
                    names.pop_last();
                }
            }
        }
        let now_ms = id::unix_millis();
        let mut docs = Vec::new();
        let mut encoded_bytes = 0usize;
        for name in names {
            if prefix.is_some_and(|prefix| !name.starts_with(prefix)) {
                continue;
            }
            let mut logs = self.docs.lock().await;
            let Some(log) = logs.get_mut(&name) else {
                continue;
            };
            let mut states = self.doc_states.lock().await;
            let Some(runtime) = states.get_mut(&name) else {
                continue;
            };
            self.expire_doc_claim(&name, log, runtime, now_ms)?;
            let Some(head) = runtime.head_version() else {
                continue;
            };
            if limit != usize::MAX {
                let Some(size) = crate::rpc::bounded_json_size(
                    &(
                        &runtime.title,
                        &head.mime,
                        runtime.claim.as_ref().map(claim_view),
                    ),
                    (crate::transport::MAX_RPC_BYTES - 16384).saturating_sub(encoded_bytes),
                ) else {
                    if docs.is_empty() {
                        return Err(BrokerError::application(
                            ErrorCode::ResourceExhausted,
                            "document metadata exceeds remote page budget",
                        ));
                    }
                    break;
                };
                encoded_bytes += size + 1024;
            }
            docs.push(DocSummary {
                name: name.clone(),
                head_version: runtime.head,
                title: runtime.title.clone(),
                mime: head.mime.clone(),
                versions: runtime.versions.len(),
                updated_at: runtime.updated_at.clone(),
                fence: runtime.fence,
                claim: runtime.claim.as_ref().map(claim_view),
            });
        }
        docs.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(ListDocsResponse {
            prefix: prefix.map(str::to_string),
            docs,
        })
    }

    pub(crate) async fn doc_for_lease(&self, lease_id: &str) -> Option<String> {
        let states = self.doc_states.lock().await;
        states.iter().find_map(|(name, runtime)| {
            runtime
                .claim
                .as_ref()
                .is_some_and(|claim| claim.lease_id == lease_id)
                .then(|| name.clone())
        })
    }

    /// Expires overdue claims in every document. Used by the sweeper and
    /// directly by tests.
    pub async fn sweep_expired_docs(&self) -> Result<u64, BrokerError> {
        let _send_guard = self.send_lock.lock().await;
        let names: Vec<String> = self.doc_states.lock().await.keys().cloned().collect();
        let now_ms = id::unix_millis();
        let mut total = 0;
        for name in names {
            let mut logs = self.docs.lock().await;
            let Some(log) = logs.get_mut(&name) else {
                continue;
            };
            let mut states = self.doc_states.lock().await;
            let Some(runtime) = states.get_mut(&name) else {
                continue;
            };
            if self.expire_doc_claim(&name, log, runtime, now_ms)? {
                total += 1;
            }
        }
        Ok(total)
    }

    /// Forces expiry of the active claim on `name`, regardless of deadline.
    ///
    /// Test/MBT support only: same event path as the sweeper
    /// (`expire_doc_claim`) with an infinite clock. Never used in production.
    #[doc(hidden)]
    pub async fn debug_force_expire_doc(&self, name: &str) -> Result<bool, BrokerError> {
        let _send_guard = self.send_lock.lock().await;
        let mut logs = self.docs.lock().await;
        let Some(log) = logs.get_mut(name) else {
            return Ok(false);
        };
        let mut states = self.doc_states.lock().await;
        let Some(runtime) = states.get_mut(name) else {
            return Ok(false);
        };
        self.expire_doc_claim(name, log, runtime, u64::MAX)
    }
}
