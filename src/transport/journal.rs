//! Private, bounded client-side record of immutable remote operation intents.
//!
//! Every update holds an interprocess lock, reads the current state, and writes
//! a same-directory replacement followed by file and directory fsync. No lock
//! spans a network exchange. A crash before receiving or saving a response
//! therefore leaves the original operation available for explicit resumption.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::client::ClientError;
use crate::operations::OperationRef;
use crate::rpc::RpcError;

use super::{is_journaled_mutation, BrokerClient, Endpoint, MAX_RPC_BYTES};

pub const MAX_JOURNAL_ENTRIES: usize = 1_000;
pub const MAX_JOURNAL_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_OPERATION_AGE_MS: u64 = 24 * 60 * 60 * 1_000;

/// The same endpoint, bus, and agent instance must be selected on resumption.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationScope {
    pub endpoint: String,
    pub bus_id: String,
    pub agent_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum JournalOutcome {
    Pending,
    /// The response was not established. Never invent a replacement ID.
    Unresolved,
    Completed {
        result: Value,
    },
    /// A source-synchronized no-effect outcome bound to this exact operation.
    Rejected {
        error: RpcError,
    },
}

impl JournalOutcome {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Rejected { .. })
    }
}

/// Authentication and JSON-RPC correlation belong to the transport, not here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalEntry {
    pub operation: OperationRef,
    pub scope: OperationScope,
    pub method: String,
    pub intent: Value,
    pub outcome: JournalOutcome,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalState {
    v: u16,
    entries: BTreeMap<String, JournalEntry>,
}

/// `directory` is a dedicated private directory, not a broker root.
#[derive(Clone, Debug)]
pub struct OperationJournal {
    directory: PathBuf,
}

impl OperationJournal {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, ClientError> {
        let directory = private_directory(directory.as_ref())?;
        let journal = Self { directory };
        let _owner = journal.lock()?;
        journal.load()?;
        Ok(journal)
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Read local state without connecting to the broker or exposing credentials.
    pub fn list(&self) -> Result<Vec<JournalEntry>, ClientError> {
        let _owner = self.lock()?;
        Ok(self.load()?.entries.into_values().collect())
    }

    pub fn get(&self, id: &str) -> Result<JournalEntry, ClientError> {
        let _owner = self.lock()?;
        self.load()?
            .entries
            .remove(id)
            .ok_or_else(|| protocol("operation does not exist in this journal"))
    }

    /// Persist a fresh intent before any attempt to submit its mutation.
    /// Callers must obtain `scope` and `epoch` from authenticated broker discovery.
    pub fn prepare(
        &self,
        scope: OperationScope,
        method: &str,
        intent: Value,
        epoch: &str,
        not_after_ms: u64,
    ) -> Result<JournalEntry, ClientError> {
        let now = crate::id::unix_millis();
        if not_after_ms <= now || not_after_ms > now.saturating_add(MAX_OPERATION_AGE_MS) {
            return Err(protocol(
                "operation submission deadline must be within 24 hours",
            ));
        }
        let entry = JournalEntry {
            operation: OperationRef {
                v: 1,
                id: crate::id::new_ulid().map_err(ClientError::Unreachable)?,
                epoch: epoch.to_owned(),
                not_after_ms,
            },
            scope,
            method: method.to_owned(),
            intent,
            outcome: JournalOutcome::Pending,
        };
        validate_entry(&entry)?;
        let _owner = self.lock()?;
        let mut state = self.load()?;
        if state.entries.len() >= MAX_JOURNAL_ENTRIES {
            return Err(protocol(
                "operation journal entry limit reached; no request was submitted",
            ));
        }
        if state
            .entries
            .insert(entry.operation.id.clone(), entry.clone())
            .is_some()
        {
            return Err(protocol("operation ID collision; no request was submitted"));
        }
        self.save(&state)?;
        Ok(entry)
    }

    fn record(&self, expected: &JournalEntry, outcome: JournalOutcome) -> Result<(), ClientError> {
        let _owner = self.lock()?;
        let mut state = self.load()?;
        let entry = state
            .entries
            .get_mut(&expected.operation.id)
            .ok_or_else(|| protocol("operation vanished from journal"))?;
        if entry.operation != expected.operation
            || entry.scope != expected.scope
            || entry.method != expected.method
            || entry.intent != expected.intent
        {
            return Err(protocol(
                "operation journal intent changed during submission",
            ));
        }
        // A later uncertain or denied exchange cannot erase either kind of
        // established outcome. Conflicting terminal outcomes fail closed.
        if entry.outcome.is_terminal() {
            if outcome.is_terminal() && entry.outcome != outcome {
                return Err(protocol("broker returned conflicting operation outcomes"));
            }
            return Ok(());
        }
        entry.outcome = outcome;
        self.save(&state)
    }

    fn lock(&self) -> Result<File, ClientError> {
        validate_directory(&self.directory)?;
        let path = self.directory.join("owner.lock");
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => open_private(&path)?,
            Err(error) => return Err(ClientError::Unreachable(error)),
        };
        file.lock().map_err(ClientError::Unreachable)?;
        // Refuse a replaced lock inode instead of silently using another lock.
        verify_open_file(&path, &file)?;
        Ok(file)
    }

    fn load(&self) -> Result<JournalState, ClientError> {
        let path = self.directory.join("operations.json");
        let mut file = match open_private(&path) {
            Ok(file) => file,
            Err(ClientError::Unreachable(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(JournalState {
                    v: 1,
                    entries: BTreeMap::new(),
                });
            }
            Err(error) => return Err(error),
        };
        if file.metadata().map_err(ClientError::Unreachable)?.len() > MAX_JOURNAL_BYTES as u64 {
            return Err(protocol("operation journal exceeds byte limit"));
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take(MAX_JOURNAL_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(ClientError::Unreachable)?;
        if bytes.len() > MAX_JOURNAL_BYTES {
            return Err(protocol("operation journal exceeds byte limit"));
        }
        let state: JournalState = serde_json::from_slice(&bytes)
            .map_err(|_| protocol("invalid operation journal; refusing to submit"))?;
        if state.v != 1 || state.entries.len() > MAX_JOURNAL_ENTRIES {
            return Err(protocol("unsupported or oversized operation journal"));
        }
        for (key, entry) in &state.entries {
            if key != &entry.operation.id {
                return Err(protocol("operation journal identity mismatch"));
            }
            validate_entry(entry)?;
        }
        // A crashed process may have completed rename but not its directory
        // fsync. Seeing those cached bytes is not enough to authorize a remote
        // mutation: synchronize recovery before exposing the original intent.
        #[cfg(feature = "mbt-injection")]
        crate::faults::reach(&path, crate::faults::FaultPoint::BeforeRecoverySync)
            .map_err(ClientError::Unreachable)?;
        file.sync_all().map_err(ClientError::Unreachable)?;
        #[cfg(feature = "mbt-injection")]
        crate::faults::reach(
            &self.directory,
            crate::faults::FaultPoint::BeforeRecoverySync,
        )
        .map_err(ClientError::Unreachable)?;
        File::open(&self.directory)
            .and_then(|directory| directory.sync_all())
            .map_err(ClientError::Unreachable)?;
        Ok(state)
    }

    fn save(&self, state: &JournalState) -> Result<(), ClientError> {
        if crate::rpc::bounded_json_size(state, MAX_JOURNAL_BYTES).is_none() {
            return Err(protocol("operation journal byte limit reached"));
        }
        let temporary = self.directory.join("operations.tmp");
        // A process may have died before rename. It never submitted an intent
        // that exists only in this scratch file; the current file is authoritative.
        match fs::symlink_metadata(&temporary) {
            Ok(_) => {
                let _file = open_private(&temporary)?;
                fs::remove_file(&temporary).map_err(ClientError::Unreachable)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(ClientError::Unreachable(error)),
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(ClientError::Unreachable)?;
        serde_json::to_writer(&mut file, state)
            .map_err(|_| protocol("cannot write operation journal"))?;
        file.flush().map_err(ClientError::Unreachable)?;
        file.sync_all().map_err(ClientError::Unreachable)?;
        fs::rename(&temporary, self.directory.join("operations.json"))
            .map_err(ClientError::Unreachable)?;
        File::open(&self.directory)
            .and_then(|dir| dir.sync_all())
            .map_err(ClientError::Unreachable)
    }
}

impl BrokerClient {
    /// Discover and persist an authenticated immutable intent. The discovery
    /// calls are reads; this method never submits the mutation itself.
    pub async fn prepare_operation(
        &self,
        journal: &OperationJournal,
        method: &str,
        mut params: Value,
        deadline: Duration,
    ) -> Result<JournalEntry, ClientError> {
        // Check the journal before making even a discovery request.
        journal.list()?;
        if !is_journaled_mutation(method) {
            return Err(protocol(
                "method is not supported by the client operation journal",
            ));
        }
        let object = params
            .as_object_mut()
            .ok_or_else(|| protocol("operation params must be an object"))?;
        let token = match object.remove("token") {
            Some(Value::String(token)) if !token.is_empty() => token,
            _ => {
                return Err(protocol(
                    "journaled operation requires current instance credentials",
                ))
            }
        };
        validate_intent(&params)?;
        if contains_secret(&params, &token) {
            return Err(protocol("operation intent contains the current credential"));
        }
        let info = self.discover_info(Some(&token), deadline).await?;
        if !info
            .supported_methods
            .iter()
            .any(|allowed| allowed == method)
        {
            return Err(protocol("broker does not advertise this operation method"));
        }
        let scope = self.operation_scope(&token, &info.bus_id, deadline).await?;
        let now = crate::id::unix_millis();
        let maximum_age = info
            .operation_max_age_ms
            .unwrap_or(MAX_OPERATION_AGE_MS)
            .min(MAX_OPERATION_AGE_MS);
        let expiry = now
            .saturating_add(maximum_age)
            .min(info.operation_epoch_expires_at_ms.unwrap_or(u64::MAX));
        journal.prepare(scope, method, params, &info.operation_epoch, expiry)
    }

    /// Submit or explicitly resume an operation by its original local ID.
    /// Current credentials are supplied separately and are never persisted.
    pub async fn submit_operation(
        &self,
        journal: &OperationJournal,
        operation_id: &str,
        token: &str,
        deadline: Duration,
    ) -> Result<Value, ClientError> {
        let entry = journal.get(operation_id)?;
        let endpoint = self.https_origin()?;
        if entry.scope.endpoint != endpoint {
            return Err(protocol("operation belongs to a different broker endpoint"));
        }
        if token.is_empty() || token.len() > 4096 || contains_secret(&entry.intent, token) {
            return Err(protocol("invalid credentials for operation resumption"));
        }
        let info = self.discover_info(Some(token), deadline).await?;
        if entry.scope.bus_id != info.bus_id {
            return Err(protocol(
                "operation belongs to a different bus or agent instance",
            ));
        }
        let scope = self.operation_scope(token, &info.bus_id, deadline).await?;
        if entry.scope != scope {
            return Err(protocol(
                "operation belongs to a different bus or agent instance",
            ));
        }
        let mut params = entry.intent.clone();
        params["token"] = json!(token);
        params["operation"] = serde_json::to_value(&entry.operation)
            .map_err(|_| protocol("invalid operation reference"))?;
        let result = self.call(&entry.method, params, deadline).await;
        let outcome = observed_outcome(&entry, &result, token);
        let terminal_rejection = matches!(outcome, JournalOutcome::Rejected { .. });
        if journal.record(&entry, outcome).is_err() {
            return Err(ClientError::OutcomeUnknown(format!("operation {operation_id}: response could not be persisted; explicitly resume this ID")));
        }
        if result
            .as_ref()
            .is_ok_and(|value| contains_secret(value, token))
        {
            // Credential-issuing methods need a separate private secret store.
            return Err(ClientError::OutcomeUnknown(format!(
                "operation {operation_id} returned a credential; response cannot be journaled"
            )));
        }
        result.map_err(|error| match error {
            // Preserve original code/message/data for a proven terminal error.
            // Managed empty-claim observation also relies on its exact message.
            ClientError::Rpc(error) if terminal_rejection => ClientError::Rpc(error),
            ClientError::Rpc(mut error) => {
                error.message = format!(
                    "operation {operation_id} remains unresolved: {}; explicitly resume this ID",
                    error.message
                );
                ClientError::Rpc(error)
            }
            other => ClientError::OutcomeUnknown(format!(
                "operation {operation_id}: {other}; explicitly resume this ID"
            )),
        })
    }

    /// Fresh explicit invocations always create a new ID. This never retries.
    pub async fn call_journaled(
        &self,
        journal: &OperationJournal,
        method: &str,
        params: Value,
        deadline: Duration,
    ) -> Result<Value, ClientError> {
        let token = params
            .get("token")
            .and_then(Value::as_str)
            .ok_or_else(|| protocol("journaled operation requires current instance credentials"))?
            .to_owned();
        let entry = self
            .prepare_operation(journal, method, params, deadline)
            .await?;
        self.submit_operation(journal, &entry.operation.id, &token, deadline)
            .await
    }

    async fn operation_scope(
        &self,
        token: &str,
        bus_id: &str,
        deadline: Duration,
    ) -> Result<OperationScope, ClientError> {
        let metadata = self
            .call("instance_metadata", json!({"token":token}), deadline)
            .await?;
        let agent_id = metadata
            .get("agent_id")
            .and_then(Value::as_str)
            .filter(|id| crate::id::is_ulid(id))
            .ok_or_else(|| protocol("broker returned an invalid agent instance identity"))?;
        Ok(OperationScope {
            endpoint: self.https_origin()?.to_owned(),
            bus_id: bus_id.to_owned(),
            agent_id: agent_id.to_owned(),
        })
    }

    fn https_origin(&self) -> Result<&str, ClientError> {
        match self.endpoint() {
            Endpoint::Https(origin) => Ok(origin.as_str()),
            Endpoint::Uds(_) => Err(protocol(
                "durable client operation journals require an HTTPS endpoint",
            )),
        }
    }
}

fn validate_entry(entry: &JournalEntry) -> Result<(), ClientError> {
    let endpoint = Endpoint::https(&entry.scope.endpoint)?;
    if !matches!(endpoint, Endpoint::Https(url) if url.as_str() == entry.scope.endpoint)
        || !crate::id::is_ulid(&entry.scope.bus_id)
        || !crate::id::is_ulid(&entry.scope.agent_id)
        || !crate::id::is_ulid(&entry.operation.id)
        || !crate::id::is_ulid(&entry.operation.epoch)
        || entry.operation.v != 1
        || entry.operation.not_after_ms == 0
        || !is_journaled_mutation(&entry.method)
    {
        return Err(protocol("invalid operation journal identity or method"));
    }
    validate_intent(&entry.intent)?;
    if let JournalOutcome::Rejected { error } = &entry.outcome {
        if !has_terminal_marker(entry, error) {
            return Err(protocol(
                "journal rejection lacks its bound durable terminal marker",
            ));
        }
    }
    Ok(())
}

fn observed_outcome(
    entry: &JournalEntry,
    result: &Result<Value, ClientError>,
    token: &str,
) -> JournalOutcome {
    match result {
        Ok(result) if !contains_secret(result, token) => JournalOutcome::Completed {
            result: result.clone(),
        },
        Err(ClientError::Rpc(error))
            if has_terminal_marker(entry, error)
                && !contains_secret(&serde_json::to_value(error).unwrap_or(Value::Null), token) =>
        {
            JournalOutcome::Rejected {
                error: error.clone(),
            }
        }
        // A denial may follow a commit when authority changed before delivery.
        // Only the broker's exact source-synchronized marker proves no effect.
        _ => JournalOutcome::Unresolved,
    }
}

fn has_terminal_marker(entry: &JournalEntry, error: &RpcError) -> bool {
    error
        .data
        .as_ref()
        .and_then(|data| data.get("durable_operation"))
        == Some(&json!({
            "v": 1,
            "operation": entry.operation,
            "method": entry.method,
            "state": "terminal_no_effect",
            "bus_id": entry.scope.bus_id,
            "agent_id": entry.scope.agent_id,
        }))
}

fn validate_intent(intent: &Value) -> Result<(), ClientError> {
    let object = intent
        .as_object()
        .ok_or_else(|| protocol("operation intent must be an object"))?;
    for forbidden in [
        "token",
        "operation",
        "authorization",
        "bearer",
        "credential",
        "device_token",
        "access_token",
        "refresh_token",
        "idempotency_key",
    ] {
        if object.contains_key(forbidden) {
            return Err(protocol(
                "operation intent contains reserved authentication or operation fields",
            ));
        }
    }
    if crate::rpc::bounded_json_size(intent, MAX_RPC_BYTES - 1024).is_none() {
        return Err(protocol("operation intent exceeds size limit"));
    }
    Ok(())
}

fn contains_secret(value: &Value, token: &str) -> bool {
    match value {
        Value::String(value) => value.contains(token),
        Value::Array(values) => values.iter().any(|value| contains_secret(value, token)),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains(token) || contains_secret(value, token)),
        _ => false,
    }
}

fn protocol(detail: &str) -> ClientError {
    ClientError::Protocol(detail.into())
}

fn effective_uid() -> u32 {
    extern "C" {
        fn geteuid() -> u32;
    }
    // POSIX geteuid has no preconditions.
    unsafe { geteuid() }
}

fn private_directory(path: &Path) -> Result<PathBuf, ClientError> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(ClientError::Unreachable)?
            .join(path)
    };
    let mut current = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::RootDir | Component::Normal(_) => current.push(component),
            Component::CurDir => continue,
            _ => {
                return Err(protocol(
                    "operation journal path must not contain parent traversal",
                ))
            }
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(protocol(
                    "operation journal path must not contain symlinks or non-directories",
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::DirBuilder::new().mode(0o700).create(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(ClientError::Unreachable(error)),
                }
                validate_directory(&current)?;
                File::open(current.parent().unwrap_or(Path::new("/")))
                    .and_then(|parent| parent.sync_all())
                    .map_err(ClientError::Unreachable)?;
            }
            Err(error) => return Err(ClientError::Unreachable(error)),
        }
    }
    validate_directory(&current)?;
    // Repair directory creation interrupted before a parent fsync. Existing
    // components on reopen need the same durability guarantee as new ones.
    for ancestor in current.ancestors() {
        #[cfg(feature = "mbt-injection")]
        crate::faults::reach(ancestor, crate::faults::FaultPoint::BeforeRecoverySync)
            .map_err(ClientError::Unreachable)?;
        File::open(ancestor)
            .and_then(|directory| directory.sync_all())
            .map_err(ClientError::Unreachable)?;
    }
    Ok(current)
}

fn validate_directory(path: &Path) -> Result<(), ClientError> {
    let metadata = fs::symlink_metadata(path).map_err(ClientError::Unreachable)?;
    if !metadata.is_dir()
        || metadata.uid() != effective_uid()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(protocol(
            "operation journal must be a private directory owned by the current user",
        ));
    }
    Ok(())
}

fn validate_file(metadata: &fs::Metadata) -> Result<(), ClientError> {
    if !metadata.is_file()
        || metadata.uid() != effective_uid()
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(protocol(
            "operation journal files must be private regular files without links",
        ));
    }
    Ok(())
}

fn open_private(path: &Path) -> Result<File, ClientError> {
    validate_file(&fs::symlink_metadata(path).map_err(ClientError::Unreachable)?)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(target_os = "linux")]
    options.custom_flags(0o400000 | 0o4000); // O_NOFOLLOW | O_NONBLOCK
    let file = options.open(path).map_err(ClientError::Unreachable)?;
    verify_open_file(path, &file)?;
    Ok(file)
}

fn verify_open_file(path: &Path, file: &File) -> Result<(), ClientError> {
    let before = fs::symlink_metadata(path).map_err(ClientError::Unreachable)?;
    let after = file.metadata().map_err(ClientError::Unreachable)?;
    validate_file(&before)?;
    validate_file(&after)?;
    if before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(protocol("operation journal file changed while opening"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestJournal(OperationJournal);

    impl TestJournal {
        fn new() -> Self {
            let directory = std::env::temp_dir().join(format!(
                "coop-client-journal-{}",
                crate::id::new_ulid().unwrap()
            ));
            Self(OperationJournal::open(directory).unwrap())
        }

        fn prepare(&self) -> JournalEntry {
            self.0
                .prepare(
                    OperationScope {
                        endpoint: "https://localhost:7420/".into(),
                        bus_id: crate::id::new_ulid().unwrap(),
                        agent_id: crate::id::new_ulid().unwrap(),
                    },
                    "claim_task",
                    json!({"queue":"work"}),
                    &crate::id::new_ulid().unwrap(),
                    crate::id::unix_millis() + 60_000,
                )
                .unwrap()
        }
    }

    impl Drop for TestJournal {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0.directory());
        }
    }

    fn empty_claim(entry: &JournalEntry) -> RpcError {
        RpcError::application_with_data(
            crate::error::ErrorCode::NotFound,
            "queue \"work\" has no claimable task",
            json!({
                "durable_operation": {"v":1,"operation":entry.operation,"method":entry.method,
                    "state":"terminal_no_effect","bus_id":entry.scope.bus_id,"agent_id":entry.scope.agent_id},
                "detail":"original no-effect result"
            }),
        )
    }

    #[test]
    fn client_journal_terminal_marker_binds_reference_method_and_scope() {
        let journal = TestJournal::new();
        let entry = journal.prepare();
        let error = empty_claim(&entry);
        let reply = |error: RpcError| Err(ClientError::Rpc(error));
        assert_eq!(
            observed_outcome(&entry, &reply(error.clone()), "current-credential"),
            JournalOutcome::Rejected {
                error: error.clone()
            }
        );
        let substitutions = [
            ("/durable_operation/v", json!(2)),
            ("/durable_operation/operation/v", json!(2)),
            (
                "/durable_operation/operation/id",
                json!(crate::id::new_ulid().unwrap()),
            ),
            (
                "/durable_operation/operation/epoch",
                json!(crate::id::new_ulid().unwrap()),
            ),
            (
                "/durable_operation/operation/not_after_ms",
                json!(entry.operation.not_after_ms + 1),
            ),
            ("/durable_operation/method", json!("enqueue")),
            ("/durable_operation/state", json!("unknown")),
            (
                "/durable_operation/bus_id",
                json!(crate::id::new_ulid().unwrap()),
            ),
            (
                "/durable_operation/agent_id",
                json!(crate::id::new_ulid().unwrap()),
            ),
        ];
        for (path, replacement) in substitutions {
            let mut wrong = error.clone();
            *wrong.data.as_mut().unwrap().pointer_mut(path).unwrap() = replacement;
            assert_eq!(
                observed_outcome(&entry, &reply(wrong), "current-credential"),
                JournalOutcome::Unresolved,
                "{path}"
            );
        }
        let mut absent = error.clone();
        absent
            .data
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("durable_operation");
        assert_eq!(
            observed_outcome(&entry, &reply(absent), "current-credential"),
            JournalOutcome::Unresolved
        );
        let mut leaked = error;
        leaked.data.as_mut().unwrap()["extra"] = json!({"message":"current-credential"});
        assert_eq!(
            observed_outcome(&entry, &reply(leaked), "current-credential"),
            JournalOutcome::Unresolved
        );
    }

    #[test]
    fn client_journal_terminal_error_is_persistent_exact_and_monotonic() {
        let journal = TestJournal::new();
        let entry = journal.prepare();
        let error = empty_claim(&entry);
        let rejected = JournalOutcome::Rejected {
            error: error.clone(),
        };
        journal.0.record(&entry, rejected.clone()).unwrap();
        let reopened = OperationJournal::open(journal.0.directory()).unwrap();
        assert_eq!(reopened.get(&entry.operation.id).unwrap().outcome, rejected);
        reopened.record(&entry, JournalOutcome::Unresolved).unwrap();
        reopened.record(&entry, rejected.clone()).unwrap();
        assert_eq!(reopened.get(&entry.operation.id).unwrap().outcome, rejected);
        assert!(reopened
            .record(
                &entry,
                JournalOutcome::Completed {
                    result: json!({"task":{"task_id":"new-task"}})
                }
            )
            .is_err());
        let mut changed = error;
        changed.message = "different outcome".into();
        assert!(reopened
            .record(&entry, JournalOutcome::Rejected { error: changed })
            .is_err());
        assert_eq!(reopened.get(&entry.operation.id).unwrap().outcome, rejected);
        let success = journal.prepare();
        let completed = JournalOutcome::Completed {
            result: json!({"task":{"lease_id":"original-lease","fence":1,"expires_at_ms":123}}),
        };
        reopened.record(&success, completed.clone()).unwrap();
        assert!(reopened
            .record(
                &success,
                JournalOutcome::Rejected {
                    error: empty_claim(&success)
                }
            )
            .is_err());
        reopened
            .record(&success, JournalOutcome::Unresolved)
            .unwrap();
        assert_eq!(
            reopened.get(&success.operation.id).unwrap().outcome,
            completed
        );
    }

    #[test]
    fn client_journal_failed_terminal_save_preserves_resumable_original_intent() {
        use std::os::unix::fs::symlink;
        let journal = TestJournal::new();
        let entry = journal.prepare();
        let authoritative = journal.0.directory().join("operations.json");
        let before = fs::read(&authoritative).unwrap();
        symlink(&authoritative, journal.0.directory().join("operations.tmp")).unwrap();
        assert!(journal
            .0
            .record(
                &entry,
                JournalOutcome::Rejected {
                    error: empty_claim(&entry)
                }
            )
            .is_err());
        assert_eq!(fs::read(authoritative).unwrap(), before);
        assert_eq!(
            OperationJournal::open(journal.0.directory())
                .unwrap()
                .get(&entry.operation.id)
                .unwrap(),
            entry
        );
    }
}
