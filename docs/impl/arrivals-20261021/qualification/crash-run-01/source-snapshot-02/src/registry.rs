//! Agent registry and token authentication (design §4).
//!
//! `registry.json` is the only mutable state file below the bus root. It maps
//! unique routing names to [`AgentRecord`]s; tokens are never stored, only their
//! SHA-256 digests. Writes are atomic (same-directory temporary file, fsync,
//! rename, parent fsync) and the file is mode 0600.
//!
//! A name is a display claim held by one agent. Re-registering an existing
//! name requires the current token and rotates it; the `agent_id` is stable
//! across rotations so inbox paths and provenance do not move.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::error::{BrokerError, ErrorCode};
use crate::id;
use crate::server::DaemonState;
use crate::storage::sha256_hex;

/// Original on-disk registry schema version, retained for disabled roots.
pub const REGISTRY_VERSION: u16 = 1;

/// Registry format that fences binaries unable to hash annotated source events.
pub const ARRIVALS_REGISTRY_VERSION: u16 = 2;

/// Required feature understood by this binary.
pub const ARRIVALS_REQUIRED_FEATURE: &str = "arrival_notifications_v1";

/// File name of the registry below the bus root.
pub const REGISTRY_FILE: &str = "registry.json";

/// Number of random bytes in an agent token (128 bits).
pub const TOKEN_BYTES: usize = 16;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One registered agent as persisted in `registry.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRecord {
    /// Stable identity assigned at first registration (a ULID).
    pub agent_id: String,
    /// `sha256:<hex>` digest of the current token.
    pub token_hash: String,
    /// Agent kind such as `codex`, `claude`, or `agent`.
    pub kind: String,
    /// Free-form capability labels reported by the agent.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// RFC 3339 timestamp of the most recent registration/rotation.
    pub registered_at: String,
    /// Shared human label; enrolled instances have distinct routing names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Hash of the initial, client-generated launch credential. Retained after
    /// rotation so retrying enrollment cannot allocate a second identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_key_hash: Option<String>,
    /// Retirement permanently reserves this identity and routing name.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retired: bool,
}

/// Authenticated identity handed to method handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentIdentity {
    pub agent_id: String,
    pub name: String,
    pub kind: String,
    pub capabilities: Vec<String>,
}

/// Successful registration response. `token` is shown exactly once.
#[derive(Debug, Clone, Serialize)]
pub struct Registration {
    pub agent_id: String,
    pub token: String,
    pub name: String,
    pub kind: String,
    pub capabilities: Vec<String>,
    /// True when an existing name was re-registered with the old token.
    pub rotated: bool,
}

/// Idempotent per-launch provisioning reply. The caller supplies the secret,
/// so a lost reply can be retried without persisting any plaintext credential.
#[derive(Debug, Clone, Serialize)]
pub struct Enrollment {
    pub agent_id: String,
    pub token: String,
    pub name: String,
    pub kind: String,
    pub display_name: String,
    pub capabilities: Vec<String>,
    pub resumed: bool,
}

/// Non-secret canonical instance metadata.
#[derive(Debug, Clone, Serialize)]
pub struct AgentMetadata {
    pub agent_id: String,
    pub name: String,
    pub kind: String,
    pub display_name: String,
    pub capabilities: Vec<String>,
    pub retired: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct RegistryFile {
    v: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required_features: Vec<String>,
    #[serde(default)]
    agents: BTreeMap<String, AgentRecord>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    revoked_token_hashes: BTreeSet<String>,
}

/// In-memory registry backed by `registry.json`.
#[derive(Debug)]
pub struct Registry {
    path: PathBuf,
    format_version: u16,
    required_features: Vec<String>,
    /// Any uncertain activation I/O requires reopening, even when replacement
    /// of the registry may already have succeeded.
    activation_fenced: bool,
    agents: BTreeMap<String, AgentRecord>,
    /// token digest -> name
    tokens: HashMap<String, String>,
    /// agent_id -> name
    ids: HashMap<String, String>,
    /// Launch-key digest -> immutable routing name, including retired launches.
    launches: HashMap<String, String>,
    revoked_token_hashes: BTreeSet<String>,
}

impl Registry {
    /// Loads the registry from `root/registry.json`, creating an empty one
    /// when the file does not exist.
    pub fn load(root: &Path) -> Result<Self, BrokerError> {
        let path = root.join(REGISTRY_FILE);
        let (format_version, required_features, agents, revoked_token_hashes) =
            match fs::read(&path) {
                Ok(bytes) => {
                    let file: RegistryFile = serde_json::from_slice(&bytes).map_err(|error| {
                        BrokerError::internal(format!("invalid {}: {error}", path.display()))
                    })?;
                    if file.v != REGISTRY_VERSION && file.v != ARRIVALS_REGISTRY_VERSION {
                        return Err(BrokerError::internal(format!(
                            "unsupported registry version {} in {}",
                            file.v,
                            path.display()
                        )));
                    }
                    for feature in &file.required_features {
                        if feature != ARRIVALS_REQUIRED_FEATURE {
                            return Err(BrokerError::internal(format!(
                                "unsupported required registry feature {feature:?} in {}",
                                path.display()
                            )));
                        }
                    }
                    let arrivals_required = file
                        .required_features
                        .iter()
                        .any(|feature| feature == ARRIVALS_REQUIRED_FEATURE);
                    if (file.v == REGISTRY_VERSION && !file.required_features.is_empty())
                        || (file.v == ARRIVALS_REGISTRY_VERSION && !arrivals_required)
                    {
                        return Err(BrokerError::internal(format!(
                            "registry version {} has incompatible required_features in {}",
                            file.v,
                            path.display()
                        )));
                    }
                    (
                        file.v,
                        file.required_features,
                        file.agents,
                        file.revoked_token_hashes,
                    )
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => (
                    REGISTRY_VERSION,
                    Vec::new(),
                    BTreeMap::new(),
                    BTreeSet::new(),
                ),
                Err(error) => return Err(error.into()),
            };

        if path.exists() {
            let metadata = fs::metadata(&path)?;
            if metadata.permissions().mode() & 0o777 != 0o600 {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
            }
        }

        let mut registry = Self {
            path,
            format_version,
            required_features,
            activation_fenced: false,
            agents,
            tokens: HashMap::new(),
            ids: HashMap::new(),
            launches: HashMap::new(),
            revoked_token_hashes,
        };
        registry.reindex()?;
        Ok(registry)
    }

    /// Loaded format, preserved by registration, rotation and retirement.
    pub fn format_version(&self) -> u16 {
        self.format_version
    }

    pub fn required_features(&self) -> &[String] {
        &self.required_features
    }

    pub fn arrivals_enabled(&self) -> bool {
        self.format_version == ARRIVALS_REGISTRY_VERSION
            && self
                .required_features
                .iter()
                .any(|feature| feature == ARRIVALS_REQUIRED_FEATURE)
    }

    /// Retired identities remain reserved and terminate arrival obligations.
    pub fn retired_agent_ids(&self) -> BTreeSet<String> {
        self.agents
            .values()
            .filter(|record| record.retired)
            .map(|record| record.agent_id.clone())
            .collect()
    }

    pub(crate) fn check_activation_ready(&self) -> Result<(), BrokerError> {
        if self.activation_fenced {
            return Err(BrokerError::internal(
                "arrival activation requires registry reopen after uncertain I/O",
            ));
        }
        Ok(())
    }

    pub(crate) fn root_path(&self) -> &Path {
        self.path.parent().unwrap_or_else(|| Path::new("."))
    }

    pub(crate) fn fence_activation(&mut self) {
        self.activation_fenced = true;
    }

    /// Called only after stable metadata and its directory entries are synced.
    /// On failure preserve the new format in memory and keep the fence: an
    /// unsuccessful directory sync does not prove rename failed.
    pub(crate) fn promote_arrivals(&mut self) -> Result<(), BrokerError> {
        self.activation_fenced = true;
        self.format_version = ARRIVALS_REGISTRY_VERSION;
        if !self
            .required_features
            .iter()
            .any(|feature| feature == ARRIVALS_REQUIRED_FEATURE)
        {
            self.required_features
                .push(ARRIVALS_REQUIRED_FEATURE.to_string());
        }
        self.write_snapshot()?;
        self.activation_fenced = false;
        Ok(())
    }

    /// Number of registered agents.
    pub fn len(&self) -> usize {
        self.records().count()
    }

    /// True when no agent is registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterates over persisted records in name order.
    pub fn records(&self) -> impl Iterator<Item = &AgentRecord> {
        self.agents.values().filter(|record| !record.retired)
    }

    /// Iterates over `(name, record)` pairs in name order.
    pub fn records_with_names(&self) -> impl Iterator<Item = (&str, &AgentRecord)> {
        self.agents
            .iter()
            .filter(|(_, record)| !record.retired)
            .map(|(name, record)| (name.as_str(), record))
    }

    /// Resolves a token to its agent identity.
    pub fn authenticate(&self, token: &str) -> Option<AgentIdentity> {
        if token.is_empty() {
            return None;
        }
        let digest = token_hash(token);
        let name = self.tokens.get(&digest)?;
        let record = self.agents.get(name)?;
        Some(identity_of(name, record))
    }

    /// Resolves a name or agent id to a registered identity.
    pub fn resolve(&self, reference: &str) -> Option<AgentIdentity> {
        if let Some(record) = self.agents.get(reference) {
            return (!record.retired).then(|| identity_of(reference, record));
        }
        let name = self.ids.get(reference)?;
        let record = self.agents.get(name)?;
        (!record.retired).then(|| identity_of(name, record))
    }

    /// Registers a new name or rotates the token of an existing one.
    pub fn register(
        &mut self,
        name: String,
        kind: Option<String>,
        capabilities: Option<Vec<String>>,
        old_token: Option<&str>,
    ) -> Result<Registration, BrokerError> {
        self.check_activation_ready()?;
        validate_name(&name)?;
        if self.ids.contains_key(&name) {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                format!("agent name {name:?} collides with an existing agent id"),
            ));
        }
        let kind = kind.unwrap_or_else(|| "agent".to_string());
        validate_kind(&kind)?;
        let capabilities = capabilities.unwrap_or_default();
        validate_capabilities(&capabilities)?;

        let token = self.unused_token()?;
        let digest = token_hash(&token);
        let previous = self.agents.clone();
        let previous_revoked = self.revoked_token_hashes.clone();

        let (agent_id, rotated) = if let Some(existing) = self.agents.get(&name) {
            if existing.retired {
                return Err(launch_conflict(existing, &name, "instance is retired"));
            }
            let old_token = old_token.ok_or_else(|| {
                BrokerError::application(
                    ErrorCode::Conflict,
                    format!(
                        "agent name {name:?} is already registered; supply the existing token to rotate"
                    ),
                )
            })?;
            let identity = self.authenticate(old_token).ok_or_else(|| {
                BrokerError::application(
                    ErrorCode::Conflict,
                    format!("token does not match the registered agent name {name:?}"),
                )
            })?;
            if identity.name != name {
                return Err(BrokerError::application(
                    ErrorCode::Conflict,
                    format!("token belongs to {:?}, not {name:?}", identity.name),
                ));
            }
            self.revoked_token_hashes
                .insert(existing.token_hash.clone());
            (existing.agent_id.clone(), true)
        } else {
            let agent_id = self.unused_id()?;
            (agent_id, false)
        };

        let display_name = self
            .agents
            .get(&name)
            .and_then(|old| old.display_name.clone());
        let launch_key_hash = self
            .agents
            .get(&name)
            .and_then(|old| old.launch_key_hash.clone());

        self.agents.insert(
            name.clone(),
            AgentRecord {
                agent_id: agent_id.clone(),
                token_hash: digest,
                kind: kind.clone(),
                capabilities: capabilities.clone(),
                registered_at: id::now_rfc3339(),
                display_name,
                launch_key_hash,
                retired: false,
            },
        );

        if let Err(error) = self.reindex().and_then(|()| self.save()) {
            self.agents = previous;
            self.revoked_token_hashes = previous_revoked;
            let _ = self.reindex();
            return Err(error);
        }

        Ok(Registration {
            agent_id,
            token,
            name,
            kind,
            capabilities,
            rotated,
        })
    }

    /// Provisions a launch using a fresh 128-bit lowercase hexadecimal secret.
    /// Reusing that secret retries this launch; a different launch must generate
    /// a different secret even when its kind and display name are identical.
    pub fn enroll(
        &mut self,
        launch_key: String,
        kind: String,
        display_name: String,
        capabilities: Option<Vec<String>>,
    ) -> Result<Enrollment, BrokerError> {
        self.check_activation_ready()?;
        validate_launch_key(&launch_key)?;
        validate_kind(&kind)?;
        validate_name(&display_name)?;
        let capabilities = capabilities.unwrap_or_default();
        validate_capabilities(&capabilities)?;
        let digest = token_hash(&launch_key);
        if let Some(name) = self.launches.get(&digest) {
            let record = self.agents.get(name).expect("indexed launch exists");
            if record.retired {
                return Err(launch_conflict(record, name, "instance is retired"));
            }
            if record.token_hash != digest {
                return Err(launch_conflict(
                    record,
                    name,
                    "launch credential was rotated; authenticate with the current credential",
                ));
            }
            if record.kind != kind
                || record.display_name.as_deref() != Some(display_name.as_str())
                || record.capabilities != capabilities
            {
                return Err(launch_conflict(
                    record,
                    name,
                    "retry metadata differs from the original launch",
                ));
            }
            return Ok(enrollment_of(name, record, launch_key, true));
        }
        if self.tokens.contains_key(&digest) || self.revoked_token_hashes.contains(&digest) {
            return Err(BrokerError::application(
                ErrorCode::Conflict,
                "launch credential is already reserved; generate a fresh launch key",
            ));
        }
        let (agent_id, name) = loop {
            let agent_id = self.unused_id()?;
            let name = format!("instance-{agent_id}");
            if !self.agents.contains_key(&name) && !self.ids.contains_key(&name) {
                break (agent_id, name);
            }
        };
        let record = AgentRecord {
            agent_id,
            token_hash: digest.clone(),
            kind,
            capabilities,
            registered_at: id::now_rfc3339(),
            display_name: Some(display_name),
            launch_key_hash: Some(digest),
            retired: false,
        };
        let response = enrollment_of(&name, &record, launch_key, false);
        self.agents.insert(name.clone(), record);
        if let Err(error) = self.reindex().and_then(|()| self.save()) {
            self.agents.remove(&name);
            let _ = self.reindex();
            return Err(error);
        }
        Ok(response)
    }

    /// Permanently retires the authenticated identity. Its ID, alias, launch
    /// association and all revoked credential hashes remain reserved on disk.
    pub fn retire(&mut self, token: &str) -> Result<AgentMetadata, BrokerError> {
        self.check_activation_ready()?;
        let identity = self.authenticate(token).ok_or_else(|| {
            BrokerError::application(ErrorCode::Unauthenticated, "invalid agent token")
        })?;
        let previous = self.agents.clone();
        let previous_revoked = self.revoked_token_hashes.clone();
        let record = self
            .agents
            .get_mut(&identity.name)
            .expect("authenticated agent exists");
        record.retired = true;
        self.revoked_token_hashes.insert(record.token_hash.clone());
        let metadata = metadata_of(&identity.name, record);
        if let Err(error) = self.reindex().and_then(|()| self.save()) {
            self.agents = previous;
            self.revoked_token_hashes = previous_revoked;
            let _ = self.reindex();
            return Err(error);
        }
        Ok(metadata)
    }

    /// Returns non-secret metadata for an authenticated active instance.
    pub fn instance_metadata(&self, token: &str) -> Result<AgentMetadata, BrokerError> {
        let identity = self.authenticate(token).ok_or_else(|| {
            BrokerError::application(ErrorCode::Unauthenticated, "invalid agent token")
        })?;
        Ok(metadata_of(&identity.name, &self.agents[&identity.name]))
    }

    fn unused_id(&self) -> Result<String, BrokerError> {
        loop {
            let candidate = id::new_ulid()
                .map_err(|error| BrokerError::internal(format!("generate agent id: {error}")))?;
            if !self.ids.contains_key(&candidate) && !self.agents.contains_key(&candidate) {
                return Ok(candidate);
            }
        }
    }

    fn unused_token(&self) -> Result<String, BrokerError> {
        loop {
            let candidate = new_token()?;
            let digest = token_hash(&candidate);
            if !self.tokens.contains_key(&digest)
                && !self.launches.contains_key(&digest)
                && !self.revoked_token_hashes.contains(&digest)
            {
                return Ok(candidate);
            }
        }
    }

    fn reindex(&mut self) -> Result<(), BrokerError> {
        self.tokens.clear();
        self.ids.clear();
        self.launches.clear();
        for (name, record) in &self.agents {
            if !id::is_ulid(&record.agent_id) {
                return Err(BrokerError::internal(format!(
                    "registry entry {name:?} has an invalid agent_id"
                )));
            }
            if let Some(digest) = &record.launch_key_hash {
                if self.launches.insert(digest.clone(), name.clone()).is_some() {
                    return Err(BrokerError::internal(
                        "duplicate launch key hash in registry",
                    ));
                }
            }
            if self
                .ids
                .insert(record.agent_id.clone(), name.clone())
                .is_some()
            {
                return Err(BrokerError::internal(format!(
                    "duplicate agent_id {} in registry",
                    record.agent_id
                )));
            }
            if record.retired {
                continue;
            }
            if self.revoked_token_hashes.contains(&record.token_hash) {
                return Err(BrokerError::internal(format!(
                    "revoked token is current for agent {name:?}"
                )));
            }
            if self
                .tokens
                .insert(record.token_hash.clone(), name.clone())
                .is_some()
            {
                return Err(BrokerError::internal(format!(
                    "duplicate token hash for agent {name:?}"
                )));
            }
        }
        Ok(())
    }

    fn save(&self) -> Result<(), BrokerError> {
        self.check_activation_ready()?;
        self.write_snapshot()
    }

    fn write_snapshot(&self) -> Result<(), BrokerError> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let sequence = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(
            ".registry.json.tmp.{}.{}.{}",
            std::process::id(),
            id::unix_millis(),
            sequence
        ));
        let file = RegistryFile {
            v: self.format_version,
            required_features: self.required_features.clone(),
            agents: self.agents.clone(),
            revoked_token_hashes: self.revoked_token_hashes.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&file)
            .map_err(|error| BrokerError::internal(format!("serialize registry: {error}")))?;

        let result = (|| -> io::Result<()> {
            let mut handle = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            handle.write_all(&bytes)?;
            handle.sync_all()?;
            drop(handle);
            fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
            fs::rename(&temporary, &self.path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.map_err(BrokerError::from)
    }
}

impl DaemonState {
    /// Enrolls one independently launched client or resumes its initial retry.
    pub async fn enroll_agent(
        &self,
        launch_key: String,
        kind: String,
        display_name: String,
        capabilities: Option<Vec<String>>,
    ) -> Result<Enrollment, BrokerError> {
        let enrollment =
            self.registry
                .write()
                .await
                .enroll(launch_key, kind, display_name, capabilities)?;
        self.ensure_inbox(&enrollment.agent_id).await?;
        let identity = AgentIdentity {
            agent_id: enrollment.agent_id.clone(),
            name: enrollment.name.clone(),
            kind: enrollment.kind.clone(),
            capabilities: enrollment.capabilities.clone(),
        };
        self.register_presence(&identity).await?;
        Ok(enrollment)
    }

    /// Revokes a credential and permanently reserves its instance identity.
    pub async fn retire_agent(&self, token: &str) -> Result<AgentMetadata, BrokerError> {
        let _serial = self.send_lock.lock().await;
        let metadata = self.registry.write().await.retire(token)?;
        if let Some(arrivals) = &self.arrivals {
            arrivals.retire_agent(&metadata.agent_id);
        }
        Ok(metadata)
    }

    /// Reads canonical instance metadata without returning any secret.
    pub async fn instance_metadata(&self, token: &str) -> Result<AgentMetadata, BrokerError> {
        self.registry.read().await.instance_metadata(token)
    }

    /// Registers a new agent name or rotates an existing name's token.
    pub async fn register_agent(
        &self,
        name: String,
        kind: Option<String>,
        capabilities: Option<Vec<String>>,
        old_token: Option<&str>,
    ) -> Result<Registration, BrokerError> {
        let registration = {
            let mut registry = self.registry.write().await;
            registry.register(name, kind, capabilities, old_token)?
        };

        self.ensure_inbox(&registration.agent_id).await?;
        let identity = AgentIdentity {
            agent_id: registration.agent_id.clone(),
            name: registration.name.clone(),
            kind: registration.kind.clone(),
            capabilities: registration.capabilities.clone(),
        };
        self.register_presence(&identity).await?;
        Ok(registration)
    }

    /// Authenticates a token and treats the call as a heartbeat.
    pub async fn authenticate(&self, token: &str) -> Result<AgentIdentity, BrokerError> {
        let identity = {
            let registry = self.registry.read().await;
            registry.authenticate(token).ok_or_else(|| {
                BrokerError::application(ErrorCode::Unauthenticated, "invalid agent token")
            })?
        };
        self.touch_presence(&identity).await;
        Ok(identity)
    }
}

fn metadata_of(name: &str, record: &AgentRecord) -> AgentMetadata {
    AgentMetadata {
        agent_id: record.agent_id.clone(),
        name: name.to_string(),
        kind: record.kind.clone(),
        display_name: record
            .display_name
            .clone()
            .unwrap_or_else(|| name.to_string()),
        capabilities: record.capabilities.clone(),
        retired: record.retired,
    }
}

fn enrollment_of(name: &str, record: &AgentRecord, token: String, resumed: bool) -> Enrollment {
    Enrollment {
        agent_id: record.agent_id.clone(),
        token,
        name: name.to_string(),
        kind: record.kind.clone(),
        display_name: record
            .display_name
            .clone()
            .unwrap_or_else(|| name.to_string()),
        capabilities: record.capabilities.clone(),
        resumed,
    }
}

fn launch_conflict(record: &AgentRecord, name: &str, reason: &str) -> BrokerError {
    BrokerError::application_with_data(
        ErrorCode::Conflict,
        reason,
        serde_json::json!({
            "agent_id": record.agent_id,
            "name": name,
            "retired": record.retired,
        }),
    )
}

fn identity_of(name: &str, record: &AgentRecord) -> AgentIdentity {
    AgentIdentity {
        agent_id: record.agent_id.clone(),
        name: name.to_string(),
        kind: record.kind.clone(),
        capabilities: record.capabilities.clone(),
    }
}

/// Generates a 128-bit token as 32 lowercase hexadecimal characters.
pub fn new_token() -> Result<String, BrokerError> {
    let bytes = id::random_bytes::<TOKEN_BYTES>()
        .map_err(|error| BrokerError::internal(format!("read random token bytes: {error}")))?;
    let mut token = String::with_capacity(TOKEN_BYTES * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(token, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(token)
}

/// Returns the stored representation of a token: `sha256:<hex>`.
pub fn token_hash(token: &str) -> String {
    format!("sha256:{}", sha256_hex(token.as_bytes()))
}

fn validate_name(name: &str) -> Result<(), BrokerError> {
    if name.is_empty() {
        return Err(BrokerError::invalid("agent name must not be empty"));
    }
    if name.len() > 256 {
        return Err(BrokerError::invalid("agent name must be at most 256 bytes"));
    }
    if name.chars().any(char::is_control) {
        return Err(BrokerError::invalid(
            "agent name must not contain control characters",
        ));
    }
    Ok(())
}

fn validate_launch_key(key: &str) -> Result<(), BrokerError> {
    if key.len() != TOKEN_BYTES * 2
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(BrokerError::invalid(
            "launch_key must be a fresh 128-bit secret encoded as 32 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_kind(kind: &str) -> Result<(), BrokerError> {
    if kind.is_empty() || kind.len() > 64 {
        return Err(BrokerError::invalid("agent kind must be 1..=64 bytes"));
    }
    if kind.chars().any(char::is_control) {
        return Err(BrokerError::invalid(
            "agent kind must not contain control characters",
        ));
    }
    Ok(())
}

fn validate_capabilities(capabilities: &[String]) -> Result<(), BrokerError> {
    if capabilities.len() > 64 {
        return Err(BrokerError::invalid(
            "at most 64 capabilities may be registered",
        ));
    }
    for capability in capabilities {
        if capability.is_empty() || capability.len() > 128 {
            return Err(BrokerError::invalid(
                "each capability must be 1..=128 bytes",
            ));
        }
        if capability.chars().any(char::is_control) {
            return Err(BrokerError::invalid(
                "capabilities must not contain control characters",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "coop-registry-{tag}-{}-{}",
            std::process::id(),
            id::unix_millis()
        ))
    }

    #[test]
    fn token_hash_is_sha256_and_tokens_are_128_bits() {
        let token = new_token().expect("token");
        assert_eq!(token.len(), 32);
        assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(token_hash(&token).starts_with("sha256:"));
        assert_eq!(token_hash(&token).len(), 7 + 64);
    }

    #[test]
    fn registry_roundtrip_keeps_only_token_hashes() {
        let root = temp_root("roundtrip");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let mut registry = Registry::load(&root).expect("load empty registry");
        let registration = registry
            .register("alice".into(), Some("codex".into()), None, None)
            .expect("register");
        assert!(!registration.rotated);
        assert!(registry.authenticate(&registration.token).is_some());

        let raw = fs::read_to_string(root.join(REGISTRY_FILE)).expect("registry bytes");
        assert!(!raw.contains(&registration.token));
        assert!(raw.contains(&token_hash(&registration.token)));
        let mode = fs::metadata(root.join(REGISTRY_FILE))
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);

        let reloaded = Registry::load(&root).expect("reload");
        let identity = reloaded.authenticate(&registration.token).expect("auth");
        assert_eq!(identity.agent_id, registration.agent_id);
        assert_eq!(identity.name, "alice");
        let _ = fs::remove_dir_all(&root);
    }
}
