//! Agent registry and token authentication (design §4).
//!
//! `registry.json` is the only mutable state file below the bus root. It maps
//! display names to [`AgentRecord`]s; tokens are never stored, only their
//! SHA-256 digests. Writes are atomic (same-directory temporary file, fsync,
//! rename, parent fsync) and the file is mode 0600.
//!
//! A name is a display claim held by one agent. Re-registering an existing
//! name requires the current token and rotates it; the `agent_id` is stable
//! across rotations so inbox paths and provenance do not move.

use std::collections::{BTreeMap, HashMap};
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

/// On-disk registry schema version.
pub const REGISTRY_VERSION: u16 = 1;

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

#[derive(Debug, Serialize, Deserialize)]
struct RegistryFile {
    v: u16,
    #[serde(default)]
    agents: BTreeMap<String, AgentRecord>,
}

/// In-memory registry backed by `registry.json`.
#[derive(Debug)]
pub struct Registry {
    path: PathBuf,
    agents: BTreeMap<String, AgentRecord>,
    /// token digest -> name
    tokens: HashMap<String, String>,
    /// agent_id -> name
    ids: HashMap<String, String>,
}

impl Registry {
    /// Loads the registry from `root/registry.json`, creating an empty one
    /// when the file does not exist.
    pub fn load(root: &Path) -> Result<Self, BrokerError> {
        let path = root.join(REGISTRY_FILE);
        let agents = match fs::read(&path) {
            Ok(bytes) => {
                let file: RegistryFile = serde_json::from_slice(&bytes).map_err(|error| {
                    BrokerError::internal(format!("invalid {}: {error}", path.display()))
                })?;
                if file.v != REGISTRY_VERSION {
                    return Err(BrokerError::internal(format!(
                        "unsupported registry version {} in {}",
                        file.v,
                        path.display()
                    )));
                }
                file.agents
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => BTreeMap::new(),
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
            agents,
            tokens: HashMap::new(),
            ids: HashMap::new(),
        };
        registry.reindex()?;
        Ok(registry)
    }

    /// Number of registered agents.
    pub fn len(&self) -> usize {
        self.agents.len()
    }

    /// True when no agent is registered.
    pub fn is_empty(&self) -> bool {
        self.agents.is_empty()
    }

    /// Iterates over persisted records in name order.
    pub fn records(&self) -> impl Iterator<Item = &AgentRecord> {
        self.agents.values()
    }

    /// Iterates over `(name, record)` pairs in name order.
    pub fn records_with_names(&self) -> impl Iterator<Item = (&str, &AgentRecord)> {
        self.agents
            .iter()
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
            return Some(identity_of(reference, record));
        }
        let name = self.ids.get(reference)?;
        let record = self.agents.get(name)?;
        Some(identity_of(name, record))
    }

    /// Registers a new name or rotates the token of an existing one.
    pub fn register(
        &mut self,
        name: String,
        kind: Option<String>,
        capabilities: Option<Vec<String>>,
        old_token: Option<&str>,
    ) -> Result<Registration, BrokerError> {
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

        let token = new_token()?;
        let digest = token_hash(&token);
        let previous = self.agents.clone();

        let (agent_id, rotated) = if let Some(existing) = self.agents.get(&name) {
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
            (existing.agent_id.clone(), true)
        } else {
            let agent_id = id::new_ulid()
                .map_err(|error| BrokerError::internal(format!("generate agent id: {error}")))?;
            (agent_id, false)
        };

        self.agents.insert(
            name.clone(),
            AgentRecord {
                agent_id: agent_id.clone(),
                token_hash: digest,
                kind: kind.clone(),
                capabilities: capabilities.clone(),
                registered_at: id::now_rfc3339(),
            },
        );

        if let Err(error) = self.reindex().and_then(|()| self.save()) {
            self.agents = previous;
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

    fn reindex(&mut self) -> Result<(), BrokerError> {
        self.tokens.clear();
        self.ids.clear();
        for (name, record) in &self.agents {
            if !id::is_ulid(&record.agent_id) {
                return Err(BrokerError::internal(format!(
                    "registry entry {name:?} has an invalid agent_id"
                )));
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
            v: REGISTRY_VERSION,
            agents: self.agents.clone(),
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
