use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{BrokerError, ErrorCode};
use crate::registry::token_hash;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permission {
    pub method: String,
    /// Exact resource or a trailing-star prefix. `*` explicitly grants all.
    pub resources: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevicePrincipal {
    pub principal_id: String,
    #[serde(default = "initial_generation")]
    pub generation: u64,
    pub token_hash: String,
    pub expires_at_ms: u64,
    #[serde(default)]
    pub allow_enroll: bool,
    /// Persistent issued identity budget, including retired identities.
    pub max_instances: usize,
    pub instance_ttl_s: u64,
    pub permissions: Vec<Permission>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkPolicy {
    pub v: u16,
    pub allowed_hosts: Vec<String>,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    pub devices: Vec<DevicePrincipal>,
    #[serde(default = "default_connections")]
    pub max_connections: usize,
    #[serde(default = "default_requests")]
    pub requests_per_principal: usize,
    #[serde(default = "default_polls")]
    pub polls_per_principal: usize,
}

fn default_connections() -> usize {
    128
}
fn initial_generation() -> u64 {
    1
}
fn default_requests() -> usize {
    16
}
fn default_polls() -> usize {
    4
}

impl NetworkPolicy {
    pub fn load(path: &Path) -> Result<Self, BrokerError> {
        let bytes = crate::transport::profile::read_bounded(path, true).map_err(|_| {
            BrokerError::invalid("network policy must be a bounded private regular file")
        })?;
        let policy: Self = serde_json::from_slice(&bytes)
            .map_err(|_| BrokerError::invalid("invalid network policy JSON"))?;
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), BrokerError> {
        if self.v != 1
            || self.devices.len() > 1024
            || self.allowed_hosts.is_empty()
            || self.allowed_hosts.len() > 32
            || self.allowed_origins.len() > 32
            || !(1..=4096).contains(&self.max_connections)
            || !(1..=128).contains(&self.requests_per_principal)
            || !(1..=32).contains(&self.polls_per_principal)
            || self.polls_per_principal >= self.requests_per_principal
        {
            return Err(BrokerError::invalid(
                "invalid network policy version or bounds",
            ));
        }
        for host in &self.allowed_hosts {
            if !host
                .parse::<http::uri::Authority>()
                .is_ok_and(|a| explicit_authority(&a))
            {
                return Err(BrokerError::invalid(
                    "network hosts must be explicit authorities",
                ));
            }
        }
        for origin in &self.allowed_origins {
            let url = reqwest::Url::parse(origin)
                .map_err(|_| BrokerError::invalid("invalid allowed origin"))?;
            if !matches!(url.scheme(), "https" | "http")
                || url.origin().ascii_serialization() != *origin
            {
                return Err(BrokerError::invalid(
                    "allowed origins must be exact HTTP(S) origins",
                ));
            }
        }
        let mut names = BTreeSet::new();
        let mut tokens = BTreeSet::new();
        for device in &self.devices {
            if device.generation == 0
                || device.principal_id.is_empty()
                || device.principal_id.len() > 128
                || !device
                    .principal_id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
                || !names.insert(&device.principal_id)
                || !tokens.insert(&device.token_hash)
                || !crate::storage::is_chain_hash(&device.token_hash)
                || !(1..=10_000).contains(&device.max_instances)
                || !(1..=604_800).contains(&device.instance_ttl_s)
                || device.permissions.len() > 64
            {
                return Err(BrokerError::invalid(
                    "invalid or duplicate network principal",
                ));
            }
            for permission in &device.permissions {
                if !super::method_contracts()
                    .iter()
                    .any(|m| m.method == permission.method)
                    || permission.resources.is_empty()
                    || permission.resources.len() > 64
                    || permission.resources.iter().any(|r| {
                        r.is_empty()
                            || r.len() > 256
                            || r.strip_suffix('*').unwrap_or(r).contains('*')
                            || r.chars().any(char::is_control)
                    })
                {
                    return Err(BrokerError::invalid(
                        "invalid network method/resource permission",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn device(&self, principal_id: &str) -> Option<&DevicePrincipal> {
        self.devices
            .iter()
            .find(|d| d.principal_id == principal_id && d.expires_at_ms > crate::id::unix_millis())
    }

    pub fn authenticate_device(&self, token: &str) -> Result<&DevicePrincipal, BrokerError> {
        let digest = token_hash(token);
        self.devices
            .iter()
            .find(|d| d.token_hash == digest && d.expires_at_ms > crate::id::unix_millis())
            .ok_or_else(|| {
                BrokerError::application(
                    ErrorCode::Unauthenticated,
                    "approved principal credential required",
                )
            })
    }

    pub fn allows(&self, principal_id: &str, method: &str, resource: &str) -> bool {
        self.device(principal_id).is_some_and(|device| {
            device.permissions.iter().any(|p| {
                p.method == method
                    && p.resources
                        .iter()
                        .any(|pattern| resource_matches(pattern, resource))
            })
        })
    }

    pub fn has_method(&self, principal_id: &str, method: &str) -> bool {
        self.device(principal_id)
            .is_some_and(|d| d.permissions.iter().any(|p| p.method == method))
    }

    pub fn headers_allowed(&self, headers: &http::HeaderMap) -> bool {
        if headers.get_all(http::header::HOST).iter().count() != 1
            || headers.get_all(http::header::ORIGIN).iter().count() > 1
        {
            return false;
        }
        let host = headers
            .get(http::header::HOST)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<http::uri::Authority>().ok())
            .filter(explicit_authority);
        let Some(host) = host else { return false };
        if !self.allowed_hosts.iter().any(|allowed| {
            let a = allowed
                .parse::<http::uri::Authority>()
                .expect("validated authority");
            a.host().eq_ignore_ascii_case(host.host()) && a.port() == host.port()
        }) {
            return false;
        }
        match headers.get(http::header::ORIGIN) {
            None => true,
            Some(value) => value
                .to_str()
                .ok()
                .is_some_and(|origin| self.allowed_origins.iter().any(|allowed| allowed == origin)),
        }
    }
}

fn explicit_authority(authority: &http::uri::Authority) -> bool {
    let raw = authority.as_str();
    raw.len() <= 256
        && !authority.host().is_empty()
        && !raw.contains(['*', '@', '/'])
        && (raw == authority.host() || authority.port_u16().is_some())
}

fn resource_matches(pattern: &str, resource: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => resource.starts_with(prefix),
        None => pattern == resource,
    }
}
