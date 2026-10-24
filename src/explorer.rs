//! Startup-configured local observation credential, independent of agents.

use std::path::Path;

use crate::client::ClientError;
use crate::registry::token_hash;

/// Reads a private regular file containing a single raw bearer token.
pub fn read_token_file(path: &Path) -> Result<String, ClientError> {
    let bytes = crate::transport::profile::read_bounded(path, true)?;
    let token = std::str::from_utf8(&bytes)
        .map_err(|_| ClientError::Protocol("token file must contain UTF-8 text".into()))?
        .trim();
    if token.is_empty() || token.len() > 4096 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(ClientError::Protocol(
            "token file must contain one nonempty token of at most 4096 bytes".into(),
        ));
    }
    Ok(token.to_owned())
}

/// Only the digest is retained after startup. This credential has no agent ID.
pub(crate) struct ExplorerCredential {
    digest: String,
}

impl std::fmt::Debug for ExplorerCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExplorerCredential([redacted])")
    }
}

impl ExplorerCredential {
    pub(crate) fn conflicts_with(
        &self,
        registry: &crate::registry::Registry,
        policy: Option<&crate::network::NetworkPolicy>,
    ) -> bool {
        registry.records().any(|record| {
            record.token_hash == self.digest
                || record.launch_key_hash.as_ref() == Some(&self.digest)
        }) || policy.is_some_and(|policy| {
            policy
                .devices
                .iter()
                .any(|device| device.token_hash == self.digest)
        })
    }

    pub(crate) fn load(path: &Path) -> Result<Self, ClientError> {
        let token = read_token_file(path)?;
        if token.len() < 32 {
            return Err(ClientError::Protocol(
                "explorer token must contain at least 32 characters".into(),
            ));
        }
        Ok(Self {
            digest: token_hash(&token),
        })
    }

    pub(crate) fn matches(&self, token: &str) -> bool {
        let digest = token_hash(token);
        self.digest
            .bytes()
            .zip(digest.bytes())
            .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
            == 0
    }
}
