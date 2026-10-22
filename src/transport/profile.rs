//! Named JSON profiles and private credential references for native clients.

use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::client::ClientError;

use super::{BrokerClient, HttpsOptions};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profiles {
    v: u16,
    profiles: BTreeMap<String, Profile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    endpoint: String,
    expected_bus_id: String,
    credential_file: PathBuf,
    ca_file: Option<PathBuf>,
    identity_file: Option<PathBuf>,
    proxy: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    token: String,
}

/// A resolved profile deliberately does not implement Debug: it owns a token.
pub struct ProfileConnection {
    pub client: BrokerClient,
    pub token: String,
    pub expected_bus_id: String,
}

pub fn load_profile(file: &Path, name: &str) -> Result<ProfileConnection, ClientError> {
    let bytes = read_bounded(file, false)?;
    let mut profiles: Profiles = serde_json::from_slice(&bytes)
        .map_err(|_| ClientError::Protocol("invalid broker profiles file".into()))?;
    if profiles.v != 1 {
        return Err(ClientError::Protocol(
            "unsupported broker profiles version".into(),
        ));
    }
    let profile = profiles
        .profiles
        .remove(name)
        .ok_or_else(|| ClientError::Protocol("broker profile does not exist".into()))?;
    if !crate::id::is_ulid(&profile.expected_bus_id) {
        return Err(ClientError::Protocol(
            "profile requires a valid expected bus ID".into(),
        ));
    }
    let parent = file.parent().unwrap_or(Path::new("."));
    let resolve = |path: PathBuf| {
        if path.is_absolute() {
            path
        } else {
            parent.join(path)
        }
    };
    let credential: Credential =
        serde_json::from_slice(&read_bounded(&resolve(profile.credential_file), true)?)
            .map_err(|_| ClientError::Protocol("invalid private broker credential file".into()))?;
    if credential.token.is_empty() || credential.token.len() > 4096 {
        return Err(ClientError::Protocol(
            "invalid private broker credential".into(),
        ));
    }
    let options = HttpsOptions {
        ca_pem: profile
            .ca_file
            .map(|path| read_bounded(&resolve(path), false))
            .transpose()?,
        identity_pem: profile
            .identity_file
            .map(|path| read_bounded(&resolve(path), true))
            .transpose()?,
        proxy: profile.proxy,
    };
    Ok(ProfileConnection {
        client: BrokerClient::https(&profile.endpoint, options)?
            .with_expected_bus_id(profile.expected_bus_id.clone())?,
        token: credential.token,
        expected_bus_id: profile.expected_bus_id,
    })
}

pub(crate) fn read_bounded(path: &Path, private: bool) -> Result<Vec<u8>, ClientError> {
    use std::os::unix::fs::MetadataExt;
    let before = std::fs::symlink_metadata(path).map_err(ClientError::Unreachable)?;
    if !before.is_file() || (private && before.permissions().mode() & 0o077 != 0) {
        return Err(ClientError::Protocol("credential/config reference must be a regular file with private credential permissions".into()));
    }
    let file = std::fs::File::open(path).map_err(ClientError::Unreachable)?;
    let after = file.metadata().map_err(ClientError::Unreachable)?;
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || (private && after.permissions().mode() & 0o077 != 0)
    {
        return Err(ClientError::Protocol(
            "credential/config reference changed while opening".into(),
        ));
    }
    let mut bytes = Vec::new();
    file.take(64 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(ClientError::Unreachable)?;
    if bytes.len() > 64 * 1024 {
        return Err(ClientError::Protocol(
            "credential/config reference exceeds 64 KiB".into(),
        ));
    }
    Ok(bytes)
}
