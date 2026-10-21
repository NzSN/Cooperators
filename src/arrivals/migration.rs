//! Irreversible root-format activation, performed under exclusive root ownership
//! and before replaying any production journal.

use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::error::BrokerError;
use crate::id;
use crate::registry::Registry;

pub const ARRIVAL_META_VERSION: u16 = 1;
pub const NOTIFICATIONS_DIRECTORY: &str = "notifications";
pub const ARRIVAL_META_FILE: &str = "meta.json";

const MAX_META_BYTES: u64 = 4096;
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Immutable identity of this bus. It must survive restarts and credential
/// changes; replacing it would silently change every delivery identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArrivalMeta {
    pub v: u16,
    pub bus_id: String,
}

/// Validate an already activated root, or explicitly enable a V1 root.
///
/// Caller holds the exclusive root lease and has not replayed source logs yet.
/// A disabled V1 root stays V1. Existing V2 metadata is always required, even
/// when this launch did not request activation; it is never regenerated.
/// Any failed activation fences this Registry until actual disk reopen.
pub fn prepare(
    root: &Path,
    registry: &mut Registry,
    request_enable: bool,
) -> Result<Option<ArrivalMeta>, BrokerError> {
    registry.check_activation_ready()?;
    if registry.root_path() != root {
        return Err(BrokerError::invalid(
            "arrival metadata root does not match the loaded registry root",
        ));
    }
    let directory = root.join(NOTIFICATIONS_DIRECTORY);
    let path = directory.join(ARRIVAL_META_FILE);
    if registry.arrivals_enabled() {
        return match read_meta(&path) {
            Ok(meta) => Ok(Some(meta)),
            Err(error) => {
                registry.fence_activation();
                Err(error)
            }
        };
    }
    if !request_enable {
        return Ok(None);
    }

    // This fence is deliberately installed before I/O: any failed stage must
    // reconcile real on-disk state before a later attempt can enable writes.
    registry.fence_activation();
    durable_create_directory(root)?;
    durable_create_directory(&directory)?;
    let meta = match read_meta(&path) {
        Ok(meta) => meta,
        Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            let meta = ArrivalMeta {
                v: ARRIVAL_META_VERSION,
                bus_id: id::new_ulid()?,
            };
            persist_meta(&directory, &path, &meta)?;
            meta
        }
        Err(error) => return Err(error),
    };
    // An orphan metadata file can predate a crash before directory sync. Sync
    // all three entries again when reusing it, before the registry guard moves.
    File::open(&path)?.sync_all()?;
    File::open(&directory)?.sync_all()?;
    File::open(root)?.sync_all()?;
    registry.promote_arrivals()?;
    Ok(Some(meta))
}

fn read_meta(path: &Path) -> Result<ArrivalMeta, BrokerError> {
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(BrokerError::internal(
            "arrival metadata is not a regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_META_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_META_BYTES {
        return Err(BrokerError::internal("arrival metadata exceeds 4096 bytes"));
    }
    let meta: ArrivalMeta = serde_json::from_slice(&bytes).map_err(|error| {
        BrokerError::internal(format!(
            "invalid arrival metadata {}: {error}",
            path.display()
        ))
    })?;
    if meta.v != ARRIVAL_META_VERSION || !id::is_ulid(&meta.bus_id) {
        return Err(BrokerError::internal(format!(
            "incompatible arrival metadata in {}",
            path.display()
        )));
    }
    Ok(meta)
}

fn persist_meta(directory: &Path, path: &Path, meta: &ArrivalMeta) -> Result<(), BrokerError> {
    let temporary = directory.join(format!(
        ".meta.json.tmp.{}.{}.{}",
        std::process::id(),
        id::unix_millis(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let bytes = serde_json::to_vec_pretty(meta)
        .map_err(|error| BrokerError::internal(format!("serialize arrival metadata: {error}")))?;
    let result = (|| -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        File::open(directory)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(BrokerError::from)
}

/// Sync each newly created directory and its parent before returning. This
/// also handles a previously nonexistent bus root, rather than syncing only
/// entries inside a root whose own parent entry is still uncertain.
fn durable_create_directory(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        return Ok(());
    }
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => return Ok(()),
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "arrival directory path is not a directory",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    durable_create_directory(parent)?;
    DirBuilder::new().mode(0o700).create(path)?;
    File::open(path)?.sync_all()?;
    File::open(if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    })?
    .sync_all()?;
    Ok(())
}
