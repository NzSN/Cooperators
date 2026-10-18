//! Root-directory and socket-path resolution.
//!
//! Precedence follows design §10: `COOPERATORS_HOME` environment variable,
//! then the `--root` flag, then the platform default
//! (`$HOME/.local/share/cooperators`). `COOP_SOCKET` may override the socket
//! path for clients that receive an explicit socket from their MCP server
//! configuration (design §4).

use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// Environment variable that overrides the bus root.
pub const ROOT_ENV: &str = "COOPERATORS_HOME";
/// Environment variable that overrides the socket path.
pub const SOCKET_ENV: &str = "COOP_SOCKET";

/// Resolves the bus root from the environment, an explicit flag, or the
/// platform default, in that order.
pub fn resolve_root(explicit: Option<&Path>) -> PathBuf {
    non_empty_env(ROOT_ENV)
        .map(PathBuf::from)
        .or_else(|| explicit.map(Path::to_path_buf))
        .unwrap_or_else(default_root)
}

/// Default root: `$HOME/.local/share/cooperators`.
pub fn default_root() -> PathBuf {
    match non_empty_env("HOME") {
        Some(home) => PathBuf::from(home).join(".local/share/cooperators"),
        None => PathBuf::from(".local/share/cooperators"),
    }
}

/// Socket path for a root: `$COOP_SOCKET` when set, otherwise
/// `<root>/run/daemon.sock`.
pub fn socket_path(root: &Path) -> PathBuf {
    non_empty_env(SOCKET_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("run/daemon.sock"))
}

/// Creates the on-disk layout and enforces 0700 on the root and run
/// directories. Existing files are never modified.
pub fn ensure_layout(root: &Path) -> io::Result<()> {
    create_private_dir(root)?;
    create_private_dir(&root.join("run"))?;
    for relative in [
        "agents",
        "channels",
        "queues",
        "dead",
        "docs",
        "blobs/sha256",
    ] {
        fs::create_dir_all(root.join(relative))?;
    }
    Ok(())
}

/// Creates a directory (if needed) and sets its mode to 0700.
pub fn create_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

fn non_empty_env(name: &str) -> Option<OsString> {
    std::env::var_os(name).filter(|value| !value.is_empty())
}
