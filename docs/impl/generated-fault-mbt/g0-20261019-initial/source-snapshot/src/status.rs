//! Status reporting: sequence heads for every log under the bus root.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

/// Top-level directories that hold JSONL logs (design §2 layout).
const LOG_DIRECTORIES: [&str; 5] = ["agents", "channels", "queues", "dead", "docs"];
/// Status reads at most this many bytes from the end of each log.
const TAIL_WINDOW: u64 = 64 * 1024;

/// Head metadata for one log file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LogHead {
    /// Highest `seq` found in the tail of the file.
    pub seq: u64,
    /// File size in bytes.
    pub bytes: u64,
}

/// Response payload for `daemon.status`.
#[derive(Debug, Clone, Serialize)]
pub struct StatusResponse {
    pub version: String,
    pub pid: u32,
    pub uptime_s: u64,
    pub root: String,
    pub started_at: String,
    pub seq_heads: BTreeMap<String, LogHead>,
}

/// Scans the known log directories for `*.jsonl` files and reports their
/// sequence heads. Paths are relative to `root` and use `/` separators.
pub fn scan_seq_heads(root: &Path) -> BTreeMap<String, LogHead> {
    let mut heads = BTreeMap::new();
    for directory in LOG_DIRECTORIES {
        walk(&root.join(directory), root, &mut heads);
    }
    heads
}

fn walk(directory: &Path, root: &Path, heads: &mut BTreeMap<String, LogHead>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            walk(&path, root, heads);
            continue;
        }
        if !file_type.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            continue;
        }
        let Some(seq) = last_sequence(&path) else {
            continue;
        };
        let bytes = entry.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        let key = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        heads.insert(key, LogHead { seq, bytes });
    }
}

fn last_sequence(path: &Path) -> Option<u64> {
    let mut file = fs::File::open(path).ok()?;
    let length = file.metadata().ok()?.len();
    let start = length.saturating_sub(TAIL_WINDOW);
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).ok()?;

    let window = if start > 0 {
        let first_newline = buffer.iter().position(|&byte| byte == b'\n')?;
        &buffer[first_newline + 1..]
    } else {
        &buffer[..]
    };

    for line in window.split(|&byte| byte == b'\n').rev() {
        if line.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_slice::<Value>(line) {
            if let Some(seq) = value.get("seq").and_then(Value::as_u64) {
                return Some(seq);
            }
        }
    }
    None
}
