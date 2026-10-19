//! Status reporting: sequence heads for every log under the bus root.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

/// Top-level directories that hold JSONL logs (design §2 layout).
const LOG_DIRECTORIES: [&str; 5] = ["agents", "channels", "queues", "dead", "docs"];
/// Initial backward-read window. Expand when a record spans this boundary.
const TAIL_WINDOW: u64 = 64 * 1024;

/// Head metadata for one log file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LogHead {
    /// Highest `seq` found in the tail of the file.
    pub seq: u64,
    /// Total bytes in the active file and its sealed segments.
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
            if entry.file_name() == ".segments" {
                // Sealed files belong to the original logical log. Include
                // even a log whose active file is absent during recovery.
                if let Ok(logs) = fs::read_dir(&path) {
                    for log in logs.flatten() {
                        if log.file_type().is_ok_and(|kind| kind.is_dir()) {
                            let logical = directory.join(log.file_name());
                            if logical.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
                                insert_head(&logical, root, heads);
                            }
                        }
                    }
                }
            } else {
                walk(&path, root, heads);
            }
            continue;
        }
        if !file_type.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            continue;
        }
        insert_head(&path, root, heads);
    }
}

fn insert_head(path: &Path, root: &Path, heads: &mut BTreeMap<String, LogHead>) {
    let Ok(paths) = crate::storage::segment_paths(path) else {
        return;
    };
    let Some(seq) = paths.iter().rev().find_map(|path| last_sequence(path)) else {
        return;
    };
    let bytes = paths
        .iter()
        .filter_map(|path| fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum();
    let key = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");
    heads.insert(key, LogHead { seq, bytes });
}

fn last_sequence(path: &Path) -> Option<u64> {
    let mut file = fs::File::open(path).ok()?;
    let length = file.metadata().ok()?.len();
    let mut window_bytes = TAIL_WINDOW.min(length);
    loop {
        let start = length.saturating_sub(window_bytes);
        file.seek(SeekFrom::Start(start)).ok()?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).ok()?;
        // Only complete record starts may be parsed. A final oversized line,
        // or a torn suffix hiding the previous record, needs a larger window.
        let window = if start > 0 {
            buffer
                .iter()
                .position(|&byte| byte == b'\n')
                .map_or(&[][..], |first| &buffer[first + 1..])
        } else {
            &buffer[..]
        };
        for line in window.split(|&byte| byte == b'\n').rev() {
            if let Ok(value) = serde_json::from_slice::<Value>(line) {
                if let Some(seq) = value.get("seq").and_then(Value::as_u64) {
                    return Some(seq);
                }
            }
        }
        if start == 0 {
            return None;
        }
        window_bytes = window_bytes.saturating_mul(2).min(length);
    }
}
