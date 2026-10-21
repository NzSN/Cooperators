//! Bounded, committed-head readers. These never repair a live source stream.

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::error::BrokerError;
use crate::storage::{hash_record, segment_paths, GENESIS_HASH};

use super::SourceCoordinate;

/// Source records can contain inline payloads; feed records have a tighter bound.
pub(super) const MAX_SOURCE_RECORD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(super) struct ScanCursor {
    pub seq: u64,
    pub hash: String,
    segment_start: u64,
    offset: u64,
    separator_missing: bool,
}

impl Default for ScanCursor {
    fn default() -> Self {
        Self {
            seq: 0,
            hash: GENESIS_HASH.to_owned(),
            segment_start: 1,
            offset: 0,
            separator_missing: false,
        }
    }
}

pub(super) struct ScannedRecord {
    pub value: Value,
    pub hash: String,
}

impl ScanCursor {
    /// Read one record and advance only within a head already certified by the
    /// authoritative source writer/recovery. Offsets survive active rotation:
    /// segment sequence ranges identify the renamed file without rereading it.
    pub fn next(
        &mut self,
        path: &Path,
        head: &SourceCoordinate,
        max_record_bytes: usize,
    ) -> Result<Option<ScannedRecord>, BrokerError> {
        if self.seq > head.seq || (self.seq == head.seq && self.hash != head.hash) {
            return Err(BrokerError::internal("arrival source head is inconsistent"));
        }
        if self.seq == head.seq {
            return Ok(None);
        }
        let paths = segment_paths(path)?;
        let mut active_start = 1;
        for segment in paths {
            let sealed = segment != path;
            let (start, end) = if sealed {
                sealed_range(&segment)?
            } else {
                (active_start, u64::MAX)
            };
            if sealed {
                active_start = end
                    .checked_add(1)
                    .ok_or_else(|| BrokerError::internal("arrival source sequence exhausted"))?;
            }
            if end <= self.seq {
                continue;
            }
            let offset = if self.segment_start == start {
                self.offset
            } else {
                if start != self.seq + 1 {
                    return Err(BrokerError::internal("arrival source segment gap"));
                }
                0
            };
            let mut file = File::open(&segment)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut reader = BufReader::new(file);
            let mut bytes = Vec::new();
            // take() prevents an unbounded allocation for a malformed line.
            let mut count = std::io::Read::take(&mut reader, max_record_bytes as u64 + 1)
                .read_until(b'\n', &mut bytes)?;
            if self.segment_start == start && self.separator_missing && bytes == b"\n" {
                // Recovery retains a valid last record without its separator.
                // The authoritative writer inserts that newline before append.
                bytes.clear();
                count += std::io::Read::take(&mut reader, max_record_bytes as u64 + 1)
                    .read_until(b'\n', &mut bytes)?;
            }
            if count == 0 {
                return Err(BrokerError::internal(
                    "committed arrival source is truncated",
                ));
            }
            if bytes.len() > max_record_bytes {
                return Err(BrokerError::internal(
                    "arrival source record exceeds scan bound",
                ));
            }
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|error| BrokerError::internal(format!("arrival source JSON: {error}")))?;
            let seq = value
                .get("seq")
                .and_then(Value::as_u64)
                .ok_or_else(|| BrokerError::internal("arrival source has no sequence"))?;
            let previous = value
                .get("prev_hash")
                .and_then(Value::as_str)
                .ok_or_else(|| BrokerError::internal("arrival source has no hash link"))?;
            if seq != self.seq + 1 || previous != self.hash {
                return Err(BrokerError::internal("arrival source chain mismatch"));
            }
            let hash = hash_record(&value)?;
            if seq == head.seq && hash != head.hash {
                return Err(BrokerError::internal(
                    "arrival source committed hash mismatch",
                ));
            }
            self.seq = seq;
            self.hash = hash.clone();
            self.segment_start = start;
            self.offset = offset + count as u64;
            self.separator_missing = bytes.last() != Some(&b'\n');
            return Ok(Some(ScannedRecord { value, hash }));
        }
        Err(BrokerError::internal("committed arrival source is missing"))
    }
}

fn sealed_range(path: &PathBuf) -> Result<(u64, u64), BrokerError> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| BrokerError::internal("invalid arrival source segment name"))?;
    let mut pieces = name.split('-');
    let start = pieces.next().and_then(|part| part.parse::<u64>().ok());
    let end = pieces.next().and_then(|part| part.parse::<u64>().ok());
    match (start, end) {
        (Some(start), Some(end)) if start > 0 && end >= start => Ok((start, end)),
        _ => Err(BrokerError::internal(
            "invalid arrival source segment range",
        )),
    }
}
