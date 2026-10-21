//! Append-only JSONL storage with a SHA-256 hash chain (design §3).
//!
//! # File format
//!
//! One JSON object per line, each line terminated by `\n`. A record carries
//! two chain fields: `seq` (strictly monotonic, starting at 1) and
//! `prev_hash` (the digest of the preceding record, or [`GENESIS_HASH`] for
//! the first record). The record itself has no self-hash field: its digest is
//! computed on demand and becomes the next record's `prev_hash`.
//!
//! # Canonicalization
//!
//! `prev_hash` is `"sha256:" + lowercase_hex(SHA-256(canonical_json))`, where
//! `canonical_json` is the record serialized as JSON **without** its
//! `prev_hash` member:
//!
//! - no insignificant whitespace;
//! - object members sorted lexicographically by key (UTF-8 byte order);
//! - arrays in their serialized order;
//! - strings escaped exactly as `serde_json` escapes them;
//! - numbers in `serde_json::Number`'s shortest round-trippable form.
//!
//! Only records whose serialized form is a top-level JSON object are valid
//! chain records. Implementors of [`ChainRecord`] must expose `seq` and
//! `prev_hash` as top-level JSON fields with those exact names.
//!
//! # Recovery
//!
//! [`LogFile::open`] replays the whole file, verifies every link, and
//! truncates a torn tail: an unparsable or hash-invalid final record is
//! removed by truncating at the last valid byte offset. Corruption in the
//! middle of a log is an error instead of silent data loss. A valid final
//! record that lacks its trailing newline is retained; the next append writes
//! the missing separator first.
//!
//! # Integrity boundary
//!
//! The chain authenticates *links*: record N+1 commits to record N. The v1
//! envelope has no self-hash field, so a modified final record cannot be
//! distinguished from a legitimate tail until another record is appended
//! (whose `prev_hash` will then mismatch). Anchor the head digest externally
//! if tamper evidence for the tail itself is required.
//!
//! # Segmentation and failed appends
//!
//! [`LogFile::open_rotating`] bounds active segments while preserving one global
//! sequence and hash chain. Sealed segments live in
//! `.segments/<original filename>/`; their names anchor their sequence range
//! and final digest. Sealed corruption is fatal. Only the original active file
//! can undergo torn-tail repair. A crash after sealing and before creating the
//! next active file is recovered by reopening the stream.
//!
//! Any append I/O error quarantines that handle, since complete or partial bytes
//! may already exist. Further appends return [`LogError::RecoveryRequired`]
//! without touching records or disk. Actual reopen/replay reconciles those bytes.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// `prev_hash` of the first record in a log: `sha256:` followed by 64 zeros.
pub const GENESIS_HASH: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// Default active segment limit for channels, queues, and document histories.
pub const DEFAULT_ROTATION_BYTES: u64 = 256 * 1024 * 1024;

/// A record that can participate in a JSONL hash chain.
pub trait ChainRecord: Serialize + DeserializeOwned {
    /// Sequence number stored in the record.
    fn seq(&self) -> u64;
    /// Assigns the sequence number chosen by the log.
    fn set_seq(&mut self, seq: u64);
    /// Hash link stored in the record.
    fn prev_hash(&self) -> &str;
    /// Assigns the hash link chosen by the log.
    fn set_prev_hash(&mut self, prev_hash: String);
}

/// Result of replaying a log file.
#[derive(Debug, Clone)]
pub struct ReplayReport<T> {
    /// Every valid record, in file order.
    pub records: Vec<T>,
    /// Highest sequence number recovered (0 for an empty log).
    pub seq_head: u64,
    /// Digest of the last valid record, or the genesis hash for an empty log.
    pub last_hash: String,
    /// Bytes removed from a torn tail (0 when the log was clean).
    pub truncated_bytes: u64,
    /// Byte offset of the end of the last valid record.
    pub last_valid_offset: u64,
    /// True when the last valid record had no trailing newline.
    pub trailing_newline_missing: bool,
}

impl<T> ReplayReport<T> {
    /// Returns true when replay repaired a torn tail.
    pub fn was_truncated(&self) -> bool {
        self.truncated_bytes > 0
    }
}

/// Receipt returned after a durable append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendReceipt {
    /// Assigned sequence number.
    pub seq: u64,
    /// Digest of the appended record.
    pub hash: String,
    /// Bytes written, including line separators.
    pub bytes_written: u64,
}

/// Errors from opening, replaying, or appending to a log.
#[derive(Debug)]
pub enum LogError {
    /// Underlying filesystem failure.
    Io(io::Error),
    /// Structurally invalid record that is not a repairable tail.
    Corrupt { line: usize, reason: String },
    /// Record could not be serialized.
    Serialize(serde_json::Error),
    /// Record is not a JSON object and therefore cannot carry chain fields.
    InvalidRecord(String),
    /// An earlier I/O failure made this handle's durable position uncertain.
    /// Drop it and open/replay the log before attempting another append.
    RecoveryRequired,
}

impl std::fmt::Display for LogError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "log I/O error: {error}"),
            Self::Corrupt { line, reason } => {
                write!(formatter, "log corruption at line {line}: {reason}")
            }
            Self::Serialize(error) => write!(formatter, "record serialization failed: {error}"),
            Self::InvalidRecord(reason) => write!(formatter, "invalid log record: {reason}"),
            Self::RecoveryRequired => {
                write!(formatter, "log recovery required before another append")
            }
        }
    }
}

impl std::error::Error for LogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Serialize(error) => Some(error),
            Self::Corrupt { .. } | Self::InvalidRecord(_) | Self::RecoveryRequired => None,
        }
    }
}

impl From<io::Error> for LogError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// One append-only log file with a single writer.
#[derive(Debug)]
pub struct LogFile<T> {
    path: PathBuf,
    file: File,
    next_seq: u64,
    last_hash: String,
    needs_newline: bool,
    record_count: u64,
    recovery_required: bool,
    rotation_bytes: Option<u64>,
    active_start_seq: u64,
    marker: PhantomData<fn() -> T>,
}

impl<T: ChainRecord> LogFile<T> {
    /// Opens (creating if needed), replays, and repairs a log file.
    ///
    /// The returned [`ReplayReport`] contains the recovered records and repair
    /// diagnostics. Torn tails are truncated before this function returns.
    pub fn open(path: impl Into<PathBuf>) -> Result<(Self, ReplayReport<T>), LogError> {
        Self::open_internal(path.into(), None)
    }

    /// Opens a segmented stream. Sequence numbers and hashes span every segment.
    /// A record larger than the threshold occupies a segment alone.
    pub fn open_rotating(
        path: impl Into<PathBuf>,
        threshold: u64,
    ) -> Result<(Self, ReplayReport<T>), LogError> {
        if threshold == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rotation threshold must be positive",
            )
            .into());
        }
        Self::open_internal(path.into(), Some(threshold))
    }

    /// Validates and reads the whole stream without creating or repairing files.
    /// Torn-tail diagnostics and offsets refer only to the active segment.
    pub fn read_segments(path: impl AsRef<Path>) -> Result<ReplayReport<T>, LogError> {
        read_stream(path.as_ref(), false).map(|(report, _)| report)
    }

    fn open_internal(
        path: PathBuf,
        rotation_bytes: Option<u64>,
    ) -> Result<(Self, ReplayReport<T>), LogError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let (report, active_start_seq) = read_stream::<T>(&path, true)?;
        let existed = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        if !existed {
            sync_parent_directory(&path)?;
        }

        if report.truncated_bytes > 0 {
            file.set_len(report.last_valid_offset)?;
        }
        // A full record from a failed sync may still exist only in the OS
        // cache. Replayed records can immediately satisfy a dedupe request,
        // so recovery must sync them before exposing the rebuilt indexes.
        #[cfg(feature = "mbt-injection")]
        crate::faults::reach(&path, crate::faults::FaultPoint::BeforeRecoverySync)?;
        file.sync_all()?;
        file.seek(SeekFrom::End(0))?;

        let log = Self {
            path,
            file,
            next_seq: report.seq_head + 1,
            last_hash: report.last_hash.clone(),
            needs_newline: report.trailing_newline_missing,
            record_count: report.records.len() as u64,
            recovery_required: false,
            rotation_bytes,
            active_start_seq,
            marker: PhantomData,
        };
        Ok((log, report))
    }

    /// Path of the underlying file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Sequence number that the next append will receive.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Highest sequence number currently durable.
    pub fn seq_head(&self) -> u64 {
        self.next_seq - 1
    }

    /// Digest of the last durable record (or the genesis hash when empty).
    pub fn last_hash(&self) -> &str {
        &self.last_hash
    }

    /// Number of valid records recovered plus appended through this handle.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// True after an uncertain I/O failure; only reopening clears this state.
    pub fn requires_recovery(&self) -> bool {
        self.recovery_required
    }

    /// Assigns `seq`/`prev_hash`, appends one line, and fsyncs before
    /// returning. An `Ok` receipt means the record is durable.
    pub fn append(&mut self, record: &mut T) -> Result<AppendReceipt, LogError> {
        if self.recovery_required {
            return Err(LogError::RecoveryRequired);
        }
        let result = self.append_inner(record);
        if matches!(&result, Err(LogError::Io(_))) {
            self.recovery_required = true;
        }
        result
    }

    /// Appends records in slice order, returning only after they are durable.
    ///
    /// For a nonrotating log, all records are serialized before any writes and
    /// one covering fsync commits the batch. The durable sequence/hash head is
    /// published only after that sync succeeds. An error can leave a complete
    /// or partial unacknowledged prefix on disk; I/O errors quarantine the
    /// handle until actual reopen/replay reconciles those bytes.
    ///
    /// Rotating streams use [`Self::append`] for each record, preserving every
    /// segment seal and directory durability boundary. They may commit a
    /// prefix before returning an error and perform one sync per record.
    /// An empty batch does no I/O, but still rejects a quarantined handle.
    pub fn append_batch(&mut self, records: &mut [T]) -> Result<Vec<AppendReceipt>, LogError> {
        if self.recovery_required {
            return Err(LogError::RecoveryRequired);
        }
        if records.is_empty() {
            return Ok(Vec::new());
        }
        if self.rotation_bytes.is_some() {
            return records
                .iter_mut()
                .map(|record| self.append(record))
                .collect();
        }
        let result = self.append_batch_inner(records);
        if matches!(&result, Err(LogError::Io(_))) {
            self.recovery_required = true;
        }
        result
    }

    fn append_batch_inner(&mut self, records: &mut [T]) -> Result<Vec<AppendReceipt>, LogError> {
        let next_seq = self
            .next_seq
            .checked_add(records.len() as u64)
            .ok_or_else(|| LogError::InvalidRecord("batch exhausts sequence numbers".into()))?;
        let mut previous_hash = self.last_hash.clone();
        let mut lines = Vec::with_capacity(records.len());
        let mut receipts = Vec::with_capacity(records.len());
        // Prepare the entire batch before touching disk. Serialization errors
        // leave the current durable position intact and the handle reusable.
        for (index, record) in records.iter_mut().enumerate() {
            let seq = self.next_seq + index as u64;
            record.set_seq(seq);
            record.set_prev_hash(previous_hash);
            let hash = hash_record(record)?;
            let serialized = serde_json::to_vec(record).map_err(LogError::Serialize)?;
            let mut line = Vec::with_capacity(serialized.len() + 2);
            if index == 0 && self.needs_newline {
                line.push(b'\n');
            }
            line.extend_from_slice(&serialized);
            line.push(b'\n');
            receipts.push(AppendReceipt {
                seq,
                hash: hash.clone(),
                bytes_written: line.len() as u64,
            });
            previous_hash = hash;
            lines.push(line);
        }

        self.file.seek(SeekFrom::End(0))?;
        #[cfg(feature = "mbt-injection")]
        let mut injections = Vec::new();
        for (_receipt, line) in receipts.iter().zip(&lines) {
            #[cfg(feature = "mbt-injection")]
            {
                crate::faults::reach(&self.path, crate::faults::FaultPoint::BeforeRotation)?;
                crate::faults::reach(&self.path, crate::faults::FaultPoint::BeforeRecordWrite)?;
            }
            #[cfg(feature = "mbt-injection")]
            let injection = crate::faults::take_append(&self.path);
            #[cfg(feature = "mbt-injection")]
            if let Some(injection) = &injection {
                if let crate::faults::AppendFault::PartialWrite { bytes } = injection.fault() {
                    let written = bytes.min(line.len() - 1);
                    if let Err(error) = self.file.write_all(&line[..written]) {
                        injection.io_failed(_receipt.seq, line.len());
                        return Err(error.into());
                    }
                    injection.partial_written(_receipt.seq, line.len(), written);
                    return Err(crate::faults::injected_error().into());
                }
            }
            #[cfg(feature = "mbt-injection")]
            if let Err(error) = self.file.write_all(line) {
                if let Some(injection) = &injection {
                    injection.io_failed(_receipt.seq, line.len());
                }
                return Err(error.into());
            }
            #[cfg(not(feature = "mbt-injection"))]
            self.file.write_all(line)?;
            #[cfg(feature = "mbt-injection")]
            if let Some(injection) = injection {
                // Keep the per-record pause/failure boundary. Successful
                // injections observe the one covering sync below.
                injection.before_sync(_receipt.seq, line.len())?;
                injections.push((injection, _receipt.seq, line.len()));
            }
        }
        let synced = self.file.sync_all();
        #[cfg(feature = "mbt-injection")]
        for (injection, seq, bytes) in injections {
            injection.synced(seq, bytes, synced.is_ok());
        }
        synced?;

        self.next_seq = next_seq;
        self.last_hash = previous_hash;
        self.needs_newline = false;
        self.record_count += records.len() as u64;
        Ok(receipts)
    }

    fn append_inner(&mut self, record: &mut T) -> Result<AppendReceipt, LogError> {
        record.set_seq(self.next_seq);
        record.set_prev_hash(self.last_hash.clone());
        let hash = hash_record(record)?;
        let serialized = serde_json::to_vec(record).map_err(LogError::Serialize)?;

        #[cfg(feature = "mbt-injection")]
        crate::faults::reach(&self.path, crate::faults::FaultPoint::BeforeRotation)?;
        self.rotate_if_needed(serialized.len() as u64 + 1)?;

        let mut line = Vec::with_capacity(serialized.len() + 2);
        if self.needs_newline {
            line.push(b'\n');
        }
        line.extend_from_slice(&serialized);
        line.push(b'\n');

        #[cfg(feature = "mbt-injection")]
        crate::faults::reach(&self.path, crate::faults::FaultPoint::BeforeRecordWrite)?;
        self.file.seek(SeekFrom::End(0))?;
        #[cfg(feature = "mbt-injection")]
        let injection = crate::faults::take_append(&self.path);
        #[cfg(feature = "mbt-injection")]
        if let Some(injection) = &injection {
            if let crate::faults::AppendFault::PartialWrite { bytes } = injection.fault() {
                let written = bytes.min(line.len() - 1);
                if let Err(error) = self.file.write_all(&line[..written]) {
                    injection.io_failed(self.next_seq, line.len());
                    return Err(error.into());
                }
                injection.partial_written(self.next_seq, line.len(), written);
                return Err(crate::faults::injected_error().into());
            }
        }
        #[cfg(feature = "mbt-injection")]
        if let Err(error) = self.file.write_all(&line) {
            if let Some(injection) = &injection {
                injection.io_failed(self.next_seq, line.len());
            }
            return Err(error.into());
        }
        #[cfg(not(feature = "mbt-injection"))]
        self.file.write_all(&line)?;
        #[cfg(feature = "mbt-injection")]
        if let Some(injection) = &injection {
            injection.before_sync(self.next_seq, line.len())?;
        }
        #[cfg(feature = "mbt-injection")]
        {
            let synced = self.file.sync_all();
            if let Some(injection) = &injection {
                injection.synced(self.next_seq, line.len(), synced.is_ok());
            }
            synced?;
        }
        #[cfg(not(feature = "mbt-injection"))]
        self.file.sync_all()?;

        let seq = self.next_seq;
        self.next_seq += 1;
        self.last_hash = hash.clone();
        self.needs_newline = false;
        self.record_count += 1;

        Ok(AppendReceipt {
            seq,
            hash,
            bytes_written: line.len() as u64,
        })
    }

    fn rotate_if_needed(&mut self, record_bytes: u64) -> Result<(), LogError> {
        let Some(threshold) = self.rotation_bytes else {
            return Ok(());
        };
        let current_bytes = self.file.metadata()?.len();
        if self.next_seq == self.active_start_seq
            || current_bytes
                .saturating_add(record_bytes)
                .saturating_add(u64::from(self.needs_newline))
                <= threshold
        {
            return Ok(());
        }
        if self.needs_newline {
            self.file.seek(SeekFrom::End(0))?;
            self.file.write_all(b"\n")?;
            self.needs_newline = false;
        }
        self.file.sync_all()?;
        let directory = segment_directory(&self.path)?;
        fs::create_dir_all(&directory)?;
        File::open(&directory)?.sync_all()?;
        sync_parent_directory(&directory)?;
        if let Some(parent) = directory.parent() {
            sync_parent_directory(parent)?;
        }
        let anchor = self
            .last_hash
            .strip_prefix("sha256:")
            .expect("chain hash prefix");
        let sealed = directory.join(format!(
            "{:020}-{:020}-{anchor}.jsonl",
            self.active_start_seq,
            self.seq_head()
        ));
        if sealed.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "sealed segment already exists; recovery required",
            )
            .into());
        }
        fs::rename(&self.path, &sealed)?;
        // Persist the destination name before persisting removal of the old
        // name. Both directory updates must precede creation of the new active.
        File::open(&directory)?.sync_all()?;
        sync_parent_directory(&self.path)?;
        #[cfg(feature = "mbt-injection")]
        crate::faults::reach(&self.path, crate::faults::FaultPoint::AfterSegmentSeal)?;
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.path)?;
        sync_parent_directory(&self.path)?;
        self.active_start_seq = self.next_seq;
        #[cfg(feature = "mbt-injection")]
        crate::faults::reach(&self.path, crate::faults::FaultPoint::AfterSegmentCreate)?;
        Ok(())
    }
}

fn segment_directory(path: &Path) -> io::Result<PathBuf> {
    let filename = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "log path needs a filename"))?;
    Ok(path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".segments")
        .join(filename))
}

/// Actual segment paths in stream order; the original active path comes last.
/// Seals live below `.segments/<entire filename>/`, keeping dotted names distinct.
pub fn segment_paths(path: impl AsRef<Path>) -> io::Result<Vec<PathBuf>> {
    let path = path.as_ref();
    let directory = segment_directory(path)?;
    let mut segments = Vec::new();
    match fs::read_dir(&directory) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected entry in sealed segment directory",
                    ));
                }
                segments.push(entry.path());
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    segments.sort();
    if path.exists() {
        segments.push(path.to_path_buf());
    }
    Ok(segments)
}

fn read_stream<T: ChainRecord>(
    path: &Path,
    allow_missing: bool,
) -> Result<(ReplayReport<T>, u64), LogError> {
    let paths = segment_paths(path)?;
    if paths.is_empty() && !allow_missing {
        return Err(io::Error::new(io::ErrorKind::NotFound, "log stream does not exist").into());
    }
    let mut records = Vec::new();
    let mut seq = 0;
    let mut hash = GENESIS_HASH.to_string();
    let mut active_start = 1;
    let mut truncated = 0;
    let mut offset = 0;
    let mut missing_newline = false;
    for segment in paths {
        let active = segment == path;
        let bytes = fs::read(&segment)?;
        let report = replay_bytes_from::<T>(&bytes, seq + 1, &hash)?;
        if !active {
            let name = segment
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            let expected = format!(
                "{:020}-{:020}-{}.jsonl",
                seq + 1,
                report.seq_head,
                report.last_hash.strip_prefix("sha256:").unwrap_or("")
            );
            if report.records.is_empty()
                || report.was_truncated()
                || report.trailing_newline_missing
                || name != expected
            {
                return Err(LogError::Corrupt {
                    line: records.len() + 1,
                    reason: format!("sealed segment integrity failure: {}", segment.display()),
                });
            }
            active_start = report.seq_head + 1;
        } else {
            truncated = report.truncated_bytes;
            offset = report.last_valid_offset;
            missing_newline = report.trailing_newline_missing;
        }
        seq = report.seq_head;
        hash = report.last_hash;
        records.extend(report.records);
    }
    Ok((
        ReplayReport {
            records,
            seq_head: seq,
            last_hash: hash,
            truncated_bytes: truncated,
            last_valid_offset: offset,
            trailing_newline_missing: missing_newline,
        },
        active_start,
    ))
}

/// Serializes a record canonically after removing its `prev_hash` member and
/// returns `sha256:<hex>`.
pub fn hash_record<T: Serialize>(record: &T) -> Result<String, LogError> {
    let value = serde_json::to_value(record).map_err(LogError::Serialize)?;
    hash_json_without_prev_hash(&value)
        .ok_or_else(|| LogError::InvalidRecord("record must serialize as a JSON object".into()))
}

/// Canonical JSON: sorted object keys, no whitespace, compact scalars.
pub fn canonical_json(value: &Value) -> String {
    let mut output = String::new();
    write_canonical(value, &mut output);
    output
}

/// SHA-256 as lowercase hexadecimal (without a prefix).
pub fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

/// Returns true for a well-formed `sha256:<64 lowercase hex>` digest.
pub fn is_chain_hash(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn write_canonical(value: &Value, output: &mut String) {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(true) => output.push_str("true"),
        Value::Bool(false) => output.push_str("false"),
        Value::Number(number) => output.push_str(&number.to_string()),
        Value::String(text) => {
            output
                .push_str(&serde_json::to_string(text).expect("serializing a string cannot fail"));
        }
        Value::Array(items) => {
            output.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_canonical(item, output);
            }
            output.push(']');
        }
        Value::Object(members) => {
            let mut keys: Vec<&String> = members.keys().collect();
            keys.sort();
            output.push('{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(
                    &serde_json::to_string(key).expect("serializing a string cannot fail"),
                );
                output.push(':');
                write_canonical(&members[*key], output);
            }
            output.push('}');
        }
    }
}

fn hash_json_without_prev_hash(value: &Value) -> Option<String> {
    let mut unsigned = value.clone();
    unsigned.as_object_mut()?.remove("prev_hash");
    Some(format!(
        "sha256:{}",
        sha256_hex(canonical_json(&unsigned).as_bytes())
    ))
}

fn replay_bytes_from<T: ChainRecord>(
    bytes: &[u8],
    start_seq: u64,
    previous_hash: &str,
) -> Result<ReplayReport<T>, LogError> {
    let mut records = Vec::new();
    let mut expected_prev = previous_hash.to_string();
    let mut expected_seq = start_seq;
    let mut last_valid_offset = 0usize;
    let mut truncated_bytes = 0u64;
    let mut trailing_newline_missing = false;
    let mut line_number = 0usize;
    let mut offset = 0usize;

    while offset < bytes.len() {
        let newline = bytes[offset..].iter().position(|&byte| byte == b'\n');
        let (end, has_newline) = match newline {
            Some(index) => (offset + index, true),
            None => (bytes.len(), false),
        };
        let line = &bytes[offset..end];

        if line.is_empty() {
            if bytes[end..].iter().all(|&byte| byte == b'\n') {
                truncated_bytes = (bytes.len() - last_valid_offset) as u64;
                break;
            }
            return Err(LogError::Corrupt {
                line: line_number + 1,
                reason: "empty line inside log".into(),
            });
        }

        line_number += 1;
        match decode_record::<T>(line, &expected_prev, expected_seq) {
            Ok((record, hash)) => {
                records.push(record);
                expected_prev = hash;
                expected_seq += 1;
                offset = end + usize::from(has_newline);
                last_valid_offset = offset;
                trailing_newline_missing = !has_newline;
            }
            Err(reason) => {
                if bytes[end..].iter().all(|&byte| byte == b'\n') {
                    truncated_bytes = (bytes.len() - last_valid_offset) as u64;
                    break;
                }
                return Err(LogError::Corrupt {
                    line: line_number,
                    reason,
                });
            }
        }
    }

    Ok(ReplayReport {
        records,
        seq_head: expected_seq.saturating_sub(1),
        last_hash: expected_prev,
        truncated_bytes,
        last_valid_offset: last_valid_offset as u64,
        trailing_newline_missing,
    })
}

fn decode_record<T: ChainRecord>(
    line: &[u8],
    expected_prev: &str,
    expected_seq: u64,
) -> Result<(T, String), String> {
    let value: Value =
        serde_json::from_slice(line).map_err(|error| format!("invalid JSON: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "record is not a JSON object".to_string())?;
    let prev_hash = object
        .get("prev_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| "record has no string prev_hash".to_string())?;
    if prev_hash != expected_prev {
        return Err(format!(
            "prev_hash mismatch: expected {expected_prev}, found {prev_hash}"
        ));
    }
    let hash = hash_json_without_prev_hash(&value)
        .ok_or_else(|| "record is not a JSON object".to_string())?;
    let record: T =
        serde_json::from_value(value).map_err(|error| format!("invalid record: {error}"))?;
    if record.seq() != expected_seq {
        return Err(format!(
            "sequence break: expected {expected_seq}, found {}",
            record.seq()
        ));
    }
    Ok((record, hash))
}

fn sync_parent_directory(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
