//! Deterministic, path-scoped fault controls for model-based tests.
//!
//! This module exists only with the `mbt-injection` feature. Hooks are one-shot:
//! an armed hook is removed from the registry when the real operation reaches
//! it. Dropping its handle cancels an unused hook and releases a blocked one.
//! No environment variable or production RPC can activate these controls.

use std::collections::HashMap;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendFault {
    /// Write a real, nonempty prefix (at most line length minus one), then fail.
    PartialWrite { bytes: usize },
    /// Write the whole line, then fail at the sync boundary without syncing.
    FailSync,
    /// Write the whole line and wait before sync until the handle is released.
    PauseBeforeSync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncOutcome {
    /// Continue to the actual filesystem operation.
    Succeed,
    /// Return an injected I/O error at the reached boundary.
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultPoint {
    BeforeRecoverySync,
    BeforeRotation,
    AfterSegmentSeal,
    BeforeRecordWrite,
    BeforeDeadProjection,
    AfterDeadProjectionAppend,
    AfterDocCreate,
    AfterSegmentCreate,
    BeforeRegistryRename,
    AfterRegistryRename,
    BeforeRegistryReplace,
    AfterRegistryReplace,
    AfterRegistrySync,
    AfterOperationSyncBeforePublish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointFault {
    Fail,
    Pause,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultStage {
    Armed,
    PartialWriteFailed,
    BeforeSync,
    Synced,
    SyncFailed,
    Reached,
    InjectedFailure,
    IoFailed,
    Cancelled,
}

/// Diagnostics from the reached I/O boundary, separate from the append result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultObservation {
    pub path: PathBuf,
    pub point: Option<FaultPoint>,
    pub stage: FaultStage,
    pub seq: Option<u64>,
    pub bytes_requested: usize,
    pub bytes_written: usize,
    /// False if a real `write_all` failed after an unknown amount of progress.
    pub bytes_written_known: bool,
    /// True only if the actual `File::sync_all` call was made.
    pub sync_attempted: bool,
    pub sync_succeeded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    path: PathBuf,
    point: Option<FaultPoint>,
}

#[derive(Debug, Clone, Copy)]
enum Action {
    Append(AppendFault),
    Point(PointFault),
}

#[derive(Debug)]
struct State {
    observation: FaultObservation,
    release: Option<SyncOutcome>,
}

#[derive(Debug)]
struct Control {
    key: Key,
    action: Action,
    state: Mutex<State>,
    changed: Condvar,
}

fn registry() -> &'static Mutex<HashMap<Key, Arc<Control>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<Key, Arc<Control>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn clocks() -> &'static Mutex<HashMap<PathBuf, Arc<AtomicU64>>> {
    static CLOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<AtomicU64>>>> = OnceLock::new();
    CLOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Clock override scoped to one broker root. Dropping it restores real time.
#[derive(Debug)]
pub struct ClockHandle {
    root: PathBuf,
    clock: Arc<AtomicU64>,
}

impl ClockHandle {
    pub fn now(&self) -> u64 {
        self.clock.load(Ordering::Relaxed)
    }

    pub fn set(&self, now_ms: u64) {
        self.clock.store(now_ms, Ordering::Relaxed);
    }

    /// Advance by a duration in milliseconds, saturating at `u64::MAX`.
    pub fn advance(&self, delta_ms: u64) -> u64 {
        self.clock
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(old.saturating_add(delta_ms))
            })
            .unwrap()
            .saturating_add(delta_ms)
    }
}

impl Drop for ClockHandle {
    fn drop(&mut self) {
        let mut clocks = clocks().lock().unwrap();
        if clocks
            .get(&self.root)
            .is_some_and(|clock| Arc::ptr_eq(clock, &self.clock))
        {
            clocks.remove(&self.root);
        }
    }
}

pub fn set_clock(root: impl AsRef<Path>, now_ms: u64) -> io::Result<ClockHandle> {
    let root = normalized_path(root.as_ref())?;
    let clock = Arc::new(AtomicU64::new(now_ms));
    let mut clocks = clocks().lock().unwrap();
    if clocks.contains_key(&root) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "test clock already installed for broker root",
        ));
    }
    clocks.insert(root.clone(), Arc::clone(&clock));
    Ok(ClockHandle { root, clock })
}

/// Returns an installed test clock, leaving real-time fallback to the caller.
pub fn now_millis(root: impl AsRef<Path>) -> Option<u64> {
    let root = normalized_path(root.as_ref()).ok()?;
    clocks()
        .lock()
        .unwrap()
        .get(&root)
        .map(|clock| clock.load(Ordering::Relaxed))
}

/// Handle for observing a reached hook and releasing a deterministic barrier.
#[derive(Debug)]
pub struct FaultHandle {
    control: Arc<Control>,
}

impl FaultHandle {
    pub fn observation(&self) -> FaultObservation {
        self.control.state.lock().unwrap().observation.clone()
    }

    /// Wait until the real operation reaches the hook, or return TimedOut.
    pub fn wait_reached(&self, timeout: Duration) -> io::Result<FaultObservation> {
        let deadline = Instant::now() + timeout;
        let mut state = self.control.state.lock().unwrap();
        while state.observation.stage == FaultStage::Armed {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "fault hook not reached",
                ));
            }
            let (next, wait) = self.control.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if wait.timed_out() && state.observation.stage == FaultStage::Armed {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "fault hook not reached",
                ));
            }
        }
        Ok(state.observation.clone())
    }

    /// Release a pause. `Succeed` permits real I/O; it does not synthesize success.
    pub fn release(&self, outcome: SyncOutcome) {
        let mut state = self.control.state.lock().unwrap();
        state.release = Some(outcome);
        self.control.changed.notify_all();
    }
}

impl Drop for FaultHandle {
    fn drop(&mut self) {
        let mut hooks = registry().lock().unwrap();
        if hooks
            .get(&self.control.key)
            .is_some_and(|hook| Arc::ptr_eq(hook, &self.control))
        {
            hooks.remove(&self.control.key);
        }
        drop(hooks);
        let mut state = self.control.state.lock().unwrap();
        if state.observation.stage == FaultStage::Armed {
            state.observation.stage = FaultStage::Cancelled;
        }
        if state.release.is_none() {
            state.release = Some(SyncOutcome::Fail);
        }
        self.control.changed.notify_all();
    }
}

pub fn arm_append(path: impl AsRef<Path>, fault: AppendFault) -> io::Result<FaultHandle> {
    if matches!(fault, AppendFault::PartialWrite { bytes: 0 }) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "partial write needs bytes > 0",
        ));
    }
    arm(path.as_ref(), None, Action::Append(fault))
}

pub fn arm_point(
    path: impl AsRef<Path>,
    point: FaultPoint,
    fault: PointFault,
) -> io::Result<FaultHandle> {
    arm(path.as_ref(), Some(point), Action::Point(fault))
}

fn arm(path: &Path, point: Option<FaultPoint>, action: Action) -> io::Result<FaultHandle> {
    let key = Key {
        path: normalized_path(path)?,
        point,
    };
    let control = Arc::new(Control {
        key: key.clone(),
        action,
        state: Mutex::new(State {
            observation: FaultObservation {
                path: key.path.clone(),
                point,
                stage: FaultStage::Armed,
                seq: None,
                bytes_requested: 0,
                bytes_written: 0,
                bytes_written_known: true,
                sync_attempted: false,
                sync_succeeded: false,
            },
            release: None,
        }),
        changed: Condvar::new(),
    });
    let mut hooks = registry().lock().unwrap();
    if hooks.contains_key(&key) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "fault hook already armed for path and point",
        ));
    }
    hooks.insert(key, Arc::clone(&control));
    Ok(FaultHandle { control })
}

fn normalized_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    // Resolve the existing ancestor, so arming before creation uses the same key
    // as reaching after creation, including roots reached through a symlink.
    let mut ancestor = normalized.as_path();
    let mut suffix = Vec::new();
    loop {
        match ancestor.canonicalize() {
            Ok(mut resolved) => {
                for component in suffix.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = ancestor.file_name().ok_or(error)?;
                suffix.push(name.to_os_string());
                ancestor = ancestor.parent().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "fault path has no existing ancestor",
                    )
                })?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn take(path: &Path, point: Option<FaultPoint>) -> Option<Arc<Control>> {
    let path = normalized_path(path).ok()?;
    registry().lock().unwrap().remove(&Key { path, point })
}

/// Reach an instrumented boundary in a real operation. No hook means no effect.
pub fn reach(path: impl AsRef<Path>, point: FaultPoint) -> io::Result<()> {
    let Some(control) = take(path.as_ref(), Some(point)) else {
        return Ok(());
    };
    let Action::Point(action) = control.action else {
        unreachable!()
    };
    let mut state = control.state.lock().unwrap();
    state.observation.stage = FaultStage::Reached;
    control.changed.notify_all();
    if action == PointFault::Pause {
        while state.release.is_none() {
            state = control.changed.wait(state).unwrap();
        }
        if state.release == Some(SyncOutcome::Succeed) {
            return Ok(());
        }
    }
    state.observation.stage = FaultStage::InjectedFailure;
    control.changed.notify_all();
    Err(injected_error())
}

pub(crate) struct AppendInjection {
    control: Arc<Control>,
}

pub(crate) fn take_append(path: &Path) -> Option<AppendInjection> {
    take(path, None).map(|control| AppendInjection { control })
}

impl AppendInjection {
    pub(crate) fn fault(&self) -> AppendFault {
        let Action::Append(fault) = self.control.action else {
            unreachable!()
        };
        fault
    }

    pub(crate) fn partial_written(&self, seq: u64, requested: usize, written: usize) {
        self.update(
            seq,
            requested,
            written,
            FaultStage::PartialWriteFailed,
            false,
            false,
        );
    }

    pub(crate) fn io_failed(&self, seq: u64, requested: usize) {
        // write_all can fail after an unknown partial write. Do not report a
        // guessed byte count as observed; adapters can read the file itself.
        let mut state = self.control.state.lock().unwrap();
        state.observation.seq = Some(seq);
        state.observation.bytes_requested = requested;
        state.observation.bytes_written = 0;
        state.observation.bytes_written_known = false;
        state.observation.stage = FaultStage::IoFailed;
        self.control.changed.notify_all();
    }

    pub(crate) fn before_sync(&self, seq: u64, bytes: usize) -> io::Result<()> {
        let mut state = self.control.state.lock().unwrap();
        state.observation.seq = Some(seq);
        state.observation.bytes_requested = bytes;
        state.observation.bytes_written = bytes;
        state.observation.bytes_written_known = true;
        state.observation.stage = FaultStage::BeforeSync;
        self.control.changed.notify_all();
        if self.fault() == AppendFault::PauseBeforeSync {
            while state.release.is_none() {
                state = self.control.changed.wait(state).unwrap();
            }
            if state.release == Some(SyncOutcome::Succeed) {
                return Ok(());
            }
        }
        state.observation.stage = FaultStage::SyncFailed;
        self.control.changed.notify_all();
        Err(injected_error())
    }

    pub(crate) fn synced(&self, seq: u64, bytes: usize, succeeded: bool) {
        self.update(
            seq,
            bytes,
            bytes,
            if succeeded {
                FaultStage::Synced
            } else {
                FaultStage::SyncFailed
            },
            true,
            succeeded,
        );
    }

    fn update(
        &self,
        seq: u64,
        requested: usize,
        written: usize,
        stage: FaultStage,
        attempted: bool,
        succeeded: bool,
    ) {
        let mut state = self.control.state.lock().unwrap();
        state.observation.seq = Some(seq);
        state.observation.bytes_requested = requested;
        state.observation.bytes_written = written;
        state.observation.bytes_written_known = true;
        state.observation.stage = stage;
        state.observation.sync_attempted = attempted;
        state.observation.sync_succeeded = succeeded;
        self.control.changed.notify_all();
    }
}

pub(crate) fn injected_error() -> io::Error {
    io::Error::other("injected MBT filesystem failure")
}
