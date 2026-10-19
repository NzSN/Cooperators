# D7: segmentation and cursor continuity

The earlier implementation had no log rotation. This was a **feature gap** at
G0, visible in the retained [channel source](../g0-20261019-initial/source-snapshot/src/channels.rs)
and [storage source](../g0-20261019-initial/source-snapshot/src/storage.rs).
The generated rotation binding initially had no Rust implementation. That
missing adapter is blocked coverage, not an executed Rust counterexample.
No pre-fix D7 generated replay is claimed here. D2's actual red storage replays
are retained separately in [the storage baseline](../d2-storage-baseline/replay-final.log).

## Production behavior

Channels, queues (including dead letters), and document histories use central
`LogFile::open_rotating`. `DaemonConfig::log_rotation_bytes` defaults to
256 MiB. Inbox and registry behavior retain their existing storage policy.
The original log path remains the logical cursor identity and active file;
sealed segments live under `.segments/<entire-original-filename>/` with names
containing their sequence range and final SHA-256 digest. Dotted stream names
therefore do not overlap.

Sequence numbers and the hash chain span sealed and active segments. An
oversized record occupies a segment alone; the next append rotates. Sealed
corruption, including modification of the sealed final record, is fatal.
Only an active torn tail can be repaired. A stream sealed before its replacement
active file was created can be reopened without losing its global head.
Read-only segment enumeration and replay never create or repair files.

Any append I/O error quarantines its handle until actual reopen/replay. Further
appends return recovery-required before changing the caller's record or disk.
This includes failures during rotation. Reopening syncs the validated active
prefix before exposing recovered records or their dedupe indexes, since a full
record from a failed sync could still exist only in the OS cache. A failed
recovery sync prevents both log opening and daemon-state construction.

## Observation and fault boundary

The handwritten [rotation SUT](../../../../spec/mbt/src/faults_rotation.rs)
implements the compiler-generated `MBFaultRotationPort`. It does not consume
expected states. Segment membership and record sizes come from actual files;
cursor reads validate and read the actual global stream. One model byte unit
maps to exactly 1024 encoded JSONL bytes, including the newline; the fixture
threshold is 2048 bytes, matching model `RotateAt = 2`.

Each append runs in an actual child process using production `LogFile::append`.
Feature-gated barriers inside that method expose the pre-rotation and
pre-write stages to generated actions. The crash trace kills the child after
preparation. Additional controls kill real child processes immediately after
sealing (active path absent) and after active creation (active file empty), then
reopen and append to check preservation of the prior receipt and sequence.

## Validation status

The final central targeted suite passed **24 tests**: six existing storage
tests, eleven fault/quarantine/recovery-sync controls, and seven segmentation
tests. The directory sync ordering persists the sealed destination before
source removal. Six additional integration tests in `tests/log_rotation.rs`
passed, covering configured channel, queue, document, and system-event rotation;
cursor/dedupe, fence/version, dotted-name, sealed-only restart, and status
behavior. The production default is configured at 256 MiB; these behavioral
tests deliberately use a small threshold.

The first generated replay attempt was stopped by the required canonical
source/interface guard before running any trace. Its log is retained as
[guard-stale.log](guard-stale.log). After the canonical corpus and generated
bridge were published, the current offline build and
[generated replay](replay.log) passed **three traces, 32 states, all eight
actions**, plus **both additional actual process crash controls**. The three
crashes (one model trace and two boundary controls) each recorded signal 9
(`SIGKILL`) in their fixture `crashes.jsonl` files.

Reproduce the final gate from the repository root with a fresh evidence directory:

```sh
cargo build --offline --manifest-path spec/mbt/Cargo.toml
MIRROR_BIN=/home/nzsn/.local/bin/ModelMirrors \
COOP_MBT_EVIDENCE_DIR=/tmp/cooperators-rotation-fresh \
spec/mbt/target/debug/cooperators-mbt --suite faults --scenario rotation
cargo test --offline --features mbt-injection \
  --test storage --test storage_faults --test storage_rotation
```

[manifest.json](manifest.json) pins the observed source, generated contract,
trace, executable, and fixture hashes for this qualification.

These checks cover controlled filesystem errors and process crashes. They do
not qualify hardware power loss, arbitrary filesystem failures, unbounded
histories, or sustained 256 MiB rotation throughput. The small threshold
exercises the same production rotation path.
