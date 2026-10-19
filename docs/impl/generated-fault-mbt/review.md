# Generated-adapter remediation review

Date: 2026-10-19. Review scope: registry/enrollment, MCP and CLI integration,
daemon root ownership, and central storage/rotation. This review was performed
by the runtime-profile worker outside its implementation ownership. Messaging,
queue, document and status repairs were implemented by that worker; findings
on those modules below came from the parent or storage worker and are recorded
with that ownership distinction.

The review found and resolved these gaps:

- **Enrollment response loss:** `fresh_stdio` retries transport failures with
  the same launch credential, but a clean socket EOF was previously classified
  as an invalid protocol response. [src/client.rs](../../../src/client.rs),
  line 106, now classifies it as `Unreachable(UnexpectedEof)`, enabling the
  existing retry in [src/mcp.rs](../../../src/mcp.rs), line 424. The real-UDS
  fixture in [tests/enrollment_retry.rs](../../../tests/enrollment_retry.rs)
  commits the first enrollment through production dispatch, closes without a
  reply, and checks that retry uses the same key and durable identity. The host
  [injection qualification log](qualification/full-tests-injection.log)
  records that test passing. A strict local sandbox run was denied UDS bind;
  that local result is not transport coverage.
- **Sealed-only resources:** the parent identified that queue/document startup
  enumerated active files only. Discovery now includes nonempty segment
  directories; lazy loading checks the complete logical stream. See
  [src/queues.rs](../../../src/queues.rs), lines 414 and 749, and
  [src/docs.rs](../../../src/docs.rs), lines 450 and 587.
  [tests/log_rotation.rs](../../../tests/log_rotation.rs) checks restart with
  absent active files, retained task/document leases, dotted names, continuing
  sequences, and exclusion of empty metadata directories. The runtime worker
  implemented this repair.
- **Rotation directory synchronization:** the independent storage review
  questioned source-directory synchronization before the sealed destination.
  The storage owner changed [src/storage.rs](../../../src/storage.rs), lines
  414-418, to synchronize the destination first and source parent second.
  Process-crash and injected I/O tests cover the implemented boundaries.
  Actual power-loss/filesystem qualification remains outside this evidence.
- **System-event channel threshold:** the storage worker found that opening a
  new daemon-event channel used the default rotation threshold. The runtime
  worker changed [src/messaging.rs](../../../src/messaging.rs), line 665, to
  use the configured threshold and added a positive integration case in
  [tests/log_rotation.rs](../../../tests/log_rotation.rs). This late finding
  requires the final source freeze to include that change. The targeted
  [rotation and ownership rerun](d3-d6-runtime-fixed/channel-threshold-ownership-tests.log)
  passed all seven tests after the repair.
- **Post-recovery projection fault fixture:** final replay exercised a newly
  selected trace that injects projection failure after the original operation
  process has crashed and recovered. The adapter previously waited for that
  absent process. It now starts the actual reconciliation sweep, waits for its
  real pre-append hook, and releases that hook with an I/O error. No timeout,
  model, expected observation, or production policy was changed. The
  [retained replay](qualification/deadletter-adapter-recovery/after.log) passes
  both dead-letter traces; the adjacent `before.log`, raw files and hashes
  preserve the fixture failure and corrected execution.

Root ownership does not end when a listener's reference disappears.
[src/server.rs](../../../src/server.rs), lines 162-166, acquires the lease
before replay and moves it into `DaemonState`. The daemon entry point uses
this constructor before binding or spawning writers. Both the daemon and
embedded stdio production paths retain an owned state. The lock file remains
in place, so subsequent owners contend on the same inode; a different socket
path does not provide a second root lease.

The presence/lease sweepers and connection/HTTP tasks hold `Arc<DaemonState>`
references. Consequently their state, including its lease, survives listener
cancellation while a writer task remains alive. In
[tests/root_ownership.rs](../../../tests/root_ownership.rs), dropping the
caller's state reference still makes a competing constructor return
`WouldBlock`; aborting and awaiting the remaining worker permits a successor
to acquire ownership. Awaiting the aborted task matters because it establishes
that the worker's state reference has been destroyed. No premature lease
release was found in this path. Public `new`/`with_config` remain unowned core
constructors used by isolated fixtures; root-exclusion evidence concerns the
production `owned` entry points.

Registry inspection found stable IDs and routing aliases, retained launch
associations, credential revocation after rotation/retirement, and compatible
loading of existing registry records. Fresh stdio launches generate separate
credentials; explicit resume retains the configured identity. Apart from the
resolved items above, no further material correctness or security finding was
identified in the reviewed boundaries. This statement is a source review and
bounded-test assessment, not an exhaustive proof of the implementation.
