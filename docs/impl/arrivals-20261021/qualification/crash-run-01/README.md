# Arrival process crash qualification

Date: 2026-10-21. Result: passed on the immutable
[second source snapshot](source-snapshot-02/), pinned by
[source-freeze-02.json](source-freeze-02.json).

The [exact command and environment](command-02.json) ran offline and locked
with `mbt-injection`, using only disposable roots. The
[complete log](crash-and-streaming-attempt-02.log) reports 6 arrival tests and
7 streaming tests passed. The arrival total includes the subprocess helper,
four actual process kill controls and the retained old executable gate.
[Structured results](results.json) retain identities, delivery IDs and hashes.

| Control | Boundary before SIGKILL | Recovery assertion |
|---|---|---|
| [Source committed](controls/source-before-feed/boundary.json) | Successful document receipt with captured recipient/subscription; projector paused at `BeforeRecordWrite`; zero feed rows | Exactly one notice with the original source hash, recipient, subscription, version, obligation and delivery ID |
| [Feed sync pending](controls/feed-before-sync/boundary.json) | One complete real feed row, paused at `PauseBeforeSync`; sync unattempted; wait has returned no receipt | Exactly the same feed row; replay dedupe appends nothing |
| [ACK successful](controls/ack-after-success/boundary.json) | Successful durable ACK receipt and empty default page | Default cursor stays acknowledged; explicit cursor zero still returns the original notice |
| [ACK sync pending](controls/ack-before-sync/boundary.json) | ACK through notice 1 succeeded; ACK through notice 2 has complete bytes but no sync or receipt | Earlier ACK is preserved; recovered ACK may cover the uncertain complete prefix; explicit historical cursor retains both notices |

Every `process-result.json` records signal **9**, executable SHA-256 and
`child_reaped: true`. Every `recovery.json` records the recovered historical
and default pages. Source bytes remain identical after recovery. No production
fault activation hooks were added; these controls use the existing private,
path-scoped `mbt-injection` hooks inside the test subprocess.

These are process crash controls on the local filesystem. They establish
SIGKILL/reopen behavior, not power-loss or storage device cache durability.
Source files, feed rows, ACK rows and registry hashes are retained under each
control root. Runtime evidence contains no plaintext token fields.

## Retained preupgrade executable

The gate actually ran `/tmp/cooperators-arrival-preupgrade-edcbc923/coop`,
SHA-256 `32d5b650e8768407d297165eb350c295ab0b7aa0da14272fcee3948765383488`.
It exited 1 with `unsupported registry version 2`.
[The guard result](controls/preupgrade-refusal/guard-result.json) records
identical before/after hashes for document, queue and inbox files containing
deliberately torn tails, and an unchanged V2 registry. No socket was created.
Thus refusal preceded any attempted log repair. Ordinary test runs may omit
this external gate when `COOP_PREUPGRADE_BINARY` is unset; this qualification
set it explicitly.

## Preserved unsuccessful attempt

[Attempt 1](crash-and-streaming-attempt-01.log) failed compilation because its
snapshot captured concurrent MCP edits before their helper functions existed.
Its [source snapshot](source-snapshot/) and [hash manifest](source-freeze.json)
remain unchanged. No crash control ran in that attempt. Attempt 2 compiled and
ran the complete gates from a newly frozen coherent source snapshot.

To repeat the run, use the recorded command with a new
`COOP_ARRIVAL_CRASH_EVIDENCE_DIR`; the tests refuse to overwrite prior evidence.
The tested snapshot is independent of subsequent working-tree changes.
