# D3-D6 generated-port implementation baseline

Captured on 2026-10-19 before the semantic repairs to messaging, queues,
documents and status. The retained files are qualification evidence, not a
production fixture or a second adapter protocol.

The six selected traces executed through compiler-generated `MBFaultInbox`,
`MBFaultDeadLetter`, `MBFaultDocument` and `MBFaultStatus` Rust bindings,
MirrorRust and the real ModelMirrors executable. Five traces exposed actual
implementation mismatches; the post-projection crash control passed.

| Profile | Result before repair | Actual effect |
| --- | --- | --- |
| Inbox | 1 failure | After both messages expired, wait returned cursor 0 instead of the scanned prefix ending at 2. |
| Dead letter | 1 failure, 1 passing control | A durable terminal task remained absent from the projection after failure and recovery. Crash after a committed projection preserved one record. |
| Document | 2 failures | Both error and process-crash interruption left an empty file that caused the same-name retry to return `exists`. |
| Status | 1 failure | A real 96 KiB final record was absent from reported sequence heads. |

Use `*-retained-final.log` for the accepted baseline run. Earlier `*-raw.log`
and `*-retained.log` attempts stopped at an unrelated stale generated identity
artifact; these are setup failures and are not counted as implementation runs.
The plain profile logs are earlier successful reproductions without retained
raw fixture files.

`raw/<profile>/<trace>/step-XX/observation.json` is the actual normalized
observation and operation result. Its adjacent `root/` tree contains the real
files at that checkpoint. Observations never open a repairing log handle.
Fixture registry credentials are temporary local test data.

`source/` preserves the instrumented, unrepaired owning modules and adapter.
`source-sha256.txt` identifies those copies. `binary-raw-sha256.txt` identifies
the binary used for retained raw runs. This is an embedded-core execution of
the full Rust harness with the `mbt-injection` feature; these profiles do not
qualify HTTP or UDS transport behavior.

The generated port drives each operation. Private fixture subprocesses execute
one actual broker operation and expose filesystem synchronization markers.
Document and dead-letter crash actions kill and reap the paused operation
process; they do not substitute an orderly error return for a crash.

For status, one model byte unit is exactly 32 KiB of encoded JSONL. The trace's
three-unit record is therefore 96 KiB and exceeds the production 64 KiB initial
status window. Model task identity 1 maps to the fixture's one enqueued task;
projection duplicates remain separate observed records.

Reproduction, after building the source identified above:

```sh
COOP_MBT_EVIDENCE_DIR=/absolute/new/evidence-directory \
MIRROR_BIN=/home/nzsn/.local/bin/ModelMirrors \
spec/mbt/target/debug/cooperators-mbt --suite faults --scenario inbox
```

Select `deadletter`, `document` or `status` for the remaining rows. The
evidence destination must be new so a prior version cannot be overwritten.
