# D1 generated writer-port reproduction

On 2026-10-19, four TLC-derived writer traces ran through compiler-generated
`MBFaultWriterPort` dispatch against real daemon subprocesses and UDS calls.
The gate returned exit 1: three implementation mismatches and one passing
handoff control. The complete Rust crate, including its HTTP dependencies, was
built; these daemon children ran with HTTP disabled.

| Trace | Actual result |
| --- | --- |
| `NoRejectedRepair` | A rejected same-socket startup truncated the torn suffix and appended a recovery record. Observed records changed from `[1]` to `[1,99]`, and `torn` changed to false. |
| `NoUnownedWrite` | An actor without a daemon attempted real production startup on a separate socket, was admitted, performed a real send, and then exited. The append returned `ok` and persisted record 1 instead of refusing ownership. |
| `NoWriterRejection` | A competing daemon using another socket was admitted. Both child PIDs became ready while sharing one bus root. |
| `NoWriterHandoff` | Stopping the old writer and starting its successor preserved the existing record. |

[replay.log](replay.log) records field-level model/implementation mismatches.
[manifest.json](manifest.json) identifies the executable hash, production-server
hash observed before the run, command, traces, retained fixtures, and copied-file
hashes. `fixtures/` preserves raw durable files before/after starts, each actual
observation, final bus files, and child stderr. Socket files and `run/` are omitted
from archival copies; original short-path fixtures remain under `/tmp/cwbase`.

The fixture verifies readiness by comparing the RPC-reported PID with its own
child PID. Observations read files without opening a recovering `LogFile`.
The unowned append reaches the production startup guard and, if admitted, the
actual append; fixture bookkeeping does not manufacture an ownership rejection.

The retained source capture includes the port and interface files. Some source
files were copied after execution while other implementation work continued;
it is not a complete frozen build input set. `serverSnapshotRecovered` in the
manifest explicitly records whether the exact observed baseline server bytes
were recoverable. The executable hash and runtime raw evidence establish the
recorded reproduction; this report does not claim a source-complete rebuild
certificate, HTTP transport qualification, simultaneous-start race coverage,
or global protection of arbitrary direct `LogFile` users.
