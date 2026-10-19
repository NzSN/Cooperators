# D2 generated storage-port controls

The latest full MBT binary passes **all three ordinary storage traces**, and
all three diagnostic controls make the required gate fail. The binary, build
command, feature, and source hashes are recorded in
[source-build-identity.json](source-build-identity.json).

| Run | Observed result | Log |
| --- | --- | --- |
| Normal | Three traces pass; exit 0 | [normal](normal/replay.log) |
| `storage-wrong-receipt` | Three field mismatches after actual successful sync; exit 1 | [wrong receipt](wrong-receipt/replay.log) |
| `storage-corrupt-observation` | Three field mismatches against independently read records; exit 1 | [corrupted observation](corrupt-observation/replay.log) |
| `storage-missing-injection` | Actual append completes without the required barrier; explicit binding failure and exit 101 | [missing injection](missing-injection/replay.log) |

Controls are activated only in the MBT harness by `COOP_MBT_CONTROL`. They do
not change expected traces or production hook behavior. Wrong-receipt and
corrupted-observation controls modify only a cloned observation returned to
generated dispatch. Each `observations.jsonl` retains the actual observation,
the diagnostic label, and the intentionally altered `reportedObservation`;
the factual successful-receipt ledger stays intact.

The missing-injection control deliberately omits `arm_append`, runs the real
append, retains its actual completion and receipt, and rejects the missing
barrier. It never manufactures a pending response. Its fatal binding failure
stops the process at the first trace; remaining traces are not executed in that
diagnostic run. This is an expected nonzero control outcome, not a broker defect.

Normal traces use real partial writes and post-write sync failures. Recovery
fixtures only choose a retained raw-byte prefix; they no longer call `sync_all`
before reopening. The production recovery path synchronizes that prefix before
returning replay-derived records. Thus fixture synchronization does not mask a
missing recovery durability barrier.

All raw storage files and observation histories are retained under each run's
`fixtures/`. [manifest.json](manifest.json) hashes the artifacts and records
return codes. These are controlled filesystem/process scenarios; they do not
prove hardware or arbitrary power-loss behavior. The earlier implementation
failure baseline remains in [d2-storage-baseline](../d2-storage-baseline/).
