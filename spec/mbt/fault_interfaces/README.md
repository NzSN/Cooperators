# Generated fault-profile interfaces

These eight typed interfaces connect the existing [MBFaults model](../../MBFaults.tla)
and [twenty-four TLC witnesses](../faults/manifest.json) to compiler-generated
MirrorRust ports. They use the same compiler target as the protocol and writer
suites: `mirrorrust-v2`. No custom NDJSON adapter protocol is involved.

| Profile | Source view | Generated port | Traces |
| --- | --- | --- | --- |
| storage | [MBFaultStorage.tla](../../MBFaultStorage.tla) | `storage/generated/MBFaultStorageMirror.generated.rs` | 3 |
| writer | [MBFaultWriter.tla](../../MBFaultWriter.tla) | `writer/generated/MBFaultWriterMirror.generated.rs` | 4 |
| inbox | [MBFaultInbox.tla](../../MBFaultInbox.tla) | `inbox/generated/MBFaultInboxMirror.generated.rs` | 1 |
| deadletter | [MBFaultDeadLetter.tla](../../MBFaultDeadLetter.tla) | `deadletter/generated/MBFaultDeadLetterMirror.generated.rs` | 2 |
| document | [MBFaultDocument.tla](../../MBFaultDocument.tla) | `document/generated/MBFaultDocumentMirror.generated.rs` | 2 |
| status | [MBFaultStatus.tla](../../MBFaultStatus.tla) | `status/generated/MBFaultStatusMirror.generated.rs` | 1 |
| rotation | [MBFaultRotation.tla](../../MBFaultRotation.tla) | `rotation/generated/MBFaultRotationMirror.generated.rs` | 3 |
| identity | [MBFaultIdentity.tla](../../MBFaultIdentity.tla) | `identity/generated/MBFaultIdentityMirror.generated.rs` | 8 |

The compiler generates trait declarations, typed input/observation structures,
codecs, action dispatch, lifecycle guards, digest binding, and coverage accounting.
Concrete Rust implementations call production operations, control real failures,
and obtain observations. Expected operation errors are `result` observations;
missing hooks, unexpected errors, and invalid observations are binding failures.
Generated files remain compiler-owned.

## Explicit interface-view bridge

The current compiler cannot ingest the canonical model and projected corpus
directly: its TLA frontend rejects the model's `SelectSeq(..., LAMBDA ...)`
expressions, and ordinary source/evidence admission requires every effective
model variable in the evidence. The original traces deliberately omit internal
`s` and `steps`. The compiler's existing projection facility requires exact
variable consumption and cannot express that omission or record-field selection.

The `MBFault*.tla` source views therefore declare the public projection only.
They contain no `Init`, `Next`, or replacement transition algorithm. **They are
interface schemas, not executable model-checking specifications.** The original
`MBFaults.Next` remains the oracle that produced and checked the witnesses.
Explicit-trace ModelMirrors replay does not generate behaviors from these views.

[generate_fault_interfaces.py](../generate_fault_interfaces.py) bridges the
canonical corpus to these schema modules. It verifies the original model,
registry, and trace hashes against the canonical corpus manifest, writes typed
ITF metadata, and records source/projected hashes in
[provenance.json](provenance.json). Each view embeds the canonical model hash.
The compiler lock then binds that view, reviewed contract, and structural trace
evidence. This is a checked provenance bridge; it is not a compiler-proved
semantic refinement theorem.

Storage's `phase` and `recoveryRequired` observation fields are omitted:
neither can be independently observed from the existing public storage API.
Actual operation results, raw record identities/sequences, and successful receipt
history remain compared. An adapter must not invent policy flags to replace
those omitted fields. Identity IDs and tokens are bijectively renamed by first
allocation occurrence, independently of original TLC choice labels. The manifest
retains each renaming; its inverse restores the original observation. Sentinels,
launch keys, kind/display values, and equality are preserved. The concrete port
independently names actual reply IDs/tokens by first occurrence. A duplicate
actual value keeps one alias and therefore remains detectable. The mapping and
expected values are never sent to the port.

Every other profile preserves its original observation, result, and parameters.
No expected outcome or next observation is passed to a generated operation
method.

## Regenerate and check

From any working directory:

```sh
python3 /home/nzsn/Repos/Cooperators/spec/mbt/generate_fault_interfaces.py
python3 /home/nzsn/Repos/Cooperators/spec/mbt/generate_fault_interfaces.py --check
```

Generation follows scaffold -> reviewed input selection -> resolve/lock ->
generate -> check -> corpus preflight. The retained scaffold records all inferred
inputs; the reviewed contract retains only the action's actual parameters.
Expected results and observations never become action inputs. `--compiler PATH`
selects another binary; its identity enters provenance. `--profile storage`
limits compiler invocations but still checks the complete canonical bridge.

`--check` does not rewrite artifacts. It rejects stale model/corpus/compiler
identities, missing or changed projections/views/contracts, stale bindings,
unexpected projected traces, and uncovered declared actions. After a canonical
model change, regenerate and validate the canonical fault corpus first; updating
only interface artifacts cannot refresh its fixed-input certificates.

## Recorded interface validation

The initial [compiler check log](evidence/compiler-check.log) checkpoint records eight clean compiler
checks and eight complete preflights: nineteen traces, 121 states, no unseen
declared actions. All eight generated Rust modules compiled together against
the local MirrorRust crate in an isolated offline compile harness
([compile log](evidence/generated-compile.log)). That compile
check does not qualify a Cooperators binary or its transports.

The expanded [compiler check](evidence/compiler-check-expanded.log) passes all
eight profile checks/preflights with **24 traces and 143 states**. Identity now
has **eight traces and 35 states**, including the generated `Retire` method.
The refreshed [bridge controls](evidence/bridge-tests-expanded.log) pass all nine
provenance/collision tests. These are separate from the initial 19-trace,
121-state checkpoint above.

Nine provenance/control tests cover retained observations plus stale models,
missing/changed canonical traces, missing/changed projected traces, changed
views, changed compiler identity, and first-allocation identity renaming that
preserves collisions:

```sh
python3 -m unittest discover -s spec/mbt/fault_interfaces -p 'test_*.py' -v
```

The [projected storage checks](evidence/projected-storage-final/results.json) retain
three correct-policy traces and four distinguishing fault checks against the
**original `MBFaults.Next`**. All seven pass: omission of internal policy fields
still detects continue-after-I/O, acknowledgment-before-sync, loss of an earlier
acknowledgment, and a wrong receipt sequence. Their TLA probes, configurations,
and raw TLC logs are retained alongside the result manifest. These are bounded
model checks, not implementation reproductions. Seven additional
[identity projection checks](evidence/projected-identity/results.json) validate
three correct-policy traces and distinguish identity-by-kind, reconnect
rebinding, shared-token, and stale-token policies after renaming.

The expanded [storage certificates](evidence/projected-storage-expanded/results.json)
pass **7/7** checks against the current canonical model. The expanded
[identity certificates](evidence/projected-identity-expanded/results.json) pass
**15/15**: eight correct-policy traces and seven distinguishing mutants,
including unstable retry, retired-ID reuse and stale enrollment acceptance.
The earlier seven-check storage/identity directories remain historical evidence.

To repeat that comparison while preserving the previous evidence:

```sh
python3 spec/mbt/fault_interfaces/check_projection.py --profile storage \
  --output /tmp/cooperators-projected-storage-new
```

Runtime results and defect closure belong in the implementation qualification
ledger required by [the execution plan](../../../docs/design/0002-generated-adapter-fault-mbt-plan.md).
A generated trait, a model mutant, or a declaration of fault capability alone
does not close an implementation defect.

The expanded identity corpus adds initial-enrollment retry, stale launch-key
rejection after rotation and retirement, retirement followed by a fresh launch,
and retired-credential rejection. Its generated methods include `retire` and
observe actual registry metadata, actual reply IDs and actual authentication
outcomes. Enrollment keeps the first launch-key hash reserved; rejected retries
return the existing ID and never restore the revoked credential.

Delivery/ack isolation, dedupe, presence and foreign task/document lease
rejection are covered separately by direct broker tests and the actual two-MCP
process acceptance test in [`tests/instance_launch.rs`](../../../tests/instance_launch.rs).
Those results are not generated identity-port replay coverage. The identity
profile's observation remains limited to lifecycle IDs, credentials, metadata,
active/connected instances and reply IDs. Persistent registry I/O failure and
entropy are outside the modeled policy.

Actual generated identity replay passes **8/8 traces, 35 states**. The
[retained runtime evidence](../../../docs/impl/generated-fault-mbt/d8-identity-generated-expanded/README.md)
includes raw registry snapshots, redacted actual RPC replies, independent
opaque-name bijections and binary/source hashes. The separate
[MCP process acceptance](../../../docs/impl/generated-fault-mbt/d8-process-acceptance/manifest.json)
passes **2/2 tests without socket skips**, including two independently launched
processes with the same configured token and metadata.
