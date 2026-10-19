# Generated adapter fault MBT execution

Execution date: 2026-10-19. All implementation and generated replay gates pass.
The approved scope is [plan 0002](../../design/0002-generated-adapter-fault-mbt-plan.md).
G0-G5 are complete. The [machine-readable result ledger](qualification/results.json)
records final counts, source/binary identities, and artifact hashes; the
[review record](review.md) describes the additional findings and repairs.

Eight compiler-generated Rust ports now call real Cooperators operations.
The default MBT `all` gate includes the protocol, original writer, and all
fault profiles. Compiler/provenance failures, absent traces, unavailable
sockets/hooks, and mismatches fail the gate.

## Qualification results

| Gate | Result and evidence |
| --- | --- |
| Aggregate generated replay | **63/63 traces pass**: 34 protocol, 5 original writers, 24 fault traces; **2 additional actual process crash controls pass**. [Final log](qualification/mbt-all-final.log). |
| Full Rust tests, injection enabled | **99 pass**, zero failures; one pre-existing ignored performance test. [Log](qualification/full-tests-injection-final.log). |
| Normal build, injection disabled | **81 pass**, zero failures; the same ignored performance test. [Log](qualification/full-tests-default-final.log). |
| Actual loopback TCP/HTTP | **19/19 checks pass** against the normal daemon: health, bearer enforcement, MCP initialization, 23 tools, canonical instance metadata, heartbeat, metrics, and retirement/revocation. [Smoke evidence](qualification/http-smoke/actual-loopback-run/results.json). |
| Compiler validation | Eight new interfaces, **24 traces / 143 projected states**, no uncovered declared actions; nine provenance controls pass. [Compiler log](../../../spec/mbt/fault_interfaces/evidence/compiler-check-expanded.log), [controls](../../../spec/mbt/fault_interfaces/evidence/bridge-tests-expanded.log). The [two original interfaces](qualification/original-interface-checks.log) also check clean. |
| Model/progress/mutation controls | **117 checks pass** across hardening (18), instance lifecycle/isolation (21), composed protocol (22), and fault profiles (56). [Hardening](qualification/hardening.log), [instances](qualification/instances.log) plus [combined completion](qualification/instances-combined-bfs.log), [composition](qualification/composed.log) plus [combined completion](qualification/composed-combined-bfs.log), [fault matrix](d8-identity-policy-v3-model/parallel-check.json). |
| Trace discrimination | **48/48 canonical fixed-input checks** pass, including every one of the 24 modeled mutants. [Corpus certificate](../../../spec/mbt/faults/manifest.json). Projected storage/identity checks add **22/22** confirmations against canonical `MBFaults.Next`. |
| Harness/tool controls | **22 tool tests**, **2 Rust adapter tests**, and all **3 required failing storage controls** succeed. [Tools](qualification/fault-tools.log), [adapter tests](qualification/mbt-unit-tests.log), [negative controls](d2-generated-storage-controls/README.md). |

[source-freeze.json](qualification/source-freeze.json) pins 228 source/model/
contract/trace files, compiler and replay binaries, Cargo versions, and dependency
revisions. MBT executable SHA-256:
`ee92e91b54d6efe23e436ece4c4dc991dcbc44b9bb9a2133e5a93f354fe3a005`.
The final run retained raw observations and files in
[raw-final](qualification/raw-final/). No frozen source changed during replay.

The instance combined DFID attempt exceeded 60 seconds; its two remaining
checks completed using BFS. A concurrent TLC standard-module extraction caused
a composed parser failure; private Java temporary directories and the completed
BFS retry resolve it. The checker now isolates those directories automatically.
The aggregate replay also exposed a missing post-recovery failure operation in
the concrete dead-letter adapter; the adapter was corrected without changing
the model, expected results, production code, or timeout. Its
[before/after evidence](qualification/deadletter-adapter-recovery/) is retained.

## Defect ledger

| ID | Baseline | Repair and acceptance |
| --- | --- | --- |
| D1 root ownership | [Generated writer baseline](d1-generated-writer-baseline/README.md): 3 mismatches and 1 passing handoff control; original writer battery also exposed 3 failures. | Kernel lock acquired before replay, independent of socket; the state and background writers retain ownership. Rejected starts leave durable bytes unchanged. |
| D2 uncertain append | [Storage baseline](d2-storage-baseline/replay-final.log): 2 mismatches and 1 receipt-preservation control; raw records and actual receipts retained. | Partial write/sync failure poisons the handle; another append requires actual reopen/recovery. Open syncs the recovered prefix before exposing it to receipt/dedupe paths; recovery sync failures prevent state construction. |
| D3 expired inbox | [Runtime baselines](d3-d6-runtime-baseline/): expired-only wait returned cursor 0 instead of 2. | Wait exposes the safely scanned expired prefix for explicit acknowledgment, releasing capacity without skipping live messages. |
| D4 dead-letter projection | Terminal event survived but projection remained missing; crash-after-projection control also retained. | Ordinary sweeper reconstructs missing projections; task identity makes retry idempotent. |
| D5 initial document | Both failed and crashed creation reserved an empty file. | A committed version reserves the name; empty interrupted creation can be retried. |
| D6 large status head | A valid final record exceeded the tail window and disappeared from status. | Backward scanning expands to a complete record; status aggregates logical sealed/active streams without repairing files. |
| D7 rotation | [Feature-gap record](d7-rotation-fixed/README.md); no pre-fix generated rotation mismatch is claimed. | Default 256 MiB segmentation; small configured threshold tests preserve global sequence/hash, cursors, versions, and fences. Actual child-process crash checks cover sealed-only and empty-active windows. |
| D8 instance identity | [Legacy shared-token observation](d8-identity-baseline/) establishes the missing launch-provisioning feature, rather than a generated-port reproduction. | Fresh MCP launches receive distinct IDs/credentials; retries and reconnects preserve identity, rotation revokes old credentials, retirement reserves history. [Actual process acceptance](d8-process-acceptance/) checks messaging, ack/dedupe, presence, and foreign leases. |

The same six D3-D6 generated traces produced five baseline failures and then
passed with the original fault activation/observation mechanisms
([intermediate repaired evidence](d3-d6-runtime-fixed/README.md)). Final
integrated results are recorded under [qualification](qualification/).

## Build and replay

The original exact `hyper =1.11.1` pin was absent from the offline cache.
It was deliberately updated to cached `=1.12.0`, in both lockfiles. Full
HTTP dependencies now build; no HTTP stub is used. MBT enables the explicit
`mbt-injection` feature; normal builds omit injection hooks.

```sh
cargo test --offline --locked
cargo test --offline --locked --features mbt-injection
cargo build --offline --locked --manifest-path spec/mbt/Cargo.toml
MIRROR_BIN=/home/nzsn/.local/bin/ModelMirrors \
  COOP_MBT_EVIDENCE_DIR=/tmp/cooperators-qualification \
  spec/mbt/target/debug/cooperators-mbt --skip-gen --suite all
```

Transport qualification sets `COOP_REQUIRE_UDS=1` and runs on a host that
permits local sockets. Sandbox EPERM is not passing transport coverage.
The full suite covers actual daemon/UDS/MCP processes and in-memory HTTP
service/authentication behavior. Actual loopback TCP/HTTP smoke evidence is
recorded separately in [http-smoke](qualification/http-smoke/).
Fault traces use real file operations and sync barriers; crash cases kill and
wait for actual child processes. Baseline files, replies, and receipts are
retained outside restarted state. Identity evidence redacts plaintext secrets.

## Proof boundary

The [interface bridge](../../../spec/mbt/fault_interfaces/README.md) uses
declaration-only typed schemas because the compiler cannot ingest the full
canonical model syntax/projection. Checked hashes bind source models, canonical
traces, projections, contracts, locks, and generated artifacts. Canonical TLA
transitions remain the oracle. This is not a compiler-proved refinement theorem.
Replay uses plain explicit traces; compiler checks bind digests locally.

Generated rotation traces exercise the production storage primitive; direct
API regressions additionally cover channel/queue/document integration. Generated
identity traces cover lifecycle decisions; real MCP/direct API acceptance
additionally covers delivery, acknowledgments, dedupe, presence, and leases.
The original protocol adapter's reconstructed-log limitation remains explicit
in [its README](../../../spec/mbt/README.md).

These are bounded model and implementation checks for D1-D8. They do not
establish absence of every defect, arbitrary direct `LogFile` writers, simultaneous
startup races, hardware power-loss durability, or unbounded liveness. The 256 MiB
default is configured and asserted; rotation execution uses small real thresholds,
not a production-size disk campaign. Inbox/expiry logs are unsegmented.
The identity model treats registry transactions atomically; implementation
acceptance covers ordinary restarts and lost replies rather than arbitrary
registry rename/write-failure campaigns.

The Python/NDJSON adapter remains a prototype/tool regression fixture; the
production fault replay path uses generated Rust bindings.

## Publication

The implementation and evidence are prepared for the requested commit/push.
The [publication record](publication.json) lists original and published hashes
for 126 archived fixture configuration files whose generated test credentials
were replaced with credential hashes. Registry and log bytes, model traces,
and the qualified source freeze are unchanged. The
[original qualification ledger](qualification/results-before-publication.json)
preserves the capture hashes; the current ledger hashes the published artifacts.
Historical per-run manifests retain their original capture hashes, resolved
through the publication record for those redacted files.

The 127 MiB diagnostic baseline executable, Python bytecode caches, and TLC
scratch state files remain local and are excluded from Git. Their paths and
hashes are recorded in the publication record. Logs, configurations, witnesses,
crash markers, lock files, and segment bytes remain in the published evidence.
