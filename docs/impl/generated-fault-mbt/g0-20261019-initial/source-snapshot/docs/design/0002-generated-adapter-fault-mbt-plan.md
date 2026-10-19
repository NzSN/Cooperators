# Generated-adapter MBT for implementation defects

Date: 2026-10-19. Status: proposed execution plan; implementation has not started.

The objective is to reproduce the seven reviewed implementation defect classes
and the accepted per-launch identity requirement through compiler-generated
adapter ports, then use the same cases to verify production fixes.

This plan follows the architecture already used by the protocol and writer
suites:

```text
TLA+ actions and traces + reviewed model-interface contract
    -> resolve/lock -> generate/check Rust port and binding
    -> concrete Rust trait implementation + real fault hooks
    -> production Cooperators operations
    -> actual responses, receipts, files, and metadata
    -> MirrorRust/ModelMirrors replay comparison
```

The compiler owns port declarations, types, codecs, dispatch, lifecycle guards,
digest bindings, and coverage plumbing. Concrete Rust trait implementations
own production calls, failure/clock/barrier controls, and observations. Production
fixes remain in the owning Cooperators modules.

The custom Python/NDJSON fault driver is a standalone prototype. It is not the
integration target. Retain the useful model, traces, and observation rules;
reuse their tests where appropriate without maintaining another adapter protocol.

## Existing foundation

- [MBFaults.tla](../../spec/MBFaults.tla): eight bounded profiles, nineteen
  exported witnesses, and twenty-one deliberately faulty policies.
- [Fault corpus](../../spec/mbt/faults/manifest.json): recorded 48 model checks
  and 40 fixed-input checks. The latter verify that the correct traces can
  distinguish all twenty-one faulty policies in the model.
- [Current generated-adapter pipeline](../../spec/mbt/README.md): generated
  protocol/writer traits implemented by `CoopSut` and `WriterSut`.
- Existing runtime evidence: 34 protocol traces and two writer controls pass;
  three writer traces fail. That evidence used a documented isolated core build
  with HTTP disabled/stubbed, not the full pinned binary.
- The new fault profiles are not integrated into generated Rust bindings or
  the current Rust harness's `--suite all`.

## Execution waves and gates

Execute one bounded wave at a time. Preserve the existing dirty work and keep
generated files compiler-owned. Do not combine instrumentation with semantic
repairs before recording the faulty implementation's behavior.

### G0 — Freeze the baseline and test environment

Scope: evidence and tool/build setup only.

1. Record checkout HEAD plus hashes of relevant dirty source/model/interface
   files. Record compiler binary/revision, MirrorRust revision, the actual
   ModelMirrors executable, build profile, and enabled features/transports.
2. Verify offline Cargo resolution. The recorded missing `hyper =1.11.1`
   dependency must be checked again. Resolve any build change explicitly;
   an isolated core build remains scoped diagnostic evidence.
3. Confirm a host environment supports real UDS/process tests. A denied socket,
   missing executable, or absent injection capability is a qualification
   failure/blocker, never a skipped passing case.
4. Preserve the existing writer failures and protocol controls as the baseline.
   Reuse source-matching evidence where valid; rerun paths changed by later work.

Gate: a reproducible build/run recipe and source/tool identities. Final runtime
acceptance must identify the real binary and tested transport scope; it cannot
inherit full-binary qualification from an HTTP-stubbed diagnostic build.

### G1 — Compiler-compatible storage interface

Scope: the storage profile first, without production correctness repairs.

1. Review the observable schema. Expose actual operation results, raw record
   identity/sequence, and factual successful receipts. Internal state such as
   `durable`, pending work, or quarantine must not be invented by the adapter.
   Remove or redesign a projected field if no authoritative observation exists.
2. Prove that the compiler accepts the TLC-derived ITF evidence, action names,
   parameter paths, initialization, and selected observations. Verify projected
   trace compatibility rather than assuming every source variable must become
   a runtime observation.
3. Default to a typed contract per fault profile, sharing Rust fixture helpers.
   This avoids requiring one field to have eight unrelated record shapes.
   A unified tagged schema is an alternative only after compiler support is
   demonstrated. Keep the existing protocol/writer interfaces working.
4. Follow the existing scaffold/review -> resolve -> generate -> check pipeline
   with the supported Rust target. Record locks, semantic digests, compiler
   ownership manifests, and generated coverage.
5. Compile the generated binding and a concrete storage trait implementation.
   Expected I/O errors and rejections become explicit result observations;
   unexpected port/tool failures remain `BindingError`.

Planned ownership: model/trace wrappers under `spec/`; reviewed fault contracts,
locks, generated bindings, and Rust implementations under `spec/mbt/`. Exact
filenames follow the accepted profile layout. No generated file is hand-edited.

Gate: compiler `check` is clean, the generated Rust binding compiles, and a
normal storage control reaches production code through generated dispatch.
This is interface acceptance, not defect reproduction.

### G2 — Real storage failures through the generated port

Scope: reusable injection seams plus a failing storage reproduction.

1. Add narrow Rust I/O controls for a partial write and a sync error after bytes
   have been written. Normal builds retain real filesystem operations with
   injection disabled. Arm faults per fixture/operation, not globally by timing.
2. Add barriers around the actual append operation: pause after write, release
   sync normally or with the armed error, and collect its real completion.
   Keep crash/recovery lifetime control separate from returning a synthetic error.
3. Implement the generated storage methods using those controls. A follow-up
   append must reach the actual writer; the adapter cannot enforce the modeled
   rejection itself.
4. Preserve successful API receipt history outside the restarted SUT. Compare
   IDs and sequences with raw recovered records. Read-only observation must not
   open a repairing `LogFile` handle or rebuild a predicted log from call order.
5. Replay sync-error recovery, partial-write recovery, and preservation of an
   earlier acknowledged record. Preserve actual mismatches before fixing storage.
6. Check normal append/restart controls and negative tests for a wrong receipt,
   corrupt observation, missing injection, and stale generated artifacts.

Primary production seam: `src/storage.rs`; concrete port code belongs in
`spec/mbt/src/`. Any instrumentation-induced semantic difference must be reviewed.

Gate: a real storage mismatch through the compiler-generated binding, with
actual bytes/receipts and matching build identity. A model mutant, a synthetic
peer, or an unavailable hook does not satisfy this gate.

### G3 — Extend generated bindings and observations to every profile

Scope: the detection matrix below, still separating injection from fixes.

1. Reuse the storage/compiler pattern for the remaining typed profiles.
   Extend the existing writer integration where appropriate; retain its
   independent-root and handoff controls.
2. Implement clock, projection/publication barriers, process lifecycle, status,
   rotation, and session-provisioning operations against real code paths.
3. Expand the nineteen-case starting corpus where necessary. In particular,
   rotation needs channel, queue, and document paths; identity needs actual
   delivery, acknowledgments, dedupe, presence, and cross-instance lease checks.
4. Expose all required cases in the existing Rust/ModelMirrors harness. The
   expanded `all` gate must fail if a required trace, profile, observation,
   generated binding, or real hook is missing. Keep explicit suite selections.
5. For each row, retain a minimal reproducing trace and authoritative evidence,
   or report an explicit blocked/missing-feature result. Do not label a missing
   capability as a reproduced implementation mismatch.

Gate: every row has a reviewed generated interface, real execution path, and
classified result. Compiler/projection issues are resolved before claiming
implementation coverage.

### G4 — Fix production code and demonstrate the same cases pass

Fix one owning subsystem at a time after its reproduction is recorded. Keep
fault activation and observation logic unchanged so the repair is measured by
the original case. Add normal-operation and boundary controls relevant to that
repair. Regenerate interfaces/traces and rerun the affected model checks when
the modeled policy or public contract changes.

Gate for each row: the known-bad behavior is recorded, the real source fix is
reviewed, and the same generated-port replay now passes with actual observations.
New features such as enrollment/rotation require positive production acceptance,
not only the disappearance of a missing-capability error.

### G5 — Full qualification and closure

1. Run all required generated-interface checks and all required runtime profiles
   against an identified full build in an environment supporting its transports.
2. Retain original protocol/writer regressions, relevant hardening progress
   properties, and fault-detection controls. Existing supported replay/digest
   limitations remain explicitly recorded rather than silently upgraded.
3. Record corpus/contract/source/compiler/build hashes and a per-defect result
   ledger with the failing and repaired evidence.
4. Retire the standalone NDJSON prototype only after the generated pipeline has
   equivalent reviewed coverage. Preserve useful model/tool regression tests.

Persist qualification reports and artifact manifests under
`docs/impl/generated-fault-mbt/`, identified by run/build ID. Retain the raw
failing fixtures and logs at the locations referenced by those manifests;
`/tmp` staging alone is not the retained qualification record.

Gate: every listed defect/requirement has passing authoritative implementation
evidence, and the aggregate gate cannot pass with missing coverage.

## Detection matrix

| ID | Finding / requirement | Trigger through generated methods | Authoritative evidence | Owning implementation |
|---|---|---|---|---|
| D1 | Multiple writers; replay before ownership | Competing starts with same/different sockets; torn-tail fixture; rejected start and owner handoff | Child PIDs/readiness, actual startup result, durable-file byte comparison, recovered sequence/hash continuity | `src/server.rs`, root ownership and storage opening |
| D2 | Failed write/sync permits unsafe continuation or acknowledged data loss | Partial write; post-write sync error; another append; controlled recovery/restart | Actual error/success receipts held outside the SUT; raw record IDs, sequence/hash chain, recovered contents | `src/storage.rs` |
| D3 | Expired-only inbox strands capacity | Fill a two-record inbox; expire via controlled clock; wait/ack with returned cursor; send again | Returned batch/cursor, actual ack and pending count, send result, inbox records | `src/messaging.rs`, clock/config plumbing |
| D4 | Lost or duplicate dead-letter projection | Durable terminal event; projection failure; restart; crash after dead append before completion; retry | Queue terminal record and all matching dead-log records; actual recovery-worker result | `src/queues.rs`, actual reconciliation path |
| D5 | Failed initial publication reserves a ghost name | Fail/crash after file creation and before initial durable publication; retry same name | Actual publication error/result, file contents, readable committed version | `src/docs.rs` |
| D6 | Status omits a large final record | Valid encoded record larger than 64 KiB; actual status call | Raw final sequence and byte size versus reported head; read leaves bytes unchanged | `src/status.rs` |
| D7 | Missing rotation / broken saved cursor continuity | Cross real configured threshold; save/read across segments; crash after segment preparation; oversized record | Actual ordered segments, complete replay, returned suffix and cursor continuity | `src/storage.rs`, channel/queue/document readers and configuration |
| D8 | Fresh launch must get its own identity | Two same-kind clients sharing config; retries, reconnect, token rotation, independent messaging/presence/leases | Actual issued IDs/tokens and metadata, auth outcomes, real inboxes/acks, presence and lease results | `src/registry.rs`, MCP/RPC/CLI provisioning and affected ownership paths |

Rotation fixtures must use a real supported threshold setting. If absent, record
that feature gap; do not pretend a requested test threshold was applied. The
design default is 256 MiB. A production-default reproduction and a small
configurable-threshold regression have different resource and qualification
scopes. Test support must not rotate files directly to manufacture success.

Identity testing must exercise actual session bootstrap, not just pre-register
two unique test names. Existing unique-name registrations already issue distinct
IDs. The missing requirement is independent launch provisioning from shared
configuration. Retrying/resuming an instance and starting another instance must
remain distinct operations. Naming normalization must be bijective and based on
actual replies; it cannot invent identities or credentials.

## Result ledger and proof boundary

Use separate states for each matrix row:

- **Not wired:** compiler contract, binding, or Rust integration is missing.
- **Blocked:** execution cannot provide the required hook, API, build, or environment.
- **Reproduced:** a real operation mismatches the model, with authoritative evidence.
- **Not reproduced within tested scope:** the case ran without a mismatch; expand or
  review coverage rather than declaring the reviewed defect absent automatically.
- **Fixed and verified:** reviewed production changes make the same case and relevant
  controls pass under recorded source/build identities.

The plan targets the enumerated findings and accepted identity requirement.
Bounded MBT and model checks provide scoped evidence; they do not prove absence
of every implementation defect. Plans and model results are not runtime closure.

The immediate next implementation wave is **G0 then G1/G2: the generated-port
storage pilot**. Expanding all profiles before that vertical slice passes its
interface and reproduction gates risks repeating the standalone-prototype detour.
