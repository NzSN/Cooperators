# G0: initial source and environment baseline

Captured 2026-10-19, beginning at **09:31:39 UTC**. This record belongs to
[execution plan 0002](../../../design/0002-generated-adapter-fault-mbt-plan.md),
G0. It freezes the input before the generated-port storage pilot. No production
repair or fresh runtime acceptance is asserted by this record.

Checkout: `/home/nzsn/Repos/Cooperators`, HEAD
`ccca77295277b10dad5234bf8ae3a916ad9a0a1a`. The initial `src/` files were clean;
model, MBT, design, and chat-document work was already dirty or untracked.
[Initial manifest](initial-source-manifest.json) records the complete initial
status and SHA-256, byte size, and modification time of 122 relevant files.
All 122 matching files are retained in [source-snapshot](source-snapshot/);
[snapshot verification](source-snapshot-status.json) records no missing initial
file. The chat archive and this evidence subtree are excluded from that source
manifest. Work by concurrent agents after this freeze requires a new build/run
manifest; it does not change the meaning of this initial snapshot.

## Build and environment gate

The original pinned dependency graph cannot resolve offline:

```text
cargo metadata --offline --locked --format-version 1
exit 101: failed to download hyper v1.11.1
          attempting to make an HTTP request, but --offline was specified
```

[Probe record](offline-metadata-probe.json) preserves the actual result.
[Cache and socket inventory](environment-probes.json) confirms that every
direct pinned dependency archive except `hyper =1.11.1` is present. Cached
`hyper` releases are **1.8.1 and 1.12.0**; pinned `hyper-util =0.1.20` is
present, as is 0.1.21. A dependency change must be recorded as a build-input
change before fresh qualification. This baseline did not edit dependencies.

Both AF_UNIX bind and IPv4 loopback bind return `PermissionError(1,
'Operation not permitted')` in this sandbox. A normal-process UDS gate therefore
remains blocked here. A host or approved execution context must run socket
tests with required sockets enabled; an unavailable socket is not a passing
test. In-process storage injection can be developed and exercised independently.

Tool versions: Cargo 1.95.0, rustc 1.95.0 (`59807616e`, LLVM 22.1.2), Python
3.12.3, OpenJDK 25.0.4.1. Full paths, executable SHA-256 hashes, byte sizes,
modification times, sibling checkout revisions/status, and command outputs are
in [tool-and-evidence-manifest.json](tool-and-evidence-manifest.json).

| Component | Observed identity |
| --- | --- |
| Interface compiler | `/home/nzsn/Repos/Mirrors/.lake/build/bin/model_interface_gen`, SHA-256 `143fbe36a1143771d8ee15b76eb0ae6e661f70a7d4c04e58eba1af67ade05c35` |
| Compiler repository | Mirrors `d1c5e7482c4adf02fd94820e239c1e39c240b1d2` |
| MirrorRust repository | `5049e99886a3d1f775e60190478c26e39c9aefc9` |
| ModelMirrors executable | `/home/nzsn/.local/bin/ModelMirrors`, SHA-256 `cb790cfc85b5152769d4d9efd7638a09a40b8dafd93276e5dd223a8b37ca348f` |
| ModelMirrors version command | `ModelMirrors --version` reports `v0.0.3.2` |
| ModelMirros repository | `14e25c905abf5f3b096d8e545791bb1efe1b812e` |

These are current observations, not recovered historical binary/source hashes.
The executable is not proven to have been built from the current sibling HEAD.
The earlier documentation's `0.1.1.0` label and current `--version` output are
different observations; the recorded plain replay limitation remains explicit.

## Preserved writer baseline

The retained [historical replay log](historical/multi-writer-replay.log) reports
**39 traces, 3 failures**: 34 protocol traces and two writer controls passed;
both different-socket competing-writer orders failed; rejected same-socket
startup also failed its read-only requirement. The actual mismatches include
two accepted PIDs for one root, and recovery truncation/append performed by a
startup that later rejects. This is historical evidence copied without rerun.

The three raw failing fixture roots and child stderr logs are retained under
[historical/fixtures](historical/fixtures/). They contain disposable local-test
registry credentials. The five writer traces, build log, negative controls,
model-check log, isolated build manifests, and isolated core module mapping are
also retained under [historical](historical/). Their individual hashes are
recorded in the tool/evidence manifest. No writer sockets or live processes
are needed to read those retained files.

The historical build substituted the unused HTTP module and included production
core source files by absolute `#[path]`. It used development/unoptimized
compilation, production daemon startup with HTTP disabled, real UDS sockets,
and subprocess lifetimes. It does **not** qualify the full pinned binary, HTTP,
fresh runtime results after instrumentation, or downstream corruption caused by
two writers. Protocol observations have the existing reconstructed-log limit;
writer observations read real files and child/process state.

Retained run-command evidence identifies the actual replay executable and
recipe (observed 03:36–03:41 UTC in the referenced session record):

```sh
MIRROR_BIN=/home/nzsn/.local/bin/ModelMirrors \
CARGO_TARGET_DIR=/tmp/cooperators-review-core/target \
cargo run --offline --locked \
  --manifest-path /tmp/cooperators-mbt-retry/Cargo.toml -- --skip-gen
```

That recipe requires the preserved isolated-core dependencies and a context
supporting UDS. It is diagnostic scope, not the final full-build recipe. The
`/tmp` binary's currently observed hash is recorded, but it is not copied as
an enduring executable and is not asserted to be a historical build pin.
Plain ModelMirrors replay is used; local compiler checks bind generated
artifacts. Server-side semantic-digest negotiation was unavailable in the
recorded pipeline and must not be claimed from plain replay.

## G0 disposition

Source/tool inputs and historical failures are retained and identifiable.
The **original pinned full build is blocked by the offline cache** and the
**sandbox socket gate is blocked by EPERM**. Subsequent work must record the
chosen cached dependency pin and run required socket tests in a capable context.
No missing hook, skipped transport, or historical success closes a new defect.
