# Cooperators — Implementation Tasks (from design 0001)

Derived from `docs/design/0001-cooperatorsd-broker.md` milestones M1–M4.
Execution: sequential waves, one delegated agent per wave; the coordinator verifies
the gate and commits between waves. Status column is the live tracker.

## Environment constraints (binding for every wave)

- Writable: repo root and `/tmp` only. `.git`, `.agents`, `.codex`, `.aws` are
  read-only tmpfs masks — never write there.
- Cargo is **offline-only**: always `cargo build|test --offline`; pin exact versions
  present in `~/.cargo/registry/cache` (verified: tokio 1.x, serde 1.0.228+,
  serde_json 1.0.149+, sha2 0.10.9, rmcp **=3.2.0**). If resolution selects an
  uncached version, pin `=x.y.z` from the cache or drop the dependency.
- Git: `GIT_DIR=/tmp/cooperators.git GIT_WORK_TREE=/home/nzsn/Repos/Cooperators`.
  Workers never commit; coordinator commits per wave.
- Sockets are DENIED in this sandbox (AF_UNIX and TCP loopback both EPERM, coordinator
  included). Socket-dependent tests must probe-and-skip; the real-socket gate runs
  host-side: `COOP_REQUIRE_UDS=1 cargo test --offline` (skip becomes failure).

## Waves

| ID | Scope | Gate (all must pass) | Status |
|----|-------|----------------------|--------|
| T1 | Cargo scaffold (`cooperators` crate, `coop` bin); envelope serde types; JSONL append+fsync+replay with SHA-256 hash chain and torn-tail recovery; per-log sequencer; UDS NDJSON JSON-RPC server with `ping`/`echo`; CLI skeleton | unit: envelope roundtrip, hash-chain verify, torn-tail truncate, seq monotonicity; integration: echo over UDS | **done** (bb44ab7) — socket tests SKIP in-sandbox (AF_UNIX EPERM, both coordinator and sub-agent); in-process paths verified |
| T2 | M1 core: registry+token auth (0600), `send/wait/ack` inboxes with peek cursors, presence heartbeats/stale transitions, dedupe by idempotency key | M1 integration suite incl. `kill -9` → replay durability | **done** (17 tests; real-daemon test skips in-sandbox) |
| T3 | M2: channels (publish/read+replay), queues with leases+fencing+dead-letters, documents publish/claim (CAS, edit leases, blob store ≤64KiB inline) | lease/fence/dead-letter suite + doc CAS/claim suite | **done** (38 tests green, fmt clean) |
| T4 | M3: `coop mcp-stdio` adapter (rmcp transport-io) + streamable HTTP endpoint; golden-transcript conformance proving UDS/stdio/HTTP parity | conformance suite green + automated MCP-client integration test | **done** (12 suites green; live UDS path deferred to host-side run) |
| T7 | MBT: Apalache trace battery (spec/MBCooperators.tla) replayed against the real daemon via MirrorRust/ModelMirrors | 34 traces, 0 failures; negative control (corrupted trace) fails as required | **done** |
| T6 | TLA+ spec of broker semantics (log, messaging, queue fencing, doc CAS/claim, crash) + TLC evidence | all 9 invariants + `TaskResolved` temporal property, exhaustive small model | **done** (1.67M distinct states, no error) |
| T5 | M4: backpressure caps, `/healthz`+`/metrics`, perf smoke vs §11 targets, real README + config reference + operator docs | perf smoke recorded in `docs/impl/perf-smoke.md`; docs complete | **done** (56 tests green; append-throughput target MISSED — fsync-bound, recorded) |

## Log

- 2026-10-18 — breakdown created; stack validated offline (tokio/serde/sha2/rmcp build from cache).
- 2026-10-18 — T1 done: 21 tests green; UDS tests skip in sub-agent sandboxes (AF_UNIX EPERM) but run and pass under the coordinator. Known limit: hash chain authenticates links; a modified final record is undetectable until next append (documented in src/storage.rs).
- 2026-10-18 — Correction: T1 UDS tests skipped in the coordinator sandbox too (SKIP hidden without --nocapture); earlier claim of real-UDS verification was wrong. No socket transport is testable in this environment; transport gates defer to host-side runs.
- 2026-10-18 — T2 done (bb44ab7 base): registry/auth, send/wait/ack with peek cursors, durable dedupe, presence, functional CLI. 17 tests green in-process; `tests/m1_daemon.rs` skips in-sandbox.
- 2026-10-18 — T3 done: channels/queues/docs + shared lease manager; 14 new RPC routes; structured CONFLICT_CLAIMED/VERSION_CONFLICT data; blob spillover >64KiB.
- 2026-10-18 — T4 done: 21 `coop_*` MCP tools over stdio (rmcp 3.2.0) + streamable HTTP /mcp on 127.0.0.1:7420 with bearer auth; golden transcripts in tests/conformance/ run against both dispatch layers; `--embedded` in-process mode for socketless testing. Live-daemon↔stdio↔UDS path must be exercised host-side.
- 2026-10-18 — T5 done: inbox caps, /healthz + bearer /metrics, stdio 4MiB cap, README + docs/operator.md + docs/impl/perf-smoke.md. Perf: send-ack p50 2.674ms PASS, wait-delivery 0.010ms PASS, replay 6.1s/1M PASS, sustained append 335msg/s MISS (fsync-bound on WSL2 ext4; durability contract kept; group commit is the documented follow-up).
- 2026-10-18 — T6 done: spec/Cooperators.tla + .cfg; TLC exhaustive at Len(log)<=6, Agents=2, Keys=1, MaxAttempts=2: 1,673,205 distinct states, no error, TaskResolved holds. Larger bounds timed out (interim-clean, not claimed).
- 2026-10-19 — T7 done: spec/mbt/ harness; 34-trace battery, 0 failures, all 10 actions covered; MBT caught a real Dead-semantics divergence (attempts counted at claim; dead on MaxAttempts-th failed delivery) — spec fixed, consistent with the TLC spec. ModelMirrors gen protocol avoided (>64KiB line cap); generation runs apalache-mc directly.
- 2026-10-19 — MBT adapter now GENERATED: scaffold→resolve→generate→check via Mirrors model_interface_gen (mirrorrust-v2); lock 59810995…; `check` clean; 34/34 traces pass through the generated binding; negative control still fails as required. Negotiated (digest-checked) replay unavailable: ModelMirrors 0.1.1.0 lacks mirrors.model-interface-negotiation/v1.

## Acceptance follow-up — 2026-10-20

The requested installed-client exercise and performance baseline have run.
Full evidence and reproduction commands are in
[`docs/impl/acceptance-20261020/README.md`](docs/impl/acceptance-20261020/README.md).

| Work | Result |
|---|---|
| Actual installed Codex CLI and Claude Code exchange | Message/task/claim/completion/ack exercise passed with Claude `MCP_PROTOCOL_NEGOTIATION=legacy`; default modern discovery failed because `tools/list` omits required cache fields. |
| Real UDS and HTTP latency | Send p50 2.680 ms / 3.378 ms; both below 10 ms. Pending-recipient delivery supplement p50 4.828 ms / 4.815 ms, with client-observed pending waits. |
| Sustained append | 341.628 messages/sec; 5,000 target still missed. |
| 100-agent baseline | 99 idle waiters preserve the predeclared diagnostic bounds; 100 active senders produce 398.864 ms send p50, missing 10 ms. |
| Actual million-record replay | Storage median 8.642 s; full daemon startup median 12.439 s. Full startup misses 10 s in two of three samples. |

Production source and the default release executable remained unchanged during
those baseline exercises. The following write-path wave changes that source.

## Durable throughput follow-up — 2026-10-20

Step 3 implemented bounded per-inbox group commit, independent committed read
views, cancellation-safe completions, shared pending-key dedupe, and recovery
quarantine. See [design 0003](docs/design/0003-durable-inbox-group-commit.md) and
the [full results](docs/impl/throughput-20261020/README.md).

| Gate | Result |
|---|---|
| Core correctness | 92 normal / 124 injection tests passed, one pre-existing ignored performance test in each; required real UDS checks did not skip. |
| Additional batch process crashes | 3/3 actual SIGKILL controls passed before sync, after receipts and after partial batch failure; separate targeted integration run passed 4 tests including its no-op child helper. |
| Generated MBT | 63/63 traces, 2/2 actual rotation crash controls, compiler/provenance checks passed against matching final core source and build manifests. |
| Native shared inbox, 32 senders | 6,030.3 durable msg/s, send p50 5.022 ms: both numeric targets PASS. |
| Native shared inbox, 100 senders | 12,834.9 durable msg/s, send p50 7.469 ms: both numeric targets PASS. |
| Native distributed inboxes, 100 agents | 4,608.1 durable msg/s, send p50 21.407 ms: both targets MISS. All eight native candidate cases passed correctness. |
| Original Python workloads | Shared-inbox C100 reaches the unchanged backlog cap; successful cases retain their target misses. Original 100-active send/wait/ack control improves to 990.8 cycles/s but send p50 27.016 ms and wait p50 25.981 ms miss. |

The write-path implementation and scoped correctness qualification are complete;
overall performance acceptance remains open. Retained failures, single-sender
overhead, and distributed/client workload limits are part of the report.
Other outstanding work is the modern MCP compatibility correction and full
startup/replay performance.
