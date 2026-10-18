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
| T3 | M2: channels (publish/read+replay), queues with leases+fencing+dead-letters, documents publish/claim (CAS, edit leases, blob store ≤64KiB inline) | lease/fence/dead-letter suite + doc CAS/claim suite | pending |
| T4 | M3: `coop mcp-stdio` adapter (rmcp transport-io) + streamable HTTP endpoint; golden-transcript conformance proving UDS/stdio/HTTP parity | conformance suite green + automated MCP-client integration test | pending |
| T5 | M4: backpressure caps, `/healthz`+`/metrics`, perf smoke vs §11 targets, real README + config reference + operator docs | perf smoke recorded in `docs/impl/perf-smoke.md`; docs complete | pending |

## Log

- 2026-10-18 — breakdown created; stack validated offline (tokio/serde/sha2/rmcp build from cache).
- 2026-10-18 — T1 done: 21 tests green; UDS tests skip in sub-agent sandboxes (AF_UNIX EPERM) but run and pass under the coordinator. Known limit: hash chain authenticates links; a modified final record is undetectable until next append (documented in src/storage.rs).
- 2026-10-18 — Correction: T1 UDS tests skipped in the coordinator sandbox too (SKIP hidden without --nocapture); earlier claim of real-UDS verification was wrong. No socket transport is testable in this environment; transport gates defer to host-side runs.
- 2026-10-18 — T2 done (bb44ab7 base): registry/auth, send/wait/ack with peek cursors, durable dedupe, presence, functional CLI. 17 tests green in-process; `tests/m1_daemon.rs` skips in-sandbox.
