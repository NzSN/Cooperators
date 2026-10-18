# Cooperators MBT harness

Replays Apalache witness traces of [`../MBCooperators.tla`](../MBCooperators.tla)
against the real daemon (`DaemonState`, in-process — no sockets) through the
ModelMirrors protocol, using MirrorRust as the client. Every step's reported
abstract state (`log`, `ackCursor`) must equal the trace's expected state;
any divergence is a step mismatch with a field-level diff.

## Prerequisites

- `apalache-mc` on `PATH` (used directly for trace generation — see below).
- `MIRROR_BIN` pointing at a built ModelMirrors executable (used as the replay
  server over stdio; no sockets needed).
- Offline cargo: `cargo build --offline --locked` (the lockfile pins only
  registry crates already extracted on this machine).

## Run

```bash
cd spec/mbt
MIRROR_BIN=/path/to/ModelMirrors cargo run --offline --locked   # generate + replay
MIRROR_BIN=... cargo run --offline --locked -- --skip-gen       # replay committed traces only
```

Exit status is nonzero iff any trace mismatches. Generated/committed traces
live in `traces/` (committed = stable acceptance battery).

## Trace battery

Eight witness invariants in `MBCooperators.tla`: `TraceComplete` (12 distinct
full-length behaviors via `--view=View` dedup) plus action-forcing witnesses
`NoAck`, `NoCrash`, `NoRelease`, `NoExpire`, `NoComplete`, `NoFail`, `NoDead`
(2 each; the length-bounded base battery never contains ack/crash/release
steps because they do not grow the log). Current battery: 34 traces covering
send/ack/claim/expire/complete/fail/release/publish/update/crash.

## Oracle boundary (read before trusting)

- Replay checks **enabled-action equivalence** and **full abstract state
  evolution**: every model-enabled action must succeed on the real daemon and
  reproduce the exact abstract log and ack cursors. Rejection paths
  (VERSION_CONFLICT, CONFLICT_CLAIMED, LEASE_LOST) are *not* probed by traces;
  they are proven by TLC on `../Cooperators.tla` instead.
- The adapter reconstructs the abstract `log` from successful daemon effects
  (call order == effect order, single-writer daemon), and reads ack cursors
  back authoritatively via `wait_message` (so `crash` reset is really
  observed). Lease `fence` values are abstract log positions; concrete fences
  (per-resource seqs) are exercised functionally, not compared numerically.
- Constant vars (`Agents`, `Keys`, `MaxAttempts`, `Q`, `D`) are not part of
  the compared state.

## Findings and limits recorded during bring-up

- **Dead-letter semantics caught by MBT:** the first battery failed 3 traces
  on the sequence claim→expire→claim→fail→**claim**. The implementation
  increments its attempt counter *at claim time* and dead-letters on the
  `MaxAttempts`-th failed delivery; the MBT spec modeled death one terminal
  later. Spec fixed (`Dead == AttemptsQ >= MaxAttempts`), consistent with the
  TLC-verified spec.
- **ModelMirrors gen protocol inlines all traces in one response line**, which
  overflows MirrorRust's 64 KiB protocol-line cap beyond ~3 Cooperators-sized
  traces. Generation therefore runs `apalache-mc` directly (exit 12 = witness
  found); ModelMirrors is used only for replay, whose messages are small.
- Deterministic expiry uses the daemon's `#[doc(hidden)]`
  `debug_force_expire_queue/doc` hooks — the exact sweeper event path with an
  infinite clock. Never used in production paths.
