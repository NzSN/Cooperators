# Arrival implementation handoff

Stopped implementation at the user's request to commit and move to a new
computer. This is a buildable implementation checkpoint, not final acceptance.

## Current state

- Broker subscriptions, source intent, V2 format guard, independent arrival
  feed/ACK, projection/recovery, managed bridge, IPC and provider adapters are
  implemented. The managed CLI supports new sessions and explicit resume.
- The last complete `cargo check --offline --locked --all-targets --features
  mbt-injection` passed.
- Focused source/service/fault/streaming, actual process-crash/old-binary guard,
  bridge/runtime, required real-UDS IPC, adapter and 128 generated model-trace
  gates have passing runs. See the retained qualification directories.
- Managed CLI launch/resume and startup redaction passed against a protocol
  fake. Real Codex/Claude model-session qualification has not run.

## Next work

1. Fix the buffered-frame fail-closed race in `src/host/adapters.rs`:
   `claude_wrong_session_and_stale_generation_never_get_a_second_wake` fails
   because a queued approval can change Offline back to WaitingForPermission
   after a wrong-session frame closes the process. Review both adapter `pump`
   paths so a closed/fatal session cannot be revived by buffered frames.
2. Rerun the complete normal and `mbt-injection` suites with required real
   sockets. `qualification/checkpoint-run-02/` retains the current failure;
   it is not a passing aggregate qualification. The earlier executable-file
   fixture race was addressed with unique, synced and closed fake scripts.
3. Run generated MBT against the final source, then real managed-provider idle,
   busy, approval, restart and resolution gates on disposable bus roots.
4. Finish projection/admission performance measurements and update the
   implementation record and operator qualification status.

## Moving machines

Source and evidence are in Git. Build outputs, private provider credentials,
host resume configurations and `/tmp` executables are not repository artifacts.
The old-binary refusal control used the pre-arrival source commit `edcbc923`;
rebuild that revision separately when the retained `/tmp` binary is unavailable.
Cargo is pinned by `Cargo.lock`; offline commands require a populated cache.

Arrival activation explicitly upgrades a bus to registry V2. Use disposable
roots for qualification; old binaries refuse upgraded roots. No global MCP
configuration or existing user bus was changed by this implementation work.
