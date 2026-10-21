# Arrival implementation handoff

Updated 2026-10-21 after resuming on the new computer. Broker qualification and
scoped managed Codex acceptance pass; full acceptance remains partial.
See the [resumed record](qualification/resume-run-01/README.md).

## Current state

- Broker subscriptions, source intent, V2 format guard, independent arrival
  feed/ACK, projection/recovery, managed bridge, IPC and provider adapters are
  implemented. The managed CLI supports new sessions and explicit resume.
- Closed transports reject buffered frames. The original Claude race and new
  Codex/Claude buffered-frame cases pass. New Codex threads receive a durable
  name before bootstrap exits so their exact ID can be resumed without a model
  turn. Protocol fixtures are immutable and the MCP schema contract is current.
- Required-socket suites passed: 181 normal and 230 injection tests, with one
  pre-existing ignored performance test each and no skips. Both include all
  128 arrival/bridge traces. Generated MBT passed 63 traces. Compiler/binding,
  crash and rebuilt old-binary refusal gates passed.
- Actual Codex CLI 0.163.0 passed three document wakes, nonce confirmation,
  pinned reads/resolution, explicit approvals and exact-binding broker/controller
  restart. A duplicate resolution retained its original receipt.
- Source/feed upper-bound p95 was 7.634 ms over 200 samples. Controlled immediate
  protocol-host admission upper-bound p95 was 239.233 ms over 30 samples.
  These do not qualify broad throughput or real-provider admission percentiles.
- Claude Code 2.1.185 reports no authentication/provider configuration, so its
  real gate could not start. The earlier machine used 2.1.300.

## Next work

1. Authenticate Claude (`claude auth login`) or explicitly configure the intended
   provider/model. Run real managed idle/busy, approval, restart and resolution
   gates on disposable roots, qualifying this installed protocol version afresh.
2. Extend real provider coverage to queue adoption, other restart orderings and
   provider-side lost replies. Related deterministic tests pass; real Codex
   evidence covers document arrivals and one restart sequence.
3. Extend performance evidence to representative concurrency/retained histories,
   memory/sync costs, installed-provider admission percentiles and startup.
   Preserve earlier distributed/100-agent and full-startup misses.
4. Correct modern Claude MCP discovery compatibility and repeat its default
   negotiation acceptance independently of the legacy workaround.

`qualification/checkpoint-run-02/` and earlier resumed failures remain historical
failures. Final passing receipts are named explicitly in the resumed record.

## Moving machines

Source and retained evidence are repository artifacts. Build outputs, private provider credentials,
host resume configurations and `/tmp` executables are not repository artifacts.
The old-binary refusal control used the pre-arrival source commit `edcbc923`;
rebuild that revision separately when the retained `/tmp` binary is unavailable.
Cargo is pinned by `Cargo.lock`; offline commands require a populated cache.

The qualified release SHA-256 is
`732ffba442bd3c89ec9e7a3f66e2e01dba902d13d934dcc732560f6ece419965`.
No global replacement executable was installed.

Arrival activation promotes a bus to registry V2. Qualify disposable roots;
old binaries refuse upgraded roots. No existing user bus or global MCP
registration was selected for these runs.
