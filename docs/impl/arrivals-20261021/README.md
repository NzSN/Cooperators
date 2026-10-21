# Arrival notifications implementation

Date: 2026-10-21. Status: implementation checkpoint; qualification in progress.

Work is stopped for transfer to a new computer. The
[handoff](HANDOFF.md) records the remaining adapter race and qualification gates.

Implements [design 0004](../../design/0004-arrival-notifications-and-agent-activation.md)
against source baseline `edcbc923`. Existing acceptance and throughput evidence
remain historical qualifications of their pinned builds.

| Wave | Work | Status |
|---|---|---|
| N1 | Registry V2 guard, stable bus ID, source/arrival schema, migration and historical-hash compatibility | Implemented; preserved old executable refuses V2 before altering guarded source files |
| N2 | Exact subscriptions, catch-up and document/queue routing including requeues | Implemented; 7 focused source tests passed with injection |
| N3 | Separate durable feed/ACK, streaming recovery and bounded replay projection | Implemented; 9 service, 4 fault and 7 streaming tests passed; four actual SIGKILL controls passed |
| N4 | Durable host bridge, binding, coalesced wakes, observation/resolution and continuation | Implemented; focused bridge/runtime and required real-UDS IPC gates passed; combined checkpoint gate pending |
| N5 | Managed Codex App Server adapter and real session qualification | Adapter and CLI implemented; deterministic adapter tests passed; real model session pending |
| N6 | Managed Claude host-mode adapter and real session qualification | Adapter and CLI implemented; deterministic adapter tests passed; real model session pending |

Optional Claude Channels preview remains opt-in and does not gate the managed
host paths. Every passing, failed or unavailable runtime gate will be recorded
with its exact source/executable identity. No existing user bus or global MCP
configuration is selected for qualification.

## Recorded focused results

- [Source routing](qualification/source-run-01/README.md), with the subsequent
  [injection run](qualification/source-run-02/cargo-test.log) covering uncertain
  subscription admission fencing.
- [Service and fault tests](qualification/service-run-01/README.md).
- [Streaming recovery](qualification/streaming-run-01/README.md).
- [Actual process crash and old-binary refusal](qualification/crash-run-01/README.md).
- Host bridge/runtime, IPC and adapter logs are retained under
  `qualification/host-bridge-run-01/`, `qualification/host-control-run-01/`
  and `qualification/host-adapters-run-01/` as available. A compiled test
  harness result is distinguished from a complete Cargo command when a
  concurrently missing CLI module interrupted an earlier build.

These are scoped results. The source/host records have independent pins;
native provider activation, the final complete broker suites, generated MBT,
and performance measurements are not yet claimed as passed by this checkpoint.

## Qualification order

1. Build the integrated broker and run focused migration, source and service
   gates. Validate root upgrade against the preserved pre-upgrade executable.
2. Run fault and actual process-crash controls for source intent, feed and ACK
   recovery, including independent recipient progress.
3. Run bridge/model protocol cases through deterministic fake hosts.
4. Exercise real UDS/MCP and managed host sessions on disposable roots, with
   explicit approval handling and retained observation/resolution evidence.
5. Run the full broker suites and generated MBT against the final source, then
   measure notification projection and host admission latency independently of
   model execution and human approval time.
