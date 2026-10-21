# Pending-recipient wait delivery — 2026-10-20

This supplement measures message delivery while the recipient already has a
pending long-poll call over an actual transport. It complements the
[transport and scale baseline](../transport-scale/README.md), whose wait
samples retrieved an already durable message after the sender's acknowledgment.
The original qualified transport harness remains byte-for-byte unchanged.

## Method and scope

For each transport, the standalone
[wakeup harness](../../../../scripts/benchmark_wait_wakeup.py) starts a fresh
release daemon and enrolls separate sender and recipient identities. Each
identity has its own persistent connection. UDS uses NDJSON JSON-RPC;
HTTP uses separate initialized streamable HTTP MCP sessions over loopback
TCP. The release executable and production source hashes must match the
prior transport baseline before the supplement can start.

For every sample, a recipient thread issues
`wait_message(timeout_s=600)`. A hook records when the UDS `sendall` or HTTP
`request` call has passed the request bytes to the operating system. The
sender waits at least 25 ms, checks that the recipient future is still
unfinished, then sends a unique 128-byte message. All 220 samples, including
warmups, met these qualification conditions. Measured qualification delays
were 25.169–25.757 ms for UDS and 25.172–25.788 ms for HTTP.

The timing witness is visible from the client. No production instrumentation
was added, so it does not prove the exact timestamp at which the server
entered its wait state. The delay and unfinished-future check support an
outstanding recipient request. The results therefore describe observed
delivery to a pending client wait and do not independently qualify an exact
production server-wakeup latency target.

The recipient thread records delivery immediately after its wait call
returns the parsed response. The sender records its send-call start and
the time its durable acknowledgment is parsed. Two timing scopes are kept:

- **Send start → delivery:** elapsed time from the sender starting its send
  call until the recipient returns the message, including durable append.
- **Signed send acknowledgment → delivery:** recipient delivery timestamp
  minus sender acknowledgment timestamp. A negative value means the
  recipient observed delivery before the sender observed its acknowledgment;
  those samples are retained without clamping.

These client timestamps include serialization, response parsing, Python
thread scheduling and the GIL. They do not isolate daemon execution time.
Each transport has 10 warmups and 100 measured samples. Percentiles use the
nearest-rank rule and exclude warmups. Every sample verifies message ID,
sequence number, payload, sender/recipient identities, and the recipient's
acknowledgment cursor. The corresponding durable records are checked after
the phase; a final wait verifies that the acknowledged inbox is empty.

## Results

The run completed from 09:16:56 to 09:17:03 UTC on 2026-10-20 on the same
Intel i5-1240P, WSL2, ext4 host as the transport baseline. All values below
are milliseconds.

| Transport and timing scope | p50 | p95 | p99 | Minimum |
|---|---:|---:|---:|---:|
| UDS send start → delivery | 4.828 | 6.948 | 9.810 | 2.809 |
| UDS signed acknowledgment → delivery | 0.205 | 0.429 | 0.514 | -0.232 |
| HTTP MCP send start → delivery | 4.815 | 7.257 | 7.866 | 2.756 |
| HTTP MCP signed acknowledgment → delivery | 0.282 | 0.581 | 0.805 | -0.619 |

UDS had 6 negative acknowledgment-to-delivery samples and HTTP had 26.
Observed send-start-to-delivery p50 was below 25 ms for both transports.
Because the server's wait entry is not instrumented, this observation is
reported without claiming that it closes an exact production wakeup gate.

All **220 receipts, deliveries, acknowledgments, and durable records**
matched. Per-inbox sequences were contiguous, and no messages remained
after acknowledgment. The retained-artifact credential scan passed.

The executable SHA-256 stayed
`c6d1047a8a4d9b0094274151c9658a101ed018ce07f82d66ed9fb512eb285321`.
Production source hashes matched before and after. The unchanged original
transport helper SHA-256 stayed
`685580eadb8b5b3f4a827ca750ea969bbba65df9d30dd519babcf62b475bf18c`.
No core source or daemon configuration was changed.

## Reproduce and inspect

From the repository root, with permission to bind local TCP and Unix sockets:

```sh
python3 scripts/benchmark_wait_wakeup.py \
  --binary target/release/coop \
  --output docs/impl/acceptance-20261020/transport-wakeup/run-01
```

Use a new output directory for another run. The prior transport baseline's
`run-01/environment.json` provides the required binary, source, and helper
pins. The standalone script imports the existing helper classes without
modifying that file. Other benchmark and build processes were idle during
this supplement.

[run-01/summary.json](run-01/summary.json) contains the full results and
timing scopes. `plan.json` was written before measurement.
`environment.json` records source, executable, script, and helper hashes.
Each transport folder contains raw per-sample timestamps, qualification
witnesses, daemon identity, and actual regular bus files. The credential
scan result and artifact hash manifest are retained alongside the summary.
