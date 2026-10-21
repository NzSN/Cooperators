# Transport and 100-agent baseline — 2026-10-20

The standalone [benchmark harness](../../../../scripts/benchmark_transport.py)
starts a fresh release daemon for each condition. It uses actual Unix sockets
and loopback TCP, retains every measured sample, and checks message delivery,
receipt IDs, sequence numbers, payloads, routing identities, acknowledgment
cursors, and the corresponding durable inbox records.

## Reproduce

Run from the repository root, with permission to bind local TCP and Unix
sockets. Keep other benchmark and build processes idle during measurement.

```sh
cargo build --offline --locked --release
python3 scripts/benchmark_transport.py \
  --binary target/release/coop \
  --output docs/impl/acceptance-20261020/transport-scale/run-01
```

The output directory must not exist. The script changes no global client
configuration and removes only the isolated temporary bus roots it creates,
after copying their regular files into the evidence directory. Registration
and MCP credentials remain in memory; the retained files are scanned for every
generated credential's plaintext before the manifest is written.

## Workload and interpretation

All messages have the same 128-byte `text/plain` payload, and every agent
sends to its own inbox. Each latency cycle sends one message, waits for the
durable acknowledgment, issues `wait_message(timeout_s=0)`, verifies the
returned message, and acknowledges the returned cursor. UDS uses persistent
NDJSON JSON-RPC connections. HTTP uses an initialized streamable HTTP MCP
session over a persistent HTTP/1.1 connection. Session initialization and
enrollment are outside the measurement window.

UDS and HTTP each collect 500 measured cycles after 20 warmups. Reported send
latency includes the durable append and the transport round trip. Wait latency
starts after the send acknowledgment; it measures retrieval of an already
durable message and includes transport and client parsing. It does not
measure the wakeup of a waiter that was already blocked before the send.
Percentiles use the nearest-rank rule and exclude warmups.

The standalone UDS append sample sends serially for at least 10 seconds. If
5,000 messages accumulate, it drains and verifies them before continuing;
this maintenance time is included in the measured wall rate. A final drain
and verification happens after the append window. Default daemon limits and
per-record fsync behavior apply.

Scale uses the same UDS send/wait/ack cycle in three conditions, each with a
fresh daemon and three warmups per active agent:

| Condition | Enrolled agents | Active agents | Measured cycles |
|---|---:|---:|---:|
| Baseline | 1 | 1 | 1,000 |
| Idle population | 100 | 1 | 1,000 |
| Concurrent population | 100 | 100 | 50 per agent; 5,000 total |

In the idle population condition, the other 99 agents have outstanding
600-second empty-inbox long polls. The harness checks that those connections
have no response available before and after the measured workload. In the
concurrent population condition, all active clients finish warmup before a
barrier releases the measured workload. Each client has at most one call in
flight, so client and server queueing contribute to the observed latency.
The Python client, its GIL, parsing, and thread scheduling are included.

The design specifies p50 send acknowledgment below 10 ms, p50 wait delivery
below 25 ms, and sustained append of at least 5,000 messages/sec. These are
reported independently for each applicable condition. The phrase “100 agents
without degradation” has no numerical definition in the design. Before the
run, this harness declares diagnostic relative thresholds: p50 and p95 send
and wait latencies at most 1.25 times the single-agent baseline, and aggregate
cycle throughput at least 0.8 times that baseline. These engineering choices
are diagnostics, not additional specification requirements. Failure of those
diagnostics must not be represented as proof that the original design set a
specific relative threshold; an absolute latency target miss remains a miss.

This is one sample on one host and filesystem, with a fixed payload and a
closed-loop workload. It does not establish behavior under other workloads,
hardware, or an externally paced arrival rate.

## Evidence layout

- `run-01/plan.json`: workload and thresholds written before measurement.
- `run-01/environment.json`: host, filesystem, source and script hashes,
  commit identity, dirty tree status, and binary identity.
- `run-01/summary.json`: all results and scale comparisons.
- `run-01/<condition>/samples.jsonl`: raw per-message samples, including
  explicitly marked warmups.
- `run-01/<condition>/daemon.json`: isolated process identity and correctness
  checks; the executable is hashed through `/proc/<pid>/exe`.
- `run-01/<condition>/raw-root/`: actual regular bus files after termination.
- `run-01/credential-check.json` and `run-01/sha256.json`: credential scan and
  retained-artifact manifest.

## Results

The complete run finished successfully from 09:09:37 to 09:10:25 UTC on
2026-10-20. All 11,783 successful send receipts, including 366 warmups,
matched the durable inbox records and verified deliveries and acknowledgments.
All per-inbox sequences were contiguous. The 99 idle long polls remained
outstanding before and after the idle-population measurement. The credential
scan passed for all retained regular files.

The release executable SHA-256 was
`c6d1047a8a4d9b0094274151c9658a101ed018ce07f82d66ed9fb512eb285321`,
built with default features from commit
`63e20514fffa039a97b0ec25e432657854d2fdef`. The actual running executable
matched that hash in all six phases. Binary and production source hashes
remained equal before and after the run. The worktree contained only the
new acceptance evidence and harness files recorded in `environment.json`.

The host was Linux 6.18.54.1-microsoft-standard-WSL2, x86_64, with a
12th Gen Intel Core i5-1240P and 12 logical CPUs. Bus roots were under `/tmp`
on `/dev/sdd`, ext4 (`data=ordered`). The client used Python 3.12.3. This
differs from the Ryzen 9 host recorded in the older performance smoke.

### Actual transport latency

All values below are milliseconds; each row has 500 measured samples.

| Transport and operation | p50 | p95 | p99 | Design p50 target |
|---|---:|---:|---:|---|
| UDS send acknowledgment | 2.680 | 5.007 | 7.428 | PASS, < 10 ms |
| UDS wait after send | 0.255 | 0.530 | 0.652 | PASS, < 25 ms |
| HTTP MCP send acknowledgment | 3.378 | 5.637 | 7.738 | PASS, < 10 ms |
| HTTP MCP wait after send | 0.788 | 1.538 | 2.052 | PASS, < 25 ms |

The single-client transport cases meet both p50 targets. These wait numbers
include the real transport round trip and retrieval of an already durable
message, as defined above.

### Sustained append

The serial UDS append window durably acknowledged **3,417 messages in
10.002095 seconds**, or **341.628 messages/sec**. All 3,417 messages were
subsequently delivered and acknowledged. The 5,000 messages/sec target is
**missed by approximately 14.6 times**. Its send acknowledgment p50/p95/p99
was 2.630/4.707/6.882 ms. No mid-window draining was necessary because the
sample remained below 5,000 pending messages.

### Agent scale

| UDS condition | Send p50/p95/p99 (ms) | Wait p50/p95/p99 (ms) | Aggregate cycles/sec | Absolute p50 targets |
|---|---|---|---:|---|
| 1 active / 1 enrolled | 2.896 / 5.076 / 6.510 | 0.302 / 0.620 / 0.802 | 268.471 | Both PASS |
| 1 active / 100 enrolled | 3.366 / 5.574 / 8.230 | 0.338 / 0.705 / 0.909 | 237.915 | Both PASS |
| 100 active / 100 enrolled | 398.864 / 529.727 / 561.372 | 4.113 / 7.255 / 9.837 | 243.170 | Send MISS; wait PASS |

With 99 additional agents blocked in empty-inbox waits, send p50 increased
16.2% and aggregate cycle throughput was 88.6% of baseline. All predeclared
relative diagnostic thresholds passed for this idle population.

With 100 clients actively sending, send p50 was 137.7 times baseline and
send p95 was 104.4 times baseline. Aggregate cycle throughput was 90.6% of
baseline, while wait p50 was 13.6 times baseline. The relative diagnostics
failed, and the documented absolute send p50 target failed. The measured
100-agent workload therefore does not establish “100 agents without
degradation.”

Similar aggregate throughput alongside much longer per-client latency is
consistent with queueing under the higher offered concurrency. Production
code serializes sends through `send_lock` and fsyncs each inbox append;
this baseline does not separately attribute time among that append path,
server scheduling, and the included Python client overhead.

Raw summaries, samples, and correctness results are in
[run-01/summary.json](run-01/summary.json). The measured gaps remain append
throughput and latency under 100 active clients. No production behavior was
changed during this measurement.
