# Durable append comparison method

This method and [plan.json](plan.json) were written before the paired baseline
and candidate measurement. The existing
[benchmark_transport.py](../../../scripts/benchmark_transport.py) remains
byte for byte unchanged. Its SHA-256 is
`685580eadb8b5b3f4a827ca750ea969bbba65df9d30dd519babcf62b475bf18c`.
The new [benchmark_throughput.py](../../../scripts/benchmark_throughput.py)
imports its persistent UDS JSON-RPC client, isolated daemon lifecycle, and
artifact helpers. Neither harness needs external Python packages or changes
global configuration.

## Fixed workloads

Run the following eight conditions separately on fresh daemons and private
bus roots, first with the preserved baseline executable and then with the
candidate release executable. Every send has a 128-byte `text/plain` body.

| Topology | Concurrent senders | Enrolled identities | Client send concurrency |
|---|---|---|---|
| Same inbox | 1, 8, 32, 100 | C senders plus one receiver | One outstanding send per sender |
| Distributed inboxes | 1, 8, 32, 100 | C; each sender sends to its own inbox | One outstanding send per sender |

Each destination inbox has its own independent read/ack connection. In the
distributed case this connection authenticates as the same enrolled identity
as its sender; the 100-sender condition therefore has exactly 100 agents.
Read/ack connections run concurrently with senders. Every 20 ms a receiver
repeatedly calls `wait_message(timeout_s=0)`, verifies each returned envelope,
and acknowledges each returned cursor until its inbox is empty. The unchanged
daemon default limits batches to 100 records. All receiver work competes with
sends during measurement, and all inbox capacity defaults remain enabled.

A barrier releases all senders for a warmup lasting at least one second and
at least three sends per sender. Warmup deliveries are fully verified and
acknowledged before the measurement barrier. The measurement lasts ten seconds;
senders issue no further requests once that deadline has passed. A send already
in progress may complete after the deadline. The throughput numerator counts
every durable receipt for requests issued in the measurement window, and the
denominator extends from barrier release through the last such receipt.
Thus final in-flight completions extend both count and elapsed wall time.
Final delivery catch-up happens outside that window and is checked separately.

No sender pipelines requests. The single-sender conditions preserve a serial
send workload with no overlapping appends to batch. They must be reported even
if concurrent batching improves the other conditions. The Python GIL, JSON
parsing, socket round trips, thread scheduling, and concurrent drain/ack work
are included in the observed numbers. This is a closed-loop load on one host,
not a claim about all payloads or externally paced request arrivals.

Report durable acknowledgments per second and nearest-rank send acknowledgment
p50, p95 and p99 for every condition. Compare each result independently against
5,000 durable acknowledgments per second. Compare p50 to the existing 10 ms
threshold with the exact workload attached. In particular, a 100-client loaded
result does not redefine the previously unspecified phrase “without
degradation.” Do not replace a failing serial or distributed condition with
the best same-inbox result.

## Identity and correctness evidence

The baseline is the preserved release binary with SHA-256
`c6d1047a8a4d9b0094274151c9658a101ed018ce07f82d66ed9fb512eb285321`.
Its source identity is pinned to the original
[source manifest](../acceptance-20261020/installed/source-manifest.json), built
from commit `63e20514fffa039a97b0ec25e432657854d2fdef`. Worktree changes made
during implementation are recorded as observations and are not attributed to
that baseline executable. Candidate evidence records the source hashes seen
at measurement, which must remain stable during the candidate run.

Each actual running executable is checked through `/proc/PID/exe`. The binary,
new harness, and unchanged imported helper must retain their hashes during
the run. Each phase retains every warmup and measured receipt, raw bus files,
daemon logs, and machine-readable summaries. Verification checks:

- Every receipt is unique, non-duplicate, and addressed to the expected inbox.
- Every delivered ID and sequence matches its receipt, sender, receiver, kind,
  MIME and every payload byte.
- Delivery sequences and ack cursors are contiguous; all destination inboxes
  are empty at completion and their acknowledged cursors equal verified heads.
- Every receipt, including warmup, corresponds to exactly one persisted inbox
  record. Every per-inbox sequence and SHA-256 chain link is independently
  checked, with no extra or missing records.
- All retained artifact bytes are scanned for every generated credential's
  plaintext before an artifact hash manifest is written.

Reading files checks their content and integrity. Separate crash and I/O fault
checks are required to assess whether success acknowledgments obey durability
requirements under failure.

## Reproduction

Run each command while other builds, checks and benchmarks are idle. Output
directories must not already exist. Local socket binding needs sandbox access.

```sh
python3 scripts/benchmark_throughput.py \
  --binary /tmp/cooperators-step3-baseline/coop \
  --source-manifest docs/impl/acceptance-20261020/installed/source-manifest.json \
  --label baseline \
  --output docs/impl/throughput-20261020/baseline/throughput

python3 scripts/benchmark_throughput.py \
  --binary target/release/coop \
  --label candidate \
  --output docs/impl/throughput-20261020/candidate/throughput
```

Keep the original serial, unloaded UDS/HTTP latency, one-active/100-idle and
100-active send/wait/ack controls with the unchanged harness and default
arguments. Original baseline controls are already retained in
[the earlier run](../acceptance-20261020/transport-scale/run-01/summary.json).
Fresh controls can be paired under `baseline/controls` and `candidate/controls`:

```sh
python3 scripts/benchmark_transport.py \
  --binary /tmp/cooperators-step3-baseline/coop \
  --output docs/impl/throughput-20261020/baseline/controls

python3 scripts/benchmark_transport.py \
  --binary target/release/coop \
  --output docs/impl/throughput-20261020/candidate/controls
```

The eight fixed throughput profiles are the primary before/after comparison.
One run per condition provides a host-specific sample and no confidence
interval. Repeat only when a failed check, source change, or identified
measurement problem requires it; retain failures and explain any replacement.
