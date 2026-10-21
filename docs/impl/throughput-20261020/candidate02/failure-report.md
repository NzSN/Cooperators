# Candidate 02: retained throughput failure

Candidate 02 improved the first three same-inbox conditions and passed their
message integrity checks, but again reached the default 10,000-message pending
limit at C=100. The original qualification run stopped there. The remaining
four distributed-inbox profiles were subsequently measured as an explicitly
partial continuation, followed by the unchanged original controls. The original
eight-condition gate remains **FAILED**; those later runs do not replace the
failed same-inbox C=100 row. See the [exact eight-row comparison](comparison.json).

| Same-inbox senders | Baseline acks/s | Candidate 02 acks/s | Candidate p50/p95/p99 ms | 5,000/s | p50 <10 ms |
|---:|---:|---:|---|---|---|
| 1 | 266.9 | 465.5 | 2.023 / 2.844 / 3.911 | MISS | PASS |
| 8 | 210.4 | 1,951.6 | 3.866 / 5.828 / 8.529 | MISS | PASS |
| 32 | 195.0 | 3,108.1 | 9.635 / 16.966 / 21.600 | MISS | PASS |
| 100 | 160.2 | Run failed at inbox capacity | Unavailable | Run failed | Unavailable |

The executable SHA-256 was
`32d5b650e8768407d297165eb350c295ab0b7aa0da14272fcee3948765383488`.
All [qualification source/test hashes](../qualification/run-02/source-manifest.json)
matched before measurement, and source, binary and frozen harness hashes stayed
stable during the run. See [preflight](resources-before.json),
[run summary](throughput/summary.json), and
[failed-phase process record](throughput/same-inbox-c100/daemon.json).
The failed phase had fully drained 4,334 warmups before measurement; 18,154
records were retained on disk when its process exited. Volatile failed-phase
receipt and drain counters were not saved by the frozen qualification harness.

A separate [diagnostic reproduction](diagnostic-c100-run01/summary.json) used
the same workload functions, payload, warmup, window, receiver cadence and caps,
with in-memory RPC tracing and 500 ms process CPU samples. Instrumentation makes
these diagnostic numbers unsuitable as replacement qualification performance.

The first capacity error occurred 2.488 seconds into measurement after 4,898
warmups had fully drained. At first error the passive observer had received
17,514 successful send receipts, with highest returned sequence 17,564, and
returned and acknowledged 7,579 messages. It had completed 76 ack calls including
warmup. During measurement before the error, 27 receiver waits consumed 1.861
seconds (p50/p95 65.473/103.764 ms), and 27 acks consumed 0.551 seconds
(18.885/49.379 ms). The 53 gaps between receiver RPCs totaled only 2.807 ms.

Four CPU sampling intervals spanning 2.287 seconds before the error measured:

| Process | User CPU | System CPU | Total CPU |
|---|---:|---:|---:|
| Python driver | 50.7% | 101.4% | 152.2% |
| Broker daemon | 41.5% | 31.9% | 73.5% |

CPU percentages use one logical CPU as 100%. Sampling boundaries may overlap
the start of measurement. The process resource sampler successfully retained
CPU stat while recording `/proc/PID/io` as unavailable. High driver system CPU
and long receiver calls are consistent with socket, framing or thread
scheduling overhead. CPU percentages do not prove Python GIL contention or
exclude server scheduling or disk wait. No such attribution is claimed.

The receiver never waits for sender receipt bookkeeping. It verifies returned
envelopes independently and immediately acks each batch. The frozen 20 ms sleep
applies after the inbox becomes empty and does not pause a backlogged drain.
The production read-view split therefore improved measured performance but did
not by itself remove this workload's receiver capacity problem.

See [timing/CPU analysis](diagnostic-c100-analysis.json),
[RPC diagnostics](diagnostic-c100-run01/rpc-diagnostics.json),
[RPC timeline](diagnostic-c100-run01/rpc-timeline.jsonl), and
[raw process samples](diagnostic-c100-run01/process-resources.json).
All failed qualification and diagnostic evidence is retained; no caps or
workload settings were changed.

## Later partial continuation and original controls

The [continuation declaration](distributed-continuation-plan.json) was written
before its measurement. The [separate wrapper](continue_distributed.py) called
the frozen phase function for distributed inboxes at C=1/8/32/100 using the
original 8 KiB readers and every original workload/default setting. Its plan
explicitly states that completing those phases cannot reverse the original
failed gate. All four phases passed receipt, delivery, ack, sequence, disk and
hash-chain checks.

| Distributed senders | Baseline acks/s | Candidate 02 acks/s | Candidate p50/p95/p99 ms | 5,000/s | p50 <10 ms |
|---:|---:|---:|---|---|---|
| 1 | 206.2 | 469.1 | 2.007 / 2.816 / 3.641 | MISS | PASS |
| 8 | 191.9 | 1,887.2 | 3.485 / 8.274 / 12.782 | MISS | PASS |
| 32 | 174.3 | 1,291.6 | 21.129 / 51.532 / 69.274 | MISS | MISS |
| 100 | 100.5 | 1,339.7 | 58.615 / 188.175 / 258.587 | MISS | MISS |

The seven successfully completed throughput conditions checked 117,533 messages
including warmup. The unchanged original transport/control harness checked
another 13,079 messages including warmup. See
[continuation results](throughput-distributed/summary.json) and
[control results](controls/summary.json). All artifact hashes for the baseline,
candidate, continuation and controls were verified when generating the exact
comparison; no diagnostic values substitute for qualification metrics.

Unloaded UDS/HTTP send p50 was 2.061/2.541 ms, with wait-after-send p50
0.151/0.469 ms. Both unloaded latency targets passed. Serial append measured
471.2 messages/sec and missed 5,000/sec. In the unchanged 100-active-agent
send/wait/ack control, send p50 was 27.016 ms and wait-after-send p50 25.981 ms;
both respective 10/25 ms targets were missed. The one-active/100-idle condition
passed the original relative diagnostics; 100-active failed them. These Python
driver costs remain included, and one sample per condition supplies no
confidence interval or proof of how much improvement comes from code versus
elapsed host conditions.
