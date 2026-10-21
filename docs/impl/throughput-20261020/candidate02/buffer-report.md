# UDS reader-buffer diagnostic

The [predeclared experiment](buffer-plan.json) changed only fresh Python UDS
reader buffering from 8 KiB to 256 KiB. Frozen benchmark scripts, payload,
topology, one outstanding send per sender, C=100, warmup, ten-second window,
receiver batches/cadence and daemon caps remained unchanged. The
[separate wrapper](diagnose_buffer.py) reused the same RPC/CPU instrumentation.
These runs are diagnostic and do not replace qualification.

Candidate 02 still reached the 10,000-message pending limit. The larger buffer
improved receiver wait latency but did not resolve its production/consumption
imbalance. The original baseline completed with no rejected calls; its append
throughput and sender p50 still missed their targets.

| Condition | C100 result | Receiver wait p50/p95 ms | Ack p50/p95 ms | Driver CPU | Daemon CPU |
|---|---|---|---|---:|---:|
| Candidate 02, default 8 KiB | Cap error at 2.488 s | 65.473 / 103.764 | 18.885 / 49.379 | 152.2% | 73.5% |
| Candidate 02, 256 KiB | Cap error at 3.584 s | 16.695 / 43.080 | 13.345 / 37.015 | 155.5% | 71.6% |
| Original baseline, 256 KiB | Complete: 570.7 acks/s | 1.607 / 2.561 | 1.591 / 2.438 | 18.7% | 22.0% |

Receiver latency and CPU for failed runs use the measured interval before the
first error; the baseline uses the completed measurement. CPU is percent of one
logical CPU. Sampling intervals can overlap the beginning of measurement.

At the buffered candidate's first error, 25,962 successful receipts had been
observed, with 16,014 messages acknowledged, including 5,535 warmups. It later
retained 37,452 successful receipts, 100 rejected sends and 27,452 acknowledged
messages. The failed harness shuts down its receiver when handling the error;
this is not evidence that the remaining durable records were lost. See
[candidate artifacts](buffer-candidate-run01/summary.json).

The buffered baseline durably acknowledged 5,810 measured messages in 10.180
seconds. Send acknowledgment p50/p95/p99 was 171.277/207.359/226.081 ms. All
6,519 messages including warmup passed receipt, delivery, acknowledgment,
sequence, hash-chain and disk checks. See
[baseline artifacts](buffer-baseline-run01/summary.json).

The improvement is consistent with reader framing overhead under the loaded
Python driver. High driver system CPU and the remaining long receiver calls
also justify isolating socket and thread scheduling overhead. This single
sequential experiment does not prove a specific GIL mechanism or exclude
server scheduling and host variation. No valid full-window candidate throughput
is reported after its cap failure. The exact calculations are retained in
[buffer-analysis.json](buffer-analysis.json).
