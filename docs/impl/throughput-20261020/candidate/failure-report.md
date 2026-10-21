# Candidate 01: retained throughput failure

Candidate 01 did not complete the predeclared eight-condition throughput run.
The first three same-inbox conditions passed their message integrity checks,
but all missed 5,000 durable acknowledgments per second. Same-inbox C=100 then
reached the unchanged 10,000-message pending inbox limit despite the independent
receiver drain. The four distributed-inbox conditions and original transport
controls were not run for this executable. These results establish neither
completion of step 3 nor passing behavior for the unmeasured conditions.

| Same-inbox senders | Baseline acks/s | Candidate 01 acks/s | Candidate p50/p95/p99 ms | 5,000/s | p50 <10 ms |
|---:|---:|---:|---|---|---|
| 1 | 266.9 | 322.0 | 2.852 / 4.589 / 5.329 | MISS | PASS |
| 8 | 210.4 | 969.2 | 7.970 / 11.896 / 14.481 | MISS | PASS |
| 32 | 195.0 | 2,109.8 | 14.228 / 25.589 / 32.506 | MISS | MISS |
| 100 | 160.2 | Run failed at inbox capacity | Unavailable | Run failed | Unavailable |

See the immutable [run summary](throughput/summary.json),
[failed-phase process record](throughput/same-inbox-c100/daemon.json), and
[offline failure analysis](failure-analysis.json). The executable SHA-256 was
`0fe56619a6fa7366322bdedadc4b2d718d6429f7081244237185ad8a7bae7029`.
The throughput harness and imported transport helper retained their predeclared
hashes. No inbox caps or workload settings were changed during measurement.

The failed C=100 phase had fully delivered and acknowledged 2,838 warmup
messages before measurement. Its retained inbox has 25,752 complete records,
including 22,914 after warmup; offline verification found contiguous sequences,
valid hash-chain links and matching payloads. Record creation timestamps span
09:58:51.246–09:59:02.323 UTC; the first measured record was created at
09:58:52.335 UTC. Those timestamps describe envelope creation, not durable
receipt or acknowledgment completion. The failed qualification path did not
persist its volatile receipt samples or final drain counters, so exact
failure-time receipt/delivery/ack counts cannot be recovered from that run.

## Separate diagnostic reproduction

A [diagnostic wrapper](diagnose_c100-run01.py) reproduced the capacity error
using the same 128-byte payload, C=100, at least one second and three sends per
sender of warmup, ten-second measurement, 20 ms empty-inbox polling interval,
and unchanged server caps. It calls the frozen workload functions and adds
in-memory per-RPC tracing. Its instrumentation overhead means its numbers are
diagnostic evidence, not replacement qualification performance.

The [RPC timeline](diagnostic-c100-run-01/rpc-timeline.jsonl) and
[diagnostic summary](diagnostic-c100-run-01/rpc-diagnostics.json) retain successful
send receipts plus every receiver wait and ack start/end, batch size and cursor.
After 2,995 warmups had fully drained, the first capacity error arrived 6.955
seconds into measurement. At that instant, the passive client observer had
received 16,861 successful send receipts (highest returned sequence 16,925),
returned and acknowledged 6,938 messages, and completed 71 ack calls including
warmup. It was inside another wait that had already lasted approximately 232 ms.

Within measurement before that error, 40 completed wait calls consumed 5.103
seconds in total, with p50/p95 118.228/229.376 ms; 40 completed ack calls consumed
1.587 seconds, with p50/p95 33.038/83.772 ms. The 79 gaps between consecutive
receiver RPCs totaled only 10.645 ms, with p95 0.473 ms. The receiver was
therefore spending nearly all that interval inside wait/ack RPCs. It was not
waiting for sender receipt bookkeeping: it independently verifies returned
envelopes and immediately acknowledges them, joining receipt identities only
after successful send windows. The 20 ms interval applies after reaching an
empty inbox and does not pause a receiver with backlog.

By diagnostic process completion, 17,693 successful send receipts and 99 failed
send calls were retained; 17,051 messages had been returned and acknowledged.
A separate [receipt/disk check](diagnostic-receipt-disk-check.json) confirmed
that all 17,693 successful receipt IDs and sequences exactly match the persisted
records. See [timing analysis](diagnostic-c100-analysis.json) for the calculation.

These timings support investigating scheduling or contention on the server's
read/ack path. They do not isolate CPU, Python GIL, disk I/O, mutex wait, or
transport serialization time. `/proc/PID/io` is unavailable in this execution
environment. The first diagnostic sampler grouped stat and I/O reads, so that
missing file also discarded the available CPU readings; no CPU attribution is
supported by this run. The updated [diagnostic wrapper](diagnose_c100.py) keeps
CPU readings independently and records unavailable I/O explicitly. It has not
been rerun at this checkpoint.

The failed run, diagnostic artifacts, and first wrapper source are retained.
Any production iteration must use a new evidence directory and rerun the same
frozen workloads; target misses and this failure remain part of the record.
