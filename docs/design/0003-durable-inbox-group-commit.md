# Durable inbox group commit

Date: 2026-10-20. Status: implemented; core correctness qualified; performance
acceptance partial. [Final measurements and limits](../impl/throughput-20261020/README.md).

## Objective and measured baseline

Increase durable direct-message throughput and reduce serialization between
independent inboxes while preserving acknowledged-message recovery, idempotent
retries, exact inbox caps, TTLs, and instance isolation. The existing acceptance
targets remain 5,000 durable messages/second, send acknowledgment p50 below
10 ms, delivery p50 below 25 ms, and 100 agents without degradation.

The [October 20 baseline](../impl/acceptance-20261020/README.md) measured 341.628
messages/second for serial UDS sends and 398.864 ms send p50 for 100 active
self-send/wait/ack clients. The unchanged original control remains useful.
Group commit requires overlapping requests, so additional same-inbox and
distributed-inbox throughput profiles have their concurrency, payload, drain
workload, and measurement duration recorded before candidate measurements in
[the throughput method](../impl/throughput-20261020/benchmark-method.md).
Serial and concurrent results are reported separately.

## Write and publication sequence

The direct-message interface stays `send_message → durable receipt`.
`InboxStore` owns inbox handles, global dedupe reservations, bounded write
queues, and finite blocking workers. Each busy inbox has at most one worker.
Different inbox workers may write and sync concurrently. Queue/document/channel
mutations retain their existing shared ordering lock.

1. Resolve the authenticated sender and recipient, ensure the shared inbox
   handle exists, and consult dedupe before capacity rejection.
2. Reserve a new sender/key or join the existing pending completion. Reserve
   and queue submission contain no await that could strand a canceled caller.
3. Queue the envelope subject to a bounded request count and byte estimate.
   The first queued request starts a worker on the blocking pool.
4. Collect a bounded batch until the oldest request's collection deadline, the
   record limit, or the byte limit. Old queued requests incur no extra dwell.
5. Admit requests in order under the individual committed-state lock. Exact serialized
   sizes and committed backlog plus earlier admitted records determine inbox
   capacity. Waiting requests have not yet been admitted and can receive
   `RESOURCE_EXHAUSTED` when processed.
6. Release committed state while the worker owns its file lock. Commit TTL
   companion records first, then append the ordered inbox batch and perform
   its covering sync. Readers can consume and acknowledge older committed data.
7. Reacquire committed state, add the new records to its current backlog
   counters, and publish records and heads. Concurrent acknowledgments may
   have reduced those counters during I/O. Release state before notifying
   recipients and completing dedupe/request receipts.

One file sync covers a batch in that file. Independent inbox files each receive
their own covering sync. The JSONL format, root layout, and per-log sequence/hash
rules stay the same. This amends the original per-message sync mechanism while
retaining its `append → fsync → success` durability requirement.

## Bounds and lifecycle

| Configuration | Default | Meaning |
|---|---:|---|
| `write_batch_messages` | 64 | Maximum records in an active batch |
| `write_batch_bytes` | 256 KiB | Batch collection byte bound; one larger admitted message forms a singleton |
| `write_batch_delay` | 250 microseconds | Maximum intentional delay from the oldest queued request; scheduling and disk time are additional |
| `write_queue_messages` | 1,024 | Maximum waiting requests for one inbox, excluding its active batch |
| `write_queue_bytes` | 8 MiB | Maximum serialized envelope size estimate waiting in one inbox queue |

The queue byte estimate uses the maximum sequence-number width. The separate
inbox cap is checked exactly at admission; it includes durable unacknowledged
records and all earlier accepted records in the active batch. The active batch
is bounded by record count and the larger of its byte limit or one admitted
singleton. The queue plus active batch therefore have explicit finite limits.
There is no promise to accept a payload larger than the queue's byte capacity.

Workers own completions independently of the calling future. A finite worker
retains the store and shared root lease until it has committed or failed its
queue. It exits when the queue empties and has no persistent sender/task cycle.
Blocking pool limits bound actual blocking threads. Reads, acknowledgments,
and backlog snapshots acquire only committed state, which the writer does not
hold during disk I/O. File locks are private to the single inbox worker.

## Failures and committed state

Pending idempotency keys are distinct from committed receipts. Followers of a
pending request receive the same result after its commit. Known pre-write
rejections release reservations. An uncertain commit failure poisons the inbox
and retains failed key reservations until actual reconstruction, including
when a retry names another recipient. Existing committed dedupe receipts remain
available. No append continues through a poisoned writer.

An unacknowledged prefix from a partially written batch may survive. Reopen
verifies and syncs that prefix before it can satisfy dedupe or become visible.
Previous acknowledged records are retained. TTL records committed without a
corresponding inbox record remain harmless orphan metadata, as before.

Readers and acknowledgments use committed inbox records and heads. Status and
metrics override raw filesystem tails with the loaded inboxes' committed
snapshots, including companion expiry heads. A raw complete line awaiting sync
is not published as a committed head.

## Qualification

- Storage batch tests cover order/hash/receipts, missing newline, pre-write
  serialization failure, partial second records, sync failure, and recovery.
- Messaging tests cover concurrent same-inbox sends, different-inbox progress,
  shared keys across recipients, exact capacity, queue saturation, canceled
  callers, root ownership, and TTL reconstruction.
- Three actual process SIGKILL controls cover before batch sync, after durable
  receipts and after partial batch failure. Reopen preserves acknowledged
  prefixes, exact receipts, order/hash chains and recovered dedupe. These are
  process-crash controls through the broker API, not hardware power-loss tests.
- Normal and injection suites, actual daemon/UDS acceptance, and the generated
  replay gate passed against the changed implementation.
- Compared the preserved baseline executable and candidate on identical serial,
  same-inbox, distributed-inbox, transport, and 100-agent workloads. Retain
  samples, correctness checks, source/binary identities, and any target misses.

The separate modern MCP tool-discovery correction and million-record startup
optimization are outside this write-path implementation.

The matched native shared-inbox cases reached 6,030.3 durable messages/second
at C=32 with send p50 5.022 ms, and 12,834.9/second at C=100 with p50 7.469 ms.
The eight-profile target gate remains failed: serial and distributed cases
miss throughput, distributed C=100 also misses latency, and the preserved
Python C=100 shared-inbox case fails at its unchanged backlog cap. The original
100-active send/wait/ack control still misses its latency thresholds. All
16 native baseline/candidate cases passed receipt/delivery/ack/disk integrity.

## First candidate and read isolation

The first qualified implementation held the committed-state lock through
sync. Its 100-sender same-inbox workload filled the 10,000-message cap while
the receiver fell behind. A retained diagnostic spent 6.69 of the 6.95 seconds
before its first rejection inside 40 wait/ack pairs, with only 0.011 seconds
between those calls. That observation motivated separating file ownership
from the committed view; it does not independently attribute time among file
sync, server scheduling, and client decoding. The
[first candidate and diagnostic](../impl/throughput-20261020/candidate/failure-report.md)
remain part of the evidence. The repeated workload uses the same caps and
client behavior.
