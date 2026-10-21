# Durable inbox group commit — 2026-10-20

**Implementation and core correctness qualification are complete. Performance
acceptance is partial.** The native shared-inbox workload meets both the
5,000 durable messages/second and send p50 <10 ms targets at 32 and 100 senders.
The full eight-profile performance gate remains **FAILED**: serial and
distributed workloads retain target misses, and the original Python
100-sender shared-inbox run fails at the unchanged backlog cap.

## Implementation

- [InboxStore](../../../src/inbox_writer.rs) owns bounded per-inbox queues,
  pending/committed idempotency reservations, and finite blocking workers.
- [Storage batches](../../../src/storage.rs) preserve sequence/hash order and
  return receipts only after a successful covering file sync. Each independent
  inbox still receives its own sync.
- [Messaging](../../../src/messaging.rs) exposes committed records to readers
  independently of file I/O. Publication, notifications and successful
  responses follow commit. Concurrent acknowledgments reduce the current
  committed backlog while a new batch syncs.
- Uncertain writes quarantine that inbox until reconstruction. Known queued
  requests that never reached I/O release their keys; canceled callers cannot
  abandon admitted writes or release root ownership early.

Default active batches are bounded by 64 records / 256 KiB, with at most
250 microseconds intentional collection delay. Waiting queues are bounded by
1,024 requests / 8 MiB per inbox. Exact inbox admission includes the active
batch; existing 10,000-message / 64 MiB backlog caps remain enabled.
See [design 0003](../../design/0003-durable-inbox-group-commit.md) and
[operator guidance](../../operator.md#daemonconfig-tunables).

## Matched native comparison

The [native method](native/method.md) was frozen before its paired runs. Both
binaries use the same Rust asynchronous UDS driver, HTTP off, 128-byte bodies,
one outstanding send per sender, one-second fully drained warmup, and a
10-second measurement extended through the final in-flight receipt. Receivers
drain and acknowledge concurrently with default batch sizes and caps.
The shared-inbox C=100 case has 100 senders plus a receiver identity; the
distributed C=100 case has exactly 100 enrolled identities sending to themselves.

| Inbox topology | Senders | Baseline msg/s | Candidate msg/s | Baseline send p50 ms | Candidate send p50 ms | ≥5,000/s | <10 ms |
|---|---:|---:|---:|---:|---:|---|---|
| Shared | 1 | 640.3 | 492.8 | 1.430 | 1.914 | MISS | PASS |
| Shared | 8 | 574.1 | 2,043.9 | 13.488 | 3.720 | MISS | PASS |
| Shared | 32 | 581.7 | 6,030.3 | 53.781 | 5.022 | PASS | PASS |
| Shared | 100 | 531.6 | 12,834.9 | 183.400 | 7.469 | PASS | PASS |
| Distributed | 1 | 522.9 | 456.4 | 1.682 | 2.048 | MISS | PASS |
| Distributed | 8 | 520.6 | 1,712.4 | 14.502 | 4.145 | MISS | PASS |
| Distributed | 32 | 469.2 | 3,712.3 | 65.268 | 7.698 | MISS | PASS |
| Distributed | 100 | 455.8 | 4,608.1 | 218.502 | 21.407 | MISS | MISS |

Shared-inbox throughput improves 10.37× at C=32 and 24.14× at C=100. The
single-sender shared-inbox sample is 23.0% lower; a single sender offers no
overlap for batching. One sample does not isolate a repeatable regression's
size or cause. Distributed C=100 improves
substantially but misses both thresholds. These observations do not establish
the broad requirement “100 agents without degradation.”

All 16 native baseline/candidate cases completed without errors. Every receipt
matched its delivered and persisted envelope, sequence/hash chain, cursor, and
final empty inbox. Executable/source/driver identities stayed stable. Those
correctness results are separate from `full_qualification_pass=false`.
See [baseline](native/baseline-run01/summary.json),
[candidate](native/candidate-run01/summary.json), and the
[native comparison](native/report.md) for quantiles, counts and audit details.

These are one-host WSL2/ext4 observations on an i5-1240P with 12 logical CPUs,
one window per condition and no confidence intervals. Closed loops do not
establish a maximum independently paced arrival rate. File readback alone is
not evidence of power-loss durability.

## Preserved Python comparison

The original [Python method](benchmark-method.md) and harness remain unchanged.
Its matched baseline/candidate results are reported independently from the
native control, whose client runtime and HTTP setting differ.

| Inbox topology | Senders | Baseline msg/s | Candidate msg/s | Candidate send p50 ms | Outcome |
|---|---:|---:|---:|---:|---|
| Shared | 1 | 266.9 | 465.5 | 2.023 | Throughput MISS |
| Shared | 8 | 210.4 | 1,951.6 | 3.866 | Throughput MISS |
| Shared | 32 | 195.0 | 3,108.1 | 9.635 | Throughput MISS |
| Shared | 100 | 160.2 | — | — | FAILED: 10,000-message cap |
| Distributed | 1 | 206.2 | 469.1 | 2.007 | Throughput MISS |
| Distributed | 8 | 191.9 | 1,887.2 | 3.485 | Throughput MISS |
| Distributed | 32 | 174.3 | 1,291.6 | 21.129 | Both targets MISS |
| Distributed | 100 | 100.5 | 1,339.7 | 58.615 | Both targets MISS |

The four distributed candidate cases ran as an explicitly recorded continuation
after the original eight-profile run failed. They do not turn that original
run into a pass. See [comparison.json](candidate02/comparison.json) and
[failure report](candidate02/failure-report.md).

The first implementation held committed state during sync. Its retained
[failure and timing diagnostic](candidate/failure-report.md) motivated separating
the read view from file ownership. Candidate 02 still reaches the cap with
the frozen Python reader. A separate
[single-variable buffer experiment](candidate02/buffer-report.md) improves
wait/ack timing but also reaches the cap. Native success establishes that the
implementation can exceed 5,000/s with that separate driver; it neither erases
the Python failures nor proves a particular GIL, scheduling or storage cause.

## Original acceptance controls on the final candidate

The unchanged original driver also ran the serial, transport and 100-agent
send/wait/ack controls. All message/delivery/ack integrity checks passed.

| Control | Candidate result | Target result |
|---|---|---|
| Unloaded UDS send / wait-after-send p50 | 2.061 / 0.151 ms | PASS |
| Unloaded HTTP send / wait-after-send p50 | 2.541 / 0.469 ms | PASS |
| Serial durable append | 471.2 msg/s | 5,000/s MISS |
| 99 idle waiters plus one active agent | Send p50 1.968 ms | Declared idle diagnostics PASS |
| 100 active send/wait/ack agents | 990.8 completed cycles/s; send p50 27.016 ms | Send <10 ms MISS |
| Same 100-active control, wait-after-send | p50 25.981 ms | <25 ms MISS |

The earlier 100-active control measured 243.2 completed cycles/s and send p50
398.864 ms; its wait-after-send p50 was 4.113 ms. The candidate improves send
and cycle throughput while wait timing regresses. Wait-after-send is a client
RPC measurement and does not establish end-to-end pending-recipient delivery
latency. Full samples and interpretation are in
[candidate controls](candidate02/controls/summary.json) and the
[original baseline](../acceptance-20261020/transport-scale/run-01/summary.json).

## Correctness qualification and identity

- Normal core suite: **92 passed**, one pre-existing ignored performance test.
- `mbt-injection` core suite: **124 passed**, the same one ignored test.
- Both use `COOP_REQUIRE_UDS=1`; no socket tests skipped. Documentation tests
  also completed with zero tests.
- [Generated MBT](qualification/mbt-run-02/summary.json): **63/63 traces**
  (39 protocol/writer, 24 fault), **2/2 actual rotation crash controls**, and
  compiler/provenance checks passed. All 22 Rust source files plus Cargo.toml
  and Cargo.lock match the final release manifest.
- Batch-specific fault tests cover partial records, failed covering/recovery
  syncs, unpublished provisional heads, cross-inbox progress, exact caps,
  concurrent retries across recipients, TTL reconstruction, cancellation and
  worker/root lifetime.
- [Batch process-crash controls](qualification/batch-crash-run-01/attempt-01/summary.json):
  **3/3 actual SIGKILL controls passed**, before covering sync, after successful
  receipts, and after partial second-record failure. Recovery preserves the
  prior acknowledged prefix, exact committed receipts, sequence/hash order
  and dedupe. These Unix process controls exercise the production broker API
  without sockets and do not simulate hardware power loss. Their separate
  targeted integration run passed four tests, including one no-op child
  helper; they are additional to the 124-test frozen injection suite.

The core test commands selected `--lib --bins --tests` while the native driver
was being developed. The finished native example was built separately and
exercised by its smoke and full paired runs. Source/binary pins are in
[the final manifest](qualification/run-02/source-manifest.json); the first
candidate's qualification and snapshot remain under `qualification/run-01/`.

| Artifact | SHA-256 |
|---|---|
| Original broker executable | `c6d1047a8a4d9b0094274151c9658a101ed018ce07f82d66ed9fb512eb285321` |
| Final release broker executable | `32d5b650e8768407d297165eb350c295ab0b7aa0da14272fcee3948765383488` |
| Native driver executable | `29c3932d6f327efdab3f4b9523e782fe44ce11cbcacf54110174dbde8d2b04a4` |

Credentials were checked/redacted by the evidence harnesses. Benchmark daemons
and their disposable live roots are owned by those harnesses and cleaned up;
copied evidence is retained. No global MCP client configuration was changed.

## Evidence publication

After all run audits, 109 generated JSONL evidence files larger than 1 MiB
were losslessly archived as `.jsonl.gz`: 1,015,216,538 original bytes became
116,750,453 compressed bytes. Every decompressed byte count and SHA-256 was
checked before removing its uncompressed copy. Small files and frozen child
manifests were retained unchanged.

[artifact-publication.json](artifact-publication.json) maps each original
path/hash to its published path/hash. Frozen run manifests continue to name
the original uncompressed bytes. To inspect one archived stream, use
`gzip -dc path/to/file.jsonl.gz`; to restore it alongside the archive, use
`gzip -dk path/to/file.jsonl.gz` and check the original hash from the map.
The [machine-readable result](summary.json) preserves separate correctness and
performance decisions. Final publication checksums are in `SHA256SUMS`; check
them with `cd docs/impl/throughput-20261020 && sha256sum -c SHA256SUMS` from the
repository root.

## Reproduction and remaining work

Use new output directories and an idle measurement host. Preserve the original
binary and its original source manifest for comparisons. Socket gates require
host socket access.

```sh
cargo build --offline --locked --release
COOP_REQUIRE_UDS=1 cargo test --offline --locked --lib --bins --tests
COOP_REQUIRE_UDS=1 cargo test --offline --locked --lib --bins --tests --features mbt-injection
cargo test --offline --locked --doc
cargo test --offline --locked --features mbt-injection --test batch_crash
cargo build --offline --locked --release --example throughput_native
```

Run the benchmark commands in the [native method](native/method.md) and
[Python method](benchmark-method.md), substituting fresh result paths. Read
the explicit target-gate fields as well as exit codes and correctness fields.

Remaining performance acceptance covers distributed inboxes, the original
100-active client workload, serial overhead, and representative repeated
measurements. Modern Claude MCP discovery and the previously measured
million-record full-startup miss remain separate follow-ups. No new
million-record startup qualification is claimed for this candidate.
