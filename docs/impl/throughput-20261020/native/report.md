# Matched native UDS throughput comparison

The native candidate completed all eight conditions without errors or inbox-capacity failures. Both 5,000/s and p50 <10 ms gates passed for **same-inbox C=32 and C=100**. The full performance gate still **fails**: all distributed-inbox throughput cases missed 5,000/s, and distributed C=100 also missed the latency target.

All **16/16 baseline and candidate conditions passed correctness**. Every successful receipt, delivered envelope, acknowledged cursor and persisted inbox row matched; every hash chain verified with read-only APIs after daemon exit. This is distinct from satisfying the performance targets.

The paired runs used the identical frozen Rust driver, four Tokio workers, persistent UDS connections, per-request authentication, HTTP off, the unchanged 128-byte payload, one outstanding send per sender, one-second/minimum-three warmup, 10-second windows, default-size 100-message drain batches, 20 ms sleep after empty and unchanged inbox caps. See [method](method.md), [declaration](plan.json) and [complete machine-readable comparison](comparison.json).

## Baseline

| Topology | Senders | Durable acks/s | p50 ms | p95 ms | p99 ms | >=5,000/s | p50 <10 ms | Correct |
|---|---:|---:|---:|---:|---:|:---:|:---:|:---:|
| Same inbox | 1 | 640.3 | 1.430 | 2.185 | 3.654 | MISS | PASS | PASS |
| Same inbox | 8 | 574.1 | 13.488 | 18.210 | 22.158 | MISS | MISS | PASS |
| Same inbox | 32 | 581.7 | 53.781 | 65.840 | 74.331 | MISS | MISS | PASS |
| Same inbox | 100 | 531.6 | 183.400 | 227.651 | 266.212 | MISS | MISS | PASS |
| Distributed | 1 | 522.9 | 1.682 | 2.951 | 6.697 | MISS | PASS | PASS |
| Distributed | 8 | 520.6 | 14.502 | 21.275 | 25.466 | MISS | MISS | PASS |
| Distributed | 32 | 469.2 | 65.268 | 88.446 | 100.719 | MISS | MISS | PASS |
| Distributed | 100 | 455.8 | 218.502 | 246.037 | 254.758 | MISS | MISS | PASS |

[Run summary](baseline-run01/summary.json), [environment and provenance](baseline-run01/environment.json), [credential audit](baseline-run01/credential-check.json), [artifact hashes](baseline-run01/sha256.json).

## Candidate

| Topology | Senders | Durable acks/s | p50 ms | p95 ms | p99 ms | >=5,000/s | p50 <10 ms | Correct |
|---|---:|---:|---:|---:|---:|:---:|:---:|:---:|
| Same inbox | 1 | 492.8 | 1.914 | 2.728 | 3.495 | MISS | PASS | PASS |
| Same inbox | 8 | 2,043.9 | 3.720 | 5.338 | 8.648 | MISS | PASS | PASS |
| Same inbox | 32 | 6,030.3 | 5.022 | 7.457 | 10.646 | PASS | PASS | PASS |
| Same inbox | 100 | 12,834.9 | 7.469 | 10.440 | 13.478 | PASS | PASS | PASS |
| Distributed | 1 | 456.4 | 2.048 | 2.957 | 4.324 | MISS | PASS | PASS |
| Distributed | 8 | 1,712.4 | 4.145 | 7.544 | 9.612 | MISS | PASS | PASS |
| Distributed | 32 | 3,712.3 | 7.698 | 12.202 | 14.782 | MISS | PASS | PASS |
| Distributed | 100 | 4,608.1 | 21.407 | 27.648 | 32.061 | MISS | MISS | PASS |

[Run summary](candidate-run01/summary.json), [environment and provenance](candidate-run01/environment.json), [credential audit](candidate-run01/credential-check.json), [artifact hashes](candidate-run01/sha256.json).

## What the comparison establishes

At C=100, same-inbox throughput rose from **531.6 to 12,834.9 acks/s** (24.14x), with p50 falling from 183.400 to 7.469 ms. Distributed C=100 rose from **455.8 to 4,608.1 acks/s** (10.11x), with p50 falling from 218.502 to 21.407 ms. Concurrent sends to a common log can exceed both targets with this client; the distributed case remains below the required throughput and above the required latency.

At C=1, candidate throughput was lower in this paired observation: 492.8 versus 640.3 acks/s for same-inbox (23.0% lower), and 456.4 versus 522.9 for distributed (12.7% lower). A single sender provides no overlap for group commit. One sample per condition does not establish the size or cause of a repeatable regression.

## CPU and cleanup

| C=100 topology | Baseline driver CPU | Baseline daemon CPU | Candidate driver CPU | Candidate daemon CPU |
|---|---:|---:|---:|---:|
| Same inbox | 9.1% | 23.5% | 80.6% | 156.4% |
| Distributed | 30.2% | 45.3% | 105.8% | 409.1% |

One logical CPU is 100%; these are per-window `/proc` CPU deltas. The native control removes the earlier same-inbox C=100 capacity failure and changes the balance of client/daemon CPU. This is consistent with client overhead contributing to the Python result; it does not prove GIL contention or exclude server/storage effects.

The baseline verified **47,792 records including warmup**. All its child PIDs and disposable roots were absent after completion. Source, binary, driver and helper identities remained stable; credential audit passed.
The candidate verified **353,789 records including warmup**. All its child PIDs and disposable roots were absent after completion. Source, binary, driver and helper identities remained stable; credential audit passed.

## Provenance and retained failures

- Baseline binary: `c6d1047a8a4d9b0094274151c9658a101ed018ce07f82d66ed9fb512eb285321`. It is pinned to the original installed acceptance source manifest.
- Candidate binary: `32d5b650e8768407d297165eb350c295ab0b7aa0da14272fcee3948765383488`, pinned to qualification run 02.
- Identical native driver binary: `29c3932d6f327efdab3f4b9523e782fe44ce11cbcacf54110174dbde8d2b04a4`.
- Identical driver source: `d33152fc4d342278c6feeefef22e0d6d88af8e79ddaf88364ace8fa30b2b1758`.

## Reproduction

The actual build used the following command. The frozen method's build example is abbreviated; use the offline, locked form for reproduction:

```sh
CARGO_BUILD_JOBS=2 cargo build --offline --locked --release --example throughput_native
```

Run the same resulting driver against each retained executable, supplying a new output directory and its matching source manifest:

```sh
target/release/examples/throughput_native \
  --binary /tmp/cooperators-step3-baseline/coop \
  --output /path/to/new/baseline-output \
  --label baseline \
  --source-manifest docs/impl/acceptance-20261020/installed/source-manifest.json

target/release/examples/throughput_native \
  --binary target/release/coop \
  --output /path/to/new/candidate-output \
  --label candidate \
  --source-manifest docs/impl/throughput-20261020/qualification/run-02/source-manifest.json
```

These executions require local UDS socket binding permission. The candidate command reproduces this comparison only while the binary hash matches the retained candidate above.

[Smoke run 01](smoke-run01/summary.json) retained the sandbox UDS-bind denial before enrollment or timing. [Smoke run 02](smoke-run02/summary.json) passed all checks after local socket permission was granted. No source/method/plan changes occurred between the successful smoke and paired runs.

The frozen [Python candidate failure report](../candidate02/failure-report.md) and all prior failed runs remain valid evidence for that driver/workload. The native driver and HTTP-off setting form a separate matched control; they do not replace those failures. These one-host, one-filesystem samples have no confidence intervals. Readback proves retained record integrity and acknowledged effects; the separate recovery qualification covers fault/crash behavior.
