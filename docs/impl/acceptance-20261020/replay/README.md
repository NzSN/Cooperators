# Actual 1,000,000-record replay — 2026-10-20

The actual production daemon restart missed the design's **1 M records < 10 s**
target in two of three samples. Its median was **12.439 s**. All integrity and
runtime reconstruction checks passed. These measurements replace extrapolation
for this host; they do not establish consistent compliance with the restart gate.

| Scope | Sample 1 | Sample 2 | Sample 3 | Median | Samples < 10 s |
|---|---:|---:|---:|---:|---:|
| `LogFile<Envelope>::open` | 12.827 s | 8.642 s | 8.640 s | 8.642 s | 2 / 3 |
| Fresh `coop daemon run` process to successful UDS `daemon.status` | 12.439 s | 12.738 s | 9.746 s | 12.439 s | 1 / 3 |

Each sample replayed **1,000,000 actual records**, rather than multiplying a
smaller run. The direct opens ran sequentially in one benchmark process. Each
daemon sample started a separate production `coop` process and stopped it after
validation. The first direct open used a fresh process; later direct samples
reused its allocator. The cause of timing variation was not isolated.

## Timing and validation

Direct storage timing starts immediately before `LogFile::open` and ends when
it returns. It includes reading the entire JSONL file, parsing, sequence and
hash-chain checks, recovery `sync_all`, and seeking to the append position.
Result assertions and record destruction occur after the timer stops. This is
the same API measured by the earlier [performance smoke](../../perf-smoke.md),
now exercised at the target record count.

Daemon timing starts immediately before spawning the real release binary and
ends after its first successful UDS `daemon.status` response. It includes process
startup, root ownership locking, registry and log replay, inbox backlog byte
reconstruction, presence initialization, socket binding, and the status round
trip. Readiness is polled every 5 ms. HTTP is explicitly disabled. Shutdown and
the final message lookup are outside the timed interval.

Every direct open verified the recovered record count, all sequence positions,
log sequence head, expected final hash, byte offset, and absence of truncation
or a missing newline. Each daemon status returned the million-record inbox head
and expected byte count. An authenticated `wait_message` starting at cursor
999999 then returned exactly record 1000000 with the expected body, proving that
the fresh process had reconstructed its runtime inbox. The fixture's byte count
and SHA-256 remained unchanged after all six measurements.

The harness completed with exit status 0 and no stderr. Peak resident memory was
1,139,856 KiB, approximately **1.09 GiB**. The complete fixture generation,
measurements, validation, and cleanup took 75.66 s. The timing report records no
major page faults or swaps.

## Fixture and environment

| Item | Value |
|---|---|
| Measurement window | 2026-10-20 09:07:47–09:09:02 UTC |
| Source base | `63e20514fffa039a97b0ec25e432657854d2fdef` |
| Build | `cargo build --offline --locked --release --bin coop --example replay_baseline`, default features |
| Rust / Cargo | 1.95.0 / 1.95.0 |
| CPU | 12th Gen Intel Core i5-1240P, 12 available logical CPUs |
| OS | Linux 6.18.54.1-microsoft-standard-WSL2, x86_64 |
| Fixture filesystem | `/tmp`, ext4 on `/dev/sdd` |
| Available memory before run | 4,492,632 KiB |
| Cache condition | Warm filesystem cache; caches were not evicted |
| Concurrency | No other Cooperators benchmark or build ran during the window; an installed Claude acceptance client was waiting on a provider response |
| Envelope records | 1,000,000, one unrotated inbox, sequence 1 through 1,000,000 |
| Fixture bytes | `327777786` (approximately 312.59 MiB) |
| Fixture file SHA-256 | `4c9b4419f9b0675f4bb9abde45ebefddd7ba4fa8670dfd1428fe4d9a684d2c22` |
| Final record chain hash | `sha256:977b1029e3fa078f6e7927afeddef27b8bb4b4631d5df060c944b21ad2c01a3a` |
| `coop` binary SHA-256 | `c6d1047a8a4d9b0094274151c9658a101ed018ce07f82d66ed9fb512eb285321` |
| Harness binary SHA-256 | `74e1c3d4d098f16d4da13ed738509f28824c502e20376054f54e95dadb966c21` |

The deterministic fixture uses valid v1 envelopes, unique ULIDs, a fixed
`2026-10-20T00:00:00.000Z` timestamp, sender `perf-sender`, recipient
`perf-recipient`, kind `perf`, and `text/plain` body `replay record {index}`.
It has no idempotency keys, expiry sidecars, document histories, or queue event
logs. Thus the process samples include inbox reconstruction but do not measure
large dedupe, expiry, queue, or document indexes. The registry contains two real
agents, and their authentication tokens are never written to evidence.

The fixture is streamed through a buffered writer. The harness flushes it,
calls file `sync_all`, and syncs its parent directory before timing begins.
This fixture preparation is outside replay timing and does not measure durable
append throughput. Its file SHA-256 is recorded before the first sample and
checked again after the last sample. The 327 MB fixture was removed after the
successful run; its deterministic recipe and hashes are retained.

The previous smoke report used a Ryzen 9 3900X and extrapolated from 100,000
records. This run used an i5-1240P and a complete million-record fixture. Those
differences prevent attributing the changed result to a software regression.

## Reproduction and retained evidence

From the repository root, supply new output and fixture paths:

```sh
docs/impl/acceptance-20261020/replay/reproduce.sh \
  /tmp/cooperators-replay-new-evidence \
  /tmp/cooperators-replay-new-fixture
```

The script refuses existing paths, builds from the locked dependencies without
network access, and runs three samples of each scope. The command needs local
UDS binding permission. The retained run used an approved execution outside the
sandbox because its UDS bind is otherwise denied. It changed no global config.

- [Harness source](../../../../examples/replay_baseline.rs)
- [Reproduction script](reproduce.sh)
- [Raw results](run/results.jsonl) and [console stdout](run/stdout.jsonl)
- [Build, source hashes, and environment](run/build-environment.txt)
- [Build stdout](run/build.stdout.log) and [stderr](run/build.stderr.log)
- [Whole-run process timing and memory](run/overall-time.txt)
- [Harness stderr](run/harness.stderr.log)
- Daemon stderr: [sample 1](run/daemon-1.stderr.log), [sample 2](run/daemon-2.stderr.log), [sample 3](run/daemon-3.stderr.log)

All six measurements use the default release build. No production source was
changed for these measurements.
