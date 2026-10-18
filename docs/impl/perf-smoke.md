# M4 performance smoke — 2026-10-18

Status: **measured, one design target missed** (sustained append). Results are
for this host and filesystem; they are not a release qualification on other
hardware.

## Environment

| Item | Value |
|---|---|
| Date (UTC) | 2026-10-18T11:14:29Z |
| Host | Linux 6.18.54.1-microsoft-standard-WSL2 (Ubuntu 24.04.3 LTS), x86_64 |
| CPU | AMD Ryzen 9 3900X 12-Core Processor, 24 logical CPUs |
| Bus root | `/tmp` on `/dev/sdd` (ext4) |
| Build | `cargo test --offline --release` |
| Command | `cargo test --offline --release --test perf_smoke -- --ignored --nocapture` |

All measurements are in-process dispatches against `DaemonState`; the test
binds no socket. Send latency and append throughput include the per-record
`fsync` performed by `LogFile::append`. Replay timing covers `LogFile::open`
(parse, sequence check, hash-chain verification) for 100 000 records; the
1 M-record figure is a linear extrapolation (×10), not a separate run.

## Results vs design §11

| Metric | Target | Measured | Result |
|---|---|---|---|
| send ack p50 (fsynced) | < 10 ms | **2.674 ms** | PASS |
| wait delivery after send p50 | < 25 ms | **0.010 ms** | PASS |
| sustained append | ≥ 5 000 msg/s | **335 msg/s** (671 messages in 2.00 s) | **MISS** |
| restart replay | < 10 s / 1 M records | **0.612 s / 100 k**, 6.119 s extrapolated / 1 M | PASS |
| agents per daemon | 100 without degradation | not measured | N/A |
| UDS round-trip send ack | (transport overhead) | not measured — socket-dependent | N/A |
| HTTP `/mcp` round-trip | (transport overhead) | not measured — socket-dependent | N/A |

Sample sizes: 100 send/wait pairs; 2.0 s append window; 100 000 replay records.

## Raw output

```text
cooperators M4 perf smoke
date: 2026-10-18T11:14:29.972Z
profile: release
host: linux / 24 logical CPUs
cpu: AMD Ryzen 9 3900X 12-Core Processor
root: /tmp/coop-test-m4-perf-673-1792322066056031874-0/root

| Metric | Target | Measured | Result |
|---|---|---|---|
| send ack p50 (fsynced) | < 10 ms | 2.674 ms | PASS |
| wait delivery after send p50 | < 25 ms | 0.010 ms | PASS |
| sustained append | >= 5 000 msg/s | 335 msg/s (671 msgs in 2.00 s) | MISS |
| restart replay | < 10 s / 1 M records | 0.612 s / 100 k, 6.119 s extrapolated / 1 M | PASS |

sample sizes: send/wait 100 each; append window 2.0 s; replay 100000 records
```

## Interpretation

- Send latency and replay are comfortably inside the targets on this host.
- Sustained append misses the 5 000 msg/s target by roughly 15×. At ~2.7 ms
  p50 per send, the measured rate is consistent with one fsync-bound durable
  append per message on this ext4 volume. The target and the durability rule
  ("ack only after fsync") are therefore in tension at this scale; meeting it
  would require group commit / fsync batching or a weaker durability mode.
  No such change was made in T5 because it would alter the send-ack contract.
- The wait-delivery figure measures dispatch after the durable send returns;
  it does not include a socket round trip. UDS and HTTP transport rows remain
  N/A in this sandbox and should be measured host-side before a release claim.
- "Agents per daemon" is a design acceptance dimension, not part of this
  smoke test; it is listed as N/A rather than inferred.

## Follow-ups (beyond T5)

- Benchmark group-commit designs against the §11 sustained-append target
  without weakening the fsynced acknowledgment contract.
- Run the same smoke plus real UDS and HTTP round-trip measurements on the
  target host (`COOP_REQUIRE_UDS=1`).
- Add a 100-agent scale run to cover the "without degradation" clause.
