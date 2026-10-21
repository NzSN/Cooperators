# Installed acceptance and performance baseline

Date: 2026-10-20. The requested installed-client exercise and remaining
performance measurements have run. The Codex and Claude CLI exchange passed
with Claude's legacy MCP setting. Default modern Claude discovery failed.
The measured performance gaps are sustained append throughput, send latency
with 100 active agents, and inconsistent million-record restart time.

| Check | Measured result | Acceptance result |
|---|---|---|
| Installed Codex and Claude CLI message/task exchange | Distinct identities; three messages; task claimed/completed with correct fence and answer; both inboxes acknowledged | PASS with `MCP_PROTOCOL_NEGOTIATION=legacy` on Claude |
| Default modern Claude MCP discovery | Connected broker; zero model-visible tools because tool-list cache fields are absent | FAIL |
| UDS send acknowledgment | p50 2.680 ms; p95 5.007 ms; p99 7.428 ms | PASS, p50 < 10 ms |
| HTTP MCP send acknowledgment | p50 3.378 ms; p95 5.637 ms; p99 7.738 ms | PASS, p50 < 10 ms |
| Pending-recipient send start to delivery | p50 4.828 ms UDS / 4.815 ms HTTP | Requests were observably pending; exact server wait entry was not instrumented |
| Sustained serial UDS append | 341.628 messages/sec, 3,417 durable messages in 10.002 s | MISS, target >= 5,000 messages/sec |
| 100 enrolled, 99 idle long polls | Active sender p50 3.366 ms; 237.915 cycles/sec | Absolute p50 targets and predeclared relative diagnostics pass |
| 100 active agents | Send p50 398.864 ms; p95 529.727 ms; 243.170 cycles/sec | MISS, send p50 < 10 ms; substantial latency degradation |
| Actual 1M-record storage replay | 12.827 / 8.642 / 8.640 s; median 8.642 s | Two of three samples < 10 s |
| Actual 1M-record daemon startup to UDS readiness | 12.439 / 12.738 / 9.746 s; median 12.439 s | MISS in two of three samples |

## Evidence and reproduction

- [Installed-client report](installed/README.md) and [independent verification](installed/verification.json).
- [Transport and 100-agent results](transport-scale/README.md), with raw samples and durable files.
- [Pending-wait delivery supplement](transport-wakeup/README.md), including signed acknowledgment-to-delivery intervals.
- [Actual million-record replay](replay/README.md), with direct storage and separate production process startup timings.
- [Modern discovery diagnosis](installed/discovery-diagnosis.md), including the compatible launch setting and proposed response correction.

All message, receipt, payload, identity, acknowledgment, and durable-record
checks passed for the completed benchmark workloads: 11,783 transport/scale
messages and 220 pending-wait samples. All six million-record integrity and
runtime reconstruction checks passed. The installed exchange was separately
verified against native tool traffic and durable broker files.

The actual installed clients were Codex CLI 0.163.0 with its configured
`gpt-6.1-sol` model, and Claude Code 2.1.300 with its configured `deepseek-flash`
provider/model. Neither model was overridden. The legacy workaround is a
condition of the successful client exercise; default modern acceptance is open.

## Measurement scope

All runs used the unchanged default release binary, SHA-256
`c6d1047a8a4d9b0094274151c9658a101ed018ce07f82d66ed9fb512eb285321`,
from source base `63e20514fffa039a97b0ec25e432657854d2fdef`. Builds used
`--offline --locked --release`. Production source hashes remained unchanged.
Only harnesses, retained evidence, and documentation were added or updated.

This host was an Intel Core i5-1240P running WSL2, with bus files on `/tmp`
ext4. The original smoke used different hardware and extrapolated from 100k
records, so the changed replay result cannot be attributed to a software
regression. Benchmarks ran sequentially with isolated broker roots. The
installed acceptance CLI was nearly idle during benchmark windows.

The scale workload used closed-loop self-send/wait/ack clients and included
Python scheduling. The 100-active comparison changes offered concurrency and
per-inbox history length, so relative ratios do not isolate the cost of agent
count alone. Storage replay used a warm filesystem cache and one million inbox
envelopes; it did not stress queue/document/dedupe indexes. Full daemon startup
was measured separately from storage opening. Detailed sample counts, timing
boundaries, source/binary pins, and reproduction commands accompany each report.

## Remaining work

1. Correct modern `tools/list` cache metadata and repeat the installed-client
   exercise with Claude's default negotiation.
2. Design and benchmark throughput improvements while preserving acknowledgment
   after fsync, then repeat the 100-active workload and recovery qualification.
3. Investigate full startup cost and replay variability before repeating the
   actual million-record restart gate.

The test daemon and client bridges were stopped. No production behavior was
changed during this acceptance run.
