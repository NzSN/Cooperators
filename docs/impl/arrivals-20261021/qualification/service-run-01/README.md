# Arrival service correctness qualification

Scope: durable subscriptions, independent arrival feed and ACK journals, source
projection and recovery. No host/model process was run. Commands used the local
checkout and cached locked dependencies.

## Commands and evidence

| Command | Result | Evidence |
| --- | --- | --- |
| `cargo test --offline --locked --test arrival_service` | 8 passed before adding sparse-anchor regression | [initial service](arrival-service.log) |
| `cargo test --offline --locked --test arrival_service` | 9 passed with sparse-anchor regression | [service plus anchors](arrival-service-with-anchors.log) |
| `cargo test --offline --locked --features mbt-injection --test arrival_faults` | 4 passed before capture admission API hardening | [initial fault tests](arrival-faults.log) |
| `cargo test --offline --locked --features mbt-injection --test arrival_service --test arrival_faults` | 9 service and 4 fault tests passed after capture admission hardening | [combined final](service-and-faults-final.log) |

Coverage includes source-commit/feed gap replay; replay after feed ACK; separate
catch-up obligation coordinates; identity-scoped client-key conflict and retry;
immutable creation receipts after removal; full-recipient isolation and source
ordering; retired identities; cursor binding and ACK bounds; validated source
heads and rotating sources; rotated retained feeds with 64-record sparse page
anchors; partial and complete uncertain feed writes; ACK quarantine; subscription
quarantine; and failed recovery sync before index publication.

These are in-process correctness tests with deterministic storage faults. They
do not establish process-kill durability, real-host activation, throughput or
retained-history startup performance. Separate source/RPC and process-crash
qualification is recorded by the owning implementation workers.

## Implementation bounds

Projection admits at most four simultaneous recipient workers. Each finite job
scans at most `scan_batch` records and retains root ownership through I/O. A
round-robin metadata queue avoids starvation between recipients. Full feeds
pause their own projection until an ACK or explicit scheduling hint; poisoned
feed handles require actual reopen.

Replay uses one-record parsing buffers, compact obligation watermarks, pending
record sizes, retained subscription metadata and one page anchor per 64 feed
records. Each page scans at most 63 records before its requested cursor and
returns at most the configured message/byte page. Sparse anchors and retained
subscription metadata grow with retained history; total memory is not constant.
No historical message payload cache or delivery-ID set is retained.
