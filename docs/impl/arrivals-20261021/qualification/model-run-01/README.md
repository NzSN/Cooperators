# Bounded arrival and bridge reference-model traces

Status: **passed**. The final frozen source passed both tests and all 128
traces in 9.70 s. `cargo-test-02.log` is the final qualification log. The
earlier `cargo-test-01.log` records the first version before the additional
claim race; it is retained as historical evidence only.

Source: `tests/arrival_model.rs`, SHA-256
`0578d31f43232d2f000a82c37359ef07a4552b51804110c353b8e2ede00b0422`.
The tests add no dependencies and invoke no provider or model process.

## Generator and replay

The fixed enumeration seed is `0x00040001` (`262145`). There is no PRNG:
each group executes every ordered pair from its eight-operation alphabet,
with case number `8 * first + second`. Each pair has a fixed setup and fair
recovery suffix. The exact alphabets, prefixes, suffixes and 128 replay filters
are retained in `trace-manifest.json`. Guarded operations without a matching
active wake or claim are stuttering transitions.

Run the complete gate:

```sh
cargo test --offline --locked --test arrival_model -- --nocapture
```

Replay one counterexample or selected trace:

```sh
COOP_MODEL_CASE=broker:17 cargo test --offline --locked --test arrival_model -- --nocapture
COOP_MODEL_CASE=bridge:38 cargo test --offline --locked --test arrival_model -- --nocapture
```

Each trace emits its stable group, seed, case and operation pair before
execution. Assertions include the case and generated operation prefix.
Malformed or out-of-bound replay filters fail rather than silently skipping
all traces. No counterexample was found in either run. The final source also
exercises adoption invalidating an older empty reply.

## Broker model: 64 traces

The system under test is a real in-process `DaemonState`, reached through
authenticated `server::dispatch` calls. Projection is initially deferred.
Explicit recovery starts the real projector and fairly drains both live
recipient identities. Generated operations remove/resubscribe either reader,
publish/update, restart, consume/ACK, and attempt future or foreign ACKs.

The model independently records which subscriptions existed at each document
mutation and the current snapshot owed at resubscription. It compares these
expectations with persisted `DocEvent.notification_intent` routes and complete
typed `ArrivalNotice` feeds. It checks pinned source hashes, dense feed
sequences, unique delivery IDs, recipient ownership, durable ACK log heads and
public ACK cursors. Ordering between catch-up and live obligations is not
assumed; the exact expected multiset is compared instead.

Each trace has two live recipient identities and at most three document
versions. Removal cannot cancel an earlier captured obligation. Restart and
ACK cannot regenerate an already materialized delivery. Future cursors and
another identity's cursor/subscription cannot change either reader's ACK.

## Bridge model: 64 traces

The system under test is a real `HostBridge` with its own durable journal,
ownership locks and replay. Queue notices are synthetic protocol inputs;
claim results are supplied through the trusted bridge API. This group does
not claim to run a broker claim or a provider tool invocation.

The model maintains a pending sequence set, immutable bounded active batch,
binding generation, outstanding claim contexts, adoption epoch and the source
boundary of usable empty evidence. After every step it compares pending
cardinality, received cursor/head, active wake, generation and in-flight claim
count with the bridge. Restarts reconstruct the real journal and invalidate
connection-specific empty evidence.

Generated operations receive newer input, restart, rebind, schedule, start or
finish claims, and attempt resolution. The suffix in every trace additionally
checks that:

- An empty reply begun before a later adoption cannot resolve the wake.
- An arrival received after an empty claim began remains pending when the
  earlier immutable batch resolves.
- Rebinding requires completion of outstanding claims.
- A prior generation's resolution and claim context cannot affect the next
  generation, including after restart.
- Fair fresh empty claims eventually resolve the remaining finite input,
  leaving no outstanding bridge claim contexts.

At most four notices, two notices per wake and three concurrent claim contexts
are exercised in one bridge trace.

## Qualification boundary

These are finite implementation-conformance traces, not a mathematical proof.
Eventual delivery is checked only after explicit recovery/projection and fair
drains, with recipients live, retained valid source history and sufficient
configured capacity. These tests do not qualify permanent storage failure,
process termination during a write, unbounded histories, real host acceptance,
model observation or model resolution. Those gates have separate evidence.
