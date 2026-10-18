# Cooperators — TLA+ specification

`Cooperators.tla` models the core safety semantics of
[`docs/design/0001-cooperatorsd-broker.md`](../docs/design/0001-cooperatorsd-broker.md):
the durable append-only log, idempotent send / peek / ack messaging, queue claim
leases with fencing, and document publish/CAS/claim. One queue (`q0`) and one
document (`d0`) are modeled; they share the lease/fence machinery, which is the
point of the design.

## Modeling assumptions (limits of the evidence)

- **A1 Durability**: append+fsync is atomic per the design ("append → fsync →
  ack"), so an acknowledged record is never lost. Torn-tail recovery drops only
  a never-acknowledged suffix and is not a separate action. Crash therefore
  preserves `log` and resets only volatile `ackCursor` (redelivery is the
  documented at-least-once consequence).
- **A2 Single writer**: the daemon is the only log writer; every action is
  atomic. Lease *renew* is omitted — it only extends a deadline, and expiry is
  already nondeterministic (TTL abstraction).
- **A3 Rejections leave no trace**: `VERSION_CONFLICT`, `CONFLICT_CLAIMED`,
  `LEASE_LOST`, and deduped resends are action-disabling. The invariants assert
  that no invalid record ever enters the log.

## Checked properties

| Property | Type | Design claim it pins |
|---|---|---|
| `TypeOK` | invariant | record shapes |
| `NoDuplicateMessages` | invariant | idempotency keys: one log record per `(from, key)` |
| `ClaimSeparation` | invariant | claim exclusion: any two claims of a resource are separated by a terminal event for the earlier fence |
| `FenceFreshness` | invariant | every `release/expire/complete/fail` carries the resource's latest claim fence — a stale lease never mutates state |
| `AttemptsBound` | invariant | `attempts ≤ MaxAttempts` |
| `DeadMeansNoActiveLease` | invariant | dead-lettered tasks are immediately unclaimable |
| `DocVersionChain` | invariant | CAS: doc versions are contiguous, each update bases on head |
| `DocHolderRespected` | invariant | a claimed doc accepts writes only from the claim holder |
| `DocWriteFenceLive` | invariant | a claimed write's lease was never released/expired before the write |
| `TaskResolved` | temporal | every claimed task is eventually completed or dead-lettered |

## Model-checked result (2026-10-18)

```
java -jar /usr/local/lib/tla2tools.jar -config spec/Cooperators.cfg -workers 4 spec/Cooperators.tla
```

Model: `Agents={a1,a2}` (symmetry), `Keys={k1}`, `MaxAttempts=2`, state
constraint `Len(log) <= 6`. Result: **no error found** —
17,825,060 states generated, 1,673,205 distinct states, 0 left on queue,
depth 9 (deepened by ack/crash cycles, not log length), all invariants and the
temporal property checked on the complete state space, fingerprint-collision
probability 6.1E-8. Runtime 6m19s on this machine.

Larger bounds (`Len(log) <= 8..10`, two keys) were attempted and exceeded a
9-minute timeout at >8.5M distinct states with no error found at any interim
checkpoint; they are **not** counted as evidence. The exhaustive claim is the
small model above. Deadlock checking is disabled: states with a full log and
all cursors at 0 are expected sink states under the state constraint.

## Artifacts

- `Cooperators.tla` — the specification (module header documents A1–A3).
- `Cooperators.cfg` — the TLC model above. Scale `Agents`, `Keys`,
  `MaxAttempts`, or `StateConstraint` up for deeper checks (super-exponential
  growth; the ack/crash depth multiplier is the dominant factor).
