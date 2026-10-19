# Cooperators — TLA+ specification

`Cooperators.tla` models the core safety semantics of
[`docs/design/0001-cooperatorsd-broker.md`](../docs/design/0001-cooperatorsd-broker.md):
the durable append-only log, idempotent send and volatile acknowledgment cursors, queue claim
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

These are assumptions about the implementation, not properties established by
checking this protocol model. The hardening models below make several of the
missing implementation obligations explicit.

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

## Agent instance identity

[`AgentInstances.tla`](AgentInstances.tla) makes the identity requirement in
design §4.1 executable. Independent launches receive independent IDs and
credentials, including when both have kind **codex** and display label
**codex**. The existing `agent_id` is the instance identity. Model launch slots
represent separate enrollment request keys, not a second runtime ID field.

Actions cover enrollment, enrollment retry, disconnect, authenticated reconnect,
token rotation, retirement, sends, targeted acknowledgments, heartbeats,
claims, completion attempts with caller/fence checks, and lease expiry.
IDs and tokens are allocated from unused pools. Retirement retains allocation
history; reconnect and rotation preserve the first issued identity. Tokens are
modeled as opaque credentials whose authenticated principal is an agent ID.

| Invariant | Meaning |
|---|---|
| `TypeOK` | State fields stay within their declared finite domains. |
| `InstanceIdentityUnique` | Distinct active launches have different IDs. |
| `IdentityNotReused` | Every issued ID retains its original launch owner, including after retirement. |
| `EnrollmentRetryStable` | Retrying a launch's enrollment returns its first issued ID. |
| `InstanceIdentityStable` | An enrolled launch retains that first ID through reconnects and rotation. |
| `CredentialBoundToInstance` | Active credentials resolve to their assigned ID and an active launch. |
| `DistinctInstanceCredentials` | Different active launches have different current credentials. |
| `RevokedCredentialsRejected` | Credentials revoked by rotation or retirement authenticate no identity. |
| `InboxAndAckIsolation` | Each inbox is exactly its addressed subsequence of accepted messages; ack effects equal only authorized updates. |
| `IdempotencyScopedToInstance` | Each sender-ID/key pair has at most one message, and a first request from another sender using that key still has its own message. |
| `PresenceAndLeaseIsolation` | Heartbeats update only the authenticated ID; a granted lease completion belongs to its holder. |
| `LeaseFenceFresh` | A granted completion also presents the current lease fence. |

Historical allocation and authorized-effect fields are ghost state used as an
independent specification reference. They do not prescribe additional mutable
production files. Rejected ack/lease attempts leave authorized data unchanged;
no type/kind or display-label comparison grants ownership.

### Check the identity model

```sh
# TLC uses depth-first iterative deepening for this safety-only model.
# It checks the finite focused graphs and the six-transition composed graph.
python3 spec/check_hardening.py --model instances

# Optional bounded check of one focused scenario with Apalache.
python3 spec/check_hardening.py --model instances --engine apalache --scenario lifecycle --timeout 60
```

The checker runs five correct-policy scenarios and requires twelve faulty
policies to produce counterexamples: kind-based ID reuse, retired-ID reuse,
new IDs on retry or reconnect, shared credentials, accepted revoked tokens,
broadcast inbox delivery, foreign acknowledgments, kind-wide dedupe,
group heartbeats, foreign lease completion, and ignored lease fences.

Four deliberately false reachability goals must also produce witnesses:
two same-kind/same-label instances with distinct identities and credentials;
distinct identities for retired and live peers; both instances
successfully using the same message key; and combined enrollment, rotation,
messaging, heartbeat, and lease activity. These are coverage goals, not broker
safety properties. The complete matrix has 21 checks.

Checked 2026-10-19 with TLC 2.19: **21/21 checks passed**. The five correct-policy
graphs completed without error; all twelve fault controls and four reachability
goals produced their required invariant counterexamples.

| Correct-policy scenario | Distinct states | Scope |
|---|---:|---|
| lifecycle | 469 | Complete finite graph: allocation, retries, disconnect/reconnect, rotation, retirement |
| messaging | 481 | Complete finite graph: per-instance inboxes, acknowledgments, and dedupe |
| presence | 73 | Complete finite graph: independently authorized heartbeat effects |
| lease | 1,297 | Complete finite graph: caller identity, claim/complete/expiry, and fences |
| combined | 54,541 | Complete graph with an explicit budget of six transitions across all actions |

The runner defaults to `--search dfid` for this safety-only model. This mode
uses an in-memory fingerprint store and runs without the local RMI socket used
by TLC's default breadth-first fingerprint store. The runner requires TLC's
explicit completion message. Installed TLC 2.19 treats `-dfid` as a search-depth
ceiling despite its help wording; the runner now uses 100 and rejects incomplete
exploration. All reported successful checks exhausted their configured graphs.
The existing hardening model retains breadth-first checking for its temporal
properties; selecting DFID for that model is rejected. Apalache checks are
bounded and optional; the combined SMT check can be substantially slower than
TLC and is not needed to reproduce the completed TLC matrix above.

### Identity model bounds and evidence

The supplied configuration has two launch request keys, two ID values, three
token values, one shared message key, two messages, and two lease claims. Both
launches have identical kind and display-label strings. Exhausted pools disable
new allocation; they never justify reusing an identity. Focused scenarios cover
lifecycle, messaging, presence, and leases independently. The combined scenario
enables all actions and uses an explicit six-transition budget; it does not
claim unrestricted composition. Apalache's default bound is also six transitions.
Both checker modes disable deadlock diagnostics because a completed finite
scenario may have no further operation and the specification permits stuttering.
These checks assert safety and reachability, without a liveness claim.

Enrollment and state updates are atomic in this model. Randomness, token
secrecy, distinct launch-key provisioning, actual session boundaries, persistent
registry recovery, and exclusive broker ownership remain implementation
obligations. Pure bearer authentication cannot distinguish processes that
deliberately share one credential. Disconnect preserves an active instance;
retirement revokes its current credential and permanently reserves its ID.

The original `Cooperators.tla` protocol takes canonical instance IDs as its
fixed `Agents` universe. The composed model below connects enrollment and
authentication to those protocol actions. Runtime per-launch provisioning is
now implemented, and the generated failure-profile identity port replays the
enrollment lifecycle. The standalone/composed models' broader delivery and
lease observations are not all exposed by that identity port. Actual MCP and
direct implementation acceptance cover those interactions separately; existing
protocol/writer replay alone does not establish the lifecycle properties.

## Instance identity composed with the broker protocol

[`CooperatorsInstances.tla`](CooperatorsInstances.tla) imports both existing
modules with `INSTANCE`. Its registry uses the lifecycle actions from
`AgentInstances`; its durable log, acknowledgments, task leases, and documents
use the actions from `Cooperators`. It does not copy those transition rules.
The registry's simplified inbox, presence, and lease fields remain unused;
presence is still covered only by the standalone identity model.

Each request resolves its credential to a canonical instance ID. Sends use
that ID as sender and scope dedupe by sender ID plus key. Acknowledgments can
advance only that ID's cursor. Task completion and document edits check the
authenticated holder and the supplied fence; document updates also check the
supplied base version. Requests with invalid or revoked credentials are
rejected. Rejections, deduplicated sends, and redundant acknowledgments record
their decision in ghost state while leaving broker data unchanged.

The wrapper checks the original eight protocol safety invariants and seven
identity lifecycle/authentication invariants, together with these monitors:

| Invariant | Meaning |
|---|---|
| `BrokerSafetyRefinement` | Each projected broker transition satisfies the original `Next` relation or stutters. |
| `IdentitySafetyRefinement` | Each projected registry transition satisfies the original correct lifecycle `Next` relation or stutters. |
| `AuthenticatedRequestsOnly` | Every accepted request has a valid credential for a live, connected instance. |
| `CanonicalActorBinding` | The actor used for an accepted operation equals the authenticated principal. |
| `RequestDecisionsCorrect` | The observed response matches the canonical authorization, dedupe, ownership, cursor, version, and fence policy. |
| `AckIsolation` | An acknowledgment attempt cannot change another instance's cursor. |
| `RequestParametersChecked` | Applied acknowledgments use the requested cursor; terminal/edit operations use the supplied current fence; updates use the supplied current version. |
| `CrashIdentityPreserved` | Broker crash/replay preserves the registry, allocation history, and credentials. |
| `LiveRecipientsOnly` | Accepted sends address a currently active instance. |

The refinement monitors evaluate the original action formulas against the
actual old and new states. Separate request checks are necessary: the original
protocol constructs a correct log entry internally and therefore cannot by
itself detect a caller presenting the wrong fence or base version. Its fixed
agent universe also permits operations by any member without authentication.

```sh
python3 spec/check_hardening.py --model composed --timeout 180
```

The matrix has four correct-policy profiles (messaging, queue, documents, and
combined), twelve deliberate faults, and six reachability goals: **22 checks**.
Fault controls require counterexamples for missing authentication, accepted
revoked tokens, sender spoofing, foreign acknowledgments, shared dedupe,
foreign task/document ownership, ignored CAS/fences, lost identities on crash,
invalid broker transitions, and invalid identity transitions. Controls disable
symmetry because some deliberate fault paths use an asymmetric `CHOOSE`.

Coverage witnesses require two same-kind instances exchanging messages with
the same key, rejection of foreign task completion and document updates, a
claim holder updating after its own token rotation, a successful acknowledgment
after crash/replay with both instances still active, and combined messaging,
task claims, and document publication. A violated reachability goal is expected
and counts as coverage, not a defect in the correct policy.

Checked 2026-10-19 with TLC 2.19: **22/22 checks passed**. All four correct-policy
graphs exhausted without error; all twelve faults and six coverage goals
produced the required invariant counterexamples. The original identity
lifecycle profile and its controls also passed again after the runner update
(9/9 regression checks).

| Correct-policy profile | Distinct states |
|---|---:|
| messaging | 2,900 |
| queue | 2,208 |
| documents | 3,538 |
| combined | 51,871 |

The combined profile took about 104 seconds here; a 60-second attempt timed
out before the successful run. Use the 180-second per-check budget above.

The configuration uses two launches, two IDs, three credentials, one message
key, one queue task, one document, an attempt cap of two, and an explicit budget
of six transitions in every profile. Correct-policy checks use symmetry over
the three separate launch/ID/token domains. TLC DFID exhausted that finite graph;
the six-transition budget is part of the specification. A separate search-depth
ceiling cannot be mistaken for completed exploration by the runner.
These checks establish safety refinement only within this configuration. They
do not prove an unbounded refinement theorem or inherit the original broker's
fairness assumptions and `TaskResolved` liveness claim.

Registry enrollment and authentication are atomic. Crash preserves the registry
by contract; persistent registry I/O and recovery are not modeled. Broker crash
resets volatile acknowledgments using the original protocol rule; disconnect
and reconnect are separate actions. A document must exist before a document
claim. Token secrecy, launch-key provisioning, and single-writer durability
remain assumptions or separate obligations. Runtime provisioning and generated
identity lifecycle replay are implemented through the failure-profile pipeline
below. This does not turn the composed safety model into an implementation
refinement proof.

## Failure and recovery contract for implementation replay

[`MBFaults.tla`](MBFaults.tla) adds explicit operation attempts, responses,
failure barriers, and observation projections to the hardening requirements.
Eight profiles cover writer ownership, write/sync failures, expired inbox
capacity, dead-letter reconciliation, initial publication, status, rotation,
and per-launch identity. Rejected attempts have responses an adapter must observe.

Storage separates bytes, sync, actual receipt history, and recovery. Projection
separates append from completion to check duplicate recovery after a crash.
Rotation separates segment preparation from commit and handles oversized records.
Identity observations include actual kind/display metadata and credentials,
with strict bijective renaming in the replay driver. The implementation-facing
wrapper rejects retries of an initial launch key after rotation or retirement,
while returning the reserved identity as structured rejection context.

```sh
python3 spec/check_hardening.py --model faults --timeout 60
PYTHONDONTWRITEBYTECODE=1 python3 spec/mbt/generate_faults.py
PYTHONDONTWRITEBYTECODE=1 python3 spec/mbt/replay_faults.py --verify-only
```

Initial checkpoint, checked 2026-10-19: **48/48 TLC checks** comprising eight safe profiles,
21 required fault counterexamples, and 19 coverage witnesses. The witnesses
are exported as source/contract-hashed ITF traces. Another **40 fixed-input checks**
require all nineteen traces to match the correct model and distinguish every
faulty policy through an observable response/state mismatch or deadlock. **22 replay-tool regression
tests pass**, including corrupt observations/receipts, forged receipt history,
missing capabilities, timeouts, stale traces, and duplicate concrete identities.

The expanded identity/retry checkpoint passes **56/56 model checks** and
**48/48 fixed-input checks**, exporting **24 required witnesses**. It adds
initial retry, stale-key rejection, retirement/fresh-allocation and retired
authentication cases, together with retry-ID, reused-ID and stale-enrollment
mutation controls. Identity remains bounded to five transitions; its safe graph
has 26,461 distinct states. The expiry coverage goal also now requires an
expired-only empty delivery, so a live-message acknowledgment cannot satisfy
that witness. The source/configuration/log certificates are retained under
[`d8-identity-policy-v3-model`](../docs/impl/generated-fault-mbt/d8-identity-policy-v3-model/).

The standalone prototype driver sends no expected observations to its port. It
collects receipt history and requires authoritative broker/file observations.
Its custom Python/NDJSON protocol is not generated by the model-interface
compiler. Current replay uses the separate reviewed compiler contracts and
generated Rust bindings under [`mbt/fault_interfaces`](mbt/fault_interfaces/README.md),
with concrete methods calling actual failure hooks and broker operations.
The expanded Rust `--suite all` also requires these fault profiles. Neither
model success nor corpus verification closes an implementation defect. See the
[replay contract and implementation acceptance gate](mbt/faults/README.md).

Each profile has a transition budget (4–12). This is a safety-only test contract,
not unrestricted cross-profile refinement or a liveness proof. Existing
composed protocol and hardening progress obligations remain separate.

## Implementation hardening obligations

[`CooperatorsHardening.tla`](CooperatorsHardening.tla) adds seven separately
checked scenarios for the defects found in the 2026-10-19 review. It adds the
intermediate state the protocol model omits: root ownership, buffered versus
durable writes, historical acknowledgments, message expiration and capacity,
recoverable dead-letter projection, incomplete publication, record sizes,
physical log files, and saved cursors.

There are **17 safety invariants**, plus `TypeOK`, and **two temporal
properties**. `Fault = "none"` selects a proposed policy that satisfies them.
Other fault values deliberately reproduce the reviewed mechanisms and must
produce counterexamples. None of these checks executes or fixes Rust code.

| Obligation | Safety invariant(s) | Meaning |
|---|---|---|
| One writer per root; acquire ownership before replay | `RootWriterExclusive`, `ReplayRequiresOwnership` | At most one owner; every process opening logs for replay/recovery holds ownership. Socket pathname does not define ownership. |
| Acknowledged writes survive failed fsync and recovery | `AcknowledgedRecordsSurvive`, `DurableSequenceValid` | Every historically acknowledged record remains in durable state after recovery; durable sequence positions remain contiguous. |
| Expired messages cannot strand inbox capacity | `InboxAccounting`, `CursorCoversScannedPrefix`, `CursorDoesNotSkipLive`, `ExpiredOnlyCursorProgress` | Returned cursors cover safely scanned records, never skip undelivered live messages, and advance over an expired-only suffix. |
| Dead tasks remain recoverable into the dead-letter log | `DeadLetterAccountedFor`, `DeadLetterHasTerminalSource` | A dead task has either a published dead letter or a durable recovery obligation; each dead letter has a terminal source event. |
| Failed initial publication can be retried | `UnpublishedDocRetryable`, `SuccessfulPublishHasVersion` | An idle document with no published version remains publishable despite an empty file; success requires a published, readable version. |
| Status agrees with durable logs | `StatusHeadMatchesDurableLog` | A status observation reports the actual head, including when the final record is larger than the tail window. Metrics derived from this scan inherit the same obligation. |
| Rotation preserves records and cursors | `RotationBounded`, `RotationPreservesRecords`, `CursorAnchorPreserved`, `CursorReadContinuity` | Files respect the modeled threshold; concatenation preserves the full log; saved cursor anchors remain valid and reads return the complete suffix. |

The progress properties are necessary in addition to safety:

```tla
ExpiredCapacityEventuallyFreed ==
  FullAndAllPendingExpired ~> HasCapacity

DeadLetterEventuallyPublished ==
  TaskTerminal ~> DeadLetterPublished
```

These are explanatory aliases; the executable definitions use the scenario's
state fields. The inbox property assumes weak fairness of the client's wait
and ack actions: the daemon cannot force a stopped client to acknowledge.
The dead-letter property assumes weak fairness of recovery, retries, and
projection, with at most one crash and one failed projection. It does not
promise progress under permanently unavailable storage.

The correct storage policy in this model poisons a writer after failed fsync
and requires recovery before another append. This is one possible way to
satisfy the invariants; safe reconciliation would also be acceptable.
`pendingProjection` is a durable obligation that can be reconstructed from
queue terminal events, rather than a requirement for a new mutable state file.

### Run and expected results

```sh
python3 spec/check_hardening.py
python3 spec/check_hardening.py --scenario storage
# Override the TLC executable if needed:
TLC_BIN=/absolute/path/to/tlc python3 spec/check_hardening.py
```

The runner snapshots the module and writes configs, complete TLC logs,
counterexamples, and model state under a new `/tmp/coop-hardening-*` directory.
It exits nonzero on an unexpected model failure, missing expected
counterexample, tool error, or timeout (default 30 seconds per invocation).
Some environments require permission for TLC's local Java RMI socket.

Checked 2026-10-19 with TLC 2.19: **18/18 checks passed**, consisting of seven
complete safe-model explorations and eleven required counterexamples.

| Safe scenario | Distinct states | Fault controls rejected |
|---|---:|---|
| writer | 5 | concurrent owners; replay before ownership |
| storage | 21 | failed fsync, later acknowledged duplicate sequence, recovery discards acknowledged record |
| inbox | 116 | expired-only cursor does not advance; capacity never freed despite fair wait/ack |
| deadletter | 17 | terminal event lacks a durable recovery obligation; projection never completes after failure/restart |
| document | 4 | failed publication leaves an empty file that blocks retry |
| status | 14 | oversized last record causes a missing reported head |
| rotation | 39 | rotation disabled; reads inspect only the active file |

### Bounds and implementation evidence

Each scenario is checked independently, not as a composed broker refinement.
Bounds are two processes, two attempted storage writes, three inbox/log
records, capacity two, batch size one, rotation threshold two fixed-size
records, and a status tail window of two abstract byte units with record sizes
one or three. Only the dead-letter scenario models an explicit process outage;
the other crash effects are their stated recovery/abort transitions.

The state space is finite by construction; no state constraint truncates the
progress checks. Deadlock checking is disabled because completed scenarios may
terminate and stuttering is allowed. Liveness is still checked, including
stuttering behaviors under the specified fairness assumptions.

Passing these checks validates the proposed policies within these bounds.
It does **not** discharge A1/A2 for the Rust broker, establish their composition,
prove payload/hash integrity, or validate real filesystem persistence and
transport behavior. The existing protocol and MBT models retain their prior
scope and generated bindings. Current implementation qualification separately
records failure-scenario replay and authoritative durable effects. The initial
MBT writer suite exposed competing writers and rejected-startup mutation;
the repaired gates exercise real processes and read-only log observations. See
[`mbt/README.md`](mbt/README.md#multiple-writer-suite). The protocol adapter's
shadow log remains insufficient evidence for the other new storage obligations.
