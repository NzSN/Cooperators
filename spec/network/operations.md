# Durable remote operation contract (N3)

Implementation base: `bfbf753`. This contract precedes the new write paths;
acceptance and exposure are recorded by method, not inferred from this document.

## Identity and source evidence

A remote durable mutation carries `operation: {v: 1, id, epoch, not_after_ms}`.
`id` is a fresh ULID for a new caller intent. `epoch` is the broker's advertised
operation epoch; `not_after_ms` is an immutable submission deadline no more than
24 hours ahead at first admission. The server binds the reference to its bus,
stable principal and agent instance. Method and canonical semantic input are
hashed with SHA-256; tokens, HTTP/JSON-RPC correlation IDs and the `operation`
field are excluded. Object keys are recursively sorted; array order and numeric
representations remain significant. Explicitly normalized method defaults and
aliases are recorded in the per-method implementation.

Reusing a scope/ID with a different reference, method, resource or semantic
digest fails with `IDEMPOTENCY_CONFLICT`. Current authentication and permission
checks apply to both original execution and historical receipt access. Historical
lease results do not grant a current lease or authorize external effects.

A successful operation proof lives in the same authoritative source record as
its effect. The proof contains the bound identity/reference, normalized-input
digest, method, authorized resource and original outcome information. A receipt
index is reconstructed from verified and synced source records before admission.
It is never a post-effect WAL whose absence permits another mutation. Duplicate
or conflicting source proofs make recovery fail closed.

A new `durable_operations_v1` required registry feature must be durable before
operation-bearing records can be written or recovered. Network operations are
explicitly enabled by policy; local unscoped APIs and absent optional fields keep
their historical behavior/hash representation. The pre-operation executable must
refuse upgraded roots before repairing logs.

## Admission, failure and recovery

Identical concurrent submissions share one completion. Admitted writes outlive
request cancellation. Success is published only after the source's covering
fsync and required projections. I/O uncertainty fences the affected admission
until authoritative reopen/replay; it never resets an operation to unseen.

Known receipts are recovered before reevaluating changed domain state, but only
after current authority permits their stored resource. An unseen reference past
its deadline/validity epoch is rejected and cannot become a fresh write. The
initial epoch allows at most 10,000 admitted operations; quota exhaustion rejects
new work before effects. Results/replay protection and epoch lifecycle receive
separate N3c acceptance before retention or rollover is advertised.

Admitted terminal no-effect outcomes (including an empty queue claim and selected
CAS failures) are source records in a dedicated outcome log under the same domain
admission serialization. Authentication, authorization and malformed-input
rejections are pre-admission failures. A committed effect with unfinished derived
work is pending recovery, not a terminal no-effect error.

## First pilot and client journal

N3a enables direct send only after its gate. It rejects the legacy
`idempotency_key` field in remote durable sends: legacy sender/key aliases do not
supply the semantic proof for a new operation. Original message ID, sequence,
timestamp and receipt fields remain identical on retries/restart. TTL metadata
must meet the existing inbox commit contract; it is part of normalized intent.

The client persists the exact reference and credential-free intent in a private,
atomically replaced, fsynced local journal before submission. Fresh explicit
invocations create fresh operation IDs; an explicit resume reuses the original
reference and intent. A transport interruption records an unresolved result and
never creates a replacement operation. Resume supplies current credentials from
the selected profile/session; bearer values are never journaled. Endpoint/bus
and instance binding must be checked before replay. Concurrent journal updates
are serialized and finite bounds reject new entries before submission.

## Ordered acceptance

1. N3a: direct sends, conflict/concurrent duplicate, response loss, cancellation,
   real crash/restart, historical hashes and old-executable refusal.
2. N3b: channel, queue and document families; original lease expiry/fence,
   terminal empty claims, CAS results and derived dead-letter/arrival obligations.
3. N3c: issuance/renewal/retirement/subscriptions, scoped status, finite retention
   with durable replay protection, local ordinary-ACK recovery, authority restore.
4. N4/N5: remote controllers/managed hosts, models and real topology/provider
   acceptance. Local HTTPS evidence does not qualify a LAN or Internet deployment.
