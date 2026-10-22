# N3a durable remote send implementation

Date: 2026-10-22. Implementation base: `bfbf753`.
The send pilot and client recovery path are implemented in the working tree.
**The local N3a qualification gate passed.** The final receipts identify one
unchanged source tree. Subsequent N3b work is paused and unqualified; see the
[checkpoint](../../../../CHECKPOINT.md) for the exact unfinished boundary. This record does not qualify
an actual LAN/Internet deployment, managed remote providers, or a full release.

Navigation: [execution tracker](../README.md),
[checkpoint](../../../../CHECKPOINT.md),
[operation contract](../../../../spec/network/operations.md),
[network design](../../../design/0005-across-networks.html),
[N2 security/read gate](../N2-security/README.md).

## Implemented boundary

The existing N2 authorized reads, waits and approved enrollment remain available.
N3a adds policy-controlled remote `send_message` and scoped `operation_status`.
Remote queue, document and channel mutations remain gated. Ordinary inbox ACK,
heartbeat, credential renewal, retirement, subscriptions and managed-host
activation also remain gated. A successful remote send therefore does not yet
establish a complete remote send → receive → ACK lifecycle.

| Source owner | Responsibility |
|---|---|
| [operations.rs](../../../../src/operations.rs) | Immutable references, normalized semantic digest, scoped admission, quota and receipt index reconstructed from source proofs. |
| [envelope.rs](../../../../src/envelope.rs), [messaging.rs](../../../../src/messaging.rs), [inbox_writer.rs](../../../../src/inbox_writer.rs) | Proof in the authoritative inbox record, shared completion for duplicates, cancellation-safe writer ownership, covering fsync and original receipt recovery. |
| [registry.rs](../../../../src/registry.rs) | Durable incompatible-format guard and fixed operation-epoch lifetime. |
| [gateway](../../../../src/network/gateway.rs), [policy](../../../../src/network/policy.rs) | Explicit exposure, current authority checks, resource-scoped historical access and authenticated discovery. |
| [client journal](../../../../src/transport/journal.rs), [HTTPS transport](../../../../src/transport/https.rs) | Private persisted intent before submission, bounded storage, credential separation and explicit same-ID resumption. |
| [CLI](../../../../src/main.rs), [MCP](../../../../src/mcp.rs) | Journal selection, local inspection, fresh send invocations and explicit recovery tools. |

## Activation and finite limits

Set `"enable_operations": true` in the existing approved network policy and
start the broker with `daemon run --network-config <policy-file>`. This opt-in
defaults to false. Activation durably installs the required registry feature
`durable_operations_v1` before operation-bearing source records are recovered
or written. The pre-operation executable refuses that upgraded root before
repairing its logs. Turning exposure off does not remove the format guard.

The device principal also needs explicit permissions for its sender instance
and intended recipient. Relevant permission entries are:

```json
[
  {"method":"instance_metadata","resources":["agent:<sender-agent-id>"]},
  {"method":"send_message","resources":["inbox:<recipient-agent-id>"]},
  {"method":"operation_status","resources":["agent:<sender-agent-id>"]}
]
```

These are entries for the existing principal's `permissions` array, not a full
policy file. Preserve the approved enrollment, identity and ingress settings
from N2. Use actual stable instance IDs and the existing private credential
references; the examples contain no bearer credentials.

| Limit | Current behavior |
|---|---|
| Operation epoch | Seven days from first durable activation. Restart does not extend it. |
| Initial submission deadline | Immutable `not_after_ms`, at most 24 hours ahead at admission. The native journal also caps it at the advertised epoch expiry. |
| Broker operation capacity | At most 10,000 indexed operation admissions in the epoch; new admission fails before effects when capacity is exhausted. |
| Local journal | At most 1,000 entries and 64 MiB of authoritative JSON state. New intents fail before submission when a bound is reached. |
| Epoch expiry | Current admission, receipt lookup and status reject an expired epoch. Automatic rollover, renewal, receipt retirement and journal pruning are not implemented. |

These bounds do not provide the N3c retention/rollover guarantee. Existing source
proofs and local entries are retained; deleting them is not a supported recovery
procedure for unknown outcomes. Within a valid epoch, a known receipt can be
recovered after its initial submission deadline; an unseen expired reference
cannot become a new write.

## Durable identity and recovery

Each remote send carries `operation: {v: 1, id, epoch, not_after_ms}`. A fresh
explicit invocation allocates a fresh ID even when its message text is identical.
The broker binds it to the bus, stable principal and sender instance. The method,
authorized recipient and normalized semantic input are bound to the source proof.
Changing that intent under the same scoped ID produces `IDEMPOTENCY_CONFLICT`.
Remote durable sends reject the legacy `idempotency_key` field.

The proof is part of the same hash-chained inbox record as the message. Its
receipt index is rebuilt from verified, synchronized source records before
admission. Concurrent identical requests share completion; a disconnected or
cancelled caller does not cancel an admitted write. TTL companion metadata must
meet the inbox commit contract. An uncertain write remains fenced until source
recovery establishes its outcome. Retries return the original receipt and do
not extend the message's TTL or create another inbox effect.

The client journal stores the exact reference and credential-free intent before
submission. Its dedicated directory is private (`0700`); files are private
(`0600`) and symlink/hardlink substitutions are rejected. Interprocess locking
serializes updates. File sync, atomic replacement and directory sync establish
the saved state; reopening synchronizes recovered state and directory ancestry
before an intent can be submitted. Local persistence or recovery-sync failures
prevent submission.

Only an established successful receipt marks a journal entry completed. Missing
responses and unannotated RPC errors leave it unresolved, because a denial may
arrive after a committed send if authority changed before receipt delivery.
Current credentials are supplied again on resume and are never stored in the
journal. Resume verifies the endpoint, bus and agent instance and uses the
original immutable reference. Current permission checks still apply to the
stored recipient. No mutation is retried automatically.

## CLI usage

Assume `team` is an existing profile with a private credential reference, the
broker policy enables operations, and the profile identifies the sender
instance. Select a dedicated local journal directory:

```sh
coop --profile team --operation-journal "$HOME/.local/state/cooperators/team-operations" \
  send --to '<recipient-agent-id>' --kind text --text "Ready for review"

coop --operation-journal "$HOME/.local/state/cooperators/team-operations" operations list

coop --profile team --operation-journal "$HOME/.local/state/cooperators/team-operations" \
  operations resume '<operation-id>'
```

Replace angle-bracket placeholders before running these examples. `operations
list` reads local saved intents and outcomes without contacting the broker.
After an unknown outcome, use the ID reported in the error or in that listing.
Repeating `send` creates another operation; `operations resume` preserves the
saved operation. Resume requires the same endpoint, bus and sender instance,
with currently valid credentials and permissions. `--idempotency-key` is not
accepted for remote journaled sends.

For direct endpoint selection, the existing `--endpoint`, `--expected-bus-id`
and `--ca-cert` options remain available. The profile form above avoids putting
credentials in command arguments. There is no implicit journal destination:
remote sends require `--operation-journal DIR`.

## MCP usage

Launch the remote stdio facade with the existing instance and the same journal:

```sh
coop --profile team --operation-journal "$HOME/.local/state/cooperators/team-operations" \
  mcp-stdio --resume
```

The remote tool catalog uses authenticated broker discovery. `coop_send_message`
is offered when the broker advertises it and a local journal is configured.
Each fresh tool invocation journals a new operation. After an unresolved
outcome, call the local recovery tool with only its saved ID:

```json
{"name":"coop_resume_operation","arguments":{"id":"<operation-id>"}}
```

The tool obtains intent and reference from the journal and supplies the current
captured credential. It does not need message text to be reconstructed by the
agent. `coop_operation_status` queries the broker using the complete saved
`operation` reference; it requires current scoped status permission and, for a
known send, permission to its stored recipient. Status does not submit an unseen
operation or replace an unknown operation with a fresh one.

Direct HTTP MCP clients have no native local journal. Their send schema requires
an explicit complete `operation` reference, and they are responsible for
persisting that reference and intent before the first request. The HTTP bearer
identity remains authoritative; request arguments cannot substitute another
credential.

## Qualification record

All three final N3a receipts have identical source manifests and report the
source unchanged. The first injection attempt remains failed; a reproduced
root-lock reacquisition failure was isolated by retaining one root lease in the
registry recovery control. Production durability code was unchanged by that fix.

| Gate | Expected retained receipt | State |
|---|---|---|
| Normal suite with required sockets | `N3a-run-02/normal/receipt.json` | 257 passed; one pre-existing ignored test; zero skips |
| Injection suite with required sockets and crash controls | `N3a-run-02/injection/receipt.json` | 330 passed; one pre-existing ignored test; zero skips |
| Generated model-trace replay | `N3a-run-02/generated/receipt.json` | 63 traces and rotation crash controls passed |

Focused controls are implemented in
[operation receipts](../../../../tests/operation_receipts.rs),
[operation journal](../../../../tests/operation_journal.rs) and
[operation crash/transport tests](../../../../tests/operation_crash.rs). They
cover semantic conflicts, concurrent duplicates, interruption and cancellation,
actual process kills around source sync, historical hash compatibility,
old-executable refusal, journal recovery-sync failures, and a real local TLS
ingress that drops a reply after the upstream committed. CLI and MCP controls
exercise fresh intents and explicit recovery through their actual facades.

Large remote MCP read results retain their full `structuredContent`; if the
duplicate text representation exceeds the response limit, `content` is empty.
Controlled clients must consume `structuredContent` for these responses. Actual
provider compatibility with this behavior remains an N5 gate.

Local TLS fixtures and subprocess crash evidence establish only their exercised
source and topology. Actual cross-machine LAN/WAN routing, deployed ingress,
remote managed providers, complete remote ACK recovery and the remaining N3b,
N3c, N4 and N5 gates remain separate work.
