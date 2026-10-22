# N2 principal, policy and bounded-read implementation

Date: 2026-10-22. This record describes the resumed N2 source implementation.
The local authorization, migration and bounded-read gate **passed** on one
unchanged source tree: 232 normal tests, 288 injection tests and 63 retained
generated traces. Resource mutation and credential-renewal gates remain later work.
The earlier N1 receipt applies only to its recorded source and does not qualify
this tree.

Navigation: [execution tracker](../README.md),
[checkpoint](../../../../CHECKPOINT.md),
[canonical design 0005](../../../design/0005-across-networks.html),
[ordered implementation plan](../../../../.agents/chatdocs/2026-10-22-across-networks-implementation-plan.html),
[method contracts](../../../../spec/network/README.md).

## Implemented scope

The implementation serves one authoritative bus with administrator-approved
device principals and distinct enrolled agent instances. Network activation is
explicit through `daemon run --network-config`. The broker's HTTP backend must
bind loopback for a same-host TLS ingress; startup fails if the requested remote
backend cannot bind. The client verifies HTTPS and can pin the bus identity
reported by authenticated `/v1/info`.

| Source owner | Implemented responsibility |
|---|---|
| [registry.rs](../../../../src/registry.rs), [arrival migration](../../../../src/arrivals/migration.rs) | Registry V3 activation, stable bus and operation-epoch metadata, principal generation history, scoped instance grants and incompatible-version refusal. |
| [network/policy.rs](../../../../src/network/policy.rs), [network/runtime.rs](../../../../src/network/runtime.rs) | Approved enrollment, instance budgets, expiry/generation authentication, method/resource permissions, policy replacement and request/poll capacity. |
| [network/gateway.rs](../../../../src/network/gateway.rs) | Shared scoped dispatch, `/v1/info`, `/v1/enroll`, `/v1/rpc`, request admission, typed read parameters, bounded pages and reauthorization before returning results. |
| [server.rs](../../../../src/server.rs), [mcp.rs](../../../../src/mcp.rs) | Scoped identities use the shared policy boundary through UDS and MCP as well as native HTTP; HTTP admission and remote tool exposure. |
| [channels.rs](../../../../src/channels.rs), [messaging.rs](../../../../src/messaging.rs), [docs.rs](../../../../src/docs.rs), [presence.rs](../../../../src/presence.rs), [arrival API](../../../../src/arrival_api.rs) | Domain selection applies item/byte bounds before cloning returned records; list selection applies current resource permissions first. |
| [transport](../../../../src/transport/mod.rs), [client.rs](../../../../src/client.rs) | Verified HTTPS, bounded response decoding, credential profiles, bus-ID verification and explicit unknown mutation outcomes. |

The N1 `remote_read_only` prototype has been removed. TLS fixtures activate a
real `NetworkPolicy` and enroll scoped instances through the approved path.
Local unscoped domain APIs retain their existing behavior; using a scoped
credential over UDS does not bypass its network policy.

## Identity and migration

Activation installs the incompatible `network_authority_v1` registry guard
before source-log recovery. V1/V2 roots retain their existing identities while
upgrading to V3. Existing arrival bus identity is adopted, including recoverable
arrival metadata from an interrupted earlier upgrade. Enabling arrivals after
network activation preserves the same bus identity and network feature.
Conflicting or malformed metadata fails before log repair.

An activated V3 root requires an approved policy when reopened. Persistent
principal history records generation floors and removal state, so removing and
re-adding a principal cannot revive grants from its old generation. The
operation epoch is metadata for subsequent operation work; N2 does not implement
durable mutation receipts merely by creating that field.

Approved enrollment serializes admission under the registry write lock and
rechecks current policy at that boundary. Issued-identity budgets include
retired identities. Retrying the same valid launch proof returns the original
instance; it does not allocate another instance or extend credential expiry.
A proof cannot transfer to a different principal or alter the original launch
metadata. Instance expiry is bounded by both the configured instance TTL and
the device principal's expiry. Remote credential renewal remains outside this
gate.

## Policy and revocation semantics

Permissions identify a method and exact resource or explicitly configured
trailing-star prefix. Unlisted methods and resources are denied. Resource names
are resolved under the authenticated identity; request-body credentials cannot
substitute another HTTP bearer identity. Native HTTP rejects `params.token`;
approved enrollment still carries its launch proof in `launch_key`. Duplicate
Authorization headers are rejected rather than selecting a
credential by header order.

Host matching requires one configured authority, with case-insensitive hostname
and exact port matching. Origin may be absent for non-browser clients; a supplied
Origin must match the explicit allowlist. Duplicate Host/Origin fields and
ambiguous authority forms are rejected. Metrics are unavailable on the activated
network interface; the existing health endpoint remains a separate status route.

Policy replacement uses the registry write lock to persist generation/removal
history before publishing the replacement policy. Capacity changes require a
restart. Removing and re-adding a principal preserves any still-held semaphore
permits; replacement cannot manufacture extra request capacity. A persistence
failure that leaves authority uncertain fences the runtime until it is reopened.
Replacement is an explicit runtime operation, not an automatic policy-file
watcher or a remote administration API.

An operation admitted before revocation may already have begun its domain read.
The gateway authenticates and authorizes again before returning any successful
result. During a long poll, policy-change notifications trigger reauthorization;
a 100 ms check also observes registry retirement, credential rotation and
wall-clock expiry. A revoked wait returns an error and releases its permits
without waiting for its full requested poll interval. The 100 ms interval is a
check cadence, not a measured latency guarantee. A replacement that still allows
the operation can leave the same read future running.

List pages are checked against current resource permissions again before return.
If an item lost permission during construction, the entire page is rejected,
including its continuation cursor. Arrival waits reject a page containing a
resource outside current subscription permission; they do not silently skip that
notice or advance its cursor.

## Bounds and continuation contracts

| Boundary | Current behavior |
|---|---|
| Native request/response body | Maximum 4 MiB; JSON byte accounting includes escaping. Native request IDs are capped at 1 KiB of encoded JSON. |
| HTTP ingress | Connection admission follows policy; headers have a 10-second deadline, 32-header cap and 64 KiB parser buffer. Request-body collection has a 10-second deadline. Activated HTTP disables idle keep-alive. |
| Per-principal admission | Concurrent requests and long polls have separate policy limits. Authenticated native body collection consumes request capacity before reading the body. Invalid or unauthorized read parameters do not obtain a long-poll permit. |
| Global long polls | At most `max_connections - 1` polls are admitted across principals. |
| Remote waits | `timeout_s` and `wait_s` are non-negative integers no greater than 25 seconds. Inbox/arrival waits default to 25 seconds; channel reads default to zero. |
| Remote pages | `limit` is an integer in `1..=64`, with default 64 where available. Inbox batches additionally respect their configured batch limit. |
| Domain response construction | Record selection reserves 8 KiB below the 4 MiB ceiling for surrounding response fields. Oversized first items fail with `RESOURCE_EXHAUSTED` rather than returning a cursor past them. |
| Document blobs | Raw reads are bounded, combined metadata/body budget is checked, and escaped JSON string size is counted before returning the body. |
| MCP result wrapper | The encoded tool-result wrapper has an additional byte check. A result that exceeds it returns `RESOURCE_EXHAUSTED`; callers can reduce list/channel/arrival page size. |

Optional read fields have strict types. For example, `"false"` is not a boolean,
`"1"` is not a numeric limit/version, and null does not silently select a default.
Unknown read parameters are rejected. Native HTTP distinguishes invalid input
(400), authentication (401), permission (403), admission exhaustion (429), body
size (413), request-body timeout (408) and internal failure (500). Application
errors remain available in the JSON-RPC error object.

`list_docs`, `list_agents` and `list_arrival_subscriptions` accept optional
`limit` and object `cursor`, and return nullable `next_cursor`. A null continuation
marks the end of the currently observed eligible rows. Their cursor is a
versioned object bound to bus, enrolled instance, principal generation, method
and normalized filter. Reusing a cursor for another instance, method or filter
fails. Changing only the page limit is permitted.

Pages use **keyset pagination, not a snapshot**: documents are ordered by name,
agents by routing name and subscriptions by subscription ID. The next request
starts strictly after the last returned key. Concurrent additions before that
key will not appear in that traversal; later additions may appear, and removed
or newly forbidden rows disappear. A caller that needs a fresh inventory starts
again without a cursor. List cursors do not acknowledge data or create a stored
snapshot, and current authorization is enforced on every request.

Authorization precedes candidate selection and cloning. Documents retain at most
`limit + 1` candidate keys; agent/subscription lists iterate their ordered source
indexes and clone only accepted rows. Inaccessible large metadata does not fill
a caller's page or trigger its response-size rejection. The bounded accumulator
counts borrowed serialized views before copying payloads.

Channel and inbox cursors beyond the committed head are rejected by the remote
read path. Byte-limited channel/inbox pages stop before the first unreturned live
record. Existing inbox handling may advance across expired records; it does not
skip an unreturned live message. Arrival cursors retain their existing bus and
identity binding. None of these peek operations acknowledges consumption.

## Verification records

The resumed campaign is under
[`resume-20261022/`](resume-20261022/). Its
[environment manifest](resume-20261022/environment-before.json) and
[control binary manifest](resume-20261022/control-binaries.json) identify the
actual tools and old-executable controls. The control paths are local
prerequisites, not portable acceptance evidence.

| Gate | Current record |
|---|---|
| Focused bounds checks | `tests/network_bounds.rs`: seven tests passed during integration, including ACL-before-pagination, cursor scope, escaped-byte limits and duplicate Authorization rejection. This focused result does not replace a final source-pinned regression receipt. |
| Required-socket normal regression | **232 passed**, zero failures/skips; one pre-existing ignored performance test. [Receipt](resume-20261022/normal-run-02/receipt.json). |
| Injection regression and migration crash controls | **288 passed**, zero failures/skips; one pre-existing ignored performance test. Includes real old-binary refusal and six registry activation SIGKILL controls. [Receipt](resume-20261022/injection-run-01/receipt.json). |
| Generated replay | **63 traces passed**: 34 protocol, five writer, 24 fault; rotation crash controls passed. [Receipt](resume-20261022/generated-replay-run-02/receipt.json). Existing generated traces cover their declared models; this is not a new network authorization model. |

[Network security tests](../../../../tests/network_security.rs) cover isolated
principals, enrollment budgets/retries, expiry, generation history, revocation,
policy persistence failure and capacity replacement.
[Migration tests](../../../../tests/network_migration.rs) cover upgrade orders,
metadata refusal, policy-required reopening and actual old-binary refusal.
[Bounds tests](../../../../tests/network_bounds.rs) cover typed reads and bounded
authorized response construction.
[TLS transport tests](../../../../tests/remote_transport.rs) exercise verified
transport, bus pinning, facade authorization and actual CLI/stdio behavior using
disposable loopback fixtures.

The [summary](resume-20261022/summary.json) verifies that all three final receipts
carry identical source manifests, unchanged during execution, with required
sockets. The generated replay also pins the external MirrorRust source and
Mirrors tools in the environment manifest. The MBT lockfile was refreshed
offline for the new HTTPS graph and the current sibling MirrorRust v1.0.0.
Formatting, whitespace and doctest checks passed (there are zero doctests).

`normal-run-01` remains a failed receipt: it found two local MCP compatibility
failures. The fix preserves local unscoped identity switching while preventing
scoped stdio identity substitution. The HTTP conformance driver now selects the
explicit transcript identity in both its header and body. The earlier generated
run passed its own source but is superseded by run 02. Disposable credentials
printed by old test helpers in the failed log were pseudonymized; original and
published hashes are retained in [the redaction record](resume-20261022/credential-redactions.json).
The helpers now omit bearer tokens from diagnostic logs.

## Remaining boundary

Remote resource mutations, subscription creation/removal, ACK, heartbeat,
retirement and administrative methods remain gated off even when a permission
pattern would otherwise allow them. Approved enrollment is the explicit identity
issuance path. HTTPS and HTTP MCP catalogs advertise reads only. The private UDS facade retains
its local catalog; scoped calls through it still pass the same authorization and
mutation gates.

N3a/N3b/N3c must supply durable mutation identity, replay of negative/empty
results, lifecycle/retention and recovery contracts before mutation retries can
be enabled. N4 controller/managed-host fencing and N5 actual network/provider
qualification remain separate work. N6 federation remains conditional.

This implementation record claims no public deployment, cross-machine acceptance,
Internet readiness, generic OAuth MCP interoperability or autonomous-site
operation. The passing local TLS fixtures and N2 regression qualify their recorded source
and scope only.
