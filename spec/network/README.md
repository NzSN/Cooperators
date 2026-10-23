# Network execution contracts

`methods.json` classifies every current broker route and MCP alias for design
0005. The matrix is a contract and an explicit exposure inventory. It does not
enable remote mutation retries. Each method needs its own persistence and
authorization gate before the remote listener advertises that capability.

The first implementation release uses one authoritative bus and controlled
coop clients. Autonomous-site federation and generic OAuth MCP clients have
separate acceptance gates in the implementation plan.

## Source-format rules

1. Remote activation is explicit and holds root ownership before any mutation.
   Adopt an existing arrival bus ID; reject conflicting identity metadata.
2. Install an incompatible registry feature/version guard before new source
   fields can be appended. Old executables must refuse before replay/repair.
3. New optional fields must preserve the hash representation of historical
   records. Keep immutable old-record fixtures and test each crash boundary.
4. Operation identity belongs in the authoritative mutation or an atomically
   replaced registry record. A receipt WAL written after an effect is not a
   substitute for source evidence.
5. A claim that observed an empty queue has a terminal result for that operation
   identity. It cannot select later work when retried. Persist its no-effect
   outcome under the same admission serialization.
6. Result compaction must retain replay protection. Rejected/expired unknown
   operation epochs cannot later become fresh writes.
7. Ordinary inbox ACK remains volatile and heartbeat remains soft state until
   an explicitly versioned capability changes those contracts.

## Gate observations

Count committed source effects independently of the implementation's receipt
index. Test identical and conflicting duplicates, concurrent requests, lost
responses, actual process crashes, restart, expiry, token renewal/revocation,
and receipt retention. Record partial or unavailable gates as such.

## N2 authority and read gateway

Network mode requires an administrator-supplied private `--network-config` on
activation and every reopen. The HTTP backend binds loopback behind a same-host
TLS ingress; it does not trust forwarding headers for identity. Device bearer
values are supplied separately; the policy contains only their SHA-256 hashes.
Only `/v1/enroll` accepts a device credential. Native RPC and HTTP MCP require an
enrolled instance credential. Scoped credentials retain these checks over UDS
and stdio. The local UDS interface remains an operator interface protected by
root/socket filesystem ownership.

The V3 registry atomically retains the approved policy and principal generation
history under the additional `network_policy_revisions_v1` required feature.
`revision` defaults to 1 for first activation and must increase for every policy
change. Reopening with the identical policy is allowed; stale or changed policies
at the same revision fail before source recovery. Removing and later restoring
a principal, or extending an already expired principal, requires a newer
principal `generation`. Increasing generation revokes existing instance grants.
Rotating the device token alone changes enrollment authority; it does not rotate
issued instance credentials. Runtime replacement persists approval before new
admission; uncertain registry I/O fences remote authentication until reopen with
the policy actually retained on disk. Capacity changes require restart. The
principal history is capped at 4,096 IDs and cannot be reset by restart.

Instance grants have a fixed expiry. Replaying enrollment preserves the original
identity and expiry and cannot revive an expired, rotated or retired credential.
The issued-instance budget includes retired and expired identities. Credential
renewal, resource mutations, ACK, heartbeat and subscription mutations are still
gated pending their N3 contracts. Enrollment's launch proof is client-generated
and persisted only as a digest. Generic OAuth MCP remains a separate gate.

`list_agents`, `list_docs` and `list_arrival_subscriptions` return at most 64
authorized rows, accepting `limit` and an object `cursor`. Their `next_cursor`
is query, bus, principal, generation and instance scoped. Rows are ordered by
routing name, document name or subscription ID respectively. Continue until an
empty page returns null `next_cursor`; cursors are lexical continuation points,
not snapshot isolation. Permission filtering occurs before row construction and
again before return. An arrival notice page containing any currently unauthorized
resource is rejected without ACK or cursor advancement.

Remote waits are capped at 25 seconds. Native request/response framing is capped
at 4 MiB; inbox/channel pages are selected within the byte budget before records
are cloned. A single oversized item returns `RESOURCE_EXHAUSTED` without advancing
the cursor. MCP reserves additional space for its structured and text copies and
may reject a result that fits the native response budget. Request IDs are capped
at 512 encoded bytes. HTTP bodies have a 10-second read deadline and consume a
principal request slot while being read; polls have a separate lower capacity.
Host authorities match exact ports, Origins require explicit approval, and body
credentials cannot replace the authenticated HTTP identity. Native RPC errors use
401 for authentication, 403 for policy/exposure denial and 429 for exhausted
capacity/budgets; ordinary domain and JSON-RPC errors retain their RPC envelope.
