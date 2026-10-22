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
