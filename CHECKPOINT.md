# Network implementation checkpoint

Date: 2026-10-22. Resumed from checkpoint commit
`44900f39e6d891da97f486fe275e72a413cbb952` on `main`.

**N2's local authorization, migration and bounded-read gate passed.**
Qualification ran on the working tree based on that commit; acceptance is bound
to the exact source manifests, not to the base Git HEAD. N3a is the next wave.
This is not an accepted Internet deployment or the complete design 0005 release.

## Design and execution records

- [Canonical design 0005](docs/design/0005-across-networks.html).
- [Ordered implementation plan](.agents/chatdocs/2026-10-22-across-networks-implementation-plan.html).
- [Execution tracker](docs/impl/networks-20261022/README.md).
- [N2 implementation and contracts](docs/impl/networks-20261022/N2-security/README.md).
- [Final evidence summary](docs/impl/networks-20261022/N2-security/resume-20261022/summary.json).
- [Method inventory](spec/network/methods.json) and
  [retry/format obligations](spec/network/README.md).

The first release remains one shared authority with controlled coop clients.
Autonomous LAN operation has not been selected. Generic remote MCP/OAuth
interoperability and N6 federation retain separate gates.

## Results actually obtained

| Gate | Result |
|---|---|
| N2 normal suite | **232 passed**, zero failed/skipped; one pre-existing ignored performance test |
| N2 injection suite | **288 passed**, zero failed/skipped; one pre-existing ignored performance test |
| Generated replay | **63 traces passed**: 34 protocol, five writer and 24 fault; rotation crash controls passed |
| Source integrity | All three final receipts contain identical source manifests and report source unchanged during execution |
| Additional checks | Formatting, whitespace and doctests passed; zero doctests exist; external model tools/sibling source unchanged |

All final suites used offline locked dependencies and required sockets. Exact
commands, source hashes, logs, environment and control binaries are retained in
[the evidence directory](docs/impl/networks-20261022/N2-security/resume-20261022/).
The generated replay validates its existing declared models; it is not a new
network authorization model.

The first normal run remains **failed**: two local MCP identity-switch
compatibility cases exposed an overly broad credential-binding change. The
corrected source preserves unscoped local behavior and fences scoped sessions.
The HTTP conformance driver selects the same explicit principal in its header
and body. Final normal/injection/replay runs all use that corrected source.
Disposable fixture credentials in the earlier failed log were pseudonymized,
with original/published hashes retained; test diagnostics now omit tokens.

Historical N0/N1 receipts retain their original source boundaries. No live
provider, user bus, cross-machine deployment or installed binary was qualified
by this resumed gate.

## Completed N2 scope

- Approved enrollment over verified HTTPS with pinned bus identity; native,
  HTTP MCP, stdio and UDS scoped calls share the authorization boundary.
- Registry V3 activation adopts existing arrival identity in either upgrade
  order, recovers orphan metadata, rejects conflicts before log repair and
  requires policy on every reopen. Actual pre-network executables refuse
  upgraded roots without changing durable bytes.
- Durable principal generation/removal history prevents stale grant revival
  across restart. Uncertain registry replacement fences further operations.
  Registry failure controls and six activation SIGKILL cases passed.
- Enrollment budgets, immutable retry metadata, expiry and final-token checks;
  policy changes and retirement/rotation/expiry cancel outstanding waits.
- Exact Host/Origin and single Authorization headers, bounded admission and
  bodies, private metrics, typed read arguments, authorized bounded pages,
  scope-bound cursors, and MCP response-wrapper limits.
- The prototype `remote_read_only` path is removed. TLS fixtures now exercise
  approved policy/enrollment; scoped stdio cannot replace its captured identity.

Remote resource mutations, ACK, heartbeat, subscription mutation, retirement
and credential renewal remain gated pending their durable lifecycle contracts.
The private UDS catalog remains the local catalog; scoped dispatch still enforces
network permissions and mutation gates.

## Resume here

1. Start **N3a** from the implementation plan: define durable operation identity,
   authoritative effect/receipt binding, replay-derived lookup, and a persisted
   client journal. Implement the send pilot with real lost-response and crash
   controls before exposing any remote mutation or automatic retry.
2. Continue **N3b** only after N3a acceptance: queue/document/channel mutations,
   including persisted negative and empty-claim results.
3. Continue **N3c**: lifecycle operations and credential renewal, ACK/recovery,
   retention/compaction, and restart behavior. N2 launch retries do not extend
   instance expiry and are not a general mutation-receipt mechanism.
4. **N4** remote managed hosts, exact provider binding and controller epochs;
   **N5** actual network/provider and deployment qualification remain pending.
   **N6** federation remains conditional on the autonomy requirement.

## Environment and local prerequisites

Required UDS/TCP/TLS gates used escalation because sandbox socket binds can fail.
Missing lockfile-pinned crates were restored before offline qualification. The
MBT lockfile now includes HTTPS dependencies and the current sibling MirrorRust
v1.0.0. Tool/source hashes are retained with the gate.

Rebuilt controls, if still present:

- Pre-network `27d6e901a683b2c651f8bbf9a3be017f3a5b22a2`:
  `/tmp/coop-network-prenetwork-20261022/target/debug/coop`.
- Pre-arrival `edcbc92372c9efc45c1a83c31f7d23d8ea11be64`:
  `/tmp/coop-network-prearrival-20261022/target/debug/coop`.
- Set `COOP_PRENETWORK_BINARY`, `COOP_PREUPGRADE_BINARY`,
  `COOP_REQUIRE_NETWORK_MIGRATION=1`, `COOP_REQUIRE_UDS=1`, and
  `COOP_REQUIRE_REMOTE=1` for full migration/socket gates.
- Generated replay uses `/home/nzsn/Repos/Mirrors/.lake/build/bin/mirror` and
  `model_interface_gen`, with sibling `/home/nzsn/Repos/MirrorRust`.

These paths are local prerequisites, not portable acceptance evidence. Rebuild
missing controls from the retained exact revisions and source manifests.
