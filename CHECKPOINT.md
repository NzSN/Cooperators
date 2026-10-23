# Network implementation checkpoint

Date: 2026-10-23. Resumed from `44900f3` on `main` (the 2026-10-22 stop).
The N2 gate was qualified as a working tree based on that checkpoint; final
receipts identify the exact files independently of the publication commit.
The original execution base remains `27d6e901a683b2c651f8bbf9a3be017f3a5b22a2`.

Publication branch: `checkpoint/n2-local-authority-20261023`. Fetch found
`origin/main` at `804781c`, containing independently published N2/N3 work and an
N3b pause. This branch preserves its own qualified N2 source; it does not replace
that newer main checkpoint or qualify a merge with it.

N0 and the source-pinned N1 gate remain historical evidence. The resumed N2
local authority/enrollment/read gate and its exact final source are recorded in
[`N2-authorization/summary.json`](docs/impl/networks-20261022/N2-authorization/summary.json).
This is not complete design 0005 implementation or Internet deployment acceptance.
Resource mutations and remote lifecycle/ACK operations remain disabled.

## Design and execution records

- [Canonical design 0005](docs/design/0005-across-networks.html).
- [Ordered implementation plan](.agents/chatdocs/2026-10-22-across-networks-implementation-plan.html).
- [Execution tracker](docs/impl/networks-20261022/README.md).
- [N2 implementation and evidence](docs/impl/networks-20261022/N2-authorization/README.md).
- [Method inventory](spec/network/methods.json) and [contracts](spec/network/README.md).

The first release remains one shared authority with controlled coop clients.
Autonomous LAN operation has not been selected. Generic remote MCP/OAuth and
N6 federation have separate acceptance gates.

## Resumed N2 changes

- Replaced the N1 prototype with approved policy/enrollment throughout TLS tests.
  Verified native HTTPS CLI, pinned bus identity, HTTP MCP and real UDS dispatch.
- Persisted approved policy revisions and bounded generation history in registry
  V3 with an additional incompatible required feature. Stale approval, removed
  principal restoration and generation rollback fail across restart; uncertain
  policy replacement fails closed until reopen.
- Tested fixed enrollment expiry/retry, persistent budgets, retirement,
  cross-principal denial, credential separation and revocation during waits.
- Reconciled orphan arrival identity, rejected conflicts before format promotion,
  and tested both activation orders and actual process-kill recovery. The pinned
  pre-network binary refused V3 before repairing deliberately torn sources.
- Finished bounded channel reads and authorized document/agent/subscription
  pagination; added exact Host/Origin, input/ID, request-body/deadline and capacity
  controls. Oversized items do not advance cursors. MCP has a framing budget.
- Updated the MBT lockfile offline for HTTPS dependencies. Updated conformance
  fixtures to select HTTP identity in the header and advertise list pagination.
  Removed credential-bearing request parameters from M1/M2 test diagnostics.

## Resume here: N3a

1. Freeze the operation record schema, canonical semantic input/digest and client
   operation journal before implementing writes. Scope operations by bus,
   principal/instance and validity epoch; never use a JSON-RPC ID as dedupe.
2. Implement the atomic receipt core and direct-send crash pilot. Put operation
   identity/result reconstruction into authoritative source records or an atomic
   registry record, never a receipt WAL appended after the effect.
3. Preserve historical event hashes with absent-field omission and install any
   required incompatible feature guard before new source fields are appended.
4. Test concurrent duplicates, conflicting payloads, actual lost replies,
   authoritative effect counts, process kills and restart result recovery on
   unchanged source before enabling that mutation capability.
5. Proceed in order to N3b queue/document/channel and negative/empty result replay;
   N3c lifecycle, cursor/recovery and retention; N4 remote managed hosts/controller
   fencing; and N5 actual network/provider qualification. N6 remains conditional.

The N2 implementation permits approved enrollment and reads. Renewal, ACK,
heartbeat, subscription mutations and domain mutations still require their N3
method gates. Local TLS fixtures and legacy generated replay do not establish
remote mutation durability, new network models, real provider behavior or WAN
readiness. No deployment or remote source transfer was performed.

## Environment and evidence hygiene

Sandbox AF_UNIX/TCP binds return EPERM. Required-socket suites use escalation;
mandatory socket tests must not silently skip. Source manifests identify the
actual dirty files; the checkpoint HEAD alone does not identify qualified code.
The final N2 receipts and `SHA256SUMS` seal retained evidence. Earlier failed and
intermediate runs are preserved with their limited scope. Disposable credential
redactions map original to published log hashes; final diagnostics omit them.

Temporary controls, if still present:

- `/tmp/coop-network-baseline/coop`: pre-network release at `27d6e90`.
- `/tmp/coop-prearrival-network/target/release/coop`: pre-arrival control.
- `/home/lsz/Codebase/Mirrors/.lake/build/bin/mirror` and `model_interface_gen`;
  sibling `MirrorRust` is the local replay dependency.

Their hashes/tool versions are retained in the N2 environment record. Missing
controls must be rebuilt from pinned revisions. No binary, private CA key,
provider credential or user bus is included. The pre-existing untracked
`.projectile-cache.eld` remains outside this work.
