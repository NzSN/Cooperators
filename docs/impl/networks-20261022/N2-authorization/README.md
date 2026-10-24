# N2 local authority, enrollment and read gate

Resumed 2026-10-23 from checkpoint `44900f3` on `main`. The implementation was
qualified as a working tree based on that checkpoint. Use the final receipts'
file hashes to identify the qualified source independently of publication HEAD. Design 0005 and its ordered implementation plan remain
unchanged. The selected scope is one authoritative bus with controlled clients.

Published separately on `checkpoint/n2-local-authority-20261023`: fetch found
`origin/main` at `804781c` with independently published N2/N3 work and a later
N3b checkpoint. These receipts qualify this branch's files, not a merge with
that main implementation.

This gate covers approved device enrollment, scoped instance reads, policy
replacement/revocation, root migration and bounded ingress/results. Resource
mutations, lifecycle renewal, ACK and managed-host/controller work remain gated
for N3/N4. Local TLS/UDS success is not cross-machine or Internet qualification.

## Source and evidence

Final gate receipts and aggregate status are listed in `summary.json`. Each
receipt records the actual command, exit status, source manifest before/after,
counts, skip detection and log hash. `environment.json` records tools and hashes
of the retained pre-network/pre-arrival controls and model tools. All disposable
socket/process tests ran through sandbox escalation because sandbox binds return
EPERM. No live user root, provider credential or provider session was selected.

The full normal suite includes real TLS native CLI, approved enrollment,
profile bus-ID checks, HTTP MCP header identity and UDS policy tests. Injection
adds real process kills before/after registry replacement, uncertain policy I/O
and the existing crash controls. Generated replay covers the existing 34
protocol, five writer and 24 fault traces, plus two real rotation crash controls;
it is regression evidence and does not introduce a network-operation model.

`full-normal-run-01` retained two M3 failures: the old catalog contract omitted
pagination and the HTTP fixture selected a different body identity instead of
changing its header. The fixture now selects the intended transport bearer;
separate negative tests require body credential substitution to fail.
`focused-run-01` retained one stale recipient-name fixture failure. Its older
harness reported zero failed tests because it counted only successful test-suite
summaries; the log shows 28 passed and one failed. The harness now counts both
successful and failed suite summaries. `generated-replay-01` failed because the
separate MBT lockfile lacked the HTTPS dependency graph; its offline update and
compile are retained in `replay-lock-update.log`.

Intermediate passing receipts predate final fixes and are not final-tree
acceptance. Old M1/M2 diagnostics emitted disposable credentials in three logs.
Those values were replaced with stable pseudonyms; `credential-redactions.json`
maps original receipt log hashes to published hashes. The final M1/M2 diagnostics
omit request parameters, so final receipts hash their published logs directly.

## Implemented boundaries

- Removed the N1 `remote_read_only` prototype. TLS fixtures use `NetworkPolicy`
  and approved enrollment; `/v1/enroll` is the only device-credential entry point.
- Persisted the approved policy revision and bounded principal generation history
  atomically in registry V3 under `network_policy_revisions_v1`. Stale or changed
  same-revision approval cannot restore authority on restart. Uncertain policy
  I/O fences remote authentication until an actual reopen.
- Preserved stable bus/operation identities, adopted orphan arrival identity,
  rejected conflicting metadata before arrival promotion, and required approval
  on every network-root reopen before source repair. The pinned pre-network
  executable actually refused V3 without changing registry/source bytes.
- Preserved launch retry identity/expiry and persistent issued-instance budgets;
  rejected cross-principal launch reuse and device credentials as launch proofs.
  Retired/expired scoped proofs cannot fall back to local provisioning over UDS.
- Applied current resource policy on native/UDS/MCP reads and again after waits.
  Unready mutations fail before resource lookup/effects. HTTP body credentials
  cannot replace header identity; metrics and administration stay private.
- Matched full Host authorities including ports, bounded Origin/header input,
  request bodies/IDs, body deadlines, principal admission and poll concurrency.
- Selected inbox/channel byte pages before cloning; built authorized ordered
  document/agent/subscription pages with instance/query-scoped continuations.
  Oversized single items fail without advancing cursors. MCP reserves capacity
  for its duplicated structured/text representation.

See `spec/network/README.md` for the executable contract, revision rules and
pagination semantics. No remote mutation retry or deployment acceptance follows
from this gate. Resume at N3a's atomic operation/receipt pilot.
