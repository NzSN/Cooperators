# Network implementation checkpoint

Date: 2026-10-22. Implementation base: `bfbf7534c3ab63a5756321070413415e917e5464`
on `main`.

**Implementation is paused at the user's explicit STOP instruction.** All workers
were interrupted; no Cooperators test/build processes remained at the stop audit.
The user requested this checkpoint, followed by commit and push. Do not resume
implementation until requested.

**N3a passed its local source gate. N3b is partially implemented and unqualified.**
The current source includes unfinished N3b review repairs and a stale regression
assertion. This checkpoint is a work-in-progress publication, not release
acceptance, an installed-binary qualification, or an actual LAN/Internet deployment.

## Navigation

- [Design 0005](docs/design/0005-across-networks.html).
- [Ordered implementation plan](.agents/chatdocs/2026-10-22-across-networks-implementation-plan.html).
- [Execution tracker](docs/impl/networks-20261022/README.md).
- [N3a implementation and usage](docs/impl/networks-20261022/N3-operations/README.md).
- [Accepted N3a receipts](docs/impl/networks-20261022/N3-operations/N3a-run-02/summary.json).
- [N3b diagnostic boundary](docs/impl/networks-20261022/N3-operations/N3b-paused/diagnostics.json).
- [Operation contract](spec/network/operations.md) and [method inventory](spec/network/methods.json).
- [Historical N2 acceptance](docs/impl/networks-20261022/N2-security/README.md).

The release remains one shared broker for controlled clients. Autonomous LAN
writes and N6 federation have not been selected. Generic remote MCP/OAuth has
its own acceptance gate.

## Accepted N3a boundary

The accepted source is identified by identical manifests in the three final
`N3a-run-02` receipts. Those receipts predate subsequent N3b edits.

| Gate | Obtained result |
|---|---|
| Normal suite | 257 passed, zero failed/skipped, one pre-existing ignored performance test |
| Injection suite | 330 passed, zero failed/skipped, one pre-existing ignored performance test |
| Generated replay | 63 traces: 34 protocol, five writer, 24 fault; rotation crash controls passed |
| Source integrity | Identical manifests; all three runs report source unchanged |

All final suites used offline locked dependencies and required sockets. The
first injection gate remains failed in `N3a-run-01`: a registry recovery control
missed its fault boundary because root-lock reacquisition failed. Parallel
reproduction exposed `WouldBlock`; the harness now holds one explicit root lease
across constructor attempts. Production durability code did not change for that
harness correction. Normal and generated gates were rerun with the fixed harness.

N3a implements scoped immutable operation references, a canonical semantic digest,
one replay-derived send receipt index, source-bound inbox proofs, covering fsync,
concurrent duplicate completion, uncertain-outcome fencing, and a private persisted
client journal. CLI send/list/resume and remote MCP fresh/explicit resume paths are
implemented. Direct HTTP MCP requires an explicit operation reference. Registry
activation installs `durable_operations_v1` before operation source recovery;
pre-operation binaries refuse upgraded roots before repairing logs.

Limits: 10,000 admitted operations per epoch; initial submission deadline at most
24 hours ahead; fixed seven-day epoch from first activation; local journals at
most 1,000 entries and 64 MiB. Automatic epoch rollover, receipt retirement and
journal pruning are not yet implemented. Large MCP replies keep full
`structuredContent` and may omit the duplicate `content` representation to fit
response limits; actual provider compatibility remains unqualified.

## N3b implementation present at STOP

- `operations.rs`: shared send/domain ID namespace, generic immutable context and
  outcomes, independent tickets/completions, source proof validation, and a synced
  hashed `operations/outcomes.jsonl` for terminal no-effect results. Exact terminal
  error markers allow client journals to persist `Rejected` outcomes.
- `network/domain.rs`: validated normalized inputs, 256 KiB input bound, detached
  operation ownership, domain serialization, and terminal-result handling.
- `queues.rs`, `docs.rs`, `channels.rs`, `leases.rs`: operation-aware mutation
  variants and replay helpers. Original claims, fences, expiries and receipts are
  retained. Dead-letter success waits for the deduplicated durable projection;
  arrival feeds project asynchronously from durable captured notification intent.
- `server.rs`: eager channel discovery and domain proof recovery before serving;
  old proof types retain omitted optional fields and historical hashes.
- Registry/policy: explicit `enable_resource_operations` (default false, requires
  `enable_operations`), with `durable_resource_operations_v1` installed before
  source recovery. The accepted N3a executable refuses this new format.
- Client/MCP: journal coverage for send plus channel/queue/document/lease methods,
  capability discovery, method-bound resume, direct HTTP operation schemas, and
  exact persisted terminal errors. No automatic mutation retries.

Focused diagnostics passed before the final review repairs: queue operation tests,
document/channel tests and existing storage/arrival regressions; journal tests
(15 normal, 17 injection), three terminal-marker tests, five MCP tests, and three
additional gateway authorization/normalization/size controls. These runs did not
pin one unchanged final tree and do not qualify the stopped source.

The retained second crash diagnostic passed 20 tests, including nine actual
SIGKILL cases across publish/enqueue/document update at pre-sync, partial-write
and post-sync/pre-receipt boundaries; real TLS lost-response recovery; terminal
empty-claim client recovery; and older-executable refusal. The first diagnostic
remains failed (19/20): a proxy event with a `method` field was counted as another
request; the test now counts only actual `/v1/rpc` request events. Both logs are
retained under `N3b-paused`; concurrent source edits prevent acceptance credit.

## Exact unfinished boundary and next actions

Two review findings were being repaired when STOP arrived:

1. **Interrupted queue creation.** An empty/recovery-only stream was treated as an
   existing queue, while enqueue could use it and later make proof recovery fail.
   The current `ensure_queue_loaded` now requires `created_seq > 0`, and creation
   can reuse an uncommitted stream. These last edits have not been qualified.
   Add the missing regression: interrupted/partial first create append → reopen →
   reject enqueue before creation → retry create → enqueue → successful reopen.
2. **Successful maintenance versus requested effects.** Completed lease-expiry or
   recovery maintenance must allow a durable terminal error for the requested
   operation. Only uncertain maintenance should fence it as unknown. Core now has
   `DomainMaintenance`, with a separate uncertainty counter and explicit
   `complete()`. Queue/document/channel call sites were changed at STOP, but their
   final integration and regression updates are unfinished.

**Known stale test:**
`tests/domain_document_operations.rs::incidental_claim_expiry_never_becomes_a_terminal_no_effect_receipt`
still expects `OUTCOME_UNKNOWN` after successful expiry. That assertion describes
the superseded behavior. Update it to preserve the exact terminal `LEASE_LOST`
through a later claim and restart, and separately test uncertain maintenance I/O.
Do not report the final tree as passing while this mismatch remains.

On a future resume:

1. Inspect the stopped edits and finish both review repairs/tests. For queues,
   additionally expire the last max-attempt task during a claim, observe terminal
   `NOT_FOUND`, enqueue later work, restart and prove the same ID stays terminal.
2. Review maintenance counter ownership: completing one guard must not clear
   another uncertain guard or a primary mutation attempt. Preserve unknown state
   on partial/failed source or projection I/O.
3. Format/check the final integrated tree, then run focused regressions. Earlier
   `cargo check` passed before these last edits; no final compile or full gate was
   run after STOP. Whitespace diff audit passed during checkpoint preparation.
4. Freeze one source tree and run full offline/locked normal, injection and
   generated replay gates with all required socket/migration controls. Retain any
   failures. Update contracts and acceptance only from those receipts.
5. Continue N3c, N4 and N5 only after N3b acceptance. Their gates remain open.

## Remaining waves and prepared decisions

**N3c:** lifecycle renewal/rotation/retirement; subscription and arrival-ACK
operation proofs; ordinary inbox receiver progress; finite receipt rollover and
restore fencing. No implementation acceptance exists for these.

Prepared lifecycle design: add `/v1/lifecycle`, authenticated by approved device
bearer plus the original launch secret, with method `instance_lifecycle` and
`agent_id`, operation reference, action `renew|retire`, expected credential
generation and optional replacement token hash. Store agent changes, revoked
hashes and receipt proof in one registry replacement. Exclude plaintext secrets
from proofs/journals. Exact receipt recovery precedes current generation/retirement
checks after current device/launch authorization; a fresh operation cannot revive
a retired instance. Clients must persist any replacement secret privately before
submitting its hash. This is a proposed contract, not implemented behavior.

Ordinary ACK must remain monotonic/volatile: persist receiver progress locally and
reapply it, with a source hash anchor to reject same-sequence divergence after a
stale restore. Arrival ACK remains durable. Startup must recover all ACK sources
before global operation IDs can be classified unseen. Epoch rollover must validate
known closed-epoch proofs while excluding their receipts from the active index;
old validity epochs remain rejected after compaction. Copied/stale roots require
external authority fencing; a local filesystem lock is insufficient.

**N4:** remote HTTPS managed hosts, local workspace/journal separation, exact
provider binding, server-issued controller attachment epochs and in-flight
wait/ACK fencing. ACK validation and controller attachment must share serialization;
a gateway-only check races the durable ACK. Approvals and resolution IPC stay local.

**N5:** full integrated regression and network/controller models; actual separate
client/broker machines, routed LAN and distinct Internet/NAT sites; selected real
provider profiles; performance, quotas, storage growth and operator recipes.
No actual topology/access details were supplied in this work session. Loopback TLS
and fake-provider tests cannot close those gates. No user bus was deployed.

## Local control binaries and test prerequisites

These paths are local caches, not portable acceptance evidence. Rebuild missing
controls from the recorded revisions/source snapshots before qualification.

- `COOP_PRENETWORK_BINARY=/tmp/coop-network-prenetwork-20261022/target/debug/coop`
  (revision `27d6e901a683b2c651f8bbf9a3be017f3a5b22a2`).
- `COOP_PREUPGRADE_BINARY=/tmp/coop-network-prearrival-20261022/target/debug/coop`
  (revision `edcbc92372c9efc45c1a83c31f7d23d8ea11be64`).
- `COOP_PREOPERATION_BINARY=/tmp/cooperators-n3a-preoperation-bfbf753/target/debug/coop`
  (revision `bfbf7534c3ab63a5756321070413415e917e5464`).
- `COOP_PRERESOURCE_BINARY=/tmp/cooperators-n3a-accepted-source-20261022/coop`.
  Accepted N3a SHA-256:
  `2a2f5a0ecee674ea39dfb3f59ce7c3ef39f41d83aca4003eafc75ddf90d74ebd`.
  Its source snapshot and manifest are in the same temporary directory; the
  committed binary manifest is under `N3a-run-02`.
- Set `COOP_REQUIRE_NETWORK_MIGRATION=1`, `COOP_REQUIRE_UDS=1`, and
  `COOP_REQUIRE_REMOTE=1`. Choose a fresh `COOP_OPERATION_CRASH_EVIDENCE_DIR`.
- Generated replay uses `/home/nzsn/Repos/Mirrors/.lake/build/bin/mirror`, sibling
  `model_interface_gen`, and `/home/nzsn/Repos/MirrorRust`. Preserve sibling edits.
- Socket gates require escalation when the sandbox denies AF_UNIX/TCP binds.
