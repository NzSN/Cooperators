# Network implementation checkpoint

Date: 2026-10-22. Execution stopped at the user's explicit request.
Base commit: `27d6e901a683b2c651f8bbf9a3be017f3a5b22a2` on `main`.

This checkpoint preserves **N0 and the source-pinned N1 transport gate, plus
unfinished N2 work**. It is not an accepted Internet deployment or a complete
implementation of design 0005. No build or runtime test was started after the
stop request; checkpoint preparation only preserved evidence and publication
state.

## Design and execution records

- [Canonical design 0005](docs/design/0005-across-networks.html).
- [Ordered implementation plan](.agents/chatdocs/2026-10-22-across-networks-implementation-plan.html).
- [Execution tracker](docs/impl/networks-20261022/README.md).
- [Method contract inventory](spec/network/methods.json) and
  [format/retry obligations](spec/network/README.md).

The first release remains one shared authority with controlled coop clients.
Whether autonomous LAN operation is required has not been selected. Generic
remote MCP/OAuth interoperability and N6 federation have separate gates.

## Results actually obtained

| Source scope | Check | Result |
|---|---|---|
| Unchanged pre-network baseline | Required-socket normal suite | 181 passed, zero failed/skipped, one pre-existing ignored performance test |
| Isolated export of the same baseline | Required-socket injection suite with rebuilt pre-arrival binary | 230 passed, zero failed/skipped, one pre-existing ignored test |
| Isolated baseline | Generated MBT | 63 traces: 34 protocol, five writer and 24 fault; zero failures; two rotation crash controls passed |
| Isolated dependency audit | Exact reqwest 0.12.28, rustls/webpki client feature graph | Offline resolution and compilation passed |
| N1 source pinned in its receipt | Full normal suite with required UDS/TCP/TLS | 189 passed, zero failed/skipped, one pre-existing ignored test; source unchanged during the run |
| Intermediate N2 states | Two `cargo check --offline --locked --all-targets` commands | Passed diagnostically; not source-pinned acceptance of the final checkpoint |

The final N2 edit rejects reopening an activated network root without its
approved policy. That last guard has not been compiled or tested. The N1
189-test receipt predates the N2 changes and must not be presented as validation
of the current checkpoint tree.

Evidence is retained under
[`docs/impl/networks-20261022/`](docs/impl/networks-20261022/).
The N1 result and its exact source hashes are in
[`N1-transport/full-normal-run-01/receipt.json`](docs/impl/networks-20261022/N1-transport/full-normal-run-01/receipt.json).
Generated disposable credentials in three test logs were replaced with stable
pseudonyms before publication. Original/published artifact hashes and counts
are in
[`credential-redactions.json`](docs/impl/networks-20261022/credential-redactions.json).

Baseline controls observed the existing gaps: repeated JSON-RPC correlation IDs
do not dedupe enqueue; a fresh claim after a lost reply can select another task;
ordinary ACK resets on restart; HTTP MCP could select a different valid body
credential from the header credential; non-loopback Host values are rejected.
Those controls used the preserved pre-network executable and disposable roots.

## Implemented source changes

- `src/transport/`: reusable UDS/HTTPS client, whole-exchange deadlines, bounded
  frames/responses, verified TLS, optional CA/client identity, explicit proxy,
  disabled redirects and implicit retries, and named JSON profiles with private
  credential references. Profile bus-ID verification uses `/v1/info`.
- `src/client.rs`, `src/main.rs`, `src/mcp.rs`: compatibility wrappers, remote
  endpoint/profile selection, HTTPS CLI/stdio forwarding, an approved-enrollment
  entry point, and a read-only remote tool catalog while mutation gates remain
  unavailable. Unknown mutation outcomes have an explicit error class.
- `spec/network/`: all 31 current native routes and 28 MCP aliases classified
  by identity, permission, resource, retry, persistence and exposure obligations.
- `tests/remote_transport.rs` and `tests/fixtures/network_tls_proxy.py`: real TLS
  ingress around a native broker fixture; certificate/hostname, redirect,
  deadline, response-bound, uncertain-result and actual CLI checks. Certificates
  and the retained server key are public test fixtures only; the fixture CA key
  remains outside the repository.
- `scripts/run_network_gate.py`: before/after source manifests, actual command
  status, test counts, skip detection and retained logs.
- `scripts/record_network_baseline.py`: disposable baseline gap observations
  that retain no bearer values.

## N2 work in progress — not accepted

The following code is present but has not passed its security, migration or full
regression gates:

- Registry V3 and `network_authority_v1` feature guard; stable bus/operation-epoch
  metadata; principal/generation/expiry grants on enrolled identities; adoption
  of existing arrival bus identity and preservation of the arrival feature.
- `src/network/policy.rs`: approved device principals, bounded permissions,
  Host/Origin policy, instance budgets and request/poll capacity settings.
- `src/network/runtime.rs`: scoped authentication/enrollment, resource
  authorization, principal generation checks, capacity gates and policy
  replacement semantics.
- `src/network/gateway.rs`: `/v1/rpc`, `/v1/enroll`, `/v1/info`, credential
  separation, shared scoped dispatch, post-wait reauthorization, filtered lists
  and remote wait/page limits. Resource mutations remain gated off.
- Shared policy routing for scoped identities through UDS and MCP/native HTTP;
  HTTP body credentials can no longer replace a different header identity.
- `daemon run --network-config`: explicit root activation, loopback-only HTTP
  backend for a same-host TLS ingress, and failure instead of silent remote
  listener fallback. Connection/header/request limits have been added.
- Bounded JSON size accounting, bounded remote inbox selection and bounded
  document/blob reads. These changes compiled before the final startup guard,
  but their new behavioral tests have not been written or run.

The original N1 `remote_read_only` fixture path remains in the tree. The existing
TLS fixture still tests that prototype rather than the new approved-policy
enrollment path. Migrate those fixtures and remove the prototype before N2
acceptance. The canonical design and implementation-plan documents themselves
were not changed during execution.

## Resume here

1. Inspect the checkpoint diff and run formatting/build checks on the complete
   tree. Then add and run N2 tests before accepting any N2 behavior.
2. Convert TLS fixtures to `NetworkPolicy` plus approved enrollment. Exercise
   pinned bus-ID checks, anonymous/local-unscoped denial, different body/header
   credentials, unauthorized resources, enrollment budgets, expiry, revocation
   during waits, and principal-generation fencing through every facade.
3. Test registry V1/V2 → V3 activation, arrival identity adoption in both upgrade
   orders, crash/failure recovery and actual pre-network binary refusal before
   log repair. Prove that a V3 root cannot reopen without its policy.
4. Finish bounded response construction for channels and list operations.
   Current list filtering happens after domain list construction; this needs
   bounded authorized pagination. Validate input types and cursor semantics.
   Review HTTP status-to-error mapping, policy replacement/capacity behavior and
   request admission ordering as part of the security gate.
5. Seal an unchanged-source N2 normal/injection regression and updated generated
   replay. Only then proceed to N3a/N3b/N3c durable operations, negative/empty
   result replay, lifecycle/retention and cursor recovery.
6. N4 remote managed hosts/controller epochs and N5 actual network/provider
   qualification remain pending. N6 federation is conditional. No remote
   mutation retry or Internet-readiness claim is earned by this checkpoint.

## Environment and retained local prerequisites

Inside the sandbox, AF_UNIX and TCP bind probes returned EPERM. Required-socket
gates ran through the environment's escalation mechanism and did not skip.
Rust/Cargo versions and the baseline manifests are retained in `N0-baseline/`.

Temporary prerequisites, useful only if they still exist:

- `/tmp/coop-network-baseline/coop`: pre-network release executable at base
  `27d6e90`; its hash is retained in `N0-baseline/binary.json`.
- `/tmp/coop-network-baseline/Cooperators`: isolated baseline source export.
- `/tmp/coop-prearrival-network/target/release/coop`: rebuilt `edcbc923`
  pre-arrival control; hash retained in `N0-baseline/prearrival-binary.json`.
- `/home/lsz/Codebase/Mirrors/.lake/build/bin/mirror` and
  `model_interface_gen`, plus sibling `MirrorRust`, were present for the baseline.

These paths are not portable acceptance evidence. Rebuild missing controls from
their pinned revisions. No binary, provider credential, user bus, live provider
session or cross-machine deployment is included in this checkpoint. The
pre-existing `.projectile-cache.eld` editor cache is excluded from the commit.
