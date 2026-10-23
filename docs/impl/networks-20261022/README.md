# Network implementation execution

Design: [0005](../../design/0005-across-networks.html).
Plan: [ordered implementation waves](../../../.agents/chatdocs/2026-10-22-across-networks-implementation-plan.html).
Execution base: `27d6e901a683b2c651f8bbf9a3be017f3a5b22a2`.

This record tracks actual execution. A local build, socket fixture or retained
provider result is not cross-machine or Internet qualification.

Execution paused at the user's request on 2026-10-22 in checkpoint `44900f3`
and resumed on 2026-10-23. N2's local authority/enrollment/read gate now passes
on the source manifests in [its final summary](N2-authorization/summary.json).
See [CHECKPOINT.md](../../../CHECKPOINT.md) for the current state and N3a resume
steps. N0/N1 receipts retain their original source scopes; none is retroactive
acceptance of later code. Final file hashes identify the qualified N2 source
independently of the publication commit.

| Wave | State | Evidence / remaining gate |
|---|---|---|
| N0 | Passed for the local baseline | Source/tool manifest, 181 normal / 230 injection tests, 63 generated MBT traces and real rotation crash controls; required sockets, one pre-existing ignored performance test per suite and zero skips. Offline HTTPS dependency audit and baseline gap controls passed. |
| N1 | Passed for the transport gate | Shared UDS/HTTPS client, CLI/stdio transport selection, private native read adapter; 189 normal tests, zero failures/skips, one pre-existing ignored test. Includes real TLS ingress, certificate/hostname/redirect, timeout and uncertain-response controls. Pinned profile identity needs the N2 info endpoint. |
| N2 | Passed for the local authority/enrollment/read gate | 213 normal / 265 injection tests, zero failures/skips, one pre-existing ignored performance test per suite; 63 generated replay traces and two real rotation crash controls. Matching unchanged-source manifests; real TLS/UDS, policy revision/generation restart fences, enrollment/resource bounds, migration/process kills and pinned old-executable refusal. Domain mutations and lifecycle renewal/ACK remain gated for N3. [Evidence](N2-authorization/README.md). |
| N3a | Pending | Atomic receipt core and send crash pilot. |
| N3b | Pending | Queue/document/channel mutation and empty-claim replay. |
| N3c | Pending | Lifecycle, ACK/recovery and retention guarantees. |
| N4 | Pending | Remote hosts, exact binding and controller fencing. |
| N5 | Pending | Full regression, actual network and provider qualification. |
| N6 | Conditional | Autonomous-site federation if required. |

The baseline required-socket suite ran outside the restricted sandbox because
both AF_UNIX and TCP probes returned EPERM inside it. Baseline source and tool
identities are retained under `N0-baseline/`. No user bus or provider session
was selected for the baseline.
