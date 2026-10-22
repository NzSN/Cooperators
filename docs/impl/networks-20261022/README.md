# Network implementation execution

Design: [0005](../../design/0005-across-networks.html).
Plan: [ordered implementation waves](../../../.agents/chatdocs/2026-10-22-across-networks-implementation-plan.html).
Execution base: `27d6e901a683b2c651f8bbf9a3be017f3a5b22a2`.

This record tracks actual execution. A local build, socket fixture or retained
provider result is not cross-machine or Internet qualification.

Execution stopped at the user's request on 2026-10-22. See
[CHECKPOINT.md](../../../CHECKPOINT.md) for the exact unfinished state and resume
steps. The N1 acceptance below applies to its pinned source, before the current
unqualified N2 changes.

| Wave | State | Evidence / remaining gate |
|---|---|---|
| N0 | Passed for the local baseline | Source/tool manifest, 181 normal / 230 injection tests, 63 generated MBT traces and real rotation crash controls; required sockets, one pre-existing ignored performance test per suite and zero skips. Offline HTTPS dependency audit and baseline gap controls passed. |
| N1 | Passed for the transport gate | Shared UDS/HTTPS client, CLI/stdio transport selection, private native read adapter; 189 normal tests, zero failures/skips, one pre-existing ignored test. Includes real TLS ingress, certificate/hostname/redirect, timeout and uncertain-response controls. Pinned profile identity needs the N2 info endpoint. |
| N2 | Stopped at user checkpoint | Scoped registry format, policy/enrollment/authorization and bounded transport integration are partial. Diagnostic compile checks passed before the final startup-policy guard; no final checkpoint build or N2 security/migration acceptance. |
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
