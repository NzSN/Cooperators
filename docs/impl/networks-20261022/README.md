# Network implementation execution

Design: [0005](../../design/0005-across-networks.html).
Plan: [ordered implementation waves](../../../.agents/chatdocs/2026-10-22-across-networks-implementation-plan.html).
Execution base: `27d6e901a683b2c651f8bbf9a3be017f3a5b22a2`.

This record tracks actual execution. A local build, socket fixture or retained
provider result is not cross-machine or Internet qualification.

Execution resumed on 2026-10-22 from checkpoint commit `44900f3`. The local N2
authorization, migration and bounded-read gate now passes on the source pinned
in [its execution record](N2-security/README.md). See
[CHECKPOINT.md](../../../CHECKPOINT.md) for the current handoff. Historical N0/N1
results remain tied to their own source; the full network release is pending.

The 2026-10-24 merge also retains the independent
[N2 authority branch evidence](N2-authorization/README.md) (213 normal / 265
injection tests for that branch's exact source). Its policy revision checks are
integrated with main's operation support. Historical receipts retain their
original source scope. See [merge validation](merge-20261024/README.md).

| Wave | State | Evidence / remaining gate |
|---|---|---|
| N0 | Passed for the local baseline | Source/tool manifest, 181 normal / 230 injection tests, 63 generated MBT traces and real rotation crash controls; required sockets, one pre-existing ignored performance test per suite and zero skips. Offline HTTPS dependency audit and baseline gap controls passed. |
| N1 | Passed for the transport gate | Shared UDS/HTTPS client, CLI/stdio transport selection, private native read adapter; 189 normal tests, zero failures/skips, one pre-existing ignored test. Includes real TLS ingress, certificate/hostname/redirect, timeout and uncertain-response controls. Pinned profile identity needs the N2 info endpoint. |
| N2 | Passed local security/migration/read gate | 232 normal / 288 injection tests, 63 generated traces, required sockets and zero skips; source unchanged across all final gates. Approved enrollment, durable generation fencing, bounded authorized pages, real TLS facades and old-binary/crash controls. Credential renewal remains N3c. [Evidence](N2-security/README.md). |
| N3a | Passed local send gate | 257 normal / 330 injection tests, 63 generated traces; required sockets, zero skips, matching source manifests. [Evidence](N3-operations/README.md). |
| N3b | Paused, unqualified | Partial queue/document/channel mutation coverage; review repairs and final gate unfinished at user STOP. See [checkpoint](../../../CHECKPOINT.md). |
| N3c | Pending | Lifecycle, ACK/recovery and retention guarantees. |
| N4 | Pending | Remote hosts, exact binding and controller fencing. |
| N5 | Pending | Full regression, actual network and provider qualification. |
| N6 | Conditional | Autonomous-site federation if required. |

The baseline required-socket suite ran outside the restricted sandbox because
both AF_UNIX and TCP probes returned EPERM inside it. Baseline source and tool
identities are retained under `N0-baseline/`. No user bus or provider session
was selected for the baseline.
