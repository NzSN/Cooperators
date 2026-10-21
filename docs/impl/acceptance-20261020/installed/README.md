# Installed Codex and Claude CLI acceptance

Date: 2026-10-20. The message and queued-task exercise **passed with
`MCP_PROTOCOL_NEGOTIATION=legacy` on Claude**. Claude's default modern MCP
discovery **failed**. This is conditional installed-client acceptance; the
default client configuration still needs the response compatibility correction
described in [the discovery diagnosis](discovery-diagnosis.md).

## Clients and broker

| Component | Configuration used |
|---|---|
| Codex CLI | 0.163.0, configured default model `gpt-6.1-sol`, real `codex exec` |
| Claude Code | 2.1.300, configured provider/model `deepseek-flash`, supervised stream-JSON host bridge |
| Claude compatibility setting | `MCP_PROTOCOL_NEGOTIATION=legacy`; negotiated MCP `2025-11-25` |
| Broker | Production `coop` 0.1.0, default release features, UDS forwarding through `coop mcp-stdio` |
| Source base | `63e20514fffa039a97b0ec25e432657854d2fdef` |
| Release SHA-256 | `c6d1047a8a4d9b0094274151c9658a101ed018ce07f82d66ed9fb512eb285321` |

Neither CLI's model was overridden. This exercise covers the installed clients
with those existing provider settings. The broker ran against a fresh isolated
root under `/tmp`, with HTTP disabled. MCP configuration was supplied for these
launches. No persistent MCP configuration or production source was edited.

The final Codex sender was `01M5C9CTZGC2SXY69YX6PRBQA4`; the Claude worker was
`01M5C99W92KK201JHBBQRX8W32`. An earlier Codex readiness launch and preliminary
Claude launches have different identities, which remain visible in the archived
registry. They are not substituted for the participants in the completed exchange.

## Completed exercise

1. Both clients obtained their actual instance metadata and heartbeats through
   native MCP tool calls. Codex also observed the Claude identity in the registry.
2. Codex created queue `installed-acceptance-20261020` and enqueued task
   `01M5C9DVQZZ14ZF7YP0HHKDF7R`, asking the worker to sum 17 and 25.
3. Codex sent Claude a durable request carrying the queue, task ID, requester ID,
   and a unique correlation nonce.
4. Claude received and acknowledged the request, claimed the task with a
   300-second lease and fence 3, then completed it with sum 42 and its own ID.
5. Claude sent the actual result to Codex. Codex received it, verified the
   correlation and answer, and acknowledged cursor 1.
6. Codex sent a receipt confirming the answer. Claude received it and
   acknowledged cursor 2. Both CLI turns returned PASS, with no Claude permission
   denials. The SDK host reviewed and approved each gated Claude MCP call.

The durable queue contains exactly `queue_created`, `task_enqueued`,
`task_claimed`, and `task_completed`. The two inboxes contain exactly three
scenario messages: request, result, and final receipt.

[Independent verification](verification.json) cross-checks native client calls,
tool arguments/results, both identities, task payload, lease/fence, message IDs,
send receipts, received messages, acknowledgment cursors, and durable hash chains.
It checks the wire and files in addition to the clients' final reports.

Codex initially used an unsupported `to_agent_id` argument. The broker rejected
that call with `-32602`, and Codex retried with `to`. The rejection and recovery
are retained in the verifier output; only the corrected request reached an inbox.

## Default discovery failure

With Claude's normal modern negotiation, `server/discover` succeeds and
`tools/list` returns 23 tools, but the client exposes none of them to its model.
The response omits the modern protocol's required `ttlMs` and `cacheScope`.
Legacy negotiation makes all 23 tools visible and permits native calls.

The [diagnosis](discovery-diagnosis.md) links the failing wire exchange and both
implementations' source. A production compatibility fix and a repeat of the
default modern exercise remain outstanding. The legacy result does not close
that failure.

Two earlier launcher attempts omitted Claude's provider settings or supplied
an empty tool inventory. Those setup failures are retained under `attempts/`.
The modern discovery failure was observed after correcting both launcher issues.

## Reproduction and evidence

Build with `cargo build --offline --locked --release`, start a daemon with a
new bus root, and configure each client to launch
[the recording stdio forwarder](../../../../scripts/record_mcp_stdio.py) around
the release binary. The retained [Codex MCP configuration](codex-mcp-config.json)
and [Claude MCP configuration](claude-mcp-config.json) contain paths from this
capture; replace their root and output paths for another run.

Run Claude through a persistent supervised bridge:

```sh
tail -f /tmp/NEW_BRIDGE/in.ndjson | env MCP_PROTOCOL_NEGOTIATION=legacy \
  claude -p --input-format stream-json --output-format stream-json --verbose \
  --permission-prompt-tool stdio --permission-mode default \
  --strict-mcp-config --mcp-config /tmp/NEW_CLAUDE_MCP_CONFIG.json \
  --no-session-persistence --setting-sources user --disable-slash-commands
```

Send the [readiness prompt](claude-initial-prompt.txt), then run Codex with the
[recorded command shape](codex-exchange-command.json) and
[exchange prompt](codex-exchange-prompt.txt), using a new nonce, queue, and
the new Claude identity. Send Claude the matching
[worker prompt](claude-exchange-prompt.txt). Review and answer the bridge's
permission requests as they arrive. Captured request IDs in the
[host input](claude-host-input.jsonl) belong to this session and cannot be reused
as approvals for another run.

Verification of this frozen capture needs no model or running daemon:

```sh
python3 scripts/verify_installed_acceptance.py \
  --evidence docs/impl/acceptance-20261020/installed \
  --root docs/impl/acceptance-20261020/installed/raw-root
```

- [Codex wire transcript](codex-mcp.jsonl), [CLI events](codex-exchange-events.jsonl),
  and [final result](codex-exchange-final.txt).
- [Claude wire transcript](claude-mcp.jsonl), [CLI events](claude-events.jsonl),
  [legacy discovery debug log](claude-legacy-debug.log), and [final result](claude-exchange-final.txt).
- [Raw durable files](raw-root/), [final daemon status](daemon-final-status.json),
  and [process/client identities](process-identities.json).
- [Source and binary identities](source-manifest.json),
  [model attribution](client-models.json), and [credential scan](credential-check.json).

The isolated daemon and CLI bridge were stopped after the exercise. Inbox
acknowledgments are verified from live tool responses; no claim is made that
those in-memory acknowledgment cursors survive a daemon restart.
