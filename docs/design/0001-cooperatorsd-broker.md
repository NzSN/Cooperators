# 0001 — cooperatorsd: Local Broker Daemon + MCP Facade

- **Status:** Draft, pre-implementation
- **Date:** 2026-10-18
- **Context:** [chatdocs survey](../../chatdocs/2026-10-18-inter-agent-communication-proposals.html) selected **P1** as the core mechanism for inter-agent communication. P0 (mailbox file contract) is the substrate this daemon owns; P2 (Codex-fork adapter) and P3 (A2A/NATS) are explicitly downstream.
- **Scope:** independent agent processes on **one machine** — multiple Codex sessions, Claude CLI via host-mode bridge, CI bots. Cross-machine transport is a non-goal for v1.

## 1. Design goals

| Goal | Consequence |
|---|---|
| One shared message bus per machine | A single daemon owns all writes; agents are thin clients |
| Cross-CLI reach | MCP is the primary facade: every target agent (Codex, Claude Code) already speaks it |
| Evidence-grade auditability | The P0 append-only JSONL logs are the source of truth; the daemon is replaceable plumbing |
| Crash-safe durability | A send is acknowledged only after it is durable |
| Work distribution, not just chat | First-class queues with leases and fencing tokens |
| Shared mutable artifacts | Named documents with version CAS and exclusive edit leases (same lease machinery, no second lock scheme) |
| No collision with fork tooling | All MCP tools are prefixed `coop_` |

Non-goals for v1: TLS, multi-machine routing, message encryption, rich presence UI, schema evolution beyond envelope v1.

## 2. Architecture

```
 Codex session A      Claude CLI B       Codex session C       curl / dashboard
  (MCP stdio)          (MCP stdio)         (MCP streamable HTTP)     (HTTP)
      │                    │                      │                     │
      ▼                    ▼                      ▼                     ▼
 ┌─────────┐         ┌─────────┐        ┌───────────────────────────────────┐
 │ coop    │         │ coop    │        │ cooperatorsd HTTP listener        │
 │ mcp-stdio│        │ mcp-stdio│       │  127.0.0.1:7420 (/mcp, /healthz)  │
 │ adapter │         │ adapter │        └───────────────┬───────────────────┘
 └────┬────┘         └────┬────┘                        │
      │  NDJSON JSON-RPC  │                             │ in-process
      ▼                   ▼                             ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │ cooperatorsd core (single process, async)                            │
 │  router ── sequencer (per log) ── append+fsync ── index ── notifiers │
 │  registry/auth │ presence │ queue & lease manager │ dead-letters     │
 └───────────────────────────────┬──────────────────────────────────────┘
                                 ▼
              $COOP_HOME  (default ~/.local/share/cooperators)
                registry.json (0600)     channels/<name>.jsonl
                agents/<id>/inbox.jsonl  queues/<name>.jsonl
                dead/<name>.jsonl        docs/<name>.jsonl
                blobs/sha256/…         run/daemon.sock (0600)
```

**Transports**

1. **Unix domain socket** (`run/daemon.sock`, mode 0600) carrying newline-delimited JSON-RPC 2.0. This is the single internal protocol; everything else is an adapter over it. Inspectable with `socat`/`jq`.
2. **`coop mcp-stdio`** — a thin stdio MCP server shipped in the same binary. Each MCP client spawns its own adapter process; the adapter authenticates to the daemon and forwards tool calls. Universal: works with any MCP client regardless of HTTP support.
3. **Streamable HTTP MCP endpoint** (`POST /mcp`) — optional convenience for clients configured for HTTP MCP, plus `/healthz` and a plain JSON REST mirror for debugging. Bound to `127.0.0.1` only.

Transport 2 is the primary integration path; transport 3 must expose the identical tool surface and is validated by the same conformance suite.

## 3. Storage and durability

The P0 envelope is normative and unchanged:

```json
{"v":1,"id":"01J…","seq":412,"ts":"…Z","from":"codex:/root/task1",
 "to":"claude:reviewer-1","kind":"request","reply_to":"01J…",
 "idempotency_key":"sha256:…","body":{"mime":"text/markdown","text":"…"},
 "prev_hash":"sha256:…"}
```

- **Single writer.** Only the daemon appends. Offline CLI append from P0 is disallowed while the daemon owns the root (`coop send` without a daemon fails fast with `DAEMON_UNREACHABLE`; a deliberate `--offline` escape hatch appends with a warning header record and is excluded from hash-chain continuity guarantees).
- **Sequencing.** One sequencer task per log file assigns `seq` strictly monotonically; ordering is total per file, absent across files.
- **Durability.** `append → fsync → ack`. Agent messaging is low-rate; fsync-per-message is acceptable at target scale. Send acknowledgement means durable.
- **Crash recovery.** On startup the daemon replays each log, verifies the hash chain, and **drops a torn tail** (last record fails hash/JSON parse ⇒ truncate at last valid offset, emit `recovery` event into the log). In-memory indexes (cursors, dedupe map, leases) are fully rebuilt from the logs; no separate state files to corrupt except `registry.json`.
- **Registry.** `registry.json` holds `{name → {agent_id, token_hash, kind, capabilities, registered_at}}`. Written tmp+rename, mode 0600. Tokens are 128-bit random, shown once at registration; only hashes persist.
- **Rotation.** Channel/queue logs rotate at a configurable size (default 256 MiB) into `<name>.<seq_hi>.jsonl`; cursors address `(file, seq)` so rotation never invalidates a reader. Retention: keep everything (evidence discipline); deletion is a manual, logged operation.
- **Documents and blobs.** Doc logs rotate like channels. Doc bodies ≤ 64 KiB are inline in the log; larger bodies go to content-addressed `blobs/sha256/<hash>` (referenced by hash + size), so logs stay small and blob integrity is checkable. Blobs are immutable; garbage collection of unreferenced blobs is deferred (§16).

## 4. Identity, authentication, trust boundary

- The boundary is **loopback + filesystem permissions**: UDS at 0600, HTTP on 127.0.0.1, root dir 0700. Any local process able to open the socket still needs a token.
- `register_agent` issues `(agent_id, token)`. Names are display claims; **tokens are the identity**. Re-registration of an existing name requires the old token and returns a rotated one.
- Sandboxed agents that cannot read the home directory get the socket path and token via their MCP server config (`command: coop mcp-stdio`, `env: {COOP_TOKEN, COOP_SOCKET}`), which is the standard MCP provisioning channel.

## 5. Message lifecycle

1. Client calls `coop_send_message(to, kind, body, reply_to?, idempotency_key?, ttl_s?)`.
2. Daemon validates envelope, checks sender token, resolves recipient.
3. **Dedupe:** `(sender, idempotency_key)` present in the dedupe index ⇒ return the original `{message_id, seq}` without appending. The index is derived at replay, so dedupe survives restarts.
4. Sequencer assigns `seq`, appends with `prev_hash`, fsyncs, then updates indexes and wakes long-pollers.
5. Recipient's `coop_wait_message(cursor, timeout_s)` long-polls its inbox from `cursor`; returns a batch plus `next_cursor`. **Wait is a peek** — the cursor is not advanced until `coop_ack(cursor)` succeeds. Default `auto_ack` is false so a crashing agent cannot lose messages silently.
6. Unknown recipient ⇒ synchronous `NOT_FOUND` (fail fast; no dead-letter for direct messages).

Error codes: `UNAUTHENTICATED`, `NOT_FOUND`, `RESOURCE_EXHAUSTED` (backpressure), `CONFLICT` (name re-registration without token), `EXISTS` (doc/queue name taken), `VERSION_CONFLICT` (doc CAS base mismatch), `CONFLICT_CLAIMED` (doc under another agent's lease), `LEASE_LOST`, `DAEMON_UNREACHABLE` (client-side).

## 6. Presence

- Agents heartbeat every 15 s (`coop_heartbeat` or implicitly via any authenticated call).
- Daemon marks an agent `stale` after 45 s silence, `gone` after 10 min; both transitions are events in the agent's inbox and in `#presence` channel so supervisors can react.
- `coop_list_agents(include_stale=false)` reports name, kind, capabilities, state, last-seen.

## 7. Work queues and leases

Queues are the mechanism that turns "chat" into "coordination".

- `coop_create_queue(name)`; `coop_enqueue(queue, payload, max_attempts=5)`.
- `coop_claim_task(queue, lease_ttl_s)` ⇒ `{task, lease_id, fence}` where `fence` is the claim's log sequence number. Completing requires the matching `lease_id` **and** current fence; a reclaimed (expired and re-issued) task carries a higher fence, so a stale worker's `complete` is rejected with `LEASE_LOST`.
- Lease expiry re-queues the task with `attempts+1`; exceeding `max_attempts` moves it to `dead/<queue>.jsonl` with the full attempt history.
- `coop_complete_task(lease_id, fence, outcome, receipt?)` and `coop_fail_task(lease_id, fence, reason)`; `coop_renew_lease(lease_id)` extends the deadline.
- Receipts are first-class: `receipt` is an envelope body recorded in the queue log, giving the evidence trail for "who did what, when, with what result".

## 8. Documents: publish/claim semantics

Messaging moves information; coordination also needs **named, mutable artifacts** — plans, design docs, shared results — with provenance and conflict control. Documents reuse the lease/fence machinery of §7 rather than inventing a second locking scheme.

**Model.** A document is a named chain of immutable versions recorded as events in `docs/<name>.jsonl` (`doc_published`, `doc_updated`, `doc_claimed`, `doc_released`, `doc_claim_expired`). Version numbers are per-doc monotonic from 1. Every mutation is a log event with actor, ts, seq, `prev_hash` — the document's provenance *is* the log; there is no separate history table.

**Rules**

- **Reads are always open.** `coop_read_doc(name, version?)` returns head (or a pinned version) plus current claim state. `name@7` is a reproducible reference suitable for task inputs and receipts.
- **Create.** `coop_publish_doc(name, mime, body, title?)` ⇒ v1; fails `EXISTS` if the name is taken. Names are not recyclable; deletion is a manual, logged operation.
- **Update is compare-and-swap.** `coop_update_doc(name, base_version, body, title?)` requires `base_version` == current head, else `VERSION_CONFLICT` — the client re-reads and retries.
- **Claim is an exclusive edit lease.** `coop_claim_doc(name, lease_ttl_s)` ⇒ `{lease_id, fence, head_version}`. While a claim is active, updates must present the matching `lease_id` + `fence`; claims by other agents fail `CONFLICT_CLAIMED` with holder and expiry. `coop_renew_lease` extends; `coop_release_doc(lease_id, fence)` releases explicitly; expiry logs `doc_claim_expired` and frees the name.
- **Two write paths, both fenced.** Unclaimed doc: open CAS updates (optimistic concurrency). Claimed doc: holder-only writes (leased exclusivity). A stale holder writing after expiry is rejected `LEASE_LOST` because expiry advanced the fence.
- **No implicit claim on publish.** Publishing creates a readable artifact; claiming is a separate, explicit act, so "publish then hand off" is a first-class workflow.

**MCP tools (M2):** `coop_publish_doc`, `coop_update_doc`, `coop_claim_doc`, `coop_release_doc`, `coop_read_doc`, `coop_list_docs(prefix?)`. `coop_renew_lease` is generic across queues and documents.

**Interactions.** Tasks and receipts reference `name@version` to pin inputs and outputs. Channels stay append-only streams; docs are the versioned artifacts those streams discuss. A doc claim is the mechanism two agents use to co-edit a plan without merge collisions.

## 9. MCP tool surface (v1)

All tools prefixed `coop_`, versioned by envelope `v`, described with JSON Schema in the MCP manifest:

| Tool | Purpose | Milestone |
|---|---|---|
| `coop_register_agent` | name → (agent_id, token) | M1 |
| `coop_send_message` / `coop_wait_message` / `coop_ack` | direct messaging, peek+ack cursors | M1 |
| `coop_heartbeat` / `coop_list_agents` | presence | M1 |
| `coop_publish` / `coop_read_channel` | channel fan-out and replay (`subscribe` = read + wait loop) | M2 |
| `coop_create_queue` / `coop_enqueue` / `coop_claim_task` / `coop_renew_lease` / `coop_complete_task` / `coop_fail_task` | work distribution with fencing | M2 |
| `coop_publish_doc` / `coop_update_doc` / `coop_claim_doc` / `coop_release_doc` / `coop_read_doc` / `coop_list_docs` | versioned docs with edit-lease fencing (`coop_renew_lease` is shared) | M2 |
| `coop_get_status` | daemon version, counts, health | M3 |

Backpressure: per-inbox cap (default 10 000 messages or 64 MiB) ⇒ `RESOURCE_EXHAUSTED` to senders; channels bounded only by rotation.

## 10. Implementation choices

- **Language: Rust**, single static binary (`coop` CLI and `cooperatorsd` as subcommands: `coop daemon run`). Rationale: matches the surrounding systems work (Codex fork, MirrorRust), trivial deployment, tokio ecosystem for async + UDS + HTTP. The TypeScript alternative was considered and rejected: a second runtime to supervise, weaker single-binary story.
- **Async core:** tokio; per-log sequencer tasks; `tokio::sync::Notify` fan-out to wake long-pollers; no locks on the hot path beyond per-log mutexes.
- **MCP SDK:** the official `rmcp` crate for both the stdio adapter and the streamable HTTP endpoint, so protocol drift is upstream's problem.
- **Config:** `$COOP_HOME/config.toml`; root resolution `COOPERATORS_HOME` env → `--root` flag → platform default. One repo-scoped root (`.cooperators/`) is supported for project-local buses.

## 11. Performance targets (acceptance-relevant, local scale)

| Metric | Target |
|---|---|
| send ack latency (fsynced) | p50 < 10 ms |
| wait delivery after send | p50 < 25 ms |
| sustained append | ≥ 5 000 msg/s |
| restart replay | 1 M records < 10 s |
| agents per daemon | 100 without degradation |

These are gates, not aspirations; the M4 hardening milestone exists to meet or revise them with measurements.

## 12. Observability

- `coop status`: uptime, per-log sizes/seq heads, registered agents, queue depths, stale agents.
- `coop tail <channel|agent>`: follow any log as rendered text (debugging without jq).
- Structured daemon log (tracing, JSON) at `$COOP_HOME/daemon.log`; `/healthz` and optional Prometheus `/metrics` on the HTTP listener.

## 13. Testing and acceptance

- **Unit:** envelope validation, hash-chain verification, sequencer monotonicity, dedupe, lease expiry/fencing, rotation cursor stability.
- **Integration (in-process):** two-agent echo; channel fan-out; claim→expire→requeue→dead-letter; restart durability across `kill -9`; torn-tail recovery.
- **Conformance:** golden JSON-RPC transcripts replayed against both the UDS API and each MCP transport — the stdio adapter and HTTP endpoint cannot silently diverge.
- **Installed acceptance (M3 gate):** one real Codex session and one real Claude CLI session exchange messages and a claimed task through `coop mcp-stdio`, with the logs retained as evidence. Partial completion of a gate is recorded as failed, not passed.

## 14. Milestones

| MS | Content | Gate |
|---|---|---|
| M1 | daemon core: registry/auth, send/wait/ack, inbox logs, replay recovery, UDS | integration + kill −9 durability green |
| M2 | channels, queues, leases, dead-letters, documents (publish/claim) | lease/fence/dead-letter + doc CAS/claim suite green |
| M3 | `coop mcp-stdio` adapter + HTTP endpoint | cross-CLI installed acceptance |
| M4 | backpressure, metrics, perf targets, operator docs | §11 measured and recorded |

## 15. Open questions

1. Default HTTP port (7420 proposed; must not collide with local services such as the 10086 webbridge).
2. Whether `coop_wait_message` gets server-side filtering (`kind`, `from`) in v1 or v1.1 — filtering reduces token spend on busy inboxes.
3. Repo-local root (`.cooperators/`) vs home default when both exist — current rule: env/flag wins, else home; a repo root is used only when explicitly requested.
4. Whether P2 (Codex-fork adapter) speaks UDS directly or also goes through MCP — recommended: MCP, one less protocol to maintain.
5. Claim force-takeover (admin break-lease) and claim handoff between agents — both deferred from v1; add only if a real workflow demands it.
6. Blob garbage collection for superseded doc versions — default is retain-all, matching evidence discipline.

## 16. Deferred

Multi-machine transport, TLS, encryption at rest, A2A/NATS bridges (P3), presence UI, envelope v2 (schemas, attachments), full-text doc search/index, claim force-takeover, blob GC. Each is compatible with this design; none is required by it.
