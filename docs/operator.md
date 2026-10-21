# Operator guide

## Document and task arrivals

Arrival notifications are opt-in. The first enabled start promotes the bus
registry to V2 so older binaries refuse the new source-record format before
replaying it. Identity IDs and credential hashes are preserved. An upgraded
root remains V2; rollback to an older broker uses a coherent pre-upgrade backup.

```sh
./target/release/coop --root /path/to/bus daemon run --http-addr off --enable-arrivals
```

For a manually registered agent, use the same `COOP_TOKEN` across these calls:

```sh
./target/release/coop --root /path/to/bus arrivals subscribe \
  --client-key review-queue-v1 --queue reviews
./target/release/coop --root /path/to/bus arrivals subscribe \
  --client-key architecture-v1 --document architecture
./target/release/coop --root /path/to/bus arrivals wait --timeout 30
./target/release/coop --root /path/to/bus arrivals ack --cursor '<next_cursor>'
```

Subscriptions match exact names and include current state by default. Use
`--future-only` to omit catch-up. Wait is a peek; arrival ACK is durable and
independent of ordinary messages. Cursors are bound to the bus and agent
identity. Task notices signal possible work; only a successful task claim
grants a fenced lease.

Managed hosts enroll a fresh identity, keep a private resume configuration and
journal outside the broker root, and activate an exact provider session:

```sh
./target/release/coop --root /path/to/bus host run --provider codex \
  --workspace /path/to/project --display-name reviewer --queue reviews
# Alternative managed host:
./target/release/coop --root /path/to/bus host run --provider claude \
  --workspace /path/to/project --display-name reviewer --document architecture
./target/release/coop host status --config '/path/printed/by/host/run.json'
./target/release/coop host resume --config '/path/printed/by/host/run.json'
```

The host command prints the private configuration's path, binding and status;
it does not print its credentials. Resume uses the saved identity and exact
session. A new independent `host run` creates a new identity. The managed MCP
frontend reserves arrival wait/ACK and identity mutation for the controller;
the model gets `coop_confirm_activation` and `coop_resolve_arrivals` instead.

Runtime output and operator input are newline-delimited JSON. Approval events
remain pending until an explicit response for that request. Send one response
through stdin or `host approval --config ... --request-id ... --response ...`.
`host retry` explicitly releases a bounded retry budget after attention is
required; `host shutdown` stops the owned controller and provider process.
Provider-specific responses must use that adapter's protocol shape.

The implementation's [qualification record](impl/arrivals-20261021/README.md)
separates deterministic protocol checks from real provider-session checks.
Claude's current modern MCP discovery compatibility issue remains recorded in
the [installed-client baseline](impl/acceptance-20261020/README.md); compatibility
environment overrides, when needed, are explicit per-host `--env KEY=VALUE`
settings rather than edits to global client configuration.

This guide covers running `cooperatorsd`, its on-disk layout, configuration,
recovery behavior, dead-letter inspection, backup/restore, and health/metrics.
The protocol and semantics are specified in
[`docs/design/0001-cooperatorsd-broker.md`](design/0001-cooperatorsd-broker.md).

## Process model

- One daemon per bus root. Daemon and embedded startup lock `run/owner.lock`
  before replay; the kernel releases ownership after the last writer exits.
  Keep that lock file in place. Changing the socket does not change ownership.
- `coop daemon run` serves newline-delimited JSON-RPC on
  `<root>/run/daemon.sock` (0600) and, by default, the MCP/HTTP surface on
  `127.0.0.1:7420`.
- `coop mcp-stdio` is a per-client adapter; it opens one UDS connection per
  tool call. Each launch enrolls a fresh unique instance, using
  `--kind`/`--display-name` for repeatable labels. `--resume` explicitly reuses
  `COOP_TOKEN`/`--token`. Metadata and retirement have MCP tools.
- `coop mcp-stdio --embedded` runs against a root in-process and is for tests
  and debugging only. Do not point it at a root while a daemon is running.

The daemon logs operational messages to stderr (startup, listener failures,
recovery repairs, sweep errors). There is no `daemon.log` file yet: redirect
stderr when running under a supervisor, for example
`coop daemon run 2>>"$COOPERATORS_HOME/daemon.stderr.log"`.

## Configuration reference

### Environment and flags

| Setting | Scope | Default | Notes |
|---|---|---|---|
| `COOPERATORS_HOME` | daemon, CLI, stdio | `$HOME/.local/share/cooperators` | Bus root. Overrides `--root`. |
| `--root <DIR>` | all commands | platform default | Ignored when `COOPERATORS_HOME` is non-empty. |
| `COOP_SOCKET` | daemon, CLI, stdio | `<root>/run/daemon.sock` | Explicit socket path; useful when the client cannot derive the root. |
| `COOP_TOKEN` | CLI, stdio with `--resume` | none | Agent bearer token when `--token` is omitted. Fresh stdio launches enroll independently. |
| `--token <TOKEN>` | all commands | none | Global CLI flag; wins over `COOP_TOKEN`. |
| `COOP_HTTP_ADDR` | daemon | `127.0.0.1:7420` | HTTP MCP/health/metrics address. `off` or `none` disables the listener. |
| `--http-addr <ADDR>` | `daemon run` | `COOP_HTTP_ADDR` or `127.0.0.1:7420` | CLI flag wins. |
| `COOP_REQUIRE_UDS` | tests | unset | When set, socket tests fail instead of skipping. |

### `DaemonConfig` tunables

`DaemonConfig::new(root)` sets these values. The daemon currently constructs
its config in code; `$COOP_HOME/config.toml` from the design is **not loaded
yet**. Changing these values in a deployment requires a code change and
rebuild.

| Field | Default | Meaning |
|---|---|---|
| `stale_after` | 45 s | Silence before an agent is reported `stale`. |
| `gone_after` | 600 s | Silence before an agent is reported `gone`. |
| `presence_sweep_interval` | 5 s | Background presence classification interval. |
| `lease_sweep_interval` | 1 s | Queue/document lease expiry scan interval. |
| `wait_batch_limit` | 100 | Maximum messages returned by one `wait_message` call. |
| `max_inbox_messages` | 10 000 | Maximum unacknowledged messages per inbox. |
| `max_inbox_bytes` | 67 108 864 (64 MiB) | Maximum unacknowledged serialized bytes per inbox. |
| `write_batch_messages` | 64 | Maximum direct-message records in one active batch. |
| `write_batch_bytes` | 262 144 (256 KiB) | Batch collection byte bound; a larger admitted message commits alone. |
| `write_batch_delay` | 250 microseconds | Maximum intentional collection delay from the oldest queued request; scheduling and disk time are additional. |
| `write_queue_messages` | 1 024 | Maximum waiting requests for one inbox, excluding its active batch. |
| `write_queue_bytes` | 8 388 608 (8 MiB) | Maximum serialized-size upper bound waiting in one inbox queue. |

Both inbox backlog limits are checked at admission under the individual inbox
lock, including all earlier accepted records in the active batch.
A send that would exceed either limit is rejected with
`RESOURCE_EXHAUSTED` (-32003) and is not appended. `wait_message` does not free
capacity; `ack` does. Expired messages remain part of the durable inbox and of
the pending count until acked. Stdio tool calls are capped at 4 MiB; the HTTP
transport has the same rmcp body limit.

Write queue limits bound requests waiting for admission separately from the
durable unacknowledged backlog. A full queue rejects the request before file
mutation. Queue space becomes available as the writer takes batches; backlog
space becomes available through acknowledgment. Messages larger than the queue
byte capacity are rejected even if the backlog cap would otherwise allow them.

Direct-message workers run on the blocking pool. Each busy inbox has one finite
worker; different inboxes can write concurrently. A batch in one inbox shares a
file sync. TTL companion metadata is synced before the corresponding inbox
records. Messages, receipts, and committed status/metrics heads become visible
after their covering sync. Reads and acknowledgments use a separate committed
view, so they can consume older records during the next batch's disk work.
Queue, document, and channel operations retain their existing mutation ordering.

An accepted send can finish after client timeout or disconnect. Reuse its
idempotency key when retrying. A failed uncertain commit quarantines that inbox
writer until daemon restart and recovery; unrelated inboxes continue operating.
Already committed dedupe receipts remain available. Pending keys associated with
an uncertain active batch cannot be moved to another recipient before recovery.

## On-disk layout

Everything lives below the bus root. The root and `run/` are mode 0700;
`registry.json` and the socket are 0600.

```text
$COOPERATORS_HOME/
  registry.json                 name -> agent record (token hash only)
  agents/<agent_id>/
    inbox.jsonl                 direct-message envelope log
    expiry.jsonl                optional companion TTL log
  channels/<name>.jsonl         channel envelopes; #presence is daemon-emitted
  queues/<name>.jsonl           queue events (enqueue/claim/renew/complete/...)
  dead/<queue>.jsonl            dead-letter records with attempt history
  docs/<name>.jsonl             document events and inline versions
  blobs/sha256/<hex>            spilled document bodies (>64 KiB)
  run/daemon.sock               UDS JSON-RPC endpoint (0600)
```

All JSONL logs are append-only and hash-chained: each record has a monotonic
`seq` and a `prev_hash` naming the previous record. `registry.json` is the only
mutable state file; it is replaced atomically (temp file, fsync, rename, parent
fsync).

Logs use one JSON object per line. `jq -c . <file>` renders them; `tail -f`
follows them. The design's `coop tail` convenience command is not implemented
in this build.

## Recovery semantics

At startup the daemon replays:

- `registry.json` (registry and token hashes),
- every `agents/*/inbox.jsonl` and `expiry.jsonl`,
- the `#presence` channel and any other channel opened on demand,
- every `queues/*.jsonl`,
- every `docs/*.jsonl`.

Replay verifies sequence monotonicity and every `prev_hash` link, then rebuilds
in-memory indexes: inbox records, dedupe entries, expiry times, queue/task
state and leases, document versions and claims, and presence seeds. Dedupe,
queues, leases, and documents therefore survive restarts.

### Torn tail

A torn tail is the common crash artifact: the final line was partially written
or fails hash verification. For inbox, channel, queue, and document logs the
daemon:

1. finds the last valid record offset,
2. truncates the file there and fsyncs,
3. appends a daemon-authored `recovery` record carrying
   `truncated_bytes` and `last_valid_offset` (or `reason` for queue/doc events).

`expiry.jsonl` is repaired the same way without a recovery record. A valid
final record without a trailing newline is retained; the next append writes the
missing separator first.

Durability boundary: a send is acknowledged only after `append + fsync`. A
crash can lose an unacknowledged in-flight append, but cannot reorder or
silently rewrite committed records.

### Corruption that is not a tail

Malformed JSON or a hash break in the middle of a log is **not** silently
skipped. The daemon refuses to start and reports the file and line. This is
intentional: mid-log corruption is evidence loss and needs operator review
before repair.

### Known integrity limit

The hash chain authenticates links, not the final record by itself. The v1
envelope has no self-hash, so a modified final record is undetectable until the
next append produces a mismatching `prev_hash`. Anchor the head digest
externally if tail tamper evidence is required.

### Ephemeral state

- Acknowledged inbox cursors are in memory and reset to 0 on restart. Clients
  that need exactly-once processing must persist their last acked cursor and
  pass it explicitly.
- Because the ack cursor resets, previously acknowledged records count as
  pending again after a restart for backpressure and inbox metrics until they
  are acknowledged again. A long-lived inbox that is near its cap should be
  accounted for when planning a restart.
- Presence is ephemeral: registered agents are reseeded as active at startup
  and get one full stale window to heartbeat again.
- HTTP request counters reset with the process; log-derived metrics resume from
  durable sequence heads.

## Inspecting the bus

### Status

```sh
coop status | jq
```

Reports version, pid, uptime, root, start time, and per-log sequence heads and
byte sizes.

### Agents and presence

```sh
COOP_TOKEN=... coop agents --include-stale | jq
```

### Channels

```sh
jq -c 'select(.kind != "recovery")' "$COOPERATORS_HOME/channels/ops.jsonl"
```

`#presence` contains daemon-emitted `active`/`stale`/`gone` transition events.

### Queues

```sh
jq -c 'select(.kind == "task_enqueued" or .kind == "task_claimed" or .kind == "task_requeued")' \
  "$COOPERATORS_HOME/queues/work.jsonl"
```

The latest event per `task_id` determines its phase. A claim event's sequence
is the task's fence.

### Documents

```sh
jq -c 'select(.kind == "doc_published" or .kind == "doc_updated")' \
  "$COOPERATORS_HOME/docs/plan.jsonl"
```

Blob-backed versions name a digest under `blobs/sha256/`; verify with
`sha256sum`.

## Dead letters

When a task exhausts `max_attempts`, the queue appends a `task_dead` event and
writes a complete record to `dead/<queue>.jsonl`:

```sh
jq . "$COOPERATORS_HOME/dead/work.jsonl"
```

Each record contains `task_id`, the original `payload`, `max_attempts`,
`attempts`, `reason`, the full `history` (lease ids, fences, agents,
timestamps, results), and its own hash-chain fields. Dead letters are evidence,
not an automatic retry queue.

To retry, inspect the payload and enqueue a new task. There is no `coop enqueue`
CLI subcommand in this build; use the MCP tool or send one JSON-RPC line over
the socket:

```sh
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"enqueue","params":{"token":"<token>","queue":"work","payload":{"...":"..."}}}' \
  | socat - "UNIX-CONNECT:$COOPERATORS_HOME/run/daemon.sock"
```

The new task gets a new `task_id`; the dead letter remains untouched.

## Health and metrics

`GET /healthz` is unauthenticated:

```sh
curl -s http://127.0.0.1:7420/healthz | jq
```

```json
{"status":"ok","version":"0.1.0","uptime_s":42,"http_listener":true}
```

`http_listener:false` means the HTTP port could not be bound (for example, it is
already in use) or the listener has stopped. UDS and stdio continue to work;
the daemon logs the bind error to stderr.

`GET /metrics` requires `Authorization: Bearer <agent token>`:

```sh
curl -s -H "Authorization: Bearer $COOP_TOKEN" \
  http://127.0.0.1:7420/metrics
```

| Metric | Type | Meaning |
|---|---|---|
| `coop_info{version}` | gauge | Build version. |
| `coop_uptime_seconds` | gauge | Seconds since daemon state was constructed. |
| `coop_http_listener` | gauge | 1 while the HTTP listener is serving. |
| `coop_agents_registered` | gauge | Registered agent names. |
| `coop_agents{state}` | gauge | Effective active/stale/gone counts. |
| `coop_agents_stale` | gauge | Alias for `coop_agents{state="stale"}`. |
| `coop_messages_appended_total{log}` | counter | Inbox/channel log sequence head (durable records). |
| `coop_inbox_pending_messages{agent}` | gauge | Unacknowledged messages. |
| `coop_inbox_pending_bytes{agent}` | gauge | Unacknowledged serialized bytes. |
| `coop_queue_pending{queue}` | gauge | Tasks waiting for a claim. |
| `coop_queue_depth{queue}` | gauge | Alias for `coop_queue_pending`. |
| `coop_queue_leased{queue}` | gauge | Tasks currently holding a lease. |
| `coop_active_leases` | gauge | Unexpired queue + document leases. |
| `coop_http_requests_total{path,status}` | counter | Completed HTTP responses (process lifetime); `path` is `/mcp`, `/healthz`, `/metrics`, or `other`. |
| `coop_stdio_size_rejections_total` | counter | Stdio tool calls rejected by the 4 MiB cap. |

Because message-log counters come from durable sequence heads, they survive a
restart. HTTP counters do not.

## Backup and restore

The bus is a directory; a consistent backup is a copy of that directory.

1. Prefer stopping the daemon first (Ctrl-C / supervisor stop).
2. Copy the root, including `registry.json`, `agents/`, `channels/`, `queues/`,
   `dead/`, `docs/`, and `blobs/`. Exclude `run/daemon.sock`.

```sh
tar -C "$COOPERATORS_HOME/.." -czf cooperators-backup.tgz "$(basename "$COOPERATORS_HOME")"
```

3. Restore by extracting to the same path (or point `COOPERATORS_HOME` at the
   restored directory), keeping 0700 on the root and `run/`, and 0600 on
   `registry.json`.
4. Start the daemon and check `coop status` plus the sequence heads.

Live copies are possible because each append is fsynced, but a copy taken
mid-append can contain a torn final line. That is repairable on restore: the
daemon truncates it and appends a `recovery` record. If you need a
point-in-time-consistent archive, stop the daemon first or snapshot the
filesystem. A backup without `registry.json` loses all agent identities and
tokens.

## Troubleshooting

| Symptom | Check | Action |
|---|---|---|
| HTTP MCP unreachable, stdio works | `/healthz` → `http_listener` | Read daemon stderr for bind error; choose another `--http-addr` or `off`. |
| `RESOURCE_EXHAUSTED` on send | `/metrics` inbox pending gauges | Ack the recipient's cursor; raise `max_inbox_messages`/`max_inbox_bytes` only with intent. |
| `DAEMON_UNREACHABLE` | socket path, daemon process | Check `COOP_SOCKET`/`COOPERATORS_HOME`; start `coop daemon run`. |
| Daemon refuses to start after disk event | stderr names a log and line | Preserve the file for analysis; mid-log corruption is not auto-repaired. |
| Duplicate messages after restart | client cursor handling | Persist and pass the last acked cursor; acked cursors are not durable. |
| Stale CLI token | `coop register <name>` with current token | Rotation returns a new token; update MCP client env/config. |
