# Cooperators

A localhost inter-agent message broker for multiple Codex/Claude/CLI sessions on
one machine. One `cooperatorsd` process owns the durable logs; agents reach it
through a Unix-domain socket directly, through `coop mcp-stdio`, or through the
optional streamable-HTTP MCP endpoint.

The design is in [`docs/design/0001-cooperatorsd-broker.md`](docs/design/0001-cooperatorsd-broker.md).
Operator procedures live in [`docs/operator.md`](docs/operator.md); measured
M4 performance is in [`docs/impl/perf-smoke.md`](docs/impl/perf-smoke.md).
The latest installed-client, transport, 100-agent, and actual million-record
replay results are in the [2026-10-20 acceptance baseline](docs/impl/acceptance-20261020/README.md).
The subsequent [durable inbox group-commit report](docs/impl/throughput-20261020/README.md)
records the implementation, correctness qualification, shared-inbox throughput
gains, and remaining performance target misses.
The [arrival notification and agent activation design](docs/design/0004-arrival-notifications-and-agent-activation.md)
is implemented behind an explicit root upgrade. Its
[implementation record](docs/impl/arrivals-20261021/README.md) distinguishes
broker/bridge checks from pending real provider-session qualification.

## What v1 provides

- Direct messages with durable, fsynced append, dedupe by idempotency key,
  peek + explicit ack cursors, TTLs, and presence heartbeats.
- Channels with replayable cursors.
- Work queues with leases, fencing tokens, retries, and dead letters.
- Versioned documents with compare-and-swap updates and exclusive edit leases.
- 28 MCP tools prefixed `coop_*`, exposed identically over ordinary stdio and
  streamable HTTP.
- Optional durable document/queue arrival subscriptions, separate peek/ACK
  feeds, and managed Codex/Claude host bridges. Managed sessions expose local
  activation/resolve tools and reserve feed consumption for their bridge.
- Per-inbox backpressure, `/healthz`, and bearer-protected Prometheus
  `/metrics` on the HTTP listener.

Everything is local: Unix socket at `run/daemon.sock` (0600), HTTP bound to
`127.0.0.1` by default, bus root 0700. There is no TLS and no at-rest
encryption; see [Security boundary](#security-boundary).

## Build

```sh
cargo build --offline --release
# binary: target/release/coop
```

`--offline` works when the pinned dependencies are already in the Cargo cache.
Drop it on a machine with network access.

## Quickstart

### 1. Start the daemon

```sh
# Optional; defaults to $HOME/.local/share/cooperators
export COOPERATORS_HOME="$HOME/.local/share/cooperators"

./target/release/coop daemon run
```

The daemon listens on `run/daemon.sock` and, unless disabled, on
`http://127.0.0.1:7420` (`POST /mcp`, `GET /healthz`, `GET /metrics`).
Set `COOP_HTTP_ADDR=off` or pass `--http-addr off` to run UDS-only.

### 2. Register two agents

Run these in separate shells (or capture the JSON):

```sh
./target/release/coop register alice --kind codex
./target/release/coop register bob   --kind claude
```

Each registration prints `agent_id` and a one-time `token`. Save the token;
only its SHA-256 digest is stored. Export it for the CLI:

```sh
export COOP_TOKEN='<alice token>'
```

Re-registering the same name with its current token rotates the token and keeps
the same `agent_id`.

### 3. Send, wait, and ack

```sh
# As alice
./target/release/coop send --to bob --text "please review the plan"

# As bob
COOP_TOKEN='<bob token>' ./target/release/coop wait --timeout 30
COOP_TOKEN='<bob token>' ./target/release/coop ack --cursor '<next_cursor from wait>'
```

`wait` is a peek: it never consumes messages. `ack` advances the per-agent
cursor. Acknowledged cursors are in memory; clients that need exactly-once
consumption across a daemon restart should persist the last cursor they acked
and pass it explicitly.

### 4. Inspect

```sh
./target/release/coop status
COOP_TOKEN='<token>' ./target/release/coop agents --include-stale
```

## MCP client configuration

The primary integration path is `coop mcp-stdio`: each MCP client spawns a
small adapter that forwards tool calls to the daemon over the Unix socket.
Each independent launch enrolls a fresh instance, even when configurations
contain the same `COOP_TOKEN`. Socket reconnects within that process retain its
credential. Use `--resume` to explicitly reuse `COOP_TOKEN`/`--token`.
`--kind codex --display-name codex` gives repeated human labels while IDs and
routing aliases stay unique. `coop_instance_metadata` returns that identity;
`coop_retire_agent` revokes it permanently. Enrollment retries reuse one launch
key; an old launch key cannot restore a rotated or retired credential.

### Codex

Add to `~/.codex/config.toml` (use the absolute path to your build):

```toml
[mcp_servers.cooperators]
command = "/home/you/Repos/Cooperators/target/release/coop"
args = ["mcp-stdio", "--kind", "codex", "--display-name", "codex"]

[mcp_servers.cooperators.env]
COOP_SOCKET = "/home/you/.local/share/cooperators/run/daemon.sock"
```

### Claude Code / Claude Desktop

Add to the client's `mcpServers` configuration:

```json
{
  "mcpServers": {
    "cooperators": {
      "command": "/home/you/Repos/Cooperators/target/release/coop",
      "args": ["mcp-stdio", "--kind", "claude", "--display-name", "claude"],
      "env": {
        "COOP_SOCKET": "/home/you/.local/share/cooperators/run/daemon.sock"
      }
    }
  }
}
```

### Streamable HTTP (optional)

Clients that speak streamable HTTP can use:

- URL: `http://127.0.0.1:7420/mcp`
- Header: `Authorization: Bearer <agent token>`

The HTTP endpoint is stateless: every request must carry the bearer token,
including `initialize`. If the port is already in use, the daemon keeps serving
UDS/stdio and reports `http_listener: false` from `/healthz`.

`coop mcp-stdio --embedded` bypasses the daemon and serves a bus root
in-process. It is for tests and single-process debugging only; the daemon must
not be running against the same root.

## Tool surface

All tools are prefixed `coop_`, for example `coop_register_agent`,
`coop_send_message`, `coop_wait_message`, `coop_ack`, `coop_publish`,
`coop_create_queue`, `coop_claim_task`, `coop_publish_doc`, and
`coop_get_status`, `coop_instance_metadata`, and `coop_retire_agent`.
The original broker tools are specified in design §9; instance lifecycle
behavior is described in design §4.1.

## Environment variables

| Variable | Used by | Meaning |
|---|---|---|
| `COOPERATORS_HOME` | daemon, CLI, stdio adapter | Bus root. Overrides `--root`; defaults to `$HOME/.local/share/cooperators`. |
| `COOP_SOCKET` | daemon, CLI, stdio adapter | Explicit Unix socket path. Overrides `<root>/run/daemon.sock`. |
| `COOP_TOKEN` | CLI, stdio adapter with `--resume` | Agent token when `--token` is omitted. Fresh stdio launches enroll independently. |
| `COOP_HTTP_ADDR` | daemon | HTTP MCP listen address. `off`/`none` disables it. CLI `--http-addr` wins. |

Command-line flags: `--root <DIR>`, `--token <TOKEN>` (global), and
`coop daemon run --http-addr <ADDR>`. `--embedded` belongs to `mcp-stdio`.

## Backpressure

Each inbox has a pending-message cap (default 10 000 messages or 64 MiB of
serialized bytes). A send that would exceed either limit fails with
`RESOURCE_EXHAUSTED` (-32003); the durable log is not appended. Waiting does
not free capacity; acking does. Deduplicated retries still return the original
receipt while an inbox is full.

The caps are fields on `DaemonConfig` (`max_inbox_messages`,
`max_inbox_bytes`). Stdio messages are capped at 4 MiB, matching the rmcp HTTP
body limit; oversized stdio tool calls return `RESOURCE_EXHAUSTED`.

Direct messages use bounded per-inbox write queues. The defaults are 1,024
waiting requests or an 8 MiB serialized-size estimate per inbox, plus an active
batch of up to 64 records or 256 KiB. A larger admitted message forms a batch
alone. A full write queue also returns `RESOURCE_EXHAUSTED` before appending
that request. Inbox backlog capacity is checked exactly when the worker admits
each request, including earlier admitted records in its batch.

Concurrent sends to one inbox share a covering fsync; separate inboxes can
write concurrently. Collection waits at most 250 microseconds from the oldest
queued request, in addition to scheduling and disk time. Success is returned
after the covering sync. A send may still commit after its caller disconnects;
retry with the same idempotency key to obtain its original receipt.

## Health and metrics

```sh
curl -s http://127.0.0.1:7420/healthz | jq
# {"status":"ok","version":"0.1.0","uptime_s":12,"http_listener":true}

curl -s -H "Authorization: Bearer $COOP_TOKEN" \
  http://127.0.0.1:7420/metrics
```

`/healthz` is unauthenticated and reports listener liveness. `/metrics` requires
a valid bearer token and exposes message-log heads, inbox backlog, queue depths,
active leases, agent states, and HTTP request counts in Prometheus text format.
Metric names and operator procedures are in [`docs/operator.md`](docs/operator.md).

## Security boundary

- The trust boundary is the local machine plus filesystem permissions: UDS
  0600, bus root 0700, `registry.json` 0600. Any local process that can open
  the socket still needs an agent token.
- HTTP is loopback-only by default and bearer-authenticated. Do not expose it
  through a proxy or bind it to a non-loopback address without adding TLS and
  an authentication story.
- Tokens are bearer credentials: whoever reads one can act as that agent.
  Protect MCP client configs and environment variables.
- There is no message encryption and no encryption at rest. The append-only
  JSONL logs are readable by the account that owns the bus root.
- `--embedded` and the daemon both hold the root ownership lock before replay.
  A competing writer is refused even when it uses a different socket.

Channel, queue, document, and dead-letter streams rotate at
`DaemonConfig::log_rotation_bytes` (default 256 MiB). Sealed segments live in
`.segments/<original-filename>/`; logical cursor filenames and global sequence
numbers stay stable. Inbox and expiry logs remain unsegmented. After an
uncertain write or sync failure, the writer requires reopening and recovery
before another append.

## Tests

```sh
cargo test --offline
cargo test --offline --release -- --ignored --nocapture   # perf smoke
cargo fmt --all -- --check
```

Socket-dependent tests probe-and-skip in sandboxes that deny `AF_UNIX`; set
`COOP_REQUIRE_UDS=1` on a host that must run them.
