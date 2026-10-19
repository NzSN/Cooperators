# Actual TCP HTTP qualification smoke

The [retained run](actual-loopback-run/results.json) passed **19/19 checks** on
2026-10-19 using the existing full `target/debug/coop` executable. This smoke
opened real loopback TCP connections and a real daemon UDS connection. It did
not invoke the in-memory HTTP handler, rebuild Cargo targets, or modify
production/test source.

The daemon PID was `409816`; UDS `daemon.status` returned that same PID. Its
actual TCP listener was `http://127.0.0.1:39151`. The running executable, hashed
through `/proc/409816/exe` before termination, was:

```text
38d17d79c3c2d5ac72fbd189e13bb9f61064f4255a34d8578ce7460553589ee6
```

The smoke verified:

- `/healthz` reports `http_listener: true` before and after the requests.
- Missing and invalid bearer credentials receive HTTP 401 for `/mcp` and
  `/metrics`, with the authentication challenge and canonical error.
- A bearer-authenticated MCP initialize and initialized notification succeed.
- Actual TCP `tools/list` exposes 23 distinct tools matching the source catalog.
- HTTP instance metadata matches the real UDS enrollment and metadata response.
- HTTP heartbeat identifies that active instance; metrics reports the active
  listener, one registered agent, and observed HTTP authentication failures.
- HTTP retirement persists the same instance as retired. Its old credential
  then receives HTTP 401 for both MCP and metrics, and UDS also rejects it.

[wire-redacted.jsonl](actual-loopback-run/wire-redacted.jsonl) retains actual
request/response statuses, headers, bodies, TCP peers, and UDS messages with
tokens, launch secrets, and session identifiers redacted. A scan confirmed the
actual token and launch secret occur in no retained artifact. Raw regular bus
files are preserved in [raw-root](actual-loopback-run/raw-root/); socket entries
are omitted. The daemon was terminated and waited on after the test.

[results.json](actual-loopback-run/results.json) records the PID, endpoint,
script hash, source hashes, checks, executable identity, cleanup status, and
artifact hashes. The broader build/source pin is
[source-freeze.json](../source-freeze.json).

To repeat against another existing build, select a new output directory:

```sh
python3 docs/impl/generated-fault-mbt/qualification/http-smoke/http_smoke.py \
  --binary /absolute/path/to/coop --output /tmp/cooperators-http-smoke-new
```

The [standard-library script](http_smoke.py) binds the daemon to port zero,
discovers its assigned loopback port from the actual listener log, uses a fresh
short bus root under `/tmp`, and preserves evidence after shutdown. It requires
host permission to bind TCP/UDS sockets. This is focused transport and
authentication acceptance, not exhaustive HTTP/MCP conformance, concurrent
client stress, or TLS qualification.
