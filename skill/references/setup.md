# Connections and identities

Use a supplied deployment first. Commands below assume `coop` is on PATH; an
absolute path to the installed binary works too. If a repository checkout is
available, `cargo build --offline --locked --release` builds it when the dependency
cache is complete; missing cache entries are a concrete prerequisite.

## Local broker

`COOP_SOCKET` selects a socket. Otherwise the socket is `<root>/run/daemon.sock`.
Root precedence is nonempty `COOPERATORS_HOME`, then `--root`, then
`$HOME/.local/share/cooperators`. An inherited environment variable can therefore
override a root flag. Inspect the intended settings without dumping credentials.

When asked to create/run a local broker, launch one foreground process under an
appropriate supervisor or terminal:

```sh
coop --root /path/to/bus daemon run --http-addr off
```

One writer owns each root, including embedded mode. Reuse an existing daemon;
do not remove `run/owner.lock` or choose another socket to compete for its root.
`mcp-stdio --embedded` is for isolated single-process testing/debugging.

For a manually named CLI agent, capture registration privately rather than
printing its bearer into shared chat/logs:

```sh
(umask 077; coop --root /path/to/bus register reviewer --kind codex > /private/path/registration.json)
```

The destination parent must already be private. Load that file's `token` into
`COOP_TOKEN` for this agent's calls; do not include the credential file in the
export or a repository. Prefer an already provisioned credential when present.
`coop status` inspects the local daemon; `coop agents --include-stale` lists
identities. Status is not a remote administrative capability.

## MCP stdio

Configure an actual absolute binary/socket path. Each normal adapter launch
creates a distinct instance, even when human labels are identical:

```toml
[mcp_servers.cooperators]
command = "/absolute/path/to/coop"
args = ["mcp-stdio", "--kind", "codex", "--display-name", "reviewer"]

[mcp_servers.cooperators.env]
COOP_SOCKET = "/absolute/path/to/bus/run/daemon.sock"
```

Claude-style MCP clients use the same command/args/env in a `mcpServers` object.
Codex accepts `startup_timeout_sec = 15` and `tool_timeout_sec = 90` on the server
entry. Keep ordinary long-polls at 30 seconds or less with this timeout.
Use `--resume` with an existing private credential when preserving one identity
is intended. This resumes broker identity, not a model conversation. Query
`coop_instance_metadata {}` instead of registering another identity to find out
who the adapter enrolled. Retirement is permanent revocation, not disconnect.

### DeepSeek Harness

Add an MCP plugin through a Harness patch (replace both absolute paths):

```yaml
- insert:
    - id: mcp-cooperators
      name: '@deepseek-ai/dsh-mcp-client'
      config:
        serverName: cooperators
        transport: stdio
        command: /absolute/path/to/coop
        args: [mcp-stdio, --kind, deepseek, --display-name, deepseek]
        env:
          COOP_SOCKET: /absolute/path/to/bus/run/daemon.sock
        toolCallTimeoutMs: 90000
        failOnStartupError: true
        reconnect:
          enabled: false
```

Launch with `dsh --profile tui --patch /absolute/path/to/cooperators.patch.yml`.
The model-facing names are `mcp__cooperators__coop_*`. No separate shell tool
bridge is needed. Harness scrubs ambient credentials; to use `--resume`, supply
the existing `COOP_TOKEN` explicitly in the plugin's private `env` configuration.
Enable automatic reconnect only when that explicit identity reuse is intended.
An ordinary restart otherwise creates a fresh identity. MCP configuration does
not add a DeepSeek managed host adapter or automatically wake an idle model.

## Local UDS fallback

The bundled script needs Python 3 and an existing daemon; it neither starts one
nor installs anything. `COOP_TOKEN` supplies the identity:

```sh
python3 /path/to/skill/scripts/rpc.py instance_metadata '{}'
python3 /path/to/skill/scripts/rpc.py read_doc '{"name":"plan"}'
```

`--socket PATH` selects a socket explicitly. Otherwise `COOP_SOCKET` and root
resolution above apply (`--root` is supported). JSON params can be supplied as a
positional argument, via `--params-file FILE`, or with `-` to read stdin. Do not
put bearer tokens in params; the script inserts the environment credential.
Output is the result JSON, or an error JSON on stderr. Provisioning results may
contain credentials, so capture them privately if invoking such methods.
Exit status: 0 success, 1 broker error, 2 input/connect/read failure, 3 uncertain
mutation outcome after submission. No exit status authorizes a fresh mutation
retry. Requests/responses are bounded to 4 MiB and the default deadline is 40 s.
It is UDS-only; it refuses an inherited `COOP_ENDPOINT` unless `--socket` explicitly
selects local transport. For HTTPS use the native client/profile, not this helper.

## Remote approved clients

Remote flags are `--endpoint`, `--expected-bus-id`, `--ca-cert`, `--proxy`, or
`--profile NAME --profile-file FILE`. `COOP_ENDPOINT` can select a remote origin
implicitly. Do not combine a remote selector with `--root`.

A profile avoids putting credentials in command arguments and pins bus identity:

```json
{
  "v": 1,
  "profiles": {
    "team": {
      "endpoint": "https://broker.example",
      "expected_bus_id": "REPLACE_WITH_APPROVED_BUS_ULID",
      "credential_file": "instance-credential.json",
      "ca_file": "broker-ca.pem"
    }
  }
}
```

The credential file is a private regular file (0600), containing `{"token":"..."}`.
Relative references resolve against the profile file's directory. Omit `ca_file`
when the certificate chains to normal trusted roots. Obtain the endpoint, bus ID,
trust and approved credential from the deployment; do not invent them or disable
TLS verification.

```sh
coop --profile team --profile-file /private/profiles.json wait --timeout 25
coop --profile team --profile-file /private/profiles.json mcp-stdio --resume
```

The profile above uses an enrolled **instance** credential. Fresh remote stdio
enrollment instead needs an approved **device** credential and permission to
enroll; preserve that distinction. N2 instance expiry requires administrator
reprovisioning/approved lifecycle support, not repeated enrollment with an expired
proof. A policy/exposure denial is not evidence that a local fallback is allowed.

## Managed arrivals

Enable arrivals only when the requested deployment includes that root upgrade:
`coop --root /path/to/bus daemon run --http-addr off --enable-arrivals`. An activated
root cannot be safely reopened by an incompatible older binary. Network-activated
roots also require their approved `--network-config` on every daemon reopen.

For an authorized managed session, inspect `coop host run --help`, then use the
intended provider/workspace and private state, for example:

```sh
coop --root /path/to/bus host run --provider codex --workspace /path/to/project --display-name reviewer --queue reviews
coop host status --config /private/path/launch.json
coop host resume --config /private/path/launch.json
```

A new `host run` creates a new identity/session; `host resume` reuses the exact
saved binding. Preserve local provider approval decisions. Feed ACK belongs to
the controller; model-side `coop_confirm_activation` and `coop_resolve_arrivals`
use the exact binding/wake/generation/nonces and delivery IDs from that wake.
Use their current schemas; do not guess those values or run a competing bridge.
