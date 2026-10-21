# Installed Claude MCP discovery diagnosis

Date: 2026-10-20. Installed Claude Code: 2.1.300. Cooperators: 0.1.0,
using `rmcp` 3.2.0. This diagnosis made no production code or user configuration
changes.

## Finding

Cooperators advertises MCP protocol version `2026-07-28`, but its `tools/list`
response omits that revision's required `ttlMs` and `cacheScope` fields. The MCP
SDK bundled with Claude validates both fields before exposing tools. Connection
discovery succeeds, then tool listing fails, leaving a connected server with
zero model-visible tools.

This is a response-shape incompatibility. The `resultType: "complete"`
discriminator is present and valid. The absence of an `initialize` request in
the failing session is expected: the modern protocol uses `server/discover`
and per-request metadata.

## Evidence

- [Modern wire transcript](attempts/claude-modern-mcp.jsonl), line 1: Claude
  probes `server/discover`, requesting protocol `2026-07-28`.
- The same transcript, line 2: Cooperators offers that version and returns
  `ttlMs: 0` and `cacheScope: "private"` on the discovery response.
- Lines 4, 6, 8 and 10: each `tools/list` response contains exactly the result
  keys `resultType` and `tools`, with all 23 tools. Neither required cache
  field is present.
- [Modern Claude events](attempts/claude-modern-events.jsonl), line 1: the
  server is connected, while the initialization tool inventory contains zero
  `mcp__cooperators__...` tools.
- [Cooperators handler](../../../../src/mcp.rs#L584) constructs the response
  through `ListToolsResult::with_all_items(...)` without setting cache fields.
- Installed `rmcp` source,
  `/home/nzsn/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/rmcp-3.2.0/src/model.rs`,
  lines 1600–1613: the field documentation says both fields are required by
  `2026-07-28`; the Rust fields are optional for older protocol compatibility
  and omit `None` values during serialization. Lines 1623–1631: the constructor
  initializes both to `None`.
- Installed Claude binary,
  `/home/nzsn/.local/share/claude/versions/2.1.300`, contains readable bundled
  JavaScript. Its modern `ListToolsResultSchema` requires a nonnegative integer
  `ttlMs` and a `cacheScope` enum of `public` or `private`. The schema occurs
  in the embedded `Fc()` module near byte offset 253715000. Modern
  `decodeResult()` near byte offset 253737564 validates this schema and returns
  an invalid-result error when validation fails.

The proxy in `scripts/record_mcp_stdio.py` records a scrubbed copy and forwards
the original bytes. Redacted credential property names in retained evidence
therefore do not affect the actual schema received by Claude.

## Compatible launch used for acceptance

Set this environment variable on the Claude process for the existing host-mode
bridge command:

```sh
MCP_PROTOCOL_NEGOTIATION=legacy
```

This opt-out is explicitly supported in the installed Claude source and its
bundled changelog. It selects the older initialization handshake without
changing persistent user settings.

The comparison succeeded: Claude negotiated `2025-11-25`, exposed all 23 tools,
and issued a native `coop_instance_metadata` tool call through the host
permission protocol. The successful session's
[wire transcript](claude-mcp.jsonl) begins with `initialize`, and its first
`tools/call` is `coop_instance_metadata`. The local bridge debug log records
`protocolEra: "legacy"`, `negotiatedProtocolVersion: "2025-11-25"`, and successful
completion of that call. The debug log also confirms normalization of the five
schemas containing top-level `anyOf`; these schemas do not explain all tools
being absent in the modern attempt.

## Follow-up

The minimal production correction is to set an explicit cache lifetime and
scope on `ListToolsResult`, for example `with_ttl_ms(0)` and
`with_cache_scope(CacheScope::Private)`, and then repeat installed-client
acceptance with Claude's default modern negotiation. That correction was not
implemented as part of this diagnosis. Modern default negotiation remains a
known acceptance failure; the current installed-client exercise uses the
documented legacy launch compatibility setting.

The exact validation exception from the failing Claude process was not captured
because that attempt did not enable a debug file. The diagnosis is grounded in
the retained wire shape, both implementations' local source, and the successful
legacy comparison; no claim is made about unobserved modern tool-call behavior.
