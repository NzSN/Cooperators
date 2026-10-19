# D3-D6 initial repair replay

The same six generated-port traces that produced five implementation failures
in `../d3-d6-runtime-baseline/` passed after these repairs:

- Inbox waits expose scanned expired records through an acknowledgeable
  cursor; batch limits still prevent skipping any unreturned live message.
- The ordinary queue sweeper reconstructs missing dead-letter projections
  from durable terminal task state. Task identity prevents a second projection
  after retry or crash.
- Document publication reserves a name after a committed version, allowing an
  interrupted empty file to be reused for version 1.
- Status expands its backward read until a complete final record is available.

Profile logs and per-step `raw/` files preserve this first green replay.
`binary-sha256.txt` identifies the binary. `runtime-tests.log` records five
targeted regression tests. This is an intermediate verification checkpoint;
later queue/document log rotation and logical status-head aggregation require
the final integrated qualification recorded by the parent execution report.

The generated adapters and fault activation mechanisms were retained across
the red and green runs. These results concern embedded broker operations and
actual files/process crashes, with `mbt-injection` enabled. They do not qualify
HTTP transport.
