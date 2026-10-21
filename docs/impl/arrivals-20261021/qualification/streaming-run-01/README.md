# Streaming storage regression qualification

Date: 2026-10-21. Result: **7 passed, 0 failed** with `mbt-injection`.

[Structured results](results.json) and the
[frozen source run log](storage-streaming-frozen.log) cover `open_streaming`,
`open_rotating_streaming` and `read_segments_streaming`:

- Records retain order, global sequence, hash chain, head and active-segment
  offsets across rotating and nonrotating replay; the next append continues
  the recovered chain.
- Read-only inspection preserves file bytes and creates no missing stream or
  parent directory.
- Torn active tails are reported without mutation, then repaired on open;
  sealed files remain unchanged. A valid final record without a newline is
  preserved and separated correctly from the next append.
- Invalid middle chains and corrupted sealed segments are fatal without
  mutating any segment. Visitor errors propagate without a usable log handle.
- A recovery sync failure after private visitation returns an error; a later
  real reopen recovers the uncertain complete prefix before allowing append.

The authoritative source and exact command are retained with the jointly run
[crash qualification](../crash-run-01/README.md):
[source hashes](../crash-run-01/source-freeze-02.json),
[immutable snapshot](../crash-run-01/source-snapshot-02/), and
[command/environment](../crash-run-01/command-02.json).

[The preliminary log](storage-streaming-attempt-01.log) also passed all 7 tests
against the then-current tree. Its unrelated temporary host-control import
warning is retained. The frozen run is the reproducible qualification source.
