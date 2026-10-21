# Native asynchronous UDS throughput control

Declared before any native measurement. The driver is
[`examples/throughput_native.rs`](../../../../examples/throughput_native.rs).
Its generated per-run `plan.json` records the final driver and executable
identities alongside the same declared workload.

The native control compares the retained baseline executable and a candidate
using one Rust driver. It helps distinguish broker throughput from overhead in
the frozen Python driver. It does not replace the Python qualification results:
those failures and their capacity errors remain valid evidence for that client
and workload. The native driver is an additional control with its own results.

## Workload and gates

Both topologies run at sender concurrency 1, 8, 32 and 100. Same-inbox uses C
enrolled senders and one additional receiver identity. Distributed-inboxes uses
C enrolled agents, each sending to itself, and a separate receiver connection
for each identity. Each sender has exactly one outstanding send. Every message
has kind `benchmark` and a `text/plain` body containing 128 ASCII `x` bytes.

Each fresh daemon/root receives an untimed enrollment followed by a one-second
warmup with at least three successful sends per sender. All warmup deliveries
are verified and acknowledged before measurement. A ready barrier followed by
a release barrier starts the 10-second closed loop. Requests admitted before
the deadline finish normally. Throughput counts their successful durable
receipts divided by time from release through the final successful receipt.
Catch-up after the window is outside that denominator.

An independent persistent receiver connection per destination continuously
drains the inbox. It repeatedly calls `wait_message(timeout_s=0)`, verifies each
returned default-size batch, acknowledges its cursor and repeats until empty.
It sleeps 20 ms only after an empty response. The default batch size 100,
10,000-message cap and 64 MiB cap remain unchanged. There are no client credits,
sender pacing or larger buffers. Final catch-up has a 30-second deadline.

Report **at least 5,000 durable acknowledgments/second** and **send p50 below
10 ms** independently for every topology/concurrency. Report nearest-rank
p50/p95/p99. Every target miss stays visible. Any phase error invalidates that
phase's performance qualification; a partial rate is descriptive only. The
aggregate gate fails on any selected phase error, correctness failure or target
miss. A filtered run is a diagnostic and cannot pass full qualification.

## Client and daemon settings

The Rust driver uses four Tokio worker threads, persistent UnixStream
connections and newline-delimited JSON-RPC 2.0. Every authenticated request
includes its token, matching the Python client. The UDS socket buffers and all
daemon options use their defaults except that HTTP is explicitly **off for
both native baseline and native candidate**. The frozen Python driver enables
an idle HTTP listener; this documented difference prevents silently treating
the native and Python result sets as the same experiment.

The selected release `coop` binary is started as a real child against a private
disposable `/tmp/ctn-*` root for each condition. The actual `/proc/PID/exe` hash
must match the selected binary. The driver owns termination/reaping through a
process guard on success and failure. It attempts all independent conditions
after a failed phase, retaining the phase errors. No existing root is opened
for daemon writes.

## Evidence and verification

Each phase retains every successful receipt and its client timing, every
validated delivery envelope, all receiver wait/ack timings and cursors, all
failed send attempts, daemon stdout/stderr, per-window driver/daemon resource
snapshots and deltas, and an unmodified copy of the stopped daemon's regular
root files. Data accumulated before a capacity error is saved. Invalid response
contents are retained with their validation error after credential redaction.
Only successful verified acks contribute to catch-up counters.

The driver checks JSON-RPC IDs/version/errors, receipt uniqueness and fields,
recipient and sender identities, the full payload, contiguous delivery
sequences and all cursor transitions. It confirms each receiver is empty at
its acknowledged head. After terminating and reaping the daemon, it verifies
every inbox with `LogFile<Envelope>::read_segments` and `storage::hash_record`.
It rejects torn tails, extra or missing records, hash or sequence gaps, and any
difference between a persisted record and its delivered envelope. The reader
does not create, truncate, repair or sync evidence. It never calls `LogFile::open`.

Tokens and launch keys stay in memory and the private daemon root only stores
their hashes. JSON evidence is redacted before writing. At the end, a byte scan
checks all retained regular files for every generated credential, including
substrings in longer hexadecimal runs. An unexpected leak is removed and
fails the audit. CPU/resource collection records `/proc` read failures instead
of substituting zero. CPU percentages use one logical CPU as 100%.

`environment.json` records host/filesystem, source hashes, driver source/binary
hashes, frozen Python helper hashes, method hash, selected binary hash and
optional retained build provenance. The baseline requires its original source
manifest and is not attributed to the current worktree. Candidate manifests
can use the existing qualification `files`/`release_binary_sha256` shape. The
same identities are checked again after the run; a change invalidates it.

`summary.json` records completion separately from target satisfaction. A
successful process exit means all requested phases completed without errors;
the target gate is the explicit `full_qualification_pass` field. Target misses
do not hide completed evidence. An error returns nonzero after all independent
phases have been attempted.

## Coordinated commands

Build only when the root agent releases the shared measurement host:

```sh
cargo build --release --example throughput_native
```

Use the same resulting driver binary for baseline and candidate:

```sh
target/release/examples/throughput_native \
  --binary /tmp/cooperators-step3-baseline/coop \
  --output docs/impl/throughput-20261020/native/baseline-run01 \
  --label baseline \
  --source-manifest docs/impl/acceptance-20261020/installed/source-manifest.json

target/release/examples/throughput_native \
  --binary /path/to/retained/candidate/coop \
  --output docs/impl/throughput-20261020/native/candidate-run01 \
  --label candidate \
  --source-manifest docs/impl/throughput-20261020/qualification/run-02/source-manifest.json
```

Optional `--profiles same-inbox` and `--concurrencies 100` select diagnostics.
Durations, minimum warmup, payload, drain interval and daemon limits cannot be
changed through this driver's CLI. Output directories must be new.

## Limits

These are one-host, one-filesystem, one-window-per-condition observations with
no confidence intervals. The client runtime and HTTP setting differ from the
frozen Python controls. CPU deltas alone cannot attribute a bottleneck to the
GIL, scheduling, sockets or storage. One outstanding C=1 sender cannot provide
group commit overlap. Closed loops do not establish a maximum independently
paced arrival rate. Readback confirms retained acknowledged effects and their
integrity; crash/power-loss durability requires the separate recovery evidence.
