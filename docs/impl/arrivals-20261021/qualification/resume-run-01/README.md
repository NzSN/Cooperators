# Arrival qualification after machine transfer

Date: 2026-10-21. Source base: `9844ea4` plus changes pinned in the command
receipts. **Broker correctness and scoped managed Codex acceptance pass.
Managed Claude acceptance remains unavailable on this machine.**

## Repairs

- Closed duplex transports reject buffered frames as well as new reads.
  Neither provider can process queued approvals, echoes or status changes after
  a fatal stream error. Codex `thread/closed` uses the same closure path.
  The original Claude race and new buffered-frame regressions passed after
  reproducing their failures.
- A new Codex thread is named `Cooperators managed session` before bootstrap
  exits. The installed App Server stages empty threads; naming persists its
  rollout so another process can resume the exact ID without a model turn.
  Metadata-only reads did not persist it. A subprocess regression covers this.
- Protocol fakes are immutable checked-in executables. A full run had still
  observed `Text file busy` with the earlier unique, synced scripts.
- The independent MCP schema contract covers all five arrival tools.
  `src/server.rs` also received import formatting required by rustfmt.

## Final gates

| Gate | Result | Evidence |
|---|---|---|
| Normal, all targets, required sockets | 181 passed; one pre-existing ignored performance test; no skips | `normal-final-02.log` and `.json` |
| Injection, all targets, required sockets | 230 passed; same ignored test; no skips; pre-upgrade binary supplied | `injection-final.log` and `.json` |
| Bounded arrival/bridge models | All 128 traces in both complete suites | `MODEL_TRACE` records in those logs |
| Generated protocol/writer/fault MBT | 63 traces, zero failures, including actual rotation crash controls | `generated-mbt-final.log` and `.json` |
| Generated fault binding/compiler checks | Passed | `generated-bindings-final.log` and `.json` |
| Arrival crashes and V2 refusal | Four actual SIGKILL controls and old-binary refusal passed; sixth test is a no-op child helper | Final injection log and crash archive |
| Managed Codex, real configured model | Three wakes observed/read/resolved; idle/busy, explicit approvals and exact-binding restart passed | `codex-release-run-01/summary.json`, `events.json`, `audit.json` |
| Managed Claude | Unavailable: installed 2.1.185 reports no authentication | `claude-unavailable/summary.json` |
| Release build, rustfmt and doctests | Passed; zero doctests | `release-build-final`, `fmt`, `doctests` receipts |

The release executable is `coop` 0.1.0, SHA-256
`732ffba442bd3c89ec9e7a3f66e2e01dba902d13d934dcc732560f6ece419965`.
The pre-upgrade control rebuilt `edcbc923` separately, with executable SHA-256
`095be02ea268cf28625dfc7d2e4e6834eabd8eff5e79743a9c766a386d4dbcf1`.
Neither executable was installed as a global replacement. Two missing pinned
crate archives were restored with checksum verification; Cargo stayed offline
and lockfiles unchanged.

Final receipts pin source/fixture bytes and record unchanged source during
execution. Provider/performance summaries additionally pin their harness and
binary. Earlier failing attempts remain diagnostic failures. Generated test
credentials were consistently redacted after capture;
`credential-redactions.json` maps original and published artifact hashes.
`SHA256SUMS` covers final published bytes.

## Installed Codex scope

Codex CLI 0.163.0 used its configured model/provider without a model override.
Qualification supplied per-launch `approval_policy="on-request"` and
`approvals_reviewer="user"`. Eleven requests were explicitly answered, including
MCP elicitation and one harmless `touch` command in the disposable root. No
standing permission grant was sent. The actual marker file was checked.

A second arrival was retained while the first wake was active. The model read
pinned versions 1, 2 and 3 through native MCP tools, confirmed their three
activation nonces and resolved all deliveries. The broker and controller were
restarted with the same binding, identity, generation, workspace and exact
thread. A repeated older resolution returned its original receipt. The
independent event audit verifies native inputs/results and the final empty
pending set. Disposable live broker files were removed; retained provider proof
is the protocol/results and controller-state record.

This qualifies real managed **document** activation, approval, observation,
resolution and one restart sequence. Real queue adoption, other restart
orderings, provider-side lost replies and broad liveness are outside this run.
Related deterministic tests are separate. Provider session artifacts use normal
CLI session storage; no global MCP registration or existing user bus was selected.

## Performance scope

| Measurement | Samples | p50 | p95 | Gate |
|---|---:|---:|---:|---|
| Document RPC, notifications disabled | 200 | 2.760 ms | 3.271 ms | Control |
| Document RPC, subscribed arrivals enabled | 200 | 2.841 ms | 3.416 ms | Observed overhead only |
| Source RPC start to committed feed observed over UDS | 200 | 6.468 ms | 7.634 ms | Conservative source-to-feed upper bound <100 ms: PASS |
| Source RPC start to accepted bridge submission observed, immediate protocol host | 30 | 213.625 ms | 239.233 ms | Conservative feed-to-host upper bound <1 s: PASS |

The source/feed run excludes ten warmups per condition. Admission excludes two
warmups and uses fresh roots with the production daemon, managed launcher,
journal and adapter plus an immediate **fake** App Server. Its read-only journal
witness requires an exact accepted turn ID and matching arrival/wake reference.
It invokes no provider/model. Intervals start before feed commit and end after
the relevant committed response is observed: these are conservative upper
bounds, not exact internal-hop timestamps. Model execution and human approval
time are excluded. See `performance-run-01/` and `admission-run-01/`.

One machine, one serial window per condition and bounded histories do not
qualify arrival throughput, representative memory, additional sync counts,
large-history startup or installed-provider admission percentiles. Earlier
distributed/100-agent and million-record startup limits remain open.

## Reproduce

From the repository root, choose fresh output directories. Socket gates need
host access; provider gates also need actual authentication. Rebuild the
pre-arrival executable separately if it is absent.

```sh
cargo build --offline --locked --release
COOP_REQUIRE_UDS=1 cargo test --offline --locked --all-targets -- --nocapture
COOP_REQUIRE_UDS=1 COOP_PREUPGRADE_BINARY=/path/to/pre-arrival/coop \
  cargo test --offline --locked --all-targets --features mbt-injection -- --nocapture
MIRROR_BIN=/path/to/ModelMirrors COOP_REQUIRE_UDS=1 \
  cargo run --offline --locked --manifest-path spec/mbt/Cargo.toml -- --skip-gen --suite all
python3 spec/mbt/generate_fault_interfaces.py --check
python3 scripts/qualify_arrivals.py provider --provider codex --output /tmp/new-codex-gate
python3 scripts/audit_arrival_provider.py /tmp/new-codex-gate
python3 scripts/qualify_arrivals.py provider --provider claude --output /tmp/new-claude-gate
python3 scripts/qualify_arrivals.py performance --samples 200 --output /tmp/new-feed-perf
python3 scripts/benchmark_arrival_admission.py --samples 30 --output /tmp/new-admission-perf
```

Remaining acceptance starts with authenticating/configuring Claude and testing
its real managed session. Its installed version differs from the earlier
2.1.300 baseline; qualify protocol compatibility afresh. Modern Claude MCP
discovery remains independently open.
