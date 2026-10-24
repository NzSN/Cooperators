# Merge validation, 2026-10-24

Merged `checkpoint/n2-local-authority-20261023` (`0e50c3b`) into independently
advanced main (`804781c`) at the user's request. This is integration validation,
not a renewed N2/N3 qualification or a network release acceptance.

Conflict resolutions retain main's N3 operation formats and feature gates,
byte-bounded authorized read pages, current-policy checks during waits, and
registry recovery sync. The branch adds durable policy revisions, bounded
principal history, MCP pagination schemas and the exported coordination skill.
The registry retains both durable principal tombstones and approved policy
history; authority-only N2 roots can load without losing identity or revocation
history. Changed approvals must advance their revision, including in test
fixtures. Main's pagination ends with a null continuation when no rows remain.
Both historical fault-point names are retained for crash controls.

N2-security, N2-authorization and N3a receipts continue to describe their own
source manifests. N3b remains paused and unqualified as documented in CHECKPOINT.
The pre-existing untracked editor cache is excluded.

## Results on the final merged source

- `cargo check --offline --locked --tests` passed during integration; both final
  test runs below compiled the final source. Targeted Rust formatting and the
  whitespace audit passed, excluding preserved historical command logs.
- [Normal full suite](normal-03/receipt.json): 308 passed, one failed, one existing
  ignored performance test; zero reported skips. The single failure is the
  already documented N3b test
  `incidental_claim_expiry_never_becomes_a_terminal_no_effect_receipt`: actual
  `LEASE_LOST` versus stale expected `OUTCOME_UNKNOWN`. Its assertion remains
  unchanged. The full suite remains failed; this merge does not finish N3b.
- [Selected injection suite](injection-03/receipt.json): 160 passed, zero failed,
  required sockets/TLS and zero reported skips; run sequentially. It includes
  policy replacement, migration SIGKILL controls, operation source recovery,
  real TLS lost replies, journal recovery and MCP identity/bounds tests.
- Both final receipts identify identical source manifests and report unchanged
  source. `source_commit` is the pre-merge main parent because the merge was
  still pending; use the file manifests for the tested source.

Two injection controls were explicitly filtered out: the pre-operation and
send-only executable refusal tests. Their historical executables from the other
machine are unavailable here. The available pinned pre-network executable
refusal controls ran. The selected injection result is not a complete injection
or N3 qualification. Generated model replay and topology/provider qualification
were outside this merge check.

[Concurrent diagnostic](injection-02/receipt.json) retained one crash-control
failure (child exited before boundary; 159 other tests passed). That control
passed an isolated diagnostic and the final sequential suite. The underlying
concurrent timing cause was not established. The final source is identical in
this diagnostic and the two final receipts.

See [summary.json](summary.json) and the accompanying logs for exact commands,
filters, counts and file hashes. Both parents' historical N2/N3 receipts remain
unchanged and retain their original source scopes.
