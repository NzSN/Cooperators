# D8 identity baseline

This is a feature-gap baseline captured before editing `src/registry.rs`.
It is not a generated-port MBT reproduction of a broker defect.

The retained test registers one legacy name, authenticates two callers using
that same credential, and reads the actual `registry.json`. Both callers resolve
to the same concrete ID and the file has one agent record. This is correct for
the legacy credential API; it cannot independently provision two new launches
from shared kind/display configuration. The per-launch enrollment API did not
exist in the retained registry source.

`baseline.log` records the live result. `registry.rs` and `instance_identity.rs`
are the source/test snapshot, and `manifest.json` hashes those retained files.
The test was compiled directly against the existing full-crate Cargo artifacts
because a concurrent Cargo build held the artifact lock. That library's binary
hash was not retained. The later ordinary offline Cargo baseline run also passed.

The new implementation requires a fresh random launch key per independent
launch. It stores only that key's hash, supports durable retries with the same
initial credential, and gives same-kind/same-display launches different actual
IDs, tokens and routing aliases. Shared legacy credentials continue to identify
one principal by design.
