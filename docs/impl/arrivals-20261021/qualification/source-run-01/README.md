# Source integration qualification, run 01

Command: `cargo test --offline --locked --test arrival_sources`

Result: exit 0; six tests passed, zero failed, 1.33 s test runtime.

Coverage: absent-intent historical typed hash round-trip; notifications-disabled source shape; document route capture/removal and version/hash snapshots after replay; enqueue, explicit failure and forced expiry; bounded queue current-state snapshots including empty queues and replay; automatic expiry through claim, renew, complete, fail and the sweeper. The expiry checks verify committed routing intent even when the requested operation subsequently returns `LEASE_LOST`.

`cargo-test.log` contains the command output. `checkout-head.txt` records the committed base; this run includes uncommitted implementation work. `sha256.txt` pins the source modules, focused integration test, lockfile and compiled CLI present for this run. This is source/broker qualification; it does not qualify a provider host adapter.
