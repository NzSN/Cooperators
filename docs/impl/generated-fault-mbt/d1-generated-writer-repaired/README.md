# D1 generated writer-port repair verification

The same four compiler-generated writer traces that previously produced three
real mismatches now pass: **4/4, exit 0**. The actual full MBT binary is identified
in [source-build-identity.json](source-build-identity.json); this run includes the
root ownership lease and recovery-prefix synchronization repairs.

[replay.log](replay.log) records all outcomes. [manifest.json](manifest.json)
hashes the retained artifacts. `fixtures/` contains raw before/after-start
snapshots, every actual observation, final bus files, and child stderr. Regular
`run/owner.lock` files are retained; socket entries are skipped by file type.
Socket files live in short `/tmp/cwf-sockets-*` fixture directories, permitting
the durable bus roots to remain directly under this long archive path.

The observations verify rejected same/different-socket starts leave the active
writer's data intact, an unowned append attempt is refused by the real startup
guard, and ownership handoff preserves records. The earlier
[generated-port baseline](../d1-generated-writer-baseline/README.md) retains the
three corresponding failures. The original five-trace writer suite also passed
after the lease repair; its log is retained here as
[original-writers-repaired.log](original-writers-repaired.log).

These checks execute production daemon startup and actual UDS calls with HTTP
disabled. They do not qualify HTTP, simultaneous-start races, or arbitrary
direct users of the low-level storage API. The canonical models remain the
transition oracle; declaration-only interface views supply compiler type/dispatch
bindings.
