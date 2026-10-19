# Expanded generated identity acceptance

Eight identity traces and 35 states passed through the compiler-generated MBFaultIdentityPort binding against actual broker RPCs and registry files. The replay log, per-step raw registry snapshots, redacted RPC requests/replies, normalized observations and independently constructed actual-ID/credential-hash bijections are retained. Plaintext credentials are not retained.

The scenarios cover same-kind distinct allocation, initial retry, reconnect, rotation/revoked auth, stale-key retry rejection, retired-key retry rejection, fresh allocation after retirement and retired-credential rejection. This generated profile does not observe message delivery, acknowledgments, dedupe, presence counters, cross-instance lease effects, registry I/O failures, or actual credential entropy. Direct and real MCP process acceptance cover the protocol interactions separately.

manifest.json identifies the executed binary, relevant sources and evidence files; the final repository-wide qualification has its own freeze identity.
