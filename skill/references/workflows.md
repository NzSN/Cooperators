# Coordination workflows

Examples show MCP tool names and JSON arguments. Namespaces may vary with the
MCP connector. Inspect the real schemas; fields from newer operation contracts
must be supplied when required. For the local UDS helper, use the native method
name (`send_message`, `claim_task`, `read_doc`, etc.); `coop_get_status` maps to
`daemon.status`, not `get_status`.

## Direct messages

```text
coop_instance_metadata {}
coop_list_agents {"include_stale":true}
coop_send_message {"to":"recipient-routing-name-or-id","kind":"request","text":"Review document plan@3 and reply with findings.","idempotency_key":"review-plan-v3-request-1"}
coop_wait_message {"timeout_s":25}
coop_ack {"cursor":"EXACT_NEXT_CURSOR_FROM_HANDLED_PAGE"}
```

Use `reply_to` with the original message ID when replying. Keep the sender
identity and the complete logical request fixed when retrying a deduplicated send.
A durable send receipt means the broker committed the message, not that the
recipient read it or completed the work. Persist the last handled inbox cursor
when recovering across daemon restart. ACK only the contiguous handled prefix.

The corresponding CLI subset is:

```sh
coop send --to recipient --kind request --text 'Review document plan@3' --idempotency-key review-plan-v3-request-1
coop wait --timeout 25
coop ack --cursor 'EXACT_NEXT_CURSOR_FROM_HANDLED_PAGE'
```

An ACK for the ordinary inbox does not acknowledge the arrival feed.

## Channels

```text
coop_publish {"channel":"reviews","text":"plan@3 is ready","idempotency_key":"plan-v3-ready"}
coop_read_channel {"channel":"reviews","limit":64,"wait_s":0}
```

Save `next_cursor` and pass the returned value as `cursor` on the next read.
Cursors belong to their channel; preserve the file/sequence tuple if returned.
Channels are replayable event streams, not exclusive task assignment. Use a queue
when only one worker should own an item.

## Queue producer and worker

Producer:

```text
coop_create_queue {"queue":"reviews"}
coop_enqueue {"queue":"reviews","payload":{"document":"plan","version":3},"max_attempts":3}
```

Worker:

```text
coop_claim_task {"queue":"reviews","lease_ttl_s":60}
coop_renew_lease {"lease_id":"EXACT_RETURNED_LEASE_ID"}
coop_complete_task {"lease_id":"EXACT_RETURNED_LEASE_ID","fence":123,"outcome":{"status":"reviewed"}}
```

Replace the fence with the actual value from the claim; `123` is illustrative.
A failure uses `coop_fail_task` with the same lease/fence plus `reason`. An empty
queue is a no-work outcome (`NOT_FOUND` in the legacy contract), not permission
to invent a task. Inspect its error reason; a missing queue is also `NOT_FOUND`.
A task notice is only a hint. Claim before acting, renew before expiry, and stop
using stale authority on `LEASE_LOST`. Retried claims may select different work
unless the deployed broker's durable operation contract proves otherwise.
Protect external effects with application idempotency/fencing as appropriate.

For a local bus without configured MCP:

```sh
python3 /path/to/skill/scripts/rpc.py create_queue '{"queue":"reviews"}'
python3 /path/to/skill/scripts/rpc.py enqueue '{"queue":"reviews","payload":{"document":"plan","version":3}}'
python3 /path/to/skill/scripts/rpc.py claim_task '{"queue":"reviews","lease_ttl_s":60}'
```

There are no `coop enqueue`, `coop claim`, or `coop doc` CLI commands in the
exported N2 build. Use MCP or native RPC instead of guessing subcommands.

## Versioned documents

```text
coop_publish_doc {"name":"plan","title":"Plan","text":"First revision"}
coop_read_doc {"name":"plan"}
coop_claim_doc {"name":"plan","lease_ttl_s":60}
coop_update_doc {"name":"plan","base_version":1,"text":"Reconciled revision","lease_id":"EXACT_RETURNED_LEASE_ID","fence":123}
coop_release_doc {"lease_id":"EXACT_RETURNED_LEASE_ID","fence":123}
```

Use the version actually read and the returned lease/fence. Re-read if the claim
reports a different head version. A claim is optional for ordinary CAS updates;
while an active claim exists, writes must satisfy its ownership/fence contract.
Publishing creates the initial document; changing an existing document uses
update. Send a versioned reference such as `plan@3` for a pinned handoff, and read
that version explicitly with `{"name":"plan","version":3}` when available.
On `VERSION_CONFLICT`, reread/reconcile; do not force an old version over the head.

## Unmanaged arrivals

Requires an arrival-enabled broker and permission for the chosen operations:

```text
coop_subscribe_arrivals {"client_key":"review-plan-v1","resource":{"type":"document","name":"plan"},"event_kinds":["doc.published","doc.updated"],"catch_up":true}
coop_subscribe_arrivals {"client_key":"review-queue-v1","resource":{"type":"queue","name":"reviews"},"event_kinds":["queue.task_available"],"catch_up":true}
coop_wait_arrivals {"timeout_s":25,"limit":64}
coop_ack_arrivals {"cursor":"EXACT_NEXT_CURSOR_AFTER_HANDOFF"}
```

Subscription resources are exact names. Reusing a client key retries the same
subscription definition; choose another key when its meaning changes. Catch-up
is enabled by default. Transfer the returned notices into your recoverable work
state, read/claim their sources, and ACK only when progress will survive a
reconnect. Managed agents use controller activation/resolution instead of this
manual wait/ACK loop.

The CLI supports `arrivals subscribe --client-key ... --document NAME` or
`--queue NAME`, `arrivals list`, `arrivals wait --timeout 25 --limit 64`,
`arrivals ack --cursor ...`, and `arrivals unsubscribe --subscription-id ...`.
`--future-only` omits catch-up. N2 remote subscription mutation and ACK are gated.

## Errors and bounded recovery

| Result | Response |
|---|---|
| `UNAUTHENTICATED` | Check the intended identity, expiry and revocation; do not re-register blindly. |
| `FORBIDDEN` | Respect resource/method/exposure policy; report the unavailable capability. |
| `RESOURCE_EXHAUSTED` | Reduce request/page size or wait for capacity; ACK already processed backlog when appropriate. |
| `VERSION_CONFLICT` / `CONFLICT_CLAIMED` | Read current document/claim state and reconcile or wait. |
| `LEASE_LOST` | Stop using the old lease/fence; reacquire/reconcile before further effects. |
| `OUTCOME_UNKNOWN` or a lost mutation response | Retain the original operation context and investigate effects before retrying. |

On remote N2 lists, use at most 64 items and pass the exact returned object
`next_cursor`; continue until an empty page returns null. Inbox/arrival cursors
are strings and channel cursors may be objects: do not interchange them. Local
legacy lists can return a complete list without `next_cursor`. Report observed
receipts, task/document versions and unresolved failures, never credentials.
