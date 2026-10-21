#!/usr/bin/env python3
"""Check installed-client wire transcripts against the broker's durable files."""

import argparse
import hashlib
import json
from pathlib import Path


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def calls(path):
    pending = {}
    completed = []
    rejected = []
    for packet in rows(path):
        message = packet["message"]
        request_id = message.get("id")
        if packet["direction"] == "client_to_broker":
            if message.get("method") == "tools/call":
                assert request_id not in pending, "duplicate in-flight MCP request ID"
                pending[request_id] = message["params"]
        elif request_id in pending:
            request = pending.pop(request_id)
            if "error" in message:
                rejected.append({"tool": request["name"], "arguments": request["arguments"], "error": message["error"]})
                continue
            result = message["result"]
            value = result.get("structuredContent")
            if value is None:
                value = json.loads(result["content"][0]["text"])
            if result.get("isError", False):
                rejected.append({"tool": request["name"], "arguments": request["arguments"], "error": value})
                continue
            completed.append({"tool": request["name"], "arguments": request["arguments"], "result": value})
    assert not pending, f"unfinished MCP tool calls: {pending}"
    return completed, rejected


def check_chain(path):
    previous = "sha256:" + "0" * 64
    data = rows(path)
    for index, record in enumerate(data, 1):
        assert record["seq"] == index, (path, index)
        assert record["prev_hash"] == previous, (path, index, "broken hash link")
        value = {key: item for key, item in record.items() if key != "prev_hash"}
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        previous = "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
    return data


def unique(items, predicate, description):
    matches = [item for item in items if predicate(item)]
    assert len(matches) == 1, (description, len(matches))
    return matches[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    evidence = args.evidence
    scenario = json.loads((evidence / "scenario.json").read_text())
    nonce = scenario["nonce"]
    claude_id = scenario["claude_agent_id"]
    queue = check_chain(args.root / "queues" / (scenario["queue"] + ".jsonl"))
    enqueued = unique(queue, lambda x: x["kind"] == "task_enqueued", "enqueued task")
    claimed = unique(queue, lambda x: x["kind"] == "task_claimed", "claimed task")
    completed = unique(queue, lambda x: x["kind"] == "task_completed", "completed task")
    task_id = enqueued["task_id"]
    codex_id = enqueued["payload"]["requester_agent_id"]
    assert codex_id != claude_id
    assert enqueued["payload"]["nonce"] == nonce
    assert enqueued["payload"]["numbers"] == scenario["numbers"]
    assert claimed["task_id"] == completed["task_id"] == task_id
    assert claimed["agent_id"] == completed["agent_id"] == claude_id
    assert claimed["lease_id"] == completed["lease_id"]
    assert completed["outcome"] == {"nonce": nonce, "sum": scenario["expected_sum"], "worker_agent_id": claude_id}

    codex_calls, codex_rejected = calls(evidence / "codex-mcp.jsonl")
    claude_calls, claude_rejected = calls(evidence / "claude-mcp.jsonl")
    assert not claude_rejected
    assert len(codex_rejected) <= 1
    for rejected in codex_rejected:
        assert rejected["tool"] == "coop_send_message"
        assert "to" not in rejected["arguments"]
        assert rejected["error"]["code"] == -32602
        assert rejected["error"]["message"] == "missing field `to`"
    for actor_id, actor_calls in [(codex_id, codex_calls), (claude_id, claude_calls)]:
        assert any(x["tool"] == "coop_instance_metadata" and x["result"]["agent_id"] == actor_id for x in actor_calls)
        assert any(x["tool"] == "coop_heartbeat" and x["result"]["agent_id"] == actor_id for x in actor_calls)

    creation_call = unique(codex_calls, lambda x: x["tool"] == "coop_create_queue", "Codex native queue creation")
    assert creation_call["arguments"]["queue"] == scenario["queue"]
    assert creation_call["result"]["queue"] == scenario["queue"]
    assert creation_call["result"]["created_seq"] == queue[0]["seq"]
    enqueue_call = unique(codex_calls, lambda x: x["tool"] == "coop_enqueue", "Codex native enqueue")
    assert enqueue_call["arguments"]["queue"] == scenario["queue"]
    assert enqueue_call["arguments"]["payload"] == enqueued["payload"]
    assert enqueue_call["arguments"]["max_attempts"] == enqueued["max_attempts"]
    assert enqueue_call["result"]["task_id"] == task_id
    assert enqueue_call["result"]["seq"] == enqueued["seq"]
    claim_call = unique(claude_calls, lambda x: x["tool"] == "coop_claim_task", "Claude native claim")
    assert claim_call["arguments"]["queue"] == scenario["queue"]
    assert claim_call["arguments"]["lease_ttl_s"] == claimed["lease_ttl_s"]
    assert claim_call["result"]["task"]["task_id"] == task_id
    assert claim_call["result"]["task"]["payload"] == enqueued["payload"]
    assert claim_call["result"]["task"]["lease"]["agent_id"] == claude_id
    assert claim_call["result"]["lease_id"] == claimed["lease_id"]
    assert claim_call["result"]["fence"] == claimed["seq"]
    completion_call = unique(claude_calls, lambda x: x["tool"] == "coop_complete_task", "Claude native completion")
    assert completion_call["arguments"]["lease_id"] == claimed["lease_id"]
    assert completion_call["arguments"]["fence"] == claimed["seq"]
    assert completion_call["arguments"]["outcome"] == completed["outcome"]
    assert completion_call["result"]["task_id"] == task_id
    assert completion_call["result"]["status"] == "completed"
    assert completion_call["result"]["seq"] == completed["seq"]

    registry = json.loads((args.root / "registry.json").read_text())["agents"]
    by_id = {item["agent_id"]: (name, item) for name, item in registry.items()}
    assert by_id[codex_id][1]["kind"] == "codex"
    assert by_id[claude_id][1]["kind"] == "claude"
    codex_name, claude_name = by_id[codex_id][0], by_id[claude_id][0]
    codex_inbox = check_chain(args.root / "agents" / codex_id / "inbox.jsonl")
    claude_inbox = check_chain(args.root / "agents" / claude_id / "inbox.jsonl")
    assert len(codex_inbox) == 1 and len(claude_inbox) == 2
    request = unique(claude_inbox, lambda x: x["kind"] == "acceptance-request", "request delivery")
    reply = unique(codex_inbox, lambda x: x["kind"] == "acceptance-result", "reply delivery")
    receipt = unique(claude_inbox, lambda x: x["kind"] == "acceptance-receipt", "receipt delivery")
    assert request["from"] == receipt["from"] == codex_name
    assert request["to"] == receipt["to"] == claude_name
    assert reply["from"] == claude_name and reply["to"] == codex_name
    for record in [request, reply, receipt]:
        body = json.loads(record["body"]["text"])
        assert body["nonce"] == nonce and body["task_id"] == task_id
    reply_body = json.loads(reply["body"]["text"])
    receipt_body = json.loads(receipt["body"]["text"])
    assert reply_body["answer"] == scenario["expected_sum"] and reply_body["completed"] is True
    assert receipt_body["received"] is True and receipt_body["verified_answer"] == scenario["expected_sum"]

    for record, actor_calls in [(request, codex_calls), (reply, claude_calls), (receipt, codex_calls)]:
        send = unique(actor_calls, lambda x: x["tool"] == "coop_send_message" and x["result"].get("message_id") == record["id"], "native send receipt")
        assert send["result"]["seq"] == record["seq"]
        assert send["arguments"]["text"] == record["body"]["text"]

    for record, actor_calls in [(request, claude_calls), (reply, codex_calls), (receipt, claude_calls)]:
        assert any(x["tool"] == "coop_wait_message" and any(m["id"] == record["id"] for m in x["result"]["messages"]) for x in actor_calls)
        assert any(x["tool"] == "coop_ack" and x["result"]["acked_cursor"] == str(record["seq"]) for x in actor_calls)

    claude_events = rows(evidence / "claude-events.jsonl")
    claude_results = [x for x in claude_events if x.get("type") == "result"]
    assert len(claude_results) >= 2 and not claude_results[-1]["is_error"]
    assert all(not x.get("permission_denials") for x in claude_results)
    assert "PASS" in claude_results[-1]["result"]
    assert json.loads((evidence / "codex-exchange-exit.json").read_text())["exit_code"] == 0
    assert "PASS" in (evidence / "codex-exchange-final.txt").read_text()

    modern_events = rows(evidence / "attempts" / "claude-modern-events.jsonl")
    modern_init = unique(modern_events, lambda x: x.get("type") == "system" and x.get("subtype") == "init", "modern Claude initialization")
    legacy_inits = [x for x in claude_events if x.get("type") == "system" and x.get("subtype") == "init"]
    assert legacy_inits
    legacy_init = legacy_inits[0]
    assert all(x["session_id"] == legacy_init["session_id"] for x in legacy_inits)
    assert not any("cooperators" in tool for tool in modern_init["tools"])
    assert len([tool for tool in legacy_init["tools"] if "cooperators" in tool]) == 23

    result = {
        "status": "pass_with_legacy_workaround",
        "default_modern_discovery": "fail",
        "legacy_workaround": "MCP_PROTOCOL_NEGOTIATION=legacy",
        "codex_agent_id": codex_id,
        "claude_agent_id": claude_id,
        "task_id": task_id,
        "queue_event_kinds": [x["kind"] for x in queue],
        "lease_id": claimed["lease_id"],
        "fence": claimed["seq"],
        "message_ids": {"request": request["id"], "reply": reply["id"], "receipt": receipt["id"]},
        "codex_ack_cursor": "1",
        "claude_ack_cursor": "2",
        "codex_completed_native_calls": len(codex_calls),
        "claude_completed_native_calls": len(claude_calls),
        "recovered_codex_argument_errors": codex_rejected,
        "claude_cli_model": legacy_init.get("model"),
        "durable_hash_chains_verified": True,
        "all_cross_checks_passed": True,
    }
    (evidence / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
