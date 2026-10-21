#!/usr/bin/env python3
"""Cross-check retained Codex operator events, native MCP results and binding."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    summary = json.loads((args.directory / "summary.json").read_text())
    events = json.loads((args.directory / "events.json").read_text())
    assert summary["passed"] and summary["owned_children_reaped"]
    assert summary["source_unchanged"] and summary["binary_unchanged"]
    binding = summary["binding"]
    starts = [e["status"]["binding"] for e in events if e.get("type") == "host_ready"]
    assert len(starts) == 2 and all(b == binding for b in starts)
    wakes, tools, shell = {}, [], []
    for event in events:
        value = event.get("event", {}).get("value", {})
        params = value.get("params", {})
        if value.get("method") != "item/completed":
            continue
        assert params["threadId"] == binding["exact_host_session_id"]
        item = params["item"]
        if item["type"] == "userMessage":
            text = "".join(v.get("text", "") for v in item["content"])
            wake = json.loads(text.split("\n", 1)[1])
            assert wake["binding_id"] == binding["binding_id"]
            assert wake["generation"] == binding["generation"]
            assert wake["agent_id"] == binding["agent_id"]
            assert wake["bus_id"] == binding["bus_id"]
            wakes[wake["wake_id"]] = wake
        elif item["type"] == "mcpToolCall" and item["server"] == "cooperators":
            assert item["status"] == "completed" and item["error"] is None
            tools.append(item)
        elif item["type"] == "commandExecution":
            assert item["exitCode"] == 0 and "approval-marker" in item["command"]
            shell.append(item["id"])
    assert len(wakes) == 3 and len(shell) == 1
    confirmations, read_versions, resolutions = set(), set(), {}
    resolved, duplicates = set(), 0
    for item in tools:
        name, request = item["tool"], item["arguments"]
        reply = item["result"]["structuredContent"]
        if name == "coop_confirm_activation":
            wake = wakes[request["wake_id"]]
            assert request["binding_id"] == binding["binding_id"]
            assert request["generation"] == binding["generation"]
            assert request["nonce"] == wake["observation_nonce"]
            assert reply["wake_id"] == wake["wake_id"] and reply["readiness"] == "verified"
            confirmations.add(wake["wake_id"])
        elif name == "coop_read_doc":
            assert request["name"] == reply["name"] == "qualification"
            assert request["version"] == reply["version"]
            read_versions.add(reply["version"])
        elif name == "coop_resolve_arrivals":
            wake = wakes[request["wake_id"]]
            assert wake["wake_id"] in confirmations
            expected = {n["delivery_id"] for n in wake["notices"]}
            assert all(n["resource"]["version"] in read_versions for n in wake["notices"])
            assert request["generation"] == reply["generation"] == binding["generation"]
            assert {d["delivery_id"] for d in request["decisions"]} == expected
            assert all(d["disposition"] == "document_read" for d in request["decisions"])
            assert set(reply["resolved_delivery_ids"]) == expected
            assert reply["wake_id"] == wake["wake_id"]
            if wake["wake_id"] in resolutions:
                assert resolutions[wake["wake_id"]] == reply
                duplicates += 1
            resolutions[wake["wake_id"]] = reply
            resolved.update(expected)
    expected = {n["delivery_id"] for w in wakes.values() for n in w["notices"]}
    assert len(expected) == 3 and resolved == expected
    assert confirmations == set(wakes) and read_versions == {1, 2, 3}
    assert summary["final_status"]["pending_notices"] == 0
    assert summary["final_status"]["active_wake_id"] is None
    report = {"passed": True, "binding_preserved_across_restart": True, "unique_wakes": len(wakes),
              "verified_document_versions": sorted(read_versions), "resolved_deliveries": len(resolved),
              "idempotent_duplicate_resolutions": duplicates, "approved_shell_commands_completed": len(shell),
              "native_mcp_tool_counts": dict(Counter(i["tool"] for i in tools)),
              "evidence_sha256": {name: hashlib.sha256((args.directory / name).read_bytes()).hexdigest()
                                  for name in ("events.json", "summary.json")},
              "audit_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "boundary": "Protocol/result and controller-state audit; disposable live broker files were removed after the run"}
    target = args.directory / "audit.json"
    if target.exists():
        raise RuntimeError("refusing to overwrite existing audit")
    target.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
