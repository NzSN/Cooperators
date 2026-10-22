#!/usr/bin/env python3
"""Observe pre-network gaps on disposable roots; never retain token values."""
import argparse
import hashlib
import http.client
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time

p = argparse.ArgumentParser()
p.add_argument("--binary", type=Path, required=True)
p.add_argument("--output", type=Path, required=True)
args = p.parse_args()

with tempfile.TemporaryDirectory(prefix="coop-network-baseline-") as directory:
    root = Path(directory) / "root"
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    daemon = None

    def rpc(method, params, discard=False):
        with socket.socket(socket.AF_UNIX) as stream:
            stream.settimeout(10)
            stream.connect(str(root / "run/daemon.sock"))
            stream.sendall((json.dumps({"jsonrpc": "2.0", "id": "reused-correlation", "method": method, "params": params}) + "\n").encode())
            if discard:
                return
            response = json.loads(stream.makefile("rb").readline())
            if "error" in response:
                raise RuntimeError(f"baseline {method} rejected with code {response['error']['code']}")
            return response["result"]

    def start():
        child = subprocess.Popen([str(args.binary.resolve()), "--root", str(root), "daemon", "run", "--http-addr", f"127.0.0.1:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(300):
            try:
                rpc("ping", {})
                return child
            except (OSError, ValueError):
                if child.poll() is not None:
                    raise RuntimeError("baseline daemon failed startup")
                time.sleep(0.01)
        child.kill(); child.wait()
        raise RuntimeError("baseline startup timeout")

    def mcp(body, token, host="localhost"):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        connection.request("POST", "/mcp", json.dumps(body), {"Host": host, "Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-03-26"})
        response = connection.getresponse()
        data = response.read().decode()
        status = response.status
        connection.close()
        try:
            value = json.loads(data)
        except ValueError:
            values = [json.loads(line[5:].strip()) for line in data.splitlines() if line.startswith("data:")]
            value = values[-1] if values else {}
        return status, value

    try:
        daemon = start()
        alice = rpc("register_agent", {"name": "baseline-alice"})
        bob = rpc("register_agent", {"name": "baseline-bob"})
        rpc("create_queue", {"token": alice["token"], "queue": "baseline"})
        task1 = rpc("enqueue", {"token": alice["token"], "queue": "baseline", "payload": {"same": True}})
        task2 = rpc("enqueue", {"token": alice["token"], "queue": "baseline", "payload": {"same": True}})
        rpc("claim_task", {"token": alice["token"], "queue": "baseline", "lease_ttl_s": 60}, discard=True)
        for _ in range(200):
            events = []
            for line in (root / "queues/baseline.jsonl").read_text().splitlines():
                try: events.append(json.loads(line))
                except ValueError: pass
            if any(e.get("kind") == "task_claimed" for e in events): break
            time.sleep(0.01)
        else: raise RuntimeError("discarded claim did not reach source")
        first_claim = next(e for e in events if e.get("kind") == "task_claimed")
        second_claim = rpc("claim_task", {"token": alice["token"], "queue": "baseline", "lease_ttl_s": 60})
        rpc("send_message", {"token": alice["token"], "to": bob["agent_id"], "kind":"text", "text": "baseline progress"})
        page = rpc("wait_message", {"token": bob["token"], "timeout_s": 0})
        rpc("ack", {"token": bob["token"], "cursor": page["next_cursor"]})
        before = rpc("wait_message", {"token": bob["token"], "timeout_s": 0})
        body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "coop_instance_metadata", "arguments": {"token": bob["token"]}}}
        http_status, metadata = mcp(body, alice["token"])
        host_status, _ = mcp(body, alice["token"], "lan.example")
        structured = metadata.get("result", {}).get("structuredContent", {})
        daemon.kill(); daemon.wait(); daemon = start()
        after = rpc("wait_message", {"token": bob["token"], "timeout_s": 0})
        observations = {
            "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
            "same_rpc_correlation_does_not_dedupe_enqueue": {"task_ids": [task1["task_id"], task2["task_id"]], "distinct": task1["task_id"] != task2["task_id"]},
            "lost_claim_reply_then_fresh_claim": {"source_first_task": first_claim["task_id"], "second_receipt_task": second_claim["task"]["task_id"], "different_task": first_claim["task_id"] != second_claim["task"]["task_id"]},
            "ordinary_ack_restart": {"messages_before_restart": len(before["messages"]), "messages_after_restart": len(after["messages"]), "acked_cursor_after_restart": after["acked_cursor"]},
            "http_body_credential": {"http_status": http_status, "result_agent_id": structured.get("agent_id"), "header_agent_id": alice["agent_id"], "body_agent_id": bob["agent_id"], "body_selected_other_principal": structured.get("agent_id") == bob["agent_id"]},
            "lan_host_status": host_status,
            "scope": "Pre-network source observations on one disposable local root; no provider or WAN qualification."
        }
        assert observations["same_rpc_correlation_does_not_dedupe_enqueue"]["distinct"]
        assert observations["lost_claim_reply_then_fresh_claim"]["different_task"]
        assert len(before["messages"]) == 0 and len(after["messages"]) == 1
        assert host_status == 403
        args.output.write_text(json.dumps(observations, indent=2) + "\n")
        print(json.dumps({"recorded": str(args.output), "body_credential_observed": observations["http_body_credential"]["body_selected_other_principal"]}))
    finally:
        if daemon is not None:
            daemon.kill(); daemon.wait()
