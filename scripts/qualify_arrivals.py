#!/usr/bin/env python3
"""Qualify arrivals on disposable roots; never select an existing user bus.

Provider mode uses the installed model configuration and the production host
launcher. Only scoped qualification MCP calls and one exact marker command may
be approved. Performance mode uses real UDS and contains no model requests.
"""

import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import shlex
import shutil
import subprocess
import tempfile
import threading
import time

from benchmark_transport import Uds


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def quantiles(values):
    ordered = sorted(values)
    return {"count": len(ordered), **{
        f"p{p}": ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)]
        for p in (50, 95, 99)
    }}


class Run:
    def __init__(self, args):
        self.args = args
        self.binary = args.binary.resolve()
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.temp = Path(tempfile.mkdtemp(prefix="caq-", dir="/tmp"))
        self.root = self.temp / "bus"
        self.workspace = self.temp / "workspace"
        self.workspace.mkdir()
        self.config = self.temp / "state" / "resume.json"
        self.marker = self.temp / "approval-marker"
        self.env = os.environ.copy()
        for key in ("COOPERATORS_HOME", "COOP_SOCKET", "COOP_TOKEN", "COOP_HTTP_ADDR"):
            self.env.pop(key, None)
        self.children = []
        self.clients = []
        self.secrets = set()
        self.events = []
        self.answered_approvals = set()
        self.summary = {"schema": "cooperators.arrival-qualification/v1", "passed": False,
                        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "binary_sha256": sha(self.binary), "script_sha256": sha(__file__),
                        "source_sha256": {str(p): sha(p) for p in sorted(Path("src").rglob("*.rs"))},
                        "mode": args.mode, "provider": args.provider}
        self.save("plan.json", {"samples": args.samples, "warmups": 10,
                               "source_to_feed_target_p95_ms": 100,
                               "provider_timeout_s": 180,
                               "provider_gates": ["idle", "busy", "approval", "resolution", "exact-session restart"],
                               "percentiles": "nearest rank; warmups excluded",
                               "limitations": ["One host, serial workload and one window per condition",
                                               "Performance mode does not measure provider admission, model execution, bridge memory or representative large-history startup"]})

    def save(self, name, value):
        text = json.dumps(value, indent=2)
        for secret in self.secrets:
            text = text.replace(secret, "<REDACTED>")
        (self.output / name).write_text(text + "\n")

    def spawn(self, command, **kwargs):
        child = subprocess.Popen(command, env=self.env, **kwargs)
        self.children.append(child)
        return child

    def daemon(self, enabled=True):
        command = [str(self.binary), "--root", str(self.root), "daemon", "run", "--http-addr", "off"]
        if enabled:
            command.append("--enable-arrivals")
        child = self.spawn(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise RuntimeError("qualification daemon exited during startup")
            try:
                client = self.client()
                assert client.call("daemon.status")["pid"] == child.pid
                return child
            except (OSError, RuntimeError):
                time.sleep(.02)
        raise RuntimeError("daemon readiness timed out")

    def client(self, token=None):
        client = Uds(self.root / "run/daemon.sock", token)
        self.clients.append(client)
        return client

    def enroll(self, name):
        agent = self.client().call("register_agent", {"name": name, "kind": "qualification"})
        self.secrets.add(agent["token"])
        return self.client(agent["token"])

    @staticmethod
    def stop(child):
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)

    def host(self, resume=False):
        if resume:
            command = [str(self.binary), "host", "resume", "--config", str(self.config)]
        else:
            command = [str(self.binary), "--root", str(self.root), "host", "run",
                       "--provider", self.args.provider, "--workspace", str(self.workspace),
                       "--state-dir", str(self.temp / "state"), "--config", str(self.config),
                       "--document", "qualification", "--request-timeout-s", "45"]
            if self.args.provider == "codex":
                command += ["--config-override", 'approval_policy="on-request"',
                            "--config-override", 'approvals_reviewer="user"']
        command += ["--shutdown-after", "300"]
        child = self.spawn(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                           stderr=subprocess.DEVNULL, text=True, bufsize=1)
        messages = queue.Queue()

        def read():
            for line in child.stdout:
                try:
                    messages.put(json.loads(line))
                except json.JSONDecodeError:
                    messages.put({"type": "invalid_operator_frame"})
            messages.put({"type": "operator_eof"})

        threading.Thread(target=read, daemon=True).start()
        self.host_child, self.messages = child, messages
        self.last_status = None
        self.answered_approvals.clear()
        return self.until(lambda: self.last_status is not None, timeout=75)

    def poll(self):
        try:
            event = self.messages.get(timeout=.1)
        except queue.Empty:
            return
        event["observed_monotonic_s"] = time.monotonic()
        self.events.append(event)
        kind = event.get("type")
        if kind in ("host_error", "receipt_error", "invalid_operator_frame", "operator_eof"):
            raise RuntimeError(f"managed host failed: {event}")
        if kind in ("host_ready", "status"):
            self.last_status = event["status"]
        if self.config.exists():
            self.secrets.add(json.loads(self.config.read_text())["token"])
        self.save("events.json", self.events)
        host_event = event.get("event", {})
        if host_event.get("type") == "approval":
            approval = host_event["approval"]
            key = json.dumps(approval["request_id"])
            if key in self.answered_approvals:
                return
            serialized = json.dumps(approval)
            params = approval.get("params", {})
            tool = params.get("tool_name", params.get("toolName", ""))
            safe_tools = ("coop_confirm_activation", "coop_read_doc", "coop_resolve_arrivals", "coop_instance_metadata")
            allowed_mcp = tool in {"mcp__cooperators__" + name for name in safe_tools}
            elicitation = (approval.get("method") == "mcpServer/elicitation/request"
                           and params.get("serverName") == "cooperators"
                           and params.get("_meta", {}).get("codex_approval_kind") == "mcp_tool_call"
                           and params.get("message") in {
                               f'Allow the cooperators MCP server to run tool "{name}"?' for name in safe_tools})
            allowed_mcp = allowed_mcp or elicitation
            marker_command = "touch " + str(self.marker)
            command = approval.get("params", {}).get("command")
            if isinstance(command, list):
                command = command[-1]
            claude_command = approval.get("params", {}).get("input", {}).get("command")
            if isinstance(command, str):
                words = shlex.split(command)
                if len(words) == 3 and Path(words[0]).name in ("zsh", "bash", "sh") and words[1] in ("-lc", "-c"):
                    command = words[2]
            allowed = allowed_mcp or command == marker_command or claude_command == marker_command
            if not allowed:
                raise RuntimeError("unexpected approval requires manual review: " + serialized)
            time.sleep(.25)  # Observe a genuinely pending per-request approval.
            response = {"decision": "accept"} if self.args.provider == "codex" else {"behavior": "allow"}
            if elicitation:
                response = {"action": "accept", "content": {}, "_meta": None}
            self.host_child.stdin.write(json.dumps({"type": "approval", "request_id": approval["request_id"],
                                                   "response": response}) + "\n")
            self.host_child.stdin.flush()
            self.answered_approvals.add(key)
            self.summary["approval_requests_answered"] = self.summary.get("approval_requests_answered", 0) + 1
            print(json.dumps({"stage": "approval_answered", "request_id": approval["request_id"]}), flush=True)

    def until(self, condition, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.poll()
            if condition():
                return self.last_status
        raise RuntimeError("managed-provider gate timed out; last status=" + json.dumps(self.last_status))

    def publish(self, client, version, marker=False):
        text = ("Controlled qualification document. Read this pinned version, confirm this wake, "
                "then resolve every supplied document notice as document_read with continuation=false. "
                "Do not change project files or perform unrelated work.")
        if marker:
            text += (" Before resolution, run exactly this shell command with sandbox_permissions=require_escalated "
                     "so its per-request approval reaches the operator; this is an authorized approval-routing test: touch " + str(self.marker))
        args = {"name": "qualification", "text": text}
        if version > 1:
            args["base_version"] = version - 1
        return client.call("publish_doc" if version == 1 else "update_doc", args)

    def resolved(self, seq):
        status = self.last_status or {}
        return (status.get("received_seq", 0) >= seq and status.get("pending_notices") == 0
                and status.get("active_wake_id") is None and status.get("readiness") == "verified")

    def provider(self):
        version = subprocess.run([self.args.provider, "--version"], capture_output=True, text=True, timeout=15)
        self.summary["provider_version"] = version.stdout.strip()
        if self.args.provider == "claude":
            auth = subprocess.run(["claude", "auth", "status"], capture_output=True, text=True, timeout=15)
            status = json.loads(auth.stdout)
            if not status.get("loggedIn"):
                self.summary["unavailable"] = "Installed Claude has no authentication/provider configuration"
                return
        daemon = self.daemon()
        author = self.enroll("qualification-author")
        self.host()
        binding = self.last_status["binding"]
        self.summary["binding"] = binding
        self.publish(author, 1, marker=True)
        self.until(lambda: (self.last_status or {}).get("lifecycle") in ("busy", "waiting_for_permission"))
        self.publish(author, 2)
        self.until(lambda: (self.last_status or {}).get("received_seq", 0) >= 2)
        self.summary["arrival_received_while_active"] = not self.resolved(2)
        self.until(lambda: self.resolved(2))
        assert self.marker.is_file(), "actual approved marker command did not execute"
        assert self.summary.get("approval_requests_answered", 0) > 0
        self.summary["idle_busy_approval_resolution_passed"] = True
        print(json.dumps({"stage": "idle_busy_approval_resolution_passed"}), flush=True)
        self.host_child.stdin.write('{"type":"shutdown"}\n')
        self.host_child.stdin.flush()
        self.host_child.wait(timeout=15)
        self.stop(daemon)
        for client in self.clients:
            client.close()
        self.clients.clear()
        daemon = self.daemon()
        author = self.client(author.token)
        self.host(resume=True)
        assert self.last_status["binding"] == binding, "resume changed exact binding"
        self.publish(author, 3)
        self.until(lambda: self.resolved(3))
        self.summary["broker_bridge_restart_exact_binding_passed"] = True
        self.summary["final_status"] = self.last_status
        self.summary["passed"] = True

    def performance(self):
        self.summary["scope"] = "Serial real-UDS document publication; conservative RPC-start-to-feed-observed upper bound includes source sync and client overhead"
        results = []
        for enabled in (False, True):
            self.root = self.temp / ("enabled" if enabled else "disabled")
            child = self.daemon(enabled)
            author = self.enroll("perf-author")
            reader = self.enroll("perf-reader") if enabled else None
            if reader:
                reader.call("subscribe_arrivals", {"client_key": "perf", "resource": {"type": "document", "name": "qualification"},
                                                  "event_kinds": ["doc.published", "doc.updated"], "catch_up": False})
            rows = []
            for index in range(self.args.samples + 10):
                start = time.monotonic_ns()
                self.publish(author, index + 1)
                committed = time.monotonic_ns()
                row = {"source_rpc_ms": (committed - start) / 1e6}
                if reader:
                    page = reader.call("wait_arrivals", {"timeout_s": 5, "limit": 64})
                    observed = time.monotonic_ns()
                    assert len(page["notices"]) == 1
                    notice = page["notices"][0]
                    assert notice["seq"] == index + 1
                    row.update({"source_start_to_feed_ms": (observed - start) / 1e6,
                                "source_response_to_feed_ms": (observed - committed) / 1e6})
                    reader.call("ack_arrivals", {"cursor": page["next_cursor"]})
                if index >= 10:
                    rows.append(row)
            results.append({"enabled": enabled, "samples": rows,
                            "quantiles_ms": {k: quantiles([r[k] for r in rows]) for k in rows[0]}})
            self.stop(child)
        self.summary["conditions"] = results
        self.summary["source_feed_p95_under_100ms"] = results[1]["quantiles_ms"]["source_start_to_feed_ms"]["p95"] < 100
        self.summary["passed"] = self.summary["source_feed_p95_under_100ms"]

    def run(self):
        try:
            self.provider() if self.args.mode == "provider" else self.performance()
        except Exception as error:
            self.summary["error"] = str(error)
        finally:
            if self.config.exists():
                self.secrets.add(json.loads(self.config.read_text())["token"])
            self.save("events.json", self.events)
            for child in reversed(self.children):
                self.stop(child)
            for client in self.clients:
                client.close()
            self.summary["owned_children_reaped"] = all(p.poll() is not None for p in self.children)
            self.summary["source_unchanged"] = all(sha(p) == digest for p, digest in self.summary["source_sha256"].items())
            self.summary["binary_unchanged"] = sha(self.binary) == self.summary["binary_sha256"]
            self.summary["script_unchanged"] = sha(__file__) == self.summary["script_sha256"]
            if not self.summary["source_unchanged"] or not self.summary["binary_unchanged"] or not self.summary["script_unchanged"]:
                self.summary["passed"] = False
            self.summary["ended_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            self.save("summary.json", self.summary)
            shutil.rmtree(self.temp)
        print(json.dumps({k: v for k, v in self.summary.items() if k not in ("source_sha256", "conditions")}), flush=True)
        return 0 if self.summary["passed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("provider", "performance"))
    parser.add_argument("--binary", type=Path, default=Path("target/release/coop"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provider", choices=("codex", "claude"), default="codex")
    parser.add_argument("--samples", type=int, default=200)
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be positive")
    return Run(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
