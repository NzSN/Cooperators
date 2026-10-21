#!/usr/bin/env python3
"""Measure a release coop daemon over actual UDS and streamable HTTP MCP.

Standard library only. All bus roots and credentials are private to this run.
Measurements are sequential, retain raw samples, and include correctness
checks. No production configuration, source, or durability option is changed.
"""

import argparse
import concurrent.futures
import datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import re
import secrets
import select
import shutil
import socket
import stat
import subprocess
import tempfile
import threading
import time


PROTOCOL = "2025-03-26"
PAYLOAD = "x" * 128


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_output(command, cwd):
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def percentiles(rows, key):
    values = sorted(row[key] for row in rows)
    return {"count": len(values), "min": values[0], "max": values[-1],
            "mean": sum(values) / len(values),
            **{f"p{p}": values[max(0, math.ceil(len(values) * p / 100) - 1)]
               for p in (50, 95, 99)}}


class Uds:
    """One persistent JSON-RPC connection; calls are serialized by its owner."""

    def __init__(self, path, token=None):
        self.connection = socket.socket(socket.AF_UNIX)
        self.connection.settimeout(30)
        self.connection.connect(str(path))
        self.reader = self.connection.makefile("rb")
        self.token = token
        self.sequence = 0

    def begin(self, method, arguments=None):
        self.sequence += 1
        params = dict(arguments or {})
        if self.token:
            params["token"] = self.token
        self.connection.sendall(json.dumps({"jsonrpc": "2.0", "id": self.sequence,
                                           "method": method, "params": params},
                                          separators=(",", ":")).encode() + b"\n")

    def call(self, method, arguments=None):
        self.begin(method, arguments)
        raw = self.reader.readline()
        if not raw:
            raise RuntimeError("UDS connection closed")
        response = json.loads(raw)
        if response.get("id") != self.sequence or "error" in response:
            raise RuntimeError(f"UDS {method}: {response}")
        return response["result"]

    def close(self):
        self.reader.close()
        self.connection.close()


class HttpMcp:
    """One persistent HTTP/1.1 connection and initialized MCP session."""

    def __init__(self, port, token):
        self.connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        self.token, self.session_id, self.sequence = token, None, 0
        value = self.rpc("initialize", {"protocolVersion": PROTOCOL, "capabilities": {},
                         "clientInfo": {"name": "cooperators-transport-benchmark", "version": "1"}})
        assert value["protocolVersion"] == PROTOCOL and value["serverInfo"]["name"] == "cooperators"
        self.post({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def post(self, body):
        headers = {"Accept": "application/json, text/event-stream",
                   "Content-Type": "application/json", "MCP-Protocol-Version": PROTOCOL,
                   "Authorization": f"Bearer {self.token}"}
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        self.connection.request("POST", "/mcp", json.dumps(body, separators=(",", ":")), headers)
        response = self.connection.getresponse()
        raw = response.read().decode()
        if not 200 <= response.status < 300:
            raise RuntimeError(f"HTTP /mcp status {response.status}: {raw}")
        session_id = response.getheader("Mcp-Session-Id")
        if session_id:
            self.session_id = session_id
        if not raw.strip():
            return []
        content_type = response.getheader("Content-Type", "")
        if "application/json" in content_type:
            return [json.loads(raw)]
        if "text/event-stream" in content_type:
            return [json.loads(line[5:].strip()) for line in raw.splitlines() if line.startswith("data:")]
        raise RuntimeError(f"Unexpected HTTP content type {content_type}")

    def rpc(self, method, params):
        self.sequence += 1
        values = self.post({"jsonrpc": "2.0", "id": self.sequence, "method": method, "params": params})
        response = next((value for value in values if value.get("id") == self.sequence), None)
        if response is None or "error" in response:
            raise RuntimeError(f"MCP {method}: {response}")
        return response["result"]

    def call(self, method, arguments=None):
        tool = "coop_get_status" if method == "daemon.status" else f"coop_{method}"
        result = self.rpc("tools/call", {"name": tool, "arguments": arguments or {}})
        if result.get("isError"):
            raise RuntimeError(f"MCP tool {tool}: {result}")
        if "structuredContent" in result:
            return result["structuredContent"]
        return json.loads(next(row["text"] for row in result["content"] if row["type"] == "text"))

    def close(self):
        self.connection.close()


class Daemon:
    def __init__(self, run, label):
        self.run, self.label = run, label
        self.output = run.output / label
        self.output.mkdir()
        self.root = Path(tempfile.mkdtemp(prefix="ctb-", dir="/tmp"))
        self.child = None
        self.clients = []
        self.info = {"label": label, "root": str(self.root), "correctness_passed": False}
        self.agents = []

    def __enter__(self):
        environment = os.environ.copy()
        for key in ("COOPERATORS_HOME", "COOP_SOCKET", "COOP_HTTP_ADDR", "COOP_TOKEN"):
            environment.pop(key, None)
        command = [str(self.run.binary), "--root", str(self.root), "daemon", "run", "--http-addr", "127.0.0.1:0"]
        with (self.output / "daemon.stdout.log").open("wb") as stdout, (self.output / "daemon.stderr.log").open("wb") as stderr:
            self.child = subprocess.Popen(command, env=environment, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
        self.info.update(command=command, pid=self.child.pid,
                         start_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.child.poll() is not None:
                raise RuntimeError("daemon exited before readiness")
            log = (self.output / "daemon.stderr.log").read_text()
            match = re.search(r"MCP streamable HTTP on http://127\.0\.0\.1:(\d+)/mcp", log)
            if match and (self.root / "run/daemon.sock").exists():
                self.port = int(match[1])
                break
            time.sleep(0.025)
        else:
            raise RuntimeError("daemon readiness deadline expired")
        self.info["running_executable_sha256"] = sha(Path(f"/proc/{self.child.pid}/exe"))
        assert self.info["running_executable_sha256"] == self.run.identity["binary_sha256"]
        control = self.uds()
        assert control.call("daemon.status")["pid"] == self.child.pid
        return self

    def uds(self, token=None):
        client = Uds(self.root / "run/daemon.sock", token)
        self.clients.append(client)
        return client

    def enroll(self, number, transport):
        client = self.uds()
        launch_key = secrets.token_hex(16)
        self.run.credentials.add(launch_key)
        enrolled = client.call("enroll_agent", {"launch_key": launch_key, "kind": "benchmark",
                              "display_name": f"{self.label}-{number}", "capabilities": []})
        self.run.credentials.add(enrolled["token"])
        agent = {key: enrolled[key] for key in ("agent_id", "name", "display_name")}
        self.agents.append(agent)
        client.token = enrolled["token"]
        if transport == "http":
            client.close()
            self.clients.remove(client)
            client = HttpMcp(self.port, enrolled["token"])
            self.clients.append(client)
        return client, agent

    def persist_samples(self, rows):
        with (self.output / "samples.jsonl").open("w") as target:
            for row in rows:
                target.write(json.dumps(row, sort_keys=True) + "\n")

    def validate_disk(self, rows):
        # Include warmups; every successful send must correspond exactly to one
        # regular persisted inbox record. Ack correctness is checked per call.
        expected = {row["message_id"]: row for row in rows}
        assert len(expected) == len(rows), "duplicate successful receipt ids"
        actual = {}
        for path in sorted(self.root.glob("agents/*/inbox.jsonl")):
            records = [json.loads(line) for line in path.read_text().splitlines()]
            assert [record["seq"] for record in records] == list(range(1, len(records) + 1))
            for record in records:
                assert record["id"] not in actual
                actual[record["id"]] = record
        assert actual.keys() == expected.keys(), "receipt ids differ from durable inbox ids"
        for message_id, row in expected.items():
            record = actual[message_id]
            assert record["seq"] == row["seq"] and record["body"]["text"] == PAYLOAD
            assert record["from"] == row["agent_name"] and record["to"] == row["agent_name"]
        self.info.update(agents=self.agents, durable_records=len(actual), receipt_count=len(rows),
                         unique_receipts=len(expected), contiguous_sequences=True,
                         receipt_payload_and_identity_match=True, correctness_passed=True)

    def __exit__(self, error_type, error, traceback):
        for client in self.clients:
            client.close()
        if self.child:
            self.child.terminate()
            try:
                self.child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait(timeout=5)
            self.info["daemon_exit_after_termination"] = self.child.returncode
        if error:
            self.info["error"] = self.run.redact(str(error))
        for path in self.root.rglob("*"):
            if stat.S_ISREG(path.lstat().st_mode):
                target = self.output / "raw-root" / path.relative_to(self.root)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)
        write_json(self.output / "daemon.json", self.info)
        shutil.rmtree(self.root)


def cycle(client, agent, iteration, warmup):
    started = time.perf_counter_ns()
    receipt = client.call("send_message", {"to": agent["agent_id"], "kind": "benchmark",
                                          "body": {"mime": "text/plain", "text": PAYLOAD}})
    sent = time.perf_counter_ns()
    batch = client.call("wait_message", {"timeout_s": 0})
    delivered = time.perf_counter_ns()
    messages = batch["messages"]
    assert not receipt["duplicate"] and not batch["timed_out"] and len(messages) == 1
    message = messages[0]
    assert message["id"] == receipt["message_id"] and message["seq"] == receipt["seq"]
    assert message["from"] == agent["name"] and message["to"] == agent["name"]
    assert message["body"]["text"] == PAYLOAD and message["body"]["mime"] == "text/plain"
    assert batch["next_cursor"] == str(receipt["seq"])
    ack = client.call("ack", {"cursor": batch["next_cursor"]})
    ended = time.perf_counter_ns()
    assert ack["acked_cursor"] == batch["next_cursor"]
    return {"agent_id": agent["agent_id"], "agent_name": agent["name"], "iteration": iteration,
            "warmup": warmup, "message_id": receipt["message_id"], "seq": receipt["seq"],
            "started_monotonic_ns": started, "send_ack_ms": (sent - started) / 1e6,
            "wait_after_send_ms": (delivered - sent) / 1e6,
            "ack_ms": (ended - delivered) / 1e6, "cycle_ms": (ended - started) / 1e6,
            "delivery_verified": True, "ack_verified": True}


class Run:
    def __init__(self, arguments):
        self.args = arguments
        self.repository = Path(__file__).resolve().parents[1]
        self.binary, self.output = arguments.binary.resolve(), arguments.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.credentials = set()
        self.results = []
        cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                    if line.startswith("model name")), "unknown")
        sources = [self.repository / "Cargo.toml", self.repository / "Cargo.lock", *sorted(self.repository.glob("src/**/*.rs"))]
        self.identity = {"start_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                         "git_head": command_output(["git", "rev-parse", "HEAD"], self.repository),
                         "git_status_before": command_output(["git", "status", "--short"], self.repository),
                         "source_sha256": {str(path.relative_to(self.repository)): sha(path) for path in sources},
                         "binary": str(self.binary), "binary_sha256": sha(self.binary),
                         "binary_build_recipe": "cargo build --offline --locked --release (default features)",
                         "script_sha256": sha(Path(__file__)), "python": platform.python_version(),
                         "platform": platform.platform(), "cpu": cpu, "logical_cpus": os.cpu_count(),
                         "filesystem_tmp": command_output(["stat", "-f", "-c", "%T", "/tmp"], self.repository),
                         "filesystem_mount": command_output(["findmnt", "-T", "/tmp", "-o", "SOURCE,FSTYPE,OPTIONS", "-n"], self.repository)}
        self.plan = {"schema": "cooperators.transport-scale-plan/v1", "payload_text_bytes": len(PAYLOAD),
                     "transports": {"uds": "Persistent AF_UNIX NDJSON JSON-RPC connection per agent",
                                    "http": "Persistent loopback HTTP/1.1 streamable HTTP MCP initialized session per agent"},
                     "latency": {"samples": arguments.samples, "warmups": arguments.warmups,
                                 "send_ack": "Elapsed client call through durable fsync acknowledgment",
                                 "wait_after_send": "Elapsed wait(timeout_s=0) call issued immediately after send acknowledgment; includes client parsing and transport",
                                 "cycle": "Self-send, wait, verify identity/payload/receipt, then acknowledge cursor"},
                     "scale": {"transport": "uds", "baseline_agents": 1, "loaded_agents": arguments.agents,
                               "single_active_cycles": arguments.scale_single_samples,
                               "loaded_cycles_per_agent": arguments.scale_rounds,
                               "warmups_per_active_agent": arguments.scale_warmups,
                               "conditions": ["1 active / 1 enrolled", f"1 active / {arguments.agents} enrolled; other agents have outstanding empty-inbox long polls", f"{arguments.agents} active / {arguments.agents} enrolled"],
                               "diagnostic_relative_thresholds_declared_before_run": {
                                   "send_ack_p50_ratio_max": 1.25, "send_ack_p95_ratio_max": 1.25,
                                   "wait_p50_ratio_max": 1.25, "wait_p95_ratio_max": 1.25,
                                   "aggregate_cycle_throughput_ratio_min": 0.8},
                               "relative_threshold_status": "Diagnostic engineering choice, not a numeric specification requirement; report loaded concurrency and absolute target misses independently"},
                     "append": {"seconds": arguments.append_seconds, "transport": "uds", "concurrency": 1,
                                "maintenance": "Drain and verify all messages every 5000 sends to remain below the default inbox limit; maintenance time included in wall rate"},
                     "specification_targets": {"send_ack_p50_ms_lt": 10, "wait_after_send_p50_ms_lt": 25,
                                               "append_messages_per_second_gte": 5000,
                                               "agents": 100, "agents_without_degradation_numeric_definition": None},
                     "limitations": ["One host/filesystem and one run per condition; no confidence intervals",
                                     "Self-send workload, fixed 128-byte text, closed-loop clients",
                                     "Wait measurements begin after durable send ack and do not measure a preexisting waiter's wakeup",
                                     "100 active clients increase offered concurrency; queueing is part of observed latency",
                                     "Python client scheduling, parsing, GIL, and MCP wrapper overhead are included"]}
        write_json(self.output / "plan.json", self.plan)
        write_json(self.output / "environment.json", self.identity)

    def redact(self, text):
        for credential in self.credentials:
            text = text.replace(credential, "[REDACTED]")
        return text

    def phase(self, label, transport, population, active, rounds, warmups, idle=False):
        print(f"START {label}: {active} active / {population} enrolled, {transport}, {rounds} cycles per active agent", flush=True)
        with Daemon(self, label) as daemon:
            agents = [daemon.enroll(index, transport) for index in range(population)]
            idle_clients = []
            if idle:
                for client, _ in agents[active:]:
                    client.begin("wait_message", {"timeout_s": 600})
                    idle_clients.append(client)
                time.sleep(0.1)
                assert not select.select([client.connection for client in idle_clients], [], [], 0)[0]
            barrier = threading.Barrier(active + 1)
            ready = threading.Barrier(active + 1)

            def worker(index):
                client, agent = agents[index]
                rows = [cycle(client, agent, iteration, True) for iteration in range(warmups)]
                ready.wait(timeout=120)
                barrier.wait(timeout=120)
                rows.extend(cycle(client, agent, iteration, False) for iteration in range(rounds))
                empty = client.call("wait_message", {"timeout_s": 0})
                assert not empty["messages"] and empty["timed_out"]
                assert empty["acked_cursor"] == str(rows[-1]["seq"])
                return rows

            with concurrent.futures.ThreadPoolExecutor(max_workers=active) as pool:
                futures = [pool.submit(worker, index) for index in range(active)]
                ready.wait(timeout=120)
                started = time.perf_counter_ns()
                barrier.wait(timeout=120)
                rows = [row for future in futures for row in future.result(timeout=300)]
                ended = time.perf_counter_ns()
            if idle:
                outstanding = len(idle_clients) - len(select.select([client.connection for client in idle_clients], [], [], 0)[0])
                assert outstanding == population - active
                daemon.info["idle_long_polls_outstanding_before_and_after"] = outstanding
            daemon.persist_samples(rows)
            daemon.validate_disk(rows)
            measured = [row for row in rows if not row["warmup"]]
            elapsed = (ended - started) / 1e9
            summary = {"label": label, "transport": transport, "enrolled_agents": population,
                       "active_agents": active, "idle_agents_with_long_poll": population - active if idle else 0,
                       "warmup_messages": active * warmups, "measured_messages": len(measured),
                       "elapsed_seconds": elapsed, "completed_message_cycles_per_second": len(measured) / elapsed,
                       **{key: percentiles(measured, key) for key in ("send_ack_ms", "wait_after_send_ms", "ack_ms", "cycle_ms")},
                       "send_ack_p50_target_pass": percentiles(measured, "send_ack_ms")["p50"] < 10,
                       "wait_after_send_p50_target_pass": percentiles(measured, "wait_after_send_ms")["p50"] < 25,
                       "correctness_passed": True, "verified_deliveries_and_acks": len(rows)}
            write_json(daemon.output / "summary.json", summary)
            self.results.append(summary)
        print(f"DONE {label}: send p50 {summary['send_ack_ms']['p50']:.3f} ms, wait p50 {summary['wait_after_send_ms']['p50']:.3f} ms, {summary['completed_message_cycles_per_second']:.1f} cycles/s", flush=True)

    def append(self):
        print("START append-uds", flush=True)
        with Daemon(self, "append-uds") as daemon:
            client, agent = daemon.enroll(0, "uds")
            rows = [cycle(client, agent, iteration, True) for iteration in range(self.args.warmups)]
            pending = []
            delivered = 0

            def drain():
                nonlocal delivered
                while pending:
                    batch = client.call("wait_message", {"timeout_s": 0})
                    assert not batch["timed_out"] and batch["messages"]
                    for message in batch["messages"]:
                        row = pending.pop(0)
                        assert message["id"] == row["message_id"] and message["seq"] == row["seq"]
                        assert message["body"]["text"] == PAYLOAD
                        row.update(delivery_verified=True, ack_verified=True)
                        delivered += 1
                    ack = client.call("ack", {"cursor": batch["next_cursor"]})
                    assert ack["acked_cursor"] == batch["next_cursor"]

            started = time.perf_counter_ns()
            deadline = started + self.args.append_seconds * 1e9
            while time.perf_counter_ns() < deadline:
                before = time.perf_counter_ns()
                receipt = client.call("send_message", {"to": agent["agent_id"], "kind": "benchmark",
                                                      "body": {"mime": "text/plain", "text": PAYLOAD}})
                after = time.perf_counter_ns()
                assert not receipt["duplicate"]
                row = {"agent_id": agent["agent_id"], "agent_name": agent["name"],
                       "iteration": len(rows) - self.args.warmups, "warmup": False,
                       "message_id": receipt["message_id"], "seq": receipt["seq"],
                       "started_monotonic_ns": before, "send_ack_ms": (after - before) / 1e6}
                rows.append(row)
                pending.append(row)
                if len(pending) >= 5000:
                    drain()
            ended = time.perf_counter_ns()
            # Final verification is outside the append measurement window.
            drain()
            empty = client.call("wait_message", {"timeout_s": 0})
            assert empty["timed_out"] and not empty["messages"]
            daemon.persist_samples(rows)
            daemon.validate_disk(rows)
            measured = [row for row in rows if not row["warmup"]]
            elapsed = (ended - started) / 1e9
            rate = len(measured) / elapsed
            summary = {"label": "append-uds", "transport": "uds", "active_agents": 1,
                       "warmup_messages": self.args.warmups, "measured_messages": len(measured),
                       "elapsed_seconds": elapsed, "messages_per_second": rate,
                       "send_ack_ms": percentiles(measured, "send_ack_ms"),
                       "append_target_5000_messages_per_second_pass": rate >= 5000,
                       "verified_measured_deliveries_and_acks": delivered, "correctness_passed": True}
            assert delivered == len(measured)
            write_json(daemon.output / "summary.json", summary)
            self.results.append(summary)
        print(f"DONE append-uds: {rate:.1f} messages/s, {len(measured)} verified messages", flush=True)

    def execute(self):
        self.phase("latency-uds", "uds", 1, 1, self.args.samples, self.args.warmups)
        self.phase("latency-http", "http", 1, 1, self.args.samples, self.args.warmups)
        self.append()
        self.phase("scale-1-active", "uds", 1, 1, self.args.scale_single_samples, self.args.scale_warmups)
        self.phase("scale-100-idle", "uds", self.args.agents, 1, self.args.scale_single_samples, self.args.scale_warmups, idle=True)
        self.phase("scale-100-active", "uds", self.args.agents, self.args.agents, self.args.scale_rounds, self.args.scale_warmups)

    def finish(self, error=None):
        comparisons = []
        baseline = next((row for row in self.results if row["label"] == "scale-1-active"), None)
        if baseline:
            for row in self.results:
                if not row["label"].startswith("scale-") or row is baseline:
                    continue
                ratios = {f"{key}_{p}_ratio": row[key][p] / baseline[key][p]
                          for key in ("send_ack_ms", "wait_after_send_ms") for p in ("p50", "p95")}
                ratios["aggregate_cycle_throughput_ratio"] = row["completed_message_cycles_per_second"] / baseline["completed_message_cycles_per_second"]
                comparisons.append({"condition": row["label"], "baseline": baseline["label"], **ratios,
                                    "diagnostic_relative_thresholds_pass": all(value <= 1.25 for key, value in ratios.items() if key != "aggregate_cycle_throughput_ratio") and ratios["aggregate_cycle_throughput_ratio"] >= 0.8})
        report = {"schema": "cooperators.transport-scale-results/v1", "completed": error is None,
                  "end_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "identity": self.identity, "plan": self.plan, "results": self.results,
                  "scale_comparisons": comparisons, "binary_sha256_after": sha(self.binary)}
        assert report["binary_sha256_after"] == self.identity["binary_sha256"]
        report["source_sha256_after"] = {name: sha(self.repository / name)
                                         for name in self.identity["source_sha256"]}
        assert report["source_sha256_after"] == self.identity["source_sha256"]
        if error:
            report["error"] = self.redact(str(error))
        write_json(self.output / "summary.json", report)
        # Verify retained artifacts contain no generated credential plaintext.
        for path in self.output.rglob("*"):
            if path.is_file():
                data = path.read_bytes()
                assert all(credential.encode() not in data for credential in self.credentials), f"credential leak in {path}"
        write_json(self.output / "credential-check.json", {"credential_plaintext_absent": True,
                   "generated_credentials_checked": len(self.credentials), "check": "all retained regular-file bytes scanned"})
        manifest = {str(path.relative_to(self.output)): sha(path) for path in sorted(self.output.rglob("*")) if path.is_file()}
        write_json(self.output / "sha256.json", manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/coop"))
    parser.add_argument("--output", type=Path, required=True, help="new evidence directory")
    parser.add_argument("--samples", type=int, default=500)
    parser.add_argument("--warmups", type=int, default=20)
    parser.add_argument("--append-seconds", type=float, default=10)
    parser.add_argument("--agents", type=int, default=100)
    parser.add_argument("--scale-single-samples", type=int, default=1000)
    parser.add_argument("--scale-rounds", type=int, default=50)
    parser.add_argument("--scale-warmups", type=int, default=3)
    arguments = parser.parse_args()
    assert all(getattr(arguments, key) > 0 for key in ("samples", "warmups", "append_seconds", "agents", "scale_single_samples", "scale_rounds", "scale_warmups"))
    run = Run(arguments)
    try:
        run.execute()
    except BaseException as error:
        run.finish(error)
        raise
    else:
        run.finish()


if __name__ == "__main__":
    main()
