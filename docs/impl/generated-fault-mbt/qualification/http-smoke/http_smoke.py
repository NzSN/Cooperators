#!/usr/bin/env python3
"""Actual loopback TCP + UDS smoke against an existing full coop executable.

Standard library only. Builds nothing, changes no production source, launches
one isolated daemon, redacts credentials in retained wire transcripts, and
retains actual regular bus files after terminating/waiting for its child.
"""
import argparse
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import stat
import subprocess
import tempfile
import time


def sha(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Smoke:
    def __init__(self, repository, binary, output):
        self.repository, self.binary, self.output = repository, binary, output
        self.root = Path(tempfile.mkdtemp(prefix="chs-", dir="/tmp"))
        self.child = None
        self.credentials = set()
        self.checks = []
        self.endpoint = None
        self.session_id = None
        self.report = {"schema": "cooperators.actual-http-smoke/v1", "checks": self.checks}

    def redact(self, value):
        if isinstance(value, dict):
            return {key: "[REDACTED]" if key.lower() in {
                "token", "launch_key", "authorization", "mcp-session-id"
            } else self.redact(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.redact(item) for item in value]
        if isinstance(value, str):
            for credential in self.credentials:
                value = value.replace(credential, "[REDACTED]")
        return value

    def wire(self, row):
        with (self.output / "wire-redacted.jsonl").open("a") as target:
            target.write(json.dumps(self.redact(row), sort_keys=True) + "\n")

    def check(self, name, condition, detail=None):
        if not condition:
            raise AssertionError(f"{name}: {self.redact(detail)}")
        self.checks.append(name)
        print(f"PASS {name}", flush=True)

    def uds(self, method, params=None):
        request = {"jsonrpc": "2.0", "id": method, "method": method, "params": params or {}}
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(5)
            connection.connect(str(self.root / "run/daemon.sock"))
            connection.sendall(json.dumps(request).encode() + b"\n")
            response = json.loads(connection.makefile("rb").readline())
        token = response.get("result", {}).get("token")
        if token:
            self.credentials.add(token)
        self.wire({"transport": "uds", "request": request, "response": response})
        return response

    def http(self, method, path, body=None, token=None):
        headers = {"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-03-26"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        connection = http.client.HTTPConnection("127.0.0.1", self.endpoint, timeout=5)
        try:
            connection.request(method, path, None if body is None else json.dumps(body), headers)
            peer = connection.sock.getpeername()
            response = connection.getresponse()
            status, response_headers = response.status, dict(response.getheaders())
            raw = response.read().decode("utf-8")
        finally:
            connection.close()
        for key, value in response_headers.items():
            if key.lower() == "mcp-session-id":
                self.session_id = value
        self.wire({"transport": "actual-tcp-http", "peer": peer,
                   "request": {"method": method, "path": path, "headers": headers, "body": body},
                   "response": {"status": status, "headers": response_headers, "body": raw}})
        messages = []
        if raw.strip():
            content_type = next((value for key, value in response_headers.items() if key.lower() == "content-type"), "")
            if "application/json" in content_type:
                messages = [json.loads(raw)]
            elif "text/event-stream" in content_type:
                messages = [json.loads(line[5:].strip()) for line in raw.splitlines() if line.startswith("data:")]
        return status, messages, raw, response_headers

    def mcp(self, method, request_id, token, params=None):
        body = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            body["params"] = params
        status, messages, _, _ = self.http("POST", "/mcp", body, token)
        if status != 200:
            raise AssertionError(f"MCP {method} status {status}")
        response = next((row for row in messages if row.get("id") == request_id), None)
        if response is None or "error" in response:
            raise AssertionError(f"MCP response failed: {self.redact(response)}")
        return response["result"]

    def tool(self, name, request_id, token):
        result = self.mcp("tools/call", request_id, token, {"name": name, "arguments": {}})
        if result.get("isError"):
            raise AssertionError(f"tool failed: {self.redact(result)}")
        if "structuredContent" in result:
            return result["structuredContent"]
        return json.loads(next(row["text"] for row in result["content"] if row["type"] == "text"))

    def run(self):
        environment = os.environ.copy()
        for name in ("COOPERATORS_HOME", "COOP_SOCKET", "COOP_HTTP_ADDR", "COOP_TOKEN"):
            environment.pop(name, None)
        command = [str(self.binary), "--root", str(self.root), "daemon", "run", "--http-addr", "127.0.0.1:0"]
        self.report.update({"dateUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "command": command, "originalRoot": str(self.root), "binaryPath": str(self.binary),
            "binarySha256Before": sha(self.binary), "scriptSha256": sha(Path(__file__)),
            "sourceHashes": {str(path.relative_to(self.repository)): sha(path) for path in sorted(self.repository.glob("src/**/*.rs"))}})
        with (self.output / "daemon.stderr.log").open("wb") as stderr, (self.output / "daemon.stdout.log").open("wb") as stdout:
            self.child = subprocess.Popen(command, env=environment, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
        self.report["pid"] = self.child.pid
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.child.poll() is not None:
                raise RuntimeError("daemon exited before readiness")
            text = (self.output / "daemon.stderr.log").read_text()
            match = re.search(r"MCP streamable HTTP on http://127\.0\.0\.1:(\d+)/mcp", text)
            if match and (self.root / "run/daemon.sock").exists():
                self.endpoint = int(match[1])
                break
            time.sleep(0.025)
        if self.endpoint is None:
            raise RuntimeError("actual HTTP listener did not become ready")
        self.report["endpoint"] = f"http://127.0.0.1:{self.endpoint}"
        self.report["runningExecutableSha256"] = sha(Path(f"/proc/{self.child.pid}/exe"))
        self.check("running executable matches selected binary", self.report["runningExecutableSha256"] == self.report["binarySha256Before"])
        status = self.uds("daemon.status")["result"]
        self.check("UDS status identifies own daemon PID", status["pid"] == self.child.pid)
        code, messages, _, _ = self.http("GET", "/healthz")
        self.check("actual HTTP health reports listener true", code == 200 and messages[0]["http_listener"] is True)
        launch_key = secrets.token_hex(16)
        self.credentials.add(launch_key)
        enrolled = self.uds("enroll_agent", {"launch_key": launch_key, "kind": "codex", "display_name": "http-smoke", "capabilities": []})["result"]
        token = enrolled["token"]
        self.report["agentId"] = enrolled["agent_id"]
        init = {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "actual-http-smoke", "version": "1"}}
        for label, denied_token in (("missing", None), ("invalid", "invalid-smoke-bearer")):
            for path in ("/mcp", "/metrics"):
                body = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": init} if path == "/mcp" else None
                code, values, _, headers = self.http("POST" if body else "GET", path, body, denied_token)
                self.check(f"{label} bearer {path} rejected 401", code == 401 and values[0]["error"]["data"]["code"] == "UNAUTHENTICATED" and any(key.lower() == "www-authenticate" for key in headers))
        initialized = self.mcp("initialize", 2, token, init)
        self.check("bearer MCP initialize over TCP", initialized["serverInfo"]["name"] == "cooperators" and initialized["protocolVersion"] == "2025-03-26")
        code, _, _, _ = self.http("POST", "/mcp", {"jsonrpc": "2.0", "method": "notifications/initialized"}, token)
        self.check("MCP initialized notification accepted", 200 <= code < 300)
        tools = self.mcp("tools/list", 3, token)["tools"]
        actual_names = [tool["name"] for tool in tools]
        expected_names = re.findall(r'tool: "(coop_[^"]+)"', (self.repository / "src/mcp.rs").read_text())
        self.check("TCP tools/list exposes all 23 source catalog tools", len(tools) == 23 and len(set(actual_names)) == 23 and set(actual_names) == set(expected_names))
        uds_metadata = self.uds("instance_metadata", {"token": token})["result"]
        metadata = self.tool("coop_instance_metadata", 4, token)
        self.check("HTTP instance metadata matches actual UDS enrollment", metadata == uds_metadata and metadata["agent_id"] == enrolled["agent_id"] and metadata["kind"] == "codex" and metadata["display_name"] == "http-smoke")
        heartbeat = self.tool("coop_heartbeat", 5, token)
        self.check("TCP heartbeat reports enrolled active instance", heartbeat["agent_id"] == enrolled["agent_id"] and heartbeat["state"] == "active")
        code, _, metrics, _ = self.http("GET", "/metrics", token=token)
        self.check("bearer metrics served by actual HTTP listener", code == 200 and "coop_http_listener 1" in metrics and "coop_agents_registered 1" in metrics and 'coop_http_requests_total{path="/mcp",status="401"} 2' in metrics)
        retired = self.tool("coop_retire_agent", 6, token)
        self.check("TCP retirement returns same retired instance", retired["agent_id"] == enrolled["agent_id"] and retired["retired"] is True)
        for path in ("/mcp", "/metrics"):
            body = {"jsonrpc": "2.0", "id": 7, "method": "tools/list"} if path == "/mcp" else None
            code, values, _, _ = self.http("POST" if body else "GET", path, body, token)
            self.check(f"retired bearer {path} rejected 401", code == 401 and values[0]["error"]["data"]["code"] == "UNAUTHENTICATED")
        revoked = self.uds("instance_metadata", {"token": token})
        self.check("UDS also rejects retired credential", revoked.get("error", {}).get("data", {}).get("code") == "UNAUTHENTICATED")
        code, health, _, _ = self.http("GET", "/healthz")
        self.check("HTTP listener remains healthy after retirement", code == 200 and health[0]["http_listener"] is True)
        registry = json.loads((self.root / "registry.json").read_text())
        self.check("retirement persisted in actual registry", any(row["agent_id"] == enrolled["agent_id"] and row["retired"] is True for row in registry["agents"].values()))

    def finish(self, error=None):
        if self.child:
            self.child.terminate()
            try:
                self.child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait(timeout=5)
            self.report["daemonExitAfterFixtureTermination"] = self.child.returncode
        target = self.output / "raw-root"
        for path in self.root.rglob("*"):
            if stat.S_ISREG(path.lstat().st_mode):
                destination = target / path.relative_to(self.root)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, destination)
        self.report["passed"] = error is None
        if error is not None:
            self.report["error"] = self.redact(str(error))
        for path in self.output.rglob("*"):
            if path.is_file():
                data = path.read_bytes()
                if any(credential.encode() in data for credential in self.credentials):
                    raise RuntimeError(f"credential leaked into retained file {path.name}")
        self.report["credentialRedactionScanPassed"] = True
        self.report["files"] = {str(path.relative_to(self.output)): sha(path) for path in sorted(self.output.rglob("*")) if path.is_file() and path.name != "results.json"}
        (self.output / "results.json").write_text(json.dumps(self.report, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[5])
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    repository = args.repository.resolve()
    binary = (args.binary or repository / "target/debug/coop").resolve()
    output = args.output or Path(__file__).resolve().parent / ("run-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    output.mkdir(parents=True, exist_ok=False)
    smoke = Smoke(repository, binary, output)
    failure = None
    try:
        smoke.run()
    except Exception as error:
        failure = error
        print(f"FAIL {smoke.redact(str(error))}", flush=True)
    finally:
        smoke.finish(failure)
    print(f"Actual TCP HTTP smoke: {len(smoke.checks)} checks; evidence {output}", flush=True)
    raise SystemExit(0 if failure is None else 1)


if __name__ == "__main__":
    main()
