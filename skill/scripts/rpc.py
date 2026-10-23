#!/usr/bin/env python3
"""One bounded UDS JSON-RPC call to an existing Cooperators daemon; no retries."""
import argparse
import json
import math
import os
from pathlib import Path
import socket
import sys
import time

MAX_BYTES = 4 * 1024 * 1024
READ_METHODS = {
    "ping", "echo", "daemon.status", "instance_metadata", "list_agents",
    "list_docs", "list_arrival_subscriptions", "wait_message", "wait_arrivals",
    "read_channel", "read_doc",
}


def reject_constant(_value):
    raise ValueError("non-finite JSON number")


def fail(code, message, status):
    print(json.dumps({"error": {"code": code, "message": message}}), file=sys.stderr)
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", help="explicit local Unix socket")
    parser.add_argument("--root", type=Path, help="root; COOPERATORS_HOME takes precedence")
    parser.add_argument("--timeout", type=float, default=40, help="whole exchange deadline in seconds")
    parser.add_argument("--params-file", type=Path, help="read bounded JSON params from a file")
    parser.add_argument("method", help="native RPC method, not the coop_ MCP tool name")
    parser.add_argument("params", nargs="?", help="JSON object; '-' reads stdin; default {}")
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        return fail("INVALID_INPUT", "timeout must be finite and positive", 2)
    if args.params_file and args.params is not None:
        return fail("INVALID_INPUT", "choose positional params or --params-file", 2)
    if os.environ.get("COOP_ENDPOINT") and not args.socket:
        return fail("INVALID_INPUT", "UDS-only helper: use the HTTPS client, or explicitly select --socket", 2)
    try:
        if args.params_file:
            with args.params_file.open("rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
        elif args.params == "-":
            raw = sys.stdin.buffer.read(MAX_BYTES + 1)
        else:
            raw = (args.params or "{}").encode("utf-8")
        if len(raw) > MAX_BYTES:
            raise ValueError("params exceed limit")
        params = json.loads(raw, parse_constant=reject_constant)
        if not isinstance(params, dict) or "token" in params:
            return fail("INVALID_INPUT", "params must be an object without token; use COOP_TOKEN", 2)
        token = os.environ.get("COOP_TOKEN")
        if token:
            params["token"] = token
        request_id = "cooperators-skill"
        frame = json.dumps({"jsonrpc": "2.0", "id": request_id,
                            "method": args.method, "params": params},
                           allow_nan=False, separators=(",", ":")).encode("utf-8") + b"\n"
        if len(frame) > MAX_BYTES:
            raise ValueError("request exceeds limit")
    except (OSError, ValueError, UnicodeError, RecursionError):
        return fail("INVALID_INPUT", "cannot read/encode bounded JSON object params", 2)

    root = Path(os.environ.get("COOPERATORS_HOME") or args.root or Path.home() / ".local/share/cooperators")
    address = args.socket or os.environ.get("COOP_SOCKET") or str(root / "run/daemon.sock")
    submitted = False
    deadline = time.monotonic() + args.timeout
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
            def set_deadline():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("exchange deadline")
                stream.settimeout(remaining)
            set_deadline()
            stream.connect(address)
            set_deadline()
            submitted = True  # A partial write may still reach the daemon.
            stream.sendall(frame)
            response = bytearray()
            while b"\n" not in response:
                set_deadline()
                chunk = stream.recv(min(65536, MAX_BYTES + 1 - len(response)))
                if not chunk:
                    raise ValueError("incomplete response")
                response.extend(chunk)
                if len(response) > MAX_BYTES:
                    raise ValueError("response exceeds limit")
        reply = json.loads(bytes(response).split(b"\n", 1)[0], parse_constant=reject_constant)
        if not isinstance(reply, dict) or reply.get("jsonrpc") != "2.0" or reply.get("id") != request_id:
            raise ValueError("invalid response identity")
        if ("result" in reply) == ("error" in reply):
            raise ValueError("invalid response outcome")
        if "error" in reply:
            error = reply["error"]
            if (not isinstance(error, dict) or type(error.get("code")) is not int
                    or not isinstance(error.get("message"), str)):
                raise ValueError("invalid RPC error")
            print(json.dumps({"error": error}, ensure_ascii=True), file=sys.stderr)
            return 1
        print(json.dumps(reply["result"], ensure_ascii=True, allow_nan=False))
        return 0
    except (OSError, ValueError, UnicodeError, RecursionError):
        if submitted and args.method not in READ_METHODS:
            return fail("OUTCOME_UNKNOWN", "mutation exchange failed after submission; reconcile before retrying", 3)
        return fail("TRANSPORT_ERROR", "UDS exchange failed; check socket, deadline and response framing", 2)


if __name__ == "__main__":
    raise SystemExit(main())
