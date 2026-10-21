#!/usr/bin/env python3
"""Forward a real MCP stdio session and retain timestamped wire evidence."""

import argparse
import datetime
import json
import subprocess
import sys
import threading


def scrub(value):
    if isinstance(value, dict):
        return {
            key: "[redacted]" if key.lower() in {"token", "authorization"} else scrub(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [scrub(item) for item in value]
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--kind", required=True)
    parser.add_argument("--display-name", required=True)
    args = parser.parse_args()
    lock = threading.Lock()
    with open(args.log, "a", encoding="utf-8", buffering=1) as evidence:
        child = subprocess.Popen(
            [args.binary, "mcp-stdio", "--kind", args.kind, "--display-name", args.display_name],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr.buffer,
        )

        def record(direction, line):
            try:
                message = scrub(json.loads(line))
            except (ValueError, UnicodeDecodeError):
                message = {"unparsed": line.decode("utf-8", errors="replace")}
            row = {
                "time_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "direction": direction,
                "message": message,
            }
            with lock:
                evidence.write(json.dumps(row, separators=(",", ":")) + "\n")

        def forward_input():
            try:
                for line in sys.stdin.buffer:
                    record("client_to_broker", line)
                    child.stdin.write(line)
                    child.stdin.flush()
            except (BrokenPipeError, OSError):
                pass
            finally:
                try:
                    child.stdin.close()
                except (BrokenPipeError, OSError):
                    pass

        threading.Thread(target=forward_input, daemon=True).start()
        try:
            for line in child.stdout:
                record("broker_to_client", line)
                sys.stdout.buffer.write(line)
                sys.stdout.buffer.flush()
            return child.wait()
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=10)


if __name__ == "__main__":
    sys.exit(main())
