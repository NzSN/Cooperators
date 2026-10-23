#!/usr/bin/env python3
"""Run a source-pinned local gate and retain its actual exit status and log."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("command", nargs=argparse.REMAINDER)
args = parser.parse_args()
command = args.command[1:] if args.command[:1] == ["--"] else args.command
if not command:
    parser.error("a command is required")
root = Path(__file__).resolve().parents[1]
args.output.mkdir(parents=True, exist_ok=False)


def manifest():
    paths = [root / "Cargo.toml", root / "Cargo.lock"]
    for directory in ["src", "tests", "scripts", "spec"]:
        paths.extend(p for p in (root / directory).rglob("*")
                     if p.is_file() and "target" not in p.relative_to(root).parts
                     and "__pycache__" not in p.parts and p.suffix in {".rs", ".py", ".tla", ".cfg", ".json", ".toml", ".lock", ".pem"})
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(paths))}


before = manifest()
started = time.time()
with (args.output / "command.log").open("w") as log:
    result = subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
after = manifest()
text = (args.output / "command.log").read_text(errors="replace")
counts = [tuple(map(int, row)) for row in re.findall(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", text)]
skips = len(re.findall(r"(?m)^SKIP", text))
required = bool(os.getenv("COOP_REQUIRE_UDS") or os.getenv("COOP_REQUIRE_REMOTE"))
receipt = {
    "command": command,
    "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    "started_unix": started,
    "duration_seconds": time.time() - started,
    "exit_code": result.returncode,
    "source_unchanged": before == after,
    "source_manifest": before,
    "source_manifest_after": after if before != after else None,
    "passed_tests": sum(row[0] for row in counts),
    "failed_tests": sum(row[1] for row in counts),
    "ignored_tests": sum(row[2] for row in counts),
    "reported_skips": skips,
    "required_sockets": required,
    "gate_environment": {name: os.environ[name] for name in [
        "COOP_REQUIRE_UDS", "COOP_REQUIRE_REMOTE", "COOP_PRENETWORK_BINARY",
        "COOP_PREUPGRADE_BINARY", "MODEL_INTERFACE_GEN", "MIRROR_BIN",
    ] if name in os.environ},
    "passed": result.returncode == 0 and before == after and (not required or skips == 0),
    "log_sha256": hashlib.sha256((args.output / "command.log").read_bytes()).hexdigest(),
}
(args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps({key: value for key, value in receipt.items() if not key.startswith("source_manifest")}))
raise SystemExit(0 if receipt["passed"] else (result.returncode or 2))
