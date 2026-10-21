#!/usr/bin/env python3
"""Retain an unchanged aggregate MBT gate and its exact source/tool provenance."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[4]
MIRROR = Path("/home/nzsn/.local/bin/ModelMirrors")
COMPILER = Path("/home/nzsn/Repos/Mirrors/.lake/build/bin/model_interface_gen")
BINARY = ROOT / "spec/mbt/target/debug/cooperators-mbt"


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def save(name, value):
    (HERE / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def capture(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def sources():
    names = capture(
        "git", "ls-files", "-c", "-o", "--exclude-standard", "--",
        "src", "spec", "Cargo.toml", "Cargo.lock",
    ).splitlines()
    excluded = {"target", "evidence", "_apalache-out"}
    return {
        name: digest(ROOT / name)
        for name in sorted(set(names))
        if (ROOT / name).is_file() and not excluded.intersection(Path(name).parts)
    }


def execute(label, command, env=None):
    started = time.time()
    print(f"starting {label}", flush=True)
    with (HERE / f"{label}.stdout.log").open("wb") as stdout:
        with (HERE / f"{label}.stderr.log").open("wb") as stderr:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=stdout, stderr=stderr)
    report = {
        "argv": command,
        "cwd": str(ROOT),
        "started_unix": started,
        "finished_unix": time.time(),
        "elapsed_seconds": time.time() - started,
        "exit_code": result.returncode,
    }
    save(f"{label}.result.json", report)
    print(f"finished {label}: exit {result.returncode}", flush=True)
    return report


def redact_credentials():
    # Runtime profile snapshots retain fixture/config.json to make process
    # startup observable. Preserve each issued credential's digest for review.
    candidates = sorted(path for path in HERE.rglob("*") if path.is_file())
    key_pattern = re.compile(rb'"(?:token|old_token|launch_key)"\s*:\s*"([0-9a-f]{32})"')
    secrets = set()
    for path in candidates:
        if path.name == "run_gate.py":
            continue
        secrets.update(key_pattern.findall(path.read_bytes()))
    replacements = {
        secret: ("sha256:" + hashlib.sha256(secret).hexdigest()).encode()
        for secret in secrets
    }
    changed = []
    for path in candidates:
        if path.name == "run_gate.py":
            continue
        before = path.read_bytes()
        after = before
        count = 0
        for secret, replacement in replacements.items():
            count += after.count(secret)
            after = after.replace(secret, replacement)
        if after != before:
            path.write_bytes(after)
            changed.append({
                "path": str(path.relative_to(HERE)),
                "before_sha256": hashlib.sha256(before).hexdigest(),
                "after_sha256": hashlib.sha256(after).hexdigest(),
                "replacements": count,
            })
    remaining = [
        str(path.relative_to(HERE))
        for path in candidates
        if path.name != "run_gate.py" and key_pattern.search(path.read_bytes())
    ]
    report = {
        "credential_count": len(secrets),
        "changed_files": changed,
        "remaining_plaintext_credential_files": remaining,
        "method": "Replace retained generated 128-bit token plaintext with sha256:<digest> after all fixture processes finish; runtime behavior and assertions are unchanged.",
    }
    save("credential-redaction.json", report)
    return report


def main():
    if (HERE / "summary.json").exists() or (HERE / "raw").exists():
        raise RuntimeError("existing run evidence must not be overwritten")
    before = sources()
    tool_paths = {
        "ModelMirrors": MIRROR,
        "model_interface_gen": COMPILER,
        "cargo": Path(capture("rustup", "which", "cargo")),
        "rustc": Path(capture("rustup", "which", "rustc")),
        "python3": Path(shutil.which("python3")).resolve(),
    }
    save("provenance-before.json", {
        "git_head": capture("git", "rev-parse", "HEAD"),
        "sources": before,
        "tools": {name: {"path": str(path), "sha256": digest(path)} for name, path in tool_paths.items()},
        "versions": {"cargo": capture("cargo", "--version"), "rustc": capture("rustc", "-Vv"), "python3": sys.version},
        "features": ["mbt-injection"],
    })
    build = execute("build", ["cargo", "build", "--offline", "--locked", "--manifest-path", "spec/mbt/Cargo.toml"])
    replay = None
    binary_sha = None
    if build["exit_code"] == 0:
        binary_sha = digest(BINARY)
        raw = HERE / "raw"
        raw.mkdir()
        env = os.environ.copy()
        env.update(MIRROR_BIN=str(MIRROR), COOP_MBT_EVIDENCE_DIR=str(raw))
        save("replay.environment.json", {
            "MIRROR_BIN": str(MIRROR),
            "COOP_MBT_EVIDENCE_DIR": str(raw),
            "binary_sha256": binary_sha,
        })
        replay = execute("replay", [str(BINARY), "--skip-gen", "--suite", "all"], env)
    after = sources()
    changed = sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))
    save("provenance-after.json", {"sources": after, "changed_sources": changed, "binary_sha256": digest(BINARY) if BINARY.is_file() else None})
    redaction = redact_credentials()
    log = (HERE / "replay.stderr.log").read_text() if replay else ""
    stdout = (HERE / "replay.stdout.log").read_text() if replay else ""
    protocol = re.search(r"mbt: (\d+) traces, (\d+) failed", log)
    faults = re.search(r"fault MBT: (\d+) traces, (\d+) failures", log)
    crashes = re.findall(r"^PASS actual process crash boundary (.+)$", log, re.MULTILINE)
    summary = {
        "build": build,
        "replay": replay,
        "binary_sha256": binary_sha,
        "protocol_and_writer_traces": int(protocol[1]) if protocol else None,
        "protocol_and_writer_failures": int(protocol[2]) if protocol else None,
        "fault_traces": int(faults[1]) if faults else None,
        "fault_failures": int(faults[2]) if faults else None,
        "actual_crash_controls": crashes,
        "compiler_and_provenance_gate_passed": "Generated fault interface bridge and selected compiler checks clean" in stdout,
        "changed_sources": changed,
        "credential_redaction": {"credential_count": redaction["credential_count"], "remaining_plaintext_credential_files": redaction["remaining_plaintext_credential_files"]},
    }
    summary["passed"] = bool(
        replay and replay["exit_code"] == 0 and not changed
        and summary["protocol_and_writer_traces"] == 39 and summary["protocol_and_writer_failures"] == 0
        and summary["fault_traces"] == 24 and summary["fault_failures"] == 0
        and len(crashes) == 2 and summary["compiler_and_provenance_gate_passed"]
        and not redaction["remaining_plaintext_credential_files"]
    )
    save("summary.json", summary)
    print(json.dumps({"passed": summary["passed"], "evidence": str(HERE)}), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
