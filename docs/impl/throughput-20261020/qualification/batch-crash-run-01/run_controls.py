#!/usr/bin/env python3
"""Run only batch process-crash controls and retain every attempt."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[4]
CORE_MANIFEST = HERE.parent / "run-02/source-manifest.json"


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def snapshot(expected):
    return {name: digest(ROOT / name) for name in expected}


def main():
    attempt_index = 1
    while (HERE / f"attempt-{attempt_index:02}").exists():
        attempt_index += 1
    attempt = HERE / f"attempt-{attempt_index:02}"
    attempt.mkdir()
    core = json.loads(CORE_MANIFEST.read_text())
    before = snapshot(core["files"])
    mismatch = [name for name, value in before.items() if value != core["files"][name]]
    save(attempt / "source-before.json", {
        "core_source_manifest": str(CORE_MANIFEST.relative_to(ROOT)),
        "expected_core_source_files": core["files"],
        "actual_core_source_files": before,
        "mismatched_files": mismatch,
        "release_binary_sha256": core["release_binary_sha256"],
        "new_test_sha256": digest(ROOT / "tests/batch_crash.rs"),
        "runner_sha256": digest(Path(__file__)),
    })
    if mismatch:
        raise RuntimeError("production source differs from final run-02 source manifest")
    command = [
        "cargo", "test", "--offline", "--locked", "--features", "mbt-injection",
        "--test", "batch_crash", "--", "--test-threads=1", "--nocapture",
    ]
    env = os.environ.copy()
    env.pop("COOP_BATCH_CRASH_MODE", None)
    env.pop("COOP_BATCH_CRASH_ROOT", None)
    env.update(CARGO_BUILD_JOBS="2", COOP_BATCH_CRASH_EVIDENCE_DIR=str(attempt / "controls"))
    started = time.time()
    with (attempt / "test.stdout.log").open("wb") as stdout:
        with (attempt / "test.stderr.log").open("wb") as stderr:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=stdout, stderr=stderr)
    finished = time.time()
    after = snapshot(core["files"])
    changed = [name for name in before if before[name] != after[name]]
    controls = []
    for mode in ("before-sync", "after-sync", "partial-failure"):
        directory = attempt / "controls" / mode
        control = {"mode": mode, "complete": False}
        required = [directory / name for name in ("process-result.json", "boundary.json", "recovery.json")]
        if all(path.is_file() for path in required):
            process, boundary, recovery = [json.loads(path.read_text()) for path in required]
            control.update(
                complete=True,
                process=process,
                binary_sha256=digest(Path(process["program"])),
                successful_batch_receipts=len(boundary["successful_batch_receipts"]),
                published_records=len(boundary["published_records"]),
                recovered_records=len(recovery["recovered_records"]),
                truncated_bytes=recovery["truncated_bytes"],
                prior_acknowledged_prefix_preserved=recovery["prior_acknowledged_prefix_preserved"],
                duplicate_retry_count=len(recovery["duplicate_retries"]),
                dedupe_left_log_unchanged=recovery["dedupe_left_log_unchanged"],
                hooks=boundary["hooks"],
            )
        controls.append(control)
    credential_pattern = re.compile(rb'"(?:token|old_token|launch_key)"\s*:\s*"[0-9a-f]{32}"')
    credential_files = [
        str(path.relative_to(attempt))
        for path in attempt.rglob("*")
        if path.is_file() and credential_pattern.search(path.read_bytes())
    ]
    summary = {
        "command": command,
        "environment": {key: env[key] for key in ("CARGO_BUILD_JOBS", "COOP_BATCH_CRASH_EVIDENCE_DIR")},
        "started_unix": started,
        "finished_unix": finished,
        "elapsed_seconds": finished - started,
        "exit_code": result.returncode,
        "core_source_files_matched": len(before) - len(mismatch),
        "changed_core_source_files": changed,
        "release_binary_sha256": core["release_binary_sha256"],
        "plaintext_credential_files": credential_files,
        "controls": controls,
        "limitations": [
            "Actual Unix SIGKILL validates process-crash recovery; it does not simulate loss of the kernel page cache or hardware power failure.",
            "Socketless production broker worker calls exercise batch sync, publication, replay and dedupe; this control does not exercise network transports.",
            "Recovery dedupe retries use the identity reconstructed from the persisted registry through the in-process broker API; authentication transport behavior is covered by other qualification evidence.",
            "The helper test returns immediately in ordinary parent test collection; the three parent tests each execute and kill a dedicated helper child.",
        ],
    }
    summary["passed"] = bool(
        result.returncode == 0 and not changed and not credential_files
        and all(control["complete"] and control["process"]["signal"] == 9
                and control["prior_acknowledged_prefix_preserved"]
                and control["dedupe_left_log_unchanged"] for control in controls)
    )
    save(attempt / "summary.json", summary)
    save(HERE / "latest-attempt.json", {"attempt": attempt.name, "passed": summary["passed"]})
    print(json.dumps({"attempt": attempt.name, "passed": summary["passed"], "elapsed_seconds": finished - started}), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
