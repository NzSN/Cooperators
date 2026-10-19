#!/usr/bin/env python3
"""Verify projected traces still distinguish mutants through MBFaults.Next.

Uses the canonical fixed-input probe generator and TLC checker. The only oracle
change is selecting runtime-observable storage fields for comparison. No typed
interface-view module supplies a transition relation.
"""
import argparse
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import shutil
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "spec/mbt/fault_interfaces"


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", action="append")
    parser.add_argument("--tlc", default="tlc")
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--output", type=Path, default=BASE / "evidence/projected-storage")
    args = parser.parse_args()
    bridge = module("bridge", ROOT / "spec/mbt/generate_fault_interfaces.py")
    groups = bridge.prepare(True)
    profiles = args.profile or list(groups)
    if set(profiles) - set(groups):
        raise ValueError("unknown fault profile")
    checker = module("checker", ROOT / "spec/mbt/check_fault_detection.py")
    checks = module("checks", ROOT / "spec/check_hardening.py")
    traces = [json.loads(path.read_bytes()) for profile in profiles for path in groups[profile]]
    goals = {trace["#meta"]["goal"] for trace in traces}
    checker.KILLERS = {fault: goal for fault, goal in checker.KILLERS.items() if goal in goals}
    selected = SimpleNamespace(
        FAULT_BOUNDS=checks.FAULT_BOUNDS,
        FAULT_CONTROLS=[row for row in checks.FAULT_CONTROLS if row[0] in profiles])
    original_probe = checker.probe_source

    def projected_probe(trace):
        source = original_probe(trace)
        selection = '''RuntimeObservation == IF Scenario = "storage"
  THEN [records |-> observation.records, receipts |-> observation.receipts]
  ELSE observation
TraceConforms == /\\ RuntimeObservation = Expected[steps + 1].observation'''
        needle = "TraceConforms == /\\ observation = Expected[steps + 1].observation"
        if source.count(needle) != 1:
            raise ValueError("canonical probe changed; review projection injection")
        return source.replace(needle, selection)

    checker.probe_source = projected_probe
    output = io.StringIO()
    with redirect_stdout(output):
        results = checker.check_detection(ROOT / "spec", traces, selected, args.tlc, args.timeout)
    text = output.getvalue()
    print(text, end="", flush=True)
    match = re.search(r"Fixed-input trace detection evidence: (.*)\n", text)
    if not match:
        raise ValueError("checker did not identify its evidence directory")
    work = Path(match[1])
    if args.output.exists():
        raise ValueError("output exists; preserve previous evidence and select a new --output")
    args.output.mkdir(parents=True)
    for case in results:
        destination = args.output / case["case"]
        destination.mkdir()
        for filename in ("TraceProbe.tla", "TraceProbe.cfg", "check.log"):
            shutil.copyfile(work / case["case"] / filename, destination / filename)
    report = {
        "schema": "cooperators.generated-interface-projection-check/v1",
        "profiles": profiles,
        "proofBoundary": "Bounded fixed-input checks against canonical MBFaults.Next; no implementation runtime execution.",
        "bridgeManifestSha256": hashlib.sha256((BASE / "provenance.json").read_bytes()).hexdigest(),
        "sourceSha256": hashlib.sha256((ROOT / "spec/MBFaults.tla").read_bytes()).hexdigest(),
        "registrySha256": hashlib.sha256((ROOT / "spec/AgentInstances.tla").read_bytes()).hexdigest(),
        "cases": results,
    }
    (args.output / "results.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"Preserved {len(results)} projected fixed-input checks at {args.output}")


if __name__ == "__main__":
    main()
