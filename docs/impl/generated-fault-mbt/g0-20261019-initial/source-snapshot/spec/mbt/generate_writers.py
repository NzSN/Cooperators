#!/usr/bin/env python3
"""Generate the five root-ownership MBT witnesses, without changing bindings."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

GOALS = ("NoCompeteW1", "NoCompeteW2", "NoIndependentRoots", "NoHandoff", "NoRejectedRecovery")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    home = Path(__file__).resolve().parent
    output = args.output or home / "writers/traces"
    output.mkdir(parents=True, exist_ok=True)
    source = home.parent / "MBWriters.tla"
    with tempfile.TemporaryDirectory(prefix="coop-writer-gen-") as work:
        work = Path(work)
        spec = work / source.name
        spec.write_bytes(source.read_bytes())
        for goal in GOALS:
            result = subprocess.run([
                "apalache-mc", "check", f"--inv={goal}", "--length=6", "--max-error=1",
                "--cinit=CInit", str(spec),
            ], cwd=work, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
            if result.returncode != 12:
                raise RuntimeError(f"{goal}: exit {result.returncode}\n{result.stdout[-5000:]}")
            traces = sorted(work.glob("_apalache-out/MBWriters.tla/*/violation.itf.json"))
            # Each goal gets a separate directory so selecting output is unambiguous.
            if len(traces) != 1:
                raise RuntimeError(f"{goal}: expected one witness, got {traces}")
            trace = json.loads(traces[0].read_text())
            for state in trace["states"]:
                state.pop("Writers", None)
                state.pop("Roots", None)
            (output / f"{goal}.itf.json").write_text(json.dumps(trace, indent=2) + "\n")
            print(f"generated {goal}", flush=True)
            shutil.rmtree(work / "_apalache-out")


if __name__ == "__main__":
    main()
