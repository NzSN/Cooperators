#!/usr/bin/env python3
"""Run only the four remaining frozen distributed-inbox phases.

This is an explicitly partial continuation. Completion of these phases does
not reverse candidate02's failed original eight-condition gate.
"""

import argparse
import json
from pathlib import Path
import sys


REPOSITORY = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPOSITORY / "scripts"))
import benchmark_throughput as benchmark
import benchmark_transport as transport


class PartialRun(benchmark.ThroughputRun):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.plan.update(
            run_scope="Distributed-only partial continuation: four remaining original phases",
            original_eight_condition_gate="FAILED: same-inbox C100 reached unchanged inbox capacity; this continuation cannot make that gate pass",
            original_failed_run="docs/impl/throughput-20261020/candidate02/throughput/summary.json",
            continuation_wrapper_sha256=transport.sha(Path(__file__)),
            continuation_declaration="docs/impl/throughput-20261020/candidate02/distributed-continuation-plan.json")
        transport.write_json(self.output / "plan.json", self.plan)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=REPOSITORY / "target/release/coop")
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    manifest = REPOSITORY / "docs/impl/throughput-20261020/qualification/run-02/source-manifest.json"
    identity = json.loads(manifest.read_text())
    assert transport.sha(arguments.binary) == identity["release_binary_sha256"]
    assert all(transport.sha(REPOSITORY / name) == expected
               for name, expected in identity["files"].items())
    arguments.label = "candidate02-distributed-partial"
    arguments.source_manifest = None
    arguments.seconds = 10.0
    arguments.warmup_seconds = 1.0
    arguments.warmup_minimum = 3
    arguments.drain_interval_ms = 20.0
    arguments.profiles = ["distributed-inboxes"]
    arguments.concurrencies = [1, 8, 32, 100]
    run = PartialRun(arguments)
    try:
        run.execute()
    except BaseException as error:
        run.finish(error)
        raise
    else:
        run.finish()


if __name__ == "__main__":
    main()
