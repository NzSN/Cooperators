#!/usr/bin/env python3
"""Measure production broker/bridge admission with an immediate protocol host.

This deliberately invokes no installed provider or model. Each sample has one
fresh bus, managed bridge, and wake. Source-RPC start through observation of a
durable accepted submission is a conservative upper bound on feed-to-host
acceptance, including source sync, coalescing, transport and bridge journal sync.
"""

import argparse
import contextlib
import json
from pathlib import Path
import time

from qualify_arrivals import Run, quantiles, sha


class AdmissionRun(Run):
    def spawn(self, command, **kwargs):
        if "host" in command and "run" in command:
            fixture = Path("tests/fixtures/host_protocol_fake.py").resolve()
            command += ["--executable", str(fixture)]
            for key, value in {
                "COOP_FAKE_KIND": "codex", "COOP_FAKE_MODE": "normal",
                "COOP_FAKE_ROOT": str(self.workspace),
                "COOP_FAKE_SESSION": "00000000-0000-4000-8000-000000000001",
            }.items():
                command += ["--env", f"{key}={value}"]
        return super().spawn(command, **kwargs)

    def performance(self):
        self.summary["mode"] = "controlled_host_admission"
        self.summary["model_invoked"] = False
        self.summary["fixture_sha256"] = sha("tests/fixtures/host_protocol_fake.py")
        self.daemon()
        author = self.enroll("admission-author")
        self.host()
        started = time.monotonic_ns()
        self.publish(author, 1)
        accepted = []

        def committed():
            for path in (self.temp / "state").glob("*/bridge.jsonl"):
                records = [json.loads(line) for line in path.read_text().splitlines()]
                accepted[:] = [r for r in records if r.get("event") == "submission_recorded"
                               and r.get("submission", {}).get("status") == "accepted"]
                if accepted:
                    self.journal = records
                    return True
            return False

        self.until(committed, timeout=10)
        elapsed = (time.monotonic_ns() - started) / 1e6
        assert len(accepted) == 1 and accepted[0]["submission"]["turn_id"] == "turn-1"
        requests = [json.loads(line) for line in (self.workspace / "requests.jsonl").read_text().splitlines()]
        turns = [r for r in requests if r.get("method") == "turn/start"]
        assert len(turns) == 1
        assert turns[0]["params"]["threadId"] == self.last_status["binding"]["exact_host_session_id"]
        received = [r for r in self.journal if r.get("event") == "received"]
        assert len(received) == 1 and len(received[0]["notices"]) == 1
        wake = next(r["wake"] for r in self.journal if r.get("event") == "wake_created")
        assert wake["notices"][0]["delivery_id"] == received[0]["notices"][0]["delivery_id"]
        assert accepted[0]["wake_id"] == wake["wake_id"]
        self.save("bridge-records.json", self.journal)
        self.summary["source_start_to_accepted_journal_observed_ms"] = elapsed
        self.summary["passed"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/coop"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=30)
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    rows = []
    for index in range(args.samples + 2):
        options = argparse.Namespace(binary=args.binary, output=args.output / f"sample-{index:03}",
                                     samples=1, mode="performance", provider="codex")
        run = AdmissionRun(options)
        with (options.output / "runner.log").open("w") as log, contextlib.redirect_stdout(log):
            result = run.run()
        if result:
            raise RuntimeError(f"controlled-host sample failed: {options.output / 'summary.json'}")
        if index >= 2:
            rows.append(run.summary["source_start_to_accepted_journal_observed_ms"])
    summary = {"schema": "cooperators.controlled-arrival-admission/v1",
               "script_sha256": sha(__file__), "helper_sha256": sha("scripts/qualify_arrivals.py"),
               "binary_sha256": sha(args.binary), "samples": rows, "warmups": 2,
               "quantiles_ms": quantiles(rows), "target_p95_ms": 1000, "model_invoked": False,
               "measurement": "Source RPC start to observed committed accepted submission; conservative feed-to-host upper bound",
               "limitations": ["Immediate fake host qualifies broker/bridge transport admission only",
                               "Installed-provider admission and model execution have separate evidence",
                               "One host, one window, isolated single-arrival roots; no large-history or throughput claim"],
               "passed": quantiles(rows)["p95"] < 1000}
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: v for k, v in summary.items() if k != "samples"}))
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
