#!/usr/bin/env python3
"""Compare every original condition, preserving the candidate's failed row.

The distributed-only continuation supplies its four corresponding results.
It never substitutes for failed same-inbox C100 or reverses the failed gate.
"""

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def load_run(directory):
    manifest = json.loads((directory / "sha256.json").read_text())
    assert all(sha(directory / name) == expected for name, expected in manifest.items())
    result = json.loads((directory / "summary.json").read_text())
    assert json.loads((directory / "credential-check.json").read_text())["credential_plaintext_absent"]
    return result


def main():
    output = ROOT / "candidate02/comparison.json"
    assert not output.exists()
    baseline = load_run(ROOT / "baseline/throughput")
    candidate = load_run(ROOT / "candidate02/throughput")
    continuation = load_run(ROOT / "candidate02/throughput-distributed")
    controls = load_run(ROOT / "candidate02/controls")
    assert baseline["completed"] and not candidate["completed"] and continuation["completed"]
    assert controls["completed"]
    assert continuation["plan"]["original_eight_condition_gate"].startswith("FAILED:")
    assert candidate["identity"]["binary_sha256"] == continuation["identity"]["binary_sha256"] == controls["identity"]["binary_sha256"]
    assert baseline["identity"]["script_sha256"] == candidate["identity"]["script_sha256"] == continuation["identity"]["script_sha256"]
    assert baseline["identity"]["unchanged_transport_helper_sha256"] == candidate["identity"]["unchanged_transport_helper_sha256"] == continuation["identity"]["unchanged_transport_helper_sha256"]
    before = {(row["profile"], row["active_sender_agents"]): row for row in baseline["results"]}
    successful = candidate["results"] + continuation["results"]
    after = {(row["profile"], row["active_sender_agents"]): row for row in successful}
    assert len(successful) == len(after) == 7
    failed_phase = json.loads((ROOT / "candidate02/throughput/same-inbox-c100/daemon.json").read_text())
    assert "is full" in failed_phase["error"]
    rows = []
    for condition, old in before.items():
        new = after.get(condition)
        row = {"profile": condition[0], "concurrent_senders": condition[1],
               "baseline_acks_per_second": old["durable_acknowledgments_per_second"],
               "baseline_send_ack_ms": old["send_ack_ms"],
               "candidate_status": "COMPLETE" if new else "FAILED_RUN"}
        if new:
            assert new["correctness_passed"]
            assert new["requested_measurement_seconds"] == old["requested_measurement_seconds"] == 10
            assert new["enrolled_agents"] == old["enrolled_agents"]
            assert new["outstanding_sends_per_sender"] == old["outstanding_sends_per_sender"] == 1
            row.update(candidate_acks_per_second=new["durable_acknowledgments_per_second"],
                       candidate_send_ack_ms=new["send_ack_ms"],
                       throughput_ratio=new["durable_acknowledgments_per_second"] / old["durable_acknowledgments_per_second"],
                       candidate_throughput_5000_per_second_target_pass=new["throughput_5000_per_second_target_pass"],
                       candidate_p50_below_10_ms_target_pass=new["send_ack_p50_below_10_ms_target_pass"])
        else:
            assert condition == ("same-inbox", 100)
            row.update(candidate_acks_per_second=None, candidate_send_ack_ms=None,
                       candidate_throughput_5000_per_second_target_pass=False,
                       candidate_p50_below_10_ms_target_pass=False,
                       failure=failed_phase["error"],
                       unavailable_metrics_reason="Run failed; do not substitute diagnostic or best-case values")
        rows.append(row)
    value = {"schema": "cooperators.partial-eight-condition-comparison/v1",
             "original_eight_condition_gate": "FAILED",
             "failure": "Same-inbox C100 hit unchanged inbox capacity",
             "completed_successful_conditions": 7, "conditions": rows,
             "baseline_binary_sha256": baseline["identity"]["binary_sha256"],
             "candidate_binary_sha256": candidate["identity"]["binary_sha256"],
             "all_input_artifact_manifest_hashes_verified": True,
             "candidate_controls": controls["results"],
             "baseline_controls": "docs/impl/acceptance-20261020/transport-scale/run-01/summary.json",
             "limitations": "One sample per condition on one host; elapsed host conditions can vary. Python driver overhead is included. Explicit partial continuation does not repair the failed eight-condition gate."}
    output.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print("| Profile | Senders | Baseline acks/s | Candidate acks/s | Candidate p50 ms | 5000/s | p50 <10ms |")
    print("|---|---:|---:|---:|---:|---|---|")
    for row in rows:
        if row["candidate_status"] == "FAILED_RUN":
            print(f"| {row['profile']} | {row['concurrent_senders']} | {row['baseline_acks_per_second']:.1f} | FAILED | — | FAILED | FAILED |")
        else:
            rate = "PASS" if row["candidate_throughput_5000_per_second_target_pass"] else "MISS"
            latency = "PASS" if row["candidate_p50_below_10_ms_target_pass"] else "MISS"
            print(f"| {row['profile']} | {row['concurrent_senders']} | {row['baseline_acks_per_second']:.1f} | {row['candidate_acks_per_second']:.1f} | {row['candidate_send_ack_ms']['p50']:.3f} | {rate} | {latency} |")


if __name__ == "__main__":
    main()
