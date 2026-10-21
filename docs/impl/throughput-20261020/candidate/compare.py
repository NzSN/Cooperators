#!/usr/bin/env python3
"""Validate paired throughput artifacts and emit their before/after comparison.

Run only after both measurement processes have completed. This does not start
daemons, alter either run, or infer a passing target from relative improvement.
"""

import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def load_run(directory):
    manifest = json.loads((directory / "sha256.json").read_text())
    for name, expected in manifest.items():
        artifact = directory / name
        assert artifact.is_file(), f"Missing artifact: {artifact}"
        assert sha(artifact) == expected, f"Artifact changed: {artifact}"
    result = json.loads((directory / "summary.json").read_text())
    assert result["completed"], f"Incomplete run: {directory}"
    assert all(row["correctness_passed"] for row in result["results"])
    check = json.loads((directory / "credential-check.json").read_text())
    assert check["credential_plaintext_absent"]
    return result, sha(directory / "sha256.json")


def compare(baseline_directory, candidate_directory, declared_plan):
    baseline, baseline_manifest = load_run(baseline_directory)
    candidate, candidate_manifest = load_run(candidate_directory)
    plan = baseline["plan"]
    assert plan == candidate["plan"], "Before/after workloads differ"
    for key, value in plan.items():
        assert value == declared_plan[key], f"Workload differs from predeclared plan: {key}"
    for field, declared in (("script_sha256", "harness_sha256"),
                            ("unchanged_transport_helper_sha256", "unchanged_transport_helper_sha256")):
        assert baseline["identity"][field] == candidate["identity"][field] == declared_plan[declared]
    assert baseline["identity"]["binary_sha256"] == declared_plan["baseline_binary_sha256"]
    assert baseline["identity"]["executable_source_identity"]["manifest_sha256"] == declared_plan["baseline_source_manifest_sha256"]

    expected = [(profile, concurrency) for profile in plan["profiles"]
                for concurrency in plan["sender_concurrencies"]]

    def index(run):
        result = {(row["profile"], row["active_sender_agents"]): row for row in run["results"]}
        assert len(result) == len(run["results"]), "Duplicate conditions"
        assert set(result) == set(expected), "Missing or extra conditions"
        return result

    before, after = index(baseline), index(candidate)
    comparisons = []
    for condition in expected:
        old, new = before[condition], after[condition]
        for value in (old, new):
            assert value["outstanding_sends_per_sender"] == plan["outstanding_sends_per_sender"]
            assert value["requested_measurement_seconds"] == plan["measurement_seconds"]
            assert value["throughput_5000_per_second_target_pass"] == (value["durable_acknowledgments_per_second"] >= 5000)
            assert value["send_ack_p50_below_10_ms_target_pass"] == (value["send_ack_ms"]["p50"] < 10)
        comparisons.append({
            "profile": condition[0], "concurrent_senders": condition[1],
            "enrolled_agents": new["enrolled_agents"],
            "baseline": {"durable_acknowledgments_per_second": old["durable_acknowledgments_per_second"],
                         "send_ack_ms": old["send_ack_ms"],
                         "measured_messages": old["measured_messages"], "elapsed_seconds": old["elapsed_seconds"]},
            "candidate": {"durable_acknowledgments_per_second": new["durable_acknowledgments_per_second"],
                          "send_ack_ms": new["send_ack_ms"],
                          "measured_messages": new["measured_messages"], "elapsed_seconds": new["elapsed_seconds"]},
            "throughput_candidate_divided_by_baseline": new["durable_acknowledgments_per_second"] / old["durable_acknowledgments_per_second"],
            "send_ack_candidate_divided_by_baseline": {
                percentile: new["send_ack_ms"][percentile] / old["send_ack_ms"][percentile]
                for percentile in ("p50", "p95", "p99")},
            "candidate_throughput_5000_per_second_target_pass": new["throughput_5000_per_second_target_pass"],
            "candidate_send_ack_p50_below_10_ms_target_pass": new["send_ack_p50_below_10_ms_target_pass"],
        })
    return {
        "schema": "cooperators.concurrent-throughput-comparison/v1",
        "workloads_and_harness_identical": True,
        "all_retained_artifact_hashes_verified": True,
        "baseline": {"directory": str(baseline_directory), "identity": baseline["identity"],
                     "artifact_manifest_sha256": baseline_manifest},
        "candidate": {"directory": str(candidate_directory), "identity": candidate["identity"],
                      "artifact_manifest_sha256": candidate_manifest},
        "comparisons": comparisons,
        "candidate_all_throughput_targets_pass": all(row["candidate_throughput_5000_per_second_target_pass"] for row in comparisons),
        "candidate_all_p50_latency_targets_pass": all(row["candidate_send_ack_p50_below_10_ms_target_pass"] for row in comparisons),
        "verified_messages_including_warmup": {
            "baseline": sum(row["verified_deliveries_and_acks"] for row in baseline["results"]),
            "candidate": sum(row["verified_deliveries_and_acks"] for row in candidate["results"])},
        "interpretation": "Relative improvements and absolute 5000/s and 10 ms comparisons are reported separately for every exact workload; one sample per condition provides no confidence interval.",
    }


def main():
    evidence = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, default=evidence / "baseline/throughput")
    parser.add_argument("--candidate", type=Path, default=evidence / "candidate/throughput")
    parser.add_argument("--plan", type=Path, default=evidence / "plan.json")
    parser.add_argument("--output", type=Path, default=evidence / "candidate/comparison.json")
    arguments = parser.parse_args()
    assert not arguments.output.exists(), "Comparison output already exists"
    result = compare(arguments.baseline, arguments.candidate,
                     json.loads(arguments.plan.read_text()))
    arguments.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print("| Topology | Senders | Baseline acks/s | Candidate acks/s | Speedup | Candidate send p50/p95/p99 ms | 5000/s | p50 <10ms |")
    print("|---|---:|---:|---:|---:|---|---|---|")
    for row in result["comparisons"]:
        old, new = row["baseline"], row["candidate"]
        quantiles = "/".join(f"{new['send_ack_ms'][percentile]:.3f}" for percentile in ("p50", "p95", "p99"))
        rate = "PASS" if row["candidate_throughput_5000_per_second_target_pass"] else "MISS"
        latency = "PASS" if row["candidate_send_ack_p50_below_10_ms_target_pass"] else "MISS"
        print(f"| {row['profile']} | {row['concurrent_senders']} | {old['durable_acknowledgments_per_second']:.1f} | {new['durable_acknowledgments_per_second']:.1f} | {row['throughput_candidate_divided_by_baseline']:.2f}x | {quantiles} | {rate} | {latency} |")


if __name__ == "__main__":
    main()
