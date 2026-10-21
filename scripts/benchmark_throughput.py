#!/usr/bin/env python3
"""Compare durable UDS append throughput using fixed concurrent workloads.

Only the Python standard library is needed. The unchanged transport benchmark
supplies its actual UDS client, private daemon lifecycle, and artifact helpers.
Run the same command options against the baseline and candidate executables.
"""

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import threading
import time

import benchmark_transport as transport


PROFILES = ("same-inbox", "distributed-inboxes")
CONCURRENCIES = (1, 8, 32, 100)
GENESIS = "sha256:" + "0" * 64


def benchmark_plan(seconds=10.0, warmup_seconds=1.0, warmup_minimum=3,
                   drain_interval_ms=20.0, profiles=PROFILES,
                   concurrencies=CONCURRENCIES):
    return {
        "schema": "cooperators.concurrent-throughput-plan/v1",
        "transport": "persistent AF_UNIX NDJSON JSON-RPC via benchmark_transport.Uds",
        "payload": {"mime": "text/plain", "text_bytes": 128, "text": transport.PAYLOAD},
        "profiles": list(profiles),
        "sender_concurrencies": list(concurrencies),
        "outstanding_sends_per_sender": 1,
        "same_inbox": "C enrolled sender sessions send to one additional receiver session",
        "distributed_inboxes": "C enrolled sender sessions self-send; one additional read/ack connection per same agent identity, so C=100 is 100 enrolled agents",
        "warmup_seconds": warmup_seconds,
        "warmup_minimum_sends_per_sender": warmup_minimum,
        "measurement_seconds": seconds,
        "scheduling": "Closed loop: each sender waits for its durable receipt before issuing its next send; a barrier starts each window",
        "rate_denominator": "Wall time from sender barrier release through the last receipt for a send admitted before the measurement deadline",
        "rate_numerator": "Every successful durable send acknowledgment for requests admitted during that window, including completions after its deadline",
        "receiver_drain": {
            "connections": "One independent receiver connection and drain thread per destination inbox",
            "interval_ms": drain_interval_ms,
            "operation": "Every interval, repeatedly wait_message(timeout_s=0) and ack each returned default-size batch until empty",
            "daemon_batch_limit": "Unchanged daemon default, 100 messages",
            "accounting": "Concurrent drain, verification, and ack overhead remain inside the measured send workload; final catch-up is outside its window",
            "capacity": "All warmups are delivered and acked before measurement; drain continuously during measurement to avoid the unchanged 10000-message inbox cap",
        },
        "targets_declared_before_measurement": {
            "durable_acknowledgments_per_second_gte": 5000,
            "application": "Report 5000/s independently for every topology and concurrency; no aggregate or best-case substitution",
            "send_ack_p50_ms_lt": 10,
            "100_active_senders": "Apply and report the absolute 10 ms p50 comparison at C=100 with this workload explicitly named; no numeric definition of without degradation is inferred",
            "unloaded_idle_controls": "Run unchanged benchmark_transport.py with defaults against each executable and retain its serial append, unloaded UDS/HTTP, one-active/100-idle, and 100-active cycle controls",
        },
        "quantiles": "Nearest rank p50/p95/p99 of client call latency in milliseconds, excluding warmups",
        "correctness": [
            "Every receipt must be unique, non-duplicate, positive-sequence, and identify the expected receiver",
            "Every delivered envelope must match its receipt ID/sequence, sender, receiver, kind, MIME and all 128 payload bytes",
            "Every batch cursor and acknowledgment must advance through contiguous delivered sequences",
            "Every receipt including warmup must match exactly one persisted inbox record; reject extra records, sequence gaps, and broken SHA-256 chain links",
            "All receivers are empty and their acknowledged cursors equal the verified heads before shutdown",
            "Running /proc/PID/exe hash must match the selected executable; source/helper/binary hashes must remain stable throughout the run",
        ],
        "limitations": [
            "One host and filesystem; one 10-second sample per condition does not establish confidence intervals",
            "The Python GIL, client parsing and thread scheduling, plus concurrent receiver maintenance, are included",
            "A single sender with one outstanding request provides no overlap for group commit",
            "Fixed 128-byte payload and closed-loop request arrival; this does not establish a maximum externally paced arrival rate",
            "Readback of persisted records checks contents and integrity; crash-durability claims require separate failure-injection evidence",
            "Fresh daemon and bus root per condition; warmup is excluded and enrollment is outside timing",
        ],
    }


class ThroughputDaemon(transport.Daemon):
    def validate_throughput_disk(self, rows, deliveries):
        expected = {row["message_id"]: row for row in rows}
        assert len(expected) == len(rows), "duplicate receipt ids"
        received = {row["message_id"]: row for row in deliveries}
        assert len(received) == len(deliveries), "duplicate deliveries"
        assert received.keys() == expected.keys(), "delivery ids differ from receipt ids"
        for message_id, row in expected.items():
            delivery = received[message_id]
            assert all(delivery[key] == row[key] for key in
                       ("seq", "sender_name", "receiver_name", "receiver_id"))
            row.update(delivery_verified=True, ack_verified=True)

        actual = set()
        inbox_heads = {}
        for path in sorted(self.root.glob("agents/*/inbox.jsonl")):
            # Inboxes currently do not rotate, but retain support for the common
            # storage segment layout so disk validation cannot silently omit it.
            paths = [*sorted((path.parent / ".segments" / path.name).glob("*.jsonl")), path]
            sequence, previous = 0, GENESIS
            for segment in paths:
                with segment.open("rb") as stream:
                    for line in stream:
                        assert line.endswith(b"\n"), "unterminated persisted record"
                        record = json.loads(line)
                        sequence += 1
                        assert record["seq"] == sequence, "noncontiguous persisted sequence"
                        assert record["prev_hash"] == previous, "broken persisted hash chain"
                        chain_value = {key: value for key, value in record.items() if key != "prev_hash"}
                        canonical = json.dumps(chain_value, sort_keys=True, separators=(",", ":"),
                                               ensure_ascii=False).encode()
                        previous = "sha256:" + hashlib.sha256(canonical).hexdigest()
                        message_id = record["id"]
                        assert message_id not in actual, "duplicate persisted message id"
                        actual.add(message_id)
                        row = expected[message_id]
                        assert record["seq"] == row["seq"]
                        assert record["from"] == row["sender_name"]
                        assert record["to"] == row["receiver_name"]
                        assert path.parent.name == row["receiver_id"]
                        assert record["kind"] == "benchmark"
                        assert record["body"] == {"mime": "text/plain", "text": transport.PAYLOAD}
            inbox_heads[path.parent.name] = sequence
        assert actual == expected.keys(), "persisted ids differ from receipt ids"
        self.info.update(agents=self.agents, durable_records=len(actual), receipt_count=len(rows),
                         verified_deliveries_and_acks=len(deliveries), unique_receipts=len(expected),
                         contiguous_sequences=True, hash_chains_verified=True,
                         receipt_payload_and_identity_match=True, inbox_heads=inbox_heads,
                         correctness_passed=True)


class ThroughputRun:
    def __init__(self, arguments):
        self.args = arguments
        self.repository = Path(__file__).resolve().parents[1]
        self.binary, self.output = arguments.binary.resolve(), arguments.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.credentials = set()
        self.results = []
        self.plan = benchmark_plan(arguments.seconds, arguments.warmup_seconds,
                                   arguments.warmup_minimum, arguments.drain_interval_ms,
                                   arguments.profiles, arguments.concurrencies)
        source_paths = [self.repository / "Cargo.toml", self.repository / "Cargo.lock",
                        *sorted(self.repository.glob("src/**/*.rs"))]
        cpu = next((line.split(":", 1)[1].strip()
                    for line in Path("/proc/cpuinfo").read_text().splitlines()
                    if line.startswith("model name")), "unknown")
        observed_sources = {str(path.relative_to(self.repository)): transport.sha(path)
                            for path in source_paths}
        source_identity = {"source_sha256": observed_sources,
                           "basis": "Current observed worktree; candidate release built from these sources separately"}
        if arguments.source_manifest:
            source_manifest = json.loads(arguments.source_manifest.read_text())
            assert source_manifest["release_binary"]["sha256"] == transport.sha(self.binary)
            source_identity = {
                "source_sha256": source_manifest["source_files"],
                "basis": "Original retained build/source evidence; current worktree is not attributed to this executable",
                "base_commit": source_manifest["base_commit"],
                "manifest": str(arguments.source_manifest.resolve()),
                "manifest_sha256": transport.sha(arguments.source_manifest),
            }
        self.identity = {
            "label": arguments.label,
            "start_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "git_head": transport.command_output(["git", "rev-parse", "HEAD"], self.repository),
            "git_status_before": transport.command_output(["git", "status", "--short"], self.repository),
            "binary": str(self.binary), "binary_sha256": transport.sha(self.binary),
            "executable_source_identity": source_identity,
            "observed_worktree_source_sha256": observed_sources,
            "script_sha256": transport.sha(Path(__file__)),
            "unchanged_transport_helper_sha256": transport.sha(Path(transport.__file__)),
            "python": platform.python_version(), "platform": platform.platform(),
            "cpu": cpu, "logical_cpus": os.cpu_count(),
            "filesystem_tmp": transport.command_output(["stat", "-f", "-c", "%T", "/tmp"], self.repository),
            "filesystem_mount": transport.command_output(["findmnt", "-T", "/tmp", "-o", "SOURCE,FSTYPE,OPTIONS", "-n"], self.repository),
        }
        transport.write_json(self.output / "plan.json", self.plan)
        transport.write_json(self.output / "environment.json", self.identity)

    def redact(self, value):
        for credential in self.credentials:
            value = value.replace(credential, "[REDACTED]")
        return value

    def phase(self, profile, concurrency):
        label = f"{profile}-c{concurrency:03d}"
        print(f"START {self.args.label}/{label}: {self.args.seconds:g}s, 128-byte text, 1 outstanding/sender", flush=True)
        with ThroughputDaemon(self, label) as daemon:
            senders = [daemon.enroll(index, "uds") for index in range(concurrency)]
            receiver_count = 1 if profile == "same-inbox" else concurrency
            if profile == "same-inbox":
                receivers = [daemon.enroll(concurrency, "uds")]
            else:
                receivers = [(daemon.uds(client.token), agent) for client, agent in senders]
            stop = threading.Event()
            failed = threading.Event()
            deliveries = [[] for _ in receivers]
            acked_heads = [0 for _ in receivers]
            drain_stats = [{"wait_calls": 0, "ack_calls": 0, "max_batch": 0} for _ in receivers]
            drain_interval = self.args.drain_interval_ms / 1000

            def drain(index):
                client, agent = receivers[index]
                try:
                    while not stop.is_set():
                        while True:
                            batch = client.call("wait_message", {"timeout_s": 0})
                            drain_stats[index]["wait_calls"] += 1
                            assert batch["cursor"] == str(acked_heads[index])
                            assert batch["acked_cursor"] == str(acked_heads[index])
                            messages = batch["messages"]
                            if not messages:
                                assert batch["timed_out"]
                                assert batch["next_cursor"] == str(acked_heads[index])
                                break
                            assert not batch["timed_out"]
                            drain_stats[index]["max_batch"] = max(drain_stats[index]["max_batch"], len(messages))
                            next_sequence = acked_heads[index]
                            batch_rows = []
                            for message in messages:
                                next_sequence += 1
                                assert message["seq"] == next_sequence
                                assert message["to"] == agent["name"]
                                assert message["kind"] == "benchmark"
                                assert message["body"] == {"mime": "text/plain", "text": transport.PAYLOAD}
                                batch_rows.append({"message_id": message["id"], "seq": message["seq"],
                                                   "sender_name": message["from"], "receiver_name": agent["name"],
                                                   "receiver_id": agent["agent_id"]})
                            assert batch["next_cursor"] == str(next_sequence)
                            ack = client.call("ack", {"cursor": batch["next_cursor"]})
                            drain_stats[index]["ack_calls"] += 1
                            assert ack["acked_cursor"] == str(next_sequence)
                            deliveries[index].extend(batch_rows)
                            acked_heads[index] = next_sequence
                        stop.wait(drain_interval)
                except BaseException:
                    failed.set()
                    raise

            def wait_caught_up(expected):
                deadline = time.monotonic() + 30
                while sum(map(len, deliveries)) < expected:
                    assert not failed.is_set(), "receiver failed"
                    assert time.monotonic() < deadline, "receiver catch-up deadline expired"
                    time.sleep(0.002)
                assert sum(map(len, deliveries)) == expected

            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency + receiver_count) as pool:
                drains = [pool.submit(drain, index) for index in range(receiver_count)]
                all_rows = []

                def window(warmup):
                    ready = threading.Barrier(concurrency + 1)
                    release = threading.Barrier(concurrency + 1)
                    duration = self.args.warmup_seconds if warmup else self.args.seconds
                    deadline_ns = 0

                    def sender(index):
                        client, agent = senders[index]
                        receiver = receivers[0 if profile == "same-inbox" else index][1]
                        rows = []
                        ready.wait(timeout=30)
                        release.wait(timeout=30)
                        while time.perf_counter_ns() < deadline_ns or (warmup and len(rows) < self.args.warmup_minimum):
                            assert not failed.is_set(), "receiver failed during send window"
                            started = time.perf_counter_ns()
                            receipt = client.call("send_message", {
                                "to": receiver["agent_id"], "kind": "benchmark",
                                "body": {"mime": "text/plain", "text": transport.PAYLOAD}})
                            ended = time.perf_counter_ns()
                            assert not receipt["duplicate"] and receipt["seq"] > 0
                            assert receipt["to"] == receiver["name"]
                            assert receipt["to_agent_id"] == receiver["agent_id"]
                            rows.append({"message_id": receipt["message_id"], "seq": receipt["seq"],
                                         "sender_id": agent["agent_id"], "sender_name": agent["name"],
                                         "receiver_id": receiver["agent_id"], "receiver_name": receiver["name"],
                                         "iteration": len(rows), "warmup": warmup,
                                         "started_monotonic_ns": started, "completed_monotonic_ns": ended,
                                         "send_ack_ms": (ended - started) / 1e6})
                        return rows

                    sends = [pool.submit(sender, index) for index in range(concurrency)]
                    ready.wait(timeout=30)
                    started_ns = time.perf_counter_ns()
                    deadline_ns = started_ns + int(duration * 1e9)
                    release.wait(timeout=30)
                    rows = [row for future in sends for row in future.result(timeout=60)]
                    assert rows, "empty send window"
                    ended_ns = max(row["completed_monotonic_ns"] for row in rows)
                    all_rows.extend(rows)
                    wait_caught_up(len(all_rows))
                    return rows, (ended_ns - started_ns) / 1e9

                try:
                    warmups, _ = window(True)
                    daemon.info["warmup_fully_delivered_and_acked_before_measurement"] = len(warmups)
                    measured, elapsed = window(False)
                finally:
                    stop.set()
                    for future in drains:
                        future.result(timeout=35)

            for index, (client, _) in enumerate(receivers):
                empty = client.call("wait_message", {"timeout_s": 0})
                assert not empty["messages"] and empty["timed_out"]
                assert empty["acked_cursor"] == str(acked_heads[index])
            delivered_rows = [row for receiver_rows in deliveries for row in receiver_rows]
            daemon.validate_throughput_disk(all_rows, delivered_rows)
            daemon.persist_samples(sorted(all_rows, key=lambda row: row["started_monotonic_ns"]))
            rate = len(measured) / elapsed
            latencies = transport.percentiles(measured, "send_ack_ms")
            summary = {
                "label": label, "profile": profile, "transport": "uds",
                "active_sender_agents": concurrency, "active_receiver_agents": receiver_count,
                "enrolled_agents": len(daemon.agents),
                "receiver_identity_shared_with_sender": profile == "distributed-inboxes",
                "outstanding_sends_per_sender": 1,
                "warmup_messages": len(warmups), "measured_messages": len(measured),
                "requested_measurement_seconds": self.args.seconds, "elapsed_seconds": elapsed,
                "durable_acknowledgments_per_second": rate, "send_ack_ms": latencies,
                "throughput_5000_per_second_target_pass": rate >= 5000,
                "send_ack_p50_below_10_ms_target_pass": latencies["p50"] < 10,
                "receiver_drain": drain_stats,
                "verified_deliveries_and_acks": len(delivered_rows), "correctness_passed": True,
            }
            daemon.info["all_receiver_inboxes_empty_with_verified_acked_heads"] = True
            transport.write_json(daemon.output / "summary.json", summary)
            self.results.append(summary)
        print(f"DONE {self.args.label}/{label}: {rate:.1f} durable acks/s; send p50/p95/p99 {latencies['p50']:.3f}/{latencies['p95']:.3f}/{latencies['p99']:.3f} ms", flush=True)

    def execute(self):
        for profile in self.args.profiles:
            for concurrency in self.args.concurrencies:
                self.phase(profile, concurrency)

    def finish(self, error=None):
        report = {"schema": "cooperators.concurrent-throughput-results/v1", "completed": error is None,
                  "end_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "identity": self.identity, "plan": self.plan, "results": self.results,
                  "binary_sha256_after": transport.sha(self.binary),
                  "script_sha256_after": transport.sha(Path(__file__)),
                  "unchanged_transport_helper_sha256_after": transport.sha(Path(transport.__file__)),
                  "observed_worktree_source_sha256_after": {
                      name: transport.sha(self.repository / name)
                      for name in self.identity["observed_worktree_source_sha256"]}}
        assert report["binary_sha256_after"] == self.identity["binary_sha256"]
        assert report["script_sha256_after"] == self.identity["script_sha256"]
        assert report["unchanged_transport_helper_sha256_after"] == self.identity["unchanged_transport_helper_sha256"]
        if self.args.source_manifest:
            report["pinned_source_manifest_sha256_after"] = transport.sha(self.args.source_manifest)
            assert report["pinned_source_manifest_sha256_after"] == self.identity["executable_source_identity"]["manifest_sha256"]
        elif self.args.label != "functional-smoke":
            assert report["observed_worktree_source_sha256_after"] == self.identity["observed_worktree_source_sha256"]
        if error:
            report["error"] = self.redact(str(error))
        transport.write_json(self.output / "summary.json", report)

        # Tokens and launch keys are 32 lowercase hex bytes. Find all such
        # substrings, including inside longer hex runs, and compare with the
        # in-memory secret set. This checks every retained byte without scanning
        # every large log once per generated credential.
        credentials = {credential.encode() for credential in self.credentials}
        assert all(re.fullmatch(rb"[0-9a-f]{32}", credential) for credential in credentials)
        for path in self.output.rglob("*"):
            if path.is_file():
                for match in re.finditer(rb"[0-9a-f]{32,}", path.read_bytes()):
                    value = match.group(0)
                    assert not any(value[index:index + 32] in credentials
                                   for index in range(len(value) - 31)), f"credential leak in {path}"
        transport.write_json(self.output / "credential-check.json", {
            "credential_plaintext_absent": True, "generated_credentials_checked": len(credentials),
            "check": "All retained regular-file bytes scanned for every generated 32-byte lowercase-hex credential, including substrings of longer runs"})
        transport.write_json(self.output / "sha256.json", {
            str(path.relative_to(self.output)): transport.sha(path)
            for path in sorted(self.output.rglob("*")) if path.is_file()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/coop"))
    parser.add_argument("--output", type=Path, required=True, help="new evidence directory")
    parser.add_argument("--label", required=True, choices=("baseline", "candidate", "functional-smoke"))
    parser.add_argument("--source-manifest", type=Path,
                        help="Original retained source-manifest.json pinning this executable's source hashes")
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--warmup-seconds", type=float, default=1)
    parser.add_argument("--warmup-minimum", type=int, default=3)
    parser.add_argument("--drain-interval-ms", type=float, default=20)
    parser.add_argument("--profiles", choices=PROFILES, nargs="+", default=list(PROFILES))
    parser.add_argument("--concurrencies", type=int, nargs="+", default=list(CONCURRENCIES))
    arguments = parser.parse_args()
    if arguments.label == "baseline":
        assert arguments.source_manifest, "baseline executable must be pinned to its original source manifest"
    assert min(arguments.seconds, arguments.warmup_seconds, arguments.warmup_minimum,
               arguments.drain_interval_ms, *arguments.concurrencies) > 0
    if arguments.label != "functional-smoke":
        assert arguments.seconds >= 10, "qualification windows must be at least 10 seconds"
        assert arguments.warmup_seconds >= 1
        assert arguments.warmup_minimum >= 3
        assert arguments.profiles == list(PROFILES)
        assert arguments.concurrencies == list(CONCURRENCIES)
        assert arguments.drain_interval_ms == 20
    run = ThroughputRun(arguments)
    try:
        run.execute()
    except BaseException as error:
        run.finish(error)
        raise
    else:
        run.finish()


if __name__ == "__main__":
    main()
