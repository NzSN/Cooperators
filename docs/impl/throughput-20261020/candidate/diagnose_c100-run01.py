#!/usr/bin/env python3
"""Trace send, wait and ack calls during the frozen C100 workload.

This is a separate diagnostic experiment, not replacement qualification data.
The frozen workload functions run unchanged. Instrumentation adds in-memory
per-call tracing, retained even when that workload fails. Nothing is measured
until this script is explicitly invoked.
"""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import sys
import threading
import time


REPOSITORY = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPOSITORY / "scripts"))
import benchmark_throughput as benchmark
import benchmark_transport as transport


class Calls:
    def __init__(self):
        self.original = transport.Uds.call
        self.connections = {}
        self.agents_by_token = {}
        self.lock = threading.Lock()
        self.live = {"successful_send_receipts": 0, "returned_messages": 0,
                     "successful_ack_calls": 0, "highest_successful_ack_cursor": 0}
        self.first_error_observer = None
        self.daemon_pid = None
        self.resource_samples = []
        self.resource_lock = threading.Lock()

    def sample_resources(self):
        sample = {"monotonic_ns": time.perf_counter_ns(), "live_receiver_observer": dict(self.live),
                  "processes": {}}
        for role, pid in (("client", os.getpid()), ("daemon", self.daemon_pid)):
            if pid is None:
                continue
            try:
                process = Path(f"/proc/{pid}")
                fields = (process / "stat").read_text().rsplit(") ", 1)[1].split()
                io = dict(line.split(":", 1) for line in (process / "io").read_text().splitlines())
                sample["processes"][role] = {
                    "pid": pid, "user_cpu_ticks": int(fields[11]),
                    "system_cpu_ticks": int(fields[12]), "rss_pages": int(fields[21]),
                    "io": {key: int(value) for key, value in io.items()}}
            except (FileNotFoundError, ProcessLookupError):
                pass
        with self.resource_lock:
            self.resource_samples.append(sample)

    def wrap(self, client, method, arguments=None):
        with self.lock:
            connection = self.connections.setdefault(client, {
                "connection": len(self.connections), "calls": []})
        call = {"method": method, "started_monotonic_ns": time.perf_counter_ns()}
        connection["calls"].append(call)
        if method == "ack":
            call["requested_cursor"] = arguments["cursor"]
        if method in ("wait_message", "ack"):
            self.live["current_receiver_call"] = {"method": method,
                                                   "started_monotonic_ns": call["started_monotonic_ns"]}
        try:
            result = self.original(client, method, arguments)
        except BaseException as error:
            call.update(completed_monotonic_ns=time.perf_counter_ns(), error=str(error))
            if method == "send_message" and self.first_error_observer is None:
                self.first_error_observer = {"observed_monotonic_ns": time.perf_counter_ns(),
                                             **self.live}
            raise
        call["completed_monotonic_ns"] = time.perf_counter_ns()
        if method == "enroll_agent":
            self.agents_by_token[result["token"]] = {
                key: result[key] for key in ("agent_id", "name", "display_name")}
            connection["agent"] = self.agents_by_token[result["token"]]
        elif method == "send_message":
            self.live["successful_send_receipts"] += 1
            call.update(message_id=result["message_id"], seq=result["seq"],
                        duplicate=result["duplicate"], receiver_id=result["to_agent_id"])
        elif method == "wait_message":
            self.live["returned_messages"] += len(result["messages"])
            call.update(cursor=result["cursor"], acked_cursor=result["acked_cursor"],
                        next_cursor=result["next_cursor"], timed_out=result["timed_out"],
                        returned_messages=len(result["messages"]))
            if result["messages"]:
                call["first_seq"] = result["messages"][0]["seq"]
                call["last_seq"] = result["messages"][-1]["seq"]
        elif method == "ack":
            self.live["successful_ack_calls"] += 1
            self.live["highest_successful_ack_cursor"] = int(result["acked_cursor"])
            call["acked_cursor"] = result["acked_cursor"]
        return result

    def persist(self, run):
        connections = []
        all_calls = []
        for client, connection in self.connections.items():
            agent = connection.get("agent") or self.agents_by_token.get(client.token)
            safe = {"connection": connection["connection"], "agent": agent}
            calls = connection["calls"]
            for call in calls:
                call.update(connection=connection["connection"], agent=agent)
                if "error" in call:
                    call["error"] = run.redact(call["error"])
                call["elapsed_ms"] = (call["completed_monotonic_ns"] - call["started_monotonic_ns"]) / 1e6
                all_calls.append(call)
            methods = {}
            for method in sorted({call["method"] for call in calls}):
                matching = [call for call in calls if call["method"] == method]
                methods[method] = {"calls": len(matching),
                                   "successful": sum("error" not in call for call in matching),
                                   "latency_ms": transport.percentiles(matching, "elapsed_ms")}
            waits = [call for call in calls if call["method"] == "wait_message" and "error" not in call]
            acks = [call for call in calls if call["method"] == "ack" and "error" not in call]
            safe.update(methods=methods, returned_messages=sum(call["returned_messages"] for call in waits),
                        highest_successful_ack_cursor=max((int(call["acked_cursor"]) for call in acks), default=0),
                        nonempty_wait_batch_sizes=dict(Counter(call["returned_messages"] for call in waits
                                                              if call["returned_messages"])))
            connections.append(safe)
        all_calls.sort(key=lambda call: call["started_monotonic_ns"])
        with (run.output / "rpc-timeline.jsonl").open("w") as stream:
            for call in all_calls:
                stream.write(json.dumps(call, sort_keys=True) + "\n")
        sends = [call for call in all_calls if call["method"] == "send_message"]
        successful_sends = [call for call in sends if "error" not in call]
        error_sends = [call for call in sends if "error" in call]
        wait_completions = [call for call in all_calls if call["method"] == "wait_message" and "error" not in call]
        ack_completions = [call for call in all_calls if call["method"] == "ack" and "error" not in call]
        first_error = min(error_sends, key=lambda call: call["completed_monotonic_ns"], default=None)
        at_error = {}
        if first_error:
            instant = first_error["completed_monotonic_ns"]
            prior_sends = [call for call in successful_sends if call["completed_monotonic_ns"] <= instant]
            prior_waits = [call for call in wait_completions if call["completed_monotonic_ns"] <= instant]
            prior_acks = [call for call in ack_completions if call["completed_monotonic_ns"] <= instant]
            at_error = {"first_error": first_error,
                        "successful_send_receipts_completed": len(prior_sends),
                        "highest_successful_send_receipt_seq": max((call["seq"] for call in prior_sends), default=0),
                        "returned_messages": sum(call["returned_messages"] for call in prior_waits),
                        "successful_ack_calls": len(prior_acks),
                        "highest_successful_ack_cursor": max((int(call["acked_cursor"]) for call in prior_acks), default=0)}
        transport.write_json(run.output / "rpc-diagnostics.json", {
            "experiment": "Separate diagnostic reproduction of frozen same-inbox C100 workload, with in-memory RPC tracing overhead",
            "frozen_harness_sha256": transport.sha(Path(benchmark.__file__)),
            "diagnostic_wrapper_sha256": transport.sha(Path(__file__)),
            "successful_send_receipts": len(successful_sends), "failed_send_calls": len(error_sends),
            "returned_messages": sum(call["returned_messages"] for call in wait_completions),
            "successful_ack_calls": len(ack_completions),
            "highest_successful_ack_cursor": max((int(call["acked_cursor"]) for call in ack_completions), default=0),
            "first_send_error_snapshot": at_error,
            "first_send_error_live_observer": self.first_error_observer, "connections": connections})
        self.resource_samples.sort(key=lambda sample: sample["monotonic_ns"])
        for previous, sample in zip(self.resource_samples, self.resource_samples[1:]):
            elapsed = (sample["monotonic_ns"] - previous["monotonic_ns"]) / 1e9
            for role, process in sample["processes"].items():
                prior = previous["processes"].get(role)
                if prior and process["pid"] == prior["pid"] and elapsed:
                    ticks = sum(process[key] - prior[key] for key in ("user_cpu_ticks", "system_cpu_ticks"))
                    process["delta"] = {"elapsed_seconds": elapsed,
                                        "cpu_percent_of_one_logical_cpu": 100 * ticks / os.sysconf("SC_CLK_TCK") / elapsed,
                                        "io": {key: value - prior["io"][key] for key, value in process["io"].items()}}
        transport.write_json(run.output / "process-resources.json", {
            "sample_interval_seconds": 0.5, "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
            "page_bytes": os.sysconf("SC_PAGE_SIZE"), "samples": self.resource_samples})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=REPOSITORY / "target/release/coop")
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    arguments.label = "diagnostic-candidate"
    arguments.source_manifest = None
    arguments.seconds = 10.0
    arguments.warmup_seconds = 1.0
    arguments.warmup_minimum = 3
    arguments.drain_interval_ms = 20.0
    arguments.profiles = ["same-inbox"]
    arguments.concurrencies = [100]
    calls = Calls()
    transport.Uds.call = lambda client, method, params=None: calls.wrap(client, method, params)
    original_enter = benchmark.ThroughputDaemon.__enter__
    original_exit = benchmark.ThroughputDaemon.__exit__

    def entered(daemon):
        result = original_enter(daemon)
        calls.daemon_pid = daemon.child.pid
        calls.sample_resources()
        return result

    def exiting(daemon, *error):
        calls.sample_resources()
        return original_exit(daemon, *error)

    benchmark.ThroughputDaemon.__enter__ = entered
    benchmark.ThroughputDaemon.__exit__ = exiting
    run = benchmark.ThroughputRun(arguments)
    stop_sampling = threading.Event()

    def sample():
        while not stop_sampling.wait(0.5):
            calls.sample_resources()

    sampler = threading.Thread(target=sample)
    calls.sample_resources()
    sampler.start()
    try:
        run.execute()
    except BaseException as error:
        stop_sampling.set()
        sampler.join()
        calls.sample_resources()
        calls.persist(run)
        run.finish(error)
        raise
    else:
        stop_sampling.set()
        sampler.join()
        calls.sample_resources()
        calls.persist(run)
        run.finish()


if __name__ == "__main__":
    main()
