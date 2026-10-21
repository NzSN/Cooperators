#!/usr/bin/env python3
"""Measure delivery to a recipient whose real transport wait is pending.

Uses the unchanged qualified transport harness's daemon/client helpers. The
request-sent witness plus delay and pending-future check is observable from
the client; it does not establish an instrumented server-entry timestamp.
"""

import argparse
import concurrent.futures
import datetime
import json
import os
from pathlib import Path
import platform
import threading
import time

import benchmark_transport as transport


class WakeupRun:
    def __init__(self, arguments):
        self.args = arguments
        self.repository = Path(__file__).resolve().parents[1]
        self.binary, self.output = arguments.binary.resolve(), arguments.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.credentials, self.results = set(), []
        sources = [self.repository / "Cargo.toml", self.repository / "Cargo.lock",
                   *sorted(self.repository.glob("src/**/*.rs"))]
        self.identity = {"start_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                         "binary": str(self.binary), "binary_sha256": transport.sha(self.binary),
                         "git_head": transport.command_output(["git", "rev-parse", "HEAD"], self.repository),
                         "git_status_before": transport.command_output(["git", "status", "--short"], self.repository),
                         "source_sha256": {str(path.relative_to(self.repository)): transport.sha(path) for path in sources},
                         "script_sha256": transport.sha(Path(__file__)),
                         "qualified_helper_sha256": transport.sha(Path(transport.__file__)),
                         "python": platform.python_version(), "platform": platform.platform(),
                         "logical_cpus": os.cpu_count(),
                         "cpu": next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")),
                         "filesystem_mount": transport.command_output(["findmnt", "-T", "/tmp", "-o", "SOURCE,FSTYPE,OPTIONS", "-n"], self.repository)}
        baseline = json.loads((self.repository / "docs/impl/acceptance-20261020/transport-scale/run-01/environment.json").read_text())
        assert self.identity["binary_sha256"] == baseline["binary_sha256"]
        assert self.identity["source_sha256"] == baseline["source_sha256"]
        assert self.identity["qualified_helper_sha256"] == baseline["script_sha256"]
        self.plan = {"schema": "cooperators.transport-wakeup-plan/v1", "samples_per_transport": arguments.samples,
                     "warmups_per_transport": arguments.warmups,
                     "transports": ["uds", "http"], "separate_sender_recipient_connections": True,
                     "recipient_wait_timeout_seconds": 600,
                     "qualification_delay_ms": arguments.qualification_delay_ms,
                     "qualification": "Recipient request sendall/HTTP request completes; wait that delay; assert recipient future is unfinished immediately before sending",
                     "server_wait_entry_witness": False,
                     "server_wait_entry_limitation": "No server instrumentation; request sent plus delayed unfinished future supports a pending client wait but cannot prove exact server wait entry",
                     "send_start_to_delivery_ms": "Sender begins send call until recipient thread returns parsed wait response",
                     "signed_send_ack_to_delivery_ms": "Recipient parsed wait-response timestamp minus sender parsed durable acknowledgment timestamp; negative means delivery observed first",
                     "percentiles": "Nearest rank, warmups excluded; signed values are not clamped",
                     "payload_bytes": 128,
                     "scope": "Actual daemon with default durability; UDS JSON-RPC or initialized streamable HTTP MCP persistent connections; client parsing, Python scheduling and GIL included",
                     "target_interpretation": "Report observed timing against25ms for context; client witness does not qualify an exact production server-wakeup latency target"}
        transport.write_json(self.output / "environment.json", self.identity)
        transport.write_json(self.output / "plan.json", self.plan)

    def redact(self, text):
        for credential in self.credentials:
            text = text.replace(credential, "[REDACTED]")
        return text

    def phase(self, name):
        print(f"START pending-wait-{name}", flush=True)
        with transport.Daemon(self, f"pending-wait-{name}") as daemon:
            sender, sender_agent = daemon.enroll(0, name)
            recipient, recipient_agent = daemon.enroll(1, name)
            # Hooks run after the outgoing request bytes have been passed to
            # the OS. No server behavior or protocol message is changed.
            witness = {}
            if name == "uds":
                original = recipient.begin

                def observed_begin(method, arguments=None):
                    original(method, arguments)
                    if method == "wait_message":
                        witness["sent_ns"] = time.perf_counter_ns()
                        witness["event"].set()

                recipient.begin = observed_begin
            else:
                original = recipient.connection.request

                def observed_request(*args, **kwargs):
                    original(*args, **kwargs)
                    witness["sent_ns"] = time.perf_counter_ns()
                    witness["event"].set()

                recipient.connection.request = observed_request

            def receive():
                batch = recipient.call("wait_message", {"timeout_s": 600})
                return batch, time.perf_counter_ns()

            rows = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                for index in range(self.args.warmups + self.args.samples):
                    warmup = index < self.args.warmups
                    witness["event"] = threading.Event()
                    future = executor.submit(receive)
                    assert witness["event"].wait(timeout=5), "wait request was not sent"
                    request_sent_ns = witness["sent_ns"]
                    time.sleep(self.args.qualification_delay_ms / 1000)
                    qualified_ns = time.perf_counter_ns()
                    pending = not future.done()
                    assert pending, "recipient wait completed before the send"
                    payload = f"wakeup:{name}:{index}".ljust(128, "x")
                    started_ns = time.perf_counter_ns()
                    receipt = sender.call("send_message", {"to": recipient_agent["agent_id"],
                                          "kind": "wakeup-benchmark", "body": {"mime": "text/plain", "text": payload}})
                    acknowledged_ns = time.perf_counter_ns()
                    batch, delivered_ns = future.result(timeout=10)
                    assert not receipt["duplicate"] and not batch["timed_out"] and len(batch["messages"]) == 1
                    message = batch["messages"][0]
                    assert message["id"] == receipt["message_id"] and message["seq"] == receipt["seq"]
                    assert message["from"] == sender_agent["name"] and message["to"] == recipient_agent["name"]
                    assert message["body"] == {"mime": "text/plain", "text": payload}
                    assert batch["next_cursor"] == str(receipt["seq"])
                    assert delivered_ns >= started_ns
                    ack = recipient.call("ack", {"cursor": batch["next_cursor"]})
                    assert ack["acked_cursor"] == batch["next_cursor"]
                    rows.append({"iteration": index, "warmup": warmup, "message_id": receipt["message_id"],
                                 "seq": receipt["seq"], "payload": payload,
                                 "sender_name": sender_agent["name"], "recipient_name": recipient_agent["name"],
                                 "wait_request_sent_ns": request_sent_ns, "qualification_check_ns": qualified_ns,
                                 "observed_wait_pending_before_send": pending,
                                 "server_wait_entry_witness": False,
                                 "qualification_elapsed_ms": (qualified_ns - request_sent_ns) / 1e6,
                                 "send_started_ns": started_ns, "send_acknowledged_ns": acknowledged_ns,
                                 "recipient_delivered_ns": delivered_ns,
                                 "send_ack_ms": (acknowledged_ns - started_ns) / 1e6,
                                 "send_start_to_delivery_ms": (delivered_ns - started_ns) / 1e6,
                                 "signed_send_ack_to_delivery_ms": (delivered_ns - acknowledged_ns) / 1e6,
                                 "delivery_verified": True, "ack_verified": True})

            # Restore instrumentation before the final empty-inbox check.
            if name == "uds":
                recipient.begin = original
            else:
                recipient.connection.request = original
            empty = recipient.call("wait_message", {"timeout_s": 0})
            assert empty["timed_out"] and not empty["messages"]
            assert empty["acked_cursor"] == str(rows[-1]["seq"])
            expected = {row["message_id"]: row for row in rows}
            records = [json.loads(line) for path in daemon.root.glob("agents/*/inbox.jsonl") for line in path.read_text().splitlines()]
            assert len(expected) == len(rows) == len(records)
            assert {record["id"] for record in records} == expected.keys()
            assert [record["seq"] for record in records] == list(range(1, len(rows) + 1))
            for record in records:
                row = expected[record["id"]]
                assert record["seq"] == row["seq"] and record["body"]["text"] == row["payload"]
                assert record["from"] == row["sender_name"] and record["to"] == row["recipient_name"]
            daemon.persist_samples(rows)
            daemon.info.update(agents=daemon.agents, durable_records=len(records), receipt_count=len(rows),
                               contiguous_sequences=True, receipt_payload_and_identity_match=True,
                               delivery_and_ack_verified=True, correctness_passed=True)
            measured = [row for row in rows if not row["warmup"]]
            summary = {"transport": name, "samples": len(measured), "warmups": self.args.warmups,
                       **{key: transport.percentiles(measured, key) for key in (
                           "qualification_elapsed_ms", "send_ack_ms", "send_start_to_delivery_ms", "signed_send_ack_to_delivery_ms")},
                       "negative_signed_ack_to_delivery_samples": sum(row["signed_send_ack_to_delivery_ms"] < 0 for row in measured),
                       "all_wait_requests_sent_and_pending_before_send": all(row["observed_wait_pending_before_send"] for row in rows),
                       "server_wait_entry_witness": False, "correctness_passed": True,
                       "send_start_to_delivery_p50_below25ms_observed": transport.percentiles(measured, "send_start_to_delivery_ms")["p50"] < 25,
                       "exact_production_wakeup_target_qualified": False}
            transport.write_json(daemon.output / "summary.json", summary)
            self.results.append(summary)
            print(f"DONE {name}: send-start→delivery p50 {summary['send_start_to_delivery_ms']['p50']:.3f}ms; signed ack→delivery p50 {summary['signed_send_ack_to_delivery_ms']['p50']:.3f}ms", flush=True)

    def finish(self, error=None):
        source_after = {name: transport.sha(self.repository / name) for name in self.identity["source_sha256"]}
        binary_after = transport.sha(self.binary)
        helper_after = transport.sha(Path(transport.__file__))
        assert source_after == self.identity["source_sha256"]
        assert binary_after == self.identity["binary_sha256"]
        assert helper_after == self.identity["qualified_helper_sha256"]
        report = {"schema": "cooperators.transport-wakeup-results/v1", "completed": error is None,
                  "end_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "identity": self.identity, "plan": self.plan, "results": self.results,
                  "source_sha256_after": source_after, "binary_sha256_after": binary_after,
                  "qualified_helper_sha256_after": helper_after}
        if error:
            report["error"] = self.redact(str(error))
        transport.write_json(self.output / "summary.json", report)
        for path in self.output.rglob("*"):
            if path.is_file():
                data = path.read_bytes()
                assert all(credential.encode() not in data for credential in self.credentials), f"credential leak in {path}"
        transport.write_json(self.output / "credential-check.json", {"credential_plaintext_absent": True,
                             "generated_credentials_checked": len(self.credentials), "check": "all retained regular-file bytes scanned"})
        transport.write_json(self.output / "sha256.json", {str(path.relative_to(self.output)): transport.sha(path)
                             for path in sorted(self.output.rglob("*")) if path.is_file()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/coop"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--qualification-delay-ms", type=float, default=25)
    arguments = parser.parse_args()
    assert arguments.samples > 0 and arguments.warmups > 0 and arguments.qualification_delay_ms > 0
    run = WakeupRun(arguments)
    try:
        run.phase("uds")
        run.phase("http")
    except BaseException as error:
        run.finish(error)
        raise
    else:
        run.finish()


if __name__ == "__main__":
    main()
