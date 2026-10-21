#!/usr/bin/env python3
"""C100 diagnostic changing only fresh UDS reader buffering to 256 KiB.

The frozen benchmark and the existing RPC/CPU diagnostic wrapper remain
unchanged. All fresh UDS readers use the larger buffer; request framing,
payloads, topology, client concurrency, drain batches/cadence and caps remain
the same. This is a diagnostic experiment, not qualification replacement.
"""

import importlib.util
import json
from pathlib import Path


EVIDENCE = Path(__file__).resolve().parents[1]
REPOSITORY = Path(__file__).resolve().parents[4]
BASELINE_HASH = "c6d1047a8a4d9b0094274151c9658a101ed018ce07f82d66ed9fb512eb285321"
BUFFER_BYTES = 256 * 1024

spec = importlib.util.spec_from_file_location("diagnose_c100", EVIDENCE / "candidate/diagnose_c100.py")
diagnostic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostic)

original_uds_init = diagnostic.transport.Uds.__init__
original_run_init = diagnostic.benchmark.ThroughputRun.__init__
original_persist = diagnostic.Calls.persist


def buffered_uds_init(client, *arguments, **keywords):
    original_uds_init(client, *arguments, **keywords)
    previous = client.reader
    client.reader = client.connection.makefile("rb", buffering=BUFFER_BYTES)
    previous.close()


def identified_run_init(run, arguments):
    if diagnostic.transport.sha(arguments.binary) == BASELINE_HASH:
        arguments.label = "diagnostic-buffer-baseline"
        arguments.source_manifest = REPOSITORY / "docs/impl/acceptance-20261020/installed/source-manifest.json"
    else:
        arguments.label = "diagnostic-buffer-candidate02"
    original_run_init(run, arguments)


def buffered_persist(calls, run):
    original_persist(calls, run)
    path = run.output / "rpc-diagnostics.json"
    value = json.loads(path.read_text())
    value["buffer_experiment"] = {
        "reader_buffer_bytes": BUFFER_BYTES,
        "original_buffer_bytes": 8192,
        "scope": "Every fresh UDS connection; the only changed workload/client behavior is reader buffering",
        "wrapper": str(Path(__file__).resolve()),
        "wrapper_sha256": diagnostic.transport.sha(Path(__file__)),
        "declaration": str(EVIDENCE / "candidate02/buffer-plan.json"),
        "declaration_sha256": diagnostic.transport.sha(EVIDENCE / "candidate02/buffer-plan.json"),
    }
    diagnostic.transport.write_json(path, value)


diagnostic.transport.Uds.__init__ = buffered_uds_init
diagnostic.benchmark.ThroughputRun.__init__ = identified_run_init
diagnostic.Calls.persist = buffered_persist


if __name__ == "__main__":
    diagnostic.main()
