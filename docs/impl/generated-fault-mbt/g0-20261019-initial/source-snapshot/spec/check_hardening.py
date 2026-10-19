#!/usr/bin/env python3
"""Check hardening requirements, identity/protocol composition, or MBFaults.

Uses the installed TLC CLI, or TLC_BIN / --tlc for another executable. Nothing
is generated into the repository; complete TLC logs and states go to /tmp by
default. This validates the models and properties, not the Rust implementation.
"""

import argparse
import os
from pathlib import Path
import re
import subprocess
import tempfile


SCENARIOS = (
    "writer", "storage", "inbox", "deadletter", "document", "status", "rotation"
)
SAFETY_CONTROLS = (
    ("writer", "writer", "RootWriterExclusive"),
    ("writer", "replay", "ReplayRequiresOwnership"),
    ("storage", "storage", "AcknowledgedRecordsSurvive"),
    ("inbox", "inbox", "ExpiredOnlyCursorProgress"),
    ("deadletter", "deadletter", "DeadLetterAccountedFor"),
    ("document", "document", "UnpublishedDocRetryable"),
    ("status", "status", "StatusHeadMatchesDurableLog"),
    ("rotation", "rotation", "RotationBounded"),
    ("rotation", "cursor", "CursorReadContinuity"),
)
TEMPORAL_CONTROLS = (
    ("inbox", "inbox", "ExpiredCapacityEventuallyFreed"),
    ("deadletter", "deadletter", "DeadLetterEventuallyPublished"),
)
INSTANCE_SCENARIOS = ("lifecycle", "messaging", "presence", "lease", "combined")
INSTANCE_CONTROLS = (
    ("lifecycle", "kind-identity", "InstanceIdentityUnique"),
    ("lifecycle", "reused-id", "IdentityNotReused"),
    ("lifecycle", "retry-id", "EnrollmentRetryStable"),
    ("lifecycle", "reconnect-id", "InstanceIdentityStable"),
    ("lifecycle", "shared-token", "DistinctInstanceCredentials"),
    ("lifecycle", "stale-token", "RevokedCredentialsRejected"),
    ("messaging", "inbox-broadcast", "InboxAndAckIsolation"),
    ("messaging", "foreign-ack", "InboxAndAckIsolation"),
    ("messaging", "kind-dedupe", "IdempotencyScopedToInstance"),
    ("presence", "group-heartbeat", "PresenceAndLeaseIsolation"),
    ("lease", "foreign-lease", "PresenceAndLeaseIsolation"),
    ("lease", "stale-fence", "LeaseFenceFresh"),
)
INSTANCE_WITNESSES = (
    ("lifecycle", "NoSameKindPeers"),
    ("lifecycle", "NoRetiredThenNew"),
    ("messaging", "NoIndependentSameKeySends"),
    ("combined", "NoCombinedActivity"),
)
COMPOSED_SCENARIOS = ("messaging", "queue", "documents", "combined")
COMPOSED_CONTROLS = (
    ("queue", "missing-auth", "AuthenticatedRequestsOnly"),
    ("queue", "revoked-token", "AuthenticatedRequestsOnly"),
    ("messaging", "sender-spoof", "CanonicalActorBinding"),
    ("messaging", "foreign-ack", "AckIsolation"),
    ("messaging", "kind-dedupe", "RequestDecisionsCorrect"),
    ("queue", "foreign-task", "CanonicalActorBinding"),
    ("documents", "foreign-doc", "CanonicalActorBinding"),
    ("documents", "cas-bypass", "RequestParametersChecked"),
    ("queue", "stale-fence", "RequestParametersChecked"),
    ("messaging", "crash-forgets-identity", "CrashIdentityPreserved"),
    ("messaging", "bad-ack", "BrokerSafetyRefinement"),
    ("messaging", "identity-rebind", "IdentitySafetyRefinement"),
)
COMPOSED_WITNESSES = (
    ("messaging", "NoSameKindExchange"),
    ("messaging", "NoCrashAndResume"),
    ("queue", "NoForeignTaskRejection"),
    ("documents", "NoForeignDocRejection"),
    ("documents", "NoRotatedDocWrite"),
    ("combined", "NoCombinedProtocol"),
)
FAULT_SCENARIOS = ("writer", "storage", "inbox", "deadletter", "document", "status", "rotation", "identity")
FAULT_BOUNDS = dict(writer=6, storage=10, inbox=8, deadletter=7, document=7, status=4, rotation=12, identity=5)
FAULT_CONTROLS = (
    ("writer", "writer", "WriterSafe"),
    ("writer", "prelock-replay", "WriterSafe"),
    ("writer", "unowned-write", "WriterSafe"),
    ("storage", "continue-after-io", "ReceiptsSurvive"),
    ("storage", "ack-before-sync", "SyncBeforeReceipt"),
    ("storage", "lose-ack", "ReceiptsSurvive"),
    ("storage", "wrong-receipt", "ReceiptSequenceMatches"),
    ("inbox", "expired-cursor", "InboxSafe"),
    ("deadletter", "drop-projection", "DeadLetterRecoverable"),
    ("deadletter", "duplicate-projection", "DeadLetterUnique"),
    ("document", "doc-file-gate", "DocumentRetryable"),
    ("document", "early-publish", "PublishReceiptSafe"),
    ("status", "tail-window", "StatusAccurate"),
    ("status", "status-mutates", "StatusReadOnly"),
    ("rotation", "no-rotation", "RotationBounded"),
    ("rotation", "last-segment", "RotationCursorSafe"),
    ("rotation", "rotation-loss", "RotationContents"),
    ("identity", "kind-identity", "IdentitySafe"),
    ("identity", "reconnect-id", "IdentitySafe"),
    ("identity", "shared-token", "IdentitySafe"),
    ("identity", "stale-token", "IdentitySafe"),
)
FAULT_WITNESSES = (
    ("writer", "NoWriterRejection"), ("writer", "NoRejectedRepair"), ("writer", "NoWriterHandoff"),
    ("writer", "NoUnownedWrite"),
    ("storage", "NoStorageRecovery"), ("storage", "NoPartialRecovery"),
    ("storage", "NoAcknowledgedRecovery"),
    ("inbox", "NoInboxCapacityRecovery"),
    ("deadletter", "NoProjectionRecovery"), ("deadletter", "NoProjectionCrashRetry"),
    ("document", "NoDocumentRetry"), ("document", "NoDocumentCrashRetry"),
    ("status", "NoLargeStatus"),
    ("rotation", "NoRotatedCursor"), ("rotation", "NoRotationCrashRecovery"),
    ("rotation", "NoOversizedRotation"),
    ("identity", "NoSameKindEnrollment"), ("identity", "NoRevokedAuthentication"),
    ("identity", "NoStableReconnect"),
)
MODULES = {"hardening": "CooperatorsHardening", "instances": "AgentInstances",
           "composed": "CooperatorsInstances", "faults": "MBFaults"}


def run_apalache(args, output, label, config, invariant):
    """Bounded safety/reachability checks; no TLC RMI socket is needed."""
    work = output / label
    work.mkdir(exist_ok=True)
    (work / "AgentInstances.tla").write_bytes((output / "AgentInstances.tla").read_bytes())
    bindings = config.split("CONSTANTS", 1)[1].split("SPECIFICATION", 1)[0]
    bindings = [line.strip() for line in bindings.splitlines() if line.strip()]
    names = config.split("INVARIANTS\n", 1)[1].split("PROPERTIES", 1)[0].split()
    chosen = invariant or "CheckInvariant"
    wrapper = "-------------------------- MODULE InstanceCheck --------------------------\n"
    wrapper += "EXTENDS AgentInstances\nCInit == " + "\n         /\\ ".join(bindings) + "\n"
    wrapper += "CheckInvariant == " + " /\\ ".join(names) + "\n"
    wrapper += "===========================================================================\n"
    (work / "InstanceCheck.tla").write_text(wrapper)
    command = [args.apalache, "check", f"--inv={chosen}", f"--length={args.bound}",
               "--cinit=CInit", "--no-deadlock=true", "InstanceCheck.tla"]
    previous = set(work.glob("_apalache-out/InstanceCheck.tla/*/violation*.itf.json"))
    result = subprocess.run(command, cwd=work, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, timeout=args.timeout, check=False)
    # Read a real counterexample so a parser/tool error cannot pass a control.
    witnesses = set(work.glob("_apalache-out/InstanceCheck.tla/*/violation*.itf.json")) - previous
    if invariant:
        # Apalache emits both violation.itf.json and its numbered alias.
        passed = result.returncode == 12 and bool(witnesses) and "state invariant" in result.stdout.lower()
    else:
        passed = result.returncode == 0 and "Checker reports no error up to computation length" in result.stdout
    return result, passed


def run_check(args, spec_dir, output, label, scenario, fault, invariant=None, prop=None):
    module = MODULES[args.model]
    config = (spec_dir / f"{module}.cfg").read_text()
    config = re.sub(r'(Scenario\s*=\s*)"[^"]+"', lambda match: f'{match[1]}"{scenario}"', config, count=1)
    config = config.replace('Fault = "none"', f'Fault = "{fault}"')
    if args.model == "faults":
        config = re.sub(r'MaxSteps\s*=\s*\d+', f'MaxSteps = {FAULT_BOUNDS[scenario]}', config)
    if args.model == "composed" and fault != "none":
        # Some fault paths deliberately use an asymmetric CHOOSE fallback.
        config = config.replace("SYMMETRY Symmetry\n", "")
    if invariant or prop:
        config = config.split("INVARIANTS", 1)[0]
        config += "INVARIANTS\n  TypeOK\n"
        if invariant:
            config += f"  {invariant}\n"
        if prop:
            config += f"PROPERTIES\n  {prop}\n"

    config_path = output / f"{label}.cfg"
    config_path.write_text(config)
    command = [
        args.tlc, "-workers", "1", "-metadir", str(output / f"{label}-states"),
        "-config", config_path.name, f"{module}.tla",
    ]
    if getattr(args, "search", "bfs") == "dfid":
        # Installed TLC 2.19 uses this as a ceiling, despite its help wording.
        # Require the explicit completion message below; exit 0 alone is not proof.
        command[1:1] = ["-dfid", "100"]
    log_path = output / f"{label}.log"
    try:
        if args.engine == "apalache":
            result, passed = run_apalache(args, output, label, config, invariant)
        else:
            result = subprocess.run(
                command, cwd=output, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=args.timeout, check=False,
            )
    except (OSError, subprocess.TimeoutExpired) as error:
        partial = getattr(error, "stdout", None) or ""
        if isinstance(partial, bytes):
            partial = partial.decode(errors="replace")
        log_path.write_text(partial + "\n" + str(error) + "\n")
        print(f"FAIL {label}: {error}", flush=True)
        return False
    log_path.write_text(result.stdout)

    if args.engine == "apalache":
        pass  # Bounded-engine success is classified above, not as TLC output.
    elif invariant:
        expected = f"Invariant {invariant} is violated."
        passed = result.returncode != 0 and expected in result.stdout
    elif prop:
        passed = result.returncode != 0 and "Temporal properties were violated" in result.stdout
    else:
        passed = result.returncode == 0 and "Model checking completed. No error has been found." in result.stdout

    stats = re.findall(r"([\d,]+) states generated, ([\d,]+) distinct states found", result.stdout)
    detail = f"; {stats[-1][1]} distinct states" if stats else ""
    print(f"{'PASS' if passed else 'FAIL'} {label}{detail} ({log_path})", flush=True)
    if not passed:
        print(result.stdout[-5000:], flush=True)
    return passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=MODULES, default="hardening")
    parser.add_argument("--engine", choices=("tlc", "apalache"), default="tlc")
    parser.add_argument("--search", choices=("bfs", "dfid"), help="TLC search: safety models default to socket-free DFID; hardening requires BFS for liveness")
    parser.add_argument("--tlc", default=os.environ.get("TLC_BIN", "tlc"))
    parser.add_argument("--apalache", default=os.environ.get("APALACHE_BIN", "apalache-mc"))
    parser.add_argument("--bound", type=int, default=6, help="Apalache transition bound")
    parser.add_argument("--timeout", type=float, default=30, help="seconds per TLC invocation")
    parser.add_argument("--scenario", choices=tuple(dict.fromkeys(SCENARIOS + INSTANCE_SCENARIOS + COMPOSED_SCENARIOS + FAULT_SCENARIOS)), help="check only this scenario and its controls")
    parser.add_argument("--output-dir", type=Path, help="retain logs/configs/states here (default: new /tmp directory)")
    args = parser.parse_args()
    args.search = args.search or ("bfs" if args.model == "hardening" else "dfid")
    scenarios = {"hardening": SCENARIOS, "instances": INSTANCE_SCENARIOS,
                 "composed": COMPOSED_SCENARIOS, "faults": FAULT_SCENARIOS}[args.model]
    controls = {"hardening": SAFETY_CONTROLS, "instances": INSTANCE_CONTROLS,
                "composed": COMPOSED_CONTROLS, "faults": FAULT_CONTROLS}[args.model]
    temporal = TEMPORAL_CONTROLS if args.model == "hardening" else ()
    witnesses = {"hardening": (), "instances": INSTANCE_WITNESSES,
                 "composed": COMPOSED_WITNESSES, "faults": FAULT_WITNESSES}[args.model]
    if args.scenario and args.scenario not in scenarios:
        parser.error(f"scenario {args.scenario} does not belong to model {args.model}")
    if args.engine == "apalache" and args.model != "instances":
        parser.error("Apalache mode currently supports only --model instances")
    if args.search == "dfid" and args.model == "hardening":
        parser.error("DFID is safety-only; the hardening temporal properties require BFS")
    if args.bound < 1:
        parser.error("--bound must be positive")
    spec_dir = Path(__file__).resolve().parent
    output = args.output_dir.resolve() if args.output_dir else Path(tempfile.mkdtemp(prefix="coop-hardening-"))
    output.mkdir(parents=True, exist_ok=True)
    module = MODULES[args.model]
    (output / f"{module}.tla").write_text((spec_dir / f"{module}.tla").read_text())
    if args.model == "composed":
        for dependency in ("Cooperators", "AgentInstances"):
            (output / f"{dependency}.tla").write_bytes((spec_dir / f"{dependency}.tla").read_bytes())
    if args.model == "faults":
        (output / "AgentInstances.tla").write_bytes((spec_dir / "AgentInstances.tla").read_bytes())
    print(f"{args.engine} evidence: {output}", flush=True)
    results = []
    for scenario in scenarios:
        if args.scenario and args.scenario != scenario:
            continue
        results.append(run_check(args, spec_dir, output, f"safe-{scenario}", scenario, "none"))
        # Stop on tool/model failures instead of reporting spurious control successes.
        if not results[-1]:
            return 1
        for case, fault, invariant in controls:
            if case == scenario:
                results.append(run_check(args, spec_dir, output, f"fault-{fault}", case, fault, invariant=invariant))
                if not results[-1]:
                    return 1
        for case, fault, prop in temporal:
            if case == scenario:
                results.append(run_check(args, spec_dir, output, f"fault-{fault}-liveness", case, fault, prop=prop))
                if not results[-1]:
                    return 1
        for case, invariant in witnesses:
            if case == scenario:
                results.append(run_check(args, spec_dir, output, f"witness-{invariant}", case, "none", invariant=invariant))
                if not results[-1]:
                    return 1
    print(f"{sum(results)}/{len(results)} checks passed", flush=True)
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
