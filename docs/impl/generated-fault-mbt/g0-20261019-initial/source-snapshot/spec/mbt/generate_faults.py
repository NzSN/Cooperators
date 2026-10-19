#!/usr/bin/env python3
"""Check all MBFaults profiles/controls and export their correct-policy ITF witnesses."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from fault_traces import project_trace
from check_fault_detection import check_detection

HOME = Path(__file__).resolve().parent
SPEC = HOME.parent
loader = importlib.util.spec_from_file_location('fault_checks', SPEC / 'check_hardening.py')
checks = importlib.util.module_from_spec(loader)
loader.loader.exec_module(checks)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_matrix(evidence):
    for name in ('MBFaults.tla', 'AgentInstances.tla'):
        if (evidence / name).read_bytes() != (SPEC / name).read_bytes():
            raise ValueError(f"stale evidence source: {name}")
    cases = [(f'safe-{profile}', profile, 'none', None) for profile in checks.FAULT_SCENARIOS]
    cases += [(f'fault-{fault}', profile, fault, inv) for profile, fault, inv in checks.FAULT_CONTROLS]
    cases += [(f'witness-{goal}', profile, 'none', goal) for profile, goal in checks.FAULT_WITNESSES]
    verified = []
    for label, profile, fault, inv in cases:
        path = evidence / f'{label}.log'
        text = path.read_text()
        expected = f'Invariant {inv} is violated.' if inv else 'Model checking completed. No error has been found.'
        if expected not in text or 'TLC threw an unexpected exception' in text:
            raise ValueError(f"incomplete or unexpected check: {label}")
        config = (SPEC / 'MBFaults.cfg').read_text()
        config = config.replace('Scenario = "storage"', f'Scenario = "{profile}"')
        config = config.replace('Fault = "none"', f'Fault = "{fault}"')
        config = re.sub(r'MaxSteps\s*=\s*\d+', f'MaxSteps = {checks.FAULT_BOUNDS[profile]}', config)
        if inv:
            config = config.split('INVARIANTS', 1)[0] + f'INVARIANTS\n  TypeOK\n  {inv}\n'
        if (evidence / f'{label}.cfg').read_text() != config:
            raise ValueError(f"mismatched evidence configuration: {label}")
        stats = re.findall(r'([\d,]+) states generated, ([\d,]+) distinct states found', text)
        if not stats:
            raise ValueError(f"missing completed statistics: {label}")
        verified.append({'case': label, 'logSha256': sha(path),
                         'configSha256': sha(evidence / f'{label}.cfg'),
                         'distinctStates': int(stats[-1][1].replace(',', ''))})
    return verified


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, help='reuse an exact, complete TLC matrix snapshot')
    parser.add_argument('--output', type=Path, default=HOME / 'faults')
    parser.add_argument('--timeout', type=float, default=60)
    parser.add_argument('--tlc', default=os.environ.get('TLC_BIN', 'tlc'))
    args = parser.parse_args()
    evidence = args.evidence.resolve() if args.evidence else Path(tempfile.mkdtemp(prefix='coop-fault-check-'))
    if not args.evidence:
        subprocess.run(['python3', str(SPEC / 'check_hardening.py'), '--model', 'faults',
                        '--timeout', str(args.timeout), '--tlc', args.tlc,
                        '--output-dir', str(evidence)], check=True)
    verified = verify_matrix(evidence)
    # Build every member before publishing any trace or replacing the manifest.
    traces = {}
    for profile, goal in checks.FAULT_WITNESSES:
        trace = project_trace((evidence / f'witness-{goal}.log').read_text(), profile, goal,
                              checks.FAULT_BOUNDS[profile])
        traces[f'{goal}.itf.json'] = json.dumps(trace, indent=2, sort_keys=True) + '\n'
    trace_checks = check_detection(SPEC, [json.loads(trace) for trace in traces.values()],
                                   checks, args.tlc, args.timeout)
    target = args.output.resolve()
    target.mkdir(parents=True, exist_ok=True)
    trace_dir = target / 'traces'
    trace_dir.mkdir(exist_ok=True)
    extras = {p.name for p in trace_dir.iterdir()} - set(traces)
    if extras:
        raise ValueError(f"unexpected corpus members; review them before regeneration: {sorted(extras)}")
    contract = HOME / 'faults/contract.json'
    retained_contract = target / 'contract.json'
    if retained_contract.resolve() != contract.resolve():
        if retained_contract.exists() and retained_contract.read_bytes() != contract.read_bytes():
            raise ValueError('review the different output contract before regeneration')
        retained_contract.write_bytes(contract.read_bytes())
    manifest = {'schema': 'cooperators.fault-corpus/v1', 'model': 'MBFaults',
                'sourceSha256': sha(SPEC / 'MBFaults.tla'),
                'registrySha256': sha(SPEC / 'AgentInstances.tla'),
                'contractSha256': sha(contract), 'profiles': checks.FAULT_BOUNDS,
                'modelChecks': verified, 'runtimeReplay': 'not performed by this generator',
                'fixedInputTraceChecks': trace_checks,
                'traces': []}
    for name, content in traces.items():
        path = trace_dir / name
        temporary = path.with_suffix('.tmp')
        temporary.write_text(content)
        temporary.replace(path)
        manifest['traces'].append({'file': f'traces/{name}', 'sha256': sha(path),
                                   'scenario': json.loads(content)['#meta']['scenario']})
    temporary = target / 'manifest.tmp'
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
    temporary.replace(target / 'manifest.json')
    print(f'{len(verified)} model checks; {len(trace_checks)} fixed-input checks; {len(traces)} ITF witnesses: {target}')


if __name__ == '__main__':
    main()
