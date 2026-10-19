#!/usr/bin/env python3
"""Fail-closed replay of MBFaults traces against a separately supplied SUT port.

This is a replay driver, not a Rust implementation adapter. --verify-only checks
the corpus without qualifying runtime behavior. Expected observations never go
over the adapter's input channel.
"""
import argparse
from contextlib import AbstractContextManager
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import tempfile
import time

from generate_faults import HOME, SPEC, checks, sha
from check_fault_detection import KILLERS

SCHEMA = 'cooperators.fault-port/v1'


class ReplayError(ValueError):
    pass


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ReplayError(f'duplicate JSON field {key}')
        result[key] = value
    return result


def read_json(text):
    return json.loads(text, object_pairs_hook=unique_object)


def canonical(value):
    """Preserve types and sequence order; compare sets/functions extensionally."""
    if type(value) is bool:
        return ['bool', value]
    if type(value) is int:
        return ['int', str(value)]
    if isinstance(value, str):
        return ['str', value]
    if isinstance(value, list):
        return ['seq', [canonical(item) for item in value]]
    if isinstance(value, dict):
        if '#bigint' in value:
            if set(value) != {'#bigint'} or not isinstance(value['#bigint'], str):
                raise ReplayError('malformed ITF integer')
            return canonical(int(value['#bigint']))
        if '#set' in value:
            if set(value) != {'#set'} or not isinstance(value['#set'], list):
                raise ReplayError('malformed ITF set')
            members = [canonical(item) for item in value['#set']]
            encoded = [json.dumps(item, sort_keys=True) for item in members]
            if len(encoded) != len(set(encoded)):
                raise ReplayError('duplicate ITF set member')
            return ['set', sorted(members, key=lambda item: json.dumps(item, sort_keys=True))]
        if '#map' in value:
            if set(value) != {'#map'} or not isinstance(value['#map'], list):
                raise ReplayError('malformed ITF map')
            pairs = []
            seen = set()
            for entry in value['#map']:
                if not isinstance(entry, list) or len(entry) != 2:
                    raise ReplayError('malformed ITF map entry')
                key, item = map(canonical, entry)
                encoded = json.dumps(key, sort_keys=True)
                if encoded in seen:
                    raise ReplayError('duplicate ITF map key')
                seen.add(encoded)
                pairs.append([key, item])
            return ['map', sorted(pairs, key=lambda item: json.dumps(item[0], sort_keys=True))]
        if any(key.startswith('#') for key in value):
            raise ReplayError('unknown ITF tag')
        return ['record', [[key, canonical(item)] for key, item in sorted(value.items())]]
    raise ReplayError(f'unsupported observation value type {type(value).__name__}')


def parameters(state):
    values = state['parameters'].copy()
    value = values.get('value')
    if isinstance(value, dict) and set(value) == {'#bigint'}:
        value = int(value['#bigint'])
    if set(values) != {'scenario', 'actor', 'value', 'mode'} or type(value) is not int:
        raise ReplayError('invalid operation parameters')
    values['value'] = value
    return values


def load_corpus(root):
    manifest = read_json((root / 'manifest.json').read_text())
    contract = read_json((root / 'contract.json').read_text())
    if manifest.get('schema') != 'cooperators.fault-corpus/v1' or contract.get('schema') != SCHEMA:
        raise ReplayError('unsupported corpus or port schema')
    for field, path in [('sourceSha256', SPEC / 'MBFaults.tla'),
                        ('registrySha256', SPEC / 'AgentInstances.tla'),
                        ('contractSha256', root / 'contract.json')]:
        if manifest[field] != sha(path):
            raise ReplayError(f'stale corpus: {path.name}')
    required = {f'traces/{goal}.itf.json': profile for profile, goal in checks.FAULT_WITNESSES}
    entries = manifest['traces']
    names = [entry['file'] for entry in entries]
    if set(names) != set(required) or len(names) != len(required):
        raise ReplayError('missing, extra, or duplicated required trace')
    if {p.name for p in (root / 'traces').iterdir()} != {Path(name).name for name in names}:
        raise ReplayError('unmanifested or missing trace file')
    expected_checks = {f'safe-{p}' for p in checks.FAULT_SCENARIOS}
    expected_checks |= {f'fault-{fault}' for _, fault, _ in checks.FAULT_CONTROLS}
    expected_checks |= {f'witness-{goal}' for _, goal in checks.FAULT_WITNESSES}
    actual_checks = [entry['case'] for entry in manifest['modelChecks']]
    if set(actual_checks) != expected_checks or len(actual_checks) != len(expected_checks):
        raise ReplayError('incomplete model-check evidence manifest')
    required_probes = {f'none-{goal}' for _, goal in checks.FAULT_WITNESSES}
    required_probes |= {f'{fault}-{goal}' for fault, goal in KILLERS.items()}
    actual_probes = [entry['case'] for entry in manifest.get('fixedInputTraceChecks', [])]
    if set(actual_probes) != required_probes or len(actual_probes) != len(required_probes):
        raise ReplayError('incomplete fixed-input trace discrimination evidence')
    traces = []
    for entry in entries:
        path = root / entry['file']
        if sha(path) != entry['sha256']:
            raise ReplayError(f'trace digest mismatch: {path.name}')
        trace = read_json(path.read_text())
        profile = required[entry['file']]
        if entry['scenario'] != profile or trace['#meta']['scenario'] != profile:
            raise ReplayError('trace profile mismatch')
        states = trace['states']
        if not 2 <= len(states) <= checks.FAULT_BOUNDS[profile] + 1:
            raise ReplayError('invalid trace length')
        for index, state in enumerate(states):
            op = state['action_taken']
            inputs = parameters(state)
            if state['#meta']['index'] != index or inputs['scenario'] != profile:
                raise ReplayError('trace index or scenario mismatch')
            if op not in (['init'] if index == 0 else contract['profiles'][profile]['actions']):
                raise ReplayError(f'undeclared operation {op}')
            if set(state['observation']) != set(contract['profiles'][profile]['observations']):
                raise ReplayError('observation schema mismatch')
            canonical(state['observation'])
        traces.append((path.name, profile, states))
    return contract, traces


class Peer(AbstractContextManager):
    def __init__(self, command, timeout, stderr):
        self.timeout = timeout
        self.stderr = stderr.open('wb')
        try:
            self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                            stderr=self.stderr, start_new_session=True)
        except OSError:
            self.stderr.close()
            raise
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.buffer = b''

    def request(self, message):
        self.process.stdin.write((json.dumps(message) + '\n').encode())
        self.process.stdin.flush()
        deadline = time.monotonic() + self.timeout
        while b'\n' not in self.buffer:
            if not self.selector.select(max(0, deadline - time.monotonic())):
                raise ReplayError('adapter response timed out')
            chunk = os.read(self.process.stdout.fileno(), 65536)
            if not chunk:
                raise ReplayError('adapter exited before its response')
            self.buffer += chunk
            if len(self.buffer) > 4 * 1024 * 1024:
                raise ReplayError('adapter response exceeds the line-size limit')
        line, self.buffer = self.buffer.split(b'\n', 1)
        reply = read_json(line.decode())
        if not isinstance(reply, dict) or type(reply.get('id')) is not int or reply['id'] != message['id']:
            raise ReplayError('adapter reply has a missing or mismatched request ID')
        if 'error' in reply:
            raise ReplayError('adapter reported an operation/injection error')
        return reply

    def hello(self, required):
        reply = self.request({'id': 0, 'op': 'hello', 'schema': SCHEMA})
        capabilities = reply.get('capabilities')
        if reply.get('schema') != SCHEMA or not isinstance(capabilities, list) or not all(isinstance(c, str) for c in capabilities):
            raise ReplayError('adapter does not implement the required fault-port protocol')
        missing = set(required) - set(capabilities)
        if missing:
            raise ReplayError('missing implementation capabilities: ' + ', '.join(sorted(missing)))
        implementation = reply.get('implementation')
        if not isinstance(implementation, dict) or not implementation:
            raise ReplayError('missing adapter implementation identity')
        return implementation

    def __exit__(self, *unused):
        try:
            os.killpg(self.process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self.process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait()
        self.selector.close()
        self.process.stdin.close()
        self.process.stdout.close()
        self.stderr.close()


def map_values(value):
    canonical(value)
    if not isinstance(value, dict) or set(value) != {'#map'}:
        raise ReplayError('identity maps must use ITF #map')
    return dict(value['#map'])


class IdentityNames:
    def __init__(self):
        self.ids = {}
        self.tokens = {}

    @staticmethod
    def bind(mapping, concrete, abstract):
        if not isinstance(concrete, str) or concrete in {'', '-id', '-token'}:
            raise ReplayError('missing concrete identity or credential in backend reply')
        if concrete in mapping and mapping[concrete] != abstract:
            raise ReplayError('different abstract instances share one concrete identity or credential')
        if abstract in mapping.values() and mapping.get(concrete) != abstract:
            raise ReplayError('an instance identity or credential changed unexpectedly')
        mapping[concrete] = abstract

    def normalize(self, state, reply):
        observed = reply['observation']
        expected = state['observation']
        actor = state['parameters']['actor']
        if state['action_taken'] in {'enroll', 'reconnect', 'rotate-token'} and reply['result'] == 'ok':
            binding = reply.get('binding', {})
            for name, mapping, field in [('agent_id', self.ids, 'ids'), ('token', self.tokens, 'currentTokens')]:
                concrete = binding.get(name)
                if map_values(observed[field]).get(actor) != concrete:
                    raise ReplayError('backend binding does not match the observed instance')
                self.bind(mapping, concrete, map_values(expected[field])[actor])
        normalized = dict(observed)
        for field, mapping, absent in [('ids', self.ids, '-id'), ('replies', self.ids, '-id'),
                                        ('currentTokens', self.tokens, '-token')]:
            values = map_values(observed[field])
            result = []
            for key, value in values.items():
                if value != absent and value not in mapping:
                    raise ReplayError('identity observation contains an unbound backend value')
                result.append([key, absent if value == absent else mapping[value]])
            normalized[field] = {'#map': result}
        return normalized


def replay(command, timeout, output, contract, traces, provenance=None):
    required = {cap for _, profile, _ in traces for cap in contract['profiles'][profile]['capabilities']}
    provenance = provenance or {'scope': 'driver invocation without a corpus qualification record'}
    try:
        with Peer(command, timeout, output / 'preflight.stderr.log') as peer:
            identity = peer.hello(required)
    except (OSError, ValueError) as error:
        (output / 'results.json').write_text(json.dumps({
            'schema': SCHEMA, 'preflightPassed': False, 'error': str(error),
            'provenance': provenance, 'traces': []}, indent=2) + '\n')
        raise
    results = []
    for name, profile, states in traces:
        try:
            with Peer(command, timeout, output / f'{name}.stderr.log') as peer:
                if peer.hello(contract['profiles'][profile]['capabilities']) != identity:
                    raise ReplayError('adapter implementation identity changed during replay')
                names = IdentityNames()
                receipts = []
                for index, state in enumerate(states):
                    reply = peer.request({'id': index + 1, 'op': state['action_taken'],
                                          'parameters': parameters(state)})
                    if reply.get('result') != state['result']:
                        raise ReplayError(f'step {index} {state["action_taken"]}: response mismatch')
                    if 'observation' not in reply:
                        raise ReplayError(f'step {index}: missing implementation observation')
                    actual = names.normalize(state, reply) if profile == 'identity' else reply['observation']
                    if profile == 'storage':
                        if not isinstance(actual, dict) or 'receipts' in actual:
                            raise ReplayError('receipt history must be collected independently by the driver')
                        if state['action_taken'] == 'sync-ok' and reply['result'] == 'ok':
                            receipt = reply.get('receipt')
                            if not isinstance(receipt, dict) or set(receipt) != {'id', 'seq'}:
                                raise ReplayError('missing actual successful append receipt')
                            canonical(receipt)
                            receipts.append(receipt)
                        elif 'receipt' in reply:
                            raise ReplayError('unexpected append receipt before sync completion')
                        actual = {**actual, 'receipts': list(receipts)}
                    if canonical(actual) != canonical(state['observation']):
                        raise ReplayError(f'step {index} {state["action_taken"]}: implementation observation mismatch')
            results.append({'trace': name, 'scenario': profile, 'passed': True})
            print(f'PASS {name}', flush=True)
        except (OSError, ValueError) as error:
            results.append({'trace': name, 'scenario': profile, 'passed': False, 'error': str(error)})
            print(f'FAIL {name}: {error}', flush=True)
    report = {'schema': SCHEMA, 'declaredImplementation': identity, 'traces': results,
              'preflightPassed': True, 'provenance': provenance,
              'note': 'Adapter source/injection provenance requires review; this driver does not establish it.'}
    (output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    return all(item['passed'] for item in results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--corpus', type=Path, default=HOME / 'faults')
    parser.add_argument('--scenario', choices=checks.FAULT_SCENARIOS)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--verify-only', action='store_true')
    group.add_argument('--adapter', help='reviewed SUT port executable; there is no built-in Rust port yet')
    parser.add_argument('--adapter-arg', action='append', default=[])
    parser.add_argument('--timeout', type=float, default=10, help='seconds per adapter response')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error('--timeout must be positive')
    try:
        contract, traces = load_corpus(args.corpus.resolve())
        if args.scenario:
            traces = [trace for trace in traces if trace[1] == args.scenario]
        if args.verify_only:
            print(f'Verified {len(traces)} selected ITF traces. No implementation replay was performed.')
            return 0
        output = args.output or Path(tempfile.mkdtemp(prefix='coop-fault-replay-'))
        output.mkdir(parents=True, exist_ok=True)
        print(f'Replay evidence: {output}', flush=True)
        provenance = {'modelSha256': sha(SPEC / 'MBFaults.tla'),
                      'registrySha256': sha(SPEC / 'AgentInstances.tla'),
                      'contractSha256': sha(args.corpus / 'contract.json'),
                      'manifestSha256': sha(args.corpus / 'manifest.json'),
                      'selectedProfiles': sorted({profile for _, profile, _ in traces})}
        return 0 if replay([args.adapter, *args.adapter_arg], args.timeout, output, contract, traces, provenance) else 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'FAIL: {error}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
