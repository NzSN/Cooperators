"""Replay-tool regressions. Synthetic peers here do not qualify the Rust SUT."""
import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import textwrap
import unittest

from fault_traces import ValueParser, project_trace
from check_fault_detection import tla
from generate_faults import HOME
from replay_faults import (IdentityNames, Peer, ReplayError, canonical,
                           load_corpus, parameters, read_json, replay)


class CodecTests(unittest.TestCase):
    def test_nested_values_and_escapes(self):
        actual = ValueParser(r'[text |-> "a\"b", numbers |-> <<1, -2>>, yes |-> TRUE, set |-> {"b", "a"}]').parse()
        self.assertEqual(actual, {'text': 'a"b', 'numbers': [1, -2], 'yes': True,
                                  'set': {'#set': ['a', 'b']}})

    def test_function_value(self):
        self.assertEqual(ValueParser('("a" :> 1 @@ "b" :> <<2>>)').parse(),
                         {'#map': [['a', 1], ['b', [2]]]})

    def test_unsupported_incomplete_duplicate_and_trailing_values_fail(self):
        for value in ('null', '<<1', '[a |-> 1, a |-> 2]', '("a" :> 1 @@ "a" :> 2)', '1 2'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ValueParser(value).parse()

    def test_canonical_types_and_collection_order(self):
        self.assertNotEqual(canonical(True), canonical(1))
        self.assertEqual(canonical({'#bigint': '42'}), canonical(42))
        self.assertEqual(canonical({'#set': [2, 1]}), canonical({'#set': [1, 2]}))
        self.assertNotEqual(canonical([2, 1]), canonical([1, 2]))
        self.assertEqual(canonical({'#map': [['a', 1], ['b', 2]]}),
                         canonical({'#map': [['b', 2], ['a', 1]]}))

    def test_duplicate_json_and_malformed_itf_fail(self):
        with self.assertRaises(ReplayError):
            read_json('{"id":1,"id":1}')
        for value in ({'#set': [1, 1]}, {'#map': [['a', 1], ['a', 2]]},
                      {'#bigint': '1', 'extra': 0}, {'#unknown': 1}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                canonical(value)

    def test_missing_counterexample_is_not_a_trace(self):
        with self.assertRaises(ValueError):
            project_trace('TLC parse failure', 'storage', 'NoStorageRecovery', 10)

    def test_probe_serializer_preserves_itf_semantics(self):
        value = {'rows': [{'id': {'#bigint': '3'}, 'ok': True}],
                 'ids': {'#map': [['a', 'opaque-a'], ['b', 'opaque-b']]},
                 'members': {'#set': []}, 'text': 'a"b'}
        self.assertEqual(canonical(ValueParser(tla(value)).parse()), canonical(value))


class CorpusTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / 'corpus'
        shutil.copytree(HOME / 'faults', self.root)

    def test_complete_corpus_and_no_ghost_observations(self):
        contract, traces = load_corpus(self.root)
        self.assertEqual(len(traces), 24)
        self.assertEqual(len(contract['profiles']), 8)
        for _, _, states in traces:
            for state in states:
                self.assertEqual(set(state), {'#meta', 'action_taken', 'parameters', 'result', 'observation'})
                for hidden in ('durable', 'nextSeq', 'committedAfterRecovery', 'pendingProjection'):
                    self.assertNotIn(hidden, state['observation'])

    def test_missing_member_fails(self):
        next((self.root / 'traces').iterdir()).unlink()
        with self.assertRaises((OSError, ReplayError)):
            load_corpus(self.root)

    def test_changed_member_fails(self):
        path = next((self.root / 'traces').iterdir())
        path.write_text(path.read_text() + ' ')
        with self.assertRaisesRegex(ReplayError, 'digest mismatch'):
            load_corpus(self.root)

    def test_removed_manifest_entry_does_not_shrink_the_gate(self):
        path = self.root / 'manifest.json'
        manifest = json.loads(path.read_text())
        manifest['traces'].pop()
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ReplayError, 'required trace'):
            load_corpus(self.root)

    def test_stale_source_fails(self):
        path = self.root / 'manifest.json'
        manifest = json.loads(path.read_text())
        manifest['sourceSha256'] = '0' * 64
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ReplayError, 'stale corpus'):
            load_corpus(self.root)

    def test_missing_discrimination_check_fails(self):
        path = self.root / 'manifest.json'
        manifest = json.loads(path.read_text())
        manifest['fixedInputTraceChecks'].pop()
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ReplayError, 'trace discrimination'):
            load_corpus(self.root)

    def test_individual_scenario_still_requires_the_full_corpus(self):
        path = self.root / 'traces/NoDocumentRetry.itf.json'
        path.unlink()
        with self.assertRaises((OSError, ReplayError)):
            load_corpus(self.root)


class IdentityTests(unittest.TestCase):
    def test_binding_is_a_bijection(self):
        names = IdentityNames()
        names.bind(names.ids, 'actual-a', 'id1')
        names.bind(names.ids, 'actual-a', 'id1')
        with self.assertRaisesRegex(ReplayError, 'share one concrete identity'):
            names.bind(names.ids, 'actual-a', 'id2')
        with self.assertRaisesRegex(ReplayError, 'changed unexpectedly'):
            names.bind(names.ids, 'actual-b', 'id1')

    def test_duplicate_actual_ids_cannot_be_hidden_by_aliases(self):
        _, traces = load_corpus(HOME / 'faults')
        states = next(states for name, _, states in traces if name == 'NoSameKindEnrollment.itf.json')
        names = IdentityNames()
        for index, state in enumerate(states):
            observed = copy.deepcopy(state['observation'])
            for field, replacement in [('ids', 'same-actual-id'), ('replies', 'same-actual-id'),
                                        ('currentTokens', 'same-actual-token')]:
                for pair in observed[field]['#map']:
                    if pair[1] not in {'-id', '-token'}:
                        pair[1] = replacement
            reply = {'result': state['result'], 'observation': observed,
                     'binding': {'agent_id': 'same-actual-id', 'token': 'same-actual-token'}}
            if index == len(states) - 1:
                with self.assertRaises(ReplayError):
                    names.normalize(state, reply)
            else:
                names.normalize(state, reply)


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        contract, traces = load_corpus(HOME / 'faults')
        self.contract = contract
        self.trace = next(trace for trace in traces if trace[0] == 'NoLargeStatus.itf.json')
        (self.root / 'fixture.json').write_text(json.dumps(self.trace[2]))
        self.peer = self.root / 'synthetic_peer.py'
        self.peer.write_text(textwrap.dedent('''\
            import json, sys, time
            from pathlib import Path
            root = Path(sys.argv[1])
            mode = sys.argv[2]
            states = json.loads((root / 'fixture.json').read_text())
            capabilities = json.loads((root / 'capabilities.json').read_text())
            index = 0
            for line in sys.stdin:
                request = json.loads(line)
                if request['op'] == 'hello':
                    reply = dict(id=0, schema='cooperators.fault-port/v1',
                                 capabilities=[] if mode == 'missing' else capabilities,
                                 implementation=dict(name='synthetic-driver-self-test', build='fixture'))
                else:
                    assert set(request) == {'id', 'op', 'parameters'}
                    state = states[index]
                    assert request['op'] == state['action_taken']
                    assert request['parameters']['scenario'] == state['parameters']['scenario']
                    reply = dict(id=request['id'], result=state['result'], observation=state['observation'])
                    if request['parameters']['scenario'] == 'storage':
                        receipts = reply['observation']['receipts']
                        if mode != 'forged-ledger':
                            reply['observation'].pop('receipts')
                        if request['op'] == 'sync-ok':
                            reply['receipt'] = receipts[-1]
                            if mode == 'wrong-receipt': reply['receipt']['seq'] = -1
                    if mode == 'corrupt' and index == len(states) - 1:
                        reply['observation']['reported'] = -1
                    index += 1
                if mode == 'wrong-id': reply['id'] = 999
                if mode == 'timeout': time.sleep(2)
                if mode == 'oversize':
                    print('x' * (4 * 1024 * 1024 + 1), flush=True)
                else:
                    print(json.dumps(reply), flush=True)
            '''))
        caps = sorted({cap for profile in contract['profiles'].values() for cap in profile['capabilities']})
        (self.root / 'capabilities.json').write_text(json.dumps(caps))

    def command(self, mode):
        return [sys.executable, str(self.peer), str(self.root), mode]

    def test_happy_wire_path_excludes_expected_observations(self):
        with contextlib.redirect_stdout(io.StringIO()):
            result = replay(self.command('ok'), 5, self.root, self.contract, [self.trace])
        self.assertTrue(result)
        report = json.loads((self.root / 'results.json').read_text())
        self.assertEqual(report['declaredImplementation']['name'], 'synthetic-driver-self-test')

    def test_wrong_observation_is_a_replay_failure(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(replay(self.command('corrupt'), 5, self.root, self.contract, [self.trace]))

    def storage_fixture(self):
        _, traces = load_corpus(HOME / 'faults')
        self.trace = next(trace for trace in traces if trace[0] == 'NoAcknowledgedRecovery.itf.json')
        (self.root / 'fixture.json').write_text(json.dumps(self.trace[2]))

    def test_receipt_history_survives_restart_without_backend_history(self):
        self.storage_fixture()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(replay(self.command('ok'), 5, self.root, self.contract, [self.trace]))

    def test_wrong_receipt_and_forged_history_are_rejected(self):
        self.storage_fixture()
        for mode in ('wrong-receipt', 'forged-ledger'):
            with self.subTest(mode=mode), contextlib.redirect_stdout(io.StringIO()):
                self.assertFalse(replay(self.command(mode), 5, self.root, self.contract, [self.trace]))

    def test_missing_capability_fails_before_initialization(self):
        with self.assertRaisesRegex(ReplayError, 'missing implementation capabilities'):
            replay(self.command('missing'), 5, self.root, self.contract, [self.trace])

    def test_response_timeout_id_and_size_are_enforced(self):
        for mode, message, timeout in [('wrong-id', 'request ID', 5),
                                        ('timeout', 'timed out', .2),
                                        ('oversize', 'line-size limit', 5)]:
            with self.subTest(mode=mode), self.assertRaisesRegex(ReplayError, message):
                with Peer(self.command(mode), timeout, self.root / f'{mode}.stderr') as peer:
                    peer.hello([])


if __name__ == '__main__':
    unittest.main()
