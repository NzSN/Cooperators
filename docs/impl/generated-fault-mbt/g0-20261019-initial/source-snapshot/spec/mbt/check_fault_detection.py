"""Check that fixed-input exported traces distinguish the actual fault policies.

Uses the original TLA+ Next, not a Python reimplementation. Fresh identity/token
choices are guided as fixture entropy; identity reuse/rebinding remains visible.
"""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

KILLERS = {
 'writer':'NoWriterRejection', 'prelock-replay':'NoRejectedRepair', 'unowned-write':'NoUnownedWrite',
 'continue-after-io':'NoStorageRecovery', 'ack-before-sync':'NoStorageRecovery',
 'lose-ack':'NoAcknowledgedRecovery', 'wrong-receipt':'NoAcknowledgedRecovery',
 'expired-cursor':'NoInboxCapacityRecovery', 'drop-projection':'NoProjectionRecovery',
 'duplicate-projection':'NoProjectionCrashRetry', 'doc-file-gate':'NoDocumentRetry',
 'early-publish':'NoDocumentRetry', 'tail-window':'NoLargeStatus', 'status-mutates':'NoLargeStatus',
 'no-rotation':'NoRotatedCursor', 'last-segment':'NoRotatedCursor', 'rotation-loss':'NoRotatedCursor',
 'kind-identity':'NoSameKindEnrollment', 'reconnect-id':'NoStableReconnect',
 'shared-token':'NoSameKindEnrollment', 'stale-token':'NoRevokedAuthentication',
}


def tla(value):
    if type(value) is bool:
        return 'TRUE' if value else 'FALSE'
    if type(value) is int:
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return '<<' + ', '.join(map(tla, value)) + '>>'
    if isinstance(value, dict):
        if set(value) == {'#bigint'}:
            return str(int(value['#bigint']))
        if set(value) == {'#set'}:
            return '{' + ', '.join(map(tla, value['#set'])) + '}'
        if set(value) == {'#map'}:
            pairs = value['#map']
            return '(' + ' @@ '.join(f'{tla(key)} :> {tla(item)}' for key, item in pairs) + ')' if pairs else '[x \\in {} |-> 0]'
        if any(not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', key) for key in value):
            raise ValueError('unsupported TLA record key')
        return '[' + ', '.join(f'{key} |-> {tla(item)}' for key, item in sorted(value.items())) + ']'
    raise ValueError('unsupported trace value')


def probe_source(trace):
    rows = [{key: state[key] for key in ('action_taken', 'parameters', 'result', 'observation')}
            for state in trace['states']]
    return '''--------------------------- MODULE TraceProbe ---------------------------
EXTENDS MBFaults, TLC
Expected == ''' + tla(rows) + '''
TraceConforms == /\\ observation = Expected[steps + 1].observation
                 /\\ result = Expected[steps + 1].result
(* Fix only genuinely fresh allocation choices as test fixture entropy. A
   faulty reuse is not fresh and is NOT filtered out. Reconnect ID changes
   are also not filtered. Neither guiding condition is sent to a SUT port. *)
FreshChoices == IF Scenario = "identity" THEN
  LET actor == parameters'.actor
      row == Expected[steps + 2].observation
  IN IF action_taken' = "enroll" /\\ actor \\notin s.enrolled
     THEN /\\ (s'.id[actor] \\in AgentIds \\ s.usedIds => s'.id[actor] = row.ids[actor])
          /\\ (s'.currentToken[actor] \\in Tokens \\ s.usedTokens =>
                s'.currentToken[actor] = row.currentTokens[actor])
     ELSE IF action_taken' = "rotate-token"
          THEN s'.currentToken[actor] = row.currentTokens[actor] ELSE TRUE
  ELSE TRUE
ProbeNext == IF steps < Len(Expected) - 1
  THEN /\\ Next
       /\\ action_taken' = Expected[steps + 2].action_taken
       /\\ parameters' = Expected[steps + 2].parameters
       /\\ FreshChoices
  ELSE UNCHANGED vars
ProbeSpec == Init /\\ [][ProbeNext]_vars
===========================================================================
'''


def check_detection(spec_dir, traces, checks, tlc_bin, timeout):
    root = Path(tempfile.mkdtemp(prefix='coop-fault-detection-'))
    indexed = {trace['#meta']['goal']: trace for trace in traces}
    if set(KILLERS) != {fault for _, fault, _ in checks.FAULT_CONTROLS}:
        raise ValueError('each fault must have an assigned distinguishing golden trace')
    cases = [(goal, 'none') for goal in indexed]
    cases += [(goal, fault) for fault, goal in KILLERS.items()]
    results = []
    print(f'Fixed-input trace detection evidence: {root}', flush=True)
    for goal, fault in cases:
        trace = indexed[goal]
        profile = trace['#meta']['scenario']
        label = f'{fault}-{goal}'
        work = root / label
        work.mkdir()
        for module in ('MBFaults', 'AgentInstances'):
            (work / f'{module}.tla').write_bytes((spec_dir / f'{module}.tla').read_bytes())
        (work / 'TraceProbe.tla').write_text(probe_source(trace))
        config = f'''CONSTANTS
  Scenario = "{profile}"
  Fault = "{fault}"
  MaxSteps = {checks.FAULT_BOUNDS[profile]}
SPECIFICATION ProbeSpec
CHECK_DEADLOCK TRUE
INVARIANTS TypeOK ObservationFaithful TraceConforms
'''
        (work / 'TraceProbe.cfg').write_text(config)
        completed = subprocess.run([tlc_bin, '-dfid', '100', '-workers', '1',
            '-config', 'TraceProbe.cfg', '-metadir', str(work / 'states'), 'TraceProbe.tla'],
            cwd=work, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
        (work / 'check.log').write_text(completed.stdout)
        if fault == 'none':
            passed = completed.returncode == 0 and 'Model checking completed. No error has been found.' in completed.stdout
        else:
            passed = completed.returncode != 0 and ('Invariant TraceConforms is violated.' in completed.stdout
                                                    or 'Deadlock reached.' in completed.stdout)
        if not passed:
            raise ValueError(f'fixed-input trace does not distinguish {label}:\n{completed.stdout[-4000:]}')
        print(f'PASS fixed-input {label}', flush=True)
        digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
        results.append({'case': label, 'trace': goal, 'fault': fault,
                        'logSha256': digest(work / 'check.log'),
                        'probeSha256': digest(work / 'TraceProbe.tla'),
                        'configSha256': digest(work / 'TraceProbe.cfg')})
    return results
