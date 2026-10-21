#!/usr/bin/env python3
import json, os, pathlib, sys

root = pathlib.Path(os.environ['COOP_FAKE_ROOT'])
kind = os.environ['COOP_FAKE_KIND']
mode = os.environ['COOP_FAKE_MODE']
session = os.environ['COOP_FAKE_SESSION']
(root / 'launch.json').write_text(json.dumps({'args': sys.argv[1:], 'pid': os.getpid(), 'cwd': os.getcwd()}))
history_path = root / 'history.json'
history = json.loads(history_path.read_text()) if history_path.exists() else []
state = 'active' if mode == 'busy' else 'idle'
last = None
count = len(history)

def emit(value):
    print(json.dumps(value), flush=True)

def response(request, result):
    emit({'id': request['id'], 'result': result})

def thread():
    return {'id': 'wrong' if mode == 'wrong_thread' else session,
            'cwd': '/' if mode == 'wrong_workspace' else os.getcwd(),
            'status': {'type': 'idle' if (root / 'idle').exists() else state,
                       'activeFlags': ['waitingOnApproval'] if state == 'approval' else []}}

def claude_echo():
    emit({'type': 'user', 'session_id': session, 'uuid': 'echo-' + str(count),
          'message': last['message']})

for line in sys.stdin:
    request = json.loads(line)
    with (root / 'requests.jsonl').open('a') as output:
        output.write(json.dumps(request) + '\n')
    if kind == 'codex':
        method = request.get('method')
        if method == 'initialize':
            if mode == 'malformed':
                print('invalid-json', flush=True)
            elif mode == 'scalar':
                emit(17)
            else:
                response(request, {'userAgent': 'fake-codex/0.163.0'})
        elif method == 'initialized':
            pass
        elif method == 'thread/start':
            assert request['params']['cwd'] == os.getcwd()
            value = thread()
            if mode == 'missing_thread':
                del value['id']
            response(request, {'thread': value})
        elif method == 'thread/resume':
            assert request['params']['threadId'] == session
            if mode == 'staged_thread' and not (root / 'persisted').exists():
                emit({'id':request['id'], 'error':{'code':-32600, 'message':'no rollout found for thread id ' + session}})
                continue
            response(request, {'thread': thread()})
        elif method == 'thread/name/set':
            assert request['params']['threadId'] == session
            assert request['params']['name'] == 'Cooperators managed session'
            (root / 'persisted').write_text(session)
            response(request, {})
        elif method == 'thread/read':
            assert request['params']['threadId'] == session
            if mode == 'noisy_approval':
                state = 'approval'
                emit({'id': 'approval-' + str(count), 'method': 'item/commandExecution/requestApproval',
                      'params': {'threadId': session, 'turnId': 'turn-noisy', 'command': 'gated command'}})
                for i in range(300):
                    emit({'method':'diagnostic', 'params':{'threadId':session, 'value':i}})
                emit({'method':'diagnostic', 'params':{'threadId':session, 'value':'x' * 100000}})
                mode = 'approval'
            value = thread()
            if state == 'approval':
                value['status']['type'] = 'active'
            response(request, {'thread': value})
        elif method == 'turn/start':
            assert request['params']['threadId'] == session
            if mode in ['buffered_invalid', 'buffered_close']:
                terminal = 17 if mode == 'buffered_invalid' else {'method':'thread/closed', 'params':{'threadId':session}}
                frames = [terminal,
                          {'id':'late-approval', 'method':'item/commandExecution/requestApproval', 'params':{'threadId':session, 'command':'must not run'}},
                          {'method':'turn/completed', 'params':{'threadId':session, 'turn':{'id':'late-turn'}}},
                          {'id':request['id'], 'result':{'turn':{'id':'late-turn'}}}]
                sys.stdout.write(''.join(json.dumps(frame) + '\n' for frame in frames))
                sys.stdout.flush()
                continue
            if mode == 'oversized':
                print('x' * (4 * 1024 * 1024 + 1), flush=True)
                continue
            if mode == 'runtime_scalar':
                emit(17)
                continue
            count += 1
            turn_id = 'turn-' + str(count)
            history.append({'turnId': turn_id, 'item': {'type': 'userMessage', 'content': request['params']['input']}})
            history_path.write_text(json.dumps(history))
            state = 'active'
            if mode not in ['lost', 'approval_lost']:
                response(request, {'turn': {'id': turn_id, 'status': 'inProgress', 'items': []}})
            if mode in ['approval', 'approval_lost']:
                state = 'approval'
                emit({'id': 'approval-' + str(count), 'method': 'item/commandExecution/requestApproval',
                      'params': {'threadId': session, 'turnId': turn_id, 'command': 'gated command'}})
        elif method == 'thread/items/list':
            assert request['params']['threadId'] == session
            response(request, {'data': list(reversed(history)), 'nextCursor': None})
        elif method is None and 'result' in request:
            assert request['id'] == 'approval-' + str(count)
            assert request['result']['decision'] in ['accept', 'decline', 'cancel']
            state = 'idle'
            emit({'method':'turn/completed', 'params': {'threadId':session, 'turn':{'id':'turn-' + str(count), 'status':'completed'}}})
        else:
            raise AssertionError(request)
    else:
        if request['type'] == 'user':
            assert request['session_id'] == session
            count += 1
            last = request
            if mode == 'buffered_invalid':
                frames = [17,
                          {'type':'control_request', 'request_id':'late-approval', 'request':{'subtype':'can_use_tool', 'tool_name':'Bash', 'input':{'command':'must not run'}}},
                          {'type':'result', 'subtype':'success', 'session_id':session, 'uuid':'late-result'}]
                sys.stdout.write(''.join(json.dumps(frame) + '\n' for frame in frames))
                sys.stdout.flush()
                continue
            if mode == 'oversized':
                print('x' * (4 * 1024 * 1024 + 1), flush=True)
                continue
            if mode == 'runtime_scalar':
                emit(17)
                continue
            if mode == 'missing_session':
                emit({'type':'result', 'subtype':'success'})
                continue
            if mode == 'silent':
                continue
            emit({'type':'system', 'subtype':'init', 'session_id': 'wrong' if mode == 'wrong_session' else session})
            if mode != 'approval_before_echo':
                claude_echo()
            emit({'type':'control_request', 'request_id':'approval-' + str(count),
                  'request':{'subtype':'can_use_tool', 'tool_name':'Bash', 'input':{'command':'gated command'}}})
        elif request['type'] == 'control_response':
            assert request['response']['subtype'] == 'success'
            assert request['response']['request_id'] == 'approval-' + str(count)
            assert request['response']['response']['behavior'] in ['allow','deny']
            if mode == 'approval_before_echo':
                claude_echo()
            emit({'type':'result', 'subtype':'success', 'session_id':session, 'uuid':'result-' + str(count)})
        else:
            raise AssertionError(request)
