#!/usr/bin/env python3
import json, os, sys
session = '00000000-0000-4000-8000-000000000123'
for raw in sys.stdin:
    request = json.loads(raw)
    if os.environ.get('COOP_CLI_TEST_RECORD'):
        with open(os.environ['COOP_CLI_TEST_RECORD'], 'a') as output:
            output.write(json.dumps(request) + '\n')
    if 'id' not in request:
        continue
    method = request.get('method')
    if method == 'initialize' and os.environ.get('COOP_CLI_TEST_FAIL'):
        message = ' '.join(os.environ.get(k, '') for k in ['COOP_TEST_SECRET', 'COOP_TOKEN', 'COOP_HOST_CONTROL_SECRET'])
        print(json.dumps({'id':request['id'],'error':{'code':-32000,'message':message}}), flush=True)
        continue
    if method == 'initialize':
        result = {'userAgent':'cooperators-protocol-fake'}
    elif method in ['thread/start', 'thread/resume', 'thread/read']:
        result = {'thread':{'id':session, 'cwd':os.getcwd(), 'status':{'type':'idle'}, 'turns':[]}}
    elif method == 'thread/items/list':
        result = {'data':[], 'nextCursor':None}
    else:
        result = {}
    print(json.dumps({'id':request['id'],'result':result}), flush=True)
