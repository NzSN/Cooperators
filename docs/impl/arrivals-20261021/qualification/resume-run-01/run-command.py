import datetime
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import time

out = pathlib.Path(sys.argv[1])
command = sys.argv[2:]
out.parent.mkdir(parents=True, exist_ok=True)
if out.exists() or out.with_suffix('.json').exists():
    raise SystemExit('Refusing to overwrite qualification evidence: ' + str(out))
paths = sorted(set(pathlib.Path('src').rglob('*.rs')) | set(pathlib.Path('tests').rglob('*.rs')) | set(pathlib.Path('tests').rglob('*.py')) | {pathlib.Path('Cargo.toml'), pathlib.Path('Cargo.lock')})
manifest = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
secrets = [v for k, v in os.environ.items() if len(v) >= 12 and any(w in k.upper() for w in ('TOKEN', 'SECRET', 'API_KEY', 'PASSWORD'))]
started = datetime.datetime.now(datetime.timezone.utc).isoformat()
begin = time.monotonic()
process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors='replace')
with out.open('x') as log:
    for line in process.stdout:
        for secret in secrets:
            line = line.replace(secret, '<REDACTED>')
        log.write(line)
        log.flush()
code = process.wait()
result = {'command': command, 'started_utc': started, 'ended_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'elapsed_s': time.monotonic()-begin, 'exit_code': code, 'passed': code == 0, 'required_uds': os.environ.get('COOP_REQUIRE_UDS') == '1', 'source_sha256': manifest, 'source_unchanged': all(hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() == digest for p, digest in manifest.items())}
out.with_suffix('.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k:v for k,v in result.items() if k != 'source_sha256'}), flush=True)
lines = out.read_text().splitlines()
print('\n'.join(lines[-35:] if code else [l for l in lines if 'test result:' in l or 'MODEL_TRACE' in l or 'finished' in l][-10:]), flush=True)
sys.exit(code)
