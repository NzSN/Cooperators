#!/usr/bin/env node
// Exercise installed client implementations without starting an LLM turn.
// No npm install, user configuration changes, or model credentials required.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { connect } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

const options = new Map();
for (let i = 2; i < process.argv.length; i += 2) {
  assert(['--binary', '--codex', '--dsh-package'].includes(process.argv[i]),
    `Unknown argument: ${process.argv[i]}`);
  assert(process.argv[i + 1], `Missing value for ${process.argv[i]}`);
  options.set(process.argv[i], process.argv[i + 1]);
}
assert(options.has('--dsh-package'),
  'Usage: node scripts/check_mcp_clients.mjs --dsh-package /path/to/@deepseek-ai/dsh/package.json [--binary /path/to/coop] [--codex /path/to/codex]');
const binary = resolve(options.get('--binary') ?? 'target/debug/coop');
const codex = options.get('--codex') ?? 'codex';
const packageFile = resolve(options.get('--dsh-package'));
const require = createRequire(packageFile);
const load = name => import(pathToFileURL(require.resolve(name)).href);

async function stop(child) {
  if (!child || child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, 'exit');
  child.stdin?.end();
  if (await Promise.race([exited.then(() => true), delay(1000).then(() => false)])) return;
  child.kill('SIGTERM');
  if (await Promise.race([exited.then(() => true), delay(2000).then(() => false)])) return;
  child.kill('SIGKILL');
  await exited;
}

// A bounded app-server RPC peer. Unsolicited requests fail explicitly: this
// check neither requests nor auto-approves provider actions or model turns.
function rpc(child) {
  let nextId = 1;
  const pending = new Map();
  let diagnostics = '';
  child.stderr.on('data', chunk => { diagnostics = (diagnostics + chunk).slice(-8192); });
  const fail = error => {
    for (const entry of pending.values()) entry.reject(error);
    pending.clear();
  };
  child.on('error', fail);
  child.on('exit', () => fail(new Error(`Codex exited: ${diagnostics}`)));
  createInterface({ input: child.stdout }).on('line', line => {
    try {
      const message = JSON.parse(line);
      if (message.method && message.id !== undefined) {
        child.stdin.write(JSON.stringify({ id: message.id, error: { code: -32601, message: 'Unsupported in MCP smoke check' } }) + '\n');
      } else if (pending.has(message.id)) {
        const entry = pending.get(message.id);
        pending.delete(message.id);
        if (message.error) entry.reject(new Error(JSON.stringify(message.error)));
        else entry.resolve(message.result);
      }
    } catch (error) { fail(error); }
  });
  return {
    notify(method) { child.stdin.write(JSON.stringify({ method }) + '\n'); },
    async call(method, params) {
      const id = nextId++;
      let timer;
      try {
        return await new Promise((resolveCall, reject) => {
          pending.set(id, { resolve: resolveCall, reject });
          timer = setTimeout(() => reject(new Error(`${method} timed out: ${diagnostics}`)), 30000);
          child.stdin.write(JSON.stringify({ id, method, params }) + '\n');
        });
      } finally {
        clearTimeout(timer);
        pending.delete(id);
      }
    },
  };
}

const temp = await mkdtemp(join(tmpdir(), 'coop-mcp-clients-'));
let daemon, app, context;
try {
  const bus = join(temp, 'bus');
  const socket = join(bus, 'run', 'daemon.sock');
  // Isolate broker selectors and Codex settings from the caller's deployment.
  const env = { PATH: process.env.PATH, HOME: temp, CODEX_HOME: join(temp, 'codex') };
  const busEnv = { COOPERATORS_HOME: bus, COOP_SOCKET: socket, COOP_ENDPOINT: '', COOP_TOKEN: '' };
  daemon = spawn(binary, ['daemon', 'run', '--http-addr', 'off'], {
    env: { ...env, ...busEnv }, stdio: ['ignore', 'ignore', 'pipe'],
  });
  let daemonError = '';
  daemon.stderr.on('data', chunk => { daemonError = (daemonError + chunk).slice(-8192); });
  daemon.on('error', error => { daemonError = String(error); });
  let ready = false;
  for (let attempt = 0; attempt < 200; attempt++) {
    ready = await new Promise(resolveProbe => {
      const probe = connect(socket);
      probe.on('connect', () => { probe.destroy(); resolveProbe(true); });
      probe.on('error', () => { probe.destroy(); resolveProbe(false); });
    });
    if (ready || daemon.pid === undefined || daemon.exitCode !== null) break;
    await delay(25);
  }
  assert(ready, `Broker unavailable (this check requires Unix sockets): ${daemonError}`);

  const [{ Context }, { default: SystemPrompt }, { default: Tools }, mcp] = await Promise.all([
    load('@deepseek-ai/cordis'), load('@deepseek-ai/dsh-system-prompt'),
    load('@deepseek-ai/dsh-tools'), load('@deepseek-ai/dsh-mcp-client'),
  ]);
  context = new Context();
  await context.plugin(SystemPrompt, {});
  await context.plugin(Tools, {});
  await context.plugin(mcp, {
    serverName: 'cooperators', transport: 'stdio', command: binary,
    args: ['mcp-stdio', '--kind', 'deepseek', '--display-name', 'dsh-smoke'],
    env: busEnv, cwd: temp, toolCallTimeoutMs: 30000,
    failOnStartupError: true, reconnect: { enabled: false },
  });
  let callId = 0;
  const dshCall = async (tool, args = {}, expectError = false) => {
    const result = await context.tools.execute({
      callId: `smoke-${++callId}`, name: `mcp__cooperators__${tool}`,
      arguments: args, signal: AbortSignal.timeout(30000),
    });
    assert.equal(result.isError, expectError, `DSH ${tool}: ${JSON.stringify(result)}`);
    return result.value?.structuredContent;
  };
  const dshIdentity = await dshCall('coop_instance_metadata');
  const dshTools = context.tools.schemas().map(tool => tool.name);
  assert(dshTools.includes('mcp__cooperators__coop_send_message'));

  await mkdir(env.CODEX_HOME);
  await writeFile(join(env.CODEX_HOME, 'config.toml'), `
model_provider = "offline"
model = "mcp-smoke"
[model_providers.offline]
name = "Offline MCP check"
base_url = "http://127.0.0.1:9/v1"
wire_api = "responses"
[mcp_servers.cooperators]
command = ${JSON.stringify(binary)}
args = ["mcp-stdio", "--kind", "codex", "--display-name", "codex-smoke"]
tool_timeout_sec = 30
[mcp_servers.cooperators.env]
${Object.entries(busEnv).map(([key, value]) => `${key} = ${JSON.stringify(value)}`).join('\n')}
`, { mode: 0o600 });
  app = spawn(codex, ['app-server', '--listen', 'stdio://'], {
    cwd: temp, env, stdio: ['pipe', 'pipe', 'pipe'],
  });
  const peer = rpc(app);
  await peer.call('initialize', {
    clientInfo: { name: 'coop-mcp-smoke', version: '1' }, capabilities: { experimentalApi: true },
  });
  peer.notify('initialized');
  const started = await peer.call('thread/start', {
    cwd: temp, model: 'mcp-smoke', modelProvider: 'offline', approvalPolicy: 'never', sandbox: 'read-only',
  });
  const threadId = started.thread.id;
  const inventory = await peer.call('mcpServerStatus/list', { threadId, serverName: 'cooperators' });
  const server = inventory.data.find(server => server.name === 'cooperators');
  assert(server && !server.toolsError, JSON.stringify(inventory));
  assert.equal(Object.keys(server.tools).length, dshTools.length);
  const codexCall = async (tool, args = {}, expectError = false) => {
    const result = await peer.call('mcpServer/tool/call', { threadId, server: 'cooperators', tool, arguments: args });
    assert.equal(result.isError ?? false, expectError, `Codex ${tool}: ${JSON.stringify(result)}`);
    assert.deepEqual(JSON.parse(result.content[0].text), result.structuredContent);
    return result.structuredContent;
  };
  const codexIdentity = await codexCall('coop_instance_metadata');
  assert.notEqual(codexIdentity.agent_id, dshIdentity.agent_id);
  await codexCall('coop_send_message', { to: dshIdentity.agent_id, kind: 'request', text: 'codex-to-dsh', idempotency_key: 'codex-smoke' });
  const received = await dshCall('coop_wait_message', { timeout_s: 0 });
  assert.equal(received.messages[0].body.text, 'codex-to-dsh');
  await dshCall('coop_ack', { cursor: received.next_cursor });
  await dshCall('coop_send_message', { to: codexIdentity.agent_id, kind: 'response', text: 'dsh-to-codex', idempotency_key: 'dsh-smoke' });
  const reply = await codexCall('coop_wait_message', { timeout_s: 0 });
  assert.equal(reply.messages[0].body.text, 'dsh-to-codex');
  await codexCall('coop_ack', { cursor: reply.next_cursor });
  assert.equal((await dshCall('coop_wait_message', { timeout_s: 0 })).messages.length, 0);
  assert.equal((await codexCall('coop_wait_message', { timeout_s: 0 })).messages.length, 0);
  await dshCall('coop_read_doc', { name: 'missing' }, true);
  assert.equal((await codexCall('coop_read_doc', { name: 'missing' }, true)).data.code, 'NOT_FOUND');
  const dshPackage = JSON.parse(await readFile(packageFile, 'utf8'));
  const codexVersion = spawnSync(codex, ['--version'], { env, encoding: 'utf8', timeout: 10000 });
  assert.equal(codexVersion.status, 0);
  console.log(JSON.stringify({
    passed: true, codex: codexVersion.stdout.trim(), dsh: dshPackage.version,
    toolCount: dshTools.length, transport: 'stdio-to-UDS',
    checks: ['discovery', 'distinct-identities', 'bidirectional-messages', 'peek-and-ack', 'tool-errors'],
    modelTurns: 0,
  }, null, 2));
} finally {
  try { if (context) await context.fiber.dispose(); }
  finally {
    await stop(app);
    await stop(daemon);
    await rm(temp, { recursive: true, force: true });
  }
}
