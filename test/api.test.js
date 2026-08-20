import assert from 'node:assert/strict';
import test from 'node:test';
import { createApp } from '../src/api.js';
import { authorization, createTestAuthenticator } from '../test-support/auth.js';

async function serve() {
  const app = createApp({ dbPath: ':memory:', authenticator: createTestAuthenticator() });
  await new Promise((resolve) => app.server.listen(0, '127.0.0.1', resolve));
  const address = app.server.address();
  return { ...app, base: `http://127.0.0.1:${address.port}` };
}

async function request(base, path, { method = 'GET', body, token = 't1-worker-a-token' } = {}) {
  const response = await fetch(`${base}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...(token ? authorization(token) : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = response.headers.get('content-type')?.includes('json') ? await response.json() : await response.text();
  return { response, data };
}

test('health and validation use stable JSON envelopes', async (t) => {
  const app = await serve();
  t.after(() => app.server.close());

  const health = await request(app.base, '/api/health', { token: null });
  assert.equal(health.response.status, 200);
  assert.equal(health.data.status, 'ok');

  const invalid = await request(app.base, '/api/runs', { method: 'POST', body: {} });
  assert.equal(invalid.response.status, 400);
  assert.equal(invalid.data.error.code, 'INVALID_GOAL');
  assert.equal(typeof invalid.data.requestId, 'string');
});

test('control API executes a complete run lifecycle and rejects stale fencing', async (t) => {
  const app = await serve();
  t.after(() => app.server.close());

  const created = await request(app.base, '/api/runs', { method: 'POST', body: { goal: 'compile report' } });
  const runId = created.data.runId;
  const leaseA = (await request(app.base, `/api/runs/${runId}/lease`, { method: 'POST', body: {} })).data;
  await request(app.base, `/api/runs/${runId}/start`, { method: 'POST', body: { lease: leaseA } });
  const takeover = (await request(app.base, `/api/runs/${runId}/lease`, {
    method: 'POST', token: 't1-worker-b-token', body: { force: true, reason: 'worker-a stopped responding' },
  })).data;

  const stale = await request(app.base, `/api/runs/${runId}/checkpoint`, { method: 'POST', body: { lease: leaseA, kind: 'FULL', state: { step: 1 } } });
  assert.equal(stale.response.status, 409);
  assert.equal(stale.data.error.code, 'FENCED_OUT');

  const checkpoint = await request(app.base, `/api/runs/${runId}/checkpoint`, { method: 'POST', token: 't1-worker-b-token', body: { lease: takeover, kind: 'FULL', state: { step: 1 } } });
  assert.equal(checkpoint.response.status, 201);
  const finished = await request(app.base, `/api/runs/${runId}/complete`, { method: 'POST', token: 't1-worker-b-token', body: { lease: takeover, result: { summary: 'ready' } } });
  assert.equal(finished.data.status, 'SUCCEEDED');
});

test('SSE endpoint replays events after the requested sequence', async (t) => {
  const app = await serve();
  t.after(() => app.server.close());
  const run = (await request(app.base, '/api/runs', { method: 'POST', body: { goal: 'observe' } })).data;
  const lease = (await request(app.base, `/api/runs/${run.runId}/lease`, { method: 'POST', body: {} })).data;
  await request(app.base, `/api/runs/${run.runId}/start`, { method: 'POST', body: { lease } });

  const response = await fetch(`${app.base}/api/events?run_id=${run.runId}&after_seq=1&once=1`, { headers: authorization() });
  const stream = await response.text();
  assert.match(response.headers.get('content-type'), /text\/event-stream/);
  assert.doesNotMatch(stream, /RUN_CREATED/);
  assert.match(stream, /LEASE_ACQUIRED/);
  assert.match(stream, /RUN_STARTED/);
});
