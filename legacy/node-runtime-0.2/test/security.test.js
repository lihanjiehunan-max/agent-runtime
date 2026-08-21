import assert from 'node:assert/strict';
import test from 'node:test';
import { createApp } from '../src/api.js';
import { authorization, createTestAuthenticator } from '../test-support/auth.js';

async function fixture(t) {
  const app = createApp({ dbPath: ':memory:', authenticator: createTestAuthenticator() });
  await new Promise((resolve) => app.server.listen(0, '127.0.0.1', resolve));
  t.after(() => app.server.close());
  return { app, base: `http://127.0.0.1:${app.server.address().port}` };
}

test('malformed JSON and oversized bodies are rejected deterministically', async (t) => {
  const { base } = await fixture(t);
  const malformed = await fetch(`${base}/api/runs`, { method: 'POST', headers: { 'content-type': 'application/json', ...authorization() }, body: '{bad' });
  assert.equal(malformed.status, 400);
  assert.equal((await malformed.json()).error.code, 'MALFORMED_JSON');

  const oversized = await fetch(`${base}/api/contexts`, { method: 'POST', headers: { 'content-type': 'application/json', ...authorization() }, body: JSON.stringify({ content: 'x'.repeat(1024 * 1024 + 1) }) });
  assert.equal(oversized.status, 413);
  assert.equal((await oversized.json()).error.code, 'BODY_TOO_LARGE');
});

test('JSON mutation routes reject unsupported media types', async (t) => {
  const { base } = await fixture(t);
  const response = await fetch(`${base}/api/runs`, { method: 'POST', headers: { 'content-type': 'text/plain', ...authorization() }, body: JSON.stringify({ goal: 'should not run' }) });
  assert.equal(response.status, 415);
  assert.equal((await response.json()).error.code, 'UNSUPPORTED_MEDIA_TYPE');
});

test('authenticated tenant cannot reuse another tenant lease payload', async (t) => {
  const { base } = await fixture(t);
  const post = async (token, path, body) => {
    const response = await fetch(`${base}${path}`, { method: 'POST', headers: { 'content-type': 'application/json', ...authorization(token) }, body: JSON.stringify(body) });
    return { status: response.status, data: await response.json() };
  };
  const run = (await post('tenant-a-token', '/api/runs', { goal: 'private task' })).data;
  const lease = (await post('tenant-a-token', `/api/runs/${run.runId}/lease`, {})).data;

  const attack = await post('tenant-b-token', `/api/runs/${run.runId}/start`, { lease });
  assert.equal(attack.status, 403);
  assert.equal(attack.data.error.code, 'WORKER_IDENTITY_MISMATCH');
});

test('static file allowlist blocks encoded path traversal', async (t) => {
  const { base } = await fixture(t);
  for (const path of ['/..%2Fsrc%2Fruntime.js', '/%2e%2e/package.json', '/src/runtime.js']) {
    const response = await fetch(`${base}${path}`);
    assert.equal(response.status, 404);
  }
});

test('invalid cursors, lease controls, and tool versions return stable client errors', async (t) => {
  const { base } = await fixture(t);
  const call = async (path, { method = 'GET', body } = {}) => {
    const response = await fetch(`${base}${path}`, {
      method,
      headers: { ...authorization(), ...(body === undefined ? {} : { 'content-type': 'application/json' }) },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { status: response.status, data: await response.json() };
  };

  const cursor = await call('/api/events?run_id=missing&after_seq=-1&once=1');
  assert.equal(cursor.status, 400);
  assert.equal(cursor.data.error.code, 'INVALID_EVENT_CURSOR');

  const run = (await call('/api/runs', { method: 'POST', body: { goal: 'validate controls' } })).data;
  const badTtl = await call(`/api/runs/${run.runId}/lease`, { method: 'POST', body: { ttlSeconds: 0 } });
  const badForce = await call(`/api/runs/${run.runId}/lease`, { method: 'POST', body: { force: 'true' } });
  const missingReason = await call(`/api/runs/${run.runId}/lease`, { method: 'POST', body: { force: true } });
  assert.deepEqual(
    [badTtl.data.error.code, badForce.data.error.code, missingReason.data.error.code],
    ['INVALID_LEASE_TTL', 'INVALID_FORCE_FLAG', 'TAKEOVER_REASON_REQUIRED'],
  );
  assert.equal((await call(`/api/runs/${run.runId}`)).data.executionEpoch, 0);

  const lease = (await call(`/api/runs/${run.runId}/lease`, { method: 'POST', body: {} })).data;
  await call(`/api/runs/${run.runId}/start`, { method: 'POST', body: { lease } });
  const version = await call(`/api/runs/${run.runId}/tools/invoke`, {
    method: 'POST',
    body: { lease, operationId: 'echo', version: '1', input: {} },
  });
  assert.equal(version.status, 400);
  assert.equal(version.data.error.code, 'INVALID_TOOL_VERSION');
});
