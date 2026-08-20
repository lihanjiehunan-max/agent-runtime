import assert from 'node:assert/strict';
import test from 'node:test';
import { createApp } from '../src/api.js';
import { authorization, createTestAuthenticator } from '../test-support/auth.js';

test('context reference, tool effect, checkpoint, and recovery form one HTTP workflow', async (t) => {
  const app = createApp({ dbPath: ':memory:', authenticator: createTestAuthenticator() });
  await new Promise((resolve) => app.server.listen(0, '127.0.0.1', resolve));
  t.after(() => app.server.close());
  const base = `http://127.0.0.1:${app.server.address().port}`;
  const call = async (path, body, token = 't1-host-1-token') => {
    const response = await fetch(`${base}${path}`, { method: 'POST', headers: { 'content-type': 'application/json', ...authorization(token) }, body: JSON.stringify(body) });
    return { status: response.status, data: await response.json() };
  };

  const context = (await call('/api/contexts', { content: 'private source material' })).data;
  const run = (await call('/api/runs', { goal: 'analyze source' })).data;
  const lease = (await call(`/api/runs/${run.runId}/lease`, {})).data;
  await call(`/api/runs/${run.runId}/start`, { lease });
  await call(`/api/runs/${run.runId}/checkpoint`, { lease, kind: 'FULL', state: { phase: 'context' } });
  const read = await call(`/api/runs/${run.runId}/contexts/${context.contextRefId}/read`, { lease });
  assert.equal(read.data.content, 'private source material');
  const effect = await call(`/api/runs/${run.runId}/tools/invoke`, { lease, operationId: 'counter.increment', version: 1, input: { name: 'reports', amount: 1 }, idempotencyKey: `${run.runId}:report` });
  assert.equal(effect.data.output.value, 1);
  await call(`/api/runs/${run.runId}/checkpoint`, { lease, kind: 'DELTA', state: { phase: 'complete' } });

  const recovered = await call(`/api/runs/${run.runId}/recover`, { reason: 'host-1 process stopped' }, 't1-host-2-token');
  assert.equal(recovered.data.status, 'RUNNING');
  assert.deepEqual(recovered.data.state, { phase: 'complete' });
  assert.equal(recovered.data.executionEpoch, lease.executionEpoch + 1);
});

test('HTTP context revocation invalidates dependent checkpoints and fences the active lease', async (t) => {
  const app = createApp({ dbPath: ':memory:', authenticator: createTestAuthenticator() });
  await new Promise((resolve) => app.server.listen(0, '127.0.0.1', resolve));
  t.after(() => app.server.close());
  const base = `http://127.0.0.1:${app.server.address().port}`;
  const call = async (path, body, token = 't1-host-1-token') => {
    const response = await fetch(`${base}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...authorization(token) },
      body: JSON.stringify(body),
    });
    return { status: response.status, data: await response.json() };
  };

  const context = (await call('/api/contexts', { content: 'revocable source' })).data;
  const run = (await call('/api/runs', { goal: 'use revocable source' })).data;
  const lease = (await call(`/api/runs/${run.runId}/lease`, {})).data;
  await call(`/api/runs/${run.runId}/start`, { lease });
  await call(`/api/runs/${run.runId}/checkpoint`, { lease, kind: 'FULL', state: { safe: true } });
  await call(`/api/runs/${run.runId}/contexts/${context.contextRefId}/read`, { lease });
  await call(`/api/runs/${run.runId}/checkpoint`, { lease, kind: 'DELTA', state: { unsafe: true } });

  const revoked = await call(`/api/contexts/${context.contextRefId}/revoke`, { reason: 'source withdrawn' });
  const stale = await call(`/api/runs/${run.runId}/checkpoint`, { lease, kind: 'DELTA', state: { later: true } });

  assert.equal(revoked.status, 200);
  assert.equal(revoked.data.reconciliations[0].action, 'ROLLBACK');
  assert.equal(stale.status, 409);
  assert.equal(stale.data.error.code, 'FENCED_OUT');
  assert.equal(app.runtime.getRun(run.runId, 't1').status, 'RECOVERING');
  assert.equal(app.db.prepare("SELECT count(*) AS count FROM checkpoints WHERE run_id = ? AND status = 'INVALIDATED_CONTEXT_REVOKED'").get(run.runId).count, 1);
});
