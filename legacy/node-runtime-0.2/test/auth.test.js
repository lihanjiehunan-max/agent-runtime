import assert from 'node:assert/strict';
import test from 'node:test';
import { createDevelopmentAuthenticator, createRuntimeAuthenticator, createStaticAuthenticator } from '../src/auth.js';
import { createApp } from '../src/api.js';

const operatorPermissions = [
  'context:write', 'context:revoke', 'effect:resolve', 'run:create', 'run:execute',
  'run:read', 'run:recover', 'run:resolve', 'run:takeover', 'tool:read',
];

const tokens = {
  'operator-a-token': {
    tenantId: 'tenant-a',
    subjectId: 'operator-a',
    workerId: 'operator-worker-a',
    permissions: operatorPermissions,
  },
  'worker-a-token': {
    tenantId: 'tenant-a',
    subjectId: 'service-worker-a',
    workerId: 'worker-a',
    permissions: ['run:execute', 'run:read', 'tool:read'],
  },
  'worker-b-token': {
    tenantId: 'tenant-a',
    subjectId: 'service-worker-b',
    workerId: 'worker-b',
    permissions: ['run:execute', 'run:read', 'tool:read'],
  },
  'operator-b-token': {
    tenantId: 'tenant-b',
    subjectId: 'operator-b',
    workerId: 'operator-worker-b',
    permissions: operatorPermissions,
  },
};

async function fixture(t) {
  const authenticator = createStaticAuthenticator(tokens);
  const app = createApp({ dbPath: ':memory:', authenticator });
  await new Promise((resolve) => app.server.listen(0, '127.0.0.1', resolve));
  t.after(() => app.server.close());
  return { app, base: `http://127.0.0.1:${app.server.address().port}` };
}

async function request(base, path, { method = 'GET', token, tenant, body } = {}) {
  const headers = {};
  if (token) headers.authorization = `Bearer ${token}`;
  if (tenant) headers['x-tenant-id'] = tenant;
  if (body !== undefined) headers['content-type'] = 'application/json';
  const response = await fetch(`${base}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { response, data: await response.json() };
}

test('control APIs require a verified principal while health remains public', async (t) => {
  const { base } = await fixture(t);
  const health = await request(base, '/api/health');
  const forgedHeader = await request(base, '/api/runs', { tenant: 'tenant-a' });

  assert.equal(health.response.status, 200);
  assert.equal(forgedHeader.response.status, 401);
  assert.equal(forgedHeader.data.error.code, 'AUTHENTICATION_REQUIRED');
});

test('tenant scope comes from the principal and rejects a mismatched diagnostic header', async (t) => {
  const { base } = await fixture(t);
  const mismatch = await request(base, '/api/runs', { token: 'operator-a-token', tenant: 'tenant-b' });
  assert.equal(mismatch.response.status, 403);
  assert.equal(mismatch.data.error.code, 'TENANT_IDENTITY_MISMATCH');

  await request(base, '/api/runs', { method: 'POST', token: 'operator-a-token', body: { goal: 'tenant-a only' } });
  const tenantB = await request(base, '/api/runs', { token: 'operator-b-token' });
  assert.deepEqual(tenantB.data.items, []);
});

test('caller supplied actor identity is rejected and audit uses the authenticated subject', async (t) => {
  const { app, base } = await fixture(t);
  const saved = await request(base, '/api/contexts', { method: 'POST', token: 'operator-a-token', body: { content: 'sensitive context' } });
  const spoof = await request(base, `/api/contexts/${saved.data.contextRefId}/revoke`, {
    method: 'POST',
    token: 'operator-a-token',
    body: { reason: 'spoof attempt', actorId: 'global-admin' },
  });
  assert.equal(spoof.response.status, 400);
  assert.equal(spoof.data.error.code, 'CALLER_IDENTITY_FORBIDDEN');

  const revoked = await request(base, `/api/contexts/${saved.data.contextRefId}/revoke`, {
    method: 'POST',
    token: 'operator-a-token',
    body: { reason: 'approved withdrawal' },
  });
  assert.equal(revoked.response.status, 200);
  assert.equal(app.db.prepare("SELECT actor_id FROM audit_events WHERE event_type = 'CONTEXT_REVOKED'").get().actor_id, 'operator-a');
});

test('worker identity is principal-bound and cannot be selected in the request body', async (t) => {
  const { base } = await fixture(t);
  const run = await request(base, '/api/runs', { method: 'POST', token: 'operator-a-token', body: { goal: 'bind worker' } });
  const mismatch = await request(base, `/api/runs/${run.data.runId}/lease`, {
    method: 'POST',
    token: 'worker-a-token',
    body: { workerId: 'worker-b' },
  });

  assert.equal(mismatch.response.status, 403);
  assert.equal(mismatch.data.error.code, 'WORKER_IDENTITY_MISMATCH');
  const current = await request(base, `/api/runs/${run.data.runId}`, { token: 'operator-a-token' });
  assert.equal(current.data.executionEpoch, 0);
  assert.equal(current.data.leaseOwner, null);
});

test('ordinary workers cannot force takeover or invoke recovery', async (t) => {
  const { base } = await fixture(t);
  const run = await request(base, '/api/runs', { method: 'POST', token: 'operator-a-token', body: { goal: 'protect takeover' } });
  const lease = await request(base, `/api/runs/${run.data.runId}/lease`, { method: 'POST', token: 'worker-a-token', body: {} });
  const force = await request(base, `/api/runs/${run.data.runId}/lease`, {
    method: 'POST', token: 'worker-b-token', body: { force: true, reason: 'unauthorized' },
  });
  const recover = await request(base, `/api/runs/${run.data.runId}/recover`, {
    method: 'POST', token: 'worker-b-token', body: { reason: 'unauthorized' },
  });

  assert.equal(force.response.status, 403);
  assert.equal(force.data.error.code, 'PERMISSION_DENIED');
  assert.equal(recover.response.status, 403);
  assert.equal(recover.data.error.code, 'PERMISSION_DENIED');
  const current = await request(base, `/api/runs/${run.data.runId}`, { token: 'operator-a-token' });
  assert.equal(current.data.executionEpoch, lease.data.executionEpoch);
  assert.equal(current.data.leaseOwner, 'worker-a');
});

test('authorized takeover records the authenticated actor, previous owner, and reason', async (t) => {
  const { base } = await fixture(t);
  const run = await request(base, '/api/runs', { method: 'POST', token: 'operator-a-token', body: { goal: 'audited takeover' } });
  await request(base, `/api/runs/${run.data.runId}/lease`, { method: 'POST', token: 'worker-a-token', body: {} });
  const takeover = await request(base, `/api/runs/${run.data.runId}/lease`, {
    method: 'POST', token: 'operator-a-token', body: { force: true, reason: 'worker health check failed' },
  });
  const events = await request(base, `/api/runs/${run.data.runId}/events`, { token: 'operator-a-token' });
  const event = events.data.items.at(-1);

  assert.equal(takeover.response.status, 200);
  assert.equal(takeover.data.workerId, 'operator-worker-a');
  assert.equal(event.eventType, 'LEASE_ACQUIRED');
  assert.equal(event.actorId, 'operator-a');
  assert.equal(event.payload.previousLeaseOwner, 'worker-a');
  assert.equal(event.payload.takeoverReason, 'worker health check failed');
});

test('a worker cannot use another authenticated worker lease payload', async (t) => {
  const { base } = await fixture(t);
  const run = await request(base, '/api/runs', { method: 'POST', token: 'operator-a-token', body: { goal: 'bind lease' } });
  const lease = await request(base, `/api/runs/${run.data.runId}/lease`, { method: 'POST', token: 'worker-a-token', body: {} });
  const attack = await request(base, `/api/runs/${run.data.runId}/start`, {
    method: 'POST', token: 'worker-b-token', body: { lease: lease.data },
  });

  assert.equal(attack.response.status, 403);
  assert.equal(attack.data.error.code, 'WORKER_IDENTITY_MISMATCH');
});

test('development credentials are refused when the process environment is production', () => {
  assert.throws(
    () => createDevelopmentAuthenticator({ environment: { NODE_ENV: 'production' } }),
    (error) => error.code === 'AUTH_CONFIGURATION_REQUIRED',
  );
});

test('runtime authentication is fail-closed unless development access is explicitly enabled', () => {
  assert.throws(
    () => createRuntimeAuthenticator({}),
    (error) => error.code === 'AUTH_CONFIGURATION_REQUIRED',
  );
  const development = createRuntimeAuthenticator({ AGENT_RUNTIME_ALLOW_DEV_AUTH: '1' });
  assert.ok(development);
  const configured = createRuntimeAuthenticator({
    AGENT_RUNTIME_IDENTITIES: JSON.stringify(tokens),
  });
  assert.ok(configured);
});

test('worker lifecycle events use the authenticated subject rather than a caller-controlled worker label', async (t) => {
  const { base } = await fixture(t);
  const run = await request(base, '/api/runs', { method: 'POST', token: 'operator-a-token', body: { goal: 'audit subject' } });
  const lease = await request(base, `/api/runs/${run.data.runId}/lease`, { method: 'POST', token: 'worker-a-token', body: {} });
  await request(base, `/api/runs/${run.data.runId}/start`, {
    method: 'POST',
    token: 'worker-a-token',
    body: { lease: lease.data },
  });
  const events = await request(base, `/api/runs/${run.data.runId}/events`, { token: 'operator-a-token' });

  assert.equal(events.data.items.find((event) => event.eventType === 'RUN_STARTED').actorId, 'service-worker-a');
});
