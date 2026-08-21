import assert from 'node:assert/strict';
import test from 'node:test';
import { createDatabase } from '../src/db.js';
import { ToolRegistry, registerBuiltinTools } from '../src/tool-contracts.js';

function fixture() {
  const db = createDatabase(':memory:');
  const registry = new ToolRegistry(db);
  registerBuiltinTools(registry);
  return { db, registry };
}

test('side effect classification belongs to a versioned operation', () => {
  const { registry } = fixture();
  assert.equal(registry.get('echo', 1).sideEffect, 'READ_ONLY');
  assert.equal(registry.get('counter.increment', 1).sideEffect, 'SIDE_EFFECT');
  assert.equal(registry.get('effect.opaque', 1).sideEffect, 'UNKNOWN');
});

test('idempotency-key-required operation rejects a missing key', () => {
  const { registry } = fixture();
  assert.throws(
    () => registry.invoke({ tenantId: 't1', runId: 'r1', operationId: 'counter.increment', version: 1, input: { name: 'jobs', amount: 2 } }),
    (error) => error.code === 'IDEMPOTENCY_KEY_REQUIRED',
  );
});

test('same logical side effect and key returns committed result without duplication', () => {
  const { db, registry } = fixture();
  const call = { tenantId: 't1', runId: 'r1', operationId: 'counter.increment', version: 1, input: { name: 'jobs', amount: 2 }, idempotencyKey: 'r1:step1' };
  const first = registry.invoke(call);
  const second = registry.invoke(call);

  assert.equal(first.output.value, 2);
  assert.equal(second.output.value, 2);
  assert.equal(second.replayed, true);
  assert.equal(db.prepare("SELECT value FROM counters WHERE tenant_id = 't1' AND name = 'jobs'").get().value, 2);
});

test('idempotency results are isolated by tool contract version', () => {
  const { db, registry } = fixture();
  let versionOneCalls = 0;
  let versionTwoCalls = 0;
  registry.register(
    { operationId: 'versioned.write', version: 1, sideEffect: 'SIDE_EFFECT', idempotency: 'IDEMPOTENCY_KEY_REQUIRED', executionMode: 'LOCAL_TRANSACTIONAL' },
    () => ({ source: 'v1', call: ++versionOneCalls }),
  );
  registry.register(
    { operationId: 'versioned.write', version: 2, sideEffect: 'SIDE_EFFECT', idempotency: 'IDEMPOTENCY_KEY_REQUIRED', executionMode: 'LOCAL_TRANSACTIONAL' },
    () => ({ source: 'v2', call: ++versionTwoCalls }),
  );
  const common = { tenantId: 't1', runId: 'r1', operationId: 'versioned.write', input: { value: 7 }, idempotencyKey: 'same-business-key' };

  const first = registry.invoke({ ...common, version: 1 });
  const second = registry.invoke({ ...common, version: 2 });

  assert.deepEqual(first.output, { source: 'v1', call: 1 });
  assert.deepEqual(second.output, { source: 'v2', call: 1 });
  assert.equal(versionOneCalls, 1);
  assert.equal(versionTwoCalls, 1);
  assert.equal(db.prepare("SELECT count(*) AS count FROM tool_effects WHERE operation_id = 'versioned.write'").get().count, 2);
});

test('same idempotency key with changed business input is rejected', () => {
  const { registry } = fixture();
  registry.invoke({ tenantId: 't1', runId: 'r1', operationId: 'counter.increment', version: 1, input: { name: 'jobs', amount: 2 }, idempotencyKey: 'r1:step1' });

  assert.throws(
    () => registry.invoke({ tenantId: 't1', runId: 'r1', operationId: 'counter.increment', version: 1, input: { name: 'jobs', amount: 3 }, idempotencyKey: 'r1:step1' }),
    (error) => error.code === 'IDEMPOTENCY_CONFLICT',
  );
});

test('unknown side effect is conservatively blocked from automatic execution', () => {
  const { registry } = fixture();
  assert.throws(
    () => registry.invoke({ tenantId: 't1', runId: 'r1', operationId: 'effect.opaque', version: 1, input: {} }),
    (error) => error.code === 'UNKNOWN_SIDE_EFFECT',
  );
});

function seedLeasedRun(db) {
  db.prepare(`
    INSERT INTO runs
      (run_id, tenant_id, goal, status, execution_epoch, lease_id, lease_owner,
       lease_expires_at, state_version, created_at, updated_at)
    VALUES ('run-fenced', 't1', 'test fencing', 'RUNNING', 3, 'lease-current', 'worker-a',
      CAST(strftime('%s', 'now') AS INTEGER) + 60, 7, '2026-08-10T00:00:00.000Z', '2026-08-10T00:00:00.000Z')
  `).run();
}

const validRunFence = {
  tenantId: 't1',
  runId: 'run-fenced',
  leaseId: 'lease-current',
  workerId: 'worker-a',
  executionEpoch: 3,
  stateVersion: 7,
};

test('a stale run fence blocks a LOCAL_TRANSACTIONAL effect and its business write', () => {
  const { db, registry } = fixture();
  seedLeasedRun(db);

  assert.throws(
    () => registry.invoke({
      tenantId: 't1',
      runId: 'run-fenced',
      executionEpoch: 3,
      operationId: 'counter.increment',
      version: 1,
      input: { name: 'jobs', amount: 2 },
      idempotencyKey: 'stale-local',
      runFence: { ...validRunFence, leaseId: 'lease-stale' },
    }),
    (error) => error.code === 'FENCED_OUT',
  );

  assert.equal(db.prepare("SELECT count(*) AS count FROM tool_effects WHERE idempotency_key = 'stale-local'").get().count, 0);
  assert.equal(db.prepare("SELECT count(*) AS count FROM counters WHERE tenant_id = 't1' AND name = 'jobs'").get().count, 0);
});

test('a stale run fence blocks an EXTERNAL_OUTBOX intent before dispatch', async () => {
  const db = createDatabase(':memory:');
  seedLeasedRun(db);
  let calls = 0;
  const registry = new ToolRegistry(db);
  registry.register({
    operationId: 'external.fenced',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => { calls += 1; return { accepted: true }; });

  await assert.rejects(Promise.resolve().then(() => registry.invoke({
    tenantId: 't1',
    runId: 'run-fenced',
    executionEpoch: 3,
    operationId: 'external.fenced',
    version: 1,
    input: { command: 'send' },
    idempotencyKey: 'stale-external',
    runFence: { ...validRunFence, stateVersion: 6 },
  })), (error) => error.code === 'FENCED_OUT');

  assert.equal(calls, 0);
  assert.equal(db.prepare("SELECT count(*) AS count FROM tool_effects WHERE idempotency_key = 'stale-external'").get().count, 0);
  assert.equal(db.prepare('SELECT count(*) AS count FROM tool_effect_outbox').get().count, 0);
});

test('legacy effect contracts deterministically remain LOCAL_TRANSACTIONAL', () => {
  const db = createDatabase(':memory:');
  const legacy = {
    operationId: 'legacy.write',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    maxAttempts: 1,
    timeoutMs: 30_000,
  };
  db.prepare('INSERT INTO tool_contracts (operation_id, version, contract_json, created_at) VALUES (?, ?, ?, ?)')
    .run(legacy.operationId, legacy.version, JSON.stringify(legacy), '2026-08-10T00:00:00.000Z');
  const registry = new ToolRegistry(db);

  assert.equal(registry.get('legacy.write', 1).executionMode, 'LOCAL_TRANSACTIONAL');
  assert.throws(
    () => registry.register({ ...legacy, executionMode: 'EXTERNAL_OUTBOX' }, () => ({ accepted: true })),
    (error) => error.code === 'TOOL_CONTRACT_IMMUTABLE',
  );
  assert.equal(JSON.parse(db.prepare("SELECT contract_json FROM tool_contracts WHERE operation_id = 'legacy.write'").get().contract_json).executionMode, undefined);

  registry.register(legacy, () => ({ applied: true }));
  assert.equal(JSON.parse(db.prepare("SELECT contract_json FROM tool_contracts WHERE operation_id = 'legacy.write'").get().contract_json).executionMode, 'LOCAL_TRANSACTIONAL');
});

test('manual effect resolution accepts UNKNOWN only and atomically CASes effect and outbox', () => {
  const { db, registry } = fixture();
  const timestamp = '2026-08-10T00:00:00.000Z';
  db.prepare(`
    INSERT INTO tool_effects
      (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
       input_hash, input_json, execution_epoch, execution_mode, status, created_at, updated_at)
    VALUES ('unknown-effect', 't1', 'r1', 'external.manual', 1, 'manual-key',
      'hash', '{}', 2, 'EXTERNAL_OUTBOX', 'UNKNOWN', ?, ?)
  `).run(timestamp, timestamp);
  db.prepare(`
    INSERT INTO tool_effect_outbox
      (outbox_id, effect_id, status, attempt_count, payload_json, created_at, updated_at)
    VALUES ('unknown-outbox', 'unknown-effect', 'UNKNOWN', 1, '{}', ?, ?)
  `).run(timestamp, timestamp);

  const resolved = db.transaction(() => registry.resolveEffectInTransaction({
    tenantId: 't1',
    runId: 'r1',
    effectId: 'unknown-effect',
    outcome: 'FAILED',
    error: { message: 'confirmed not applied' },
  }));
  assert.equal(resolved.status, 'FAILED');
  assert.equal(db.prepare("SELECT status FROM tool_effects WHERE effect_id = 'unknown-effect'").get().status, 'FAILED');
  assert.equal(db.prepare("SELECT status FROM tool_effect_outbox WHERE effect_id = 'unknown-effect'").get().status, 'FAILED');

  assert.throws(
    () => registry.resolveEffect({ tenantId: 't1', runId: 'r1', effectId: 'unknown-effect', outcome: 'COMMITTED', output: {} }),
    (error) => error.code === 'TOOL_EFFECT_ALREADY_RESOLVED',
  );
});

test('manual resolution refuses active dispatch and rolls back when the outbox CAS loses', () => {
  const { db, registry } = fixture();
  const timestamp = '2026-08-10T00:00:00.000Z';
  const insertEffect = db.prepare(`
    INSERT INTO tool_effects
      (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
       input_hash, input_json, execution_epoch, execution_mode, status, created_at, updated_at)
    VALUES (?, 't1', 'r1', 'external.manual', 1, ?, 'hash', '{}', 2,
      'EXTERNAL_OUTBOX', ?, ?, ?)
  `);
  const insertOutbox = db.prepare(`
    INSERT INTO tool_effect_outbox
      (outbox_id, effect_id, status, attempt_count, payload_json, created_at, updated_at)
    VALUES (?, ?, ?, 1, '{}', ?, ?)
  `);
  insertEffect.run('active-effect', 'active-key', 'DISPATCHING', timestamp, timestamp);
  insertOutbox.run('active-outbox', 'active-effect', 'DISPATCHING', timestamp, timestamp);
  assert.throws(
    () => registry.resolveEffect({ tenantId: 't1', runId: 'r1', effectId: 'active-effect', outcome: 'FAILED' }),
    (error) => error.code === 'TOOL_EFFECT_STILL_ACTIVE',
  );

  insertEffect.run('lost-cas-effect', 'lost-cas-key', 'UNKNOWN', timestamp, timestamp);
  insertOutbox.run('lost-cas-outbox', 'lost-cas-effect', 'DELIVERED', timestamp, timestamp);
  assert.throws(
    () => registry.resolveEffect({ tenantId: 't1', runId: 'r1', effectId: 'lost-cas-effect', outcome: 'FAILED' }),
    (error) => error.code === 'TOOL_EFFECT_RESOLUTION_CONFLICT',
  );
  assert.equal(db.prepare("SELECT status FROM tool_effects WHERE effect_id = 'lost-cas-effect'").get().status, 'UNKNOWN');
  assert.equal(db.prepare("SELECT status FROM tool_effect_outbox WHERE effect_id = 'lost-cas-effect'").get().status, 'DELIVERED');
});
