import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { createDatabase } from '../src/db.js';
import { ContextStore } from '../src/context-store.js';
import { ToolRegistry } from '../src/tool-contracts.js';
import { Runtime } from '../src/runtime.js';

function externalFixture(executor) {
  const db = createDatabase(':memory:');
  const registry = new ToolRegistry(db);
  registry.register({
    operationId: 'external.send',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, executor);
  return { db, registry };
}

const invocation = {
  tenantId: 't1',
  runId: 'r1',
  executionEpoch: 7,
  operationId: 'external.send',
  version: 1,
  input: { recipient: 'customer-7', message: 'ready' },
  idempotencyKey: 'send-command-7',
};

test('external success followed by timeout persists UNKNOWN and is never blindly redelivered', async () => {
  let externalWrites = 0;
  let adapterContext;
  const { db, registry } = externalFixture(async (context) => {
    adapterContext = context;
    externalWrites += 1;
    throw new Error('response timed out after the remote system accepted the request');
  });

  await assert.rejects(registry.invoke(invocation), (error) => error.code === 'EFFECT_STATUS_UNKNOWN');
  const effect = db.prepare('SELECT effect_id, status FROM tool_effects WHERE idempotency_key = ?').get(invocation.idempotencyKey);
  const outbox = db.prepare('SELECT status, attempt_count FROM tool_effect_outbox WHERE effect_id = ?').get(effect.effect_id);
  assert.equal(effect.status, 'UNKNOWN');
  assert.deepEqual({ ...outbox }, { status: 'UNKNOWN', attempt_count: 1 });
  assert.equal(adapterContext.effectId, effect.effect_id);
  assert.equal(adapterContext.idempotencyKey, invocation.idempotencyKey);

  await assert.rejects(registry.invoke(invocation), (error) => error.code === 'EFFECT_STATUS_UNKNOWN');
  assert.equal(externalWrites, 1);
});

test('intent and outbox are durable before an external adapter is called', async () => {
  let observed;
  const { db, registry } = externalFixture(() => {
    observed = {
      effect: { ...db.prepare('SELECT status, input_json FROM tool_effects WHERE idempotency_key = ?').get(invocation.idempotencyKey) },
      outbox: { ...db.prepare('SELECT status, attempt_count FROM tool_effect_outbox').get() },
    };
    return { accepted: true };
  });

  await registry.invoke(invocation);

  assert.deepEqual(observed, {
    effect: { status: 'DISPATCHING', input_json: JSON.stringify(invocation.input) },
    outbox: { status: 'DISPATCHING', attempt_count: 1 },
  });
});

test('committed external result is replayed without invoking the adapter twice', async () => {
  let calls = 0;
  const { db, registry } = externalFixture(async () => ({ receipt: `receipt-${++calls}` }));

  const first = await registry.invoke(invocation);
  const replay = await registry.invoke(invocation);

  assert.deepEqual(first.output, { receipt: 'receipt-1' });
  assert.deepEqual(replay.output, { receipt: 'receipt-1' });
  assert.equal(replay.replayed, true);
  assert.equal(calls, 1);
  assert.equal(db.prepare('SELECT status FROM tool_effect_outbox').get().status, 'DELIVERED');
});

test('an explicit not-applied rejection persists FAILED and is not redelivered', async () => {
  let calls = 0;
  const rejection = new Error('remote validation rejected the request before execution');
  rejection.effectOutcome = 'FAILED';
  const { db, registry } = externalFixture(async () => {
    calls += 1;
    throw rejection;
  });

  await assert.rejects(registry.invoke(invocation), (error) => error.code === 'EFFECT_EXECUTION_FAILED');
  await assert.rejects(registry.invoke(invocation), (error) => error.code === 'EFFECT_EXECUTION_FAILED');

  assert.equal(calls, 1);
  assert.equal(db.prepare('SELECT status FROM tool_effects').get().status, 'FAILED');
  assert.equal(db.prepare('SELECT status FROM tool_effect_outbox').get().status, 'FAILED');
});

test('registry construction leaves a live dispatch alone until a newer epoch fences it', () => {
  const directory = mkdtempSync(join(tmpdir(), 'agent-runtime-effect-restart-'));
  const path = join(directory, 'runtime.db');
  const db = createDatabase(path);
  db.prepare(`
    INSERT INTO tool_effects
      (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
       input_hash, input_json, execution_epoch, execution_mode, status, created_at, updated_at)
    VALUES ('interrupted-effect', 't1', 'r1', 'external.send', 1, 'restart-key',
      'hash', '{}', 3, 'EXTERNAL_OUTBOX', 'DISPATCHING', '2026-08-10T00:00:00.000Z', '2026-08-10T00:00:00.000Z')
  `).run();
  db.prepare(`
    INSERT INTO tool_effect_outbox
      (outbox_id, effect_id, status, attempt_count, payload_json, created_at, updated_at)
    VALUES ('outbox-1', 'interrupted-effect', 'DISPATCHING', 1, '{}', '2026-08-10T00:00:00.000Z', '2026-08-10T00:00:00.000Z')
  `).run();
  db.close();

  const reopened = createDatabase(path);
  const registry = new ToolRegistry(reopened);

  assert.equal(reopened.prepare("SELECT status FROM tool_effects WHERE effect_id = 'interrupted-effect'").get().status, 'DISPATCHING');
  assert.equal(reopened.prepare("SELECT status FROM tool_effect_outbox WHERE effect_id = 'interrupted-effect'").get().status, 'DISPATCHING');

  const fenced = reopened.transaction(() => registry.fenceInterruptedEffects('r1', 4));
  assert.equal(fenced, 1);
  assert.equal(reopened.prepare("SELECT status FROM tool_effects WHERE effect_id = 'interrupted-effect'").get().status, 'UNKNOWN');
  assert.equal(reopened.prepare("SELECT status FROM tool_effect_outbox WHERE effect_id = 'interrupted-effect'").get().status, 'UNKNOWN');
  reopened.close();
});

test('constructing another registry cannot clobber an in-flight dispatcher', async () => {
  let release;
  let entered;
  const enteredPromise = new Promise((resolve) => { entered = resolve; });
  const releasePromise = new Promise((resolve) => { release = resolve; });
  const { db, registry } = externalFixture(async () => {
    entered();
    await releasePromise;
    return { accepted: true };
  });

  const pending = registry.invoke(invocation);
  await enteredPromise;
  new ToolRegistry(db);
  assert.equal(db.prepare('SELECT status FROM tool_effects').get().status, 'DISPATCHING');

  release();
  const result = await pending;
  assert.deepEqual(result.output, { accepted: true });
  assert.equal(db.prepare('SELECT status FROM tool_effects').get().status, 'COMMITTED');
  assert.equal(db.prepare('SELECT status FROM tool_effect_outbox').get().status, 'DELIVERED');
});

test('a newer execution epoch wins over a successful old external dispatcher', async () => {
  let release;
  let entered;
  const enteredPromise = new Promise((resolve) => { entered = resolve; });
  const releasePromise = new Promise((resolve) => { release = resolve; });
  const { db, registry } = externalFixture(async () => {
    entered();
    await releasePromise;
    return { accepted: true };
  });

  const pending = registry.invoke(invocation);
  await enteredPromise;
  db.transaction(() => registry.fenceInterruptedEffects(invocation.runId, invocation.executionEpoch + 1));
  release();

  await assert.rejects(pending, (error) => error.code === 'EFFECT_STATUS_UNKNOWN');
  assert.equal(db.prepare('SELECT status FROM tool_effects').get().status, 'UNKNOWN');
  assert.equal(db.prepare('SELECT status FROM tool_effect_outbox').get().status, 'UNKNOWN');
});

test('a newer execution epoch prevents an old explicit failure from overwriting UNKNOWN', async () => {
  let release;
  let entered;
  const enteredPromise = new Promise((resolve) => { entered = resolve; });
  const releasePromise = new Promise((resolve) => { release = resolve; });
  const rejection = new Error('rejected before execution');
  rejection.effectOutcome = 'FAILED';
  const { db, registry } = externalFixture(async () => {
    entered();
    await releasePromise;
    throw rejection;
  });

  const pending = registry.invoke(invocation);
  await enteredPromise;
  db.transaction(() => registry.fenceInterruptedEffects(invocation.runId, invocation.executionEpoch + 1));
  release();

  await assert.rejects(pending, (error) => error.code === 'EFFECT_STATUS_UNKNOWN');
  assert.equal(db.prepare('SELECT status FROM tool_effects').get().status, 'UNKNOWN');
  assert.equal(db.prepare('SELECT status FROM tool_effect_outbox').get().status, 'UNKNOWN');
});

test('manual reconciliation is required before an UNKNOWN effect run can resume', async () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  tools.register({
    operationId: 'external.timeout',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => {
    throw new Error('remote outcome unavailable');
  });
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'reconcile effect' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);

  await assert.rejects(runtime.invokeTool(run.runId, lease, {
    operationId: 'external.timeout',
    version: 1,
    input: { command: 'send' },
    idempotencyKey: 'unknown-command',
  }), (error) => error.code === 'EFFECT_STATUS_UNKNOWN');
  const effectId = db.prepare("SELECT effect_id FROM tool_effects WHERE idempotency_key = 'unknown-command'").get().effect_id;
  assert.equal(runtime.getRun(run.runId, 't1').status, 'WAITING_MANUAL');
  assert.throws(
    () => runtime.resolveManual(run.runId, { tenantId: 't1', actorId: 'operator-1', decision: 'RESUME', reason: 'attempted before effect reconciliation' }),
    (error) => error.code === 'PENDING_EFFECT_RESOLUTION_REQUIRED',
  );

  runtime.resolveToolEffect(run.runId, {
    tenantId: 't1',
    actorId: 'operator-1',
    effectId,
    outcome: 'FAILED',
    reason: 'downstream confirmed the request was not applied',
  });
  const resolved = runtime.resolveManual(run.runId, {
    tenantId: 't1',
    actorId: 'operator-1',
    decision: 'RESUME',
    reason: 'effect outcome is now known',
  });

  assert.equal(resolved.status, 'RECOVERING');
  assert.equal(db.prepare('SELECT status FROM tool_effects WHERE effect_id = ?').get(effectId).status, 'FAILED');
  assert.equal(runtime.listEvents(run.runId, 't1').some((event) => event.eventType === 'TOOL_EFFECT_RESOLVED' && event.payload.effectId === effectId), true);
});

test('an explicitly rejected external effect fails the run with an auditable event', async () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  const rejection = new Error('request rejected before execution');
  rejection.effectOutcome = 'FAILED';
  tools.register({
    operationId: 'external.reject',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => { throw rejection; });
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'fail on rejection' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);

  await assert.rejects(runtime.invokeTool(run.runId, lease, {
    operationId: 'external.reject',
    version: 1,
    input: { command: 'reject' },
    idempotencyKey: 'rejected-command',
  }), (error) => error.code === 'EFFECT_EXECUTION_FAILED');

  assert.equal(runtime.getRun(run.runId, 't1').status, 'FAILED');
  assert.equal(runtime.listEvents(run.runId, 't1').at(-1).eventType, 'RUN_FAILED');
});

test('replaying a FAILED effect from another Run also fails the current Run', async () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  let calls = 0;
  tools.register({
    operationId: 'external.shared-rejection',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => {
    calls += 1;
    const rejection = new Error('request rejected before execution');
    rejection.effectOutcome = 'FAILED';
    throw rejection;
  });
  const runtime = new Runtime(db, { contexts, tools });
  const invoke = (runId, lease) => runtime.invokeTool(runId, lease, {
    operationId: 'external.shared-rejection',
    version: 1,
    input: { command: 'same' },
    idempotencyKey: 'shared-rejection',
  });

  const runA = runtime.createRun({ tenantId: 't1', goal: 'first rejection' });
  const leaseA = runtime.acquireLease(runA.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(runA.runId, leaseA);
  await assert.rejects(invoke(runA.runId, leaseA), (error) => error.code === 'EFFECT_EXECUTION_FAILED');

  const runB = runtime.createRun({ tenantId: 't1', goal: 'replay rejection' });
  const leaseB = runtime.acquireLease(runB.runId, { tenantId: 't1', workerId: 'worker-b' });
  runtime.start(runB.runId, leaseB);
  await assert.rejects(invoke(runB.runId, leaseB), (error) => error.code === 'EFFECT_EXECUTION_FAILED');

  assert.equal(calls, 1);
  assert.equal(runtime.getRun(runB.runId, 't1').status, 'FAILED');
  const failure = runtime.listEvents(runB.runId, 't1').at(-1);
  assert.equal(failure.eventType, 'RUN_FAILED');
  assert.equal(typeof failure.payload.effectId, 'string');
  assert.throws(
    () => runtime.checkpoint(runB.runId, leaseB, { kind: 'FULL', state: { mustNotCommit: true } }),
    (error) => error.code === 'FENCED_OUT',
  );
});

test('takeover that fences an active external effect commits a manual gate instead of a usable lease', async () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  let entered;
  let release;
  const enteredPromise = new Promise((resolve) => { entered = resolve; });
  const releasePromise = new Promise((resolve) => { release = resolve; });
  tools.register({
    operationId: 'external.gated',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => {
    entered();
    await releasePromise;
    return { delivered: true };
  });
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'block on interrupted effect' });
  const oldLease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, oldLease);
  const invocation = runtime.invokeTool(run.runId, oldLease, {
    operationId: 'external.gated',
    version: 1,
    input: { command: 'send' },
    idempotencyKey: 'gated-takeover',
  });
  await enteredPromise;

  assert.throws(
    () => runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-b', force: true }),
    (error) => error.code === 'EFFECT_STATUS_UNKNOWN',
  );
  const blocked = runtime.getRun(run.runId, 't1');
  assert.equal(blocked.status, 'WAITING_MANUAL');
  assert.equal(blocked.leaseOwner, null);
  assert.equal(db.prepare("SELECT status FROM tool_effects WHERE idempotency_key = 'gated-takeover'").get().status, 'UNKNOWN');

  release();
  await assert.rejects(invocation, (error) => error.code === 'FENCED_OUT');
});

test('a Run cannot reach a terminal state while an external effect is still active', async () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  let entered;
  let release;
  const enteredPromise = new Promise((resolve) => { entered = resolve; });
  const releasePromise = new Promise((resolve) => { release = resolve; });
  tools.register({
    operationId: 'external.finish-before-run',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => {
    entered();
    await releasePromise;
    return { delivered: true };
  });
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'await external effect' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  const invocation = runtime.invokeTool(run.runId, lease, {
    operationId: 'external.finish-before-run',
    version: 1,
    input: { command: 'send' },
    idempotencyKey: 'finish-before-run',
  });
  await enteredPromise;

  assert.throws(
    () => runtime.complete(run.runId, lease, { done: true }),
    (error) => error.code === 'PENDING_EFFECTS',
  );
  assert.equal(runtime.getRun(run.runId, 't1').status, 'RUNNING');

  release();
  await invocation;
  assert.equal(runtime.complete(run.runId, lease, { done: true }).status, 'SUCCEEDED');
  assert.equal(runtime.listEvents(run.runId, 't1').some((event) => event.eventType === 'TOOL_COMMITTED'), true);
});

test('an active effect prevents a concurrent rejecting effect from prematurely failing the Run', async () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  let entered;
  let release;
  let rejectingCalls = 0;
  const enteredPromise = new Promise((resolve) => { entered = resolve; });
  const releasePromise = new Promise((resolve) => { release = resolve; });
  tools.register({
    operationId: 'external.slow-a',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => {
    entered();
    await releasePromise;
    return { delivered: true };
  });
  tools.register({
    operationId: 'external.reject-b',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => {
    rejectingCalls += 1;
    const rejection = new Error('known rejection');
    rejection.effectOutcome = 'FAILED';
    throw rejection;
  });
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'serialize external effects' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  const slow = runtime.invokeTool(run.runId, lease, {
    operationId: 'external.slow-a',
    version: 1,
    input: { command: 'slow' },
    idempotencyKey: 'slow-a',
  });
  await enteredPromise;

  assert.throws(
    () => runtime.invokeTool(run.runId, lease, {
      operationId: 'external.reject-b',
      version: 1,
      input: { command: 'reject' },
      idempotencyKey: 'reject-b',
    }),
    (error) => error.code === 'PENDING_EFFECTS',
  );
  assert.equal(rejectingCalls, 0);
  assert.equal(runtime.getRun(run.runId, 't1').status, 'RUNNING');

  release();
  await slow;
  assert.equal(runtime.complete(run.runId, lease, { done: true }).status, 'SUCCEEDED');
});
