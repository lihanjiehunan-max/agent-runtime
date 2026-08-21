import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { createDatabase } from '../src/db.js';
import { ContextStore } from '../src/context-store.js';
import { ToolRegistry, registerBuiltinTools } from '../src/tool-contracts.js';
import { Runtime } from '../src/runtime.js';

function compose(db) {
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  registerBuiltinTools(tools);
  return new Runtime(db, { contexts, tools });
}

function interceptOnce(db, pattern, beforeResult) {
  let armed = true;
  return new Proxy(db, {
    get(target, property) {
      if (property === 'prepare') {
        return (sql) => {
          const statement = target.prepare(sql);
          if (!armed || !sql.includes(pattern)) return statement;
          const invoke = (method) => (...args) => {
            armed = false;
            beforeResult();
            return statement[method](...args);
          };
          return new Proxy(statement, {
            get(statementTarget, statementProperty) {
              if (statementProperty === 'get' || statementProperty === 'all') return invoke(statementProperty);
              const value = Reflect.get(statementTarget, statementProperty, statementTarget);
              return typeof value === 'function' ? value.bind(statementTarget) : value;
            },
          });
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function interceptNth(db, pattern, occurrence, beforeResult) {
  let seen = 0;
  return new Proxy(db, {
    get(target, property) {
      if (property === 'prepare') {
        return (sql) => {
          const statement = target.prepare(sql);
          if (!sql.includes(pattern)) return statement;
          seen += 1;
          if (seen !== occurrence) return statement;
          const invoke = (method) => (...args) => {
            beforeResult();
            return statement[method](...args);
          };
          return new Proxy(statement, {
            get(statementTarget, statementProperty) {
              if (statementProperty === 'get' || statementProperty === 'all') return invoke(statementProperty);
              const value = Reflect.get(statementTarget, statementProperty, statementTarget);
              return typeof value === 'function' ? value.bind(statementTarget) : value;
            },
          });
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function interceptAfterOnce(db, pattern, afterResult) {
  let armed = true;
  return new Proxy(db, {
    get(target, property) {
      if (property === 'prepare') {
        return (sql) => {
          const statement = target.prepare(sql);
          if (!armed || !sql.includes(pattern)) return statement;
          const invoke = (method) => (...args) => {
            const result = statement[method](...args);
            armed = false;
            afterResult();
            return result;
          };
          return new Proxy(statement, {
            get(statementTarget, statementProperty) {
              if (statementProperty === 'get' || statementProperty === 'all') return invoke(statementProperty);
              const value = Reflect.get(statementTarget, statementProperty, statementTarget);
              return typeof value === 'function' ? value.bind(statementTarget) : value;
            },
          });
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function failEventAppend(db, eventType) {
  return new Proxy(db, {
    get(target, property) {
      if (property === 'prepare') {
        return (sql) => {
          const statement = target.prepare(sql);
          if (!sql.includes('INSERT INTO run_events')) return statement;
          return new Proxy(statement, {
            get(statementTarget, statementProperty) {
              if (statementProperty === 'run') {
                return (...args) => {
                  if (args[4] === eventType) throw new Error(`injected ${eventType} audit failure`);
                  return statementTarget.run(...args);
                };
              }
              const value = Reflect.get(statementTarget, statementProperty, statementTarget);
              return typeof value === 'function' ? value.bind(statementTarget) : value;
            },
          });
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function interceptTransactionOnce(db, beforeTransaction) {
  let armed = true;
  return new Proxy(db, {
    get(target, property) {
      if (property === 'transaction') {
        return (work) => {
          if (armed) {
            armed = false;
            beforeTransaction();
          }
          return target.transaction(work);
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function failNthTransaction(db, occurrence, message) {
  let seen = 0;
  return new Proxy(db, {
    get(target, property) {
      if (property === 'transaction') {
        return (work) => {
          seen += 1;
          if (seen === occurrence) throw new Error(message);
          return target.transaction(work);
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

test('database restart preserves checkpoint recovery and fences the previous owner', () => {
  const directory = mkdtempSync(join(tmpdir(), 'agent-runtime-'));
  const path = join(directory, 'runtime.db');
  const db1 = createDatabase(path);
  const runtime1 = compose(db1);
  const run = runtime1.createRun({ tenantId: 't1', goal: 'survive restart' });
  const oldLease = runtime1.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime1.start(run.runId, oldLease);
  runtime1.checkpoint(run.runId, oldLease, { kind: 'FULL', state: { completed: ['prepare'] } });
  db1.close();

  const db2 = createDatabase(path);
  const runtime2 = compose(db2);
  const recovered = runtime2.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' });
  assert.deepEqual(recovered.state, { completed: ['prepare'] });
  assert.equal(recovered.executionEpoch, oldLease.executionEpoch + 1);
  assert.throws(() => runtime2.complete(run.runId, oldLease, {}), (error) => error.code === 'FENCED_OUT');
  db2.close();
});

test('corrupted checkpoint content blocks recovery instead of skipping the damage', () => {
  const db = createDatabase(':memory:');
  const runtime = compose(db);
  const run = runtime.createRun({ tenantId: 't1', goal: 'detect corruption' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { phase: 'safe' } });
  db.prepare("UPDATE checkpoints SET state_json = '{\"phase\":\"tampered\"}' WHERE run_id = ?").run(run.runId);

  assert.throws(
    () => runtime.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' }),
    (error) => error.code === 'CHECKPOINT_CORRUPT',
  );
  assert.equal(runtime.getRun(run.runId, 't1').status, 'RECOVERY_BLOCKED');
});

for (const mutation of [
  { name: 'schema version', sql: 'UPDATE checkpoints SET schema_version = schema_version + 1 WHERE run_id = ?' },
  { name: 'event sequence', sql: 'UPDATE checkpoints SET event_seq = event_seq + 100 WHERE run_id = ?' },
  { name: 'execution epoch', sql: 'UPDATE checkpoints SET execution_epoch = execution_epoch + 1 WHERE run_id = ?' },
  { name: 'checkpoint kind', sql: "UPDATE checkpoints SET kind = 'DELTA' WHERE run_id = ?" },
  { name: 'checkpoint status', sql: "UPDATE checkpoints SET status = 'INVALIDATED_CONTEXT_REVOKED' WHERE run_id = ?" },
]) {
  test(`checkpoint ${mutation.name} tampering blocks recovery`, () => {
    const db = createDatabase(':memory:');
    const runtime = compose(db);
    const run = runtime.createRun({ tenantId: 't1', goal: 'detect metadata corruption' });
    const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
    runtime.start(run.runId, lease);
    runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { phase: 'safe' } });
    db.prepare(mutation.sql).run(run.runId);

    assert.throws(
      () => runtime.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' }),
      (error) => error.code === 'CHECKPOINT_CORRUPT',
    );
  });
}

test('checkpoint event payload tampering blocks recovery', () => {
  const db = createDatabase(':memory:');
  const runtime = compose(db);
  const run = runtime.createRun({ tenantId: 't1', goal: 'cross-check checkpoint event' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { phase: 'safe' } });
  db.prepare("UPDATE run_events SET payload_json = '{\"checkpointId\":\"cp_forged\",\"kind\":\"FULL\"}' WHERE run_id = ? AND event_type = 'CHECKPOINT_COMMITTED'").run(run.runId);

  assert.throws(
    () => runtime.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' }),
    (error) => error.code === 'CHECKPOINT_CORRUPT',
  );
});

test('a stale recovery cannot write a pending-effect block after a newer takeover', () => {
  const db = createDatabase(':memory:');
  const runtime = compose(db);
  const run = runtime.createRun({ tenantId: 't1', goal: 'fence pending recovery' });
  const first = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, first);
  db.prepare(`
    INSERT INTO tool_effects
      (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
       input_hash, status, created_at, updated_at)
    VALUES ('pending-effect', 't1', ?, 'external.write', 1, 'pending-key', 'hash', 'PREPARED',
      '2026-08-10T00:00:00.000Z', '2026-08-10T00:00:00.000Z')
  `).run(run.runId);
  let newerRun;
  runtime.db = interceptNth(db, 'SELECT count(*) AS count FROM tool_effects', 2, () => {
    assert.throws(
      () => runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-b', force: true }),
      (error) => error.code === 'EFFECT_STATUS_UNKNOWN',
    );
    newerRun = runtime.getRun(run.runId, 't1');
  });

  assert.throws(
    () => runtime.recover(run.runId, { tenantId: 't1', workerId: 'recovering-worker' }),
    (error) => error.code === 'FENCED_OUT',
  );
  const current = runtime.getRun(run.runId, 't1');
  assert.equal(current.status, 'WAITING_MANUAL');
  assert.equal(current.executionEpoch, newerRun.executionEpoch);
  assert.equal(current.leaseOwner, null);
  assert.equal(runtime.listEvents(run.runId, 't1').some((event) => event.eventType === 'RECOVERY_BLOCKED' && event.executionEpoch < newerRun.executionEpoch), false);
});

test('a stale recovery cannot write RECOVERY_BLOCKED after a newer takeover', () => {
  const db = createDatabase(':memory:');
  const runtime = compose(db);
  const run = runtime.createRun({ tenantId: 't1', goal: 'fence corrupt recovery' });
  const first = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, first);
  runtime.checkpoint(run.runId, first, { kind: 'FULL', state: { safe: true } });
  db.prepare("UPDATE checkpoints SET state_json = '{\"tampered\":true}' WHERE run_id = ?").run(run.runId);
  let newerLease;
  runtime.db = interceptOnce(db, 'SELECT * FROM checkpoints WHERE run_id = ? ORDER BY checkpoint_seq', () => {
    newerLease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-b', force: true });
  });

  assert.throws(
    () => runtime.recover(run.runId, { tenantId: 't1', workerId: 'recovering-worker' }),
    (error) => error.code === 'FENCED_OUT',
  );
  const current = runtime.getRun(run.runId, 't1');
  assert.equal(current.status, 'RUNNING');
  assert.equal(current.executionEpoch, newerLease.executionEpoch);
  assert.equal(current.leaseOwner, 'worker-b');
  assert.equal(runtime.listEvents(run.runId, 't1').some((event) => event.eventType === 'RECOVERY_BLOCKED' && event.executionEpoch < newerLease.executionEpoch), false);
});

test('duplicate delivery of an idempotent side effect changes business state once', () => {
  const db = createDatabase(':memory:');
  const runtime = compose(db);
  const run = runtime.createRun({ tenantId: 't1', goal: 'dedupe command' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  const invocation = { operationId: 'counter.increment', version: 1, input: { name: 'sent', amount: 1 }, idempotencyKey: 'logical-command-1' };
  runtime.invokeTool(run.runId, lease, invocation);
  runtime.invokeTool(run.runId, lease, invocation);

  assert.equal(db.prepare("SELECT value FROM counters WHERE tenant_id = 't1' AND name = 'sent'").get().value, 1);
  assert.equal(db.prepare("SELECT count(*) AS count FROM tool_effects WHERE idempotency_key = 'logical-command-1'").get().count, 1);
});

test('recovery cannot overwrite a checkpoint committed after its state-version snapshot', () => {
  const db = createDatabase(':memory:');
  const runtime = compose(db);
  const run = runtime.createRun({ tenantId: 't1', goal: 'preserve concurrent checkpoint' });
  const first = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, first);
  runtime.checkpoint(run.runId, first, { kind: 'FULL', state: { phase: 'old' } });
  let recoveryLease;
  runtime.db = interceptAfterOnce(db, "SELECT * FROM run_events WHERE run_id = ? AND event_type = 'CHECKPOINT_COMMITTED'", () => {
    const current = db.prepare('SELECT * FROM runs WHERE run_id = ?').get(run.runId);
    recoveryLease = {
      runId: run.runId,
      tenantId: 't1',
      workerId: current.lease_owner,
      leaseId: current.lease_id,
      executionEpoch: current.execution_epoch,
      leaseExpiresAt: current.lease_expires_at,
    };
    runtime.checkpoint(run.runId, recoveryLease, { kind: 'DELTA', state: { phase: 'concurrent-newer' } });
  });

  assert.throws(
    () => runtime.recover(run.runId, { tenantId: 't1', workerId: 'recovering-worker' }),
    (error) => error.code === 'FENCED_OUT',
  );
  assert.deepEqual(runtime.getRun(run.runId, 't1').runtimeState, { phase: 'concurrent-newer' });
  assert.equal(runtime.listEvents(run.runId, 't1').some((event) => event.eventType === 'RUN_RECOVERED' && event.executionEpoch === recoveryLease.executionEpoch), false);
});

test('takeover between runtime verification and a local effect transaction prevents every stale write', () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  registerBuiltinTools(tools);
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'fence stale local effect' });
  const staleLease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, staleLease);
  let takeover;
  runtime.tools = new Proxy(tools, {
    get(target, property, receiver) {
      if (property !== 'invoke') return Reflect.get(target, property, receiver);
      return (args) => {
        takeover = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-b', force: true });
        return target.invoke(args);
      };
    },
  });

  assert.throws(
    () => runtime.invokeTool(run.runId, staleLease, {
      operationId: 'counter.increment',
      version: 1,
      input: { name: 'must-not-change', amount: 1 },
      idempotencyKey: 'stale-local-effect',
    }),
    (error) => error.code === 'FENCED_OUT',
  );
  assert.equal(takeover.executionEpoch, staleLease.executionEpoch + 1);
  assert.equal(db.prepare("SELECT count(*) AS count FROM counters WHERE name = 'must-not-change'").get().count, 0);
  assert.equal(db.prepare("SELECT count(*) AS count FROM tool_effects WHERE idempotency_key = 'stale-local-effect'").get().count, 0);
});

test('checkpoint event tenant tampering blocks recovery', () => {
  const db = createDatabase(':memory:');
  const runtime = compose(db);
  const run = runtime.createRun({ tenantId: 't1', goal: 'validate checkpoint tenant' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { safe: true } });
  db.prepare("UPDATE run_events SET tenant_id = 't2' WHERE run_id = ? AND event_type = 'CHECKPOINT_COMMITTED'").run(run.runId);

  assert.throws(
    () => runtime.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' }),
    (error) => error.code === 'CHECKPOINT_CORRUPT',
  );
});

test('manual effect outcome and its authenticated audit event commit or roll back together', () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  registerBuiltinTools(tools);
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'atomically reconcile effect' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  db.prepare("UPDATE runs SET status = 'WAITING_MANUAL' WHERE run_id = ?").run(run.runId);
  db.prepare(`
    INSERT INTO tool_effects
      (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
       input_hash, input_json, execution_epoch, execution_mode, status, created_at, updated_at)
    VALUES ('unknown-effect', 't1', ?, 'external.send', 1, 'unknown-key',
      'hash', '{}', ?, 'EXTERNAL_OUTBOX', 'UNKNOWN',
      '2026-08-10T00:00:00.000Z', '2026-08-10T00:00:00.000Z')
  `).run(run.runId, lease.executionEpoch);
  db.prepare(`
    INSERT INTO tool_effect_outbox
      (outbox_id, effect_id, status, attempt_count, payload_json, created_at, updated_at)
    VALUES ('unknown-outbox', 'unknown-effect', 'UNKNOWN', 1, '{}',
      '2026-08-10T00:00:00.000Z', '2026-08-10T00:00:00.000Z')
  `).run();
  runtime.db = failEventAppend(db, 'TOOL_EFFECT_RESOLVED');

  assert.throws(
    () => runtime.resolveToolEffect(run.runId, {
      tenantId: 't1',
      actorId: 'operator-1',
      effectId: 'unknown-effect',
      outcome: 'FAILED',
      reason: 'downstream confirmed it was not applied',
    }),
    /injected TOOL_EFFECT_RESOLVED audit failure/,
  );
  assert.equal(db.prepare("SELECT status FROM tool_effects WHERE effect_id = 'unknown-effect'").get().status, 'UNKNOWN');
  assert.equal(db.prepare("SELECT status FROM tool_effect_outbox WHERE effect_id = 'unknown-effect'").get().status, 'UNKNOWN');
  assert.equal(db.prepare("SELECT count(*) AS count FROM run_events WHERE event_type = 'TOOL_EFFECT_RESOLVED'").get().count, 0);
});

test('takeover before the Context read transaction prevents stale access and its audit record', () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  registerBuiltinTools(tools);
  const runtime = new Runtime(db, { contexts, tools });
  const takeoverRuntime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'fence stale Context access' });
  const staleLease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, staleLease);
  const saved = contexts.put({ tenantId: 't1', content: 'lease-protected evidence' });
  runtime.db = interceptTransactionOnce(db, () => {
    takeoverRuntime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-b', force: true });
  });

  assert.throws(
    () => runtime.readContext(run.runId, staleLease, saved.contextRefId),
    (error) => error.code === 'FENCED_OUT',
  );
  assert.equal(db.prepare("SELECT count(*) AS count FROM audit_events WHERE event_type = 'ASSET_READ'").get().count, 0);
  assert.equal(db.prepare('SELECT count(*) AS count FROM run_context_reads').get().count, 0);
});

test('a Worker entry self-heals the crash window after an UNKNOWN effect was persisted', async () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  tools.register({
    operationId: 'external.crash-unknown',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => { throw new Error('ambiguous timeout'); });
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'self-heal unknown crash window' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  runtime.db = failNthTransaction(db, 2, 'simulated crash before RUN_WAITING_MANUAL');

  await assert.rejects(
    runtime.invokeTool(run.runId, lease, {
      operationId: 'external.crash-unknown',
      version: 1,
      input: { command: 'send' },
      idempotencyKey: 'crash-unknown',
    }),
    /simulated crash before RUN_WAITING_MANUAL/,
  );
  assert.equal(db.prepare("SELECT status FROM tool_effects WHERE idempotency_key = 'crash-unknown'").get().status, 'UNKNOWN');
  assert.equal(runtime.getRun(run.runId, 't1').status, 'RUNNING');

  runtime.db = db;
  assert.throws(
    () => runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { mustNotCommit: true } }),
    (error) => error.code === 'EFFECT_STATUS_UNKNOWN',
  );
  const healed = runtime.getRun(run.runId, 't1');
  assert.equal(healed.status, 'WAITING_MANUAL');
  assert.equal(healed.leaseOwner, null);
  assert.equal(db.prepare('SELECT count(*) AS count FROM checkpoints WHERE run_id = ?').get(run.runId).count, 0);
});

test('a policy-blocked effect commits its manual gate before returning the error', () => {
  const db = createDatabase(':memory:');
  const runtime = compose(db);
  const run = runtime.createRun({ tenantId: 't1', goal: 'atomically gate unknown policy' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  runtime.db = failNthTransaction(db, 2, 'obsolete second policy-gate transaction');

  assert.throws(
    () => runtime.invokeTool(run.runId, lease, {
      operationId: 'effect.opaque',
      version: 1,
      input: {},
    }),
    (error) => error.code === 'UNKNOWN_SIDE_EFFECT',
  );
  const gated = runtime.getRun(run.runId, 't1');
  assert.equal(gated.status, 'WAITING_MANUAL');
  assert.equal(gated.leaseOwner, null);
  assert.equal(db.prepare('SELECT count(*) AS count FROM tool_effects WHERE run_id = ?').get(run.runId).count, 0);
  runtime.db = db;
  assert.throws(
    () => runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { mustNotCommit: true } }),
    (error) => error.code === 'FENCED_OUT',
  );
});

test('lease acquisition self-heals the crash window after a known FAILED effect was persisted', async () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  const rejection = new Error('known not-applied rejection');
  rejection.effectOutcome = 'FAILED';
  tools.register({
    operationId: 'external.crash-failed',
    version: 1,
    sideEffect: 'SIDE_EFFECT',
    idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
    executionMode: 'EXTERNAL_OUTBOX',
  }, async () => { throw rejection; });
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'self-heal failed crash window' });
  const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, lease);
  runtime.db = failNthTransaction(db, 2, 'simulated crash before RUN_FAILED');

  await assert.rejects(
    runtime.invokeTool(run.runId, lease, {
      operationId: 'external.crash-failed',
      version: 1,
      input: { command: 'reject' },
      idempotencyKey: 'crash-failed',
    }),
    /simulated crash before RUN_FAILED/,
  );
  assert.equal(db.prepare("SELECT status FROM tool_effects WHERE idempotency_key = 'crash-failed'").get().status, 'FAILED');
  assert.equal(runtime.getRun(run.runId, 't1').status, 'RUNNING');

  runtime.db = db;
  assert.throws(
    () => runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-b', force: true }),
    (error) => error.code === 'EFFECT_EXECUTION_FAILED',
  );
  const healed = runtime.getRun(run.runId, 't1');
  assert.equal(healed.status, 'FAILED');
  assert.equal(healed.leaseOwner, null);
  assert.equal(runtime.listEvents(run.runId, 't1').at(-1).eventType, 'RUN_FAILED');
});

for (const scenario of [
  { status: 'UNKNOWN', expectedRunStatus: 'WAITING_MANUAL', errorCode: 'EFFECT_STATUS_UNKNOWN' },
  { status: 'FAILED', expectedRunStatus: 'FAILED', errorCode: 'EFFECT_EXECUTION_FAILED' },
]) {
  test(`a cross-Run ${scenario.status} replay commits the current Run disposition before returning`, async () => {
    const db = createDatabase(':memory:');
    const contexts = new ContextStore(db);
    const tools = new ToolRegistry(db);
    let calls = 0;
    tools.register({
      operationId: `external.cross-run-${scenario.status.toLowerCase()}`,
      version: 1,
      sideEffect: 'SIDE_EFFECT',
      idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
      executionMode: 'EXTERNAL_OUTBOX',
    }, async () => {
      calls += 1;
      const error = new Error(`external ${scenario.status.toLowerCase()} outcome`);
      if (scenario.status === 'FAILED') error.effectOutcome = 'FAILED';
      throw error;
    });
    const runtime = new Runtime(db, { contexts, tools });
    const invocation = {
      operationId: `external.cross-run-${scenario.status.toLowerCase()}`,
      version: 1,
      input: { command: 'same' },
      idempotencyKey: `cross-run-${scenario.status.toLowerCase()}`,
    };

    const first = runtime.createRun({ tenantId: 't1', goal: `create ${scenario.status} effect` });
    const firstLease = runtime.acquireLease(first.runId, { tenantId: 't1', workerId: 'worker-a' });
    runtime.start(first.runId, firstLease);
    await assert.rejects(runtime.invokeTool(first.runId, firstLease, invocation), (error) => error.code === scenario.errorCode);

    const second = runtime.createRun({ tenantId: 't1', goal: `replay ${scenario.status} effect` });
    const secondLease = runtime.acquireLease(second.runId, { tenantId: 't1', workerId: 'worker-b' });
    runtime.start(second.runId, secondLease);
    runtime.db = failNthTransaction(db, 2, 'obsolete second cross-Run disposition transaction');
    await assert.rejects(
      runtime.invokeTool(second.runId, secondLease, invocation),
      (error) => error.code === scenario.errorCode,
    );

    assert.equal(calls, 1);
    assert.equal(runtime.getRun(second.runId, 't1').status, scenario.expectedRunStatus);
    assert.equal(db.prepare('SELECT count(*) AS count FROM tool_effects WHERE run_id = ?').get(second.runId).count, 0);
    runtime.db = db;
    assert.throws(
      () => runtime.checkpoint(second.runId, secondLease, { kind: 'FULL', state: { mustNotCommit: true } }),
      (error) => error.code === 'FENCED_OUT',
    );
  });
}
