import assert from 'node:assert/strict';
import test from 'node:test';
import { checkpointHash } from '../src/checkpoint-integrity.js';
import { createDatabase } from '../src/db.js';
import { ContextStore } from '../src/context-store.js';
import { ToolRegistry, registerBuiltinTools } from '../src/tool-contracts.js';
import { Runtime } from '../src/runtime.js';

function fixture() {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  registerBuiltinTools(tools);
  return { db, contexts, tools, runtime: new Runtime(db, { contexts, tools }) };
}

function running() {
  const f = fixture();
  const run = f.runtime.createRun({ tenantId: 't1', goal: 'produce a report' });
  const lease = f.runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  f.runtime.start(run.runId, lease);
  return { ...f, run, lease };
}

test('run follows legal transitions and appends monotonic events', () => {
  const { runtime, run, lease } = running();
  runtime.complete(run.runId, lease, { summary: 'done' });

  assert.equal(runtime.getRun(run.runId, 't1').status, 'SUCCEEDED');
  assert.deepEqual(runtime.listEvents(run.runId, 't1').map((event) => [event.eventSeq, event.eventType]), [
    [1, 'RUN_CREATED'],
    [2, 'LEASE_ACQUIRED'],
    [3, 'RUN_STARTED'],
    [4, 'RUN_SUCCEEDED'],
  ]);
  assert.throws(() => runtime.start(run.runId, lease), (error) => error.code === 'INVALID_RUN_TRANSITION');
});

test('new lease epoch fences all writes from the old worker', () => {
  const { runtime, run, lease } = running();
  const takeover = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-b', force: true });

  assert.equal(takeover.executionEpoch, lease.executionEpoch + 1);
  assert.throws(() => runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { step: 1 } }), (error) => error.code === 'FENCED_OUT');
  const committed = runtime.checkpoint(run.runId, takeover, { kind: 'FULL', state: { step: 1 } });
  assert.equal(committed.executionEpoch, takeover.executionEpoch);
});

test('checkpoint atomically commits state, cursor, and event', () => {
  const { db, runtime, run, lease } = running();
  const checkpoint = runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { completed: ['collect'] } });
  const row = db.prepare('SELECT status, event_seq, state_json FROM checkpoints WHERE checkpoint_id = ?').get(checkpoint.checkpointId);

  assert.equal(row.status, 'COMMITTED');
  assert.equal(row.event_seq, 4);
  assert.deepEqual(JSON.parse(row.state_json), { completed: ['collect'] });
  assert.equal(runtime.getRun(run.runId, 't1').lastEventSeq, 4);
});

test('context reads are tied to run and revoked assets trigger conservative rollback', () => {
  const { contexts, runtime, run, lease } = running();
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { phase: 'before-read' } });
  const saved = contexts.put({ tenantId: 't1', content: 'policy v1' });
  runtime.readContext(run.runId, lease, saved.contextRefId);
  contexts.revoke(saved.contextRefId, { tenantId: 't1', reason: 'superseded' });

  const result = runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' });
  assert.equal(result.action, 'ROLLBACK');
  assert.equal(result.checkpointState.phase, 'before-read');
  assert.equal(runtime.getRun(run.runId, 't1').status, 'RECOVERING');
});

test('revoked context rollback materializes a DELTA safe head and fences its old lease', () => {
  const { contexts, runtime, run, lease } = running();
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { anchor: 'kept', phase: 'full' } });
  runtime.checkpoint(run.runId, lease, { kind: 'DELTA', state: { phase: 'before-read' } });
  const saved = contexts.put({ tenantId: 't1', content: 'withdrawn source' });
  runtime.readContext(run.runId, lease, saved.contextRefId);
  contexts.revoke(saved.contextRefId, { tenantId: 't1', reason: 'withdrawn' });

  const result = runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' });

  assert.deepEqual(result.checkpointState, { anchor: 'kept', phase: 'before-read' });
  assert.equal(runtime.getRun(run.runId, 't1').executionEpoch, lease.executionEpoch + 1);
  assert.throws(
    () => runtime.checkpoint(run.runId, lease, { kind: 'DELTA', state: { stale: true } }),
    (error) => error.code === 'FENCED_OUT',
  );
});

test('recovery never replays checkpoints derived from a revoked context', () => {
  const { db, contexts, runtime, run, lease } = running();
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { safe: true, phase: 'before-read' } });
  const saved = contexts.put({ tenantId: 't1', content: 'revoked evidence' });
  runtime.readContext(run.runId, lease, saved.contextRefId);
  runtime.checkpoint(run.runId, lease, { kind: 'DELTA', state: { phase: 'unsafe', revokedDerived: true } });
  contexts.revoke(saved.contextRefId, { tenantId: 't1', reason: 'invalid evidence' });
  runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' });

  const recovered = runtime.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' });

  assert.deepEqual(recovered.state, { safe: true, phase: 'before-read' });
  assert.equal(db.prepare("SELECT count(*) AS count FROM checkpoints WHERE run_id = ? AND status = 'INVALIDATED_CONTEXT_REVOKED'").get(run.runId).count, 1);
});

test('unknown tool effects force manual handling instead of automatic replay', () => {
  const { runtime, run, lease } = running();
  assert.throws(
    () => runtime.invokeTool(run.runId, lease, { operationId: 'effect.opaque', version: 1, input: {} }),
    (error) => error.code === 'UNKNOWN_SIDE_EFFECT',
  );
  assert.equal(runtime.getRun(run.runId, 't1').status, 'WAITING_MANUAL');
});

test('ordinary recovery cannot bypass WAITING_MANUAL or acquire a new lease', () => {
  const { runtime, run, lease } = running();
  assert.throws(
    () => runtime.invokeTool(run.runId, lease, { operationId: 'effect.opaque', version: 1, input: {} }),
    (error) => error.code === 'UNKNOWN_SIDE_EFFECT',
  );
  const before = runtime.getRun(run.runId, 't1');
  const eventCount = runtime.listEvents(run.runId, 't1').length;

  assert.throws(
    () => runtime.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' }),
    (error) => error.code === 'MANUAL_INTERVENTION_REQUIRED',
  );

  const after = runtime.getRun(run.runId, 't1');
  assert.equal(after.status, 'WAITING_MANUAL');
  assert.equal(after.executionEpoch, before.executionEpoch);
  assert.equal(after.leaseOwner, before.leaseOwner);
  assert.equal(runtime.listEvents(run.runId, 't1').length, eventCount);
});

test('operator resolution is explicit, audited, and fences the previous lease', () => {
  const { runtime, run, lease } = running();
  assert.throws(
    () => runtime.invokeTool(run.runId, lease, { operationId: 'effect.opaque', version: 1, input: {} }),
    (error) => error.code === 'UNKNOWN_SIDE_EFFECT',
  );

  const resolved = runtime.resolveManual(run.runId, {
    tenantId: 't1',
    actorId: 'operator-1',
    decision: 'RESUME',
    reason: 'reviewed opaque operation and approved clean restart',
  });

  assert.equal(resolved.status, 'RECOVERING');
  assert.equal(resolved.executionEpoch, lease.executionEpoch + 2);
  assert.equal(resolved.leaseOwner, null);
  const event = runtime.listEvents(run.runId, 't1').at(-1);
  assert.equal(event.eventType, 'MANUAL_RESOLVED');
  assert.equal(event.actorId, 'operator-1');
  assert.deepEqual(event.payload, { decision: 'RESUME', reason: 'reviewed opaque operation and approved clean restart' });
});

test('reconciling an active context is a no-op and a revoked context is idempotent', () => {
  const { contexts, runtime, run, lease } = running();
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { safe: true } });
  const saved = contexts.put({ tenantId: 't1', content: 'eventually revoked source' });
  runtime.readContext(run.runId, lease, saved.contextRefId);
  runtime.checkpoint(run.runId, lease, { kind: 'DELTA', state: { unsafe: true } });

  assert.deepEqual(
    runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' }),
    { action: 'CONTINUE' },
  );
  const before = runtime.getRun(run.runId, 't1');

  contexts.revoke(saved.contextRefId, { tenantId: 't1', reason: 'withdrawn' });
  const first = runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' });
  const afterFirst = runtime.getRun(run.runId, 't1');
  const eventCount = runtime.listEvents(run.runId, 't1').length;
  const second = runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' });
  const afterSecond = runtime.getRun(run.runId, 't1');

  assert.equal(first.action, 'ROLLBACK');
  assert.equal(second.action, 'ROLLBACK');
  assert.equal(afterFirst.executionEpoch, before.executionEpoch + 1);
  assert.equal(afterSecond.executionEpoch, afterFirst.executionEpoch);
  assert.equal(afterSecond.stateVersion, afterFirst.stateVersion);
  assert.equal(runtime.listEvents(run.runId, 't1').length, eventCount);
});

test('context reconciliation preserves an existing manual gate', () => {
  const { contexts, runtime, run, lease } = running();
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { safe: true } });
  const saved = contexts.put({ tenantId: 't1', content: 'source revoked during manual review' });
  runtime.readContext(run.runId, lease, saved.contextRefId);
  assert.throws(
    () => runtime.invokeTool(run.runId, lease, { operationId: 'effect.opaque', version: 1, input: {} }),
    (error) => error.code === 'UNKNOWN_SIDE_EFFECT',
  );
  contexts.revoke(saved.contextRefId, { tenantId: 't1', reason: 'withdrawn during review' });
  const before = runtime.getRun(run.runId, 't1');
  const eventCount = runtime.listEvents(run.runId, 't1').length;

  const result = runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' });
  const after = runtime.getRun(run.runId, 't1');

  assert.equal(result.action, 'MANUAL_REQUIRED');
  assert.equal(after.status, 'WAITING_MANUAL');
  assert.equal(after.executionEpoch, before.executionEpoch);
  assert.equal(after.stateVersion, before.stateVersion);
  assert.equal(runtime.listEvents(run.runId, 't1').length, eventCount);

  runtime.resolveManual(run.runId, {
    tenantId: 't1',
    actorId: 'operator-1',
    decision: 'RESUME',
    reason: 'resume through the recovery path',
  });
  assert.throws(
    () => runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-b' }),
    (error) => error.code === 'INVALID_RUN_TRANSITION',
  );
  const recovered = runtime.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' });
  assert.deepEqual(recovered.state, { safe: true });
});

test('manual resume after a revoked read with a committed effect still restores only the safe branch', () => {
  const { db, contexts, runtime, run, lease } = running();
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { safe: true } });
  const saved = contexts.put({ tenantId: 't1', content: 'effect input later revoked' });
  runtime.readContext(run.runId, lease, saved.contextRefId);
  runtime.invokeTool(run.runId, lease, {
    operationId: 'counter.increment',
    version: 1,
    input: { name: 'writes', amount: 1 },
    idempotencyKey: 'revoked-effect',
  });
  runtime.checkpoint(run.runId, lease, { kind: 'DELTA', state: { unsafe: true } });
  contexts.revoke(saved.contextRefId, { tenantId: 't1', reason: 'invalid source' });

  const reconciled = runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' });
  assert.equal(reconciled.action, 'WAITING_MANUAL');
  assert.equal(runtime.getRun(run.runId, 't1').status, 'WAITING_MANUAL');
  assert.equal(db.prepare("SELECT status FROM checkpoints WHERE kind = 'DELTA'").get().status, 'INVALIDATED_CONTEXT_REVOKED');

  runtime.resolveManual(run.runId, {
    tenantId: 't1',
    actorId: 'operator-1',
    decision: 'RESUME',
    reason: 'effect was reviewed; resume from trusted evidence only',
  });
  const recovered = runtime.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' });

  assert.deepEqual(recovered.state, { safe: true });
});

test('checkpoint corruption during revoked Context reconciliation fences the old lease and blocks the Run', () => {
  const { db, contexts, runtime, run, lease } = running();
  runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { safe: true } });
  const saved = contexts.put({ tenantId: 't1', content: 'source with corrupt derived state' });
  runtime.readContext(run.runId, lease, saved.contextRefId);
  runtime.checkpoint(run.runId, lease, { kind: 'DELTA', state: { unsafe: true } });
  db.prepare("UPDATE checkpoints SET content_hash = 'tampered' WHERE run_id = ? AND kind = 'DELTA'").run(run.runId);
  contexts.revoke(saved.contextRefId, { tenantId: 't1', reason: 'withdrawn' });

  assert.throws(
    () => runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' }),
    (error) => error.code === 'CHECKPOINT_CORRUPT',
  );
  const blocked = runtime.getRun(run.runId, 't1');
  assert.equal(blocked.status, 'RECOVERY_BLOCKED');
  assert.equal(blocked.leaseOwner, null);
  assert.equal(blocked.executionEpoch, lease.executionEpoch + 1);
  assert.throws(
    () => runtime.invokeTool(run.runId, lease, {
      operationId: 'counter.increment',
      version: 1,
      input: { name: 'must-not-run', amount: 1 },
      idempotencyKey: 'blocked-corrupt-revoke',
    }),
    (error) => error.code === 'FENCED_OUT',
  );
});

test('one corrupt dependent Run does not prevent revoked Context propagation to healthy Runs', () => {
  const { db, contexts, runtime } = fixture();
  const saved = contexts.put({ tenantId: 't1', content: 'shared revocable source' });
  const runs = [];
  for (const goal of ['dependent-a', 'dependent-b']) {
    const run = runtime.createRun({ tenantId: 't1', goal });
    const lease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: goal });
    runtime.start(run.runId, lease);
    runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { safe: goal } });
    runtime.readContext(run.runId, lease, saved.contextRefId);
    runtime.checkpoint(run.runId, lease, { kind: 'DELTA', state: { unsafe: goal } });
    runs.push(run);
  }
  const ordered = [...runs].sort((left, right) => left.runId.localeCompare(right.runId));
  db.prepare("UPDATE checkpoints SET content_hash = 'tampered' WHERE run_id = ? AND kind = 'DELTA'").run(ordered[0].runId);
  contexts.revoke(saved.contextRefId, { tenantId: 't1', reason: 'withdrawn everywhere' });

  const results = runtime.reconcileRevokedContextReads(saved.contextRefId, { tenantId: 't1', actorId: 'operator-1' });

  assert.equal(results.length, 2);
  assert.equal(runtime.getRun(ordered[0].runId, 't1').status, 'RECOVERY_BLOCKED');
  assert.equal(runtime.getRun(ordered[1].runId, 't1').status, 'RECOVERING');
  assert.equal(results.find((entry) => entry.runId === ordered[0].runId).action, 'RECOVERY_BLOCKED');
  assert.equal(results.find((entry) => entry.runId === ordered[1].runId).action, 'ROLLBACK');
});

test('revoked reconciliation also fail-closes a hash-valid safe lineage with no FULL anchor', () => {
  const { db, contexts, runtime, run, lease } = running();
  const safe = runtime.checkpoint(run.runId, lease, { kind: 'FULL', state: { safe: true } });
  const saved = contexts.put({ tenantId: 't1', content: 'source after malformed anchor' });
  runtime.readContext(run.runId, lease, saved.contextRefId);
  runtime.checkpoint(run.runId, lease, { kind: 'DELTA', state: { unsafe: true } });
  const row = db.prepare('SELECT * FROM checkpoints WHERE checkpoint_id = ?').get(safe.checkpointId);
  const malformed = { ...row, kind: 'DELTA' };
  db.prepare("UPDATE checkpoints SET kind = 'DELTA', content_hash = ? WHERE checkpoint_id = ?")
    .run(checkpointHash(malformed), safe.checkpointId);
  db.prepare(`
    UPDATE run_events SET payload_json = ?
    WHERE run_id = ? AND event_seq = ? AND event_type = 'CHECKPOINT_COMMITTED'
  `).run(JSON.stringify({ checkpointId: safe.checkpointId, kind: 'DELTA' }), run.runId, safe.eventSeq);
  contexts.revoke(saved.contextRefId, { tenantId: 't1', reason: 'withdrawn' });

  assert.throws(
    () => runtime.reconcileRevokedContext(run.runId, saved.contextRefId, { tenantId: 't1' }),
    (error) => error.code === 'CHECKPOINT_CORRUPT',
  );
  assert.equal(runtime.getRun(run.runId, 't1').status, 'RECOVERY_BLOCKED');
  assert.throws(
    () => runtime.checkpoint(run.runId, lease, { kind: 'DELTA', state: { stale: true } }),
    (error) => error.code === 'FENCED_OUT',
  );
});
