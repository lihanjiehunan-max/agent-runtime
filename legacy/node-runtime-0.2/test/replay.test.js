import assert from 'node:assert/strict';
import test from 'node:test';
import { createDatabase } from '../src/db.js';
import { ContextStore } from '../src/context-store.js';
import { ToolRegistry, registerBuiltinTools } from '../src/tool-contracts.js';
import { Runtime } from '../src/runtime.js';

test('recovery replays deltas from the latest full anchor under a new epoch', () => {
  const db = createDatabase(':memory:');
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  registerBuiltinTools(tools);
  const runtime = new Runtime(db, { contexts, tools });
  const run = runtime.createRun({ tenantId: 't1', goal: 'resume me' });
  const firstLease = runtime.acquireLease(run.runId, { tenantId: 't1', workerId: 'worker-a' });
  runtime.start(run.runId, firstLease);
  runtime.checkpoint(run.runId, firstLease, { kind: 'FULL', state: { phase: 'plan', completed: [] } });
  runtime.checkpoint(run.runId, firstLease, { kind: 'DELTA', state: { phase: 'execute', completed: ['plan'] } });

  const recovered = runtime.recover(run.runId, { tenantId: 't1', workerId: 'worker-b' });

  assert.equal(recovered.executionEpoch, firstLease.executionEpoch + 1);
  assert.deepEqual(recovered.state, { phase: 'execute', completed: ['plan'] });
  assert.equal(runtime.getRun(run.runId, 't1').status, 'RUNNING');
  assert.throws(() => runtime.complete(run.runId, firstLease, {}), (error) => error.code === 'FENCED_OUT');
});
