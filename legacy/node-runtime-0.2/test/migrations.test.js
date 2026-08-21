import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { createDatabase } from '../src/db.js';
import { ToolRegistry } from '../src/tool-contracts.js';

function uniqueIndexColumnSets(db) {
  const rows = db.prepare(`
    SELECT indexes.name AS index_name, columns.seqno, columns.name AS column_name
    FROM pragma_index_list('tool_effects') AS indexes
    JOIN pragma_index_info(indexes.name) AS columns
    WHERE indexes."unique" = 1
    ORDER BY indexes.name, columns.seqno
  `).all();
  const byIndex = new Map();
  for (const row of rows) {
    const columns = byIndex.get(row.index_name) ?? [];
    columns.push(row.column_name);
    byIndex.set(row.index_name, columns);
  }
  return [...byIndex.values()];
}

function assertVersionedEffectUniqueness(db) {
  const indexes = uniqueIndexColumnSets(db);
  assert.ok(indexes.some((columns) => (
    columns.join(',') === 'tenant_id,operation_id,contract_version,idempotency_key'
  )));
  assert.ok(!indexes.some((columns) => (
    columns.join(',') === 'tenant_id,operation_id,idempotency_key'
  )));
}

test('legacy tool effect uniqueness migrates without losing committed history', () => {
  const directory = mkdtempSync(join(tmpdir(), 'agent-runtime-migration-'));
  const path = join(directory, 'runtime.db');
  const legacy = new DatabaseSync(path);
  legacy.exec(`
    CREATE TABLE tool_effects (
      effect_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      operation_id TEXT NOT NULL,
      contract_version INTEGER NOT NULL,
      idempotency_key TEXT NOT NULL,
      input_hash TEXT NOT NULL,
      status TEXT NOT NULL,
      result_json TEXT,
      created_at TEXT NOT NULL,
      committed_at TEXT,
      UNIQUE (tenant_id, operation_id, idempotency_key)
    );
    INSERT INTO tool_effects
      (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
       input_hash, status, result_json, created_at, committed_at)
    VALUES
      ('legacy-effect', 't1', 'r1', 'versioned.write', 1, 'shared-key',
       'legacy-hash', 'COMMITTED', '{"source":"v1"}', '2026-08-09T00:00:00.000Z', '2026-08-09T00:00:01.000Z');
  `);
  legacy.close();

  const migrated = createDatabase(path);
  migrated.prepare(`
    INSERT INTO tool_effects
      (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
       input_hash, status, result_json, created_at, updated_at, committed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    'new-effect', 't1', 'r2', 'versioned.write', 2, 'shared-key', 'new-hash', 'COMMITTED',
    '{"source":"v2"}', '2026-08-10T00:00:00.000Z', '2026-08-10T00:00:01.000Z', '2026-08-10T00:00:01.000Z',
  );

  const rows = migrated.prepare('SELECT effect_id, contract_version, result_json FROM tool_effects ORDER BY contract_version').all().map((row) => ({ ...row }));
  assert.deepEqual(rows, [
    { effect_id: 'legacy-effect', contract_version: 1, result_json: '{"source":"v1"}' },
    { effect_id: 'new-effect', contract_version: 2, result_json: '{"source":"v2"}' },
  ]);
  migrated.close();

  const reopened = createDatabase(path);
  assert.equal(reopened.prepare('SELECT count(*) AS count FROM tool_effects').get().count, 2);
  assertVersionedEffectUniqueness(reopened);
  reopened.close();
});

test('named compact legacy uniqueness migrates while preserving outbox foreign keys', () => {
  const directory = mkdtempSync(join(tmpdir(), 'agent-runtime-named-migration-'));
  const path = join(directory, 'runtime.db');
  const legacy = new DatabaseSync(path);
  legacy.exec(`
    PRAGMA foreign_keys = ON;
    CREATE TABLE tool_effects (
      effect_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      operation_id TEXT NOT NULL,
      contract_version INTEGER NOT NULL,
      idempotency_key TEXT NOT NULL,
      input_hash TEXT NOT NULL,
      status TEXT NOT NULL,
      result_json TEXT,
      created_at TEXT NOT NULL,
      committed_at TEXT,
      CONSTRAINT legacy_effect_key UNIQUE(tenant_id,operation_id,idempotency_key)
    );
    CREATE TABLE tool_effect_outbox (
      outbox_id TEXT PRIMARY KEY,
      effect_id TEXT NOT NULL UNIQUE REFERENCES tool_effects(effect_id),
      status TEXT NOT NULL,
      attempt_count INTEGER NOT NULL DEFAULT 0,
      payload_json TEXT NOT NULL,
      last_error_json TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    INSERT INTO tool_effects
      (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
       input_hash, status, result_json, created_at, committed_at)
    VALUES
      ('legacy-effect', 't1', 'r1', 'external.send', 1, 'same-key',
       'legacy-hash', 'DISPATCHING', NULL, '2026-08-09T00:00:00.000Z', NULL);
    INSERT INTO tool_effect_outbox
      (outbox_id, effect_id, status, attempt_count, payload_json, created_at, updated_at)
    VALUES
      ('legacy-outbox', 'legacy-effect', 'DISPATCHING', 1, '{"message":"sent"}',
       '2026-08-09T00:00:00.000Z', '2026-08-09T00:00:01.000Z');
  `);
  legacy.close();

  const migrated = createDatabase(path);
  assertVersionedEffectUniqueness(migrated);
  assert.deepEqual(
    { ...migrated.prepare('SELECT outbox_id, effect_id, status, attempt_count FROM tool_effect_outbox').get() },
    { outbox_id: 'legacy-outbox', effect_id: 'legacy-effect', status: 'DISPATCHING', attempt_count: 1 },
  );
  assert.equal(
    migrated.prepare("SELECT \"table\" FROM pragma_foreign_key_list('tool_effect_outbox') WHERE \"from\" = 'effect_id'").get().table,
    'tool_effects',
  );
  assert.equal(migrated.prepare("SELECT execution_mode FROM tool_effects WHERE effect_id = 'legacy-effect'").get().execution_mode, 'EXTERNAL_OUTBOX');
  const registry = new ToolRegistry(migrated);
  assert.equal(migrated.transaction(() => registry.fenceInterruptedEffects('r1', 1)), 1);
  assert.equal(registry.resolveEffect({
    tenantId: 't1',
    runId: 'r1',
    effectId: 'legacy-effect',
    outcome: 'FAILED',
    error: { message: 'legacy dispatch outcome reconciled' },
  }).status, 'FAILED');
  assert.deepEqual(migrated.prepare('PRAGMA foreign_key_check').all(), []);
  migrated.close();

  const reopened = createDatabase(path);
  assertVersionedEffectUniqueness(reopened);
  assert.equal(reopened.prepare('SELECT count(*) AS count FROM tool_effect_outbox').get().count, 1);
  assert.deepEqual(reopened.prepare('PRAGMA foreign_key_check').all(), []);
  reopened.close();
});

test('standalone legacy unique index is detected and replaced deterministically', () => {
  const directory = mkdtempSync(join(tmpdir(), 'agent-runtime-index-migration-'));
  const path = join(directory, 'runtime.db');
  const legacy = new DatabaseSync(path);
  legacy.exec(`
    CREATE TABLE tool_effects (
      effect_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      operation_id TEXT NOT NULL,
      contract_version INTEGER NOT NULL,
      idempotency_key TEXT NOT NULL,
      input_hash TEXT NOT NULL,
      input_json TEXT NOT NULL DEFAULT '{}',
      execution_epoch INTEGER NOT NULL DEFAULT 0,
      execution_mode TEXT NOT NULL DEFAULT 'LOCAL_TRANSACTIONAL',
      status TEXT NOT NULL,
      result_json TEXT,
      error_json TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      committed_at TEXT
    );
    CREATE UNIQUE INDEX legacy_effect_key
      ON tool_effects ( tenant_id , operation_id , idempotency_key );
    INSERT INTO tool_effects
      (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
       input_hash, input_json, execution_epoch, execution_mode, status, result_json, error_json,
       created_at, updated_at, committed_at)
    VALUES
      ('legacy-effect', 't1', 'r1', 'external.send', 7, 'same-key', 'legacy-hash',
       '{"message":"preserve-me"}', 12, 'EXTERNAL_OUTBOX', 'UNKNOWN', NULL, '{"code":"timeout"}',
       '2026-08-09T00:00:00.000Z', '2026-08-09T00:00:01.000Z', NULL);
  `);
  legacy.close();

  const migrated = createDatabase(path);
  assertVersionedEffectUniqueness(migrated);
  assert.deepEqual(
    { ...migrated.prepare(`
      SELECT input_json, execution_epoch, execution_mode, error_json, updated_at
      FROM tool_effects WHERE effect_id = 'legacy-effect'
    `).get() },
    {
      input_json: '{"message":"preserve-me"}',
      execution_epoch: 12,
      execution_mode: 'EXTERNAL_OUTBOX',
      error_json: '{"code":"timeout"}',
      updated_at: '2026-08-09T00:00:01.000Z',
    },
  );
  assert.deepEqual(
    { ...migrated.prepare(`
      SELECT effect_id, status, attempt_count, payload_json
      FROM tool_effect_outbox WHERE effect_id = 'legacy-effect'
    `).get() },
    {
      effect_id: 'legacy-effect',
      status: 'UNKNOWN',
      attempt_count: 0,
      payload_json: '{"message":"preserve-me"}',
    },
  );
  const registry = new ToolRegistry(migrated);
  assert.equal(registry.resolveEffect({
    tenantId: 't1',
    runId: 'r1',
    effectId: 'legacy-effect',
    outcome: 'FAILED',
    error: { message: 'legacy missing outbox reconciled' },
  }).status, 'FAILED');
  migrated.close();

  const reopened = createDatabase(path);
  assertVersionedEffectUniqueness(reopened);
  assert.equal(reopened.prepare('SELECT count(*) AS count FROM tool_effects').get().count, 1);
  reopened.close();
});

test('legacy checkpoint hashes upgrade to integrity v2 without losing state', () => {
  const directory = mkdtempSync(join(tmpdir(), 'agent-runtime-checkpoint-migration-'));
  const path = join(directory, 'runtime.db');
  const legacy = new DatabaseSync(path);
  const stateJson = '{"phase":"legacy"}';
  const legacyHash = createHash('sha256').update(`:${stateJson}`).digest('hex');
  legacy.exec(`
    PRAGMA foreign_keys = ON;
    CREATE TABLE runs (
      run_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, goal TEXT NOT NULL, status TEXT NOT NULL,
      execution_epoch INTEGER NOT NULL DEFAULT 0, lease_id TEXT, lease_owner TEXT, lease_expires_at INTEGER,
      state_version INTEGER NOT NULL DEFAULT 0, last_event_seq INTEGER NOT NULL DEFAULT 0,
      runtime_state_json TEXT NOT NULL DEFAULT '{}', result_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    );
    CREATE TABLE run_events (
      event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id), tenant_id TEXT NOT NULL,
      event_seq INTEGER NOT NULL, event_type TEXT NOT NULL, actor_id TEXT NOT NULL, execution_epoch INTEGER NOT NULL,
      payload_json TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE (run_id, event_seq)
    );
    CREATE TABLE checkpoints (
      checkpoint_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id), checkpoint_seq INTEGER NOT NULL,
      parent_checkpoint_id TEXT, kind TEXT NOT NULL, status TEXT NOT NULL, event_seq INTEGER NOT NULL,
      execution_epoch INTEGER NOT NULL, schema_version INTEGER NOT NULL, state_json TEXT NOT NULL,
      content_hash TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE (run_id, checkpoint_seq)
    );
    INSERT INTO runs
      (run_id, tenant_id, goal, status, execution_epoch, state_version, last_event_seq, runtime_state_json, created_at, updated_at)
    VALUES ('legacy-run', 't1', 'resume legacy', 'RUNNING', 1, 1, 1, '${stateJson}', '2026-08-09T00:00:00.000Z', '2026-08-09T00:00:00.000Z');
    INSERT INTO run_events
      (event_id, run_id, tenant_id, event_seq, event_type, actor_id, execution_epoch, payload_json, created_at)
    VALUES ('legacy-event', 'legacy-run', 't1', 1, 'CHECKPOINT_COMMITTED', 'worker-a', 1,
      '{"checkpointId":"legacy-cp","kind":"FULL"}', '2026-08-09T00:00:00.000Z');
    INSERT INTO checkpoints
      (checkpoint_id, run_id, checkpoint_seq, parent_checkpoint_id, kind, status, event_seq,
       execution_epoch, schema_version, state_json, content_hash, created_at)
    VALUES ('legacy-cp', 'legacy-run', 1, NULL, 'FULL', 'COMMITTED', 1, 1, 1,
      '${stateJson}', '${legacyHash}', '2026-08-09T00:00:00.000Z');
  `);
  legacy.close();

  const migrated = createDatabase(path);
  const checkpoint = migrated.prepare('SELECT integrity_version, state_json FROM checkpoints WHERE checkpoint_id = ?').get('legacy-cp');
  assert.equal(checkpoint.integrity_version, 2);
  assert.equal(checkpoint.state_json, stateJson);
  migrated.close();
});
