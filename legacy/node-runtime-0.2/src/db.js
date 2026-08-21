import { DatabaseSync } from 'node:sqlite';
import { checkpointHash, legacyCheckpointHash } from './checkpoint-integrity.js';

const SCHEMA = `
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS contexts (
  context_ref_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  content TEXT NOT NULL,
  classification TEXT NOT NULL,
  created_at TEXT NOT NULL,
  revoked_at TEXT,
  revoke_reason TEXT,
  UNIQUE (tenant_id, content_hash, classification)
);

CREATE TABLE IF NOT EXISTS audit_events (
  audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  tenant_id TEXT NOT NULL,
  run_id TEXT,
  actor_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  subject_id TEXT,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tool_contracts (
  operation_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  contract_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (operation_id, version)
);

CREATE TABLE IF NOT EXISTS tool_effects (
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
  committed_at TEXT,
  UNIQUE (tenant_id, operation_id, contract_version, idempotency_key)
);

CREATE TABLE IF NOT EXISTS tool_effect_outbox (
  outbox_id TEXT PRIMARY KEY,
  effect_id TEXT NOT NULL UNIQUE REFERENCES tool_effects(effect_id),
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  payload_json TEXT NOT NULL,
  last_error_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS counters (
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  value INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  goal TEXT NOT NULL,
  status TEXT NOT NULL,
  execution_epoch INTEGER NOT NULL DEFAULT 0,
  lease_id TEXT,
  lease_owner TEXT,
  lease_expires_at INTEGER,
  state_version INTEGER NOT NULL DEFAULT 0,
  last_event_seq INTEGER NOT NULL DEFAULT 0,
  runtime_state_json TEXT NOT NULL DEFAULT '{}',
  result_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_events (
  event_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  tenant_id TEXT NOT NULL,
  event_seq INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  execution_epoch INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (run_id, event_seq)
);

CREATE TABLE IF NOT EXISTS checkpoints (
  checkpoint_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  checkpoint_seq INTEGER NOT NULL,
  parent_checkpoint_id TEXT,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  event_seq INTEGER NOT NULL,
  execution_epoch INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  state_json TEXT NOT NULL,
  integrity_version INTEGER NOT NULL DEFAULT 2,
  content_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (run_id, checkpoint_seq)
);

CREATE TABLE IF NOT EXISTS run_context_reads (
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  context_ref_id TEXT NOT NULL REFERENCES contexts(context_ref_id),
  first_read_event_seq INTEGER NOT NULL,
  read_at TEXT NOT NULL,
  PRIMARY KEY (run_id, context_ref_id)
);
`;

export function createDatabase(path = ':memory:') {
  const db = new DatabaseSync(path);
  db.exec('PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;');
  migrateToolEffectUniqueness(db);
  db.exec(SCHEMA);
  migrateToolEffectColumns(db);
  migrateExternalEffectModes(db);
  migrateExternalEffectOutboxIntegrity(db);
  migrateCheckpointIntegrity(db);
  db.transaction = (work) => {
    db.exec('BEGIN IMMEDIATE');
    try {
      const result = work();
      db.exec('COMMIT');
      return result;
    } catch (error) {
      db.exec('ROLLBACK');
      throw error;
    }
  };
  return db;
}

function migrateToolEffectColumns(db) {
  const additions = [
    ['input_json', "ALTER TABLE tool_effects ADD COLUMN input_json TEXT NOT NULL DEFAULT '{}'"],
    ['execution_epoch', 'ALTER TABLE tool_effects ADD COLUMN execution_epoch INTEGER NOT NULL DEFAULT 0'],
    ['execution_mode', "ALTER TABLE tool_effects ADD COLUMN execution_mode TEXT NOT NULL DEFAULT 'LOCAL_TRANSACTIONAL'"],
    ['error_json', 'ALTER TABLE tool_effects ADD COLUMN error_json TEXT'],
    ['updated_at', 'ALTER TABLE tool_effects ADD COLUMN updated_at TEXT'],
  ];
  for (const [column, sql] of additions) {
    if (!hasColumn(db, 'tool_effects', column)) db.exec(sql);
  }
  db.exec('UPDATE tool_effects SET updated_at = COALESCE(updated_at, committed_at, created_at) WHERE updated_at IS NULL');
}

function migrateExternalEffectModes(db) {
  db.exec(`
    UPDATE tool_effects SET execution_mode = 'EXTERNAL_OUTBOX'
    WHERE EXISTS (
      SELECT 1 FROM tool_effect_outbox AS outbox
      WHERE outbox.effect_id = tool_effects.effect_id
    ) AND execution_mode <> 'EXTERNAL_OUTBOX'
  `);
}

function migrateExternalEffectOutboxIntegrity(db) {
  const missing = db.prepare(`
    SELECT count(*) AS count
    FROM tool_effects AS effect
    WHERE effect.execution_mode = 'EXTERNAL_OUTBOX'
      AND NOT EXISTS (
        SELECT 1 FROM tool_effect_outbox AS outbox
        WHERE outbox.effect_id = effect.effect_id
      )
  `).get().count;
  if (missing === 0) return;

  const timestamp = new Date().toISOString();
  const migrationError = JSON.stringify({
    name: 'ExternalEffectMigrationUncertainty',
    code: 'EXTERNAL_EFFECT_OUTBOX_MISSING',
    message: 'A legacy external effect had no durable Outbox row; its outcome requires reconciliation',
  });
  db.exec('BEGIN IMMEDIATE');
  try {
    db.prepare(`
      UPDATE tool_effects AS effect
      SET status = 'UNKNOWN', error_json = COALESCE(error_json, ?), updated_at = ?
      WHERE effect.execution_mode = 'EXTERNAL_OUTBOX'
        AND effect.status NOT IN ('COMMITTED', 'FAILED', 'UNKNOWN')
        AND NOT EXISTS (
          SELECT 1 FROM tool_effect_outbox AS outbox
          WHERE outbox.effect_id = effect.effect_id
        )
    `).run(migrationError, timestamp);
    db.prepare(`
      INSERT INTO tool_effect_outbox
        (outbox_id, effect_id, status, attempt_count, payload_json, last_error_json, created_at, updated_at)
      SELECT
        'outbox_migrated_' || lower(hex(randomblob(16))),
        effect.effect_id,
        CASE effect.status
          WHEN 'COMMITTED' THEN 'DELIVERED'
          WHEN 'FAILED' THEN 'FAILED'
          ELSE 'UNKNOWN'
        END,
        0,
        effect.input_json,
        CASE WHEN effect.status = 'COMMITTED' THEN NULL ELSE effect.error_json END,
        effect.created_at,
        effect.updated_at
      FROM tool_effects AS effect
      WHERE effect.execution_mode = 'EXTERNAL_OUTBOX'
        AND NOT EXISTS (
          SELECT 1 FROM tool_effect_outbox AS outbox
          WHERE outbox.effect_id = effect.effect_id
        )
    `).run();
    const unresolved = db.prepare(`
      SELECT count(*) AS count
      FROM tool_effects AS effect
      WHERE effect.execution_mode = 'EXTERNAL_OUTBOX'
        AND NOT EXISTS (
          SELECT 1 FROM tool_effect_outbox AS outbox
          WHERE outbox.effect_id = effect.effect_id
        )
    `).get().count;
    if (unresolved !== 0) {
      const error = new Error('External Tool effect migration could not establish Outbox integrity');
      error.code = 'TOOL_EFFECT_MIGRATION_OUTBOX_INTEGRITY';
      error.missing = unresolved;
      throw error;
    }
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}

function hasColumn(db, table, column) {
  return db.prepare(`PRAGMA table_info(${table})`).all().some((entry) => entry.name === column);
}

function migrateCheckpointIntegrity(db) {
  if (!hasColumn(db, 'checkpoints', 'integrity_version')) {
    db.exec('ALTER TABLE checkpoints ADD COLUMN integrity_version INTEGER NOT NULL DEFAULT 1');
  }
  const legacy = db.prepare('SELECT * FROM checkpoints WHERE integrity_version = 1 ORDER BY run_id, checkpoint_seq').all();
  if (legacy.length === 0) return;

  db.exec('BEGIN IMMEDIATE');
  try {
    const update = db.prepare('UPDATE checkpoints SET integrity_version = 2, content_hash = ? WHERE checkpoint_id = ? AND integrity_version = 1');
    for (const checkpoint of legacy) {
      if (checkpoint.content_hash !== legacyCheckpointHash(checkpoint)) {
        const error = new Error(`Legacy checkpoint ${checkpoint.checkpoint_id} failed integrity migration`);
        error.code = 'CHECKPOINT_MIGRATION_CORRUPT';
        throw error;
      }
      const upgraded = { ...checkpoint, integrity_version: 2 };
      update.run(checkpointHash(upgraded), checkpoint.checkpoint_id);
    }
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}

function migrateToolEffectUniqueness(db) {
  const exists = db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'tool_effects'").get();
  if (!exists) return;

  const indexes = uniqueIndexes(db, 'tool_effects');
  const expected = ['tenant_id', 'operation_id', 'contract_version', 'idempotency_key'];
  const legacy = ['tenant_id', 'operation_id', 'idempotency_key'];
  const hasExpected = indexes.some((index) => !index.partial && sameColumns(index.columns, expected));
  const hasLegacy = indexes.some((index) => !index.partial && sameColumns(index.columns, legacy));
  if (hasExpected && !hasLegacy) return;

  const columns = new Set(db.prepare('PRAGMA table_info(tool_effects)').all().map((entry) => entry.name));
  const required = [
    'effect_id', 'tenant_id', 'run_id', 'operation_id', 'contract_version',
    'idempotency_key', 'input_hash', 'status', 'created_at',
  ];
  const missing = required.filter((column) => !columns.has(column));
  if (missing.length > 0) {
    const error = new Error(`Cannot migrate tool_effects; missing required columns: ${missing.join(', ')}`);
    error.code = 'TOOL_EFFECT_MIGRATION_UNSUPPORTED';
    throw error;
  }

  const indexByName = new Map(indexes.map((index) => [index.name, index]));
  const dependentObjects = db.prepare(`
    SELECT type, name, sql
    FROM sqlite_master
    WHERE tbl_name = 'tool_effects'
      AND type IN ('index', 'trigger')
      AND sql IS NOT NULL
    ORDER BY type, name
  `).all().filter((object) => {
    if (object.type !== 'index') return true;
    const index = indexByName.get(object.name);
    return !index || (!sameColumns(index.columns, legacy) && !sameColumns(index.columns, expected));
  });

  const expression = (column, fallback) => (columns.has(column) ? `"${column}"` : fallback);
  const nullable = (column) => expression(column, 'NULL');
  const inputJson = columns.has('input_json') ? `COALESCE("input_json", '{}')` : `'{}'`;
  const executionEpoch = columns.has('execution_epoch') ? 'COALESCE("execution_epoch", 0)' : '0';
  const outboxExists = db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'tool_effect_outbox'").get();
  const inferredExecutionMode = outboxExists
    ? `CASE WHEN EXISTS (
        SELECT 1 FROM tool_effect_outbox AS outbox
        WHERE outbox.effect_id = tool_effects."effect_id"
      ) THEN 'EXTERNAL_OUTBOX' ELSE 'LOCAL_TRANSACTIONAL' END`
    : `'LOCAL_TRANSACTIONAL'`;
  const executionMode = columns.has('execution_mode')
    ? `COALESCE("execution_mode", ${inferredExecutionMode})`
    : inferredExecutionMode;
  const updatedAtParts = [
    columns.has('updated_at') ? '"updated_at"' : null,
    columns.has('committed_at') ? '"committed_at"' : null,
    '"created_at"',
  ].filter(Boolean);
  const updatedAt = `COALESCE(${updatedAtParts.join(', ')})`;
  const foreignKeysEnabled = db.prepare('PRAGMA foreign_keys').get().foreign_keys === 1;
  let transactionStarted = false;

  db.exec('PRAGMA foreign_keys = OFF');
  try {
    db.exec('BEGIN IMMEDIATE');
    transactionStarted = true;
    db.exec(`
      CREATE TABLE tool_effects__migration_v2 (
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
        committed_at TEXT,
        UNIQUE (tenant_id, operation_id, contract_version, idempotency_key)
      );
    `);
    db.exec(`
      INSERT INTO tool_effects__migration_v2
        (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
         input_hash, input_json, execution_epoch, execution_mode, status, result_json, error_json,
         created_at, updated_at, committed_at)
      SELECT "effect_id", "tenant_id", "run_id", "operation_id", "contract_version", "idempotency_key",
         "input_hash", ${inputJson}, ${executionEpoch}, ${executionMode}, "status", ${nullable('result_json')},
         ${nullable('error_json')}, "created_at", ${updatedAt}, ${nullable('committed_at')}
      FROM tool_effects;
      DROP TABLE tool_effects;
      ALTER TABLE tool_effects__migration_v2 RENAME TO tool_effects;
    `);
    for (const object of dependentObjects) db.exec(object.sql);

    const violations = db.prepare('PRAGMA foreign_key_check').all();
    if (violations.length > 0) {
      const error = new Error('tool_effects migration would violate foreign keys');
      error.code = 'TOOL_EFFECT_MIGRATION_FOREIGN_KEY';
      error.violations = violations;
      throw error;
    }
    db.exec('COMMIT');
    transactionStarted = false;
  } catch (error) {
    if (transactionStarted) db.exec('ROLLBACK');
    throw error;
  } finally {
    if (foreignKeysEnabled) db.exec('PRAGMA foreign_keys = ON');
  }
}

function uniqueIndexes(db, table) {
  return db.prepare(`PRAGMA index_list("${table}")`).all()
    .filter((index) => index.unique === 1)
    .map((index) => ({
      name: index.name,
      partial: index.partial === 1,
      columns: db.prepare('SELECT name FROM pragma_index_info(?) ORDER BY seqno').all(index.name)
        .map((column) => column.name),
    }));
}

function sameColumns(actual, expected) {
  return actual.length === expected.length && actual.every((column, index) => column === expected[index]);
}
