import { createHash, randomUUID } from 'node:crypto';
import { DomainError, invariant } from './errors.js';

const SIDE_EFFECTS = new Set(['READ_ONLY', 'SIDE_EFFECT', 'UNKNOWN']);
const IDEMPOTENCY = new Set(['IDEMPOTENT', 'IDEMPOTENCY_KEY_REQUIRED', 'NON_IDEMPOTENT', 'UNKNOWN']);
const EXECUTION_MODES = new Set(['READ_ONLY', 'LOCAL_TRANSACTIONAL', 'EXTERNAL_OUTBOX']);

function now() {
  return new Date().toISOString();
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

function inputHash(value) {
  return createHash('sha256').update(stableJson(value)).digest('hex');
}

function normalizeContract(contract, missingEffectExecutionMode = 'EXTERNAL_OUTBOX') {
  invariant(typeof contract.operationId === 'string' && contract.operationId.length > 0, 'INVALID_TOOL_CONTRACT', 'operationId is required');
  invariant(Number.isInteger(contract.version) && contract.version > 0, 'INVALID_TOOL_CONTRACT', 'positive version is required');
  invariant(SIDE_EFFECTS.has(contract.sideEffect), 'INVALID_TOOL_CONTRACT', 'invalid sideEffect');
  invariant(IDEMPOTENCY.has(contract.idempotency), 'INVALID_TOOL_CONTRACT', 'invalid idempotency');
  const executionMode = contract.sideEffect === 'READ_ONLY'
    ? 'READ_ONLY'
    : (contract.executionMode ?? missingEffectExecutionMode);
  invariant(EXECUTION_MODES.has(executionMode), 'INVALID_TOOL_CONTRACT', 'invalid executionMode');
  invariant(contract.sideEffect !== 'READ_ONLY' || executionMode === 'READ_ONLY', 'INVALID_TOOL_CONTRACT', 'read-only operations must use READ_ONLY execution');
  invariant(contract.sideEffect === 'READ_ONLY' || executionMode !== 'READ_ONLY', 'INVALID_TOOL_CONTRACT', 'effect operations cannot use READ_ONLY execution');
  return Object.freeze({
    operationId: contract.operationId,
    version: contract.version,
    sideEffect: contract.sideEffect,
    idempotency: contract.idempotency,
    executionMode,
    maxAttempts: contract.maxAttempts ?? 1,
    timeoutMs: contract.timeoutMs ?? 30_000,
  });
}

function encodedError(error) {
  return JSON.stringify({
    name: error?.name ?? 'Error',
    code: error?.code ?? null,
    message: error?.message ?? 'Tool execution failed',
  });
}

export class ToolRegistry {
  constructor(db) {
    this.db = db;
    this.executors = new Map();
  }

  register(contract, executor) {
    let normalized = normalizeContract(contract);
    invariant(typeof executor === 'function', 'INVALID_TOOL_EXECUTOR', 'tool executor must be a function');
    const existing = this.db.prepare('SELECT contract_json FROM tool_contracts WHERE operation_id = ? AND version = ?').get(normalized.operationId, normalized.version);
    if (existing) {
      const raw = JSON.parse(existing.contract_json);
      if (!raw.executionMode && !contract.executionMode) {
        normalized = normalizeContract(contract, 'LOCAL_TRANSACTIONAL');
      }
      const encoded = stableJson(normalized);
      const existingNormalized = normalizeContract(raw, 'LOCAL_TRANSACTIONAL');
      if (stableJson(existingNormalized) !== encoded) {
        throw new DomainError('TOOL_CONTRACT_IMMUTABLE', 'Published tool contract version cannot be changed', 409);
      }
      if (!raw.executionMode) {
        this.db.prepare('UPDATE tool_contracts SET contract_json = ? WHERE operation_id = ? AND version = ?')
          .run(encoded, normalized.operationId, normalized.version);
      }
    } else {
      const encoded = stableJson(normalized);
      this.db.prepare('INSERT INTO tool_contracts (operation_id, version, contract_json, created_at) VALUES (?, ?, ?, ?)')
        .run(normalized.operationId, normalized.version, encoded, now());
    }
    this.executors.set(`${normalized.operationId}:${normalized.version}`, executor);
    return normalized;
  }

  get(operationId, version) {
    invariant(typeof operationId === 'string' && operationId.length > 0, 'INVALID_TOOL_OPERATION', 'operationId is required');
    invariant(Number.isInteger(version) && version > 0, 'INVALID_TOOL_VERSION', 'version must be a positive integer');
    const row = this.db.prepare('SELECT contract_json FROM tool_contracts WHERE operation_id = ? AND version = ?').get(operationId, version);
    if (!row) throw new DomainError('TOOL_CONTRACT_NOT_FOUND', 'Tool operation contract not found', 404);
    return normalizeContract(JSON.parse(row.contract_json), 'LOCAL_TRANSACTIONAL');
  }

  list() {
    return this.db.prepare('SELECT contract_json FROM tool_contracts ORDER BY operation_id, version').all()
      .map((row) => normalizeContract(JSON.parse(row.contract_json), 'LOCAL_TRANSACTIONAL'));
  }

  invoke({
    tenantId,
    runId,
    executionEpoch = 0,
    operationId,
    version,
    input = {},
    idempotencyKey = null,
    runFence = null,
    onBlockedReplay = null,
  }) {
    invariant(onBlockedReplay === null || typeof onBlockedReplay === 'function', 'INVALID_REPLAY_DISPOSITION', 'onBlockedReplay must be a function');
    this.#verifyRunFence(runFence, { tenantId, runId, executionEpoch });
    const contract = this.get(operationId, version);
    const executor = this.executors.get(`${operationId}:${version}`);
    if (!executor) throw new DomainError('TOOL_EXECUTOR_NOT_FOUND', 'Tool executor is unavailable', 503);
    if (contract.sideEffect === 'UNKNOWN') {
      throw new DomainError('UNKNOWN_SIDE_EFFECT', 'Unknown side effect requires manual handling', 409);
    }
    if (contract.idempotency === 'IDEMPOTENCY_KEY_REQUIRED' && !idempotencyKey) {
      throw new DomainError('IDEMPOTENCY_KEY_REQUIRED', 'This operation requires an idempotency key');
    }
    if (contract.sideEffect === 'READ_ONLY') {
      return { output: executor({ tenantId, input, db: this.db }), replayed: false, contract };
    }
    if (contract.idempotency === 'NON_IDEMPOTENT' || contract.idempotency === 'UNKNOWN') {
      throw new DomainError('UNSAFE_AUTOMATIC_EFFECT', 'Non-idempotent effect cannot run automatically', 409);
    }

    const hash = inputHash(input);
    const effectiveKey = idempotencyKey ?? `effect-${randomUUID()}`;
    const existing = this.#findEffect({ tenantId, operationId, version, idempotencyKey: effectiveKey });
    if (existing) {
      const replay = this.db.transaction(() => {
        const replayInvocation = { tenantId, runId, executionEpoch, operationId, version, idempotencyKey: effectiveKey };
        this.#verifyRunFence(runFence, replayInvocation);
        const current = this.#findEffect(replayInvocation);
        this.#recordBlockedReplay(current, hash, onBlockedReplay);
        return current;
      });
      if (contract.executionMode === 'EXTERNAL_OUTBOX') {
        return Promise.resolve().then(() => this.#replayEffect(replay, hash, contract));
      }
      return this.#replayEffect(replay, hash, contract);
    }

    const invocation = {
      tenantId,
      runId,
      executionEpoch,
      operationId,
      version,
      input,
      idempotencyKey: effectiveKey,
      runFence,
      onBlockedReplay,
    };
    if (contract.executionMode === 'LOCAL_TRANSACTIONAL') {
      return this.#invokeLocal(invocation, hash, contract, executor);
    }
    return this.#invokeExternal(invocation, hash, contract, executor);
  }

  resolveEffect({ tenantId, runId, effectId, outcome, output = null, error = null }) {
    return this.db.transaction(() => this.resolveEffectInTransaction({ tenantId, runId, effectId, outcome, output, error }));
  }

  resolveEffectInTransaction({ tenantId, runId, effectId, outcome, output = null, error = null }) {
    invariant(outcome === 'COMMITTED' || outcome === 'FAILED', 'INVALID_EFFECT_OUTCOME', 'outcome must be COMMITTED or FAILED');
    const effect = this.db.prepare('SELECT * FROM tool_effects WHERE effect_id = ? AND tenant_id = ? AND run_id = ?')
      .get(effectId, tenantId, runId);
    if (!effect) throw new DomainError('TOOL_EFFECT_NOT_FOUND', 'Tool effect not found', 404);
    if (effect.status !== 'UNKNOWN') {
      if (['COMMITTED', 'FAILED'].includes(effect.status)) {
        throw new DomainError('TOOL_EFFECT_ALREADY_RESOLVED', `Tool effect is already ${effect.status}`, 409);
      }
      throw new DomainError('TOOL_EFFECT_STILL_ACTIVE', `Tool effect is still ${effect.status}`, 409);
    }
    if (effect.execution_mode !== 'EXTERNAL_OUTBOX') {
      throw new DomainError('TOOL_EFFECT_NOT_RECONCILABLE', 'Only uncertain external effects can be manually resolved', 409);
    }
    const timestamp = now();
    const resultJson = outcome === 'COMMITTED' ? JSON.stringify(output ?? {}) : null;
    const errorJson = outcome === 'FAILED' ? JSON.stringify(error ?? { message: 'manually resolved as not applied' }) : null;
    const effectUpdate = this.db.prepare(`
      UPDATE tool_effects SET status = ?, result_json = ?, error_json = ?, committed_at = ?, updated_at = ?
      WHERE effect_id = ? AND tenant_id = ? AND run_id = ? AND status = 'UNKNOWN'
    `).run(outcome, resultJson, errorJson, outcome === 'COMMITTED' ? timestamp : null, timestamp, effectId, tenantId, runId);
    const outboxUpdate = this.db.prepare(`
      UPDATE tool_effect_outbox SET status = ?, last_error_json = ?, updated_at = ?
      WHERE effect_id = ? AND status = 'UNKNOWN'
    `).run(outcome === 'COMMITTED' ? 'DELIVERED' : 'FAILED', errorJson, timestamp, effectId);
    if (effectUpdate.changes !== 1 || outboxUpdate.changes !== 1) {
      throw new DomainError('TOOL_EFFECT_RESOLUTION_CONFLICT', 'Tool effect changed before reconciliation could commit', 409, { effectId });
    }
    return { effectId, status: outcome, output: output ?? null, error: error ?? null };
  }

  fenceInterruptedEffects(runId, newExecutionEpoch) {
    invariant(typeof runId === 'string' && runId.length > 0, 'INVALID_RUN', 'runId is required');
    invariant(Number.isInteger(newExecutionEpoch) && newExecutionEpoch > 0, 'INVALID_EXECUTION_EPOCH', 'newExecutionEpoch must be a positive integer');
    const eligible = this.db.prepare(`
      SELECT count(*) AS count FROM tool_effects
      WHERE run_id = ? AND execution_mode = 'EXTERNAL_OUTBOX'
        AND status IN ('PREPARED', 'DISPATCHING') AND execution_epoch < ?
    `).get(runId, newExecutionEpoch).count;
    if (eligible === 0) return 0;
    const timestamp = now();
    const failure = JSON.stringify({
      name: 'ExecutionEpochFenced',
      code: 'EXECUTION_EPOCH_FENCED',
      message: 'A newer execution epoch superseded the external dispatcher before its outcome was recorded',
    });
    const outboxUpdate = this.db.prepare(`
      UPDATE tool_effect_outbox SET status = 'UNKNOWN',
        last_error_json = COALESCE(last_error_json, ?), updated_at = ?
      WHERE status IN ('READY', 'DISPATCHING') AND effect_id IN (
        SELECT effect_id FROM tool_effects
        WHERE run_id = ? AND execution_mode = 'EXTERNAL_OUTBOX'
          AND status IN ('PREPARED', 'DISPATCHING') AND execution_epoch < ?
      )
    `).run(failure, timestamp, runId, newExecutionEpoch);
    const effectUpdate = this.db.prepare(`
      UPDATE tool_effects SET status = 'UNKNOWN', error_json = COALESCE(error_json, ?), updated_at = ?
      WHERE run_id = ? AND execution_mode = 'EXTERNAL_OUTBOX'
        AND status IN ('PREPARED', 'DISPATCHING') AND execution_epoch < ?
    `).run(failure, timestamp, runId, newExecutionEpoch);
    if (effectUpdate.changes !== eligible || outboxUpdate.changes !== eligible) {
      throw new DomainError('TOOL_EFFECT_OUTBOX_INTEGRITY', 'Tool effect and outbox could not be fenced together', 500, {
        runId,
        effects: effectUpdate.changes,
        outbox: outboxUpdate.changes,
        expected: eligible,
      });
    }
    return effectUpdate.changes;
  }

  #invokeLocal(invocation, hash, contract, executor) {
    const outcome = this.db.transaction(() => {
      this.#verifyRunFence(invocation.runFence, invocation);
      const raced = this.#findEffect(invocation);
      if (raced) {
        this.#recordBlockedReplay(raced, hash, invocation.onBlockedReplay);
        return { existing: raced };
      }
      const effectId = `eff_${randomUUID()}`;
      const timestamp = now();
      this.db.prepare(`
        INSERT INTO tool_effects
          (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
           input_hash, input_json, execution_epoch, execution_mode, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'LOCAL_TRANSACTIONAL', 'PREPARED', ?, ?)
      `).run(
        effectId,
        invocation.tenantId,
        invocation.runId,
        invocation.operationId,
        invocation.version,
        invocation.idempotencyKey,
        hash,
        JSON.stringify(invocation.input),
        invocation.executionEpoch,
        timestamp,
        timestamp,
      );
      const output = executor({
        tenantId: invocation.tenantId,
        input: invocation.input,
        db: this.db,
        effectId,
        idempotencyKey: invocation.idempotencyKey,
        operationId: invocation.operationId,
        version: invocation.version,
      });
      if (output && typeof output.then === 'function') {
        throw new DomainError('ASYNC_LOCAL_TOOL_UNSUPPORTED', 'LOCAL_TRANSACTIONAL executors must complete synchronously', 500);
      }
      const committed = this.db.prepare(`
        UPDATE tool_effects SET status = 'COMMITTED', result_json = ?, committed_at = ?, updated_at = ?
        WHERE effect_id = ? AND status = 'PREPARED'
      `).run(JSON.stringify(output ?? null), timestamp, timestamp, effectId);
      if (committed.changes !== 1) {
        throw new DomainError('TOOL_EFFECT_COMMIT_CONFLICT', 'Local effect changed before commit', 409, { effectId });
      }
      return { output, replayed: false, contract, effectId };
    });
    if (outcome.existing) return this.#replayEffect(outcome.existing, hash, contract);
    return outcome;
  }

  async #invokeExternal(invocation, hash, contract, executor) {
    const prepared = this.db.transaction(() => {
      this.#verifyRunFence(invocation.runFence, invocation);
      const raced = this.#findEffect(invocation);
      if (raced) {
        this.#recordBlockedReplay(raced, hash, invocation.onBlockedReplay);
        return { existing: raced };
      }
      const effectId = `eff_${randomUUID()}`;
      const outboxId = `out_${randomUUID()}`;
      const timestamp = now();
      this.db.prepare(`
        INSERT INTO tool_effects
          (effect_id, tenant_id, run_id, operation_id, contract_version, idempotency_key,
           input_hash, input_json, execution_epoch, execution_mode, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'EXTERNAL_OUTBOX', 'PREPARED', ?, ?)
      `).run(
        effectId,
        invocation.tenantId,
        invocation.runId,
        invocation.operationId,
        invocation.version,
        invocation.idempotencyKey,
        hash,
        JSON.stringify(invocation.input),
        invocation.executionEpoch,
        timestamp,
        timestamp,
      );
      this.db.prepare(`
        INSERT INTO tool_effect_outbox
          (outbox_id, effect_id, status, attempt_count, payload_json, created_at, updated_at)
        VALUES (?, ?, 'READY', 0, ?, ?, ?)
      `).run(outboxId, effectId, JSON.stringify({
        tenantId: invocation.tenantId,
        runId: invocation.runId,
        executionEpoch: invocation.executionEpoch,
        operationId: invocation.operationId,
        version: invocation.version,
        input: invocation.input,
        idempotencyKey: invocation.idempotencyKey,
      }), timestamp, timestamp);
      return { effectId, outboxId };
    });
    if (prepared.existing) return this.#replayEffect(prepared.existing, hash, contract);

    const dispatchAt = now();
    this.db.transaction(() => {
      this.#verifyRunFence(invocation.runFence, invocation);
      const effect = this.db.prepare("UPDATE tool_effects SET status = 'DISPATCHING', updated_at = ? WHERE effect_id = ? AND status = 'PREPARED'")
        .run(dispatchAt, prepared.effectId);
      const outbox = this.db.prepare("UPDATE tool_effect_outbox SET status = 'DISPATCHING', attempt_count = attempt_count + 1, updated_at = ? WHERE outbox_id = ? AND status = 'READY'")
        .run(dispatchAt, prepared.outboxId);
      if (effect.changes !== 1 || outbox.changes !== 1) {
        throw new DomainError('EFFECT_STATUS_UNKNOWN', 'External effect could not be safely claimed for dispatch', 409, { effectId: prepared.effectId });
      }
    });

    let output;
    try {
      output = await executor({
        tenantId: invocation.tenantId,
        input: invocation.input,
        db: this.db,
        effectId: prepared.effectId,
        idempotencyKey: invocation.idempotencyKey,
        operationId: invocation.operationId,
        version: invocation.version,
      });
    } catch (error) {
      return this.#finalizeExternalFailure(prepared, error);
    }

    const committedAt = now();
    try {
      this.db.transaction(() => {
        const effect = this.db.prepare(`
          UPDATE tool_effects SET status = 'COMMITTED', result_json = ?, error_json = NULL,
            committed_at = ?, updated_at = ?
          WHERE effect_id = ? AND status = 'DISPATCHING'
        `).run(JSON.stringify(output ?? null), committedAt, committedAt, prepared.effectId);
        const outbox = this.db.prepare(`
          UPDATE tool_effect_outbox SET status = 'DELIVERED', last_error_json = NULL, updated_at = ?
          WHERE outbox_id = ? AND status = 'DISPATCHING'
        `).run(committedAt, prepared.outboxId);
        if (effect.changes !== 1 || outbox.changes !== 1) {
          throw new DomainError('EFFECT_FINALIZATION_CONFLICT', 'External effect changed before success could be recorded', 409, { effectId: prepared.effectId });
        }
      });
    } catch (error) {
      if (error.code !== 'EFFECT_FINALIZATION_CONFLICT') {
        try {
          this.#casExternalFailure(prepared, 'UNKNOWN', encodedError(error));
        } catch {
          // The durable pair may already have been fenced or manually resolved.
        }
      }
      throw new DomainError('EFFECT_STATUS_UNKNOWN', 'External effect outcome is unknown and requires reconciliation', 409, { effectId: prepared.effectId });
    }
    return { output, replayed: false, contract, effectId: prepared.effectId };
  }

  #finalizeExternalFailure(prepared, error) {
    const requestedStatus = error?.effectOutcome === 'FAILED' ? 'FAILED' : 'UNKNOWN';
    try {
      this.#casExternalFailure(prepared, requestedStatus, encodedError(error));
    } catch (conflict) {
      if (conflict.code !== 'EFFECT_FINALIZATION_CONFLICT') throw conflict;
      throw new DomainError('EFFECT_STATUS_UNKNOWN', 'External effect outcome was superseded and requires reconciliation', 409, { effectId: prepared.effectId });
    }
    if (requestedStatus === 'FAILED') {
      throw new DomainError('EFFECT_EXECUTION_FAILED', 'External effect was explicitly rejected before execution', 409, { effectId: prepared.effectId });
    }
    throw new DomainError('EFFECT_STATUS_UNKNOWN', 'External effect outcome is unknown and requires reconciliation', 409, { effectId: prepared.effectId });
  }

  #casExternalFailure(prepared, status, failure) {
    const failedAt = now();
    this.db.transaction(() => {
      const effect = this.db.prepare(`
        UPDATE tool_effects SET status = ?, error_json = ?, updated_at = ?
        WHERE effect_id = ? AND status = 'DISPATCHING'
      `).run(status, failure, failedAt, prepared.effectId);
      const outbox = this.db.prepare(`
        UPDATE tool_effect_outbox SET status = ?, last_error_json = ?, updated_at = ?
        WHERE outbox_id = ? AND status = 'DISPATCHING'
      `).run(status, failure, failedAt, prepared.outboxId);
      if (effect.changes !== 1 || outbox.changes !== 1) {
        throw new DomainError('EFFECT_FINALIZATION_CONFLICT', 'External effect changed before failure could be recorded', 409, { effectId: prepared.effectId });
      }
    });
  }

  #verifyRunFence(runFence, invocation) {
    if (!runFence) return;
    const structurallyValid = typeof runFence.tenantId === 'string'
      && typeof runFence.runId === 'string'
      && typeof runFence.leaseId === 'string'
      && typeof runFence.workerId === 'string'
      && Number.isInteger(runFence.executionEpoch)
      && Number.isInteger(runFence.stateVersion)
      && runFence.tenantId === invocation.tenantId
      && runFence.runId === invocation.runId
      && runFence.executionEpoch === invocation.executionEpoch;
    if (!structurallyValid) throw new DomainError('FENCED_OUT', 'Worker run fence is malformed or does not match the invocation', 409);
    const current = this.db.prepare(`
      SELECT 1 AS valid FROM runs
      WHERE run_id = ? AND tenant_id = ? AND lease_id = ? AND lease_owner = ?
        AND execution_epoch = ? AND state_version = ?
        AND lease_expires_at >= CAST(strftime('%s', 'now') AS INTEGER)
    `).get(
      runFence.runId,
      runFence.tenantId,
      runFence.leaseId,
      runFence.workerId,
      runFence.executionEpoch,
      runFence.stateVersion,
    );
    if (!current) throw new DomainError('FENCED_OUT', 'Worker lease, execution epoch, or state version is stale', 409);
  }

  #findEffect({ tenantId, operationId, version, idempotencyKey }) {
    return this.db.prepare(`
      SELECT * FROM tool_effects
      WHERE tenant_id = ? AND operation_id = ? AND contract_version = ? AND idempotency_key = ?
    `).get(tenantId, operationId, version, idempotencyKey);
  }

  #recordBlockedReplay(existing, hash, onBlockedReplay) {
    if (!existing) throw new DomainError('TOOL_EFFECT_REPLAY_CONFLICT', 'Tool effect disappeared before replay', 409);
    if (existing.input_hash !== hash) throw new DomainError('IDEMPOTENCY_CONFLICT', 'Idempotency key was used with different input', 409);
    if (existing.status !== 'COMMITTED' && onBlockedReplay) {
      onBlockedReplay({
        effectId: existing.effect_id,
        ownerRunId: existing.run_id,
        status: existing.status,
        executionMode: existing.execution_mode,
      });
    }
  }

  #replayEffect(existing, hash, contract) {
    if (existing.input_hash !== hash) throw new DomainError('IDEMPOTENCY_CONFLICT', 'Idempotency key was used with different input', 409);
    if (existing.status === 'COMMITTED') {
      return { output: JSON.parse(existing.result_json), replayed: true, contract, effectId: existing.effect_id };
    }
    if (existing.status === 'FAILED') {
      throw new DomainError('EFFECT_EXECUTION_FAILED', 'Previous external effect was explicitly rejected', 409, { effectId: existing.effect_id });
    }
    throw new DomainError('EFFECT_STATUS_UNKNOWN', 'Previous effect has not reached a safely replayable state', 409, { effectId: existing.effect_id, status: existing.status });
  }

}

export function registerBuiltinTools(registry) {
  registry.register(
    { operationId: 'echo', version: 1, sideEffect: 'READ_ONLY', idempotency: 'IDEMPOTENT' },
    ({ input }) => ({ echoed: input }),
  );
  registry.register(
    {
      operationId: 'counter.increment',
      version: 1,
      sideEffect: 'SIDE_EFFECT',
      idempotency: 'IDEMPOTENCY_KEY_REQUIRED',
      executionMode: 'LOCAL_TRANSACTIONAL',
      maxAttempts: 3,
    },
    ({ tenantId, input, db }) => {
      invariant(typeof input.name === 'string' && input.name.length > 0, 'INVALID_TOOL_INPUT', 'counter name is required');
      invariant(Number.isInteger(input.amount), 'INVALID_TOOL_INPUT', 'counter amount must be an integer');
      db.prepare(`
        INSERT INTO counters (tenant_id, name, value) VALUES (?, ?, ?)
        ON CONFLICT (tenant_id, name) DO UPDATE SET value = value + excluded.value
      `).run(tenantId, input.name, input.amount);
      return { name: input.name, value: db.prepare('SELECT value FROM counters WHERE tenant_id = ? AND name = ?').get(tenantId, input.name).value };
    },
  );
  registry.register(
    { operationId: 'effect.opaque', version: 1, sideEffect: 'UNKNOWN', idempotency: 'UNKNOWN' },
    () => ({ accepted: true }),
  );
}
