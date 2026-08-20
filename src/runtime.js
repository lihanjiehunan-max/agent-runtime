import { randomUUID } from 'node:crypto';
import { CHECKPOINT_INTEGRITY_VERSION, checkpointHash, checkpointHashMatches } from './checkpoint-integrity.js';
import { DomainError, invariant } from './errors.js';

const TERMINAL = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED']);

function now() {
  return new Date().toISOString();
}

function parse(value, fallback = {}) {
  return value ? JSON.parse(value) : fallback;
}

export class Runtime {
  constructor(db, { contexts, tools }) {
    this.db = db;
    this.contexts = contexts;
    this.tools = tools;
  }

  createRun({ tenantId, goal, actorId = 'user' }) {
    invariant(typeof tenantId === 'string' && tenantId.length > 0, 'INVALID_TENANT', 'tenantId is required');
    invariant(typeof goal === 'string' && goal.trim().length > 0, 'INVALID_GOAL', 'goal is required');
    const runId = `run_${randomUUID()}`;
    this.db.transaction(() => {
      this.db.prepare(`
        INSERT INTO runs (run_id, tenant_id, goal, status, created_at, updated_at)
        VALUES (?, ?, ?, 'CREATED', ?, ?)
      `).run(runId, tenantId, goal.trim(), now(), now());
      this.#appendEvent(runId, tenantId, 'RUN_CREATED', actorId, 0, { goal: goal.trim() });
    });
    return this.getRun(runId, tenantId);
  }

  acquireLease(runId, options) {
    return this.#acquireLease(runId, { ...options, allowedStatuses: ['CREATED', 'RUNNING'] }, false);
  }

  #acquireLease(runId, {
    tenantId,
    workerId,
    ttlSeconds = 30,
    force = false,
    actorId = workerId,
    takeoverReason = null,
    allowedStatuses = null,
  }, allowPendingEffects) {
    invariant(typeof workerId === 'string' && workerId.length > 0, 'INVALID_WORKER', 'workerId is required');
    invariant(Number.isInteger(ttlSeconds) && ttlSeconds > 0, 'INVALID_LEASE_TTL', 'ttlSeconds must be a positive integer');
    invariant(typeof force === 'boolean', 'INVALID_FORCE_FLAG', 'force must be a boolean');
    const outcome = this.db.transaction(() => {
      const run = this.#row(runId, tenantId);
      if (TERMINAL.has(run.status)) throw new DomainError('RUN_TERMINAL', 'Terminal run cannot be leased', 409);
      if (['WAITING_MANUAL', 'RECOVERY_BLOCKED'].includes(run.status)) {
        throw new DomainError('MANUAL_INTERVENTION_REQUIRED', `Run status ${run.status} requires an explicit operator decision`, 409);
      }
      if (allowedStatuses && !allowedStatuses.includes(run.status)) {
        throw new DomainError('INVALID_RUN_TRANSITION', `Run status ${run.status} does not allow lease acquisition`, 409);
      }
      const dbNow = this.#dbNow();
      if (!force && run.lease_owner && run.lease_expires_at >= dbNow && run.lease_owner !== workerId) {
        throw new DomainError('LEASE_HELD', 'Run lease is held by another worker', 409);
      }
      const executionEpoch = run.execution_epoch + 1;
      const fencedEffects = this.tools.fenceInterruptedEffects?.(runId, executionEpoch) ?? 0;
      const dispositionError = this.#applyEffectDisposition(run, actorId);
      if (dispositionError && dispositionError.code !== 'PENDING_EFFECTS') {
        return { blocked: true, error: dispositionError };
      }
      const pendingEffects = this.db.prepare(`
        SELECT count(*) AS count FROM tool_effects
        WHERE run_id = ? AND tenant_id = ? AND status NOT IN ('COMMITTED', 'FAILED')
      `).get(runId, tenantId).count;
      if (pendingEffects > 0 && !allowPendingEffects) {
        this.db.prepare(`
          UPDATE runs SET status = 'WAITING_MANUAL', execution_epoch = ?, lease_id = NULL,
            lease_owner = NULL, lease_expires_at = NULL, state_version = state_version + 1, updated_at = ?
          WHERE run_id = ? AND tenant_id = ?
        `).run(executionEpoch, now(), runId, tenantId);
        this.#appendEvent(runId, tenantId, 'RUN_WAITING_MANUAL', actorId, executionEpoch, {
          reason: 'UNRESOLVED_TOOL_EFFECTS',
          pendingEffects,
          fencedEffects,
          previousLeaseOwner: run.lease_owner ?? null,
          takeoverReason,
        });
        return {
          blocked: true,
          error: new DomainError(
            'EFFECT_STATUS_UNKNOWN',
            'Run has unresolved Tool effects and requires manual reconciliation',
            409,
            { executionEpoch, pendingEffects, fencedEffects },
          ),
        };
      }
      const leaseId = randomUUID();
      const leaseExpiresAt = dbNow + ttlSeconds;
      this.db.prepare(`
        UPDATE runs SET execution_epoch = ?, lease_id = ?, lease_owner = ?, lease_expires_at = ?,
          state_version = state_version + 1, updated_at = ? WHERE run_id = ?
      `).run(executionEpoch, leaseId, workerId, leaseExpiresAt, now(), runId);
      this.#appendEvent(runId, tenantId, 'LEASE_ACQUIRED', actorId, executionEpoch, {
        leaseId,
        workerId,
        force,
        previousLeaseOwner: run.lease_owner ?? null,
        takeoverReason,
      });
      return { runId, tenantId, workerId, leaseId, executionEpoch, leaseExpiresAt };
    });
    if (outcome.blocked) {
      throw outcome.error;
    }
    return outcome;
  }

  start(runId, lease) {
    return this.#transition(runId, lease, ['CREATED'], 'RUNNING', 'RUN_STARTED');
  }

  complete(runId, lease, result) {
    return this.#transition(runId, lease, ['RUNNING'], 'SUCCEEDED', 'RUN_SUCCEEDED', result);
  }

  fail(runId, lease, error) {
    return this.#transition(runId, lease, ['RUNNING', 'RECOVERING'], 'FAILED', 'RUN_FAILED', error);
  }

  readContext(runId, lease, contextRefId) {
    return this.#workerTransaction(runId, lease, ['RUNNING'], (run) => {
      const actorId = this.#leaseActor(lease);
      const value = this.contexts.read(contextRefId, { tenantId: run.tenant_id, runId, actorId });
      const eventSeq = this.#appendEvent(runId, run.tenant_id, 'CONTEXT_READ', actorId, lease.executionEpoch, {
        contextRefId,
        contentHash: value.contentHash,
        classification: value.classification,
      });
      this.db.prepare(`
        INSERT INTO run_context_reads (run_id, context_ref_id, first_read_event_seq, read_at)
        VALUES (?, ?, ?, ?) ON CONFLICT (run_id, context_ref_id) DO NOTHING
      `).run(runId, contextRefId, eventSeq, now());
      return value;
    });
  }

  invokeTool(runId, lease, invocation) {
    const actorId = this.#leaseActor(lease);
    let replayDispositionCommitted = false;
    const preflight = this.#workerTransaction(runId, lease, ['RUNNING'], (current) => {
      const contract = this.tools.get(invocation.operationId, invocation.version);
      let policyError = null;
      if (contract.sideEffect === 'UNKNOWN') {
        policyError = new DomainError('UNKNOWN_SIDE_EFFECT', 'Unknown side effect requires manual handling', 409);
      } else if (contract.sideEffect !== 'READ_ONLY' && ['NON_IDEMPOTENT', 'UNKNOWN'].includes(contract.idempotency)) {
        policyError = new DomainError('UNSAFE_AUTOMATIC_EFFECT', 'Non-idempotent effect cannot run automatically', 409);
      }
      if (!policyError) return { run: current };
      this.#commitEffectGate(current, actorId, policyError.code, {
        operationId: invocation.operationId,
        version: invocation.version,
      });
      return { policyError };
    });
    if (preflight.policyError) throw preflight.policyError;
    const run = preflight.run;
    const onBlockedReplay = ({ effectId, ownerRunId, status }) => {
      const current = this.#verifyLease(runId, lease, ['RUNNING']);
      const localDisposition = this.#applyEffectDisposition(current, actorId);
      if (localDisposition && localDisposition.code !== 'PENDING_EFFECTS') {
        replayDispositionCommitted = true;
        return;
      }
      if (status === 'FAILED' && !localDisposition) {
        this.#commitEffectFailure(current, actorId, [effectId]);
      } else {
        this.#commitEffectGate(
          current,
          actorId,
          status === 'FAILED' ? 'FAILED_EFFECT_WITH_PENDING' : 'REPLAYED_UNRESOLVED_TOOL_EFFECT',
          { effectIds: [effectId], ownerRunId, replayedStatus: status },
        );
      }
      replayDispositionCommitted = true;
    };
    const commit = (result) => {
      this.#workerTransaction(runId, lease, ['RUNNING'], (current) => {
        this.#appendEvent(runId, current.tenant_id, 'TOOL_COMMITTED', actorId, lease.executionEpoch, {
          operationId: invocation.operationId,
          version: invocation.version,
          replayed: result.replayed,
          sideEffect: result.contract.sideEffect,
          effectId: result.effectId ?? null,
        });
      });
      return result;
    };
    const reject = (error) => {
      if (replayDispositionCommitted) throw error;
      if (['UNKNOWN_SIDE_EFFECT', 'EFFECT_STATUS_UNKNOWN', 'UNSAFE_AUTOMATIC_EFFECT', 'EFFECT_EXECUTION_FAILED'].includes(error.code)) {
        try {
          this.#workerTransaction(runId, lease, ['RUNNING'], (current) => {
            if (error.code === 'EFFECT_EXECUTION_FAILED') {
              this.#commitEffectFailure(current, actorId, error.details?.effectId ? [error.details.effectId] : []);
              return;
            }
            const executionEpoch = current.execution_epoch + 1;
            const updated = this.db.prepare(`
              UPDATE runs SET status = 'WAITING_MANUAL', execution_epoch = ?, lease_id = NULL,
                lease_owner = NULL, lease_expires_at = NULL, state_version = state_version + 1, updated_at = ?
              WHERE run_id = ? AND tenant_id = ? AND state_version = ? AND execution_epoch = ?
            `).run(executionEpoch, now(), runId, current.tenant_id, current.state_version, current.execution_epoch);
            if (updated.changes !== 1) throw new DomainError('STATE_VERSION_CONFLICT', 'Run changed before manual Tool gate could commit', 409);
            this.tools.fenceInterruptedEffects?.(runId, executionEpoch);
            this.#appendEvent(runId, current.tenant_id, 'RUN_WAITING_MANUAL', actorId, executionEpoch, {
              reason: error.code,
              operationId: invocation.operationId,
              effectId: error.details?.effectId ?? null,
            });
          });
        } catch (dispositionError) {
          if (!['EFFECT_STATUS_UNKNOWN', 'EFFECT_EXECUTION_FAILED', 'PENDING_EFFECTS'].includes(dispositionError.code)) {
            throw dispositionError;
          }
        }
      }
      throw error;
    };
    try {
      const result = this.tools.invoke({
        ...invocation,
        tenantId: run.tenant_id,
        runId,
        executionEpoch: lease.executionEpoch,
        runFence: {
          tenantId: run.tenant_id,
          runId,
          leaseId: lease.leaseId,
          workerId: lease.workerId,
          executionEpoch: lease.executionEpoch,
          stateVersion: run.state_version,
        },
        onBlockedReplay,
      });
      if (result && typeof result.then === 'function') return result.then(commit, reject);
      return commit(result);
    } catch (error) {
      return reject(error);
    }
  }

  checkpoint(runId, lease, { kind, state }) {
    invariant(kind === 'FULL' || kind === 'DELTA', 'INVALID_CHECKPOINT_KIND', 'Checkpoint kind must be FULL or DELTA');
    invariant(state && typeof state === 'object' && !Array.isArray(state), 'INVALID_CHECKPOINT_STATE', 'Checkpoint state must be an object');
    return this.#workerTransaction(runId, lease, ['RUNNING', 'RECOVERING'], (run) => {
      const checkpoints = this.#loadVerifiedCheckpoints(runId);
      const previous = checkpoints.filter((entry) => entry.status === 'COMMITTED').at(-1);
      if (!previous && kind !== 'FULL') throw new DomainError('CHECKPOINT_ANCHOR_REQUIRED', 'First checkpoint must be a full anchor', 409);
      const checkpointId = `cp_${randomUUID()}`;
      const eventSeq = this.#appendEvent(runId, run.tenant_id, 'CHECKPOINT_COMMITTED', this.#leaseActor(lease), lease.executionEpoch, { checkpointId, kind });
      const encoded = JSON.stringify(state);
      const checkpointSeq = (checkpoints.at(-1)?.checkpoint_seq ?? 0) + 1;
      const createdAt = now();
      const checkpoint = {
        checkpoint_id: checkpointId,
        run_id: runId,
        checkpoint_seq: checkpointSeq,
        parent_checkpoint_id: previous?.checkpoint_id ?? null,
        kind,
        status: 'COMMITTED',
        event_seq: eventSeq,
        execution_epoch: lease.executionEpoch,
        schema_version: 1,
        state_json: encoded,
        integrity_version: CHECKPOINT_INTEGRITY_VERSION,
        created_at: createdAt,
      };
      this.db.prepare(`
        INSERT INTO checkpoints
          (checkpoint_id, run_id, checkpoint_seq, parent_checkpoint_id, kind, status, event_seq,
           execution_epoch, schema_version, state_json, integrity_version, content_hash, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        checkpoint.checkpoint_id,
        checkpoint.run_id,
        checkpoint.checkpoint_seq,
        checkpoint.parent_checkpoint_id,
        checkpoint.kind,
        checkpoint.status,
        checkpoint.event_seq,
        checkpoint.execution_epoch,
        checkpoint.schema_version,
        checkpoint.state_json,
        checkpoint.integrity_version,
        checkpointHash(checkpoint),
        checkpoint.created_at,
      );
      const merged = kind === 'FULL' ? state : { ...parse(run.runtime_state_json), ...state };
      this.#fencedUpdate(run, lease, 'runtime_state_json = ?, state_version = state_version + 1, updated_at = ?', [JSON.stringify(merged), now()]);
      return { checkpointId, checkpointSeq, eventSeq, executionEpoch: lease.executionEpoch, state: merged };
    });
  }

  recover(runId, { tenantId, workerId, actorId = workerId, reason = 'worker recovery' }) {
    this.#reconcileRevokedReads(runId, tenantId, actorId);
    const lease = this.#acquireLease(runId, {
      tenantId,
      workerId,
      force: true,
      actorId,
      takeoverReason: reason,
      allowedStatuses: ['CREATED', 'RUNNING', 'RECOVERING'],
    }, true);
    const recoverySnapshot = this.#verifyLease(runId, lease);
    const pending = this.db.prepare("SELECT count(*) AS count FROM tool_effects WHERE run_id = ? AND status NOT IN ('COMMITTED', 'FAILED')").get(runId).count;
    if (pending > 0) {
      this.db.transaction(() => {
        this.#fencedUpdate(recoverySnapshot, lease, "status = 'WAITING_MANUAL', state_version = state_version + 1, updated_at = ?", [now()]);
        this.#appendEvent(runId, tenantId, 'RECOVERY_BLOCKED', actorId, lease.executionEpoch, { pendingEffects: pending });
      });
      return { ...lease, status: 'WAITING_MANUAL', state: null };
    }
    let state;
    try {
      state = this.#rebuildCheckpointState(runId);
    } catch (error) {
      if (error.code !== 'CHECKPOINT_CORRUPT') throw error;
      this.db.transaction(() => {
        this.#fencedUpdate(recoverySnapshot, lease, "status = 'RECOVERY_BLOCKED', state_version = state_version + 1, updated_at = ?", [now()]);
        this.#appendEvent(runId, tenantId, 'RECOVERY_BLOCKED', actorId, lease.executionEpoch, { reason: error.code, details: error.details });
      });
      throw error;
    }
    this.db.transaction(() => {
      this.#fencedUpdate(recoverySnapshot, lease, "status = 'RUNNING', runtime_state_json = ?, state_version = state_version + 1, updated_at = ?", [JSON.stringify(state), now()]);
      this.#appendEvent(runId, tenantId, 'RUN_RECOVERED', actorId, lease.executionEpoch, { state });
    });
    return { ...lease, status: 'RUNNING', state };
  }

  resolveManual(runId, { tenantId, actorId, decision, reason }) {
    invariant(typeof actorId === 'string' && actorId.length > 0, 'INVALID_ACTOR', 'actorId is required');
    invariant(decision === 'RESUME' || decision === 'FAIL', 'INVALID_MANUAL_DECISION', 'decision must be RESUME or FAIL');
    invariant(typeof reason === 'string' && reason.trim().length > 0, 'MANUAL_REASON_REQUIRED', 'manual resolution reason is required');
    return this.db.transaction(() => {
      const run = this.#row(runId, tenantId);
      if (!['WAITING_MANUAL', 'RECOVERY_BLOCKED'].includes(run.status)) {
        throw new DomainError('INVALID_RUN_TRANSITION', `Run status ${run.status} does not allow manual resolution`, 409);
      }
      if (decision === 'RESUME') {
        const pending = this.db.prepare("SELECT count(*) AS count FROM tool_effects WHERE run_id = ? AND status NOT IN ('COMMITTED', 'FAILED')").get(runId).count;
        if (pending > 0) {
          throw new DomainError('PENDING_EFFECT_RESOLUTION_REQUIRED', 'All uncertain Tool effects must be reconciled before resuming', 409, { pendingEffects: pending });
        }
        const failedEffects = this.#effectLedgerState(runId, tenantId).unacknowledgedFailed;
        if (failedEffects.length > 0) {
          throw new DomainError(
            'RUN_FAILURE_REQUIRES_FAIL',
            'A known Tool execution failure requires the operator to fail the Run',
            409,
            { effectIds: failedEffects.map((effect) => effect.effect_id) },
          );
        }
      }
      const executionEpoch = run.execution_epoch + 1;
      const status = decision === 'RESUME' ? 'RECOVERING' : 'FAILED';
      const result = this.db.prepare(`
        UPDATE runs SET status = ?, execution_epoch = ?, lease_id = NULL, lease_owner = NULL,
          lease_expires_at = NULL, state_version = state_version + 1, result_json = ?, updated_at = ?
        WHERE run_id = ? AND tenant_id = ? AND state_version = ? AND execution_epoch = ?
      `).run(
        status,
        executionEpoch,
        decision === 'FAIL' ? JSON.stringify({ reason: reason.trim(), resolvedBy: actorId }) : null,
        now(),
        runId,
        tenantId,
        run.state_version,
        run.execution_epoch,
      );
      if (result.changes !== 1) throw new DomainError('STATE_VERSION_CONFLICT', 'Run changed before manual resolution could commit', 409);
      this.tools.fenceInterruptedEffects?.(runId, executionEpoch);
      this.#appendEvent(runId, tenantId, 'MANUAL_RESOLVED', actorId, executionEpoch, { decision, reason: reason.trim() });
      return this.getRun(runId, tenantId);
    });
  }

  resolveToolEffect(runId, { tenantId, actorId, effectId, outcome, reason, output = null }) {
    invariant(typeof actorId === 'string' && actorId.length > 0, 'INVALID_ACTOR', 'actorId is required');
    invariant(typeof reason === 'string' && reason.trim().length > 0, 'MANUAL_REASON_REQUIRED', 'effect resolution reason is required');
    return this.db.transaction(() => {
      const current = this.#row(runId, tenantId);
      if (!['WAITING_MANUAL', 'RECOVERY_BLOCKED'].includes(current.status)) {
        throw new DomainError('INVALID_RUN_TRANSITION', `Run status ${current.status} does not allow effect resolution`, 409);
      }
      const resolved = this.tools.resolveEffectInTransaction({
        tenantId,
        runId,
        effectId,
        outcome,
        output,
        error: outcome === 'FAILED' ? { message: reason.trim(), resolvedBy: actorId } : null,
      });
      this.#appendEvent(runId, tenantId, 'TOOL_EFFECT_RESOLVED', actorId, current.execution_epoch, {
        effectId,
        outcome,
        reason: reason.trim(),
      });
      return resolved;
    });
  }

  reconcileRevokedContextReads(contextRefId, { tenantId, actorId = 'system' }) {
    const runs = this.db.prepare(`
      SELECT reads.run_id FROM run_context_reads AS reads
      JOIN runs ON runs.run_id = reads.run_id
      WHERE reads.context_ref_id = ? AND runs.tenant_id = ?
      ORDER BY reads.run_id
    `).all(contextRefId, tenantId);
    const results = [];
    for (const { run_id: runId } of runs) {
      try {
        results.push({ runId, ...this.reconcileRevokedContext(runId, contextRefId, { tenantId, actorId }) });
      } catch (error) {
        if (error.code !== 'CHECKPOINT_CORRUPT') throw error;
        results.push({
          runId,
          action: 'RECOVERY_BLOCKED',
          error: { code: error.code, message: error.message, details: error.details },
        });
      }
    }
    return results;
  }

  reconcileRevokedContext(runId, contextRefId, { tenantId, actorId = 'system' }) {
    const outcome = this.db.transaction(() => {
      const run = this.#row(runId, tenantId);
      const context = this.db.prepare('SELECT revoked_at FROM contexts WHERE context_ref_id = ? AND tenant_id = ?')
        .get(contextRefId, tenantId);
      if (!context) throw new DomainError('CONTEXT_NOT_FOUND', 'Context reference not found', 404);
      if (!context.revoked_at) return { action: 'CONTINUE' };
      const read = this.db.prepare('SELECT * FROM run_context_reads WHERE run_id = ? AND context_ref_id = ?').get(runId, contextRefId);
      if (!read) return { action: 'CONTINUE' };

      const prior = this.db.prepare(`
        SELECT event_type, payload_json FROM run_events
        WHERE run_id = ? AND event_type IN ('REVOKED_CONTEXT_ROLLBACK', 'REVOKED_CONTEXT_REQUIRES_MANUAL')
        ORDER BY event_seq DESC
      `).all(runId).find((event) => parse(event.payload_json).contextRefId === contextRefId);
      if (prior) {
        const payload = parse(prior.payload_json);
        return prior.event_type === 'REVOKED_CONTEXT_REQUIRES_MANUAL'
          ? { action: 'WAITING_MANUAL' }
          : { action: 'ROLLBACK', checkpointId: payload.checkpointId ?? null, checkpointState: parse(run.runtime_state_json) };
      }
      if (['WAITING_MANUAL', 'RECOVERY_BLOCKED'].includes(run.status)) {
        return { action: 'MANUAL_REQUIRED' };
      }

      const effects = this.db.prepare("SELECT count(*) AS count FROM tool_effects WHERE run_id = ? AND status = 'COMMITTED' AND committed_at >= ?").get(runId, read.read_at).count;
      const fencedEpoch = run.execution_epoch + 1;
      let checkpoints;
      let checkpoint;
      let checkpointState;
      let invalidate;
      try {
        checkpoints = this.#loadVerifiedCheckpoints(runId);
        checkpoint = checkpoints.filter((entry) => entry.status === 'COMMITTED' && entry.event_seq < read.first_read_event_seq).at(-1);
        checkpointState = checkpoint ? this.#materializeCheckpointState(runId, checkpoint.checkpoint_id, checkpoints) : {};
        invalidate = checkpoints.filter((entry) => entry.status === 'COMMITTED' && entry.event_seq >= read.first_read_event_seq);
      } catch (error) {
        if (error.code !== 'CHECKPOINT_CORRUPT') throw error;
        const blocked = this.db.prepare(`
          UPDATE runs SET status = 'RECOVERY_BLOCKED', execution_epoch = ?, lease_id = NULL,
            lease_owner = NULL, lease_expires_at = NULL, state_version = state_version + 1, updated_at = ?
          WHERE run_id = ? AND tenant_id = ? AND state_version = ? AND execution_epoch = ?
        `).run(fencedEpoch, now(), runId, tenantId, run.state_version, run.execution_epoch);
        if (blocked.changes !== 1) throw new DomainError('STATE_VERSION_CONFLICT', 'Run changed before corrupt revoked Context reconciliation could be fenced', 409);
        this.tools.fenceInterruptedEffects?.(runId, fencedEpoch);
        this.#appendEvent(runId, tenantId, 'REVOKED_CONTEXT_RECOVERY_BLOCKED', actorId, fencedEpoch, {
          contextRefId,
          reason: error.code,
          details: error.details,
        });
        return { failure: error };
      }
      const updateCheckpoint = this.db.prepare(`
        UPDATE checkpoints SET status = 'INVALIDATED_CONTEXT_REVOKED', integrity_version = 2, content_hash = ?
        WHERE checkpoint_id = ? AND status = 'COMMITTED'
      `);
      for (const entry of invalidate) {
        const invalidated = { ...entry, status: 'INVALIDATED_CONTEXT_REVOKED', integrity_version: CHECKPOINT_INTEGRITY_VERSION };
        updateCheckpoint.run(checkpointHash(invalidated), entry.checkpoint_id);
      }
      const targetStatus = effects > 0 ? 'WAITING_MANUAL' : 'RECOVERING';
      const updated = this.db.prepare(`
        UPDATE runs SET status = ?, runtime_state_json = ?, execution_epoch = ?,
          lease_id = NULL, lease_owner = NULL, lease_expires_at = NULL,
          state_version = state_version + 1, updated_at = ?
        WHERE run_id = ? AND tenant_id = ? AND state_version = ? AND execution_epoch = ?
      `).run(targetStatus, JSON.stringify(checkpointState), fencedEpoch, now(), runId, tenantId, run.state_version, run.execution_epoch);
      if (updated.changes !== 1) throw new DomainError('STATE_VERSION_CONFLICT', 'Run changed before revoked Context reconciliation could commit', 409);
      this.tools.fenceInterruptedEffects?.(runId, fencedEpoch);
      const eventType = effects > 0 ? 'REVOKED_CONTEXT_REQUIRES_MANUAL' : 'REVOKED_CONTEXT_ROLLBACK';
      this.#appendEvent(runId, tenantId, eventType, actorId, fencedEpoch, {
        contextRefId,
        checkpointId: checkpoint?.checkpoint_id ?? null,
        invalidatedCheckpointIds: invalidate.map((entry) => entry.checkpoint_id),
      });
      return {
        action: effects > 0 ? 'WAITING_MANUAL' : 'ROLLBACK',
        checkpointId: checkpoint?.checkpoint_id ?? null,
        checkpointState,
      };
    });
    if (outcome?.failure) throw outcome.failure;
    return outcome;
  }

  getRun(runId, tenantId) {
    const row = this.#row(runId, tenantId);
    return this.#mapRun(row);
  }

  listRuns(tenantId) {
    return this.db.prepare('SELECT * FROM runs WHERE tenant_id = ? ORDER BY created_at DESC').all(tenantId).map((row) => this.#mapRun(row));
  }

  listEvents(runId, tenantId, afterSeq = 0) {
    this.#row(runId, tenantId);
    return this.db.prepare('SELECT * FROM run_events WHERE run_id = ? AND event_seq > ? ORDER BY event_seq').all(runId, afterSeq).map((row) => ({
      eventId: row.event_id,
      runId: row.run_id,
      eventSeq: row.event_seq,
      eventType: row.event_type,
      actorId: row.actor_id,
      executionEpoch: row.execution_epoch,
      payload: parse(row.payload_json),
      createdAt: row.created_at,
    }));
  }

  #transition(runId, lease, allowed, target, eventType, payload = {}) {
    return this.#workerTransaction(runId, lease, allowed, (run) => {
      if (TERMINAL.has(target)) {
        const pending = this.db.prepare(`
          SELECT count(*) AS count FROM tool_effects
          WHERE run_id = ? AND tenant_id = ? AND status NOT IN ('COMMITTED', 'FAILED')
        `).get(runId, run.tenant_id).count;
        if (pending > 0) {
          throw new DomainError('PENDING_EFFECTS', 'Run cannot become terminal while Tool effects are unresolved', 409, { pendingEffects: pending });
        }
      }
      this.#fencedUpdate(run, lease, 'status = ?, result_json = ?, state_version = state_version + 1, updated_at = ?', [
        target,
        TERMINAL.has(target) ? JSON.stringify(payload) : null,
        now(),
      ]);
      this.#appendEvent(runId, run.tenant_id, eventType, this.#leaseActor(lease), lease.executionEpoch, payload);
      return this.getRun(runId, run.tenant_id);
    });
  }

  #workerTransaction(runId, lease, allowedStatuses, work) {
    const outcome = this.db.transaction(() => {
      const run = this.#verifyLease(runId, lease, allowedStatuses);
      const dispositionError = this.#applyEffectDisposition(run, this.#leaseActor(lease));
      if (dispositionError) return { dispositionError };
      return { value: work(run) };
    });
    if (outcome.dispositionError) throw outcome.dispositionError;
    return outcome.value;
  }

  #applyEffectDisposition(run, actorId) {
    const ledger = this.#effectLedgerState(run.run_id, run.tenant_id);
    if (ledger.unknown.length > 0) {
      const effectIds = ledger.unknown.map((effect) => effect.effect_id);
      this.#commitEffectGate(run, actorId, 'UNRESOLVED_TOOL_EFFECTS', {
        effectIds,
        pendingEffects: ledger.unknown.length + ledger.active.length,
      });
      return new DomainError('EFFECT_STATUS_UNKNOWN', 'Run has unknown Tool effects and requires manual reconciliation', 409, { effectIds });
    }
    if (ledger.unacknowledgedFailed.length > 0) {
      const effectIds = ledger.unacknowledgedFailed.map((effect) => effect.effect_id);
      if (ledger.active.length > 0) {
        this.#commitEffectGate(run, actorId, 'FAILED_EFFECT_WITH_PENDING', {
          effectIds,
          pendingEffects: ledger.active.length,
        });
      } else {
        this.#commitEffectFailure(run, actorId, effectIds);
      }
      return new DomainError('EFFECT_EXECUTION_FAILED', 'A Tool effect failed and the Run cannot continue', 409, { effectIds });
    }
    if (ledger.active.length > 0) {
      return new DomainError('PENDING_EFFECTS', 'Run cannot advance while Tool effects are active', 409, {
        effectIds: ledger.active.map((effect) => effect.effect_id),
      });
    }
    return null;
  }

  #commitEffectGate(run, actorId, reason, details) {
    const executionEpoch = run.execution_epoch + 1;
    const updated = this.db.prepare(`
      UPDATE runs SET status = 'WAITING_MANUAL', execution_epoch = ?, lease_id = NULL,
        lease_owner = NULL, lease_expires_at = NULL, state_version = state_version + 1, updated_at = ?
      WHERE run_id = ? AND tenant_id = ? AND state_version = ? AND execution_epoch = ?
    `).run(executionEpoch, now(), run.run_id, run.tenant_id, run.state_version, run.execution_epoch);
    if (updated.changes !== 1) throw new DomainError('STATE_VERSION_CONFLICT', 'Run changed before Tool effect gate could commit', 409);
    this.tools.fenceInterruptedEffects?.(run.run_id, executionEpoch);
    this.#appendEvent(run.run_id, run.tenant_id, 'RUN_WAITING_MANUAL', actorId, executionEpoch, { reason, ...details });
  }

  #commitEffectFailure(run, actorId, effectIds) {
    const executionEpoch = run.execution_epoch + 1;
    const failure = { code: 'EFFECT_EXECUTION_FAILED', effectId: effectIds[0] ?? null, effectIds };
    const updated = this.db.prepare(`
      UPDATE runs SET status = 'FAILED', execution_epoch = ?, lease_id = NULL, lease_owner = NULL,
        lease_expires_at = NULL, state_version = state_version + 1, result_json = ?, updated_at = ?
      WHERE run_id = ? AND tenant_id = ? AND state_version = ? AND execution_epoch = ?
    `).run(executionEpoch, JSON.stringify(failure), now(), run.run_id, run.tenant_id, run.state_version, run.execution_epoch);
    if (updated.changes !== 1) throw new DomainError('STATE_VERSION_CONFLICT', 'Run changed before Tool failure could commit', 409);
    this.#appendEvent(run.run_id, run.tenant_id, 'RUN_FAILED', actorId, executionEpoch, failure);
  }

  #effectLedgerState(runId, tenantId) {
    const effects = this.db.prepare(`
      SELECT effect_id, status FROM tool_effects WHERE run_id = ? AND tenant_id = ?
    `).all(runId, tenantId);
    const acknowledged = new Set();
    const events = this.db.prepare(`
      SELECT payload_json FROM run_events
      WHERE run_id = ? AND event_type IN ('RUN_FAILED', 'TOOL_EFFECT_RESOLVED')
    `).all(runId);
    for (const event of events) {
      let payload;
      try {
        payload = parse(event.payload_json);
      } catch {
        continue;
      }
      if (typeof payload.effectId === 'string') acknowledged.add(payload.effectId);
      if (Array.isArray(payload.effectIds)) {
        for (const effectId of payload.effectIds) if (typeof effectId === 'string') acknowledged.add(effectId);
      }
    }
    return {
      unknown: effects.filter((effect) => effect.status === 'UNKNOWN'),
      active: effects.filter((effect) => ['PREPARED', 'DISPATCHING'].includes(effect.status)),
      unacknowledgedFailed: effects.filter((effect) => effect.status === 'FAILED' && !acknowledged.has(effect.effect_id)),
    };
  }

  #verifyLease(runId, lease, allowedStatuses = null) {
    const run = this.#row(runId, lease.tenantId);
    const valid = run.lease_id === lease.leaseId
      && run.lease_owner === lease.workerId
      && run.execution_epoch === lease.executionEpoch
      && run.lease_expires_at >= this.#dbNow();
    if (!valid) throw new DomainError('FENCED_OUT', 'Worker lease or execution epoch is stale', 409);
    if (allowedStatuses && !allowedStatuses.includes(run.status)) {
      throw new DomainError('INVALID_RUN_TRANSITION', `Run status ${run.status} does not allow this operation`, 409);
    }
    return run;
  }

  #leaseActor(lease) {
    return lease.actorId ?? lease.workerId;
  }

  #fencedUpdate(run, lease, setClause, values) {
    const result = this.db.prepare(`
      UPDATE runs SET ${setClause}
      WHERE run_id = ? AND tenant_id = ? AND lease_id = ? AND lease_owner = ?
        AND execution_epoch = ? AND state_version = ? AND lease_expires_at >= ?
    `).run(
      ...values,
      run.run_id,
      run.tenant_id,
      lease.leaseId,
      lease.workerId,
      lease.executionEpoch,
      run.state_version,
      this.#dbNow(),
    );
    if (result.changes !== 1) throw new DomainError('FENCED_OUT', 'Worker lease, execution epoch, or state version is stale', 409);
  }

  #appendEvent(runId, tenantId, eventType, actorId, executionEpoch, payload) {
    const run = this.db.prepare('SELECT last_event_seq FROM runs WHERE run_id = ?').get(runId);
    const eventSeq = run.last_event_seq + 1;
    this.db.prepare(`
      INSERT INTO run_events (event_id, run_id, tenant_id, event_seq, event_type, actor_id, execution_epoch, payload_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(randomUUID(), runId, tenantId, eventSeq, eventType, actorId, executionEpoch, JSON.stringify(payload), now());
    this.db.prepare('UPDATE runs SET last_event_seq = ?, updated_at = ? WHERE run_id = ?').run(eventSeq, now(), runId);
    return eventSeq;
  }

  #row(runId, tenantId) {
    const row = this.db.prepare('SELECT * FROM runs WHERE run_id = ? AND tenant_id = ?').get(runId, tenantId);
    if (!row) throw new DomainError('RUN_NOT_FOUND', 'Run not found', 404);
    return row;
  }

  #mapRun(row) {
    return {
      runId: row.run_id,
      tenantId: row.tenant_id,
      goal: row.goal,
      status: row.status,
      executionEpoch: row.execution_epoch,
      leaseOwner: row.lease_owner,
      stateVersion: row.state_version,
      lastEventSeq: row.last_event_seq,
      runtimeState: parse(row.runtime_state_json),
      result: parse(row.result_json, null),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  #rebuildCheckpointState(runId) {
    const checkpoints = this.#loadVerifiedCheckpoints(runId);
    const head = checkpoints.filter((entry) => entry.status === 'COMMITTED').at(-1);
    return head ? this.#materializeCheckpointState(runId, head.checkpoint_id, checkpoints) : {};
  }

  #reconcileRevokedReads(runId, tenantId, actorId) {
    this.#row(runId, tenantId);
    const revoked = this.db.prepare(`
      SELECT reads.context_ref_id FROM run_context_reads AS reads
      JOIN contexts ON contexts.context_ref_id = reads.context_ref_id
      WHERE reads.run_id = ? AND contexts.tenant_id = ? AND contexts.revoked_at IS NOT NULL
      ORDER BY reads.first_read_event_seq
    `).all(runId, tenantId);
    for (const { context_ref_id: contextRefId } of revoked) {
      this.reconcileRevokedContext(runId, contextRefId, { tenantId, actorId });
    }
  }

  #loadVerifiedCheckpoints(runId) {
    const checkpoints = this.db.prepare('SELECT * FROM checkpoints WHERE run_id = ? ORDER BY checkpoint_seq').all(runId);
    const events = this.db.prepare("SELECT * FROM run_events WHERE run_id = ? AND event_type = 'CHECKPOINT_COMMITTED' ORDER BY event_seq").all(runId);
    if (events.length !== checkpoints.length) {
      throw new DomainError('CHECKPOINT_CORRUPT', 'Checkpoint event ledger does not match stored checkpoints', 409, { runId });
    }
    const eventsBySeq = new Map(events.map((event) => [event.event_seq, event]));
    const tenantId = this.db.prepare('SELECT tenant_id FROM runs WHERE run_id = ?').get(runId)?.tenant_id;
    for (const checkpoint of checkpoints) {
      this.#verifyCheckpoint(checkpoint, eventsBySeq.get(checkpoint.event_seq), tenantId);
    }
    return checkpoints;
  }

  #materializeCheckpointState(runId, headCheckpointId, checkpoints = this.#loadVerifiedCheckpoints(runId)) {
    const byId = new Map(checkpoints.map((checkpoint) => [checkpoint.checkpoint_id, checkpoint]));
    let current = byId.get(headCheckpointId);
    const reverseChain = [];
    const seen = new Set();
    while (current) {
      if (current.run_id !== runId || current.status !== 'COMMITTED' || seen.has(current.checkpoint_id)) {
        throw new DomainError('CHECKPOINT_CORRUPT', 'Checkpoint active lineage is invalid', 409, { checkpointId: current.checkpoint_id });
      }
      seen.add(current.checkpoint_id);
      reverseChain.push(current);
      if (current.kind === 'FULL') break;
      if (current.kind !== 'DELTA' || !current.parent_checkpoint_id) {
        throw new DomainError('CHECKPOINT_CORRUPT', 'Checkpoint lineage has no full anchor', 409, { checkpointId: current.checkpoint_id });
      }
      current = byId.get(current.parent_checkpoint_id);
    }
    if (reverseChain.at(-1)?.kind !== 'FULL') {
      throw new DomainError('CHECKPOINT_CORRUPT', 'Checkpoint lineage has no full anchor', 409, { checkpointId: headCheckpointId });
    }
    const chain = reverseChain.reverse();
    let state = parse(chain[0].state_json);
    for (const delta of chain.slice(1)) state = { ...state, ...parse(delta.state_json) };
    return state;
  }

  #verifyCheckpoint(checkpoint, event, tenantId) {
    if (!checkpointHashMatches(checkpoint)) {
      throw new DomainError('CHECKPOINT_CORRUPT', 'Checkpoint hash validation failed', 409, { checkpointId: checkpoint.checkpoint_id });
    }
    try {
      parse(checkpoint.state_json);
    } catch {
      throw new DomainError('CHECKPOINT_CORRUPT', 'Checkpoint state is not valid JSON', 409, { checkpointId: checkpoint.checkpoint_id });
    }
    let payload;
    try {
      payload = event ? parse(event.payload_json) : null;
    } catch {
      payload = null;
    }
    const eventMatches = event
      && event.run_id === checkpoint.run_id
      && event.tenant_id === tenantId
      && event.event_seq === checkpoint.event_seq
      && event.event_type === 'CHECKPOINT_COMMITTED'
      && event.execution_epoch === checkpoint.execution_epoch
      && payload?.checkpointId === checkpoint.checkpoint_id
      && payload?.kind === checkpoint.kind;
    if (!eventMatches) {
      throw new DomainError('CHECKPOINT_CORRUPT', 'Checkpoint event validation failed', 409, { checkpointId: checkpoint.checkpoint_id });
    }
  }

  #dbNow() {
    return Number(this.db.prepare("SELECT strftime('%s', 'now') AS now").get().now);
  }
}
