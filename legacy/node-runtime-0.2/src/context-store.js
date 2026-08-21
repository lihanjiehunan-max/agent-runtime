import { createHash, randomUUID } from 'node:crypto';
import { DomainError, invariant } from './errors.js';

function now() {
  return new Date().toISOString();
}

function hash(value) {
  return createHash('sha256').update(value).digest('hex');
}

export class ContextStore {
  constructor(db) {
    this.db = db;
  }

  put({ tenantId, content, classification = 'internal' }) {
    invariant(typeof tenantId === 'string' && tenantId.length > 0, 'INVALID_TENANT', 'tenantId is required');
    invariant(typeof content === 'string' && content.length > 0, 'INVALID_CONTEXT', 'content is required');
    const contentHash = hash(content);
    const contextRefId = `ctx_${randomUUID()}`;
    this.db.prepare(`
      INSERT INTO contexts
        (context_ref_id, tenant_id, content_hash, content, classification, created_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT (tenant_id, content_hash, classification) DO NOTHING
    `).run(contextRefId, tenantId, contentHash, content, classification, now());
    const stored = this.db.prepare(`
      SELECT context_ref_id FROM contexts
      WHERE tenant_id = ? AND content_hash = ? AND classification = ?
    `).get(tenantId, contentHash, classification);
    return { contextRefId: stored.context_ref_id, contentHash };
  }

  read(contextRefId, { tenantId, runId = null, actorId }) {
    const row = this.db.prepare(`
      SELECT * FROM contexts WHERE context_ref_id = ? AND tenant_id = ?
    `).get(contextRefId, tenantId);
    if (!row) throw new DomainError('CONTEXT_NOT_FOUND', 'Context reference not found', 404);
    if (row.revoked_at) throw new DomainError('CONTEXT_REVOKED', 'Context reference has been revoked', 409, { reason: row.revoke_reason });

    this.#audit({ tenantId, runId, actorId, eventType: 'ASSET_READ', subjectId: contextRefId });
    return {
      contextRefId,
      content: row.content,
      contentHash: row.content_hash,
      classification: row.classification,
    };
  }

  revoke(contextRefId, { tenantId, reason, actorId = 'system' }) {
    const result = this.db.prepare(`
      UPDATE contexts SET revoked_at = ?, revoke_reason = ?
      WHERE context_ref_id = ? AND tenant_id = ? AND revoked_at IS NULL
    `).run(now(), reason, contextRefId, tenantId);
    if (result.changes === 0) {
      const existing = this.db.prepare('SELECT revoked_at FROM contexts WHERE context_ref_id = ? AND tenant_id = ?')
        .get(contextRefId, tenantId);
      if (!existing) throw new DomainError('CONTEXT_NOT_FOUND', 'Context reference not found', 404);
      return { revoked: false };
    }
    this.#audit({ tenantId, actorId, eventType: 'CONTEXT_REVOKED', subjectId: contextRefId, payload: { reason } });
    return { revoked: true };
  }

  #audit({ tenantId, runId = null, actorId, eventType, subjectId, payload = {} }) {
    this.db.prepare(`
      INSERT INTO audit_events
        (event_id, tenant_id, run_id, actor_id, event_type, subject_id, payload_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(randomUUID(), tenantId, runId, actorId, eventType, subjectId, JSON.stringify(payload), now());
  }
}
