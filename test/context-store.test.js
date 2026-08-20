import assert from 'node:assert/strict';
import test from 'node:test';
import { createDatabase } from '../src/db.js';
import { ContextStore } from '../src/context-store.js';

function fixture() {
  const db = createDatabase(':memory:');
  return { db, store: new ContextStore(db) };
}

test('identical context is content-addressed and stored once', () => {
  const { db, store } = fixture();
  const first = store.put({ tenantId: 't1', content: 'large context', classification: 'internal' });
  const second = store.put({ tenantId: 't1', content: 'large context', classification: 'internal' });

  assert.equal(first.contextRefId, second.contextRefId);
  assert.equal(db.prepare('SELECT count(*) AS count FROM contexts').get().count, 1);
});

test('identical content has opaque references scoped by tenant and classification', () => {
  const { db, store } = fixture();
  const tenantAInternal = store.put({ tenantId: 'tenant-a', content: 'shared bytes', classification: 'internal' });
  const tenantBInternal = store.put({ tenantId: 'tenant-b', content: 'shared bytes', classification: 'internal' });
  const tenantARestricted = store.put({ tenantId: 'tenant-a', content: 'shared bytes', classification: 'restricted' });

  assert.notEqual(tenantAInternal.contextRefId, tenantBInternal.contextRefId);
  assert.notEqual(tenantAInternal.contextRefId, tenantARestricted.contextRefId);
  assert.match(tenantAInternal.contextRefId, /^ctx_[0-9a-f-]{36}$/);
  assert.equal(db.prepare('SELECT count(*) AS count FROM contexts').get().count, 3);
  assert.throws(
    () => store.read(tenantAInternal.contextRefId, { tenantId: 'tenant-b', actorId: 'worker-b' }),
    (error) => error.code === 'CONTEXT_NOT_FOUND',
  );
});

test('reading context records an ASSET_READ audit before returning content', () => {
  const { db, store } = fixture();
  const saved = store.put({ tenantId: 't1', content: 'employee data' });
  const value = store.read(saved.contextRefId, { tenantId: 't1', runId: 'run-1', actorId: 'worker-1' });

  assert.equal(value.content, 'employee data');
  const event = db.prepare('SELECT event_type, run_id, actor_id FROM audit_events').get();
  assert.equal(event.event_type, 'ASSET_READ');
  assert.equal(event.run_id, 'run-1');
  assert.equal(event.actor_id, 'worker-1');
});

test('revoked context rejects future reads without erasing history', () => {
  const { db, store } = fixture();
  const saved = store.put({ tenantId: 't1', content: 'withdrawn policy' });
  store.revoke(saved.contextRefId, { tenantId: 't1', reason: 'policy invalidated' });

  assert.throws(
    () => store.read(saved.contextRefId, { tenantId: 't1', runId: 'run-2', actorId: 'worker-1' }),
    (error) => error.code === 'CONTEXT_REVOKED',
  );
  assert.equal(db.prepare('SELECT revoked_at IS NOT NULL AS revoked FROM contexts').get().revoked, 1);
  assert.equal(db.prepare('SELECT count(*) AS count FROM audit_events').get().count, 1);
});

test('tenant boundary prevents cross-tenant context reads', () => {
  const { store } = fixture();
  const saved = store.put({ tenantId: 't1', content: 'private' });
  assert.throws(
    () => store.read(saved.contextRefId, { tenantId: 't2', runId: 'run-3', actorId: 'worker-2' }),
    (error) => error.code === 'CONTEXT_NOT_FOUND',
  );
});
