import { createStaticAuthenticator } from '../src/auth.js';

export const TEST_PERMISSIONS = Object.freeze([
  'context:write', 'context:revoke', 'effect:resolve', 'run:create', 'run:execute',
  'run:read', 'run:recover', 'run:resolve', 'run:takeover', 'tool:read',
]);

export const TEST_IDENTITIES = Object.freeze({
  't1-worker-a-token': { tenantId: 't1', subjectId: 'worker-a-subject', workerId: 'worker-a', permissions: TEST_PERMISSIONS },
  't1-worker-b-token': { tenantId: 't1', subjectId: 'worker-b-subject', workerId: 'worker-b', permissions: TEST_PERMISSIONS },
  't1-host-1-token': { tenantId: 't1', subjectId: 'host-1-subject', workerId: 'host-1', permissions: TEST_PERMISSIONS },
  't1-host-2-token': { tenantId: 't1', subjectId: 'host-2-subject', workerId: 'host-2', permissions: TEST_PERMISSIONS },
  'tenant-a-token': { tenantId: 'tenant-a', subjectId: 'tenant-a-subject', workerId: 'worker-a', permissions: TEST_PERMISSIONS },
  'tenant-b-token': { tenantId: 'tenant-b', subjectId: 'tenant-b-subject', workerId: 'worker-b', permissions: TEST_PERMISSIONS },
});

export function createTestAuthenticator() {
  return createStaticAuthenticator(TEST_IDENTITIES);
}

export function authorization(token = 't1-worker-a-token') {
  return { authorization: `Bearer ${token}` };
}
