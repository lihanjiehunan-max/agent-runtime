import { DomainError, invariant } from './errors.js';

export const DEVELOPMENT_TOKEN = 'dev-token';

export const DEVELOPMENT_PERMISSIONS = Object.freeze([
  'context:write',
  'context:revoke',
  'effect:resolve',
  'run:create',
  'run:execute',
  'run:read',
  'run:recover',
  'run:resolve',
  'run:takeover',
  'tool:read',
]);

function normalizePrincipal(principal) {
  invariant(principal && typeof principal === 'object', 'INVALID_AUTH_CONFIGURATION', 'principal must be an object', 500);
  invariant(typeof principal.tenantId === 'string' && principal.tenantId.length > 0, 'INVALID_AUTH_CONFIGURATION', 'principal tenantId is required', 500);
  invariant(typeof principal.subjectId === 'string' && principal.subjectId.length > 0, 'INVALID_AUTH_CONFIGURATION', 'principal subjectId is required', 500);
  invariant(principal.workerId === undefined || (typeof principal.workerId === 'string' && principal.workerId.length > 0), 'INVALID_AUTH_CONFIGURATION', 'principal workerId is invalid', 500);
  invariant(Array.isArray(principal.permissions) && principal.permissions.every((permission) => typeof permission === 'string'), 'INVALID_AUTH_CONFIGURATION', 'principal permissions must be strings', 500);
  return Object.freeze({
    tenantId: principal.tenantId,
    subjectId: principal.subjectId,
    workerId: principal.workerId ?? null,
    permissions: Object.freeze([...new Set(principal.permissions)]),
  });
}

export function createStaticAuthenticator(tokens) {
  invariant(tokens && typeof tokens === 'object', 'INVALID_AUTH_CONFIGURATION', 'token map is required', 500);
  const principals = new Map();
  for (const [token, principal] of Object.entries(tokens)) {
    invariant(typeof token === 'string' && token.length > 0, 'INVALID_AUTH_CONFIGURATION', 'token must not be empty', 500);
    principals.set(token, normalizePrincipal(principal));
  }
  invariant(principals.size > 0, 'INVALID_AUTH_CONFIGURATION', 'at least one token is required', 500);
  return Object.freeze({
    authenticate(request) {
      const authorization = request.headers.authorization;
      if (typeof authorization !== 'string' || !authorization.startsWith('Bearer ')) {
        throw new DomainError('AUTHENTICATION_REQUIRED', 'A valid Bearer token is required', 401);
      }
      const token = authorization.slice('Bearer '.length);
      const principal = principals.get(token);
      if (!principal) throw new DomainError('INVALID_ACCESS_TOKEN', 'Bearer token is invalid', 401);
      return principal;
    },
  });
}

export function createDevelopmentAuthenticator({ environment = process.env } = {}) {
  if (environment.NODE_ENV === 'production') {
    throw new DomainError('AUTH_CONFIGURATION_REQUIRED', 'Production requires AGENT_RUNTIME_IDENTITIES', 500);
  }
  return createStaticAuthenticator({
    [DEVELOPMENT_TOKEN]: {
      tenantId: 'demo-tenant',
      subjectId: 'local-operator',
      workerId: 'host-runtime-01',
      permissions: DEVELOPMENT_PERMISSIONS,
    },
  });
}

export function createRuntimeAuthenticator(environment = process.env) {
  if (typeof environment.AGENT_RUNTIME_IDENTITIES === 'string' && environment.AGENT_RUNTIME_IDENTITIES.length > 0) {
    let identities;
    try {
      identities = JSON.parse(environment.AGENT_RUNTIME_IDENTITIES);
    } catch {
      throw new DomainError('INVALID_AUTH_CONFIGURATION', 'AGENT_RUNTIME_IDENTITIES must be valid JSON', 500);
    }
    return createStaticAuthenticator(identities);
  }
  if (environment.AGENT_RUNTIME_ALLOW_DEV_AUTH === '1') {
    return createDevelopmentAuthenticator({ environment });
  }
  throw new DomainError(
    'AUTH_CONFIGURATION_REQUIRED',
    'Configure AGENT_RUNTIME_IDENTITIES or explicitly opt in to development authentication',
    500,
  );
}

export function requirePermission(principal, permission) {
  if (!principal.permissions.includes(permission)) {
    throw new DomainError('PERMISSION_DENIED', `Permission ${permission} is required`, 403);
  }
}
