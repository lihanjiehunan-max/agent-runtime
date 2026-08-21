import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createDatabase } from './db.js';
import { requirePermission } from './auth.js';
import { ContextStore } from './context-store.js';
import { DomainError } from './errors.js';
import { ToolRegistry, registerBuiltinTools } from './tool-contracts.js';
import { Runtime } from './runtime.js';

const JSON_TYPE = 'application/json; charset=utf-8';

function sendJson(response, status, value, requestId) {
  const data = JSON.stringify(requestId ? { ...value, requestId } : value);
  response.writeHead(status, {
    'content-type': JSON_TYPE,
    'content-length': Buffer.byteLength(data),
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  });
  response.end(data);
}

async function readJson(request, maxBytes = 1024 * 1024) {
  const contentType = request.headers['content-type'];
  if (typeof contentType !== 'string' || !contentType.toLowerCase().startsWith('application/json')) {
    throw new DomainError('UNSUPPORTED_MEDIA_TYPE', 'Content-Type must be application/json', 415);
  }
  let size = 0;
  const chunks = [];
  for await (const chunk of request) {
    size += chunk.length;
    if (size > maxBytes) throw new DomainError('BODY_TOO_LARGE', 'Request body exceeds the allowed size', 413);
    chunks.push(chunk);
  }
  if (chunks.length === 0) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new DomainError('MALFORMED_JSON', 'Request body must be valid JSON', 400);
  }
}

function match(pathname, pattern) {
  const found = pathname.match(pattern);
  return found?.groups ?? null;
}

const defaultPublicDir = resolve(dirname(fileURLToPath(import.meta.url)), '..', 'public');

export function createApp({ dbPath = ':memory:', publicDir = defaultPublicDir, authenticator } = {}) {
  if (!authenticator || typeof authenticator.authenticate !== 'function') {
    throw new DomainError('AUTH_CONFIGURATION_REQUIRED', 'An authenticator must be configured', 500);
  }
  const db = createDatabase(dbPath);
  const contexts = new ContextStore(db);
  const tools = new ToolRegistry(db);
  registerBuiltinTools(tools);
  const runtime = new Runtime(db, { contexts, tools });

  const server = createServer(async (request, response) => {
    const requestId = randomUUID();
    try {
      const url = new URL(request.url, 'http://localhost');
      const { pathname } = url;

      if (request.method === 'GET' && pathname === '/api/health') {
        return sendJson(response, 200, { status: 'ok', service: 'enterprise-agent-runtime', version: '0.2.0' });
      }

      if (request.method === 'GET' && !pathname.startsWith('/api/')) {
        return serveStatic(response, pathname, publicDir);
      }

      const principal = authenticator.authenticate(request);
      const claimedTenant = request.headers['x-tenant-id'];
      if (claimedTenant !== undefined && claimedTenant !== principal.tenantId) {
        throw new DomainError('TENANT_IDENTITY_MISMATCH', 'x-tenant-id does not match the authenticated principal', 403);
      }
      const tenantId = principal.tenantId;
      if (request.method === 'GET' && pathname === '/api/tools') {
        requirePermission(principal, 'tool:read');
        return sendJson(response, 200, { items: tools.list() });
      }
      if (request.method === 'POST' && pathname === '/api/contexts') {
        requirePermission(principal, 'context:write');
        const body = await readJson(request);
        rejectCallerIdentity(body);
        return sendJson(response, 201, contexts.put({ tenantId, content: body.content, classification: body.classification }));
      }
      let params = match(pathname, /^\/api\/contexts\/(?<contextRefId>[^/]+)\/revoke$/);
      if (request.method === 'POST' && params) {
        requirePermission(principal, 'context:revoke');
        const body = await readJson(request);
        rejectCallerIdentity(body);
        contexts.revoke(params.contextRefId, { tenantId, reason: body.reason ?? 'revoked by operator', actorId: principal.subjectId });
        const reconciliations = runtime.reconcileRevokedContextReads(params.contextRefId, {
          tenantId,
          actorId: principal.subjectId,
        });
        return sendJson(response, 200, { revoked: true, reconciliations });
      }
      if (request.method === 'POST' && pathname === '/api/runs') {
        requirePermission(principal, 'run:create');
        const body = await readJson(request);
        rejectCallerIdentity(body);
        return sendJson(response, 201, runtime.createRun({ tenantId, goal: body.goal, actorId: principal.subjectId }));
      }
      if (request.method === 'GET' && pathname === '/api/runs') {
        requirePermission(principal, 'run:read');
        return sendJson(response, 200, { items: runtime.listRuns(tenantId) });
      }
      if (request.method === 'GET' && pathname === '/api/events') {
        requirePermission(principal, 'run:read');
        return streamEvents(response, runtime, {
          runId: url.searchParams.get('run_id'),
          tenantId,
          afterSeq: nonNegativeInteger(url.searchParams.get('after_seq') ?? '0', 'INVALID_EVENT_CURSOR', 'after_seq'),
          once: url.searchParams.get('once') === '1',
        });
      }

      params = match(pathname, /^\/api\/runs\/(?<runId>[^/]+)$/);
      if (request.method === 'GET' && params) {
        requirePermission(principal, 'run:read');
        return sendJson(response, 200, runtime.getRun(params.runId, tenantId));
      }
      params = match(pathname, /^\/api\/runs\/(?<runId>[^/]+)\/events$/);
      if (request.method === 'GET' && params) {
        requirePermission(principal, 'run:read');
        return sendJson(response, 200, { items: runtime.listEvents(params.runId, tenantId, nonNegativeInteger(url.searchParams.get('after_seq') ?? '0', 'INVALID_EVENT_CURSOR', 'after_seq')) });
      }

      params = match(pathname, /^\/api\/runs\/(?<runId>[^/]+)\/(?<action>lease|start|checkpoint|complete|fail|recover)$/);
      if (request.method === 'POST' && params) {
        const body = await readJson(request);
        rejectCallerIdentity(body, ['tenantId', 'actorId']);
        if (params.action === 'lease') {
          requirePermission(principal, 'run:execute');
          const workerId = principalWorker(principal, body.workerId);
          if (body.force !== undefined && typeof body.force !== 'boolean') throw new DomainError('INVALID_FORCE_FLAG', 'force must be a boolean');
          const force = body.force ?? false;
          if (force) {
            requirePermission(principal, 'run:takeover');
            requireReason(body.reason, 'TAKEOVER_REASON_REQUIRED');
          }
          return sendJson(response, 200, runtime.acquireLease(params.runId, {
            tenantId,
            workerId,
            ttlSeconds: body.ttlSeconds,
            force,
            actorId: principal.subjectId,
            takeoverReason: force ? body.reason.trim() : null,
          }));
        }
        if (params.action === 'recover') {
          requirePermission(principal, 'run:execute');
          requirePermission(principal, 'run:recover');
          requireReason(body.reason, 'RECOVERY_REASON_REQUIRED');
          return sendJson(response, 200, runtime.recover(params.runId, {
            tenantId,
            workerId: principalWorker(principal, body.workerId),
            actorId: principal.subjectId,
            reason: body.reason.trim(),
          }));
        }
        requirePermission(principal, 'run:execute');
        if (params.action === 'start') return sendJson(response, 200, runtime.start(params.runId, bindLease(body.lease, principal)));
        if (params.action === 'checkpoint') return sendJson(response, 201, runtime.checkpoint(params.runId, bindLease(body.lease, principal), { kind: body.kind, state: body.state }));
        if (params.action === 'complete') return sendJson(response, 200, runtime.complete(params.runId, bindLease(body.lease, principal), body.result ?? {}));
        if (params.action === 'fail') return sendJson(response, 200, runtime.fail(params.runId, bindLease(body.lease, principal), body.error ?? {}));
      }

      params = match(pathname, /^\/api\/runs\/(?<runId>[^/]+)\/manual\/resolve$/);
      if (request.method === 'POST' && params) {
        requirePermission(principal, 'run:resolve');
        const body = await readJson(request);
        rejectCallerIdentity(body);
        return sendJson(response, 200, runtime.resolveManual(params.runId, {
          tenantId,
          actorId: principal.subjectId,
          decision: body.decision,
          reason: body.reason,
        }));
      }

      params = match(pathname, /^\/api\/runs\/(?<runId>[^/]+)\/effects\/(?<effectId>[^/]+)\/resolve$/);
      if (request.method === 'POST' && params) {
        requirePermission(principal, 'effect:resolve');
        const body = await readJson(request);
        rejectCallerIdentity(body);
        return sendJson(response, 200, runtime.resolveToolEffect(params.runId, {
          tenantId,
          actorId: principal.subjectId,
          effectId: params.effectId,
          outcome: body.outcome,
          reason: body.reason,
          output: body.output,
        }));
      }

      params = match(pathname, /^\/api\/runs\/(?<runId>[^/]+)\/contexts\/(?<contextRefId>[^/]+)\/read$/);
      if (request.method === 'POST' && params) {
        requirePermission(principal, 'run:execute');
        const body = await readJson(request);
        rejectCallerIdentity(body);
        return sendJson(response, 200, runtime.readContext(params.runId, bindLease(body.lease, principal), params.contextRefId));
      }
      params = match(pathname, /^\/api\/runs\/(?<runId>[^/]+)\/tools\/invoke$/);
      if (request.method === 'POST' && params) {
        requirePermission(principal, 'run:execute');
        const body = await readJson(request);
        rejectCallerIdentity(body);
        return sendJson(response, 200, await runtime.invokeTool(params.runId, bindLease(body.lease, principal), {
          operationId: body.operationId,
          version: body.version,
          input: body.input,
          idempotencyKey: body.idempotencyKey,
        }));
      }

      throw new DomainError('NOT_FOUND', 'Route not found', 404);
    } catch (error) {
      const domain = error instanceof DomainError
        ? error
        : new DomainError('INTERNAL_ERROR', 'Unexpected server error', 500);
      if (!response.headersSent) {
        sendJson(response, domain.status, { error: { code: domain.code, message: domain.message, details: domain.details } }, requestId);
      } else {
        response.end();
      }
    }
  });

  return { server, db, contexts, tools, runtime };
}

function bindLease(lease, principal) {
  if (!lease || typeof lease !== 'object') throw new DomainError('LEASE_REQUIRED', 'lease is required');
  if (!principal.workerId) throw new DomainError('WORKER_PRINCIPAL_REQUIRED', 'Authenticated principal is not a Worker', 403);
  if (lease.workerId !== principal.workerId) throw new DomainError('WORKER_IDENTITY_MISMATCH', 'Lease Worker does not match the authenticated principal', 403);
  return {
    ...lease,
    tenantId: principal.tenantId,
    workerId: principal.workerId,
    actorId: principal.subjectId,
  };
}

function principalWorker(principal, claimedWorkerId) {
  if (!principal.workerId) throw new DomainError('WORKER_PRINCIPAL_REQUIRED', 'Authenticated principal is not a Worker', 403);
  if (claimedWorkerId !== undefined && claimedWorkerId !== principal.workerId) {
    throw new DomainError('WORKER_IDENTITY_MISMATCH', 'workerId does not match the authenticated principal', 403);
  }
  return principal.workerId;
}

function rejectCallerIdentity(body, fields = ['tenantId', 'actorId']) {
  for (const field of fields) {
    if (Object.hasOwn(body, field)) throw new DomainError('CALLER_IDENTITY_FORBIDDEN', `${field} is derived from the authenticated principal`, 400);
  }
}

function requireReason(reason, code) {
  if (typeof reason !== 'string' || reason.trim().length === 0) throw new DomainError(code, 'A non-empty reason is required');
}

function nonNegativeInteger(value, code, name) {
  if (!/^\d+$/.test(String(value))) throw new DomainError(code, `${name} must be a non-negative integer`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new DomainError(code, `${name} must be a safe non-negative integer`);
  return parsed;
}

function serveStatic(response, pathname, publicDir) {
  const files = {
    '/': ['index.html', 'text/html; charset=utf-8'],
    '/index.html': ['index.html', 'text/html; charset=utf-8'],
    '/app.js': ['app.js', 'text/javascript; charset=utf-8'],
    '/styles.css': ['styles.css', 'text/css; charset=utf-8'],
  };
  const entry = files[pathname];
  if (!entry) throw new DomainError('NOT_FOUND', 'Asset not found', 404);
  const content = readFileSync(resolve(publicDir, entry[0]));
  response.writeHead(200, {
    'content-type': entry[1],
    'content-length': content.length,
    'cache-control': entry[0] === 'index.html' ? 'no-cache' : 'public, max-age=300',
    'x-content-type-options': 'nosniff',
    'content-security-policy': "default-src 'self'; connect-src 'self'; script-src 'self'; style-src 'self'",
  });
  response.end(content);
}

function streamEvents(response, runtime, { runId, tenantId, afterSeq, once }) {
  if (!runId) throw new DomainError('RUN_ID_REQUIRED', 'run_id query parameter is required');
  response.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    'x-content-type-options': 'nosniff',
  });
  let cursor = afterSeq;
  const flush = () => {
    const events = runtime.listEvents(runId, tenantId, cursor);
    for (const event of events) {
      response.write(`id: ${event.eventSeq}\nevent: ${event.eventType}\ndata: ${JSON.stringify(event)}\n\n`);
      cursor = event.eventSeq;
    }
  };
  flush();
  if (once) return response.end();
  const timer = setInterval(flush, 500);
  response.on('close', () => clearInterval(timer));
}
