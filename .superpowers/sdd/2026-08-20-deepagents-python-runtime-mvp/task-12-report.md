# Task 12 report — Runtime Console

## Delivered

Created a fresh React + TypeScript + Vite console in `apps/runtime_console`.
The console does not import or modify the legacy `public/app.js` application.

The five views are:

- Agents: pinned package id, version, digest, runtime and load state;
- Sessions: in-memory Session selection, status, thread, package pin and the
  explicit `Runtime-managed` TTL limitation;
- Chat: multi-turn messages on one Session with stream, sync and async modes;
- Trace: ordered normalized SSE events with package/execution/trace metadata;
- Dashboard: bounded success rate, P95 duration, token totals, cache-hit rate
  and active Session count.

## API contract consumed

The typed `RuntimeClient` uses the existing versioned Python API:

- `GET /api/v1/runtime/status` for LIVE connection probing;
- `POST /api/v1/runtime/sessions` and `GET /api/v1/runtime/sessions/{id}`;
- `POST /api/v1/runtime/sessions/{id}/executions` for sync turns;
- `POST /api/v1/runtime/sessions/{id}/executions/stream` for streaming turns;
- `GET /api/v1/runtime/sessions/{id}/events?after_sequence=N` with
  `Last-Event-ID: N` for resumable SSE;
- `POST/GET /api/v1/runtime/sessions/{id}/tasks...` for async submission,
  polling and cancellation.

Authentication is held only by the in-memory `RuntimeClient` and React state.
The token is sent in a Bearer header, never added to URLs, local storage, logs,
or rendered error messages. The default API base is same-origin
`/api/v1/runtime`; Vite proxies that path to `VITE_RUNTIME_PROXY_TARGET` or
`http://127.0.0.1:8000` during local development.

## Provenance and demo limits

No token starts the console in `DEMO`. A configured but unreachable Runtime is
shown as `DEMO` with a connection-failure reason and a visible `Use DEMO`
action; it is never presented as a live model answer. Demo replies and events
are deterministic fixtures, including a synthetic `query_metric` event, and
do not call a model or external service.

Because the current API does not expose package/session/trace catalog endpoints,
the first console keeps the visible Sessions and event timeline in memory. It
does not invent a backend TTL or fabricate a persistent catalog. Prometheus and
the Runtime status endpoint remain available to an integrated deployment.

## Verification

- `NPM_CONFIG_CACHE=/tmp/runtime-console-npm-cache npm test` — 10/10 passed;
- `NPM_CONFIG_CACHE=/tmp/runtime-console-npm-cache npm run build` — passed;
- `UV_CACHE_DIR=/tmp/runtime-console-uv-cache uv run pytest tests/acceptance/test_console_runtime.py -q` — 5/5 passed;
- no real model, database, Redis, MinIO, or external service was used.
