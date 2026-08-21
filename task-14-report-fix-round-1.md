# Task 14 fix round 1 report

## Findings closed

This round closes the independent review findings against the original Task 14
deployment commit:

- the API now has an explicit production composition root guarded by
  `RUNTIME_ENABLE_PRODUCTION_COMPOSITION=1`;
- PostgreSQL repositories, Redis lock/queue, MinIO payload storage,
  `AsyncPostgresSaver`, model gateway, read-only `query_metric`, event
  projection, SSE replay/live feed, Session Manager, Execution Manager, and
  task service are wired as one runtime bundle;
- missing production authentication remains fail-closed, with an explicit
  deployment-owned `RUNTIME_PRINCIPAL_VERIFIER_FACTORY` seam;
- readiness reports PostgreSQL, Redis, MinIO, model gateway, Tool Gateway,
  SDK version, and runtime binding state, returning 503 until the bundle is
  actually bound and Redis, MinIO bucket, model gateway, and Tool Gateway
  probes succeed;
- Compose passes the worker identity, MinIO credentials/configuration, and
  canonical `MODEL_GATEWAY_*` names consumed by the model client;
- Tool Gateway audit events are retained in the request-scoped Runtime context
  and flushed into the durable normalized event/trace path rather than being
  discarded;
- acceptance now fences stale old-worker event append and terminal submission,
  in addition to stale checkpoint writes;
- default Compose dependency images use explicit versions rather than broad or
  `latest` tags.

## Verification

```text
UV_CACHE_DIR=/tmp/task14-uv-cache uv run pytest -q -rs
291 passed, 12 skipped, 1 warning

UV_CACHE_DIR=/tmp/task14-uv-cache uv run pytest \
  tests/contract/test_runtime_composition.py \
  tests/acceptance/test_three_turn_metric_session.py -q -rs
8 passed, 1 skipped, 1 warning

UV_CACHE_DIR=/tmp/task14-uv-cache uv run ruff check \
  apps/runtime_api/main.py apps/runtime_api/routes/status.py \
  apps/runtime_api/composition.py packages/execution_manager/service.py \
  packages/tool_gateway/runtime_context.py \
  tests/contract/test_runtime_composition.py \
  tests/acceptance/test_three_turn_metric_session.py
All checks passed!

UV_CACHE_DIR=/tmp/task14-uv-cache uv run pyright \
  apps/runtime_api/main.py apps/runtime_api/routes/status.py \
  apps/runtime_api/composition.py packages/execution_manager/service.py \
  packages/tool_gateway/runtime_context.py \
  tests/contract/test_runtime_composition.py \
  tests/acceptance/test_three_turn_metric_session.py
0 errors, 0 warnings, 0 informations

git diff --check
passed
```

The twelve skips are the existing explicit live PostgreSQL, Redis, MinIO,
dependency-chaos, trace-retention, and Task 14 live acceptance gates. The one
warning is the upstream Starlette/httpx deprecation warning.

The Runtime Console regression/build gate also passed: 20 Vitest tests,
TypeScript check, and Vite production build.

## Remaining deployment gates

This workspace has no live PostgreSQL, Redis, MinIO, model gateway, Tool
Gateway, Docker/Podman runtime, or deployment-owned principal verifier factory.
Therefore this round proves composition/configuration contracts and local
fail-closed behavior, not provider availability or production capacity.

The readiness probe uses provider `/health` endpoints, Redis `PING`, MinIO
`bucket_exists`, PostgreSQL checkpoint setup, and the installed SDK
compatibility check. Probe failures are represented as `unavailable` and keep
`/health/ready` at HTTP 503 without returning provider errors or credentials.

Before traffic cutover, the operator must provide the documented environment,
install the verifier factory, run migrations, provision the payload bucket,
start the dependencies, and run the opt-in live acceptance test. The worker service still depends on the
packaged long-running `runtime-worker` launcher; this source workspace does
not claim that launcher has been built.
