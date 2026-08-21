# Task 10 implementation report

## Scope delivered

- Added the Redis Streams asynchronous task command/status contract and durable cancellation token.
- Added `RedisTaskQueue.execute_async`, bounded consumer-group claim/auto-claim, status lookup, cancellation, terminal marking, and ACK ordering.
- Added `TaskConsumer.consume_once` and API submit/status/cancel routes.
- Added `ExecutionManager.execute_async` with cooperative cancellation, package/request timeout resolution, bounded partial output on timeout, execution-epoch fencing, and terminal status handling.
- Kept the existing SYNC/STREAM execution path and its live-event behavior unchanged.

## Queue/consumer caller contract

The API/service caller submits:

```text
RedisTaskQueue.execute_async(
    session_id,
    input_text,
    principal,
    *,
    execution_id=None,
    trace_id=None,
    execution_epoch=0,
    timeout_seconds=None,
) -> TaskHandle
```

The emitted `runtime.task.command.v1` contains the authenticated tenant, user,
actor, and worker identity, session/execution/trace IDs, execution epoch, input,
`mode=ASYNC`, and `retry_count=max_retries=0`. Caller-supplied tenant/worker
identity is rejected at the API boundary.

`TaskConsumer.consume_once()` claims via `XAUTOCLAIM` followed by bounded
`XREADGROUP`, validates command identity against its verified worker principal,
marks the task claimed, and calls:

```text
ExecutionManager.execute_async(
    session_id,
    input_text,
    principal,
    execution_id=command.execution_id,
    trace_id=command.trace_id,
    execution_epoch=command.execution_epoch,
    cancellation_token=queue.cancellation_token(command),
    timeout_seconds=command.timeout_seconds,
)
```

The consumer writes the durable terminal task status before `XACK`. There is no
automatic retry path. Runtime terminal events use the existing durable
`EventRepository.append_terminal` CAS boundary, so cancellation/timeout/late
worker races have one durable terminal winner; cancellation after a terminal is
idempotent.

## Verification evidence

All test doubles below are deterministic in-process Redis/graph doubles; no live
Redis, PostgreSQL, or model service was used.

```text
uv run pytest tests/integration/test_timeout.py \
  tests/integration/test_cancel.py \
  tests/integration/test_async_task.py -q -rs
10 passed, 1 warning

uv run pytest tests/unit/test_event_normalizer.py \
  tests/integration/test_sync_execution.py \
  tests/integration/test_stream_execution.py \
  tests/contract/test_sse_resume.py -q -rs
34 passed, 1 warning

uv run pytest tests/contract/test_execution_manager_contract.py \
  tests/integration/test_repository_cas.py \
  tests/integration/test_postgres_schema.py -q -rs
19 passed, 3 skipped
```

The three skips are the PostgreSQL integration checks because
`RUNTIME_TEST_DATABASE_URL` is not configured. Focused Task 10 tests have no
live-service skips.

```text
UV_CACHE_DIR=/tmp/uv-cache-task10-final3 uv run ruff check \
  apps/runtime_api/main.py apps/runtime_api/routes/executions.py \
  apps/runtime_api/routes/tasks.py apps/runtime_worker/consumer.py \
  packages/deepagents_adapter/factory.py packages/execution_manager \
  packages/runtime_contracts packages/runtime_persistence/repositories.py \
  tests/integration/test_timeout.py tests/integration/test_cancel.py \
  tests/integration/test_async_task.py
All checks passed!

UV_CACHE_DIR=/tmp/uv-cache-task10-final3 uv run pyright \
  apps/runtime_api apps/runtime_worker packages/execution_manager \
  packages/deepagents_adapter packages/runtime_contracts \
  packages/runtime_persistence packages/session_manager \
  tests/integration/test_timeout.py tests/integration/test_cancel.py \
  tests/integration/test_async_task.py
0 errors, 0 warnings, 0 informations

git diff --check
clean
```

Task 11 files already present in the shared worktree were left untouched and
were excluded from the Task 10 commit.
