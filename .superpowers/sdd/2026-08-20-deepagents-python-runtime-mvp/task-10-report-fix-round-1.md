# Task 10 fix round 1 report

## Scope

Fixed all four findings from the independent Task 10 review of `6aa85a1`.
Only Task 10 queue, consumer, service, API, tests, and this report were
changed. Task 11 and Task 12 work in the shared worktree was preserved and was
not staged.

## Fixes

1. **Principal and Session authorization**
   - Task status hashes now retain `tenant_id`, `user_id`, `actor_id`, and
     `worker_id` from the verified `Principal`.
   - Status and cancellation require the URL/session identity and compare all
     stored identity fields against the authenticated principal.
   - Mismatched tenant, user, actor, worker, or session returns the stable
     unavailable/not-found result without revealing task ownership.
   - API status and cancel routes pass the authenticated URL `session_id` to
     the service boundary.

2. **Timeout and outer-cancellation cleanup**
   - `_run_with_cancellation` now cancels and awaits the graph operation task
     when the timeout context or an outer task cancellation interrupts the
     waiter.
   - Cooperative token cancellation also awaits the cancelled operation before
     returning the cancellation result.

3. **ACK-crash redelivery idempotence**
   - The consumer reads durable task status after claim and before marking the
     task `RUNNING`.
   - A task already in a terminal state is ACKed and represented as a terminal
     result without invoking the graph again.

4. **Cancellation/terminal race**
   - Cancellation re-reads durable status after setting the cancellation token
     and again after recording the status flag.
   - If terminal completion wins either observation, the response returns that
     terminal status with `already_terminal=true`; it never reports accepted
     cancellation after a terminal state has been observed.

## Exact changed files

- `packages/execution_manager/queue.py`
- `packages/execution_manager/service.py`
- `apps/runtime_worker/consumer.py`
- `apps/runtime_api/routes/tasks.py`
- `tests/integration/test_async_task.py`
- `tests/integration/test_cancel.py`
- `tests/integration/test_timeout.py`
- `.superpowers/sdd/2026-08-20-deepagents-python-runtime-mvp/task-10-report-fix-round-1.md`

## Verification

```text
uv run pytest tests/integration/test_async_task.py tests/integration/test_cancel.py tests/integration/test_timeout.py -q -rs
14 passed, 1 warning

uv run pytest tests/unit/test_event_normalizer.py tests/integration/test_sync_execution.py tests/integration/test_stream_execution.py tests/contract/test_sse_resume.py -q -rs
34 passed, 1 warning

uv run pytest tests/contract/test_execution_manager_contract.py tests/integration/test_repository_cas.py tests/integration/test_postgres_schema.py -q -rs
19 passed, 3 skipped

uv run ruff check [Task 10 files and relevant runtime packages]
All checks passed

uv run pyright [Task 10/runtime packages and focused tests]
0 errors, 0 warnings, 0 informations

git diff --check
clean
```

The three skips are PostgreSQL integration checks because
`RUNTIME_TEST_DATABASE_URL` is not configured. No live Redis, PostgreSQL, or
model service was called. Deterministic in-process doubles cover the new
authorization, redelivery, cancellation-race, and graph-task cleanup cases.

The cancellation contract remains cooperative: a non-cooperative provider may
delay cancellation beyond the target response time. The runtime now awaits the
known asyncio graph task on timeout/outer cancellation, but it cannot hard-kill
provider work that ignores cancellation.
