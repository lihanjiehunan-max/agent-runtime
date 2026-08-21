# Task 9 report

## Delivered

- Added the Deep Agents v3 event normalizer with bounded payloads, secret redaction,
  hidden-reasoning omission, tool output/completion distinction, and state/output
  summaries.
- Added the controlled execution state machine and execution manager for sync and
  streaming turns. A turn allocates one execution ID and one trace ID, uses the
  fenced checkpoint configuration, renews/releases its execution lease, and emits
  exactly one terminal event.
- Added the durable `RepositoryEventSink` boundary, transactional
  `EventRepository.append_next` sequence allocation, and atomic
  `EventRepository.append_terminal` event/status/session release. The manager has
  no process-local persistence fallback.
- Added execution POST/stream routes and the SSE events route with tenant-scoped
  access checks, durable replay, `Last-Event-ID` resume, bounded heartbeat waits,
  explicit live-feed exhaustion handling, and explicit 503 behavior when runtime
  wiring is absent.
- Included the routers and formal `execution_manager`/`event_stream` injection in
  `create_api` without changing existing callers.

## Fix round 1

- Awaitable results from `graph.astream_events` are resolved before iteration while
  direct async-iterator test doubles remain supported.
- Terminal event append, persisted `RuntimeExecution` terminal status, completion
  timestamp/error, and Session lease release now use one `EventRepository` transaction;
  persisted terminal status rejects duplicate terminal writes with `EXECUTION_FENCED`.
- Lease-renewal exceptions normalize to `EXECUTION_FENCED`, and lease release is
  attempted without masking the execution result, including cancellation-shaped
  release failures.
- Tool/data output deltas, top-level usage, recursive sensitive-key redaction, and
  hidden-reasoning-free values summaries are covered by normalizer tests.
- `ExecutionMode.ASYNC` is rejected as reserved for Task 10.

## Verification

Focused Task 9 command:

```text
UV_CACHE_DIR=/tmp/uv-cache-task9-fix uv run pytest tests/unit/test_event_normalizer.py tests/integration/test_sync_execution.py tests/integration/test_stream_execution.py tests/contract/test_sse_resume.py -q -rs
25 passed, 1 warning
```

The warning is the existing Starlette/httpx TestClient deprecation warning.

Additional execution-contract and repository command:

```text
UV_CACHE_DIR=/tmp/uv-cache-task9-fix uv run pytest tests/contract/test_execution_manager_contract.py tests/integration/test_repository_cas.py -q -rs
14 passed, 2 skipped
```

The two PostgreSQL tests skip because `RUNTIME_TEST_DATABASE_URL` is not set. The
repository contract doubles verify the manager boundary; they are not production
atomicity proof. Production atomicity is implemented in `EventRepository.append_terminal`.

```text
UV_CACHE_DIR=/tmp/uv-cache-task9-fix uv run ruff check <Task 9 and repository scope>  # passed
UV_CACHE_DIR=/tmp/uv-cache-task9-fix uv run pyright apps packages tests             # 0 errors
git diff --check                                                                  # passed
```

No live PostgreSQL, Redis, model, or tool service was started or used. SSE and
execution tests use deterministic graph/coordinator/live-feed doubles; the live
PostgreSQL atomic-terminal test is present and explicitly skipped without the
environment variable. Task 10+ behavior and a second chat-history implementation
were not added.

## Fix round 2

- Added one per-execution `asyncio.Lock` shared by lease renewal and terminal
  persistence. The renewal loop records lease/Redis failures as
  `EXECUTION_FENCED`; terminal persistence takes the same guard, performs a final
  guarded renewal, rechecks recorded failures, and only then calls
  `append_terminal`. Lease release remains in `finally` and is not allowed to
  mask the execution result.
- Normalized the pinned LangGraph tool-output-delta shape from `data["delta"]`,
  while retaining the existing `output` and `data` forms. Quoted JSON values for
  `api_key`, `password`, `authorization`, `access_token`, `secret`, and `token`
  are redacted without exposing their contents; `token_count` and identifier
  fields remain safe. `messages` events with `event="error"` now produce only the
  stable `runtime.error` envelope and code.
- The execution stream endpoint now explicitly rejects `sync` and `async` modes
  with HTTP 400 and passes the requested `stream` mode through. ASYNC remains
  reserved for Task 10 and is not run inline.

## Fix round 2 verification

Focused Task 9 command:

```text
UV_CACHE_DIR=/tmp/uv-cache-task9-fix2 uv run pytest tests/unit/test_event_normalizer.py tests/integration/test_sync_execution.py tests/integration/test_stream_execution.py tests/contract/test_sse_resume.py -q -rs
31 passed, 1 warning
```

The warning is the existing Starlette/httpx TestClient deprecation warning. The
new normalizer, lease-race, stable-error, and stream-mode tests were observed
failing before their production fixes and pass in this run.

Repository/CAS/schema command:

```text
UV_CACHE_DIR=/tmp/uv-cache-task9-fix2 uv run pytest tests/contract/test_execution_manager_contract.py tests/integration/test_repository_cas.py tests/integration/test_postgres_schema.py -q -rs
19 passed, 3 skipped
```

Two repository CAS cases and one schema case skip because
`RUNTIME_TEST_DATABASE_URL` is not set. These deterministic repository doubles
and schema tests are not live PostgreSQL atomicity proof.

```text
UV_CACHE_DIR=/tmp/uv-cache-task9-fix2 uv run ruff check apps/runtime_api/main.py apps/runtime_api/routes/events.py apps/runtime_api/routes/executions.py packages/event_normalizer packages/execution_manager packages/runtime_persistence/repositories.py tests/unit/test_event_normalizer.py tests/integration/test_sync_execution.py tests/integration/test_stream_execution.py tests/contract/test_sse_resume.py
All checks passed!

UV_CACHE_DIR=/tmp/uv-cache-task9-fix2 uv run pyright apps packages tests/unit/test_event_normalizer.py tests/integration/test_sync_execution.py tests/integration/test_stream_execution.py tests/contract/test_sse_resume.py
0 errors, 0 warnings, 0 informations

git diff --check
git diff --check e95ec8b 073d53d3eb20d000bfc6719e0c32a4bdd57f6ab4
```

No live PostgreSQL, Redis, model, or tool service was started or used. The
execution, lease, and SSE checks use deterministic doubles; no live Redis or
provider/model integration is claimed. Task 10 queue/cancellation/timeout
behavior and a second mutable chat-history store remain out of scope.

## Fix round 3

- Extended the quoted JSON redaction pattern to cover `client_secret`,
  `refresh_token`, and `credential`, matching the recursive sensitive-key policy.
- Added parameterized normalizer regressions for all three quoted keys. Each case
  verifies that the secret is replaced with `[REDACTED]` while `token_count` and
  `token_id` remain visible.
- The execution-fencing boundary is irreducible and intentional: Redis is
  advisory coordination; the PostgreSQL execution epoch and terminal transaction
  are authoritative. The final guarded Redis renewal occurs immediately before
  the terminal append, under the same per-execution guard. A Redis TTL expiring
  after that point without a replacement epoch is not evidence of a stale-worker
  write; only authoritative PostgreSQL fencing/epoch state establishes that
  condition.

## Fix round 3 verification

TDD regression cycle:

```text
UV_CACHE_DIR=/tmp/uv-cache-task9-final-fix uv run pytest tests/unit/test_event_normalizer.py::test_redacts_additional_quoted_sensitive_keys_without_hiding_safe_token_fields -q
3 failed in 0.07s  # before the production regex change
3 passed in 0.03s  # after the minimal regex change
```

Focused normalizer and execution tests:

```text
UV_CACHE_DIR=/tmp/uv-cache-task9-final-fix uv run pytest tests/unit/test_event_normalizer.py tests/integration/test_sync_execution.py tests/integration/test_stream_execution.py tests/contract/test_sse_resume.py -q -rs
34 passed, 1 warning in 0.71s
```

The warning is the existing Starlette/httpx TestClient deprecation warning.

```text
UV_CACHE_DIR=/tmp/uv-cache-task9-final-fix uv run ruff check apps packages tests
All checks passed!

UV_CACHE_DIR=/tmp/uv-cache-task9-final-fix uv run pyright apps packages tests
0 errors, 0 warnings, 0 informations

git diff --check
```

No live PostgreSQL or Redis service was used for this fix round.
