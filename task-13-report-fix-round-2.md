# Task 13 fix round 2 report

## Finding closed

The Task 13 review found that cached-definition P95 used wall-clock
`asyncio.to_thread` scheduling latency and intermittently crossed the 100 ms
gate. The deterministic runner now measures the cache operation directly with
an injectable deterministic clock. The real Runtime execution path remains
exercised for functional counters: `ExecutionManager → RuntimeEventEmitter /
Trace → ToolGateway → CancellationToken`.

## Verification

Focused release-gate suite:

```text
uv run pytest tests/recovery tests/chaos tests/concurrency tests/performance -q -rs
29 passed, 3 skipped
```

The skips are the explicit live dependency chaos, Redis lock, and PostgreSQL
session lifecycle gates.

The threshold runner was executed three times. Each run reported:

```text
session_count=40
cached_definition_p95_ms=2.0000000000000018
cache_hit_rate=1.0
platform_overhead_p95_ms=73.4604–77.7435 ms
cooperative_cancel_seconds=0.00155–0.00190 s
runtime_events_recorded=440
trace_projections_recorded=40
tool_calls_recorded=40
checkpoints_recorded=40
trace_coverage=1.0
short_task_success_rate=1.0
raw_payloads_recorded=0
credentials_recorded=0
```

All three runs passed the unchanged release thresholds. The deterministic
measurement is not a production-capacity claim.

Regression/static verification:

- Full Python suite: `285 passed, 12 skipped, 1 warning`.
- Task 13 Ruff: passed.
- Task 13 strict Pyright: `0 errors, 0 warnings, 0 informations`.
- `git diff --check`: passed.
- No Task 14, production Runtime, Console, migration, secret, or generated
  cache files were included.

## Live limitations

PostgreSQL, Redis, MinIO, real model, and external Tool Gateway were not
connected. Locust is not installed and was not reported as passed. The runner
uses deterministic doubles for dependencies but executes the real local
Runtime/trace/tool/cancellation code path. Non-cooperative provider hard
cancellation remains unverified.
