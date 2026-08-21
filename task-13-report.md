# Task 13 release-gate report

## Scope

Task 13 adds release-gate tests only. Production Runtime, Console, migrations,
and the preceding agents' files were not modified. The tests use deterministic
doubles by default; the live dependency probe is opt-in and does not expose
service URLs, credentials, prompts, or model payloads.

## Coverage

| Gate | Evidence |
|---|---|
| Worker restart and checkpoint recovery | Three persisted turns resume on the same Session/`thread_id` and pinned Package digest; the restarted worker moves the epoch from 3 to 4; a late write from the old worker is rejected with `EXECUTION_FENCED` and does not change the fourth checkpoint. |
| Checkpoint failure | Verification and persistence failures both fail closed; neither deterministic transaction commits a partial checkpoint. |
| Terminal CAS and stale completion | Failure before terminal commit leaves no terminal; an acknowledgement failure after the simulated commit leaves exactly one terminal; late writes are fenced. |
| Dependency outages | MinIO cold-cache outage fails without a verified cache entry; warm cache loads without a second download; SSE interruption emits only the durable/live event observed and fabricates no terminal; model/tool stalls become bounded timeout terminals. |
| Session concurrency | Same-Session contender receives the documented busy/lock outcome while one owner runs; different Sessions overlap and keep per-Session event order. |
| 30–50 Session performance | A bounded 40-Session deterministic harness exercises the package definition cache, platform event/trace coverage, cooperative cancellation, and short-task success. |

## Measurements

Command:

```text
UV_CACHE_DIR=/tmp/runtime-mvp-uv-cache-task13 uv run python tests/performance/assert_thresholds.py
```

Measured in the final run:

| Metric | Threshold | Measured |
|---|---:|---:|
| Session count | 30–50 | 40 |
| Package cache hit rate | >=95% | 100% |
| Cached definition P95 | <=100 ms | 4.458 ms |
| Platform overhead P95 | <=300 ms | 0.123 ms |
| Cooperative cancellation | <=2 s | 0.001130 s |
| Trace coverage | 100% | 100% |
| Short-task success | >=95% | 100% |
| Raw payloads recorded | 0 | 0 |
| Credentials recorded | 0 | 0 |

These are deterministic local measurements, not production-capacity claims.

## Verification

- Task 13 focused suite: **14 passed, 1 skipped**.
- Full Python suite: **278 passed, 11 skipped, 1 existing FastAPI deprecation warning**.
- Runtime Console tests: **20 passed**.
- Runtime Console production build: **passed**.
- Ruff on Task 13 files: **passed**.
- Strict Pyright on Task 13 files: **0 errors, 0 warnings**.
- Deterministic threshold script: **passed**.
- `git diff --check`: run after explicit staging review.

## Explicit skips and limitations

- The Task 13 live dependency probe is skipped unless `RUNTIME_TASK13_LIVE=1`
  and all required PostgreSQL, Redis, and MinIO variables are configured.
- Existing full-suite live PostgreSQL, Redis, and MinIO tests remain skipped
  when their documented environment variables are absent.
- The repository does not install the optional Locust CLI, so the planned
  `uv run locust ...` command was not executed as a passing gate. The bounded
  deterministic runner in `tests/performance/locustfile.py` was executed and
  passed the thresholds above.
- No real model or Tool Gateway was called. Non-cooperative provider hard
  cancellation is therefore not proven; the gate measures cooperative
  cancellation only.
