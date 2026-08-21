# Task 14 report — deployment, cutover, and acceptance evidence

> This report records the original deployment-only baseline commit. The
> adjacent production composition/readiness/configuration findings were closed
> in `task-14-report-fix-round-1.md`; that fix-round report is the current
> release evidence.

## Scope

The original Task 14 baseline commit added only the deployment process
contracts, integration compose definition, environment template, operations
procedures, and final acceptance evidence. The adjacent production seam fixes
are intentionally recorded separately in
`task-14-report-fix-round-1.md`; neither round modifies the React console,
migrations, or frozen Node implementation.

## Evidence inventory

- Deep Agents: `0.7.7`
- `uv.lock` SHA-256: `d194d2dcc6308998b9ad8352f519303e8c5c2e329d6c31686932943be0ba0d53`
- Acceptance package digest:
  `sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04`
- Package checksum file SHA-256:
  `17e78ea023b77e5c351f7de9244277d469e69555cf98e55eaf9e49db309949fc`
- Migration head: `0002_trace_projection_event_index`

## Commands and results

The final scoped verification was run without PostgreSQL, Redis, MinIO, model,
Tool Gateway, Docker, or an external network call:

```text
UV_CACHE_DIR=/tmp/task14-uv-cache uv run pytest \
  tests/acceptance/test_three_turn_metric_session.py -q -rs
2 passed, 1 skipped in 0.75s
SKIPPED: live Task 14 acceptance requires RUNTIME_TASK14_LIVE=1

UV_CACHE_DIR=/tmp/task14-uv-cache uv run ruff check \
  tests/acceptance/test_three_turn_metric_session.py
All checks passed!

UV_CACHE_DIR=/tmp/task14-uv-cache uv run pyright \
  tests/acceptance/test_three_turn_metric_session.py
0 errors, 0 warnings, 0 informations

deployment_config_validation=passed
services=grafana,minio,model-gateway,postgres,prometheus,redis,
         runtime-api,runtime-console,runtime-worker,tool-gateway
```

`git diff --check` is run again on the staged nine-file scope immediately
before commit. `systemd-analyze verify` parsed both unit files but returned a
non-zero result because `/opt/agent-runtime/.venv/bin/uvicorn` and the packaged
`/opt/agent-runtime/bin/runtime-worker` launcher do not exist in this source
worktree. Those paths are deliberate target-host placeholders; the install
procedure must rerun the command after the release artifact is installed.

The worktree also contains unrelated, uncommitted Task 13 edits. They are not
staged or included in the Task 14 commit.

## Live gate

The live acceptance test is guarded by `RUNTIME_TASK14_LIVE=1` and requires:

- `RUNTIME_TASK14_LIVE_BASE_URL`
- `RUNTIME_TASK14_LIVE_TOKEN`

Without those variables, the live test is an explicit skip. No live model,
Tool Gateway, PostgreSQL, Redis, or MinIO call is claimed by this report.

## Exact limitations

- Deterministic acceptance uses in-process checkpoint, event, model-stream, and
  `query_metric` doubles. It proves identity, fencing, SSE, trace, and
  isolation contracts but not provider or infrastructure availability.
- Task 13's final deterministic harness exercises the local
  `ExecutionManager → EventEmitter/Trace → ToolGateway → CancellationToken`
  path, but it is not a production capacity claim. Locust was not installed
  or run.
- The original deployment-only commit left `/health/ready` as a placeholder;
  the fix round adds the composition-aware readiness path and records its
  remaining live-provider gates.
- The systemd worker unit references the packaged `runtime-worker` launcher;
  the repository's `TaskConsumer` has no new `main` entrypoint in this task.
- Cancellation evidence remains cooperative and does not prove hard provider
  interruption.
