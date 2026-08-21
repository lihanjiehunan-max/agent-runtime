# Enterprise Agent Runtime

This repository is the migration workspace for the Python 3.12 Enterprise Agent Runtime. The new API and worker are intentionally minimal in this first vertical slice; the existing Node.js 0.2 project remains in place as compatibility reference evidence.

## Python workspace

The workspace uses Python 3.12, uv, and a lockfile. Deep Agents is pinned to `0.7.7`.

接手项目时请先阅读：[Agent Runtime MVP 交接文档](docs/operations/handoff.md)。

```bash
uv sync
uv run pytest tests/contract/test_legacy_baseline.py -q
uv run ruff check apps/runtime_api apps/runtime_worker packages \
  tests/contract/test_runtime_composition.py \
  tests/acceptance/test_three_turn_metric_session.py
uv run pyright
```

全仓 Ruff 当前还会报告 `tests/acceptance/test_console_runtime.py:1` 的已知历史 import 顺序问题；详见[交接文档](docs/operations/handoff.md)。

Run the API as a local process:

```bash
uv run uvicorn apps.runtime_api.main:app --host 127.0.0.1 --port 8000
```

The public liveness endpoint is `/health/live`. The readiness endpoint is
`/health/ready`; it reports PostgreSQL, Redis, MinIO, model gateway, Tool
Gateway, SDK, and runtime-binding state and returns HTTP 503 until the
production composition has been bound successfully.

Run the worker health command as a local process:

```bash
uv run python -m apps.runtime_worker.main health
```

## Node compatibility baseline

The original Node entrypoints, source, and tests remain available at the repository root. A frozen copy and its 92/92 test evidence are stored under [`legacy/`](legacy/README.md). No Node entrypoint or test has been removed.
