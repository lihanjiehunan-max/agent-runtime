# Enterprise Agent Runtime

This repository is the migration workspace for the Python 3.12 Enterprise Agent Runtime. The new API and worker are intentionally minimal in this first vertical slice; the existing Node.js 0.2 project remains in place as compatibility reference evidence.

## Python workspace

The workspace uses Python 3.12, uv, and a lockfile. Deep Agents is pinned to `0.7.7`.

```bash
uv sync
uv run pytest tests/contract/test_legacy_baseline.py -q
uv run ruff check .
uv run pyright
```

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
