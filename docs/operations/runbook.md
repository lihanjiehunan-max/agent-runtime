# Agent Runtime MVP Operations Runbook

## Scope and release posture

This runbook operates the Python Runtime MVP alongside the frozen Node 0.2
compatibility build. The Python API is versioned under `/api/v1/runtime`.
Node SQLite data and the legacy `/api/runs` surface remain read-only evidence;
they are not converted into Python Sessions.

The deployment contract has three application processes:

| Process | Contract | Evidence in this change |
|---|---|---|
| API | FastAPI/uvicorn, `/api/v1/runtime` | `deploy/process/runtime-api.service` |
| Worker | `apps.runtime_worker.consumer:TaskConsumer` launcher | `deploy/process/runtime-worker.service` |
| Console | Static React bundle, same-origin `/api/v1/runtime` proxy | `deploy/compose/compose.integration.yml` |

PostgreSQL, Redis, MinIO, Prometheus, Grafana, Model Gateway, and Tool
Gateway are required integration dependencies. Credentials are injected by
`EnvironmentFile` or the deployment secret store; they are never committed to
this repository or sent to the browser.

## Version and evidence baseline

- Python: `>=3.12,<3.13`
- Deep Agents: `0.7.7`
- LangGraph PostgreSQL checkpoint package: `3.1.2`
- Dependency lock SHA-256: `d194d2dcc6308998b9ad8352f519303e8c5c2e329d6c31686932943be0ba0d53`
- Acceptance package digest: `sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04`
- Package checksum file SHA-256: `17e78ea023b77e5c351f7de9244277d469e69555cf98e55eaf9e49db309949fc`
- Database migration head: `0002_trace_projection_event_index`

Recalculate the lock hash after every dependency change:

```bash
sha256sum uv.lock
uv run python -c "from importlib.metadata import version; print(version('deepagents'))"
```

## First deployment

1. Provision a service account `agent-runtime` and `/opt/agent-runtime`.
2. Install the exact checkout with `uv sync --locked`.
3. Copy `deploy/env.example` to an untracked runtime environment file and
   replace every `<...>` value through the secret/configuration system.
4. Apply migrations with `uv run alembic upgrade head` and record the reported
   revision. Never point the Python migration at the legacy SQLite file.
5. Provision the configured MinIO bucket (`RUNTIME_MINIO_BUCKET`) with the
   injected runtime credentials before enabling traffic; readiness probes this
   bucket and fails closed when it is absent.
6. Install the two systemd units, run `systemctl daemon-reload`, and enable
   `runtime-api` and `runtime-worker`.
7. Deploy the console as a static bundle whose reverse proxy sends only
   `/api/v1/runtime` to the Python API. Keep the legacy console route available.
8. Verify the dependency health checks in the integration compose file or the
   equivalent production probes before accepting traffic.

## Health and readiness checks

```bash
curl --fail http://127.0.0.1:8000/health/live
curl --fail http://127.0.0.1:8000/health/ready
curl --fail -H "Authorization: Bearer $RUNTIME_OPERATOR_TOKEN" \
  http://127.0.0.1:8000/api/v1/runtime/status
systemctl is-active runtime-api runtime-worker
```

The compose stack gates API and worker startup on healthy PostgreSQL, Redis,
MinIO, Model Gateway, and Tool Gateway containers. The API's production
composition is enabled explicitly by `RUNTIME_ENABLE_PRODUCTION_COMPOSITION=1`.
`/health/ready` returns 503 until the configured repositories, checkpoint saver,
model/tool clients, and runtime bundle are bound; it never treats liveness as
readiness. The deployment-owned
`RUNTIME_PRINCIPAL_VERIFIER_FACTORY` must also be installed because the runtime
has no permissive production-auth fallback.

## Acceptance checklist

Run the deterministic contract first:

```bash
UV_CACHE_DIR=/tmp/runtime-mvp-uv-cache-task14 \
  uv run pytest tests/acceptance/test_three_turn_metric_session.py -q -rs
```

The expected local evidence is two deterministic tests passing and one live
test skipped when `RUNTIME_TASK14_LIVE` is not `1`. The six design gates are:

| Gate | Required evidence | Current evidence status |
|---|---|---|
| Package digest | checksum and digest verification, tamper rejection | covered by package-loader suites; see Task 4 report |
| Three turns | one Session/thread, three Executions, pinned digest | deterministic Task 14 acceptance |
| Tool/model trace | `query_metric`, SSE, trace references | deterministic Task 14 acceptance; no live provider |
| Restart continuation | fourth turn after a new worker object and epoch 4 | deterministic Task 14 acceptance |
| Session isolation | separate thread/checkpoint/event namespaces | deterministic Task 14 acceptance |
| 30–50 Sessions | runtime execution load and agreed latency gates | Task 13 deterministic release harness: 40 sessions, 2 ms cached-definition P95, ~73–78 ms platform-overhead P95; not production capacity proof |

Do not mark the MVP production-accepted while the last row or live dependency
rows are unresolved. Task 13 measured 40 sessions, 100% cache hits, 2 ms
cached-definition P95, ~73–78 ms platform-overhead P95, ~0.00155–0.00190 s
cooperative cancellation, 100% trace coverage, and 100% short-task success.
These are local deterministic measurements, not production capacity evidence.

## Worker restart procedure

1. Stop or restart only the worker: `systemctl restart runtime-worker`.
2. Confirm the old worker no longer owns the Session lease.
3. Resume the same Session using its existing `session_id` and `thread_id`.
4. Confirm the package digest is unchanged, the execution epoch increased, and
   the old worker's late checkpoint write is rejected with `EXECUTION_FENCED`.
5. Inspect the trace and SSE replay before returning traffic to the console.

The Redis lock coordinates ownership; PostgreSQL epoch CAS and the LangGraph
checkpointer are the authoritative safety boundaries.

## Incident response

- **Model or Tool Gateway outage:** stop new traffic if the dependency probe is
  failing; preserve existing traces and return a bounded gateway error.
- **Redis outage:** do not bypass session locks or queue fencing. Restore Redis
  and replay only commands whose durable terminal state is absent.
- **PostgreSQL outage:** do not claim completion from an in-memory result. The
  terminal event and execution state must commit through PostgreSQL.
- **MinIO outage:** fail closed for cold package loads and large payloads; use
  only verified warm cache entries.
- **Credential exposure concern:** rotate the provider credential in the
  secret store, invalidate the affected worker, and inspect logs and payload
  references. Do not copy raw payloads into tickets.

## Exact limitations

- No live PostgreSQL, Redis, MinIO, Model Gateway, or Tool Gateway proof is
  produced unless the documented environment gates are enabled.
- The optional Locust CLI is not installed in the current workspace; the
  deterministic threshold script must not be described as a Locust run.
- Cancellation is cooperative. A non-cooperative provider may outlive a client
  request until its provider timeout.
- The systemd worker launcher is a packaging boundary; this repository exposes
  `TaskConsumer` but does not add a new worker `main` entrypoint in Task 14.
- The live provider and deployment-owned principal verifier gates were not run
  in this workspace; the API fails closed when they are missing.
