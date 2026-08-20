# Deep Agents Python Agent Runtime MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Node proof-of-concept execution path with a Python 3.12, Deep Agents 0.7.7 Runtime MVP that deterministically loads Agent Packages, manages persistent multi-turn Sessions, executes controlled turns, and exposes complete traces and metrics.

**Architecture:** Build a Python monorepo with separate FastAPI API and Deep Agents worker entrypoints, a React/TypeScript console, PostgreSQL metadata plus LangGraph checkpoints, Redis Streams for execution commands/live events/cancellation, and MinIO for packages and large payloads. Preserve the Node baseline as a behavioral reference and port its critical identity, fencing, checkpoint-audit, Context, and least-privilege tool invariants.

**Tech Stack:** Python 3.12, `deepagents==0.7.7`, LangGraph/LangChain versions frozen by `uv.lock`, FastAPI, Uvicorn, SQLAlchemy 2 async, Alembic, PostgreSQL, Redis, MinIO, Prometheus client, React, TypeScript, Vite, pytest, testcontainers for integration tests.

**Spec:** `docs/superpowers/specs/2026-08-20-deepagents-python-runtime-mvp-design.md`

## Global Constraints

- Keep `deepagents==0.7.7` pinned and commit `uv.lock`; dependency upgrades require Package, Session, tool, event-stream, and recovery regression suites.
- LangGraph Checkpointer is the only authoritative mutable graph state; Runtime tables do not store a second mutable chat history.
- Every mutable worker write is tenant-scoped and guarded by `execution_epoch` CAS.
- The authenticated principal supplies tenant, user, actor, and worker identity; request JSON cannot override them.
- The MVP enables no subagents, Shell, arbitrary code execution, arbitrary filesystem, direct database access, or unrestricted network access.
- Only the allowlisted read-only `query_metric` tool is enabled in the acceptance Agent Package.
- Local development uses process-based services; integration and CI may use containers.
- Retry hooks exist in contracts, but automatic execution retry is not enabled in the MVP.
- Token delta events are live-only or aggregated; no per-token durable event rows.
- Large package, tool, event, and result bodies are stored in MinIO and represented by immutable references.
- Existing Node 0.2/0.3 behavior is a compatibility reference; new console work targets `/api/v1/runtime`, not `/api/runs`.

---

## Target file map

```text
agent-runtime-mvp/
├─ apps/
│  ├─ runtime_api/
│  │  ├─ main.py
│  │  ├─ dependencies.py
│  │  └─ routes/{agents,sessions,executions,events,status}.py
│  ├─ runtime_worker/
│  │  ├─ main.py
│  │  └─ consumer.py
│  └─ runtime_console/
│     ├─ package.json
│     └─ src/{api,components,pages,types}/
├─ packages/
│  ├─ runtime_contracts/
│  ├─ runtime_persistence/
│  ├─ package_loader/
│  ├─ session_manager/
│  ├─ execution_manager/
│  ├─ event_model/
│  ├─ event_normalizer/
│  ├─ deepagents_adapter/
│  ├─ model_gateway/
│  ├─ tool_gateway/
│  └─ object_store/
├─ agents/agent-metric-query/
├─ migrations/versions/
├─ tests/{unit,integration,contract,concurrency,recovery,acceptance}/
├─ deploy/{process,compose,prometheus,grafana}/
├─ pyproject.toml
└─ uv.lock
```

## Delivery sequence and ownership

| Window | Primary outcome | Main owners |
|---|---|---|
| Days 1-5 | Python SDK/API feasibility gate | Runtime backend 1, AI quality |
| Week 1 | contracts, repository, persistence baseline | Runtime backend 1-2, DevOps |
| Week 2 | package loading, factory, model/tool boundary | Runtime backend 1-2 |
| Week 3 | Session and PostgreSQL Checkpointer | Runtime backend 1-2, DevOps |
| Week 4 | sync/stream/async execution, timeout, cancel | Runtime backend 1-2 |
| Week 5 | event normalization, trace, metrics, console | Backend 2, frontend, DevOps |
| Week 6 | recovery, concurrency, performance, acceptance | whole team |

---

### Task 1: Freeze the baseline and create the Python repository skeleton

**Files:**
- Preserve: `legacy/node-runtime-0.2/**`
- Create: `README.md`
- Create: `pyproject.toml`
- Create: `.python-version`
- Create: `apps/runtime_api/main.py`
- Create: `apps/runtime_worker/main.py`
- Create: `packages/runtime_contracts/__init__.py`
- Create: `tests/contract/test_legacy_baseline.py`

**Interfaces:**
- Consumes: persisted Node 0.2 archive and documented 0.3 HTTP/event behavior.
- Produces: importable Python workspace; `create_api() -> FastAPI`; executable worker health command; frozen legacy contract inventory.

- [ ] **Step 1: Preserve baseline evidence**

Copy the restored Node project into `legacy/node-runtime-0.2`, add `legacy/README.md` recording 92/92 tests and the non-persisted 0.3 behavior list, and save the raw test output under `legacy/evidence/node-0.2-tests.txt`.

- [ ] **Step 2: Write the failing workspace smoke test**

```python
def test_runtime_packages_import() -> None:
    from apps.runtime_api.main import create_api
    from packages.runtime_contracts import RuntimeType
    assert create_api().title == "Enterprise Agent Runtime"
    assert RuntimeType.DEEPAGENTS.value == "deepagents"
```

Run: `uv run pytest tests/contract/test_legacy_baseline.py -q`

Expected: FAIL because the Python workspace does not exist.

- [ ] **Step 3: Create the locked workspace**

Set Python to `3.12`, pin `deepagents==0.7.7`, add FastAPI/Uvicorn/Pydantic/SQLAlchemy/Alembic/asyncpg/redis/minio/prometheus-client and pytest dependencies, then run `uv lock`.

- [ ] **Step 4: Implement the minimum app factories**

Define `RuntimeType(StrEnum)` and `create_api()`. Add public `/health/live` and `/health/ready` routes; readiness initially reports dependency status as `not_configured` rather than falsely healthy.

- [ ] **Step 5: Verify and checkpoint**

Run: `uv run pytest tests/contract/test_legacy_baseline.py -q && uv run ruff check . && uv run pyright`

Expected: PASS.

Commit: `chore: establish Python runtime workspace and freeze Node baseline`

---

### Task 2: Define stable contracts, identity, errors, and event envelopes

**Files:**
- Create: `packages/runtime_contracts/identity.py`
- Create: `packages/runtime_contracts/packages.py`
- Create: `packages/runtime_contracts/sessions.py`
- Create: `packages/runtime_contracts/executions.py`
- Create: `packages/runtime_contracts/events.py`
- Create: `packages/runtime_contracts/errors.py`
- Create: `apps/runtime_api/dependencies.py`
- Test: `tests/unit/test_contracts.py`
- Test: `tests/contract/test_identity_boundary.py`

**Interfaces:**
- Consumes: `RuntimeType` from Task 1.
- Produces: `Principal`, `AgentPackageRef`, `RuntimeSession`, `RuntimeExecution`, `RuntimeEvent`, `RuntimeError`, and authenticated FastAPI dependency `current_principal`.

- [ ] **Step 1: Freeze schema behavior in tests**

Test that IDs are UUID/ULID-safe and below 255 characters, event versions are explicit, unknown fields are rejected, timestamps require timezone, and request payloads containing `tenant_id`, `user_id`, `actor_id`, or `worker_id` are rejected.

- [ ] **Step 2: Implement immutable Pydantic contracts**

Use frozen models for package/session/execution identity and mutable command DTOs only at service boundaries. Define error codes including `PACKAGE_NOT_FOUND`, `DIGEST_MISMATCH`, `RUNTIME_INCOMPATIBLE`, `SESSION_BUSY`, `SESSION_CLOSED`, `EXECUTION_FENCED`, `EXECUTION_TIMED_OUT`, `EXECUTION_CANCELLED`, `MODEL_ERROR`, `TOOL_PERMISSION_DENIED`, and `CHECKPOINT_RECOVERY_FAILED`.

- [ ] **Step 3: Implement fail-closed principal resolution**

Accept development bearer tokens only when `RUNTIME_ALLOW_DEV_AUTH=1`. Production startup fails when no verifier is configured. `x-tenant-id` is diagnostic only and must match the verified principal.

- [ ] **Step 4: Verify stable JSON and SSE envelopes**

Run: `uv run pytest tests/unit/test_contracts.py tests/contract/test_identity_boundary.py -q`

Expected: PASS with deterministic error bodies and event envelopes.

Commit: `feat: define runtime contracts and identity boundary`

---

### Task 3: Build PostgreSQL metadata persistence and migrations

**Files:**
- Create: `packages/runtime_persistence/models.py`
- Create: `packages/runtime_persistence/repositories.py`
- Create: `packages/runtime_persistence/database.py`
- Create: `migrations/env.py`
- Create: `migrations/versions/0001_runtime_metadata.py`
- Test: `tests/integration/test_postgres_schema.py`
- Test: `tests/integration/test_repository_cas.py`

**Interfaces:**
- Consumes: Task 2 contracts.
- Produces: `PackageRepository`, `SessionRepository`, `ExecutionRepository`, `EventRepository`; transactional `begin_execution(session_id, execution_id, principal) -> epoch` and epoch-guarded completion methods.

- [ ] **Step 1: Write schema and tenant isolation tests**

Assert tables `agent_package`, `runtime_session`, `runtime_execution`, `runtime_event`, and `trace_projection`; unique package version/digest constraints; tenant columns on every runtime row; monotonic `(session_id, sequence)`; and foreign keys.

- [ ] **Step 2: Write stale-worker CAS tests**

Create two epochs for one Session and prove an update carrying the old epoch changes zero rows and raises `EXECUTION_FENCED`.

- [ ] **Step 3: Implement migrations and repositories**

Use async SQLAlchemy transactions. `begin_execution` atomically checks Session state, sets `active_execution_id`, increments `execution_epoch` and `revision`, and creates the Execution. Completion clears the active execution only when both ID and epoch match.

- [ ] **Step 4: Run integration tests**

Run: `uv run pytest tests/integration/test_postgres_schema.py tests/integration/test_repository_cas.py -q`

Expected: PASS against PostgreSQL, with no SQLite fallback in integration mode.

Commit: `feat: add runtime metadata persistence and fencing CAS`

---

### Task 4: Implement Agent Package schema, local resolver, and digest verification

**Files:**
- Create: `packages/package_loader/schema.py`
- Create: `packages/package_loader/resolver.py`
- Create: `packages/package_loader/validator.py`
- Create: `packages/package_loader/service.py`
- Create: `agents/agent-metric-query/{manifest.yaml,agent.yaml,tool-bindings.yaml,backend.yaml,limits.yaml,observability.yaml,checksums.txt}`
- Create: `agents/agent-metric-query/prompts/system.md`
- Create: `agents/agent-metric-query/runtime/deepagents/runtime.yaml`
- Test: `tests/unit/package_loader/test_manifest.py`
- Test: `tests/unit/package_loader/test_digest.py`
- Test: `tests/integration/test_local_package_load.py`

**Interfaces:**
- Consumes: `AgentPackageRef`, `PackageRepository`.
- Produces: `LoadedPackage`; `PackageLoader.load(agent_id: str, version: str) -> LoadedPackage`.

- [ ] **Step 1: Write invalid-package tests**

Cover missing file, path traversal, duplicate checksum entry, checksum mismatch, digest mismatch, unsupported schema, runtime mismatch, SDK mismatch, unknown tool binding, and manifest files outside the package root.

- [ ] **Step 2: Implement canonical digest rules**

Sort normalized relative paths, reject symlinks and absolute paths, hash each file, validate `checksums.txt`, then compute the package digest over canonical path/hash pairs. The manifest's digest field is excluded from self-reference and validated against the computed value.

- [ ] **Step 3: Implement local resolver and load state events**

Emit `package.resolve.started/completed`, `package.verify.completed`, `package.cache.miss`, and `agent.definition.created` using the Task 2 event envelope.

- [ ] **Step 4: Build the acceptance package**

Pin `agent-metric-query:0.1.0`, `runtime.type=deepagents`, `sdk_version=0.7.7`, one model reference, one `query_metric` allowlist entry, Session TTL 1440 minutes, timeout 60 seconds, maximum model calls 6, tool calls 8, and tokens 20000.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/unit/package_loader tests/integration/test_local_package_load.py -q`

Commit: `feat: add deterministic local Agent Package loading`

---

### Task 5: Add MinIO package source, local cache, and digest singleflight

**Files:**
- Create: `packages/object_store/client.py`
- Create: `packages/package_loader/minio_source.py`
- Create: `packages/package_loader/cache.py`
- Test: `tests/integration/test_minio_package_source.py`
- Test: `tests/concurrency/test_package_singleflight.py`

**Interfaces:**
- Consumes: Package Loader from Task 4.
- Produces: `PackageSource`, `PackageCache`, and `SingleflightLoader`; cache key `agent-definition:{digest}`.

- [ ] **Step 1: Write source outage and tampering tests**

Assert a cold load fails with `DOWNLOAD_FAILED`, a cached verified package continues to load during source outage, and changed bytes under the same object key fail digest verification.

- [ ] **Step 2: Write 100-request singleflight test**

Start 100 concurrent requests for one digest; assert one download, one validation, one definition build, and identical immutable definitions returned to all callers.

- [ ] **Step 3: Implement atomic cache population**

Download to a unique temporary directory, verify, atomically rename into `<cache_root>/<digest>`, store a success marker last, and ignore incomplete cache directories.

- [ ] **Step 4: Add bounded LRU definition cache**

Use configurable maximum entries and idle TTL; never cache Session state, messages, principal, or mutable Agent instances.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py -q`

Commit: `feat: add verified package cache and cold-load singleflight`

---

### Task 6: Prove Deep Agents 0.7.7 and build the least-privilege Agent Factory

**Files:**
- Create: `packages/deepagents_adapter/factory.py`
- Create: `packages/deepagents_adapter/profile.py`
- Create: `packages/deepagents_adapter/version.py`
- Create: `packages/model_gateway/client.py`
- Test: `tests/contract/test_deepagents_077.py`
- Test: `tests/unit/test_agent_factory.py`

**Interfaces:**
- Consumes: `LoadedPackage` and model/tool references.
- Produces: `AgentDefinition`; `AgentFactory.get(package_digest: str) -> CompiledStateGraph`.

- [ ] **Step 1: Write an installed-SDK compatibility test**

Assert the runtime imports `deepagents==0.7.7`, `create_deep_agent` accepts the pinned checkpointer/backend arguments, and Event Streaming v3 exposes messages, tool calls, values, and output used by the adapter.

- [ ] **Step 2: Write visible-tool tests**

Capture the model request and assert the model sees only `query_metric`; it must not see Shell/execute, unrestricted filesystem, `write_todos`, `task`, subagent, or unregistered network tools.

- [ ] **Step 3: Implement Model Gateway client**

Read model base URL, API key, and model name only from server configuration; standardize timeout, provider error, usage metadata, and request correlation. Do not log keys or raw authorization headers.

- [ ] **Step 4: Implement Agent Factory**

Build from the immutable package definition, `StateBackend`, supplied checkpointer, model, and exact tool allowlist. Cache graph definitions by digest and keep request-scoped principal outside the graph definition.

- [ ] **Step 5: Verify against a local OpenAI-compatible stub**

Run: `uv run pytest tests/contract/test_deepagents_077.py tests/unit/test_agent_factory.py -q`

Expected: a real installed-package call completes one tool turn with no external model traffic.

Commit: `feat: add pinned Deep Agents factory and model gateway`

---

### Task 7: Implement Tool Gateway and real read-only `query_metric`

**Files:**
- Create: `packages/tool_gateway/contracts.py`
- Create: `packages/tool_gateway/service.py`
- Create: `packages/tool_gateway/query_metric.py`
- Test: `tests/contract/test_query_metric.py`
- Test: `tests/security/test_tool_permissions.py`

**Interfaces:**
- Consumes: `Principal`, package tool allowlist, event emitter.
- Produces: LangChain tool `query_metric(metric: str, period: str, org: str) -> MetricResult`.

- [ ] **Step 1: Freeze the tool schema and result**

Test required string fields, maximum lengths, allowed period syntax, stable error codes, identity propagation, timeout, and result size limit. Use a deterministic test result for 营业收入 / 本月 / 散运公司.

- [ ] **Step 2: Implement permission enforcement**

Reject calls when tool name is absent from the package allowlist or principal permissions. Record `tool.started`, then exactly one of `tool.completed` or `tool.failed`.

- [ ] **Step 3: Implement the gateway adapter**

Use an HTTP client with explicit connect/read timeout, request ID, tenant/user propagation, bounded JSON, and error normalization. The Agent never receives gateway credentials.

- [ ] **Step 4: Preserve future effect semantics**

Define execution mode `READ_ONLY | LOCAL_TRANSACTIONAL | EXTERNAL_OUTBOX` in contracts, mark `query_metric` as `READ_ONLY`, and add no write tool implementation in this MVP.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/contract/test_query_metric.py tests/security/test_tool_permissions.py -q`

Commit: `feat: add least-privilege metric query tool gateway`

---

### Task 8: Implement Session Manager and AsyncPostgresSaver

**Files:**
- Create: `packages/session_manager/service.py`
- Create: `packages/session_manager/locks.py`
- Create: `packages/session_manager/checkpointer.py`
- Create: `apps/runtime_api/routes/sessions.py`
- Test: `tests/integration/test_session_lifecycle.py`
- Test: `tests/integration/test_session_checkpointer.py`
- Test: `tests/concurrency/test_session_lock.py`

**Interfaces:**
- Consumes: package repository/loader and PostgreSQL repositories.
- Produces: `create_session`, `get_session`, `resume_session`, `close_session`, `expire_sessions`; `thread_id == session_id`.

- [ ] **Step 1: Write lifecycle tests**

Cover `CREATED -> ACTIVE -> EXECUTING -> IDLE -> CLOSED`, TTL expiration, package digest immutability, tenant isolation, closed/expired rejection, and no Session hot migration.

- [ ] **Step 2: Write checkpoint continuity test**

Execute two graph turns under one `thread_id`, recreate the worker process/graph, then prove the next turn loads prior state through `AsyncPostgresSaver`.

- [ ] **Step 3: Write lock-expiry fencing test**

Let worker A's Redis lock expire, let worker B begin a newer epoch, and prove A cannot append events, checkpoint audit, completion, or Session state.

- [ ] **Step 4: Implement Session Manager**

Pin package digest at creation, keep IDs below 255 characters, acquire a renewable Redis lock, and pair lock ownership with PostgreSQL epoch CAS. Record LangGraph checkpoint ID and an integrity hash over enterprise metadata without duplicating graph state.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/integration/test_session_lifecycle.py tests/integration/test_session_checkpointer.py tests/concurrency/test_session_lock.py -q`

Commit: `feat: add persistent fenced runtime sessions`

---

### Task 9: Implement Execution Manager, normalized Event Streaming v3, and SSE

**Files:**
- Create: `packages/execution_manager/service.py`
- Create: `packages/execution_manager/state.py`
- Create: `packages/event_normalizer/deepagents_v3.py`
- Create: `apps/runtime_api/routes/executions.py`
- Create: `apps/runtime_api/routes/events.py`
- Test: `tests/unit/test_event_normalizer.py`
- Test: `tests/integration/test_sync_execution.py`
- Test: `tests/integration/test_stream_execution.py`
- Test: `tests/contract/test_sse_resume.py`

**Interfaces:**
- Consumes: Agent Factory, Session Manager, Tool Gateway, repositories.
- Produces: `execute_turn`, `execute_turn_stream`; durable event sequence and resumable SSE via `Last-Event-ID`/`after_sequence`.

- [ ] **Step 1: Write state and event-order tests**

Freeze `ACCEPTED -> LOADING_AGENT -> ACQUIRING_SESSION_LOCK -> RUNNING -> SUCCEEDED` and terminal failures. Assert ordered `execution.accepted`, `execution.started`, model/tool events, and one terminal event.

- [ ] **Step 2: Write Event Streaming v3 normalization tests**

Map model start/delta/completion, tool start/output/completion/failure, values, output, and runtime errors. Aggregate model text into bounded chunks and extract usage metadata without persisting hidden reasoning.

- [ ] **Step 3: Implement execution orchestration**

Create Execution and epoch transactionally, load pinned graph, invoke with `configurable.thread_id=session_id` and immutable metadata, and use epoch CAS for every durable write.

- [ ] **Step 4: Implement resumable SSE**

Replay PostgreSQL events after the requested sequence, tail the Redis live stream, emit heartbeats, close on terminal event, and prevent cross-tenant stream subscription.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/unit/test_event_normalizer.py tests/integration/test_sync_execution.py tests/integration/test_stream_execution.py tests/contract/test_sse_resume.py -q`

Commit: `feat: add controlled turn execution and resumable SSE`

---

### Task 10: Add timeout, cooperative cancellation, and simplified async tasks

**Files:**
- Create: `packages/execution_manager/cancellation.py`
- Create: `packages/execution_manager/queue.py`
- Create: `apps/runtime_worker/consumer.py`
- Create: `apps/runtime_api/routes/tasks.py`
- Test: `tests/integration/test_timeout.py`
- Test: `tests/integration/test_cancel.py`
- Test: `tests/integration/test_async_task.py`

**Interfaces:**
- Consumes: Execution Manager and Redis.
- Produces: `execute_async`, `get_task_status`, `cancel_task`; Redis command stream and cancellation token.

- [ ] **Step 1: Write timeout and cancellation race tests**

Cover cancellation before claim, during model wait, during tool wait, after terminal completion, and simultaneous timeout/cancel. Exactly one terminal state must win through CAS.

- [ ] **Step 2: Implement Redis command consumer**

Use a consumer group, bounded claim timeout, command identity, Session/Execution epoch, and explicit acknowledgement only after durable terminal state. Do not enable automatic retry; leave retry policy fields at zero.

- [ ] **Step 3: Implement cooperative cancellation**

Set a Redis cancellation key and event; worker checks before/after model and tool boundaries and cancels the in-process task. Emit `execution.cancelled` within 2 seconds when dependencies cooperate.

- [ ] **Step 4: Implement timeout**

Enforce the package limit with `asyncio.timeout`; normalize to `EXECUTION_TIMED_OUT`, preserve partial bounded output reference, release lock, and fence late writes.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/integration/test_timeout.py tests/integration/test_cancel.py tests/integration/test_async_task.py -q`

Commit: `feat: add cancellable asynchronous runtime tasks`

---

### Task 11: Build trace projection, metrics, payload offload, and retention

**Files:**
- Create: `packages/event_model/emitter.py`
- Create: `packages/event_model/projection.py`
- Create: `packages/event_model/payloads.py`
- Create: `packages/event_model/metrics.py`
- Create: `apps/runtime_api/routes/status.py`
- Create: `deploy/prometheus/prometheus.yml`
- Create: `deploy/grafana/runtime-dashboard.json`
- Test: `tests/integration/test_trace_projection.py`
- Test: `tests/integration/test_payload_offload.py`
- Test: `tests/unit/test_metrics_labels.py`

**Interfaces:**
- Consumes: normalized events and MinIO client.
- Produces: trace timeline DTO, `/metrics`, `/api/v1/runtime/status`, payload references, retention job.

- [ ] **Step 1: Write trace completeness tests**

For one Execution, assert 100% trace ID coverage and reconstruct package resolve/cache, Session lock, model, tool, checkpoint, output, and terminal status in sequence.

- [ ] **Step 2: Write payload boundary tests**

Payloads above the configured threshold are uploaded to immutable object keys and stored as `payload_ref`; token deltas are aggregated; credentials and authorization headers are redacted.

- [ ] **Step 3: Implement bounded Prometheus metrics**

Expose execution count/success/failure, duration histogram, model/tool duration, token totals, package cache hit ratio, active Sessions, queue depth, and cancellation latency. Labels may include stable low-cardinality status/tool/model aliases, never user/session/execution IDs.

- [ ] **Step 4: Implement retention**

Close expired Sessions, retain closed Session metadata/checkpoints for 30 days, prune live Redis streams after durable persistence, and delete unreferenced MinIO payloads only after database reconciliation.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/integration/test_trace_projection.py tests/integration/test_payload_offload.py tests/unit/test_metrics_labels.py -q`

Commit: `feat: add runtime tracing metrics and payload retention`

---

### Task 12: Rebuild the Runtime Console around Agent, Session, chat, trace, and dashboard

**Files:**
- Create: `apps/runtime_console/package.json`
- Create: `apps/runtime_console/src/api/runtimeClient.ts`
- Create: `apps/runtime_console/src/types/runtime.ts`
- Create: `apps/runtime_console/src/pages/AgentsPage.tsx`
- Create: `apps/runtime_console/src/pages/SessionsPage.tsx`
- Create: `apps/runtime_console/src/pages/ChatPage.tsx`
- Create: `apps/runtime_console/src/pages/TracePage.tsx`
- Create: `apps/runtime_console/src/pages/DashboardPage.tsx`
- Create: `apps/runtime_console/src/components/ModeBadge.tsx`
- Test: `apps/runtime_console/src/**/*.test.tsx`
- Test: `tests/acceptance/test_console_runtime.py`

**Interfaces:**
- Consumes: versioned API and SSE from Tasks 8-11.
- Produces: operational console with explicit `LIVE` or `DEMO` mode.

- [ ] **Step 1: Write client contract tests**

Test bearer authentication, no token in URL/logs, Session creation, resumable SSE parsing, cancellation, and normalized error rendering.

- [ ] **Step 2: Implement five console views**

Agents shows package version/digest/load state; Sessions shows status/TTL/package pin; Chat executes multiple turns in one Session; Trace shows package/model/tool/checkpoint/result timeline; Dashboard shows success, P95 latency, tokens, cache hit, and active Sessions.

- [ ] **Step 3: Preserve honest fallback behavior**

No configured API produces explicit DEMO mode. A configured API failure shows connection failure and offers manual DEMO switch; it must not silently label generated demo output as a live model result.

- [ ] **Step 4: Eliminate localhost/public-site mismatch**

In integrated deployment serve console and `/api/v1/runtime` behind the same HTTPS origin. Local development uses a Vite proxy. Do not expect a public static site to reach browser `127.0.0.1` without an explicitly configured local deployment.

- [ ] **Step 5: Verify**

Run: `npm test --prefix apps/runtime_console && npm run build --prefix apps/runtime_console && uv run pytest tests/acceptance/test_console_runtime.py -q`

Commit: `feat: add live Runtime Package Session and trace console`

---

### Task 13: Add recovery, concurrency, chaos, and performance gates

**Files:**
- Create: `tests/recovery/test_worker_restart.py`
- Create: `tests/recovery/test_checkpoint_failure.py`
- Create: `tests/concurrency/test_50_sessions.py`
- Create: `tests/concurrency/test_same_session_serialization.py`
- Create: `tests/chaos/test_dependency_outages.py`
- Create: `tests/performance/locustfile.py`
- Create: `tests/performance/assert_thresholds.py`

**Interfaces:**
- Consumes: complete runtime.
- Produces: reproducible release gates and performance report.

- [ ] **Step 1: Implement worker restart recovery test**

Complete three turns, terminate the worker after a persisted checkpoint, start a new worker, execute a fourth turn, and assert same thread/package/tenant with a newer execution epoch.

- [ ] **Step 2: Implement same-Session race test**

Submit two simultaneous turns to one Session; exactly one runs and the other receives `SESSION_BUSY` or remains queued by the documented mode. No message or checkpoint interleaving is allowed.

- [ ] **Step 3: Implement dependency chaos tests**

Cover MinIO outage with warm/cold package cache, Redis interruption during live SSE, PostgreSQL interruption before/after terminal CAS, model timeout, tool timeout, and stale worker late completion.

- [ ] **Step 4: Implement 30-50 concurrent Session load test**

Use deterministic model/tool stubs for platform latency and a separate real-gateway smoke run. Assert package cache hit at least 95%, cached definition lookup P95 at most 100 ms, platform-added P95 at most 300 ms, cancellation at most 2 seconds, trace coverage 100%, and short-task success at least 95%.

- [ ] **Step 5: Run the full release gate**

Run: `uv run pytest tests -q && npm test --prefix apps/runtime_console && npm run build --prefix apps/runtime_console && uv run locust -f tests/performance/locustfile.py --headless -u 50 -r 10 -t 5m && uv run python tests/performance/assert_thresholds.py`

Commit: `test: add runtime recovery chaos and performance gates`

---

### Task 14: Deployment, migration cutover, and final acceptance

**Files:**
- Create: `deploy/process/runtime-api.service`
- Create: `deploy/process/runtime-worker.service`
- Create: `deploy/compose/compose.integration.yml`
- Create: `deploy/env.example`
- Create: `docs/operations/runbook.md`
- Create: `docs/operations/dependency-upgrade.md`
- Create: `docs/operations/cutover.md`
- Create: `tests/acceptance/test_three_turn_metric_session.py`

**Interfaces:**
- Consumes: all prior tasks.
- Produces: process deployment, integration stack, cutover/rollback procedure, and acceptance evidence.

- [ ] **Step 1: Define process and integration deployment**

Run API, worker, and console as separate processes; provision PostgreSQL, Redis, MinIO, Prometheus, Grafana, Model Gateway, and Tool Gateway in integration. Health readiness checks each required dependency and pinned SDK version.

- [ ] **Step 2: Define migration and rollback**

Keep Node SQLite data as read-only evidence; do not transform demo Runs into Sessions. Run Python API alongside Node under `/api/v1/runtime`, switch console traffic after acceptance, and rollback by restoring the previous console/API route without mutating Python package or Session records.

- [ ] **Step 3: Execute the three-turn acceptance conversation**

Use `agent-metric-query:0.1.0` and ask:

1. `本月散运营业收入是多少？`
2. `同比呢？`
3. `按航线拆开看。`

Assert one Session/thread/digest, three Executions, real `query_metric` calls, SSE output, complete traces, and correct evidence references.

- [ ] **Step 4: Execute worker restart and fourth-turn acceptance**

Restart the worker, ask `其中收入最高的是哪条航线？`, and assert checkpoint continuation with no state leakage across Sessions.

- [ ] **Step 5: Produce release evidence**

Save dependency lock hashes, package digest/checksums, migration version, test output, performance report, trace screenshots, and signed acceptance checklist. Do not label the MVP complete unless all six design acceptance gates pass.

Commit: `docs: add runtime deployment cutover and acceptance evidence`

---

## Definition of done

- Agent Package can be loaded by ID/version, verified by digest, cached by digest, and rejected on tampering or incompatibility.
- Session is first-class, pins package digest, maps to one LangGraph thread, supports multi-turn continuation, TTL, close, and tenant isolation.
- Same Session cannot execute concurrently; stale workers are fenced after lock expiry or takeover.
- Sync, SSE streaming, timeout, cancellation, and simplified async task modes work through one Execution Manager.
- Worker restart continues the next turn from `AsyncPostgresSaver` without a second mutable chat history.
- Model and `query_metric` calls use server-side credentials, strict allowlists, identity propagation, bounded results, and stable errors.
- Every Execution has a reconstructable trace and bounded event stream; metrics meet low-cardinality rules.
- Console shows Agent, Session, live conversation, Execution timeline, and dashboard, with honest LIVE/DEMO labeling.
- Full unit, contract, integration, security, recovery, chaos, concurrency, acceptance, frontend, and performance gates pass.

## Plan self-review

- Spec coverage: all four target capabilities, six proof statements, three deployment units, infrastructure dependencies, MVP exclusions, and performance criteria map to Tasks 1-14.
- Non-regression coverage: authenticated identity, epoch fencing, immutable event order, checkpoint evidence, Context/tool least privilege, and future Tool Effect modes are explicitly retained.
- Placeholder scan: no deferred implementation placeholders are used; every excluded capability is named under Global Constraints or the design scope.
- Type consistency: `AgentPackageRef`, `LoadedPackage`, `RuntimeSession`, `RuntimeExecution`, `RuntimeEvent`, `Principal`, and `execution_epoch` are introduced before downstream consumption.
