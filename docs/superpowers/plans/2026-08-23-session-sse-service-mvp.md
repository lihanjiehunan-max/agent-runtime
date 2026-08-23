# Session SSE Service MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the static Deep Agents runtime as an externally reachable, Bearer-authenticated Session API with true SSE Chat streaming.

**Architecture:** Extend the existing single-process Python runtime with an in-memory Session manager, a tool-disabled Deep Agents graph, and a streaming service that directly adapts LangGraph message chunks to three business-safe event types. FastAPI keeps operations deployment separate from business Session/Chat DTOs while using one environment-configured Bearer token for this MVP.

**Tech Stack:** Python 3.12, FastAPI, Pydantic 2, Deep Agents 0.7.7, LangGraph `InMemorySaver`, `langchain-openai`, pytest, httpx, Uvicorn

**Spec:** `docs/superpowers/specs/2026-08-23-session-sse-service-mvp-design.md`

## Global Constraints

- Gateway is exactly `https://token.zero-api.cc.cd/v1`, model `gpt-5.6-sol`, and API mode `chat_completions`.
- Read credentials only from `MODEL_API_KEY` and `SERVICE_API_KEY`; never commit, log, render, or return either value.
- Bind Uvicorn to `0.0.0.0:8000` in the documented external-service command.
- Agent deployment is explicit and idempotent through the operations endpoint.
- Business requests use logical Agent ID and Session ID only; they never receive Agent Instance, thread, digest, Skill, model, gateway, prompt, or graph details.
- Chat uses true `graph.astream(..., stream_mode="messages")` output and emits only `delta`, `done`, and terminal `error` SSE events.
- `session_id == thread_id`; one active stream per Session, different Sessions may stream concurrently.
- Disable every built-in Deep Agents filesystem, execute, and task tool and disable its default general-purpose subagent.
- No database, Redis, worker, queue, retry, frontend, complex event store, RBAC, resumable stream, or demo fallback.
- External gateway tests are opt-in and must report NOT RUN rather than PASS when either credential is absent.

---

### Task 1: Add service configuration and Session lifecycle

**Files:**
- Modify: `apps/validation_runtime/config.py`
- Modify: `apps/validation_runtime/domain.py`
- Modify: `apps/validation_runtime/services/agent_instance_registry.py`
- Create: `apps/validation_runtime/services/session_manager.py`
- Create: `tests/validation_runtime/test_config.py`
- Create: `tests/validation_runtime/test_sessions.py`

**Interfaces:**
- Consumes: `RuntimeConfig`, `AgentRouter.resolve(agent_id)`, and internal `AgentInstanceRecord` values.
- Produces: `RuntimeConfig.service_api_key`, `RuntimeSession`, `SessionStatus`, `AgentInstanceRegistry.get_record(instance_id)`, `SessionManager.create(agent_id)`, `get(session_id)`, `begin_stream(session_id)`, `lock_for(session_id)`, `mark_succeeded(session_id)`, `mark_failed(session_id)`, and `close(session_id)`.

- [ ] **Step 1: Write failing configuration tests**

Add tests proving `RuntimeConfig.from_env()` wraps both environment values as `SecretStr`, preserves missing values as `None`, and never includes cleartext in `repr(config)`:

```python
def test_runtime_config_reads_service_and_model_secrets(monkeypatch):
    monkeypatch.setenv("MODEL_API_KEY", "model-secret")
    monkeypatch.setenv("SERVICE_API_KEY", "service-secret")
    config = RuntimeConfig.from_env()
    assert config.model_api_key.get_secret_value() == "model-secret"
    assert config.service_api_key.get_secret_value() == "service-secret"
    assert "model-secret" not in repr(config)
    assert "service-secret" not in repr(config)
```

- [ ] **Step 2: Run the configuration tests and observe the red phase**

Run: `PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest tests/validation_runtime/test_config.py -q`

Expected: FAIL because `service_api_key` does not exist.

- [ ] **Step 3: Extend `RuntimeConfig`**

Add `service_api_key: SecretStr | None` and load `SERVICE_API_KEY` without changing the existing asset root or model credential behavior.

- [ ] **Step 4: Write failing Session tests**

Cover:

```python
def test_session_is_bound_to_resolved_instance(runtime_registry, router):
    sessions = SessionManager(router, runtime_registry)
    session = sessions.create("shipping-analyst")
    assert session.session_id.startswith("ses_")
    assert session.thread_id == session.session_id
    assert session.bound_agent_instance_id.startswith("ain_")
    assert session.status is SessionStatus.IDLE


def test_second_stream_claim_is_rejected(sessions, session):
    sessions.begin_stream(session.session_id)
    with pytest.raises(RuntimeServiceError) as exc:
        sessions.begin_stream(session.session_id)
    assert exc.value.detail.code == "SESSION_BUSY"
```

Also assert unavailable routing, unknown Session, immutable Agent/thread/digest binding, success-only turn increments, failure returning to `IDLE`, and close permanently rejecting execution.

- [ ] **Step 5: Run focused Session tests and observe the red phase**

Run: `PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest tests/validation_runtime/test_sessions.py -q`

Expected: collection fails because Session types and manager do not exist.

- [ ] **Step 6: Implement Session domain and manager**

Add frozen snapshot models:

```python
class SessionStatus(StrEnum):
    IDLE = "IDLE"
    STREAMING = "STREAMING"
    CLOSED = "CLOSED"


class RuntimeSession(BaseModel):
    model_config = ConfigDict(frozen=True)
    session_id: str
    thread_id: str
    agent_id: str
    bound_agent_instance_id: str
    package_digest: str
    status: SessionStatus
    turn_count: int
    created_at: datetime
    updated_at: datetime
```

`SessionManager` stores snapshots and one `asyncio.Lock` by ID. `begin_stream` synchronously changes `IDLE` to `STREAMING`, so two requests cannot both claim the same Session in one event loop; the streaming service additionally holds `lock_for(session_id)` around graph consumption. `mark_succeeded` increments once and returns to `IDLE`; `mark_failed` returns to `IDLE` without increment; `close` sets `CLOSED`. Update snapshots with `model_copy(update=...)` and never change binding fields. Test that repeated `lock_for` calls return the same lock for one Session and different locks for different Sessions.

Add `AgentInstanceRegistry.get_record(instance_id) -> AgentInstanceRecord | None` for internal graph lookup; keep `get()` and `list()` public-record-only.

- [ ] **Step 7: Run focused and full tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest tests/validation_runtime/test_config.py tests/validation_runtime/test_sessions.py -q
PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest -q
git diff --check
```

Expected: all pass and diff check is silent.

- [ ] **Step 8: Commit Task 1**

```bash
git add apps/validation_runtime/config.py apps/validation_runtime/domain.py apps/validation_runtime/services/agent_instance_registry.py apps/validation_runtime/services/session_manager.py tests/validation_runtime/test_config.py tests/validation_runtime/test_sessions.py
git commit -m "feat: add runtime sessions"
```

---

### Task 2: Build the tool-disabled Agent and streaming Chat service

**Files:**
- Modify: `apps/validation_runtime/domain.py`
- Create: `apps/validation_runtime/services/model_factory.py`
- Create: `apps/validation_runtime/services/agent_factory.py`
- Create: `apps/validation_runtime/services/streaming_chat_service.py`
- Create: `tests/validation_runtime/fakes.py`
- Create: `tests/validation_runtime/test_agent_factory.py`
- Create: `tests/validation_runtime/test_streaming_chat.py`

**Interfaces:**
- Consumes: `LoadedAgentAssets`, `RuntimeConfig.model_api_key`, `AgentInstanceRegistry.get_record()`, and `SessionManager` lifecycle methods.
- Produces: `ModelFactory.create(profile, key: SecretStr)`, `AgentFactory.build(assets, key: SecretStr | None)`, `ChatDelta`, `ChatDone`, `ChatError`, and async `StreamingChatService.open_stream(session_id, message) -> AsyncIterator[ChatEvent]`.

- [ ] **Step 1: Write failing model and Agent factory tests**

Verify:

```python
def test_model_factory_uses_static_chat_completions_profile(monkeypatch, assets):
    model = ModelFactory().create(assets.gateway, SecretStr("test-secret"))
    assert model.model_name == "gpt-5.6-sol"
    assert str(model.openai_api_base).rstrip("/") == "https://token.zero-api.cc.cd/v1"
    assert model.max_retries == 0


def test_agent_factory_builds_with_skill_prompt_and_no_tools(monkeypatch, assets):
    captured = {}
    monkeypatch.setattr(agent_factory_module, "create_deep_agent", recording_builder(captured))
    AgentFactory(ModelFactory()).build(assets, SecretStr("test-secret"))
    assert captured["tools"] == []
    assert "结论" in captured["system_prompt"]
    assert captured["subagents"] == []
```

Also test missing model key raises `MODEL_API_KEY_MISSING`, an `InMemorySaver` is supplied, and the registered Deep Agents harness profile excludes `ls`, `read_file`, `write_file`, `edit_file`, `delete`, `glob`, `grep`, `execute`, and `task` while disabling the general-purpose subagent.

- [ ] **Step 2: Run factory tests and observe the red phase**

Run: `PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest tests/validation_runtime/test_agent_factory.py -q`

Expected: collection fails because the factories do not exist.

- [ ] **Step 3: Implement model and Agent factories**

Construct `ChatOpenAI` with the static model/base URL, `max_retries=0`, and the profile read timeout. Register a model-specific `HarnessProfile` for `openai:gpt-5.6-sol`:

```python
HarnessProfile(
    excluded_tools=frozenset({
        "ls", "read_file", "write_file", "edit_file", "delete",
        "glob", "grep", "execute", "task",
    }),
    general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
)
```

Build with `create_deep_agent(model=model, tools=[], system_prompt=prompt, subagents=[], checkpointer=InMemorySaver())`. Join the exact loaded Skill instructions; do not enable filesystem-backed Skill discovery.

- [ ] **Step 4: Write failing streaming service tests**

Create `RecordingStreamingGraph.astream(input, config, stream_mode)` that records calls and yields `AIMessageChunk` values. Test:

```python
events = [event async for event in service.open_stream(session.session_id, "hello")]
assert [event.event for event in events] == ["delta", "delta", "done"]
assert "".join(event.content for event in events if event.event == "delta") == "你好"
assert graph.thread_ids == [session.session_id]
assert sessions.get(session.session_id).turn_count == 1
```

Also cover blank or longer-than-`manifest.limits.max_input_characters` input (`INVALID_MESSAGE` before streaming), unknown/closed/busy Session, list-style LangChain text content, different Sessions overlapping, upstream exception → one terminal `MODEL_GATEWAY_ERROR`, timeout → one terminal `MODEL_GATEWAY_TIMEOUT`, cancellation/failure not incrementing turns, and no error followed by done.

- [ ] **Step 5: Run streaming tests and observe the red phase**

Run: `PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest tests/validation_runtime/test_streaming_chat.py -q`

Expected: collection fails because streaming service and event types do not exist.

- [ ] **Step 6: Implement the streaming service**

Define three frozen event models with literal `event` names. `open_stream` validates and claims the Session before returning an inner async iterator. The iterator obtains the bound graph, runs:

```python
async for chunk, metadata in graph.astream(
    {"messages": [{"role": "user", "content": message}]},
    {"configurable": {"thread_id": session.thread_id}},
    stream_mode="messages",
):
    ...
```

Forward only non-empty `AIMessageChunk` text. Hold the Session's stable `asyncio.Lock` while consuming the graph and wrap consumption in `asyncio.timeout(record.assets.manifest.limits.execution_timeout_seconds)`. On success mark the Session succeeded and yield one `ChatDone`; on timeout/provider exception mark failed and yield one normalized `ChatError`; on `CancelledError`, mark failed and re-raise. Do not expose exception text.

- [ ] **Step 7: Run focused and full tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest tests/validation_runtime/test_agent_factory.py tests/validation_runtime/test_streaming_chat.py -q
PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest -q
git diff --check
```

Expected: all pass.

- [ ] **Step 8: Commit Task 2**

```bash
git add apps/validation_runtime/domain.py apps/validation_runtime/services/model_factory.py apps/validation_runtime/services/agent_factory.py apps/validation_runtime/services/streaming_chat_service.py tests/validation_runtime/fakes.py tests/validation_runtime/test_agent_factory.py tests/validation_runtime/test_streaming_chat.py
git commit -m "feat: stream agent chat"
```

---

### Task 3: Expose Bearer-authenticated operations, Session, and SSE APIs

**Files:**
- Create: `apps/validation_runtime/api_models.py`
- Create: `apps/validation_runtime/auth.py`
- Create: `apps/validation_runtime/container.py`
- Create: `apps/validation_runtime/api.py`
- Create: `apps/validation_runtime/main.py`
- Create: `tests/validation_runtime/conftest.py`
- Create: `tests/validation_runtime/test_api.py`

**Interfaces:**
- Consumes: factories, registry, router, Session manager, streaming service, and both runtime secrets.
- Produces: `RuntimeContainer.deploy(agent_id)`, `create_app(container=None, config=None) -> FastAPI`, `/healthz`, deployment/list operations routes, Session creation route, and SSE Chat route.

- [ ] **Step 1: Write failing authentication and deployment API tests**

With an injected fake container/config, test unauthenticated and wrong-token runtime calls return:

```json
{"error":{"code":"AUTHENTICATION_REQUIRED","message":"Authentication is required"}}
```

Test `/healthz` needs no token and reveals only `{"status":"ok"}`. Test operations deployment returns the approved instance fields, is idempotent, and list returns public instance records without graph, URL, or key.

- [ ] **Step 2: Write failing business boundary and SSE tests**

Test Session creation returns exactly `session_id`, `agent_id`, `status`, and `turn_count`. Recursively reject these keys in every business JSON/SSE payload:

```python
FORBIDDEN = {
    "agent_instance_id", "bound_agent_instance_id", "thread_id",
    "package_digest", "skill_ids", "model_alias", "model_gateway_id",
    "gateway", "graph", "prompt", "api_key",
}
```

For Chat, assert `text/event-stream`, `no-cache`, `X-Accel-Buffering: no`, exact event framing/order, UTF-8 JSON, exactly one terminal event, and pre-stream errors remain JSON with appropriate 401/404/409/422 status.

- [ ] **Step 3: Run API tests and observe the red phase**

Run: `PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest tests/validation_runtime/test_api.py -q`

Expected: collection fails because the application modules do not exist.

- [ ] **Step 4: Implement explicit DTOs and authentication**

Create separate operations and business Pydantic response models. Do not derive business models by excluding fields. Implement a FastAPI dependency that parses an exact Bearer scheme and compares with `secrets.compare_digest`; emit only the stable normalized error.

- [ ] **Step 5: Implement the container and routes**

`RuntimeContainer.deploy` loads `shipping-analyst`, requires the model key, builds the graph, and calls the registry. Wire:

- `GET /healthz`
- `POST /api/v1/runtime/ops/agents/{agent_id}/deploy`
- `GET /api/v1/runtime/ops/agent-instances`
- `POST /api/v1/runtime/agents/{agent_id}/sessions`
- `POST /api/v1/runtime/sessions/{session_id}/chat`

Serialize SSE with one-line compact JSON and a blank line after every event. Use `StreamingResponse` and close the async iterator if the response is cancelled. Add exception handlers mapping `RuntimeServiceError` codes to the spec's HTTP statuses. `main.py` exports `app = create_app()`.

- [ ] **Step 6: Verify OpenAPI and startup contracts**

Add tests that missing `SERVICE_API_KEY` fails default application creation, injected test config works, OpenAPI business schemas omit every forbidden field, and operations schemas contain `agent_instance_id` but not gateway URL/key fields.

- [ ] **Step 7: Run focused and full tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest tests/validation_runtime/test_api.py -q
PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest -q
git diff --check
```

Expected: all pass.

- [ ] **Step 8: Commit Task 3**

```bash
git add apps/validation_runtime/api_models.py apps/validation_runtime/auth.py apps/validation_runtime/container.py apps/validation_runtime/api.py apps/validation_runtime/main.py tests/validation_runtime/conftest.py tests/validation_runtime/test_api.py
git commit -m "feat: expose session sse api"
```

---

### Task 4: Add external smoke validation and release evidence

**Files:**
- Create: `tests/validation_runtime/test_real_gateway_stream.py`
- Create: `scripts/validate_streaming_service.py`
- Create: `docs/operations/session-sse-service.md`
- Create: `docs/operations/session-sse-validation-result.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: the running service at a configurable base URL and the two environment credentials.
- Produces: a three-turn SSE validation command, opt-in pytest smoke, operator runbook, and exact verification record.

- [ ] **Step 1: Add an opt-in real-gateway test**

Mark the test `real_gateway` and skip unless both `MODEL_API_KEY` and `SERVICE_API_KEY` are present. Through the ASGI app or a live local server, deploy, create a Session, stream one short turn, and assert at least one non-empty `delta` precedes one `done`. Never assert exact model wording in this smoke.

- [ ] **Step 2: Add the external three-turn validator**

Implement `scripts/validate_streaming_service.py --base-url http://127.0.0.1:8000`. Read `SERVICE_API_KEY` only from the environment. Use `httpx.Client.stream()` to call deployment, Session creation, and the three approved turns:

1. `请记住，我叫 Herry，负责散运业务。`
2. `我叫什么名字，负责什么业务？`
3. `分析航运经营收入时应该关注哪些维度？没有数据的地方不要编造。`

Parse `event:` and `data:` lines, print assistant text only, assert turn two contains `Herry` and `散运`, turn three contains `结论`, `依据`, and `建议`, and every turn has `delta` before exactly one `done`. Scan serialized responses to ensure neither configured secret appears.

- [ ] **Step 3: Add operator documentation**

Document environment variables, the exact `0.0.0.0:8000` Uvicorn command, firewall/proxy port exposure, Bearer header, curl examples for deploy/Session/SSE, restart data loss, no retry/fallback, SSE proxy buffering requirements, and the validation script.

- [ ] **Step 4: Run deterministic release gates**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run pytest -m "not real_gateway" -q
node --test --test-reporter=spec
PYTHONDONTWRITEBYTECODE=1 UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv run python -m compileall -q apps scripts tests
UV_CACHE_DIR=/tmp/agent-runtime-uv-cache uv lock --check
git diff --check
```

Expected: all deterministic Python tests, all 92 baseline Node tests, compile, lock, and diff gates pass.

- [ ] **Step 5: Run or accurately skip external validation**

Check only whether both variables are present; never print values. When configured, start Uvicorn on localhost, run the real pytest marker and three-turn validator, then stop the process. Otherwise record exactly `NOT RUN — MODEL_API_KEY and/or SERVICE_API_KEY not configured`.

- [ ] **Step 6: Record evidence**

Write commit SHA, Python/uv/deepagents versions, deterministic Python and Node counts, compile/lock/diff results, external result, one Agent package digest, business forbidden-field scan result, and secret scan result to `docs/operations/session-sse-validation-result.md`. Never claim external PASS without a real completed call.

- [ ] **Step 7: Commit Task 4**

```bash
git add tests/validation_runtime/test_real_gateway_stream.py scripts/validate_streaming_service.py docs/operations/session-sse-service.md docs/operations/session-sse-validation-result.md README.md
git commit -m "test: validate external sse service"
```

## Final Acceptance Gate

- Explicit operations deployment returns an idempotent active `ain_...` instance using `gpt-5.6-sol`.
- Authenticated business Session creation exposes no runtime instance details.
- Chat response is real `text/event-stream` with ordered `delta` events and exactly one `done` or `error` terminal event.
- The same Session retains Herry/散运 context and uses its Session ID as LangGraph thread ID.
- Same-Session overlap returns `409 SESSION_BUSY`; separate Sessions can stream concurrently.
- Service starts with `--host 0.0.0.0 --port 8000`; docs explain proxy/TLS responsibility.
- Deterministic Python and Node regressions pass; external result is truthfully PASS or NOT RUN.
