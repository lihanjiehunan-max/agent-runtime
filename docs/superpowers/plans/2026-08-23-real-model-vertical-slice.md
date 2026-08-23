# Deep Agents Real-Model Vertical Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a retained Python validation runtime that deploys one static Deep Agents Agent Instance, routes a logical Agent to it, creates a stateful Session, and completes real multi-turn conversation through the configured `gpt-5.6-sol` gateway while keeping Agent Instance details out of the business surface.

**Architecture:** Add a Python 3.12 FastAPI application beside the existing Node Kernel. One process contains static asset loading, Agent Instance registry, deterministic logical-Agent router, in-memory Session and event stores, a Deep Agents 0.7.7 adapter with `InMemorySaver`, synchronous Execution service, separate business and operations APIs, and separate `/` and `/ops` pages.

**Tech Stack:** Python 3.12, uv, `deepagents==0.7.7`, LangGraph `InMemorySaver`, `langchain-openai`, FastAPI, Uvicorn, Pydantic, pytest, HTTPX, plain HTML/CSS/JavaScript.

**Spec:** `docs/superpowers/specs/2026-08-23-real-model-vertical-slice-design.md`

## Global Constraints

- Keep the existing Node.js 24 Runtime Kernel runnable and its tests unchanged.
- Pin `deepagents==0.7.7`; resolve and commit all other Python dependencies through `uv.lock`.
- Use gateway `https://token.zero-api.cc.cd/v1`, model `gpt-5.6-sol`, and API mode `chat_completions`.
- Read the credential only from `MODEL_API_KEY`; never commit, log, render, or return it.
- Load one static Skill, `shipping-operations-analyst`, and one static Agent, `shipping-analyst:0.1.0`.
- Do not enable tools, MCP, Shell, filesystem access, code execution, unrestricted network tools, or subagents.
- Do not add Redis, PostgreSQL, MinIO, a separate worker, retries, async tasks, or durable restart recovery.
- Use `session_id` as LangGraph `thread_id`; bind each Session immutably to one Agent Instance and digest.
- Permit at most one active Execution per Session using an in-process lock.
- Business APIs never expose Agent Instance, thread, trace, digest, Skill, model, gateway, or internal events.
- Operations APIs expose routing evidence but never the API key.
- Serve business page `/` and operations page `/ops`; neither may synthesize Demo output.

---

### Task 1: Bootstrap Python and validate static runtime assets

**Files:**
- Create: `pyproject.toml`
- Create: `uv.lock`
- Create: `apps/__init__.py`
- Create: `apps/validation_runtime/__init__.py`
- Create: `apps/validation_runtime/config.py`
- Create: `apps/validation_runtime/domain.py`
- Create: `apps/validation_runtime/errors.py`
- Create: `apps/validation_runtime/services/__init__.py`
- Create: `apps/validation_runtime/services/asset_loader.py`
- Create: `runtime_assets/model_gateways/zero-api.json`
- Create: `runtime_assets/skills/shipping-operations-analyst/SKILL.md`
- Create: `runtime_assets/agents/shipping-analyst/manifest.json`
- Create: `tests/validation_runtime/__init__.py`
- Create: `tests/validation_runtime/test_assets.py`

**Interfaces:**
- Consumes: repository root and `MODEL_API_KEY` environment variable.
- Produces: `RuntimeConfig.from_env()`, `AssetLoader.load_agent(agent_id)`, `AgentManifest`, `ModelGatewayProfile`, `LoadedSkill`, `LoadedAgentAssets`, `RuntimeServiceError`.

- [ ] **Step 1: Add Python dependencies and failing asset tests**

Create `pyproject.toml`:

```toml
[project]
name = "agent-runtime-validation"
version = "0.1.0"
requires-python = ">=3.12,<3.13"
dependencies = [
  "deepagents==0.7.7",
  "fastapi",
  "langchain-openai",
  "pydantic>=2",
  "uvicorn[standard]",
]

[dependency-groups]
dev = ["httpx", "pytest", "pytest-asyncio"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
markers = ["real_gateway: calls the configured external model gateway"]
```

Create `test_assets.py` with these exact contracts:

```python
def test_loads_static_assets(asset_root):
    assets = AssetLoader(asset_root).load_agent("shipping-analyst")
    assert assets.manifest.version == "0.1.0"
    assert str(assets.gateway.base_url).rstrip("/") == "https://token.zero-api.cc.cd/v1"
    assert assets.gateway.model == "gpt-5.6-sol"
    assert [x.skill_id for x in assets.skills] == ["shipping-operations-analyst"]
    assert "结论" in assets.skills[0].instructions
    assert len(assets.package_digest) == 64


def test_unknown_agent_has_stable_error(asset_root):
    with pytest.raises(RuntimeServiceError) as exc:
        AssetLoader(asset_root).load_agent("missing")
    assert exc.value.detail.code == "AGENT_NOT_FOUND"
```

- [ ] **Step 2: Run the tests and verify failure**

```bash
uv lock
uv run pytest tests/validation_runtime/test_assets.py -q
```

Expected: collection fails because application modules and assets are absent.

- [ ] **Step 3: Add exact static assets**

Create `zero-api.json` with gateway ID `zero-api-gpt-5.6-sol`, the approved URL/model, `api_key_env: MODEL_API_KEY`, connect timeout 10 seconds, read timeout 120 seconds, and `api_mode: chat_completions`. Create the manifest from the spec. Create `SKILL.md` that requires `结论`, `依据`, `建议`, preserves same-Session facts, distinguishes facts from assumptions, and prohibits claims of system access.

- [ ] **Step 4: Implement immutable asset models and loader**

Use frozen Pydantic models. `AssetLoader.load_agent()` parses JSON and Skill front matter, resolves every reference, canonicalizes sorted compact JSON, hashes manifest bytes, gateway bytes without resolved secrets, exact Skill bytes, and `deepagents:0.7.7`, then returns:

```python
class RuntimeConfig(BaseModel):
    asset_root: Path
    model_api_key: SecretStr | None

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        key = os.getenv("MODEL_API_KEY")
        return cls(
            asset_root=Path(__file__).parents[2] / "runtime_assets",
            model_api_key=SecretStr(key) if key else None,
        )


class RuntimeErrorDetail(BaseModel):
    code: str
    message: str


class RuntimeServiceError(Exception):
    def __init__(self, code: str, message: str):
        self.detail = RuntimeErrorDetail(code=code, message=message)
        super().__init__(message)


class ModelGatewayProfile(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str
    provider: Literal["openai-compatible"]
    base_url: AnyHttpUrl
    model: str
    api_mode: Literal["chat_completions"]
    api_key_env: Literal["MODEL_API_KEY"]
    connect_timeout_seconds: int
    read_timeout_seconds: int


class AgentLimits(BaseModel):
    model_config = ConfigDict(frozen=True)
    execution_timeout_seconds: int
    max_input_characters: int


class AgentManifest(BaseModel):
    model_config = ConfigDict(frozen=True)
    schema_version: Literal["1.0"]
    agent_id: str
    version: str
    runtime: Literal["deepagents-python"]
    model_gateway_id: str
    skills: tuple[str, ...]
    limits: AgentLimits


class LoadedSkill(BaseModel):
    model_config = ConfigDict(frozen=True)
    skill_id: str
    instructions: str
    source_bytes: bytes


class LoadedAgentAssets(BaseModel):
    model_config = ConfigDict(frozen=True)
    manifest: AgentManifest
    gateway: ModelGatewayProfile
    skills: tuple[LoadedSkill, ...]
    package_digest: str
```

Invalid references raise `AGENT_ASSET_INVALID`; missing Agent raises `AGENT_NOT_FOUND`.

- [ ] **Step 5: Pass tests and commit**

```bash
uv run pytest tests/validation_runtime/test_assets.py -q
uv lock --check
git add pyproject.toml uv.lock apps/validation_runtime runtime_assets tests/validation_runtime
git commit -m "feat: add static validation runtime assets"
```

---

### Task 2: Implement operations-only Agent Instances and logical routing

**Files:**
- Modify: `apps/validation_runtime/domain.py`
- Create: `apps/validation_runtime/services/agent_instance_registry.py`
- Create: `apps/validation_runtime/services/agent_router.py`
- Create: `tests/validation_runtime/test_agent_instances.py`
- Create: `tests/validation_runtime/test_agent_routing.py`

**Interfaces:**
- Consumes: `LoadedAgentAssets` and an opaque compiled graph.
- Produces: `AgentInstanceRegistry.deploy(assets, graph)`, `get(instance_id)`, `list()`; `AgentRouter.resolve(agent_id)` and `list_logical_agents()`.

- [ ] **Step 1: Write failing registry and routing tests**

```python
def test_deploy_is_idempotent(loaded_assets, fake_graph):
    registry = AgentInstanceRegistry()
    first = registry.deploy(loaded_assets, fake_graph)
    second = registry.deploy(loaded_assets, fake_graph)
    assert first.agent_instance_id == second.agent_instance_id
    assert first.package_digest == loaded_assets.package_digest


def test_router_rejects_agent_without_active_instance():
    with pytest.raises(RuntimeServiceError) as exc:
        AgentRouter(AgentInstanceRegistry()).resolve("shipping-analyst")
    assert exc.value.detail.code == "AGENT_INSTANCE_UNAVAILABLE"
```

- [ ] **Step 2: Verify focused tests fail**

Run: `uv run pytest tests/validation_runtime/test_agent_instances.py tests/validation_runtime/test_agent_routing.py -q`

Expected: imports fail for registry and router.

- [ ] **Step 3: Add immutable instance contracts**

```python
class AgentInstanceStatus(StrEnum):
    ACTIVE = "ACTIVE"
    FAILED = "FAILED"


class AgentInstance(BaseModel):
    model_config = ConfigDict(frozen=True)
    agent_instance_id: str
    agent_id: str
    agent_version: str
    package_digest: str
    runtime_type: str
    model_gateway_id: str
    model_alias: str
    skill_ids: tuple[str, ...]
    status: AgentInstanceStatus
    created_at: datetime


class LogicalAgent(BaseModel):
    model_config = ConfigDict(frozen=True)
    agent_id: str
    display_name: str
    available: bool


@dataclass(frozen=True)
class AgentInstanceRecord:
    instance: AgentInstance
    assets: LoadedAgentAssets
    graph: object
```

Keep the compiled graph only in internal `AgentInstanceRecord`.

- [ ] **Step 4: Implement registry and deterministic routing**

Generate `ain_` UUID IDs, index by ID and digest, and return the same active instance for repeat deployment of a digest. Router returns the sole active exact `agent_id` match; zero matches raises `AGENT_INSTANCE_UNAVAILABLE`; multiple matches raise `AGENT_ROUTING_AMBIGUOUS` instead of selecting arbitrarily.

- [ ] **Step 5: Pass tests and commit**

```bash
uv run pytest tests/validation_runtime/test_agent_instances.py tests/validation_runtime/test_agent_routing.py -q
git add apps/validation_runtime/domain.py apps/validation_runtime/services/agent_instance_registry.py apps/validation_runtime/services/agent_router.py tests/validation_runtime/test_agent_instances.py tests/validation_runtime/test_agent_routing.py
git commit -m "feat: add agent instance routing"
```

---

### Task 3: Implement Session lifecycle and immutable instance binding

**Files:**
- Modify: `apps/validation_runtime/domain.py`
- Create: `apps/validation_runtime/services/session_manager.py`
- Create: `tests/validation_runtime/test_sessions.py`

**Interfaces:**
- Consumes: `AgentRouter.resolve(agent_id)`.
- Produces: `SessionManager.create(agent_id)`, `get(session_id)`, `close(session_id)`, `lock_for(session_id)`, `list_by_instance(agent_instance_id)`.

- [ ] **Step 1: Write failing Session tests**

```python
def test_session_pins_thread_instance_and_digest(runtime_services):
    session = runtime_services.sessions.create("shipping-analyst")
    assert session.thread_id == session.session_id
    assert session.bound_agent_instance_id == runtime_services.instance.agent_instance_id
    assert session.package_digest == runtime_services.instance.package_digest


def test_closed_session_is_not_executable(runtime_services):
    session = runtime_services.sessions.create("shipping-analyst")
    runtime_services.sessions.close(session.session_id)
    with pytest.raises(RuntimeServiceError) as exc:
        runtime_services.sessions.require_executable(session.session_id)
    assert exc.value.detail.code == "SESSION_CLOSED"
```

- [ ] **Step 2: Verify tests fail**

Run: `uv run pytest tests/validation_runtime/test_sessions.py -q`

Expected: import failure for `SessionManager`.

- [ ] **Step 3: Add frozen Session model and explicit transitions**

```python
class SessionStatus(StrEnum):
    ACTIVE = "ACTIVE"
    EXECUTING = "EXECUTING"
    IDLE = "IDLE"
    CLOSED = "CLOSED"


class RuntimeSession(BaseModel):
    model_config = ConfigDict(frozen=True)
    session_id: str
    thread_id: str
    agent_id: str
    bound_agent_instance_id: str
    package_digest: str
    status: SessionStatus
    created_at: datetime
    last_execution_id: str | None = None
    turn_count: int = 0
```

Transitions replace the frozen stored value with `model_copy(update={"status": SessionStatus.EXECUTING})` and corresponding explicit update mappings. No transition accepts a new instance ID or digest.

- [ ] **Step 4: Implement lifecycle and per-Session lock**

Generate `ses_` UUID IDs and set `thread_id=session_id`. Store one `asyncio.Lock` per Session. Implement `mark_executing`, `mark_succeeded`, and `mark_failed`; success increments turn count and returns Session to `IDLE`, failure returns it to `IDLE`, and close moves it permanently to `CLOSED`.

- [ ] **Step 5: Prove same-Session critical sections do not overlap**

Add two concurrent coroutines using `async with sessions.lock_for(session_id)` and assert a shared active counter never exceeds one.

Run: `uv run pytest tests/validation_runtime/test_sessions.py -q`

Expected: binding, lifecycle, closing, and serialization tests pass.

- [ ] **Step 6: Commit Session management**

```bash
git add apps/validation_runtime/domain.py apps/validation_runtime/services/session_manager.py tests/validation_runtime/test_sessions.py
git commit -m "feat: add bound runtime sessions"
```

---

### Task 4: Build model, Deep Agents, Execution, and event services

**Files:**
- Modify: `apps/validation_runtime/domain.py`
- Create: `apps/validation_runtime/services/model_factory.py`
- Create: `apps/validation_runtime/services/agent_factory.py`
- Create: `apps/validation_runtime/services/event_recorder.py`
- Create: `apps/validation_runtime/services/execution_service.py`
- Create: `tests/validation_runtime/fakes.py`
- Create: `tests/validation_runtime/test_agent_factory.py`
- Create: `tests/validation_runtime/test_executions.py`

**Interfaces:**
- Consumes: loaded assets, `MODEL_API_KEY`, `AgentInstanceRecord`, `SessionManager`, and `InMemorySaver`.
- Produces: `ModelFactory.create(profile)`, `AgentFactory.build(assets)`, `EventRecorder.append(event_type, session, execution, data) -> RuntimeEvent`, `ExecutionService.execute(session_id, message) -> RuntimeExecution`.

- [ ] **Step 1: Write failing factory tests**

```python
def test_model_uses_profile_and_server_secret(monkeypatch, gateway):
    monkeypatch.setenv("MODEL_API_KEY", "test-secret")
    model = ModelFactory().create(gateway)
    assert model.model_name == "gpt-5.6-sol"
    assert str(model.openai_api_base).rstrip("/") == "https://token.zero-api.cc.cd/v1"
    assert "test-secret" not in repr(model)


def test_agent_has_no_tools_and_contains_skill(monkeypatch, loaded_assets):
    captured = {}
    monkeypatch.setattr(
        "apps.validation_runtime.services.agent_factory.create_deep_agent",
        lambda **kwargs: captured.update(kwargs) or object(),
    )
    AgentFactory(fake_model=object()).build(loaded_assets)
    assert captured["tools"] == []
    assert "结论" in captured["system_prompt"]
    assert captured["checkpointer"] is not None
```

- [ ] **Step 2: Verify factory tests fail**

Run: `uv run pytest tests/validation_runtime/test_agent_factory.py -q`

Expected: imports fail for both factories.

- [ ] **Step 3: Implement real model and Agent factories**

Missing key raises `MODEL_API_KEY_MISSING`. Create `ChatOpenAI` with the static base URL/model, zero retries, and configured timeout. `AgentFactory` owns one `InMemorySaver` and calls:

```python
create_deep_agent(
    model=model,
    tools=[],
    system_prompt="\n\n".join(skill.instructions for skill in assets.skills),
    checkpointer=self._checkpointer,
)
```

Do not cache Session, messages, principal, or instance state in the factory.

- [ ] **Step 4: Write failing Execution and event tests**

Add the exact domain contracts before the tests:

```python
class ExecutionStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"


class RuntimeExecution(BaseModel):
    model_config = ConfigDict(frozen=True)
    execution_id: str
    session_id: str
    agent_id: str
    executing_agent_instance_id: str
    trace_id: str
    status: ExecutionStatus
    input_message: str
    output_message: str | None = None
    started_at: datetime
    completed_at: datetime | None = None
    error: RuntimeErrorDetail | None = None


class RuntimeEvent(BaseModel):
    model_config = ConfigDict(frozen=True)
    sequence: int
    event_type: str
    session_id: str
    execution_id: str
    trace_id: str
    agent_id: str
    agent_instance_id: str
    package_digest: str
    model_alias: str
    skill_ids: tuple[str, ...]
    timestamp: datetime
    data: dict[str, object]
```

Implement test `RecordingGraph.ainvoke(input, config)` that records `config["configurable"]["thread_id"]` and returns a deterministic `AIMessage`. Freeze:

```python
@pytest.mark.asyncio
async def test_two_turns_share_thread(runtime_services):
    session = runtime_services.sessions.create("shipping-analyst")
    first = await runtime_services.executions.execute(session.session_id, "我叫 Herry")
    second = await runtime_services.executions.execute(session.session_id, "我叫什么")
    assert first.execution_id != second.execution_id
    assert runtime_services.graph.thread_ids == [session.session_id, session.session_id]
    assert runtime_services.sessions.get(session.session_id).turn_count == 2


@pytest.mark.asyncio
async def test_success_event_sequence(runtime_services):
    session = runtime_services.sessions.create("shipping-analyst")
    await runtime_services.executions.execute(session.session_id, "hello")
    assert [x.event_type for x in runtime_services.events.for_session(session.session_id)] == [
        "execution.accepted", "execution.started", "model.started",
        "model.completed", "execution.succeeded",
    ]
```

- [ ] **Step 5: Implement orchestration and bounded redacted events**

Generate `exe_` and `trc_` IDs, validate input, acquire Session lock, emit the exact event sequence, and call:

```python
await graph.ainvoke(
    {"messages": [{"role": "user", "content": message}]},
    {"configurable": {"thread_id": session.thread_id}},
)
```

Wrap execution in `asyncio.timeout(120)`. Emit one terminal event. Map timeout and provider failures to stable codes. Bound event JSON to 4 KiB and redact keys matching `authorization`, `api_key`, `token`, or `secret` case-insensitively.

- [ ] **Step 6: Pass execution tests and commit**

```bash
uv run pytest tests/validation_runtime/test_agent_factory.py tests/validation_runtime/test_executions.py -q
git add apps/validation_runtime tests/validation_runtime/fakes.py tests/validation_runtime/test_agent_factory.py tests/validation_runtime/test_executions.py
git commit -m "feat: add deep agents execution core"
```

---

### Task 5: Expose separate business and operations APIs

**Files:**
- Create: `apps/validation_runtime/api_models.py`
- Create: `apps/validation_runtime/container.py`
- Create: `apps/validation_runtime/api.py`
- Create: `apps/validation_runtime/main.py`
- Create: `tests/validation_runtime/conftest.py`
- Create: `tests/validation_runtime/test_api.py`

**Interfaces:**
- Consumes: Tasks 1–4 services.
- Produces: `create_app(container=None) -> FastAPI`, business `/api/v1/runtime/*`, operations `/api/v1/runtime/ops/*`.

- [ ] **Step 1: Write failing audience-boundary tests**

```python
BUSINESS_FORBIDDEN = {
    "agent_instance_id", "bound_agent_instance_id", "thread_id", "trace_id",
    "package_digest", "skill_ids", "model_alias", "model_gateway_id",
}


def assert_business_safe(value):
    if isinstance(value, dict):
        assert not (BUSINESS_FORBIDDEN & value.keys())
        for child in value.values():
            assert_business_safe(child)
    elif isinstance(value, list):
        for child in value:
            assert_business_safe(child)
```

Test operations deploy, business Agent listing, business Session creation, business execution, operations Session inspection, and operations events. Apply `assert_business_safe` to every business payload.

- [ ] **Step 2: Verify API tests fail**

Run: `uv run pytest tests/validation_runtime/test_api.py -q`

Expected: import failure for app and DTOs.

- [ ] **Step 3: Implement explicit DTOs**

```python
class BusinessSessionResponse(BaseModel):
    session_id: str
    agent_id: str
    status: SessionStatus
    turn_count: int


class BusinessExecutionResponse(BaseModel):
    execution_id: str
    session_id: str
    agent_id: str
    status: ExecutionStatus
    message: str


class OpsSessionResponse(BaseModel):
    session_id: str
    thread_id: str
    agent_id: str
    bound_agent_instance_id: str
    package_digest: str
    model_alias: str
    skill_ids: tuple[str, ...]
    status: SessionStatus
    turn_count: int
```

Do not derive business DTOs by dynamically excluding operations fields.

- [ ] **Step 4: Build container, routes, and error mapping**

Implement every route in spec section 7. Static catalog loads at startup; Agent Instance is created only by operations deploy. Map an unavailable instance to `{ "error": { "code": "AGENT_INSTANCE_UNAVAILABLE", "message": "No active Agent Instance is available for shipping-analyst" } }`; use 404 unknown, 409 conflict, 422 invalid input/assets, 502 gateway, and 504 timeout.

- [ ] **Step 5: Check OpenAPI schemas and pass tests**

Add a test that business schemas in `/openapi.json` contain no forbidden field while operations schemas contain `agent_instance_id`.

```bash
uv run pytest tests/validation_runtime/test_api.py -q
git add apps/validation_runtime/api_models.py apps/validation_runtime/container.py apps/validation_runtime/api.py apps/validation_runtime/main.py tests/validation_runtime/conftest.py tests/validation_runtime/test_api.py
git commit -m "feat: separate business and operations APIs"
```

---

### Task 6: Build separate business and operations pages

**Files:**
- Create: `apps/validation_runtime/static/index.html`
- Create: `apps/validation_runtime/static/business.js`
- Create: `apps/validation_runtime/static/ops.html`
- Create: `apps/validation_runtime/static/ops.js`
- Create: `apps/validation_runtime/static/styles.css`
- Modify: `apps/validation_runtime/api.py`
- Create: `tests/validation_runtime/test_console.py`

**Interfaces:**
- Consumes: Task 5 APIs.
- Produces: business page `/`, operations page `/ops`, and assets under `/static`.

- [ ] **Step 1: Write failing page-boundary tests**

```python
def test_business_page_uses_no_ops_surface(client):
    html = client.get("/").text
    js = client.get("/static/business.js").text
    assert "/api/v1/runtime/agents" in js
    assert "/api/v1/runtime/ops" not in html + js
    assert "agent_instance_id" not in html + js


def test_ops_page_uses_ops_surface(client):
    html = client.get("/ops").text
    js = client.get("/static/ops.js").text
    assert "/api/v1/runtime/ops" in js
    assert "Agent Instance" in html
```

Also assert both surfaces contain no API key, authorization header construction, or Demo fixtures.

- [ ] **Step 2: Verify page tests fail**

Run: `uv run pytest tests/validation_runtime/test_console.py -q`

Expected: routes return 404.

- [ ] **Step 3: Implement business page**

Load logical Agents, create Session via `/agents/{agent_id}/sessions`, submit turns via `/sessions/{session_id}/executions`, and render only Agent ID, Session ID, turn count, Execution ID, status, messages, and business-safe errors. Disable chat before Session creation and during execution.

- [ ] **Step 4: Implement operations page**

Show readiness, deploy `shipping-analyst`, list instances, show ID/version/digest/Skill/model/status, list Sessions bound to a selected instance, and inspect Session events and Execution traces. Never accept or render an API key.

- [ ] **Step 5: Serve pages, pass tests, and commit**

```bash
uv run pytest tests/validation_runtime/test_console.py tests/validation_runtime/test_api.py -q
git add apps/validation_runtime/static apps/validation_runtime/api.py tests/validation_runtime/test_console.py
git commit -m "feat: add business and operations consoles"
```

---

### Task 7: Add real-gateway smoke and three-turn acceptance

**Files:**
- Create: `tests/validation_runtime/test_real_gateway_smoke.py`
- Create: `scripts/validate_real_model.py`
- Create: `docs/operations/real-model-validation.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: running validation runtime and `MODEL_API_KEY`.
- Produces: opt-in external smoke test, executable acceptance script, and exact operator guide.

- [ ] **Step 1: Write opt-in real test**

```python
pytestmark = [
    pytest.mark.real_gateway,
    pytest.mark.skipif(
        not os.getenv("MODEL_API_KEY"), reason="MODEL_API_KEY not configured"
    ),
]


def test_real_gateway_completes_one_turn():
    with TestClient(create_app()) as client:
        instance = client.post(
            "/api/v1/runtime/ops/agents/shipping-analyst/instances"
        ).json()
        session = client.post(
            "/api/v1/runtime/agents/shipping-analyst/sessions"
        ).json()
        answer = client.post(
            f"/api/v1/runtime/sessions/{session['session_id']}/executions",
            json={"message": "请用一句话说明你能做什么。"},
        ).json()
        assert instance["model_alias"] == "gpt-5.6-sol"
        assert answer["status"] == "SUCCEEDED"
        assert answer["message"].strip()
```

- [ ] **Step 2: Verify missing-key behavior**

Run: `uv run pytest tests/validation_runtime/test_real_gateway_smoke.py -q`

Expected without a key: one skipped test and no network call.

- [ ] **Step 3: Implement three-turn acceptance script**

`scripts/validate_real_model.py` accepts `--base-url`, default `http://127.0.0.1:8000`, then deploys through operations API, creates through business API, and sends:

1. `请记住，我叫 Herry，负责散运业务。`
2. `我叫什么名字，负责什么业务？`
3. `分析航运经营收入时应该关注哪些维度？没有数据的地方不要编造。`

Assert turn two contains `Herry` and `散运`; turn three contains `结论`, `依据`, `建议`; all business payloads pass the forbidden-key scan; operations proves `thread_id == session_id` and one instance/digest/model/Skill; serialized output does not contain the key value.

- [ ] **Step 4: Document startup and run commands**

```bash
uv sync
MODEL_API_KEY='<set locally>' uv run uvicorn apps.validation_runtime.main:app --host 127.0.0.1 --port 8000
uv run python scripts/validate_real_model.py
```

Document `/` as business and `/ops` as operations. State restart loses Sessions and no Demo fallback exists.

- [ ] **Step 5: Pass checks and commit**

```bash
uv run pytest tests/validation_runtime/test_real_gateway_smoke.py tests/validation_runtime/test_console.py -q
uv run python -m compileall -q apps scripts/validate_real_model.py
git add tests/validation_runtime/test_real_gateway_smoke.py scripts/validate_real_model.py docs/operations/real-model-validation.md README.md
git commit -m "test: add real model validation flow"
```

---

### Task 8: Run release gates and record exact evidence

**Files:**
- Create: `docs/operations/real-model-validation-result.md`

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces: deterministic release evidence and a separately recorded external-gateway result.

- [ ] **Step 1: Run Python deterministic tests**

```bash
uv run pytest tests/validation_runtime -m "not real_gateway" -q
```

Expected: all asset, registry, routing, Session, Execution, API, and console tests pass.

- [ ] **Step 2: Run Node regression and static checks**

```bash
npm test
npm run check
uv run python -m compileall -q apps scripts/validate_real_model.py
uv lock --check
git diff --check
```

Expected: Node tests and checks pass, Python compiles, lock is current, and diff has no whitespace errors.

- [ ] **Step 3: Run real gateway gate only with credential**

```bash
uv run pytest tests/validation_runtime/test_real_gateway_smoke.py -m real_gateway -q
uv run python scripts/validate_real_model.py
```

Expected with configured key: smoke and three-turn script pass. Without a key record `NOT RUN — MODEL_API_KEY not configured`; never record PASS.

- [ ] **Step 4: Record evidence**

Record commit SHA, Python/uv/deepagents versions, deterministic and Node results, external result, Agent digest, business forbidden-field scan, and secret scan in `real-model-validation-result.md`.

- [ ] **Step 5: Commit evidence**

```bash
git add docs/operations/real-model-validation-result.md
git commit -m "docs: record runtime validation evidence"
```

## Definition of Done

- Static Agent, Skill, and Gateway load deterministically with stable digest.
- Operations deployment returns an idempotent active Agent Instance using `gpt-5.6-sol`.
- Business discovery and Session creation never expose runtime implementation fields.
- Router binds logical Agent to one active instance; Session retains instance/digest.
- `session_id == thread_id`; multi-turn continuity, Session isolation, and same-Session serialization pass.
- Deep Agents 0.7.7 executes with `InMemorySaver`, no tools, and no subagents.
- Business and operations APIs/pages pass recursive forbidden-field tests.
- Real gateway returns a non-empty answer when `MODEL_API_KEY` is configured.
- Three-turn conversation proves memory continuity and Skill response structure.
- Python tests, Node regressions, syntax, lock, whitespace, and secret checks pass.
