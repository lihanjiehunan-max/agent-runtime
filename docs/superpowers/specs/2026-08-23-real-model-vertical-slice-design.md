# Deep Agents Real-Model Vertical Slice Design

## 1. Purpose

This design defines the smallest retained implementation that proves the core Agent Runtime path against a real OpenAI-compatible model gateway:

1. load one static Skill;
2. load one static Model Gateway profile;
3. deploy one static Agent package as an operational Agent Instance;
4. let a business client create a Session through the logical Agent ID while the runtime binds it to that instance;
5. execute multiple conversational turns in the same Session;
6. expose logical Agent, Session, and Execution data to the business surface while exposing instance, Skill, model, digest, and routing evidence only to the operations surface.

The slice is deliberately smaller than the full Python Runtime MVP. It is a single-process technical validation, not the final production topology.

## 2. Decisions

### 2.1 Chosen approach

Build a Python 3.12 single-process vertical slice using FastAPI, `deepagents==0.7.7`, a LangGraph in-memory checkpointer, and a minimal browser console served by the same process.

The process contains the API, Deep Agents adapter, Session manager, synchronous Execution service, and static console. It calls a real external model gateway. Redis, PostgreSQL, MinIO, a separate worker, and React are deferred until the path is proven.

### 2.2 Why not extend the Node kernel

The current Node Kernel remains the safety and behavior reference. Extending it would validate a different runtime SDK from the approved Deep Agents Python direction and create code that would soon be replaced.

### 2.3 Why not build the full target topology now

The immediate question is whether a configured Agent can be deployed, given a Session, and used for stateful conversation. Infrastructure distribution does not affect that answer. Adding the full queue, persistence, object storage, and worker topology would slow the validation and obscure SDK or gateway failures.

## 3. Scope

### 3.1 Included

- Python 3.12 and `deepagents==0.7.7`.
- One static Skill file: `shipping-operations-analyst`.
- One static OpenAI-compatible Model Gateway profile.
- One static Agent manifest referencing that Skill and Model Gateway.
- Agent Instance deployment through a versioned operations endpoint.
- Separate business and operations API contracts, even though both are served by one process.
- In-memory Agent Instance, routing, and Session registries.
- `session_id == LangGraph thread_id`.
- Multiple synchronous Executions in one Session.
- Separate minimal browser pages for operations validation and business chat validation.
- Runtime status, structured errors, execution identifiers, and a bounded event timeline.
- Unit and API tests with a deterministic fake chat model.
- A separate opt-in smoke test and manual acceptance flow against the real gateway.

### 3.2 Excluded

- Redis, PostgreSQL, MinIO, Prometheus, and Grafana.
- Separate API and worker processes.
- Durable recovery after process restart.
- SSE token streaming and asynchronous tasks.
- MCP, Tool Gateway, tools, and tool calls.
- Dynamic Skill, model, or Agent editing.
- Agent Builder, package upload, and online asset management.
- Authentication, tenancy, RBAC, quotas, retries, and production secret management.
- Shell, filesystem, arbitrary network tools, code execution, and subagents.

## 4. Static runtime assets

The validation uses files rather than database-managed assets. “Static” means version-controlled and read-only at runtime; it does not mean credentials are committed.

### 4.1 Model Gateway profile

Path:

```text
runtime_assets/model_gateways/zero-api.json
```

Content contract:

```json
{
  "id": "zero-api-gpt-5.6-sol",
  "provider": "openai-compatible",
  "base_url": "https://token.zero-api.cc.cd/v1",
  "model": "gpt-5.6-sol",
  "api_mode": "chat_completions",
  "api_key_env": "MODEL_API_KEY",
  "connect_timeout_seconds": 10,
  "read_timeout_seconds": 120
}
```

The client sends OpenAI-compatible Chat Completions requests through the configured base URL. The API key is read only from `MODEL_API_KEY`; the profile, logs, API responses, and browser never contain the key.

### 4.2 Skill

Path:

```text
runtime_assets/skills/shipping-operations-analyst/SKILL.md
```

The Skill defines a narrow behavior that is easy to verify manually:

- act as a shipping operations analysis assistant;
- preserve facts supplied earlier in the same Session;
- distinguish known facts from assumptions;
- answer analysis questions in three sections: `结论`, `依据`, and `建议`;
- state explicitly when source data has not been provided;
- never claim to have queried a business system because this slice has no tools.

The Skill is loaded as controlled Agent instructions. It is not exposed as an unrestricted runtime plugin.

### 4.3 Agent manifest

Path:

```text
runtime_assets/agents/shipping-analyst/manifest.json
```

Content contract:

```json
{
  "schema_version": "1.0",
  "agent_id": "shipping-analyst",
  "version": "0.1.0",
  "runtime": "deepagents-python",
  "model_gateway_id": "zero-api-gpt-5.6-sol",
  "skills": ["shipping-operations-analyst"],
  "limits": {
    "execution_timeout_seconds": 120,
    "max_input_characters": 12000
  }
}
```

Agent Instance deployment computes a SHA-256 digest over the canonical manifest, referenced Skill bytes, Model Gateway profile without resolved secrets, and pinned runtime identity. That digest identifies the deployed configuration.

## 5. Architecture

```text
Business Console / Operations Console
      |
      | REST/JSON
      v
FastAPI Runtime
      |-- Static Asset Loader
      |-- Agent Instance Registry
      |-- Agent Router
      |-- Session Manager
      |-- Execution Service
      |-- Deep Agents Adapter
      |-- Event Recorder
      |
      +--> InMemorySaver
      +--> OpenAI-Compatible Model Gateway
```

### 5.1 Components

#### Static Asset Loader

Loads and validates the Model Gateway profile, Skill, and Agent manifest. It rejects missing references, invalid schemas, unsupported runtime names, unreadable Skill files, duplicate IDs, and missing `MODEL_API_KEY` readiness.

#### Agent Instance Registry

Creates an immutable in-memory Agent Instance from the static manifest. Repeated deployment of the same digest is idempotent and returns the existing instance. An instance includes the logical Agent ID, version, digest, model alias, Skill IDs, runtime status, and creation time. The instance is an operations resource and is never returned by a business API.

#### Agent Router

Resolves a stable logical `agent_id` from the business API to one active Agent Instance. The validation slice has exactly one active instance per logical Agent and therefore uses deterministic routing. The selected `agent_instance_id` is stored on the internal Session record and remains fixed for the Session lifetime. Unknown or inactive instances are never selected.

#### Session Manager

Creates in-memory Sessions from a logical Agent request after routing it to an active Agent Instance. It maps the Session ID one-to-one to the LangGraph thread ID and rejects unknown Agents, unavailable instances, unknown Sessions, and closed Sessions. Business Session DTOs omit the bound instance and other runtime implementation details.

#### Execution Service

Creates one Execution per conversational turn, invokes the deployed graph using the Session thread ID, records bounded events, and returns the final assistant message. The single-process slice serializes Executions per Session with an in-process lock.

#### Deep Agents Adapter

Builds the graph from the immutable Skill instructions and Model Gateway profile. It supplies the in-memory checkpointer and passes `configurable.thread_id=session_id` on every turn. It does not enable default Shell, filesystem, task, subagent, or network tools.

#### Event Recorder

Stores a bounded in-memory timeline for manual proof and API tests. It records identifiers and normalized phases, not hidden reasoning or authorization data.

#### Browser Console

Serves a business page at `/` and a separate operations page at `/ops`. The operations page deploys and inspects Agent Instances and runtime evidence. The business page selects only the logical Agent, creates a Session, and chats without receiving an Agent Instance ID, package digest, Skill ID, model alias, or internal event payload.

## 6. Domain model

### 6.1 AgentInstance

Required fields:

- `agent_instance_id`
- `agent_id`
- `agent_version`
- `package_digest`
- `runtime_type`
- `model_gateway_id`
- `model_alias`
- `skill_ids`
- `status`
- `created_at`

`AgentInstance` is visible to operations personnel only. Business users address the logical `agent_id` and never use an instance ID in requests or responses.

### 6.2 RuntimeSession

Required fields:

- `session_id`
- `thread_id`, equal to `session_id`
- `agent_id`
- internal `bound_agent_instance_id`
- `package_digest`
- `status`: `ACTIVE | EXECUTING | IDLE | CLOSED`
- `created_at`
- `last_execution_id`
- `turn_count`

The Session cannot change its bound Agent Instance or package digest in this slice. Its business representation includes `session_id`, `agent_id`, status, and turn count; it excludes `thread_id`, `bound_agent_instance_id`, package digest, model, and Skill data. The operations representation includes the full binding and runtime evidence.

### 6.3 RuntimeExecution

Required fields:

- `execution_id`
- `session_id`
- `agent_id`
- internal `executing_agent_instance_id`
- `trace_id`
- `status`: `ACCEPTED | RUNNING | SUCCEEDED | FAILED | TIMED_OUT`
- `input_message`
- `output_message`
- `started_at`
- `completed_at`
- `error`

### 6.4 RuntimeEvent

Required fields:

- `sequence`
- `event_type`
- `session_id`
- `execution_id`
- `trace_id`
- `agent_id`
- `agent_instance_id`
- `package_digest`
- `model_alias`
- `skill_ids`
- `timestamp`
- bounded `data`

Minimum event sequence:

```text
execution.accepted
execution.started
model.started
model.completed | model.failed
execution.succeeded | execution.failed | execution.timed_out
```

## 7. API contract and audience boundary

All runtime APIs use the target prefix `/api/v1/runtime`. Route and DTO separation is enforced now even though authentication and RBAC are deferred: business APIs never expose Agent Instances, while operations APIs expose the routing and runtime details required to deploy, diagnose, and manage them. In the production architecture, business routes require an invoke permission and `/ops/*` routes require a distinct runtime-operations permission; this validation slice proves the contract and data-minimization boundary, not identity enforcement.

### 7.1 Business API

#### `GET /api/v1/runtime/agents`

Returns logical Agents available to business clients. It exposes stable Agent ID, display name, and availability only. It excludes instance IDs, runtime type, package digest, model, Skill composition, and gateway configuration.

#### `POST /api/v1/runtime/agents/{agent_id}/sessions`

Creates a Session for the logical Agent. The router selects an active Agent Instance and stores the binding internally. The response returns a new business Session with `session_id`, `agent_id`, status, and turn count. It does not return `agent_instance_id` or `thread_id`.

#### `GET /api/v1/runtime/sessions/{session_id}`

Returns the business Session representation. Internal routing and runtime evidence are omitted.

#### `POST /api/v1/runtime/sessions/{session_id}/executions`

Request:

```json
{"message": "请记住，我叫 Herry，负责散运业务。"}
```

Runs one synchronous turn and returns:

```json
{
  "execution_id": "exe_...",
  "session_id": "ses_...",
  "agent_id": "shipping-analyst",
  "status": "SUCCEEDED",
  "message": "..."
}
```

The business response excludes Trace ID, Agent Instance ID, package digest, Skill IDs, model alias, and provider events.

#### `POST /api/v1/runtime/sessions/{session_id}/close`

Closes the Session. Later execution attempts return `SESSION_CLOSED`.

### 7.2 Operations API

#### `GET /api/v1/runtime/ops/status`

Returns process readiness, Deep Agents version, configured logical Agents, gateway ID/model, whether the API key is configured, Agent Instance count, active Session count, and execution count. It never returns the key.

#### `POST /api/v1/runtime/ops/agents/{agent_id}/instances`

Loads the static manifest and references, validates gateway readiness, builds the graph, computes the digest, and returns the immutable Agent Instance.

#### `GET /api/v1/runtime/ops/agent-instances`

Returns all Agent Instances with logical Agent, version, status, runtime, model alias, Skill IDs, and package digest.

#### `GET /api/v1/runtime/ops/agent-instances/{agent_instance_id}`

Returns one Agent Instance and its runtime status.

#### `GET /api/v1/runtime/ops/agent-instances/{agent_instance_id}/sessions`

Returns Sessions currently bound to the instance for operational routing, load inspection, and diagnosis.

#### `GET /api/v1/runtime/ops/sessions/{session_id}`

Returns the operations Session representation, including `thread_id`, bound Agent Instance, package digest, model alias, Skill IDs, status, and turn count.

#### `GET /api/v1/runtime/ops/sessions/{session_id}/events`

Returns the bounded ordered internal event timeline for that Session.

#### `GET /api/v1/runtime/ops/executions/{execution_id}`

Returns the operations Execution representation, including Trace ID, executing Agent Instance, package digest, model alias, timing, status, and normalized error.

### 7.3 Internal ownership rule

The persisted relationship is:

```text
logical Agent
    -> active Agent Instance selected by router
    -> Session.bound_agent_instance_id
    -> Execution.executing_agent_instance_id
```

Business clients cannot choose or override the instance. Operations clients may inspect the binding but cannot mutate it through the Session API. This prevents a caller from supplying inconsistent Agent and Session identifiers.

## 8. Execution flow

1. Operator starts the process with `MODEL_API_KEY` set.
2. The operations page checks runtime status and displays the gateway alias, model, SDK version, and readiness.
3. Operator deploys `shipping-analyst`; the runtime creates an Agent Instance and marks it active for that logical Agent.
4. Runtime loads the manifest, Skill, and Model Gateway profile; validates references; computes the digest; and builds the graph.
5. A business client selects the logical `shipping-analyst` Agent and creates a Session without seeing or submitting an instance ID.
6. The router selects the active instance; Runtime stores the internal binding and uses the new Session ID as the LangGraph thread ID.
7. The business client sends a message using only the Session ID.
8. Runtime resolves the Session binding, serializes access, creates an Execution and trace, and invokes Deep Agents with the same thread ID and bound instance.
9. The model client calls the configured real gateway using the server-side key.
10. Runtime records normalized events, returns only business-safe fields to the caller, and makes full evidence available through operations APIs.
11. Later turns repeat with the same Session/thread/instance/digest, allowing the checkpointer to provide conversation continuity.

## 9. Console behavior

The process serves two separate pages so the business experience never renders operational data.

The operations page at `/ops` provides:

- runtime readiness and configured model display;
- a `部署 Agent` button;
- Agent Instance ID, Agent version, Skill ID, model alias, and digest display;
- instance-bound Session list;
- an internal Session/Execution inspector with Trace ID and normalized event timeline.

The business page at `/` provides:

- a logical Agent selector;
- a `创建会话` button;
- Session ID, Agent ID, status, and turn count display;
- a conversation panel and message input;
- Execution ID for every assistant response;
- clear business-safe error cards for Agent availability, Session, and execution failures.

The business page does not display or receive Agent Instance ID, thread ID, Trace ID, package digest, Skill IDs, model alias, gateway details, or internal events. The operations page may display them. Buttons are enabled in sequence, and the chat input is disabled until a Session exists. Neither page contains an automatic demo fallback; all successful answers in this slice must come from the configured real gateway.

## 10. Error handling

Stable error codes:

- `MODEL_API_KEY_MISSING`
- `MODEL_GATEWAY_UNREACHABLE`
- `MODEL_GATEWAY_AUTH_FAILED`
- `MODEL_GATEWAY_TIMEOUT`
- `MODEL_GATEWAY_BAD_RESPONSE`
- `AGENT_NOT_FOUND`
- `AGENT_ASSET_INVALID`
- `AGENT_DEPLOYMENT_FAILED`
- `AGENT_INSTANCE_NOT_FOUND`
- `AGENT_INSTANCE_UNAVAILABLE`
- `SESSION_NOT_FOUND`
- `SESSION_CLOSED`
- `SESSION_BUSY`
- `INPUT_TOO_LARGE`
- `EXECUTION_TIMED_OUT`
- `EXECUTION_FAILED`

External provider response bodies are bounded and sanitized before they are included in server logs or API errors. Authorization headers, environment values, and raw credentials are never returned. A gateway failure does not create a fake assistant response.

## 11. Testing strategy

### 11.1 Automated tests

Automated tests do not depend on the external gateway. They inject a deterministic `BaseChatModel` fake while exercising the same Agent Instance, routing, Session, Execution, and checkpointer boundaries.

Required coverage:

- static asset schema and reference validation;
- deterministic digest computation;
- Agent Instance deployment idempotency;
- deterministic logical Agent-to-instance routing;
- business APIs and DTOs never exposing Agent Instance, thread, trace, digest, Skill, model, or gateway fields;
- operations APIs exposing Agent Instance and Session routing evidence;
- missing API key readiness without secret exposure;
- Session creation through logical Agent ID and internal instance/digest pinning;
- two or more turns sharing one Session/thread;
- different Sessions not sharing conversation state;
- same-Session serialization;
- closed Session rejection;
- bounded ordered event timeline;
- structured gateway timeout/auth/error mapping;
- console smoke and API contract tests.

### 11.2 Real-gateway smoke test

An opt-in smoke test runs only when `MODEL_API_KEY` is set. It deploys an Agent Instance through the operations API, creates a Session through the logical Agent business API, submits a short message, and asserts a non-empty business answer plus a successful model event visible through the operations API. It does not assert exact wording.

## 12. Manual acceptance

The vertical slice passes only when all steps succeed against the real gateway:

1. The operations status reports `zero-api-gpt-5.6-sol`, model `gpt-5.6-sol`, and gateway readiness.
2. Deploying `shipping-analyst` through the operations page returns an Agent Instance ID, version `0.1.0`, Skill ID, and stable digest.
3. The business page lists logical Agent `shipping-analyst` without returning the Agent Instance, Skill, model, or digest.
4. Creating a Session through the logical Agent returns a Session ID without returning the Agent Instance or thread ID; the operations inspector proves that the internal thread ID equals the Session ID and that the Session is pinned to the deployed instance and digest.
5. First turn: `请记住，我叫 Herry，负责散运业务。`
6. Second turn: `我叫什么名字，负责什么业务？`
7. The second answer identifies `Herry` and `散运业务`, proving same-thread continuity.
8. Third turn: `分析航运经营收入时应该关注哪些维度？没有数据的地方不要编造。`
9. The third answer follows the Skill's `结论／依据／建议` structure and identifies missing source data rather than claiming a system query.
10. Business responses show the same Session and logical Agent with distinct Execution IDs, but never expose Agent Instance, thread, trace, digest, Skill, model, or gateway data.
11. The operations inspector proves all turns used the same Session, thread, Agent Instance, digest, Skill, and model, with distinct Execution and Trace IDs.
12. The operations event timeline contains successful model and execution terminal events for every turn and contains no API key.

Restart recovery is explicitly not an acceptance criterion because this slice uses in-memory state.

## 13. Proposed repository layout

```text
apps/
└── validation_runtime/
    ├── api.py
    ├── config.py
    ├── main.py
    ├── static/
    │   ├── business.js
    │   ├── index.html
    │   ├── ops.html
    │   ├── ops.js
    │   └── styles.css
    └── services/
        ├── asset_loader.py
        ├── agent_instance_registry.py
        ├── agent_router.py
        ├── event_recorder.py
        ├── execution_service.py
        ├── model_factory.py
        └── session_manager.py

runtime_assets/
├── agents/shipping-analyst/manifest.json
├── model_gateways/zero-api.json
└── skills/shipping-operations-analyst/SKILL.md

tests/validation_runtime/
├── test_assets.py
├── test_agent_instances.py
├── test_agent_routing.py
├── test_sessions.py
├── test_executions.py
├── test_api.py
├── test_console.py
└── test_real_gateway_smoke.py
```

Project-level Python dependency and command configuration is added without deleting or rewriting the current Node Kernel. Node remains runnable as the archived baseline while the Python validation process uses a separate port and entrypoint.

## 14. Upgrade path after validation

If the acceptance flow passes, the retained interfaces migrate toward the target Runtime MVP in this order:

1. replace in-memory registries and event storage with PostgreSQL;
2. replace `InMemorySaver` with `AsyncPostgresSaver`;
3. introduce Redis Session locks and execution epoch fencing;
4. split synchronous execution into API and worker processes;
5. add resumable SSE and cooperative cancellation;
6. add Tool Gateway and the read-only `query_metric` tool;
7. replace the static console with the target React operational console.

The static asset contracts, logical Agent business contract, operations-only Agent Instance contract, Session/thread mapping, Execution API shape, event names, and error codes should be retained unless the validation reveals a concrete incompatibility.
