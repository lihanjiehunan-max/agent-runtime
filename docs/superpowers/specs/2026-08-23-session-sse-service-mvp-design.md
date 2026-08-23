# Session SSE Service MVP Design

**Date:** 2026-08-23  
**Status:** Proposed for final review  
**Supersedes for the active implementation:** the API, UI, event-store, and release scope in `2026-08-23-real-model-vertical-slice-design.md`; completed static-asset and Agent Instance work remains reusable.

## 1. Goal

Deliver the smallest retained service that proves four behaviors against the configured real model gateway:

1. an operator explicitly deploys the static `shipping-analyst:0.1.0` Agent;
2. an external business client creates a Session through logical Agent ID `shipping-analyst`;
3. the client sends multiple turns to that Session;
4. Chat output is forwarded as true Server-Sent Events while the upstream Deep Agents graph is still generating it.

The process listens on `0.0.0.0`, is callable by external clients, and uses one low-cost Bearer token configured through the environment.

## 2. Scope

### Included

- Python 3.12, FastAPI, Deep Agents 0.7.7, LangGraph in-memory checkpointing.
- Existing static Model Gateway, Skill, Agent manifest, asset loader, Agent Instance registry, and logical router.
- Explicit idempotent operations deployment.
- In-memory stateful Sessions with `session_id == thread_id`.
- One true streaming Chat endpoint using SSE.
- A minimal health endpoint and a documented `uvicorn` command for external binding.
- Deterministic tests plus an opt-in real-gateway smoke test.

### Excluded

- Database, Redis, object storage, durable restart recovery, background workers, queues, retries, resumable SSE, WebSocket, tools, MCP, Shell, filesystem tools, product subagents, frontend pages, complex event storage, traces, metrics dashboards, RBAC, multi-tenant policy, and automatic demo responses.
- OpenAI-compatible `/v1/chat/completions`; this MVP exposes a Session-oriented API only.
- Additional security hardening beyond Bearer authentication, secret non-disclosure, existing asset-root containment, basic input validation, and timeouts.

Process restart loses Agent Instances, Sessions, and conversation checkpoints. That limitation is explicit and acceptable for this validation slice.

## 3. Architecture

One FastAPI process owns:

- `AssetLoader`: loads the one static gateway, Skill, and Agent manifest.
- `ModelFactory`: builds the configured `ChatOpenAI` client from `MODEL_API_KEY`.
- `AgentFactory`: compiles one Deep Agents graph with the static Skill prompt, no tools, and one `InMemorySaver`.
- `AgentInstanceRegistry`: retains the compiled graph internally and exposes public instance metadata only from operations routes.
- `AgentRouter`: resolves logical `agent_id` to exactly one active instance.
- `SessionManager`: stores in-memory Session metadata and one execution lock per Session.
- `StreamingChatService`: validates the Session, invokes the bound graph with `thread_id=session_id`, and converts model message chunks to SSE events.

There is no worker boundary or internal event bus. The HTTP request coroutine drives the model stream directly.

## 4. Configuration and Authentication

Required environment variables:

- `MODEL_API_KEY`: credential sent only to the configured model gateway.
- `SERVICE_API_KEY`: Bearer token accepted by all `/api/v1/runtime/*` endpoints.

Missing `SERVICE_API_KEY` fails application startup. Missing `MODEL_API_KEY` leaves the process live for health checks but makes explicit Agent deployment fail with `503 MODEL_API_KEY_MISSING`.

Requests send:

```http
Authorization: Bearer <SERVICE_API_KEY>
```

Missing or invalid credentials return `401` with a stable `AUTHENTICATION_REQUIRED` error. The service compares the token without logging it. Neither environment value appears in responses, SSE payloads, logs, Agent digests, or documentation examples.

This MVP uses one token for both business and operations routes. Route and DTO separation prevents accidental instance metadata in business responses, but it does not implement personnel roles or RBAC.

`GET /healthz` is unauthenticated and returns only `{ "status": "ok" }` so an external load balancer can perform a liveness check without learning gateway or runtime details.

## 5. HTTP API

All JSON request and non-stream response bodies use UTF-8.

### 5.1 Deploy Agent — operations

`POST /api/v1/runtime/ops/agents/{agent_id}/deploy`

The service loads the static Agent assets, verifies `MODEL_API_KEY` readiness, builds the graph, and deploys it into the in-memory registry. Repeating the same package digest is idempotent.

Success: `200 OK`

```json
{
  "agent_instance_id": "ain_...",
  "agent_id": "shipping-analyst",
  "version": "0.1.0",
  "package_digest": "...",
  "model_alias": "gpt-5.6-sol",
  "skill_ids": ["shipping-operations-analyst"],
  "status": "ACTIVE"
}
```

The instance ID is an operations resource and is never accepted by Session or Chat business endpoints.

### 5.2 List Agent Instances — operations

`GET /api/v1/runtime/ops/agent-instances`

Returns the in-memory public instance records needed for deployment verification. It never returns the compiled graph, gateway URL, or credentials.

### 5.3 Create Session — business

`POST /api/v1/runtime/agents/{agent_id}/sessions`

The router resolves exactly one active Agent Instance and binds it internally for the Session lifetime.

Success: `201 Created`

```json
{
  "session_id": "ses_...",
  "agent_id": "shipping-analyst",
  "status": "IDLE",
  "turn_count": 0
}
```

The response excludes Agent Instance ID, thread ID, model, Skill, gateway, digest, and graph details.

### 5.4 Stream Chat — business

`POST /api/v1/runtime/sessions/{session_id}/chat`

Request:

```json
{
  "message": "请记住，我叫 Herry，负责散运业务。"
}
```

`message` must be a non-empty string after trimming. The service returns `Content-Type: text/event-stream`, `Cache-Control: no-cache`, and `X-Accel-Buffering: no`.

The only SSE event types are:

```text
event: delta
data: {"content":"部分文本"}

event: done
data: {"execution_id":"exe_...","turn_count":1}

event: error
data: {"code":"MODEL_GATEWAY_ERROR","message":"Model gateway request failed"}
```

Every event is terminated by a blank line. JSON is encoded on one `data:` line. `delta.content` contains only assistant text. `done` is emitted exactly once after successful upstream completion. `error` is terminal and is never followed by `done`.

The implementation calls the compiled graph's asynchronous stream with message stream mode and:

```python
input = {"messages": [{"role": "user", "content": message}]}
config = {"configurable": {"thread_id": session_id}}
```

Only assistant message chunks are forwarded. Graph metadata, tool messages, instance data, model names, prompts, and provider payloads are not emitted.

One Session permits one active Chat stream. A concurrent request for the same Session returns `409 SESSION_BUSY`; different Sessions may stream concurrently. On client disconnect, the generator stops consuming and cancels the in-flight invocation on a best-effort basis. A successful stream increments `turn_count`; a failed or disconnected stream does not.

## 6. Error Contract

Authentication, input, routing, Session lookup, and concurrency errors detected before the SSE response starts use JSON:

```json
{
  "error": {
    "code": "SESSION_NOT_FOUND",
    "message": "Session was not found"
  }
}
```

Stable mappings:

- `401 AUTHENTICATION_REQUIRED`: missing or invalid Bearer token.
- `404 AGENT_NOT_FOUND`: unknown static logical Agent.
- `404 SESSION_NOT_FOUND`: unknown Session.
- `409 AGENT_INSTANCE_UNAVAILABLE`: Session requested before deployment.
- `409 AGENT_ROUTING_AMBIGUOUS`: more than one active instance exists.
- `409 SESSION_BUSY`: another stream is active for the same Session.
- `422 INVALID_MESSAGE`: blank or invalid Chat input.
- `422 AGENT_ASSET_INVALID`: invalid static assets.
- `503 MODEL_API_KEY_MISSING`: deployment requested without the model credential.

The Chat endpoint starts its SSE response after the pre-stream checks above. Gateway failure or the 120-second timeout is therefore represented by one terminal `MODEL_GATEWAY_ERROR` or `MODEL_GATEWAY_TIMEOUT` event because the HTTP status can no longer change. Error messages are normalized and never include exception representations, request headers, response bodies, prompts, or secret values.

## 7. Data and State

Each internal Session record stores:

- `session_id` and equal `thread_id`;
- logical `agent_id`;
- bound `agent_instance_id` and package digest;
- status (`IDLE`, `STREAMING`, or `CLOSED`);
- `turn_count` and timestamps.

The thread, Agent Instance, and digest binding fields never change; lifecycle fields are updated by the Session manager. Business DTOs are explicit allow-lists. The operations deployment DTO is separate and may expose the instance metadata listed in section 5.1.

The Deep Agents graph uses `InMemorySaver`; passing the same Session ID as LangGraph thread ID supplies multi-turn continuity. No message transcript is duplicated into a second application store.

## 8. Deployment and External Access

Run from the repository root:

```bash
MODEL_API_KEY='<configured locally>' \
SERVICE_API_KEY='<configured locally>' \
uv run uvicorn apps.validation_runtime.main:app \
  --host 0.0.0.0 --port 8000
```

Port `8000` must be exposed by the host, container runtime, or reverse proxy. TLS termination is expected at the external proxy/load balancer and is not implemented inside Uvicorn for this MVP.

## 9. Verification

Deterministic tests use a recording streaming graph and never contact the gateway. They verify:

- Bearer token required for every runtime API.
- Deployment is explicit and idempotent.
- Session creation fails before deployment and contains no instance/runtime fields afterward.
- SSE framing, headers, ordered `delta` chunks, exactly one `done`, and terminal `error` behavior.
- Same Session uses the same thread ID and preserves facts across turns.
- Same-Session concurrent streams return `SESSION_BUSY`; distinct Sessions can overlap.
- Disconnect/failure does not increment turn count or leave the Session busy.
- Business JSON and SSE recursively exclude instance, thread, digest, Skill, model, gateway, prompt, and secret fields.
- Existing asset and Agent Instance tests remain green.

An opt-in real-gateway smoke test runs only when both environment variables are configured. It:

1. deploys `shipping-analyst` through the operations endpoint;
2. creates a Session through the business endpoint;
3. streams the approved three turns;
4. asserts that turn two contains `Herry` and `散运`;
5. asserts that turn three contains `结论`, `依据`, and `建议`;
6. asserts at least one `delta` arrives before `done` for every turn;
7. scans all returned JSON/SSE text to ensure neither configured secret value appears.

Without credentials, external verification is recorded as `NOT RUN`; it is never reported as PASS.

## 10. Acceptance Criteria

- An external client with the correct Bearer token can deploy the static Agent, create a Session, and receive true SSE Chat output through port 8000.
- The client uses only logical Agent ID and Session ID; no business request or response contains an Agent Instance ID.
- Operations deployment returns the Agent Instance metadata needed for validation.
- `session_id == thread_id`, multi-turn memory works, and same-Session overlap is rejected predictably.
- All deterministic tests and existing Node regressions pass.
- The real-gateway three-turn smoke passes when credentials are configured, or is reported exactly as NOT RUN when they are absent.
