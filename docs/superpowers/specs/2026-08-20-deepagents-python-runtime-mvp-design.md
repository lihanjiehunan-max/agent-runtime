# Deep Agents Python Agent Runtime MVP Target Design

## 1. Purpose

This design upgrades the current Enterprise Agent Runtime proof of concept into an MVP centered on four runtime capabilities:

1. deterministic Agent Package loading;
2. first-class Session management;
3. controlled single-turn, streaming, and asynchronous execution;
4. complete runtime observation across package, session, model, tool, checkpoint, and result.

The source requirement is the user-provided document `粘贴的 markdown (1)。md`, titled “基于 LangChain Deep Agents 的 Agent Runtime MVP 落地计划”.

## 2. Current baseline

### 2.1 Persisted code baseline

The persisted project archive is Enterprise Agent Runtime Kernel 0.2:

- Node.js 24 modular monolith;
- SQLite persistence;
- Run lifecycle and immutable event sequence;
- Worker lease and execution epoch fencing;
- checkpoint integrity envelope and recovery;
- tenant-scoped immutable Context references and revocation;
- versioned Tool Contracts;
- Tool Effect Ledger and durable Outbox;
- authenticated principal boundary;
- SSE event replay;
- 92 automated tests, all currently passing.

### 2.2 Previously completed but not persisted as a source archive

Runtime 0.3 previously added:

- `deepagents.js` and OpenAI-compatible model execution;
- `POST /api/runs/:runId/execute`;
- `GET /api/agent/status`;
- normalized Agent stream events;
- a least-privilege `read_context` tool profile;
- a chat test console;
- 114 passing tests.

The 0.3 workspace copy was removed by workspace maintenance and must not be treated as a restorable code baseline. Its behavior remains a compatibility requirement.

## 3. Gap summary

| Capability | Current project | Target MVP | Decision |
|---|---|---|---|
| Agent definition | hard-coded adapter/config | versioned Agent Package | build new Package Manager |
| Package reproducibility | no package digest pinning | `agent_id + version + digest` | add registry metadata, checksum validation, cache |
| Conversation object | Run only | Session with many Executions | introduce Session as first-class aggregate |
| Multi-turn state | no durable multi-turn thread | LangGraph `thread_id` + Checkpointer | use `AsyncPostgresSaver`; do not create another mutable chat history |
| Agent SDK | Node `deepagents.js` | Python `deepagents==0.7.7` | replace execution adapter with Python implementation |
| Persistence | SQLite | PostgreSQL + Redis + MinIO | migrate metadata model; retain SQLite only as archived baseline |
| Execution modes | asynchronous Run execution and SSE events | sync, SSE stream, simplified async task | implement one Execution Manager and three delivery modes |
| Cancellation | Run state controls only | cooperative timeout and cancel | add cancellation token and worker cancellation checks |
| Model integration | environment-based OpenAI-compatible client | Model Gateway with usage/error normalization | retain server-side secret boundary, add metrics and standard errors |
| Tools | built-ins and `read_context` | allowlisted `query_metric` through Tool Gateway | preserve least privilege and authenticated identity propagation |
| Observability | immutable Run events | normalized Runtime/Deep Agent events, Trace, metrics, dashboard | introduce event schema, projection, Prometheus metrics |
| Console | kernel scenario console and chat demo | Agent, Session, conversation, trace, dashboard | rebuild as React/TypeScript control and test console |
| Recovery | custom checkpoint replay with fencing | LangGraph checkpoint continuation after worker restart | keep LangGraph as state authority; retain audit hash and fencing around enterprise writes |
| Deployment | one local Node process | API, worker, console plus infrastructure | three application units in one repository |

## 4. Architecture decision

### 4.1 Alternatives considered

**A. Continue extending Node 0.3.** This preserves code reuse, but conflicts with the approved Python Deep Agents target and would require recreating Python-specific Session, backend, and checkpointer behavior in JavaScript.

**B. Permanently retain Node Kernel and add a Python Agent worker.** This preserves most current code, but creates a long-lived cross-language protocol, duplicate lifecycle concepts, and two operational stacks before the MVP has proven value.

**C. Phased replacement with a Python runtime, using Node as a behavioral reference. Recommended.** The new runtime is Python end to end for API and worker. The Node baseline is frozen, its critical safety invariants are ported as tests and contracts, and the console switches only after the Python vertical slice passes parity gates.

### 4.2 Target topology

```text
Runtime Console (React/TypeScript)
        |
        | REST + SSE
        v
agent-runtime-api (FastAPI)
        |-- Package API
        |-- Session API
        |-- Execution/Task API
        |-- Trace Query API
        |
        | Redis Streams: commands, live events, cancel signals
        v
deepagent-runtime-worker (Python 3.12)
        |-- Package Loader / Agent Factory
        |-- Deep Agents 0.7.7 Adapter
        |-- AsyncPostgresSaver
        |-- Model Gateway Client
        |-- Tool Gateway Client
        |-- Event Normalizer
        |
        +--> PostgreSQL: metadata, events, traces, LangGraph checkpoints
        +--> Redis: queue, session lock, cancellation, live stream
        +--> MinIO: package bytes, large payloads, final artifacts
        +--> Prometheus: runtime metrics
```

## 5. Non-regression decisions

The Python migration must not remove the following safety properties already proven by the Node baseline:

1. Tenant and actor identity come from an authenticated principal, never from request JSON.
2. A Session permits at most one active Execution.
3. Redis lock ownership is not sufficient by itself. Every worker write carries `execution_epoch`, and PostgreSQL rejects stale epochs.
4. LangGraph Checkpointer is the authoritative mutable graph state. Runtime metadata stores checkpoint identifiers and integrity evidence, not a second mutable message history.
5. Package digest, agent version, model reference, tool versions, tenant, Session, and Execution identifiers are present on every trace.
6. Deep Agents default Shell, unrestricted filesystem, network, and subagent tools are disabled in the MVP.
7. Tool permission is enforced by Tool Gateway. The model cannot expand its own allowlist.
8. Large model/tool payloads are replaced by MinIO references before durable event storage.
9. Token deltas may be sent live but are durably aggregated into bounded message chunks.
10. Tool Effect Ledger and Outbox contracts are retained for later write tools; the MVP `query_metric` tool is read-only.

## 6. Domain model

### 6.1 AgentPackage

Key: `(tenant_scope, agent_id, version)`; immutable identity: `digest`.

Required fields: schema version, runtime type, SDK version constraint, package location, status, timestamps, manifest JSON, and checksum evidence.

### 6.2 RuntimeSession

A Session binds tenant, user, `agent_id`, package version, and package digest. It maps one-to-one to a LangGraph `thread_id`, supports many Executions, and cannot change package digest.

Required concurrency fields: `revision`, `execution_epoch`, `active_execution_id`, `last_checkpoint_id`, and `last_event_sequence`.

### 6.3 RuntimeExecution

An Execution is one turn. Required fields include tenant, Session, request, trace, mode, status, execution epoch, timing, model/tool counters, tokens, error, and result reference.

### 6.4 RuntimeEvent

Events are append-only and ordered per Execution and Session. Every event includes tenant, trace/span relationship, Session, Execution, package identity, worker identity, SDK version, type, phase, duration, bounded payload, and optional payload reference.

## 7. Execution flow

1. API authenticates the principal and validates the Session.
2. API creates an Execution with a new trace ID.
3. API increments and binds `execution_epoch` using PostgreSQL CAS.
4. Command is appended to a Redis Stream.
5. Worker claims the command and acquires/renews `lock:session:{session_id}`.
6. Worker resolves the pinned package digest and obtains a digest-cached graph definition.
7. Worker invokes Deep Agents with `thread_id=session_id` and execution metadata.
8. Stream events are normalized, persisted in bounded form, and published live.
9. Tool calls pass through Tool Gateway with authenticated identity and package allowlist.
10. Completion updates Execution and Session using epoch CAS; stale workers are fenced.
11. API SSE replays durable events and then tails the live Redis stream.

## 8. API contract

The new versioned API prefix is `/api/v1/runtime`.

- `POST /agents/load`
- `GET /agents`
- `POST /sessions`
- `GET /sessions`
- `GET /sessions/{session_id}`
- `POST /sessions/{session_id}/close`
- `POST /sessions/{session_id}/executions`
- `POST /sessions/{session_id}/executions/stream`
- `POST /sessions/{session_id}/tasks`
- `GET /executions/{execution_id}`
- `POST /executions/{execution_id}/cancel`
- `GET /sessions/{session_id}/events?after_sequence=N`
- `GET /executions/{execution_id}/trace`
- `GET /runtime/status`

The existing `/api/runs` endpoints remain available only in the frozen compatibility build during migration. New console code does not depend on them.

## 9. Scope

### Included

- one package: `agent-metric-query:0.1.0`;
- one OpenAI-compatible model gateway;
- one read-only `query_metric` tool;
- local and MinIO package sources;
- PostgreSQL checkpointer and metadata;
- Redis command/event transport, lock, and cancellation;
- sync, SSE stream, and simplified async task execution;
- trace timeline and runtime dashboard;
- 30-50 concurrent Session test;
- worker restart and checkpoint continuation test.

### Excluded

- online Agent Builder and full Asset Center;
- complex intent router;
- subagent teams and dynamic subagent creation;
- runtime plugin installation;
- long-term personal memory across Sessions;
- arbitrary Shell, code execution, filesystem, database, or network access;
- write tools and automatic compensation;
- full multi-region disaster recovery;
- multi-runtime scheduling;
- automated evaluation and optimization;
- production budget governance and HITL.

## 10. Acceptance gates

The MVP is accepted only when all six scenarios pass:

1. Load `agent-metric-query:0.1.0`, verify checksums and digest, and reject tampering.
2. Execute three turns in one Session: “本月散运营业收入是多少？” → “同比呢？” → “按航线拆开看。”
3. Prove all three turns use the same Session, thread ID, package digest, and tenant.
4. Restart the worker and complete a fourth turn from the persisted checkpoint.
5. Observe model and real `query_metric` tool events in SSE and reconstruct the full trace.
6. Sustain 30-50 concurrent Sessions while meeting the agreed performance baseline.

Performance targets:

- package cache hit rate at least 95%;
- platform-added latency P95 at most 300 ms, excluding model/tool latency;
- cached Agent Definition lookup P95 at most 100 ms;
- cancellation effective within 2 seconds;
- critical trace coverage 100%;
- short-task success rate at least 95%.

## 11. Delivery strategy

The migration uses a strangler sequence:

1. freeze and preserve the Node baseline;
2. prove the Python Deep Agents vertical slice;
3. implement Package and Session foundations;
4. add controlled execution and observation;
5. switch the console to the versioned API;
6. run parity and acceptance tests;
7. archive the Node runtime as a reference, not a production service.
