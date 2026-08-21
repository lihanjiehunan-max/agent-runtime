# Task 6 Report: Deep Agents 0.7.7 Agent Factory and Model Gateway

## Status

Implementation complete in commit `84aaf907ad7da4613e4a04bf9c974af80386300e`
(`feat: add pinned Deep Agents factory and model gateway`).
Task 6 adds a digest-cached, least-privilege Deep Agents factory and a server-configured
OpenAI-compatible Model Gateway client. No push was performed, no dependency metadata changed,
and no Tool Gateway or production `query_metric` implementation was added.

## Files

- `packages/deepagents_adapter/factory.py`
- `packages/deepagents_adapter/profile.py`
- `packages/deepagents_adapter/version.py`
- `packages/model_gateway/client.py`
- `tests/contract/test_deepagents_077.py`
- `tests/unit/test_agent_factory.py`
- `.superpowers/sdd/2026-08-20-deepagents-python-runtime-mvp/task-6-report.md`

## Implementation

### Pinned SDK and Event Streaming v3

- `DEEPAGENTS_VERSION` is pinned to `0.7.7`, with a runtime assertion before graph construction.
- The contract test verifies the installed distribution version and the public
  `create_deep_agent` `backend` and `checkpointer` parameters.
- A real graph built by `deepagents==0.7.7` runs one `query_metric` tool turn against a loopback
  OpenAI-compatible HTTP stub. The first model call requests the tool, the test tool returns
  `revenue=42`, and the second model call returns `Revenue is 42.`. No external model traffic is
  used.
- The contract exercises the v3 `messages`, per-message `tool_calls`, `values`, and final `output`
  surfaces used by the later adapter/event normalizer.

### Least-privilege factory

- `AgentFactory` receives immutable `LoadedPackage` values plus explicit model/tool mappings,
  checkpointer, and backend.
- `AgentDefinition` is a frozen two-field definition containing only package digest and compiled
  graph. It has no principal, Session, request message, or thread state.
- Definitions are cached in-process by authoritative package digest only. Package map keys are
  checked against `LoadedPackage.reference.digest` before build.
- The package's logical model reference resolves only through the supplied server-side model map.
- Tools are selected in package allowlist order. An unregistered allowlisted tool fails closed,
  and tools absent from the allowlist are never passed to Deep Agents.
- The exact-model Deep Agents harness profile disables the default general-purpose subagent and
  excludes filesystem, shell, todo, synchronous task, and asynchronous task tools. A captured real
  model request contains only `query_metric`; the registered `unregistered_network` test tool and
  all Deep Agents built-ins are absent.
- Graph construction uses the supplied `StateBackend`, checkpointer, resolved model, immutable
  system prompt, and exact tool tuple. Request principals and Session state remain future
  invocation metadata and are not factory inputs or cache members.

### Model Gateway

- `ModelGatewayConfig.from_env()` reads `MODEL_GATEWAY_BASE_URL`, `MODEL_GATEWAY_API_KEY`,
  `MODEL_GATEWAY_MODEL`, and optional timeout from server environment only. Direct construction is
  also supported for server settings/tests. The API key is excluded from representations.
- Request-time `api_key`, `authorization`, and `headers` overrides are rejected. Authorization is
  generated only from the server configuration.
- The client sends OpenAI-compatible `/chat/completions` requests with a generated request ID,
  parses assistant/tool-call responses, and serializes assistant/tool history for multi-call turns.
- Usage is normalized to LangChain `input_tokens`, `output_tokens`, and `total_tokens` metadata.
- Correlation and provider request IDs are returned in response metadata.
- Timeouts, transport failures, non-success status codes, malformed bodies, malformed tool calls,
  and malformed usage are normalized to stable `ModelGatewayError` categories. Logs contain only
  error class, correlation ID, and status; provider bodies, API keys, and authorization are never
  logged.
- The internally owned HTTP client uses `trust_env=False`, preventing ambient proxy variables from
  silently rerouting server-configured Model Gateway traffic. Tests may inject a bounded client.

## TDD evidence

Tests preceded each production behavior and were observed failing for the intended reason:

1. SDK contract: missing `packages.deepagents_adapter` import.
2. Factory: missing factory module.
3. Visible tools: the unprofiled real graph exposed `ls`, `read_file`, `write_file`, `edit_file`,
   `delete`, `glob`, `grep`, and `task` alongside `query_metric`.
4. Model Gateway: missing client module.
5. Timeout normalization: missing normalized error types.
6. Provider status normalization: raw `httpx.HTTPStatusError` escaped.
7. Server environment: missing `from_env()`.
8. Malformed success response: raw `ValueError` escaped.
9. Real tool turn: ambient proxy inheritance initially blocked loopback construction; after
   deterministic transport setup, the current parser completed only one model call because it did
   not yet convert provider tool calls.
10. Event Streaming v3: the graph completed the two-call tool turn, then the test established that
    v3 projections are caller-driven and must be subscribed together to retain both messages and
    values.

## Verification

### Final bounded Task 6 acceptance

```text
timeout 30s env UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task6 uv run pytest \
  tests/contract/test_deepagents_077.py tests/unit/test_agent_factory.py -q
```

Result: `9 passed in 2.03s`.

### Final bounded Ruff

```text
timeout 30s env UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task6 uv run ruff check \
  packages/deepagents_adapter/factory.py packages/deepagents_adapter/profile.py \
  packages/deepagents_adapter/version.py packages/model_gateway/client.py \
  tests/contract/test_deepagents_077.py tests/unit/test_agent_factory.py
```

Result: `All checks passed!`.

### Final bounded Pyright

```text
timeout 30s env UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task6 uv run pyright \
  packages/deepagents_adapter/factory.py packages/deepagents_adapter/profile.py \
  packages/deepagents_adapter/version.py packages/model_gateway/client.py \
  tests/contract/test_deepagents_077.py tests/unit/test_agent_factory.py
```

Result: `0 errors, 0 warnings, 0 informations`.

### Earlier full Python suite

Before the user's bounded-verification instruction, the full suite completed without waiting on
external services: `91 passed, 3 skipped, 1 warning in 4.47s`. Skips were the live MinIO test and
two live PostgreSQL tests because their environment variables were not configured. The warning was
the pre-existing Starlette `httpx` TestClient deprecation. After the final type-boundary refactor,
the user requested focused checks only; the final 9-test acceptance above re-proved all Task 6
runtime behavior.

## SDK compatibility nuance / concerns

- Deep Agents/LangGraph Event Streaming v3 is experimental and caller-driven in the installed
  stack. Draining `run.output` before subscribing to a projection leaves that later projection
  empty. The contract therefore uses `run.interleave("messages", "values")`, then reads `output`.
  This is concrete 0.7.7 compatibility behavior for Task 9's normalizer, not a hang or blocker.
- The harness profile registry is process-global in Deep Agents. Task 6 registers an idempotent,
  exact `provider:model` profile under a lock, avoiding provider-wide privilege changes.
- No live Model Gateway, MinIO, PostgreSQL, Redis, or Tool Gateway was contacted. The acceptance
  test uses loopback HTTP only and has no external-service dependency.
- The local test tool is intentionally test-only. Task 7 still owns Tool Gateway authorization,
  principal propagation, and the production `query_metric` implementation.

## Fix round 1

### Review findings addressed

- Deep Agents 0.7.7 resolves a frozen `HarnessProfile` during `create_deep_agent` and materializes
  its middleware into the compiled graph. Task 6 now also wraps the selected model with a
  graph-owned, immutable exact tool-name allowlist. Later process-global profile registrations
  therefore cannot expose newly injected middleware tools to either an existing graph or a graph
  rebuilt for the same provider/model key. The least-privilege profile is reapplied before every
  build so a conflicting registration cannot re-enable general-purpose subagent construction.
- Transport exceptions are converted to `ModelGatewayError` only after leaving the `except` block.
  The normalized error retains its stable category and correlation ID but has neither a raw
  `__cause__` nor `__context__`; traceback-visible text does not contain the provider exception's
  URL, authorization, headers, or secret-bearing message.
- Focused coverage now rejects request-time `api_key`, `authorization`, and `headers` overrides,
  proves owned HTTP clients use `trust_env=False`, and verifies malformed tool-call arguments and
  malformed usage normalize to `MODEL_PROVIDER_ERROR`.

### TDD evidence

The focused unit file was run before the fixes. It reported `2 failed, 12 passed`: a graph rebuilt
after a conflicting profile registration exposed `unregistered_network`, and the normalized timeout
retained the raw secret-bearing `httpx.ReadTimeout` as `__cause__`. After the minimal fixes, the same
unit file reported `14 passed in 1.37s`.

### Bounded verification

```text
timeout 30s env UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task6 uv run pytest \
  tests/contract/test_deepagents_077.py tests/unit/test_agent_factory.py -q
```

Result: `16 passed in 3.68s`.

```text
timeout 30s env UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task6 uv run ruff check \
  packages/deepagents_adapter/factory.py packages/deepagents_adapter/profile.py \
  packages/deepagents_adapter/version.py packages/model_gateway/client.py \
  tests/contract/test_deepagents_077.py tests/unit/test_agent_factory.py
```

Result: `All checks passed!`.

```text
timeout 30s env UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task6 uv run pyright \
  packages/deepagents_adapter/factory.py packages/deepagents_adapter/profile.py \
  packages/deepagents_adapter/version.py packages/model_gateway/client.py \
  tests/contract/test_deepagents_077.py tests/unit/test_agent_factory.py
```

Result: `0 errors, 0 warnings, 0 informations`.

### Remaining concern

The Deep Agents harness registry remains process-global because 0.7.7 exposes no per-call profile
parameter. Security does not depend on that registry remaining unchanged: the compiled graph owns
the exact allowlist wrapper, while the registry is used only to suppress construction of SDK
defaults. No external service was contacted during this fix round.
