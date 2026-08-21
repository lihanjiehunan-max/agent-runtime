# Task 7 Report: Least-Privilege Tool Gateway and `query_metric`

## Status

Complete and committed with message `feat: add least-privilege metric query tool gateway`.
Task 7 adds a frozen read-only tool contract, a least-privilege authorization/event boundary,
and a server-credentialed HTTP client for the real `query_metric` Tool Gateway endpoint. No push
was performed. No Task 2 contract or parallel-lane file was modified.

## Files

- `packages/tool_gateway/contracts.py`
- `packages/tool_gateway/service.py`
- `packages/tool_gateway/query_metric.py`
- `tests/contract/test_query_metric.py`
- `tests/security/test_tool_permissions.py`
- `.superpowers/sdd/2026-08-20-deepagents-python-runtime-mvp/task-7-report.md`

## Implementation

- Added immutable, extra-forbidding `MetricQuery`, `MetricResult`, `ToolContract`, and
  `ToolEventContext` Pydantic contracts. Query strings are required, trimmed, and bounded at 128
  characters. The accepted period grammar is explicit: current/previous month, quarter, or year;
  `YYYY`; `YYYY-MM`; or `YYYY-QN`.
- Added stable `ToolGatewayErrorCode` values for permission denial, timeout, unavailable/rejected
  gateway calls, invalid responses, and oversized results. Normalized exceptions do not include
  provider messages or credentials.
- Preserved future effect semantics with `ToolExecutionMode.READ_ONLY`,
  `LOCAL_TRANSACTIONAL`, and `EXTERNAL_OUTBOX`. The only published tool contract is
  `query_metric`, marked `READ_ONLY`; no write tool or write executor was added.
- Added `QueryMetricClient`, which calls `POST /v1/tools/query_metric` with compact bounded JSON,
  an explicit `httpx.Timeout` containing connect/read/write/pool values, and server-owned bearer
  credentials. `X-Request-ID`, `X-Tenant-ID`, and `X-User-ID` are propagated as headers and are
  not accepted from model tool arguments.
- Response bodies are streamed and bounded before JSON parsing. Oversized, malformed,
  schema-invalid, non-echoing, timeout, request, 4xx, 429, and 5xx outcomes normalize to stable
  errors. The configured result limit cannot exceed the hard 64 KiB cap.
- Added `ToolGateway`, which captures the authenticated `Principal` and immutable package
  allowlist outside model-controlled arguments. Both exact package authorization for
  `query_metric` and principal permission `tool:query_metric` are required; tenant and worker
  context must also match.
- Every valid invocation emits `tool.started`, then exactly one `tool.completed` or `tool.failed`
  using the existing Task 2 `RuntimeEvent` and `Principal` contracts. Payloads contain bounded
  correlation/identity metadata and stable failure codes, not query results or credentials.
  Event sequence allocation and sink append are serialized so overlapping calls remain globally
  monotonic while each call retains one span ID.
- `ToolGateway.as_langchain_tool()` returns a `StructuredTool` named `query_metric` whose only
  model-visible arguments are `metric`, `period`, and `org`, and whose result is `MetricResult`.
- The deterministic contract case is `营业收入 / 本月 / 散运公司 = 1280000.00 CNY`.

## TDD evidence

### Initial contract RED

The contract/transport tests were created before Task 7 production modules. The first focused run
failed during collection for the intended missing-feature reason:

```text
ModuleNotFoundError: No module named 'packages.tool_gateway'
1 error in 0.11s
```

After the minimal frozen contracts and HTTP adapter were added, the contract slice passed:

```text
25 passed in 0.04s
```

### Permission/event RED

The security tests were then written before the event context and gateway service. Collection
failed because `ToolEventContext` and the service did not yet exist. After implementing the
authorization, event, and LangChain boundaries, the initial security slice passed:

```text
7 passed in 0.15s
```

### Concurrent event-order RED/GREEN

A deterministic overlap test blocked the first HTTP call while a second completed. Before the
fix, event append order was non-monotonic:

```text
FAILED test_overlapping_tool_calls_emit_monotonic_event_sequences
assert [41, 43, 44, 42] == [41, 42, 43, 44]
1 failed in 0.17s
```

Sequence assignment now occurs atomically with sink append. The same regression test then passed:

```text
1 passed in 0.14s
```

## Final bounded verification

Exact Task 7 test command:

```text
timeout 30s env UV_CACHE_DIR=/tmp/uv-cache uv run pytest \
  tests/contract/test_query_metric.py tests/security/test_tool_permissions.py -q
.................................                                        [100%]
33 passed in 0.16s
```

Scoped Ruff:

```text
All checks passed!
```

Scoped strict Pyright:

```text
0 errors, 0 warnings, 0 informations
```

Lockfile consistency and whitespace validation:

```text
Resolved 82 packages in 0.72ms
git diff --check: exit 0
```

## External integration availability

- PostgreSQL integration configuration was unavailable (`RUNTIME_TEST_DATABASE_URL` unset).
- Redis integration configuration was unavailable (`RUNTIME_TEST_REDIS_URL` and
  `RUNTIME_REDIS_URL` unset).
- No PostgreSQL or Redis integration result is claimed, and neither service was replaced with an
  unsafe in-memory fake.
- No live Tool Gateway endpoint was configured. Contract tests exercise the real HTTP client,
  streaming limit, headers, and timeout/error behavior through deterministic
  `httpx.MockTransport`; they do not claim live gateway connectivity.

## Concerns

- The downstream Tool Gateway must implement `POST /v1/tools/query_metric` and the frozen
  `tool.metric-result.v1` response exactly; mismatched echoed dimensions fail closed as
  `TOOL_INVALID_RESPONSE`.
- The original implementation allowed event sink failures to propagate without normalization;
  Fix round 1 below supersedes that behavior while leaving durable persistence to Task 9.

## Fix round 1: Identity, lifecycle, sequencing, event persistence, and HTTPS

### Status

The five Task 7 review findings are addressed in a scoped follow-up. No Task 9 implementation was
added, and no Task 8 source, test, dependency, or lockfile change was modified or staged.

### Changes

- Tool invocation now requires `Principal.worker_id` to be non-null and exactly equal to
  `ToolEventContext.worker_id` before any event append or Tool Gateway HTTP request. Null and
  mismatched workers return stable `TOOL_PERMISSION_DENIED` with no lifecycle events.
- `MetricQuery` validation now occurs after `tool.started` inside the invocation lifecycle.
  Direct malformed calls normalize Pydantic failures to `TOOL_INVALID_ARGUMENT`, append exactly
  one `tool.failed`, and never make an HTTP request or expose a raw `ValidationError`.
- Sequence ownership moved out of `ToolGateway`. Gateways now consume the injectable
  `SequencedToolEventSink.append(event_factory)` contract, which atomically allocates a sequence
  and appends the event. `LocalSequencedToolEventSink` is explicitly process-local and can be
  shared by multiple gateway instances; it is not described as PostgreSQL durability.
- Event append failures normalize to retryable `TOOL_EVENT_PERSISTENCE_FAILED`. A failed
  `tool.started` append prevents the HTTP call. If a started invocation's terminal append fails,
  the gateway makes exactly one bounded `tool.failed` recovery append and always raises the
  persistence error instead of returning a successful metric result.
- `QueryMetricConfig` now accepts only a valid HTTPS URL with a host. Plaintext HTTP, including
  loopback HTTP, hostless HTTPS, and malformed URLs are rejected before bearer credentials can be
  retained by a client.

### Task 9 durable handoff

`SequencedToolEventSink` is the handoff boundary for the later durable Event layer. Its contract
requires sequence allocation and event persistence to be atomic: a successful return means the
event is committed; an exception means no event was committed and no sequence was consumed.
Task 9 must implement that boundary with the session sequence/epoch fence and durable append in
one transaction. The Task 7 `LocalSequencedToolEventSink` only provides a lock and in-process
counter around a synchronous callback; it does not claim cross-process coordination, crash
recovery, or database durability.

### TDD evidence

Each review finding was reproduced before its fix:

- Worker identity RED: null worker made the HTTP call and mismatched worker emitted two events;
  `2 failed`. GREEN: `2 passed` with no events or HTTP.
- Direct validation RED: all three malformed calls raised raw Pydantic `ValidationError` before
  `tool.started`; `3 failed`. GREEN: `3 passed` with stable failed terminals.
- Shared sequence RED: two gateways produced `[41, 42, 41, 42]` instead of
  `[41, 42, 43, 44]`; `1 failed`. GREEN: the two-instance and overlapping-call regressions both
  passed with one shared sink.
- Event sink RED: start, transient terminal, and permanent terminal failures escaped raw
  `RuntimeError`; `3 failed`. GREEN: `3 passed` with stable failure and one bounded recovery.
- HTTPS RED: two plaintext URLs and one hostless HTTPS URL were accepted; `3 failed`. GREEN:
  those cases passed. Strict Pyright then exposed a malformed-URL unbound path; adding
  `https://[` reproduced it as `UnboundLocalError`, and the boolean validation boundary passed
  all four invalid URL cases with zero type errors.

### Bounded verification

```text
UV_CACHE_DIR=/tmp/uv-cache uv run pytest \
  tests/contract/test_query_metric.py tests/security/test_tool_permissions.py -q
..............................................                           [100%]
46 passed in 0.19s
```

```text
UV_CACHE_DIR=/tmp/uv-cache uv run ruff check \
  packages/tool_gateway/contracts.py packages/tool_gateway/service.py \
  packages/tool_gateway/query_metric.py tests/contract/test_query_metric.py \
  tests/security/test_tool_permissions.py
All checks passed!
```

```text
UV_CACHE_DIR=/tmp/uv-cache uv run pyright \
  packages/tool_gateway/contracts.py packages/tool_gateway/service.py \
  packages/tool_gateway/query_metric.py tests/contract/test_query_metric.py \
  tests/security/test_tool_permissions.py
0 errors, 0 warnings, 0 informations
```

### Remaining concern

Until Task 9 supplies the durable implementation, all gateways participating in one execution
must receive the same `LocalSequencedToolEventSink` instance. Separate processes or independently
constructed local sinks do not coordinate sequences and are intentionally not presented as safe
durable event writers.

## Fix round 2: Reject credential-bearing gateway URLs

### Finding and fix

`QueryMetricConfig` previously accepted HTTPS URLs containing userinfo, query strings, or
fragments. Because `QueryMetricClient.__repr__` includes the configured base URL, credentials in
those components could be exposed through logs or diagnostics.

Configuration now rejects:

- URL userinfo, with or without a password;
- any query string; and
- any fragment.

The existing HTTPS-with-host requirement remains unchanged, including rejection of plaintext
loopback HTTP. Since unsafe structured URL components cannot enter a valid config, the client repr
cannot expose credentials carried in those components. The server-owned API key remains excluded
from both config and client repr output.

### TDD evidence

The contract regression was added before the implementation change. Userinfo, username-only,
query-string, and fragment cases were all accepted:

```text
FFFF                                                                     [100%]
4 failed in 0.08s
```

After extending the config validation boundary, the same regression passed:

```text
....                                                                     [100%]
4 passed in 0.04s
```

### Bounded verification

```text
UV_CACHE_DIR=/tmp/uv-cache uv run pytest \
  tests/contract/test_query_metric.py tests/security/test_tool_permissions.py -q
..................................................                       [100%]
50 passed in 1.59s
```

Scoped Ruff: `All checks passed!`

Scoped strict Pyright: `0 errors, 0 warnings, 0 informations`.

No Task 8 source, test, dependency, or lockfile change was modified or staged by this fix round.
