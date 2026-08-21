# Task 11 report

## Delivered

- Added a bounded `TraceProjector` and `TraceProjection` DTO backed by the existing
  `TraceProjectionRow` model. Events must match tenant, trace, session, execution,
  package, and SDK identity. Duplicate event IDs with identical content are idempotent;
  duplicate sequences or forged identities fail closed. The projection keeps a bounded
  ordered timeline and summary for package resolution/cache, session lock, model, tool,
  checkpoint, output, and terminal events.
- Added `RuntimeEventEmitter` for normalized events. It preserves the existing durable
  event sink as the sequence source, attaches the execution trace identity, aggregates
  adjacent model text deltas, applies payload redaction/offload, persists the projection
  through an explicit store, and optionally observes metrics.
- Added payload offload and retention contracts. The default offload threshold is 48 KiB,
  below the 64 KiB event envelope. Large payloads are stored under:

  ```text
  payloads/{tenant_id}/{execution_id}/{event_id}-{sha256(payload)}.json
  ```

  Only `payload_ref` remains in the event when offloaded. Payloads are recursively
  redacted before sizing/upload; credentials and authorization values are not retained.
  The production offload path raises when object storage is absent. Retention lists only
  one tenant prefix, enforces a 500-object bound, asks the durable repository for
  referenced-key evidence, and deletes only explicit unreferenced keys. No broad globs
  are used. Session expiry, 30-day closed metadata/checkpoint pruning, and live-stream
  pruning are exposed only through explicit bounded repository methods.
- Added duplicate-safe Prometheus collectors. Metric families and labels are:

  | Metric family | Labels |
  |---|---|
  | `runtime_executions_total` | `status` (`accepted`, `running`, `succeeded`, `failed`, `timed_out`, `cancelled`, `other`) |
  | `runtime_execution_duration_seconds` | `status` from the same bounded set |
  | `runtime_model_duration_seconds` | none |
  | `runtime_tool_duration_seconds` | none |
  | `runtime_tokens_total` | `direction` (`input`, `output`) |
  | `runtime_package_cache_events_total` | `result` (`hit`, `miss`) |
  | `runtime_active_sessions` | none |
  | `runtime_queue_depth` | none |
  | `runtime_cancellation_latency_seconds` | none |

  No tenant, user, session, execution, trace, worker, model-input, or tool-input labels
  are emitted. The dashboard uses these same names.
- Wired `/metrics` and authenticated `/api/v1/runtime/status` through `create_api()`.
  Status returns only stable configured/not-configured dependency states and never raw
  provider errors or secret values. Added valid Prometheus and Grafana configurations.
- Extended `MinioObjectStoreClient` with upload, delete, and bounded list operations while
  keeping the existing package-loader download protocol compatible with its deterministic
  test doubles.

## Verification

Focused Task 11 command:

```text
UV_CACHE_DIR=/tmp/uv-cache-task11-focused2 uv run pytest \
  tests/integration/test_trace_projection.py \
  tests/integration/test_payload_offload.py \
  tests/unit/test_metrics_labels.py -q -rs
9 passed, 2 skipped, 1 warning
```

The two skips are explicit live PostgreSQL and live MinIO/PostgreSQL retention skips.
No PostgreSQL, MinIO, Redis, model, or provider service was started.

Task 11 static checks:

```text
UV_CACHE_DIR=/tmp/uv-cache-task11-ruff5 uv run ruff check <Task 11 scope>
All checks passed!

UV_CACHE_DIR=/tmp/uv-cache-task11-pyright5 uv run pyright <Task 11 scope>
0 errors, 0 warnings, 0 informations

git diff --check
passed
```

Relevant schema/contract and Task 9 regression command produced 66 passed and 5 explicit
service skips. One existing streaming test failed because the concurrently present Task 10
changes in `packages/execution_manager/service.py` alter the first live-event delivery
timing; Task 11 did not modify that file or attempt to fix that out-of-scope failure.

Full-repository Ruff/Pyright were also attempted. Their failures are confined to the
concurrent Task 10 files (`packages/execution_manager/__init__.py`, `queue.py`, and its
async-task/cancellation tests); the Task 11 scope is clean as shown above.

## Handoff

Commit message:

```text
feat: add runtime tracing metrics and payload retention
```
