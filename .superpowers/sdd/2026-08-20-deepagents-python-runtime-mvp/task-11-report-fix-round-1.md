# Task 11 fix round 1 report

## Review findings resolved

- `ExecutionManager` now accepts an optional `RuntimeEventEmitter` seam. When configured,
  normalized execution events use the emitter for the existing durable sequence/terminal sink,
  projection store, payload offload, and low-cardinality metrics path; the legacy sink path remains
  unchanged for callers that have not configured the production dependencies.
- Trace timeline entries retain `event_id` and the SHA-256 event digest. PostgreSQL projection
  writes lock the execution/projection rows, verify the persisted execution trace identity, and
  reject stale, conflicting, duplicate-sequence, or conflicting-digest updates. A new bounded
  `trace_projection.event_index` JSONB column stores the durable event identity index independently
  of the public summary size bound; migration `0002_trace_projection_event_index` adds it.
- Projection status is derived from the ordered timeline, so a late lower-sequence `started` event
  cannot regress a higher-sequence terminal event. Oversized summary fallback is deterministic and
  rechecked to stay below 48 KiB.
- Nested credential redaction now covers hyphenated, underscored, camelCase, `x-api-key`,
  `accessToken`, `proxy-authorization`, and common authorization/secret suffix and prefix forms
  before payload sizing and upload.
- Retention preflights object-store/repository availability before session or stream cleanup,
  passes an explicit tenant to every repository cleanup operation, and only lists/deletes keys
  matching the generated `payloads/{tenant}/{execution}/{event}-{sha256}.json` grammar after
  durable reference evidence.
- Duplicate Prometheus collector reuse now compares label schemas and raises a clear error for an
  incompatible same-name collector.

## Verification

Focused Task 11:

```text
17 passed, 2 skipped
```

The skips are explicit live PostgreSQL trace projection and live MinIO/PostgreSQL retention gates.

Task 9 regression:

```text
22 passed, 1 warning
```

Task 10 regression:

```text
14 passed, 1 warning
```

Full Python repository:

```text
256 passed, 10 explicit infrastructure skips, 1 warning
```

Scoped Ruff and strict Pyright passed. No model provider, PostgreSQL, Redis, or MinIO service was
started, and no external credential was used.

## Caveats

- The emitter seam is dependency-injected; this round does not invent a new database/object-store
  composition root or silently claim live infrastructure verification.
- The PostgreSQL projection guard uses row locks plus persisted event-index/version checks. A stale
  projector fails closed and must replay/rebuild from the durable normalized event stream before
  retrying; it never overwrites a newer projection.
