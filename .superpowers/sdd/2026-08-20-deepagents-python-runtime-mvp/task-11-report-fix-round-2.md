# Task 11 fix round 2 report

## Review findings resolved

- `RuntimeEventEmitter` now selects a projector by the complete execution trace
  identity before calling the durable event sink. A configurable
  `projector_factory` creates a fresh `TraceProjector` for a new execution; the
  projector is reused only for that execution and released after its terminal
  event is persisted. A deterministic two-execution regression proves that the
  second event cannot be appended through the first execution's projector.
- Credential-key normalization now separates camelCase boundaries before
  applying the existing sensitive-key rules. `secretKey`,
  `authorizationHeader`, `tokenValue`, and `clientSecretValue` are redacted in
  nested payloads before sizing and object-store upload. Existing
  `token_count` accounting remains non-sensitive.
- `PostgresTraceProjectionStore.save()` now locks and validates the persisted
  `RuntimeSessionRow` package tuple `(agent_id, package_version, package_digest)`
  before querying or inserting the first projection row. A mismatch fails
  closed and the deterministic session proves no projection row is added.

## TDD evidence

The three new regressions were first run against the previous implementation:

```text
3 failed
```

The failures were the fixed-projector identity error after the second durable
append, missing session-package validation, and leaked `secretKey` payload
content. After the minimal implementation changes:

```text
3 passed
```

## Verification

Task 11 focused tests:

```text
19 passed, 2 skipped, 1 warning
```

The skips are explicit live PostgreSQL trace projection and live
MinIO/PostgreSQL retention gates.

Task 9 regression:

```text
34 passed, 1 warning
```

Task 10 regression:

```text
14 passed, 1 warning
```

Static checks:

```text
Ruff: all checks passed
Pyright: 0 errors, 0 warnings, 0 informations
git diff --check: passed
```

No PostgreSQL, Redis, MinIO, model provider, or external credential was used.
The live service boundaries remain explicit release-gate caveats; deterministic
doubles do not claim live infrastructure or provider compatibility.

## Scope

Only the Task 11 emitter, normalizer, projection persistence, deterministic
regressions, and this report are intended for the fix commit. Task 10 and Task
12 commits/files remain preserved; concurrent uncommitted Task 12 console edits
were not modified or staged.

Commit message:

```text
fix: close Task 11 review round 2 findings
```
