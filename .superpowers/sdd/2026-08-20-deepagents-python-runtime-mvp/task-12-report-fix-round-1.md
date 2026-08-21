# Task 12 review fix round 1

## Scope

Fixed only the new Runtime Console and its acceptance/client tests. Backend
routes, the legacy UI, and concurrent Task 11 files were not changed.

## Findings addressed

- Changed `RuntimeExecutionResult` to the flat Python API response shape and
  rendered `result.status` instead of an absent nested execution object.
- Added bounded output extraction for `model.delta` `payload.text`, nested
  `messages`, assistant content blocks, and final `execution.output` payloads.
- Made cancellation provenance explicit: sync/stream shows browser-only stop
  semantics; async requests the available Runtime task cancellation. The
  client preserves `AbortError` so the UI does not mislabel an aborted request
  as a Runtime outage.
- Reset Sessions, messages, and event timelines whenever LIVE/DEMO changes;
  the send path also rejects a Session whose mode provenance does not match.
- Extended generic error sanitization to Bearer, token, API-key,
  authorization, secret, password, and related credential forms before they
  reach rendered errors.

## Verification

- `NPM_CONFIG_CACHE=/tmp/runtime-console-npm-cache npm test` — 16/16 passed;
  deterministic tests only.
- `NPM_CONFIG_CACHE=/tmp/runtime-console-npm-cache npm run build` — passed.
- `UV_CACHE_DIR=/tmp/runtime-console-uv-cache uv run pytest tests/acceptance/test_console_runtime.py -q` — 6/6 passed.
- `git diff --check` — passed.
- No real Runtime, model, PostgreSQL, Redis, MinIO, or external service was
  used.
