# Task 12 review fix round 2

## Scope

Fixed only the Runtime Console and its deterministic tests. Backend routes,
Task 11 files, and the legacy UI were not changed.

## Findings addressed

- Preserved bounded `model.delta` text for completed async tasks. The async
  status string is now only a fallback when no answer or error text exists;
  added a regression covering a successful async task with streamed model text.
- Added `last_message_text` to execution output extraction for the Python
  backend response shape, with a direct deterministic regression.
- Applied the 8192-character bound to the accumulated streamed answer, not
  only to each individual delta; added a two-delta regression.
- Redacted complete `Basic` and `Bearer` authorization values plus common
  `token`, `api_key`, `x-api-key`, and camel-case credential key forms before
  error text reaches the UI; added a sanitizer regression.

## TDD and verification

- Initial red run: the four new regressions failed for the reported reasons.
- Green run: targeted console tests passed 19/19.
- `npm test` — 20/20 passed.
- `npm run build` — TypeScript check and Vite production build passed.
- `UV_CACHE_DIR=/tmp/runtime-mvp-uv-cache-task12-round2 uv run pytest tests/acceptance/test_console_runtime.py -q` — 6/6 passed.
- `git diff --check` — passed.
- No live Runtime, model, PostgreSQL, Redis, MinIO, or external service was
  called.
