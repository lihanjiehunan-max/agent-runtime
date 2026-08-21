# Node-to-Python Runtime Cutover and Rollback

## Migration principle

Use a strangler cutover. The frozen Node Runtime remains available for legacy
Run evidence while the Python Runtime serves only the versioned
`/api/v1/runtime` surface. Do not transform Node SQLite demo Runs into Python
Sessions, and do not share mutable chat history between the two runtimes.

## Pre-cutover gates

Before switching console traffic, record:

1. `uv.lock` SHA-256 and the installed `deepagents==0.7.7` version.
2. The `agent-metric-query:0.1.0` package digest and `checksums.txt` hash.
3. Alembic migration head and PostgreSQL backup/restore evidence.
4. Redis, MinIO, Model Gateway, and Tool Gateway health evidence.
5. Deterministic Task 14 acceptance output, including the explicit live skip.
6. The Task 13 performance output with its deterministic-only limitation.

Do not declare release acceptance while a live dependency gate is missing or
while the runtime execution-load/performance gap is unacknowledged.

## Side-by-side deployment

1. Keep the current Node API and console route serving legacy `/api/runs`.
2. Deploy the Python API and worker with the two process units in
   `deploy/process/`.
3. Deploy the React console bundle separately. Configure the reverse proxy so
   `/api/v1/runtime/*` routes to Python and legacy routes remain on Node.
4. Run the Python console in DEMO mode only when no Runtime endpoint is
   configured; a public browser must never be expected to reach an operator's
   localhost address.
5. Create a new Python Session and execute the three acceptance turns. Verify
   one `thread_id`, one package digest, three Execution records, tool events,
   SSE replay, and trace references.
6. Restart the worker and complete the fourth turn on the same Session. Verify
   epoch fencing and separate Session/checkpoint namespaces.
7. Only after the gates pass, change the console's default Runtime base path to
   the Python `/api/v1/runtime` proxy. Confirm `/health/ready` is HTTP 200 and
   runtime status shows all provider integrations configured.

## Traffic switch

The traffic switch is a reversible routing/configuration change, not a data
copy. Change the console/API route in the reverse proxy or deployment
configuration, perform a smoke request, and retain the previous route as an
immediate fallback. Never place model or Tool Gateway credentials in the
console bundle.

## Rollback

Trigger rollback for failed health checks, trace identity violations,
checkpoint fencing failures, cross-Session leakage, or provider errors that
cannot be bounded.

1. Stop new Python console traffic and restore the previous console/API route.
2. Leave Python PostgreSQL, Redis, MinIO, package, Session, Execution, event,
   trace, and checkpoint records intact for diagnosis and forward recovery.
3. Keep Node SQLite read-only; do not import Python state into Node Runs.
4. Drain or stop Python workers only after active executions reach a durable
   terminal state or are explicitly marked for recovery.
5. Verify the legacy console and `/api/runs` smoke path, then capture the
   Python trace/health evidence for the incident.

Rollback does not mean deleting Python records or downgrading the database.
When Python is retried, use the existing Session only if its package digest and
checkpoint state are valid; otherwise create a new explicitly versioned
Session.

## Current cutover limitations

- No production proxy configuration is changed by Task 14.
- The live service acceptance path is opt-in through documented environment
  variables and is skipped when they are absent.
- The API readiness endpoint is configuration- and composition-aware; it is
  still not a substitute for the opt-in live acceptance or provider probes.
- Worker restart and fourth-turn continuity are proven with deterministic
  in-process doubles, not a live process restart in this workspace.
