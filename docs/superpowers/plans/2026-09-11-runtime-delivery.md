# Distributed Runtime Delivery Implementation Plan

**Goal:** close the usable-software gap after the nine-container baseline, and produce fresh, independently inspectable verification evidence.
**Baseline:** remote commit 400a740f43a477f91af89200bbfb0beed5f0df6f. Local workspace is extracted from its CI source archive; local Git history is not the remote history.
**Architecture:** retain the existing Python API/Worker, SQL authority, Redis notification and object storage boundaries. Extend the existing runtime rather than replace it. Keep the old Node implementation as regression evidence only.
**Tech stack:** Python 3.12 release runtime, DeepAgents 0.7.7, SQLAlchemy/PostgreSQL, Redis, S3; React/TypeScript console built separately. Local Python 3.13 tests supplement, not replace, release-version CI.

## Acceptance and limits
- Preserve all existing tests and the 15 isolated Docker scenarios.
- Business/operations API credentials remain server-configured; the model and business request cannot select a tool URL or credential.
- No automatic write replay, no production credential in source, no implicit fixture fallback in the live deployment.
- Integration status is PASS / FAIL / BLOCKED; absent real model/tool credentials must never produce a live PASS.
- Same-host containers do not prove physical-host HA. Physical deployment requires an accessible authorized target; no target has been supplied.
- Real business payloads and receipts must not be uploaded to public CI artifacts. Live evidence contains IDs, hashes and structural assertions only.

## Work items and verification
- [ ] Tool contract: metric, period, org, optional comparison/group_by; separate tool credential, correlation, bounded HTTP, read-only failure recovery. Files: tools.py, harness.py, worker.py; tests/test_delivery_tools.py. Run failing tests before implementation.
- [ ] Operations and chat: optional separate operations credential, bounded synchronous wait, session history projections, paged traces, version activation, drain, Prometheus metrics. Files: api.py, config.py, operations.py, db.py; tests/test_delivery_api.py. Verify unauthorized writes, stable sessions, retained idempotency and restart behavior.
- [ ] Checkpoint integrity: additive checksum metadata for new checkpoints/pending writes, verified before deserialization, no silent retroactive trust of old records. Files: checkpoints.py, db.py, integrity.py; tests/test_delivery_integrity.py. Verify tampered blob/metadata/identity, stale writers and explicit legacy migration boundary.
- [ ] Console: real Agent/Session/chat/execution/worker/trace actions; streamed events with reconnect cursors and memory-only credential. Files: apps/runtime_console; browser test scripts. Verify built assets and end-to-end browser actions against the actual API and Worker.
- [ ] Deployment and acceptance: release image excludes fixtures, required environment, split API/Worker secret scope, reverse proxy, preflight/live gate, read-only live multi-turn acceptance, bounded soak, deployment/runbook. No deployment is labelled production-ready without live acceptance.
- [ ] Fresh verification: local RED/GREEN evidence, Python release-version CI, Node baseline, original container faults, new console/deployment tests. Pin evidence to the pushed commit, review diffs and publish source and reports.
