# Agent Runtime Kernel Iteration 1 Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the eight confirmed P0 runtime defects with migrations, durable effect intent, trusted principals, fenced recovery, complete Checkpoint lineage validation, and regression tests.

**Architecture:** Keep the Node.js 24 modular monolith and built-in SQLite. Add focused integrity/auth modules, versioned schema migration, an external-effect Outbox path, and one active Checkpoint lineage; preserve existing public resource paths while replacing caller-declared identity with authenticated principals.

**Tech Stack:** Node.js 24 ESM, `node:http`, `node:sqlite`, `node:test`, browser-native HTML/CSS/JavaScript.

**Status:** Completed on 2026-08-10. The checklist records the executed TDD sequence; independent review findings were added to the regression suite before final verification.

## Global Constraints

- Development and all tests remain container-free and dependency-free.
- No production code is changed before its regression test is observed failing for the intended reason.
- Unknown external outcomes are persisted and never automatically replayed.
- Every Run state write from a Worker is fenced by tenant, lease, owner, epoch, expiry, and state version.
- Historical invalidated Checkpoints and effect attempts remain queryable; no repair deletes audit evidence.
- Existing resource URLs stay stable; authentication semantics may intentionally become stricter.
- Phase one still excludes cost/token budgets, step importance, dead-letter queues, Temporal, Kafka, Kubernetes, and microservice decomposition.

---

### Task 1: Context and versioned idempotency integrity

**Files:** Modify `test/context-store.test.js`, `test/tool-contracts.test.js`, `src/context-store.js`, `src/db.js`, and `src/tool-contracts.js`; create `test/migrations.test.js`.

**Interfaces:** Preserve `ContextStore.put/read/revoke` and `ToolRegistry.register/get/invoke`. Produce schema migration from the legacy Tool effect unique key to `(tenant_id, operation_id, contract_version, idempotency_key)`.

- [ ] Add a Context test proving two tenants and two classifications can store identical content with distinct opaque references while the same tenant/classification still deduplicates.
- [ ] Run `node --test test/context-store.test.js` and observe the legacy global `context_ref_id` collision.
- [ ] Change new Context IDs to `ctx_<uuid>` after the existing tuple lookup and rerun the focused test.
- [ ] Add a Tool test registering operation versions 1 and 2 with the same idempotency key and literal distinct outputs; assert both executors run once and two effects persist.
- [ ] Run `node --test test/tool-contracts.test.js` and observe v2 incorrectly replays v1.
- [ ] Rebuild the legacy Tool effect table with version-scoped uniqueness, update all lookup predicates, and add a file-backed migration test preserving a legacy committed effect.
- [ ] Run the three focused test files and confirm all assertions pass.

### Task 2: Checkpoint integrity v2 and revoked lineage

**Files:** Create `src/checkpoint-integrity.js`; modify `src/db.js`, `src/runtime.js`, `test/chaos.test.js`, `test/runtime.test.js`, `test/replay.test.js`, and `test/migrations.test.js`.

**Interfaces:** Produce v1 verification/v2 canonical hashing, `integrity_version`, full row/event validation, parent-walk reconstruction through an optional head, and active status `COMMITTED` versus `INVALIDATED_CONTEXT_REVOKED`.

- [ ] Add table-driven corruption tests that independently alter `schema_version`, `event_seq`, `execution_epoch`, `kind`, `status`, and the matching event payload; each recovery must throw `CHECKPOINT_CORRUPT`.
- [ ] Run the corruption tests and observe legacy recovery accept the tampering.
- [ ] Add rollback tests where the safe head is a DELTA and where a later recover must not replay a revoked-derived Checkpoint.
- [ ] Run the rollback tests and observe the missing anchor field and replayed unsafe state.
- [ ] Implement canonical v2 hashing, legacy upgrade, event cross-checking, parent-walk reconstruction, active-parent selection, and transactional invalidation.
- [ ] Fence the old lease during revoked rollback by incrementing epoch and clearing lease ownership in the same transaction.
- [ ] Rerun focused corruption, rollback, replay, and migration tests.

### Task 3: Manual gate and recovery fencing

**Files:** Modify `src/runtime.js`, `test/runtime.test.js`, and `test/chaos.test.js`.

**Interfaces:** Add explicit operator `resolveManual`, guarded recovery source states, and a common fenced Run update using lease/epoch/state-version compare-and-set.

- [ ] Add a test proving `WAITING_MANUAL` recovery throws before creating a lease or incrementing epoch.
- [ ] Add an instrumented database interleaving test that lets another Worker take over after the first recovery lease and before a pending/corrupt branch finalizes; assert the stale recovery writes no state or event.
- [ ] Run the focused tests and observe the bypass and stale branch write.
- [ ] Guard lease acquisition by allowed status and implement fenced compare-and-set for success, pending-effect, and corrupt-Checkpoint recovery outcomes.
- [ ] Add explicit manual resolution to `RECOVERING` or `FAILED`, increment epoch, clear the old lease, require a reason, and append the authenticated operator decision.
- [ ] Rerun the focused tests and the existing state-machine suite.

### Task 4: Durable external effect intent and Outbox

**Files:** Modify `src/db.js`, `src/tool-contracts.js`, `src/runtime.js`, and `test/tool-contracts.test.js`; create `test/effects.test.js`.

**Interfaces:** Add contract `executionMode`, `ToolExecutionError`, `tool_effect_outbox`, durable effect statuses `PREPARED/DISPATCHING/COMMITTED/FAILED/UNKNOWN`, and manual effect resolution.

- [ ] Add a fake external adapter test that increments an external counter then throws a timeout; assert one `UNKNOWN` intent/outbox remains and a repeated key never invokes it again.
- [ ] Add explicit rejection, success replay, durable-intent-before-dispatch, and restart-with-dispatch-in-flight tests using literal outcomes.
- [ ] Run `node --test test/effects.test.js` and observe missing execution mode/outbox behavior.
- [ ] Persist intent and Outbox atomically, dispatch outside SQLite, and record success/rejection/unknown outcomes in a second transaction.
- [ ] Pass stable `effectId` and idempotency key to adapters; never redeliver `UNKNOWN` or in-flight effects.
- [ ] Ensure runtime transitions to `WAITING_MANUAL` for unknown outcomes and records an explicit failed Run for known failed effects.
- [ ] Rerun effect, Tool contract, runtime, and chaos tests.

### Task 5: Authenticated principal and takeover authorization

**Files:** Create `src/auth.js` and `test/auth.test.js`; modify `src/api.js`, `src/server.js`, `scripts/dev.js`, `public/app.js`, `public/index.html`, `test/api.test.js`, `test/security.test.js`, `test/e2e.test.js`, and `test/console.test.js`.

**Interfaces:** Produce `createStaticAuthenticator`, principal permission checks, Bearer authentication, principal-bound lease use, and production identity configuration through `AGENT_RUNTIME_IDENTITIES`.

- [ ] Add HTTP tests for missing token, tenant-header mismatch, actor spoofing, Worker mismatch, ordinary Worker force takeover, unauthorized recovery, and authorized reasoned takeover.
- [ ] Run the auth/security tests and observe current forged headers and bodies succeed.
- [ ] Implement opaque Bearer principal resolution and permission checks; derive tenant, actor, and Worker only from the principal.
- [ ] Require `run:takeover`/`run:recover` plus a reason, and include subject, old owner, and reason in lease/recovery events.
- [ ] Update all HTTP fixtures and the local console to use the explicit local development token.
- [ ] Validate event cursors, lease TTL, booleans, contract versions, and identity-bearing body fields into stable domain errors.
- [ ] Rerun API, auth, security, console, and end-to-end tests.

### Task 6: Documentation, complete verification, and independent review

**Files:** Modify `README.md`, `package.json`, and the delivery archive.

**Interfaces:** Document version `0.2.0`, trust assumptions, migration behavior, effect states, manual gates, local token, and exact verification commands.

- [ ] Update README claims so SQLite/Outbox is not described as cross-system exactly-once and production adapters remain target architecture rather than completed behavior.
- [ ] Run `npm test` and require zero failed/cancelled/skipped/todo tests.
- [ ] Run `npm run check` and require exit code 0.
- [ ] Start a real process on an ephemeral port, verify public console, authenticated API, unauthorized API, and graceful shutdown.
- [ ] Dispatch an independent whole-project reviewer against this design and plan; fix every Critical/Important finding and rerun affected tests.
- [ ] Build the archive from the validated source tree, list its contents, extract it into a temporary directory, and rerun `npm test` plus `npm run check` from the extracted copy.
