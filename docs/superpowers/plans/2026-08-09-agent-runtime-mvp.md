# Enterprise Agent Runtime MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a locally runnable enterprise Agent Runtime MVP covering run orchestration, context references, versioned tool contracts, effect safety, checkpoints, lease fencing, event audit, recovery, and an operator console.

**Architecture:** A zero-container modular monolith runs on Node.js 24. Domain modules are isolated behind services; authoritative state is persisted with built-in SQLite, large context is referenced by immutable IDs, HTTP/SSE expose control and event APIs, and the static console consumes only those APIs. The contracts remain portable to the target Go/PostgreSQL/Temporal architecture.

**Tech Stack:** Node.js 24 ESM, `node:http`, `node:sqlite`, `node:test`, browser-native HTML/CSS/JavaScript.

## Global Constraints

- No container is required for development or tests.
- One mutable run has one lease owner per monotonically increasing `execution_epoch`.
- Events, state changes, and committed checkpoints are transactionally consistent.
- Large context is passed by immutable `context_ref_id`, not copied into run events.
- Tool side effects are classified per versioned operation contract.
- Unknown or non-idempotent external effects are never silently replayed.
- Phase one does not enforce cost/token budgets, exact causal asset impact, step importance, or dead-letter queues.
- Child-agent failures return to the parent decision path; platform safety rules remain mandatory.

---

## File Structure

- `src/db.js`: SQLite schema, transactions, and lifecycle.
- `src/errors.js`: typed domain errors and HTTP mapping.
- `src/context-store.js`: immutable context writes/reads and read audit.
- `src/tool-contracts.js`: operation contracts, validation, and built-in executors.
- `src/runtime.js`: run state machine, leases, tool execution, checkpoints, recovery, and event ledger.
- `src/api.js`: REST/SSE boundary and request validation.
- `src/server.js`: process composition and startup.
- `public/*`: operator console.
- `test/*.test.js`: unit, contract, replay, security, chaos, and end-to-end tests.

### Task 1: Persistence, Context Store, and Event Ledger

**Files:** Create `package.json`, `src/db.js`, `src/errors.js`, `src/context-store.js`, `test/context-store.test.js`.

**Interfaces:** Produces `createDatabase(path)`, `ContextStore.put/read/revoke`, and immutable event sequencing.

- [ ] Write tests proving content-addressed deduplication, immutable references, revocation, and `ASSET_READ` audit.
- [ ] Run `node --test test/context-store.test.js` and confirm failure because modules are missing.
- [ ] Implement the minimal schema and context service.
- [ ] Re-run the focused test and confirm it passes.

### Task 2: Versioned Tool Contracts and Safe Invocation

**Files:** Create `src/tool-contracts.js`, `test/tool-contracts.test.js`.

**Interfaces:** Produces `ToolRegistry.register/get/invoke`; consumes SQLite transactions and run fencing data.

- [ ] Write tests for operation-level `READ_ONLY/SIDE_EFFECT/UNKNOWN`, idempotency keys, conflict detection, and retry classification.
- [ ] Run the focused test and confirm the missing implementation failure.
- [ ] Implement contract registration and deterministic built-in operations (`echo`, `counter.increment`, `effect.opaque`).
- [ ] Re-run the focused test and confirm it passes.

### Task 3: Run State Machine, Lease Fencing, and Checkpoints

**Files:** Create `src/runtime.js`, `test/runtime.test.js`, `test/replay.test.js`.

**Interfaces:** Produces `Runtime.createRun/acquireLease/start/readContext/invokeTool/checkpoint/fail/complete/recover`.

- [ ] Write tests for legal transitions, stale epoch rejection, atomic checkpoint/event/state commits, anchor-plus-delta recovery, and revoked-context rollback.
- [ ] Run focused tests and confirm missing behavior failures.
- [ ] Implement the state machine and transaction boundaries.
- [ ] Re-run focused tests and confirm they pass.

### Task 4: Control API, SSE, and End-to-End Flow

**Files:** Create `src/api.js`, `src/server.js`, `test/api.test.js`, `test/e2e.test.js`.

**Interfaces:** Produces `/api/health`, `/api/contexts`, `/api/tools`, `/api/runs`, run actions, timeline, and `/api/events` SSE.

- [ ] Write real HTTP tests for validation, lifecycle actions, stale fencing, tool effects, recovery, and event replay from `after_seq`.
- [ ] Run focused tests and confirm routes are unavailable.
- [ ] Implement HTTP composition and stable JSON error envelopes.
- [ ] Re-run focused tests and confirm they pass.

### Task 5: Operator Console and Local Operations

**Files:** Create `public/index.html`, `public/app.js`, `public/styles.css`, `scripts/dev.js`, `README.md`, `.gitignore`.

**Interfaces:** Consumes the REST/SSE API and provides run creation, action controls, status cards, and an audit timeline.

- [ ] Write a server-level test proving console assets and SPA entry are served with safe paths.
- [ ] Run the focused test and confirm asset routes fail.
- [ ] Implement the responsive console and startup script.
- [ ] Re-run the focused test and confirm it passes.

### Task 6: Security, Chaos, and Full Verification

**Files:** Create `test/security.test.js`, `test/chaos.test.js`; modify implementation only for failures exposed by tests.

**Interfaces:** Verifies all public and recovery boundaries.

- [ ] Write tests for malformed JSON, body limits, path traversal, stale-worker writes, duplicate commands, unknown effects, and database restart recovery.
- [ ] Run tests and confirm each newly specified behavior fails for the intended reason.
- [ ] Implement the smallest fixes required by failing tests.
- [ ] Run `npm test`, `npm run check`, and a live smoke test against `/api/health`.
- [ ] Review every global constraint against tests and documentation; package the validated project for delivery.
