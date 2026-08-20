# Agent Runtime Kernel Iteration 1 Hardening Design

## Purpose

Iteration 1 hardens the existing Node.js/SQLite runtime kernel before any real model or external Tool is connected. It preserves the modular-monolith and no-container development baseline while closing the eight P0 correctness and trust-boundary defects found during the second engineering review.

## Scope

The iteration must deliver these invariants:

1. A Context reference is opaque and unique across tenants while tenant-local `(content, classification)` deduplication remains deterministic.
2. Tool idempotency is scoped by tenant, operation, contract version, and idempotency key.
3. Every current and migrated Checkpoint is protected by a versioned integrity envelope covering immutable identity, lineage, event cursor, epoch, schema, state, status, and creation time.
4. A revoked-Context rollback materializes the full state at the safe head, invalidates the unsafe branch without deleting evidence, and fences the previous Worker.
5. `WAITING_MANUAL` and `RECOVERY_BLOCKED` cannot be escaped by ordinary lease acquisition or recovery.
6. Every recovery outcome performs a lease, epoch, and state-version compare-and-set before changing Run state or appending a Run event.
7. An external side effect is never executed before its durable intent and Outbox item commit. Ambiguous outcomes become `UNKNOWN` and cannot be automatically replayed.
8. HTTP tenant, actor, Worker, and takeover authority come only from an authenticated principal; caller-supplied identity fields never grant authority.

## Architecture

### Persistence and migrations

`src/db.js` owns forward-only, idempotent SQLite migrations. The Tool effect uniqueness constraint is rebuilt to include `contract_version`. New effect/outbox and integrity columns are added without deleting history. Legacy Checkpoint hashes are verified with the v1 formula and upgraded in place to integrity v2; upgrade establishes a new integrity baseline but does not claim to prove metadata that v1 never covered.

### Context references

`ContextStore.put` generates a random `ctx_<uuid>` only when the tenant-local unique tuple does not already exist. This prevents cross-tenant correlation and global primary-key collisions. Existing references remain readable and continue to deduplicate because the database tuple lookup runs before ID generation.

### Checkpoint integrity and active lineage

`src/checkpoint-integrity.js` defines the canonical v2 envelope. Runtime recovery first validates all Checkpoint rows and their matching `CHECKPOINT_COMMITTED` events, then reconstructs state by walking parent links from an explicit active head back to the nearest FULL anchor.

Revoked-Context reconciliation marks all Checkpoints at or after the unsafe read as `INVALIDATED_CONTEXT_REVOKED`, recalculates their legitimate v2 status hash, reconstructs the selected safe head including its FULL anchor and DELTAs, updates Run state, increments `execution_epoch`, clears the lease, and appends one rollback event in the same transaction. New Checkpoints only parent the latest active `COMMITTED` head.

### Recovery state machine

Ordinary lease acquisition accepts only `CREATED` or `RUNNING`; the dedicated recovery path may additionally continue `RECOVERING`. Both reject manual/blocked states before issuing a lease. Operator resolution is explicit and audited. Entering a manual or corruption gate increments the epoch and clears the old lease. All recovery terminal branches use a common fenced compare-and-set guarded by tenant, lease ID, lease owner, execution epoch, state version, and lease expiry.

### Effects and Outbox

Tool contracts explicitly select `LOCAL_TRANSACTIONAL` or `EXTERNAL_OUTBOX`; side-effect contracts default conservatively to `EXTERNAL_OUTBOX`, while the built-in counter declares `LOCAL_TRANSACTIONAL`.

For `EXTERNAL_OUTBOX`, one transaction writes `tool_effects(PREPARED)` and `tool_effect_outbox(READY)`. A second transaction marks dispatch. The executor then runs outside SQLite. Success commits `COMMITTED/DELIVERED`; explicit not-applied rejection commits `FAILED`; every other exception commits `UNKNOWN`. Repeating the same key returns the committed result or the durable terminal/pending state and never blindly invokes the adapter again. This is durable at-least-once intent handling, not a cross-system exactly-once claim.

Every Worker mutation begins with a transaction-local ledger disposition check. Durable `UNKNOWN` rows atomically fence the current epoch and put the Run in `WAITING_MANUAL`; an unacknowledged known `FAILED` row atomically fails the Run, or enters the manual gate while another effect is still active. `PREPARED` and `DISPATCHING` rows block concurrent Worker progress and terminal transitions. When a tenant-scoped idempotency key replays a non-committed effect from another Run, ToolRegistry invokes the current Run disposition inside the same lookup transaction before returning the replay error. These guards self-heal the unavoidable process-crash windows around external outcome finalization without redelivery.

### Trusted HTTP principal

`src/auth.js` resolves an opaque Bearer token to `{ tenantId, subjectId, workerId, permissions }`. `createApp` requires an injected authenticator. The ordinary server entry point fails closed without configured identities; only the dedicated development entry point explicitly enables the known local token. `x-tenant-id`, if supplied for correlation, must match the principal. Worker mutations require the authenticated Worker to match the lease. Force takeover and recovery require separate permissions and an audited reason.

## Completed review hardening

The implementation also closes the independent-review interleavings discovered after the initial design: HTTP revocation propagation plus recovery self-healing, idempotent/manual-gate-safe revocation reconciliation, recovery-start state-version snapshots, transaction-local Tool run fences, epoch-based external-dispatch fencing, atomic effect resolution plus audit, effect-ledger and cross-Run replay crash-window self-healing, concurrent terminal-effect gates, structural legacy-index migration, deterministic legacy execution modes, missing legacy Outbox reconstruction, and Checkpoint event tenant validation.

## Error handling

- Integrity corruption: `CHECKPOINT_CORRUPT`, Run becomes `RECOVERY_BLOCKED` only if the recovery lease is still current.
- Manual gate: `MANUAL_INTERVENTION_REQUIRED`; entering the gate fences the current Worker by incrementing epoch and clearing its lease.
- Ambiguous external outcome: `EFFECT_STATUS_UNKNOWN`; the effect persists as `UNKNOWN`, and the originating call or next Worker entry atomically moves the Run to `WAITING_MANUAL`.
- Explicit external rejection: `EFFECT_EXECUTION_FAILED`; the effect persists as `FAILED`, and the originating call or next Worker/lease entry atomically records `RUN_FAILED` unless another active effect first requires a manual gate.
- Identity mismatch or missing capability: `403`; missing/invalid token: `401`.
- Invalid cursors, TTLs, versions, booleans, or identity-bearing body fields: stable `400`/`403` domain errors, never an accidental `500`.

## Testing strategy

Every correction starts with a behavior test that fails on the current implementation. Coverage includes cross-tenant Context writes, version-isolated idempotency, metadata and event tampering, DELTA-head rollback, rollback-then-recover, stale recovery interleavings, manual-gate bypass, external success-then-timeout, restart with dispatch in flight, authenticated tenant/Worker binding, unauthorized takeover, actor spoofing, migrations, full HTTP lifecycle, and a live-process smoke test.

## Non-goals

No Kafka, Temporal, DLQ, Kubernetes, JWT framework, multi-node dispatcher, automatic compensation, cost budget, step-importance policy, model gateway, or microservice split is added in this iteration.
