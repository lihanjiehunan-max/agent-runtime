# Dependency Upgrade Procedure

## Frozen baseline

The release is locked to Python 3.12 and the following runtime-critical
versions:

| Dependency | Locked requirement | Evidence |
|---|---|---|
| `deepagents` | `==0.7.7` | `uv.lock`, contract suite |
| `langgraph-checkpoint-postgres` | `==3.1.2` | `uv.lock`, checkpointer suite |
| Python | `>=3.12,<3.13` | `pyproject.toml` |
| Agent package | `agent-metric-query:0.1.0` | digest and checksums |
| Alembic head | `0002_trace_projection_event_index` | migration directory |

Current evidence hashes:

```text
uv.lock: d194d2dcc6308998b9ad8352f519303e8c5c2e329d6c31686932943be0ba0d53
agents/agent-metric-query/checksums.txt: 17e78ea023b77e5c351f7de9244277d469e69555cf98e55eaf9e49db309949fc
agent package digest: sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04
```

## Upgrade steps

1. Create a branch and record the current lock hash, package digest, migration
   head, and the active console/API contract.
2. Change only the intended requirement in `pyproject.toml`, then run
   `uv lock` and record the new `sha256sum uv.lock`.
3. Review transitive changes. Do not accept a lock refresh that changes the
   Deep Agents SDK, LangGraph checkpoint package, or provider client without an
   explicit compatibility decision.
4. Run the focused regression suites:

   ```bash
   UV_CACHE_DIR=/tmp/runtime-mvp-uv-cache-upgrade \
     uv run pytest \
       tests/unit/package_loader \
       tests/unit/test_agent_factory.py \
       tests/integration/test_session_checkpointer.py \
       tests/integration/test_trace_projection.py \
       tests/integration/test_stream_execution.py \
       tests/recovery/test_worker_restart.py \
       tests/acceptance/test_three_turn_metric_session.py -q -rs
   ```

5. Run `uv run ruff check .`, `uv run pyright`, and `git diff --check`.
6. Rebuild the API, worker, and console artifacts. Run the compose health
   checks and the deterministic acceptance contract before any cutover.
7. Update this file's version/hash evidence in the same reviewed change.

## Database and package compatibility

Dependency upgrades do not authorize a mutable Session-history migration.
LangGraph Checkpointer remains the graph-state authority. If a dependency
requires schema changes, add a forward migration, run it against a copy, and
record a reversible rollback plan. Never rewrite the legacy Node SQLite file.

The acceptance package digest is an immutable identity. A package content
change requires a new digest and a new package version; it must not be silently
associated with an existing Session.

## Rollback

Rollback is an artifact rollback: restore the prior lockfile, images, and
deployment units, then route traffic back to the prior API/console. Do not
delete Python package, Session, Execution, event, trace, or checkpoint rows.
Do not downgrade a database schema in place unless the migration explicitly
defines and has tested a safe downgrade. Preserve the newer rows for forward
recovery.

## Evidence boundary

Task 13's reported 40-Session measurements are deterministic local evidence;
they do not prove a real Runtime/Redis/PostgreSQL/Model/Tool Gateway load run.
The optional Locust command was not executed because the CLI is not installed.
Record any live run separately with its environment gate and service versions.
