import asyncio
import importlib.util
import operator
import os
import uuid
from asyncio import Lock
from datetime import UTC, datetime
from importlib.metadata import version
from types import SimpleNamespace
from typing import Annotated, TypedDict, cast

import pytest
from langgraph.graph import END, START, StateGraph  # pyright: ignore[reportMissingTypeStubs]
from sqlalchemy.engine import make_url

from packages.runtime_contracts import (
    AgentPackageRef,
    ErrorCode,
    Principal,
    RuntimeSession,
    RuntimeType,
    SessionStatus,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.repositories import RuntimeRepositoryError
from packages.session_manager.checkpointer import (
    CheckpointAudit,
    CheckpointerDependencyUnavailable,
    FencedAsyncPostgresSaver,
    FencedCheckpointContext,
    FencedCheckpointWriter,
    PsycopgCheckpointTransaction,
    async_postgres_saver_available,
    checkpoint_config,
    checkpoint_write_config,
    create_async_postgres_saver,
)


def _session() -> RuntimeSession:
    timestamp = datetime(2026, 8, 20, 8, 0, tzinfo=UTC)
    return RuntimeSession(
        session_id="session-a",
        thread_id="session-a",
        tenant_id="tenant-a",
        user_id="user-a",
        package=AgentPackageRef(
            tenant_id="tenant-a",
            agent_id="agent-a",
            version="0.1.0",
            digest=f"sha256:{'a' * 64}",
            runtime_type=RuntimeType.DEEPAGENTS,
            sdk_version="0.7.7",
        ),
        status=SessionStatus.OPEN,
        revision=4,
        execution_epoch=7,
        active_execution_id="execution-a",
        last_checkpoint_id="checkpoint-previous",
        last_event_sequence=9,
        created_at=timestamp,
        updated_at=timestamp,
    )


def _principal() -> Principal:
    return Principal(
        tenant_id="tenant-a",
        user_id="user-a",
        actor_id="user-a",
        worker_id="worker-a",
    )


def test_checkpoint_audit_hashes_only_canonical_enterprise_metadata() -> None:
    audit = CheckpointAudit.create(
        runtime_session=_session(),
        execution_id="execution-a",
        execution_epoch=7,
        checkpoint_id="checkpoint-a",
    )

    assert audit.integrity_hash == (
        "sha256:f8ebabf343ee6961109c37708dfff1991225cb8fb82f86d36965970fa06cfa9e"
    )
    assert audit.metadata == {
        "schema_version": "runtime.checkpoint-audit.v1",
        "tenant_id": "tenant-a",
        "session_id": "session-a",
        "thread_id": "session-a",
        "execution_id": "execution-a",
        "execution_epoch": 7,
        "package_digest": f"sha256:{'a' * 64}",
        "checkpoint_id": "checkpoint-a",
        "integrity_hash": audit.integrity_hash,
    }
    assert "messages" not in audit.metadata
    assert "state" not in audit.metadata


def test_checkpoint_config_uses_session_id_as_thread_and_pins_audited_head() -> None:
    runtime_session = _session()
    config = checkpoint_config(runtime_session)

    assert config == {
        "configurable": {
            "thread_id": "session-a",
            "checkpoint_ns": "tenant:tenant-a",
            "checkpoint_id": "checkpoint-previous",
        }
    }


def test_checkpoint_namespace_prevents_cross_tenant_thread_collision() -> None:
    tenant_a = _session()
    tenant_b = tenant_a.model_copy(
        update={
            "tenant_id": "tenant-b",
            "user_id": "user-b",
            "package": tenant_a.package.model_copy(update={"tenant_id": "tenant-b"}),
        }
    )

    assert checkpoint_config(tenant_a)["configurable"]["thread_id"] == "session-a"
    assert checkpoint_config(tenant_b)["configurable"]["thread_id"] == "session-a"
    assert checkpoint_config(tenant_a)["configurable"]["checkpoint_ns"] == (
        "tenant:tenant-a"
    )
    assert checkpoint_config(tenant_b)["configurable"]["checkpoint_ns"] == (
        "tenant:tenant-b"
    )


class _CheckpointTransaction:
    def __init__(self, *, stale: bool) -> None:
        self.stale = stale
        self.entered = False
        self.storage: list[tuple[dict[str, object], dict[str, object]]] = []

    async def __aenter__(self) -> "_CheckpointTransaction":
        self.entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        self.entered = False

    async def verify(self, context: FencedCheckpointContext) -> None:
        assert self.entered
        if self.stale:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message=f"epoch {context.execution_epoch} is stale",
                )
            )

    async def aput(
        self,
        config: dict[str, object],
        checkpoint: dict[str, object],
        metadata: dict[str, object],
        new_versions: dict[str, object],
        audit: CheckpointAudit,
    ) -> dict[str, object]:
        del new_versions
        assert self.entered
        self.storage.append((checkpoint, metadata))
        return {
            "configurable": {
                "thread_id": audit.thread_id,
                "checkpoint_ns": config["configurable"]["checkpoint_ns"],  # type: ignore[index]
                "checkpoint_id": audit.checkpoint_id,
            }
        }


def test_fenced_writer_rejects_stale_epoch_before_checkpoint_mutation() -> None:
    async def scenario() -> None:
        runtime_session = _session()
        config = checkpoint_write_config(
            runtime_session,
            _principal(),
            execution_id="execution-a",
            execution_epoch=7,
        )
        context = FencedCheckpointContext.from_config(config)
        transaction = _CheckpointTransaction(stale=True)
        writer = FencedCheckpointWriter(lambda _context: transaction)  # type: ignore[arg-type]

        with pytest.raises(RuntimeRepositoryError) as fenced:
            await writer.aput(
                context=context,
                config=config,
                checkpoint={"id": "checkpoint-a", "channel_values": {}},  # type: ignore[arg-type]
                metadata={},
                new_versions={},
            )
        assert fenced.value.error.code is ErrorCode.EXECUTION_FENCED
        assert transaction.storage == []

    asyncio.run(scenario())


def test_fenced_writer_persists_audit_inside_verified_transaction() -> None:
    async def scenario() -> None:
        runtime_session = _session()
        config = checkpoint_write_config(
            runtime_session,
            _principal(),
            execution_id="execution-a",
            execution_epoch=7,
        )
        context = FencedCheckpointContext.from_config(config)
        transaction = _CheckpointTransaction(stale=False)
        writer = FencedCheckpointWriter(lambda _context: transaction)  # type: ignore[arg-type]

        result = await writer.aput(
            context=context,
            config=config,
            checkpoint={"id": "checkpoint-a", "channel_values": {}},  # type: ignore[arg-type]
            metadata={"source": "loop"},
            new_versions={},
        )

        assert result["configurable"]["checkpoint_id"] == "checkpoint-a"  # type: ignore[index]
        assert transaction.storage[0][1]["enterprise_audit"]["integrity_hash"] == (  # type: ignore[index]
            "sha256:f8ebabf343ee6961109c37708dfff1991225cb8fb82f86d36965970fa06cfa9e"
        )

    asyncio.run(scenario())


class _PsycopgCursorDouble:
    def __init__(self, connection: "_PsycopgConnectionDouble") -> None:
        self.connection = connection
        self.last_query = ""

    async def __aenter__(self) -> "_PsycopgCursorDouble":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        return None

    async def execute(self, query: str, parameters: object = None) -> None:
        self.last_query = " ".join(query.split())
        self.connection.executions.append((self.last_query, parameters))
        if "FROM runtime_session AS s" in self.last_query:
            self.connection.operations.append("verify_epoch_for_update")
        elif self.last_query.startswith("UPDATE runtime_session"):
            self.connection.operations.append("update_checkpoint_head")
        elif "pg_advisory_xact_lock" in self.last_query:
            self.connection.operations.append("advisory_lock")
        else:
            self.connection.operations.append("serializable")

    async def fetchone(self) -> tuple[int] | None:
        if "FROM runtime_session AS s" in self.last_query:
            return (1,) if self.connection.current_epoch else None
        if self.last_query.startswith("UPDATE runtime_session"):
            return (1,)
        return None


class _PsycopgTransactionDouble:
    def __init__(self, operations: list[str]) -> None:
        self.operations = operations

    async def __aenter__(self) -> None:
        self.operations.append("transaction_begin")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        self.operations.append("transaction_commit" if exc_type is None else "rollback")


class _PsycopgConnectionDouble:
    def __init__(self, *, current_epoch: bool) -> None:
        self.current_epoch = current_epoch
        self.operations: list[str] = []
        self.executions: list[tuple[str, object]] = []

    def transaction(self) -> _PsycopgTransactionDouble:
        return _PsycopgTransactionDouble(self.operations)

    def cursor(self) -> _PsycopgCursorDouble:
        return _PsycopgCursorDouble(self)


class _RawSaverDouble:
    def __init__(self, operations: list[str]) -> None:
        self.operations = operations

    async def aput(
        self,
        config: dict[str, object],
        checkpoint: dict[str, object],
        metadata: dict[str, object],
        new_versions: dict[str, object],
    ) -> dict[str, object]:
        del metadata, new_versions
        self.operations.append("official_saver_write")
        return {
            "configurable": {
                "thread_id": config["configurable"]["thread_id"],  # type: ignore[index]
                "checkpoint_ns": config["configurable"]["checkpoint_ns"],  # type: ignore[index]
                "checkpoint_id": checkpoint["id"],
            }
        }


def test_production_transaction_verifies_fence_before_official_saver_write() -> None:
    async def scenario(
        current_epoch: bool,
    ) -> tuple[list[str], list[tuple[str, object]], bool]:
        runtime_session = _session()
        config = checkpoint_write_config(
            runtime_session,
            _principal(),
            execution_id="execution-a",
            execution_epoch=7,
        )
        context = FencedCheckpointContext.from_config(config)
        connection = _PsycopgConnectionDouble(current_epoch=current_epoch)
        raw_saver = _RawSaverDouble(connection.operations)
        writer = FencedCheckpointWriter(
            lambda current: PsycopgCheckpointTransaction(
                raw_saver,
                connection,
                current,
                Lock(),
            )
        )
        wrote = True
        try:
            await writer.aput(
                context=context,
                config=config,
                checkpoint={"id": "checkpoint-a", "channel_values": {}},
                metadata={},
                new_versions={},
            )
        except RuntimeRepositoryError as error:
            assert error.error.code is ErrorCode.EXECUTION_FENCED
            wrote = False
        return connection.operations, connection.executions, wrote

    current_operations, current_executions, current_wrote = asyncio.run(scenario(True))
    stale_operations, _, stale_wrote = asyncio.run(scenario(False))

    assert current_wrote is True
    assert current_operations == [
        "transaction_begin",
        "serializable",
        "advisory_lock",
        "verify_epoch_for_update",
        "official_saver_write",
        "update_checkpoint_head",
        "transaction_commit",
    ]
    assert stale_wrote is False
    assert "official_saver_write" not in stale_operations
    assert stale_operations[-1] == "rollback"
    verify_parameters = next(
        parameters
        for query, parameters in current_executions
        if "FROM runtime_session AS s" in query
    )
    assert verify_parameters == (
        "tenant-a",
        "user-a",
        "session-a",
        "session-a",
        f"sha256:{'a' * 64}",
        "execution-a",
        7,
        "worker-a",
    )


def test_async_postgres_saver_dependency_is_production_installable() -> None:
    assert version("langgraph-checkpoint-postgres") == "3.1.2"
    assert version("psycopg").startswith("3.")
    assert async_postgres_saver_available() is True
    saver_context = create_async_postgres_saver(
        "postgresql://runtime:runtime@localhost/runtime"
    )
    assert saver_context is not None


def test_missing_async_postgres_saver_dependency_fails_without_memory_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        def missing_spec(_name: str) -> None:
            return None

        monkeypatch.setattr(importlib.util, "find_spec", missing_spec)
        with pytest.raises(CheckpointerDependencyUnavailable):
            async with create_async_postgres_saver(
                "postgresql://runtime:runtime@localhost/runtime"
            ):
                raise AssertionError("unavailable saver context must not open")

    asyncio.run(scenario())


def test_fenced_saver_rejects_raw_saver_on_a_different_connection() -> None:
    raw_saver = SimpleNamespace(serde=None, conn=object())

    with pytest.raises(ValueError, match="same psycopg connection"):
        FencedAsyncPostgresSaver(raw_saver, object())


class _ConversationState(TypedDict):
    history: Annotated[list[str], operator.add]


def _graph(checkpointer: object) -> object:
    builder = StateGraph(_ConversationState)

    def remember(state: _ConversationState) -> _ConversationState:
        return {"history": [f"seen:{state['history'][-1]}"]}

    builder.add_node("remember", remember)  # pyright: ignore[reportUnknownMemberType]
    builder.add_edge(START, "remember")
    builder.add_edge("remember", END)
    return builder.compile(checkpointer=cast(object, checkpointer))  # type: ignore[arg-type]


def _checkpoint_connection_string() -> str:
    database_url = os.getenv("RUNTIME_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip(
            "PostgreSQL integration dependency unavailable: set "
            "RUNTIME_TEST_DATABASE_URL=postgresql+asyncpg://..."
        )
    url = make_url(database_url)
    if not url.drivername.startswith("postgresql"):
        pytest.fail("RUNTIME_TEST_DATABASE_URL must be PostgreSQL; SQLite is unsupported")
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


async def _assert_worker_restart_continues_checkpoint(connection_string: str) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    thread_id = f"session-task8-{uuid.uuid4().hex}"
    config = {"configurable": {"thread_id": thread_id}}
    async with AsyncPostgresSaver.from_conn_string(connection_string) as first_saver:
        await first_saver.setup()
        first_graph = _graph(first_saver)
        first = await first_graph.ainvoke({"history": ["one"]}, config)  # type: ignore[attr-defined]
        second = await first_graph.ainvoke({"history": ["two"]}, config)  # type: ignore[attr-defined]
        assert first["history"] == ["one", "seen:one"]
        assert second["history"] == ["one", "seen:one", "two", "seen:two"]

    async with AsyncPostgresSaver.from_conn_string(connection_string) as restarted_saver:
        restarted_graph = _graph(restarted_saver)
        third = await restarted_graph.ainvoke({"history": ["three"]}, config)  # type: ignore[attr-defined]
        assert third["history"] == [
            "one",
            "seen:one",
            "two",
            "seen:two",
            "three",
            "seen:three",
        ]
        await restarted_saver.adelete_thread(thread_id)


def test_async_postgres_saver_continues_after_worker_graph_recreation() -> None:
    try:
        saver_spec = importlib.util.find_spec("langgraph.checkpoint.postgres.aio")
    except ModuleNotFoundError:
        saver_spec = None
    if saver_spec is None:
        pytest.skip(
            "AsyncPostgresSaver integration dependency unavailable: install "
            "langgraph-checkpoint-postgres and psycopg"
        )
    asyncio.run(_assert_worker_restart_continues_checkpoint(_checkpoint_connection_string()))
