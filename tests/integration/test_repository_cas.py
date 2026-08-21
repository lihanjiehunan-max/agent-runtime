import asyncio
import inspect
import os
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.sql import ClauseElement

from packages.runtime_contracts import (
    AgentPackageRef,
    ErrorCode,
    ExecutionStatus,
    Principal,
    RuntimeEvent,
    RuntimeSession,
    RuntimeType,
    SessionStatus,
)
from packages.runtime_persistence.models import RuntimeSessionRow
from packages.runtime_persistence.repositories import (
    EventRepository,
    ExecutionRepository,
    PackageRepository,
    RuntimeRepositoryError,
    SessionRepository,
    _build_begin_session_update,  # pyright: ignore[reportPrivateUsage]
    _build_complete_session_update,  # pyright: ignore[reportPrivateUsage]
    session_advisory_lock_key,
)


@dataclass(frozen=True)
class _ExecutionLeaseProof:
    tenant_id: str
    session_id: str
    owner_id: str
    active: bool = True


def _lease(principal: Principal, session_id: str) -> _ExecutionLeaseProof:
    assert principal.worker_id is not None
    return _ExecutionLeaseProof(
        tenant_id=principal.tenant_id,
        session_id=session_id,
        owner_id=principal.worker_id,
    )


def _compiled_sql(statement: ClauseElement) -> str:
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_begin_execution_query_is_tenant_scoped_and_requires_idle_open_session() -> None:
    sql = _compiled_sql(
        _build_begin_session_update(
            tenant_id="tenant-a",
            session_id="session-a",
            execution_id="execution-a",
            updated_at=datetime(2026, 8, 20, tzinfo=UTC),
        )
    )

    assert "runtime_session.tenant_id = 'tenant-a'" in sql
    assert "runtime_session.session_id = 'session-a'" in sql
    assert "runtime_session.status = 'open'" in sql
    assert "runtime_session.active_execution_id IS NULL" in sql
    assert "execution_epoch=(runtime_session.execution_epoch + 1)" in sql
    assert "revision=(runtime_session.revision + 1)" in sql


def test_begin_execution_requires_coordination_lease_capability() -> None:
    lease_parameter = inspect.signature(
        ExecutionRepository.begin_execution
    ).parameters["lease"]

    assert lease_parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert lease_parameter.default is inspect.Parameter.empty


def test_begin_execution_rejects_mismatched_lease_before_database_access() -> None:
    async def scenario() -> None:
        session = AsyncSession()
        principal = Principal(
            tenant_id="tenant-a",
            user_id="user-a",
            actor_id="user-a",
            worker_id="worker-a",
        )
        try:
            with pytest.raises(RuntimeRepositoryError) as mismatch:
                await ExecutionRepository(session).begin_execution(
                    "session-a",
                    "execution-a",
                    principal,
                    lease=_ExecutionLeaseProof(
                        tenant_id="tenant-a",
                        session_id="session-a",
                        owner_id="worker-b",
                    ),
                )
            assert mismatch.value.error.code is ErrorCode.TENANT_IDENTITY_MISMATCH
        finally:
            await session.close()

    asyncio.run(scenario())


def test_session_advisory_lock_identity_is_stable_and_tenant_scoped() -> None:
    first = session_advisory_lock_key("tenant-a", "session-a")

    assert first == session_advisory_lock_key("tenant-a", "session-a")
    assert first != session_advisory_lock_key("tenant-b", "session-a")
    assert first != session_advisory_lock_key("tenant-a", "session-b")
    assert -(2**63) <= first < 2**63


def test_completion_query_guards_tenant_execution_id_and_epoch() -> None:
    sql = _compiled_sql(
        _build_complete_session_update(
            tenant_id="tenant-a",
            session_id="session-a",
            execution_id="execution-a",
            execution_epoch=7,
            updated_at=datetime(2026, 8, 20, tzinfo=UTC),
        )
    )

    assert "runtime_session.tenant_id = 'tenant-a'" in sql
    assert "runtime_session.session_id = 'session-a'" in sql
    assert "runtime_session.active_execution_id = 'execution-a'" in sql
    assert "runtime_session.execution_epoch = 7" in sql


async def _assert_cross_tenant_package_write_is_rejected() -> None:
    session = AsyncSession()
    try:
        package = AgentPackageRef(
            tenant_id="tenant-a",
            agent_id="agent-a",
            version="0.1.0",
            digest=f"sha256:{'a' * 64}",
            runtime_type=RuntimeType.DEEPAGENTS,
            sdk_version="0.7.7",
        )
        other_tenant = Principal(
            tenant_id="tenant-b",
            user_id="user-b",
            actor_id="actor-b",
        )

        with pytest.raises(RuntimeRepositoryError) as mismatch:
            await PackageRepository(session).add(package, other_tenant)
        assert mismatch.value.error.code is ErrorCode.TENANT_IDENTITY_MISMATCH
    finally:
        await session.close()


def test_repository_rejects_cross_tenant_identity_before_database_access() -> None:
    asyncio.run(_assert_cross_tenant_package_write_is_rejected())


def test_session_repository_rejects_thread_identity_mismatch_before_database_access() -> None:
    async def scenario() -> None:
        session = AsyncSession()
        try:
            timestamp = datetime(2026, 8, 20, tzinfo=UTC)
            runtime_session = RuntimeSession(
                session_id="session-a",
                thread_id="thread-other",
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
                revision=0,
                execution_epoch=0,
                active_execution_id=None,
                last_checkpoint_id=None,
                last_event_sequence=0,
                created_at=timestamp,
                updated_at=timestamp,
            )
            principal = Principal(
                tenant_id="tenant-a",
                user_id="user-a",
                actor_id="user-a",
            )
            with pytest.raises(RuntimeRepositoryError) as mismatch:
                await SessionRepository(session).add(runtime_session, principal)
            assert mismatch.value.error.code is ErrorCode.TENANT_IDENTITY_MISMATCH
        finally:
            await session.close()

    asyncio.run(scenario())


def _event(
    *,
    package_tenant_id: str,
    worker_id: str,
    event_id: str = "event-a",
    execution_id: str = "execution-a",
    sequence: int = 1,
    event_type: str = "execution.started",
    package_version: str = "0.1.0",
    package_digest: str | None = None,
) -> RuntimeEvent:
    return RuntimeEvent(
        schema_version="runtime.event.v1",
        event_id=event_id,
        sequence=sequence,
        occurred_at=datetime(2026, 8, 20, tzinfo=UTC),
        tenant_id="tenant-a",
        trace_id="trace-a",
        span_id="span-a",
        parent_span_id=None,
        session_id="session-a",
        execution_id=execution_id,
        package=AgentPackageRef(
            tenant_id=package_tenant_id,
            agent_id="agent-a",
            version=package_version,
            digest=package_digest or f"sha256:{'a' * 64}",
            runtime_type=RuntimeType.DEEPAGENTS,
            sdk_version="0.7.7",
        ),
        worker_id=worker_id,
        sdk_version="0.7.7",
        event_type=event_type,
        phase="execution",
        duration_ms=None,
        payload={},
        payload_ref=None,
    )


async def _assert_event_identity_is_rejected(event: RuntimeEvent) -> None:
    session = AsyncSession()
    try:
        principal = Principal(
            tenant_id="tenant-a",
            user_id="user-a",
            actor_id="actor-a",
            worker_id="worker-a",
        )
        with pytest.raises(RuntimeRepositoryError) as mismatch:
            await EventRepository(session).append(event, principal, execution_epoch=1)
        assert mismatch.value.error.code is ErrorCode.TENANT_IDENTITY_MISMATCH
    finally:
        await session.close()


def test_event_repository_rejects_cross_tenant_package_identity() -> None:
    asyncio.run(
        _assert_event_identity_is_rejected(
            _event(package_tenant_id="tenant-b", worker_id="worker-a")
        )
    )


def test_event_repository_rejects_unverified_worker_identity() -> None:
    asyncio.run(
        _assert_event_identity_is_rejected(
            _event(package_tenant_id="tenant-a", worker_id="worker-b")
        )
    )


class _RecordingTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        return None


class _RecordingSession:
    statement: ClauseElement | None = None

    def begin(self) -> _RecordingTransaction:
        return _RecordingTransaction()

    async def scalar(self, statement: ClauseElement) -> int:
        self.statement = statement
        return 1

    def add(self, instance: object) -> None:
        del instance


class _BeginRecordingSession(_RecordingSession):
    def __init__(self) -> None:
        self.operations: list[str] = []

    async def execute(self, statement: ClauseElement) -> object:
        self.operations.append(str(statement))
        return object()

    async def scalar(self, statement: ClauseElement) -> int:
        self.operations.append(str(statement))
        return 1


def test_begin_execution_takes_session_advisory_lock_before_epoch_cas() -> None:
    async def scenario() -> None:
        recording_session = _BeginRecordingSession()
        principal = Principal(
            tenant_id="tenant-a",
            user_id="user-a",
            actor_id="user-a",
            worker_id="worker-a",
        )

        epoch = await ExecutionRepository(
            cast(AsyncSession, recording_session)
        ).begin_execution(
            "session-a",
            "execution-a",
            principal,
            lease=_lease(principal, "session-a"),
        )

        assert epoch == 1
        assert "pg_advisory_xact_lock" in recording_session.operations[0]
        assert recording_session.operations[1].startswith("UPDATE runtime_session")

    asyncio.run(scenario())


async def _assert_event_cas_binds_persisted_identity() -> None:
    recording_session = _RecordingSession()
    principal = Principal(
        tenant_id="tenant-a",
        user_id="user-a",
        actor_id="actor-a",
        worker_id="worker-a",
    )

    await EventRepository(cast(AsyncSession, recording_session)).append(
        _event(package_tenant_id="tenant-a", worker_id="worker-a"),
        principal,
        execution_epoch=7,
    )

    assert recording_session.statement is not None
    sql = _compiled_sql(recording_session.statement)
    assert "runtime_session.agent_id = 'agent-a'" in sql
    assert "runtime_session.package_version = '0.1.0'" in sql
    assert f"runtime_session.package_digest = 'sha256:{'a' * 64}'" in sql
    assert "runtime_execution.worker_id = 'worker-a'" in sql
    assert "runtime_session.execution_epoch = 7" in sql
    assert (
        "runtime_execution.execution_epoch = runtime_session.execution_epoch" in sql
    )
    assert "runtime_execution.session_id = runtime_session.session_id" in sql
    assert (
        "runtime_execution.execution_id = runtime_session.active_execution_id" in sql
    )


def test_event_append_cas_binds_persisted_package_and_worker_identity() -> None:
    asyncio.run(_assert_event_cas_binds_persisted_identity())


class _TerminalRecordingSession:
    def __init__(self, *, execution_status: str = "running") -> None:
        self.transaction_count = 0
        self.scalar_count = 0
        self.added: list[object] = []
        self.runtime_session = SimpleNamespace(
            tenant_id="tenant-a",
            session_id="session-a",
            agent_id="agent-a",
            package_version="0.1.0",
            package_digest=f"sha256:{'a' * 64}",
            active_execution_id="execution-a",
            execution_epoch=7,
            last_event_sequence=2,
            revision=4,
            updated_at=datetime(2026, 8, 20, tzinfo=UTC),
        )
        self.runtime_execution = SimpleNamespace(
            tenant_id="tenant-a",
            session_id="session-a",
            execution_id="execution-a",
            execution_epoch=7,
            worker_id="worker-a",
            status=execution_status,
            completed_at=None,
            error=None,
        )

    def begin(self) -> _RecordingTransaction:
        self.transaction_count += 1
        return _RecordingTransaction()

    async def scalar(self, _statement: ClauseElement) -> object:
        self.scalar_count += 1
        if self.scalar_count == 1:
            return self.runtime_session
        return self.runtime_execution

    def add(self, instance: object) -> None:
        self.added.append(instance)


def test_terminal_append_releases_session_and_updates_execution_in_one_transaction() -> None:
    async def scenario() -> None:
        session = _TerminalRecordingSession()
        principal = Principal(
            tenant_id="tenant-a",
            user_id="user-a",
            actor_id="user-a",
            worker_id="worker-a",
        )

        event = await EventRepository(cast(AsyncSession, session)).append_terminal(
            lambda sequence: _event(
                package_tenant_id="tenant-a",
                worker_id="worker-a",
                event_id="event-terminal",
                execution_id="execution-a",
                sequence=sequence,
                event_type="execution.succeeded",
            ),
            principal,
            execution_epoch=7,
            status=ExecutionStatus.SUCCEEDED,
        )

        assert event.sequence == 3
        assert session.transaction_count == 1
        assert session.runtime_execution.status == ExecutionStatus.SUCCEEDED.value
        assert session.runtime_session.active_execution_id is None
        assert session.runtime_session.last_event_sequence == 3
        assert len(session.added) == 1

    asyncio.run(scenario())


def test_terminal_append_rejects_persisted_terminal_status_without_writing_again() -> None:
    async def scenario() -> None:
        session = _TerminalRecordingSession(execution_status="succeeded")
        principal = Principal(
            tenant_id="tenant-a",
            user_id="user-a",
            actor_id="user-a",
            worker_id="worker-a",
        )

        with pytest.raises(RuntimeRepositoryError) as duplicate:
            await EventRepository(cast(AsyncSession, session)).append_terminal(
                lambda sequence: _event(
                    package_tenant_id="tenant-a",
                    worker_id="worker-a",
                    event_id="event-duplicate",
                    execution_id="execution-a",
                    sequence=sequence,
                    event_type="execution.succeeded",
                ),
                principal,
                execution_epoch=7,
                status=ExecutionStatus.SUCCEEDED,
            )

        assert duplicate.value.error.code is ErrorCode.EXECUTION_FENCED
        assert session.runtime_session.active_execution_id == "execution-a"
        assert session.added == []

    asyncio.run(scenario())


def _postgres_url() -> str:
    database_url = os.getenv("RUNTIME_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip(
            "PostgreSQL integration dependency unavailable: set "
            "RUNTIME_TEST_DATABASE_URL=postgresql+asyncpg://..."
        )
    url = make_url(database_url)
    if url.drivername != "postgresql+asyncpg":
        pytest.fail("RUNTIME_TEST_DATABASE_URL must use postgresql+asyncpg; SQLite is unsupported")
    return database_url


def _upgrade(connection: Connection) -> None:
    config = Config()
    config.set_main_option("script_location", "migrations")
    config.attributes["connection"] = connection
    command.upgrade(config, "head")


async def _schema_engine(
    database_url: str,
) -> AsyncGenerator[AsyncEngine, None]:
    administration_engine = create_async_engine(database_url)
    schema_name = f"runtime_task3_{uuid.uuid4().hex}"
    runtime_engine: AsyncEngine | None = None
    try:
        async with administration_engine.connect() as migration_connection:
            await migration_connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            await migration_connection.execute(
                text(f'SET search_path TO "{schema_name}"')
            )
            await migration_connection.commit()
            await migration_connection.run_sync(_upgrade)
        runtime_engine = create_async_engine(
            database_url,
            connect_args={"server_settings": {"search_path": schema_name}},
        )
        yield runtime_engine
    finally:
        if runtime_engine is not None:
            await runtime_engine.dispose()
        async with administration_engine.begin() as cleanup_connection:
            await cleanup_connection.execute(
                text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            )
        await administration_engine.dispose()


async def _assert_stale_epoch_is_fenced(database_url: str) -> None:
    engine_iterator = _schema_engine(database_url)
    engine = await anext(engine_iterator)
    try:
        worker_one_principal = Principal(
            tenant_id="tenant-a",
            user_id="user-a",
            actor_id="actor-a",
            worker_id="worker-one",
        )
        worker_two_principal = Principal(
            tenant_id="tenant-a",
            user_id="user-a",
            actor_id="actor-a",
            worker_id="worker-two",
        )
        package = AgentPackageRef(
            tenant_id=worker_one_principal.tenant_id,
            agent_id="agent-a",
            version="0.1.0",
            digest=f"sha256:{'a' * 64}",
            runtime_type=RuntimeType.DEEPAGENTS,
            sdk_version="0.7.7",
        )
        now = datetime.now(UTC)
        runtime_session = RuntimeSession(
            session_id="session-a",
            thread_id="session-a",
            tenant_id=worker_one_principal.tenant_id,
            user_id=worker_one_principal.user_id,
            package=package,
            status=SessionStatus.OPEN,
            revision=0,
            execution_epoch=0,
            active_execution_id=None,
            last_checkpoint_id=None,
            last_event_sequence=0,
            created_at=now,
            updated_at=now,
        )

        async with engine.connect() as setup_connection, AsyncSession(
            bind=setup_connection, expire_on_commit=False
        ) as setup_session:
            await PackageRepository(setup_session).add(
                package, worker_one_principal
            )
            await SessionRepository(setup_session).add(
                runtime_session, worker_one_principal
            )

        async with (
            engine.connect() as worker_one_connection,
            engine.connect() as worker_two_connection,
            AsyncSession(
                bind=worker_one_connection, expire_on_commit=False
            ) as worker_one_session,
            AsyncSession(
                bind=worker_two_connection, expire_on_commit=False
            ) as worker_two_session,
        ):
            worker_one_executions = ExecutionRepository(worker_one_session)
            worker_two_executions = ExecutionRepository(worker_two_session)

            epoch_one = await worker_one_executions.begin_execution(
                runtime_session.session_id,
                "execution-one",
                worker_one_principal,
                lease=_lease(worker_one_principal, runtime_session.session_id),
            )
            await worker_one_executions.complete_execution(
                runtime_session.session_id,
                "execution-one",
                worker_one_principal,
                epoch_one,
                ExecutionStatus.SUCCEEDED,
            )
            epoch_two = await worker_two_executions.begin_execution(
                runtime_session.session_id,
                "execution-two",
                worker_two_principal,
                lease=_lease(worker_two_principal, runtime_session.session_id),
            )

            assert epoch_two == epoch_one + 1
            with pytest.raises(RuntimeRepositoryError) as package_fenced:
                await EventRepository(worker_two_session).append(
                    _event(
                        event_id="event-wrong-package",
                        execution_id="execution-two",
                        package_tenant_id="tenant-a",
                        package_version="0.2.0",
                        package_digest=f"sha256:{'b' * 64}",
                        worker_id="worker-two",
                    ),
                    worker_two_principal,
                    execution_epoch=epoch_two,
                )
            assert package_fenced.value.error.code is ErrorCode.EXECUTION_FENCED

            with pytest.raises(RuntimeRepositoryError) as worker_fenced:
                await EventRepository(worker_one_session).append(
                    _event(
                        event_id="event-wrong-worker",
                        execution_id="execution-two",
                        package_tenant_id="tenant-a",
                        worker_id="worker-one",
                    ),
                    worker_one_principal,
                    execution_epoch=epoch_two,
                )
            assert worker_fenced.value.error.code is ErrorCode.EXECUTION_FENCED

            with pytest.raises(RuntimeRepositoryError) as fenced:
                await worker_one_executions.complete_execution(
                    runtime_session.session_id,
                    "execution-two",
                    worker_one_principal,
                    epoch_one,
                    ExecutionStatus.SUCCEEDED,
                )
            assert fenced.value.error.code is ErrorCode.EXECUTION_FENCED

        async with engine.connect() as observer_connection, AsyncSession(
            bind=observer_connection, expire_on_commit=False
        ) as observer_session:
            stored_session = await observer_session.scalar(
                select(RuntimeSessionRow).where(
                    RuntimeSessionRow.tenant_id
                    == worker_one_principal.tenant_id,
                    RuntimeSessionRow.session_id
                    == runtime_session.session_id,
                )
            )
            assert stored_session is not None
            assert stored_session.active_execution_id == "execution-two"
            assert stored_session.execution_epoch == epoch_two
    finally:
        await engine_iterator.aclose()


async def _assert_terminal_append_is_atomic(database_url: str) -> None:
    engine_iterator = _schema_engine(database_url)
    engine = await anext(engine_iterator)
    try:
        principal = Principal(
            tenant_id="tenant-a",
            user_id="user-a",
            actor_id="actor-a",
            worker_id="worker-a",
        )
        package = AgentPackageRef(
            tenant_id="tenant-a",
            agent_id="agent-a",
            version="0.1.0",
            digest=f"sha256:{'a' * 64}",
            runtime_type=RuntimeType.DEEPAGENTS,
            sdk_version="0.7.7",
        )
        now = datetime.now(UTC)
        runtime_session = RuntimeSession(
            session_id="session-terminal",
            thread_id="session-terminal",
            tenant_id="tenant-a",
            user_id="user-a",
            package=package,
            status=SessionStatus.OPEN,
            revision=0,
            execution_epoch=0,
            active_execution_id=None,
            last_checkpoint_id=None,
            last_event_sequence=0,
            created_at=now,
            updated_at=now,
        )

        async with engine.connect() as setup_connection, AsyncSession(
            bind=setup_connection, expire_on_commit=False
        ) as setup_session:
            await PackageRepository(setup_session).add(package, principal)
            await SessionRepository(setup_session).add(runtime_session, principal)

        async with engine.connect() as connection, AsyncSession(
            bind=connection, expire_on_commit=False
        ) as database_session:
            executions = ExecutionRepository(database_session)
            epoch = await executions.begin_execution(
                runtime_session.session_id,
                "execution-terminal",
                principal,
                lease=_lease(principal, runtime_session.session_id),
            )
            events = EventRepository(database_session)
            first = await events.append_terminal(
                lambda sequence: _event(
                    package_tenant_id="tenant-a",
                    worker_id="worker-a",
                    event_id="event-terminal",
                    execution_id="execution-terminal",
                    sequence=sequence,
                    event_type="execution.succeeded",
                ),
                principal,
                epoch,
                ExecutionStatus.SUCCEEDED,
            )

            persisted_execution = await executions.get("execution-terminal", principal)
            persisted_session = await SessionRepository(database_session).get(
                runtime_session.session_id,
                principal,
            )
            persisted_events = await events.list_after(
                runtime_session.session_id,
                0,
                principal,
            )
            assert first.sequence == 1
            assert persisted_execution is not None
            assert persisted_execution.status is ExecutionStatus.SUCCEEDED
            assert persisted_execution.completed_at is not None
            assert persisted_session is not None
            assert persisted_session.active_execution_id is None
            assert [event.event_type for event in persisted_events] == [
                "execution.succeeded"
            ]

            with pytest.raises(RuntimeRepositoryError) as duplicate:
                await events.append_terminal(
                    lambda sequence: _event(
                        package_tenant_id="tenant-a",
                        worker_id="worker-a",
                        event_id="event-terminal-duplicate",
                        execution_id="execution-terminal",
                        sequence=sequence,
                        event_type="execution.succeeded",
                    ),
                    principal,
                    epoch,
                    ExecutionStatus.SUCCEEDED,
                )
            assert duplicate.value.error.code is ErrorCode.EXECUTION_FENCED

            unchanged_execution = await executions.get("execution-terminal", principal)
            unchanged_session = await SessionRepository(database_session).get(
                runtime_session.session_id,
                principal,
            )
            unchanged_events = await events.list_after(
                runtime_session.session_id,
                0,
                principal,
            )
            assert unchanged_execution is not None
            assert unchanged_execution.status is ExecutionStatus.SUCCEEDED
            assert unchanged_session is not None
            assert unchanged_session.active_execution_id is None
            assert len(unchanged_events) == 1
    finally:
        await engine_iterator.aclose()


def test_stale_completion_changes_zero_rows_and_surfaces_execution_fenced() -> None:
    asyncio.run(_assert_stale_epoch_is_fenced(_postgres_url()))


def test_live_terminal_append_is_atomic_and_rejects_duplicate() -> None:
    asyncio.run(_assert_terminal_append_is_atomic(_postgres_url()))
