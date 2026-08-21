import asyncio
import os
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Protocol, cast

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from httpx import Response
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from apps.runtime_api.main import create_api
from packages.package_loader.service import PackageLoader
from packages.runtime_contracts import (
    AgentPackageRef,
    CreateSessionRequest,
    ErrorCode,
    ExecutionStatus,
    Principal,
    RuntimeSession,
    RuntimeType,
    SessionStatus,
)
from packages.runtime_persistence.repositories import (
    ExecutionLease,
    ExecutionRepository,
    PackageRepository,
)
from packages.session_manager.service import (
    LoadedPackage,
    PostgresPackageCatalog,
    PostgresSessionStore,
    SessionLifecycle,
    SessionManager,
    SessionManagerError,
    _build_activate_session_update,  # pyright: ignore[reportPrivateUsage]
    _build_close_session_update,  # pyright: ignore[reportPrivateUsage]
    session_lifecycle,
)


class _PackageCatalog:
    def __init__(self, package: AgentPackageRef) -> None:
        self.package = package

    async def get(
        self, agent_id: str, version: str, principal: Principal
    ) -> AgentPackageRef | None:
        if (
            principal.tenant_id != self.package.tenant_id
            or agent_id != self.package.agent_id
            or version != self.package.version
        ):
            return None
        return self.package


class _RouteClient(Protocol):
    def post(self, url: str, **kwargs: Any) -> Response: ...

    def get(self, url: str, **kwargs: Any) -> Response: ...


class _PackageLoader:
    def __init__(self, package: AgentPackageRef, ttl_minutes: int = 30) -> None:
        self.package = package
        self.ttl_minutes = ttl_minutes

    def load(self, agent_id: str, version: str) -> LoadedPackage:
        assert agent_id == self.package.agent_id
        assert version == self.package.version
        return cast(
            LoadedPackage,
            SimpleNamespace(
                reference=self.package,
                limits=SimpleNamespace(session_ttl_minutes=self.ttl_minutes),
            ),
        )


class _SessionStore:
    def __init__(self) -> None:
        self.sessions: dict[tuple[str, str], RuntimeSession] = {}

    async def add(
        self, runtime_session: RuntimeSession, principal: Principal
    ) -> RuntimeSession:
        self.sessions[(principal.tenant_id, runtime_session.session_id)] = runtime_session
        return runtime_session

    async def get(
        self, session_id: str, principal: Principal
    ) -> RuntimeSession | None:
        return self.sessions.get((principal.tenant_id, session_id))

    async def activate(
        self,
        runtime_session: RuntimeSession,
        principal: Principal,
        now: datetime,
    ) -> RuntimeSession | None:
        current = await self.get(runtime_session.session_id, principal)
        if (
            current is None
            or current.revision != runtime_session.revision
            or current.status is not SessionStatus.OPEN
            or current.active_execution_id is not None
        ):
            return None
        activated = current.model_copy(
            update={"revision": current.revision + 1, "updated_at": now}
        )
        self.sessions[(principal.tenant_id, current.session_id)] = activated
        return activated

    async def close(
        self,
        runtime_session: RuntimeSession,
        principal: Principal,
        now: datetime,
    ) -> RuntimeSession | None:
        current = await self.get(runtime_session.session_id, principal)
        if (
            current is None
            or current.revision != runtime_session.revision
            or current.status is not SessionStatus.OPEN
            or current.active_execution_id is not None
        ):
            return None
        closed = current.model_copy(
            update={
                "status": SessionStatus.CLOSED,
                "revision": current.revision + 1,
                "updated_at": now,
            }
        )
        self.sessions[(principal.tenant_id, current.session_id)] = closed
        return closed

    async def list_open_idle(self, principal: Principal) -> tuple[RuntimeSession, ...]:
        return tuple(
            runtime_session
            for (tenant_id, _), runtime_session in self.sessions.items()
            if tenant_id == principal.tenant_id
            and runtime_session.user_id == principal.user_id
            and runtime_session.status is SessionStatus.OPEN
            and runtime_session.active_execution_id is None
        )

    def force(self, runtime_session: RuntimeSession) -> None:
        self.sessions[(runtime_session.tenant_id, runtime_session.session_id)] = runtime_session


def _package(digest_character: str = "a") -> AgentPackageRef:
    return AgentPackageRef(
        tenant_id="tenant-a",
        agent_id="agent-a",
        version="0.1.0",
        digest=f"sha256:{digest_character * 64}",
        runtime_type=RuntimeType.DEEPAGENTS,
        sdk_version="0.7.7",
    )


def _principal(*, tenant_id: str = "tenant-a", user_id: str = "user-a") -> Principal:
    return Principal(
        tenant_id=tenant_id,
        user_id=user_id,
        actor_id=user_id,
    )


def _manager(
    now: list[datetime],
    *,
    package: AgentPackageRef | None = None,
    ttl_minutes: int = 30,
) -> tuple[SessionManager, _SessionStore, _PackageLoader]:
    authoritative_package = package or _package()
    store = _SessionStore()
    loader = _PackageLoader(authoritative_package, ttl_minutes)
    manager = SessionManager(
        _PackageCatalog(authoritative_package),
        store,
        loader,
        now=lambda: now[0],
        session_id_factory=lambda: "session-a",
    )
    return manager, store, loader


async def _create(manager: SessionManager, principal: Principal) -> RuntimeSession:
    return await manager.create_session(
        CreateSessionRequest(agent_id="agent-a", version="0.1.0"),
        principal,
    )


def test_session_lifecycle_is_created_active_executing_idle_closed() -> None:
    async def scenario() -> None:
        now = [datetime(2026, 8, 20, 8, 0, tzinfo=UTC)]
        principal = _principal()
        manager, store, _ = _manager(now)

        created = await _create(manager, principal)
        assert created.thread_id == created.session_id == "session-a"
        assert session_lifecycle(created) is SessionLifecycle.CREATED

        now[0] += timedelta(minutes=1)
        active = await manager.resume_session(created.session_id, principal)
        assert active.package.digest == created.package.digest
        assert session_lifecycle(active) is SessionLifecycle.ACTIVE

        executing = active.model_copy(
            update={
                "active_execution_id": "execution-a",
                "execution_epoch": 1,
                "revision": active.revision + 1,
            }
        )
        store.force(executing)
        fetched_executing = await manager.get_session("session-a", principal)
        assert fetched_executing is not None
        assert session_lifecycle(fetched_executing) is SessionLifecycle.EXECUTING

        idle = executing.model_copy(
            update={
                "active_execution_id": None,
                "revision": executing.revision + 1,
            }
        )
        store.force(idle)
        fetched_idle = await manager.get_session("session-a", principal)
        assert fetched_idle is not None
        assert session_lifecycle(fetched_idle) is SessionLifecycle.IDLE

        closed = await manager.close_session("session-a", principal)
        assert session_lifecycle(closed) is SessionLifecycle.CLOSED

    asyncio.run(scenario())


def test_expired_session_is_closed_and_rejected_without_reviving_it() -> None:
    async def scenario() -> None:
        now = [datetime(2026, 8, 20, 8, 0, tzinfo=UTC)]
        principal = _principal()
        manager, store, _ = _manager(now, ttl_minutes=5)
        created = await _create(manager, principal)

        now[0] += timedelta(minutes=5)
        with pytest.raises(SessionManagerError) as expired:
            await manager.resume_session(created.session_id, principal)
        assert expired.value.error.code is ErrorCode.SESSION_CLOSED
        persisted = await store.get(created.session_id, principal)
        assert persisted is not None
        assert persisted.status is SessionStatus.CLOSED

    asyncio.run(scenario())


def test_expired_read_returns_persisted_closed_session_and_close_is_idempotent() -> None:
    async def scenario() -> None:
        now = [datetime(2026, 8, 20, 8, 0, tzinfo=UTC)]
        principal = _principal()
        manager, store, _ = _manager(now, ttl_minutes=5)
        created = await _create(manager, principal)

        now[0] += timedelta(minutes=5)
        fetched = await manager.get_session(created.session_id, principal)
        assert fetched is not None
        assert fetched.status is SessionStatus.CLOSED
        assert (await store.get(created.session_id, principal)) == fetched
        assert await manager.close_session(created.session_id, principal) == fetched

    asyncio.run(scenario())


def test_default_session_ids_are_global_random_thread_identities() -> None:
    async def scenario() -> None:
        timestamp = datetime(2026, 8, 20, 8, 0, tzinfo=UTC)

        def manager_for(package: AgentPackageRef) -> SessionManager:
            return SessionManager(
                _PackageCatalog(package),
                _SessionStore(),
                _PackageLoader(package),
                now=lambda: timestamp,
            )

        package_a = _package()
        package_b = package_a.model_copy(update={"tenant_id": "tenant-b"})
        session_a = await _create(manager_for(package_a), _principal())
        session_b = await _create(
            manager_for(package_b),
            _principal(tenant_id="tenant-b", user_id="user-b"),
        )

        assert session_a.session_id.startswith("session_")
        assert session_b.session_id.startswith("session_")
        assert session_a.session_id != session_b.session_id
        assert session_a.thread_id == session_a.session_id
        assert session_b.thread_id == session_b.session_id

    asyncio.run(scenario())


def test_expire_sessions_closes_only_stale_idle_sessions() -> None:
    async def scenario() -> None:
        now = [datetime(2026, 8, 20, 8, 0, tzinfo=UTC)]
        principal = _principal()
        manager, store, _ = _manager(now, ttl_minutes=10)
        created = await _create(manager, principal)
        executing = created.model_copy(update={"active_execution_id": "execution-a"})
        store.force(executing)

        now[0] += timedelta(minutes=11)
        assert await manager.expire_sessions(principal) == 0
        store.force(created)
        assert await manager.expire_sessions(principal) == 1
        assert (await store.get(created.session_id, principal)).status is SessionStatus.CLOSED  # type: ignore[union-attr]

    asyncio.run(scenario())


def test_resume_rejects_package_digest_change_without_hot_migration() -> None:
    async def scenario() -> None:
        now = [datetime(2026, 8, 20, 8, 0, tzinfo=UTC)]
        principal = _principal()
        manager, store, loader = _manager(now)
        created = await _create(manager, principal)

        loader.package = _package("b")
        with pytest.raises(SessionManagerError) as mismatch:
            await manager.resume_session(created.session_id, principal)
        assert mismatch.value.error.code is ErrorCode.DIGEST_MISMATCH
        persisted = await store.get(created.session_id, principal)
        assert persisted is not None
        assert persisted.package.digest == f"sha256:{'a' * 64}"

    asyncio.run(scenario())


def test_tenant_and_user_isolation_hide_sessions_and_block_resume() -> None:
    async def scenario() -> None:
        now = [datetime(2026, 8, 20, 8, 0, tzinfo=UTC)]
        owner = _principal()
        manager, _, _ = _manager(now)
        created = await _create(manager, owner)

        for outsider in (
            _principal(tenant_id="tenant-b", user_id="user-a"),
            _principal(tenant_id="tenant-a", user_id="user-b"),
        ):
            assert await manager.get_session(created.session_id, outsider) is None
            with pytest.raises(SessionManagerError) as unavailable:
                await manager.resume_session(created.session_id, outsider)
            assert unavailable.value.error.code is ErrorCode.SESSION_CLOSED

    asyncio.run(scenario())


def test_closed_and_busy_sessions_reject_resume_or_close() -> None:
    async def scenario() -> None:
        now = [datetime(2026, 8, 20, 8, 0, tzinfo=UTC)]
        principal = _principal()
        manager, store, _ = _manager(now)
        created = await _create(manager, principal)
        busy = created.model_copy(update={"active_execution_id": "execution-a"})
        store.force(busy)

        with pytest.raises(SessionManagerError) as busy_error:
            await manager.close_session(created.session_id, principal)
        assert busy_error.value.error.code is ErrorCode.SESSION_BUSY

        store.force(created)
        await manager.close_session(created.session_id, principal)
        with pytest.raises(SessionManagerError) as closed_error:
            await manager.resume_session(created.session_id, principal)
        assert closed_error.value.error.code is ErrorCode.SESSION_CLOSED

    asyncio.run(scenario())


def test_session_id_must_fit_persisted_identifier_limit() -> None:
    async def scenario() -> None:
        now = [datetime(2026, 8, 20, 8, 0, tzinfo=UTC)]
        package = _package()
        manager = SessionManager(
            _PackageCatalog(package),
            _SessionStore(),
            _PackageLoader(package),
            now=lambda: now[0],
            session_id_factory=lambda: "s" * 255,
        )

        with pytest.raises(ValueError, match="fewer than 255"):
            await _create(manager, _principal())

    asyncio.run(scenario())


def test_postgres_session_transitions_are_tenant_owner_revision_and_package_scoped() -> None:
    package = _package()
    runtime_session = RuntimeSession(
        session_id="session-a",
        thread_id="session-a",
        tenant_id="tenant-a",
        user_id="user-a",
        package=package,
        status=SessionStatus.OPEN,
        revision=4,
        execution_epoch=2,
        active_execution_id=None,
        last_checkpoint_id="checkpoint-a",
        last_event_sequence=7,
        created_at=datetime(2026, 8, 20, 8, 0, tzinfo=UTC),
        updated_at=datetime(2026, 8, 20, 8, 5, tzinfo=UTC),
    )

    for statement in (
        _build_activate_session_update(runtime_session, _principal(), runtime_session.updated_at),
        _build_close_session_update(runtime_session, _principal(), runtime_session.updated_at),
    ):
        sql = str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        )
        assert "runtime_session.tenant_id = 'tenant-a'" in sql
        assert "runtime_session.user_id = 'user-a'" in sql
        assert "runtime_session.session_id = 'session-a'" in sql
        assert "runtime_session.thread_id = 'session-a'" in sql
        assert "runtime_session.revision = 4" in sql
        assert "runtime_session.status = 'open'" in sql
        assert "runtime_session.active_execution_id IS NULL" in sql
        assert "runtime_session.agent_id = 'agent-a'" in sql
        assert "runtime_session.package_version = '0.1.0'" in sql
        assert f"runtime_session.package_digest = 'sha256:{'a' * 64}'" in sql


class _TransitionBoundary:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        return None


class _TransitionSession:
    def __init__(self) -> None:
        self.operations: list[str] = []

    async def __aenter__(self) -> "_TransitionSession":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        return None

    def begin(self) -> _TransitionBoundary:
        return _TransitionBoundary()

    async def execute(self, statement: object) -> object:
        self.operations.append(str(statement))
        return object()

    async def scalar(self, statement: object) -> None:
        self.operations.append(str(statement))
        return None


def test_postgres_resume_transition_takes_same_advisory_lock_before_cas() -> None:
    async def scenario() -> None:
        timestamp = datetime(2026, 8, 20, 8, 0, tzinfo=UTC)
        runtime_session = RuntimeSession(
            session_id="session-a",
            thread_id="session-a",
            tenant_id="tenant-a",
            user_id="user-a",
            package=_package(),
            status=SessionStatus.OPEN,
            revision=0,
            execution_epoch=0,
            active_execution_id=None,
            last_checkpoint_id=None,
            last_event_sequence=0,
            created_at=timestamp,
            updated_at=timestamp,
        )
        database_session = _TransitionSession()
        store = PostgresSessionStore(cast(Any, lambda: database_session))

        assert await store.activate(runtime_session, _principal(), timestamp) is None
        assert "pg_advisory_xact_lock" in database_session.operations[0]
        assert database_session.operations[1].startswith("UPDATE runtime_session")

    asyncio.run(scenario())


@pytest.mark.filterwarnings("ignore:Using `httpx` with.*:Warning")
def test_minimal_session_routes_create_get_and_close_without_identity_in_body() -> None:
    now = [datetime(2026, 8, 20, 8, 0, tzinfo=UTC)]
    manager, _, _ = _manager(now)

    async def verifier(_token: str) -> Principal:
        return _principal()

    app = create_api(
        principal_verifier=verifier,
        session_manager=manager,
        environ={},
    )
    with TestClient(app) as raw_client:
        client = cast(_RouteClient, raw_client)
        created = client.post(
            "/api/v1/runtime/sessions",
            json={"agent_id": "agent-a", "version": "0.1.0"},
            headers={"authorization": "Bearer test-token"},
        )
        assert created.status_code == 201
        assert created.json()["session_id"] == created.json()["thread_id"]
        assert "tenant_id" not in created.request.content.decode()

        fetched = client.get(
            "/api/v1/runtime/sessions/session-a",
            headers={"authorization": "Bearer test-token"},
        )
        assert fetched.status_code == 200
        assert fetched.json()["package"]["digest"] == f"sha256:{'a' * 64}"

        closed = client.post(
            "/api/v1/runtime/sessions/session-a/close",
            headers={"authorization": "Bearer test-token"},
        )
        assert closed.status_code == 200
        assert closed.json()["status"] == "closed"


def test_postgres_adapters_are_constructible_from_a_session_factory() -> None:
    class _Factory:
        pass

    factory = _Factory()
    assert PostgresPackageCatalog(factory).session_factory is factory  # type: ignore[arg-type]
    assert PostgresSessionStore(factory).session_factory is factory  # type: ignore[arg-type]


def _postgres_url() -> str:
    database_url = os.getenv("RUNTIME_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip(
            "PostgreSQL integration dependency unavailable: set "
            "RUNTIME_TEST_DATABASE_URL=postgresql+asyncpg://..."
        )
    url = make_url(database_url)
    if url.drivername != "postgresql+asyncpg":
        pytest.fail("RUNTIME_TEST_DATABASE_URL must use postgresql+asyncpg")
    return database_url


def _upgrade(connection: Connection) -> None:
    config = Config()
    config.set_main_option("script_location", "migrations")
    config.attributes["connection"] = connection
    command.upgrade(config, "head")


async def _schema_engine(database_url: str) -> AsyncGenerator[AsyncEngine, None]:
    administration_engine = create_async_engine(database_url)
    schema_name = f"runtime_task8_lifecycle_{uuid.uuid4().hex}"
    runtime_engine: AsyncEngine | None = None
    try:
        async with administration_engine.connect() as migration_connection:
            await migration_connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            await migration_connection.execute(text(f'SET search_path TO "{schema_name}"'))
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


async def _assert_live_postgres_lifecycle(database_url: str) -> None:
    engine_iterator = _schema_engine(database_url)
    engine = await anext(engine_iterator)
    try:
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        principal = _principal()
        package = AgentPackageRef(
            tenant_id="tenant-a",
            agent_id="agent-metric-query",
            version="0.1.0",
            digest=(
                "sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04"
            ),
            runtime_type=RuntimeType.DEEPAGENTS,
            sdk_version="0.7.7",
        )
        async with factory() as setup_session:
            await PackageRepository(setup_session).add(package, principal)
        loader = PackageLoader.local(
            Path("agents"),
            tenant_id="tenant-a",
            expected_packages={(package.agent_id, package.version): package},
        )
        manager = SessionManager(
            PostgresPackageCatalog(factory),
            PostgresSessionStore(factory),
            loader,
            session_id_factory=lambda: "session-live",
        )

        created = await manager.create_session(
            CreateSessionRequest(agent_id=package.agent_id, version=package.version),
            principal,
        )
        assert session_lifecycle(created) is SessionLifecycle.CREATED
        assert created.thread_id == created.session_id
        active = await manager.resume_session(created.session_id, principal)
        assert session_lifecycle(active) is SessionLifecycle.ACTIVE
        execution_principal = principal.model_copy(update={"worker_id": "worker-live"})
        lease = cast(
            ExecutionLease,
            SimpleNamespace(
                tenant_id=principal.tenant_id,
                session_id=created.session_id,
                owner_id="worker-live",
                active=True,
            ),
        )

        async with factory() as execution_session:
            executions = ExecutionRepository(execution_session)
            epoch = await executions.begin_execution(
                created.session_id,
                "execution-live",
                execution_principal,
                lease=lease,
            )
        executing = await manager.get_session(created.session_id, principal)
        assert executing is not None
        assert session_lifecycle(executing) is SessionLifecycle.EXECUTING

        async with factory() as execution_session:
            await ExecutionRepository(execution_session).complete_execution(
                created.session_id,
                "execution-live",
                execution_principal,
                epoch,
                ExecutionStatus.SUCCEEDED,
            )
        idle = await manager.get_session(created.session_id, principal)
        assert idle is not None
        assert session_lifecycle(idle) is SessionLifecycle.IDLE
        assert idle.package.digest == created.package.digest
        assert (
            await manager.get_session(
                created.session_id, _principal(tenant_id="tenant-b")
            )
            is None
        )
        closed = await manager.close_session(created.session_id, principal)
        assert session_lifecycle(closed) is SessionLifecycle.CLOSED
    finally:
        await engine_iterator.aclose()


def test_live_postgres_session_lifecycle_and_tenant_isolation() -> None:
    asyncio.run(_assert_live_postgres_lifecycle(_postgres_url()))
