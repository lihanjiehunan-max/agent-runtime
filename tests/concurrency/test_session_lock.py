import asyncio
import os
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from time import monotonic
from typing import cast

import pytest
from alembic import command
from alembic.config import Config
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

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
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.models import RuntimeSessionRow
from packages.runtime_persistence.repositories import (
    EventRepository,
    ExecutionRepository,
    PackageRepository,
    RuntimeRepositoryError,
    SessionRepository,
)
from packages.session_manager.locks import (
    AsyncRedis,
    SessionExecutionCoordinator,
    SessionLockManager,
    SessionLockUnavailable,
    session_lock_key,
)
from packages.session_manager.service import PostgresSessionStore


class _RedisDouble:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(
        self,
        name: str,
        value: str,
        *,
        nx: bool,
        px: int,
    ) -> bool | None:
        assert nx is True
        assert px > 0
        if name in self.values:
            return None
        self.values[name] = value
        return True

    async def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str | int,
    ) -> int:
        assert script
        assert numkeys == 1
        key = str(keys_and_args[0])
        token = str(keys_and_args[1])
        arguments = keys_and_args[2:]
        if self.values.get(key) != token:
            return 0
        if arguments:
            assert int(arguments[0]) > 0
            return 1
        del self.values[key]
        return 1


def test_lock_key_is_tenant_and_session_scoped() -> None:
    assert session_lock_key("tenant-a", "session-a") == (
        "lock:session:tenant-a:session-a"
    )


def test_lock_ownership_token_controls_renewal_and_release() -> None:
    async def scenario() -> None:
        redis = _RedisDouble()
        manager = SessionLockManager(redis, ttl_seconds=5)  # type: ignore[arg-type]
        lease = await manager.acquire("tenant-a", "session-a", "worker-a")

        with pytest.raises(SessionLockUnavailable):
            await manager.acquire("tenant-a", "session-a", "worker-b")
        assert await manager.renew(
            "tenant-a", "session-a", "forged-token"
        ) is False
        assert await lease.renew() is True
        assert await manager.release(
            "tenant-a", "session-a", "forged-token"
        ) is False
        assert await lease.release() is True

        replacement = await manager.acquire(
            "tenant-a", "session-a", "worker-b"
        )
        assert replacement.token != lease.token

    asyncio.run(scenario())


def test_distinct_sessions_have_independent_lock_ownership() -> None:
    async def scenario() -> None:
        redis = _RedisDouble()
        manager = SessionLockManager(redis, ttl_seconds=5)  # type: ignore[arg-type]

        first, second = await asyncio.gather(
            manager.acquire("tenant-a", "session-a", "worker-a"),
            manager.acquire("tenant-a", "session-b", "worker-b"),
        )
        assert first.key != second.key
        assert await first.renew() is True
        assert await second.renew() is True

    asyncio.run(scenario())


class _EpochRepository:
    def __init__(self, epoch: int | None) -> None:
        self.epoch = epoch
        self.calls: list[tuple[str, str]] = []

    async def begin_execution(
        self,
        session_id: str,
        execution_id: str,
        principal: Principal,
        **_kwargs: object,
    ) -> int:
        self.calls.append((session_id, execution_id))
        if self.epoch is None:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.SESSION_BUSY,
                    message="epoch CAS failed",
                )
            )
        assert principal.worker_id is not None
        return self.epoch


def test_execution_coordinator_returns_lease_only_after_epoch_cas() -> None:
    async def scenario() -> None:
        redis = _RedisDouble()
        locks = SessionLockManager(redis, ttl_seconds=5)  # type: ignore[arg-type]
        coordinator = SessionExecutionCoordinator(locks)
        principal = _principal("worker-a")

        first = await coordinator.begin_execution(
            _EpochRepository(3),
            "session-a",
            "execution-a",
            principal,
        )
        second = await coordinator.begin_execution(
            _EpochRepository(8),
            "session-b",
            "execution-b",
            principal,
        )

        assert (first.session_id, first.execution_epoch) == ("session-a", 3)
        assert (second.session_id, second.execution_epoch) == ("session-b", 8)
        assert first.lease.key != second.lease.key
        await first.lease.release()
        await second.lease.release()

    asyncio.run(scenario())


def test_execution_coordinator_releases_redis_lease_when_epoch_cas_fails() -> None:
    async def scenario() -> None:
        redis = _RedisDouble()
        locks = SessionLockManager(redis, ttl_seconds=5)  # type: ignore[arg-type]
        coordinator = SessionExecutionCoordinator(locks)
        principal = _principal("worker-a")

        with pytest.raises(RuntimeRepositoryError):
            await coordinator.begin_execution(
                _EpochRepository(None),
                "session-a",
                "execution-a",
                principal,
            )

        replacement = await locks.acquire("tenant-a", "session-a", "worker-b")
        assert replacement.owner_id == "worker-b"

    asyncio.run(scenario())


async def _acquire_after_expiry(
    manager: SessionLockManager,
) -> tuple[bool, bool]:
    first = await manager.acquire("tenant-a", "session-expiry", "worker-a")
    deadline = monotonic() + 2
    second = None
    while monotonic() < deadline:
        try:
            second = await manager.acquire(
                "tenant-a", "session-expiry", "worker-b"
            )
            break
        except SessionLockUnavailable:
            await asyncio.sleep(0.02)
    assert second is not None
    stale_renewed = await first.renew()
    replacement_renewed = await second.renew()
    await second.release()
    return stale_renewed, replacement_renewed


def test_live_redis_expiry_transfers_coordination_without_reviving_old_owner() -> None:
    async def scenario(redis_url: str) -> None:
        redis = Redis.from_url(  # pyright: ignore[reportUnknownMemberType]
            redis_url, decode_responses=True
        )
        try:
            try:
                await redis.ping()  # pyright: ignore[reportUnknownMemberType]
            except Exception as error:
                pytest.skip(f"Redis integration dependency unavailable: {error}")
            manager = SessionLockManager(cast(AsyncRedis, redis), ttl_seconds=0.1)
            stale_renewed, replacement_renewed = await _acquire_after_expiry(manager)
            assert stale_renewed is False
            assert replacement_renewed is True
        finally:
            await redis.aclose()

    redis_url = os.getenv("RUNTIME_TEST_REDIS_URL")
    if redis_url is None:
        pytest.skip(
            "Redis integration dependency unavailable: set "
            "RUNTIME_TEST_REDIS_URL=redis://..."
        )
    asyncio.run(scenario(redis_url))


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


def _redis_url() -> str:
    redis_url = os.getenv("RUNTIME_TEST_REDIS_URL")
    if redis_url is None:
        pytest.skip(
            "Redis integration dependency unavailable: set "
            "RUNTIME_TEST_REDIS_URL=redis://..."
        )
    return redis_url


def _upgrade(connection: Connection) -> None:
    config = Config()
    config.set_main_option("script_location", "migrations")
    config.attributes["connection"] = connection
    command.upgrade(config, "head")


async def _schema_engine(database_url: str) -> AsyncGenerator[AsyncEngine, None]:
    administration_engine = create_async_engine(database_url)
    schema_name = f"runtime_task8_{uuid.uuid4().hex}"
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


def _package() -> AgentPackageRef:
    return AgentPackageRef(
        tenant_id="tenant-a",
        agent_id="agent-a",
        version="0.1.0",
        digest=f"sha256:{'a' * 64}",
        runtime_type=RuntimeType.DEEPAGENTS,
        sdk_version="0.7.7",
    )


def _principal(worker_id: str) -> Principal:
    return Principal(
        tenant_id="tenant-a",
        user_id="user-a",
        actor_id="user-a",
        worker_id=worker_id,
    )


def _event(package: AgentPackageRef) -> RuntimeEvent:
    return RuntimeEvent(
        schema_version="runtime.event.v1",
        event_id="event-stale-worker-a",
        sequence=1,
        occurred_at=datetime.now(UTC),
        tenant_id="tenant-a",
        trace_id="trace-worker-a",
        span_id="span-worker-a",
        parent_span_id=None,
        session_id="session-fence",
        execution_id="execution-a",
        package=package,
        worker_id="worker-a",
        sdk_version="0.7.7",
        event_type="execution.stale",
        phase="execution",
        duration_ms=None,
        payload={},
        payload_ref=None,
    )


async def _assert_new_epoch_fences_old_worker(
    database_url: str,
    redis_url: str,
) -> None:
    engine_iterator = _schema_engine(database_url)
    engine = await anext(engine_iterator)
    redis = Redis.from_url(  # pyright: ignore[reportUnknownMemberType]
        redis_url, decode_responses=True
    )
    try:
        try:
            await redis.ping()  # pyright: ignore[reportUnknownMemberType]
        except Exception as error:
            pytest.skip(f"Redis integration dependency unavailable: {error}")
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        package = _package()
        worker_a = _principal("worker-a")
        worker_b = _principal("worker-b")
        timestamp = datetime.now(UTC)
        runtime_session = RuntimeSession(
            session_id="session-fence",
            thread_id="session-fence",
            tenant_id="tenant-a",
            user_id="user-a",
            package=package,
            status=SessionStatus.OPEN,
            revision=0,
            execution_epoch=0,
            active_execution_id=None,
            last_checkpoint_id=None,
            last_event_sequence=0,
            created_at=timestamp,
            updated_at=timestamp,
        )
        async with factory() as setup_session:
            await PackageRepository(setup_session).add(package, worker_a)
            await SessionRepository(setup_session).add(runtime_session, worker_a)

        lock_manager = SessionLockManager(cast(AsyncRedis, redis), ttl_seconds=0.1)
        coordinator = SessionExecutionCoordinator(lock_manager)
        async with factory() as worker_a_session:
            worker_a_executions = ExecutionRepository(worker_a_session)
            stale_fence = await coordinator.begin_execution(
                worker_a_executions,
                "session-fence",
                "execution-a",
                worker_a,
            )
            stale_lease = stale_fence.lease
            epoch_a = stale_fence.execution_epoch
            await worker_a_session.rollback()
            await worker_a_executions.complete_execution(
                "session-fence",
                "execution-a",
                worker_a,
                epoch_a,
                ExecutionStatus.SUCCEEDED,
            )
        idle_snapshot = await PostgresSessionStore(factory).get(
            "session-fence", worker_a
        )
        assert idle_snapshot is not None

        async with factory() as worker_b_session:
            deadline = monotonic() + 2
            while True:
                try:
                    replacement_fence = await coordinator.begin_execution(
                        ExecutionRepository(worker_b_session),
                        "session-fence",
                        "execution-b",
                        worker_b,
                    )
                    break
                except SessionLockUnavailable:
                    if monotonic() >= deadline:
                        raise AssertionError(
                            "replacement worker did not acquire fence"
                        ) from None
                    await asyncio.sleep(0.02)
        assert await stale_lease.renew() is False
        replacement_lease = replacement_fence.lease
        epoch_b = replacement_fence.execution_epoch
        assert epoch_b == epoch_a + 1

        async with factory() as stale_worker_session:
            with pytest.raises(RuntimeRepositoryError) as event_fenced:
                await EventRepository(stale_worker_session).append(
                    _event(package), worker_a, epoch_a
                )
            assert event_fenced.value.error.code is ErrorCode.EXECUTION_FENCED

            with pytest.raises(RuntimeRepositoryError) as completion_fenced:
                await ExecutionRepository(stale_worker_session).complete_execution(
                    "session-fence",
                    "execution-a",
                    worker_a,
                    epoch_a,
                    ExecutionStatus.SUCCEEDED,
                )
            assert completion_fenced.value.error.code is ErrorCode.EXECUTION_FENCED

        assert await PostgresSessionStore(factory).activate(
            idle_snapshot, worker_a, datetime.now(UTC)
        ) is None

        async with factory() as observer:
            persisted = await observer.scalar(
                select(RuntimeSessionRow).where(
                    RuntimeSessionRow.tenant_id == "tenant-a",
                    RuntimeSessionRow.session_id == "session-fence",
                )
            )
            assert persisted is not None
            assert persisted.active_execution_id == "execution-b"
            assert persisted.execution_epoch == epoch_b
            assert persisted.last_checkpoint_id is None
            assert persisted.last_event_sequence == 0
        await replacement_lease.release()  # type: ignore[attr-defined]
    finally:
        await redis.aclose()
        await engine_iterator.aclose()


def test_expired_lock_then_new_epoch_fences_every_stale_worker_write() -> None:
    asyncio.run(_assert_new_epoch_fences_old_worker(_postgres_url(), _redis_url()))
