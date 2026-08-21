from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol
from uuid import uuid4

from sqlalchemy import Select, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.dml import Update

from packages.runtime_contracts import (
    AgentPackageRef,
    CreateSessionRequest,
    ErrorCode,
    Principal,
    RuntimeSession,
    SessionStatus,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.models import RuntimeSessionRow
from packages.runtime_persistence.repositories import (
    PackageRepository,
    SessionRepository,
    acquire_session_advisory_lock,
)


class SessionManagerError(Exception):
    def __init__(self, error: RuntimeContractError) -> None:
        super().__init__(error.message)
        self.error = error


def _session_error(code: ErrorCode, message: str) -> SessionManagerError:
    return SessionManagerError(RuntimeContractError(code=code, message=message))


class PackageCatalog(Protocol):
    async def get(
        self, agent_id: str, version: str, principal: Principal
    ) -> AgentPackageRef | None: ...


class PackageLimits(Protocol):
    @property
    def session_ttl_minutes(self) -> int: ...


class LoadedPackage(Protocol):
    @property
    def reference(self) -> AgentPackageRef: ...

    @property
    def limits(self) -> PackageLimits: ...


class PackageLoader(Protocol):
    def load(self, agent_id: str, version: str) -> LoadedPackage: ...


class SessionStore(Protocol):
    async def add(
        self, runtime_session: RuntimeSession, principal: Principal
    ) -> RuntimeSession: ...

    async def get(
        self, session_id: str, principal: Principal
    ) -> RuntimeSession | None: ...

    async def activate(
        self,
        runtime_session: RuntimeSession,
        principal: Principal,
        now: datetime,
    ) -> RuntimeSession | None: ...

    async def close(
        self,
        runtime_session: RuntimeSession,
        principal: Principal,
        now: datetime,
    ) -> RuntimeSession | None: ...

    async def list_open_idle(self, principal: Principal) -> Sequence[RuntimeSession]: ...


def _build_activate_session_update(
    runtime_session: RuntimeSession,
    principal: Principal,
    updated_at: datetime,
) -> Update:
    return (
        _base_transition_update(runtime_session, principal)
        .values(
            revision=RuntimeSessionRow.revision + 1,
            updated_at=updated_at,
        )
        .returning(RuntimeSessionRow.session_id)
    )


def _build_close_session_update(
    runtime_session: RuntimeSession,
    principal: Principal,
    updated_at: datetime,
) -> Update:
    return (
        _base_transition_update(runtime_session, principal)
        .values(
            status=SessionStatus.CLOSED.value,
            revision=RuntimeSessionRow.revision + 1,
            updated_at=updated_at,
        )
        .returning(RuntimeSessionRow.session_id)
    )


def _base_transition_update(
    runtime_session: RuntimeSession,
    principal: Principal,
) -> Update:
    return update(RuntimeSessionRow).where(
        RuntimeSessionRow.tenant_id == principal.tenant_id,
        RuntimeSessionRow.user_id == principal.user_id,
        RuntimeSessionRow.session_id == runtime_session.session_id,
        RuntimeSessionRow.thread_id == runtime_session.session_id,
        RuntimeSessionRow.agent_id == runtime_session.package.agent_id,
        RuntimeSessionRow.package_version == runtime_session.package.version,
        RuntimeSessionRow.package_digest == runtime_session.package.digest,
        RuntimeSessionRow.revision == runtime_session.revision,
        RuntimeSessionRow.status == SessionStatus.OPEN.value,
        RuntimeSessionRow.active_execution_id.is_(None),
    )


class PostgresPackageCatalog:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def get(
        self, agent_id: str, version: str, principal: Principal
    ) -> AgentPackageRef | None:
        async with self.session_factory() as database_session:
            return await PackageRepository(database_session).get(
                agent_id, version, principal
            )


class PostgresSessionStore:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def add(
        self, runtime_session: RuntimeSession, principal: Principal
    ) -> RuntimeSession:
        async with self.session_factory() as database_session:
            return await SessionRepository(database_session).add(
                runtime_session, principal
            )

    async def get(
        self, session_id: str, principal: Principal
    ) -> RuntimeSession | None:
        async with self.session_factory() as database_session:
            return await SessionRepository(database_session).get(session_id, principal)

    async def activate(
        self,
        runtime_session: RuntimeSession,
        principal: Principal,
        now: datetime,
    ) -> RuntimeSession | None:
        return await self._transition(
            _build_activate_session_update(runtime_session, principal, now),
            runtime_session.session_id,
            principal,
        )

    async def close(
        self,
        runtime_session: RuntimeSession,
        principal: Principal,
        now: datetime,
    ) -> RuntimeSession | None:
        return await self._transition(
            _build_close_session_update(runtime_session, principal, now),
            runtime_session.session_id,
            principal,
        )

    async def list_open_idle(self, principal: Principal) -> Sequence[RuntimeSession]:
        statement: Select[tuple[str]] = select(RuntimeSessionRow.session_id).where(
            RuntimeSessionRow.tenant_id == principal.tenant_id,
            RuntimeSessionRow.user_id == principal.user_id,
            RuntimeSessionRow.status == SessionStatus.OPEN.value,
            RuntimeSessionRow.active_execution_id.is_(None),
        )
        async with self.session_factory() as database_session:
            session_ids = tuple(await database_session.scalars(statement))
            repository = SessionRepository(database_session)
            sessions = [
                await repository.get(session_id, principal)
                for session_id in session_ids
            ]
        return tuple(runtime_session for runtime_session in sessions if runtime_session is not None)

    async def _transition(
        self,
        statement: Update,
        session_id: str,
        principal: Principal,
    ) -> RuntimeSession | None:
        async with self.session_factory() as database_session:
            async with database_session.begin():
                await acquire_session_advisory_lock(
                    database_session,
                    principal.tenant_id,
                    session_id,
                )
                matched = await database_session.scalar(statement)
            if matched is None:
                return None
            return await SessionRepository(database_session).get(session_id, principal)


class SessionLifecycle(StrEnum):
    CREATED = "created"
    ACTIVE = "active"
    EXECUTING = "executing"
    IDLE = "idle"
    CLOSED = "closed"


def session_lifecycle(runtime_session: RuntimeSession) -> SessionLifecycle:
    if runtime_session.status is SessionStatus.CLOSED:
        return SessionLifecycle.CLOSED
    if runtime_session.active_execution_id is not None:
        return SessionLifecycle.EXECUTING
    if runtime_session.execution_epoch > 0:
        return SessionLifecycle.IDLE
    if runtime_session.revision == 0:
        return SessionLifecycle.CREATED
    return SessionLifecycle.ACTIVE


class SessionManager:
    def __init__(
        self,
        package_catalog: PackageCatalog,
        session_store: SessionStore,
        package_loader: PackageLoader,
        *,
        now: Callable[[], datetime] | None = None,
        session_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._package_catalog = package_catalog
        self._session_store = session_store
        self._package_loader = package_loader
        self._now = now or (lambda: datetime.now(UTC))
        self._session_id_factory = session_id_factory or (
            lambda: f"session_{uuid4().hex}"
        )

    async def create_session(
        self, request: CreateSessionRequest, principal: Principal
    ) -> RuntimeSession:
        package = await self._package_catalog.get(
            request.agent_id, request.version, principal
        )
        if package is None:
            raise _session_error(
                ErrorCode.PACKAGE_NOT_FOUND,
                "the requested agent package is unavailable",
            )
        self._load_pinned(package)

        session_id = self._session_id_factory()
        if len(session_id) >= 255:
            raise ValueError("session identifiers must contain fewer than 255 characters")
        timestamp = self._now()
        runtime_session = RuntimeSession(
            session_id=session_id,
            thread_id=session_id,
            tenant_id=principal.tenant_id,
            user_id=principal.user_id,
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
        return await self._session_store.add(runtime_session, principal)

    async def get_session(
        self, session_id: str, principal: Principal
    ) -> RuntimeSession | None:
        runtime_session = await self._session_store.get(session_id, principal)
        if runtime_session is None or runtime_session.user_id != principal.user_id:
            return None
        if (
            runtime_session.status is SessionStatus.OPEN
            and runtime_session.active_execution_id is None
        ):
            loaded = self._load_pinned(runtime_session.package)
            timestamp = self._now()
            if self._is_expired(
                runtime_session,
                loaded.limits.session_ttl_minutes,
                timestamp,
            ):
                closed = await self._session_store.close(
                    runtime_session,
                    principal,
                    timestamp,
                )
                if closed is not None:
                    return closed
                return await self._session_store.get(session_id, principal)
        return runtime_session

    async def resume_session(
        self, session_id: str, principal: Principal
    ) -> RuntimeSession:
        runtime_session = await self._require_open_session(session_id, principal)
        if runtime_session.active_execution_id is not None:
            raise _session_error(
                ErrorCode.SESSION_BUSY,
                "session already has an active execution",
            )
        loaded = self._load_pinned(runtime_session.package)
        timestamp = self._now()
        if self._is_expired(runtime_session, loaded.limits.session_ttl_minutes, timestamp):
            await self._session_store.close(runtime_session, principal, timestamp)
            raise _session_error(
                ErrorCode.SESSION_CLOSED,
                "session TTL expired",
            )
        activated = await self._session_store.activate(
            runtime_session, principal, timestamp
        )
        if activated is None:
            current = await self.get_session(session_id, principal)
            if current is not None and current.active_execution_id is not None:
                raise _session_error(
                    ErrorCode.SESSION_BUSY,
                    "session already has an active execution",
                )
            raise _session_error(
                ErrorCode.SESSION_CLOSED,
                "session changed while it was being resumed",
            )
        return activated

    async def close_session(
        self, session_id: str, principal: Principal
    ) -> RuntimeSession:
        runtime_session = await self.get_session(session_id, principal)
        if runtime_session is None:
            raise _session_error(
                ErrorCode.SESSION_CLOSED,
                "session is closed or unavailable",
            )
        if runtime_session.status is SessionStatus.CLOSED:
            return runtime_session
        if runtime_session.active_execution_id is not None:
            raise _session_error(
                ErrorCode.SESSION_BUSY,
                "an executing session cannot be closed",
            )
        closed = await self._session_store.close(
            runtime_session, principal, self._now()
        )
        if closed is None:
            raise _session_error(
                ErrorCode.SESSION_CLOSED,
                "session changed while it was being closed",
            )
        return closed

    async def expire_sessions(self, principal: Principal) -> int:
        timestamp = self._now()
        expired_count = 0
        for runtime_session in await self._session_store.list_open_idle(principal):
            loaded = self._load_pinned(runtime_session.package)
            if not self._is_expired(
                runtime_session, loaded.limits.session_ttl_minutes, timestamp
            ):
                continue
            closed = await self._session_store.close(
                runtime_session, principal, timestamp
            )
            expired_count += int(closed is not None)
        return expired_count

    async def _require_open_session(
        self, session_id: str, principal: Principal
    ) -> RuntimeSession:
        runtime_session = await self.get_session(session_id, principal)
        if runtime_session is None or runtime_session.status is SessionStatus.CLOSED:
            raise _session_error(
                ErrorCode.SESSION_CLOSED,
                "session is closed or unavailable",
            )
        return runtime_session

    def _load_pinned(self, package: AgentPackageRef) -> LoadedPackage:
        loaded = self._package_loader.load(package.agent_id, package.version)
        if loaded.reference != package:
            raise _session_error(
                ErrorCode.DIGEST_MISMATCH,
                "session package identity differs from its pinned package digest",
            )
        return loaded

    @staticmethod
    def _is_expired(
        runtime_session: RuntimeSession,
        ttl_minutes: int,
        now: datetime,
    ) -> bool:
        return runtime_session.updated_at + timedelta(minutes=ttl_minutes) <= now


__all__ = [
    "LoadedPackage",
    "PackageCatalog",
    "PackageLoader",
    "PostgresPackageCatalog",
    "PostgresSessionStore",
    "SessionLifecycle",
    "SessionManager",
    "SessionManagerError",
    "SessionStore",
    "session_lifecycle",
]
