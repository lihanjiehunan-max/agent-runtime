import hashlib
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Never, Protocol, cast

from pydantic import JsonValue
from sqlalchemy import Select, exists, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Update

from packages.runtime_contracts import (
    AgentPackageRef,
    ErrorCode,
    ExecutionMode,
    ExecutionStatus,
    Principal,
    RuntimeEvent,
    RuntimeExecution,
    RuntimeSession,
    RuntimeType,
    SessionStatus,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_contracts._immutable_json import thaw_json_object
from packages.runtime_persistence.models import (
    AgentPackageRow,
    RuntimeEventRow,
    RuntimeExecutionRow,
    RuntimeSessionRow,
)


class RuntimeRepositoryError(Exception):
    def __init__(self, error: RuntimeContractError) -> None:
        super().__init__(error.message)
        self.error = error


def _repository_error(code: ErrorCode, message: str) -> RuntimeRepositoryError:
    return RuntimeRepositoryError(RuntimeContractError(code=code, message=message))


def _require_tenant(principal: Principal, tenant_id: str) -> None:
    if tenant_id != principal.tenant_id:
        raise _repository_error(
            ErrorCode.TENANT_IDENTITY_MISMATCH,
            "persisted tenant identity does not match the verified principal",
        )


def _require_session_owner(principal: Principal, runtime_session: RuntimeSession) -> None:
    _require_tenant(principal, runtime_session.tenant_id)
    if runtime_session.user_id != principal.user_id:
        raise _repository_error(
            ErrorCode.TENANT_IDENTITY_MISMATCH,
            "persisted user identity does not match the verified principal",
        )


class ExecutionLease(Protocol):
    @property
    def tenant_id(self) -> str: ...

    @property
    def session_id(self) -> str: ...

    @property
    def owner_id(self) -> str: ...

    @property
    def active(self) -> bool: ...


def _require_execution_lease(
    lease: ExecutionLease,
    session_id: str,
    principal: Principal,
) -> None:
    if (
        not lease.active
        or principal.worker_id is None
        or lease.tenant_id != principal.tenant_id
        or lease.session_id != session_id
        or lease.owner_id != principal.worker_id
    ):
        raise _repository_error(
            ErrorCode.TENANT_IDENTITY_MISMATCH,
            "execution epoch CAS requires the coordinator's matching active Redis lease",
        )


def session_advisory_lock_key(tenant_id: str, session_id: str) -> int:
    identity = f"{tenant_id}\0{session_id}".encode()
    digest = hashlib.sha256(identity).digest()[:8]
    return int.from_bytes(digest, byteorder="big", signed=True)


async def acquire_session_advisory_lock(
    session: AsyncSession,
    tenant_id: str,
    session_id: str,
) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)").bindparams(
            lock_key=session_advisory_lock_key(tenant_id, session_id)
        )
    )


def _require_event_identity(principal: Principal, event: RuntimeEvent) -> None:
    _require_tenant(principal, event.tenant_id)
    _require_tenant(principal, event.package.tenant_id)
    if principal.worker_id is None or event.worker_id != principal.worker_id:
        raise _repository_error(
            ErrorCode.TENANT_IDENTITY_MISMATCH,
            "event worker identity does not match the verified principal",
        )


_TERMINAL_EXECUTION_STATUSES = frozenset(
    {
        ExecutionStatus.SUCCEEDED.value,
        ExecutionStatus.FAILED.value,
        ExecutionStatus.TIMED_OUT.value,
        ExecutionStatus.CANCELLED.value,
    }
)
_TERMINAL_EVENT_TYPES = {
    ExecutionStatus.SUCCEEDED: "execution.succeeded",
    ExecutionStatus.FAILED: "execution.failed",
    ExecutionStatus.TIMED_OUT: "execution.timed_out",
    ExecutionStatus.CANCELLED: "execution.cancelled",
}


def _event_row(event: RuntimeEvent, principal: Principal) -> RuntimeEventRow:
    return RuntimeEventRow(
        tenant_id=principal.tenant_id,
        event_id=event.event_id,
        sequence=event.sequence,
        occurred_at=event.occurred_at,
        trace_id=event.trace_id,
        span_id=event.span_id,
        parent_span_id=event.parent_span_id,
        session_id=event.session_id,
        execution_id=event.execution_id,
        agent_id=event.package.agent_id,
        package_version=event.package.version,
        package_digest=event.package.digest,
        runtime_type=event.package.runtime_type.value,
        worker_id=event.worker_id,
        sdk_version=event.sdk_version,
        event_type=event.event_type,
        phase=event.phase,
        duration_ms=event.duration_ms,
        payload=thaw_json_object(event.payload),
        payload_ref=event.payload_ref,
    )


class PackageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        package: AgentPackageRef,
        principal: Principal,
        *,
        package_uri: str | None = None,
        manifest: Mapping[str, JsonValue] | None = None,
        checksum_evidence: Mapping[str, JsonValue] | None = None,
        status: str = "active",
        now: datetime | None = None,
    ) -> AgentPackageRef:
        _require_tenant(principal, package.tenant_id)
        timestamp = now or datetime.now(UTC)
        row = AgentPackageRow(
            tenant_id=principal.tenant_id,
            agent_id=package.agent_id,
            version=package.version,
            digest=package.digest,
            runtime_type=package.runtime_type.value,
            sdk_version=package.sdk_version,
            package_uri=package_uri
            or f"registry://{principal.tenant_id}/{package.agent_id}/{package.version}",
            status=status,
            manifest=dict(manifest or {}),
            checksum_evidence=dict(checksum_evidence or {}),
            created_at=timestamp,
            updated_at=timestamp,
        )
        async with self._session.begin():
            self._session.add(row)
        return package

    async def get(
        self, agent_id: str, version: str, principal: Principal
    ) -> AgentPackageRef | None:
        statement = select(AgentPackageRow).where(
            AgentPackageRow.tenant_id == principal.tenant_id,
            AgentPackageRow.agent_id == agent_id,
            AgentPackageRow.version == version,
        )
        row = await self._session.scalar(statement)
        if row is None:
            return None
        return _package_from_row(row)


class SessionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self, runtime_session: RuntimeSession, principal: Principal
    ) -> RuntimeSession:
        _require_session_owner(principal, runtime_session)
        _require_tenant(principal, runtime_session.package.tenant_id)
        if runtime_session.thread_id != runtime_session.session_id:
            raise _repository_error(
                ErrorCode.TENANT_IDENTITY_MISMATCH,
                "LangGraph thread identity must equal the runtime session identity",
            )
        row = RuntimeSessionRow(
            tenant_id=principal.tenant_id,
            session_id=runtime_session.session_id,
            thread_id=runtime_session.thread_id,
            user_id=principal.user_id,
            agent_id=runtime_session.package.agent_id,
            package_version=runtime_session.package.version,
            package_digest=runtime_session.package.digest,
            status=runtime_session.status.value,
            revision=runtime_session.revision,
            execution_epoch=runtime_session.execution_epoch,
            active_execution_id=runtime_session.active_execution_id,
            last_checkpoint_id=runtime_session.last_checkpoint_id,
            last_event_sequence=runtime_session.last_event_sequence,
            created_at=runtime_session.created_at,
            updated_at=runtime_session.updated_at,
        )
        async with self._session.begin():
            self._session.add(row)
        return runtime_session

    async def get(self, session_id: str, principal: Principal) -> RuntimeSession | None:
        statement = (
            select(RuntimeSessionRow, AgentPackageRow)
            .join(
                AgentPackageRow,
                (AgentPackageRow.tenant_id == RuntimeSessionRow.tenant_id)
                & (AgentPackageRow.agent_id == RuntimeSessionRow.agent_id)
                & (AgentPackageRow.version == RuntimeSessionRow.package_version)
                & (AgentPackageRow.digest == RuntimeSessionRow.package_digest),
            )
            .where(
                RuntimeSessionRow.tenant_id == principal.tenant_id,
                RuntimeSessionRow.session_id == session_id,
            )
        )
        result = await self._session.execute(statement)
        row = result.one_or_none()
        if row is None:
            return None
        session_row, package_row = row.tuple()
        return _session_from_rows(session_row, package_row)


def _build_begin_session_update(
    *,
    tenant_id: str,
    session_id: str,
    execution_id: str,
    updated_at: datetime,
) -> Update:
    return (
        update(RuntimeSessionRow)
        .where(
            RuntimeSessionRow.tenant_id == tenant_id,
            RuntimeSessionRow.session_id == session_id,
            RuntimeSessionRow.status == SessionStatus.OPEN.value,
            RuntimeSessionRow.active_execution_id.is_(None),
        )
        .values(
            active_execution_id=execution_id,
            execution_epoch=RuntimeSessionRow.execution_epoch + 1,
            revision=RuntimeSessionRow.revision + 1,
            updated_at=updated_at,
        )
        .returning(RuntimeSessionRow.execution_epoch)
    )


def _build_complete_session_update(
    *,
    tenant_id: str,
    session_id: str,
    execution_id: str,
    execution_epoch: int,
    updated_at: datetime,
) -> Update:
    return (
        update(RuntimeSessionRow)
        .where(
            RuntimeSessionRow.tenant_id == tenant_id,
            RuntimeSessionRow.session_id == session_id,
            RuntimeSessionRow.active_execution_id == execution_id,
            RuntimeSessionRow.execution_epoch == execution_epoch,
        )
        .values(
            active_execution_id=None,
            revision=RuntimeSessionRow.revision + 1,
            updated_at=updated_at,
        )
        .returning(RuntimeSessionRow.execution_epoch)
    )


class ExecutionRepository:
    """Persistence primitive; production starts must enter through the coordinator."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def begin_execution(
        self,
        session_id: str,
        execution_id: str,
        principal: Principal,
        *,
        lease: ExecutionLease,
        mode: ExecutionMode = ExecutionMode.SYNC,
        trace_id: str | None = None,
        request_input: str | None = None,
        created_at: datetime | None = None,
    ) -> int:
        """Apply the epoch CAS only with the coordinator's matching Redis lease."""
        _require_execution_lease(lease, session_id, principal)
        timestamp = created_at or datetime.now(UTC)
        statement = _build_begin_session_update(
            tenant_id=principal.tenant_id,
            session_id=session_id,
            execution_id=execution_id,
            updated_at=timestamp,
        )
        async with self._session.begin():
            await acquire_session_advisory_lock(
                self._session,
                principal.tenant_id,
                session_id,
            )
            execution_epoch = cast(int | None, await self._session.scalar(statement))
            if execution_epoch is None:
                await self._raise_session_unavailable(session_id, principal)
            self._session.add(
                RuntimeExecutionRow(
                    tenant_id=principal.tenant_id,
                    execution_id=execution_id,
                    session_id=session_id,
                    user_id=principal.user_id,
                    actor_id=principal.actor_id,
                    worker_id=principal.worker_id,
                    trace_id=trace_id or execution_id,
                    execution_epoch=execution_epoch,
                    mode=mode.value,
                    status=ExecutionStatus.ACCEPTED.value,
                    request_input=request_input,
                    created_at=timestamp,
                    started_at=None,
                    completed_at=None,
                )
            )
        return execution_epoch

    async def _raise_session_unavailable(
        self, session_id: str, principal: Principal
    ) -> Never:
        state = await self._session.execute(
            select(RuntimeSessionRow.status, RuntimeSessionRow.active_execution_id).where(
                RuntimeSessionRow.tenant_id == principal.tenant_id,
                RuntimeSessionRow.session_id == session_id,
            )
        )
        row = state.one_or_none()
        if row is not None and row.active_execution_id is not None:
            raise _repository_error(
                ErrorCode.SESSION_BUSY, "session already has an active execution"
            )
        raise _repository_error(
            ErrorCode.SESSION_CLOSED, "session is closed or unavailable"
        )

    async def complete_execution(
        self,
        session_id: str,
        execution_id: str,
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        *,
        completed_at: datetime | None = None,
        result_ref: str | None = None,
        error: RuntimeContractError | None = None,
    ) -> None:
        timestamp = completed_at or datetime.now(UTC)
        session_statement = _build_complete_session_update(
            tenant_id=principal.tenant_id,
            session_id=session_id,
            execution_id=execution_id,
            execution_epoch=execution_epoch,
            updated_at=timestamp,
        )
        execution_statement = (
            update(RuntimeExecutionRow)
            .where(
                RuntimeExecutionRow.tenant_id == principal.tenant_id,
                RuntimeExecutionRow.session_id == session_id,
                RuntimeExecutionRow.execution_id == execution_id,
                RuntimeExecutionRow.execution_epoch == execution_epoch,
            )
            .values(
                status=status.value,
                completed_at=timestamp,
                result_ref=result_ref,
                error=error.model_dump(mode="json") if error is not None else None,
            )
            .returning(RuntimeExecutionRow.execution_id)
        )
        async with self._session.begin():
            await acquire_session_advisory_lock(
                self._session,
                principal.tenant_id,
                session_id,
            )
            matched_epoch = await self._session.scalar(session_statement)
            if matched_epoch is None:
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "execution completion was rejected by the session epoch fence",
                )
            matched_execution = await self._session.scalar(execution_statement)
            if matched_execution is None:
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "execution completion did not match the persisted execution epoch",
                )

    async def get(
        self, execution_id: str, principal: Principal
    ) -> RuntimeExecution | None:
        statement = select(RuntimeExecutionRow).where(
            RuntimeExecutionRow.tenant_id == principal.tenant_id,
            RuntimeExecutionRow.execution_id == execution_id,
        )
        row = await self._session.scalar(statement)
        return _execution_from_row(row) if row is not None else None


class EventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(
        self,
        event: RuntimeEvent,
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        _require_event_identity(principal, event)
        persisted_execution_identity = exists(
            select(RuntimeExecutionRow.execution_id).where(
                RuntimeExecutionRow.tenant_id == RuntimeSessionRow.tenant_id,
                RuntimeExecutionRow.session_id == RuntimeSessionRow.session_id,
                RuntimeExecutionRow.execution_id
                == RuntimeSessionRow.active_execution_id,
                RuntimeExecutionRow.execution_epoch
                == RuntimeSessionRow.execution_epoch,
                RuntimeExecutionRow.worker_id == event.worker_id,
            )
        ).correlate(RuntimeSessionRow)
        session_statement = (
            update(RuntimeSessionRow)
            .where(
                RuntimeSessionRow.tenant_id == principal.tenant_id,
                RuntimeSessionRow.session_id == event.session_id,
                RuntimeSessionRow.agent_id == event.package.agent_id,
                RuntimeSessionRow.package_version == event.package.version,
                RuntimeSessionRow.package_digest == event.package.digest,
                RuntimeSessionRow.active_execution_id == event.execution_id,
                RuntimeSessionRow.execution_epoch == execution_epoch,
                RuntimeSessionRow.last_event_sequence == event.sequence - 1,
                persisted_execution_identity,
            )
            .values(
                last_event_sequence=event.sequence,
                revision=RuntimeSessionRow.revision + 1,
                updated_at=event.occurred_at,
            )
            .returning(RuntimeSessionRow.last_event_sequence)
        )
        row = RuntimeEventRow(
            tenant_id=principal.tenant_id,
            event_id=event.event_id,
            sequence=event.sequence,
            occurred_at=event.occurred_at,
            trace_id=event.trace_id,
            span_id=event.span_id,
            parent_span_id=event.parent_span_id,
            session_id=event.session_id,
            execution_id=event.execution_id,
            agent_id=event.package.agent_id,
            package_version=event.package.version,
            package_digest=event.package.digest,
            runtime_type=event.package.runtime_type.value,
            worker_id=event.worker_id,
            sdk_version=event.sdk_version,
            event_type=event.event_type,
            phase=event.phase,
            duration_ms=event.duration_ms,
            payload=thaw_json_object(event.payload),
            payload_ref=event.payload_ref,
        )
        async with self._session.begin():
            matched_sequence = await self._session.scalar(session_statement)
            if matched_sequence is None:
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "event append was rejected by the session epoch or sequence fence",
                )
            self._session.add(row)
        return event

    async def append_next(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        """Allocate and append the next event under the durable Session fence."""

        probe = event_factory(1)
        _require_event_identity(principal, probe)
        async with self._session.begin():
            session_statement = (
                select(RuntimeSessionRow)
                .where(
                    RuntimeSessionRow.tenant_id == principal.tenant_id,
                    RuntimeSessionRow.session_id == probe.session_id,
                )
                .with_for_update()
            )
            runtime_session = await self._session.scalar(session_statement)
            if runtime_session is None:
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "event append was rejected because the Session is unavailable",
                )

            sequence = runtime_session.last_event_sequence + 1
            event = probe if sequence == 1 else event_factory(sequence)
            _require_event_identity(principal, event)
            if (
                event.sequence != sequence
                or event.session_id != runtime_session.session_id
                or event.execution_id != runtime_session.active_execution_id
                or event.package.agent_id != runtime_session.agent_id
                or event.package.version != runtime_session.package_version
                or event.package.digest != runtime_session.package_digest
                or runtime_session.execution_epoch != execution_epoch
            ):
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "event append was rejected by the Session identity or epoch fence",
                )

            execution_statement = select(RuntimeExecutionRow.execution_id).where(
                RuntimeExecutionRow.tenant_id == principal.tenant_id,
                RuntimeExecutionRow.session_id == event.session_id,
                RuntimeExecutionRow.execution_id == event.execution_id,
                RuntimeExecutionRow.execution_epoch == execution_epoch,
                RuntimeExecutionRow.worker_id == event.worker_id,
            )
            persisted_execution_id = await self._session.scalar(execution_statement)
            if persisted_execution_id is None:
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "event append was rejected because the worker execution is stale",
                )

            runtime_session.last_event_sequence = sequence
            runtime_session.revision += 1
            runtime_session.updated_at = event.occurred_at
            self._session.add(
                RuntimeEventRow(
                    tenant_id=principal.tenant_id,
                    event_id=event.event_id,
                    sequence=event.sequence,
                    occurred_at=event.occurred_at,
                    trace_id=event.trace_id,
                    span_id=event.span_id,
                    parent_span_id=event.parent_span_id,
                    session_id=event.session_id,
                    execution_id=event.execution_id,
                    agent_id=event.package.agent_id,
                    package_version=event.package.version,
                    package_digest=event.package.digest,
                    runtime_type=event.package.runtime_type.value,
                    worker_id=event.worker_id,
                    sdk_version=event.sdk_version,
                    event_type=event.event_type,
                    phase=event.phase,
                    duration_ms=event.duration_ms,
                    payload=thaw_json_object(event.payload),
                    payload_ref=event.payload_ref,
                )
            )
        return event

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        """Append a terminal event and release its execution atomically."""

        expected_event_type = _TERMINAL_EVENT_TYPES.get(status)
        if expected_event_type is None:
            raise _repository_error(
                ErrorCode.RUNTIME_INCOMPATIBLE,
                "terminal append requires a terminal execution status",
            )

        probe = event_factory(1)
        _require_event_identity(principal, probe)
        if probe.event_type != expected_event_type:
            raise _repository_error(
                ErrorCode.RUNTIME_INCOMPATIBLE,
                "terminal event type does not match execution status",
            )

        async with self._session.begin():
            session_statement = (
                select(RuntimeSessionRow)
                .where(
                    RuntimeSessionRow.tenant_id == principal.tenant_id,
                    RuntimeSessionRow.session_id == probe.session_id,
                )
                .with_for_update()
            )
            runtime_session = await self._session.scalar(session_statement)
            if runtime_session is None:
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "terminal append was rejected because the Session is unavailable",
                )

            execution_statement = (
                select(RuntimeExecutionRow)
                .where(
                    RuntimeExecutionRow.tenant_id == principal.tenant_id,
                    RuntimeExecutionRow.session_id == probe.session_id,
                    RuntimeExecutionRow.execution_id == probe.execution_id,
                    RuntimeExecutionRow.execution_epoch == execution_epoch,
                )
                .with_for_update()
            )
            runtime_execution = await self._session.scalar(execution_statement)
            if runtime_execution is None:
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "terminal append was rejected because the execution is stale",
                )

            persisted_status = getattr(runtime_execution.status, "value", runtime_execution.status)
            if persisted_status in _TERMINAL_EXECUTION_STATUSES:
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "terminal append was rejected because the execution is already terminal",
                )

            sequence = runtime_session.last_event_sequence + 1
            event = probe if sequence == 1 else event_factory(sequence)
            _require_event_identity(principal, event)
            if (
                event.event_type != expected_event_type
                or event.sequence != sequence
                or event.session_id != runtime_session.session_id
                or event.execution_id != runtime_session.active_execution_id
                or event.package.agent_id != runtime_session.agent_id
                or event.package.version != runtime_session.package_version
                or event.package.digest != runtime_session.package_digest
                or event.worker_id != runtime_execution.worker_id
                or runtime_session.execution_epoch != execution_epoch
                or runtime_execution.session_id != event.session_id
                or runtime_execution.execution_id != event.execution_id
                or runtime_execution.execution_epoch != execution_epoch
            ):
                raise _repository_error(
                    ErrorCode.EXECUTION_FENCED,
                    "terminal append was rejected by the Session identity or epoch fence",
                )

            runtime_execution.status = status.value
            runtime_execution.completed_at = event.occurred_at
            runtime_execution.error = (
                error.model_dump(mode="json") if error is not None else None
            )
            runtime_session.active_execution_id = None
            runtime_session.last_event_sequence = sequence
            runtime_session.revision += 1
            runtime_session.updated_at = event.occurred_at
            self._session.add(_event_row(event, principal))
        return event

    async def list_after(
        self,
        session_id: str,
        after_sequence: int,
        principal: Principal,
    ) -> Sequence[RuntimeEvent]:
        statement: Select[tuple[RuntimeEventRow]] = (
            select(RuntimeEventRow)
            .where(
                RuntimeEventRow.tenant_id == principal.tenant_id,
                RuntimeEventRow.session_id == session_id,
                RuntimeEventRow.sequence > after_sequence,
            )
            .order_by(RuntimeEventRow.sequence)
        )
        rows = (await self._session.scalars(statement)).all()
        return tuple(_event_from_row(row) for row in rows)


def _package_from_row(row: AgentPackageRow) -> AgentPackageRef:
    return AgentPackageRef(
        tenant_id=row.tenant_id,
        agent_id=row.agent_id,
        version=row.version,
        digest=row.digest,
        runtime_type=RuntimeType(row.runtime_type),
        sdk_version=row.sdk_version,
    )


def _session_from_rows(
    row: RuntimeSessionRow, package_row: AgentPackageRow
) -> RuntimeSession:
    return RuntimeSession(
        session_id=row.session_id,
        thread_id=row.thread_id,
        tenant_id=row.tenant_id,
        user_id=row.user_id,
        package=_package_from_row(package_row),
        status=SessionStatus(row.status),
        revision=row.revision,
        execution_epoch=row.execution_epoch,
        active_execution_id=row.active_execution_id,
        last_checkpoint_id=row.last_checkpoint_id,
        last_event_sequence=row.last_event_sequence,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _execution_from_row(row: RuntimeExecutionRow) -> RuntimeExecution:
    return RuntimeExecution(
        execution_id=row.execution_id,
        session_id=row.session_id,
        tenant_id=row.tenant_id,
        user_id=row.user_id,
        actor_id=row.actor_id,
        worker_id=row.worker_id,
        trace_id=row.trace_id,
        execution_epoch=row.execution_epoch,
        mode=ExecutionMode(row.mode),
        status=ExecutionStatus(row.status),
        created_at=row.created_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
    )


def _event_from_row(row: RuntimeEventRow) -> RuntimeEvent:
    return RuntimeEvent(
        schema_version="runtime.event.v1",
        event_id=row.event_id,
        sequence=row.sequence,
        occurred_at=row.occurred_at,
        tenant_id=row.tenant_id,
        trace_id=row.trace_id,
        span_id=row.span_id,
        parent_span_id=row.parent_span_id,
        session_id=row.session_id,
        execution_id=row.execution_id,
        package=AgentPackageRef(
            tenant_id=row.tenant_id,
            agent_id=row.agent_id,
            version=row.package_version,
            digest=row.package_digest,
            runtime_type=RuntimeType(row.runtime_type),
            sdk_version=row.sdk_version,
        ),
        worker_id=row.worker_id,
        sdk_version=row.sdk_version,
        event_type=row.event_type,
        phase=row.phase,
        duration_ms=row.duration_ms,
        payload=row.payload,
        payload_ref=row.payload_ref,
    )


__all__ = [
    "EventRepository",
    "ExecutionRepository",
    "PackageRepository",
    "RuntimeRepositoryError",
    "SessionRepository",
]
