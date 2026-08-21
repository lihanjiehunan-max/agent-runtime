from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import JsonValue

from packages.event_model.emitter import EventContext, RuntimeEventEmitter
from packages.event_model.projection import (
    MAX_TRACE_SUMMARY_BYTES,
    PostgresTraceProjectionStore,
    TraceIdentity,
    TraceProjection,
    TraceProjectionError,
    TraceProjector,
)
from packages.event_normalizer.deepagents_v3 import NormalizedEvent
from packages.execution_manager.service import ExecutionManager
from packages.runtime_contracts import (
    AgentPackageRef,
    ExecutionStatus,
    Principal,
    RuntimeEvent,
    RuntimeType,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.models import (
    RuntimeExecutionRow,
    RuntimeSessionRow,
    TraceProjectionRow,
)
from packages.session_manager.locks import ExecutionFence


class DeterministicEventSink:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        del principal, execution_epoch
        event = event_factory(len(self.events) + 1)
        self.events.append(event)
        return event

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        del status, error
        return await self.append(event_factory, principal, execution_epoch)


class DeterministicProjectionStore:
    def __init__(self) -> None:
        self.projections: list[TraceProjection] = []

    async def save(self, projection: TraceProjection) -> None:
        self.projections.append(projection)


class RecordingEmitter:
    def __init__(self) -> None:
        self.calls: list[tuple[NormalizedEvent, object, Principal, int]] = []

    async def emit(
        self,
        normalized: NormalizedEvent,
        context: object,
        principal: Principal,
        *,
        execution_epoch: int,
        terminal_status: ExecutionStatus | None = None,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        del terminal_status, error
        self.calls.append((normalized, context, principal, execution_epoch))
        event_context = cast(EventContext, context)
        return RuntimeEvent(
            schema_version="runtime.event.v1",
            event_id="event-emitted",
            sequence=1,
            occurred_at=datetime(2026, 8, 20, tzinfo=UTC),
            tenant_id=event_context.identity.tenant_id,
            trace_id=event_context.identity.trace_id,
            span_id=normalized.span_id or "span-emitted",
            parent_span_id=normalized.parent_span_id,
            session_id=event_context.identity.session_id,
            execution_id=event_context.identity.execution_id,
            package=event_context.identity.package,
            worker_id=event_context.worker_id,
            sdk_version=event_context.sdk_version,
            event_type=normalized.event_type,
            phase=normalized.phase,
            duration_ms=None,
            payload=normalized.payload,
            payload_ref=None,
        )


class RenewableLease:
    async def renew(self) -> bool:
        return True

    async def release(self) -> bool:
        return True


class _AsyncTransaction:
    async def __aenter__(self) -> _AsyncTransaction:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class _ProjectionSession:
    def __init__(
        self,
        *,
        execution: object,
        runtime_session: object,
    ) -> None:
        self.execution = execution
        self.runtime_session = runtime_session
        self.added: object | None = None
        self.queried: list[type[object]] = []

    async def __aenter__(self) -> _ProjectionSession:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    def begin(self) -> _AsyncTransaction:
        return _AsyncTransaction()

    async def scalar(self, statement: object) -> object:
        entity = cast(
            type[object],
            statement.column_descriptions[0]["entity"],  # type: ignore[attr-defined]
        )
        self.queried.append(entity)
        if entity is RuntimeExecutionRow:
            return self.execution
        if entity is RuntimeSessionRow:
            return self.runtime_session
        if entity is TraceProjectionRow:
            return None
        raise AssertionError(f"unexpected projection query entity: {entity!r}")

    def add(self, row: object) -> None:
        self.added = row


class _ProjectionSessionFactory:
    def __init__(self, session: _ProjectionSession) -> None:
        self.session = session

    def __call__(self) -> _ProjectionSession:
        return self.session


def _context() -> tuple[EventContext, Principal, AgentPackageRef]:
    package = AgentPackageRef(
        tenant_id="tenant-a",
        agent_id="metric-agent",
        version="0.1.0",
        digest=f"sha256:{'a' * 64}",
        runtime_type=RuntimeType.DEEPAGENTS,
        sdk_version="0.7.7",
    )
    identity = TraceIdentity(
        tenant_id="tenant-a",
        trace_id="trace-a",
        session_id="session-a",
        execution_id="execution-a",
        package=package,
    )
    return (
        EventContext(
            identity=identity,
            worker_id="worker-a",
            sdk_version=package.sdk_version,
        ),
        Principal(
            tenant_id="tenant-a",
            user_id="user-a",
            actor_id="actor-a",
            worker_id="worker-a",
        ),
        package,
    )


def _normalized(
    event_type: str,
    phase: str,
    payload: Mapping[str, JsonValue] | None = None,
) -> NormalizedEvent:
    return NormalizedEvent(
        event_type=event_type,
        phase=phase,
        payload=payload or {},
        span_id=f"span-{event_type.replace('.', '-')}",
    )


def _runtime_event(
    context: EventContext,
    *,
    event_id: str,
    sequence: int,
    event_type: str,
    phase: str,
    payload: Mapping[str, JsonValue] | None = None,
) -> RuntimeEvent:
    return RuntimeEvent(
        schema_version="runtime.event.v1",
        event_id=event_id,
        sequence=sequence,
        occurred_at=datetime(2026, 8, 20, tzinfo=UTC),
        tenant_id=context.identity.tenant_id,
        trace_id=context.identity.trace_id,
        span_id=f"span-{event_id}",
        parent_span_id=None,
        session_id=context.identity.session_id,
        execution_id=context.identity.execution_id,
        package=context.identity.package,
        worker_id=context.worker_id,
        sdk_version=context.sdk_version,
        event_type=event_type,
        phase=phase,
        duration_ms=None,
        payload=payload or {},
        payload_ref=None,
    )


def test_one_execution_reconstructs_bounded_trace_with_complete_trace_ids() -> None:
    context, principal, package = _context()
    sink = DeterministicEventSink()
    store = DeterministicProjectionStore()
    emitter = RuntimeEventEmitter(
        event_sink=sink,
        projection_store=store,
        projector=TraceProjector(context.identity),
    )
    normalized = (
        _normalized("package.resolve.started", "resolve"),
        _normalized("package.resolve.completed", "resolve"),
        _normalized("package.cache.miss", "cache"),
        _normalized("session.lock.acquired", "session"),
        _normalized("model.started", "model", {"model_ref": "gateway-default"}),
        _normalized(
            "model.completed",
            "model",
            {"model_ref": "gateway-default", "input_tokens": 4, "output_tokens": 6},
        ),
        _normalized("tool.started", "tool", {"tool_name": "query_metric"}),
        _normalized(
            "tool.completed",
            "tool",
            {"tool_name": "query_metric", "tool_version": "1"},
        ),
        _normalized("checkpoint.saved", "checkpoint", {"checkpoint_id": "cp-a"}),
        _normalized("execution.output", "state", {"message_count": 2}),
        _normalized("execution.succeeded", "terminal", {"status": "succeeded"}),
    )

    asyncio.run(
        emitter.emit_many(
            normalized,
            context,
            principal,
            execution_epoch=3,
        )
    )

    assert len(sink.events) == len(normalized)
    assert all(event.trace_id for event in sink.events)
    assert {event.trace_id for event in sink.events} == {"trace-a"}
    assert store.projections[-1].agent_id == package.agent_id
    assert store.projections[-1].package_version == package.version
    assert store.projections[-1].package_digest == package.digest
    assert store.projections[-1].status == "succeeded"
    assert [entry.event_type for entry in store.projections[-1].timeline] == [
        event.event_type for event in normalized
    ]
    assert store.projections[-1].summary["model"]["output_tokens"] == 6  # type: ignore[index]
    assert store.projections[-1].summary["tool"]["calls"] == 2  # type: ignore[index]


def test_emitter_scopes_projector_to_each_execution_before_durable_append() -> None:
    context, principal, _package = _context()
    second_identity = TraceIdentity(
        tenant_id=context.identity.tenant_id,
        trace_id="trace-b",
        session_id="session-b",
        execution_id="execution-b",
        package=context.identity.package,
    )
    second_context = EventContext(
        identity=second_identity,
        worker_id=context.worker_id,
        sdk_version=context.sdk_version,
    )
    sink = DeterministicEventSink()
    store = DeterministicProjectionStore()
    emitter = RuntimeEventEmitter(
        event_sink=sink,
        projection_store=store,
        projector=TraceProjector(context.identity),
    )

    async def run() -> None:
        await emitter.emit(
            _normalized("execution.started", "started"),
            context,
            principal,
            execution_epoch=1,
        )
        await emitter.emit(
            _normalized("execution.started", "started"),
            second_context,
            principal,
            execution_epoch=1,
        )

    asyncio.run(run())

    assert len(sink.events) == 2
    assert [event.execution_id for event in sink.events] == [
        "execution-a",
        "execution-b",
    ]
    assert [projection.execution_id for projection in store.projections] == [
        "execution-a",
        "execution-b",
    ]


def test_projection_is_idempotent_for_duplicates_and_rejects_forged_identity() -> None:
    context, _principal, _package = _context()
    projector = TraceProjector(context.identity)
    event = RuntimeEvent(
        schema_version="runtime.event.v1",
        event_id="event-a",
        sequence=1,
        occurred_at=datetime(2026, 8, 20, tzinfo=UTC),
        tenant_id="tenant-a",
        trace_id="trace-a",
        span_id="span-a",
        parent_span_id=None,
        session_id="session-a",
        execution_id="execution-a",
        package=context.identity.package,
        worker_id="worker-a",
        sdk_version="0.7.7",
        event_type="execution.started",
        phase="started",
        duration_ms=None,
        payload={},
        payload_ref=None,
    )

    first = projector.apply(event)
    second = projector.apply(event)
    assert first == second
    assert len(second.timeline) == 1

    forged = event.model_copy(update={"tenant_id": "tenant-b"})
    with pytest.raises(TraceProjectionError, match="tenant identity"):
        projector.apply(forged)


def test_projection_orders_status_and_retains_event_identity_when_events_arrive_late() -> None:
    context, _principal, _package = _context()
    projector = TraceProjector(context.identity)

    projector.apply(
        _runtime_event(
            context,
            event_id="event-terminal",
            sequence=2,
            event_type="execution.succeeded",
            phase="terminal",
            payload={"status": "succeeded"},
        )
    )
    projection = projector.apply(
        _runtime_event(
            context,
            event_id="event-started",
            sequence=1,
            event_type="execution.started",
            phase="started",
        )
    )

    assert projection.status == "succeeded"
    assert [entry.event_id for entry in projection.timeline] == [
        "event-started",
        "event-terminal",
    ]
    assert all(entry.event_digest for entry in projection.timeline)
    assert projection.summary["timeline"][0]["event_id"] == "event-started"  # type: ignore[index]


def test_projection_summary_fallback_is_strictly_bounded() -> None:
    context, _principal, _package = _context()
    projector = TraceProjector(context.identity)
    for sequence in range(1, 513):
        projector.apply(
            _runtime_event(
                context,
                event_id=f"event-{sequence}",
                sequence=sequence,
                event_type="model.delta",
                phase="model",
                payload={"text": "x" * 3000},
            )
        )

    summary = projector.snapshot().summary
    encoded = json.dumps(summary, ensure_ascii=False, separators=(",", ":")).encode()
    assert len(encoded) < MAX_TRACE_SUMMARY_BYTES
    assert summary["truncated"] is True  # type: ignore[index]


def test_durable_projection_guard_rejects_stale_and_conflicting_updates() -> None:
    context, _principal, _package = _context()
    projector = TraceProjector(context.identity)
    first = projector.apply(
        _runtime_event(
            context,
            event_id="event-started",
            sequence=1,
            event_type="execution.started",
            phase="started",
        )
    )
    row = first.to_row()
    second = projector.apply(
        _runtime_event(
            context,
            event_id="event-terminal",
            sequence=2,
            event_type="execution.succeeded",
            phase="terminal",
        )
    )

    PostgresTraceProjectionStore._validate_durable_update(  # pyright: ignore[reportPrivateUsage]
        row, second
    )
    assert set(row.event_index) == {"event-started"}

    stale = TraceProjector(context.identity).apply(
        _runtime_event(
            context,
            event_id="event-terminal",
            sequence=2,
            event_type="execution.succeeded",
            phase="terminal",
        )
    )
    with pytest.raises(TraceProjectionError, match="stale"):
        PostgresTraceProjectionStore._validate_durable_update(  # pyright: ignore[reportPrivateUsage]
            row, stale
        )

    conflicting = TraceProjector(context.identity).apply(
        _runtime_event(
            context,
            event_id="event-started",
            sequence=1,
            event_type="execution.started",
            phase="started",
            payload={"unexpected": "changed"},
        )
    )
    with pytest.raises(TraceProjectionError, match="digest conflicts"):
        PostgresTraceProjectionStore._validate_durable_update(  # pyright: ignore[reportPrivateUsage]
            row, conflicting
        )


def test_first_postgres_projection_write_rejects_session_package_mismatch() -> None:
    context, _principal, _package = _context()
    projection = TraceProjector(context.identity).apply(
        _runtime_event(
            context,
            event_id="event-started",
            sequence=1,
            event_type="execution.started",
            phase="started",
        )
    )
    session = _ProjectionSession(
        execution=SimpleNamespace(
            trace_id=context.identity.trace_id,
            session_id=context.identity.session_id,
        ),
        runtime_session=SimpleNamespace(
            tenant_id=context.identity.tenant_id,
            session_id=context.identity.session_id,
            agent_id=context.identity.package.agent_id,
            package_version=context.identity.package.version,
            package_digest="sha256:" + "b" * 64,
        ),
    )
    store = PostgresTraceProjectionStore(
        cast(Any, _ProjectionSessionFactory(session))
    )

    with pytest.raises(TraceProjectionError, match="session package identity"):
        asyncio.run(store.save(projection))

    assert RuntimeSessionRow in session.queried
    assert session.added is None


def test_execution_manager_routes_persistence_through_injected_runtime_emitter() -> None:
    context, principal, _package = _context()
    emitter = RecordingEmitter()
    manager = ExecutionManager(
        agent_factory=cast(Any, object()),
        session_manager=cast(Any, object()),
        execution_repository=cast(Any, object()),
        execution_coordinator=cast(Any, object()),
        event_sink=cast(Any, object()),
        event_emitter=cast(Any, emitter),
        lease_renew_interval_seconds=0,
    )
    fence = ExecutionFence(
        tenant_id=principal.tenant_id,
        session_id=context.identity.session_id,
        execution_id=context.identity.execution_id,
        execution_epoch=3,
        worker_id=principal.worker_id or "",
        lease=cast(Any, RenewableLease()),
    )
    runtime_session = cast(Any, SimpleNamespace(package=context.identity.package))
    events: list[RuntimeEvent] = []
    emitted: list[RuntimeEvent] = []

    async def forward(event: RuntimeEvent) -> None:
        emitted.append(event)

    async def run() -> None:
        await manager._persist_named(  # type: ignore[reportPrivateUsage]
            "model.completed",
            "model",
            {"model_ref": "gateway-default"},
            context.identity.trace_id,
            runtime_session,
            fence,
            principal,
            events,
            forward,
            lease_guard=asyncio.Lock(),
            renew_failures=[],
        )

    asyncio.run(run())
    assert len(emitter.calls) == 1
    assert events[0].event_id == "event-emitted"
    assert emitted == events


@pytest.mark.skip(
    reason=(
        "Live PostgreSQL trace projection verification is explicitly opt-in "
        "and is not run by Task 11"
    ),
)
def test_live_postgres_trace_projection_is_not_claimed() -> None:
    pass
