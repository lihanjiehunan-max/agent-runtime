from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from packages.event_model.payloads import PayloadOffloader, TokenDeltaAggregator
from packages.event_model.projection import (
    TraceIdentity,
    TraceProjectionStore,
    TraceProjector,
)
from packages.event_normalizer.deepagents_v3 import NormalizedEvent
from packages.runtime_contracts import (
    ExecutionStatus,
    Principal,
    RuntimeEvent,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError


class EventEmitterError(RuntimeError):
    """Raised when event emission cannot prove its runtime identity."""


class DurableEventSink(Protocol):
    async def append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent: ...

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent: ...


@dataclass(frozen=True, slots=True)
class EventContext:
    identity: TraceIdentity
    worker_id: str
    sdk_version: str
    parent_span_id: str | None = None


_TERMINAL_EVENT_STATUS = {
    "execution.succeeded": ExecutionStatus.SUCCEEDED,
    "execution.failed": ExecutionStatus.FAILED,
    "execution.timed_out": ExecutionStatus.TIMED_OUT,
    "execution.cancelled": ExecutionStatus.CANCELLED,
}


class RuntimeEventEmitter:
    """Attach durable runtime identity, payload policy, projection, and metrics hooks."""

    def __init__(
        self,
        *,
        event_sink: DurableEventSink,
        projection_store: TraceProjectionStore | None,
        projector: TraceProjector | None = None,
        projector_factory: Callable[[TraceIdentity], TraceProjector] | None = None,
        payload_offloader: PayloadOffloader | None = None,
        metrics: object | None = None,
        id_factory: Callable[[], str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if projection_store is None:
            raise EventEmitterError("PostgreSQL trace projection is required")
        if projector is not None and projector_factory is not None:
            raise ValueError("provide projector or projector_factory, not both")
        self._event_sink = event_sink
        self._projection_store = projection_store
        self._projector_factory = projector_factory or TraceProjector
        self._projectors: dict[tuple[str, ...], TraceProjector] = {}
        if projector is not None:
            self._projectors[self._identity_key(projector.identity)] = projector
        self._payload_offloader = payload_offloader or PayloadOffloader(
            None,
            bucket_name="runtime-payloads",
        )
        self._metrics = metrics
        self._id_factory = id_factory or (lambda: str(uuid4()))
        self._clock = clock or (lambda: datetime.now(UTC))

    async def emit(
        self,
        normalized: NormalizedEvent,
        context: EventContext,
        principal: Principal,
        *,
        execution_epoch: int,
        terminal_status: ExecutionStatus | None = None,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        self._validate_context(context, principal)
        projector = self._projector_for(context.identity)
        event_id = self._id_factory()
        status = terminal_status or _TERMINAL_EVENT_STATUS.get(normalized.event_type)

        def event_factory(sequence: int) -> RuntimeEvent:
            prepared = self._payload_offloader.prepare(
                tenant_id=context.identity.tenant_id,
                execution_id=context.identity.execution_id,
                event_id=event_id,
                payload=normalized.payload,
            )
            return RuntimeEvent(
                schema_version="runtime.event.v1",
                event_id=event_id,
                sequence=sequence,
                occurred_at=self._clock(),
                tenant_id=context.identity.tenant_id,
                trace_id=context.identity.trace_id,
                span_id=normalized.span_id or self._id_factory(),
                parent_span_id=normalized.parent_span_id or context.parent_span_id,
                session_id=context.identity.session_id,
                execution_id=context.identity.execution_id,
                package=context.identity.package,
                worker_id=context.worker_id,
                sdk_version=context.sdk_version,
                event_type=normalized.event_type,
                phase=normalized.phase,
                duration_ms=None,
                payload=prepared.payload,
                payload_ref=prepared.payload_ref,
            )

        if status is None:
            event = await self._event_sink.append(
                event_factory,
                principal,
                execution_epoch,
            )
        else:
            event = await self._event_sink.append_terminal(
                event_factory,
                principal,
                execution_epoch,
                status,
                error,
            )
        projection = projector.apply(event)
        await self._projection_store.save(projection)
        self._observe_metric(event)
        if status is not None:
            self._projectors.pop(self._identity_key(context.identity), None)
        return event

    async def emit_many(
        self,
        normalized_events: Iterable[NormalizedEvent],
        context: EventContext,
        principal: Principal,
        *,
        execution_epoch: int,
        error: RuntimeContractError | None = None,
    ) -> tuple[RuntimeEvent, ...]:
        normalized = TokenDeltaAggregator().aggregate(normalized_events)
        emitted: list[RuntimeEvent] = []
        for event in normalized:
            emitted.append(
                await self.emit(
                    event,
                    context,
                    principal,
                    execution_epoch=execution_epoch,
                    error=error,
                )
            )
        return tuple(emitted)

    @staticmethod
    def _validate_context(context: EventContext, principal: Principal) -> None:
        if context.identity.tenant_id != principal.tenant_id:
            raise EventEmitterError("event tenant identity does not match principal")
        if principal.worker_id is None or context.worker_id != principal.worker_id:
            raise EventEmitterError("event worker identity does not match principal")
        if context.sdk_version != context.identity.package.sdk_version:
            raise EventEmitterError("event SDK identity does not match package")

    @staticmethod
    def _identity_key(identity: TraceIdentity) -> tuple[str, ...]:
        package = identity.package
        return (
            identity.tenant_id,
            identity.trace_id,
            identity.session_id,
            identity.execution_id,
            package.tenant_id,
            package.agent_id,
            package.version,
            package.digest,
            package.runtime_type.value,
            package.sdk_version,
        )

    def _projector_for(self, identity: TraceIdentity) -> TraceProjector:
        key = self._identity_key(identity)
        projector = self._projectors.get(key)
        if projector is None:
            projector = self._projector_factory(identity)
            if projector.identity != identity:
                raise EventEmitterError("projector identity does not match execution")
            self._projectors[key] = projector
        return projector

    def _observe_metric(self, event: RuntimeEvent) -> None:
        observer = getattr(self._metrics, "observe_event", None)
        if callable(observer):
            observer(event)


__all__ = [
    "DurableEventSink",
    "EventContext",
    "EventEmitterError",
    "RuntimeEventEmitter",
]
