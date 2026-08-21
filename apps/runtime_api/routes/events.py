from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Annotated, Protocol, cast

from fastapi import APIRouter, Depends, Header, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.runtime_api.dependencies import api_error, current_principal
from packages.runtime_contracts import ErrorCode, Principal, RuntimeEvent
from packages.runtime_persistence.repositories import EventRepository, SessionRepository

router = APIRouter(prefix="/api/v1/runtime/sessions", tags=["events"])

TERMINAL_EVENT_TYPES = frozenset(
    {
        "execution.succeeded",
        "execution.failed",
        "execution.timed_out",
        "execution.cancelled",
    }
)


class SSEAccessError(RuntimeError):
    """Raised before a stream when the principal cannot access the Session."""


class SSEResumeError(ValueError):
    """Raised when a resume cursor cannot be interpreted safely."""


class EventReader(Protocol):
    async def owns_session(self, session_id: str, principal: Principal) -> bool: ...

    async def list_after(
        self,
        session_id: str,
        after_sequence: int,
        principal: Principal,
    ) -> Sequence[RuntimeEvent]: ...


class LiveEventSource(Protocol):
    def subscribe(
        self,
        session_id: str,
        principal: Principal,
        after_sequence: int,
    ) -> AsyncIterator[RuntimeEvent]: ...


class EventRepositoryReader:
    """Tenant/user-scoped adapter over the durable Task 8 repositories."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def owns_session(self, session_id: str, principal: Principal) -> bool:
        async with self._session_factory() as database_session:
            runtime_session = await SessionRepository(database_session).get(
                session_id,
                principal,
            )
            return runtime_session is not None and runtime_session.user_id == principal.user_id

    async def list_after(
        self,
        session_id: str,
        after_sequence: int,
        principal: Principal,
    ) -> Sequence[RuntimeEvent]:
        async with self._session_factory() as database_session:
            return await EventRepository(database_session).list_after(
                session_id,
                after_sequence,
                principal,
            )


class BoundedLiveEventFeed:
    """A bounded in-process live-feed protocol for deterministic tests and local runs.

    This is deliberately not a Redis implementation. Production wiring must provide a
    live source explicitly; durable replay remains the source of truth when this feed
    drops an event because its bounded queue is full.
    """

    def __init__(self, *, max_events: int = 256) -> None:
        if max_events < 1:
            raise ValueError("max_events must be positive")
        self._max_events = max_events
        self._events: deque[RuntimeEvent] = deque(maxlen=max_events)
        self._subscribers: set[asyncio.Queue[RuntimeEvent]] = set()

    def publish_nowait(self, event: RuntimeEvent) -> None:
        self._events.append(event)
        for subscriber in tuple(self._subscribers):
            if subscriber.full():
                subscriber.get_nowait()
            subscriber.put_nowait(event)

    def subscribe(
        self,
        session_id: str,
        principal: Principal,
        after_sequence: int,
    ) -> AsyncIterator[RuntimeEvent]:
        return self._subscribe(session_id, principal, after_sequence)

    async def _subscribe(
        self,
        session_id: str,
        principal: Principal,
        after_sequence: int,
    ) -> AsyncIterator[RuntimeEvent]:
        queue: asyncio.Queue[RuntimeEvent] = asyncio.Queue(maxsize=self._max_events)
        self._subscribers.add(queue)
        cursor = after_sequence
        try:
            for event in tuple(self._events):
                if _visible_event(event, session_id, principal) and event.sequence > cursor:
                    cursor = event.sequence
                    yield event
            while True:
                event = await queue.get()
                if not _visible_event(event, session_id, principal) or event.sequence <= cursor:
                    continue
                cursor = event.sequence
                yield event
        finally:
            self._subscribers.discard(queue)


class SSEEventStream:
    def __init__(
        self,
        *,
        reader: EventReader,
        live_source: LiveEventSource | None,
        heartbeat_seconds: float = 15.0,
    ) -> None:
        if heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")
        self._reader = reader
        self._live_source = live_source
        self._heartbeat_seconds = heartbeat_seconds

    @staticmethod
    def parse_last_event_id(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            sequence = int(value)
        except ValueError as error:
            raise SSEResumeError("Last-Event-ID must be a non-negative integer") from error
        if sequence < 0:
            raise SSEResumeError("Last-Event-ID must be a non-negative integer")
        return sequence

    async def ensure_access(self, session_id: str, principal: Principal) -> None:
        if not await self._reader.owns_session(session_id, principal):
            raise SSEAccessError("session is unavailable")

    async def stream(
        self,
        session_id: str,
        principal: Principal,
        *,
        last_event_id: str | None = None,
        after_sequence: int = 0,
    ) -> AsyncIterator[str]:
        if after_sequence < 0:
            raise SSEResumeError("after_sequence must be a non-negative integer")
        await self.ensure_access(session_id, principal)
        header_sequence = self.parse_last_event_id(last_event_id)
        cursor = max(after_sequence, header_sequence or 0)

        persisted = await self._reader.list_after(session_id, cursor, principal)
        for event in persisted:
            if not _visible_event(event, session_id, principal) or event.sequence <= cursor:
                continue
            cursor = event.sequence
            yield event.to_sse()
            if event.event_type in TERMINAL_EVENT_TYPES:
                return

        if self._live_source is None:
            return

        live_iterator = self._live_source.subscribe(session_id, principal, cursor)
        pending: asyncio.Task[RuntimeEvent] = asyncio.create_task(
            _next_live_event(live_iterator)
        )
        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        asyncio.shield(pending),
                        timeout=self._heartbeat_seconds,
                    )
                except TimeoutError:
                    yield ": heartbeat\n\n"
                    continue
                except StopAsyncIteration:
                    return
                pending = asyncio.create_task(_next_live_event(live_iterator))
                if not _visible_event(event, session_id, principal) or event.sequence <= cursor:
                    continue
                cursor = event.sequence
                yield event.to_sse()
                if event.event_type in TERMINAL_EVENT_TYPES:
                    return
        finally:
            if not pending.done():
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            close = getattr(live_iterator, "aclose", None)
            if callable(close):
                await cast(Callable[[], Awaitable[object]], close)()


async def _next_live_event(iterator: AsyncIterator[RuntimeEvent]) -> RuntimeEvent:
    return await anext(iterator)


def _visible_event(event: RuntimeEvent, session_id: str, principal: Principal) -> bool:
    return event.session_id == session_id and event.tenant_id == principal.tenant_id


def event_stream_service(request: Request) -> SSEEventStream:
    service = getattr(request.app.state, "event_stream", None)
    if not isinstance(service, SSEEventStream):
        raise api_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ErrorCode.RUNTIME_INCOMPATIBLE,
            "SSE event streaming is not configured",
        )
    return service


@router.get("/{session_id}/events")
async def stream_session_events(
    session_id: str,
    principal: Annotated[Principal, Depends(current_principal)],
    service: Annotated[SSEEventStream, Depends(event_stream_service)],
    after_sequence: Annotated[int, Query(ge=0)] = 0,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
) -> StreamingResponse:
    try:
        await service.ensure_access(session_id, principal)
        service.parse_last_event_id(last_event_id)
    except SSEAccessError as error:
        raise api_error(
            status.HTTP_404_NOT_FOUND,
            ErrorCode.SESSION_CLOSED,
            str(error),
        ) from error
    except SSEResumeError as error:
        raise api_error(
            status.HTTP_400_BAD_REQUEST,
            ErrorCode.RUNTIME_INCOMPATIBLE,
            str(error),
        ) from error

    return StreamingResponse(
        service.stream(
            session_id,
            principal,
            last_event_id=last_event_id,
            after_sequence=after_sequence,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


__all__ = [
    "BoundedLiveEventFeed",
    "EventReader",
    "EventRepositoryReader",
    "LiveEventSource",
    "SSEAccessError",
    "SSEEventStream",
    "SSEResumeError",
    "router",
]
