from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import cast

from packages.execution_manager.service import TERMINAL_EVENT_TYPES, ExecutionManager
from packages.runtime_contracts import (
    ExecutionStatus,
    RuntimeEvent,
)
from tests.integration.test_sync_execution import (
    PACKAGE,
    PRINCIPAL,
    CoordinatorDouble,
    EventSinkDouble,
    ExecutionRepositoryDouble,
    SessionManagerDouble,
)

EMPTY_METADATA: dict[str, object] = {}


def _raw(method: str, data: object) -> dict[str, object]:
    return {"method": method, "params": {"namespace": [], "data": data}}


class _FailingRun:
    def __aiter__(self) -> AsyncIterator[dict[str, object]]:
        async def iterator() -> AsyncIterator[dict[str, object]]:
            yield _raw(
                "messages",
                ({"event": "message-start", "role": "ai"}, EMPTY_METADATA),
            )
            raise RuntimeError("provider returned Bearer live-secret")

        return iterator()

    async def output(self) -> dict[str, object]:
        return {}


class _StreamingRun:
    def __init__(self, gate: asyncio.Event) -> None:
        self.gate = gate

    def __aiter__(self) -> AsyncIterator[dict[str, object]]:
        async def iterator() -> AsyncIterator[dict[str, object]]:
            yield _raw(
                "messages",
                ({"event": "message-start", "role": "ai"}, EMPTY_METADATA),
            )
            await self.gate.wait()
            yield _raw(
                "messages",
                (
                    {
                        "event": "content-block-delta",
                        "index": 0,
                        "delta": {"type": "text-delta", "text": "done"},
                    },
                    EMPTY_METADATA,
                ),
            )

        return iterator()

    async def output(self) -> dict[str, object]:
        return {"messages": [{"type": "ai", "content": "done"}]}


class _Graph:
    def __init__(self, run: _StreamingRun | _FailingRun) -> None:
        self.run = run

    def astream_events(
        self,
        input: object,
        *,
        config: dict[str, dict[str, str | int]],
        version: str,
    ) -> _StreamingRun | _FailingRun:
        del input
        assert version == "v3"
        assert cast(dict[str, object], config["configurable"])["thread_id"] == "session-a"
        return self.run


class _Factory:
    def __init__(self, graph: _Graph) -> None:
        self.graph = graph

    def get(self, package_digest: str) -> _Graph:
        assert package_digest == PACKAGE.digest
        return self.graph


def _manager(
    graph: _Graph,
) -> tuple[
    ExecutionManager,
    EventSinkDouble,
    ExecutionRepositoryDouble,
    CoordinatorDouble,
]:
    sessions = SessionManagerDouble()
    coordinator = CoordinatorDouble(sessions)
    sink = EventSinkDouble()
    executions = ExecutionRepositoryDouble()
    manager = ExecutionManager(
        agent_factory=_Factory(graph),
        session_manager=sessions,
        execution_repository=executions,
        execution_coordinator=coordinator,
        event_sink=sink,
        lease_renew_interval_seconds=0,
    )
    return manager, sink, executions, coordinator


def test_stream_turn_yields_live_normalized_events_and_one_terminal_event() -> None:
    gate = asyncio.Event()
    manager, sink, executions, coordinator = _manager(_Graph(_StreamingRun(gate)))

    async def scenario() -> list[RuntimeEvent]:
        stream = manager.execute_turn_stream("session-a", "stream this", PRINCIPAL)
        received: list[RuntimeEvent] = []

        async def consume() -> None:
            async for event in stream:
                received.append(event)

        task = asyncio.create_task(consume())
        while not received:
            await asyncio.sleep(0)
        assert received[0].event_type == "execution.accepted"
        assert any(event.event_type == "model.started" for event in received)
        gate.set()
        await task

        assert received[-1].event_type == "execution.succeeded"
        assert sum(event.event_type in TERMINAL_EVENT_TYPES for event in received) == 1
        assert [event.sequence for event in received] == list(
            range(1, len(received) + 1)
        )
        assert executions.completions == []
        assert coordinator.lease.releases == 1
        return received

    received = asyncio.run(scenario())

    assert sink.events == received


def test_stream_failure_redacts_provider_text_and_emits_only_one_failed_terminal() -> None:
    manager, sink, executions, coordinator = _manager(_Graph(_FailingRun()))

    async def scenario() -> list[RuntimeEvent]:
        stream = manager.execute_turn_stream("session-a", "fail this", PRINCIPAL)
        events = [event async for event in stream]
        assert stream.result is not None
        assert stream.result.status is ExecutionStatus.FAILED
        return events

    events = asyncio.run(scenario())
    serialized = " ".join(event.to_json() for event in events)
    assert "live-secret" not in serialized
    assert events[-1].event_type == "execution.failed"
    assert sum(event.event_type in TERMINAL_EVENT_TYPES for event in events) == 1
    assert executions.completions == []
    assert coordinator.lease.releases == 1
    assert sink.events == events
