from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import cast

import pytest
from fastapi.testclient import TestClient
from httpx import Client
from pydantic import JsonValue

from apps.runtime_api.main import create_api
from apps.runtime_api.routes.events import (
    BoundedLiveEventFeed,
    SSEAccessError,
    SSEEventStream,
)
from packages.execution_manager.service import ExecutionResult
from packages.runtime_contracts import (
    AgentPackageRef,
    ExecutionMode,
    Principal,
    RuntimeEvent,
    RuntimeType,
)

PACKAGE = AgentPackageRef(
    tenant_id="tenant-a",
    agent_id="agent-metric-query",
    version="0.1.0",
    digest="sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04",
    runtime_type=RuntimeType.DEEPAGENTS,
    sdk_version="0.7.7",
)
PRINCIPAL = Principal(
    tenant_id="tenant-a",
    user_id="user-a",
    actor_id="actor-a",
    worker_id="worker-a",
)


def _event(
    sequence: int,
    event_type: str,
    payload: dict[str, JsonValue] | None = None,
) -> RuntimeEvent:
    now = datetime(2026, 8, 20, tzinfo=UTC)
    return RuntimeEvent(
        schema_version="runtime.event.v1",
        event_id=f"event-{sequence}",
        sequence=sequence,
        occurred_at=now,
        tenant_id="tenant-a",
        trace_id="trace-a",
        span_id=f"span-{sequence}",
        parent_span_id=None,
        session_id="session-a",
        execution_id="execution-a",
        package=PACKAGE,
        worker_id="worker-a",
        sdk_version="0.7.7",
        event_type=event_type,
        phase="test",
        duration_ms=None,
        payload=payload or {},
        payload_ref=None,
    )


class _Reader:
    def __init__(self, events: Sequence[RuntimeEvent], *, allowed: bool = True) -> None:
        self.events = tuple(events)
        self.allowed = allowed
        self.after_calls: list[int] = []

    async def owns_session(self, session_id: str, principal: Principal) -> bool:
        return self.allowed and session_id == "session-a" and principal.tenant_id == "tenant-a"

    async def list_after(
        self,
        session_id: str,
        after_sequence: int,
        principal: Principal,
    ) -> Sequence[RuntimeEvent]:
        assert session_id == "session-a"
        assert principal.tenant_id == "tenant-a"
        self.after_calls.append(after_sequence)
        return tuple(event for event in self.events if event.sequence > after_sequence)


class _StreamExecutionManager:
    def __init__(self) -> None:
        self.modes: list[ExecutionMode] = []

    async def execute_turn(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        *,
        mode: ExecutionMode = ExecutionMode.SYNC,
    ) -> ExecutionResult:
        del session_id, input_text, principal, mode
        raise AssertionError("sync execution must not be used by stream route")

    def execute_turn_stream(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        *,
        mode: ExecutionMode = ExecutionMode.STREAM,
    ) -> AsyncIterator[RuntimeEvent]:
        del session_id, input_text, principal
        self.modes.append(mode)

        async def events() -> AsyncIterator[RuntimeEvent]:
            yield _event(1, "execution.succeeded", {"output": "safe"})

        return events()


def test_sse_resumes_from_last_event_id_replays_in_order_and_closes_on_terminal() -> None:
    persisted = [_event(1, "execution.accepted"), _event(2, "model.delta", {"text": "safe"})]
    terminal = _event(3, "execution.succeeded", {"output": "safe"})
    reader = _Reader(persisted)
    feed = BoundedLiveEventFeed(max_events=8)
    feed.publish_nowait(terminal)
    stream = SSEEventStream(reader=reader, live_source=feed)

    async def scenario() -> list[str]:
        return [
            chunk
            async for chunk in stream.stream(
                "session-a",
                PRINCIPAL,
                last_event_id="1",
                after_sequence=0,
            )
        ]

    chunks = asyncio.run(scenario())

    assert reader.after_calls == [1]
    assert [chunk.splitlines()[0] for chunk in chunks] == ["id: 2", "id: 3"]
    assert chunks[-1].endswith("\n\n")
    assert all("Bearer" not in chunk for chunk in chunks)


def test_sse_rejects_cross_tenant_or_unknown_session_before_replay() -> None:
    reader = _Reader([], allowed=False)
    stream = SSEEventStream(reader=reader, live_source=None)

    async def scenario() -> None:
        with pytest.raises(SSEAccessError):
            async for _chunk in stream.stream("session-a", PRINCIPAL):
                pass

    asyncio.run(scenario())
    assert reader.after_calls == []


def test_actual_fastapi_app_exposes_sse_router_with_last_event_id() -> None:
    event = _event(2, "execution.succeeded", {"output": "safe"})
    reader = _Reader([event])
    service = SSEEventStream(reader=reader, live_source=None)

    async def verify_token(token: str) -> Principal | None:
        return PRINCIPAL if token == "dev-token" else None

    app = create_api(
        principal_verifier=verify_token,
        event_stream=service,
        environ={"RUNTIME_ALLOW_DEV_AUTH": "1"},
    )

    with TestClient(app) as client:
        typed_client = cast(Client, client)
        response = typed_client.get(
            "/api/v1/runtime/sessions/session-a/events?after_sequence=0",
            headers={"Authorization": "Bearer dev-token", "Last-Event-ID": "1"},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert "id: 2" in response.text
    assert reader.after_calls == [1]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_stream_execution_endpoint_rejects_non_stream_modes_without_inline_execution(
    mode: str,
) -> None:
    manager = _StreamExecutionManager()

    async def verify_token(token: str) -> Principal | None:
        return PRINCIPAL if token == "dev-token" else None

    app = create_api(
        principal_verifier=verify_token,
        execution_manager=manager,
        environ={"RUNTIME_ALLOW_DEV_AUTH": "1"},
    )

    with TestClient(app) as client:
        typed_client = cast(Client, client)
        response = typed_client.post(
            "/api/v1/runtime/sessions/session-a/executions/stream",
            json={"input": "stream this", "mode": mode},
            headers={"Authorization": "Bearer dev-token"},
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "RUNTIME_INCOMPATIBLE"
    assert manager.modes == []


def test_sse_emits_heartbeat_while_waiting_for_a_bounded_live_event() -> None:
    reader = _Reader([])
    feed = BoundedLiveEventFeed(max_events=8)
    stream = SSEEventStream(reader=reader, live_source=feed, heartbeat_seconds=0.01)
    terminal = _event(1, "execution.succeeded", {"output": "safe"})

    async def scenario() -> list[str]:
        async def publish_terminal() -> None:
            await asyncio.sleep(0.025)
            feed.publish_nowait(terminal)

        publisher = asyncio.create_task(publish_terminal())
        try:
            return [
                chunk
                async for chunk in stream.stream("session-a", PRINCIPAL)
            ]
        finally:
            await publisher

    chunks = asyncio.run(scenario())

    assert any(chunk == ": heartbeat\n\n" for chunk in chunks)
    assert chunks[-1].startswith("id: 1\n")
