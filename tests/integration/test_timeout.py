from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from packages.execution_manager.cancellation import RedisCancellationToken
from packages.execution_manager.service import ExecutionManager
from packages.runtime_contracts import (
    AgentPackageRef,
    ErrorCode,
    ExecutionMode,
    ExecutionStatus,
    Principal,
    RuntimeEvent,
    RuntimeSession,
    RuntimeType,
    SessionStatus,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.repositories import RuntimeRepositoryError
from packages.session_manager.locks import ExecutionFence

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


class Lease:
    tenant_id = "tenant-a"
    session_id = "session-a"
    owner_id = "worker-a"
    active = True

    def __init__(self) -> None:
        self.releases = 0

    async def renew(self) -> bool:
        return True

    async def release(self) -> bool:
        self.releases += 1
        self.active = False
        return True


class Sessions:
    def __init__(self) -> None:
        timestamp = datetime(2026, 8, 20, tzinfo=UTC)
        self.session = RuntimeSession(
            session_id="session-a",
            thread_id="session-a",
            tenant_id="tenant-a",
            user_id="user-a",
            package=PACKAGE,
            status=SessionStatus.OPEN,
            revision=0,
            execution_epoch=0,
            active_execution_id=None,
            last_checkpoint_id=None,
            last_event_sequence=0,
            created_at=timestamp,
            updated_at=timestamp,
        )

    async def get_session(self, session_id: str, principal: Principal) -> RuntimeSession | None:
        if session_id != "session-a" or principal.tenant_id != "tenant-a":
            return None
        return self.session


class Coordinator:
    def __init__(self, lease: Lease, sessions: Sessions) -> None:
        self.lease = lease
        self.sessions = sessions

    async def begin_execution(
        self,
        repository: object,
        session_id: str,
        execution_id: str,
        principal: Principal,
        *,
        mode: ExecutionMode,
        trace_id: str,
        request_input: str,
    ) -> ExecutionFence:
        del repository, session_id, trace_id, request_input
        assert mode is ExecutionMode.ASYNC
        assert principal == PRINCIPAL
        self.sessions.session = self.sessions.session.model_copy(
            update={"active_execution_id": execution_id, "execution_epoch": 8}
        )
        return ExecutionFence(
            tenant_id="tenant-a",
            session_id="session-a",
            execution_id=execution_id,
            execution_epoch=8,
            worker_id="worker-a",
            lease=cast(Any, self.lease),
        )


class EventSink:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []
        self.terminals: list[ExecutionStatus] = []

    async def append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        assert principal == PRINCIPAL
        assert execution_epoch == 8
        if self.terminals:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="late worker write fenced",
                )
            )
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
        del error
        if self.terminals:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="terminal winner already persisted",
                )
            )
        event = event_factory(len(self.events) + 1)
        self.events.append(event)
        self.terminals.append(status)
        return event


class ExecutionRepository:
    async def complete_execution(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("terminal status must use append_terminal")


class SlowRun:
    def __init__(self) -> None:
        self.cancelled = False

    def __aiter__(self) -> AsyncIterator[Mapping[str, object]]:
        async def iterator() -> AsyncIterator[Mapping[str, object]]:
            yield {
                "method": "values",
                "params": {
                    "data": {
                        "messages": [{"type": "ai", "content": "safe evidence"}]
                    }
                },
            }
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise

        return iterator()

    async def output(self) -> dict[str, str]:
        await asyncio.Event().wait()
        return {"partial": "safe evidence"}


class SlowGraph:
    def __init__(self) -> None:
        self.run = SlowRun()
        self.calls = 0

    def astream_events(
        self,
        input: object,
        *,
        config: dict[str, dict[str, str | int]],
        version: str,
    ) -> SlowRun:
        del input, config
        assert version == "v3"
        self.calls += 1
        return self.run


class Factory:
    def __init__(self, graph: SlowGraph) -> None:
        self.graph = graph

    def get(self, package_digest: str) -> SlowGraph:
        assert package_digest == PACKAGE.digest
        return self.graph

    def get_limits(self, package_digest: str) -> Any:
        assert package_digest == PACKAGE.digest
        return type("Limits", (), {"execution_timeout_seconds": 0.1})()


def test_package_timeout_persists_bounded_partial_output_and_fences_late_writes() -> None:
    sessions = Sessions()
    lease = Lease()
    sink = EventSink()
    graph = SlowGraph()
    manager = ExecutionManager(
        agent_factory=Factory(graph),
        session_manager=sessions,
        execution_repository=ExecutionRepository(),
        execution_coordinator=Coordinator(lease, sessions),
        event_sink=sink,
        lease_renew_interval_seconds=0,
    )

    async def scenario() -> Any:
        result = await manager.execute_async(
            "session-a",
            "slow",
            PRINCIPAL,
            execution_id="execution-timeout",
            trace_id="trace-timeout",
            cancellation_token=RedisCancellationToken(
                _RedisDouble(), "tenant-a", "execution-timeout"
            ),
        )
        return result, graph.run.cancelled

    result, graph_cancelled = asyncio.run(scenario())

    assert result.status is ExecutionStatus.TIMED_OUT
    assert graph_cancelled
    assert result.error is not None
    assert result.error.code is ErrorCode.EXECUTION_TIMED_OUT
    assert result.output == {
        "message_count": 1,
        "last_message_type": "ai",
        "last_message_text": "safe evidence",
    }
    assert sink.terminals == [ExecutionStatus.TIMED_OUT]
    assert lease.releases == 1
    assert sink.events[-1].event_type == "execution.timed_out"
    assert sink.events[-1].payload["partial_output"] == {
        "message_count": 1,
        "last_message_type": "ai",
        "last_message_text": "safe evidence",
    }
    with pytest.raises(RuntimeRepositoryError) as late_write:
        asyncio.run(sink.append(lambda _sequence: sink.events[-1], PRINCIPAL, 8))
    assert late_write.value.error.code is ErrorCode.EXECUTION_FENCED


def test_simultaneous_timeout_and_cancel_has_one_durable_terminal_winner() -> None:
    sessions = Sessions()
    lease = Lease()
    sink = EventSink()
    redis = _RedisDouble()
    token = RedisCancellationToken(redis, "tenant-a", "execution-race")
    manager = ExecutionManager(
        agent_factory=Factory(SlowGraph()),
        session_manager=sessions,
        execution_repository=ExecutionRepository(),
        execution_coordinator=Coordinator(lease, sessions),
        event_sink=sink,
        lease_renew_interval_seconds=0,
    )

    async def scenario() -> Any:
        async def cancel_at_deadline() -> None:
            await asyncio.sleep(0.1)
            await token.cancel()

        worker = asyncio.create_task(
            manager.execute_async(
                "session-a",
                "race",
                PRINCIPAL,
                execution_id="execution-race",
                trace_id="trace-race",
                cancellation_token=token,
            )
        )
        canceller = asyncio.create_task(cancel_at_deadline())
        result = await worker
        await canceller
        return result

    result = asyncio.run(scenario())

    assert result.status in {
        ExecutionStatus.CANCELLED,
        ExecutionStatus.TIMED_OUT,
    }
    assert len(sink.terminals) == 1


class _RedisDouble:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(
        self,
        name: str,
        value: str,
        *,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool:
        del nx, ex, px
        self.values[name] = value
        return True

    async def get(self, name: str) -> str | None:
        return self.values.get(name)
