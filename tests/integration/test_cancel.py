from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import UTC, datetime
from typing import Any, cast

from packages.execution_manager.cancellation import RedisCancellationToken
from packages.execution_manager.queue import RedisTaskQueue, task_status_key
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
from tests.integration.test_async_task import RedisDouble as QueueRedisDouble

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


class RedisDouble:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    async def set(
        self,
        name: str,
        value: str,
        *,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool:
        del ex, px
        if nx and name in self.values:
            return False
        self.values[name] = value
        return True

    async def get(self, name: str) -> str | None:
        return self.values.get(name)

    async def hset(self, name: str, mapping: Mapping[str, str]) -> int:
        self.hashes.setdefault(name, {}).update(mapping)
        return len(mapping)

    async def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))


class TerminalDuringCancelRedis(QueueRedisDouble):
    def __init__(self) -> None:
        super().__init__()
        self.after_cancel_set: Callable[[], Any] | None = None

    async def set(
        self,
        name: str,
        value: str,
        *,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool:
        result = await super().set(name, value, nx=nx, ex=ex, px=px)
        if name.startswith("cancel:") and self.after_cancel_set is not None:
            callback = self.after_cancel_set
            self.after_cancel_set = None
            await callback()
        return result


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

    def activate(self, execution_id: str) -> None:
        self.session = self.session.model_copy(
            update={"active_execution_id": execution_id, "execution_epoch": 7}
        )

    async def get_session(self, session_id: str, principal: Principal) -> RuntimeSession | None:
        if session_id != self.session.session_id or principal.tenant_id != "tenant-a":
            return None
        return self.session


class Coordinator:
    def __init__(self, sessions: Sessions, lease: Lease) -> None:
        self.sessions = sessions
        self.lease = lease

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
        del repository, mode, trace_id, request_input
        assert session_id == "session-a"
        assert principal == PRINCIPAL
        self.sessions.activate(execution_id)
        return ExecutionFence(
            tenant_id="tenant-a",
            session_id="session-a",
            execution_id=execution_id,
            execution_epoch=7,
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
        assert execution_epoch == 7
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
        self.terminals.append(status)
        return await self.append(event_factory, principal, execution_epoch)


class ExecutionRepository:
    async def complete_execution(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("terminal state must be persisted by append_terminal")


class BlockingRun:
    def __init__(self, *, tool_phase: bool) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.tool_phase = tool_phase

    def __aiter__(self) -> AsyncIterator[Mapping[str, object]]:
        async def iterator() -> AsyncIterator[Mapping[str, object]]:
            self.started.set()
            if self.tool_phase:
                yield {
                    "method": "tools",
                    "params": {
                        "data": {
                            "event": "tool-started",
                            "tool_name": "query_metric",
                        }
                    },
                }
            await self.release.wait()
            yield {"method": "output", "params": {"data": {"answer": "done"}}}

        return iterator()

    async def output(self) -> dict[str, str]:
        await self.release.wait()
        return {"answer": "done"}


class BlockingGraph:
    def __init__(self, *, tool_phase: bool) -> None:
        self.run = BlockingRun(tool_phase=tool_phase)
        self.calls = 0

    def astream_events(
        self,
        input: object,
        *,
        config: dict[str, dict[str, str | int]],
        version: str,
    ) -> BlockingRun:
        del input, config
        assert version == "v3"
        self.calls += 1
        return self.run


class Factory:
    def __init__(self, graph: BlockingGraph) -> None:
        self.graph = graph

    def get(self, package_digest: str) -> BlockingGraph:
        assert package_digest == PACKAGE.digest
        return self.graph

    def get_limits(self, package_digest: str) -> Any:
        assert package_digest == PACKAGE.digest
        return type("Limits", (), {"execution_timeout_seconds": 5})()


def manager_for(graph: BlockingGraph) -> tuple[ExecutionManager, EventSink, Lease]:
    sessions = Sessions()
    lease = Lease()
    sink = EventSink()
    manager = ExecutionManager(
        agent_factory=Factory(graph),
        session_manager=sessions,
        execution_repository=ExecutionRepository(),
        execution_coordinator=Coordinator(sessions, lease),
        event_sink=sink,
        lease_renew_interval_seconds=0,
    )
    return manager, sink, lease


def test_cancel_during_model_wait_persists_one_terminal_and_releases_lease() -> None:
    graph = BlockingGraph(tool_phase=False)
    manager, sink, lease = manager_for(graph)
    redis = QueueRedisDouble()
    token = RedisCancellationToken(redis, "tenant-a", "execution-model")

    async def scenario() -> Any:
        task = asyncio.create_task(
            manager.execute_async(
                "session-a",
                "wait for model",
                PRINCIPAL,
                execution_id="execution-model",
                trace_id="trace-model",
                cancellation_token=token,
            )
        )
        await graph.run.started.wait()
        await token.cancel()
        return await asyncio.wait_for(task, timeout=2)

    result = asyncio.run(scenario())

    assert result.status is ExecutionStatus.CANCELLED
    assert result.error is not None
    assert result.error.code is ErrorCode.EXECUTION_CANCELLED
    assert sink.terminals == [ExecutionStatus.CANCELLED]
    assert lease.releases == 1


def test_cancel_before_claim_is_observed_by_the_worker_and_is_acked_after_terminal() -> None:
    from apps.runtime_worker.consumer import TaskConsumer

    graph = BlockingGraph(tool_phase=False)
    manager, sink, lease = manager_for(graph)
    redis = QueueRedisDouble()
    queue = RedisTaskQueue(
        redis,
        stream_name="runtime:commands:test",
        consumer_group="runtime-workers-test",
    )

    async def scenario() -> Any:
        await queue.execute_async(
            "session-a",
            "cancel before claim",
            PRINCIPAL,
            execution_id="execution-before-claim",
            trace_id="trace-before-claim",
        )
        response = await queue.cancel_task(
            "execution-before-claim", PRINCIPAL, session_id="session-a"
        )
        consumer = TaskConsumer(
            queue=queue,
            execution_manager=manager,
            principal=PRINCIPAL,
            consumer_name="worker-a",
            claim_block_ms=0,
        )
        result = await consumer.consume_once()
        return response, result

    response, result = asyncio.run(scenario())

    assert response.cancellation_requested
    assert result is not None
    assert result.status is ExecutionStatus.CANCELLED
    assert graph.calls == 0
    assert sink.terminals == [ExecutionStatus.CANCELLED]
    assert lease.releases == 1
    assert redis.acked == ["1-0"]


def test_cancel_during_tool_wait_is_cooperative() -> None:
    graph = BlockingGraph(tool_phase=True)
    manager, sink, _lease = manager_for(graph)
    token = RedisCancellationToken(RedisDouble(), "tenant-a", "execution-tool")

    async def scenario() -> Any:
        task = asyncio.create_task(
            manager.execute_async(
                "session-a",
                "wait for tool",
                PRINCIPAL,
                execution_id="execution-tool",
                trace_id="trace-tool",
                cancellation_token=token,
            )
        )
        await graph.run.started.wait()
        await token.cancel()
        return await asyncio.wait_for(task, timeout=2)

    result = asyncio.run(scenario())

    assert result.status is ExecutionStatus.CANCELLED
    assert sink.terminals == [ExecutionStatus.CANCELLED]


def test_cancel_after_terminal_completion_is_idempotent() -> None:
    from tests.integration.test_sync_execution import GraphDouble, manager_for_test

    manager, _sessions, _coordinator, sink, _executions = manager_for_test(GraphDouble([]))
    redis = QueueRedisDouble()
    queue = RedisTaskQueue(redis, stream_name="commands", consumer_group="workers")

    async def scenario() -> tuple[Any, Any]:
        await redis.hset(
            task_status_key("tenant-a", "execution-done"),
            mapping={
                "command_id": "command-done",
                "execution_id": "execution-done",
                "session_id": "session-a",
                "tenant_id": "tenant-a",
                "user_id": "user-a",
                "actor_id": "actor-a",
                "worker_id": "worker-a",
                "execution_epoch": "7",
                "status": ExecutionStatus.ACCEPTED.value,
                "cancellation_requested": "0",
            },
        )
        result = await manager.execute_async(
            "session-a",
            "finish",
            PRINCIPAL,
            execution_id="execution-done",
            trace_id="trace-done",
            cancellation_token=RedisCancellationToken(redis, "tenant-a", "execution-done"),
            timeout_seconds=60,
        )
        await redis.hset(
            task_status_key("tenant-a", "execution-done"),
            mapping={"status": ExecutionStatus.SUCCEEDED.value},
        )
        response = await queue.cancel_task(
            "execution-done", PRINCIPAL, session_id="session-a"
        )
        return result, response

    result, response = asyncio.run(scenario())

    assert result.status is ExecutionStatus.SUCCEEDED
    assert response.already_terminal
    terminal_events = [
        event
        for event in sink.events
        if event.event_type.startswith("execution.") and event.phase == "terminal"
    ]
    assert len(terminal_events) == 1


def test_cancel_returns_terminal_state_when_completion_wins_during_request() -> None:
    redis = TerminalDuringCancelRedis()
    queue = RedisTaskQueue(redis, stream_name="commands", consumer_group="workers")

    async def scenario() -> Any:
        await queue.execute_async(
            "session-a",
            "race",
            PRINCIPAL,
            execution_id="execution-cancel-race",
            trace_id="trace-cancel-race",
        )

        async def complete_before_cancel_response() -> None:
            await redis.hset(
                task_status_key("tenant-a", "execution-cancel-race"),
                mapping={
                    "status": ExecutionStatus.SUCCEEDED.value,
                    "terminal_committed": "1",
                },
            )

        redis.after_cancel_set = complete_before_cancel_response
        return await queue.cancel_task(
            "execution-cancel-race", PRINCIPAL, session_id="session-a"
        )

    response = asyncio.run(scenario())

    assert response.status is ExecutionStatus.SUCCEEDED
    assert response.already_terminal
    assert not response.cancellation_requested
