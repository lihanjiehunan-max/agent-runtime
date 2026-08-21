from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from apps.runtime_api.main import create_api
from packages.execution_manager.cancellation import CancellationToken
from packages.execution_manager.queue import (
    AsyncTaskStatus,
    CancelTaskResponse,
    RedisTaskQueue,
    TaskNotFound,
    task_status_key,
)
from packages.execution_manager.service import ExecutionResult
from packages.runtime_contracts import (
    ErrorCode,
    ExecutionMode,
    ExecutionStatus,
    Principal,
)

PRINCIPAL = Principal(
    tenant_id="tenant-a",
    user_id="user-a",
    actor_id="actor-a",
    worker_id="worker-a",
)


class RedisDouble:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.acked: list[str] = []
        self.added: list[tuple[str, dict[str, str]]] = []
        self.hashes: dict[str, dict[str, str]] = {}
        self.values: dict[str, str] = {}

    async def xgroup_create(
        self, name: str, groupname: str, *, id: str = "$", mkstream: bool = False
    ) -> bool:
        del groupname, id
        if mkstream:
            self.streams.setdefault(name, [])
        return True

    async def xadd(
        self, name: str, fields: Mapping[str, str], *, id: str = "*"
    ) -> str:
        del id
        message_id = f"{len(self.streams.setdefault(name, [])) + 1}-0"
        value = dict(fields)
        self.streams[name].append((message_id, value))
        self.added.append((name, value))
        return message_id

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Mapping[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        del groupname, consumername, count, block
        for stream_name, stream_id in streams.items():
            if stream_id != ">":
                continue
            messages = self.streams.get(stream_name, [])
            if messages:
                message = messages.pop(0)
                return [(stream_name, [message])]
        return []

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        *,
        count: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, str]]], list[str]]:
        del name, groupname, consumername, min_idle_time, start_id, count
        return "0-0", [], []

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        del name, groupname
        self.acked.extend(ids)
        return len(ids)

    async def hset(self, name: str, mapping: Mapping[str, str]) -> int:
        self.hashes.setdefault(name, {}).update(mapping)
        return len(mapping)

    async def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))

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


class AsyncOnlyManager:
    def __init__(self) -> None:
        self.async_calls: list[tuple[str, str, str, str, ExecutionMode]] = []
        self.sync_calls = 0
        self.terminal_committed = False

    async def execute_async(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        *,
        execution_id: str,
        trace_id: str,
        execution_epoch: int,
        cancellation_token: CancellationToken,
        timeout_seconds: float | None,
    ) -> ExecutionResult:
        del principal, cancellation_token, timeout_seconds
        self.async_calls.append(
            (session_id, input_text, execution_id, trace_id, ExecutionMode.ASYNC)
        )
        self.terminal_committed = True
        return ExecutionResult(
            execution_id=execution_id,
            trace_id=trace_id,
            execution_epoch=execution_epoch,
            status=ExecutionStatus.SUCCEEDED,
            output={"answer": "done"},
            events=(),
        )

    async def execute_turn(self, *_args: object, **_kwargs: object) -> ExecutionResult:
        self.sync_calls += 1
        raise AssertionError("ASYNC must not call the inline Task 9 path")


def test_async_command_carries_fence_identity_and_has_no_retry_policy() -> None:
    redis = RedisDouble()
    queue = RedisTaskQueue(
        redis,
        stream_name="runtime:commands:test",
        consumer_group="runtime-workers-test",
    )

    handle = asyncio.run(
        queue.execute_async(
            "session-a",
            "run this",
            PRINCIPAL,
            execution_id="execution-a",
            trace_id="trace-a",
            execution_epoch=9,
        )
    )

    assert handle.execution_id == "execution-a"
    fields = redis.added[0][1]
    assert fields["command_id"] == handle.command_id
    assert fields["tenant_id"] == "tenant-a"
    assert fields["session_id"] == "session-a"
    assert fields["execution_epoch"] == "9"
    assert fields["worker_id"] == "worker-a"
    assert fields["mode"] == ExecutionMode.ASYNC.value
    assert fields["retry_count"] == "0"
    assert fields["max_retries"] == "0"
    status_fields = redis.hashes[task_status_key("tenant-a", "execution-a")]
    assert status_fields["user_id"] == "user-a"
    assert status_fields["actor_id"] == "actor-a"
    assert status_fields["worker_id"] == "worker-a"


def test_consumer_uses_async_path_and_ack_follows_terminal_commit() -> None:
    from apps.runtime_worker.consumer import TaskConsumer

    redis = RedisDouble()
    queue = RedisTaskQueue(
        redis,
        stream_name="runtime:commands:test",
        consumer_group="runtime-workers-test",
    )
    asyncio.run(
        queue.execute_async(
            "session-a",
            "run this",
            PRINCIPAL,
            execution_id="execution-a",
            trace_id="trace-a",
            execution_epoch=9,
        )
    )
    manager = AsyncOnlyManager()
    consumer = TaskConsumer(
        queue=queue,
        execution_manager=manager,
        principal=PRINCIPAL,
        consumer_name="worker-a",
        claim_block_ms=0,
    )

    result = asyncio.run(consumer.consume_once())

    assert result is not None
    assert result.status is ExecutionStatus.SUCCEEDED
    assert manager.async_calls == [
        ("session-a", "run this", "execution-a", "trace-a", ExecutionMode.ASYNC)
    ]
    assert manager.sync_calls == 0
    assert manager.terminal_committed
    assert redis.acked == ["1-0"]
    status = asyncio.run(
        queue.get_task_status("execution-a", PRINCIPAL, session_id="session-a")
    )
    assert status.status is ExecutionStatus.SUCCEEDED


def test_task_status_and_cancellation_are_bound_to_principal_and_session() -> None:
    redis = RedisDouble()
    queue = RedisTaskQueue(
        redis,
        stream_name="runtime:commands:test",
        consumer_group="runtime-workers-test",
    )
    other_principal = Principal(
        tenant_id="tenant-a",
        user_id="user-b",
        actor_id="actor-b",
        worker_id="worker-b",
    )

    async def scenario() -> None:
        await queue.execute_async(
            "session-a",
            "private task",
            PRINCIPAL,
            execution_id="execution-private",
            trace_id="trace-private",
        )
        with pytest.raises(TaskNotFound):
            await queue.get_task_status(
                "execution-private", other_principal, session_id="session-a"
            )
        with pytest.raises(TaskNotFound):
            await queue.cancel_task(
                "execution-private", other_principal, session_id="session-a"
            )
        with pytest.raises(TaskNotFound):
            await queue.get_task_status(
                "execution-private", PRINCIPAL, session_id="session-b"
            )

    asyncio.run(scenario())


class RedeliveringRedisDouble(RedisDouble):
    def __init__(self) -> None:
        super().__init__()
        self.redelivery: tuple[str, dict[str, str]] | None = None

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        *,
        count: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, str]]], list[str]]:
        del name, groupname, consumername, min_idle_time, start_id, count
        if self.redelivery is None:
            return "0-0", [], []
        message = self.redelivery
        self.redelivery = None
        return "0-0", [message], []


def test_consumer_ack_crash_redelivery_does_not_reexecute_terminal_task() -> None:
    from apps.runtime_worker.consumer import TaskConsumer

    redis = RedeliveringRedisDouble()
    queue = RedisTaskQueue(
        redis,
        stream_name="runtime:commands:test",
        consumer_group="runtime-workers-test",
    )
    manager = AsyncOnlyManager()
    consumer = TaskConsumer(
        queue=queue,
        execution_manager=manager,
        principal=PRINCIPAL,
        consumer_name="worker-a",
        claim_block_ms=0,
    )

    async def scenario() -> tuple[ExecutionResult | None, ExecutionResult | None]:
        await queue.execute_async(
            "session-a",
            "run once",
            PRINCIPAL,
            execution_id="execution-redelivery",
            trace_id="trace-redelivery",
        )
        first = await consumer.consume_once()
        assert redis.added
        redis.redelivery = ("1-0", redis.added[0][1])
        second = await consumer.consume_once()
        return first, second

    first, second = asyncio.run(scenario())

    assert first is not None
    assert second is not None
    assert second.status is ExecutionStatus.SUCCEEDED
    assert len(manager.async_calls) == 1
    assert redis.acked == ["1-0", "1-0"]


def test_consumer_rejects_tenant_or_worker_mismatch_without_ack() -> None:
    from apps.runtime_worker.consumer import TaskConsumer, TaskConsumerError

    redis = RedisDouble()
    queue = RedisTaskQueue(
        redis,
        stream_name="runtime:commands:test",
        consumer_group="runtime-workers-test",
    )
    asyncio.run(
        queue.execute_async(
            "session-a",
            "run this",
            PRINCIPAL,
            execution_id="execution-a",
            trace_id="trace-a",
            execution_epoch=9,
        )
    )
    manager = AsyncOnlyManager()
    consumer = TaskConsumer(
        queue=queue,
        execution_manager=manager,
        principal=Principal(
            tenant_id="tenant-b",
            user_id="user-a",
            actor_id="actor-a",
            worker_id="worker-b",
        ),
        consumer_name="worker-b",
        claim_block_ms=0,
    )

    with pytest.raises(TaskConsumerError) as raised:
        asyncio.run(consumer.consume_once())

    assert raised.value.error.code is ErrorCode.TENANT_IDENTITY_MISMATCH
    assert redis.acked == []
    assert manager.async_calls == []


def test_api_task_contract_uses_verified_principal_and_rejects_identity_payload() -> None:
    class TaskServiceDouble:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, Principal]] = []

        async def execute_async(
            self, session_id: str, input_text: str, principal: Principal
        ) -> AsyncTaskStatus:
            self.calls.append((session_id, input_text, principal))
            return AsyncTaskStatus(
                command_id="command-a",
                execution_id="execution-a",
                session_id=session_id,
                tenant_id=principal.tenant_id,
                execution_epoch=0,
                status=ExecutionStatus.ACCEPTED,
            )

        async def get_task_status(
            self,
            execution_id: str,
            principal: Principal,
            *,
            session_id: str,
        ) -> AsyncTaskStatus:
            del execution_id, principal, session_id
            raise AssertionError("status lookup is not used by this route test")

        async def cancel_task(
            self,
            execution_id: str,
            principal: Principal,
            *,
            session_id: str,
        ) -> CancelTaskResponse:
            del principal, session_id
            raise AssertionError(f"cancellation is not used for {execution_id}")

    service = TaskServiceDouble()

    async def verify(token: str) -> Principal | None:
        return PRINCIPAL if token == "valid" else None

    app = create_api(
        principal_verifier=verify,
        task_service=service,
        environ={"RUNTIME_ALLOW_DEV_AUTH": "0"},
    )

    with TestClient(app) as client:
        response = cast(Any, client).post(
            "/api/v1/runtime/sessions/session-a/tasks",
            headers={"Authorization": "Bearer valid"},
            json={
                "input": "run this",
                "tenant_id": "attacker-tenant",
                "worker_id": "attacker-worker",
            },
        )

    assert response.status_code == 422
    assert service.calls == []


def test_api_status_and_cancel_pass_the_authenticated_session_boundary() -> None:
    class TaskServiceDouble:
        def __init__(self) -> None:
            self.status_calls: list[tuple[str, Principal, str]] = []
            self.cancel_calls: list[tuple[str, Principal, str]] = []

        async def execute_async(
            self, session_id: str, input_text: str, principal: Principal
        ) -> AsyncTaskStatus:
            del input_text
            return AsyncTaskStatus(
                command_id="command-a",
                execution_id="execution-a",
                session_id=session_id,
                tenant_id=principal.tenant_id,
                execution_epoch=0,
                status=ExecutionStatus.ACCEPTED,
            )

        async def get_task_status(
            self,
            execution_id: str,
            principal: Principal,
            *,
            session_id: str,
        ) -> AsyncTaskStatus:
            self.status_calls.append((execution_id, principal, session_id))
            return AsyncTaskStatus(
                command_id="command-a",
                execution_id=execution_id,
                session_id=session_id,
                tenant_id=principal.tenant_id,
                execution_epoch=0,
                status=ExecutionStatus.RUNNING,
            )

        async def cancel_task(
            self,
            execution_id: str,
            principal: Principal,
            *,
            session_id: str,
        ) -> CancelTaskResponse:
            self.cancel_calls.append((execution_id, principal, session_id))
            return CancelTaskResponse(
                execution_id=execution_id,
                status=ExecutionStatus.CANCELLED,
                cancellation_requested=True,
                already_terminal=False,
            )

    service = TaskServiceDouble()

    async def verify(token: str) -> Principal | None:
        return PRINCIPAL if token == "valid" else None

    app = create_api(
        principal_verifier=verify,
        task_service=service,
        environ={"RUNTIME_ALLOW_DEV_AUTH": "0"},
    )

    with TestClient(app) as client:
        status_response = cast(Any, client).get(
            "/api/v1/runtime/sessions/session-a/tasks/execution-a",
            headers={"Authorization": "Bearer valid"},
        )
        cancel_response = cast(Any, client).post(
            "/api/v1/runtime/sessions/session-a/tasks/execution-a/cancel",
            headers={"Authorization": "Bearer valid"},
        )

    assert status_response.status_code == 200
    assert cancel_response.status_code == 200
    assert service.status_calls == [("execution-a", PRINCIPAL, "session-a")]
    assert service.cancel_calls == [("execution-a", PRINCIPAL, "session-a")]


__all__ = ["PRINCIPAL", "RedisDouble"]
