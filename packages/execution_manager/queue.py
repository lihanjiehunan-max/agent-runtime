from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol, cast

from pydantic import Field, NonNegativeInt, PositiveFloat, field_validator

from packages.execution_manager.cancellation import (
    CancellationRedis,
    RedisCancellationToken,
)
from packages.execution_manager.service import ExecutionResult
from packages.runtime_contracts import (
    ErrorCode,
    ExecutionMode,
    ExecutionStatus,
    Principal,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_contracts.identity import CommandContract, FrozenContract, Identifier


class RedisStreams(Protocol):
    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        *,
        id: str = "$",
        mkstream: bool = False,
    ) -> bool | int | str | bytes | None: ...

    async def xadd(
        self,
        name: str,
        fields: Mapping[str, str],
        *,
        id: str = "*",
    ) -> str | bytes: ...

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Mapping[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> object: ...

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        *,
        count: int | None = None,
    ) -> object: ...

    async def xack(self, name: str, groupname: str, *ids: str) -> int: ...

    async def hset(self, name: str, mapping: Mapping[str, str]) -> int: ...

    async def hgetall(self, name: str) -> Mapping[str, str | bytes]: ...


class RedisTaskClient(RedisStreams, CancellationRedis, Protocol):
    pass


class QueueError(Exception):
    def __init__(self, error: RuntimeContractError) -> None:
        super().__init__(error.message)
        self.error = error


class TaskNotFound(QueueError):
    pass


class TaskCommand(CommandContract):
    schema_version: Literal["runtime.task.command.v1"] = "runtime.task.command.v1"
    command_id: Identifier
    execution_id: Identifier
    trace_id: Identifier
    session_id: Identifier
    tenant_id: Identifier
    user_id: Identifier
    actor_id: Identifier
    worker_id: Identifier
    input: str = Field(min_length=1, max_length=32_768)
    mode: Literal[ExecutionMode.ASYNC] = ExecutionMode.ASYNC
    execution_epoch: NonNegativeInt = 0
    timeout_seconds: PositiveFloat | None = None
    retry_count: NonNegativeInt = 0
    max_retries: Literal[0] = 0

    @field_validator("retry_count")
    @classmethod
    def retries_are_disabled(cls, value: int) -> int:
        if value != 0:
            raise ValueError("automatic retry is disabled for runtime task commands")
        return value

    def to_fields(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "command_id": self.command_id,
            "execution_id": self.execution_id,
            "trace_id": self.trace_id,
            "session_id": self.session_id,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "actor_id": self.actor_id,
            "worker_id": self.worker_id,
            "input": self.input,
            "mode": self.mode.value,
            "execution_epoch": str(self.execution_epoch),
            "timeout_seconds": (
                str(self.timeout_seconds) if self.timeout_seconds is not None else ""
            ),
            "retry_count": str(self.retry_count),
            "max_retries": str(self.max_retries),
        }


AsyncTaskCommand = TaskCommand


class AsyncTaskStatus(FrozenContract):
    command_id: Identifier
    execution_id: Identifier
    session_id: Identifier
    tenant_id: Identifier
    execution_epoch: NonNegativeInt
    status: ExecutionStatus
    cancellation_requested: bool = False
    error: RuntimeContractError | None = None

    def to_body(self) -> dict[str, object]:
        return {
            "command_id": self.command_id,
            "execution_id": self.execution_id,
            "session_id": self.session_id,
            "tenant_id": self.tenant_id,
            "execution_epoch": self.execution_epoch,
            "status": self.status.value,
            "cancellation_requested": self.cancellation_requested,
            "error": self.error.model_dump(mode="json") if self.error else None,
        }


@dataclass(frozen=True, slots=True)
class TaskHandle:
    command_id: str
    execution_id: str
    stream_id: str

    def to_body(self) -> dict[str, str]:
        return {
            "command_id": self.command_id,
            "execution_id": self.execution_id,
            "stream_id": self.stream_id,
        }


@dataclass(frozen=True, slots=True)
class CancelTaskResponse:
    execution_id: str
    status: ExecutionStatus
    cancellation_requested: bool
    already_terminal: bool

    def to_body(self) -> dict[str, object]:
        return {
            "execution_id": self.execution_id,
            "status": self.status.value,
            "cancellation_requested": self.cancellation_requested,
            "already_terminal": self.already_terminal,
        }


@dataclass(frozen=True, slots=True)
class ClaimedTask:
    message_id: str
    command: TaskCommand


def task_status_key(tenant_id: str, execution_id: str) -> str:
    return f"runtime:task:{tenant_id}:{execution_id}"


class RedisTaskQueue:
    def __init__(
        self,
        redis: RedisTaskClient,
        *,
        stream_name: str = "runtime:execution-commands",
        consumer_group: str = "runtime-execution-workers",
        claim_idle_ms: int = 1_000,
        claim_block_ms: int = 100,
        default_timeout_seconds: float | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        if claim_idle_ms < 0 or claim_block_ms < 0:
            raise ValueError("Redis claim and wait bounds cannot be negative")
        if default_timeout_seconds is not None and default_timeout_seconds <= 0:
            raise ValueError("default task timeout must be positive")
        self.redis = redis
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self.claim_idle_ms = claim_idle_ms
        self.claim_block_ms = claim_block_ms
        self.default_timeout_seconds = default_timeout_seconds
        self._id_factory = id_factory or _uuid_hex
        self._group_ready = False

    async def execute_async(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        *,
        execution_id: str | None = None,
        trace_id: str | None = None,
        execution_epoch: int = 0,
        timeout_seconds: float | None = None,
    ) -> TaskHandle:
        if principal.worker_id is None:
            raise QueueError(
                RuntimeContractError(
                    code=ErrorCode.TENANT_IDENTITY_MISMATCH,
                    message="async task submission requires a verified worker identity",
                )
            )
        if execution_epoch < 0:
            raise ValueError("execution epoch cannot be negative")
        selected_timeout = (
            timeout_seconds
            if timeout_seconds is not None
            else self.default_timeout_seconds
        )
        command = TaskCommand(
            command_id=f"command-{self._id_factory()}",
            execution_id=execution_id or f"execution-{self._id_factory()}",
            trace_id=trace_id or f"trace-{self._id_factory()}",
            session_id=session_id,
            tenant_id=principal.tenant_id,
            user_id=principal.user_id,
            actor_id=principal.actor_id,
            worker_id=principal.worker_id,
            input=input_text,
            execution_epoch=execution_epoch,
            timeout_seconds=selected_timeout,
        )
        await self._ensure_group()
        stream_id = _as_text(
            await self.redis.xadd(self.stream_name, command.to_fields(), id="*")
        )
        await self.redis.hset(
            task_status_key(command.tenant_id, command.execution_id),
            mapping={
                "command_id": command.command_id,
                "execution_id": command.execution_id,
                "session_id": command.session_id,
                "tenant_id": command.tenant_id,
                "user_id": command.user_id,
                "actor_id": command.actor_id,
                "worker_id": command.worker_id,
                "execution_epoch": str(command.execution_epoch),
                "status": ExecutionStatus.ACCEPTED.value,
                "cancellation_requested": "0",
            },
        )
        return TaskHandle(command.command_id, command.execution_id, stream_id)

    async def get_task_status(
        self,
        execution_id: str,
        principal: Principal,
        *,
        session_id: str,
    ) -> AsyncTaskStatus:
        raw = await self.redis.hgetall(task_status_key(principal.tenant_id, execution_id))
        if not raw:
            raise _task_not_found()
        values = {_as_text(key): _as_text(value) for key, value in raw.items()}
        if not _task_matches_principal(values, principal) or values.get(
            "session_id"
        ) != session_id:
            raise _task_not_found()
        error = _error_from_fields(values)
        return AsyncTaskStatus(
            command_id=values["command_id"],
            execution_id=values["execution_id"],
            session_id=values["session_id"],
            tenant_id=values["tenant_id"],
            execution_epoch=int(values.get("execution_epoch", "0")),
            status=ExecutionStatus(values.get("status", ExecutionStatus.ACCEPTED.value)),
            cancellation_requested=values.get("cancellation_requested") == "1",
            error=error,
        )

    async def cancel_task(
        self,
        execution_id: str,
        principal: Principal,
        *,
        session_id: str,
    ) -> CancelTaskResponse:
        current = await self.get_task_status(
            execution_id, principal, session_id=session_id
        )
        if current.status in _TERMINAL_STATUSES:
            return _terminal_cancel_response(execution_id, current.status)
        token = RedisCancellationToken(self.redis, principal.tenant_id, execution_id)
        await token.cancel()
        observed = await self.get_task_status(
            execution_id, principal, session_id=session_id
        )
        if observed.status in _TERMINAL_STATUSES:
            return _terminal_cancel_response(execution_id, observed.status)
        await self.redis.hset(
            task_status_key(principal.tenant_id, execution_id),
            mapping={"cancellation_requested": "1"},
        )
        final = await self.get_task_status(
            execution_id, principal, session_id=session_id
        )
        if final.status in _TERMINAL_STATUSES:
            return _terminal_cancel_response(execution_id, final.status)
        return CancelTaskResponse(
            execution_id=execution_id,
            status=final.status,
            cancellation_requested=True,
            already_terminal=False,
        )

    async def claim_once(
        self,
        consumer_name: str,
        *,
        block_ms: int | None = None,
    ) -> ClaimedTask | None:
        await self._ensure_group()
        claimed = await self.redis.xautoclaim(
            self.stream_name,
            self.consumer_group,
            consumer_name,
            self.claim_idle_ms,
            "0-0",
            count=1,
        )
        claimed_messages = _messages_from_autoclaim(claimed)
        if claimed_messages:
            message_id, fields = claimed_messages[0]
            return ClaimedTask(message_id, _command_from_fields(fields))
        response = await self.redis.xreadgroup(
            self.consumer_group,
            consumer_name,
            {self.stream_name: ">"},
            count=1,
            block=self.claim_block_ms if block_ms is None else block_ms,
        )
        messages = _messages_from_read(response, self.stream_name)
        if not messages:
            return None
        message_id, fields = messages[0]
        return ClaimedTask(message_id, _command_from_fields(fields))

    async def mark_claimed(self, command: TaskCommand) -> None:
        await self.redis.hset(
            task_status_key(command.tenant_id, command.execution_id),
            mapping={"status": ExecutionStatus.RUNNING.value},
        )

    async def mark_terminal(self, result: ExecutionResult, command: TaskCommand) -> None:
        values = {
            "status": result.status.value,
            "execution_epoch": str(result.execution_epoch),
            "terminal_committed": "1",
        }
        if result.error is not None:
            values["error_code"] = result.error.code.value
            values["error_message"] = result.error.message
        await self.redis.hset(
            task_status_key(command.tenant_id, command.execution_id), mapping=values
        )

    async def ack(self, message_id: str) -> None:
        await self.redis.xack(self.stream_name, self.consumer_group, message_id)

    def cancellation_token(self, command: TaskCommand) -> RedisCancellationToken:
        return RedisCancellationToken(self.redis, command.tenant_id, command.execution_id)

    async def _ensure_group(self) -> None:
        if self._group_ready:
            return
        try:
            await self.redis.xgroup_create(
                self.stream_name,
                self.consumer_group,
                id="0-0",
                mkstream=True,
            )
        except Exception as error:
            if "BUSYGROUP" not in str(error):
                raise
        self._group_ready = True


_TERMINAL_STATUSES = frozenset(
    {
        ExecutionStatus.SUCCEEDED,
        ExecutionStatus.FAILED,
        ExecutionStatus.TIMED_OUT,
        ExecutionStatus.CANCELLED,
    }
)


def _task_not_found() -> TaskNotFound:
    return TaskNotFound(
        RuntimeContractError(
            code=ErrorCode.SESSION_CLOSED,
            message="async task is unavailable",
        )
    )


def _task_matches_principal(
    values: Mapping[str, str], principal: Principal
) -> bool:
    return all(
        values.get(field) == expected
        for field, expected in {
            "tenant_id": principal.tenant_id,
            "user_id": principal.user_id,
            "actor_id": principal.actor_id,
            "worker_id": principal.worker_id or "",
        }.items()
    )


def _terminal_cancel_response(
    execution_id: str, status: ExecutionStatus
) -> CancelTaskResponse:
    return CancelTaskResponse(
        execution_id=execution_id,
        status=status,
        cancellation_requested=False,
        already_terminal=True,
    )


def _error_from_fields(values: Mapping[str, str]) -> RuntimeContractError | None:
    code = values.get("error_code")
    message = values.get("error_message")
    if code is None or message is None:
        return None
    try:
        error_code = ErrorCode(code)
    except ValueError:
        error_code = ErrorCode.MODEL_ERROR
    return RuntimeContractError(code=error_code, message=message)


def _messages_from_autoclaim(value: object) -> list[tuple[str, dict[str, str]]]:
    if not isinstance(value, (tuple, list)):
        return []
    parts = cast(Sequence[object], value)
    if len(parts) < 2:
        return []
    return _coerce_messages(parts[1])


def _command_from_fields(fields: Mapping[str, str]) -> TaskCommand:
    payload: dict[str, object] = dict(fields)
    for name in ("execution_epoch", "retry_count", "max_retries"):
        payload[name] = int(fields[name])
    timeout = fields.get("timeout_seconds", "")
    if timeout == "":
        payload.pop("timeout_seconds", None)
    else:
        payload["timeout_seconds"] = float(timeout)
    return TaskCommand.model_validate(payload)


def _messages_from_read(value: object, stream_name: str) -> list[tuple[str, dict[str, str]]]:
    if not isinstance(value, (tuple, list)):
        return []
    for stream in cast(Sequence[object], value):
        if not isinstance(stream, (tuple, list)):
            continue
        parts = cast(Sequence[object], stream)
        if len(parts) < 2:
            continue
        if _as_text(parts[0]) != stream_name:
            continue
        return _coerce_messages(parts[1])
    return []


def _coerce_messages(value: object) -> list[tuple[str, dict[str, str]]]:
    if not isinstance(value, (tuple, list)):
        return []
    messages: list[tuple[str, dict[str, str]]] = []
    for message in cast(Sequence[object], value):
        if not isinstance(message, (tuple, list)):
            continue
        parts = cast(Sequence[object], message)
        if len(parts) < 2:
            continue
        message_id = _as_text(parts[0])
        raw_fields = parts[1]
        if not isinstance(raw_fields, Mapping):
            continue
        fields = cast(Mapping[object, object], raw_fields)
        messages.append(
            (
                message_id,
                {_as_text(key): _as_text(item) for key, item in fields.items()},
            )
        )
    return messages


def _as_text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _uuid_hex() -> str:
    from uuid import uuid4

    return uuid4().hex


__all__ = [
    "AsyncTaskCommand",
    "AsyncTaskStatus",
    "CancelTaskResponse",
    "ClaimedTask",
    "QueueError",
    "RedisTaskClient",
    "RedisTaskQueue",
    "TaskCommand",
    "TaskHandle",
    "TaskNotFound",
    "task_status_key",
]
