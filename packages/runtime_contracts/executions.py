from enum import StrEnum
from typing import Annotated

from pydantic import AwareDatetime, NonNegativeInt, StringConstraints

from packages.runtime_contracts.identity import CommandContract, FrozenContract, Identifier


class ExecutionMode(StrEnum):
    SYNC = "sync"
    STREAM = "stream"
    ASYNC = "async"


class ExecutionStatus(StrEnum):
    ACCEPTED = "accepted"
    LOADING_AGENT = "loading_agent"
    ACQUIRING_SESSION_LOCK = "acquiring_session_lock"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


class RuntimeExecution(FrozenContract):
    execution_id: Identifier
    session_id: Identifier
    tenant_id: Identifier
    user_id: Identifier
    actor_id: Identifier
    worker_id: Identifier | None
    trace_id: Identifier
    execution_epoch: NonNegativeInt
    mode: ExecutionMode
    status: ExecutionStatus
    created_at: AwareDatetime
    started_at: AwareDatetime | None
    completed_at: AwareDatetime | None


class CreateExecutionRequest(CommandContract):
    input: Annotated[str, StringConstraints(min_length=1, max_length=32_768)]
    mode: ExecutionMode = ExecutionMode.SYNC


__all__ = [
    "CreateExecutionRequest",
    "ExecutionMode",
    "ExecutionStatus",
    "RuntimeExecution",
]
