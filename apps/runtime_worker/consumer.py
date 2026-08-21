from __future__ import annotations

from typing import Protocol

from packages.execution_manager.cancellation import CancellationToken
from packages.execution_manager.queue import (
    QueueError,
    RedisTaskQueue,
    TaskCommand,
)
from packages.execution_manager.service import ExecutionResult
from packages.runtime_contracts import ErrorCode, ExecutionStatus, Principal
from packages.runtime_contracts import RuntimeError as RuntimeContractError


class AsyncExecutionManager(Protocol):
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
    ) -> ExecutionResult: ...


class TaskConsumerError(QueueError):
    pass


class TaskConsumer:
    """Claim and execute one Redis task with terminal-before-ack ordering."""

    def __init__(
        self,
        *,
        queue: RedisTaskQueue,
        execution_manager: AsyncExecutionManager,
        principal: Principal,
        consumer_name: str,
        claim_block_ms: int | None = None,
    ) -> None:
        if principal.worker_id is None:
            raise ValueError("task consumers require a verified worker identity")
        self._queue = queue
        self._execution_manager = execution_manager
        self._principal = principal
        self._consumer_name = consumer_name
        self._claim_block_ms = claim_block_ms

    async def consume_once(self) -> ExecutionResult | None:
        claimed = await self._queue.claim_once(
            self._consumer_name,
            block_ms=self._claim_block_ms,
        )
        if claimed is None:
            return None
        self._validate_command(claimed.command)
        status = await self._queue.get_task_status(
            claimed.command.execution_id,
            self._principal,
            session_id=claimed.command.session_id,
        )
        if status.status in {
            ExecutionStatus.SUCCEEDED,
            ExecutionStatus.FAILED,
            ExecutionStatus.TIMED_OUT,
            ExecutionStatus.CANCELLED,
        }:
            await self._queue.ack(claimed.message_id)
            return ExecutionResult(
                execution_id=claimed.command.execution_id,
                trace_id=claimed.command.trace_id,
                execution_epoch=status.execution_epoch,
                status=status.status,
                output=None,
                events=(),
                error=status.error,
            )
        await self._queue.mark_claimed(claimed.command)
        token = self._queue.cancellation_token(claimed.command)
        result = await self._execution_manager.execute_async(
            claimed.command.session_id,
            claimed.command.input,
            self._principal,
            execution_id=claimed.command.execution_id,
            trace_id=claimed.command.trace_id,
            execution_epoch=claimed.command.execution_epoch,
            cancellation_token=token,
            timeout_seconds=claimed.command.timeout_seconds,
        )
        # ExecutionManager returns only after EventRepository.append_terminal CAS
        # commits. The queue status update and ACK therefore happen afterward.
        await self._queue.mark_terminal(result, claimed.command)
        await self._queue.ack(claimed.message_id)
        return result

    async def run_once(self) -> ExecutionResult | None:
        return await self.consume_once()

    def _validate_command(self, command: TaskCommand) -> None:
        if (
            command.tenant_id != self._principal.tenant_id
            or command.user_id != self._principal.user_id
            or command.actor_id != self._principal.actor_id
        ):
            raise TaskConsumerError(
                RuntimeContractError(
                    code=ErrorCode.TENANT_IDENTITY_MISMATCH,
                    message="task command identity does not match the verified principal",
                )
            )
        if self._principal.worker_id != command.worker_id:
            raise TaskConsumerError(
                RuntimeContractError(
                    code=ErrorCode.TENANT_IDENTITY_MISMATCH,
                    message="task command worker identity does not match the consumer",
                )
            )


__all__ = ["AsyncExecutionManager", "TaskConsumer", "TaskConsumerError"]
