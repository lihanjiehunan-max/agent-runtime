from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from packages.runtime_contracts import ErrorCode
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.repositories import RuntimeRepositoryError
from packages.session_manager.checkpointer import (
    FencedCheckpointContext,
    FencedCheckpointWriter,
)


def _context(*, epoch: int = 4) -> FencedCheckpointContext:
    return FencedCheckpointContext(
        tenant_id="tenant-a",
        user_id="user-a",
        worker_id="worker-a",
        session_id="session-checkpoint",
        thread_id="session-checkpoint",
        execution_id="execution-checkpoint",
        execution_epoch=epoch,
        package_digest=f"sha256:{'a' * 64}",
    )


def _config(context: FencedCheckpointContext) -> dict[str, dict[str, str | int]]:
    return {
        "configurable": {
            "thread_id": context.thread_id,
            "checkpoint_ns": f"tenant:{context.tenant_id}",
            "runtime_tenant_id": context.tenant_id,
            "runtime_user_id": context.user_id,
            "runtime_worker_id": context.worker_id,
            "runtime_session_id": context.session_id,
            "runtime_execution_id": context.execution_id,
            "runtime_execution_epoch": context.execution_epoch,
            "runtime_package_digest": context.package_digest,
        }
    }


class _FailingCheckpointTransaction:
    def __init__(self, *, fail_during: str) -> None:
        self.fail_during = fail_during
        self.committed = False
        self.verified = False

    async def __aenter__(self) -> _FailingCheckpointTransaction:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_value, traceback
        if exc_type is None:
            self.committed = True

    async def verify(self, context: FencedCheckpointContext) -> None:
        del context
        if self.fail_during == "verify":
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.CHECKPOINT_RECOVERY_FAILED,
                    message="checkpoint fence verification failed",
                )
            )
        self.verified = True

    async def aput(
        self,
        config: dict[str, Any],
        checkpoint: dict[str, Any],
        metadata: dict[str, Any],
        new_versions: dict[str, Any],
        audit: object,
    ) -> dict[str, Any]:
        del checkpoint, metadata, new_versions, audit
        if self.fail_during == "aput":
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.CHECKPOINT_RECOVERY_FAILED,
                    message="checkpoint persistence failed before commit",
                )
            )
        self.committed = True
        return {"configurable": cast(dict[str, object], config["configurable"])}


@pytest.mark.parametrize("fail_during", ["verify", "aput"])
def test_checkpoint_failure_fails_closed_without_a_partial_commit(fail_during: str) -> None:
    async def scenario() -> None:
        context = _context()
        transaction = _FailingCheckpointTransaction(fail_during=fail_during)
        writer = FencedCheckpointWriter(lambda _context: transaction)

        with pytest.raises(RuntimeRepositoryError) as raised:
            await writer.aput(
                context=context,
                config=_config(context),
                checkpoint={"id": "checkpoint-failure", "channel_values": {"turns": [1]}},
                metadata={"source": "deterministic-release-gate"},
                new_versions={},
            )

        assert raised.value.error.code is ErrorCode.CHECKPOINT_RECOVERY_FAILED
        assert transaction.committed is False
        assert transaction.verified is (fail_during == "aput")

    asyncio.run(scenario())
