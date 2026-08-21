from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from packages.runtime_contracts import (
    AgentPackageRef,
    ErrorCode,
    Principal,
    RuntimeSession,
    RuntimeType,
    SessionStatus,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.repositories import RuntimeRepositoryError
from packages.session_manager.checkpointer import (
    FencedCheckpointContext,
    FencedCheckpointWriter,
    checkpoint_write_config,
)

PACKAGE = AgentPackageRef(
    tenant_id="tenant-a",
    agent_id="agent-metric-query",
    version="0.1.0",
    digest="sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04",
    runtime_type=RuntimeType.DEEPAGENTS,
    sdk_version="0.7.7",
)


def _principal(worker_id: str) -> Principal:
    return Principal(
        tenant_id="tenant-a",
        user_id="user-a",
        actor_id="actor-a",
        worker_id=worker_id,
    )


def _runtime_session(execution_id: str, execution_epoch: int) -> RuntimeSession:
    timestamp = datetime(2026, 8, 20, tzinfo=UTC)
    return RuntimeSession(
        session_id="session-restart",
        thread_id="session-restart",
        tenant_id="tenant-a",
        user_id="user-a",
        package=PACKAGE,
        status=SessionStatus.OPEN,
        revision=execution_epoch,
        execution_epoch=execution_epoch,
        active_execution_id=execution_id,
        last_checkpoint_id=None,
        last_event_sequence=0,
        created_at=timestamp,
        updated_at=timestamp,
    )


def _fenced_error(message: str) -> RuntimeRepositoryError:
    return RuntimeRepositoryError(
        RuntimeContractError(code=ErrorCode.EXECUTION_FENCED, message=message)
    )


@dataclass(frozen=True, slots=True)
class _StoredCheckpoint:
    execution_id: str
    execution_epoch: int
    worker_id: str
    thread_id: str
    package_digest: str
    values: Mapping[str, object]


class _CheckpointStore:
    """Small durable-state double with the same fence decision as PostgreSQL."""

    def __init__(self) -> None:
        self.epoch = 0
        self.active_execution_id: str | None = None
        self.worker_id: str | None = None
        self.latest: _StoredCheckpoint | None = None
        self.writes: list[_StoredCheckpoint] = []

    def begin(self, execution_id: str, worker_id: str) -> int:
        self.epoch += 1
        self.active_execution_id = execution_id
        self.worker_id = worker_id
        return self.epoch

    def complete(self, execution_id: str, execution_epoch: int) -> None:
        if (
            self.active_execution_id != execution_id
            or self.epoch != execution_epoch
        ):
            raise _fenced_error("completion belongs to a stale execution")
        self.active_execution_id = None

    def verify(self, context: FencedCheckpointContext) -> None:
        if (
            context.tenant_id != "tenant-a"
            or context.session_id != "session-restart"
            or context.thread_id != "session-restart"
            or context.execution_id != self.active_execution_id
            or context.execution_epoch != self.epoch
            or context.worker_id != self.worker_id
            or context.package_digest != PACKAGE.digest
        ):
            raise _fenced_error("checkpoint write was rejected by the epoch fence")

    def transaction(self, context: FencedCheckpointContext) -> _CheckpointTransaction:
        return _CheckpointTransaction(self, context)


class _CheckpointTransaction:
    def __init__(self, store: _CheckpointStore, context: FencedCheckpointContext) -> None:
        self._store = store
        self._context = context

    async def __aenter__(self) -> _CheckpointTransaction:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback

    async def verify(self, context: FencedCheckpointContext) -> None:
        self._store.verify(context)

    async def aput(
        self,
        config: dict[str, Any],
        checkpoint: dict[str, Any],
        metadata: dict[str, Any],
        new_versions: dict[str, Any],
        audit: object,
    ) -> dict[str, Any]:
        del new_versions
        self._store.verify(self._context)
        values = checkpoint.get("channel_values")
        if not isinstance(values, Mapping):
            raise AssertionError("checkpoint values must be a mapping")
        typed_values = cast(Mapping[str, object], values)
        audit_metadata = cast(Mapping[str, object], metadata["enterprise_audit"])
        if audit_metadata["package_digest"] != PACKAGE.digest:
            raise AssertionError("checkpoint audit lost the pinned package digest")
        stored = _StoredCheckpoint(
            execution_id=self._context.execution_id,
            execution_epoch=self._context.execution_epoch,
            worker_id=self._context.worker_id,
            thread_id=self._context.thread_id,
            package_digest=self._context.package_digest,
            values=dict(typed_values),
        )
        self._store.latest = stored
        self._store.writes.append(stored)
        configurable = cast(Mapping[str, object], config["configurable"])
        return {
            "configurable": {
                "thread_id": configurable["thread_id"],
                "checkpoint_ns": configurable["checkpoint_ns"],
                "checkpoint_id": checkpoint["id"],
            }
        }


class _Worker:
    def __init__(self, store: _CheckpointStore, worker_id: str) -> None:
        self._store = store
        self.worker_id = worker_id

    async def turn(
        self,
        turn_number: int,
        *,
        keep_active: bool = False,
    ) -> tuple[int, dict[str, dict[str, str | int]]]:
        execution_id = f"execution-{turn_number}"
        epoch = self._store.begin(execution_id, self.worker_id)
        runtime_session = _runtime_session(execution_id, epoch)
        principal = _principal(self.worker_id)
        config = checkpoint_write_config(
            runtime_session,
            principal,
            execution_id=execution_id,
            execution_epoch=epoch,
        )
        previous_turns: list[int] = []
        if self._store.latest is not None:
            previous = self._store.latest.values.get("turns", [])
            previous_turns = list(cast(list[int], previous))
        writer = FencedCheckpointWriter(self._store.transaction)
        await writer.aput(
            context=FencedCheckpointContext.from_config(config),
            config=config,
            checkpoint={
                "id": f"checkpoint-{turn_number}",
                "channel_values": {
                    "turns": [*previous_turns, turn_number],
                    "last_input": f"turn-{turn_number}",
                },
            },
            metadata={"source": "deterministic-release-gate"},
            new_versions={},
        )
        if not keep_active:
            self._store.complete(execution_id, epoch)
        return epoch, config


def test_worker_restart_resumes_checkpoint_and_fences_stale_worker() -> None:
    async def scenario() -> None:
        store = _CheckpointStore()
        first_worker = _Worker(store, "worker-a")
        last_epoch = 0
        stale_config: dict[str, dict[str, str | int]] | None = None
        for turn_number in range(1, 4):
            last_epoch, stale_config = await first_worker.turn(turn_number)

        assert store.latest is not None
        assert store.latest.values["turns"] == [1, 2, 3]
        assert store.latest.thread_id == "session-restart"
        assert store.latest.package_digest == PACKAGE.digest
        assert stale_config is not None

        restarted_worker = _Worker(store, "worker-b")
        restarted_epoch, restarted_config = await restarted_worker.turn(4)

        assert restarted_epoch > last_epoch
        assert restarted_epoch == 4
        assert restarted_config["configurable"]["thread_id"] == "session-restart"
        assert restarted_config["configurable"]["checkpoint_ns"] == "tenant:tenant-a"
        assert store.latest is not None
        assert store.latest.values["turns"] == [1, 2, 3, 4]
        assert store.latest.worker_id == "worker-b"
        assert store.latest.package_digest == PACKAGE.digest

        stale_context = FencedCheckpointContext.from_config(stale_config)
        stale_writer = FencedCheckpointWriter(store.transaction)
        with pytest.raises(RuntimeRepositoryError) as fenced:
            await stale_writer.aput(
                context=stale_context,
                config=stale_config,
                checkpoint={
                    "id": "checkpoint-stale",
                    "channel_values": {"turns": [1, 2, 3, 999]},
                },
                metadata={"source": "stale-worker"},
                new_versions={},
            )
        assert fenced.value.error.code is ErrorCode.EXECUTION_FENCED
        assert store.latest.values["turns"] == [1, 2, 3, 4]
        assert [item.execution_epoch for item in store.writes] == [1, 2, 3, 4]

    import asyncio

    asyncio.run(scenario())
