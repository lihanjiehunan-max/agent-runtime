from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from packages.runtime_contracts import ExecutionMode, Principal
from packages.session_manager.locks import (
    ExecutionFence,
    SessionExecutionCoordinator,
    SessionLockLease,
    SessionLockManager,
    SessionLockUnavailable,
)


@dataclass
class _SessionState:
    thread_id: str
    event_ids: list[str] = field(default_factory=list[str])
    messages: list[str] = field(default_factory=list[str])
    checkpoint_values: list[str] = field(default_factory=list[str])


class _AtomicRedis:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def set(
        self,
        name: str,
        value: str,
        *,
        nx: bool,
        px: int,
    ) -> bool:
        assert nx is True
        assert px > 0
        async with self._lock:
            if name in self._values:
                return False
            self._values[name] = value
            return True

    async def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str | int,
    ) -> int:
        assert script
        assert numkeys == 1
        key = str(keys_and_args[0])
        token = str(keys_and_args[1])
        async with self._lock:
            if self._values.get(key) != token:
                return 0
            if len(keys_and_args) > 2:
                return 1
            del self._values[key]
            return 1


class _EpochRepository:
    def __init__(self) -> None:
        self.epochs: dict[str, int] = {}
        self.active: set[str] = set()

    async def begin_execution(
        self,
        session_id: str,
        execution_id: str,
        principal: Principal,
        *,
        lease: SessionLockLease,
        mode: ExecutionMode = ExecutionMode.SYNC,
        trace_id: str | None = None,
        request_input: str | None = None,
    ) -> int:
        del execution_id, mode, trace_id, request_input
        assert principal.worker_id is not None
        assert lease.active is True
        if session_id in self.active:
            raise AssertionError("the session lock must serialize the CAS")
        self.active.add(session_id)
        self.epochs[session_id] = self.epochs.get(session_id, 0) + 1
        return self.epochs[session_id]

    def complete(self, session_id: str) -> None:
        self.active.remove(session_id)


def _principal(worker_id: str) -> Principal:
    return Principal(
        tenant_id="tenant-a",
        user_id="user-a",
        actor_id="actor-a",
        worker_id=worker_id,
    )


def test_same_session_turns_have_one_owner_and_second_is_busy() -> None:
    async def scenario() -> None:
        redis = _AtomicRedis()
        coordinator = SessionExecutionCoordinator(
            SessionLockManager(redis, ttl_seconds=5)
        )
        repository = _EpochRepository()
        owner_started = asyncio.Event()
        release_owner = asyncio.Event()
        log: list[str] = []

        async def owner() -> None:
            fence = await coordinator.begin_execution(
                repository,
                "session-shared",
                "execution-owner",
                _principal("worker-a"),
                mode=ExecutionMode.STREAM,
                trace_id="trace-owner",
                request_input="turn-owner",
            )
            log.append("owner.started")
            owner_started.set()
            await release_owner.wait()
            log.append("owner.finished")
            repository.complete("session-shared")
            await fence.lease.release()

        owner_task = asyncio.create_task(owner())
        await owner_started.wait()
        with pytest.raises(SessionLockUnavailable):
            await coordinator.begin_execution(
                repository,
                "session-shared",
                "execution-contender",
                _principal("worker-b"),
                mode=ExecutionMode.STREAM,
                trace_id="trace-contender",
                request_input="turn-contender",
            )
        assert log == ["owner.started"]
        assert repository.epochs["session-shared"] == 1
        release_owner.set()
        await owner_task
        assert log == ["owner.started", "owner.finished"]
        assert repository.active == set()

    asyncio.run(scenario())


def test_different_sessions_overlap_without_cross_session_interleaving() -> None:
    async def scenario() -> None:
        redis = _AtomicRedis()
        coordinator = SessionExecutionCoordinator(
            SessionLockManager(redis, ttl_seconds=5)
        )
        repository = _EpochRepository()
        session_states: dict[str, _SessionState] = {}
        for session_id in ("session-a", "session-b"):
            session_states[session_id] = _SessionState(thread_id=session_id)
        started = {"session-a": asyncio.Event(), "session-b": asyncio.Event()}
        both_started = asyncio.Event()
        log: list[tuple[str, str]] = []

        async def run(session_id: str, worker_id: str) -> ExecutionFence:
            fence = await coordinator.begin_execution(
                repository,
                session_id,
                f"execution-{session_id}",
                _principal(worker_id),
                mode=ExecutionMode.STREAM,
                trace_id=f"trace-{session_id}",
                request_input=f"turn-{session_id}",
            )
            log.append((session_id, "started"))
            state = session_states[session_id]
            state.event_ids.append(f"event-{session_id}")
            state.messages.append(f"message-{session_id}")
            state.checkpoint_values.append(f"checkpoint-{session_id}")
            started[session_id].set()
            if all(event.is_set() for event in started.values()):
                both_started.set()
            await both_started.wait()
            log.append((session_id, "finished"))
            repository.complete(session_id)
            return fence

        first, second = await asyncio.gather(
            run("session-a", "worker-a"),
            run("session-b", "worker-b"),
        )
        await first.lease.release()
        await second.lease.release()

        assert repository.epochs == {"session-a": 1, "session-b": 1}
        assert repository.active == set()
        assert len(log) == 4
        for session_id in ("session-a", "session-b"):
            session_events = [event for event in log if event[0] == session_id]
            assert session_events == [(session_id, "started"), (session_id, "finished")]
            state = session_states[session_id]
            assert state.thread_id == session_id
            assert state.event_ids == [f"event-{session_id}"]
            assert state.messages == [f"message-{session_id}"]
            assert state.checkpoint_values == [f"checkpoint-{session_id}"]

        assert {
            item
            for state in session_states.values()
            for item in state.event_ids
        } == {"event-session-a", "event-session-b"}
        assert {
            item
            for state in session_states.values()
            for item in state.messages
        } == {"message-session-a", "message-session-b"}
        assert log.index(("session-a", "started")) < log.index(("session-a", "finished"))
        assert log.index(("session-b", "started")) < log.index(("session-b", "finished"))
        first_finished = min(
            log.index(("session-a", "finished")),
            log.index(("session-b", "finished")),
        )
        assert log.index(("session-a", "started")) < first_finished
        assert log.index(("session-b", "started")) < first_finished

    asyncio.run(scenario())
