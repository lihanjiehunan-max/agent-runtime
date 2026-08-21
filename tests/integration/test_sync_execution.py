from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from packages.execution_manager.service import (
    TERMINAL_EVENT_TYPES,
    ExecutionManager,
    ExecutionManagerError,
    ExecutionResult,
    RepositoryEventSink,
)
from packages.execution_manager.state import (
    ExecutionState,
    ExecutionStateMachine,
    InvalidExecutionTransition,
    TerminalEventAlreadyEmitted,
)
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


def _session() -> RuntimeSession:
    timestamp = datetime(2026, 8, 20, tzinfo=UTC)
    return RuntimeSession(
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


def _raw(method: str, data: object) -> dict[str, object]:
    return {"method": method, "params": {"namespace": [], "data": data}}

EMPTY_METADATA: dict[str, object] = {}


class _Lease:
    tenant_id = "tenant-a"
    session_id = "session-a"
    owner_id = "worker-a"
    active = True

    def __init__(
        self,
        *,
        renew_exception_at: int | None = None,
        release_error: bool = False,
    ) -> None:
        self.renewals = 0
        self.releases = 0
        self.renew_exception_at = renew_exception_at
        self.release_error = release_error

    async def renew(self) -> bool:
        self.renewals += 1
        if self.renewals == self.renew_exception_at:
            raise RuntimeError("redis lease renewal failed")
        return True

    async def release(self) -> bool:
        self.releases += 1
        self.active = False
        if self.release_error:
            raise RuntimeError("redis lease release failed")
        return True


class _CancelledReleaseLease(_Lease):
    async def release(self) -> bool:
        self.releases += 1
        self.active = False
        raise asyncio.CancelledError


class _Coordinator:
    def __init__(self, session_manager: _SessionManager, lease: _Lease | None = None) -> None:
        self.session_manager = session_manager
        self.lease = lease or _Lease()
        self.calls: list[tuple[str, str, int]] = []

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
        assert principal == PRINCIPAL
        self.calls.append((session_id, execution_id, 7))
        self.session_manager.activate(execution_id, 7)
        return ExecutionFence(
            tenant_id=principal.tenant_id,
            session_id=session_id,
            execution_id=execution_id,
            execution_epoch=7,
            worker_id=cast(str, principal.worker_id),
            lease=cast(Any, self.lease),
        )


class _SessionManager:
    def __init__(self) -> None:
        self.current = _session()

    def activate(self, execution_id: str, epoch: int) -> None:
        self.current = self.current.model_copy(
            update={"active_execution_id": execution_id, "execution_epoch": epoch}
        )

    async def get_session(
        self,
        session_id: str,
        principal: Principal,
    ) -> RuntimeSession | None:
        if session_id != self.current.session_id or principal.tenant_id != "tenant-a":
            return None
        return self.current


class _EventSink:
    def __init__(self, *, fence_error_after: int | None = None) -> None:
        self.events: list[RuntimeEvent] = []
        self.fence_error_after = fence_error_after
        self.terminal_calls: list[
            tuple[ExecutionStatus, RuntimeContractError | None]
        ] = []

    async def append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        if self.fence_error_after is not None and len(self.events) >= self.fence_error_after:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="stale worker epoch",
                )
            )
        event = event_factory(len(self.events) + 1)
        assert event.tenant_id == principal.tenant_id
        assert event.worker_id == principal.worker_id
        assert execution_epoch == 7
        assert event.sequence == len(self.events) + 1
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
        event = await self.append(event_factory, principal, execution_epoch)
        self.terminal_calls.append((status, error))
        return event


class _TerminalAppendFailsSink(_EventSink):
    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        del event_factory, principal, execution_epoch, status, error
        raise RuntimeRepositoryError(
            RuntimeContractError(
                code=ErrorCode.EXECUTION_FENCED,
                message="durable terminal append failed",
            )
        )


class _ExecutionRepository:
    def __init__(self) -> None:
        self.completions: list[tuple[str, ExecutionStatus, int]] = []

    async def complete_execution(
        self,
        session_id: str,
        execution_id: str,
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        **_kwargs: object,
    ) -> None:
        assert session_id == "session-a"
        assert principal == PRINCIPAL
        self.completions.append((execution_id, status, execution_epoch))


CoordinatorDouble = _Coordinator
EventSinkDouble = _EventSink
ExecutionRepositoryDouble = _ExecutionRepository
SessionManagerDouble = _SessionManager


class _Run:
    def __init__(self, events: list[dict[str, object]]) -> None:
        self.events = events
        self.config: dict[str, dict[str, str | int]] | None = None

    def __aiter__(self) -> AsyncIterator[dict[str, object]]:
        async def iterator() -> AsyncIterator[dict[str, object]]:
            for event in self.events:
                yield event

        return iterator()

    async def output(self) -> dict[str, object]:
        return {"messages": [{"type": "ai", "content": "Revenue is 42."}]}


class _Graph:
    def __init__(
        self,
        events: list[dict[str, object]],
        *,
        awaitable: bool = False,
    ) -> None:
        self.events = events
        self.run: _Run | None = None
        self.awaitable = awaitable

    def astream_events(
        self,
        input: object,
        *,
        config: dict[str, dict[str, str | int]],
        version: str,
    ) -> _Run | Awaitable[_Run]:
        del input
        assert version == "v3"
        configurable = cast(dict[str, object], config["configurable"])
        assert configurable["thread_id"] == "session-a"
        assert configurable["runtime_execution_epoch"] == 7
        assert configurable["runtime_worker_id"] == "worker-a"
        self.run = _Run(self.events)
        self.run.config = config
        if self.awaitable:
            async def resolve() -> _Run:
                assert self.run is not None
                return self.run

            return resolve()
        return self.run


class _TerminalRaceLease(_Lease):
    def __init__(self) -> None:
        super().__init__()
        self.background_renewal_started = asyncio.Event()
        self.allow_background_failure = asyncio.Event()
        self.failure_recorded = asyncio.Event()

    async def renew(self) -> bool:
        self.renewals += 1
        if self.renewals == 4:
            self.background_renewal_started.set()
            await self.allow_background_failure.wait()
            self.failure_recorded.set()
            raise RuntimeError("redis lease renewal failed during terminal preparation")
        if self.renewals == 5:
            await self.failure_recorded.wait()
        return True


class _TerminalRaceRun:
    def __init__(self, lease: _TerminalRaceLease) -> None:
        self.lease = lease

    def __aiter__(self) -> AsyncIterator[dict[str, object]]:
        async def iterator() -> AsyncIterator[dict[str, object]]:
            if False:
                yield {}

        return iterator()

    async def output(self) -> dict[str, object]:
        await self.lease.background_renewal_started.wait()

        async def release_background_renewal() -> None:
            await asyncio.sleep(0)
            self.lease.allow_background_failure.set()

        asyncio.create_task(release_background_renewal())
        return {}


class _TerminalRaceGraph:
    def __init__(self, lease: _TerminalRaceLease) -> None:
        self.run = _TerminalRaceRun(lease)

    def astream_events(
        self,
        input: object,
        *,
        config: dict[str, dict[str, str | int]],
        version: str,
    ) -> _TerminalRaceRun:
        del input, config
        assert version == "v3"
        return self.run


class _Factory:
    def __init__(self, graph: _Graph) -> None:
        self.graph = graph
        self.requested: list[str] = []

    def get(self, package_digest: str) -> _Graph:
        self.requested.append(package_digest)
        return self.graph


def _manager(
    graph: _Graph,
    *,
    sink: _EventSink | None = None,
    lease: _Lease | None = None,
    lease_renew_interval_seconds: float = 0,
) -> tuple[ExecutionManager, _SessionManager, _Coordinator, _EventSink, _ExecutionRepository]:
    session_manager = _SessionManager()
    coordinator = _Coordinator(session_manager, lease)
    event_sink = sink or _EventSink()
    execution_repository = _ExecutionRepository()
    manager = ExecutionManager(
        agent_factory=_Factory(graph),
        session_manager=session_manager,
        execution_repository=execution_repository,
        execution_coordinator=coordinator,
        event_sink=event_sink,
        lease_renew_interval_seconds=lease_renew_interval_seconds,
    )
    return manager, session_manager, coordinator, event_sink, execution_repository


GraphDouble = _Graph
manager_for_test = _manager


def test_sync_turn_uses_fence_checkpoint_config_and_emits_one_ordered_terminal_event() -> None:
    graph = _Graph(
        [
            _raw(
                "messages",
                (
                    {"event": "message-start", "role": "ai", "id": "model-1"},
                    EMPTY_METADATA,
                ),
            ),
            _raw(
                "messages",
                (
                    {
                        "event": "content-block-delta",
                        "index": 0,
                        "delta": {"type": "text-delta", "text": "Revenue is 42."},
                    },
                    EMPTY_METADATA,
                ),
            ),
            _raw(
                "messages",
                (
                    {"event": "message-finish", "metadata": {"finish_reason": "stop"}},
                    EMPTY_METADATA,
                ),
            ),
            _raw("output", {"messages": [{"type": "ai", "content": "Revenue is 42."}]}),
        ]
    )
    manager, _sessions, coordinator, sink, executions = _manager(graph)

    result = asyncio.run(manager.execute_turn("session-a", "What is revenue?", PRINCIPAL))

    assert isinstance(result, ExecutionResult)
    assert result.status is ExecutionStatus.SUCCEEDED
    assert result.output == {"messages": [{"type": "ai", "content": "Revenue is 42."}]}
    assert [event.sequence for event in sink.events] == list(range(1, len(sink.events) + 1))
    assert [event.event_type for event in sink.events[:2]] == [
        "execution.accepted",
        "execution.started",
    ]
    assert sink.events[-1].event_type == "execution.succeeded"
    assert sum(event.event_type in TERMINAL_EVENT_TYPES for event in sink.events) == 1
    assert executions.completions == []
    assert coordinator.lease.renewals >= 1
    assert coordinator.lease.releases == 1


def test_stale_epoch_is_propagated_and_never_reported_as_success() -> None:
    graph = _Graph([])
    sink = _EventSink(fence_error_after=1)
    manager, _sessions, coordinator, _sink, executions = _manager(graph, sink=sink)

    try:
        asyncio.run(manager.execute_turn("session-a", "What is revenue?", PRINCIPAL))
    except RuntimeRepositoryError as error:
        assert error.error.code is ErrorCode.EXECUTION_FENCED
    else:
        raise AssertionError("stale epoch must be rejected")

    assert all(event.event_type != "execution.succeeded" for event in sink.events)
    assert executions.completions == []
    assert coordinator.lease.releases == 1


def test_execution_state_machine_rejects_illegal_transition_and_duplicate_terminal() -> None:
    machine = ExecutionStateMachine()
    machine.transition(ExecutionState.LOADING_AGENT)

    with pytest.raises(InvalidExecutionTransition):
        machine.transition(ExecutionState.SUCCEEDED)

    machine.transition(ExecutionState.ACQUIRING_SESSION_LOCK)
    machine.transition(ExecutionState.RUNNING)
    machine.transition(ExecutionState.FAILED)
    machine.mark_terminal_event()

    with pytest.raises(TerminalEventAlreadyEmitted):
        machine.mark_terminal_event()


def test_execution_events_use_result_trace_identity() -> None:
    graph = _Graph([])
    manager, _sessions, _coordinator, sink, _executions = _manager(graph)

    result = asyncio.run(manager.execute_turn("session-a", "trace this", PRINCIPAL))

    assert result.trace_id
    assert {event.trace_id for event in sink.events} == {result.trace_id}


def test_terminal_event_and_execution_status_use_one_durable_sink_operation() -> None:
    manager, _sessions, _coordinator, sink, executions = _manager(_Graph([]))

    result = asyncio.run(manager.execute_turn("session-a", "finish this", PRINCIPAL))

    assert result.status is ExecutionStatus.SUCCEEDED
    assert sink.terminal_calls == [(ExecutionStatus.SUCCEEDED, None)]
    assert executions.completions == []


def test_renew_loop_converts_lease_exceptions_to_execution_fenced() -> None:
    lease = _Lease(renew_exception_at=1)
    manager, _sessions, coordinator, _sink, _executions = _manager(
        _Graph([]), lease=lease
    )
    fence = ExecutionFence(
        tenant_id=PRINCIPAL.tenant_id,
        session_id="session-a",
        execution_id="execution-a",
        execution_epoch=7,
        worker_id="worker-a",
        lease=cast(Any, coordinator.lease),
    )
    failures: list[RuntimeRepositoryError] = []

    asyncio.run(manager._renew_loop(fence, failures))  # pyright: ignore[reportPrivateUsage]

    assert len(failures) == 1
    assert failures[0].error.code is ErrorCode.EXECUTION_FENCED


def test_lease_renew_exception_never_returns_success() -> None:
    lease = _Lease(renew_exception_at=3)
    manager, _sessions, _coordinator, sink, executions = _manager(
        _Graph([]), lease=lease
    )

    with pytest.raises(RuntimeRepositoryError) as raised:
        asyncio.run(manager.execute_turn("session-a", "renew this", PRINCIPAL))

    assert raised.value.error.code is ErrorCode.EXECUTION_FENCED
    assert all(event.event_type != "execution.succeeded" for event in sink.events)
    assert executions.completions == []


def test_lease_failure_during_terminal_preparation_never_returns_success() -> None:
    lease = _TerminalRaceLease()
    manager, _sessions, coordinator, sink, _executions = _manager(
        cast(_Graph, _TerminalRaceGraph(lease)),
        lease=lease,
        lease_renew_interval_seconds=0.001,
    )

    with pytest.raises(RuntimeRepositoryError) as raised:
        asyncio.run(manager.execute_turn("session-a", "terminal race", PRINCIPAL))

    assert raised.value.error.code is ErrorCode.EXECUTION_FENCED
    assert all(event.event_type != "execution.succeeded" for event in sink.events)
    assert coordinator.lease.releases == 1


def test_release_exception_does_not_mask_completed_result() -> None:
    lease = _Lease(release_error=True)
    manager, _sessions, _coordinator, _sink, _executions = _manager(
        _Graph([]), lease=lease
    )

    result = asyncio.run(manager.execute_turn("session-a", "release this", PRINCIPAL))

    assert result.status is ExecutionStatus.SUCCEEDED
    assert lease.releases == 1


def test_cancelled_release_does_not_mask_completed_result() -> None:
    lease = _CancelledReleaseLease()
    manager, _sessions, _coordinator, _sink, _executions = _manager(
        _Graph([]), lease=lease
    )

    result = asyncio.run(manager.execute_turn("session-a", "release this", PRINCIPAL))

    assert result.status is ExecutionStatus.SUCCEEDED
    assert lease.releases == 1


def test_durable_terminal_failure_never_returns_success() -> None:
    sink = _TerminalAppendFailsSink()
    manager, _sessions, _coordinator, _sink, executions = _manager(
        _Graph([]), sink=sink
    )

    with pytest.raises(RuntimeRepositoryError) as raised:
        asyncio.run(manager.execute_turn("session-a", "durable failure", PRINCIPAL))

    assert raised.value.error.code is ErrorCode.EXECUTION_FENCED
    assert all(event.event_type != "execution.succeeded" for event in sink.events)
    assert executions.completions == []


def test_async_execution_mode_is_rejected_until_task_10() -> None:
    manager, _sessions, _coordinator, _sink, _executions = _manager(_Graph([]))

    with pytest.raises(ExecutionManagerError) as raised:
        asyncio.run(
            manager.execute_turn(
                "session-a",
                "async this",
                PRINCIPAL,
                mode=ExecutionMode.ASYNC,
            )
        )

    assert raised.value.error.code is ErrorCode.RUNTIME_INCOMPATIBLE


class _AppendNextRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[Principal, int]] = []

    async def append_next(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        self.calls.append((principal, execution_epoch))
        return event_factory(11)

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        del status, error
        return await self.append_next(event_factory, principal, execution_epoch)


def test_repository_event_sink_delegates_sequence_allocation_to_durable_repository() -> None:
    repository = _AppendNextRepository()
    sink = RepositoryEventSink(repository)

    event = asyncio.run(
        sink.append(
            lambda sequence: RuntimeEvent(
                schema_version="runtime.event.v1",
                event_id="event-durable",
                sequence=sequence,
                occurred_at=datetime(2026, 8, 20, tzinfo=UTC),
                tenant_id=PRINCIPAL.tenant_id,
                trace_id="trace-durable",
                span_id="span-durable",
                parent_span_id=None,
                session_id="session-a",
                execution_id="execution-a",
                package=PACKAGE,
                worker_id="worker-a",
                sdk_version=PACKAGE.sdk_version,
                event_type="execution.accepted",
                phase="accepted",
                duration_ms=None,
                payload={},
                payload_ref=None,
            ),
            PRINCIPAL,
            7,
        )
    )

    assert event.sequence == 11
    assert repository.calls == [(PRINCIPAL, 7)]
