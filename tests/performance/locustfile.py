from __future__ import annotations

import asyncio
import json
import math
import shutil
import tempfile
import time
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from packages.event_model.emitter import RuntimeEventEmitter
from packages.event_model.payloads import PayloadOffloader
from packages.event_model.projection import TraceProjection
from packages.execution_manager.cancellation import RedisCancellationToken
from packages.execution_manager.service import ExecutionManager
from packages.package_loader.cache import PackageCache, SingleflightLoader
from packages.package_loader.schema import AgentDefinition, LoadedPackage
from packages.runtime_contracts import (
    AgentPackageRef,
    ExecutionMode,
    ExecutionStatus,
    Principal,
    RuntimeEvent,
    RuntimeSession,
    SessionStatus,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.repositories import RuntimeRepositoryError
from packages.session_manager.locks import ExecutionFence
from packages.tool_gateway.contracts import MetricQuery, MetricResult, ToolEventContext
from packages.tool_gateway.service import LocalSequencedToolEventSink, ToolGateway
from tests.unit.package_loader._package_builder import build_package, package_ref

MIN_SESSIONS = 30
MAX_SESSIONS = 50
DEFAULT_SESSIONS = 40
_RAW_CREDENTIAL_MARKER = "api_key=deterministic-test-secret"


class _DeterministicClock:
    def __init__(self, step_seconds: float = 0.001) -> None:
        self._step_seconds = step_seconds
        self._current = 0.0

    def __call__(self) -> float:
        current = self._current
        self._current += self._step_seconds
        return current


@dataclass(frozen=True, slots=True)
class PerformanceReport:
    session_count: int
    cache_hit_rate: float
    cached_definition_p95_ms: float
    platform_overhead_p95_ms: float
    cooperative_cancel_seconds: float
    trace_coverage: float
    short_task_success_rate: float
    raw_payloads_recorded: int
    credentials_recorded: int
    runtime_events_recorded: int
    trace_projections_recorded: int
    tool_calls_recorded: int
    tool_events_recorded: int
    checkpoints_recorded: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class _DirectoryPackageSource:
    def __init__(self, roots: dict[str, Path]) -> None:
        self._roots = roots
        self.downloads = 0

    def download(self, package: AgentPackageRef, destination: Path) -> None:
        self.downloads += 1
        shutil.copytree(self._roots[package.digest], destination, dirs_exist_ok=True)


class _RuntimeLease:
    def __init__(self) -> None:
        self.active = True

    async def renew(self) -> bool:
        return self.active

    async def release(self) -> bool:
        was_active = self.active
        self.active = False
        return was_active


class _RuntimeSessionStore:
    def __init__(self, package: AgentPackageRef, session_ids: tuple[str, ...]) -> None:
        timestamp = datetime(2026, 8, 20, tzinfo=UTC)
        self.sessions: dict[str, RuntimeSession] = {}
        self.principals: dict[str, Principal] = {}
        for index, session_id in enumerate(session_ids):
            principal = Principal(
                tenant_id=package.tenant_id,
                user_id=f"user-{index}",
                actor_id=f"actor-{index}",
                worker_id=f"worker-{index}",
                permissions=("tool:query_metric",),
            )
            self.principals[session_id] = principal
            self.sessions[session_id] = RuntimeSession(
                session_id=session_id,
                thread_id=session_id,
                tenant_id=package.tenant_id,
                user_id=principal.user_id,
                package=package,
                status=SessionStatus.OPEN,
                revision=0,
                execution_epoch=0,
                active_execution_id=None,
                last_checkpoint_id=None,
                last_event_sequence=0,
                created_at=timestamp,
                updated_at=timestamp,
            )

    async def get_session(
        self,
        session_id: str,
        principal: Principal,
    ) -> RuntimeSession | None:
        session = self.sessions.get(session_id)
        if session is None:
            return None
        if (
            session.tenant_id != principal.tenant_id
            or session.user_id != principal.user_id
            or session.status is not SessionStatus.OPEN
        ):
            return None
        return session

    def activate(self, session_id: str, execution_id: str, epoch: int) -> None:
        current = self.sessions[session_id]
        self.sessions[session_id] = current.model_copy(
            update={
                "active_execution_id": execution_id,
                "execution_epoch": epoch,
                "revision": current.revision + 1,
            }
        )


class _RuntimeEventStore:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []
        self._next_sequence: dict[str, int] = {}
        self._fences: dict[str, tuple[str, str, str, int, str]] = {}
        self.terminals: set[str] = set()

    def activate(
        self,
        *,
        session_id: str,
        tenant_id: str,
        execution_id: str,
        worker_id: str,
        epoch: int,
    ) -> None:
        self._fences[session_id] = (
            tenant_id,
            execution_id,
            worker_id,
            epoch,
            session_id,
        )

    async def append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        return self._append(event_factory, principal, execution_epoch)

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        del status, error
        probe = event_factory(1)
        if probe.execution_id in self.terminals:
            raise self._fenced("terminal state already committed")
        event = self._append(event_factory, principal, execution_epoch)
        self.terminals.add(event.execution_id)
        return event

    def _append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        probe = event_factory(1)
        expected = self._fences.get(probe.session_id)
        if expected is None:
            raise self._fenced("event session has no active execution fence")
        tenant_id, execution_id, worker_id, epoch, session_id = expected
        next_sequence = self._next_sequence.get(probe.execution_id, 0) + 1
        event = event_factory(next_sequence)
        if (
            event.tenant_id != tenant_id
            or event.tenant_id != principal.tenant_id
            or event.session_id != session_id
            or event.execution_id != execution_id
            or event.worker_id != worker_id
            or principal.worker_id != worker_id
            or execution_epoch != epoch
        ):
            raise self._fenced("event append was rejected by the execution fence")
        self._next_sequence[event.execution_id] = next_sequence
        self.events.append(event)
        return event

    @staticmethod
    def _fenced(message: str) -> RuntimeRepositoryError:
        from packages.runtime_contracts import ErrorCode

        return RuntimeRepositoryError(
            RuntimeContractError(code=ErrorCode.EXECUTION_FENCED, message=message)
        )


class _RuntimeTraceStore:
    def __init__(self) -> None:
        self.projections: dict[str, TraceProjection] = {}

    async def save(self, projection: TraceProjection) -> None:
        self.projections[projection.execution_id] = projection


class _MetricClient:
    def __init__(self) -> None:
        self.calls: list[tuple[MetricQuery, Principal, str]] = []

    def query(
        self,
        query: MetricQuery,
        *,
        principal: Principal,
        request_id: str,
    ) -> MetricResult:
        self.calls.append((query, principal, request_id))
        return MetricResult(
            metric=query.metric,
            period=query.period,
            org=query.org,
            value=Decimal("1280000.00"),
            unit="CNY",
        )


class _CheckpointStore:
    def __init__(self) -> None:
        self.records: list[tuple[str, str, str, int]] = []

    def record(
        self,
        config: dict[str, dict[str, str | int]],
        prompt: str,
    ) -> str:
        configurable = config["configurable"]
        checkpoint_id = f"checkpoint-{configurable['runtime_session_id']}"
        self.records.append(
            (
                str(configurable["thread_id"]),
                prompt,
                checkpoint_id,
                int(configurable["runtime_execution_epoch"]),
            )
        )
        return checkpoint_id


class _RuntimeRun:
    def __init__(
        self,
        events: list[Mapping[str, object]],
        answer: str,
        *,
        started: asyncio.Event | None = None,
        stall: bool = False,
    ) -> None:
        self._events = events
        self._answer = answer
        self._started = started
        self._stall = stall

    def __aiter__(self) -> AsyncIterator[Mapping[str, object]]:
        async def iterator() -> AsyncIterator[Mapping[str, object]]:
            if self._started is not None:
                self._started.set()
            for event in self._events:
                yield event
            if self._stall:
                await asyncio.Event().wait()

        return iterator()

    async def output(self) -> dict[str, object]:
        if self._stall:
            await asyncio.Event().wait()
        return {
            "messages": [{"type": "ai", "content": self._answer}],
            "last_message_text": self._answer,
        }


class _RuntimeGraph:
    def __init__(
        self,
        package: AgentPackageRef,
        metric_client: _MetricClient,
        tool_events: list[RuntimeEvent],
        checkpoints: _CheckpointStore,
        *,
        started: asyncio.Event | None = None,
        stall: bool = False,
    ) -> None:
        self.package = package
        self.metric_client = metric_client
        self.tool_events = tool_events
        self.checkpoints = checkpoints
        self.started = started
        self.stall = stall

    def astream_events(
        self,
        input: object,
        *,
        config: dict[str, dict[str, str | int]],
        version: str,
    ) -> _RuntimeRun:
        assert version == "v3"
        configurable = config["configurable"]
        session_id = str(configurable["runtime_session_id"])
        execution_id = str(configurable["runtime_execution_id"])
        worker_id = str(configurable["runtime_worker_id"])
        thread_id = str(configurable["thread_id"])
        assert thread_id == session_id
        assert str(configurable["runtime_package_digest"]) == self.package.digest
        input_mapping = cast(Mapping[str, object], input)
        messages = cast(list[object], input_mapping["messages"])
        prompt = str(cast(Mapping[str, object], messages[-1])["content"])
        principal = Principal(
            tenant_id=self.package.tenant_id,
            user_id=str(configurable["runtime_user_id"]),
            actor_id=str(configurable["runtime_user_id"]),
            worker_id=worker_id,
            permissions=("tool:query_metric",),
        )
        event_context = ToolEventContext(
            trace_id=f"trace-{execution_id}",
            parent_span_id=None,
            session_id=session_id,
            execution_id=execution_id,
            worker_id=worker_id,
            package=self.package,
        )
        gateway = ToolGateway(
            client=self.metric_client,
            principal=principal,
            package_allowlist=("query_metric",),
            event_context=event_context,
            event_sink=LocalSequencedToolEventSink(self.tool_events.append),
            request_id_factory=lambda: f"tool-request-{len(self.metric_client.calls) + 1}",
        )
        result = gateway.query_metric(
            metric="营业收入",
            period="本月",
            org="performance-harness",
        )
        checkpoint_id = self.checkpoints.record(config, prompt)
        result_json = cast(dict[str, object], result.model_dump(mode="json"))
        answer = f"本月营业收入为 {result.value} CNY（{_RAW_CREDENTIAL_MARKER}）"
        call_id = f"call-{execution_id}"
        events: list[Mapping[str, object]] = [
            _raw_messages(
                {"event": "message-start", "role": "ai", "id": f"model-{execution_id}"}
            ),
            _raw_messages(
                {
                    "event": "content-block-delta",
                    "index": 0,
                    "delta": {"type": "text-delta", "text": answer},
                }
            ),
            _raw_tools(
                {
                    "event": "tool-started",
                    "tool_call_id": call_id,
                    "tool_name": "query_metric",
                    "input": {"metric": "营业收入", "period": "本月"},
                }
            ),
            _raw_tools(
                {
                    "event": "tool-output",
                    "tool_call_id": call_id,
                    "tool_name": "query_metric",
                    "output": result_json,
                }
            ),
            _raw_tools(
                {
                    "event": "tool-completed",
                    "tool_call_id": call_id,
                    "tool_name": "query_metric",
                    "output": result_json,
                }
            ),
            _raw_messages(
                {
                    "event": "message-finish",
                    "metadata": {
                        "finish_reason": "stop",
                        "usage": {"input_tokens": 20, "output_tokens": 16},
                    },
                }
            ),
            {
                "method": "values",
                "params": {
                    "data": {
                        "messages": [{"type": "ai", "content": answer}],
                        "thread_id": thread_id,
                        "checkpoint_id": checkpoint_id,
                    }
                },
            },
            {
                "method": "output",
                "params": {
                    "data": {
                        "messages": [{"type": "ai", "content": answer}],
                        "last_message_text": answer,
                        "checkpoint_id": checkpoint_id,
                    }
                },
            },
        ]
        return _RuntimeRun(events, answer, started=self.started, stall=self.stall)


class _RuntimeFactory:
    def __init__(self, graph: _RuntimeGraph) -> None:
        self.graph = graph

    def get(self, package_digest: str) -> _RuntimeGraph:
        assert package_digest == self.graph.package.digest
        return self.graph

    def get_limits(self, package_digest: str) -> object:
        assert package_digest == self.graph.package.digest
        return type("Limits", (), {"execution_timeout_seconds": 60.0})()


class _RuntimeExecutionRepository:
    async def complete_execution(self, *args: object, **kwargs: object) -> None:
        del args, kwargs


class _RuntimeCoordinator:
    def __init__(self, sessions: _RuntimeSessionStore, events: _RuntimeEventStore) -> None:
        self.sessions = sessions
        self.events = events

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
        current = await self.sessions.get_session(session_id, principal)
        if current is None or principal.worker_id is None:
            raise RuntimeError("performance session is unavailable")
        epoch = current.execution_epoch + 1
        self.sessions.activate(session_id, execution_id, epoch)
        self.events.activate(
            session_id=session_id,
            tenant_id=principal.tenant_id,
            execution_id=execution_id,
            worker_id=principal.worker_id,
            epoch=epoch,
        )
        return ExecutionFence(
            tenant_id=principal.tenant_id,
            session_id=session_id,
            execution_id=execution_id,
            execution_epoch=epoch,
            worker_id=principal.worker_id,
            lease=cast(Any, _RuntimeLease()),
        )


class _CancellationRedis:
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
        del ex, px
        if nx and name in self.values:
            return False
        self.values[name] = value
        return True

    async def get(self, name: str) -> str | None:
        return self.values.get(name)


@dataclass(frozen=True, slots=True)
class _RuntimeLoadMeasurements:
    definition_latencies: list[float]
    platform_latencies: list[float]
    successful: int
    covered: int
    runtime_events_recorded: int
    trace_projections_recorded: int
    tool_calls_recorded: int
    tool_events_recorded: int
    checkpoints_recorded: int
    raw_payloads_recorded: int
    credentials_recorded: int


def _p95(values: list[float]) -> float:
    if not values:
        raise ValueError("cannot calculate a percentile without measurements")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * 0.95) - 1))
    return ordered[index]


def _build_definition(package: LoadedPackage) -> AgentDefinition:
    return package.agent


def _raw_messages(event: Mapping[str, object]) -> Mapping[str, object]:
    return cast(
        Mapping[str, object],
        {"method": "messages", "params": {"data": (dict(event), {})}},
    )


def _raw_tools(data: Mapping[str, object]) -> Mapping[str, object]:
    return {"method": "tools", "params": {"data": dict(data)}}


def _build_runtime_path(
    package: AgentPackageRef,
    session_ids: tuple[str, ...],
    *,
    stall: bool = False,
) -> tuple[
    ExecutionManager,
    _RuntimeSessionStore,
    _RuntimeEventStore,
    _RuntimeTraceStore,
    _MetricClient,
    _CheckpointStore,
    list[RuntimeEvent],
    asyncio.Event | None,
]:
    sessions = _RuntimeSessionStore(package, session_ids)
    event_store = _RuntimeEventStore()
    trace_store = _RuntimeTraceStore()
    metric_client = _MetricClient()
    checkpoints = _CheckpointStore()
    tool_events: list[RuntimeEvent] = []
    started = asyncio.Event() if stall else None
    graph = _RuntimeGraph(
        package,
        metric_client,
        tool_events,
        checkpoints,
        started=started,
        stall=stall,
    )
    factory = _RuntimeFactory(graph)
    coordinator = _RuntimeCoordinator(sessions, event_store)
    next_id = 0

    def runtime_id() -> str:
        nonlocal next_id
        next_id += 1
        return f"runtime-id-{next_id}"

    emitter = RuntimeEventEmitter(
        event_sink=event_store,
        projection_store=trace_store,
        payload_offloader=PayloadOffloader(None, bucket_name="runtime-payloads"),
        id_factory=runtime_id,
    )
    manager = ExecutionManager(
        agent_factory=factory,
        session_manager=sessions,
        execution_repository=_RuntimeExecutionRepository(),
        execution_coordinator=coordinator,
        event_sink=event_store,
        event_emitter=emitter,
        lease_renew_interval_seconds=0,
        id_factory=runtime_id,
    )
    return (
        manager,
        sessions,
        event_store,
        trace_store,
        metric_client,
        checkpoints,
        tool_events,
        started,
    )


def _payload_audit(events: list[RuntimeEvent]) -> tuple[int, int]:
    payload_refs = {event.payload_ref for event in events if event.payload_ref is not None}
    credential_values = (
        "deterministic-test-secret",
        "sk-live",
        "Bearer deterministic",
    )
    credential_events = sum(
        any(value in event.to_json() for value in credential_values) for event in events
    )
    return len(payload_refs), credential_events


async def _run_sessions(
    session_count: int,
    loader: SingleflightLoader[AgentDefinition],
    package: AgentPackageRef,
    *,
    clock: Callable[[], float],
) -> _RuntimeLoadMeasurements:
    session_ids = tuple(f"session-{index}" for index in range(session_count))
    (
        manager,
        sessions,
        event_store,
        traces,
        metric_client,
        checkpoints,
        tool_events,
        _started,
    ) = _build_runtime_path(package, session_ids)
    definition_latencies: list[float] = []
    platform_latencies: list[float] = []
    results: list[tuple[str, ExecutionStatus, str]] = []

    async def run_one(session_number: int) -> None:
        session_id = session_ids[session_number]
        principal = sessions.principals[session_id]
        started = clock()
        # The cached definition load is synchronous by design here: measuring
        # it directly isolates cache work from thread-pool scheduling noise.
        definition = loader.load(package)
        definition_latencies.append((clock() - started) * 1000)
        platform_started = time.perf_counter()
        result = await manager.execute_async(
            session_id,
            f"请查询第 {session_number} 个会话的营业收入",
            principal,
            execution_id=f"execution-{session_number}",
            trace_id=f"trace-{session_number}",
            timeout_seconds=1.0,
        )
        platform_latencies.append((time.perf_counter() - platform_started) * 1000)
        results.append((result.execution_id, result.status, result.trace_id))
        assert definition.name

    await asyncio.gather(*(run_one(index) for index in range(session_count)))
    covered = 0
    for execution_id, status, trace_id in results:
        projection = traces.projections.get(execution_id)
        execution_events = [
            event for event in event_store.events if event.execution_id == execution_id
        ]
        if (
            status is ExecutionStatus.SUCCEEDED
            and projection is not None
            and projection.status == ExecutionStatus.SUCCEEDED.value
            and projection.timeline
            and {event.trace_id for event in execution_events} == {trace_id}
        ):
            covered += 1
    raw_payloads, credentials = _payload_audit(event_store.events + tool_events)
    return _RuntimeLoadMeasurements(
        definition_latencies=definition_latencies,
        platform_latencies=platform_latencies,
        successful=sum(status is ExecutionStatus.SUCCEEDED for _, status, _ in results),
        covered=covered,
        runtime_events_recorded=len(event_store.events),
        trace_projections_recorded=len(traces.projections),
        tool_calls_recorded=len(metric_client.calls),
        tool_events_recorded=len(tool_events),
        checkpoints_recorded=len(checkpoints.records),
        raw_payloads_recorded=raw_payloads,
        credentials_recorded=credentials,
    )


def _measure_cancellation(package: AgentPackageRef) -> float:
    async def scenario() -> float:
        (
            manager,
            sessions,
            _event_store,
            _traces,
            _metric_client,
            _checkpoints,
            _tool_events,
            started,
        ) = _build_runtime_path(package, ("session-cancel",), stall=True)
        if started is None:
            raise AssertionError("cancellation path must expose a graph start signal")
        principal = sessions.principals["session-cancel"]
        execution_id = "execution-cancel"
        token = RedisCancellationToken(
            _CancellationRedis(),
            principal.tenant_id,
            execution_id,
            poll_interval_seconds=0.0005,
        )
        execution = asyncio.create_task(
            manager.execute_async(
                "session-cancel",
                "请查询可取消的营业收入",
                principal,
                execution_id=execution_id,
                trace_id="trace-cancel",
                cancellation_token=token,
                timeout_seconds=2.0,
            )
        )
        await started.wait()
        started_at = time.perf_counter()
        await token.cancel()
        result = await execution
        assert result.status is ExecutionStatus.CANCELLED
        return time.perf_counter() - started_at

    return asyncio.run(scenario())


def run_deterministic_load(
    *,
    session_count: int = DEFAULT_SESSIONS,
    clock: Callable[[], float] | None = None,
) -> PerformanceReport:
    if not MIN_SESSIONS <= session_count <= MAX_SESSIONS:
        raise ValueError(f"session_count must be between {MIN_SESSIONS} and {MAX_SESSIONS}")
    if clock is None:
        clock = _DeterministicClock()

    with tempfile.TemporaryDirectory(prefix="runtime-task13-") as temporary_root:
        root = Path(temporary_root)
        package_root = root / "package"
        digest = build_package(package_root)
        package = package_ref(digest)
        source = _DirectoryPackageSource({digest: package_root})
        loader = SingleflightLoader[AgentDefinition](
            PackageCache(root / "cache", source),
            _build_definition,
            max_entries=4,
            idle_ttl_seconds=60.0,
            clock=clock,
        )
        loader.load(package)
        measurements = asyncio.run(
            _run_sessions(session_count, loader, package, clock=clock)
        )
        cancellation_seconds = _measure_cancellation(package)

    cache_misses = max(0, source.downloads - 1)
    return PerformanceReport(
        session_count=session_count,
        cache_hit_rate=(session_count - min(session_count, cache_misses)) / session_count,
        cached_definition_p95_ms=_p95(measurements.definition_latencies),
        platform_overhead_p95_ms=_p95(measurements.platform_latencies),
        cooperative_cancel_seconds=cancellation_seconds,
        trace_coverage=measurements.covered / session_count,
        short_task_success_rate=measurements.successful / session_count,
        raw_payloads_recorded=measurements.raw_payloads_recorded,
        credentials_recorded=measurements.credentials_recorded,
        runtime_events_recorded=measurements.runtime_events_recorded,
        trace_projections_recorded=measurements.trace_projections_recorded,
        tool_calls_recorded=measurements.tool_calls_recorded,
        tool_events_recorded=measurements.tool_events_recorded,
        checkpoints_recorded=measurements.checkpoints_recorded,
    )


if __name__ == "__main__":
    print(json.dumps(run_deterministic_load().to_dict(), sort_keys=True))
