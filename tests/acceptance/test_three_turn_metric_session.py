from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import yaml

from apps.runtime_api.routes.events import SSEEventStream
from packages.event_model.emitter import RuntimeEventEmitter
from packages.event_model.payloads import PayloadOffloader
from packages.event_model.projection import TraceProjection
from packages.execution_manager.service import ExecutionManager, ExecutionResult
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
from packages.session_manager.locks import ExecutionFence, SessionLockLease
from packages.tool_gateway.contracts import (
    MetricQuery,
    MetricResult,
    ToolEventContext,
)
from packages.tool_gateway.service import LocalSequencedToolEventSink, ToolGateway

ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIGEST = "sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04"
PACKAGE = AgentPackageRef(
    tenant_id="tenant-a",
    agent_id="agent-metric-query",
    version="0.1.0",
    digest=PACKAGE_DIGEST,
    runtime_type=RuntimeType.DEEPAGENTS,
    sdk_version="0.7.7",
)
PRINCIPAL = Principal(
    tenant_id="tenant-a",
    user_id="user-a",
    actor_id="actor-a",
    worker_id="worker-a",
    permissions=("tool:query_metric",),
)

_PROMPTS = (
    "本月散运营业收入是多少？",
    "同比呢？",
    "按航线拆开看。",
    "其中收入最高的是哪条航线？",
)
_QUERIES = (
    ("营业收入", "本月", "散运公司"),
    ("营业收入", "上年", "散运公司"),
    ("营业收入-航线", "本月", "散运公司"),
    ("营业收入-航线", "本月", "散运公司"),
)
_ANSWERS = (
    "本月散运营业收入为 1280000.00 元。",
    "本月散运营业收入同比增长 12.5%。",
    "按航线拆分：远东航线 720000.00 元，欧洲航线 560000.00 元。",
    "其中收入最高的是远东航线。",
)


def _raw(method: str, data: object) -> dict[str, object]:
    return {"method": method, "params": {"namespace": [], "data": data}}


def _session(session_id: str, principal: Principal) -> RuntimeSession:
    timestamp = datetime(2026, 8, 20, tzinfo=UTC)
    return RuntimeSession(
        session_id=session_id,
        thread_id=session_id,
        tenant_id=principal.tenant_id,
        user_id=principal.user_id,
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


@dataclass(frozen=True, slots=True)
class CheckpointRecord:
    thread_id: str
    checkpoint_id: str
    execution_id: str
    execution_epoch: int
    input_text: str
    turn_number: int
    package_digest: str


class DeterministicCheckpointStore:
    """A durable-state double with the same thread and epoch invariants."""

    def __init__(self) -> None:
        self.records: dict[str, list[CheckpointRecord]] = {}
        self.configs: list[dict[str, dict[str, str | int]]] = []
        self._active: dict[str, tuple[str, int]] = {}

    def fence(
        self,
        thread_id: str,
        execution_id: str,
        execution_epoch: int,
    ) -> None:
        self._active[thread_id] = (execution_id, execution_epoch)

    def write(
        self,
        config: dict[str, dict[str, str | int]],
        input_text: str,
    ) -> CheckpointRecord:
        configurable = config["configurable"]
        thread_id = str(configurable["thread_id"])
        execution_id = str(configurable["runtime_execution_id"])
        execution_epoch = int(configurable["runtime_execution_epoch"])
        if self._active.get(thread_id) != (execution_id, execution_epoch):
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="checkpoint write was rejected by the epoch fence",
                )
            )
        previous = self.records.setdefault(thread_id, [])
        record = CheckpointRecord(
            thread_id=thread_id,
            checkpoint_id=f"checkpoint-{thread_id}-{len(previous) + 1}",
            execution_id=execution_id,
            execution_epoch=execution_epoch,
            input_text=input_text,
            turn_number=len(previous) + 1,
            package_digest=str(configurable["runtime_package_digest"]),
        )
        previous.append(record)
        self.configs.append(config)
        return record


class SessionStore:
    def __init__(self, sessions: tuple[RuntimeSession, ...]) -> None:
        self.sessions = {
            (item.tenant_id, item.session_id): item for item in sessions
        }

    async def get_session(
        self,
        session_id: str,
        principal: Principal,
    ) -> RuntimeSession | None:
        session = self.sessions.get((principal.tenant_id, session_id))
        if session is None or session.user_id != principal.user_id:
            return None
        return session

    def activate(
        self,
        session_id: str,
        principal: Principal,
        execution_id: str,
        epoch: int,
    ) -> None:
        key = (principal.tenant_id, session_id)
        current = self.sessions[key]
        self.sessions[key] = current.model_copy(
            update={
                "active_execution_id": execution_id,
                "execution_epoch": epoch,
                "revision": current.revision + 1,
                "updated_at": datetime.now(UTC),
            }
        )


class RenewableLease:
    def __init__(self, session_id: str, worker_id: str) -> None:
        self.tenant_id = "tenant-a"
        self.session_id = session_id
        self.owner_id = worker_id
        self.active = True
        self.releases = 0

    async def renew(self) -> bool:
        return self.active

    async def release(self) -> bool:
        self.releases += 1
        self.active = False
        return True


class EpochCoordinator:
    def __init__(
        self,
        sessions: SessionStore,
        checkpoints: DeterministicCheckpointStore,
        event_store: DurableEventStore,
    ) -> None:
        self.sessions = sessions
        self.checkpoints = checkpoints
        self.event_store = event_store
        self.epochs: dict[str, int] = {}
        self.leases: list[RenewableLease] = []

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
        if current is None:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.SESSION_CLOSED,
                    message="session is unavailable",
                )
            )
        epoch = self.epochs.get(session_id, current.execution_epoch) + 1
        self.epochs[session_id] = epoch
        self.sessions.activate(session_id, principal, execution_id, epoch)
        self.checkpoints.fence(session_id, execution_id, epoch)
        self.event_store.set_epoch(session_id, epoch)
        worker_id = principal.worker_id
        if worker_id is None:
            raise AssertionError("acceptance principal must have worker identity")
        lease = RenewableLease(session_id, worker_id)
        self.leases.append(lease)
        return ExecutionFence(
            tenant_id=principal.tenant_id,
            session_id=session_id,
            execution_id=execution_id,
            execution_epoch=epoch,
            worker_id=worker_id,
            lease=cast(SessionLockLease, lease),
        )


class DurableEventStore:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []
        self._next_sequence: dict[str, int] = {}
        self.terminal_executions: set[str] = set()
        self.epochs: dict[str, int] = {}

    def set_epoch(self, session_id: str, epoch: int) -> None:
        self.epochs[session_id] = epoch

    async def append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        return self._append(event_factory, principal, execution_epoch, terminal=False)

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        del error
        event = self._append(event_factory, principal, execution_epoch, terminal=True)
        self.terminal_executions.add(event.execution_id)
        assert status in {
            ExecutionStatus.SUCCEEDED,
            ExecutionStatus.FAILED,
            ExecutionStatus.TIMED_OUT,
            ExecutionStatus.CANCELLED,
        }
        return event

    def _append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        *,
        terminal: bool,
    ) -> RuntimeEvent:
        event = event_factory(1)
        next_sequence = self._next_sequence.get(event.session_id, 0) + 1
        if event.sequence != next_sequence:
            event = event_factory(next_sequence)
        if terminal and event.execution_id in self.terminal_executions:
            raise AssertionError("deterministic terminal CAS accepted a duplicate")
        if event.tenant_id != principal.tenant_id:
            raise AssertionError("event crossed the tenant boundary")
        if principal.worker_id != event.worker_id:
            raise AssertionError("event crossed the worker boundary")
        if self.epochs.get(event.session_id) != execution_epoch:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="event append was rejected by the epoch fence",
                )
            )
        self._next_sequence[event.session_id] = next_sequence
        self.events.append(event)
        return event


class TraceStore:
    def __init__(self) -> None:
        self.projections: dict[str, TraceProjection] = {}

    async def save(self, projection: TraceProjection) -> None:
        self.projections[projection.execution_id] = projection


class MetricGatewayDouble:
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
        values = {
            ("营业收入", "本月"): Decimal("1280000.00"),
            ("营业收入", "上年"): Decimal("1137777.78"),
            ("营业收入-航线", "本月"): Decimal("1280000.00"),
        }
        return MetricResult(
            metric=query.metric,
            period=query.period,
            org=query.org,
            value=values[(query.metric, query.period)],
            unit="CNY",
        )


class DeterministicGraph:
    def __init__(
        self,
        *,
        principal: Principal,
        checkpoints: DeterministicCheckpointStore,
        metric_client: MetricGatewayDouble,
        tool_events: list[RuntimeEvent],
    ) -> None:
        self.principal = principal
        self.checkpoints = checkpoints
        self.metric_client = metric_client
        self.tool_events = tool_events
        self.configs: list[dict[str, dict[str, str | int]]] = []

    def astream_events(
        self,
        input: object,
        *,
        config: dict[str, dict[str, str | int]],
        version: str,
    ) -> _DeterministicRun:
        assert version == "v3"
        configurable = config["configurable"]
        session_id = str(configurable["thread_id"])
        execution_id = str(configurable["runtime_execution_id"])
        worker_id = str(configurable["runtime_worker_id"])
        assert session_id == str(configurable["runtime_session_id"])
        assert str(configurable["runtime_package_digest"]) == PACKAGE_DIGEST
        input_mapping = cast(Mapping[str, object], input)
        messages = cast(list[object], input_mapping["messages"])
        prompt = str(cast(Mapping[str, object], messages[-1])["content"])
        turn = len(self.checkpoints.records.get(session_id, []))
        if turn >= len(_QUERIES):
            raise AssertionError("deterministic acceptance only defines four turns")
        metric, period, org = _QUERIES[turn]
        answer = _ANSWERS[turn]
        self.configs.append(config)
        event_context = ToolEventContext(
            trace_id=f"trace-{execution_id}",
            parent_span_id=None,
            session_id=session_id,
            execution_id=execution_id,
            worker_id=worker_id,
            package=PACKAGE,
        )
        tool_gateway = ToolGateway(
            client=self.metric_client,
            principal=self.principal,
            package_allowlist=("query_metric",),
            event_context=event_context,
            event_sink=LocalSequencedToolEventSink(self.tool_events.append),
            request_id_factory=lambda: f"tool-request-{len(self.metric_client.calls) + 1}",
        )
        result = tool_gateway.query_metric(metric=metric, period=period, org=org)
        checkpoint = self.checkpoints.write(config, prompt)
        result_json = cast(dict[str, object], result.model_dump(mode="json"))
        empty_metadata: dict[str, object] = {}
        raw_events = [
            _raw(
                "messages",
                (
                    {"event": "message-start", "role": "ai", "id": f"model-{turn + 1}"},
                    empty_metadata,
                ),
            ),
            _raw(
                "messages",
                (
                    {
                        "event": "content-block-delta",
                        "index": 0,
                        "delta": {"type": "text-delta", "text": answer},
                    },
                    empty_metadata,
                ),
            ),
            _raw(
                "tools",
                {
                    "event": "tool-started",
                    "tool_call_id": f"call-{turn + 1}",
                    "tool_name": "query_metric",
                    "input": {"metric": metric, "period": period, "org": org},
                },
            ),
            _raw(
                "tools",
                {
                    "event": "tool-output",
                    "tool_call_id": f"call-{turn + 1}",
                    "tool_name": "query_metric",
                    "output": result_json,
                },
            ),
            _raw(
                "tools",
                {
                    "event": "tool-completed",
                    "tool_call_id": f"call-{turn + 1}",
                    "tool_name": "query_metric",
                    "output": result_json,
                },
            ),
            _raw(
                "messages",
                (
                    {
                        "event": "message-finish",
                        "metadata": {
                            "finish_reason": "stop",
                            "usage": {"input_tokens": 20, "output_tokens": 16},
                        },
                    },
                    empty_metadata,
                ),
            ),
            _raw(
                "values",
                {
                    "thread_id": session_id,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "turn_number": checkpoint.turn_number,
                },
            ),
            _raw(
                "output",
                {
                    "messages": [{"type": "ai", "content": answer}],
                    "last_message_text": answer,
                    "checkpoint_id": checkpoint.checkpoint_id,
                },
            ),
        ]
        return _DeterministicRun(raw_events, answer)


class _DeterministicRun:
    def __init__(self, events: list[dict[str, object]], answer: str) -> None:
        self.events = events
        self.answer = answer

    def __aiter__(self) -> AsyncIterator[dict[str, object]]:
        async def iterator() -> AsyncIterator[dict[str, object]]:
            for event in self.events:
                yield event

        return iterator()

    async def output(self) -> dict[str, object]:
        return {
            "messages": [{"type": "ai", "content": self.answer}],
            "last_message_text": self.answer,
        }


class DeterministicFactory:
    def __init__(self, graph: DeterministicGraph) -> None:
        self.graph = graph
        self.requested_digests: list[str] = []

    def get(self, package_digest: str) -> DeterministicGraph:
        self.requested_digests.append(package_digest)
        assert package_digest == PACKAGE_DIGEST
        return self.graph

    def get_limits(self, package_digest: str) -> object:
        assert package_digest == PACKAGE_DIGEST
        return type("Limits", (), {"execution_timeout_seconds": 60.0})()


class ExecutionRepositoryDouble:
    async def complete_execution(self, *args: object, **kwargs: object) -> None:
        del args, kwargs


class MetricsObserver:
    def observe_event(self, _event: RuntimeEvent) -> None:
        return None


class SessionEventReader:
    def __init__(self, sessions: SessionStore, events: DurableEventStore) -> None:
        self.sessions = sessions
        self.events = events

    async def owns_session(self, session_id: str, principal: Principal) -> bool:
        return await self.sessions.get_session(session_id, principal) is not None

    async def list_after(
        self,
        session_id: str,
        after_sequence: int,
        principal: Principal,
    ) -> tuple[RuntimeEvent, ...]:
        if not await self.owns_session(session_id, principal):
            return ()
        return tuple(
            event
            for event in self.events.events
            if event.session_id == session_id
            and event.tenant_id == principal.tenant_id
            and event.sequence > after_sequence
        )


def _build_manager(
    *,
    principal: Principal,
    sessions: SessionStore,
    checkpoints: DeterministicCheckpointStore,
    event_store: DurableEventStore,
    traces: TraceStore,
    metric_client: MetricGatewayDouble,
    tool_events: list[RuntimeEvent],
    runtime_id_factory: Callable[[], str] | None = None,
    event_id_factory: Callable[[], str] | None = None,
) -> tuple[ExecutionManager, DeterministicFactory]:
    graph = DeterministicGraph(
        principal=principal,
        checkpoints=checkpoints,
        metric_client=metric_client,
        tool_events=tool_events,
    )
    factory = DeterministicFactory(graph)
    coordinator = EpochCoordinator(sessions, checkpoints, event_store)
    next_id = 0

    def next_runtime_id() -> str:
        nonlocal next_id
        next_id += 1
        return f"runtime-id-{next_id}"

    next_event = 0

    def next_event_id() -> str:
        nonlocal next_event
        next_event += 1
        return f"event-id-{next_event}"

    emitter = RuntimeEventEmitter(
        event_sink=event_store,
        projection_store=traces,
        payload_offloader=PayloadOffloader(None, bucket_name="runtime-payloads"),
        metrics=MetricsObserver(),
        id_factory=event_id_factory or next_event_id,
    )
    manager = ExecutionManager(
        agent_factory=factory,
        session_manager=sessions,
        execution_repository=ExecutionRepositoryDouble(),
        execution_coordinator=coordinator,
        event_sink=event_store,
        event_emitter=emitter,
        lease_renew_interval_seconds=0,
        id_factory=runtime_id_factory or next_runtime_id,
    )
    return manager, factory


async def _collect_sse(
    stream: SSEEventStream,
    session_id: str,
    principal: Principal,
    after_sequence: int,
) -> tuple[list[str], int]:
    chunks = [
        chunk
        async for chunk in stream.stream(
            session_id,
            principal,
            after_sequence=after_sequence,
        )
    ]
    sequence = after_sequence
    for chunk in chunks:
        first_line = chunk.splitlines()[0]
        if first_line.startswith("id: "):
            sequence = max(sequence, int(first_line.removeprefix("id: ")))
    return chunks, sequence


def test_deployment_contract_is_separate_pinned_and_secret_free() -> None:
    process_files = {
        "runtime-api.service": ROOT / "deploy/process/runtime-api.service",
        "runtime-worker.service": ROOT / "deploy/process/runtime-worker.service",
    }
    process_text = "\n".join(path.read_text(encoding="utf-8") for path in process_files.values())
    assert "apps.runtime_api.main:app" in process_text
    assert "apps.runtime_worker.consumer:TaskConsumer" in process_text
    assert "/api/v1/runtime" in process_text
    assert "0.7.7" in process_text

    compose_path = ROOT / "deploy/compose/compose.integration.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    assert isinstance(compose, dict)
    services = cast(dict[str, object], compose["services"])
    for service in (
        "runtime-api",
        "runtime-worker",
        "runtime-console",
        "postgres",
        "redis",
        "minio",
        "prometheus",
        "grafana",
        "model-gateway",
        "tool-gateway",
    ):
        assert service in services
    assert "runtime-console" in services
    assert "condition: service_healthy" in compose_path.read_text(encoding="utf-8")
    runtime_api = cast(dict[str, object], services["runtime-api"])
    runtime_environment = cast(dict[str, object], runtime_api["environment"])
    for variable in (
        "RUNTIME_WORKER_ID",
        "RUNTIME_MINIO_ENDPOINT",
        "RUNTIME_MINIO_ACCESS_KEY",
        "RUNTIME_MINIO_SECRET_KEY",
        "RUNTIME_MINIO_BUCKET",
        "MODEL_GATEWAY_BASE_URL",
        "MODEL_GATEWAY_API_KEY",
        "MODEL_GATEWAY_MODEL",
    ):
        assert variable in runtime_environment
    assert "RUNTIME_MODEL_API_KEY" not in runtime_environment
    minio_environment = cast(
        dict[str, object],
        cast(dict[str, object], services["minio"])["environment"],
    )
    assert minio_environment["MINIO_ROOT_USER"] == (
        "${RUNTIME_MINIO_ACCESS_KEY:?set RUNTIME_MINIO_ACCESS_KEY}"
    )
    assert minio_environment["MINIO_ROOT_PASSWORD"] == (
        "${RUNTIME_MINIO_SECRET_KEY:?set RUNTIME_MINIO_SECRET_KEY}"
    )
    for service in ("postgres", "redis", "minio", "prometheus", "grafana"):
        image = str(cast(dict[str, object], services[service])["image"])
        assert ":latest" not in image
        assert image.rsplit(":", 1)[-1] not in {"8", "12", "18", "v3"}

    env_text = (ROOT / "deploy/env.example").read_text(encoding="utf-8")
    all_text = process_text + compose_path.read_text(encoding="utf-8") + env_text
    assert "MODEL_GATEWAY_API_KEY" in env_text
    assert "RUNTIME_TOOL_GATEWAY_API_KEY" in env_text
    assert "dev-token" not in all_text
    assert "sk-" not in all_text
    assert "password=secret" not in all_text.lower()


def test_deterministic_three_turn_session_survives_worker_restart_and_fences_old_worker() -> None:
    async def scenario() -> None:
        checkpoints = DeterministicCheckpointStore()
        event_store = DurableEventStore()
        traces = TraceStore()
        metric_client = MetricGatewayDouble()
        tool_events: list[RuntimeEvent] = []
        principal = PRINCIPAL
        second_principal = principal.model_copy(
            update={"user_id": "user-b", "actor_id": "actor-b"}
        )
        sessions = SessionStore(
            (_session("session-main", principal), _session("session-other", second_principal))
        )

        runtime_counter = 0
        active_execution: str | None = None

        def next_runtime_id() -> str:
            nonlocal runtime_counter, active_execution
            if active_execution is None:
                runtime_counter += 1
                active_execution = f"execution-{runtime_counter}"
                return active_execution
            trace_id = f"trace-{active_execution}"
            active_execution = None
            return trace_id

        event_counter = 0

        def next_event_id() -> str:
            nonlocal event_counter
            event_counter += 1
            return f"event-{event_counter}"

        first_worker, first_factory = _build_manager(
            principal=principal,
            sessions=sessions,
            checkpoints=checkpoints,
            event_store=event_store,
            traces=traces,
            metric_client=metric_client,
            tool_events=tool_events,
            runtime_id_factory=next_runtime_id,
            event_id_factory=next_event_id,
        )
        results: list[ExecutionResult] = []
        for prompt in _PROMPTS[:3]:
            results.append(
                await first_worker.execute_turn(
                    "session-main",
                    prompt,
                    principal,
                    mode=ExecutionMode.SYNC,
                )
            )

        restarted_principal = principal.model_copy(update={"worker_id": "worker-b"})
        restarted_worker, restarted_factory = _build_manager(
            principal=restarted_principal,
            sessions=sessions,
            checkpoints=checkpoints,
            event_store=event_store,
            traces=traces,
            metric_client=metric_client,
            tool_events=tool_events,
            runtime_id_factory=next_runtime_id,
            event_id_factory=next_event_id,
        )
        fourth = await restarted_worker.execute_turn(
            "session-main",
            _PROMPTS[3],
            restarted_principal,
            mode=ExecutionMode.SYNC,
        )
        results.append(fourth)

        assert len(results) == 4
        assert all(result.status is ExecutionStatus.SUCCEEDED for result in results)
        assert len({result.trace_id for result in results}) == 4
        assert first_factory.requested_digests == [PACKAGE_DIGEST] * 3
        assert restarted_factory.requested_digests == [PACKAGE_DIGEST]
        assert [item.turn_number for item in checkpoints.records["session-main"]] == [1, 2, 3, 4]
        assert [item.package_digest for item in checkpoints.records["session-main"]] == [
            PACKAGE_DIGEST
        ] * 4
        assert {item.thread_id for item in checkpoints.records["session-main"]} == {"session-main"}

        stale_config = checkpoints.configs[2]
        with pytest.raises(RuntimeRepositoryError) as fenced:
            checkpoints.write(stale_config, "late write from worker-a")
        assert fenced.value.error.code is ErrorCode.EXECUTION_FENCED
        assert [item.turn_number for item in checkpoints.records["session-main"]] == [1, 2, 3, 4]

        stale_event = next(
            event
            for event in event_store.events
            if event.execution_id == results[2].execution_id
            and event.event_type == "model.delta"
        )
        with pytest.raises(RuntimeRepositoryError) as stale_event_fenced:
            await event_store.append(
                lambda sequence: stale_event.model_copy(update={"sequence": sequence}),
                principal,
                3,
            )
        assert stale_event_fenced.value.error.code is ErrorCode.EXECUTION_FENCED

        stale_terminal = next(
            event
            for event in event_store.events
            if event.execution_id == results[2].execution_id
            and event.event_type == "execution.succeeded"
        ).model_copy(update={"execution_id": "execution-stale-terminal"})
        with pytest.raises(RuntimeRepositoryError) as stale_terminal_fenced:
            await event_store.append_terminal(
                lambda sequence: stale_terminal.model_copy(update={"sequence": sequence}),
                principal,
                3,
                ExecutionStatus.SUCCEEDED,
            )
        assert stale_terminal_fenced.value.error.code is ErrorCode.EXECUTION_FENCED

        assert len(metric_client.calls) == 4
        assert all(call[1].tenant_id == "tenant-a" for call in metric_client.calls)
        assert all(call[1].permissions == ("tool:query_metric",) for call in metric_client.calls)
        assert len(tool_events) == 8
        assert {event.event_type for event in tool_events} == {
            "tool.started",
            "tool.completed",
        }
        assert all(event.package.digest == PACKAGE_DIGEST for event in tool_events)

        for result in results:
            trace = traces.projections[result.execution_id]
            assert trace.trace_id == result.trace_id
            assert trace.package_digest == PACKAGE_DIGEST
            assert trace.status == "succeeded"
            assert trace.timeline
            assert {entry.event_id for entry in trace.timeline}
            assert any(entry.event_type == "tool.started" for entry in trace.timeline)
            assert any(entry.event_type == "tool.output" for entry in trace.timeline)
            assert any(entry.event_type == "tool.completed" for entry in trace.timeline)
            assert any(entry.event_type == "execution.succeeded" for entry in trace.timeline)
            execution_events = [
                event for event in event_store.events if event.execution_id == result.execution_id
            ]
            assert execution_events
            assert {event.trace_id for event in execution_events} == {result.trace_id}
            assert {event.package.digest for event in execution_events} == {PACKAGE_DIGEST}

        sse = SSEEventStream(
            reader=SessionEventReader(sessions, event_store),
            live_source=None,
        )
        cursor = 0
        for result in results:
            chunks, cursor = await _collect_sse(
                sse,
                "session-main",
                restarted_principal,
                cursor,
            )
            serialized = "".join(chunks)
            assert "text/event-stream" not in serialized
            assert "execution.succeeded" in serialized
            assert result.trace_id in serialized
            assert f"{PACKAGE_DIGEST}" in serialized

        other_worker, _other_factory = _build_manager(
            principal=second_principal.model_copy(update={"worker_id": "worker-c"}),
            sessions=sessions,
            checkpoints=checkpoints,
            event_store=event_store,
            traces=traces,
            metric_client=metric_client,
            tool_events=tool_events,
            runtime_id_factory=next_runtime_id,
            event_id_factory=next_event_id,
        )
        other = await other_worker.execute_turn(
            "session-other",
            _PROMPTS[0],
            second_principal.model_copy(update={"worker_id": "worker-c"}),
        )
        assert other.status is ExecutionStatus.SUCCEEDED
        assert [item.turn_number for item in checkpoints.records["session-other"]] == [1]
        assert checkpoints.records["session-other"][0].thread_id != checkpoints.records[
            "session-main"
        ][0].thread_id
        other_events = [
            event for event in event_store.events if event.execution_id == other.execution_id
        ]
        assert other_events
        assert all(event.session_id == "session-other" for event in other_events)
        main_checkpoints = {
            item.checkpoint_id for item in checkpoints.records["session-main"]
        }
        other_checkpoints = {
            item.checkpoint_id for item in checkpoints.records["session-other"]
        }
        main_serialized = "".join(
            event.to_json()
            for event in event_store.events
            if event.session_id == "session-main"
        )
        other_serialized = "".join(
            event.to_json()
            for event in event_store.events
            if event.session_id == "session-other"
        )
        assert all(checkpoint not in other_serialized for checkpoint in main_checkpoints)
        assert all(checkpoint not in main_serialized for checkpoint in other_checkpoints)

    asyncio.run(scenario())


@pytest.mark.skipif(
    os.environ.get("RUNTIME_TASK14_LIVE") != "1",
    reason="live Task 14 acceptance requires RUNTIME_TASK14_LIVE=1",
)
def test_live_three_turn_acceptance_is_explicitly_gated() -> None:
    required = ("RUNTIME_TASK14_LIVE_BASE_URL", "RUNTIME_TASK14_LIVE_TOKEN")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.skip("live Task 14 acceptance missing documented environment variables")

    base_url = os.environ["RUNTIME_TASK14_LIVE_BASE_URL"].rstrip("/")
    token = os.environ["RUNTIME_TASK14_LIVE_TOKEN"]
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(timeout=30.0, trust_env=False) as client:
        created = client.post(
            f"{base_url}/api/v1/runtime/sessions",
            json={"agent_id": "agent-metric-query", "version": "0.1.0"},
            headers=headers,
        )
        assert created.status_code == 201
        session = cast(dict[str, Any], created.json())
        session_id = str(session["session_id"])
        assert session["thread_id"] == session_id
        assert cast(dict[str, Any], session["package"])["digest"]

        for prompt in _PROMPTS[:3]:
            with client.stream(
                "POST",
                f"{base_url}/api/v1/runtime/sessions/{session_id}/executions/stream",
                json={"input": prompt, "mode": "stream"},
                headers=headers,
            ) as response:
                assert response.status_code == 200
                body = "".join(response.iter_text())
            assert "execution.succeeded" in body
            assert "trace_id" in body

        fetched = client.get(
            f"{base_url}/api/v1/runtime/sessions/{session_id}",
            headers=headers,
        )
        assert fetched.status_code == 200
        final_session = cast(dict[str, Any], fetched.json())
        assert final_session["thread_id"] == session_id
        assert cast(dict[str, Any], final_session["package"])["digest"] == cast(
            dict[str, Any], session["package"]
        )["digest"]
