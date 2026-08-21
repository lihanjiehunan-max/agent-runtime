from __future__ import annotations

import asyncio
import io
import os
import tarfile
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from minio import Minio
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from apps.runtime_api.routes.events import SSEEventStream
from packages.execution_manager.service import ExecutionManager
from packages.package_loader.cache import PackageCache
from packages.package_loader.minio_source import (
    MinioPackageSource,
    PackageSourceError,
    PackageSourceErrorCode,
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
from tests.unit.package_loader._package_builder import build_package, package_ref

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


class _ObjectStoreDouble:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.available = True
        self.downloads = 0

    def download_object(self, bucket_name: str, object_name: str, destination: Path) -> None:
        self.downloads += 1
        if not self.available:
            raise OSError("object store unavailable")
        destination.write_bytes(self.objects[(bucket_name, object_name)])


def _archive_bytes(package_root: Path) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for path in sorted(package_root.rglob("*")):
            if path.is_file():
                archive.add(path, arcname=path.relative_to(package_root).as_posix())
    return output.getvalue()


def test_cold_package_cache_outage_fails_closed_without_a_verified_entry(tmp_path: Path) -> None:
    client = _ObjectStoreDouble()
    client.available = False
    source = MinioPackageSource(client, bucket_name="agent-packages")
    cache = PackageCache(tmp_path / "cache", source)

    with pytest.raises(PackageSourceError) as raised:
        cache.load(package_ref(f"sha256:{'1' * 64}"))

    assert raised.value.code is PackageSourceErrorCode.DOWNLOAD_FAILED
    assert not list((tmp_path / "cache").iterdir())


def test_warm_package_cache_survives_minio_outage(tmp_path: Path) -> None:
    package_root = tmp_path / "origin"
    digest = build_package(package_root)
    reference = package_ref(digest)
    client = _ObjectStoreDouble()
    source = MinioPackageSource(client, bucket_name="agent-packages")
    client.objects["agent-packages", source.object_key(reference)] = _archive_bytes(package_root)
    cache = PackageCache(tmp_path / "cache", source)

    first = cache.load(reference)
    client.available = False
    second = cache.load(reference)

    assert first.reference == reference
    assert second.reference == reference
    assert client.downloads == 1


class _Lease:
    tenant_id = "tenant-a"
    session_id = "session-chaos"
    owner_id = "worker-a"

    def __init__(self) -> None:
        self.active = True
        self.releases = 0

    async def renew(self) -> bool:
        return self.active

    async def release(self) -> bool:
        self.releases += 1
        self.active = False
        return True


class _Sessions:
    def __init__(self) -> None:
        timestamp = datetime(2026, 8, 20, tzinfo=UTC)
        self.session = RuntimeSession(
            session_id="session-chaos",
            thread_id="session-chaos",
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

    async def get_session(
        self,
        session_id: str,
        principal: Principal,
    ) -> RuntimeSession | None:
        if session_id != self.session.session_id or principal.tenant_id != "tenant-a":
            return None
        return self.session


class _Coordinator:
    def __init__(self, sessions: _Sessions, lease: _Lease) -> None:
        self.sessions = sessions
        self.lease = lease

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
        del repository, trace_id, request_input
        assert session_id == "session-chaos"
        assert principal == PRINCIPAL
        assert mode is ExecutionMode.ASYNC
        self.sessions.session = self.sessions.session.model_copy(
            update={"active_execution_id": execution_id, "execution_epoch": 8}
        )
        return ExecutionFence(
            tenant_id="tenant-a",
            session_id=session_id,
            execution_id=execution_id,
            execution_epoch=8,
            worker_id="worker-a",
            lease=cast(Any, self.lease),
        )


class _ExecutionRepository:
    async def complete_execution(self, *args: object, **kwargs: object) -> None:
        del args, kwargs


class _EventSink:
    def __init__(self, *, fail_terminal: str | None = None) -> None:
        self.events: list[RuntimeEvent] = []
        self.terminals: list[ExecutionStatus] = []
        self.fail_terminal = fail_terminal
        self.current_tenant_id = "tenant-a"
        self.current_session_id = "session-chaos"
        self.current_worker_id = "worker-a"
        self.current_execution_epoch = 8
        self.current_execution_id: str | None = None

    async def append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        if self.terminals:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="late worker write was fenced after terminal commit",
                )
            )
        event = event_factory(len(self.events) + 1)
        self._validate_fence(event, principal, execution_epoch)
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
        del error
        if self.terminals:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="duplicate terminal commit was fenced",
                )
            )
        event = event_factory(len(self.events) + 1)
        self._validate_fence(event, principal, execution_epoch)
        if self.fail_terminal == "before":
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="terminal CAS failed before commit",
                )
            )
        self.events.append(event)
        self.terminals.append(status)
        if self.fail_terminal == "after":
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="terminal CAS acknowledgement was interrupted",
                )
            )
        return event

    def _validate_fence(
        self,
        event: RuntimeEvent,
        principal: Principal,
        execution_epoch: int,
    ) -> None:
        if (
            event.tenant_id != self.current_tenant_id
            or event.tenant_id != principal.tenant_id
            or event.session_id != self.current_session_id
            or event.worker_id != self.current_worker_id
            or principal.worker_id != self.current_worker_id
            or execution_epoch != self.current_execution_epoch
            or (
                self.current_execution_id is not None
                and event.execution_id != self.current_execution_id
            )
        ):
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="event append was rejected by the current worker and epoch fence",
                )
            )
        if self.current_execution_id is None:
            self.current_execution_id = event.execution_id


class _QuickRun:
    def __aiter__(self) -> AsyncIterator[Mapping[str, object]]:
        async def iterator() -> AsyncIterator[Mapping[str, object]]:
            yield {
                "method": "values",
                "params": {"data": {"messages": [{"type": "ai", "content": "safe"}]}},
            }

        return iterator()

    async def output(self) -> dict[str, object]:
        return {"messages": [{"type": "ai", "content": "safe"}]}


class _StallRun:
    def __init__(self, stall: str) -> None:
        self.stall = stall
        self.cancelled = False

    def __aiter__(self) -> AsyncIterator[Mapping[str, object]]:
        async def iterator() -> AsyncIterator[Mapping[str, object]]:
            if self.stall == "tool":
                yield {
                    "method": "tools",
                    "params": {
                        "data": {
                            "event": "tool-started",
                            "tool_name": "query_metric",
                            "tool_call_id": "call-1",
                            "input": {"metric": "revenue"},
                        }
                    },
                }
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise

        return iterator()

    async def output(self) -> dict[str, object]:
        await asyncio.Event().wait()
        return {"messages": [{"type": "ai", "content": "never returned"}]}


class _Graph:
    def __init__(self, stall: str | None = None) -> None:
        self.stall = stall
        self.run: _QuickRun | _StallRun | None = None

    def astream_events(
        self,
        input: object,
        *,
        config: dict[str, dict[str, str | int]],
        version: str,
    ) -> _QuickRun | _StallRun:
        del input, config
        assert version == "v3"
        self.run = _StallRun(self.stall) if self.stall is not None else _QuickRun()
        return self.run


class _Factory:
    def __init__(self, graph: _Graph) -> None:
        self.graph = graph

    def get(self, package_digest: str) -> _Graph:
        assert package_digest == PACKAGE.digest
        return self.graph

    def get_limits(self, package_digest: str) -> object:
        assert package_digest == PACKAGE.digest
        return type("Limits", (), {"execution_timeout_seconds": 0.05})()


def _manager(
    graph: _Graph,
    sink: _EventSink,
) -> tuple[ExecutionManager, _Sessions, _Lease]:
    sessions = _Sessions()
    lease = _Lease()
    return (
        ExecutionManager(
            agent_factory=_Factory(graph),
            session_manager=sessions,
            execution_repository=_ExecutionRepository(),
            execution_coordinator=_Coordinator(sessions, lease),
            event_sink=sink,
            lease_renew_interval_seconds=0,
        ),
        sessions,
        lease,
    )


@pytest.mark.parametrize("fail_terminal", ["before", "after"])
def test_terminal_cas_outage_never_creates_a_second_terminal(fail_terminal: str) -> None:
    sink = _EventSink(fail_terminal=fail_terminal)
    manager, _sessions, lease = _manager(_Graph(), sink)

    async def scenario() -> None:
        with pytest.raises(RuntimeRepositoryError) as raised:
            await manager.execute_async(
                "session-chaos",
                "deterministic-turn",
                PRINCIPAL,
                execution_id=f"execution-terminal-{fail_terminal}",
                trace_id=f"trace-terminal-{fail_terminal}",
                timeout_seconds=0.05,
            )
        assert raised.value.error.code is ErrorCode.EXECUTION_FENCED

    asyncio.run(scenario())
    assert len(sink.terminals) == (0 if fail_terminal == "before" else 1)
    assert lease.releases == 1
    assert not any(event.event_type == "execution.succeeded" for event in sink.events[:2])


@pytest.mark.parametrize("stall", ["model", "tool"])
def test_model_and_tool_timeout_leave_bounded_terminal_audit(stall: str) -> None:
    sink = _EventSink()
    graph = _Graph(stall)
    manager, _sessions, lease = _manager(graph, sink)

    async def scenario() -> None:
        result = await manager.execute_async(
            "session-chaos",
            "deterministic-timeout",
            PRINCIPAL,
            execution_id=f"execution-{stall}-timeout",
            trace_id=f"trace-{stall}-timeout",
            timeout_seconds=0.05,
        )
        assert result.status is ExecutionStatus.TIMED_OUT
        assert result.error is not None
        assert result.error.code is ErrorCode.EXECUTION_TIMED_OUT

    asyncio.run(scenario())
    assert sink.terminals == [ExecutionStatus.TIMED_OUT]
    assert sink.events[-1].event_type == "execution.timed_out"
    assert lease.releases == 1
    assert isinstance(graph.run, _StallRun)
    assert graph.run.cancelled
    if stall == "tool":
        assert any(event.event_type == "tool.started" for event in sink.events)


def test_stale_worker_late_completion_is_fenced_after_terminal() -> None:
    sink = _EventSink()
    manager, _sessions, _lease = _manager(_Graph(), sink)

    async def scenario() -> None:
        result = await manager.execute_async(
            "session-chaos",
            "deterministic-success",
            PRINCIPAL,
            execution_id="execution-stale",
            trace_id="trace-stale",
            timeout_seconds=0.05,
        )
        assert result.status is ExecutionStatus.SUCCEEDED

    asyncio.run(scenario())
    with pytest.raises(RuntimeRepositoryError) as late_write:
        asyncio.run(sink.append(lambda _sequence: sink.events[0], PRINCIPAL, 8))
    assert late_write.value.error.code is ErrorCode.EXECUTION_FENCED
    assert sink.terminals == [ExecutionStatus.SUCCEEDED]


@pytest.mark.parametrize("stale_field", ["worker", "epoch"])
def test_stale_worker_identity_and_epoch_are_fenced_before_terminal(
    stale_field: str,
) -> None:
    sink = _EventSink()

    def event_factory(sequence: int) -> RuntimeEvent:
        return RuntimeEvent(
            schema_version="runtime.event.v1",
            event_id=f"event-stale-{sequence}",
            sequence=sequence,
            occurred_at=datetime(2026, 8, 20, tzinfo=UTC),
            tenant_id="tenant-a",
            trace_id="trace-stale-before-terminal",
            span_id="span-stale",
            parent_span_id=None,
            session_id="session-chaos",
            execution_id="execution-stale-before-terminal",
            package=PACKAGE,
            worker_id="worker-a",
            sdk_version=PACKAGE.sdk_version,
            event_type="execution.started",
            phase="started",
            duration_ms=None,
            payload={},
            payload_ref=None,
        )

    stale_principal = (
        PRINCIPAL.model_copy(update={"worker_id": "worker-old"})
        if stale_field == "worker"
        else PRINCIPAL
    )
    stale_epoch = 7 if stale_field == "epoch" else 8
    with pytest.raises(RuntimeRepositoryError) as raised:
        asyncio.run(sink.append(event_factory, stale_principal, stale_epoch))

    assert raised.value.error.code is ErrorCode.EXECUTION_FENCED
    assert sink.events == []
    assert sink.terminals == []


class _Reader:
    async def owns_session(self, session_id: str, principal: Principal) -> bool:
        return session_id == "session-sse" and principal.tenant_id == "tenant-a"

    async def list_after(
        self,
        session_id: str,
        after_sequence: int,
        principal: Principal,
    ) -> tuple[RuntimeEvent, ...]:
        del session_id, after_sequence, principal
        return ()


def _sse_event(event_type: str, sequence: int) -> RuntimeEvent:
    return RuntimeEvent(
        schema_version="runtime.event.v1",
        event_id=f"event-sse-{sequence}",
        sequence=sequence,
        occurred_at=datetime(2026, 8, 20, tzinfo=UTC),
        tenant_id="tenant-a",
        trace_id="trace-sse",
        span_id=f"span-sse-{sequence}",
        parent_span_id=None,
        session_id="session-sse",
        execution_id="execution-sse",
        package=PACKAGE,
        worker_id="worker-a",
        sdk_version="0.7.7",
        event_type=event_type,
        phase="runtime",
        duration_ms=None,
        payload={"sequence": sequence},
        payload_ref=None,
    )


class _InterruptedLiveSource:
    def subscribe(
        self,
        session_id: str,
        principal: Principal,
        after_sequence: int,
    ) -> AsyncIterator[RuntimeEvent]:
        del session_id, principal, after_sequence

        async def feed() -> AsyncIterator[RuntimeEvent]:
            yield _sse_event("execution.started", 1)
            raise ConnectionError("live event dependency interrupted")

        return feed()


def test_redis_interruption_during_sse_does_not_fabricate_terminal_state() -> None:
    async def scenario() -> list[str]:
        stream = SSEEventStream(
            reader=_Reader(),
            live_source=_InterruptedLiveSource(),
            heartbeat_seconds=0.01,
        )
        chunks: list[str] = []
        with pytest.raises(ConnectionError):
            async for chunk in stream.stream(
                "session-sse",
                PRINCIPAL,
            ):
                chunks.append(chunk)
        return chunks

    chunks = asyncio.run(scenario())
    assert len(chunks) == 1
    assert "execution.started" in chunks[0]
    assert "execution.succeeded" not in "".join(chunks)
    assert "execution.failed" not in "".join(chunks)


@pytest.mark.skipif(
    os.getenv("RUNTIME_TASK13_LIVE") != "1",
    reason="live dependency chaos is opt-in; deterministic doubles are the default",
)
def test_live_dependency_probe_is_explicit_and_secret_free() -> None:
    required = (
        "RUNTIME_TEST_REDIS_URL",
        "RUNTIME_TEST_DATABASE_URL",
        "RUNTIME_TEST_MINIO_ENDPOINT",
        "RUNTIME_TEST_MINIO_ACCESS_KEY",
        "RUNTIME_TEST_MINIO_SECRET_KEY",
    )
    if any(os.getenv(name) is None for name in required):
        pytest.skip("live dependency chaos requires all configured service variables")

    redis_url = os.environ["RUNTIME_TEST_REDIS_URL"]
    database_url = os.environ["RUNTIME_TEST_DATABASE_URL"]
    minio_endpoint = os.environ["RUNTIME_TEST_MINIO_ENDPOINT"]
    access_key = os.environ["RUNTIME_TEST_MINIO_ACCESS_KEY"]
    secret_key = os.environ["RUNTIME_TEST_MINIO_SECRET_KEY"]

    async def probe_database_and_redis() -> None:
        redis = cast(Any, Redis.from_url(  # pyright: ignore[reportUnknownMemberType]
            redis_url,
            decode_responses=True,
        ))
        engine = create_async_engine(database_url)
        try:
            await redis.ping()
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        finally:
            await redis.aclose()
            await engine.dispose()

    try:
        asyncio.run(probe_database_and_redis())
        Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=os.getenv("RUNTIME_TEST_MINIO_SECURE", "1") == "1",
        ).list_buckets()
    except Exception:
        raise AssertionError("configured live dependency probe failed") from None
