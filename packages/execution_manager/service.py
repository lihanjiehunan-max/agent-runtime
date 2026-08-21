from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from uuid import uuid4

from pydantic import JsonValue

from packages.event_model.emitter import EventContext, RuntimeEventEmitter
from packages.event_model.projection import TraceIdentity
from packages.event_normalizer.deepagents_v3 import (
    DeepAgentsV3Normalizer,
    NormalizedEvent,
    sanitize_output,
)
from packages.execution_manager.cancellation import (
    CancellationRequested,
    CancellationToken,
)
from packages.execution_manager.state import (
    ExecutionState,
    ExecutionStateMachine,
)
from packages.runtime_contracts import (
    ErrorCode,
    ExecutionMode,
    ExecutionStatus,
    Principal,
    RuntimeEvent,
    RuntimeSession,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.repositories import RuntimeRepositoryError
from packages.session_manager.checkpointer import checkpoint_write_config
from packages.session_manager.locks import ExecutionFence
from packages.tool_gateway.contracts import ToolEventContext
from packages.tool_gateway.runtime_context import (
    RuntimeToolContext,
    bind_runtime_tool_context,
)

TERMINAL_EVENT_TYPES = frozenset(
    {
        "execution.succeeded",
        "execution.failed",
        "execution.timed_out",
        "execution.cancelled",
    }
)


class AgentGraphRun(Protocol):
    def __aiter__(self) -> AsyncIterator[Mapping[str, object]]: ...

    def output(self) -> Awaitable[object] | object: ...


class AgentGraph(Protocol):
    def astream_events(
        self,
        input: object,
        *,
        config: dict[str, dict[str, str | int]],
        version: str,
    ) -> AgentGraphRun | Awaitable[AgentGraphRun]: ...


class AgentFactoryProtocol(Protocol):
    def get(self, package_digest: str) -> AgentGraph: ...


class SessionManagerProtocol(Protocol):
    async def get_session(
        self,
        session_id: str,
        principal: Principal,
    ) -> RuntimeSession | None: ...


class ExecutionRepositoryProtocol(Protocol):
    async def complete_execution(
        self,
        session_id: str,
        execution_id: str,
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        *,
        completed_at: datetime | None = None,
        result_ref: str | None = None,
        error: RuntimeContractError | None = None,
    ) -> None: ...


class ExecutionCoordinatorProtocol(Protocol):
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
    ) -> ExecutionFence: ...


class DurableEventSink(Protocol):
    """The sequence allocation and event append boundary.

    Implementations must allocate the sequence in the same durable transaction as
    the append. The execution manager intentionally has no process-local fallback.
    """

    async def append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent: ...

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent: ...


class DurableEventRepository(Protocol):
    async def append_next(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent: ...

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent: ...


class RepositoryEventSink:
    """Production event sink that delegates sequence allocation to persistence."""

    def __init__(self, repository: DurableEventRepository) -> None:
        self._repository = repository

    async def append(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        return await self._repository.append_next(
            event_factory,
            principal,
            execution_epoch,
        )

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        return await self._repository.append_terminal(
            event_factory,
            principal,
            execution_epoch,
            status,
            error,
        )


class ExecutionManagerError(Exception):
    def __init__(self, error: RuntimeContractError) -> None:
        super().__init__(error.message)
        self.error = error


class _NormalizedRuntimeFailure(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class _NeverCancelled:
    async def cancel(self) -> bool:
        return False

    async def is_cancelled(self) -> bool:
        return False

    async def wait(self) -> None:
        await asyncio.Event().wait()

    async def check(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    execution_id: str
    trace_id: str
    execution_epoch: int
    status: ExecutionStatus
    output: object | None
    events: tuple[RuntimeEvent, ...]
    error: RuntimeContractError | None = None

    def to_body(self) -> dict[str, object]:
        return {
            "execution_id": self.execution_id,
            "trace_id": self.trace_id,
            "execution_epoch": self.execution_epoch,
            "status": self.status.value,
            "output": self.output,
            "error": self.error.model_dump(mode="json") if self.error is not None else None,
        }


_END = object()


class ExecutionStream(AsyncIterator[RuntimeEvent]):
    def __init__(
        self,
        manager: ExecutionManager,
        session_id: str,
        input_text: str,
        principal: Principal,
        mode: ExecutionMode,
    ) -> None:
        self._manager = manager
        self._session_id = session_id
        self._input_text = input_text
        self._principal = principal
        self._mode = mode
        self._queue: asyncio.Queue[RuntimeEvent | object] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self.result: ExecutionResult | None = None
        self.error: BaseException | None = None

    def __aiter__(self) -> ExecutionStream:
        return self

    async def __anext__(self) -> RuntimeEvent:
        if self._task is None:
            self._task = asyncio.create_task(self._run())
        item = await self._queue.get()
        if item is _END:
            if self.error is not None:
                raise self.error
            raise StopAsyncIteration
        return cast(RuntimeEvent, item)

    async def _run(self) -> None:
        async def emit(event: RuntimeEvent) -> None:
            await self._queue.put(event)

        try:
            self.result = await self._manager.run_turn(
                self._session_id,
                self._input_text,
                self._principal,
                self._mode,
                emit,
            )
        except BaseException as error:
            self.error = error
        finally:
            await self._queue.put(_END)


class ExecutionManager:
    def __init__(
        self,
        *,
        agent_factory: AgentFactoryProtocol,
        session_manager: SessionManagerProtocol,
        execution_repository: ExecutionRepositoryProtocol,
        execution_coordinator: ExecutionCoordinatorProtocol,
        event_sink: DurableEventSink,
        event_emitter: RuntimeEventEmitter | None = None,
        normalizer: DeepAgentsV3Normalizer | None = None,
        lease_renew_interval_seconds: float = 10.0,
        execution_timeout_seconds: float | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        if lease_renew_interval_seconds < 0:
            raise ValueError("lease renewal interval cannot be negative")
        if execution_timeout_seconds is not None and execution_timeout_seconds <= 0:
            raise ValueError("execution timeout must be positive")
        self._agent_factory = agent_factory
        self._session_manager = session_manager
        self._execution_repository = execution_repository
        self._execution_coordinator = execution_coordinator
        self._event_sink = event_sink
        self._event_emitter = event_emitter
        self._normalizer = normalizer or DeepAgentsV3Normalizer()
        self._lease_renew_interval_seconds = lease_renew_interval_seconds
        self._execution_timeout_seconds = execution_timeout_seconds
        self._id_factory = id_factory or (lambda: str(uuid4()))

    async def execute_turn(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        *,
        mode: ExecutionMode = ExecutionMode.SYNC,
    ) -> ExecutionResult:
        stream = self.execute_turn_stream(
            session_id,
            input_text,
            principal,
            mode=mode,
        )
        _events = [event async for event in stream]
        del _events
        if stream.result is None:
            if stream.error is not None:
                raise stream.error
            raise RuntimeError("execution stream ended without a result")
        return stream.result

    async def execute_async(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        *,
        execution_id: str | None = None,
        trace_id: str | None = None,
        execution_epoch: int = 0,
        cancellation_token: CancellationToken | None = None,
        timeout_seconds: float | None = None,
    ) -> ExecutionResult:
        async def emit(_event: RuntimeEvent) -> None:
            return None

        return await self.run_turn(
            session_id,
            input_text,
            principal,
            ExecutionMode.ASYNC,
            emit,
            allow_async=True,
            execution_id=execution_id,
            trace_id=trace_id,
            expected_execution_epoch=execution_epoch,
            cancellation_token=cancellation_token,
            timeout_seconds=timeout_seconds,
        )

    def execute_turn_stream(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        *,
        mode: ExecutionMode = ExecutionMode.STREAM,
    ) -> ExecutionStream:
        self._ensure_supported_mode(mode)
        return ExecutionStream(self, session_id, input_text, principal, mode)

    async def run_turn(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        mode: ExecutionMode,
        emit: Callable[[RuntimeEvent], Awaitable[None]],
        *,
        allow_async: bool = False,
        execution_id: str | None = None,
        trace_id: str | None = None,
        expected_execution_epoch: int = 0,
        cancellation_token: CancellationToken | None = None,
        timeout_seconds: float | None = None,
    ) -> ExecutionResult:
        self._ensure_supported_mode(mode, allow_async=allow_async)
        worker_id = principal.worker_id
        if worker_id is None:
            raise ExecutionManagerError(
                RuntimeContractError(
                    code=ErrorCode.TENANT_IDENTITY_MISMATCH,
                    message="execution requires a verified worker identity",
                )
            )
        execution_id = execution_id or self._id_factory()
        trace_id = trace_id or self._id_factory()
        token = cancellation_token or _NeverCancelled()
        state = ExecutionStateMachine()
        events: list[RuntimeEvent] = []
        fence: ExecutionFence | None = None
        renew_task: asyncio.Task[None] | None = None
        renew_failures: list[RuntimeRepositoryError] = []
        renew_guard = asyncio.Lock()
        terminal_written = False
        runtime_session: RuntimeSession | None = None
        output: object | None = None
        graph: AgentGraph | None = None
        cancelled_before_claim = False

        try:
            runtime_session = await self._session_manager.get_session(session_id, principal)
            if runtime_session is None:
                raise ExecutionManagerError(
                    RuntimeContractError(
                        code=ErrorCode.SESSION_CLOSED,
                        message="session is closed or unavailable",
                    )
                )
            state.transition(ExecutionState.LOADING_AGENT)
            if mode is ExecutionMode.ASYNC:
                cancelled_before_claim = await token.is_cancelled()
            if not cancelled_before_claim:
                graph = self._agent_factory.get(runtime_session.package.digest)
            state.transition(ExecutionState.ACQUIRING_SESSION_LOCK)
            fence = await self._execution_coordinator.begin_execution(
                self._execution_repository,
                session_id,
                execution_id,
                principal,
                mode=mode,
                trace_id=trace_id,
                request_input=input_text,
            )
            self._validate_fence(fence, session_id, execution_id, principal)
            if (
                expected_execution_epoch > 0
                and fence.execution_epoch != expected_execution_epoch
            ):
                raise RuntimeRepositoryError(
                    RuntimeContractError(
                        code=ErrorCode.EXECUTION_FENCED,
                        message="queued execution epoch does not match the authoritative fence",
                    )
                )
            active_session = await self._session_manager.get_session(session_id, principal)
            if active_session is None:
                raise ExecutionManagerError(
                    RuntimeContractError(
                        code=ErrorCode.EXECUTION_FENCED,
                        message="session disappeared after execution fencing",
                    )
                )
            runtime_session = active_session
            await self._renew_with_guard(fence, renew_guard, renew_failures)
            await self._persist_named(
                "execution.accepted",
                "accepted",
                {"mode": mode.value},
                trace_id,
                runtime_session,
                fence,
                principal,
                events,
                emit,
                lease_guard=renew_guard,
                renew_failures=renew_failures,
            )
            state.transition(ExecutionState.RUNNING)
            await self._persist_named(
                "execution.started",
                "started",
                {"execution_epoch": fence.execution_epoch},
                trace_id,
                runtime_session,
                fence,
                principal,
                events,
                emit,
                lease_guard=renew_guard,
                renew_failures=renew_failures,
            )
            if self._lease_renew_interval_seconds > 0:
                renew_task = asyncio.create_task(
                    self._renew_loop(fence, renew_failures, renew_guard)
                )
            if cancelled_before_claim:
                raise CancellationRequested("execution was cancelled before worker claim")
            await token.check()
            if graph is None:
                graph = self._agent_factory.get(runtime_session.package.digest)
            config = checkpoint_write_config(
                runtime_session,
                principal,
                execution_id=fence.execution_id,
                execution_epoch=fence.execution_epoch,
            )
            try:
                async def consume_graph() -> object | None:
                    nonlocal output
                    event_context = ToolEventContext(
                        trace_id=trace_id,
                        parent_span_id=None,
                        session_id=runtime_session.session_id,
                        execution_id=fence.execution_id,
                        worker_id=worker_id,
                        package=runtime_session.package,
                    )
                    tool_context = RuntimeToolContext(
                        principal,
                        event_context,
                        [],
                    )
                    flushed_audits = 0

                    async def flush_tool_audits() -> None:
                        nonlocal flushed_audits
                        while flushed_audits < len(tool_context.audit_events):
                            audit = tool_context.audit_events[flushed_audits]
                            flushed_audits += 1
                            normalized_audit = NormalizedEvent(
                                event_type=audit.event_type.replace(
                                    "tool.",
                                    "tool.gateway.",
                                    1,
                                ),
                                phase=audit.phase,
                                payload=audit.payload,
                                span_id=audit.span_id,
                                parent_span_id=audit.parent_span_id,
                            )
                            await self._persist_normalized(
                                normalized_audit,
                                trace_id,
                                runtime_session,
                                fence,
                                principal,
                                events,
                                emit,
                                lease_guard=renew_guard,
                                renew_failures=renew_failures,
                            )

                    with bind_runtime_tool_context(tool_context):
                        try:
                            await token.check()
                            active_graph = graph
                            run = active_graph.astream_events(
                                {"messages": [{"role": "user", "content": input_text}]},
                                config=config,
                                version="v3",
                            )
                            if inspect.isawaitable(run):
                                run = await run
                            await token.check()
                            async for raw_event in run:
                                await token.check()
                                self._raise_renewal_failure(renew_failures)
                                for normalized in self._normalizer.normalize(raw_event):
                                    await token.check()
                                    await self._persist_normalized(
                                        normalized,
                                        trace_id,
                                        runtime_session,
                                        fence,
                                        principal,
                                        events,
                                        emit,
                                        lease_guard=renew_guard,
                                        renew_failures=renew_failures,
                                    )
                                    if normalized.event_type in {
                                        "execution.output",
                                        "execution.values",
                                    }:
                                        output = dict(normalized.payload)
                                    if normalized.event_type == "runtime.error":
                                        raise _NormalizedRuntimeFailure(
                                            str(normalized.payload.get("code", "RUNTIME_ERROR"))
                                        )
                                    await flush_tool_audits()
                                    await token.check()
                            await flush_tool_audits()
                            await token.check()
                            output_value = getattr(run, "output", None)
                            if callable(output_value):
                                output_value = output_value()
                            await token.check()
                            if inspect.isawaitable(output_value):
                                output = sanitize_output(await output_value)
                            elif output_value is not None:
                                output = sanitize_output(output_value)
                            await token.check()
                            return output
                        finally:
                            await flush_tool_audits()

                execution_timeout = (
                    self._resolve_execution_timeout(
                        runtime_session.package.digest,
                        timeout_seconds,
                    )
                    if mode is ExecutionMode.ASYNC
                    else None
                )
                execution = consume_graph()
                if mode is ExecutionMode.ASYNC:
                    assert execution_timeout is not None
                    async with asyncio.timeout(execution_timeout):
                        output = await self._run_with_cancellation(execution, token)
                else:
                    output = await execution
            except RuntimeRepositoryError:
                raise
            except CancellationRequested:
                raise
            except TimeoutError:
                raise
            except _NormalizedRuntimeFailure:
                raise
            except BaseException as error:
                normalized = self._normalizer.normalize_exception(error)
                await self._persist_normalized(
                    normalized,
                    trace_id,
                    runtime_session,
                    fence,
                    principal,
                    events,
                    emit,
                    lease_guard=renew_guard,
                    renew_failures=renew_failures,
                )
                raise _NormalizedRuntimeFailure("DEEPAGENTS_RUNTIME_ERROR") from None

            await token.check()
            self._raise_renewal_failure(renew_failures)
            state.transition(ExecutionState.SUCCEEDED)
            await self._persist_terminal(
                state,
                "execution.succeeded",
                ExecutionStatus.SUCCEEDED,
                {"status": ExecutionStatus.SUCCEEDED.value},
                trace_id,
                runtime_session,
                fence,
                principal,
                events,
                emit,
                lease_guard=renew_guard,
                renew_failures=renew_failures,
            )
            terminal_written = True
            return ExecutionResult(
                execution_id=execution_id,
                trace_id=trace_id,
                execution_epoch=fence.execution_epoch,
                status=ExecutionStatus.SUCCEEDED,
                output=output,
                events=tuple(events),
            )
        except CancellationRequested:
            if fence is None or runtime_session is None:
                raise
            return await self._finish_failure(
                state,
                fence,
                runtime_session,
                principal,
                execution_id,
                trace_id,
                events,
                emit,
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_CANCELLED,
                    message="execution was cooperatively cancelled",
                ),
                terminal_written,
                renew_guard,
                renew_failures,
                status=ExecutionStatus.CANCELLED,
                output=output,
            )
        except TimeoutError:
            if fence is None or runtime_session is None:
                raise
            return await self._finish_failure(
                state,
                fence,
                runtime_session,
                principal,
                execution_id,
                trace_id,
                events,
                emit,
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_TIMED_OUT,
                    message="execution exceeded its package timeout",
                ),
                terminal_written,
                renew_guard,
                renew_failures,
                status=ExecutionStatus.TIMED_OUT,
                output=output,
            )
        except RuntimeRepositoryError as error:
            if error.error.code is ErrorCode.EXECUTION_FENCED:
                raise
            if fence is None or runtime_session is None:
                raise ExecutionManagerError(error.error) from None
            return await self._finish_failure(
                state,
                fence,
                runtime_session,
                principal,
                execution_id,
                trace_id,
                events,
                emit,
                error.error,
                terminal_written,
                renew_guard,
                renew_failures,
            )
        except BaseException as error:
            if fence is None or runtime_session is None:
                raise
            failure = self._as_runtime_error(error)
            return await self._finish_failure(
                state,
                fence,
                runtime_session,
                principal,
                execution_id,
                trace_id,
                events,
                emit,
                failure,
                terminal_written,
                renew_guard,
                renew_failures,
            )
        finally:
            if renew_task is not None:
                renew_task.cancel()
                with suppress(asyncio.CancelledError):
                    await renew_task
            if fence is not None:
                with suppress(BaseException):
                    await fence.lease.release()

    @staticmethod
    def _ensure_supported_mode(mode: ExecutionMode, *, allow_async: bool = False) -> None:
        if mode is ExecutionMode.ASYNC and not allow_async:
            raise ExecutionManagerError(
                RuntimeContractError(
                    code=ErrorCode.RUNTIME_INCOMPATIBLE,
                    message="async execution mode is reserved for Task 10",
                )
            )

    async def _finish_failure(
        self,
        state: ExecutionStateMachine,
        fence: ExecutionFence,
        runtime_session: RuntimeSession,
        principal: Principal,
        execution_id: str,
        trace_id: str,
        events: list[RuntimeEvent],
        emit: Callable[[RuntimeEvent], Awaitable[None]],
        error: RuntimeContractError,
        terminal_written: bool,
        renew_guard: asyncio.Lock,
        renew_failures: list[RuntimeRepositoryError],
        *,
        status: ExecutionStatus = ExecutionStatus.FAILED,
        output: object | None = None,
    ) -> ExecutionResult:
        if not terminal_written:
            if not state.terminal:
                state.transition(ExecutionState.from_status(status))
            event_type = {
                ExecutionStatus.FAILED: "execution.failed",
                ExecutionStatus.TIMED_OUT: "execution.timed_out",
                ExecutionStatus.CANCELLED: "execution.cancelled",
            }[status]
            payload: dict[str, JsonValue] = {
                "status": status.value,
                "error_code": error.code.value,
            }
            if output is not None:
                payload["partial_output"] = sanitize_output(output)
            await self._persist_terminal(
                state,
                event_type,
                status,
                payload,
                trace_id,
                runtime_session,
                fence,
                principal,
                events,
                emit,
                error=error,
                lease_guard=renew_guard,
                renew_failures=renew_failures,
            )
            terminal_written = True
        return ExecutionResult(
            execution_id=execution_id,
            trace_id=trace_id,
            execution_epoch=fence.execution_epoch,
            status=status,
            output=output,
            events=tuple(events),
            error=error,
        )

    async def _run_with_cancellation(
        self,
        operation: Coroutine[Any, Any, object | None],
        token: CancellationToken,
    ) -> object | None:
        operation_task: asyncio.Task[object | None] = asyncio.create_task(operation)
        cancellation_task: asyncio.Task[None] = asyncio.create_task(token.wait())
        tasks: set[asyncio.Task[object | None] | asyncio.Task[None]] = {
            operation_task,
            cancellation_task,
        }
        try:
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if operation_task in done:
                return await operation_task
            operation_task.cancel()
            with suppress(BaseException):
                await operation_task
            raise CancellationRequested("execution was cooperatively cancelled")
        except asyncio.CancelledError:
            operation_task.cancel()
            with suppress(BaseException):
                await operation_task
            raise
        finally:
            cancellation_task.cancel()
            with suppress(BaseException):
                await cancellation_task

    def _resolve_execution_timeout(
        self,
        package_digest: str,
        requested_timeout_seconds: float | None,
    ) -> float:
        package_timeout: float | None = None
        provider = getattr(self._agent_factory, "get_limits", None)
        if callable(provider):
            limits = provider(package_digest)
            raw_timeout = getattr(limits, "execution_timeout_seconds", None)
            if isinstance(raw_timeout, (float, int)) and raw_timeout > 0:
                package_timeout = float(raw_timeout)
        if package_timeout is None:
            package_timeout = self._execution_timeout_seconds
        if package_timeout is None:
            package_timeout = requested_timeout_seconds
        if package_timeout is None or package_timeout <= 0:
            raise ExecutionManagerError(
                RuntimeContractError(
                    code=ErrorCode.RUNTIME_INCOMPATIBLE,
                    message="package execution timeout is not configured",
                )
            )
        if requested_timeout_seconds is None:
            return package_timeout
        if requested_timeout_seconds <= 0:
            raise ValueError("execution timeout must be positive")
        return min(package_timeout, requested_timeout_seconds)

    async def _persist_terminal(
        self,
        state: ExecutionStateMachine,
        event_type: str,
        status: ExecutionStatus,
        payload: Mapping[str, JsonValue],
        trace_id: str,
        runtime_session: RuntimeSession,
        fence: ExecutionFence,
        principal: Principal,
        events: list[RuntimeEvent],
        emit: Callable[[RuntimeEvent], Awaitable[None]],
        *,
        error: RuntimeContractError | None = None,
        lease_guard: asyncio.Lock,
        renew_failures: list[RuntimeRepositoryError],
    ) -> RuntimeEvent:
        event = await self._persist_named(
            event_type,
            "terminal",
            payload,
            trace_id,
            runtime_session,
            fence,
            principal,
            events,
            emit,
            terminal_status=status,
            terminal_error=error,
            lease_guard=lease_guard,
            renew_failures=renew_failures,
        )
        state.mark_terminal_event()
        return event

    async def _persist_normalized(
        self,
        normalized: NormalizedEvent,
        trace_id: str,
        runtime_session: RuntimeSession,
        fence: ExecutionFence,
        principal: Principal,
        events: list[RuntimeEvent],
        emit: Callable[[RuntimeEvent], Awaitable[None]],
        *,
        lease_guard: asyncio.Lock,
        renew_failures: list[RuntimeRepositoryError],
    ) -> RuntimeEvent:
        return await self._persist_named(
            normalized.event_type,
            normalized.phase,
            normalized.payload,
            trace_id,
            runtime_session,
            fence,
            principal,
            events,
            emit,
            span_id=normalized.span_id,
            parent_span_id=normalized.parent_span_id,
            lease_guard=lease_guard,
            renew_failures=renew_failures,
        )

    async def _persist_named(
        self,
        event_type: str,
        phase: str,
        payload: Mapping[str, JsonValue],
        trace_id: str,
        runtime_session: RuntimeSession,
        fence: ExecutionFence,
        principal: Principal,
        events: list[RuntimeEvent],
        emit: Callable[[RuntimeEvent], Awaitable[None]],
        *,
        span_id: str | None = None,
        parent_span_id: str | None = None,
        terminal_status: ExecutionStatus | None = None,
        terminal_error: RuntimeContractError | None = None,
        lease_guard: asyncio.Lock,
        renew_failures: list[RuntimeRepositoryError],
    ) -> RuntimeEvent:
        event_payload = cast(Mapping[str, JsonValue], dict(payload))
        event_span_id = _valid_identifier(span_id) or f"span-{self._id_factory()}"
        event_parent_span_id = _valid_identifier(parent_span_id)
        event_context = EventContext(
            identity=TraceIdentity(
                tenant_id=principal.tenant_id,
                trace_id=trace_id,
                session_id=fence.session_id,
                execution_id=fence.execution_id,
                package=runtime_session.package,
            ),
            worker_id=fence.worker_id,
            sdk_version=runtime_session.package.sdk_version,
            parent_span_id=event_parent_span_id,
        )
        normalized_event = NormalizedEvent(
            event_type=event_type,
            phase=phase,
            payload=event_payload,
            span_id=event_span_id,
            parent_span_id=event_parent_span_id,
        )

        def build(sequence: int) -> RuntimeEvent:
            return RuntimeEvent(
                schema_version="runtime.event.v1",
                event_id=f"event-{self._id_factory()}",
                sequence=sequence,
                occurred_at=datetime.now(UTC),
                tenant_id=principal.tenant_id,
                trace_id=trace_id,
                span_id=event_span_id,
                parent_span_id=event_parent_span_id,
                session_id=fence.session_id,
                execution_id=fence.execution_id,
                package=runtime_session.package,
                worker_id=fence.worker_id,
                sdk_version=runtime_session.package.sdk_version,
                event_type=event_type,
                phase=phase,
                duration_ms=None,
                payload=event_payload,
                payload_ref=None,
            )

        if terminal_status is None:
            await self._renew_with_guard(fence, lease_guard, renew_failures)
            if self._event_emitter is not None:
                event = await self._event_emitter.emit(
                    normalized_event,
                    event_context,
                    principal,
                    execution_epoch=fence.execution_epoch,
                )
            else:
                event = await self._event_sink.append(
                    build,
                    principal,
                    fence.execution_epoch,
                )
        else:
            async with lease_guard:
                prior_failure = renew_failures[0] if renew_failures else None
                try:
                    await self._renew_or_fence(fence)
                except RuntimeRepositoryError as error:
                    renew_failures.append(error)
                    raise
                if prior_failure is not None:
                    raise prior_failure
                self._raise_renewal_failure(renew_failures)
                if self._event_emitter is not None:
                    event = await self._event_emitter.emit(
                        normalized_event,
                        event_context,
                        principal,
                        execution_epoch=fence.execution_epoch,
                        terminal_status=terminal_status,
                        error=terminal_error,
                    )
                else:
                    event = await self._event_sink.append_terminal(
                        build,
                        principal,
                        fence.execution_epoch,
                        terminal_status,
                        terminal_error,
                    )
        events.append(event)
        await emit(event)
        return event

    async def _renew_with_guard(
        self,
        fence: ExecutionFence,
        lease_guard: asyncio.Lock,
        failures: list[RuntimeRepositoryError],
    ) -> None:
        async with lease_guard:
            self._raise_renewal_failure(failures)
            try:
                await self._renew_or_fence(fence)
            except RuntimeRepositoryError as error:
                failures.append(error)
                raise

    async def _renew_or_fence(self, fence: ExecutionFence) -> None:
        try:
            renewed = await fence.lease.renew()
        except Exception as error:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="execution lease renewal failed",
                )
            ) from error
        if not renewed:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="execution lease is no longer owned",
                )
            )

    async def _renew_loop(
        self,
        fence: ExecutionFence,
        failures: list[RuntimeRepositoryError],
        lease_guard: asyncio.Lock | None = None,
    ) -> None:
        guard = lease_guard or asyncio.Lock()
        try:
            while True:
                await asyncio.sleep(self._lease_renew_interval_seconds)
                async with guard:
                    try:
                        await self._renew_or_fence(fence)
                    except RuntimeRepositoryError as error:
                        failures.append(error)
                        return
        except asyncio.CancelledError:
            raise

    @staticmethod
    def _raise_renewal_failure(failures: list[RuntimeRepositoryError]) -> None:
        if failures:
            raise failures[0]

    @staticmethod
    def _validate_fence(
        fence: ExecutionFence,
        session_id: str,
        execution_id: str,
        principal: Principal,
    ) -> None:
        if (
            fence.tenant_id != principal.tenant_id
            or fence.session_id != session_id
            or fence.execution_id != execution_id
            or fence.worker_id != principal.worker_id
        ):
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="execution fence identity does not match the verified principal",
                )
            )

    @staticmethod
    def _as_runtime_error(error: BaseException) -> RuntimeContractError:
        if isinstance(error, _NormalizedRuntimeFailure):
            return RuntimeContractError(
                code=ErrorCode.MODEL_ERROR,
                message="Deep Agents runtime execution failed",
                details={"runtime_code": error.code},
            )
        return RuntimeContractError(
            code=ErrorCode.MODEL_ERROR,
            message="Deep Agents runtime execution failed",
            details={"exception_type": type(error).__name__},
        )


def _valid_identifier(value: str | None) -> str | None:
    if value is None:
        return None
    if len(value) < 1 or len(value) > 254:
        return None
    if not value[0].isalnum() or any(not (char.isalnum() or char in "_-") for char in value):
        return None
    return value


__all__ = [
    "DurableEventRepository",
    "DurableEventSink",
    "ExecutionManager",
    "ExecutionManagerError",
    "ExecutionResult",
    "ExecutionStream",
    "RepositoryEventSink",
    "TERMINAL_EVENT_TYPES",
]
