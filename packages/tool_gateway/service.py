from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from threading import Lock
from time import perf_counter
from typing import Protocol
from uuid import uuid4

from langchain_core.tools import StructuredTool
from pydantic import ValidationError

from packages.runtime_contracts import Principal, RuntimeEvent
from packages.tool_gateway.contracts import (
    QUERY_METRIC_CONTRACT,
    MetricQuery,
    MetricResult,
    ToolEventContext,
    ToolGatewayError,
    ToolGatewayErrorCode,
)

ToolEventSink = Callable[[RuntimeEvent], None]
RequestIdFactory = Callable[[], str]
ToolEventFactory = Callable[[int], RuntimeEvent]


class SequencedToolEventSink(Protocol):
    """Atomically allocate the next sequence and persist the built event.

    An exception means the event was not committed and its sequence was not consumed.
    Task 9 can implement this boundary with one durable transaction.
    """

    def append(self, event_factory: ToolEventFactory, /) -> RuntimeEvent: ...


class LocalSequencedToolEventSink:
    """Process-local atomic sequence allocation and event append boundary."""

    def __init__(
        self,
        event_sink: ToolEventSink,
        *,
        sequence_start: int = 1,
    ) -> None:
        if sequence_start < 1:
            raise ValueError("tool event sequence must start at a positive integer")
        self._event_sink = event_sink
        self._lock = Lock()
        self._next_sequence = sequence_start

    def append(self, event_factory: ToolEventFactory, /) -> RuntimeEvent:
        with self._lock:
            event = event_factory(self._next_sequence)
            self._event_sink(event)
            self._next_sequence += 1
            return event


class MetricQueryClient(Protocol):
    def query(
        self,
        query: MetricQuery,
        *,
        principal: Principal,
        request_id: str,
    ) -> MetricResult: ...


class ToolGateway:
    def __init__(
        self,
        *,
        client: MetricQueryClient,
        principal: Principal,
        package_allowlist: Iterable[str],
        event_context: ToolEventContext,
        event_sink: SequencedToolEventSink,
        request_id_factory: RequestIdFactory | None = None,
    ) -> None:
        self._client = client
        self._principal = principal
        self._package_allowlist = frozenset(package_allowlist)
        self._event_context = event_context
        self._event_sink = event_sink
        self._request_id_factory = request_id_factory or (lambda: str(uuid4()))

    def query_metric(self, metric: str, period: str, org: str) -> MetricResult:
        request_id = self._request_id_factory()
        self._require_worker_identity(request_id)
        span_id = str(uuid4())
        started_at = perf_counter()
        common_payload = {
            "tool_name": QUERY_METRIC_CONTRACT.name,
            "request_id": request_id,
            "execution_mode": QUERY_METRIC_CONTRACT.execution_mode.value,
            "user_id": self._principal.user_id,
        }
        started_error = self._try_emit(
            event_type="tool.started",
            span_id=span_id,
            duration_ms=None,
            payload=common_payload,
        )
        if started_error is not None:
            raise started_error from None

        result: MetricResult | None = None
        operation_error: ToolGatewayError | None = None
        try:
            self._authorize_query_metric(request_id)
            try:
                query = MetricQuery(metric=metric, period=period, org=org)
            except ValidationError:
                raise ToolGatewayError(
                    ToolGatewayErrorCode.INVALID_ARGUMENT,
                    request_id=request_id,
                ) from None
            result = self._client.query(
                query,
                principal=self._principal,
                request_id=request_id,
            )
        except ToolGatewayError as error:
            operation_error = error
        except Exception:
            operation_error = ToolGatewayError(
                ToolGatewayErrorCode.UNAVAILABLE,
                request_id=request_id,
                retryable=True,
            )

        if operation_error is not None:
            terminal_error = self._try_emit(
                event_type="tool.failed",
                span_id=span_id,
                duration_ms=_elapsed_ms(started_at),
                payload={
                    **common_payload,
                    "error_code": operation_error.code.value,
                    "retryable": operation_error.retryable,
                },
            )
            if terminal_error is not None:
                self._recover_failed_terminal(
                    common_payload=common_payload,
                    span_id=span_id,
                    started_at=started_at,
                )
                raise terminal_error from None
            raise operation_error from None

        terminal_error = self._try_emit(
            event_type="tool.completed",
            span_id=span_id,
            duration_ms=_elapsed_ms(started_at),
            payload=common_payload,
        )
        if terminal_error is not None:
            self._recover_failed_terminal(
                common_payload=common_payload,
                span_id=span_id,
                started_at=started_at,
            )
            raise terminal_error from None
        if result is None:
            raise RuntimeError("query_metric client returned no result")
        return result

    def as_langchain_tool(self) -> StructuredTool:
        def query_metric(metric: str, period: str, org: str) -> MetricResult:
            """Query one authorized business metric for a bounded period and organization."""
            return self.query_metric(metric=metric, period=period, org=org)

        return StructuredTool.from_function(
            func=query_metric,
            name=QUERY_METRIC_CONTRACT.name,
            description=(
                "Query one read-only business metric for an allowed period and organization."
            ),
            args_schema=MetricQuery,
        )

    def _authorize_query_metric(self, request_id: str) -> None:
        context = self._event_context
        if (
            QUERY_METRIC_CONTRACT.name not in self._package_allowlist
            or QUERY_METRIC_CONTRACT.required_permission not in self._principal.permissions
            or context.package.tenant_id != self._principal.tenant_id
        ):
            raise ToolGatewayError(
                ToolGatewayErrorCode.PERMISSION_DENIED,
                request_id=request_id,
            )

    def _require_worker_identity(self, request_id: str) -> None:
        worker_id = self._principal.worker_id
        if worker_id is None or worker_id != self._event_context.worker_id:
            raise ToolGatewayError(
                ToolGatewayErrorCode.PERMISSION_DENIED,
                request_id=request_id,
            )

    def _emit(
        self,
        *,
        event_type: str,
        span_id: str,
        duration_ms: float | None,
        payload: Mapping[str, str | bool],
    ) -> None:
        context = self._event_context
        self._event_sink.append(
            lambda sequence: (
                RuntimeEvent(
                    schema_version="runtime.event.v1",
                    event_id=str(uuid4()),
                    sequence=sequence,
                    occurred_at=datetime.now(UTC),
                    tenant_id=self._principal.tenant_id,
                    trace_id=context.trace_id,
                    span_id=span_id,
                    parent_span_id=context.parent_span_id,
                    session_id=context.session_id,
                    execution_id=context.execution_id,
                    package=context.package,
                    worker_id=context.worker_id,
                    sdk_version=context.package.sdk_version,
                    event_type=event_type,
                    phase="invoke",
                    duration_ms=duration_ms,
                    payload=payload,
                    payload_ref=None,
                )
            )
        )

    def _try_emit(
        self,
        *,
        event_type: str,
        span_id: str,
        duration_ms: float | None,
        payload: Mapping[str, str | bool],
    ) -> ToolGatewayError | None:
        try:
            self._emit(
                event_type=event_type,
                span_id=span_id,
                duration_ms=duration_ms,
                payload=payload,
            )
        except Exception:
            return ToolGatewayError(
                ToolGatewayErrorCode.EVENT_PERSISTENCE_FAILED,
                request_id=str(payload["request_id"]),
                retryable=True,
            )
        return None

    def _recover_failed_terminal(
        self,
        *,
        common_payload: Mapping[str, str],
        span_id: str,
        started_at: float,
    ) -> None:
        self._try_emit(
            event_type="tool.failed",
            span_id=span_id,
            duration_ms=_elapsed_ms(started_at),
            payload={
                **common_payload,
                "error_code": ToolGatewayErrorCode.EVENT_PERSISTENCE_FAILED.value,
                "retryable": True,
            },
        )


def _elapsed_ms(started_at: float) -> float:
    return max(0.0, (perf_counter() - started_at) * 1000)


__all__ = [
    "LocalSequencedToolEventSink",
    "MetricQueryClient",
    "RequestIdFactory",
    "SequencedToolEventSink",
    "ToolEventFactory",
    "ToolEventSink",
    "ToolGateway",
]
