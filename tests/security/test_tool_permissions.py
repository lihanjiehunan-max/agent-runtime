from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Event

import httpx
import pytest
from langchain_core.tools import StructuredTool

from packages.runtime_contracts import AgentPackageRef, Principal, RuntimeEvent, RuntimeType
from packages.tool_gateway.contracts import (
    MetricResult,
    ToolEventContext,
    ToolGatewayError,
    ToolGatewayErrorCode,
)
from packages.tool_gateway.query_metric import QueryMetricClient, QueryMetricConfig
from packages.tool_gateway.service import LocalSequencedToolEventSink, ToolGateway


def _principal(
    *,
    worker_id: str | None = "worker-01",
    permissions: tuple[str, ...] = ("tool:query_metric",),
) -> Principal:
    return Principal(
        tenant_id="tenant-a",
        user_id="user-01",
        actor_id="actor-01",
        worker_id=worker_id,
        permissions=permissions,
    )


def _event_context() -> ToolEventContext:
    return ToolEventContext(
        trace_id="trace-tool-01",
        parent_span_id="span-parent-01",
        session_id="session-tool-01",
        execution_id="execution-tool-01",
        worker_id="worker-01",
        package=AgentPackageRef(
            tenant_id="tenant-a",
            agent_id="agent-metric-query",
            version="0.1.0",
            digest=f"sha256:{'a' * 64}",
            runtime_type=RuntimeType.DEEPAGENTS,
            sdk_version="0.7.7",
        ),
    )


def _event_sink(events: list[RuntimeEvent]) -> LocalSequencedToolEventSink:
    return LocalSequencedToolEventSink(events.append, sequence_start=41)


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> tuple[QueryMetricClient, httpx.Client]:
    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = QueryMetricClient(
        QueryMetricConfig(
            base_url="https://tool-gateway.internal",
            api_key="server-only-secret",
        ),
        http_client=http_client,
    )
    return client, http_client


def _result_response(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        request=request,
        json={
            "schema_version": "tool.metric-result.v1",
            "metric": "营业收入",
            "period": "本月",
            "org": "散运公司",
            "value": "1280000.00",
            "unit": "CNY",
        },
    )


def test_allowlisted_authorized_tool_emits_started_then_completed() -> None:
    events: list[RuntimeEvent] = []
    client, http_client = _client(_result_response)
    try:
        gateway = ToolGateway(
            client=client,
            principal=_principal(),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=_event_sink(events),
            request_id_factory=lambda: "request-tool-01",
        )

        result = gateway.query_metric(metric="营业收入", period="本月", org="散运公司")
    finally:
        http_client.close()

    assert result == MetricResult(
        metric="营业收入",
        period="本月",
        org="散运公司",
        value=Decimal("1280000.00"),
        unit="CNY",
    )
    assert [event.event_type for event in events] == ["tool.started", "tool.completed"]
    assert [event.sequence for event in events] == [41, 42]
    assert len({event.span_id for event in events}) == 1
    assert all(event.tenant_id == "tenant-a" for event in events)
    assert all(event.package == _event_context().package for event in events)
    assert all(event.session_id == "session-tool-01" for event in events)
    assert all(event.execution_id == "execution-tool-01" for event in events)
    assert all(event.worker_id == "worker-01" for event in events)
    assert events[0].model_dump(mode="json")["payload"] == {
        "tool_name": "query_metric",
        "request_id": "request-tool-01",
        "execution_mode": "READ_ONLY",
        "user_id": "user-01",
    }
    assert events[1].model_dump(mode="json")["payload"] == {
        "tool_name": "query_metric",
        "request_id": "request-tool-01",
        "execution_mode": "READ_ONLY",
        "user_id": "user-01",
    }
    assert events[0].duration_ms is None
    assert events[1].duration_ms is not None


@pytest.mark.parametrize(
    ("allowlist", "permissions"),
    [
        ((), ("tool:query_metric",)),
        (("query_metric",), ()),
        (("other_tool",), ("tool:query_metric",)),
        (("query_metric",), ("tool:other_tool",)),
    ],
)
def test_tool_call_fails_closed_when_package_or_principal_does_not_allow_it(
    allowlist: tuple[str, ...],
    permissions: tuple[str, ...],
) -> None:
    def must_not_call_gateway(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unauthorized request escaped to {request.url}")

    events: list[RuntimeEvent] = []
    client, http_client = _client(must_not_call_gateway)
    try:
        gateway = ToolGateway(
            client=client,
            principal=_principal(permissions=permissions),
            package_allowlist=allowlist,
            event_context=_event_context(),
            event_sink=_event_sink(events),
            request_id_factory=lambda: "request-denied",
        )

        with pytest.raises(ToolGatewayError) as captured:
            gateway.query_metric(metric="营业收入", period="本月", org="散运公司")
    finally:
        http_client.close()

    assert captured.value.code is ToolGatewayErrorCode.PERMISSION_DENIED
    assert captured.value.retryable is False
    assert [event.event_type for event in events] == ["tool.started", "tool.failed"]
    assert events[1].model_dump(mode="json")["payload"]["error_code"] == (
        "TOOL_PERMISSION_DENIED"
    )
    assert sum(event.event_type == "tool.completed" for event in events) == 0
    assert sum(event.event_type == "tool.failed" for event in events) == 1


@pytest.mark.parametrize("worker_id", [None, "worker-other"])
def test_worker_identity_is_rejected_before_events_or_http(worker_id: str | None) -> None:
    requests: list[httpx.Request] = []

    def gateway_response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _result_response(request)

    events: list[RuntimeEvent] = []
    client, http_client = _client(gateway_response)
    try:
        gateway = ToolGateway(
            client=client,
            principal=_principal(worker_id=worker_id),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=_event_sink(events),
            request_id_factory=lambda: "request-worker-denied",
        )

        with pytest.raises(ToolGatewayError) as captured:
            gateway.query_metric(metric="营业收入", period="本月", org="散运公司")
    finally:
        http_client.close()

    assert captured.value.code is ToolGatewayErrorCode.PERMISSION_DENIED
    assert events == []
    assert requests == []


@pytest.mark.parametrize(
    ("metric", "period", "org"),
    [
        ("   ", "本月", "散运公司"),
        ("营业收入", "2026-13", "散运公司"),
        ("营业收入", "本月", "   "),
    ],
)
def test_malformed_direct_call_emits_one_stable_failed_terminal(
    metric: str,
    period: str,
    org: str,
) -> None:
    requests: list[httpx.Request] = []

    def gateway_response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _result_response(request)

    events: list[RuntimeEvent] = []
    client, http_client = _client(gateway_response)
    try:
        gateway = ToolGateway(
            client=client,
            principal=_principal(),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=_event_sink(events),
            request_id_factory=lambda: "request-invalid",
        )

        with pytest.raises(ToolGatewayError) as captured:
            gateway.query_metric(metric=metric, period=period, org=org)
    finally:
        http_client.close()

    assert captured.value.code is ToolGatewayErrorCode.INVALID_ARGUMENT
    assert requests == []
    assert [event.event_type for event in events] == ["tool.started", "tool.failed"]
    assert events[1].model_dump(mode="json")["payload"]["error_code"] == (
        "TOOL_INVALID_ARGUMENT"
    )
    assert sum(event.event_type == "tool.failed" for event in events) == 1


def test_gateway_failure_emits_exactly_one_failed_terminal_event() -> None:
    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("downstream timed out", request=request)

    events: list[RuntimeEvent] = []
    client, http_client = _client(timeout)
    try:
        gateway = ToolGateway(
            client=client,
            principal=_principal(),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=_event_sink(events),
            request_id_factory=lambda: "request-timeout",
        )

        with pytest.raises(ToolGatewayError) as captured:
            gateway.query_metric(metric="营业收入", period="本月", org="散运公司")
    finally:
        http_client.close()

    assert captured.value.code is ToolGatewayErrorCode.TIMEOUT
    assert [event.event_type for event in events] == ["tool.started", "tool.failed"]
    assert events[1].model_dump(mode="json")["payload"] == {
        "tool_name": "query_metric",
        "request_id": "request-timeout",
        "execution_mode": "READ_ONLY",
        "user_id": "user-01",
        "error_code": "TOOL_TIMEOUT",
        "retryable": True,
    }
    assert sum(event.event_type == "tool.completed" for event in events) == 0
    assert sum(event.event_type == "tool.failed" for event in events) == 1


def test_langchain_tool_exposes_only_validated_query_arguments() -> None:
    events: list[RuntimeEvent] = []
    client, http_client = _client(_result_response)
    try:
        gateway = ToolGateway(
            client=client,
            principal=_principal(),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=_event_sink(events),
            request_id_factory=lambda: "request-langchain",
        )
        tool = gateway.as_langchain_tool()

        result = tool.invoke({"metric": "营业收入", "period": "本月", "org": "散运公司"})
    finally:
        http_client.close()

    assert isinstance(tool, StructuredTool)
    assert tool.name == "query_metric"
    assert tool.args == {
        "metric": {"maxLength": 128, "minLength": 1, "title": "Metric", "type": "string"},
        "period": {
            "pattern": (
                "^(?:本月|上月|本季度|上季度|本年|上年|"
                "[0-9]{4}(?:-(?:0[1-9]|1[0-2])|-Q[1-4])?)$"
            ),
            "title": "Period",
            "type": "string",
        },
        "org": {"maxLength": 128, "minLength": 1, "title": "Org", "type": "string"},
    }
    assert isinstance(result, MetricResult)
    assert [event.event_type for event in events] == ["tool.started", "tool.completed"]


def test_overlapping_tool_calls_emit_monotonic_event_sequences() -> None:
    first_started = Event()
    release_first = Event()

    def interleaved_gateway(request: httpx.Request) -> httpx.Response:
        query = json.loads(request.content)
        if query["metric"] == "营业收入":
            first_started.set()
            assert release_first.wait(timeout=2)
        return httpx.Response(
            200,
            request=request,
            json={
                "schema_version": "tool.metric-result.v1",
                **query,
                "value": "1280000.00",
                "unit": "CNY",
            },
        )

    events: list[RuntimeEvent] = []
    client, http_client = _client(interleaved_gateway)
    gateway = ToolGateway(
        client=client,
        principal=_principal(),
        package_allowlist=("query_metric",),
        event_context=_event_context(),
        event_sink=_event_sink(events),
    )
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(
                gateway.query_metric,
                metric="营业收入",
                period="本月",
                org="散运公司",
            )
            assert first_started.wait(timeout=2)
            second = executor.submit(
                gateway.query_metric,
                metric="利润",
                period="本月",
                org="散运公司",
            )
            assert second.result(timeout=2).metric == "利润"
            release_first.set()
            assert first.result(timeout=2).metric == "营业收入"
    finally:
        http_client.close()

    assert [event.sequence for event in events] == [41, 42, 43, 44]
    assert [event.event_type for event in events] == [
        "tool.started",
        "tool.started",
        "tool.completed",
        "tool.completed",
    ]


def test_two_gateway_instances_share_one_monotonic_event_sequence() -> None:
    events: list[RuntimeEvent] = []
    event_sink = _event_sink(events)
    client, http_client = _client(_result_response)
    try:
        first_gateway = ToolGateway(
            client=client,
            principal=_principal(),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=event_sink,
            request_id_factory=lambda: "request-instance-01",
        )
        second_gateway = ToolGateway(
            client=client,
            principal=_principal(),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=event_sink,
            request_id_factory=lambda: "request-instance-02",
        )

        first_gateway.query_metric(metric="营业收入", period="本月", org="散运公司")
        second_gateway.query_metric(metric="营业收入", period="本月", org="散运公司")
    finally:
        http_client.close()

    assert [event.sequence for event in events] == [41, 42, 43, 44]


def test_started_event_failure_is_stable_and_prevents_http() -> None:
    requests: list[httpx.Request] = []
    append_attempts: list[str] = []

    def gateway_response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _result_response(request)

    def failing_sink(event: RuntimeEvent) -> None:
        append_attempts.append(event.event_type)
        raise RuntimeError("sink-secret")

    client, http_client = _client(gateway_response)
    try:
        gateway = ToolGateway(
            client=client,
            principal=_principal(),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=LocalSequencedToolEventSink(failing_sink, sequence_start=41),
            request_id_factory=lambda: "request-started-sink-failure",
        )

        with pytest.raises(ToolGatewayError) as captured:
            gateway.query_metric(metric="营业收入", period="本月", org="散运公司")
    finally:
        http_client.close()

    assert captured.value.code is ToolGatewayErrorCode.EVENT_PERSISTENCE_FAILED
    assert captured.value.retryable is True
    assert "sink-secret" not in str(captured.value)
    assert requests == []
    assert append_attempts == ["tool.started"]


def test_completed_event_failure_recovers_one_failed_terminal_and_never_returns_result() -> None:
    persisted_events: list[RuntimeEvent] = []
    append_attempts: list[str] = []

    def transient_terminal_failure(event: RuntimeEvent) -> None:
        append_attempts.append(event.event_type)
        if event.event_type == "tool.completed":
            raise RuntimeError("transient append failure")
        persisted_events.append(event)

    client, http_client = _client(_result_response)
    try:
        gateway = ToolGateway(
            client=client,
            principal=_principal(),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=LocalSequencedToolEventSink(
                transient_terminal_failure,
                sequence_start=41,
            ),
            request_id_factory=lambda: "request-terminal-recovery",
        )

        with pytest.raises(ToolGatewayError) as captured:
            gateway.query_metric(metric="营业收入", period="本月", org="散运公司")
    finally:
        http_client.close()

    assert captured.value.code is ToolGatewayErrorCode.EVENT_PERSISTENCE_FAILED
    assert append_attempts == ["tool.started", "tool.completed", "tool.failed"]
    assert [event.event_type for event in persisted_events] == ["tool.started", "tool.failed"]
    assert [event.sequence for event in persisted_events] == [41, 42]
    assert persisted_events[1].model_dump(mode="json")["payload"]["error_code"] == (
        "TOOL_EVENT_PERSISTENCE_FAILED"
    )


def test_failed_terminal_recovery_is_attempted_only_once() -> None:
    persisted_events: list[RuntimeEvent] = []
    append_attempts: list[str] = []

    def permanent_terminal_failure(event: RuntimeEvent) -> None:
        append_attempts.append(event.event_type)
        if event.event_type != "tool.started":
            raise RuntimeError("persistent append failure")
        persisted_events.append(event)

    client, http_client = _client(_result_response)
    try:
        gateway = ToolGateway(
            client=client,
            principal=_principal(),
            package_allowlist=("query_metric",),
            event_context=_event_context(),
            event_sink=LocalSequencedToolEventSink(
                permanent_terminal_failure,
                sequence_start=41,
            ),
            request_id_factory=lambda: "request-terminal-failure",
        )

        with pytest.raises(ToolGatewayError) as captured:
            gateway.query_metric(metric="营业收入", period="本月", org="散运公司")
    finally:
        http_client.close()

    assert captured.value.code is ToolGatewayErrorCode.EVENT_PERSISTENCE_FAILED
    assert append_attempts == ["tool.started", "tool.completed", "tool.failed"]
    assert [event.event_type for event in persisted_events] == ["tool.started"]
