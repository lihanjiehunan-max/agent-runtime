from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
from pydantic import ValidationError

from packages.runtime_contracts import Principal
from packages.tool_gateway.contracts import (
    MAX_QUERY_TEXT_LENGTH,
    MAX_TOOL_RESULT_BYTES,
    QUERY_METRIC_CONTRACT,
    MetricQuery,
    MetricResult,
    ToolExecutionMode,
    ToolGatewayError,
    ToolGatewayErrorCode,
)
from packages.tool_gateway.query_metric import QueryMetricClient, QueryMetricConfig


def _principal() -> Principal:
    return Principal(
        tenant_id="tenant-a",
        user_id="user-01",
        actor_id="actor-01",
        worker_id="worker-01",
        permissions=("tool:query_metric",),
    )


def test_query_metric_contract_is_read_only_and_accepts_deterministic_case() -> None:
    query = MetricQuery(metric="营业收入", period="本月", org="散运公司")
    result = MetricResult(
        metric="营业收入",
        period="本月",
        org="散运公司",
        value=Decimal("1280000.00"),
        unit="CNY",
    )

    assert QUERY_METRIC_CONTRACT.name == "query_metric"
    assert QUERY_METRIC_CONTRACT.execution_mode is ToolExecutionMode.READ_ONLY
    assert {mode.value for mode in ToolExecutionMode} == {
        "READ_ONLY",
        "LOCAL_TRANSACTIONAL",
        "EXTERNAL_OUTBOX",
    }
    assert query.model_dump() == {
        "metric": "营业收入",
        "period": "本月",
        "org": "散运公司",
    }
    assert result.model_dump(mode="json") == {
        "schema_version": "tool.metric-result.v1",
        "metric": "营业收入",
        "period": "本月",
        "org": "散运公司",
        "value": "1280000.00",
        "unit": "CNY",
    }

    with pytest.raises(ValidationError):
        query.metric = "利润"


@pytest.mark.parametrize("field", ["metric", "period", "org"])
def test_query_metric_requires_nonempty_string_fields(field: str) -> None:
    values = {"metric": "营业收入", "period": "本月", "org": "散运公司"}
    values[field] = "   "

    with pytest.raises(ValidationError):
        MetricQuery.model_validate(values)


@pytest.mark.parametrize("field", ["metric", "org"])
def test_query_metric_bounds_free_text_fields(field: str) -> None:
    values = {"metric": "营业收入", "period": "本月", "org": "散运公司"}
    values[field] = "字" * (MAX_QUERY_TEXT_LENGTH + 1)

    with pytest.raises(ValidationError):
        MetricQuery.model_validate(values)


@pytest.mark.parametrize(
    "period",
    ["本月", "上月", "本季度", "上季度", "本年", "上年", "2026", "2026-08", "2026-Q3"],
)
def test_query_metric_accepts_only_documented_period_forms(period: str) -> None:
    assert MetricQuery(metric="营业收入", period=period, org="散运公司").period == period


@pytest.mark.parametrize("period", ["", "本月同比", "2026-13", "2026-Q5", "next month"])
def test_query_metric_rejects_unsupported_period_forms(period: str) -> None:
    with pytest.raises(ValidationError):
        MetricQuery(metric="营业收入", period=period, org="散运公司")


def test_gateway_propagates_identity_and_request_id_with_explicit_timeouts() -> None:
    requests: list[httpx.Request] = []

    def gateway(request: httpx.Request) -> httpx.Response:
        requests.append(request)
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

    config = QueryMetricConfig(
        base_url="https://tool-gateway.internal",
        api_key="gateway-secret",
        connect_timeout_seconds=1.25,
        read_timeout_seconds=4.5,
    )
    with httpx.Client(transport=httpx.MockTransport(gateway)) as http_client:
        client = QueryMetricClient(config, http_client=http_client)
        result = client.query(
            MetricQuery(metric="营业收入", period="本月", org="散运公司"),
            principal=_principal(),
            request_id="request-01",
        )

    assert result.value == Decimal("1280000.00")
    assert len(requests) == 1
    request = requests[0]
    assert str(request.url) == "https://tool-gateway.internal/v1/tools/query_metric"
    assert request.headers["authorization"] == "Bearer gateway-secret"
    assert request.headers["x-request-id"] == "request-01"
    assert request.headers["x-tenant-id"] == "tenant-a"
    assert request.headers["x-user-id"] == "user-01"
    assert request.extensions["timeout"] == {
        "connect": 1.25,
        "read": 4.5,
        "write": 4.5,
        "pool": 1.25,
    }
    assert request.content == (
        b'{"metric":"\xe8\x90\xa5\xe4\xb8\x9a\xe6\x94\xb6\xe5\x85\xa5",'
        b'"period":"\xe6\x9c\xac\xe6\x9c\x88",'
        b'"org":"\xe6\x95\xa3\xe8\xbf\x90\xe5\x85\xac\xe5\x8f\xb8"}'
    )
    assert b"gateway-secret" not in request.content
    assert b"tenant-a" not in request.content
    assert b"user-01" not in request.content


@pytest.mark.parametrize(
    "base_url",
    [
        "http://tool-gateway.internal",
        "http://127.0.0.1:8080",
        "https:///missing-host",
        "https://[",
    ],
)
def test_gateway_config_rejects_plaintext_or_hostless_urls(base_url: str) -> None:
    with pytest.raises(ValueError, match="HTTPS URL"):
        QueryMetricConfig(base_url=base_url, api_key="gateway-secret")


@pytest.mark.parametrize(
    "base_url",
    [
        "https://user:secret@tool-gateway.internal",
        "https://user@tool-gateway.internal",
        "https://tool-gateway.internal?api_key=secret",
        "https://tool-gateway.internal#access_token=secret",
    ],
)
def test_gateway_config_rejects_url_components_that_can_carry_credentials(
    base_url: str,
) -> None:
    with pytest.raises(ValueError, match="HTTPS URL"):
        QueryMetricConfig(base_url=base_url, api_key="gateway-secret")


def test_gateway_normalizes_timeout_without_leaking_credentials() -> None:
    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("gateway-secret timed out", request=request)

    config = QueryMetricConfig(
        base_url="https://tool-gateway.internal",
        api_key="gateway-secret",
    )
    with httpx.Client(transport=httpx.MockTransport(timeout)) as http_client:
        client = QueryMetricClient(config, http_client=http_client)
        with pytest.raises(ToolGatewayError) as captured:
            client.query(
                MetricQuery(metric="营业收入", period="本月", org="散运公司"),
                principal=_principal(),
                request_id="request-timeout",
            )

    assert captured.value.code is ToolGatewayErrorCode.TIMEOUT
    assert captured.value.request_id == "request-timeout"
    assert captured.value.retryable is True
    assert "gateway-secret" not in str(captured.value)


def test_gateway_rejects_a_result_larger_than_the_limit() -> None:
    def oversized(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            content=b'{' + b'"padding":"' + (b"x" * MAX_TOOL_RESULT_BYTES) + b'"}',
        )

    config = QueryMetricConfig(
        base_url="https://tool-gateway.internal",
        api_key="gateway-secret",
    )
    with httpx.Client(transport=httpx.MockTransport(oversized)) as http_client:
        client = QueryMetricClient(config, http_client=http_client)
        with pytest.raises(ToolGatewayError) as captured:
            client.query(
                MetricQuery(metric="营业收入", period="本月", org="散运公司"),
                principal=_principal(),
                request_id="request-large",
            )

    assert captured.value.code is ToolGatewayErrorCode.RESULT_TOO_LARGE
    assert captured.value.request_id == "request-large"


@pytest.mark.parametrize(
    ("response", "expected_code", "retryable"),
    [
        (httpx.Response(503), ToolGatewayErrorCode.UNAVAILABLE, True),
        (httpx.Response(200, content=b"not-json"), ToolGatewayErrorCode.INVALID_RESPONSE, False),
    ],
)
def test_gateway_normalizes_status_and_invalid_json(
    response: httpx.Response,
    expected_code: ToolGatewayErrorCode,
    retryable: bool,
) -> None:
    def gateway(request: httpx.Request) -> httpx.Response:
        response.request = request
        return response

    config = QueryMetricConfig(
        base_url="https://tool-gateway.internal",
        api_key="gateway-secret",
    )
    with httpx.Client(transport=httpx.MockTransport(gateway)) as http_client:
        client = QueryMetricClient(config, http_client=http_client)
        with pytest.raises(ToolGatewayError) as captured:
            client.query(
                MetricQuery(metric="营业收入", period="本月", org="散运公司"),
                principal=_principal(),
                request_id="request-error",
            )

    assert captured.value.code is expected_code
    assert captured.value.retryable is retryable
