from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, StringConstraints

from packages.runtime_contracts import AgentPackageRef
from packages.runtime_contracts.identity import FrozenContract, Identifier, Permission

MAX_QUERY_TEXT_LENGTH = 128
MAX_TOOL_REQUEST_BYTES = 4 * 1024
MAX_TOOL_RESULT_BYTES = 64 * 1024

QueryText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_QUERY_TEXT_LENGTH),
]
Period = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        pattern=(
            r"^(?:本月|上月|本季度|上季度|本年|上年|"
            r"[0-9]{4}(?:-(?:0[1-9]|1[0-2])|-Q[1-4])?)$"
        ),
    ),
]
Unit = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=32)]
MetricValue = Annotated[Decimal, Field(allow_inf_nan=False, max_digits=30, decimal_places=6)]


class ToolExecutionMode(StrEnum):
    READ_ONLY = "READ_ONLY"
    LOCAL_TRANSACTIONAL = "LOCAL_TRANSACTIONAL"
    EXTERNAL_OUTBOX = "EXTERNAL_OUTBOX"


class ToolGatewayErrorCode(StrEnum):
    PERMISSION_DENIED = "TOOL_PERMISSION_DENIED"
    INVALID_ARGUMENT = "TOOL_INVALID_ARGUMENT"
    TIMEOUT = "TOOL_TIMEOUT"
    UNAVAILABLE = "TOOL_GATEWAY_UNAVAILABLE"
    REJECTED = "TOOL_GATEWAY_REJECTED"
    INVALID_RESPONSE = "TOOL_INVALID_RESPONSE"
    RESULT_TOO_LARGE = "TOOL_RESULT_TOO_LARGE"
    EVENT_PERSISTENCE_FAILED = "TOOL_EVENT_PERSISTENCE_FAILED"


class ToolGatewayError(Exception):
    def __init__(
        self,
        code: ToolGatewayErrorCode,
        *,
        request_id: str,
        retryable: bool = False,
        status_code: int | None = None,
    ) -> None:
        super().__init__(f"tool gateway request failed ({code.value})")
        self.code = code
        self.request_id = request_id
        self.retryable = retryable
        self.status_code = status_code


class ToolContract(FrozenContract):
    name: Literal["query_metric"]
    execution_mode: ToolExecutionMode
    required_permission: Permission


class MetricQuery(FrozenContract):
    metric: QueryText
    period: Period
    org: QueryText


class MetricResult(FrozenContract):
    schema_version: Literal["tool.metric-result.v1"] = "tool.metric-result.v1"
    metric: QueryText
    period: Period
    org: QueryText
    value: MetricValue
    unit: Unit


class ToolEventContext(FrozenContract):
    trace_id: Identifier
    parent_span_id: Identifier | None
    session_id: Identifier
    execution_id: Identifier
    worker_id: Identifier
    package: AgentPackageRef


QUERY_METRIC_CONTRACT = ToolContract(
    name="query_metric",
    execution_mode=ToolExecutionMode.READ_ONLY,
    required_permission="tool:query_metric",
)


__all__ = [
    "MAX_QUERY_TEXT_LENGTH",
    "MAX_TOOL_REQUEST_BYTES",
    "MAX_TOOL_RESULT_BYTES",
    "MetricQuery",
    "MetricResult",
    "QUERY_METRIC_CONTRACT",
    "ToolContract",
    "ToolEventContext",
    "ToolExecutionMode",
    "ToolGatewayError",
    "ToolGatewayErrorCode",
]
