from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import cast
from urllib.parse import urlsplit

import httpx
from pydantic import ValidationError

from packages.runtime_contracts import Principal
from packages.tool_gateway.contracts import (
    MAX_TOOL_REQUEST_BYTES,
    MAX_TOOL_RESULT_BYTES,
    MetricQuery,
    MetricResult,
    ToolGatewayError,
    ToolGatewayErrorCode,
)


@dataclass(frozen=True, slots=True)
class QueryMetricConfig:
    base_url: str
    api_key: str = field(repr=False)
    connect_timeout_seconds: float = 2.0
    read_timeout_seconds: float = 10.0
    max_result_bytes: int = MAX_TOOL_RESULT_BYTES

    def __post_init__(self) -> None:
        if not self.base_url or not self.api_key:
            raise ValueError("tool gateway base URL and API key are required")
        is_secure_url = False
        try:
            parsed_url = urlsplit(self.base_url)
            is_secure_url = (
                parsed_url.scheme.lower() == "https" and parsed_url.hostname is not None
                and parsed_url.username is None
                and parsed_url.password is None
                and not parsed_url.query
                and not parsed_url.fragment
            )
        except ValueError:
            pass
        if not is_secure_url:
            raise ValueError("tool gateway base URL must be an HTTPS URL with a host")
        if self.connect_timeout_seconds <= 0 or self.read_timeout_seconds <= 0:
            raise ValueError("tool gateway timeouts must be positive")
        if not 1 <= self.max_result_bytes <= MAX_TOOL_RESULT_BYTES:
            raise ValueError(
                f"tool gateway result limit must be between 1 and {MAX_TOOL_RESULT_BYTES} bytes"
            )


class QueryMetricClient:
    def __init__(
        self,
        config: QueryMetricConfig,
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._config = config
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.Client(trust_env=False)

    def query(
        self,
        query: MetricQuery,
        *,
        principal: Principal,
        request_id: str,
    ) -> MetricResult:
        payload = json.dumps(
            query.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(payload) > MAX_TOOL_REQUEST_BYTES:
            raise ValueError(f"query_metric request exceeds {MAX_TOOL_REQUEST_BYTES} bytes")

        timeout = httpx.Timeout(
            connect=self._config.connect_timeout_seconds,
            read=self._config.read_timeout_seconds,
            write=self._config.read_timeout_seconds,
            pool=self._config.connect_timeout_seconds,
        )
        try:
            with self._http_client.stream(
                "POST",
                f"{self._config.base_url.rstrip('/')}/v1/tools/query_metric",
                headers={
                    "Authorization": f"Bearer {self._config.api_key}",
                    "Content-Type": "application/json",
                    "X-Request-ID": request_id,
                    "X-Tenant-ID": principal.tenant_id,
                    "X-User-ID": principal.user_id,
                },
                content=payload,
                timeout=timeout,
            ) as response:
                if not response.is_success:
                    retryable = response.status_code == 429 or response.status_code >= 500
                    code = (
                        ToolGatewayErrorCode.UNAVAILABLE
                        if retryable
                        else ToolGatewayErrorCode.REJECTED
                    )
                    raise ToolGatewayError(
                        code,
                        request_id=request_id,
                        retryable=retryable,
                        status_code=response.status_code,
                    )
                raw_result = _read_bounded(
                    response,
                    limit=self._config.max_result_bytes,
                    request_id=request_id,
                )
        except httpx.TimeoutException:
            raise ToolGatewayError(
                ToolGatewayErrorCode.TIMEOUT,
                request_id=request_id,
                retryable=True,
            ) from None
        except httpx.RequestError:
            raise ToolGatewayError(
                ToolGatewayErrorCode.UNAVAILABLE,
                request_id=request_id,
                retryable=True,
            ) from None

        try:
            body = cast(object, json.loads(raw_result.decode("utf-8")))
            result = MetricResult.model_validate(body)
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, TypeError):
            raise ToolGatewayError(
                ToolGatewayErrorCode.INVALID_RESPONSE,
                request_id=request_id,
            ) from None
        if (result.metric, result.period, result.org) != (
            query.metric,
            query.period,
            query.org,
        ):
            raise ToolGatewayError(
                ToolGatewayErrorCode.INVALID_RESPONSE,
                request_id=request_id,
            )
        return result

    def close(self) -> None:
        if self._owns_http_client:
            self._http_client.close()

    def __repr__(self) -> str:
        return f"QueryMetricClient(base_url={self._config.base_url!r})"


def _read_bounded(
    response: httpx.Response,
    *,
    limit: int,
    request_id: str,
) -> bytes:
    content_length = response.headers.get("content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except ValueError:
            declared_length = -1
        if declared_length > limit:
            raise ToolGatewayError(
                ToolGatewayErrorCode.RESULT_TOO_LARGE,
                request_id=request_id,
            )

    chunks: list[bytes] = []
    total = 0
    for chunk in response.iter_bytes():
        total += len(chunk)
        if total > limit:
            raise ToolGatewayError(
                ToolGatewayErrorCode.RESULT_TOO_LARGE,
                request_id=request_id,
            )
        chunks.append(chunk)
    return b"".join(chunks)


__all__ = ["QueryMetricClient", "QueryMetricConfig"]
