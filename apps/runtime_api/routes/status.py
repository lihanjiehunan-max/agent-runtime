from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Literal, Protocol, cast

from fastapi import APIRouter, Depends, Request, Response, status
from prometheus_client import CONTENT_TYPE_LATEST
from pydantic import BaseModel, ConfigDict

from apps.runtime_api.dependencies import api_error, current_principal
from packages.event_model.metrics import RuntimeMetrics
from packages.runtime_contracts import ErrorCode, Principal

router = APIRouter(tags=["status"])

DependencyState = Literal["configured", "not_configured", "unavailable"]
OverallState = Literal["ready", "degraded", "not_ready"]


class RuntimeDependencies(BaseModel):
    model_config = ConfigDict(extra="forbid")

    postgres: DependencyState
    minio: DependencyState
    redis: DependencyState


class RuntimeIntegrations(BaseModel):
    model_gateway: DependencyState
    tool_gateway: DependencyState
    deepagents_sdk: DependencyState


class RuntimeStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["runtime.status.v1"] = "runtime.status.v1"
    status: OverallState
    dependencies: RuntimeDependencies
    integrations: RuntimeIntegrations
    metrics: DependencyState


class RuntimeStatusProvider(Protocol):
    def snapshot(self) -> RuntimeStatus: ...


class EnvironmentRuntimeStatus:
    """Expose configuration state without returning credentials or provider errors."""

    def __init__(
        self,
        environ: Mapping[str, str],
        *,
        metrics_configured: bool,
        runtime_configured: bool = False,
        probe_results: Mapping[str, bool] | None = None,
    ) -> None:
        self._environ = environ
        self._metrics_configured = metrics_configured
        self._runtime_configured = runtime_configured
        self._probe_results = dict(probe_results or {})

    def mark_runtime_configured(self) -> None:
        self._runtime_configured = True

    def set_probe_results(self, results: Mapping[str, bool]) -> None:
        self._probe_results = dict(results)

    def _state(self, key: str, configured: bool) -> DependencyState:
        if not configured:
            return "not_configured"
        if key in self._probe_results and not self._probe_results[key]:
            return "unavailable"
        return "configured"

    def snapshot(self) -> RuntimeStatus:
        dependencies = RuntimeDependencies(
            postgres=self._state("postgres", bool(self._environ.get("RUNTIME_DATABASE_URL"))),
            minio=self._state("minio", bool(self._environ.get("RUNTIME_MINIO_ENDPOINT"))),
            redis=self._state("redis", bool(self._environ.get("RUNTIME_REDIS_URL"))),
        )
        integrations = RuntimeIntegrations(
            model_gateway=self._state(
                "model_gateway",
                bool(self._environ.get("MODEL_GATEWAY_BASE_URL"))
                and bool(self._environ.get("MODEL_GATEWAY_API_KEY"))
                and bool(self._environ.get("MODEL_GATEWAY_MODEL")),
            ),
            tool_gateway=self._state(
                "tool_gateway",
                bool(self._environ.get("RUNTIME_TOOL_GATEWAY_URL"))
                and bool(self._environ.get("RUNTIME_TOOL_GATEWAY_API_KEY")),
            ),
            deepagents_sdk=self._state(
                "deepagents_sdk",
                self._environ.get("RUNTIME_DEEPAGENTS_SDK_VERSION", "0.7.7")
                == "0.7.7",
            ),
        )
        dependency_values = (
            dependencies.postgres,
            dependencies.minio,
            dependencies.redis,
            integrations.model_gateway,
            integrations.tool_gateway,
            integrations.deepagents_sdk,
        )
        overall: OverallState = (
            "ready"
            if self._runtime_configured
            and all(value == "configured" for value in dependency_values)
            else "not_ready"
        )
        return RuntimeStatus(
            status=overall,
            dependencies=dependencies,
            integrations=integrations,
            metrics="configured" if self._metrics_configured else "not_configured",
        )


def runtime_metrics(request: Request) -> RuntimeMetrics:
    metrics = getattr(request.app.state, "runtime_metrics", None)
    if not isinstance(metrics, RuntimeMetrics):
        raise api_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ErrorCode.RUNTIME_INCOMPATIBLE,
            "runtime metrics are not configured",
        )
    return metrics


def runtime_status_provider(request: Request) -> RuntimeStatusProvider:
    provider = getattr(request.app.state, "runtime_status", None)
    if provider is None or not hasattr(provider, "snapshot"):
        raise api_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ErrorCode.RUNTIME_INCOMPATIBLE,
            "runtime status is not configured",
        )
    return cast(RuntimeStatusProvider, provider)


@router.get("/metrics")
def metrics_endpoint(
    metrics: Annotated[RuntimeMetrics, Depends(runtime_metrics)],
) -> Response:
    return Response(content=metrics.exposition(), media_type=CONTENT_TYPE_LATEST)


@router.get("/api/v1/runtime/status", response_model=RuntimeStatus)
def runtime_status_endpoint(
    _principal: Annotated[Principal, Depends(current_principal)],
    provider: Annotated[RuntimeStatusProvider, Depends(runtime_status_provider)],
) -> RuntimeStatus:
    return provider.snapshot()


__all__ = [
    "EnvironmentRuntimeStatus",
    "RuntimeDependencies",
    "RuntimeIntegrations",
    "RuntimeStatus",
    "RuntimeStatusProvider",
    "metrics_endpoint",
    "router",
    "runtime_metrics",
    "runtime_status_endpoint",
]
