from __future__ import annotations

from decimal import Decimal
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from httpx import Response

from apps.runtime_api.composition import (
    RequestScopedQueryMetricTool,
    RuntimeCompositionConfig,
    RuntimeConfigurationError,
)
from apps.runtime_api.main import create_api
from apps.runtime_api.routes.status import EnvironmentRuntimeStatus
from packages.runtime_contracts import (
    AgentPackageRef,
    Principal,
    RuntimeEvent,
    RuntimeType,
)
from packages.tool_gateway.contracts import MetricQuery, MetricResult, ToolEventContext
from packages.tool_gateway.runtime_context import (
    RuntimeToolContext,
    bind_runtime_tool_context,
)


def _environment() -> dict[str, str]:
    return {
        "RUNTIME_DATABASE_URL": "postgresql+asyncpg://runtime:password@postgres/runtime",
        "RUNTIME_REDIS_URL": "redis://redis:6379/0",
        "RUNTIME_MINIO_ENDPOINT": "minio:9000",
        "RUNTIME_MINIO_ACCESS_KEY": "runtime-access",
        "RUNTIME_MINIO_SECRET_KEY": "runtime-secret",
        "RUNTIME_MINIO_BUCKET": "runtime-payloads",
        "MODEL_GATEWAY_BASE_URL": "https://model-gateway.internal/v1",
        "MODEL_GATEWAY_API_KEY": "model-secret",
        "MODEL_GATEWAY_MODEL": "metric-query",
        "RUNTIME_TOOL_GATEWAY_URL": "https://tool-gateway.internal",
        "RUNTIME_TOOL_GATEWAY_API_KEY": "tool-secret",
        "RUNTIME_WORKER_ID": "worker-a",
        "RUNTIME_DEFAULT_TENANT_ID": "tenant-a",
        "RUNTIME_DEFAULT_AGENT_ID": "agent-metric-query",
        "RUNTIME_DEFAULT_AGENT_VERSION": "0.1.0",
        "RUNTIME_DEFAULT_PACKAGE_DIGEST": "sha256:" + "a" * 64,
    }


def _get(client: TestClient, path: str) -> Response:
    return cast(Response, client.get(path))  # pyright: ignore[reportUnknownMemberType]


def test_production_composition_requires_canonical_model_gateway_names() -> None:
    environment = _environment()
    environment["RUNTIME_MODEL_API_KEY"] = environment.pop("MODEL_GATEWAY_API_KEY")

    with pytest.raises(RuntimeConfigurationError, match="MODEL_GATEWAY_API_KEY"):
        RuntimeCompositionConfig.from_env(environment)


def test_production_composition_carries_worker_and_package_identity() -> None:
    config = RuntimeCompositionConfig.from_env(_environment())

    assert config.worker_id == "worker-a"
    assert config.package_ref.agent_id == "agent-metric-query"
    assert config.package_ref.digest.startswith("sha256:")
    assert config.deepagents_sdk_version == "0.7.7"


def test_readiness_reports_provider_integrations_and_stays_closed_until_bound() -> None:
    environment = _environment()
    status = EnvironmentRuntimeStatus(environment, metrics_configured=True)
    app = create_api(
        environ={"RUNTIME_ALLOW_DEV_AUTH": "1", **environment},
        runtime_status=status,
    )

    with TestClient(app) as client:
        response = _get(client, "/health/ready")

    assert response.status_code == 503
    body = cast(dict[str, object], response.json())
    assert body["status"] == "not_ready"
    assert body["integrations"] == {
        "model_gateway": "configured",
        "tool_gateway": "configured",
        "deepagents_sdk": "configured",
    }


def test_readiness_becomes_ready_only_after_explicit_runtime_binding() -> None:
    environment = _environment()
    status = EnvironmentRuntimeStatus(
        environment,
        metrics_configured=True,
        runtime_configured=True,
    )
    app = create_api(
        environ={"RUNTIME_ALLOW_DEV_AUTH": "1", **environment},
        runtime_status=status,
    )

    with TestClient(app) as client:
        response = _get(client, "/health/ready")

    assert response.status_code == 200
    body = cast(dict[str, object], response.json())
    assert body["status"] == "ready"


def test_readiness_stays_503_when_a_bound_provider_probe_fails() -> None:
    environment = _environment()
    status = EnvironmentRuntimeStatus(
        environment,
        metrics_configured=True,
        runtime_configured=True,
        probe_results={
            "postgres": True,
            "redis": False,
            "minio": True,
            "model_gateway": True,
            "tool_gateway": True,
            "deepagents_sdk": True,
        },
    )
    app = create_api(
        environ={"RUNTIME_ALLOW_DEV_AUTH": "1", **environment},
        runtime_status=status,
    )

    with TestClient(app) as client:
        response = _get(client, "/health/ready")

    body = cast(dict[str, object], response.json())
    dependencies = cast(dict[str, object], body["dependencies"])
    assert response.status_code == 503
    assert dependencies["redis"] == "unavailable"


def test_request_scoped_tool_keeps_gateway_audit_events() -> None:
    class MetricClient:
        def query(
            self,
            query: MetricQuery,
            *,
            principal: Principal,
            request_id: str,
        ) -> MetricResult:
            del principal, request_id
            return MetricResult(
                metric=query.metric,
                period=query.period,
                org=query.org,
                value=Decimal("42"),
                unit="CNY",
            )

    package = AgentPackageRef(
        tenant_id="tenant-a",
        agent_id="agent-metric-query",
        version="0.1.0",
        digest="sha256:" + "a" * 64,
        runtime_type=RuntimeType.DEEPAGENTS,
        sdk_version="0.7.7",
    )
    principal = Principal(
        tenant_id="tenant-a",
        user_id="user-a",
        actor_id="actor-a",
        worker_id="worker-a",
        permissions=("tool:query_metric",),
    )
    audit_events: list[RuntimeEvent] = []
    context = RuntimeToolContext(
        principal,
        ToolEventContext(
            trace_id="trace-a",
            parent_span_id=None,
            session_id="session-a",
            execution_id="execution-a",
            worker_id="worker-a",
            package=package,
        ),
        audit_events,
    )
    tool = RequestScopedQueryMetricTool(cast(Any, MetricClient())).as_langchain_tool()

    with bind_runtime_tool_context(context):
        result = tool.invoke(
            {"metric": "营业收入", "period": "本月", "org": "org-a"}
        )

    assert cast(MetricResult, result).value == 42
    assert [event.event_type for event in audit_events] == [
        "tool.started",
        "tool.completed",
    ]
