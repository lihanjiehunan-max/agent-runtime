from __future__ import annotations

from typing import cast

import pytest
from httpx import Response
from prometheus_client import CollectorRegistry, Counter, generate_latest
from starlette.testclient import TestClient

from apps.runtime_api.main import create_api
from packages.event_model.metrics import ALLOWED_METRIC_LABELS, RuntimeMetrics


def _get(
    client: TestClient,
    path: str,
    *,
    headers: dict[str, str] | None = None,
) -> Response:
    return cast(
        Response,
        client.get(path, headers=headers),  # pyright: ignore[reportUnknownMemberType]
    )


def test_metrics_registration_is_duplicate_safe_and_labels_are_bounded() -> None:
    registry = CollectorRegistry()
    first = RuntimeMetrics(registry=registry)
    second = RuntimeMetrics(registry=registry)
    first.record_execution("succeeded", duration_seconds=0.25)
    first.record_execution("tenant-a", duration_seconds=0.5)
    first.record_tokens(input_tokens=4, output_tokens=6)
    first.record_package_cache(hit=True)
    first.observe_model_duration(0.2)
    first.observe_tool_duration(0.1)
    first.set_active_sessions(2)
    first.set_queue_depth(0)
    first.observe_cancellation_latency(0.05)

    assert second.registry is registry
    collectors = registry._names_to_collectors  # type: ignore[attr-defined]
    metric_labels = {
        name: tuple(getattr(collectors[name], "_labelnames", ()))
        for name in ALLOWED_METRIC_LABELS
    }
    assert metric_labels["runtime_executions"] == ("status",)
    assert metric_labels["runtime_execution_duration_seconds"] == ("status",)
    assert metric_labels["runtime_model_duration_seconds"] == ()
    assert metric_labels["runtime_tool_duration_seconds"] == ()
    assert metric_labels["runtime_tokens"] == ("direction",)
    assert metric_labels["runtime_package_cache_events"] == ("result",)
    assert metric_labels["runtime_active_sessions"] == ()
    assert metric_labels["runtime_queue_depth"] == ()
    assert metric_labels["runtime_cancellation_latency_seconds"] == ()
    all_labels = {label for labels in metric_labels.values() for label in labels}
    assert not all_labels.intersection(
        {"tenant_id", "user_id", "session_id", "execution_id", "trace_id", "worker_id"}
    )
    exposition = generate_latest(registry).decode()
    assert "tenant-a" not in exposition


def test_duplicate_metric_name_with_incompatible_labels_fails_clearly() -> None:
    registry = CollectorRegistry()
    Counter(
        "runtime_executions",
        "incompatible test collector",
        ("unexpected",),
        registry=registry,
    )

    with pytest.raises(ValueError, match="incompatible label schema"):
        RuntimeMetrics(registry=registry)


def test_status_and_metrics_are_wired_through_create_api_without_raw_dependency_errors() -> None:
    registry = CollectorRegistry()
    metrics = RuntimeMetrics(registry=registry)
    app = create_api(
        metrics=metrics,
        environ={"RUNTIME_ALLOW_DEV_AUTH": "1"},
    )

    with TestClient(app) as client:
        metrics_response = _get(client, "/metrics")
        status_response = _get(
            client,
            "/api/v1/runtime/status",
            headers={"authorization": "Bearer dev-token"},
        )

    assert metrics_response.status_code == 200
    assert "runtime_executions" in metrics_response.text
    assert status_response.status_code == 200
    body = status_response.json()
    assert body["schema_version"] == "runtime.status.v1"
    assert body["dependencies"] == {
        "postgres": "not_configured",
        "minio": "not_configured",
        "redis": "not_configured",
    }
    assert "password" not in status_response.text.lower()
    assert "provider" not in status_response.text.lower()
