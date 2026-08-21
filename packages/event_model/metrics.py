from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import cast

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client.metrics import MetricWrapperBase

from packages.runtime_contracts import RuntimeEvent

ALLOWED_METRIC_LABELS: Mapping[str, tuple[str, ...]] = {
    "runtime_executions": ("status",),
    "runtime_execution_duration_seconds": ("status",),
    "runtime_model_duration_seconds": (),
    "runtime_tool_duration_seconds": (),
    "runtime_tokens": ("direction",),
    "runtime_package_cache_events": ("result",),
    "runtime_active_sessions": (),
    "runtime_queue_depth": (),
    "runtime_cancellation_latency_seconds": (),
}

_STATUSES = frozenset(
    {"accepted", "running", "succeeded", "failed", "timed_out", "cancelled", "other"}
)
_TERMINAL_STATUS = {
    "execution.succeeded": "succeeded",
    "execution.failed": "failed",
    "execution.timed_out": "timed_out",
    "execution.cancelled": "cancelled",
}


def _existing_collector(
    registry: CollectorRegistry,
    name: str,
) -> MetricWrapperBase | None:
    collectors = getattr(registry, "_names_to_collectors", {})
    return cast(MetricWrapperBase | None, collectors.get(name))


def _duplicate_safe(
    registry: CollectorRegistry,
    name: str,
    factory: Callable[[], MetricWrapperBase],
    labels: tuple[str, ...] = (),
) -> MetricWrapperBase:
    try:
        return factory()
    except ValueError as error:
        existing = _existing_collector(registry, name)
        if existing is None:
            raise
        existing_labels = tuple(getattr(existing, "_labelnames", ()))
        if existing_labels != labels:
            raise ValueError(
                f"collector {name!r} has incompatible label schema: "
                f"existing={existing_labels!r}, expected={labels!r}"
            ) from error
        return existing


def _counter(
    registry: CollectorRegistry,
    name: str,
    documentation: str,
    labels: tuple[str, ...] = (),
) -> Counter:
    return cast(
        Counter,
        _duplicate_safe(
            registry,
            name,
            lambda: Counter(name, documentation, labels, registry=registry),
            labels,
        ),
    )


def _histogram(
    registry: CollectorRegistry,
    name: str,
    documentation: str,
    labels: tuple[str, ...] = (),
) -> Histogram:
    return cast(
        Histogram,
        _duplicate_safe(
            registry,
            name,
            lambda: Histogram(name, documentation, labels, registry=registry),
            labels,
        ),
    )


def _gauge(
    registry: CollectorRegistry,
    name: str,
    documentation: str,
    labels: tuple[str, ...] = (),
) -> Gauge:
    return cast(
        Gauge,
        _duplicate_safe(
            registry,
            name,
            lambda: Gauge(name, documentation, labels, registry=registry),
            labels,
        ),
    )


def _bounded_status(value: object) -> str:
    candidate = getattr(value, "value", value)
    return candidate if isinstance(candidate, str) and candidate in _STATUSES else "other"


def _bounded_seconds(value: float) -> float:
    if value < 0:
        raise ValueError("metric duration cannot be negative")
    return value


class RuntimeMetrics:
    """Low-cardinality runtime metrics with safe reuse of a test registry."""

    def __init__(self, *, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or REGISTRY
        self.executions = _counter(
            self.registry,
            "runtime_executions",
            "Total runtime executions by bounded terminal or lifecycle status.",
            ("status",),
        )
        self.execution_duration_seconds = _histogram(
            self.registry,
            "runtime_execution_duration_seconds",
            "Runtime execution duration in seconds by bounded status.",
            ("status",),
        )
        self.model_duration_seconds = _histogram(
            self.registry,
            "runtime_model_duration_seconds",
            "Aggregate model boundary duration in seconds.",
        )
        self.tool_duration_seconds = _histogram(
            self.registry,
            "runtime_tool_duration_seconds",
            "Aggregate tool boundary duration in seconds.",
        )
        self.tokens = _counter(
            self.registry,
            "runtime_tokens",
            "Total normalized model tokens by stable direction.",
            ("direction",),
        )
        self.package_cache_events = _counter(
            self.registry,
            "runtime_package_cache_events",
            "Package cache events by hit or miss result.",
            ("result",),
        )
        self.active_sessions = _gauge(
            self.registry,
            "runtime_active_sessions",
            "Current active runtime sessions.",
        )
        self.queue_depth = _gauge(
            self.registry,
            "runtime_queue_depth",
            "Current bounded runtime queue depth.",
        )
        self.cancellation_latency_seconds = _histogram(
            self.registry,
            "runtime_cancellation_latency_seconds",
            "Cooperative cancellation latency in seconds.",
        )

    def record_execution(self, status: object, *, duration_seconds: float | None = None) -> None:
        label = _bounded_status(status)
        self.executions.labels(status=label).inc()
        if duration_seconds is not None:
            self.execution_duration_seconds.labels(status=label).observe(
                _bounded_seconds(duration_seconds)
            )

    def observe_model_duration(self, duration_seconds: float) -> None:
        self.model_duration_seconds.observe(_bounded_seconds(duration_seconds))

    def observe_tool_duration(self, duration_seconds: float) -> None:
        self.tool_duration_seconds.observe(_bounded_seconds(duration_seconds))

    def record_tokens(self, *, input_tokens: int = 0, output_tokens: int = 0) -> None:
        if input_tokens < 0 or output_tokens < 0:
            raise ValueError("metric token totals cannot be negative")
        self.tokens.labels(direction="input").inc(input_tokens)
        self.tokens.labels(direction="output").inc(output_tokens)

    def record_package_cache(self, *, hit: bool) -> None:
        self.package_cache_events.labels(result="hit" if hit else "miss").inc()

    def set_active_sessions(self, value: int) -> None:
        if value < 0:
            raise ValueError("active session gauge cannot be negative")
        self.active_sessions.set(value)

    def set_queue_depth(self, value: int) -> None:
        if value < 0:
            raise ValueError("queue depth gauge cannot be negative")
        self.queue_depth.set(value)

    def observe_cancellation_latency(self, duration_seconds: float) -> None:
        self.cancellation_latency_seconds.observe(_bounded_seconds(duration_seconds))

    def observe_event(self, event: RuntimeEvent) -> None:
        if event.event_type in _TERMINAL_STATUS:
            duration = event.duration_ms / 1000 if event.duration_ms is not None else None
            self.record_execution(_TERMINAL_STATUS[event.event_type], duration_seconds=duration)
        elif event.event_type == "execution.accepted":
            self.record_execution("accepted")
        elif event.event_type == "execution.started":
            self.record_execution("running")
        if event.duration_ms is not None:
            duration_seconds = event.duration_ms / 1000
            if event.phase == "model":
                self.observe_model_duration(duration_seconds)
            elif event.phase == "tool":
                self.observe_tool_duration(duration_seconds)
        if event.event_type.startswith("package.cache."):
            self.record_package_cache(hit=event.event_type.endswith(".hit"))
        if event.event_type == "model.completed":
            input_tokens = event.payload.get("input_tokens", 0)
            output_tokens = event.payload.get("output_tokens", 0)
            self.record_tokens(
                input_tokens=input_tokens if isinstance(input_tokens, int) else 0,
                output_tokens=output_tokens if isinstance(output_tokens, int) else 0,
            )

    def exposition(self) -> bytes:
        return generate_latest(self.registry)


__all__ = ["ALLOWED_METRIC_LABELS", "RuntimeMetrics"]
