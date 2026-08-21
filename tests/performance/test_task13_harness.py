from __future__ import annotations

import math

from tests.performance.locustfile import run_deterministic_load


class _StepClock:
    def __init__(self, step_seconds: float) -> None:
        self._step_seconds = step_seconds
        self._current = 0.0

    def __call__(self) -> float:
        current = self._current
        self._current += self._step_seconds
        return current


def test_deterministic_load_reports_real_runtime_path_evidence() -> None:
    report = run_deterministic_load(session_count=30)

    assert report.runtime_events_recorded >= 30
    assert report.trace_projections_recorded == 30
    assert report.tool_calls_recorded == 30
    assert report.checkpoints_recorded == 30
    assert report.raw_payloads_recorded == 0
    assert report.credentials_recorded == 0


def test_cached_definition_p95_uses_injected_clock() -> None:
    report = run_deterministic_load(
        session_count=30,
        clock=_StepClock(step_seconds=0.001),
    )

    assert math.isclose(report.cached_definition_p95_ms, 2.0, abs_tol=1e-9)


def test_cached_definition_p95_is_stable_across_repeated_runs() -> None:
    reports = [run_deterministic_load(session_count=30) for _ in range(3)]

    values = [report.cached_definition_p95_ms for report in reports]
    assert all(math.isclose(value, 2.0, abs_tol=1e-9) for value in values)
    assert len({round(value, 9) for value in values}) == 1
    assert all(value <= 100.0 for value in values)
