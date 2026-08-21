from __future__ import annotations

from tests.performance.locustfile import (
    MAX_SESSIONS,
    MIN_SESSIONS,
    run_deterministic_load,
)


def test_forty_session_release_gate_meets_bounded_thresholds() -> None:
    report = run_deterministic_load(session_count=40)

    assert MIN_SESSIONS <= report.session_count <= MAX_SESSIONS
    assert report.cache_hit_rate >= 0.95
    assert report.cached_definition_p95_ms <= 100.0
    assert report.platform_overhead_p95_ms <= 300.0
    assert report.cooperative_cancel_seconds <= 2.0
    assert report.trace_coverage == 1.0
    assert report.short_task_success_rate >= 0.95
    assert report.raw_payloads_recorded == 0
    assert report.credentials_recorded == 0
