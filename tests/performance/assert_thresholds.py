from __future__ import annotations

import json
import sys
from pathlib import Path

if __package__:
    from .locustfile import PerformanceReport, run_deterministic_load
else:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from locustfile import PerformanceReport, run_deterministic_load


def assert_release_thresholds(report: PerformanceReport) -> None:
    assert 30 <= report.session_count <= 50
    assert report.cache_hit_rate >= 0.95
    assert report.cached_definition_p95_ms <= 100.0
    assert report.platform_overhead_p95_ms <= 300.0
    assert report.cooperative_cancel_seconds <= 2.0
    assert report.trace_coverage == 1.0
    assert report.short_task_success_rate >= 0.95
    assert report.raw_payloads_recorded == 0
    assert report.credentials_recorded == 0


def main() -> None:
    report = run_deterministic_load()
    assert_release_thresholds(report)
    print(json.dumps(report.to_dict(), sort_keys=True))


if __name__ == "__main__":
    main()
