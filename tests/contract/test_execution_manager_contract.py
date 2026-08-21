from __future__ import annotations

import asyncio

from packages.runtime_contracts import ExecutionStatus
from tests.integration.test_sync_execution import (
    PRINCIPAL,
    GraphDouble,
    manager_for_test,
)


def test_awaitable_astream_events_is_resolved_before_async_iteration() -> None:
    manager, _sessions, _coordinator, sink, _executions = manager_for_test(
        GraphDouble([], awaitable=True)
    )

    result = asyncio.run(manager.execute_turn("session-a", "await this", PRINCIPAL))

    assert result.status is ExecutionStatus.SUCCEEDED
    assert all(event.event_type != "runtime.error" for event in sink.events)
