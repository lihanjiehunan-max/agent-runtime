from pathlib import Path

from packages.package_loader.schema import PackageEventContext
from packages.package_loader.service import PackageLoader
from packages.runtime_contracts import AgentPackageRef, RuntimeEvent, RuntimeType

ACCEPTANCE_DIGEST = "sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04"


def test_acceptance_package_loads_with_pinned_definition_and_bounded_events() -> None:
    events: list[RuntimeEvent] = []
    loader = PackageLoader.local(
        Path("agents"),
        tenant_id="tenant-a",
        expected_packages={
            ("agent-metric-query", "0.1.0"): AgentPackageRef(
                tenant_id="tenant-a",
                agent_id="agent-metric-query",
                version="0.1.0",
                digest=ACCEPTANCE_DIGEST,
                runtime_type=RuntimeType.DEEPAGENTS,
                sdk_version="0.7.7",
            )
        },
        event_context=PackageEventContext(
            trace_id="trace-package-load",
            parent_span_id=None,
            session_id="session-package-load",
            execution_id="execution-package-load",
            worker_id="worker-01",
            sequence_start=11,
        ),
        event_sink=events.append,
    )

    loaded = loader.load("agent-metric-query", "0.1.0")

    assert loaded.reference.tenant_id == "tenant-a"
    assert loaded.reference.agent_id == "agent-metric-query"
    assert loaded.reference.version == "0.1.0"
    assert loaded.reference.runtime_type is RuntimeType.DEEPAGENTS
    assert loaded.reference.sdk_version == "0.7.7"
    assert loaded.reference.digest.startswith("sha256:")
    assert loaded.agent.model.ref == "model://metric-query-primary"
    assert [(tool.name, tool.effect) for tool in loaded.tool_bindings.allowlist] == [
        ("query_metric", "read_only")
    ]
    assert loaded.limits.session_ttl_minutes == 1440
    assert loaded.limits.execution_timeout_seconds == 60
    assert loaded.limits.max_model_calls == 6
    assert loaded.limits.max_tool_calls == 8
    assert loaded.limits.max_tokens == 20000
    assert loaded.system_prompt.strip()

    assert [event.event_type for event in events] == [
        "package.resolve.started",
        "package.resolve.completed",
        "package.verify.completed",
        "package.cache.miss",
        "agent.definition.created",
    ]
    assert [event.sequence for event in events] == [11, 12, 13, 14, 15]
    assert all(event.package == loaded.reference for event in events)
    assert all(event.tenant_id == "tenant-a" for event in events)
    assert all(event.payload_ref is None for event in events)
    assert all(len(event.to_json().encode()) < 2048 for event in events)
    expected_payload = {
        "agent_id": "agent-metric-query",
        "version": "0.1.0",
        "digest": ACCEPTANCE_DIGEST,
    }
    assert all(
        event.model_dump(mode="json")["payload"] == expected_payload
        for event in events
    )
    serialized_events = "\n".join(event.to_json() for event in events)
    for secret_definition_value in (
        loaded.system_prompt.strip(),
        loaded.agent.model.ref,
        "query_metric",
        "session_ttl_minutes",
        "checksums",
    ):
        assert secret_definition_value not in serialized_events
