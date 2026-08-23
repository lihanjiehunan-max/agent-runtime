from dataclasses import replace

import pytest

from apps.validation_runtime.domain import AgentInstanceStatus
from apps.validation_runtime.errors import RuntimeServiceError
from apps.validation_runtime.services.agent_instance_registry import AgentInstanceRegistry
from apps.validation_runtime.services.agent_router import AgentRouter
from apps.validation_runtime.services.asset_loader import AssetLoader


@pytest.fixture
def loaded_assets():
    return AssetLoader("runtime_assets").load_agent("shipping-analyst")


@pytest.fixture
def fake_graph():
    return object()


def test_router_resolves_the_sole_active_exact_agent_id(loaded_assets, fake_graph):
    registry = AgentInstanceRegistry()
    deployed = registry.deploy(loaded_assets, fake_graph)

    assert AgentRouter(registry).resolve("shipping-analyst") == deployed


def test_router_rejects_agent_without_active_instance():
    with pytest.raises(RuntimeServiceError) as exc:
        AgentRouter(AgentInstanceRegistry()).resolve("shipping-analyst")

    assert exc.value.detail.code == "AGENT_INSTANCE_UNAVAILABLE"


def test_router_rejects_an_agent_with_only_inactive_instances(loaded_assets, fake_graph):
    registry = AgentInstanceRegistry()
    instance = registry.deploy(loaded_assets, fake_graph)
    record = registry._records_by_id[instance.agent_instance_id]
    registry._records_by_id[instance.agent_instance_id] = replace(
        record,
        instance=record.instance.model_copy(
            update={"status": AgentInstanceStatus.FAILED}
        ),
    )

    with pytest.raises(RuntimeServiceError) as exc:
        AgentRouter(registry).resolve("shipping-analyst")

    assert exc.value.detail.code == "AGENT_INSTANCE_UNAVAILABLE"


def test_router_rejects_ambiguous_active_agent_instances(loaded_assets, fake_graph):
    registry = AgentInstanceRegistry()
    registry.deploy(loaded_assets, fake_graph)
    registry.deploy(
        loaded_assets.model_copy(update={"package_digest": "different-digest"}),
        object(),
    )

    with pytest.raises(RuntimeServiceError) as exc:
        AgentRouter(registry).resolve("shipping-analyst")

    assert exc.value.detail.code == "AGENT_ROUTING_AMBIGUOUS"


def test_list_logical_agents_reports_only_deterministically_available_agents(
    loaded_assets, fake_graph
):
    registry = AgentInstanceRegistry()
    registry.deploy(loaded_assets, fake_graph)

    logical_agents = AgentRouter(registry).list_logical_agents()

    assert [agent.model_dump() for agent in logical_agents] == [
        {
            "agent_id": "shipping-analyst",
            "display_name": "shipping-analyst",
            "available": True,
        }
    ]
