import re

import pytest
from pydantic import ValidationError

from apps.validation_runtime.services.agent_instance_registry import AgentInstanceRegistry
from apps.validation_runtime.services.asset_loader import AssetLoader


@pytest.fixture
def loaded_assets():
    return AssetLoader("runtime_assets").load_agent("shipping-analyst")


@pytest.fixture
def fake_graph():
    return object()


def test_deploy_is_idempotent(loaded_assets, fake_graph):
    registry = AgentInstanceRegistry()

    first = registry.deploy(loaded_assets, fake_graph)
    second = registry.deploy(loaded_assets, fake_graph)

    assert first.agent_instance_id == second.agent_instance_id
    assert first.package_digest == loaded_assets.package_digest


def test_deploy_creates_an_active_instance_from_loaded_assets(loaded_assets, fake_graph):
    instance = AgentInstanceRegistry().deploy(loaded_assets, fake_graph)

    assert re.fullmatch(r"ain_[0-9a-f-]{36}", instance.agent_instance_id)
    assert instance.agent_id == "shipping-analyst"
    assert instance.agent_version == "0.1.0"
    assert instance.runtime_type == "deepagents-python"
    assert instance.model_gateway_id == "zero-api-gpt-5.6-sol"
    assert instance.model_alias == "gpt-5.6-sol"
    assert instance.skill_ids == ("shipping-operations-analyst",)
    assert instance.status.value == "ACTIVE"
    assert instance.created_at.tzinfo is not None


def test_get_returns_none_for_unknown_instance_id():
    assert AgentInstanceRegistry().get("ain_missing") is None


def test_instance_is_immutable_and_does_not_expose_the_compiled_graph(
    loaded_assets, fake_graph
):
    registry = AgentInstanceRegistry()
    instance = registry.deploy(loaded_assets, fake_graph)

    with pytest.raises(ValidationError):
        instance.status = "FAILED"

    assert not hasattr(instance, "graph")
    assert registry._records_by_id[instance.agent_instance_id].graph is fake_graph
