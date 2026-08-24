from pydantic import SecretStr
import pytest

from apps.validation_runtime.errors import RuntimeServiceError
from apps.validation_runtime.services import agent_factory as agent_factory_module
from apps.validation_runtime.services.agent_factory import AgentFactory
from apps.validation_runtime.services.asset_loader import AssetLoader
from apps.validation_runtime.services.model_factory import ModelFactory


@pytest.fixture
def assets():
    return AssetLoader("runtime_assets").load_agent("shipping-analyst")


def test_model_factory_uses_static_chat_completions_profile(assets):
    model = ModelFactory().create(assets.gateway, SecretStr("test-secret"))

    assert model.model_name == "gpt-5.6-sol"
    assert str(model.openai_api_base).rstrip("/") == "https://token.zero-api.cc.cd/v1"
    assert model.max_retries == 0
    assert model.use_responses_api is False


def test_agent_factory_rejects_missing_model_key(assets):
    with pytest.raises(RuntimeServiceError) as exc:
        AgentFactory(ModelFactory()).build(assets, None)

    assert exc.value.detail.code == "MODEL_API_KEY_MISSING"


def test_agent_factory_builds_with_skill_prompt_checkpoint_and_no_extra_tools(
    monkeypatch, assets
):
    captured = {}
    graph = object()

    def recording_builder(**kwargs):
        captured.update(kwargs)
        return graph

    monkeypatch.setattr(agent_factory_module, "create_deep_agent", recording_builder)
    monkeypatch.setattr(
        agent_factory_module,
        "register_harness_profile",
        lambda key, profile: captured.update(profile_key=key, profile=profile),
    )

    result = AgentFactory(ModelFactory()).build(assets, SecretStr("test-secret"))

    assert result is graph
    assert captured["tools"] == []
    assert captured["subagents"] == []
    assert "结论" in captured["system_prompt"]
    assert "依据" in captured["system_prompt"]
    assert "建议" in captured["system_prompt"]
    assert captured["checkpointer"].__class__.__name__ == "InMemorySaver"


def test_agent_factory_disables_all_builtin_tools_and_default_subagent(
    monkeypatch, assets
):
    captured = {}
    monkeypatch.setattr(agent_factory_module, "create_deep_agent", lambda **kwargs: object())
    monkeypatch.setattr(
        agent_factory_module,
        "register_harness_profile",
        lambda key, profile: captured.update(key=key, profile=profile),
    )

    AgentFactory(ModelFactory()).build(assets, SecretStr("test-secret"))

    assert captured["key"] == "openai:gpt-5.6-sol"
    assert captured["profile"].excluded_tools == frozenset(
        {
            "ls",
            "read_file",
            "write_file",
            "edit_file",
            "delete",
            "glob",
            "grep",
            "execute",
            "task",
        }
    )
    assert captured["profile"].general_purpose_subagent.enabled is False
