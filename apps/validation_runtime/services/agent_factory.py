from deepagents import (
    GeneralPurposeSubagentProfile,
    HarnessProfile,
    create_deep_agent,
    register_harness_profile,
)
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import SecretStr

from apps.validation_runtime.domain import LoadedAgentAssets
from apps.validation_runtime.errors import RuntimeServiceError
from apps.validation_runtime.services.model_factory import ModelFactory


DISABLED_BUILTIN_TOOLS = frozenset(
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


class AgentFactory:
    def __init__(self, model_factory: ModelFactory) -> None:
        self._model_factory = model_factory

    def build(self, assets: LoadedAgentAssets, key: SecretStr | None):
        if key is None:
            raise RuntimeServiceError(
                "MODEL_API_KEY_MISSING",
                "Model API key is not configured.",
            )

        register_harness_profile(
            f"openai:{assets.gateway.model}",
            HarnessProfile(
                excluded_tools=DISABLED_BUILTIN_TOOLS,
                general_purpose_subagent=GeneralPurposeSubagentProfile(
                    enabled=False
                ),
            ),
        )
        prompt = "\n\n".join(skill.instructions for skill in assets.skills)
        model = self._model_factory.create(assets.gateway, key)
        return create_deep_agent(
            model=model,
            tools=[],
            system_prompt=prompt,
            subagents=[],
            checkpointer=InMemorySaver(),
        )
