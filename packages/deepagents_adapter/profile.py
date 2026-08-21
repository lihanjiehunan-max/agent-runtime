from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from threading import Lock
from typing import Any, cast

from deepagents import (
    GeneralPurposeSubagentProfile,
    HarnessProfile,
    register_harness_profile,
)
from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from langchain_core.language_models import (
    BaseChatModel,
    LangSmithParams,
    LanguageModelInput,
)
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatResult
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool

BLOCKED_DEEPAGENTS_TOOLS = frozenset(
    {
        "cancel_async_task",
        "check_async_task",
        "delete",
        "edit_file",
        "execute",
        "glob",
        "grep",
        "list_async_tasks",
        "ls",
        "read_file",
        "start_async_task",
        "task",
        "update_async_task",
        "write_file",
        "write_todos",
    }
)

LEAST_PRIVILEGE_PROFILE = HarnessProfile(
    excluded_tools=BLOCKED_DEEPAGENTS_TOOLS,
    general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
)

_registration_lock = Lock()


class _AllowlistedChatModel(BaseChatModel):
    wrapped_model: BaseChatModel
    allowed_tool_names: frozenset[str]
    model_name: str

    @property
    def _llm_type(self) -> str:
        return "deepagents-least-privilege"

    def _get_ls_params(
        self,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> LangSmithParams:
        return self.wrapped_model._get_ls_params(  # pyright: ignore[reportPrivateUsage]
            stop=stop,
            **kwargs,
        )

    def bind_tools(
        self,
        tools: Sequence[dict[str, Any] | type | Callable[..., Any] | BaseTool],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        filtered = [
            candidate
            for candidate in tools
            if _tool_name(candidate) in self.allowed_tool_names
        ]
        return self.wrapped_model.bind_tools(
            filtered,
            tool_choice=tool_choice,
            **kwargs,
        )

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        return self.wrapped_model._generate(  # pyright: ignore[reportPrivateUsage]
            messages,
            stop=stop,
            run_manager=run_manager,
            **kwargs,
        )


def register_least_privilege_profile(model: BaseChatModel) -> None:
    provider, model_name = _model_identity(model)
    with _registration_lock:
        register_harness_profile(
            f"{provider}:{model_name}",
            LEAST_PRIVILEGE_PROFILE,
        )


def isolated_least_privilege_model(
    model: BaseChatModel,
    *,
    allowed_tool_names: frozenset[str],
) -> BaseChatModel:
    register_least_privilege_profile(model)
    _, model_name = _model_identity(model)
    return _AllowlistedChatModel(
        wrapped_model=model,
        allowed_tool_names=allowed_tool_names,
        model_name=model_name,
    )


def _model_identity(model: BaseChatModel) -> tuple[str, str]:
    params = _model_params(
        model._get_ls_params()  # pyright: ignore[reportPrivateUsage]
    )
    provider = params.get("ls_provider")
    model_name = params.get("ls_model_name")
    if not isinstance(provider, str) or not provider or ":" in provider:
        raise ValueError("model must expose a stable LangSmith provider identifier")
    if not isinstance(model_name, str) or not model_name or ":" in model_name:
        raise ValueError("model must expose a stable LangSmith model identifier")
    return provider, model_name


def _tool_name(
    candidate: dict[str, Any] | type | Callable[..., Any] | BaseTool,
) -> str | None:
    if isinstance(candidate, BaseTool):
        return candidate.name
    if isinstance(candidate, dict):
        tool_mapping = cast(Mapping[str, object], candidate)
        direct_name = tool_mapping.get("name")
        if isinstance(direct_name, str):
            return direct_name
        function = tool_mapping.get("function")
        if isinstance(function, Mapping):
            function_mapping = cast(Mapping[object, object], function)
            nested_name = function_mapping.get("name")
            return nested_name if isinstance(nested_name, str) else None
        return None
    name = getattr(candidate, "__name__", None)
    return name if isinstance(name, str) else None


def _model_params(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("model must expose LangSmith model parameters")
    result: dict[str, object] = {}
    for raw_key, raw_value in cast(Mapping[object, object], value).items():
        if not isinstance(raw_key, str):
            raise ValueError("model must expose string LangSmith parameter names")
        result[raw_key] = raw_value
    return result


__all__ = [
    "BLOCKED_DEEPAGENTS_TOOLS",
    "LEAST_PRIVILEGE_PROFILE",
    "isolated_least_privilege_model",
    "register_least_privilege_profile",
]
