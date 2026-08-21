from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import Lock
from types import MappingProxyType
from typing import Any, cast

from deepagents import (
    create_deep_agent as _create_deep_agent,  # pyright: ignore[reportUnknownVariableType]
)
from deepagents.backends.protocol import BackendProtocol
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph.state import CompiledStateGraph  # pyright: ignore[reportMissingTypeStubs]

from packages.deepagents_adapter.profile import isolated_least_privilege_model
from packages.deepagents_adapter.version import assert_compatible_deepagents
from packages.package_loader.schema import LoadedPackage

ToolReference = BaseTool | Callable[..., Any] | dict[str, Any]
CompiledAgentGraph = CompiledStateGraph[Any, Any, Any, Any]
CheckpointerReference = bool | BaseCheckpointSaver[Any] | None
_build_deep_agent = cast(Callable[..., CompiledAgentGraph], _create_deep_agent)


@dataclass(frozen=True, slots=True)
class AgentDefinition:
    package_digest: str
    graph: CompiledAgentGraph


class AgentFactory:
    def __init__(
        self,
        *,
        packages: Mapping[str, LoadedPackage],
        models: Mapping[str, BaseChatModel],
        tools: Mapping[str, ToolReference],
        checkpointer: CheckpointerReference,
        backend: BackendProtocol,
    ) -> None:
        self._packages = MappingProxyType(dict(packages))
        self._models = MappingProxyType(dict(models))
        self._tools = MappingProxyType(dict(tools))
        self._checkpointer = checkpointer
        self._backend = backend
        self._definitions: dict[str, AgentDefinition] = {}
        self._definition_lock = Lock()

    def definition(self, package_digest: str) -> AgentDefinition:
        cached = self._definitions.get(package_digest)
        if cached is not None:
            return cached

        with self._definition_lock:
            cached = self._definitions.get(package_digest)
            if cached is not None:
                return cached
            definition = self._build_definition(package_digest)
            self._definitions[package_digest] = definition
            return definition

    def get(self, package_digest: str) -> CompiledAgentGraph:
        return self.definition(package_digest).graph

    def get_limits(self, package_digest: str) -> object:
        try:
            return self._packages[package_digest].limits
        except KeyError as error:
            raise KeyError(f"unknown package digest: {package_digest}") from error

    def _build_definition(self, package_digest: str) -> AgentDefinition:
        assert_compatible_deepagents()
        try:
            package = self._packages[package_digest]
        except KeyError as error:
            raise KeyError(f"unknown package digest: {package_digest}") from error
        if package.reference.digest != package_digest:
            raise ValueError("package map key must match the authoritative package digest")

        try:
            model = self._models[package.agent.model.ref]
        except KeyError as error:
            raise KeyError(f"unknown model reference: {package.agent.model.ref}") from error

        allowed_tools: list[ToolReference] = []
        for binding in package.tool_bindings.allowlist:
            try:
                allowed_tools.append(self._tools[binding.name])
            except KeyError as error:
                raise KeyError(f"unregistered allowlisted tool: {binding.name}") from error

        isolated_model = isolated_least_privilege_model(
            model,
            allowed_tool_names=frozenset(
                binding.name for binding in package.tool_bindings.allowlist
            ),
        )
        graph = _build_deep_agent(
            model=isolated_model,
            tools=allowed_tools,
            system_prompt=package.system_prompt,
            subagents=[],
            skills=None,
            memory=None,
            backend=self._backend,
            checkpointer=self._checkpointer,
            name=package.agent.name,
        )
        return AgentDefinition(package_digest=package_digest, graph=graph)


__all__ = [
    "AgentDefinition",
    "AgentFactory",
    "CheckpointerReference",
    "CompiledAgentGraph",
    "ToolReference",
]
