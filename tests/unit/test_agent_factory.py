from __future__ import annotations

import json
import traceback
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from deepagents import GeneralPurposeSubagentProfile, HarnessProfile, register_harness_profile
from deepagents.backends import StateBackend
from langchain.agents.middleware import AgentMiddleware
from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from langchain_core.language_models import (
    BaseChatModel,
    LangSmithParams,
    LanguageModelInput,
)
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool, tool
from langchain_core.utils.function_calling import convert_to_openai_tool
from langgraph.checkpoint.memory import InMemorySaver

from packages.deepagents_adapter.factory import AgentFactory
from packages.model_gateway.client import (
    ModelGatewayClient,
    ModelGatewayConfig,
    ModelGatewayError,
    ModelGatewayErrorCode,
)
from packages.package_loader.schema import LoadedPackage
from packages.package_loader.service import PackageLoader
from packages.runtime_contracts import AgentPackageRef, RuntimeType

PACKAGE_DIGEST = "sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04"


class RecordingChatModel(BaseChatModel):
    model_name: str = "factory-test-model"
    seen_tool_names: list[tuple[str, ...]] = []

    @property
    def _llm_type(self) -> str:
        return "task6-recording-model"

    def _get_ls_params(
        self,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> LangSmithParams:
        del stop, kwargs
        return {
            "ls_provider": "task6-test",
            "ls_model_name": self.model_name,
            "ls_model_type": "chat",
        }

    def bind_tools(
        self,
        tools: Sequence[dict[str, Any] | type | Callable[..., Any] | BaseTool],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        formatted = [convert_to_openai_tool(candidate) for candidate in tools]
        return self.bind(tools=formatted, tool_choice=tool_choice, **kwargs)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        del messages, stop, run_manager
        tool_names = tuple(tool_spec["function"]["name"] for tool_spec in kwargs.get("tools", []))
        self.seen_tool_names.append(tool_names)
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="done"))])


@tool
def query_metric(metric: str) -> str:
    """Return a deterministic metric value for factory tests."""
    return f"{metric}=42"


@tool
def unregistered_network(url: str) -> str:
    """Represent a tool that must never be visible to the model."""
    return url


class ConflictingNetworkMiddleware(AgentMiddleware[Any, Any, Any]):
    tools = [unregistered_network]


def _loaded_package() -> LoadedPackage:
    reference = AgentPackageRef(
        tenant_id="tenant-a",
        agent_id="agent-metric-query",
        version="0.1.0",
        digest=PACKAGE_DIGEST,
        runtime_type=RuntimeType.DEEPAGENTS,
        sdk_version="0.7.7",
    )
    return PackageLoader.local(
        Path("agents"),
        tenant_id="tenant-a",
        expected_packages={("agent-metric-query", "0.1.0"): reference},
    ).load("agent-metric-query", "0.1.0")


def _factory(model: RecordingChatModel | None = None) -> AgentFactory:
    loaded = _loaded_package()
    selected_model = model or RecordingChatModel()
    return AgentFactory(
        packages={PACKAGE_DIGEST: loaded},
        models={loaded.agent.model.ref: selected_model},
        tools={
            "query_metric": query_metric,
            "unregistered_network": unregistered_network,
        },
        checkpointer=InMemorySaver(),
        backend=StateBackend(),
    )


def test_factory_caches_one_immutable_definition_per_package_digest() -> None:
    factory = _factory()

    first = factory.definition(PACKAGE_DIGEST)
    second = factory.definition(PACKAGE_DIGEST)

    assert first is second
    assert first.package_digest == PACKAGE_DIGEST
    assert factory.get(PACKAGE_DIGEST) is first.graph
    assert set(first.__dataclass_fields__) == {"package_digest", "graph"}


def test_model_request_sees_only_the_package_allowlisted_tool() -> None:
    model = RecordingChatModel()
    graph = _factory(model).get(PACKAGE_DIGEST)

    cast(Any, graph).invoke(
        {"messages": [{"role": "user", "content": "What is revenue?"}]},
        config={"configurable": {"thread_id": "session-visible-tools"}},
    )

    assert model.seen_tool_names == [("query_metric",)]
    visible = set(model.seen_tool_names[0])
    assert visible.isdisjoint(
        {
            "execute",
            "ls",
            "read_file",
            "write_file",
            "edit_file",
            "delete",
            "glob",
            "grep",
            "write_todos",
            "task",
            "start_async_task",
            "unregistered_network",
        }
    )


def test_graph_tool_snapshot_survives_later_conflicting_profile_registration() -> None:
    model_name = "profile-snapshot-model"
    first_model = RecordingChatModel(model_name=model_name)
    first_graph = _factory(first_model).get(PACKAGE_DIGEST)

    register_harness_profile(
        f"task6-test:{model_name}",
        HarnessProfile(
            extra_middleware=(ConflictingNetworkMiddleware(),),
            general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=True),
        ),
    )

    cast(Any, first_graph).invoke(
        {"messages": [{"role": "user", "content": "What is revenue?"}]},
        config={"configurable": {"thread_id": "existing-profile-snapshot"}},
    )
    rebuilt_model = RecordingChatModel(model_name=model_name)
    rebuilt_graph = _factory(rebuilt_model).get(PACKAGE_DIGEST)
    cast(Any, rebuilt_graph).invoke(
        {"messages": [{"role": "user", "content": "What is revenue?"}]},
        config={"configurable": {"thread_id": "rebuilt-profile-snapshot"}},
    )

    assert first_model.seen_tool_names == [("query_metric",)]
    assert rebuilt_model.seen_tool_names == [("query_metric",)]


def test_model_gateway_uses_server_config_and_normalizes_usage_and_correlation() -> None:
    requests: list[httpx.Request] = []

    def provider(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"x-request-id": "provider-request-7"},
            json={
                "id": "completion-7",
                "model": "provider-model",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "Revenue is 42."},
                    }
                ],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 4,
                    "total_tokens": 15,
                },
            },
        )

    config = ModelGatewayConfig(
        base_url="https://model-gateway.internal/v1",
        api_key="server-only-secret",
        model_name="configured-model",
        timeout_seconds=3.0,
    )
    with httpx.Client(transport=httpx.MockTransport(provider)) as http_client:
        gateway = ModelGatewayClient(config, http_client=http_client)
        response = gateway.chat_model.invoke("What is revenue?")

    assert len(requests) == 1
    request = requests[0]
    payload = cast(dict[str, object], json.loads(request.content))
    assert payload["model"] == "configured-model"
    assert request.headers["authorization"] == "Bearer server-only-secret"
    assert request.headers["x-request-id"]
    assert response.content == "Revenue is 42."
    assert response.usage_metadata == {
        "input_tokens": 11,
        "output_tokens": 4,
        "total_tokens": 15,
    }
    assert response.response_metadata["correlation_id"] == request.headers["x-request-id"]
    assert response.response_metadata["provider_request_id"] == "provider-request-7"
    assert "server-only-secret" not in repr(config)
    assert "server-only-secret" not in repr(gateway)


def test_model_gateway_normalizes_timeout_without_logging_authorization(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "timeout-server-secret"

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout(f"provider timed out with {secret}", request=request)

    config = ModelGatewayConfig(
        base_url="https://model-gateway.internal/v1",
        api_key=secret,
        model_name="configured-model",
        timeout_seconds=0.25,
    )
    with httpx.Client(transport=httpx.MockTransport(timeout)) as http_client:
        gateway = ModelGatewayClient(config, http_client=http_client)
        with pytest.raises(ModelGatewayError) as caught:
            gateway.chat_model.invoke("What is revenue?")

    assert caught.value.code is ModelGatewayErrorCode.TIMEOUT
    assert caught.value.correlation_id
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    traceback_text = "".join(
        traceback.format_exception(caught.type, caught.value, caught.tb)
    )
    assert secret not in str(caught.value)
    assert secret not in traceback_text
    assert secret not in caplog.text
    assert "authorization" not in caplog.text.lower()


def test_model_gateway_normalizes_provider_error_without_logging_body(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "provider-body-secret"

    def unavailable(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            request=request,
            json={"error": {"message": f"upstream unavailable: {secret}"}},
        )

    config = ModelGatewayConfig(
        base_url="https://model-gateway.internal/v1",
        api_key="configured-key",
        model_name="configured-model",
    )
    with httpx.Client(transport=httpx.MockTransport(unavailable)) as http_client:
        gateway = ModelGatewayClient(config, http_client=http_client)
        with pytest.raises(ModelGatewayError) as caught:
            gateway.chat_model.invoke("What is revenue?")

    assert caught.value.code is ModelGatewayErrorCode.PROVIDER_ERROR
    assert caught.value.status_code == 503
    assert caught.value.correlation_id
    assert secret not in str(caught.value)
    assert secret not in caplog.text


def test_model_gateway_config_reads_credentials_from_server_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_BASE_URL", "https://gateway.example/v1/")
    monkeypatch.setenv("MODEL_GATEWAY_API_KEY", "environment-secret")
    monkeypatch.setenv("MODEL_GATEWAY_MODEL", "environment-model")
    monkeypatch.setenv("MODEL_GATEWAY_TIMEOUT_SECONDS", "12.5")

    config = ModelGatewayConfig.from_env()

    assert config.base_url == "https://gateway.example/v1/"
    assert config.model_name == "environment-model"
    assert config.timeout_seconds == 12.5
    assert "environment-secret" not in repr(config)


@pytest.mark.parametrize(
    "override",
    [
        {"api_key": "request-key"},
        {"authorization": "Bearer request-key"},
        {"headers": {"Authorization": "Bearer request-key"}},
    ],
    ids=["api-key", "authorization", "headers"],
)
def test_model_gateway_rejects_request_time_credential_and_header_overrides(
    override: dict[str, Any],
) -> None:
    requests: list[httpx.Request] = []

    def provider(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500, request=request)

    config = ModelGatewayConfig(
        base_url="https://model-gateway.internal/v1",
        api_key="configured-key",
        model_name="configured-model",
    )
    with httpx.Client(transport=httpx.MockTransport(provider)) as http_client:
        gateway = ModelGatewayClient(config, http_client=http_client)
        with pytest.raises(
            ValueError,
            match="credentials and headers are server-configured",
        ):
            gateway.chat_model.invoke("What is revenue?", **override)

    assert requests == []


def test_model_gateway_owned_http_client_disables_environment_trust() -> None:
    config = ModelGatewayConfig(
        base_url="https://model-gateway.internal/v1",
        api_key="configured-key",
        model_name="configured-model",
    )
    gateway = ModelGatewayClient(config)
    try:
        assert cast(Any, gateway)._http_client.trust_env is False
    finally:
        gateway.close()


def test_model_gateway_normalizes_malformed_provider_response() -> None:
    def malformed(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, json={"unexpected": "shape"})

    config = ModelGatewayConfig(
        base_url="https://model-gateway.internal/v1",
        api_key="configured-key",
        model_name="configured-model",
    )
    with httpx.Client(transport=httpx.MockTransport(malformed)) as http_client:
        gateway = ModelGatewayClient(config, http_client=http_client)
        with pytest.raises(ModelGatewayError) as caught:
            gateway.chat_model.invoke("What is revenue?")

    assert caught.value.code is ModelGatewayErrorCode.PROVIDER_ERROR
    assert caught.value.status_code == 200
    assert caught.value.correlation_id


def test_model_gateway_normalizes_malformed_tool_call_arguments() -> None:
    def malformed(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            json={
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call-invalid",
                                    "type": "function",
                                    "function": {
                                        "name": "query_metric",
                                        "arguments": "{not-json}",
                                    },
                                }
                            ],
                        }
                    }
                ]
            },
        )

    config = ModelGatewayConfig(
        base_url="https://model-gateway.internal/v1",
        api_key="configured-key",
        model_name="configured-model",
    )
    with httpx.Client(transport=httpx.MockTransport(malformed)) as http_client:
        gateway = ModelGatewayClient(config, http_client=http_client)
        with pytest.raises(ModelGatewayError) as caught:
            gateway.chat_model.invoke("What is revenue?")

    assert caught.value.code is ModelGatewayErrorCode.PROVIDER_ERROR
    assert caught.value.status_code == 200
    assert caught.value.correlation_id


def test_model_gateway_normalizes_malformed_usage() -> None:
    def malformed(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=request,
            json={
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "Revenue is 42.",
                        }
                    }
                ],
                "usage": {
                    "prompt_tokens": "eleven",
                    "completion_tokens": 4,
                    "total_tokens": 15,
                },
            },
        )

    config = ModelGatewayConfig(
        base_url="https://model-gateway.internal/v1",
        api_key="configured-key",
        model_name="configured-model",
    )
    with httpx.Client(transport=httpx.MockTransport(malformed)) as http_client:
        gateway = ModelGatewayClient(config, http_client=http_client)
        with pytest.raises(ModelGatewayError) as caught:
            gateway.chat_model.invoke("What is revenue?")

    assert caught.value.code is ModelGatewayErrorCode.PROVIDER_ERROR
    assert caught.value.status_code == 200
    assert caught.value.correlation_id
