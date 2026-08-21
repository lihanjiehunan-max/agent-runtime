from __future__ import annotations

import json
from collections.abc import Callable, Generator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.metadata import version
from inspect import signature
from pathlib import Path
from threading import Thread
from typing import Any, ClassVar, cast

import pytest
from deepagents import (
    create_deep_agent as _create_deep_agent,  # pyright: ignore[reportUnknownVariableType]
)
from deepagents.backends import StateBackend
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from packages.deepagents_adapter.factory import AgentFactory
from packages.deepagents_adapter.version import DEEPAGENTS_VERSION
from packages.model_gateway.client import ModelGatewayClient, ModelGatewayConfig
from packages.package_loader.service import PackageLoader
from packages.runtime_contracts import AgentPackageRef, RuntimeType

PACKAGE_DIGEST = "sha256:dc94787f5a91816d0cde36dfe6e83dd198d005819f8ef6f74587e961dbbfbb04"


class _OpenAIStubHandler(BaseHTTPRequestHandler):
    requests: ClassVar[list[dict[str, Any]]] = []
    authorizations: ClassVar[list[str]] = []

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers["content-length"])
        body = cast(dict[str, Any], json.loads(self.rfile.read(length)))
        self.requests.append(body)
        self.authorizations.append(self.headers.get("authorization", ""))

        if len(self.requests) == 1:
            response = {
                "id": "provider-tool-turn",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "tool_calls",
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call-query-metric",
                                    "type": "function",
                                    "function": {
                                        "name": "query_metric",
                                        "arguments": json.dumps({"metric": "revenue"}),
                                    },
                                }
                            ],
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 3,
                    "total_tokens": 13,
                },
            }
        else:
            response = {
                "id": "provider-final-turn",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": "Revenue is 42.",
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 18,
                    "completion_tokens": 4,
                    "total_tokens": 22,
                },
            }

        encoded = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.send_header("x-request-id", f"stub-{len(self.requests)}")
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:
        del format, args


@pytest.fixture
def openai_stub() -> Generator[tuple[str, type[_OpenAIStubHandler]], None, None]:
    _OpenAIStubHandler.requests = []
    _OpenAIStubHandler.authorizations = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OpenAIStubHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}/v1", _OpenAIStubHandler
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@tool
def query_metric(metric: str) -> str:
    """Return a deterministic metric value from the local test gateway."""
    return f"{metric}=42"


def test_installed_deepagents_surface_is_pinned_to_077() -> None:
    create_deep_agent = cast(Callable[..., object], _create_deep_agent)
    parameters = signature(create_deep_agent).parameters

    assert version("deepagents") == "0.7.7"
    assert DEEPAGENTS_VERSION == "0.7.7"
    assert "backend" in parameters
    assert "checkpointer" in parameters


@pytest.mark.filterwarnings(
    "ignore:The v3 streaming protocol.*:langchain_core._api.beta_decorator.LangChainBetaWarning"
)
def test_real_package_tool_turn_exposes_event_streaming_v3_surface(
    openai_stub: tuple[str, type[_OpenAIStubHandler]],
) -> None:
    base_url, handler = openai_stub
    reference = AgentPackageRef(
        tenant_id="tenant-a",
        agent_id="agent-metric-query",
        version="0.1.0",
        digest=PACKAGE_DIGEST,
        runtime_type=RuntimeType.DEEPAGENTS,
        sdk_version="0.7.7",
    )
    loaded = PackageLoader.local(
        Path("agents"),
        tenant_id="tenant-a",
        expected_packages={("agent-metric-query", "0.1.0"): reference},
    ).load("agent-metric-query", "0.1.0")
    gateway = ModelGatewayClient(
        ModelGatewayConfig(
            base_url=base_url,
            api_key="local-stub-key",
            model_name="local-stub-model",
        )
    )
    try:
        graph = AgentFactory(
            packages={PACKAGE_DIGEST: loaded},
            models={loaded.agent.model.ref: gateway.chat_model},
            tools={"query_metric": query_metric},
            checkpointer=InMemorySaver(),
            backend=StateBackend(),
        ).get(PACKAGE_DIGEST)

        run = cast(Any, graph).stream_events(
            {"messages": [{"role": "user", "content": "What is revenue?"}]},
            config={"configurable": {"thread_id": "session-v3-contract"}},
            version="v3",
        )
        interleaved = list(run.interleave("messages", "values"))
        message_streams = [item for name, item in interleaved if name == "messages"]
        values = [item for name, item in interleaved if name == "values"]
        output = run.output
    finally:
        gateway.close()

    tool_calls = [
        tool_call
        for message_stream in message_streams
        for tool_call in message_stream.tool_calls.get()
    ]
    assert len(handler.requests) == 2
    assert all(request["model"] == "local-stub-model" for request in handler.requests)
    assert all(auth == "Bearer local-stub-key" for auth in handler.authorizations)
    assert [tool_spec["function"]["name"] for tool_spec in handler.requests[0]["tools"]] == [
        "query_metric"
    ]
    assert [tool_call["name"] for tool_call in tool_calls] == ["query_metric"]
    assert values
    assert output is not None
    assert output["messages"][-1].text == "Revenue is 42."
