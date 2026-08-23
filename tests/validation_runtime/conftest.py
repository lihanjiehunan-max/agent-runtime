from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from apps.validation_runtime.api import create_app
from apps.validation_runtime.config import RuntimeConfig
from apps.validation_runtime.container import RuntimeContainer
from tests.validation_runtime.fakes import RecordingStreamingGraph


class StaticAgentFactory:
    def __init__(self, graph) -> None:
        self.graph = graph

    def build(self, assets, key):
        return self.graph


@pytest.fixture
def graph():
    return RecordingStreamingGraph(["你", "好"])


@pytest.fixture
def runtime_config():
    return RuntimeConfig(
        asset_root=Path("runtime_assets"),
        model_api_key=SecretStr("model-secret"),
        service_api_key=SecretStr("service-secret"),
    )


@pytest.fixture
def runtime_container(runtime_config, graph):
    return RuntimeContainer(
        runtime_config,
        agent_factory=StaticAgentFactory(graph),
    )


@pytest.fixture
def client(runtime_config, runtime_container):
    with TestClient(create_app(container=runtime_container, config=runtime_config)) as value:
        yield value


@pytest.fixture
def auth_headers():
    return {"Authorization": "Bearer service-secret"}
