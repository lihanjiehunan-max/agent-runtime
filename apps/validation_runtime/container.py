from apps.validation_runtime.config import RuntimeConfig
from apps.validation_runtime.domain import AgentInstance
from apps.validation_runtime.services.agent_factory import AgentFactory
from apps.validation_runtime.services.agent_instance_registry import AgentInstanceRegistry
from apps.validation_runtime.services.agent_router import AgentRouter
from apps.validation_runtime.services.asset_loader import AssetLoader
from apps.validation_runtime.services.model_factory import ModelFactory
from apps.validation_runtime.services.session_manager import SessionManager
from apps.validation_runtime.services.streaming_chat_service import StreamingChatService


class RuntimeContainer:
    def __init__(
        self,
        config: RuntimeConfig,
        *,
        agent_factory: AgentFactory | object | None = None,
    ) -> None:
        self.config = config
        self.assets = AssetLoader(config.asset_root)
        self.registry = AgentInstanceRegistry()
        self.router = AgentRouter(self.registry)
        self.sessions = SessionManager(self.router, self.registry)
        self.chat = StreamingChatService(self.registry, self.sessions)
        self.agent_factory = agent_factory or AgentFactory(ModelFactory())

    def deploy(self, agent_id: str) -> AgentInstance:
        assets = self.assets.load_agent(agent_id)
        graph = self.agent_factory.build(assets, self.config.model_api_key)
        return self.registry.deploy(assets, graph)
