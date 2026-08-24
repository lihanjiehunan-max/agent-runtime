from apps.validation_runtime.domain import AgentInstance, AgentInstanceStatus, LogicalAgent
from apps.validation_runtime.errors import RuntimeServiceError
from apps.validation_runtime.services.agent_instance_registry import AgentInstanceRegistry


class AgentRouter:
    def __init__(self, registry: AgentInstanceRegistry) -> None:
        self._registry = registry

    def resolve(self, agent_id: str) -> AgentInstance:
        active_instances = self._active_instances(agent_id)
        if not active_instances:
            raise RuntimeServiceError(
                "AGENT_INSTANCE_UNAVAILABLE",
                f"Agent '{agent_id}' does not have an active instance.",
            )
        if len(active_instances) > 1:
            raise RuntimeServiceError(
                "AGENT_ROUTING_AMBIGUOUS",
                f"Agent '{agent_id}' has multiple active instances.",
            )
        return active_instances[0]

    def list_logical_agents(self) -> tuple[LogicalAgent, ...]:
        instances_by_agent_id: dict[str, list[AgentInstance]] = {}
        for instance in self._registry.list():
            instances_by_agent_id.setdefault(instance.agent_id, []).append(instance)

        return tuple(
            LogicalAgent(
                agent_id=agent_id,
                display_name=agent_id,
                available=len(self._active_instances(agent_id)) == 1,
            )
            for agent_id in sorted(instances_by_agent_id)
        )

    def _active_instances(self, agent_id: str) -> list[AgentInstance]:
        return [
            instance
            for instance in self._registry.list()
            if instance.agent_id == agent_id
            and instance.status is AgentInstanceStatus.ACTIVE
        ]
