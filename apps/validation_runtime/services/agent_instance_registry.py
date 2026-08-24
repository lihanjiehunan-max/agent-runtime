from datetime import datetime, timezone
from uuid import uuid4

from apps.validation_runtime.domain import (
    AgentInstance,
    AgentInstanceRecord,
    AgentInstanceStatus,
    LoadedAgentAssets,
)


class AgentInstanceRegistry:
    def __init__(self) -> None:
        self._records_by_id: dict[str, AgentInstanceRecord] = {}
        self._instance_ids_by_digest: dict[str, str] = {}

    def deploy(self, assets: LoadedAgentAssets, graph: object) -> AgentInstance:
        current_instance_id = self._instance_ids_by_digest.get(assets.package_digest)
        if current_instance_id is not None:
            current_record = self._records_by_id[current_instance_id]
            if current_record.instance.status is AgentInstanceStatus.ACTIVE:
                return current_record.instance

        instance = AgentInstance(
            agent_instance_id=f"ain_{uuid4()}",
            agent_id=assets.manifest.agent_id,
            agent_version=assets.manifest.version,
            package_digest=assets.package_digest,
            runtime_type=assets.manifest.runtime,
            model_gateway_id=assets.manifest.model_gateway_id,
            model_alias=assets.gateway.model,
            skill_ids=assets.manifest.skills,
            status=AgentInstanceStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
        )
        self._records_by_id[instance.agent_instance_id] = AgentInstanceRecord(
            instance=instance,
            assets=assets,
            graph=graph,
        )
        self._instance_ids_by_digest[assets.package_digest] = instance.agent_instance_id
        return instance

    def get(self, instance_id: str) -> AgentInstance | None:
        record = self._records_by_id.get(instance_id)
        return record.instance if record is not None else None

    def get_record(self, instance_id: str) -> AgentInstanceRecord | None:
        return self._records_by_id.get(instance_id)

    def list(self) -> tuple[AgentInstance, ...]:
        return tuple(
            record.instance
            for _, record in sorted(self._records_by_id.items())
        )
