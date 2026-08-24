from typing import Literal

from pydantic import BaseModel, ConfigDict

from apps.validation_runtime.domain import AgentInstance, RuntimeSession


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"


class AgentInstanceResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    agent_instance_id: str
    agent_id: str
    version: str
    package_digest: str
    model_alias: str
    skill_ids: tuple[str, ...]
    status: str

    @classmethod
    def from_domain(cls, instance: AgentInstance) -> "AgentInstanceResponse":
        return cls(
            agent_instance_id=instance.agent_instance_id,
            agent_id=instance.agent_id,
            version=instance.agent_version,
            package_digest=instance.package_digest,
            model_alias=instance.model_alias,
            skill_ids=instance.skill_ids,
            status=instance.status.value,
        )


class BusinessSessionResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    session_id: str
    agent_id: str
    status: str
    turn_count: int

    @classmethod
    def from_domain(cls, session: RuntimeSession) -> "BusinessSessionResponse":
        return cls(
            session_id=session.session_id,
            agent_id=session.agent_id,
            status=session.status.value,
            turn_count=session.turn_count,
        )


class ChatRequest(BaseModel):
    message: str
