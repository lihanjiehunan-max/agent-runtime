from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, ConfigDict


class ModelGatewayProfile(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    provider: Literal["openai-compatible"]
    base_url: AnyHttpUrl
    model: str
    api_mode: Literal["chat_completions"]
    api_key_env: Literal["MODEL_API_KEY"]
    connect_timeout_seconds: int
    read_timeout_seconds: int


class AgentLimits(BaseModel):
    model_config = ConfigDict(frozen=True)

    execution_timeout_seconds: int
    max_input_characters: int


class AgentManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: Literal["1.0"]
    agent_id: str
    version: str
    runtime: Literal["deepagents-python"]
    model_gateway_id: str
    skills: tuple[str, ...]
    limits: AgentLimits


class LoadedSkill(BaseModel):
    model_config = ConfigDict(frozen=True)

    skill_id: str
    instructions: str
    source_bytes: bytes


class LoadedAgentAssets(BaseModel):
    model_config = ConfigDict(frozen=True)

    manifest: AgentManifest
    gateway: ModelGatewayProfile
    skills: tuple[LoadedSkill, ...]
    package_digest: str


class AgentInstanceStatus(StrEnum):
    ACTIVE = "ACTIVE"
    FAILED = "FAILED"


class AgentInstance(BaseModel):
    model_config = ConfigDict(frozen=True)

    agent_instance_id: str
    agent_id: str
    agent_version: str
    package_digest: str
    runtime_type: str
    model_gateway_id: str
    model_alias: str
    skill_ids: tuple[str, ...]
    status: AgentInstanceStatus
    created_at: datetime


class LogicalAgent(BaseModel):
    model_config = ConfigDict(frozen=True)

    agent_id: str
    display_name: str
    available: bool


class SessionStatus(StrEnum):
    IDLE = "IDLE"
    STREAMING = "STREAMING"
    CLOSED = "CLOSED"


class RuntimeSession(BaseModel):
    model_config = ConfigDict(frozen=True)

    session_id: str
    thread_id: str
    agent_id: str
    bound_agent_instance_id: str
    package_digest: str
    status: SessionStatus
    turn_count: int
    created_at: datetime
    updated_at: datetime


class ChatDelta(BaseModel):
    model_config = ConfigDict(frozen=True)

    event: Literal["delta"] = "delta"
    content: str


class ChatDone(BaseModel):
    model_config = ConfigDict(frozen=True)

    event: Literal["done"] = "done"
    execution_id: str
    turn_count: int


class ChatError(BaseModel):
    model_config = ConfigDict(frozen=True)

    event: Literal["error"] = "error"
    code: str
    message: str


ChatEvent = ChatDelta | ChatDone | ChatError


@dataclass(frozen=True)
class AgentInstanceRecord:
    instance: AgentInstance
    assets: LoadedAgentAssets
    graph: object
