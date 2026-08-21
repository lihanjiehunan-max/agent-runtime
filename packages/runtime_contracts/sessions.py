from enum import StrEnum

from pydantic import AwareDatetime, NonNegativeInt

from packages.runtime_contracts.identity import CommandContract, FrozenContract, Identifier
from packages.runtime_contracts.packages import AgentPackageRef, Version


class SessionStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


class RuntimeSession(FrozenContract):
    session_id: Identifier
    thread_id: Identifier
    tenant_id: Identifier
    user_id: Identifier
    package: AgentPackageRef
    status: SessionStatus
    revision: NonNegativeInt
    execution_epoch: NonNegativeInt
    active_execution_id: Identifier | None
    last_checkpoint_id: Identifier | None
    last_event_sequence: NonNegativeInt
    created_at: AwareDatetime
    updated_at: AwareDatetime


class CreateSessionRequest(CommandContract):
    agent_id: Identifier
    version: Version


__all__ = ["CreateSessionRequest", "RuntimeSession", "SessionStatus"]
