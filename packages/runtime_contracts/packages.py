from enum import StrEnum
from typing import Annotated

from pydantic import StringConstraints

from packages.runtime_contracts.identity import FrozenContract, Identifier

Version = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=254,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._+-]*$",
    ),
]
Sha256Digest = Annotated[
    str,
    StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$"),
]


class RuntimeType(StrEnum):
    DEEPAGENTS = "deepagents"


class AgentPackageRef(FrozenContract):
    tenant_id: Identifier
    agent_id: Identifier
    version: Version
    digest: Sha256Digest
    runtime_type: RuntimeType
    sdk_version: Version


__all__ = ["AgentPackageRef", "RuntimeType", "Sha256Digest", "Version"]
