from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal

from pydantic import Field, PositiveInt, field_validator

from packages.runtime_contracts import AgentPackageRef
from packages.runtime_contracts.identity import FrozenContract, Identifier
from packages.runtime_contracts.packages import Sha256Digest, Version

PACKAGE_SCHEMA_VERSION = "agent.package.v1"
DEEPAGENTS_SDK_VERSION = "0.7.7"


class PackageErrorCode(StrEnum):
    PACKAGE_NOT_FOUND = "PACKAGE_NOT_FOUND"
    AUTHORITATIVE_REFERENCE_REQUIRED = "PACKAGE_REFERENCE_REQUIRED"
    FILE_MISSING = "PACKAGE_FILE_MISSING"
    FILE_TOO_LARGE = "PACKAGE_FILE_TOO_LARGE"
    INVALID_PATH = "PACKAGE_INVALID_PATH"
    SYMLINK_FORBIDDEN = "PACKAGE_SYMLINK_FORBIDDEN"
    CHECKSUM_INVALID = "PACKAGE_CHECKSUM_INVALID"
    CHECKSUM_DUPLICATE = "PACKAGE_CHECKSUM_DUPLICATE"
    CHECKSUM_MISSING = "PACKAGE_CHECKSUM_MISSING"
    CHECKSUM_MISMATCH = "PACKAGE_CHECKSUM_MISMATCH"
    DIGEST_MISMATCH = "DIGEST_MISMATCH"
    MANIFEST_INVALID = "PACKAGE_MANIFEST_INVALID"
    SCHEMA_UNSUPPORTED = "PACKAGE_SCHEMA_UNSUPPORTED"
    RUNTIME_UNSUPPORTED = "RUNTIME_INCOMPATIBLE"
    SDK_INCOMPATIBLE = "SDK_INCOMPATIBLE"
    TOOL_BINDING_UNKNOWN = "TOOL_BINDING_UNKNOWN"
    PACKAGE_STATE_INCOMPATIBLE = "PACKAGE_STATE_INCOMPATIBLE"


class PackageLoadError(Exception):
    def __init__(self, code: PackageErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class ManifestRuntime(FrozenContract):
    type: Literal["deepagents"]
    sdk_version: Literal["0.7.7"]


class ManifestFiles(FrozenContract):
    agent: str
    tool_bindings: str
    backend: str
    limits: str
    observability: str
    system_prompt: str
    runtime: str


class PackageManifest(FrozenContract):
    schema_version: Literal["agent.package.v1"]
    agent_id: Identifier
    version: Version
    digest: Sha256Digest
    status: Literal["active"]
    runtime: ManifestRuntime
    files: ManifestFiles


class ModelReference(FrozenContract):
    ref: Annotated[str, Field(min_length=1, max_length=2048)]


class AgentDefinition(FrozenContract):
    schema_version: Literal["agent.definition.v1"]
    name: Annotated[str, Field(min_length=1, max_length=254)]
    model: ModelReference


class ToolBinding(FrozenContract):
    name: Literal["query_metric"]
    effect: Literal["read_only"]


class ToolBindings(FrozenContract):
    schema_version: Literal["agent.tool-bindings.v1"]
    allowlist: tuple[ToolBinding, ...]

    @field_validator("allowlist")
    @classmethod
    def require_unique_tools(cls, value: tuple[ToolBinding, ...]) -> tuple[ToolBinding, ...]:
        names = [binding.name for binding in value]
        if len(names) != len(set(names)):
            raise ValueError("tool allowlist entries must be unique")
        return value


class BackendDefinition(FrozenContract):
    schema_version: Literal["agent.backend.v1"]
    type: Literal["state"]


class PackageLimits(FrozenContract):
    schema_version: Literal["agent.limits.v1"]
    session_ttl_minutes: PositiveInt
    execution_timeout_seconds: PositiveInt
    max_model_calls: PositiveInt
    max_tool_calls: PositiveInt
    max_tokens: PositiveInt


class ObservabilityDefinition(FrozenContract):
    schema_version: Literal["agent.observability.v1"]
    emit_runtime_events: bool


class DeepAgentsRuntime(FrozenContract):
    schema_version: Literal["agent.runtime.deepagents.v1"]
    type: Literal["deepagents"]
    sdk_version: Literal["0.7.7"]


class ChecksumEntry(FrozenContract):
    path: str
    digest: Sha256Digest


class LoadedPackage(FrozenContract):
    reference: AgentPackageRef
    root: Path
    manifest: PackageManifest
    agent: AgentDefinition
    tool_bindings: ToolBindings
    backend: BackendDefinition
    limits: PackageLimits
    observability: ObservabilityDefinition
    runtime: DeepAgentsRuntime
    system_prompt: str
    checksums: tuple[ChecksumEntry, ...]


class PackageEventContext(FrozenContract):
    trace_id: Identifier
    parent_span_id: Identifier | None
    session_id: Identifier
    execution_id: Identifier
    worker_id: Identifier
    sequence_start: PositiveInt = 1


def freeze_package_refs(
    refs: dict[tuple[str, str], AgentPackageRef] | None,
) -> MappingProxyType[tuple[str, str], AgentPackageRef]:
    return MappingProxyType(dict(refs or {}))


__all__ = [
    "AgentDefinition",
    "BackendDefinition",
    "ChecksumEntry",
    "DEEPAGENTS_SDK_VERSION",
    "DeepAgentsRuntime",
    "LoadedPackage",
    "ManifestFiles",
    "ManifestRuntime",
    "ModelReference",
    "ObservabilityDefinition",
    "PACKAGE_SCHEMA_VERSION",
    "PackageErrorCode",
    "PackageEventContext",
    "PackageLimits",
    "PackageLoadError",
    "PackageManifest",
    "ToolBinding",
    "ToolBindings",
]
