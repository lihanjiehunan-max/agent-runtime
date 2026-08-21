import json
from collections.abc import Mapping
from enum import StrEnum

from pydantic import Field, JsonValue, field_serializer, field_validator

from packages.runtime_contracts._immutable_json import freeze_json_object, thaw_json_object
from packages.runtime_contracts.identity import FrozenContract

MAX_ERROR_DETAILS_BYTES = 16 * 1024


class ErrorCode(StrEnum):
    PACKAGE_NOT_FOUND = "PACKAGE_NOT_FOUND"
    DIGEST_MISMATCH = "DIGEST_MISMATCH"
    RUNTIME_INCOMPATIBLE = "RUNTIME_INCOMPATIBLE"
    SESSION_BUSY = "SESSION_BUSY"
    SESSION_CLOSED = "SESSION_CLOSED"
    EXECUTION_FENCED = "EXECUTION_FENCED"
    EXECUTION_TIMED_OUT = "EXECUTION_TIMED_OUT"
    EXECUTION_CANCELLED = "EXECUTION_CANCELLED"
    MODEL_ERROR = "MODEL_ERROR"
    TOOL_PERMISSION_DENIED = "TOOL_PERMISSION_DENIED"
    CHECKPOINT_RECOVERY_FAILED = "CHECKPOINT_RECOVERY_FAILED"
    AUTH_CONFIGURATION_REQUIRED = "AUTH_CONFIGURATION_REQUIRED"
    AUTHENTICATION_REQUIRED = "AUTHENTICATION_REQUIRED"
    INVALID_ACCESS_TOKEN = "INVALID_ACCESS_TOKEN"
    TENANT_IDENTITY_MISMATCH = "TENANT_IDENTITY_MISMATCH"


class RuntimeError(FrozenContract):
    schema_version: str = Field(default="runtime.error.v1", pattern=r"^runtime\.error\.v1$")
    code: ErrorCode
    message: str = Field(min_length=1, max_length=1024)
    retryable: bool = False
    details: Mapping[str, JsonValue] = Field(default_factory=dict, validate_default=True)

    @field_validator("details")
    @classmethod
    def bound_details(cls, value: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
        if len(encoded) > MAX_ERROR_DETAILS_BYTES:
            raise ValueError(f"error details exceed {MAX_ERROR_DETAILS_BYTES} bytes")
        return freeze_json_object(value)

    @field_serializer("details")
    def serialize_details(self, value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        return thaw_json_object(value)

    def to_body(self) -> dict[str, object]:
        return {"error": self.model_dump(mode="json")}


__all__ = ["ErrorCode", "RuntimeError"]
