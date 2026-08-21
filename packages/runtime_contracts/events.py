import json
from collections.abc import Mapping
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    Field,
    JsonValue,
    NonNegativeFloat,
    PositiveInt,
    field_serializer,
    field_validator,
)

from packages.runtime_contracts._immutable_json import freeze_json_object, thaw_json_object
from packages.runtime_contracts.identity import FrozenContract, Identifier
from packages.runtime_contracts.packages import AgentPackageRef, Version

MAX_EVENT_PAYLOAD_BYTES = 64 * 1024


class RuntimeEvent(FrozenContract):
    schema_version: Literal["runtime.event.v1"]
    event_id: Identifier
    sequence: PositiveInt
    occurred_at: AwareDatetime
    tenant_id: Identifier
    trace_id: Identifier
    span_id: Identifier
    parent_span_id: Identifier | None
    session_id: Identifier
    execution_id: Identifier
    package: AgentPackageRef
    worker_id: Identifier
    sdk_version: Version
    event_type: Annotated[str, Field(min_length=1, max_length=254, pattern=r"^[a-z][a-z0-9_.-]*$")]
    phase: Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_-]*$")]
    duration_ms: NonNegativeFloat | None
    payload: Mapping[str, JsonValue]
    payload_ref: Annotated[str, Field(min_length=1, max_length=2048)] | None

    @field_validator("payload")
    @classmethod
    def bound_payload(cls, value: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(encoded) > MAX_EVENT_PAYLOAD_BYTES:
            raise ValueError(f"event payload exceeds {MAX_EVENT_PAYLOAD_BYTES} bytes")
        return freeze_json_object(value)

    @field_serializer("payload")
    def serialize_payload(self, value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        return thaw_json_object(value)

    def to_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    def to_sse(self) -> str:
        return f"id: {self.sequence}\nevent: {self.event_type}\ndata: {self.to_json()}\n\n"


__all__ = ["MAX_EVENT_PAYLOAD_BYTES", "RuntimeEvent"]
