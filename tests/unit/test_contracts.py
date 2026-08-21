from collections.abc import Mapping, MutableMapping, MutableSequence
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

import pytest
from pydantic import JsonValue, ValidationError

from packages.runtime_contracts import (
    AgentPackageRef,
    CreateExecutionRequest,
    CreateSessionRequest,
    ErrorCode,
    ExecutionMode,
    ExecutionStatus,
    Principal,
    RuntimeError,
    RuntimeEvent,
    RuntimeExecution,
    RuntimeSession,
    RuntimeType,
    SessionStatus,
)

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
SESSION_ID = str(uuid4())
EXECUTION_ID = "01K36W3K0B7Y0Q9A4Q2V8Z5M1N"


def package_ref(**overrides: object) -> AgentPackageRef:
    values: dict[str, object] = {
        "tenant_id": "tenant-a",
        "agent_id": "agent-metric-query",
        "version": "0.1.0",
        "digest": f"sha256:{'a' * 64}",
        "runtime_type": RuntimeType.DEEPAGENTS,
        "sdk_version": "0.7.7",
    }
    values.update(overrides)
    return AgentPackageRef.model_validate(values)


def runtime_event(**overrides: object) -> RuntimeEvent:
    values: dict[str, object] = {
        "schema_version": "runtime.event.v1",
        "event_id": str(uuid4()),
        "sequence": 7,
        "occurred_at": NOW,
        "tenant_id": "tenant-a",
        "trace_id": str(uuid4()),
        "span_id": "01K36W3K0B7Y0Q9A4Q2V8Z5M1P",
        "parent_span_id": None,
        "session_id": SESSION_ID,
        "execution_id": EXECUTION_ID,
        "package": package_ref(),
        "worker_id": "worker-01",
        "sdk_version": "0.7.7",
        "event_type": "execution.started",
        "phase": "running",
        "duration_ms": None,
        "payload": {"attempt": 1},
        "payload_ref": None,
    }
    values.update(overrides)
    return RuntimeEvent.model_validate(values)


@pytest.mark.parametrize("safe_id", [str(uuid4()), "01K36W3K0B7Y0Q9A4Q2V8Z5M1Q"])
def test_contract_identifiers_accept_uuid_and_ulid_safe_values(safe_id: str) -> None:
    principal = Principal(
        tenant_id=safe_id,
        user_id="user-01",
        actor_id="actor-01",
        worker_id=None,
        permissions=(),
    )

    assert principal.tenant_id == safe_id


@pytest.mark.parametrize("unsafe_id", ["tenant/escape", "x" * 255])
def test_contract_identifiers_reject_unsafe_or_255_character_values(unsafe_id: str) -> None:
    with pytest.raises(ValidationError):
        Principal(
            tenant_id=unsafe_id,
            user_id="user-01",
            actor_id="actor-01",
            worker_id=None,
            permissions=(),
        )


def test_identity_contracts_are_frozen_and_reject_unknown_fields() -> None:
    ref = package_ref()

    with pytest.raises(ValidationError):
        ref.agent_id = "other-agent"
    with pytest.raises(ValidationError):
        package_ref(unexpected="not-part-of-the-contract")


def test_session_and_execution_snapshots_are_immutable() -> None:
    session = RuntimeSession(
        session_id=SESSION_ID,
        thread_id=SESSION_ID,
        tenant_id="tenant-a",
        user_id="user-01",
        package=package_ref(),
        status=SessionStatus.OPEN,
        revision=0,
        execution_epoch=0,
        active_execution_id=None,
        last_checkpoint_id=None,
        last_event_sequence=0,
        created_at=NOW,
        updated_at=NOW,
    )
    execution = RuntimeExecution(
        execution_id=EXECUTION_ID,
        session_id=SESSION_ID,
        tenant_id="tenant-a",
        user_id="user-01",
        actor_id="actor-01",
        worker_id=None,
        trace_id=str(uuid4()),
        execution_epoch=1,
        mode=ExecutionMode.SYNC,
        status=ExecutionStatus.ACCEPTED,
        created_at=NOW,
        started_at=None,
        completed_at=None,
    )

    with pytest.raises(ValidationError):
        session.revision = 1
    with pytest.raises(ValidationError):
        execution.status = ExecutionStatus.RUNNING


def test_event_version_is_required_and_timestamp_must_be_timezone_aware() -> None:
    values = runtime_event().model_dump()
    values.pop("schema_version")

    with pytest.raises(ValidationError):
        RuntimeEvent.model_validate(values)
    with pytest.raises(ValidationError):
        runtime_event(schema_version="runtime.event.v2")
    with pytest.raises(ValidationError):
        runtime_event(occurred_at=datetime(2026, 8, 20, 12, 0))


def test_event_payload_is_bounded() -> None:
    with pytest.raises(ValidationError):
        runtime_event(payload={"value": "x" * (64 * 1024)})


def test_event_payload_is_deeply_immutable_and_detached_from_input() -> None:
    source: dict[str, JsonValue] = {
        "nested": {"items": [{"value": "original"}]},
    }
    event = runtime_event(payload=source)

    with pytest.raises(TypeError):
        cast(MutableMapping[str, JsonValue], event.payload)["added"] = True
    nested = cast(Mapping[str, JsonValue], event.payload["nested"])
    with pytest.raises(TypeError):
        cast(MutableMapping[str, JsonValue], nested)["added"] = True
    items = cast(tuple[JsonValue, ...], nested["items"])
    with pytest.raises(TypeError):
        cast(MutableSequence[JsonValue], items)[0] = None

    source_nested = cast(dict[str, JsonValue], source["nested"])
    source_items = cast(list[JsonValue], source_nested["items"])
    source_items.append({"value": "external mutation"})

    assert event.model_dump(mode="json")["payload"] == {
        "nested": {"items": [{"value": "original"}]},
    }


def test_event_json_and_sse_envelopes_are_deterministic() -> None:
    event_id = str(uuid4())
    trace_id = str(uuid4())
    event_a = runtime_event(
        event_id=event_id,
        trace_id=trace_id,
        payload={"z": 1, "a": "value"},
    )
    event_b = runtime_event(
        event_id=event_id,
        trace_id=trace_id,
        payload={"a": "value", "z": 1},
    )

    assert event_a.to_json() == event_b.to_json()
    assert event_a.to_sse() == (
        f"id: {event_a.sequence}\n"
        f"event: {event_a.event_type}\n"
        f"data: {event_a.to_json()}\n\n"
    )


@pytest.mark.parametrize("field", ["tenant_id", "user_id", "actor_id", "worker_id"])
@pytest.mark.parametrize(
    ("request_type", "payload"),
    [
        (
            CreateSessionRequest,
            {
                "agent_id": "agent-metric-query",
                "version": "0.1.0",
            },
        ),
        (CreateExecutionRequest, {"input": "What is revenue?", "mode": "sync"}),
    ],
)
def test_request_contracts_reject_caller_supplied_identity(
    field: str,
    request_type: type[CreateSessionRequest] | type[CreateExecutionRequest],
    payload: dict[str, str],
) -> None:
    with pytest.raises(ValidationError):
        request_type.model_validate({**payload, field: "forged-identity"})


def test_runtime_error_has_stable_versioned_body_and_required_codes() -> None:
    required_codes = {
        "PACKAGE_NOT_FOUND",
        "DIGEST_MISMATCH",
        "RUNTIME_INCOMPATIBLE",
        "SESSION_BUSY",
        "SESSION_CLOSED",
        "EXECUTION_FENCED",
        "EXECUTION_TIMED_OUT",
        "EXECUTION_CANCELLED",
        "MODEL_ERROR",
        "TOOL_PERMISSION_DENIED",
        "CHECKPOINT_RECOVERY_FAILED",
    }
    error = RuntimeError(
        code=ErrorCode.SESSION_BUSY,
        message="Session already has an active execution",
    )

    assert required_codes <= {code.value for code in ErrorCode}
    assert error.to_body() == {
        "error": {
            "schema_version": "runtime.error.v1",
            "code": "SESSION_BUSY",
            "message": "Session already has an active execution",
            "retryable": False,
            "details": {},
        }
    }


def test_error_details_are_deeply_immutable_and_detached_from_input() -> None:
    source: dict[str, JsonValue] = {
        "nested": {"items": [{"value": "original"}]},
    }
    error = RuntimeError(
        code=ErrorCode.MODEL_ERROR,
        message="Model request failed",
        details=source,
    )

    with pytest.raises(TypeError):
        cast(MutableMapping[str, JsonValue], error.details)["added"] = True
    nested = cast(Mapping[str, JsonValue], error.details["nested"])
    with pytest.raises(TypeError):
        cast(MutableMapping[str, JsonValue], nested)["added"] = True
    items = cast(tuple[JsonValue, ...], nested["items"])
    with pytest.raises(TypeError):
        cast(MutableSequence[JsonValue], items)[0] = None

    source_nested = cast(dict[str, JsonValue], source["nested"])
    source_items = cast(list[JsonValue], source_nested["items"])
    source_items.append({"value": "external mutation"})

    assert error.to_body()["error"] == {
        "schema_version": "runtime.error.v1",
        "code": "MODEL_ERROR",
        "message": "Model request failed",
        "retryable": False,
        "details": {"nested": {"items": [{"value": "original"}]}},
    }


def test_default_error_details_are_immutable() -> None:
    error = RuntimeError(
        code=ErrorCode.MODEL_ERROR,
        message="Model request failed",
    )

    with pytest.raises(TypeError):
        cast(MutableMapping[str, JsonValue], error.details)["added"] = True
