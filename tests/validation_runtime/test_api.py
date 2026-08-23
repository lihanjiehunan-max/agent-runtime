import json
from pathlib import Path

import pytest
from pydantic import SecretStr

from apps.validation_runtime.api import create_app
from apps.validation_runtime.config import RuntimeConfig
from apps.validation_runtime.errors import RuntimeServiceError


FORBIDDEN_BUSINESS_KEYS = {
    "agent_instance_id",
    "bound_agent_instance_id",
    "thread_id",
    "package_digest",
    "skill_ids",
    "model_alias",
    "model_gateway_id",
    "gateway",
    "graph",
    "prompt",
    "api_key",
}


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer wrong"},
        {"Authorization": "Basic service-secret"},
    ],
)
def test_runtime_api_requires_exact_bearer_token(client, headers):
    response = client.get("/api/v1/runtime/ops/agent-instances", headers=headers)

    assert response.status_code == 401
    assert response.json() == {
        "error": {
            "code": "AUTHENTICATION_REQUIRED",
            "message": "Authentication is required",
        }
    }
    assert "service-secret" not in response.text


def test_health_is_public_and_reveals_only_liveness(client):
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_deploy_is_explicit_idempotent_and_listable(client, auth_headers):
    first = client.post(
        "/api/v1/runtime/ops/agents/shipping-analyst/deploy",
        headers=auth_headers,
    )
    second = client.post(
        "/api/v1/runtime/ops/agents/shipping-analyst/deploy",
        headers=auth_headers,
    )
    listed = client.get(
        "/api/v1/runtime/ops/agent-instances",
        headers=auth_headers,
    )

    assert first.status_code == 200
    assert first.json() == second.json()
    assert set(first.json()) == {
        "agent_instance_id",
        "agent_id",
        "version",
        "package_digest",
        "model_alias",
        "skill_ids",
        "status",
    }
    assert first.json()["agent_instance_id"].startswith("ain_")
    assert first.json()["model_alias"] == "gpt-5.6-sol"
    assert first.json()["skill_ids"] == ["shipping-operations-analyst"]
    assert listed.json() == [first.json()]
    assert "token.zero-api" not in listed.text
    assert "secret" not in listed.text
    assert "graph" not in listed.text


def test_session_creation_requires_deployment(client, auth_headers):
    response = client.post(
        "/api/v1/runtime/agents/shipping-analyst/sessions",
        headers=auth_headers,
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "AGENT_INSTANCE_UNAVAILABLE"


def test_business_session_response_is_an_explicit_safe_projection(
    client, auth_headers
):
    _deploy(client, auth_headers)

    response = client.post(
        "/api/v1/runtime/agents/shipping-analyst/sessions",
        headers=auth_headers,
    )

    assert response.status_code == 201
    assert set(response.json()) == {"session_id", "agent_id", "status", "turn_count"}
    assert response.json()["session_id"].startswith("ses_")
    assert response.json()["status"] == "IDLE"
    assert_business_safe(response.json())


def test_chat_returns_exact_sse_headers_and_business_safe_events(
    client, auth_headers
):
    _deploy(client, auth_headers)
    session = _create_session(client, auth_headers)

    with client.stream(
        "POST",
        f"/api/v1/runtime/sessions/{session['session_id']}/chat",
        headers=auth_headers,
        json={"message": "hello"},
    ) as response:
        body = response.read().decode()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"
    events = parse_sse(body)
    assert [event[0] for event in events] == ["delta", "delta", "done"]
    assert "".join(event[1]["content"] for event in events[:-1]) == "你好"
    assert events[-1][1]["turn_count"] == 1
    assert events[-1][1]["execution_id"].startswith("exe_")
    for _, data in events:
        assert_business_safe(data)
    assert "model-secret" not in body
    assert "service-secret" not in body


@pytest.mark.parametrize(
    ("path", "payload", "status", "code"),
    [
        ("/api/v1/runtime/sessions/ses_missing/chat", {"message": "hello"}, 404, "SESSION_NOT_FOUND"),
        ("/api/v1/runtime/sessions/ses_missing/chat", {"message": " "}, 404, "SESSION_NOT_FOUND"),
        ("/api/v1/runtime/sessions/ses_missing/chat", {}, 422, "INVALID_MESSAGE"),
    ],
)
def test_pre_stream_errors_are_normalized_json(
    client, auth_headers, path, payload, status, code
):
    response = client.post(path, headers=auth_headers, json=payload)

    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["error"]["code"] == code


def test_busy_session_returns_json_409(client, auth_headers, runtime_container):
    _deploy(client, auth_headers)
    session = _create_session(client, auth_headers)
    runtime_container.sessions.begin_stream(session["session_id"])

    response = client.post(
        f"/api/v1/runtime/sessions/{session['session_id']}/chat",
        headers=auth_headers,
        json={"message": "hello"},
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "SESSION_BUSY"
    runtime_container.sessions.mark_failed(session["session_id"])


def test_missing_service_key_rejects_application_creation():
    config = RuntimeConfig(
        asset_root=Path("runtime_assets"),
        model_api_key=SecretStr("model-secret"),
        service_api_key=None,
    )

    with pytest.raises(RuntimeServiceError) as exc:
        create_app(config=config)

    assert exc.value.detail.code == "SERVICE_API_KEY_MISSING"


def test_openapi_keeps_business_and_operations_schemas_separate(
    client, auth_headers
):
    schema = client.get("/openapi.json").json()
    business_schema = schema["components"]["schemas"]["BusinessSessionResponse"]
    operation_schema = schema["components"]["schemas"]["AgentInstanceResponse"]

    assert not (set(business_schema["properties"]) & FORBIDDEN_BUSINESS_KEYS)
    assert "agent_instance_id" in operation_schema["properties"]
    assert "base_url" not in operation_schema["properties"]
    assert "api_key" not in operation_schema["properties"]


def _deploy(client, headers):
    response = client.post(
        "/api/v1/runtime/ops/agents/shipping-analyst/deploy",
        headers=headers,
    )
    assert response.status_code == 200
    return response.json()


def _create_session(client, headers):
    response = client.post(
        "/api/v1/runtime/agents/shipping-analyst/sessions",
        headers=headers,
    )
    assert response.status_code == 201
    return response.json()


def parse_sse(body):
    events = []
    for block in body.strip().split("\n\n"):
        lines = block.splitlines()
        assert len(lines) == 2
        assert lines[0].startswith("event: ")
        assert lines[1].startswith("data: ")
        events.append(
            (lines[0].removeprefix("event: "), json.loads(lines[1].removeprefix("data: ")))
        )
    return events


def assert_business_safe(value):
    if isinstance(value, dict):
        assert not (set(value) & FORBIDDEN_BUSINESS_KEYS)
        for child in value.values():
            assert_business_safe(child)
    elif isinstance(value, list):
        for child in value:
            assert_business_safe(child)
