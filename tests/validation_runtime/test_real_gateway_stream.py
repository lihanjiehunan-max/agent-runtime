import os

import pytest
from fastapi.testclient import TestClient

from apps.validation_runtime.api import create_app
from apps.validation_runtime.config import RuntimeConfig
from scripts.validate_streaming_service import parse_sse_lines


@pytest.mark.real_gateway
@pytest.mark.skipif(
    not os.getenv("MODEL_API_KEY") or not os.getenv("SERVICE_API_KEY"),
    reason="MODEL_API_KEY and/or SERVICE_API_KEY not configured",
)
def test_real_gateway_streams_delta_before_done():
    config = RuntimeConfig.from_env()
    headers = {
        "Authorization": f"Bearer {config.service_api_key.get_secret_value()}"
    }

    with TestClient(create_app(config=config)) as client:
        deployed = client.post(
            "/api/v1/runtime/ops/agents/shipping-analyst/deploy",
            headers=headers,
        )
        assert deployed.status_code == 200
        created = client.post(
            "/api/v1/runtime/agents/shipping-analyst/sessions",
            headers=headers,
        )
        assert created.status_code == 201
        session_id = created.json()["session_id"]
        with client.stream(
            "POST",
            f"/api/v1/runtime/sessions/{session_id}/chat",
            headers=headers,
            json={"message": "用一句话说明你能提供什么帮助。"},
        ) as response:
            assert response.status_code == 200
            events = list(parse_sse_lines(response.iter_lines()))

    assert [event for event, _ in events][-1] == "done"
    assert any(event == "delta" and data["content"] for event, data in events)
