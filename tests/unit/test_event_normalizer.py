from __future__ import annotations

import json
from typing import cast

import pytest
from pydantic import JsonValue

from packages.event_normalizer.deepagents_v3 import (
    MAX_NORMALIZED_TEXT_CHARS,
    DeepAgentsV3Normalizer,
)
from packages.runtime_contracts.events import MAX_EVENT_PAYLOAD_BYTES

EMPTY_METADATA: dict[str, object] = {}


def _raw(method: str, data: object) -> dict[str, object]:
    return {"method": method, "params": {"namespace": [], "data": data}}


def test_normalizes_actual_deepagents_077_v3_model_tool_values_and_output_shapes() -> None:
    normalizer = DeepAgentsV3Normalizer()
    raw_events = [
        _raw(
            "messages",
            (
                {"event": "message-start", "role": "ai", "id": "model-1"},
                {"run_id": "run-1", "langgraph_node": "model"},
            ),
        ),
        _raw(
            "messages",
            (
                {
                    "event": "content-block-delta",
                    "index": 0,
                    "delta": {"type": "text-delta", "text": "Revenue is 42."},
                },
                EMPTY_METADATA,
            ),
        ),
        _raw(
            "messages",
            (
                {
                    "event": "message-finish",
                    "metadata": {
                        "finish_reason": "stop",
                        "input_tokens": 11,
                        "output_tokens": 4,
                    },
                },
                EMPTY_METADATA,
            ),
        ),
        _raw(
            "tools",
            {
                "event": "tool-started",
                "tool_call_id": "call-1",
                "tool_name": "query_metric",
                "input": {"metric": "revenue", "period": "本月", "org": "散运公司"},
            },
        ),
        _raw(
            "tools",
            {
                "event": "tool-finished",
                "tool_call_id": "call-1",
                "output": {"value": "1280000.00", "unit": "CNY"},
            },
        ),
        _raw("values", {"messages": [{"type": "human"}, {"type": "ai"}], "files": {}}),
        _raw("output", {"messages": [{"type": "ai", "content": "Revenue is 42."}]}),
    ]

    normalized = [
        item
        for raw_event in raw_events
        for item in normalizer.normalize(raw_event)
    ]

    assert [item.event_type for item in normalized] == [
        "model.started",
        "model.delta",
        "model.completed",
        "tool.started",
        "tool.completed",
        "execution.values",
        "execution.output",
    ]
    assert normalized[1].payload["text"] == "Revenue is 42."
    assert normalized[3].payload["tool_name"] == "query_metric"
    assert normalized[4].payload["output"] == {"value": "1280000.00", "unit": "CNY"}
    assert normalized[-2].payload["message_count"] == 2
    assert normalized[-1].payload["message_count"] == 1


def test_aggregates_text_into_bounded_redacted_chunks_and_omits_hidden_reasoning() -> None:
    normalizer = DeepAgentsV3Normalizer()
    secret_text = (
        "Bearer provider-secret "
        + "x" * (MAX_NORMALIZED_TEXT_CHARS * 3)
    )
    raw_events = [
        _raw(
            "messages",
            (
                {
                    "event": "content-block-delta",
                    "index": 0,
                    "delta": {"type": "text-delta", "text": secret_text},
                },
                EMPTY_METADATA,
            ),
        ),
        _raw(
            "messages",
            (
                {
                    "event": "content-block-delta",
                    "index": 1,
                    "delta": {
                        "type": "reasoning-delta",
                        "text": "hidden reasoning must never be persisted",
                    },
                },
                EMPTY_METADATA,
            ),
        ),
    ]

    normalized = [
        item
        for raw_event in raw_events
        for item in normalizer.normalize(raw_event)
    ]

    assert normalized
    assert all(len(str(item.payload["text"])) <= MAX_NORMALIZED_TEXT_CHARS for item in normalized)
    encoded = json.dumps(
        [item.payload for item in normalized],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    assert len(encoded) < MAX_EVENT_PAYLOAD_BYTES
    serialized = json.dumps([item.payload for item in normalized], ensure_ascii=False)
    assert "provider-secret" not in serialized
    assert "hidden reasoning" not in serialized


def test_malformed_and_unknown_events_become_bounded_runtime_errors() -> None:
    normalizer = DeepAgentsV3Normalizer()

    malformed = normalizer.normalize({"method": "messages", "params": {"data": None}})
    unknown = normalizer.normalize(
        _raw("new-sdk-event", {"message": "Bearer should not leak"})
    )

    assert len(malformed) == 1
    assert malformed[0].event_type == "runtime.error"
    assert malformed[0].payload["code"] == "MALFORMED_STREAM_EVENT"
    assert len(unknown) == 1
    assert unknown[0].event_type == "runtime.error"
    assert unknown[0].payload["code"] == "UNKNOWN_STREAM_EVENT"
    assert "Bearer" not in json.dumps(unknown[0].payload)


def test_tool_output_and_completion_are_distinct_events() -> None:
    normalizer = DeepAgentsV3Normalizer()

    output = normalizer.normalize(
        _raw(
            "tools",
            {
                "event": "tool-output",
                "tool_call_id": "call-1",
                "tool_name": "query_metric",
                "output": {"value": "1280000.00"},
            },
        )
    )
    completion = normalizer.normalize(
        _raw(
            "tools",
            {
                "event": "tool-finished",
                "tool_call_id": "call-1",
                "tool_name": "query_metric",
            },
        )
    )

    assert [item.event_type for item in output] == ["tool.output"]
    assert [item.event_type for item in completion] == ["tool.completed"]


def test_normalizes_observed_tool_output_delta_and_data_delta_shapes() -> None:
    normalizer = DeepAgentsV3Normalizer()

    tool_delta = normalizer.normalize(
        _raw(
            "tools",
            {
                "event": "tool-output-delta",
                "tool_call_id": "call-1",
                "tool_name": "query_metric",
                "data": {
                    "value": "partial",
                    "api_key": "must-not-persist",
                    "token_count": 3,
                },
            },
        )
    )
    data_delta = normalizer.normalize(
        _raw(
            "data-delta",
            {
                "tool_call_id": "call-1",
                "tool_name": "query_metric",
                "data": {"value": "partial-2"},
            },
        )
    )

    assert [item.event_type for item in tool_delta] == ["tool.output"]
    assert [item.event_type for item in data_delta] == ["tool.output"]
    assert tool_delta[0].payload["output"] == {
        "value": "partial",
        "api_key": "[REDACTED]",
        "token_count": 3,
    }
    assert data_delta[0].payload["output"] == {"value": "partial-2"}


def test_normalizes_pinned_tool_output_delta_and_preserves_output_data_forms() -> None:
    normalizer = DeepAgentsV3Normalizer()

    normalized = [
        normalizer.normalize(
            _raw(
                "tools",
                {
                    "event": "tool-output-delta",
                    "tool_call_id": "call-1",
                    "tool_name": "query_metric",
                    field: {"value": f"partial-{field}"},
                },
            )
        )[0]
        for field in ("delta", "output", "data")
    ]

    assert [item.event_type for item in normalized] == [
        "tool.output",
        "tool.output",
        "tool.output",
    ]
    assert [item.payload["output"] for item in normalized] == [
        {"value": "partial-delta"},
        {"value": "partial-output"},
        {"value": "partial-data"},
    ]


def test_redacts_sensitive_values_inside_quoted_json_without_hiding_safe_token_fields() -> None:
    normalizer = DeepAgentsV3Normalizer()
    text = json.dumps(
        {
            "api_key": "api-key-value",
            "password": "password-value",
            "authorization": "Bearer authorization-value",
            "access_token": "access-token-value",
            "secret": "secret-value",
            "token": "token-value",
            "token_count": 3,
            "token_id": "token-id-safe",
        },
        separators=(",", ":"),
    )

    normalized = normalizer.normalize(
        _raw(
            "messages",
            (
                {
                    "event": "content-block-delta",
                    "index": 0,
                    "delta": {"type": "text-delta", "text": text},
                },
                EMPTY_METADATA,
            ),
        )
    )

    redacted_text = "".join(str(item.payload["text"]) for item in normalized)
    for secret in (
        "api-key-value",
        "password-value",
        "authorization-value",
        "access-token-value",
        "secret-value",
        "token-value",
    ):
        assert secret not in redacted_text
    assert '"token_count":3' in redacted_text
    assert '"token_id":"token-id-safe"' in redacted_text


@pytest.mark.parametrize(
    ("sensitive_key", "secret_value"),
    [
        ("client_secret", "client-secret-value"),
        ("refresh_token", "refresh-token-value"),
        ("credential", "credential-value"),
    ],
)
def test_redacts_additional_quoted_sensitive_keys_without_hiding_safe_token_fields(
    sensitive_key: str,
    secret_value: str,
) -> None:
    normalizer = DeepAgentsV3Normalizer()
    text = json.dumps(
        {
            sensitive_key: secret_value,
            "token_count": 3,
            "token_id": "token-id-safe",
        },
        separators=(",", ":"),
    )

    normalized = normalizer.normalize(
        _raw(
            "messages",
            (
                {
                    "event": "content-block-delta",
                    "index": 0,
                    "delta": {"type": "text-delta", "text": text},
                },
                EMPTY_METADATA,
            ),
        )
    )

    redacted_text = "".join(str(item.payload["text"]) for item in normalized)
    assert secret_value not in redacted_text
    assert f'"{sensitive_key}":"[REDACTED]"' in redacted_text
    assert '"token_count":3' in redacted_text
    assert '"token_id":"token-id-safe"' in redacted_text


def test_normalizes_messages_error_to_stable_runtime_error_without_provider_text() -> None:
    normalizer = DeepAgentsV3Normalizer()

    normalized = normalizer.normalize(
        _raw(
            "messages",
            (
                {
                    "event": "error",
                    "error": "provider said Bearer provider-secret",
                },
                EMPTY_METADATA,
            ),
        )
    )

    assert normalized[0].event_type == "runtime.error"
    assert normalized[0].payload == {"code": "DEEPAGENTS_RUNTIME_ERROR"}
    assert "provider-secret" not in json.dumps(normalized[0].payload)


def test_extracts_top_level_and_nested_message_usage_without_leaking_hidden_reasoning() -> None:
    normalizer = DeepAgentsV3Normalizer()
    usage_event: dict[str, object] = {
        "event": "message-finish",
        "usage": {
            "input_tokens": 7,
            "output_tokens": 2,
            "total_tokens": 9,
        },
    }

    top_level = normalizer.normalize(
        _raw(
            "messages",
            (usage_event, EMPTY_METADATA),
        )
    )
    nested_state = normalizer.normalize(
        _raw(
            "values",
            {
                "messages": [
                    {"type": "human", "content": "visible"},
                    {"type": "reasoning", "content": "hidden-reasoning-secret"},
                ],
                "token_count": 4,
                "access_token": "must-not-persist",
            },
        )
    )

    assert top_level[0].payload["input_tokens"] == 7
    assert top_level[0].payload["output_tokens"] == 2
    assert top_level[0].payload["total_tokens"] == 9
    serialized = json.dumps(nested_state[0].payload, ensure_ascii=False)
    assert "hidden-reasoning-secret" not in serialized
    assert "access_token" not in serialized


def test_recursively_redacts_sensitive_tool_keys_without_redacting_safe_counts() -> None:
    normalizer = DeepAgentsV3Normalizer()

    output = normalizer.normalize(
        _raw(
            "tools",
            {
                "event": "tool-output",
                "output": {
                    "nested": {
                        "Authorization": "Bearer provider-secret",
                        "secret": "nested-secret",
                        "token_count": 3,
                        "safe": "visible",
                    }
                },
            },
        )
    )

    tool_output = cast(dict[str, JsonValue], output[0].payload["output"])
    nested = cast(dict[str, JsonValue], tool_output["nested"])
    assert nested["Authorization"] == "[REDACTED]"
    assert nested["secret"] == "[REDACTED]"
    assert nested["token_count"] == 3
    assert nested["safe"] == "visible"
