import pytest

from scripts.validate_streaming_service import parse_sse_lines, validate_turns


def test_parse_sse_lines_preserves_event_order_and_unicode():
    lines = [
        "event: delta",
        'data: {"content":"你"}',
        "",
        "event: delta",
        'data: {"content":"好"}',
        "",
        "event: done",
        'data: {"execution_id":"exe_1","turn_count":1}',
        "",
    ]

    events = list(parse_sse_lines(lines))

    assert events == [
        ("delta", {"content": "你"}),
        ("delta", {"content": "好"}),
        ("done", {"execution_id": "exe_1", "turn_count": 1}),
    ]


def test_validate_turns_accepts_memory_and_structured_analysis():
    turns = [
        [("delta", {"content": "已记住"}), ("done", {"turn_count": 1})],
        [
            ("delta", {"content": "你叫 Herry，负责散运"}),
            ("done", {"turn_count": 2}),
        ],
        [
            ("delta", {"content": "结论：关注收入。依据：暂无数据。建议：补数。"}),
            ("done", {"turn_count": 3}),
        ],
    ]

    validate_turns(turns, secrets_to_scan=("service-secret", "model-secret"))


def test_validate_turns_rejects_error_terminal_event():
    turns = [
        [("error", {"code": "MODEL_GATEWAY_ERROR", "message": "failed"})],
        [("delta", {"content": "Herry 散运"}), ("done", {"turn_count": 2})],
        [
            ("delta", {"content": "结论 依据 建议"}),
            ("done", {"turn_count": 3}),
        ],
    ]

    with pytest.raises(AssertionError, match="delta.*done"):
        validate_turns(turns, secrets_to_scan=())


def test_validate_turns_rejects_secret_in_payload():
    turns = [
        [("delta", {"content": "service-secret"}), ("done", {"turn_count": 1})],
        [("delta", {"content": "Herry 散运"}), ("done", {"turn_count": 2})],
        [
            ("delta", {"content": "结论 依据 建议"}),
            ("done", {"turn_count": 3}),
        ],
    ]

    with pytest.raises(AssertionError, match="secret"):
        validate_turns(turns, secrets_to_scan=("service-secret",))
