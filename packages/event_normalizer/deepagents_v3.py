from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from pydantic import JsonValue

from packages.runtime_contracts.events import MAX_EVENT_PAYLOAD_BYTES

MAX_NORMALIZED_TEXT_CHARS = 4096
_MAX_SAFE_DEPTH = 4
_MAX_SAFE_ITEMS = 64
_REDACTION_PATTERNS = (
    (re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+"), "Bearer [REDACTED]"),
    (
        re.compile(
            r"(?i)(api[_-]?key|authorization|access[_-]?token|password|secret|token)"
            r"(\s*[:=]\s*)[^\s,;]+"
        ),
        r"\1\2[REDACTED]",
    ),
)
_QUOTED_JSON_SECRET_PATTERN = re.compile(
    r'(?i)(?P<prefix>"(?:x[-_ ]?api[-_ ]?key|proxy[-_ ]?authorization|'
    r'api[_-]?key|authorization|access[_-]?token|client[_-]?secret|credential|'
    r'password|refresh[_-]?token|secret|token)"\s*:\s*)'
    r'"(?P<value>(?:\\.|[^"\\])*)"'
)
_HIDDEN_BLOCK_TYPES = frozenset(
    {
        "reasoning",
        "reasoning-delta",
        "reasoning-content-delta",
        "thinking",
        "thinking-delta",
        "analysis",
        "redacted_thinking",
    }
)
_SENSITIVE_KEY_NAMES = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "authorization",
        "client_secret",
        "credential",
        "password",
        "refresh_token",
        "secret",
        "token",
    }
)


@dataclass(frozen=True, slots=True)
class NormalizedEvent:
    """A bounded event projection before runtime identity and sequence are attached."""

    event_type: str
    phase: str
    payload: Mapping[str, JsonValue]
    span_id: str | None = None
    parent_span_id: str | None = None


class DeepAgentsV3Normalizer:
    """Normalize the installed Deep Agents 0.7.7/LangGraph v3 protocol."""

    def normalize(self, raw_event: object) -> tuple[NormalizedEvent, ...]:
        raw = _as_string_mapping(raw_event)
        if raw is None:
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        method = raw.get("method")
        params = _as_string_mapping(raw.get("params"))
        if not isinstance(method, str) or params is None:
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        data = params.get("data")
        try:
            if method == "messages":
                return self._messages(data)
            if method == "tools":
                return self._tools(data)
            if method in {"data-delta", "tool-output-delta"}:
                return self._tool_delta(data)
            if method in {"data", "custom"}:
                data_mapping = _as_string_mapping(data)
                if data_mapping is not None and data_mapping.get("event") in {
                    "data-delta",
                    "tool-output-delta",
                }:
                    return self._tool_delta(data_mapping)
            if method == "values":
                return (self._state_event("execution.values", data),)
            if method == "output":
                return (self._state_event("execution.output", data),)
            if method in {"error", "runtime-error", "runtime_error"}:
                return (self._runtime_error("DEEPAGENTS_RUNTIME_ERROR"),)
        except (TypeError, ValueError, KeyError):
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        return (self._runtime_error("UNKNOWN_STREAM_EVENT"),)

    def normalize_exception(self, _error: BaseException) -> NormalizedEvent:
        """Turn an SDK/provider exception into a stable, secret-free projection."""

        return self._runtime_error("DEEPAGENTS_RUNTIME_ERROR")

    def _messages(self, data: object) -> tuple[NormalizedEvent, ...]:
        if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        items = list(cast(Sequence[object], data))
        if len(items) != 2:
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        event = _as_string_mapping(items[0])
        metadata = _as_string_mapping(items[1])
        if event is None or metadata is None:
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        event_name = event.get("event")
        if not isinstance(event_name, str):
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        if event_name == "error":
            return (self._runtime_error("DEEPAGENTS_RUNTIME_ERROR"),)
        span_id = _safe_identifier(event.get("id")) or _safe_identifier(metadata.get("run_id"))
        if event_name == "message-start":
            return (
                NormalizedEvent(
                    event_type="model.started",
                    phase="model",
                    span_id=span_id,
                    payload=self._payload(
                        {
                            "role": _safe_string(event.get("role")),
                            "message_id": _safe_string(event.get("id")),
                        }
                    ),
                ),
            )
        if event_name == "content-block-delta":
            return self._message_delta(event, span_id)
        if event_name == "message-finish":
            finish_metadata = _as_string_mapping(event.get("metadata")) or {}
            payload: dict[str, JsonValue] = {
                "finish_reason": _safe_string(
                    event.get("finish_reason", finish_metadata.get("finish_reason"))
                ),
            }
            payload.update(
                _usage_payload(
                    event.get("usage"),
                    finish_metadata.get("usage"),
                    metadata.get("usage"),
                    event,
                    finish_metadata,
                    metadata,
                )
            )
            return (
                NormalizedEvent(
                    event_type="model.completed",
                    phase="model",
                    span_id=span_id,
                    payload=self._payload(payload),
                ),
            )
        if event_name in {"content-block-start", "content-block-finish"}:
            return ()
        return (self._runtime_error("UNKNOWN_STREAM_EVENT"),)

    def _message_delta(
        self,
        event: Mapping[str, object],
        span_id: str | None,
    ) -> tuple[NormalizedEvent, ...]:
        delta = _as_string_mapping(event.get("delta"))
        if delta is None:
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        delta_type = delta.get("type")
        if not isinstance(delta_type, str):
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        if delta_type in _HIDDEN_BLOCK_TYPES:
            return ()
        if delta_type == "text-delta":
            text = _redact_text(_safe_string(delta.get("text")))
            return tuple(
                NormalizedEvent(
                    event_type="model.delta",
                    phase="model",
                    span_id=span_id,
                    payload=self._payload({"text": chunk}),
                )
                for chunk in _chunks(text)
                if chunk
            )
        if delta_type == "block-delta":
            fields = _as_string_mapping(delta.get("fields"))
            if fields is None:
                return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
            if fields.get("type") in _HIDDEN_BLOCK_TYPES:
                return ()
            if fields.get("type") == "tool_call_chunk":
                payload = {
                    "content_type": "tool_call",
                    "tool_call_id": _safe_string(fields.get("id")),
                    "tool_name": _safe_string(fields.get("name")),
                    "args": _redact_text(_safe_string(fields.get("args"))),
                }
                return (
                    NormalizedEvent(
                        event_type="model.delta",
                        phase="model",
                        span_id=span_id,
                        payload=self._payload(payload),
                    ),
                )
        return (self._runtime_error("UNKNOWN_STREAM_EVENT"),)

    def _tools(self, data: object) -> tuple[NormalizedEvent, ...]:
        data_mapping = _as_string_mapping(data)
        if data_mapping is None:
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        event_name = data_mapping.get("event")
        if not isinstance(event_name, str):
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        common = {
            "tool_call_id": _safe_string(data_mapping.get("tool_call_id")),
            "tool_name": _safe_string(data_mapping.get("tool_name")),
        }
        if event_name in {"tool-started", "tool-start"}:
            return (
                NormalizedEvent(
                    event_type="tool.started",
                    phase="tool",
                    payload=self._payload(
                        {"input": _safe_json(data_mapping.get("input")), **common}
                    ),
                ),
            )
        if event_name in {
            "tool-output",
            "tool-chunk",
            "tool-stream",
            "tool-output-delta",
            "data-delta",
        }:
            output = data_mapping.get(
                "delta",
                data_mapping.get("output", data_mapping.get("data")),
            )
            return (
                NormalizedEvent(
                    event_type="tool.output",
                    phase="tool",
                    payload=self._payload(
                        {"output": _safe_json(output), **common}
                    ),
                ),
            )
        if event_name in {"tool-finished", "tool-completed"}:
            payload: dict[str, JsonValue] = dict(common)
            if "output" in data_mapping:
                payload["output"] = _safe_json(data_mapping.get("output"))
            return (
                NormalizedEvent(
                    event_type="tool.completed",
                    phase="tool",
                    payload=self._payload(payload),
                ),
            )
        if event_name in {"tool-error", "tool-failed"}:
            return (
                NormalizedEvent(
                    event_type="tool.failed",
                    phase="tool",
                    payload=self._payload({
                        **common,
                        "error_code": "TOOL_RUNTIME_ERROR",
                    }),
                ),
            )
        return (self._runtime_error("UNKNOWN_STREAM_EVENT"),)

    def _tool_delta(self, data: object) -> tuple[NormalizedEvent, ...]:
        data_mapping = _as_string_mapping(data)
        if data_mapping is None:
            return (self._runtime_error("MALFORMED_STREAM_EVENT"),)
        return self._tools(
            {
                **data_mapping,
                "event": "tool-output-delta",
            }
        )

    def _state_event(self, event_type: str, data: object) -> NormalizedEvent:
        summary = _summarize_state(data)
        return NormalizedEvent(
            event_type=event_type,
            phase="state",
            payload=self._payload(summary),
        )

    def _runtime_error(self, code: str) -> NormalizedEvent:
        return NormalizedEvent(
            event_type="runtime.error",
            phase="runtime",
            payload={"code": code},
        )

    @staticmethod
    def _payload(value: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        if len(encoded) <= MAX_EVENT_PAYLOAD_BYTES:
            return value
        return {"truncated": True, "payload_bytes": len(encoded)}


def _chunks(value: str) -> tuple[str, ...]:
    return tuple(
        value[index : index + MAX_NORMALIZED_TEXT_CHARS]
        for index in range(0, len(value), MAX_NORMALIZED_TEXT_CHARS)
    )


def _safe_identifier(value: object) -> str | None:
    candidate = _safe_string(value)
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,253}", candidate):
        return candidate
    return None


def _safe_string(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return _redact_text(value)[:MAX_NORMALIZED_TEXT_CHARS]
    if isinstance(value, (int, float, bool)):
        return str(value)
    return _redact_text(str(value))[:MAX_NORMALIZED_TEXT_CHARS]


def _redact_text(value: str) -> str:
    redacted = _QUOTED_JSON_SECRET_PATTERN.sub(
        r'\g<prefix>"[REDACTED]"',
        value,
    )
    for pattern, replacement in _REDACTION_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def _usage_payload(*sources: object) -> dict[str, JsonValue]:
    aliases = {
        "input_tokens": ("input_tokens", "prompt_tokens", "prompt_token_count", "input"),
        "output_tokens": (
            "output_tokens",
            "completion_tokens",
            "completion_token_count",
            "output",
        ),
        "total_tokens": ("total_tokens", "total_token_count", "total"),
    }
    usage: dict[str, JsonValue] = {}
    for source in sources:
        mapping = _as_string_mapping(source)
        if mapping is None:
            continue
        for output_key, source_keys in aliases.items():
            if output_key in usage:
                continue
            for source_key in source_keys:
                value = mapping.get(source_key)
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    usage[output_key] = value
                    break
    return usage


def _is_sensitive_key(value: object) -> bool:
    text = str(value).strip()
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", text)
    normalized = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    compact = normalized.replace("_", "")
    return (
        normalized in _SENSITIVE_KEY_NAMES
        or compact
        in {
            "xapikey",
            "proxyauthorization",
            "accesstoken",
            "clientsecret",
            "refreshtoken",
        }
        or normalized.endswith(
            (
                "_token",
                "_secret",
                "_password",
                "_credential",
                "_authorization",
                "_api_key",
                "_access_token",
                "_refresh_token",
                "_client_secret",
            )
        )
        or normalized.startswith(
            (
                "secret_",
                "password_",
                "api_key_",
                "authorization_",
                "access_token_",
                "refresh_token_",
                "proxy_authorization_",
                "x_api_key_",
                "client_secret_",
            )
        )
        or normalized in {"token_value", "token_secret", "token_header"}
    )


def _is_hidden_type(value: object) -> bool:
    return isinstance(value, str) and value.lower() in _HIDDEN_BLOCK_TYPES


def _as_string_mapping(value: object) -> Mapping[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    return cast(Mapping[str, object], value)


def _safe_json(value: object, *, depth: int = 0) -> JsonValue:
    if depth >= _MAX_SAFE_DEPTH:
        return "[TRUNCATED]"
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, str):
            return _redact_text(value)[:MAX_NORMALIZED_TEXT_CHARS]
        return value
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        kind = mapping.get("type")
        if _is_hidden_type(kind):
            return {"type": "hidden"}
        result: dict[str, JsonValue] = {}
        for index, (key, item) in enumerate(mapping.items()):
            if index >= _MAX_SAFE_ITEMS:
                result["_truncated"] = True
                break
            safe_key = _safe_string(key)[:128]
            result[safe_key] = (
                "[REDACTED]"
                if _is_sensitive_key(key)
                else _safe_json(item, depth=depth + 1)
            )
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = list(cast(Sequence[object], value))
        return [
            _safe_json(item, depth=depth + 1)
            for item in items[:_MAX_SAFE_ITEMS]
        ]
    if _is_hidden_type(getattr(value, "type", None)):
        return {"type": "hidden"}
    content = getattr(value, "content", None)
    if content is not None and content is not value:
        return _safe_json(content, depth=depth + 1)
    return _safe_string(value)


def sanitize_output(value: object) -> JsonValue:
    """Return a bounded, credential-free output projection for API consumers."""

    return _safe_json(value)


def _summarize_state(value: object) -> dict[str, JsonValue]:
    state = _as_string_mapping(value)
    if state is None:
        return {"message_count": 0}
    messages = state.get("messages")
    message_items = (
        list(cast(Sequence[object], messages))
        if isinstance(messages, Sequence) and not isinstance(messages, (str, bytes))
        else []
    )
    visible_messages = [
        message_summary
        for message in message_items
        if (message_summary := _message_summary(message)) is not None
    ]
    summary: dict[str, JsonValue] = {"message_count": len(visible_messages)}
    files = state.get("files")
    if isinstance(files, Mapping):
        summary["file_count"] = len(cast(Mapping[object, object], files))
    if visible_messages:
        last_message_type, last_message_text = visible_messages[-1]
        summary["last_message_type"] = last_message_type
        if last_message_text:
            summary["last_message_text"] = last_message_text
    return summary


def _message_summary(value: object) -> tuple[str, str] | None:
    mapping = _as_string_mapping(value)
    if mapping is not None:
        message_type = mapping.get("type", "")
        content = mapping.get("content")
    else:
        message_type = getattr(value, "type", "")
        content = getattr(value, "content", None)
    if _is_hidden_type(message_type):
        return None
    return _safe_string(message_type), _visible_content_text(content)


def _visible_content_text(value: object) -> str:
    if isinstance(value, str):
        return _redact_text(value)[:MAX_NORMALIZED_TEXT_CHARS]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        parts: list[str] = []
        for item in cast(Sequence[object], value)[:_MAX_SAFE_ITEMS]:
            mapping = _as_string_mapping(item)
            if mapping is not None:
                if _is_hidden_type(mapping.get("type")):
                    continue
                content = mapping.get("text", mapping.get("content"))
            else:
                if _is_hidden_type(getattr(item, "type", None)):
                    continue
                content = getattr(item, "text", getattr(item, "content", None))
            if isinstance(content, str):
                parts.append(_redact_text(content))
        return _redact_text(" ".join(parts))[:MAX_NORMALIZED_TEXT_CHARS]
    if value is None:
        return ""
    return _safe_string(value)


__all__ = [
    "DeepAgentsV3Normalizer",
    "MAX_NORMALIZED_TEXT_CHARS",
    "NormalizedEvent",
    "sanitize_output",
]
