from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, cast
from uuid import uuid4

import httpx
from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from langchain_core.language_models import (
    BaseChatModel,
    LangSmithParams,
    LanguageModelInput,
)
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
    ToolCall,
    ToolCallChunk,
    ToolMessage,
    UsageMetadata,
)
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool
from langchain_core.utils.function_calling import convert_to_openai_tool

logger = logging.getLogger(__name__)


class ModelGatewayErrorCode(StrEnum):
    TIMEOUT = "MODEL_TIMEOUT"
    PROVIDER_ERROR = "MODEL_PROVIDER_ERROR"


class ModelGatewayError(Exception):
    def __init__(
        self,
        code: ModelGatewayErrorCode,
        *,
        correlation_id: str,
        status_code: int | None = None,
    ) -> None:
        super().__init__(f"model gateway request failed ({code})")
        self.code = code
        self.correlation_id = correlation_id
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class ModelGatewayConfig:
    base_url: str
    api_key: str = field(repr=False)
    model_name: str
    timeout_seconds: float = 30.0

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> ModelGatewayConfig:
        source = os.environ if environ is None else environ
        timeout_value = source.get("MODEL_GATEWAY_TIMEOUT_SECONDS", "30")
        try:
            timeout_seconds = float(timeout_value)
        except ValueError as error:
            raise ValueError("MODEL_GATEWAY_TIMEOUT_SECONDS must be numeric") from error
        return cls(
            base_url=source.get("MODEL_GATEWAY_BASE_URL", ""),
            api_key=source.get("MODEL_GATEWAY_API_KEY", ""),
            model_name=source.get("MODEL_GATEWAY_MODEL", ""),
            timeout_seconds=timeout_seconds,
        )

    def __post_init__(self) -> None:
        if not self.base_url or not self.api_key or not self.model_name:
            raise ValueError("model gateway base URL, API key, and model name are required")
        if self.timeout_seconds <= 0:
            raise ValueError("model gateway timeout must be positive")


class _ModelGatewayChatModel(BaseChatModel):
    model_name: str
    gateway_base_url: str
    timeout_seconds: float
    _api_key: str
    _http_client: httpx.Client

    def __init__(
        self,
        config: ModelGatewayConfig,
        http_client: httpx.Client,
    ) -> None:
        model_kwargs: dict[str, Any] = {
            "model_name": config.model_name,
            "gateway_base_url": config.base_url.rstrip("/"),
            "timeout_seconds": config.timeout_seconds,
        }
        super().__init__(**model_kwargs)
        self._api_key = config.api_key
        self._http_client = http_client

    @property
    def _llm_type(self) -> str:
        return "model-gateway-openai-compatible"

    def _get_ls_params(
        self,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> LangSmithParams:
        del stop, kwargs
        return {
            "ls_provider": "model-gateway",
            "ls_model_name": self.model_name,
            "ls_model_type": "chat",
        }

    def bind_tools(
        self,
        tools: Sequence[dict[str, Any] | type | Callable[..., Any] | BaseTool],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        formatted = [convert_to_openai_tool(candidate) for candidate in tools]
        return self.bind(tools=formatted, tool_choice=tool_choice, **kwargs)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        del run_manager
        forbidden = {"api_key", "authorization", "headers"}.intersection(kwargs)
        if forbidden:
            raise ValueError("model gateway credentials and headers are server-configured")

        correlation_id = str(uuid4())
        payload: dict[str, Any] = {
            "model": self.model_name,
            "messages": [_serialize_message(message) for message in messages],
        }
        if stop is not None:
            payload["stop"] = stop
        if "tools" in kwargs:
            payload["tools"] = kwargs["tools"]
        if kwargs.get("tool_choice") is not None:
            payload["tool_choice"] = kwargs["tool_choice"]

        transport_error: ModelGatewayError | None = None
        response: httpx.Response | None = None
        try:
            response = self._http_client.post(
                f"{self.gateway_base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "X-Request-ID": correlation_id,
                },
                json=payload,
                timeout=self.timeout_seconds,
            )
        except httpx.TimeoutException:
            logger.warning(
                "model gateway timeout correlation_id=%s",
                correlation_id,
            )
            transport_error = ModelGatewayError(
                ModelGatewayErrorCode.TIMEOUT,
                correlation_id=correlation_id,
            )
        except httpx.RequestError:
            logger.warning(
                "model gateway provider error correlation_id=%s",
                correlation_id,
            )
            transport_error = ModelGatewayError(
                ModelGatewayErrorCode.PROVIDER_ERROR,
                correlation_id=correlation_id,
            )
        if transport_error is not None:
            raise transport_error from None
        if response is None:
            raise RuntimeError("model gateway transport completed without a response")
        if not response.is_success:
            logger.warning(
                "model gateway provider error correlation_id=%s status_code=%s",
                correlation_id,
                response.status_code,
            )
            raise ModelGatewayError(
                ModelGatewayErrorCode.PROVIDER_ERROR,
                correlation_id=correlation_id,
                status_code=response.status_code,
            )
        try:
            body = _string_object_dict(
                cast(object, response.json()),
                "model gateway returned an invalid response",
            )
            message = _parse_message(body, response, correlation_id)
        except (TypeError, ValueError) as error:
            logger.warning(
                "model gateway invalid response correlation_id=%s status_code=%s",
                correlation_id,
                response.status_code,
            )
            raise ModelGatewayError(
                ModelGatewayErrorCode.PROVIDER_ERROR,
                correlation_id=correlation_id,
                status_code=response.status_code,
            ) from error
        return ChatResult(generations=[ChatGeneration(message=message)])

    def _stream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> Iterator[ChatGenerationChunk]:
        result = self._generate(
            messages,
            stop=stop,
            run_manager=run_manager,
            **kwargs,
        )
        message = result.generations[0].message
        if not isinstance(message, AIMessage):
            raise RuntimeError("model gateway generated a non-AI message")
        tool_call_chunks = [
            ToolCallChunk(
                name=tool_call["name"],
                args=json.dumps(tool_call["args"], separators=(",", ":")),
                id=tool_call["id"],
                index=index,
                type="tool_call_chunk",
            )
            for index, tool_call in enumerate(message.tool_calls)
        ]
        yield ChatGenerationChunk(
            message=AIMessageChunk(
                content=message.content,
                tool_call_chunks=tool_call_chunks,
                usage_metadata=message.usage_metadata,
                response_metadata=message.response_metadata,
            )
        )


class ModelGatewayClient:
    def __init__(
        self,
        config: ModelGatewayConfig,
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._config = config
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.Client(trust_env=False)
        self.chat_model: BaseChatModel = _ModelGatewayChatModel(config, self._http_client)

    def close(self) -> None:
        if self._owns_http_client:
            self._http_client.close()

    def __repr__(self) -> str:
        return (
            f"ModelGatewayClient(base_url={self._config.base_url!r}, "
            f"model_name={self._config.model_name!r})"
        )


def _serialize_message(message: BaseMessage) -> dict[str, Any]:
    role_by_type = {
        "ai": "assistant",
        "human": "user",
        "system": "system",
        "tool": "tool",
    }
    serialized: dict[str, Any] = {
        "role": role_by_type.get(message.type, message.type),
        "content": message.content,
    }
    if isinstance(message, ToolMessage):
        serialized["tool_call_id"] = message.tool_call_id
    if isinstance(message, AIMessage) and message.tool_calls:
        serialized["tool_calls"] = [
            {
                "id": tool_call["id"],
                "type": "function",
                "function": {
                    "name": tool_call["name"],
                    "arguments": json.dumps(tool_call["args"], separators=(",", ":")),
                },
            }
            for tool_call in message.tool_calls
        ]
    return serialized


def _parse_message(
    body: Mapping[str, object],
    response: httpx.Response,
    correlation_id: str,
) -> AIMessage:
    choices_value = body.get("choices")
    if not isinstance(choices_value, list) or not choices_value:
        raise ValueError("model gateway returned an invalid response")
    choices = cast(list[object], choices_value)
    choice = _string_object_dict(
        choices[0],
        "model gateway returned an invalid response",
    )
    raw_message = _string_object_dict(
        choice.get("message"),
        "model gateway returned an invalid response",
    )
    content = raw_message.get("content")
    if content is None:
        content = ""
    elif not isinstance(content, str):
        raise ValueError("model gateway returned an invalid response")
    tool_calls = _parse_tool_calls(raw_message.get("tool_calls"))

    usage_metadata: UsageMetadata | None = None
    usage_value = body.get("usage")
    if usage_value is not None:
        usage = _string_object_dict(
            usage_value,
            "model gateway returned invalid usage metadata",
        )
        input_tokens = _nonnegative_int(
            usage.get("prompt_tokens", usage.get("input_tokens", 0))
        )
        output_tokens = _nonnegative_int(
            usage.get("completion_tokens", usage.get("output_tokens", 0))
        )
        total_tokens = _nonnegative_int(usage.get("total_tokens", 0))
        usage_metadata = UsageMetadata(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
        )

    body_request_id = body.get("id")
    provider_request_id = response.headers.get("x-request-id")
    if provider_request_id is None and isinstance(body_request_id, str):
        provider_request_id = body_request_id

    return AIMessage(
        content=content,
        tool_calls=tool_calls,
        usage_metadata=usage_metadata,
        response_metadata={
            "correlation_id": correlation_id,
            "provider_request_id": provider_request_id,
            "finish_reason": choice.get("finish_reason"),
        },
    )


def _parse_tool_calls(value: object) -> list[ToolCall]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("model gateway returned invalid tool calls")

    parsed: list[ToolCall] = []
    for raw_tool_call in cast(list[object], value):
        tool_call = _string_object_dict(
            raw_tool_call,
            "model gateway returned invalid tool calls",
        )
        raw_function = tool_call.get("function")
        call_id = tool_call.get("id")
        if not isinstance(call_id, str):
            raise ValueError("model gateway returned invalid tool calls")
        function = _string_object_dict(
            raw_function,
            "model gateway returned invalid tool calls",
        )
        name = function.get("name")
        arguments = function.get("arguments")
        if not isinstance(name, str) or not isinstance(arguments, str):
            raise ValueError("model gateway returned invalid tool calls")
        try:
            args = cast(object, json.loads(arguments))
        except json.JSONDecodeError as error:
            raise ValueError("model gateway returned invalid tool calls") from error
        parsed_args = _string_object_dict(
            args,
            "model gateway returned invalid tool calls",
        )
        parsed.append(
            ToolCall(
                name=name,
                args=cast(dict[str, Any], parsed_args),
                id=call_id,
                type="tool_call",
            )
        )
    return parsed


def _string_object_dict(value: object, error_message: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(error_message)
    result: dict[str, object] = {}
    for raw_key, raw_value in cast(Mapping[object, object], value).items():
        if not isinstance(raw_key, str):
            raise ValueError(error_message)
        result[raw_key] = raw_value
    return result


def _nonnegative_int(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("model gateway returned invalid usage metadata")
    return value


__all__ = [
    "ModelGatewayClient",
    "ModelGatewayConfig",
    "ModelGatewayError",
    "ModelGatewayErrorCode",
]
