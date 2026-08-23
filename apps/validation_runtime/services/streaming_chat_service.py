import asyncio
from collections.abc import AsyncIterator
from uuid import uuid4

from langchain_core.messages import AIMessageChunk

from apps.validation_runtime.domain import (
    AgentInstanceRecord,
    ChatDelta,
    ChatDone,
    ChatError,
    ChatEvent,
    RuntimeSession,
    SessionStatus,
)
from apps.validation_runtime.errors import RuntimeServiceError
from apps.validation_runtime.services.agent_instance_registry import AgentInstanceRegistry
from apps.validation_runtime.services.session_manager import SessionManager


class StreamingChatService:
    def __init__(
        self,
        registry: AgentInstanceRegistry,
        sessions: SessionManager,
    ) -> None:
        self._registry = registry
        self._sessions = sessions

    async def open_stream(
        self,
        session_id: str,
        message: str,
    ) -> AsyncIterator[ChatEvent]:
        session = self._sessions.get(session_id)
        record = self._registry.get_record(session.bound_agent_instance_id)
        if record is None:
            raise RuntimeServiceError(
                "AGENT_INSTANCE_UNAVAILABLE",
                f"Agent '{session.agent_id}' does not have an active instance.",
            )
        normalized_message = message.strip() if isinstance(message, str) else ""
        if (
            not normalized_message
            or len(normalized_message)
            > record.assets.manifest.limits.max_input_characters
        ):
            raise RuntimeServiceError(
                "INVALID_MESSAGE",
                "Message must be non-empty and within the configured limit.",
            )

        claimed = self._sessions.begin_stream(session_id)
        return self._consume(claimed, record, normalized_message)

    async def _consume(
        self,
        session: RuntimeSession,
        record: AgentInstanceRecord,
        message: str,
    ) -> AsyncIterator[ChatEvent]:
        execution_id = f"exe_{uuid4()}"
        lock = self._sessions.lock_for(session.session_id)
        try:
            async with lock:
                async with asyncio.timeout(
                    record.assets.manifest.limits.execution_timeout_seconds
                ):
                    async for chunk, _metadata in record.graph.astream(
                        {"messages": [{"role": "user", "content": message}]},
                        {"configurable": {"thread_id": session.thread_id}},
                        stream_mode="messages",
                    ):
                        if not isinstance(chunk, AIMessageChunk):
                            continue
                        content = chunk.text
                        if content:
                            yield ChatDelta(content=content)
            completed = self._sessions.mark_succeeded(session.session_id)
            yield ChatDone(
                execution_id=execution_id,
                turn_count=completed.turn_count,
            )
        except TimeoutError:
            self._mark_failed_if_streaming(session.session_id)
            yield ChatError(
                code="MODEL_GATEWAY_TIMEOUT",
                message="Model gateway request timed out",
            )
        except asyncio.CancelledError:
            self._mark_failed_if_streaming(session.session_id)
            raise
        except Exception:
            self._mark_failed_if_streaming(session.session_id)
            yield ChatError(
                code="MODEL_GATEWAY_ERROR",
                message="Model gateway request failed",
            )
        finally:
            self._mark_failed_if_streaming(session.session_id)

    def _mark_failed_if_streaming(self, session_id: str) -> None:
        session = self._sessions.get(session_id)
        if session.status is SessionStatus.STREAMING:
            self._sessions.mark_failed(session_id)
