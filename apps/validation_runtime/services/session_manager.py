import asyncio
from datetime import datetime, timezone
from uuid import uuid4

from apps.validation_runtime.domain import RuntimeSession, SessionStatus
from apps.validation_runtime.errors import RuntimeServiceError
from apps.validation_runtime.services.agent_instance_registry import AgentInstanceRegistry
from apps.validation_runtime.services.agent_router import AgentRouter


class SessionManager:
    def __init__(
        self,
        router: AgentRouter,
        registry: AgentInstanceRegistry,
    ) -> None:
        self._router = router
        self._registry = registry
        self._sessions: dict[str, RuntimeSession] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def create(self, agent_id: str) -> RuntimeSession:
        instance = self._router.resolve(agent_id)
        if self._registry.get_record(instance.agent_instance_id) is None:
            raise RuntimeServiceError(
                "AGENT_INSTANCE_UNAVAILABLE",
                f"Agent '{agent_id}' does not have an active instance.",
            )

        now = datetime.now(timezone.utc)
        session_id = f"ses_{uuid4()}"
        session = RuntimeSession(
            session_id=session_id,
            thread_id=session_id,
            agent_id=agent_id,
            bound_agent_instance_id=instance.agent_instance_id,
            package_digest=instance.package_digest,
            status=SessionStatus.IDLE,
            turn_count=0,
            created_at=now,
            updated_at=now,
        )
        self._sessions[session_id] = session
        self._locks[session_id] = asyncio.Lock()
        return session

    def get(self, session_id: str) -> RuntimeSession:
        try:
            return self._sessions[session_id]
        except KeyError as exc:
            raise RuntimeServiceError(
                "SESSION_NOT_FOUND",
                "Session was not found.",
            ) from exc

    def begin_stream(self, session_id: str) -> RuntimeSession:
        session = self.get(session_id)
        if session.status is SessionStatus.CLOSED:
            raise RuntimeServiceError("SESSION_CLOSED", "Session is closed.")
        if session.status is SessionStatus.STREAMING:
            raise RuntimeServiceError(
                "SESSION_BUSY",
                "Another Chat stream is active for this Session.",
            )
        return self._update(session, status=SessionStatus.STREAMING)

    def lock_for(self, session_id: str) -> asyncio.Lock:
        self.get(session_id)
        return self._locks[session_id]

    def mark_succeeded(self, session_id: str) -> RuntimeSession:
        session = self._require_streaming(session_id)
        return self._update(
            session,
            status=SessionStatus.IDLE,
            turn_count=session.turn_count + 1,
        )

    def mark_failed(self, session_id: str) -> RuntimeSession:
        session = self._require_streaming(session_id)
        return self._update(session, status=SessionStatus.IDLE)

    def close(self, session_id: str) -> RuntimeSession:
        session = self.get(session_id)
        if session.status is SessionStatus.STREAMING:
            raise RuntimeServiceError(
                "SESSION_BUSY",
                "Another Chat stream is active for this Session.",
            )
        return self._update(session, status=SessionStatus.CLOSED)

    def _require_streaming(self, session_id: str) -> RuntimeSession:
        session = self.get(session_id)
        if session.status is not SessionStatus.STREAMING:
            raise RuntimeServiceError(
                "SESSION_NOT_STREAMING",
                "Session does not have an active Chat stream.",
            )
        return session

    def _update(self, session: RuntimeSession, **changes: object) -> RuntimeSession:
        updated = session.model_copy(
            update={**changes, "updated_at": datetime.now(timezone.utc)}
        )
        self._sessions[session.session_id] = updated
        return updated
