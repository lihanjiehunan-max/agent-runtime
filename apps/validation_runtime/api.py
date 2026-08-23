import json

from fastapi import Depends, FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse

from apps.validation_runtime.api_models import (
    AgentInstanceResponse,
    BusinessSessionResponse,
    ChatRequest,
    HealthResponse,
)
from apps.validation_runtime.auth import build_bearer_authenticator
from apps.validation_runtime.config import RuntimeConfig
from apps.validation_runtime.container import RuntimeContainer
from apps.validation_runtime.domain import ChatEvent
from apps.validation_runtime.errors import RuntimeServiceError


ERROR_STATUS = {
    "AUTHENTICATION_REQUIRED": 401,
    "AGENT_NOT_FOUND": 404,
    "SESSION_NOT_FOUND": 404,
    "AGENT_INSTANCE_UNAVAILABLE": 409,
    "AGENT_ROUTING_AMBIGUOUS": 409,
    "SESSION_BUSY": 409,
    "SESSION_CLOSED": 409,
    "INVALID_MESSAGE": 422,
    "AGENT_ASSET_INVALID": 422,
    "MODEL_API_KEY_MISSING": 503,
}


def create_app(
    container: RuntimeContainer | None = None,
    config: RuntimeConfig | None = None,
) -> FastAPI:
    runtime_config = config or RuntimeConfig.from_env()
    require_bearer = build_bearer_authenticator(runtime_config)
    runtime = container or RuntimeContainer(runtime_config)
    app = FastAPI(title="Agent Runtime Session SSE MVP", version="0.1.0")

    @app.exception_handler(RuntimeServiceError)
    async def handle_runtime_error(_request, exc: RuntimeServiceError):
        return JSONResponse(
            status_code=ERROR_STATUS.get(exc.detail.code, 500),
            content={"error": exc.detail.model_dump()},
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_request, _exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "INVALID_MESSAGE",
                    "message": "Message is invalid",
                }
            },
        )

    @app.get("/healthz", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse()

    @app.post(
        "/api/v1/runtime/ops/agents/{agent_id}/deploy",
        response_model=AgentInstanceResponse,
        dependencies=[Depends(require_bearer)],
    )
    async def deploy_agent(agent_id: str) -> AgentInstanceResponse:
        return AgentInstanceResponse.from_domain(runtime.deploy(agent_id))

    @app.get(
        "/api/v1/runtime/ops/agent-instances",
        response_model=list[AgentInstanceResponse],
        dependencies=[Depends(require_bearer)],
    )
    async def list_agent_instances() -> list[AgentInstanceResponse]:
        return [
            AgentInstanceResponse.from_domain(instance)
            for instance in runtime.registry.list()
        ]

    @app.post(
        "/api/v1/runtime/agents/{agent_id}/sessions",
        response_model=BusinessSessionResponse,
        status_code=201,
        dependencies=[Depends(require_bearer)],
    )
    async def create_session(agent_id: str) -> BusinessSessionResponse:
        return BusinessSessionResponse.from_domain(runtime.sessions.create(agent_id))

    @app.post(
        "/api/v1/runtime/sessions/{session_id}/chat",
        dependencies=[Depends(require_bearer)],
    )
    async def stream_chat(session_id: str, request: ChatRequest):
        events = await runtime.chat.open_stream(session_id, request.message)
        return StreamingResponse(
            _encode_sse(events),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    app.state.runtime = runtime
    return app


async def _encode_sse(events):
    try:
        async for event in events:
            yield _serialize_event(event)
    finally:
        await events.aclose()


def _serialize_event(event: ChatEvent) -> str:
    payload = json.dumps(
        event.model_dump(exclude={"event"}),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return f"event: {event.event}\ndata: {payload}\n\n"
