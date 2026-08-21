from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated, Protocol, cast

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import StreamingResponse

from apps.runtime_api.dependencies import RuntimeAPIError, api_error, current_principal
from packages.execution_manager.service import (
    ExecutionManager,
    ExecutionManagerError,
    ExecutionResult,
)
from packages.runtime_contracts import (
    CreateExecutionRequest,
    ErrorCode,
    ExecutionMode,
    Principal,
    RuntimeEvent,
)

router = APIRouter(prefix="/api/v1/runtime/sessions", tags=["executions"])


class ExecutionService(Protocol):
    async def execute_turn(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        *,
        mode: ExecutionMode = ExecutionMode.SYNC,
    ) -> ExecutionResult: ...

    def execute_turn_stream(
        self,
        session_id: str,
        input_text: str,
        principal: Principal,
        *,
        mode: ExecutionMode = ExecutionMode.STREAM,
    ) -> AsyncIterator[RuntimeEvent]: ...


def execution_manager(request: Request) -> ExecutionService:
    manager = getattr(request.app.state, "execution_manager", None)
    if manager is None:
        raise api_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ErrorCode.RUNTIME_INCOMPATIBLE,
            "execution manager is not configured",
        )
    return cast(ExecutionManager, manager)


def _execution_api_error(error: ExecutionManagerError) -> RuntimeAPIError:
    if error.error.code is ErrorCode.SESSION_CLOSED:
        code_status = status.HTTP_404_NOT_FOUND
    elif error.error.code in {ErrorCode.SESSION_BUSY, ErrorCode.EXECUTION_FENCED}:
        code_status = status.HTTP_409_CONFLICT
    elif error.error.code is ErrorCode.MODEL_ERROR:
        code_status = status.HTTP_502_BAD_GATEWAY
    else:
        code_status = status.HTTP_409_CONFLICT
    return RuntimeAPIError(code_status, error.error)


async def _sse_events(events: AsyncIterator[RuntimeEvent]) -> AsyncIterator[str]:
    async for event in events:
        yield event.to_sse()


@router.post("/{session_id}/executions")
async def execute_session_turn(
    session_id: str,
    command: CreateExecutionRequest,
    principal: Annotated[Principal, Depends(current_principal)],
    manager: Annotated[ExecutionService, Depends(execution_manager)],
) -> dict[str, object]:
    if command.mode is ExecutionMode.ASYNC:
        raise api_error(
            status.HTTP_400_BAD_REQUEST,
            ErrorCode.RUNTIME_INCOMPATIBLE,
            "async execution must be submitted through the task queue",
        )
    try:
        result = await manager.execute_turn(
            session_id,
            command.input,
            principal,
            mode=command.mode,
        )
    except ExecutionManagerError as error:
        raise _execution_api_error(error) from error
    return result.to_body()


@router.post("/{session_id}/executions/stream")
async def stream_session_turn(
    session_id: str,
    command: CreateExecutionRequest,
    principal: Annotated[Principal, Depends(current_principal)],
    manager: Annotated[ExecutionService, Depends(execution_manager)],
) -> StreamingResponse:
    if command.mode is not ExecutionMode.STREAM:
        raise api_error(
            status.HTTP_400_BAD_REQUEST,
            ErrorCode.RUNTIME_INCOMPATIBLE,
            "stream execution endpoint requires mode=stream",
        )
    try:
        events = manager.execute_turn_stream(
            session_id,
            command.input,
            principal,
            mode=command.mode,
        )
    except ExecutionManagerError as error:
        raise _execution_api_error(error) from error
    return StreamingResponse(
        _sse_events(events),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


__all__ = ["ExecutionService", "execution_manager", "router"]
