from __future__ import annotations

from typing import Annotated, Protocol, cast

from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel, ConfigDict, Field

from apps.runtime_api.dependencies import RuntimeAPIError, api_error, current_principal
from packages.execution_manager.queue import (
    AsyncTaskStatus,
    CancelTaskResponse,
    QueueError,
    TaskHandle,
)
from packages.runtime_contracts import ErrorCode, ExecutionStatus, Principal


class TaskService(Protocol):
    async def execute_async(
        self, session_id: str, input_text: str, principal: Principal
    ) -> AsyncTaskStatus | TaskHandle: ...

    async def get_task_status(
        self, execution_id: str, principal: Principal, *, session_id: str
    ) -> AsyncTaskStatus: ...

    async def cancel_task(
        self, execution_id: str, principal: Principal, *, session_id: str
    ) -> CancelTaskResponse: ...


class TaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    input: str = Field(min_length=1, max_length=32_768)


router = APIRouter(prefix="/api/v1/runtime/sessions", tags=["tasks"])


def task_service(request: Request) -> TaskService:
    service = getattr(request.app.state, "task_service", None)
    if service is None:
        raise api_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ErrorCode.RUNTIME_INCOMPATIBLE,
            "async task service is not configured",
        )
    return cast(TaskService, service)


def _queue_api_error(error: QueueError) -> RuntimeAPIError:
    if error.error.code is ErrorCode.SESSION_CLOSED:
        code_status = status.HTTP_404_NOT_FOUND
    elif error.error.code is ErrorCode.TENANT_IDENTITY_MISMATCH:
        code_status = status.HTTP_403_FORBIDDEN
    else:
        code_status = status.HTTP_409_CONFLICT
    return RuntimeAPIError(code_status, error.error)


@router.post(
    "/{session_id}/tasks",
    response_model=AsyncTaskStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def execute_async_task(
    session_id: str,
    command: TaskRequest,
    principal: Annotated[Principal, Depends(current_principal)],
    service: Annotated[TaskService, Depends(task_service)],
) -> AsyncTaskStatus:
    try:
        created = await service.execute_async(session_id, command.input, principal)
    except QueueError as error:
        raise _queue_api_error(error) from error
    if isinstance(created, TaskHandle):
        return AsyncTaskStatus(
            command_id=created.command_id,
            execution_id=created.execution_id,
            session_id=session_id,
            tenant_id=principal.tenant_id,
            execution_epoch=0,
            status=ExecutionStatus.ACCEPTED,
        )
    return created


@router.get("/{session_id}/tasks/{execution_id}", response_model=AsyncTaskStatus)
async def get_async_task_status(
    session_id: str,
    execution_id: str,
    principal: Annotated[Principal, Depends(current_principal)],
    service: Annotated[TaskService, Depends(task_service)],
) -> AsyncTaskStatus:
    try:
        task = await service.get_task_status(
            execution_id, principal, session_id=session_id
        )
    except QueueError as error:
        raise _queue_api_error(error) from error
    if task.session_id != session_id:
        raise api_error(
            status.HTTP_403_FORBIDDEN,
            ErrorCode.TENANT_IDENTITY_MISMATCH,
            "task does not belong to the requested session",
        )
    return task


@router.post("/{session_id}/tasks/{execution_id}/cancel", response_model=CancelTaskResponse)
async def cancel_async_task(
    session_id: str,
    execution_id: str,
    principal: Annotated[Principal, Depends(current_principal)],
    service: Annotated[TaskService, Depends(task_service)],
) -> CancelTaskResponse:
    try:
        response = await service.cancel_task(
            execution_id, principal, session_id=session_id
        )
    except QueueError as error:
        raise _queue_api_error(error) from error
    if response.execution_id != execution_id:
        raise api_error(
            status.HTTP_403_FORBIDDEN,
            ErrorCode.TENANT_IDENTITY_MISMATCH,
            "task identity does not match the requested execution",
        )
    return response


__all__ = ["TaskRequest", "TaskService", "router"]
