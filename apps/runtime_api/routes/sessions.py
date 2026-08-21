from typing import Annotated, cast

from fastapi import APIRouter, Depends, Request, status

from apps.runtime_api.dependencies import RuntimeAPIError, api_error, current_principal
from packages.runtime_contracts import (
    CreateSessionRequest,
    ErrorCode,
    Principal,
    RuntimeSession,
)
from packages.session_manager.service import SessionManager, SessionManagerError

router = APIRouter(prefix="/api/v1/runtime/sessions", tags=["sessions"])


def session_manager(request: Request) -> SessionManager:
    manager = getattr(request.app.state, "session_manager", None)
    if manager is None:
        raise RuntimeError("session manager is not configured")
    return cast(SessionManager, manager)


def _as_api_error(error: SessionManagerError) -> RuntimeAPIError:
    if error.error.code is ErrorCode.PACKAGE_NOT_FOUND:
        status_code = status.HTTP_404_NOT_FOUND
    elif error.error.code is ErrorCode.SESSION_BUSY:
        status_code = status.HTTP_409_CONFLICT
    elif error.error.code is ErrorCode.SESSION_CLOSED:
        status_code = status.HTTP_410_GONE
    else:
        status_code = status.HTTP_409_CONFLICT
    return RuntimeAPIError(status_code, error.error)


@router.post("", response_model=RuntimeSession, status_code=status.HTTP_201_CREATED)
async def create_session(
    request: CreateSessionRequest,
    principal: Annotated[Principal, Depends(current_principal)],
    manager: Annotated[SessionManager, Depends(session_manager)],
) -> RuntimeSession:
    try:
        return await manager.create_session(request, principal)
    except SessionManagerError as error:
        raise _as_api_error(error) from error


@router.get("/{session_id}", response_model=RuntimeSession)
async def get_session(
    session_id: str,
    principal: Annotated[Principal, Depends(current_principal)],
    manager: Annotated[SessionManager, Depends(session_manager)],
) -> RuntimeSession:
    runtime_session = await manager.get_session(session_id, principal)
    if runtime_session is None:
        raise api_error(
            status.HTTP_404_NOT_FOUND,
            ErrorCode.SESSION_CLOSED,
            "session is unavailable",
        )
    return runtime_session


@router.post("/{session_id}/close", response_model=RuntimeSession)
async def close_session(
    session_id: str,
    principal: Annotated[Principal, Depends(current_principal)],
    manager: Annotated[SessionManager, Depends(session_manager)],
) -> RuntimeSession:
    try:
        return await manager.close_session(session_id, principal)
    except SessionManagerError as error:
        raise _as_api_error(error) from error


__all__ = ["router"]
