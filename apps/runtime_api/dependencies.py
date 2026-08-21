from collections.abc import Awaitable, Callable
from typing import Annotated, cast

from fastapi import Header, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from packages.runtime_contracts import ErrorCode, Principal
from packages.runtime_contracts import RuntimeError as RuntimeContractError

DEVELOPMENT_TOKEN = "dev-token"
DEVELOPMENT_PRINCIPAL = Principal(
    tenant_id="demo-tenant",
    user_id="local-operator",
    actor_id="local-operator",
    worker_id="host-runtime-01",
    permissions=(),
)

PrincipalVerifier = Callable[[str], Awaitable[Principal | None]]

bearer_scheme = HTTPBearer(auto_error=False)


class RuntimeAPIError(Exception):
    def __init__(self, status_code: int, error: RuntimeContractError) -> None:
        super().__init__(error.message)
        self.status_code = status_code
        self.error = error


def api_error(status_code: int, code: ErrorCode, message: str) -> RuntimeAPIError:
    return RuntimeAPIError(status_code, RuntimeContractError(code=code, message=message))


async def current_principal(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Security(bearer_scheme),
    ],
    x_tenant_id: Annotated[str | None, Header(alias="x-tenant-id")] = None,
) -> Principal:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "A valid Bearer token is required",
        )

    verifier = cast(
        PrincipalVerifier | None,
        getattr(request.app.state, "principal_verifier", None),
    )
    allow_dev_auth = bool(getattr(request.app.state, "allow_dev_auth", False))

    if verifier is not None:
        principal = await verifier(credentials.credentials)
    elif allow_dev_auth and credentials.credentials == DEVELOPMENT_TOKEN:
        principal = DEVELOPMENT_PRINCIPAL
    else:
        principal = None

    if principal is None:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.INVALID_ACCESS_TOKEN,
            "Bearer token is invalid",
        )
    if x_tenant_id is not None and x_tenant_id != principal.tenant_id:
        raise api_error(
            status.HTTP_403_FORBIDDEN,
            ErrorCode.TENANT_IDENTITY_MISMATCH,
            "x-tenant-id does not match the verified principal",
        )
    return principal


__all__ = [
    "DEVELOPMENT_PRINCIPAL",
    "DEVELOPMENT_TOKEN",
    "PrincipalVerifier",
    "RuntimeAPIError",
    "current_principal",
]
