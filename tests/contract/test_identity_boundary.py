from collections.abc import Awaitable, Callable
from typing import Annotated, cast

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient
from httpx import Response

from apps.runtime_api.dependencies import current_principal
from apps.runtime_api.main import create_api
from packages.runtime_contracts import Principal

Verifier = Callable[[str], Awaitable[Principal | None]]


def client_get(
    client: TestClient,
    path: str,
    *,
    headers: dict[str, str] | None = None,
) -> Response:
    return cast(Response, client.get(path, headers=headers))  # pyright: ignore[reportUnknownMemberType]


def app_with_principal_route(
    *,
    verifier: Verifier | None = None,
    environ: dict[str, str] | None = None,
):
    app = create_api(principal_verifier=verifier, environ=environ or {})

    async def read_principal(
        principal: Annotated[Principal, Depends(current_principal)],
    ) -> Principal:
        return principal

    app.add_api_route("/_test/principal", read_principal, methods=["GET"])
    return app


def test_runtime_startup_fails_closed_without_a_verifier() -> None:
    with (
        pytest.raises(RuntimeError, match="production token verifier is required"),
        TestClient(create_api(environ={})),
    ):
        pass


def test_development_bearer_token_requires_explicit_opt_in() -> None:
    async def reject_all_tokens(_token: str) -> Principal | None:
        return None

    app = app_with_principal_route(verifier=reject_all_tokens, environ={})

    with TestClient(app) as client:
        response = client_get(
            client,
            "/_test/principal",
            headers={"authorization": "Bearer dev-token"},
        )

    assert response.status_code == 401
    assert response.json() == {
        "error": {
            "schema_version": "runtime.error.v1",
            "code": "INVALID_ACCESS_TOKEN",
            "message": "Bearer token is invalid",
            "retryable": False,
            "details": {},
        }
    }


def test_explicit_development_auth_resolves_a_server_owned_principal() -> None:
    app = app_with_principal_route(environ={"RUNTIME_ALLOW_DEV_AUTH": "1"})

    with TestClient(app) as client:
        response = client_get(
            client,
            "/_test/principal",
            headers={
                "authorization": "Bearer dev-token",
                "x-tenant-id": "demo-tenant",
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "tenant_id": "demo-tenant",
        "user_id": "local-operator",
        "actor_id": "local-operator",
        "worker_id": "host-runtime-01",
        "permissions": [],
    }


def test_verified_principal_is_authoritative_and_tenant_header_is_diagnostic() -> None:
    principal = Principal(
        tenant_id="tenant-a",
        user_id="user-a",
        actor_id="operator-a",
        worker_id=None,
        permissions=("session:create",),
    )

    async def verify_token(token: str) -> Principal | None:
        return principal if token == "verified-token" else None

    app = app_with_principal_route(verifier=verify_token, environ={})

    with TestClient(app) as client:
        accepted = client_get(
            client,
            "/_test/principal",
            headers={
                "authorization": "Bearer verified-token",
                "x-tenant-id": "tenant-a",
            },
        )
        mismatch = client_get(
            client,
            "/_test/principal",
            headers={
                "authorization": "Bearer verified-token",
                "x-tenant-id": "tenant-b",
            },
        )

    assert accepted.status_code == 200
    assert accepted.json()["tenant_id"] == "tenant-a"
    assert mismatch.status_code == 403
    assert mismatch.json() == {
        "error": {
            "schema_version": "runtime.error.v1",
            "code": "TENANT_IDENTITY_MISMATCH",
            "message": "x-tenant-id does not match the verified principal",
            "retryable": False,
            "details": {},
        }
    }


def test_health_routes_remain_public_with_configured_authentication() -> None:
    async def reject_all_tokens(_token: str) -> Principal | None:
        return None

    app = app_with_principal_route(verifier=reject_all_tokens, environ={})

    with TestClient(app) as client:
        live = client_get(client, "/health/live")
        ready = client_get(client, "/health/ready")

    assert live.status_code == 200
    assert live.json() == {"status": "live"}
    assert ready.status_code == 503
    assert ready.json()["status"] == "not_ready"
