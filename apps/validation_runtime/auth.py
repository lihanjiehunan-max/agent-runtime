import secrets
from collections.abc import Callable

from fastapi import Header

from apps.validation_runtime.config import RuntimeConfig
from apps.validation_runtime.errors import RuntimeServiceError


def build_bearer_authenticator(
    config: RuntimeConfig,
) -> Callable[..., None]:
    if config.service_api_key is None:
        raise RuntimeServiceError(
            "SERVICE_API_KEY_MISSING",
            "Service API key is not configured.",
        )
    expected = config.service_api_key.get_secret_value()

    def require_bearer(authorization: str | None = Header(default=None)) -> None:
        scheme, separator, token = (authorization or "").partition(" ")
        if (
            scheme != "Bearer"
            or separator != " "
            or not token
            or not secrets.compare_digest(token, expected)
        ):
            raise RuntimeServiceError(
                "AUTHENTICATION_REQUIRED",
                "Authentication is required",
            )

    return require_bearer
