from __future__ import annotations

import asyncio
import re
from typing import Protocol

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,253}$")
_DEFAULT_CANCEL_TTL_SECONDS = 24 * 60 * 60


class CancellationRedis(Protocol):
    async def set(
        self,
        name: str,
        value: str,
        *,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool | int | str | bytes | None: ...

    async def get(self, name: str) -> str | bytes | int | None: ...


class CancellationRequested(RuntimeError):
    """Raised when a worker observes a durable cancellation request."""


def cancellation_key(tenant_id: str, execution_id: str) -> str:
    if _IDENTIFIER.fullmatch(tenant_id) is None:
        raise ValueError("tenant_id must be a runtime identifier below 255 characters")
    if _IDENTIFIER.fullmatch(execution_id) is None:
        raise ValueError("execution_id must be a runtime identifier below 255 characters")
    return f"cancel:execution:{tenant_id}:{execution_id}"


class CancellationToken(Protocol):
    async def cancel(self) -> bool: ...

    async def is_cancelled(self) -> bool: ...

    async def wait(self) -> None: ...

    async def check(self) -> None: ...


class RedisCancellationToken:
    """Durable cooperative cancellation backed by a Redis key.

    The local worker only polls the Redis key; it is never the source of truth.
    """

    def __init__(
        self,
        redis: CancellationRedis,
        tenant_id: str,
        execution_id: str,
        *,
        poll_interval_seconds: float = 0.05,
        ttl_seconds: int = _DEFAULT_CANCEL_TTL_SECONDS,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("cancellation poll interval must be positive")
        if ttl_seconds <= 0:
            raise ValueError("cancellation TTL must be positive")
        self._redis = redis
        self.tenant_id = tenant_id
        self.execution_id = execution_id
        self.key = cancellation_key(tenant_id, execution_id)
        self._poll_interval_seconds = poll_interval_seconds
        self._ttl_seconds = ttl_seconds

    async def cancel(self) -> bool:
        result = await self._redis.set(
            self.key,
            "1",
            nx=True,
            ex=self._ttl_seconds,
        )
        return _redis_truthy(result)

    async def is_cancelled(self) -> bool:
        return _redis_truthy(await self._redis.get(self.key))

    async def wait(self) -> None:
        while not await self.is_cancelled():
            await asyncio.sleep(self._poll_interval_seconds)

    async def check(self) -> None:
        if await self.is_cancelled():
            raise CancellationRequested(
                f"execution {self.execution_id} was cooperatively cancelled"
            )

    async def throw_if_cancelled(self) -> None:
        await self.check()


def _redis_truthy(value: str | bytes | int | bool | None) -> bool:
    return value not in (None, False, 0, "", b"", "0", b"0")


__all__ = [
    "CancellationRedis",
    "CancellationRequested",
    "CancellationToken",
    "RedisCancellationToken",
    "cancellation_key",
]
