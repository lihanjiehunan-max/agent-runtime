from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4

from packages.runtime_contracts import ExecutionMode, Principal

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,253}$")

_RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""


class AsyncRedis(Protocol):
    async def set(
        self,
        name: str,
        value: str,
        *,
        nx: bool,
        px: int,
    ) -> bool | int | str | bytes | None: ...

    async def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str | int,
    ) -> bool | int | str | bytes | None: ...


class SessionLockUnavailable(RuntimeError):
    pass


def _require_identifier(name: str, value: str) -> None:
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a runtime identifier below 255 characters")


def session_lock_key(tenant_id: str, session_id: str) -> str:
    _require_identifier("tenant_id", tenant_id)
    _require_identifier("session_id", session_id)
    return f"lock:session:{tenant_id}:{session_id}"


@dataclass(slots=True)
class SessionLockLease:
    manager: SessionLockManager
    tenant_id: str
    session_id: str
    owner_id: str
    token: str
    key: str
    _released: bool = False

    @property
    def active(self) -> bool:
        return not self._released

    async def renew(self) -> bool:
        if self._released:
            return False
        return await self.manager.renew(
            self.tenant_id,
            self.session_id,
            self.token,
        )

    async def release(self) -> bool:
        if self._released:
            return False
        released = await self.manager.release(
            self.tenant_id,
            self.session_id,
            self.token,
        )
        self._released = True
        return released


class SessionLockManager:
    def __init__(self, redis: AsyncRedis, *, ttl_seconds: float) -> None:
        ttl_milliseconds = int(ttl_seconds * 1000)
        if ttl_milliseconds < 1:
            raise ValueError("session lock TTL must be at least one millisecond")
        self._redis = redis
        self._ttl_milliseconds = ttl_milliseconds

    async def acquire(
        self,
        tenant_id: str,
        session_id: str,
        owner_id: str,
    ) -> SessionLockLease:
        _require_identifier("owner_id", owner_id)
        key = session_lock_key(tenant_id, session_id)
        token = f"{owner_id}:{uuid4().hex}"
        acquired = await self._redis.set(
            key,
            token,
            nx=True,
            px=self._ttl_milliseconds,
        )
        if not acquired:
            raise SessionLockUnavailable("session lock is already owned")
        return SessionLockLease(
            manager=self,
            tenant_id=tenant_id,
            session_id=session_id,
            owner_id=owner_id,
            token=token,
            key=key,
        )

    async def renew(
        self,
        tenant_id: str,
        session_id: str,
        token: str,
    ) -> bool:
        result = await self._redis.eval(
            _RENEW_SCRIPT,
            1,
            session_lock_key(tenant_id, session_id),
            token,
            self._ttl_milliseconds,
        )
        return int(result or 0) == 1

    async def release(
        self,
        tenant_id: str,
        session_id: str,
        token: str,
    ) -> bool:
        result = await self._redis.eval(
            _RELEASE_SCRIPT,
            1,
            session_lock_key(tenant_id, session_id),
            token,
        )
        return int(result or 0) == 1


class ExecutionEpochRepository(Protocol):
    async def begin_execution(
        self,
        session_id: str,
        execution_id: str,
        principal: Principal,
        *,
        lease: SessionLockLease,
        mode: ExecutionMode = ExecutionMode.SYNC,
        trace_id: str | None = None,
        request_input: str | None = None,
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class ExecutionFence:
    tenant_id: str
    session_id: str
    execution_id: str
    execution_epoch: int
    worker_id: str
    lease: SessionLockLease


class SessionExecutionCoordinator:
    """Pairs the Redis lease with the authoritative PostgreSQL epoch CAS."""

    def __init__(self, lock_manager: SessionLockManager) -> None:
        self._lock_manager = lock_manager

    async def begin_execution(
        self,
        repository: ExecutionEpochRepository,
        session_id: str,
        execution_id: str,
        principal: Principal,
        *,
        mode: ExecutionMode = ExecutionMode.SYNC,
        trace_id: str | None = None,
        request_input: str | None = None,
    ) -> ExecutionFence:
        if principal.worker_id is None:
            raise ValueError("execution coordination requires a verified worker identity")
        lease = await self._lock_manager.acquire(
            principal.tenant_id,
            session_id,
            principal.worker_id,
        )
        try:
            epoch = await repository.begin_execution(
                session_id,
                execution_id,
                principal,
                lease=lease,
                mode=mode,
                trace_id=trace_id,
                request_input=request_input,
            )
        except BaseException:
            await lease.release()
            raise
        return ExecutionFence(
            tenant_id=principal.tenant_id,
            session_id=session_id,
            execution_id=execution_id,
            execution_epoch=epoch,
            worker_id=principal.worker_id,
            lease=lease,
        )

__all__ = [
    "AsyncRedis",
    "ExecutionFence",
    "SessionLockLease",
    "SessionLockManager",
    "SessionLockUnavailable",
    "SessionExecutionCoordinator",
    "session_lock_key",
]
