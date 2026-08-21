from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
from asyncio import Lock
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from collections.abc import AsyncIterator as AsyncIteratorABC
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)

from packages.runtime_contracts import ErrorCode, Principal, RuntimeSession
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.repositories import (
    RuntimeRepositoryError,
    session_advisory_lock_key,
)

CHECKPOINT_AUDIT_SCHEMA_VERSION = "runtime.checkpoint-audit.v1"


class CheckpointerDependencyUnavailable(RuntimeError):
    pass


def async_postgres_saver_available() -> bool:
    try:
        return all(
            importlib.util.find_spec(module_name) is not None
            for module_name in ("langgraph.checkpoint.postgres.aio", "psycopg")
        )
    except ModuleNotFoundError:
        return False


@asynccontextmanager
async def create_async_postgres_saver(
    connection_string: str,
) -> AsyncGenerator[BaseCheckpointSaver[Any]]:
    if not async_postgres_saver_available():
        raise CheckpointerDependencyUnavailable(
            "AsyncPostgresSaver requires langgraph-checkpoint-postgres and psycopg; "
            "an in-memory checkpoint fallback is forbidden"
        )
    try:
        postgres_module = importlib.import_module("langgraph.checkpoint.postgres.aio")
        psycopg_module = importlib.import_module("psycopg")
        rows_module = importlib.import_module("psycopg.rows")
    except ImportError as error:
        raise CheckpointerDependencyUnavailable(
            "AsyncPostgresSaver requires langgraph-checkpoint-postgres and psycopg; "
            "an in-memory checkpoint fallback is forbidden"
        ) from error
    connection = await psycopg_module.AsyncConnection.connect(
        connection_string,
        autocommit=True,
        prepare_threshold=0,
        row_factory=rows_module.dict_row,
    )
    try:
        raw_saver = postgres_module.AsyncPostgresSaver(connection)
        yield FencedAsyncPostgresSaver(raw_saver, connection)
    finally:
        await connection.close()


def checkpoint_namespace(tenant_id: str) -> str:
    return f"tenant:{tenant_id}"


@dataclass(frozen=True, slots=True)
class FencedCheckpointContext:
    tenant_id: str
    user_id: str
    worker_id: str
    session_id: str
    thread_id: str
    execution_id: str
    execution_epoch: int
    package_digest: str

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> FencedCheckpointContext:
        configurable = cast(Mapping[str, Any], config.get("configurable", {}))
        try:
            context = cls(
                tenant_id=str(configurable["runtime_tenant_id"]),
                user_id=str(configurable["runtime_user_id"]),
                worker_id=str(configurable["runtime_worker_id"]),
                session_id=str(configurable["runtime_session_id"]),
                thread_id=str(configurable["thread_id"]),
                execution_id=str(configurable["runtime_execution_id"]),
                execution_epoch=int(configurable["runtime_execution_epoch"]),
                package_digest=str(configurable["runtime_package_digest"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("checkpoint config lacks a complete execution fence") from error
        if context.thread_id != context.session_id:
            raise ValueError("LangGraph thread_id must equal session_id")
        if configurable.get("checkpoint_ns") != checkpoint_namespace(context.tenant_id):
            raise ValueError("checkpoint namespace must be scoped to the verified tenant")
        if not context.worker_id:
            raise ValueError("checkpoint writes require a verified worker identity")
        return context


class CheckpointWriteTransaction(Protocol):
    async def __aenter__(self) -> CheckpointWriteTransaction: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None: ...

    async def verify(self, context: FencedCheckpointContext) -> None: ...

    async def aput(
        self,
        config: dict[str, Any],
        checkpoint: dict[str, Any],
        metadata: dict[str, Any],
        new_versions: dict[str, Any],
        audit: CheckpointAudit,
    ) -> dict[str, Any]: ...


class FencedCheckpointWriter:
    def __init__(
        self,
        transaction_factory: Callable[
            [FencedCheckpointContext], CheckpointWriteTransaction
        ],
    ) -> None:
        self._transaction_factory = transaction_factory

    async def aput(
        self,
        *,
        context: FencedCheckpointContext,
        config: dict[str, Any],
        checkpoint: dict[str, Any],
        metadata: dict[str, Any],
        new_versions: dict[str, Any],
    ) -> dict[str, Any]:
        checkpoint_id = str(checkpoint.get("id", ""))
        audit = CheckpointAudit.from_context(context, checkpoint_id=checkpoint_id)
        audited_metadata = dict(metadata)
        audited_metadata["enterprise_audit"] = dict(audit.metadata)
        async with self._transaction_factory(context) as transaction:
            await transaction.verify(context)
            result = await transaction.aput(
                config,
                checkpoint,
                audited_metadata,
                new_versions,
                audit,
            )
        result_configurable = cast(dict[str, Any], result["configurable"])
        source_configurable = cast(dict[str, Any], config["configurable"])
        for key, value in source_configurable.items():
            if key.startswith("runtime_"):
                result_configurable[key] = value
        return result


@dataclass(frozen=True, slots=True)
class CheckpointAudit:
    tenant_id: str
    session_id: str
    thread_id: str
    execution_id: str
    execution_epoch: int
    package_digest: str
    checkpoint_id: str
    integrity_hash: str

    @classmethod
    def create(
        cls,
        *,
        runtime_session: RuntimeSession,
        execution_id: str,
        execution_epoch: int,
        checkpoint_id: str,
    ) -> CheckpointAudit:
        if runtime_session.thread_id != runtime_session.session_id:
            raise ValueError("LangGraph thread_id must equal session_id")
        if runtime_session.active_execution_id != execution_id:
            raise ValueError("checkpoint execution does not own the active session")
        if runtime_session.execution_epoch != execution_epoch:
            raise ValueError("checkpoint epoch differs from the active session epoch")
        for name, value in (
            ("execution_id", execution_id),
            ("checkpoint_id", checkpoint_id),
        ):
            if not value or len(value) >= 255:
                raise ValueError(f"{name} must contain fewer than 255 characters")
        context = FencedCheckpointContext(
            tenant_id=runtime_session.tenant_id,
            user_id=runtime_session.user_id,
            worker_id="audit-only",
            session_id=runtime_session.session_id,
            thread_id=runtime_session.thread_id,
            execution_id=execution_id,
            execution_epoch=execution_epoch,
            package_digest=runtime_session.package.digest,
        )
        return cls.from_context(context, checkpoint_id=checkpoint_id)

    @classmethod
    def from_context(
        cls,
        context: FencedCheckpointContext,
        *,
        checkpoint_id: str,
    ) -> CheckpointAudit:
        if context.thread_id != context.session_id:
            raise ValueError("LangGraph thread_id must equal session_id")
        for name, value in (
            ("execution_id", context.execution_id),
            ("checkpoint_id", checkpoint_id),
        ):
            if not value or len(value) >= 255:
                raise ValueError(f"{name} must contain fewer than 255 characters")
        metadata: dict[str, str | int] = {
            "checkpoint_id": checkpoint_id,
            "execution_epoch": context.execution_epoch,
            "execution_id": context.execution_id,
            "package_digest": context.package_digest,
            "schema_version": CHECKPOINT_AUDIT_SCHEMA_VERSION,
            "session_id": context.session_id,
            "tenant_id": context.tenant_id,
            "thread_id": context.thread_id,
        }
        canonical = json.dumps(
            metadata,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        integrity_hash = f"sha256:{hashlib.sha256(canonical).hexdigest()}"
        return cls(
            tenant_id=context.tenant_id,
            session_id=context.session_id,
            thread_id=context.thread_id,
            execution_id=context.execution_id,
            execution_epoch=context.execution_epoch,
            package_digest=context.package_digest,
            checkpoint_id=checkpoint_id,
            integrity_hash=integrity_hash,
        )

    @property
    def metadata(self) -> Mapping[str, str | int]:
        return {
            "schema_version": CHECKPOINT_AUDIT_SCHEMA_VERSION,
            "tenant_id": self.tenant_id,
            "session_id": self.session_id,
            "thread_id": self.thread_id,
            "execution_id": self.execution_id,
            "execution_epoch": self.execution_epoch,
            "package_digest": self.package_digest,
            "checkpoint_id": self.checkpoint_id,
            "integrity_hash": self.integrity_hash,
        }


def checkpoint_config(runtime_session: RuntimeSession) -> dict[str, dict[str, str]]:
    if runtime_session.thread_id != runtime_session.session_id:
        raise ValueError("LangGraph thread_id must equal session_id")
    configurable = {
        "thread_id": runtime_session.session_id,
        "checkpoint_ns": checkpoint_namespace(runtime_session.tenant_id),
    }
    if runtime_session.last_checkpoint_id is not None:
        configurable["checkpoint_id"] = runtime_session.last_checkpoint_id
    return {"configurable": configurable}


def checkpoint_write_config(
    runtime_session: RuntimeSession,
    principal: Principal,
    *,
    execution_id: str,
    execution_epoch: int,
) -> dict[str, dict[str, str | int]]:
    if runtime_session.tenant_id != principal.tenant_id:
        raise ValueError("checkpoint tenant must match the verified principal")
    if runtime_session.user_id != principal.user_id:
        raise ValueError("checkpoint user must match the verified principal")
    if principal.worker_id is None:
        raise ValueError("checkpoint writes require a verified worker identity")
    if runtime_session.thread_id != runtime_session.session_id:
        raise ValueError("LangGraph thread_id must equal session_id")
    if runtime_session.active_execution_id != execution_id:
        raise ValueError("checkpoint execution does not own the active session")
    if runtime_session.execution_epoch != execution_epoch:
        raise ValueError("checkpoint epoch differs from the active session epoch")
    configurable: dict[str, str | int] = {
        "thread_id": runtime_session.thread_id,
        "checkpoint_ns": checkpoint_namespace(runtime_session.tenant_id),
        "runtime_tenant_id": runtime_session.tenant_id,
        "runtime_user_id": runtime_session.user_id,
        "runtime_worker_id": principal.worker_id,
        "runtime_session_id": runtime_session.session_id,
        "runtime_execution_id": execution_id,
        "runtime_execution_epoch": execution_epoch,
        "runtime_package_digest": runtime_session.package.digest,
    }
    if runtime_session.last_checkpoint_id is not None:
        configurable["checkpoint_id"] = runtime_session.last_checkpoint_id
    return {"configurable": configurable}


class PsycopgCheckpointTransaction:
    """One atomic PostgreSQL boundary for fence validation and saver mutation."""

    def __init__(
        self,
        raw_saver: Any,
        connection: Any,
        context: FencedCheckpointContext,
        write_lock: Lock,
    ) -> None:
        self._raw_saver = raw_saver
        self._connection = connection
        self._context = context
        self._write_lock = write_lock
        self._transaction: Any = None
        self._verified = False

    async def __aenter__(self) -> PsycopgCheckpointTransaction:
        await self._write_lock.acquire()
        try:
            self._transaction = self._connection.transaction()
            await self._transaction.__aenter__()
            async with self._connection.cursor() as cursor:
                await cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                await cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (
                        session_advisory_lock_key(
                            self._context.tenant_id,
                            self._context.session_id,
                        ),
                    ),
                )
        except BaseException as error:
            if self._transaction is not None:
                await self._transaction.__aexit__(
                    type(error),
                    error,
                    error.__traceback__,
                )
            self._write_lock.release()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        try:
            await self._transaction.__aexit__(exc_type, exc_value, traceback)
        finally:
            self._write_lock.release()

    async def verify(self, context: FencedCheckpointContext) -> None:
        if context != self._context:
            raise ValueError("checkpoint transaction context changed")
        async with self._connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT 1
                FROM runtime_session AS s
                JOIN runtime_execution AS e
                  ON e.tenant_id = s.tenant_id
                 AND e.session_id = s.session_id
                 AND e.execution_id = s.active_execution_id
                 AND e.execution_epoch = s.execution_epoch
                WHERE s.tenant_id = %s
                  AND s.user_id = %s
                  AND s.session_id = %s
                  AND s.thread_id = %s
                  AND s.package_digest = %s
                  AND s.status = 'open'
                  AND s.active_execution_id = %s
                  AND s.execution_epoch = %s
                  AND e.worker_id = %s
                FOR UPDATE OF s, e
                """,
                (
                    context.tenant_id,
                    context.user_id,
                    context.session_id,
                    context.thread_id,
                    context.package_digest,
                    context.execution_id,
                    context.execution_epoch,
                    context.worker_id,
                ),
            )
            matched = await cursor.fetchone()
        if matched is None:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="checkpoint write was rejected before mutation by the execution fence",
                )
            )
        self._verified = True

    async def aput(
        self,
        config: dict[str, Any],
        checkpoint: dict[str, Any],
        metadata: dict[str, Any],
        new_versions: dict[str, Any],
        audit: CheckpointAudit,
    ) -> dict[str, Any]:
        if not self._verified:
            raise RuntimeError("checkpoint fence must be verified before saver mutation")
        result = await self._raw_saver.aput(
            config,
            checkpoint,
            metadata,
            new_versions,
        )
        async with self._connection.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE runtime_session
                SET last_checkpoint_id = %s,
                    revision = revision + 1
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND session_id = %s
                  AND thread_id = %s
                  AND package_digest = %s
                  AND status = 'open'
                  AND active_execution_id = %s
                  AND execution_epoch = %s
                RETURNING last_checkpoint_id
                """,
                (
                    audit.checkpoint_id,
                    self._context.tenant_id,
                    self._context.user_id,
                    self._context.session_id,
                    self._context.thread_id,
                    self._context.package_digest,
                    self._context.execution_id,
                    self._context.execution_epoch,
                ),
            )
            matched = await cursor.fetchone()
        if matched is None:
            raise RuntimeRepositoryError(
                RuntimeContractError(
                    code=ErrorCode.EXECUTION_FENCED,
                    message="checkpoint head update was rejected by the execution fence",
                )
            )
        return cast(dict[str, Any], result)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str,
    ) -> None:
        if not self._verified:
            raise RuntimeError("checkpoint fence must be verified before saver mutation")
        await self._raw_saver.aput_writes(config, writes, task_id, task_path)


class FencedAsyncPostgresSaver(BaseCheckpointSaver[Any]):
    """AsyncPostgresSaver adapter that fences every mutable graph-state write."""

    def __init__(self, raw_saver: Any, connection: Any) -> None:
        if getattr(raw_saver, "conn", None) is not connection:
            raise ValueError(
                "fenced saver and raw AsyncPostgresSaver must share the same psycopg connection"
            )
        super().__init__(serde=raw_saver.serde)
        self._raw_saver = raw_saver
        self._connection = connection
        self._write_lock = Lock()

    async def setup(self) -> None:
        await self._raw_saver.setup()

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return cast(CheckpointTuple | None, await self._raw_saver.aget_tuple(config))

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIteratorABC[CheckpointTuple]:
        async for item in self._raw_saver.alist(
            config,
            filter=filter,
            before=before,
            limit=limit,
        ):
            yield cast(CheckpointTuple, item)

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        context = FencedCheckpointContext.from_config(config)
        writer = FencedCheckpointWriter(
            lambda current: PsycopgCheckpointTransaction(
                self._raw_saver,
                self._connection,
                current,
                self._write_lock,
            )
        )
        result = await writer.aput(
            context=context,
            config=cast(dict[str, Any], config),
            checkpoint=cast(dict[str, Any], checkpoint),
            metadata=cast(dict[str, Any], metadata),
            new_versions=cast(dict[str, Any], new_versions),
        )
        return cast(RunnableConfig, result)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        context = FencedCheckpointContext.from_config(config)
        transaction = PsycopgCheckpointTransaction(
            self._raw_saver,
            self._connection,
            context,
            self._write_lock,
        )
        async with transaction:
            await transaction.verify(context)
            await transaction.aput_writes(config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        del thread_id
        raise RuntimeError(
            "thread deletion is disabled because the saver API lacks an execution fence"
        )


__all__ = [
    "CHECKPOINT_AUDIT_SCHEMA_VERSION",
    "CheckpointAudit",
    "CheckpointerDependencyUnavailable",
    "FencedAsyncPostgresSaver",
    "FencedCheckpointContext",
    "FencedCheckpointWriter",
    "PsycopgCheckpointTransaction",
    "async_postgres_saver_available",
    "checkpoint_config",
    "checkpoint_write_config",
    "create_async_postgres_saver",
]
