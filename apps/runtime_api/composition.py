from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

import httpx
import redis.asyncio as redis
from deepagents.backends import StateBackend
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from apps.runtime_api.routes.events import (
    BoundedLiveEventFeed,
    EventRepositoryReader,
    SSEEventStream,
)
from apps.runtime_api.routes.status import EnvironmentRuntimeStatus
from packages.deepagents_adapter.factory import AgentFactory
from packages.deepagents_adapter.version import assert_compatible_deepagents
from packages.event_model.emitter import RuntimeEventEmitter
from packages.event_model.metrics import RuntimeMetrics
from packages.event_model.payloads import PayloadOffloader
from packages.event_model.projection import PostgresTraceProjectionStore
from packages.execution_manager.queue import RedisTaskQueue
from packages.execution_manager.service import (
    ExecutionManager,
    RepositoryEventSink,
)
from packages.model_gateway.client import ModelGatewayClient, ModelGatewayConfig
from packages.object_store.client import MinioObjectStoreClient
from packages.package_loader.service import PackageLoader
from packages.runtime_contracts import (
    AgentPackageRef,
    ExecutionMode,
    ExecutionStatus,
    Principal,
    RuntimeEvent,
    RuntimeType,
)
from packages.runtime_contracts import RuntimeError as RuntimeContractError
from packages.runtime_persistence.database import (
    create_runtime_engine,
    create_session_factory,
)
from packages.runtime_persistence.repositories import (
    EventRepository,
    ExecutionLease,
    ExecutionRepository,
)
from packages.session_manager.checkpointer import create_async_postgres_saver
from packages.session_manager.locks import SessionExecutionCoordinator, SessionLockManager
from packages.session_manager.service import (
    PostgresPackageCatalog,
    PostgresSessionStore,
    SessionManager,
)
from packages.tool_gateway.query_metric import QueryMetricClient, QueryMetricConfig
from packages.tool_gateway.runtime_context import current_runtime_tool_context
from packages.tool_gateway.service import LocalSequencedToolEventSink, ToolGateway


class RuntimeConfigurationError(ValueError):
    """Raised when the production composition cannot be built safely."""


def _required(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name, "").strip()
    if not value:
        raise RuntimeConfigurationError(f"{name} is required for production composition")
    return value


def _health_url(base_url: str) -> str | None:
    parsed = urlsplit(base_url)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/health"


def _probe_http_provider(base_url: str, api_key: str) -> bool:
    health_url = _health_url(base_url)
    if health_url is None:
        return False
    try:
        with httpx.Client(timeout=2.0, trust_env=False) as client:
            response = client.get(
                health_url,
                headers={"Authorization": f"Bearer {api_key}"},
            )
        return response.is_success
    except httpx.HTTPError:
        return False


async def _probe_redis(redis_client: Any) -> bool:
    try:
        return bool(await redis_client.ping())
    except Exception:
        return False


async def _probe_minio(client: Minio, bucket_name: str) -> bool:
    try:
        return bool(await asyncio.to_thread(client.bucket_exists, bucket_name))
    except Exception:
        return False


@dataclass(frozen=True, slots=True)
class RuntimeCompositionConfig:
    database_url: str
    redis_url: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    minio_bucket: str
    model_base_url: str
    model_api_key: str
    model_name: str
    tool_gateway_url: str
    tool_gateway_api_key: str
    worker_id: str
    tenant_id: str
    agent_id: str
    agent_version: str
    package_digest: str
    packages_root: Path
    deepagents_sdk_version: str = "0.7.7"

    @classmethod
    def from_env(cls, environ: Mapping[str, str]) -> RuntimeCompositionConfig:
        sdk_version = environ.get("RUNTIME_DEEPAGENTS_SDK_VERSION", "0.7.7").strip()
        if sdk_version != "0.7.7":
            raise RuntimeConfigurationError(
                "RUNTIME_DEEPAGENTS_SDK_VERSION must be pinned to 0.7.7"
            )
        return cls(
            database_url=_required(environ, "RUNTIME_DATABASE_URL"),
            redis_url=_required(environ, "RUNTIME_REDIS_URL"),
            minio_endpoint=_required(environ, "RUNTIME_MINIO_ENDPOINT"),
            minio_access_key=_required(environ, "RUNTIME_MINIO_ACCESS_KEY"),
            minio_secret_key=_required(environ, "RUNTIME_MINIO_SECRET_KEY"),
            minio_secure=environ.get("RUNTIME_MINIO_SECURE", "0") == "1",
            minio_bucket=_required(environ, "RUNTIME_MINIO_BUCKET"),
            model_base_url=_required(environ, "MODEL_GATEWAY_BASE_URL"),
            model_api_key=_required(environ, "MODEL_GATEWAY_API_KEY"),
            model_name=_required(environ, "MODEL_GATEWAY_MODEL"),
            tool_gateway_url=_required(environ, "RUNTIME_TOOL_GATEWAY_URL"),
            tool_gateway_api_key=_required(environ, "RUNTIME_TOOL_GATEWAY_API_KEY"),
            worker_id=_required(environ, "RUNTIME_WORKER_ID"),
            tenant_id=_required(environ, "RUNTIME_DEFAULT_TENANT_ID"),
            agent_id=_required(environ, "RUNTIME_DEFAULT_AGENT_ID"),
            agent_version=_required(environ, "RUNTIME_DEFAULT_AGENT_VERSION"),
            package_digest=_required(environ, "RUNTIME_DEFAULT_PACKAGE_DIGEST"),
            packages_root=Path(environ.get("RUNTIME_PACKAGES_ROOT", "agents")),
            deepagents_sdk_version=sdk_version,
        )

    @property
    def package_ref(self) -> AgentPackageRef:
        return AgentPackageRef(
            tenant_id=self.tenant_id,
            agent_id=self.agent_id,
            version=self.agent_version,
            digest=self.package_digest,
            runtime_type=RuntimeType.DEEPAGENTS,
            sdk_version=self.deepagents_sdk_version,
        )


class SessionFactoryExecutionRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def begin_execution(
        self,
        session_id: str,
        execution_id: str,
        principal: Principal,
        *,
        lease: ExecutionLease,
        mode: ExecutionMode,
        trace_id: str | None = None,
        request_input: str | None = None,
    ) -> int:
        async with self._session_factory() as session:
            return await ExecutionRepository(session).begin_execution(
                session_id,
                execution_id,
                principal,
                lease=lease,
                mode=mode,
                trace_id=trace_id,
                request_input=request_input,
            )

    async def complete_execution(
        self,
        session_id: str,
        execution_id: str,
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        *,
        completed_at: Any = None,
        result_ref: str | None = None,
        error: RuntimeContractError | None = None,
    ) -> None:
        async with self._session_factory() as session:
            await ExecutionRepository(session).complete_execution(
                session_id,
                execution_id,
                principal,
                execution_epoch,
                status,
                completed_at=completed_at,
                result_ref=result_ref,
                error=error,
            )


class SessionFactoryEventRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def append_next(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        async with self._session_factory() as session:
            return await EventRepository(session).append_next(
                event_factory,
                principal,
                execution_epoch,
            )

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        async with self._session_factory() as session:
            return await EventRepository(session).append_terminal(
                event_factory,
                principal,
                execution_epoch,
                status,
                error,
            )


class PublishingEventRepository(SessionFactoryEventRepository):
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        live_feed: BoundedLiveEventFeed,
    ) -> None:
        super().__init__(session_factory)
        self._live_feed = live_feed

    async def append_next(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
    ) -> RuntimeEvent:
        event = await super().append_next(event_factory, principal, execution_epoch)
        self._live_feed.publish_nowait(event)
        return event

    async def append_terminal(
        self,
        event_factory: Callable[[int], RuntimeEvent],
        principal: Principal,
        execution_epoch: int,
        status: ExecutionStatus,
        error: RuntimeContractError | None = None,
    ) -> RuntimeEvent:
        event = await super().append_terminal(
            event_factory,
            principal,
            execution_epoch,
            status,
            error,
        )
        self._live_feed.publish_nowait(event)
        return event


class RequestScopedQueryMetricTool:
    def __init__(self, client: QueryMetricClient) -> None:
        self._client = client

    def as_langchain_tool(self) -> Any:
        def query_metric(metric: str, period: str, org: str) -> object:
            context = current_runtime_tool_context()
            gateway = ToolGateway(
                client=self._client,
                principal=context.principal,
                package_allowlist=("query_metric",),
                event_context=context.event_context,
                event_sink=LocalSequencedToolEventSink(context.audit_events.append),
            )
            return gateway.query_metric(metric=metric, period=period, org=org)

        from langchain_core.tools import StructuredTool

        return StructuredTool.from_function(
            func=query_metric,
            name="query_metric",
            description=(
                "Query one read-only business metric for an allowed period and organization."
            ),
        )


@dataclass(slots=True)
class RuntimeServiceBundle:
    session_manager: SessionManager
    execution_manager: ExecutionManager
    task_service: RedisTaskQueue
    event_stream: SSEEventStream
    metrics: RuntimeMetrics
    runtime_status: EnvironmentRuntimeStatus


class RuntimeComposition:
    """Small production composition root for the API process.

    It deliberately requires every provider and identity setting. No local
    database, in-memory checkpoint, or fake model is substituted when a
    deployment is incomplete.
    """

    def __init__(self, config: RuntimeCompositionConfig) -> None:
        self.config = config
        self._stack = AsyncExitStack()
        self._engine: AsyncEngine | None = None
        self._model: ModelGatewayClient | None = None
        self._tool: QueryMetricClient | None = None
        self._redis: Any = None
        self._bundle: RuntimeServiceBundle | None = None

    async def start(self) -> RuntimeServiceBundle:
        if self._bundle is not None:
            return self._bundle
        self._engine = create_runtime_engine(self.config.database_url)
        self._stack.callback(self._engine.sync_engine.dispose)
        session_factory = create_session_factory(self._engine)
        self._redis = cast(
            Any,
            redis.from_url(  # type: ignore[reportUnknownMemberType]
                self.config.redis_url,
                decode_responses=False,
            ),
        )
        self._stack.push_async_callback(self._redis.aclose)

        checkpoint_url = self.config.database_url.replace(
            "postgresql+asyncpg://", "postgresql://", 1
        )
        checkpointer = await self._stack.enter_async_context(
            create_async_postgres_saver(checkpoint_url)
        )
        await cast(Any, checkpointer).setup()

        minio_client = Minio(
            self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            secure=self.config.minio_secure,
        )
        payload_store = MinioObjectStoreClient(minio_client)
        package_loader = PackageLoader.local(
            self.config.packages_root,
            tenant_id=self.config.tenant_id,
            expected_packages={
                (self.config.agent_id, self.config.agent_version): self.config.package_ref
            },
        )
        loaded_package = package_loader.load(
            self.config.agent_id,
            self.config.agent_version,
        )
        self._model = ModelGatewayClient(
            ModelGatewayConfig(
                base_url=self.config.model_base_url,
                api_key=self.config.model_api_key,
                model_name=self.config.model_name,
            )
        )
        self._stack.callback(self._model.close)
        self._tool = QueryMetricClient(
            QueryMetricConfig(
                base_url=self.config.tool_gateway_url,
                api_key=self.config.tool_gateway_api_key,
            )
        )
        self._stack.callback(self._tool.close)

        live_feed = BoundedLiveEventFeed()
        event_repository = PublishingEventRepository(session_factory, live_feed)
        event_sink = RepositoryEventSink(event_repository)
        metrics = RuntimeMetrics()
        projection_store = PostgresTraceProjectionStore(session_factory)
        emitter = RuntimeEventEmitter(
            event_sink=event_sink,
            projection_store=projection_store,
            payload_offloader=PayloadOffloader(
                payload_store,
                bucket_name=self.config.minio_bucket,
            ),
            metrics=metrics,
        )
        agent_factory = AgentFactory(
            packages={loaded_package.reference.digest: loaded_package},
            models={loaded_package.agent.model.ref: self._model.chat_model},
            tools={
                "query_metric": RequestScopedQueryMetricTool(self._tool).as_langchain_tool()
            },
            checkpointer=checkpointer,
            backend=StateBackend(),
        )
        session_manager = SessionManager(
            PostgresPackageCatalog(session_factory),
            PostgresSessionStore(session_factory),
            package_loader,
        )
        lock_manager = SessionLockManager(self._redis, ttl_seconds=30)
        execution_manager = ExecutionManager(
            agent_factory=cast(Any, agent_factory),
            session_manager=session_manager,
            execution_repository=SessionFactoryExecutionRepository(session_factory),
            execution_coordinator=cast(
                Any,
                SessionExecutionCoordinator(lock_manager),
            ),
            event_sink=event_sink,
            event_emitter=emitter,
            execution_timeout_seconds=float(
                loaded_package.limits.execution_timeout_seconds
            ),
        )
        event_stream = SSEEventStream(
            reader=EventRepositoryReader(session_factory),
            live_source=live_feed,
        )
        task_service = RedisTaskQueue(
            self._redis,
            default_timeout_seconds=float(loaded_package.limits.execution_timeout_seconds),
        )
        try:
            assert_compatible_deepagents()
            sdk_available = True
        except Exception:
            sdk_available = False
        probe_results = {
            "postgres": True,
            "redis": await _probe_redis(self._redis),
            "minio": await _probe_minio(minio_client, self.config.minio_bucket),
            "model_gateway": await asyncio.to_thread(
                _probe_http_provider,
                self.config.model_base_url,
                self.config.model_api_key,
            ),
            "tool_gateway": await asyncio.to_thread(
                _probe_http_provider,
                self.config.tool_gateway_url,
                self.config.tool_gateway_api_key,
            ),
            "deepagents_sdk": sdk_available,
        }
        runtime_status = EnvironmentRuntimeStatus(
            {
                "RUNTIME_DATABASE_URL": self.config.database_url,
                "RUNTIME_REDIS_URL": self.config.redis_url,
                "RUNTIME_MINIO_ENDPOINT": self.config.minio_endpoint,
                "MODEL_GATEWAY_BASE_URL": self.config.model_base_url,
                "MODEL_GATEWAY_API_KEY": self.config.model_api_key,
                "MODEL_GATEWAY_MODEL": self.config.model_name,
                "RUNTIME_TOOL_GATEWAY_URL": self.config.tool_gateway_url,
                "RUNTIME_TOOL_GATEWAY_API_KEY": self.config.tool_gateway_api_key,
                "RUNTIME_DEEPAGENTS_SDK_VERSION": self.config.deepagents_sdk_version,
            },
            metrics_configured=True,
            runtime_configured=True,
            probe_results=probe_results,
        )
        self._bundle = RuntimeServiceBundle(
            session_manager=session_manager,
            execution_manager=execution_manager,
            task_service=task_service,
            event_stream=event_stream,
            metrics=metrics,
            runtime_status=runtime_status,
        )
        return self._bundle

    async def close(self) -> None:
        await self._stack.aclose()


def build_runtime_composition(
    environ: Mapping[str, str],
) -> RuntimeComposition:
    return RuntimeComposition(RuntimeCompositionConfig.from_env(environ))


__all__ = [
    "RuntimeComposition",
    "RuntimeCompositionConfig",
    "RuntimeConfigurationError",
    "RuntimeServiceBundle",
    "build_runtime_composition",
]
