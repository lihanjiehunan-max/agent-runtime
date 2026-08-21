import os
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from importlib import import_module
from typing import cast

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from apps.runtime_api.dependencies import PrincipalVerifier, RuntimeAPIError
from apps.runtime_api.routes.events import SSEEventStream
from apps.runtime_api.routes.events import router as events_router
from apps.runtime_api.routes.executions import ExecutionService
from apps.runtime_api.routes.executions import router as executions_router
from apps.runtime_api.routes.sessions import router as sessions_router
from apps.runtime_api.routes.status import EnvironmentRuntimeStatus, RuntimeStatusProvider
from apps.runtime_api.routes.status import router as status_router
from apps.runtime_api.routes.tasks import TaskService
from apps.runtime_api.routes.tasks import router as tasks_router
from packages.event_model.metrics import RuntimeMetrics
from packages.runtime_contracts import Principal
from packages.session_manager.service import SessionManager


async def health_live() -> dict[str, str]:
    return {"status": "live"}


async def health_ready() -> dict[str, object]:
    return {
        "status": "not_ready",
        "dependencies": {
            "minio": "not_configured",
            "postgres": "not_configured",
            "redis": "not_configured",
        },
    }


def _load_principal_verifier(environment: Mapping[str, str]) -> PrincipalVerifier | None:
    factory_path = environment.get("RUNTIME_PRINCIPAL_VERIFIER_FACTORY", "").strip()
    if not factory_path:
        return None
    module_name, separator, attribute_name = factory_path.partition(":")
    if not separator or not module_name or not attribute_name:
        raise RuntimeError(
            "RUNTIME_PRINCIPAL_VERIFIER_FACTORY must use module:attribute syntax"
        )
    factory = getattr(import_module(module_name), attribute_name, None)
    if not callable(factory):
        raise RuntimeError("configured principal verifier factory is not callable")
    verifier = factory()
    if not callable(verifier):
        raise RuntimeError("configured principal verifier factory returned a non-callable")
    return cast(PrincipalVerifier, verifier)


def _bind_worker_identity(
    verifier: PrincipalVerifier | None,
    environment: Mapping[str, str],
) -> PrincipalVerifier | None:
    expected_worker_id = environment.get("RUNTIME_WORKER_ID", "").strip()
    if verifier is None or not expected_worker_id:
        return verifier

    async def verify(token: str) -> Principal | None:
        principal = await verifier(token)
        if principal is None or principal.worker_id != expected_worker_id:
            return None
        return principal

    return verify


def create_api(
    *,
    principal_verifier: PrincipalVerifier | None = None,
    session_manager: SessionManager | None = None,
    execution_manager: ExecutionService | None = None,
    task_service: TaskService | None = None,
    event_stream: SSEEventStream | None = None,
    metrics: RuntimeMetrics | None = None,
    runtime_status: RuntimeStatusProvider | None = None,
    environ: Mapping[str, str] | None = None,
) -> FastAPI:
    environment = os.environ if environ is None else environ
    allow_dev_auth = environment.get("RUNTIME_ALLOW_DEV_AUTH") == "1"
    production_composition = None
    if (
        environment.get("RUNTIME_ENABLE_PRODUCTION_COMPOSITION") == "1"
        and session_manager is None
        and execution_manager is None
        and task_service is None
        and event_stream is None
    ):
        from apps.runtime_api.composition import build_runtime_composition

        production_composition = build_runtime_composition(environment)
    configured_principal_verifier = (
        principal_verifier or _load_principal_verifier(environment)
    )
    configured_principal_verifier = _bind_worker_identity(
        configured_principal_verifier,
        environment,
    )

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
        if application.state.principal_verifier is None and not application.state.allow_dev_auth:
            raise RuntimeError("production token verifier is required")
        if production_composition is None:
            yield
            return
        try:
            bundle = await production_composition.start()
        except BaseException:
            await production_composition.close()
            raise
        application.state.session_manager = bundle.session_manager
        application.state.execution_manager = bundle.execution_manager
        application.state.task_service = bundle.task_service
        application.state.event_stream = bundle.event_stream
        application.state.runtime_metrics = bundle.metrics
        application.state.runtime_status = bundle.runtime_status
        try:
            yield
        finally:
            await production_composition.close()

    app = FastAPI(title="Enterprise Agent Runtime", lifespan=lifespan)
    app.state.principal_verifier = configured_principal_verifier
    app.state.allow_dev_auth = allow_dev_auth
    app.state.session_manager = session_manager
    app.state.execution_manager = execution_manager
    app.state.task_service = task_service
    app.state.event_stream = event_stream
    app.state.runtime_metrics = metrics or RuntimeMetrics()
    app.state.runtime_status = runtime_status or EnvironmentRuntimeStatus(
        environment,
        metrics_configured=True,
    )

    async def runtime_api_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        runtime_error = cast(RuntimeAPIError, exc)
        return JSONResponse(
            status_code=runtime_error.status_code,
            content=runtime_error.error.to_body(),
        )

    app.add_exception_handler(RuntimeAPIError, runtime_api_error_handler)
    app.include_router(sessions_router)
    app.include_router(executions_router)
    app.include_router(tasks_router)
    app.include_router(events_router)
    app.include_router(status_router)
    app.add_api_route("/health/live", health_live, methods=["GET"])
    async def health_ready_route() -> JSONResponse:
        provider = cast(RuntimeStatusProvider, app.state.runtime_status)
        snapshot = provider.snapshot()
        response_status = (
            status.HTTP_200_OK
            if snapshot.status == "ready"
            else status.HTTP_503_SERVICE_UNAVAILABLE
        )
        return JSONResponse(
            status_code=response_status,
            content=snapshot.model_dump(mode="json"),
        )

    app.add_api_route("/health/ready", health_ready_route, methods=["GET"])
    return app


app = create_api()
