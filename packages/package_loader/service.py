from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from uuid import uuid4

from packages.package_loader.resolver import LocalPackageResolver
from packages.package_loader.schema import (
    LoadedPackage,
    PackageErrorCode,
    PackageEventContext,
    PackageLoadError,
)
from packages.package_loader.validator import validate_package
from packages.runtime_contracts import AgentPackageRef, RuntimeEvent

PackageEventSink = Callable[[RuntimeEvent], None]

_EVENTS = (
    ("package.resolve.started", "resolve"),
    ("package.resolve.completed", "resolve"),
    ("package.verify.completed", "verify"),
    ("package.cache.miss", "cache"),
    ("agent.definition.created", "definition"),
)


class PackageLoader:
    def __init__(
        self,
        resolver: LocalPackageResolver,
        *,
        tenant_id: str,
        expected_packages: Mapping[tuple[str, str], AgentPackageRef] | None = None,
        event_context: PackageEventContext | None = None,
        event_sink: PackageEventSink | None = None,
    ) -> None:
        if (event_context is None) != (event_sink is None):
            raise ValueError("event_context and event_sink must be configured together")
        self._resolver = resolver
        self._tenant_id = tenant_id
        self._expected_packages = MappingProxyType(dict(expected_packages or {}))
        self._event_context = event_context
        self._event_sink = event_sink

    @classmethod
    def local(
        cls,
        packages_root: Path,
        *,
        tenant_id: str,
        expected_packages: Mapping[tuple[str, str], AgentPackageRef] | None = None,
        event_context: PackageEventContext | None = None,
        event_sink: PackageEventSink | None = None,
    ) -> PackageLoader:
        return cls(
            LocalPackageResolver(packages_root),
            tenant_id=tenant_id,
            expected_packages=expected_packages,
            event_context=event_context,
            event_sink=event_sink,
        )

    def load(self, agent_id: str, version: str) -> LoadedPackage:
        expected = self._expected_packages.get((agent_id, version))
        if expected is None:
            raise PackageLoadError(
                PackageErrorCode.AUTHORITATIVE_REFERENCE_REQUIRED,
                "an authoritative package reference is required",
            )
        if expected.tenant_id != self._tenant_id:
            raise PackageLoadError(
                PackageErrorCode.MANIFEST_INVALID,
                "expected package tenant does not match loader tenant",
            )
        root = self._resolver.resolve(agent_id, version)
        loaded = validate_package(root, tenant_id=self._tenant_id, expected_ref=expected)
        if loaded.reference.agent_id != agent_id or loaded.reference.version != version:
            raise PackageLoadError(
                PackageErrorCode.MANIFEST_INVALID,
                "resolved manifest identity does not match the requested package key",
            )
        self._emit_events(loaded.reference)
        return loaded

    def _emit_events(self, package: AgentPackageRef) -> None:
        if self._event_context is None or self._event_sink is None:
            return
        context = self._event_context
        payload = {
            "agent_id": package.agent_id,
            "version": package.version,
            "digest": package.digest,
        }
        for offset, (event_type, phase) in enumerate(_EVENTS):
            self._event_sink(
                RuntimeEvent(
                    schema_version="runtime.event.v1",
                    event_id=str(uuid4()),
                    sequence=context.sequence_start + offset,
                    occurred_at=datetime.now(UTC),
                    tenant_id=package.tenant_id,
                    trace_id=context.trace_id,
                    span_id=str(uuid4()),
                    parent_span_id=context.parent_span_id,
                    session_id=context.session_id,
                    execution_id=context.execution_id,
                    package=package,
                    worker_id=context.worker_id,
                    sdk_version=package.sdk_version,
                    event_type=event_type,
                    phase=phase,
                    duration_ms=None,
                    payload=payload,
                    payload_ref=None,
                )
            )


__all__ = ["PackageEventSink", "PackageLoader"]
