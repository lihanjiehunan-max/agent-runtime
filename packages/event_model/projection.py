from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Protocol, cast

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from packages.event_normalizer.deepagents_v3 import sanitize_output
from packages.runtime_contracts import AgentPackageRef, RuntimeEvent
from packages.runtime_persistence.models import (
    RuntimeExecutionRow,
    RuntimeSessionRow,
    TraceProjectionRow,
)

MAX_TRACE_TIMELINE_EVENTS = 512
MAX_TRACE_SUMMARY_BYTES = 48 * 1024
MAX_TIMELINE_PAYLOAD_BYTES = 2048
_TERMINAL_EVENT_STATUS = {
    "execution.succeeded": "succeeded",
    "execution.failed": "failed",
    "execution.timed_out": "timed_out",
    "execution.cancelled": "cancelled",
}
_EVENT_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class TraceProjectionError(ValueError):
    """Raised when an event cannot be safely applied to its execution trace."""


class TraceProjectionUnavailable(RuntimeError):
    """Raised when the durable PostgreSQL projection boundary is absent."""


@dataclass(frozen=True, slots=True)
class TraceIdentity:
    tenant_id: str
    trace_id: str
    session_id: str
    execution_id: str
    package: AgentPackageRef


@dataclass(frozen=True, slots=True)
class TraceTimelineEntry:
    event_id: str
    event_digest: str
    sequence: int
    event_type: str
    phase: str
    span_id: str
    parent_span_id: str | None
    duration_ms: float | None
    payload_ref: str | None
    payload: Mapping[str, JsonValue]

    def to_dict(self) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "event_id": self.event_id,
            "event_digest": self.event_digest,
            "sequence": self.sequence,
            "event_type": self.event_type,
            "phase": self.phase,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "duration_ms": self.duration_ms,
            "payload_ref": self.payload_ref,
        }
        if self.payload:
            result["payload"] = dict(self.payload)
        return result


@dataclass(frozen=True, slots=True)
class TraceProjection:
    tenant_id: str
    trace_id: str
    session_id: str
    execution_id: str
    agent_id: str
    package_version: str
    package_digest: str
    model_ref: str | None
    tool_versions: Mapping[str, JsonValue]
    status: str
    timeline: tuple[TraceTimelineEntry, ...]
    summary: Mapping[str, JsonValue]
    created_at: datetime
    updated_at: datetime

    def to_row(self) -> TraceProjectionRow:
        return TraceProjectionRow(
            tenant_id=self.tenant_id,
            trace_id=self.trace_id,
            session_id=self.session_id,
            execution_id=self.execution_id,
            agent_id=self.agent_id,
            package_version=self.package_version,
            package_digest=self.package_digest,
            model_ref=self.model_ref,
            tool_versions=dict(self.tool_versions),
            status=self.status,
            summary=dict(self.summary),
            event_index={
                entry.event_id: {
                    "sequence": entry.sequence,
                    "event_digest": entry.event_digest,
                }
                for entry in self.timeline
            },
            created_at=self.created_at,
            updated_at=self.updated_at,
        )

    def to_body(self) -> dict[str, JsonValue]:
        return {
            "schema_version": "runtime.trace.v1",
            "tenant_id": self.tenant_id,
            "trace_id": self.trace_id,
            "session_id": self.session_id,
            "execution_id": self.execution_id,
            "agent_id": self.agent_id,
            "package_version": self.package_version,
            "package_digest": self.package_digest,
            "model_ref": self.model_ref,
            "tool_versions": dict(self.tool_versions),
            "status": self.status,
            "timeline": [entry.to_dict() for entry in self.timeline],
            "summary": dict(self.summary),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


def _safe_payload(event: RuntimeEvent) -> dict[str, JsonValue]:
    sanitized = sanitize_output(event.payload)
    if not isinstance(sanitized, dict):
        return {}
    payload = cast(dict[str, JsonValue], sanitized)
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    if len(encoded) <= MAX_TIMELINE_PAYLOAD_BYTES:
        return payload
    return {"truncated": True, "payload_bytes": len(encoded)}


class TraceProjector:
    """Build one bounded projection from the durable normalized event stream."""

    def __init__(
        self,
        identity: TraceIdentity,
        *,
        max_timeline_events: int = MAX_TRACE_TIMELINE_EVENTS,
    ) -> None:
        if max_timeline_events <= 0 or max_timeline_events > MAX_TRACE_TIMELINE_EVENTS:
            raise ValueError("trace timeline bound is outside the supported range")
        self.identity = identity
        self._max_timeline_events = max_timeline_events
        self._event_digests: dict[str, str] = {}
        self._sequence_events: dict[int, str] = {}
        self._timeline: list[TraceTimelineEntry] = []
        self._model_ref: str | None = None
        self._tool_versions: dict[str, JsonValue] = {}
        self._created_at: datetime | None = None
        self._updated_at: datetime | None = None

    def apply(self, event: RuntimeEvent) -> TraceProjection:
        self._validate_identity(event)
        event_digest = sha256(event.to_json().encode("utf-8")).hexdigest()
        previous_digest = self._event_digests.get(event.event_id)
        if previous_digest is not None:
            if previous_digest != event_digest:
                raise TraceProjectionError("duplicate event identity has different content")
            return self.snapshot()
        previous_event_id = self._sequence_events.get(event.sequence)
        if previous_event_id is not None:
            raise TraceProjectionError("event sequence is already projected")
        if len(self._timeline) >= self._max_timeline_events:
            raise TraceProjectionError("trace timeline exceeds its bounded projection size")

        self._event_digests[event.event_id] = event_digest
        self._sequence_events[event.sequence] = event.event_id
        payload = _safe_payload(event)
        self._timeline.append(
            TraceTimelineEntry(
                event_id=event.event_id,
                event_digest=event_digest,
                sequence=event.sequence,
                event_type=event.event_type,
                phase=event.phase,
                span_id=event.span_id,
                parent_span_id=event.parent_span_id,
                duration_ms=event.duration_ms,
                payload_ref=event.payload_ref,
                payload=payload,
            )
        )
        self._timeline.sort(key=lambda entry: entry.sequence)
        self._record_fields(event)
        return self.snapshot()

    def snapshot(self) -> TraceProjection:
        timeline = tuple(self._timeline)
        summary = self._build_summary(timeline)
        created_at = self._created_at or datetime.now(UTC)
        updated_at = self._updated_at or created_at
        return TraceProjection(
            tenant_id=self.identity.tenant_id,
            trace_id=self.identity.trace_id,
            session_id=self.identity.session_id,
            execution_id=self.identity.execution_id,
            agent_id=self.identity.package.agent_id,
            package_version=self.identity.package.version,
            package_digest=self.identity.package.digest,
            model_ref=self._model_ref,
            tool_versions=dict(self._tool_versions),
            status=self._derive_status(timeline),
            timeline=timeline,
            summary=summary,
            created_at=created_at,
            updated_at=updated_at,
        )

    def _validate_identity(self, event: RuntimeEvent) -> None:
        if event.tenant_id != self.identity.tenant_id:
            raise TraceProjectionError("tenant identity does not match trace identity")
        if event.trace_id != self.identity.trace_id:
            raise TraceProjectionError("trace identity does not match projection")
        if event.session_id != self.identity.session_id:
            raise TraceProjectionError("session identity does not match projection")
        if event.execution_id != self.identity.execution_id:
            raise TraceProjectionError("execution identity does not match projection")
        if event.package != self.identity.package:
            raise TraceProjectionError("package identity does not match projection")
        if event.sdk_version != self.identity.package.sdk_version:
            raise TraceProjectionError("SDK identity does not match package identity")

    def _record_fields(self, event: RuntimeEvent) -> None:
        self._created_at = (
            event.occurred_at
            if self._created_at is None
            else min(self._created_at, event.occurred_at)
        )
        self._updated_at = (
            event.occurred_at
            if self._updated_at is None
            else max(self._updated_at, event.occurred_at)
        )
        payload = event.payload
        for key in ("model_ref", "model", "model_name"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                self._model_ref = value[:254]
                break
        tool_name = payload.get("tool_name")
        tool_version = payload.get("tool_version", payload.get("version"))
        if isinstance(tool_name, str) and tool_name and isinstance(tool_version, str):
            self._tool_versions[tool_name[:254]] = tool_version[:254]

    def _build_summary(
        self,
        timeline: tuple[TraceTimelineEntry, ...],
    ) -> dict[str, JsonValue]:
        model_calls = 0
        model_duration = 0.0
        input_tokens = 0
        output_tokens = 0
        tool_calls = 0
        tool_duration = 0.0
        checkpoint_count = 0
        output: dict[str, JsonValue] | None = None
        terminal: dict[str, JsonValue] | None = None
        for entry in timeline:
            if entry.phase == "model":
                model_calls += 1
                model_duration += entry.duration_ms or 0.0
                input_tokens += _nonnegative_int(entry.payload.get("input_tokens"))
                output_tokens += _nonnegative_int(entry.payload.get("output_tokens"))
            if entry.phase == "tool":
                tool_calls += 1
                tool_duration += entry.duration_ms or 0.0
            if entry.phase == "checkpoint":
                checkpoint_count += 1
            if entry.event_type == "execution.output":
                output = dict(entry.payload)
            if entry.event_type in _TERMINAL_EVENT_STATUS:
                terminal = {
                    "event_type": entry.event_type,
                    "status": _TERMINAL_EVENT_STATUS[entry.event_type],
                }
        summary: dict[str, JsonValue] = {
            "event_count": len(timeline),
            "trace_id_coverage": {
                "events": len(timeline),
                "trace_ids": 1 if timeline else 0,
                "complete": True,
            },
            "model": {
                "calls": model_calls,
                "duration_ms": model_duration,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            },
            "tool": {"calls": tool_calls, "duration_ms": tool_duration},
            "checkpoint": {"count": checkpoint_count},
            "timeline": [entry.to_dict() for entry in timeline],
        }
        if output is not None:
            summary["output"] = output
        if terminal is not None:
            summary["terminal"] = terminal
        encoded = json.dumps(summary, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        if len(encoded) <= MAX_TRACE_SUMMARY_BYTES:
            return summary
        fallback: dict[str, JsonValue] = {
            "event_count": len(timeline),
            "trace_id_coverage": summary["trace_id_coverage"],
            "model": summary["model"],
            "tool": summary["tool"],
            "checkpoint": summary["checkpoint"],
            "truncated": True,
        }
        fallback_encoded = json.dumps(
            fallback,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(fallback_encoded) < MAX_TRACE_SUMMARY_BYTES:
            return fallback
        return {
            "event_count": len(timeline),
            "trace_id_coverage": summary["trace_id_coverage"],
            "truncated": True,
        }

    @staticmethod
    def _derive_status(timeline: tuple[TraceTimelineEntry, ...]) -> str:
        status = "accepted"
        for entry in timeline:
            if entry.event_type in _TERMINAL_EVENT_STATUS:
                status = _TERMINAL_EVENT_STATUS[entry.event_type]
            elif entry.event_type == "execution.started":
                status = "running"
            elif entry.event_type == "execution.accepted":
                status = "accepted"
        return status


def _nonnegative_int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


class TraceProjectionStore(Protocol):
    async def save(self, projection: TraceProjection) -> None: ...


class PostgresTraceProjectionStore:
    """Durable projection adapter; it has no in-memory production fallback."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] | None,
    ) -> None:
        if session_factory is None:
            raise TraceProjectionUnavailable("PostgreSQL trace projection is not configured")
        self._session_factory = session_factory

    async def save(self, projection: TraceProjection) -> None:
        async with self._session_factory() as session, session.begin():
            execution = await session.scalar(
                select(RuntimeExecutionRow)
                .where(
                    RuntimeExecutionRow.tenant_id == projection.tenant_id,
                    RuntimeExecutionRow.execution_id == projection.execution_id,
                )
                .with_for_update()
            )
            if execution is None or (
                execution.trace_id != projection.trace_id
                or execution.session_id != projection.session_id
            ):
                raise TraceProjectionError(
                    "durable execution trace identity is forged or unavailable"
                )

            runtime_session = await session.scalar(
                select(RuntimeSessionRow)
                .where(
                    RuntimeSessionRow.tenant_id == projection.tenant_id,
                    RuntimeSessionRow.session_id == projection.session_id,
                )
                .with_for_update()
            )
            if runtime_session is None or (
                runtime_session.tenant_id != projection.tenant_id
                or runtime_session.session_id != projection.session_id
                or runtime_session.agent_id != projection.agent_id
                or runtime_session.package_version != projection.package_version
                or runtime_session.package_digest != projection.package_digest
            ):
                raise TraceProjectionError(
                    "persisted session package identity does not match projection"
                )

            existing = await session.scalar(
                select(TraceProjectionRow)
                .where(
                    TraceProjectionRow.tenant_id == projection.tenant_id,
                    TraceProjectionRow.execution_id == projection.execution_id,
                )
                .with_for_update()
            )
            if existing is None:
                self._validate_projection_events(projection)
                session.add(projection.to_row())
                return

            self._validate_row_identity(existing, projection)
            self._validate_durable_update(existing, projection)
            existing.model_ref = projection.model_ref
            existing.tool_versions = dict(projection.tool_versions)
            existing.status = projection.status
            existing.summary = dict(projection.summary)
            existing.event_index = {
                entry.event_id: {
                    "sequence": entry.sequence,
                    "event_digest": entry.event_digest,
                }
                for entry in projection.timeline
            }
            existing.updated_at = projection.updated_at

    @staticmethod
    def _validate_row_identity(
        existing: TraceProjectionRow,
        projection: TraceProjection,
    ) -> None:
        if (
            existing.tenant_id != projection.tenant_id
            or existing.trace_id != projection.trace_id
            or existing.session_id != projection.session_id
            or existing.execution_id != projection.execution_id
            or existing.agent_id != projection.agent_id
            or existing.package_version != projection.package_version
            or existing.package_digest != projection.package_digest
        ):
            raise TraceProjectionError("durable projection identity is forged")

    @classmethod
    def _validate_durable_update(
        cls,
        existing: TraceProjectionRow,
        projection: TraceProjection,
    ) -> None:
        incoming_by_id = cls._timeline_metadata(projection)
        existing_by_id = cls._timeline_metadata_from_index(existing.event_index)
        for event_id, old in existing_by_id.items():
            current = incoming_by_id.get(event_id)
            if current is None:
                raise TraceProjectionError("durable projection update is stale")
            if current != old:
                raise TraceProjectionError("durable projection event digest conflicts")

        existing_by_sequence = {item["sequence"]: item for item in existing_by_id.values()}
        for item in incoming_by_id.values():
            old = existing_by_sequence.get(item["sequence"])
            if old is not None and old["event_id"] != item["event_id"]:
                raise TraceProjectionError("durable projection sequence conflicts")

        existing_version = max(
            (cast(int, item["sequence"]) for item in existing_by_id.values()),
            default=0,
        )
        incoming_version = max(
            (cast(int, item["sequence"]) for item in incoming_by_id.values()),
            default=0,
        )
        if incoming_version < existing_version:
            raise TraceProjectionError("durable projection version is stale")
        if incoming_version == existing_version:
            if (
                len(incoming_by_id) != len(existing_by_id)
                or dict(existing.summary) != dict(projection.summary)
                or existing.model_ref != projection.model_ref
                or dict(existing.tool_versions) != dict(projection.tool_versions)
                or existing.status != projection.status
            ):
                raise TraceProjectionError("durable projection version conflicts")
            return
        if len(incoming_by_id) <= len(existing_by_id):
            raise TraceProjectionError("durable projection version is not advancing")

    @classmethod
    def _validate_projection_events(cls, projection: TraceProjection) -> None:
        cls._timeline_metadata(projection)

    @staticmethod
    def _timeline_metadata(projection: TraceProjection) -> dict[str, dict[str, object]]:
        return PostgresTraceProjectionStore._timeline_metadata_from_dicts(
            [entry.to_dict() for entry in projection.timeline]
        )

    @staticmethod
    def _timeline_metadata_from_dicts(
        timeline: list[object],
    ) -> dict[str, dict[str, object]]:
        by_id: dict[str, dict[str, object]] = {}
        by_sequence: set[int] = set()
        for raw in timeline:
            if not isinstance(raw, dict):
                raise TraceProjectionError("durable projection timeline is malformed")
            mapping = cast(Mapping[str, object], raw)
            event_id = mapping.get("event_id")
            event_digest = mapping.get("event_digest")
            sequence = mapping.get("sequence")
            if (
                not isinstance(event_id, str)
                or not isinstance(event_digest, str)
                or _EVENT_DIGEST_PATTERN.fullmatch(event_digest) is None
                or not isinstance(sequence, int)
                or sequence <= 0
                or event_id in by_id
                or sequence in by_sequence
            ):
                raise TraceProjectionError("durable projection event metadata is invalid")
            by_id[event_id] = {
                "event_id": event_id,
                "event_digest": event_digest,
                "sequence": sequence,
            }
            by_sequence.add(sequence)
        return by_id

    @staticmethod
    def _timeline_metadata_from_index(
        event_index: Mapping[str, object],
    ) -> dict[str, dict[str, object]]:
        if not event_index:
            raise TraceProjectionError("durable projection metadata is unavailable")
        by_id: dict[str, dict[str, object]] = {}
        by_sequence: set[int] = set()
        for raw_event_id, raw in cast(Mapping[object, object], event_index).items():
            event_id = raw_event_id
            if not isinstance(raw, Mapping):
                raise TraceProjectionError("durable projection event metadata is invalid")
            mapping = cast(Mapping[str, object], raw)
            sequence = mapping.get("sequence")
            event_digest = mapping.get("event_digest")
            if (
                not isinstance(event_id, str)
                or not isinstance(sequence, int)
                or sequence <= 0
                or not isinstance(event_digest, str)
                or _EVENT_DIGEST_PATTERN.fullmatch(event_digest) is None
                or event_id in by_id
                or sequence in by_sequence
            ):
                raise TraceProjectionError("durable projection event metadata is invalid")
            by_id[event_id] = {
                "event_id": event_id,
                "event_digest": event_digest,
                "sequence": sequence,
            }
            by_sequence.add(sequence)
        return by_id


__all__ = [
    "MAX_TRACE_SUMMARY_BYTES",
    "MAX_TRACE_TIMELINE_EVENTS",
    "PostgresTraceProjectionStore",
    "TraceIdentity",
    "TraceProjection",
    "TraceProjectionError",
    "TraceProjectionStore",
    "TraceProjectionUnavailable",
    "TraceProjector",
    "TraceTimelineEntry",
]
