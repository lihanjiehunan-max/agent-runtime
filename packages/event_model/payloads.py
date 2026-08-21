from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Protocol, cast

from pydantic import JsonValue

from packages.event_normalizer.deepagents_v3 import NormalizedEvent, sanitize_output
from packages.runtime_contracts.events import MAX_EVENT_PAYLOAD_BYTES

PAYLOAD_OFFLOAD_THRESHOLD_BYTES = 48 * 1024
PAYLOAD_KEY_PREFIX = "payloads"
RETENTION_DAYS = 30
MAX_RETENTION_OBJECTS = 500
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,253}$")
_IDENTIFIER_PATTERN_TEXT = r"[A-Za-z0-9][A-Za-z0-9_-]{0,253}"


class PayloadStorageUnavailable(RuntimeError):
    """Raised when a large payload needs storage but MinIO is not configured."""


class RetentionUnavailable(RuntimeError):
    """Raised when a retention operation lacks an explicit durable dependency."""


class RetentionBoundExceeded(RuntimeError):
    """Raised before retention can delete more objects than its explicit bound."""


class PayloadObjectStore(Protocol):
    def upload_object(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        *,
        content_type: str,
    ) -> None: ...

    def delete_object(self, bucket_name: str, object_name: str) -> None: ...

    def list_objects(
        self,
        bucket_name: str,
        prefix: str,
        *,
        max_keys: int,
    ) -> Iterable[str]: ...


@dataclass(frozen=True, slots=True)
class PreparedPayload:
    payload: Mapping[str, JsonValue]
    payload_ref: str | None
    size_bytes: int


def _require_identifier(value: str, field_name: str) -> str:
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a safe runtime identifier")
    return value


def _canonical_json(payload: Mapping[str, JsonValue]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


class PayloadOffloader:
    """Redact event payloads and offload only bodies above a safe envelope margin."""

    def __init__(
        self,
        object_store: PayloadObjectStore | None,
        *,
        bucket_name: str,
        threshold_bytes: int = PAYLOAD_OFFLOAD_THRESHOLD_BYTES,
        key_prefix: str = PAYLOAD_KEY_PREFIX,
    ) -> None:
        if threshold_bytes <= 0 or threshold_bytes >= MAX_EVENT_PAYLOAD_BYTES:
            raise ValueError(
                "payload offload threshold must be positive and below the event envelope"
            )
        if not bucket_name or "/" in bucket_name or "\\" in bucket_name:
            raise ValueError("bucket name must be a single safe segment")
        if not key_prefix or "/" in key_prefix or "\\" in key_prefix:
            raise ValueError("payload key prefix must be a single safe segment")
        _require_identifier(key_prefix, "payload key prefix")
        self._object_store = object_store
        self.bucket_name = bucket_name
        self.threshold_bytes = threshold_bytes
        self.key_prefix = key_prefix

    def prepare(
        self,
        *,
        tenant_id: str,
        execution_id: str,
        event_id: str,
        payload: Mapping[str, JsonValue],
    ) -> PreparedPayload:
        safe_payload = sanitize_output(payload)
        if not isinstance(safe_payload, dict):
            raise ValueError("normalized event payload must be a JSON object")
        bounded_payload = cast(dict[str, JsonValue], safe_payload)
        encoded = _canonical_json(bounded_payload)
        if len(encoded) <= self.threshold_bytes:
            return PreparedPayload(
                payload=bounded_payload,
                payload_ref=None,
                size_bytes=len(encoded),
            )
        if self._object_store is None:
            raise PayloadStorageUnavailable(
                "object storage is not configured for an offloaded payload"
            )
        payload_ref = self.object_key(tenant_id, execution_id, event_id, encoded)
        self._object_store.upload_object(
            self.bucket_name,
            payload_ref,
            encoded,
            content_type="application/json",
        )
        return PreparedPayload(payload={}, payload_ref=payload_ref, size_bytes=len(encoded))

    def object_key(
        self,
        tenant_id: str,
        execution_id: str,
        event_id: str,
        payload_bytes: bytes,
    ) -> str:
        tenant = _require_identifier(tenant_id, "tenant_id")
        execution = _require_identifier(execution_id, "execution_id")
        event = _require_identifier(event_id, "event_id")
        digest = sha256(payload_bytes).hexdigest()
        return f"{self.key_prefix}/{tenant}/{execution}/{event}-{digest}.json"


class TokenDeltaAggregator:
    """Collapse adjacent model text deltas before durable event persistence."""

    def __init__(self, *, max_text_chars: int = 4096) -> None:
        if max_text_chars <= 0:
            raise ValueError("max_text_chars must be positive")
        self._max_text_chars = max_text_chars

    def aggregate(self, events: Iterable[NormalizedEvent]) -> tuple[NormalizedEvent, ...]:
        result: list[NormalizedEvent] = []
        text_parts: list[str] = []
        delta_count = 0
        first_span_id: str | None = None
        first_parent_span_id: str | None = None

        def flush() -> None:
            nonlocal delta_count, first_parent_span_id, first_span_id, text_parts
            if delta_count == 0:
                return
            result.append(
                NormalizedEvent(
                    event_type="model.delta",
                    phase="model",
                    payload={
                        "text": "".join(text_parts),
                        "delta_count": delta_count,
                    },
                    span_id=first_span_id,
                    parent_span_id=first_parent_span_id,
                )
            )
            text_parts = []
            delta_count = 0
            first_span_id = None
            first_parent_span_id = None

        for event in events:
            text = event.payload.get("text") if event.event_type == "model.delta" else None
            if event.phase == "model" and isinstance(text, str):
                if delta_count == 0:
                    first_span_id = event.span_id
                    first_parent_span_id = event.parent_span_id
                remaining = self._max_text_chars - sum(len(part) for part in text_parts)
                if remaining > 0:
                    text_parts.append(text[:remaining])
                delta_count += 1
                continue
            flush()
            result.append(event)
        flush()
        return tuple(result)


class PayloadReferenceRepository(Protocol):
    def referenced_payload_refs(
        self,
        tenant_id: str,
        candidates: tuple[str, ...],
    ) -> set[str]: ...


@dataclass(frozen=True, slots=True)
class PayloadRetentionReport:
    candidates: tuple[str, ...]
    referenced: tuple[str, ...]
    deleted: tuple[str, ...]


class PayloadRetentionReconciler:
    """Delete explicitly listed, tenant-scoped objects only after DB evidence."""

    def __init__(
        self,
        object_store: PayloadObjectStore | None,
        *,
        bucket_name: str,
        key_prefix: str = PAYLOAD_KEY_PREFIX,
    ) -> None:
        self._object_store = object_store
        self._bucket_name = bucket_name
        self._key_prefix = key_prefix
        if not key_prefix or "/" in key_prefix or "\\" in key_prefix:
            raise ValueError("payload key prefix must be a single safe segment")
        _require_identifier(key_prefix, "payload key prefix")
        self._key_pattern = re.compile(
            rf"^{re.escape(key_prefix)}/{_IDENTIFIER_PATTERN_TEXT}/"
            rf"{_IDENTIFIER_PATTERN_TEXT}/{_IDENTIFIER_PATTERN_TEXT}-[0-9a-f]{{64}}\.json$"
        )

    def delete_unreferenced(
        self,
        tenant_id: str,
        repository: PayloadReferenceRepository | None,
        *,
        max_objects: int = MAX_RETENTION_OBJECTS,
    ) -> PayloadRetentionReport:
        if self._object_store is None or repository is None:
            raise RetentionUnavailable(
                "payload retention requires configured object storage and a durable repository"
            )
        if max_objects <= 0 or max_objects > MAX_RETENTION_OBJECTS:
            raise ValueError("max_objects is outside the bounded retention policy")
        tenant = _require_identifier(tenant_id, "tenant_id")
        prefix = f"{self._key_prefix}/{tenant}/"
        listed = tuple(
            self._object_store.list_objects(
                self._bucket_name,
                prefix,
                max_keys=max_objects + 1,
            )
        )
        if len(listed) > max_objects:
            raise RetentionBoundExceeded(
                f"retention candidate set exceeds the bound of {max_objects} objects"
            )
        candidates = tuple(
            key
            for key in listed
            if self._key_pattern.fullmatch(key) is not None
            and key.startswith(prefix)
        )
        referenced = tuple(
            sorted(
                set(repository.referenced_payload_refs(tenant, candidates)).intersection(
                    candidates
                )
            )
        )
        referenced_set = set(referenced)
        deleted: list[str] = []
        for key in candidates:
            if key in referenced_set:
                continue
            self._object_store.delete_object(self._bucket_name, key)
            deleted.append(key)
        return PayloadRetentionReport(
            candidates=candidates,
            referenced=referenced,
            deleted=tuple(deleted),
        )


class RetentionRepository(PayloadReferenceRepository, Protocol):
    def close_expired_sessions(
        self,
        *,
        tenant_id: str,
        now: datetime,
        limit: int,
    ) -> int: ...

    def prune_closed_metadata_and_checkpoints(
        self,
        *,
        tenant_id: str,
        before: datetime,
        limit: int,
    ) -> int: ...

    def prune_live_streams_after_persistence(self, *, tenant_id: str, limit: int) -> int: ...


@dataclass(frozen=True, slots=True)
class RetentionRunReport:
    closed_sessions: int
    pruned_metadata: int
    pruned_live_streams: int
    payloads: PayloadRetentionReport


class RetentionJob:
    """Run bounded metadata, stream, and payload retention through explicit contracts."""

    def __init__(
        self,
        *,
        repository: RetentionRepository | None,
        payload_reconciler: PayloadRetentionReconciler,
    ) -> None:
        self._repository = repository
        self._payload_reconciler = payload_reconciler

    def run_once(
        self,
        *,
        tenant_id: str,
        now: datetime,
        limit: int = MAX_RETENTION_OBJECTS,
    ) -> RetentionRunReport:
        if self._repository is None:
            raise RetentionUnavailable("retention requires a configured durable repository")
        if limit <= 0 or limit > MAX_RETENTION_OBJECTS:
            raise ValueError("retention limit is outside the bounded policy")
        payloads = self._payload_reconciler.delete_unreferenced(
            tenant_id,
            self._repository,
            max_objects=limit,
        )
        closed_sessions = self._repository.close_expired_sessions(
            tenant_id=tenant_id,
            now=now,
            limit=limit,
        )
        pruned_metadata = self._repository.prune_closed_metadata_and_checkpoints(
            tenant_id=tenant_id,
            before=now - timedelta(days=RETENTION_DAYS),
            limit=limit,
        )
        pruned_live_streams = self._repository.prune_live_streams_after_persistence(
            tenant_id=tenant_id,
            limit=limit,
        )
        return RetentionRunReport(
            closed_sessions=closed_sessions,
            pruned_metadata=pruned_metadata,
            pruned_live_streams=pruned_live_streams,
            payloads=payloads,
        )


__all__ = [
    "MAX_RETENTION_OBJECTS",
    "PAYLOAD_OFFLOAD_THRESHOLD_BYTES",
    "PAYLOAD_KEY_PREFIX",
    "PayloadOffloader",
    "PayloadObjectStore",
    "PayloadReferenceRepository",
    "PayloadRetentionReconciler",
    "PayloadRetentionReport",
    "PayloadStorageUnavailable",
    "RetentionJob",
    "RetentionRepository",
    "RetentionRunReport",
    "RetentionUnavailable",
    "TokenDeltaAggregator",
]
