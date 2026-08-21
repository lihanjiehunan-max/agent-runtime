from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from io import BytesIO

import pytest

from packages.event_model.payloads import (
    PayloadOffloader,
    PayloadRetentionReconciler,
    PayloadStorageUnavailable,
    RetentionJob,
    RetentionUnavailable,
    TokenDeltaAggregator,
)
from packages.event_normalizer.deepagents_v3 import NormalizedEvent


class DeterministicObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.uploads: list[tuple[str, str]] = []
        self.deletes: list[tuple[str, str]] = []

    def download_object(self, bucket_name: str, object_name: str, destination: object) -> None:
        del bucket_name, object_name, destination

    def upload_object(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes | BytesIO,
        *,
        content_type: str,
    ) -> None:
        del content_type
        value = data.getvalue() if isinstance(data, BytesIO) else data
        self.objects[(bucket_name, object_name)] = value
        self.uploads.append((bucket_name, object_name))

    def delete_object(self, bucket_name: str, object_name: str) -> None:
        self.deletes.append((bucket_name, object_name))
        self.objects.pop((bucket_name, object_name), None)

    def list_objects(self, bucket_name: str, prefix: str, *, max_keys: int) -> Iterable[str]:
        keys = [
            object_name
            for bucket, object_name in self.objects
            if bucket == bucket_name and object_name.startswith(prefix)
        ]
        return tuple(keys[:max_keys])


class DeterministicReferenceRepository:
    def __init__(self, referenced: Iterable[str] = ()) -> None:
        self.referenced = set(referenced)

    def referenced_payload_refs(self, tenant_id: str, candidates: tuple[str, ...]) -> set[str]:
        del tenant_id
        return self.referenced.intersection(candidates)


class DeterministicRetentionRepository(DeterministicReferenceRepository):
    def __init__(self) -> None:
        super().__init__()
        self.closed_calls: list[tuple[str, datetime, int]] = []
        self.pruned_calls: list[tuple[str, datetime, int]] = []
        self.stream_calls: list[tuple[str, int]] = []

    def close_expired_sessions(self, *, tenant_id: str, now: datetime, limit: int) -> int:
        self.closed_calls.append((tenant_id, now, limit))
        return 2

    def prune_closed_metadata_and_checkpoints(
        self,
        *,
        tenant_id: str,
        before: datetime,
        limit: int,
    ) -> int:
        self.pruned_calls.append((tenant_id, before, limit))
        return 3

    def prune_live_streams_after_persistence(self, *, tenant_id: str, limit: int) -> int:
        self.stream_calls.append((tenant_id, limit))
        return 4


def test_large_payload_is_redacted_and_offloaded_to_immutable_tenant_key() -> None:
    store = DeterministicObjectStore()
    offloader = PayloadOffloader(
        store,
        bucket_name="runtime-payloads",
        threshold_bytes=1024,
    )
    prepared = offloader.prepare(
        tenant_id="tenant-a",
        execution_id="execution-a",
        event_id="event-a",
        payload={
            "authorization": "Bearer do-not-store",
            "api_key": "secret-value",
            "text": "x" * 4096,
        },
    )

    assert prepared.payload == {}
    assert prepared.payload_ref is not None
    assert prepared.payload_ref.startswith("payloads/tenant-a/execution-a/event-a-")
    stored = store.objects[("runtime-payloads", prepared.payload_ref)]
    assert b"do-not-store" not in stored
    assert b"secret-value" not in stored
    assert b"[REDACTED]" in stored
    assert store.uploads == [("runtime-payloads", prepared.payload_ref)]


def test_nested_credential_key_variants_are_redacted_before_upload() -> None:
    store = DeterministicObjectStore()
    offloader = PayloadOffloader(store, bucket_name="runtime-payloads", threshold_bytes=1024)
    prepared = offloader.prepare(
        tenant_id="tenant-a",
        execution_id="execution-a",
        event_id="event-a",
        payload={
            "nested": {
                "x-api-key": "x-api-secret",
                "accessToken": "access-secret",
                "proxy-authorization": "proxy-secret",
                "secretKey": "secret-key-secret",
                "authorizationHeader": "authorization-header-secret",
                "tokenValue": "token-value-secret",
                "clientSecretValue": "client-secret-value-secret",
            },
            "text": "x" * 4096,
        },
    )

    assert prepared.payload_ref is not None
    stored = store.objects[("runtime-payloads", prepared.payload_ref)]
    for secret in (
        b"x-api-secret",
        b"access-secret",
        b"proxy-secret",
        b"secret-key-secret",
        b"authorization-header-secret",
        b"token-value-secret",
        b"client-secret-value-secret",
    ):
        assert secret not in stored
    assert stored.count(b"[REDACTED]") >= 7


def test_small_payload_is_inline_redacted_and_token_deltas_are_aggregated() -> None:
    offloader = PayloadOffloader(
        DeterministicObjectStore(),
        bucket_name="runtime-payloads",
        threshold_bytes=1024,
    )
    prepared = offloader.prepare(
        tenant_id="tenant-a",
        execution_id="execution-a",
        event_id="event-a",
        payload={"authorization": "Bearer hidden", "token_count": 4},
    )
    assert prepared.payload_ref is None
    assert prepared.payload["authorization"] == "[REDACTED]"
    assert prepared.payload["token_count"] == 4

    aggregated = TokenDeltaAggregator().aggregate(
        (
            NormalizedEvent("model.delta", "model", {"text": "hel"}),
            NormalizedEvent("model.delta", "model", {"text": "lo"}),
            NormalizedEvent("model.completed", "model", {"output_tokens": 2}),
        )
    )
    assert [event.event_type for event in aggregated] == ["model.delta", "model.completed"]
    assert aggregated[0].payload == {"text": "hello", "delta_count": 2}


def test_payload_offload_fails_closed_without_object_storage() -> None:
    offloader = PayloadOffloader(
        None,
        bucket_name="runtime-payloads",
        threshold_bytes=1024,
    )
    with pytest.raises(PayloadStorageUnavailable, match="object storage is not configured"):
        offloader.prepare(
            tenant_id="tenant-a",
            execution_id="execution-a",
            event_id="event-a",
            payload={"text": "x" * 4096},
        )


def test_retention_deletes_only_db_unreferenced_tenant_payloads() -> None:
    store = DeterministicObjectStore()
    offloader = PayloadOffloader(store, bucket_name="runtime-payloads", threshold_bytes=1024)
    referenced = offloader.object_key("tenant-a", "execution-a", "event-a", b"ref")
    unreferenced = offloader.object_key("tenant-a", "execution-a", "event-b", b"unref")
    other_tenant = offloader.object_key("tenant-b", "execution-a", "event-c", b"other")
    store.objects.update(
        {
            ("runtime-payloads", referenced): b"ref",
            ("runtime-payloads", unreferenced): b"unref",
            ("runtime-payloads", other_tenant): b"other",
        }
    )
    report = PayloadRetentionReconciler(store, bucket_name="runtime-payloads").delete_unreferenced(
        "tenant-a",
        DeterministicReferenceRepository({referenced}),
        max_objects=10,
    )

    assert report.deleted == (unreferenced,)
    assert ("runtime-payloads", referenced) in store.objects
    assert ("runtime-payloads", other_tenant) in store.objects
    assert ("runtime-payloads", unreferenced) not in store.objects


def test_retention_ignores_objects_outside_generated_payload_key_grammar() -> None:
    store = DeterministicObjectStore()
    offloader = PayloadOffloader(store, bucket_name="runtime-payloads", threshold_bytes=1024)
    valid = offloader.object_key("tenant-a", "execution-a", "event-a", b"valid")
    malformed = "payloads/tenant-a/execution-a/not-generated.json"
    store.objects.update(
        {
            ("runtime-payloads", valid): b"valid",
            ("runtime-payloads", malformed): b"do-not-delete",
        }
    )

    report = PayloadRetentionReconciler(
        store,
        bucket_name="runtime-payloads",
    ).delete_unreferenced(
        "tenant-a",
        DeterministicReferenceRepository(),
        max_objects=10,
    )

    assert report.candidates == (valid,)
    assert ("runtime-payloads", malformed) in store.objects


def test_retention_job_uses_bounded_expiry_and_durable_ordering() -> None:
    repository = DeterministicRetentionRepository()
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    job = RetentionJob(
        repository=repository,
        payload_reconciler=PayloadRetentionReconciler(
            DeterministicObjectStore(),
            bucket_name="runtime-payloads",
        ),
    )

    result = job.run_once(tenant_id="tenant-a", now=now, limit=25)

    assert result.closed_sessions == 2
    assert result.pruned_metadata == 3
    assert result.pruned_live_streams == 4
    assert repository.closed_calls == [("tenant-a", now, 25)]
    assert repository.pruned_calls == [("tenant-a", now - timedelta(days=30), 25)]
    assert repository.stream_calls == [("tenant-a", 25)]


def test_retention_preflights_dependencies_before_closing_sessions() -> None:
    repository = DeterministicRetentionRepository()
    job = RetentionJob(
        repository=repository,
        payload_reconciler=PayloadRetentionReconciler(
            None,
            bucket_name="runtime-payloads",
        ),
    )

    with pytest.raises(RetentionUnavailable, match="object storage"):
        job.run_once(
            tenant_id="tenant-a",
            now=datetime(2026, 8, 20, 12, 0, tzinfo=UTC),
            limit=25,
        )

    assert repository.closed_calls == []
    assert repository.pruned_calls == []
    assert repository.stream_calls == []


@pytest.mark.skip(
    reason=(
        "Live MinIO/PostgreSQL retention verification is explicitly opt-in "
        "and is not run by Task 11"
    ),
)
def test_live_minio_postgres_retention_is_not_claimed() -> None:
    pass
