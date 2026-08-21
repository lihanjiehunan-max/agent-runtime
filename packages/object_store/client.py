from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Protocol

from minio import Minio


class ObjectStoreClient(Protocol):
    def download_object(
        self,
        bucket_name: str,
        object_name: str,
        destination: Path,
    ) -> None: ...


class MinioObjectStoreClient:
    """Small MinIO boundary used by package sources and replaceable in tests."""

    def __init__(self, client: Minio) -> None:
        self._client = client

    def download_object(
        self,
        bucket_name: str,
        object_name: str,
        destination: Path,
    ) -> None:
        self._client.fget_object(bucket_name, object_name, str(destination))

    def upload_object(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        *,
        content_type: str,
    ) -> None:
        self._client.put_object(
            bucket_name,
            object_name,
            BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

    def delete_object(self, bucket_name: str, object_name: str) -> None:
        self._client.remove_object(bucket_name, object_name)

    def list_objects(
        self,
        bucket_name: str,
        prefix: str,
        *,
        max_keys: int,
    ) -> tuple[str, ...]:
        if max_keys <= 0:
            return ()
        names: list[str] = []
        for item in self._client.list_objects(
            bucket_name,
            prefix=prefix,
            recursive=True,
        ):
            if item.object_name is None:
                continue
            names.append(item.object_name)
            if len(names) >= max_keys:
                break
        return tuple(names)


__all__ = ["MinioObjectStoreClient", "ObjectStoreClient"]
