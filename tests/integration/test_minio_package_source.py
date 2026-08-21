from __future__ import annotations

import io
import os
import shutil
import tarfile
from pathlib import Path

import pytest
from minio import Minio

from packages.object_store.client import MinioObjectStoreClient
from packages.package_loader.cache import (
    CACHE_COMPLETE_MARKER,
    CACHE_PACKAGE_DIRECTORY,
    PackageCache,
)
from packages.package_loader.minio_source import (
    MinioPackageSource,
    PackageSourceError,
    PackageSourceErrorCode,
)
from packages.package_loader.schema import PackageErrorCode, PackageLoadError
from tests.unit.package_loader._package_builder import (
    build_package,
    package_ref,
    rewrite_integrity,
)


class FakeObjectStoreClient:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.available = True
        self.downloads = 0

    def download_object(self, bucket_name: str, object_name: str, destination: Path) -> None:
        self.downloads += 1
        if not self.available:
            raise OSError("object store unavailable")
        destination.write_bytes(self.objects[(bucket_name, object_name)])


def _archive_bytes(package_root: Path) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for path in sorted(package_root.rglob("*")):
            if path.is_file():
                archive.add(path, arcname=path.relative_to(package_root).as_posix())
    return output.getvalue()


def _archive_from_members(members: list[tarfile.TarInfo]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz", format=tarfile.PAX_FORMAT) as archive:
        for member in members:
            archive.addfile(member)
    return output.getvalue()


def _truncated_extended_header(header_type: bytes) -> bytes:
    member = tarfile.TarInfo("extended-header")
    member.type = header_type
    member.size = 1_000_000
    return member.tobuf(format=tarfile.PAX_FORMAT)


def _download_archive(archive_bytes: bytes, destination: Path) -> None:
    expected = package_ref(f"sha256:{'2' * 64}")
    client = FakeObjectStoreClient()
    source = MinioPackageSource(client, bucket_name="agent-packages")
    client.objects[("agent-packages", source.object_key(expected))] = archive_bytes
    source.download(expected, destination)


def _source_with_package(
    tmp_path: Path,
) -> tuple[FakeObjectStoreClient, MinioPackageSource, str]:
    package_root = tmp_path / "origin"
    digest = build_package(package_root)
    expected = package_ref(digest)
    client = FakeObjectStoreClient()
    source = MinioPackageSource(client, bucket_name="agent-packages")
    client.objects[("agent-packages", source.object_key(expected))] = _archive_bytes(package_root)
    return client, source, digest


def test_cold_load_fails_with_download_failed(tmp_path: Path) -> None:
    client = FakeObjectStoreClient()
    client.available = False
    source = MinioPackageSource(client, bucket_name="agent-packages")
    cache = PackageCache(tmp_path / "cache", source)

    with pytest.raises(PackageSourceError) as raised:
        cache.load(package_ref(f"sha256:{'1' * 64}"))

    assert raised.value.code is PackageSourceErrorCode.DOWNLOAD_FAILED
    assert list((tmp_path / "cache").iterdir()) == []


def test_cached_verified_package_loads_during_source_outage(tmp_path: Path) -> None:
    client, source, digest = _source_with_package(tmp_path)
    cache_root = tmp_path / "cache"
    expected = package_ref(digest)

    first = PackageCache(cache_root, source).load(expected)
    client.available = False
    second = PackageCache(cache_root, source).load(expected)

    assert first.reference == expected
    assert second.reference == expected
    assert client.downloads == 1
    cache_entry = cache_root / digest
    assert (cache_entry / CACHE_COMPLETE_MARKER).read_text(encoding="utf-8") == f"{digest}\n"
    assert second.root == (cache_entry / CACHE_PACKAGE_DIRECTORY).resolve()
    assert not any(path.name.startswith(".package-") for path in cache_root.iterdir())


def test_changed_bytes_under_same_object_key_fail_authoritative_digest(
    tmp_path: Path,
) -> None:
    client, source, original_digest = _source_with_package(tmp_path)
    expected = package_ref(original_digest)
    changed_root = tmp_path / "changed"
    shutil.copytree(tmp_path / "origin", changed_root)
    (changed_root / "prompts/system.md").write_text("Tampered prompt.\n", encoding="utf-8")
    changed_digest = rewrite_integrity(changed_root)
    assert changed_digest != original_digest
    object_key = source.object_key(expected)
    client.objects[("agent-packages", object_key)] = _archive_bytes(changed_root)

    with pytest.raises(PackageLoadError) as raised:
        PackageCache(tmp_path / "cache", source).load(expected)

    assert raised.value.code is PackageErrorCode.DIGEST_MISMATCH
    assert not (tmp_path / "cache" / original_digest / CACHE_COMPLETE_MARKER).exists()
    assert client.downloads == 1


def test_cache_ignores_directory_without_success_marker(tmp_path: Path) -> None:
    package_root = tmp_path / "origin"
    digest = build_package(package_root)
    expected = package_ref(digest)
    cache_root = tmp_path / "cache"
    incomplete_package = cache_root / digest / CACHE_PACKAGE_DIRECTORY
    shutil.copytree(package_root, incomplete_package)
    client = FakeObjectStoreClient()
    client.available = False
    source = MinioPackageSource(client, bucket_name="agent-packages")

    with pytest.raises(PackageSourceError) as raised:
        PackageCache(cache_root, source).load(expected)

    assert raised.value.code is PackageSourceErrorCode.DOWNLOAD_FAILED
    assert client.downloads == 1


def test_cache_ignores_malformed_success_marker(tmp_path: Path) -> None:
    package_root = tmp_path / "origin"
    digest = build_package(package_root)
    expected = package_ref(digest)
    cache_root = tmp_path / "cache"
    cache_entry = cache_root / digest
    shutil.copytree(package_root, cache_entry / CACHE_PACKAGE_DIRECTORY)
    (cache_entry / CACHE_COMPLETE_MARKER).write_bytes(b"\xff")
    client = FakeObjectStoreClient()
    client.available = False
    source = MinioPackageSource(client, bucket_name="agent-packages")

    with pytest.raises(PackageSourceError) as raised:
        PackageCache(cache_root, source).load(expected)

    assert raised.value.code is PackageSourceErrorCode.DOWNLOAD_FAILED
    assert client.downloads == 1


def test_package_source_rejects_directory_member_bomb_before_materialization(
    tmp_path: Path,
) -> None:
    members: list[tarfile.TarInfo] = []
    for index in range(1024):
        member = tarfile.TarInfo(f"directories/{index}/")
        member.type = tarfile.DIRTYPE
        members.append(member)
    destination = tmp_path / "download"

    with pytest.raises(PackageSourceError) as raised:
        _download_archive(_archive_from_members(members), destination)

    assert raised.value.code is PackageSourceErrorCode.DOWNLOAD_FAILED
    assert list(destination.iterdir()) == []


def test_package_source_rejects_oversized_member_path_before_materialization(
    tmp_path: Path,
) -> None:
    path = "/".join(f"directory-{index:03d}" for index in range(100)) + "/"
    member = tarfile.TarInfo(path)
    member.type = tarfile.DIRTYPE
    destination = tmp_path / "download"

    with pytest.raises(PackageSourceError) as raised:
        _download_archive(_archive_from_members([member]), destination)

    assert raised.value.code is PackageSourceErrorCode.DOWNLOAD_FAILED
    assert list(destination.iterdir()) == []


def test_package_source_rejects_oversized_extended_header_before_materialization(
    tmp_path: Path,
) -> None:
    member = tarfile.TarInfo("metadata/")
    member.type = tarfile.DIRTYPE
    member.pax_headers = {"comment": "x" * 20_000}
    destination = tmp_path / "download"

    with pytest.raises(PackageSourceError) as raised:
        _download_archive(_archive_from_members([member]), destination)

    assert raised.value.code is PackageSourceErrorCode.DOWNLOAD_FAILED
    assert list(destination.iterdir()) == []


@pytest.mark.parametrize("header_type", [tarfile.XHDTYPE, tarfile.XGLTYPE])
def test_package_source_rejects_extended_header_bomb_before_payload_read(
    tmp_path: Path,
    header_type: bytes,
) -> None:
    destination = tmp_path / "download"

    with pytest.raises(PackageSourceError) as raised:
        _download_archive(_truncated_extended_header(header_type), destination)

    assert raised.value.code is PackageSourceErrorCode.DOWNLOAD_FAILED
    assert raised.value.__cause__ is None
    assert list(destination.iterdir()) == []


@pytest.mark.parametrize("header_type", [tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK])
def test_package_source_rejects_gnu_long_header_bomb_before_payload_read(
    tmp_path: Path,
    header_type: bytes,
) -> None:
    destination = tmp_path / "download"

    with pytest.raises(PackageSourceError) as raised:
        _download_archive(_truncated_extended_header(header_type), destination)

    assert raised.value.code is PackageSourceErrorCode.DOWNLOAD_FAILED
    assert raised.value.__cause__ is None
    assert list(destination.iterdir()) == []


_LIVE_MINIO_ENDPOINT = os.getenv("RUNTIME_TEST_MINIO_ENDPOINT")


@pytest.mark.skipif(
    _LIVE_MINIO_ENDPOINT is None,
    reason="RUNTIME_TEST_MINIO_ENDPOINT is not configured for the live MinIO integration",
)
def test_live_minio_package_source_when_explicitly_configured(tmp_path: Path) -> None:
    endpoint = _LIVE_MINIO_ENDPOINT
    assert endpoint is not None
    bucket_name = os.environ["RUNTIME_TEST_MINIO_BUCKET"]
    digest = os.environ["RUNTIME_TEST_MINIO_PACKAGE_DIGEST"]
    expected = package_ref(digest)
    object_key = os.environ["RUNTIME_TEST_MINIO_PACKAGE_OBJECT_KEY"]
    minio = Minio(
        endpoint=endpoint,
        access_key=os.environ["RUNTIME_TEST_MINIO_ACCESS_KEY"],
        secret_key=os.environ["RUNTIME_TEST_MINIO_SECRET_KEY"],
        secure=os.getenv("RUNTIME_TEST_MINIO_SECURE", "1") == "1",
    )
    source = MinioPackageSource(
        MinioObjectStoreClient(minio),
        bucket_name=bucket_name,
        object_key_factory=lambda _package: object_key,
    )

    loaded = PackageCache(tmp_path / "cache", source).load(expected)

    assert loaded.reference == expected
