from __future__ import annotations

import shutil
import tarfile
import tempfile
from collections.abc import Callable
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Protocol, Self, cast

from packages.object_store.client import ObjectStoreClient
from packages.package_loader.validator import (
    MAX_PACKAGE_FILE_BYTES,
    MAX_PACKAGE_FILES,
    normalize_relative_path,
)
from packages.runtime_contracts import AgentPackageRef

MAX_ARCHIVE_MEMBERS = (MAX_PACKAGE_FILES + 1) * 2
MAX_ARCHIVE_PATH_BYTES = 1024
MAX_ARCHIVE_EXTENDED_HEADER_BYTES = 16 * 1024


class PackageSourceErrorCode(StrEnum):
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"


class PackageSourceError(Exception):
    def __init__(self, code: PackageSourceErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class _BoundedTarFile(tarfile.TarFile):
    raw_header_count = 0


class _BoundedTarInfo(tarfile.TarInfo):
    @classmethod
    def fromtarfile(cls, tarfile: tarfile.TarFile) -> Self:
        bounded_archive = cast(_BoundedTarFile, tarfile)
        bounded_archive.raw_header_count += 1
        if bounded_archive.raw_header_count > MAX_ARCHIVE_MEMBERS + 1:
            raise PackageSourceError(
                PackageSourceErrorCode.DOWNLOAD_FAILED,
                "package archive contains too many raw headers",
            )
        return super().fromtarfile(tarfile)

    def _proc_pax(self, archive: tarfile.TarFile) -> tarfile.TarInfo:
        if self.size < 0 or self.size > MAX_ARCHIVE_EXTENDED_HEADER_BYTES:
            raise PackageSourceError(
                PackageSourceErrorCode.DOWNLOAD_FAILED,
                "package archive extended header is too large",
            )
        return tarfile.TarInfo._proc_pax(  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue, reportUnknownVariableType]
            self,
            archive,
        )

    def _proc_gnulong(self, archive: tarfile.TarFile) -> tarfile.TarInfo:
        if self.size < 0 or self.size > MAX_ARCHIVE_EXTENDED_HEADER_BYTES:
            raise PackageSourceError(
                PackageSourceErrorCode.DOWNLOAD_FAILED,
                "package archive GNU long header is too large",
            )
        return tarfile.TarInfo._proc_gnulong(  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue, reportUnknownVariableType]
            self,
            archive,
        )


class PackageSource(Protocol):
    def download(self, package: AgentPackageRef, destination: Path) -> None: ...


PackageObjectKeyFactory = Callable[[AgentPackageRef], str]


def _default_object_key(package: AgentPackageRef) -> str:
    return f"{package.tenant_id}/{package.agent_id}/{package.version}/package.tar.gz"


class MinioPackageSource:
    def __init__(
        self,
        client: ObjectStoreClient,
        *,
        bucket_name: str,
        object_key_factory: PackageObjectKeyFactory = _default_object_key,
    ) -> None:
        if not bucket_name:
            raise ValueError("bucket_name must not be empty")
        self._client = client
        self._bucket_name = bucket_name
        self._object_key_factory = object_key_factory

    def object_key(self, package: AgentPackageRef) -> str:
        object_key = self._object_key_factory(package)
        if not object_key:
            raise ValueError("package object key must not be empty")
        return object_key

    def download(self, package: AgentPackageRef, destination: Path) -> None:
        destination.mkdir(parents=True, exist_ok=False)
        archive_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                prefix=".package-archive-",
                suffix=".tar",
                dir=destination.parent,
                delete=False,
            ) as archive_file:
                archive_path = Path(archive_file.name)
            self._client.download_object(
                self._bucket_name,
                self.object_key(package),
                archive_path,
            )
            _extract_archive(archive_path, destination)
        except PackageSourceError:
            raise
        except Exception as error:
            raise PackageSourceError(
                PackageSourceErrorCode.DOWNLOAD_FAILED,
                "package download failed",
            ) from error
        finally:
            if archive_path is not None:
                archive_path.unlink(missing_ok=True)


def _extract_archive(archive_path: Path, destination: Path) -> None:
    _preflight_archive(archive_path)
    with _open_archive(archive_path) as archive:
        for member in archive:
            normalized = _validated_member_path(member)
            target = destination.joinpath(*PurePosixPath(normalized).parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            source = archive.extractfile(member)
            if source is None:
                raise PackageSourceError(
                    PackageSourceErrorCode.DOWNLOAD_FAILED,
                    "package archive entry cannot be read",
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            with source, target.open("xb") as output:
                shutil.copyfileobj(source, output)


def _preflight_archive(archive_path: Path) -> None:
    seen_paths: set[str] = set()
    regular_files = 0
    archive_members = 0
    with _open_archive(archive_path) as archive:
        for member in archive:
            archive_members += 1 + len(member.pax_headers)
            if archive_members > MAX_ARCHIVE_MEMBERS:
                raise PackageSourceError(
                    PackageSourceErrorCode.DOWNLOAD_FAILED,
                    "package archive contains too many members",
                )
            normalized = _validated_member_path(member)
            if normalized in seen_paths:
                raise PackageSourceError(
                    PackageSourceErrorCode.DOWNLOAD_FAILED,
                    "package archive contains duplicate paths",
                )
            seen_paths.add(normalized)
            if member.isdir():
                continue
            regular_files += 1
            if regular_files > MAX_PACKAGE_FILES + 1:
                raise PackageSourceError(
                    PackageSourceErrorCode.DOWNLOAD_FAILED,
                    "package archive contains too many files",
                )


def _open_archive(archive_path: Path) -> tarfile.TarFile:
    return _BoundedTarFile.open(
        archive_path,
        mode="r|*",
        tarinfo=_BoundedTarInfo,
    )


def _validated_member_path(member: tarfile.TarInfo) -> str:
    if len(member.name.encode("utf-8")) > MAX_ARCHIVE_PATH_BYTES:
        raise PackageSourceError(
            PackageSourceErrorCode.DOWNLOAD_FAILED,
            "package archive member path is too long",
        )
    extended_header_bytes = sum(
        len(key.encode("utf-8")) + len(value.encode("utf-8"))
        for key, value in member.pax_headers.items()
    )
    if extended_header_bytes > MAX_ARCHIVE_EXTENDED_HEADER_BYTES:
        raise PackageSourceError(
            PackageSourceErrorCode.DOWNLOAD_FAILED,
            "package archive member header is too large",
        )
    if member.isdir():
        if member.size != 0:
            raise PackageSourceError(
                PackageSourceErrorCode.DOWNLOAD_FAILED,
                "package archive directory header declares content",
            )
    elif not member.isfile() or member.size < 0 or member.size > MAX_PACKAGE_FILE_BYTES:
        raise PackageSourceError(
            PackageSourceErrorCode.DOWNLOAD_FAILED,
            "package archive contains an unsupported entry",
        )
    return normalize_relative_path(member.name)


__all__ = [
    "MinioPackageSource",
    "PackageSource",
    "PackageSourceError",
    "PackageSourceErrorCode",
]
