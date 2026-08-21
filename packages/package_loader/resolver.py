from __future__ import annotations

import re
from pathlib import Path

from packages.package_loader.schema import PackageErrorCode, PackageLoadError

_SAFE_AGENT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,253}$")
_SAFE_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,253}$")


class LocalPackageResolver:
    def __init__(self, packages_root: Path) -> None:
        if packages_root.is_symlink():
            raise PackageLoadError(
                PackageErrorCode.SYMLINK_FORBIDDEN,
                "configured package root cannot be a symlink",
            )
        self._packages_root = packages_root.resolve()

    def resolve(self, agent_id: str, version: str) -> Path:
        if not _SAFE_AGENT_ID.fullmatch(agent_id) or not _SAFE_VERSION.fullmatch(version):
            raise PackageLoadError(
                PackageErrorCode.INVALID_PATH,
                "package identity cannot contain an unsafe path",
            )

        agent_root = self._packages_root / agent_id
        version_root = agent_root / version
        package_root = version_root if version_root.is_dir() else agent_root
        if package_root.is_symlink():
            raise PackageLoadError(
                PackageErrorCode.SYMLINK_FORBIDDEN,
                "package root cannot be a symlink",
            )
        if not package_root.is_dir():
            raise PackageLoadError(
                PackageErrorCode.PACKAGE_NOT_FOUND,
                f"package {agent_id}:{version} was not found",
            )
        try:
            package_root.resolve().relative_to(self._packages_root)
        except ValueError as error:
            raise PackageLoadError(
                PackageErrorCode.INVALID_PATH,
                "resolved package is outside the configured package root",
            ) from error
        return package_root


__all__ = ["LocalPackageResolver"]
