from pathlib import Path

import pytest

from packages.package_loader.resolver import LocalPackageResolver
from packages.package_loader.schema import PackageErrorCode, PackageLoadError


@pytest.mark.parametrize(
    ("agent_id", "version"),
    [
        ("../escape", "0.1.0"),
        ("agent-metric-query", "../../escape"),
        ("agent/child", "0.1.0"),
    ],
)
def test_resolver_rejects_unsafe_package_identity(
    tmp_path: Path, agent_id: str, version: str
) -> None:
    resolver = LocalPackageResolver(tmp_path)

    with pytest.raises(PackageLoadError) as raised:
        resolver.resolve(agent_id, version)

    assert raised.value.code is PackageErrorCode.INVALID_PATH


def test_resolver_rejects_configured_root_symlink(tmp_path: Path) -> None:
    actual_root = tmp_path / "actual"
    actual_root.mkdir()
    linked_root = tmp_path / "linked"
    linked_root.symlink_to(actual_root, target_is_directory=True)

    with pytest.raises(PackageLoadError) as raised:
        LocalPackageResolver(linked_root)

    assert raised.value.code is PackageErrorCode.SYMLINK_FORBIDDEN


def test_resolver_rejects_package_symlink_that_escapes_root(tmp_path: Path) -> None:
    packages_root = tmp_path / "packages"
    packages_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (packages_root / "agent-metric-query").symlink_to(
        outside, target_is_directory=True
    )
    resolver = LocalPackageResolver(packages_root)

    with pytest.raises(PackageLoadError) as raised:
        resolver.resolve("agent-metric-query", "0.1.0")

    assert raised.value.code is PackageErrorCode.SYMLINK_FORBIDDEN
