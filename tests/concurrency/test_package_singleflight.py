from __future__ import annotations

import shutil
import threading
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import pytest
from pydantic import ValidationError

from packages.package_loader.cache import (
    PackageCache,
    SingleflightLoader,
    definition_cache_key,
)
from packages.package_loader.schema import AgentDefinition, LoadedPackage
from packages.package_loader.validator import validate_package
from packages.runtime_contracts import AgentPackageRef
from tests.unit.package_loader._package_builder import (
    build_package,
    package_ref,
    rewrite_integrity,
)


class DirectoryPackageSource:
    def __init__(self, roots: dict[str, Path], *, delay: float = 0.0) -> None:
        self._roots = roots
        self._delay = delay
        self._lock = threading.Lock()
        self.downloads: Counter[str] = Counter()

    def download(self, package: AgentPackageRef, destination: Path) -> None:
        with self._lock:
            self.downloads[package.digest] += 1
        if self._delay:
            time.sleep(self._delay)
        shutil.copytree(self._roots[package.digest], destination, dirs_exist_ok=True)


class CoordinatedDirectoryPackageSource(DirectoryPackageSource):
    def __init__(self, roots: dict[str, Path]) -> None:
        super().__init__(roots)
        self.second_download_started = threading.Event()

    def download(self, package: AgentPackageRef, destination: Path) -> None:
        with self._lock:
            self.downloads[package.digest] += 1
            download_number = self.downloads[package.digest]
        if download_number == 2:
            self.second_download_started.set()
        shutil.copytree(self._roots[package.digest], destination, dirs_exist_ok=True)


class CountingValidator:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.calls: Counter[str] = Counter()

    def __call__(
        self,
        root: Path,
        *,
        tenant_id: str,
        expected_ref: AgentPackageRef | None = None,
    ) -> LoadedPackage:
        assert expected_ref is not None
        with self._lock:
            self.calls[expected_ref.digest] += 1
        return validate_package(root, tenant_id=tenant_id, expected_ref=expected_ref)


class DefinitionBuilder:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.calls: Counter[str] = Counter()

    def __call__(self, package: LoadedPackage) -> AgentDefinition:
        with self._lock:
            self.calls[package.reference.digest] += 1
        return package.agent


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _package_variant(root: Path, prompt: str) -> str:
    digest = build_package(root)
    (root / "prompts/system.md").write_text(prompt, encoding="utf-8")
    return rewrite_integrity(root) if prompt != "Answer metric questions.\n" else digest


def test_one_hundred_cold_requests_share_download_validation_and_definition(
    tmp_path: Path,
) -> None:
    package_root = tmp_path / "origin"
    digest = build_package(package_root)
    expected = package_ref(digest)
    source = DirectoryPackageSource({digest: package_root}, delay=0.05)
    validator = CountingValidator()
    builder = DefinitionBuilder()
    loader = SingleflightLoader[AgentDefinition](
        PackageCache(tmp_path / "cache", source, validator=validator),
        builder,
        max_entries=8,
        idle_ttl_seconds=60.0,
    )
    barrier = threading.Barrier(100)

    def load(_request: int) -> AgentDefinition:
        barrier.wait()
        return loader.load(expected)

    with ThreadPoolExecutor(max_workers=100) as executor:
        definitions = list(executor.map(load, range(100)))

    assert source.downloads[digest] == 1
    assert validator.calls[digest] == 1
    assert builder.calls[digest] == 1
    assert all(definition is definitions[0] for definition in definitions)
    with pytest.raises(ValidationError):
        definitions[0].name = "mutable"
    assert definition_cache_key(digest) == f"agent-definition:{digest}"


def test_concurrent_package_caches_preserve_active_marker_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package_root = tmp_path / "origin"
    digest = build_package(package_root)
    expected = package_ref(digest)
    source = CoordinatedDirectoryPackageSource({digest: package_root})
    cache_root = tmp_path / "cache"
    first_cache = PackageCache(cache_root, source)
    second_cache = PackageCache(cache_root, source)
    marker_window_open = threading.Event()
    second_call_started = threading.Event()
    original_marker_writer = cast(
        Callable[[Path, str], None],
        PackageCache._write_complete_marker,  # pyright: ignore[reportPrivateUsage]
    )

    def pause_first_marker(cache_entry: Path, marker_digest: str) -> None:
        marker_window_open.set()
        assert second_call_started.wait(timeout=1.0)
        source.second_download_started.wait(timeout=0.2)
        original_marker_writer(cache_entry, marker_digest)

    monkeypatch.setattr(
        PackageCache,
        "_write_complete_marker",
        staticmethod(pause_first_marker),
    )

    def second_load() -> LoadedPackage:
        second_call_started.set()
        return second_cache.load(expected)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_result = executor.submit(first_cache.load, expected)
        assert marker_window_open.wait(timeout=1.0)
        second_result = executor.submit(second_load)
        loaded = (first_result.result(timeout=2.0), second_result.result(timeout=2.0))

    assert all(package.reference == expected for package in loaded)
    assert source.downloads[digest] == 1
    cache_entry = cache_root / digest
    assert (cache_entry / "package").is_dir()
    assert (cache_entry / ".verified").read_text(encoding="utf-8") == f"{digest}\n"


def test_definition_cache_is_lru_bounded(tmp_path: Path) -> None:
    roots: dict[str, Path] = {}
    refs: list[AgentPackageRef] = []
    for index, prompt in enumerate(("One.\n", "Two.\n", "Three.\n")):
        root = tmp_path / f"origin-{index}"
        digest = _package_variant(root, prompt)
        roots[digest] = root
        refs.append(package_ref(digest))
    source = DirectoryPackageSource(roots)
    builder = DefinitionBuilder()
    loader = SingleflightLoader[AgentDefinition](
        PackageCache(tmp_path / "cache", source),
        builder,
        max_entries=2,
        idle_ttl_seconds=60.0,
    )

    loader.load(refs[0])
    loader.load(refs[1])
    loader.load(refs[0])
    loader.load(refs[2])
    loader.load(refs[1])

    assert builder.calls[refs[0].digest] == 1
    assert builder.calls[refs[1].digest] == 2
    assert builder.calls[refs[2].digest] == 1
    assert source.downloads == Counter({ref.digest: 1 for ref in refs})


def test_definition_cache_expires_after_idle_ttl(tmp_path: Path) -> None:
    package_root = tmp_path / "origin"
    digest = build_package(package_root)
    expected = package_ref(digest)
    source = DirectoryPackageSource({digest: package_root})
    builder = DefinitionBuilder()
    clock = FakeClock()
    loader = SingleflightLoader[AgentDefinition](
        PackageCache(tmp_path / "cache", source),
        builder,
        max_entries=2,
        idle_ttl_seconds=10.0,
        clock=clock,
    )

    first = loader.load(expected)
    clock.now = 9.0
    assert loader.load(expected) is first
    clock.now = 20.0
    second = loader.load(expected)

    assert second is not first
    assert second == first
    assert builder.calls[digest] == 2
    assert source.downloads[digest] == 1


def test_singleflight_rejects_mutable_definition(tmp_path: Path) -> None:
    package_root = tmp_path / "origin"
    digest = build_package(package_root)
    expected = package_ref(digest)
    source = DirectoryPackageSource({digest: package_root})

    class MutableDefinition:
        pass

    def build_mutable(_package: LoadedPackage) -> AgentDefinition:
        return cast(AgentDefinition, MutableDefinition())

    loader = SingleflightLoader[AgentDefinition](
        PackageCache(tmp_path / "cache", source),
        build_mutable,
        max_entries=1,
        idle_ttl_seconds=60.0,
    )

    with pytest.raises(TypeError, match="immutable definition"):
        loader.load(expected)
