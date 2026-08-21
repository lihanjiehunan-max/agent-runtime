from __future__ import annotations

import fcntl
import os
import shutil
import tempfile
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from packages.package_loader.minio_source import PackageSource
from packages.package_loader.schema import LoadedPackage
from packages.package_loader.validator import validate_package
from packages.runtime_contracts import AgentPackageRef
from packages.runtime_contracts.identity import FrozenContract

CACHE_COMPLETE_MARKER = ".verified"
CACHE_PACKAGE_DIRECTORY = "package"


class PackageValidator(Protocol):
    def __call__(
        self,
        root: Path,
        *,
        tenant_id: str,
        expected_ref: AgentPackageRef | None = None,
    ) -> LoadedPackage: ...


def definition_cache_key(digest: str) -> str:
    return f"agent-definition:{digest}"


def _require_immutable_definition(definition: object) -> None:
    if not isinstance(definition, FrozenContract):
        raise TypeError("definition_builder must return an immutable definition")


class PackageCache:
    def __init__(
        self,
        cache_root: Path,
        source: PackageSource,
        *,
        validator: PackageValidator = validate_package,
    ) -> None:
        self._cache_root = cache_root
        self._source = source
        self._validator = validator

    def load(self, expected_ref: AgentPackageRef) -> LoadedPackage:
        self._cache_root.mkdir(parents=True, exist_ok=True)
        cache_entry = self._cache_root / expected_ref.digest
        if self._is_complete(cache_entry, expected_ref.digest):
            return self._validate(cache_entry / CACHE_PACKAGE_DIRECTORY, expected_ref)

        with self._publication_lock(expected_ref.digest):
            if self._is_complete(cache_entry, expected_ref.digest):
                return self._validate(cache_entry / CACHE_PACKAGE_DIRECTORY, expected_ref)
            if cache_entry.exists():
                shutil.rmtree(cache_entry)
            return self._populate(cache_entry, expected_ref)

    def _populate(
        self,
        cache_entry: Path,
        expected_ref: AgentPackageRef,
    ) -> LoadedPackage:
        staging = Path(tempfile.mkdtemp(prefix=".package-", dir=self._cache_root))
        staging_package = staging / CACHE_PACKAGE_DIRECTORY
        try:
            self._source.download(expected_ref, staging_package)
            loaded = self._validate(staging_package, expected_ref)
            staging.rename(cache_entry)
            self._write_complete_marker(cache_entry, expected_ref.digest)
            return loaded.model_copy(
                update={"root": (cache_entry / CACHE_PACKAGE_DIRECTORY).resolve()}
            )
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    @contextmanager
    def _publication_lock(self, digest: str) -> Generator[None]:
        lock_root = self._cache_root.parent / f".{self._cache_root.name}.locks"
        lock_root.mkdir(parents=True, exist_ok=True)
        with (lock_root / f"{digest}.lock").open("a+b") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _validate(self, root: Path, expected_ref: AgentPackageRef) -> LoadedPackage:
        return self._validator(
            root,
            tenant_id=expected_ref.tenant_id,
            expected_ref=expected_ref,
        )

    @staticmethod
    def _is_complete(cache_entry: Path, digest: str) -> bool:
        marker = cache_entry / CACHE_COMPLETE_MARKER
        package_root = cache_entry / CACHE_PACKAGE_DIRECTORY
        try:
            return package_root.is_dir() and marker.read_text(encoding="utf-8") == f"{digest}\n"
        except (OSError, UnicodeError):
            return False

    @staticmethod
    def _write_complete_marker(cache_entry: Path, digest: str) -> None:
        temporary_marker = cache_entry / f"{CACHE_COMPLETE_MARKER}.{uuid4().hex}.tmp"
        temporary_marker.write_text(f"{digest}\n", encoding="utf-8")
        os.replace(temporary_marker, cache_entry / CACHE_COMPLETE_MARKER)


@dataclass
class _DefinitionEntry[DefinitionT: FrozenContract]:
    definition: DefinitionT
    last_access: float


@dataclass
class _Flight[DefinitionT: FrozenContract]:
    completed: threading.Event
    definition: DefinitionT | None = None
    error: Exception | None = None


class SingleflightLoader[DefinitionT: FrozenContract]:
    def __init__(
        self,
        package_cache: PackageCache,
        definition_builder: Callable[[LoadedPackage], DefinitionT],
        *,
        max_entries: int,
        idle_ttl_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        if idle_ttl_seconds <= 0:
            raise ValueError("idle_ttl_seconds must be positive")
        self._package_cache = package_cache
        self._definition_builder = definition_builder
        self._max_entries = max_entries
        self._idle_ttl_seconds = idle_ttl_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._definitions: OrderedDict[str, _DefinitionEntry[DefinitionT]] = OrderedDict()
        self._inflight: dict[str, _Flight[DefinitionT]] = {}

    def load(self, expected_ref: AgentPackageRef) -> DefinitionT:
        key = definition_cache_key(expected_ref.digest)
        now = self._clock()
        with self._lock:
            self._evict_idle(now)
            cached = self._definitions.get(key)
            if cached is not None:
                cached.last_access = now
                self._definitions.move_to_end(key)
                return cached.definition
            flight = self._inflight.get(key)
            if flight is None:
                flight = _Flight[DefinitionT](completed=threading.Event())
                self._inflight[key] = flight
                leader = True
            else:
                leader = False

        if not leader:
            flight.completed.wait()
            if flight.error is not None:
                raise flight.error
            if flight.definition is None:
                raise RuntimeError("singleflight completed without a definition")
            return flight.definition

        try:
            loaded = self._package_cache.load(expected_ref)
            definition = self._definition_builder(loaded)
            _require_immutable_definition(definition)
        except Exception as error:
            with self._lock:
                flight.error = error
                self._inflight.pop(key, None)
                flight.completed.set()
            raise

        with self._lock:
            self._definitions[key] = _DefinitionEntry(
                definition=definition,
                last_access=self._clock(),
            )
            self._definitions.move_to_end(key)
            while len(self._definitions) > self._max_entries:
                self._definitions.popitem(last=False)
            flight.definition = definition
            self._inflight.pop(key, None)
            flight.completed.set()
        return definition

    def _evict_idle(self, now: float) -> None:
        expired = [
            key
            for key, entry in self._definitions.items()
            if now - entry.last_access >= self._idle_ttl_seconds
        ]
        for key in expired:
            del self._definitions[key]


__all__ = [
    "CACHE_COMPLETE_MARKER",
    "CACHE_PACKAGE_DIRECTORY",
    "PackageCache",
    "PackageValidator",
    "SingleflightLoader",
    "definition_cache_key",
]
