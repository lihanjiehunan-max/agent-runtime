from __future__ import annotations

import hashlib
import posixpath
import re
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Protocol, cast

import yaml
from pydantic import ValidationError
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode, Node
from yaml.resolver import BaseResolver

from packages.package_loader.schema import (
    DEEPAGENTS_SDK_VERSION,
    PACKAGE_SCHEMA_VERSION,
    AgentDefinition,
    BackendDefinition,
    ChecksumEntry,
    DeepAgentsRuntime,
    LoadedPackage,
    ObservabilityDefinition,
    PackageErrorCode,
    PackageLimits,
    PackageLoadError,
    PackageManifest,
    ToolBindings,
)
from packages.runtime_contracts import AgentPackageRef, RuntimeType
from packages.runtime_contracts.identity import FrozenContract

MAX_PACKAGE_FILES = 128
MAX_PACKAGE_FILE_BYTES = 1024 * 1024
_CHECKSUM_LINE = re.compile(r"^([0-9a-f]{64})[ \t]+(?:\*| )?(.+)$")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[/\\]")


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


class _ObjectConstructor(Protocol):
    def construct_object(self, node: Node, deep: bool = False) -> object: ...


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: MappingNode,
    deep: bool = False,
) -> dict[object, object]:
    loader.flatten_mapping(node)
    constructor = cast(_ObjectConstructor, loader)
    mapping: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = constructor.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as error:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable mapping key",
                key_node.start_mark,
            ) from error
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = constructor.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def normalize_relative_path(raw_path: str) -> str:
    if not raw_path or "\x00" in raw_path or "\\" in raw_path:
        raise PackageLoadError(PackageErrorCode.INVALID_PATH, "package path is invalid")
    if raw_path.startswith("/") or _WINDOWS_ABSOLUTE.match(raw_path):
        raise PackageLoadError(
            PackageErrorCode.INVALID_PATH, "absolute package paths are forbidden"
        )
    pure_path = PurePosixPath(raw_path)
    if pure_path.is_absolute() or ".." in pure_path.parts:
        raise PackageLoadError(PackageErrorCode.INVALID_PATH, "package path traversal is forbidden")
    normalized = posixpath.normpath(raw_path)
    if normalized in {"", ".", ".."} or normalized.startswith("../"):
        raise PackageLoadError(PackageErrorCode.INVALID_PATH, "package path traversal is forbidden")
    return normalized


def parse_checksums(content: str) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for line_number, line in enumerate(content.splitlines(), start=1):
        if not line.strip():
            continue
        match = _CHECKSUM_LINE.fullmatch(line)
        if match is None:
            raise PackageLoadError(
                PackageErrorCode.CHECKSUM_INVALID,
                f"invalid checksum entry at line {line_number}",
            )
        file_hash, raw_path = match.groups()
        normalized = normalize_relative_path(raw_path)
        if normalized in checksums:
            raise PackageLoadError(
                PackageErrorCode.CHECKSUM_DUPLICATE,
                f"duplicate checksum entry for {normalized}",
            )
        checksums[normalized] = f"sha256:{file_hash}"
    if not checksums:
        raise PackageLoadError(PackageErrorCode.CHECKSUM_INVALID, "checksums.txt is empty")
    return checksums


def canonical_package_digest(path_hashes: Mapping[str, str]) -> str:
    normalized: dict[str, str] = {}
    for raw_path, digest in path_hashes.items():
        path = normalize_relative_path(raw_path)
        if path in normalized:
            raise PackageLoadError(
                PackageErrorCode.CHECKSUM_DUPLICATE,
                f"duplicate canonical path {path}",
            )
        if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
            raise PackageLoadError(
                PackageErrorCode.CHECKSUM_INVALID,
                f"invalid SHA-256 digest for {path}",
            )
        normalized[path] = digest
    canonical = "".join(
        f"{path}\0{normalized[path]}\n" for path in sorted(normalized)
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _read_bounded_bytes(
    path: Path,
    *,
    invalid_code: PackageErrorCode,
    label: str,
) -> bytes:
    try:
        if path.stat().st_size > MAX_PACKAGE_FILE_BYTES:
            raise PackageLoadError(
                PackageErrorCode.FILE_TOO_LARGE,
                f"{label} exceeds the package file size limit",
            )
        with path.open("rb") as stream:
            content = stream.read(MAX_PACKAGE_FILE_BYTES + 1)
    except PackageLoadError:
        raise
    except OSError as error:
        raise PackageLoadError(invalid_code, f"cannot read {label}") from error
    if len(content) > MAX_PACKAGE_FILE_BYTES:
        raise PackageLoadError(
            PackageErrorCode.FILE_TOO_LARGE,
            f"{label} exceeds the package file size limit",
        )
    return content


def _read_bounded_text(
    path: Path,
    *,
    invalid_code: PackageErrorCode,
    label: str,
) -> str:
    content = _read_bounded_bytes(path, invalid_code=invalid_code, label=label)
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PackageLoadError(invalid_code, f"{label} must be UTF-8 text") from error


def _yaml_mapping(path: Path) -> dict[str, object]:
    content = _read_bounded_text(
        path,
        invalid_code=PackageErrorCode.MANIFEST_INVALID,
        label=f"package definition {path.name}",
    )
    try:
        raw_object = cast(
            object,
            yaml.load(
                content,
                Loader=_UniqueKeySafeLoader,
            ),
        )
    except yaml.YAMLError as error:
        raise PackageLoadError(
            PackageErrorCode.MANIFEST_INVALID,
            f"invalid YAML in package definition {path.name}",
        ) from error
    if not isinstance(raw_object, dict):
        raise PackageLoadError(
            PackageErrorCode.MANIFEST_INVALID,
            f"package definition {path.name} must be a string-keyed mapping",
        )
    raw_mapping = cast(dict[object, object], raw_object)
    if not all(isinstance(key, str) for key in raw_mapping):
        raise PackageLoadError(
            PackageErrorCode.MANIFEST_INVALID,
            f"package definition {path.name} must be a string-keyed mapping",
        )
    return {cast(str, key): value for key, value in raw_mapping.items()}


def _load_schema[SchemaT: FrozenContract](
    path: Path, model: type[SchemaT], expected_schema: str
) -> SchemaT:
    raw = _yaml_mapping(path)
    if raw.get("schema_version") != expected_schema:
        raise PackageLoadError(
            PackageErrorCode.SCHEMA_UNSUPPORTED,
            f"unsupported schema in {path.name}",
        )
    try:
        return model.model_validate(raw)
    except ValidationError as error:
        raise PackageLoadError(
            PackageErrorCode.MANIFEST_INVALID,
            f"invalid package definition in {path.name}",
        ) from error


def _load_manifest(path: Path) -> PackageManifest:
    raw = _yaml_mapping(path)
    if raw.get("schema_version") != PACKAGE_SCHEMA_VERSION:
        raise PackageLoadError(
            PackageErrorCode.SCHEMA_UNSUPPORTED,
            "unsupported package manifest schema",
        )
    runtime = raw.get("runtime")
    if not isinstance(runtime, dict):
        raise PackageLoadError(
            PackageErrorCode.RUNTIME_UNSUPPORTED,
            "only the deepagents runtime is supported",
        )
    runtime_mapping = cast(dict[object, object], runtime)
    if runtime_mapping.get("type") != "deepagents":
        raise PackageLoadError(
            PackageErrorCode.RUNTIME_UNSUPPORTED,
            "only the deepagents runtime is supported",
        )
    if runtime_mapping.get("sdk_version") != DEEPAGENTS_SDK_VERSION:
        raise PackageLoadError(
            PackageErrorCode.SDK_INCOMPATIBLE,
            f"package SDK must be {DEEPAGENTS_SDK_VERSION}",
        )
    if raw.get("status") != "active":
        raise PackageLoadError(
            PackageErrorCode.PACKAGE_STATE_INCOMPATIBLE,
            "only active packages can be loaded",
        )
    try:
        return PackageManifest.model_validate(raw)
    except ValidationError as error:
        raise PackageLoadError(
            PackageErrorCode.MANIFEST_INVALID,
            "package manifest is invalid",
        ) from error


def _load_runtime(path: Path) -> DeepAgentsRuntime:
    raw = _yaml_mapping(path)
    if raw.get("schema_version") != "agent.runtime.deepagents.v1":
        raise PackageLoadError(
            PackageErrorCode.SCHEMA_UNSUPPORTED,
            "unsupported schema in runtime.yaml",
        )
    if raw.get("type") != "deepagents":
        raise PackageLoadError(
            PackageErrorCode.RUNTIME_UNSUPPORTED,
            "runtime definition must use deepagents",
        )
    if raw.get("sdk_version") != DEEPAGENTS_SDK_VERSION:
        raise PackageLoadError(
            PackageErrorCode.SDK_INCOMPATIBLE,
            f"runtime SDK must be {DEEPAGENTS_SDK_VERSION}",
        )
    try:
        return DeepAgentsRuntime.model_validate(raw)
    except ValidationError as error:
        raise PackageLoadError(
            PackageErrorCode.MANIFEST_INVALID,
            "invalid package definition in runtime.yaml",
        ) from error


def _reject_symlinks(root: Path) -> None:
    if root.is_symlink():
        raise PackageLoadError(
            PackageErrorCode.SYMLINK_FORBIDDEN,
            "package root cannot be a symlink",
        )
    for path in root.rglob("*"):
        if path.is_symlink():
            raise PackageLoadError(
                PackageErrorCode.SYMLINK_FORBIDDEN,
                "package cannot contain symlinks",
            )


def _package_file(root: Path, raw_path: str) -> tuple[str, Path]:
    normalized = normalize_relative_path(raw_path)
    root_resolved = root.resolve()
    candidate = root.joinpath(*PurePosixPath(normalized).parts)
    try:
        candidate.resolve(strict=False).relative_to(root_resolved)
    except ValueError as error:
        raise PackageLoadError(
            PackageErrorCode.INVALID_PATH,
            "manifest file is outside the package root",
        ) from error
    if not candidate.is_file():
        raise PackageLoadError(
            PackageErrorCode.FILE_MISSING,
            f"required package file {normalized} is missing",
        )
    return normalized, candidate


def _manifest_digest_bytes(manifest_path: Path) -> bytes:
    raw = _yaml_mapping(manifest_path)
    raw.pop("digest", None)
    return yaml.safe_dump(raw, allow_unicode=True, sort_keys=True).encode("utf-8")


def _file_digest(path: Path, *, manifest_path: Path) -> str:
    content = (
        _manifest_digest_bytes(path)
        if path == manifest_path
        else _read_bounded_bytes(
            path,
            invalid_code=PackageErrorCode.MANIFEST_INVALID,
            label=f"package file {path.name}",
        )
    )
    return f"sha256:{hashlib.sha256(content).hexdigest()}"


def _verify_checksums(root: Path, manifest_path: Path) -> tuple[dict[str, str], str]:
    checksums_path = root / "checksums.txt"
    if not checksums_path.is_file():
        raise PackageLoadError(PackageErrorCode.FILE_MISSING, "checksums.txt is missing")
    checksums = parse_checksums(
        _read_bounded_text(
            checksums_path,
            invalid_code=PackageErrorCode.CHECKSUM_INVALID,
            label="checksums.txt",
        )
    )
    package_files = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path != checksums_path
    }
    if len(package_files) > MAX_PACKAGE_FILES:
        raise PackageLoadError(
            PackageErrorCode.MANIFEST_INVALID,
            "package contains too many files",
        )
    missing_checksums = package_files - checksums.keys()
    unknown_checksums = checksums.keys() - package_files
    if missing_checksums or unknown_checksums:
        raise PackageLoadError(
            PackageErrorCode.CHECKSUM_MISSING,
            "checksums.txt must cover every package file exactly once",
        )
    calculated: dict[str, str] = {}
    for relative in sorted(package_files):
        path = root.joinpath(*PurePosixPath(relative).parts)
        calculated[relative] = _file_digest(path, manifest_path=manifest_path)
        if calculated[relative] != checksums[relative]:
            raise PackageLoadError(
                PackageErrorCode.CHECKSUM_MISMATCH,
                f"checksum mismatch for {relative}",
            )
    return calculated, canonical_package_digest(calculated)


def _validate_expected_ref(
    actual: AgentPackageRef, expected: AgentPackageRef | None
) -> None:
    if expected is None:
        return
    if actual != expected:
        code = (
            PackageErrorCode.DIGEST_MISMATCH
            if actual.digest != expected.digest
            else PackageErrorCode.MANIFEST_INVALID
        )
        raise PackageLoadError(code, "package does not match its immutable metadata reference")


def validate_package(
    root: Path,
    *,
    tenant_id: str,
    expected_ref: AgentPackageRef | None = None,
) -> LoadedPackage:
    if expected_ref is None:
        raise PackageLoadError(
            PackageErrorCode.AUTHORITATIVE_REFERENCE_REQUIRED,
            "an authoritative package reference is required",
        )
    if not root.is_dir():
        raise PackageLoadError(PackageErrorCode.PACKAGE_NOT_FOUND, "package root was not found")
    _reject_symlinks(root)
    manifest_path = root / "manifest.yaml"
    if not manifest_path.is_file():
        raise PackageLoadError(PackageErrorCode.FILE_MISSING, "manifest.yaml is missing")
    manifest = _load_manifest(manifest_path)

    file_paths = {
        field: _package_file(root, raw_path)
        for field, raw_path in manifest.files.model_dump().items()
    }
    checksums, computed_digest = _verify_checksums(root, manifest_path)
    if computed_digest != manifest.digest:
        raise PackageLoadError(
            PackageErrorCode.DIGEST_MISMATCH,
            "manifest digest does not match canonical package bytes",
        )

    reference = AgentPackageRef(
        tenant_id=tenant_id,
        agent_id=manifest.agent_id,
        version=manifest.version,
        digest=computed_digest,
        runtime_type=RuntimeType.DEEPAGENTS,
        sdk_version=manifest.runtime.sdk_version,
    )
    _validate_expected_ref(reference, expected_ref)

    agent = _load_schema(file_paths["agent"][1], AgentDefinition, "agent.definition.v1")
    bindings_path = file_paths["tool_bindings"][1]
    bindings_raw = _yaml_mapping(bindings_path)
    if bindings_raw.get("schema_version") != "agent.tool-bindings.v1":
        raise PackageLoadError(
            PackageErrorCode.SCHEMA_UNSUPPORTED,
            "unsupported schema in tool-bindings.yaml",
        )
    try:
        tool_bindings = ToolBindings.model_validate(bindings_raw)
    except ValidationError as error:
        raise PackageLoadError(
            PackageErrorCode.TOOL_BINDING_UNKNOWN,
            "tool bindings contain an unknown or unsupported entry",
        ) from error
    backend = _load_schema(
        file_paths["backend"][1], BackendDefinition, "agent.backend.v1"
    )
    limits = _load_schema(file_paths["limits"][1], PackageLimits, "agent.limits.v1")
    observability = _load_schema(
        file_paths["observability"][1],
        ObservabilityDefinition,
        "agent.observability.v1",
    )
    runtime = _load_runtime(file_paths["runtime"][1])
    if runtime.type != manifest.runtime.type:
        raise PackageLoadError(
            PackageErrorCode.RUNTIME_UNSUPPORTED,
            "runtime definition does not match the manifest",
        )
    if runtime.sdk_version != manifest.runtime.sdk_version:
        raise PackageLoadError(
            PackageErrorCode.SDK_INCOMPATIBLE,
            "runtime SDK does not match the manifest",
        )
    system_prompt = _read_bounded_text(
        file_paths["system_prompt"][1],
        invalid_code=PackageErrorCode.MANIFEST_INVALID,
        label="system prompt",
    )
    if not system_prompt.strip():
        raise PackageLoadError(
            PackageErrorCode.MANIFEST_INVALID,
            "system prompt cannot be empty",
        )

    return LoadedPackage(
        reference=reference,
        root=root.resolve(),
        manifest=manifest,
        agent=agent,
        tool_bindings=tool_bindings,
        backend=backend,
        limits=limits,
        observability=observability,
        runtime=runtime,
        system_prompt=system_prompt,
        checksums=tuple(
            ChecksumEntry(path=path, digest=digest)
            for path, digest in sorted(checksums.items())
        ),
    )


__all__ = [
    "canonical_package_digest",
    "normalize_relative_path",
    "parse_checksums",
    "validate_package",
]
