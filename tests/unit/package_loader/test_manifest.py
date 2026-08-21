from pathlib import Path

import pytest

from packages.package_loader.schema import PackageErrorCode, PackageLoadError
from packages.package_loader.validator import validate_package
from tests.unit.package_loader._package_builder import (
    build_package,
    package_ref,
    read_yaml,
    rewrite_integrity,
    write_yaml,
)


def _assert_error(root: Path, code: PackageErrorCode, expected_digest: str) -> None:
    with pytest.raises(PackageLoadError) as raised:
        validate_package(
            root,
            tenant_id="tenant-a",
            expected_ref=package_ref(expected_digest),
        )
    assert raised.value.code is code


def test_missing_manifest_file_is_rejected(tmp_path: Path) -> None:
    digest = build_package(tmp_path)
    (tmp_path / "backend.yaml").unlink()

    _assert_error(tmp_path, PackageErrorCode.FILE_MISSING, digest)


@pytest.mark.parametrize("unsafe_path", ["../outside.yaml", "/tmp/outside.yaml"])
def test_manifest_file_outside_package_root_is_rejected(
    tmp_path: Path, unsafe_path: str
) -> None:
    build_package(tmp_path)
    manifest = read_yaml(tmp_path / "manifest.yaml")
    manifest["files"]["agent"] = unsafe_path
    write_yaml(tmp_path / "manifest.yaml", manifest)
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.INVALID_PATH, digest)


def test_unsupported_manifest_schema_fails_closed(tmp_path: Path) -> None:
    build_package(tmp_path)
    manifest = read_yaml(tmp_path / "manifest.yaml")
    manifest["schema_version"] = "agent.package.v2"
    write_yaml(tmp_path / "manifest.yaml", manifest)
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.SCHEMA_UNSUPPORTED, digest)


def test_unsupported_nested_schema_fails_closed(tmp_path: Path) -> None:
    build_package(tmp_path)
    agent = read_yaml(tmp_path / "agent.yaml")
    agent["schema_version"] = "agent.definition.v2"
    write_yaml(tmp_path / "agent.yaml", agent)
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.SCHEMA_UNSUPPORTED, digest)


def test_runtime_mismatch_fails_closed(tmp_path: Path) -> None:
    build_package(tmp_path)
    runtime = read_yaml(tmp_path / "runtime/deepagents/runtime.yaml")
    runtime["type"] = "other-runtime"
    write_yaml(tmp_path / "runtime/deepagents/runtime.yaml", runtime)
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.RUNTIME_UNSUPPORTED, digest)


def test_runtime_file_must_match_manifest_runtime(tmp_path: Path) -> None:
    build_package(tmp_path)
    runtime = read_yaml(tmp_path / "runtime/deepagents/runtime.yaml")
    runtime["sdk_version"] = "0.7.6"
    write_yaml(tmp_path / "runtime/deepagents/runtime.yaml", runtime)
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.SDK_INCOMPATIBLE, digest)


def test_sdk_mismatch_fails_closed(tmp_path: Path) -> None:
    build_package(tmp_path)
    manifest = read_yaml(tmp_path / "manifest.yaml")
    manifest["runtime"]["sdk_version"] = "0.7.6"
    write_yaml(tmp_path / "manifest.yaml", manifest)
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.SDK_INCOMPATIBLE, digest)


def test_unknown_tool_binding_fails_closed(tmp_path: Path) -> None:
    build_package(tmp_path)
    bindings = read_yaml(tmp_path / "tool-bindings.yaml")
    bindings["allowlist"] = [{"name": "shell", "effect": "read_only"}]
    write_yaml(tmp_path / "tool-bindings.yaml", bindings)
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.TOOL_BINDING_UNKNOWN, digest)


def test_non_read_only_metric_binding_fails_closed(tmp_path: Path) -> None:
    build_package(tmp_path)
    bindings = read_yaml(tmp_path / "tool-bindings.yaml")
    bindings["allowlist"] = [{"name": "query_metric", "effect": "external_outbox"}]
    write_yaml(tmp_path / "tool-bindings.yaml", bindings)
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.TOOL_BINDING_UNKNOWN, digest)


def test_incompatible_package_state_fails_closed(tmp_path: Path) -> None:
    build_package(tmp_path)
    manifest = read_yaml(tmp_path / "manifest.yaml")
    manifest["status"] = "disabled"
    write_yaml(tmp_path / "manifest.yaml", manifest)
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.PACKAGE_STATE_INCOMPATIBLE, digest)


def test_duplicate_manifest_key_is_rejected_as_invalid_yaml(tmp_path: Path) -> None:
    digest = build_package(tmp_path)
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8") + "status: disabled\n",
        encoding="utf-8",
    )

    _assert_error(tmp_path, PackageErrorCode.MANIFEST_INVALID, digest)


def test_duplicate_nested_yaml_key_is_rejected(tmp_path: Path) -> None:
    build_package(tmp_path)
    runtime_path = tmp_path / "runtime/deepagents/runtime.yaml"
    runtime_path.write_text(
        runtime_path.read_text(encoding="utf-8") + "type: other-runtime\n",
        encoding="utf-8",
    )
    digest = rewrite_integrity(tmp_path)

    _assert_error(tmp_path, PackageErrorCode.MANIFEST_INVALID, digest)


def test_oversized_manifest_is_rejected_before_yaml_parsing(tmp_path: Path) -> None:
    digest = build_package(tmp_path)
    (tmp_path / "manifest.yaml").write_bytes(b"x" * 1_048_577)

    with pytest.raises(PackageLoadError) as raised:
        validate_package(
            tmp_path,
            tenant_id="tenant-a",
            expected_ref=package_ref(digest),
        )

    assert raised.value.code.value == "PACKAGE_FILE_TOO_LARGE"
