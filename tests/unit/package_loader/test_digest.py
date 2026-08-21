import hashlib
from pathlib import Path
from typing import IO, Any, cast

import pytest

from packages.package_loader.schema import PackageErrorCode, PackageLoadError
from packages.package_loader.service import PackageLoader
from packages.package_loader.validator import (
    canonical_package_digest,
    parse_checksums,
    validate_package,
)
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


def test_canonical_digest_sorts_normalized_path_hash_pairs() -> None:
    pairs = {
        "b.txt": f"sha256:{'b' * 64}",
        "a.txt": f"sha256:{'a' * 64}",
    }

    assert canonical_package_digest(pairs) == (
        "sha256:5ff48a29c8e2b87323d3915674bf64c71efe4fc4c3fb2833aef2c5c5edece372"
    )


def test_duplicate_checksum_entry_is_rejected() -> None:
    checksum = "a" * 64

    with pytest.raises(PackageLoadError) as raised:
        parse_checksums(f"{checksum}  file.txt\n{checksum}  ./file.txt\n")

    assert raised.value.code is PackageErrorCode.CHECKSUM_DUPLICATE


@pytest.mark.parametrize("unsafe_path", ["../escape.txt", "/tmp/escape.txt", "C:/escape.txt"])
def test_unsafe_checksum_path_is_rejected(unsafe_path: str) -> None:
    with pytest.raises(PackageLoadError) as raised:
        parse_checksums(f"{'a' * 64}  {unsafe_path}\n")

    assert raised.value.code is PackageErrorCode.INVALID_PATH


def test_checksum_mismatch_is_rejected(tmp_path: Path) -> None:
    digest = build_package(tmp_path)
    (tmp_path / "prompts/system.md").write_text("tampered\n", encoding="utf-8")

    _assert_error(tmp_path, PackageErrorCode.CHECKSUM_MISMATCH, digest)


def test_declared_digest_mismatch_is_rejected(tmp_path: Path) -> None:
    digest = build_package(tmp_path)
    manifest = read_yaml(tmp_path / "manifest.yaml")
    manifest["digest"] = f"sha256:{'f' * 64}"
    write_yaml(tmp_path / "manifest.yaml", manifest)

    _assert_error(tmp_path, PackageErrorCode.DIGEST_MISMATCH, digest)


def test_symlink_is_rejected_even_when_it_points_inside_root(tmp_path: Path) -> None:
    digest = build_package(tmp_path)
    prompt = tmp_path / "prompts/system.md"
    original = tmp_path / "prompts/original.md"
    prompt.rename(original)
    prompt.symlink_to(original.name)

    _assert_error(tmp_path, PackageErrorCode.SYMLINK_FORBIDDEN, digest)


def test_bytes_changed_under_same_immutable_key_are_rejected(tmp_path: Path) -> None:
    original_digest = build_package(tmp_path)
    expected = package_ref(original_digest)
    (tmp_path / "prompts/system.md").write_text("attacker replacement\n", encoding="utf-8")
    rewrite_integrity(tmp_path)

    with pytest.raises(PackageLoadError) as raised:
        validate_package(tmp_path, tenant_id="tenant-a", expected_ref=expected)

    assert raised.value.code is PackageErrorCode.DIGEST_MISMATCH


def test_manifest_digest_value_is_excluded_from_its_file_checksum(tmp_path: Path) -> None:
    digest = build_package(tmp_path)
    before = parse_checksums((tmp_path / "checksums.txt").read_text(encoding="utf-8"))[
        "manifest.yaml"
    ]
    manifest = read_yaml(tmp_path / "manifest.yaml")
    manifest["digest"] = f"sha256:{hashlib.sha256(b'other').hexdigest()}"
    write_yaml(tmp_path / "manifest.yaml", manifest)

    with pytest.raises(PackageLoadError) as raised:
        validate_package(
            tmp_path,
            tenant_id="tenant-a",
            expected_ref=package_ref(digest),
        )

    after = parse_checksums((tmp_path / "checksums.txt").read_text(encoding="utf-8"))[
        "manifest.yaml"
    ]
    assert before == after
    assert raised.value.code is PackageErrorCode.DIGEST_MISMATCH


def test_rewritten_local_metadata_without_authoritative_reference_is_rejected(
    tmp_path: Path,
) -> None:
    package_root = tmp_path / "agent-metric-query"
    build_package(package_root)
    (package_root / "prompts/system.md").write_text(
        "attacker replacement\n", encoding="utf-8"
    )
    rewrite_integrity(package_root)

    loader = PackageLoader.local(tmp_path, tenant_id="tenant-a")
    with pytest.raises(PackageLoadError) as raised:
        loader.load("agent-metric-query", "0.1.0")

    assert raised.value.code.value == "PACKAGE_REFERENCE_REQUIRED"


def test_validator_requires_authoritative_reference_before_reading_package(
    tmp_path: Path,
) -> None:
    build_package(tmp_path)

    with pytest.raises(PackageLoadError) as raised:
        validate_package(tmp_path, tenant_id="tenant-a")

    assert raised.value.code.value == "PACKAGE_REFERENCE_REQUIRED"


def test_invalid_utf8_checksums_are_normalized_to_stable_error(tmp_path: Path) -> None:
    digest = build_package(tmp_path)
    (tmp_path / "checksums.txt").write_bytes(b"\xff\xfe")

    _assert_error(tmp_path, PackageErrorCode.CHECKSUM_INVALID, digest)


def test_checksums_read_oserror_is_normalized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    digest = build_package(tmp_path)
    original_open = Path.open

    def fail_checksums_open(
        path: Path, *args: Any, **kwargs: Any
    ) -> IO[Any]:
        if path.name == "checksums.txt":
            raise OSError("simulated read failure")
        return cast(IO[Any], original_open(path, *args, **kwargs))

    monkeypatch.setattr(Path, "open", fail_checksums_open)

    _assert_error(tmp_path, PackageErrorCode.CHECKSUM_INVALID, digest)


def test_oversized_checksums_are_rejected_before_parsing(tmp_path: Path) -> None:
    digest = build_package(tmp_path)
    (tmp_path / "checksums.txt").write_bytes(b"x" * 1_048_577)

    with pytest.raises(PackageLoadError) as raised:
        validate_package(
            tmp_path,
            tenant_id="tenant-a",
            expected_ref=package_ref(digest),
        )

    assert raised.value.code.value == "PACKAGE_FILE_TOO_LARGE"
