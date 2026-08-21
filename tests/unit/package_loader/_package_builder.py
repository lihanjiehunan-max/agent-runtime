from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, cast

import yaml

from packages.runtime_contracts import AgentPackageRef, RuntimeType

ZERO_DIGEST = f"sha256:{'0' * 64}"


def package_ref(digest: str, *, tenant_id: str = "tenant-a") -> AgentPackageRef:
    return AgentPackageRef(
        tenant_id=tenant_id,
        agent_id="agent-metric-query",
        version="0.1.0",
        digest=digest,
        runtime_type=RuntimeType.DEEPAGENTS,
        sdk_version="0.7.7",
    )


def write_yaml(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(value, allow_unicode=True, sort_keys=True),
        encoding="utf-8",
    )


def read_yaml(path: Path) -> dict[str, Any]:
    value = cast(object, yaml.safe_load(path.read_text(encoding="utf-8")))
    assert isinstance(value, dict)
    return cast(dict[str, Any], value)


def _manifest_hash(manifest: dict[str, Any]) -> str:
    without_digest = dict(manifest)
    without_digest.pop("digest", None)
    canonical = yaml.safe_dump(
        without_digest,
        allow_unicode=True,
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def rewrite_integrity(root: Path) -> str:
    manifest_path = root / "manifest.yaml"
    manifest = read_yaml(manifest_path)
    hashes: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "checksums.txt":
            continue
        relative = path.relative_to(root).as_posix()
        if relative == "manifest.yaml":
            hashes[relative] = _manifest_hash(manifest)
        else:
            hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()

    canonical_pairs = "".join(
        f"{path}\0sha256:{file_hash}\n" for path, file_hash in sorted(hashes.items())
    ).encode()
    digest = f"sha256:{hashlib.sha256(canonical_pairs).hexdigest()}"
    manifest["digest"] = digest
    write_yaml(manifest_path, manifest)
    (root / "checksums.txt").write_text(
        "".join(f"{file_hash}  {path}\n" for path, file_hash in sorted(hashes.items())),
        encoding="utf-8",
    )
    return digest


def build_package(root: Path) -> str:
    write_yaml(
        root / "manifest.yaml",
        {
            "schema_version": "agent.package.v1",
            "agent_id": "agent-metric-query",
            "version": "0.1.0",
            "digest": ZERO_DIGEST,
            "status": "active",
            "runtime": {"type": "deepagents", "sdk_version": "0.7.7"},
            "files": {
                "agent": "agent.yaml",
                "tool_bindings": "tool-bindings.yaml",
                "backend": "backend.yaml",
                "limits": "limits.yaml",
                "observability": "observability.yaml",
                "system_prompt": "prompts/system.md",
                "runtime": "runtime/deepagents/runtime.yaml",
            },
        },
    )
    write_yaml(
        root / "agent.yaml",
        {
            "schema_version": "agent.definition.v1",
            "name": "Metric Query Agent",
            "model": {"ref": "model://metric-query-primary"},
        },
    )
    write_yaml(
        root / "tool-bindings.yaml",
        {
            "schema_version": "agent.tool-bindings.v1",
            "allowlist": [{"name": "query_metric", "effect": "read_only"}],
        },
    )
    write_yaml(
        root / "backend.yaml",
        {"schema_version": "agent.backend.v1", "type": "state"},
    )
    write_yaml(
        root / "limits.yaml",
        {
            "schema_version": "agent.limits.v1",
            "session_ttl_minutes": 1440,
            "execution_timeout_seconds": 60,
            "max_model_calls": 6,
            "max_tool_calls": 8,
            "max_tokens": 20000,
        },
    )
    write_yaml(
        root / "observability.yaml",
        {"schema_version": "agent.observability.v1", "emit_runtime_events": True},
    )
    (root / "prompts").mkdir(parents=True, exist_ok=True)
    (root / "prompts/system.md").write_text("Answer metric questions.\n", encoding="utf-8")
    write_yaml(
        root / "runtime/deepagents/runtime.yaml",
        {
            "schema_version": "agent.runtime.deepagents.v1",
            "type": "deepagents",
            "sdk_version": "0.7.7",
        },
    )
    return rewrite_integrity(root)
