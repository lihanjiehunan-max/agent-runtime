import hashlib
import json
import re
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from apps.validation_runtime.domain import (
    AgentManifest,
    LoadedAgentAssets,
    LoadedSkill,
    ModelGatewayProfile,
)
from apps.validation_runtime.errors import RuntimeServiceError


RUNTIME_IDENTITY = b"deepagents:0.7.7"
LOGICAL_ID_PATTERN = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


class AssetLoader:
    def __init__(self, asset_root: Path):
        self._asset_root = Path(asset_root)

    def load_agent(self, agent_id: str) -> LoadedAgentAssets:
        if not self._is_logical_id(agent_id):
            raise RuntimeServiceError("AGENT_NOT_FOUND", f"Agent '{agent_id}' was not found.")

        manifest_path = self._contained_path(
            self._asset_root / "agents", agent_id, "manifest.json"
        )
        if not manifest_path.is_file():
            raise RuntimeServiceError("AGENT_NOT_FOUND", f"Agent '{agent_id}' was not found.")

        manifest_data, _ = self._load_json(manifest_path)
        try:
            manifest = AgentManifest.model_validate(manifest_data)
        except ValidationError as error:
            raise self._invalid(f"Agent manifest is invalid: {error}") from error

        if manifest.agent_id != agent_id:
            raise self._invalid("Agent manifest ID does not match its directory.")
        if len(set(manifest.skills)) != len(manifest.skills):
            raise self._invalid("Agent manifest contains duplicate Skill references.")

        gateway, gateway_bytes = self._load_gateway(manifest.model_gateway_id)
        skills = tuple(self._load_skill(skill_id) for skill_id in manifest.skills)
        manifest_bytes = self._canonical_json_bytes(manifest_data)
        package_digest = self._package_digest(manifest_bytes, gateway_bytes, skills)

        return LoadedAgentAssets(
            manifest=manifest,
            gateway=gateway,
            skills=skills,
            package_digest=package_digest,
        )

    def _load_gateway(self, gateway_id: str) -> tuple[ModelGatewayProfile, bytes]:
        gateway_directory = self._asset_root / "model_gateways"
        if not gateway_directory.is_dir():
            raise self._invalid("Model gateway assets directory is missing.")

        matched: list[tuple[ModelGatewayProfile, bytes]] = []
        for gateway_path in sorted(gateway_directory.glob("*.json")):
            gateway_data, _ = self._load_json(gateway_path)
            try:
                gateway = ModelGatewayProfile.model_validate(gateway_data)
            except ValidationError as error:
                raise self._invalid(f"Model gateway profile is invalid: {error}") from error
            if gateway.id == gateway_id:
                matched.append((gateway, self._canonical_json_bytes(gateway_data)))

        if len(matched) != 1:
            raise self._invalid(f"Model gateway '{gateway_id}' must resolve exactly once.")
        return matched[0]

    def _load_skill(self, skill_id: str) -> LoadedSkill:
        if not self._is_logical_id(skill_id):
            raise self._invalid("Skill reference is not a valid logical ID.")

        skill_path = self._contained_path(
            self._asset_root / "skills", skill_id, "SKILL.md"
        )
        source_bytes = self._read_bytes(skill_path, "Skill")
        try:
            source_text = source_bytes.decode("utf-8")
        except UnicodeDecodeError as error:
            raise self._invalid("Skill must be UTF-8 encoded.") from error

        front_matter, instructions = self._parse_front_matter(source_text)
        declared_id = front_matter.get("skill_id") or front_matter.get("id")
        if declared_id != skill_id:
            raise self._invalid("Skill front matter ID does not match its directory.")
        if not instructions.strip():
            raise self._invalid("Skill instructions are empty.")

        return LoadedSkill(
            skill_id=skill_id,
            instructions=instructions,
            source_bytes=source_bytes,
        )

    def _load_json(self, path: Path) -> tuple[dict[str, Any], bytes]:
        source_bytes = self._read_bytes(path, "JSON asset")
        try:
            data = json.loads(source_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise self._invalid("JSON asset cannot be parsed.") from error
        if not isinstance(data, dict):
            raise self._invalid("JSON asset must be an object.")
        return data, source_bytes

    def _read_bytes(self, path: Path, asset_type: str) -> bytes:
        try:
            return path.read_bytes()
        except OSError as error:
            raise self._invalid(f"{asset_type} is unreadable or missing.") from error

    def _parse_front_matter(self, source_text: str) -> tuple[dict[str, str], str]:
        lines = source_text.splitlines(keepends=True)
        if not lines or lines[0].strip() != "---":
            raise self._invalid("Skill must begin with front matter.")

        closing_index = next(
            (index for index, line in enumerate(lines[1:], start=1) if line.strip() == "---"),
            None,
        )
        if closing_index is None:
            raise self._invalid("Skill front matter is not terminated.")

        front_matter: dict[str, str] = {}
        for line in lines[1:closing_index]:
            key, separator, value = line.partition(":")
            if not separator or not key.strip() or not value.strip():
                raise self._invalid("Skill front matter is invalid.")
            front_matter[key.strip()] = value.strip().strip('"')
        return front_matter, "".join(lines[closing_index + 1 :])

    @staticmethod
    def _canonical_json_bytes(data: dict[str, Any]) -> bytes:
        return json.dumps(
            data,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @staticmethod
    def _is_logical_id(value: str) -> bool:
        return bool(LOGICAL_ID_PATTERN.fullmatch(value))

    def _contained_path(self, base_directory: Path, *parts: str) -> Path:
        try:
            resolved_asset_root = self._asset_root.resolve()
            resolved_base = base_directory.resolve()
            resolved_base.relative_to(resolved_asset_root)
            candidate = base_directory.joinpath(*parts).resolve()
            candidate.relative_to(resolved_base)
        except (OSError, RuntimeError, ValueError) as error:
            raise self._invalid("Asset path escapes its configured root.") from error
        return candidate

    @staticmethod
    def _package_digest(
        manifest_bytes: bytes,
        gateway_bytes: bytes,
        skills: tuple[LoadedSkill, ...],
    ) -> str:
        digest = hashlib.sha256()
        digest.update(manifest_bytes)
        digest.update(gateway_bytes)
        for skill in skills:
            digest.update(skill.source_bytes)
        digest.update(RUNTIME_IDENTITY)
        return digest.hexdigest()

    @staticmethod
    def _invalid(message: str) -> RuntimeServiceError:
        return RuntimeServiceError("AGENT_ASSET_INVALID", message)
