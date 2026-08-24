import json
import shutil
from pathlib import Path

import pytest

from apps.validation_runtime.errors import RuntimeServiceError
from apps.validation_runtime.services.asset_loader import AssetLoader


@pytest.fixture
def asset_root() -> Path:
    return Path(__file__).parents[2] / "runtime_assets"


def test_loads_static_assets(asset_root):
    assets = AssetLoader(asset_root).load_agent("shipping-analyst")
    assert assets.manifest.version == "0.1.0"
    assert str(assets.gateway.base_url).rstrip("/") == "https://token.zero-api.cc.cd/v1"
    assert assets.gateway.model == "gpt-5.6-sol"
    assert [x.skill_id for x in assets.skills] == ["shipping-operations-analyst"]
    assert "结论" in assets.skills[0].instructions
    assert len(assets.package_digest) == 64


def test_unknown_agent_has_stable_error(asset_root):
    with pytest.raises(RuntimeServiceError) as exc:
        AssetLoader(asset_root).load_agent("missing")
    assert exc.value.detail.code == "AGENT_NOT_FOUND"


def test_rejects_absolute_agent_id_before_loading_external_manifest(asset_root, tmp_path):
    external_agent = tmp_path / "external-agent"
    external_agent.mkdir()
    (external_agent / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "agent_id": str(external_agent),
                "version": "0.1.0",
                "runtime": "deepagents-python",
                "model_gateway_id": "zero-api-gpt-5.6-sol",
                "skills": ["shipping-operations-analyst"],
                "limits": {
                    "execution_timeout_seconds": 120,
                    "max_input_characters": 12000,
                },
            }
        )
    )

    with pytest.raises(RuntimeServiceError) as exc:
        AssetLoader(asset_root).load_agent(str(external_agent))

    assert exc.value.detail.code == "AGENT_NOT_FOUND"


def test_rejects_parent_traversal_agent_id_before_loading_external_manifest(
    asset_root, tmp_path
):
    isolated_root = tmp_path / "runtime_assets"
    shutil.copytree(asset_root, isolated_root)
    external_agent = tmp_path / "outside-agent"
    external_agent.mkdir()
    agent_id = "../../outside-agent"
    (external_agent / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "agent_id": agent_id,
                "version": "0.1.0",
                "runtime": "deepagents-python",
                "model_gateway_id": "zero-api-gpt-5.6-sol",
                "skills": ["shipping-operations-analyst"],
                "limits": {
                    "execution_timeout_seconds": 120,
                    "max_input_characters": 12000,
                },
            }
        )
    )

    with pytest.raises(RuntimeServiceError) as exc:
        AssetLoader(isolated_root).load_agent(agent_id)

    assert exc.value.detail.code == "AGENT_NOT_FOUND"


def test_rejects_manifest_skill_traversal_before_loading_external_skill(
    asset_root, tmp_path
):
    isolated_root = tmp_path / "runtime_assets"
    shutil.copytree(asset_root, isolated_root)
    skill_id = "../../outside-skill"
    manifest_path = isolated_root / "agents" / "shipping-analyst" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["skills"] = [skill_id]
    manifest_path.write_text(json.dumps(manifest))

    external_skill = tmp_path / "outside-skill"
    external_skill.mkdir()
    (external_skill / "SKILL.md").write_text(
        f"---\nskill_id: {skill_id}\n---\nExternal instructions\n"
    )

    with pytest.raises(RuntimeServiceError) as exc:
        AssetLoader(isolated_root).load_agent("shipping-analyst")

    assert exc.value.detail.code == "AGENT_ASSET_INVALID"


def test_rejects_agents_base_symlink_to_external_assets(asset_root, tmp_path):
    isolated_root = tmp_path / "runtime_assets"
    shutil.copytree(asset_root, isolated_root)
    external_agents = tmp_path / "external-agents"
    shutil.copytree(isolated_root / "agents", external_agents)
    shutil.rmtree(isolated_root / "agents")
    (isolated_root / "agents").symlink_to(external_agents, target_is_directory=True)

    with pytest.raises(RuntimeServiceError) as exc:
        AssetLoader(isolated_root).load_agent("shipping-analyst")

    assert exc.value.detail.code == "AGENT_ASSET_INVALID"


def test_rejects_skills_base_symlink_to_external_assets(asset_root, tmp_path):
    isolated_root = tmp_path / "runtime_assets"
    shutil.copytree(asset_root, isolated_root)
    external_skills = tmp_path / "external-skills"
    shutil.copytree(isolated_root / "skills", external_skills)
    shutil.rmtree(isolated_root / "skills")
    (isolated_root / "skills").symlink_to(external_skills, target_is_directory=True)

    with pytest.raises(RuntimeServiceError) as exc:
        AssetLoader(isolated_root).load_agent("shipping-analyst")

    assert exc.value.detail.code == "AGENT_ASSET_INVALID"
