import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, SecretStr


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset_root: Path
    model_api_key: SecretStr | None
    service_api_key: SecretStr | None

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        model_key = os.getenv("MODEL_API_KEY")
        service_key = os.getenv("SERVICE_API_KEY")
        return cls(
            asset_root=Path(__file__).parents[2] / "runtime_assets",
            model_api_key=SecretStr(model_key) if model_key else None,
            service_api_key=SecretStr(service_key) if service_key else None,
        )
