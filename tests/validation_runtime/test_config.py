from apps.validation_runtime.config import RuntimeConfig


def test_runtime_config_reads_service_and_model_secrets(monkeypatch):
    monkeypatch.setenv("MODEL_API_KEY", "model-secret")
    monkeypatch.setenv("SERVICE_API_KEY", "service-secret")

    config = RuntimeConfig.from_env()

    assert config.model_api_key is not None
    assert config.service_api_key is not None
    assert config.model_api_key.get_secret_value() == "model-secret"
    assert config.service_api_key.get_secret_value() == "service-secret"
    assert "model-secret" not in repr(config)
    assert "service-secret" not in repr(config)


def test_runtime_config_preserves_missing_secrets_as_none(monkeypatch):
    monkeypatch.delenv("MODEL_API_KEY", raising=False)
    monkeypatch.delenv("SERVICE_API_KEY", raising=False)

    config = RuntimeConfig.from_env()

    assert config.model_api_key is None
    assert config.service_api_key is None
