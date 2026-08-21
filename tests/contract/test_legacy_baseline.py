def test_runtime_packages_import() -> None:
    from apps.runtime_api.main import create_api
    from packages.runtime_contracts import RuntimeType

    assert create_api().title == "Enterprise Agent Runtime"
    assert RuntimeType.DEEPAGENTS.value == "deepagents"
