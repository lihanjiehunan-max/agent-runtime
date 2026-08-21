from importlib.metadata import version

DEEPAGENTS_VERSION = "0.7.7"


def assert_compatible_deepagents() -> None:
    installed = version("deepagents")
    if installed != DEEPAGENTS_VERSION:
        raise RuntimeError(f"deepagents {DEEPAGENTS_VERSION} is required; found {installed}")


__all__ = ["DEEPAGENTS_VERSION", "assert_compatible_deepagents"]
