from collections.abc import Mapping
from types import MappingProxyType
from typing import cast

from pydantic import JsonValue


def freeze_json_object(value: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
    frozen = {key: _freeze_json_value(item) for key, item in value.items()}
    return cast(Mapping[str, JsonValue], MappingProxyType(frozen))


def thaw_json_object(value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    return {key: _thaw_json_value(item) for key, item in value.items()}


def _freeze_json_value(value: JsonValue) -> object:
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_json_value(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json_value(item) for item in value)
    return value


def _thaw_json_value(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return {
            cast(str, key): _thaw_json_value(item)
            for key, item in mapping.items()
        }
    if isinstance(value, tuple):
        items = cast(tuple[object, ...], value)
        return [_thaw_json_value(item) for item in items]
    return cast(JsonValue, value)


__all__ = ["freeze_json_object", "thaw_json_object"]
