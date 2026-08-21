from collections.abc import Iterable
from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator

Identifier = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=254,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$",
    ),
]
Permission = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=254,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9:_-]*$",
    ),
]


class FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CommandContract(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class Principal(FrozenContract):
    tenant_id: Identifier
    user_id: Identifier
    actor_id: Identifier
    worker_id: Identifier | None = None
    permissions: tuple[Permission, ...] = ()

    @field_validator("permissions", mode="before")
    @classmethod
    def normalize_permissions(cls, value: Iterable[str]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))


__all__ = ["CommandContract", "FrozenContract", "Identifier", "Permission", "Principal"]
