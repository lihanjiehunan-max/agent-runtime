"""Bounded business tool inputs. No credential, URL, SQL or executable input."""
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=256)]

class MetricQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    metric: Name
    period: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=64)] | None = None
    org: Name | None = None
    comparison: Literal['yoy', 'mom'] | None = None
    group_by: list[Name] | None = Field(default=None, max_length=8)

class AgentPackage(BaseModel):
    """An immutable deployable definition; infrastructure stays in the environment."""
    model_config = ConfigDict(extra='forbid', strict=True)
    agent_id: Annotated[str, StringConstraints(pattern=r'^[a-z0-9]+(?:-[a-z0-9]+)*$', min_length=1, max_length=96)]
    version: Annotated[str, StringConstraints(min_length=1, max_length=96)]
    engine: Literal['deepagents']
    engine_version: Annotated[str, StringConstraints(min_length=1, max_length=32)]
    prompt: Annotated[str, StringConstraints(min_length=1, max_length=64000)]
    capabilities: list[Name] = Field(max_length=32)
    timeout: float = Field(ge=1, le=86400, allow_inf_nan=False)
    tools: list[Literal['query_metric','record_metric']] | None = Field(default=None,max_length=2)
    allowed_agents: list[Name] | None = Field(default=None,max_length=32)
