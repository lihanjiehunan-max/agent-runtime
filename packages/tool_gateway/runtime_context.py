from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass

from packages.runtime_contracts import Principal, RuntimeEvent
from packages.tool_gateway.contracts import ToolEventContext


@dataclass(frozen=True, slots=True)
class RuntimeToolContext:
    """Request-scoped identity used by a graph-owned Tool Gateway binding."""

    principal: Principal
    event_context: ToolEventContext
    audit_events: list[RuntimeEvent]


_CURRENT_CONTEXT: ContextVar[RuntimeToolContext | None] = ContextVar(
    "runtime_tool_context",
    default=None,
)


def current_runtime_tool_context() -> RuntimeToolContext:
    context = _CURRENT_CONTEXT.get()
    if context is None:
        raise RuntimeError("query_metric is only available during a Runtime execution")
    return context


@contextmanager
def bind_runtime_tool_context(context: RuntimeToolContext) -> Generator[None, None, None]:
    token: Token[RuntimeToolContext | None] = _CURRENT_CONTEXT.set(context)
    try:
        yield
    finally:
        _CURRENT_CONTEXT.reset(token)


__all__ = [
    "RuntimeToolContext",
    "bind_runtime_tool_context",
    "current_runtime_tool_context",
]
