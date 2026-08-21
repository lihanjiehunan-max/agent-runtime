from __future__ import annotations

from enum import StrEnum

from packages.runtime_contracts import ExecutionStatus


class ExecutionState(StrEnum):
    ACCEPTED = "accepted"
    LOADING_AGENT = "loading_agent"
    ACQUIRING_SESSION_LOCK = "acquiring_session_lock"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"

    @classmethod
    def from_status(cls, status: ExecutionStatus) -> ExecutionState:
        return cls(status.value)


TERMINAL_STATES = frozenset(
    {
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.TIMED_OUT,
        ExecutionState.CANCELLED,
    }
)

_ALLOWED_TRANSITIONS: dict[ExecutionState, frozenset[ExecutionState]] = {
    ExecutionState.ACCEPTED: frozenset({ExecutionState.LOADING_AGENT}),
    ExecutionState.LOADING_AGENT: frozenset(
        {ExecutionState.ACQUIRING_SESSION_LOCK, ExecutionState.FAILED}
    ),
    ExecutionState.ACQUIRING_SESSION_LOCK: frozenset(
        {ExecutionState.RUNNING, ExecutionState.FAILED}
    ),
    ExecutionState.RUNNING: frozenset(
        {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.TIMED_OUT,
            ExecutionState.CANCELLED,
        }
    ),
    ExecutionState.SUCCEEDED: frozenset(),
    ExecutionState.FAILED: frozenset(),
    ExecutionState.TIMED_OUT: frozenset(),
    ExecutionState.CANCELLED: frozenset(),
}


class InvalidExecutionTransition(RuntimeError):
    def __init__(self, current: ExecutionState, requested: ExecutionState) -> None:
        super().__init__(f"illegal execution transition: {current.value} -> {requested.value}")
        self.current = current
        self.requested = requested


class TerminalEventAlreadyEmitted(RuntimeError):
    pass


class ExecutionStateMachine:
    def __init__(self) -> None:
        self.state = ExecutionState.ACCEPTED
        self._terminal_event_emitted = False

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def terminal_event_emitted(self) -> bool:
        return self._terminal_event_emitted

    def transition(self, requested: ExecutionState) -> ExecutionState:
        if requested not in _ALLOWED_TRANSITIONS[self.state]:
            raise InvalidExecutionTransition(self.state, requested)
        self.state = requested
        return self.state

    def mark_terminal_event(self) -> None:
        if not self.terminal:
            raise InvalidExecutionTransition(self.state, self.state)
        if self._terminal_event_emitted:
            raise TerminalEventAlreadyEmitted(self.state.value)
        self._terminal_event_emitted = True


__all__ = [
    "ExecutionState",
    "ExecutionStateMachine",
    "InvalidExecutionTransition",
    "TERMINAL_STATES",
    "TerminalEventAlreadyEmitted",
]
