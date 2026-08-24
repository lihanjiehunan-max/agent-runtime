import re

import pytest

from apps.validation_runtime.domain import SessionStatus
from apps.validation_runtime.errors import RuntimeServiceError
from apps.validation_runtime.services.agent_instance_registry import AgentInstanceRegistry
from apps.validation_runtime.services.agent_router import AgentRouter
from apps.validation_runtime.services.asset_loader import AssetLoader
from apps.validation_runtime.services.session_manager import SessionManager


@pytest.fixture
def loaded_assets():
    return AssetLoader("runtime_assets").load_agent("shipping-analyst")


@pytest.fixture
def registry(loaded_assets):
    value = AgentInstanceRegistry()
    value.deploy(loaded_assets, object())
    return value


@pytest.fixture
def sessions(registry):
    return SessionManager(AgentRouter(registry), registry)


@pytest.fixture
def session(sessions):
    return sessions.create("shipping-analyst")


def test_session_is_bound_to_resolved_instance(sessions, session, registry):
    instance = registry.list()[0]

    assert re.fullmatch(r"ses_[0-9a-f-]{36}", session.session_id)
    assert session.thread_id == session.session_id
    assert session.agent_id == "shipping-analyst"
    assert session.bound_agent_instance_id == instance.agent_instance_id
    assert session.package_digest == instance.package_digest
    assert session.status is SessionStatus.IDLE
    assert session.turn_count == 0
    assert session.created_at.tzinfo is not None
    assert session.updated_at.tzinfo is not None


def test_session_creation_requires_an_active_instance():
    registry = AgentInstanceRegistry()
    sessions = SessionManager(AgentRouter(registry), registry)

    with pytest.raises(RuntimeServiceError) as exc:
        sessions.create("shipping-analyst")

    assert exc.value.detail.code == "AGENT_INSTANCE_UNAVAILABLE"


def test_unknown_session_raises_stable_error(sessions):
    with pytest.raises(RuntimeServiceError) as exc:
        sessions.get("ses_missing")

    assert exc.value.detail.code == "SESSION_NOT_FOUND"


def test_second_stream_claim_is_rejected(sessions, session):
    claimed = sessions.begin_stream(session.session_id)

    assert claimed.status is SessionStatus.STREAMING
    with pytest.raises(RuntimeServiceError) as exc:
        sessions.begin_stream(session.session_id)

    assert exc.value.detail.code == "SESSION_BUSY"


def test_success_increments_turn_without_changing_binding(sessions, session):
    sessions.begin_stream(session.session_id)

    completed = sessions.mark_succeeded(session.session_id)

    assert completed.status is SessionStatus.IDLE
    assert completed.turn_count == 1
    assert completed.thread_id == session.thread_id
    assert completed.bound_agent_instance_id == session.bound_agent_instance_id
    assert completed.package_digest == session.package_digest


def test_failure_returns_to_idle_without_incrementing_turn(sessions, session):
    sessions.begin_stream(session.session_id)

    failed = sessions.mark_failed(session.session_id)

    assert failed.status is SessionStatus.IDLE
    assert failed.turn_count == 0


def test_closed_session_cannot_stream_again(sessions, session):
    closed = sessions.close(session.session_id)

    assert closed.status is SessionStatus.CLOSED
    with pytest.raises(RuntimeServiceError) as exc:
        sessions.begin_stream(session.session_id)

    assert exc.value.detail.code == "SESSION_CLOSED"


def test_session_locks_are_stable_and_isolated(sessions, session):
    other = sessions.create("shipping-analyst")

    assert sessions.lock_for(session.session_id) is sessions.lock_for(session.session_id)
    assert sessions.lock_for(session.session_id) is not sessions.lock_for(other.session_id)
