import asyncio

import pytest

from apps.validation_runtime.domain import SessionStatus
from apps.validation_runtime.errors import RuntimeServiceError
from apps.validation_runtime.services.agent_instance_registry import AgentInstanceRegistry
from apps.validation_runtime.services.agent_router import AgentRouter
from apps.validation_runtime.services.asset_loader import AssetLoader
from apps.validation_runtime.services.session_manager import SessionManager
from apps.validation_runtime.services.streaming_chat_service import StreamingChatService
from tests.validation_runtime.fakes import RecordingStreamingGraph, human_chunk


def build_runtime(graph, *, timeout_seconds=120):
    assets = AssetLoader("runtime_assets").load_agent("shipping-analyst")
    if timeout_seconds != 120:
        limits = assets.manifest.limits.model_copy(
            update={"execution_timeout_seconds": timeout_seconds}
        )
        manifest = assets.manifest.model_copy(update={"limits": limits})
        assets = assets.model_copy(update={"manifest": manifest})
    registry = AgentInstanceRegistry()
    registry.deploy(assets, graph)
    sessions = SessionManager(AgentRouter(registry), registry)
    service = StreamingChatService(registry, sessions)
    return assets, sessions, service


@pytest.mark.asyncio
async def test_streams_assistant_deltas_then_one_done_on_the_session_thread():
    graph = RecordingStreamingGraph(["你", "好"])
    _, sessions, service = build_runtime(graph)
    session = sessions.create("shipping-analyst")

    stream = await service.open_stream(session.session_id, "hello")
    events = [event async for event in stream]

    assert [event.event for event in events] == ["delta", "delta", "done"]
    assert "".join(event.content for event in events[:-1]) == "你好"
    assert events[-1].turn_count == 1
    assert graph.calls == [
        {
            "input": {"messages": [{"role": "user", "content": "hello"}]},
            "config": {"configurable": {"thread_id": session.session_id}},
            "stream_mode": "messages",
        }
    ]
    assert sessions.get(session.session_id).turn_count == 1


@pytest.mark.asyncio
async def test_stream_forwards_list_text_and_ignores_non_assistant_chunks():
    graph = RecordingStreamingGraph(
        [
            human_chunk("not assistant"),
            [{"type": "text", "text": "有效文本"}],
            "",
        ]
    )
    _, sessions, service = build_runtime(graph)
    session = sessions.create("shipping-analyst")

    events = [
        event
        async for event in await service.open_stream(session.session_id, "hello")
    ]

    assert [event.event for event in events] == ["delta", "done"]
    assert events[0].content == "有效文本"


@pytest.mark.asyncio
@pytest.mark.parametrize("message", ["", "   "])
async def test_rejects_blank_message_before_claiming_session(message):
    _, sessions, service = build_runtime(RecordingStreamingGraph())
    session = sessions.create("shipping-analyst")

    with pytest.raises(RuntimeServiceError) as exc:
        await service.open_stream(session.session_id, message)

    assert exc.value.detail.code == "INVALID_MESSAGE"
    assert sessions.get(session.session_id).status is SessionStatus.IDLE


@pytest.mark.asyncio
async def test_rejects_message_larger_than_manifest_limit():
    assets, sessions, service = build_runtime(RecordingStreamingGraph())
    session = sessions.create("shipping-analyst")

    with pytest.raises(RuntimeServiceError) as exc:
        await service.open_stream(
            session.session_id,
            "x" * (assets.manifest.limits.max_input_characters + 1),
        )

    assert exc.value.detail.code == "INVALID_MESSAGE"


@pytest.mark.asyncio
async def test_rejects_unknown_closed_and_busy_sessions():
    _, sessions, service = build_runtime(RecordingStreamingGraph())

    with pytest.raises(RuntimeServiceError) as unknown:
        await service.open_stream("ses_missing", "hello")
    assert unknown.value.detail.code == "SESSION_NOT_FOUND"

    closed = sessions.create("shipping-analyst")
    sessions.close(closed.session_id)
    with pytest.raises(RuntimeServiceError) as closed_error:
        await service.open_stream(closed.session_id, "hello")
    assert closed_error.value.detail.code == "SESSION_CLOSED"

    busy = sessions.create("shipping-analyst")
    first = await service.open_stream(busy.session_id, "hello")
    with pytest.raises(RuntimeServiceError) as busy_error:
        await service.open_stream(busy.session_id, "again")
    assert busy_error.value.detail.code == "SESSION_BUSY"
    await first.aclose()


@pytest.mark.asyncio
async def test_provider_failure_emits_one_terminal_error_without_incrementing_turn():
    graph = RecordingStreamingGraph(["partial"], error=OSError("secret upstream body"))
    _, sessions, service = build_runtime(graph)
    session = sessions.create("shipping-analyst")

    events = [
        event
        async for event in await service.open_stream(session.session_id, "hello")
    ]

    assert [event.event for event in events] == ["delta", "error"]
    assert events[-1].code == "MODEL_GATEWAY_ERROR"
    assert events[-1].message == "Model gateway request failed"
    assert "secret" not in events[-1].message
    current = sessions.get(session.session_id)
    assert current.status is SessionStatus.IDLE
    assert current.turn_count == 0


@pytest.mark.asyncio
async def test_timeout_emits_one_terminal_error_and_releases_session():
    graph = RecordingStreamingGraph(wait_forever=True)
    _, sessions, service = build_runtime(graph, timeout_seconds=0)
    session = sessions.create("shipping-analyst")

    events = [
        event
        async for event in await service.open_stream(session.session_id, "hello")
    ]

    assert [event.event for event in events] == ["error"]
    assert events[0].code == "MODEL_GATEWAY_TIMEOUT"
    assert sessions.get(session.session_id).status is SessionStatus.IDLE


@pytest.mark.asyncio
async def test_closing_stream_early_releases_session_without_incrementing_turn():
    graph = RecordingStreamingGraph(["first"], wait_forever=True)
    _, sessions, service = build_runtime(graph)
    session = sessions.create("shipping-analyst")
    stream = await service.open_stream(session.session_id, "hello")

    first = await anext(stream)
    await stream.aclose()

    assert first.event == "delta"
    current = sessions.get(session.session_id)
    assert current.status is SessionStatus.IDLE
    assert current.turn_count == 0


@pytest.mark.asyncio
async def test_different_sessions_can_stream_concurrently():
    graph = RecordingStreamingGraph(["ok"])
    _, sessions, service = build_runtime(graph)
    first_session = sessions.create("shipping-analyst")
    second_session = sessions.create("shipping-analyst")

    first_stream, second_stream = await asyncio.gather(
        service.open_stream(first_session.session_id, "one"),
        service.open_stream(second_session.session_id, "two"),
    )
    first_events, second_events = await asyncio.gather(
        _collect(first_stream),
        _collect(second_stream),
    )

    assert first_events[-1].event == "done"
    assert second_events[-1].event == "done"


async def _collect(stream):
    return [event async for event in stream]
