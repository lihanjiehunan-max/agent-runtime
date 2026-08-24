import asyncio
from collections.abc import Iterable

from langchain_core.messages import AIMessageChunk, BaseMessageChunk, HumanMessageChunk


class RecordingStreamingGraph:
    def __init__(
        self,
        chunks: Iterable[str | list[dict[str, str]] | BaseMessageChunk] = (),
        *,
        error: Exception | None = None,
        wait_forever: bool = False,
    ) -> None:
        self._chunks = tuple(chunks)
        self._error = error
        self._wait_forever = wait_forever
        self.calls: list[dict[str, object]] = []

    async def astream(self, input, config, *, stream_mode):
        self.calls.append(
            {"input": input, "config": config, "stream_mode": stream_mode}
        )
        for chunk in self._chunks:
            if isinstance(chunk, BaseMessageChunk):
                message = chunk
            else:
                message = AIMessageChunk(content=chunk)
            yield message, {"langgraph_node": "model"}
        if self._wait_forever:
            await asyncio.Event().wait()
        if self._error is not None:
            raise self._error


def human_chunk(content: str) -> HumanMessageChunk:
    return HumanMessageChunk(content=content)
