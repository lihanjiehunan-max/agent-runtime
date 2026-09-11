"""Bounded, auditable parent access to immutable direct-child results.

Summaries are deterministic prefix excerpts, not LLM-generated conclusions.
Only small JSON results are inlined. Full bytes stay in the artifact store.
"""
from . import db as tables
from .store import Claim, Conflict, NotFound, canonical

INLINE_BYTES = 2048
SUMMARY_CHARS = 512
MAX_READ_CHARS = 2048


class ChildResultAccess:
    def __init__(self, store, artifacts):
        self.store = store
        self.artifacts = artifacts

    def _load(self, claim: Claim, child_id: str):
        # Do not accept an arbitrary object-store ref from the model. Resolve it
        # through the persisted parent-child relationship and current lease.
        with self.store.db.tx(False) as connection:
            self.store.owned(connection, claim)
            try:
                child = self.store._row(connection, tables.executions, child_id)
            except NotFound:
                raise Conflict('Not a completed direct child') from None
            if child['parent_id'] != claim.execution_id or child['status'] != 'COMPLETED':
                raise Conflict('Not a completed direct child')
            ref = child['output_ref']
        value = self.artifacts.get_json(ref)  # Includes content-digest verification.
        text = value.get('message') if isinstance(value, dict) else None
        if not isinstance(text, str):
            text = canonical(value).decode('utf-8')
        return ref, value, text

    def summaries(self, claim: Claim, group: str) -> list[dict]:
        with self.store.db.tx(False) as connection:
            self.store.owned(connection, claim)
        children = self.store.child_results(claim.execution_id, group)
        if not children:
            raise Conflict('Unknown delegation group')
        results = []
        for child in children:
            ref, value, text = self._load(claim, child['execution_id'])
            inline = len(canonical(value)) <= INLINE_BYTES
            descriptor = dict(child, summary=text[:SUMMARY_CHARS],
                              summary_kind='prefix_excerpt', content_chars=len(text),
                              truncated=len(text) > SUMMARY_CHARS, inline=inline)
            if inline:
                descriptor['result'] = value
            # Recheck ownership after storage I/O before releasing any content.
            self.store.emit(claim, 'child_result.summarized', {
                'child_id': child['execution_id'], 'output_ref': ref,
                'content_chars': len(text), 'inline': inline,
                'summary_kind': 'prefix_excerpt'})
            results.append(descriptor)
        return results

    def read(self, claim: Claim, child_id: str, offset: int = 0,
             limit: int = MAX_READ_CHARS) -> dict:
        if type(offset) is not int or type(limit) is not int or offset < 0 or not 1 <= limit <= MAX_READ_CHARS:
            raise Conflict('Use a nonnegative character offset and a limit of 1..2048')
        ref, _, text = self._load(claim, child_id)
        if offset > len(text):
            raise Conflict('Offset is beyond the result')
        end = min(len(text), offset + limit)
        eof = end == len(text)
        metadata = {'child_id': child_id, 'output_ref': ref, 'offset': offset,
                    'content_chars': len(text), 'returned_chars': end - offset,
                    'next_offset': None if eof else end, 'eof': eof}
        self.store.emit(claim, 'child_result.read', metadata)
        return dict(metadata, text=text[offset:end])
