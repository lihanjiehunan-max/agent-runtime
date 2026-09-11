"""LangGraph checkpoint authority with transactional execution fencing.

Full typed checkpoints and pending writes are stored in SQL. No mutable shadow
chat history. Intermediate writes may arrive before their checkpoint record.
"""
import asyncio
import sqlalchemy as sa
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple, WRITES_IDX_MAP
from . import db as t
from .store import Conflict


class FencedSQLSaver(BaseCheckpointSaver):
    def __init__(self, store, claim):
        super().__init__()
        self.store, self.claim = store, claim
        self.thread_id = store.execution(claim.execution_id)['session_id']

    def _parts(self, config):
        parts = config['configurable']
        if parts['thread_id'] != self.thread_id:
            raise Conflict('Checkpoint thread does not belong to this execution')
        return parts['thread_id'], parts.get('checkpoint_ns', '')

    def get_tuple(self, config):
        thread, ns = self._parts(config)
        with self.store.db.tx(False) as c:
            q = sa.select(t.checkpoints).where(t.checkpoints.c.thread_id == thread,
                                              t.checkpoints.c.namespace == ns)
            ident = config['configurable'].get('checkpoint_id')
            if ident:
                q = q.where(t.checkpoints.c.checkpoint_id == ident)
            row = c.execute(q.order_by(t.checkpoints.c.checkpoint_id.desc()).limit(1)).mappings().first()
            if not row:
                return None
            writes = c.execute(sa.select(t.checkpoint_writes).where(
                t.checkpoint_writes.c.thread_id == thread, t.checkpoint_writes.c.namespace == ns,
                t.checkpoint_writes.c.checkpoint_id == row['checkpoint_id']).order_by(
                    t.checkpoint_writes.c.task_id, t.checkpoint_writes.c.idx)).mappings().all()
        cfg = {'configurable': {'thread_id': thread, 'checkpoint_ns': ns, 'checkpoint_id': row['checkpoint_id']}}
        parent = {'configurable': dict(cfg['configurable'], checkpoint_id=row['parent_id'])} if row['parent_id'] else None
        return CheckpointTuple(config=cfg,
            checkpoint=self.serde.loads_typed((row['type'], bytes(row['blob']))),
            metadata=self.serde.loads_typed((row['meta_type'], bytes(row['meta']))),
            parent_config=parent,
            pending_writes=[(w['task_id'], w['channel'], self.serde.loads_typed((w['type'], bytes(w['blob'])))) for w in writes])

    def list(self, config, *, filter=None, before=None, limit=None):
        if config is None:
            config = {'configurable': {'thread_id': self.thread_id}}
        thread, ns = self._parts(config)
        with self.store.db.tx(False) as c:
            q = sa.select(t.checkpoints.c.checkpoint_id).where(t.checkpoints.c.thread_id == thread,
                                                            t.checkpoints.c.namespace == ns)
            if before:
                q = q.where(t.checkpoints.c.checkpoint_id < before['configurable']['checkpoint_id'])
            ids = c.execute(q.order_by(t.checkpoints.c.checkpoint_id.desc())).scalars().all()
        count = 0
        for ident in ids:
            item = self.get_tuple({'configurable': {'thread_id': thread, 'checkpoint_ns': ns, 'checkpoint_id': ident}})
            if filter and any(item.metadata.get(k) != v for k, v in filter.items()):
                continue
            if limit is not None and count >= limit:
                return
            count += 1
            yield item

    def put(self, config, checkpoint, metadata, new_versions):
        thread, ns = self._parts(config)
        cp_type, blob = self.serde.dumps_typed(checkpoint)
        meta_type, meta = self.serde.dumps_typed(metadata)
        ident = checkpoint['id']
        with self.store.db.tx() as c:
            self.store.owned(c, self.claim)
            where = (t.checkpoints.c.thread_id == thread, t.checkpoints.c.namespace == ns,
                     t.checkpoints.c.checkpoint_id == ident)
            old = c.execute(sa.select(t.checkpoints.c.checkpoint_id).where(*where)).first()
            if not old:
                c.execute(t.checkpoints.insert().values(thread_id=thread, namespace=ns, checkpoint_id=ident,
                    execution_id=self.claim.execution_id, parent_id=config['configurable'].get('checkpoint_id'),
                    type=cp_type, blob=blob, meta_type=meta_type, meta=meta))
            if not ns:
                c.execute(t.executions.update().where(t.executions.c.id == self.claim.execution_id).values(checkpoint_id=ident))
        return {'configurable': {'thread_id': thread, 'checkpoint_ns': ns, 'checkpoint_id': ident}}

    def put_writes(self, config, writes, task_id, task_path=''):
        thread, ns = self._parts(config)
        with self.store.db.tx() as c:
            self.store.owned(c, self.claim)
            for idx, (channel, value) in enumerate(writes):
                index = WRITES_IDX_MAP.get(channel, idx)
                values = dict(execution_id=self.claim.execution_id, thread_id=thread, namespace=ns,
                              checkpoint_id=config['configurable']['checkpoint_id'], task_id=task_id, idx=index)
                where = [t.checkpoint_writes.c[k] == v for k, v in values.items()]
                old = c.execute(sa.select(t.checkpoint_writes.c.idx).where(*where)).first()
                typ, blob = self.serde.dumps_typed(value)
                if old:
                    if index < 0:
                        c.execute(t.checkpoint_writes.update().where(*where).values(channel=channel, type=typ, blob=blob))
                else:
                    c.execute(t.checkpoint_writes.insert().values(**values, channel=channel, type=typ, blob=blob))

    async def aget_tuple(self, config):
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(self, config, *, filter=None, before=None, limit=None):
        for item in await asyncio.to_thread(lambda: list(self.list(config, filter=filter, before=before, limit=limit))):
            yield item

    async def aput(self, config, checkpoint, metadata, new_versions):
        return await asyncio.to_thread(self.put, config, checkpoint, metadata, new_versions)

    async def aput_writes(self, config, writes, task_id, task_path=''):
        await asyncio.to_thread(self.put_writes, config, writes, task_id, task_path)
