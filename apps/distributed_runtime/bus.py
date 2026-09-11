"""At-least-once wakeups. Losing Redis never loses authoritative SQL work."""
from dataclasses import dataclass
import sqlalchemy as sa
from . import db as t


class OutboxPublisher:
    def __init__(self,store,bus): self.store,self.bus=store,bus

    def pending(self,limit=100):
        with self.store.db.tx(False) as c:
            return [dict(x) for x in c.execute(sa.select(t.outbox).where(
                t.outbox.c.delivered.is_(False)).order_by(t.outbox.c.id).limit(limit)).mappings()]

    def flush(self,limit=100):
        count=0
        for row in self.pending(limit):
            # Outside the SQL transaction. A crash here causes duplicate publication, not lost work.
            self.bus.publish(row['id'],row['execution_id'])
            with self.store.db.tx() as c:
                c.execute(t.outbox.update().where(t.outbox.c.id==row['id']).values(delivered=True))
            count+=1
        return count


@dataclass(frozen=True)
class Delivery:
    message_id: str
    execution_id: str


class RedisBus:
    def __init__(self,url,namespace='agent-runtime',consumer='worker'):
        import redis
        self.client=redis.Redis.from_url(url,decode_responses=True,socket_connect_timeout=2,socket_timeout=2)
        self.stream=namespace+':commands'
        self.group=namespace+':workers'
        self.consumer=consumer

    def _ensure_group(self):
        from redis.exceptions import ResponseError
        try: self.client.xgroup_create(self.stream,self.group,id='0',mkstream=True)
        except ResponseError as exc:
            if 'BUSYGROUP' not in str(exc): raise

    def publish(self,outbox_id,execution_id):
        return self.client.xadd(self.stream,{'outbox_id':str(outbox_id),'execution_id':execution_id},
            maxlen=10000,approximate=True)

    def poll(self,count=1,block=100):
        from redis.exceptions import ResponseError
        try:
            batches=self.client.xreadgroup(self.group,self.consumer,{self.stream:'>'},count=count,block=block)
        except ResponseError as exc:
            if 'NOGROUP' not in str(exc): raise
            self._ensure_group(); return []
        return [Delivery(mid,data['execution_id']) for _,rows in batches for mid,data in rows]

    def ack(self,delivery): self.client.xack(self.stream,self.group,delivery.message_id)
    def healthy(self): return bool(self.client.ping())
    def close(self): self.client.close()
